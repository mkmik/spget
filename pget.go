package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/rcrowley/go-metrics"
)

var (
	output    = flag.String("o", "-", "output file")
	workers   = flag.Int("n", 4, "parallel connections")
	chunkSize = flag.Int("c", 100*1024, "chunk size")
	debug     = flag.Bool("d", false, "debug")
)

const (
	numRetries = 8
)

type chunk struct {
	offset int64
	size   int64
	file   string
}

type download struct {
	wg       sync.WaitGroup
	url      string
	size     int64
	startPos int64
	writer   io.Writer

	succeeded     counter
	downloaded    counter
	downloadMeter metrics.Meter
	progress      counter
}

type counter struct {
	v int64
}

func (c *counter) add(delta int64) {
	atomic.AddInt64(&c.v, delta)
}

func (c *counter) value() int64 {
	return atomic.LoadInt64(&c.v)
}

func newDownload(url string) (*download, error) {
	size, err := contentSize(url)
	if err != nil {
		return nil, err
	}

	return &download{
		url:           url,
		size:          size,
		writer:        os.Stdout,
		downloadMeter: metrics.NewMeter(),
	}, nil
}

// chunkFeeder generates chunk structures and sends them to the out channel.
// The chunk size is determined via commandline params. The last chunk is
// is sized accordingly.
func (d *download) chunkFeeder(out chan<- *chunk) {
	// when resuming a download. the chunk feeder won't emit chunks
	// that are already been written.
	d.succeeded.add(d.startPos)

	if *debug {
		log.Printf("Feeder is running")
	}
	for i := int64(0); ; i++ {
		size := int64(*chunkSize)
		off := i * size

		if off+size <= d.startPos {
			continue
		}
		if d.startPos-off > 0 && d.startPos-off < size {
			size -= d.startPos - off
			off = d.startPos
		}

		if off >= d.size {
			break
		}
		if off+size >= d.size {
			size = d.size - off
		}
		if *debug {
			log.Printf("Enqueuing chunk %d size %d", off, size)
		}
		out <- &chunk{
			offset: off,
			size:   size}
	}
	if *debug {
		log.Printf("Feeder is closing")
	}
	close(out)
}

// chunkWorker task chunks from the in channel, fetches the chunk from
// the http resource into the buffer in the chunk structure.
// The completed chunk is then sent to the out channel.
// Any error causes to panic.
func (d *download) chunkWorker(n int, in <-chan *chunk, out chan<- *chunk) {
	if *debug {
		log.Println("Started chunk worker", n)
	}

	for ch := range in {
		if *debug {
			log.Printf("Chunk worker %d fetching chunk %d (%d)\n", n, ch.offset, ch.size)
		}

		basename := *output
		if *output == "-" {
			basename = "/tmp/pget.tmp"
		}

		offset := ch.offset
		size := ch.size

		var f *os.File
		var err error

		tmpName := fmt.Sprintf("%s.chunk-%d", basename, ch.offset)
		if st, err := os.Stat(tmpName); os.IsNotExist(err) {
			f, err = os.Create(tmpName)
			if err != nil {
				log.Fatalf("Cannot create temp file for chunk for offset %d, %s", ch.offset, err)
			}
		} else if err == nil {
			f, err = os.OpenFile(tmpName, os.O_APPEND|os.O_WRONLY, 0)
			if err != nil {
				log.Fatalf("Cannot open temp file for appending chunk for offset %d, %s", ch.offset, err)
			}
			offset += st.Size()
			size -= st.Size()
		} else {
			log.Fatal("Cannot stat ", tmpName, err)
		}

		var body io.ReadCloser
		if size < 1 {
			if *debug {
				log.Printf("Chunk worker %d NOT fetching chunk %d (%d), because tmp data is complete.\n", n, ch.offset, ch.size)
			}

			body = ioutil.NopCloser(&bytes.Buffer{})
		} else {
			if *debug {
				log.Printf("Worker %d fetching chunk %d (%d) partial from %d -> size %d\n", n, ch.offset, ch.size, offset, size)
			}

			body, err = d.retryFetch(offset, size)
			if err != nil {
				log.Fatal(err)
			}
			// mark the whole chunk as succeeded
			// even if we continued an interrupted download.
			d.succeeded.add(ch.size)
		}

		if bs, err := io.Copy(f, body); err != nil {
			log.Fatalf("Error saving chunk %d (%d) after %d bytes: %s", ch.offset, ch.size, bs, err)
		}
		body.Close()
		f.Close()

		ch.file = tmpName
		out <- ch
	}

	if *debug {
		log.Println("Finishing chunk worker", n)
	}
}

// chunkWriter reads completed chunks from the in channel,
// and writes them out in the correct order.
// Out of order chunks are kept in memory until the missing pieces
// are delivered.
func (d *download) chunkWriter(in <-chan *chunk) {
	readyChunks := make(map[int64]*chunk)
	outPos := d.startPos
	d.progress.add(d.startPos)

	for ch := range in {
		if *debug {
			log.Printf("Chunk writer processing completed chunk %d (%d)\n", ch.offset, ch.size)
		}
		readyChunks[ch.offset] = ch

		for {
			if c, ok := readyChunks[outPos]; ok {
				f, err := os.Open(c.file)
				if err != nil {
					log.Fatal(err)
				}

				n, err := io.CopyN(d.writer, f, ch.size)
				if err != nil {
					log.Fatalf("Error merging chunk %d (%d) after %d bytes: %s", ch.offset, ch.size, n, err)
				}
				d.progress.add(n)

				f.Close()
				os.Remove(c.file)

				outPos += c.size
			} else {
				break
			}
		}
	}
}

// start
func start() error {
	url := flag.Arg(0)

	d, err := newDownload(url)
	if err != nil {
		return err
	}

	lastPos := int64(0)
	if *output != "-" {
		//f, err := os.Create(*output)
		f, err := os.OpenFile(*output, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return err
		}
		defer f.Close()
		d.writer = f

		lastPos, err = f.Seek(0, 2)
		if err != nil {
			log.Fatal(err)
		}
	}
	d.startPos = lastPos

	if *debug {
		log.Println("Size", d.size)
		log.Println("Last pos", lastPos)
	}

	ich := make(chan *chunk, 2)
	och := make(chan *chunk, 2)

	// spawn the chunk feeder.
	go d.chunkFeeder(ich)

	// spawn the workers in such a way
	// that we know when they are all done.
	for i := 0; i < *workers; i++ {
		d.wg.Add(1)
		go func(i int) {
			d.chunkWorker(i, ich, och)
			d.wg.Done()
		}(i)
	}

	// when all the workers are gone
	// close the chunk writer input
	go func() {
		d.wg.Wait()
		close(och)
	}()

	done := make(chan struct{})
	go d.showProgress(done)

	// the chunk writer block until
	// all the chunks are received, reordered and written.
	d.chunkWriter(och)

	close(done)
	d.printProgress()

	return nil
}

func (d *download) showProgress(done <-chan struct{}) {
	d.printProgress()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.printProgress()
		case <-done:
			return
		}
	}
}

// percentRatio returns the ration of two integers as a percentage.
func percentRatio(a, b int64) float64 {
	return float64(a) / float64(b) * 100.0
}

func (d *download) printProgress() {
	fmt.Fprintf(os.Stderr, "succeeded: %.2f%%; progress: %.2f%%; downloaded: %s; rate: %s/s\n", percentRatio(d.succeeded.value(), d.size), percentRatio(d.progress.value(), d.size), humanize.Bytes(uint64(d.downloaded.value())), humanize.Bytes(uint64(d.downloadMeter.Rate1())))
}

// contentSize returns the size of the resource
// at the given url.
func contentSize(url string) (int64, error) {
	resp, err := http.Get(url)
	if err != nil {
		return 0, err
	}
	return resp.ContentLength, nil
}

// followRedirect ensures that all headers are passed through the redirect.
// This is necessary for the range header to work across redirects.
func followRedirect(req *http.Request, via []*http.Request) error {
	req.Header = via[0].Header
	return nil
}

// retryFetch will retry a fetch a few times.
func (d *download) retryFetch(from, len int64) (r io.ReadCloser, err error) {
	t := 1 * time.Second
	for i := 0; i < numRetries; i++ {
		r, err = d.fetch(from, len)
		if err == nil {
			return
		}

		if *debug {
			log.Printf("waiting %s, retry %d", t, i+1)
		}
		time.Sleep(t)
		t *= 2
	}
	return
}

// fetch returns a ReadCloser with the content of a
// section of the resource found at url starting at from
// and len long.
func (d *download) fetch(from, len int64) (io.ReadCloser, error) {
	req, err := http.NewRequest("GET", d.url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", from, from+len-1))

	client := &http.Client{
		CheckRedirect: followRedirect,
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusPartialContent {
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return nil, fmt.Errorf("Server doesn't implement partial content")
		}
		return nil, errors.New(resp.Status)
	}
	return &measuringReader{resp.Body, &d.downloaded, d.downloadMeter}, nil
}

type measuringReader struct {
	io.ReadCloser
	*counter
	metrics.Meter
}

func (m *measuringReader) Read(p []byte) (int, error) {
	n, err := m.ReadCloser.Read(p)
	m.add(int64(n))
	m.Mark(int64(n))
	return n, err
}

func main() {
	flag.Parse()
	if err := start(); err != nil {
		log.Fatal(err)
	}
}
