package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
)

var (
	output    = flag.String("o", "-", "output file")
	workers   = flag.Int("n", 4, "parallel connections")
	chunkSize = flag.Int("c", 100*1024, "chunk size")
	debug     = flag.Bool("d", false, "debug")
)

type chunk struct {
	offset int64
	size   int64
	reader io.ReadCloser
}

type download struct {
	wg     sync.WaitGroup
	url    string
	size   int64
	writer io.Writer
}

func newDownload(url string) (*download, error) {
	size, err := contentSize(url)
	if err != nil {
		return nil, err
	}

	return &download{
		url:    url,
		size:   size,
		writer: os.Stdout,
	}, nil
}

// chunkFeeder generates chunk structures and sends them to the out channel.
// The chunk size is determined via commandline params. The last chunk is
// is sized accordingly.
func (d *download) chunkFeeder(out chan<- *chunk) {
	if *debug {
		log.Printf("Feeder is running")
	}
	size := int64(*chunkSize)
	for i := int64(0); ; i++ {
		if i*size >= d.size {
			break
		}
		if i*size+size > d.size {
			size = i*size + size - d.size
		}
		out <- &chunk{
			offset: i * size,
			size:   size}
	}
	if *debug {
		log.Printf("Feeder is closing")
	}
	close(out)
}

type FileDeleter struct {
	*os.File
}

func (f FileDeleter) Close() error {
	os.Remove(f.Name())
	return f.File.Close()
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
		body, err := fetch(d.url, ch.offset, ch.size)
		if err != nil {
			log.Fatal(err)
		}

		// TODO(mkm) check errors
		basename := *output
		if *output == "-" {
			basename = "/tmp/pget.tmp"
		}

		f, err := os.Create(fmt.Sprintf("%s.chunk-%d", basename, ch.offset))
		if err != nil {
			log.Fatalf("Cannot create temp file for chunk for offset %d, %s", ch.offset, err)
		}

		io.Copy(f, body)
		body.Close()
		f.Seek(0, 0)

		ch.reader = FileDeleter{f}
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
	outPos := int64(0)

	for ch := range in {
		if *debug {
			log.Printf("Chunk writer processing completed chunk %d (%d)\n", ch.offset, ch.size)
		}
		readyChunks[ch.offset] = ch

		for {
			if c, ok := readyChunks[outPos]; ok {
				// TODO(mkm) check for errors
				io.Copy(d.writer, c.reader)
				c.reader.Close()
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

	if *output != "-" {
		f, err := os.Create(*output)
		if err != nil {
			return err
		}
		defer f.Close()
		d.writer = f
	}

	if *debug {
		log.Println("Size", d.size)
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

	// the chunk writer block until
	// all the chunks are received, reordered and written.
	d.chunkWriter(och)

	return nil
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

// fetch returns a ReadCloser with the content of a
// section of the resource found at url starting at from
// and len long.
func fetch(url string, from, len int64) (io.ReadCloser, error) {
	req, err := http.NewRequest("GET", url, nil)
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
	return resp.Body, nil
}

func main() {
	flag.Parse()
	if err := start(); err != nil {
		log.Fatal(err)
	}
}
