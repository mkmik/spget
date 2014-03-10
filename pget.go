package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
)

type chunk struct {
	offset int64
	size   int64
	buffer bytes.Buffer
}

type download struct {
	wg          sync.WaitGroup
	url         string
	readyChunks map[int64]*chunk
	outPos      int64
	size        int64
	writer      io.Writer
}

func newDownload(url string) (*download, error) {
	size, err := contentSize(url)
	if err != nil {
		return nil, err
	}

	return &download{
		url:         url,
		readyChunks: make(map[int64]*chunk),
		size:        size,
		writer:      os.Stdout,
	}, nil
}

func (d *download) chunkFeeder(out chan<- *chunk) {
	log.Printf("Feeder is running")
	size := int64(100 * 1024)
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
	log.Printf("Feeder is closing")
	close(out)
}

func (d *download) chunkWorker(n int, in <-chan *chunk, out chan<- *chunk) {
	log.Println("Started chunk worker", n)
	defer d.wg.Done()

	for ch := range in {
		log.Printf("Chunk worker %d fetching chunk %d (%d)\n", n, ch.offset, ch.size)
		body, err := fetch(d.url, ch.offset, ch.size)
		if err != nil {
			log.Fatal(err)
		}

		// TODO(mkm) check errors
		io.Copy(&ch.buffer, body)
		body.Close()
		out <- ch
	}

	log.Println("Finishing chunk worker", n)
}

func (d *download) chunkWriter(in <-chan *chunk) {
	for ch := range in {
		log.Printf("Chunk writer processing completed chunk %d (%d)\n", ch.offset, len(ch.buffer.Bytes()))
		d.readyChunks[ch.offset] = ch

		for {
			if c, ok := d.readyChunks[d.outPos]; ok {
				io.Copy(d.writer, &c.buffer)
				d.outPos += c.size
			} else {
				break
			}
		}
	}
}

func start() error {
	url := flag.Arg(0)

	d, err := newDownload(url)
	if err != nil {
		return err
	}

	log.Println("Size", d.size)

	ich := make(chan *chunk, 1)
	och := make(chan *chunk, 1)

	go d.chunkFeeder(ich)
	for i := 0; i < 32; i++ {
		d.wg.Add(1)
		go d.chunkWorker(i, ich, och)
	}

	// when all the workers are gone
	// close the chunk writer input
	go func() {
		d.wg.Wait()
		close(och)
	}()

	d.chunkWriter(och)
	log.Printf("Chunk writer finished")

	/*
		body, err := fetch(url, 0, 12)
		if err != nil {
			return err
		}

		defer body.Close()
		io.Copy(os.Stdout, body)
	*/
	return nil
}

func contentSize(url string) (int64, error) {
	resp, err := http.Get(url)
	if err != nil {
		return 0, err
	}
	return resp.ContentLength, nil
}

func fetch(url string, from, len int64) (io.ReadCloser, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", from, from+len-1))

	client := &http.Client{}
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
