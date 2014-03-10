package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

func fetch(url string, from, len int) (io.ReadCloser, error) {
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

func start() error {
	url := flag.Arg(0)
	body, err := fetch(url, 1, 1)
	if err != nil {
		return err
	}

	defer body.Close()
	io.Copy(os.Stdout, body)
	return nil
}

func main() {
	flag.Parse()
	if err := start(); err != nil {
		log.Fatal(err)
	}
}
