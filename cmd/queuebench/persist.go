package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/elastic/apm-queue/cmd/queuebench/pkg/benchmark"
)

func persist(file string, r benchmark.Result) error {
	openfile := func(file string) (*os.File, error) {
		f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			return nil, fmt.Errorf("cannot open specified file: %w", err)
		}
		return f, nil
	}

	o := os.Stdout
	if file != "" {
		var err error
		o, err = openfile(file)
		if err != nil {
			return err
		}
	}

	b, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("cannot marshal result to json: %w", err)
	}

	log.Println("persisting results")
	if _, err := o.Write(b); err != nil {
		log.Panicf("cannot persist results to io.Writer: %s", err)
	}
	o.Write([]byte("\n"))

	return nil
}
