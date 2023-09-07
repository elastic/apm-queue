package benchmark

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
)

func LoadFromJSON(name string) (Result, error) {
	fp, err := os.Open(name)
	if err != nil {
		return Result{}, fmt.Errorf("cannot open file: %w", err)
	}
	b, err := io.ReadAll(fp)
	if err != nil {
		return Result{}, fmt.Errorf("cannot read file: %w", err)
	}

	var data Result
	err = json.Unmarshal(b, &data)
	if err != nil {
		return Result{}, fmt.Errorf("cannot unmarshal content: %w", err)
	}

	return data, nil
}

func (r Result) WriteToJSON(w io.Writer) error {
	b, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("cannot marshal result to json: %w", err)
	}

	log.Println("persisting results")
	if _, err := w.Write(b); err != nil {
		log.Panicf("cannot persist results to io.Writer: %s", err)
	}
	w.Write([]byte("\n"))

	return nil
}
