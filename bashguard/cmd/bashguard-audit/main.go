package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"codebench/bashguard/internal/analyze"
)

func main() {
	if err := auditStream(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func auditStream(in io.Reader, out io.Writer) error {
	decoder := json.NewDecoder(in)
	encoder := json.NewEncoder(out)
	for {
		var command *string
		err := decoder.Decode(&command)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil || command == nil {
			return errors.New("audit input must be a stream of JSON strings")
		}
		if err := encoder.Encode(analyze.AuditSyntax(*command)); err != nil {
			return fmt.Errorf("write syntax audit: %w", err)
		}
	}
}
