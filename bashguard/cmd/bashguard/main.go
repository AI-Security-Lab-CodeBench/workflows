package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"codebench/bashguard/internal/analyze"
	"codebench/bashguard/internal/policy"
)

type request struct {
	ID      string         `json:"id,omitempty"`
	Command string         `json:"command"`
	Context policy.Context `json:"context"`
	Edits   []analyze.Edit `json:"edits"`
}

type response struct {
	ID       string            `json:"id,omitempty"`
	Command  string            `json:"command,omitempty"`
	Analysis *analyze.Document `json:"analysis,omitempty"`
	Result   *policy.Result    `json:"result,omitempty"`
	Masked   *string           `json:"masked,omitempty"`
	Error    string            `json:"error,omitempty"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	mode := flag.String("mode", "classify", "classify, analyze, mask, or benchmark")
	rules := flag.String("rules", "rules/cat3.json", "JSON bundle compiled once at startup")
	flag.Parse()
	if flag.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	var engine *policy.Engine
	switch *mode {
	case "classify":
		file, err := os.Open(*rules)
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(io.LimitReader(file, (1<<20)+1))
		closeErr := file.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		engine, err = policy.Compile(data)
		if err != nil {
			return err
		}
	case "analyze", "mask":
	case "benchmark":
		return benchmark(os.Stdin, os.Stdout)
	default:
		return fmt.Errorf("unknown mode %q", *mode)
	}
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	encoder := json.NewEncoder(os.Stdout)
	invalid := false
	for scanner.Scan() {
		item, err := decode(scanner.Bytes())
		if err != nil {
			invalid = true
			if err := encoder.Encode(response{Error: err.Error()}); err != nil {
				return err
			}
			continue
		}
		output := response{ID: item.ID, Command: item.Command}
		switch *mode {
		case "analyze":
			doc, err := analyze.Analyze(item.Command)
			if err == nil {
				output.Analysis = &doc
			} else {
				output.Error = err.Error()
			}
		case "mask":
			masked, err := analyze.Mask(item.Command, item.Edits)
			if err == nil {
				output.Masked = &masked
			} else {
				output.Error = err.Error()
			}
		case "classify":
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			result, err := engine.Classify(ctx, item.Command, item.Context)
			cancel()
			output.Result = &result
			if err != nil {
				output.Error = err.Error()
			}
		}
		invalid = invalid || output.Error != ""
		if err := encoder.Encode(output); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if invalid {
		return fmt.Errorf("one or more requests failed; see per-record errors")
	}
	return nil
}

func decode(data []byte) (request, error) {
	var item request
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&item); err != nil {
		return item, err
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return item, fmt.Errorf("unexpected trailing JSON")
	}
	if strings.TrimSpace(item.Command) == "" {
		return item, fmt.Errorf("command is required")
	}
	if err := item.Context.Validate(); err != nil {
		return item, err
	}
	return item, nil
}
