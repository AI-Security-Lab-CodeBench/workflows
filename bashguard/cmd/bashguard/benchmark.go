package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"codebench/bashguard/internal/analyze"
	"mvdan.cc/sh/v3/syntax"
)

func benchmark(input io.Reader, output io.Writer) error {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	var sources []string
	for scanner.Scan() {
		item, err := decode(scanner.Bytes())
		if err != nil {
			return err
		}
		if len(item.Command) > 65536 || len(sources) >= 100000 {
			return fmt.Errorf("benchmark input limit exceeded")
		}
		sources = append(sources, item.Command)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	parseErrors := 0
	start := time.Now()
	for _, source := range sources {
		parser := syntax.NewParser(syntax.Variant(syntax.LangBash), syntax.KeepComments(true))
		if _, err := parser.Parse(strings.NewReader(source), ""); err != nil {
			parseErrors++
		}
	}
	parseNS := time.Since(start).Nanoseconds()
	start = time.Now()
	analysisErrors := 0
	for _, source := range sources {
		if _, err := analyze.Analyze(source); err != nil {
			analysisErrors++
		}
	}
	result := struct {
		Count          int   `json:"count"`
		ParseNS        int64 `json:"parse_ns"`
		AnalyzeNS      int64 `json:"analyze_ns"`
		Errors         int   `json:"errors"`
		AnalysisErrors int   `json:"analysis_errors"`
	}{len(sources), parseNS, time.Since(start).Nanoseconds(), parseErrors, analysisErrors}
	return json.NewEncoder(output).Encode(result)
}
