package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"

	"codebench/bashguard/internal/analyze"
	"codebench/bashguard/internal/policy"
	"mvdan.cc/sh/v3/syntax"
)

const (
	maxRequestBytes = 2 << 20
	maxRows         = 128
	maxRowIDBytes   = 256
)

type rowID string

type rowInput struct {
	ID      rowID          `json:"id"`
	Command *string        `json:"command"`
	Context policy.Context `json:"context"`
}

type batchInput struct {
	RequiredRuntimeVersion RuntimeVersion  `json:"required_runtime_version"`
	Bundle                 json.RawMessage `json:"bundle"`
	Rows                   []rowInput      `json:"rows"`
}

func decodeBatch(body io.Reader) (batchInput, int, *diagnostic) {
	var batch batchInput
	bytesRead, err := io.ReadAll(body)
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		return batch, http.StatusRequestEntityTooLarge, &diagnostic{
			Code: "request_too_large", Message: "request body exceeds 2 MiB", Phase: "request",
		}
	}
	issue := &diagnostic{Code: "invalid_request", Phase: "request"}
	if err != nil {
		issue.Message = "could not read request body"
		return batch, http.StatusBadRequest, issue
	}
	if !utf8.Valid(bytesRead) || !json.Valid(bytesRead) {
		issue.Message = "request must contain exactly one valid UTF-8 JSON value"
		return batch, http.StatusBadRequest, issue
	}
	decoder := json.NewDecoder(bytes.NewReader(bytesRead))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&batch); err != nil {
		issue.Message = err.Error()
		return batch, http.StatusBadRequest, issue
	}
	if strings.TrimSpace(string(batch.RequiredRuntimeVersion)) == "" {
		issue.Field, issue.Message = "required_runtime_version", "runtime version is required"
		return batch, http.StatusBadRequest, issue
	}
	return batch, http.StatusOK, nil
}

func (b batchInput) validate() *diagnostic {
	issue := &diagnostic{Code: "invalid_request", Phase: "request"}
	if len(b.Bundle) == 0 || b.Bundle[0] != '{' {
		issue.Field, issue.Message = "bundle", "bundle must be a JSON object"
		return issue
	}
	if len(b.Rows) == 0 || len(b.Rows) > maxRows {
		issue.Field, issue.Message = "rows", fmt.Sprintf("expected 1..%d rows", maxRows)
		return issue
	}
	seen := make(map[rowID]struct{}, len(b.Rows))
	for i, row := range b.Rows {
		if strings.TrimSpace(string(row.ID)) == "" || len(row.ID) > maxRowIDBytes {
			issue.Field, issue.Message = fmt.Sprintf("rows[%d].id", i), "ID must be nonblank and at most 256 bytes"
			return issue
		}
		if _, duplicate := seen[row.ID]; duplicate {
			issue.Field, issue.Message = fmt.Sprintf("rows[%d].id", i), "duplicate row ID"
			return issue
		}
		seen[row.ID] = struct{}{}
		if row.Command == nil || strings.TrimSpace(*row.Command) == "" || len(*row.Command) > analyze.MaxSourceBytes {
			issue.Field, issue.Message = fmt.Sprintf("rows[%d].command", i), "command must be nonblank and at most 64 KiB"
			return issue
		}
	}
	return nil
}

func setBashPosition(issue *diagnostic, problem *analyze.Error) {
	if parse, ok := errors.AsType[syntax.ParseError](problem); ok {
		issue.Line, issue.Column = parse.Pos.Line(), parse.Pos.Col()
	}
}
