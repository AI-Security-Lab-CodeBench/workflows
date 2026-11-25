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

type analysisInput struct {
	RequiredRuntimeVersion RuntimeVersion `json:"required_runtime_version"`
	Rows                   []rowInput     `json:"rows"`
}

func decodeBatch(body io.Reader) (batchInput, int, *diagnostic) {
	var batch batchInput
	status, issue := decodeRequest(body, &batch)
	if issue != nil {
		return batch, status, issue
	}
	if issue := requireVersion(batch.RequiredRuntimeVersion); issue != nil {
		return batch, http.StatusBadRequest, issue
	}
	return batch, http.StatusOK, nil
}

func decodeRequest[T batchInput | analysisInput](body io.Reader, input *T) (int, *diagnostic) {
	bytesRead, err := io.ReadAll(body)
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		return http.StatusRequestEntityTooLarge, &diagnostic{
			Code: "request_too_large", Message: "request body exceeds 2 MiB", Phase: "request",
		}
	}
	issue := &diagnostic{Code: "invalid_request", Phase: "request"}
	if err != nil {
		issue.Message = "could not read request body"
		return http.StatusBadRequest, issue
	}
	if !utf8.Valid(bytesRead) || !json.Valid(bytesRead) {
		issue.Message = "request must contain exactly one valid UTF-8 JSON value"
		return http.StatusBadRequest, issue
	}
	decoder := json.NewDecoder(bytes.NewReader(bytesRead))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(input); err != nil {
		issue.Message = err.Error()
		return http.StatusBadRequest, issue
	}
	return http.StatusOK, nil
}

func requireVersion(version RuntimeVersion) *diagnostic {
	if strings.TrimSpace(string(version)) == "" {
		return &diagnostic{Code: "invalid_request", Phase: "request", Field: "required_runtime_version", Message: "runtime version is required"}
	}
	return nil
}

func (b batchInput) validate() *diagnostic {
	issue := &diagnostic{Code: "invalid_request", Phase: "request"}
	if len(b.Bundle) == 0 || b.Bundle[0] != '{' {
		issue.Field, issue.Message = "bundle", "bundle must be a JSON object"
		return issue
	}
	return validateRows(b.Rows, maxRows)
}

func validateRows(rows []rowInput, limit int) *diagnostic {
	issue := &diagnostic{Code: "invalid_request", Phase: "request"}
	if len(rows) == 0 || len(rows) > limit {
		issue.Field, issue.Message = "rows", fmt.Sprintf("expected 1..%d rows", limit)
		return issue
	}
	seen := make(map[rowID]struct{}, len(rows))
	for i, row := range rows {
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
