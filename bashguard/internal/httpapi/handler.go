package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"codebench/bashguard/internal/analyze"
	"codebench/bashguard/internal/policy"
)

type RuntimeVersion string

type handler struct {
	version RuntimeVersion
	batches chan struct{}
}

type diagnostic struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Phase   string `json:"phase,omitempty"`
	RuleID  string `json:"rule_id,omitempty"`
	Field   string `json:"field,omitempty"`
	Line    uint   `json:"line,omitempty"`
	Column  uint   `json:"column,omitempty"`
}

type identity struct {
	RuntimeVersion RuntimeVersion `json:"runtime_version"`
}

type failure struct {
	identity
	Error diagnostic `json:"error"`
}

type evaluation struct {
	identity
	BundleSHA256 string      `json:"bundle_sha256"`
	Results      []rowResult `json:"results"`
}

type rowResult struct {
	ID     rowID         `json:"id"`
	Result policy.Result `json:"result"`
	Error  *diagnostic   `json:"error,omitempty"`
}

func New(version RuntimeVersion) http.Handler {
	return &handler{version: version, batches: make(chan struct{}, 4)}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var method string
	switch r.URL.Path {
	case "/healthz", "/v1/version":
		method = http.MethodGet
	case "/v1/evaluate":
		method = http.MethodPost
	default:
		h.fail(w, http.StatusNotFound, diagnostic{Code: "not_found", Message: "unknown endpoint"})
		return
	}
	if r.Method != method {
		w.Header().Set("Allow", method)
		h.fail(w, http.StatusMethodNotAllowed, diagnostic{Code: "method_not_allowed", Message: "expected " + method})
		return
	}
	if method == http.MethodGet {
		writeJSON(w, http.StatusOK, identity{RuntimeVersion: h.version})
		return
	}
	h.evaluate(w, r)
}

func (h *handler) evaluate(w http.ResponseWriter, r *http.Request) {
	select {
	case h.batches <- struct{}{}:
		defer func() { <-h.batches }()
	default:
		h.fail(w, http.StatusServiceUnavailable, diagnostic{Code: "busy", Message: "evaluation capacity reached"})
		return
	}
	batch, status, issue := decodeBatch(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	if issue != nil {
		h.fail(w, status, *issue)
		return
	}
	if batch.RequiredRuntimeVersion != h.version {
		h.fail(w, http.StatusConflict, diagnostic{
			Code: "runtime_version_mismatch", Message: "requested runtime does not match the running executable",
			Phase: "version", Field: "required_runtime_version",
		})
		return
	}
	if issue := batch.validate(); issue != nil {
		h.fail(w, http.StatusBadRequest, *issue)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	engine, err := policy.Compile(batch.Bundle)
	if err != nil {
		issue := diagnostic{Code: "invalid_bundle", Message: err.Error(), Phase: "compile", Field: "bundle"}
		if rule, ok := errors.AsType[*policy.RuleError](err); ok {
			issue.RuleID, issue.Field, issue.Message = rule.RuleID, rule.Field, rule.Detail
		}
		h.fail(w, http.StatusUnprocessableEntity, issue)
		return
	}
	results := make([]rowResult, 0, len(batch.Rows))
	for _, row := range batch.Rows {
		if ctx.Err() != nil {
			break
		}
		results = append(results, evaluateRow(ctx, engine, row))
	}
	if ctx.Err() != nil {
		h.fail(w, http.StatusServiceUnavailable, diagnostic{Code: "evaluation_timeout", Message: "batch did not finish", Phase: "evaluate"})
		return
	}
	writeJSON(w, http.StatusOK, evaluation{
		identity: identity{RuntimeVersion: h.version}, BundleSHA256: engine.BundleSHA256(), Results: results,
	})
}

func evaluateRow(ctx context.Context, engine *policy.Engine, row rowInput) rowResult {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	result, err := engine.ClassifyWithDiagnostics(ctx, *row.Command, row.Context)
	out := rowResult{ID: row.ID, Result: result}
	switch {
	case ctx.Err() != nil:
		out.Error = &diagnostic{Code: "evaluation_timeout", Message: ctx.Err().Error(), Phase: "evaluate"}
	case err != nil:
		out.Error = &diagnostic{Code: "evaluation_error", Message: err.Error(), Phase: "evaluate"}
		if problem, ok := errors.AsType[*policy.RuntimeError](err); ok {
			out.Error.Code, out.Error.RuleID, out.Error.Field = "cel_runtime_error", problem.RuleID, "when"
		} else if errors.Is(err, policy.ErrContext) {
			out.Error.Code, out.Error.Field = "invalid_context", "context"
		} else if problem, ok := errors.AsType[*analyze.Error](err); ok {
			out.Error.Code, out.Error.Field = "invalid_command", "command"
			setBashPosition(out.Error, problem)
		}
	}
	return out
}

func (h *handler) fail(w http.ResponseWriter, status int, issue diagnostic) {
	writeJSON(w, status, failure{identity: identity{RuntimeVersion: h.version}, Error: issue})
}

func writeJSON[T identity | failure | evaluation](w http.ResponseWriter, status int, value T) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("bashguard HTTP response write failed: %v", err)
	}
}
