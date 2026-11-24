package httpapi

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const testVersion RuntimeVersion = "sha256:test"

const bundleTemplate = `{
  "version": 1,
  "rules": [{
    "id": "demo.pwd", "when": %q,
    "labels": {"category":"CAT-6","risk":"CAT-6.1","severity":"low","action":"allow"},
    "rationale":"Synthetic test", "sources":["https://example.com/pwd"]
  }]
}`

func requestBody(version RuntimeVersion, bundle, rows string) string {
	return fmt.Sprintf(`{"required_runtime_version":%q, "bundle": %s , "rows":%s}`, version, bundle, rows)
}

func TestEvaluateReturnsExactBundleHashAndOrderedNativeResults(t *testing.T) {
	bundle := fmt.Sprintf(bundleTemplate, "argv == ['pwd', '-P']")
	rows := `[{"id":"match","command":"pwd -P"},{"id":"near-miss","command":"pwd -L"},{"id":"parse","command":"pwd |"},{"id":"context","command":"pwd","context":{"approval":"invalid"}}]`
	rec := post(t, requestBody(testVersion, bundle, rows), http.StatusOK)
	var got evaluation
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.RuntimeVersion != testVersion || got.BundleSHA256 != fmt.Sprintf("%x", sha256.Sum256([]byte(bundle))) || len(got.Results) != 4 {
		t.Fatalf("incorrect identity or completeness: %+v", got)
	}
	match, near, parse, context := got.Results[0], got.Results[1], got.Results[2], got.Results[3]
	if match.ID != "match" || match.Error != nil || match.Result.Status != "matched" ||
		match.Result.Labels == nil || match.Result.Labels.Risk != "CAT-6.1" || match.Result.BundleSHA256 != got.BundleSHA256 {
		t.Fatalf("expected native match: %+v", match)
	}
	if near.ID != "near-miss" || near.Error != nil || near.Result.Status != "abstain" {
		t.Fatalf("expected clean abstention: %+v", near)
	}
	if parse.ID != "parse" || parse.Error == nil || parse.Error.Code != "invalid_command" ||
		parse.Error.Line != 1 || parse.Error.Column != 5 {
		t.Fatalf("expected a positioned Bash diagnostic: %+v, %+v", parse, parse.Error)
	}
	if context.ID != "context" || context.Error == nil || context.Error.Code != "invalid_context" {
		t.Fatalf("expected invalid context: %+v", context)
	}
}

func TestEvaluatePreservesNativeCELRuntimeDiagnostic(t *testing.T) {
	rec := post(t, requestBody(testVersion, fmt.Sprintf(bundleTemplate, "argv[99] == 'pwd'"),
		`[{"id":"runtime","command":"pwd"}]`), http.StatusOK)
	var got evaluation
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Results) != 1 {
		t.Fatalf("expected a result: %+v", got)
	}
	row := got.Results[0]
	if row.Error == nil || row.Error.Code != "cel_runtime_error" || row.Error.RuleID != "demo.pwd" ||
		!strings.Contains(row.Error.Message, "index out of bounds") || row.Result.Status != "abstain" || row.Result.Labels != nil {
		t.Fatalf("expected fail-closed result with native CEL diagnostic: %+v, %+v", row, row.Error)
	}
}

func TestEvaluateRejectsInvalidBatchesBeforeReturningResults(t *testing.T) {
	bundle := fmt.Sprintf(bundleTemplate, "argv == ['pwd', '-P'")
	rows := `[{"id":"1","command":"pwd -P"}]`
	cases := []struct {
		name, body, code, field string
		status                  int
	}{
		{"mismatch before invalid CEL", requestBody("sha256:wrong", bundle, rows), "runtime_version_mismatch", "required_runtime_version", 409},
		{"native CEL caret", requestBody(testVersion, bundle, rows), "invalid_bundle", "when", 422},
		{"duplicate IDs", requestBody(testVersion, bundle, `[{"id":"1","command":"pwd"},{"id":"1","command":"pwd -P"}]`), "invalid_request", "rows[1].id", 400},
		{"too many rows", requestBody(testVersion, bundle, "["+strings.Repeat(`{"id":"1","command":"pwd"},`, 128)+`{"id":"129","command":"pwd"}]`), "invalid_request", "rows", 400},
		{"oversized body", strings.Repeat(" ", (2<<20)+1), "request_too_large", "", 413},
		{"trailing value", requestBody(testVersion, bundle, rows) + " null", "invalid_request", "", 400},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := post(t, tc.body, tc.status)
			var got failure
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if got.RuntimeVersion != testVersion || got.Error.Code != tc.code || got.Error.Field != tc.field {
				t.Fatalf("unexpected failure: %+v", got)
			}
			if tc.status == 422 && (got.Error.RuleID != "demo.pwd" ||
				!strings.Contains(got.Error.Message, "Syntax error") || !strings.Contains(got.Error.Message, "^")) {
				t.Fatalf("CEL diagnostic lost: %+v", got)
			}
		})
	}
}

func post(t *testing.T, body string, status int) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	New(testVersion).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/evaluate", strings.NewReader(body)))
	if rec.Code != status {
		t.Fatalf("status %d, want %d: %s", rec.Code, status, rec.Body)
	}
	return rec
}
