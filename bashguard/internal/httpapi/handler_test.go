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

func TestAnalyzeReturnsNativeSyntaxWithoutRunningCommand(t *testing.T) {
	body := fmt.Sprintf(`{"required_runtime_version":%q,"rows":[
		{"id":"compound","command":"cat -- 'report.txt' > result.txt"},
		{"id":"literal","command":"awk -F , -- 'NF > 0' 'report.txt'"},
		{"id":"invalid","command":"cat ("}
	]}`, testVersion)
	rec := httptest.NewRecorder()
	New(testVersion).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/analyze", strings.NewReader(body)))
	var batch analysisResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &batch); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || batch.RuntimeVersion != testVersion || len(batch.Results) != 3 {
		t.Fatalf("incorrect pinned analysis: %s", rec.Body)
	}
	compound, literal, invalid := batch.Results[0], batch.Results[1], batch.Results[2]
	if compound.Analysis == nil || compound.Analysis.Eligible || compound.ID != "compound" {
		t.Fatalf("expected ineligible shell structure: %+v", compound)
	}
	foundRedirect := false
	for _, node := range compound.Analysis.Nodes {
		foundRedirect = foundRedirect || node.Kind == "Redirect" && node.Operator == ">"
	}
	if !foundRedirect || literal.Analysis == nil || !literal.Analysis.Eligible ||
		len(literal.Analysis.Calls) != 1 || len(literal.Analysis.Calls[0].Words) != 6 ||
		literal.Analysis.Calls[0].Words[4].Value != "NF > 0" {
		t.Fatalf("expected native redirect and literal awk word boundaries: %s", rec.Body)
	}
	if invalid.Error == nil || invalid.Error.Code != "invalid_command" {
		t.Fatalf("expected typed parser error: %+v", invalid)
	}
}

func TestLearnedAllowIsRestrictedToSupportedPositiveShape(t *testing.T) {
	bundle := strings.Replace(fmt.Sprintf(bundleTemplate, "true"), `"when"`, `"capture_allow_shapes":[["cat","-n","--",""]], "when"`, 1)
	rec := post(t, requestBody(testVersion, bundle, `[
		{"id":"first","command":"cat -n -- 'one report.txt'"},
		{"id":"rename","command":"cat -n -- -two.csv"},
		{"id":"no-barrier","command":"cat -n -two.csv"},
		{"id":"stdin","command":"cat -n -- -"},
		{"id":"independent","command":"pwd -P"},
		{"id":"opaque","command":"awk -- 'NF > 0' report.txt"}
	]`), http.StatusOK)
	var batch evaluation
	if err := json.Unmarshal(rec.Body.Bytes(), &batch); err != nil {
		t.Fatal(err)
	}
	if len(batch.Results) != 6 {
		t.Fatalf("unexpected native results: %s", rec.Body)
	}
	for index, row := range batch.Results {
		if row.Error != nil || (len(row.Result.RuleIDs) > 0) != (index < 2) {
			t.Fatalf("shape guard gave unexpected decision for %s: %s", row.ID, rec.Body)
		}
	}
}

func TestLearnedPredicateRejectsRawEqualityWithoutDeclaredTokens(t *testing.T) {
	for _, when := range []string{
		`source == 'cat -n'`,
		`argv[0] == 'unregistered-tool'`,
		`argv.size() > 1 && argv[1] == 'custom-database-name'`,
		`argv.size() > 2 && argv[2] == r"NF > 0"`,
		`argv == ['cat', '-n', '--', 'sample.csv']`,
	} {
		t.Run(when, func(t *testing.T) {
			bundle := strings.Replace(fmt.Sprintf(bundleTemplate, when), `"when"`,
				`"capture_guarded":true, "when"`, 1)
			rec := post(t, requestBody(testVersion, bundle, `[{"id":"row","command":"cat -n"}]`), http.StatusUnprocessableEntity)
			var rejected failure
			if err := json.Unmarshal(rec.Body.Bytes(), &rejected); err != nil {
				t.Fatal(err)
			}
			if rejected.Error.Code != "raw_literal_overfit" {
				t.Fatalf("expected explicit learned policy rejection: %s", rec.Body)
			}
		})
	}
}

func TestLearnedPredicateAcceptsRuntimeVocabulary(t *testing.T) {
	bundle := strings.Replace(fmt.Sprintf(bundleTemplate, `argv.size() == 4 && argv[0] == 'cat' && argv[1] == '-n' && argv[2] == '--'`),
		`"when"`, `"capture_guarded":true, "when"`, 1)
	rec := post(t, requestBody(testVersion, bundle, `[{"id":"row","command":"cat -n -- renamed.csv"}]`), http.StatusOK)
	var accepted evaluation
	if err := json.Unmarshal(rec.Body.Bytes(), &accepted); err != nil {
		t.Fatal(err)
	}
	if len(accepted.Results) != 1 || len(accepted.Results[0].Result.RuleIDs) != 1 {
		t.Fatalf("expected structural matcher to survive compiler gate: %s", rec.Body)
	}
}

func TestLearnedASTPredicateUsesNoCLITokens(t *testing.T) {
	const structural = `ast.Stmts[0].Redirs.exists(r, operator(r) == '>')`
	const raw = `ast.Stmts[0].Redirs.exists(r, syntax.Lit(r.Word.Parts[0]).Value == 'report.txt')`
	const rows = `[{"id":"redirect","command":"cat >report.txt"},{"id":"near-miss","command":"cat <renamed.txt"}]`
	for _, test := range []struct {
		name, predicate, fields string
		status                  int
	}{
		{"learned structural", structural, `"capture_guarded":true,`, http.StatusOK},
		{"learned raw", raw, `"capture_guarded":true,`, http.StatusUnprocessableEntity},
		{"legacy authored raw", raw, "", http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			bundle := strings.Replace(fmt.Sprintf(bundleTemplate, test.predicate), `"when"`,
				test.fields+` "scope":"ast", "when"`, 1)
			bundle = strings.Replace(bundle, `"action":"allow"`, `"action":"ask"`, 1)
			rec := post(t, requestBody(testVersion, bundle, rows), test.status)
			if test.status == http.StatusUnprocessableEntity {
				var rejected failure
				if err := json.Unmarshal(rec.Body.Bytes(), &rejected); err != nil {
					t.Fatal(err)
				}
				if rejected.Error.Code != "raw_literal_overfit" {
					t.Fatalf("expected raw AST rejection: %s", rec.Body)
				}
				return
			}
			var accepted evaluation
			if err := json.Unmarshal(rec.Body.Bytes(), &accepted); err != nil {
				t.Fatal(err)
			}
			if len(accepted.Results) != 2 || accepted.Results[0].Result.Status != "matched" ||
				accepted.Results[1].Result.Status != "abstain" {
				t.Fatalf("expected only the output redirect to match: %s", rec.Body)
			}
		})
	}
}

func TestLearnedCodecAllowRequiresReadOnlyStaticShape(t *testing.T) {
	bundle := strings.Replace(fmt.Sprintf(bundleTemplate, "true"), `"when"`,
		`"capture_guarded":true, "capture_allow_shapes":[["base64","-w","@decimal",""],`+
			`["base64","--decode",""],["base32","-w","@decimal","-"]], "when"`, 1)
	rows := `[
		{"id":"wrapped","command":"base64 -w 0 one.bin"},
		{"id":"renamed","command":"base64 -w 76 other.csv"},
		{"id":"decode","command":"base64 --decode other.csv"},
		{"id":"stdin","command":"base32 -w 12 -"},
		{"id":"arbitrary-number","command":"base64 -w bogus one.bin"},
		{"id":"oversized-number","command":"base64 -w 99999 one.bin"},
		{"id":"dynamic","command":"base64 -w \"$WIDTH\" one.bin"},
		{"id":"wildcard","command":"base64 -w 76 *.bin"},
		{"id":"quoted-wildcard","command":"base64 -w 76 '*.bin'"},
		{"id":"unknown","command":"base64 -w 76 --output=out.bin"},
		{"id":"redirect","command":"base64 -w 76 one.bin > out.bin"},
		{"id":"execute","command":"base64 --decode one.bin | bash"}
	]`
	rec := post(t, requestBody(testVersion, bundle, rows), http.StatusOK)
	var got evaluation
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Results) != 12 {
		t.Fatalf("expected all test rows: %s", rec.Body)
	}
	for index, row := range got.Results {
		if row.Error != nil || (row.Result.Status == "matched") != (index < 4) {
			t.Fatalf("unexpected codec decision: %+v", row)
		}
	}
}
