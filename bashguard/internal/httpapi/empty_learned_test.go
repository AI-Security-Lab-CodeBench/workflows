package httpapi

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
)

func TestEvaluateExplicitEmptyLearnedBundle(t *testing.T) {
	bundle := `{"version":1,"kind":"empty_learned","rules":[]}`
	rows := `[{"id":"simple","command":"pwd"},{"id":"compound","command":"pwd && pwd"},{"id":"invalid","command":"pwd |"}]`
	rec := post(t, requestBody(testVersion, bundle, rows), http.StatusOK)
	var got evaluation
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(bundle)))
	if got.BundleSHA256 != digest || got.RuntimeVersion != testVersion || len(got.Results) != 3 {
		t.Fatalf("incorrect identity or completeness: %+v", got)
	}
	for i, id := range []rowID{"simple", "compound"} {
		row := got.Results[i]
		if row.ID != id || row.Error != nil || row.Result.Status != "abstain" ||
			row.Result.BundleSHA256 != digest || row.Result.Labels != nil || len(row.Result.RuleIDs) != 0 ||
			len(row.Result.Sources) != 0 || len(row.Result.Rationales) != 0 ||
			!slices.Contains(row.Result.Reasons, "no_proven_rule") {
			t.Fatalf("expected attested no-match for %s: %+v", id, row)
		}
	}
	invalid := got.Results[2]
	if invalid.ID != "invalid" || invalid.Error == nil || invalid.Error.Code != "invalid_command" {
		t.Fatalf("expected a parser error, not an abstention: %+v", invalid)
	}
}

func TestEvaluateRejectsMalformedEmptyBundles(t *testing.T) {
	for name, bundle := range map[string]string{
		"ordinary empty": `{"version":1,"rules":[]}`,
		"missing rules":  `{"version":1,"kind":"empty_learned"}`,
		"null rules":     `{"version":1,"kind":"empty_learned","rules":null}`,
		"null kind":      `{"version":1,"kind":null,"rules":[]}`,
		"unknown kind":   `{"version":1,"kind":"other","rules":[]}`,
		"wrong version":  `{"version":2,"kind":"empty_learned","rules":[]}`,
		"nonempty":       strings.Replace(fmt.Sprintf(bundleTemplate, "true"), `"version": 1`, `"version": 1, "kind":"empty_learned"`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			rec := post(t, requestBody(testVersion, bundle, `[{"id":"row","command":"pwd"}]`), http.StatusUnprocessableEntity)
			var got failure
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if got.Error.Code != "invalid_bundle" || got.Error.Field != "bundle" {
				t.Fatalf("expected invalid bundle diagnostic: %+v", got)
			}
		})
	}
}
