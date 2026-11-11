package analyze_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"codebench/bashguard/internal/analyze"
	"mvdan.cc/sh/v3/syntax/typedjson"
)

func TestAnalysisJSONRoundTripsTheNativeAST(t *testing.T) {
	for _, source := range []string{
		`kubectl delete namespace "payments"`,
		`printf "$(redis-cli FLUSHALL)" >audit.log`,
		`redis-cli FLUSHALL | cat`,
		`if true; then printf yes; else printf no; fi`,
		`for x in one two; do printf "$x"; done`,
		`values=(one "two"); printf "${values[@]}"`,
		`printf "${ACTION:-FLUSHALL}"`,
		"# heading\nprintf café # trailing\n",
		"cat <<'EOF'\ntext\nEOF\n",
	} {
		t.Run(source, func(t *testing.T) {
			document, err := analyze.Analyze(source)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			var payload struct {
				Source string
				AST    json.RawMessage
			}
			if err := json.Unmarshal(encoded, &payload); err != nil {
				t.Fatal(err)
			}
			restored, err := typedjson.Decode(bytes.NewReader(payload.AST))
			if err != nil {
				t.Fatal(err)
			}
			if payload.Source != source || !reflect.DeepEqual(document.Tree, restored) {
				t.Fatalf("native AST lost fields during JSON roundtrip: %s", encoded)
			}
		})
	}
}
