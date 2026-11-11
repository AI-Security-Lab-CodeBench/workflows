package analyze_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"codebench/bashguard/internal/analyze"
)

func TestAnalyzeDecodesStaticWords(t *testing.T) {
	cases := []struct {
		name   string
		source string
		want   []string
	}{
		{"concatenation", `pri""ntf 'a b'"c"d "" ''`, []string{"printf", "a bcd", "", ""}},
		{"unquoted escapes", `printf a\ b \* \~ \{x,y\}`, []string{"printf", "a b", "*", "~", "{x,y}"}},
		{"double escapes", "printf \"\\q\\$\\\"\\\\\"", []string{"printf", "\\q$\"\\"}},
		{"continuations", "ec\\\nho a\\\nb \"c\\\nd\" 'e\\\nf'", []string{"echo", "ab", "cd", "e\\\nf"}},
		{"unicode", `printf café "雪"`, []string{"printf", "café", "雪"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc, err := analyze.Analyze(tc.source)
			if err != nil {
				t.Fatal(err)
			}
			if !doc.Eligible || len(doc.Calls) != 1 {
				t.Fatalf("expected one eligible call, got %+v", doc)
			}
			got := []string{}
			for _, word := range doc.Calls[0].Words {
				if !word.Static {
					t.Fatalf("word should be static: %+v", word)
				}
				got = append(got, word.Value)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("values = %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestAnalyzeAbstainsOnNonliteralOrCompoundRequests(t *testing.T) {
	for _, source := range []string{
		`printf "$HOME"`, `printf $(echo x)`, "printf `echo x`",
		`printf <(echo x)`, `printf $((1+2))`, `printf $'x'`, `printf $"x"`,
		`printf *.txt`, `printf a{b,c}`, `printf ~`, `printf @(x|y)`,
		`printf x | cat`, `printf x && cat`, `printf x; cat`, `printf x &`,
		`! printf x`, `X=1 printf x`, `printf x >out`, `declare X=1`,
		`f() { printf x; }`, ``, `# comment`, "printf \"a\r\nb\"",
	} {
		t.Run(source, func(t *testing.T) {
			doc, err := analyze.Analyze(source)
			if err != nil {
				t.Fatal(err)
			}
			if doc.Eligible || len(doc.Reasons) == 0 {
				t.Fatalf("expected abstention, got %+v", doc)
			}
		})
	}
}

func TestAnalyzeRejectsInvalidInputWithTypedErrors(t *testing.T) {
	cases := []struct {
		name, source, kind string
	}{
		{"size", strings.Repeat("a", analyze.MaxSourceBytes+1), "size"},
		{"utf8", "echo \xff", "utf8"},
		{"nul", "echo a\x00b", "nul"},
		{"syntax", "echo '", "parse"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := analyze.Analyze(tc.source)
			var typed *analyze.Error
			if !errors.As(err, &typed) || typed.Kind != tc.kind {
				t.Fatalf("error = %v, want typed kind %q", err, tc.kind)
			}
		})
	}
}

func TestAnalyzeRetainsOriginalByteSpansAndComments(t *testing.T) {
	source := "# head\nprintf café # tail\n# end\n"
	doc, err := analyze.Analyze(source)
	if err != nil {
		t.Fatal(err)
	}
	word := doc.Calls[0].Words[1]
	if word.Start != 14 || word.End != 19 || word.Raw != "café" {
		t.Fatalf("incorrect UTF-8 byte span: %+v", word)
	}
	var comments []string
	var parents []string
	for _, node := range doc.Nodes {
		if node.Kind == "Comment" {
			comments = append(comments, source[node.Start:node.End])
			parents = append(parents, doc.Nodes[node.Parent].Kind)
		}
	}
	if !reflect.DeepEqual(comments, []string{"# head", "# tail", "# end"}) {
		t.Fatalf("comments = %#v", comments)
	}
	if !reflect.DeepEqual(parents, []string{"Stmt", "Stmt", "File"}) {
		t.Fatalf("comment parents = %#v", parents)
	}
}

func TestAnalyzeRetainsNestedCallsWithoutMakingRequestEligible(t *testing.T) {
	doc, err := analyze.Analyze(`printf "$(echo x)"`)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Eligible || len(doc.Calls) != 2 || doc.Calls[1].Words[0].Value != "echo" {
		t.Fatalf("nested call representation incorrect: %+v", doc)
	}
	var parent string
	for _, node := range doc.Nodes {
		if node.Kind == "Stmt" && node.Start == 10 {
			parent = doc.Nodes[node.Parent].Kind
		}
	}
	if parent != "CmdSubst" {
		t.Fatalf("nested statement parent = %q", parent)
	}
}

func TestAnalyzeExposesOperatorsForComplexMatching(t *testing.T) {
	doc, err := analyze.Analyze(`echo "${x:-fallback}" | cat >out; ((++x + 2))`)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string][]string{}
	for _, node := range doc.Nodes {
		if node.Operator != "" {
			got[node.Kind] = append(got[node.Kind], node.Operator)
		}
	}
	want := map[string][]string{
		"BinaryCmd": {"|"}, "ParamExp": {":-"}, "Redirect": {">"},
		"BinaryArithm": {"+"}, "UnaryArithm": {"++"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("operators = %#v, want %#v", got, want)
	}
}

func TestAnalyzeRejectsExcessiveASTDepth(t *testing.T) {
	source := strings.Repeat("( ", analyze.MaxASTDepth) + "echo x" + strings.Repeat(" )", analyze.MaxASTDepth)

	_, err := analyze.Analyze(source)

	var typed *analyze.Error
	if !errors.As(err, &typed) || typed.Kind != "complexity" {
		t.Fatalf("expected complexity error, got %v", err)
	}
}
