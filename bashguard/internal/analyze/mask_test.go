package analyze_test

import (
	"errors"
	"testing"

	"codebench/bashguard/internal/analyze"
)

func TestMaskTargetsOneRepeatedOccurrenceByByteSpan(t *testing.T) {
	source := "printf café café # café\n"
	edits := []analyze.Edit{{Start: 13, End: 18, Replacement: "<value>"}}

	got, err := analyze.Mask(source, edits)

	if err != nil {
		t.Fatal(err)
	}
	if got != "printf café <value> # café\n" {
		t.Fatalf("masked source = %q", got)
	}
}

func TestMaskSortsEditsWithoutMutatingCallerSlice(t *testing.T) {
	edits := []analyze.Edit{
		{Start: 9, End: 10, Replacement: "second"},
		{Start: 7, End: 8, Replacement: "first"},
	}

	got, err := analyze.Mask("printf a b", edits)

	if err != nil {
		t.Fatal(err)
	}
	if got != "printf first second" || edits[0].Start != 9 {
		t.Fatalf("masked source = %q, edits = %+v", got, edits)
	}
}

func TestMaskRejectsInvalidOrOverlappingSpans(t *testing.T) {
	cases := []struct {
		name, source, kind string
		edits              []analyze.Edit
	}{
		{"negative", "echo x", "edit_bounds", []analyze.Edit{{Start: -1, End: 4}}},
		{"past end", "echo x", "edit_bounds", []analyze.Edit{{Start: 5, End: 8}}},
		{"empty", "echo x", "edit_bounds", []analyze.Edit{{Start: 5, End: 5}}},
		{"partial", "echo café", "edit_span", []analyze.Edit{{Start: 5, End: 8}}},
		{"mid rune", "echo café", "edit_span", []analyze.Edit{{Start: 5, End: 9}}},
		{"duplicates", "echo x", "edit_overlap", []analyze.Edit{{Start: 5, End: 6}, {Start: 5, End: 6}}},
		{"nested", `echo "$(echo x)"`, "edit_overlap", []analyze.Edit{{Start: 5, End: 16}, {Start: 13, End: 14}}},
		{"replacement utf8", "echo x", "edit_encoding", []analyze.Edit{{Start: 5, End: 6, Replacement: "\xff"}}},
		{"heredoc body", "cat <<'EOF'\nsecret\nEOF\n", "edit_span", []analyze.Edit{{Start: 12, End: 22}}},
		{"heredoc delimiter", "cat <<'EOF'\nsecret\nEOF\n", "edit_span", []analyze.Edit{{Start: 6, End: 11}}},
		{"crlf word", "echo x\r\n", "edit_span", []analyze.Edit{{Start: 5, End: 7}}},
		{"backquote remapping", "printf \"`printf \\\"secret\\\"`\"", "edit_syntax", []analyze.Edit{{Start: 17, End: 26}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := analyze.Mask(tc.source, tc.edits)
			var typed *analyze.Error
			if !errors.As(err, &typed) || typed.Kind != tc.kind {
				t.Fatalf("error = %v, want kind %q", err, tc.kind)
			}
		})
	}
}
