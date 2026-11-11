package analyze

import (
	"cmp"
	"slices"
	"strings"
	"unicode/utf8"
)

func Mask(source string, edits []Edit) (string, error) {
	doc, err := Analyze(source)
	if err != nil {
		return "", err
	}
	for _, node := range doc.Nodes {
		if node.Kind == "CmdSubst" && node.Operator == "`" {
			return "", &Error{Kind: "edit_syntax", Detail: "legacy backquote editing is unsupported"}
		}
	}
	type span struct{ start, end int }
	words := make(map[span]struct{})
	for _, call := range doc.Calls {
		for _, word := range call.Words {
			if !strings.ContainsRune(word.Raw, '\r') {
				words[span{word.Start, word.End}] = struct{}{}
			}
		}
	}
	ordered := slices.Clone(edits)
	slices.SortFunc(ordered, func(a, b Edit) int {
		if order := cmp.Compare(a.Start, b.Start); order != 0 {
			return order
		}
		return cmp.Compare(a.End, b.End)
	})
	end := 0
	var result strings.Builder
	for _, edit := range ordered {
		if edit.Start < 0 || edit.End <= edit.Start || edit.End > len(source) {
			return "", &Error{Kind: "edit_bounds", Detail: "edit has invalid byte bounds"}
		}
		if !utf8.ValidString(edit.Replacement) || strings.IndexByte(edit.Replacement, 0) >= 0 {
			return "", &Error{Kind: "edit_encoding", Detail: "replacement must be UTF-8 without NUL"}
		}
		if edit.Start < end {
			return "", &Error{Kind: "edit_overlap", Detail: "edits overlap"}
		}
		if _, ok := words[span{edit.Start, edit.End}]; !ok {
			return "", &Error{Kind: "edit_span", Detail: "edit must cover one whole parsed word"}
		}
		result.WriteString(source[end:edit.Start])
		result.WriteString(edit.Replacement)
		end = edit.End
	}
	result.WriteString(source[end:])
	return result.String(), nil
}
