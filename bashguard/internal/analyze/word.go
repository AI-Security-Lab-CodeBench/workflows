package analyze

import (
	"strings"

	"mvdan.cc/sh/v3/syntax"
)

func staticParts(parts []syntax.WordPart) bool {
	for _, part := range parts {
		switch part := part.(type) {
		case *syntax.Lit:
		case *syntax.SglQuoted:
			if part.Dollar {
				return false
			}
		case *syntax.DblQuoted:
			if part.Dollar || !staticParts(part.Parts) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func staticWord(word *syntax.Word, raw string) (string, bool) {
	if !staticParts(word.Parts) || strings.ContainsRune(raw, '\r') {
		return "", false
	}
	var value strings.Builder
	var quote byte
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		switch {
		case quote == '\'':
			if c == '\'' {
				quote = 0
			} else {
				value.WriteByte(c)
			}
		case c == '\\':
			i++
			if i == len(raw) {
				return "", false
			}
			next := raw[i]
			if next == '\n' {
				continue
			}
			if quote == '"' && !strings.ContainsRune("$`\"\\", rune(next)) {
				value.WriteByte('\\')
			}
			value.WriteByte(next)
		case quote == '"':
			if c == '"' {
				quote = 0
			} else {
				value.WriteByte(c)
			}
		case c == '\'' || c == '"':
			quote = c
		case strings.ContainsRune("*?[{}~", rune(c)):
			return "", false
		default:
			value.WriteByte(c)
		}
	}
	if quote != 0 {
		return "", false
	}
	return value.String(), true
}
