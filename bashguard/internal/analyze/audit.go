package analyze

import (
	"maps"
	"slices"

	"mvdan.cc/sh/v3/syntax"
)

type SyntaxAudit struct {
	Status string   `json:"status"`
	Flags  []string `json:"flags"`
}

// AuditSyntax reports node presence without projecting or decoding shell words.
func AuditSyntax(source string) SyntaxAudit {
	file, err := parseBash(source)
	if err != nil {
		return SyntaxAudit{Status: err.Kind, Flags: []string{}}
	}
	if len(file.Stmts) == 0 {
		return SyntaxAudit{Status: "empty", Flags: []string{}}
	}
	flags := map[string]struct{}{}
	if len(file.Stmts) > 1 {
		flags["multiple_top_level_statements"] = struct{}{}
	}
	depth, count := 0, 0
	limited := false
	syntax.Walk(file, func(node syntax.Node) bool {
		if node == nil {
			depth--
			return true
		}
		if limited || depth >= MaxASTDepth || count >= MaxASTNodes {
			limited = true
			return false
		}
		depth++
		count++
		switch n := node.(type) {
		case *syntax.Assign:
			flags["assignment"] = struct{}{}
		case *syntax.BinaryCmd:
			switch n.Op {
			case syntax.Pipe, syntax.PipeAll:
				flags["pipeline"] = struct{}{}
			case syntax.AndStmt, syntax.OrStmt:
				flags["conditional"] = struct{}{}
			}
		case *syntax.IfClause, *syntax.CaseClause:
			flags["conditional"] = struct{}{}
		case *syntax.Redirect:
			flags["redirection"] = struct{}{}
		case *syntax.Stmt:
			if n.Background {
				flags["background"] = struct{}{}
			}
		case *syntax.CmdSubst:
			flags["command_substitution"] = struct{}{}
		case *syntax.ProcSubst:
			flags["process_substitution"] = struct{}{}
		case *syntax.ParamExp:
			flags["parameter_expansion"] = struct{}{}
		case *syntax.ArithmExp:
			flags["arithmetic_expansion"] = struct{}{}
		case *syntax.FuncDecl:
			flags["function_definition"] = struct{}{}
		}
		return true
	})
	if limited {
		return SyntaxAudit{Status: "complexity", Flags: []string{}}
	}
	ordered := slices.AppendSeq([]string{}, maps.Keys(flags))
	slices.Sort(ordered)
	return SyntaxAudit{Status: "parsed", Flags: ordered}
}
