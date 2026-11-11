package policy

import (
	"context"
	"strings"
	"testing"

	"codebench/bashguard/internal/analyze"
)

func navigationEngine(t *testing.T, expression string) *Engine {
	t.Helper()
	rule := testRule(expression)
	rule.Scope = "ast"
	engine, err := compileBundle(t, rule)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func TestBindSelectedNodeAndNavigate(t *testing.T) {
	engine := navigationEngine(t, `
		cel.bind(cmd, syntax.CallExpr(ast.Stmts[0].Cmd),
			ast.parent() == null &&
			ast.children().size() == 1 &&
			cmd.parent() == ast.Stmts[0] &&
			cmd.parent().parent() == ast &&
			cmd.children().size() == 2 &&
			cmd.children()[0] == cmd.Args[0] &&
			cmd.children()[1] == cmd.Args[1] &&
			cmd.Args[1].children()[0].parent() == cmd.Args[1] &&
			cmd.Args[1].children()[0].children().size() == 0)`)
	for source, expected := range map[string]string{"printf x": "matched", "printf x y": "abstain"} {
		result, err := engine.Classify(context.Background(), source, Context{})
		if err != nil || result.Status != expected {
			t.Fatalf("%q: %+v, %v", source, result, err)
		}
	}
}

func TestNavigationPreservesRepeatedNodeIdentity(t *testing.T) {
	engine := navigationEngine(t, `
		cel.bind(cmd, syntax.CallExpr(ast.Stmts[0].Cmd),
			cmd.Args[1].children()[0].parent() == cmd.Args[1] &&
			cmd.Args[2].children()[0].parent() == cmd.Args[2] &&
			start(cmd.Args[1].children()[0].parent()) == 7 &&
			start(cmd.Args[2].children()[0].parent()) == 9)`)
	result, err := engine.Classify(context.Background(), "printf x x", Context{})
	if err != nil || result.Status != "matched" {
		t.Fatalf("repeated values collapsed: %+v, %v", result, err)
	}
}

func TestNavigationHandlesCommentValuesAndPointers(t *testing.T) {
	engine := navigationEngine(t, `
		ast.Stmts[0].Comments.size() == 2 &&
		ast.Stmts[0].Comments.all(c, c.parent() == ast.Stmts[0] && c.children().size() == 0) &&
		ast.Last.size() == 1 && ast.Last[0].parent() == ast &&
		ast_nodes.filter(n, type(n) == syntax.Comment).all(c, c.parent() != null) &&
		ast.Stmts[0].children().filter(n, type(n) == syntax.Comment).size() == 2`)
	result, err := engine.Classify(context.Background(), "# head\nprintf x # tail\n# end\n", Context{})
	if err != nil || result.Status != "matched" {
		t.Fatalf("comment navigation failed: %+v, %v", result, err)
	}
}

func TestNavigationInsideCommandSubstitution(t *testing.T) {
	engine := navigationEngine(t, `
		ast_nodes.exists(n, type(n) == syntax.CmdSubst &&
			cel.bind(sub, syntax.CmdSubst(n),
				sub.children().exists(s, type(s) == syntax.Stmt && s.parent() == sub &&
					s.children().exists(c, type(c) == syntax.CallExpr &&
						c.parent() == s && c.children().size() == 2))))`)
	for source, expected := range map[string]string{
		`printf "$(redis-cli FLUSHALL)"`: "matched",
		`printf '$(redis-cli FLUSHALL)'`: "abstain",
	} {
		result, err := engine.Classify(context.Background(), source, Context{})
		if err != nil || result.Status != expected {
			t.Fatalf("%q: %+v, %v", source, result, err)
		}
	}
}

func TestBindingScopeAndTypesRemainChecked(t *testing.T) {
	for _, expression := range []string{
		`cel.bind(local, ast, true) && local.parent() == null`,
		`cel.bind(cmd, syntax.CallExpr(ast.Stmts[0].Cmd), cmd.Arggs.size() == 2)`,
		`cel.bind(value, 1, value)`,
		`ast.parent(1) == null`,
	} {
		if _, err := compileBundle(t, testRule(expression)); err == nil {
			t.Fatalf("invalid binding or method compiled: %s", expression)
		}
	}
}

func TestInvalidNavigationReceiversFailClosed(t *testing.T) {
	for _, expression := range []string{
		`(1).children().size() == 0`,
		`null.parent() == null`,
		`ast.parent().children().size() == 0`,
		`syntax.File{}.children().size() == 0`,
	} {
		engine := navigationEngine(t, expression)
		result, err := engine.Classify(context.Background(), "printf x", Context{})
		if err != nil || result.Status != "abstain" || result.Labels != nil ||
			!strings.HasPrefix(result.Reasons[0], "rule_evaluation_error:") {
			t.Fatalf("%q: %+v, %v", expression, result, err)
		}
	}
}

func TestNavigationIsIsolatedAcrossConcurrentRequests(t *testing.T) {
	engine := navigationEngine(t, `
		cel.bind(root, ast, root.parent() == null &&
			ast_nodes.all(n, n == root ? n.parent() == null :
				n.parent().children().exists(child, child == n)))`)
	for _, source := range []string{
		"printf x", "echo café", "printf x x", "printf x | cat",
		`printf "$(echo x)"`, "# head\nprintf x # tail\n# last\n",
	} {
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			for range 32 {
				result, err := engine.Classify(context.Background(), source, Context{})
				if err != nil || result.Status != "matched" {
					t.Fatalf("navigation leaked across requests: %+v, %v", result, err)
				}
			}
		})
	}
}

func TestNavigationRejectsNodesFromAnotherParse(t *testing.T) {
	first, err := analyze.Analyze("printf x")
	if err != nil {
		t.Fatal(err)
	}
	second, err := analyze.Analyze("printf x")
	if err != nil {
		t.Fatal(err)
	}
	index := newASTNavigation(first)
	for _, node := range second.ASTNodes {
		if _, found := index.nativeNodeID(node); found {
			t.Fatal("foreign parse node accepted by identity index")
		}
	}
}
