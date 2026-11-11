package policy

import (
	"context"
	"strings"
	"testing"
)

func TestRulesInspectNativeASTFields(t *testing.T) {
	engine, err := compileBundle(t, testRule(
		`ast.Stmts.size() == 1 && !ast.Stmts[0].Negated &&
		type(ast.Stmts[0].Cmd) == syntax.CallExpr &&
		syntax.Lit(syntax.CallExpr(ast.Stmts[0].Cmd).Args[1].Parts[0]).Value == 'FLUSHALL'`,
	))
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		source string
		status string
	}{
		{"redis-cli FLUSHALL", "matched"},
		{"redis-cli INFO", "abstain"},
	} {
		result, err := engine.Classify(context.Background(), item.source, Context{})
		if err != nil || result.Status != item.status {
			t.Fatalf("%q: %+v, %v", item.source, result, err)
		}
	}
}

func TestRootASTFieldTyposFailCompilation(t *testing.T) {
	for _, field := range []string{
		`ast.Stmts[0].Negtaed`,
		`syntax.CallExpr(ast.Stmts[0].Cmd).Arggs`,
	} {
		_, err := compileBundle(t, testRule(field+` == false`))
		if err == nil || !strings.Contains(err.Error(), "undefined field") {
			t.Fatalf("expected field diagnostic, got %v", err)
		}
	}
}

func TestASTScopeEvaluatesNestedStructures(t *testing.T) {
	cases := []struct {
		name, expression, positive, negative string
	}{
		{
			"redirect",
			`ast.Stmts.size() == 1 && ast.Stmts[0].Redirs.exists(r,
				operator(r) == '>' && syntax.Lit(r.Word.Parts[0]).Value == 'audit.log')`,
			"redis-cli FLUSHALL >audit.log", "redis-cli FLUSHALL <audit.log",
		},
		{
			"pipeline",
			`ast.Stmts.size() == 1 && type(ast.Stmts[0].Cmd) == syntax.BinaryCmd &&
				operator(ast.Stmts[0].Cmd) == '|' &&
				type(syntax.BinaryCmd(ast.Stmts[0].Cmd).X.Cmd) == syntax.CallExpr &&
				syntax.Lit(syntax.CallExpr(syntax.BinaryCmd(ast.Stmts[0].Cmd).X.Cmd).Args[0].Parts[0]).Value == 'redis-cli'`,
			"redis-cli FLUSHALL | cat", "redis-cli FLUSHALL && cat",
		},
		{
			"substitution",
			`ast.Stmts.size() == 1 && type(ast.Stmts[0].Cmd) == syntax.CallExpr &&
				syntax.CallExpr(ast.Stmts[0].Cmd).Args[1].Parts.exists(p,
					type(p) == syntax.DblQuoted && syntax.DblQuoted(p).Parts.exists(q,
						type(q) == syntax.CmdSubst &&
						syntax.CmdSubst(q).Stmts.exists(s, type(s.Cmd) == syntax.CallExpr &&
							syntax.Lit(syntax.CallExpr(s.Cmd).Args[0].Parts[0]).Value == 'redis-cli')))`,
			`printf "$(redis-cli FLUSHALL)"`, `printf '$(redis-cli FLUSHALL)'`,
		},
		{
			"parameter",
			`ast.Stmts.size() == 1 && syntax.CallExpr(ast.Stmts[0].Cmd).Args[1].Parts.exists(p,
				type(p) == syntax.DblQuoted && syntax.DblQuoted(p).Parts.exists(q,
					type(q) == syntax.ParamExp && syntax.ParamExp(q).Param.Value == 'ACTION' &&
					has(syntax.ParamExp(q).Exp) && operator(q) == ':-'))`,
			`printf "${ACTION:-FLUSHALL}"`, `printf "$ACTION"`,
		},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			rule := testRule(item.expression)
			rule.Scope = "ast"
			engine, err := compileBundle(t, rule)
			if err != nil {
				t.Fatal(err)
			}
			for source, status := range map[string]string{item.positive: "matched", item.negative: "abstain"} {
				result, err := engine.Classify(context.Background(), source, Context{})
				if err != nil || result.Status != status {
					t.Fatalf("%q: %+v, %v", source, result, err)
				}
				if status == "matched" && result.Basis != "bash_ast" {
					t.Fatalf("wrong match basis: %+v", result)
				}
			}
		})
	}
}

func TestASTScopePreservesParsedFlagsAndAbsentCommand(t *testing.T) {
	for _, item := range []struct {
		expression, source string
	}{
		{`ast.Stmts[0].Background && !ast.Stmts[0].Negated`, "printf x &"},
		{`ast.Stmts[0].Cmd == null && ast.Stmts[0].Redirs.size() == 1`, ">out"},
		{`!has(ast.Stmts[0].Cmd) && ast.Stmts[0].Redirs[0].Op > 0u`, ">out"},
		{`source == 'printf x' && calls[0].Start == start(ast.Stmts[0].Cmd) &&
			end(ast.Stmts[0].Cmd) == 8 && kind(ast.Stmts[0].Cmd) == 'CallExpr'`, "printf x"},
		{`ast_nodes.exists(n, type(n) == syntax.CmdSubst &&
			syntax.CmdSubst(n).Stmts.size() == 1)`, `printf "$(redis-cli FLUSHALL)"`},
		{`offset(syntax.BinaryCmd(ast.Stmts[0].Cmd).OpPos) == 9`, "printf x | cat"},
		{`ast.Last.size() == 1 && kind(ast.Last[0]) == 'Comment' && start(ast.Last[0]) == 0`, "# note\n"},
	} {
		rule := testRule(item.expression)
		rule.Scope = "ast"
		engine, err := compileBundle(t, rule)
		if err != nil {
			t.Fatal(err)
		}
		result, err := engine.Classify(context.Background(), item.source, Context{})
		if err != nil || result.Status != "matched" {
			t.Fatalf("%q: %+v, %v", item.expression, result, err)
		}
	}
}

func TestASTScopeRejectsWrongCastsAndConstruction(t *testing.T) {
	for _, expression := range []string{
		`syntax.BinaryCmd(ast.Stmts[0].Cmd).Op > 0u`,
		`start(syntax.Word{}) == 0`,
		`start(1) == 0`,
		`ast.Stmts[0].Cmd.Arggs.size() > 0`,
	} {
		rule := testRule(expression)
		rule.Scope = "ast"
		engine, err := compileBundle(t, rule)
		if err != nil {
			t.Fatal(err)
		}
		result, err := engine.Classify(context.Background(), "printf x", Context{})
		if err != nil || result.Status != "abstain" || result.Labels != nil ||
			!strings.HasPrefix(result.Reasons[0], "rule_evaluation_error:") {
			t.Fatalf("%q: %+v, %v", expression, result, err)
		}
	}
}

func TestScopeIsValidatedAndSimpleScopeKeepsItsGate(t *testing.T) {
	rule := testRule("true")
	rule.Scope = "unknown"
	if _, err := compileBundle(t, rule); err == nil {
		t.Fatal("invalid scope accepted")
	}
	rule.Scope = "simple"
	engine, err := compileBundle(t, rule)
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.Classify(context.Background(), "printf x | cat", Context{})
	if err != nil || result.Status != "abstain" {
		t.Fatalf("simple gate bypassed: %+v, %v", result, err)
	}
	rule.Scope = "ast"
	engine, err = compileBundle(t, rule)
	if err != nil {
		t.Fatal(err)
	}
	result, err = engine.Classify(context.Background(), "printf '", Context{})
	if err == nil || result.Status != "abstain" || result.Labels != nil {
		t.Fatalf("invalid syntax evaluated: %+v, %v", result, err)
	}
}
