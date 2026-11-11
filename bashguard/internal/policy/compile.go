package policy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"reflect"
	"regexp"
	"slices"
	"strings"

	"codebench/bashguard/internal/analyze"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"
)

type compiledRule struct {
	rule    Rule
	program cel.Program
}

type Engine struct {
	rules []compiledRule
	hash  string
}

func Compile(data []byte) (*Engine, error) {
	if len(data) > 1<<20 {
		return nil, fmt.Errorf("%w: bundle exceeds 1 MiB", ErrRules)
	}
	var bundle Bundle
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&bundle); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRules, err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("%w: trailing JSON", ErrRules)
	}
	if bundle.Version != 1 || len(bundle.Rules) == 0 || len(bundle.Rules) > 128 {
		return nil, fmt.Errorf("%w: version must be 1 with 1..128 rules", ErrRules)
	}
	env, err := cel.NewEnv(
		cel.ParserExpressionSizeLimit(8192),
		cel.ParserRecursionLimit(64),
		ext.Bindings(),
		ext.NativeTypes(reflect.TypeOf(analyze.Node{}), reflect.TypeOf(analyze.Call{}), reflect.TypeOf(Context{})),
		nativeAST,
		astNavigationFunctions,
		cel.Variable("ast", cel.ObjectType("syntax.File")),
		cel.Variable("ast_nodes", cel.ListType(cel.DynType)),
		cel.Variable("source", cel.StringType),
		cel.Variable("calls", cel.ListType(cel.ObjectType("analyze.Call"))),
		cel.Variable("argv", cel.ListType(cel.StringType)),
		cel.Variable("nodes", cel.ListType(cel.ObjectType("analyze.Node"))),
		cel.Variable("context", cel.ObjectType("policy.Context")),
	)
	if err != nil {
		return nil, fmt.Errorf("%w: environment: %v", ErrRules, err)
	}
	env, err = env.Extend(astFunctions()...)
	if err != nil {
		return nil, fmt.Errorf("%w: AST functions: %v", ErrRules, err)
	}
	digest := sha256.Sum256(data)
	engine := &Engine{hash: hex.EncodeToString(digest[:])}
	seen := make(map[string]bool)
	for _, rule := range bundle.Rules {
		if err := validateRule(rule, seen); err != nil {
			return nil, err
		}
		seen[rule.ID] = true
		ast, issues := env.Compile(rule.When)
		if issues.Err() != nil {
			return nil, fmt.Errorf("%w: %s: %v", ErrRules, rule.ID, issues.Err())
		}
		if !ast.OutputType().IsExactType(cel.BoolType) {
			return nil, fmt.Errorf("%w: %s must return bool", ErrRules, rule.ID)
		}
		program, err := env.Program(ast, cel.CostLimit(10000), cel.InterruptCheckFrequency(32))
		if err != nil {
			return nil, fmt.Errorf("%w: %s: %v", ErrRules, rule.ID, err)
		}
		engine.rules = append(engine.rules, compiledRule{rule, program})
	}
	return engine, nil
}

func validateRule(rule Rule, seen map[string]bool) error {
	category := regexp.MustCompile(`^CAT-[1-9][0-9]* .+$`)
	risk := regexp.MustCompile(`^CAT-[1-9][0-9]*\.[1-9][0-9]* .+$`)
	if !slices.Contains([]string{"", "simple", "ast"}, rule.Scope) {
		return fmt.Errorf("%w: %s scope must be simple or ast", ErrRules, rule.ID)
	}
	if rule.ID == "" || seen[rule.ID] || len(rule.When) == 0 || len(rule.When) > 8192 {
		return fmt.Errorf("%w: missing/duplicate id or expression size", ErrRules)
	}
	categoryCode := strings.SplitN(rule.Labels.Category, " ", 2)[0]
	if !category.MatchString(rule.Labels.Category) || !risk.MatchString(rule.Labels.Risk) ||
		!strings.HasPrefix(rule.Labels.Risk, categoryCode+".") {
		return fmt.Errorf("%w: %s category/risk", ErrRules, rule.ID)
	}
	if !slices.Contains([]string{"critical", "high", "medium", "low"}, rule.Labels.Severity) ||
		!slices.Contains([]string{"allow", "deny", "ask"}, rule.Labels.Action) {
		return fmt.Errorf("%w: %s severity/action", ErrRules, rule.ID)
	}
	if strings.TrimSpace(rule.Rationale) == "" || len(rule.Sources) == 0 {
		return fmt.Errorf("%w: %s needs rationale and sources", ErrRules, rule.ID)
	}
	for _, source := range rule.Sources {
		u, err := url.Parse(source)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
			return fmt.Errorf("%w: %s invalid source URL", ErrRules, rule.ID)
		}
	}
	return nil
}
