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

func (e *Engine) BundleSHA256() string {
	return e.hash
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
			return nil, &RuleError{RuleID: rule.ID, Field: "when", Detail: issues.Err().Error()}
		}
		if !ast.OutputType().IsExactType(cel.BoolType) {
			return nil, &RuleError{RuleID: rule.ID, Field: "when", Detail: "must return bool"}
		}
		if rule.CaptureGuarded {
			if err := validateCaptureExpression(rule, ast); err != nil {
				return nil, err
			}
		}
		program, err := env.Program(ast, cel.CostLimit(10000), cel.InterruptCheckFrequency(32))
		if err != nil {
			return nil, &RuleError{RuleID: rule.ID, Field: "when", Detail: err.Error()}
		}
		engine.rules = append(engine.rules, compiledRule{rule, program})
	}
	return engine, nil
}

var (
	categoryCodePattern   = regexp.MustCompile(`^CAT-[1-9][0-9]*$`)
	riskCodePattern       = regexp.MustCompile(`^CAT-[1-9][0-9]*\.[1-9][0-9]*$`)
	captureProgramPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)
)

func validateRule(rule Rule, seen map[string]bool) error {
	if !slices.Contains([]string{"", "simple", "ast"}, rule.Scope) {
		return &RuleError{RuleID: rule.ID, Field: "scope", Detail: "must be simple or ast"}
	}
	if len(rule.CaptureAllowShapes) > 0 {
		if rule.Scope == "ast" || rule.Labels.Action != "allow" || len(rule.CaptureAllowShapes) > 16 {
			return &RuleError{RuleID: rule.ID, Field: "capture_allow_shapes", Detail: "requires a simple allow and at most 16 positive shapes"}
		}
		for _, shape := range rule.CaptureAllowShapes {
			if len(shape) == 0 || len(shape) > 32 || !captureProgramPattern.MatchString(shape[0]) {
				return &RuleError{RuleID: rule.ID, Field: "capture_allow_shapes", Detail: "expected a bare program name and at most 31 argument roles"}
			}
		}
	}
	if rule.ID == "" || seen[rule.ID] {
		return &RuleError{RuleID: rule.ID, Field: "id", Detail: "missing or duplicate rule ID"}
	}
	if len(rule.When) == 0 || len(rule.When) > 8192 {
		return &RuleError{RuleID: rule.ID, Field: "when", Detail: "expression length must be 1..8192 bytes"}
	}
	categoryCode, categoryName, categoryHasName := strings.Cut(rule.Labels.Category, " ")
	if !categoryCodePattern.MatchString(categoryCode) ||
		(categoryHasName && (strings.TrimSpace(categoryName) == "" || strings.ContainsRune(categoryName, '\n'))) {
		return &RuleError{RuleID: rule.ID, Field: "labels.category", Detail: "expected a canonical category code with an optional display name"}
	}
	riskCode, riskName, riskHasName := strings.Cut(rule.Labels.Risk, " ")
	if !riskCodePattern.MatchString(riskCode) || !strings.HasPrefix(riskCode, categoryCode+".") ||
		(riskHasName && (strings.TrimSpace(riskName) == "" || strings.ContainsRune(riskName, '\n'))) {
		return &RuleError{RuleID: rule.ID, Field: "labels.risk", Detail: "expected a canonical risk code under the category with an optional display name"}
	}
	if !slices.Contains([]string{"critical", "high", "medium", "low"}, rule.Labels.Severity) {
		return &RuleError{RuleID: rule.ID, Field: "labels.severity", Detail: "must be critical, high, medium, or low"}
	}
	if !slices.Contains([]string{"allow", "deny", "ask"}, rule.Labels.Action) {
		return &RuleError{RuleID: rule.ID, Field: "labels.action", Detail: "must be allow, deny, or ask"}
	}
	if strings.TrimSpace(rule.Rationale) == "" {
		return &RuleError{RuleID: rule.ID, Field: "rationale", Detail: "must not be blank"}
	}
	if len(rule.Sources) == 0 {
		return &RuleError{RuleID: rule.ID, Field: "sources", Detail: "at least one source is required"}
	}
	for _, source := range rule.Sources {
		u, err := url.Parse(source)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
			return &RuleError{RuleID: rule.ID, Field: "sources", Detail: "source must be an HTTPS URL without credentials"}
		}
	}
	return nil
}
