package policy

import (
	"context"
	"fmt"
	"slices"

	"codebench/bashguard/internal/analyze"
	"github.com/google/cel-go/common/types"
)

func (e *Engine) Classify(ctx context.Context, source string, facts Context) (Result, error) {
	result := Result{
		Status: "abstain", Basis: "literal_cli_form", BundleSHA256: e.hash,
		RuleIDs: []string{}, Rationales: []string{}, Sources: []string{}, Reasons: []string{},
	}
	if err := facts.Validate(); err != nil {
		return result, err
	}
	if ctx.Err() != nil {
		result.Reasons = []string{"canceled"}
		return result, nil
	}
	document, err := analyze.Analyze(source)
	if err != nil {
		result.Reasons = []string{"parse_error"}
		return result, err
	}
	argv := []string{}
	if document.Eligible {
		for _, word := range document.Calls[0].Words {
			argv = append(argv, word.Value)
		}
	}
	navigation := newASTNavigation(document)
	var matches []Rule
	for _, compiled := range e.rules {
		if compiled.rule.Scope != "ast" && !document.Eligible {
			continue
		}
		if ctx.Err() != nil {
			result.Reasons = []string{"canceled"}
			return result, nil
		}
		value, _, err := compiled.program.ContextEval(ctx, map[string]any{
			navigationVariable: navigation,
			"ast":              document.Tree,
			"source":           source,
			"calls":            document.Calls,
			"ast_nodes":        document.ASTNodes,
			"argv":             argv,
			"nodes":            document.Nodes,
			"context":          facts,
		})
		if err != nil || types.IsError(value) || types.IsUnknown(value) {
			result.Reasons = []string{"rule_evaluation_error:" + compiled.rule.ID}
			return result, nil
		}
		if value == types.True {
			matches = append(matches, compiled.rule)
		} else if value != types.False {
			return result, fmt.Errorf("%w: nonboolean runtime result", ErrRules)
		}
	}
	if len(matches) == 0 {
		result.Reasons = []string{"no_proven_rule"}
		if !document.Eligible {
			result.Reasons = append(result.Reasons, document.Reasons...)
		}
		return result, nil
	}
	labels := matches[0].Labels
	for _, rule := range matches {
		if rule.Scope == "ast" {
			result.Basis = "bash_ast"
		}
		if rule.Labels != labels {
			result.Reasons = []string{"conflicting_rules"}
			return result, nil
		}
	}
	result.Status = "matched"
	result.Labels = &labels
	for _, rule := range matches {
		result.RuleIDs = append(result.RuleIDs, rule.ID)
		result.Rationales = append(result.Rationales, rule.Rationale)
		for _, source := range rule.Sources {
			if !slices.Contains(result.Sources, source) {
				result.Sources = append(result.Sources, source)
			}
		}
	}
	return result, nil
}
