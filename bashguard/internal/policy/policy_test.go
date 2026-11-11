package policy

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func seedEngine(t *testing.T) *Engine {
	t.Helper()
	data, err := os.ReadFile("../../rules/cat3.json")
	if err != nil {
		t.Fatal(err)
	}
	engine, err := Compile(data)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func TestSeedRulesRequireCompleteLiteralIntent(t *testing.T) {
	engine := seedEngine(t)
	cases := []struct {
		source string
		facts  Context
		risk   string
		action string
	}{
		{"terraform destroy -auto-approve", Context{Environment: "nonproduction"}, "CAT-3.1", "ask"},
		{"terraform destroy -auto-approve", Context{Environment: "production"}, "CAT-3.1", "deny"},
		{"kubectl delete ns example", Context{Environment: "production"}, "CAT-3.2", "ask"},
		{"docker volume prune --force", Context{}, "CAT-3.3", "ask"},
		{"redis-cli FLUSHALL SYNC", Context{}, "CAT-3.4", "ask"},
		{"vercel deploy --yes --prod", Context{Approval: "not_granted"}, "CAT-3.5", "ask"},
		{"red'is'-cli 'FLUSHALL'", Context{}, "CAT-3.4", "ask"},
		{"redis\\-cli FLUSHALL # literal comment", Context{}, "CAT-3.4", "ask"},
	}
	for _, item := range cases {
		t.Run(item.source+item.facts.Environment, func(t *testing.T) {
			result, err := engine.Classify(context.Background(), item.source, item.facts)
			if err != nil {
				t.Fatal(err)
			}
			if result.Status != "matched" || result.Labels == nil {
				t.Fatalf("expected match: %+v", result)
			}
			if !strings.HasPrefix(result.Labels.Risk, item.risk+" ") || result.Labels.Action != item.action {
				t.Fatalf("wrong labels: %+v", result.Labels)
			}
			if len(result.RuleIDs) != 1 || len(result.BundleSHA256) != 64 {
				t.Fatalf("missing provenance: %+v", result)
			}
		})
	}
}

func TestAdversarialCommandsNeverReceiveLabels(t *testing.T) {
	engine := seedEngine(t)
	cases := []string{
		"terraform destroy -auto-approve=false",
		"kubectl delete ns example --dry-run=client",
		"kubectl delete ns --help",
		"docker volume prune --force --filter label=keep",
		"redis-cli FLUSHALL --help",
		"redis-cli --help FLUSHALL",
		"redis-cli -n 1 FLUSHALL",
		"redis-cli INFO keyspace",
		"printf '%s' 'redis-cli FLUSHALL'",
		"# redis-cli FLUSHALL",
		"false && redis-cli FLUSHALL",
		"redis-cli FLUSHALL || true",
		"redis-cli FLUSHALL; printf done",
		"redis-cli FLUSHALL &",
		"! redis-cli FLUSHALL",
		"redis-cli FLUSHALL >out",
		"REDISCLI_AUTH=example redis-cli FLUSHALL",
		"redis-cli \"$ACTION\"",
		"redis-cli ${ACTION:-FLUSHALL}",
		"redis-cli $(printf FLUSHALL)",
		"redis-cli `printf FLUSHALL`",
		"redis-cli FLUSH*",
		"redis-cli {FLUSHALL,INFO}",
		"redis-cli $'FLUSHALL'",
		"redis-cli $\"FLUSHALL\"",
		"redis-cli() { :; }; redis-cli FLUSHALL",
		"alias redis-cli=echo; redis-cli FLUSHALL",
		"bash -c 'redis-cli FLUSHALL'",
		"eval 'redis-cli FLUSHALL'",
		"sudo redis-cli FLUSHALL",
		"/usr/bin/redis-cli FLUSHALL",
		"if false; then redis-cli FLUSHALL; fi",
		"cat <<'EOF'\nredis-cli FLUSHALL\nEOF\n",
		"redis-cli FLUSHALL\x00",
		"redis-cli 'FLUSHALL",
		"redis-cli FLUSHALL |",
	}
	for _, source := range cases {
		t.Run(source, func(t *testing.T) {
			result, _ := engine.Classify(context.Background(), source, Context{
				Environment: "production", Approval: "not_granted",
			})
			if result.Status != "abstain" || result.Labels != nil {
				t.Fatalf("false match: %+v", result)
			}
		})
	}
}

func TestMissingContextNeverDefaultsToKnownContext(t *testing.T) {
	engine := seedEngine(t)
	for _, source := range []string{"terraform destroy -auto-approve", "vercel deploy --prod --yes", "kubectl delete ns example"} {
		result, err := engine.Classify(context.Background(), source, Context{})
		if err != nil || result.Status != "abstain" || result.Labels != nil {
			t.Fatalf("unknown context classified: %+v, %v", result, err)
		}
	}
}

func testRule(expression string) Rule {
	return Rule{
		ID: "test", When: expression,
		Labels:    Labels{"CAT-3 Деструкция облачной инфраструктуры и БД", "CAT-3.4 Cloud Storage & Database Drop", "critical", "ask"},
		Rationale: "Test policy fixture", Sources: []string{"https://redis.io/docs/latest/commands/flushall/"},
	}
}

func compileBundle(t *testing.T, rules ...Rule) (*Engine, error) {
	t.Helper()
	data, err := json.Marshal(Bundle{Version: 1, Rules: rules})
	if err != nil {
		t.Fatal(err)
	}
	return Compile(data)
}

func TestMalformedRulesFailBeforeEvaluation(t *testing.T) {
	for _, expression := range []string{"1", "1 + 'x'", "argv.missing", "nodes.exists(n, n.Unknown == 'x')", "unknown == true"} {
		t.Run(expression, func(t *testing.T) {
			if _, err := compileBundle(t, testRule(expression)); err == nil {
				t.Fatal("invalid rule compiled")
			}
		})
	}
	duplicate := testRule("true")
	if _, err := compileBundle(t, duplicate, duplicate); err == nil {
		t.Fatal("duplicate rule id compiled")
	}
	duplicate.Labels.Action = "modify"
	if _, err := compileBundle(t, duplicate); err == nil {
		t.Fatal("invalid v0 action compiled")
	}
}

func TestEvaluationFailureAndConflictDiscardAllLabels(t *testing.T) {
	for _, expression := range []string{"argv[999] == 'x'", "argv.all(x, argv.all(y, x == y || x != y))"} {
		t.Run(expression, func(t *testing.T) {
			engine, err := compileBundle(t, testRule(expression))
			if err != nil {
				t.Fatal(err)
			}
			result, err := engine.Classify(context.Background(), "printf "+strings.Repeat("x ", 250), Context{})
			if err != nil || result.Status != "abstain" || result.Labels != nil ||
				len(result.Reasons) != 1 || !strings.HasPrefix(result.Reasons[0], "rule_evaluation_error:") {
				t.Fatalf("expected fail-closed evaluation: %+v, %v", result, err)
			}
		})
	}
	first, second := testRule("true"), testRule("true")
	second.ID, second.Labels.Action = "other", "deny"
	engine, err := compileBundle(t, first, second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.Classify(context.Background(), "redis-cli FLUSHALL", Context{})
	if err != nil || result.Labels != nil || len(result.Reasons) != 1 || result.Reasons[0] != "conflicting_rules" {
		t.Fatalf("expected conflict abstention: %+v, %v", result, err)
	}
}

func TestTypedNodesAndCancellation(t *testing.T) {
	engine, err := compileBundle(t, testRule("nodes.exists(n, n.Kind == 'CallExpr' && n.Parent >= 0) && argv[0] == 'printf'"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.Classify(context.Background(), "printf x", Context{})
	if err != nil || result.Status != "matched" {
		t.Fatalf("typed AST rule failed: %+v, %v", result, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err = engine.Classify(ctx, "printf x", Context{})
	if err != nil || result.Status != "abstain" || result.Labels != nil || result.Reasons[0] != "canceled" {
		t.Fatalf("canceled request classified: %+v, %v", result, err)
	}
}
