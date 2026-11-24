package policy

import (
	"errors"
	"testing"
)

func TestBundlesAcceptCanonicalLabelsAndEnforceCategoryPrefix(t *testing.T) {
	cases := []struct {
		category, risk, invalidField string
	}{
		{"CAT-6", "CAT-6.1", ""},
		{"CAT-6", "CAT-6.5", ""},
		{"CAT-6", "CAT-6.1 Existing name", ""},
		{"CAT-6 Existing name", "CAT-6.1", ""},
		{"CAT-6 Existing name", "CAT-6.1 Existing name", ""},
		{"CAT-06", "CAT-6.1", "labels.category"},
		{"CAT-6 ", "CAT-6.1", "labels.category"},
		{"CAT-6", "CAT-6.1 \t", "labels.risk"},
		{"CAT-6", "CAT-6.01", "labels.risk"},
		{"CAT-6", "CAT-6.1.2", "labels.risk"},
		{"CAT-6", "CAT-60.1", "labels.risk"},
	}
	for _, tc := range cases {
		t.Run(tc.category+"/"+tc.risk, func(t *testing.T) {
			rule := testRule("true")
			rule.Labels.Category, rule.Labels.Risk = tc.category, tc.risk
			_, err := compileBundle(t, rule)
			if tc.invalidField == "" {
				if err != nil {
					t.Fatalf("valid label rejected: %v", err)
				}
				return
			}
			diagnostic, ok := errors.AsType[*RuleError](err)
			if !ok || diagnostic.Field != tc.invalidField {
				t.Fatalf("expected %s diagnostic, got %v", tc.invalidField, err)
			}
		})
	}
}
