package policy

import "fmt"

type RuleError struct {
	RuleID string
	Field  string
	Detail string
	Code   string
}

func (e *RuleError) Error() string {
	return fmt.Sprintf("%s: %s %s: %s", ErrRules, e.RuleID, e.Field, e.Detail)
}

func (e *RuleError) Unwrap() error {
	return ErrRules
}

type RuntimeError struct {
	RuleID string
	Detail string
}

func (e *RuntimeError) Error() string {
	return e.Detail
}
