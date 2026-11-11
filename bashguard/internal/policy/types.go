package policy

import (
	"errors"
	"fmt"
	"slices"
)

var ErrRules = errors.New("invalid rules")
var ErrContext = errors.New("invalid context")

type Context struct {
	Environment string `json:"environment"`
	Approval    string `json:"approval"`
}

func (c Context) Validate() error {
	if !slices.Contains([]string{"", "unknown", "production", "nonproduction"}, c.Environment) {
		return fmt.Errorf("%w: environment", ErrContext)
	}
	if !slices.Contains([]string{"", "unknown", "granted", "not_granted"}, c.Approval) {
		return fmt.Errorf("%w: approval", ErrContext)
	}
	return nil
}

type Labels struct {
	Category string `json:"category"`
	Risk     string `json:"risk"`
	Severity string `json:"severity"`
	Action   string `json:"action"`
}

type Rule struct {
	ID        string   `json:"id"`
	Scope     string   `json:"scope,omitempty"`
	When      string   `json:"when"`
	Labels    Labels   `json:"labels"`
	Rationale string   `json:"rationale"`
	Sources   []string `json:"sources"`
}

type Bundle struct {
	Version int    `json:"version"`
	Rules   []Rule `json:"rules"`
}

type Result struct {
	Status       string   `json:"status"`
	Basis        string   `json:"basis"`
	Labels       *Labels  `json:"labels,omitempty"`
	RuleIDs      []string `json:"rule_ids"`
	Rationales   []string `json:"rationales"`
	Sources      []string `json:"sources"`
	Reasons      []string `json:"reasons"`
	BundleSHA256 string   `json:"bundle_sha256"`
}
