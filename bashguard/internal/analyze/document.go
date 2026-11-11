package analyze

import "mvdan.cc/sh/v3/syntax"

type Document struct {
	Tree     *syntax.File  `json:"-"`
	ASTNodes []syntax.Node `json:"-"`
	Source   string        `json:"source"`
	Nodes    []Node        `json:"nodes"`
	Calls    []Call        `json:"calls"`
	Eligible bool          `json:"eligible"`
	Reasons  []string      `json:"reasons"`
}

type Node struct {
	ID       int    `json:"id"`
	Parent   int    `json:"parent"`
	Kind     string `json:"kind"`
	Start    int    `json:"start"`
	End      int    `json:"end"`
	Operator string `json:"operator,omitempty"`
}

type Call struct {
	Start int    `json:"start"`
	End   int    `json:"end"`
	Words []Word `json:"words"`
}

type Word struct {
	Start  int    `json:"start"`
	End    int    `json:"end"`
	Raw    string `json:"raw"`
	Value  string `json:"value"`
	Static bool   `json:"static"`
}

type Edit struct {
	Start       int    `json:"start"`
	End         int    `json:"end"`
	Replacement string `json:"replacement"`
}

type Error struct {
	Kind   string
	Detail string
	Cause  error
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return e.Kind + ": " + e.Detail + ": " + e.Cause.Error()
	}
	return e.Kind + ": " + e.Detail
}

func (e *Error) Unwrap() error {
	return e.Cause
}
