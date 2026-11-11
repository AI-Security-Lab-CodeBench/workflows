package analyze

import (
	"reflect"
	"strings"
	"unicode/utf8"

	"mvdan.cc/sh/v3/syntax"
)

const MaxSourceBytes = 64 * 1024
const MaxASTDepth = 256
const MaxASTNodes = 32768

func Analyze(source string) (Document, error) {
	doc := Document{Source: source, Nodes: []Node{}, Calls: []Call{}, Reasons: []string{}}
	switch {
	case len(source) > MaxSourceBytes:
		return doc, &Error{Kind: "size", Detail: "source exceeds 64 KiB"}
	case !utf8.ValidString(source):
		return doc, &Error{Kind: "utf8", Detail: "source is not valid UTF-8"}
	case strings.IndexByte(source, 0) >= 0:
		return doc, &Error{Kind: "nul", Detail: "source contains NUL"}
	}
	parser := syntax.NewParser(syntax.Variant(syntax.LangBash), syntax.KeepComments(true))
	file, err := parser.Parse(strings.NewReader(source), "")
	if err != nil {
		return doc, &Error{Kind: "parse", Detail: "invalid Bash syntax", Cause: err}
	}
	stack := []int{}
	commentParents := make(map[uint]int)
	astNodes := []syntax.Node{}
	var traversalErr error
	syntax.Walk(file, func(n syntax.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		if traversalErr != nil {
			return false
		}
		if len(stack) >= MaxASTDepth || len(doc.Nodes) >= MaxASTNodes {
			traversalErr = &Error{Kind: "complexity", Detail: "AST depth or node limit exceeded"}
			return false
		}
		parent := -1
		if len(stack) > 0 {
			parent = stack[len(stack)-1]
		}
		if comment, ok := n.(*syntax.Comment); ok {
			if owner, exists := commentParents[comment.Pos().Offset()]; exists {
				parent = owner
			}
		}
		id := len(doc.Nodes)
		doc.Nodes = append(doc.Nodes, Node{
			ID: id, Parent: parent, Kind: reflect.TypeOf(n).Elem().Name(), Operator: Operator(n),
		})
		astNodes = append(astNodes, n)
		stack = append(stack, id)
		if stmt, ok := n.(*syntax.Stmt); ok {
			for _, comment := range stmt.Comments {
				commentParents[comment.Pos().Offset()] = id
			}
		}
		return true
	})
	if traversalErr != nil {
		return doc, traversalErr
	}
	doc.Tree = file
	doc.ASTNodes = astNodes
	for i, n := range astNodes {
		start, end := int(n.Pos().Offset()), int(n.End().Offset())
		doc.Nodes[i].Start, doc.Nodes[i].End = start, end
		if call, ok := n.(*syntax.CallExpr); ok {
			record := Call{Start: start, End: end, Words: []Word{}}
			for _, word := range call.Args {
				from, to := int(word.Pos().Offset()), int(word.End().Offset())
				raw := source[from:to]
				value, static := staticWord(word, raw)
				record.Words = append(record.Words, Word{
					Start: from, End: to, Raw: raw, Value: value, Static: static,
				})
			}
			doc.Calls = append(doc.Calls, record)
		}
	}
	if len(file.Stmts) != 1 {
		doc.Reasons = append(doc.Reasons, "requires_one_top_level_statement")
		return doc, nil
	}
	stmt := file.Stmts[0]
	call, ok := stmt.Cmd.(*syntax.CallExpr)
	if !ok {
		doc.Reasons = append(doc.Reasons, "requires_ordinary_simple_command")
		return doc, nil
	}
	if stmt.Background || stmt.Negated || stmt.Coprocess || stmt.Disown {
		doc.Reasons = append(doc.Reasons, "requires_foreground_unmodified_statement")
	}
	if len(stmt.Redirs) > 0 {
		doc.Reasons = append(doc.Reasons, "redirections_not_supported")
	}
	if len(call.Assigns) > 0 {
		doc.Reasons = append(doc.Reasons, "assignments_not_supported")
	}
	if len(call.Args) == 0 {
		doc.Reasons = append(doc.Reasons, "requires_command_word")
	}
	for _, record := range doc.Calls {
		for _, word := range record.Words {
			if !word.Static {
				doc.Reasons = append(doc.Reasons, "nonliteral_word")
				doc.Eligible = false
				return doc, nil
			}
		}
	}
	doc.Eligible = len(doc.Reasons) == 0
	return doc, nil
}
