package policy

import (
	"codebench/bashguard/internal/analyze"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"mvdan.cc/sh/v3/syntax"
)

const navigationVariable = "__ast_navigation"

type astNavigation struct {
	nodes      []syntax.Node
	metadata   []analyze.Node
	children   [][]syntax.Node
	nodeIDs    map[syntax.Node]int
	commentIDs map[syntax.Comment]int
}

func newASTNavigation(document analyze.Document) *astNavigation {
	index := &astNavigation{
		nodes:      document.ASTNodes,
		metadata:   document.Nodes,
		children:   make([][]syntax.Node, len(document.ASTNodes)),
		nodeIDs:    make(map[syntax.Node]int, len(document.ASTNodes)),
		commentIDs: make(map[syntax.Comment]int),
	}
	for id, node := range document.ASTNodes {
		index.nodeIDs[node] = id
		if comment, ok := node.(*syntax.Comment); ok {
			index.commentIDs[*comment] = id
		}
		if parent := document.Nodes[id].Parent; parent >= 0 {
			index.children[parent] = append(index.children[parent], node)
		}
	}
	return index
}

func (index *astNavigation) nativeNodeID(value any) (int, bool) {
	switch node := value.(type) {
	case syntax.Node:
		id, found := index.nodeIDs[node]
		return id, found
	case syntax.Comment:
		id, found := index.commentIDs[node]
		return id, found
	default:
		return 0, false
	}
}

func astNavigationFunctions(env *cel.Env) (*cel.Env, error) {
	adapter := env.CELTypeAdapter()
	options := []cel.EnvOption{cel.Variable(navigationVariable, cel.DynType)}
	for _, method := range []string{"parent", "children"} {
		function := "__ast_" + method
		resultType := cel.DynType
		if method == "children" {
			resultType = cel.ListType(cel.DynType)
		}
		options = append(options,
			cel.Macros(cel.ReceiverMacro(method, 0,
				func(factory cel.MacroExprFactory, target ast.Expr, _ []ast.Expr) (ast.Expr, *cel.Error) {
					return factory.NewCall(function, target, factory.NewIdent(navigationVariable)), nil
				})),
			cel.Function(function, cel.Overload(function+"_node_index",
				[]*cel.Type{cel.DynType, cel.DynType}, resultType,
				cel.BinaryBinding(func(value, navigation ref.Val) ref.Val {
					index, ok := navigation.Value().(*astNavigation)
					if !ok {
						return types.NewErr("AST navigation context is unavailable")
					}
					id, found := index.nativeNodeID(value.Value())
					if !found {
						return types.NewErr("%s requires a node from the current AST", method)
					}
					if method == "children" {
						return types.NewDynamicList(adapter, index.children[id])
					}
					parent := index.metadata[id].Parent
					if parent < 0 {
						return types.NullValue
					}
					return adapter.NativeToValue(index.nodes[parent])
				}))))
	}
	return env.Extend(options...)
}
