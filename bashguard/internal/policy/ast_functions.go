package policy

import (
	"reflect"

	"codebench/bashguard/internal/analyze"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"mvdan.cc/sh/v3/syntax"
)

func astFunctions() []cel.EnvOption {
	options := []cel.EnvOption{
		cel.Function("offset", cel.Overload("bash_pos_offset",
			[]*cel.Type{cel.ObjectType("syntax.Pos")}, cel.IntType,
			cel.UnaryBinding(func(value ref.Val) ref.Val {
				position, ok := value.Value().(syntax.Pos)
				if !ok || !position.IsValid() {
					return types.NewErr("offset requires a valid source position")
				}
				return types.Int(position.Offset())
			}))),
	}
	for _, name := range []string{"kind", "operator", "start", "end"} {
		resultType := cel.StringType
		if name == "start" || name == "end" {
			resultType = cel.IntType
		}
		options = append(options, cel.Function(name,
			cel.Overload("bash_ast_"+name, []*cel.Type{cel.DynType}, resultType,
				cel.UnaryBinding(func(value ref.Val) ref.Val {
					node, ok := value.Value().(syntax.Node)
					if comment, isComment := value.Value().(syntax.Comment); isComment {
						node, ok = &comment, true
					}
					if !ok || reflect.ValueOf(node).IsNil() {
						return types.NewErr("%s requires a parsed Bash node", name)
					}
					switch name {
					case "kind":
						return types.String(reflect.TypeOf(node).Elem().Name())
					case "operator":
						return types.String(analyze.Operator(node))
					case "start":
						if node.Pos().IsValid() {
							return types.Int(node.Pos().Offset())
						}
					case "end":
						if node.End().IsValid() {
							return types.Int(node.End().Offset())
						}
					}
					return types.NewErr("%s requires a valid source position", name)
				}))))
	}
	return options
}
