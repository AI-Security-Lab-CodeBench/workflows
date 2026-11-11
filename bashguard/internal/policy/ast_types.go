package policy

import (
	"reflect"

	"mvdan.cc/sh/v3/syntax"
)

func syntaxTypes() map[string]reflect.Type {
	roots := []reflect.Type{
		reflect.TypeFor[syntax.File](), reflect.TypeFor[syntax.Comment](),
		reflect.TypeFor[syntax.Stmt](), reflect.TypeFor[syntax.Assign](),
		reflect.TypeFor[syntax.Redirect](), reflect.TypeFor[syntax.CallExpr](),
		reflect.TypeFor[syntax.Subshell](), reflect.TypeFor[syntax.Block](),
		reflect.TypeFor[syntax.IfClause](), reflect.TypeFor[syntax.WhileClause](),
		reflect.TypeFor[syntax.ForClause](), reflect.TypeFor[syntax.WordIter](),
		reflect.TypeFor[syntax.CStyleLoop](), reflect.TypeFor[syntax.BinaryCmd](),
		reflect.TypeFor[syntax.FuncDecl](), reflect.TypeFor[syntax.Word](),
		reflect.TypeFor[syntax.Lit](), reflect.TypeFor[syntax.SglQuoted](),
		reflect.TypeFor[syntax.DblQuoted](), reflect.TypeFor[syntax.CmdSubst](),
		reflect.TypeFor[syntax.ParamExp](), reflect.TypeFor[syntax.ArithmExp](),
		reflect.TypeFor[syntax.ArithmCmd](), reflect.TypeFor[syntax.BinaryArithm](),
		reflect.TypeFor[syntax.UnaryArithm](), reflect.TypeFor[syntax.ParenArithm](),
		reflect.TypeFor[syntax.FlagsArithm](), reflect.TypeFor[syntax.CaseClause](),
		reflect.TypeFor[syntax.CaseItem](), reflect.TypeFor[syntax.TestClause](),
		reflect.TypeFor[syntax.BinaryTest](), reflect.TypeFor[syntax.UnaryTest](),
		reflect.TypeFor[syntax.ParenTest](), reflect.TypeFor[syntax.DeclClause](),
		reflect.TypeFor[syntax.ArrayExpr](), reflect.TypeFor[syntax.ArrayElem](),
		reflect.TypeFor[syntax.ExtGlob](), reflect.TypeFor[syntax.ProcSubst](),
		reflect.TypeFor[syntax.TimeClause](), reflect.TypeFor[syntax.CoprocClause](),
		reflect.TypeFor[syntax.LetClause](), reflect.TypeFor[syntax.BraceExp](),
		reflect.TypeFor[syntax.TestDecl](),
	}
	result := make(map[string]reflect.Type)
	var visit func(reflect.Type)
	visit = func(t reflect.Type) {
		switch t.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array:
			visit(t.Elem())
		case reflect.Struct:
			name := "syntax." + t.Name()
			if _, exists := result[name]; exists {
				return
			}
			result[name] = t
			for field := range t.Fields() {
				if field.IsExported() {
					visit(field.Type)
				}
			}
		}
	}
	for _, root := range roots {
		visit(root)
	}
	return result
}
