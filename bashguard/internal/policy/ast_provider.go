package policy

import (
	"reflect"
	"slices"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/ext"
)

type astProvider struct {
	types.Provider
	structs map[string]reflect.Type
}

func nativeAST(env *cel.Env) (*cel.Env, error) {
	structs := syntaxTypes()
	names := make([]string, 0, len(structs))
	for name := range structs {
		names = append(names, name)
	}
	slices.Sort(names)
	registered := make([]any, 0, len(names))
	for _, name := range names {
		registered = append(registered, structs[name])
	}
	native, err := ext.NativeTypes(registered...)(env)
	if err != nil {
		return nil, err
	}
	native, err = cel.CustomTypeProvider(&astProvider{native.CELTypeProvider(), structs})(native)
	if err != nil {
		return nil, err
	}
	casts := make([]cel.EnvOption, 0, len(names))
	for _, name := range names {
		casts = append(casts, cel.Function(name,
			cel.Overload("cast_"+name, []*cel.Type{cel.DynType}, cel.ObjectType(name),
				cel.UnaryBinding(func(value ref.Val) ref.Val {
					if value.Type().TypeName() != name {
						return types.NewErr("expected %s, got %s", name, value.Type().TypeName())
					}
					return value
				}))))
	}
	return native.Extend(casts...)
}

func (p *astProvider) FindStructFieldType(name, field string) (*types.FieldType, bool) {
	if result, found := p.Provider.FindStructFieldType(name, field); found {
		return result, true
	}
	t, exists := p.structs[name]
	if !exists {
		return nil, false
	}
	f, exists := t.FieldByName(field)
	if !exists || !f.IsExported() {
		return nil, false
	}
	fieldType := interfaceFieldType(f.Type)
	if fieldType == nil {
		return nil, false
	}
	return &types.FieldType{
		Type: fieldType,
		IsSet: func(value any) bool {
			return !reflect.Indirect(reflect.ValueOf(value)).FieldByIndex(f.Index).IsZero()
		},
		GetFrom: func(value any) (any, error) {
			return reflect.Indirect(reflect.ValueOf(value)).FieldByIndex(f.Index).Interface(), nil
		},
	}, true
}

func interfaceFieldType(t reflect.Type) *types.Type {
	switch t.Kind() {
	case reflect.Interface:
		return cel.DynType
	case reflect.Slice, reflect.Array:
		if element := interfaceFieldType(t.Elem()); element != nil {
			return cel.ListType(element)
		}
	}
	return nil
}

func (p *astProvider) NewValue(name string, fields map[string]ref.Val) ref.Val {
	if _, exists := p.structs[name]; exists {
		return types.NewErr("Bash AST values are read-only parser inputs")
	}
	return p.Provider.NewValue(name, fields)
}
