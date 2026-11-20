"""Index Python bindings without importing or executing the asset package."""

import ast
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

type Function = ast.FunctionDef | ast.AsyncFunctionDef


def expressions(node: ast.AST) -> Iterator[ast.AST]:
    for child in ast.iter_child_nodes(node):
        yield child
        if not isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            yield from expressions(child)


@dataclass(eq=False, slots=True)
class Scope:
    name: str
    path: Path
    node: ast.Module | ast.ClassDef | Function
    parent: "Scope | None" = None
    names: dict[str, set[str]] = field(default_factory=dict)
    assignments: dict[str, list[ast.expr]] = field(default_factory=dict)
    with_values: set[ast.expr] = field(default_factory=set)
    comprehension_sources: dict[ast.Name, ast.expr] = field(default_factory=dict)
    path_unpack: dict[str, tuple[ast.Attribute, bool]] = field(default_factory=dict)

    @property
    def module(self) -> str:
        return self.parent.module if self.parent is not None else self.name

    def bind(self, name: str, symbol: str) -> None:
        self.names.setdefault(name, set()).add(symbol)

    def binding_scope(self, name: str) -> "Scope":
        owner = self
        while name not in owner.names and name not in owner.assignments and owner.parent:
            parent = owner.parent
            owner = parent.parent if isinstance(parent.node, ast.ClassDef) and parent.parent else parent
        return owner


class Index:
    def __init__(self, package: Path) -> None:
        self.scopes: dict[str, Scope] = {}
        self.lambdas: dict[ast.Lambda, str] = {}
        for path in sorted(package.rglob("*.py")):
            relative = path.relative_to(package.parent).with_suffix("")
            parts = relative.parts[:-1] if path.name == "__init__.py" else relative.parts
            self.add(Scope(".".join(parts), path, ast.parse(path.read_bytes(), filename=str(path))))

    def add(self, scope: Scope) -> None:
        self.scopes[scope.name] = scope
        if isinstance(scope.node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            args = scope.node.args
            for arg in (*args.posonlyargs, *args.args, *args.kwonlyargs):
                annotations = scope.assignments.setdefault(arg.arg, [])
                if arg.annotation is not None:
                    annotations.append(arg.annotation)
            if scope.parent and isinstance(scope.parent.node, ast.ClassDef):
                if args.args and args.args[0].arg in {"self", "cls"}:
                    scope.bind(args.args[0].arg, scope.parent.name)
        for stmt in scope.node.body:
            for item in (
                (stmt, *expressions(stmt))
                if not isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
                else (stmt,)
            ):
                match item:
                    case ast.Lambda(args=args, body=body):
                        name = f"{scope.name}.<lambda:{item.lineno}:{item.col_offset}>"
                        self.lambdas[item] = name
                        function = ast.FunctionDef(
                            name=name,
                            args=args,
                            body=[ast.Return(value=body)],
                            decorator_list=[],
                            returns=None,
                            type_params=[],
                        )
                        self.add(Scope(name, scope.path, function, scope))
                    case (
                        ast.FunctionDef(name=name) | ast.AsyncFunctionDef(name=name) | ast.ClassDef(name=name)
                    ):
                        symbol = f"{scope.name}.{name}"
                        scope.bind(name, symbol)
                        self.add(Scope(symbol, scope.path, item, scope))
                    case ast.Import(names=names):
                        for alias in names:
                            top = alias.name.split(".")[0]
                            scope.bind(alias.asname or top, alias.name if alias.asname else top)
                    case ast.ImportFrom(module=module, level=level, names=names):
                        parts = scope.module.split(".")
                        if scope.path.name == "__init__.py":
                            parts.append("__init__")
                        prefix = ".".join(parts[:-level]) if level else ""
                        for alias in names:
                            scope.bind(
                                alias.asname or alias.name,
                                ".".join(part for part in (prefix, module, alias.name) if part),
                            )
                    case ast.Assign(targets=targets, value=value):
                        for target in targets:
                            self.assign(scope, target, value)
                    case ast.AnnAssign(target=target, value=value, annotation=annotation):
                        self.assign(scope, target, value or annotation)
                    case ast.With(items=items) | ast.AsyncWith(items=items):
                        for entry in items:
                            if entry.optional_vars is not None:
                                scope.with_values.add(entry.context_expr)
                                self.assign(scope, entry.optional_vars, entry.context_expr)
                    case ast.For(target=ast.Name(id=name), iter=iterable):
                        scope.assignments.setdefault(name, []).append(iterable)
                    case ast.For(target=ast.Tuple() as target, iter=ast.Call() as iterable):
                        self.assign(scope, target, iterable)
                    case ast.GeneratorExp() | ast.ListComp() | ast.SetComp() | ast.DictComp():
                        self.index_comprehension(scope, item)
                    case _:
                        pass

    def index_comprehension(
        self, scope: Scope, expression: ast.GeneratorExp | ast.ListComp | ast.SetComp | ast.DictComp
    ) -> None:
        terms = (
            (expression.key, expression.value) if isinstance(expression, ast.DictComp) else (expression.elt,)
        )
        for position, iterator in enumerate(expression.generators):
            if not isinstance(iterator.target, ast.Name):
                continue
            name = iterator.target.id
            later = expression.generators[position + 1 :]
            if any(isinstance(gen.target, ast.Name) and gen.target.id == name for gen in later):
                continue
            for term in (*terms, *iterator.ifs, *(part for gen in later for part in (gen.iter, *gen.ifs))):
                for node in (term, *expressions(term)):
                    if isinstance(node, ast.Name) and node.id == name and isinstance(node.ctx, ast.Load):
                        scope.comprehension_sources[node] = iterator.iter

    def assign(self, scope: Scope, target: ast.expr, value: ast.expr) -> None:
        match target:
            case ast.Name(id=name):
                scope.assignments.setdefault(name, []).append(value)
            case ast.Attribute(value=ast.Name(id="self"), attr=name) if scope.parent:
                scope.parent.assignments.setdefault(name, []).append(value)
            case ast.Tuple(elts=targets) if isinstance(value, (ast.Tuple, ast.List)):
                for part, expression in zip(targets, value.elts, strict=False):
                    self.assign(scope, part, expression)
            case ast.Tuple(elts=targets) if isinstance(value, ast.Call):
                for position, part in enumerate(targets):
                    self.assign(scope, part, ast.Subscript(value=value, slice=ast.Constant(value=position)))
            case ast.Tuple(elts=[ast.Name(id=head), ast.Starred(value=ast.Name(id=tail))]) if (
                isinstance(value, ast.Attribute) and value.attr == "parts"
            ):
                for name, first in ((head, True), (tail, False)):
                    scope.assignments.setdefault(name, []).append(value)
                    scope.path_unpack[name] = (value, first)
            case _:
                pass

    def resolve(self, scope: Scope, expr: ast.AST | None, seen: frozenset[str] = frozenset()) -> set[str]:
        match expr:
            case ast.Lambda() if expr in self.lambdas:
                return {self.lambdas[expr]}
            case ast.Name(id=name):
                owner = scope.binding_scope(name)
                if name not in owner.names and name not in owner.assignments:
                    return (
                        {f"builtins.{name}"}
                        if name
                        in {
                            "open",
                            "getattr",
                            "map",
                            "filter",
                            "any",
                            "sorted",
                            "str",
                            "tuple",
                            "list",
                            "property",
                            "classmethod",
                            "isinstance",
                        }
                        else set()
                    )
                key = f"{owner.name}:{name}"
                if key in seen:
                    return set()
                imported = owner.names.get(name, set())
                return (
                    imported
                    | {
                        symbol
                        for value in owner.assignments.get(name, ())
                        for symbol in self.resolve(owner, value, seen | {key})
                    }
                    | {
                        symbol
                        for qualified in imported
                        if (reexport := self.scopes.get(qualified.rpartition(".")[0])) is not None
                        and reexport is not owner
                        for symbol in self.resolve(
                            reexport, ast.Name(id=qualified.rpartition(".")[2]), seen | {key}
                        )
                    }
                )
            case ast.Attribute(value=value, attr=name):
                result: set[str] = set()
                for symbol in self.resolve(scope, value, seen):
                    member = f"{symbol}.{name}"
                    result.add(member)
                    owner = self.scopes.get(symbol)
                    if owner and member not in seen:
                        result.update(owner.names.get(name, ()))
                        for assigned in owner.assignments.get(name, ()):
                            result.update(self.resolve(owner, assigned, seen | {member}))
                        if isinstance(owner.node, ast.ClassDef) and member not in self.scopes:
                            for base in owner.node.bases:
                                result.update(
                                    f"{base_type}.{name}"
                                    for base_type in self.resolve(owner, base, seen | {member})
                                )
                        method = self.scopes.get(member)
                        if method and isinstance(method.node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                            if any(
                                isinstance(d, ast.Name) and d.id == "property"
                                for d in method.node.decorator_list
                            ):
                                result.update(self.resolve(method, method.node.returns, seen | {member}))
                return result
            case ast.Call(func=func, args=[base, ast.Constant(value=str() as member), *_]) if self.resolve(
                scope, func, seen
            ) == {"builtins.getattr"}:
                return {f"{symbol}.{member}" for symbol in self.resolve(scope, base, seen)}
            case ast.Call(func=func):
                result = set()
                for name in self.resolve(scope, func, seen):
                    called = self.scopes.get(name)
                    if called and isinstance(called.node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        if name not in seen:
                            result.update(self.resolve(called, called.node.returns, seen | {name}))
                    else:
                        result.add(name)
                return result
            case ast.Subscript(value=value, slice=inner):
                names = self.resolve(scope, value, seen)
                if (
                    isinstance(value, ast.Call)
                    and isinstance(inner, ast.Constant)
                    and isinstance(inner.value, int)
                ):
                    return {
                        symbol
                        for name in self.resolve(scope, value.func, seen)
                        if (target := self.scopes.get(name)) is not None
                        and isinstance(target.node, (ast.FunctionDef, ast.AsyncFunctionDef))
                        and isinstance(returns := target.node.returns, ast.Subscript)
                        and isinstance(returns.slice, ast.Tuple)
                        and 0 <= inner.value < len(returns.slice.elts)
                        for symbol in self.resolve(target, returns.slice.elts[inner.value], seen)
                    }
                if "dagster.ResourceParam" in names:
                    return self.resolve(scope, inner, seen)
                return names - {"os.environ", "os.environb"}
            case ast.Constant(value=str() as annotation):
                if annotation.isidentifier() and (
                    annotation in scope.binding_scope(annotation).assignments
                    or annotation in scope.binding_scope(annotation).names
                ):
                    return self.resolve(scope, ast.Name(id=annotation), seen)
                return {annotation}
            case ast.IfExp(body=left, orelse=right) | ast.BinOp(left=left, right=right):
                return self.resolve(scope, left, seen) | self.resolve(scope, right, seen)
            case _:
                return set()


def names_used(scope: Scope, expr: ast.expr, seen: frozenset[str] = frozenset()) -> set[str]:
    names = {node.id for node in ast.walk(expr) if isinstance(node, ast.Name)}
    return names | {
        dependency
        for name in names - seen
        for value in scope.assignments.get(name, ())
        for dependency in names_used(scope, value, seen | {name})
    }


def operations(scope: Scope) -> Iterator[ast.AST]:
    if isinstance(scope.node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        for default in (*scope.node.args.defaults, *scope.node.args.kw_defaults):
            if default is not None:
                yield default
                yield from expressions(default)
    for stmt in scope.node.body:
        yield stmt
        if not isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            yield from expressions(stmt)


def eager_inputs(index: Index, scope: Scope, node: ast.AST) -> Iterator[tuple[Scope, ast.expr]]:
    if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
        owner = scope.binding_scope(node.id)
        if owner is not scope:
            for value in owner.assignments.get(node.id, ()):
                yield owner, value
        for name in owner.names.get(node.id, ()):
            module, _, member = name.rpartition(".")
            if (imported := index.scopes.get(module)) is not None and imported is not owner:
                for value in imported.assignments.get(member, ()):
                    yield imported, value
    if isinstance(node, ast.Attribute) and isinstance(node.ctx, ast.Load):
        for name in index.resolve(scope, node.value):
            owner = index.scopes.get(name)
            if owner is not None and isinstance(owner.node, ast.Module):
                for value in owner.assignments.get(node.attr, ()):
                    yield owner, value
