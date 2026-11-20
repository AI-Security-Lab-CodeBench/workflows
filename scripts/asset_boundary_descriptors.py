"""Recognize the filesystem checks that establish descriptor provenance."""

import ast
from collections.abc import Iterator

from scripts.asset_boundary_containment import prior_statements, rejection_conditions
from scripts.asset_boundary_index import Index, Scope
from scripts.asset_boundary_policy import argument

COMPONENT_PATTERN = r"[A-Za-z0-9][A-Za-z0-9_.-]*"


def rejections(scope: Scope, use: ast.AST) -> Iterator[ast.expr]:
    for statement in prior_statements(scope.node, use):
        if isinstance(statement, ast.If) and statement.body and isinstance(statement.body[0], ast.Raise):
            yield from rejection_conditions(statement.test)


def validates_component(
    index: Index, scope: Scope, call: ast.expr, subject: ast.expr, *, returns_value: bool = True
) -> bool:
    match call:
        case ast.Call(func=func, args=[value], keywords=[]):
            if ast.dump(value) != ast.dump(subject):
                return False
            targets = index.resolve(scope, func)
            if len(targets) != 1:
                return False
            target = index.scopes.get(next(iter(targets)))
            if target is None or not isinstance(target.node, ast.FunctionDef):
                return False
            if not returns_value and rejects_invalid_component(target.node):
                return True
            match target.node:
                case ast.FunctionDef(
                    args=ast.arguments(posonlyargs=[], args=[ast.arg(arg=parameter)]),
                    body=[
                        ast.If(
                            test=ast.Compare(
                                left=ast.Call(
                                    func=matcher,
                                    args=[ast.Constant(value=pattern), ast.Name(id=name)],
                                    keywords=[],
                                ),
                                ops=[ast.Is()],
                                comparators=[ast.Constant(value=None)],
                            ),
                            body=[ast.Raise()],
                            orelse=[],
                        ),
                        ast.Return(value=ast.Name(id=returned)),
                    ],
                ):
                    return (
                        parameter == name == returned
                        and pattern == COMPONENT_PATTERN
                        and index.resolve(target, matcher) == {"re.fullmatch"}
                    )
                case _:
                    return False
        case _:
            return False


def rejects_invalid_component(node: ast.FunctionDef) -> bool:
    match node:
        case ast.FunctionDef(
            args=ast.arguments(posonlyargs=[], args=[ast.arg(arg=parameter)]),
            body=[ast.If(test=ast.BoolOp(op=ast.Or(), values=tests), body=[ast.Raise()], orelse=[])],
        ):
            rejects: set[str] = set()
            for test in tests:
                match test:
                    case ast.Compare(
                        left=ast.Name(id=name),
                        ops=[ast.In()],
                        comparators=[ast.Set(elts=entries)],
                    ) if name == parameter and all(isinstance(entry, ast.Constant) for entry in entries):
                        rejects.update(
                            entry.value
                            for entry in entries
                            if isinstance(entry, ast.Constant) and isinstance(entry.value, str)
                        )
                    case ast.Compare(
                        left=ast.Constant(value=str() as token),
                        ops=[ast.In()],
                        comparators=[ast.Name(id=name)],
                    ) if name == parameter:
                        rejects.add(token)
                    case _:
                        pass
            return {"", ".", "..", "/", "\0"} <= rejects
        case _:
            return False


def checked_parts(index: Index, scope: Scope, use: ast.Starred) -> bool:
    match use.value:
        case ast.GeneratorExp(
            elt=element,
            generators=[
                ast.comprehension(
                    target=ast.Name(id=name),
                    iter=ast.Call(
                        func=ast.Attribute(attr="split"),
                        args=[ast.Constant(value="/")],
                        keywords=[],
                    ),
                    ifs=[],
                    is_async=0,
                )
            ],
        ):
            return validates_component(index, scope, element, ast.Name(id=name, ctx=ast.Load()))
        case _:
            return False


def returns_relative_path(index: Index, scope: Scope, node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    match node:
        case ast.FunctionDef(
            args=ast.arguments(posonlyargs=[], args=[ast.arg(arg=parameter)]),
            body=[
                ast.If(test=test, body=[ast.Raise()], orelse=[]),
                ast.Return(value=ast.Name(id=returned)),
            ],
        ) if parameter == returned:
            for rejection in rejection_conditions(test):
                match rejection:
                    case ast.Call(
                        func=predicate,
                        args=[
                            ast.GeneratorExp(
                                elt=ast.Compare(
                                    left=ast.Name(id=item),
                                    ops=[ast.In()],
                                    comparators=[ast.Set(elts=invalid)],
                                ),
                                generators=[
                                    ast.comprehension(
                                        target=ast.Name(id=part),
                                        iter=ast.Call(
                                            func=ast.Attribute(value=ast.Name(id=source), attr="split"),
                                            args=[ast.Constant(value="/")],
                                            keywords=[],
                                        ),
                                        ifs=[],
                                        is_async=0,
                                    )
                                ],
                            ),
                        ],
                        keywords=[],
                    ) if (
                        source == parameter
                        and part == item
                        and index.resolve(scope, predicate) == {"builtins.any"}
                        and {"", ".", ".."}
                        <= {entry.value for entry in invalid if isinstance(entry, ast.Constant)}
                    ):
                        return True
                    case _:
                        continue
        case _:
            return False
    return False


def rejects_parent_parts(scope: Scope, parts: ast.Attribute, path: ast.Name) -> bool:
    return any(
        isinstance(test, ast.Compare)
        and isinstance(test.left, ast.Constant)
        and test.left.value == ".."
        and len(test.ops) == 1
        and isinstance(test.ops[0], ast.In)
        and len(test.comparators) == 1
        and isinstance(test.comparators[0], ast.Attribute)
        and test.comparators[0].attr == "parts"
        and isinstance(test.comparators[0].value, ast.Name)
        and test.comparators[0].value.id == path.id
        for test in rejections(scope, parts)
    )


def flags(
    index: Index, scope: Scope, expr: ast.expr | None, seen: frozenset[ast.expr] = frozenset()
) -> set[str]:
    if expr is None or expr in seen:
        return set()
    nested = seen | {expr}
    match expr:
        case ast.BinOp(left=left, op=ast.BitOr(), right=right):
            return flags(index, scope, left, nested) | flags(index, scope, right, nested)
        case ast.Name(id=name):
            owner = scope.binding_scope(name)
            if name in owner.assignments:
                definitions = owner.assignments[name]
                if len(definitions) == 1 and name not in owner.names:
                    return flags(index, owner, definitions[0], nested)
                return set()
        case ast.Attribute():
            pass
        case _:
            return set()
    symbols = index.resolve(scope, expr)
    return symbols if len(symbols) == 1 else set()


def checked_regular_file(index: Index, scope: Scope, call: ast.Call, handle: ast.expr) -> bool:
    if not isinstance(handle, ast.Name) or len(scope.assignments.get(handle.id, ())) != 1:
        return False
    for test in rejections(scope, call):
        match test:
            case ast.UnaryOp(
                op=ast.Not(),
                operand=ast.Call(
                    func=predicate,
                    args=[
                        ast.Attribute(
                            attr="st_mode",
                            value=ast.Call(func=fstat, args=[ast.Name(id=name)], keywords=[]),
                        )
                    ],
                    keywords=[],
                ),
            ) if (
                name == handle.id
                and scope.assignments[handle.id][0].lineno < test.lineno
                and index.resolve(scope, predicate) == {"stat.S_ISREG"}
                and index.resolve(scope, fstat) == {"os.fstat"}
            ):
                return True
            case _:
                pass
    return False


def open_flags(index: Index, scope: Scope, call: ast.Call) -> set[str]:
    return flags(index, scope, argument(call, 1, "flags"))
