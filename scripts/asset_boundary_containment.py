"""Recognize guards that confine a resolved address before it is used."""

import ast
from collections.abc import Iterator

from scripts.asset_boundary_index import Scope


def suites(node: ast.AST) -> Iterator[list[ast.stmt]]:
    match node:
        case (
            ast.If(body=body, orelse=orelse)
            | ast.For(body=body, orelse=orelse)
            | ast.While(body=body, orelse=orelse)
            | ast.AsyncFor(body=body, orelse=orelse)
        ):
            yield body
            yield orelse
        case (
            ast.Try(body=body, handlers=handlers, orelse=orelse, finalbody=finalbody)
            | ast.TryStar(body=body, handlers=handlers, orelse=orelse, finalbody=finalbody)
        ):
            yield body
            yield orelse
            yield finalbody
            yield from (handler.body for handler in handlers)
        case ast.Match(cases=cases):
            yield from (case.body for case in cases)
        case (
            ast.With(body=body)
            | ast.AsyncWith(body=body)
            | ast.FunctionDef(body=body)
            | ast.AsyncFunctionDef(body=body)
        ):
            yield body
        case _:
            return


def prior_statements(node: ast.AST, use: ast.AST) -> Iterator[ast.stmt]:
    for suite in suites(node):
        previous: list[ast.stmt] = []
        for entry in suite:
            if any(child is use for child in ast.walk(entry)):
                yield from previous
                yield from prior_statements(entry, use)
                break
            previous.append(entry)


def rejection_conditions(test: ast.expr) -> Iterator[ast.expr]:
    if isinstance(test, ast.BoolOp) and isinstance(test.op, ast.Or):
        for part in test.values:
            yield from rejection_conditions(part)
    else:
        yield test


def resolves(expr: ast.expr, name: str | None = None) -> bool:
    match expr:
        case ast.Call(func=ast.Attribute(value=receiver, attr="resolve"), args=[], keywords=keywords):
            strict = not keywords or (
                len(keywords) == 1
                and keywords[0].arg == "strict"
                and isinstance(keywords[0].value, ast.Constant)
                and keywords[0].value.value is True
            )
            return strict and (name is None or (isinstance(receiver, ast.Name) and receiver.id == name))
        case _:
            return False


def guarded_root(scope: Scope, use: ast.Name) -> ast.expr | None:
    """Return the checked boundary, never an expected digest or an untrusted root."""
    definitions = scope.assignments.get(use.id, ())
    if len(definitions) != 1:
        return None
    definition = definitions[0]
    if not isinstance(definition, (ast.Call, ast.BinOp)):
        return None
    resolved = resolves(definition)
    roots: list[ast.expr] = []
    for prior in prior_statements(scope.node, use):
        if not isinstance(prior, ast.If) or not prior.body or not isinstance(prior.body[0], ast.Raise):
            continue
        if prior.lineno <= definition.lineno:
            continue
        for test in rejection_conditions(prior.test):
            match test:
                case ast.UnaryOp(
                    op=ast.Not(),
                    operand=ast.Call(
                        func=ast.Attribute(value=ast.Name(id=name), attr="is_relative_to"),
                        args=[root],
                        keywords=[],
                    ),
                ) if name == use.id:
                    roots.append(root)
                case ast.Compare(left=left, ops=[ast.NotEq()], comparators=[ast.Name(id=name)]) if (
                    name == use.id
                ):
                    resolved |= resolves(left, use.id)
                case _:
                    pass
    return roots[0] if resolved and len(roots) == 1 else None


def checked_directory_entry(scope: Scope, use: ast.Name) -> bool:
    for node in ast.walk(scope.node):
        if not isinstance(node, ast.BoolOp) or not isinstance(node.op, ast.Or):
            continue
        checked = False
        for value in node.values:
            if any(item is use for item in ast.walk(value)):
                if checked:
                    return True
                break
            match value:
                case ast.Call(
                    func=ast.Attribute(value=ast.Name(id=name) as entry, attr="is_symlink"),
                    args=[],
                    keywords=[],
                ) if name == use.id and scope.comprehension_sources.get(
                    entry
                ) is scope.comprehension_sources.get(use):
                    checked = True
                case _:
                    pass
    return False
