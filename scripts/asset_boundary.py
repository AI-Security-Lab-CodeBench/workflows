"""Reject ambient inputs reachable from non-configuration Dagster assets.

This is a source lint for ordinary Python bindings, not a runtime sandbox.
Use explicit imports, asset decorators/factories and typed resource parameters
so their dependencies are visible without importing the code location.
"""

import ast
import sys
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from scripts.asset_boundary_index import (
    Function,
    Index,
    Scope,
    eager_inputs,
    expressions,
    operations,
)
from scripts.asset_boundary_policy import (
    ENV_GET,
    ENVIRONMENT,
    REGISTRY_READS,
    argument,
    file_input,
    resource_environment,
)
from scripts.asset_boundary_provenance import Bindings, Callback, Origin, Provenance, resource

ASSET_DECORATORS = frozenset(
    f"dagster.{name}"
    for name in ("asset", "multi_asset", "graph_asset", "graph_multi_asset", "observable_source_asset")
)
RESOURCE_TYPES = frozenset(
    {"dagster.ConfigurableResource", "dagster.ConfigurableIOManager", "dagster.ResourceDefinition"}
)
IO_MANAGER_TYPES = frozenset({"dagster.ConfigurableIOManager", "dagster.IOManager"})
STORE_OPERATIONS = frozenset(
    f"codebench_pipeline.object_store.ObjectStore.{name}"
    for name in ("read", "write", "from_environment", "_client")
)
_CHECKPOINT_TYPE = "codebench_pipeline.labeling_checkpoint_store.LabelingCheckpointCache"
_CHECKPOINT_NORMALIZER = "codebench_pipeline.labeling_checkpoint_store._checkpoint_digest"
_LABELING_ROOT = "codebench_pipeline.dataset_assets_annotations.annotation_assets.labeling_attempts"
_CHECKPOINT_KEY = ast.dump(ast.parse('f"generations/{generation}/{batch}.json"', mode="eval").body)
_CHECKPOINT_TERMINALS = {
    f"{_CHECKPOINT_TYPE}.resume": "codebench_pipeline.object_store.ObjectStore.read",
    f"{_CHECKPOINT_TYPE}.complete": "codebench_pipeline.object_store.ObjectStore.write",
}
SDK_FACTORIES = frozenset(
    f"{prefix}.{method}"
    for prefix in ("boto3", "boto3.Session", "boto3.session.Session", "botocore.session.Session")
    for method in ("client", "resource", "create_client")
)
EXTERNAL_CALLBACKS = frozenset(
    {
        "functools.partial",
        "builtins.map",
        "builtins.filter",
        "concurrent.futures.ThreadPoolExecutor.map",
        "concurrent.futures.ThreadPoolExecutor.submit",
    }
)


@dataclass(frozen=True, slots=True)
class Finding:
    path: Path
    line: int
    reason: str
    chain: tuple[str, ...]


def call_targets(expr: ast.expr) -> Iterator[ast.expr]:
    match expr:
        case ast.IfExp(body=left, orelse=right):
            yield from call_targets(left)
            yield from call_targets(right)
        case _:
            yield expr


class Boundary:
    def __init__(self, index: Index) -> None:
        self.index = index
        self.configuration_roots: set[str] = set()

    def roots(self) -> set[str]:
        roots: set[str] = set()
        for scope in self.index.scopes.values():
            if isinstance(scope.node, (ast.FunctionDef, ast.AsyncFunctionDef)) and scope.parent:
                for decorator in scope.node.decorator_list:
                    self.add_roots(roots, scope.parent, decorator, {scope.name})
            for node in operations(scope):
                if isinstance(node, ast.Call) and node.args:
                    targets = {
                        name
                        for target in call_targets(node.args[0])
                        for name in self.index.resolve(scope, target)
                    }
                    if isinstance(node.func, ast.Call):
                        self.add_roots(roots, scope, node.func, targets)
                    else:
                        self.add_roots(roots, scope, node, targets)
        return roots

    def add_roots(self, roots: set[str], scope: Scope, decorator: ast.expr, targets: set[str]) -> None:
        factory = decorator.func if isinstance(decorator, ast.Call) else decorator
        if not self.index.resolve(scope, factory) & ASSET_DECORATORS:
            return
        if isinstance(decorator, ast.Call):
            group = argument(decorator, len(decorator.args), "group_name")
            if self.index.resolve(scope, group) == {"configuration"}:
                self.configuration_roots.update(targets & self.index.scopes.keys())
                return
        roots.update(name for name in targets if name in self.index.scopes)

    def injected(self, scope: Scope, node: Function) -> frozenset[str]:
        result: set[str] = set()
        for parameter in (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs):
            annotation = parameter.annotation
            symbols = self.index.resolve(scope, annotation)
            if isinstance(annotation, ast.Subscript):
                if "dagster.ResourceParam" in self.index.resolve(scope, annotation.value):
                    result.add(parameter.arg)
            for name in symbols & self.index.scopes.keys():
                declaration = self.index.scopes[name]
                if isinstance(declaration.node, ast.ClassDef) and any(
                    self.index.resolve(declaration, base) & RESOURCE_TYPES for base in declaration.node.bases
                ):
                    result.add(parameter.arg)
        return frozenset(result)

    def findings(self) -> Iterator[Finding]:
        processing = self.roots()
        for root in sorted(processing | self.configuration_roots):
            asset = self.index.scopes[root]
            if isinstance(asset.node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for finding in self.walk(asset, self.injected(asset, asset.node)):
                    if root in processing or finding.reason == "stage access to durable object storage":
                        yield finding

        for scope in self.index.scopes.values():
            if isinstance(scope.node, ast.ClassDef) and any(
                self.index.resolve(scope, base) & IO_MANAGER_TYPES for base in scope.node.bases
            ):
                for name in ("load_input", "handle_output"):
                    callback = self.index.scopes.get(f"{scope.name}.{name}")
                    if callback is not None and isinstance(
                        callback.node, (ast.FunctionDef, ast.AsyncFunctionDef)
                    ):
                        parameters = (*callback.node.args.posonlyargs, *callback.node.args.args)
                        if len(parameters) >= 2:
                            receiver, context = parameters[:2]
                            yield from self.walk(
                                callback, frozenset({receiver.arg}), frozenset({context.arg})
                            )

    def checkpoint_parameter(self, root: Scope, name: str) -> bool:
        if root.name != _LABELING_ROOT or not isinstance(root.node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return False
        return any(
            part.arg == name
            and isinstance(part.annotation, ast.Subscript)
            and self.index.resolve(root, part.annotation.value) == {"dagster.ResourceParam"}
            and self.index.resolve(root, part.annotation.slice) == {_CHECKPOINT_TYPE}
            for part in (*root.node.args.posonlyargs, *root.node.args.args, *root.node.args.kwonlyargs)
        )

    def checkpoint_terminal(
        self, scope: Scope, node: ast.Call, targets: set[str], bindings: Bindings
    ) -> bool:
        terminal = _CHECKPOINT_TERMINALS.get(scope.name)
        if (
            terminal is None
            or targets != {terminal}
            or (scope.name, "self", Origin.LABELING_CHECKPOINT) not in bindings
        ):
            return False
        address = argument(node, 0, "key")
        if address is None or ast.dump(address) != _CHECKPOINT_KEY:
            return False
        assignments = [
            name
            for name in ast.walk(scope.node)
            if isinstance(name, ast.Name)
            and isinstance(name.ctx, ast.Store)
            and name.id in {"generation", "batch"}
        ]
        if sorted(name.id for name in assignments) != ["batch", "generation"]:
            return False
        normalizer = (
            next(
                (
                    statement
                    for statement in scope.node.body
                    if isinstance(statement, ast.Assign)
                    and ast.unparse(statement)
                    == "generation, batch = (_checkpoint_digest(generation), _checkpoint_digest(batch))"
                ),
                None,
            )
            if isinstance(scope.node, (ast.FunctionDef, ast.AsyncFunctionDef))
            else None
        )
        if (
            normalizer is None
            or normalizer.lineno >= node.lineno
            or self.index.resolve(scope, ast.Name(id="_checkpoint_digest", ctx=ast.Load()))
            != {_CHECKPOINT_NORMALIZER}
        ):
            return False
        validator = self.index.scopes.get(_CHECKPOINT_NORMALIZER)
        if validator is None or not isinstance(validator.node, ast.FunctionDef):
            return False
        match validator.node.body:
            case [
                ast.If(
                    test=ast.Compare(
                        left=ast.Call(
                            func=matcher, args=[ast.Constant(value="[a-f0-9]{64}"), ast.Name(id="value")]
                        ),
                        ops=[ast.Is()],
                        comparators=[ast.Constant(value=None)],
                    ),
                    body=[ast.Raise()],
                    orelse=[],
                ),
                ast.Return(value=ast.Name(id="value")),
            ] if self.index.resolve(validator, matcher) == {"re.fullmatch"}:
                pass
            case _:
                return False
        if not isinstance(node.func, ast.Attribute) or not isinstance(node.func.value, ast.Name):
            return False
        match scope.assignments.get(node.func.value.id):
            case [
                ast.Call(
                    func=constructor, args=[ast.Call(func=config, args=[], keywords=keywords)], keywords=[]
                )
            ]:
                if self.index.resolve(scope, constructor) != {"codebench_pipeline.object_store.ObjectStore"}:
                    return False
                if self.index.resolve(scope, config) != {"codebench_pipeline.object_store.StorageConfig"}:
                    return False
                return {part.arg: ast.unparse(part.value) for part in keywords} == {
                    "local_root": "str(self.root)",
                    "prefix": "'labeling-checkpoints-v1'",
                }
            case _:
                return False

    def walk(
        self, root: Scope, injected: frozenset[str], input_keys: frozenset[str] = frozenset()
    ) -> Iterator[Finding]:
        pending: deque[tuple[Scope, ast.expr | None, tuple[str, ...], Bindings, frozenset[str]]] = deque(
            [
                (
                    root,
                    None,
                    (root.name,),
                    frozenset(
                        (
                            root.name,
                            name,
                            Origin.LABELING_CHECKPOINT
                            if self.checkpoint_parameter(root, name)
                            else Origin.INJECTED,
                        )
                        for name in injected
                    ),
                    input_keys,
                )
            ]
        )
        visited: set[tuple[str, ast.expr | None, Bindings, frozenset[str]]] = set()
        reported: set[tuple[Path, int, str]] = set()
        while pending:
            scope, initializer, chain, bindings, addressed = pending.popleft()
            state = (scope.name, initializer, bindings, addressed)
            if state in visited:
                continue
            visited.add(state)
            provenance = Provenance(self.index, bindings)
            nodes = (
                (initializer, *expressions(initializer))
                if initializer is not None
                else tuple(operations(scope))
            )
            unreachable: set[ast.AST] = set()
            for expression in nodes:
                if not isinstance(expression, ast.IfExp) or expression in unreachable:
                    continue
                selected = provenance.condition(scope, expression.test, frozenset())
                if selected is not None:
                    unreachable.update(ast.walk(expression.orelse if selected else expression.body))
            consumed_environment = {
                id(ref)
                for node in nodes
                if isinstance(node, ast.Call) and self.index.resolve(scope, node.func) & ENV_GET
                for ref in ast.walk(node.func)
            }
            for node in nodes:
                if node in unreachable:
                    continue
                if isinstance(node, (ast.Name, ast.Attribute)):
                    for owner, value in eager_inputs(self.index, scope, node):
                        captured = frozenset(
                            item
                            for item in bindings
                            if owner.name == item[0] or owner.name.startswith(f"{item[0]}.")
                        )
                        pending.append((owner, value, (*chain, owner.name), captured, frozenset()))
                targets: set[str] = set()
                reason = None
                if isinstance(node, ast.Call):
                    targets = self.index.resolve(scope, node.func)
                    invoked = provenance.invoked_callback(scope, node.func, frozenset())
                    if invoked is not None:
                        targets = {invoked.name}
                    if self.checkpoint_terminal(scope, node, targets, bindings):
                        continue
                    if not input_keys and (
                        not targets.isdisjoint(STORE_OPERATIONS)
                        or any(
                            name.startswith(
                                (
                                    "mypy_boto3_s3.S3Client.",
                                    "mypy_boto3_s3.client.S3Client.",
                                    "s3fs.S3FileSystem",
                                    "minio.Minio",
                                )
                            )
                            for name in targets
                        )
                        or (
                            not targets.isdisjoint(SDK_FACTORIES)
                            and not (
                                isinstance(service := argument(node, 0, "service_name"), ast.Constant)
                                and isinstance(service.value, str)
                                and service.value != "s3"
                            )
                        )
                    ):
                        reason = "stage access to durable object storage"
                    elif targets & REGISTRY_READS:
                        reason = "registry business configuration"
                    elif targets & {"dagster.EnvVar", "dagster.EnvVar.int"} or (
                        targets & ENV_GET
                        and not (
                            any(resource(origin) for _, _, origin in bindings)
                            and resource_environment(self.index, scope, argument(node, 0, "key"))
                        )
                    ):
                        reason = "ambient environment"
                    address = file_input(self.index, scope, node, targets)
                    if address is not None and not provenance.allows(
                        scope, node, address, targets, addressed
                    ):
                        reason = "ambient local file input"
                    forwarded = bool(
                        targets & {"functools.partial", "concurrent.futures.ThreadPoolExecutor.submit"}
                    )
                    callback_args = node.args[:1] if targets & EXTERNAL_CALLBACKS else ()
                    for parameter in callback_args:
                        for target in call_targets(parameter):
                            if isinstance(target, (ast.Name, ast.Attribute, ast.Lambda)):
                                callback_binding = provenance.origin(scope, target)
                                callback_targets = (
                                    {callback_binding.name}
                                    if isinstance(callback_binding, Callback)
                                    else self.index.resolve(scope, target)
                                )
                                for name in sorted(callback_targets):
                                    if name in self.index.scopes:
                                        candidate = self.index.scopes[name]
                                        if isinstance(
                                            candidate.node, (ast.FunctionDef, ast.AsyncFunctionDef)
                                        ):
                                            callback = ast.Call(
                                                func=target,
                                                args=node.args[1:] if forwarded else [],
                                                keywords=node.keywords if forwarded else [],
                                            )
                                            pending.append(
                                                (
                                                    candidate,
                                                    None,
                                                    (*chain, name),
                                                    provenance.call(
                                                        scope, callback, candidate, frozenset()
                                                    ).bindings,
                                                    frozenset(),
                                                )
                                            )
                elif isinstance(node, (ast.Name, ast.Attribute)) and isinstance(node.ctx, ast.Load):
                    if id(node) not in consumed_environment and self.index.resolve(scope, node) & ENVIRONMENT:
                        reason = "ambient environment"
                if reason is not None and isinstance(node, ast.expr):
                    key = (scope.path, node.lineno, reason)
                    if key not in reported:
                        reported.add(key)
                        yield Finding(*key, chain)
                for name in sorted((targets - REGISTRY_READS) & self.index.scopes.keys()):
                    target = self.index.scopes[name]
                    if isinstance(target.node, ast.ClassDef):
                        target = self.index.scopes.get(f"{name}.__init__", target)
                    if not isinstance(target.node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        continue
                    call = node
                    if not isinstance(call, ast.Call):
                        continue
                    args = [*target.node.args.posonlyargs, *target.node.args.args]
                    method = bool(args and args[0].arg == "self")
                    called = provenance.call(scope, call, target, frozenset())
                    bound = method and any(
                        owner == target.name and variable == "self" and resource(value)
                        for owner, variable, value in called.bindings
                    )
                    if method:
                        args = args[1:]
                    addressed_parameters: frozenset[str] = (
                        frozenset(
                            {
                                arg.arg
                                for pos, arg in enumerate(args)
                                if argument(call, pos, arg.arg) is not None
                            }
                            | {kw.arg for kw in call.keywords if kw.arg is not None}
                        )
                        if bound
                        else frozenset()
                    )
                    pending.append(
                        (
                            target,
                            None,
                            (*chain, target.name),
                            called.bindings,
                            addressed_parameters,
                        )
                    )


def main() -> int:
    package = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("codebench_pipeline")
    if not package.is_dir():
        print(f"Asset boundary package not found: {package}", file=sys.stderr)
        return 2
    boundary = Boundary(Index(package))
    findings = sorted(set(boundary.findings()), key=lambda row: (row.path, row.line, row.chain, row.reason))
    for row in findings:
        print(f"{row.path}:{row.line}: {row.reason} via {' -> '.join(row.chain)}", file=sys.stderr)
    return int(bool(findings))


if __name__ == "__main__":
    sys.exit(main())
