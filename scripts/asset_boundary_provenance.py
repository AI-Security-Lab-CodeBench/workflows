"""Track origins across assignments, lexical captures and local calls."""

import ast
from dataclasses import dataclass
from enum import Enum, auto

from scripts.asset_boundary_containment import checked_directory_entry, guarded_root, prior_statements
from scripts.asset_boundary_descriptors import (
    checked_parts,
    checked_regular_file,
    open_flags,
    rejections,
    rejects_parent_parts,
    returns_relative_path,
    validates_component,
)
from scripts.asset_boundary_index import Index, Scope, expressions, names_used
from scripts.asset_boundary_policy import ENCODER_DIRECTORY, ENV_GET, argument


class Origin(Enum):
    INJECTED = auto()
    LABELING_CHECKPOINT = auto()
    RESOURCE = auto()
    RESOURCE_CHILD = auto()
    RESOURCE_DYNAMIC = auto()
    SCRATCH = auto()
    ENTRY = auto()
    PACKAGE = auto()
    INSTALLED = auto()
    INSTALLED_CHILD = auto()
    ABSENT = auto()
    ARTIFACT = auto()
    DESCRIPTOR = auto()
    DIRECTORY = auto()
    LEAF = auto()
    ANCHOR = auto()
    COMPONENT = auto()
    RELATIVE = auto()


@dataclass(frozen=True, slots=True)
class Fields:
    values: frozenset[tuple[str, "Value"]]
    declared_type: str


@dataclass(frozen=True, slots=True)
class Alternatives:
    values: frozenset["Value"]


@dataclass(frozen=True, slots=True)
class Callback:
    name: str
    captures: "Bindings"
    receiver: "Value | None" = None


type Value = Origin | Fields | Alternatives | Callback
type Bindings = frozenset[tuple[str, str, Value]]

RESOURCE_PATHS = frozenset({Origin.RESOURCE, Origin.RESOURCE_CHILD, Origin.RESOURCE_DYNAMIC})
OWNED_PATHS = frozenset(
    {
        Origin.RESOURCE,
        Origin.RESOURCE_CHILD,
        Origin.SCRATCH,
        Origin.INSTALLED,
        Origin.INSTALLED_CHILD,
        Origin.ARTIFACT,
    }
)


def resource(value: Value | None) -> bool:
    match value:
        case Fields(values=fields):
            return any(resource(origin) for _, origin in fields)
        case Alternatives(values=values):
            return all(resource(origin) for origin in values)
        case _:
            return value in RESOURCE_PATHS | {Origin.INJECTED, Origin.LABELING_CHECKPOINT}


def parameters(node: ast.FunctionDef | ast.AsyncFunctionDef) -> tuple[ast.arg, ...]:
    return (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs)


def supplied(call: ast.Call, args: tuple[ast.arg, ...]) -> dict[str, ast.expr]:
    return {
        param.arg: value
        for position, param in enumerate(args)
        if (value := argument(call, position, param.arg)) is not None
    }


@dataclass(frozen=True, slots=True)
class Provenance:
    index: Index
    bindings: Bindings
    assumed_directory: tuple[Scope, str] | None = None

    def allows(
        self, scope: Scope, call: ast.Call, address: ast.expr, targets: set[str], addressed: frozenset[str]
    ) -> bool:
        root = self.origin(scope, address)
        if isinstance(call.func, ast.Attribute) and call.func.attr in {"glob", "rglob"}:
            pattern = self.origin(scope, argument(call, 0, "pattern"))
            links = argument(call, len(call.args), "recurse_symlinks")
            if pattern not in {Origin.COMPONENT, Origin.RELATIVE} or (
                links is not None and not (isinstance(links, ast.Constant) and links.value is False)
            ):
                return False
        if "os.fdopen" in targets:
            return root is Origin.DESCRIPTOR or (
                root is Origin.LEAF and checked_regular_file(self.index, scope, call, address)
            )
        if "os.open" in targets:
            return self.descriptor(scope, call, frozenset()) is not None
        if "os.fwalk" in targets:
            return self.owned_walk(scope, call, frozenset())
        if "os.listdir" in targets and root is Origin.DIRECTORY:
            return True
        if root in {Origin.SCRATCH, Origin.INSTALLED, Origin.INSTALLED_CHILD, Origin.ARTIFACT}:
            return True
        if root in RESOURCE_PATHS:
            used = names_used(scope, address)
            if "self" in used:
                return bool(used & addressed)
            return not any(name in used and origin is Origin.INJECTED for _, name, origin in self.bindings)
        return False

    def bind(self, target: Scope, values: dict[str, Value]) -> "Provenance":
        names = set(target.assignments)
        captures = {
            item for item in self.bindings if target.name.startswith(f"{item[0]}.") and item[1] not in names
        }
        return Provenance(
            self.index, frozenset(captures | {(target.name, name, origin) for name, origin in values.items()})
        )

    def call(self, scope: Scope, call: ast.Call, target: Scope, seen: frozenset[ast.AST]) -> "Provenance":
        if not isinstance(target.node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return self.bind(target, {})
        args = parameters(target.node)
        values: dict[str, Value] = {}
        invoked = self.invoked_callback(scope, call.func, seen)
        callback = invoked if isinstance(invoked, Callback) and invoked.name == target.name else None
        if args and args[0].arg == "self":
            receiver = (
                callback.receiver
                if callback is not None
                else (
                    self.origin(scope, call.func.value, seen)
                    if isinstance(call.func, ast.Attribute) and target.node.name != "__init__"
                    else None
                )
            )
            if receiver is not None:
                values["self"] = receiver
            args = args[1:]
        elif args and any(
            self.index.resolve(target, decorator) == {"builtins.classmethod"}
            for decorator in target.node.decorator_list
        ):
            args = args[1:]
        for name, expr in supplied(call, args).items():
            source = self.origin(scope, expr, seen)
            if source is None:
                source = self.callback(scope, expr, seen)
            if source is not None:
                values[name] = source
        lexical = Provenance(self.index, callback.captures) if callback is not None else self
        return lexical.bind(target, values)

    def callback(self, scope: Scope, expr: ast.expr, seen: frozenset[ast.AST]) -> Callback | None:
        if not isinstance(expr, (ast.Name, ast.Attribute, ast.Lambda)):
            return None
        functions = [
            target
            for name in self.index.resolve(scope, expr)
            if (target := self.index.scopes.get(name)) is not None
            and isinstance(target.node, (ast.FunctionDef, ast.AsyncFunctionDef))
        ]
        if len(functions) != 1:
            return None
        target = functions[0]
        receiver = self.origin(scope, expr.value, seen) if isinstance(expr, ast.Attribute) else None
        return Callback(target.name, self.bind(target, {}).bindings, receiver)

    def invoked_callback(self, scope: Scope, expr: ast.expr, seen: frozenset[ast.AST]) -> Callback | None:
        match expr:
            case ast.Name():
                value = self.origin(scope, expr, seen)
            case ast.Attribute(value=receiver, attr=member):
                owner = self.origin(scope, receiver, seen)
                value = self.field(owner, member) if isinstance(owner, Fields) else None
            case _:
                return None
        return value if isinstance(value, Callback) else None

    def origin(
        self, scope: Scope, expr: ast.AST | None, seen: frozenset[ast.AST] = frozenset()
    ) -> Value | None:
        if isinstance(expr, ast.Name) and self.assumed_directory == (scope, expr.id):
            return Origin.DIRECTORY
        if expr is None or expr in seen:
            return None
        nested = seen | {expr}
        match expr:
            case ast.Name(id=name):
                if (
                    self.assumed_directory is None
                    and (directory := self.walk_directory(scope, expr, nested)) is not None
                ):
                    return directory
                source = scope.comprehension_sources.get(expr)
                value = (
                    self.name_origin(scope, name, nested)
                    if source is None
                    else self.origin(scope, source, nested)
                )
                if value is Origin.ENTRY and checked_directory_entry(scope, expr):
                    return Origin.SCRATCH
                if (
                    isinstance(value, Alternatives)
                    and Origin.ABSENT in value.values
                    and any(
                        isinstance(test, ast.Compare)
                        and isinstance(test.left, ast.Name)
                        and test.left.id == name
                        and len(test.ops) == 1
                        and isinstance(test.ops[0], ast.Is)
                        and len(test.comparators) == 1
                        and isinstance(test.comparators[0], ast.Constant)
                        and test.comparators[0].value is None
                        and all(
                            definition.lineno < test.lineno
                            for definition in scope.binding_scope(name).assignments.get(name, ())
                        )
                        for test in rejections(scope, expr)
                    )
                ):
                    present = value.values - {Origin.ABSENT}
                    return next(iter(present)) if len(present) == 1 else Alternatives(present)
                if value is None and (root := guarded_root(scope, expr)) is not None:
                    owned = self.origin(scope, root, nested)
                    if owned in {Origin.SCRATCH, Origin.RESOURCE, Origin.INSTALLED, Origin.ARTIFACT}:
                        return owned
                return value
            case ast.Constant(value=str() as text):
                if "\\" in text or not all(part not in {"", ".", ".."} for part in text.split("/")):
                    return None
                return Origin.RELATIVE if "/" in text else Origin.COMPONENT
            case ast.Constant(value=None):
                return Origin.ABSENT
            case ast.Attribute(value=value, attr="parent"):
                parent = self.origin(scope, value, nested)
                if parent is Origin.RESOURCE_CHILD:
                    return Origin.RESOURCE
                if parent is Origin.INSTALLED_CHILD:
                    return Origin.INSTALLED
                return Origin.PACKAGE if parent is Origin.PACKAGE else None
            case ast.Attribute(value=value, attr="name") if self.origin(scope, value, nested) in OWNED_PATHS:
                return Origin.COMPONENT
            case ast.Attribute(value=value, attr=member):
                return self.member_origin(scope, value, member, nested)
            case ast.BinOp(left=left, op=ast.Div(), right=right):
                root = self.origin(scope, left, nested)
                name = self.origin(scope, right, nested)
                if root in OWNED_PATHS | {Origin.RESOURCE_DYNAMIC, Origin.PACKAGE} and name in {
                    Origin.COMPONENT,
                    Origin.RELATIVE,
                }:
                    if root is Origin.INSTALLED:
                        return Origin.INSTALLED_CHILD
                    return root
                if root in RESOURCE_PATHS and not isinstance(right, ast.Constant):
                    return Origin.RESOURCE_DYNAMIC
                return None
            case ast.Subscript(value=ast.Call() as call, slice=ast.Constant(value=int() as position)):
                return self.item_origin(scope, call, position, nested)
            case ast.Subscript(value=value):
                return self.origin(scope, value, nested)
            case ast.Starred():
                return Origin.COMPONENT if checked_parts(self.index, scope, expr) else None
            case ast.BoolOp(op=ast.Or(), values=branches):
                alternatives: set[Value] = set()
                for position, branch in enumerate(branches):
                    match self.origin(scope, branch, nested):
                        case None:
                            return None
                        case Origin.ABSENT if position < len(branches) - 1:
                            continue
                        case Alternatives(values=values):
                            alternatives.update(values)
                        case value:
                            alternatives.add(value)
                return (
                    next(iter(alternatives))
                    if len(alternatives) == 1
                    else Alternatives(frozenset(alternatives))
                    if alternatives
                    else Origin.ABSENT
                )
            case ast.IfExp(test=test, body=left, orelse=right):
                match self.condition(scope, test, nested):
                    case True:
                        return self.origin(scope, left, nested)
                    case False:
                        return self.origin(scope, right, nested)
                    case None:
                        pass
                a, b = self.origin(scope, left, nested), self.origin(scope, right, nested)
                if a is not None and b is not None and Origin.ABSENT in {a, b} and a != b:
                    present = b if a is Origin.ABSENT else a
                    values = present.values if isinstance(present, Alternatives) else frozenset({present})
                    return Alternatives(values | {Origin.ABSENT})
                return a if a == b else None
            case ast.Call():
                return self.call_origin(scope, expr, nested)
            case _:
                return None

    def name_origin(self, scope: Scope, name: str, seen: frozenset[ast.AST]) -> Value | None:
        owner = scope.binding_scope(name)
        if name == "__file__" and name not in owner.assignments and name not in owner.names:
            return Origin.PACKAGE
        assigned = {self.origin(owner, value, seen) for value in owner.assignments.get(name, ())}
        if (unpacked := owner.path_unpack.get(name)) is not None and owner.assignments.get(name) == [
            unpacked[0]
        ]:
            parts, first = unpacked
            match parts.value:
                case ast.Attribute(value=ast.Name(id=absolute) as path, attr="parent") | (
                    ast.Name(id=absolute) as path
                ):
                    match owner.assignments.get(absolute, ()):
                        case [
                            ast.Call(
                                func=ast.Attribute(value=source, attr="absolute"), args=[], keywords=[]
                            ) as definition
                        ] if (
                            definition.lineno < parts.lineno
                            and self.origin(owner, source, seen) in OWNED_PATHS
                            and rejects_parent_parts(owner, parts, path)
                        ):
                            return Origin.ANCHOR if first else Origin.COMPONENT
                        case _:
                            pass
                case _:
                    pass
        bound = {
            origin for frame, variable, origin in self.bindings if (frame, variable) == (owner.name, name)
        }
        if bound:
            if isinstance(owner.node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                annotations = {arg.annotation for arg in parameters(owner.node) if arg.arg == name}
                assigned = {
                    self.origin(owner, value, seen)
                    for value in owner.assignments.get(name, ())
                    if value not in annotations
                }
            assigned |= bound
        return next(iter(assigned)) if len(assigned) == 1 else None

    def member_origin(
        self, scope: Scope, value: ast.expr, member: str, seen: frozenset[ast.AST]
    ) -> Value | None:
        base = self.origin(scope, value, seen)
        owners = self.index.resolve(scope, value)
        properties = [
            target
            for owner in owners
            if (target := self.index.scopes.get(f"{owner}.{member}")) is not None
            and isinstance(target.node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and any(
                self.index.resolve(target, decorator) == {"builtins.property"}
                for decorator in target.node.decorator_list
            )
        ]
        if properties:
            results = {
                self.bind(prop, {"self": base} if base is not None else {}).returned(prop, seen)
                for prop in properties
            }
            result = next(iter(results)) if len(results) == 1 else None
            if resource(base) and isinstance(result, Fields):
                return Fields(
                    frozenset(
                        (name, Origin.INSTALLED if origin is Origin.PACKAGE else origin)
                        for name, origin in result.values
                    ),
                    result.declared_type,
                )
            return result
        return self.field(base, member)

    def field(self, base: Value | None, member: str) -> Value | None:
        match base:
            case Fields(values=fields):
                return next((origin for name, origin in fields if name == member), None)
            case Alternatives(values=values):
                fields = {self.field(value, member) for value in values if value is not Origin.ABSENT}
                return next(iter(fields)) if len(fields) == 1 else None
            case Origin.INJECTED | Origin.LABELING_CHECKPOINT:
                return Origin.RESOURCE
            case _:
                return base if base in RESOURCE_PATHS | {Origin.PACKAGE} else None

    def returned(self, scope: Scope, seen: frozenset[ast.AST]) -> Value | None:
        values = {
            self.origin(scope, node.value, seen)
            for node in expressions(scope.node)
            if isinstance(node, (ast.Return, ast.Yield)) and node.value is not None
        }
        return next(iter(values)) if len(values) == 1 else None

    def condition(self, scope: Scope, test: ast.expr, seen: frozenset[ast.AST]) -> bool | None:
        match test:
            case ast.Compare(
                left=subject,
                ops=[ast.Is() | ast.IsNot() as operation],
                comparators=[ast.Constant(value=None)],
            ):
                value = self.origin(scope, subject, seen)
                if value is Origin.ABSENT:
                    return isinstance(operation, ast.Is)
                if isinstance(value, Fields):
                    return isinstance(operation, ast.IsNot)
                if isinstance(value, Alternatives) and all(isinstance(item, Fields) for item in value.values):
                    return isinstance(operation, ast.IsNot)
            case ast.Call(func=func, args=[subject, expected], keywords=[]) if self.index.resolve(
                scope, func
            ) == {"builtins.isinstance"}:
                value = self.origin(scope, subject, seen)
                if isinstance(value, Fields) and (model := self.index.scopes.get(value.declared_type)):
                    targets = self.index.resolve(scope, expected)
                    if targets == {model.name}:
                        return True
                    if (
                        targets == {"pathlib.Path"}
                        and isinstance(model.node, ast.ClassDef)
                        and not model.node.bases
                        and not model.node.keywords
                    ):
                        return False
            case _:
                pass
        return None

    def item_origin(
        self, scope: Scope, call: ast.Call, position: int, seen: frozenset[ast.AST]
    ) -> Value | None:
        names = self.index.resolve(scope, call.func)
        if (invoked := self.invoked_callback(scope, call.func, seen)) is not None:
            names = {invoked.name}
        if names == {"os.fwalk"}:
            if not self.owned_walk(scope, call, seen):
                return None
            return {0: Origin.SCRATCH, 1: Origin.COMPONENT, 2: Origin.COMPONENT, 3: Origin.DESCRIPTOR}.get(
                position
            )
        results = {
            self.call(scope, call, target, seen).origin(target, node.value.elts[position], seen)
            for name in names
            if (target := self.index.scopes.get(name)) is not None
            for node in expressions(target.node)
            if isinstance(node, ast.Return)
            and isinstance(node.value, (ast.Tuple, ast.List))
            and 0 <= position < len(node.value.elts)
        }
        return next(iter(results)) if len(results) == 1 else None

    def owned_walk(self, scope: Scope, call: ast.Call, seen: frozenset[ast.AST]) -> bool:
        follow = argument(call, len(call.args), "follow_symlinks")
        if follow is not None and not (isinstance(follow, ast.Constant) and follow.value is False):
            return False
        top = argument(call, 0, "top")
        directory = argument(call, len(call.args), "dir_fd")
        if directory is None:
            return self.origin(scope, top, seen) is Origin.SCRATCH
        return (
            isinstance(top, ast.Constant)
            and top.value == "."
            and self.origin(scope, directory, seen) is Origin.DIRECTORY
        )

    def descriptor(self, scope: Scope, call: ast.Call, seen: frozenset[ast.AST]) -> Origin | None:
        path = argument(call, 0, "path")
        origin = self.origin(scope, path, seen)
        parent_expr = argument(call, len(call.args), "dir_fd")
        flags = open_flags(self.index, scope, call)
        directory_flags = {"os.O_DIRECTORY", "os.O_NOFOLLOW"} <= flags
        if parent_expr is None:
            if origin is Origin.ANCHOR and directory_flags:
                return Origin.DIRECTORY
            if origin in OWNED_PATHS - {Origin.ARTIFACT}:
                return Origin.DESCRIPTOR
            return None
        parent = self.origin(scope, parent_expr, seen)
        checked = origin is Origin.COMPONENT or (
            isinstance(path, ast.Name)
            and isinstance(scope.node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and any(
                param.arg == path.id
                and scope.assignments.get(path.id) == ([param.annotation] if param.annotation else [])
                for param in parameters(scope.node)
            )
            and any(
                isinstance(stmt, ast.Expr)
                and validates_component(self.index, scope, stmt.value, path, returns_value=False)
                for stmt in prior_statements(scope.node, call)
            )
        )
        if not checked or "os.O_NOFOLLOW" not in flags:
            return None
        if parent is Origin.DIRECTORY:
            return Origin.DIRECTORY if directory_flags else Origin.LEAF
        return Origin.DESCRIPTOR if parent is Origin.DESCRIPTOR else None

    def walk_directory(self, scope: Scope, use: ast.Name, seen: frozenset[ast.AST]) -> Origin | None:
        definitions = scope.assignments.get(use.id, ())
        if len(definitions) < 2 or not all(isinstance(item, ast.Call) for item in definitions):
            return None
        seed, *steps = definitions
        if not isinstance(seed, ast.Call) or self.index.resolve(scope, seed.func) != {"os.open"}:
            return None
        if not any(
            isinstance(stmt, ast.Assign) and stmt.value is seed for stmt in prior_statements(scope.node, use)
        ):
            return None
        if self.descriptor(scope, seed, seen) is not Origin.DIRECTORY:
            return None
        pinned = Provenance(self.index, self.bindings, (scope, use.id))
        if all(
            isinstance(step, ast.Call)
            and any(
                isinstance(stmt, ast.Assign) and stmt.value is seed
                for stmt in prior_statements(scope.node, step)
            )
            and pinned.origin(scope, step, seen) is Origin.DIRECTORY
            for step in steps
        ):
            return Origin.DIRECTORY
        return None

    def call_origin(self, scope: Scope, call: ast.Call, seen: frozenset[ast.AST]) -> Value | None:
        names = self.index.resolve(scope, call.func)
        first = call.args[0] if call.args else None
        if (
            names & ENV_GET
            and any(
                frame == scope.name and name == "self" and resource(origin)
                for frame, name, origin in self.bindings
            )
            and self.encoder_key(scope, argument(call, 0, "key"))
        ):
            return Origin.ARTIFACT
        if names == {"tempfile.mkdtemp"} or (
            names == {"tempfile.TemporaryDirectory"} and call in scope.with_values
        ):
            return Origin.SCRATCH
        if names == {"os.open"}:
            return self.descriptor(scope, call, seen)
        if names & {"builtins.sorted", "builtins.tuple", "builtins.list"}:
            return self.origin(scope, first, seen)
        if names == {"builtins.str"} and len(call.args) == 1 and not call.keywords:
            value = self.origin(scope, first, seen)
            return value if value in OWNED_PATHS | {Origin.PACKAGE} else None
        if names == {"pathlib.Path"}:
            root = self.origin(scope, first, seen)
            if root in OWNED_PATHS | {Origin.PACKAGE} and any(
                self.origin(scope, part, seen) not in {Origin.COMPONENT, Origin.RELATIVE}
                for part in call.args[1:]
            ):
                return Origin.RESOURCE_DYNAMIC if root is Origin.RESOURCE else None
            return root
        if isinstance(call.func, ast.Attribute) and call.func.attr in {
            "enter_context",
            "absolute",
            "joinpath",
            "resolve",
            "rglob",
            "glob",
            "iterdir",
            "with_name",
            "with_suffix",
        }:
            if call.func.attr == "enter_context" and self.index.resolve(scope, call.func.value) == {
                "contextlib.ExitStack"
            }:
                return self.origin(scope, first, seen)
            base = self.origin(scope, call.func.value, seen)
            if base in OWNED_PATHS | {Origin.RESOURCE_DYNAMIC, Origin.PACKAGE}:
                if base is Origin.SCRATCH and call.func.attr in {"iterdir", "glob", "rglob"}:
                    return Origin.ENTRY
                if call.func.attr == "with_name" and self.origin(scope, first, seen) is not Origin.COMPONENT:
                    return None
                if call.func.attr == "joinpath" and any(
                    self.origin(scope, part, seen) not in {Origin.COMPONENT, Origin.RELATIVE}
                    for part in call.args
                ):
                    return None
                if call.func.attr == "joinpath" and call.args and base is Origin.RESOURCE:
                    return Origin.RESOURCE_CHILD
                if call.func.attr == "joinpath" and call.args and base is Origin.INSTALLED:
                    return Origin.INSTALLED_CHILD
                return base
        invoked = self.invoked_callback(scope, call.func, seen)
        if invoked is not None:
            names = {invoked.name}
        results = {
            (
                Origin.RELATIVE
                if returns_relative_path(self.index, target, target.node)
                else self.call(scope, call, target, seen).returned(target, seen)
            )
            if isinstance(target.node, (ast.FunctionDef, ast.AsyncFunctionDef))
            else self.construct(scope, call, target, seen)
            for name in names
            if (target := self.index.scopes.get(name)) is not None
        }
        return next(iter(results)) if len(results) == 1 else None

    def encoder_key(self, scope: Scope, key: ast.expr | None) -> bool:
        names = self.index.resolve(scope, key)
        if isinstance(key, ast.Name):
            names -= scope.binding_scope(key.id).names.get(key.id, set())
        return names == {ENCODER_DIRECTORY}

    def construct(
        self, scope: Scope, call: ast.Call, target: Scope, seen: frozenset[ast.AST]
    ) -> Fields | None:
        is_dataclass = isinstance(target.node, ast.ClassDef) and any(
            "dataclasses.dataclass" in self.index.resolve(target, d.func if isinstance(d, ast.Call) else d)
            for d in target.node.decorator_list
        )
        if not isinstance(target.node, ast.ClassDef):
            return None
        init = self.index.scopes.get(f"{target.name}.__init__")
        if init is not None:
            constructor = self.call(scope, call, init, seen)
            members = {
                (member.attr, value)
                for stmt in init.node.body
                if isinstance(stmt, ast.Assign)
                for member in stmt.targets
                if isinstance(member, ast.Attribute)
                and isinstance(member.value, ast.Name)
                and member.value.id == "self"
                if (value := constructor.origin(init, stmt.value, seen)) is not None
            }
            return Fields(frozenset(members), target.name) if members else None
        roots = [
            field
            for field in target.node.body
            if isinstance(field, ast.AnnAssign) and isinstance(field.target, ast.Name)
        ]
        post_init = self.index.scopes.get(f"{target.name}.__post_init__") if is_dataclass else None
        paths: set[tuple[str, Value]] = set()
        for position, field in enumerate(roots):
            if not isinstance(field.target, ast.Name):
                continue
            value = argument(call, position if is_dataclass else len(call.args), field.target.id)
            parent = (
                self.origin(scope, value, seen)
                if value is not None
                else (self.origin(target, field.value, seen) if is_dataclass else None)
            )
            if post_init is not None and any(
                isinstance(stmt, ast.Expr)
                and validates_component(
                    self.index,
                    post_init,
                    stmt.value,
                    ast.Attribute(
                        value=ast.Name(id="self", ctx=ast.Load()), attr=field.target.id, ctx=ast.Load()
                    ),
                )
                for stmt in post_init.node.body
            ):
                parent = Origin.COMPONENT
            if parent is not None:
                paths.add((field.target.id, parent))
        return Fields(frozenset(paths), target.name) if paths else None
