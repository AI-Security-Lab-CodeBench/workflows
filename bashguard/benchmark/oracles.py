from __future__ import annotations

import re
import shlex
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Final, Protocol, TypedDict, TypeVar, runtime_checkable

import bashlex
import tree_sitter_bash
from bashlex import ast
from bashlex.errors import ParsingError
from tree_sitter import Language, Node, Parser


class KindNode(Protocol):
    kind: str


@runtime_checkable
class WordNode(Protocol):
    kind: str
    parts: list[KindNode]
    word: str
    pos: tuple[int, int]


@dataclass(frozen=True, slots=True)
class Fixture:
    name: str
    source: str
    accepted: bool
    heads: tuple[str | None, ...] | None


@dataclass(frozen=True, slots=True)
class Span:
    kind: str
    start: int
    end: int
    unit: str


@dataclass(frozen=True, slots=True)
class Observation:
    accepted: bool | None
    status: str
    heads: tuple[str | None, ...]
    spans: tuple[Span, ...] = ()
    diagnostics: tuple[str, ...] = ()
    eligible: bool | None = None


@dataclass(frozen=True, slots=True)
class ProtocolError(Exception):
    detail: str

    def __str__(self) -> str:
        return self.detail


class WordPayload(TypedDict):
    value: str
    static: bool


class CallPayload(TypedDict):
    words: list[WordPayload]


class NodePayload(TypedDict):
    kind: str
    start: int
    end: int


class AnalysisPayload(TypedDict, total=False):
    source: str
    nodes: list[NodePayload]
    calls: list[CallPayload]
    eligible: bool
    reasons: list[str]


class Response(TypedDict, total=False):
    analysis: AnalysisPayload
    error: str
    parse_ns: int
    count: int
    errors: int
    analyze_ns: int


class Timing(TypedDict, total=False):
    metric: str
    total_ns: int
    count: int
    ns_per_attempt: float
    raised_attempts: int
    subprocess_transport_ns: int
    error: str
    response: list[Response]
    go_errors: int
    go_analyze_ns: int


InputT = TypeVar("InputT")
ResultT = TypeVar("ResultT")


FIXTURES: Final = (
    Fixture("simple", "printf '%s\\n' hello", True, ("printf",)),
    Fixture("quoted_head", "'printf' x", True, ("printf",)),
    Fixture("concatenated_head", "p'r'intf x", True, ("printf",)),
    Fixture("continued_head", "pri\\\nntf x", True, ("printf",)),
    Fixture("continued_argument", "printf a\\\nb", True, ("printf",)),
    Fixture("escaped_space", "printf x \\  y", True, ("printf",)),
    Fixture("literal_command_data", "printf '%s' '$(touch inert)'", True, ("printf",)),
    Fixture("double_quoted_substitution", 'printf "%s" "$(printf x)"', True, ("printf", "printf")),
    Fixture("comment", "# touch inert\nprintf real", True, ("printf",)),
    Fixture("pipeline", "printf x | cat", True, ("printf", "cat")),
    Fixture("list", "true && printf yes; false || printf no", True, ("true", "printf", "false", "printf")),
    Fixture("assignment_prefix", "A=1 B='two' command -- printf x", True, ("command",)),
    Fixture("array", "a=(one 'two three'); printf '%s' \"${a[@]}\"", True, ("printf",)),
    Fixture("arithmetic", "(( x = 1 + $(printf 2) ))", True, None),
    Fixture("test", "[[ $x =~ ^a[0-9]+$ ]] && printf yes", True, None),
    Fixture("process_substitution", "diff <(printf a) <(printf b)", True, ("diff", "printf", "printf")),
    Fixture("nested_substitution", 'printf "$(printf "$(printf x)")"', True, ("printf", "printf", "printf")),
    Fixture("heredoc", "cat <<EOF\n$(printf x)\nEOF\n", True, ("cat", "printf")),
    Fixture("quoted_heredoc", "cat <<'EOF'\n$(printf x)\nEOF\n", True, ("cat",)),
    Fixture("multiple_heredocs", "cat <<A <<B\none\nA\ntwo\nB\n", True, ("cat",)),
    Fixture("here_string", 'cat <<<"$(printf x)"', True, ("cat", "printf")),
    Fixture("redirect_only", "> output", True, ()),
    Fixture("redirect_order", "printf x 2>&1 >out", True, ("printf",)),
    Fixture("function", "f() { printf x; }; f", True, ("printf", "f")),
    Fixture("dynamic_head", '"$tool" "$target"', True, (None,)),
    Fixture("eval_data", "eval 'printf x'", True, ("eval",)),
    Fixture("unicode", "printf '%s' café; printf done", True, ("printf", "printf")),
    Fixture("missing_substitution_closer", "echo $(printf x", False, None),
    Fixture("missing_pipeline_command", "echo ok |", False, None),
    Fixture("unterminated_quote", 'echo "unterminated', False, None),
)

LEXICAL_SCAN: Final = re.compile(r"(?:\A|[;&|\n])\s*(?P<head>[^\s;&|()<>]+)")
LANGUAGE: Final = Language(tree_sitter_bash.language())
TREE_PARSER: Final = Parser(LANGUAGE)


def tree_nodes(root: Node) -> Iterator[Node]:
    stack = [root]
    while stack:
        node = stack.pop()
        yield node
        stack.extend(reversed(node.children))


def lexical_head(raw: str) -> str:
    words = shlex.split(raw.replace("\\\n", ""), posix=True)
    return words[0] if len(words) == 1 else raw


def observe_tree(source: str) -> Observation:
    raw = source.encode("utf-8")
    root = TREE_PARSER.parse(raw).root_node
    heads: list[str | None] = []
    spans: list[Span] = []
    diagnostics: list[str] = []
    for node in tree_nodes(root):
        if node.is_error or node.is_missing:
            diagnostics.append(
                f"{'MISSING' if node.is_missing else 'ERROR'} {node.type} "
                f"[{node.start_byte},{node.end_byte})"
            )
        if node.type == "command":
            name = node.child_by_field_name("name")
            if name is None:
                continue
            dynamic = any(
                "expansion" in child.type or "substitution" in child.type
                for child in tree_nodes(name)
            )
            text = raw[name.start_byte:name.end_byte].decode("utf-8")
            heads.append(None if dynamic else lexical_head(text))
            spans.append(Span("command_head", name.start_byte, name.end_byte, "utf8_bytes"))
    return Observation(
        not root.has_error,
        "syntax_error" if root.has_error else "accepted",
        tuple(heads),
        tuple(spans),
        tuple(diagnostics),
    )


def observe_bashlex(source: str) -> Observation:
    try:
        roots = bashlex.parse(source)
    except ParsingError as error:
        return Observation(False, "syntax_error", (), diagnostics=(str(error),))
    except NotImplementedError as error:
        return Observation(None, "unsupported", (), diagnostics=(str(error),))
    heads: list[str | None] = []
    spans: list[Span] = []

    class Visitor(ast.nodevisitor):
        def visitcommand(self, n: ast.node, parts: list[ast.node]) -> None:
            words = [part for part in parts if isinstance(part, WordNode) and part.kind == "word"]
            if words:
                word = words[0]
                dynamic = any(
                    part.kind in {"parameter", "commandsubstitution", "processsubstitution", "tilde"}
                    for part in word.parts
                )
                heads.append(None if dynamic else word.word)
                spans.append(Span("command_head", word.pos[0], word.pos[1], "unicode_characters"))

    visitor = Visitor()
    for root in roots:
        visitor.visit(root)
    return Observation(True, "accepted", tuple(heads), tuple(spans))


def observe_regex(source: str) -> Observation:
    matches = tuple(LEXICAL_SCAN.finditer(source))
    return Observation(
        None,
        "lexical_scan_not_parser",
        tuple(match.group("head") for match in matches),
        tuple(Span("candidate_head", *match.span("head"), "unicode_characters") for match in matches),
    )


def mismatch(fixture: Fixture, observation: Observation) -> list[str]:
    result = []
    if observation.accepted is not None and observation.accepted != fixture.accepted:
        result.append("syntax_acceptance")
    if observation.status == "unsupported":
        result.append("unsupported_syntax")
    if fixture.heads is not None and observation.heads != fixture.heads:
        result.append("command_heads")
    return result
