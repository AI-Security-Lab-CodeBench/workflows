from bisect import bisect_left
from collections.abc import Iterator
from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser
from re import DOTALL, IGNORECASE, MULTILINE, compile
from shlex import split
from typing import Final

from codebench_pipeline.grounding_policy import version_constraints

_BLOCKS: Final = compile(
    rb"<(?P<tag>p|para|pre|screen|programlisting)(?:\s[^>]*)?>.*?</(?P=tag)\s*>|"
    rb"^ {0,3}```(?:bash|shell|sh|console)?[ \t]*\r?\n.*?^ {0,3}```[ \t]*(?=\r?$)",
    DOTALL | MULTILINE | IGNORECASE,
)
_PARAGRAPHS: Final = compile(rb"(?:\A|(?<=\n\n))(?=\S)[^\r\n]+(?:\r?\n(?!\r?$)[^\r\n]+)*", MULTILINE)
_INLINE: Final = compile(
    r"<(?P<tag>code|command|literal)(?:\s[^>]*)?>(?P<marked>.*?)</(?P=tag)\s*>|`(?P<markdown>[^`\n]+)`",
    DOTALL | IGNORECASE,
)
_TAGS: Final = compile(r"</?[a-z][^>]*>", IGNORECASE)
_EFFECT: Final = compile(
    r"\b(?:checks?|creates?|deletes?|displays?|enumerates?|lists?|previews?|prints?|"
    r"removes?|replaces?|reports?|restarts?|returns?|shows?|updates?|validates?|waits?)\b",
    IGNORECASE,
)
_VERSION_WORD: Final = compile(
    r"\b(?:versions?|releases?|deprecated|since|minimum|maximum|new in|"
    r"(?:old|older|legacy|newer|latest|current)\s+(?:client|server|cli)|"
    r"unsupported|incompatible|compatible|earlier|later|requires?|required|prerequisites?|"
    r"only\s+(?:if|for|on|with|when)|must\s+(?:be|have|use)|"
    r"does\s+not\s+(?:work|apply|support))\b",
    IGNORECASE,
)
_VERSION_VALUE: Final = compile(r"(?<![\w.])v?\d+\.\d+(?:\.\d+)*\b", IGNORECASE)
_DOCUMENT_SCOPE: Final = compile(
    r"\b(?:throughout|in|for|on|across)\s+(?:(?:this|the)(?:\s+entire)?\s+)?"
    r"(?:document|page|guide|reference|manual|article|documentation)\b"
    r"|\b(?:document|page|guide|manual)[ -]wide\b",
    IGNORECASE,
)
_ATX: Final = compile(rb"^ {0,3}(#{1,6})[ \t]+[^ \t\r\n]")
_FENCE: Final = compile(rb"^ {0,3}(`{3,}|~{3,})([^\r\n]*)")
_HTML_COMMENT: Final = compile(rb"<!--.*?(?:-->|\Z)", DOTALL)
_INLINE_CODE: Final = compile(rb"(`+)(?!`)[^\r\n]*?(?<!`)\1(?!`)")
_HTML_BLOCK: Final = compile(
    rb"<(?:html|body|main|article|section|div|p|pre|code|blockquote|aside|nav|ul|ol|li|table|script|style|template|"
    rb"h[1-6]|para|screen|programlisting)(?=[\s>/])",
    IGNORECASE,
)
_HTML_SECTIONS: Final = frozenset({"html", "body", "main", "article", "section", "div"})
_SECTIONING: Final = frozenset({"section", "article"})
_HTML_HEADINGS: Final = frozenset(f"h{level}" for level in range(1, 7))
_HTML_VOID: Final = frozenset(
    {
        "area",
        "base",
        "br",
        "col",
        "embed",
        "hr",
        "img",
        "input",
        "link",
        "meta",
        "param",
        "source",
        "track",
        "wbr",
    }
)
_SHELL_ANNOTATION: Final = compile(
    rb"(?m)^(?:[ \t]*#[ \t]*(?P<comment>[^\r\n]+)\r?\n)?(?P<line>[ \t]*(?:\$ )?[^\r\n#]+?)(?:[ \t]+#[ \t]*(?P<inline>[^\r\n]+))?[ \t]*(?=\r?$)"
)
_REPLACEABLE: Final = compile(r"<replaceable>([a-z][a-z0-9_-]*)</replaceable>", IGNORECASE)
_NAMED_OPERAND: Final = compile(
    r"(?:<(?P<angle>[a-z][a-z0-9_-]*)>|\{\{(?P<braces>[a-z][a-z0-9_-]*)\}\}|(?P<upper>[A-Z][A-Z0-9_]*))",
    IGNORECASE,
)
_ARGUMENT: Final = compile(r"[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}")
_FLAGS: Final = compile(r"(?<![\w-])--?[a-z][a-z0-9-]*(?![\w-])", IGNORECASE)
_OPERAND_NAMES: Final = frozenset(
    {
        "name",
        "resource-name",
        "pod",
        "pod-name",
        "deployment-name",
        "container-name",
        "container",
        "namespace",
        "namespace-name",
        "cluster",
        "cluster-name",
        "database",
        "database-name",
        "dbname",
        "bucket",
        "bucket-name",
        "image",
        "image-name",
        "service-name",
        "job-name",
        "project",
        "project-id",
        "region",
        "zone",
    }
)


@dataclass(frozen=True, slots=True)
class OperationPassage:
    start: int
    end: int
    tokens: tuple[str, ...]

    @property
    def route(self) -> tuple[str, ...]:
        if len(self.tokens) == 1 or self.tokens[1].startswith("-") or _operand_slot(self.tokens[1]):
            return self.tokens[:1]
        return self.tokens[:2]

    def matches(self, candidate: tuple[str, ...]) -> bool:
        if candidate == self.tokens:
            return True
        if len(candidate) != len(self.tokens) or candidate[: len(self.route)] != self.route:
            return False
        utility = self.tokens[0]
        if len(self.route) == 1 and utility not in {"psql", "dropdb", "pg_dump"}:
            return False
        if (
            utility in {"aws", "az", "gcloud", "kubectl", "docker"}
            and len(self.tokens) > 2
            and (
                _operand_slot(self.tokens[2])
                or self.tokens[2].startswith("-")
                or self.tokens[2] != candidate[2]
            )
        ):
            return False
        for documented, observed in zip(self.tokens, candidate, strict=True):
            if documented == observed:
                continue
            flag, separator, value = documented.partition("=")
            if separator:
                observed_flag, observed_separator, observed_value = observed.partition("=")
                if not flag.startswith("-") or (flag, separator) != (observed_flag, observed_separator):
                    return False
            else:
                value, observed_value = documented, observed
            if not _operand_slot(value) or not _ARGUMENT.fullmatch(observed_value):
                return False
        return True


def _operand_slot(word: str) -> bool:
    match = _NAMED_OPERAND.fullmatch(word)
    if match is None or (match["upper"] is not None and not word.isupper()):
        return False
    return (match["angle"] or match["braces"] or match["upper"]).lower().replace("_", "-") in _OPERAND_NAMES


def _invocation(command: str, start: int, end: int, effect: str) -> OperationPassage | None:
    try:
        tokens = tuple(split(command.removeprefix("$ ")))
    except ValueError:
        return None
    if (
        not tokens
        or not _ARGUMENT.fullmatch(tokens[0])
        or len(command) > 256
        or "\n" in command
        or not set(_FLAGS.findall(effect)) <= {token.partition("=")[0] for token in tokens}
    ):
        return None
    return OperationPassage(start, end, tokens)


def operation_passages(body: bytes) -> Iterator[OperationPassage]:
    """Yield exact byte spans only when surrounding prose does not restrict applicability."""
    restricted = Applicability(body)
    for passage in _operation_passages(body):
        if not restricted.overlaps(passage.start, passage.end):
            yield passage


def _operation_passages(body: bytes) -> Iterator[OperationPassage]:
    previous: tuple[int, int, str] | None = None
    for start, end in _passage_blocks(body):
        if end - start > 1200:
            previous = None
            continue
        raw = body[start:end].decode()
        plain = _visible(raw) if raw.startswith("<") else raw
        if _restricts(plain):
            previous = None
            continue
        if raw.lstrip().lower().startswith(("```", "<pre", "<screen", "<programlisting")):
            lines = plain.splitlines()
            if lines and lines[0].lstrip().startswith("```"):
                lines = lines[1:-1]
            invocations = [line.strip() for line in lines if line.strip()]
            if (
                previous is not None
                and not body[previous[1] : start].strip()
                and end - previous[0] <= 1200
                and len(invocations) == 1
                and (passage := _invocation(invocations[0], previous[0], end, previous[2])) is not None
            ):
                yield passage
            if raw.lstrip().startswith("```"):
                for line in _SHELL_ANNOTATION.finditer(body, start, end):
                    effect = line["comment"] or line["inline"]
                    if (
                        effect is not None
                        and _EFFECT.search(note := effect.decode())
                        and not _restricts(note)
                        and (
                            passage := _invocation(
                                line["line"].decode().strip(), line.start(), line.end(), note
                            )
                        )
                        is not None
                    ):
                        yield passage
            previous = None
            continue
        prose = unescape(_TAGS.sub("", _INLINE.sub("", raw)))
        inline_commands = tuple(
            (_visible(code["marked"]) if code["marked"] is not None else code["markdown"]).strip()
            for code in _INLINE.finditer(raw)
        )
        if (
            not prose.strip()
            and len(inline_commands) == 1
            and previous is not None
            and not body[previous[1] : start].strip()
            and end - previous[0] <= 1200
            and (passage := _invocation(inline_commands[0], previous[0], end, previous[2])) is not None
        ):
            yield passage
        if not _EFFECT.search(prose):
            previous = None
            continue
        if (
            len(inline_commands) == 1
            and (passage := _invocation(inline_commands[0], start, end, prose)) is not None
        ):
            yield passage
        previous = (start, end, plain)


def _visible(text: str) -> str:
    marked = _REPLACEABLE.sub(lambda item: "&lt;" + item[1] + "&gt;", text)
    return unescape(_TAGS.sub("", marked))


def _restricts(text: str) -> bool:
    return (
        version_constraints(text) != ()
        or _VERSION_WORD.search(text) is not None
        or _VERSION_VALUE.search(text) is not None
    )


class _HtmlSections(HTMLParser):
    def __init__(self, body: bytes) -> None:
        super().__init__()
        self.stack: list[tuple[str, int | None]] = []
        self.headings: list[tuple[int, int]] = []
        self.sections: list[tuple[int, int]] = []
        self.ambiguous = False
        self.lines = body.split(b"\n")
        offsets = [0]
        for line in self.lines:
            offsets.append(offsets[-1] + len(line) + 1)
        self.offsets = offsets
        self.feed(body.decode())
        self.close()

    def _offset(self) -> int:
        line, column = self.getpos()
        return self.offsets[line - 1] + len(self.lines[line - 1].decode()[:column].encode())

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in _HTML_VOID:
            return
        parents = tuple(parent for parent, _ in self.stack)
        if tag in _HTML_HEADINGS and any(parent in _HTML_HEADINGS for parent in parents):
            self.ambiguous = True
        structural = all(parent in _HTML_SECTIONS for parent in parents)
        if tag in _HTML_HEADINGS:
            structural = structural and not any(
                parent in _SECTIONING or parent == "div" for parent in parents
            )
        self.stack.append(
            (tag, self._offset() if structural and tag in (_HTML_HEADINGS | _SECTIONING) else None)
        )

    def handle_endtag(self, tag: str) -> None:
        if not self.stack or self.stack[-1][0] != tag:
            self.ambiguous = True
            return
        _, start = self.stack.pop()
        if start is not None:
            if tag in _SECTIONING:
                self.sections.append((start, self._offset()))
            else:
                self.headings.append((start, int(tag[1])))

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag not in _HTML_VOID:
            self.ambiguous = True


def _blanks(content: bytes) -> bytes:
    return bytes(byte if byte in (10, 13) else 32 for byte in content)


def _section_scopes(body: bytes) -> tuple[tuple[int, int], ...]:
    markup = _HTML_COMMENT.sub(lambda comment: _blanks(comment[0]), body)
    markdown: list[tuple[int, int]] = []
    html_input: list[bytes] = []
    has_html = False
    fence = b""
    offset = 0
    for line in markup.splitlines(keepends=True):
        boundary = _FENCE.match(line)
        if fence:
            html_input.append(_blanks(line))
            if boundary and boundary[1].startswith(fence) and not boundary[2].strip():
                fence = b""
        elif boundary:
            fence = boundary[1]
            html_input.append(_blanks(line))
        elif line.startswith((b"    ", b"\t")):
            html_input.append(line)
        else:
            visible = _INLINE_CODE.sub(lambda code: _blanks(code[0]), line)
            has_html |= _HTML_BLOCK.search(visible) is not None
            html_input.append(visible)
            if heading := _ATX.match(line):
                markdown.append((offset, len(heading[1])))
        offset += len(line)
    markup = b"".join(html_input)
    if not has_html:
        return _heading_scopes(markdown, len(body))
    if markdown:
        return ()
    try:
        html = _HtmlSections(markup)
    except AssertionError:
        return ()
    if html.ambiguous or html.stack:
        return ()
    return (*html.sections, *_heading_scopes(html.headings, len(body)))


def _heading_scopes(headings: list[tuple[int, int]], end: int) -> tuple[tuple[int, int], ...]:
    if any(level == 1 for _, level in headings[1:]):
        return ()
    scopes: list[tuple[int, int]] = []
    open_headings: list[tuple[int, int]] = []
    for start, level in [*headings, (end, 0)]:
        while open_headings and open_headings[-1][1] >= level:
            left, _ = open_headings.pop()
            scopes.append((left, start))
        open_headings.append((start, level))
    return tuple(scopes)


class Applicability:
    """Fail closed across the enclosing section, its descendants, or the full capture."""

    def __init__(self, body: bytes) -> None:
        scopes = sorted(_section_scopes(body), key=lambda span: (span[0], -span[1]))
        boundaries = sorted({0, len(body), *(point for span in scopes for point in span)})
        pending = iter(scopes)
        upcoming = next(pending, None)
        open_sections = [(0, len(body))]
        ranges: list[tuple[int, int]] = []
        for start, end in zip(boundaries, boundaries[1:]):
            while open_sections[-1][1] <= start:
                open_sections.pop()
            while upcoming is not None and upcoming[0] == start:
                open_sections.append(upcoming)
                upcoming = next(pending, None)
            text = unescape(body[start:end].decode())
            prose = (" ".join(_TAGS.sub(separator, text).split()) for separator in (" ", ""))
            restrictions = tuple(value for value in prose if _restricts(value))
            if any(_DOCUMENT_SCOPE.search(value) for value in restrictions):
                ranges = [(0, len(body))]
                break
            if restrictions:
                ranges.append(open_sections[-1])
        merged: list[tuple[int, int]] = []
        for left, right in sorted(ranges):
            if merged and left <= merged[-1][1]:
                merged[-1] = merged[-1][0], max(merged[-1][1], right)
            else:
                merged.append((left, right))
        self._starts = tuple(left for left, _ in merged)
        self._ends = tuple(right for _, right in merged)

    def overlaps(self, start: int, end: int) -> bool:
        nearest = bisect_left(self._starts, end) - 1
        return nearest >= 0 and start < self._ends[nearest]


def _passage_blocks(body: bytes) -> Iterator[tuple[int, int]]:
    html = any(
        marker in body.lower() for marker in (b"<html", b"<pre", b"<p>", b"<p ", b"<para>", b"<screen>")
    )
    previous = 0
    for block in _BLOCKS.finditer(body):
        if not html:
            chunk = body[previous : block.start()]
            for paragraph in _PARAGRAPHS.finditer(chunk):
                yield previous + paragraph.start(), previous + len(chunk[: paragraph.end()].rstrip())
        yield block.span()
        previous = block.end()
    if not html:
        chunk = body[previous:]
        for paragraph in _PARAGRAPHS.finditer(chunk):
            yield previous + paragraph.start(), previous + len(chunk[: paragraph.end()].rstrip())
