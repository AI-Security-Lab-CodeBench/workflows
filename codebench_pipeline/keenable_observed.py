from html.parser import HTMLParser
from re import fullmatch
from typing import Final, Literal
from urllib.parse import urlsplit

from pydantic import ValidationError

from .keenable_policy import COMMANDS, Case, KeenableError, Record, ReviewedSpan, digest
from .keenable_receipts import CallReceipt, ObservedPageLicense

_LICENSE: Final[Literal["https://creativecommons.org/licenses/by/4.0/"]] = (
    "https://creativecommons.org/licenses/by/4.0/"
)
_INERT: Final = frozenset({"template", "textarea", "title", "style", "script", "noscript", "iframe"})


class _Page(HTMLParser):
    def __init__(self, text: str) -> None:
        super().__init__(convert_charrefs=False)
        self.text = text
        self.lines = [0]
        for line in text.split("\n")[:-1]:
            self.lines.append(self.lines[-1] + len(line) + 1)
        self.main: list[tuple[int, int]] = []
        self.notices: list[tuple[str | None, str]] = []
        self.authors: set[str] = set()
        self.blocks: list[tuple[int, int, bool]] = []
        self.inert: list[str] = []
        self.main_start: int | None = None
        self.pre_start: int | None = None
        self.pre_shell = False
        self.head = False
        self.page_license: str | None = None
        self.ambiguous = False
        self.feed(text)
        self.close()

    def source_offset(self) -> int:
        line, column = self.getpos()
        return self.lines[line - 1] + column

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in _INERT:
            self.inert.append(tag)
        if self.inert:
            return
        attributes = dict(attrs)
        literal = self.get_starttag_text()
        assert literal is not None
        if tag == "head":
            self.head = True
        if tag == "body":
            self.head = False
        if tag == "main":
            if self.main_start is not None:
                self.ambiguous = True
            self.main_start = self.source_offset() + len(literal)
        if tag in {"a", "link", "area"} and any(
            key == "rel" and value and "license" in value.lower().split() for key, value in attrs
        ):
            href = attributes.get("href") if len(attributes) == len(attrs) else None
            self.notices.append((href, literal))
            if tag == "link" and self.head and href == _LICENSE and self.page_license is None:
                self.page_license = literal
        if tag == "meta" and self.head and (attributes.get("name") or "").lower() == "author":
            if author := " ".join((attributes.get("content") or "").split()):
                self.authors.add(author)
        if tag == "pre":
            self.pre_start = self.source_offset() + len(literal)
            self.pre_shell = False
        if tag == "code" and self.pre_start == self.source_offset():
            self.pre_shell = bool(
                set((attributes.get("class") or "").lower().split())
                & {"language-bash", "language-shell", "language-sh", "language-console"}
            )

    def handle_endtag(self, tag: str) -> None:
        if self.inert:
            if self.inert[-1] == tag:
                self.inert.pop()
            return
        offset = self.source_offset()
        if tag == "head":
            self.head = False
        if tag == "main" and self.main_start is not None:
            self.main.append((self.main_start, offset))
            self.main_start = None
        if tag == "pre" and self.pre_start is not None:
            self.blocks.append((self.pre_start, offset, self.pre_shell))
            self.pre_start = None


class LicensedContent(Record):
    content: str
    license: ObservedPageLicense

    @classmethod
    def replay(cls, receipt: CallReceipt, payload: bytes) -> tuple[bytes, ObservedPageLicense]:
        try:
            stored = cls.model_validate_json(payload)
        except ValidationError as error:
            raise KeenableError("origin_body_integrity") from error
        body = stored.content.encode()
        evidence = stored.license
        notice = _Page(f"<head>{evidence.notice}</head>")
        if (
            receipt.retained_sha256 != digest(payload)
            or receipt.response_sha256 is None
            or receipt.response_bytes is None
            or not body.strip()
            or evidence.retained_start + len(body) > receipt.response_bytes
            or evidence.notice_sha256 != digest(evidence.notice.encode())
            or notice.page_license != evidence.notice
        ):
            raise KeenableError("origin_body_integrity")
        return body, evidence


def licensed_main(text: str, url: str) -> LicensedContent | None:
    """Accept an explicit license for the document's main content and retain only that content."""
    page = _Page(text)
    notice = page.page_license
    if page.ambiguous or page.main_start is not None or len(page.main) != 1 or notice is None:
        return None
    if any(href != _LICENSE or len(raw) > 512 for href, raw in page.notices):
        return None
    attribution = ", ".join(sorted(page.authors)) or urlsplit(url).netloc
    attribution += "; unchanged excerpt"
    if len(attribution) > 256:
        return None
    start, end = page.main[0]
    retained = text[start:end]
    if not retained.strip():
        return None
    return LicensedContent(
        content=retained,
        license=ObservedPageLicense(
            license_url=_LICENSE,
            attribution=attribution,
            notice=notice,
            notice_sha256=digest(notice.encode()),
            retained_start=len(text[:start].encode()),
        ),
    )


def licensed_shell_spans(body: bytes, case: Case) -> tuple[ReviewedSpan, ...]:
    """Read literal administrative examples from explicit shell blocks or shell prompts."""
    text = body.decode()
    spans: list[ReviewedSpan] = []
    utilities = {command.split()[0] for command in COMMANDS[case]}
    previous = 0
    offset = 0
    for start, end, shell in _Page(text).blocks:
        if text[start:end].startswith("<code"):
            start = text.index(">", start, end) + 1
            if text[start:end].endswith("</code>"):
                end -= len("</code>")
        offset += len(text[previous:start].encode())
        for line in text[start:end].encode().splitlines(keepends=True):
            raw = line.strip(b" \t\r\n")
            literal = raw.removeprefix(b"$ ")
            if (shell or literal != raw) and 0 < len(literal) <= 256:
                command = literal.decode()
                if (
                    fullmatch(r"[\x20-\x7e]+", command)
                    and not any(marker in command for marker in "<>&")
                    and command.split()[0] in utilities
                ):
                    begin = offset + line.index(literal)
                    spans.append(ReviewedSpan(start=begin, end=begin + len(literal)))
                    if len(spans) == 20:
                        return tuple(spans)
            offset += len(line)
        previous = end
    return tuple(spans)
