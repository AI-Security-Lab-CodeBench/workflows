from dataclasses import dataclass
from typing import Final, Literal, Self

from pydantic import model_validator

from codebench_pipeline.rules_contracts import Diagnostic, EvaluationInput, Gold
from codebench_pipeline.typed_config import ConfigArtifact


class NativeNode(ConfigArtifact):
    id: int
    parent: int
    kind: str
    operator: str = ""
    start: int
    end: int


class NativeWord(ConfigArtifact):
    start: int
    end: int
    raw: str
    value: str
    static: bool


class NativeCall(ConfigArtifact):
    start: int
    end: int
    words: tuple[NativeWord, ...]


class NativeAnalysis(ConfigArtifact):
    eligible: bool
    reasons: tuple[str, ...]
    nodes: tuple[NativeNode, ...]
    calls: tuple[NativeCall, ...]


class AnalysisRow(ConfigArtifact):
    id: str
    analysis: NativeAnalysis | None = None
    error: Diagnostic | None = None

    @model_validator(mode="after")
    def one_outcome(self) -> Self:
        if (self.analysis is None) == (self.error is None):
            raise ValueError("Expected one analysis outcome")
        return self


class AnalysisBatch(ConfigArtifact):
    runtime_version: str
    results: tuple[AnalysisRow, ...]


@dataclass(frozen=True, slots=True)
class Operation:
    documentation: str
    selectors: frozenset[str] = frozenset()
    filename_operands: bool = False


OPERATIONS: Final = {
    "awk": Operation("POSIX awk", frozenset({"-F", "-v", "-f"}), True),
    "cat": Operation("GNU coreutils cat", frozenset({"-n", "-b", "--number", "--number-nonblank"}), True),
    "date": Operation("GNU coreutils date", frozenset({"-u", "--utc", "--universal"})),
    "head": Operation("GNU coreutils head", frozenset({"-q", "--quiet", "--silent", "-v", "--verbose"}), True),
    "ls": Operation("GNU coreutils ls", frozenset({"-l", "-a", "-d", "-h", "-la", "-al", "--all"}), True),
    "pwd": Operation("GNU coreutils pwd", frozenset({"-L", "-P", "--logical", "--physical"})),
    "stat": Operation("GNU coreutils stat", frozenset({"-L", "--dereference", "-f", "--file-system"}), True),
    "wc": Operation("GNU coreutils wc", frozenset({"-c", "-l", "-m", "-w", "--bytes", "--lines"}), True),
}
_POSIX_PROGRAMS: Final = frozenset("""
    alias ar asa at basename bc bg break c99 cal cat cd cflow chgrp chmod chown cksum cmp comm command
    compress continue cp crontab csplit ctags cut date dd df diff dirname du echo ed env ex exec exit
    expand export expr false fc fg file find fold fuser gencat getconf getopts grep hash head iconv id
    ipcrm ipcs jobs join kill lex link ln locale localedef logger logname lp ls m4 mailx make man mesg
    mkdir mkfifo more mv newgrp nice nl nohup od paste patch pathchk pax pr printf ps pwd read readonly
    renice rm rmdir sed sh sleep sort split strings strip stty tabs tail talk tee test time touch tput
    tr trap true tsort tty type ulimit umask unalias uname uncompress unexpand uniq unlink unset
    uudecode uuencode vi wait wc what who write xargs yacc zcat awk
""".split())
_GNU_PROGRAMS: Final = frozenset("""
    base32 base64 basenc b2sum chcon chroot dir dircolors factor fmt groups install join md5sum
    mktemp nproc numfmt pinky printenv ptx readlink realpath seq sha1sum sha224sum sha256sum sha384sum
    sha512sum shred shuf stat stdbuf sum tac timeout truncate users vdir whoami yes
""".split())
_LINUX_PROGRAMS: Final = frozenset("""
    blkid column dmesg fallocate flock free getent hexdump hostname ip journalctl lsblk lscpu lsns
    mount namei nsenter partx pgrep pidof pkill prlimit ps pstree readprofile rev runuser setarch
    setpriv setsid ss su swapon switch_root systemctl taskset unshare uptime vmstat wall whereis
""".split())
PUBLIC_PROGRAMS: Final = {**dict.fromkeys(_POSIX_PROGRAMS, "POSIX utility"),
                          **dict.fromkeys(_GNU_PROGRAMS, "GNU coreutils"),
                          **dict.fromkeys(_LINUX_PROGRAMS, "Linux command"),
                          **{name: op.documentation.rsplit(" ", 1)[0] for name, op in OPERATIONS.items()}}
_RENAME: Final = ("codebench-probe-notes.txt", "codebench-probe-record.csv")


class ArgumentRole(ConfigArtifact):
    role: Literal[
        "program", "selector", "option_boundary", "stdin", "filename", "value", "opaque",
        "embedded_program", "program_file", "expansion",
    ]
    token: str | None = None


class StructuralNode(ConfigArtifact):
    id: int
    parent: int
    kind: str
    operator: str = ""


class CaptureRow(ConfigArtifact):
    id: str
    gold: Gold
    literal_cli_eligible: bool = False
    reasons: tuple[str, ...] = ()
    calls: tuple[tuple[ArgumentRole, ...], ...] = ()
    ast_nodes: tuple[StructuralNode, ...] = ()
    truncated: bool = False


def roles(call: NativeCall) -> tuple[ArgumentRole, ...]:
    if not call.words:
        return ()
    first, *arguments = call.words
    operation = OPERATIONS.get(first.value) if first.static else None
    if operation is None:
        program = (
            ArgumentRole(role="program", token=first.value) if first.static and first.value in PUBLIC_PROGRAMS
            else ArgumentRole(role="opaque" if first.static else "expansion")
        )
        return program, *(ArgumentRole(role="opaque" if word.static else "expansion") for word in arguments)
    result = [ArgumentRole(role="program", token=first.value)]
    is_awk = first.value == "awk"
    options = True
    understood = True
    awk_program = False
    awk_value: Literal["value", "program_file"] | None = None
    for word in arguments:
        value = word.value
        if awk_value is not None:
            item = ArgumentRole(role=awk_value if word.static else "expansion")
            awk_value = None
        elif not word.static:
            understood = False
            item = ArgumentRole(role="expansion")
        elif options and understood and value == "--":
            options = False
            item = ArgumentRole(role="option_boundary", token=value)
        elif is_awk and options and understood and value[:2] in operation.selectors:
            selector = value[:2]
            awk_program |= selector == "-f"
            value_role: Literal["value", "program_file"] = "program_file" if selector == "-f" else "value"
            if value == selector:
                awk_value = value_role
                item = ArgumentRole(role="selector", token=selector)
            else:
                item = ArgumentRole(role=value_role, token=selector)
        elif options and understood and value in operation.selectors:
            item = ArgumentRole(role="selector", token=value)
        elif is_awk and understood and not awk_program and (not options or not value.startswith("-")):
            awk_program = True
            options = False
            item = ArgumentRole(role="embedded_program")
        elif operation.filename_operands and understood and value == "-":
            item = ArgumentRole(role="stdin", token=value)
        elif operation.filename_operands and understood and value and (not options or not value.startswith("-")):
            options = options and not is_awk
            item = ArgumentRole(role="value" if is_awk and "=" in value else "filename")
        else:
            understood = False
            item = ArgumentRole(role="value")
        result.append(item)
    return tuple(result)


def allow_shape(analysis: AnalysisRow) -> tuple[str, ...] | None:
    doc = analysis.analysis
    if doc is None or not doc.eligible or len(doc.calls) != 1:
        return None
    arguments = roles(doc.calls[0])
    if not 1 <= len(arguments) <= 32 or arguments[0].token not in OPERATIONS or arguments[0].token == "awk":
        return None
    if any(item.role not in {"program", "selector", "option_boundary", "filename", "stdin"} for item in arguments):
        return None
    return tuple(item.token or "" for item in arguments)


def project_row(row_id: str, gold: Gold, analysis: AnalysisRow) -> CaptureRow:
    document = analysis.analysis
    if document is None:
        return CaptureRow(id=row_id, gold=gold, reasons=("native_parse_error",))
    return CaptureRow(
        id=row_id, gold=gold, literal_cli_eligible=document.eligible, reasons=document.reasons,
        calls=tuple(roles(call)[:32] for call in document.calls[:12]),
        ast_nodes=tuple(
            StructuralNode(id=node.id, parent=node.parent, kind=node.kind, operator=node.operator)
            for node in document.nodes[:96]
        ),
        truncated=len(document.nodes) > 96 or len(document.calls) > 12
        or any(len(call.words) > 32 for call in document.calls[:12]),
    )


def filename_probes(row: EvaluationInput, analysis: AnalysisRow) -> tuple[EvaluationInput, ...]:
    document = analysis.analysis
    if document is None:
        return ()
    words = sorted(
        (word for call in document.calls for word, role in zip(call.words, roles(call), strict=True)
         if role.role == "filename"),
        key=lambda word: word.start,
    )
    if not words:
        return ()
    original = row.command.encode()
    probes: list[EvaluationInput] = []
    for index, name in enumerate(_RENAME):
        if any(word.value == name for word in words):
            continue
        body = original
        for word in reversed(words):
            body = body[:word.start] + name.encode() + body[word.end:]
        probes.append(row.model_copy(update={"id": f"probe:{index}:{row.id}", "command": body.decode()}))
    return tuple(probes)
