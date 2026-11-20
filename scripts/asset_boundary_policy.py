"""Classify file and environment operations at the asset input boundary."""

import ast

from scripts.asset_boundary_index import Index, Scope

REGISTRY_READS = frozenset(
    {
        "codebench_pipeline.dataset_registry.DatasetRegistry.get",
        "codebench_pipeline.dataset_registry.DatasetRegistry.get_snapshot",
        "codebench_pipeline.dataset_registry.DatasetRegistry.location",
        "codebench_pipeline.dataset_registry.DatasetRegistry.keys",
    }
)
ENVIRONMENT = frozenset({"os.environ", "os.environb"})
ENV_GET = frozenset({"os.getenv", "os.getenvb", "os.environ.get", "os.environb.get"})
CREDENTIAL_SELECTORS = frozenset({"api_key_env", "model_api_key_env", "access_key_env", "secret_key_env"})
ENCODER_DIRECTORY = "CODEBENCH_COMMAND_ENCODER_DIR"
RUNTIME_ENV = frozenset({"KEENABLE_API_KEY", ENCODER_DIRECTORY})
FILE_OPEN = frozenset(
    {
        "builtins.open",
        "io.open",
        "pathlib.Path.open",
        "os.open",
        "os.fdopen",
        "gzip.open",
        "bz2.open",
        "lzma.open",
    }
)
URL_OPEN = frozenset({"urllib.request.build_opener.open", "urllib.request.OpenerDirector.open"})
FILE_READ = frozenset(
    {
        "pandas.read_csv",
        "pandas.read_json",
        "pandas.read_parquet",
        "polars.read_csv",
        "polars.read_json",
        "polars.read_ndjson",
        "polars.read_parquet",
        "polars.scan_csv",
        "polars.scan_parquet",
        "pyarrow.parquet.read_table",
        "glob.glob",
        "glob.iglob",
        "os.listdir",
        "os.scandir",
        "os.fwalk",
        "dotenv.load_dotenv",
        "dotenv.dotenv_values",
        "importlib.resources.files",
    }
)


def write_only(index: Index, scope: Scope, call: ast.Call, names: set[str]) -> bool:
    if "os.open" in names:
        flags = argument(call, 1, "flags")
        if flags is None:
            return False
        symbols = index.resolve(scope, flags)
        return "os.O_WRONLY" in symbols and not symbols & {"os.O_RDWR", "os.O_RDONLY"}
    position = (
        0 if isinstance(call.func, ast.Attribute) and not names & (FILE_OPEN - {"pathlib.Path.open"}) else 1
    )
    modes = index.resolve(scope, argument(call, position, "mode"))
    return bool(modes) and all(mode and mode[0] in "wax" and "+" not in mode for mode in modes)


def argument(call: ast.Call, position: int, name: str) -> ast.expr | None:
    return next(
        (part.value for part in call.keywords if part.arg == name),
        call.args[position] if position < len(call.args) else None,
    )


def file_input(index: Index, scope: Scope, call: ast.Call, names: set[str]) -> ast.expr | None:
    if isinstance(call.func, ast.Attribute):
        receiver = call.func.value
        match call.func.attr:
            case "read_text" | "read_bytes":
                return receiver
            case "glob" | "rglob" | "iterdir" if not names or "pathlib.Path" in index.resolve(
                scope, receiver
            ):
                return receiver
            case "open" if not names & (index.scopes.keys() | URL_OPEN | (FILE_OPEN - {"pathlib.Path.open"})):
                return None if write_only(index, scope, call, names) else receiver
            case _:
                pass
    if names & FILE_OPEN:
        return (
            None
            if write_only(index, scope, call, names)
            else (argument(call, 0, "file") or argument(call, 0, "path") or argument(call, 0, "fd") or call)
        )
    if any(name.rsplit(".", 1)[-1] in {"read_text", "read_bytes"} for name in names):
        return call
    if names & FILE_READ:
        address = (
            argument(call, 0, "filepath_or_buffer")
            or argument(call, 0, "source")
            or argument(call, 0, "top")
            or (argument(call, 0, "path") if names & {"os.listdir", "os.scandir"} else None)
        )
        if index.resolve(scope, address) & {"io.BytesIO", "io.StringIO", "pyarrow.BufferReader"}:
            return None
        return address or call
    return None


def resource_environment(index: Index, scope: Scope, key: ast.expr | None) -> bool:
    match key:
        case ast.Attribute(attr=name) | ast.Name(id=name) if name in CREDENTIAL_SELECTORS:
            return True
        case ast.Constant(value=str() as name):
            return name in RUNTIME_ENV
        case _:
            return bool(index.resolve(scope, key) & RUNTIME_ENV)
