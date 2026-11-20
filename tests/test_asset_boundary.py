import subprocess
import sys
import textwrap
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]


def lint(**modules: str) -> subprocess.CompletedProcess[str]:
    with TemporaryDirectory() as directory:
        package = Path(directory) / "codebench_pipeline"
        package.mkdir()
        for name, body in modules.items():
            (package / f"{name}.py").write_text(textwrap.dedent(body).lstrip(), encoding="utf-8")
        return subprocess.run(
            [sys.executable, "-B", "-m", "scripts.asset_boundary", str(package)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )


class AssetBoundaryTests(unittest.TestCase):
    def test_stage_cannot_call_a_persister_hook_to_fetch_an_undeclared_input(self) -> None:
        result = lint(
            assets="""
            from boto3 import client as connect
            from dagster import ConfigurableIOManager, asset
            class Store(ConfigurableIOManager):
                def load_input(self, context):
                    return connect("s3").get_object(Bucket="fixture", Key="undeclared")
            @asset
            def transform(store: Store):
                return store.load_input(None)
        """
        )
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("stage access to durable object storage", result.stderr)
        self.assertIn("transform -> codebench_pipeline.assets.Store.load_input", result.stderr)

    def test_object_storage_is_only_allowed_from_framework_io_hooks(self) -> None:
        persister = """
            import boto3
            from dagster import ConfigurableIOManager
            def fetch():
                return boto3.client("s3").get_object(Bucket="fixture", Key="table")
            class Persister(ConfigurableIOManager):
                def load_input(self, context):
                    return fetch()
        """
        self.assertEqual(lint(persister=persister).returncode, 0)
        for group in ("configuration", "processing"):
            with self.subTest(group=group):
                result = lint(
                    persister=persister,
                    assets=f"""
                    from dagster import asset
                    from codebench_pipeline.persister import fetch
                    @asset(group_name="{group}")
                    def out():
                        return fetch()
                """,
                )
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("stage access to durable object storage", result.stderr)

    def test_labeling_checkpoint_requires_injection_and_fixed_generation_address(self) -> None:
        cache = """
            import re
            from codebench_pipeline.object_store import ObjectStore, StorageConfig
            def _checkpoint_digest(value):
                if re.fullmatch(r"[a-f0-9]{64}", value) is None:
                    raise ValueError("digest required")
                return value
            class LabelingCheckpointCache:
                root: str
                def resume(self, generation, batch):
                    generation, batch = _checkpoint_digest(generation), _checkpoint_digest(batch)
                    store = ObjectStore(StorageConfig(local_root=str(self.root), prefix='labeling-checkpoints-v1'))
                    return store.read(f"generations/{generation}/{batch}.json")
        """
        asset_source = """
            from concurrent.futures import ThreadPoolExecutor
            from dagster import ResourceParam, asset
            from codebench_pipeline.labeling_checkpoint_store import LabelingCheckpointCache
            def submit(callback):
                with ThreadPoolExecutor() as executor:
                    return executor.submit(callback).result()
            def annotation_assets():
                @asset
                def labeling_attempts(cache: ResourceParam[LabelingCheckpointCache]):
                    def resume():
                        return cache.resume("generation", "batch")
                    return submit(resume)
                return labeling_attempts
        """

        def scan(source: str, definition: str) -> subprocess.CompletedProcess[str]:
            return lint(dataset_assets_annotations=source, labeling_checkpoint_store=definition)

        allowed = scan(asset_source, cache)
        self.assertEqual((allowed.returncode, allowed.stderr), (0, ""))
        for source, definition in (
            (asset_source.replace("cache.resume", "LabelingCheckpointCache().resume"), cache),
            (asset_source, cache.replace('f"generations/{generation}/{batch}.json"', "generation")),
            (asset_source, cache.replace("local_root=str(self.root)", "local_root='arbitrary'")),
            (asset_source, cache.replace("[a-f0-9]{64}", ".*")),
            (asset_source.replace("def labeling_attempts", "def unrelated"), cache),
        ):
            with self.subTest(source=source, definition=definition):
                result = scan(source, definition)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("stage access to durable object storage", result.stderr)

    def test_local_io_requires_an_owned_root_and_a_validated_key_on_every_branch(self) -> None:
        source = """
            import os
            from pathlib import Path
            from dagster import ConfigurableIOManager

            def checked(key: str) -> str:
                if any(part in {"", ".", $PARENT} for part in key.split("/")):
                    raise ValueError(key)
                return key

            class Reader:
                def __init__(self, root: str):
                    self.root = root
                def read(self, key: str):
                    directory = Path(self.root).joinpath(checked(key))
                    fd = os.open(directory, os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY)
                    try:
                        leaf = os.open("value", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=fd)
                        with os.fdopen(leaf, "rb") as stream:
                            return stream.read()
                    finally:
                        os.close(fd)

            class Store(ConfigurableIOManager):
                root: str
                bound_reader: Reader | None
                def reader(self) -> Reader:
                    return self.bound_reader or Reader($ROOT)
                def load_input(self, context):
                    return self.reader().read(context.key)
        """
        for root, parent, accepted in (
            ("self.root", '".."', True),
            ('"ambient"', '".."', False),
            ("self.root", '"..."', False),
        ):
            with self.subTest(root=root, parent=parent):
                result = lint(assets=source.replace("$ROOT", root).replace("$PARENT", parent))
                if accepted:
                    self.assertEqual((result.returncode, result.stderr), (0, ""))
                else:
                    self.assertEqual(result.returncode, 1, result.stderr)
                    self.assertIn("ambient local file input", result.stderr)

    def assert_origin_pair(
        self, source: str, trusted: str, untrusted: str, diagnostics: int = 1, **modules: str
    ) -> None:
        accepted = lint(assets=source.replace("$ORIGIN", trusted), **modules)
        rejected = lint(assets=source.replace("$ORIGIN", untrusted), **modules)
        self.assertEqual((accepted.returncode, accepted.stderr), (0, ""))
        self.assertEqual(rejected.returncode, 1, rejected.stderr)
        self.assertEqual(len(rejected.stderr.splitlines()), diagnostics, rejected.stderr)
        for line in rejected.stderr.splitlines():
            self.assertIn("ambient local file input", line)

    def test_scratch_walk_and_open_relative_require_actual_owned_scratch(self) -> None:
        self.assert_origin_pair(
            """
            import os
            from pathlib import Path
            from tempfile import TemporaryDirectory as Scratch
            from dagster import ConfigurableResource, asset

            def retain(directory: Path):
                for _, _, files, parent in os.fwalk(directory, follow_symlinks=False):
                    for name in sorted(files):
                        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=parent)
                        with os.fdopen(fd, "rb") as stream:
                            return stream.read()

            class Store(ConfigurableResource):
                def retain(self, directory: Path):
                    return retain(directory)

            @asset
            def process(store: Store, caller_path: str):
                with Scratch() as own:
                    return store.retain(Path($ORIGIN))
            """,
            "own",
            "caller_path",
            diagnostics=3,
        )

    def test_step_receipts_walk_only_the_addressed_directory_descriptor(self) -> None:
        pipeline = ROOT / "codebench_pipeline"
        storage = (pipeline / "storage.py").read_text(encoding="utf-8")
        source = """
            import os
            from pathlib import Path
            from dagster import IOManager
            from codebench_pipeline.storage import open_directory, open_regular_file

            def checked(key):
                if any(part in {"", ".", ".."} for part in key.split("/")):
                    raise ValueError("Invalid receipt address")
                return key

            class Tables(IOManager):
                root: Path

                def handle_output(self, context, value):
                    directory = $ROOT
                    with open_directory(directory) as step_fd:
                        for _, _, files, parent in os.fwalk($TOP, dir_fd=$FD, follow_symlinks=$FOLLOW):
                            for name in files:
                                with open_regular_file(parent, name) as stream:
                                    return stream.read()
        """
        for label, overrides in (
            ("owned step address", {}),
            ("caller path", {"$ROOT": "Path(value)"}),
            ("raw descriptor", {"$FD": "value"}),
            ("absolute top overrides dir_fd", {"$TOP": '"/ambient"'}),
            ("parent traversal", {"$TOP": '".."'}),
            ("symlink traversal", {"$FOLLOW": "True"}),
        ):
            with self.subTest(label=label):
                fixture = source
                for key, default in {
                    "$ROOT": "self.root.joinpath(checked(Path(value).name))",
                    "$TOP": '"."',
                    "$FD": "step_fd",
                    "$FOLLOW": "False",
                }.items():
                    fixture = fixture.replace(key, overrides.get(key, default))
                result = lint(assets=fixture, storage=storage)
                self.assertEqual(result.returncode, int(bool(overrides)), result.stderr)
                if overrides:
                    self.assertIn("ambient local file input", result.stderr)

    def test_repository_descriptors_require_owned_root_and_checked_components(self) -> None:
        pipeline = ROOT / "codebench_pipeline"
        receipts = (pipeline / "repository_receipts.py").read_text(encoding="utf-8")
        config = (pipeline / "repository_config.py").read_text(encoding="utf-8")
        storage = (pipeline / "storage.py").read_text(encoding="utf-8")
        assets = """
            from pathlib import Path
            from dagster import ConfigurableResource, asset
            from codebench_pipeline.repository_receipts import RepositoryRun

            class Store(ConfigurableResource):
                home: str

                def run(self, dataset_id: str) -> RepositoryRun:
                    return RepositoryRun(Path(self.home), dataset_id, "test")

            class Evidence:
                @classmethod
                def inspect(cls, run: RepositoryRun, key: str):
                    run.write_evidence(key, b"payload")
                    return run.read_evidence(key)

            @asset
            def receipt(store: Store, dataset_id: str, key: str, caller_path: str):
                return Evidence.inspect(store.run(dataset_id), key)
            """
        accepted = lint(
            assets=assets, repository_receipts=receipts, repository_config=config, storage=storage
        )
        self.assertEqual(accepted.returncode, 0, accepted.stderr)
        self.assertEqual(accepted.stderr, "")

        def lines_for(source: str, *operations: str) -> set[int]:
            return {
                number
                for number, line in enumerate(source.splitlines(), 1)
                if any(operation in line for operation in operations)
            }

        anchor = "os.open(anchor"
        descent = "os.open(name, _DIRECTORY"
        leaf = "os.open(name, _READ_FILE"
        stream = 'os.fdopen(descriptor, "rb"'
        all_sites = lines_for(storage, anchor, descent, leaf, stream)
        for label, fixture, receipt_source, storage_source, expected in (
            (
                "caller-owned path",
                assets.replace(
                    "store.run(dataset_id)", 'RepositoryRun(Path(caller_path), dataset_id, "test")'
                ),
                receipts,
                storage,
                all_sites,
            ),
            (
                "unchecked relative key",
                assets,
                receipts.replace("repository_component(part)", "part"),
                storage,
                all_sites,
            ),
            (
                "unchecked dataset component",
                assets,
                receipts.replace("repository_component(self.dataset_id)", "pass"),
                storage,
                all_sites,
            ),
            (
                "descent without guaranteed nofollow",
                assets,
                receipts,
                storage.replace("os.open(name, _DIRECTORY,", "os.open(name, _DIRECTORY & ~os.O_NOFOLLOW,"),
                lines_for(storage, descent, leaf, stream),
            ),
            (
                "caller supplied descriptor",
                assets,
                receipts,
                storage.replace(
                    "os.open(name, _READ_FILE, dir_fd=directory)", "os.open(name, _READ_FILE, dir_fd=0)"
                ),
                lines_for(storage, leaf, stream),
            ),
            (
                "missing leaf type check",
                assets,
                receipts,
                storage.replace("if not S_ISREG(os.fstat(descriptor).st_mode):", "if False:"),
                lines_for(storage, stream),
            ),
        ):
            with self.subTest(label=label):
                rejected = lint(
                    assets=fixture,
                    repository_receipts=receipt_source,
                    repository_config=config,
                    storage=storage_source,
                )
                self.assertEqual(rejected.returncode, 1, rejected.stderr)
                findings = rejected.stderr.splitlines()
                for finding in findings:
                    self.assertIn("storage.py:", finding)
                    self.assertIn("ambient local file input", finding)
                self.assertEqual({int(line.split(":")[1]) for line in findings}, expected, rejected.stderr)

    def test_listdir_requires_a_pinned_directory_descriptor(self) -> None:
        storage = (ROOT / "codebench_pipeline" / "storage.py").read_text(encoding="utf-8")
        self.assert_origin_pair(
            """
            import os
            from pathlib import Path
            from tempfile import TemporaryDirectory
            from dagster import asset
            from codebench_pipeline.storage import open_directory

            def entries(fd: int):
                return os.listdir(path=fd)

            @asset
            def scan(arbitrary_fd: int):
                with TemporaryDirectory() as scratch:
                    with open_directory(Path(scratch) / "objects", create=True) as owned_fd:
                        return entries($ORIGIN)
            """,
            "owned_fd",
            "arbitrary_fd",
            storage=storage,
        )

    def test_package_reader_distinguishes_default_root_from_caller_override(self) -> None:
        source = """
            from dataclasses import dataclass
            from pathlib import Path
            from dagster import ConfigurableResource, asset

            @dataclass(frozen=True)
            class InstalledSource:
                root: Path = Path(__file__).parent

                def read(self, name: str):
                    return (self.root / name).read_bytes()

            class Runtime(ConfigurableResource):
                @property
                def source(self) -> InstalledSource:
                    return InstalledSource()

            @asset
            def process(runtime: Runtime, caller_path: str):
                return $ORIGIN
            """
        self.assert_origin_pair(
            source,
            'runtime.source.read("compute.py")',
            'InstalledSource(Path(caller_path)).read("compute.py")',
        )
        self.assert_origin_pair(
            source,
            'runtime.source.read("compute.py")',
            "runtime.source.read(caller_path)",
        )

    def test_hash_pinned_parser_requires_an_injected_installation_and_checked_relative_address(self) -> None:
        installed_source = (
            (ROOT / "codebench_pipeline" / "stage_implementation.py")
            .read_text(encoding="utf-8")
            .split("def processing_runtime", 1)[0]
        )
        storage = (ROOT / "codebench_pipeline" / "storage.py").read_text(encoding="utf-8")
        source = """
            import hashlib
            from pathlib import Path
            from dagster import ConfigurableResource, asset
            from codebench_pipeline.stage_implementation import ImplementationSource

            class Parsers(ConfigurableResource):
                @property
                def installation(self) -> ImplementationSource:
                    return ImplementationSource()

            def verify(name: str, reviewed_hash: str, source: ImplementationSource | None):
                reader = ImplementationSource() if source is None else source
                payload = reader.read(name)
                if hashlib.sha256(payload).hexdigest() != reviewed_hash:
                    raise ValueError("Installed parser changed")
                return payload

            @asset
            def process(parsers: Parsers, caller_path: str, reviewed_hash: str):
                return verify(caller_path, reviewed_hash, $SOURCE)
        """
        for label, reader, implementation, accepted in (
            ("bound installation", "parsers.installation", installed_source, True),
            ("ambient root", "ImplementationSource(Path(caller_path))", installed_source, False),
            ("unbound checkout", "None", installed_source, False),
            (
                "unchecked key",
                "parsers.installation",
                installed_source.replace("_installed_relative(name))", "name)"),
                False,
            ),
            (
                "unpacked string is not a sequence of checked path components",
                "parsers.installation",
                installed_source.replace(
                    "joinpath(_installed_relative(name))", "joinpath(*_installed_relative(name))"
                ),
                False,
            ),
        ):
            with self.subTest(label=label):
                result = lint(
                    assets=source.replace("$SOURCE", reader),
                    stage_implementation=implementation,
                    storage=storage,
                )
                self.assertEqual(result.returncode, 0 if accepted else 1, result.stderr)
                if not accepted:
                    self.assertIn("ambient local file input", result.stderr)

    def test_known_absent_receipt_directory_does_not_hide_unknown_directory_reads(self) -> None:
        self.assert_origin_pair(
            """
            from pathlib import Path
            from dagster import IOManager

            def retain(path: Path | None):
                return b"" if path is None else path.read_bytes()

            class Tables(IOManager):
                def handle_output(self, context, value):
                    return retain($ORIGIN)
            """,
            "None",
            "value",
        )

    def test_stored_loader_preserves_the_captured_resource_root(self) -> None:
        self.assert_origin_pair(
            """
            from pathlib import Path
            from tempfile import TemporaryDirectory
            from dagster import IOManager

            class Reader:
                def __init__(self, resolve):
                    self.resolve = resolve

                def load(self):
                    return self.resolve().read_bytes()

            class Tables(IOManager):
                def handle_output(self, context, value):
                    with TemporaryDirectory() as directory:
                        def address() -> Path:
                            return Path($ORIGIN) / "receipt.json"
                        return Reader(address).load()
            """,
            "directory",
            "value",
        )

    def test_resource_injection_does_not_authorize_raw_reads_in_the_asset(self) -> None:
        self.assert_origin_pair(
            """
            from pathlib import Path
            from dagster import ConfigurableResource, asset

            class Store(ConfigurableResource):
                base: str

                def read(self, key: str):
                    return (Path(self.base) / key).read_bytes()

            @asset
            def process(store: Store):
                return $ORIGIN
            """,
            'store.read("receipt.json")',
            '(Path(store.base) / "local-policy.json").read_bytes()',
        )

    def test_output_callback_receives_its_actual_staging_root(self) -> None:
        for constructor in ("context", "factory"):
            with self.subTest(constructor=constructor):
                allocate = "temporary" if constructor == "context" else "mkdtemp(dir=temporary)"
                self.assert_origin_pair(
                    f"""
                    from pathlib import Path
                    from tempfile import TemporaryDirectory, mkdtemp
                    from dagster import asset

                    def materialize(build, caller_path):
                        with TemporaryDirectory() as temporary:
                            directory = {allocate}
                            return build(Path($ORIGIN))

                    def inspect_output(path):
                        entries = tuple(path.iterdir())
                        return entries, (path / "report.json").read_bytes()

                    @asset
                    def report(caller_path):
                        def build(out):
                            return inspect_output(out)
                        return materialize(lambda out: build(out), caller_path)
                    """,
                    "directory",
                    "caller_path",
                    diagnostics=2,
                )

    def test_digest_verification_does_not_establish_path_containment(self) -> None:
        source = """
            import hashlib
            from pathlib import Path
            from tempfile import TemporaryDirectory
            from dagster import asset

            def verify(root, reference, expected):
                path = (root / reference).resolve()
                $CHECK
                payload = path.read_bytes()
                if hashlib.sha256(payload).hexdigest() != expected:
                    raise ValueError("Evidence bytes changed")
                return payload

            @asset
            def report(reference, expected, caller_path):
                with TemporaryDirectory() as directory:
                    return verify(Path($ORIGIN), reference, expected)
            """
        guarded = source.replace(
            "$CHECK",
            'if not path.is_relative_to(root.resolve()):\n                    raise ValueError("Outside workspace")',
        )
        self.assert_origin_pair(guarded, "directory", "caller_path")
        unchecked = lint(assets=source.replace("$CHECK", "").replace("$ORIGIN", "directory"))
        self.assertEqual(unchecked.returncode, 1, unchecked.stderr)
        self.assertIn("ambient local file input", unchecked.stderr)

    def test_owned_packet_listing_requires_nonlink_directory_and_bounded_pattern(self) -> None:
        source = """
            from pathlib import Path
            from tempfile import TemporaryDirectory
            from dagster import asset

            def commit(root, packet_path, pattern):
                rows = root / packet_path / "label"
                if not rows.is_relative_to(root / "packets") or rows.resolve() != rows:
                    raise ValueError("Outside packet workspace")
                return any(
                    directory.is_symlink()
                    or any(item.is_symlink() for item in directory.rglob($ORIGIN))
                    for directory in tuple(rows.iterdir())
                )

            @asset
            def report(packet_path, pattern):
                with TemporaryDirectory() as directory:
                    return commit(Path(directory), packet_path, pattern)
            """
        self.assert_origin_pair(source, '"*"', "pattern")
        unguarded = lint(
            assets=source.replace("directory.is_symlink()\n                    or any", "any").replace(
                "$ORIGIN", '"*"'
            ),
        )
        self.assertEqual(unguarded.returncode, 1, unguarded.stderr)
        self.assertIn("ambient local file input", unguarded.stderr)

    def test_dynamic_resource_address_needs_containment_before_open(self) -> None:
        source = """
            from pathlib import Path
            from dagster import ConfigurableResource, asset

            class Store(ConfigurableResource):
                base: str

                def read(self, path_key):
                    root = Path(self.base)
                    path = root.joinpath(*path_key.split("/")).resolve()
                    $CHECK
                    return path.read_bytes()

            @asset
            def report(store: Store, key):
                return store.read(key)
            """
        checked = source.replace(
            "$CHECK",
            'if not path.is_relative_to(root.resolve()):\n                        raise ValueError("Outside store")',
        )
        result = lint(assets=checked)
        self.assertEqual((result.returncode, result.stderr), (0, ""))
        result = lint(assets=source.replace("$CHECK", ""))
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("ambient local file input", result.stderr)

    def test_run_artifact_and_revision_pointer_require_resource_owned_roots(self) -> None:
        self.assert_origin_pair(
            """
            from dataclasses import dataclass
            from pathlib import Path
            from dagster import ConfigurableResource, asset

            @dataclass(frozen=True)
            class Artifacts:
                root: Path

                def run_output(self, run_id):
                    return (self.root / "produced" / run_id / "settings.json").read_bytes()

            class Revisions:
                def __init__(self, home: Path):
                    self.root = home / "versions"

                def digest(self, dataset_id):
                    return (self.root / dataset_id / "current").read_text()

            class Runtime(ConfigurableResource):
                base: str

                def inputs(self, run_id, dataset_id):
                    root = Path(self.base)
                    def load():
                        return read($ORIGIN, run_id, dataset_id)
                    return load()

            def read(root, run_id, dataset_id):
                return Artifacts(root).run_output(run_id), Revisions(root).digest(dataset_id)

            @asset
            def process(runtime: Runtime, run_id, dataset_id):
                return runtime.inputs(run_id, dataset_id)
            """,
            "root",
            "Path(run_id)",
            diagnostics=2,
        )

    def test_encoder_callback_uses_only_its_injected_artifact_directory(self) -> None:
        self.assert_origin_pair(
            """
            import os
            from functools import partial
            from pathlib import Path
            from dagster import ConfigurableResource, asset
            from .settings import ENCODER_SETTING as directory_key

            def encode(text, *, directory: Path | None):
                if directory is None:
                    raise RuntimeError("Missing encoder")
                return (directory.resolve() / "tokenizer.json").read_bytes()

            class Runtime(ConfigurableResource):
                def encoder(self, caller_path):
                    configured = os.environ.get(directory_key)
                    return partial(encode, directory=$ORIGIN)

            @asset
            def process(runtime: Runtime, caller_path):
                return runtime.encoder(caller_path)("example")
            """,
            "Path(configured) if configured else None",
            "Path(caller_path)",
            settings='ENCODER_SETTING = "CODEBENCH_COMMAND_ENCODER_DIR"',
        )

    def test_returned_resource_handle_carries_only_its_actual_root(self) -> None:
        self.assert_origin_pair(
            """
            from dataclasses import dataclass
            from pathlib import Path
            from dagster import ConfigurableResource, asset

            @dataclass(frozen=True)
            class Receipts:
                root: Path
                def read(self, key: str):
                    return (self.root / key).read_bytes()

            class Store(ConfigurableResource):
                base: str
                def run(self) -> Receipts:
                    return Receipts(Path(self.base))

            def bind(store: Store, root: str) -> tuple[int, Receipts]:
                return 0, $ORIGIN

            def load(run: Receipts, key: str):
                bound = Receipts(run) if isinstance(run, Path) else run
                return bound.read(key)

            @asset
            def process(store: Store, caller_path: str):
                _, run = bind(store, caller_path)
                return load(run, "receipt.json")
            """,
            "store.run()",
            "Receipts(Path(root))",
        )

    def test_fdopen_distinguishes_owned_descriptor_from_raw_caller_fd(self) -> None:
        self.assert_origin_pair(
            """
            import os
            from contextlib import ExitStack, contextmanager
            from pathlib import Path
            from dagster import ConfigurableResource, asset

            @contextmanager
            def directory(root: Path):
                fd = os.open(root, os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY)
                try:
                    yield fd
                finally:
                    os.close(fd)

            def read_file(parent: int):
                fd = os.open("receipt.json", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=parent)
                with os.fdopen(fd, "rb") as stream:
                    return stream.read()

            class Store(ConfigurableResource):
                base: str
                def read(self, caller_fd: int):
                    with ExitStack() as stack:
                        own = stack.enter_context(directory(Path(self.base)))
                        return read_file($ORIGIN)

            @asset
            def process(store: Store, caller_fd: int):
                return store.read(caller_fd)
            """,
            "own",
            "caller_fd",
            diagnostics=2,
        )

    def test_configuration_edges_addressed_io_and_output_writes_pass(self) -> None:
        result = lint(
            assets="""
                from dagster import ConfigurableResource, asset
                from pathlib import Path
                import os

                class Evidence(ConfigurableResource):
                    base: str

                    def read(self, digest):
                        if os.getenv("KEENABLE_API_KEY") is None:
                            raise RuntimeError("Credentials are required")
                        path = Path(self.base) / digest
                        return path.read_bytes()

                def uncalled():
                    return open("other.yaml").read()

                @asset(group_name="configuration")
                def settings():
                    return Path("business-configs/limits.json").read_text()

                @asset
                def report(settings, evidence: Evidence):
                    payload = evidence.read(settings)
                    Path("report.txt").write_bytes(payload)
                    with Path("report2.txt").open("wb") as stream:
                        stream.write(payload)
                    with open("report3.txt", mode="a") as stream:
                        stream.write(settings)
                    descriptor = os.open("report4.txt", os.O_CREAT | os.O_WRONLY)
                    os.close(descriptor)
                    return payload
            """,
        )
        self.assertEqual((result.returncode, result.stderr), (0, ""))

    def test_reports_a_transitive_file_read_through_an_aliased_factory(self) -> None:
        result = lint(
            assets="""
                import dagster as dg
                from .public_api import get_policy as project

                def factory(group):
                    def compute():
                        return project()

                    return dg.asset(group_name=group)(compute)
            """,
            public_api="from .facade import parse_declared as get_policy",
            facade="""
                from .loader import load_authored as load

                def parse_declared():
                    return load()
            """,
            loader="""
                from io import open as load_bytes

                def load_authored():
                    return load_bytes("taxonomy.json", mode="rb").read()
            """,
        )
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(len(result.stderr.splitlines()), 1, result.stderr)
        self.assertIn("loader.py:4: ambient local file input", result.stderr)
        for part in ("factory.compute", "facade.parse_declared", "loader.load_authored"):
            self.assertIn(part, result.stderr)

    def test_rejects_ambient_reads_before_asset_invocation(self) -> None:
        result = lint(
            assets="""
                import dagster as dg
                from .loader import LIMITS as declared

                @dg.asset
                def process(fallback=open("default.json").read()):
                    return declared or fallback
            """,
            loader="""
                import os
                LIMITS = os.environ["POLICY_OVERRIDES"]
            """,
        )
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(len(result.stderr.splitlines()), 2, result.stderr)
        self.assertIn("assets.py:5: ambient local file input", result.stderr)
        self.assertIn("loader.py:2: ambient environment", result.stderr)

    def test_rejects_local_reads_and_business_environment_reads(self) -> None:
        for imports, read, reason in (
            ("", 'open("rules.toml", "r+")', "ambient local file input"),
            ("from pathlib import Path", 'Path("rules.toml").resolve().open()', "ambient local file input"),
            ("from os import environ as settings", 'settings["MAX_ROWS"]', "ambient environment"),
            ("import os as runtime", 'runtime.getenv("MAX_ROWS")', "ambient environment"),
            ("from dagster import EnvVar", 'EnvVar.int("MAX_ROWS")', "ambient environment"),
        ):
            with self.subTest(read=read):
                result = lint(
                    assets=f"""
                    from dagster import asset
                    {imports}

                    @asset
                    def process():
                        return {read}
                """
                )
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn(f"assets.py:6: {reason}", result.stderr)

    def test_injected_resources_cannot_hide_business_settings_or_ambient_file_reads(self) -> None:
        result = lint(
            assets="""
                from dagster import ConfigurableResource, asset
                from pathlib import Path
                import os
                from .dataset_registry import DatasetRegistry

                class Store(ConfigurableResource):
                    base: str

                    def read(self, digest):
                        policy = Path(self.base, "private-policy.json").read_bytes()
                        registry = DatasetRegistry(self.base)
                        spec = registry.get_snapshot(digest)
                        return policy, spec, os.getenv("THRESHOLD")

                @asset
                def process(receipt_id, storage: Store):
                    return storage.read(receipt_id)
            """,
            dataset_registry="""
                class DatasetRegistry:
                    def get_snapshot(self, key):
                        raise AssertionError("The linter must not execute the input package")
            """,
        )
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(
            {row.split(": ", 1)[1].split(" via ", 1)[0] for row in result.stderr.splitlines()},
            {"ambient local file input", "registry business configuration", "ambient environment"},
        )

    def test_locally_constructing_a_resource_does_not_grant_io_authority(self) -> None:
        result = lint(
            assets="""
            from dagster import ConfigurableResource, asset
            from pathlib import Path

            class Store(ConfigurableResource):
                base: str

                def read(self, digest):
                    return (Path(self.base) / digest).read_bytes()

            @asset
            def process(receipt_id):
                storage = Store(base=".")
                return storage.read(receipt_id)
        """
        )
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("assets.py:8: ambient local file input", result.stderr)

    def test_framework_io_callbacks_cannot_substitute_ambient_policy_for_an_input(self) -> None:
        source = """
            import os
            from pathlib import Path

            from dagster import $IO_MANAGER, asset

            class Store($IO_MANAGER):
                base: str

                def load_input(self, context):
                    body = (Path(self.base) / context.asset_key.path[-1]).read_bytes()
                    return body[:int(os.getenv("MAX_ROWS", "5"))]

                def handle_output(self, context, obj):
                    (Path(self.base) / context.asset_key.path[-1]).write_bytes(obj)

            @asset(io_manager_key="store")
            def process():
                return b"example"
        """
        for manager in ("ConfigurableIOManager", "IOManager"):
            with self.subTest(manager=manager):
                result = lint(assets=source.replace("$IO_MANAGER", manager))
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertEqual(len(result.stderr.splitlines()), 1, result.stderr)
                self.assertIn("assets.py:11: ambient environment", result.stderr)
