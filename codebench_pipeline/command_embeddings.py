from __future__ import annotations

import os
from collections.abc import Callable, Sequence
from functools import lru_cache
from hashlib import sha256
from importlib import import_module
from pathlib import Path
from typing import Final, Protocol

import numpy as np
import numpy.typing as npt

ENCODER_DIRECTORY_ENV: Final = "CODEBENCH_COMMAND_ENCODER_DIR"
ENCODER_REVISION: Final = "751bff37182d3f1213fa05d7196b954e230abad9"
_TOKENIZER_SHA256: Final = "da0e79933b9ed51798a3ae27893d3c5fa4a201126cef75586296df9b4d2c62a0"
_MODEL_SHA256: Final = "afdb6f1a0e45b715d0bb9b11772f032c399babd23bfc31fed1c170afc848bdb1"
_TOKEN_LIMIT: Final = 256
_INFERENCE_BATCH_SIZE: Final = 32


class _Tokens(Protocol):
    @property
    def ids(self) -> list[int]: ...

    @property
    def attention_mask(self) -> list[int]: ...

    @property
    def type_ids(self) -> list[int]: ...


class _Tokenizer(Protocol):
    def enable_truncation(self, max_length: int) -> None: ...
    def enable_padding(self, *, pad_id: int, pad_token: str) -> None: ...
    def encode_batch(self, inputs: list[str]) -> Sequence[_Tokens]: ...


class _SessionOptions(Protocol):
    intra_op_num_threads: int
    inter_op_num_threads: int


class _Session(Protocol):
    def run(
        self, output_names: list[str], input_feed: dict[str, npt.NDArray[np.int64]]
    ) -> Sequence[object]: ...


class CommandEncoderError(RuntimeError):
    def __init__(self, problem: str) -> None:
        super().__init__(
            f"{problem}. Set {ENCODER_DIRECTORY_ENV} to the verified offline files for "
            f"Xenova/all-MiniLM-L6-v2 at {ENCODER_REVISION}"
        )


def _verified_bytes(path: Path, expected: str) -> bytes:
    try:
        contents = path.read_bytes()
    except OSError as error:
        raise CommandEncoderError(f"Cannot read encoder artifact {path}: {error.strerror}") from error
    if sha256(contents).hexdigest() != expected:
        raise CommandEncoderError(f"SHA-256 mismatch for encoder artifact {path}")
    return contents


@lru_cache(maxsize=1)
def _load_encoder(directory: Path) -> tuple[_Tokenizer, _Session]:
    tokenizer_bytes = _verified_bytes(directory / "tokenizer.json", _TOKENIZER_SHA256)
    model = _verified_bytes(directory / "onnx" / "model_quantized.onnx", _MODEL_SHA256)
    parse_tokenizer: Callable[[bytes], _Tokenizer] = import_module("tokenizers").Tokenizer.from_buffer
    tokenizer = parse_tokenizer(tokenizer_bytes)
    tokenizer.enable_truncation(max_length=_TOKEN_LIMIT)
    tokenizer.enable_padding(pad_id=0, pad_token="[PAD]")
    runtime = import_module("onnxruntime")
    options: _SessionOptions = runtime.SessionOptions()
    options.intra_op_num_threads = 1
    options.inter_op_num_threads = 1
    session: _Session = runtime.InferenceSession(
        model, sess_options=options, providers=["CPUExecutionProvider"]
    )
    return tokenizer, session


def encode_commands(commands: Sequence[str]) -> npt.NDArray[np.float32]:
    """Embed only command text, using masked mean pooling and L2 normalization."""
    directory = os.environ.get(ENCODER_DIRECTORY_ENV)
    return encode_from_directory(commands, Path(directory) if directory else None)


def encode_from_directory(commands: Sequence[str], directory: Path | None) -> npt.NDArray[np.float32]:
    if directory is None:
        raise CommandEncoderError("The offline command encoder directory is not configured")
    tokenizer, session = _load_encoder(directory.resolve())
    vectors = np.empty((len(commands), 384), dtype=np.float32)
    for start in range(0, len(commands), _INFERENCE_BATCH_SIZE):
        stop = start + _INFERENCE_BATCH_SIZE
        tokens = tokenizer.encode_batch(list(commands[start:stop]))
        attention_mask = np.asarray([token.attention_mask for token in tokens], dtype=np.int64)
        hidden_states = np.asarray(
            session.run(
                ["last_hidden_state"],
                {
                    "input_ids": np.asarray([token.ids for token in tokens], dtype=np.int64),
                    "attention_mask": attention_mask,
                    "token_type_ids": np.asarray([token.type_ids for token in tokens], dtype=np.int64),
                },
            )[0],
            dtype=np.float32,
        )
        weights = attention_mask.astype(np.float32)
        pooled = (hidden_states * weights[..., None]).sum(axis=1) / weights.sum(axis=1, keepdims=True)
        vectors[start:stop] = pooled / np.linalg.norm(pooled, axis=1, keepdims=True)
    return vectors
