import json
from pathlib import Path

from codebench_pipeline.trace_payloads import load_payload, save_payload


def test_private_fields_are_withheld_before_persistence(tmp_path: Path) -> None:
    directory = tmp_path / "details"
    secret = "synthetic-credential"
    ref = save_payload(directory, {
        "choices": [{"message": {"content": "kept", "reasoning_content": "private reasoning"}}],
        "headers": {"Authorization": secret},
    })
    stored = (directory / ref).read_bytes()
    assert secret.encode() not in stored and b"private reasoning" not in stored
    assert json.loads(load_payload(directory, ref))["choices"][0]["message"]["content"] == "kept"
    assert (directory / ref).stat().st_mode & 0o077 == 0


def test_storage_or_decode_failure_is_not_a_fabricated_result(tmp_path: Path) -> None:
    ref = save_payload(tmp_path, b'{"status":"produced"}')
    (tmp_path / ref).write_text('{"status":"rejected"}')
    assert json.loads(load_payload(tmp_path, ref)) == {"capture_status": "payload_unavailable", "sha256": ref}
    assert json.loads(load_payload(None, ref))["capture_status"] == "payload_unavailable"
    for value, path, status in ((b"not JSON", tmp_path, "not_json"), (b"null", tmp_path / ref, "storage_error")):
        assert json.loads(load_payload(path, save_payload(path, value))) == {"capture_status": status}
