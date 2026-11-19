import json
import os
import socket
import ssl
from contextlib import closing
from dataclasses import dataclass, field
from http.client import HTTPConnection, HTTPException, HTTPResponse
from ipaddress import IPv4Address
from queue import Empty, Queue
from re import fullmatch
from threading import Thread, Timer
from time import monotonic
from typing import Final
from urllib.parse import urlsplit

from .keenable_policy import MAX_RESULTS, PROVIDER, QUERIES, RESPONSE_BYTES, Case, KeenableError

TIMEOUT: Final = 30.0


@dataclass(frozen=True, slots=True)
class Request:
    url: str
    media_type: str
    payload: bytes | None = None
    credential: str = field(default="", repr=False)


@dataclass(frozen=True, slots=True)
class DirectResponse:
    body: bytes = field(repr=False)
    peer: str


def search_payload(base: str, scopes: tuple[str, ...]) -> bytes:
    query = (
        base + " (" + " OR ".join("site:" + scope.removeprefix("https://") for scope in scopes) + ")"
        if scopes
        else base
    )
    return json.dumps(
        {"query": query, "max_results": MAX_RESULTS, "snippet_max_length": 180},
        separators=(",", ":"),
    ).encode()


def search_request(case: Case, scopes: tuple[str, ...] = (), *, payload: bytes | None = None) -> Request:
    api_key = os.getenv("KEENABLE_API_KEY", "")
    if not fullmatch(r"[!-~]{1,512}", api_key):
        raise KeenableError("provider_credential_unavailable")
    return Request(PROVIDER, "application/json", payload or search_payload(QUERIES[case], scopes), api_key)


def _public_address(host: str, timeout: float) -> str:
    result: Queue[tuple[str, ...]] = Queue(maxsize=1)

    def resolve() -> None:
        try:
            records = socket.getaddrinfo(host, 443, socket.AF_INET, socket.SOCK_STREAM)
            result.put(tuple(str(record[4][0]) for record in records))
        except OSError:
            result.put(())

    Thread(target=resolve, daemon=True).start()
    try:
        addresses = tuple(IPv4Address(value) for value in result.get(timeout=timeout))
    except Empty:
        raise KeenableError("request_timeout") from None
    if not addresses:
        raise KeenableError("dns_empty")
    if any(not ip.is_global or ip.is_multicast or ip.is_reserved for ip in addresses):
        raise KeenableError("dns_nonpublic_address")
    return str(addresses[0])


def _remaining(deadline: float) -> float:
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise KeenableError("request_timeout")
    return remaining


def _read_response(response: HTTPResponse, expected: str) -> bytes:
    if response.status != 200:
        raise KeenableError("http_status", response.status)
    headers = response.headers
    lengths = headers.get_all("Content-Length", [])
    transfers = headers.get_all("Transfer-Encoding", [])
    encodings = headers.get_all("Content-Encoding", [])
    content_types = headers.get_all("Content-Type", [])
    if (
        headers.defects
        or headers.get_all("Content-Range")
        or [encoding.lower() for encoding in encodings] not in ([], ["identity"])
        or len(content_types) != 1
        or headers.get_content_type() != expected
        or headers.get_content_charset("utf-8") != "utf-8"
    ):
        raise KeenableError("response_metadata", response.status)
    if [transfer.lower() for transfer in transfers] == ["chunked"] and not lengths:
        return _read_chunks(response)
    if transfers or len(lengths) != 1 or not fullmatch(r"[0-9]{1,10}", lengths[0]):
        raise KeenableError("response_framing", response.status)
    length = int(lengths[0])
    if length > RESPONSE_BYTES:
        raise KeenableError("response_too_large", response.status)
    body = response.read(length)
    if not body or length != len(body):
        raise KeenableError("response_incomplete", response.status)
    return body


def _read_chunks(response: HTTPResponse) -> bytes:
    stream = response.fp
    body = bytearray()
    for _ in range(4096):
        line = stream.readline(19)
        if not fullmatch(rb"[0-9a-fA-F]{1,16}\r\n", line):
            raise KeenableError("response_framing", response.status)
        size = int(line, 16)
        if size > RESPONSE_BYTES - len(body):
            raise KeenableError("response_too_large", response.status)
        chunk = stream.read(size)
        if len(chunk) != size or stream.read(2) != b"\r\n":
            raise KeenableError("response_incomplete", response.status)
        if size == 0:
            if not body:
                raise KeenableError("response_incomplete", response.status)
            return bytes(body)
        body.extend(chunk)
    raise KeenableError("response_framing", response.status)


def request_direct(request: Request) -> DirectResponse:
    """Perform one verified, public, identity-encoded HTTPS request without proxies or retries."""
    deadline = monotonic() + TIMEOUT
    parts = urlsplit(request.url)
    host = parts.hostname
    if host is None:
        raise KeenableError("invalid_destination")
    peer = _public_address(host, _remaining(deadline))
    headers = {"Accept": request.media_type, "Accept-Encoding": "identity", "Connection": "close"}
    if request.payload is not None:
        headers.update({"Content-Type": "application/json", "X-API-Key": request.credential})
    context = ssl.create_default_context()
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as raw:
            raw.settimeout(_remaining(deadline))
            raw.connect((peer, 443))
            with (
                raw.dup() as interrupt,
                context.wrap_socket(raw, server_hostname=host, do_handshake_on_connect=False) as tls,
            ):
                timer = Timer(_remaining(deadline), interrupt.shutdown, args=(socket.SHUT_RDWR,))
                timer.daemon = True
                timer.start()
                try:
                    tls.settimeout(_remaining(deadline))
                    tls.do_handshake()
                    with closing(HTTPConnection(host)) as connection:
                        connection.sock = tls
                        connection.request(
                            "GET" if request.payload is None else "POST",
                            parts.path,
                            body=request.payload,
                            headers=headers,
                        )
                        with connection.getresponse() as response:
                            body = _read_response(response, request.media_type)
                    _remaining(deadline)
                    return DirectResponse(body, peer)
                finally:
                    timer.cancel()
                    timer.join()
    except (HTTPException, OSError) as error:
        _remaining(deadline)
        if isinstance(error, TimeoutError):
            raise KeenableError("request_timeout") from None
        raise KeenableError("transport_error") from None
