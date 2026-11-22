from __future__ import annotations

import ssl
import unittest
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from http.client import HTTPException
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from unittest.mock import patch

from codebench_pipeline.repository_tree import fetch_repository_bytes
from codebench_pipeline.storage import IntegrityError

_OK = b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndocs"
_UNAVAILABLE = b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n"


@contextmanager
def _serve(*responses: bytes) -> Generator[tuple[str, list[tuple[str, int]]]]:
    requests: list[tuple[str, int]] = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            requests.append((self.path, self.client_address[1]))
            index = len(requests) - 1
            response = responses[min(index, len(responses) - 1)]
            if response:
                self.wfile.write(response)
            self.close_connection = not response or b"Connection: close\r\n" in response

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}/pinned", requests
        finally:
            server.shutdown()
            thread.join()


class RepositoryTransportTests(unittest.TestCase):
    def test_retries_disconnected_transport_and_unavailable_status(self) -> None:
        def midpoint(low: float, high: float) -> float:
            return (low + high) / 2

        with (
            _serve(b"", _UNAVAILABLE, _OK) as (url, requests),
            patch("time.sleep") as sleep,
            patch("random.uniform", side_effect=midpoint),
        ):
            self.assertEqual(fetch_repository_bytes(url, 4), b"docs")

        self.assertEqual([path for path, _ in requests], ["/pinned"] * 3)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [0.625, 1.25])

    def test_stops_retrying_after_four_attempts(self) -> None:
        truncated = b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nno"
        for response, error in ((_UNAVAILABLE, "503"), (truncated, "complete message body")):
            with (
                self.subTest(error=error),
                _serve(response) as (url, requests),
                patch("time.sleep") as sleep,
                patch("random.uniform", return_value=0),
            ):
                with self.assertRaisesRegex(HTTPException, error):
                    fetch_repository_bytes(url, 4)

            self.assertEqual(len(requests), 4)
            self.assertEqual([call.args[0] for call in sleep.call_args_list], [0.5, 1, 2])

    def test_waits_for_retry_after_within_the_cap(self) -> None:
        for header, delay in (
            (b"3", 3),
            (b"9" * 5000, 10),
            (b"Sun, 22 Nov 2026 12:00:06 GMT", 6),
            (b"Sun, 22 Nov 2026 11:59:57 GMT", 0.5),
            (b"invalid", 0.5),
        ):
            with (
                self.subTest(retry_after=header[:48]),
                _serve(
                    b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nRetry-After: "
                    + header
                    + b"\r\n\r\n",
                    _OK,
                ) as (url, requests),
                patch("time.sleep") as sleep,
                patch("random.uniform", return_value=0),
                patch("codebench_pipeline.repository_tree.datetime", wraps=datetime, create=True) as clock,
            ):
                clock.now.return_value = datetime(2026, 11, 22, 12, tzinfo=UTC)
                self.assertEqual(fetch_repository_bytes(url, 4), b"docs")
                self.assertEqual(len(requests), 2)
                sleep.assert_called_once_with(delay)

    def test_restarts_truncated_bodies_without_returning_partial_bytes(self) -> None:
        for framing in (b"Content-Length: 4\r\n\r\nno", b"Transfer-Encoding: chunked\r\n\r\n4\r\nno"):
            with (
                self.subTest(framing=framing),
                _serve(b"HTTP/1.1 200 OK\r\nConnection: close\r\n" + framing, _OK) as (url, requests),
                patch("time.sleep") as sleep,
            ):
                self.assertEqual(fetch_repository_bytes(url, 4), b"docs")
                self.assertEqual(len(requests), 2)
                sleep.assert_called_once()

    def test_permanent_http_failures_and_redirects_do_not_retry_or_follow(self) -> None:
        for status in (
            b"206 Partial Content",
            b"403 Forbidden",
            b"404 Not Found",
            b"501 Not Implemented",
            b"302 Found",
            b"307 Temporary Redirect",
        ):
            response = b"HTTP/1.1 " + status + b"\r\nContent-Length: 0\r\nLocation: /other\r\n\r\n"
            with (
                self.subTest(status=status),
                _serve(response, _OK) as (url, requests),
                patch("time.sleep") as sleep,
            ):
                with self.assertRaisesRegex(HTTPException, status[:3].decode()):
                    fetch_repository_bytes(url, 4)
                self.assertEqual([path for path, _ in requests], ["/pinned"])
                sleep.assert_not_called()

    def test_tls_certificate_failure_does_not_retry(self) -> None:
        with (
            _serve(_OK) as (url, requests),
            patch(
                "ssl.SSLContext.wrap_socket",
                side_effect=ssl.SSLCertVerificationError("untrusted certificate"),
            ),
            patch("time.sleep") as sleep,
        ):
            with self.assertRaisesRegex(HTTPException, "untrusted certificate"):
                fetch_repository_bytes(url.replace("http://", "https://"), 4)

        self.assertEqual(requests, [])
        sleep.assert_not_called()

    def test_oversized_body_does_not_retry(self) -> None:
        for response in (
            b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 5\r\n\r\nx",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nextra\r\n0\r\n\r\n",
        ):
            with (
                self.subTest(response=response),
                _serve(response, _OK) as (url, requests),
                patch("time.sleep") as sleep,
            ):
                with self.assertRaises(IntegrityError):
                    fetch_repository_bytes(url, 4)
                self.assertEqual(len(requests), 1)
                sleep.assert_not_called()

    def test_broken_chunk_framing_does_not_retry(self) -> None:
        with (
            _serve(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nxyz\r\n", _OK) as (url, requests),
            patch("time.sleep") as sleep,
        ):
            with self.assertRaises(HTTPException):
                fetch_repository_bytes(url, 4)

        self.assertEqual(len(requests), 1)
        sleep.assert_not_called()

    def test_reuses_the_connection_after_a_complete_capture(self) -> None:
        with _serve(_OK, _OK) as (url, requests):
            self.assertEqual(fetch_repository_bytes(url, 4), b"docs")
            self.assertEqual(fetch_repository_bytes(url + "-next", 4), b"docs")

        self.assertEqual([path for path, _ in requests], ["/pinned", "/pinned-next"])
        self.assertEqual(len({port for _, port in requests}), 1)
