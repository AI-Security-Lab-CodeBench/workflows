import socket
import unittest
from http.client import HTTPResponse
from unittest.mock import patch

from codebench_pipeline.keenable_policy import KeenableError
from codebench_pipeline.keenable_transport import HttpStatusError, _public_address, _read_response


def parse(response: bytes) -> bytes:
    left, right = socket.socketpair()
    with left, right:
        right.sendall(response)
        right.shutdown(socket.SHUT_WR)
        with HTTPResponse(left) as incoming:
            incoming.begin()
            return _read_response(incoming, "text/html")


class KeenableTransportTests(unittest.TestCase):
    def test_status_error_preserves_retry_after_without_reading_its_body(self) -> None:
        response = b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 120\r\nContent-Length: 6\r\n\r\n"
        with self.assertRaises(HttpStatusError) as raised:
            parse(response)
        self.assertEqual((raised.exception.status, raised.exception.retry_after), (429, 120))

    def test_identity_body_and_complete_chunked_body_produce_the_same_exact_bytes(self) -> None:
        header = b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n"
        self.assertEqual(parse(header + b"Content-Length: 5\r\n\r\nabcde"), b"abcde")
        self.assertEqual(
            parse(header + b"Transfer-Encoding: chunked\r\n\r\n2\r\nab\r\n3\r\ncde\r\n0\r\n\r\n"), b"abcde"
        )

    def test_origin_rejects_redirect_partial_encoding_bad_framing_and_oversize(self) -> None:
        success = b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n"
        variants = (
            ("http_status", b"HTTP/1.1 302 Found\r\nLocation: https://docs.docker.com/\r\n\r\n"),
            ("http_status", b"HTTP/1.1 206 Partial Content\r\nContent-Length: 3\r\n\r\nabc"),
            ("response_metadata", success + b"Content-Encoding: gzip\r\nContent-Length: 3\r\n\r\nabc"),
            ("response_framing", success + b"Content-Length: 3\r\nContent-Length: 3\r\n\r\nabc"),
            ("response_incomplete", success + b"Content-Length: 4\r\n\r\nabc"),
            ("response_incomplete", success + b"Transfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n"),
            ("response_too_large", success + b"Content-Length: 524289\r\n\r\n"),
        )
        for expected, response in variants:
            with self.subTest(expected=expected), self.assertRaisesRegex(KeenableError, expected):
                parse(response)

    def test_dns_mixed_public_and_private_answers_fail_closed(self) -> None:
        records = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, 443))
            for address in ("1.1.1.1", "127.0.0.1")
        ]
        with (
            patch("socket.getaddrinfo", return_value=records),
            self.assertRaisesRegex(KeenableError, "dns_nonpublic_address"),
        ):
            _public_address("docs.docker.com", 1)
