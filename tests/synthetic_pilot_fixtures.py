import json
from collections.abc import Callable, Generator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

from codebench_pipeline.openai_transport import HTTPResponse


def completion(command: str, util: str = "terraform", class_name: str = "allow") -> HTTPResponse:
    content = json.dumps(
        {
            "candidates": [
                {
                    "command": command,
                    "util": util,
                    "description": "untrusted free text",
                    "class": class_name,
                    "severity": "low",
                }
            ]
        }
    )
    envelope = {
        "id": "fixture",
        "model": "local-alias",
        "choices": [
            {"index": 0, "finish_reason": "stop", "message": {"role": "assistant", "content": content}}
        ],
    }
    return HTTPResponse(json.dumps(envelope).encode(), None)


@contextmanager
def recording_chat_endpoint(response: Callable[[], HTTPResponse]) -> Generator[tuple[str, list[bytes]]]:
    requests: list[bytes] = []

    class ChatEndpoint(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            requests.append(self.rfile.read(int(self.headers["Content-Length"])))
            if self.path != "/v1/chat/completions" or self.headers.get("Authorization") is not None:
                self.send_error(400)
                return
            self.send_response(200)
            self.end_headers()
            self.wfile.write(response().body)

    with HTTPServer(("127.0.0.1", 0), ChatEndpoint) as server:
        worker = Thread(target=server.serve_forever)
        worker.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}/v1/", requests
        finally:
            server.shutdown()
            worker.join()
