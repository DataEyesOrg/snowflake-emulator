"""Run the emulator inside a test process.

with running_server() as server:
    conn = snowflake.connector.connect(**server.connection_parameters())
"""

from __future__ import annotations

import contextlib
import socket
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import uvicorn

from sfemu.server.app import create_app


@dataclass(frozen=True)
class RunningServer:
    host: str
    port: int

    def connection_parameters(self, **overrides: Any) -> dict[str, Any]:
        """Keyword arguments for ``snowflake.connector.connect``."""
        parameters: dict[str, Any] = {
            "user": "test",
            "password": "test",
            "account": "test",
            "host": self.host,
            "port": self.port,
            "protocol": "http",
        }
        parameters.update(overrides)
        return parameters


@contextlib.contextmanager
def running_server(
    host: str = "127.0.0.1", port: int = 0, **app_options: Any
) -> Iterator[RunningServer]:
    """Serve a fresh, empty emulator on a background thread; port 0 picks a free port."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((host, port))
    app = create_app(**app_options)
    server = uvicorn.Server(uvicorn.Config(app, log_level="warning"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 10
        while not server.started:
            if not thread.is_alive() or time.monotonic() > deadline:
                raise RuntimeError("sfemu server failed to start")
            time.sleep(0.01)
        yield RunningServer(host, listener.getsockname()[1])
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        listener.close()
        app.state.engine.close()
