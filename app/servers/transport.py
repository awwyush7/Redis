import os
import socket
import tempfile
from dataclasses import dataclass


INTERNAL_TCP_HOST = "127.0.0.1"
SHARD_BASE_PORT = 9090
ORCHESTRATOR_PORT = 6379


@dataclass(frozen=True)
class Endpoint:
    kind: str
    host: str | None = None
    port: int | None = None
    path: str | None = None

    def describe(self) -> str:
        if self.kind == "unix":
            return self.path or "<missing unix socket path>"
        return f"{self.host}:{self.port}"


def unix_sockets_supported() -> bool:
    return os.name != "nt" and hasattr(socket, "AF_UNIX")


def _runtime_dir() -> str:
    runtime_dir = os.path.join(tempfile.gettempdir(), "pyredis-shared-nothing")
    os.makedirs(runtime_dir, exist_ok=True)
    return runtime_dir


def shard_endpoint(node: int) -> Endpoint:
    if unix_sockets_supported():
        return Endpoint(kind="unix", path=os.path.join(_runtime_dir(), f"shard-{node}.sock"))
    return Endpoint(kind="tcp", host=INTERNAL_TCP_HOST, port=SHARD_BASE_PORT + node)


def orchestrator_endpoint() -> Endpoint:
    if unix_sockets_supported():
        return Endpoint(kind="unix", path=os.path.join(_runtime_dir(), "orchestrator.sock"))
    return Endpoint(kind="tcp", host=INTERNAL_TCP_HOST, port=ORCHESTRATOR_PORT)


def cleanup_endpoint(endpoint: Endpoint) -> None:
    if endpoint.kind != "unix" or not endpoint.path:
        return

    try:
        os.unlink(endpoint.path)
    except FileNotFoundError:
        pass