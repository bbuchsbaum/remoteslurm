"""``rslurm dash``: make sure the observer runs, serve the page on 127.0.0.1, open a browser."""

from __future__ import annotations

import secrets
import socket
import sys
import threading
import time
import urllib.request
import webbrowser
from typing import Any

from ..errors import InvalidArgument, RemoteSlurmError


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def ensure_observer(config: Any, hosts: list[Any], *, restart: bool) -> list[str]:
    """Turn monitoring on, lease discovery for ``hosts``, and start (or restart) the daemon."""
    from .. import daemon
    from ..activity.discovery import lease
    from ..activity.monitor import control
    from ..identity import control_identity

    notes: list[str] = []
    control(True)
    lease(hosts)
    if daemon.daemon_available():
        current = daemon.daemon_status().get("build_id")
        if current != control_identity()["build_id"]:
            if restart:
                daemon.stop_daemon()
                for _ in range(100):
                    if not daemon.daemon_available():
                        break
                    time.sleep(0.1)
            else:
                notes.append(
                    "The background observer is running an older remoteslurm build, so this "
                    "page won't discover jobs submitted elsewhere. Restart it with: "
                    "rslurm dash --restart-daemon"
                )
    if not daemon.daemon_available() and not daemon.spawn_daemon(config_path=config.path):
        notes.append("The background observer could not start; the page shows cached state.")
    return notes


def wait_healthy(url: str, timeout: float = 10.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url + "/healthz", timeout=1) as response:
                if response.status == 200:
                    return True
        except OSError:
            time.sleep(0.1)
    return False


def run(
    config: Any,
    host: str | None,
    *,
    port: int | None = None,
    open_browser: bool = True,
    demo: bool = False,
    restart_daemon: bool = False,
) -> int:
    try:
        import uvicorn
    except ImportError as exc:  # pragma: no cover - declared dependency
        raise InvalidArgument(
            "the dashboard needs uvicorn and starlette", action="reinstall remoteslurm"
        ) from exc
    from .provider import DemoProvider, LiveProvider
    from .server import create_app

    if demo:
        provider: Any = DemoProvider()
        scope = "example data"
    else:
        if not config.hosts:
            raise InvalidArgument("no hosts are configured", action="run `rslurm init` first")
        hosts = [config.host(host)] if host else list(config.hosts.values())
        for note in ensure_observer(config, hosts, restart=restart_daemon):
            print(note, file=sys.stderr)
        provider = LiveProvider(config, hosts)
        scope = ", ".join(h.name for h in hosts)
    if port:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            try:
                probe.bind(("127.0.0.1", port))
            except OSError:
                raise InvalidArgument(
                    f"port {port} is already in use", action="pick another --port, or omit it"
                ) from None
    chosen = port or free_port()
    token = secrets.token_urlsafe(24)
    app = create_app(provider, token=token, port=chosen)
    base = f"http://127.0.0.1:{chosen}"
    url = f"{base}/?token={token}"
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=chosen, log_level="warning", access_log=False)
    )

    def announce() -> None:
        if not wait_healthy(base):
            print("dashboard did not start; see the error above", file=sys.stderr)
            return
        print(f"remoteslurm dash ({scope}) at {url}", flush=True)
        print("Press Ctrl-C to stop the page. Jobs and the observer keep running.", flush=True)
        if open_browser:
            webbrowser.open(url)

    threading.Thread(target=announce, daemon=True).start()
    try:
        server.run()
    except KeyboardInterrupt:
        pass
    except OSError as exc:
        raise RemoteSlurmError(f"could not serve on 127.0.0.1:{chosen}: {exc}") from exc
    return 0
