"""Local, read-only dashboard server (Starlette). Bound to 127.0.0.1; token-gated."""

from __future__ import annotations

import asyncio
import hmac
import json
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import (
    FileResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles
from starlette.types import ASGIApp, Receive, Scope, Send

from .state import digest

STATIC = Path(__file__).parent / "static"
COOKIE = "rslurm_dash"
CSP = (
    "default-src 'self'; img-src 'self' data:; style-src 'self'; script-src 'self'; "
    "font-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; "
    "form-action 'none'"
)
HEADERS = {
    "content-security-policy": CSP,
    "x-content-type-options": "nosniff",
    "referrer-policy": "no-referrer",
    "x-frame-options": "DENY",
    "cache-control": "no-store",
    "cross-origin-opener-policy": "same-origin",
    "cross-origin-resource-policy": "same-origin",
}
STATE_EVERY = 2.0
PING_EVERY = 15.0
LEASE_EVERY = 30.0
STREAM_ROUNDS: int | None = None  # tests bound the otherwise endless stream


class Guard:
    """Host check (DNS rebinding), token-for-cookie exchange, auth, and security headers."""

    def __init__(self, app: ASGIApp, *, token: str, port: int) -> None:
        self.app, self.token = app, token.encode()
        self.cookie = f"{COOKIE}_{port}"  # two dashboards on two ports don't log each other out
        self.hosts = {f"127.0.0.1:{port}", f"localhost:{port}"}

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        request = Request(scope)
        host = request.headers.get("host", "")
        if host not in self.hosts:
            await self.respond(
                PlainTextResponse("unexpected Host header", 421), scope, receive, send
            )
            return
        path = scope["path"]
        if path == "/healthz":
            await self.respond(JSONResponse({"ok": True}), scope, receive, send)
            return
        offered = request.query_params.get("token")
        if path == "/" and offered is not None:
            if hmac.compare_digest(offered.encode(), self.token):
                response = RedirectResponse("/", 303)
                response.set_cookie(
                    self.cookie,
                    self.token.decode(),
                    httponly=True,
                    samesite="strict",
                    path="/",
                    secure=False,
                )
                await self.respond(response, scope, receive, send)
            else:
                await self.respond(PlainTextResponse("invalid token", 403), scope, receive, send)
            return
        if not hmac.compare_digest(request.cookies.get(self.cookie, "").encode(), self.token):
            await self.respond(
                PlainTextResponse(
                    "Open the link printed by `rslurm dash` (it carries a one-time token).", 401
                ),
                scope,
                receive,
                send,
            )
            return
        if scope["method"] == "POST" and (
            request.headers.get("x-rslurm") != "1"
            or not request.headers.get("content-type", "").startswith("application/json")
        ):
            await self.respond(PlainTextResponse("forbidden", 403), scope, receive, send)
            return

        async def send_with_headers(message: Any) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                present = {k.decode().lower() for k, _ in headers}
                for key, value in HEADERS.items():
                    if key not in present:
                        headers.append((key.encode(), value.encode()))
                message = {**message, "headers": headers}
            await send(message)

        await self.app(scope, receive, send_with_headers)

    @staticmethod
    async def respond(response: Response, scope: Scope, receive: Receive, send: Send) -> None:
        for key, value in HEADERS.items():
            response.headers.setdefault(key, value)
        await response(scope, receive, send)


def create_app(provider: Any, *, token: str, port: int) -> Starlette:
    """``provider`` supplies state(), activity(after, limit), tail(...), files(...), poll(...)."""
    from anyio import to_thread

    async def index(request: Request) -> Response:
        return FileResponse(STATIC / "index.html", media_type="text/html")

    async def state(request: Request) -> Response:
        return JSONResponse(await to_thread.run_sync(provider.state))

    async def activity(request: Request) -> Response:
        after = request.query_params.get("after") or None
        try:
            limit = max(1, min(500, int(request.query_params.get("limit", "200"))))
        except ValueError:
            limit = 200
        try:
            page = await to_thread.run_sync(lambda: provider.activity(after, limit))
        except Exception:
            page = await to_thread.run_sync(lambda: provider.activity(None, limit))
            page["resync"] = True
        return JSONResponse(page)

    async def body(request: Request) -> dict:
        try:
            data = await request.json()
        except (ValueError, UnicodeDecodeError):
            return {}
        return data if isinstance(data, dict) else {}

    def job_args(data: dict) -> tuple[str, str] | None:
        host, job = data.get("host"), data.get("job")
        if not isinstance(host, str) or not isinstance(job, str) or not job.isdigit():
            return None
        return host, job

    async def tail(request: Request) -> Response:
        data = await body(request)
        args = job_args(data)
        which = data.get("which")
        task = data.get("task")
        if (
            args is None
            or which not in ("out", "err")
            or not (task is None or (isinstance(task, int) and task >= 0))
        ):
            return JSONResponse({"error": "invalid request"}, 400)
        return JSONResponse(await to_thread.run_sync(lambda: provider.tail(*args, which, task)))

    async def files(request: Request) -> Response:
        args = job_args(await body(request))
        if args is None:
            return JSONResponse({"error": "invalid request"}, 400)
        return JSONResponse(await to_thread.run_sync(lambda: provider.files(*args)))

    def poller(kind: str):
        async def poll(request: Request) -> Response:
            rid = request.path_params["rid"]
            if not rid.isalnum() or len(rid) > 64:
                return JSONResponse({"error": "invalid request"}, 400)
            return JSONResponse(await to_thread.run_sync(lambda: provider.poll(rid, kind)))

        return poll

    async def stream(request: Request) -> Response:
        cursor = request.query_params.get("after") or None

        async def events() -> AsyncIterator[str]:
            nonlocal cursor
            last_digest, last_ping = None, time.monotonic()
            yield "retry: 3000\n\n"
            rounds = 0
            while STREAM_ROUNDS is None or rounds < STREAM_ROUNDS:
                rounds += 1
                if await request.is_disconnected():
                    return
                current = await to_thread.run_sync(provider.state)
                key = digest(current)
                if key != last_digest:
                    last_digest = key
                    yield sse("state", current)
                try:
                    page = await to_thread.run_sync(provider.activity, cursor, 200)
                except Exception:
                    page = await to_thread.run_sync(lambda: provider.activity(None, 200))
                    page["resync"] = True
                if cursor is None:
                    cursor = page["cursor"]  # the page loaded history itself; only send news
                else:
                    if page["events"] or page.get("resync"):
                        yield sse("activity", page)
                    cursor = page["cursor"]
                if time.monotonic() - last_ping >= PING_EVERY:
                    last_ping = time.monotonic()
                    yield sse("ping", {"now": time.time()})
                await asyncio.sleep(STATE_EVERY)

        return StreamingResponse(
            events(),
            media_type="text/event-stream",
            headers={"cache-control": "no-store", "x-accel-buffering": "no"},
        )

    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        async def keepalive() -> None:
            while True:
                try:
                    await to_thread.run_sync(provider.keepalive)
                except Exception:
                    pass
                await asyncio.sleep(LEASE_EVERY)

        task = asyncio.create_task(keepalive())
        try:
            yield
        finally:
            task.cancel()

    from contextlib import asynccontextmanager

    return Starlette(
        routes=[
            Route("/", index),
            Route("/api/state", state),
            Route("/api/activity", activity),
            Route("/api/stream", stream),
            Route("/api/tail", tail, methods=["POST"]),
            Route("/api/tail/{rid}", poller("tail")),
            Route("/api/files", files, methods=["POST"]),
            Route("/api/files/{rid}", poller("list")),
            Mount("/static", StaticFiles(directory=STATIC, check_dir=False), name="static"),
        ],
        middleware=[Middleware(Guard, token=token, port=port)],
        lifespan=asynccontextmanager(lifespan),
    )


def sse(event: str, data: Any) -> str:
    payload = json.dumps(data, separators=(",", ":"), default=str)
    return f"event: {event}\ndata: {payload}\n\n"
