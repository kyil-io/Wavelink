from __future__ import annotations

import asyncio
import socket
import weakref
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import aiohttp
import discord
from aiohttp import web


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    import wavelink


INFO_PAYLOAD: dict[str, object] = {
    "version": {"semver": "4.0.0", "major": 4, "minor": 0, "patch": 0},
    "buildTime": 0,
    "git": {"branch": "local", "commit": "local", "commitTime": 0},
    "jvm": "local",
    "lavaplayer": "local",
    "sourceManagers": [],
    "filters": [],
    "plugins": [],
}


def install_offline_client_user(client: discord.Client) -> None:
    payload: dict[str, Any] = {
        "id": "123456789012345678",
        "username": "local-test",
        "discriminator": "0000",
        "avatar": None,
        "bot": True,
        "system": False,
        "verified": True,
        "locale": "en-US",
        "mfa_enabled": False,
        "flags": 0,
    }
    client._connection.user = discord.ClientUser(
        state=client._connection,
        data=cast("Any", payload),
    )


class SessionSpy:
    def __init__(self, real: aiohttp.ClientSession) -> None:
        self.real = real
        self.ws_connect_calls = 0
        self.call_events: list[tuple[int, bool]] = []
        self.latest_wrapper_ref: weakref.ReferenceType[_TransparentClientWebSocket] | None = None
        self.latest_raw_websocket: aiohttp.ClientWebSocketResponse[bool] | None = None
        self.boundary_calls = 0
        self.boundary_marked = False
        self.post_boundary_call = asyncio.Event()
        self.close_gate: TransportCloseGate | None = None
        self.connect_return_gate: ConnectReturnGate | None = None
        self.connect_cancel_gate: ConnectCancelGate | None = None
        self.receive_gate: ReceiveCancelGate | None = None

    @property
    def closed(self) -> bool:
        return self.real.closed

    def ws_connect(self, *args: Any, **kwargs: Any) -> Awaitable[Any]:
        self.ws_connect_calls += 1
        self.call_events.append((self.ws_connect_calls, self.real.closed))

        if self.boundary_marked and self.ws_connect_calls > self.boundary_calls:
            self.post_boundary_call.set()

        return self._connect_and_wrap(*args, **kwargs)

    async def _connect_and_wrap(self, *args: Any, **kwargs: Any) -> Any:
        try:
            websocket = await self.real.ws_connect(*args, **kwargs)
        except asyncio.CancelledError:
            if self.connect_cancel_gate is None:
                raise
            self.connect_cancel_gate.entered.set()
            await self.connect_cancel_gate.release.wait()
            self.connect_cancel_gate.completed.set()
            raise
        wrapped = _TransparentClientWebSocket(websocket, self)
        self.latest_wrapper_ref = weakref.ref(wrapped)
        self.latest_raw_websocket = websocket
        if self.connect_return_gate is not None:
            await self.connect_return_gate.on_candidate_ready()
        return wrapped

    def mark_boundary(self) -> None:
        self.boundary_calls = self.ws_connect_calls
        self.boundary_marked = True

    def __getattr__(self, name: str) -> Any:
        return getattr(self.real, name)


class TransportCloseGate:
    def __init__(self, *, close_error: Exception | None = None, raise_before_close: bool = False) -> None:
        self.close_calls = 0
        self.entered = asyncio.Event()
        self._release = asyncio.Event()
        self._armed = True
        self._close_error = close_error
        self._raise_before_close = raise_before_close

    def arm(self) -> None:
        self._armed = True

    def disarm(self) -> None:
        self._armed = False

    def release(self) -> None:
        self._release.set()

    async def before_close(self) -> None:
        self.close_calls += 1
        if self._armed:
            self.entered.set()
            await self._release.wait()
        if self._close_error is not None and self._raise_before_close:
            raise self._close_error

    async def after_close(self) -> None:
        if self._close_error is not None and not self._raise_before_close:
            raise self._close_error


class ConnectReturnGate:
    def __init__(self, *, hold_on_cancel: bool = False) -> None:
        self.entered = asyncio.Event()
        self._release = asyncio.Event()
        self.cancel_entered = asyncio.Event()
        self._cancel_release = asyncio.Event()
        self._hold_on_cancel = hold_on_cancel

    def release(self) -> None:
        self._release.set()

    def release_cancel(self) -> None:
        self._cancel_release.set()

    async def on_candidate_ready(self) -> None:
        self.entered.set()
        try:
            await self._release.wait()
        except asyncio.CancelledError:
            self.cancel_entered.set()
            if self._hold_on_cancel:
                await self._cancel_release.wait()
            raise


class ConnectCancelGate:
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.completed = asyncio.Event()


class _TransparentClientWebSocket:
    def __init__(self, websocket: aiohttp.ClientWebSocketResponse[bool], session_spy: SessionSpy) -> None:
        self._websocket = websocket
        self._session_spy = session_spy

    async def close(self, *args: Any, **kwargs: Any) -> bool:
        if self._session_spy.close_gate is not None:
            await self._session_spy.close_gate.before_close()
        result = await self._websocket.close(*args, **kwargs)
        if self._session_spy.close_gate is not None:
            await self._session_spy.close_gate.after_close()
        return result

    async def receive(self, *args: Any, **kwargs: Any) -> Any:
        if self._session_spy.receive_gate is None:
            return await self._websocket.receive(*args, **kwargs)
        return await self._session_spy.receive_gate.receive(self._websocket.receive(*args, **kwargs))

    @property
    def closed(self) -> bool:
        return self._websocket.closed

    def __getattr__(self, name: str) -> Any:
        return getattr(self._websocket, name)


class ScriptedCloseSocket:
    def __init__(
        self, websocket: aiohttp.ClientWebSocketResponse[bool], *, close_without_closing_attempts: int
    ) -> None:
        self._websocket = websocket
        self._remaining_close_without_closing_attempts = close_without_closing_attempts
        self.close_calls = 0

    async def close(self, *args: Any, **kwargs: Any) -> bool:
        self.close_calls += 1
        if self._remaining_close_without_closing_attempts > 0:
            self._remaining_close_without_closing_attempts -= 1
            return True
        return await self._websocket.close(*args, **kwargs)

    async def receive(self, *args: Any, **kwargs: Any) -> Any:
        return await self._websocket.receive(*args, **kwargs)

    @property
    def closed(self) -> bool:
        return self._websocket.closed

    def __getattr__(self, name: str) -> Any:
        return getattr(self._websocket, name)


class EventRecorder:
    def __init__(self) -> None:
        self.counts: defaultdict[str, int] = defaultdict(int)
        self.payloads: defaultdict[str, list[tuple[Any, ...]]] = defaultdict(list)
        self._count_waiters: defaultdict[str, list[tuple[int, asyncio.Event]]] = defaultdict(list)
        self._count_hooks: defaultdict[str, list[tuple[int, Callable[[], None]]]] = defaultdict(list)

    def attach(self, client: discord.Client) -> None:
        original_dispatch = client.dispatch

        def dispatch_wrapper(event: str, *args: Any, **kwargs: Any) -> None:
            self.counts[event] += 1
            self.payloads[event].append(args)
            current_count = self.counts[event]
            pending_waiters = self._count_waiters[event]
            for minimum_count, waiter in pending_waiters[:]:
                if current_count >= minimum_count:
                    waiter.set()
                    pending_waiters.remove((minimum_count, waiter))
            pending_hooks = self._count_hooks[event]
            for minimum_count, callback in pending_hooks[:]:
                if current_count >= minimum_count:
                    callback()
                    pending_hooks.remove((minimum_count, callback))
            original_dispatch(event, *args, **kwargs)

        client.dispatch = dispatch_wrapper  # type: ignore[method-assign]

    async def wait_for_count(self, event: str, minimum_count: int, *, timeout: float = 5.0) -> None:
        if self.counts[event] >= minimum_count:
            return
        waiter = asyncio.Event()
        self._count_waiters[event].append((minimum_count, waiter))
        await asyncio.wait_for(waiter.wait(), timeout=timeout)

    def on_count(self, event: str, minimum_count: int, callback: Callable[[], None]) -> None:
        if self.counts[event] >= minimum_count:
            callback()
            return
        self._count_hooks[event].append((minimum_count, callback))


class ReceiveCancelGate:
    def __init__(self) -> None:
        self.cancel_entered = asyncio.Event()
        self.cancel_completed = asyncio.Event()
        self._release = asyncio.Event()

    def release(self) -> None:
        self._release.set()

    async def receive(self, receive_coro: Awaitable[Any]) -> Any:
        try:
            return await receive_coro
        except asyncio.CancelledError:
            self.cancel_entered.set()
            await self._release.wait()
            self.cancel_completed.set()
            raise


class CloseJoinBarrier:
    def __init__(self) -> None:
        self.first_call_entered = asyncio.Event()
        self.second_call_join_boundary = asyncio.Event()
        self.third_call_join_boundary = asyncio.Event()
        self._active_calls = 0
        self._total_calls = 0
        self._guard = asyncio.Lock()

    def install(self, node: wavelink.Node) -> None:
        original_close = node.close

        async def close_with_join_boundary(*args: Any, **kwargs: Any) -> None:
            second_call = False
            third_call = False
            async with self._guard:
                self._active_calls += 1
                self._total_calls += 1
                if self._active_calls == 1:
                    self.first_call_entered.set()
                if self._total_calls == 2:
                    second_call = True
                elif self._total_calls == 3:
                    third_call = True
            try:
                if second_call:
                    self.second_call_join_boundary.set()
                elif third_call:
                    self.third_call_join_boundary.set()
                await original_close(*args, **kwargs)
            finally:
                async with self._guard:
                    self._active_calls -= 1

        node.close = close_with_join_boundary  # type: ignore[method-assign]


class LoopbackLavalink:
    def __init__(self, websocket_handler: Callable[[web.Request, int], Awaitable[web.StreamResponse]]) -> None:
        self._websocket_handler = websocket_handler
        self.request_count = 0
        self.info_count = 0
        self.uri = ""
        self._runner: web.AppRunner | None = None
        self._sock: socket.socket | None = None

    async def websocket(self, request: web.Request) -> web.StreamResponse:
        self.request_count += 1
        return await self._websocket_handler(request, self.request_count)

    async def info(self, request: web.Request) -> web.Response:
        self.info_count += 1
        return web.json_response(INFO_PAYLOAD)

    async def start(self) -> None:
        app = web.Application()
        app.router.add_get("/v4/websocket", self.websocket)
        app.router.add_get("/v4/info", self.info)

        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(socket.SOMAXCONN)
        self._sock.setblocking(False)
        port = int(self._sock.getsockname()[1])
        await web.SockSite(self._runner, self._sock).start()
        self.uri = f"http://127.0.0.1:{port}"

    async def close(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
        if self._sock is not None:
            self._sock.close()


@dataclass
class FixtureNode:
    server: LoopbackLavalink
    session_spy: SessionSpy
    node: wavelink.Node
