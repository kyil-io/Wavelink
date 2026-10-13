from __future__ import annotations

import asyncio
import contextlib
import inspect
import socket
import weakref
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import aiohttp
import discord
from aiohttp import web


if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence

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


def build_ready_payload(session_id: str, *, resumed: bool = False) -> dict[str, Any]:
    return {"op": "ready", "resumed": resumed, "sessionId": session_id}


def build_player_update_payload(guild_id: str, *, time: int = 0, position: int = 0, ping: int = 0) -> dict[str, Any]:
    return {"op": "playerUpdate", "guildId": guild_id, "state": {"time": time, "position": position, "ping": ping}}


async def close_when_client_finishes(
    websocket: web.WebSocketResponse,
    *,
    waiting_for_client_close: asyncio.Event | None = None,
    client_close_observed: asyncio.Event | None = None,
    server_close_started: asyncio.Event | None = None,
) -> web.WebSocketResponse:
    if waiting_for_client_close is not None:
        waiting_for_client_close.set()
    while not websocket.closed:
        message = await websocket.receive()
        if message.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
            if client_close_observed is not None:
                client_close_observed.set()
            break
    if server_close_started is not None:
        server_close_started.set()
    await websocket.close()
    return websocket


async def wait_for_client_close(websocket: web.WebSocketResponse) -> web.WebSocketResponse:
    while not websocket.closed:
        message = await websocket.receive()
        if message.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
            break
    return websocket


def make_ready_websocket_handler(
    session_id: str, *, include_connection_number: bool = False, extra_payloads: Sequence[dict[str, Any]] = ()
) -> Callable[[web.Request, int], Awaitable[web.StreamResponse]]:
    async def websocket_handler(request: web.Request, number: int) -> web.StreamResponse:
        websocket = web.WebSocketResponse()
        await websocket.prepare(request)
        ready_session_id = f"{session_id}-{number}" if include_connection_number else session_id
        await websocket.send_json(build_ready_payload(ready_session_id))
        for payload in extra_payloads:
            await websocket.send_json(payload)
        return await wait_for_client_close(websocket)

    return websocket_handler


def make_pre_ready_close_then_handshake_failure_handler(
    *,
    handshake_status: int,
    reconnect_attempted: asyncio.Event | None = None,
    send_invalid_json_before_close: bool = False,
    wait_for_client_close: bool = False,
    wait_for_client_close_entered: asyncio.Event | None = None,
    client_close_observed: asyncio.Event | None = None,
    server_close_started: asyncio.Event | None = None,
) -> Callable[[web.Request, int], Awaitable[web.StreamResponse]]:
    async def websocket_handler(request: web.Request, number: int) -> web.StreamResponse:
        if number == 1:
            websocket = web.WebSocketResponse()
            await websocket.prepare(request)
            if send_invalid_json_before_close:
                await websocket.send_str("{")
            if wait_for_client_close:
                return await close_when_client_finishes(
                    websocket,
                    waiting_for_client_close=wait_for_client_close_entered,
                    client_close_observed=client_close_observed,
                    server_close_started=server_close_started,
                )
            await websocket.close()
            return websocket
        if reconnect_attempted is not None:
            reconnect_attempted.set()
        return web.Response(status=handshake_status)

    return websocket_handler


class EventRecorder:
    def __init__(self) -> None:
        self.counts: defaultdict[str, int] = defaultdict(int)
        self.payloads: defaultdict[str, list[tuple[Any, ...]]] = defaultdict(list)
        self._count_waiters: defaultdict[str, list[tuple[int, asyncio.Event]]] = defaultdict(list)
        self._count_hooks: defaultdict[str, list[tuple[int, Callable[[], None]]]] = defaultdict(list)
        self._waiter_count_hooks: defaultdict[str, list[tuple[int, Callable[[], None]]]] = defaultdict(list)

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
        waiter_registration = (minimum_count, waiter)
        pending_waiters = self._count_waiters[event]
        pending_waiters.append(waiter_registration)
        current_waiter_count = len(pending_waiters)
        pending_waiter_hooks = self._waiter_count_hooks[event]
        for minimum_waiters, callback in pending_waiter_hooks[:]:
            if current_waiter_count >= minimum_waiters:
                callback()
                pending_waiter_hooks.remove((minimum_waiters, callback))
        try:
            await asyncio.wait_for(waiter.wait(), timeout=timeout)
        finally:
            if waiter_registration in pending_waiters:
                pending_waiters.remove(waiter_registration)

    def on_count(self, event: str, minimum_count: int, callback: Callable[[], None]) -> None:
        if self.counts[event] >= minimum_count:
            callback()
            return
        self._count_hooks[event].append((minimum_count, callback))

    def on_waiter_count(self, event: str, minimum_waiters: int, callback: Callable[[], None]) -> None:
        if len(self._count_waiters[event]) >= minimum_waiters:
            callback()
            return
        self._waiter_count_hooks[event].append((minimum_waiters, callback))


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


@dataclass
class SameIdentifierConnectRace:
    first_gate: ConnectReturnGate
    second_gate: ConnectReturnGate
    first_task: asyncio.Task[object]
    second_task: asyncio.Task[object]

    async def wait_until_both_candidates_ready(self, *, timeout: float = 5.0) -> None:
        await asyncio.wait_for(self.first_gate.entered.wait(), timeout=timeout)
        await asyncio.wait_for(self.second_gate.entered.wait(), timeout=timeout)

    def release_first(self) -> None:
        self.first_gate.release()

    def release_second(self) -> None:
        self.second_gate.release()

    async def settle(self, *, timeout: float = 5.0) -> None:
        await asyncio.wait_for(self.first_task, timeout=timeout)
        await asyncio.wait_for(self.second_task, timeout=timeout)

    async def cancel_and_drain(self, *, timeout: float = 5.0) -> None:
        self.first_gate.release()
        self.second_gate.release()
        tasks = (self.first_task, self.second_task)
        for task in tasks:
            if task.done():
                continue
            task.cancel()

        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await asyncio.wait_for(task, timeout=timeout)


def begin_same_identifier_connect_race(
    *, first: FixtureNode, second: FixtureNode, client: discord.Client
) -> SameIdentifierConnectRace:
    import wavelink

    first_gate = ConnectReturnGate()
    second_gate = ConnectReturnGate()
    first.session_spy.connect_return_gate = first_gate
    second.session_spy.connect_return_gate = second_gate
    first_task = asyncio.create_task(wavelink.Pool.connect(nodes=[first.node], client=client))
    second_task = asyncio.create_task(wavelink.Pool.connect(nodes=[second.node], client=client))
    return SameIdentifierConnectRace(
        first_gate=first_gate,
        second_gate=second_gate,
        first_task=first_task,
        second_task=second_task,
    )


class LoopbackLavalink:
    def __init__(
        self,
        websocket_handler: Callable[[web.Request, int], web.StreamResponse | Awaitable[web.StreamResponse]],
    ) -> None:
        self._websocket_handler = websocket_handler
        self.request_count = 0
        self.info_count = 0
        self.uri = ""
        self._runner: web.AppRunner | None = None
        self._sock: socket.socket | None = None

    async def websocket(self, request: web.Request) -> web.StreamResponse:
        self.request_count += 1
        response = self._websocket_handler(request, self.request_count)
        if inspect.isawaitable(response):
            return await response
        return response

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


class LifecycleFixtureRig:
    def __init__(self) -> None:
        self.client: discord.Client | None = None
        self.events = EventRecorder()
        self.resources: list[FixtureNode] = []
        self._entered = False

    async def enter(self) -> None:
        import wavelink

        if self._entered:
            raise RuntimeError("LifecycleFixtureRig.enter() cannot be called while already entered")

        if wavelink.Pool.nodes:
            node_identifiers = tuple(wavelink.Pool.nodes)
            raise RuntimeError(f"Pool.nodes must be empty before lifecycle rig setup, found {node_identifiers}")

        self.client = discord.Client(intents=discord.Intents.none())
        install_offline_client_user(self.client)
        await self.client.__aenter__()
        self.events.attach(self.client)
        self._entered = True

    async def close(self) -> None:
        if not self._entered:
            return

        first_error: BaseException | None = None

        async def run(operation: Awaitable[Any]) -> None:
            nonlocal first_error
            try:
                await operation
            except BaseException as exc:
                if first_error is None:
                    first_error = exc

        for resource in tuple(reversed(self.resources)):
            await run(self.close_fixture(resource, suppress_errors=False))
        self.resources.clear()

        if self.client is not None:
            await run(asyncio.wait_for(self.client.__aexit__(None, None, None), timeout=5.0))
            self.client = None
        self._entered = False
        if first_error is not None:
            raise first_error

    async def close_fixture(
        self,
        fixture: FixtureNode,
        *,
        suppress_errors: bool = False,
        suppress_node_close_error: bool = False,
    ) -> None:
        import wavelink

        first_error: BaseException | None = None

        async def run(operation: Awaitable[Any], *, suppress_error: bool = False) -> None:
            nonlocal first_error
            try:
                await asyncio.wait_for(operation, timeout=5.0)
            except BaseException as exc:
                if suppress_error:
                    return
                if first_error is None:
                    first_error = exc

        await run(fixture.node.close(eject=True), suppress_error=suppress_node_close_error)
        await run(fixture.session_spy.real.close())
        await run(fixture.server.close())
        self.force_pool_remove(fixture.node)
        if fixture in self.resources:
            self.resources.remove(fixture)

        if first_error is None:
            registry: dict[str, wavelink.Node] = getattr(wavelink.Pool, "_Pool__nodes")
            if registry.get(fixture.node.identifier) is fixture.node:
                first_error = RuntimeError(f"fixture node leaked in Pool after close: {fixture.node.identifier}")

        if first_error is not None and not suppress_errors:
            raise first_error

    async def make_node(
        self,
        *,
        identifier: str,
        websocket_handler: Callable[[web.Request, int], Awaitable[web.StreamResponse]],
        retries: int | None = 1,
        managed: bool = True,
    ) -> FixtureNode:
        import wavelink

        if self.client is None:
            raise RuntimeError("LifecycleFixtureRig.enter() must be called before creating nodes")

        server = LoopbackLavalink(websocket_handler)
        await server.start()
        session_spy = SessionSpy(aiohttp.ClientSession())
        node = wavelink.Node(
            identifier=identifier,
            uri=server.uri,
            password="placeholder",
            session=cast("aiohttp.ClientSession", session_spy),
            heartbeat=30.0,
            retries=retries,
            client=self.client,
            resume_timeout=0,
        )
        fixture = FixtureNode(server=server, session_spy=session_spy, node=node)
        if managed:
            self.resources.append(fixture)
        return fixture

    @contextlib.asynccontextmanager
    async def unmanaged_node(
        self,
        *,
        identifier: str,
        websocket_handler: Callable[[web.Request, int], Awaitable[web.StreamResponse]],
        retries: int | None = 1,
    ) -> AsyncGenerator[FixtureNode, None]:
        fixture = await self.make_node(
            identifier=identifier,
            websocket_handler=websocket_handler,
            retries=retries,
            managed=False,
        )
        try:
            yield fixture
        finally:
            await self.close_fixture(fixture, suppress_node_close_error=True)

    @staticmethod
    def force_pool_remove(node: wavelink.Node) -> None:
        import wavelink

        registry: dict[str, wavelink.Node] = getattr(wavelink.Pool, "_Pool__nodes")
        if registry.get(node.identifier) is node:
            registry.pop(node.identifier, None)
