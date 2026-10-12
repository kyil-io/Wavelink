from __future__ import annotations

import asyncio
import socket
import unittest
import weakref
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import aiohttp
import discord
from aiohttp import web

import wavelink


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


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


class EventRecorder:
    def __init__(self) -> None:
        self.counts: defaultdict[str, int] = defaultdict(int)
        self.payloads: defaultdict[str, list[tuple[Any, ...]]] = defaultdict(list)

    def attach(self, client: discord.Client) -> None:
        original_dispatch = client.dispatch

        def dispatch_wrapper(event: str, *args: Any, **kwargs: Any) -> None:
            self.counts[event] += 1
            self.payloads[event].append(args)
            original_dispatch(event, *args, **kwargs)

        client.dispatch = dispatch_wrapper  # type: ignore[method-assign]


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


class TestWebsocketLifecycle(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        if wavelink.Pool.nodes:
            self.fail(f"test precondition violated: Pool.nodes must be empty, found {tuple(wavelink.Pool.nodes)}")

        self.client = discord.Client(intents=discord.Intents.none())
        install_offline_client_user(self.client)
        await self.client.__aenter__()
        self.addAsyncCleanup(self._cleanup_client_exit)

        self.events = EventRecorder()
        self.events.attach(self.client)

        self.resources: list[FixtureNode] = []
        self.addAsyncCleanup(self._cleanup_assert_no_owned_nodes)

    async def _cleanup_client_exit(self) -> None:
        await asyncio.wait_for(self.client.__aexit__(None, None, None), timeout=5.0)

    async def _cleanup_assert_no_owned_nodes(self) -> None:
        remaining = set(wavelink.Pool.nodes)
        owned = {resource.node.identifier for resource in self.resources}
        leaked_owned = remaining.intersection(owned)
        self.assertFalse(leaked_owned, f"owned nodes leaked in Pool after teardown: {sorted(leaked_owned)}")

    async def _cleanup_node_close(self, resource: FixtureNode) -> None:
        await asyncio.wait_for(resource.node.close(eject=True), timeout=5.0)

    async def _cleanup_session_close(self, resource: FixtureNode) -> None:
        await asyncio.wait_for(resource.session_spy.real.close(), timeout=5.0)

    async def _cleanup_server_close(self, resource: FixtureNode) -> None:
        await asyncio.wait_for(resource.server.close(), timeout=5.0)

    async def _cleanup_close_eject_with_expected_runtime_error(
        self,
        node: wavelink.Node,
        *,
        expected_message: str | None = None,
        expected_exception: RuntimeError | None = None,
    ) -> None:
        try:
            await asyncio.wait_for(node.close(eject=True), timeout=5.0)
        except BaseException as exc:
            if expected_exception is not None:
                self.assertIs(exc, expected_exception)
            elif expected_message is not None:
                self.assertIsInstance(exc, RuntimeError)
                self.assertIn(expected_message, str(exc))
            else:
                raise
        self.assertNotIn(node.identifier, wavelink.Pool.nodes)

    async def _make_node(
        self,
        *,
        identifier: str,
        websocket_handler: Callable[[web.Request, int], Awaitable[web.StreamResponse]],
        retries: int | None = 1,
        managed: bool = True,
    ) -> FixtureNode:
        server = LoopbackLavalink(websocket_handler)
        await server.start()

        session_spy = SessionSpy(aiohttp.ClientSession())
        node = wavelink.Node(
            identifier=identifier,
            uri=server.uri,
            password="local-password",
            session=cast("aiohttp.ClientSession", session_spy),
            heartbeat=30.0,
            retries=retries,
            client=self.client,
            resume_timeout=0,
        )
        resource = FixtureNode(server=server, session_spy=session_spy, node=node)
        if managed:
            self.resources.append(resource)
            self.addAsyncCleanup(self._cleanup_server_close, resource)
            self.addAsyncCleanup(self._cleanup_session_close, resource)
            self.addAsyncCleanup(self._cleanup_node_close, resource)
        return resource

    async def _wait_until(self, predicate: Callable[[], bool], *, timeout: float = 5.0) -> None:
        async def waiter() -> None:
            while not predicate():
                await asyncio.sleep(0)

        await asyncio.wait_for(waiter(), timeout=timeout)

    @staticmethod
    async def _pool_close_websocket_handler(request: web.Request, number: int) -> web.StreamResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.send_json({"op": "ready", "resumed": False, "sessionId": f"pool-close-{number}"})
        while not ws.closed:
            message = await ws.receive()
            if message.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
                break
        return ws

    async def test_pool_close_continues_after_node_close_failure_and_reraises_first_error(self) -> None:
        errors = [RuntimeError("first-close-failure"), RuntimeError("later-close-failure")]
        existing_cause = RuntimeError("existing-first-node-cause")
        errors[0].__cause__ = existing_cause
        fixtures: list[FixtureNode] = []
        for index, error in enumerate(errors):
            fixture = await self._make_node(
                identifier=f"pool-close-{index}", websocket_handler=self._pool_close_websocket_handler, managed=False
            )
            fixtures.append(fixture)
            self.resources.append(fixture)
            self.addAsyncCleanup(self._cleanup_server_close, fixture)
            self.addAsyncCleanup(self._cleanup_session_close, fixture)
            self.addAsyncCleanup(
                self._cleanup_close_eject_with_expected_runtime_error,
                fixture.node,
                expected_exception=error,
            )
        await asyncio.wait_for(
            wavelink.Pool.connect(nodes=[fixture.node for fixture in fixtures], client=self.client), timeout=5.0
        )
        await self._wait_until(
            lambda: all(
                fixture.node.status is wavelink.NodeStatus.CONNECTED and fixture.node.session_id is not None
                for fixture in fixtures
            )
        )
        gates = [TransportCloseGate(close_error=error) for error in errors]
        for fixture, gate in zip(fixtures, gates, strict=True):
            fixture.session_spy.close_gate = gate
            gate.disarm()
        sockets = [fixture.session_spy.latest_raw_websocket for fixture in fixtures]
        lifecycles = [fixture.node._websocket for fixture in fixtures]
        self.assertTrue(all(socket is not None for socket in sockets))
        self.assertTrue(all(lifecycle is not None for lifecycle in lifecycles))
        with self.assertRaises(RuntimeError) as raised:
            await asyncio.wait_for(wavelink.Pool.close(), timeout=5.0)
        self.assertIs(raised.exception, errors[0])
        self.assertEqual([gate.close_calls for gate in gates], [1, 1])
        self.assertEqual(
            [fixture.node.status for fixture in fixtures],
            [wavelink.NodeStatus.DISCONNECTED, wavelink.NodeStatus.DISCONNECTED],
        )
        for raw_socket in sockets:
            assert raw_socket is not None
            self.assertTrue(raw_socket.closed)
        for lifecycle in lifecycles:
            assert lifecycle is not None
            task = lifecycle.keep_alive_task
            self.assertTrue(task is None or task.done())
        for fixture in fixtures:
            self.assertIs(wavelink.Pool.nodes.get(fixture.node.identifier), fixture.node)
        self.assertIs(errors[0].__cause__, existing_cause)
        self.assertIs(existing_cause.__cause__, errors[1])
        self.assertIsNone(errors[1].__cause__)
        wavelink.Node._append_error_chain(errors[0], errors[1])
        self.assertIsNone(errors[1].__cause__, "re-appending the terminal error must not create a cycle")

    async def test_pool_close_does_not_convert_caller_cancellation(self) -> None:
        calls: list[str] = []
        registry: dict[str, Any] = getattr(wavelink.Pool, "_Pool__nodes")
        self.addCleanup(registry.clear)

        class CancellingNode:
            async def close(self) -> None:
                calls.append("cancel")
                raise asyncio.CancelledError

        class LaterNode:
            async def close(self) -> None:
                calls.append("later")

        registry["cancel"] = CancellingNode()
        registry["later"] = LaterNode()
        with self.assertRaises(asyncio.CancelledError):
            await wavelink.Pool.close()
        self.assertEqual(calls, ["cancel"])

    async def test_node_close_socket_failure_is_observable_and_still_ejects(self) -> None:
        async def websocket_handler(request: web.Request, _: int) -> web.StreamResponse:
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            await ws.send_json({"op": "ready", "resumed": False, "sessionId": "node-close-failure"})
            while not ws.closed:
                message = await ws.receive()
                if message.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
                    break
            return ws

        fixture = await self._make_node(
            identifier="node-close-failure",
            websocket_handler=websocket_handler,
            retries=1,
            managed=False,
        )
        try:
            await asyncio.wait_for(wavelink.Pool.connect(nodes=[fixture.node], client=self.client), timeout=5.0)
            await self._wait_until(
                lambda: fixture.node.status is wavelink.NodeStatus.CONNECTED and fixture.node.session_id is not None
            )
            fixture.session_spy.close_gate = TransportCloseGate(
                close_error=RuntimeError("synthetic-node-close-failure"),
            )
            fixture.session_spy.close_gate.disarm()

            with self.assertRaisesRegex(RuntimeError, "synthetic-node-close-failure"):
                await asyncio.wait_for(fixture.node.close(eject=True), timeout=5.0)

            self.assertEqual(fixture.node.status, wavelink.NodeStatus.DISCONNECTED)
            self.assertNotIn(fixture.node.identifier, wavelink.Pool.nodes)
            self.assertEqual(self.events.counts["wavelink_node_disconnected"], 1)
            self.assertEqual(self.events.counts["wavelink_node_closed"], 1)
        finally:
            await asyncio.wait_for(fixture.session_spy.real.close(), timeout=5.0)
            await asyncio.wait_for(fixture.server.close(), timeout=5.0)

    async def test_pool_close_handles_concurrent_eject_without_iteration_error(self) -> None:
        async def websocket_handler(request: web.Request, number: int) -> web.StreamResponse:
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            await ws.send_json({"op": "ready", "resumed": False, "sessionId": f"session-{number}"})
            while not ws.closed:
                message = await ws.receive()
                if message.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
                    break
            return ws

        fixture_a = await self._make_node(identifier="node-a", websocket_handler=websocket_handler, retries=1)
        fixture_b = await self._make_node(identifier="node-b", websocket_handler=websocket_handler, retries=1)

        await asyncio.wait_for(
            wavelink.Pool.connect(nodes=[fixture_a.node, fixture_b.node], client=self.client), timeout=5.0
        )
        await self._wait_until(
            lambda: all(
                fixture.node.status is wavelink.NodeStatus.CONNECTED and fixture.node.session_id is not None
                for fixture in (fixture_a, fixture_b)
            )
        )

        pool_close_task = asyncio.create_task(wavelink.Pool.close())
        eject_task = asyncio.create_task(fixture_a.node.close(eject=True))
        done, pending = await asyncio.wait({pool_close_task, eject_task}, timeout=5.0)

        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self.assertFalse(pending, "concurrent Pool.close + Node.close(eject=True) should complete")
        for task in done:
            exc = task.exception()
            if exc is not None:
                raise exc

        self.assertNotIn(fixture_a.node.identifier, wavelink.Pool.nodes)
        self.assertIn(fixture_b.node.identifier, wavelink.Pool.nodes)
        self.assertEqual(fixture_a.node.status, wavelink.NodeStatus.DISCONNECTED)
        self.assertEqual(fixture_b.node.status, wavelink.NodeStatus.DISCONNECTED)
