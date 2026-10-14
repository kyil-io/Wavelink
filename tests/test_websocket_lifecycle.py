from __future__ import annotations

import asyncio
import contextlib
import unittest
from typing import Any

import aiohttp
from aiohttp import web

import wavelink
from tests.lifecycle_fixtures import (
    ConnectReturnGate,
    FixtureNode,
    LifecycleFixtureRig,
    ReceiveCancelGate,
    ScriptedCloseSocket,
    TransportCloseGate,
    begin_same_identifier_connect_race,
    build_player_update_payload,
    make_pre_ready_close_then_handshake_failure_handler,
    make_ready_websocket_handler,
)
from wavelink.websocket import Websocket


class TestWebsocketLifecycle(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.rig = LifecycleFixtureRig()
        await self.rig.enter()
        self.addAsyncCleanup(self._cleanup_assert_pool_empty)
        self.addAsyncCleanup(self.rig.close)
        self.client = self.rig.client
        self.events = self.rig.events

    async def _cleanup_assert_pool_empty(self) -> None:
        self.assertFalse(wavelink.Pool.nodes, f"nodes leaked after teardown: {tuple(wavelink.Pool.nodes)}")

    async def _cleanup_unmanaged_fixture(self, fixture: FixtureNode) -> None:
        await self.rig.close_fixture(fixture, suppress_node_close_error=True)

    async def _make_unmanaged_node(
        self, *, identifier: str, websocket_handler: Any, retries: int | None = 1
    ) -> FixtureNode:
        return await self.rig.make_node(
            identifier=identifier, websocket_handler=websocket_handler, retries=retries, managed=False
        )

    async def _connect_node_and_get_lifecycle(self, fixture: FixtureNode) -> Any:
        ready_count = self.events.counts["wavelink_node_ready"] + 1
        await asyncio.wait_for(wavelink.Pool.connect(nodes=[fixture.node], client=self.client), timeout=5.0)
        await self.events.wait_for_count("wavelink_node_ready", ready_count)
        return fixture.node._websocket

    async def _await_task_if_pending(self, task: asyncio.Task[Any] | None, *, cancel: bool = False) -> None:
        if task is None or task.done():
            return
        if cancel:
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await asyncio.wait_for(task, timeout=5.0)

    async def _close_socket_if_open(self, socket: Any | None) -> None:
        if socket is None or socket.closed:
            return
        with contextlib.suppress(Exception):
            await asyncio.wait_for(socket.close(), timeout=5.0)

    async def _cleanup_close_eject_with_expected_runtime_error(
        self,
        fixture: FixtureNode,
        *,
        expected_message: str | None = None,
        expected_exception: RuntimeError | None = None,
    ) -> None:
        try:
            await self.rig.close_fixture(fixture, suppress_errors=False)
        except BaseException as exc:
            if expected_exception is not None:
                self.assertIs(exc, expected_exception)
            elif expected_message is not None:
                self.assertIsInstance(exc, RuntimeError)
                self.assertIn(expected_message, str(exc))
            else:
                raise
        self.assertNotIn(fixture.node.identifier, wavelink.Pool.nodes)

    async def test_pool_close_continues_after_node_close_failure_and_reraises_first_error(self) -> None:
        errors = [RuntimeError("first-close-failure"), RuntimeError("later-close-failure")]
        existing_cause = RuntimeError("existing-first-node-cause")
        errors[0].__cause__ = existing_cause
        fixtures: list[FixtureNode] = []
        for index, error in enumerate(errors):
            fixture = await self._make_unmanaged_node(
                identifier=f"pool-close-{index}",
                websocket_handler=make_ready_websocket_handler("pool-close", include_connection_number=True),
            )
            fixtures.append(fixture)
            self.addAsyncCleanup(
                self._cleanup_close_eject_with_expected_runtime_error, fixture, expected_exception=error
            )
        await asyncio.wait_for(
            wavelink.Pool.connect(nodes=[fixture.node for fixture in fixtures], client=self.client), timeout=5.0
        )
        await self.events.wait_for_count("wavelink_node_ready", 2)
        self.assertTrue(
            all(
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
        self.assertTrue(
            all(socket is not None for socket in sockets) and all(lifecycle is not None for lifecycle in lifecycles)
        )
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

    def test_append_error_chain_does_not_create_reverse_cause_cycle(self) -> None:
        for helper in (wavelink.Node._append_error_chain, Websocket._append_error_chain):
            with self.subTest(helper=helper.__qualname__):
                primary = RuntimeError("primary")
                appended = RuntimeError("appended")
                appended.__cause__ = primary
                helper(primary, appended)
                self.assertIsNone(primary.__cause__)
                self.assertIs(appended.__cause__, primary)

    async def test_pre_ready_transport_end_never_dispatches_disconnect_and_observes_keep_alive_failure(self) -> None:
        for mode in ("closed", "invalid-json"):
            with self.subTest(mode=mode):
                fixture = await self._make_unmanaged_node(
                    identifier=f"pre-ready-{mode}",
                    websocket_handler=make_pre_ready_close_then_handshake_failure_handler(
                        handshake_status=200, send_invalid_json_before_close=(mode == "invalid-json")
                    ),
                )
                try:
                    disconnected_before = self.events.counts["wavelink_node_disconnected"]
                    await asyncio.wait_for(wavelink.Pool.connect(nodes=[fixture.node], client=self.client), timeout=5.0)
                    lifecycle = fixture.node._websocket
                    assert lifecycle is not None and lifecycle.keep_alive_task is not None
                    await asyncio.wait_for(asyncio.wait({lifecycle.keep_alive_task}), timeout=5.0)
                    with contextlib.suppress(Exception):
                        await asyncio.wait_for(fixture.node.close(eject=True), timeout=5.0)
                    self.assertEqual(self.events.counts["wavelink_node_disconnected"] - disconnected_before, 0)
                    if mode == "invalid-json":
                        self.assertFalse(getattr(lifecycle.keep_alive_task, "_log_traceback", False))
                finally:
                    await self._cleanup_unmanaged_fixture(fixture)

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
        fixture = await self._make_unmanaged_node(
            identifier="node-close-failure",
            websocket_handler=make_ready_websocket_handler("node-close-failure"),
            retries=1,
        )
        try:
            await asyncio.wait_for(wavelink.Pool.connect(nodes=[fixture.node], client=self.client), timeout=5.0)
            await self.events.wait_for_count("wavelink_node_ready", 1)
            self.assertEqual(fixture.node.status, wavelink.NodeStatus.CONNECTED)
            self.assertIsNotNone(fixture.node.session_id)
            fixture.session_spy.close_gate = TransportCloseGate(
                close_error=RuntimeError("synthetic-node-close-failure")
            )
            fixture.session_spy.close_gate.disarm()
            with self.assertRaisesRegex(RuntimeError, "synthetic-node-close-failure"):
                await asyncio.wait_for(fixture.node.close(eject=True), timeout=5.0)
            self.assertEqual(fixture.node.status, wavelink.NodeStatus.DISCONNECTED)
            self.assertNotIn(fixture.node.identifier, wavelink.Pool.nodes)
            self.assertEqual(self.events.counts["wavelink_node_disconnected"], 1)
            self.assertEqual(self.events.counts["wavelink_node_closed"], 1)
        finally:
            await self._cleanup_unmanaged_fixture(fixture)

    async def test_pool_close_handles_concurrent_eject_without_iteration_error(self) -> None:
        websocket_handler = make_ready_websocket_handler("session", include_connection_number=True)
        fixture_a = await self.rig.make_node(identifier="node-a", websocket_handler=websocket_handler, retries=1)
        fixture_b = await self.rig.make_node(identifier="node-b", websocket_handler=websocket_handler, retries=1)
        await asyncio.wait_for(
            wavelink.Pool.connect(nodes=[fixture_a.node, fixture_b.node], client=self.client), timeout=5.0
        )
        await self.events.wait_for_count("wavelink_node_ready", 2)
        self.assertTrue(
            all(
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
        self.assertFalse(pending)
        for task in done:
            exc = task.exception()
            if exc is not None:
                raise exc
        self.assertNotIn(fixture_a.node.identifier, wavelink.Pool.nodes)
        self.assertIn(fixture_b.node.identifier, wavelink.Pool.nodes)
        self.assertEqual(fixture_a.node.status, wavelink.NodeStatus.DISCONNECTED)
        self.assertEqual(fixture_b.node.status, wavelink.NodeStatus.DISCONNECTED)

    async def test_cleanup_cancels_and_drains_published_reconnect_without_opening_replacement_socket(self) -> None:
        fixture = await self._make_unmanaged_node(
            identifier="retained-reconnect-owner-c",
            websocket_handler=make_ready_websocket_handler("retained-reconnect", include_connection_number=True),
            retries=None,
        )
        original_ws_connect: Any | None = None
        try:
            lifecycle = await self._connect_node_and_get_lifecycle(fixture)
            primary_socket = lifecycle.socket
            assert primary_socket is not None
            reconnect_connect_entered = asyncio.Event()
            reconnect_cancel_entered = asyncio.Event()
            reconnect_cancel_release = asyncio.Event()
            reconnect_attempts = 0
            original_ws_connect = fixture.session_spy.ws_connect

            async def blocked_reconnect_ws_connect(*args: Any, **kwargs: Any) -> Any:
                nonlocal reconnect_attempts
                reconnect_attempts += 1
                reconnect_connect_entered.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    reconnect_cancel_entered.set()
                    await reconnect_cancel_release.wait()
                    raise

            fixture.session_spy.ws_connect = blocked_reconnect_ws_connect  # type: ignore[method-assign]
            await asyncio.wait_for(primary_socket.close(), timeout=5.0)
            await asyncio.wait_for(reconnect_connect_entered.wait(), timeout=5.0)
            reconnect_task = lifecycle.reconnect_task
            assert reconnect_task is not None
            self.assertFalse(reconnect_task.done() or not primary_socket.closed)
            cleanup_task = asyncio.create_task(lifecycle.cleanup())
            await asyncio.wait_for(reconnect_cancel_entered.wait(), timeout=5.0)
            cleanup_owner = lifecycle._cleanup_task
            assert cleanup_owner is not None
            await asyncio.wait_for(cleanup_owner, timeout=5.0)
            self.assertFalse(
                cleanup_task.done(),
                "cleanup caller should remain pending while draining cancelled reconnect snapshot task",
            )
            self.assertIn(reconnect_task, lifecycle._cleanup_snapshot)
            self.assertFalse(reconnect_task.done())
            self.assertTrue(primary_socket.closed)
            reconnect_cancel_release.set()
            await asyncio.wait_for(cleanup_task, timeout=5.0)
            self.assertTrue(reconnect_task.done() and reconnect_task.cancelled())
            self.assertEqual(reconnect_attempts, 1)
            self.assertIs(fixture.session_spy.latest_raw_websocket, primary_socket._websocket)
            outcome = cleanup_owner.result()
            self.assertIs(outcome.primary_socket, primary_socket)
            self.assertTrue(outcome.primary_socket_disposed)
        finally:
            if original_ws_connect is not None:
                fixture.session_spy.ws_connect = original_ws_connect  # type: ignore[method-assign]
            await self._cleanup_unmanaged_fixture(fixture)

    async def test_cleanup_snapshot_propagates_caller_cancellation_while_draining_owned_task(self) -> None:
        fixture = await self._make_unmanaged_node(
            identifier="cleanup-snapshot-caller-cancel",
            websocket_handler=make_ready_websocket_handler("cleanup-snapshot-caller-cancel"),
            retries=None,
        )
        drain_task = owned_task = None
        owned_task_cancel_release = asyncio.Event()
        owned_task_cancel_entered = asyncio.Event()
        try:
            lifecycle = await self._connect_node_and_get_lifecycle(fixture)

            async def cleanup_snapshot_owned_task() -> None:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    owned_task_cancel_entered.set()
                    await owned_task_cancel_release.wait()
                    raise

            owned_task = asyncio.create_task(cleanup_snapshot_owned_task())
            lifecycle._cleanup_snapshot = (owned_task,)
            drain_task = asyncio.create_task(lifecycle._drain_cleanup_snapshot())
            advance_loop = asyncio.Event()
            asyncio.get_running_loop().call_soon(advance_loop.set)
            await asyncio.wait_for(advance_loop.wait(), timeout=5.0)
            drain_task.cancel()
            await asyncio.wait_for(owned_task_cancel_entered.wait(), timeout=5.0)
            owned_task_cancel_release.set()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(drain_task, timeout=5.0)
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(owned_task, timeout=5.0)
        finally:
            owned_task_cancel_release.set()
            await self._await_task_if_pending(drain_task, cancel=True)
            await self._await_task_if_pending(owned_task)
            await self._cleanup_unmanaged_fixture(fixture)

    async def test_cleanup_from_websocket_owned_task_skips_initiator_and_avoids_self_await_deadlock(self) -> None:
        fixture = await self._make_unmanaged_node(
            identifier="owned-task-cleanup",
            websocket_handler=make_ready_websocket_handler("owned-task-cleanup"),
            retries=1,
        )
        try:
            lifecycle = await self._connect_node_and_get_lifecycle(fixture)
            owned_keep_alive_task = lifecycle.keep_alive_task

            async def owned_reconnect_cleanup_caller() -> None:
                lifecycle.reconnect_task = asyncio.current_task()
                await lifecycle.cleanup()

            owned_reconnect_task = asyncio.create_task(owned_reconnect_cleanup_caller())
            lifecycle.reconnect_task = owned_reconnect_task
            await asyncio.wait_for(owned_reconnect_task, timeout=5.0)
            self.assertIn(owned_reconnect_task, lifecycle._cleanup_snapshot)
            self.assertIn(owned_keep_alive_task, lifecycle._cleanup_snapshot)
            self.assertTrue(owned_reconnect_task.done() and not owned_reconnect_task.cancelled())
            self.assertTrue(owned_keep_alive_task.done() and owned_keep_alive_task.cancelled())
        finally:
            await self._cleanup_unmanaged_fixture(fixture)

    async def test_cancelled_sole_close_eject_waiter_still_ejects_after_owner_settles(self) -> None:
        fixture = await self._make_unmanaged_node(
            identifier="cancelled-sole-close-eject",
            websocket_handler=make_ready_websocket_handler("cancelled-sole-close-eject"),
            retries=1,
        )
        close_task: asyncio.Task[None] | None = None
        close_state_task: asyncio.Task[Any] | None = None
        try:
            await asyncio.wait_for(wavelink.Pool.connect(nodes=[fixture.node], client=self.client), timeout=5.0)
            await self.events.wait_for_count("wavelink_node_ready", 1)
            close_gate = TransportCloseGate()
            fixture.session_spy.close_gate = close_gate
            close_task = asyncio.create_task(fixture.node.close(eject=True))
            await asyncio.wait_for(close_gate.entered.wait(), timeout=5.0)
            close_state = fixture.node._close_state
            close_state_task = close_state.task
            self.assertFalse(close_state_task.cancelled())
            close_task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await close_task
            self.assertFalse(close_state_task.done())
            close_gate.release()
            await asyncio.wait_for(close_state_task, timeout=5.0)
            self.assertEqual(fixture.node.status, wavelink.NodeStatus.DISCONNECTED)
            self.assertTrue(close_state_task.done() and not close_state_task.cancelled())
            self.assertNotIn(
                fixture.node.identifier,
                wavelink.Pool.nodes,
                "a cancelled sole close(eject=True) waiter must still eject the exact node identity after settle",
            )
        finally:
            if close_state_task is not None and not close_state_task.done():
                fixture.session_spy.close_gate.release()
                await asyncio.wait_for(close_state_task, timeout=5.0)
            if close_task is not None and not close_task.done():
                close_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await close_task
            await self._cleanup_unmanaged_fixture(fixture)

    async def test_obsolete_late_connect_after_terminal_close_does_not_revive_or_leak_resources(self) -> None:
        fixture = await self._make_unmanaged_node(
            identifier="late-obsolete-connect",
            websocket_handler=make_ready_websocket_handler("late-obsolete-connect"),
            retries=1,
        )
        fixture.session_spy.connect_return_gate = connect_gate = ConnectReturnGate()
        connect_task = asyncio.create_task(wavelink.Pool.connect(nodes=[fixture.node], client=self.client))
        obsolete_lifecycle: Any | None = None
        try:
            await asyncio.wait_for(connect_gate.entered.wait(), timeout=5.0)
            obsolete_lifecycle = fixture.node._websocket
            self.assertIsNone(obsolete_lifecycle.socket, "connect gate must block before socket publication")
            await asyncio.wait_for(fixture.node.close(eject=True), timeout=5.0)
            self.assertEqual(fixture.node.status, wavelink.NodeStatus.DISCONNECTED)
            self.assertNotIn(fixture.node.identifier, wavelink.Pool.nodes)
            connect_gate.release()
            await asyncio.wait_for(connect_task, timeout=5.0)
            late_socket = fixture.session_spy.latest_raw_websocket
            self.assertIsNotNone(late_socket, "late obsolete connect must have produced a candidate socket")
            assert late_socket is not None
            self.assertTrue(late_socket.closed)
            keep_alive_task = obsolete_lifecycle.keep_alive_task
            self.assertTrue(
                keep_alive_task is None or keep_alive_task.done(),
                "late obsolete keep-alive task must not remain live after terminal close",
            )
            self.assertNotIn(fixture.node.identifier, wavelink.Pool.nodes)
            self.assertEqual(fixture.node.status, wavelink.NodeStatus.DISCONNECTED)
            self.assertIsNone(fixture.node.session_id)
            self.assertEqual(
                self.events.counts["wavelink_node_ready"],
                0,
                "late obsolete connect must not revive the node with a ready event after terminal close",
            )
        finally:
            connect_gate.release()
            if not connect_task.done():
                await asyncio.wait_for(connect_task, timeout=5.0)
            if obsolete_lifecycle is not None:
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(obsolete_lifecycle.cleanup(), timeout=5.0)
            await self._cleanup_unmanaged_fixture(fixture)

    async def test_successful_close_then_reconnect_then_close_again_reuses_node_identity(self) -> None:
        fixture = await self._make_unmanaged_node(
            identifier="node-reuse",
            websocket_handler=make_ready_websocket_handler("node-reuse", include_connection_number=True),
        )
        try:
            first_lifecycle = await self._connect_node_and_get_lifecycle(fixture)
            await asyncio.wait_for(fixture.node.close(), timeout=5.0)
            self.assertIs(wavelink.Pool.nodes.get(fixture.node.identifier), fixture.node)
            self.assertEqual(fixture.node.status, wavelink.NodeStatus.DISCONNECTED)
            await asyncio.wait_for(wavelink.Pool.reconnect(), timeout=5.0)
            await self.events.wait_for_count("wavelink_node_ready", 2)
            self.assertEqual(fixture.node.status, wavelink.NodeStatus.CONNECTED)
            self.assertIsNotNone(fixture.node.session_id)
            self.assertIsNot(fixture.node._websocket, first_lifecycle)
            await asyncio.wait_for(fixture.node.close(eject=True), timeout=5.0)
            self.assertNotIn(fixture.node.identifier, wavelink.Pool.nodes)
        finally:
            await self._cleanup_unmanaged_fixture(fixture)

    async def test_cancelled_close_waiter_logs_owner_failure_once_and_replays_same_exception(self) -> None:
        fixture = await self._make_unmanaged_node(
            identifier="cancelled-waiter-owner-failure",
            websocket_handler=make_ready_websocket_handler("cancelled-waiter-owner-failure"),
        )
        close_waiter: asyncio.Task[None] | None = None
        close_error = RuntimeError("retained-close-owner-failure")
        try:
            await asyncio.wait_for(wavelink.Pool.connect(nodes=[fixture.node], client=self.client), timeout=5.0)
            await self.events.wait_for_count("wavelink_node_ready", 1)
            close_gate = TransportCloseGate(close_error=close_error)
            fixture.session_spy.close_gate = close_gate
            with self.assertLogs("wavelink.node", level="ERROR") as logs:
                close_waiter = asyncio.create_task(fixture.node.close(eject=True))
                await asyncio.wait_for(close_gate.entered.wait(), timeout=5.0)
                retained = fixture.node._close_state
                self.assertFalse(retained.task.done())
                close_waiter.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await close_waiter
                self.assertFalse(retained.task.done())
                close_gate.release()
                await asyncio.wait_for(retained.task, timeout=5.0)
                with self.assertRaises(RuntimeError) as raised_later:
                    await asyncio.wait_for(fixture.node.close(eject=True), timeout=5.0)
            self.assertIs(raised_later.exception, close_error)
            self.assertEqual(len(logs.records), 1, "owner close failure should be observed/logged exactly once")
            self.assertNotIn(fixture.node.identifier, wavelink.Pool.nodes)
        finally:
            if close_waiter is not None and not close_waiter.done():
                close_waiter.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await close_waiter
            await self._cleanup_unmanaged_fixture(fixture)

    async def test_reconnect_handshake_failures_dispatch_once_and_consume_reconnect_task_exception(self) -> None:
        for status, expected_exception in ((401, wavelink.AuthorizationFailedException), (404, wavelink.NodeException)):
            with self.subTest(status=status, phase="initial-handshake"):
                fixture = await self._make_unmanaged_node(
                    identifier=f"initial-handshake-failure-{status}",
                    websocket_handler=lambda _req, _num, s=status: web.Response(status=s),
                    retries=1,
                )
                try:
                    disconnected_before = self.events.counts["wavelink_node_disconnected"]
                    with contextlib.suppress(expected_exception):
                        await asyncio.wait_for(
                            wavelink.Pool.connect(nodes=[fixture.node], client=self.client), timeout=5.0
                        )
                    self.assertEqual(
                        self.events.counts["wavelink_node_disconnected"] - disconnected_before,
                        0,
                        "initial handshake failure before ready must not dispatch disconnected",
                    )
                finally:
                    await self._cleanup_unmanaged_fixture(fixture)
            with self.subTest(status=status, phase="reconnect-handshake"):

                async def websocket_handler(request: web.Request, number: int) -> web.StreamResponse:
                    if number == 1:
                        return await make_ready_websocket_handler(f"fail-{status}-ready")(request, number)
                    return web.Response(status=status)

                fixture = await self._make_unmanaged_node(
                    identifier=f"reconnect-failure-{status}",
                    websocket_handler=websocket_handler,
                    retries=1,
                )
                try:
                    disconnected_before = self.events.counts["wavelink_node_disconnected"]
                    lifecycle = await self._connect_node_and_get_lifecycle(fixture)
                    assert lifecycle.socket is not None
                    fixture.session_spy.mark_boundary()
                    await asyncio.wait_for(lifecycle.socket.close(), timeout=5.0)
                    await asyncio.wait_for(fixture.session_spy.post_boundary_call.wait(), timeout=5.0)
                    reconnect_task = lifecycle.reconnect_task
                    assert reconnect_task is not None
                    await asyncio.wait({reconnect_task}, timeout=5.0)
                    await self.events.wait_for_count("wavelink_node_disconnected", disconnected_before + 1)
                    self.assertEqual(
                        self.events.counts["wavelink_node_disconnected"] - disconnected_before,
                        1,
                        "reconnect failure should dispatch disconnected exactly once per connected interval",
                    )
                    self.assertIsInstance(getattr(reconnect_task, "_exception", None), expected_exception)
                    self.assertFalse(
                        getattr(reconnect_task, "_log_traceback", False),
                        "reconnect task exception should be observed to avoid unretrieved-task diagnostics",
                    )
                finally:
                    await self._cleanup_unmanaged_fixture(fixture)

    async def test_pre_ready_close_then_reconnect_handshake_failure_never_dispatches_disconnect(self) -> None:
        for status, expected_exception in ((401, wavelink.AuthorizationFailedException), (404, wavelink.NodeException)):
            with self.subTest(status=status):
                reconnect_attempted = asyncio.Event()
                fixture = await self._make_unmanaged_node(
                    identifier=f"pre-ready-close-reconnect-{status}",
                    websocket_handler=make_pre_ready_close_then_handshake_failure_handler(
                        handshake_status=status, reconnect_attempted=reconnect_attempted, wait_for_client_close=True
                    ),
                    retries=1,
                )
                try:
                    disconnected_before = self.events.counts["wavelink_node_disconnected"]
                    await asyncio.wait_for(wavelink.Pool.connect(nodes=[fixture.node], client=self.client), timeout=5.0)
                    lifecycle = fixture.node._websocket
                    assert lifecycle is not None and lifecycle.socket is not None
                    await asyncio.wait_for(lifecycle.socket.close(), timeout=5.0)
                    await asyncio.wait_for(reconnect_attempted.wait(), timeout=5.0)
                    reconnect_task = lifecycle.reconnect_task
                    await asyncio.wait_for(asyncio.wait({reconnect_task}), timeout=5.0)
                    self.assertEqual(self.events.counts["wavelink_node_disconnected"] - disconnected_before, 0)
                    self.assertIsInstance(getattr(reconnect_task, "_exception", None), expected_exception)
                    self.assertFalse(
                        getattr(reconnect_task, "_log_traceback", False),
                        "pre-ready reconnect handshake failures should be consumed/observed",
                    )
                finally:
                    await self._cleanup_unmanaged_fixture(fixture)

    async def test_concurrent_connect_same_identifier_retires_loser_resources_and_events(self) -> None:
        winner = await self._make_unmanaged_node(
            identifier="shared-concurrent-node",
            websocket_handler=make_ready_websocket_handler("winner-session"),
        )
        loser = await self._make_unmanaged_node(
            identifier="shared-concurrent-node",
            websocket_handler=make_ready_websocket_handler(
                "loser-session", extra_payloads=(build_player_update_payload("1"),)
            ),
        )
        race = begin_same_identifier_connect_race(first=winner, second=loser, client=self.client)
        try:
            await race.wait_until_both_candidates_ready()
            loser_lifecycle = loser.node._websocket
            race.release_first()
            await asyncio.wait_for(race.first_task, timeout=5.0)
            race.release_second()
            await asyncio.wait_for(race.second_task, timeout=5.0)
            await self.events.wait_for_count("wavelink_node_ready", 1)
            self.assertIs(wavelink.Pool.nodes.get("shared-concurrent-node"), winner.node)
            self.assertIsNone(loser.node._websocket)
            self.assertTrue(loser_lifecycle.socket is not None and loser_lifecycle.socket.closed)
            self.assertTrue(
                (loser_lifecycle.keep_alive_task is None or loser_lifecycle.keep_alive_task.done())
                and (loser_lifecycle.reconnect_task is None or loser_lifecycle.reconnect_task.done())
            )
            self.assertFalse(winner.session_spy.real.closed or loser.session_spy.real.closed)
            self.assertEqual(self.events.counts["wavelink_player_update"], 0)
            self.assertIs(self.events.payloads["wavelink_node_ready"][0][0].node, winner.node)
        finally:
            await race.cancel_and_drain()
            await self._cleanup_unmanaged_fixture(winner)
            await self._cleanup_unmanaged_fixture(loser)

    async def test_obsolete_candidate_retirement_propagates_caller_cancellation_while_draining_owned_task(self) -> None:
        fixture = await self._make_unmanaged_node(
            identifier="obsolete-retirement-caller-cancel",
            websocket_handler=make_ready_websocket_handler("obsolete-retirement-caller-cancel"),
            retries=1,
        )
        fixture.session_spy.receive_gate = receive_gate = ReceiveCancelGate()
        retirement_task = owned_task = unresolved_socket = None
        try:
            lifecycle = await self._connect_node_and_get_lifecycle(fixture)
            owned_task = lifecycle.keep_alive_task
            assert owned_task is not None and lifecycle.socket is not None
            unresolved_socket = lifecycle.socket
            unresolved_socket._websocket = ScriptedCloseSocket(
                unresolved_socket._websocket, close_without_closing_attempts=1
            )
            retirement_task = asyncio.create_task(fixture.node._retire_obsolete_connect_candidate(websocket=lifecycle))
            await asyncio.wait_for(receive_gate.cancel_entered.wait(), timeout=5.0)
            retirement_task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(retirement_task, timeout=5.0)
            receive_gate.release()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(owned_task, timeout=5.0)
            self.assertTrue(
                fixture.node._retained_candidate_sockets == [unresolved_socket] and not unresolved_socket.closed
            )
        finally:
            receive_gate.release()
            fixture.session_spy.receive_gate = None
            await self._await_task_if_pending(retirement_task, cancel=True)
            await self._await_task_if_pending(owned_task)
            await self._close_socket_if_open(unresolved_socket)
            await self._cleanup_unmanaged_fixture(fixture)

    async def test_obsolete_candidate_retirement_fail_closed_retains_unresolved_candidate_and_blocks_reuse(
        self,
    ) -> None:
        for scenario in ("close-without-closing", "cancel-during-close"):
            with self.subTest(scenario=scenario):
                fixture = await self._make_unmanaged_node(
                    identifier=f"obsolete-retained-{scenario}",
                    websocket_handler=make_ready_websocket_handler("obsolete-retained"),
                    retries=1,
                )
                fixture.session_spy.connect_return_gate = connect_gate = ConnectReturnGate()
                connect_task = asyncio.create_task(wavelink.Pool.connect(nodes=[fixture.node], client=self.client))
                close_gate = unresolved_socket = None
                try:
                    await asyncio.wait_for(connect_gate.entered.wait(), timeout=5.0)
                    wrapper_ref = fixture.session_spy.latest_wrapper_ref
                    candidate_socket = wrapper_ref()
                    if scenario == "close-without-closing":
                        candidate_socket._websocket = ScriptedCloseSocket(
                            candidate_socket._websocket, close_without_closing_attempts=1
                        )
                        unresolved_socket = candidate_socket
                    else:
                        unresolved_socket = candidate_socket
                        close_gate = TransportCloseGate()
                        fixture.session_spy.close_gate = close_gate
                    await asyncio.wait_for(fixture.node.close(eject=True), timeout=5.0)
                    connect_gate.release()
                    if close_gate is None:
                        await asyncio.wait_for(connect_task, timeout=5.0)
                    else:
                        await asyncio.wait_for(close_gate.entered.wait(), timeout=5.0)
                        connect_task.cancel()
                        close_gate.release()
                        with self.assertRaises(asyncio.CancelledError):
                            await connect_task
                    self.assertTrue(
                        fixture.node._retained_candidate_sockets == [unresolved_socket] and not unresolved_socket.closed
                    )
                    if scenario == "close-without-closing":
                        self.assertEqual(unresolved_socket._websocket.close_calls, 1)
                    with self.assertRaisesRegex(RuntimeError, "retained websocket candidate remains unresolved"):
                        await asyncio.wait_for(
                            wavelink.Pool.connect(nodes=[fixture.node], client=self.client), timeout=5.0
                        )
                finally:
                    connect_gate.release()
                    if close_gate is not None:
                        close_gate.release()
                    fixture.session_spy.close_gate = None
                    await self._await_task_if_pending(connect_task, cancel=True)
                    await self._close_socket_if_open(unresolved_socket)
                    await self._cleanup_unmanaged_fixture(fixture)

    async def test_late_candidate_disposal_close_failure_bubbles_to_pool_connect(self) -> None:
        for close_error in (
            RuntimeError("late-candidate-disposal-close-runtime-failure"),
            wavelink.NodeException("late-candidate-disposal-close-node-failure", status=404),
            wavelink.AuthorizationFailedException(),
        ):
            with self.subTest(close_error_type=type(close_error).__name__):
                fixture = await self._make_unmanaged_node(
                    identifier=f"late-candidate-disposal-{type(close_error).__name__}",
                    websocket_handler=make_ready_websocket_handler("late-candidate-disposal"),
                    retries=1,
                )
                fixture.session_spy.connect_return_gate = connect_gate = ConnectReturnGate()
                fixture.session_spy.close_gate = TransportCloseGate(close_error=close_error)
                fixture.session_spy.close_gate.disarm()
                connect_task = asyncio.create_task(wavelink.Pool.connect(nodes=[fixture.node], client=self.client))
                try:
                    await asyncio.wait_for(connect_gate.entered.wait(), timeout=5.0)
                    await asyncio.wait_for(fixture.node.close(eject=True), timeout=5.0)
                    self.assertNotIn(fixture.node.identifier, wavelink.Pool.nodes)
                    connect_gate.release()
                    with self.assertRaises(type(close_error)) as raised:
                        await asyncio.wait_for(connect_task, timeout=5.0)
                    self.assertIs(raised.exception, close_error)
                    self.assertIs(getattr(connect_task, "_exception", None), close_error)
                finally:
                    connect_gate.release()
                    await self._await_task_if_pending(connect_task)
                    fixture.session_spy.close_gate = None
                    await self._cleanup_unmanaged_fixture(fixture)

    async def test_pre_disposal_close_failure_blocks_reuse_and_replays_retained_exception(self) -> None:
        retained_errors = (
            RuntimeError("pre-disposal-close-failure"),
            wavelink.NodeException("pre-disposal-node-failure", status=404),
            wavelink.AuthorizationFailedException(),
        )
        for retained_error in retained_errors:
            for operation in ("connect", "reconnect"):
                with self.subTest(retained_error=type(retained_error).__name__, operation=operation):
                    fixture = await self._make_unmanaged_node(
                        identifier=f"pre-disposal-retained-{type(retained_error).__name__}-{operation}",
                        websocket_handler=make_ready_websocket_handler("pre-disposal-retained-failure"),
                        retries=1,
                    )
                    raw_socket: aiohttp.ClientWebSocketResponse[bool] | None = None
                    try:
                        await asyncio.wait_for(
                            wavelink.Pool.connect(nodes=[fixture.node], client=self.client), timeout=5.0
                        )
                        await self.events.wait_for_count("wavelink_node_ready", 1)
                        raw_socket = fixture.session_spy.latest_raw_websocket
                        assert raw_socket is not None
                        fixture.session_spy.close_gate = TransportCloseGate(
                            close_error=retained_error, raise_before_close=True
                        )
                        fixture.session_spy.close_gate.disarm()
                        with self.assertRaises(type(retained_error)) as raised_close:
                            await asyncio.wait_for(fixture.node.close(), timeout=5.0)
                        self.assertIs(raised_close.exception, retained_error)
                        self.assertFalse(
                            raw_socket.closed, "pre-disposal close failure should leave the existing socket open"
                        )
                        ws_connect_calls_before = fixture.session_spy.ws_connect_calls
                        if operation == "connect":
                            self.rig.force_pool_remove(fixture.node)
                            with self.assertRaises(type(retained_error)) as raised_connect:
                                await asyncio.wait_for(
                                    wavelink.Pool.connect(nodes=[fixture.node], client=self.client), timeout=5.0
                                )
                            self.assertIs(raised_connect.exception, retained_error)
                        else:
                            with self.assertRaises(type(retained_error)) as raised_reconnect:
                                await asyncio.wait_for(wavelink.Pool.reconnect(), timeout=5.0)
                            self.assertIs(raised_reconnect.exception, retained_error)
                        self.assertEqual(fixture.session_spy.ws_connect_calls, ws_connect_calls_before)
                        self.assertIs(fixture.session_spy.latest_raw_websocket, raw_socket)
                        self.assertFalse(raw_socket.closed)
                    finally:
                        fixture.session_spy.close_gate = None
                        if raw_socket is not None and not raw_socket.closed:
                            with contextlib.suppress(Exception):
                                await asyncio.wait_for(raw_socket.close(), timeout=5.0)
                        await self._cleanup_unmanaged_fixture(fixture)
