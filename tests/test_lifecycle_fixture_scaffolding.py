from __future__ import annotations

import asyncio
import unittest

import aiohttp

import wavelink
from tests.lifecycle_fixtures import (
    ConnectReturnGate,
    FinalRegistrationGenerationBarrier,
    LifecycleFixtureRig,
    SameIdentifierConnectRace,
    ScriptedCloseSocket,
    begin_same_identifier_connect_race,
    build_player_update_payload,
    build_ready_payload,
    make_blocked_ready_websocket_handler,
    make_pre_ready_close_then_handshake_failure_handler,
    make_ready_websocket_handler,
)


class TestLifecycleFixtureScaffolding(unittest.IsolatedAsyncioTestCase):
    async def test_ready_payload_helpers_build_expected_shape(self) -> None:
        ready = build_ready_payload("session-1")
        self.assertEqual(ready, {"op": "ready", "resumed": False, "sessionId": "session-1"})

        player_update = build_player_update_payload("42", time=3, position=7, ping=11)
        self.assertEqual(
            player_update,
            {"op": "playerUpdate", "guildId": "42", "state": {"time": 3, "position": 7, "ping": 11}},
        )

    async def test_ready_websocket_handler_sends_ready_and_extra_payloads(self) -> None:
        rig = LifecycleFixtureRig()
        await rig.enter()
        self.addAsyncCleanup(rig.close)

        fixture = await rig.make_node(
            identifier="handler-ready",
            websocket_handler=make_ready_websocket_handler(
                "ready-handler",
                include_connection_number=True,
                extra_payloads=(build_player_update_payload("1"),),
            ),
            managed=False,
        )
        self.addAsyncCleanup(rig.close_fixture, fixture, suppress_errors=True)

        async with (
            aiohttp.ClientSession() as session,
            session.ws_connect(f"{fixture.server.uri}/v4/websocket") as websocket,
        ):
            first = await websocket.receive_json()
            second = await websocket.receive_json()
            self.assertEqual(first, {"op": "ready", "resumed": False, "sessionId": "ready-handler-1"})
            self.assertEqual(second["op"], "playerUpdate")
            await websocket.close()

    async def test_blocked_ready_websocket_handler_waits_for_release_before_sending_ready(self) -> None:
        ready_release = asyncio.Event()
        transport_connected = asyncio.Event()
        rig = LifecycleFixtureRig()
        await rig.enter()
        self.addAsyncCleanup(rig.close)

        fixture = await rig.make_node(
            identifier="handler-blocked-ready",
            websocket_handler=make_blocked_ready_websocket_handler(
                "blocked-ready",
                ready_release=ready_release,
                transport_connected=transport_connected,
            ),
            managed=False,
        )
        self.addAsyncCleanup(rig.close_fixture, fixture, suppress_errors=True)

        async with aiohttp.ClientSession() as session:
            websocket = await session.ws_connect(f"{fixture.server.uri}/v4/websocket")
            try:
                await asyncio.wait_for(transport_connected.wait(), timeout=1.0)
                receive_task = asyncio.create_task(websocket.receive())
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(asyncio.shield(receive_task), timeout=0.05)
                self.assertFalse(receive_task.done())
                ready_release.set()
                ready_message = await asyncio.wait_for(receive_task, timeout=1.0)
                self.assertEqual(ready_message.json(), {"op": "ready", "resumed": False, "sessionId": "blocked-ready"})
            finally:
                await websocket.close()

    async def test_blocked_ready_websocket_handler_after_release_stays_pending_until_client_close(self) -> None:
        ready_release = asyncio.Event()
        transport_connected = asyncio.Event()
        handler_task_ready: asyncio.Future[asyncio.Task[object]] = asyncio.Future()
        rig = LifecycleFixtureRig()
        await rig.enter()
        self.addAsyncCleanup(rig.close)

        fixture = await rig.make_node(
            identifier="handler-blocked-ready-release-pending-close",
            websocket_handler=make_blocked_ready_websocket_handler(
                "blocked-ready-release-pending-close",
                ready_release=ready_release,
                transport_connected=transport_connected,
                handler_task_ready=handler_task_ready,
            ),
            managed=False,
        )
        self.addAsyncCleanup(rig.close_fixture, fixture, suppress_errors=True)

        async with aiohttp.ClientSession() as session:
            websocket = await session.ws_connect(f"{fixture.server.uri}/v4/websocket")
            try:
                await asyncio.wait_for(transport_connected.wait(), timeout=1.0)
                handler_task = await asyncio.wait_for(handler_task_ready, timeout=1.0)
                ready_release.set()
                ready_message = await asyncio.wait_for(websocket.receive_json(), timeout=1.0)
                self.assertEqual(
                    ready_message,
                    {"op": "ready", "resumed": False, "sessionId": "blocked-ready-release-pending-close"},
                )
                self.assertFalse(handler_task.done())

                await websocket.close()
                await asyncio.wait_for(handler_task, timeout=1.0)
                self.assertFalse(getattr(handler_task, "_log_traceback", False))
            finally:
                await websocket.close()

    async def test_blocked_ready_websocket_handler_release_gates_extra_payloads_and_preserves_order(self) -> None:
        ready_release = asyncio.Event()
        transport_connected = asyncio.Event()
        rig = LifecycleFixtureRig()
        await rig.enter()
        self.addAsyncCleanup(rig.close)

        first_payload = build_player_update_payload("100", time=100, position=7, ping=1)
        second_payload = build_player_update_payload("200", time=200, position=8, ping=2)
        fixture = await rig.make_node(
            identifier="handler-blocked-ready-extra-payloads",
            websocket_handler=make_blocked_ready_websocket_handler(
                "blocked-ready-extra-payloads",
                ready_release=ready_release,
                transport_connected=transport_connected,
                extra_payloads=(first_payload, second_payload),
            ),
            managed=False,
        )
        self.addAsyncCleanup(rig.close_fixture, fixture, suppress_errors=True)

        async with aiohttp.ClientSession() as session:
            websocket = await session.ws_connect(f"{fixture.server.uri}/v4/websocket")
            try:
                await asyncio.wait_for(transport_connected.wait(), timeout=1.0)
                first_receive = asyncio.create_task(websocket.receive())
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(asyncio.shield(first_receive), timeout=0.05)
                self.assertFalse(first_receive.done())

                ready_release.set()
                ready_message = await asyncio.wait_for(first_receive, timeout=1.0)
                first_extra = await asyncio.wait_for(websocket.receive_json(), timeout=1.0)
                second_extra = await asyncio.wait_for(websocket.receive_json(), timeout=1.0)
                self.assertEqual(
                    ready_message.json(),
                    {"op": "ready", "resumed": False, "sessionId": "blocked-ready-extra-payloads"},
                )
                self.assertEqual(first_extra, first_payload)
                self.assertEqual(second_extra, second_payload)
            finally:
                await websocket.close()

    async def test_blocked_ready_websocket_handler_client_close_before_release_returns_without_ready(self) -> None:
        ready_release = asyncio.Event()
        transport_connected = asyncio.Event()
        client_close_won = asyncio.Event()
        rig = LifecycleFixtureRig()
        await rig.enter()
        self.addAsyncCleanup(rig.close)

        fixture = await rig.make_node(
            identifier="handler-blocked-ready-client-close",
            websocket_handler=make_blocked_ready_websocket_handler(
                "blocked-ready-client-close",
                ready_release=ready_release,
                transport_connected=transport_connected,
                client_close_won=client_close_won,
            ),
            managed=False,
        )
        self.addAsyncCleanup(rig.close_fixture, fixture, suppress_errors=True)

        async with aiohttp.ClientSession() as session:
            websocket = await session.ws_connect(f"{fixture.server.uri}/v4/websocket")
            await asyncio.wait_for(transport_connected.wait(), timeout=1.0)
            receive_task = asyncio.create_task(websocket.receive())
            await websocket.close()
            close_message = await asyncio.wait_for(receive_task, timeout=1.0)
            self.assertIn(
                close_message.type,
                (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING),
            )
            await asyncio.wait_for(client_close_won.wait(), timeout=1.0)
            self.assertFalse(ready_release.is_set())

    async def test_blocked_ready_websocket_handler_handler_cancellation_before_release_closes_transport(self) -> None:
        ready_release = asyncio.Event()
        transport_connected = asyncio.Event()
        handler_cancelled = asyncio.Event()
        handler_task_ready: asyncio.Future[asyncio.Task[object]] = asyncio.Future()
        rig = LifecycleFixtureRig()
        await rig.enter()
        self.addAsyncCleanup(rig.close)

        fixture = await rig.make_node(
            identifier="handler-blocked-ready-cancelled",
            websocket_handler=make_blocked_ready_websocket_handler(
                "blocked-ready-cancelled",
                ready_release=ready_release,
                transport_connected=transport_connected,
                handler_cancelled_before_release=handler_cancelled,
                handler_task_ready=handler_task_ready,
            ),
            managed=False,
        )
        self.addAsyncCleanup(rig.close_fixture, fixture, suppress_errors=True)

        async with aiohttp.ClientSession() as session:
            websocket = await session.ws_connect(f"{fixture.server.uri}/v4/websocket")
            try:
                await asyncio.wait_for(transport_connected.wait(), timeout=1.0)
                handler_task = await asyncio.wait_for(handler_task_ready, timeout=1.0)
                handler_task.cancel()
                await asyncio.wait_for(handler_cancelled.wait(), timeout=1.0)
                with self.assertRaises(asyncio.CancelledError):
                    await handler_task
                close_message = await asyncio.wait_for(websocket.receive(), timeout=1.0)
                self.assertIn(
                    close_message.type,
                    (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING),
                )
                self.assertFalse(getattr(handler_task, "_log_traceback", False))
            finally:
                await websocket.close()

    async def test_final_registration_generation_barrier_blocks_once_after_connect_and_delegates_result(self) -> None:
        connect_result = object()
        validation_result = object()

        class DummyNode:
            def __init__(self) -> None:
                self.connect_calls = 0
                self.validation_calls = 0

            async def _connect(self, *args: object, **kwargs: object) -> object:
                self.connect_calls += 1
                return connect_result

            async def _is_connect_generation_invalid(self, *args: object, **kwargs: object) -> object:
                self.validation_calls += 1
                if kwargs["generation"] == 99:
                    return True
                return validation_result

        node = DummyNode()
        barrier = FinalRegistrationGenerationBarrier(candidate_count=1)
        candidate_token = barrier.create_candidate_token()
        connect = barrier.connect_then_arm(candidate_token=candidate_token, connect=node._connect)
        validate = barrier.delegated_validation_then_block(
            candidate_token=candidate_token,
            validate=node._is_connect_generation_invalid,
        )

        self.assertIs(await validate(generation=1), validation_result)
        self.assertIs(await connect(client=None, generation=1), connect_result)
        blocked_validation = asyncio.create_task(validate(generation=2))
        await asyncio.wait_for(barrier.all_candidates_blocked.wait(), timeout=1.0)
        self.assertFalse(blocked_validation.done())
        barrier.release()
        self.assertIs(await asyncio.wait_for(blocked_validation, timeout=1.0), validation_result)
        self.assertTrue(await validate(generation=99))
        self.assertIs(await validate(generation=2), validation_result)
        self.assertEqual(node.connect_calls, 1)
        self.assertEqual(node.validation_calls, 4)

    async def test_final_registration_generation_barrier_supports_real_node_connect_and_explicit_validation_callable(
        self,
    ) -> None:
        rig = LifecycleFixtureRig()
        await rig.enter()
        self.addAsyncCleanup(rig.close)

        fixture = await rig.make_node(
            identifier="barrier-real-node",
            websocket_handler=make_ready_websocket_handler("barrier-real-node"),
            managed=False,
        )
        self.addAsyncCleanup(rig.close_fixture, fixture, suppress_errors=True)

        validation_entered = asyncio.Event()

        async def explicit_validation(*_args: object, **_kwargs: object) -> bool:
            validation_entered.set()
            return False

        barrier = FinalRegistrationGenerationBarrier(candidate_count=1)
        candidate_token = barrier.create_candidate_token()
        connect = barrier.connect_then_arm(candidate_token=candidate_token, connect=fixture.node._connect)
        validate = barrier.delegated_validation_then_block(
            candidate_token=candidate_token,
            validate=explicit_validation,
        )

        await connect(client=rig.client)
        blocked_validation = asyncio.create_task(validate(generation=1))
        await asyncio.wait_for(validation_entered.wait(), timeout=1.0)
        await asyncio.wait_for(barrier.all_candidates_blocked.wait(), timeout=1.0)
        self.assertFalse(blocked_validation.done())
        barrier.release()
        self.assertFalse(await asyncio.wait_for(blocked_validation, timeout=1.0))
        self.assertFalse(await asyncio.wait_for(validate(generation=2), timeout=1.0))
        await rig.close_fixture(fixture, suppress_errors=False)

    async def test_final_registration_generation_barrier_requires_explicit_candidate_tokens(self) -> None:
        barrier = FinalRegistrationGenerationBarrier(candidate_count=1)

        async def connect(*_args: object, **_kwargs: object) -> None:
            return None

        async def validate(*_args: object, **_kwargs: object) -> bool:
            return False

        with self.assertRaises(TypeError):
            barrier.connect_then_arm(candidate_token="shared-identifier", connect=connect)
        with self.assertRaises(TypeError):
            barrier.delegated_validation_then_block(
                candidate_token="shared-identifier",
                validate=validate,
            )

    async def test_final_registration_generation_barrier_blocks_two_same_identifier_nodes_with_unique_tokens(
        self,
    ) -> None:
        rig = LifecycleFixtureRig()
        await rig.enter()
        self.addAsyncCleanup(rig.close)

        first = await rig.make_node(
            identifier="shared-identifier",
            websocket_handler=make_ready_websocket_handler("same-id-first"),
            managed=False,
        )
        second = await rig.make_node(
            identifier="shared-identifier",
            websocket_handler=make_ready_websocket_handler("same-id-second"),
            managed=False,
        )
        self.addAsyncCleanup(rig.close_fixture, second, suppress_errors=True)
        self.addAsyncCleanup(rig.close_fixture, first, suppress_errors=True)

        if rig.client is None:
            raise RuntimeError("LifecycleFixtureRig.enter() must initialize client")

        first_validation_entered = asyncio.Event()
        second_validation_entered = asyncio.Event()

        async def first_validate(*_args: object, **_kwargs: object) -> bool:
            first_validation_entered.set()
            return False

        async def second_validate(*_args: object, **_kwargs: object) -> bool:
            second_validation_entered.set()
            return False

        barrier = FinalRegistrationGenerationBarrier(candidate_count=2)
        first_token = barrier.create_candidate_token()
        second_token = barrier.create_candidate_token()

        first_connect = barrier.connect_then_arm(candidate_token=first_token, connect=first.node._connect)
        second_connect = barrier.connect_then_arm(candidate_token=second_token, connect=second.node._connect)
        first_gated_validate = barrier.delegated_validation_then_block(
            candidate_token=first_token,
            validate=first_validate,
        )
        second_gated_validate = barrier.delegated_validation_then_block(
            candidate_token=second_token,
            validate=second_validate,
        )

        await first_connect(client=rig.client)
        await second_connect(client=rig.client)

        first_blocked_validation = asyncio.create_task(first_gated_validate(generation=1))
        await asyncio.wait_for(first_validation_entered.wait(), timeout=1.0)
        self.assertFalse(barrier.all_candidates_blocked.is_set())
        self.assertFalse(first_blocked_validation.done())

        second_blocked_validation = asyncio.create_task(second_gated_validate(generation=1))
        await asyncio.wait_for(second_validation_entered.wait(), timeout=1.0)
        await asyncio.wait_for(barrier.all_candidates_blocked.wait(), timeout=1.0)
        self.assertFalse(second_blocked_validation.done())

        barrier.release()
        self.assertFalse(await asyncio.wait_for(first_blocked_validation, timeout=1.0))
        self.assertFalse(await asyncio.wait_for(second_blocked_validation, timeout=1.0))
        self.assertFalse(await asyncio.wait_for(first_gated_validate(generation=2), timeout=1.0))
        self.assertFalse(await asyncio.wait_for(second_gated_validate(generation=2), timeout=1.0))

    async def test_final_registration_generation_barrier_preserves_connect_exception_without_arming(self) -> None:
        connect_error = RuntimeError("connect-failed")

        async def failing_connect(*_args: object, **_kwargs: object) -> None:
            raise connect_error

        async def validate(*_args: object, **_kwargs: object) -> bool:
            return False

        barrier = FinalRegistrationGenerationBarrier(candidate_count=1)
        candidate_token = barrier.create_candidate_token()
        connect = barrier.connect_then_arm(candidate_token=candidate_token, connect=failing_connect)
        gated_validate = barrier.delegated_validation_then_block(candidate_token=candidate_token, validate=validate)

        with self.assertRaises(RuntimeError) as raised_connect:
            await connect()
        self.assertIs(raised_connect.exception, connect_error)
        self.assertFalse(barrier.all_candidates_blocked.is_set())
        self.assertFalse(await gated_validate(generation=1))

    async def test_final_registration_generation_barrier_candidate_count_two_rendezvous_is_deterministic(self) -> None:
        first_validation_entered = asyncio.Event()
        second_validation_entered = asyncio.Event()

        async def connect(*_args: object, **_kwargs: object) -> None:
            return None

        async def first_validate(*_args: object, **_kwargs: object) -> bool:
            first_validation_entered.set()
            return False

        async def second_validate(*_args: object, **_kwargs: object) -> bool:
            second_validation_entered.set()
            return False

        barrier = FinalRegistrationGenerationBarrier(candidate_count=2)
        first_token = barrier.create_candidate_token()
        second_token = barrier.create_candidate_token()
        first_connect = barrier.connect_then_arm(candidate_token=first_token, connect=connect)
        second_connect = barrier.connect_then_arm(candidate_token=second_token, connect=connect)
        first_gated_validate = barrier.delegated_validation_then_block(
            candidate_token=first_token,
            validate=first_validate,
        )
        second_gated_validate = barrier.delegated_validation_then_block(
            candidate_token=second_token,
            validate=second_validate,
        )

        await first_connect()
        await second_connect()

        first_blocked_validation = asyncio.create_task(first_gated_validate(generation=1))
        await asyncio.wait_for(first_validation_entered.wait(), timeout=1.0)
        self.assertFalse(barrier.all_candidates_blocked.is_set())
        self.assertFalse(first_blocked_validation.done())

        second_blocked_validation = asyncio.create_task(second_gated_validate(generation=1))
        await asyncio.wait_for(second_validation_entered.wait(), timeout=1.0)
        await asyncio.wait_for(barrier.all_candidates_blocked.wait(), timeout=1.0)
        self.assertFalse(second_blocked_validation.done())

        barrier.release()
        self.assertFalse(await asyncio.wait_for(first_blocked_validation, timeout=1.0))
        self.assertFalse(await asyncio.wait_for(second_blocked_validation, timeout=1.0))

    async def test_final_registration_generation_barrier_cancelled_candidate_does_not_satisfy_rendezvous(self) -> None:
        first_validation_entered = asyncio.Event()
        second_validation_entered = asyncio.Event()

        async def connect(*_args: object, **_kwargs: object) -> None:
            return None

        async def first_validate(*_args: object, **_kwargs: object) -> bool:
            first_validation_entered.set()
            return False

        async def second_validate(*_args: object, **_kwargs: object) -> bool:
            second_validation_entered.set()
            return False

        barrier = FinalRegistrationGenerationBarrier(candidate_count=2)
        first_token = barrier.create_candidate_token()
        second_token = barrier.create_candidate_token()
        first_connect = barrier.connect_then_arm(candidate_token=first_token, connect=connect)
        second_connect = barrier.connect_then_arm(candidate_token=second_token, connect=connect)
        first_gated_validate = barrier.delegated_validation_then_block(
            candidate_token=first_token,
            validate=first_validate,
        )
        second_gated_validate = barrier.delegated_validation_then_block(
            candidate_token=second_token,
            validate=second_validate,
        )

        await first_connect()
        await second_connect()

        first_blocked_validation = asyncio.create_task(first_gated_validate(generation=1))
        await asyncio.wait_for(first_validation_entered.wait(), timeout=1.0)
        self.assertFalse(barrier.all_candidates_blocked.is_set())
        self.assertFalse(first_blocked_validation.done())

        first_blocked_validation.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(first_blocked_validation, timeout=1.0)
        self.assertFalse(barrier.all_candidates_blocked.is_set())

        second_blocked_validation = asyncio.create_task(second_gated_validate(generation=1))
        await asyncio.wait_for(second_validation_entered.wait(), timeout=1.0)
        self.assertFalse(barrier.all_candidates_blocked.is_set())
        self.assertFalse(second_blocked_validation.done())

        barrier.release()
        self.assertFalse(await asyncio.wait_for(second_blocked_validation, timeout=1.0))

    async def test_final_registration_generation_barrier_validation_exception_keeps_one_shot_for_later_success(
        self,
    ) -> None:
        validation_error = RuntimeError("validation-failed")
        successful_validation_entered = asyncio.Event()

        async def connect(*_args: object, **_kwargs: object) -> None:
            return None

        async def validate(*_args: object, **kwargs: object) -> bool:
            if kwargs["generation"] == 10:
                raise validation_error
            successful_validation_entered.set()
            return False

        barrier = FinalRegistrationGenerationBarrier(candidate_count=1)
        candidate_token = barrier.create_candidate_token()
        gated_connect = barrier.connect_then_arm(candidate_token=candidate_token, connect=connect)
        gated_validate = barrier.delegated_validation_then_block(candidate_token=candidate_token, validate=validate)
        await gated_connect()

        with self.assertRaises(RuntimeError) as raised_validation:
            await gated_validate(generation=10)
        self.assertIs(raised_validation.exception, validation_error)
        self.assertFalse(barrier.all_candidates_blocked.is_set())

        blocked_successful_validation = asyncio.create_task(gated_validate(generation=11))
        await asyncio.wait_for(successful_validation_entered.wait(), timeout=1.0)
        await asyncio.wait_for(barrier.all_candidates_blocked.wait(), timeout=1.0)
        self.assertFalse(blocked_successful_validation.done())

        barrier.release()
        self.assertFalse(await asyncio.wait_for(blocked_successful_validation, timeout=1.0))
        self.assertFalse(await asyncio.wait_for(gated_validate(generation=12), timeout=1.0))

    async def test_final_registration_generation_barrier_preserves_connect_cancellation_without_arming(self) -> None:
        connect_entered = asyncio.Event()
        connect_release = asyncio.Event()

        async def connect(*_args: object, **_kwargs: object) -> None:
            connect_entered.set()
            await connect_release.wait()

        async def validate(*_args: object, **_kwargs: object) -> bool:
            return False

        barrier = FinalRegistrationGenerationBarrier(candidate_count=1)
        candidate_token = barrier.create_candidate_token()
        gated_connect = barrier.connect_then_arm(candidate_token=candidate_token, connect=connect)
        gated_validate = barrier.delegated_validation_then_block(candidate_token=candidate_token, validate=validate)

        cancelled_connect = asyncio.create_task(gated_connect())
        await asyncio.wait_for(connect_entered.wait(), timeout=1.0)
        cancelled_connect.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await cancelled_connect
        self.assertFalse(barrier.all_candidates_blocked.is_set())
        self.assertFalse(await gated_validate(generation=1))
        connect_release.set()

    async def test_final_registration_generation_barrier_preserves_validation_cancellation(self) -> None:
        validation_started = asyncio.Event()
        validation_release = asyncio.Event()

        async def connect(*_args: object, **_kwargs: object) -> None:
            return None

        async def validate(*_args: object, **_kwargs: object) -> bool:
            validation_started.set()
            await validation_release.wait()
            return False

        barrier = FinalRegistrationGenerationBarrier(candidate_count=1)
        candidate_token = barrier.create_candidate_token()
        gated_connect = barrier.connect_then_arm(candidate_token=candidate_token, connect=connect)
        gated_validate = barrier.delegated_validation_then_block(candidate_token=candidate_token, validate=validate)
        await gated_connect()

        cancelled_validation = asyncio.create_task(gated_validate(generation=1))
        await asyncio.wait_for(validation_started.wait(), timeout=1.0)
        cancelled_validation.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await cancelled_validation
        self.assertFalse(barrier.all_candidates_blocked.is_set())
        validation_release.set()

    async def test_final_registration_generation_barrier_wrappers_allow_explicit_assignment_and_restoration(
        self,
    ) -> None:
        class DummyNode:
            async def _connect(self, *_args: object, **_kwargs: object) -> str:
                return "original-connect"

            async def _is_connect_generation_invalid(self, *_args: object, **_kwargs: object) -> str:
                return "original-validation"

        node = DummyNode()
        original_connect = node._connect
        original_validation = node._is_connect_generation_invalid

        barrier = FinalRegistrationGenerationBarrier(candidate_count=1)
        candidate_token = barrier.create_candidate_token()
        node._connect = barrier.connect_then_arm(candidate_token=candidate_token, connect=node._connect)  # type: ignore[method-assign]
        node._is_connect_generation_invalid = barrier.delegated_validation_then_block(  # type: ignore[method-assign]
            candidate_token=candidate_token,
            validate=node._is_connect_generation_invalid,
        )

        self.assertEqual(await node._connect(), "original-connect")
        blocked_validation = asyncio.create_task(node._is_connect_generation_invalid())
        await asyncio.wait_for(barrier.all_candidates_blocked.wait(), timeout=1.0)
        self.assertFalse(blocked_validation.done())
        barrier.release()
        self.assertEqual(await asyncio.wait_for(blocked_validation, timeout=1.0), "original-validation")

        node._connect = original_connect  # type: ignore[method-assign]
        node._is_connect_generation_invalid = original_validation  # type: ignore[method-assign]
        self.assertIs(node._connect.__self__, original_connect.__self__)
        self.assertIs(node._connect.__func__, original_connect.__func__)
        self.assertIs(node._is_connect_generation_invalid.__self__, original_validation.__self__)
        self.assertIs(node._is_connect_generation_invalid.__func__, original_validation.__func__)

    async def test_pre_ready_close_then_handshake_failure_handler_sequences_connections(self) -> None:
        reconnect_attempted = asyncio.Event()
        rig = LifecycleFixtureRig()
        await rig.enter()
        self.addAsyncCleanup(rig.close)

        fixture = await rig.make_node(
            identifier="handler-pre-ready-close",
            websocket_handler=make_pre_ready_close_then_handshake_failure_handler(
                handshake_status=401,
                reconnect_attempted=reconnect_attempted,
            ),
            managed=False,
        )
        self.addAsyncCleanup(rig.close_fixture, fixture, suppress_errors=True)

        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(f"{fixture.server.uri}/v4/websocket") as websocket:
                close_message = await websocket.receive()
                self.assertIn(
                    close_message.type,
                    (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING),
                )

            with self.assertRaises(aiohttp.WSServerHandshakeError) as raised:
                await session.ws_connect(f"{fixture.server.uri}/v4/websocket")
            self.assertEqual(raised.exception.status, 401)

        await asyncio.wait_for(reconnect_attempted.wait(), timeout=1.0)

    async def test_pre_ready_close_handler_wait_for_client_close_keeps_client_as_close_initiator(self) -> None:
        reconnect_attempted = asyncio.Event()
        wait_for_client_close_entered = asyncio.Event()
        client_close_observed = asyncio.Event()
        server_close_started = asyncio.Event()
        rig = LifecycleFixtureRig()
        await rig.enter()
        self.addAsyncCleanup(rig.close)

        fixture = await rig.make_node(
            identifier="handler-pre-ready-client-close",
            websocket_handler=make_pre_ready_close_then_handshake_failure_handler(
                handshake_status=401,
                reconnect_attempted=reconnect_attempted,
                wait_for_client_close=True,
                wait_for_client_close_entered=wait_for_client_close_entered,
                client_close_observed=client_close_observed,
                server_close_started=server_close_started,
            ),
            managed=False,
        )
        self.addAsyncCleanup(rig.close_fixture, fixture, suppress_errors=True)

        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(f"{fixture.server.uri}/v4/websocket") as websocket:
                receive_task = asyncio.create_task(websocket.receive())
                await asyncio.wait_for(wait_for_client_close_entered.wait(), timeout=1.0)
                self.assertFalse(receive_task.done())
                self.assertFalse(server_close_started.is_set())
                await websocket.close()
                close_message = await asyncio.wait_for(receive_task, timeout=1.0)
                self.assertIn(
                    close_message.type,
                    (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING),
                )
                await asyncio.wait_for(client_close_observed.wait(), timeout=1.0)
                self.assertTrue(server_close_started.is_set())

            with self.assertRaises(aiohttp.WSServerHandshakeError) as raised:
                await session.ws_connect(f"{fixture.server.uri}/v4/websocket")
            self.assertEqual(raised.exception.status, 401)

        await asyncio.wait_for(reconnect_attempted.wait(), timeout=1.0)

    async def test_same_identifier_connect_race_releases_connect_tasks_deterministically(self) -> None:
        rig = LifecycleFixtureRig()
        await rig.enter()
        self.addAsyncCleanup(rig.close)

        first = await rig.make_node(
            identifier="shared-identifier",
            websocket_handler=make_ready_websocket_handler("first"),
            retries=1,
            managed=False,
        )
        second = await rig.make_node(
            identifier="shared-identifier",
            websocket_handler=make_ready_websocket_handler("second"),
            retries=1,
            managed=False,
        )
        self.addAsyncCleanup(rig.close_fixture, second, suppress_errors=True)
        self.addAsyncCleanup(rig.close_fixture, first, suppress_errors=True)

        client = rig.client
        if client is None:
            raise RuntimeError("LifecycleFixtureRig.enter() must initialize client")
        race = begin_same_identifier_connect_race(first=first, second=second, client=client)
        try:
            await race.wait_until_both_candidates_ready()
            self.assertFalse(race.first_task.done())
            self.assertFalse(race.second_task.done())

            ready_waiter_registered = asyncio.Event()
            rig.events.on_waiter_count("wavelink_node_ready", 1, ready_waiter_registered.set)
            ready_waiter = asyncio.create_task(rig.events.wait_for_count("wavelink_node_ready", 1))
            await asyncio.wait_for(ready_waiter_registered.wait(), timeout=1.0)

            race.release_first()
            await asyncio.wait_for(ready_waiter, timeout=1.0)
            self.assertEqual(rig.events.counts["wavelink_node_ready"], 1)
            self.assertEqual(
                rig.events.payloads["wavelink_node_ready"][0][0].node.identifier,
                "shared-identifier",
            )
            self.assertIs(rig.events.payloads["wavelink_node_ready"][0][0].node, first.node)
            self.assertFalse(race.second_task.done())

            race.release_second()
            await race.settle()
            await race.cancel_and_drain()
            self.assertFalse(getattr(race.first_task, "_log_traceback", False))
            self.assertEqual(rig.events.counts["wavelink_node_ready"], 1)

            await rig.close_fixture(first, suppress_errors=False)
            await rig.close_fixture(second, suppress_errors=False)
            self.assertNotIn("shared-identifier", wavelink.Pool.nodes)
        finally:
            await race.cancel_and_drain()

    async def test_scripted_close_socket_can_defer_real_websocket_close_once(self) -> None:
        rig = LifecycleFixtureRig()
        await rig.enter()
        self.addAsyncCleanup(rig.close)
        fixture = await rig.make_node(
            identifier="scripted-close",
            websocket_handler=make_ready_websocket_handler("scripted"),
            managed=False,
        )
        self.addAsyncCleanup(rig.close_fixture, fixture, suppress_errors=True)

        async with aiohttp.ClientSession() as session:
            raw = await session.ws_connect(f"{fixture.server.uri}/v4/websocket")
            scripted = ScriptedCloseSocket(raw, close_without_closing_attempts=1)
            self.assertTrue(await scripted.close())
            self.assertFalse(raw.closed)
            self.assertTrue(await scripted.close())
            self.assertTrue(raw.closed)

    async def test_lifecycle_fixture_rig_close_ejects_managed_nodes(self) -> None:
        rig = LifecycleFixtureRig()
        await rig.enter()

        fixture = await rig.make_node(
            identifier="managed-resource",
            websocket_handler=make_ready_websocket_handler("managed-resource"),
            managed=True,
        )
        await asyncio.wait_for(wavelink.Pool.connect(nodes=[fixture.node], client=rig.client), timeout=5.0)
        await rig.events.wait_for_count("wavelink_node_ready", 1)
        self.assertIn("managed-resource", wavelink.Pool.nodes)

        await rig.close()
        self.assertNotIn("managed-resource", wavelink.Pool.nodes)

    async def test_lifecycle_fixture_rig_enter_reentry_raises(self) -> None:
        rig = LifecycleFixtureRig()
        await rig.enter()
        self.addAsyncCleanup(rig.close)
        with self.assertRaisesRegex(RuntimeError, "already entered"):
            await rig.enter()

    async def test_unmanaged_node_context_performs_deterministic_cleanup(self) -> None:
        rig = LifecycleFixtureRig()
        await rig.enter()
        self.addAsyncCleanup(rig.close)

        async with rig.unmanaged_node(
            identifier="unmanaged-resource",
            websocket_handler=make_ready_websocket_handler("unmanaged-resource"),
        ) as fixture:
            await asyncio.wait_for(wavelink.Pool.connect(nodes=[fixture.node], client=rig.client), timeout=5.0)
            await rig.events.wait_for_count("wavelink_node_ready", 1)
            self.assertIn("unmanaged-resource", wavelink.Pool.nodes)

        self.assertNotIn("unmanaged-resource", wavelink.Pool.nodes)

    async def test_cancel_and_drain_observes_done_failed_task_and_clears_traceback_logging(self) -> None:
        async def fail_immediately() -> None:
            raise RuntimeError("done-failed-connect-task")

        async def block_until_cancelled() -> None:
            wait_forever = asyncio.Event()
            try:
                await wait_forever.wait()
            except asyncio.CancelledError:
                raise

        done_failed_task = asyncio.create_task(fail_immediately())
        done_failed_recorded = asyncio.Event()
        done_failed_task.add_done_callback(lambda _task: done_failed_recorded.set())
        pending_task = asyncio.create_task(block_until_cancelled())
        await asyncio.wait_for(done_failed_recorded.wait(), timeout=1.0)
        self.assertTrue(done_failed_task.done())

        race = SameIdentifierConnectRace(
            first_gate=ConnectReturnGate(),
            second_gate=ConnectReturnGate(),
            first_task=done_failed_task,
            second_task=pending_task,
        )
        await race.cancel_and_drain()

        self.assertTrue(pending_task.done())
        self.assertFalse(
            getattr(done_failed_task, "_log_traceback", False),
            "done failed tasks must be awaited/observed by cancel_and_drain",
        )

    async def test_close_fixture_runs_full_cleanup_and_reraises_first_error(self) -> None:
        rig = LifecycleFixtureRig()
        await rig.enter()
        self.addAsyncCleanup(rig.close)

        fixture = await rig.make_node(
            identifier="close-fixture-first-error",
            websocket_handler=make_ready_websocket_handler("close-fixture-first-error"),
            managed=True,
        )

        import wavelink

        foreign = object()
        pool_registry = getattr(wavelink.Pool, "_Pool__nodes")
        pool_registry[fixture.node.identifier] = foreign  # type: ignore[assignment]

        call_order: list[str] = []
        first_error = RuntimeError("node-close-first-error")
        second_error = RuntimeError("session-close-second-error")
        third_error = RuntimeError("server-close-third-error")

        original_session_close = fixture.session_spy.real.close
        original_server_close = fixture.server.close

        async def close_node_fail(*_args: object, **_kwargs: object) -> None:
            call_order.append("node")
            raise first_error

        async def close_session_fail() -> None:
            call_order.append("session")
            await original_session_close()
            raise second_error

        async def close_server_fail() -> None:
            call_order.append("server")
            await original_server_close()
            raise third_error

        fixture.node.close = close_node_fail  # type: ignore[method-assign]
        fixture.session_spy.real.close = close_session_fail  # type: ignore[method-assign]
        fixture.server.close = close_server_fail  # type: ignore[method-assign]

        try:
            with self.assertRaises(RuntimeError) as raised:
                await rig.close_fixture(fixture, suppress_errors=False)
            self.assertIs(raised.exception, first_error)
            self.assertEqual(call_order, ["node", "session", "server"])
            self.assertNotIn(fixture, rig.resources)
            self.assertIs(pool_registry.get(fixture.node.identifier), foreign)
        finally:
            pool_registry.pop(fixture.node.identifier, None)

    async def test_close_fixture_with_suppressed_errors_still_cleans_all_resources(self) -> None:
        rig = LifecycleFixtureRig()
        await rig.enter()
        self.addAsyncCleanup(rig.close)

        fixture = await rig.make_node(
            identifier="close-fixture-suppressed-errors",
            websocket_handler=make_ready_websocket_handler("close-fixture-suppressed-errors"),
            managed=True,
        )

        call_order: list[str] = []
        original_session_close = fixture.session_spy.real.close
        original_server_close = fixture.server.close

        async def close_node_fail(*_args: object, **_kwargs: object) -> None:
            call_order.append("node")
            raise RuntimeError("node-close-error")

        async def close_session_fail() -> None:
            call_order.append("session")
            await original_session_close()
            raise RuntimeError("session-close-error")

        async def close_server_fail() -> None:
            call_order.append("server")
            await original_server_close()
            raise RuntimeError("server-close-error")

        fixture.node.close = close_node_fail  # type: ignore[method-assign]
        fixture.session_spy.real.close = close_session_fail  # type: ignore[method-assign]
        fixture.server.close = close_server_fail  # type: ignore[method-assign]

        await rig.close_fixture(fixture, suppress_errors=True)

        self.assertEqual(call_order, ["node", "session", "server"])
        self.assertNotIn(fixture, rig.resources)
        self.assertNotIn(fixture.node.identifier, wavelink.Pool.nodes)

    async def test_close_fixture_can_suppress_node_close_error_only(self) -> None:
        rig = LifecycleFixtureRig()
        await rig.enter()
        self.addAsyncCleanup(rig.close)

        fixture = await rig.make_node(
            identifier="close-fixture-suppress-node-only",
            websocket_handler=make_ready_websocket_handler("close-fixture-suppress-node-only"),
            managed=True,
        )

        call_order: list[str] = []
        original_session_close = fixture.session_spy.real.close
        original_server_close = fixture.server.close

        async def close_node_fail(*_args: object, **_kwargs: object) -> None:
            call_order.append("node")
            raise RuntimeError("node-close-error")

        async def close_session_success() -> None:
            call_order.append("session")
            await original_session_close()

        async def close_server_success() -> None:
            call_order.append("server")
            await original_server_close()

        fixture.node.close = close_node_fail  # type: ignore[method-assign]
        fixture.session_spy.real.close = close_session_success  # type: ignore[method-assign]
        fixture.server.close = close_server_success  # type: ignore[method-assign]

        await rig.close_fixture(
            fixture,
            suppress_errors=False,
            suppress_node_close_error=True,
        )

        self.assertEqual(call_order, ["node", "session", "server"])
        self.assertTrue(fixture.session_spy.real.closed)
        self.assertNotIn(fixture, rig.resources)
        self.assertNotIn(fixture.node.identifier, wavelink.Pool.nodes)

    async def test_close_fixture_suppressed_node_error_reraises_session_failure_and_settles_server(self) -> None:
        rig = LifecycleFixtureRig()
        await rig.enter()
        self.addAsyncCleanup(rig.close)

        fixture = await rig.make_node(
            identifier="close-fixture-session-failure-visible",
            websocket_handler=make_ready_websocket_handler("close-fixture-session-failure-visible"),
            managed=True,
        )

        call_order: list[str] = []
        session_error = RuntimeError("session-close-visible-error")
        original_session_close = fixture.session_spy.real.close
        original_server_close = fixture.server.close

        async def close_node_fail(*_args: object, **_kwargs: object) -> None:
            call_order.append("node")
            raise RuntimeError("node-close-suppressed-error")

        async def close_session_fail() -> None:
            call_order.append("session")
            await original_session_close()
            raise session_error

        async def close_server_success() -> None:
            call_order.append("server")
            await original_server_close()

        fixture.node.close = close_node_fail  # type: ignore[method-assign]
        fixture.session_spy.real.close = close_session_fail  # type: ignore[method-assign]
        fixture.server.close = close_server_success  # type: ignore[method-assign]

        with self.assertRaises(RuntimeError) as raised:
            await rig.close_fixture(
                fixture,
                suppress_errors=False,
                suppress_node_close_error=True,
            )
        self.assertIs(raised.exception, session_error)
        self.assertEqual(call_order, ["node", "session", "server"])
        self.assertNotIn(fixture, rig.resources)
        self.assertNotIn(fixture.node.identifier, wavelink.Pool.nodes)

    async def test_close_fixture_suppressed_node_error_reraises_server_failure_when_session_succeeds(self) -> None:
        rig = LifecycleFixtureRig()
        await rig.enter()
        self.addAsyncCleanup(rig.close)

        fixture = await rig.make_node(
            identifier="close-fixture-server-failure-visible",
            websocket_handler=make_ready_websocket_handler("close-fixture-server-failure-visible"),
            managed=True,
        )

        call_order: list[str] = []
        server_error = RuntimeError("server-close-visible-error")
        original_session_close = fixture.session_spy.real.close
        original_server_close = fixture.server.close

        async def close_node_fail(*_args: object, **_kwargs: object) -> None:
            call_order.append("node")
            raise RuntimeError("node-close-suppressed-error")

        async def close_session_success() -> None:
            call_order.append("session")
            await original_session_close()

        async def close_server_fail() -> None:
            call_order.append("server")
            await original_server_close()
            raise server_error

        fixture.node.close = close_node_fail  # type: ignore[method-assign]
        fixture.session_spy.real.close = close_session_success  # type: ignore[method-assign]
        fixture.server.close = close_server_fail  # type: ignore[method-assign]

        with self.assertRaises(RuntimeError) as raised:
            await rig.close_fixture(
                fixture,
                suppress_errors=False,
                suppress_node_close_error=True,
            )
        self.assertIs(raised.exception, server_error)
        self.assertEqual(call_order, ["node", "session", "server"])
        self.assertNotIn(fixture, rig.resources)
        self.assertNotIn(fixture.node.identifier, wavelink.Pool.nodes)

    async def test_rig_close_surfaces_first_resource_failure_and_settles_later_resources(self) -> None:
        rig = LifecycleFixtureRig()
        await rig.enter()

        first_fixture = await rig.make_node(
            identifier="close-rig-first",
            websocket_handler=make_ready_websocket_handler("close-rig-first"),
            managed=True,
        )
        second_fixture = await rig.make_node(
            identifier="close-rig-second",
            websocket_handler=make_ready_websocket_handler("close-rig-second"),
            managed=True,
        )

        call_order: list[str] = []
        first_error = RuntimeError("close-rig-first-error")
        original_first_session_close = first_fixture.session_spy.real.close
        original_second_session_close = second_fixture.session_spy.real.close

        async def close_second_node_fail(*_args: object, **_kwargs: object) -> None:
            call_order.append("second-node")
            raise first_error

        async def close_first_session() -> None:
            call_order.append("first-session")
            await original_first_session_close()

        async def close_second_session() -> None:
            call_order.append("second-session")
            await original_second_session_close()

        second_fixture.node.close = close_second_node_fail  # type: ignore[method-assign]
        first_fixture.session_spy.real.close = close_first_session  # type: ignore[method-assign]
        second_fixture.session_spy.real.close = close_second_session  # type: ignore[method-assign]

        with self.assertRaises(RuntimeError) as raised:
            await rig.close()
        self.assertIs(raised.exception, first_error)
        self.assertEqual(call_order, ["second-node", "second-session", "first-session"])
        self.assertTrue(first_fixture.session_spy.real.closed)
        self.assertTrue(second_fixture.session_spy.real.closed)
        self.assertFalse(rig.resources)
        self.assertIsNone(rig.client)
        self.assertFalse(rig._entered)
