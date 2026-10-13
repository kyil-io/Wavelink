from __future__ import annotations

import asyncio
import unittest

import aiohttp

import wavelink
from tests.lifecycle_fixtures import (
    LifecycleFixtureRig,
    ScriptedCloseSocket,
    begin_same_identifier_connect_race,
    build_player_update_payload,
    build_ready_payload,
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

        async with aiohttp.ClientSession() as session, session.ws_connect(
            f"{fixture.server.uri}/v4/websocket"
        ) as websocket:
            first = await websocket.receive_json()
            second = await websocket.receive_json()
            self.assertEqual(first, {"op": "ready", "resumed": False, "sessionId": "ready-handler-1"})
            self.assertEqual(second["op"], "playerUpdate")
            await websocket.close()

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
                await websocket.close()

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

        race = begin_same_identifier_connect_race(first=first, second=second, client=rig.client)
        try:
            await race.wait_until_both_candidates_ready()
            race.release_first()
            race.release_second()
            await race.settle()
            self.assertGreaterEqual(first.session_spy.ws_connect_calls, 1)
            self.assertGreaterEqual(second.session_spy.ws_connect_calls, 1)
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
