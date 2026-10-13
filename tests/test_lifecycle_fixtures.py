from __future__ import annotations

import asyncio
import unittest
from urllib.parse import urlparse

import aiohttp
import discord
from aiohttp import web

from tests.lifecycle_fixtures import (
    ConnectReturnGate,
    EventRecorder,
    LoopbackLavalink,
    SessionSpy,
    TransportCloseGate,
    install_offline_client_user,
)


class TestLifecycleFixtures(unittest.IsolatedAsyncioTestCase):
    def test_support_module_is_importable(self) -> None:
        assert install_offline_client_user is not None
        assert SessionSpy is not None
        assert EventRecorder is not None
        assert TransportCloseGate is not None
        assert ConnectReturnGate is not None
        assert LoopbackLavalink is not None

    async def test_transport_close_gate_waits_for_release(self) -> None:
        gate = TransportCloseGate()

        pending = asyncio.create_task(gate.before_close())
        await asyncio.wait_for(gate.entered.wait(), timeout=1.0)
        self.assertFalse(pending.done())

        gate.release()
        await asyncio.wait_for(pending, timeout=1.0)
        self.assertEqual(gate.close_calls, 1)

    async def test_event_recorder_wait_for_count_reacts_to_dispatch(self) -> None:
        client = discord.Client(intents=discord.Intents.none())
        install_offline_client_user(client)
        await client.__aenter__()
        self.addAsyncCleanup(client.__aexit__, None, None, None)

        recorder = EventRecorder()
        recorder.attach(client)
        waiter_registered = asyncio.Event()
        recorder.on_waiter_count("wavelink_node_ready", 1, waiter_registered.set)
        waiter = asyncio.create_task(recorder.wait_for_count("wavelink_node_ready", 1))

        await asyncio.wait_for(waiter_registered.wait(), timeout=1.0)
        self.assertFalse(waiter.done())

        client.dispatch("wavelink_node_ready")
        await asyncio.wait_for(waiter, timeout=1.0)
        self.assertEqual(recorder.counts["wavelink_node_ready"], 1)

    async def test_event_recorder_timeout_waiter_cleanup_removes_only_timed_out_waiter(self) -> None:
        client = discord.Client(intents=discord.Intents.none())
        install_offline_client_user(client)
        await client.__aenter__()
        self.addAsyncCleanup(client.__aexit__, None, None, None)

        recorder = EventRecorder()
        recorder.attach(client)
        both_waiters_registered = asyncio.Event()
        recorder.on_waiter_count("wavelink_node_ready", 2, both_waiters_registered.set)

        surviving_waiter = asyncio.create_task(recorder.wait_for_count("wavelink_node_ready", 1, timeout=5.0))
        timed_out_waiter = asyncio.create_task(recorder.wait_for_count("wavelink_node_ready", 2, timeout=0.0))

        await asyncio.wait_for(both_waiters_registered.wait(), timeout=1.0)
        with self.assertRaises(asyncio.TimeoutError):
            await timed_out_waiter

        abandoned_waiters_counted = asyncio.Event()
        recorder.on_waiter_count("wavelink_node_ready", 2, abandoned_waiters_counted.set)
        self.assertFalse(abandoned_waiters_counted.is_set())

        client.dispatch("wavelink_node_ready")
        await asyncio.wait_for(surviving_waiter, timeout=1.0)
        self.assertEqual(recorder.counts["wavelink_node_ready"], 1)

    async def test_event_recorder_cancelled_waiter_cleanup_removes_only_cancelled_waiter(self) -> None:
        client = discord.Client(intents=discord.Intents.none())
        install_offline_client_user(client)
        await client.__aenter__()
        self.addAsyncCleanup(client.__aexit__, None, None, None)

        recorder = EventRecorder()
        recorder.attach(client)
        both_waiters_registered = asyncio.Event()
        recorder.on_waiter_count("wavelink_node_ready", 2, both_waiters_registered.set)

        surviving_waiter = asyncio.create_task(recorder.wait_for_count("wavelink_node_ready", 1, timeout=5.0))
        cancelled_waiter = asyncio.create_task(recorder.wait_for_count("wavelink_node_ready", 2, timeout=5.0))

        await asyncio.wait_for(both_waiters_registered.wait(), timeout=1.0)
        cancelled_waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await cancelled_waiter

        abandoned_waiters_counted = asyncio.Event()
        recorder.on_waiter_count("wavelink_node_ready", 2, abandoned_waiters_counted.set)
        self.assertFalse(abandoned_waiters_counted.is_set())

        client.dispatch("wavelink_node_ready")
        await asyncio.wait_for(surviving_waiter, timeout=1.0)
        self.assertEqual(recorder.counts["wavelink_node_ready"], 1)

    async def test_loopback_lavalink_serves_info_and_websocket(self) -> None:
        async def websocket_handler(request: web.Request, number: int) -> web.StreamResponse:
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            await ws.send_json({"op": "ready", "sessionId": f"s-{number}"})
            await ws.close()
            return ws

        server = LoopbackLavalink(websocket_handler)
        await server.start()
        self.addAsyncCleanup(server.close)

        async with aiohttp.ClientSession() as session:
            async with session.get(f"{server.uri}/v4/info") as response:
                payload = await response.json()
                self.assertEqual(response.status, 200)
                self.assertIn("version", payload)

            async with session.ws_connect(f"{server.uri}/v4/websocket") as websocket:
                message = await websocket.receive_json()
                self.assertEqual(message["op"], "ready")

        parsed_uri = urlparse(server.uri)
        self.assertIsNotNone(parsed_uri.hostname)
        self.assertIsNotNone(parsed_uri.port)
        await server.close()
        with self.assertRaises(OSError):
            _, writer = await asyncio.open_connection(parsed_uri.hostname or "", parsed_uri.port or 0)
            writer.close()
            await writer.wait_closed()

        self.assertEqual(server.info_count, 1)
        self.assertEqual(server.request_count, 1)

    async def test_session_spy_marks_post_boundary_connect(self) -> None:
        async def websocket_handler(request: web.Request, number: int) -> web.StreamResponse:
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            await ws.send_json({"n": number})
            await ws.close()
            return ws

        server = LoopbackLavalink(websocket_handler)
        await server.start()
        self.addAsyncCleanup(server.close)

        session_spy = SessionSpy(aiohttp.ClientSession())
        self.addAsyncCleanup(session_spy.real.close)

        first_socket = await session_spy.ws_connect(f"{server.uri}/v4/websocket")
        await first_socket.receive_json()
        await first_socket.close()

        session_spy.mark_boundary()
        second_socket = await session_spy.ws_connect(f"{server.uri}/v4/websocket")
        await second_socket.receive_json()
        await second_socket.close()

        await asyncio.wait_for(session_spy.post_boundary_call.wait(), timeout=1.0)
        self.assertEqual(session_spy.ws_connect_calls, 2)
        self.assertEqual(session_spy.call_events, [(1, False), (2, False)])

        await session_spy.real.close()
        self.assertTrue(session_spy.closed)
