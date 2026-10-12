"""
MIT License

Copyright (c) 2019-Current PythonistaGuild, EvieePy

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar

import aiohttp

from . import __version__
from .backoff import Backoff
from .enums import NodeStatus
from .exceptions import AuthorizationFailedException, NodeException
from .payloads import *
from .tracks import Playable


if TYPE_CHECKING:
    from collections.abc import Coroutine

    from .node import Node
    from .player import Player
    from .types.request import UpdateSessionRequest
    from .types.response import InfoResponse
    from .types.state import PlayerState
    from .types.websocket import TrackExceptionPayload, WebsocketOP


logger: logging.Logger = logging.getLogger(__name__)
LOGGER_TRACK: logging.Logger = logging.getLogger("TrackException")
T = TypeVar("T")


@dataclass(frozen=True)
class _CleanupOutcome:
    drain_snapshot: tuple[asyncio.Task[Any], ...]
    errors: tuple[Exception, ...]
    primary_socket_disposed: bool


class Websocket:
    def __init__(self, *, node: Node) -> None:
        self.node = node

        self.backoff: Backoff = Backoff()

        self.socket: aiohttp.ClientWebSocketResponse | None = None
        self.keep_alive_task: asyncio.Task[None] | None = None
        self.reconnect_task: asyncio.Task[None] | None = None
        self.cleanup_task: asyncio.Task[_CleanupOutcome] | None = None
        self._connected_interval_active: bool = False
        self._last_cleanup_outcome: _CleanupOutcome | None = None

    @staticmethod
    def _observe_task_result(task: asyncio.Task[Any], *, role: str, node: Node) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("Unexpected %s task failure on node=%r", role, node.identifier)

    def _create_owned_task(
        self,
        coro: Coroutine[Any, Any, T],
        *,
        role: str,
    ) -> asyncio.Task[T]:
        task = asyncio.create_task(coro)
        task.add_done_callback(lambda t: self._observe_task_result(t, role=role, node=self.node))
        return task

    def _dispatch_disconnected_once(self) -> None:
        if not self._connected_interval_active:
            return

        self._connected_interval_active = False
        payload: NodeDisconnectedEventPayload = NodeDisconnectedEventPayload(node=self.node)
        self.dispatch("node_disconnected", payload)

    @staticmethod
    def _append_error_chain(primary: Exception, secondary: Exception) -> None:
        cursor: Exception = primary
        while cursor.__cause__ is not None and isinstance(cursor.__cause__, Exception):
            cursor = cursor.__cause__
        if cursor is secondary:
            return
        cursor.__cause__ = secondary

    @property
    def headers(self) -> dict[str, str]:
        assert self.node.client is not None
        assert self.node.client.user is not None

        data = {
            "Authorization": self.node.password,
            "User-Id": str(self.node.client.user.id),
            "Client-Name": f"Wavelink/{__version__}",
        }

        if self.node.session_id:
            data["Session-Id"] = self.node.session_id

        return data

    def is_connected(self) -> bool:
        return self.socket is not None and not self.socket.closed

    async def _update_node(self) -> None:
        if self.node._resume_timeout > 0:
            udata: UpdateSessionRequest = {"resuming": True, "timeout": self.node._resume_timeout}
            await self.node._update_session(data=udata)

        info: InfoResponse = await self.node._fetch_info()
        if "spotify" in info["sourceManagers"]:
            self.node._spotify_enabled = True

    async def connect(self) -> None:
        if self.node._terminal_close_requested or self.node._websocket is not self:
            return

        self.node._status = NodeStatus.CONNECTING

        if self.keep_alive_task:
            try:
                self.keep_alive_task.cancel()
            except Exception as e:
                logger.debug(
                    (
                        "Failed to cancel websocket keep alive while connecting. "
                        "This is most likely not a problem and will not affect websocket connection: '%s'"
                    ),
                    e,
                )

        retries: int | None = self.node._retries
        session: aiohttp.ClientSession = self.node._session
        heartbeat: float = self.node.heartbeat
        uri: str = f"{self.node.uri.removesuffix('/')}/v4/websocket"
        github: str = "https://github.com/PythonistaGuild/Wavelink/issues"

        while True:
            if self.node._terminal_close_requested or self.node._websocket is not self:
                return

            candidate_socket: aiohttp.ClientWebSocketResponse | None = None
            try:
                candidate_socket = await session.ws_connect(url=uri, heartbeat=heartbeat, headers=self.headers)  # type: ignore
            except Exception as e:
                if isinstance(e, aiohttp.WSServerHandshakeError) and e.status == 401:
                    await self.cleanup()
                    raise AuthorizationFailedException from e
                elif isinstance(e, aiohttp.WSServerHandshakeError) and e.status == 404:
                    await self.cleanup()
                    raise NodeException from e
                else:
                    logger.warning(
                        (
                            'An unexpected error occurred while connecting %r to Lavalink: "%s"\n'
                            "If this error persists or wavelink is unable to reconnect, please see: %s"
                        ),
                        self.node,
                        e,
                        github,
                    )
            else:
                if self.node._terminal_close_requested or self.node._websocket is not self:
                    if candidate_socket is not None:
                        try:
                            await candidate_socket.close()
                        except Exception:
                            self.node._retain_failed_candidate_socket(candidate_socket)
                            raise
                    return

                self.socket = candidate_socket

            if self.is_connected():
                self.keep_alive_task = self._create_owned_task(self.keep_alive(), role="keep_alive")
                break

            if retries == 0:
                logger.warning(
                    (
                        '%r was unable to successfully connect/reconnect to Lavalink after "%s" connection attempt. '
                        "This Node has exhausted the retry count."
                    ),
                    self.node,
                    retries + 1,
                )

                await self.cleanup()
                break

            if retries:
                retries -= 1

            delay: float = self.backoff.calculate()
            logger.info('%r retrying websocket connection in "%s" seconds.', self.node, delay)

            await asyncio.sleep(delay)
            if self.node._terminal_close_requested or self.node._websocket is not self:
                return

    async def keep_alive(self) -> None:
        assert self.socket is not None

        while True:
            message: aiohttp.WSMessage = await self.socket.receive()

            if message.type in (  # pyright: ignore[reportUnknownMemberType]
                aiohttp.WSMsgType.CLOSED,
                aiohttp.WSMsgType.CLOSING,
            ):
                self._dispatch_disconnected_once()

                if self.node._terminal_close_requested or self.node._websocket is not self:
                    break

                reconnect_task = self.reconnect_task
                if reconnect_task is None or reconnect_task.done():
                    reconnect_task = self._create_owned_task(self.connect(), role="reconnect")
                    reconnect_task.add_done_callback(self._retire_reconnect_task)
                    self.reconnect_task = reconnect_task
                break

            if message.data is None:  # pyright: ignore[reportUnknownMemberType]
                logger.debug("Received an empty message from Lavalink websocket. Disregarding.")
                continue

            data: WebsocketOP = message.json()

            if data["op"] == "ready":
                resumed: bool = data["resumed"]
                session_id: str = data["sessionId"]

                self.node._status = NodeStatus.CONNECTED
                self.node._session_id = session_id
                self._connected_interval_active = True

                await self._update_node()

                ready_payload: NodeReadyEventPayload = NodeReadyEventPayload(
                    node=self.node, resumed=resumed, session_id=session_id
                )
                self.dispatch("node_ready", ready_payload)

            elif data["op"] == "playerUpdate":
                playerup: Player | None = self.get_player(data["guildId"])
                state: PlayerState = data["state"]

                updatepayload: PlayerUpdateEventPayload = PlayerUpdateEventPayload(player=playerup, state=state)
                self.dispatch("player_update", updatepayload)

                if playerup:
                    asyncio.create_task(playerup._update_event(updatepayload))

            elif data["op"] == "stats":
                statspayload: StatsEventPayload = StatsEventPayload(data=data)
                self.node._total_player_count = statspayload.players
                self.dispatch("stats_update", statspayload)

            elif data["op"] == "event":
                player: Player | None = self.get_player(data["guildId"])

                if data["type"] == "TrackStartEvent":
                    track: Playable = Playable(data["track"])

                    startpayload: TrackStartEventPayload = TrackStartEventPayload(player=player, track=track)
                    self.dispatch("track_start", startpayload)

                    if player:
                        asyncio.create_task(player._track_start(startpayload))

                elif data["type"] == "TrackEndEvent":
                    track: Playable = Playable(data["track"])
                    reason: str = data["reason"]

                    if player and reason != "replaced":
                        player._current = None

                    endpayload: TrackEndEventPayload = TrackEndEventPayload(player=player, track=track, reason=reason)
                    self.dispatch("track_end", endpayload)

                    if player:
                        asyncio.create_task(player._auto_play_event(endpayload))

                elif data["type"] == "TrackExceptionEvent":
                    track: Playable = Playable(data["track"])
                    exception: TrackExceptionPayload = data["exception"]

                    excpayload: TrackExceptionEventPayload = TrackExceptionEventPayload(
                        player=player, track=track, exception=exception
                    )

                    LOGGER_TRACK.error(
                        (
                            "A Lavalink TrackException was received on %r for "
                            "player %r: %s, caused by: %s, with severity: %s"
                        ),
                        self.node,
                        player,
                        exception.get("message", ""),
                        exception["cause"],
                        exception["severity"],
                    )
                    self.dispatch("track_exception", excpayload)

                elif data["type"] == "TrackStuckEvent":
                    track: Playable = Playable(data["track"])
                    threshold: int = data["thresholdMs"]

                    stuckpayload: TrackStuckEventPayload = TrackStuckEventPayload(
                        player=player, track=track, threshold=threshold
                    )
                    self.dispatch("track_stuck", stuckpayload)

                elif data["type"] == "WebSocketClosedEvent":
                    code: int = data["code"]
                    reason: str = data["reason"]
                    by_remote: bool = data["byRemote"]

                    wcpayload: WebsocketClosedEventPayload = WebsocketClosedEventPayload(
                        player=player, code=code, reason=reason, by_remote=by_remote
                    )
                    self.dispatch("websocket_closed", wcpayload)

                    if player:
                        asyncio.create_task(player._disconnected_wait(code, by_remote))

                else:
                    other_payload: ExtraEventPayload = ExtraEventPayload(node=self.node, player=player, data=data)
                    self.dispatch("extra_event", other_payload)
            else:
                logger.debug("'Received an unknown OP from Lavalink '%s'. Disregarding.", data["op"])

    def get_player(self, guild_id: str | int) -> Player | None:
        return self.node.get_player(int(guild_id))

    def dispatch(self, event: str, /, *args: Any, **kwargs: Any) -> None:
        assert self.node.client is not None

        self.node.client.dispatch(f"wavelink_{event}", *args, **kwargs)
        logger.debug("%r dispatched the event 'on_wavelink_%s'", self.node, event)

    def _retire_reconnect_task(self, task: asyncio.Task[Any]) -> None:
        if self.reconnect_task is task:
            self.reconnect_task = None

    def _retire_cleanup_task(self, task: asyncio.Task[Any]) -> None:
        if self.cleanup_task is task:
            self.cleanup_task = None

    async def _cleanup_owner(self, *, initiator: asyncio.Task[Any] | None) -> _CleanupOutcome:
        owned_tasks: list[asyncio.Task[Any]] = []

        if self.keep_alive_task is not None:
            owned_tasks.append(self.keep_alive_task)
        if self.reconnect_task is not None:
            owned_tasks.append(self.reconnect_task)

        drain_snapshot: list[asyncio.Task[Any]] = []
        for task in owned_tasks:
            drain_snapshot.append(task)
            if task is initiator:
                continue
            task.cancel()

        close_error: Exception | None = None
        dispatch_error: Exception | None = None
        socket = self.socket
        primary_socket_disposed = socket is None
        if socket is not None:
            try:
                await socket.close()
            except Exception as exc:
                close_error = exc
            else:
                self.socket = None
            primary_socket_disposed = socket.closed

        should_clear_node_state = self.node._websocket is self
        was_connected = self.node._status is NodeStatus.CONNECTED
        if should_clear_node_state:
            self.node._status = NodeStatus.DISCONNECTED
            self.node._session_id = None
            self.node._players = {}
            if close_error is None:
                self.node._websocket = None

        if should_clear_node_state and was_connected:
            try:
                self._dispatch_disconnected_once()
            except Exception as exc:
                dispatch_error = exc

        self.node._release_closed_retained_candidate_sockets()

        if close_error is None and dispatch_error is None:
            logger.debug("Successfully cleaned up the websocket for %r", self.node)
        else:
            logger.debug("Websocket cleanup for %r completed with close error", self.node)

        errors = tuple(error for error in (close_error, dispatch_error) if error is not None)
        return _CleanupOutcome(
            drain_snapshot=tuple(drain_snapshot),
            errors=errors,
            primary_socket_disposed=primary_socket_disposed,
        )

    async def cleanup(self) -> None:
        caller = asyncio.current_task()
        cleanup_task = self.cleanup_task

        if cleanup_task is None or cleanup_task.done():
            cleanup_task = self._create_owned_task(self._cleanup_owner(initiator=caller), role="cleanup")
            cleanup_task.add_done_callback(self._retire_cleanup_task)
            self.cleanup_task = cleanup_task

        cleanup_outcome = await asyncio.shield(cleanup_task)
        self._last_cleanup_outcome = cleanup_outcome

        for task in cleanup_outcome.drain_snapshot:
            if task is caller:
                continue
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.debug(
                    (
                        "Owned task failure during websocket cleanup for %r was already observed by the owned task "
                        "callback."
                    ),
                    self.node,
                )

        if cleanup_outcome.errors:
            primary_error = cleanup_outcome.errors[0]
            for secondary_error in cleanup_outcome.errors[1:]:
                self._append_error_chain(primary_error, secondary_error)
            raise primary_error
