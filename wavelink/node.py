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
import secrets
import urllib.parse
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypeAlias

import aiohttp
from discord.utils import classproperty

from . import __version__
from .enums import NodeStatus
from .exceptions import (
    AuthorizationFailedException,
    InvalidClientException,
    InvalidNodeException,
    LavalinkException,
    LavalinkLoadException,
    NodeException,
)
from .lfu import LFUCache
from .payloads import *
from .tracks import Playable, Playlist
from .websocket import Websocket


if TYPE_CHECKING:
    from collections.abc import Iterable

    import discord

    from .player import Player
    from .types.request import Request, UpdateSessionRequest
    from .types.response import (
        EmptyLoadedResponse,
        ErrorLoadedResponse,
        ErrorResponse,
        InfoResponse,
        PlayerResponse,
        PlaylistLoadedResponse,
        SearchLoadedResponse,
        StatsResponse,
        TrackLoadedResponse,
        UpdateResponse,
    )
    from .types.tracks import TrackPayload

    LoadedResponse: TypeAlias = (
        TrackLoadedResponse | SearchLoadedResponse | PlaylistLoadedResponse | EmptyLoadedResponse | ErrorLoadedResponse
    )


__all__ = ("Node", "Pool")


logger: logging.Logger = logging.getLogger(__name__)


Method = Literal["GET", "POST", "PATCH", "DELETE", "PUT", "OPTIONS"]


@dataclass(frozen=True, slots=True)
class _RetainedCloseOutcome:
    close_boundary: Websocket | None
    primary_socket: Any | None
    primary_socket_disposed: bool
    first_error: BaseException | None


@dataclass(slots=True)
class _RetainedCloseState:
    generation: int
    task: asyncio.Task[_RetainedCloseOutcome]
    close_boundary: Websocket | None
    eject_requested: bool
    ejected: bool = False
    owner_failure_observed: bool = False


class Node:
    """The Node represents a connection to Lavalink.

    The Node is responsible for keeping the websocket alive, resuming session, sending API requests and keeping track
    of connected all :class:`~wavelink.Player`.

    .. container:: operations

        .. describe:: node == other

            Equality check to determine whether this Node is equal to another reference of a Node.

        .. describe:: repr(node)

            The official string representation of this Node.

    Parameters
    ----------
    identifier: str | None
        A unique identifier for this Node. Could be ``None`` to generate a random one on creation.
    uri: str
        The URL/URI that wavelink will use to connect to Lavalink. Usually this is in the form of something like:
        ``http://localhost:2333`` which includes the port. But you could also provide a domain which won't require a
        port like ``https://lavalink.example.com`` or a public IP address and port like ``http://111.333.444.55:2333``.
    password: str
        The password used to connect and authorize this Node.
    session: aiohttp.ClientSession | None
        An optional :class:`aiohttp.ClientSession` used to connect this Node over websocket and REST.
        If ``None``, one will be generated for you. Defaults to ``None``.
    heartbeat: Optional[float]
        A ``float`` in seconds to ping your websocket keep alive. Usually you would not change this.
    retries: int | None
        A ``int`` of retries to attempt when connecting or reconnecting this Node. When the retries are exhausted
        the Node will be closed and cleaned-up. ``None`` will retry forever. Defaults to ``None``.
    client: :class:`discord.Client` | None
        The :class:`discord.Client` or subclasses, E.g. ``commands.Bot`` used to connect this Node. If this is *not*
        passed you must pass this to :meth:`wavelink.Pool.connect`.
    resume_timeout: Optional[int]
        The seconds this Node should configure Lavalink for resuming its current session in case of network issues.
        If this is ``0`` or below, resuming will be disabled. Defaults to ``60``.
    inactive_player_timeout: int | None
        Set the default for :attr:`wavelink.Player.inactive_timeout` on every player that connects to this node.
        Defaults to ``300``.
    inactive_channel_tokens: int | None
        Sets the default for :attr:`wavelink.Player.inactive_channel_tokens` on every player that connects to this node.
        Defaults to ``3``.

        See also: :func:`on_wavelink_inactive_player`.
    """

    def __init__(
        self,
        *,
        identifier: str | None = None,
        uri: str,
        password: str,
        session: aiohttp.ClientSession | None = None,
        heartbeat: float = 15.0,
        retries: int | None = None,
        client: discord.Client | None = None,
        resume_timeout: int = 60,
        inactive_player_timeout: int | None = 300,
        inactive_channel_tokens: int | None = 3,
    ) -> None:
        self._identifier = identifier or secrets.token_urlsafe(12)
        self._uri = uri.removesuffix("/")
        self._password = password
        self._session = session or aiohttp.ClientSession()
        self._heartbeat = heartbeat
        self._retries = retries
        self._client = client
        self._resume_timeout = resume_timeout

        self._status: NodeStatus = NodeStatus.DISCONNECTED
        self._has_closed: bool = False
        self._session_id: str | None = None

        self._players: dict[int, Player] = {}
        self._total_player_count: int | None = None

        self._spotify_enabled: bool = False

        self._websocket: Websocket | None = None
        self._retained_candidate_sockets: list[Any] = []
        self._retired_candidate_disposal_error: BaseException | None = None
        self._close_state: _RetainedCloseState | None = None
        self._close_state_lock: asyncio.Lock = asyncio.Lock()
        self._connect_generation: int = 0

        if inactive_player_timeout and inactive_player_timeout < 10:
            logger.warning('Setting "inactive_player_timeout" below 10 seconds may result in unwanted side effects.')

        self._inactive_player_timeout = (
            inactive_player_timeout if inactive_player_timeout and inactive_player_timeout > 0 else None
        )

        self._inactive_channel_tokens = inactive_channel_tokens

    def __repr__(self) -> str:
        return f"Node(identifier={self.identifier}, uri={self.uri}, status={self.status}, players={len(self.players)})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Node):
            return NotImplemented

        return other.identifier == self.identifier

    @property
    def headers(self) -> dict[str, str]:
        """A property that returns the headers configured for sending API and websocket requests.

        .. warning::

            This includes your Node password. Please be vigilant when using this property.
        """
        assert self.client is not None
        assert self.client.user is not None

        data = {
            "Authorization": self.password,
            "User-Id": str(self.client.user.id),
            "Client-Name": f"Wavelink/{__version__}",
        }

        return data

    @property
    def identifier(self) -> str:
        """The unique identifier for this :class:`Node`.


        .. versionchanged:: 3.0.0

            This property was previously known as ``id``.
        """
        return self._identifier

    @property
    def uri(self) -> str:
        """The URI used to connect this :class:`Node` to Lavalink."""
        return self._uri

    @property
    def status(self) -> NodeStatus:
        """The current :class:`Node` status.

        Refer to: :class:`~wavelink.NodeStatus`
        """
        return self._status

    @property
    def players(self) -> dict[int, Player]:
        """A mapping of :attr:`discord.Guild.id` to :class:`~wavelink.Player`.


        .. versionchanged:: 3.1.1

            This property now returns a shallow copy of the internal mapping.
        """
        return self._players.copy()

    @property
    def client(self) -> discord.Client | None:
        """Returns the :class:`discord.Client` associated with this :class:`Node`.

        Could be ``None`` if it has not been set yet.


        .. versionadded:: 3.0.0
        """
        return self._client

    @property
    def password(self) -> str:
        """Returns the password used to connect this :class:`Node` to Lavalink.

        .. versionadded:: 3.0.0
        """
        return self._password

    @property
    def heartbeat(self) -> float:
        """Returns the duration in seconds that the :class:`Node` websocket should send a heartbeat.

        .. versionadded:: 3.0.0
        """
        return self._heartbeat

    @property
    def session_id(self) -> str | None:
        """Returns the Lavalink session ID. Could be None if this :class:`Node` has not connected yet.

        .. versionadded:: 3.0.0
        """
        return self._session_id

    async def _pool_closer(self) -> None:
        try:
            await self._session.close()
        except Exception:
            pass

        if not self._has_closed:
            await self.close()

    async def close(self, eject: bool = False) -> None:
        """Method to close this Node and cleanup.

        After this method has finished, the event ``on_wavelink_node_closed`` will be fired.

        This method renders the Node websocket disconnected and disconnects all players.

        Parameters
        ----------
        eject: bool
            If ``True``, this will remove the Node from the Pool. Defaults to ``False``.


        .. versionchanged:: 3.2.1

            Added the ``eject`` parameter. Fixed a bug where the connected Players were not being disconnected.
        """
        close_state = await self._retain_close_state(eject=eject)
        outcome = await asyncio.shield(close_state.task)
        await self._maybe_eject_retained_state(close_state)

        if outcome.first_error is not None:
            raise outcome.first_error

    async def _retain_close_state(self, *, eject: bool) -> _RetainedCloseState:
        async with self._close_state_lock:
            state = self._close_state
            if state is None:
                self._connect_generation += 1
                close_boundary = self._websocket
                generation = self._connect_generation
                task = asyncio.create_task(
                    self._run_retained_close_owner(generation=generation, close_boundary=close_boundary)
                )
                state = _RetainedCloseState(
                    generation=generation,
                    task=task,
                    close_boundary=close_boundary,
                    eject_requested=eject,
                )
                task.add_done_callback(lambda task_: self._observe_retained_close_owner_result(state=state, task=task_))
                self._close_state = state
            else:
                state.eject_requested = state.eject_requested or eject
            return state

    def _observe_retained_close_owner_result(
        self, *, state: _RetainedCloseState, task: asyncio.Task[_RetainedCloseOutcome]
    ) -> None:
        try:
            outcome = task.result()
        except asyncio.CancelledError:
            return
        except Exception:
            if state.owner_failure_observed:
                return
            state.owner_failure_observed = True
            logger.exception("Unexpected retained close owner task failure for %r", self)
            return

        failure = outcome.first_error
        if failure is None or state.owner_failure_observed:
            return

        state.owner_failure_observed = True
        logger.error(
            "Retained close owner failed for %r",
            self,
            exc_info=(type(failure), failure, failure.__traceback__),
        )

    async def _run_retained_close_owner(
        self, *, generation: int, close_boundary: Websocket | None
    ) -> _RetainedCloseOutcome:
        first_error: BaseException | None = None
        closed_dispatch_error: BaseException | None = None
        disconnected: list[Player] = []
        primary_socket: Any | None = None
        primary_socket_disposed = True

        for player in self._players.copy().values():
            try:
                await player.disconnect()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("An error occured while disconnecting a player in the close method of %r: %s", self, exc)
                if first_error is None:
                    first_error = exc

            disconnected.append(player)

        if close_boundary is not None:
            try:
                await close_boundary.cleanup()
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                if first_error is None:
                    first_error = exc

            cleanup_task = close_boundary._cleanup_task
            if cleanup_task is not None and cleanup_task.done():
                try:
                    cleanup_outcome = cleanup_task.result()
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:
                    if first_error is None:
                        first_error = exc
                else:
                    primary_socket = cleanup_outcome.primary_socket
                    primary_socket_disposed = cleanup_outcome.primary_socket_disposed
                    if first_error is None and cleanup_outcome.first_error is not None:
                        first_error = cleanup_outcome.first_error
        else:
            self._status = NodeStatus.DISCONNECTED
            self._session_id = None
            self._players = {}
            self._websocket = None

        self._has_closed = True

        await self._maybe_eject_retained_state(self._close_state, allow_owner_before_done=True)

        # Dispatch Node Closed Event... node, list of disconnected players
        if self.client is not None:
            try:
                self.client.dispatch("wavelink_node_closed", self, disconnected)
            except BaseException as exc:
                closed_dispatch_error = exc

        if first_error is None:
            first_error = closed_dispatch_error
        elif closed_dispatch_error is not None:
            self._append_error_chain(first_error, closed_dispatch_error)

        return _RetainedCloseOutcome(
            close_boundary=close_boundary,
            primary_socket=primary_socket,
            primary_socket_disposed=primary_socket_disposed,
            first_error=first_error,
        )

    async def _maybe_eject_retained_state(
        self, state: _RetainedCloseState | None, *, allow_owner_before_done: bool = False
    ) -> None:
        if state is None:
            return

        async with self._close_state_lock:
            if (current_state := self._close_state) is None or current_state is not state:
                return

            owner_incomplete = allow_owner_before_done and asyncio.current_task() is current_state.task
            if not current_state.task.done() and not owner_incomplete:
                return

            if not current_state.eject_requested or current_state.ejected:
                return

            nodes = getattr(Pool, "_Pool__nodes")
            if nodes.get(self.identifier) is self:
                nodes.pop(self.identifier, None)
            current_state.ejected = True

    async def _reserve_connect_generation(self) -> int:
        async with self._close_state_lock:
            self._connect_generation += 1
            return self._connect_generation

    def _retain_failed_candidate_socket(self, socket: Any) -> None:
        if socket.closed:
            return
        if socket in self._retained_candidate_sockets:
            return
        self._retained_candidate_sockets.append(socket)

    def _release_closed_retained_candidate_sockets(self) -> None:
        if not self._retained_candidate_sockets:
            return
        self._retained_candidate_sockets = [socket for socket in self._retained_candidate_sockets if not socket.closed]

    def _has_unresolved_retained_candidate_sockets(self) -> bool:
        self._release_closed_retained_candidate_sockets()
        return any(not socket.closed for socket in self._retained_candidate_sockets)

    def _is_current_pool_member(self) -> bool:
        return getattr(Pool, "_Pool__nodes").get(self.identifier) is self

    async def _is_connect_generation_invalid(self, *, generation: int) -> bool:
        async with self._close_state_lock:
            obsolete = generation != self._connect_generation
            return obsolete

    async def _join_and_claim_retained_close_state(self, *, generation: int) -> None:
        state: _RetainedCloseState | None
        async with self._close_state_lock:
            state = self._close_state

        if state is None:
            return

        await asyncio.shield(state.task)

        async with self._close_state_lock:
            current_state = self._close_state
            if current_state is not state:
                return

            if generation != self._connect_generation:
                raise NodeException(f"Unable to connect {self!r} due to obsolete generation or closed lifecycle state.")

            outcome = state.task.result()
            if outcome.first_error is not None and not outcome.primary_socket_disposed:
                raise outcome.first_error

            if not outcome.primary_socket_disposed:
                raise RuntimeError(
                    f"Unable to connect {self!r} while a previously retained websocket candidate remains unresolved."
                )

            if self._has_unresolved_retained_candidate_sockets():
                raise RuntimeError(
                    f"Unable to connect {self!r} while a previously retained websocket candidate remains unresolved."
                )

            if state.close_boundary is not outcome.close_boundary:
                raise NodeException(f"Unable to connect {self!r} due to retained close boundary mismatch.")

            self._close_state = None

    async def _connect(
        self, *, client: discord.Client | None, generation: int | None = None, require_pool_membership: bool = False
    ) -> None:
        client_ = self._client or client

        if not client_:
            raise InvalidClientException(f"Unable to connect {self!r} as you have not provided a valid discord.Client.")

        if require_pool_membership and not self._is_current_pool_member():
            return

        if generation is None:
            generation = await self._reserve_connect_generation()

        await self._join_and_claim_retained_close_state(generation=generation)

        if require_pool_membership and not self._is_current_pool_member():
            return

        if await self._is_connect_generation_invalid(generation=generation):
            raise NodeException(f"Unable to connect {self!r} due to obsolete generation or closed lifecycle state.")

        if self._has_unresolved_retained_candidate_sockets():
            raise RuntimeError(
                f"Unable to connect {self!r} while a previously retained websocket candidate remains unresolved."
            )

        if require_pool_membership and not self._is_current_pool_member():
            return

        self._client = client_
        self._has_closed = False
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession()

        websocket: Websocket = Websocket(node=self)
        self._websocket = websocket
        self._retired_candidate_disposal_error = None
        await websocket.connect()

        if await self._is_connect_generation_invalid(generation=generation):
            try:
                await self._retire_obsolete_connect_candidate(websocket=websocket)
            except BaseException as exc:
                self._retired_candidate_disposal_error = exc
                raise
            raise NodeException(f"Unable to connect {self!r} due to obsolete generation or closed lifecycle state.")

    async def _retire_obsolete_connect_candidate(self, *, websocket: Websocket) -> None:
        current_task = asyncio.current_task()
        owned_tasks: list[asyncio.Task[Any]] = []
        first_error: BaseException | None = None

        for task in (websocket.keep_alive_task, websocket.reconnect_task):
            if task is None or task is current_task:
                continue
            task.cancel()
            owned_tasks.append(task)

        socket = websocket.socket
        if socket is not None and not socket.closed:
            try:
                await socket.close()
            except asyncio.CancelledError:
                self._retain_failed_candidate_socket(socket)
                raise
            except BaseException as exc:
                self._retain_failed_candidate_socket(socket)
                if first_error is None:
                    first_error = exc
            else:
                if not socket.closed:
                    self._retain_failed_candidate_socket(socket)

        for task in (websocket.keep_alive_task, websocket.reconnect_task):
            if task is None or task is current_task or task in owned_tasks:
                continue
            task.cancel()
            owned_tasks.append(task)

        if owned_tasks:
            settled_tasks = await asyncio.gather(*owned_tasks, return_exceptions=True)
            for settled in settled_tasks:
                if isinstance(settled, asyncio.CancelledError):
                    continue
                if isinstance(settled, BaseException) and first_error is None:
                    first_error = settled

        if self._websocket is websocket:
            self._status = NodeStatus.DISCONNECTED
            self._session_id = None
            self._players = {}
            self._websocket = None

        if first_error is not None:
            raise first_error

    @staticmethod
    def _append_error_chain(first_error: BaseException, appended_error: BaseException) -> None:
        if first_error is appended_error:
            return

        visited: set[int] = set()
        primary_chain_ids: set[int] = set()
        current: BaseException = first_error

        while True:
            current_id = id(current)
            if current_id in visited:
                return

            visited.add(current_id)
            primary_chain_ids.add(current_id)
            cause = current.__cause__

            if cause is None:
                appended_visited: set[int] = set()
                appended_current: BaseException | None = appended_error
                while appended_current is not None:
                    appended_id = id(appended_current)
                    if appended_id in primary_chain_ids:
                        return
                    if appended_id in appended_visited:
                        break
                    appended_visited.add(appended_id)
                    appended_current = appended_current.__cause__

                current.__cause__ = appended_error
                return

            if cause is appended_error:
                return

            current = cause

    async def send(
        self, method: Method = "GET", *, path: str, data: Any | None = None, params: dict[str, Any] | None = None
    ) -> Any:
        """Method for making requests to the Lavalink node.

        .. warning::

            Usually you wouldn't use this method. Please use the built in methods of :class:`~Node`, :class:`~Pool`
            and :class:`~wavelink.Player`, unless you need to send specific plugin data to Lavalink.

            Using this method may have unwanted side effects on your players and/or nodes.

        Parameters
        ----------
        method: Optional[str]
            The method to use when making this request. Available methods are
            "GET", "POST", "PATCH", "PUT", "DELETE" and "OPTIONS". Defaults to "GET".
        path: str
            The path to make this request to. E.g. "/v4/stats".
        data: Any | None
            The optional JSON data to send along with your request to Lavalink. This should be a dict[str, Any]
            and able to be converted to JSON.
        params: Optional[dict[str, Any]]
            An optional dict of query parameters to send with your request to Lavalink. If you include your query
            parameters in the ``path`` parameter, do not pass them here as well. E.g. {"thing": 1, "other": 2}
            would equate to "?thing=1&other=2".

        Returns
        -------
        Any
            The response from Lavalink which will either be None, a str or JSON.

        Raises
        ------
        LavalinkException
            An error occurred while making this request to Lavalink.
        NodeException
            An error occured while making this request to Lavalink,
            and Lavalink was unable to send any error information.


        .. versionadded:: 3.0.0
        """
        clean_path: str = path.removesuffix("/")
        uri: str = f"{self.uri}/{clean_path}"

        if params is None:
            params = {}

        async with self._session.request(
            method=method, url=uri, params=params, json=data, headers=self.headers
        ) as resp:
            if resp.status == 204:
                return

            if resp.status >= 300:
                try:
                    exc_data: ErrorResponse = await resp.json()
                except Exception as e:
                    logger.warning("An error occured making a request on %r: %s", self, e)
                    raise NodeException(status=resp.status)

                raise LavalinkException(data=exc_data)

            try:
                rdata: Any = await resp.json()
            except aiohttp.ContentTypeError:
                pass
            else:
                return rdata

            try:
                body: str = await resp.text()
            except aiohttp.ClientError:
                return

            return body

    async def _fetch_players(self) -> list[PlayerResponse]:
        uri: str = f"{self.uri}/v4/sessions/{self.session_id}/players"

        async with self._session.get(url=uri, headers=self.headers) as resp:
            if resp.status == 200:
                resp_data: list[PlayerResponse] = await resp.json()
                return resp_data

            else:
                try:
                    exc_data: ErrorResponse = await resp.json()
                except Exception as e:
                    logger.warning("An error occured making a request on %r: %s", self, e)
                    raise NodeException(status=resp.status)

                raise LavalinkException(data=exc_data)

    async def fetch_players(self) -> list[PlayerResponsePayload]:
        """Method to fetch the player information Lavalink holds for every connected player on this node.

        .. warning::

            This payload is not the same as the :class:`wavelink.Player` class. This is the data received from
            Lavalink about the players.


        Returns
        -------
        list[:class:`PlayerResponsePayload`]
            A list of :class:`PlayerResponsePayload` representing each player connected to this node.

        Raises
        ------
        LavalinkException
            An error occurred while making this request to Lavalink.
        NodeException
            An error occured while making this request to Lavalink,
            and Lavalink was unable to send any error information.


        .. versionadded:: 3.1.0
        """
        data: list[PlayerResponse] = await self._fetch_players()

        payload: list[PlayerResponsePayload] = [PlayerResponsePayload(p) for p in data]
        return payload

    async def _fetch_player(self, guild_id: int, /) -> PlayerResponse:
        uri: str = f"{self.uri}/v4/sessions/{self.session_id}/players/{guild_id}"

        async with self._session.get(url=uri, headers=self.headers) as resp:
            if resp.status == 200:
                resp_data: PlayerResponse = await resp.json()
                return resp_data

            else:
                try:
                    exc_data: ErrorResponse = await resp.json()
                except Exception as e:
                    logger.warning("An error occured making a request on %r: %s", self, e)
                    raise NodeException(status=resp.status)

                raise LavalinkException(data=exc_data)

    async def fetch_player_info(self, guild_id: int, /) -> PlayerResponsePayload | None:
        """Method to fetch the player information Lavalink holds for the specific guild.

        .. warning::

            This payload is not the same as the :class:`wavelink.Player` class. This is the data received from
            Lavalink about the player. See: :meth:`~wavelink.Node.get_player`


        Parameters
        ----------
        guild_id: int
            The ID of the guild you want to fetch info for.

        Returns
        -------
        :class:`PlayerResponsePayload` | None
            The :class:`PlayerResponsePayload` representing the player info for the guild ID connected to this node.
            Could be ``None`` if no player is found with the given guild ID.

        Raises
        ------
        LavalinkException
            An error occurred while making this request to Lavalink.
        NodeException
            An error occured while making this request to Lavalink,
            and Lavalink was unable to send any error information.


        .. versionadded:: 3.1.0
        """
        try:
            data: PlayerResponse = await self._fetch_player(guild_id)
        except LavalinkException as e:
            if e.status == 404:
                return None

            raise e

        payload: PlayerResponsePayload = PlayerResponsePayload(data)
        return payload

    async def _update_player(self, guild_id: int, /, *, data: Request, replace: bool = False) -> PlayerResponse:
        no_replace: bool = not replace

        uri: str = f"{self.uri}/v4/sessions/{self.session_id}/players/{guild_id}?noReplace={no_replace}"

        async with self._session.patch(url=uri, json=data, headers=self.headers) as resp:
            if resp.status == 200:
                resp_data: PlayerResponse = await resp.json()
                return resp_data

            else:
                try:
                    exc_data: ErrorResponse = await resp.json()
                except Exception as e:
                    logger.warning("An error occured making a request on %r: %s", self, e)
                    raise NodeException(status=resp.status)

                raise LavalinkException(data=exc_data)

    async def _destroy_player(self, guild_id: int, /) -> None:
        uri: str = f"{self.uri}/v4/sessions/{self.session_id}/players/{guild_id}"

        async with self._session.delete(url=uri, headers=self.headers) as resp:
            if resp.status == 204:
                return

            try:
                exc_data: ErrorResponse = await resp.json()
            except Exception as e:
                logger.warning("An error occured making a request on %r: %s", self, e)
                raise NodeException(status=resp.status)

            raise LavalinkException(data=exc_data)

    async def _update_session(self, *, data: UpdateSessionRequest) -> UpdateResponse:
        uri: str = f"{self.uri}/v4/sessions/{self.session_id}"

        async with self._session.patch(url=uri, json=data, headers=self.headers) as resp:
            if resp.status == 200:
                resp_data: UpdateResponse = await resp.json()
                return resp_data

            else:
                try:
                    exc_data: ErrorResponse = await resp.json()
                except Exception as e:
                    logger.warning("An error occured making a request on %r: %s", self, e)
                    raise NodeException(status=resp.status)

                raise LavalinkException(data=exc_data)

    async def _fetch_tracks(self, query: str) -> LoadedResponse:
        uri: str = f"{self.uri}/v4/loadtracks?identifier={query}"

        async with self._session.get(url=uri, headers=self.headers) as resp:
            if resp.status == 200:
                resp_data: LoadedResponse = await resp.json()
                return resp_data

            else:
                try:
                    exc_data: ErrorResponse = await resp.json()
                except Exception as e:
                    logger.warning("An error occured making a request on %r: %s", self, e)
                    raise NodeException(status=resp.status)

                raise LavalinkException(data=exc_data)

    async def _decode_track(self) -> TrackPayload: ...

    async def _decode_tracks(self) -> list[TrackPayload]: ...

    async def _fetch_info(self) -> InfoResponse:
        uri: str = f"{self.uri}/v4/info"

        async with self._session.get(url=uri, headers=self.headers) as resp:
            if resp.status == 200:
                resp_data: InfoResponse = await resp.json()
                return resp_data

            else:
                try:
                    exc_data: ErrorResponse = await resp.json()
                except Exception as e:
                    logger.warning("An error occured making a request on %r: %s", self, e)
                    raise NodeException(status=resp.status)

                raise LavalinkException(data=exc_data)

    async def fetch_info(self) -> InfoResponsePayload:
        """Method to fetch this Lavalink Nodes info response data.

        Returns
        -------
        :class:`InfoResponsePayload`
            The :class:`InfoResponsePayload` associated with this Node.

        Raises
        ------
        LavalinkException
            An error occurred while making this request to Lavalink.
        NodeException
            An error occured while making this request to Lavalink,
            and Lavalink was unable to send any error information.


        .. versionadded:: 3.1.0
        """
        data: InfoResponse = await self._fetch_info()

        payload: InfoResponsePayload = InfoResponsePayload(data)
        return payload

    async def _fetch_stats(self) -> StatsResponse:
        uri: str = f"{self.uri}/v4/stats"

        async with self._session.get(url=uri, headers=self.headers) as resp:
            if resp.status == 200:
                resp_data: StatsResponse = await resp.json()
                return resp_data

            else:
                try:
                    exc_data: ErrorResponse = await resp.json()
                except Exception as e:
                    logger.warning("An error occured making a request on %r: %s", self, e)
                    raise NodeException(status=resp.status)

                raise LavalinkException(data=exc_data)

    async def fetch_stats(self) -> StatsResponsePayload:
        """Method to fetch this Lavalink Nodes stats response data.

        Returns
        -------
        :class:`StatsResponsePayload`
            The :class:`StatsResponsePayload` associated with this Node.

        Raises
        ------
        LavalinkException
            An error occurred while making this request to Lavalink.
        NodeException
            An error occured while making this request to Lavalink,
            and Lavalink was unable to send any error information.


        .. versionadded:: 3.1.0
        """
        data: StatsResponse = await self._fetch_stats()

        payload: StatsResponsePayload = StatsResponsePayload(data)
        return payload

    async def _fetch_version(self) -> str:
        uri: str = f"{self.uri}/version"

        async with self._session.get(url=uri, headers=self.headers) as resp:
            if resp.status == 200:
                return await resp.text()

            try:
                exc_data: ErrorResponse = await resp.json()
            except Exception as e:
                logger.warning("An error occured making a request on %r: %s", self, e)
                raise NodeException(status=resp.status)

            raise LavalinkException(data=exc_data)

    async def fetch_version(self) -> str:
        """Method to fetch this Lavalink version string.

        Returns
        -------
        str
            The version string associated with this Lavalink node.

        Raises
        ------
        LavalinkException
            An error occurred while making this request to Lavalink.
        NodeException
            An error occured while making this request to Lavalink,
            and Lavalink was unable to send any error information.


        .. versionadded:: 3.1.0
        """
        data: str = await self._fetch_version()
        return data

    def get_player(self, guild_id: int, /) -> Player | None:
        """Return a :class:`~wavelink.Player` associated with the provided :attr:`discord.Guild.id`.

        Parameters
        ----------
        guild_id: int
            The :attr:`discord.Guild.id` to retrieve a :class:`~wavelink.Player` for.

        Returns
        -------
        Optional[:class:`~wavelink.Player`]
            The Player associated with this guild ID. Could be None if no :class:`~wavelink.Player` exists
            for this guild.
        """
        return self._players.get(guild_id, None)


class Pool:
    """The wavelink Pool represents a collection of :class:`~wavelink.Node` and helper methods for searching tracks.

    To connect a :class:`~wavelink.Node` please use this Pool.

    .. note::

        All methods and attributes on this class are class level, not instance. Do not create an instance of this class.
    """

    __nodes: ClassVar[dict[str, Node]] = {}
    __cache: LFUCache | None = None

    @staticmethod
    def _is_retained_pre_disposal_failure(node: Node, exc: BaseException) -> bool:
        state = node._close_state
        if state is None or not state.task.done():
            return False

        try:
            outcome = state.task.result()
        except BaseException:
            return False

        return outcome.first_error is exc and not outcome.primary_socket_disposed

    @staticmethod
    def _consume_retired_candidate_disposal_failure(node: Node, exc: BaseException) -> bool:
        retained = node._retired_candidate_disposal_error
        if retained is not exc:
            return False
        node._retired_candidate_disposal_error = None
        return True

    @classmethod
    async def connect(
        cls, *, nodes: Iterable[Node], client: discord.Client | None = None, cache_capacity: int | None = None
    ) -> dict[str, Node]:
        """Connect the provided Iterable[:class:`Node`] to Lavalink.

        Parameters
        ----------
        nodes: Iterable[:class:`Node`]
            The :class:`Node`'s to connect to Lavalink.
        client: :class:`discord.Client` | None
            The :class:`discord.Client` to use to connect the :class:`Node`. If the Node already has a client
            set, this method will **not** override it. Defaults to None.
        cache_capacity: int | None
            An optional integer of the amount of track searches to cache. This is an experimental mode.
            Passing ``None`` will disable this experiment. Defaults to ``None``.

        Returns
        -------
        dict[str, :class:`Node`]
            A mapping of :attr:`Node.identifier` to :class:`Node` associated with the :class:`Pool`.


        Raises
        ------
        AuthorizationFailedException
            The node password was incorrect.
        InvalidClientException
            The :class:`discord.Client` passed was not valid.
        NodeException
            The node failed to connect properly. Please check that your Lavalink version is version 4.


        .. versionchanged:: 3.0.0

            The ``client`` parameter is no longer required.
            Added the ``cache_capacity`` parameter.
        """
        for node in nodes:
            client_ = node.client or client

            if node.identifier in cls.__nodes:
                msg: str = f'Unable to connect {node!r} as you already have a node with identifier "{node.identifier}"'
                logger.error(msg)

                continue

            if node.status in (NodeStatus.CONNECTING, NodeStatus.CONNECTED):
                logger.error("Unable to connect %r as it is already in a connecting or connected state.", node)
                continue

            generation = await node._reserve_connect_generation()
            if await node._is_connect_generation_invalid(generation=generation):
                logger.error(
                    "Unable to connect %r as its lifecycle state is terminal for generation %s.", node, generation
                )
                continue

            try:
                await node._connect(client=client_, generation=generation)
            except InvalidClientException as e:
                logger.error(e)
            except AuthorizationFailedException as e:
                if cls._consume_retired_candidate_disposal_failure(node, e):
                    raise
                if cls._is_retained_pre_disposal_failure(node, e):
                    raise
                logger.error("Failed to authenticate %r on Lavalink with the provided password.", node)
            except NodeException as e:
                if cls._consume_retired_candidate_disposal_failure(node, e):
                    raise
                if cls._is_retained_pre_disposal_failure(node, e):
                    raise
                logger.error(
                    (
                        "Failed to connect to %r. Check that your Lavalink major version is"
                        " '4' and that you are trying to connect to Lavalink on the correct port."
                    ),
                    node,
                )
            else:
                existing = cls.__nodes.get(node.identifier)
                conflict = existing is not None and existing is not node
                if conflict or await node._is_connect_generation_invalid(generation=generation):
                    if conflict:
                        logger.error(
                            "Unable to register %r because another node instance with identifier %r is active.",
                            node,
                            node.identifier,
                        )
                    else:
                        logger.error("Unable to register %r due to obsolete generation %s.", node, generation)
                    if (websocket := node._websocket) is not None:
                        try:
                            await node._retire_obsolete_connect_candidate(websocket=websocket)
                        except BaseException as exc:
                            node._retired_candidate_disposal_error = exc
                            raise
                    continue

                cls.__nodes[node.identifier] = node

        if cache_capacity is not None and cls.nodes:
            if cache_capacity <= 0:
                logger.warning("LFU Request cache capacity must be > 0. Not enabling cache.")

            else:
                cls.__cache = LFUCache(capacity=cache_capacity)
                logger.info("Experimental request caching has been toggled ON. To disable run Pool.toggle_cache()")

        return cls.nodes

    @classmethod
    async def reconnect(cls) -> dict[str, Node]:
        for node in tuple(cls.__nodes.values()):
            if node.status is not NodeStatus.DISCONNECTED:
                continue

            try:
                await node._connect(client=None, require_pool_membership=True)
            except InvalidClientException as e:
                logger.error(e)
            except AuthorizationFailedException as e:
                if cls._consume_retired_candidate_disposal_failure(node, e):
                    raise
                if cls._is_retained_pre_disposal_failure(node, e):
                    raise
                logger.error("Failed to authenticate %r on Lavalink with the provided password.", node)
            except NodeException as e:
                if cls._consume_retired_candidate_disposal_failure(node, e):
                    raise
                if cls._is_retained_pre_disposal_failure(node, e):
                    raise
                logger.error(
                    (
                        "Failed to connect to %r. Check that your Lavalink major version "
                        "is '4' and that you are trying to connect to Lavalink on the correct port."
                    ),
                    node,
                )

        return cls.nodes

    @classmethod
    async def close(cls) -> None:
        """Close and clean up all :class:`~wavelink.Node` on this Pool.

        This calls :meth:`wavelink.Node.close` on each node.


        .. versionadded:: 3.0.0
        """
        nodes_snapshot = tuple(cls.__nodes.values())
        first_error: Exception | None = None

        for node in nodes_snapshot:
            try:
                await node.close()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if first_error is None:
                    first_error = exc
                else:
                    Node._append_error_chain(first_error, exc)

        if first_error is not None:
            raise first_error

    @classproperty
    def nodes(cls) -> dict[str, Node]:
        """A mapping of :attr:`Node.identifier` to :class:`Node` that have previously been successfully connected.


        .. versionchanged:: 3.0.0

            This property now returns a copy.
        """
        nodes = cls.__nodes.copy()
        return nodes

    @classmethod
    def get_node(cls, identifier: str | None = None, /) -> Node:
        """Retrieve a :class:`Node` from the :class:`Pool` with the given identifier.

        If no identifier is provided, this method returns the ``best`` node.

        Parameters
        ----------
        identifier: str | None
            An optional identifier to retrieve a :class:`Node`.

        Raises
        ------
        InvalidNodeException
            Raised when a Node can not be found, or no :class:`Node` exists on the :class:`Pool`.


        .. versionchanged:: 3.0.0

            The ``id`` parameter was changed to ``identifier`` and is positional only.
        """
        if identifier:
            if identifier not in cls.__nodes:
                raise InvalidNodeException(f'A Node with the identifier "{identifier}" does not exist.')

            return cls.__nodes[identifier]

        nodes: list[Node] = [n for n in cls.__nodes.values() if n.status is NodeStatus.CONNECTED]
        if not nodes:
            raise InvalidNodeException("No nodes are currently assigned to the wavelink.Pool in a CONNECTED state.")

        return sorted(nodes, key=lambda n: n._total_player_count or len(n.players))[0]

    @classmethod
    async def fetch_tracks(cls, query: str, /, *, node: Node | None = None) -> list[Playable] | Playlist:
        """Search for a list of :class:`~wavelink.Playable` or a :class:`~wavelink.Playlist`, with the given query.

        Parameters
        ----------
        query: str
            The query to search tracks for. If this is not a URL based search you should provide the appropriate search
            prefix, e.g. "ytsearch:Rick Roll"
        node: :class:`~wavelink.Node` | None
            An optional :class:`~wavelink.Node` to use when fetching tracks. Defaults to ``None``, which selects the
            most appropriate :class:`~wavelink.Node` automatically.

        Returns
        -------
        list[Playable] | Playlist
            A list of :class:`~wavelink.Playable` or a :class:`~wavelink.Playlist`
            based on your search ``query``. Could be an empty list, if no tracks were found.

        Raises
        ------
        LavalinkLoadException
            Exception raised when Lavalink fails to load results based on your query.


        .. versionchanged:: 3.0.0

            This method was previously known as both ``.get_tracks`` and ``.get_playlist``. This method now searches
            for both :class:`~wavelink.Playable` and :class:`~wavelink.Playlist` and returns the appropriate type,
            or an empty list if no results were found.

            This method no longer accepts the ``cls`` parameter.


        .. versionadded:: 3.4.0

            Added the ``node`` Keyword-Only argument.
        """

        # TODO: Documentation Extension for `.. positional-only::` marker.
        encoded_query: str = urllib.parse.quote(query)

        if cls.__cache is not None:
            potential: list[Playable] | Playlist = cls.__cache.get(encoded_query, None)

            if potential:
                return potential

        node_: Node = node or cls.get_node()
        resp: LoadedResponse = await node_._fetch_tracks(encoded_query)

        if resp["loadType"] == "track":
            track = Playable(data=resp["data"])

            if cls.__cache is not None and not track.is_stream:
                cls.__cache.put(encoded_query, [track])

            return [track]

        elif resp["loadType"] == "search":
            tracks = [Playable(data=tdata) for tdata in resp["data"]]

            if cls.__cache is not None:
                cls.__cache.put(encoded_query, tracks)

            return tracks

        if resp["loadType"] == "playlist":
            playlist: Playlist = Playlist(data=resp["data"])

            if cls.__cache is not None:
                cls.__cache.put(encoded_query, playlist)

            return playlist

        elif resp["loadType"] == "empty":
            return []

        elif resp["loadType"] == "error":
            raise LavalinkLoadException(data=resp["data"])

        else:
            return []

    @classmethod
    def cache(cls, capacity: int | bool | None = None) -> None:
        if capacity in (None, False) or capacity <= 0:
            cls.__cache = None
            return

        if not isinstance(capacity, int):  # type: ignore
            raise ValueError("The LFU cache expects an integer, None or bool.")

        cls.__cache = LFUCache(capacity=capacity)

    @classmethod
    def has_cache(cls) -> bool:
        return cls.__cache is not None
