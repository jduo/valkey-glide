# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

import asyncio
import os
import struct
import sys
import threading
from typing import (
    Dict,
    List,
    Optional,
    Tuple,
    Union,
    cast,
)

from glide_shared._fast_response import parse_response as _c_parse_response
from glide_shared._glide_ffi import _GlideFFI
from glide_shared.cluster_scan_cursor import ClusterScanCursor
from glide_shared.commands.command_args import ObjectType
from glide_shared.commands.core_options import PubSubMsg
from glide_shared.config import (
    BaseClientConfiguration,
    GlideClientConfiguration,
    GlideClusterClientConfiguration,
    ServerCredentials,
)
from glide_shared.constants import (
    OK,
    TEncodable,
    TResult,
)
from glide_shared.exceptions import (
    ClosingError,
    ConfigurationError,
    RequestError,
    get_request_error_class,
)
from glide_shared.ffi_helpers import (
    ENCODING,
    FFIClientTypeEnum,
    convert_commands_to_c_batch_info,
    create_c_batch_options,
    parse_push_notification,
    to_c_route_ptr_and_len,
    to_c_strings,
)
from glide_shared.routes import Route

from .async_commands.cluster_commands import ClusterCommands
from .async_commands.core import CoreCommands, RequestType
from .async_commands.standalone_commands import StandaloneCommands
from .logger import Level as LogLevel
from .logger import Logger as ClientLogger
from .opentelemetry import OpenTelemetry

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


_shared_pipe_read_fd: int = -1
_shared_pipe_registered: bool = False
_client_registry: dict = {}
_pipe_remainder: bytes = b""


def _free_orphaned_frame(response_ptr, arena_or_err):
    """Free resources from a pipe frame whose client has been closed."""
    any_c = next(iter(_client_registry.values()), None)
    if any_c is None:
        return
    try:
        if response_ptr != 0 and arena_or_err != 0:
            any_c._lib.free_response_arena(any_c._ffi.cast("void*", arena_or_err))
        elif response_ptr == 0 and arena_or_err != 0:
            err_ptr = arena_or_err & 0x00FFFFFFFFFFFFFF
            if err_ptr:
                any_c._lib.free_pipe_error_string(any_c._ffi.cast("char*", err_ptr))
    except Exception:
        pass


def _handle_pipe_success(client, request_id, response_ptr, arena_or_err):
    """Handle a success frame from the shared pipe."""
    try:
        result, _ = _c_parse_response(response_ptr)
    except Exception as e:
        result = e
    finally:
        if arena_or_err:
            client._lib.free_response_arena(client._ffi.cast("void*", arena_or_err))
    fut = client._pending_futures.pop(request_id, None)
    if fut is not None and not fut.done():
        if isinstance(result, Exception):
            fut.set_exception(result)
        else:
            fut.set_result(result)


def _handle_pipe_error(client, request_id, arena_or_err):
    """Handle an error frame from the shared pipe."""
    error_type = (arena_or_err >> 56) & 0xFF
    err_ptr = arena_or_err & 0x00FFFFFFFFFFFFFF
    msg = "Unknown error"
    if err_ptr:
        try:
            msg = client._ffi.string(client._ffi.cast("char*", err_ptr)).decode("utf-8")
        except Exception:
            pass
        finally:
            client._lib.free_pipe_error_string(client._ffi.cast("char*", err_ptr))
    exc = get_request_error_class(error_type)(msg)
    fut = client._pending_futures.pop(request_id, None)
    if fut is not None and not fut.done():
        fut.set_exception(exc)


def _on_shared_pipe_readable() -> None:
    global _pipe_remainder
    try:
        data = os.read(_shared_pipe_read_fd, 32 * 512)
    except (BlockingIOError, OSError):
        return
    if not data:
        return
    if _pipe_remainder:
        data = _pipe_remainder + data
        _pipe_remainder = b""
    offset = 0
    while offset + 32 <= len(data):
        client_id, request_id, response_ptr, arena_or_err = struct.unpack_from(
            "=QQQQ", data, offset
        )
        offset += 32
        client = _client_registry.get(client_id)
        if client is None:
            _free_orphaned_frame(response_ptr, arena_or_err)
            continue
        if response_ptr != 0:
            _handle_pipe_success(client, request_id, response_ptr, arena_or_err)
        else:
            _handle_pipe_error(client, request_id, arena_or_err)
    if offset < len(data):
        _pipe_remainder = data[offset:]


class BaseClient(CoreCommands):
    def __init__(self, config: BaseClientConfiguration):
        """To create a new client, use the `create` classmethod"""
        _glide_ffi = _GlideFFI()
        self._ffi = _glide_ffi.ffi
        self._lib = _glide_ffi.lib
        self.config: BaseClientConfiguration = config
        self._is_closed: bool = False
        self._core_client = None
        self._loop: asyncio.AbstractEventLoop  # set in _create_client
        self._pending_futures: Dict[int, asyncio.Future] = {}
        self._callback_counter = 0
        self._lock = threading.Lock()
        self._success_callback_ref = None
        self._failure_callback_ref = None
        self._pubsub_callback_ref = None
        self._pubsub_futures: List[asyncio.Future] = []
        self._pubsub_lock = threading.Lock()
        self._pending_push_notifications: List[PubSubMsg] = []
        self._pipe_client_id: int = 0

    @classmethod
    async def create(cls, config: BaseClientConfiguration) -> Self:
        """Creates a Glide client.

        Args:
            config (ClientConfiguration): The configuration options for the client.

        Returns:
            Self: A promise that resolves to a connected client instance.
        """
        self = cls(config)
        self._loop = asyncio.get_running_loop()

        # Create CFFI callbacks
        @self._ffi.callback("SuccessCallback")
        def _success_cb(index_ptr, message):
            self._on_success(index_ptr, message)

        @self._ffi.callback("FailureCallback")
        def _failure_cb(index_ptr, error_message, error_type):
            self._on_failure(index_ptr, error_message, error_type)

        self._success_callback_ref = _success_cb
        self._failure_callback_ref = _failure_cb

        # Build connection request
        conn_req = config._create_a_protobuf_conn_request(
            cluster_mode=isinstance(config, GlideClusterClientConfiguration)
        )
        conn_req_bytes = conn_req.SerializeToString()

        # Create AsyncClient type
        client_type = self._ffi.new(
            "ClientType*",
            {
                "_type": self._ffi.cast("ClientTypeEnum", FFIClientTypeEnum.Async),
                "async_client": {
                    "success_callback": _success_cb,
                    "failure_callback": _failure_cb,
                    "allow_stack_response": True,
                },
            },
        )

        # Create pubsub callback
        python_callback = self._create_push_handle_callback()
        pubsub_callback = self._ffi.callback("PubSubCallback", python_callback)
        self._pubsub_callback_ref = pubsub_callback

        client_response_ptr = self._lib.create_client(
            conn_req_bytes,
            len(conn_req_bytes),
            client_type,
            pubsub_callback,
        )

        ClientLogger.log(LogLevel.INFO, "connection info", "new connection established")

        if client_response_ptr == self._ffi.NULL:
            raise ClosingError("Failed to create client, response pointer is NULL.")

        client_response = self._ffi.cast("ConnectionResponse*", client_response_ptr)
        if client_response.conn_ptr != self._ffi.NULL:
            self._core_client = client_response.conn_ptr
        else:
            error_msg = (
                self._ffi.string(client_response.connection_error_message).decode(
                    ENCODING
                )
                if client_response.connection_error_message != self._ffi.NULL
                else "Unknown error"
            )
            self._lib.free_connection_response(client_response_ptr)
            raise ClosingError(error_msg)

        self._lib.free_connection_response(client_response_ptr)

        global _shared_pipe_read_fd, _shared_pipe_registered
        self._pipe_client_id = id(self)
        if _shared_pipe_read_fd < 0:
            try:
                _shared_pipe_read_fd, pw = os.pipe()
                os.set_blocking(_shared_pipe_read_fd, False)
                self._lib.init_shared_pipe(pw)
            except OSError:
                _shared_pipe_read_fd = -1
                self._pipe_client_id = 0
        if _shared_pipe_read_fd >= 0 and self._pipe_client_id:
            self._lib.set_pipe_client_id(self._core_client, self._pipe_client_id)
            _client_registry[self._pipe_client_id] = self
            if not _shared_pipe_registered:
                self._loop.add_reader(_shared_pipe_read_fd, _on_shared_pipe_readable)
                _shared_pipe_registered = True

        return self

    # ==================== Callback Handling ====================

    def _get_callback_id(self) -> int:
        self._callback_counter += 1
        return self._callback_counter

    def _on_success(self, index_ptr: int, message) -> None:
        """Called from Rust thread on command success."""
        if message == self._ffi.NULL:
            result = None
        else:
            addr = int(self._ffi.cast("uintptr_t", message))
            arena_ptr = 0
            try:
                result, arena_ptr = _c_parse_response(addr)
            except Exception as e:
                result = e
            finally:
                if arena_ptr:
                    self._lib.free_response_arena(self._ffi.cast("void*", arena_ptr))

        fut = self._pending_futures.pop(index_ptr, None)
        if fut is not None and not fut.done():
            if isinstance(result, Exception):
                self._loop.call_soon_threadsafe(fut.set_exception, result)
            else:
                self._loop.call_soon_threadsafe(fut.set_result, result)

    def _on_failure(self, index_ptr: int, error_message, error_type: int) -> None:
        """Called from Rust thread on command failure."""
        try:
            msg = self._ffi.string(error_message).decode(ENCODING)
        except Exception:
            msg = "Unknown error"

        exc = get_request_error_class(error_type)(msg)  # type: ignore[arg-type]

        fut = self._pending_futures.pop(index_ptr, None)
        if fut is not None and not fut.done():
            self._loop.call_soon_threadsafe(fut.set_exception, exc)

    # ==================== PubSub ====================

    def _create_push_handle_callback(self):
        """Create the FFI pubsub callback function."""

        def _pubsub_callback(
            client_ptr,
            kind,
            message_ptr,
            message_len,
            channel_ptr,
            channel_len,
            pattern_ptr,
            pattern_len,
        ):
            try:
                message_kind, message, channel, pattern = parse_push_notification(
                    self._ffi,
                    kind,
                    message_ptr,
                    message_len,
                    channel_ptr,
                    channel_len,
                    pattern_ptr,
                    pattern_len,
                )

                if message_kind == "Disconnection":
                    ClientLogger.log(
                        LogLevel.WARN,
                        "disconnect notification",
                        "Transport disconnected, messages might be lost",
                    )
                elif message_kind in ("Message", "PMessage", "SMessage"):
                    pubsub_msg = PubSubMsg(
                        message=message, channel=channel, pattern=pattern
                    )
                    with self._pubsub_lock:
                        user_callback, context = (
                            self.config._get_pubsub_callback_and_context()
                        )
                        if user_callback:
                            user_callback(pubsub_msg, context)
                        else:
                            self._pending_push_notifications.append(pubsub_msg)
                            self._complete_pubsub_futures_safe()
            except Exception as e:
                ClientLogger.log(
                    LogLevel.ERROR,
                    "pubsub_callback",
                    f"Error in pubsub callback: {e}",
                )

        return _pubsub_callback

    def _complete_pubsub_futures_safe(self):
        """Complete pending pubsub futures with available messages. Must hold _pubsub_lock."""
        loop = self._loop
        while self._pending_push_notifications and self._pubsub_futures:
            msg = self._pending_push_notifications.pop(0)
            fut = self._pubsub_futures.pop(0)
            if not fut.done() and loop and not loop.is_closed():
                loop.call_soon_threadsafe(fut.set_result, msg)

    async def get_pubsub_message(self) -> PubSubMsg:
        if self._is_closed:
            raise ClosingError("Client is closed.")
        if self.config._get_pubsub_callback_and_context()[0] is not None:
            raise ConfigurationError(
                "The operation will never complete since messages will be passed to the configured callback."
            )
        fut: asyncio.Future = self._loop.create_future()
        with self._pubsub_lock:
            self._pubsub_futures.append(fut)
            self._complete_pubsub_futures_safe()
        return await fut

    def try_get_pubsub_message(self) -> Optional[PubSubMsg]:
        if self._is_closed:
            raise ClosingError("Client is closed.")
        if self.config._get_pubsub_callback_and_context()[0] is not None:
            raise ConfigurationError(
                "The operation will never complete since messages will be passed to the configured callback."
            )
        with self._pubsub_lock:
            if self._pending_push_notifications:
                return self._pending_push_notifications.pop(0)
            return None

    # ==================== Response Parsing ====================

    def _handle_response(self, message):
        """Parse a CommandResponse pointer into a Python object.

        For the async client, NULL means no response (returns None) and the arena
        is freed here since responses arrive via the pipe path without automatic
        cleanup. The sync client's _handle_response raises on NULL and relies on
        free_command_result to free the arena.
        """
        if message == self._ffi.NULL:
            return None
        addr = int(self._ffi.cast("uintptr_t", message))
        arena_ptr = 0
        try:
            result, arena_ptr = _c_parse_response(addr)
            return result
        finally:
            if arena_ptr:
                self._lib.free_response_arena(self._ffi.cast("void*", arena_ptr))

    # ==================== FFI Helpers ====================

    def _to_c_strings(self, args):
        return to_c_strings(self._ffi, args)

    def _to_c_route_ptr_and_len(self, route):
        return to_c_route_ptr_and_len(self._ffi, route)

    # ==================== Command Execution ====================

    async def _execute_command(
        self,
        request_type: int,
        args: List[TEncodable],
        route: Optional[Route] = None,
    ) -> TResult:
        if self._is_closed:
            raise ClosingError(
                "Unable to execute requests; the client is closed. Please create a new client."
            )

        callback_id = self._get_callback_id()
        fut = self._loop.create_future()

        self._pending_futures[callback_id] = fut

        c_args, c_lengths, buffers = self._to_c_strings(args)

        # OTel span creation only when initialized (rare)
        span = 0
        if OpenTelemetry._instance is not None and OpenTelemetry.should_sample():
            span_name_cstr = self._ffi.new(
                "char[]", RequestType.Name(request_type).encode()
            )
            span = self._lib.create_named_otel_span(span_name_cstr)

        if route is None:
            self._lib.command(
                self._core_client,
                callback_id,
                request_type,
                len(args),
                c_args,
                c_lengths,
                self._ffi.NULL,
                0,
                span,
            )
        else:
            route_ptr, route_len, route_bytes = self._to_c_route_ptr_and_len(route)
            self._lib.command(
                self._core_client,
                callback_id,
                request_type,
                len(args),
                c_args,
                c_lengths,
                route_ptr,
                route_len,
                span,
            )

        try:
            return await fut
        finally:
            if span:
                self._lib.drop_otel_span(span)

    async def _execute_batch(
        self,
        commands: List[Tuple[int, List[TEncodable]]],
        is_atomic: bool,
        raise_on_error: bool = False,
        retry_server_error: bool = False,
        retry_connection_error: bool = False,
        route: Optional[Route] = None,
        timeout: Optional[int] = None,
    ) -> List[TResult]:
        if self._is_closed:
            raise ClosingError(
                "Unable to execute requests; the client is closed. Please create a new client."
            )

        callback_id = self._get_callback_id()
        fut = self._loop.create_future()

        self._pending_futures[callback_id] = fut

        span = 0
        if OpenTelemetry.should_sample():
            span = self._lib.create_batch_otel_span()

        batch_info, batch_refs = convert_commands_to_c_batch_info(
            self._ffi, commands, is_atomic
        )
        batch_options, opts_refs = create_c_batch_options(
            self._ffi,
            route,
            retry_server_error=retry_server_error,
            retry_connection_error=retry_connection_error,
            timeout=timeout,
        )
        _refs = batch_refs + opts_refs  # noqa: F841  prevent GC

        self._lib.batch(
            self._core_client,
            callback_id,
            batch_info,
            raise_on_error,
            batch_options,
            span,
        )

        try:
            return await fut
        finally:
            if span != 0:
                self._lib.drop_otel_span(span)

    async def _execute_script(
        self,
        hash: str,
        keys: Optional[List[TEncodable]] = None,
        args: Optional[List[TEncodable]] = None,
        route: Optional[Route] = None,
    ) -> TResult:
        if self._is_closed:
            raise ClosingError(
                "Unable to execute requests; the client is closed. Please create a new client."
            )

        callback_id = self._get_callback_id()
        fut = self._loop.create_future()

        self._pending_futures[callback_id] = fut

        if keys is None:
            keys = []
        if args is None:
            args = []

        keys_c_args, keys_c_lengths, keys_buffers = self._to_c_strings(keys)
        args_c_args, args_c_lengths, args_buffers = self._to_c_strings(args)

        hash_bytes = hash.encode(ENCODING) + b"\0"
        hash_buffer = self._ffi.from_buffer(hash_bytes)

        route_ptr, route_len, route_bytes = self._to_c_route_ptr_and_len(route)

        self._lib.invoke_script(
            self._core_client,
            callback_id,
            hash_buffer,
            len(keys),
            keys_c_args,
            keys_c_lengths,
            len(args),
            args_c_args,
            args_c_lengths,
            route_ptr,
            route_len,
            0,
        )

        return await fut

    # ==================== Connection Management ====================

    async def _update_connection_password(
        self, password: Optional[str], immediate_auth: bool
    ) -> TResult:
        if self._is_closed:
            raise ClosingError("Client is closed.")

        callback_id = self._get_callback_id()
        fut = self._loop.create_future()

        self._pending_futures[callback_id] = fut

        c_password = (
            self._ffi.new("char[]", password.encode(ENCODING))
            if password is not None
            else self._ffi.new("char[]", b"")
        )

        self._lib.update_connection_password(
            self._core_client,
            callback_id,
            c_password,
            immediate_auth,
        )

        result = await fut
        if result is OK:
            if self.config.credentials is None:
                self.config.credentials = ServerCredentials(password=password or "")
            self.config.credentials.password = password or ""
        return result

    async def _refresh_iam_token(self) -> TResult:
        if self._is_closed:
            raise ClosingError("Client is closed.")

        callback_id = self._get_callback_id()
        fut = self._loop.create_future()

        self._pending_futures[callback_id] = fut

        self._lib.refresh_iam_token(
            self._core_client,
            callback_id,
        )

        return await fut

    async def get_statistics(self) -> dict:
        stats = self._lib.get_statistics()
        return {
            "total_connections": stats.total_connections,
            "total_clients": stats.total_clients,
            "total_values_compressed": stats.total_values_compressed,
            "total_values_decompressed": stats.total_values_decompressed,
            "total_original_bytes": stats.total_original_bytes,
            "total_bytes_compressed": stats.total_bytes_compressed,
            "total_bytes_decompressed": stats.total_bytes_decompressed,
            "compression_skipped_count": stats.compression_skipped_count,
            "subscription_out_of_sync_count": stats.subscription_out_of_sync_count,
            "subscription_last_sync_timestamp": stats.subscription_last_sync_timestamp,
        }

    def _parse_pubsub_state(self, result: TResult, is_cluster: bool):
        if not isinstance(result, list) or len(result) != 4:
            raise RequestError("Invalid response format from GetSubscriptions")

        desired_dict = result[1]
        actual_dict = result[3]

        if is_cluster:
            PubSubChannelModes = GlideClusterClientConfiguration.PubSubChannelModes
            StateClass = GlideClusterClientConfiguration.PubSubState
            mode_map = {
                "Exact": PubSubChannelModes.Exact,
                "Pattern": PubSubChannelModes.Pattern,
                "Sharded": PubSubChannelModes.Sharded,
            }
        else:
            PubSubChannelModes = GlideClientConfiguration.PubSubChannelModes  # type: ignore[assignment]
            StateClass = GlideClientConfiguration.PubSubState  # type: ignore[assignment]
            mode_map = {
                "Exact": PubSubChannelModes.Exact,
                "Pattern": PubSubChannelModes.Pattern,
            }

        desired_subscriptions = {}
        actual_subscriptions = {}

        for key_bytes, value_list in desired_dict.items():  # type: ignore[union-attr]
            key = key_bytes.decode() if isinstance(key_bytes, bytes) else key_bytes
            if key in mode_map:
                values = {v.decode() if isinstance(v, bytes) else v for v in value_list}
                desired_subscriptions[mode_map[key]] = values

        for key_bytes, value_list in actual_dict.items():  # type: ignore[union-attr]
            key = key_bytes.decode() if isinstance(key_bytes, bytes) else key_bytes
            if key in mode_map:
                values = {v.decode() if isinstance(v, bytes) else v for v in value_list}
                actual_subscriptions[mode_map[key]] = values

        return StateClass(
            desired_subscriptions=desired_subscriptions,
            actual_subscriptions=actual_subscriptions,
        )

    async def close(self, err_message: Optional[str] = None) -> None:
        if not self._is_closed:
            self._is_closed = True
            err_message = "" if err_message is None else err_message

            with self._lock:
                for fut in self._pending_futures.values():
                    if not fut.done():
                        fut.set_exception(ClosingError(err_message))
                self._pending_futures.clear()

            with self._pubsub_lock:
                for fut in self._pubsub_futures:
                    if not fut.done():
                        fut.set_exception(ClosingError(err_message))
                self._pubsub_futures.clear()

            _client_registry.pop(getattr(self, "_pipe_client_id", 0), None)

            if self._core_client is not None:
                self._lib.close_client(self._core_client)
                self._core_client = None


class GlideClusterClient(BaseClient, ClusterCommands):
    """
    Client used for connection to cluster servers.
    Use :func:`~BaseClient.create` to request a client.
    For full documentation, see
    [Valkey GLIDE Documentation](https://glide.valkey.io/how-to/client-initialization/#cluster)
    """

    async def _cluster_scan(
        self,
        cursor: ClusterScanCursor,
        match: Optional[TEncodable] = None,
        count: Optional[int] = None,
        type: Optional[ObjectType] = None,
        allow_non_covered_slots: bool = False,
    ) -> List[Union[ClusterScanCursor, List[bytes]]]:
        if self._is_closed:
            raise ClosingError(
                "Unable to execute requests; the client is closed. Please create a new client."
            )

        callback_id = self._get_callback_id()
        fut = self._loop.create_future()

        self._pending_futures[callback_id] = fut

        # Build scan args
        args = []
        if match is not None:
            encoded_match = match.encode(ENCODING) if isinstance(match, str) else match
            args.extend([b"MATCH", encoded_match])
        if count is not None:
            args.extend([b"COUNT", str(count).encode(ENCODING)])
        if type is not None:
            args.extend([b"TYPE", type.value.encode(ENCODING)])
        if allow_non_covered_slots:
            args.extend([b"ALLOW_NON_COVERED_SLOTS"])

        cursor_string = cursor.get_cursor()
        cursor_bytes = cursor_string.encode(ENCODING) + b"\0"
        cursor_buffer = self._ffi.from_buffer(cursor_bytes)

        if args:
            args_array, args_len_array, arg_buffers = self._to_c_strings(args)
            arg_count = len(args)
        else:
            args_array = self._ffi.NULL
            args_len_array = self._ffi.NULL
            arg_count = 0

        self._lib.request_cluster_scan(
            self._core_client,
            callback_id,
            cursor_buffer,
            arg_count,
            args_array,
            args_len_array,
        )

        response_data = await fut

        if not isinstance(response_data, list) or len(response_data) != 2:
            raise RequestError("Unexpected cluster scan response format")

        new_cursor = response_data[0]
        if isinstance(new_cursor, bytes):
            new_cursor = new_cursor.decode(ENCODING)

        keys_list = response_data[1] if response_data[1] is not None else []
        return [ClusterScanCursor(new_cursor), keys_list]

    async def get_subscriptions(
        self,
    ) -> GlideClusterClientConfiguration.PubSubState:
        result = await self._execute_command(RequestType.GetSubscriptions, [])
        return cast(
            GlideClusterClientConfiguration.PubSubState,
            self._parse_pubsub_state(result, is_cluster=True),
        )


class GlideClient(BaseClient, StandaloneCommands):
    """
    Client used for connection to standalone servers.
    Use :func:`~BaseClient.create` to request a client.
    For full documentation, see
    [Valkey GLIDE Documentation](https://glide.valkey.io/how-to/client-initialization/#standalone)
    """

    async def get_subscriptions(
        self,
    ) -> GlideClientConfiguration.PubSubState:
        result = await self._execute_command(RequestType.GetSubscriptions, [])
        return cast(
            GlideClientConfiguration.PubSubState,
            self._parse_pubsub_state(result, is_cluster=False),
        )


TGlideClient = Union[GlideClient, GlideClusterClient]
