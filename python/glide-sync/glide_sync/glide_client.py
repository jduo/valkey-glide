# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

import os
import sys
import threading
from typing import Any, List, Optional, Tuple, Union

from glide_shared._fast_response import parse_response as _fast_parse_response
from glide_shared._glide_ffi import _GlideFFI
from glide_shared.commands.command_args import ObjectType
from glide_shared.commands.core_options import PubSubMsg
from glide_shared.config import (
    BaseClientConfiguration,
    GlideClientConfiguration,
    GlideClusterClientConfiguration,
)
from glide_shared.constants import TEncodable, TResult
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
from glide_shared.request_type import RequestType
from glide_shared.routes import Route

from .logger import Level, Logger
from .opentelemetry import OpenTelemetry
from .sync_commands.cluster_commands import ClusterCommands
from .sync_commands.cluster_scan_cursor import ClusterScanCursor
from .sync_commands.core import CoreCommands
from .sync_commands.standalone_commands import StandaloneCommands

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class BaseClient(CoreCommands):

    def __init__(self, config: BaseClientConfiguration):
        """
        To create a new client, use the `create` classmethod
        """
        _glide_ffi = _GlideFFI()
        self._ffi = _glide_ffi.ffi
        self._lib = _glide_ffi.lib
        self._config: BaseClientConfiguration = config
        self._pubsub_queue: List[PubSubMsg] = []
        self._pubsub_lock = threading.Lock()
        self._pubsub_condition = threading.Condition(self._pubsub_lock)
        self._pubsub_callback_ref = None  # Keep callback alive

        self._is_closed: bool = False

    @classmethod
    def create(cls, config: BaseClientConfiguration) -> Self:
        if not isinstance(
            config, (GlideClientConfiguration, GlideClusterClientConfiguration)
        ):
            raise ConfigurationError(
                "Configuration must be an instance of the sync version of "
                "GlideClientConfiguration or GlideClusterClientConfiguration, "
                "imported from glide_sync.config."
            )
        self = cls(config)
        self._config = config
        self._is_closed = False

        os.register_at_fork(after_in_child=self._create_core_client)

        self._create_core_client()

        return self

    def _create_core_client(self):
        # This check is needed in case a fork happens after the client already closed
        # In that case the registered fork function will kick in even if the
        # client already closed, and recreate it anyway.
        if self._is_closed:
            return
        conn_req = self._config._create_a_protobuf_conn_request(
            cluster_mode=type(self._config) is GlideClusterClientConfiguration
        )
        conn_req_bytes = conn_req.SerializeToString()
        client_type = self._ffi.new(
            "ClientType*",
            {
                "_type": self._ffi.cast("ClientTypeEnum", FFIClientTypeEnum.Sync),
            },
        )

        # Always create pubsub callback to support dynamic subscriptions
        # This ensures messages are always handled by the wrapper, whether they originate
        # from configured subscriptions or from dynamic subscriptions added at runtime
        python_callback = self._create_push_handle_callback()
        pubsub_callback = self._ffi.callback("PubSubCallback", python_callback)
        # Store reference to prevent garbage collection
        self._pubsub_callback_ref = pubsub_callback

        client_response_ptr = self._lib.create_client(
            conn_req_bytes,
            len(conn_req_bytes),
            client_type,
            pubsub_callback,
        )

        Logger.log(Level.INFO, "connection info", "new connection established")

        # Handle the connection response
        if client_response_ptr != self._ffi.NULL:
            client_response = self._try_ffi_cast(
                "ConnectionResponse*", client_response_ptr
            )
            if client_response.conn_ptr != self._ffi.NULL:
                self._core_client = client_response.conn_ptr
            else:
                error_message = (
                    self._ffi.string(client_response.connection_error_message).decode(
                        ENCODING
                    )
                    if client_response.connection_error_message != self._ffi.NULL
                    else "Unknown error"
                )
                raise ClosingError(error_message)

            # Free the connection response to avoid memory leaks
            self._lib.free_connection_response(client_response_ptr)
        else:
            raise ClosingError("Failed to create client, response pointer is NULL.")

    def _create_push_handle_callback(self):
        """Create the FFI pubsub callback function"""

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
                    Logger.log(
                        Level.WARN,
                        "disconnect notification",
                        "Transport disconnected, messages might be lost",
                    )
                elif message_kind in ["Message", "PMessage", "SMessage"]:
                    pubsub_msg = PubSubMsg(
                        message=message, channel=channel, pattern=pattern
                    )

                    with self._pubsub_condition:
                        user_callback, context = (
                            self._config._get_pubsub_callback_and_context()
                        )
                        if user_callback:
                            user_callback(pubsub_msg, context)
                        else:
                            self._pubsub_queue.append(pubsub_msg)
                            self._pubsub_condition.notify()
                elif message_kind in [
                    "PSubscribe",
                    "Subscribe",
                    "SSubscribe",
                    "Unsubscribe",
                    "PUnsubscribe",
                    "SUnsubscribe",
                ]:
                    pass  # Ignore subscription confirmations
                else:
                    Logger.log(
                        Level.WARN,
                        "unknown notification",
                        f"Unknown notification message: '{message_kind}'",
                    )

            except Exception as e:
                Logger.log(
                    Level.ERROR, "pubsub_callback", f"Error in pubsub callback: {e}"
                )

        return _pubsub_callback

    def _handle_response(self, message):
        if message == self._ffi.NULL:
            raise RequestError("Received NULL message.")
        addr = int(self._ffi.cast("uintptr_t", message))
        result, _arena_ptr = _fast_parse_response(addr)
        # Arena is freed by free_command_result in _handle_cmd_result's finally block
        return result

    def _try_ffi_cast(self, type, source):
        try:
            return self._ffi.cast(type, source)
        except Exception as e:
            raise ClosingError(f"FFI casting failed: {e}")

    def _to_c_strings(self, args):
        return to_c_strings(self._ffi, args)

    # `route_bytes` must remain alive for the duration of the FFI call that consumes `route_ptr`
    def _to_c_route_ptr_and_len(self, route):
        return to_c_route_ptr_and_len(self._ffi, route)

    def _handle_cmd_result(self, command_result):
        try:
            if command_result == self._ffi.NULL:
                raise ClosingError("Internal error: Received NULL as a command result")
            if command_result.command_error != self._ffi.NULL:
                # Handle the error case
                error = self._try_ffi_cast(
                    "CommandError*", command_result.command_error
                )
                error_message = self._ffi.string(error.command_error_message).decode(
                    ENCODING
                )
                error_class = get_request_error_class(error.command_error_type)
                # Free the error message to avoid memory leaks
                raise error_class(error_message)
            else:
                return self._handle_response(command_result.response)
                # Free the error message to avoid memory leaks
        finally:
            self._lib.free_command_result(command_result)

    def _execute_command(
        self,
        request_type: int,
        args: List[TEncodable],
        route: Optional[Route] = None,
        response_buffer: Optional[memoryview] = None,
    ) -> TResult:
        if self._is_closed:
            raise ClosingError(
                "Unable to execute requests; the client is closed. Please create a new client."
            )
        client_adapter_ptr = self._core_client
        if client_adapter_ptr == self._ffi.NULL:
            raise ValueError("Invalid client pointer.")
        if response_buffer:
            if response_buffer.readonly:
                raise TypeError("response_buffer must be writable")
            if not response_buffer.c_contiguous:
                raise TypeError("response_buffer must be C-contiguous")

        # Create span if OpenTelemetry is configured and sampling indicates we should trace
        span = 0
        span_name_cstr = None
        if OpenTelemetry.should_sample():
            command_name = RequestType.Name(request_type)
            span_name_cstr = self._ffi.new("char[]", command_name.encode())
            span = self._lib.create_named_otel_span(span_name_cstr)

        try:
            # Convert the arguments to C-compatible pointers
            c_args, c_lengths, buffers = self._to_c_strings(args)

            # Route bytes should be kept alive in the scope of the FFI call
            route_ptr, route_len, route_bytes = self._to_c_route_ptr_and_len(route)

            buf_ptr = (
                self._ffi.from_buffer(response_buffer)
                if response_buffer
                else self._ffi.NULL
            )
            buf_len = len(response_buffer) if response_buffer else 0
            result = self._lib.command_with_buffer(
                client_adapter_ptr,
                0,
                request_type,
                len(args),
                c_args,
                c_lengths,
                route_ptr,
                route_len,
                buf_ptr,
                buf_len,
                span,
            )
        finally:
            # Drop span if it was created
            if span != 0:
                self._lib.drop_otel_span(span)
        return self._handle_cmd_result(result)

    def _update_connection_password(
        self,
        password: Optional[str],
        immediate_auth: bool = False,
    ) -> TResult:
        """
        Update the current connection password with a new password.

        Note:
            This method updates the client's internal password configuration and does
            not perform password rotation on the server side.

        This method is useful in scenarios where the server password has changed or when
        utilizing short-lived passwords for enhanced security. It allows the client to
        update its password to reconnect upon disconnection without the need to recreate
        the client instance. This ensures that the internal reconnection mechanism can
        handle reconnection seamlessly, preventing the loss of in-flight commands.

        Args:
            password (`Optional[str]`): The new password to use for the connection,
                if `None` the password will be removed.
            immediate_auth (`bool`):
                `True`: The client will authenticate immediately with the new password against all connections, Using `AUTH`
                command. If password supplied is an empty string, auth will not be performed and warning will be returned.
                The default is `False`.

        Returns:
            TOK: A simple OK response. If `immediate_auth=True` returns OK if the reauthenticate succeed.

        Example:
            >>> client.update_connection_password("new_password", immediate_auth=True)
            'OK'
        """
        if self._is_closed:
            raise ClosingError("Client is closed.")
        client_adapter_ptr = self._core_client
        if client_adapter_ptr == self._ffi.NULL:
            raise ValueError("Invalid client pointer.")

        # Prepare C string for password
        c_password = (
            self._ffi.new("char[]", password.encode(ENCODING))
            if password is not None
            else self._ffi.new("char[]", b"")
        )

        result = self._lib.update_connection_password(
            client_adapter_ptr,
            0,  # Request ID (0 for sync use)
            c_password,
            immediate_auth,
        )
        return self._handle_cmd_result(result)

    def _execute_batch(
        self,
        commands: List[Tuple[int, List[TEncodable]]],
        is_atomic: bool,
        raise_on_error: bool,
        retry_server_error: bool = False,
        retry_connection_error: bool = False,
        route: Optional[Route] = None,
        timeout: Optional[int] = None,
    ) -> List[TResult]:
        """
        Execute a batch of commands synchronously using the FFI batch function.
        Accepts pre-extracted parameters from exec().
        """

        if self._is_closed:
            raise ClosingError(
                "Unable to execute requests; the client is closed. Please create a new client."
            )

        client_adapter_ptr = self._core_client
        if client_adapter_ptr == self._ffi.NULL:
            raise ValueError("Invalid client pointer.")

        # Create span if OpenTelemetry is configured and sampling indicates we should trace
        span = 0
        if OpenTelemetry.should_sample():
            span = self._lib.create_batch_otel_span()

        try:
            # Note: batch_refs and opts_refs must remain in scope
            # throughout this entire function call to prevent garbage collection of Python objects
            # that have C pointers pointing to them via ffi.from_buffer().

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

            result = self._lib.batch(
                client_adapter_ptr,
                0,  # callback_index (0 for sync)
                batch_info,
                raise_on_error,
                batch_options,
                span,  # span_ptr for tracing
            )
            return self._handle_cmd_result(result)
        finally:
            # Drop span if it was created
            if span != 0:
                self._lib.drop_otel_span(span)

    def _execute_script(
        self,
        script_hash: str,
        keys: Optional[List[TEncodable]] = None,
        args: Optional[List[TEncodable]] = None,
        route: Optional[Route] = None,
    ) -> TResult:

        if self._is_closed:
            raise ClosingError(
                "Unable to execute requests; the client is closed. Please create a new client."
            )

        client_adapter_ptr = self._core_client
        if client_adapter_ptr == self._ffi.NULL:
            raise ValueError("Invalid client pointer.")

        # Default to empty lists if None provided
        if keys is None:
            keys = []
        if args is None:
            args = []

        # Convert keys to C-compatible format
        keys_c_args, keys_c_lengths, keys_buffers = self._to_c_strings(keys)

        # Convert args to C-compatible format
        args_c_args, args_c_lengths, args_buffers = self._to_c_strings(args)

        # Convert script hash to C string
        hash_bytes = script_hash.encode(ENCODING) + b"\0"
        hash_buffer = self._ffi.from_buffer(hash_bytes)

        # Route bytes should be kept alive in the scope of the FFI call
        route_ptr, route_len, route_bytes = self._to_c_route_ptr_and_len(route)

        # Create span if OpenTelemetry is configured and sampling
        from .opentelemetry import OpenTelemetry

        span = 0
        span_name_cstr = None
        if OpenTelemetry.should_sample():
            span_name_cstr = self._ffi.new("char[]", b"EVALSHA")
            span = self._lib.create_named_otel_span(span_name_cstr)

        try:
            result = self._lib.invoke_script(
                client_adapter_ptr,
                0,  # Request ID - placeholder for sync clients
                hash_buffer,
                len(keys),
                keys_c_args,
                keys_c_lengths,
                len(args),
                args_c_args,
                args_c_lengths,
                route_ptr,
                route_len,
                span,
            )
            return self._handle_cmd_result(result)
        finally:
            if span != 0:
                self._lib.drop_otel_span(span)

    def try_get_pubsub_message(self) -> Optional[PubSubMsg]:
        """Try to get a pubsub message without blocking"""
        if self._is_closed:
            raise ClosingError(
                "Unable to execute requests; the client is closed. Please create a new client."
            )

        if self._config._get_pubsub_callback_and_context()[0] is not None:
            raise ConfigurationError(
                "The operation will never succeed since messages will be passed to the configured callback."
            )

        with self._pubsub_condition:
            if self._pubsub_queue:
                return self._pubsub_queue.pop(0)
            else:
                return None

    def get_pubsub_message(self) -> PubSubMsg:
        """Get a pubsub message, blocking until one is available"""
        if self._is_closed:
            raise ClosingError(
                "Unable to execute requests; the client is closed. Please create a new client."
            )

        if self._config._get_pubsub_callback_and_context()[0] is not None:
            raise ConfigurationError(
                "The operation will never complete since messages will be passed to the configured callback."
            )

        with self._pubsub_condition:
            while not self._pubsub_queue:
                if self._is_closed:
                    raise ClosingError("Client was closed while waiting for message")

                # Block indefinitely until notify() is called
                self._pubsub_condition.wait()

            return self._pubsub_queue.pop(0)

    def get_statistics(self) -> dict:
        """
        Get compression and connection statistics for this client.

        Returns:
            dict: A dictionary containing statistics with integer values:
                - total_connections: Total number of connections
                - total_clients: Total number of clients
                - total_values_compressed: Number of values successfully compressed
                - total_values_decompressed: Number of values successfully decompressed
                - total_original_bytes: Total bytes of original data before compression
                - total_bytes_compressed: Total bytes after compression
                - total_bytes_decompressed: Total bytes after decompression
                - compression_skipped_count: Number of times compression was skipped
                - subscription_out_of_sync_count: Failed reconciliation attempts
                - subscription_last_sync_timestamp: Last successful sync (milliseconds since epoch)
        """
        # Call the C FFI get_statistics function (returns by value, no manual free needed)
        stats = self._lib.get_statistics()

        # Access the struct fields and convert to a dictionary
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

    def get_subscriptions(self):
        """Get subscription state (desired vs actual)."""
        result = self._execute_command(RequestType.GetSubscriptions, [])
        return self._parse_pubsub_state(
            result, is_cluster=isinstance(self, GlideClusterClient)
        )

    def _parse_pubsub_state(self, result, is_cluster):
        """Parse subscription state from Rust response."""
        if not isinstance(result, list) or len(result) != 4:
            raise RequestError("Invalid response format from GetSubscriptions")

        desired_dict = result[1]
        actual_dict = result[3]

        if is_cluster:
            from glide_shared.config import GlideClusterClientConfiguration

            PubSubChannelModes = GlideClusterClientConfiguration.PubSubChannelModes
            StateClass = GlideClusterClientConfiguration.PubSubState
            mode_map = {
                "Exact": PubSubChannelModes.Exact,
                "Pattern": PubSubChannelModes.Pattern,
                "Sharded": PubSubChannelModes.Sharded,
            }
        else:
            from glide_shared.config import GlideClientConfiguration

            PubSubChannelModes = GlideClientConfiguration.PubSubChannelModes
            StateClass = GlideClientConfiguration.PubSubState
            mode_map = {
                "Exact": PubSubChannelModes.Exact,
                "Pattern": PubSubChannelModes.Pattern,
            }

        desired_subscriptions = {}
        actual_subscriptions = {}

        for key_bytes, value_list in desired_dict.items():
            key = key_bytes.decode() if isinstance(key_bytes, bytes) else key_bytes
            if key in mode_map:
                values = {v.decode() if isinstance(v, bytes) else v for v in value_list}
                desired_subscriptions[mode_map[key]] = values

        for key_bytes, value_list in actual_dict.items():
            key = key_bytes.decode() if isinstance(key_bytes, bytes) else key_bytes
            if key in mode_map:
                values = {v.decode() if isinstance(v, bytes) else v for v in value_list}
                actual_subscriptions[mode_map[key]] = values

        return StateClass(
            desired_subscriptions=desired_subscriptions,
            actual_subscriptions=actual_subscriptions,
        )

    def close(self):
        if not self._is_closed:
            self._is_closed = True
            with self._pubsub_condition:
                self._pubsub_condition.notify_all()
            self._lib.close_client(self._core_client)
            self._core_client = self._ffi.NULL
            self._pubsub_callback_ref = None


class GlideClusterClient(BaseClient, ClusterCommands):
    """
    Client used for connection to cluster servers.
    For full documentation, see
    https://glide.valkey.io/how-to/client-initialization/#cluster
    """

    def _build_cluster_scan_args(self, match, count, type, allow_non_covered_slots):
        args = []
        if match is not None:
            # Encode match pattern
            if isinstance(match, str):
                encoded_match = match.encode(ENCODING)
            else:
                encoded_match = match
            args.extend([b"MATCH", encoded_match])

        if count is not None:
            args.extend([b"COUNT", str(count).encode(ENCODING)])
        if type is not None:
            args.extend([b"TYPE", type.value.encode(ENCODING)])
        if allow_non_covered_slots:
            args.extend([b"ALLOW_NON_COVERED_SLOTS"])

        return args

    def _cluster_scan(
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

        client_adapter_ptr = self._core_client
        if client_adapter_ptr == self._ffi.NULL:
            raise ValueError("Invalid client pointer.")

        # Use helper method to build args
        args = self._build_cluster_scan_args(
            match, count, type, allow_non_covered_slots
        )
        # Convert cursor to C string
        cursor_string = cursor.get_cursor()
        cursor_bytes = cursor_string.encode(ENCODING) + b"\0"  # Null terminate for C

        # Keep references to prevent GC
        temp_buffers: List[Any] = [cursor_bytes]
        cursor_buffer = self._ffi.from_buffer(cursor_bytes)

        # Prepare FFI arguments
        if args:
            args_array, args_len_array, arg_buffers = self._to_c_strings(args)
            temp_buffers.extend(arg_buffers)  # Keep references alive
            arg_count = len(args)
        else:
            args_array = self._ffi.NULL
            args_len_array = self._ffi.NULL
            arg_count = 0

        result_ptr = self._lib.request_cluster_scan(
            client_adapter_ptr,
            0,
            cursor_buffer,
            arg_count,
            args_array,
            args_len_array,
        )

        response_data = self._handle_cmd_result(result_ptr)

        if not isinstance(response_data, list) or len(response_data) != 2:
            raise RequestError("Unexpected cluster scan response format")

        new_cursor = response_data[0]
        if isinstance(new_cursor, bytes):
            new_cursor = new_cursor.decode(ENCODING)

        keys_list = response_data[1] if response_data[1] is not None else []

        return [ClusterScanCursor(new_cursor), keys_list]


class GlideClient(BaseClient, StandaloneCommands):
    """
    Client used for connection to standalone servers.
    For full documentation, see
    https://glide.valkey.io/how-to/client-initialization/#standalone
    """


TGlideClient = Union[GlideClient, GlideClusterClient]
