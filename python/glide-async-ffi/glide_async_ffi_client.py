# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

"""
Async Python client using direct FFI calls with async callbacks.

Instead of protobuf+UDS (glide-async) or blocking FFI (glide-sync),
this uses the FFI's AsyncClient mode: command() spawns a tokio task
and returns immediately, then a callback fires from the Rust thread
to resolve the Python asyncio.Future via loop.call_soon_threadsafe().
"""

import asyncio
import sys
import threading
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from glide_shared.config import (
    BaseClientConfiguration,
    GlideClientConfiguration,
    GlideClusterClientConfiguration,
)
from glide_shared.constants import OK, TEncodable, TResult
from glide_shared.exceptions import (
    ClosingError,
    ConfigurationError,
    RequestError,
    get_request_error_class,
)
from glide_shared.protobuf.command_request_pb2 import RequestType
from glide_shared.routes import Route, build_protobuf_route

# Import FFI from the sync client
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent / "glide-sync"))
from glide_sync._glide_ffi import _GlideFFI
from glide_sync.glide_client import ENCODING

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class GlideAsyncFFIClient:
    """Async client using direct FFI calls instead of protobuf+socket."""

    def __init__(self, config: GlideClientConfiguration):
        _glide_ffi = _GlideFFI()
        self._ffi = _glide_ffi.ffi
        self._lib = _glide_ffi.lib
        self._config = config
        self._is_closed = False
        self._pending_futures: Dict[int, asyncio.Future] = {}
        self._callback_counter = 0
        self._lock = threading.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._core_client = None

    @classmethod
    async def create(cls, config: GlideClientConfiguration) -> Self:
        self = cls(config)
        self._loop = asyncio.get_running_loop()

        # Create CFFI callbacks that will be called from Rust threads
        @self._ffi.callback("SuccessCallback")
        def _success_callback(index_ptr, message):
            self._on_success(index_ptr, message)

        @self._ffi.callback("FailureCallback")
        def _failure_callback(index_ptr, error_message, error_type):
            self._on_failure(index_ptr, error_message, error_type)

        # Store references to prevent GC
        self._success_callback = _success_callback
        self._failure_callback = _failure_callback

        # Build connection request protobuf
        conn_req = config._create_a_protobuf_conn_request(
            cluster_mode=isinstance(config, GlideClusterClientConfiguration)
        )
        conn_req_bytes = conn_req.SerializeToString()

        # Create AsyncClient type with callbacks
        client_type = self._ffi.new(
            "ClientType*",
            {
                "_type": self._ffi.cast("ClientTypeEnum", 0),  # Async = 0
                "async_client": {
                    "success_callback": _success_callback,
                    "failure_callback": _failure_callback,
                },
            },
        )

        client_response_ptr = self._lib.create_client(
            conn_req_bytes,
            len(conn_req_bytes),
            client_type,
            self._ffi.NULL,  # no pubsub callback for now
        )

        if client_response_ptr == self._ffi.NULL:
            raise ClosingError("Failed to create client, response pointer is NULL.")

        client_response = self._ffi.cast("ConnectionResponse*", client_response_ptr)
        if client_response.conn_ptr != self._ffi.NULL:
            self._core_client = client_response.conn_ptr
        else:
            error_msg = (
                self._ffi.string(client_response.connection_error_message).decode(ENCODING)
                if client_response.connection_error_message != self._ffi.NULL
                else "Unknown error"
            )
            self._lib.free_connection_response(client_response_ptr)
            raise ClosingError(error_msg)

        self._lib.free_connection_response(client_response_ptr)
        return self

    def _get_callback_id(self) -> int:
        with self._lock:
            self._callback_counter += 1
            return self._callback_counter

    def _on_success(self, index_ptr: int, message) -> None:
        """Called from Rust thread on command success. Must copy data synchronously."""
        # Parse the response while we still have the pointer (Rust frees it after callback returns)
        try:
            result = self._handle_response(message)
        except Exception as e:
            result = e

        # Resolve the future on the event loop thread
        loop = self._loop
        if loop is None or loop.is_closed():
            return

        with self._lock:
            fut = self._pending_futures.pop(index_ptr, None)

        if fut is not None and not fut.done():
            if isinstance(result, Exception):
                loop.call_soon_threadsafe(fut.set_exception, result)
            else:
                loop.call_soon_threadsafe(fut.set_result, result)

    def _on_failure(self, index_ptr: int, error_message, error_type: int) -> None:
        """Called from Rust thread on command failure. Must copy data synchronously."""
        try:
            msg = self._ffi.string(error_message).decode(ENCODING)
        except Exception:
            msg = "Unknown error"

        error_class = get_request_error_class(error_type)
        exc = error_class(msg)

        loop = self._loop
        if loop is None or loop.is_closed():
            return

        with self._lock:
            fut = self._pending_futures.pop(index_ptr, None)

        if fut is not None and not fut.done():
            loop.call_soon_threadsafe(fut.set_exception, exc)

    def _handle_response(self, message):
        """Parse CommandResponse into Python objects. Called from Rust thread."""
        if message == self._ffi.NULL:
            return None

        msg = message[0] if self._ffi.typeof(message).cname == "CommandResponse *" else message

        return self._parse_command_response(msg)

    def _parse_command_response(self, msg):
        rt = msg.response_type
        if rt == 0:  # Null
            return None
        elif rt == 1:  # Int
            return msg.int_value
        elif rt == 2:  # Float
            return msg.float_value
        elif rt == 3:  # Bool
            return bool(msg.bool_value)
        elif rt == 4:  # String
            return bytes(self._ffi.buffer(msg.string_value, msg.string_value_len))
        elif rt == 5:  # Array
            return [
                self._parse_command_response(
                    self._ffi.cast("struct CommandResponse*", msg.array_value + i)[0]
                )
                for i in range(msg.array_value_len)
            ]
        elif rt == 6:  # Map
            result = {}
            for i in range(msg.array_value_len):
                elem = self._ffi.cast("struct CommandResponse*", msg.array_value + i)[0]
                key = self._parse_command_response(
                    self._ffi.cast("struct CommandResponse*", elem.map_key)[0]
                )
                val = self._parse_command_response(
                    self._ffi.cast("struct CommandResponse*", elem.map_value)[0]
                )
                result[key] = val
            return result
        elif rt == 7:  # Sets
            result = set()
            for i in range(msg.sets_value_len):
                elem = self._ffi.cast(
                    f"struct CommandResponse[{msg.sets_value_len}]", msg.sets_value
                )[i]
                result.add(self._parse_command_response(elem))
            return result
        elif rt == 8:  # Ok
            return OK
        elif rt == 9:  # Error
            error_msg = bytes(self._ffi.buffer(msg.string_value, msg.string_value_len))
            raise RequestError(str(error_msg))
        else:
            raise RequestError(f"Unknown response type: {rt}")

    def _to_c_strings(self, args: List[TEncodable]):
        """Convert Python arguments to C-compatible pointers and lengths."""
        c_strings = []
        string_lengths = []
        buffers = []

        for arg in args:
            if isinstance(arg, str):
                arg_bytes = arg.encode(ENCODING)
            elif isinstance(arg, (bytes, bytearray, memoryview)):
                arg_bytes = bytes(arg)
            else:
                raise TypeError(f"Unsupported argument type: {type(arg)}")

            buffers.append(arg_bytes)
            c_strings.append(
                self._ffi.cast("size_t", self._ffi.from_buffer(arg_bytes))
            )
            string_lengths.append(len(arg_bytes))

        return (
            self._ffi.new("size_t[]", c_strings),
            self._ffi.new("unsigned long[]", string_lengths),
            buffers,
        )

    def _to_c_route_ptr_and_len(self, route: Optional[Route]):
        proto_route = build_protobuf_route(route)
        if proto_route:
            route_bytes = proto_route.SerializeToString()
            route_ptr = self._ffi.from_buffer(route_bytes)
            route_len = len(route_bytes)
        else:
            route_bytes = None
            route_ptr = self._ffi.NULL
            route_len = 0
        return route_ptr, route_len, route_bytes

    async def _execute_command(
        self,
        request_type: RequestType.ValueType,
        args: List[TEncodable],
        route: Optional[Route] = None,
    ) -> TResult:
        if self._is_closed:
            raise ClosingError("Client is closed.")

        callback_id = self._get_callback_id()
        fut = self._loop.create_future()

        with self._lock:
            self._pending_futures[callback_id] = fut

        c_args, c_lengths, buffers = self._to_c_strings(args)
        route_ptr, route_len, route_bytes = self._to_c_route_ptr_and_len(route)

        # command() returns NULL for async clients — the callback resolves the future
        self._lib.command(
            self._core_client,
            callback_id,
            request_type,
            len(args),
            c_args,
            c_lengths,
            route_ptr,
            route_len,
            0,  # span_ptr
        )

        return await fut

    # ==================== Command Methods ====================

    async def set(self, key: TEncodable, value: TEncodable) -> TResult:
        return await self._execute_command(RequestType.Set, [key, value])

    async def get(self, key: TEncodable) -> TResult:
        return await self._execute_command(RequestType.Get, [key])

    async def hgetall(self, key: TEncodable) -> TResult:
        return await self._execute_command(RequestType.HGetAll, [key])

    async def hset(self, key: TEncodable, field_value_map: Dict[TEncodable, TEncodable]) -> TResult:
        args: List[TEncodable] = [key]
        for field, value in field_value_map.items():
            args.extend([field, value])
        return await self._execute_command(RequestType.HSet, args)

    async def scan(
        self,
        cursor: TEncodable,
        match: Optional[TEncodable] = None,
        count: Optional[int] = None,
        type: Optional[str] = None,
    ) -> Tuple[bytes, List[bytes]]:
        args: List[TEncodable] = [cursor]
        if match is not None:
            args.extend(["MATCH", match])
        if count is not None:
            args.extend(["COUNT", str(count)])
        if type is not None:
            args.extend(["TYPE", type])
        result = await self._execute_command(RequestType.Scan, args)
        # Result is [cursor_bytes, [key1, key2, ...]]
        return result[0], result[1]

    async def close(self) -> None:
        if self._is_closed:
            return
        self._is_closed = True

        with self._lock:
            for fut in self._pending_futures.values():
                if not fut.done():
                    fut.cancel()
            self._pending_futures.clear()

        if self._core_client is not None:
            self._lib.close_client(self._core_client)
            self._core_client = None
