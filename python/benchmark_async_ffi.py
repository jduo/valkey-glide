"""
Benchmark: Compare async-UDS, sync-FFI, and async-FFI clients.

Usage:
    # Start a valkey server first:
    valkey-server --save '' --port 6379 --logfile ''

    # Run from the python/ directory with the venv activated:
    cd ~/gh/jduo/valkey-glide/python
    source .env/bin/activate
    python benchmark_async_ffi.py
"""

import asyncio
import sys
import time
from pathlib import Path

# Ensure glide-async-ffi is importable
sys.path.insert(0, str(Path(__file__).resolve().parent / "glide-async-ffi"))

N_ITERATIONS = 10_000
KEY = "bench_key"
VALUE = "bench_value"
HOST = "localhost"
PORT = 6379


async def bench_glide_async_uds():
    """Current async client (PyO3 + UDS)"""
    from glide import GlideClient, GlideClientConfiguration, NodeAddress

    config = GlideClientConfiguration([NodeAddress(HOST, PORT)])
    client = await GlideClient.create(config)

    start = time.perf_counter()
    for _ in range(N_ITERATIONS):
        await client.set(KEY, VALUE)
    elapsed = time.perf_counter() - start
    print(f"glide async (UDS):     {elapsed:.4f}s  ({N_ITERATIONS/elapsed:.0f} ops/s)")

    await client.close()


def bench_glide_sync_ffi():
    """Current sync client (CFFI + FFI blocking)"""
    from glide_sync import GlideClient, GlideClientConfiguration, NodeAddress

    config = GlideClientConfiguration([NodeAddress(HOST, PORT)])
    client = GlideClient.create(config)

    start = time.perf_counter()
    for _ in range(N_ITERATIONS):
        client.set(KEY, VALUE)
    elapsed = time.perf_counter() - start
    print(f"glide sync (FFI):      {elapsed:.4f}s  ({N_ITERATIONS/elapsed:.0f} ops/s)")

    client.close()


async def bench_glide_async_ffi():
    """New async client (CFFI + FFI async callbacks)"""
    from glide_async_ffi_client import GlideAsyncFFIClient
    from glide_shared.config import GlideClientConfiguration, NodeAddress

    config = GlideClientConfiguration([NodeAddress(HOST, PORT)])
    client = await GlideAsyncFFIClient.create(config)

    start = time.perf_counter()
    for _ in range(N_ITERATIONS):
        await client.set(KEY, VALUE)
    elapsed = time.perf_counter() - start
    print(f"glide async (FFI):     {elapsed:.4f}s  ({N_ITERATIONS/elapsed:.0f} ops/s)")

    await client.close()


async def bench_glide_async_ffi_get():
    """Async FFI GET benchmark"""
    from glide_async_ffi_client import GlideAsyncFFIClient
    from glide_shared.config import GlideClientConfiguration, NodeAddress

    config = GlideClientConfiguration([NodeAddress(HOST, PORT)])
    client = await GlideAsyncFFIClient.create(config)

    # Ensure key exists
    await client.set(KEY, VALUE)

    start = time.perf_counter()
    for _ in range(N_ITERATIONS):
        await client.get(KEY)
    elapsed = time.perf_counter() - start
    print(f"glide async (FFI) GET: {elapsed:.4f}s  ({N_ITERATIONS/elapsed:.0f} ops/s)")

    await client.close()


async def bench_glide_async_uds_get():
    """Current async GET benchmark"""
    from glide import GlideClient, GlideClientConfiguration, NodeAddress

    config = GlideClientConfiguration([NodeAddress(HOST, PORT)])
    client = await GlideClient.create(config)

    await client.set(KEY, VALUE)

    start = time.perf_counter()
    for _ in range(N_ITERATIONS):
        await client.get(KEY)
    elapsed = time.perf_counter() - start
    print(f"glide async (UDS) GET: {elapsed:.4f}s  ({N_ITERATIONS/elapsed:.0f} ops/s)")

    await client.close()


def bench_glide_sync_ffi_get():
    """Sync FFI GET benchmark"""
    from glide_sync import GlideClient, GlideClientConfiguration, NodeAddress

    config = GlideClientConfiguration([NodeAddress(HOST, PORT)])
    client = GlideClient.create(config)

    client.set(KEY, VALUE)

    start = time.perf_counter()
    for _ in range(N_ITERATIONS):
        client.get(KEY)
    elapsed = time.perf_counter() - start
    print(f"glide sync (FFI) GET:  {elapsed:.4f}s  ({N_ITERATIONS/elapsed:.0f} ops/s)")

    client.close()


async def main():
    print(f"Python {sys.version}")
    print(f"Iterations: {N_ITERATIONS}")
    print(f"Server: {HOST}:{PORT}")
    print()

    print("=== SET benchmark ===")
    bench_glide_sync_ffi()
    await bench_glide_async_uds()
    await bench_glide_async_ffi()

    print()
    print("=== GET benchmark ===")
    bench_glide_sync_ffi_get()
    await bench_glide_async_uds_get()
    await bench_glide_async_ffi_get()


if __name__ == "__main__":
    asyncio.run(main())
