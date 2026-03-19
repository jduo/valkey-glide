"""Profiling script - run one client type at a time for clean profiles."""
import sys
import time

N = 50_000
HOST = "localhost"
PORT = 6379
KEY = "pkey"
VALUE = "pvalue"

def profile_sync_set():
    from glide_sync import GlideClient, GlideClientConfiguration, NodeAddress
    c = GlideClient.create(GlideClientConfiguration([NodeAddress(HOST, PORT)]))
    # warmup
    for _ in range(100):
        c.set(KEY, VALUE)
    time.sleep(0.1)
    # profiled section
    for _ in range(N):
        c.set(KEY, VALUE)
    c.close()

def profile_sync_get():
    from glide_sync import GlideClient, GlideClientConfiguration, NodeAddress
    c = GlideClient.create(GlideClientConfiguration([NodeAddress(HOST, PORT)]))
    c.set(KEY, VALUE)
    for _ in range(100):
        c.get(KEY)
    time.sleep(0.1)
    for _ in range(N):
        c.get(KEY)
    c.close()

def profile_async_uds_set():
    import asyncio
    from glide import GlideClient, GlideClientConfiguration, NodeAddress
    async def run():
        c = await GlideClient.create(GlideClientConfiguration([NodeAddress(HOST, PORT)]))
        for _ in range(100):
            await c.set(KEY, VALUE)
        time.sleep(0.1)
        for _ in range(N):
            await c.set(KEY, VALUE)
        await c.close()
    asyncio.run(run())

def profile_async_uds_get():
    import asyncio
    from glide import GlideClient, GlideClientConfiguration, NodeAddress
    async def run():
        c = await GlideClient.create(GlideClientConfiguration([NodeAddress(HOST, PORT)]))
        await c.set(KEY, VALUE)
        for _ in range(100):
            await c.get(KEY)
        time.sleep(0.1)
        for _ in range(N):
            await c.get(KEY)
        await c.close()
    asyncio.run(run())

def profile_async_ffi_set():
    import asyncio
    sys.path.insert(0, "glide-async-ffi")
    from glide_async_ffi_client import GlideAsyncFFIClient
    from glide_shared.config import GlideClientConfiguration, NodeAddress
    async def run():
        c = await GlideAsyncFFIClient.create(GlideClientConfiguration([NodeAddress(HOST, PORT)]))
        for _ in range(100):
            await c.set(KEY, VALUE)
        time.sleep(0.1)
        for _ in range(N):
            await c.set(KEY, VALUE)
        await c.close()
    asyncio.run(run())

def profile_async_ffi_get():
    import asyncio
    sys.path.insert(0, "glide-async-ffi")
    from glide_async_ffi_client import GlideAsyncFFIClient
    from glide_shared.config import GlideClientConfiguration, NodeAddress
    async def run():
        c = await GlideAsyncFFIClient.create(GlideClientConfiguration([NodeAddress(HOST, PORT)]))
        await c.set(KEY, VALUE)
        for _ in range(100):
            await c.get(KEY)
        time.sleep(0.1)
        for _ in range(N):
            await c.get(KEY)
        await c.close()
    asyncio.run(run())

def profile_redis_sync_set():
    import redis
    c = redis.Redis(HOST, PORT)
    for _ in range(100):
        c.set(KEY, VALUE)
    time.sleep(0.1)
    for _ in range(N):
        c.set(KEY, VALUE)
    c.close()

def profile_redis_sync_get():
    import redis
    c = redis.Redis(HOST, PORT)
    c.set(KEY, VALUE)
    for _ in range(100):
        c.get(KEY)
    time.sleep(0.1)
    for _ in range(N):
        c.get(KEY)
    c.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python profile_client.py <mode>")
        print("Modes: sync_set sync_get async_uds_set async_uds_get async_ffi_set async_ffi_get redis_sync_set redis_sync_get")
        sys.exit(1)
    mode = sys.argv[1]
    fn = globals().get(f"profile_{mode}")
    if fn is None:
        print(f"Unknown mode: {mode}")
        sys.exit(1)
    print(f"Profiling {mode} with {N} iterations...")
    t = time.perf_counter()
    fn()
    print(f"Done in {time.perf_counter()-t:.2f}s")
