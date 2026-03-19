"""Self-sampling profiler: writes PID, waits for signal, then runs."""
import os, signal, sys, time
sys.path.insert(0, "glide-async-ffi")

N = 50_000
HOST = "localhost"
PORT = 6379

def run_sync_set():
    from glide_sync import GlideClient, GlideClientConfiguration, NodeAddress
    c = GlideClient.create(GlideClientConfiguration([NodeAddress(HOST, PORT)]))
    for _ in range(500): c.set("k","v")  # warmup
    # Write PID so sampler can attach
    with open("/tmp/profile_pid", "w") as f: f.write(str(os.getpid()))
    print(f"PID {os.getpid()} ready, waiting for SIGUSR1...", flush=True)
    ready = [False]
    signal.signal(signal.SIGUSR1, lambda *_: ready.__setitem__(0, True))
    while not ready[0]: time.sleep(0.01)
    print("Running...", flush=True)
    t = time.perf_counter()
    for _ in range(N): c.set("k","v")
    print(f"Done in {time.perf_counter()-t:.2f}s", flush=True)
    c.close()

def run_redis_sync_set():
    import redis
    c = redis.Redis(HOST, PORT)
    for _ in range(500): c.set("k","v")
    with open("/tmp/profile_pid", "w") as f: f.write(str(os.getpid()))
    print(f"PID {os.getpid()} ready, waiting for SIGUSR1...", flush=True)
    ready = [False]
    signal.signal(signal.SIGUSR1, lambda *_: ready.__setitem__(0, True))
    while not ready[0]: time.sleep(0.01)
    print("Running...", flush=True)
    t = time.perf_counter()
    for _ in range(N): c.set("k","v")
    print(f"Done in {time.perf_counter()-t:.2f}s", flush=True)
    c.close()

if __name__ == "__main__":
    globals()[f"run_{sys.argv[1]}"]()
