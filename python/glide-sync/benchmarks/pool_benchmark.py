#!/usr/bin/env python3
# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

"""
Benchmark: Client Pool vs Fresh Client per Operation (Python sync).

Demonstrates the value of client-instance pooling for synchronous Python workloads
(Django thread workers, Celery tasks, Flask/Gunicorn handlers).

Usage:
    python3 pool_benchmark.py [--host localhost] [--port 6379] [--iterations 100]
"""

import argparse
import sys
import time
import uuid
from pathlib import Path

# Add parent packages to path for development
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "glide-shared"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from glide_sync import GlideClient, GlideClientConfiguration, NodeAddress
from glide_sync.client_pool import ClientPool, PoolConfig


def benchmark_fresh_client_per_op(config, iterations):
    """Scenario A: Create a new client for each operation (expensive)."""
    counter_key = f"bench-pool-{uuid.uuid4()}"

    # Setup
    setup_client = GlideClient.create(config)
    setup_client.set(counter_key, "0")
    setup_client.close()

    start = time.perf_counter()
    for i in range(iterations):
        client = GlideClient.create(config)
        val = client.get(counter_key)
        client.set(counter_key, str(int(val) + 1))
        client.close()
    elapsed = time.perf_counter() - start

    # Verify
    verify_client = GlideClient.create(config)
    final_val = verify_client.get(counter_key)
    verify_client.delete([counter_key])
    verify_client.close()

    return elapsed, int(final_val)


def benchmark_pooled(config, iterations):
    """Scenario B: Use the pool (reuse connections)."""
    counter_key = f"bench-pool-{uuid.uuid4()}"
    pool_config = PoolConfig(max_size=5, min_idle=1)

    pool = ClientPool(config, pool_config)

    # Wait for min_idle warmup
    time.sleep(2)

    # Setup
    with pool.borrow() as client:
        client.set(counter_key, "0")

    start = time.perf_counter()
    for i in range(iterations):
        with pool.borrow() as client:
            val = client.get(counter_key)
            client.set(counter_key, str(int(val) + 1))
    elapsed = time.perf_counter() - start

    # Verify
    with pool.borrow() as client:
        final_val = client.get(counter_key)
        client.delete([counter_key])

    pool.close()
    return elapsed, int(final_val)


def benchmark_shared_client(config, iterations):
    """Scenario C: Single shared client (baseline — best case for sync single-thread)."""
    counter_key = f"bench-pool-{uuid.uuid4()}"

    client = GlideClient.create(config)
    client.set(counter_key, "0")

    start = time.perf_counter()
    for i in range(iterations):
        val = client.get(counter_key)
        client.set(counter_key, str(int(val) + 1))
    elapsed = time.perf_counter() - start

    final_val = client.get(counter_key)
    client.delete([counter_key])
    client.close()

    return elapsed, int(final_val)


def main():
    parser = argparse.ArgumentParser(description="Pool benchmark for Python sync client")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--iterations", type=int, default=100)
    args = parser.parse_args()

    config = GlideClientConfiguration([NodeAddress(args.host, args.port)])

    print(f"\n=== Python Sync Client Pool Benchmark ===")
    print(f"Server: {args.host}:{args.port}")
    print(f"Iterations: {args.iterations}")
    print(f"Each iteration: GET + SET (increment counter)")
    print()

    # Scenario A
    print("--- Scenario A: Fresh client per operation ---")
    a_elapsed, a_final = benchmark_fresh_client_per_op(config, args.iterations)
    a_per_op = (a_elapsed / args.iterations) * 1000
    print(f"    Total: {a_elapsed*1000:.1f} ms")
    print(f"    Per operation: {a_per_op:.2f} ms")
    print(f"    Counter: {a_final} (expected: {args.iterations})")
    print()

    # Scenario B
    print("--- Scenario B: Pooled client ---")
    b_elapsed, b_final = benchmark_pooled(config, args.iterations)
    b_per_op = (b_elapsed / args.iterations) * 1000
    print(f"    Total: {b_elapsed*1000:.1f} ms")
    print(f"    Per operation: {b_per_op:.2f} ms")
    print(f"    Counter: {b_final} (expected: {args.iterations})")
    print()

    # Scenario C
    print("--- Scenario C: Shared client (baseline) ---")
    c_elapsed, c_final = benchmark_shared_client(config, args.iterations)
    c_per_op = (c_elapsed / args.iterations) * 1000
    print(f"    Total: {c_elapsed*1000:.1f} ms")
    print(f"    Per operation: {c_per_op:.2f} ms")
    print(f"    Counter: {c_final} (expected: {args.iterations})")
    print()

    # Summary
    print("=== Summary ===")
    print(f"    Fresh client per op:  {a_per_op:.2f} ms/op")
    print(f"    Pooled client:        {b_per_op:.2f} ms/op")
    print(f"    Shared client:        {c_per_op:.2f} ms/op")
    print(f"    Pool speedup vs fresh: {a_per_op / b_per_op:.1f}x")
    print(f"    Pool overhead vs shared: +{b_per_op - c_per_op:.2f} ms/op")
    print()


if __name__ == "__main__":
    main()
