#!/bin/bash
# Run the inflight pressure test on EC2 against ElastiCache.
# This is more reliable than Docker-local because:
#   1. Real network latency to ElastiCache causes natural request pileup
#   2. EC2 cgroup memory limits are enforced strictly
#
# Usage:
#   ./run-on-ec2.sh <elasticache-endpoint> <port> <path-to-glide-jar>
#
# Example:
#   ./run-on-ec2.sh intuit-repro-valkey.nra7gl.clustercfg.usw2.cache.amazonaws.com 6379 ./glide-local.jar

set -e

if [ $# -lt 3 ]; then
    echo "Usage: $0 <endpoint> <port> <jar-path>"
    exit 1
fi

ENDPOINT="$1"
PORT="$2"
JAR_PATH="$3"

if [ ! -f "$JAR_PATH" ]; then
    echo "ERROR: JAR not found: $JAR_PATH"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Inflight Pressure Test (EC2 mode) ==="
echo "Endpoint: $ENDPOINT:$PORT"
echo "JAR: $JAR_PATH"
echo ""

# Compile the test
echo "[BUILD] Compiling test..."
javac -cp "$JAR_PATH" -d /tmp/inflight-test "$SCRIPT_DIR/src/main/java/InflightPressureTest.java"

# Run with memory constraints similar to Docker
echo "[RUN] Starting test with memory constraints..."
echo ""

# Use -Xmx1g to simulate tight native memory (assumes EC2 has cgroup or we rely on natural pressure)
java \
    -Xmx1g -Xms1g \
    -XX:MaxDirectMemorySize=96m \
    -XX:+UseG1GC \
    -cp "/tmp/inflight-test:$JAR_PATH" \
    InflightPressureTest "$ENDPOINT" "$PORT"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "=== TEST PASSED ==="
else
    echo ""
    echo "=== TEST FAILED (exit code: $EXIT_CODE) ==="
fi

exit $EXIT_CODE
