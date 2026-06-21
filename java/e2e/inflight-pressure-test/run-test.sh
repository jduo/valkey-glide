#!/bin/bash
# Build and run the inflight pressure E2E test.
#
# Prerequisites:
#   - Docker and docker compose installed
#   - glide-local.jar in this directory (built from the branch under test)
#
# To build glide-local.jar from the current branch:
#   cd ../../ && ./gradlew :client:buildAllRelease
#   cp client/build/libs/glide-valkey-java-*-all.jar ../e2e/inflight-pressure-test/glide-local.jar
#
# Exit codes:
#   0 = test passed (thread pool stayed bounded)
#   1 = test failed (thread pool exploded or stuck)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [ ! -f glide-local.jar ]; then
    echo "ERROR: glide-local.jar not found in $SCRIPT_DIR"
    echo ""
    echo "Build it from the java/ directory:"
    echo "  cd ../../ && ./gradlew :client:buildAllRelease"
    echo "  cp client/build/libs/glide-valkey-java-*-all.jar ../e2e/inflight-pressure-test/glide-local.jar"
    exit 1
fi

echo "=== Building and running inflight pressure test ==="
echo "JAR: $(ls -la glide-local.jar)"
echo ""

# Clean up any previous run
docker compose down --remove-orphans 2>/dev/null || true

# Build and run
docker compose up --build --abort-on-container-exit --exit-code-from test

EXIT_CODE=$?
docker compose down --remove-orphans 2>/dev/null || true

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "=== TEST PASSED ==="
else
    echo ""
    echo "=== TEST FAILED (exit code: $EXIT_CODE) ==="
fi

exit $EXIT_CODE
