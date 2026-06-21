#!/bin/bash
# Deploy and run the inflight limit test on EC2.
# Compares behavior WITH fix vs WITHOUT fix.
#
# Prerequisites:
#   - SSH access to EC2 instance
#   - ElastiCache cluster endpoint
#   - Both JARs pre-built (with-fix and without-fix)
#
# Usage:
#   ./deploy-and-test-ec2.sh <ec2-ip> <ssh-key> <elasticache-endpoint> <port>
#
# Example:
#   ./deploy-and-test-ec2.sh 54.191.246.11 ../../deadlock-repro/infra/intuit-repro-key.pem \
#     intuit-repro-valkey.nra7gl.clustercfg.usw2.cache.amazonaws.com 6379

set -e

if [ $# -lt 4 ]; then
    echo "Usage: $0 <ec2-ip> <ssh-key> <elasticache-endpoint> <port>"
    exit 1
fi

EC2_IP="$1"
SSH_KEY="$2"
ENDPOINT="$3"
PORT="$4"

SSH="ssh -i $SSH_KEY -o StrictHostKeyChecking=no ec2-user@$EC2_IP"
SCP="scp -i $SSH_KEY -o StrictHostKeyChecking=no"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

echo "=== Inflight Limit Test: EC2 Deployment ==="
echo "EC2: $EC2_IP"
echo "Endpoint: $ENDPOINT:$PORT"
echo ""

# Step 1: Upload test source
echo "[1/4] Uploading test source..."
$SSH "mkdir -p ~/inflight-test"
$SCP "$SCRIPT_DIR/src/main/java/InflightLimitTest.java" "ec2-user@$EC2_IP:~/inflight-test/"
$SCP "$SCRIPT_DIR/src/main/java/InflightPressureTest.java" "ec2-user@$EC2_IP:~/inflight-test/"

# Step 2: Check for pre-built JARs on EC2
echo "[2/4] Checking for GLIDE JARs on EC2..."
$SSH "ls -la ~/inflight-test/*.jar 2>/dev/null || echo 'No JARs found'"

echo ""
echo "To run the test, ensure you have JARs on EC2:"
echo "  ~/inflight-test/glide-with-fix.jar    (built from jduo/sync-inflight-check-r22)"
echo "  ~/inflight-test/glide-without-fix.jar  (built from release-2.2 base)"
echo ""
echo "Build commands (run on EC2 in the valkey-glide repo):"
echo "  # With fix:"
echo "  git checkout jduo/sync-inflight-check-r22"
echo "  cd java && ./gradlew :client:buildAllRelease"
echo "  cp client/build/libs/client-linux-x86_64.jar ~/inflight-test/glide-with-fix.jar"
echo ""
echo "  # Without fix:"
echo "  git checkout release-2.2"
echo "  cd java && ./gradlew :client:buildAllRelease"
echo "  cp client/build/libs/client-linux-x86_64.jar ~/inflight-test/glide-without-fix.jar"
echo ""

# Step 3: Create run script on EC2
echo "[3/4] Creating run script on EC2..."
$SSH 'cat > ~/inflight-test/run-comparison.sh << '\''SCRIPT'\''
#!/bin/bash
set -e

ENDPOINT="$1"
PORT="$2"

if [ -z "$ENDPOINT" ] || [ -z "$PORT" ]; then
    echo "Usage: ./run-comparison.sh <endpoint> <port>"
    exit 1
fi

cd ~/inflight-test

echo "=========================================="
echo "  Inflight Limit Test: WITH FIX"
echo "=========================================="
if [ -f glide-with-fix.jar ]; then
    javac -cp glide-with-fix.jar -d /tmp/inflight-with-fix InflightLimitTest.java
    java -Xmx1g -Xms1g -XX:MaxDirectMemorySize=96m -XX:+UseG1GC \
        -cp "/tmp/inflight-with-fix:glide-with-fix.jar" \
        InflightLimitTest "$ENDPOINT" "$PORT"
    WITH_FIX_EXIT=$?
    echo ""
    echo "WITH FIX exit code: $WITH_FIX_EXIT"
else
    echo "SKIP: glide-with-fix.jar not found"
    WITH_FIX_EXIT=-1
fi

echo ""
echo "=========================================="
echo "  Inflight Limit Test: WITHOUT FIX"
echo "=========================================="
if [ -f glide-without-fix.jar ]; then
    javac -cp glide-without-fix.jar -d /tmp/inflight-without-fix InflightLimitTest.java 2>/dev/null || \
    javac -cp glide-without-fix.jar -d /tmp/inflight-without-fix InflightPressureTest.java 2>/dev/null
    java -Xmx1g -Xms1g -XX:MaxDirectMemorySize=96m -XX:+UseG1GC \
        -cp "/tmp/inflight-without-fix:glide-without-fix.jar" \
        InflightLimitTest "$ENDPOINT" "$PORT" || true
    WITHOUT_FIX_EXIT=$?
    echo ""
    echo "WITHOUT FIX exit code: $WITHOUT_FIX_EXIT"
else
    echo "SKIP: glide-without-fix.jar not found"
    WITHOUT_FIX_EXIT=-1
fi

echo ""
echo "=========================================="
echo "  COMPARISON RESULTS"
echo "=========================================="
echo "WITH FIX:    exit=$WITH_FIX_EXIT (expected: 0 = PASS)"
echo "WITHOUT FIX: exit=$WITHOUT_FIX_EXIT (expected: 1 = FAIL)"

if [ "$WITH_FIX_EXIT" = "0" ] && [ "$WITHOUT_FIX_EXIT" = "1" ]; then
    echo ""
    echo "SUCCESS: Test correctly differentiates fix vs no-fix"
elif [ "$WITH_FIX_EXIT" = "0" ] && [ "$WITHOUT_FIX_EXIT" = "0" ]; then
    echo ""
    echo "INCONCLUSIVE: Both passed - test may not be sensitive enough"
else
    echo ""
    echo "UNEXPECTED: Review results above"
fi
SCRIPT'
$SSH "chmod +x ~/inflight-test/run-comparison.sh"

# Step 4: Instructions
echo "[4/4] Setup complete!"
echo ""
echo "To run the comparison test on EC2:"
echo "  ssh -i $SSH_KEY ec2-user@$EC2_IP"
echo "  cd ~/inflight-test"
echo "  ./run-comparison.sh $ENDPOINT $PORT"
echo ""
echo "Or run just the with-fix test:"
echo "  javac -cp glide-with-fix.jar -d /tmp/test InflightLimitTest.java"
echo "  java -Xmx1g -Xms1g -cp '/tmp/test:glide-with-fix.jar' InflightLimitTest $ENDPOINT $PORT"
