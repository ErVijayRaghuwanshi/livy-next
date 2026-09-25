#!/bin/bash
set -e

# Target Spark Connect endpoint (defaults to local Spark Connect)
TARGET_REMOTE="${SPARK_REMOTE:-sc://localhost:15002}"

# Decide whether to start local Spark Connect Server:
# Start local Spark Connect if:
# 1. START_LOCAL_SPARK is "true" OR
# 2. START_LOCAL_SPARK is not "false" AND TARGET_REMOTE points to localhost / 127.0.0.1
START_LOCAL="${START_LOCAL_SPARK:-auto}"

if [ "$START_LOCAL" = "true" ] || { [ "$START_LOCAL" != "false" ] && echo "$TARGET_REMOTE" | grep -Eq 'localhost|127\.0\.0\.1'; }; then
    echo "=========================================================="
    echo " Starting Local Apache Spark Connect Server (Spark 4.1.2) "
    echo " Master: local[*] | Port: 15002                           "
    echo "=========================================================="

    # Spark's spark-submit fails with:
    # "Remote cannot be specified with master and/or deploy mode"
    # if SPARK_REMOTE is in the environment. We must unset SPARK_REMOTE
    # when starting the internal Spark Connect Server.
    env -u SPARK_REMOTE /opt/spark/sbin/start-connect-server.sh --master "local[*]"

    echo "Waiting for Spark Connect Server to accept gRPC connections on port 15002..."
    max_wait=60
    waited=0
    while ! python3 -c "import socket, sys; s = socket.socket(); err = s.connect_ex(('127.0.0.1', 15002)); s.close(); sys.exit(err)" 2>/dev/null; do
        sleep 1
        waited=$((waited + 1))
        if [ "$waited" -ge "$max_wait" ]; then
            echo "ERROR: Timed out waiting for Spark Connect Server on port 15002 after ${max_wait}s."
            echo "--- Spark Connect Server Logs ---"
            cat /opt/spark/logs/* 2>/dev/null || true
            exit 1
        fi
    done
    echo "Spark Connect Server is healthy and listening on port 15002! (ready in ${waited}s)"
else
    echo "=========================================================="
    echo " Connecting to External Spark Connect Remote Endpoint      "
    echo " Target: $TARGET_REMOTE                                   "
    echo "=========================================================="
fi

echo "=========================================================="
echo " Starting Livy-Next REST API Gateway                       "
echo " Bind Address: ${LIVY_ADDR:-:8998}                        "
echo " Spark Remote: $TARGET_REMOTE                             "
echo "=========================================================="

EXTRA_ARGS=()
if [ -n "$IDLE_TIMEOUT" ]; then
    EXTRA_ARGS+=(--idle-timeout "$IDLE_TIMEOUT")
fi
if [ -n "$DEAD_TIMEOUT" ]; then
    EXTRA_ARGS+=(--dead-timeout "$DEAD_TIMEOUT")
fi
if [ -n "$CORS_ALLOWED_ORIGINS" ]; then
    EXTRA_ARGS+=(--cors-allowed-origins "$CORS_ALLOWED_ORIGINS")
else
    EXTRA_ARGS+=(--cors-allowed-origins "*")
fi

exec /usr/local/bin/livy-next \
    --addr "${LIVY_ADDR:-:8998}" \
    --spark-remote "$TARGET_REMOTE" \
    "${EXTRA_ARGS[@]}" \
    "$@"
