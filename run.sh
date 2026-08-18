#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

TEMPORAL_PID=""
WORKER_PID=""
SERVER_PID=""

cleanup() {
    local status=$?
    trap - EXIT
    echo ""
    echo "Shutting down..."

    for pid in "$SERVER_PID" "$WORKER_PID" "$TEMPORAL_PID"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    for pid in "$SERVER_PID" "$WORKER_PID" "$TEMPORAL_PID"; do
        if [[ -n "$pid" ]]; then
            wait "$pid" 2>/dev/null || true
        fi
    done

    echo "Done."
    exit "$status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Load .env if present
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

for command_name in uv temporal; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "Error: $command_name is required." >&2
        if [[ "$command_name" == "uv" ]]; then
            echo "Install it with 'brew install uv' or see https://docs.astral.sh/uv/." >&2
        else
            echo "Install it with 'brew install temporal' or see https://docs.temporal.io/cli." >&2
        fi
        exit 1
    fi
done

use_existing_temporal=false
if command -v lsof >/dev/null 2>&1; then
    if lsof -nP -iTCP:8080 -sTCP:LISTEN >/dev/null 2>&1; then
        echo "Error: port 8080 is already in use. Stop that process and retry." >&2
        exit 1
    fi

    if lsof -nP -iTCP:7233 -sTCP:LISTEN >/dev/null 2>&1; then
        if temporal operator cluster health 2>/dev/null | grep -q "SERVING"; then
            use_existing_temporal=true
        else
            echo "Error: port 7233 is in use by a service that is not a healthy Temporal server." >&2
            exit 1
        fi
    elif lsof -nP -iTCP:8233 -sTCP:LISTEN >/dev/null 2>&1; then
        echo "Error: port 8233 is already in use. Stop that process and retry." >&2
        exit 1
    fi
fi

echo "Syncing dependencies with uv..."
uv sync --all-extras

echo "Cleaning up state..."
rm -f fleet_state.db fleet_state.db-wal fleet_state.db-shm

if [[ "$use_existing_temporal" == true ]]; then
    echo "Using existing healthy Temporal server on localhost:7233..."
else
    echo "Starting Temporal dev server..."
    temporal server start-dev &
    TEMPORAL_PID=$!

    echo "Waiting for Temporal to be ready..."
    temporal_ready=false
    for _ in {1..60}; do
        if ! kill -0 "$TEMPORAL_PID" 2>/dev/null; then
            wait "$TEMPORAL_PID"
            echo "Error: Temporal development server exited before becoming ready." >&2
            exit 1
        fi
        if temporal operator cluster health 2>/dev/null | grep -q "SERVING"; then
            temporal_ready=true
            break
        fi
        sleep 0.5
    done
    if [[ "$temporal_ready" != true ]]; then
        echo "Error: Temporal development server was not ready after 30 seconds." >&2
        exit 1
    fi
fi

echo "Starting workers..."
uv run python -m agent_fleet.worker &
WORKER_PID=$!
sleep 2

echo "Starting server..."
uv run python -m agent_fleet.server &
SERVER_PID=$!

echo ""
echo "  App:      http://localhost:8080"
echo "  Temporal: http://localhost:8233"
echo ""
echo "Press Ctrl+C to stop."

while kill -0 "$WORKER_PID" 2>/dev/null && kill -0 "$SERVER_PID" 2>/dev/null; do
    sleep 1
done

if ! kill -0 "$WORKER_PID" 2>/dev/null; then
    wait "$WORKER_PID"
else
    wait "$SERVER_PID"
fi
