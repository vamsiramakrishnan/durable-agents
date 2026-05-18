#!/usr/bin/env bash
# Boots a temporary Tape server (in-memory SQLite), exports TAPE_URL, runs the
# provided command, then tears the server down. Used by `make quickstart-*`.
#
# Usage:  ./scripts/quickstart-run.sh <language-tag> "<command...>"

set -euo pipefail

LANG_TAG="${1:?usage: $0 <language-tag> <command>}"; shift
CMD="${1:?usage: $0 <language-tag> <command>}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER_BIN="$ROOT/tape/server/target/release/tape-server"
[[ -x "$SERVER_BIN" ]] || SERVER_BIN="$ROOT/tape/server/target/debug/tape-server"

if [[ ! -x "$SERVER_BIN" ]]; then
    echo "tape-server not built. Run: make build-server" >&2
    exit 1
fi

# Pick a free port.
PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
export TAPE_URL="tape://127.0.0.1:${PORT}"

# Suppress server output unless the user asks for it via RUST_LOG.
LOG_LEVEL="${RUST_LOG:-tape_server=warn}"
RUST_LOG="$LOG_LEVEL" "$SERVER_BIN" --listen "127.0.0.1:${PORT}" --store memory \
    >/tmp/tape-quickstart-${LANG_TAG}.log 2>&1 &
SERVER_PID=$!

cleanup() { kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

# Wait for the server to accept connections.
for _ in $(seq 1 50); do
    if python3 -c "import socket; s=socket.socket(); s.settimeout(0.2); s.connect(('127.0.0.1', ${PORT}))" 2>/dev/null; then
        break
    fi
    sleep 0.1
done

# Run the quickstart.
cd "$ROOT"
echo ""
eval "$CMD"
rc=$?
echo ""
exit "$rc"
