#!/usr/bin/env bash
# scripts/doctor.sh — tick/cross diagnostic for a fresh Tape clone.
#
# Run via:  make doctor
#
# Checks (each prints OK / FAIL / SKIP):
#   1. Toolchain — mise + the pinned versions of rust, python, node, go, java
#   2. Build artefacts — tape-server binary, Python SDK editable-install
#   3. Server reachability — TAPE_URL accepts a gRPC channel within 2 s
#   4. SDK round-trip — Python BeginRun → EndRun against the live server
#
# Exits 0 if every required check passes, 1 if any required check fails. SKIPs
# (e.g. server not running) are not failures.

set -uo pipefail

# ─── colors ──────────────────────────────────────────────────────────────────

if [[ -t 1 ]]; then
    BOLD=$'\033[1m'; DIM=$'\033[2m'; NC=$'\033[0m'
    GREEN=$'\033[0;32m'; RED=$'\033[0;31m'; YELLOW=$'\033[0;33m'; BLUE=$'\033[0;34m'
else
    BOLD=''; DIM=''; NC=''; GREEN=''; RED=''; YELLOW=''; BLUE=''
fi

PASSES=0
FAILS=0
SKIPS=0

ok()   { printf "  ${GREEN}✓${NC} %-28s %s\n" "$1" "${2:-}"; PASSES=$((PASSES+1)); }
fail() { printf "  ${RED}✗${NC} %-28s %s\n" "$1" "${2:-}"; FAILS=$((FAILS+1)); }
skip() { printf "  ${YELLOW}~${NC} %-28s %s\n" "$1" "${2:-}"; SKIPS=$((SKIPS+1)); }
hdr()  { printf "\n${BOLD}%s${NC}\n" "$1"; }

# ─── paths ───────────────────────────────────────────────────────────────────

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER_BIN_DEBUG="$ROOT/tape/server/target/debug/tape-server"
SERVER_BIN_RELEASE="$ROOT/tape/server/target/release/tape-server"
SDK_PY="$ROOT/tape/sdk/python"
TAPE_URL="${TAPE_URL:-tape://127.0.0.1:7878}"

# ─── 1. Toolchain ────────────────────────────────────────────────────────────

hdr "Toolchain"

if command -v mise >/dev/null 2>&1; then
    ok "mise"                "$(mise --version)"
else
    fail "mise"              "not installed — run ./setup.sh"
fi

for tool in rustc python node go java just; do
    if command -v "$tool" >/dev/null 2>&1; then
        case "$tool" in
            rustc)  v=$($tool --version 2>&1 | awk '{print $2}') ;;
            python) v=$($tool --version 2>&1 | awk '{print $2}') ;;
            node)   v=$($tool --version 2>&1 | tr -d 'v') ;;
            go)     v=$($tool version 2>&1 | awk '{print $3}' | tr -d 'go') ;;
            java)   v=$($tool -version 2>&1 | head -n1 | sed 's/.*"\(.*\)".*/\1/') ;;
            just)   v=$($tool --version 2>&1 | awk '{print $2}') ;;
        esac
        ok "$tool"           "$v"
    else
        fail "$tool"         "not on PATH"
    fi
done

# ─── 2. Build artefacts ──────────────────────────────────────────────────────

hdr "Build artefacts"

if [[ -x "$SERVER_BIN_RELEASE" ]]; then
    ok "tape-server (release)" "$SERVER_BIN_RELEASE"
elif [[ -x "$SERVER_BIN_DEBUG" ]]; then
    ok "tape-server (debug)"   "$SERVER_BIN_DEBUG"
else
    fail "tape-server"        "not built — run: make build-server"
fi

if [[ -f "$SDK_PY/pyproject.toml" ]]; then
    if PYTHONPATH="$SDK_PY" python -c "import tape" 2>/dev/null; then
        ok "tape-py (importable)" "via PYTHONPATH"
    elif python -c "import tape" 2>/dev/null; then
        ok "tape-py (installed)"
    else
        fail "tape-py"        "not importable — run: pip install -e $SDK_PY"
    fi
else
    fail "tape-py"            "missing pyproject.toml"
fi

if command -v tape >/dev/null 2>&1; then
    ok "tape CLI"             "$(which tape)"
else
    skip "tape CLI"           "not on PATH (optional — run: pip install -e tape/cli)"
fi

# ─── 3. Server reachability ──────────────────────────────────────────────────

hdr "Server reachability ($TAPE_URL)"

host="${TAPE_URL#*://}"
host="${host%%/*}"
hostname="${host%:*}"
port="${host##*:}"
[[ "$port" == "$host" ]] && port="7878"

if command -v nc >/dev/null 2>&1; then
    if nc -z -w 2 "$hostname" "$port" 2>/dev/null; then
        ok "tcp ${hostname}:${port}"  "reachable"
        SERVER_REACHABLE=1
    else
        skip "tcp ${hostname}:${port}" "not reachable (run: make serve  or  make docker-up)"
        SERVER_REACHABLE=0
    fi
elif command -v python >/dev/null 2>&1; then
    if python -c "import socket,sys; s=socket.socket(); s.settimeout(2); s.connect(('$hostname',int('$port'))); s.close()" 2>/dev/null; then
        ok "tcp ${hostname}:${port}"  "reachable"
        SERVER_REACHABLE=1
    else
        skip "tcp ${hostname}:${port}" "not reachable (run: make serve  or  make docker-up)"
        SERVER_REACHABLE=0
    fi
else
    skip "tcp ${hostname}:${port}" "no nc / python on PATH to probe"
    SERVER_REACHABLE=0
fi

# ─── 4. SDK round-trip ───────────────────────────────────────────────────────

hdr "SDK round-trip"

if [[ "${SERVER_REACHABLE:-0}" -eq 1 ]]; then
    if PYTHONPATH="$SDK_PY" python - <<EOF >/dev/null 2>&1
import os, sys
from tape.client import TapeClient
url = os.environ.get("TAPE_URL", "$TAPE_URL")
with TapeClient(url) as c:
    r = c.begin_run(app_name="doctor", user_id="doctor", session_id="doctor",
                    invocation_id="doctor-1", lease_owner="doctor", lease_ttl_ms=10_000)
    c.end_run(run_id=r.run_id)
    print(r.run_id)
EOF
    then
        ok "python begin_run/end_run"  "OK"
    else
        fail "python begin_run/end_run" "failed — see RUST_LOG output from the server"
    fi
else
    skip "python begin_run/end_run" "server not reachable"
fi

# ─── summary ─────────────────────────────────────────────────────────────────

printf "\n${BOLD}Summary${NC}  ${GREEN}%d ok${NC}  ${RED}%d fail${NC}  ${YELLOW}%d skip${NC}\n" \
    "$PASSES" "$FAILS" "$SKIPS"

if [[ "$FAILS" -gt 0 ]]; then
    printf "\nNext step:  fix the ${RED}✗${NC} rows. If the toolchain is missing, ${BOLD}./setup.sh${NC}.\n"
    exit 1
fi
if [[ "$SKIPS" -gt 0 ]]; then
    printf "\nNext step:  ${BOLD}make serve${NC} or ${BOLD}make docker-up${NC}, then re-run ${BOLD}make doctor${NC}.\n"
fi
exit 0
