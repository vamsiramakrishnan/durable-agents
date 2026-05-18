#!/usr/bin/env bash
# Tape — One-command setup.
#
# Installs mise, every pinned toolchain (rust, python, node, go, java, just),
# builds the Rust server, and editable-installs the Python SDK + CLI. After
# this you can `make demo`, `tape dev`, or `make sdk-test-all`.
#
# Usage:
#   ./setup.sh                  # full bootstrap
#   ./setup.sh --skip-build     # tools only (no cargo build / pip install)
#   ./setup.sh --minimal        # rust + python only (skip ts/go/java toolchains)
#
# Everything after this is `make ...` or `tape ...`.

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; BLUE='\033[0;34m'; YELLOW='\033[0;33m'; NC='\033[0m'
info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKIP_BUILD=0
MINIMAL=0
for arg in "$@"; do
    case "$arg" in
        --skip-build) SKIP_BUILD=1 ;;
        --minimal)    MINIMAL=1 ;;
        -h|--help)
            sed -n '2,12p' "$0" | sed 's/^# //; s/^#//'
            exit 0 ;;
    esac
done

# ─── Step 1: mise ───────────────────────────────────────────
install_mise() {
    if command -v mise &>/dev/null; then
        ok "mise $(mise --version)"
        return
    fi
    if [[ -x "$HOME/.local/bin/mise" ]]; then
        export PATH="$HOME/.local/bin:$PATH"
        ok "mise $(mise --version)"
        return
    fi
    info "Installing mise (tool version manager) from https://mise.run..."
    curl -fsSL https://mise.run | sh
    export PATH="$HOME/.local/bin:$PATH"
    ok "mise $(mise --version) installed"

    local shell_rc="$HOME/.bashrc"
    [[ "${SHELL:-}" == */zsh ]] && shell_rc="$HOME/.zshrc"
    if [[ -w "$shell_rc" ]] && ! grep -q 'mise activate' "$shell_rc" 2>/dev/null; then
        echo 'eval "$(~/.local/bin/mise activate bash)"' >> "$shell_rc"
        info "Added mise activation to $shell_rc"
    fi
}

# ─── Step 2: Tools from .mise.toml ─────────────────────────
install_tools() {
    if [[ ! -f "$SCRIPT_DIR/.mise.toml" ]]; then
        err "No .mise.toml found in $SCRIPT_DIR"
        exit 1
    fi
    mise trust "$SCRIPT_DIR/.mise.toml" 2>/dev/null || true
    eval "$(mise activate bash)" 2>/dev/null || true

    if (( MINIMAL )); then
        info "Installing minimal toolchain (rust + python + just)..."
        (cd "$SCRIPT_DIR" && mise install --yes rust python just)
    else
        info "Installing every toolchain from .mise.toml..."
        (cd "$SCRIPT_DIR" && mise install --yes)
    fi
    ok "Toolchains installed"
    (cd "$SCRIPT_DIR" && mise ls --current 2>/dev/null) | sed 's/^/       /'
}

# ─── Step 3: Build server + install Python SDK + CLI ──────
build_and_install() {
    if (( SKIP_BUILD )); then
        warn "Skipping cargo build + pip install (--skip-build)"
        return
    fi
    eval "$(mise activate bash)" 2>/dev/null || true

    info "Building tape-server (release; this is a one-time ~2–5 min build)..."
    (cd "$SCRIPT_DIR/tape/server" && cargo build --release)
    ok "tape-server built → tape/server/target/release/tape-server"

    info "Installing the Python SDK and CLI (editable)..."
    pip install --quiet --upgrade pip
    pip install --quiet -e "$SCRIPT_DIR/tape/sdk/python"
    pip install --quiet -e "$SCRIPT_DIR/tape/cli"
    ok "Python SDK + CLI installed"

    if command -v tape &>/dev/null; then
        ok "tape CLI ready → $(which tape)"
    else
        warn "'tape' CLI not on PATH — try a fresh shell or 'eval \"\$(mise activate bash)\"'"
    fi
}

# ─── Step 4: Friendly next-steps ──────────────────────────
next_steps() {
    echo ""
    ok "Setup complete."
    echo ""
    echo "  Next steps:"
    echo "    make demo            # treasury example end-to-end"
    echo "    make demo-resume     # kill mid-wire, recover, prove ONE wire"
    echo "    make sdk-test-all    # smoke-test every SDK"
    echo "    make docker-up       # Postgres-backed tape-server (scale-out)"
    echo ""
    echo "  CLI on-ramp:"
    echo "    tape init my-agent   # scaffold a new durable ADK agent"
    echo "    tape dev             # server + reactors + agent (sqlite)"
    echo "    tape doctor          # tick/cross diagnostic"
    echo ""
    echo "  Docs:    https://vamsiramakrishnan.github.io/durable-agents/"
    echo "  Parity:  ./SDK_PARITY.md"
    echo ""
}

# ─── Main ───────────────────────────────────────────────────
main() {
    echo ""
    echo "  ╔══════════════════════════════════════════════╗"
    echo "  ║   Tape — durable-execution for ADK agents    ║"
    echo "  ╚══════════════════════════════════════════════╝"
    echo ""
    install_mise
    install_tools
    build_and_install
    next_steps
}

main "$@"
