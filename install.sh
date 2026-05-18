#!/usr/bin/env bash
# Tape — Binary installer (downloads pre-built tape-server from GitHub Releases
# and pip-installs the Python SDK + CLI).
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/vamsiramakrishnan/durable-agents/main/install.sh | sh
#   curl -fsSL ... | sh -s -- --version v0.1.0          # pin a version
#   curl -fsSL ... | sh -s -- --no-cli                  # server binary only
#   curl -fsSL ... | sh -s -- --no-server               # Python SDK + CLI only
#
# Installs to:
#   ~/.local/bin/tape-server   (compiled Rust server)
#   tape-py, tape-cli          (via pip — `tape`, `tape-reactors`, `tape-outbox`)

set -euo pipefail

REPO="vamsiramakrishnan/durable-agents"
INSTALL_DIR="${TAPE_INSTALL_DIR:-$HOME/.local/bin}"
VERSION="latest"
INSTALL_SERVER=1
INSTALL_CLI=1

RED='\033[0;31m'; GREEN='\033[0;32m'; BLUE='\033[0;34m'; YELLOW='\033[0;33m'; NC='\033[0m'
info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --version)    VERSION="$2"; shift 2 ;;
        --no-cli)     INSTALL_CLI=0; shift ;;
        --no-server)  INSTALL_SERVER=0; shift ;;
        -h|--help)
            sed -n '2,14p' "$0" | sed 's/^# //; s/^#//'
            exit 0 ;;
        *) err "Unknown flag: $1" ;;
    esac
done

# ─── Detect platform ───────────────────────────────────────
detect_platform() {
    OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
    ARCH="$(uname -m)"
    case "$ARCH" in
        x86_64)         ARCH="x86_64" ;;
        aarch64|arm64)  ARCH="aarch64" ;;
        *) err "Unsupported architecture: $ARCH" ;;
    esac
    case "$OS" in
        linux)         TARGET="${ARCH}-unknown-linux-gnu" ;;
        darwin)        TARGET="${ARCH}-apple-darwin" ;;
        mingw*|msys*|cygwin*)
                       TARGET="${ARCH}-pc-windows-msvc"; OS="windows" ;;
        *) err "Unsupported OS: $OS" ;;
    esac
}

resolve_version() {
    if [[ "$VERSION" == "latest" ]]; then
        VERSION=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" 2>/dev/null \
            | grep '"tag_name"' | head -n1 | cut -d'"' -f4 || true)
        if [[ -z "$VERSION" ]]; then
            no_release_fallback
        fi
    fi
}

no_release_fallback() {
    cat <<EOF >&2
${YELLOW}[!]${NC}  No published GitHub Release for ${REPO} yet.

    The curl-pipe installer fetches prebuilt binaries; until the maintainer
    cuts a tag, the install path is to build from source from a clone:

        git clone https://github.com/${REPO}
        cd durable-agents
        ./setup.sh && make demo

    Or pin a specific version explicitly when one exists:

        curl -fsSL https://raw.githubusercontent.com/${REPO}/main/install.sh \\
            | sh -s -- --version v0.1.0

EOF
    exit 0
}

# ─── Server binary ─────────────────────────────────────────
install_server() {
    (( INSTALL_SERVER )) || return 0

    local ext="tar.gz"
    [[ "$OS" == "windows" ]] && ext="zip"

    local filename="tape-server_${VERSION#v}_${TARGET}.${ext}"
    local url="https://github.com/${REPO}/releases/download/${VERSION}/${filename}"

    info "Downloading tape-server ${VERSION} for ${TARGET}..."
    local tmpdir
    tmpdir="$(mktemp -d)"
    trap 'rm -rf "$tmpdir"' EXIT
    curl -fsSL "$url" -o "${tmpdir}/${filename}" \
        || err "Download failed (${url}). Check the version: --version v0.1.0"

    if [[ "$ext" == "zip" ]]; then
        unzip -o "${tmpdir}/${filename}" -d "$tmpdir" >/dev/null
    else
        tar -xzf "${tmpdir}/${filename}" -C "$tmpdir"
    fi

    mkdir -p "$INSTALL_DIR"
    mv "${tmpdir}/tape-server" "${INSTALL_DIR}/tape-server"
    chmod +x "${INSTALL_DIR}/tape-server"
    ok "tape-server ${VERSION} → ${INSTALL_DIR}/tape-server"
}

# ─── Python CLI ────────────────────────────────────────────
install_cli() {
    (( INSTALL_CLI )) || return 0

    if ! command -v pip &>/dev/null && ! command -v pip3 &>/dev/null; then
        warn "Python's pip not found. Install Python 3.10+ and rerun:"
        warn "  pip install tape-py tape-cli"
        return
    fi
    local PIP="pip"; command -v pip &>/dev/null || PIP="pip3"

    info "Installing tape-py + tape-cli via ${PIP}..."
    if "$PIP" install --quiet --user tape-py tape-cli 2>/dev/null; then
        ok "tape CLI installed (PyPI)"
    else
        warn "PyPI install failed (packages may not be published yet)."
        warn "From a clone:  pip install -e tape/sdk/python -e tape/cli"
    fi
}

# ─── PATH check ───────────────────────────────────────────
check_path() {
    if [[ ":$PATH:" != *":${INSTALL_DIR}:"* ]]; then
        echo ""
        info "Add to your PATH:"
        echo "  export PATH=\"${INSTALL_DIR}:\$PATH\""
        local shell_rc="$HOME/.bashrc"
        [[ "${SHELL:-}" == */zsh ]] && shell_rc="$HOME/.zshrc"
        info "Or permanently:"
        echo "  echo 'export PATH=\"${INSTALL_DIR}:\$PATH\"' >> ${shell_rc}"
    fi
}

# ─── Main ─────────────────────────────────────────────────
main() {
    echo ""
    echo "  Tape installer"
    echo ""
    detect_platform
    resolve_version
    install_server
    install_cli
    check_path

    echo ""
    info "Get started:"
    echo "  tape-server --listen 127.0.0.1:7878 --store sqlite:./tape.db &"
    echo "  tape init my-agent          # scaffold a durable ADK agent"
    echo "  tape dev                    # server + reactors + agent (sqlite)"
    echo ""
    echo "  Or, from a clone:           ./setup.sh && make demo"
    echo "  Docs:                       https://vamsiramakrishnan.github.io/durable-agents/"
    echo ""
}

main "$@"
