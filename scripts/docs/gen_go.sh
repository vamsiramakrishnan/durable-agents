#!/usr/bin/env bash
# Generate the Go package reference using gomarkdoc.
#
# Output: tape/docs/reference/go/api.md (single page).
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
out="$repo/tape/docs/reference/go/api.md"

if ! command -v gomarkdoc >/dev/null 2>&1; then
  echo "Installing gomarkdoc into ${GOBIN:-$HOME/go/bin}..."
  GOBIN="${GOBIN:-$HOME/go/bin}" go install github.com/princjef/gomarkdoc/cmd/gomarkdoc@latest
  export PATH="${GOBIN:-$HOME/go/bin}:$PATH"
fi

cd "$repo/tape/sdk/go"

# Header for the generated page.
cat > "$out" <<'HEADER'
# Go package reference

!!! info "Generated"
    Generated from godoc by `gomarkdoc`. Edits here will be overwritten — to change the
    content, edit the SDK's godoc comments and run `scripts/docs/gen_go.sh`.

HEADER

# Generate per-package and concatenate. Skip the generated protobuf
# package (./tapepb is mechanical re-export of proto/tape.proto) and the
# CLI binary package (./cmd/tape-outbox is documented in
# how-to/outbox-daemon.md, not the API reference).
for pkg in . ./connectors ./sinks; do
  echo "--> gomarkdoc $pkg"
  gomarkdoc --format github "$pkg" >> "$out"
  echo "" >> "$out"
done

echo "wrote $out"
