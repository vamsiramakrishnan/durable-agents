#!/usr/bin/env bash
# Generate the Go gRPC client from ../../proto/tape.proto.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$here/tapepb"
protoc -I"$here/../../proto" \
  --go_out="$here/tapepb" --go_opt=paths=source_relative \
  --go-grpc_out="$here/tapepb" --go-grpc_opt=paths=source_relative \
  "$here/../../proto/tape.proto"
echo "generated -> $here/tapepb"
