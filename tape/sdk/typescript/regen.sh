#!/usr/bin/env bash
# Generate the TypeScript gRPC client from ../../proto/tape.proto.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
proto="$here/../../proto/tape.proto"
out="$here/src/_gen"
mkdir -p "$out"
# Option A — connect-es / protobuf-es:
#   npx buf generate  (with a buf.gen.yaml using protoc-gen-es + protoc-gen-connect-es)
# Option B — grpc-tools:
#   npx grpc_tools_node_protoc -I"$here/../../proto" \
#     --js_out=import_style=commonjs,binary:"$out" \
#     --grpc_out=grpc_js:"$out" "$proto"
echo "see the commented commands in this script; pick the toolchain your project uses." >&2
exit 1
