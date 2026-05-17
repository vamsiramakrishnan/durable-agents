#!/usr/bin/env bash
# Regenerate the Python gRPC stubs from ../../proto/tape.proto.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
proto_dir="$here/../../proto"
out_dir="$here/tape/_gen"
mkdir -p "$out_dir"
python -m grpc_tools.protoc -I"$proto_dir" \
  --python_out="$out_dir" --grpc_python_out="$out_dir" "$proto_dir/tape.proto"
# Make the cross-import package-relative.
sed -i 's/^import tape_pb2 as tape__pb2/from . import tape_pb2 as tape__pb2/' "$out_dir/tape_pb2_grpc.py"
touch "$out_dir/__init__.py"
echo "regenerated -> $out_dir"
