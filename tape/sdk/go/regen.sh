#!/usr/bin/env bash
# Regenerate the Go gRPC client from tape.proto. Needs protoc on PATH plus:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
# (Or set PATH=/tmp/gobin:$PATH for the locally-installed copies.)
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
cd "$here"
mkdir -p tapepb
protoc -I. -I../../proto \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  tape.proto
mv tape.pb.go tape_grpc.pb.go tapepb/ 2>/dev/null || true
echo "regenerated -> $here/tapepb"
