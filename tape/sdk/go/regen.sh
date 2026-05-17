#!/usr/bin/env bash
# Regenerate the Go gRPC client from tape.proto. Needs protoc on PATH plus:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
# (Or set PATH=/tmp/gobin:$PATH for the locally-installed copies.)
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
cd "$here"
mkdir -p tapepb
# The `Mtape.proto=./tapepb` opts set the generated Go package to `tapepb`
# (newer protoc-gen-go requires either `option go_package` in the .proto or
# an `M`-mapping on the command line — we use the latter to keep the .proto
# tidy and the same across SDKs).
protoc -I. \
  --go_out=. --go_opt=paths=source_relative --go_opt=Mtape.proto=./tapepb \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative --go-grpc_opt=Mtape.proto=./tapepb \
  tape.proto
mv tape.pb.go tape_grpc.pb.go tapepb/ 2>/dev/null || true
echo "regenerated -> $here/tapepb"
