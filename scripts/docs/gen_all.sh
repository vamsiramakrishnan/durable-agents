#!/usr/bin/env bash
# Generate everything the docs site needs that isn't handwritten.
# Idempotent — safe to run repeatedly.
#
#   scripts/docs/gen_all.sh
#
# Per-step scripts live alongside this one; run them individually for faster
# iteration on a single language.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/../.." && pwd)"

echo "==> mirroring design-principles into docs/design"
"$here/gen_design.sh"

echo "==> generating Go package reference (gomarkdoc)"
# gomarkdoc is installed by gen_go.sh into $HOME/go/bin if missing; make sure
# that path is visible to subsequent commands in this run.
export PATH="${GOBIN:-$HOME/go/bin}:$PATH"
"$here/gen_go.sh"

echo "==> generating TypeScript package reference (typedoc)"
"$here/gen_typescript.sh"

echo "==> generating Java javadoc (mvn javadoc:javadoc)"
"$here/gen_java.sh"

echo "==> generating CLI reference (typer introspection)"
"$here/gen_cli.py"

echo "==> docs ready under $repo/tape/docs/"
