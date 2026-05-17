#!/usr/bin/env bash
# Generate the TypeScript package reference using typedoc + typedoc-plugin-markdown.
#
# Output: tape/docs/reference/typescript/api.md (single page).
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
out_dir="$repo/tape/docs/reference/typescript"
out_file="$out_dir/api.md"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

cd "$repo/tape/sdk/typescript"

# Ensure deps + typedoc are installed locally (idempotent).
npm install --silent --no-fund --no-audit
npm install --silent --no-save --no-fund --no-audit typedoc typedoc-plugin-markdown

# Run typedoc in single-file mode.
npx --no-install typedoc \
  --plugin typedoc-plugin-markdown \
  --out "$tmp/typedoc" \
  --hideBreadcrumbs \
  --hidePageHeader \
  --readme none \
  --entryFileName api.md \
  --entryPoints src/index.ts \
  --tsconfig tsconfig.json

# TypeDoc emits a tree (index + classes/ + interfaces/ + functions/ + ...).
# Replace the existing tree wholesale and prepend our generated-note header.
rm -rf "$out_dir/classes" "$out_dir/interfaces" "$out_dir/functions" \
       "$out_dir/types" "$out_dir/type-aliases" "$out_dir/variables" \
       "$out_dir/enumerations" "$out_dir/modules"

# Copy the per-symbol pages verbatim.
for sub in classes interfaces functions types type-aliases variables enumerations modules; do
  if [[ -d "$tmp/typedoc/$sub" ]]; then
    cp -r "$tmp/typedoc/$sub" "$out_dir/$sub"
  fi
done

# Build the top-level api.md from the generated index + our header.
cat > "$out_file" <<'HEADER'
# TypeScript package reference

!!! info "Generated"
    Generated from TSDoc by `typedoc --plugin typedoc-plugin-markdown`. Edits here will be
    overwritten — change the TSDoc comments and re-run `scripts/docs/gen_typescript.sh`.

HEADER

if [[ -f "$tmp/typedoc/api.md" ]]; then
  # Drop typedoc's own `# tape-ts` heading; ours replaces it.
  sed '1,/^# /d' "$tmp/typedoc/api.md" >> "$out_file"
elif [[ -f "$tmp/typedoc/README.md" ]]; then
  sed '1,/^# /d' "$tmp/typedoc/README.md" >> "$out_file"
fi

echo "wrote $out_file (+ classes/, interfaces/, functions/, ...)"
