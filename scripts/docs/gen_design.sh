#!/usr/bin/env bash
# Mirror design-principles/*.md into tape/docs/design/, rewriting relative
# image paths so they resolve under the docs site.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
src="$repo/design-principles"
dst="$repo/tape/docs/design"
mkdir -p "$dst"

# Copy every .md from design-principles. We deliberately don't follow image
# references — design-principles/figures/ is mirrored separately.
for f in "$src"/*.md; do
  name="$(basename "$f")"
  cp "$f" "$dst/$name"
done

# Mirror sibling assets (figures, SVGs, PNGs) so image references resolve.
for ext in svg png jpg jpeg gif webp; do
  for f in "$src"/*."$ext"; do
    [[ -e "$f" ]] || continue
    cp "$f" "$dst/"
  done
done
if [[ -d "$src/figures" ]]; then
  rm -rf "$dst/figures"
  cp -r "$src/figures" "$dst/figures"
fi

echo "design pages mirrored: $(ls "$dst"/*.md | wc -l | tr -d ' ')"
echo "design assets:        $(ls "$dst" | grep -vE '\.md$' | wc -l | tr -d ' ')"
