#!/usr/bin/env bash
# Generate Javadoc HTML and copy it under tape/docs/reference/java/javadoc/.
#
# Javadoc HTML is served as a static sibling — it doesn't reflow into the
# Material theme, but it's the canonical view Java developers expect.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
dst="$repo/tape/docs/reference/java/javadoc"

cd "$repo/tape/sdk/java"

mvn -q -DskipTests javadoc:javadoc

src="target/site/apidocs"
if [[ ! -d "$src" ]]; then
  echo "no javadoc output at $src" >&2
  exit 1
fi

rm -rf "$dst"
mkdir -p "$dst"
cp -r "$src/." "$dst/"

echo "wrote $dst (entry: $dst/index.html)"
