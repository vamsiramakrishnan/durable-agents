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

# `-Xdoclint:none` silences the doclint warnings from generated protobuf code
# (no @return / @param on auto-generated builders) which would otherwise be
# 100+ warnings per build. `-quiet` keeps the log readable. `-Dadditional
# JOption=...` is the Maven Javadoc plugin's pass-through to the javadoc tool.
#
# Newer maven-javadoc-plugin versions output to target/reports/apidocs;
# older ones use target/site/apidocs. We check both.
mvn -q -DskipTests \
    -Dadditional.option="-Xdoclint:none -quiet" \
    -Dadditionalparam="-Xdoclint:none -quiet" \
    -Dmaven.javadoc.failOnError=true \
    -Dmaven.javadoc.failOnWarnings=false \
    javadoc:javadoc

src=""
for cand in target/reports/apidocs target/site/apidocs; do
  if [[ -d "$cand" ]]; then src="$cand"; break; fi
done
if [[ -z "$src" ]]; then
  echo "no javadoc output found under target/{reports,site}/apidocs" >&2
  exit 1
fi

rm -rf "$dst"
mkdir -p "$dst"
cp -r "$src/." "$dst/"

echo "wrote $dst (entry: $dst/index.html)"
