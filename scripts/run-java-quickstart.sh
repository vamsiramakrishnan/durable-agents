#!/usr/bin/env bash
# Compile + run examples/QuickstartJava.java against the Java SDK's built jar.
# Driven by `make quickstart-java`, which boots a tmp Tape server first and
# exports TAPE_URL.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAVA_SDK="$ROOT/tape/sdk/java"
CP_FILE="$JAVA_SDK/target/cp.txt"
CLASSES="$JAVA_SDK/target/classes"

[[ -f "$CP_FILE" && -d "$CLASSES" ]] || {
    echo "Java SDK not built. Run: make build-java" >&2
    exit 1
}

CP="$(cat "$CP_FILE"):$CLASSES"

cd "$ROOT/examples"
javac -cp "$CP" QuickstartJava.java
java -cp ".:$CP" QuickstartJava
