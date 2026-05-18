# Tape — durable-execution substrate for ADK agents.
#
# Common entry points (every target also has `make help` documentation):
#
#   make setup          One-command bootstrap: mise + tools + build + SDKs + CLI
#   make dev            Start tape-server on localhost (sqlite, foreground)
#   make demo           Run the treasury example end-to-end against a fresh server
#   make demo-resume    Kill mid-wire, recover, prove ONE wire — the punchline
#   make test           Rust unit tests + Python kill-and-resume integration test
#   make sdk-test-all   Run every SDK's smoke test (python, ts, go, java)
#   make docs           Build the docs site (mkdocs + per-lang reference)
#   make docker-up      docker compose up (Postgres + tape-server)
#
# Every recipe documents itself for `make help`. Add `## <text>` to expose.

.PHONY: help \
        setup install deps \
        build build-server build-python build-ts build-go build-java \
        test test-server test-python \
        sdk-test-all sdk-test-python sdk-test-ts sdk-test-go sdk-test-java \
        dev serve demo demo-resume \
        docker-up docker-down docker-clean \
        docs docs-serve docs-clean \
        clean release-dry

# ─── Config ─────────────────────────────────────────────────────

SHELL        := /bin/bash
SERVER_DIR   := tape/server
SDK_PYTHON   := tape/sdk/python
SDK_TS       := tape/sdk/typescript
SDK_GO       := tape/sdk/go
SDK_JAVA     := tape/sdk/java
CLI_DIR      := tape/cli
EXAMPLES_DIR := tape/examples
TESTS_DIR    := tape/tests

TAPE_LISTEN  ?= 127.0.0.1:7878
TAPE_URL     ?= tape://127.0.0.1:7878
TAPE_STORE   ?= sqlite:./tape.db

# ─── Help ───────────────────────────────────────────────────────

help: ## Show this help
	@printf "\n  \033[1mTape\033[0m — durable-execution substrate for ADK agents\n\n"
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
	    awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@printf "\nFirst-time setup:  \033[1m./setup.sh\033[0m   (or:  make setup)\n"
	@printf "Quickstart docs:   https://vamsiramakrishnan.github.io/durable-agents/\n\n"

# ─── Setup ──────────────────────────────────────────────────────

setup: ## One-command bootstrap (mise + tools + build + SDKs + CLI)
	@chmod +x setup.sh && ./setup.sh

deps: ## Show installed tool versions (run `make setup` if missing)
	@command -v mise >/dev/null 2>&1 && mise ls --current || \
	    echo "mise not installed. Run: make setup"

install: build-server build-python ## Build server + install Python SDK and CLI editable
	pip install -e $(SDK_PYTHON)
	pip install -e $(CLI_DIR)

# ─── Build ──────────────────────────────────────────────────────

build: build-server build-python build-ts build-go build-java ## Build every SDK + the server

build-server: ## Cargo build the Rust tape-server (release)
	cd $(SERVER_DIR) && cargo build --release

build-python: ## Editable-install the Python SDK + CLI
	pip install -e $(SDK_PYTHON)
	pip install -e $(CLI_DIR)

build-ts: ## npm install + tsc for the TypeScript SDK
	cd $(SDK_TS) && npm install && npm run build

build-go: ## go build for the Go SDK
	cd $(SDK_GO) && go build ./...

build-java: ## mvn package for the Java SDK (skips tests; use sdk-test-java)
	cd $(SDK_JAVA) && mvn -q -DskipTests package

# ─── Test ───────────────────────────────────────────────────────

test: test-server test-python ## Rust unit tests + Python integration tests (kill-and-resume)

test-server: ## cargo test for the Rust server
	cd $(SERVER_DIR) && cargo test

test-python: build-server ## pytest the kill-and-resume + features integration suites
	cd $(SERVER_DIR) && cargo build  # the tests use the debug binary
	python -m pytest $(TESTS_DIR) -q

sdk-test-all: sdk-test-python sdk-test-ts sdk-test-go sdk-test-java ## Run every SDK's smoke test

sdk-parity: build-server build-python ## Cross-SDK outbox parity (drives the same scenario through Python/TS/Go/Java)
	cd $(SDK_TS) && npm install --silent
	PYTHONPATH=$(SDK_PYTHON) python -m pytest $(TESTS_DIR)/parity -v

sdk-test-python: ## Python SDK round-trip tests
	cd $(SDK_PYTHON) && python -m pytest -q || \
	    echo "(pytest may need: pip install -e tape/sdk/python[dev])"

sdk-test-ts: ## TypeScript SDK smoke test (spawns tape-server in-memory)
	cd $(SDK_TS) && npm install --silent && npm test

sdk-test-go: ## Go SDK round-trip tests
	cd $(SDK_GO) && go test -timeout 60s ./...

sdk-test-java: ## Java SDK smoke test (mvn test)
	cd $(SDK_JAVA) && mvn -q test

# ─── Run ────────────────────────────────────────────────────────

dev: serve ## Alias for `make serve` — start tape-server in foreground

serve: build-server ## Start tape-server on $(TAPE_LISTEN) with $(TAPE_STORE)
	./$(SERVER_DIR)/target/release/tape-server \
	    --listen $(TAPE_LISTEN) --store $(TAPE_STORE)

demo: ## Run the treasury example end-to-end against a fresh local server
	cd tape && just demo

demo-resume: ## Kill mid-wire, recover, prove ONE wire — the punchline demo
	cd tape && just demo-resume

# ─── Docker Compose ─────────────────────────────────────────────

docker-up: ## docker compose up (Postgres + tape-server)
	docker compose up -d --build
	@echo ""
	@echo "Services running:"
	@echo "  tape-server: localhost:7878  (gRPC)"
	@echo "  postgres:    localhost:5432  (tape/tape)"
	@echo ""
	@echo "Point the SDK at:  TAPE_URL=tape://localhost:7878"

docker-down: ## docker compose down
	docker compose down

docker-clean: ## docker compose down -v (also removes the Postgres volume)
	docker compose down -v

# ─── Docs ───────────────────────────────────────────────────────

docs: ## Build the docs site (mkdocs + per-language reference)
	pip install -r docs/requirements.txt
	pip install -e $(SDK_PYTHON) -e $(CLI_DIR)
	bash scripts/docs/gen_all.sh
	mkdocs build --site-dir ./site

docs-serve: ## Serve the docs site at http://127.0.0.1:8000
	pip install -r docs/requirements.txt
	pip install -e $(SDK_PYTHON) -e $(CLI_DIR)
	bash scripts/docs/gen_all.sh
	mkdocs serve

docs-clean: ## Remove generated docs site
	rm -rf site/

# ─── Release ────────────────────────────────────────────────────

release-dry: ## Cross-build tape-server binaries locally (no publish)
	@echo "Building tape-server for current platform (release)..."
	cd $(SERVER_DIR) && cargo build --release
	@echo "For cross-platform release artifacts, push a tag and let .github/workflows/release.yml build them."

# ─── Clean ──────────────────────────────────────────────────────

clean: ## Remove every build artifact
	cd $(SERVER_DIR) && cargo clean
	rm -rf $(SDK_TS)/node_modules $(SDK_TS)/dist
	rm -rf $(SDK_JAVA)/target $(SDK_JAVA)/.mvn
	find $(SDK_GO) -name 'bin' -type d -prune -exec rm -rf {} +
	rm -rf site/ ./tape.db ./tape.db-wal ./tape.db-shm
