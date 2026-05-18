# Run the outbox dispatcher in any language

The outbox reactor is the loop that drains PENDING + OUTBOX effects through
their registered connectors. Python has shipped it from day one as
`python -m tape.reactors.outbox`. The same loop now ships as a packaged daemon
in TypeScript, Go, and Java with the same flags and the same safety contract.

The dispatch loop is identical across languages:

```
list effects to dispatch (PENDING + OUTBOX + due)
for each:
  claim (atomic CAS lease)
  look up the connector
  dispatch through it
  record result:
    confirmed   → CompleteEffect(CONFIRMED)
    failed      → record_dispatch_attempt(next_at=backoff)  (eventually FAILED)
    unknown     → record_dispatch_attempt(next_at=0)        (status UNKNOWN —
                                                              the reconciler
                                                              resolves; never
                                                              blindly retry)
```

The `non_idempotent` safety contract is enforced both by every SDK at
construction time *and* by the server's CAS on `claim_effect_dispatch`. Even
an older SDK can't slip a blind retry through.

## CLIs

Each language ships a CLI with the same flag set:

| Flag                            | Default                  | What                                                          |
|---------------------------------|--------------------------|---------------------------------------------------------------|
| `--url`                         | `tape://localhost:7878`  | Tape server URL                                               |
| `--connector NAME`              | empty (all)              | Restrict to one connector name                                |
| `--interval MS`                 | `1000`                   | Poll interval                                                 |
| `--max-attempts N`              | `5`                      | Mark FAILED after N attempts                                  |
| `--claimer ID`                  | `<host>:<pid>`           | Identity recorded as `dispatch_claimed_by`                    |
| `--once`                        | off                      | Run one pass and exit (cron / tests)                          |
| `--register-log-connector`      | off                      | Register the built-in `LogConnector` (tests / demos)          |
| `--log-connector-path PATH`     | `/tmp/tape-outbox.jsonl` | Where the LogConnector writes                                 |

=== ":material-language-python: Python"

    ```bash
    python -m tape.reactors.outbox \
        --url tape://localhost:7878 \
        --connector bank.wire \
        --load my_app.connectors.bank
    ```

    Connectors are loaded by Python import path. `--load
    module:attr` imports the module so its top-level
    `tape.connectors.register(...)` runs.

=== ":material-language-typescript: TypeScript"

    ```bash
    cd tape/sdk/typescript
    npm run outbox -- \
        --url tape://localhost:7878 \
        --connector bank.wire \
        --load ./my_app/connectors/bank.ts
    ```

    Or the published binary after `npm install -g tape-ts`:

    ```bash
    tape-outbox-ts --url tape://localhost:7878 --once --register-log-connector
    ```

    `--load` is a repeatable flag that imports a TypeScript / JavaScript
    module (relative path or absolute path). The module's top-level
    `CONNECTORS.register(...)` call wires the connector.

=== ":simple-go: Go"

    ```bash
    cd tape/sdk/go
    go run ./cmd/tape-outbox \
        --url tape://localhost:7878 \
        --connector bank.wire
    ```

    Go connectors register via `connectors.Default.Register("bank.wire", ...)`
    in an `init()` of the binary that imports this dispatcher. For one-off
    operational use, `--register-log-connector` registers the built-in
    `LogConnector`.

=== ":fontawesome-brands-java: Java"

    ```bash
    cd tape/sdk/java
    mvn -q -DskipTests package
    mvn -q dependency:build-classpath -Dmdep.outputFile=target/cp.txt -Dmdep.includeScope=runtime
    java -cp "$(cat target/cp.txt):target/classes" dev.tape.cli.TapeOutbox \
        --url tape://localhost:7878 --once --register-log-connector
    ```

    Java connectors register via
    `ConnectorRegistry.DEFAULT.register("bank.wire", new MyConnector(...))`.
    For a fat-jar deployment, set the `Main-Class` to
    `dev.tape.cli.TapeOutbox` and bundle your connector classes in.

## Operating it

Each dispatcher is **idempotent**: the per-row lease + the server's CAS make
two racing dispatchers harmless (the loser short-circuits). Scale out with as
many copies as you have throughput:

```yaml
# kubernetes
apiVersion: apps/v1
kind: Deployment
metadata: { name: tape-outbox }
spec:
  replicas: 3
  template:
    spec:
      containers:
        - name: outbox
          image: ghcr.io/example/my-agent-outbox:latest
          args:
            - --url
            - tape://tape-server:7878
            - --connector
            - bank.wire
            - --max-attempts
            - "10"
```

Each replica claims its own work; effects that the loser couldn't claim are
picked up on the next tick by either replica.

## Backoff and the FAILED terminal

`--max-attempts` controls how many connector failures the dispatcher tolerates
before driving the effect to **terminal FAILED** via
`RecordExternalObservation(resolution=FAILED)`. Below that limit, each
`DispatchResult.outcome == "failed"` reschedules the next attempt:

```
next_at_ms = now + min(base * 2^attempt, 60s)        # exponential, capped at 60s
```

Connector-side `unknown` outcomes are *not* subject to this — they go straight
to `EffectStatus.UNKNOWN` and stay there until the reconciler resolves them.
This is the entire safety claim for non-idempotent upstreams.

## Local testing

The `--once` flag runs exactly one pass and exits, which is what the
[**cross-SDK parity harness**](cross-sdk-parity.md) uses to drive every
language through the same scenario:

```bash
# Spawn a tmp server, create a PENDING+OUTBOX effect, then:
python -m tape.reactors.outbox --url $TAPE_URL --once --load <module>
node --experimental-strip-types --no-warnings tape/sdk/typescript/bin/tape-outbox-ts.ts --url $TAPE_URL --once --register-log-connector
go run tape/sdk/go/cmd/tape-outbox --url $TAPE_URL --once --register-log-connector
java -cp ... dev.tape.cli.TapeOutbox --url $TAPE_URL --once --register-log-connector
```

`make sdk-parity` runs that exact loop and asserts the effect went `PENDING →
CONFIRMED` after every language ran a pass.

## See also

- [Effects & idempotency](../concepts/effects.md) — the contract the
  dispatcher honours.
- [UNKNOWN — the third outcome](../concepts/unknown.md) — why
  `record_dispatch_attempt(next_at=0)` is the safety exit, not a retry.
- [Reactors](../concepts/reactors.md) — the broader family of WAL-driven
  loops the outbox dispatcher belongs to.
- [Write a custom connector](custom-connector.md) — what the dispatcher
  calls.
- [Sinks](sinks.md) — the WAL fan-out side, also now in every language.
