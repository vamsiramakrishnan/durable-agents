## Summary

<!-- One paragraph. What does this change do, and why? -->

## Touches

<!-- Tick everything that applies. -->

- [ ] The wire protocol (`tape/proto/tape.proto`)
- [ ] The Rust server (`tape/server/`)
- [ ] Python SDK (`tape/sdk/python/`)
- [ ] TypeScript SDK (`tape/sdk/typescript/`)
- [ ] Go SDK (`tape/sdk/go/`)
- [ ] Java SDK (`tape/sdk/java/`)
- [ ] CLI (`tape/cli/`)
- [ ] Docs / examples
- [ ] CI / release / devex

## Tests

<!-- What ran green locally? -->

- [ ] `make sdk-test-<language>` for every touched SDK
- [ ] `make sdk-parity` (if you changed the protocol or any outbox path)
- [ ] `make test` (if you touched the Rust server)
- [ ] `make doctor` is clean

## Parity

<!-- If you added a new primitive, did every SDK gain it? If not, link the
     follow-up row in SDK_PARITY.md. -->

## Safety contract

<!-- If you changed any safety invariant (non-idempotent dispatch, the per-run
     lease, the CAS path), call it out explicitly. -->
