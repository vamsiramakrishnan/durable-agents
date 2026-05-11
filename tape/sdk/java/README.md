# tape-java (scaffold)

The Java SDK for [Tape](../../../design-principles/tape.md). **Status: scaffold.**
The contract is [`../../proto/tape.proto`](../../proto/tape.proto); the Python
adapter in [`../python/tape/adk/`](../python/tape/adk/) is the reference for the
`TapePlugin` / `TapeSessionService` wiring (the `TODO` here).

Generate the gRPC client with the `protobuf-maven-plugin` (or `protoc` +
`protoc-gen-grpc-java`) against `../../proto/tape.proto`, then implement
`TapePlugin` and `TapeSessionService` mirroring the Python adapter. The protocol
is the stable surface until then.
