# treasury-idempotent

The treatise's treasury agent, ported to the standalone scaffold. Same
business logic as `tape/examples/treasury/`, but wired through
`tape.adk.durable_app(...)` and ready for `tape provision gcp` /
`tape deploy gcp`.

See `../../treasury/` for the original.
