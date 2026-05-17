# tape-cli — the standalone Tape developer experience

```bash
pip install -e tape/sdk/python    # tape-py: the SDK + ADK adapter
pip install -e tape/cli           # tape: the CLI

tape init treasury
cd treasury
tape dev
tape doctor
tape provision gcp --store alloydb --events pubsub --apply
tape deploy gcp --target cloud-run
tape doctor --gcp
```

The CLI composes the substrate (Tape) and the cloud (GCP) without making
developers learn every seam first. It is **agents-cli aware, not
agents-cli dependent** — there is no import of `google/agents-cli` here.

See `tape/docs/quickstart.md` for the full walkthrough.
