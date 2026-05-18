# Examples — the same minimal scenario in every SDK

The four files in this directory each:

1. Connect to `tape://127.0.0.1:7878`,
2. Begin a run + record one decision + run one effect end-to-end,
3. Print the run id + effect status from the journal.

That's it. The point is to feel how the SDKs map to the wire protocol in
under thirty lines of code, in any language you're comfortable in. The
deeper, full-agent examples (treasury, non-idempotent bank) live under
[`tape/examples/`](../tape/examples/).

Run any of them against a local Tape server:

```bash
make demo &                            # or:  make serve  (foreground)
# then, in any of the four:
python   examples/quickstart.py
node     --experimental-strip-types --no-warnings examples/quickstart.ts
go       run examples/quickstart.go
cd examples && javac -cp $(cat ../tape/sdk/java/target/cp.txt):../tape/sdk/java/target/classes QuickstartJava.java && \
    java -cp .:$(cat ../tape/sdk/java/target/cp.txt):../tape/sdk/java/target/classes QuickstartJava
```

Or, the one-shot:

```bash
make quickstart-python      # any of: python | ts | go | java
make quickstart-all         # all four, against the same fresh server
```

Each one prints the same shape:

```
[quickstart/<lang>] begin_run    → run-id=<...>
[quickstart/<lang>] record_decision  decision_index=0
[quickstart/<lang>] begin_effect   → key=<...>  status=PENDING
[quickstart/<lang>] complete_effect → status=CONFIRMED
[quickstart/<lang>] get_effect     status=CONFIRMED  response={"ok":true,"who":"<lang>"}
```

If you run all four against the same server, the journal contains four
independent runs and four confirmed effects — proof that every SDK round-trips
the lifecycle in the same way.
