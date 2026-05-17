# Observability

Tape emits **structured logs** (JSON) and **OpenTelemetry spans**. On GCP,
both go to Cloud Logging and Cloud Trace automatically when the standard
service-account roles are granted.

## Structured logs

Every server + reactor log line carries these fields (when known):

```
tenant_id  app_name  run_id  invocation_id  session_id
seq  effect_key  decision_index  reactor  lease_owner
```

`tape.obs.log_json("msg", **fields)` writes one line in that order. Cloud
Logging indexes the JSON keys; your queries look like:

```
jsonPayload.app_name="treasury" AND jsonPayload.run_id="r-..."
```

## OpenTelemetry spans

The SDK opens spans on the boundaries you care about:

```
tape.begin_run             tape.resume_run
tape.record_decision
tape.begin_effect          tape.complete_effect
tape.reconcile_effect      tape.dispatch_effect
tape.compensate            tape.redrive
tape.await_signal          tape.send_signal
```

Bring your own exporter, or call `tape.obs.configure_cloud_trace_exporter()`
at process start. The exporter is lazy — no OTel SDK installed, no spans, no
crash.

## Dashboard + log-based metrics

The `observability` Terraform module provisions:

  * `tape/runs/running`, `tape/runs/stuck`
  * `tape/effects/unknown`
  * `tape/obligations/unresolved`
  * `tape/reactor/lag_ms`

…and a Cloud Monitoring dashboard wiring all five. Customize the dashboard
JSON in
`tape/deploy/gcp/terraform/modules/observability/dashboard.json`.

## What to alert on

  * `tape/runs/stuck > 0` for > 5m — escalate.
  * `tape/effects/unknown` non-zero **and** rising for > 10m — reconciler is
    failing to resolve, the upstream is degraded, or a status check is missing.
  * `tape/obligations/unresolved > 0` for > 15m — compensation is failing.
  * `tape/reactor/lag_ms > 60_000` — a reactor is starved; check pod CPU.
