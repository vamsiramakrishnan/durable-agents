# Log-based metrics + a starter Cloud Monitoring dashboard for the Tape
# server / reactors. Dashboards are kept JSON for portability; edit the JSON
# in dashboard.json next to this module to tune charts.

resource "google_logging_metric" "runs_running" {
  project = var.project_id
  name    = "tape/runs/running"
  filter  = "resource.type=\"cloud_run_revision\" AND jsonPayload.msg=\"run.running\""
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
  }
}

resource "google_logging_metric" "runs_stuck" {
  project = var.project_id
  name    = "tape/runs/stuck"
  filter  = "resource.type=\"cloud_run_revision\" AND jsonPayload.msg=\"run.stuck\""
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
  }
}

resource "google_logging_metric" "effects_unknown" {
  project = var.project_id
  name    = "tape/effects/unknown"
  filter  = "resource.type=\"cloud_run_revision\" AND jsonPayload.msg=\"effect.unknown\""
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
  }
}

resource "google_logging_metric" "obligations_unresolved" {
  project = var.project_id
  name    = "tape/obligations/unresolved"
  filter  = "resource.type=\"cloud_run_revision\" AND jsonPayload.msg=\"obligation.unresolved\""
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
  }
}

resource "google_logging_metric" "reactor_lag_ms" {
  project = var.project_id
  name    = "tape/reactor/lag_ms"
  filter  = "resource.type=\"cloud_run_revision\" AND jsonPayload.msg=\"reactor.tick\""
  metric_descriptor {
    metric_kind = "GAUGE"
    value_type  = "INT64"
  }
  value_extractor = "EXTRACT(jsonPayload.lag_ms)"
}

resource "google_monitoring_dashboard" "tape" {
  project        = var.project_id
  dashboard_json = file("${path.module}/dashboard.json")
}
