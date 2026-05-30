//! Tape server — the durable-execution substrate for ADK agents.
//!
//! Run it (the store is chosen by URL — that's the whole "wiring"):
//!   tape-server --listen 0.0.0.0:7878 --store sqlite:./tape.db
//!   tape-server --listen 0.0.0.0:7878 --store postgres://tape:tape@db:5432/tape
//!   tape-server --listen 0.0.0.0:7878 --store memory          # ephemeral, for demos/tests
//!
//! For horizontal scaling: point N replicas at the same `postgres://...` behind
//! a load balancer. The server is stateless between requests; "one driver per
//! run at a time" is the per-run lease in `tape_runs`; every mutating RPC is
//! idempotent, so a double-drive (two recovery workers racing) is harmless.

mod bigtable_change_stream;
mod cel;
mod chaos;
#[cfg(test)]
mod dst;
mod healthz;
#[cfg(test)]
mod lin;
mod matcher;
mod pb;
mod service;
#[cfg(all(test, feature = "sim"))]
mod sim;
mod store;
mod subjects;

use clap::Parser;
use tonic::transport::Server;

use pb::tape_server::TapeServer;
use service::TapeService;

#[derive(Parser, Debug)]
#[command(name = "tape-server", about = "Tape — a durable-execution substrate for ADK agents")]
struct Args {
    /// Address to listen on.
    #[arg(long, env = "TAPE_LISTEN", default_value = "0.0.0.0:7878")]
    listen: String,
    /// HTTP healthz listener address (for K8s liveness / readiness
    /// probes). Set to empty to disable.
    #[arg(long, env = "TAPE_HEALTHZ_LISTEN", default_value = "0.0.0.0:7879")]
    healthz_listen: String,
    /// Store URL: sqlite:<path> | sqlite::memory: | postgres://… | memory.
    #[arg(long, env = "TAPE_STORE")]
    store: Option<String>,
    /// Deprecated alias for `--store sqlite:<path>`.
    #[arg(long, env = "TAPE_DB")]
    db: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "tape_server=info,tonic=warn".into()),
        )
        .init();

    // No-op when built without `--features chaos`. With it, this registers
    // every `fail::fail_point!` site declared in `service.rs` against the
    // `FAILPOINTS` env var. See `chaos.rs` + `design-principles/chaos.md`.
    chaos::init();

    let args = Args::parse();
    let store_url = match (args.store.as_deref(), args.db.as_deref()) {
        (Some(s), _) => s.to_string(),
        (None, Some(db)) => {
            if db == ":memory:" { "memory".to_string() } else { format!("sqlite:{db}") }
        }
        (None, None) => "sqlite:tape.db".to_string(),
    };
    let addr = args.listen.parse()?;
    let store = store::open(&store_url).await.map_err(|e| anyhow::anyhow!(e.to_string()))?;
    tracing::info!(store = %store_url, listen = %args.listen, "tape server starting");

    // In-server matcher: tails the journal, produces tasks/runs for matching
    // reactions. See design-principles/tape-event-bus.md §2.3.
    matcher::spawn(store.clone());

    // K8s probe surface (separate port, HTTP/1.1). Empty value disables.
    if !args.healthz_listen.is_empty() {
        if let Err(err) = healthz::spawn(&args.healthz_listen, store.clone()).await {
            tracing::warn!(%err, "healthz listener failed to bind — probes disabled");
        }
    }

    let svc = TapeServer::new(TapeService::new(store));
    Server::builder()
        .add_service(svc)
        .serve_with_shutdown(addr, async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutdown signal received");
        })
        .await?;
    Ok(())
}
