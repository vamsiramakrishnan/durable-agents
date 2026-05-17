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
mod matcher;
mod pb;
mod service;
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
