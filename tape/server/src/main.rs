//! Tape server — the durable-execution substrate for ADK agents.
//!
//! Run it:
//!   tape-server --listen 0.0.0.0:7878 --db ./tape.db
//!
//! It speaks the `tape.v1` gRPC service (see ../proto/tape.proto). The SDKs are
//! thin clients; the agent code stays in whatever language writes agents.

mod pb;
mod service;
mod store;

use std::sync::Arc;

use clap::Parser;
use tonic::transport::Server;

use pb::tape_server::TapeServer;
use service::TapeService;
use store::Store;

#[derive(Parser, Debug)]
#[command(name = "tape-server", about = "Tape — a durable-execution substrate for ADK agents")]
struct Args {
    /// Address to listen on.
    #[arg(long, env = "TAPE_LISTEN", default_value = "0.0.0.0:7878")]
    listen: String,
    /// SQLite database path. Use ":memory:" for an ephemeral store (tests).
    #[arg(long, env = "TAPE_DB", default_value = "tape.db")]
    db: String,
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
    let addr = args.listen.parse()?;
    let store = Arc::new(Store::open(&args.db)?);
    tracing::info!(db = %args.db, listen = %args.listen, "tape server starting");

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
