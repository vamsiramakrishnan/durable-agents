//! Liveness + readiness HTTP endpoints for K8s probes.
//!
//! tape-server's primary surface is gRPC, but K8s liveness / readiness
//! checks expect HTTP. Rather than pull in axum/hyper as direct
//! dependencies we run a tiny `tokio::net::TcpListener` on
//! `TAPE_HEALTHZ_LISTEN` (default `0.0.0.0:7879`) that speaks just
//! enough HTTP/1.1 to return:
//!
//!   GET /healthz     → 200 if the process is up (always)
//!   GET /readyz      → 200 if `store.ping()` succeeds, 503 otherwise
//!
//! The split matters: liveness gates restart (the process is wedged →
//! kill it), readiness gates traffic (the data path is unhealthy →
//! drain it). A shallow "is the process up" check for both fails open
//! during DB outages — the goal here is to fail closed on readiness so
//! the load balancer drains us.

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::store::RunStore;

const HTTP_200: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 3\r\nConnection: close\r\n\r\nok\n";
const HTTP_404: &[u8] = b"HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: 10\r\nConnection: close\r\n\r\nnot found\n";
const HTTP_503_PREFIX: &[u8] = b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\nConnection: close\r\nContent-Length: ";

/// Spawn the healthz listener. Returns an error only if the listener
/// can't bind; the per-connection loop logs and continues on errors so
/// a flaky DB doesn't take down the listener itself.
pub async fn spawn(addr: &str, store: Arc<dyn RunStore>) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(addr = %addr, "healthz listener ready");
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((sock, _)) => {
                    let store = store.clone();
                    tokio::spawn(async move {
                        if let Err(err) = handle(sock, store).await {
                            tracing::debug!(%err, "healthz: connection error");
                        }
                    });
                }
                Err(err) => {
                    tracing::warn!(%err, "healthz: accept failed");
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        }
    });
    Ok(())
}

async fn handle(mut sock: TcpStream, store: Arc<dyn RunStore>) -> std::io::Result<()> {
    // Read just the request line + headers; 1KB is more than enough for
    // a probe and bounds memory regardless of the peer.
    let mut buf = [0u8; 1024];
    let n = sock.read(&mut buf).await?;
    let head = std::str::from_utf8(&buf[..n]).unwrap_or("");
    let first = head.lines().next().unwrap_or("");
    // first line: "GET /path HTTP/1.1"
    let mut parts = first.split_whitespace();
    let _method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");
    let resp: Vec<u8> = match path {
        "/healthz" | "/" => HTTP_200.to_vec(),
        "/readyz" => match store.ping().await {
            Ok(()) => HTTP_200.to_vec(),
            Err(err) => {
                let body = format!("store ping failed: {err}\n");
                let mut out = Vec::with_capacity(HTTP_503_PREFIX.len() + 16 + body.len());
                out.extend_from_slice(HTTP_503_PREFIX);
                out.extend_from_slice(body.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n\r\n");
                out.extend_from_slice(body.as_bytes());
                out
            }
        },
        _ => HTTP_404.to_vec(),
    };
    sock.write_all(&resp).await?;
    sock.shutdown().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store;
    use tokio::io::AsyncReadExt;

    async fn read_status(addr: &str, path: &str) -> String {
        let mut sock = TcpStream::connect(addr).await.unwrap();
        let req = format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
        sock.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        sock.read_to_end(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf).into_owned()
    }

    #[tokio::test]
    async fn healthz_returns_200_when_store_is_up() {
        let s = store::open("memory").await.unwrap();
        spawn("127.0.0.1:17891", s).await.unwrap();
        // Tiny pause so the listener is accepting.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let resp = read_status("127.0.0.1:17891", "/healthz").await;
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "got: {resp}");
    }

    #[tokio::test]
    async fn readyz_returns_200_when_ping_ok() {
        let s = store::open("sqlite::memory:").await.unwrap();
        spawn("127.0.0.1:17892", s).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let resp = read_status("127.0.0.1:17892", "/readyz").await;
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "got: {resp}");
    }

    #[tokio::test]
    async fn unknown_path_404s() {
        let s = store::open("memory").await.unwrap();
        spawn("127.0.0.1:17893", s).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let resp = read_status("127.0.0.1:17893", "/nope").await;
        assert!(resp.starts_with("HTTP/1.1 404"), "got: {resp}");
    }
}
