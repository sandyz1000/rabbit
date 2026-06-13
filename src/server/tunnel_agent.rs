use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::net::TcpStream;
use tokio::sync::{oneshot, Mutex};
use tracing::debug;

use crate::domain::error::RoutingError;

/// Manages the pool of TCP tunnel sockets for a single registered client.
///
/// The agent maintains two queues:
/// - `available`: idle sockets ready to serve the next request
/// - `waiters`: pending acquirers that arrived before a socket was ready
pub struct TunnelAgent {
    available: Mutex<VecDeque<TcpStream>>,
    waiters: Mutex<VecDeque<oneshot::Sender<TcpStream>>>,
    total: AtomicUsize,
}

impl TunnelAgent {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            available: Mutex::new(VecDeque::new()),
            waiters: Mutex::new(VecDeque::new()),
            total: AtomicUsize::new(0),
        })
    }

    /// Called by TunnelServer when a new socket arrives for this client.
    pub async fn on_socket(self: &Arc<Self>, stream: TcpStream) {
        self.total.fetch_add(1, Ordering::Relaxed);

        // If a request is already waiting, hand the socket directly to it.
        let waiter = self.waiters.lock().await.pop_front();
        if let Some(tx) = waiter {
            debug!("dispatching socket to waiting requester");
            // If the waiter already gave up (receiver dropped), push to pool instead.
            if tx.send(stream).is_err() {
                self.total.fetch_sub(1, Ordering::Relaxed);
            }
            return;
        }

        self.available.lock().await.push_back(stream);
    }

    /// Acquire a tunnel socket, waiting if none are currently available.
    pub async fn acquire(self: &Arc<Self>) -> Result<TcpStream, RoutingError> {
        // Fast path: pool has an idle socket.
        if let Some(stream) = self.available.lock().await.pop_front() {
            self.total.fetch_sub(1, Ordering::Relaxed);
            return Ok(stream);
        }

        // Slow path: register a waiter and block until a socket arrives.
        let (tx, rx) = oneshot::channel();
        self.waiters.lock().await.push_back(tx);
        rx.await.map_err(|_| RoutingError::NoSocket)
    }

    /// Number of idle sockets in the pool.
    pub async fn available_count(&self) -> usize {
        self.available.lock().await.len()
    }

    /// Total sockets currently tracked (idle + in-flight).
    pub fn total_count(&self) -> usize {
        self.total.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;

    use super::*;

    async fn loopback_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let connect = TcpStream::connect(addr);
        let (accept, connect) = tokio::join!(listener.accept(), connect);
        (accept.unwrap().0, connect.unwrap())
    }

    #[tokio::test]
    async fn tunnel_agent_should_return_idle_socket_immediately() {
        let agent = TunnelAgent::new();
        let (s, _) = loopback_pair().await;
        agent.on_socket(s).await;
        let acquired = agent.acquire().await;
        assert!(acquired.is_ok());
    }

    #[tokio::test]
    async fn tunnel_agent_should_deliver_socket_to_waiter_when_pool_empty() {
        let agent = TunnelAgent::new();
        let agent2 = Arc::clone(&agent);

        // Start a waiter before any socket arrives.
        let waiter = tokio::spawn(async move { agent2.acquire().await });

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let (s, _) = loopback_pair().await;
        agent.on_socket(s).await;

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            waiter,
        )
        .await
        .expect("timed out")
        .expect("task panicked");
        assert!(result.is_ok());
    }
}
