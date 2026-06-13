use std::net::SocketAddr;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::{debug, warn};

use crate::config::{TUNNEL_ID_MAX_BYTES, TUNNEL_ID_READ_TIMEOUT};
use crate::domain::error::TunnelError;

/// Accepts agent TCP connections on the tunnel port.
///
/// Each connecting agent sends `<id>\n` as its first bytes. The server reads
/// the id, looks up the registered handler channel, and forwards the socket.
pub struct TunnelServer {
    handlers: Arc<DashMap<String, mpsc::Sender<TcpStream>>>,
}

impl TunnelServer {
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(DashMap::new()),
        }
    }

    pub fn register(&self, id: &str, tx: mpsc::Sender<TcpStream>) {
        self.handlers.insert(id.to_owned(), tx);
    }

    pub fn unregister(&self, id: &str) {
        self.handlers.remove(id);
    }

    pub async fn listen(self: Arc<Self>, addr: SocketAddr) -> anyhow::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        tracing::info!("tunnel server listening on {addr}");
        loop {
            let (stream, peer) = listener.accept().await?;
            debug!("tunnel connection from {peer}");
            let srv = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(e) = srv.handle_connection(stream).await {
                    warn!("tunnel handshake error from {peer}: {e}");
                }
            });
        }
    }

    async fn handle_connection(&self, mut stream: TcpStream) -> Result<(), TunnelError> {
        let mut buf = Vec::with_capacity(64);

        // Read bytes one at a time until newline, with a timeout and size cap.
        let read_id = async {
            loop {
                let mut byte = [0u8; 1];
                stream.read_exact(&mut byte).await?;
                if byte[0] == b'\n' {
                    break;
                }
                buf.push(byte[0]);
                if buf.len() > TUNNEL_ID_MAX_BYTES {
                    return Err(TunnelError::IdTooLong);
                }
            }
            Ok(())
        };

        timeout(TUNNEL_ID_READ_TIMEOUT, read_id)
            .await
            .map_err(|_| TunnelError::IdTimeout)??;

        let id = String::from_utf8_lossy(&buf).into_owned();
        let id = id.trim().to_owned();

        match self.handlers.get(&id) {
            Some(tx) => {
                debug!("routing tunnel socket to '{id}'");
                // Ignore send error — client may have unregistered between accept and route.
                let _ = tx.send(stream).await;
                Ok(())
            }
            None => Err(TunnelError::UnknownId),
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    use super::*;

    async fn connect_and_send(addr: SocketAddr, payload: &str) -> TcpStream {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(payload.as_bytes()).await.unwrap();
        s
    }

    #[tokio::test]
    async fn tunnel_server_should_route_socket_to_registered_handler() {
        let srv = Arc::new(TunnelServer::new());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let (tx, mut rx) = mpsc::channel(1);
        srv.register("myapp", tx);

        let srv2 = Arc::clone(&srv);
        tokio::spawn(async move { srv2.listen(addr).await });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        connect_and_send(addr, "myapp\n").await;
        let received = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            rx.recv(),
        )
        .await
        .expect("timed out waiting for socket")
        .expect("channel closed");
        drop(received);
    }

    #[tokio::test]
    async fn tunnel_server_should_reject_unknown_id() {
        let srv = Arc::new(TunnelServer::new());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let srv2 = Arc::clone(&srv);
        tokio::spawn(async move { srv2.listen(addr).await });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Connection should be dropped by the server after unknown id
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(b"ghost\n").await.unwrap();
        let mut buf = [0u8; 1];
        // Server drops the socket on unknown id → read returns 0 bytes
        let n = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            s.read(&mut buf),
        )
        .await
        .expect("timed out")
        .unwrap_or(0);
        assert_eq!(n, 0);
    }
}
