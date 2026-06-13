use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tracing::{debug, warn};

/// Manages a single persistent TCP tunnel connection to the server.
///
/// On each iteration of the serve loop:
/// 1. Connect to the server's tunnel port.
/// 2. Send `<id>\n` so TunnelServer can route the socket.
/// 3. Connect to the local service and bidirectionally copy traffic.
/// 4. On any error or disconnect, wait briefly and reconnect.
pub struct TunnelCluster {
    pub id: String,
    pub server_addr: SocketAddr,
    pub local_host: String,
    pub local_port: u16,
}

impl TunnelCluster {
    pub async fn run(&self) {
        loop {
            match self.serve_once().await {
                Ok(()) => debug!("tunnel '{}' connection closed, reconnecting", self.id),
                Err(e) => warn!("tunnel '{}' error: {e}, reconnecting", self.id),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn serve_once(&self) -> anyhow::Result<()> {
        let mut remote = TcpStream::connect(self.server_addr).await?;
        remote.set_nodelay(true)?;

        // Identify this connection to the TunnelServer.
        remote
            .write_all(format!("{}\n", self.id).as_bytes())
            .await?;

        let local_addr = format!("{}:{}", self.local_host, self.local_port);
        let mut local = match TcpStream::connect(&local_addr).await {
            Ok(s) => s,
            Err(e) => {
                // Local service unavailable — end the remote connection cleanly
                // so the server socket returns to the pool rather than hanging.
                let _ = remote.shutdown().await;
                return Err(e.into());
            }
        };
        local.set_nodelay(true)?;

        tokio::io::copy_bidirectional(&mut remote, &mut local).await?;
        Ok(())
    }
}
