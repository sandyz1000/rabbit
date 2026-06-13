use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;

use crate::config::ServerConfig;
use crate::server::client_manager::ClientManager;
use crate::server::http::AppState;
use crate::server::tunnel_server::TunnelServer;

pub mod client_manager;
pub mod http;
pub mod tunnel_agent;
pub mod tunnel_server;

pub async fn run(config: ServerConfig) -> anyhow::Result<()> {
    let tunnel_server = Arc::new(TunnelServer::new());

    let manager = ClientManager::new(
        Arc::clone(&tunnel_server),
        config.domain.clone(),
        config.tunnel_port,
        config.secret.as_deref(),
    );

    let state = AppState {
        manager: Arc::clone(&manager),
        domain: config.domain.clone(),
    };

    let http_addr: SocketAddr = ([0, 0, 0, 0], config.http_port).into();
    let tunnel_addr: SocketAddr = ([0, 0, 0, 0], config.tunnel_port).into();

    let listener = TcpListener::bind(http_addr).await?;
    tracing::info!("http server listening on {http_addr}");

    let router = http::router(state);

    let tunnel_srv = Arc::clone(&tunnel_server);
    tokio::spawn(async move {
        if let Err(e) = tunnel_srv.listen(tunnel_addr).await {
            tracing::error!("tunnel server error: {e}");
        }
    });

    axum::serve(listener, router).await?;
    Ok(())
}
