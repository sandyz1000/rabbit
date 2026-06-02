use std::ops::RangeInclusive;
use std::sync::Arc;

use anyhow::Result;
use axum::Router;
use axum::routing::{any, get, post};
use tracing::info;

use crate::adapter::coordinator::{AppState, command_handler};
use crate::auth::Authenticator;
use crate::coordinator::RegistryCoordinator;
use crate::domain::coordinator::TunnelCoordinator;

pub struct Server {
    port_range: RangeInclusive<u16>,
    auth: Option<Authenticator>,
}

impl Server {
    pub fn new(port_range: RangeInclusive<u16>, secret: Option<&str>) -> Self {
        assert!(!port_range.is_empty(), "must provide at least one port");
        Server {
            port_range,
            auth: secret.map(Authenticator::new),
        }
    }

    /// Start serving on `bind_port`. All routes share a single TCP listener.
    pub async fn listen(self, bind_port: u16) -> Result<()> {
        let auth_arc = self.auth.map(Arc::new);

        let coordinator: Arc<dyn TunnelCoordinator> =
            Arc::new(RegistryCoordinator::new(self.port_range, auth_arc.clone()));

        let state = AppState {
            coordinator: Arc::clone(&coordinator),
            auth: auth_arc,
        };

        let app = Router::new()
            .route("/health", get(crate::proxy::health))
            .route("/rabbit", post(command_handler))
            .fallback(any(crate::proxy::proxy_handler))
            .with_state(state);

        let addr = format!("0.0.0.0:{bind_port}");
        let listener = tokio::net::TcpListener::bind(&addr).await?;
        info!(%addr, "rabbit server listening");

        axum::serve(listener, app).await?;
        Ok(())
    }
}
