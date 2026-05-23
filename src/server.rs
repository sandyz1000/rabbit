/// Server entry point — merges tonic gRPC router with Axum HTTP router.
///
/// gRPC (application/grpc content-type) → Tunnel(), ListServices(), GetPorts()
/// Plain HTTP → /health liveness probe, ANY /* proxy_handler
use std::ops::RangeInclusive;
use std::sync::Arc;

use anyhow::Result;
use axum::Router;
use axum::routing::any;
use http::Request;
use tower::util::ServiceExt as _;
use tracing::info;

use crate::adapter::coordinator::TonicCoordinatorWithAuth;
use crate::auth::Authenticator;
use crate::coordinator::RegistryCoordinator;
use crate::domain::coordinator::TunnelCoordinator;
use crate::rabbit::rabbit_server::RabbitServer;

/// Public server struct.
pub struct Server {
    port_range: RangeInclusive<u16>,
    auth: Option<Authenticator>,
}

impl Server {
    pub fn new(port_range: RangeInclusive<u16>, secret: Option<&str>) -> Self {
        assert!(!port_range.is_empty(), "must provide at least one port");
        Server { port_range, auth: secret.map(Authenticator::new) }
    }

    /// Start serving gRPC + HTTP on `bind_port`.
    pub async fn listen(self, bind_port: u16) -> Result<()> {
        let auth_arc = self.auth.map(|a| Arc::new(a));

        let coordinator: Arc<dyn TunnelCoordinator> = Arc::new(RegistryCoordinator::new(
            self.port_range,
            auth_arc.clone(),
        ));

        // Route gRPC paths directly — replicates what tonic::service::Routes::add_service()
        // does internally so we avoid the "two fallbacks" panic when merging routers.
        let grpc_svc = RabbitServer::new(TonicCoordinatorWithAuth::new(
            Arc::clone(&coordinator),
            auth_arc,
        ))
        .map_request(|req: Request<axum::body::Body>| req.map(tonic::body::Body::new));

        let app = Router::new()
            .route("/health", axum::routing::get(crate::proxy::health))
            .route_service("/rabbit.Rabbit/{*rest}", grpc_svc)
            .fallback(any(crate::proxy::proxy_handler))
            .with_state(Arc::clone(&coordinator));

        let addr = format!("0.0.0.0:{bind_port}");
        let listener = tokio::net::TcpListener::bind(&addr).await?;
        info!(%addr, "rabbit server listening (gRPC + HTTP)");

        axum::serve(listener, app).await?;
        Ok(())
    }
}
