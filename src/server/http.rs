use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::{Json, Router};
use axum::routing::{get, any};
use http::Request;
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use tracing::warn;

use crate::auth::Authenticator;
use crate::config::AUTH_WINDOW_SECS;
use crate::domain::error::ConnectError;
use crate::domain::types::{AuthProof, ServiceInfo, StatusInfo};
use crate::server::client_manager::ClientManager;

#[derive(Clone)]
pub struct AppState {
    pub manager: Arc<ClientManager>,
    pub domain: Option<String>,
}

#[derive(Deserialize)]
struct RegisterParams {
    id: String,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/api/status", get(status_handler))
        .route("/api/tunnels", get(list_tunnels_handler))
        .route("/api/tunnels/:id", get(get_tunnel_handler))
        .route("/api/tunnel", get(register_handler))
        .fallback(any(proxy_handler))
        .with_state(state)
}

async fn health_handler() -> impl IntoResponse {
    Json(serde_json::json!({"ok": true}))
}

async fn status_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json(StatusInfo {
        tunnels: state.manager.tunnel_count(),
        uptime_secs: state.manager.uptime_secs(),
    })
}

async fn list_tunnels_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.manager.list().await)
}

async fn get_tunnel_handler(
    Path(id): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    match state.manager.get(&id) {
        Some(client) => {
            let info = ServiceInfo {
                id: client.id.clone(),
                url: client.url.clone(),
                available_sockets: client.agent.available_count().await,
                total_sockets: client.agent.total_count(),
                connected_at: client.connected_at,
            };
            Json(info).into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn register_handler(
    Query(params): Query<RegisterParams>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let proof = extract_auth_proof(&headers);
    match state.manager.register(&params.id, proof).await {
        Ok(info) => Json(info).into_response(),
        Err(ConnectError::Unauthenticated | ConnectError::TimestampExpired) => {
            StatusCode::UNAUTHORIZED.into_response()
        }
        Err(ConnectError::IdInUse) => StatusCode::CONFLICT.into_response(),
        Err(ConnectError::InvalidId) => StatusCode::BAD_REQUEST.into_response(),
    }
}

async fn proxy_handler(
    State(state): State<AppState>,
    mut req: Request<axum::body::Body>,
) -> impl IntoResponse {
    let Some(id) = resolve_tunnel_id(&req, state.domain.as_deref()) else {
        return StatusCode::BAD_REQUEST.into_response();
    };

    let Some(client) = state.manager.get(&id) else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let Ok(socket) = client.agent.acquire().await else {
        return (StatusCode::BAD_GATEWAY, "no tunnel socket available").into_response();
    };

    let is_upgrade = req.headers().contains_key(http::header::UPGRADE);

    // Register the upgrade future before forwarding so hyper tracks it.
    let client_upgrade = if is_upgrade {
        Some(hyper::upgrade::on(&mut req))
    } else {
        None
    };

    let io = TokioIo::new(socket);
    let (mut sender, conn) = match http1::handshake(io).await {
        Ok(pair) => pair,
        Err(e) => {
            warn!("http1 handshake failed for '{id}': {e}");
            return StatusCode::BAD_GATEWAY.into_response();
        }
    };
    // with_upgrades keeps the connection alive through a 101 response.
    tokio::spawn(conn.with_upgrades());

    let mut response = match sender.send_request(req).await {
        Ok(r) => r,
        Err(e) => {
            warn!("tunnel request failed for '{id}': {e}");
            return StatusCode::BAD_GATEWAY.into_response();
        }
    };

    // If both sides agreed to upgrade, bridge them bidirectionally.
    if response.status() == StatusCode::SWITCHING_PROTOCOLS {
        if let Some(client_fut) = client_upgrade {
            let tunnel_upgrade = hyper::upgrade::on(&mut response);
            tokio::spawn(async move {
                match tokio::join!(client_fut, tunnel_upgrade) {
                    (Ok(client_io), Ok(tunnel_io)) => {
                        let mut c = TokioIo::new(client_io);
                        let mut t = TokioIo::new(tunnel_io);
                        if let Err(e) = tokio::io::copy_bidirectional(&mut c, &mut t).await {
                            warn!("ws bridge error for '{id}': {e}");
                        }
                    }
                    (Err(e), _) | (_, Err(e)) => {
                        warn!("upgrade future failed for '{id}': {e}");
                    }
                }
            });
        }
    }

    response.map(axum::body::Body::new).into_response()
}

/// Resolve the tunnel id from `X-Tunnel-Id` header (dev) or subdomain (prod).
fn resolve_tunnel_id(req: &Request<axum::body::Body>, domain: Option<&str>) -> Option<String> {
    // Dev path: explicit header takes priority.
    if let Some(id) = req.headers().get("x-tunnel-id") {
        return id.to_str().ok().map(str::to_owned);
    }

    // Prod path: extract subdomain from Host header.
    let host = req.headers().get(http::header::HOST)?.to_str().ok()?;
    let domain = domain?;
    extract_subdomain(host, domain)
}

fn extract_subdomain(host: &str, domain: &str) -> Option<String> {
    // Strip port if present.
    let host = host.split(':').next()?;
    let suffix = format!(".{domain}");
    host.strip_suffix(suffix.as_str()).map(str::to_owned)
}

fn extract_auth_proof(headers: &HeaderMap) -> Option<AuthProof> {
    let ts: u64 = headers
        .get("x-rabbit-ts")?
        .to_str()
        .ok()?
        .parse()
        .ok()?;
    let tag = headers.get("x-rabbit-auth")?.to_str().ok()?.to_owned();
    Some(AuthProof { ts, tag })
}

pub fn validate_auth(
    auth: &Authenticator,
    proof: &AuthProof,
    now: u64,
) -> Result<(), ConnectError> {
    if now.abs_diff(proof.ts) > AUTH_WINDOW_SECS {
        return Err(ConnectError::TimestampExpired);
    }
    if auth.verify(&proof.ts.to_le_bytes(), &proof.tag).is_err() {
        return Err(ConnectError::Unauthenticated);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_subdomain_should_return_subdomain_prefix() {
        let id = extract_subdomain("myapp.tunnel.example.com", "tunnel.example.com");
        assert_eq!(id.as_deref(), Some("myapp"));
    }

    #[test]
    fn extract_subdomain_should_strip_port() {
        let id = extract_subdomain("myapp.tunnel.example.com:8080", "tunnel.example.com");
        assert_eq!(id.as_deref(), Some("myapp"));
    }

    #[test]
    fn extract_subdomain_should_return_none_for_root_domain() {
        let id = extract_subdomain("tunnel.example.com", "tunnel.example.com");
        assert!(id.is_none());
    }
}
