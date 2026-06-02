/// Inbound proxy handler and health endpoint.
///
/// Catches all HTTP requests not handled by the tunnel endpoint.
/// Reads X-Tunnel-Port, routes to the registered agent via TunnelCoordinator,
/// and streams the response back chunk by chunk.
use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use crate::domain::coordinator::TunnelCoordinator;
use crate::domain::error::RoutingError;
use crate::domain::types::InboundRequest;
use crate::shared::RELAY_TIMEOUT;

pub(crate) async fn health() -> impl IntoResponse {
    axum::Json(serde_json::json!({"ok": true}))
}

pub(crate) async fn proxy_handler(
    State(coordinator): State<Arc<dyn TunnelCoordinator>>,
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let port: u16 = match headers
        .get("x-tunnel-port")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
    {
        Some(p) => p,
        None => {
            return (StatusCode::BAD_REQUEST, "missing X-Tunnel-Port header").into_response();
        }
    };

    let mut fwd_headers: HashMap<String, String> = HashMap::new();
    for (k, v) in &headers {
        let name = k.as_str();
        if matches!(name, "x-tunnel-port" | "host" | "connection" | "transfer-encoding") {
            continue;
        }
        if let Ok(s) = v.to_str() {
            fwd_headers.insert(name.to_string(), s.to_string());
        }
    }

    // Build a placeholder InboundRequest — coordinator replaces the id.
    let req = InboundRequest {
        id:      crate::domain::types::RequestId(String::new()),
        method:  method.to_string(),
        path:    uri.path().to_string(),
        query:   uri.query().unwrap_or("").to_string(),
        headers: fwd_headers,
        body,
    };

    let receiver = match coordinator.route_request(port, req).await {
        Ok(r) => r,
        Err(RoutingError::NoAgent(p)) => {
            return (StatusCode::BAD_GATEWAY, format!("no tunnel agent on port {p}"))
                .into_response();
        }
        Err(RoutingError::AgentDisconnected) => {
            return (StatusCode::BAD_GATEWAY, "tunnel agent disconnected").into_response();
        }
    };

    let head = match tokio::time::timeout(RELAY_TIMEOUT, receiver.0).await {
        Ok(Ok(h)) => h,
        Ok(Err(_)) => {
            return (StatusCode::BAD_GATEWAY, "tunnel response channel closed").into_response();
        }
        Err(_) => {
            return (StatusCode::GATEWAY_TIMEOUT, "tunnel relay timed out").into_response();
        }
    };

    let body_stream = ReceiverStream::new(head.body_rx)
        .map(Ok::<Bytes, std::convert::Infallible>);

    let mut builder = Response::builder().status(head.status as u16);
    for (k, v) in &head.headers {
        if let (Ok(name), Ok(val)) = (
            HeaderName::from_bytes(k.as_bytes()),
            HeaderValue::from_str(v),
        ) {
            builder = builder.header(name, val);
        }
    }
    builder
        .body(Body::from_stream(body_stream))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}
