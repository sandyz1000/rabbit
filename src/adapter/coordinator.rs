use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::body::Body;
use axum::extract::{FromRef, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::ReceiverStream;
use tracing::warn;

use crate::adapter::codec::{FrameDecoder, encode_frame};
use crate::auth::Authenticator;
use crate::domain::coordinator::TunnelCoordinator;
use crate::domain::types::{AuthProof, ServiceInfo, TunnelFrame};
use crate::shared::AUTH_WINDOW_SECS;

/// Shared router state: coordinator + optional auth secret.
#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) coordinator: Arc<dyn TunnelCoordinator>,
    pub(crate) auth: Option<Arc<Authenticator>>,
}

/// Allows handlers that only need the coordinator to keep `State<Arc<dyn TunnelCoordinator>>`.
impl FromRef<AppState> for Arc<dyn TunnelCoordinator> {
    fn from_ref(state: &AppState) -> Self {
        Arc::clone(&state.coordinator)
    }
}

#[derive(Debug)]
enum RabbitCommand {
    Tunnel,
    ListServices,
    GetPorts,
}

impl TryFrom<&str> for RabbitCommand {
    type Error = ();

    fn try_from(s: &str) -> Result<Self, ()> {
        match s {
            "tunnel" => Ok(Self::Tunnel),
            "list_services" => Ok(Self::ListServices),
            "get_ports" => Ok(Self::GetPorts),
            _ => Err(()),
        }
    }
}

/// POST /rabbit — single command dispatcher.
///
/// The `X-Rabbit-Cmd` header selects the operation; remaining headers carry
/// auth and parameters. Returns 400 for unknown commands.
pub(crate) async fn command_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: axum::extract::Request,
) -> Response {
    let cmd = match header_str(&headers, "x-rabbit-cmd").map(RabbitCommand::try_from) {
        Some(Ok(cmd)) => cmd,
        _ => return (StatusCode::BAD_REQUEST, "missing or unknown X-Rabbit-Cmd").into_response(),
    };

    match cmd {
        RabbitCommand::Tunnel => handle_tunnel(state, headers, request).await,
        RabbitCommand::ListServices => handle_list_services(state, headers).await,
        RabbitCommand::GetPorts => handle_get_ports(state, headers).await,
    }
}

// ── private handlers ──────────────────────────────────────────────────────────

async fn handle_tunnel(
    state: AppState,
    headers: HeaderMap,
    request: axum::extract::Request,
) -> Response {
    let namespace = header_str(&headers, "x-rabbit-service")
        .unwrap_or("")
        .to_string();
    let ts: u64 = header_str(&headers, "x-rabbit-ts")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let auth_tag = header_str(&headers, "x-rabbit-auth").unwrap_or("");
    let requested_port: u16 = header_str(&headers, "x-rabbit-port")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let auth_proof = (ts > 0 || !auth_tag.is_empty()).then(|| AuthProof {
        ts,
        tag: auth_tag.to_string(),
    });

    let handle = match state
        .coordinator
        .register_agent(namespace, auth_proof, requested_port)
        .await
    {
        Ok(h) => h,
        Err(e) => {
            warn!(%e, "agent registration failed");
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };

    let inbound_tx = handle.inbound_tx;
    tokio::spawn(async move {
        relay_inbound(request.into_body(), inbound_tx).await;
    });

    let outbound_stream = ReceiverStream::new(handle.outbound_rx)
        .filter_map(|frame| encode_frame(&frame).ok())
        .map(Ok::<_, std::convert::Infallible>);

    Response::builder()
        .header("content-type", "application/x-rabbit-stream")
        .body(Body::from_stream(outbound_stream))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

async fn handle_list_services(state: AppState, headers: HeaderMap) -> Response {
    let caller_fp = match extract_and_validate_auth(&state.auth, &headers) {
        Ok(fp) => fp,
        Err(()) => return StatusCode::UNAUTHORIZED.into_response(),
    };
    let services: Vec<ServiceInfo> = state.coordinator.list_services(caller_fp).await;
    Json(services).into_response()
}

async fn handle_get_ports(state: AppState, headers: HeaderMap) -> Response {
    let caller_fp = match extract_and_validate_auth(&state.auth, &headers) {
        Ok(fp) => fp,
        Err(()) => return StatusCode::UNAUTHORIZED.into_response(),
    };
    let name = header_str(&headers, "x-rabbit-service").unwrap_or("");
    let ports: Vec<u16> = state.coordinator.get_ports(name, caller_fp).await;
    Json(ports).into_response()
}

// ── shared helpers ────────────────────────────────────────────────────────────

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name)?.to_str().ok()
}

fn extract_and_validate_auth(
    auth: &Option<Arc<Authenticator>>,
    headers: &HeaderMap,
) -> Result<Option<[u8; 32]>, ()> {
    let Some(authenticator) = auth else {
        return Ok(None);
    };
    let ts: u64 = header_str(headers, "x-rabbit-ts")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let tag = header_str(headers, "x-rabbit-auth").unwrap_or("");

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if now.abs_diff(ts) > AUTH_WINDOW_SECS {
        warn!("service query rejected: timestamp out of window");
        return Err(());
    }
    if authenticator.verify(&ts.to_le_bytes(), tag).is_err() {
        warn!("service query rejected: invalid auth");
        return Err(());
    }
    Ok(Some(authenticator.fingerprint()))
}

async fn relay_inbound(body: Body, inbound_tx: tokio::sync::mpsc::Sender<TunnelFrame>) {
    let mut decoder = FrameDecoder::new();
    let mut data_stream = body.into_data_stream();

    while let Some(chunk) = data_stream.next().await {
        let Ok(data) = chunk else { break };
        decoder.feed(data);
        while let Some(result) = decoder.next_frame() {
            let Ok(frame) = result else { break };
            if inbound_tx.send(frame).await.is_err() {
                return;
            }
        }
    }
}
