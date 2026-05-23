/// gRPC tunnel client — wraps AgentTransport and handles per-request HTTP relay.
///
/// No tonic or protobuf types appear here. All transport is via AgentTransport;
/// all framing is via domain TunnelFrame.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tracing::{error, info, warn};

use crate::adapter::agent::AgentTransport;
use crate::auth::Authenticator;
use crate::domain::types::{InboundRequest, TunnelFrame};

/// Shared state across concurrent request-relay tasks.
struct Inner {
    local_host: String,
    local_port: u16,
    http: Arc<reqwest::Client>,
    transport: AgentTransport,
}

pub struct Client {
    inner: Arc<Inner>,
    inbound_rx: mpsc::Receiver<TunnelFrame>,
}

impl Client {
    /// Connect to the rabbit server and receive the Hello frame.
    pub async fn new(
        local_host: &str,
        local_port: u16,
        to: &str,
        port: u16,
        secret: Option<&str>,
        service: Option<String>,
    ) -> Result<Self> {
        let auth = secret.map(Authenticator::new);
        let namespace = service.as_deref().unwrap_or("");

        let (assigned_port, transport, inbound_rx) =
            AgentTransport::connect(to, namespace, auth.as_ref(), port).await?;

        info!(assigned_port, "connected to rabbit server (gRPC)");
        info!("listening at {to}:{assigned_port}");

        let http = Arc::new(
            reqwest::Client::builder()
                .timeout(Duration::from_secs(120))
                .build()?,
        );

        let inner = Arc::new(Inner {
            local_host: local_host.to_string(),
            local_port,
            http,
            transport,
        });

        Ok(Client { inner, inbound_rx })
    }

    /// Process inbound frames until the connection is closed.
    pub async fn listen(mut self) -> Result<()> {
        while let Some(frame) = self.inbound_rx.recv().await {
            match frame {
                TunnelFrame::Inbound(req) => {
                    let inner = Arc::clone(&self.inner);
                    tokio::spawn(async move {
                        if let Err(e) = relay_request(req, inner).await {
                            warn!(%e, "request relay failed");
                        }
                    });
                }
                TunnelFrame::Heartbeat => {}
                TunnelFrame::Error(e) => error!(%e, "server error frame"),
                _ => warn!("unexpected frame from server"),
            }
        }
        Ok(())
    }
}

/// Relay one inbound HTTP request to the local service and stream the response back.
async fn relay_request(req: InboundRequest, inner: Arc<Inner>) -> Result<()> {
    let local_url = if req.query.is_empty() {
        format!("http://{}:{}{}", inner.local_host, inner.local_port, req.path)
    } else {
        format!("http://{}:{}{}?{}", inner.local_host, inner.local_port, req.path, req.query)
    };

    let method = req.method.parse::<reqwest::Method>()?;
    let mut builder = inner.http.request(method, &local_url).body(req.body);
    for (k, v) in &req.headers {
        builder = builder.header(k.as_str(), v.as_str());
    }

    let local_resp = builder.send().await?;
    let status = local_resp.status().as_u16() as u32;

    let resp_headers: HashMap<String, String> = local_resp
        .headers()
        .iter()
        .filter_map(|(k, v)| {
            let name = k.as_str();
            if matches!(name, "transfer-encoding" | "connection" | "keep-alive" | "trailer") {
                return None;
            }
            Some((name.to_string(), v.to_str().ok()?.to_string()))
        })
        .collect();

    inner.transport.send_frame(TunnelFrame::ResponseMeta {
        request_id: req.id.clone(),
        status,
        headers: resp_headers,
    })?;

    let mut byte_stream = local_resp.bytes_stream();
    while let Some(chunk) = byte_stream.next().await {
        let data: Bytes = chunk?;
        inner.transport.send_frame(TunnelFrame::ResponseChunk {
            request_id: req.id.clone(),
            data,
        })?;
    }

    inner.transport.send_frame(TunnelFrame::ResponseEnd { request_id: req.id })?;

    Ok(())
}
