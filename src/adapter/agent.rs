use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, bail};
use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use tokio::sync::mpsc;
use tokio_stream::StreamExt as _;

use crate::adapter::codec::{FrameDecoder, encode_frame};
use crate::auth::Authenticator;
use crate::domain::types::{ServiceInfo, TunnelFrame};

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn make_url(base: &str, path: &str) -> String {
    let base = base.trim_end_matches('/');
    format!("{base}{path}")
}

fn auth_headers(ts: u64, auth: Option<&Authenticator>) -> HeaderMap {
    let mut map = HeaderMap::new();
    if let Ok(v) = HeaderValue::from_str(&ts.to_string()) {
        map.insert(HeaderName::from_static("x-rabbit-ts"), v);
    }
    let tag = auth.map(|a| a.sign(&ts.to_le_bytes())).unwrap_or_default();
    if let Ok(v) = HeaderValue::from_str(&tag) {
        map.insert(HeaderName::from_static("x-rabbit-auth"), v);
    }
    map
}

fn build_http2_client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder().http2_prior_knowledge().build()?)
}

/// Client-side transport handle for a connected tunnel agent.
pub struct AgentTransport {
    tx: mpsc::UnboundedSender<Bytes>,
}

impl AgentTransport {
    /// Connect to the rabbit server and perform the initial Hello handshake.
    ///
    /// Returns `(assigned_port, transport, domain_frame_receiver)`.
    /// The receiver yields domain `TunnelFrame` values; wire encoding stays inside this module.
    pub async fn connect(
        server_url: &str,
        namespace: &str,
        auth: Option<&Authenticator>,
        requested_port: u16,
    ) -> Result<(u16, Self, mpsc::Receiver<TunnelFrame>)> {
        let client = build_http2_client()?;
        let ts = now_secs();

        let (body_tx, body_rx) = mpsc::unbounded_channel::<Bytes>();
        let body_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(body_rx)
            .map(Ok::<_, std::convert::Infallible>);

        let mut headers = auth_headers(ts, auth);
        headers.insert(
            HeaderName::from_static("x-rabbit-cmd"),
            HeaderValue::from_static("tunnel"),
        );
        headers.insert(
            HeaderName::from_static("x-rabbit-service"),
            HeaderValue::from_str(namespace).unwrap_or_else(|_| HeaderValue::from_static("")),
        );
        headers.insert(
            HeaderName::from_static("x-rabbit-port"),
            HeaderValue::from_str(&requested_port.to_string())?,
        );

        let response = client
            .post(make_url(server_url, "/rabbit"))
            .headers(headers)
            .body(reqwest::Body::wrap_stream(body_stream))
            .send()
            .await?;

        if !response.status().is_success() {
            bail!("server rejected connection: {}", response.status());
        }

        // First frame from the server must be Hello { assigned_port }.
        let mut byte_stream = response.bytes_stream();
        let mut decoder = FrameDecoder::new();

        let assigned_port = loop {
            let Some(chunk) = byte_stream.next().await else {
                bail!("server closed stream before Hello");
            };
            decoder.feed(chunk?);
            if let Some(result) = decoder.next_frame() {
                match result? {
                    TunnelFrame::Hello { assigned_port } => break assigned_port,
                    TunnelFrame::Error(e) => bail!("server error: {e}"),
                    _ => bail!("unexpected initial frame from server"),
                }
            }
        };

        // Spawn: server → client stream decoded into domain frames.
        let (domain_tx, domain_rx) = mpsc::channel::<TunnelFrame>(64);
        tokio::spawn(async move {
            while let Some(result) = byte_stream.next().await {
                let Ok(chunk) = result else { break };
                decoder.feed(chunk);
                while let Some(frame_result) = decoder.next_frame() {
                    let Ok(frame) = frame_result else { break };
                    if domain_tx.send(frame).await.is_err() {
                        return;
                    }
                }
            }
        });

        Ok((assigned_port, Self { tx: body_tx }, domain_rx))
    }

    /// Encode and send a domain frame to the server.
    pub fn send_frame(&self, frame: TunnelFrame) -> Result<()> {
        let encoded = encode_frame(&frame)?;
        self.tx
            .send(encoded)
            .map_err(|_| anyhow::anyhow!("tunnel send channel closed"))
    }

    // ── Service-discovery helpers (used by the `services` CLI subcommand) ──

    pub async fn list_services(
        server_url: &str,
        auth: Option<&Authenticator>,
    ) -> Result<Vec<ServiceInfo>> {
        let client = build_http2_client()?;
        let ts = now_secs();
        let mut headers = auth_headers(ts, auth);
        headers.insert(
            HeaderName::from_static("x-rabbit-cmd"),
            HeaderValue::from_static("list_services"),
        );
        let response = client
            .post(make_url(server_url, "/rabbit"))
            .headers(headers)
            .send()
            .await?;
        Ok(response.json::<Vec<ServiceInfo>>().await?)
    }

    pub async fn get_ports(
        server_url: &str,
        namespace: &str,
        auth: Option<&Authenticator>,
    ) -> Result<Vec<u16>> {
        let client = build_http2_client()?;
        let ts = now_secs();
        let mut headers = auth_headers(ts, auth);
        headers.insert(
            HeaderName::from_static("x-rabbit-cmd"),
            HeaderValue::from_static("get_ports"),
        );
        headers.insert(
            HeaderName::from_static("x-rabbit-service"),
            HeaderValue::from_str(namespace).unwrap_or_else(|_| HeaderValue::from_static("")),
        );
        let response = client
            .post(make_url(server_url, "/rabbit"))
            .headers(headers)
            .send()
            .await?;
        Ok(response.json::<Vec<u16>>().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod make_url {
        use super::*;

        #[test]
        fn appends_path_to_base() {
            assert_eq!(
                make_url("http://localhost:8080", "/rabbit"),
                "http://localhost:8080/rabbit"
            );
        }

        #[test]
        fn strips_trailing_slash_from_base() {
            assert_eq!(
                make_url("http://localhost:8080/", "/rabbit"),
                "http://localhost:8080/rabbit"
            );
        }

        #[test]
        fn works_without_scheme() {
            assert_eq!(
                make_url("localhost:8080", "/rabbit"),
                "localhost:8080/rabbit"
            );
        }
    }
}
