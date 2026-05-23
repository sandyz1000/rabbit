/// AgentTransport — wraps the generated RabbitTunnelClient.
/// This is the only place in the codebase that constructs a RabbitTunnelClient.
/// All public methods speak domain types; generated names stay inside this file.
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, bail};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Request;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

use crate::auth::Authenticator;
use crate::domain::types::{ServiceInfo, TunnelFrame};
use crate::rabbit::{
    GetPortsRequest, ListServicesRequest, TunnelMessage, rabbit_client::RabbitClient,
};

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn make_endpoint(to: &str) -> String {
    if to.starts_with("http://") || to.starts_with("https://") {
        to.to_string()
    } else {
        format!("http://{to}")
    }
}

/// Client-side transport handle for a connected tunnel agent.
pub struct AgentTransport {
    /// Sends domain TunnelFrames to the server over the gRPC stream.
    tx: mpsc::UnboundedSender<TunnelMessage>,
    /// Assigned virtual port returned by the server in the Hello frame.
    #[allow(dead_code)]
    pub assigned_port: u16,
}

impl AgentTransport {
    /// Connect to the rabbit server and perform the initial Hello handshake.
    /// Returns `(assigned_port, transport, domain_frame_receiver)`.
    /// The receiver yields domain `TunnelFrame` values; proto types stay inside this module.
    pub async fn connect(
        server_url: &str,
        namespace: &str,
        auth: Option<&Authenticator>,
        requested_port: u16,
    ) -> Result<(u16, Self, mpsc::Receiver<TunnelFrame>)> {
        let channel = Channel::from_shared(make_endpoint(server_url))?
            .connect()
            .await?;
        let mut grpc = RabbitClient::new(channel);

        let (tx, rx) = mpsc::unbounded_channel::<TunnelMessage>();

        // Send the client-side Hello with port hint as the first frame.
        tx.send(TunnelMessage::from(TunnelFrame::Hello {
            assigned_port: requested_port,
        }))?;

        let ts = now_secs();
        let auth_tag = auth.map(|a| a.sign(&ts.to_le_bytes())).unwrap_or_default();

        let mut request = Request::new(UnboundedReceiverStream::new(rx));
        let meta = request.metadata_mut();
        meta.insert("x-rabbit-service", MetadataValue::try_from(namespace)?);
        meta.insert(
            "x-rabbit-ts",
            MetadataValue::try_from(ts.to_string().as_str())?,
        );
        meta.insert("x-rabbit-auth", MetadataValue::try_from(auth_tag.as_str())?);
        meta.insert(
            "x-rabbit-port",
            MetadataValue::try_from(requested_port.to_string().as_str())?,
        );

        let response = grpc.tunnel(request).await?;
        let mut stream = response.into_inner();

        // First frame must be Hello { assigned_port }.
        let assigned_port = match stream.next().await {
            Some(Ok(msg)) => match TunnelFrame::try_from(msg) {
                Ok(TunnelFrame::Hello { assigned_port }) => assigned_port,
                Ok(TunnelFrame::Error(e)) => bail!("server error: {e}"),
                _ => bail!("unexpected initial frame from server"),
            },
            Some(Err(e)) => bail!("connection error: {e}"),
            None => bail!("server closed stream before Hello"),
        };

        // Spawn a converter task: proto stream → domain frame channel.
        // This keeps TunnelMessage out of the caller (client.rs).
        let (domain_tx, domain_rx) = mpsc::channel::<TunnelFrame>(64);
        tokio::spawn(async move {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(msg) => match TunnelFrame::try_from(msg) {
                        Ok(frame) => {
                            if domain_tx.send(frame).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => {}
                    },
                    Err(_) => break,
                }
            }
        });

        Ok((assigned_port, Self { tx, assigned_port }, domain_rx))
    }

    /// Send a domain frame to the server.
    pub fn send_frame(&self, frame: TunnelFrame) -> Result<()> {
        Ok(self.tx.send(TunnelMessage::from(frame))?)
    }

    // ── Service-discovery helpers (used by the `services` CLI subcommand) ──

    pub async fn list_services(
        server_url: &str,
        auth: Option<&Authenticator>,
    ) -> Result<Vec<ServiceInfo>> {
        let channel = Channel::from_shared(make_endpoint(server_url))?
            .connect()
            .await?;
        let mut grpc = RabbitClient::new(channel);
        let ts = now_secs();
        let auth_tag = auth.map(|a| a.sign(&ts.to_le_bytes())).unwrap_or_default();
        let resp = grpc
            .list_services(ListServicesRequest { ts, auth: auth_tag })
            .await?;
        Ok(resp
            .into_inner()
            .services
            .into_iter()
            .map(ServiceInfo::from)
            .collect())
    }

    pub async fn get_ports(
        server_url: &str,
        namespace: &str,
        auth: Option<&Authenticator>,
    ) -> Result<Vec<u16>> {
        let channel = Channel::from_shared(make_endpoint(server_url))?
            .connect()
            .await?;
        let mut grpc = RabbitClient::new(channel);
        let ts = now_secs();
        let auth_tag = auth.map(|a| a.sign(&ts.to_le_bytes())).unwrap_or_default();
        let resp = grpc
            .get_ports(GetPortsRequest {
                name: namespace.to_string(),
                ts,
                auth: auth_tag,
            })
            .await?;
        Ok(resp
            .into_inner()
            .ports
            .into_iter()
            .map(|p| p as u16)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn make_endpoint_already_http() {
        assert_eq!(
            make_endpoint("http://localhost:8080"),
            "http://localhost:8080"
        );
    }

    #[test]
    fn make_endpoint_no_scheme() {
        assert_eq!(make_endpoint("localhost:8080"), "http://localhost:8080");
    }

    #[test]
    fn make_endpoint_https_passthrough() {
        assert_eq!(
            make_endpoint("https://rabbit.fly.dev"),
            "https://rabbit.fly.dev"
        );
    }
}
