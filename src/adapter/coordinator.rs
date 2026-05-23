/// TonicCoordinator — implements the generated Rabbit tonic service trait.
/// This is the only file besides adapter/proto.rs that may import generated
/// service names (Rabbit, RabbitServer, rabbit_server, TunnelMessage, *Request, *Response).
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::warn;

use crate::domain::coordinator::TunnelCoordinator;
use crate::domain::types::{AuthProof, TunnelFrame};
use crate::rabbit::{
    GetPortsRequest, GetPortsResponse, ListServicesRequest, ListServicesResponse, ServiceEntry,
    TunnelMessage,
    rabbit_server::Rabbit,
};
use crate::shared::AUTH_WINDOW_SECS;

type OutStream = ReceiverStream<Result<TunnelMessage, Status>>;

/// Wraps `Arc<dyn TunnelCoordinator>` and implements the generated `Rabbit` service trait.
pub(crate) struct TonicCoordinator {
    inner: Arc<dyn TunnelCoordinator>,
}

impl TonicCoordinator {
    pub(crate) fn new(inner: Arc<dyn TunnelCoordinator>) -> Self {
        Self { inner }
    }

}

#[tonic::async_trait]
impl Rabbit for TonicCoordinator {
    type TunnelStream = OutStream;

    async fn tunnel(
        &self,
        request: Request<Streaming<TunnelMessage>>,
    ) -> Result<Response<Self::TunnelStream>, Status> {
        let namespace = metadata_str(request.metadata(), "x-rabbit-service").unwrap_or_default();
        let ts: u64 = metadata_str(request.metadata(), "x-rabbit-ts")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let auth_tag = metadata_str(request.metadata(), "x-rabbit-auth").unwrap_or_default();
        let requested_port: u16 = metadata_str(request.metadata(), "x-rabbit-port")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let auth_proof = if ts > 0 || !auth_tag.is_empty() {
            Some(AuthProof { ts, tag: auth_tag })
        } else {
            None
        };

        let mut inbound_stream = request.into_inner();

        let mut handle = self
            .inner
            .register_agent(namespace, auth_proof, requested_port)
            .await
            .map_err(|e| Status::unauthenticated(e.to_string()))?;

        // Outbound: coordinator → gRPC stream (TunnelFrame → TunnelMessage).
        let (out_tx, out_rx) = mpsc::channel::<Result<TunnelMessage, Status>>(64);
        let out_tx2 = out_tx.clone();
        tokio::spawn(async move {
            while let Some(frame) = handle.outbound_rx.recv().await {
                let msg: TunnelMessage = frame.into();
                if out_tx2.send(Ok(msg)).await.is_err() {
                    break;
                }
            }
        });

        // Inbound: gRPC stream → domain TunnelFrame → relay_loop via handle.inbound_tx.
        let inbound_tx = handle.inbound_tx;
        tokio::spawn(async move {
            while let Some(result) = inbound_stream.next().await {
                match result {
                    Ok(msg) => match TunnelFrame::try_from(msg) {
                        // The agent sends an initial Hello; skip it — port is already
                        // assigned by the server from the x-rabbit-port metadata header.
                        Ok(TunnelFrame::Hello { .. }) => {}
                        Ok(frame) => {
                            if inbound_tx.send(frame).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => {}
                    },
                    Err(e) => {
                        warn!(%e, "gRPC stream error from agent");
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(out_rx)))
    }

    async fn list_services(
        &self,
        request: Request<ListServicesRequest>,
    ) -> Result<Response<ListServicesResponse>, Status> {
        let r = request.into_inner();
        let caller_fp = validate_auth_fields(self.inner.as_ref(), r.ts, &r.auth).await?;
        let services = self.inner.list_services(caller_fp).await;
        let entries: Vec<ServiceEntry> = services
            .into_iter()
            .map(|s| ServiceEntry::from((s.port, s.namespace.as_str(), s.connected_at)))
            .collect();
        Ok(Response::new(ListServicesResponse { services: entries }))
    }

    async fn get_ports(
        &self,
        request: Request<GetPortsRequest>,
    ) -> Result<Response<GetPortsResponse>, Status> {
        let r = request.into_inner();
        let caller_fp = validate_auth_fields(self.inner.as_ref(), r.ts, &r.auth).await?;
        let ports: Vec<u32> = self
            .inner
            .get_ports(&r.name, caller_fp)
            .await
            .into_iter()
            .map(|p| p as u32)
            .collect();
        Ok(Response::new(GetPortsResponse { ports }))
    }
}

async fn validate_auth_fields(
    coordinator: &dyn TunnelCoordinator,
    ts: u64,
    auth_tag: &str,
) -> Result<Option<[u8; 32]>, Status> {
    let _ = (coordinator, ts, auth_tag);
    Ok(None) // overridden in TonicCoordinatorWithAuth
}

/// Full coordinator adapter when a shared secret is configured.
pub(crate) struct TonicCoordinatorWithAuth {
    inner: Arc<dyn TunnelCoordinator>,
    auth: Option<Arc<crate::auth::Authenticator>>,
}

impl TonicCoordinatorWithAuth {
    pub(crate) fn new(
        inner: Arc<dyn TunnelCoordinator>,
        auth: Option<Arc<crate::auth::Authenticator>>,
    ) -> Self {
        Self { inner, auth }
    }

    fn check_auth(&self, ts: u64, auth_tag: &str) -> Result<Option<[u8; 32]>, Status> {
        if let Some(authenticator) = &self.auth {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            if now.abs_diff(ts) > AUTH_WINDOW_SECS {
                warn!("service query rejected: timestamp out of window");
                return Err(Status::unauthenticated("timestamp out of window"));
            }
            if authenticator.verify(&ts.to_le_bytes(), auth_tag).is_err() {
                warn!("service query rejected: invalid auth");
                return Err(Status::unauthenticated("invalid auth"));
            }
            Ok(Some(authenticator.fingerprint()))
        } else {
            Ok(None)
        }
    }
}

#[tonic::async_trait]
impl Rabbit for TonicCoordinatorWithAuth {
    type TunnelStream = OutStream;

    async fn tunnel(
        &self,
        request: Request<Streaming<TunnelMessage>>,
    ) -> Result<Response<Self::TunnelStream>, Status> {
        TonicCoordinator::new(Arc::clone(&self.inner))
            .tunnel(request)
            .await
    }

    async fn list_services(
        &self,
        request: Request<ListServicesRequest>,
    ) -> Result<Response<ListServicesResponse>, Status> {
        let r = request.into_inner();
        let caller_fp = self.check_auth(r.ts, &r.auth)?;
        let services = self.inner.list_services(caller_fp).await;
        let entries: Vec<ServiceEntry> = services
            .into_iter()
            .map(|s| ServiceEntry::from((s.port, s.namespace.as_str(), s.connected_at)))
            .collect();
        Ok(Response::new(ListServicesResponse { services: entries }))
    }

    async fn get_ports(
        &self,
        request: Request<GetPortsRequest>,
    ) -> Result<Response<GetPortsResponse>, Status> {
        let r = request.into_inner();
        let caller_fp = self.check_auth(r.ts, &r.auth)?;
        let ports: Vec<u32> = self
            .inner
            .get_ports(&r.name, caller_fp)
            .await
            .into_iter()
            .map(|p| p as u32)
            .collect();
        Ok(Response::new(GetPortsResponse { ports }))
    }
}

fn metadata_str(metadata: &tonic::metadata::MetadataMap, key: &str) -> Option<String> {
    metadata.get(key)?.to_str().ok().map(|s| s.to_string())
}
