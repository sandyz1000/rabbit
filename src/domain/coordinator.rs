use std::collections::HashMap;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use super::error::{ConnectError, RoutingError};
use super::types::{AuthProof, InboundRequest, ServiceInfo, TunnelFrame};

/// Server-side handle to a newly registered agent.
#[derive(Debug)]
pub struct AgentHandle {
    #[allow(dead_code)]
    pub assigned_port: u16,
    /// Coordinator → gRPC stream: coordinator pushes frames here; the adapter drains and sends.
    pub outbound_rx: mpsc::Receiver<TunnelFrame>,
    /// gRPC stream → coordinator: adapter pushes decoded frames here; relay_loop receives them.
    pub inbound_tx: mpsc::Sender<TunnelFrame>,
}

/// Head of an HTTP response — delivered via oneshot once ResponseMeta arrives.
pub struct ResponseHead {
    pub status: u32,
    pub headers: HashMap<String, String>,
    /// Body chunks stream in as ResponseChunk frames arrive; closed on ResponseEnd.
    pub body_rx: mpsc::Receiver<Bytes>,
}

/// Wraps the oneshot receiver for a `ResponseHead`.
pub struct ResponseReceiver(pub oneshot::Receiver<ResponseHead>);

/// Core coordinator interface — implemented by `RegistryCoordinator`, testable via mocks.
#[async_trait]
pub trait TunnelCoordinator: Send + Sync {
    /// Register a new agent connection. Returns an `AgentHandle` on success.
    async fn register_agent(
        &self,
        namespace: String,
        auth: Option<AuthProof>,
        requested_port: u16,
    ) -> Result<AgentHandle, ConnectError>;

    /// Route an inbound HTTP request to the agent on `port`.
    async fn route_request(
        &self,
        port: u16,
        req: InboundRequest,
    ) -> Result<ResponseReceiver, RoutingError>;

    /// List all visible service entries filtered by caller's auth fingerprint.
    async fn list_services(&self, caller_fingerprint: Option<[u8; 32]>) -> Vec<ServiceInfo>;

    /// Return all ports registered under `namespace` visible to the caller.
    async fn get_ports(&self, namespace: &str, caller_fingerprint: Option<[u8; 32]>) -> Vec<u16>;
}
