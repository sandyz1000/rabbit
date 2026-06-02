/// RegistryCoordinator — the production implementation of TunnelCoordinator.
/// Replaces AppState + the business logic in tunnel.rs and services.rs.
use std::collections::BTreeSet;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::info;
use uuid::Uuid;

use crate::auth::Authenticator;
use crate::coordinator::port_pool::assign_port;
use crate::coordinator::session::relay_loop;
use crate::domain::coordinator::{AgentHandle, ResponseHead, ResponseReceiver, TunnelCoordinator};
use crate::domain::error::{ConnectError, RoutingError};
use crate::domain::types::{AuthProof, InboundRequest, RequestId, ServiceInfo, TunnelFrame};
use crate::shared::{AUTH_WINDOW_SECS, HEARTBEAT_INTERVAL};

/// One connected agent entry stored in the registry.
struct AgentSession {
    namespace: String,
    connected_at: u64,
    auth_fingerprint: Option<[u8; 32]>,
    /// Sends domain TunnelFrames out to the agent's tunnel stream (via the adapter).
    tx: mpsc::Sender<TunnelFrame>,
}

/// The main server-side registry. Holds all connected agents and pending HTTP requests.
pub struct RegistryCoordinator {
    /// port → connected agent session.
    conns: Arc<DashMap<u16, Arc<AgentSession>>>,
    /// port → namespace name (mirrors conns for quick iteration; None = anonymous).
    active: Arc<DashMap<u16, Option<String>>>,
    /// req_id → oneshot sender waiting for ResponseHead.
    pending: Arc<DashMap<String, oneshot::Sender<ResponseHead>>>,
    /// Free-port bookkeeping.
    pool: Arc<Mutex<BTreeSet<u16>>>,
    /// Optional authenticator.
    auth: Option<Arc<Authenticator>>,
    port_range: RangeInclusive<u16>,
}

impl RegistryCoordinator {
    pub fn new(port_range: RangeInclusive<u16>, auth: Option<Arc<Authenticator>>) -> Self {
        assert!(!port_range.is_empty(), "port range must contain at least one port");
        let pool: BTreeSet<u16> = port_range.clone().collect();
        Self {
            conns: Arc::new(DashMap::new()),
            active: Arc::new(DashMap::new()),
            pending: Arc::new(DashMap::new()),
            pool: Arc::new(Mutex::new(pool)),
            auth,
            port_range,
        }
    }

    /// Convenience constructor for tests and CLI where a secret string is available.
    #[allow(dead_code)]
    pub fn with_secret(port_range: RangeInclusive<u16>, secret: Option<&str>) -> Self {
        Self::new(port_range, secret.map(|s| Arc::new(Authenticator::new(s))))
    }
}

/// Returns true if a session's fingerprint is visible to the caller's fingerprint.
fn fp_matches(caller_fp: Option<[u8; 32]>, session_fp: Option<[u8; 32]>) -> bool {
    match (caller_fp, session_fp) {
        (None, _)           => true,
        (Some(cf), Some(sf)) => cf == sf,
        (Some(_), None)     => false,
    }
}

#[async_trait]
impl TunnelCoordinator for RegistryCoordinator {
    async fn register_agent(
        &self,
        namespace: String,
        auth: Option<AuthProof>,
        requested_port: u16,
    ) -> Result<AgentHandle, ConnectError> {
        // Validate auth proof if a secret is configured.
        if let Some(authenticator) = &self.auth {
            match auth {
                None => return Err(ConnectError::Unauthenticated),
                Some(proof) => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    if now.abs_diff(proof.ts) > AUTH_WINDOW_SECS {
                        return Err(ConnectError::TimestampExpired);
                    }
                    if authenticator.verify(&proof.ts.to_le_bytes(), &proof.tag).is_err() {
                        return Err(ConnectError::Unauthenticated);
                    }
                }
            }
        }

        let connected_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let port = {
            let mut pool = self.pool.lock().await;
            assign_port(&mut pool, &self.active, &self.port_range, requested_port)?
        };

        self.active.insert(port, Some(namespace.clone()));
        info!(port, %namespace, "agent registered");

        // outbound channel: coordinator → tunnel stream (Heartbeat, InboundRequest).
        // session.tx (a clone of out_tx) allows route_request() to push Inbound frames to the agent.
        let (out_tx, out_rx) = mpsc::channel::<TunnelFrame>(64);
        // inbound channel: tunnel stream → relay_loop (ResponseMeta/Chunk/End).
        let (in_tx, in_rx) = mpsc::channel::<TunnelFrame>(64);
        let adapter_inbound_tx = in_tx;

        // Send Hello before cloning out_tx into the session.
        let _ = out_tx.send(TunnelFrame::Hello { assigned_port: port }).await;

        // Heartbeat task — keeps Fly.io idle connections alive.
        let out_tx2 = out_tx.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(HEARTBEAT_INTERVAL);
            loop {
                ticker.tick().await;
                if out_tx2.send(TunnelFrame::Heartbeat).await.is_err() {
                    break;
                }
            }
        });

        let auth_fingerprint = self.auth.as_ref().map(|a| a.fingerprint());
        let session = Arc::new(AgentSession {
            namespace: namespace.clone(),
            connected_at,
            auth_fingerprint,
            tx: out_tx,  // route_request() sends Inbound frames here → adapter → agent
        });
        self.conns.insert(port, Arc::clone(&session));

        // Relay loop — dispatches inbound response frames to pending request slots.
        let pending = Arc::clone(&self.pending);
        let conns = Arc::clone(&self.conns);
        let active = Arc::clone(&self.active);
        let pool = Arc::clone(&self.pool);
        tokio::spawn(async move {
            relay_loop(in_rx, pending).await;
            // Cleanup on disconnect.
            conns.remove(&port);
            active.remove(&port);
            pool.lock().await.insert(port);
            info!(port, "agent disconnected");
        });

        Ok(AgentHandle { assigned_port: port, outbound_rx: out_rx, inbound_tx: adapter_inbound_tx })
    }

    async fn route_request(
        &self,
        port: u16,
        req: InboundRequest,
    ) -> Result<ResponseReceiver, RoutingError> {
        let session = self
            .conns
            .get(&port)
            .map(|e| Arc::clone(&*e))
            .ok_or(RoutingError::NoAgent(port))?;

        let req_id = Uuid::new_v4().to_string();

        let (head_tx, head_rx) = oneshot::channel::<ResponseHead>();
        self.pending.insert(req_id.clone(), head_tx);

        // Rewrite the request id with the server-assigned one.
        let frame = TunnelFrame::Inbound(InboundRequest {
            id: RequestId(req_id.clone()),
            ..req
        });

        if session.tx.send(frame).await.is_err() {
            self.pending.remove(&req_id);
            return Err(RoutingError::AgentDisconnected);
        }

        Ok(ResponseReceiver(head_rx))
    }

    async fn list_services(&self, caller_fingerprint: Option<[u8; 32]>) -> Vec<ServiceInfo> {
        self.conns
            .iter()
            .filter(|e| fp_matches(caller_fingerprint, e.auth_fingerprint))
            .map(|e| ServiceInfo {
                namespace:    e.namespace.clone(),
                port:         *e.key(),
                connected_at: e.connected_at,
            })
            .collect()
    }

    async fn get_ports(&self, namespace: &str, caller_fingerprint: Option<[u8; 32]>) -> Vec<u16> {
        self.conns
            .iter()
            .filter(|e| {
                e.namespace == namespace && fp_matches(caller_fingerprint, e.auth_fingerprint)
            })
            .map(|e| *e.key())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fp_matches_none_caller_sees_all() {
        assert!(fp_matches(None, None));
        assert!(fp_matches(None, Some([1u8; 32])));
    }

    #[test]
    fn fp_matches_same_secret() {
        let fp = [42u8; 32];
        assert!(fp_matches(Some(fp), Some(fp)));
    }

    #[test]
    fn fp_matches_different_secrets() {
        assert!(!fp_matches(Some([1u8; 32]), Some([2u8; 32])));
    }

    #[test]
    fn fp_matches_caller_with_secret_no_session_secret() {
        assert!(!fp_matches(Some([1u8; 32]), None));
    }

    #[tokio::test]
    async fn register_assigns_port_and_sends_hello() {
        let coord = RegistryCoordinator::with_secret(5000..=5100, None);
        let mut handle = coord
            .register_agent("my-svc".into(), None, 0)
            .await
            .expect("registration failed");
        assert!(handle.assigned_port >= 5000 && handle.assigned_port <= 5100);

        let first = handle.outbound_rx.recv().await.unwrap();
        assert!(matches!(first, TunnelFrame::Hello { .. }));
    }

    #[tokio::test]
    async fn register_specific_port() {
        let coord = RegistryCoordinator::with_secret(5000..=5100, None);
        let handle = coord
            .register_agent("svc".into(), None, 5050)
            .await
            .expect("registration failed");
        assert_eq!(handle.assigned_port, 5050);
    }

    #[tokio::test]
    async fn register_port_in_use_fails() {
        let coord = RegistryCoordinator::with_secret(5000..=5100, None);
        let _h = coord.register_agent("svc".into(), None, 5050).await.unwrap();
        let err = coord.register_agent("svc2".into(), None, 5050).await.unwrap_err();
        assert_eq!(err, ConnectError::PortInUse);
    }

    #[tokio::test]
    async fn auth_required_rejects_missing_proof() {
        let coord = RegistryCoordinator::with_secret(5000..=5100, Some("secret"));
        let err = coord.register_agent("svc".into(), None, 0).await.unwrap_err();
        assert_eq!(err, ConnectError::Unauthenticated);
    }
}
