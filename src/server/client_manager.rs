use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::info;

use crate::auth::Authenticator;
use crate::config::AUTH_WINDOW_SECS;
use crate::domain::error::ConnectError;
use crate::domain::types::{AuthProof, RegistrationInfo, ServiceInfo};
use crate::server::tunnel_agent::TunnelAgent;
use crate::server::tunnel_server::TunnelServer;

pub struct TunnelClient {
    pub id: String,
    pub url: String,
    pub agent: Arc<TunnelAgent>,
    pub connected_at: u64,
}

pub struct ClientManager {
    clients: Arc<DashMap<String, Arc<TunnelClient>>>,
    tunnel_server: Arc<TunnelServer>,
    domain: Option<String>,
    tunnel_port: u16,
    auth: Option<Arc<Authenticator>>,
    started_at: u64,
}

impl ClientManager {
    pub fn new(
        tunnel_server: Arc<TunnelServer>,
        domain: Option<String>,
        tunnel_port: u16,
        secret: Option<&str>,
    ) -> Arc<Self> {
        Arc::new(Self {
            clients: Arc::new(DashMap::new()),
            tunnel_server,
            domain,
            tunnel_port,
            auth: secret.map(|s| Arc::new(Authenticator::new(s))),
            started_at: now_secs(),
        })
    }

    /// Register a new tunnel agent. Returns connection details on success.
    pub async fn register(
        self: &Arc<Self>,
        id: &str,
        proof: Option<AuthProof>,
    ) -> Result<RegistrationInfo, ConnectError> {
        // Validate auth if a secret is configured.
        if let Some(auth) = &self.auth {
            let Some(p) = proof else {
                return Err(ConnectError::Unauthenticated);
            };
            let window = AUTH_WINDOW_SECS;
            let now = now_secs();
            if now.abs_diff(p.ts) > window {
                return Err(ConnectError::TimestampExpired);
            }
            if auth.verify(&p.ts.to_le_bytes(), &p.tag).is_err() {
                return Err(ConnectError::Unauthenticated);
            }
        }

        // Validate the requested id: 4-63 lowercase alphanumeric + hyphen.
        if !is_valid_id(id) {
            return Err(ConnectError::InvalidId);
        }

        if self.clients.contains_key(id) {
            return Err(ConnectError::IdInUse);
        }

        let agent = TunnelAgent::new();
        let url = self.build_url(id);

        // Create an mpsc channel so TunnelServer can push sockets to this agent.
        let (tx, mut rx) = mpsc::channel::<TcpStream>(16);
        self.tunnel_server.register(id, tx);

        let agent_clone = Arc::clone(&agent);
        tokio::spawn(async move {
            while let Some(stream) = rx.recv().await {
                agent_clone.on_socket(stream).await;
            }
        });

        let client = Arc::new(TunnelClient {
            id: id.to_owned(),
            url: url.clone(),
            agent,
            connected_at: now_secs(),
        });

        self.clients.insert(id.to_owned(), client);
        info!("registered tunnel '{id}' at {url}");

        let tunnel_host = self
            .domain
            .clone()
            .unwrap_or_else(|| "localhost".to_owned());

        Ok(RegistrationInfo {
            id: id.to_owned(),
            url,
            tunnel_host,
            tunnel_port: self.tunnel_port,
        })
    }

    pub fn remove(&self, id: &str) {
        self.clients.remove(id);
        self.tunnel_server.unregister(id);
        info!("removed tunnel '{id}'");
    }

    pub fn get(&self, id: &str) -> Option<Arc<TunnelClient>> {
        self.clients.get(id).map(|r| Arc::clone(r.value()))
    }

    pub async fn list(&self) -> Vec<ServiceInfo> {
        let mut out = Vec::new();
        for entry in self.clients.iter() {
            let c = entry.value();
            out.push(ServiceInfo {
                id: c.id.clone(),
                url: c.url.clone(),
                available_sockets: c.agent.available_count().await,
                total_sockets: c.agent.total_count(),
                connected_at: c.connected_at,
            });
        }
        out
    }

    pub fn tunnel_count(&self) -> usize {
        self.clients.len()
    }

    pub fn uptime_secs(&self) -> u64 {
        now_secs().saturating_sub(self.started_at)
    }

    fn build_url(&self, id: &str) -> String {
        match &self.domain {
            Some(domain) => format!("https://{id}.{domain}"),
            None => format!("http://localhost/{id}"),
        }
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn is_valid_id(id: &str) -> bool {
    let len = id.len();
    if len < 4 || len > 63 {
        return false;
    }
    id.chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !id.starts_with('-')
        && !id.ends_with('-')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::tunnel_server::TunnelServer;

    fn make_manager() -> Arc<ClientManager> {
        let ts = Arc::new(TunnelServer::new());
        ClientManager::new(ts, None, 8081, None)
    }

    #[tokio::test]
    async fn client_manager_should_register_valid_id() {
        let m = make_manager();
        let info = m.register("myapp", None).await;
        assert!(info.is_ok());
        assert_eq!(info.unwrap().id, "myapp");
    }

    #[tokio::test]
    async fn client_manager_should_reject_duplicate_id() {
        let m = make_manager();
        m.register("myapp", None).await.unwrap();
        let second = m.register("myapp", None).await;
        assert_eq!(second.unwrap_err(), ConnectError::IdInUse);
    }

    #[tokio::test]
    async fn client_manager_should_reject_invalid_id_too_short() {
        let m = make_manager();
        let err = m.register("ab", None).await.unwrap_err();
        assert_eq!(err, ConnectError::InvalidId);
    }

    #[tokio::test]
    async fn client_manager_should_reject_invalid_id_uppercase() {
        let m = make_manager();
        let err = m.register("MyApp", None).await.unwrap_err();
        assert_eq!(err, ConnectError::InvalidId);
    }

    #[tokio::test]
    async fn client_manager_should_remove_tunnel() {
        let m = make_manager();
        m.register("myapp", None).await.unwrap();
        assert!(m.get("myapp").is_some());
        m.remove("myapp");
        assert!(m.get("myapp").is_none());
    }
}
