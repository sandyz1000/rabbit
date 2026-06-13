use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthProof {
    pub ts: u64,
    pub tag: String,
}

/// Returned to the agent after successful registration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistrationInfo {
    pub id: String,
    pub url: String,
    pub tunnel_host: String,
    pub tunnel_port: u16,
}

/// Per-tunnel status entry used in the monitoring API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInfo {
    pub id: String,
    pub url: String,
    pub available_sockets: usize,
    pub total_sockets: usize,
    pub connected_at: u64,
}

/// Top-level status returned by GET /api/status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusInfo {
    pub tunnels: usize,
    pub uptime_secs: u64,
}
