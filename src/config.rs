use std::time::Duration;

pub const AUTH_WINDOW_SECS: u64 = 30;
pub const TUNNEL_ID_READ_TIMEOUT: Duration = Duration::from_secs(5);
pub const TUNNEL_ID_MAX_BYTES: usize = 100;
pub const DEFAULT_MAX_SOCKETS: usize = 10;

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub http_port: u16,
    pub tunnel_port: u16,
    pub domain: Option<String>,
    pub secret: Option<String>,
    pub max_sockets: usize,
}

#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub server_url: String,
    pub tunnel_id: String,
    pub local_host: String,
    pub local_port: u16,
    pub secret: Option<String>,
}
