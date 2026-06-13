use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use tracing::info;

use crate::auth::Authenticator;
use crate::client::tunnel_cluster::TunnelCluster;
use crate::config::ClientConfig;
use crate::domain::types::RegistrationInfo;

pub mod tunnel_cluster;

pub struct Client {
    config: ClientConfig,
}

impl Client {
    pub fn new(config: ClientConfig) -> Self {
        Self { config }
    }

    /// Register with the server and start the tunnel loop.
    ///
    /// Returns the assigned URL on successful registration.
    pub async fn connect(&self) -> anyhow::Result<RegistrationInfo> {
        let info = self.register().await?;
        info!(
            "tunnel '{}' active at {} ({}:{})",
            info.id, info.url, info.tunnel_host, info.tunnel_port
        );
        Ok(info)
    }

    /// Resolve the server's tunnel TCP address from registration info.
    pub async fn start_tunnel(&self, info: &RegistrationInfo) -> anyhow::Result<()> {
        let addr: SocketAddr = format!("{}:{}", info.tunnel_host, info.tunnel_port)
            .parse()
            .context("invalid tunnel address")?;

        let cluster = TunnelCluster {
            id: info.id.clone(),
            server_addr: addr,
            local_host: self.config.local_host.clone(),
            local_port: self.config.local_port,
        };

        cluster.run().await;
        Ok(())
    }

    async fn register(&self) -> anyhow::Result<RegistrationInfo> {
        let url = format!(
            "{}/api/tunnel?id={}",
            self.config.server_url.trim_end_matches('/'),
            self.config.tunnel_id
        );

        let mut req = reqwest_register_request(&url)?;

        if let Some(secret) = &self.config.secret {
            let auth = Authenticator::new(secret);
            let ts = now_secs();
            let tag = auth.sign(&ts.to_le_bytes());
            req = req
                .header("x-rabbit-ts", ts.to_string())
                .header("x-rabbit-auth", tag);
        }

        let resp = req.send().await.context("registration request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("server rejected registration ({status}): {body}");
        }

        resp.json::<RegistrationInfo>()
            .await
            .context("failed to parse registration response")
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn reqwest_register_request(url: &str) -> anyhow::Result<reqwest::RequestBuilder> {
    let client = reqwest::Client::builder()
        .build()
        .context("failed to build http client")?;
    Ok(client.get(url))
}
