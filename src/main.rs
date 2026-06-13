mod auth;
mod client;
mod config;
mod domain;
mod server;

use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing::warn;

use crate::client::Client;
use crate::config::{ClientConfig, ServerConfig};

#[derive(Parser, Debug)]
#[clap(author, version, about = "rabbit — HTTP/WebSocket tunnel")]
struct Args {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Exposes a local port through the remote rabbit server.
    Local {
        /// The local port to expose.
        #[clap(env = "RABBIT_LOCAL_PORT")]
        local_port: u16,

        /// Local host to forward traffic to.
        #[clap(long, default_value = "localhost")]
        local_host: String,

        /// URL of the remote rabbit server (e.g. https://rabbit.fly.dev).
        #[clap(short, long, env = "RABBIT_SERVER")]
        to: String,

        /// Subdomain/id to register (must be 4-63 lowercase alphanumeric + hyphen).
        #[clap(long, env = "RABBIT_ID")]
        id: String,

        /// Optional shared secret for authentication.
        #[clap(short, long, env = "RABBIT_SECRET", hide_env_values = true)]
        secret: Option<String>,
    },

    /// Runs the remote rabbit server.
    Server {
        /// Port for the public HTTP server.
        #[clap(long, default_value_t = 8080, env = "PORT")]
        http_port: u16,

        /// Port for the agent TCP tunnel connections.
        #[clap(long, default_value_t = 8081, env = "RABBIT_TUNNEL_PORT")]
        tunnel_port: u16,

        /// Base domain for tunnel subdomains (e.g. tunnel.example.com).
        /// Without this, routing falls back to the X-Tunnel-Id header.
        #[clap(long, env = "RABBIT_DOMAIN")]
        domain: Option<String>,

        /// Optional shared secret for authentication.
        #[clap(short, long, env = "RABBIT_SECRET", hide_env_values = true)]
        secret: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rabbit=info".parse().unwrap()),
        )
        .init();

    run(Args::parse().command).await
}

async fn run(command: Command) -> Result<()> {
    match command {
        Command::Local {
            local_port,
            local_host,
            to,
            id,
            secret,
        } => {
            let config = ClientConfig {
                server_url: to,
                tunnel_id: id,
                local_host,
                local_port,
                secret,
            };
            local_loop(config).await;
        }

        Command::Server {
            http_port,
            tunnel_port,
            domain,
            secret,
        } => {
            let config = ServerConfig {
                http_port,
                tunnel_port,
                domain,
                secret,
                max_sockets: crate::config::DEFAULT_MAX_SOCKETS,
            };
            server::run(config).await?;
        }
    }
    Ok(())
}

/// Reconnect loop with exponential backoff (1s → 60s cap).
async fn local_loop(config: ClientConfig) {
    let mut backoff = Duration::from_secs(1);
    loop {
        let client = Client::new(config.clone());
        match client.connect().await {
            Ok(info) => {
                backoff = Duration::from_secs(1);
                // start_tunnel loops internally; returns only on fatal error.
                if let Err(e) = client.start_tunnel(&info).await {
                    warn!("tunnel error: {e}");
                }
            }
            Err(e) => {
                warn!("connection failed: {e}, retrying in {backoff:?}");
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(60));
    }
}
