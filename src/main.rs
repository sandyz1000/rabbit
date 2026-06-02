mod adapter;
mod auth;
mod client;
mod coordinator;
mod domain;
mod proxy;
mod server;
mod shared;

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use clap::{error::ErrorKind, CommandFactory, Parser, Subcommand};
use tracing::warn;

use adapter::agent::AgentTransport;
use auth::Authenticator;
use client::Client;
use server::Server;

#[derive(Parser, Debug)]
#[clap(author, version, about = "rabbit — HTTP tunnel over HTTP/2")]
struct Args {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Starts a local proxy to the remote rabbit server.
    Local {
        /// The local port to expose.
        #[clap(env = "RABBIT_LOCAL_PORT")]
        local_port: u16,

        /// The local host to expose.
        #[clap(short, long, value_name = "HOST", default_value = "localhost")]
        local_host: String,

        /// Address of the remote rabbit server (e.g. https://tider-bore.fly.dev).
        #[clap(short, long, env = "RABBIT_SERVER")]
        to: String,

        /// Optional port hint on the remote server (0 = server assigns any).
        #[clap(short, long, default_value_t = 0)]
        port: u16,

        /// Optional secret for authentication.
        #[clap(short, long, env = "RABBIT_SECRET", hide_env_values = true)]
        secret: Option<String>,

        /// Service name to register under (used for service discovery).
        #[clap(long, env = "RABBIT_SERVICE")]
        service: Option<String>,
    },

    /// Runs the remote proxy server.
    Server {
        /// Minimum accepted virtual port number.
        #[clap(long, default_value_t = 1024, env = "RABBIT_MIN_PORT")]
        min_port: u16,

        /// Maximum accepted virtual port number.
        #[clap(long, default_value_t = 65535, env = "RABBIT_MAX_PORT")]
        max_port: u16,

        /// Optional secret for authentication.
        #[clap(short, long, env = "RABBIT_SECRET", hide_env_values = true)]
        secret: Option<String>,

        /// Port to bind the HTTP/2 server on.
        #[clap(long, default_value_t = 8080, env = "PORT")]
        bind_port: u16,
    },

    /// List active services and their tunnel ports on a remote rabbit server.
    Services {
        /// Address of the remote rabbit server.
        #[clap(short, long, env = "RABBIT_SERVER")]
        to: String,

        /// Secret for authentication (required if server uses --secret).
        #[clap(short, long, env = "RABBIT_SECRET", hide_env_values = true)]
        secret: Option<String>,

        /// Filter to a single service name. Omit to list all services.
        name: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    run(Args::parse().command).await
}

async fn run(command: Command) -> Result<()> {
    match command {
        Command::Local { local_host, local_port, to, port, secret, service } => {
            let mut backoff = Duration::from_secs(1);
            loop {
                match Client::new(&local_host, local_port, &to, port, secret.as_deref(), service.clone()).await {
                    Ok(client) => {
                        backoff = Duration::from_secs(1);
                        if let Err(e) = client.listen().await {
                            warn!(%e, "tunnel disconnected, reconnecting");
                        }
                    }
                    Err(e) => warn!(%e, "failed to connect, retrying"),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
        }

        Command::Server { min_port, max_port, secret, bind_port } => {
            let port_range = min_port..=max_port;
            if port_range.is_empty() {
                Args::command()
                    .error(ErrorKind::InvalidValue, "port range is empty")
                    .exit();
            }
            Server::new(port_range, secret.as_deref()).listen(bind_port).await?;
        }

        Command::Services { to, secret, name } => {
            let auth = secret.as_deref().map(Authenticator::new);
            match &name {
                Some(n) => {
                    let ports = AgentTransport::get_ports(&to, n, auth.as_ref()).await?;
                    if ports.is_empty() {
                        println!("(no ports for service '{n}')");
                    } else {
                        let mut sorted = ports.clone();
                        sorted.sort_unstable();
                        println!("{n}: {}", sorted.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", "));
                    }
                }
                None => {
                    let services = AgentTransport::list_services(&to, auth.as_ref()).await?;
                    let mut map: HashMap<String, Vec<u16>> = HashMap::new();
                    for s in services {
                        map.entry(s.namespace).or_default().push(s.port);
                    }
                    print_services(&map);
                }
            }
        }
    }
    Ok(())
}

fn print_services(services: &HashMap<String, Vec<u16>>) {
    if services.is_empty() {
        println!("(no active services)");
        return;
    }
    let mut entries: Vec<_> = services.iter().collect();
    entries.sort_by_key(|(k, _)| k.as_str());
    for (name, ports) in entries {
        let label = if name.is_empty() { "(anonymous)" } else { name };
        let mut sorted_ports = ports.clone();
        sorted_ports.sort_unstable();
        let port_list: Vec<String> = sorted_ports.iter().map(|p| p.to_string()).collect();
        println!("{label}: {}", port_list.join(", "));
    }
}
