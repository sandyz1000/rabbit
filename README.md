# rabbit

Expose a local HTTP service through a remote server via a persistent HTTP/2 tunnel.

Built specifically for [Fly.io](https://fly.io), where only ports 80 and 443 are publicly reachable and TLS is terminated at the edge. Raw TCP tunnels (bore, frp) don't work there — rabbit uses HTTP/2 bidirectional streaming, which passes cleanly through Fly's proxy.

## How it works

```
browser → Fly edge (TLS) → rabbit server (HTTP/2) → tunnel stream → rabbit agent → local service
```

The agent opens an outbound HTTP/2 stream to the server (`POST /rabbit`). The server assigns a virtual port and routes inbound HTTP requests to the right agent via an `X-Tunnel-Port` header. No inbound firewall rules needed on the agent side.

## Protocol

All agent-to-server communication goes through a single endpoint:

```http
POST /rabbit
X-Rabbit-Cmd: tunnel | list_services | get_ports
```

Frames are length-prefixed JSON over the HTTP/2 stream body. No code generation or external tooling required to build.

## Build

```sh
cargo build --release
```

## Usage

**Start the server** (runs on Fly.io or any host):

```sh
rabbit server --bind-port 8080 --secret mysecret
```

**Expose a local service** (runs on your machine):

```sh
rabbit local 3000 --to https://your-server.fly.dev --secret mysecret --service myapp
```

This forwards requests arriving at `your-server.fly.dev` (with `X-Tunnel-Port: <assigned>`) to `localhost:3000`.

**List connected services**:

```sh
rabbit services --to https://your-server.fly.dev --secret mysecret
```

## Options

| Flag                        | Env                                   | Default        | Description                                        |
| --------------------------- | ------------------------------------- | -------------- | -------------------------------------------------- |
| `local <port>`              | `RABBIT_LOCAL_PORT`                   | —              | Local port to expose                               |
| `--to`                      | `RABBIT_SERVER`                       | —              | Remote server URL                                  |
| `--secret`                  | `RABBIT_SECRET`                       | none           | Shared HMAC secret                                 |
| `--service`                 | `RABBIT_SERVICE`                      | `""`           | Service name for discovery                         |
| `--port`                    | —                                     | `0`            | Request a specific virtual port (0 = server picks) |
| `--bind-port`               | `PORT`                                | `8080`         | Server listen port                                 |
| `--min-port` / `--max-port` | `RABBIT_MIN_PORT` / `RABBIT_MAX_PORT` | `1024`–`65535` | Virtual port range                                 |

## Fly.io deployment

`fly.toml` must set `h2_backend = true` so Fly forwards HTTP/2 frames to the backend rather than downgrading to HTTP/1.1:

```toml
[[services]]
  internal_port = 8080
  protocol = "tcp"

  [services.concurrency]
    type = "requests"

[http_service]
  internal_port = 8080
  force_https = true
  h2_backend = true
```

## Auth

When `--secret` is set, every agent connection and service-discovery call is authenticated with an HMAC-SHA256 signature over a Unix timestamp. Connections outside a ±30-second window are rejected. Agents with the same secret can see each other's services; agents with different secrets are isolated.
