# rabbit

Expose a local HTTP service — including WebSocket and SSE — through a remote server via a persistent TCP tunnel.

Built for [Fly.io](https://fly.io), where only ports 80/443 are publicly reachable. Rabbit opens an outbound TCP connection from your machine to the server, so no inbound firewall rules are needed.

## How it works

```text
[Browser / API client]
        │  HTTP / WebSocket  (port 80/443)
        ▼
[rabbit server]  ── routes by subdomain or X-Tunnel-Id ──► [TunnelAgent pool]
                                                                    │
                                                              raw TCP socket
[rabbit tunnel port :8081] ◄──────── id\n ──────────── [rabbit local]
                                                                    │
                                                           [Local service :3000]
```

The agent connects to the server's tunnel port and sends its id as the first line. The server holds that socket in a pool. When a public HTTP request arrives for that id, the server grabs a socket from the pool, pipes the raw HTTP bytes through it, and the agent forwards them to your local service. Because traffic is raw TCP, WebSocket upgrades and SSE streams work with no special handling.

## Quick start

**1. Start the server** (Fly.io or any VPS):

```sh
rabbit server --domain tunnel.example.com --secret mysecret
```

**2. Expose a local port** (your laptop):

```sh
rabbit local 3000 --to https://tunnel.example.com --id myapp --secret mysecret
```

Your service is now reachable at `https://myapp.tunnel.example.com`.

---

## Build

```sh
cargo build --release
# binary at target/release/rabbit
```

---

## Usage

### `rabbit local` — expose a local service

```sh
rabbit local <port> --to <server-url> --id <subdomain>
```

| Flag           | Env                 | Default     | Description                                           |
| -------------- | ------------------- | ----------- | ----------------------------------------------------- |
| `<port>`       | `RABBIT_LOCAL_PORT` | required    | Local port to expose                                  |
| `--to`         | `RABBIT_SERVER`     | required    | Remote server URL (e.g. `https://rabbit.fly.dev`)     |
| `--id`         | `RABBIT_ID`         | required    | Subdomain slug: 4–63 lowercase alphanumeric + hyphen  |
| `--local-host` | —                   | `localhost` | Local host to forward traffic to                      |
| `--secret`     | `RABBIT_SECRET`     | none        | Shared HMAC secret (must match server)                |

The agent reconnects automatically with exponential backoff (1 s → 60 s cap) if the connection drops.

**Examples:**

```sh
# Expose localhost:3000 as myapp.tunnel.example.com
rabbit local 3000 --to https://tunnel.example.com --id myapp --secret s3cr3t

# Expose a service on a different host
rabbit local 5432 --to https://tunnel.example.com --id db --local-host 10.0.0.5

# No secret (open server)
rabbit local 8080 --to http://localhost:8081-server --id test
```

### `rabbit server` — run the tunnel server

```sh
rabbit server [options]
```

| Flag             | Env                   | Default | Description                                                   |
| ---------------- | --------------------- | ------- | ------------------------------------------------------------- |
| `--http-port`    | `PORT`                | `8080`  | Port for the public HTTP server                               |
| `--tunnel-port`  | `RABBIT_TUNNEL_PORT`  | `8081`  | Port agents connect to (TCP)                                  |
| `--domain`       | `RABBIT_DOMAIN`       | none    | Base domain for subdomain routing (e.g. `tunnel.example.com`) |
| `--secret`       | `RABBIT_SECRET`       | none    | Shared HMAC secret; if set, all agents must authenticate      |

Without `--domain`, routing falls back to the `X-Tunnel-Id` header (useful for local dev).

---

## Local development (no domain)

Without a domain configured on the server, route requests using the `X-Tunnel-Id` header:

```sh
# Server (no domain)
rabbit server --http-port 8080 --tunnel-port 8081

# Agent
rabbit local 3000 --to http://localhost:8080 --id myapp

# Test with curl
curl -H "X-Tunnel-Id: myapp" http://localhost:8080/

# WebSocket test
websocat -H "X-Tunnel-Id: myapp" ws://localhost:8080/ws
```

---

## WebSocket and SSE

No extra configuration needed. Because rabbit pipes raw TCP bytes, any protocol that runs over HTTP/1.1 is forwarded transparently:

```sh
# WebSocket (production, subdomain routing)
wscat -c wss://myapp.tunnel.example.com/ws

# Server-Sent Events
curl -N https://myapp.tunnel.example.com/events
```

---

## Monitoring API

```text
GET /health                  → { "ok": true }
GET /api/status              → { "tunnels": 3, "uptime_secs": 3600 }
GET /api/tunnels             → [ { id, url, available_sockets, total_sockets, connected_at } ]
GET /api/tunnels/:id         → { id, url, available_sockets, total_sockets, connected_at }
```

---

## Auth

When `--secret` is set on the server, every registration request must carry an HMAC-SHA256 signature over a Unix timestamp:

```text
X-Rabbit-Ts:   <unix seconds>
X-Rabbit-Auth: <hex(HMAC-SHA256(secret, ts_le_bytes))>
```

Connections outside a ±30-second window are rejected (replay protection). If no secret is set, the server accepts all registrations.

---

## Fly.io deployment

A `fly.toml` and `Dockerfile` are included. Deploy with:

```sh
fly launch --no-deploy
fly secrets set RABBIT_SECRET=<your-secret>
fly deploy
```

The `fly.toml` exposes two services:

- **Port 80/443** — public HTTP (TLS terminated by Fly edge)
- **Port 8081** — agent tunnel connections (raw TCP)

For wildcard subdomain routing (`*.tunnel.example.com → your-fly-app`), add a CNAME in your DNS:

```text
*.tunnel.example.com  CNAME  your-app.fly.dev
```

Then pass `--domain tunnel.example.com` to `rabbit server`.
