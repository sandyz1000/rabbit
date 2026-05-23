use std::collections::HashMap;

use bytes::Bytes;

/// Opaque identifier for a connected agent (assigned port, stringified).
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AgentId(pub String);

/// Logical app / service group name — one namespace can have multiple agents.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Namespace(pub String);

/// Per-request correlation identifier (UUID string).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequestId(pub String);

/// HMAC proof carried by agent-connect and service-discovery calls.
#[derive(Debug, Clone)]
pub struct AuthProof {
    pub ts: u64,
    pub tag: String,
}

/// Domain frame — replaces the generated `TunnelMessage` oneof across all non-adapter code.
#[derive(Debug)]
pub enum TunnelFrame {
    /// Server → agent: assigned virtual port after successful connect.
    Hello { assigned_port: u16 },
    /// Server → agent: an inbound HTTP request to relay.
    Inbound(InboundRequest),
    /// Agent → server: HTTP response status + headers.
    ResponseMeta {
        request_id: RequestId,
        status: u32,
        headers: HashMap<String, String>,
    },
    /// Agent → server: one body chunk.
    ResponseChunk { request_id: RequestId, data: Bytes },
    /// Agent → server: end-of-response marker.
    ResponseEnd { request_id: RequestId },
    /// Bidirectional keep-alive.
    Heartbeat,
    /// Error signal — either side may send.
    Error(String),
}

/// An inbound HTTP request forwarded to an agent.
#[derive(Debug, Clone)]
pub struct InboundRequest {
    pub id: RequestId,
    pub method: String,
    pub path: String,
    pub query: String,
    pub headers: HashMap<String, String>,
    pub body: Bytes,
}

/// One entry returned by service-discovery queries.
#[derive(Debug, Clone)]
pub struct ServiceInfo {
    pub namespace: String,
    pub port: u16,
    pub connected_at: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_id_equality() {
        let a = RequestId("abc".into());
        let b = RequestId("abc".into());
        let c = RequestId("xyz".into());
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn tunnel_frame_hello_roundtrip_fields() {
        let frame = TunnelFrame::Hello {
            assigned_port: 4321,
        };
        match frame {
            TunnelFrame::Hello { assigned_port } => assert_eq!(assigned_port, 4321),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn inbound_request_clone() {
        let req = InboundRequest {
            id: RequestId("r1".into()),
            method: "GET".into(),
            path: "/ping".into(),
            query: String::new(),
            headers: HashMap::new(),
            body: Bytes::new(),
        };
        let cloned = req.clone();
        assert_eq!(cloned.method, "GET");
        assert_eq!(cloned.path, "/ping");
    }
}
