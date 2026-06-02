use std::collections::HashMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AgentId(pub String);

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Namespace(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestId(pub String);

#[derive(Debug, Clone)]
pub struct AuthProof {
    pub ts: u64,
    pub tag: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum TunnelFrame {
    Hello {
        assigned_port: u16,
    },
    Inbound(InboundRequest),
    ResponseMeta {
        request_id: RequestId,
        status: u32,
        headers: HashMap<String, String>,
    },
    ResponseChunk {
        request_id: RequestId,
        #[serde(with = "bytes_base64")]
        data: Bytes,
    },
    ResponseEnd {
        request_id: RequestId,
    },
    Heartbeat,
    Error(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboundRequest {
    pub id: RequestId,
    pub method: String,
    pub path: String,
    pub query: String,
    pub headers: HashMap<String, String>,
    #[serde(with = "bytes_base64")]
    pub body: Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInfo {
    pub namespace: String,
    pub port: u16,
    pub connected_at: u64,
}

/// Serde helper: serialize `Bytes` as a base64 string in JSON.
mod bytes_base64 {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use bytes::Bytes;
    use serde::{Deserializer, Serializer, de::Error as _};

    pub fn serialize<S: Serializer>(bytes: &Bytes, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&STANDARD.encode(bytes.as_ref()))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Bytes, D::Error> {
        let s: &str = serde::Deserialize::deserialize(de)?;
        STANDARD
            .decode(s)
            .map(Bytes::from)
            .map_err(D::Error::custom)
    }
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

    #[test]
    fn tunnel_frame_json_roundtrip_hello() {
        let frame = TunnelFrame::Hello {
            assigned_port: 5000,
        };
        let json = serde_json::to_string(&frame).unwrap();
        let back: TunnelFrame = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            back,
            TunnelFrame::Hello {
                assigned_port: 5000
            }
        ));
    }

    #[test]
    fn tunnel_frame_json_roundtrip_response_chunk() {
        let frame = TunnelFrame::ResponseChunk {
            request_id: RequestId("req-1".into()),
            data: Bytes::from_static(b"\x00\x01\x02\xff"),
        };
        let json = serde_json::to_string(&frame).unwrap();
        let back: TunnelFrame = serde_json::from_str(&json).unwrap();
        match back {
            TunnelFrame::ResponseChunk { request_id, data } => {
                assert_eq!(request_id.0, "req-1");
                assert_eq!(data.as_ref(), b"\x00\x01\x02\xff");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn tunnel_frame_json_roundtrip_inbound() {
        let mut headers = HashMap::new();
        headers.insert("content-type".into(), "text/plain".into());
        let frame = TunnelFrame::Inbound(InboundRequest {
            id: RequestId("req-2".into()),
            method: "POST".into(),
            path: "/api".into(),
            query: "a=1".into(),
            headers,
            body: Bytes::from_static(b"hello"),
        });
        let json = serde_json::to_string(&frame).unwrap();
        let back: TunnelFrame = serde_json::from_str(&json).unwrap();
        match back {
            TunnelFrame::Inbound(r) => {
                assert_eq!(r.id.0, "req-2");
                assert_eq!(r.body.as_ref(), b"hello");
            }
            _ => panic!("wrong variant"),
        }
    }
}
