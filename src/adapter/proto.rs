/// Conversions between domain types and generated protobuf types.
/// This is the only file (besides gen/mod.rs) that may import from crate::proto_gen::rabbit.
use bytes::Bytes;

use crate::domain::types::{InboundRequest, RequestId, ServiceInfo, TunnelFrame};
use crate::rabbit::{self as pb, tunnel_message::Payload};

// ── TunnelFrame → TunnelMessage ──────────────────────────────────────────────

impl From<TunnelFrame> for pb::TunnelMessage {
    fn from(frame: TunnelFrame) -> Self {
        let payload = match frame {
            TunnelFrame::Hello { assigned_port } => Payload::Hello(pb::Hello {
                port: assigned_port as u32,
            }),
            TunnelFrame::Inbound(req) => Payload::Request(pb::InboundRequest {
                id: req.id.0,
                method: req.method,
                path: req.path,
                query: req.query,
                headers: req.headers,
                body: req.body.to_vec(),
            }),
            TunnelFrame::ResponseMeta {
                request_id,
                status,
                headers,
            } => Payload::ResponseMeta(pb::ResponseMeta {
                id: request_id.0,
                status,
                headers,
            }),
            TunnelFrame::ResponseChunk { request_id, data } => {
                Payload::ResponseChunk(pb::ResponseChunk {
                    id: request_id.0,
                    data: data.to_vec(),
                })
            }
            TunnelFrame::ResponseEnd { request_id } => {
                Payload::ResponseEnd(pb::ResponseEnd { id: request_id.0 })
            }
            TunnelFrame::Heartbeat => Payload::Heartbeat(pb::Heartbeat {}),
            TunnelFrame::Error(msg) => Payload::Error(pb::ErrorFrame { message: msg }),
        };
        pb::TunnelMessage {
            payload: Some(payload),
        }
    }
}

// ── TunnelMessage → TunnelFrame ──────────────────────────────────────────────

#[derive(Debug)]
pub enum ProtoConvertError {
    EmptyPayload,
}

impl TryFrom<pb::TunnelMessage> for TunnelFrame {
    type Error = ProtoConvertError;

    fn try_from(msg: pb::TunnelMessage) -> Result<Self, ProtoConvertError> {
        match msg.payload.ok_or(ProtoConvertError::EmptyPayload)? {
            Payload::Hello(h) => Ok(TunnelFrame::Hello {
                assigned_port: h.port as u16,
            }),
            Payload::Request(r) => Ok(TunnelFrame::Inbound(InboundRequest {
                id: RequestId(r.id),
                method: r.method,
                path: r.path,
                query: r.query,
                headers: r.headers,
                body: Bytes::from(r.body),
            })),
            Payload::ResponseMeta(m) => Ok(TunnelFrame::ResponseMeta {
                request_id: RequestId(m.id),
                status: m.status,
                headers: m.headers,
            }),
            Payload::ResponseChunk(c) => Ok(TunnelFrame::ResponseChunk {
                request_id: RequestId(c.id),
                data: Bytes::from(c.data),
            }),
            Payload::ResponseEnd(e) => Ok(TunnelFrame::ResponseEnd {
                request_id: RequestId(e.id),
            }),
            Payload::Heartbeat(_) => Ok(TunnelFrame::Heartbeat),
            Payload::Error(e) => Ok(TunnelFrame::Error(e.message)),
        }
    }
}

// ── ServiceInfo ↔ ServiceEntry ────────────────────────────────────────────────

impl From<(u16, &str, u64)> for pb::ServiceEntry {
    fn from((port, namespace, connected_at): (u16, &str, u64)) -> Self {
        pb::ServiceEntry {
            service: namespace.to_string(),
            port: port as u32,
            connected_at,
        }
    }
}

impl From<pb::ServiceEntry> for ServiceInfo {
    fn from(e: pb::ServiceEntry) -> Self {
        ServiceInfo {
            namespace: e.service,
            port: e.port as u16,
            connected_at: e.connected_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn hello_msg(port: u32) -> pb::TunnelMessage {
        pb::TunnelMessage {
            payload: Some(Payload::Hello(pb::Hello { port })),
        }
    }

    #[test]
    fn hello_roundtrip() {
        let frame = TunnelFrame::Hello {
            assigned_port: 5000,
        };
        let msg: pb::TunnelMessage = frame.into();
        let back = TunnelFrame::try_from(msg).unwrap();
        match back {
            TunnelFrame::Hello { assigned_port } => assert_eq!(assigned_port, 5000),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn heartbeat_roundtrip() {
        let frame = TunnelFrame::Heartbeat;
        let msg: pb::TunnelMessage = frame.into();
        let back = TunnelFrame::try_from(msg).unwrap();
        assert!(matches!(back, TunnelFrame::Heartbeat));
    }

    #[test]
    fn error_roundtrip() {
        let frame = TunnelFrame::Error("boom".into());
        let msg: pb::TunnelMessage = frame.into();
        let back = TunnelFrame::try_from(msg).unwrap();
        match back {
            TunnelFrame::Error(s) => assert_eq!(s, "boom"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn response_meta_roundtrip() {
        let mut headers = HashMap::new();
        headers.insert("content-type".into(), "application/json".into());
        let frame = TunnelFrame::ResponseMeta {
            request_id: RequestId("req-1".into()),
            status: 200,
            headers: headers.clone(),
        };
        let msg: pb::TunnelMessage = frame.into();
        let back = TunnelFrame::try_from(msg).unwrap();
        match back {
            TunnelFrame::ResponseMeta {
                request_id,
                status,
                headers: h,
            } => {
                assert_eq!(request_id.0, "req-1");
                assert_eq!(status, 200);
                assert_eq!(h["content-type"], "application/json");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn response_chunk_roundtrip() {
        let data = Bytes::from_static(b"hello world");
        let frame = TunnelFrame::ResponseChunk {
            request_id: RequestId("req-2".into()),
            data: data.clone(),
        };
        let msg: pb::TunnelMessage = frame.into();
        let back = TunnelFrame::try_from(msg).unwrap();
        match back {
            TunnelFrame::ResponseChunk {
                request_id,
                data: d,
            } => {
                assert_eq!(request_id.0, "req-2");
                assert_eq!(d, data);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn response_end_roundtrip() {
        let frame = TunnelFrame::ResponseEnd {
            request_id: RequestId("req-3".into()),
        };
        let msg: pb::TunnelMessage = frame.into();
        let back = TunnelFrame::try_from(msg).unwrap();
        match back {
            TunnelFrame::ResponseEnd { request_id } => assert_eq!(request_id.0, "req-3"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn inbound_request_roundtrip() {
        let mut headers = HashMap::new();
        headers.insert("x-foo".into(), "bar".into());
        let frame = TunnelFrame::Inbound(InboundRequest {
            id: RequestId("req-4".into()),
            method: "POST".into(),
            path: "/api/test".into(),
            query: "a=1".into(),
            headers: headers.clone(),
            body: Bytes::from_static(b"body"),
        });
        let msg: pb::TunnelMessage = frame.into();
        let back = TunnelFrame::try_from(msg).unwrap();
        match back {
            TunnelFrame::Inbound(r) => {
                assert_eq!(r.id.0, "req-4");
                assert_eq!(r.method, "POST");
                assert_eq!(r.path, "/api/test");
                assert_eq!(r.query, "a=1");
                assert_eq!(r.headers["x-foo"], "bar");
                assert_eq!(r.body, b"body".as_ref());
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn empty_payload_error() {
        let msg = pb::TunnelMessage { payload: None };
        assert!(matches!(
            TunnelFrame::try_from(msg),
            Err(ProtoConvertError::EmptyPayload)
        ));
    }

    #[test]
    fn service_entry_roundtrip() {
        let entry = pb::ServiceEntry::from((8080u16, "my-svc", 1234567890u64));
        assert_eq!(entry.service, "my-svc");
        assert_eq!(entry.port, 8080);
        let info = ServiceInfo::from(entry);
        assert_eq!(info.namespace, "my-svc");
        assert_eq!(info.port, 8080);
        assert_eq!(info.connected_at, 1234567890);
    }
}
