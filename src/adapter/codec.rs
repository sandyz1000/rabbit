/// Length-prefixed JSON frame codec.
///
/// Wire format per frame:
///   [4 bytes big-endian u32 = JSON length][JSON bytes]
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::domain::types::TunnelFrame;

const MAX_FRAME_LEN: u32 = 16 * 1024 * 1024; // 16 MiB safety cap

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("frame too large: {0} bytes (limit {MAX_FRAME_LEN})")]
    FrameTooLarge(u32),
    #[error("JSON codec error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Encode a single `TunnelFrame` into a length-prefixed JSON byte buffer.
pub fn encode_frame(frame: &TunnelFrame) -> Result<Bytes, CodecError> {
    let json = serde_json::to_vec(frame)?;
    let len = json.len() as u32;
    let mut buf = BytesMut::with_capacity(4 + json.len());
    buf.put_u32(len);
    buf.extend_from_slice(&json);
    Ok(buf.freeze())
}

/// Stateful decoder — buffers incoming bytes and yields complete `TunnelFrame` values.
///
/// Feed chunks of bytes as they arrive from the stream; call `next_frame` after
/// each feed to drain any complete frames.
#[derive(Default)]
pub struct FrameDecoder {
    buf: BytesMut,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn feed(&mut self, chunk: Bytes) {
        self.buf.extend_from_slice(&chunk);
    }

    /// Returns the next complete frame if enough bytes have been buffered.
    pub fn next_frame(&mut self) -> Option<Result<TunnelFrame, CodecError>> {
        if self.buf.len() < 4 {
            return None;
        }
        let len = u32::from_be_bytes(self.buf[..4].try_into().unwrap());
        if len > MAX_FRAME_LEN {
            return Some(Err(CodecError::FrameTooLarge(len)));
        }
        let total = 4 + len as usize;
        if self.buf.len() < total {
            return None;
        }
        self.buf.advance(4);
        let json = self.buf.split_to(len as usize);
        Some(serde_json::from_slice(&json).map_err(CodecError::from))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::domain::types::{InboundRequest, RequestId};

    fn roundtrip(frame: TunnelFrame) -> TunnelFrame {
        let encoded = encode_frame(&frame).expect("encode");
        let mut dec = FrameDecoder::new();
        dec.feed(encoded);
        dec.next_frame().expect("frame present").expect("decode ok")
    }

    #[test]
    fn hello_roundtrip() {
        let f = roundtrip(TunnelFrame::Hello {
            assigned_port: 9000,
        });
        assert!(matches!(
            f,
            TunnelFrame::Hello {
                assigned_port: 9000
            }
        ));
    }

    #[test]
    fn heartbeat_roundtrip() {
        let f = roundtrip(TunnelFrame::Heartbeat);
        assert!(matches!(f, TunnelFrame::Heartbeat));
    }

    #[test]
    fn error_roundtrip() {
        let f = roundtrip(TunnelFrame::Error("oops".into()));
        assert!(matches!(f, TunnelFrame::Error(ref s) if s == "oops"));
    }

    #[test]
    fn response_meta_roundtrip() {
        let mut h = HashMap::new();
        h.insert("content-type".into(), "application/json".into());
        let f = roundtrip(TunnelFrame::ResponseMeta {
            request_id: RequestId("r1".into()),
            status: 200,
            headers: h.clone(),
        });
        match f {
            TunnelFrame::ResponseMeta {
                request_id,
                status,
                headers,
            } => {
                assert_eq!(request_id.0, "r1");
                assert_eq!(status, 200);
                assert_eq!(headers["content-type"], "application/json");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn response_chunk_roundtrip() {
        let data = Bytes::from_static(b"\x00\xde\xad\xbe\xef");
        let f = roundtrip(TunnelFrame::ResponseChunk {
            request_id: RequestId("r2".into()),
            data: data.clone(),
        });
        match f {
            TunnelFrame::ResponseChunk {
                request_id,
                data: d,
            } => {
                assert_eq!(request_id.0, "r2");
                assert_eq!(d, data);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn response_end_roundtrip() {
        let f = roundtrip(TunnelFrame::ResponseEnd {
            request_id: RequestId("r3".into()),
        });
        assert!(matches!(f, TunnelFrame::ResponseEnd { ref request_id } if request_id.0 == "r3"));
    }

    #[test]
    fn inbound_roundtrip() {
        let mut headers = HashMap::new();
        headers.insert("x-foo".into(), "bar".into());
        let f = roundtrip(TunnelFrame::Inbound(InboundRequest {
            id: RequestId("r4".into()),
            method: "POST".into(),
            path: "/api".into(),
            query: "k=v".into(),
            headers,
            body: Bytes::from_static(b"body bytes"),
        }));
        match f {
            TunnelFrame::Inbound(r) => {
                assert_eq!(r.id.0, "r4");
                assert_eq!(r.method, "POST");
                assert_eq!(r.body.as_ref(), b"body bytes");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn partial_feed_then_complete() {
        let encoded = encode_frame(&TunnelFrame::Hello { assigned_port: 42 }).unwrap();
        let mid = encoded.len() / 2;
        let (first, second) = encoded.split_at(mid);

        let mut dec = FrameDecoder::new();
        dec.feed(Bytes::copy_from_slice(first));
        assert!(
            dec.next_frame().is_none(),
            "incomplete — should yield nothing"
        );
        dec.feed(Bytes::copy_from_slice(second));
        let f = dec.next_frame().expect("frame present").expect("decode ok");
        assert!(matches!(f, TunnelFrame::Hello { assigned_port: 42 }));
    }

    #[test]
    fn two_frames_in_one_feed() {
        let a = encode_frame(&TunnelFrame::Heartbeat).unwrap();
        let b = encode_frame(&TunnelFrame::Hello { assigned_port: 7 }).unwrap();
        let mut combined = BytesMut::new();
        combined.extend_from_slice(&a);
        combined.extend_from_slice(&b);

        let mut dec = FrameDecoder::new();
        dec.feed(combined.freeze());
        assert!(matches!(
            dec.next_frame().unwrap().unwrap(),
            TunnelFrame::Heartbeat
        ));
        assert!(matches!(
            dec.next_frame().unwrap().unwrap(),
            TunnelFrame::Hello { assigned_port: 7 }
        ));
        assert!(dec.next_frame().is_none());
    }
}
