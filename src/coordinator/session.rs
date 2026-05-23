/// Per-agent relay loop — dispatches domain frames arriving from the agent stream
/// to the pending request table.
use std::collections::HashMap;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, warn};

use crate::domain::coordinator::ResponseHead;
use crate::domain::types::TunnelFrame;

/// Drive the inbound frame loop for one connected agent.
///
/// `inbound_rx`  – domain frames from the agent (already converted from proto by the adapter).
/// `pending`     – shared table of oneshot senders waiting for `ResponseHead`.
pub(crate) async fn relay_loop(
    mut inbound_rx: mpsc::Receiver<TunnelFrame>,
    pending: std::sync::Arc<DashMap<String, oneshot::Sender<ResponseHead>>>,
) {
    // req_id → body sender; created on ResponseMeta, dropped on ResponseEnd.
    let mut body_senders: HashMap<String, mpsc::Sender<Bytes>> = HashMap::new();

    while let Some(frame) = inbound_rx.recv().await {
        match frame {
            TunnelFrame::ResponseMeta {
                request_id,
                status,
                headers,
            } => {
                let (body_tx, body_rx) = mpsc::channel::<Bytes>(16);
                body_senders.insert(request_id.0.clone(), body_tx);

                if let Some((_, head_tx)) = pending.remove(&request_id.0) {
                    let _ = head_tx.send(ResponseHead {
                        status,
                        headers,
                        body_rx,
                    });
                } else {
                    warn!(id = %request_id.0, "ResponseMeta for unknown request");
                }
            }
            TunnelFrame::ResponseChunk { request_id, data } => {
                if let Some(tx) = body_senders.get(&request_id.0) {
                    if tx.send(data).await.is_err() {
                        body_senders.remove(&request_id.0);
                    }
                }
            }
            TunnelFrame::ResponseEnd { request_id } => {
                body_senders.remove(&request_id.0);
            }
            TunnelFrame::Heartbeat => {}
            TunnelFrame::Error(msg) => error!(%msg, "agent error frame"),
            // Hello and Inbound should not arrive from the agent side.
            _ => warn!("unexpected frame from agent"),
        }
    }
}
