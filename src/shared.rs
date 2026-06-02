//! Shared protocol constants used by both server and client.

use std::time::Duration;

pub const RELAY_TIMEOUT: Duration = Duration::from_secs(30);
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

/// Window within which a connect timestamp is considered fresh (replay protection).
pub const AUTH_WINDOW_SECS: u64 = 30;
