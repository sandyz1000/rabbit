//! Shared constants for the rabbit HTTP tunnel protocol.
//!
//! Message types are now defined in rabbit.proto (compiled by build.rs).
//! This file retains only the protocol constants used by both server and client.

use std::time::Duration;

/// Timeout for relay requests to the local service.
pub const RELAY_TIMEOUT: Duration = Duration::from_secs(30);

/// Interval between gRPC heartbeat frames (keeps Fly.io proxy from closing idle streams).
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

/// Window within which a connect timestamp is considered fresh (replay protection).
pub const AUTH_WINDOW_SECS: u64 = 30;
