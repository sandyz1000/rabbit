use std::fmt;

/// Errors that can occur when an agent attempts to register.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum ConnectError {
    #[error("unauthenticated")]
    Unauthenticated,
    #[error("timestamp out of window")]
    TimestampExpired,
    #[error("tunnel id already in use")]
    IdInUse,
    #[error("tunnel id contains invalid characters")]
    InvalidId,
}

/// Errors that can occur when routing an inbound HTTP request to a tunnel.
#[derive(Debug, PartialEq, Eq)]
pub enum RoutingError {
    NoClient(String),
    NoSocket,
}

impl fmt::Display for RoutingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoClient(id) => write!(f, "no tunnel registered for id '{id}'"),
            Self::NoSocket => write!(f, "tunnel has no available sockets"),
        }
    }
}

/// Low-level tunnel I/O errors.
#[derive(Debug, thiserror::Error)]
pub enum TunnelError {
    #[error("tunnel id read timed out")]
    IdTimeout,
    #[error("tunnel id too long (max 100 bytes)")]
    IdTooLong,
    #[error("unknown tunnel id")]
    UnknownId,
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_error_display_unauthenticated() {
        assert_eq!(ConnectError::Unauthenticated.to_string(), "unauthenticated");
    }

    #[test]
    fn connect_error_display_id_in_use() {
        assert_eq!(ConnectError::IdInUse.to_string(), "tunnel id already in use");
    }

    #[test]
    fn routing_error_display_no_client() {
        assert_eq!(
            RoutingError::NoClient("myapp".into()).to_string(),
            "no tunnel registered for id 'myapp'"
        );
    }

    #[test]
    fn routing_error_display_no_socket() {
        assert_eq!(RoutingError::NoSocket.to_string(), "tunnel has no available sockets");
    }
}
