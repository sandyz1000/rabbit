use std::fmt;

/// Errors that can occur when an agent attempts to connect.
#[derive(Debug, PartialEq, Eq)]
pub enum ConnectError {
    Unauthenticated,
    TimestampExpired,
    PortExhausted,
    PortInUse,
    PortOutOfRange,
}

impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unauthenticated => write!(f, "unauthenticated"),
            Self::TimestampExpired => write!(f, "timestamp out of window"),
            Self::PortExhausted => write!(f, "failed to find an available port"),
            Self::PortInUse => write!(f, "port already in use"),
            Self::PortOutOfRange => write!(f, "port number not in allowed range"),
        }
    }
}

/// Errors that can occur when routing an inbound HTTP request to an agent.
#[derive(Debug, PartialEq, Eq)]
pub enum RoutingError {
    NoAgent(u16),
    AgentDisconnected,
}

impl fmt::Display for RoutingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoAgent(p) => write!(f, "no tunnel agent on port {p}"),
            Self::AgentDisconnected => write!(f, "tunnel agent disconnected"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_error_display() {
        assert_eq!(ConnectError::Unauthenticated.to_string(), "unauthenticated");
        assert_eq!(
            ConnectError::PortExhausted.to_string(),
            "failed to find an available port"
        );
        assert_eq!(ConnectError::PortInUse.to_string(), "port already in use");
        assert_eq!(
            ConnectError::PortOutOfRange.to_string(),
            "port number not in allowed range"
        );
    }

    #[test]
    fn routing_error_display() {
        assert_eq!(
            RoutingError::NoAgent(8080).to_string(),
            "no tunnel agent on port 8080"
        );
        assert_eq!(
            RoutingError::AgentDisconnected.to_string(),
            "tunnel agent disconnected"
        );
    }
}
