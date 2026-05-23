/// Pure port-assignment logic — no I/O, easily unit-tested.
///
/// Probabilistic strategy from bore: 150 random attempts gives 99.999% success
/// at 85% port-range utilization.
use std::collections::BTreeSet;
use std::ops::RangeInclusive;

use dashmap::DashMap;

use crate::domain::error::ConnectError;

/// Assign a virtual port from the pool.
///
/// * `pool`     – set of ports not currently active (returned on disconnect)
/// * `active`   – ports that are currently in use (authoritative live set)
/// * `range`    – allowed port range
/// * `requested`– client hint (0 = server picks any)
pub(crate) fn assign_port(
    pool: &mut BTreeSet<u16>,
    active: &DashMap<u16, Option<String>>,
    range: &RangeInclusive<u16>,
    requested: u16,
) -> Result<u16, ConnectError> {
    if requested > 0 {
        if !range.contains(&requested) {
            return Err(ConnectError::PortOutOfRange);
        }
        if active.contains_key(&requested) {
            return Err(ConnectError::PortInUse);
        }
        pool.remove(&requested);
        Ok(requested)
    } else {
        for _ in 0..150 {
            let port = fastrand::u16(range.clone());
            if !active.contains_key(&port) {
                pool.remove(&port);
                return Ok(port);
            }
        }
        Err(ConnectError::PortExhausted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pool(range: RangeInclusive<u16>) -> BTreeSet<u16> {
        range.clone().collect()
    }

    #[test]
    fn assigns_requested_port() {
        let range = 3000u16..=4000;
        let mut pool = make_pool(range.clone());
        let active: DashMap<u16, Option<String>> = DashMap::new();
        let port = assign_port(&mut pool, &active, &range, 3500).unwrap();
        assert_eq!(port, 3500);
        // Pool should no longer contain the assigned port.
        assert!(!pool.contains(&3500));
    }

    #[test]
    fn rejects_out_of_range() {
        let range = 3000u16..=4000;
        let mut pool = make_pool(range.clone());
        let active: DashMap<u16, Option<String>> = DashMap::new();
        let err = assign_port(&mut pool, &active, &range, 2999).unwrap_err();
        assert_eq!(err, ConnectError::PortOutOfRange);
    }

    #[test]
    fn rejects_in_use() {
        let range = 3000u16..=4000;
        let mut pool = make_pool(range.clone());
        let active: DashMap<u16, Option<String>> = DashMap::new();
        active.insert(3500, Some("existing".into()));
        let err = assign_port(&mut pool, &active, &range, 3500).unwrap_err();
        assert_eq!(err, ConnectError::PortInUse);
    }

    #[test]
    fn assigns_random_port_when_zero() {
        let range = 3000u16..=4000;
        let mut pool = make_pool(range.clone());
        let active: DashMap<u16, Option<String>> = DashMap::new();
        let port = assign_port(&mut pool, &active, &range, 0).unwrap();
        assert!(range.contains(&port));
        assert!(!active.contains_key(&port));
    }

    #[test]
    fn exhausted_when_all_active() {
        // Use a tiny range so we can fill it completely.
        let range = 3000u16..=3002;
        let mut pool = make_pool(range.clone());
        let active: DashMap<u16, Option<String>> = DashMap::new();
        for p in range.clone() {
            active.insert(p, None);
        }
        let err = assign_port(&mut pool, &active, &range, 0).unwrap_err();
        assert_eq!(err, ConnectError::PortExhausted);
    }
}
