//! Job id generation: 12 hex chars, no `rand` dependency.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A 12-hex-character (48-bit) id, unique enough for human-paced job creation:
/// the wall clock in nanoseconds, the pid, and a process-lifetime counter are
/// mixed so two ids minted in the same nanosecond still differ.
pub fn new_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let count = COUNTER.fetch_add(1, Ordering::Relaxed) as u128;
    let pid = std::process::id() as u128;
    let mixed = nanos
        ^ count.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ (pid << 17);
    format!("{:012x}", (mixed as u64) & 0xFFFF_FFFF_FFFF)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn id_is_twelve_lowercase_hex() {
        let id = new_id();
        assert_eq!(id.len(), 12, "got {id}");
        assert!(id.chars().all(|c| c.is_ascii_hexdigit() && !c.is_uppercase()));
    }

    #[test]
    fn ids_are_unique_in_a_tight_loop() {
        let set: HashSet<String> = (0..5000).map(|_| new_id()).collect();
        assert_eq!(set.len(), 5000, "collisions occurred");
    }
}
