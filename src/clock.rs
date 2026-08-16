//! Shared wall-clock source for expiration and lifecycle timestamps.

use std::time::{SystemTime, UNIX_EPOCH};

#[inline]
pub fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
