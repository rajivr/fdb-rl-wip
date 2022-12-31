//! Provides [`RecordContext`] type and associated items.
use crate::scan::{ByteScanLimiter, KeyValueScanLimiter, TimeScanLimiter};

/// Provides context information for use by other types in the Record
/// Layer.
///
/// [`RecordContext`] is a singleton within a FDB transaction. There
/// **must** be only one value of [`RecordContext`] in a FDB
/// transaction.
#[derive(Debug)]
pub struct RecordContext {
    /// Out-of-band [`ByteScanLimiter`].
    byte_scan_limiter: Option<ByteScanLimiter>,
    /// Out-of-band [`KeyValueScanLimiter`].
    keyvalue_scan_limiter: Option<KeyValueScanLimiter>,
    /// Out-of-band [`TimeScanLimiter`].
    time_scan_limiter: Option<TimeScanLimiter>,

    /// Used by `RecordVersion`
    incarnation_version: Option<u64>,
    local_version: u16,
}
