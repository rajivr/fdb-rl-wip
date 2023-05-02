use crate::scan::ScanLimiter;

/// The reason why [`LimitManager::try_keyvalue_scan`] returned an
/// error.
// It is consistent to name the variant with the same postfix
// `LimitReached`.
#[allow(clippy::enum_variant_names)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum LimitManagerStoppedReason {
    /// The limit on the number of key-values to scan was reached.
    KeyValueLimitReached,
    /// The limit on the number of bytes to scan was reached.
    ByteLimitReached,
    /// The limit on the amount of time that a scan can take was
    /// reached.
    TimeLimitReached,
}

/// Handles the logic of tracking out-of-band limits.
///
/// Out-of-band limits are:
///  1. Keyvalue limit
///  2. Byte Limit
///  3. Time Limit
///
/// The limit manager merely tracks the limit state and provides
/// coherent answers to the questions:
///
/// 1. Can the cursor advance without exceeding the limit?
/// 2. What limit has been exceeded?
///
/// Unlike Java RecordLayer, we do not have a notion of "free initial
/// pass". This is because in our cursor implementation, we can return
/// a begin continuation in case a out-of-band limit was reached
/// before the cursor could make any progress. The client can then
/// return the begin continuation back and the cursor will start from
/// the beginning.
#[derive(Debug)]
pub(crate) struct LimitManager {
    scan_limiter: ScanLimiter,

    halted_due_to_keyvalue_scan_limit: bool,
    halted_due_to_byte_scan_limit: bool,
    halted_due_to_time_scan_limit: bool,
}

impl LimitManager {
    /// Create a new [`LimitManager`] from a value of [`ScanLimiter`].
    pub(crate) fn new(scan_limiter: ScanLimiter) -> LimitManager {
        LimitManager {
            scan_limiter,

            halted_due_to_keyvalue_scan_limit: false,
            halted_due_to_byte_scan_limit: false,
            halted_due_to_time_scan_limit: false,
        }
    }

    /// Return `true` if keyvalue can be read otherwise return
    /// `false`.
    ///
    /// If we have a `Some(KeyValueScanLimiter)` in `ScanLimiter`, it
    /// also increments its `keyvalues_scanned` field.
    ///
    /// Used by `<KeyValueCursor as Cursor<KeyValue>>::next`.
    pub(crate) fn try_keyvalue_scan(&mut self) -> Result<(), LimitManagerStoppedReason> {
        self.halted_due_to_keyvalue_scan_limit = self
            .scan_limiter
            .get_keyvalue_scan_limiter_ref()
            .map(|x| !x.try_keyvalue_scan())
            .unwrap_or(false);

        self.halted_due_to_byte_scan_limit = self
            .scan_limiter
            .get_byte_scan_limiter_ref()
            .map(|x| !x.try_keyvalue_scan())
            .unwrap_or(false);

        self.halted_due_to_time_scan_limit = self
            .scan_limiter
            .get_time_scan_limiter_ref()
            .map(|x| !x.try_keyvalue_scan())
            .unwrap_or(false);

        // In Java RecordLayer, this part of the code is in
        // `getStoppedReason` method. We just return the same
        // information as `Err` variant.
        //
        // Following quote from this forum post [1] explains the
        // motivation for the order.
        //
        // "The number of keys scanned is a pretty good indicator
        // of poor index definition / selection, that might still
        // slip in under a byte limit (all the index entries /
        // records are small) or time limit (you can get a lot
        // read in a few seconds)."
        //
        // [1] https://forums.foundationdb.org/t/record-layer-design-questions/3468/4
        if self.halted_due_to_keyvalue_scan_limit {
            Err(LimitManagerStoppedReason::KeyValueLimitReached)
        } else if self.halted_due_to_byte_scan_limit {
            Err(LimitManagerStoppedReason::ByteLimitReached)
        } else if self.halted_due_to_time_scan_limit {
            Err(LimitManagerStoppedReason::TimeLimitReached)
        } else {
            Ok(())
        }
    }

    /// Increment the number of bytes scanned by the given number of
    /// bytes.
    ///
    /// Used by `<KeyValueCursor as Cursor<KeyValue>>::next`.
    pub(crate) fn register_scanned_bytes(&self, byte_size: usize) {
        if let Some(x) = self.scan_limiter.get_byte_scan_limiter_ref() {
            x.register_scanned_bytes(byte_size);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;
    use std::time::Duration;

    use crate::scan::{ByteScanLimiter, KeyValueScanLimiter, ScanLimiter, TimeScanLimiter};

    use super::{LimitManager, LimitManagerStoppedReason};

    #[test]
    fn try_keyvalue_scan() {
        let mut limit_manager = LimitManager::new(ScanLimiter::new(
            Some(KeyValueScanLimiter::enforcing(10)),
            Some(ByteScanLimiter::enforcing(100)),
            Some(TimeScanLimiter::new(5)),
        ));

        sleep(Duration::from_millis(10));

        // Out-of-band time limiter has expired.
        assert!(!limit_manager
            .scan_limiter
            .get_time_scan_limiter_ref()
            .unwrap()
            .try_keyvalue_scan());

        // This one should return `Err(...)`
        assert_eq!(
            limit_manager.try_keyvalue_scan(),
            Err(LimitManagerStoppedReason::TimeLimitReached)
        );

        let mut limit_manager = LimitManager::new(ScanLimiter::new(
            Some(KeyValueScanLimiter::enforcing(2)),
            Some(ByteScanLimiter::untracked()),
            Some(TimeScanLimiter::unlimited()),
        ));

        // We should be able to `try_keyvalue_scan()` two times.
        assert_eq!(limit_manager.try_keyvalue_scan(), Ok(()));
        assert_eq!(limit_manager.try_keyvalue_scan(), Ok(()));
        assert_eq!(
            limit_manager.try_keyvalue_scan(),
            Err(LimitManagerStoppedReason::KeyValueLimitReached)
        );

        let mut limit_manager = LimitManager::new(ScanLimiter::new(
            None,
            Some(ByteScanLimiter::enforcing(10)),
            None,
        ));

        limit_manager.register_scanned_bytes(20);

        assert_eq!(
            limit_manager.try_keyvalue_scan(),
            Err(LimitManagerStoppedReason::ByteLimitReached)
        );

        let mut limit_manager = LimitManager::new(ScanLimiter::new(
            None,
            Some(ByteScanLimiter::enforcing(10)),
            None,
        ));

        // Initial pass
        assert_eq!(limit_manager.try_keyvalue_scan(), Ok(()));

        limit_manager.register_scanned_bytes(3);

        assert_eq!(limit_manager.try_keyvalue_scan(), Ok(()));

        limit_manager.register_scanned_bytes(3);

        assert_eq!(limit_manager.try_keyvalue_scan(), Ok(()));

        limit_manager.register_scanned_bytes(3);

        assert_eq!(limit_manager.try_keyvalue_scan(), Ok(()));

        limit_manager.register_scanned_bytes(3);

        assert_eq!(
            limit_manager.try_keyvalue_scan(),
            Err(LimitManagerStoppedReason::ByteLimitReached)
        );

        // When `KeyValueScanLimiter`, `ByteScanLimiter`, and
        // `TimeScanLimiter` limits are reached,
        // `KeyValueLimitReached` should be returned.

        let mut limit_manager = LimitManager::new(ScanLimiter::new(
            Some(KeyValueScanLimiter::enforcing(2)),
            Some(ByteScanLimiter::enforcing(100)),
            Some(TimeScanLimiter::new(5)),
        ));

        assert_eq!(limit_manager.try_keyvalue_scan(), Ok(()));
        assert_eq!(limit_manager.try_keyvalue_scan(), Ok(()));

        sleep(Duration::from_millis(10));
        limit_manager.register_scanned_bytes(100);

        assert!(!limit_manager
            .scan_limiter
            .get_time_scan_limiter_ref()
            .unwrap()
            .try_keyvalue_scan());
        assert!(!limit_manager
            .scan_limiter
            .get_byte_scan_limiter_ref()
            .unwrap()
            .try_keyvalue_scan());

        assert_eq!(
            limit_manager.try_keyvalue_scan(),
            Err(LimitManagerStoppedReason::KeyValueLimitReached)
        );

        // When `ByteScanLimiter` and `TimeScanLimiter` limits are
        // reached, `ByteScanLimiter` should be returned.

        let mut limit_manager = LimitManager::new(ScanLimiter::new(
            None,
            Some(ByteScanLimiter::enforcing(100)),
            Some(TimeScanLimiter::new(5)),
        ));

        sleep(Duration::from_millis(10));
        limit_manager.register_scanned_bytes(100);

        // When both time limit and byte limit has occurred, byte
        // limit error gets signalled,

        assert!(!limit_manager
            .scan_limiter
            .get_time_scan_limiter_ref()
            .unwrap()
            .try_keyvalue_scan());

        assert_eq!(
            limit_manager.try_keyvalue_scan(),
            Err(LimitManagerStoppedReason::ByteLimitReached)
        );
    }

    #[test]
    fn register_scanned_bytes() {
        let limit_manager = LimitManager::new(ScanLimiter::new(
            None,
            Some(ByteScanLimiter::tracking()),
            None,
        ));

        limit_manager.register_scanned_bytes(100);
        limit_manager.register_scanned_bytes(100);

        assert_eq!(
            limit_manager
                .scan_limiter
                .get_byte_scan_limiter_ref()
                .unwrap()
                .get_scanned_bytes(),
            200
        );
    }
}
