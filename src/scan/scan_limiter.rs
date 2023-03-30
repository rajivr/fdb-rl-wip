use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Used internally by [`KeyValueScanLimiter`] and
/// [`ByteScanLimiter`].
#[derive(Debug, Clone, PartialEq)]
enum LimiterMode {
    Enforcing,
    Tracking,
    Untracked,
}

/// Used internally by [`TimeScanLimiter`].
#[derive(Debug, Clone, PartialEq)]
enum TimeLimiterMode {
    Limited,
    Unlimited,
}

/// Track the number of keyvalues scanned up to some limit, after
/// which scans should not be allowed.
//
// In Java RecordLayer, the equivalent class is
// `RecordScanLimiter`. We are renaming it because the use-case of
// this limiter is to only operates on `KeyValue`s. If
// `try_keyvalue_scan()` returns `true` then `keyvalues_scanned` is
// incremented.
//
// The motivation for having a `KeyValueScanLimiter` is
// subtle. Quoting from this forum post [1]
//
// "The number of keys scanned is a pretty good indicator of poor
// index definition / selection, that might still slip in under a byte
// limit (all the index entries / records are small) or time limit
// (you can get a lot read in a few seconds)."
//
// Also, see comment mentioned for type `KeyValueCursorBuilder`
// regarding `PartialEq` and unit testing.
//
// [1] https://forums.foundationdb.org/t/record-layer-design-questions/3468/4
#[derive(Debug, Clone)]
pub struct KeyValueScanLimiter {
    limiter_mode: LimiterMode,

    limit: usize,
    keyvalues_scanned: Arc<AtomicUsize>,
}

impl KeyValueScanLimiter {
    /// Create a value of [`KeyValueScanLimiter`] that enforces a maximum
    /// number of keyvalues that can be processed in a single scan.
    pub fn enforcing(limit: usize) -> KeyValueScanLimiter {
        KeyValueScanLimiter {
            limiter_mode: LimiterMode::Enforcing,
            limit,
            keyvalues_scanned: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a value of [`KeyValueScanLimiter`] that tracks the
    /// number of keyvalues that has been scanned, but does not impose
    /// a limit.
    pub fn tracking() -> KeyValueScanLimiter {
        KeyValueScanLimiter {
            limiter_mode: LimiterMode::Tracking,
            limit: 0,
            keyvalues_scanned: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a value of [`KeyValueScanLimiter`] that neither
    /// enforces nor tracks the number of keyvalues scanned.
    pub fn untracked() -> KeyValueScanLimiter {
        KeyValueScanLimiter {
            limiter_mode: LimiterMode::Untracked,
            limit: 0,
            keyvalues_scanned: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Get the keyvalue scan limit.
    pub fn get_limit(&self) -> usize {
        match self.limiter_mode {
            LimiterMode::Enforcing => self.limit,
            LimiterMode::Tracking => usize::MAX,
            LimiterMode::Untracked => usize::MAX,
        }
    }

    /// Returns the number of keyvalues that have been scanned thus far.
    pub fn get_scanned_keyvalues(&self) -> usize {
        match self.limiter_mode {
            LimiterMode::Enforcing | LimiterMode::Tracking => {
                self.keyvalues_scanned.load(Ordering::Relaxed)
            }
            LimiterMode::Untracked => 0,
        }
    }

    /// Return `true` if keyvalue can be read otherwise return
    /// `false`.
    ///
    /// *Note*: When it returns `true` it **also** increments
    /// `keyvalues_scanned` field.
    ///
    /// Used by `LimitManager::try_keyvalue_scan` (Linking does not work!).
    pub(crate) fn try_keyvalue_scan(&self) -> bool {
        match self.limiter_mode {
            LimiterMode::Enforcing => {
                let limit = self.limit;
                let keyvalues_scanned = self.keyvalues_scanned.load(Ordering::Relaxed);
                match limit.checked_sub(keyvalues_scanned) {
                    Some(x) => {
                        if x == 0 {
                            // limit and keyvalues_scanned are the same value.
                            false
                        } else {
                            self.keyvalues_scanned.fetch_add(1, Ordering::SeqCst);
                            true
                        }
                    }
                    None => {
                        // Overflow occurred
                        false
                    }
                }
            }
            LimiterMode::Tracking => {
                self.keyvalues_scanned.fetch_add(1, Ordering::SeqCst);
                true
            }
            LimiterMode::Untracked => true,
        }
    }

    /// Return whether or not this limiter is actually enforcing the
    /// limit.
    pub fn is_enforcing(&self) -> bool {
        match self.limiter_mode {
            LimiterMode::Enforcing => true,
            LimiterMode::Tracking | LimiterMode::Untracked => false,
        }
    }
}

// It is **totally** unsafe to do this in normal code, as the
// `AtomicUsize` can be mutated by different threads. We are doing it
// only for tests.
//
// See comment mentioned for type `KeyValueCursorBuilder` regarding
// `PartialEq` and unit testing.
#[cfg(test)]
impl PartialEq for KeyValueScanLimiter {
    fn eq(&self, other: &KeyValueScanLimiter) -> bool {
        let self_keyvalues_scanned = unsafe { *(&*self.keyvalues_scanned).as_ptr() };

        let other_keyvalues_scanned = unsafe { *(&*other.keyvalues_scanned).as_ptr() };

        (self_keyvalues_scanned == other_keyvalues_scanned)
            && (self.limiter_mode == other.limiter_mode)
            && (self.limit == other.limit)
    }
}

/// Track the number of bytes scanned up to some limit, after which
/// scans should not be allowed.
#[derive(Debug, Clone)]
pub struct ByteScanLimiter {
    limiter_mode: LimiterMode,

    limit: usize,
    bytes_scanned: Arc<AtomicUsize>,
}

impl ByteScanLimiter {
    /// Create a value of [`ByteScanLimiter`] that enforces a maximum
    /// number of bytes that can be processed in a single scan.
    pub fn enforcing(limit: usize) -> ByteScanLimiter {
        ByteScanLimiter {
            limiter_mode: LimiterMode::Enforcing,
            limit,
            bytes_scanned: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a value of [`ByteScanLimiter`] that tracks the number
    /// of bytes that has been scanned, but does not impose a limit.
    pub fn tracking() -> ByteScanLimiter {
        ByteScanLimiter {
            limiter_mode: LimiterMode::Tracking,
            limit: 0,
            bytes_scanned: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a value of [`ByteScanLimiter`] that neither enforces
    /// nor tracks the number of bytes scanned.
    pub fn untracked() -> ByteScanLimiter {
        ByteScanLimiter {
            limiter_mode: LimiterMode::Untracked,
            limit: 0,
            bytes_scanned: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Get the byte scan limit.
    pub fn get_limit(&self) -> usize {
        match self.limiter_mode {
            LimiterMode::Enforcing => self.limit,
            LimiterMode::Tracking => usize::MAX,
            LimiterMode::Untracked => usize::MAX,
        }
    }

    /// Returns the number of bytes that have been scanned thus far.
    pub fn get_scanned_bytes(&self) -> usize {
        match self.limiter_mode {
            LimiterMode::Enforcing | LimiterMode::Tracking => {
                self.bytes_scanned.load(Ordering::Relaxed)
            }
            LimiterMode::Untracked => 0,
        }
    }

    /// Return whether or not this limiter is actually enforcing the
    /// limit.
    pub fn is_enforcing(&self) -> bool {
        match self.limiter_mode {
            LimiterMode::Enforcing => true,
            LimiterMode::Tracking | LimiterMode::Untracked => false,
        }
    }

    /// Return `true` if keyvalue can be read otherwise return
    /// `false`.
    ///
    /// *Note*: In Java RecordLayer, this method is called
    /// `hasBytesRemaining`. We are renaming it so that the API is
    /// consistent with `KeyValueScanLimiter` and `ByteScanLimiter`.
    ///
    /// Used by `LimitManager::try_keyvalue_scan` (Linking does not work!).
    pub(crate) fn try_keyvalue_scan(&self) -> bool {
        match self.limiter_mode {
            LimiterMode::Enforcing => {
                let limit = self.limit;
                let bytes_scanned = self.bytes_scanned.load(Ordering::Relaxed);
                match limit.checked_sub(bytes_scanned) {
                    Some(x) => {
                        // if limit and bytes_scanned are the same
                        // value, return `false`, otherwise return
                        // `true`.
                        x != 0
                    }
                    None => {
                        // Overflow occurred
                        false
                    }
                }
            }
            LimiterMode::Tracking | LimiterMode::Untracked => true,
        }
    }

    /// Increment the number of bytes scanned by the given number of
    /// bytes.
    ///
    /// Used by `LimitManager::register_scanned_bytes` (Linking does not work!).
    pub(crate) fn register_scanned_bytes(&self, bytes: usize) {
        if let LimiterMode::Enforcing | LimiterMode::Tracking = self.limiter_mode {
            self.bytes_scanned.fetch_add(bytes, Ordering::SeqCst);
        }
    }
}

/// It is **totally** unsafe to do this in normal code, as the
/// `AtomicUsize` can be mutated by different threads. We are doing it
/// only for tests.
#[cfg(test)]
impl PartialEq for ByteScanLimiter {
    fn eq(&self, other: &ByteScanLimiter) -> bool {
        let self_bytes_scanned = unsafe { *(&*self.bytes_scanned).as_ptr() };

        let other_bytes_scanned = unsafe { *(&*other.bytes_scanned).as_ptr() };

        (self_bytes_scanned == other_bytes_scanned)
            && (self.limiter_mode == other.limiter_mode)
            && (self.limit == other.limit)
    }
}

/// Track time remaining until a given time limit, after which scans
/// should not be allowed.
#[derive(Debug, Clone, PartialEq)]
pub struct TimeScanLimiter {
    limiter_mode: TimeLimiterMode,
    start_instant: Option<Instant>,
    time_limit_duration: Option<Duration>,
}

impl TimeScanLimiter {
    /// Create a [`TimeScanLimiter`] with `time_limit` specified in
    /// milliseconds.
    pub fn new(time_limit: u64) -> TimeScanLimiter {
        TimeScanLimiter {
            limiter_mode: TimeLimiterMode::Limited,
            start_instant: Some(Instant::now()),
            time_limit_duration: Some(Duration::from_millis(time_limit)),
        }
    }

    /// Create an unlimited [`TimeScanLimiter`]
    pub fn unlimited() -> TimeScanLimiter {
        TimeScanLimiter {
            limiter_mode: TimeLimiterMode::Unlimited,
            start_instant: None,
            time_limit_duration: None,
        }
    }

    /// Return `true` if keyvalue can be read otherwise return
    /// `false`.
    ///
    /// Used by `LimitManager::try_keyvalue_scan` (Linking does not work!).
    pub(crate) fn try_keyvalue_scan(&self) -> bool {
        if let TimeLimiterMode::Unlimited = self.limiter_mode {
            true
        } else {
            // Limited mode
            //
            // Safety: Safe to unwarp here because in
            // `TimeLimiterMode::Limited` mode, `start_instant` and
            // `time_limit_duration` will be a `Some(x)` value.
            let since_start_duration = self.start_instant.as_ref().unwrap().elapsed();
            let time_limit_duration = self.time_limit_duration.unwrap();

            since_start_duration < time_limit_duration
        }
    }
}

/// An encapsulation of the out-of-band state of scan execution.
///
/// Key-value limit, byte limit, time limit, are considered
/// out-of-band states.
//
// See comment mentioned for type `KeyValueCursorBuilder` regarding
// `PartialEq` and unit testing.
#[cfg(not(test))]
#[derive(Debug, Clone)]
pub struct ScanLimiter {
    keyvalue_scan_limiter: Option<KeyValueScanLimiter>,
    byte_scan_limiter: Option<ByteScanLimiter>,
    time_scan_limiter: Option<TimeScanLimiter>,
}

/// We need to derive `PartialEq` for testing.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq)]
pub struct ScanLimiter {
    keyvalue_scan_limiter: Option<KeyValueScanLimiter>,
    byte_scan_limiter: Option<ByteScanLimiter>,
    time_scan_limiter: Option<TimeScanLimiter>,
}

impl ScanLimiter {
    /// Creates an execute state with a supplied set of resource limiters.
    pub fn new(
        keyvalue_scan_limiter: Option<KeyValueScanLimiter>,
        byte_scan_limiter: Option<ByteScanLimiter>,
        time_scan_limiter: Option<TimeScanLimiter>,
    ) -> ScanLimiter {
        ScanLimiter {
            keyvalue_scan_limiter,
            time_scan_limiter,
            byte_scan_limiter,
        }
    }

    /// If present, extract [`KeyValueScanLimiter`],
    /// [`ByteScanLimiter`], [`TimeScanLimiter`] from [`ScanLimiter`].
    pub fn into_parts(
        self,
    ) -> (
        Option<KeyValueScanLimiter>,
        Option<ByteScanLimiter>,
        Option<TimeScanLimiter>,
    ) {
        let ScanLimiter {
            keyvalue_scan_limiter,
            time_scan_limiter,
            byte_scan_limiter,
        } = self;
        (keyvalue_scan_limiter, byte_scan_limiter, time_scan_limiter)
    }

    /// Get reference to [`KeyValueScanLimiter`] if one is present.
    pub fn get_keyvalue_scan_limiter_ref(&self) -> Option<&KeyValueScanLimiter> {
        self.keyvalue_scan_limiter.as_ref()
    }

    /// Get reference to [`ByteScanLimiter`] if one is present.
    pub fn get_byte_scan_limiter_ref(&self) -> Option<&ByteScanLimiter> {
        self.byte_scan_limiter.as_ref()
    }

    /// Get reference to [`TimeScanLimiter`] if one is present.
    pub fn get_time_scan_limiter_ref(&self) -> Option<&TimeScanLimiter> {
        self.time_scan_limiter.as_ref()
    }
}

#[cfg(test)]
mod tests {
    mod keyvalue_scan_limiter {
        use super::super::KeyValueScanLimiter;

        #[test]
        fn get_limit() {
            let enforcing = KeyValueScanLimiter::enforcing(3);
            let tracking = KeyValueScanLimiter::tracking();
            let untracked = KeyValueScanLimiter::untracked();

            assert_eq!(enforcing.get_limit(), 3);
            assert_eq!(tracking.get_limit(), usize::MAX);
            assert_eq!(untracked.get_limit(), usize::MAX);
        }

        #[test]
        fn get_scanned_keyvalues() {
            let enforcing = KeyValueScanLimiter::enforcing(3);
            let tracking = KeyValueScanLimiter::tracking();
            let untracked = KeyValueScanLimiter::untracked();

            // Intially zero
            assert_eq!(enforcing.get_scanned_keyvalues(), 0);
            assert_eq!(tracking.get_scanned_keyvalues(), 0);
            assert_eq!(untracked.get_scanned_keyvalues(), 0);

            assert!(enforcing.try_keyvalue_scan());
            assert!(enforcing.try_keyvalue_scan());

            assert!(tracking.try_keyvalue_scan());
            assert!(tracking.try_keyvalue_scan());
            assert!(tracking.try_keyvalue_scan());

            assert!(untracked.try_keyvalue_scan());
            assert!(untracked.try_keyvalue_scan());
            assert!(untracked.try_keyvalue_scan());
            assert!(untracked.try_keyvalue_scan());

            // Enforcing and tracking will see changes
            assert_eq!(enforcing.get_scanned_keyvalues(), 2);
            assert_eq!(tracking.get_scanned_keyvalues(), 3);
            assert_eq!(untracked.get_scanned_keyvalues(), 0);

            assert!(enforcing.try_keyvalue_scan());
            assert_eq!(enforcing.get_scanned_keyvalues(), 3);

            assert!(!enforcing.try_keyvalue_scan());
            assert_eq!(enforcing.get_scanned_keyvalues(), 3);
        }

        #[test]
        fn try_keyvalue_scan() {
            let enforcing = KeyValueScanLimiter::enforcing(3);
            let tracking = KeyValueScanLimiter::tracking();
            let untracked = KeyValueScanLimiter::untracked();

            assert!(enforcing.try_keyvalue_scan());
            assert!(enforcing.try_keyvalue_scan());
            assert!(enforcing.try_keyvalue_scan());

            // It should return false now.
            assert!(!enforcing.try_keyvalue_scan());

            // Always returns true
            assert!(tracking.try_keyvalue_scan());
            assert!(tracking.try_keyvalue_scan());
            assert!(tracking.try_keyvalue_scan());

            // Always returns true
            assert!(untracked.try_keyvalue_scan());
            assert!(untracked.try_keyvalue_scan());
            assert!(untracked.try_keyvalue_scan());
            assert!(untracked.try_keyvalue_scan());
        }

        #[test]
        fn is_enforcing() {
            let enforcing = KeyValueScanLimiter::enforcing(3);
            let tracking = KeyValueScanLimiter::tracking();
            let untracked = KeyValueScanLimiter::untracked();

            assert!(enforcing.is_enforcing());
            assert!(!tracking.is_enforcing());
            assert!(!untracked.is_enforcing());
        }
    }

    mod byte_scan_limiter {
        use super::super::ByteScanLimiter;

        #[test]
        fn get_limit() {
            let enforcing = ByteScanLimiter::enforcing(1000);
            let tracking = ByteScanLimiter::tracking();
            let untracked = ByteScanLimiter::untracked();

            assert_eq!(enforcing.get_limit(), 1000);
            assert_eq!(tracking.get_limit(), usize::MAX);
            assert_eq!(untracked.get_limit(), usize::MAX);
        }

        #[test]
        fn get_scanned_bytes() {
            let enforcing = ByteScanLimiter::enforcing(1000);
            let tracking = ByteScanLimiter::tracking();
            let untracked = ByteScanLimiter::untracked();

            // Initially zero
            assert_eq!(enforcing.get_scanned_bytes(), 0);
            assert_eq!(tracking.get_scanned_bytes(), 0);
            assert_eq!(untracked.get_scanned_bytes(), 0);

            enforcing.register_scanned_bytes(100);
            tracking.register_scanned_bytes(100);
            untracked.register_scanned_bytes(100);

            // Enforcing and tracking will see changes
            assert_eq!(enforcing.get_scanned_bytes(), 100);
            assert_eq!(tracking.get_scanned_bytes(), 100);
            assert_eq!(untracked.get_scanned_bytes(), 0);

            enforcing.register_scanned_bytes(100);
            tracking.register_scanned_bytes(100);
            untracked.register_scanned_bytes(100);

            // Check that summming works correctly
            assert_eq!(enforcing.get_scanned_bytes(), 200);
            assert_eq!(tracking.get_scanned_bytes(), 200);
            assert_eq!(untracked.get_scanned_bytes(), 0);
        }

        #[test]
        fn try_keyvalue_scan() {
            let enforcing = ByteScanLimiter::enforcing(1000);
            let tracking = ByteScanLimiter::tracking();
            let untracked = ByteScanLimiter::untracked();

            tracking.register_scanned_bytes(10000);
            untracked.register_scanned_bytes(10000);

            assert!(tracking.try_keyvalue_scan());
            assert!(untracked.try_keyvalue_scan());

            enforcing.register_scanned_bytes(500);

            assert!(enforcing.try_keyvalue_scan());

            enforcing.register_scanned_bytes(500);

            // limit reached
            assert!(!enforcing.try_keyvalue_scan());

            enforcing.register_scanned_bytes(500);

            // limit overflow
            assert!(!enforcing.try_keyvalue_scan());
        }

        #[test]
        fn is_enforcing() {
            let enforcing = ByteScanLimiter::enforcing(1000);
            let tracking = ByteScanLimiter::tracking();
            let untracked = ByteScanLimiter::untracked();

            assert!(enforcing.is_enforcing());
            assert!(!tracking.is_enforcing());
            assert!(!untracked.is_enforcing());
        }

        #[test]
        fn register_scanned_bytes() {
            let enforcing = ByteScanLimiter::enforcing(1000);
            let tracking = ByteScanLimiter::tracking();
            let untracked = ByteScanLimiter::untracked();

            untracked.register_scanned_bytes(1000);
            assert_eq!(untracked.get_scanned_bytes(), 0);

            enforcing.register_scanned_bytes(1000);
            tracking.register_scanned_bytes(1000);

            assert_eq!(enforcing.get_scanned_bytes(), 1000);
            assert_eq!(tracking.get_scanned_bytes(), 1000);

            enforcing.register_scanned_bytes(1000);
            tracking.register_scanned_bytes(1000);

            assert_eq!(enforcing.get_scanned_bytes(), 2000);
            assert_eq!(tracking.get_scanned_bytes(), 2000);
        }

        #[test]
        fn arc() {
            let enforcing1 = ByteScanLimiter::enforcing(1000);
            let enforcing2 = enforcing1.clone();

            enforcing1.register_scanned_bytes(500);
            assert!(enforcing1.try_keyvalue_scan());
            drop(enforcing1);

            enforcing2.register_scanned_bytes(500);
            assert!(!enforcing2.try_keyvalue_scan());
        }
    }

    mod time_scan_limiter {
        use std::thread::sleep;
        use std::time::Duration;

        use super::super::TimeScanLimiter;

        #[test]
        fn try_keyvalue_scan() {
            let unlimited = TimeScanLimiter::unlimited();

            assert!(unlimited.try_keyvalue_scan());

            let limited = TimeScanLimiter::new(100);
            let limited_zero = TimeScanLimiter::new(0);

            assert!(!limited_zero.try_keyvalue_scan());

            sleep(Duration::from_millis(50));
            assert!(limited.try_keyvalue_scan());

            sleep(Duration::from_millis(50));
            assert!(!limited.try_keyvalue_scan());
        }
    }

    mod scan_limiter {
        use super::super::{ByteScanLimiter, KeyValueScanLimiter, ScanLimiter, TimeScanLimiter};

        #[test]
        fn from_parts() {
            let scan_limiter = ScanLimiter::new(
                Some(KeyValueScanLimiter::untracked()),
                Some(ByteScanLimiter::untracked()),
                Some(TimeScanLimiter::unlimited()),
            );

            let (keyvalue_scan_limiter, byte_scan_limiter, time_scan_limiter) =
                scan_limiter.into_parts();

            // Safety: `unwrap` should be okay as we are passing a
            // `Some(x)` value above.
            let keyvalue_scan_limiter = keyvalue_scan_limiter.unwrap();
            let byte_scan_limiter = byte_scan_limiter.unwrap();
            let time_scan_limiter = time_scan_limiter.unwrap();

            assert!(!keyvalue_scan_limiter.is_enforcing());
            assert!(!byte_scan_limiter.is_enforcing());
            assert!(time_scan_limiter.try_keyvalue_scan());
        }

        #[test]
        fn get_keyvalue_scan_limiter_ref() {
            let scan_limiter = ScanLimiter::new(None, None, None);

            assert!(scan_limiter.get_keyvalue_scan_limiter_ref().is_none());

            let scan_limiter = ScanLimiter::new(Some(KeyValueScanLimiter::untracked()), None, None);

            // Safety: Safe to `unwrap` as we are passing `Some(x)` value
            // above.
            assert!(!scan_limiter
                .get_keyvalue_scan_limiter_ref()
                .unwrap()
                .is_enforcing());
        }

        #[test]
        fn get_byte_scan_limiter_ref() {
            let scan_limiter = ScanLimiter::new(None, None, None);

            assert!(scan_limiter.get_byte_scan_limiter_ref().is_none());

            let scan_limiter = ScanLimiter::new(None, Some(ByteScanLimiter::untracked()), None);

            // Safety: Safe to `unwrap` as we are passing `Some(x)` value
            // above.
            assert!(!scan_limiter
                .get_byte_scan_limiter_ref()
                .unwrap()
                .is_enforcing());
        }

        #[test]
        fn get_time_scan_limiter_ref() {
            let scan_limiter = ScanLimiter::new(None, None, None);

            assert!(scan_limiter.get_time_scan_limiter_ref().is_none());

            let scan_limiter = ScanLimiter::new(None, None, Some(TimeScanLimiter::unlimited()));

            // Safety: Safe to `unwrap` as we are passing `Some(x)` value
            // above.
            assert!(scan_limiter
                .get_time_scan_limiter_ref()
                .unwrap()
                .try_keyvalue_scan());
        }
    }
}
