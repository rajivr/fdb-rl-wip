//! Provides [`RecordContext`] type and associated items.

use fdb::error::{FdbError, FdbResult};

use std::convert::TryFrom;
use std::time::Instant;

use crate::error::{
    RECORD_CONTEXT_INCARNATION_VERSION_ALREADY_SET, RECORD_CONTEXT_INVALID_TRANSACTION_AGE,
    RECORD_CONTEXT_LOCAL_VERSION_OVERFLOW,
};

/// Provides context information for use by other types in the Record
/// Layer.
///
/// [`RecordContext`] is a singleton within a FDB transaction. There
/// **must** be only one value of [`RecordContext`] in a FDB
/// transaction.
#[derive(Debug)]
pub struct RecordContext {
    /// Instant when this [`RecordContext`] was created.
    instant: Instant,

    /// Used by `RecordVersion`.
    incarnation_version: Option<u64>,

    /// Used by `RecordVersion`.
    local_version: u16,
}

impl RecordContext {
    /// Claims a local version that is unique within a single transaction.
    ///
    /// This means that any two calls to this method will return a
    /// different value. If the ordering of the calls is
    /// deterministic, then it is also guaranteed that the earlier
    /// calls will receive a smaller version than the newer calls.
    pub fn claim_local_version(&mut self) -> FdbResult<u16> {
        let local_version = self.local_version;

        // We return an error after we have returned `u16::MAX - 1`,
        // as it simplifies our implementation logic. If this error is
        // being returned, then almost certainly means there is a
        // runaway loop in the code that is calling this method, which
        // we want to catch, instead of panicing.
        match self.local_version.checked_add(1) {
            Some(x) => {
                self.local_version = x;
                Ok(local_version)
            }
            None => Err(FdbError::new(RECORD_CONTEXT_LOCAL_VERSION_OVERFLOW)),
        }
    }

    /// Get the number of milliseconds since context was created.
    pub fn get_transaction_age(&self) -> FdbResult<u64> {
        u64::try_from(self.instant.elapsed().as_millis())
            .map_err(|_| FdbError::new(RECORD_CONTEXT_INVALID_TRANSACTION_AGE))
    }

    /// Set the incarnation version that can be used by other methods
    /// within a Record Layer transaction.
    ///
    /// Attempting to set this value twice, will result in an error.
    pub fn set_incarnation_version(&mut self, incarnation_version: u64) -> FdbResult<()> {
        if self.incarnation_version.is_none() {
            self.incarnation_version = Some(incarnation_version);
            Ok(())
        } else {
            Err(FdbError::new(
                RECORD_CONTEXT_INCARNATION_VERSION_ALREADY_SET,
            ))
        }
    }

    /// Get the incarnation version that can be used by other methods
    /// within a Record Layer transaction.
    ///
    /// Returns `None`, if no incarnation version was previously set.
    pub fn get_incarnation_version(&self) -> Option<u64> {
        self.incarnation_version
    }
}

impl Default for RecordContext {
    fn default() -> RecordContext {
        RecordContext {
            instant: Instant::now(),
            incarnation_version: None,
            local_version: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use fdb::error::FdbError;

    use std::thread::sleep;
    use std::time::Duration;

    use crate::error::{
        RECORD_CONTEXT_INCARNATION_VERSION_ALREADY_SET, RECORD_CONTEXT_LOCAL_VERSION_OVERFLOW,
    };

    use super::RecordContext;

    #[test]
    fn claim_local_version() {
        let mut record_context = RecordContext::default();

        assert_eq!(Ok(0), record_context.claim_local_version());
        assert_eq!(Ok(1), record_context.claim_local_version());
        assert_eq!(Ok(2), record_context.claim_local_version());

        let mut record_context = RecordContext::default();
        record_context.local_version = u16::MAX - 2;

        assert_eq!(Ok(u16::MAX - 2), record_context.claim_local_version());
        assert_eq!(Ok(u16::MAX - 1), record_context.claim_local_version());
        assert_eq!(
            Err(FdbError::new(RECORD_CONTEXT_LOCAL_VERSION_OVERFLOW)),
            record_context.claim_local_version()
        );
    }

    #[test]
    fn get_transaction_age() {
        let record_context = RecordContext::default();

        // sleep for 100 milliseconds
        sleep(Duration::from_millis(100));

        // Safety: Safe to unwrap here because we should really not
        //         extend `u128::MAX` milliseconds.
        assert!(record_context.get_transaction_age().unwrap() >= 100);

        // sleep for another 100 milliseconds
        sleep(Duration::from_millis(100));

        // Safety: Safe to unwrap here because we should really not
        //         extend `u128::MAX` milliseconds.
        //
        // *Note:* We do not have a good way to test the error case
        //         here. We are assuming that the type system will do
        //         its work correctly. Also, if we are to exceed
        //         `u64::MAX` milliseconds, there is a problem
        //         somewhere else!
        assert!(record_context.get_transaction_age().unwrap() >= 200);
    }

    #[test]
    fn set_incarnation_version() {
        let mut record_context = RecordContext::default();

        let res = record_context.set_incarnation_version(7);
        assert_eq!(Ok(()), res);

        assert_eq!(Some(7), record_context.get_incarnation_version());

        let res = record_context.set_incarnation_version(8);

        assert_eq!(
            Err(FdbError::new(
                RECORD_CONTEXT_INCARNATION_VERSION_ALREADY_SET
            )),
            res
        );

        assert_eq!(Some(7), record_context.get_incarnation_version());
    }

    #[test]
    fn get_incarnation_version() {
        let mut record_context = RecordContext::default();

        assert_eq!(None, record_context.get_incarnation_version());

        let res = record_context.set_incarnation_version(0);
        assert_eq!(Ok(()), res);

        assert_eq!(Some(0), record_context.get_incarnation_version());
    }
}
