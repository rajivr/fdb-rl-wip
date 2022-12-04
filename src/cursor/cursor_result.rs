use fdb::error::FdbError;

use std::error::Error;
use std::fmt::{self, Display};
use std::sync::Arc;

use crate::cursor::Continuation;

/// Result obtained when [`Cursor`] advances.
///
/// It represents everything that one can learn each time a [`Cursor`]
/// advances. It consists of two variants:
///
/// 1. `Ok(`[`CursorSuccess`]`<T>)` is the next object of type `T`
///    produced by the cursor. In addition to the next object, it also
///    includes a [`CursorResultContinuation`] that can be used to
///    continue the cursor after the last object is returned.
///
/// 2. `Err(`[`CursorError`]`<T>)` represents an error condition which
///    could be an [`FdbError`] or cursor termination due to in-band
///    or out-of-band cursor termination ([`NoNextReason`]). In both
///    the cases a [`CursorResultContinuation`] is included that can
///    be used to continue the cursor.
///
/// [`Cursor`]: crate::cursor::Cursor
pub type CursorResult<T> = Result<CursorSuccess<T>, CursorError>;

/// A [`Continuation`] that is present within [`CursorResult`] and
/// returned when the [`Cursor`] advances.
///
/// [`Continuation`]: crate::cursor::Continuation
/// [`Cursor`]: crate::cursor::Cursor
pub type CursorResultContinuation = Arc<dyn Continuation + Send + Sync + 'static>;

/// Track reason for in-band or out-of-band cursor termination.
#[derive(Clone, Debug)]
pub enum NoNextReason {
    /// The underlying scan, irrespective of any limit, has reached
    /// the end (in-band).
    SourceExhausted(CursorResultContinuation),
    /// The limit on the number of items to return was reached (in-band).
    ReturnLimitReached(CursorResultContinuation),
    /// The limit on the amount of time that a scan can take was
    /// reached (out-of-band).
    TimeLimitReached(CursorResultContinuation),
    /// The limit on the number of bytes to scan was reached (out-of-band).
    ByteLimitReached(CursorResultContinuation),
    /// The limit on the number of key-values to scan was reached (out-of-band).
    KeyValueLimitReached(CursorResultContinuation),
}

/// Object of type `T` produced by a [`Cursor`] along with a
/// [`CursorResultContinuation`].
///
/// [`Cursor`]: crate::cursor::Cursor
#[derive(Clone, Debug)]
pub struct CursorSuccess<T> {
    value: T,
    continuation: CursorResultContinuation,
}

impl<T> CursorSuccess<T> {
    /// Construct a new [`CursorSuccess`].
    pub(crate) fn new(value: T, continuation: CursorResultContinuation) -> CursorSuccess<T> {
        CursorSuccess {
            value,
            continuation,
        }
    }

    /// Gets a reference to success value from [`CursorSuccess`].
    pub fn get_value_ref(&self) -> &T {
        &self.value
    }

    /// Gets a reference to continuation from [`CursorSuccess`].
    pub fn get_continuation_ref(&self) -> &CursorResultContinuation {
        &self.continuation
    }

    /// Extract success value from [`CursorSuccess`].
    pub fn into_value(self) -> T {
        self.value
    }

    /// Extract continuation from [`CursorSuccess`].
    pub fn into_continuation(self) -> CursorResultContinuation {
        self.continuation
    }

    /// Extract success value and continuation from [`CursorSuccess`].
    pub fn into_parts(self) -> (T, CursorResultContinuation) {
        (self.value, self.continuation)
    }
}

/// Error that occurred when advancing the [`Cursor`].
///
/// Includes a [`CursorResultContinuation`] that can be used to
/// continue the cursor.
///
/// [`Cursor`]: crate::cursor::Cursor
#[derive(Clone, Debug)]
pub enum CursorError {
    /// [`FdbError`] occurred.
    FdbError(FdbError, CursorResultContinuation),
    /// In-band or out-of-band cursor termination occurred.
    NoNextReason(NoNextReason),
}

impl Error for CursorError {}

impl Display for CursorError {
    fn fmt<'a>(&self, f: &mut fmt::Formatter<'a>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[cfg(test)]
mod tests {
    // No tests here as we are just defining types.
}
