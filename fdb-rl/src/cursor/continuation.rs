use bytes::Bytes;

use fdb::error::FdbResult;

use std::any::Any;
use std::fmt::{self, Debug};

/// Prevent users from implementing private trait.
mod private {
    use crate::cursor::KeyValueContinuationInternal;
    use crate::raw_record::RawRecordContinuationInternal;

    pub trait Sealed {}

    impl Sealed for KeyValueContinuationInternal {}

    impl Sealed for RawRecordContinuationInternal {}
}

/// Types that represent the continuation of a [`Cursor`].
///
/// A [`Continuation`] represents the current position of a cursor and
/// can be used to restart a cursor at a point immediately after the
/// value returned by [`CursorSuccess`] or [`CursorError`].
///
/// A continuation can be serialized to an opaque byte array that can
/// be passed to a client.
///
/// [`Cursor`]: crate::cursor::Cursor
/// [`CursorSuccess`]: crate::cursor::CursorSuccess
/// [`CursorError`]: crate::cursor::CursorError
pub trait Continuation: private::Sealed + Any {
    /// Serialize this continuation to a [`Bytes`] value.
    // The reason why we need to return `FdbResult<Bytes>` is because
    // Protobuf serialization can return an error.
    fn to_bytes(&self) -> FdbResult<Bytes>;

    /// Return whether this continuation is at the *begin marker*
    /// position.
    ///
    /// Begin marker is the position in the cursor before any items
    /// are returned.
    fn is_begin_marker(&self) -> bool;

    /// Return whether this continuation is at the *end marker*
    /// position.
    ///
    /// End marker is the position in the cursor after all the items
    /// are returned.
    fn is_end_marker(&self) -> bool;
}

impl Debug for (dyn Continuation + Send + Sync + 'static) {
    fn fmt<'a>(&self, f: &mut fmt::Formatter<'a>) -> fmt::Result {
        // *Note:* In the output you will see `Ok(b"...")` because of
        // Protobuf serialization.
        write!(f, "{:?}", self.to_bytes())
    }
}

#[cfg(test)]
mod tests {
    // The methods on the continuation trait is tested in the
    // integration tests for `Cursor` trait for `KeyValueCursor` type.
}
