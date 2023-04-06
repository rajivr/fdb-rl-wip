//! Provides types and traits for Record Layer cursors.

// Do this so we do not need to give a special name for the trait
// file.
#[allow(clippy::module_inception)]
mod cursor;

mod continuation;
mod cursor_result;
mod key_value_cursor;
mod limit_manager;

pub(crate) mod pb {
    pub(crate) use super::key_value_cursor::pb::{
        BeginMarkerV1, ContinuationV1, EndMarkerV1, KeyValueContinuationEnumV1,
        KeyValueContinuationInternalV1,
    };
}

pub(crate) use key_value_cursor::KeyValueContinuationInternal;

pub(crate) use limit_manager::{LimitManager, LimitManagerStoppedReason};

pub use continuation::Continuation;
pub use cursor::{Cursor, CursorFilter, CursorMap};
pub use cursor_result::{
    CursorError, CursorResult, CursorResultContinuation, CursorSuccess, NoNextReason,
};
pub use key_value_cursor::{KeyValueCursor, KeyValueCursorBuilder};
