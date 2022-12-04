//! Provides types and traits for Record Layer cursors.

// Do this so we do not need to give a special name for the trait
// file.
#[allow(clippy::module_inception)]
mod cursor;

mod continuation;
mod cursor_result;
mod key_value_cursor;
mod limit_manager;

pub(crate) use key_value_cursor::{KeyValueContinuationInternal, KeyValueContinuationV0};
pub(crate) use limit_manager::{LimitManager, LimitManagerStoppedReason};

pub use continuation::Continuation;
pub use cursor::Cursor;
pub use cursor_result::{
    CursorError, CursorResult, CursorResultContinuation, CursorSuccess, NoNextReason,
};
pub use key_value_cursor::{KeyValueCursor, KeyValueCursorBuilder};
