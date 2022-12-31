//! Provides error constants for use with [`FdbError`] and
//! [`FdbResult`].
//!
//! [`FdbError`]: fdb::error::FdbError
//! [`FdbResult`]: fdb::error::FdbResult

// 200 - `cursor` module
// 210 - `split_helper` module

/// Provided continuation was invalid.
pub const CURSOR_INVALID_CONTINUATION: i32 = 200;

/// Error occurred when trying to create a value of type
/// [`KeyValueCursor`] using the [`build`] method.
///
/// [`KeyValueCursor`]: crate::cursor::KeyValueCursor
/// [`build`]: crate::cursor::KeyValueCursorBuilder::build
pub const CURSOR_KEYVALUE_CURSOR_BUILDER_ERROR: i32 = 201;

/// TODO
pub const SPLIT_HELPER_LOAD_INVALID_RECORD_VERSION: i32 = 210;

/// TODO
pub const SPLIT_HELPER_LOAD_INVALID_SERIALIZED_BYTES: i32 = 211;

/// TODO
pub const SPLIT_HELPER_LOAD_SCAN_LIMIT_REACHED: i32 = 212;

/// TODO
pub const SPLIT_HELPER_SAVE_INVALID_SERIALIZED_BYTES_SIZE: i32 = 213;
