//! Provides error constants for use with [`FdbError`] and
//! [`FdbResult`].
//!
//! [`FdbError`]: fdb::error::FdbError
//! [`FdbResult`]: fdb::error::FdbResult

// 200 - `cursor` module
// 210 - `split_helper` module
// 220 - `RecordContext` type

/// Provided continuation was invalid.
pub const CURSOR_INVALID_CONTINUATION: i32 = 200;

/// Error occurred when trying to create a value of type
/// [`KeyValueCursor`] using the [`build`] method.
///
/// [`KeyValueCursor`]: crate::cursor::KeyValueCursor
/// [`build`]: crate::cursor::KeyValueCursorBuilder::build
pub const CURSOR_KEYVALUE_CURSOR_BUILDER_ERROR: i32 = 201;

/// TODO
pub const SPLIT_HELPER_SCAN_LIMIT_REACHED: i32 = 210;

/// TODO
pub const SPLIT_HELPER_INVALID_PRIMARY_KEY: i32 = 211;

/// TODO
pub const SPLIT_HELPER_LOAD_INVALID_RECORD_VERSION: i32 = 212;

/// TODO
pub const SPLIT_HELPER_LOAD_INVALID_SERIALIZED_BYTES: i32 = 213;

/// TODO
pub const SPLIT_HELPER_SAVE_INVALID_SERIALIZED_BYTES_SIZE: i32 = 214;

/// TODO
pub const RECORD_CONTEXT_LOCAL_VERSION_OVERFLOW: i32 = 220;

/// TODO
pub const RECORD_CONTEXT_INVALID_TRANSACTION_AGE: i32 = 221;

/// TODO
pub const RECORD_CONTEXT_INCARNATION_VERSION_ALREADY_SET: i32 = 222;
