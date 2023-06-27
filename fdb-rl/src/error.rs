//! Provides error constants for use with [`FdbError`] and
//! [`FdbResult`].
//!
//! [`FdbError`]: fdb::error::FdbError
//! [`FdbResult`]: fdb::error::FdbResult

// 200 - `cursor` module
// 210 - `split_helper` module
// 220 - `RecordContext` type
// 230 - `raw_record` module (private, see `raw_record/error.rs`)

/// Continuation is invalid.
///
/// This error can also occur when Prost [`encode`] and [`decode`] fails.
///
/// [`encode`]: prost::Message::encode
/// [`decode`]: prost::Message::decode
pub const CURSOR_INVALID_CONTINUATION: i32 = 200;

/// Error occurred when trying to create a value of type
/// [`KeyValueCursor`] using the [`build`] method.
///
/// [`KeyValueCursor`]: crate::cursor::KeyValueCursor
/// [`build`]: crate::cursor::KeyValueCursorBuilder::build
pub const CURSOR_KEYVALUE_CURSOR_BUILDER_ERROR: i32 = 202;

/// Scan limit reached when calling [`split_helper::load`] function.
///
/// [`split_helper::load`]: crate::split_helper::load
pub const SPLIT_HELPER_SCAN_LIMIT_REACHED: i32 = 210;

/// Primary key provided to [`split_helper::load`],
/// [`split_helper::save`] and [`split_helper::delete`] functions is
/// invalid.
///
/// [`split_helper::load`]: crate::split_helper::load
/// [`split_helper::save`]: crate::split_helper::save
/// [`split_helper::delete`]: crate::split_helper::delete
pub const SPLIT_HELPER_INVALID_PRIMARY_KEY: i32 = 211;

/// [`split_helper::load`] function encountered an invalid record
/// header for the primary key.
///
/// [`split_helper::load`]: crate::split_helper::load
pub const SPLIT_HELPER_LOAD_INVALID_RECORD_HEADER: i32 = 212;

/// [`split_helper::load`] function encountered an invalid record
/// seralized bytes for the primary key.
///
/// [`split_helper::load`]: crate::split_helper::load
pub const SPLIT_HELPER_LOAD_INVALID_SERIALIZED_BYTES: i32 = 213;

/// The size of the seralized bytes provided to [`split_helper::save`]
/// function is invalid.
///
/// [`split_helper::save`]: crate::split_helper::save
pub const SPLIT_HELPER_SAVE_INVALID_SERIALIZED_BYTES_SIZE: i32 = 214;

/// The local version counter maintained within [`RecordContext`]
/// overflowed.
///
/// [`RecordContext`]: crate::RecordContext
pub const RECORD_CONTEXT_LOCAL_VERSION_OVERFLOW: i32 = 220;

/// The timer maintained within [`RecordContext`] is no longer valid.
///
/// [`RecordContext`]: crate::RecordContext
pub const RECORD_CONTEXT_INVALID_TRANSACTION_AGE: i32 = 221;

/// The incarnation version maintained within [`RecordContext`] was
/// previously set, and cannot be updated.
///
/// [`RecordContext`]: crate::RecordContext
pub const RECORD_CONTEXT_INCARNATION_VERSION_ALREADY_SET: i32 = 222;
