//! Provides error constants for use with [`FdbError`] and
//! [`FdbResult`].
//!
//! [`FdbError`]: fdb::error::FdbError
//! [`FdbResult`]: fdb::error::FdbResult

// 200 - `cursor` module
// 210 - `split_helper` module
// 220 - `RecordContext` type
// 230 - `raw_record` module

/// Provided continuation was invalid.
pub const CURSOR_INVALID_CONTINUATION: i32 = 200;

/// TODO (remove the other one and add documentation)
pub const CURSOR_INVALID_KEYVALUE_CONTINUATION_INTERNAL: i32 = 201;

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
/// version for the primary key.
///
/// [`split_helper::load`]: crate::split_helper::load
pub const SPLIT_HELPER_LOAD_INVALID_RECORD_VERSION: i32 = 212;

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

/// The [`TupleSchema`] used to create a primary key is invalid.
pub const RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA: i32 = 230;

/// The [`Tuple`] used for primary key does not match its schema.
pub const RAW_RECORD_PRIMARY_KEY_TUPLE_SCHEMA_MISMATCH: i32 = 231;

/// TODO
pub const RAW_RECORD_CURSOR_BUILDER_ERROR: i32 = 232;
