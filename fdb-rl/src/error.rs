//! Provides error constants for use with [`FdbError`] and
//! [`FdbResult`].
//!
//! [`FdbError`]: fdb::error::FdbError
//! [`FdbResult`]: fdb::error::FdbResult

// 200 - `cursor` module
// 210 - `split_helper` module
// 220 - `RecordContext` type
// 230 - `raw_record` module (private, see `raw_record/error.rs`)
// 240 - `protobuf` module (private, see `protobuf/error.rs`)
// 250 - `partiql` module (private, see `partiql/error.rs`)
// 260 - `metadata` module (private, see `metadata/error.rs`)
// 270 - `todo` module (private, see `todo/todo.rs`)

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
