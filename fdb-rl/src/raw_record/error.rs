//! Provides error constants for raw record module.
//!
//! Also see `src/error.rs` for assigned error ranges.

/// The [`TupleSchema`] used to create a primary key is invalid.
///
/// [`TupleSchema`]: fdb::tuple::TupleSchema
pub(crate) const RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA: i32 = 230;

/// The [`Tuple`] used for primary key does not match its schema.
///
/// [`Tuple`]: fdb::tuple::Tuple
pub(crate) const RAW_RECORD_PRIMARY_KEY_TUPLE_SCHEMA_MISMATCH: i32 = 231;

/// Error occurred when trying to create a value of type
/// [`RawRecordCursor`] using the [`build`] method.
///
/// <p style="background:rgba(255,181,77,0.16);padding:0.75em;">
/// <strong>Warning:</strong> This const is <strong>not</strong> meant
/// to be public. We need to make this const public to support
/// integration tests. Do not use this const in your code.</p>
///
/// [`RawRecordCursor`]: crate::raw_record::RawRecordCursor
/// [`build`]: crate::raw_record::RawRecordCursorBuilder::build
pub const RAW_RECORD_CURSOR_BUILDER_ERROR: i32 = 232;

/// Error occured when parsing the key or value
///
/// <p style="background:rgba(255,181,77,0.16);padding:0.75em;">
/// <strong>Warning:</strong> This const is <strong>not</strong> meant
/// to be public. We need to make this const public to support
/// integration tests. Do not use this const in your code.</p>
pub const RAW_RECORD_CURSOR_NEXT_ERROR: i32 = 233;

/// The cursor state machine entered an invalid state.
pub(crate) const RAW_RECORD_CURSOR_STATE_ERROR: i32 = 234;
