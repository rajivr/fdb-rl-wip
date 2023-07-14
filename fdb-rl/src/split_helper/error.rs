//! Provides error constants for split helper module.
//!
//! Also see `src/error.rs` for assigned error ranges.

/// Scan limit reached when calling [`split_helper::load`] function.
///
/// <p style="background:rgba(255,181,77,0.16);padding:0.75em;">
/// <strong>Warning:</strong> This const is <strong>not</strong> meant
/// to be public. We need to make this const public to support
/// integration tests. Do not use this const in your code.</p>
///
/// [`split_helper::load`]: super::load
pub const SPLIT_HELPER_SCAN_LIMIT_REACHED: i32 = 210;

/// Primary key provided to [`split_helper::load`],
/// [`split_helper::save`] and [`split_helper::delete`] functions is
/// invalid.
///
/// <p style="background:rgba(255,181,77,0.16);padding:0.75em;">
/// <strong>Warning:</strong> This const is <strong>not</strong> meant
/// to be public. We need to make this const public to support
/// integration tests. Do not use this const in your code.</p>
///
/// [`split_helper::load`]: super::load
/// [`split_helper::save`]: super::save
/// [`split_helper::delete`]: super::delete
pub const SPLIT_HELPER_INVALID_PRIMARY_KEY: i32 = 211;

/// [`split_helper::load`] function encountered an invalid record
/// header for the primary key.
///
/// <p style="background:rgba(255,181,77,0.16);padding:0.75em;">
/// <strong>Warning:</strong> This const is <strong>not</strong> meant
/// to be public. We need to make this const public to support
/// integration tests. Do not use this const in your code.</p>
///
/// [`split_helper::load`]: super::load
pub const SPLIT_HELPER_LOAD_INVALID_RECORD_HEADER: i32 = 212;

/// [`split_helper::load`] function encountered an invalid record
/// seralized bytes for the primary key.
///
/// <p style="background:rgba(255,181,77,0.16);padding:0.75em;">
/// <strong>Warning:</strong> This const is <strong>not</strong> meant
/// to be public. We need to make this const public to support
/// integration tests. Do not use this const in your code.</p>
///
/// [`split_helper::load`]: super::load
pub const SPLIT_HELPER_LOAD_INVALID_SERIALIZED_BYTES: i32 = 213;

/// The size of the seralized bytes provided to [`split_helper::save`]
/// function is invalid.
///
/// <p style="background:rgba(255,181,77,0.16);padding:0.75em;">
/// <strong>Warning:</strong> This const is <strong>not</strong> meant
/// to be public. We need to make this const public to support
/// integration tests. Do not use this const in your code.</p>
///
/// [`split_helper::save`]: super::save
pub const SPLIT_HELPER_SAVE_INVALID_SERIALIZED_BYTES_SIZE: i32 = 214;
