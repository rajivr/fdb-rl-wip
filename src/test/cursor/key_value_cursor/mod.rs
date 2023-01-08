use bytes::Bytes;

use fdb::error::FdbResult;
use fdb::range::Range;
use fdb::subspace::Subspace;
use fdb::Key;

use std::convert::{TryFrom, TryInto};

use crate::cursor::{KeyValueContinuationInternal, KeyValueContinuationV0, KeyValueCursorBuilder};
use crate::range::KeyRange;

/// Expose [`KeyValueCursorBuilder::build_range`] method.
pub fn key_value_cursor_builder_build_range(
    maybe_subspace: &Option<Subspace>,
    key_range: KeyRange,
    continuation: Option<Bytes>,
    reverse: bool,
) -> FdbResult<Range> {
    let maybe_continuation_internal = match continuation {
        Some(b) => Some(KeyValueContinuationInternal::try_from(b)?),
        None => None,
    };
    KeyValueCursorBuilder::build_range(
        maybe_subspace,
        key_range,
        maybe_continuation_internal,
        reverse,
    )
}

/// Expose [`KeyValueContinuationV0::continuation`].
pub fn key_value_continuation_v0_continuation_bytes(key: Key) -> FdbResult<Bytes> {
    KeyValueContinuationV0::continuation(key).try_into()
}

/// Expose [`KeyValueContinuationV0::begin_marker`].
pub fn key_value_continuation_v0_begin_marker_bytes() -> FdbResult<Bytes> {
    KeyValueContinuationV0::begin_marker().try_into()
}

/// Expose [`KeyValueContinuationV0::end_marker`].
pub fn key_value_continuation_v0_end_marker_bytes() -> FdbResult<Bytes> {
    KeyValueContinuationV0::end_marker().try_into()
}
