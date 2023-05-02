use bytes::Bytes;

use fdb::error::FdbResult;
use fdb::range::Range;
use fdb::subspace::Subspace;
use fdb::Key;

use std::convert::{TryFrom, TryInto};

use crate::cursor::pb;
use crate::cursor::{KeyValueContinuationInternal, KeyValueCursorBuilder};
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

/// Expose [`KeyValueContinuationEnumV1::Continuation`].
pub fn key_value_continuation_v1_continuation_bytes(key: Key) -> FdbResult<Bytes> {
    KeyValueContinuationInternal::V1(pb::KeyValueContinuationInternalV1 {
        key_value_continuation: pb::ProtoKeyValueContinuationEnumV1::Continuation(
            pb::ProtoContinuationV1 {
                continuation: Bytes::from(key),
            },
        ),
    })
    .try_into()
}

/// Expose [`KeyValueContinuationEnumV1::BeginMarker`].
pub fn key_value_continuation_v1_begin_marker_bytes() -> FdbResult<Bytes> {
    KeyValueContinuationInternal::V1(pb::KeyValueContinuationInternalV1 {
        key_value_continuation: pb::ProtoKeyValueContinuationEnumV1::BeginMarker(
            pb::ProtoBeginMarkerV1 {},
        ),
    })
    .try_into()
}

/// Expose [`KeyValueContinuationEnumV1::EndMarker`].
pub fn key_value_continuation_v1_end_marker_bytes() -> FdbResult<Bytes> {
    KeyValueContinuationInternal::V1(pb::KeyValueContinuationInternalV1 {
        key_value_continuation: pb::ProtoKeyValueContinuationEnumV1::EndMarker(
            pb::ProtoEndMarkerV1 {},
        ),
    })
    .try_into()
}
