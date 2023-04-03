use bytes::{BufMut, Bytes, BytesMut};

use fdb::error::FdbResult;
use fdb::range::Range;
use fdb::subspace::Subspace;
use fdb::tuple::key_util;

use crate::range::{HighEndpoint, KeyRange, LowEndpoint};

/// Low endpoint with continuation of a key (represented using
/// [`Bytes`]).
///
/// `BytesLowEndpointWithContinuation::Continuation` arises during a
/// forward scan.
///
/// You *cannot* have a situation where you have
/// `BytesLowEndpointWithContinuation::Continuation` and
/// `BytesHighEndpointWithContinuation::Continuation` at the same
/// time.
pub(crate) enum BytesLowEndpointWithContinuation {
    /// Start of the range. `Start(None)` indicates the very beginning
    /// of the FDB range. `Start(Some(subspace))`, indicates the start
    /// of a subspace.
    Start(Option<Bytes>),
    /// Includes endpoint value.
    RangeInclusive(Bytes),
    /// Excludes endpoint value.
    RangeExclusive(Bytes),
    /// Represents a continuation.
    Continuation(Bytes),
}

/// High endpoint with continuation of a key (represented using
/// [`Bytes`]).
///
/// `BytesHighEndpointWithContinuation::Continuation` arises during a
/// reverse scan.
///
/// You *cannot* have a situation where you have
/// `BytesLowEndpointWithContinuation::Continuation` and
/// `BytesHighEndpointWithContinuation::Continuation` at the same
/// time.
pub(crate) enum BytesHighEndpointWithContinuation {
    /// Includes endpoint value.
    RangeInclusive(Bytes),
    /// Excludes endpoint value.
    RangeExclusive(Bytes),
    /// Represents a continuation.
    Continuation(Bytes),
    /// End of a range. `End(None)` indicates the very end of the FDB
    /// range. `End(Some(subspace))`, indicate the end of the
    /// subspace.
    End(Option<Bytes>),
}

/// Build a range with an optional continuation that is *not* a
/// begin marker or end marker.
///
/// Begin marker and end marker are special cases that needs to be
/// handled seperately. That is done in
/// [`KeyValueCursorBuilder::build_range`] method.
///
/// *Note:* This method along with
/// [`KeyValueCursorBuilder::build_range`] and
/// [`bytes_endpoint::build_range_bytes`] has extensive integration
/// tests to verify its correctness. Exercise care when refactoring
/// this code.
///
/// [`KeyValueCursorBuilder::build_range`]: crate::cursor::KeyValueCursorBuilder::build_range
/// [`bytes_endpoint::build_range_bytes`]: build_range_bytes
pub(crate) fn build_range_continuation(
    maybe_subspace: &Option<Subspace>,
    key_range: KeyRange,
    maybe_continuation_key: Option<Bytes>,
    reverse: bool,
) -> FdbResult<Range> {
    let (key_low_endpoint, key_high_endpoint) = key_range.into_parts();

    let (bytes_low_endpoint_with_continuation, bytes_high_endpoint_with_continuation) =
        if let Some(continuation_key) = maybe_continuation_key {
            // Continuation case. In this case we return
            // `BytesLowEndpointWithContinuation::Continuation` or
            // `BytesHighEndpointWithContinuation::Continuation`
            // depending on whether we are doing forward scan or
            // reverse scan.
            let continuation_key_bytes = {
                let mut raw_bytes = BytesMut::new();
                if let Some(s) = maybe_subspace {
                    raw_bytes.put(s.pack());
                }
                raw_bytes.put(Bytes::from(continuation_key));
                raw_bytes.into()
            };

            if reverse {
                // Reverse scan. So in this case high endpoint
                // would be a continuation.
                let bytes_high_endpoint_with_continuation =
                    BytesHighEndpointWithContinuation::Continuation(continuation_key_bytes);

                let bytes_low_endpoint_with_continuation = match key_low_endpoint.map(|key| {
                    // The `map` takes care of adding subspace to
                    // `KeyLowEndpoint::RangeInclusive` and
                    // `KeyLowEndpoint::RangeExclusive`. The case that
                    // will we need to handled is
                    // `KeyLowEndpoint::Start`.
                    let mut raw_bytes = BytesMut::new();
                    if let Some(s) = maybe_subspace {
                        raw_bytes.put(s.pack());
                    }
                    raw_bytes.put(Bytes::from(key));
                    Bytes::from(raw_bytes)
                }) {
                    LowEndpoint::Start => {
                        if let Some(s) = maybe_subspace {
                            BytesLowEndpointWithContinuation::Start(Some(s.pack()))
                        } else {
                            BytesLowEndpointWithContinuation::Start(None)
                        }
                    }
                    LowEndpoint::RangeInclusive(b) => {
                        BytesLowEndpointWithContinuation::RangeInclusive(b)
                    }
                    LowEndpoint::RangeExclusive(b) => {
                        BytesLowEndpointWithContinuation::RangeExclusive(b)
                    }
                };

                (
                    bytes_low_endpoint_with_continuation,
                    bytes_high_endpoint_with_continuation,
                )
            } else {
                // Forward scan. So in this case low endpoint
                // would be a continuation.
                let bytes_low_endpoint_with_continuation =
                    BytesLowEndpointWithContinuation::Continuation(continuation_key_bytes);

                let bytes_high_endpoint_with_continuation = match key_high_endpoint.map(|key| {
                    // The `map` takes care of adding subspace to
                    // `KeyHighEndpoint::RangeInclusive` and
                    // `KeyHighEndpoint::RangeExclusive`. The case
                    // that we will need to handled is
                    // `KeyHighEndpoint::End`.
                    let mut raw_bytes = BytesMut::new();
                    if let Some(s) = maybe_subspace {
                        raw_bytes.put(s.pack());
                    }
                    raw_bytes.put(Bytes::from(key));
                    Bytes::from(raw_bytes)
                }) {
                    HighEndpoint::RangeInclusive(b) => {
                        BytesHighEndpointWithContinuation::RangeInclusive(b)
                    }
                    HighEndpoint::RangeExclusive(b) => {
                        BytesHighEndpointWithContinuation::RangeExclusive(b)
                    }
                    HighEndpoint::End => {
                        if let Some(s) = maybe_subspace {
                            BytesHighEndpointWithContinuation::End(Some(s.pack()))
                        } else {
                            BytesHighEndpointWithContinuation::End(None)
                        }
                    }
                };

                (
                    bytes_low_endpoint_with_continuation,
                    bytes_high_endpoint_with_continuation,
                )
            }
        } else {
            // Non continuation case. In this case we won't return
            // `BytesLowEndpointWithContinuation::Continuation` or
            // `BytesHighEndpointWithContinuation::Continuation`.
            let bytes_low_endpoint_with_continuation = match key_low_endpoint.map(|key| {
                // The `map` takes care of adding subspace to
                // `KeyLowEndpoint::RangeInclusive` and
                // `KeyLowEndpoint::RangeExclusive`. The case that
                // will we need to handled is
                // `KeyLowEndpoint::Start`.
                let mut raw_bytes = BytesMut::new();
                if let Some(s) = maybe_subspace {
                    raw_bytes.put(s.pack());
                }
                raw_bytes.put(Bytes::from(key));
                Bytes::from(raw_bytes)
            }) {
                LowEndpoint::Start => {
                    if let Some(s) = maybe_subspace {
                        BytesLowEndpointWithContinuation::Start(Some(s.pack()))
                    } else {
                        BytesLowEndpointWithContinuation::Start(None)
                    }
                }
                LowEndpoint::RangeInclusive(b) => {
                    BytesLowEndpointWithContinuation::RangeInclusive(b)
                }
                LowEndpoint::RangeExclusive(b) => {
                    BytesLowEndpointWithContinuation::RangeExclusive(b)
                }
            };

            let bytes_high_endpoint_with_continuation = match key_high_endpoint.map(|key| {
                // The `map` takes care of adding subspace to
                // `KeyHighEndpoint::RangeInclusive` and
                // `KeyHighEndpoint::RangeExclusive`. The case
                // that we will need to handled is
                // `KeyHighEndpoint::End`.
                let mut raw_bytes = BytesMut::new();
                if let Some(s) = maybe_subspace {
                    raw_bytes.put(s.pack());
                }
                raw_bytes.put(Bytes::from(key));
                Bytes::from(raw_bytes)
            }) {
                HighEndpoint::RangeInclusive(b) => {
                    BytesHighEndpointWithContinuation::RangeInclusive(b)
                }
                HighEndpoint::RangeExclusive(b) => {
                    BytesHighEndpointWithContinuation::RangeExclusive(b)
                }
                HighEndpoint::End => {
                    if let Some(s) = maybe_subspace {
                        BytesHighEndpointWithContinuation::End(Some(s.pack()))
                    } else {
                        BytesHighEndpointWithContinuation::End(None)
                    }
                }
            };

            (
                bytes_low_endpoint_with_continuation,
                bytes_high_endpoint_with_continuation,
            )
        };

    build_range_bytes(
        bytes_low_endpoint_with_continuation,
        bytes_high_endpoint_with_continuation,
    )
}

/// Convert [`BytesLowEndpointWithContinuation`] and
/// [`BytesHighEndpointWithContinuation`] into a FDB [`Range`].
///
/// *Note:* This method along with
/// [`KeyValueCursorBuilder::build_range`],
/// [`bytes_endpoint::build_range_continuation`] has extensive
/// integration test to verify its correctness. Exercise care when
/// refactoring this code.
///
/// Also the use of [`key_util::strinc`], [`key_util::key_after`] and
/// how inclusiveness and exclusiveness is handled depends on using
/// [`KeySelector::first_greater_or_equal`], which is done in
/// [`KeyValueCursorBuilder::build`].
///
/// [`KeyValueCursorBuilder::build_range`]: crate::cursor::KeyValueCursorBuilder::build_range
/// [`bytes_endpoint::build_range_continuation`]: build_range_continuation
/// [`KeySelector::first_greater_or_equal`]: fdb::KeySelector::first_greater_or_equal
/// [`KeyValueCursorBuilder::build`]: crate::cursor::KeyValueCursorBuilder::build
pub(crate) fn build_range_bytes(
    low_endpoint: BytesLowEndpointWithContinuation,
    high_endpoint: BytesHighEndpointWithContinuation,
) -> FdbResult<Range> {
    let begin_bytes = match low_endpoint {
        BytesLowEndpointWithContinuation::Start(x) => match x {
            Some(b) => b,
            None => Bytes::new(),
        },
        BytesLowEndpointWithContinuation::RangeInclusive(b) => b,
        BytesLowEndpointWithContinuation::RangeExclusive(b) => {
            // Returns the first key that does not have `b` as prefix.
            key_util::strinc(b)?.into()
        }
        BytesLowEndpointWithContinuation::Continuation(b) => {
            // The very next key, including prefix matches
            key_util::key_after(b).into()
        }
    };

    let end_bytes = match high_endpoint {
        BytesHighEndpointWithContinuation::RangeInclusive(b) => {
            // Returns the first key that does not have `b` as prefix.
            key_util::strinc(b)?.into()
        }
        BytesHighEndpointWithContinuation::RangeExclusive(b)
        | BytesHighEndpointWithContinuation::Continuation(b) => {
            // The end key selector is *exclusive*.
            //
            // We rely on this fact in two ways.
            //
            // 1. `BytesHighEndpointWithContinuation::RangeExclusive`
            //    implicitly uses this property of end key
            //    selector.
            //
            // 2. `BytesHighEndpointWithContinuation::Continuation`
            //    is used when doing reverse scan and when a
            //    continuation has been passed. Because
            //    "continuation" is the last read key of the
            //    range, it needs to be "exclusive".
            b
        }
        BytesHighEndpointWithContinuation::End(x) => match x {
            Some(b) => {
                // Returns the first key that does not have `b` as prefix.
                key_util::strinc(b)?.into()
            }
            None => Bytes::from_static(b"\xFF"),
        },
    };

    Ok(Range::new(begin_bytes, end_bytes))
}

#[cfg(test)]
mod tests {
    // There are extensive integration tests, see:
    // `examples/test/cursor/key_value_cursor/KeyValueCursorBuilder/build_range.rs`
    // that exercises `build_range_continuation` (and
    // `build_range_bytes`).
    //
    // In addition tests for `TupleRange::into_key_range` also
    // indirectly calls `build_range_continuation` using the `TryFrom`
    // trait.
}
