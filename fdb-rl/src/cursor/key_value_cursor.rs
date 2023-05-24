use bytes::{Bytes, BytesMut};

use fdb::error::{FdbError, FdbResult};
use fdb::future::FdbStreamKeyValue;
use fdb::range::{Range, KEYVALUE_LIMIT_UNLIMITED};
use fdb::subspace::Subspace;
use fdb::transaction::ReadTransaction;
use fdb::tuple::Tuple;
use fdb::{Key, KeySelector, KeyValue, Value};

use prost::Message;

use tokio_stream::StreamExt;

use std::collections::BTreeMap;
use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use crate::cursor::{
    Continuation, Cursor, CursorError, CursorResult, CursorSuccess, LimitManager,
    LimitManagerStoppedReason, NoNextReason,
};

use crate::error::{CURSOR_INVALID_CONTINUATION, CURSOR_KEYVALUE_CURSOR_BUILDER_ERROR};
use crate::range::{bytes_endpoint, KeyRange};
use crate::scan::ScanProperties;

/// Protobuf types.
pub(crate) mod pb {
    use fdb::error::{FdbError, FdbResult};

    use std::convert::TryFrom;

    use crate::error::CURSOR_INVALID_CONTINUATION;

    /// Protobuf generated types renamed to prepend `Proto` and append
    /// version (and add `Enum` suffix).
    pub(crate) use fdb_rl_proto::cursor::v1::key_value_continuation::{
        BeginMarker as ProtoBeginMarkerV1, EndMarker as ProtoEndMarkerV1,
        KeyMarker as ProtoKeyMarkerV1, KeyValueContinuation as ProtoKeyValueContinuationEnumV1,
    };

    /// Protobuf generated types renamed to append version.
    pub(crate) use fdb_rl_proto::cursor::v1::KeyValueContinuation as ProtoKeyValueContinuationV1;

    /// Protobuf message `fdb_rl.cursor.v1.KeyValueContinuation`
    /// contains a `Required` field. So, we need to define this type.
    #[derive(Clone, Debug, PartialEq)]
    pub(crate) struct KeyValueContinuationInternalV1 {
        pub(crate) key_value_continuation: ProtoKeyValueContinuationEnumV1,
    }

    impl TryFrom<ProtoKeyValueContinuationV1> for KeyValueContinuationInternalV1 {
        type Error = FdbError;

        fn try_from(
            keyvalue_continuation_v1: ProtoKeyValueContinuationV1,
        ) -> FdbResult<KeyValueContinuationInternalV1> {
            keyvalue_continuation_v1
                .key_value_continuation
                .ok_or_else(|| FdbError::new(CURSOR_INVALID_CONTINUATION))
                .map(
                    |keyvalue_continuation_enum_v1| KeyValueContinuationInternalV1 {
                        key_value_continuation: keyvalue_continuation_enum_v1,
                    },
                )
        }
    }

    impl From<KeyValueContinuationInternalV1> for ProtoKeyValueContinuationV1 {
        fn from(
            keyvalue_continuation_internal_v1: KeyValueContinuationInternalV1,
        ) -> ProtoKeyValueContinuationV1 {
            ProtoKeyValueContinuationV1 {
                key_value_continuation: Some(
                    keyvalue_continuation_internal_v1.key_value_continuation,
                ),
            }
        }
    }
}

/// Internal representation of key-value continuation.
///
/// We define this type so we can implement [`Continuation`] trait on
/// it. In addition it has `TryFrom<Bytes> for
/// KeyValueContinuationInternal` and
/// `TryFrom<KeyValueContinuationInternal> for Bytes` traits
/// implemented so we can convert between `Bytes` and
/// `KeyValueContinuationInternal`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum KeyValueContinuationInternal {
    V1(pb::KeyValueContinuationInternalV1),
}

impl KeyValueContinuationInternal {
    /// Create a `V1` begin marker value.
    pub(crate) fn new_v1_begin_marker() -> KeyValueContinuationInternal {
        KeyValueContinuationInternal::V1(pb::KeyValueContinuationInternalV1 {
            key_value_continuation: pb::ProtoKeyValueContinuationEnumV1::BeginMarker(
                pb::ProtoBeginMarkerV1 {},
            ),
        })
    }

    /// Create a `V1` key marker value.
    pub(crate) fn new_v1_key_marker(key_marker: Bytes) -> KeyValueContinuationInternal {
        KeyValueContinuationInternal::V1(pb::KeyValueContinuationInternalV1 {
            key_value_continuation: pb::ProtoKeyValueContinuationEnumV1::KeyMarker(
                pb::ProtoKeyMarkerV1 { key_marker },
            ),
        })
    }

    /// Create a `V1` end marker value.
    pub(crate) fn new_v1_end_marker() -> KeyValueContinuationInternal {
        KeyValueContinuationInternal::V1(pb::KeyValueContinuationInternalV1 {
            key_value_continuation: pb::ProtoKeyValueContinuationEnumV1::EndMarker(
                pb::ProtoEndMarkerV1 {},
            ),
        })
    }
}

impl TryFrom<KeyValueContinuationInternal> for Bytes {
    type Error = FdbError;

    fn try_from(keyvalue_continuation_internal: KeyValueContinuationInternal) -> FdbResult<Bytes> {
        match keyvalue_continuation_internal {
            KeyValueContinuationInternal::V1(keyvalue_continuation_internal_v1) => {
                let keyvalue_continuation_v1 =
                    pb::ProtoKeyValueContinuationV1::from(keyvalue_continuation_internal_v1);

                let mut buf = BytesMut::with_capacity(keyvalue_continuation_v1.encoded_len());

                keyvalue_continuation_v1
                    .encode(&mut buf)
                    .map_err(|_| FdbError::new(CURSOR_INVALID_CONTINUATION))
                    .map(|_| {
                        // (version, bytes). Version is `1`.
                        let continuation_tup: (i64, Bytes) = (1, Bytes::from(buf));

                        let continuation_bytes = {
                            let mut tup = Tuple::new();

                            // version
                            tup.push_back::<i64>(continuation_tup.0);

                            tup.push_back::<Bytes>(continuation_tup.1);

                            tup
                        }
                        .pack();

                        Bytes::from(continuation_bytes)
                    })
            }
        }
    }
}

impl TryFrom<Bytes> for KeyValueContinuationInternal {
    type Error = FdbError;

    fn try_from(continuation: Bytes) -> FdbResult<KeyValueContinuationInternal> {
        let (version, continuation): (usize, Bytes) = Tuple::try_from(continuation)
            .and_then(|tup| {
                tup.get::<i64>(0)
                    .and_then(|x| usize::try_from(x).ok())
                    .and_then(|version| {
                        tup.get::<&Bytes>(1).and_then(|bytes_ref| {
                            let continuation = bytes_ref.clone();
                            Some((version, continuation))
                        })
                    })
                    .ok_or_else(|| FdbError::new(CURSOR_INVALID_CONTINUATION))
            })
            .map_err(|_| FdbError::new(CURSOR_INVALID_CONTINUATION))?;

        // Currently there is only one version.
        if version == 1 {
            let keyvalue_continuation_internal_v1 =
                pb::ProtoKeyValueContinuationV1::decode(continuation)
                    .map_err(|_| FdbError::new(CURSOR_INVALID_CONTINUATION))?
                    .try_into()?;

            Ok(KeyValueContinuationInternal::V1(
                keyvalue_continuation_internal_v1,
            ))
        } else {
            Err(FdbError::new(CURSOR_INVALID_CONTINUATION))
        }
    }
}

impl Continuation for KeyValueContinuationInternal {
    fn to_bytes(&self) -> FdbResult<Bytes> {
        self.clone().try_into()
    }

    fn is_begin_marker(&self) -> bool {
        matches!(
            self,
            KeyValueContinuationInternal::V1(pb::KeyValueContinuationInternalV1 {
                key_value_continuation: pb::ProtoKeyValueContinuationEnumV1::BeginMarker(
                    pb::ProtoBeginMarkerV1 {}
                )
            })
        )
    }

    fn is_end_marker(&self) -> bool {
        matches!(
            self,
            KeyValueContinuationInternal::V1(pb::KeyValueContinuationInternalV1 {
                key_value_continuation: pb::ProtoKeyValueContinuationEnumV1::EndMarker(
                    pb::ProtoEndMarkerV1 {}
                )
            })
        )
    }
}

/// A builder for [`KeyValueCursor`]. A value of [`KeyValueCursor`]
/// can be built as shown below.
///
/// ```ignore
/// let kv_cursor = {
///     let mut kv_cursor_builder = KeyValueCursorBuilder::new();
///
///     kv_cursor_builder
///         .subspace(Subspace::new(Bytes::new()).subspace(&{
///             let tup: (&str, &str) = ("sub", "space");
///
///             let mut t = Tuple::new();
///             t.push_back::<String>(tup.0.to_string());
///             t.push_back::<String>(tup.1.to_string());
///
///             t
///         }))
///         .key_range(TupleRange::all().into_key_range(&None))
///         .continuation(continuation_bytes)
///         .scan_properties(ScanPropertiesBuilder::default().build());
///
///     kv_cursor_builder.build(&tr)
/// }?;
/// ```
///
/// Methods [`KeyValueCursorBuilder::subspace`] and
/// [`KeyValueCursorBuilder::continuation`] can be used when needed,
/// and are not needed to build a value of type [`KeyValueCursor`].
///
/// **Note:** Please make sure you read about the limitation mentioned
/// in the documentation for
/// [`KeyValueCursorBuilder::scan_properties`].
//
//
// It is *not* possible for `KeyValueCursorBuilder` to safely derive
// `ParitialEq`. This is because, `ScanProperties` type contains a
// `ScanLimiter`. A `ScanLimiter` type can optionally contain a
// `KeyValueScanLimiter`. A `KeyValueScanLimiter` type contains a
// `Arc<AtomicUsize>`. `AtomicUsize` cannot safely implement
// `PartialEq`. Therefore the Rust compiler does not allow us derive
// `ParitialEq` for `KeyValueCursorBuilder`.
//
// However, for unit testing purpose *only* we cheat a little bit and
// use `unsafe` code to implement `PartialEq` for
// `KeyValueCursorBuilder`.
#[cfg(not(test))]
#[derive(Debug, Clone)]
pub struct KeyValueCursorBuilder {
    subspace: Option<Subspace>,
    scan_properties: Option<ScanProperties>,
    key_range: Option<KeyRange>,
    continuation: Option<Bytes>,
}

/// We need to derive `PartialEq` for testing.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq)]
pub struct KeyValueCursorBuilder {
    subspace: Option<Subspace>,
    scan_properties: Option<ScanProperties>,
    key_range: Option<KeyRange>,
    continuation: Option<Bytes>,
}

impl KeyValueCursorBuilder {
    /// Return a new builder.
    pub fn new() -> KeyValueCursorBuilder {
        KeyValueCursorBuilder {
            subspace: None,
            scan_properties: None,
            key_range: None,
            continuation: None,
        }
    }

    /// Sets the [`Subspace`].
    ///
    /// **Note:** If you intend to set a continuation, then you *must*
    /// use the same [`Subspace`] used to build the [`KeyValueCursor`]
    /// that returned the continuation.
    pub fn subspace(&mut self, subspace: Subspace) -> &mut KeyValueCursorBuilder {
        self.subspace = Some(subspace);
        self
    }

    /// Sets the [`ScanProperties`].
    ///
    /// # Note
    ///
    /// There is **no way** in the [`RangeOptions`] API (within
    /// [`ScanProperties`]) to set a limit of `0`. *Infact* if you set
    /// the limit to `0`, you are indicating that you want [unlimited]
    /// rows, which almost always is not the behavior that you
    /// want.
    ///
    /// The correct way to handle this is not to create the
    /// [`KeyValueCursor`], when you want a limit of `0`. If you
    /// provide a limit of `0` or less, the behavior that **you will**
    /// actually get is that you will be requesting [`i32::MAX`] key
    /// values.
    ///
    /// Since [`KeyValueCursor`] is a low-level API we expect the
    /// users of this API to be aware of this limitation if you are
    /// using this API.
    ///
    /// If you intend to set a continuation, then you *must* use the
    /// same [`RangeOptions`] (within [`ScanProperties`]) used to
    /// build the [`KeyValueCursor`] that returned the continuation
    /// and if limit is used adjust then its value accordingly.
    ///
    /// [`RangeOptions`]: fdb::range::RangeOptions
    /// [unlimited]: fdb::range::KEYVALUE_LIMIT_UNLIMITED
    pub fn scan_properties(
        &mut self,
        scan_properties: ScanProperties,
    ) -> &mut KeyValueCursorBuilder {
        self.scan_properties = Some(scan_properties);
        self
    }

    /// Sets the [`KeyRange`].
    ///
    /// **Note:** If you intend to set a continuation, then you *must*
    /// use the same [`KeyRange`] used to build the [`KeyValueCursor`]
    /// that returned the continuation.
    pub fn key_range(&mut self, key_range: KeyRange) -> &mut KeyValueCursorBuilder {
        self.key_range = Some(key_range);
        self
    }

    /// Sets the [continuation] bytes that was previously returned.
    ///
    /// [continuation]: crate::cursor::Continuation::to_bytes
    pub fn continuation(&mut self, continuation: Bytes) -> &mut KeyValueCursorBuilder {
        self.continuation = Some(continuation);
        self
    }

    /// Creates the configured [`KeyValueCursor`].
    pub fn build<Tr>(self, read_transaction: &Tr) -> FdbResult<KeyValueCursor>
    where
        Tr: ReadTransaction,
    {
        let scan_properties = self
            .scan_properties
            .ok_or_else(|| FdbError::new(CURSOR_KEYVALUE_CURSOR_BUILDER_ERROR))?;

        let key_range = self
            .key_range
            .ok_or_else(|| FdbError::new(CURSOR_KEYVALUE_CURSOR_BUILDER_ERROR))?;

        let reverse = scan_properties.get_range_options_ref().get_reverse();

        let maybe_continuation_internal = match self.continuation {
            Some(b) => Some(
                KeyValueContinuationInternal::try_from(b)
                    .map_err(|_| FdbError::new(CURSOR_KEYVALUE_CURSOR_BUILDER_ERROR))?,
            ),
            None => None,
        };

        let subspace_length = if let Some(ref s) = self.subspace {
            s.pack().len()
        } else {
            0
        };

        let (range_begin, range_end) = KeyValueCursorBuilder::build_range(
            &self.subspace,
            key_range,
            maybe_continuation_internal.clone(),
            reverse,
        )?
        .into_parts();

        // In order to correctly build a keyvalue stream from the
        // range provided by `KeyValueCursorBuilder::build_range`, we
        // *must* use `KeySelector::first_greater_or_equal`. See
        // `KeyValueCursorBuilder::build_range_bytes` and the
        // integration tests.
        let begin = KeySelector::first_greater_or_equal(range_begin);
        let end = KeySelector::first_greater_or_equal(range_end);

        // Unlike Java RecordLayer, we don't support `skip` because
        // "Key selectors with large offsets are slow".
        //
        // [1] https://apple.github.io/foundationdb/known-limitations.html#dont-use-key-selectors-for-paging

        let values_limit = {
            let limit = scan_properties.get_range_options_ref().get_limit();
            // `KEYVALUE_LIMIT_UNLIMITED` is actually `0`, but since
            // we are dealing with a `i32` type, force any value `<=
            // 0` to become unlimited.
            if limit <= KEYVALUE_LIMIT_UNLIMITED {
                i32::MAX
            } else {
                limit
            }
        };

        let (range_options, scan_limiter) = scan_properties.into_parts();

        let fdb_stream_keyvalue = read_transaction.get_range(begin, end, range_options);

        let limit_manager = LimitManager::new(scan_limiter);

        Ok(match maybe_continuation_internal {
            Some(continuation_internal) => KeyValueCursor::new(
                subspace_length,
                fdb_stream_keyvalue,
                limit_manager,
                values_limit,
                continuation_internal,
            ),
            None => {
                // No continuation was passed. Assume `BeginMarker`.
                KeyValueCursor::new(
                    subspace_length,
                    fdb_stream_keyvalue,
                    limit_manager,
                    values_limit,
                    KeyValueContinuationInternal::new_v1_begin_marker(),
                )
            }
        })
    }

    /// Build a range with an optional continuation.
    ///
    /// *Note:* This method along with
    /// [`bytes_endpoint::build_range_continuation`] and
    /// [`bytes_endpoint::build_range_bytes`] has extensive
    /// integration tests to verify its correctness. Exercise care
    /// when refactoring this code.
    ///
    /// [`bytes_endpoint::build_range_bytes`]: crate::range::bytes_endpoint::build_range_bytes
    pub(crate) fn build_range(
        maybe_subspace: &Option<Subspace>,
        key_range: KeyRange,
        maybe_continuation_internal: Option<KeyValueContinuationInternal>,
        reverse: bool,
    ) -> FdbResult<Range> {
        match maybe_continuation_internal {
            Some(continuation_internal) => match continuation_internal {
                KeyValueContinuationInternal::V1(pb::KeyValueContinuationInternalV1 {
                    key_value_continuation:
                        pb::ProtoKeyValueContinuationEnumV1::BeginMarker(pb::ProtoBeginMarkerV1 {}),
                }) => {
                    // A begin marker is only returned in the event
                    // there was an out-of-band limit (such as a
                    // timeout) even before we could attempt to read
                    // any value in the range. In this case we can
                    // basically build the range assuming no
                    // continuation was passed.
                    bytes_endpoint::build_range_continuation(
                        maybe_subspace,
                        key_range,
                        None,
                        reverse,
                    )
                }
                KeyValueContinuationInternal::V1(pb::KeyValueContinuationInternalV1 {
                    key_value_continuation:
                        pb::ProtoKeyValueContinuationEnumV1::KeyMarker(pb::ProtoKeyMarkerV1 {
                            key_marker,
                        }),
                }) => bytes_endpoint::build_range_continuation(
                    maybe_subspace,
                    key_range,
                    Some(key_marker),
                    reverse,
                ),
                KeyValueContinuationInternal::V1(pb::KeyValueContinuationInternalV1 {
                    key_value_continuation:
                        pb::ProtoKeyValueContinuationEnumV1::EndMarker(pb::ProtoEndMarkerV1 {}),
                }) => {
                    // A end marker means that we have exhausted the
                    // range, but the client is still trying to read
                    // it. In this case, we need to build an empty
                    // range.
                    //
                    // For consistency, how we build the range would
                    // depend on if we are are doing a forward scan or
                    // a reverse scan.
                    let (begin_key, end_key) = bytes_endpoint::build_range_continuation(
                        maybe_subspace,
                        key_range,
                        None,
                        reverse,
                    )?
                    .into_parts();

                    if reverse {
                        Ok(Range::new(begin_key.clone(), begin_key))
                    } else {
                        Ok(Range::new(end_key.clone(), end_key))
                    }
                }
            },
            None => {
                bytes_endpoint::build_range_continuation(maybe_subspace, key_range, None, reverse)
            }
        }
    }
}

impl Default for KeyValueCursorBuilder {
    fn default() -> KeyValueCursorBuilder {
        KeyValueCursorBuilder::new()
    }
}

/// The basic cursor for scanning ranges of the FDB database.
#[derive(Debug)]
pub struct KeyValueCursor {
    subspace_length: usize,
    fdb_stream_keyvalue: FdbStreamKeyValue,
    limit_manager: LimitManager,
    values_limit: i32,
    // Unlike Java RecordLayer we do not maintain `last_key`. Instead
    // we have `continuation`.
    continuation: KeyValueContinuationInternal,
    values_seen: i32,
    error: Option<CursorError>,
}

impl KeyValueCursor {
    /// Create a new [`KeyValueCursor`].
    fn new(
        subspace_length: usize,
        fdb_stream_keyvalue: FdbStreamKeyValue,
        limit_manager: LimitManager,
        values_limit: i32,
        continuation: KeyValueContinuationInternal,
    ) -> KeyValueCursor {
        KeyValueCursor {
            subspace_length,
            fdb_stream_keyvalue,
            limit_manager,
            values_limit,
            continuation,
            values_seen: 0,
            error: None,
        }
    }

    /// Drain the cursor and build a [`BTreeMap`] of [`Key`] and [`Value`].
    pub async fn into_btreemap(mut self) -> (BTreeMap<Key, Value>, CursorError) {
        let mut b: BTreeMap<Key, Value> = BTreeMap::new();

        let iter = &mut self;

        loop {
            match iter.next().await {
                Ok(res) => {
                    let (key, value) = res.into_value().into_parts();
                    b.insert(key, value);
                }
                Err(err) => return (b, err),
            }
        }
    }
}

impl Cursor<KeyValue> for KeyValueCursor {
    async fn next(&mut self) -> CursorResult<KeyValue> {
        if let Some(e) = self.error.as_ref() {
            // First check if `KeyValueCursor` is already in an error
            // state. If so, return the previous error.
            Err(e.clone())
        } else {
            match self.limit_manager.try_keyvalue_scan() {
                Ok(()) => {
                    // No out-of-band limit has been hit, so we can
                    // attempt to get the next value from the stream.
                    //
                    // Check if `values_limit` has been hit or if
                    // `KeyValueCursor` was created using end
                    // marker contiunation? In that case, we can
                    // return an error.
                    if self.values_seen == self.values_limit {
                        let cursor_error = {
                            // The complier complains if we try to use
                            // `CursorResultContinuation::new`
                            // here. Therefore use `Arc::new`.
                            let cursor_result_continuation = Arc::new(self.continuation.clone());
                            CursorError::NoNextReason(NoNextReason::ReturnLimitReached(
                                cursor_result_continuation,
                            ))
                        };

                        // Move cursor into error state and return
                        // error.
                        self.error = Some(cursor_error.clone());
                        Err(cursor_error)
                    } else if let KeyValueContinuationInternal::V1(
                        pb::KeyValueContinuationInternalV1 {
                            key_value_continuation:
                                pb::ProtoKeyValueContinuationEnumV1::EndMarker(pb::ProtoEndMarkerV1 {}),
                        },
                    ) = self.continuation
                    {
                        // KeyValueCursor was built using a
                        // end marker continuation. If so, set
                        // the error state and just return
                        // `SourceExhausted`, without having
                        // to await on the keyvalue stream.
                        let cursor_error = {
                            // The complier complains if we try to
                            // use `CursorResultContinuation::new`
                            // here. Therefore use `Arc::new`.
                            let cursor_result_continuation =
                                Arc::new(KeyValueContinuationInternal::new_v1_end_marker());
                            CursorError::NoNextReason(NoNextReason::SourceExhausted(
                                cursor_result_continuation,
                            ))
                        };

                        // Move cursor into error state and return
                        // error.
                        self.error = Some(cursor_error.clone());
                        Err(cursor_error)
                    } else {
                        self.fdb_stream_keyvalue
                            .next()
                            .await
                            .ok_or_else(|| {
                                // Range exhausted
                                let cursor_error = {
                                    // The complier complains if we try to use
                                    // `CursorResultContinuation::new`
                                    // here. Therefore use `Arc::new`.
                                    let cursor_result_continuation =
                                        Arc::new(KeyValueContinuationInternal::new_v1_end_marker());
                                    CursorError::NoNextReason(NoNextReason::SourceExhausted(
                                        cursor_result_continuation,
                                    ))
                                };

                                // Move cursor into error state and return
                                // error.
                                self.error = Some(cursor_error.clone());
                                cursor_error
                            })
                            .and_then(|kv_res| {
                                kv_res
                                    .map(|kv| {
                                        let (key, value) = kv.into_parts();

                                        let key_bytes = Bytes::from(key);
                                        let value_bytes = Bytes::from(value);

                                        // Update the scanned
                                        // bytes if we are
                                        // tracking it and values
                                        // seen.
                                        self.limit_manager.register_scanned_bytes(
                                            key_bytes.len() + value_bytes.len(),
                                        );
                                        self.values_seen += 1;

                                        // Extract key without subspace
                                        let key =
                                            Key::from(key_bytes.slice(self.subspace_length..));

                                        // Update continuation
                                        self.continuation =
                                            KeyValueContinuationInternal::new_v1_key_marker(
                                                Bytes::from(key.clone()),
                                            );

                                        let keyvalue = KeyValue::new(key, value_bytes);

                                        // The complier complains if we try to use
                                        // `CursorResultContinuation::new`
                                        // here. Therefore use `Arc::new`.
                                        let cursor_result_continuation =
                                            Arc::new(self.continuation.clone());

                                        CursorSuccess::new(keyvalue, cursor_result_continuation)
                                    })
                                    .map_err(|fdb_error| {
                                        let cursor_error = {
                                            // The complier complains if we try to use
                                            // `CursorResultContinuation::new`
                                            // here. Therefore use `Arc::new`.
                                            let cursor_result_continuation =
                                                Arc::new(self.continuation.clone());

                                            CursorError::FdbError(
                                                fdb_error,
                                                cursor_result_continuation,
                                            )
                                        };

                                        // Move cursor into error state and return
                                        // error.
                                        self.error = Some(cursor_error.clone());
                                        cursor_error
                                    })
                            })
                    }
                }
                Err(limit_manager_stopped_reason) => {
                    // Out-of-band limit has been hit. Figure out
                    // which out-of-band limit was hit.
                    let cursor_error = {
                        // The complier complains if we try to use
                        // `CursorResultContinuation::new`
                        // here. Therefore use `Arc::new`.
                        let cursor_result_continuation = Arc::new(self.continuation.clone());
                        CursorError::NoNextReason(match limit_manager_stopped_reason {
                            LimitManagerStoppedReason::KeyValueLimitReached => {
                                NoNextReason::KeyValueLimitReached(cursor_result_continuation)
                            }
                            LimitManagerStoppedReason::ByteLimitReached => {
                                NoNextReason::ByteLimitReached(cursor_result_continuation)
                            }
                            LimitManagerStoppedReason::TimeLimitReached => {
                                NoNextReason::TimeLimitReached(cursor_result_continuation)
                            }
                        })
                    };

                    // Move cursor into error state and return
                    // error.
                    self.error = Some(cursor_error.clone());
                    Err(cursor_error)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    mod key_value_continuation_internal {
        use bytes::Bytes;

        use fdb::error::FdbError;
        use fdb::tuple::Tuple;

        use std::convert::TryFrom;

        use crate::cursor::Continuation;
        use crate::error::CURSOR_INVALID_CONTINUATION;

        use super::super::KeyValueContinuationInternal;

        #[test]
        fn continuation_to_bytes() {
            assert_eq!(
                KeyValueContinuationInternal::new_v1_begin_marker(),
                KeyValueContinuationInternal::try_from(
                    KeyValueContinuationInternal::to_bytes(
                        &KeyValueContinuationInternal::new_v1_begin_marker()
                    )
                    .unwrap(),
                )
                .unwrap()
            );

            assert_eq!(
                KeyValueContinuationInternal::new_v1_key_marker(Bytes::from_static(b"hello_world")),
                KeyValueContinuationInternal::try_from(
                    KeyValueContinuationInternal::to_bytes(
                        &KeyValueContinuationInternal::new_v1_key_marker(Bytes::from_static(
                            b"hello_world"
                        ))
                    )
                    .unwrap(),
                )
                .unwrap()
            );

            assert_eq!(
                KeyValueContinuationInternal::new_v1_end_marker(),
                KeyValueContinuationInternal::try_from(
                    KeyValueContinuationInternal::to_bytes(
                        &KeyValueContinuationInternal::new_v1_end_marker()
                    )
                    .unwrap(),
                )
                .unwrap()
            );
        }

        #[test]
        fn continuation_is_begin_marker() {
            assert!(KeyValueContinuationInternal::is_begin_marker(
                &KeyValueContinuationInternal::new_v1_begin_marker()
            ));

            assert!(!KeyValueContinuationInternal::is_begin_marker(
                &KeyValueContinuationInternal::new_v1_end_marker()
            ));
        }

        #[test]
        fn continuation_is_end_marker() {
            assert!(KeyValueContinuationInternal::is_end_marker(
                &KeyValueContinuationInternal::new_v1_end_marker()
            ));

            assert!(!KeyValueContinuationInternal::is_end_marker(
                &KeyValueContinuationInternal::new_v1_begin_marker()
            ));
        }

        #[test]
        fn try_from_bytes_try_from() {
            {
                let res = <KeyValueContinuationInternal as TryFrom<Bytes>>::try_from(
                    Bytes::from_static(b"some_garbage"),
                );
                assert_eq!(Err(FdbError::new(CURSOR_INVALID_CONTINUATION)), res);
            }

            {
                let continuation_bytes = {
                    let continuation_tup: (i8, Bytes) = (1, Bytes::from_static(b"some_garbage"));
                    let mut tup = Tuple::new();

                    tup.push_back::<i8>(continuation_tup.0);
                    tup.push_back::<Bytes>(continuation_tup.1);

                    tup
                }
                .pack();
                let res =
                    <KeyValueContinuationInternal as TryFrom<Bytes>>::try_from(continuation_bytes);
                assert_eq!(Err(FdbError::new(CURSOR_INVALID_CONTINUATION)), res);
            }

            // valid case
            {
                let keyvalue_continuation_internal =
                    KeyValueContinuationInternal::new_v1_key_marker(Bytes::from_static(
                        b"hello_world",
                    ));

                let continuation_bytes = keyvalue_continuation_internal.to_bytes().unwrap();
                let res =
                    <KeyValueContinuationInternal as TryFrom<Bytes>>::try_from(continuation_bytes);
                assert_eq!(Ok(keyvalue_continuation_internal), res);
            }
        }

        #[test]
        fn try_from_key_value_continuation_internal_try_from() {
            // We do not a have a way to generate Protobuf message
            // `encode` error. So, we can only test valid cases.
            //
            // *Note:*: `to_bytes` and `try_from` uses the same code
            // path.
            {
                let keyvalue_continuation_internal =
                    KeyValueContinuationInternal::new_v1_key_marker(Bytes::from_static(
                        b"hello_world",
                    ));

                let continuation_bytes = keyvalue_continuation_internal.to_bytes();

                let res = <Bytes as TryFrom<KeyValueContinuationInternal>>::try_from(
                    keyvalue_continuation_internal,
                );

                assert_eq!(continuation_bytes, res);
            }

            {
                let keyvalue_continuation_internal =
                    KeyValueContinuationInternal::new_v1_begin_marker();

                let continuation_bytes = keyvalue_continuation_internal.to_bytes();

                let res = <Bytes as TryFrom<KeyValueContinuationInternal>>::try_from(
                    keyvalue_continuation_internal,
                );

                assert_eq!(continuation_bytes, res);
            }

            {
                let keyvalue_continuation_internal =
                    KeyValueContinuationInternal::new_v1_end_marker();

                let continuation_bytes = keyvalue_continuation_internal.to_bytes();

                let res = <Bytes as TryFrom<KeyValueContinuationInternal>>::try_from(
                    keyvalue_continuation_internal,
                );

                assert_eq!(continuation_bytes, res);
            }
        }
    }

    mod key_value_cursor_builder {
        use bytes::Bytes;

        use fdb::subspace::Subspace;

        use crate::range::KeyRange;
        use crate::scan::ScanPropertiesBuilder;

        use super::super::KeyValueCursorBuilder;

        // *Note:* There is no unit-test for
        //         `KeyValueCursorBuilder::build` as that requires us
        //         to create a reference to a value that implements
        //         `ReadTransaction` trait. In our design we cannot do
        //         that without opening an actual FDB database. So we
        //         test that in the integration test.
        //
        //         `KeyValueCursorBuilder::build_range` is also
        //         testing using integration tests.
        #[test]
        fn subspace() {
            let mut kv_cursor_builder = KeyValueCursorBuilder::new();

            kv_cursor_builder.subspace(Subspace::new(Bytes::from_static(b"hello_world")));

            assert_eq!(
                Some(Subspace::new(Bytes::from_static(b"hello_world"))),
                kv_cursor_builder.subspace
            );
        }

        #[test]
        fn scan_properties() {
            let mut kv_cursor_builder = KeyValueCursorBuilder::new();

            kv_cursor_builder.scan_properties(ScanPropertiesBuilder::new().build());

            assert_eq!(
                Some(ScanPropertiesBuilder::new().build()),
                kv_cursor_builder.scan_properties
            );
        }

        #[test]
        fn key_range() {
            let mut kv_cursor_builder = KeyValueCursorBuilder::new();

            kv_cursor_builder.key_range(KeyRange::from_keys(
                Bytes::from_static(b"aaa").into(),
                Bytes::from_static(b"bbb").into(),
            ));

            assert_eq!(
                Some(KeyRange::from_keys(
                    Bytes::from_static(b"aaa").into(),
                    Bytes::from_static(b"bbb").into()
                )),
                kv_cursor_builder.key_range
            );
        }

        #[test]
        fn continuation() {
            let mut kv_cursor_builder = KeyValueCursorBuilder::new();

            kv_cursor_builder.continuation(Bytes::from_static(b"hello_world").into());

            assert_eq!(
                Some(Bytes::from_static(b"hello_world").into()),
                kv_cursor_builder.continuation
            );
        }

        #[test]
        fn default_default() {
            let kv_cursor_builder = KeyValueCursorBuilder::default();

            assert_eq!(None, kv_cursor_builder.subspace,);

            assert_eq!(None, kv_cursor_builder.scan_properties);

            assert_eq!(None, kv_cursor_builder.key_range,);

            assert_eq!(None, kv_cursor_builder.continuation,);
        }
    }

    mod key_value_cursor {
        // There are no unit tests for `KeyValueCursor` as the primary
        // use of this type is to implement the `Cursor` trait. There
        // are integration tests that checks the behaviour of the
        // methods in the `Cursor` trait.
    }
}
