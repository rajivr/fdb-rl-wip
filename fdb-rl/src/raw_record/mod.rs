//! Provides [`RawRecord`] type and associated items.

mod primary_key;
mod scan_state_machine;

use bytes::{Bytes, BytesMut};

use fdb::error::{FdbError, FdbResult};
use fdb::range::StreamingMode;
use fdb::subspace::Subspace;
use fdb::transaction::ReadTransaction;
use fdb::tuple::Tuple;

use prost::Message;

use std::convert::{TryFrom, TryInto};

use crate::cursor::{
    Continuation, Cursor, CursorResult, KeyValueContinuationInternal, KeyValueCursor,
};
use crate::error::CURSOR_INVALID_CONTINUATION;
use crate::scan::ScanLimiter;
use crate::RecordVersion;

pub(crate) use primary_key::{RawRecordPrimaryKey, RawRecordPrimaryKeySchema};

use scan_state_machine::RawRecordStateMachine;

/// Protobuf types.
pub(crate) mod pb {
    use fdb::error::{FdbError, FdbResult};

    use std::convert::{TryFrom, TryInto};

    use crate::error::CURSOR_INVALID_CONTINUATION;

    pub(crate) use crate::cursor::pb::KeyValueContinuationInternalV1;

    // Protobuf generated types renamed to prepend `Proto` and append
    // version.
    pub(crate) use fdb_rl_proto::cursor::v1::RawRecordContinuation as ProtoRawRecordContinuationV1;

    /// Protobuf message `fdb_rl.cursor.v1.RawRecordContinuation`
    /// contains a `Required` field. So, we need to define this type.
    ///
    /// The `inner` field `KeyValueContinuationV1` *also* contains a
    /// `Required` field. So, rather than using protobuf generated
    /// `KeyValueContinuationV1`, we use
    /// `KeyValueContinuationInternalV1`.
    #[derive(Clone, Debug, PartialEq)]
    pub(crate) struct RawRecordContinuationInternalV1 {
        pub(crate) inner: KeyValueContinuationInternalV1,
    }

    impl TryFrom<ProtoRawRecordContinuationV1> for RawRecordContinuationInternalV1 {
        type Error = FdbError;

        fn try_from(
            rawrecord_continuation_v1: ProtoRawRecordContinuationV1,
        ) -> FdbResult<RawRecordContinuationInternalV1> {
            rawrecord_continuation_v1
                .inner
                .ok_or_else(|| FdbError::new(CURSOR_INVALID_CONTINUATION))
                .and_then(|keyvalue_continuation_v1| {
                    keyvalue_continuation_v1
                        .try_into()
                        .map(
                            |keyvalue_continuation_internal_v1| RawRecordContinuationInternalV1 {
                                inner: keyvalue_continuation_internal_v1,
                            },
                        )
                })
        }
    }

    impl From<RawRecordContinuationInternalV1> for ProtoRawRecordContinuationV1 {
        fn from(
            rawrecord_continuation_internal_v1: RawRecordContinuationInternalV1,
        ) -> ProtoRawRecordContinuationV1 {
            ProtoRawRecordContinuationV1 {
                inner: Some(rawrecord_continuation_internal_v1.inner.into()),
            }
        }
    }
}

/// A wrapper around all information that can be determined about a
/// record before serializing and deserializing it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RawRecord {
    primary_key: RawRecordPrimaryKey,
    version: RecordVersion,
    record_bytes: Bytes,
}

// No tests for this because we are just constructing a [`RawRecord`].
impl From<(RawRecordPrimaryKey, RecordVersion, Bytes)> for RawRecord {
    fn from(
        (primary_key, version, record_bytes): (RawRecordPrimaryKey, RecordVersion, Bytes),
    ) -> RawRecord {
        RawRecord {
            primary_key,
            version,
            record_bytes,
        }
    }
}

/// Internal representation of [`RawRecord`] continuation.
///
/// We need define this type so we can implement [`Continuation`]
/// trait on it. In addition it has `TryFrom<Bytes> for
/// RawRecordContinuationInternal` and
/// `TryFrom<RawRecordContinuationInternal> for Bytes` traits
/// implemented so we can convert between `Bytes` and
/// `KeyValueContinuationInternal`.
///
/// We do not implement `new_v1_begin_marker()`,
/// `new_v1_key_marker(..)`, `new_v1_end_marker()` for
/// [`RawRecordContinuationInternal`] because it is just a wrapper
/// around [`pb::KeyValueContinuationInternalV1`].
///
/// Values of [`RawRecordContinuationInternal`] can be generated using
/// `new_v1_begin_marker()`, `new_v1_key_marker(..)`,
/// `new_v1_end_marker()` methods on [`KeyValueContinuationInternal`],
/// extracting out [`pb::KeyValueContinuationInternalV1`] value and
/// using the `From` trait.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RawRecordContinuationInternal {
    V1(pb::RawRecordContinuationInternalV1),
}

impl From<pb::KeyValueContinuationInternalV1> for RawRecordContinuationInternal {
    fn from(
        pb_keyvalue_continuation_internal_v1: pb::KeyValueContinuationInternalV1,
    ) -> RawRecordContinuationInternal {
        RawRecordContinuationInternal::V1(pb::RawRecordContinuationInternalV1 {
            inner: pb_keyvalue_continuation_internal_v1,
        })
    }
}

impl TryFrom<RawRecordContinuationInternal> for Bytes {
    type Error = FdbError;

    fn try_from(
        rawrecord_continuation_internal: RawRecordContinuationInternal,
    ) -> FdbResult<Bytes> {
        match rawrecord_continuation_internal {
            RawRecordContinuationInternal::V1(rawrecord_continuation_internal_v1) => {
                let rawrecord_continuation_v1 =
                    pb::ProtoRawRecordContinuationV1::from(rawrecord_continuation_internal_v1);

                let mut buf = BytesMut::with_capacity(rawrecord_continuation_v1.encoded_len());

                rawrecord_continuation_v1
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

impl TryFrom<Bytes> for RawRecordContinuationInternal {
    type Error = FdbError;

    fn try_from(continuation: Bytes) -> FdbResult<RawRecordContinuationInternal> {
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

        // Currently there is only one version
        if version == 1 {
            let rawrecord_continuation_internal_v1 =
                pb::ProtoRawRecordContinuationV1::decode(continuation)
                    .map_err(|_| FdbError::new(CURSOR_INVALID_CONTINUATION))?
                    .try_into()?;

            Ok(RawRecordContinuationInternal::V1(
                rawrecord_continuation_internal_v1,
            ))
        } else {
            Err(FdbError::new(CURSOR_INVALID_CONTINUATION))
        }
    }
}

impl Continuation for RawRecordContinuationInternal {
    fn to_bytes(&self) -> FdbResult<Bytes> {
        self.clone().try_into()
    }

    fn is_begin_marker(&self) -> bool {
        match self {
            RawRecordContinuationInternal::V1(pb::RawRecordContinuationInternalV1 { inner }) => {
                KeyValueContinuationInternal::V1(inner.clone()).is_begin_marker()
            }
        }
    }

    fn is_end_marker(&self) -> bool {
        match self {
            RawRecordContinuationInternal::V1(pb::RawRecordContinuationInternalV1 { inner }) => {
                KeyValueContinuationInternal::V1(inner.clone()).is_end_marker()
            }
        }
    }
}

/// A builder for [`RawRecordCursor`]. A value of [`RawRecordCursor`]
/// can be built as shown below.
///
/// ```ignore
/// TODO
/// ```
//
// TODO: You need to take care of issues around limit. Limit *cannot*
// be zero.
//
// The `KeyValueCursorBuilder` takes a value of `ScanProperties`. We
// *cannot* directly expose `ScanProperties` to the user of
// `RawRecordCursorBuilder` because that would allow the user to do
// weird things like setting `RangeOptions` limit value and cause a
// in-band `NoNextReason::ReturnLimitReached` error on the inner
// `KeyValueCursor`.
//
// Getting a in-band `NoNextReason::ReturnLimitReached` for the inner
// `KeyValueCursor` does not make any sense in the context of
// `RawRecordCursor`.
//
// In the context of `RawRecordCursor` we would need to return
// `NoNextReason::ReturnLimitReached` *only* when we have returned
// `limit` number of records. That is in no way connected to getting a
// `NoNextReason::ReturnLimitReached` from the underlying
// `KeyValueCursor`. Infact we should *never* get
// ``NoNextReason::ReturnLimitReached` from the underlying
// `KeyValueCursor`.
//
// To prevent such condition from happening, we take in a
// `ScanLimiter` and `StreamingMode` and create the `ScanProperties`
// value.
pub(crate) struct RawRecordCursorBuilder {
    primary_key_schema: Option<RawRecordPrimaryKeySchema>,
    subspace: Option<Subspace>,
    scan_limiter: Option<ScanLimiter>,
    streaming_mode: Option<StreamingMode>,
    limit: Option<usize>,
    reverse: Option<bool>,
    continuation: Option<Bytes>,
}

impl RawRecordCursorBuilder {
    /// Return a new builder.
    pub(crate) fn new() -> RawRecordCursorBuilder {
        RawRecordCursorBuilder {
            primary_key_schema: None,
            subspace: None,
            scan_limiter: None,
            streaming_mode: None,
            continuation: None,
            limit: None,
            reverse: None,
        }
    }

    /// Sets the [`RawRecordPrimaryKeySchema`].
    ///
    /// **Note:** If you intend to set a continuation, then you *must*
    /// use the same [`RawRecordPrimaryKeySchema`] used to build the
    /// [`RawRecordCursor`] that returned the continuation.
    pub(crate) fn primary_key_schema(
        &mut self,
        primary_key_schema: RawRecordPrimaryKeySchema,
    ) -> &mut RawRecordCursorBuilder {
        self.primary_key_schema = Some(primary_key_schema);
        self
    }

    /// Sets the [`Subspace`]
    ///
    /// **Note:** If you intend to set a continuation, then you *must*
    /// use the same [`Subspace`] used to build the
    /// [`RawRecordCursor`] that returned the continuation.
    pub(crate) fn subspace(&mut self, subspace: Subspace) -> &mut RawRecordCursorBuilder {
        self.subspace = Some(subspace);
        self
    }

    /// Sets the [`ScanLimiter`]
    pub(crate) fn scan_limiter(
        &mut self,
        scan_limiter: ScanLimiter,
    ) -> &mut RawRecordCursorBuilder {
        self.scan_limiter = Some(scan_limiter);
        self
    }

    /// Sets the [`StreamingMode`]
    ///
    /// **Note:** If you intend to set a continuation, then you *must*
    /// use the same [`StreamingMode`] used to build the
    /// [`RawRecordCursor`] that returned the continuation.
    pub(crate) fn streaming_mode(
        &mut self,
        streaming_mode: StreamingMode,
    ) -> &mut RawRecordCursorBuilder {
        self.streaming_mode = Some(streaming_mode);
        self
    }

    /// Sets the limit for the number of [`RawRecord`]s to return.
    ///
    /// # Note
    ///
    /// You **cannot** set the the limit to `0`. If you intend to set
    /// a continuation, then you *must* adjust the limit parameter
    /// based on already returned number of [`RawRecord`]s.
    //
    // This limit is in *no way* connected to limit on the inner
    // `KeyValueCursor`. Infact, the inner `KeyValueCursor` cannot
    // have in-band limit. Since the two limits are orthogonal, we can
    // safely use a value of type `usize` for `RawRecordCursor` limit.
    pub(crate) fn limit(&mut self, limit: usize) -> &mut RawRecordCursorBuilder {
        self.limit = Some(limit);
        self
    }

    /// Sets read order (lexicographic or non-lexicographic) of the
    /// primary key.
    ///
    /// **Note:** If you intend to set a continuation, then you *must*
    /// use the same value of `reverse` used to build the
    /// [`RawRecordCursor`] that returned the continuation.
    pub(crate) fn reverse(&mut self, reverse: bool) -> &mut RawRecordCursorBuilder {
        self.reverse = Some(reverse);
        self
    }

    /// Sets the [continuation] bytes that was previously returned.
    ///
    /// [continuation]: crate::cursor::Continuation::to_bytes
    pub(crate) fn continuation(&mut self, continuation: Bytes) -> &mut RawRecordCursorBuilder {
        self.continuation = Some(continuation);
        self
    }

    /// Creates the configured [`RawRecordCursor`].
    pub(crate) fn build<Tr>(self, read_transaction: &Tr) -> FdbResult<RawRecordCursor>
    where
        Tr: ReadTransaction,
    {
        let RawRecordCursorBuilder {
            primary_key_schema,
            subspace,
            scan_limiter,
            streaming_mode,
            continuation,
            limit,
            reverse,
        } = self;

        // TODO: Continue from here.

        todo!();
    }
}

/// A cursor that returns [`RawRecord`]s from the FDB database.
//
// TODO: `NoNextReason::ReturnLimitReached` would be specific number
// of `RawRecord`.
#[derive(Debug)]
pub(crate) struct RawRecordCursor {
    primary_key_schema: RawRecordPrimaryKeySchema,
    values_limit: usize,
    key_value_cursor: KeyValueCursor,
    raw_record_state_machine: RawRecordStateMachine,
}

impl Cursor<RawRecord> for RawRecordCursor {
    /// Return the next [`RawRecord`].
    ///
    /// In regular state machines, where transitions are represented
    /// using `event [guard] / action` and we directly send the event
    /// to the state machine.
    ///
    /// *However*, in this case, all side effect of reading from the
    /// database is managed by the driver loop (below) and we only use
    /// the state machine to manage state data.
    ///
    /// When we are in a state where we can return data (or error), we
    /// exit the loop and return the data. This is managed by
    /// returning a `Some(_: CursorResult<RawRecord>)` value.
    async fn next(&mut self) -> CursorResult<RawRecord> {
        loop {
            match self.raw_record_state_machine {
                RawRecordStateMachine::ForwardScan(ref mut forward_scan_state_machine) => {
                    if let Some(res) = forward_scan_state_machine
                        .next(
                            &mut self.key_value_cursor,
                            &self.primary_key_schema,
                            self.values_limit,
                        )
                        .await
                    {
                        return res;
                    }
                }
                RawRecordStateMachine::ReverseScan(ref mut reverse_scan_state_machine) => {
                    if let Some(res) = reverse_scan_state_machine
                        .next(
                            &mut self.key_value_cursor,
                            &self.primary_key_schema,
                            self.values_limit,
                        )
                        .await
                    {
                        return res;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    mod raw_record_continuation_internal {
        use bytes::Bytes;

        use fdb::error::FdbError;
        use fdb::tuple::Tuple;

        use std::convert::TryFrom;

        use crate::cursor::{Continuation, KeyValueContinuationInternal};
        use crate::error::CURSOR_INVALID_CONTINUATION;

        use super::super::RawRecordContinuationInternal;

        #[test]
        fn continuation_to_bytes() {
            assert_eq!(
                {
                    let KeyValueContinuationInternal::V1(pb_keyvalue_continuation_internal_v1) =
                        KeyValueContinuationInternal::new_v1_begin_marker();

                    RawRecordContinuationInternal::from(pb_keyvalue_continuation_internal_v1)
                },
                RawRecordContinuationInternal::try_from(
                    RawRecordContinuationInternal::to_bytes(&{
                        let KeyValueContinuationInternal::V1(pb_keyvalue_continuation_internal_v1) =
                            KeyValueContinuationInternal::new_v1_begin_marker();

                        RawRecordContinuationInternal::from(pb_keyvalue_continuation_internal_v1)
                    })
                    .unwrap(),
                )
                .unwrap()
            );

            assert_eq!(
                {
                    let KeyValueContinuationInternal::V1(pb_keyvalue_continuation_internal_v1) =
                        KeyValueContinuationInternal::new_v1_key_marker(Bytes::from_static(
                            b"hello_world",
                        ));

                    RawRecordContinuationInternal::from(pb_keyvalue_continuation_internal_v1)
                },
                RawRecordContinuationInternal::try_from(
                    RawRecordContinuationInternal::to_bytes(&{
                        let KeyValueContinuationInternal::V1(pb_keyvalue_continuation_internal_v1) =
                            KeyValueContinuationInternal::new_v1_key_marker(Bytes::from_static(
                                b"hello_world",
                            ));

                        RawRecordContinuationInternal::from(pb_keyvalue_continuation_internal_v1)
                    })
                    .unwrap(),
                )
                .unwrap()
            );

            assert_eq!(
                {
                    let KeyValueContinuationInternal::V1(pb_keyvalue_continuation_internal_v1) =
                        KeyValueContinuationInternal::new_v1_end_marker();

                    RawRecordContinuationInternal::from(pb_keyvalue_continuation_internal_v1)
                },
                RawRecordContinuationInternal::try_from(
                    RawRecordContinuationInternal::to_bytes(&{
                        let KeyValueContinuationInternal::V1(pb_keyvalue_continuation_internal_v1) =
                            KeyValueContinuationInternal::new_v1_end_marker();

                        RawRecordContinuationInternal::from(pb_keyvalue_continuation_internal_v1)
                    })
                    .unwrap(),
                )
                .unwrap()
            );
        }

        #[test]
        fn continuation_is_begin_marker() {
            assert!(RawRecordContinuationInternal::is_begin_marker(&{
                let KeyValueContinuationInternal::V1(pb_keyvalue_continuation_internal_v1) =
                    KeyValueContinuationInternal::new_v1_begin_marker();

                RawRecordContinuationInternal::from(pb_keyvalue_continuation_internal_v1)
            }));

            assert!(!RawRecordContinuationInternal::is_begin_marker(&{
                let KeyValueContinuationInternal::V1(pb_keyvalue_continuation_internal_v1) =
                    KeyValueContinuationInternal::new_v1_end_marker();

                RawRecordContinuationInternal::from(pb_keyvalue_continuation_internal_v1)
            }));
        }

        #[test]
        fn continuation_is_end_marker() {
            assert!(RawRecordContinuationInternal::is_end_marker(&{
                let KeyValueContinuationInternal::V1(pb_keyvalue_continuation_internal_v1) =
                    KeyValueContinuationInternal::new_v1_end_marker();

                RawRecordContinuationInternal::from(pb_keyvalue_continuation_internal_v1)
            }));

            assert!(!RawRecordContinuationInternal::is_end_marker(&{
                let KeyValueContinuationInternal::V1(pb_keyvalue_continuation_internal_v1) =
                    KeyValueContinuationInternal::new_v1_begin_marker();

                RawRecordContinuationInternal::from(pb_keyvalue_continuation_internal_v1)
            }));
        }

        #[test]
        fn try_from_bytes_try_from() {
            {
                let res = <RawRecordContinuationInternal as TryFrom<Bytes>>::try_from(
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
                    <RawRecordContinuationInternal as TryFrom<Bytes>>::try_from(continuation_bytes);
                assert_eq!(Err(FdbError::new(CURSOR_INVALID_CONTINUATION)), res);
            }

            // valid case
            {
                let raw_record_continuation_internal = {
                    let KeyValueContinuationInternal::V1(pb_keyvalue_continuation_internal_v1) =
                        KeyValueContinuationInternal::new_v1_key_marker(Bytes::from_static(
                            b"hello_world",
                        ));

                    RawRecordContinuationInternal::from(pb_keyvalue_continuation_internal_v1)
                };

                let continuation_bytes = raw_record_continuation_internal.to_bytes().unwrap();
                let res =
                    <RawRecordContinuationInternal as TryFrom<Bytes>>::try_from(continuation_bytes);
                assert_eq!(Ok(raw_record_continuation_internal), res);
            }
        }

        #[test]
        fn try_from_raw_record_continuation_internal_try_from() {
            // We do not a have a way to generate Protobuf message
            // `encode` error. So, we can only test valid cases.
            //
            // *Note:*: `to_bytes` and `try_from` uses the same code
            // path.
            {
                let raw_record_continuation_internal = {
                    let KeyValueContinuationInternal::V1(pb_keyvalue_continuation_internal_v1) =
                        KeyValueContinuationInternal::new_v1_key_marker(Bytes::from_static(
                            b"hello_world",
                        ));

                    RawRecordContinuationInternal::from(pb_keyvalue_continuation_internal_v1)
                };

                let continuation_bytes = raw_record_continuation_internal.to_bytes();

                let res = <Bytes as TryFrom<RawRecordContinuationInternal>>::try_from(
                    raw_record_continuation_internal,
                );

                assert_eq!(continuation_bytes, res);
            }

            {
                let raw_record_continuation_internal = {
                    let KeyValueContinuationInternal::V1(pb_keyvalue_continuation_internal_v1) =
                        KeyValueContinuationInternal::new_v1_begin_marker();

                    RawRecordContinuationInternal::from(pb_keyvalue_continuation_internal_v1)
                };

                let continuation_bytes = raw_record_continuation_internal.to_bytes();

                let res = <Bytes as TryFrom<RawRecordContinuationInternal>>::try_from(
                    raw_record_continuation_internal,
                );

                assert_eq!(continuation_bytes, res);
            }

            {
                let raw_record_continuation_internal = {
                    let KeyValueContinuationInternal::V1(pb_keyvalue_continuation_internal_v1) =
                        KeyValueContinuationInternal::new_v1_end_marker();

                    RawRecordContinuationInternal::from(pb_keyvalue_continuation_internal_v1)
                };

                let continuation_bytes = raw_record_continuation_internal.to_bytes();

                let res = <Bytes as TryFrom<RawRecordContinuationInternal>>::try_from(
                    raw_record_continuation_internal,
                );

                assert_eq!(continuation_bytes, res);
            }
        }
    }
}
