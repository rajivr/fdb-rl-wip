//! Provides [`RawRecord`] type and associated items.
use bytes::{BufMut, Bytes, BytesMut};

use fdb::error::{FdbError, FdbResult};
use fdb::range::StreamingMode;
use fdb::subspace::Subspace;
use fdb::transaction::ReadTransaction;
use fdb::tuple::{Tuple, TupleSchema, TupleSchemaElement};
use fdb::Value;

use prost::Message;

use std::any::Any;
use std::collections::BTreeMap;
use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use crate::cursor::{
    Continuation, Cursor, CursorError, CursorResult, CursorSuccess, KeyValueContinuationInternal,
    KeyValueCursor, LimitManagerStoppedReason, NoNextReason,
};
use crate::error::{
    CURSOR_INVALID_CONTINUATION, RAW_RECORD_CURSOR_NEXT_ERROR, RAW_RECORD_CURSOR_STATE_ERROR,
    RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA, RAW_RECORD_PRIMARY_KEY_TUPLE_SCHEMA_MISMATCH,
};
use crate::split_helper::RecordHeaderV0;
use crate::RecordVersion;

/// Protobuf types.
pub(crate) mod pb {
    use fdb::error::{FdbError, FdbResult};

    use std::convert::{TryFrom, TryInto};

    use crate::error::CURSOR_INVALID_CONTINUATION;

    pub(crate) use crate::cursor::pb::KeyValueContinuationInternalV1;

    // Protobuf generated types renamed to append version.
    pub(crate) use fdb_rl_proto::cursor::v1::RawRecordContinuation as RawRecordContinuationV1;

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

    impl TryFrom<RawRecordContinuationV1> for RawRecordContinuationInternalV1 {
        type Error = FdbError;

        fn try_from(
            rawrecord_continuation_v1: RawRecordContinuationV1,
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

    impl From<RawRecordContinuationInternalV1> for RawRecordContinuationV1 {
        fn from(
            rawrecord_continuation_internal_v1: RawRecordContinuationInternalV1,
        ) -> RawRecordContinuationV1 {
            RawRecordContinuationV1 {
                inner: Some(rawrecord_continuation_internal_v1.inner.into()),
            }
        }
    }
}

trait Visitor {
    fn visit_tuple_schema_element(&self, tuple_schema_element: &TupleSchemaElement) -> bool;
}

/// We do not allow the primary key's [`TupleSchema`] of [`RawRecord`]
/// to be empty or to have a `Null` or `Versionstamp` or a nullable
/// type (such as `MaybeXYZ`) or a empty nested tuple.
#[derive(Debug)]
struct PrimaryKeySchemaValidatorVisitor;

impl Visitor for PrimaryKeySchemaValidatorVisitor {
    fn visit_tuple_schema_element(&self, tuple_schema_element: &TupleSchemaElement) -> bool {
        match tuple_schema_element {
            TupleSchemaElement::Null
            | TupleSchemaElement::Versionstamp
            | TupleSchemaElement::MaybeBytes
            | TupleSchemaElement::MaybeString
            | TupleSchemaElement::MaybeTuple(_)
            | TupleSchemaElement::MaybeInteger
            | TupleSchemaElement::MaybeFloat
            | TupleSchemaElement::MaybeDouble
            | TupleSchemaElement::MaybeBoolean
            | TupleSchemaElement::MaybeUuid
            | TupleSchemaElement::MaybeVersionstamp => false,
            TupleSchemaElement::Bytes
            | TupleSchemaElement::String
            | TupleSchemaElement::Integer
            | TupleSchemaElement::Float
            | TupleSchemaElement::Double
            | TupleSchemaElement::Boolean
            | TupleSchemaElement::Uuid => true,
            TupleSchemaElement::Tuple(ts) => walk_tuple_schema(self, ts),
        }
    }
}

fn walk_tuple_schema(visitor: &dyn Visitor, tuple_schema: &TupleSchema) -> bool {
    if tuple_schema.len() == 0 {
        false
    } else {
        for tuple_schema_element in tuple_schema.iter() {
            if !walk_tuple_schema_element(visitor, tuple_schema_element) {
                return false;
            }
        }
        true
    }
}

fn walk_tuple_schema_element(
    visitor: &dyn Visitor,
    tuple_schema_element: &TupleSchemaElement,
) -> bool {
    visitor.visit_tuple_schema_element(tuple_schema_element)
}

/// Represents the schema for a [`RawRecordPrimaryKey`].
///
/// It consists of a [`TupleSchema`]. When we have a value of
/// [`RawRecordPrimaryKeySchema`], that means that the [`TupleSchema`]
/// satisfies the constraints to be a primary key schema.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RawRecordPrimaryKeySchema {
    inner: TupleSchema,
}

impl RawRecordPrimaryKeySchema {
    /// Get reference to [`TupleSchema].
    fn get_tuple_schema_ref(&self) -> &TupleSchema {
        &self.inner
    }
}

impl TryFrom<TupleSchema> for RawRecordPrimaryKeySchema {
    type Error = FdbError;

    fn try_from(tuple_schema: TupleSchema) -> FdbResult<RawRecordPrimaryKeySchema> {
        if walk_tuple_schema(&PrimaryKeySchemaValidatorVisitor, &tuple_schema) {
            Ok(RawRecordPrimaryKeySchema {
                inner: tuple_schema,
            })
        } else {
            Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
        }
    }
}

/// Represents a [`RawRecord`]'s primary key which is a [`Tuple`].
///
/// When we have a value of [`RawRecordPrimaryKey`], that means that
/// it conforms to [`RawRecordPrimaryKeySchema`].
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RawRecordPrimaryKey {
    schema: RawRecordPrimaryKeySchema,
    key: Tuple,
}

impl TryFrom<(RawRecordPrimaryKeySchema, Tuple)> for RawRecordPrimaryKey {
    type Error = FdbError;

    fn try_from(
        (schema, key): (RawRecordPrimaryKeySchema, Tuple),
    ) -> FdbResult<RawRecordPrimaryKey> {
        if schema.get_tuple_schema_ref().validate(&key) {
            Ok(RawRecordPrimaryKey { schema, key })
        } else {
            Err(FdbError::new(RAW_RECORD_PRIMARY_KEY_TUPLE_SCHEMA_MISMATCH))
        }
    }
}

/// A wrapper around all information that can be determined about a
/// record before serializing and deserializing it.
#[derive(Debug)]
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
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RawRecordContinuationInternal {
    V1(pb::RawRecordContinuationInternalV1),
}

impl TryFrom<RawRecordContinuationInternal> for Bytes {
    type Error = FdbError;

    fn try_from(
        rawrecord_continuation_internal: RawRecordContinuationInternal,
    ) -> FdbResult<Bytes> {
        match rawrecord_continuation_internal {
            RawRecordContinuationInternal::V1(rawrecord_continuation_internal_v1) => {
                let rawrecord_continuation_v1 =
                    pb::RawRecordContinuationV1::from(rawrecord_continuation_internal_v1);

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
                pb::RawRecordContinuationV1::decode(continuation)
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

// TODO: You need to take care of issues around limit. Limit *cannot* be zero.
pub(crate) struct RawRecordCursorBuilder {
    primary_key_schema: Option<RawRecordPrimaryKeySchema>,
    subspace: Option<Subspace>,
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
        todo!();
    }
}

#[derive(Debug)]
enum RawRecordForwardScanStateMachineState {
    InitiateRecordVersionRead,
    ReadRecordVersion,
    RawRecordAvailable,
    RawRecordNextError,
    RawRecordLimitReached,
    // When the underlying key value cursor ends in a consistent
    // state, the cursor would enter `RawRecordEndOfStream`
    // state. Otherwise we would enter `RawRecordNextError` state.
    RawRecordEndOfStream,
    OutOfBandError,
    FdbError,
}

#[derive(Debug)]
enum RawRecordForwardScanStateMachineStateData {
    InitiateRecordVersionRead {
        continuation: RawRecordContinuationInternal,
    },
    ReadRecordVersion {
        data_splits: i8,
        record_version: RecordVersion,
        primary_key: RawRecordPrimaryKey,
        continuation: RawRecordContinuationInternal,
        records_already_returned: usize,
    },
    RawRecordAvailable {
        raw_record: RawRecord,
        continuation: RawRecordContinuationInternal,
        records_already_returned: usize,
    },
    RawRecordNextError {
        continuation: RawRecordContinuationInternal,
    },
    RawRecordLimitReached {
        continuation: RawRecordContinuationInternal,
    },
    RawRecordEndOfStream,
    OutOfBandError {
        out_of_band_error_type: LimitManagerStoppedReason,
        continuation: RawRecordContinuationInternal,
    },
    FdbError {
        fdb_error: FdbError,
        continuation: RawRecordContinuationInternal,
    },
}

#[derive(Debug)]
enum RawRecordForwardScanStateMachineEvent {
    RecordVersionOk {
        data_splits: i8,
        record_version: RecordVersion,
        primary_key: RawRecordPrimaryKey,
        continuation: RawRecordContinuationInternal,
        records_already_returned: usize,
    },
    Available {
        raw_record: RawRecord,
        continuation: RawRecordContinuationInternal,
        records_already_returned: usize,
    },
    NextRecordVersionOk {
        record_version: RecordVersion,
        primary_key: RawRecordPrimaryKey,
        continuation: RawRecordContinuationInternal,
        records_already_returned: usize,
    },
    NextError {
        continuation: RawRecordContinuationInternal,
    },
    LimitReached {
        continuation: RawRecordContinuationInternal,
    },
    EndOfStream,
    OutOfBandError {
        out_of_band_error_type: LimitManagerStoppedReason,
        continuation: RawRecordContinuationInternal,
    },
    FdbError {
        fdb_error: FdbError,
        continuation: RawRecordContinuationInternal,
    },
}

#[derive(Debug)]
/// A state machine that implements forward scan and returns values of
/// of type [`RawRecord`].
///
/// See `sismic/...TODO` for the design of the state machine.
struct RawRecordForwardScanStateMachine {
    state_machine_state: RawRecordForwardScanStateMachineState,
    // We use `Option` here so that we can take ownership of the data
    // and pass it as part of the event. This would avoid unnecessary
    // cloning.
    //
    // This value is taken in `next` method and assigned `Some(...)`
    // value in `step_once_with_event` method. In final states, we do
    // not take the value, so there is no need to assign it back.
    state_machine_data: Option<RawRecordForwardScanStateMachineStateData>,
}

impl RawRecordForwardScanStateMachine {
    /// If needed, perform the action (side effect) and state
    /// transition. Return an `Option` value or `None` in case we need
    /// to further drive the loop.
    async fn next(
        &mut self,
        key_value_cursor: &mut KeyValueCursor,
        primary_key_schema: &RawRecordPrimaryKeySchema,
    ) -> Option<CursorResult<RawRecord>> {
        let _: Option<CursorResult<RawRecord>> = match self.state_machine_state {
            RawRecordForwardScanStateMachineState::InitiateRecordVersionRead => {
                // Extract and verify state data.
                //
                // Non-final state. We *must* call
                // `step_once_with_event`.
                let continuation = self
                    .state_machine_data
                    .take()
                    .and_then(|state_machine_data| {
                        if let RawRecordForwardScanStateMachineStateData::InitiateRecordVersionRead {
                            continuation,
                        } = state_machine_data
                        {
                            Some(continuation)
                        } else {
                            None
                        }
                    })
                    .expect("invalid state_machine_data");

                let next_kv = key_value_cursor.next().await;

                match next_kv {
                    Ok(cursor_success) => {
                        let (key, value) = cursor_success.into_value().into_parts();

                        // Extract a value of type
                        // `FdbResult<(RawRecordPrimaryKey, i8,
                        // RecordVersion)>`, which will give us the
                        // information that we need to make the
                        // correct transition.
                        let res = Tuple::try_from(key)
                            .and_then(|mut tup| {
                                // Verify that the split index is
                                // `-1`.
                                let idx = tup
                                    .pop_back::<i8>()
                                    .ok_or_else(|| FdbError::new(RAW_RECORD_CURSOR_NEXT_ERROR))?;
                                if idx == -1 {
                                    Ok(tup)
                                } else {
                                    Err(FdbError::new(RAW_RECORD_CURSOR_NEXT_ERROR))
                                }
                            })
                            .and_then(|tup| {
                                // Verify that tuple matches the
                                // primary key schema.
                                RawRecordPrimaryKey::try_from((primary_key_schema.clone(), tup))
                            })
                            .and_then(|primary_key| {
                                let (data_splits, record_version) =
                                    RecordHeaderV0::try_from(value)?.into_parts();

                                Ok((primary_key, data_splits, record_version))
                            });

                        match res {
                            Ok((primary_key, data_splits, record_version)) => {
                                // We have only the first record's
                                // record version, primary key, data
                                // splits and not its contents yet. So
                                // we set this value to `0`.
                                let records_already_returned = 0;

                                self.step_once_with_event(
                                    RawRecordForwardScanStateMachineEvent::RecordVersionOk {
                                        data_splits,
                                        record_version,
                                        primary_key,
                                        continuation,
                                        records_already_returned,
                                    },
                                );
                                None
                            }
                            Err(_) => {
                                self.step_once_with_event(
                                    RawRecordForwardScanStateMachineEvent::NextError {
                                        continuation,
                                    },
                                );
                                None
                            }
                        }
                    }
                    Err(cursor_error) => match cursor_error {
                        CursorError::FdbError(fdb_error, _) => {
                            self.step_once_with_event(
                                RawRecordForwardScanStateMachineEvent::FdbError {
                                    fdb_error,
                                    continuation,
                                },
                            );
                            None
                        }
                        CursorError::NoNextReason(no_next_reason) => match no_next_reason {
                            NoNextReason::SourceExhausted(_) => {
                                // We encountered the end of stream
                                // before reading the
                                // `RecordVersion`. So we can safely
                                // enter `EndOfStream` state.
                                self.step_once_with_event(
                                    RawRecordForwardScanStateMachineEvent::EndOfStream,
                                );
                                None
                            }
                            NoNextReason::ReturnLimitReached(_) => {
                                // We do not set in-band limit on the
                                // key value cursor. This is an
                                // unexpected state error.
                                let fdb_error = FdbError::new(RAW_RECORD_CURSOR_STATE_ERROR);
                                self.step_once_with_event(
                                    RawRecordForwardScanStateMachineEvent::FdbError {
                                        fdb_error,
                                        continuation,
                                    },
                                );
                                None
                            }
                            // Out of band errors
                            NoNextReason::TimeLimitReached(_) => {
                                let out_of_band_error_type =
                                    LimitManagerStoppedReason::TimeLimitReached;
                                self.step_once_with_event(
                                    RawRecordForwardScanStateMachineEvent::OutOfBandError {
                                        out_of_band_error_type,
                                        continuation,
                                    },
                                );
                                None
                            }
                            NoNextReason::ByteLimitReached(_) => {
                                let out_of_band_error_type =
                                    LimitManagerStoppedReason::ByteLimitReached;
                                self.step_once_with_event(
                                    RawRecordForwardScanStateMachineEvent::OutOfBandError {
                                        out_of_band_error_type,
                                        continuation,
                                    },
                                );
                                None
                            }
                            NoNextReason::KeyValueLimitReached(_) => {
                                let out_of_band_error_type =
                                    LimitManagerStoppedReason::KeyValueLimitReached;
                                self.step_once_with_event(
                                    RawRecordForwardScanStateMachineEvent::OutOfBandError {
                                        out_of_band_error_type,
                                        continuation,
                                    },
                                );
                                None
                            }
                        },
                    },
                }
            }
            RawRecordForwardScanStateMachineState::ReadRecordVersion => {
                // Extract and verify state data.
                //
                // Non-final state. We *must* call
                // `step_once_with_event`.
                let (
                    data_splits,
                    record_version,
                    primary_key,
                    continuation,
                    records_already_returned,
                ) = self
                    .state_machine_data
                    .take()
                    .and_then(|state_machine_data| {
                        if let RawRecordForwardScanStateMachineStateData::ReadRecordVersion {
                            data_splits,
                            record_version,
                            primary_key,
                            continuation,
                            records_already_returned,
                        } = state_machine_data
                        {
                            Some((
                                data_splits,
                                record_version,
                                primary_key,
                                continuation,
                                records_already_returned,
                            ))
                        } else {
                            None
                        }
                    })
                    .expect("invalid state_machine_data");

                let mut split_index = 0;

                let mut record_data_buf = BytesMut::new();

                // Extract a value of type
                // `CursorResult<FdbResult<Bytes>>`.
                //
                // The inner `Bytes` would be the unsplit record data.
                let next_record_data = loop {
                    let next_kv = key_value_cursor.next().await;

                    // Extract a value of type
                    // `CursorResult<FdbResult<Bytes>>`.
                    let res = next_kv.map(|cursor_success| {
                        cursor_success.map(|keyvalue| {
                            let (key, value) = keyvalue.into_parts();

                            // Extract a value of type `FdbResult<Bytes>`
                            // once you verify that the key is well
                            // formed.
                            Tuple::try_from(key)
                                .and_then(|mut tup| {
                                    // Verify that the index in the key
                                    // tuple matches `split_index`.
                                    let idx = tup.pop_back::<i8>().ok_or_else(|| {
                                        FdbError::new(RAW_RECORD_CURSOR_NEXT_ERROR)
                                    })?;

                                    if idx == split_index {
                                        Ok(tup)
                                    } else {
                                        Err(FdbError::new(RAW_RECORD_CURSOR_NEXT_ERROR))
                                    }
                                })
                                .and_then(|tup| {
                                    // We check if our primary key tuple
                                    // matches with the tuple that we are
                                    // seeing at the current
                                    // `split_index`.
                                    if primary_key.key == tup {
                                        Ok(())
                                    } else {
                                        Err(FdbError::new(RAW_RECORD_CURSOR_NEXT_ERROR))
                                    }
                                })
                                .and_then(|_| {
                                    // The key is well formed. We can
                                    // safely return the value and the
                                    // key value continuation.
                                    Ok(Bytes::from(value))
                                })
                        })
                    });

                    match res {
                        Ok(cursor_success) => {
                            let (fdb_result, kv_continuation) = cursor_success.into_parts();

                            match fdb_result {
                                Ok(bytes) => {
                                    // increment `split_index`
                                    split_index += 1;

                                    record_data_buf.put(bytes);

                                    if split_index == data_splits {
                                        break Ok(CursorSuccess::new(
                                            Ok(Bytes::from(record_data_buf)),
                                            kv_continuation,
                                        ));
                                    }

                                    // Continue iterating the loop.
                                }
                                Err(err) => {
                                    break Ok(CursorSuccess::new(Err(err), kv_continuation))
                                }
                            }
                        }
                        Err(cursor_error) => break Err(cursor_error),
                    }
                };

                match next_record_data {
                    Ok(cursor_success) => {
                        // `cursor_success` is a value of type
                        // `CursorResult<FdbResult<Bytes>>`. If we
                        // have a inner `Err` value, then we assume it
                        // to be a next error.
                        let (res, kv_continuation) = cursor_success.into_parts();
                        match res {
                            Ok(record_bytes) => {
                                let kv_continuation: Arc<dyn Any + Send + Sync + 'static> =
                                    kv_continuation;

                                // Downcasting should not fail. But if
                                // does, send `NextError` event.
                                match kv_continuation.downcast::<KeyValueContinuationInternal>() {
                                    Ok(arc_kv_continuation_internal) => {
                                        let kv_continuation_internal =
                                            Arc::unwrap_or_clone(arc_kv_continuation_internal);
                                        let KeyValueContinuationInternal::V1(
                                            pb_keyvalue_continuation_internal_v1,
                                        ) = kv_continuation_internal;

                                        // This is our new
                                        // continuation based on
                                        // `kv_continuation.
                                        let continuation = RawRecordContinuationInternal::V1(
                                            pb::RawRecordContinuationInternalV1::from(
                                                pb::RawRecordContinuationInternalV1 {
                                                    inner: pb_keyvalue_continuation_internal_v1,
                                                },
                                            ),
                                        );

                                        let raw_record = RawRecord::from((
                                            primary_key,
                                            record_version,
                                            record_bytes,
                                        ));

                                        self.step_once_with_event(
                                            RawRecordForwardScanStateMachineEvent::Available {
                                                raw_record,
                                                continuation,
                                                records_already_returned,
                                            },
                                        );
                                        None
                                    }
                                    Err(_) => {
                                        self.step_once_with_event(
                                            RawRecordForwardScanStateMachineEvent::NextError {
                                                continuation,
                                            },
                                        );
                                        None
                                    }
                                }
                            }
                            Err(_) => {
                                self.step_once_with_event(
                                    RawRecordForwardScanStateMachineEvent::NextError {
                                        continuation,
                                    },
                                );
                                None
                            }
                        }
                    }
                    Err(cursor_error) => match cursor_error {
                        CursorError::FdbError(fdb_error, _) => {
                            self.step_once_with_event(
                                RawRecordForwardScanStateMachineEvent::FdbError {
                                    fdb_error,
                                    continuation,
                                },
                            );
                            None
                        }
                        CursorError::NoNextReason(no_next_reason) => match no_next_reason {
                            NoNextReason::SourceExhausted(_) => {
                                // We are not suppose to get a
                                // `SourceExhausted` error. This is
                                // because even an empty record value
                                // will contain atleast one data
                                // split.
                                self.step_once_with_event(
                                    RawRecordForwardScanStateMachineEvent::NextError {
                                        continuation,
                                    },
                                );
                                None
                            }
                            NoNextReason::ReturnLimitReached(_) => {
                                // We do not set in-band limit on the
                                // key value cursor. This is an
                                // unexpected state error.
                                let fdb_error = FdbError::new(RAW_RECORD_CURSOR_STATE_ERROR);
                                self.step_once_with_event(
                                    RawRecordForwardScanStateMachineEvent::FdbError {
                                        fdb_error,
                                        continuation,
                                    },
                                );
                                None
                            }
                            // Out of band errors
                            NoNextReason::TimeLimitReached(_) => {
                                let out_of_band_error_type =
                                    LimitManagerStoppedReason::TimeLimitReached;
                                self.step_once_with_event(
                                    RawRecordForwardScanStateMachineEvent::OutOfBandError {
                                        out_of_band_error_type,
                                        continuation,
                                    },
                                );
                                None
                            }
                            NoNextReason::ByteLimitReached(_) => {
                                let out_of_band_error_type =
                                    LimitManagerStoppedReason::ByteLimitReached;
                                self.step_once_with_event(
                                    RawRecordForwardScanStateMachineEvent::OutOfBandError {
                                        out_of_band_error_type,
                                        continuation,
                                    },
                                );
                                None
                            }
                            NoNextReason::KeyValueLimitReached(_) => {
                                let out_of_band_error_type =
                                    LimitManagerStoppedReason::KeyValueLimitReached;
                                self.step_once_with_event(
                                    RawRecordForwardScanStateMachineEvent::OutOfBandError {
                                        out_of_band_error_type,
                                        continuation,
                                    },
                                );
                                None
                            }
                        },
                    },
                }
            }
            RawRecordForwardScanStateMachineState::RawRecordAvailable => todo!(),
            RawRecordForwardScanStateMachineState::RawRecordNextError => todo!(),
            RawRecordForwardScanStateMachineState::RawRecordLimitReached => todo!(),
            RawRecordForwardScanStateMachineState::RawRecordEndOfStream => todo!(),
            RawRecordForwardScanStateMachineState::OutOfBandError => todo!(),
            RawRecordForwardScanStateMachineState::FdbError => todo!(),
        };
        todo!();
    }

    // TODO: This can be easily unit tested.
    fn step_once_with_event(&mut self, event: RawRecordForwardScanStateMachineEvent) {
        self.state_machine_state = match self.state_machine_state {
            RawRecordForwardScanStateMachineState::InitiateRecordVersionRead => match event {
                RawRecordForwardScanStateMachineEvent::RecordVersionOk {
                    data_splits,
                    record_version,
                    primary_key,
                    continuation,
                    records_already_returned,
                } => {
                    self.state_machine_data = Some(
                        RawRecordForwardScanStateMachineStateData::ReadRecordVersion {
                            data_splits,
                            record_version,
                            primary_key,
                            continuation,
                            records_already_returned,
                        },
                    );
                    RawRecordForwardScanStateMachineState::ReadRecordVersion
                }
                RawRecordForwardScanStateMachineEvent::NextError { continuation } => {
                    self.state_machine_data = Some(
                        RawRecordForwardScanStateMachineStateData::RawRecordNextError {
                            continuation,
                        },
                    );
                    RawRecordForwardScanStateMachineState::RawRecordNextError
                }
                RawRecordForwardScanStateMachineEvent::EndOfStream => {
                    self.state_machine_data =
                        Some(RawRecordForwardScanStateMachineStateData::RawRecordEndOfStream);
                    RawRecordForwardScanStateMachineState::RawRecordEndOfStream
                }
                RawRecordForwardScanStateMachineEvent::OutOfBandError {
                    out_of_band_error_type,
                    continuation,
                } => {
                    self.state_machine_data =
                        Some(RawRecordForwardScanStateMachineStateData::OutOfBandError {
                            out_of_band_error_type,
                            continuation,
                        });
                    RawRecordForwardScanStateMachineState::OutOfBandError
                }
                RawRecordForwardScanStateMachineEvent::FdbError {
                    fdb_error,
                    continuation,
                } => {
                    self.state_machine_data =
                        Some(RawRecordForwardScanStateMachineStateData::FdbError {
                            fdb_error,
                            continuation,
                        });
                    RawRecordForwardScanStateMachineState::FdbError
                }
                _ => panic!("Invalid event!"),
            },
            RawRecordForwardScanStateMachineState::ReadRecordVersion => match event {
                RawRecordForwardScanStateMachineEvent::Available {
                    raw_record,
                    continuation,
                    records_already_returned,
                } => {
                    self.state_machine_data = Some(
                        RawRecordForwardScanStateMachineStateData::RawRecordAvailable {
                            raw_record,
                            continuation,
                            records_already_returned,
                        },
                    );
                    RawRecordForwardScanStateMachineState::RawRecordAvailable
                }
                RawRecordForwardScanStateMachineEvent::NextError { continuation } => {
                    self.state_machine_data = Some(
                        RawRecordForwardScanStateMachineStateData::RawRecordNextError {
                            continuation,
                        },
                    );
                    RawRecordForwardScanStateMachineState::RawRecordNextError
                }
                RawRecordForwardScanStateMachineEvent::OutOfBandError {
                    out_of_band_error_type,
                    continuation,
                } => {
                    self.state_machine_data =
                        Some(RawRecordForwardScanStateMachineStateData::OutOfBandError {
                            out_of_band_error_type,
                            continuation,
                        });
                    RawRecordForwardScanStateMachineState::OutOfBandError
                }
                RawRecordForwardScanStateMachineEvent::FdbError {
                    fdb_error,
                    continuation,
                } => {
                    self.state_machine_data =
                        Some(RawRecordForwardScanStateMachineStateData::FdbError {
                            fdb_error,
                            continuation,
                        });
                    RawRecordForwardScanStateMachineState::FdbError
                }
                _ => panic!("Invalid event!"),
            },
            RawRecordForwardScanStateMachineState::RawRecordAvailable => todo!(),
            RawRecordForwardScanStateMachineState::RawRecordNextError
            | RawRecordForwardScanStateMachineState::RawRecordLimitReached
            | RawRecordForwardScanStateMachineState::RawRecordEndOfStream
            | RawRecordForwardScanStateMachineState::OutOfBandError
            | RawRecordForwardScanStateMachineState::FdbError => {
                // Final states. No event should be received.
                panic!("Invalid event!");
            }
        };
    }
}

#[derive(Debug)]
enum RawRecordReverseScanStateMachineState {
    InitiateLastSplitRead,
    ReadLastSplit,
    RawRecordAvailable,
    RawRecordNextError,
    RawRecordLimitReached,
    // When the underlying key value cursor ends in a consistent
    // state, the cursor would enter `RawRecordEndOfStream`
    // state. Otherwise we would enter `RawRecordNextError` state.
    RawRecordEndOfStream,
    OutOfBandError,
    FdbError,
}

#[derive(Debug)]
enum RawRecordReverseScanStateMachineStateData {
    InitiateLastSplitRead {
        continuation: RawRecordContinuationInternal,
    },
    ReadLastSplit {
        data_splits: i8,
        primary_key: RawRecordPrimaryKey,
        last_split_value: Value,
        continuation: RawRecordContinuationInternal,
        records_already_returned: usize,
    },
    RawRecordAvailable {
        raw_record: RawRecord,
        continuation: RawRecordContinuationInternal,
        records_already_returned: usize,
    },
    RawRecordNextError {
        continuation: RawRecordContinuationInternal,
    },
    RawRecordLimitReached {
        continuation: RawRecordContinuationInternal,
    },
    RawRecordEndOfStream,
    OutOfBandError {
        out_of_band_error_type: LimitManagerStoppedReason,
        continuation: RawRecordContinuationInternal,
    },
    FdbError {
        fdb_error: FdbError,
        continuation: RawRecordContinuationInternal,
    },
}

#[derive(Debug)]
enum RawRecordReverseScanStateMachineEvent {
    LastSplitOk {
        primary_key: RawRecordPrimaryKey,
        data_splits: i8,
        last_split_value: Value,
        continuation: RawRecordContinuationInternal,
        records_already_returned: usize,
    },
    Available {
        raw_record: RawRecord,
        continuation: RawRecordContinuationInternal,
        records_already_returned: usize,
    },
    NextLastSplitOk {
        record_btree: BTreeMap<i8, Value>,
        primary_key: RawRecordPrimaryKey,
        continuation: RawRecordContinuationInternal,
        records_already_returned: usize,
    },
    NextError {
        continuation: RawRecordContinuationInternal,
    },
    LimitReached {
        continuation: RawRecordContinuationInternal,
    },
    EndOfStream,
    OutOfBandError {
        out_of_band_error_type: LimitManagerStoppedReason,
        continuation: RawRecordContinuationInternal,
    },
    FdbError {
        fdb_error: FdbError,
        continuation: RawRecordContinuationInternal,
    },
}

#[derive(Debug)]
/// A state machine that implements reverse scan and returns values of
/// of type [`RawRecord`].
///
/// See `sismic/...TODO` for the design of the state machine.
struct RawRecordReverseScanStateMachine {
    state_machine_state: RawRecordReverseScanStateMachineState,
    // We use `Option` here so that we can take ownership of the data
    // and pass it as part of the event. This would avoid unnecessary
    // cloning.
    //
    // This value is taken in `next` method and assigned `Some(...)`
    // value in `step_once_with_event` method. In final states, we do
    // not take the value, so there is no need to assign it back.
    state_machine_data: Option<RawRecordReverseScanStateMachineStateData>,
}

impl RawRecordReverseScanStateMachine {
    /// If needed, perform the action (side effect) and state
    /// transition. Return an `Option` value or `None` in case we need
    /// to further drive the loop.
    async fn next(
        &mut self,
        key_value_cursor: &mut KeyValueCursor,
        primary_key_schema: &RawRecordPrimaryKeySchema,
    ) -> Option<CursorResult<RawRecord>> {
        let _: Option<CursorResult<RawRecord>> = match self.state_machine_state {
            RawRecordReverseScanStateMachineState::InitiateLastSplitRead => {
                // Extract and verify state data.
                //
                // Non-final state. We *must* call
                // `step_once_with_event`.
                let continuation = self
                    .state_machine_data
                    .take()
                    .and_then(|state_machine_data| {
                        if let RawRecordReverseScanStateMachineStateData::InitiateLastSplitRead {
                            continuation,
                        } = state_machine_data
                        {
                            Some(continuation)
                        } else {
                            None
                        }
                    })
                    .expect("invalid state_machine_data");

                let next_kv = key_value_cursor.next().await;

                match next_kv {
                    Ok(cursor_success) => {
                        let (key, value) = cursor_success.into_value().into_parts();

                        // Extract a value of type
                        // `FdbResult<(RawRecordPrimaryKey, i8,
                        // Value)>, which will give us the information
                        // that we need to make the correct
                        // transition.
                        let res = Tuple::try_from(key)
                            .and_then(|mut tup| {
                                // Verify that the split index is
                                // `>=0`.
                                let idx = tup
                                    .pop_back::<i8>()
                                    .ok_or_else(|| FdbError::new(RAW_RECORD_CURSOR_NEXT_ERROR))?;
                                if idx >= 0 {
                                    // `data_splits` is last index plus one.
                                    let data_splits = idx + 1;
                                    Ok((tup, data_splits))
                                } else {
                                    Err(FdbError::new(RAW_RECORD_CURSOR_NEXT_ERROR))
                                }
                            })
                            .and_then(|(tup, data_splits)| {
                                // Verify that tuple matches the
                                // primary key schema.
                                let primary_key = RawRecordPrimaryKey::try_from((
                                    primary_key_schema.clone(),
                                    tup,
                                ))?;

                                let last_data_split_value = value;

                                Ok((primary_key, data_splits, last_data_split_value))
                            });

                        match res {
                            Ok((primary_key, data_splits, last_split_value)) => {
                                // We only have last record's last
                                // data split, number of data splits
                                // in the last record and the primary
                                // key. So, we set this value to `0`.
                                let records_already_returned = 0;

                                self.step_once_with_event(
                                    RawRecordReverseScanStateMachineEvent::LastSplitOk {
                                        primary_key,
                                        data_splits,
                                        last_split_value,
                                        continuation,
                                        records_already_returned,
                                    },
                                );
                                None
                            }
                            Err(_) => {
                                self.step_once_with_event(
                                    RawRecordReverseScanStateMachineEvent::NextError {
                                        continuation,
                                    },
                                );
                                None
                            }
                        }
                    }
                    Err(cursor_error) => match cursor_error {
                        CursorError::FdbError(fdb_error, _) => {
                            self.step_once_with_event(
                                RawRecordReverseScanStateMachineEvent::FdbError {
                                    fdb_error,
                                    continuation,
                                },
                            );
                            None
                        }
                        CursorError::NoNextReason(no_next_reason) => match no_next_reason {
                            NoNextReason::SourceExhausted(_) => {
                                // We encountered the end of stream
                                // before reading the last split. So
                                // we can safely enter `EndOfStream`
                                // state.
                                self.step_once_with_event(
                                    RawRecordReverseScanStateMachineEvent::EndOfStream,
                                );
                                None
                            }
                            NoNextReason::ReturnLimitReached(_) => {
                                // We do not set in-band limit on the
                                // key value cursor. This is an
                                // unexpected state error.
                                let fdb_error = FdbError::new(RAW_RECORD_CURSOR_STATE_ERROR);
                                self.step_once_with_event(
                                    RawRecordReverseScanStateMachineEvent::FdbError {
                                        fdb_error,
                                        continuation,
                                    },
                                );
                                None
                            }
                            // Out of band errors
                            NoNextReason::TimeLimitReached(_) => {
                                let out_of_band_error_type =
                                    LimitManagerStoppedReason::TimeLimitReached;
                                self.step_once_with_event(
                                    RawRecordReverseScanStateMachineEvent::OutOfBandError {
                                        out_of_band_error_type,
                                        continuation,
                                    },
                                );
                                None
                            }
                            NoNextReason::ByteLimitReached(_) => {
                                let out_of_band_error_type =
                                    LimitManagerStoppedReason::ByteLimitReached;
                                self.step_once_with_event(
                                    RawRecordReverseScanStateMachineEvent::OutOfBandError {
                                        out_of_band_error_type,
                                        continuation,
                                    },
                                );
                                None
                            }
                            NoNextReason::KeyValueLimitReached(_) => {
                                let out_of_band_error_type =
                                    LimitManagerStoppedReason::KeyValueLimitReached;
                                self.step_once_with_event(
                                    RawRecordReverseScanStateMachineEvent::OutOfBandError {
                                        out_of_band_error_type,
                                        continuation,
                                    },
                                );
                                None
                            }
                        },
                    },
                }
            }
            RawRecordReverseScanStateMachineState::ReadLastSplit => todo!(),
            RawRecordReverseScanStateMachineState::RawRecordAvailable => todo!(),
            RawRecordReverseScanStateMachineState::RawRecordNextError => todo!(),
            RawRecordReverseScanStateMachineState::RawRecordLimitReached => todo!(),
            RawRecordReverseScanStateMachineState::RawRecordEndOfStream => todo!(),
            RawRecordReverseScanStateMachineState::OutOfBandError => todo!(),
            RawRecordReverseScanStateMachineState::FdbError => todo!(),
        };
        todo!();
    }

    // TODO: This can be easily unit tested.
    fn step_once_with_event(&mut self, event: RawRecordReverseScanStateMachineEvent) {
        self.state_machine_state = match self.state_machine_state {
            RawRecordReverseScanStateMachineState::InitiateLastSplitRead => match event {
                RawRecordReverseScanStateMachineEvent::LastSplitOk {
                    primary_key,
                    data_splits,
                    last_split_value,
                    continuation,
                    records_already_returned,
                } => {
                    self.state_machine_data =
                        Some(RawRecordReverseScanStateMachineStateData::ReadLastSplit {
                            primary_key,
                            data_splits,
                            last_split_value,
                            continuation,
                            records_already_returned,
                        });
                    RawRecordReverseScanStateMachineState::ReadLastSplit
                }
                RawRecordReverseScanStateMachineEvent::NextError { continuation } => {
                    self.state_machine_data = Some(
                        RawRecordReverseScanStateMachineStateData::RawRecordNextError {
                            continuation,
                        },
                    );
                    RawRecordReverseScanStateMachineState::RawRecordNextError
                }
                RawRecordReverseScanStateMachineEvent::EndOfStream => {
                    self.state_machine_data =
                        Some(RawRecordReverseScanStateMachineStateData::RawRecordEndOfStream);
                    RawRecordReverseScanStateMachineState::RawRecordEndOfStream
                }
                RawRecordReverseScanStateMachineEvent::OutOfBandError {
                    out_of_band_error_type,
                    continuation,
                } => {
                    self.state_machine_data =
                        Some(RawRecordReverseScanStateMachineStateData::OutOfBandError {
                            out_of_band_error_type,
                            continuation,
                        });
                    RawRecordReverseScanStateMachineState::OutOfBandError
                }
                RawRecordReverseScanStateMachineEvent::FdbError {
                    fdb_error,
                    continuation,
                } => {
                    self.state_machine_data =
                        Some(RawRecordReverseScanStateMachineStateData::FdbError {
                            fdb_error,
                            continuation,
                        });
                    RawRecordReverseScanStateMachineState::FdbError
                }
                _ => panic!("Invalid event!"),
            },
            RawRecordReverseScanStateMachineState::ReadLastSplit => {
		// TODO: Continue from here.
		todo!();
	    }
            RawRecordReverseScanStateMachineState::RawRecordAvailable => todo!(),
            RawRecordReverseScanStateMachineState::RawRecordNextError
            | RawRecordReverseScanStateMachineState::RawRecordLimitReached
            | RawRecordReverseScanStateMachineState::RawRecordEndOfStream
            | RawRecordReverseScanStateMachineState::OutOfBandError
            | RawRecordReverseScanStateMachineState::FdbError => {
                // Final states. No event should be received.
                panic!("Invalid event!");
            }
        };
    }
}

#[derive(Debug)]
enum RawRecordStateMachine {
    ForwardScan(RawRecordForwardScanStateMachine),
    ReverseScan(RawRecordReverseScanStateMachine),
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
                        .next(&mut self.key_value_cursor, &self.primary_key_schema)
                        .await
                    {
                        return res;
                    }
                }
                RawRecordStateMachine::ReverseScan(ref mut reverse_scan_state_machine) => {
                    if let Some(res) = reverse_scan_state_machine
                        .next(&mut self.key_value_cursor, &self.primary_key_schema)
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
    mod rawrecord_continuation_internal {
        // TODO
    }

    mod raw_record_primary_key_schema {
        use fdb::error::FdbError;
        use fdb::tuple::{TupleSchema, TupleSchemaElement};

        use std::convert::TryFrom;

        use crate::error::RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA;

        use super::super::RawRecordPrimaryKeySchema;

        #[test]
        fn try_from_tuple_schema_try_from() {
            // Valid schema
            {
                let mut ts = TupleSchema::new();
                ts.push_back(TupleSchemaElement::Bytes);
                ts.push_back(TupleSchemaElement::String);
                ts.push_back(TupleSchemaElement::Integer);
                ts.push_back(TupleSchemaElement::Float);
                ts.push_back(TupleSchemaElement::Double);
                ts.push_back(TupleSchemaElement::Uuid);
                ts.push_back(TupleSchemaElement::Tuple({
                    let mut ts_inner = TupleSchema::new();
                    ts_inner.push_back(TupleSchemaElement::String);
                    ts_inner
                }));

                assert_eq!(
                    RawRecordPrimaryKeySchema::try_from(ts.clone()),
                    Ok(RawRecordPrimaryKeySchema { inner: ts })
                );
            }

            // Empty schema is invalid
            {
                let ts = TupleSchema::new();

                assert_eq!(
                    RawRecordPrimaryKeySchema::try_from(ts),
                    Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                );
            }

            // Invalid schema elements
            {
                {
                    let mut ts = TupleSchema::new();
                    ts.push_back(TupleSchemaElement::Null);

                    assert_eq!(
                        RawRecordPrimaryKeySchema::try_from(ts),
                        Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                    );
                }
                {
                    let mut ts = TupleSchema::new();
                    ts.push_back(TupleSchemaElement::Versionstamp);

                    assert_eq!(
                        RawRecordPrimaryKeySchema::try_from(ts),
                        Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                    );
                }
                {
                    let mut ts = TupleSchema::new();
                    ts.push_back(TupleSchemaElement::MaybeString);

                    assert_eq!(
                        RawRecordPrimaryKeySchema::try_from(ts),
                        Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                    );
                }
                // Valid nested tuple, but within a `MaybeTuple`.
                {
                    let mut ts = TupleSchema::new();
                    ts.push_back(TupleSchemaElement::MaybeTuple({
                        let mut ts_inner = TupleSchema::new();
                        ts_inner.push_back(TupleSchemaElement::String);
                        ts_inner
                    }));

                    assert_eq!(
                        RawRecordPrimaryKeySchema::try_from(ts),
                        Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                    );
                }
                // Empty tuple, in an otherwise valid schema.
                // Valid nested tuple, but within a `MaybeTuple`.
                {
                    let mut ts = TupleSchema::new();
                    ts.push_back(TupleSchemaElement::Bytes);
                    ts.push_back(TupleSchemaElement::Tuple({
                        let ts_inner = TupleSchema::new();
                        ts_inner
                    }));

                    assert_eq!(
                        RawRecordPrimaryKeySchema::try_from(ts),
                        Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                    );
                }
                {
                    let mut ts = TupleSchema::new();
                    ts.push_back(TupleSchemaElement::MaybeInteger);

                    assert_eq!(
                        RawRecordPrimaryKeySchema::try_from(ts),
                        Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                    );
                }
                {
                    let mut ts = TupleSchema::new();
                    ts.push_back(TupleSchemaElement::MaybeFloat);

                    assert_eq!(
                        RawRecordPrimaryKeySchema::try_from(ts),
                        Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                    );
                }
                {
                    let mut ts = TupleSchema::new();
                    ts.push_back(TupleSchemaElement::MaybeDouble);

                    assert_eq!(
                        RawRecordPrimaryKeySchema::try_from(ts),
                        Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                    );
                }
                {
                    let mut ts = TupleSchema::new();
                    ts.push_back(TupleSchemaElement::MaybeBoolean);

                    assert_eq!(
                        RawRecordPrimaryKeySchema::try_from(ts),
                        Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                    );
                }
                {
                    let mut ts = TupleSchema::new();
                    ts.push_back(TupleSchemaElement::MaybeUuid);

                    assert_eq!(
                        RawRecordPrimaryKeySchema::try_from(ts),
                        Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                    );
                }
                {
                    let mut ts = TupleSchema::new();
                    ts.push_back(TupleSchemaElement::MaybeVersionstamp);

                    assert_eq!(
                        RawRecordPrimaryKeySchema::try_from(ts),
                        Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                    );
                }
            }
        }
    }

    mod raw_record_primary_key {
        use bytes::Bytes;

        use fdb::error::FdbError;
        use fdb::tuple::{Tuple, TupleSchema, TupleSchemaElement};

        use std::convert::TryFrom;

        use crate::error::RAW_RECORD_PRIMARY_KEY_TUPLE_SCHEMA_MISMATCH;

        use super::super::{RawRecordPrimaryKey, RawRecordPrimaryKeySchema};

        #[test]
        fn try_from_raw_record_primary_key_schema_tuple_try_from() {
            // `Tuple` matches `RawRecordPrimaryKeySchema`
            {
                let mut ts = TupleSchema::new();
                ts.push_back(TupleSchemaElement::Bytes);
                ts.push_back(TupleSchemaElement::String);
                ts.push_back(TupleSchemaElement::Integer);

                let schema = RawRecordPrimaryKeySchema::try_from(ts).unwrap();

                let mut key = Tuple::new();
                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                key.push_back::<String>("world".to_string());
                key.push_back::<i8>(0);

                assert_eq!(
                    RawRecordPrimaryKey::try_from((schema.clone(), key.clone())),
                    Ok(RawRecordPrimaryKey { schema, key })
                );
            }

            // `Tuple` does not match `RawRecordPrimaryKeySchema`
            {
                let mut ts = TupleSchema::new();
                ts.push_back(TupleSchemaElement::Bytes);
                ts.push_back(TupleSchemaElement::String);
                ts.push_back(TupleSchemaElement::Integer);

                let schema = RawRecordPrimaryKeySchema::try_from(ts).unwrap();

                let mut key = Tuple::new();
                key.push_back::<i8>(0);
                key.push_back::<String>("world".to_string());
                key.push_back::<Bytes>(Bytes::from_static(b"hello"));

                assert_eq!(
                    RawRecordPrimaryKey::try_from((schema, key)),
                    Err(FdbError::new(RAW_RECORD_PRIMARY_KEY_TUPLE_SCHEMA_MISMATCH))
                );
            }
        }
    }
}
