//! Provides types related to forward and reverse scan state machine.
use bytes::{BufMut, Bytes, BytesMut};

use fdb::error::FdbError;
use fdb::tuple::Tuple;
use fdb::Value;

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use std::convert::TryFrom;

use crate::cursor::{
    Cursor, CursorError, CursorResult, CursorSuccess, KeyValueContinuationInternal, KeyValueCursor,
    LimitManagerStoppedReason, NoNextReason,
};
use crate::error::{RAW_RECORD_CURSOR_NEXT_ERROR, RAW_RECORD_CURSOR_STATE_ERROR};
use crate::split_helper::RecordHeaderV0;
use crate::RecordVersion;

use super::{
    RawRecord, RawRecordContinuationInternal, RawRecordPrimaryKey, RawRecordPrimaryKeySchema,
};

/// Forward scan state machine state.
#[derive(Debug, PartialEq)]
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

/// Forward scan state machine state data.
#[derive(Debug, PartialEq)]
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

/// Forward scan state machine event.
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
/// See `sismic/raw_record_forward_scan.yaml` for the design of the
/// state machine.
pub(super) struct RawRecordForwardScanStateMachine {
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
    pub(super) async fn next(
        &mut self,
        key_value_cursor: &mut KeyValueCursor,
        primary_key_schema: &RawRecordPrimaryKeySchema,
        values_limit: usize,
    ) -> Option<CursorResult<RawRecord>> {
        match self.state_machine_state {
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

                // When `next_split_index == data_splits` then we
                // would have read the entire record data.
                let mut next_split_index = 0;

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

                                    if idx == next_split_index {
                                        Ok(tup)
                                    } else {
                                        Err(FdbError::new(RAW_RECORD_CURSOR_NEXT_ERROR))
                                    }
                                })
                                .and_then(|tup| {
                                    // We check if our primary key
                                    // tuple matches with the tuple
                                    // that we are seeing at the
                                    // current `next_split_index`.
                                    if primary_key.key_ref() == &tup {
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
                                    // Increment `next_split_index`
                                    next_split_index += 1;

                                    record_data_buf.put(bytes);

                                    if next_split_index == data_splits {
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
                                        let continuation = RawRecordContinuationInternal::from(
                                            pb_keyvalue_continuation_internal_v1,
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
            RawRecordForwardScanStateMachineState::RawRecordAvailable => {
                // Extract and verify state data.
                //
                // Non-final state. We *must* call
                // `step_once_with_event`.
                //
                // In addition, we will be returning a `Some(...)` (a
                // value of `RawRecord`) in this state.
                let (raw_record, continuation, mut records_already_returned) = self
                    .state_machine_data
                    .take()
                    .and_then(|state_machine_data| {
                        if let RawRecordForwardScanStateMachineStateData::RawRecordAvailable {
                            raw_record,
                            continuation,
                            records_already_returned,
                        } = state_machine_data
                        {
                            Some((raw_record, continuation, records_already_returned))
                        } else {
                            None
                        }
                    })
                    .expect("invalid state_machine_data");

                if records_already_returned + 1 == values_limit {
                    // First check if we are going to hit the
                    // `values_limit`.
                    //
                    // This also ensures that in case we encounter a
                    // situation of `LimitReached` and
                    // `SourceExhausted` happening at the same time,
                    // we will return `LimitReached`.
                    self.step_once_with_event(
                        RawRecordForwardScanStateMachineEvent::LimitReached {
                            continuation: continuation.clone(),
                        },
                    );
                } else {
                    // We still need to return more raw records. Attempt
                    // to read the next raw record's record version.

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
                                    let idx = tup.pop_back::<i8>().ok_or_else(|| {
                                        FdbError::new(RAW_RECORD_CURSOR_NEXT_ERROR)
                                    })?;
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
                                    // We will be returning a raw
                                    // record below.
                                    records_already_returned += 1;

                                    self.step_once_with_event(
                                        RawRecordForwardScanStateMachineEvent::RecordVersionOk {
                                            data_splits,
                                            record_version,
                                            primary_key,
                                            continuation: continuation.clone(),
                                            records_already_returned,
                                        },
                                    );
                                }
                                Err(_) => {
                                    self.step_once_with_event(
                                        RawRecordForwardScanStateMachineEvent::NextError {
                                            continuation: continuation.clone(),
                                        },
                                    );
                                }
                            }
                        }
                        Err(cursor_error) => match cursor_error {
                            CursorError::FdbError(fdb_error, _) => {
                                self.step_once_with_event(
                                    RawRecordForwardScanStateMachineEvent::FdbError {
                                        fdb_error,
                                        continuation: continuation.clone(),
                                    },
                                );
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
                                }
                                NoNextReason::ReturnLimitReached(_) => {
                                    // We do not set in-band limit on the
                                    // key value cursor. This is an
                                    // unexpected state error.
                                    let fdb_error = FdbError::new(RAW_RECORD_CURSOR_STATE_ERROR);
                                    self.step_once_with_event(
                                        RawRecordForwardScanStateMachineEvent::FdbError {
                                            fdb_error,
                                            continuation: continuation.clone(),
                                        },
                                    );
                                }
                                // Out of band errors
                                NoNextReason::TimeLimitReached(_) => {
                                    let out_of_band_error_type =
                                        LimitManagerStoppedReason::TimeLimitReached;
                                    self.step_once_with_event(
                                        RawRecordForwardScanStateMachineEvent::OutOfBandError {
                                            out_of_band_error_type,
                                            continuation: continuation.clone(),
                                        },
                                    );
                                }
                                NoNextReason::ByteLimitReached(_) => {
                                    let out_of_band_error_type =
                                        LimitManagerStoppedReason::ByteLimitReached;
                                    self.step_once_with_event(
                                        RawRecordForwardScanStateMachineEvent::OutOfBandError {
                                            out_of_band_error_type,
                                            continuation: continuation.clone(),
                                        },
                                    );
                                }
                                NoNextReason::KeyValueLimitReached(_) => {
                                    let out_of_band_error_type =
                                        LimitManagerStoppedReason::KeyValueLimitReached;
                                    self.step_once_with_event(
                                        RawRecordForwardScanStateMachineEvent::OutOfBandError {
                                            out_of_band_error_type,
                                            continuation: continuation.clone(),
                                        },
                                    );
                                }
                            },
                        },
                    }
                }

                let cursor_result_continuation = Arc::new(continuation);

                Some(Ok(CursorSuccess::new(
                    raw_record,
                    cursor_result_continuation,
                )))
            }
            RawRecordForwardScanStateMachineState::RawRecordNextError => {
                // Final state. We do not `take` state data from
                // `state_machine_data`.
                //
                // We also do not call `step_once_with_event`.
                //
                // We will be returning a `Some(...)` (a value of
                // `CursorError`) in this state.
                let continuation = self
                    .state_machine_data
                    .as_ref()
                    .and_then(|state_machine_data| {
                        if let RawRecordForwardScanStateMachineStateData::RawRecordNextError {
                            continuation,
                        } = state_machine_data
                        {
                            Some(continuation.clone())
                        } else {
                            None
                        }
                    })
                    .expect("invalid state_machine_data");

                let cursor_result_continuation = Arc::new(continuation);
                let fdb_error = FdbError::new(RAW_RECORD_CURSOR_NEXT_ERROR);

                Some(Err(CursorError::FdbError(
                    fdb_error,
                    cursor_result_continuation,
                )))
            }
            RawRecordForwardScanStateMachineState::RawRecordLimitReached => {
                // Final state. We do not `take` state data from
                // `state_machine_data`.
                //
                // We also do not call `step_once_with_event`.
                //
                // We will be returning a `Some(...)` (a value of
                // `CursorError`) in this state.
                let continuation = self
                    .state_machine_data
                    .as_ref()
                    .and_then(|state_machine_data| {
                        if let RawRecordForwardScanStateMachineStateData::RawRecordLimitReached {
                            continuation,
                        } = state_machine_data
                        {
                            Some(continuation.clone())
                        } else {
                            None
                        }
                    })
                    .expect("invalid state_machine_data");

                let cursor_result_continuation = Arc::new(continuation);

                Some(Err(CursorError::NoNextReason(
                    NoNextReason::ReturnLimitReached(cursor_result_continuation),
                )))
            }
            RawRecordForwardScanStateMachineState::RawRecordEndOfStream => {
                // Final state. We do not `take` state data from
                // `state_machine_data`.
                //
                // We also do not call `step_once_with_event`.
                //
                // We will be returning a `Some(...)` (a value of
                // `CursorError`) in this state.
                let continuation = {
                    let KeyValueContinuationInternal::V1(pb_keyvalue_continuation_internal_v1) =
                        KeyValueContinuationInternal::new_v1_end_marker();

                    RawRecordContinuationInternal::from(pb_keyvalue_continuation_internal_v1)
                };

                let cursor_result_continuation = Arc::new(continuation);

                Some(Err(CursorError::NoNextReason(
                    NoNextReason::SourceExhausted(cursor_result_continuation),
                )))
            }
            RawRecordForwardScanStateMachineState::OutOfBandError => {
                // Final state. We do not `take` state data from
                // `state_machine_data`.
                //
                // We also do not call `step_once_with_event`.
                //
                // We will be returning a `Some(...)` (a value of
                // `CursorError`) in this state.
                let (out_of_band_error_type, continuation) = self
                    .state_machine_data
                    .as_ref()
                    .and_then(|state_machine_data| {
                        if let RawRecordForwardScanStateMachineStateData::OutOfBandError {
                            out_of_band_error_type,
                            continuation,
                        } = state_machine_data
                        {
                            Some((out_of_band_error_type.clone(), continuation.clone()))
                        } else {
                            None
                        }
                    })
                    .expect("invalid state_machine_data");

                let cursor_result_continuation = Arc::new(continuation);

                let no_next_reason = match out_of_band_error_type {
                    LimitManagerStoppedReason::TimeLimitReached => {
                        NoNextReason::TimeLimitReached(cursor_result_continuation)
                    }
                    LimitManagerStoppedReason::ByteLimitReached => {
                        NoNextReason::ByteLimitReached(cursor_result_continuation)
                    }
                    LimitManagerStoppedReason::KeyValueLimitReached => {
                        NoNextReason::KeyValueLimitReached(cursor_result_continuation)
                    }
                };

                Some(Err(CursorError::NoNextReason(no_next_reason)))
            }
            RawRecordForwardScanStateMachineState::FdbError => {
                // Final state. We do not `take` state data from
                // `state_machine_data`.
                //
                // We also do not call `step_once_with_event`.
                //
                // We will be returning a `Some(...)` (a value of
                // `CursorError`) in this state.
                let (fdb_error, continuation) = self
                    .state_machine_data
                    .as_ref()
                    .and_then(|state_machine_data| {
                        if let RawRecordForwardScanStateMachineStateData::FdbError {
                            fdb_error,
                            continuation,
                        } = state_machine_data
                        {
                            Some((fdb_error.clone(), continuation.clone()))
                        } else {
                            None
                        }
                    })
                    .expect("invalid state_machine_data");

                let cursor_result_continuation = Arc::new(continuation);

                Some(Err(CursorError::FdbError(
                    fdb_error,
                    cursor_result_continuation,
                )))
            }
        }
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
            RawRecordForwardScanStateMachineState::RawRecordAvailable => match event {
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
                RawRecordForwardScanStateMachineEvent::LimitReached { continuation } => {
                    self.state_machine_data = Some(
                        RawRecordForwardScanStateMachineStateData::RawRecordLimitReached {
                            continuation,
                        },
                    );
                    RawRecordForwardScanStateMachineState::RawRecordLimitReached
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

/// Reverse scan state machine state.
#[derive(Debug, PartialEq)]
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

/// Reverse scan state machine state data.
#[derive(Debug, PartialEq)]
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

/// Reverse scan state machine event.
#[derive(Debug)]
enum RawRecordReverseScanStateMachineEvent {
    LastSplitOk {
        data_splits: i8,
        primary_key: RawRecordPrimaryKey,
        last_split_value: Value,
        continuation: RawRecordContinuationInternal,
        records_already_returned: usize,
    },
    Available {
        raw_record: RawRecord,
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
/// See `sismic/raw_record_reverse_scan.yaml` for the design of the
/// state machine.
pub(super) struct RawRecordReverseScanStateMachine {
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
    pub(super) async fn next(
        &mut self,
        key_value_cursor: &mut KeyValueCursor,
        primary_key_schema: &RawRecordPrimaryKeySchema,
        values_limit: usize,
    ) -> Option<CursorResult<RawRecord>> {
        match self.state_machine_state {
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
            RawRecordReverseScanStateMachineState::ReadLastSplit => {
                // Extract and verify state data.
                //
                // Non-final state. We *must* call
                // `step_once_with_event`.
                let (
                    data_splits,
                    primary_key,
                    last_split_value,
                    continuation,
                    records_already_returned,
                ) = self
                    .state_machine_data
                    .take()
                    .and_then(|state_machine_data| {
                        if let RawRecordReverseScanStateMachineStateData::ReadLastSplit {
                            data_splits,
                            primary_key,
                            last_split_value,
                            continuation,
                            records_already_returned,
                        } = state_machine_data
                        {
                            Some((
                                data_splits,
                                primary_key,
                                last_split_value,
                                continuation,
                                records_already_returned,
                            ))
                        } else {
                            None
                        }
                    })
                    .expect("invalid state_machine_data");

                // When `next_split_index == -1` then we would have
                // read the entire record data.
                let mut next_split_index = data_splits - 1;

                let mut record_data_btreemap = BTreeMap::new();

                record_data_btreemap.insert(next_split_index, Bytes::from(last_split_value));

                next_split_index = next_split_index - 1;

                // Extract a value of type `Result<FdbResult<Bytes>,
                // CursorError>`.
                //
                // The inner `Bytes` would be the record data that has
                // been reassembled.
                //
                // As we are returning a value of
                // `Result<FdbResult<Bytes>, CursorError>`, we are not
                // capturing continuation information.
                //
                // Continuation information would have gotten captured
                // if we returned a value of type
                // `CursorResult<FdbResult<Bytes>>`.
                //
                // However, we do not need continuation information at
                // this stage. Continuation information becomes
                // relevant when we read the record header key.
                let next_record_data = loop {
                    if next_split_index == -1 {
                        // When `next_split_index == -1`, means we
                        // expect to read record header next. So we
                        // can reassemble the record data.
                        let mut record_data_buf = BytesMut::new();

                        record_data_btreemap.into_values().for_each(|b| {
                            record_data_buf.put(b);
                        });

                        break Ok(Ok(Bytes::from(record_data_buf)));
                    } else {
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

                                        if idx == next_split_index {
                                            Ok(tup)
                                        } else {
                                            Err(FdbError::new(RAW_RECORD_CURSOR_NEXT_ERROR))
                                        }
                                    })
                                    .and_then(|tup| {
                                        // We check if our primary key
                                        // tuple matches with the tuple
                                        // that we are seeing at the
                                        // current `next_split_index`.
                                        if primary_key.key_ref() == &tup {
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
                                // `cursor_success` is a value of type
                                // `CursorResult<FdbResult<Bytes>>`. If we
                                // have a inner `Err` value, then we assume it
                                // to be a next error.
                                //
                                // We do not care about
                                // `CursorResultContinuation` inside
                                // `CursorResult` because the actual
                                // continuation that we need to return
                                // in case of success would be the
                                // record header continuation.
                                let fdb_result = cursor_success.into_value();

                                match fdb_result {
                                    Ok(record_bytes) => {
                                        record_data_btreemap
                                            .insert(next_split_index, Bytes::from(record_bytes));

                                        // Decrement `next_split_index`
                                        next_split_index -= 1;

                                        // Continue iterating the loop.
                                    }
                                    Err(err) => break Ok(Err(err)),
                                }
                            }
                            Err(cursor_error) => break Err(cursor_error),
                        }
                    }
                };

                match next_record_data {
                    Ok(fdb_result) => match fdb_result {
                        Ok(record_bytes) => {
                            // We have record bytes. We now need to
                            // read the record version.
                            let next_kv = key_value_cursor.next().await;

                            // Extract a value of type
                            // `CursorResult<FdbResult<RecordVersion>>`.
                            let res = next_kv.map(|cursor_success| {
                                cursor_success.map(|key_value| {
                                    let (key, value) = key_value.into_parts();

                                    // Extract a value of type
                                    // `FdbResult<RecordVersion>`,
                                    // which will give us the
                                    // information that we need to
                                    // make the correct transition.
                                    Tuple::try_from(key)
                                        .and_then(|mut tup| {
                                            // Verify that the split index is
                                            // `-1`.
                                            let idx = tup.pop_back::<i8>().ok_or_else(|| {
                                                FdbError::new(RAW_RECORD_CURSOR_NEXT_ERROR)
                                            })?;
                                            if idx == -1 {
                                                Ok(tup)
                                            } else {
                                                Err(FdbError::new(RAW_RECORD_CURSOR_NEXT_ERROR))
                                            }
                                        })
                                        .and_then(|tup| {
                                            // We check if our primary key
                                            // tuple matches with the tuple
                                            // that we are seeing at the
                                            // current `next_split_index`.
                                            if primary_key.key_ref() == &tup {
                                                Ok(())
                                            } else {
                                                Err(FdbError::new(RAW_RECORD_CURSOR_NEXT_ERROR))
                                            }
                                        })
                                        .and_then(|_| {
                                            let (record_header_data_splits, record_version) =
                                                RecordHeaderV0::try_from(value)?.into_parts();

                                            // Ensure that data splits
                                            // in the header matches
                                            // with what we saw when
                                            // we read the last value
                                            // split.
                                            if record_header_data_splits == data_splits {
                                                Ok(record_version)
                                            } else {
                                                Err(FdbError::new(RAW_RECORD_CURSOR_NEXT_ERROR))
                                            }
                                        })
                                })
                            });

                            match res {
                                Ok(cursor_success) => {
                                    // `cursor_success` is a value of
                                    // type
                                    // `CursorResult<FdbResult<RecordVersion>>`. If
                                    // we have a inner `Err` value,
                                    // then we assume it to be a next
                                    // error.
                                    let (res, kv_continuation) = cursor_success.into_parts();
                                    match res {
                                        Ok(record_version) => {
                                            let kv_continuation: Arc<
                                                dyn Any + Send + Sync + 'static,
                                            > = kv_continuation;

                                            // Downcasting should not fail. But if
                                            // does, send `NextError` event.
                                            match kv_continuation
                                                .downcast::<KeyValueContinuationInternal>()
                                            {
                                                Ok(arc_kv_continuation_internal) => {
                                                    let kv_continuation_internal =
                                                        Arc::unwrap_or_clone(
                                                            arc_kv_continuation_internal,
                                                        );
                                                    let KeyValueContinuationInternal::V1(
                                                        pb_keyvalue_continuation_internal_v1,
                                                    ) = kv_continuation_internal;

                                                    // This is our new
                                                    // continuation based on
                                                    // `kv_continuation.
                                                    let continuation =
                                                        RawRecordContinuationInternal::from(
                                                            pb_keyvalue_continuation_internal_v1,
                                                        );

                                                    let raw_record = RawRecord::from((
                                                        primary_key,
                                                        record_version,
                                                        record_bytes,
                                                    ));

                                                    self.step_once_with_event(
							RawRecordReverseScanStateMachineEvent::Available {
							    raw_record,
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
                                    CursorError::NoNextReason(no_next_reason) => {
                                        match no_next_reason {
                                            NoNextReason::SourceExhausted(_) => {
                                                // We are not suppose to get a
                                                // `SourceExhausted` error.
                                                self.step_once_with_event(
						    RawRecordReverseScanStateMachineEvent::NextError {
							continuation
						    });
                                                None
                                            }
                                            NoNextReason::ReturnLimitReached(_) => {
                                                // We do not set in-band limit on the
                                                // key value cursor. This is an
                                                // unexpected state error.
                                                let fdb_error =
                                                    FdbError::new(RAW_RECORD_CURSOR_STATE_ERROR);
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
                                        }
                                    }
                                },
                            }
                        }
                        Err(_) => {
                            self.step_once_with_event(
                                RawRecordReverseScanStateMachineEvent::NextError { continuation },
                            );
                            None
                        }
                    },
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
                                // We are not suppose to get a
                                // `SourceExhausted` error. We have
                                // not read the record header yet, so
                                // this is totally unexpected.
                                self.step_once_with_event(
                                    RawRecordReverseScanStateMachineEvent::NextError {
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
            RawRecordReverseScanStateMachineState::RawRecordAvailable => {
                // Extract and verify state data.
                //
                // Non-final state. We *must* call
                // `step_once_with_event`.
                //
                // In addition, we will be returning a `Some(...)` (a
                // value of `RawRecord`) in this state.
                let (raw_record, continuation, mut records_already_returned) = self
                    .state_machine_data
                    .take()
                    .and_then(|state_machine_data| {
                        if let RawRecordReverseScanStateMachineStateData::RawRecordAvailable {
                            raw_record,
                            continuation,
                            records_already_returned,
                        } = state_machine_data
                        {
                            Some((raw_record, continuation, records_already_returned))
                        } else {
                            None
                        }
                    })
                    .expect("invalid state_machine_data");

                if records_already_returned + 1 == values_limit {
                    // First check if we are going to hit the
                    // `values_limit`.
                    //
                    // This also ensures that in case we encounter a
                    // situation of `LimitReached` and
                    // `SourceExhausted` happening at the same time,
                    // we will return `LimitReached`.
                    self.step_once_with_event(
                        RawRecordReverseScanStateMachineEvent::LimitReached {
                            continuation: continuation.clone(),
                        },
                    );
                } else {
                    // We still need to return more raw records. Attempt
                    // to read the next raw record's last data split.

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
                                    let idx = tup.pop_back::<i8>().ok_or_else(|| {
                                        FdbError::new(RAW_RECORD_CURSOR_NEXT_ERROR)
                                    })?;
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
                                    // We will be returning a raw
                                    // record below.
                                    records_already_returned += 1;

                                    self.step_once_with_event(
                                        RawRecordReverseScanStateMachineEvent::LastSplitOk {
                                            primary_key,
                                            data_splits,
                                            last_split_value,
                                            continuation: continuation.clone(),
                                            records_already_returned,
                                        },
                                    );
                                }
                                Err(_) => {
                                    self.step_once_with_event(
                                        RawRecordReverseScanStateMachineEvent::NextError {
                                            continuation: continuation.clone(),
                                        },
                                    );
                                }
                            }
                        }
                        Err(cursor_error) => match cursor_error {
                            CursorError::FdbError(fdb_error, _) => {
                                self.step_once_with_event(
                                    RawRecordReverseScanStateMachineEvent::FdbError {
                                        fdb_error,
                                        continuation: continuation.clone(),
                                    },
                                );
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
                                }
                                NoNextReason::ReturnLimitReached(_) => {
                                    // We do not set in-band limit on the
                                    // key value cursor. This is an
                                    // unexpected state error.
                                    let fdb_error = FdbError::new(RAW_RECORD_CURSOR_STATE_ERROR);
                                    self.step_once_with_event(
                                        RawRecordReverseScanStateMachineEvent::FdbError {
                                            fdb_error,
                                            continuation: continuation.clone(),
                                        },
                                    );
                                }
                                // Out of band errors
                                NoNextReason::TimeLimitReached(_) => {
                                    let out_of_band_error_type =
                                        LimitManagerStoppedReason::TimeLimitReached;
                                    self.step_once_with_event(
                                        RawRecordReverseScanStateMachineEvent::OutOfBandError {
                                            out_of_band_error_type,
                                            continuation: continuation.clone(),
                                        },
                                    );
                                }
                                NoNextReason::ByteLimitReached(_) => {
                                    let out_of_band_error_type =
                                        LimitManagerStoppedReason::ByteLimitReached;
                                    self.step_once_with_event(
                                        RawRecordReverseScanStateMachineEvent::OutOfBandError {
                                            out_of_band_error_type,
                                            continuation: continuation.clone(),
                                        },
                                    );
                                }
                                NoNextReason::KeyValueLimitReached(_) => {
                                    let out_of_band_error_type =
                                        LimitManagerStoppedReason::KeyValueLimitReached;
                                    self.step_once_with_event(
                                        RawRecordReverseScanStateMachineEvent::OutOfBandError {
                                            out_of_band_error_type,
                                            continuation: continuation.clone(),
                                        },
                                    );
                                }
                            },
                        },
                    }
                }

                let cursor_result_continuation = Arc::new(continuation);
                Some(Ok(CursorSuccess::new(
                    raw_record,
                    cursor_result_continuation,
                )))
            }
            RawRecordReverseScanStateMachineState::RawRecordNextError => {
                // Final state. We do not `take` state data from
                // `state_machine_data`.
                //
                // We also do not call `step_once_with_event`.
                //
                // We will be returning a `Some(...)` (a value of
                // `CursorError`) in this state.
                let continuation = self
                    .state_machine_data
                    .as_ref()
                    .and_then(|state_machine_data| {
                        if let RawRecordReverseScanStateMachineStateData::RawRecordNextError {
                            continuation,
                        } = state_machine_data
                        {
                            Some(continuation.clone())
                        } else {
                            None
                        }
                    })
                    .expect("invalid state_machine_data");

                let cursor_result_continuation = Arc::new(continuation);
                let fdb_error = FdbError::new(RAW_RECORD_CURSOR_NEXT_ERROR);

                Some(Err(CursorError::FdbError(
                    fdb_error,
                    cursor_result_continuation,
                )))
            }
            RawRecordReverseScanStateMachineState::RawRecordLimitReached => {
                // Final state. We do not `take` state data from
                // `state_machine_data`.
                //
                // We also do not call `step_once_with_event`.
                //
                // We will be returning a `Some(...)` (a value of
                // `CursorError`) in this state.
                let continuation = self
                    .state_machine_data
                    .as_ref()
                    .and_then(|state_machine_data| {
                        if let RawRecordReverseScanStateMachineStateData::RawRecordLimitReached {
                            continuation,
                        } = state_machine_data
                        {
                            Some(continuation.clone())
                        } else {
                            None
                        }
                    })
                    .expect("invalid state_machine_data");

                let cursor_result_continuation = Arc::new(continuation);

                Some(Err(CursorError::NoNextReason(
                    NoNextReason::ReturnLimitReached(cursor_result_continuation),
                )))
            }
            RawRecordReverseScanStateMachineState::RawRecordEndOfStream => {
                // Final state. We do not `take` state data from
                // `state_machine_data`.
                //
                // We also do not call `step_once_with_event`.
                //
                // We will be returning a `Some(...)` (a value of
                // `CursorError`) in this state.
                let continuation = {
                    let KeyValueContinuationInternal::V1(pb_keyvalue_continuation_internal_v1) =
                        KeyValueContinuationInternal::new_v1_end_marker();

                    RawRecordContinuationInternal::from(pb_keyvalue_continuation_internal_v1)
                };

                let cursor_result_continuation = Arc::new(continuation);

                Some(Err(CursorError::NoNextReason(
                    NoNextReason::SourceExhausted(cursor_result_continuation),
                )))
            }
            RawRecordReverseScanStateMachineState::OutOfBandError => {
                // Final state. We do not `take` state data from
                // `state_machine_data`.
                //
                // We also do not call `step_once_with_event`.
                //
                // We will be returning a `Some(...)` (a value of
                // `CursorError`) in this state.
                let (out_of_band_error_type, continuation) = self
                    .state_machine_data
                    .as_ref()
                    .and_then(|state_machine_data| {
                        if let RawRecordReverseScanStateMachineStateData::OutOfBandError {
                            out_of_band_error_type,
                            continuation,
                        } = state_machine_data
                        {
                            Some((out_of_band_error_type.clone(), continuation.clone()))
                        } else {
                            None
                        }
                    })
                    .expect("invalid state_machine_data");

                let cursor_result_continuation = Arc::new(continuation);

                let no_next_reason = match out_of_band_error_type {
                    LimitManagerStoppedReason::TimeLimitReached => {
                        NoNextReason::TimeLimitReached(cursor_result_continuation)
                    }
                    LimitManagerStoppedReason::ByteLimitReached => {
                        NoNextReason::ByteLimitReached(cursor_result_continuation)
                    }
                    LimitManagerStoppedReason::KeyValueLimitReached => {
                        NoNextReason::KeyValueLimitReached(cursor_result_continuation)
                    }
                };

                Some(Err(CursorError::NoNextReason(no_next_reason)))
            }
            RawRecordReverseScanStateMachineState::FdbError => {
                // Final state. We do not `take` state data from
                // `state_machine_data`.
                //
                // We also do not call `step_once_with_event`.
                //
                // We will be returning a `Some(...)` (a value of
                // `CursorError`) in this state.
                let (fdb_error, continuation) = self
                    .state_machine_data
                    .as_ref()
                    .and_then(|state_machine_data| {
                        if let RawRecordReverseScanStateMachineStateData::FdbError {
                            fdb_error,
                            continuation,
                        } = state_machine_data
                        {
                            Some((fdb_error.clone(), continuation.clone()))
                        } else {
                            None
                        }
                    })
                    .expect("invalid state_machine_data");

                let cursor_result_continuation = Arc::new(continuation);

                Some(Err(CursorError::FdbError(
                    fdb_error,
                    cursor_result_continuation,
                )))
            }
        }
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
            RawRecordReverseScanStateMachineState::ReadLastSplit => match event {
                RawRecordReverseScanStateMachineEvent::Available {
                    raw_record,
                    continuation,
                    records_already_returned,
                } => {
                    self.state_machine_data = Some(
                        RawRecordReverseScanStateMachineStateData::RawRecordAvailable {
                            raw_record,
                            continuation,
                            records_already_returned,
                        },
                    );
                    RawRecordReverseScanStateMachineState::RawRecordAvailable
                }
                RawRecordReverseScanStateMachineEvent::NextError { continuation } => {
                    self.state_machine_data = Some(
                        RawRecordReverseScanStateMachineStateData::RawRecordNextError {
                            continuation,
                        },
                    );
                    RawRecordReverseScanStateMachineState::RawRecordNextError
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
            RawRecordReverseScanStateMachineState::RawRecordAvailable => match event {
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
                RawRecordReverseScanStateMachineEvent::LimitReached { continuation } => {
                    self.state_machine_data = Some(
                        RawRecordReverseScanStateMachineStateData::RawRecordLimitReached {
                            continuation,
                        },
                    );
                    RawRecordReverseScanStateMachineState::RawRecordLimitReached
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

/// State machine consisting of [`RawRecordForwardScanStateMachine`]
/// or [`RawRecordReverseScanStateMachine`].
#[derive(Debug)]
pub(super) enum RawRecordStateMachine {
    ForwardScan(RawRecordForwardScanStateMachine),
    ReverseScan(RawRecordReverseScanStateMachine),
}

#[cfg(test)]
mod tests {
    mod raw_record_forward_scan_state_machine {
        use bytes::{BufMut, Bytes, BytesMut};

        use fdb::error::FdbError;
        use fdb::tuple::{Tuple, TupleSchema, TupleSchemaElement, Versionstamp};

        use std::convert::TryFrom;
        use std::panic::{self, AssertUnwindSafe};

        use crate::cursor::{KeyValueContinuationInternal, LimitManagerStoppedReason};
        use crate::RecordVersion;

        use super::super::{
            RawRecordForwardScanStateMachine, RawRecordForwardScanStateMachineEvent,
            RawRecordForwardScanStateMachineState, RawRecordForwardScanStateMachineStateData,
        };

        use super::super::super::{
            RawRecord, RawRecordContinuationInternal, RawRecordPrimaryKey,
            RawRecordPrimaryKeySchema,
        };

        #[test]
        fn step_once_with_event() {
            // `InitiateRecordVersionRead` state
            {
                // Valid
                {
                    // `RecordVersionOk` event
                    {
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::InitiateRecordVersionRead,
                                state_machine_data: None,
                            };

                        let event_data = (
                            1,
                            RecordVersion::from(Versionstamp::complete(
                                {
                                    let mut b = BytesMut::new();
                                    b.put_u64(1066);
                                    b.put_u16(1);
                                    Bytes::from(b)
                                },
                                10,
                            )),
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

                                RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_begin_marker();

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            0,
                        );

                        let (
                            data_splits,
                            record_version,
                            primary_key,
                            continuation,
                            records_already_returned,
                        ) = event_data.clone();

                        let event = RawRecordForwardScanStateMachineEvent::RecordVersionOk {
                            data_splits,
                            record_version,
                            primary_key,
                            continuation,
                            records_already_returned,
                        };

                        raw_record_forward_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_state,
                            RawRecordForwardScanStateMachineState::ReadRecordVersion
                        );

                        let (
                            data_splits,
                            record_version,
                            primary_key,
                            continuation,
                            records_already_returned,
                        ) = event_data;

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_data,
                            Some(
                                RawRecordForwardScanStateMachineStateData::ReadRecordVersion {
                                    data_splits,
                                    record_version,
                                    primary_key,
                                    continuation,
                                    records_already_returned,
                                }
                            )
                        );
                    }
                    // `NextError` event
                    {
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::InitiateRecordVersionRead,
                                state_machine_data: None,
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data.clone();

                        let event =
                            RawRecordForwardScanStateMachineEvent::NextError { continuation };

                        raw_record_forward_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_state,
                            RawRecordForwardScanStateMachineState::RawRecordNextError
                        );

                        let (continuation,) = event_data;

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_data,
                            Some(
                                RawRecordForwardScanStateMachineStateData::RawRecordNextError {
                                    continuation
                                }
                            )
                        );
                    }
                    // `EndOfStream` event
                    {
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::InitiateRecordVersionRead,
                                state_machine_data: None,
                            };

                        let event = RawRecordForwardScanStateMachineEvent::EndOfStream;

                        raw_record_forward_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_state,
                            RawRecordForwardScanStateMachineState::RawRecordEndOfStream
                        );

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_data,
                            Some(RawRecordForwardScanStateMachineStateData::RawRecordEndOfStream)
                        );
                    }
                    // `OutOfBandError` event
                    {
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::InitiateRecordVersionRead,
                                state_machine_data: None,
                            };

                        let event_data = (LimitManagerStoppedReason::TimeLimitReached, {
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        });

                        let (out_of_band_error_type, continuation) = event_data.clone();

                        let event = RawRecordForwardScanStateMachineEvent::OutOfBandError {
                            out_of_band_error_type,
                            continuation,
                        };

                        raw_record_forward_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_state,
                            RawRecordForwardScanStateMachineState::OutOfBandError
                        );

                        let (out_of_band_error_type, continuation) = event_data;

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_data,
                            Some(RawRecordForwardScanStateMachineStateData::OutOfBandError {
                                out_of_band_error_type,
                                continuation,
                            })
                        );
                    }
                    // `FdbError` event
                    {
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::InitiateRecordVersionRead,
                                state_machine_data: None,
                            };

                        let event_data = (
                            {
                                // `100` is an arbitrary number that we
                                // are using. There is no specific reason
                                // for using it.
                                FdbError::new(100)
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                        );

                        let (fdb_error, continuation) = event_data.clone();

                        let event = RawRecordForwardScanStateMachineEvent::FdbError {
                            fdb_error,
                            continuation,
                        };

                        raw_record_forward_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_state,
                            RawRecordForwardScanStateMachineState::FdbError,
                        );

                        let (fdb_error, continuation) = event_data.clone();

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_data,
                            Some(RawRecordForwardScanStateMachineStateData::FdbError {
                                fdb_error,
                                continuation,
                            })
                        );
                    }
                }
                // Invalid
                {
                    // `Available` event
                    {
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::InitiateRecordVersionRead,
                                state_machine_data: None,
                            };

                        let event_data = (
                            {
                                let primary_key = {
                                    let mut ts = TupleSchema::new();
                                    ts.push_back(TupleSchemaElement::Bytes);
                                    ts.push_back(TupleSchemaElement::String);
                                    ts.push_back(TupleSchemaElement::Integer);

                                    let schema = RawRecordPrimaryKeySchema::try_from(ts).unwrap();

                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                                };

                                let version = RecordVersion::from(Versionstamp::complete(
                                    {
                                        let mut b = BytesMut::new();
                                        b.put_u64(1066);
                                        b.put_u16(1);
                                        Bytes::from(b)
                                    },
                                    10,
                                ));

                                let record_bytes = Bytes::from_static(b"Hello world!");

                                RawRecord::from((primary_key, version, record_bytes))
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            1,
                        );

                        let (raw_record, continuation, records_already_returned) = event_data;

                        let event = RawRecordForwardScanStateMachineEvent::Available {
                            raw_record,
                            continuation,
                            records_already_returned,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `LimitReached` event
                    {
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::InitiateRecordVersionRead,
                                state_machine_data: None,
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data;

                        let event =
                            RawRecordForwardScanStateMachineEvent::LimitReached { continuation };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                }
            }
            // `ReadRecordVersion` state
            {
                // Valid
                {
                    // `Available` event
                    {
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::ReadRecordVersion,
                                state_machine_data: None,
                            };

                        let event_data = (
                            {
                                let primary_key = {
                                    let mut ts = TupleSchema::new();
                                    ts.push_back(TupleSchemaElement::Bytes);
                                    ts.push_back(TupleSchemaElement::String);
                                    ts.push_back(TupleSchemaElement::Integer);

                                    let schema = RawRecordPrimaryKeySchema::try_from(ts).unwrap();

                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                                };

                                let version = RecordVersion::from(Versionstamp::complete(
                                    {
                                        let mut b = BytesMut::new();
                                        b.put_u64(1066);
                                        b.put_u16(1);
                                        Bytes::from(b)
                                    },
                                    10,
                                ));

                                let record_bytes = Bytes::from_static(b"Hello world!");

                                RawRecord::from((primary_key, version, record_bytes))
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            1,
                        );

                        let (raw_record, continuation, records_already_returned) =
                            event_data.clone();

                        let event = RawRecordForwardScanStateMachineEvent::Available {
                            raw_record,
                            continuation,
                            records_already_returned,
                        };

                        raw_record_forward_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_state,
                            RawRecordForwardScanStateMachineState::RawRecordAvailable
                        );
                    }
                    // `NextError` event
                    {
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::ReadRecordVersion,
                                state_machine_data: None,
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data.clone();

                        let event =
                            RawRecordForwardScanStateMachineEvent::NextError { continuation };

                        raw_record_forward_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_state,
                            RawRecordForwardScanStateMachineState::RawRecordNextError
                        );

                        let (continuation,) = event_data;

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_data,
                            Some(
                                RawRecordForwardScanStateMachineStateData::RawRecordNextError {
                                    continuation
                                }
                            )
                        );
                    }
                    // `OutOfBandError` event
                    {
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::ReadRecordVersion,
                                state_machine_data: None,
                            };

                        let event_data = (LimitManagerStoppedReason::TimeLimitReached, {
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        });

                        let (out_of_band_error_type, continuation) = event_data.clone();

                        let event = RawRecordForwardScanStateMachineEvent::OutOfBandError {
                            out_of_band_error_type,
                            continuation,
                        };

                        raw_record_forward_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_state,
                            RawRecordForwardScanStateMachineState::OutOfBandError
                        );

                        let (out_of_band_error_type, continuation) = event_data;

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_data,
                            Some(RawRecordForwardScanStateMachineStateData::OutOfBandError {
                                out_of_band_error_type,
                                continuation,
                            })
                        );
                    }
                    // `FdbError` event
                    {
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::ReadRecordVersion,
                                state_machine_data: None,
                            };

                        let event_data = (
                            {
                                // `100` is an arbitrary number that we
                                // are using. There is no specific reason
                                // for using it.
                                FdbError::new(100)
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                        );

                        let (fdb_error, continuation) = event_data.clone();

                        let event = RawRecordForwardScanStateMachineEvent::FdbError {
                            fdb_error,
                            continuation,
                        };

                        raw_record_forward_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_state,
                            RawRecordForwardScanStateMachineState::FdbError,
                        );

                        let (fdb_error, continuation) = event_data.clone();

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_data,
                            Some(RawRecordForwardScanStateMachineStateData::FdbError {
                                fdb_error,
                                continuation,
                            })
                        );
                    }
                }
                // Invalid
                {
                    // `RecordVersionOk` event
                    {
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::ReadRecordVersion,
                                state_machine_data: None,
                            };

                        let event_data = (
                            1,
                            RecordVersion::from(Versionstamp::complete(
                                {
                                    let mut b = BytesMut::new();
                                    b.put_u64(1066);
                                    b.put_u16(1);
                                    Bytes::from(b)
                                },
                                10,
                            )),
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

                                RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_begin_marker();

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            0,
                        );

                        let (
                            data_splits,
                            record_version,
                            primary_key,
                            continuation,
                            records_already_returned,
                        ) = event_data.clone();

                        let event = RawRecordForwardScanStateMachineEvent::RecordVersionOk {
                            data_splits,
                            record_version,
                            primary_key,
                            continuation,
                            records_already_returned,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `LimitReached` event
                    {
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::ReadRecordVersion,
                                state_machine_data: None,
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data;

                        let event =
                            RawRecordForwardScanStateMachineEvent::LimitReached { continuation };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `EndOfStream` event
                    {
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::ReadRecordVersion,
                                state_machine_data: None,
                            };

                        let event = RawRecordForwardScanStateMachineEvent::EndOfStream;

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                }
            }
            // `RawRecordAvailable` state
            {
                // Valid
                {
                    // `RecordVersionOk` event
                    {
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordAvailable,
                                state_machine_data: None,
                            };

                        let event_data = (
                            1,
                            RecordVersion::from(Versionstamp::complete(
                                {
                                    let mut b = BytesMut::new();
                                    b.put_u64(1066);
                                    b.put_u16(1);
                                    Bytes::from(b)
                                },
                                10,
                            )),
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

                                RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_begin_marker();

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            0,
                        );

                        let (
                            data_splits,
                            record_version,
                            primary_key,
                            continuation,
                            records_already_returned,
                        ) = event_data.clone();

                        let event = RawRecordForwardScanStateMachineEvent::RecordVersionOk {
                            data_splits,
                            record_version,
                            primary_key,
                            continuation,
                            records_already_returned,
                        };

                        raw_record_forward_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_state,
                            RawRecordForwardScanStateMachineState::ReadRecordVersion
                        );

                        let (
                            data_splits,
                            record_version,
                            primary_key,
                            continuation,
                            records_already_returned,
                        ) = event_data;

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_data,
                            Some(
                                RawRecordForwardScanStateMachineStateData::ReadRecordVersion {
                                    data_splits,
                                    record_version,
                                    primary_key,
                                    continuation,
                                    records_already_returned,
                                }
                            )
                        );
                    }
                    // `NextError` event
                    {
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordAvailable,
                                state_machine_data: None,
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data.clone();

                        let event =
                            RawRecordForwardScanStateMachineEvent::NextError { continuation };

                        raw_record_forward_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_state,
                            RawRecordForwardScanStateMachineState::RawRecordNextError
                        );

                        let (continuation,) = event_data;

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_data,
                            Some(
                                RawRecordForwardScanStateMachineStateData::RawRecordNextError {
                                    continuation
                                }
                            )
                        );
                    }
                    // `LimitReached` event
                    {
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordAvailable,
                                state_machine_data: None,
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data.clone();

                        let event =
                            RawRecordForwardScanStateMachineEvent::LimitReached { continuation };

                        raw_record_forward_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_state,
                            RawRecordForwardScanStateMachineState::RawRecordLimitReached
                        );

                        let (continuation,) = event_data;

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_data,
                            Some(
                                RawRecordForwardScanStateMachineStateData::RawRecordLimitReached {
                                    continuation
                                }
                            )
                        );
                    }
                    // `EndOfStream` event
                    {
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordAvailable,
                                state_machine_data: None,
                            };

                        let event = RawRecordForwardScanStateMachineEvent::EndOfStream;

                        raw_record_forward_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_state,
                            RawRecordForwardScanStateMachineState::RawRecordEndOfStream
                        );

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_data,
                            Some(RawRecordForwardScanStateMachineStateData::RawRecordEndOfStream)
                        );
                    }
                    // `OutOfBandError` event
                    {
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordAvailable,
                                state_machine_data: None,
                            };

                        let event_data = (LimitManagerStoppedReason::TimeLimitReached, {
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        });

                        let (out_of_band_error_type, continuation) = event_data.clone();

                        let event = RawRecordForwardScanStateMachineEvent::OutOfBandError {
                            out_of_band_error_type,
                            continuation,
                        };

                        raw_record_forward_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_state,
                            RawRecordForwardScanStateMachineState::OutOfBandError
                        );

                        let (out_of_band_error_type, continuation) = event_data;

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_data,
                            Some(RawRecordForwardScanStateMachineStateData::OutOfBandError {
                                out_of_band_error_type,
                                continuation,
                            })
                        );
                    }
                    // `FdbError` event
                    {
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordAvailable,
                                state_machine_data: None,
                            };

                        let event_data = (
                            {
                                // `100` is an arbitrary number that we
                                // are using. There is no specific reason
                                // for using it.
                                FdbError::new(100)
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                        );

                        let (fdb_error, continuation) = event_data.clone();

                        let event = RawRecordForwardScanStateMachineEvent::FdbError {
                            fdb_error,
                            continuation,
                        };

                        raw_record_forward_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_state,
                            RawRecordForwardScanStateMachineState::FdbError,
                        );

                        let (fdb_error, continuation) = event_data.clone();

                        assert_eq!(
                            raw_record_forward_scan_state_machine.state_machine_data,
                            Some(RawRecordForwardScanStateMachineStateData::FdbError {
                                fdb_error,
                                continuation,
                            })
                        );
                    }
                }
                // Invalid
                {
                    // `Available` event
                    {
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordAvailable,
                                state_machine_data: None,
                            };

                        let event_data = (
                            {
                                let primary_key = {
                                    let mut ts = TupleSchema::new();
                                    ts.push_back(TupleSchemaElement::Bytes);
                                    ts.push_back(TupleSchemaElement::String);
                                    ts.push_back(TupleSchemaElement::Integer);

                                    let schema = RawRecordPrimaryKeySchema::try_from(ts).unwrap();

                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                                };

                                let version = RecordVersion::from(Versionstamp::complete(
                                    {
                                        let mut b = BytesMut::new();
                                        b.put_u64(1066);
                                        b.put_u16(1);
                                        Bytes::from(b)
                                    },
                                    10,
                                ));

                                let record_bytes = Bytes::from_static(b"Hello world!");

                                RawRecord::from((primary_key, version, record_bytes))
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            1,
                        );

                        let (raw_record, continuation, records_already_returned) = event_data;

                        let event = RawRecordForwardScanStateMachineEvent::Available {
                            raw_record,
                            continuation,
                            records_already_returned,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                }
            }
            // `RawRecordNextError` state
            {
                // Final state. All events would be invalid
                {
                    // `RecordVersionOk` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordNextError,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::RawRecordNextError {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (
                            1,
                            RecordVersion::from(Versionstamp::complete(
                                {
                                    let mut b = BytesMut::new();
                                    b.put_u64(1066);
                                    b.put_u16(1);
                                    Bytes::from(b)
                                },
                                10,
                            )),
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

                                RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_begin_marker();

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            0,
                        );

                        let (
                            data_splits,
                            record_version,
                            primary_key,
                            continuation,
                            records_already_returned,
                        ) = event_data.clone();

                        let event = RawRecordForwardScanStateMachineEvent::RecordVersionOk {
                            data_splits,
                            record_version,
                            primary_key,
                            continuation,
                            records_already_returned,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `Available` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordNextError,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::RawRecordNextError {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (
                            {
                                let primary_key = {
                                    let mut ts = TupleSchema::new();
                                    ts.push_back(TupleSchemaElement::Bytes);
                                    ts.push_back(TupleSchemaElement::String);
                                    ts.push_back(TupleSchemaElement::Integer);

                                    let schema = RawRecordPrimaryKeySchema::try_from(ts).unwrap();

                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                                };

                                let version = RecordVersion::from(Versionstamp::complete(
                                    {
                                        let mut b = BytesMut::new();
                                        b.put_u64(1066);
                                        b.put_u16(1);
                                        Bytes::from(b)
                                    },
                                    10,
                                ));

                                let record_bytes = Bytes::from_static(b"Hello world!");

                                RawRecord::from((primary_key, version, record_bytes))
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            1,
                        );

                        let (raw_record, continuation, records_already_returned) = event_data;

                        let event = RawRecordForwardScanStateMachineEvent::Available {
                            raw_record,
                            continuation,
                            records_already_returned,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `NextError` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordNextError,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::RawRecordNextError {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data;

                        let event =
                            RawRecordForwardScanStateMachineEvent::NextError { continuation };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `LimitReached` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordNextError,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::RawRecordNextError {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data;

                        let event =
                            RawRecordForwardScanStateMachineEvent::LimitReached { continuation };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `EndOfStream` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordNextError,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::RawRecordNextError {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event = RawRecordForwardScanStateMachineEvent::EndOfStream;

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `OutOfBandError` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordNextError,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::RawRecordNextError {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (LimitManagerStoppedReason::TimeLimitReached, {
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        });

                        let (out_of_band_error_type, continuation) = event_data;

                        let event = RawRecordForwardScanStateMachineEvent::OutOfBandError {
                            out_of_band_error_type,
                            continuation,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `FdbError` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordNextError,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::RawRecordNextError {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (
                            {
                                // `100` is an arbitrary number that we
                                // are using. There is no specific reason
                                // for using it.
                                FdbError::new(100)
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                        );

                        let (fdb_error, continuation) = event_data;

                        let event = RawRecordForwardScanStateMachineEvent::FdbError {
                            fdb_error,
                            continuation,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                }
            }
            // `RawRecordLimitReached` state
            {
                // Final state. All events would be invalid
                {
                    // `RecordVersionOk` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordLimitReached,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::RawRecordLimitReached {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (
                            1,
                            RecordVersion::from(Versionstamp::complete(
                                {
                                    let mut b = BytesMut::new();
                                    b.put_u64(1066);
                                    b.put_u16(1);
                                    Bytes::from(b)
                                },
                                10,
                            )),
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

                                RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_begin_marker();

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            0,
                        );

                        let (
                            data_splits,
                            record_version,
                            primary_key,
                            continuation,
                            records_already_returned,
                        ) = event_data.clone();

                        let event = RawRecordForwardScanStateMachineEvent::RecordVersionOk {
                            data_splits,
                            record_version,
                            primary_key,
                            continuation,
                            records_already_returned,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `Available` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordLimitReached,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::RawRecordLimitReached {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (
                            {
                                let primary_key = {
                                    let mut ts = TupleSchema::new();
                                    ts.push_back(TupleSchemaElement::Bytes);
                                    ts.push_back(TupleSchemaElement::String);
                                    ts.push_back(TupleSchemaElement::Integer);

                                    let schema = RawRecordPrimaryKeySchema::try_from(ts).unwrap();

                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                                };

                                let version = RecordVersion::from(Versionstamp::complete(
                                    {
                                        let mut b = BytesMut::new();
                                        b.put_u64(1066);
                                        b.put_u16(1);
                                        Bytes::from(b)
                                    },
                                    10,
                                ));

                                let record_bytes = Bytes::from_static(b"Hello world!");

                                RawRecord::from((primary_key, version, record_bytes))
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            1,
                        );

                        let (raw_record, continuation, records_already_returned) = event_data;

                        let event = RawRecordForwardScanStateMachineEvent::Available {
                            raw_record,
                            continuation,
                            records_already_returned,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `NextError` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordLimitReached,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::RawRecordLimitReached {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data;

                        let event =
                            RawRecordForwardScanStateMachineEvent::NextError { continuation };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `LimitReached` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordLimitReached,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::RawRecordLimitReached {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data;

                        let event =
                            RawRecordForwardScanStateMachineEvent::LimitReached { continuation };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `EndOfStream` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordLimitReached,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::RawRecordLimitReached {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event = RawRecordForwardScanStateMachineEvent::EndOfStream;

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `OutOfBandError` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordLimitReached,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::RawRecordLimitReached {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (LimitManagerStoppedReason::TimeLimitReached, {
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        });

                        let (out_of_band_error_type, continuation) = event_data;

                        let event = RawRecordForwardScanStateMachineEvent::OutOfBandError {
                            out_of_band_error_type,
                            continuation,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `FdbError` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordLimitReached,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::RawRecordLimitReached {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (
                            {
                                // `100` is an arbitrary number that we
                                // are using. There is no specific reason
                                // for using it.
                                FdbError::new(100)
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                        );

                        let (fdb_error, continuation) = event_data;

                        let event = RawRecordForwardScanStateMachineEvent::FdbError {
                            fdb_error,
                            continuation,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                }
            }
            // `RawRecordEndOfStream` state
            {
                // Final state. All events would be invalid
                {
                    // `RecordVersionOk` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordEndOfStream,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::RawRecordEndOfStream,
                                ),
                            };

                        let event_data = (
                            1,
                            RecordVersion::from(Versionstamp::complete(
                                {
                                    let mut b = BytesMut::new();
                                    b.put_u64(1066);
                                    b.put_u16(1);
                                    Bytes::from(b)
                                },
                                10,
                            )),
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

                                RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_begin_marker();

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            0,
                        );

                        let (
                            data_splits,
                            record_version,
                            primary_key,
                            continuation,
                            records_already_returned,
                        ) = event_data.clone();

                        let event = RawRecordForwardScanStateMachineEvent::RecordVersionOk {
                            data_splits,
                            record_version,
                            primary_key,
                            continuation,
                            records_already_returned,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `Available` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordEndOfStream,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::RawRecordEndOfStream,
                                ),
                            };

                        let event_data = (
                            {
                                let primary_key = {
                                    let mut ts = TupleSchema::new();
                                    ts.push_back(TupleSchemaElement::Bytes);
                                    ts.push_back(TupleSchemaElement::String);
                                    ts.push_back(TupleSchemaElement::Integer);

                                    let schema = RawRecordPrimaryKeySchema::try_from(ts).unwrap();

                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                                };

                                let version = RecordVersion::from(Versionstamp::complete(
                                    {
                                        let mut b = BytesMut::new();
                                        b.put_u64(1066);
                                        b.put_u16(1);
                                        Bytes::from(b)
                                    },
                                    10,
                                ));

                                let record_bytes = Bytes::from_static(b"Hello world!");

                                RawRecord::from((primary_key, version, record_bytes))
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            1,
                        );

                        let (raw_record, continuation, records_already_returned) = event_data;

                        let event = RawRecordForwardScanStateMachineEvent::Available {
                            raw_record,
                            continuation,
                            records_already_returned,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `NextError` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordEndOfStream,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::RawRecordEndOfStream,
                                ),
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data;

                        let event =
                            RawRecordForwardScanStateMachineEvent::NextError { continuation };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `LimitReached` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordEndOfStream,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::RawRecordEndOfStream,
                                ),
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data;

                        let event =
                            RawRecordForwardScanStateMachineEvent::LimitReached { continuation };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `EndOfStream` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordEndOfStream,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::RawRecordEndOfStream,
                                ),
                            };

                        let event = RawRecordForwardScanStateMachineEvent::EndOfStream;

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `OutOfBandError` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordEndOfStream,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::RawRecordEndOfStream,
                                ),
                            };

                        let event_data = (LimitManagerStoppedReason::TimeLimitReached, {
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        });

                        let (out_of_band_error_type, continuation) = event_data;

                        let event = RawRecordForwardScanStateMachineEvent::OutOfBandError {
                            out_of_band_error_type,
                            continuation,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `FdbError` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::RawRecordEndOfStream,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::RawRecordEndOfStream,
                                ),
                            };

                        let event_data = (
                            {
                                // `100` is an arbitrary number that we
                                // are using. There is no specific reason
                                // for using it.
                                FdbError::new(100)
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                        );

                        let (fdb_error, continuation) = event_data;

                        let event = RawRecordForwardScanStateMachineEvent::FdbError {
                            fdb_error,
                            continuation,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                }
            }
            // `OutOfBandError` state
            {
                // Final state. All events would be invalid
                {
                    // `RecordVersionOk` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::OutOfBandError,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::OutOfBandError {
                                        out_of_band_error_type:
                                            LimitManagerStoppedReason::TimeLimitReached,
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (
                            1,
                            RecordVersion::from(Versionstamp::complete(
                                {
                                    let mut b = BytesMut::new();
                                    b.put_u64(1066);
                                    b.put_u16(1);
                                    Bytes::from(b)
                                },
                                10,
                            )),
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

                                RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_begin_marker();

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            0,
                        );

                        let (
                            data_splits,
                            record_version,
                            primary_key,
                            continuation,
                            records_already_returned,
                        ) = event_data.clone();

                        let event = RawRecordForwardScanStateMachineEvent::RecordVersionOk {
                            data_splits,
                            record_version,
                            primary_key,
                            continuation,
                            records_already_returned,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `Available` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::OutOfBandError,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::OutOfBandError {
                                        out_of_band_error_type:
                                            LimitManagerStoppedReason::TimeLimitReached,
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (
                            {
                                let primary_key = {
                                    let mut ts = TupleSchema::new();
                                    ts.push_back(TupleSchemaElement::Bytes);
                                    ts.push_back(TupleSchemaElement::String);
                                    ts.push_back(TupleSchemaElement::Integer);

                                    let schema = RawRecordPrimaryKeySchema::try_from(ts).unwrap();

                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                                };

                                let version = RecordVersion::from(Versionstamp::complete(
                                    {
                                        let mut b = BytesMut::new();
                                        b.put_u64(1066);
                                        b.put_u16(1);
                                        Bytes::from(b)
                                    },
                                    10,
                                ));

                                let record_bytes = Bytes::from_static(b"Hello world!");

                                RawRecord::from((primary_key, version, record_bytes))
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            1,
                        );

                        let (raw_record, continuation, records_already_returned) = event_data;

                        let event = RawRecordForwardScanStateMachineEvent::Available {
                            raw_record,
                            continuation,
                            records_already_returned,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `NextError` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::OutOfBandError,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::OutOfBandError {
                                        out_of_band_error_type:
                                            LimitManagerStoppedReason::TimeLimitReached,
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data;

                        let event =
                            RawRecordForwardScanStateMachineEvent::NextError { continuation };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `LimitReached` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::OutOfBandError,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::OutOfBandError {
                                        out_of_band_error_type:
                                            LimitManagerStoppedReason::TimeLimitReached,
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data;

                        let event =
                            RawRecordForwardScanStateMachineEvent::LimitReached { continuation };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `EndOfStream` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::OutOfBandError,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::OutOfBandError {
                                        out_of_band_error_type:
                                            LimitManagerStoppedReason::TimeLimitReached,
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event = RawRecordForwardScanStateMachineEvent::EndOfStream;

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `OutOfBandError` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::OutOfBandError,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::OutOfBandError {
                                        out_of_band_error_type:
                                            LimitManagerStoppedReason::TimeLimitReached,
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (LimitManagerStoppedReason::TimeLimitReached, {
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        });

                        let (out_of_band_error_type, continuation) = event_data;

                        let event = RawRecordForwardScanStateMachineEvent::OutOfBandError {
                            out_of_band_error_type,
                            continuation,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `FdbError` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::OutOfBandError,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::OutOfBandError {
                                        out_of_band_error_type:
                                            LimitManagerStoppedReason::TimeLimitReached,
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (
                            {
                                // `100` is an arbitrary number that we
                                // are using. There is no specific reason
                                // for using it.
                                FdbError::new(100)
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                        );

                        let (fdb_error, continuation) = event_data;

                        let event = RawRecordForwardScanStateMachineEvent::FdbError {
                            fdb_error,
                            continuation,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                }
            }
            // `FdbError` state
            {
                // Final state. All events would be invalid
                {
                    // `RecordVersionOk` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::FdbError,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::FdbError {
                                        // `100` is an arbitrary number that we
                                        // are using. There is no specific reason
                                        // for using it.
                                        fdb_error: FdbError::new(100),
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (
                            1,
                            RecordVersion::from(Versionstamp::complete(
                                {
                                    let mut b = BytesMut::new();
                                    b.put_u64(1066);
                                    b.put_u16(1);
                                    Bytes::from(b)
                                },
                                10,
                            )),
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

                                RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_begin_marker();

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            0,
                        );

                        let (
                            data_splits,
                            record_version,
                            primary_key,
                            continuation,
                            records_already_returned,
                        ) = event_data.clone();

                        let event = RawRecordForwardScanStateMachineEvent::RecordVersionOk {
                            data_splits,
                            record_version,
                            primary_key,
                            continuation,
                            records_already_returned,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `Available` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::FdbError,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::FdbError {
                                        // `100` is an arbitrary number that we
                                        // are using. There is no specific reason
                                        // for using it.
                                        fdb_error: FdbError::new(100),
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (
                            {
                                let primary_key = {
                                    let mut ts = TupleSchema::new();
                                    ts.push_back(TupleSchemaElement::Bytes);
                                    ts.push_back(TupleSchemaElement::String);
                                    ts.push_back(TupleSchemaElement::Integer);

                                    let schema = RawRecordPrimaryKeySchema::try_from(ts).unwrap();

                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                                };

                                let version = RecordVersion::from(Versionstamp::complete(
                                    {
                                        let mut b = BytesMut::new();
                                        b.put_u64(1066);
                                        b.put_u16(1);
                                        Bytes::from(b)
                                    },
                                    10,
                                ));

                                let record_bytes = Bytes::from_static(b"Hello world!");

                                RawRecord::from((primary_key, version, record_bytes))
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            1,
                        );

                        let (raw_record, continuation, records_already_returned) = event_data;

                        let event = RawRecordForwardScanStateMachineEvent::Available {
                            raw_record,
                            continuation,
                            records_already_returned,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `NextError` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::FdbError,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::FdbError {
                                        // `100` is an arbitrary number that we
                                        // are using. There is no specific reason
                                        // for using it.
                                        fdb_error: FdbError::new(100),
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data;

                        let event =
                            RawRecordForwardScanStateMachineEvent::NextError { continuation };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `LimitReached` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::FdbError,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::FdbError {
                                        // `100` is an arbitrary number that we
                                        // are using. There is no specific reason
                                        // for using it.
                                        fdb_error: FdbError::new(100),
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data;

                        let event =
                            RawRecordForwardScanStateMachineEvent::LimitReached { continuation };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `EndOfStream` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::FdbError,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::FdbError {
                                        // `100` is an arbitrary number that we
                                        // are using. There is no specific reason
                                        // for using it.
                                        fdb_error: FdbError::new(100),
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event = RawRecordForwardScanStateMachineEvent::EndOfStream;

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `OutOfBandError` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::FdbError,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::FdbError {
                                        // `100` is an arbitrary number that we
                                        // are using. There is no specific reason
                                        // for using it.
                                        fdb_error: FdbError::new(100),
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (LimitManagerStoppedReason::TimeLimitReached, {
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        });

                        let (out_of_band_error_type, continuation) = event_data;

                        let event = RawRecordForwardScanStateMachineEvent::OutOfBandError {
                            out_of_band_error_type,
                            continuation,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `FdbError` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_forward_scan_state_machine =
                            RawRecordForwardScanStateMachine {
                                state_machine_state:
                                    RawRecordForwardScanStateMachineState::FdbError,
                                state_machine_data: Some(
                                    RawRecordForwardScanStateMachineStateData::FdbError {
                                        // `100` is an arbitrary number that we
                                        // are using. There is no specific reason
                                        // for using it.
                                        fdb_error: FdbError::new(100),
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (
                            {
                                // `100` is an arbitrary number that we
                                // are using. There is no specific reason
                                // for using it.
                                FdbError::new(100)
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                        );

                        let (fdb_error, continuation) = event_data;

                        let event = RawRecordForwardScanStateMachineEvent::FdbError {
                            fdb_error,
                            continuation,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_forward_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                }
            }
        }
    }

    mod raw_record_reverse_scan_state_machine {
        use bytes::{BufMut, Bytes, BytesMut};

        use fdb::error::FdbError;
        use fdb::tuple::{Tuple, TupleSchema, TupleSchemaElement, Versionstamp};
        use fdb::Value;

        use std::convert::TryFrom;
        use std::panic::{self, AssertUnwindSafe};

        use crate::cursor::{KeyValueContinuationInternal, LimitManagerStoppedReason};
        use crate::RecordVersion;

        use super::super::{
            RawRecordReverseScanStateMachine, RawRecordReverseScanStateMachineEvent,
            RawRecordReverseScanStateMachineState, RawRecordReverseScanStateMachineStateData,
        };

        use super::super::super::{
            RawRecord, RawRecordContinuationInternal, RawRecordPrimaryKey,
            RawRecordPrimaryKeySchema,
        };

        #[test]
        fn step_once_with_event() {
            // `InitiateLastSplitRead` state
            {
                // Valid
                {
                    // `LastSplitOk` event
                    {
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::InitiateLastSplitRead,
                                state_machine_data: None,
                            };

                        let event_data = (
                            1,
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

                                RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                            },
                            Value::from(Bytes::from_static(b"hello")),
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_begin_marker();

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            0,
                        );

                        let (
                            data_splits,
                            primary_key,
                            last_split_value,
                            continuation,
                            records_already_returned,
                        ) = event_data.clone();

                        let event = RawRecordReverseScanStateMachineEvent::LastSplitOk {
                            data_splits,
                            primary_key,
                            last_split_value,
                            continuation,
                            records_already_returned,
                        };

                        raw_record_reverse_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_state,
                            RawRecordReverseScanStateMachineState::ReadLastSplit
                        );

                        let (
                            data_splits,
                            primary_key,
                            last_split_value,
                            continuation,
                            records_already_returned,
                        ) = event_data;

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_data,
                            Some(RawRecordReverseScanStateMachineStateData::ReadLastSplit {
                                data_splits,
                                primary_key,
                                last_split_value,
                                continuation,
                                records_already_returned
                            })
                        );
                    }
                    // `NextError` event
                    {
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::InitiateLastSplitRead,
                                state_machine_data: None,
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data.clone();

                        let event =
                            RawRecordReverseScanStateMachineEvent::NextError { continuation };

                        raw_record_reverse_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_state,
                            RawRecordReverseScanStateMachineState::RawRecordNextError
                        );

                        let (continuation,) = event_data;

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_data,
                            Some(
                                RawRecordReverseScanStateMachineStateData::RawRecordNextError {
                                    continuation
                                }
                            )
                        );
                    }
                    // `EndOfStream` event
                    {
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::InitiateLastSplitRead,
                                state_machine_data: None,
                            };

                        let event = RawRecordReverseScanStateMachineEvent::EndOfStream;

                        raw_record_reverse_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_state,
                            RawRecordReverseScanStateMachineState::RawRecordEndOfStream
                        );

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_data,
                            Some(RawRecordReverseScanStateMachineStateData::RawRecordEndOfStream)
                        );
                    }
                    // `OutOfBandError` event
                    {
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::InitiateLastSplitRead,
                                state_machine_data: None,
                            };

                        let event_data = (LimitManagerStoppedReason::TimeLimitReached, {
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        });

                        let (out_of_band_error_type, continuation) = event_data.clone();

                        let event = RawRecordReverseScanStateMachineEvent::OutOfBandError {
                            out_of_band_error_type,
                            continuation,
                        };

                        raw_record_reverse_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_state,
                            RawRecordReverseScanStateMachineState::OutOfBandError
                        );

                        let (out_of_band_error_type, continuation) = event_data;

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_data,
                            Some(RawRecordReverseScanStateMachineStateData::OutOfBandError {
                                out_of_band_error_type,
                                continuation,
                            })
                        );
                    }
                    // `FdbError` event
                    {
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::InitiateLastSplitRead,
                                state_machine_data: None,
                            };

                        let event_data = (
                            {
                                // `100` is an arbitrary number that we
                                // are using. There is no specific reason
                                // for using it.
                                FdbError::new(100)
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                        );

                        let (fdb_error, continuation) = event_data.clone();

                        let event = RawRecordReverseScanStateMachineEvent::FdbError {
                            fdb_error,
                            continuation,
                        };

                        raw_record_reverse_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_state,
                            RawRecordReverseScanStateMachineState::FdbError,
                        );

                        let (fdb_error, continuation) = event_data.clone();

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_data,
                            Some(RawRecordReverseScanStateMachineStateData::FdbError {
                                fdb_error,
                                continuation,
                            })
                        );
                    }
                }
                // Invalid
                {
                    // `Available` event
                    {
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::InitiateLastSplitRead,
                                state_machine_data: None,
                            };

                        let event_data = (
                            {
                                let primary_key = {
                                    let mut ts = TupleSchema::new();
                                    ts.push_back(TupleSchemaElement::Bytes);
                                    ts.push_back(TupleSchemaElement::String);
                                    ts.push_back(TupleSchemaElement::Integer);

                                    let schema = RawRecordPrimaryKeySchema::try_from(ts).unwrap();

                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                                };

                                let version = RecordVersion::from(Versionstamp::complete(
                                    {
                                        let mut b = BytesMut::new();
                                        b.put_u64(1066);
                                        b.put_u16(1);
                                        Bytes::from(b)
                                    },
                                    10,
                                ));

                                let record_bytes = Bytes::from_static(b"Hello world!");

                                RawRecord::from((primary_key, version, record_bytes))
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            1,
                        );

                        let (raw_record, continuation, records_already_returned) = event_data;

                        let event = RawRecordReverseScanStateMachineEvent::Available {
                            raw_record,
                            continuation,
                            records_already_returned,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_reverse_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `LimitReached` event
                    {
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::InitiateLastSplitRead,
                                state_machine_data: None,
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data;

                        let event =
                            RawRecordReverseScanStateMachineEvent::LimitReached { continuation };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_reverse_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                }
            }
            // `ReadLastSplit` state
            {
                // Valid
                {
                    // `Available` event
                    {
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::ReadLastSplit,
                                state_machine_data: None,
                            };

                        let event_data = (
                            {
                                let primary_key = {
                                    let mut ts = TupleSchema::new();
                                    ts.push_back(TupleSchemaElement::Bytes);
                                    ts.push_back(TupleSchemaElement::String);
                                    ts.push_back(TupleSchemaElement::Integer);

                                    let schema = RawRecordPrimaryKeySchema::try_from(ts).unwrap();

                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                                };

                                let version = RecordVersion::from(Versionstamp::complete(
                                    {
                                        let mut b = BytesMut::new();
                                        b.put_u64(1066);
                                        b.put_u16(1);
                                        Bytes::from(b)
                                    },
                                    10,
                                ));

                                let record_bytes = Bytes::from_static(b"Hello world!");

                                RawRecord::from((primary_key, version, record_bytes))
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            1,
                        );

                        let (raw_record, continuation, records_already_returned) =
                            event_data.clone();

                        let event = RawRecordReverseScanStateMachineEvent::Available {
                            raw_record,
                            continuation,
                            records_already_returned,
                        };

                        raw_record_reverse_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_state,
                            RawRecordReverseScanStateMachineState::RawRecordAvailable
                        );
                    }
                    // `NextError` event
                    {
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::ReadLastSplit,
                                state_machine_data: None,
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data.clone();

                        let event =
                            RawRecordReverseScanStateMachineEvent::NextError { continuation };

                        raw_record_reverse_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_state,
                            RawRecordReverseScanStateMachineState::RawRecordNextError
                        );

                        let (continuation,) = event_data;

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_data,
                            Some(
                                RawRecordReverseScanStateMachineStateData::RawRecordNextError {
                                    continuation
                                }
                            )
                        );
                    }
                    // `OutOfBandError` event
                    {
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::ReadLastSplit,
                                state_machine_data: None,
                            };

                        let event_data = (LimitManagerStoppedReason::TimeLimitReached, {
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        });

                        let (out_of_band_error_type, continuation) = event_data.clone();

                        let event = RawRecordReverseScanStateMachineEvent::OutOfBandError {
                            out_of_band_error_type,
                            continuation,
                        };

                        raw_record_reverse_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_state,
                            RawRecordReverseScanStateMachineState::OutOfBandError
                        );

                        let (out_of_band_error_type, continuation) = event_data;

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_data,
                            Some(RawRecordReverseScanStateMachineStateData::OutOfBandError {
                                out_of_band_error_type,
                                continuation,
                            })
                        );
                    }
                    // `FdbError` event
                    {
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::ReadLastSplit,
                                state_machine_data: None,
                            };

                        let event_data = (
                            {
                                // `100` is an arbitrary number that we
                                // are using. There is no specific reason
                                // for using it.
                                FdbError::new(100)
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                        );

                        let (fdb_error, continuation) = event_data.clone();

                        let event = RawRecordReverseScanStateMachineEvent::FdbError {
                            fdb_error,
                            continuation,
                        };

                        raw_record_reverse_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_state,
                            RawRecordReverseScanStateMachineState::FdbError,
                        );

                        let (fdb_error, continuation) = event_data.clone();

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_data,
                            Some(RawRecordReverseScanStateMachineStateData::FdbError {
                                fdb_error,
                                continuation,
                            })
                        );
                    }
                }
                // Invalid
                {
                    // `LastSplitOk` event
                    {
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::ReadLastSplit,
                                state_machine_data: None,
                            };

                        let event_data = (
                            1,
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

                                RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                            },
                            Value::from(Bytes::from_static(b"hello")),
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_begin_marker();

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            0,
                        );

                        let (
                            data_splits,
                            primary_key,
                            last_split_value,
                            continuation,
                            records_already_returned,
                        ) = event_data;

                        let event = RawRecordReverseScanStateMachineEvent::LastSplitOk {
                            data_splits,
                            primary_key,
                            last_split_value,
                            continuation,
                            records_already_returned,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_reverse_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `LimitReached` event
                    {
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::ReadLastSplit,
                                state_machine_data: None,
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data;

                        let event =
                            RawRecordReverseScanStateMachineEvent::LimitReached { continuation };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_reverse_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `EndOfStream` event
                    {
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::ReadLastSplit,
                                state_machine_data: None,
                            };

                        let event = RawRecordReverseScanStateMachineEvent::EndOfStream;

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_reverse_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                }
            }
            // `RawRecordAvailable` state
            {
                // Valid
                {
                    // `LastSplitOk` event
                    {
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::RawRecordAvailable,
                                state_machine_data: None,
                            };

                        let event_data = (
                            1,
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

                                RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                            },
                            Value::from(Bytes::from_static(b"hello")),
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_begin_marker();

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            0,
                        );

                        let (
                            data_splits,
                            primary_key,
                            last_split_value,
                            continuation,
                            records_already_returned,
                        ) = event_data.clone();

                        let event = RawRecordReverseScanStateMachineEvent::LastSplitOk {
                            data_splits,
                            primary_key,
                            last_split_value,
                            continuation,
                            records_already_returned,
                        };

                        raw_record_reverse_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_state,
                            RawRecordReverseScanStateMachineState::ReadLastSplit
                        );

                        let (
                            data_splits,
                            primary_key,
                            last_split_value,
                            continuation,
                            records_already_returned,
                        ) = event_data;

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_data,
                            Some(RawRecordReverseScanStateMachineStateData::ReadLastSplit {
                                data_splits,
                                primary_key,
                                last_split_value,
                                continuation,
                                records_already_returned
                            })
                        );
                    }
                    // `NextError` event
                    {
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::RawRecordAvailable,
                                state_machine_data: None,
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data.clone();

                        let event =
                            RawRecordReverseScanStateMachineEvent::NextError { continuation };

                        raw_record_reverse_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_state,
                            RawRecordReverseScanStateMachineState::RawRecordNextError
                        );

                        let (continuation,) = event_data;

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_data,
                            Some(
                                RawRecordReverseScanStateMachineStateData::RawRecordNextError {
                                    continuation
                                }
                            )
                        );
                    }
                    // `LimitReached` event
                    {
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::RawRecordAvailable,
                                state_machine_data: None,
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data.clone();

                        let event =
                            RawRecordReverseScanStateMachineEvent::LimitReached { continuation };

                        raw_record_reverse_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_state,
                            RawRecordReverseScanStateMachineState::RawRecordLimitReached
                        );

                        let (continuation,) = event_data;

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_data,
                            Some(
                                RawRecordReverseScanStateMachineStateData::RawRecordLimitReached {
                                    continuation
                                }
                            )
                        );
                    }
                    // `EndOfStream` event
                    {
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::RawRecordAvailable,
                                state_machine_data: None,
                            };

                        let event = RawRecordReverseScanStateMachineEvent::EndOfStream;

                        raw_record_reverse_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_state,
                            RawRecordReverseScanStateMachineState::RawRecordEndOfStream
                        );

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_data,
                            Some(RawRecordReverseScanStateMachineStateData::RawRecordEndOfStream)
                        );
                    }
                    // `OutOfBandError` event
                    {
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::RawRecordAvailable,
                                state_machine_data: None,
                            };

                        let event_data = (LimitManagerStoppedReason::TimeLimitReached, {
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        });

                        let (out_of_band_error_type, continuation) = event_data.clone();

                        let event = RawRecordReverseScanStateMachineEvent::OutOfBandError {
                            out_of_band_error_type,
                            continuation,
                        };

                        raw_record_reverse_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_state,
                            RawRecordReverseScanStateMachineState::OutOfBandError
                        );

                        let (out_of_band_error_type, continuation) = event_data;

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_data,
                            Some(RawRecordReverseScanStateMachineStateData::OutOfBandError {
                                out_of_band_error_type,
                                continuation,
                            })
                        );
                    }
                    // `FdbError` event
                    {
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::RawRecordAvailable,
                                state_machine_data: None,
                            };

                        let event_data = (
                            {
                                // `100` is an arbitrary number that we
                                // are using. There is no specific reason
                                // for using it.
                                FdbError::new(100)
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                        );

                        let (fdb_error, continuation) = event_data.clone();

                        let event = RawRecordReverseScanStateMachineEvent::FdbError {
                            fdb_error,
                            continuation,
                        };

                        raw_record_reverse_scan_state_machine.step_once_with_event(event);

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_state,
                            RawRecordReverseScanStateMachineState::FdbError,
                        );

                        let (fdb_error, continuation) = event_data.clone();

                        assert_eq!(
                            raw_record_reverse_scan_state_machine.state_machine_data,
                            Some(RawRecordReverseScanStateMachineStateData::FdbError {
                                fdb_error,
                                continuation,
                            })
                        );
                    }
                }
                // Invalid
                {
                    // `Available` event
                    {
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::RawRecordAvailable,
                                state_machine_data: None,
                            };

                        let event_data = (
                            {
                                let primary_key = {
                                    let mut ts = TupleSchema::new();
                                    ts.push_back(TupleSchemaElement::Bytes);
                                    ts.push_back(TupleSchemaElement::String);
                                    ts.push_back(TupleSchemaElement::Integer);

                                    let schema = RawRecordPrimaryKeySchema::try_from(ts).unwrap();

                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                                };

                                let version = RecordVersion::from(Versionstamp::complete(
                                    {
                                        let mut b = BytesMut::new();
                                        b.put_u64(1066);
                                        b.put_u16(1);
                                        Bytes::from(b)
                                    },
                                    10,
                                ));

                                let record_bytes = Bytes::from_static(b"Hello world!");

                                RawRecord::from((primary_key, version, record_bytes))
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            1,
                        );

                        let (raw_record, continuation, records_already_returned) = event_data;

                        let event = RawRecordReverseScanStateMachineEvent::Available {
                            raw_record,
                            continuation,
                            records_already_returned,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_reverse_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                }
            }
            // `RawRecordNextError` state
            {
                // Final state. All events would be invalid
                {
                    // `LastSplitOk` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::RawRecordNextError,
                                state_machine_data: Some(
                                    RawRecordReverseScanStateMachineStateData::RawRecordNextError {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (
                            1,
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

                                RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                            },
                            Value::from(Bytes::from_static(b"hello")),
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_begin_marker();

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            0,
                        );

                        let (
                            data_splits,
                            primary_key,
                            last_split_value,
                            continuation,
                            records_already_returned,
                        ) = event_data;

                        let event = RawRecordReverseScanStateMachineEvent::LastSplitOk {
                            data_splits,
                            primary_key,
                            last_split_value,
                            continuation,
                            records_already_returned,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_reverse_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `Available` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::RawRecordNextError,
                                state_machine_data: Some(
                                    RawRecordReverseScanStateMachineStateData::RawRecordNextError {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (
                            {
                                let primary_key = {
                                    let mut ts = TupleSchema::new();
                                    ts.push_back(TupleSchemaElement::Bytes);
                                    ts.push_back(TupleSchemaElement::String);
                                    ts.push_back(TupleSchemaElement::Integer);

                                    let schema = RawRecordPrimaryKeySchema::try_from(ts).unwrap();

                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                                };

                                let version = RecordVersion::from(Versionstamp::complete(
                                    {
                                        let mut b = BytesMut::new();
                                        b.put_u64(1066);
                                        b.put_u16(1);
                                        Bytes::from(b)
                                    },
                                    10,
                                ));

                                let record_bytes = Bytes::from_static(b"Hello world!");

                                RawRecord::from((primary_key, version, record_bytes))
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            1,
                        );

                        let (raw_record, continuation, records_already_returned) = event_data;

                        let event = RawRecordReverseScanStateMachineEvent::Available {
                            raw_record,
                            continuation,
                            records_already_returned,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_reverse_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `NextError` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::RawRecordNextError,
                                state_machine_data: Some(
                                    RawRecordReverseScanStateMachineStateData::RawRecordNextError {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data;

                        let event =
                            RawRecordReverseScanStateMachineEvent::NextError { continuation };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_reverse_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `LimitReached` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::RawRecordNextError,
                                state_machine_data: Some(
                                    RawRecordReverseScanStateMachineStateData::RawRecordNextError {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data;

                        let event =
                            RawRecordReverseScanStateMachineEvent::LimitReached { continuation };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_reverse_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `EndOfStream` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::RawRecordNextError,
                                state_machine_data: Some(
                                    RawRecordReverseScanStateMachineStateData::RawRecordNextError {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event = RawRecordReverseScanStateMachineEvent::EndOfStream;

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_reverse_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `OutOfBandError` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::RawRecordNextError,
                                state_machine_data: Some(
                                    RawRecordReverseScanStateMachineStateData::RawRecordNextError {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (LimitManagerStoppedReason::TimeLimitReached, {
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        });

                        let (out_of_band_error_type, continuation) = event_data;

                        let event = RawRecordReverseScanStateMachineEvent::OutOfBandError {
                            out_of_band_error_type,
                            continuation,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_reverse_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `FdbError` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::RawRecordNextError,
                                state_machine_data: Some(
                                    RawRecordReverseScanStateMachineStateData::RawRecordNextError {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (
                            {
                                // `100` is an arbitrary number that we
                                // are using. There is no specific reason
                                // for using it.
                                FdbError::new(100)
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                        );

                        let (fdb_error, continuation) = event_data;

                        let event = RawRecordReverseScanStateMachineEvent::FdbError {
                            fdb_error,
                            continuation,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_reverse_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                }
            }
            // `RawRecordLimitReached` state
            {
                // Final state. All events would be invalid
                {
                    // `LastSplitOk` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::RawRecordLimitReached,
                                state_machine_data: Some(
                                    RawRecordReverseScanStateMachineStateData::RawRecordLimitReached {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (
                            1,
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

                                RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                            },
                            Value::from(Bytes::from_static(b"hello")),
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_begin_marker();

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            0,
                        );

                        let (
                            data_splits,
                            primary_key,
                            last_split_value,
                            continuation,
                            records_already_returned,
                        ) = event_data;

                        let event = RawRecordReverseScanStateMachineEvent::LastSplitOk {
                            data_splits,
                            primary_key,
                            last_split_value,
                            continuation,
                            records_already_returned,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_reverse_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `Available` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::RawRecordLimitReached,
                                state_machine_data: Some(
                                    RawRecordReverseScanStateMachineStateData::RawRecordLimitReached {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (
                            {
                                let primary_key = {
                                    let mut ts = TupleSchema::new();
                                    ts.push_back(TupleSchemaElement::Bytes);
                                    ts.push_back(TupleSchemaElement::String);
                                    ts.push_back(TupleSchemaElement::Integer);

                                    let schema = RawRecordPrimaryKeySchema::try_from(ts).unwrap();

                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    RawRecordPrimaryKey::try_from((schema, key)).unwrap()
                                };

                                let version = RecordVersion::from(Versionstamp::complete(
                                    {
                                        let mut b = BytesMut::new();
                                        b.put_u64(1066);
                                        b.put_u16(1);
                                        Bytes::from(b)
                                    },
                                    10,
                                ));

                                let record_bytes = Bytes::from_static(b"Hello world!");

                                RawRecord::from((primary_key, version, record_bytes))
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                            1,
                        );

                        let (raw_record, continuation, records_already_returned) = event_data;

                        let event = RawRecordReverseScanStateMachineEvent::Available {
                            raw_record,
                            continuation,
                            records_already_returned,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_reverse_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `NextError` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::RawRecordLimitReached,
                                state_machine_data: Some(
                                    RawRecordReverseScanStateMachineStateData::RawRecordLimitReached {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data;

                        let event =
                            RawRecordReverseScanStateMachineEvent::NextError { continuation };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_reverse_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `LimitReached` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::RawRecordLimitReached,
                                state_machine_data: Some(
                                    RawRecordReverseScanStateMachineStateData::RawRecordLimitReached {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = ({
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        },);

                        let (continuation,) = event_data;

                        let event =
                            RawRecordReverseScanStateMachineEvent::LimitReached { continuation };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_reverse_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `EndOfStream` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::RawRecordLimitReached,
                                state_machine_data: Some(
                                    RawRecordReverseScanStateMachineStateData::RawRecordLimitReached {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event = RawRecordReverseScanStateMachineEvent::EndOfStream;

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_reverse_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `OutOfBandError` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::RawRecordLimitReached,
                                state_machine_data: Some(
                                    RawRecordReverseScanStateMachineStateData::RawRecordLimitReached {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (LimitManagerStoppedReason::TimeLimitReached, {
                            let KeyValueContinuationInternal::V1(
                                pb_keyvalue_continuation_internal_v1,
                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                let mut key = Tuple::new();
                                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                key.push_back::<String>("world".to_string());
                                key.push_back::<i8>(0);

                                key.pack()
                            });

                            RawRecordContinuationInternal::from(
                                pb_keyvalue_continuation_internal_v1,
                            )
                        });

                        let (out_of_band_error_type, continuation) = event_data;

                        let event = RawRecordReverseScanStateMachineEvent::OutOfBandError {
                            out_of_band_error_type,
                            continuation,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_reverse_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                    // `FdbError` event
                    {
                        // `state_machine_data` is `Some(...)` for final state.
                        let mut raw_record_reverse_scan_state_machine =
                            RawRecordReverseScanStateMachine {
                                state_machine_state:
                                    RawRecordReverseScanStateMachineState::RawRecordLimitReached,
                                state_machine_data: Some(
                                    RawRecordReverseScanStateMachineStateData::RawRecordLimitReached {
                                        continuation: {
                                            let KeyValueContinuationInternal::V1(
                                                pb_keyvalue_continuation_internal_v1,
                                            ) = KeyValueContinuationInternal::new_v1_key_marker({
                                                let mut key = Tuple::new();
                                                key.push_back::<Bytes>(Bytes::from_static(
                                                    b"hello",
                                                ));
                                                key.push_back::<String>("world".to_string());
                                                key.push_back::<i8>(0);

                                                key.pack()
                                            });

                                            RawRecordContinuationInternal::from(
                                                pb_keyvalue_continuation_internal_v1,
                                            )
                                        },
                                    },
                                ),
                            };

                        let event_data = (
                            {
                                // `100` is an arbitrary number that we
                                // are using. There is no specific reason
                                // for using it.
                                FdbError::new(100)
                            },
                            {
                                let KeyValueContinuationInternal::V1(
                                    pb_keyvalue_continuation_internal_v1,
                                ) = KeyValueContinuationInternal::new_v1_key_marker({
                                    let mut key = Tuple::new();
                                    key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                                    key.push_back::<String>("world".to_string());
                                    key.push_back::<i8>(0);

                                    key.pack()
                                });

                                RawRecordContinuationInternal::from(
                                    pb_keyvalue_continuation_internal_v1,
                                )
                            },
                        );

                        let (fdb_error, continuation) = event_data;

                        let event = RawRecordReverseScanStateMachineEvent::FdbError {
                            fdb_error,
                            continuation,
                        };

                        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
                            raw_record_reverse_scan_state_machine.step_once_with_event(event);
                        }))
                        .is_err());
                    }
                }
            }
            // `RawRecordEndOfStream` state
            {
                // Final state. All events would be invalid
                {
                    // TODO: Continue from here.
                }
            }
            // `OutOfBandError` state
            {
                // Final state. All events would be invalid
                {}
            }
            // `FdbError` state
            {
                // Final state. All events would be invalid
                {}
            }
        }
    }
}
