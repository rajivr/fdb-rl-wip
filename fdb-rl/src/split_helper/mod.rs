//! Helper module for splitting records across multiple key-value pairs.
//!
//! FoundationDB has key-value size [limitation] of 10KB and 100KB
//! respectively. While we do not explicitly check the size of our
//! keys, this module provides functions that when required will split
//! serialized record into multiple 100KB chunks and spreads them
//! across multiple key-value pairs.
//!
//! This is done by adding a suffix to the key. The keyspace for a
//! record is organized as shown below.
//!
//! ```
//! |------------------------------+---------------------|
//! | Key                          | Value               |
//! |------------------------------+---------------------|
//! | (Subspace, Primary Key, -1,) | Record header       |
//! |------------------------------+---------------------|
//! | (Subspace, Primary Key, 0,)  | 100KB chunk or less |
//! |------------------------------+---------------------|
//! | (Subspace, Primary Key, 1,)  | 100KB chunk or less |
//! |------------------------------+---------------------|
//! | (Subspace, Primary Key, 2,)  | 100KB chunk or less |
//! |------------------------------+---------------------|
//! | ...                          | ...                 |
//! | ...                          | ...                 |
//! |------------------------------+---------------------|
//! ```
//!
//! There is a record header at suffix of `-1`. The record header is a
//! tuple of the form:
//!
//! ```
//! (header_version, data_splits, incarnation, versionstamp)
//! ```
//!
//! * The `header_version` is a number describing the version of the
//! header. The form described above is `header_version` of `0`.
//!
//! * `data_splits` is the number of splits of data that is contained
//! within this record. At a minimum, a record will have atleast one
//! data split. This is true *even* in case of an empty record.
//!
//! * `incarnation` is the incarnation of the record. Incarnation is
//! managed using [`RecordContext`] and is incremented each time a
//! record is migrated between FoundationDB clusters.
//!
//! * The `versionstamp` contains information about
//! [`RecordVersion`]'s global version and local version.
//!
//! There is *no* tuple encoding for the data, that is *100KB chunk or
//! less* value is stored in the raw format. This is because tuple
//! encoding would introduce escape sequences which depending on the
//! data might exceed the 100KB limit.
//!
//! **Note:** In the key that gets generated, we flatten the primary
//! key tuple when suffix is appended to it. The primary key is not a
//! *nested* tuple.
//!
//! A minimal record will be of the form
//!
//! ```
//! |------------------------------+---------------|
//! | Key                          | Value         |
//! |------------------------------+---------------|
//! | (Subspace, Primary Key, -1,) | Record header |
//! |------------------------------+---------------|
//! | (Subspace, Primary Key, 0,)  | ""            |
//! |------------------------------+---------------|
//! ```
//!
//! [limitation]: https://apple.github.io/foundationdb/known-limitations.html#large-keys-and-values
//! [`RecordContext`]: crate::RecordContext
//
// In the design, we use atleast one data split even in case of an
// empty record because it helps us model our `RawRecordCursor` more
// easily. This is because when doing forward *or* reverse scan we can
// easily determine the number of key-values to read before expecting
// an record header.

pub(crate) mod error;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use num_bigint::BigInt;

use fdb::error::{FdbError, FdbResult};
use fdb::range::{Range, StreamingMode};
use fdb::subspace::Subspace;
use fdb::transaction::{MutationType, ReadTransaction, Transaction};
use fdb::tuple::{Null, Tuple, Versionstamp};
use fdb::{Key, Value};

use std::convert::{TryFrom, TryInto};
use std::ops::ControlFlow;

use crate::cursor::{CursorError, KeyValueCursorBuilder, NoNextReason};
use crate::range::TupleRange;
use crate::scan::{ScanLimiter, ScanPropertiesBuilder};
use crate::RecordVersion;

use error::{
    SPLIT_HELPER_INVALID_PRIMARY_KEY, SPLIT_HELPER_LOAD_INVALID_RECORD_HEADER,
    SPLIT_HELPER_LOAD_INVALID_SERIALIZED_BYTES, SPLIT_HELPER_SAVE_INVALID_SERIALIZED_BYTES_SIZE,
    SPLIT_HELPER_SCAN_LIMIT_REACHED,
};

/// If a record is greater than this size (in bytes) it will be split
/// into multiple key-value pairs.
const SPLIT_RECORD_SIZE: usize = 100_000;

/// Record header version `0`.
///
/// The [`RawRecord`] type cursor implementation uses this type. If we
/// decide to transition to newer record headers versions, we will
/// need to make corresponding changes to [`RawRecord`].
///
/// [`RawRecord`]: crate::raw_record::RawRecord;
#[derive(Debug, PartialEq)]
pub(crate) struct RecordHeaderV0 {
    header_version: i8,
    data_splits: i8,
    incarnation: Option<u64>,
    versionstamp: Versionstamp,
}

impl RecordHeaderV0 {
    fn new(
        data_splits: i8,
        incarnation: Option<u64>,
        versionstamp: Versionstamp,
    ) -> RecordHeaderV0 {
        RecordHeaderV0 {
            header_version: 0,
            data_splits,
            incarnation,
            versionstamp,
        }
    }

    fn save<Tr>(
        self,
        tr: &Tr,
        maybe_subspace: &Option<Subspace>,
        primary_key: &Tuple,
        complete: bool,
    ) -> FdbResult<()>
    where
        Tr: Transaction,
    {
        let RecordHeaderV0 {
            header_version,
            data_splits,
            incarnation,
            versionstamp,
        } = self;

        // (subspace, primary_key, -1)
        let record_version_key = Key::from({
            let key_tup = {
                // (primary_key, -1)
                let mut t = primary_key.clone();
                t.push_back::<i8>(-1);
                t
            };

            maybe_subspace
                .as_ref()
                .map(|s| s.subspace(&key_tup).pack())
                .unwrap_or_else(|| key_tup.pack())
        });

        let record_version_value_tuple = {
            let tup: (i8, i8, Option<u64>, Versionstamp) =
                (header_version, data_splits, incarnation, versionstamp);

            let mut t = Tuple::new();

            // header_version
            t.push_back::<i8>(tup.0);

            // data_splits
            t.push_back::<i8>(tup.1);

            // incarnation
            if let Some(i) = tup.2 {
                t.push_back::<BigInt>(i.into());
            } else {
                t.push_back::<Null>(Null);
            }

            // versionstamp
            t.push_back::<Versionstamp>(tup.3);

            t
        };

        if complete {
            let record_version_value = Value::from(record_version_value_tuple.pack());

            tr.set(record_version_key, record_version_value);
        } else {
            // We need a value of `Bytes` type here, because of how the
            // API is designed.
            let record_version_value =
                record_version_value_tuple.pack_with_versionstamp(Bytes::new())?;

            unsafe {
                tr.mutate(
                    MutationType::SetVersionstampedValue,
                    record_version_key,
                    record_version_value,
                );
            }
        }

        Ok(())
    }

    pub(crate) fn into_parts(self) -> (i8, RecordVersion) {
        let RecordHeaderV0 {
            data_splits,
            incarnation,
            versionstamp,
            ..
        } = self;

        let record_version = if let Some(i) = incarnation {
            RecordVersion::from((i, versionstamp))
        } else {
            RecordVersion::from(versionstamp)
        };

        (data_splits, record_version)
    }
}

impl TryFrom<Value> for RecordHeaderV0 {
    type Error = FdbError;

    fn try_from(value: Value) -> FdbResult<RecordHeaderV0> {
        Tuple::try_from(value)
            .and_then(|mut tup| {
                // Ensure that the first tuple element is
                // `0`. Otherwise we have a invalid version.
                tup.pop_front::<i8>()
                    .and_then(|x| if x == 0 { Some(tup) } else { None })
                    .ok_or_else(|| FdbError::new(SPLIT_HELPER_LOAD_INVALID_RECORD_HEADER))
            })
            .and_then(|mut tup| {
                // `i8` is good enough to hold values upto 100. 100KB
                // * 100 = 10MB.
                tup.pop_front::<i8>()
                    .and_then(|s| {
                        // There should be at-least one data split for
                        // a record. Otherwise it is an error.
                        if (1..=100).contains(&s) {
                            Some((s, tup))
                        } else {
                            None
                        }
                    })
                    .ok_or_else(|| FdbError::new(SPLIT_HELPER_LOAD_INVALID_RECORD_HEADER))
            })
            .and_then(|(data_splits, mut tup)| {
                // Extract incarnation
                let maybe_incarnation_version = if let Some(x) = tup.pop_front::<BigInt>() {
                    Some(
                        u64::try_from(x)
                            .map_err(|_| FdbError::new(SPLIT_HELPER_LOAD_INVALID_RECORD_HEADER))?,
                    )
                } else if let Some(Null) = tup.pop_front::<Null>() {
                    None
                } else {
                    return Err(FdbError::new(SPLIT_HELPER_LOAD_INVALID_RECORD_HEADER));
                };

                Ok((data_splits, maybe_incarnation_version, tup))
            })
            .and_then(|(data_splits, incarnation, mut tup)| {
                let versionstamp = tup
                    .pop_front::<Versionstamp>()
                    .ok_or_else(|| FdbError::new(SPLIT_HELPER_LOAD_INVALID_RECORD_HEADER))?;

                Ok(RecordHeaderV0::new(data_splits, incarnation, versionstamp))
            })
            .map_err(|_| FdbError::new(SPLIT_HELPER_LOAD_INVALID_RECORD_HEADER))
    }
}

/// Delete the serialized representation of a record **without** any
/// safety checks.
///
/// ### Note
///
/// You will *never* want to use this function. Any mistake with the
/// `primary_key` can seriously damage the database, as it will issue
/// a [`Transaction::clear_range`] **without** any checks.
///
/// The *only* place where this is useful is in the [`delete`] and
/// [`save`] methods, where we have already checked the validity of
/// `primary_key`.
unsafe fn delete_unchecked<Tr>(
    tr: &Tr,
    maybe_subspace: &Option<Subspace>,
    primary_key: &Tuple,
) -> FdbResult<()>
where
    Tr: Transaction,
{
    // This function does not really fail. Because we are converting
    // `TupleRange -> KeyRange -> Range` and due to
    // `Range::try_from(_: KeyRange)`, we get value of `FdbResult`
    // type.
    tr.clear_range(Range::try_from(
        TupleRange::all_of(primary_key).into_key_range(maybe_subspace),
    )?);

    Ok(())
}

/// Delete the serialized representation of a record.
///
/// <p style="background:rgba(255,181,77,0.16);padding:0.75em;">
/// <strong>Warning:</strong> This function is <strong>not</strong>
/// meant to be public. We need to make this function public to
/// support integration tests. Do not use this function in your
/// code.</p>
pub async fn delete<Tr>(
    tr: &Tr,
    maybe_scan_limiter: &Option<ScanLimiter>,
    maybe_subspace: &Option<Subspace>,
    primary_key: &Tuple,
) -> FdbResult<()>
where
    Tr: Transaction,
{
    // Ensure that `primary_key` is valid. We check this by attempting
    // to load the record. When we get either no record or a valid
    // record, then we proceed to delete. Otherwise, we return an
    // error.
    load(tr, maybe_scan_limiter, maybe_subspace, primary_key)
        .await
        .map_err(|e| {
            // `load` function specific errors are:
            //    - `SPLIT_HELPER_LOAD_INVALID_RECORD_HEADER`
            //    - `SPLIT_HELPER_LOAD_INVALID_SERIALIZED_BYTES`
            //
            //  If we see that then convert it to
            //  `SPLIT_HELPER_INVALID_PRIMARY_KEY`, which is a more
            //  general error.
            let error_code = e.code();

            if error_code == SPLIT_HELPER_LOAD_INVALID_RECORD_HEADER
                || error_code == SPLIT_HELPER_LOAD_INVALID_SERIALIZED_BYTES
            {
                FdbError::new(SPLIT_HELPER_INVALID_PRIMARY_KEY)
            } else {
                e
            }
        })
        .and_then(|_| {
            // Safety: If we are here, then it means that
            //         `primary_key` has either a valid record *or* is
            //         empty. We can safely issue a
            //         `delete_unchecked`.
            unsafe { delete_unchecked(tr, maybe_subspace, primary_key) }
        })
}

/// Save serialized representation using multiple keys if necessary.
///
/// ### Note
///
/// If this function returns an error, then in the *context* of the
/// transaction, any previously stored data *will be* deleted.
///
/// This is *only* in the context of transaction. It will not be
/// deleted from the database till the transaction is committed. If
/// you do not want the data to be deleted, you should not commit the
/// transaction.
///
/// If you want to have the data in the event of an error, you must
/// [`load`] it, before calling [`save`].
///
/// <p style="background:rgba(255,181,77,0.16);padding:0.75em;">
/// <strong>Warning:</strong> This function is <strong>not</strong>
/// meant to be public. We need to make this function public to
/// support integration tests. Do not use this function in your
/// code.</p>
pub async fn save<Tr>(
    tr: &Tr,
    maybe_scan_limiter: &Option<ScanLimiter>,
    maybe_subspace: &Option<Subspace>,
    primary_key: &Tuple,
    serialized: Bytes,
    record_version: RecordVersion,
) -> FdbResult<()>
where
    Tr: Transaction,
{
    // *Note:* If this function returns an error, then you *must*
    //         assume that the primary key is in an inconsistent
    //         state.
    fn save_inner<Tr>(
        tr: &Tr,
        maybe_subspace: &Option<Subspace>,
        primary_key: &Tuple,
        mut serialized: Bytes,
        record_version: RecordVersion,
    ) -> FdbResult<()>
    where
        Tr: Transaction,
    {
        // While FoundationDB has 10MB limit [1] for mutations
        // (including for key-values that is written), we do not track
        // that information for the *entire transaction*. For the
        // *entire transaction* the only what this would get surfaced
        // is via `transaction_too_large (2101)` [2] error at the time
        // of committing.
        //
        // We do track when the serialized bytes is greater than 10MB
        // and return an error if that is the case.
        //
        // [1]: https://apple.github.io/foundationdb/known-limitations.html#large-transactions
        // [2]: https://apple.github.io/foundationdb/api-error-codes.html

        // Return error if serialized bytes is greater than 10MB.
        if serialized.len() > (100 * SPLIT_RECORD_SIZE) {
            return Err(FdbError::new(
                SPLIT_HELPER_SAVE_INVALID_SERIALIZED_BYTES_SIZE,
            ));
        }

        let mut suffix = 0;

        loop {
            let value = Value::from(if serialized.len() < SPLIT_RECORD_SIZE {
                serialized.copy_to_bytes(serialized.len())
            } else {
                serialized.copy_to_bytes(SPLIT_RECORD_SIZE)
            });

            let key_tup = {
                // (primary_key, suffix)
                let mut t = primary_key.clone();
                // `i8` is good enough to hold values upto 100. 100KB *
                // 100 = 10MB.
                t.push_back::<i8>(suffix);
                t
            };

            let key = Key::from(
                maybe_subspace
                    .as_ref()
                    .map(|s| s.subspace(&key_tup).pack())
                    .unwrap_or_else(|| key_tup.pack()),
            );

            tr.set(key, value);

            // There is no risk of overflow here because we are
            // checking at the beginning of this function to make sure
            // we return an error in case serialized bytes is greater
            // than 10MB.
            suffix += 1;

            if serialized.len() == 0 {
                break;
            }
        }

        // `suffix` is incremented by 1 before exiting from the loop.
        let data_splits = suffix;

        let (incarnation, global_version, local_version, complete) = record_version.into_parts();

        let versionstamp = Versionstamp::try_from((global_version, local_version))?;

        let record_header_v0 = RecordHeaderV0::new(data_splits, incarnation, versionstamp);

        record_header_v0.save(tr, maybe_subspace, primary_key, complete)?;

        Ok(())
    }

    delete(tr, maybe_scan_limiter, maybe_subspace, primary_key).await?;

    let res = save_inner(tr, maybe_subspace, primary_key, serialized, record_version);

    res.map_err(|e| {
        // Safety: We are checking validity of the `primary_key` in
        //         the call to `delete` above.
        unsafe {
            let _ = delete_unchecked(tr, maybe_subspace, primary_key);
        }
        e
    })
}

/// Load serialized byte array that may be split among several keys.
///
/// When a value of `Ok(None)` is returned, it means that there is no
/// serialized byte array associated with the `primary_key`. If there
/// is a serialized byte array associated with the `primary_key`, then
/// we would return `Ok(Some((record_version, seralized_bytes)))`
/// value. Otherwise, an `Err` value is returned.
///
/// <p style="background:rgba(255,181,77,0.16);padding:0.75em;">
/// <strong>Warning:</strong> This function is <strong>not</strong>
/// meant to be public. We need to make this function public to
/// support integration tests. Do not use this function in your
/// code.</p>
pub async fn load<Tr>(
    tr: &Tr,
    maybe_scan_limiter: &Option<ScanLimiter>,
    maybe_subspace: &Option<Subspace>,
    primary_key: &Tuple,
) -> FdbResult<Option<(RecordVersion, Bytes)>>
where
    Tr: ReadTransaction,
{
    let kv_cursor = {
        let scan_properties = {
            let mut scan_properites_builder = ScanPropertiesBuilder::default();

            unsafe {
                scan_properites_builder.set_range_options(|range_options| {
                    range_options.set_mode(StreamingMode::WantAll)
                });
            }

            if let Some(scan_limiter_ref) = maybe_scan_limiter.as_ref() {
                scan_properites_builder.set_scan_limiter(scan_limiter_ref.clone());
            }

            scan_properites_builder.build()
        };

        let subspace = if let Some(subspace_ref) = maybe_subspace.as_ref() {
            subspace_ref.clone().subspace(primary_key)
        } else {
            Subspace::new(Bytes::new()).subspace(primary_key)
        };

        let mut kv_cursor_builder = KeyValueCursorBuilder::new();

        kv_cursor_builder
            .subspace(subspace)
            .key_range(TupleRange::all().into_key_range(&None))
            .scan_properties(scan_properties);

        kv_cursor_builder.build(tr)
    }?;

    let (mut kv_btree, err) = kv_cursor.into_btreemap().await;

    match err {
        // `NoNextReason::SourceExhausted` is the condition that we
        // need as it indicates that we have read the entire key-value
        // cursor.
        CursorError::NoNextReason(NoNextReason::SourceExhausted(_)) => {
            if kv_btree.len() == 0 {
                Ok(None)
            } else {
                // Extract Record header key-value pair.
                //
                // Note: Here we are assuming the first key in the
                //       BTreeMap is Tuple `(-1,)`. If we change this
                //       structure in the future, then the logic below
                //       must be rewritten.
                //
                // Safety: Safe to unwrap here because we are checking
                //         `len == 0` above.
                let (k, v) = kv_btree.pop_first().unwrap();

                let record_header_key = Key::from(
                    {
                        let tup: (i8,) = (-1,);

                        let mut t = Tuple::new();
                        t.push_back::<i8>(tup.0);
                        t
                    }
                    .pack(),
                );

                if record_header_key == k {
                    // Currently we have only one version of record header.
                    let record_header_v0 = RecordHeaderV0::try_from(v)?;

                    let RecordHeaderV0 {
                        data_splits,
                        incarnation,
                        versionstamp,
                        ..
                    } = record_header_v0;

                    // Ensure that data splits mentioned in the record
                    // header is consistent with the number of key
                    // values in record data.
                    if data_splits
                        != kv_btree.len().try_into().map_err(|_| {
                            FdbError::new(SPLIT_HELPER_LOAD_INVALID_SERIALIZED_BYTES)
                        })?
                    {
                        return Err(FdbError::new(SPLIT_HELPER_LOAD_INVALID_RECORD_HEADER));
                    }

                    let record_version = match incarnation {
                        Some(incarnation_version) => {
                            RecordVersion::from((incarnation_version, versionstamp))
                        }
                        None => RecordVersion::from(versionstamp),
                    };

                    let serialized_bytes =
                        match (0..kv_btree.len()).try_fold(BytesMut::new(), |mut acc, x| {
                            let key = Key::from(
                                {
                                    let tup: (BigInt,) = (x.into(),);

                                    let mut t = Tuple::new();
                                    t.push_back::<BigInt>(tup.0);
                                    t
                                }
                                .pack(),
                            );

                            match kv_btree.remove(&key) {
                                Some(value) => ControlFlow::Continue({
                                    acc.put(Bytes::from(value));
                                    acc
                                }),
                                None => ControlFlow::Break(FdbError::new(
                                    SPLIT_HELPER_LOAD_INVALID_SERIALIZED_BYTES,
                                )),
                            }
                        }) {
                            ControlFlow::Continue(bytes_mut) => Ok(Bytes::from(bytes_mut)),
                            ControlFlow::Break(err) => Err(err),
                        }?;

                    Ok(Some((record_version, serialized_bytes)))
                } else {
                    // There is no `(Subspace, Primary Key Tuple, -1)`
                    // key *and* the range is not empty. Therefore it
                    // is an error.
                    Err(FdbError::new(SPLIT_HELPER_INVALID_PRIMARY_KEY))
                }
            }
        }
        CursorError::NoNextReason(_) => Err(FdbError::new(SPLIT_HELPER_SCAN_LIMIT_REACHED)),
        CursorError::FdbError(err, _) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    mod record_header_v0 {
        use bytes::Bytes;

        use num_bigint::BigInt;

        use fdb::error::FdbError;
        use fdb::tuple::{Null, Tuple, Versionstamp};
        use fdb::Value;

        use std::convert::TryFrom;

        use super::super::{error::SPLIT_HELPER_LOAD_INVALID_RECORD_HEADER, RecordHeaderV0};

        #[test]
        fn try_from_value_try_from() {
            // invalid header
            {
                // `header_version` must be `0`.
                {
                    let value = Value::from(
                        {
                            let header_version = 1;
                            let data_splits = 1;
                            let incarnation = None;
                            let versionstamp = Versionstamp::complete(
                                Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                                0,
                            );

                            let tup: (i8, i8, Option<u64>, Versionstamp) =
                                (header_version, data_splits, incarnation, versionstamp);

                            let mut t = Tuple::new();

                            // header_version
                            t.push_back::<i8>(tup.0);

                            // data_splits
                            t.push_back::<i8>(tup.1);

                            // incarnation
                            if let Some(i) = tup.2 {
                                t.push_back::<BigInt>(i.into());
                            } else {
                                t.push_back::<Null>(Null);
                            }

                            // versionstamp
                            t.push_back::<Versionstamp>(tup.3);

                            t
                        }
                        .pack(),
                    );

                    assert_eq!(
                        RecordHeaderV0::try_from(value),
                        Err(FdbError::new(SPLIT_HELPER_LOAD_INVALID_RECORD_HEADER))
                    );
                }
                // `data_splits` must be between 1 and 100
                {
                    let value = Value::from(
                        {
                            let header_version = 0;
                            let data_splits = 0;
                            let incarnation = None;
                            let versionstamp = Versionstamp::complete(
                                Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                                0,
                            );

                            let tup: (i8, i8, Option<u64>, Versionstamp) =
                                (header_version, data_splits, incarnation, versionstamp);

                            let mut t = Tuple::new();

                            // header_version
                            t.push_back::<i8>(tup.0);

                            // data_splits
                            t.push_back::<i8>(tup.1);

                            // incarnation
                            if let Some(i) = tup.2 {
                                t.push_back::<BigInt>(i.into());
                            } else {
                                t.push_back::<Null>(Null);
                            }

                            // versionstamp
                            t.push_back::<Versionstamp>(tup.3);

                            t
                        }
                        .pack(),
                    );

                    assert_eq!(
                        RecordHeaderV0::try_from(value),
                        Err(FdbError::new(SPLIT_HELPER_LOAD_INVALID_RECORD_HEADER))
                    );
                }
                // `incarnation` must be an `Option<i64>` and not bool.
                {
                    let value = Value::from(
                        {
                            let header_version = 0;
                            // Must be between 1 and 100
                            let data_splits = 1;
                            let incarnation = false;
                            let versionstamp = Versionstamp::complete(
                                Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                                0,
                            );

                            let tup: (i8, i8, bool, Versionstamp) =
                                (header_version, data_splits, incarnation, versionstamp);

                            let mut t = Tuple::new();

                            // header_version
                            t.push_back::<i8>(tup.0);

                            // data_splits
                            t.push_back::<i8>(tup.1);

                            // incarnation
                            t.push_back::<bool>(true);

                            // versionstamp
                            t.push_back::<Versionstamp>(tup.3);

                            t
                        }
                        .pack(),
                    );

                    assert_eq!(
                        RecordHeaderV0::try_from(value),
                        Err(FdbError::new(SPLIT_HELPER_LOAD_INVALID_RECORD_HEADER))
                    );
                }
                // `versionstamp` cannot be of `Null` type.
                {
                    let value = Value::from(
                        {
                            let header_version = 0;
                            let data_splits = 1;
                            let incarnation = None;
                            let versionstamp = Null;

                            let tup: (i8, i8, Option<u64>, Null) =
                                (header_version, data_splits, incarnation, versionstamp);

                            let mut t = Tuple::new();

                            // header_version
                            t.push_back::<i8>(tup.0);

                            // data_splits
                            t.push_back::<i8>(tup.1);

                            // incarnation
                            if let Some(i) = tup.2 {
                                t.push_back::<BigInt>(i.into());
                            } else {
                                t.push_back::<Null>(Null);
                            }

                            // versionstamp
                            t.push_back::<Null>(tup.3);

                            t
                        }
                        .pack(),
                    );

                    assert_eq!(
                        RecordHeaderV0::try_from(value),
                        Err(FdbError::new(SPLIT_HELPER_LOAD_INVALID_RECORD_HEADER))
                    );
                }
            }
            // valid header.
            {
                let value = Value::from(
                    {
                        let header_version = 0;
                        let data_splits = 1;
                        let incarnation = None;
                        let versionstamp = Versionstamp::complete(
                            Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                            0,
                        );

                        let tup: (i8, i8, Option<u64>, Versionstamp) =
                            (header_version, data_splits, incarnation, versionstamp);

                        let mut t = Tuple::new();

                        // header_version
                        t.push_back::<i8>(tup.0);

                        // data_splits
                        t.push_back::<i8>(tup.1);

                        // incarnation
                        if let Some(i) = tup.2 {
                            t.push_back::<BigInt>(i.into());
                        } else {
                            t.push_back::<Null>(Null);
                        }

                        // versionstamp
                        t.push_back::<Versionstamp>(tup.3);

                        t
                    }
                    .pack(),
                );

                assert_eq!(
                    RecordHeaderV0::try_from(value),
                    Ok(RecordHeaderV0::new(
                        1,
                        None,
                        Versionstamp::complete(
                            Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                            0,
                        )
                    ))
                );
            }
        }
    }
}
