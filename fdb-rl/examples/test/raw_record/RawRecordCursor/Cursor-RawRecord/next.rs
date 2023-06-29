#![allow(non_snake_case)]
#![feature(lazy_cell)]

use bytes::{BufMut, Bytes, BytesMut};

use fdb::database::FdbDatabase;
use fdb::error::{FdbError, FdbResult};
use fdb::range::Range;
use fdb::subspace::Subspace;
use fdb::transaction::Transaction;
use fdb::tuple::{Tuple, TupleSchema, TupleSchemaElement, Versionstamp};
use fdb::{Key, Value};

use fdb_rl::cursor::{Cursor, CursorError, NoNextReason};
use fdb_rl::scan::{KeyValueScanLimiter, ScanLimiter};
use fdb_rl::test::raw_record::{
    raw_record_cursor_builder_build, RawRecord, RawRecordPrimaryKey, RawRecordPrimaryKeySchema,
    RAW_RECORD_CURSOR_BUILDER_ERROR, RAW_RECORD_CURSOR_NEXT_ERROR,
};
use fdb_rl::test::split_helper;
use fdb_rl::RecordVersion;

use libtest_mimic::{Arguments, Failed, Trial};

use tokio::runtime::Builder;

use std::convert::TryFrom;
use std::env;
use std::error::Error;
use std::ops::Deref;
use std::sync::LazyLock;

static mut FDB_DATABASE: Option<FdbDatabase> = None;

// From the traditional nursery rhyme
const HUMPTY_DUMPTY: &str = concat!(
    "Humpty Dumpty sat on a wall,\n",
    "Humpty Dumpty had a great fall\n",
    "All the king's horses and all the king's men\n",
    "Couldn't put Humpty Dumpty together again.\n",
);

// `SHORT_STRING`, `MEDIUM_STRING`, `LONG_STRING` and
// `VERY_LONG_STRING` is taken from Java recordLayer's
// `SplitHelperTest.java`.

static SHORT_STRING: LazyLock<Bytes> =
    LazyLock::new(|| Bytes::from_static(HUMPTY_DUMPTY.as_bytes()));

static MEDIUM_STRING: LazyLock<Bytes> = LazyLock::new(|| {
    let mut x = BytesMut::new();

    (0..5).for_each(|_| x.put(HUMPTY_DUMPTY.as_bytes()));

    x.into()
});

static LONG_STRING: LazyLock<Bytes> = LazyLock::new(|| {
    let mut x = BytesMut::new();

    // requires 1 split
    (0..1_000).for_each(|_| x.put(HUMPTY_DUMPTY.as_bytes()));

    x.into()
});

static VERY_LONG_STRING: LazyLock<Bytes> = LazyLock::new(|| {
    let mut x = BytesMut::new();

    // requires 2 split
    (0..2_000).for_each(|_| x.put(HUMPTY_DUMPTY.as_bytes()));

    x.into()
});

fn main() -> Result<(), Box<dyn Error>> {
    let args = Arguments::from_args();

    // By default `libthread_mimic` uses a threadpool to run the
    // tests. We are not running the tests in a threadpool.
    //
    // In the main thread we are creating a Tokio current_thread
    // runtime to run the test. This allows us to see the output of
    // the tests in the order that is in the source code.
    //
    // If you want to use a threadpool, then comment the following
    // line.
    let args = {
        let mut args = args;
        args.test_threads = Some(1);
        args
    };

    let fdb_cluster_file = env::var("FDB_CLUSTER_FILE").expect("FDB_CLUSTER_FILE not defined!");

    unsafe {
        fdb::select_api_version(fdb::FDB_API_VERSION as i32);
        fdb::start_network();
    }

    let fdb_database = fdb::open_database(fdb_cluster_file)?;

    unsafe {
        FDB_DATABASE = Some(fdb_database);
    }

    setup_no_subspace_normal()?;

    let tests = vec![
        Trial::test(
            "cursor_no_subspace_normal_all_forward",
            cursor_no_subspace_normal_all_forward,
        ),
        Trial::test(
            "cursor_no_subspace_normal_all_reverse",
            cursor_no_subspace_normal_all_reverse,
        ),
    ];

    let _ = libtest_mimic::run(&args, tests);

    setup_subspace_normal()?;

    let tests = vec![
        Trial::test(
            "cursor_subspace_normal_all_forward",
            cursor_subspace_normal_all_forward,
        ),
        Trial::test(
            "cursor_subspace_normal_all_reverse",
            cursor_subspace_normal_all_reverse,
        ),
        Trial::test(
            "cursor_subspace_normal_empty_forward",
            cursor_subspace_normal_empty_forward,
        ),
        Trial::test(
            "cursor_subspace_normal_empty_reverse",
            cursor_subspace_normal_empty_reverse,
        ),
        Trial::test(
            "cursor_subspace_normal_in_band_limit_forward",
            cursor_subspace_normal_in_band_limit_forward,
        ),
        Trial::test(
            "cursor_subspace_normal_in_band_limit_reverse",
            cursor_subspace_normal_in_band_limit_reverse,
        ),
        Trial::test(
            "cursor_subspace_normal_out_of_band_limit_forward",
            cursor_subspace_normal_out_of_band_limit_forward,
        ),
        Trial::test(
            "cursor_subspace_normal_out_of_band_limit_reverse",
            cursor_subspace_normal_out_of_band_limit_reverse,
        ),
    ];

    let _ = libtest_mimic::run(&args, tests);

    setup_subspace_abnormal()?;

    let tests = vec![
        Trial::test(
            "cursor_subspace_abnormal_forward_state_initiate_record_version_read",
            cursor_subspace_abnormal_forward_state_initiate_record_version_read,
        ),
        Trial::test(
            "cursor_subspace_abnormal_reverse_state_initiate_last_split_read",
            cursor_subspace_abnormal_reverse_state_initiate_last_split_read,
        ),
        Trial::test(
            "cursor_subspace_abnormal_forward_state_read_record_version",
            cursor_subspace_abnormal_forward_state_read_record_version,
        ),
    ];

    let _ = libtest_mimic::run(&args, tests);

    let fdb_database = unsafe { FDB_DATABASE.take().unwrap() };

    drop(fdb_database);

    unsafe {
        fdb::stop_network();
    }

    Ok(())
}

async fn save_raw_record<Tr>(
    tr: &Tr,
    maybe_subspace: &Option<Subspace>,
    raw_record: RawRecord,
) -> FdbResult<()>
where
    Tr: Transaction,
{
    let (raw_record_primary_key, record_version, record_bytes) = raw_record.into_parts();
    let (_, primary_key_tuple) = raw_record_primary_key.into_parts();

    split_helper::save(
        tr,
        &None,
        maybe_subspace,
        &primary_key_tuple,
        record_bytes,
        record_version,
    )
    .await
}

fn setup_no_subspace_normal() -> Result<(), Box<dyn Error>> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .run(|tr| async move {
                    // Clear the database.
                    tr.clear_range(Range::new(Bytes::new(), Bytes::from_static(b"\xFF")));

                    let maybe_subspace = &None;

                    for (pk, local_version, record_bytes) in [
                        // This is the lexicographic order.
                        ("long", 0, LONG_STRING.deref().clone()),
                        ("medium", 1, MEDIUM_STRING.deref().clone()),
                        ("short", 2, SHORT_STRING.deref().clone()),
                        ("very_long", 3, VERY_LONG_STRING.deref().clone()),
                    ] {
                        let version = RecordVersion::from(Versionstamp::complete(
                            Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                            local_version,
                        ));

                        let primary_key = RawRecordPrimaryKey::try_from((
                            RawRecordPrimaryKeySchema::try_from({
                                let mut tuple_schema = TupleSchema::new();
                                tuple_schema.push_front(TupleSchemaElement::String);
                                tuple_schema
                            })?,
                            {
                                let tup: (&str,) = (pk,);

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t
                            },
                        ))?;

                        let raw_record = RawRecord::from((primary_key, version, record_bytes));
                        save_raw_record(&tr, maybe_subspace, raw_record).await?;
                    }

                    Ok(())
                })
                .await?;
            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn setup_subspace_normal() -> Result<(), Box<dyn Error>> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .run(|tr| async move {
                    // Clear the database.
                    tr.clear_range(Range::new(Bytes::new(), Bytes::from_static(b"\xFF")));

                    let subspace = Subspace::new(Bytes::new()).subspace(&{
                        let tup: (&str, &str, &str) = ("sub", "space", "normal");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<String>(tup.2.to_string());
                        t
                    });

                    let maybe_subspace = &Some(subspace);

                    for (pk, local_version, record_bytes) in [
                        // This is the lexicographic order.
                        ("long", 0, LONG_STRING.deref().clone()),
                        ("medium", 1, MEDIUM_STRING.deref().clone()),
                        ("short", 2, SHORT_STRING.deref().clone()),
                        ("very_long", 3, VERY_LONG_STRING.deref().clone()),
                    ] {
                        let version = RecordVersion::from(Versionstamp::complete(
                            Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                            local_version,
                        ));

                        let primary_key = RawRecordPrimaryKey::try_from((
                            RawRecordPrimaryKeySchema::try_from({
                                let mut tuple_schema = TupleSchema::new();
                                tuple_schema.push_front(TupleSchemaElement::String);
                                tuple_schema
                            })?,
                            {
                                let tup: (&str,) = (pk,);

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t
                            },
                        ))?;

                        let raw_record = RawRecord::from((primary_key, version, record_bytes));
                        save_raw_record(&tr, maybe_subspace, raw_record).await?;
                    }

                    Ok(())
                })
                .await?;
            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn setup_subspace_abnormal() -> Result<(), Box<dyn Error>> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .run(|tr| async move {
                    // Clear the database.
                    tr.clear_range(Range::new(Bytes::new(), Bytes::from_static(b"\xFF")));

                    // For
                    // `cursor_subspace_abnormal_forward_state_initiate_record_version_read`
                    // test.
                    {
                        // `("sub", "space", "abnormal", "01", "long",
                        // "non_number")`.
                        {
                            let full_primary_key_tuple = {
                                let tup: (&str, &str, &str, &str, &str) =
                                    ("sub", "space", "abnormal", "01", "long");

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t.push_back::<String>(tup.1.to_string());
                                t.push_back::<String>(tup.2.to_string());
                                t.push_back::<String>(tup.3.to_string());
                                t.push_back::<String>(tup.4.to_string());
                                t
                            };

                            let key = Key::from(
                                {
                                    let tup: (&str,) = ("non_number",);

                                    let mut t = full_primary_key_tuple.clone();
                                    t.push_back::<String>(tup.0.to_string());
                                    t
                                }
                                .pack(),
                            );

                            let value = Value::from(Bytes::from_static(b"invalid record header"));

                            tr.set(key, value);
                        }

                        // `("sub", "space", "abnormal", "02", "long", 0)`
                        //
                        // `("sub", "space", "abnormal", "02", "long",
                        // -1)` key is missing.
                        {
                            let full_primary_key_tuple = {
                                let tup: (&str, &str, &str, &str, &str) =
                                    ("sub", "space", "abnormal", "02", "long");

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t.push_back::<String>(tup.1.to_string());
                                t.push_back::<String>(tup.2.to_string());
                                t.push_back::<String>(tup.3.to_string());
                                t.push_back::<String>(tup.4.to_string());
                                t
                            };

                            let key = Key::from(
                                {
                                    let tup: (i8,) = (0,);

                                    let mut t = full_primary_key_tuple.clone();
                                    t.push_back::<i8>(tup.0);
                                    t
                                }
                                .pack(),
                            );

                            let value = Value::from(Bytes::from_static(b"valid record content"));

                            tr.set(key, value);
                        }
                    }

                    // For
                    // `cursor_subspace_abnormal_reverse_state_initiate_last_split_read`
                    // test.
                    {
                        // `("sub", "space", "abnormal", "03", "long", "non_number")`
                        {
                            let full_primary_key_tuple = {
                                let tup: (&str, &str, &str, &str, &str) =
                                    ("sub", "space", "abnormal", "03", "long");

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t.push_back::<String>(tup.1.to_string());
                                t.push_back::<String>(tup.2.to_string());
                                t.push_back::<String>(tup.3.to_string());
                                t.push_back::<String>(tup.4.to_string());
                                t
                            };

                            let key = Key::from(
                                {
                                    let tup: (&str,) = ("non_number",);

                                    let mut t = full_primary_key_tuple.clone();
                                    t.push_back::<String>(tup.0.to_string());
                                    t
                                }
                                .pack(),
                            );

                            let value = Value::from(Bytes::from_static(b"invalid record content"));

                            tr.set(key, value);
                        }

                        // `("sub", "space", "abnormal", "04", "long", -1)`
                        //
                        // `("sub", "space", "abnormal", "04", "long", X)`
                        //
                        // `X` must be greater than or equal to 0, but
                        // we are introducing `-1`, which is actually
                        // the record header. When doing reverse scan,
                        // we must first see a key-value pair for
                        // record data, before record header.
                        {
                            let full_primary_key_tuple = {
                                let tup: (&str, &str, &str, &str, &str) =
                                    ("sub", "space", "abnormal", "04", "long");

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t.push_back::<String>(tup.1.to_string());
                                t.push_back::<String>(tup.2.to_string());
                                t.push_back::<String>(tup.3.to_string());
                                t.push_back::<String>(tup.4.to_string());
                                t
                            };

                            let key = Key::from(
                                {
                                    let tup: (i8,) = (-1,);

                                    let mut t = full_primary_key_tuple.clone();
                                    t.push_back::<i8>(tup.0);
                                    t
                                }
                                .pack(),
                            );

                            let value = Value::from(Bytes::from_static(b"invalid record content"));

                            tr.set(key, value);
                        }
                    }

                    // For
                    // `cursor_subspace_abnormal_forward_state_read_record_version`
                    // test.
                    {
                        // Create a record with a missing key-value
                        //
                        // ```
                        // ("sub", "space", "abnormal", "05", "long", -1)
                        // ("sub", "space", "abnormal", "05", "long",  0) missing
                        // ("sub", "space", "abnormal", "05", "long",  1)
                        // ```
                        {
                            let subspace = Subspace::new(Bytes::new()).subspace(&{
                                let tup: (&str, &str, &str, &str) =
                                    ("sub", "space", "abnormal", "05");

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t.push_back::<String>(tup.1.to_string());
                                t.push_back::<String>(tup.2.to_string());
                                t.push_back::<String>(tup.3.to_string());
                                t
                            });

                            let maybe_subspace = &Some(subspace);

                            let (pk, local_version, record_bytes) =
                                ("long", 0, LONG_STRING.deref().clone());

                            let version = RecordVersion::from(Versionstamp::complete(
                                Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                                local_version,
                            ));

                            let primary_key = RawRecordPrimaryKey::try_from((
                                RawRecordPrimaryKeySchema::try_from({
                                    let mut tuple_schema = TupleSchema::new();
                                    tuple_schema.push_front(TupleSchemaElement::String);
                                    tuple_schema
                                })?,
                                {
                                    let tup: (&str,) = (pk,);

                                    let mut t = Tuple::new();
                                    t.push_back::<String>(tup.0.to_string());
                                    t
                                },
                            ))?;

                            let raw_record = RawRecord::from((primary_key, version, record_bytes));
                            save_raw_record(&tr, maybe_subspace, raw_record).await?;

                            // Clear the key `("sub", "space",
                            // "abnormal", "05", "long", 0)`.
                            let full_primary_key_tuple = {
                                let tup: (&str, &str, &str, &str, &str) =
                                    ("sub", "space", "abnormal", "05", "long");

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t.push_back::<String>(tup.1.to_string());
                                t.push_back::<String>(tup.2.to_string());
                                t.push_back::<String>(tup.3.to_string());
                                t.push_back::<String>(tup.4.to_string());
                                t
                            };

                            let key = Key::from(
                                {
                                    let tup: (i8,) = (0,);

                                    let mut t = full_primary_key_tuple.clone();
                                    t.push_back::<i8>(tup.0);
                                    t
                                }
                                .pack(),
                            );

                            tr.clear(key);
                        }

                        // Record with a correct sequence but invalid
                        // primary key.
                        //
                        // ```
                        // ("sub", "space", "abnormal", "06", "medium", -1)
                        // ("sub", "space", "abnormal", "06", "medium",  0) missing
                        // ("sub", "space", "abnormal", "06", "short",  -1) missing
                        // ("sub", "space", "abnormal", "06", "short",   0)
                        // ```
                        {
                            let subspace = Subspace::new(Bytes::new()).subspace(&{
                                let tup: (&str, &str, &str, &str) =
                                    ("sub", "space", "abnormal", "06");

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t.push_back::<String>(tup.1.to_string());
                                t.push_back::<String>(tup.2.to_string());
                                t.push_back::<String>(tup.3.to_string());
                                t
                            });

                            let maybe_subspace = &Some(subspace);

                            for (pk, local_version, record_bytes) in [
                                // This is the lexicographic order.
                                ("medium", 1, MEDIUM_STRING.deref().clone()),
                                ("short", 2, SHORT_STRING.deref().clone()),
                            ] {
                                let version = RecordVersion::from(Versionstamp::complete(
                                    Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                                    local_version,
                                ));

                                let primary_key = RawRecordPrimaryKey::try_from((
                                    RawRecordPrimaryKeySchema::try_from({
                                        let mut tuple_schema = TupleSchema::new();
                                        tuple_schema.push_front(TupleSchemaElement::String);
                                        tuple_schema
                                    })?,
                                    {
                                        let tup: (&str,) = (pk,);

                                        let mut t = Tuple::new();
                                        t.push_back::<String>(tup.0.to_string());
                                        t
                                    },
                                ))?;

                                let raw_record =
                                    RawRecord::from((primary_key, version, record_bytes));
                                save_raw_record(&tr, maybe_subspace, raw_record).await?;

                                // Clear the key `("sub", "space",
                                // "abnormal", "06", "medium", 0)`.
                                tr.clear(Key::from(
                                    {
                                        let tup: (&str, &str, &str, &str, &str, i8) =
                                            ("sub", "space", "abnormal", "06", "medium", 0);

                                        let mut t = Tuple::new();
                                        t.push_back::<String>(tup.0.to_string());
                                        t.push_back::<String>(tup.1.to_string());
                                        t.push_back::<String>(tup.2.to_string());
                                        t.push_back::<String>(tup.3.to_string());
                                        t.push_back::<String>(tup.4.to_string());
                                        t.push_back::<i8>(tup.5);
                                        t
                                    }
                                    .pack(),
                                ));

                                // Clear the key `("sub", "space",
                                // "abnormal", "06", "short", -1)`.
                                tr.clear(Key::from(
                                    {
                                        let tup: (&str, &str, &str, &str, &str, i8) =
                                            ("sub", "space", "abnormal", "06", "short", -1);

                                        let mut t = Tuple::new();
                                        t.push_back::<String>(tup.0.to_string());
                                        t.push_back::<String>(tup.1.to_string());
                                        t.push_back::<String>(tup.2.to_string());
                                        t.push_back::<String>(tup.3.to_string());
                                        t.push_back::<String>(tup.4.to_string());
                                        t.push_back::<i8>(tup.5);
                                        t
                                    }
                                    .pack(),
                                ));
                            }
                        }
                    }

                    // TODO continue from here.

                    Ok(())
                })
                .await?;
            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

// Tests

fn cursor_no_subspace_normal_all_forward() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    let mut raw_record_cursor = {
                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            &tr,
                        )?
                    };

                    for (pk, local_version, record_bytes) in [
                        // This is the lexicographic order.
                        ("long", 0, LONG_STRING.deref().clone()),
                        ("medium", 1, MEDIUM_STRING.deref().clone()),
                        ("short", 2, SHORT_STRING.deref().clone()),
                        ("very_long", 3, VERY_LONG_STRING.deref().clone()),
                    ] {
                        let version = RecordVersion::from(Versionstamp::complete(
                            Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                            local_version,
                        ));

                        let primary_key = RawRecordPrimaryKey::try_from((
                            RawRecordPrimaryKeySchema::try_from({
                                let mut tuple_schema = TupleSchema::new();
                                tuple_schema.push_front(TupleSchemaElement::String);
                                tuple_schema
                            })?,
                            {
                                let tup: (&str,) = (pk,);

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t
                            },
                        ))?;

                        let raw_record = RawRecord::from((primary_key, version, record_bytes));

                        // We expect `CursorSuccess<RawRecord>`.
                        let (res, continuation) =
                            raw_record_cursor.next().await.unwrap().into_parts();

                        assert_eq!(raw_record, res);

                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
                    }

                    let res = raw_record_cursor.next().await;

                    assert!(matches!(
                        res,
                        Err(CursorError::NoNextReason(NoNextReason::SourceExhausted(_)))
                    ));

                    if let Err(CursorError::NoNextReason(NoNextReason::SourceExhausted(
                        continuation,
                    ))) = res
                    {
                        assert!(continuation.is_end_marker());
                    }

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_no_subspace_normal_all_reverse() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    let mut raw_record_cursor = {
                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        let reverse = true;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            None,
                            None,
                            None,
                            None,
                            Some(reverse),
                            None,
                            &tr,
                        )?
                    };

                    for (pk, local_version, record_bytes) in [
                        // This is the lexicographic order.
                        ("long", 0, LONG_STRING.deref().clone()),
                        ("medium", 1, MEDIUM_STRING.deref().clone()),
                        ("short", 2, SHORT_STRING.deref().clone()),
                        ("very_long", 3, VERY_LONG_STRING.deref().clone()),
                    ]
                    .iter()
                    .rev()
                    {
                        let version = RecordVersion::from(Versionstamp::complete(
                            Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                            *local_version,
                        ));

                        let primary_key = RawRecordPrimaryKey::try_from((
                            RawRecordPrimaryKeySchema::try_from({
                                let mut tuple_schema = TupleSchema::new();
                                tuple_schema.push_front(TupleSchemaElement::String);
                                tuple_schema
                            })?,
                            {
                                let tup: (&str,) = (pk,);

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t
                            },
                        ))?;

                        let raw_record =
                            RawRecord::from((primary_key, version, record_bytes.clone()));

                        // We expect `CursorSuccess<RawRecord>`.
                        let (res, continuation) =
                            raw_record_cursor.next().await.unwrap().into_parts();

                        assert_eq!(raw_record, res);

                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
                    }

                    let res = raw_record_cursor.next().await;

                    assert!(matches!(
                        res,
                        Err(CursorError::NoNextReason(NoNextReason::SourceExhausted(_)))
                    ));

                    if let Err(CursorError::NoNextReason(NoNextReason::SourceExhausted(
                        continuation,
                    ))) = res
                    {
                        assert!(continuation.is_end_marker());
                    }

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_subspace_normal_all_forward() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    let mut raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            None,
                            None,
                            None,
                            None,
                            None,
                            &tr,
                        )?
                    };

                    for (pk, local_version, record_bytes) in [
                        // This is the lexicographic order.
                        ("long", 0, LONG_STRING.deref().clone()),
                        ("medium", 1, MEDIUM_STRING.deref().clone()),
                        ("short", 2, SHORT_STRING.deref().clone()),
                        ("very_long", 3, VERY_LONG_STRING.deref().clone()),
                    ] {
                        let version = RecordVersion::from(Versionstamp::complete(
                            Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                            local_version,
                        ));

                        let primary_key = RawRecordPrimaryKey::try_from((
                            RawRecordPrimaryKeySchema::try_from({
                                let mut tuple_schema = TupleSchema::new();
                                tuple_schema.push_front(TupleSchemaElement::String);
                                tuple_schema
                            })?,
                            {
                                let tup: (&str,) = (pk,);

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t
                            },
                        ))?;

                        let raw_record = RawRecord::from((primary_key, version, record_bytes));

                        // We expect `CursorSuccess<RawRecord>`.
                        let (res, continuation) =
                            raw_record_cursor.next().await.unwrap().into_parts();

                        assert_eq!(raw_record, res);

                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
                    }

                    let res = raw_record_cursor.next().await;

                    assert!(matches!(
                        res,
                        Err(CursorError::NoNextReason(NoNextReason::SourceExhausted(_)))
                    ));

                    if let Err(CursorError::NoNextReason(NoNextReason::SourceExhausted(
                        continuation,
                    ))) = res
                    {
                        assert!(continuation.is_end_marker());
                    }

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_subspace_normal_all_reverse() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    let mut raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        let reverse = true;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            None,
                            None,
                            None,
                            Some(reverse),
                            None,
                            &tr,
                        )?
                    };

                    for (pk, local_version, record_bytes) in [
                        // This is the lexicographic order.
                        ("long", 0, LONG_STRING.deref().clone()),
                        ("medium", 1, MEDIUM_STRING.deref().clone()),
                        ("short", 2, SHORT_STRING.deref().clone()),
                        ("very_long", 3, VERY_LONG_STRING.deref().clone()),
                    ]
                    .iter()
                    .rev()
                    {
                        let version = RecordVersion::from(Versionstamp::complete(
                            Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                            *local_version,
                        ));

                        let primary_key = RawRecordPrimaryKey::try_from((
                            RawRecordPrimaryKeySchema::try_from({
                                let mut tuple_schema = TupleSchema::new();
                                tuple_schema.push_front(TupleSchemaElement::String);
                                tuple_schema
                            })?,
                            {
                                let tup: (&str,) = (pk,);

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t
                            },
                        ))?;

                        let raw_record =
                            RawRecord::from((primary_key, version, record_bytes.clone()));

                        // We expect `CursorSuccess<RawRecord>`.
                        let (res, continuation) =
                            raw_record_cursor.next().await.unwrap().into_parts();

                        assert_eq!(raw_record, res);

                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
                    }

                    let res = raw_record_cursor.next().await;

                    assert!(matches!(
                        res,
                        Err(CursorError::NoNextReason(NoNextReason::SourceExhausted(_)))
                    ));

                    if let Err(CursorError::NoNextReason(NoNextReason::SourceExhausted(
                        continuation,
                    ))) = res
                    {
                        assert!(continuation.is_end_marker());
                    }

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_subspace_normal_empty_forward() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    let mut raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal_empty");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            None,
                            None,
                            None,
                            None,
                            None,
                            &tr,
                        )?
                    };

                    let res = raw_record_cursor.next().await;

                    assert!(matches!(
                        res,
                        Err(CursorError::NoNextReason(NoNextReason::SourceExhausted(_)))
                    ));

                    if let Err(CursorError::NoNextReason(NoNextReason::SourceExhausted(
                        continuation,
                    ))) = res
                    {
                        assert!(continuation.is_end_marker());
                    }

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_subspace_normal_empty_reverse() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    let mut raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal_empty");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        let reverse = true;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            None,
                            None,
                            None,
                            Some(reverse),
                            None,
                            &tr,
                        )?
                    };

                    let res = raw_record_cursor.next().await;

                    assert!(matches!(
                        res,
                        Err(CursorError::NoNextReason(NoNextReason::SourceExhausted(_)))
                    ));

                    if let Err(CursorError::NoNextReason(NoNextReason::SourceExhausted(
                        continuation,
                    ))) = res
                    {
                        assert!(continuation.is_end_marker());
                    }

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_subspace_normal_in_band_limit_forward() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    let raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            None,
                            None,
                            Some(0),
                            None,
                            None,
                            &tr,
                        )
                    };

                    assert!(matches!(raw_record_cursor, Err(_),));

                    if let Err(err) = raw_record_cursor {
                        assert_eq!(err, FdbError::new(RAW_RECORD_CURSOR_BUILDER_ERROR));
                    }

                    let mut raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            None,
                            None,
                            Some(1),
                            None,
                            None,
                            &tr,
                        )?
                    };

                    // We expect `CursorSuccess<RawRecord>`.
                    let (res, continuation) = raw_record_cursor.next().await.unwrap().into_parts();
                    let raw_record = {
                        let (pk, local_version, record_bytes) =
 ("long", 0, LONG_STRING.deref().clone());

                        let version = RecordVersion::from(Versionstamp::complete(
                            Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                            local_version,
                        ));

                        let primary_key = RawRecordPrimaryKey::try_from((
                            RawRecordPrimaryKeySchema::try_from({
                                let mut tuple_schema = TupleSchema::new();
                                tuple_schema.push_front(TupleSchemaElement::String);
                                tuple_schema
                            })?,
                            {
                                let tup: (&str,) = (pk,);

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t
                            },
                        ))?;

                        RawRecord::from((primary_key, version, record_bytes))
                    };

                    assert_eq!(raw_record, res);

                    assert!(!continuation.is_begin_marker());
                    assert!(!continuation.is_end_marker());

                    let continuation_prev = continuation;

                    // We expect `CursorError`.
                    let res = raw_record_cursor.next().await.unwrap_err();

                    assert!(matches!(
                        res,
                        CursorError::NoNextReason(NoNextReason::ReturnLimitReached(_))
                    ));

                    let continuation = if let CursorError::NoNextReason(NoNextReason::ReturnLimitReached(
                        continuation,
                    )) = res
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
			continuation
                    } else {
			panic!("CursorError::NoNextReason(NoNextReason::ReturnLimitReached(_)) expected");
		    };

		    assert_eq!(continuation_prev.to_bytes(), continuation.to_bytes());

                    let mut raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            None,
                            None,
                            Some(2),
                            None,
			    // We expect `Ok(bytes)`.
                            Some(continuation.to_bytes().unwrap()),
                            &tr,
                        )?
                    };

                    for (pk, local_version, record_bytes) in [
                        // This is the lexicographic order.
                        ("medium", 1, MEDIUM_STRING.deref().clone()),
                        ("short", 2, SHORT_STRING.deref().clone()),
                    ] {
                        let version = RecordVersion::from(Versionstamp::complete(
                            Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                            local_version,
                        ));

                        let primary_key = RawRecordPrimaryKey::try_from((
                            RawRecordPrimaryKeySchema::try_from({
                                let mut tuple_schema = TupleSchema::new();
                                tuple_schema.push_front(TupleSchemaElement::String);
                                tuple_schema
                            })?,
                            {
                                let tup: (&str,) = (pk,);

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t
                            },
                        ))?;

                        let raw_record = RawRecord::from((primary_key, version, record_bytes));

                        // We expect `CursorSuccess<RawRecord>`.
                        let (res, continuation) =
                            raw_record_cursor.next().await.unwrap().into_parts();

                        assert_eq!(raw_record, res);

                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
                    }

                    // We expect `CursorError`.
                    let res = raw_record_cursor.next().await.unwrap_err();

                    assert!(matches!(
                        res,
                        CursorError::NoNextReason(NoNextReason::ReturnLimitReached(_))
                    ));

                    let continuation = if let CursorError::NoNextReason(NoNextReason::ReturnLimitReached(
                        continuation,
                    )) = res
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
			continuation
                    } else {
			panic!("CursorError::NoNextReason(NoNextReason::ReturnLimitReached(_)) expected");
		    };

                    let mut raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            None,
                            None,
                            Some(1),
                            None,
			    // We expect `Ok(bytes)`.
                            Some(continuation.to_bytes().unwrap()),
                            &tr,
                        )?
                    };

                    // We expect `CursorSuccess<RawRecord>`.
                    let (res, continuation) = raw_record_cursor.next().await.unwrap().into_parts();

                    let raw_record = {
                        let (pk, local_version, record_bytes) = ("very_long", 3, VERY_LONG_STRING.deref().clone());

                        let version = RecordVersion::from(Versionstamp::complete(
                            Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                            local_version,
                        ));

                        let primary_key = RawRecordPrimaryKey::try_from((
                            RawRecordPrimaryKeySchema::try_from({
                                let mut tuple_schema = TupleSchema::new();
                                tuple_schema.push_front(TupleSchemaElement::String);
                                tuple_schema
                            })?,
                            {
                                let tup: (&str,) = (pk,);

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t
                            },
                        ))?;

                        RawRecord::from((primary_key, version, record_bytes))
                    };

                    assert_eq!(raw_record, res);

                    assert!(!continuation.is_begin_marker());
                    assert!(!continuation.is_end_marker());

                    let continuation_prev = continuation;

                    // We expect `CursorError`.
                    let res = raw_record_cursor.next().await.unwrap_err();

                    assert!(matches!(
                        res,
                        CursorError::NoNextReason(NoNextReason::ReturnLimitReached(_))
                    ));

                    let continuation = if let CursorError::NoNextReason(NoNextReason::ReturnLimitReached(
                        continuation,
                    )) = res
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
			continuation
                    } else {
			panic!("CursorError::NoNextReason(NoNextReason::ReturnLimitReached(_)) expected");
		    };

		    assert_eq!(continuation_prev.to_bytes(), continuation.to_bytes());

                    let mut raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            None,
                            None,
                            None,
                            None,
			    // We expect `Ok(bytes)`.
                            Some(continuation.to_bytes().unwrap()),
                            &tr,
                        )?
                    };

                    // We expect `CursorError`.
                    let res = raw_record_cursor.next().await.unwrap_err();

                    assert!(matches!(
                        res,
                        CursorError::NoNextReason(NoNextReason::SourceExhausted(_))
                    ));

		    if let CursorError::NoNextReason(NoNextReason::SourceExhausted(
                        continuation,
                    )) = res
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(continuation.is_end_marker());
                    }

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_subspace_normal_in_band_limit_reverse() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    let raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            None,
                            None,
                            Some(0),
                            Some(true),
                            None,
                            &tr,
                        )
                    };

                    assert!(matches!(raw_record_cursor, Err(_),));

                    if let Err(err) = raw_record_cursor {
                        assert_eq!(err, FdbError::new(RAW_RECORD_CURSOR_BUILDER_ERROR));
                    }

                    let mut raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            None,
                            None,
                            Some(1),
                            Some(true),
                            None,
                            &tr,
                        )?
                    };

                    // We expect `CursorSuccess<RawRecord>`.
                    let (res, continuation) = raw_record_cursor.next().await.unwrap().into_parts();

                    let raw_record = {
                        let (pk, local_version, record_bytes) = ("very_long", 3, VERY_LONG_STRING.deref().clone());

                        let version = RecordVersion::from(Versionstamp::complete(
                            Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                            local_version,
                        ));

                        let primary_key = RawRecordPrimaryKey::try_from((
                            RawRecordPrimaryKeySchema::try_from({
                                let mut tuple_schema = TupleSchema::new();
                                tuple_schema.push_front(TupleSchemaElement::String);
                                tuple_schema
                            })?,
                            {
                                let tup: (&str,) = (pk,);

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t
                            },
                        ))?;

                        RawRecord::from((primary_key, version, record_bytes))
                    };

                    assert_eq!(raw_record, res);

                    assert!(!continuation.is_begin_marker());
                    assert!(!continuation.is_end_marker());

                    let continuation_prev = continuation;

                    // We expect `CursorError`.
                    let res = raw_record_cursor.next().await.unwrap_err();

                    assert!(matches!(
                        res,
                        CursorError::NoNextReason(NoNextReason::ReturnLimitReached(_))
                    ));

                    let continuation = if let CursorError::NoNextReason(NoNextReason::ReturnLimitReached(
                        continuation,
                    )) = res
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
			continuation
                    } else {
			panic!("CursorError::NoNextReason(NoNextReason::ReturnLimitReached(_)) expected");
		    };

		    assert_eq!(continuation_prev.to_bytes(), continuation.to_bytes());

                    let mut raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            None,
                            None,
                            Some(2),
                            Some(true),
			    // We expect `Ok(bytes)`.
                            Some(continuation.to_bytes().unwrap()),
                            &tr,
                        )?
                    };

                    for (pk, local_version, record_bytes) in [
                        // This is the lexicographic order.
                        ("medium", 1, MEDIUM_STRING.deref().clone()),
                        ("short", 2, SHORT_STRING.deref().clone()),
                    ].iter()
		    .rev() {
                        let version = RecordVersion::from(Versionstamp::complete(
                            Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                            *local_version,
                        ));

                        let primary_key = RawRecordPrimaryKey::try_from((
                            RawRecordPrimaryKeySchema::try_from({
                                let mut tuple_schema = TupleSchema::new();
                                tuple_schema.push_front(TupleSchemaElement::String);
                                tuple_schema
                            })?,
                            {
                                let tup: (&str,) = (pk,);

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t
                            },
                        ))?;

                        let raw_record =
                            RawRecord::from((primary_key, version, record_bytes.clone()));

                        // We expect `CursorSuccess<RawRecord>`.
                        let (res, continuation) =
                            raw_record_cursor.next().await.unwrap().into_parts();

                        assert_eq!(raw_record, res);

                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
		    }

                    // We expect `CursorError`.
                    let res = raw_record_cursor.next().await.unwrap_err();

                    assert!(matches!(
                        res,
                        CursorError::NoNextReason(NoNextReason::ReturnLimitReached(_))
                    ));

                    let continuation = if let CursorError::NoNextReason(NoNextReason::ReturnLimitReached(
                        continuation,
                    )) = res
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
			continuation
                    } else {
			panic!("CursorError::NoNextReason(NoNextReason::ReturnLimitReached(_)) expected");
		    };

                    let mut raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            None,
                            None,
                            Some(1),
                            Some(true),
			    // We expect `Ok(bytes)`.
                            Some(continuation.to_bytes().unwrap()),
                            &tr,
                        )?
                    };

                    // We expect `CursorSuccess<RawRecord>`.
                    let (res, continuation) = raw_record_cursor.next().await.unwrap().into_parts();

                    let raw_record = {
                        let (pk, local_version, record_bytes) = ("long", 0, LONG_STRING.deref().clone());

                        let version = RecordVersion::from(Versionstamp::complete(
                            Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                            local_version,
                        ));

                        let primary_key = RawRecordPrimaryKey::try_from((
                            RawRecordPrimaryKeySchema::try_from({
                                let mut tuple_schema = TupleSchema::new();
                                tuple_schema.push_front(TupleSchemaElement::String);
                                tuple_schema
                            })?,
                            {
                                let tup: (&str,) = (pk,);

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t
                            },
                        ))?;

                        RawRecord::from((primary_key, version, record_bytes))
                    };

                    assert_eq!(raw_record, res);

                    assert!(!continuation.is_begin_marker());
                    assert!(!continuation.is_end_marker());

                    let continuation_prev = continuation;

                    // We expect `CursorError`.
                    let res = raw_record_cursor.next().await.unwrap_err();

                    assert!(matches!(
                        res,
                        CursorError::NoNextReason(NoNextReason::ReturnLimitReached(_))
                    ));

                    let continuation = if let CursorError::NoNextReason(NoNextReason::ReturnLimitReached(
                        continuation,
                    )) = res
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
			continuation
                    } else {
			panic!("CursorError::NoNextReason(NoNextReason::ReturnLimitReached(_)) expected");
		    };

		    assert_eq!(continuation_prev.to_bytes(), continuation.to_bytes());

                    let mut raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            None,
                            None,
                            None,
                            Some(true),
			    // We expect `Ok(bytes)`.
                            Some(continuation.to_bytes().unwrap()),
                            &tr,
                        )?
                    };

                    // We expect `CursorError`.
                    let res = raw_record_cursor.next().await.unwrap_err();

                    assert!(matches!(
                        res,
                        CursorError::NoNextReason(NoNextReason::SourceExhausted(_))
                    ));

		    if let CursorError::NoNextReason(NoNextReason::SourceExhausted(
                        continuation,
                    )) = res
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(continuation.is_end_marker());
                    }

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_subspace_normal_out_of_band_limit_forward() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    // When in-band and out-of-band limits overlap,
                    // in-band limit takes precedence.
                    //
                    // `long` key is three key-values
                    //
                    // ```
                    // `\x02sub\x00\x02space\x00\x02normal\x00\x02long\x00\x13\xfe'
                    // `\x02sub\x00\x02space\x00\x02normal\x00\x02long\x00\x14'
                    // `\x02sub\x00\x02space\x00\x02normal\x00\x02long\x00\x15\x01'
                    // ```
                    let mut raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            Some(ScanLimiter::new(
                                Some(KeyValueScanLimiter::enforcing(3)),
                                None,
                                None,
                            )),
                            None,
                            Some(1),
                            None,
                            None,
                            &tr,
                        )?
                    };

                    // We expect `CursorSuccess<RawRecord>`.
                    let (res, continuation) = raw_record_cursor.next().await.unwrap().into_parts();
                    let raw_record = {
                        let (pk, local_version, record_bytes) =
 ("long", 0, LONG_STRING.deref().clone());

                        let version = RecordVersion::from(Versionstamp::complete(
                            Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                            local_version,
                        ));

                        let primary_key = RawRecordPrimaryKey::try_from((
                            RawRecordPrimaryKeySchema::try_from({
                                let mut tuple_schema = TupleSchema::new();
                                tuple_schema.push_front(TupleSchemaElement::String);
                                tuple_schema
                            })?,
                            {
                                let tup: (&str,) = (pk,);

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t
                            },
                        ))?;

                        RawRecord::from((primary_key, version, record_bytes))
                    };

                    assert_eq!(raw_record, res);

                    assert!(!continuation.is_begin_marker());
                    assert!(!continuation.is_end_marker());

                    let continuation_prev = continuation;

                    // We expect `CursorError`.
                    let res = raw_record_cursor.next().await.unwrap_err();

                    assert!(matches!(
                        res,
                        CursorError::NoNextReason(NoNextReason::ReturnLimitReached(_))
                    ));

                    let continuation = if let CursorError::NoNextReason(NoNextReason::ReturnLimitReached(
                        continuation,
                    )) = res
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
                        continuation
                    } else {
                        panic!("CursorError::NoNextReason(NoNextReason::ReturnLimitReached(_)) expected");
                    };

                    assert_eq!(continuation_prev.to_bytes(), continuation.to_bytes());

                    // Out-of-band limit when no records are read,
                    // should return the begin continuation.
                    let mut raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            Some(ScanLimiter::new(
                                Some(KeyValueScanLimiter::enforcing(0)),
                                None,
                                None,
                            )),
                            None,
                            None,
                            None,
                            None,
                            &tr,
                        )?
                    };

                    // We expect `CursorError`.
                    let res = raw_record_cursor.next().await.unwrap_err();

                    assert!(matches!(
                        res,
                        CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(_))
                    ));

                    let continuation = if let CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(
                        continuation,
                    )) = res
                    {
                        assert!(continuation.is_begin_marker());
                        continuation
                    } else {
                        panic!("CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(_)) expected");
                    };

                    // Let us take the begin marker continuation and
                    // build a cursor that reads first record
                    // partially (two key-values). It should still
                    // return begin continuation.
                    let mut raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            Some(ScanLimiter::new(
                                Some(KeyValueScanLimiter::enforcing(2)),
                                None,
                                None,
                            )),
                            None,
                            None,
                            None,
                            // We expect `Ok(bytes)`.
                            Some(continuation.to_bytes().unwrap()),
                            &tr,
                        )?
                    };

                    // We expect `CursorError`.
                    let res = raw_record_cursor.next().await.unwrap_err();

                    assert!(matches!(
                        res,
                        CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(_))
                    ));

                    let continuation = if let CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(
                        continuation,
                    )) = res
                    {
                        assert!(continuation.is_begin_marker());
                        continuation
                    } else {
                        panic!("CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(_)) expected");
                    };

                    // Let us take the begin marker continuation and
                    // build a cursor that reads first record fully,
                    // and second record partially (four key-values).
                    let mut raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            Some(ScanLimiter::new(
                                Some(KeyValueScanLimiter::enforcing(4)),
                                None,
                                None,
                            )),
                            None,
                            None,
                            None,
                            // We expect `Ok(bytes)`.
                            Some(continuation.to_bytes().unwrap()),
                            &tr,
                        )?
                    };

                    // We expect `CursorSuccess<RawRecord>`.
                    let (res, continuation) = raw_record_cursor.next().await.unwrap().into_parts();
                    let raw_record = {
                        let (pk, local_version, record_bytes) =
 ("long", 0, LONG_STRING.deref().clone());

                        let version = RecordVersion::from(Versionstamp::complete(
                            Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                            local_version,
                        ));

                        let primary_key = RawRecordPrimaryKey::try_from((
                            RawRecordPrimaryKeySchema::try_from({
                                let mut tuple_schema = TupleSchema::new();
                                tuple_schema.push_front(TupleSchemaElement::String);
                                tuple_schema
                            })?,
                            {
                                let tup: (&str,) = (pk,);

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t
                            },
                        ))?;

                        RawRecord::from((primary_key, version, record_bytes))
                    };

                    assert_eq!(raw_record, res);

                    assert!(!continuation.is_begin_marker());
                    assert!(!continuation.is_end_marker());

                    let continuation_prev = continuation;

                    // We expect `CursorError`.
                    let res = raw_record_cursor.next().await.unwrap_err();

                    assert!(matches!(
                        res,
                        CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(_))
                    ));

                    let continuation = if let CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(
                        continuation,
                    )) = res
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
                        continuation
                    } else {
                        panic!("CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(_)) expected");
                    };

                    assert_eq!(continuation_prev.to_bytes(), continuation.to_bytes());

                    // Let us take the previous continuation and build
                    // a cursor that reads next two records fully
                    // (second and third record) and one record
                    // partially (forth record). That would be six or
                    // seven key-values.
                    let mut raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            Some(ScanLimiter::new(
                                Some(KeyValueScanLimiter::enforcing(7)),
                                None,
                                None,
                            )),
                            None,
                            None,
                            None,
                            // We expect `Ok(bytes)`.
                            Some(continuation.to_bytes().unwrap()),
                            &tr,
                        )?
                    };

                    let continuation = {
                        let mut idx = 0;

                        let data = [
                            // This is the lexicographic order.
                            ("medium", 1, MEDIUM_STRING.deref().clone()),
                            ("short", 2, SHORT_STRING.deref().clone()),
                        ];

                        loop {
                            let (pk, local_version, record_bytes) = &data[idx];

                            let version = RecordVersion::from(
                                Versionstamp::complete(
                                    Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                                    *local_version,
                                )
                            );

                            let primary_key = RawRecordPrimaryKey::try_from((
                                RawRecordPrimaryKeySchema::try_from({
                                    let mut tuple_schema = TupleSchema::new();
                                    tuple_schema.push_front(TupleSchemaElement::String);
                                    tuple_schema
                                })?,
                                {
                                    let tup: (&str,) = (pk,);

                                    let mut t = Tuple::new();
                                    t.push_back::<String>(tup.0.to_string());
                                    t
                                },
                            ))?;

                            let raw_record = RawRecord::from((primary_key, version, record_bytes.clone()));

                            // We expect `CursorSuccess<RawRecord>`.
                            let (res, continuation) =
                                raw_record_cursor.next().await.unwrap().into_parts();

                            assert_eq!(raw_record, res);

                            assert!(!continuation.is_begin_marker());
                            assert!(!continuation.is_end_marker());

                            idx += 1;

                            if idx == 2 {
                                break continuation;
                            }
                        }
                    };

                    let continuation_prev = continuation;

                    // We expect `CursorError`.
                    let res = raw_record_cursor.next().await.unwrap_err();

                    assert!(matches!(
                        res,
                        CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(_))
                    ));

                    let continuation = if let CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(
                        continuation,
                    )) = res
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
                        continuation
                    } else {
                        panic!("CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(_)) expected");
                    };

                    assert_eq!(continuation_prev.to_bytes(), continuation.to_bytes());

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_subspace_normal_out_of_band_limit_reverse() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    // When in-band and out-of-band limits overlap,
                    // in-band limit takes precedence.
                    //
                    // `long` key is three key-values
                    //
                    // ```
                    // `\x02sub\x00\x02space\x00\x02normal\x00\x02very_long\x00\x13\xfe'
                    // `\x02sub\x00\x02space\x00\x02normal\x00\x02very_long\x00\x14'
                    // `\x02sub\x00\x02space\x00\x02normal\x00\x02very_long\x00\x15\x01'
                    // `\x02sub\x00\x02space\x00\x02normal\x00\x02very_long\x00\x15\x02'
                    // ```
                    let mut raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            Some(ScanLimiter::new(
                                Some(KeyValueScanLimiter::enforcing(4)),
                                None,
                                None,
                            )),
                            None,
                            Some(1),
                            Some(true),
                            None,
                            &tr,
                        )?
                    };

                    // We expect `CursorSuccess<RawRecord>`.
                    let (res, continuation) = raw_record_cursor.next().await.unwrap().into_parts();
                    let raw_record = {
                        let (pk, local_version, record_bytes) =
 ("very_long", 3, VERY_LONG_STRING.deref().clone());

                        let version = RecordVersion::from(Versionstamp::complete(
                            Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                            local_version,
                        ));

                        let primary_key = RawRecordPrimaryKey::try_from((
                            RawRecordPrimaryKeySchema::try_from({
                                let mut tuple_schema = TupleSchema::new();
                                tuple_schema.push_front(TupleSchemaElement::String);
                                tuple_schema
                            })?,
                            {
                                let tup: (&str,) = (pk,);

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t
                            },
                        ))?;

                        RawRecord::from((primary_key, version, record_bytes))
                    };

                    assert_eq!(raw_record, res);

                    assert!(!continuation.is_begin_marker());
                    assert!(!continuation.is_end_marker());

                    let continuation_prev = continuation;

                    // We expect `CursorError`.
                    let res = raw_record_cursor.next().await.unwrap_err();

                    assert!(matches!(
                        res,
                        CursorError::NoNextReason(NoNextReason::ReturnLimitReached(_))
                    ));

                    let continuation = if let CursorError::NoNextReason(NoNextReason::ReturnLimitReached(
                        continuation,
                    )) = res
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
                        continuation
                    } else {
                        panic!("CursorError::NoNextReason(NoNextReason::ReturnLimitReached(_)) expected");
                    };

                    assert_eq!(continuation_prev.to_bytes(), continuation.to_bytes());

                    // Out-of-band limit when no records are read,
                    // should return the begin continuation.
                    let mut raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            Some(ScanLimiter::new(
                                Some(KeyValueScanLimiter::enforcing(0)),
                                None,
                                None,
                            )),
                            None,
			    None,
                            Some(true),
                            None,
                            &tr,
                        )?
                    };

                    // We expect `CursorError`.
                    let res = raw_record_cursor.next().await.unwrap_err();

                    assert!(matches!(
                        res,
                        CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(_))
                    ));

                    let continuation = if let CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(
                        continuation,
                    )) = res
                    {
                        assert!(continuation.is_begin_marker());
                        continuation
                    } else {
                        panic!("CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(_)) expected");
                    };

                    // Let us take the begin marker continuation and
                    // build a cursor that reads last record
                    // partially (two key-values). It should still
                    // return begin continuation.
                    let mut raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            Some(ScanLimiter::new(
                                Some(KeyValueScanLimiter::enforcing(2)),
                                None,
                                None,
                            )),
                            None,
			    None,
                            Some(true),
                            // We expect `Ok(bytes)`.
                            Some(continuation.to_bytes().unwrap()),
                            &tr,
                        )?
                    };

                    // We expect `CursorError`.
                    let res = raw_record_cursor.next().await.unwrap_err();

                    assert!(matches!(
                        res,
                        CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(_))
                    ));

                    let continuation = if let CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(
                        continuation,
                    )) = res
                    {
                        assert!(continuation.is_begin_marker());
                        continuation
                    } else {
                        panic!("CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(_)) expected");
                    };

                    // Let us take the begin marker continuation and
                    // build a cursor that reads last record fully,
                    // and second last record partially (five
                    // key-values).
                    let mut raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            Some(ScanLimiter::new(
                                Some(KeyValueScanLimiter::enforcing(5)),
                                None,
                                None,
                            )),
                            None,
			    None,
                            Some(true),
                            // We expect `Ok(bytes)`.
                            Some(continuation.to_bytes().unwrap()),
                            &tr,
                        )?
                    };

                    // We expect `CursorSuccess<RawRecord>`.
                    let (res, continuation) = raw_record_cursor.next().await.unwrap().into_parts();
                    let raw_record = {
                        let (pk, local_version, record_bytes) =
 ("very_long", 3, VERY_LONG_STRING.deref().clone());

                        let version = RecordVersion::from(Versionstamp::complete(
                            Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                            local_version,
                        ));

                        let primary_key = RawRecordPrimaryKey::try_from((
                            RawRecordPrimaryKeySchema::try_from({
                                let mut tuple_schema = TupleSchema::new();
                                tuple_schema.push_front(TupleSchemaElement::String);
                                tuple_schema
                            })?,
                            {
                                let tup: (&str,) = (pk,);

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t
                            },
                        ))?;

                        RawRecord::from((primary_key, version, record_bytes))
                    };

                    assert_eq!(raw_record, res);

                    assert!(!continuation.is_begin_marker());
                    assert!(!continuation.is_end_marker());

                    let continuation_prev = continuation;

                    // We expect `CursorError`.
                    let res = raw_record_cursor.next().await.unwrap_err();

                    assert!(matches!(
                        res,
                        CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(_))
                    ));

                    let continuation = if let CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(
                        continuation,
                    )) = res
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
                        continuation
                    } else {
                        panic!("CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(_)) expected");
                    };

                    assert_eq!(continuation_prev.to_bytes(), continuation.to_bytes());

                    // Let us take the previous continuation and build
                    // a cursor that reads next two records fully
                    // (second last and third last record) and one
                    // record partially (forth last record). That
                    // would be five or six key-values.
                    let mut raw_record_cursor = {
                        let subspace = Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str, &str) = ("sub", "space", "normal");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        });

                        let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                            let mut tuple_schema = TupleSchema::new();
                            tuple_schema.push_front(TupleSchemaElement::String);
                            tuple_schema
                        })?;

                        raw_record_cursor_builder_build(
                            Some(primary_key_schema),
                            Some(subspace),
                            Some(ScanLimiter::new(
                                Some(KeyValueScanLimiter::enforcing(5)),
                                None,
                                None,
                            )),
                            None,
			    None,
                            Some(true),
                            // We expect `Ok(bytes)`.
                            Some(continuation.to_bytes().unwrap()),
                            &tr,
                        )?
                    };

                    let continuation = {
                        let mut idx = 0;

                        let data = [
                            // This is the lexicographic order.
                            ("medium", 1, MEDIUM_STRING.deref().clone()),
                            ("short", 2, SHORT_STRING.deref().clone()),
                        ];

                        loop {
			    // `1 - idx` gives us reverse order.
                            let (pk, local_version, record_bytes) = &data[1 - idx];

                            let version = RecordVersion::from(
                                Versionstamp::complete(
                                    Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                                    *local_version,
                                )
                            );

                            let primary_key = RawRecordPrimaryKey::try_from((
                                RawRecordPrimaryKeySchema::try_from({
                                    let mut tuple_schema = TupleSchema::new();
                                    tuple_schema.push_front(TupleSchemaElement::String);
                                    tuple_schema
                                })?,
                                {
                                    let tup: (&str,) = (pk,);

                                    let mut t = Tuple::new();
                                    t.push_back::<String>(tup.0.to_string());
                                    t
                                },
                            ))?;

                            let raw_record = RawRecord::from((primary_key, version, record_bytes.clone()));

                            // We expect `CursorSuccess<RawRecord>`.
                            let (res, continuation) =
                                raw_record_cursor.next().await.unwrap().into_parts();

                            assert_eq!(raw_record, res);

                            assert!(!continuation.is_begin_marker());
                            assert!(!continuation.is_end_marker());

                            idx += 1;

                            if idx == 2 {
                                break continuation;
                            }
                        }
                    };

                    let continuation_prev = continuation;

                    // We expect `CursorError`.
                    let res = raw_record_cursor.next().await.unwrap_err();

                    assert!(matches!(
                        res,
                        CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(_))
                    ));

                    let continuation = if let CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(
                        continuation,
                    )) = res
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
                        continuation
                    } else {
                        panic!("CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(_)) expected");
                    };

                    assert_eq!(continuation_prev.to_bytes(), continuation.to_bytes());

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_subspace_abnormal_forward_state_initiate_record_version_read() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    // `("sub", "space", "abnormal", "01", "long", "non_number")`
                    {
                        let mut raw_record_cursor = {
                            let subspace = Subspace::new(Bytes::new()).subspace(&{
                                let tup: (&str, &str, &str, &str) =
                                    ("sub", "space", "abnormal", "01");

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t.push_back::<String>(tup.1.to_string());
                                t.push_back::<String>(tup.2.to_string());
                                t.push_back::<String>(tup.3.to_string());
                                t
                            });

                            let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                                let mut tuple_schema = TupleSchema::new();
                                tuple_schema.push_front(TupleSchemaElement::String);
                                tuple_schema
                            })?;

                            raw_record_cursor_builder_build(
                                Some(primary_key_schema),
                                Some(subspace),
                                None,
                                None,
                                None,
                                None,
                                None,
                                &tr,
                            )?
                        };

                        // We expect `CursorError`.
                        let res = raw_record_cursor.next().await.unwrap_err();

                        assert!(matches!(res, CursorError::FdbError(_, _)));

                        if let CursorError::FdbError(fdb_error, continuation) = res {
                            assert_eq!(fdb_error, FdbError::new(RAW_RECORD_CURSOR_NEXT_ERROR));
                            assert!(continuation.is_begin_marker());
                        }
                    }

                    // `("sub", "space", "abnormal", "02", "long", 0)`
                    {
                        let mut raw_record_cursor = {
                            let subspace = Subspace::new(Bytes::new()).subspace(&{
                                let tup: (&str, &str, &str, &str) =
                                    ("sub", "space", "abnormal", "02");

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t.push_back::<String>(tup.1.to_string());
                                t.push_back::<String>(tup.2.to_string());
                                t.push_back::<String>(tup.3.to_string());
                                t
                            });

                            let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                                let mut tuple_schema = TupleSchema::new();
                                tuple_schema.push_front(TupleSchemaElement::String);
                                tuple_schema
                            })?;

                            raw_record_cursor_builder_build(
                                Some(primary_key_schema),
                                Some(subspace),
                                None,
                                None,
                                None,
                                None,
                                None,
                                &tr,
                            )?
                        };

                        // We expect `CursorError`.
                        let res = raw_record_cursor.next().await.unwrap_err();

                        assert!(matches!(res, CursorError::FdbError(_, _)));

                        if let CursorError::FdbError(fdb_error, continuation) = res {
                            assert_eq!(fdb_error, FdbError::new(RAW_RECORD_CURSOR_NEXT_ERROR));
                            assert!(continuation.is_begin_marker());
                        }
                    }

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_subspace_abnormal_reverse_state_initiate_last_split_read() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    // ("sub", "space", "abnormal", "03", "long", "non_number")
                    {
                        let mut raw_record_cursor = {
                            let subspace = Subspace::new(Bytes::new()).subspace(&{
                                let tup: (&str, &str, &str, &str) =
                                    ("sub", "space", "abnormal", "03");

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t.push_back::<String>(tup.1.to_string());
                                t.push_back::<String>(tup.2.to_string());
                                t.push_back::<String>(tup.3.to_string());
                                t
                            });

                            let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                                let mut tuple_schema = TupleSchema::new();
                                tuple_schema.push_front(TupleSchemaElement::String);
                                tuple_schema
                            })?;

                            raw_record_cursor_builder_build(
                                Some(primary_key_schema),
                                Some(subspace),
                                None,
                                None,
                                None,
                                Some(true),
                                None,
                                &tr,
                            )?
                        };

                        // We expect `CursorError`.
                        let res = raw_record_cursor.next().await.unwrap_err();

                        assert!(matches!(res, CursorError::FdbError(_, _)));

                        if let CursorError::FdbError(fdb_error, continuation) = res {
                            assert_eq!(fdb_error, FdbError::new(RAW_RECORD_CURSOR_NEXT_ERROR));
                            assert!(continuation.is_begin_marker());
                        }
                    }

                    // ("sub", "space", "abnormal", "04", "long", -1)
                    {
                        let mut raw_record_cursor = {
                            let subspace = Subspace::new(Bytes::new()).subspace(&{
                                let tup: (&str, &str, &str, &str) =
                                    ("sub", "space", "abnormal", "04");

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t.push_back::<String>(tup.1.to_string());
                                t.push_back::<String>(tup.2.to_string());
                                t.push_back::<String>(tup.3.to_string());
                                t
                            });

                            let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                                let mut tuple_schema = TupleSchema::new();
                                tuple_schema.push_front(TupleSchemaElement::String);
                                tuple_schema
                            })?;

                            raw_record_cursor_builder_build(
                                Some(primary_key_schema),
                                Some(subspace),
                                None,
                                None,
                                None,
                                Some(true),
                                None,
                                &tr,
                            )?
                        };

                        // We expect `CursorError`.
                        let res = raw_record_cursor.next().await.unwrap_err();

                        assert!(matches!(res, CursorError::FdbError(_, _)));

                        if let CursorError::FdbError(fdb_error, continuation) = res {
                            assert_eq!(fdb_error, FdbError::new(RAW_RECORD_CURSOR_NEXT_ERROR));
                            assert!(continuation.is_begin_marker());
                        }
                    }

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_subspace_abnormal_forward_state_read_record_version() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    // Record with a missing key-value
                    //
                    // ```
                    // ("sub", "space", "abnormal", "05", "long", -1)
                    // ("sub", "space", "abnormal", "05", "long",  0) missing
                    // ("sub", "space", "abnormal", "05", "long",  1)
                    // ```
                    {
                        let mut raw_record_cursor = {
                            let subspace = Subspace::new(Bytes::new()).subspace(&{
                                let tup: (&str, &str, &str, &str) =
                                    ("sub", "space", "abnormal", "05");

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t.push_back::<String>(tup.1.to_string());
                                t.push_back::<String>(tup.2.to_string());
                                t.push_back::<String>(tup.3.to_string());
                                t
                            });

                            let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                                let mut tuple_schema = TupleSchema::new();
                                tuple_schema.push_front(TupleSchemaElement::String);
                                tuple_schema
                            })?;

                            raw_record_cursor_builder_build(
                                Some(primary_key_schema),
                                Some(subspace),
                                None,
                                None,
                                None,
                                None,
                                None,
                                &tr,
                            )?
                        };

                        // We expect `CursorError`.
                        let res = raw_record_cursor.next().await.unwrap_err();

                        assert!(matches!(res, CursorError::FdbError(_, _)));

                        if let CursorError::FdbError(fdb_error, continuation) = res {
                            assert_eq!(fdb_error, FdbError::new(RAW_RECORD_CURSOR_NEXT_ERROR));
                            assert!(continuation.is_begin_marker());
                        }
                    }

                    // Record with a correct sequence but invalid
                    // primary key.
                    //
                    // ```
                    // ("sub", "space", "abnormal", "06", "medium", -1)
                    // ("sub", "space", "abnormal", "06", "medium",  0) missing
                    // ("sub", "space", "abnormal", "06", "short",  -1) missing
                    // ("sub", "space", "abnormal", "06", "short",   0)
                    // ```
                    {
                        let mut raw_record_cursor = {
                            let subspace = Subspace::new(Bytes::new()).subspace(&{
                                let tup: (&str, &str, &str, &str) =
                                    ("sub", "space", "abnormal", "06");

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t.push_back::<String>(tup.1.to_string());
                                t.push_back::<String>(tup.2.to_string());
                                t.push_back::<String>(tup.3.to_string());
                                t
                            });

                            let primary_key_schema = RawRecordPrimaryKeySchema::try_from({
                                let mut tuple_schema = TupleSchema::new();
                                tuple_schema.push_front(TupleSchemaElement::String);
                                tuple_schema
                            })?;

                            raw_record_cursor_builder_build(
                                Some(primary_key_schema),
                                Some(subspace),
                                None,
                                None,
                                None,
                                None,
                                None,
                                &tr,
                            )?
                        };

                        // We expect `CursorError`.
                        let res = raw_record_cursor.next().await.unwrap_err();

                        assert!(matches!(res, CursorError::FdbError(_, _)));

                        if let CursorError::FdbError(fdb_error, continuation) = res {
                            assert_eq!(fdb_error, FdbError::new(RAW_RECORD_CURSOR_NEXT_ERROR));
                            assert!(continuation.is_begin_marker());
                        }
                    }

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}
