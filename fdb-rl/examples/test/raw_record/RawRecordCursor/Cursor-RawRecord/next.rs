#![allow(non_snake_case)]
#![feature(once_cell)]

use bytes::{BufMut, Bytes, BytesMut};

use fdb::database::FdbDatabase;
use fdb::error::FdbResult;
use fdb::range::Range;
use fdb::subspace::Subspace;
use fdb::transaction::Transaction;
use fdb::tuple::{Tuple, TupleSchema, TupleSchemaElement, Versionstamp};

use fdb_rl::cursor::{Cursor, CursorError, NoNextReason};
use fdb_rl::test::raw_record::{
    raw_record_cursor_builder_build, RawRecord, RawRecordPrimaryKey, RawRecordPrimaryKeySchema,
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

    let tests = vec![];

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

// Tests

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
