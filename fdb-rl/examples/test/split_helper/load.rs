#![feature(lazy_cell)]

use bytes::{Buf, BufMut, Bytes, BytesMut};

use fdb::database::FdbDatabase;
use fdb::error::FdbError;
use fdb::range::Range;
use fdb::subspace::Subspace;
use fdb::transaction::{MutationType, Transaction};
use fdb::tuple::{Null, Tuple, Versionstamp};
use fdb::{Key, Value};

use fdb_rl::scan::{KeyValueScanLimiter, ScanLimiter};
use fdb_rl::test::split_helper::{
    self, SPLIT_HELPER_INVALID_PRIMARY_KEY, SPLIT_HELPER_LOAD_INVALID_RECORD_HEADER,
    SPLIT_HELPER_LOAD_INVALID_SERIALIZED_BYTES, SPLIT_HELPER_SCAN_LIMIT_REACHED,
};
use fdb_rl::RecordVersion;

use libtest_mimic::{Arguments, Failed, Trial};

use num_bigint::BigInt;

use tokio::runtime::Builder;

use std::env;
use std::error::Error;
use std::ops::Deref;
use std::sync::LazyLock;

static mut FDB_DATABASE: Option<FdbDatabase> = None;

const SPLIT_RECORD_SIZE: usize = 100_000;

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

    setup()?;

    let tests = vec![
        Trial::test("load", load),
        Trial::test("load_primary_key_invalid", load_primary_key_invalid),
        Trial::test(
            "load_primary_key_invalid_record_header",
            load_primary_key_invalid_record_header,
        ),
        Trial::test(
            "load_primary_key_invalid_record_content",
            load_primary_key_invalid_record_content,
        ),
        Trial::test(
            "load_primary_key_valid_with_empty_record",
            load_primary_key_valid_with_empty_record,
        ),
        Trial::test(
            "load_primary_key_valid_and_missing",
            load_primary_key_valid_and_missing,
        ),
        Trial::test("load_scan_limiter", load_scan_limiter),
        Trial::test("load_transaction_save_load", load_transaction_save_load),
        Trial::test("load_exact_100kb", load_exact_100kb),
        Trial::test("load_exact_200kb", load_exact_200kb),
        Trial::test("load_no_subspace", load_no_subspace),
    ];

    let _ = libtest_mimic::run(&args, tests);

    let fdb_database = unsafe { FDB_DATABASE.take().unwrap() };

    drop(fdb_database);

    unsafe {
        fdb::stop_network();
    }

    Ok(())
}

fn setup() -> Result<(), Box<dyn Error>> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .run(|tr| async move {
                    // Clear the database.
                    tr.clear_range(Range::new(Bytes::new(), Bytes::from_static(b"\xFF")));

                    let subspace = Some(Subspace::new(Bytes::new()).subspace(&{
                        let tup: (&str, &str) = ("sub", "space");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t
                    }));

                    // add SHORT_STRING record
                    {
                        let primary_key_tuple = {
                            let tup: (&str, &str, &str) = ("primary", "key", "short");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        };

                        let serialized_bytes = SHORT_STRING.deref().clone();

                        let record_version = RecordVersion::from(Versionstamp::incomplete(10));

                        split_helper::save(
                            &tr,
                            &None,
                            &subspace,
                            &primary_key_tuple,
                            serialized_bytes,
                            record_version,
                        )
                        .await?;
                    }

                    // add MEDIUM_STRING record
                    {
                        let primary_key_tuple = {
                            let tup: (&str, &str, &str) = ("primary", "key", "medium");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        };

                        let serialized_bytes = MEDIUM_STRING.deref().clone();

                        let record_version = RecordVersion::from(Versionstamp::incomplete(11));

                        split_helper::save(
                            &tr,
                            &None,
                            &subspace,
                            &primary_key_tuple,
                            serialized_bytes,
                            record_version,
                        )
                        .await?;
                    }

                    // add LONG_STRING record
                    {
                        let primary_key_tuple = {
                            let tup: (&str, &str, &str) = ("primary", "key", "long");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        };

                        let serialized_bytes = LONG_STRING.deref().clone();

                        let record_version = RecordVersion::from(Versionstamp::incomplete(12));

                        split_helper::save(
                            &tr,
                            &None,
                            &subspace,
                            &primary_key_tuple,
                            serialized_bytes,
                            record_version,
                        )
                        .await?;
                    }

                    // add VERY_LONG_STRING record
                    {
                        let primary_key_tuple = {
                            let tup: (&str, &str, &str) = ("primary", "key", "very_long");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        };

                        let serialized_bytes = VERY_LONG_STRING.deref().clone();

                        let record_version = RecordVersion::from(Versionstamp::incomplete(13));

                        split_helper::save(
                            &tr,
                            &None,
                            &subspace,
                            &primary_key_tuple,
                            serialized_bytes,
                            record_version,
                        )
                        .await?;
                    }

                    // add empty record
                    {
                        let primary_key_tuple = {
                            let tup: (&str, &str, &str) = ("primary", "key", "empty");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        };

                        let serialized_bytes = Bytes::new();

                        let record_version = RecordVersion::from(Versionstamp::incomplete(14));

                        split_helper::save(
                            &tr,
                            &None,
                            &subspace,
                            &primary_key_tuple,
                            serialized_bytes,
                            record_version,
                        )
                        .await?;
                    }

                    // add invalid record header
                    {
                        let full_primary_key_tuple = {
                            let tup: (&str, &str, &str, &str, &str) =
                                ("sub", "space", "primary", "key", "invalid_record_header");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t.push_back::<String>(tup.3.to_string());
                            t.push_back::<String>(tup.4.to_string());
                            t
                        };

                        // write a invalid record version
                        let key = Key::from(
                            {
                                let tup: (i8,) = (-1,);

                                let mut t = full_primary_key_tuple.clone();
                                t.push_back::<i8>(tup.0);
                                t
                            }
                            .pack(),
                        );

                        let value = Value::from(Bytes::from_static(b"invalid record header"));

                        tr.set(key, value);

                        // write a valid record content
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

                    // add invalid record content
                    {
                        let full_primary_key_tuple = {
                            let tup: (&str, &str, &str, &str, &str) =
                                ("sub", "space", "primary", "key", "invalid_record_content");

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

                        let value = {
                            // (header_version, data_splits, incarnation, versionstamp)
                            let tup: (i8, i8, Option<u64>, Versionstamp) =
                                (0, 2, None, Versionstamp::incomplete(15));

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
                        .pack_with_versionstamp(Bytes::new())?;

                        unsafe {
                            tr.mutate(MutationType::SetVersionstampedValue, key, value);
                        }

                        // write invalid record content
                        let key = Key::from(
                            {
                                // `(-1,)` is where record version is.
                                let tup: (i8,) = (0,);

                                let mut t = full_primary_key_tuple.clone();
                                t.push_back::<i8>(tup.0);
                                t
                            }
                            .pack(),
                        );

                        let value = Value::from(Bytes::from_static(b"invalid record content"));

                        tr.set(key, value);

                        let key = Key::from(
                            {
                                let tup: (bool,) = (false,);

                                let mut t = full_primary_key_tuple.clone();
                                t.push_back::<bool>(tup.0);
                                t
                            }
                            .pack(),
                        );

                        let value = Value::from(Bytes::from_static(b"invalid record content"));

                        tr.set(key, value);
                    }

                    // add exact 100kb record
                    {
                        let primary_key_tuple = {
                            let tup: (&str, &str, &str) = ("primary", "key", "exact_100kb");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        };

                        let serialized_bytes =
                            LONG_STRING.deref().clone().copy_to_bytes(SPLIT_RECORD_SIZE);
                        let record_version = RecordVersion::from(Versionstamp::incomplete(16));

                        split_helper::save(
                            &tr,
                            &None,
                            &subspace,
                            &primary_key_tuple,
                            serialized_bytes,
                            record_version,
                        )
                        .await?;
                    }

                    // add exact 200kb record
                    {
                        let primary_key_tuple = {
                            let tup: (&str, &str, &str) = ("primary", "key", "exact_200kb");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        };

                        let serialized_bytes = VERY_LONG_STRING
                            .deref()
                            .clone()
                            .copy_to_bytes(SPLIT_RECORD_SIZE * 2);
                        let record_version = RecordVersion::from(Versionstamp::incomplete(17));

                        split_helper::save(
                            &tr,
                            &None,
                            &subspace,
                            &primary_key_tuple,
                            serialized_bytes,
                            record_version,
                        )
                        .await?;
                    }

                    // add SHORT_STRING record with no subspace.
                    {
                        let primary_key_tuple = {
                            let tup: (&str, &str, &str) = ("primary", "key", "short");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.push_back::<String>(tup.2.to_string());
                            t
                        };

                        let serialized_bytes = SHORT_STRING.deref().clone();

                        let record_version = RecordVersion::from(Versionstamp::incomplete(18));

                        split_helper::save(
                            &tr,
                            &None,
                            &None,
                            &primary_key_tuple,
                            serialized_bytes,
                            record_version,
                        )
                        .await?;
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

fn load() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .run(|tr| async move {
                    let subspace = Some(Subspace::new(Bytes::new()).subspace(&{
                        let tup: (&str, &str) = ("sub", "space");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t
                    }));

                    // Basic assertions for `SHORT_STRING`,
                    // `MEDIUM_STRING`, `LONG_STRING` and
                    // `VERY_LONG_STRING`.

                    let primary_key_tuple_short = {
                        let tup: (&str, &str, &str) = ("primary", "key", "short");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<String>(tup.2.to_string());
                        t
                    };

                    let (_, serialized_bytes) =
                        split_helper::load(&tr, &None, &subspace, &primary_key_tuple_short)
                            .await?
                            .unwrap();

                    assert_eq!(serialized_bytes, SHORT_STRING.deref().clone());

                    let primary_key_tuple_medium = {
                        let tup: (&str, &str, &str) = ("primary", "key", "medium");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<String>(tup.2.to_string());
                        t
                    };

                    let (_, serialized_bytes) =
                        split_helper::load(&tr, &None, &subspace, &primary_key_tuple_medium)
                            .await?
                            .unwrap();

                    assert_eq!(serialized_bytes, MEDIUM_STRING.deref().clone());

                    let primary_key_tuple_long = {
                        let tup: (&str, &str, &str) = ("primary", "key", "long");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<String>(tup.2.to_string());
                        t
                    };

                    let (_, serialized_bytes) =
                        split_helper::load(&tr, &None, &subspace, &primary_key_tuple_long)
                            .await?
                            .unwrap();

                    assert_eq!(serialized_bytes, LONG_STRING.deref().clone());

                    let primary_key_tuple_very_long = {
                        let tup: (&str, &str, &str) = ("primary", "key", "very_long");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<String>(tup.2.to_string());
                        t
                    };

                    let (_, serialized_bytes) =
                        split_helper::load(&tr, &None, &subspace, &primary_key_tuple_very_long)
                            .await?
                            .unwrap();

                    assert_eq!(serialized_bytes, VERY_LONG_STRING.deref().clone());

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn load_primary_key_invalid() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .run(|tr| async move {
                    let subspace = Some(Subspace::new(Bytes::new()).subspace(&{
                        let tup: (&str, &str) = ("sub", "space");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t
                    }));

                    let primary_key_tuple_invalid = {
                        let tup: (&str, &str) = ("primary", "key");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t
                    };

                    let res =
                        split_helper::load(&tr, &None, &subspace, &primary_key_tuple_invalid).await;

                    assert_eq!(res, Err(FdbError::new(SPLIT_HELPER_INVALID_PRIMARY_KEY)));

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn load_primary_key_invalid_record_header() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .run(|tr| async move {
                    let subspace = Some(Subspace::new(Bytes::new()).subspace(&{
                        let tup: (&str, &str) = ("sub", "space");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t
                    }));

                    let primary_key_tuple = {
                        let tup: (&str, &str, &str) = ("primary", "key", "invalid_record_header");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<String>(tup.2.to_string());
                        t
                    };

                    let res = split_helper::load(&tr, &None, &subspace, &primary_key_tuple).await;

                    assert_eq!(
                        res,
                        Err(FdbError::new(SPLIT_HELPER_LOAD_INVALID_RECORD_HEADER))
                    );

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn load_primary_key_invalid_record_content() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .run(|tr| async move {
                    let subspace = Some(Subspace::new(Bytes::new()).subspace(&{
                        let tup: (&str, &str) = ("sub", "space");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t
                    }));

                    let primary_key_tuple = {
                        let tup: (&str, &str, &str) = ("primary", "key", "invalid_record_content");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<String>(tup.2.to_string());
                        t
                    };

                    let res = split_helper::load(&tr, &None, &subspace, &primary_key_tuple).await;

                    assert_eq!(
                        res,
                        Err(FdbError::new(SPLIT_HELPER_LOAD_INVALID_SERIALIZED_BYTES))
                    );

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn load_primary_key_valid_with_empty_record() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .run(|tr| async move {
                    let subspace = Some(Subspace::new(Bytes::new()).subspace(&{
                        let tup: (&str, &str) = ("sub", "space");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t
                    }));

                    let primary_key_tuple = {
                        let tup: (&str, &str, &str) = ("primary", "key", "empty");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<String>(tup.2.to_string());
                        t
                    };

                    let res = split_helper::load(&tr, &None, &subspace, &primary_key_tuple).await;

                    assert!(res.is_ok());

                    let (_, serialized_bytes) = res?.unwrap();

                    assert_eq!(serialized_bytes, Bytes::new());

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn load_primary_key_valid_and_missing() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .run(|tr| async move {
                    let subspace = Some(Subspace::new(Bytes::new()).subspace(&{
                        let tup: (&str, &str) = ("sub", "space");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t
                    }));

                    let primary_key_tuple = {
                        let tup: (&str, &str, &str) = ("primary", "key", "missing");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<String>(tup.2.to_string());
                        t
                    };

                    let res = split_helper::load(&tr, &None, &subspace, &primary_key_tuple).await;

                    assert!(res.is_ok());

                    assert!(res?.is_none());

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn load_scan_limiter() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .run(|tr| async move {
                    let subspace = Some(Subspace::new(Bytes::new()).subspace(&{
                        let tup: (&str, &str) = ("sub", "space");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t
                    }));

                    let primary_key_tuple_very_long = {
                        let tup: (&str, &str, &str) = ("primary", "key", "very_long");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<String>(tup.2.to_string());
                        t
                    };

                    // VERY_LONG_STRING has 2 splits. Along with
                    // record version that would 3 key-value
                    // pairs. Setup a scan limiter for 2 key-value
                    // pair.
                    let scan_limiter = Some(ScanLimiter::new(
                        Some(KeyValueScanLimiter::enforcing(2)),
                        None,
                        None,
                    ));

                    let res = split_helper::load(
                        &tr,
                        &scan_limiter,
                        &subspace,
                        &primary_key_tuple_very_long,
                    )
                    .await;

                    assert_eq!(res, Err(FdbError::new(SPLIT_HELPER_SCAN_LIMIT_REACHED)));

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn load_transaction_save_load() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            // This transaction will error. We are asserting the error
            // inside the transaction and ignoring the error here.
            let _ = fdb_database
                .run(|tr| async move {
                    let subspace = Some(Subspace::new(Bytes::new()).subspace(&{
                        let tup: (&str, &str) = ("sub", "space");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t
                    }));

                    let primary_key_tuple = {
                        let tup: (&str, &str, &str) = ("primary", "key", "save_load_incomplete");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<String>(tup.2.to_string());
                        t
                    };

                    let serialized_bytes = SHORT_STRING.deref().clone();

                    let record_version = RecordVersion::from(Versionstamp::incomplete(16));

                    split_helper::save(
                        &tr,
                        &None,
                        &subspace,
                        &primary_key_tuple,
                        serialized_bytes,
                        record_version,
                    )
                    .await?;

                    let res = split_helper::load(&tr, &None, &subspace, &primary_key_tuple).await;

                    // `1036` is `accessed_unreadable`, which means
                    // "Read or wrote an unreadable key"
                    assert_eq!(res, Err(FdbError::new(1036)),);

                    Ok(())
                })
                .await;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .run(|tr| async move {
                    let subspace = Some(Subspace::new(Bytes::new()).subspace(&{
                        let tup: (&str, &str) = ("sub", "space");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t
                    }));

                    let primary_key_tuple = {
                        let tup: (&str, &str, &str) = ("primary", "key", "save_load_complete");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<String>(tup.2.to_string());
                        t
                    };

                    let serialized_bytes_orig = SHORT_STRING.deref().clone();

                    let record_version_orig = RecordVersion::from(Versionstamp::complete(
                        Bytes::from_static(b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A"),
                        16,
                    ));

                    split_helper::save(
                        &tr,
                        &None,
                        &subspace,
                        &primary_key_tuple,
                        serialized_bytes_orig,
                        record_version_orig.clone(),
                    )
                    .await?;

                    let (record_version, serialized_bytes) =
                        split_helper::load(&tr, &None, &subspace, &primary_key_tuple)
                            .await?
                            .unwrap();

                    assert_eq!(record_version, record_version_orig);
                    assert_eq!(serialized_bytes, SHORT_STRING.deref().clone());

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn load_exact_100kb() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .run(|tr| async move {
                    let subspace = Some(Subspace::new(Bytes::new()).subspace(&{
                        let tup: (&str, &str) = ("sub", "space");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t
                    }));

                    let primary_key_tuple = {
                        let tup: (&str, &str, &str) = ("primary", "key", "exact_100kb");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<String>(tup.2.to_string());
                        t
                    };

                    let (_, serialized_bytes) =
                        split_helper::load(&tr, &None, &subspace, &primary_key_tuple)
                            .await?
                            .unwrap();

                    assert_eq!(
                        serialized_bytes,
                        LONG_STRING.deref().clone().copy_to_bytes(SPLIT_RECORD_SIZE)
                    );

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn load_exact_200kb() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .run(|tr| async move {
                    let subspace = Some(Subspace::new(Bytes::new()).subspace(&{
                        let tup: (&str, &str) = ("sub", "space");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t
                    }));

                    let primary_key_tuple = {
                        let tup: (&str, &str, &str) = ("primary", "key", "exact_200kb");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<String>(tup.2.to_string());
                        t
                    };

                    let (_, serialized_bytes) =
                        split_helper::load(&tr, &None, &subspace, &primary_key_tuple)
                            .await?
                            .unwrap();

                    assert_eq!(
                        serialized_bytes,
                        VERY_LONG_STRING
                            .deref()
                            .clone()
                            .copy_to_bytes(SPLIT_RECORD_SIZE * 2)
                    );

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn load_no_subspace() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .run(|tr| async move {
                    let primary_key_tuple_short = {
                        let tup: (&str, &str, &str) = ("primary", "key", "short");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<String>(tup.2.to_string());
                        t
                    };

                    let (_, serialized_bytes) =
                        split_helper::load(&tr, &None, &None, &primary_key_tuple_short)
                            .await?
                            .unwrap();

                    assert_eq!(serialized_bytes, SHORT_STRING.deref().clone());

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}
