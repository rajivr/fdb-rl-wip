#![allow(non_snake_case)]

use bytes::Bytes;

use fdb::database::FdbDatabase;
use fdb::range::Range;
use fdb::subspace::Subspace;
use fdb::transaction::Transaction;
use fdb::tuple::Tuple;

use fdb_rl::cursor::{CursorError, KeyValueCursorBuilder, NoNextReason};
use fdb_rl::range::TupleRange;
use fdb_rl::scan::ScanPropertiesBuilder;

use libtest_mimic::{Arguments, Failed, Trial};

use tokio::runtime::Builder;

use std::convert::TryFrom;
use std::env;
use std::error::Error;

static mut FDB_DATABASE: Option<FdbDatabase> = None;

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

    let tests = vec![Trial::test(
        "keyvaluecursor_into_btreemap",
        keyvaluecursor_into_btreemap,
    )];

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

                    let subspace = Subspace::new(Bytes::new()).subspace(&{
                        let tup: (&str, &str) = ("sub", "space");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t
                    });

                    let iter = (0..5).map(|i| (0..5).map(move |j| (i, j))).flatten();

                    for (i, j) in iter {
                        let tuple = {
                            let tup: (i8, i8) = (i, j);

                            let mut t = Tuple::new();
                            t.push_back::<i8>(tup.0);
                            t.push_back::<i8>(tup.1);
                            t
                        };

                        let key = subspace.subspace(&tuple).pack();
                        let value = tuple.pack();

                        tr.set(key, value);
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

fn keyvaluecursor_into_btreemap() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    let kv_cursor = {
                        let mut kv_cursor_builder = KeyValueCursorBuilder::new();

                        kv_cursor_builder
                            .subspace(Subspace::new(Bytes::new()).subspace(&{
                                let tup: (&str, &str) = ("sub", "space");

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t.push_back::<String>(tup.1.to_string());
                                t
                            }))
                            .key_range(TupleRange::all().into_key_range(&None))
                            .scan_properties(ScanPropertiesBuilder::default().build());

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (kv_btree, err) = kv_cursor.into_btreemap().await;

                    let iter = (0..5)
                        .map(|i| (0..5).map(move |j| (i, j)))
                        .flatten()
                        .zip(kv_btree.iter());

                    for ((i, j), (key_ref, value_ref)) in iter {
                        let tuple = {
                            let tup: (i8, i8) = (i, j);

                            let mut t = Tuple::new();
                            t.push_back::<i8>(tup.0);
                            t.push_back::<i8>(tup.1);
                            t
                        };

                        assert_eq!(tuple, Tuple::try_from(key_ref.clone()).unwrap());
                        assert_eq!(tuple, Tuple::try_from(value_ref.clone()).unwrap());
                    }

                    assert!(matches!(
                        err,
                        CursorError::NoNextReason(NoNextReason::SourceExhausted(_))
                    ));

                    if let CursorError::NoNextReason(NoNextReason::SourceExhausted(continuation)) =
                        err
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
