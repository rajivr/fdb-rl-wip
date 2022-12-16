#![allow(non_snake_case)]

use bytes::Bytes;

use fdb::database::FdbDatabase;
use fdb::range::Range;
use fdb::subspace::Subspace;
use fdb::transaction::Transaction;
use fdb::tuple::Tuple;

use fdb_rl::cursor::{Cursor, CursorError, KeyValueCursorBuilder, NoNextReason};
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

    let tests = vec![
        Trial::test("cursor_collect_non_empty", cursor_collect_non_empty),
        Trial::test("cursor_collect_empty", cursor_collect_empty),
        Trial::test("cursor_filter_non_empty", cursor_filter_non_empty),
        Trial::test("cursor_filter_empty", cursor_filter_empty),
        Trial::test("cursor_map_non_empty", cursor_map_non_empty),
        Trial::test("cursor_map_empty", cursor_map_empty),
        // Trial::test(
        //     "cursor_error_no_next_reason_keyvalue_limit",
        //     cursor_error_no_next_reason_keyvalue_limit,
        // ),
        // Trial::test(
        //     "cursor_error_no_next_reason_byte_limit",
        //     cursor_error_no_next_reason_byte_limit,
        // ),
        // Trial::test(
        //     "cursor_error_no_next_reason_time_limit",
        //     cursor_error_no_next_reason_time_limit,
        // ),
        // Trial::test(
        //     "cursor_error_no_next_reason_return_limit",
        //     cursor_error_no_next_reason_return_limit,
        // ),
        // Trial::test(
        //     "cursor_error_no_next_reason_source_exhausted",
        //     cursor_error_no_next_reason_source_exhausted,
        // ),
        // Trial::test(
        //     "cursor_error_multiple_next_invocation",
        //     cursor_error_multiple_next_invocation,
        // ),
        // // Tests below are adapted from RecordLayer's
        // // `KeyValueCursorTest.java`.
        // //
        // //  - We do not have `limiterWithLookahead()` test as we do
        // //    not implement `.skip` method on our `Cursor`.
        // //
        // //  - We do not have `emptyScanSplit` as where is no
        // //    implementation of `SplitHelper` yet.
        // //
        // //  - We do not have `buildWithoutRequiredProperties`, because
        // //    in our case `context` *must* be passed to `build`
        // //    method. So, this test does not make sense.
        // Trial::test("cursor_all", cursor_all),
        // Trial::test("cursor_begins_with", cursor_begins_with),
        // Trial::test("cursor_inclusive_range", cursor_inclusive_range),
        // Trial::test("cursor_exclusive_range", cursor_exclusive_range),
        // Trial::test("cursor_inclusive_null", cursor_inclusive_null),
        // Trial::test("cursor_exclusive_null", cursor_exclusive_null),
        // Trial::test("cursor_no_next_reasons", cursor_no_next_reasons),
        // Trial::test(
        //     "cursor_simple_keyvalue_scan_limit",
        //     cursor_simple_keyvalue_scan_limit,
        // ),
        // Trial::test(
        //     "cursor_keyvalue_limit_not_reached",
        //     cursor_keyvalue_limit_not_reached,
        // ),
        // Trial::test("cursor_shared_limiter", cursor_shared_limiter),
        // Trial::test("cursor_empty_scan", cursor_empty_scan),
        // Trial::test(
        //     "cursor_build_without_scan_properties",
        //     cursor_build_without_scan_properties,
        // ),
        // Trial::test(
        //     "cursor_build_with_required_properties",
        //     cursor_build_with_required_properties,
        // ),
        // // Add tests without subspace
        // Trial::test("cursor_no_subspace_all", cursor_no_subspace_all),
        // Trial::test(
        //     "cursor_no_subspace_begins_with",
        //     cursor_no_subspace_begins_with,
        // ),
        // Trial::test(
        //     "cursor_no_subspace_inclusive_range",
        //     cursor_no_subspace_inclusive_range,
        // ),
        // Trial::test(
        //     "cursor_no_subspace_exclusive_range",
        //     cursor_no_subspace_exclusive_range,
        // ),
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

fn cursor_collect_non_empty() -> Result<(), Failed> {
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

                    let (kvs_tup, err) = kv_cursor
                        .map(|kv| async {
                            let (key, value) = kv.into_parts();

                            // We expect a proper `Tuple`.
                            let key_tup = Tuple::try_from(key).unwrap();
                            let value_tup = Tuple::try_from(value).unwrap();

                            (key_tup, value_tup)
                        })
                        .await
                        .collect()
                        .await;

                    let iter = (0..5)
                        .map(|i| (0..5).map(move |j| (i, j)))
                        .flatten()
                        .zip(kvs_tup);

                    for ((i, j), (key_tup, value_tup)) in iter {
                        let tuple = {
                            let tup: (i8, i8) = (i, j);

                            let mut t = Tuple::new();
                            t.push_back::<i8>(tup.0);
                            t.push_back::<i8>(tup.1);
                            t
                        };

                        assert_eq!(tuple, key_tup);
                        assert_eq!(tuple, value_tup);
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

fn cursor_collect_empty() -> Result<(), Failed> {
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
                            .key_range(
                                // ("sub", "space", 5,) would be
                                // empty.
                                TupleRange::all_of({
                                    let tup: (i8,) = (5,);

                                    let mut t = Tuple::new();
                                    t.push_back::<i8>(tup.0);
                                    t
                                })
                                .into_key_range(&None),
                            )
                            .scan_properties(ScanPropertiesBuilder::default().build());

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (kvs, err) = kv_cursor.collect().await;

                    assert_eq!(0, kvs.len());

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

fn cursor_filter_non_empty() -> Result<(), Failed> {
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

                    let (kvs_tup, err) = kv_cursor
                        .map(|kv| async {
                            let (key, value) = kv.into_parts();

                            // We expect a proper `Tuple`.
                            let key_tup = Tuple::try_from(key).unwrap();
                            let value_tup = Tuple::try_from(value).unwrap();

                            (key_tup, value_tup)
                        })
                        .await
                        .filter(|x| {
                            let value_tup = x.1.clone();
                            async move {
                                // Filter the second element of
                                // `value_tup` and ignore the the
                                // rest.
                                if value_tup.get::<i8>(1).unwrap() == 0 {
                                    true
                                } else {
                                    false
                                }
                            }
                        })
                        .await
                        .collect()
                        .await;

                    let iter = (0..5).zip(kvs_tup);

                    for (i, (key_tup, value_tup)) in iter {
                        let tuple = {
                            let tup: (i8, i8) = (i, 0);

                            let mut t = Tuple::new();
                            t.push_back::<i8>(tup.0);
                            t.push_back::<i8>(tup.1);
                            t
                        };

                        assert_eq!(tuple, key_tup);
                        assert_eq!(tuple, value_tup);
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

fn cursor_filter_empty() -> Result<(), Failed> {
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
                            .key_range(
                                // ("sub", "space", 5,) would be
                                // empty.
                                TupleRange::all_of({
                                    let tup: (i8,) = (5,);

                                    let mut t = Tuple::new();
                                    t.push_back::<i8>(tup.0);
                                    t
                                })
                                .into_key_range(&None),
                            )
                            .scan_properties(ScanPropertiesBuilder::default().build());

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (kvs_tup, err) = kv_cursor
                        .map(|kv| async {
                            let (key, value) = kv.into_parts();

                            // We expect a proper `Tuple`.
                            let key_tup = Tuple::try_from(key).unwrap();
                            let value_tup = Tuple::try_from(value).unwrap();

                            (key_tup, value_tup)
                        })
                        .await
                        .filter(|x| {
                            let value_tup = x.1.clone();
                            async move {
                                // Filter the second element of
                                // `value_tup` and ignore the the
                                // rest.
                                if value_tup.get::<i8>(1).unwrap() == 0 {
                                    true
                                } else {
                                    false
                                }
                            }
                        })
                        .await
                        .collect()
                        .await;

                    assert_eq!(0, kvs_tup.len());

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

// Same as `cursor_collect_non_empty`.
fn cursor_map_non_empty() -> Result<(), Failed> {
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

                    let (kvs_tup, err) = kv_cursor
                        .map(|kv| async {
                            let (key, value) = kv.into_parts();

                            // We expect a proper `Tuple`.
                            let key_tup = Tuple::try_from(key).unwrap();
                            let value_tup = Tuple::try_from(value).unwrap();

                            (key_tup, value_tup)
                        })
                        .await
                        .collect()
                        .await;

                    let iter = (0..5)
                        .map(|i| (0..5).map(move |j| (i, j)))
                        .flatten()
                        .zip(kvs_tup);

                    for ((i, j), (key_tup, value_tup)) in iter {
                        let tuple = {
                            let tup: (i8, i8) = (i, j);

                            let mut t = Tuple::new();
                            t.push_back::<i8>(tup.0);
                            t.push_back::<i8>(tup.1);
                            t
                        };

                        assert_eq!(tuple, key_tup);
                        assert_eq!(tuple, value_tup);
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

fn cursor_map_empty() -> Result<(), Failed> {
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
                            .key_range(
                                // ("sub", "space", 5,) would be
                                // empty.
                                TupleRange::all_of({
                                    let tup: (i8,) = (5,);

                                    let mut t = Tuple::new();
                                    t.push_back::<i8>(tup.0);
                                    t
                                })
                                .into_key_range(&None),
                            )
                            .scan_properties(ScanPropertiesBuilder::default().build());

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (kvs_tup, err) = kv_cursor
                        .map(|kv| async {
                            let (key, value) = kv.into_parts();

                            // We expect a proper `Tuple`.
                            let key_tup = Tuple::try_from(key).unwrap();
                            let value_tup = Tuple::try_from(value).unwrap();

                            (key_tup, value_tup)
                        })
                        .await
                        .collect()
                        .await;

                    assert_eq!(0, kvs_tup.len());

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
