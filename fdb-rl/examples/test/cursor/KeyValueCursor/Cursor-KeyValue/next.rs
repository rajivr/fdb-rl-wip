#![allow(non_snake_case)]

use bytes::Bytes;

use fdb::database::FdbDatabase;
use fdb::error::FdbError;
use fdb::range::Range;
use fdb::subspace::Subspace;
use fdb::transaction::Transaction;
use fdb::tuple::{Null, Tuple};

use fdb_rl::cursor::{Cursor, CursorError, KeyValueCursorBuilder, NoNextReason};
use fdb_rl::error::CURSOR_KEYVALUE_CURSOR_BUILDER_ERROR;
use fdb_rl::range::{TupleHighEndpoint, TupleLowEndpoint, TupleRange};
use fdb_rl::scan::{
    ByteScanLimiter, KeyValueScanLimiter, ScanLimiter, ScanPropertiesBuilder, TimeScanLimiter,
};

use libtest_mimic::{Arguments, Failed, Trial};

use tokio::runtime::Builder;
use tokio::time::sleep;

use std::convert::TryFrom;
use std::env;
use std::error::Error;
use std::time::Duration;

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
        Trial::test(
            "cursor_error_no_next_reason_keyvalue_limit",
            cursor_error_no_next_reason_keyvalue_limit,
        ),
        Trial::test(
            "cursor_error_no_next_reason_byte_limit",
            cursor_error_no_next_reason_byte_limit,
        ),
        Trial::test(
            "cursor_error_no_next_reason_time_limit",
            cursor_error_no_next_reason_time_limit,
        ),
        Trial::test(
            "cursor_error_no_next_reason_return_limit",
            cursor_error_no_next_reason_return_limit,
        ),
        Trial::test(
            "cursor_error_no_next_reason_source_exhausted",
            cursor_error_no_next_reason_source_exhausted,
        ),
        Trial::test(
            "cursor_error_multiple_next_invocation",
            cursor_error_multiple_next_invocation,
        ),
        // Tests below are adapted from RecordLayer's
        // `KeyValueCursorTest.java`.
        //
        //  - We do not have `limiterWithLookahead()` test as we do
        //    not implement `.skip` method on our `Cursor`.
        //
        //  - We do not have `emptyScanSplit` as where is no
        //    implementation of `SplitHelper` yet.
        //
        //  - We do not have `buildWithoutRequiredProperties`, because
        //    in our case `context` *must* be passed to `build`
        //    method. So, this test does not make sense.
        Trial::test("cursor_all", cursor_all),
        Trial::test("cursor_begins_with", cursor_begins_with),
        Trial::test("cursor_inclusive_range", cursor_inclusive_range),
        Trial::test("cursor_exclusive_range", cursor_exclusive_range),
        // See this [1] comment regarding `cursor_inclusive_null` and
        // `cursor_exclusive_null`.
        //
        // [1]: https://forums.foundationdb.org/t/record-layer-design-questions/3468/11
        Trial::test("cursor_inclusive_null", cursor_inclusive_null),
        Trial::test("cursor_exclusive_null", cursor_exclusive_null),
        Trial::test("cursor_no_next_reasons", cursor_no_next_reasons),
        Trial::test(
            "cursor_simple_keyvalue_scan_limit",
            cursor_simple_keyvalue_scan_limit,
        ),
        Trial::test(
            "cursor_keyvalue_limit_not_reached",
            cursor_keyvalue_limit_not_reached,
        ),
        Trial::test("cursor_shared_limiter", cursor_shared_limiter),
        Trial::test("cursor_empty_scan", cursor_empty_scan),
        Trial::test(
            "cursor_build_without_scan_properties",
            cursor_build_without_scan_properties,
        ),
        Trial::test(
            "cursor_build_with_required_properties",
            cursor_build_with_required_properties,
        ),
        // Add tests without subspace
        Trial::test("cursor_no_subspace_all", cursor_no_subspace_all),
        Trial::test(
            "cursor_no_subspace_begins_with",
            cursor_no_subspace_begins_with,
        ),
        Trial::test(
            "cursor_no_subspace_inclusive_range",
            cursor_no_subspace_inclusive_range,
        ),
        Trial::test(
            "cursor_no_subspace_exclusive_range",
            cursor_no_subspace_exclusive_range,
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

fn cursor_error_no_next_reason_keyvalue_limit() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    // KeyValue limit of zero (out-of-band).
                    {
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
                                .scan_properties({
                                    let mut scan_properites_builder =
                                        ScanPropertiesBuilder::default();
                                    scan_properites_builder.set_scan_limiter(ScanLimiter::new(
                                        Some(KeyValueScanLimiter::enforcing(0)),
                                        None,
                                        None,
                                    ));
                                    scan_properites_builder.build()
                                });

                            kv_cursor_builder.build(&tr)
                        }?;

                        let (kvs, err) = kv_cursor.collect().await;

                        assert_eq!(kvs.len(), 0);

                        assert!(matches!(
                            err,
                            CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(_))
                        ));

                        if let CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(
                            continuation,
                        )) = err
                        {
                            assert!(continuation.is_begin_marker());
                        }
                    }

                    // KeyValue limit of non-zero (out-of-band).
                    {
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
                                .scan_properties({
                                    let mut scan_properites_builder =
                                        ScanPropertiesBuilder::default();
                                    scan_properites_builder.set_scan_limiter(ScanLimiter::new(
                                        Some(KeyValueScanLimiter::enforcing(2)),
                                        None,
                                        None,
                                    ));
                                    scan_properites_builder.build()
                                });

                            kv_cursor_builder.build(&tr)
                        }?;

                        let (kvs, err) = kv_cursor.collect().await;

                        assert_eq!(kvs.len(), 2);

                        assert!(matches!(
                            err,
                            CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(_))
                        ));

                        if let CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(
                            continuation,
                        )) = err
                        {
                            assert!(!continuation.is_begin_marker());
                            assert!(!continuation.is_end_marker());
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

fn cursor_error_no_next_reason_byte_limit() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    // Byte limit of zero (out-of-band).
                    {
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
                                .scan_properties({
                                    let mut scan_properites_builder =
                                        ScanPropertiesBuilder::default();
                                    scan_properites_builder.set_scan_limiter(ScanLimiter::new(
                                        None,
                                        Some(ByteScanLimiter::enforcing(0)),
                                        None,
                                    ));
                                    scan_properites_builder.build()
                                });

                            kv_cursor_builder.build(&tr)
                        }?;

                        let (kvs, err) = kv_cursor.collect().await;

                        assert_eq!(kvs.len(), 0);

                        assert!(matches!(
                            err,
                            CursorError::NoNextReason(NoNextReason::ByteLimitReached(_))
                        ));

                        if let CursorError::NoNextReason(NoNextReason::ByteLimitReached(
                            continuation,
                        )) = err
                        {
                            assert!(continuation.is_begin_marker());
                        }
                    }

                    // Byte limit of non-zero (out-of-band).
                    {
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
                                .scan_properties({
                                    let mut scan_properites_builder =
                                        ScanPropertiesBuilder::default();
                                    scan_properites_builder.set_scan_limiter(ScanLimiter::new(
                                        None,
                                        Some(ByteScanLimiter::enforcing(10)),
                                        None,
                                    ));
                                    scan_properites_builder.build()
                                });

                            kv_cursor_builder.build(&tr)
                        }?;

                        let (kvs, err) = kv_cursor.collect().await;

                        assert_eq!(kvs.len(), 1);

                        assert!(matches!(
                            err,
                            CursorError::NoNextReason(NoNextReason::ByteLimitReached(_))
                        ));

                        if let CursorError::NoNextReason(NoNextReason::ByteLimitReached(
                            continuation,
                        )) = err
                        {
                            assert!(!continuation.is_begin_marker());
                            assert!(!continuation.is_end_marker());
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

fn cursor_error_no_next_reason_time_limit() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().enable_time().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    // Time limit of zero (out-of-band).
                    {
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
                                .scan_properties({
                                    let mut scan_properites_builder =
                                        ScanPropertiesBuilder::default();
                                    scan_properites_builder.set_scan_limiter(ScanLimiter::new(
                                        None,
                                        None,
                                        Some(TimeScanLimiter::new(0)),
                                    ));
                                    scan_properites_builder.build()
                                });

                            kv_cursor_builder.build(&tr)
                        }?;

                        let (kvs, err) = kv_cursor.collect().await;

                        assert_eq!(kvs.len(), 0);

                        assert!(matches!(
                            err,
                            CursorError::NoNextReason(NoNextReason::TimeLimitReached(_))
                        ));

                        if let CursorError::NoNextReason(NoNextReason::TimeLimitReached(
                            continuation,
                        )) = err
                        {
                            assert!(continuation.is_begin_marker());
                        }
                    }

                    // Time limit of non-zero (out-of-band).
                    {
                        let mut kv_cursor = {
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
                                .scan_properties({
                                    let mut scan_properites_builder =
                                        ScanPropertiesBuilder::default();
                                    scan_properites_builder.set_scan_limiter(ScanLimiter::new(
                                        None,
                                        None,
                                        Some(TimeScanLimiter::new(200)),
                                    ));
                                    scan_properites_builder.build()
                                });

                            kv_cursor_builder.build(&tr)
                        }?;

                        let res = kv_cursor.next().await;

                        // Here we assume we were able to get one
                        // result within 200ms.
                        assert!(matches!(res, Ok(_)));

                        // Sleep for 210ms.
                        sleep(Duration::from_millis(210)).await;

                        let res = kv_cursor.next().await;

                        assert!(matches!(
                            res,
                            Err(CursorError::NoNextReason(NoNextReason::TimeLimitReached(_)))
                        ));

                        if let Err(CursorError::NoNextReason(NoNextReason::TimeLimitReached(
                            continuation,
                        ))) = res
                        {
                            assert!(!continuation.is_begin_marker());
                            assert!(!continuation.is_end_marker());
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

fn cursor_error_no_next_reason_return_limit() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    // Setting in-band limit to zero means unlimited.
                    {
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
                                .scan_properties({
                                    let mut scan_properites_builder =
                                        ScanPropertiesBuilder::default();
                                    unsafe {
                                        scan_properites_builder.set_range_options(
                                            |range_options| range_options.set_limit(0),
                                        );
                                    }
                                    scan_properites_builder.build()
                                });

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

                        if let CursorError::NoNextReason(NoNextReason::SourceExhausted(
                            continuation,
                        )) = err
                        {
                            assert!(continuation.is_end_marker());
                        }
                    }

                    // Setting in-band limit to negative number means
                    // unlimited.
                    {
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
                                .scan_properties({
                                    let mut scan_properites_builder =
                                        ScanPropertiesBuilder::default();
                                    unsafe {
                                        scan_properites_builder.set_range_options(
                                            |range_options| range_options.set_limit(-1),
                                        );
                                    }
                                    scan_properites_builder.build()
                                });

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

                        if let CursorError::NoNextReason(NoNextReason::SourceExhausted(
                            continuation,
                        )) = err
                        {
                            assert!(continuation.is_end_marker());
                        }
                    }

                    // Set in-band limit of `2` and verify that we get
                    // `ReturnLimitReached`. Then take the
                    // continuation, build a new cursor with that
                    // continuation, and set the in-band limit to
                    // `1`. Verify that we get `ReturnLimitReached`.
                    {
                        let mut kv_cursor = {
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
                                .scan_properties({
                                    let mut scan_properites_builder =
                                        ScanPropertiesBuilder::default();
                                    unsafe {
                                        scan_properites_builder.set_range_options(
                                            |range_options| range_options.set_limit(2),
                                        );
                                    }
                                    scan_properites_builder.build()
                                });

                            kv_cursor_builder.build(&tr)
                        }?;

                        // Check the first two `KeyValues`.
                        for j in 0..=1 {
                            let tuple = {
                                let tup: (i8, i8) = (0, j);

                                let mut t = Tuple::new();
                                t.push_back::<i8>(tup.0);
                                t.push_back::<i8>(tup.1);
                                t
                            };

                            // We expect
                            // `CursorSuccess<KeyValue>`.
                            let (key, value) =
                                kv_cursor.next().await.unwrap().into_value().into_parts();
                            // We expect a proper `Tuple`.
                            assert_eq!(tuple, Tuple::try_from(key).unwrap());
                            assert_eq!(tuple, Tuple::try_from(value).unwrap());
                        }

                        let res = kv_cursor.next().await;

                        assert!(matches!(
                            res,
                            Err(CursorError::NoNextReason(NoNextReason::ReturnLimitReached(
                                _
                            )))
                        ));

                        // Extract out the continuation
                        let continuation_bytes = if let Err(CursorError::NoNextReason(
                            NoNextReason::ReturnLimitReached(continuation),
                        )) = res
                        {
                            assert!(!continuation.is_begin_marker());
                            assert!(!continuation.is_end_marker());
                            continuation.to_bytes().unwrap()
                        } else {
                            panic!("Found Ok(_) variant, instead of Err(_) variant");
                        };

                        // Build new cursor with continuation and limit 1
                        let mut kv_cursor = {
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
                                .continuation(continuation_bytes)
                                .scan_properties({
                                    let mut scan_properites_builder =
                                        ScanPropertiesBuilder::default();
                                    unsafe {
                                        scan_properites_builder.set_range_options(
                                            |range_options| range_options.set_limit(1),
                                        );
                                    }
                                    scan_properites_builder.build()
                                });

                            kv_cursor_builder.build(&tr)
                        }?;

                        let res = kv_cursor.next().await;

                        // Check the third `KeyValue`
                        let tuple = {
                            let tup: (i8, i8) = (0, 2);

                            let mut t = Tuple::new();
                            t.push_back::<i8>(tup.0);
                            t.push_back::<i8>(tup.1);
                            t
                        };

                        // We expect `CursorSuccess<KeyValue>`.
                        let (key, value) = res.unwrap().into_value().into_parts();
                        // We expect a proper `Tuple`.
                        assert_eq!(tuple, Tuple::try_from(key).unwrap());
                        assert_eq!(tuple, Tuple::try_from(value).unwrap());

                        // Check that we get another
                        // `ReturnLimitReached`.

                        let res = kv_cursor.next().await;

                        assert!(matches!(
                            res,
                            Err(CursorError::NoNextReason(NoNextReason::ReturnLimitReached(
                                _
                            )))
                        ));
                    }

                    // `ReturnLimit` takes precedence over
                    // `SourceExhausted`.
                    {
                        let mut kv_cursor = {
                            // The range would be `[(4, 3), (4, 4)]`.
                            let tuple_range = TupleRange::new(
                                TupleLowEndpoint::RangeInclusive({
                                    let tup: (i8, i8) = (4, 3);

                                    let mut t = Tuple::new();
                                    t.push_back::<i8>(tup.0);
                                    t.push_back::<i8>(tup.1);
                                    t
                                }),
                                TupleHighEndpoint::End,
                            );

                            let mut kv_cursor_builder = KeyValueCursorBuilder::new();

                            kv_cursor_builder
                                .subspace(Subspace::new(Bytes::new()).subspace(&{
                                    let tup: (&str, &str) = ("sub", "space");

                                    let mut t = Tuple::new();
                                    t.push_back::<String>(tup.0.to_string());
                                    t.push_back::<String>(tup.1.to_string());
                                    t
                                }))
                                .key_range(tuple_range.into_key_range(&None))
                                .scan_properties({
                                    let mut scan_properites_builder =
                                        ScanPropertiesBuilder::default();
                                    unsafe {
                                        scan_properites_builder.set_range_options(
                                            |range_options| range_options.set_limit(2),
                                        );
                                    }
                                    scan_properites_builder.build()
                                });

                            kv_cursor_builder.build(&tr)
                        }?;

                        let res = kv_cursor.next().await;

                        // Check the first `KeyValue`
                        let tuple = {
                            let tup: (i8, i8) = (4, 3);

                            let mut t = Tuple::new();
                            t.push_back::<i8>(tup.0);
                            t.push_back::<i8>(tup.1);
                            t
                        };

                        // We expect `CursorSuccess<KeyValue>`.
                        let (key, value) = res.unwrap().into_value().into_parts();
                        // We expect a proper `Tuple`.
                        assert_eq!(tuple, Tuple::try_from(key).unwrap());
                        assert_eq!(tuple, Tuple::try_from(value).unwrap());

                        let res = kv_cursor.next().await;

                        // Check the second `KeyValue`
                        let tuple = {
                            let tup: (i8, i8) = (4, 4);

                            let mut t = Tuple::new();
                            t.push_back::<i8>(tup.0);
                            t.push_back::<i8>(tup.1);
                            t
                        };

                        // We expect `CursorSuccess<KeyValue>`.
                        let (key, value) = res.unwrap().into_value().into_parts();
                        // We expect a proper `Tuple`.
                        assert_eq!(tuple, Tuple::try_from(key).unwrap());
                        assert_eq!(tuple, Tuple::try_from(value).unwrap());

                        let res = kv_cursor.next().await;

                        // Under normal circumstances, we should see
                        // `SourceExhaused`. However, because the
                        // return limit has been reached, that takes
                        // precedence and we get `ReturnLimitReached`
                        // instead.
                        assert!(matches!(
                            res,
                            Err(CursorError::NoNextReason(NoNextReason::ReturnLimitReached(
                                _
                            )))
                        ));

                        // Extract out the continuation
                        let continuation_bytes = if let Err(CursorError::NoNextReason(
                            NoNextReason::ReturnLimitReached(continuation),
                        )) = res
                        {
                            assert!(!continuation.is_begin_marker());
                            assert!(!continuation.is_end_marker());
                            continuation.to_bytes().unwrap()
                        } else {
                            panic!("Found Ok(_) variant, instead of Err(_) variant");
                        };

                        // Build a new cursor with continuation and
                        // limit 1 and verify that we now get
                        // `SourceExhausted`.
                        let mut kv_cursor = {
                            // The range would be `[(4, 3), (4, 4)]`.
                            let tuple_range = TupleRange::new(
                                TupleLowEndpoint::RangeInclusive({
                                    let tup: (i8, i8) = (4, 3);

                                    let mut t = Tuple::new();
                                    t.push_back::<i8>(tup.0);
                                    t.push_back::<i8>(tup.1);
                                    t
                                }),
                                TupleHighEndpoint::End,
                            );

                            let mut kv_cursor_builder = KeyValueCursorBuilder::new();

                            kv_cursor_builder
                                .subspace(Subspace::new(Bytes::new()).subspace(&{
                                    let tup: (&str, &str) = ("sub", "space");

                                    let mut t = Tuple::new();
                                    t.push_back::<String>(tup.0.to_string());
                                    t.push_back::<String>(tup.1.to_string());
                                    t
                                }))
                                .key_range(tuple_range.into_key_range(&None))
                                .continuation(continuation_bytes)
                                .scan_properties({
                                    let mut scan_properites_builder =
                                        ScanPropertiesBuilder::default();
                                    unsafe {
                                        scan_properites_builder.set_range_options(
                                            |range_options| range_options.set_limit(1),
                                        );
                                    }
                                    scan_properites_builder.build()
                                });

                            kv_cursor_builder.build(&tr)
                        }?;

                        let res = kv_cursor.next().await;

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
                    }

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_error_no_next_reason_source_exhausted() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().enable_time().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    // Empty range `("sub", "space", 5,)` should
                    // immediately return `SourceExhausted`.
                    {
                        let mut kv_cursor = {
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
                                    TupleRange::all_of(&{
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

                        let res = kv_cursor.next().await;

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
                    }

                    // `SourceExhausted` should be returned at the end
                    // of a forward scan.
                    {
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
                                    // ("sub", "space", 4,)
                                    TupleRange::all_of(&{
                                        let tup: (i8,) = (4,);

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

                        let iter = (0..5).zip(kvs_tup);

                        for (j, (key_tup, value_tup)) in iter {
                            let tuple = {
                                let tup: (i8, i8) = (4, j);

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

                        // Extract out the continuation
                        let continuation_bytes = if let CursorError::NoNextReason(
                            NoNextReason::SourceExhausted(continuation),
                        ) = err
                        {
                            assert!(continuation.is_end_marker());
                            continuation.to_bytes().unwrap()
                        } else {
                            panic!("Found Ok(_) variant, instead of Err(_) variant");
                        };

                        // Build new cursor with the extracted continuation.
                        let mut kv_cursor = {
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
                                    // ("sub", "space", 4,)
                                    TupleRange::all_of(&{
                                        let tup: (i8,) = (4,);

                                        let mut t = Tuple::new();
                                        t.push_back::<i8>(tup.0);
                                        t
                                    })
                                    .into_key_range(&None),
                                )
                                .continuation(continuation_bytes)
                                .scan_properties(ScanPropertiesBuilder::default().build());

                            kv_cursor_builder.build(&tr)
                        }?;

                        let res = kv_cursor.next().await;

                        assert!(matches!(
                            res,
                            Err(CursorError::NoNextReason(NoNextReason::SourceExhausted(_)))
                        ));
                    }

                    // `SourceExhausted` should be returned at the end
                    // of a reverse scan.
                    {
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
                                    // ("sub", "space", 3,)
                                    TupleRange::all_of(&{
                                        let tup: (i8,) = (3,);

                                        let mut t = Tuple::new();
                                        t.push_back::<i8>(tup.0);
                                        t
                                    })
                                    .into_key_range(&None),
                                )
                                .scan_properties({
                                    let mut scan_properites_builder =
                                        ScanPropertiesBuilder::default();
                                    unsafe {
                                        scan_properites_builder.set_range_options(
                                            |range_options| range_options.set_reverse(true),
                                        );
                                    }
                                    scan_properites_builder.build()
                                });

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

                        let iter = (0..5).rev().zip(kvs_tup);

                        for (j, (key_tup, value_tup)) in iter {
                            let tuple = {
                                let tup: (i8, i8) = (3, j);

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

                        // Extract out the continuation
                        let continuation_bytes = if let CursorError::NoNextReason(
                            NoNextReason::SourceExhausted(continuation),
                        ) = err
                        {
                            assert!(continuation.is_end_marker());
                            continuation.to_bytes().unwrap()
                        } else {
                            panic!("Found Ok(_) variant, instead of Err(_) variant");
                        };

                        // Build new cursor with the extracted continuation.
                        let mut kv_cursor = {
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
                                    // ("sub", "space", 3,)
                                    TupleRange::all_of(&{
                                        let tup: (i8,) = (3,);

                                        let mut t = Tuple::new();
                                        t.push_back::<i8>(tup.0);
                                        t
                                    })
                                    .into_key_range(&None),
                                )
                                .continuation(continuation_bytes)
                                .scan_properties({
                                    let mut scan_properites_builder =
                                        ScanPropertiesBuilder::default();
                                    unsafe {
                                        scan_properites_builder.set_range_options(
                                            |range_options| range_options.set_reverse(true),
                                        );
                                    }
                                    scan_properites_builder.build()
                                });

                            kv_cursor_builder.build(&tr)
                        }?;

                        let res = kv_cursor.next().await;

                        assert!(matches!(
                            res,
                            Err(CursorError::NoNextReason(NoNextReason::SourceExhausted(_)))
                        ));
                    }

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

// Invoking `.next()` multiple times on a cursor which is in an error
// state, does not change its result.
fn cursor_error_multiple_next_invocation() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().enable_time().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    // Generate a error condition using KeyValue limit of 1.
                    let mut kv_cursor = {
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
                            .scan_properties({
                                let mut scan_properites_builder = ScanPropertiesBuilder::default();
                                scan_properites_builder.set_scan_limiter(ScanLimiter::new(
                                    Some(KeyValueScanLimiter::enforcing(1)),
                                    None,
                                    None,
                                ));
                                scan_properites_builder.build()
                            });

                        kv_cursor_builder.build(&tr)
                    }?;

                    let res = kv_cursor.next().await;

                    assert!(matches!(res, Ok(_)));

                    let res = kv_cursor.next().await;

                    assert!(matches!(
                        res,
                        Err(CursorError::NoNextReason(
                            NoNextReason::KeyValueLimitReached(_)
                        ))
                    ));

                    let continuation_bytes = if let Err(CursorError::NoNextReason(
                        NoNextReason::KeyValueLimitReached(continuation),
                    )) = res
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
                        continuation.to_bytes().unwrap()
                    } else {
                        panic!("Found Ok(_) variant, instead of Err(_) variant");
                    };

                    // Invoke `.next()` twice after error has been
                    // returned and verify that we get the same
                    // continuation.

                    let res = kv_cursor.next().await;

                    assert!(matches!(
                        res,
                        Err(CursorError::NoNextReason(
                            NoNextReason::KeyValueLimitReached(_)
                        ))
                    ));

                    if let Err(CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(
                        continuation,
                    ))) = res
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
                        assert_eq!(continuation_bytes, continuation.to_bytes().unwrap());
                    }

                    let res = kv_cursor.next().await;

                    assert!(matches!(
                        res,
                        Err(CursorError::NoNextReason(
                            NoNextReason::KeyValueLimitReached(_)
                        ))
                    ));

                    if let Err(CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(
                        continuation,
                    ))) = res
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
                        assert_eq!(continuation_bytes, continuation.to_bytes().unwrap());
                    }

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_all() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    {
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

                        if let CursorError::NoNextReason(NoNextReason::SourceExhausted(
                            continuation,
                        )) = err
                        {
                            assert!(continuation.is_end_marker());
                        }
                    }

                    {
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
                                .scan_properties({
                                    let mut scan_properites_builder =
                                        ScanPropertiesBuilder::default();
                                    unsafe {
                                        scan_properites_builder.set_range_options(
                                            |range_options| range_options.set_limit(10),
                                        );
                                    }
                                    scan_properites_builder.build()
                                });

                            kv_cursor_builder.build(&tr)
                        }?;

                        let (kvs_tup, err) = kv_cursor.collect().await;

                        assert_eq!(10, kvs_tup.len());

                        // Extract out the continuation
                        let continuation_bytes = if let CursorError::NoNextReason(
                            NoNextReason::ReturnLimitReached(continuation),
                        ) = err
                        {
                            assert!(!continuation.is_begin_marker());
                            assert!(!continuation.is_end_marker());
                            continuation.to_bytes().unwrap()
                        } else {
                            panic!("Invalid err");
                        };

                        // Build a new cursor with continuation

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
                                .continuation(continuation_bytes)
                                .scan_properties(ScanPropertiesBuilder::default().build());

                            kv_cursor_builder.build(&tr)
                        }?;

                        let (kvs_tup, _) = kv_cursor.collect().await;

                        assert_eq!(15, kvs_tup.len());
                    }

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_begins_with() -> Result<(), Failed> {
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
                                // ("sub", "space", 3,)
                                TupleRange::all_of(&{
                                    let tup: (i8,) = (3,);

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

                    let iter = (0..5).zip(kvs_tup);

                    for (j, (key_tup, value_tup)) in iter {
                        let tuple = {
                            let tup: (i8, i8) = (3, j);

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
                                // ("sub", "space", 3,)
                                TupleRange::all_of(&{
                                    let tup: (i8,) = (3,);

                                    let mut t = Tuple::new();
                                    t.push_back::<i8>(tup.0);
                                    t
                                })
                                .into_key_range(&None),
                            )
                            .scan_properties({
                                let mut scan_properites_builder = ScanPropertiesBuilder::default();
                                unsafe {
                                    scan_properites_builder.set_range_options(|range_options| {
                                        range_options.set_limit(2)
                                    });
                                }
                                scan_properites_builder.build()
                            });

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (kvs, err) = kv_cursor.collect().await;

                    assert_eq!(2, kvs.len());

                    // Extract out the continuation
                    let continuation_bytes = if let CursorError::NoNextReason(
                        NoNextReason::ReturnLimitReached(continuation),
                    ) = err
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
                        continuation.to_bytes().unwrap()
                    } else {
                        panic!("Incorrect NoNextReason");
                    };

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
                                // ("sub", "space", 3,)
                                TupleRange::all_of(&{
                                    let tup: (i8,) = (3,);

                                    let mut t = Tuple::new();
                                    t.push_back::<i8>(tup.0);
                                    t
                                })
                                .into_key_range(&None),
                            )
                            .continuation(continuation_bytes)
                            .scan_properties({
                                let mut scan_properites_builder = ScanPropertiesBuilder::default();
                                unsafe {
                                    scan_properites_builder.set_range_options(|range_options| {
                                        range_options.set_limit(3)
                                    });
                                }
                                scan_properites_builder.build()
                            });

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (kvs, _) = kv_cursor.collect().await;

                    assert_eq!(3, kvs.len());

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_inclusive_range() -> Result<(), Failed> {
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
                            .key_range({
                                // ("sub", "space", 3, 3)..=("sub", "space", 4, 2)
                                TupleRange::new(
                                    TupleLowEndpoint::RangeInclusive({
                                        let tup: (i8, i8) = (3, 3);

                                        let mut t = Tuple::new();
                                        t.push_back::<i8>(tup.0);
                                        t.push_back::<i8>(tup.1);
                                        t
                                    }),
                                    TupleHighEndpoint::RangeInclusive({
                                        let tup: (i8, i8) = (4, 2);

                                        let mut t = Tuple::new();
                                        t.push_back::<i8>(tup.0);
                                        t.push_back::<i8>(tup.1);
                                        t
                                    }),
                                )
                                .into_key_range(&None)
                            })
                            .scan_properties(ScanPropertiesBuilder::default().build());

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (values_tup, _) = kv_cursor
                        .map(|kv| async { Tuple::try_from(kv.into_value()).unwrap() })
                        .await
                        .collect()
                        .await;

                    let iter = vec![(3, 3), (3, 4), (4, 0), (4, 1), (4, 2)]
                        .into_iter()
                        .zip(values_tup);

                    for ((i, j), value_tup) in iter {
                        let tuple = {
                            let tup: (i8, i8) = (i, j);

                            let mut t = Tuple::new();
                            t.push_back::<i8>(tup.0);
                            t.push_back::<i8>(tup.1);
                            t
                        };

                        assert_eq!(tuple, value_tup);
                    }

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
                            .key_range({
                                // ("sub", "space", 3, 3)..=("sub", "space", 4, 2)
                                TupleRange::new(
                                    TupleLowEndpoint::RangeInclusive({
                                        let tup: (i8, i8) = (3, 3);

                                        let mut t = Tuple::new();
                                        t.push_back::<i8>(tup.0);
                                        t.push_back::<i8>(tup.1);
                                        t
                                    }),
                                    TupleHighEndpoint::RangeInclusive({
                                        let tup: (i8, i8) = (4, 2);

                                        let mut t = Tuple::new();
                                        t.push_back::<i8>(tup.0);
                                        t.push_back::<i8>(tup.1);
                                        t
                                    }),
                                )
                                .into_key_range(&None)
                            })
                            .scan_properties({
                                let mut scan_properites_builder = ScanPropertiesBuilder::default();
                                unsafe {
                                    scan_properites_builder.set_range_options(|range_options| {
                                        range_options.set_limit(2)
                                    });
                                }
                                scan_properites_builder.build()
                            });

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (values_tup, err) = kv_cursor
                        .map(|kv| async { Tuple::try_from(kv.into_value()).unwrap() })
                        .await
                        .collect()
                        .await;

                    let iter = vec![(3, 3), (3, 4)].into_iter().zip(values_tup);

                    for ((i, j), value_tup) in iter {
                        let tuple = {
                            let tup: (i8, i8) = (i, j);

                            let mut t = Tuple::new();
                            t.push_back::<i8>(tup.0);
                            t.push_back::<i8>(tup.1);
                            t
                        };

                        assert_eq!(tuple, value_tup);
                    }

                    // Extract out the continuation
                    let continuation_bytes = if let CursorError::NoNextReason(
                        NoNextReason::ReturnLimitReached(continuation),
                    ) = err
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
                        continuation.to_bytes().unwrap()
                    } else {
                        panic!("Incorrect NoNextReason");
                    };

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
                            .key_range({
                                // ("sub", "space", 3, 3)..=("sub", "space", 4, 2)
                                TupleRange::new(
                                    TupleLowEndpoint::RangeInclusive({
                                        let tup: (i8, i8) = (3, 3);

                                        let mut t = Tuple::new();
                                        t.push_back::<i8>(tup.0);
                                        t.push_back::<i8>(tup.1);
                                        t
                                    }),
                                    TupleHighEndpoint::RangeInclusive({
                                        let tup: (i8, i8) = (4, 2);

                                        let mut t = Tuple::new();
                                        t.push_back::<i8>(tup.0);
                                        t.push_back::<i8>(tup.1);
                                        t
                                    }),
                                )
                                .into_key_range(&None)
                            })
                            .continuation(continuation_bytes)
                            .scan_properties(ScanPropertiesBuilder::default().build());

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (values_tup, _) = kv_cursor
                        .map(|kv| async { Tuple::try_from(kv.into_value()).unwrap() })
                        .await
                        .collect()
                        .await;

                    let iter = vec![(4, 0), (4, 1), (4, 2)].into_iter().zip(values_tup);

                    for ((i, j), value_tup) in iter {
                        let tuple = {
                            let tup: (i8, i8) = (i, j);

                            let mut t = Tuple::new();
                            t.push_back::<i8>(tup.0);
                            t.push_back::<i8>(tup.1);
                            t
                        };

                        assert_eq!(tuple, value_tup);
                    }

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_exclusive_range() -> Result<(), Failed> {
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
                            .key_range({
                                // ("sub", "space", 3, 4)..("sub", "space", 4, 2)
                                TupleRange::new(
                                    TupleLowEndpoint::RangeExclusive({
                                        let tup: (i8, i8) = (3, 3);

                                        let mut t = Tuple::new();
                                        t.push_back::<i8>(tup.0);
                                        t.push_back::<i8>(tup.1);
                                        t
                                    }),
                                    TupleHighEndpoint::RangeExclusive({
                                        let tup: (i8, i8) = (4, 2);

                                        let mut t = Tuple::new();
                                        t.push_back::<i8>(tup.0);
                                        t.push_back::<i8>(tup.1);
                                        t
                                    }),
                                )
                                .into_key_range(&None)
                            })
                            .scan_properties(ScanPropertiesBuilder::default().build());

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (values_tup, _) = kv_cursor
                        .map(|kv| async { Tuple::try_from(kv.into_value()).unwrap() })
                        .await
                        .collect()
                        .await;

                    let iter = vec![(3, 4), (4, 0), (4, 1)].into_iter().zip(values_tup);

                    for ((i, j), value_tup) in iter {
                        let tuple = {
                            let tup: (i8, i8) = (i, j);

                            let mut t = Tuple::new();
                            t.push_back::<i8>(tup.0);
                            t.push_back::<i8>(tup.1);
                            t
                        };

                        assert_eq!(tuple, value_tup);
                    }

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
                            .key_range({
                                // ("sub", "space", 3, 4)..("sub", "space", 4, 2)
                                TupleRange::new(
                                    TupleLowEndpoint::RangeExclusive({
                                        let tup: (i8, i8) = (3, 3);

                                        let mut t = Tuple::new();
                                        t.push_back::<i8>(tup.0);
                                        t.push_back::<i8>(tup.1);
                                        t
                                    }),
                                    TupleHighEndpoint::RangeExclusive({
                                        let tup: (i8, i8) = (4, 2);

                                        let mut t = Tuple::new();
                                        t.push_back::<i8>(tup.0);
                                        t.push_back::<i8>(tup.1);
                                        t
                                    }),
                                )
                                .into_key_range(&None)
                            })
                            .scan_properties({
                                let mut scan_properites_builder = ScanPropertiesBuilder::default();
                                unsafe {
                                    scan_properites_builder.set_range_options(|range_options| {
                                        range_options.set_limit(2)
                                    });
                                }
                                scan_properites_builder.build()
                            });

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (values_tup, err) = kv_cursor
                        .map(|kv| async { Tuple::try_from(kv.into_value()).unwrap() })
                        .await
                        .collect()
                        .await;

                    let iter = vec![(3, 4), (4, 0)].into_iter().zip(values_tup);

                    for ((i, j), value_tup) in iter {
                        let tuple = {
                            let tup: (i8, i8) = (i, j);

                            let mut t = Tuple::new();
                            t.push_back::<i8>(tup.0);
                            t.push_back::<i8>(tup.1);
                            t
                        };

                        assert_eq!(tuple, value_tup);
                    }

                    // Extract out the continuation
                    let continuation_bytes = if let CursorError::NoNextReason(
                        NoNextReason::ReturnLimitReached(continuation),
                    ) = err
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
                        continuation.to_bytes().unwrap()
                    } else {
                        panic!("Incorrect NoNextReason");
                    };

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
                            .key_range({
                                // ("sub", "space", 3, 4)..("sub", "space", 4, 2)
                                TupleRange::new(
                                    TupleLowEndpoint::RangeExclusive({
                                        let tup: (i8, i8) = (3, 3);

                                        let mut t = Tuple::new();
                                        t.push_back::<i8>(tup.0);
                                        t.push_back::<i8>(tup.1);
                                        t
                                    }),
                                    TupleHighEndpoint::RangeExclusive({
                                        let tup: (i8, i8) = (4, 2);

                                        let mut t = Tuple::new();
                                        t.push_back::<i8>(tup.0);
                                        t.push_back::<i8>(tup.1);
                                        t
                                    }),
                                )
                                .into_key_range(&None)
                            })
                            .continuation(continuation_bytes)
                            .scan_properties(ScanPropertiesBuilder::default().build());

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (values_tup, _) = kv_cursor
                        .map(|kv| async { Tuple::try_from(kv.into_value()).unwrap() })
                        .await
                        .collect()
                        .await;

                    let iter = vec![(4, 1)].into_iter().zip(values_tup);

                    for ((i, j), value_tup) in iter {
                        let tuple = {
                            let tup: (i8, i8) = (i, j);

                            let mut t = Tuple::new();
                            t.push_back::<i8>(tup.0);
                            t.push_back::<i8>(tup.1);
                            t
                        };

                        assert_eq!(tuple, value_tup);
                    }

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_inclusive_null() -> Result<(), Failed> {
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
                            .key_range({
                                // (4,)..=(Null,)
                                TupleRange::new(
                                    TupleLowEndpoint::RangeInclusive({
                                        let tup: (i8,) = (4,);

                                        let mut t = Tuple::new();
                                        t.push_back::<i8>(tup.0);
                                        t
                                    }),
                                    TupleHighEndpoint::RangeInclusive({
                                        let tup: (Null,) = (Null,);

                                        let mut t = Tuple::new();
                                        t.push_back::<Null>(tup.0);
                                        t
                                    }),
                                )
                                .into_key_range(&None)
                            })
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

                    let iter = (0..5).zip(kvs_tup);

                    for (j, (key_tup, value_tup)) in iter {
                        let tuple = {
                            let tup: (i8, i8) = (4, j);

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

fn cursor_exclusive_null() -> Result<(), Failed> {
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
                            .key_range({
                                // (4, 0){exclusive}..(Null,)
                                TupleRange::new(
                                    TupleLowEndpoint::RangeExclusive({
                                        let tup: (i8, i8) = (4, 0);

                                        let mut t = Tuple::new();
                                        t.push_back::<i8>(tup.0);
                                        t.push_back::<i8>(tup.1);
                                        t
                                    }),
                                    TupleHighEndpoint::RangeInclusive({
                                        let tup: (Null,) = (Null,);

                                        let mut t = Tuple::new();
                                        t.push_back::<Null>(tup.0);
                                        t
                                    }),
                                )
                                .into_key_range(&None)
                            })
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

fn cursor_no_next_reasons() -> Result<(), Failed> {
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
                                // ("sub", "space", 3,)
                                TupleRange::all_of(&{
                                    let tup: (i8,) = (3,);

                                    let mut t = Tuple::new();
                                    t.push_back::<i8>(tup.0);
                                    t
                                })
                                .into_key_range(&None),
                            )
                            .scan_properties({
                                let mut scan_properites_builder = ScanPropertiesBuilder::default();
                                unsafe {
                                    scan_properites_builder.set_range_options(|range_options| {
                                        range_options.set_limit(3)
                                    });
                                }
                                scan_properites_builder.build()
                            });

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (values_tup, err) = kv_cursor
                        .map(|kv| async { Tuple::try_from(kv.into_value()).unwrap() })
                        .await
                        .collect()
                        .await;

                    let iter = vec![(3, 0), (3, 1), (3, 2)].into_iter().zip(values_tup);

                    for ((i, j), value_tup) in iter {
                        let tuple = {
                            let tup: (i8, i8) = (i, j);

                            let mut t = Tuple::new();
                            t.push_back::<i8>(tup.0);
                            t.push_back::<i8>(tup.1);
                            t
                        };

                        assert_eq!(tuple, value_tup);
                    }

                    assert!(matches!(
                        err,
                        CursorError::NoNextReason(NoNextReason::ReturnLimitReached(_))
                    ));

                    // Extract out the continuation
                    let continuation_bytes = if let CursorError::NoNextReason(
                        NoNextReason::ReturnLimitReached(continuation),
                    ) = err
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
                        continuation.to_bytes().unwrap()
                    } else {
                        panic!("Incorrect NoNextReason");
                    };

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
                                // ("sub", "space", 3,)
                                TupleRange::all_of(&{
                                    let tup: (i8,) = (3,);

                                    let mut t = Tuple::new();
                                    t.push_back::<i8>(tup.0);
                                    t
                                })
                                .into_key_range(&None),
                            )
                            .continuation(continuation_bytes)
                            .scan_properties({
                                let mut scan_properites_builder = ScanPropertiesBuilder::default();
                                unsafe {
                                    scan_properites_builder.set_range_options(|range_options| {
                                        range_options.set_limit(3)
                                    });
                                }
                                scan_properites_builder.build()
                            });

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (values_tup, err) = kv_cursor
                        .map(|kv| async { Tuple::try_from(kv.into_value()).unwrap() })
                        .await
                        .collect()
                        .await;

                    let iter = vec![(3, 3), (3, 4)].into_iter().zip(values_tup);

                    for ((i, j), value_tup) in iter {
                        let tuple = {
                            let tup: (i8, i8) = (i, j);

                            let mut t = Tuple::new();
                            t.push_back::<i8>(tup.0);
                            t.push_back::<i8>(tup.1);
                            t
                        };

                        assert_eq!(tuple, value_tup);
                    }

                    assert!(matches!(
                        err,
                        CursorError::NoNextReason(NoNextReason::SourceExhausted(_))
                    ));

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

// In RecordLayer tests this is called `simpleScanLimit`.
fn cursor_simple_keyvalue_scan_limit() -> Result<(), Failed> {
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
                            .scan_properties({
                                let mut scan_properites_builder = ScanPropertiesBuilder::default();
                                scan_properites_builder.set_scan_limiter(ScanLimiter::new(
                                    Some(KeyValueScanLimiter::enforcing(2)),
                                    None,
                                    None,
                                ));
                                scan_properites_builder.build()
                            });

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (kvs, err) = kv_cursor.collect().await;

                    assert_eq!(2, kvs.len());

                    assert!(matches!(
                        err,
                        CursorError::NoNextReason(NoNextReason::KeyValueLimitReached(_))
                    ));

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

// In RecordLayer tests this is called `LimitNotReached`.
fn cursor_keyvalue_limit_not_reached() -> Result<(), Failed> {
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
                            .key_range({
                                TupleRange::new(
                                    TupleLowEndpoint::RangeExclusive({
                                        let tup: (i8, i8) = (3, 3);

                                        let mut t = Tuple::new();
                                        t.push_back::<i8>(tup.0);
                                        t.push_back::<i8>(tup.1);
                                        t
                                    }),
                                    TupleHighEndpoint::RangeExclusive({
                                        let tup: (i8, i8) = (4, 2);

                                        let mut t = Tuple::new();
                                        t.push_back::<i8>(tup.0);
                                        t.push_back::<i8>(tup.1);
                                        t
                                    }),
                                )
                                .into_key_range(&None)
                            })
                            .scan_properties({
                                let mut scan_properites_builder = ScanPropertiesBuilder::default();
                                scan_properites_builder.set_scan_limiter(ScanLimiter::new(
                                    Some(KeyValueScanLimiter::enforcing(4)),
                                    None,
                                    None,
                                ));
                                scan_properites_builder.build()
                            });

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (kvs, err) = kv_cursor.collect().await;

                    assert_eq!(3, kvs.len());

                    assert!(matches!(
                        err,
                        CursorError::NoNextReason(NoNextReason::SourceExhausted(_))
                    ));

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_shared_limiter() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    let scan_limiter =
                        ScanLimiter::new(Some(KeyValueScanLimiter::enforcing(4)), None, None);

                    let mut kv_cursor_builder = KeyValueCursorBuilder::new();

                    kv_cursor_builder
                        .subspace(Subspace::new(Bytes::new()).subspace(&{
                            let tup: (&str, &str) = ("sub", "space");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t
                        }))
                        .key_range({
                            TupleRange::new(
                                TupleLowEndpoint::RangeExclusive({
                                    let tup: (i8, i8) = (3, 3);

                                    let mut t = Tuple::new();
                                    t.push_back::<i8>(tup.0);
                                    t.push_back::<i8>(tup.1);
                                    t
                                }),
                                TupleHighEndpoint::RangeExclusive({
                                    let tup: (i8, i8) = (4, 2);

                                    let mut t = Tuple::new();
                                    t.push_back::<i8>(tup.0);
                                    t.push_back::<i8>(tup.1);
                                    t
                                }),
                            )
                            .into_key_range(&None)
                        })
                        .scan_properties({
                            let mut scan_properites_builder = ScanPropertiesBuilder::default();
                            scan_properites_builder.set_scan_limiter(scan_limiter);
                            scan_properites_builder.build()
                        });

                    let mut kv_cursor_1 = kv_cursor_builder.clone().build(&tr)?;
                    let mut kv_cursor_2 = kv_cursor_builder.build(&tr)?;

                    let res = kv_cursor_1.next().await;
                    assert!(matches!(res, Ok(_)));

                    let res = kv_cursor_2.next().await;
                    assert!(matches!(res, Ok(_)));

                    let res = kv_cursor_1.next().await;
                    assert!(matches!(res, Ok(_)));

                    let res = kv_cursor_2.next().await;
                    assert!(matches!(res, Ok(_)));

                    let res = kv_cursor_1.next().await;
                    assert!(matches!(
                        res,
                        Err(CursorError::NoNextReason(
                            NoNextReason::KeyValueLimitReached(_)
                        ))
                    ));

                    let res = kv_cursor_2.next().await;
                    assert!(matches!(
                        res,
                        Err(CursorError::NoNextReason(
                            NoNextReason::KeyValueLimitReached(_)
                        ))
                    ));

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_empty_scan() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    let mut kv_cursor = {
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
                                // ("sub", "space", 9,)
                                TupleRange::all_of(&{
                                    let tup: (i8,) = (9,);

                                    let mut t = Tuple::new();
                                    t.push_back::<i8>(tup.0);
                                    t
                                })
                                .into_key_range(&None),
                            )
                            .scan_properties(ScanPropertiesBuilder::default().build());

                        kv_cursor_builder.build(&tr)
                    }?;

                    let res = kv_cursor.next().await;

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

fn cursor_build_without_scan_properties() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    let mut kv_cursor_builder = KeyValueCursorBuilder::new();

                    kv_cursor_builder.subspace(Subspace::new(Bytes::new()).subspace(&{
                        let tup: (&str, &str) = ("sub", "space");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t
                    }));

                    let res = kv_cursor_builder.build(&tr).unwrap_err();

                    assert_eq!(res, FdbError::new(CURSOR_KEYVALUE_CURSOR_BUILDER_ERROR));

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_build_with_required_properties() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
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

                    let res = kv_cursor_builder.build(&tr);

                    assert!(matches!(res, Ok(_)));

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_no_subspace_all() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    {
                        let kv_cursor = {
                            let mut kv_cursor_builder = KeyValueCursorBuilder::new();

                            kv_cursor_builder
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

                            let expected_tuple_key = {
                                let tup: (&str, &str) = ("sub", "space");

                                let mut t = Tuple::new();
                                t.push_back::<String>(tup.0.to_string());
                                t.push_back::<String>(tup.1.to_string());
                                t.append(&mut tuple.clone());
                                t
                            };

                            let expected_tuple_value = tuple;

                            assert_eq!(expected_tuple_key, key_tup);
                            assert_eq!(expected_tuple_value, value_tup);
                        }

                        assert!(matches!(
                            err,
                            CursorError::NoNextReason(NoNextReason::SourceExhausted(_))
                        ));

                        if let CursorError::NoNextReason(NoNextReason::SourceExhausted(
                            continuation,
                        )) = err
                        {
                            assert!(continuation.is_end_marker());
                        }
                    }

                    {
                        let kv_cursor = {
                            let mut kv_cursor_builder = KeyValueCursorBuilder::new();

                            kv_cursor_builder
                                .key_range(TupleRange::all().into_key_range(&None))
                                .scan_properties({
                                    let mut scan_properites_builder =
                                        ScanPropertiesBuilder::default();
                                    unsafe {
                                        scan_properites_builder.set_range_options(
                                            |range_options| range_options.set_limit(10),
                                        );
                                    }
                                    scan_properites_builder.build()
                                });

                            kv_cursor_builder.build(&tr)
                        }?;

                        let (kvs_tup, err) = kv_cursor.collect().await;

                        assert_eq!(10, kvs_tup.len());

                        // Extract out the continuation
                        let continuation_bytes = if let CursorError::NoNextReason(
                            NoNextReason::ReturnLimitReached(continuation),
                        ) = err
                        {
                            assert!(!continuation.is_begin_marker());
                            assert!(!continuation.is_end_marker());
                            continuation.to_bytes().unwrap()
                        } else {
                            panic!("Invalid err");
                        };

                        // Build a new cursor with continuation

                        let kv_cursor = {
                            let mut kv_cursor_builder = KeyValueCursorBuilder::new();

                            kv_cursor_builder
                                .key_range(TupleRange::all().into_key_range(&None))
                                .continuation(continuation_bytes)
                                .scan_properties(ScanPropertiesBuilder::default().build());

                            kv_cursor_builder.build(&tr)
                        }?;

                        let (kvs_tup, _) = kv_cursor.collect().await;

                        assert_eq!(15, kvs_tup.len());
                    }

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_no_subspace_begins_with() -> Result<(), Failed> {
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
                            .key_range(
                                // ("sub", "space", 3,)
                                TupleRange::all_of(&{
                                    let tup: (&str, &str, i8) = ("sub", "space", 3);

                                    let mut t = Tuple::new();
                                    t.push_back::<String>(tup.0.to_string());
                                    t.push_back::<String>(tup.1.to_string());
                                    t.push_back::<i8>(tup.2);
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

                    let iter = (0..5).zip(kvs_tup);

                    for (j, (key_tup, value_tup)) in iter {
                        let tuple = {
                            let tup: (i8, i8) = (3, j);

                            let mut t = Tuple::new();
                            t.push_back::<i8>(tup.0);
                            t.push_back::<i8>(tup.1);
                            t
                        };

                        let expected_tuple_key = {
                            let tup: (&str, &str) = ("sub", "space");

                            let mut t = Tuple::new();
                            t.push_back::<String>(tup.0.to_string());
                            t.push_back::<String>(tup.1.to_string());
                            t.append(&mut tuple.clone());
                            t
                        };

                        let expected_tuple_value = tuple;

                        assert_eq!(expected_tuple_key, key_tup);
                        assert_eq!(expected_tuple_value, value_tup);
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

                    let kv_cursor = {
                        let mut kv_cursor_builder = KeyValueCursorBuilder::new();

                        kv_cursor_builder
                            .key_range(
                                // ("sub", "space", 3,)
                                TupleRange::all_of(&{
                                    let tup: (&str, &str, i8) = ("sub", "space", 3);

                                    let mut t = Tuple::new();
                                    t.push_back::<String>(tup.0.to_string());
                                    t.push_back::<String>(tup.1.to_string());
                                    t.push_back::<i8>(tup.2);
                                    t
                                })
                                .into_key_range(&None),
                            )
                            .scan_properties({
                                let mut scan_properites_builder = ScanPropertiesBuilder::default();
                                unsafe {
                                    scan_properites_builder.set_range_options(|range_options| {
                                        range_options.set_limit(2)
                                    });
                                }
                                scan_properites_builder.build()
                            });

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (kvs, err) = kv_cursor.collect().await;

                    assert_eq!(2, kvs.len());

                    // Extract out the continuation
                    let continuation_bytes = if let CursorError::NoNextReason(
                        NoNextReason::ReturnLimitReached(continuation),
                    ) = err
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
                        continuation.to_bytes().unwrap()
                    } else {
                        panic!("Incorrect NoNextReason");
                    };

                    let kv_cursor = {
                        let mut kv_cursor_builder = KeyValueCursorBuilder::new();

                        kv_cursor_builder
                            .key_range(
                                // ("sub", "space", 3,)
                                TupleRange::all_of(&{
                                    let tup: (&str, &str, i8) = ("sub", "space", 3);

                                    let mut t = Tuple::new();
                                    t.push_back::<String>(tup.0.to_string());
                                    t.push_back::<String>(tup.1.to_string());
                                    t.push_back::<i8>(tup.2);
                                    t
                                })
                                .into_key_range(&None),
                            )
                            .continuation(continuation_bytes)
                            .scan_properties({
                                let mut scan_properites_builder = ScanPropertiesBuilder::default();
                                unsafe {
                                    scan_properites_builder.set_range_options(|range_options| {
                                        range_options.set_limit(3)
                                    });
                                }
                                scan_properites_builder.build()
                            });

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (kvs, _) = kv_cursor.collect().await;

                    assert_eq!(3, kvs.len());

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_no_subspace_inclusive_range() -> Result<(), Failed> {
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
                            .key_range({
                                // ("sub", "space", 3, 3)..=("sub", "space", 4, 2)
                                TupleRange::new(
                                    TupleLowEndpoint::RangeInclusive({
                                        let tup: (&str, &str, i8, i8) = ("sub", "space", 3, 3);

                                        let mut t = Tuple::new();
                                        t.push_back::<String>(tup.0.to_string());
                                        t.push_back::<String>(tup.1.to_string());
                                        t.push_back::<i8>(tup.2);
                                        t.push_back::<i8>(tup.3);
                                        t
                                    }),
                                    TupleHighEndpoint::RangeInclusive({
                                        let tup: (&str, &str, i8, i8) = ("sub", "space", 4, 2);

                                        let mut t = Tuple::new();
                                        t.push_back::<String>(tup.0.to_string());
                                        t.push_back::<String>(tup.1.to_string());
                                        t.push_back::<i8>(tup.2);
                                        t.push_back::<i8>(tup.3);
                                        t
                                    }),
                                )
                                .into_key_range(&None)
                            })
                            .scan_properties(ScanPropertiesBuilder::default().build());

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (values_tup, _) = kv_cursor
                        .map(|kv| async { Tuple::try_from(kv.into_value()).unwrap() })
                        .await
                        .collect()
                        .await;

                    let iter = vec![(3, 3), (3, 4), (4, 0), (4, 1), (4, 2)]
                        .into_iter()
                        .zip(values_tup);

                    for ((i, j), value_tup) in iter {
                        let tuple = {
                            let tup: (i8, i8) = (i, j);

                            let mut t = Tuple::new();
                            t.push_back::<i8>(tup.0);
                            t.push_back::<i8>(tup.1);
                            t
                        };

                        assert_eq!(tuple, value_tup);
                    }

                    let kv_cursor = {
                        let mut kv_cursor_builder = KeyValueCursorBuilder::new();

                        kv_cursor_builder
                            .key_range({
                                // ("sub", "space", 3, 3)..=("sub", "space", 4, 2)
                                TupleRange::new(
                                    TupleLowEndpoint::RangeInclusive({
                                        let tup: (&str, &str, i8, i8) = ("sub", "space", 3, 3);

                                        let mut t = Tuple::new();
                                        t.push_back::<String>(tup.0.to_string());
                                        t.push_back::<String>(tup.1.to_string());
                                        t.push_back::<i8>(tup.2);
                                        t.push_back::<i8>(tup.3);
                                        t
                                    }),
                                    TupleHighEndpoint::RangeInclusive({
                                        let tup: (i8, i8) = (4, 2);

                                        let mut t = Tuple::new();
                                        t.push_back::<i8>(tup.0);
                                        t.push_back::<i8>(tup.1);
                                        t
                                    }),
                                )
                                .into_key_range(&None)
                            })
                            .scan_properties({
                                let mut scan_properites_builder = ScanPropertiesBuilder::default();
                                unsafe {
                                    scan_properites_builder.set_range_options(|range_options| {
                                        range_options.set_limit(2)
                                    });
                                }
                                scan_properites_builder.build()
                            });

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (values_tup, err) = kv_cursor
                        .map(|kv| async { Tuple::try_from(kv.into_value()).unwrap() })
                        .await
                        .collect()
                        .await;

                    let iter = vec![(3, 3), (3, 4)].into_iter().zip(values_tup);

                    for ((i, j), value_tup) in iter {
                        let tuple = {
                            let tup: (i8, i8) = (i, j);

                            let mut t = Tuple::new();
                            t.push_back::<i8>(tup.0);
                            t.push_back::<i8>(tup.1);
                            t
                        };

                        assert_eq!(tuple, value_tup);
                    }

                    // Extract out the continuation
                    let continuation_bytes = if let CursorError::NoNextReason(
                        NoNextReason::ReturnLimitReached(continuation),
                    ) = err
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
                        continuation.to_bytes().unwrap()
                    } else {
                        panic!("Incorrect NoNextReason");
                    };

                    let kv_cursor = {
                        let mut kv_cursor_builder = KeyValueCursorBuilder::new();

                        kv_cursor_builder
                            .key_range({
                                // ("sub", "space", 3, 3)..=("sub", "space", 4, 2)
                                TupleRange::new(
                                    TupleLowEndpoint::RangeInclusive({
                                        let tup: (&str, &str, i8, i8) = ("sub", "space", 3, 3);

                                        let mut t = Tuple::new();
                                        t.push_back::<String>(tup.0.to_string());
                                        t.push_back::<String>(tup.1.to_string());
                                        t.push_back::<i8>(tup.2);
                                        t.push_back::<i8>(tup.3);
                                        t
                                    }),
                                    TupleHighEndpoint::RangeInclusive({
                                        let tup: (&str, &str, i8, i8) = ("sub", "space", 4, 2);

                                        let mut t = Tuple::new();
                                        t.push_back::<String>(tup.0.to_string());
                                        t.push_back::<String>(tup.1.to_string());
                                        t.push_back::<i8>(tup.2);
                                        t.push_back::<i8>(tup.3);
                                        t
                                    }),
                                )
                                .into_key_range(&None)
                            })
                            .continuation(continuation_bytes)
                            .scan_properties(ScanPropertiesBuilder::default().build());

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (values_tup, _) = kv_cursor
                        .map(|kv| async { Tuple::try_from(kv.into_value()).unwrap() })
                        .await
                        .collect()
                        .await;

                    let iter = vec![(4, 0), (4, 1), (4, 2)].into_iter().zip(values_tup);

                    for ((i, j), value_tup) in iter {
                        let tuple = {
                            let tup: (i8, i8) = (i, j);

                            let mut t = Tuple::new();
                            t.push_back::<i8>(tup.0);
                            t.push_back::<i8>(tup.1);
                            t
                        };

                        assert_eq!(tuple, value_tup);
                    }

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn cursor_no_subspace_exclusive_range() -> Result<(), Failed> {
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
                            .key_range({
                                // ("sub", "space", 3, 4)..("sub", "space", 4, 2)
                                TupleRange::new(
                                    TupleLowEndpoint::RangeExclusive({
                                        let tup: (&str, &str, i8, i8) = ("sub", "space", 3, 3);

                                        let mut t = Tuple::new();
                                        t.push_back::<String>(tup.0.to_string());
                                        t.push_back::<String>(tup.1.to_string());
                                        t.push_back::<i8>(tup.2);
                                        t.push_back::<i8>(tup.3);
                                        t
                                    }),
                                    TupleHighEndpoint::RangeExclusive({
                                        let tup: (&str, &str, i8, i8) = ("sub", "space", 4, 2);

                                        let mut t = Tuple::new();
                                        t.push_back::<String>(tup.0.to_string());
                                        t.push_back::<String>(tup.1.to_string());
                                        t.push_back::<i8>(tup.2);
                                        t.push_back::<i8>(tup.3);
                                        t
                                    }),
                                )
                                .into_key_range(&None)
                            })
                            .scan_properties(ScanPropertiesBuilder::default().build());

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (values_tup, _) = kv_cursor
                        .map(|kv| async { Tuple::try_from(kv.into_value()).unwrap() })
                        .await
                        .collect()
                        .await;

                    let iter = vec![(3, 4), (4, 0), (4, 1)].into_iter().zip(values_tup);

                    for ((i, j), value_tup) in iter {
                        let tuple = {
                            let tup: (i8, i8) = (i, j);

                            let mut t = Tuple::new();
                            t.push_back::<i8>(tup.0);
                            t.push_back::<i8>(tup.1);
                            t
                        };

                        assert_eq!(tuple, value_tup);
                    }

                    let kv_cursor = {
                        let mut kv_cursor_builder = KeyValueCursorBuilder::new();

                        kv_cursor_builder
                            .key_range({
                                // ("sub", "space", 3, 4)..("sub", "space", 4, 2)
                                TupleRange::new(
                                    TupleLowEndpoint::RangeExclusive({
                                        let tup: (&str, &str, i8, i8) = ("sub", "space", 3, 3);

                                        let mut t = Tuple::new();
                                        t.push_back::<String>(tup.0.to_string());
                                        t.push_back::<String>(tup.1.to_string());
                                        t.push_back::<i8>(tup.2);
                                        t.push_back::<i8>(tup.3);
                                        t
                                    }),
                                    TupleHighEndpoint::RangeExclusive({
                                        let tup: (&str, &str, i8, i8) = ("sub", "space", 4, 2);

                                        let mut t = Tuple::new();
                                        t.push_back::<String>(tup.0.to_string());
                                        t.push_back::<String>(tup.1.to_string());
                                        t.push_back::<i8>(tup.2);
                                        t.push_back::<i8>(tup.3);
                                        t
                                    }),
                                )
                                .into_key_range(&None)
                            })
                            .scan_properties({
                                let mut scan_properites_builder = ScanPropertiesBuilder::default();
                                unsafe {
                                    scan_properites_builder.set_range_options(|range_options| {
                                        range_options.set_limit(2)
                                    });
                                }
                                scan_properites_builder.build()
                            });

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (values_tup, err) = kv_cursor
                        .map(|kv| async { Tuple::try_from(kv.into_value()).unwrap() })
                        .await
                        .collect()
                        .await;

                    let iter = vec![(3, 4), (4, 0)].into_iter().zip(values_tup);

                    for ((i, j), value_tup) in iter {
                        let tuple = {
                            let tup: (i8, i8) = (i, j);

                            let mut t = Tuple::new();
                            t.push_back::<i8>(tup.0);
                            t.push_back::<i8>(tup.1);
                            t
                        };

                        assert_eq!(tuple, value_tup);
                    }

                    // Extract out the continuation
                    let continuation_bytes = if let CursorError::NoNextReason(
                        NoNextReason::ReturnLimitReached(continuation),
                    ) = err
                    {
                        assert!(!continuation.is_begin_marker());
                        assert!(!continuation.is_end_marker());
                        continuation.to_bytes().unwrap()
                    } else {
                        panic!("Incorrect NoNextReason");
                    };

                    let kv_cursor = {
                        let mut kv_cursor_builder = KeyValueCursorBuilder::new();

                        kv_cursor_builder
                            .key_range({
                                // ("sub", "space", 3, 4)..("sub", "space", 4, 2)
                                TupleRange::new(
                                    TupleLowEndpoint::RangeExclusive({
                                        let tup: (&str, &str, i8, i8) = ("sub", "space", 3, 3);

                                        let mut t = Tuple::new();
                                        t.push_back::<String>(tup.0.to_string());
                                        t.push_back::<String>(tup.1.to_string());
                                        t.push_back::<i8>(tup.2);
                                        t.push_back::<i8>(tup.3);
                                        t
                                    }),
                                    TupleHighEndpoint::RangeExclusive({
                                        let tup: (&str, &str, i8, i8) = ("sub", "space", 4, 2);

                                        let mut t = Tuple::new();
                                        t.push_back::<String>(tup.0.to_string());
                                        t.push_back::<String>(tup.1.to_string());
                                        t.push_back::<i8>(tup.2);
                                        t.push_back::<i8>(tup.3);
                                        t
                                    }),
                                )
                                .into_key_range(&None)
                            })
                            .continuation(continuation_bytes)
                            .scan_properties(ScanPropertiesBuilder::default().build());

                        kv_cursor_builder.build(&tr)
                    }?;

                    let (values_tup, _) = kv_cursor
                        .map(|kv| async { Tuple::try_from(kv.into_value()).unwrap() })
                        .await
                        .collect()
                        .await;

                    let iter = vec![(4, 1)].into_iter().zip(values_tup);

                    for ((i, j), value_tup) in iter {
                        let tuple = {
                            let tup: (i8, i8) = (i, j);

                            let mut t = Tuple::new();
                            t.push_back::<i8>(tup.0);
                            t.push_back::<i8>(tup.1);
                            t
                        };

                        assert_eq!(tuple, value_tup);
                    }

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}
