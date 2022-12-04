#![allow(non_snake_case)]

use bytes::Bytes;

use fdb::database::FdbDatabase;
use fdb::range::Range;
use fdb::subspace::Subspace;
use fdb::transaction::Transaction;
use fdb::tuple::Tuple;

use fdb_rl::cursor::{Cursor, CursorError, KeyValueCursorBuilder, NoNextReason};
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

    // TODO: Continue from here
    // - [ ] Implement all the Java RecordLayer tests
    // - [ ] No subspace get range.
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
        Trial::test("cursor_ok_all", cursor_ok_all),
        Trial::test("cursor_ok_begins_with", cursor_ok_begins_with),
        Trial::test("cursor_ok_begins_with", cursor_ok_begins_with),
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

                    for i in 0..5 {
                        for j in 0..5 {
                            let key = subspace.subspace(&{
                                let tup: (i8, i8) = (i, j);

                                let mut t = Tuple::new();
                                t.push_back::<i8>(tup.0);
                                t.push_back::<i8>(tup.1);
                                t
                            });

                            let value = {
                                let tup: (i8, i8) = (i, j);

                                let mut t = Tuple::new();
                                t.push_back::<i8>(tup.0);
                                t.push_back::<i8>(tup.1);
                                t
                            };

                            tr.set(key.pack(), value.pack());
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
                                        Some(KeyValueScanLimiter::enforcing(0)),
                                        None,
                                        None,
                                    ));
                                    scan_properites_builder.build()
                                });

                            kv_cursor_builder.build(&tr)
                        }?;

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
                            assert!(continuation.is_begin_marker());
                        }
                    }

                    // KeyValue limit of non-zero (out-of-band).
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
                                        Some(KeyValueScanLimiter::enforcing(2)),
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

                        assert!(matches!(res, Ok(_)));

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
                    // Byte limit of zero
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
                                        Some(ByteScanLimiter::enforcing(0)),
                                        None,
                                    ));
                                    scan_properites_builder.build()
                                });

                            kv_cursor_builder.build(&tr)
                        }?;

                        let res = kv_cursor.next().await;

                        assert!(matches!(
                            res,
                            Err(CursorError::NoNextReason(NoNextReason::ByteLimitReached(_)))
                        ));

                        if let Err(CursorError::NoNextReason(NoNextReason::ByteLimitReached(
                            continuation,
                        ))) = res
                        {
                            assert!(continuation.is_begin_marker());
                        }
                    }

                    // Byte limit of non-zero.
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
                                        Some(ByteScanLimiter::enforcing(10)),
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
                            Err(CursorError::NoNextReason(NoNextReason::ByteLimitReached(_)))
                        ));

                        if let Err(CursorError::NoNextReason(NoNextReason::ByteLimitReached(
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

fn cursor_error_no_next_reason_time_limit() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().enable_time().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .read(|tr| async move {
                    // Time limit of zero
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
                                        Some(TimeScanLimiter::new(0)),
                                    ));
                                    scan_properites_builder.build()
                                });

                            kv_cursor_builder.build(&tr)
                        }?;

                        let res = kv_cursor.next().await;

                        assert!(matches!(
                            res,
                            Err(CursorError::NoNextReason(NoNextReason::TimeLimitReached(_)))
                        ));

                        if let Err(CursorError::NoNextReason(NoNextReason::TimeLimitReached(
                            continuation,
                        ))) = res
                        {
                            assert!(continuation.is_begin_marker());
                        }
                    }

                    // Time limit of non-zero.
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

                        // Here we assume we were able to get the
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
                                            |range_options| range_options.set_limit(0),
                                        );
                                    }
                                    scan_properites_builder.build()
                                });

                            kv_cursor_builder.build(&tr)
                        }?;

                        for i in 0..5 {
                            for j in 0..5 {
                                let tuple = {
                                    let tup: (i8, i8) = (i, j);

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
                        }

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

                    // Setting in-band limit to negative number means
                    // unlimited.
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
                                            |range_options| range_options.set_limit(-1),
                                        );
                                    }
                                    scan_properites_builder.build()
                                });

                            kv_cursor_builder.build(&tr)
                        }?;

                        for i in 0..5 {
                            for j in 0..5 {
                                let tuple = {
                                    let tup: (i8, i8) = (i, j);

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
                        }

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

                        // Check the first `KeyValue`
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
                                    TupleRange::all_of({
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

                        for j in 0..5 {
                            let tuple = {
                                let tup: (i8, i8) = (4, j);

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
                            Err(CursorError::NoNextReason(NoNextReason::SourceExhausted(_)))
                        ));

                        // Extract out the continuation
                        let continuation_bytes = if let Err(CursorError::NoNextReason(
                            NoNextReason::SourceExhausted(continuation),
                        )) = res
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
                                    TupleRange::all_of({
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
                                    TupleRange::all_of({
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

                        for j in (0..5).rev() {
                            let tuple = {
                                let tup: (i8, i8) = (3, j);

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
                            Err(CursorError::NoNextReason(NoNextReason::SourceExhausted(_)))
                        ));

                        // Extract out the continuation
                        let continuation_bytes = if let Err(CursorError::NoNextReason(
                            NoNextReason::SourceExhausted(continuation),
                        )) = res
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
                                    TupleRange::all_of({
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

fn cursor_ok_all() -> Result<(), Failed> {
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
                            .key_range(TupleRange::all().into_key_range(&None))
                            .scan_properties(ScanPropertiesBuilder::default().build());

                        kv_cursor_builder.build(&tr)
                    }?;

                    for i in 0..5 {
                        for j in 0..5 {
                            let tuple = {
                                let tup: (i8, i8) = (i, j);

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
                    }

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

fn cursor_ok_begins_with() -> Result<(), Failed> {
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
                                // ("sub", "space", 3,)
                                TupleRange::all_of({
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

                    for j in 0..5 {
                        let tuple = {
                            let tup: (i8, i8) = (3, j);

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
