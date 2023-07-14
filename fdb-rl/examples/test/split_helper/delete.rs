use bytes::Bytes;

use fdb::database::FdbDatabase;
use fdb::error::FdbError;
use fdb::range::Range;
use fdb::subspace::Subspace;
use fdb::transaction::Transaction;
use fdb::tuple::{Tuple, Versionstamp};

use fdb_rl::test::split_helper::{self, SPLIT_HELPER_INVALID_PRIMARY_KEY};
use fdb_rl::RecordVersion;

use libtest_mimic::{Arguments, Failed, Trial};

use tokio::runtime::Builder;

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
        Trial::test("delete", delete),
        Trial::test("delete_primary_key_invalid", delete_primary_key_invalid),
        Trial::test(
            "delete_primary_key_valid_with_empty_record",
            delete_primary_key_valid_with_empty_record,
        ),
        Trial::test(
            "delete_primary_key_valid_and_missing",
            delete_primary_key_valid_and_missing,
        ),
        Trial::test("delete_no_subspace", delete_no_subspace),
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

                    Ok(())
                })
                .await?;
            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

// Tests

fn delete() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    // add record
    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .run(|tr| async move {
                    let subspace = Subspace::new(Bytes::new()).subspace(&{
                        let tup: (&str, &str) = ("sub", "space");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t
                    });

                    let primary_key_tuple = {
                        let tup: (&str, &str, i8) = ("primary", "key", 0);

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<i8>(tup.2);
                        t
                    };

                    let serialized_bytes = Bytes::from_static(b"Hello world!");

                    let record_version = RecordVersion::from(Versionstamp::incomplete(10));

                    split_helper::save(
                        &tr,
                        &None,
                        &Some(subspace),
                        &primary_key_tuple,
                        serialized_bytes,
                        record_version,
                    )
                    .await?;

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    // verify record exits and delete
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
                        let tup: (&str, &str, i8) = ("primary", "key", 0);

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<i8>(tup.2);
                        t
                    };

                    let res = split_helper::load(&tr, &None, &subspace, &primary_key_tuple).await?;
                    // record exists
                    assert!(res.is_some());

                    // delete record
                    split_helper::delete(&tr, &None, &subspace, &primary_key_tuple).await?;

                    let res = split_helper::load(&tr, &None, &subspace, &primary_key_tuple).await?;
                    // record no longer exists
                    assert!(res.is_none());

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    // ensure record is deleted after transaction is commited.
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
                        let tup: (&str, &str, i8) = ("primary", "key", 0);

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<i8>(tup.2);
                        t
                    };

                    let res = split_helper::load(&tr, &None, &subspace, &primary_key_tuple).await?;
                    // record does not exist
                    assert!(res.is_none());

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn delete_primary_key_invalid() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    // add two records
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

                    let primary_key_tuple_0 = {
                        let tup: (&str, &str, i8) = ("primary", "key", 0);

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<i8>(tup.2);
                        t
                    };

                    let primary_key_tuple_1 = {
                        let tup: (&str, &str, i8) = ("primary", "key", 1);

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<i8>(tup.2);
                        t
                    };

                    let serialized_bytes = Bytes::from_static(b"Hello world!");

                    let record_version_0 = RecordVersion::from(Versionstamp::incomplete(10));

                    let record_version_1 = RecordVersion::from(Versionstamp::incomplete(11));

                    split_helper::save(
                        &tr,
                        &None,
                        &subspace,
                        &primary_key_tuple_0,
                        serialized_bytes.clone(),
                        record_version_0,
                    )
                    .await?;

                    split_helper::save(
                        &tr,
                        &None,
                        &subspace,
                        &primary_key_tuple_1,
                        serialized_bytes,
                        record_version_1,
                    )
                    .await?;

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    // attempting to delete invalid primary key should fail
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
                        let tup: (&str, &str) = ("primary", "key");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t
                    };

                    // delete record
                    let res = split_helper::delete(&tr, &None, &subspace, &primary_key_tuple).await;

                    assert_eq!(res, Err(FdbError::new(SPLIT_HELPER_INVALID_PRIMARY_KEY)));

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    // deleting with valid primary key should succeed.
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

                    let primary_key_tuple_0 = {
                        let tup: (&str, &str, i8) = ("primary", "key", 0);

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<i8>(tup.2);
                        t
                    };

                    let primary_key_tuple_1 = {
                        let tup: (&str, &str, i8) = ("primary", "key", 1);

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<i8>(tup.2);
                        t
                    };

                    // delete record
                    let res =
                        split_helper::delete(&tr, &None, &subspace, &primary_key_tuple_0).await;

                    assert!(res.is_ok());

                    let res =
                        split_helper::delete(&tr, &None, &subspace, &primary_key_tuple_1).await;

                    assert!(res.is_ok());

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn delete_primary_key_valid_with_empty_record() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    // add an empty record.
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

                    let serialized_bytes = Bytes::new();

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

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    // verify empty record exists and delete
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

                    let res = split_helper::load(&tr, &None, &subspace, &primary_key_tuple).await?;
                    // record exists
                    assert!(res.is_some());

                    // delete record
                    split_helper::delete(&tr, &None, &subspace, &primary_key_tuple).await?;

                    let res = split_helper::load(&tr, &None, &subspace, &primary_key_tuple).await?;
                    // record no longer exists
                    assert!(res.is_none());

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    // ensure empty record is deleted after transaction is commited.
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

                    let res = split_helper::load(&tr, &None, &subspace, &primary_key_tuple).await?;
                    // record does not exist
                    assert!(res.is_none());

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn delete_primary_key_valid_and_missing() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    // There is no primary key `("primary", "key", -1)`. We should be
    // able to delete a non-existent primary key.
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

                    // delete record
                    let res = split_helper::delete(&tr, &None, &subspace, &primary_key_tuple).await;

                    assert!(res.is_ok());

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}

fn delete_no_subspace() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    // add record
    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .run(|tr| async move {
                    let primary_key_tuple = {
                        let tup: (&str, &str, i8) = ("primary", "key", 0);

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<i8>(tup.2);
                        t
                    };

                    let serialized_bytes = Bytes::from_static(b"Hello world!");

                    let record_version = RecordVersion::from(Versionstamp::incomplete(10));

                    split_helper::save(
                        &tr,
                        &None,
                        &None,
                        &primary_key_tuple,
                        serialized_bytes,
                        record_version,
                    )
                    .await?;

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    // verify record exits and delete
    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .run(|tr| async move {
                    let primary_key_tuple = {
                        let tup: (&str, &str, i8) = ("primary", "key", 0);

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<i8>(tup.2);
                        t
                    };

                    let res = split_helper::load(&tr, &None, &None, &primary_key_tuple).await?;
                    // record exists
                    assert!(res.is_some());

                    // delete record
                    split_helper::delete(&tr, &None, &None, &primary_key_tuple).await?;

                    let res = split_helper::load(&tr, &None, &None, &primary_key_tuple).await?;
                    // record no longer exists
                    assert!(res.is_none());

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    // ensure record is deleted after transaction is commited.
    rt.block_on({
        let fdb_database = fdb_database_ref.clone();
        async move {
            fdb_database
                .run(|tr| async move {
                    let primary_key_tuple = {
                        let tup: (&str, &str, i8) = ("primary", "key", 0);

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<i8>(tup.2);
                        t
                    };

                    let res = split_helper::load(&tr, &None, &None, &primary_key_tuple).await?;
                    // record does not exist
                    assert!(res.is_none());

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}
