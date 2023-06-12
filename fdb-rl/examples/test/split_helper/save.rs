#![feature(lazy_cell)]

use bytes::{Buf, BufMut, Bytes, BytesMut};

use fdb::database::FdbDatabase;
use fdb::error::FdbError;
use fdb::range::Range;
use fdb::subspace::Subspace;
use fdb::transaction::Transaction;
use fdb::tuple::{Tuple, Versionstamp};

use fdb_rl::error::SPLIT_HELPER_SAVE_INVALID_SERIALIZED_BYTES_SIZE;
use fdb_rl::test::split_helper;
use fdb_rl::RecordVersion;

use libtest_mimic::{Arguments, Failed, Trial};

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

static LONG_STRING: LazyLock<Bytes> = LazyLock::new(|| {
    let mut x = BytesMut::new();

    // requires 1 split
    (0..1_000).for_each(|_| x.put(HUMPTY_DUMPTY.as_bytes()));

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

    // The `load` function tests takes care of most the potential test
    // cases for `save`. Hence there is only one test case here.
    let tests = vec![Trial::test("save_10mb", save_10mb)];

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

fn save_10mb() -> Result<(), Failed> {
    let rt = Builder::new_current_thread().build()?;

    let fdb_database_ref = unsafe { FDB_DATABASE.as_ref().unwrap() };

    // attempt to add a record of 10mb size.
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
                        let tup: (&str, &str, &str) = ("primary", "key", "10mb");

                        let mut t = Tuple::new();
                        t.push_back::<String>(tup.0.to_string());
                        t.push_back::<String>(tup.1.to_string());
                        t.push_back::<String>(tup.2.to_string());
                        t
                    };

                    let long_string_100kb =
                        LONG_STRING.deref().clone().copy_to_bytes(SPLIT_RECORD_SIZE);

                    // Greater than 10MB.
                    let long_string_over_10mb = {
                        let mut x = BytesMut::new();

                        (0..101).for_each(|_| x.put((&long_string_100kb).clone()));

                        Bytes::from(x)
                    };

                    let record_version = RecordVersion::from(Versionstamp::incomplete(10));

                    let res = split_helper::save(
                        &tr,
                        &None,
                        &Some(subspace),
                        &primary_key_tuple,
                        long_string_over_10mb,
                        record_version,
                    )
                    .await;

                    assert_eq!(
                        res,
                        Err(FdbError::new(
                            SPLIT_HELPER_SAVE_INVALID_SERIALIZED_BYTES_SIZE
                        ))
                    );

                    Ok(())
                })
                .await?;

            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}
