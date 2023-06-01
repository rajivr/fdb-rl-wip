#![allow(non_snake_case)]
#![feature(once_cell)]

use bytes::{BufMut, Bytes, BytesMut};

use fdb::database::FdbDatabase;
use fdb::range::Range;
use fdb::subspace::Subspace;
use fdb::transaction::Transaction;
use fdb::tuple::{Tuple, Versionstamp};

use fdb_rl::RecordVersion;

use libtest_mimic::Arguments;

use tokio::runtime::Builder;

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

    let tests = vec![];

    let _ = libtest_mimic::run(&args, tests);

    let fdb_database = unsafe { FDB_DATABASE.take().unwrap() };

    drop(fdb_database);

    unsafe {
        fdb::stop_network();
    }

    Ok(())
}

// TODO
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

                    for (pk, local_version, value_bytes) in [
                        ("short", 0, SHORT_STRING.deref().clone()),
                        ("medium", 1, MEDIUM_STRING.deref().clone()),
                        ("long", 2, LONG_STRING.deref().clone()),
                        ("very_long", 3, VERY_LONG_STRING.deref().clone()),
                    ] {
                        let record_version = RecordVersion::from(Versionstamp::complete(
                            Bytes::from_static(b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"),
                            local_version,
                        ));
                    }

		    // TODO: Continue from here...

                    Ok(())
                })
                .await?;
            Result::<(), Box<dyn Error>>::Ok(())
        }
    })?;

    Ok(())
}
