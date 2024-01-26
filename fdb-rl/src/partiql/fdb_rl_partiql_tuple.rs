use fdb::error::{FdbError, FdbResult};
use fdb::tuple::{Tuple as FdbTuple, TupleSchema as FdbTupleSchema};

use partiql_value::{List, Value};

use std::convert::TryFrom;

/// TODO
pub(crate) struct FdbRLPartiQLTuple<'ts> {
    tuple_schema: &'ts FdbTupleSchema,
    tuple: FdbTuple,
}

impl<'ts> FdbRLPartiQLTuple<'ts> {
    fn try_from_inner<'t>(
        partiql_list: List,
        fdb_tuple_ref: &'t mut FdbTuple,
    ) -> FdbResult<&'t mut FdbTuple> {
        todo!();
    }
}

impl<'ts> TryFrom<(&'ts FdbTupleSchema, Value)> for FdbRLPartiQLTuple<'ts> {
    type Error = FdbError;

    fn try_from(
        (fdb_tuple_schema_ref, partiql_value): (&'ts FdbTupleSchema, Value),
    ) -> FdbResult<FdbRLPartiQLTuple<'ts>> {
        todo!();
    }
}
