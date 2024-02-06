use bytes::Bytes;

use fdb::error::{FdbError, FdbResult};
use fdb::tuple::{Null as FdbTupleNull, Tuple as FdbTuple, TupleSchema as FdbTupleSchema};

use partiql_value::{List, Value};

use std::convert::TryFrom;

use super::error::PARTIQL_FDB_TUPLE_INVALID_VALUE;

/// TODO
pub(crate) struct FdbRLPartiQLTuple<'ts> {
    tuple_schema: &'ts FdbTupleSchema,
    tuple: FdbTuple,
}

/// TODO
///
/// Can we use `Value::Tuple(_)` as a way to introduce versionstamp?
impl<'ts> FdbRLPartiQLTuple<'ts> {
    fn try_from_inner<'t>(partiql_list: List, fdb_tuple_ref: &'t mut FdbTuple) -> FdbResult<()> {
        for partiql_value in partiql_list {
            match partiql_value {
                Value::Missing
                | Value::Real(_)
                | Value::DateTime(_)
                | Value::Bag(_)
                | Value::Tuple(_) => return Err(FdbError::new(PARTIQL_FDB_TUPLE_INVALID_VALUE)),
                Value::Null => fdb_tuple_ref.push_back(FdbTupleNull),
                Value::Boolean(b) => fdb_tuple_ref.push_back(b),
                Value::Integer(i) => fdb_tuple_ref.push_back(i),
                Value::Decimal(boxed_decimal) => fdb_tuple_ref.push_back(
                    f64::try_from(*boxed_decimal)
                        .map_err(|_| FdbError::new(PARTIQL_FDB_TUPLE_INVALID_VALUE))?,
                ),
                Value::String(boxed_string) => fdb_tuple_ref.push_back(*boxed_string),
                Value::Blob(boxed_vec_u8) => fdb_tuple_ref.push_back(Bytes::from(*boxed_vec_u8)),
                Value::List(boxed_list) => {
                    let mut inner_fdb_tuple = FdbTuple::new();
                    FdbRLPartiQLTuple::try_from_inner(*boxed_list, &mut inner_fdb_tuple)?;
                    fdb_tuple_ref.push_back(inner_fdb_tuple);
                }
            }
        }

        Ok(())
    }
}

impl<'ts> TryFrom<(&'ts FdbTupleSchema, Value)> for FdbRLPartiQLTuple<'ts> {
    type Error = FdbError;

    fn try_from(
        (fdb_tuple_schema_ref, partiql_value): (&'ts FdbTupleSchema, Value),
    ) -> FdbResult<FdbRLPartiQLTuple<'ts>> {
        if let Value::List(boxed_list) = partiql_value {
            let mut fdb_tuple = FdbTuple::new();
            FdbRLPartiQLTuple::try_from_inner(*boxed_list, &mut fdb_tuple)?;

            if fdb_tuple_schema_ref.validate(&fdb_tuple) {
                Ok(FdbRLPartiQLTuple {
                    tuple_schema: fdb_tuple_schema_ref,
                    tuple: fdb_tuple,
                })
            } else {
                Err(FdbError::new(PARTIQL_FDB_TUPLE_INVALID_VALUE))
            }
        } else {
            Err(FdbError::new(PARTIQL_FDB_TUPLE_INVALID_VALUE))
        }
    }
}

// TODO: Write tests.
