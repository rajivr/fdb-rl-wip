//! TODO
use fdb::error::{FdbError, FdbResult};
use fdb::tuple::Tuple as FdbTuple;

use partiql_value::{Bag, List, Value};

use std::collections::HashMap;
use std::iter::FromIterator;

use super::error::PARTIQL_FDB_TUPLE_INVALID_PRIMARY_KEY_VALUE;

/// TODO
pub fn primary_key_value(value: Value) -> FdbResult<FdbTuple> {
    // Extract PartiQL array from Bag.
    let partiql_list = if let Value::Bag(boxed_bag) = value {
        Some(boxed_bag)
    } else {
        None
    }
    .map(|boxed_bag| *boxed_bag)
    .map(Bag::to_vec)
    .map(|vec| {
        let len = vec.len();
        (vec, len)
    })
    .and_then(|(mut vec, len)| if len == 1 { vec.pop() } else { None })
    .and_then(|val| {
        if let Value::List(boxed_list) = val {
            Some(boxed_list)
        } else {
            None
        }
    })
    .map(|boxed_list| *boxed_list)
    .map(List::to_vec)
    .ok_or_else(|| FdbError::new(PARTIQL_FDB_TUPLE_INVALID_PRIMARY_KEY_VALUE))?;

    let mut fdb_tuple = FdbTuple::new();

    for val in partiql_list {
        let mut partiql_tuple = if let Value::Tuple(boxed_tuple) = val {
            Some(boxed_tuple)
        } else {
            None
        }
        .map(|boxed_tuple| *boxed_tuple)
        .and_then(|tuple| {
            if tuple.len() == 2 {
                Some(HashMap::<String, Value>::from_iter(tuple.into_pairs()))
            } else {
                None
            }
        })
        .ok_or_else(|| FdbError::new(PARTIQL_FDB_TUPLE_INVALID_PRIMARY_KEY_VALUE))?;

        if partiql_tuple.contains_key("fdb_type") && partiql_tuple.contains_key("fdb_value") {
            let (fdb_type, fdb_value) = partiql_tuple
                .remove("fdb_type")
                .and_then(|x| {
                    if let Value::String(boxed_string) = x {
                        Some(boxed_string)
                    } else {
                        None
                    }
                    .map(|boxed_string| *boxed_string)
                })
                .and_then(|fdb_type| {
                    partiql_tuple
                        .remove("fdb_value")
                        .map(|fdb_value| (fdb_type, fdb_value))
                })
                .ok_or_else(|| FdbError::new(PARTIQL_FDB_TUPLE_INVALID_PRIMARY_KEY_VALUE))?;

	    // TODO: continue from here.
	    todo!();

        } else {
            return Err(FdbError::new(PARTIQL_FDB_TUPLE_INVALID_PRIMARY_KEY_VALUE));
        }
    }

    todo!();
}

/// TODO
pub fn index_value(value: Value) -> FdbResult<Vec<FdbTuple>> {
    todo!();
}
