//! TODO
use bytes::Bytes;

use fdb::error::{FdbError, FdbResult};
use fdb::tuple::{Null as FdbTupleNull, Tuple as FdbTuple, Versionstamp};

use partiql_value::{Bag, BindingsName, List, Value};

use uuid::Uuid;

use std::collections::HashMap;
use std::convert::TryFrom;
use std::iter::FromIterator;

use super::error::{
    PARTIQL_FDB_TUPLE_INVALID_INDEX_VALUE, PARTIQL_FDB_TUPLE_INVALID_PRIMARY_KEY_VALUE,
};

// Following is the mapping between an `fdb_rl_type` (from protobuf)
// and `fdb_type`.
//
// ```
// |---------------------------------------+-------------------------+-----------------|
// | fdb_rl_type                           | TupleSchemaElement      | fdb_type        |
// |---------------------------------------+-------------------------+-----------------|
// | double                                | Double  or MaybeDouble  | double          |
// | float                                 | Float   or MaybeFloat   | float           |
// | int32                                 | Integer or MaybeInteger | integer         |
// | int64                                 | Integer or MaybeInteger | integer         |
// | sint32                                | Integer or MaybeInteger | integer         |
// | sint64                                | Integer or MaybeInteger | integer         |
// | sfixed32                              | Integer or MaybeInteger | integer         |
// | sfixed64                              | Integer or MaybeInteger | integer         |
// | bool                                  | Boolean or MaybeBoolean | bool            |
// | string                                | String  or MaybeString  | string          |
// | bytes                                 | Bytes   or MaybeBytes   | bytes           |
// | message_fdb_rl.field.v1.UUID          | Uuid    or MaybeUuid    | v1_uuid         |
// | ...                                   | Versionstamp            | versionstamp    |
// | repeated_double                       | ListOfDouble            | list_of_double  |
// | repeated_float                        | ListOfFloat             | list_of_float   |
// | repeated_int32                        | ListOfInteger           | list_of_integer |
// | repeated_int64                        | ListOfInteger           | list_of_integer |
// | repeated_sint32                       | ListOfInteger           | list_of_integer |
// | repeated_sint64                       | ListOfInteger           | list_of_integer |
// | repeated_sfixed32                     | ListOfInteger           | list_of_integer |
// | repeated_sfixed64                     | ListOfInteger           | list_of_integer |
// | repeated_bool                         | ListOfBoolean           | list_of_bool    |
// | repeated_string                       | ListOfString            | list_of_string  |
// | repeated_bytes                        | ListOfBytes             | list_of_bytes   |
// | repeated_message_fdb_rl.field.v1.UUID | ListOfUuid              | list_of_v1_uuid |
// |---------------------------------------+-------------------------+-----------------|
// ```
//
// Primary key cannot have `NULL`. So `TupleSchemaElement` of
// `Maybe...` will not work for primary key.
//
// `fdb_type` of `list_of_...` is only supported for secondary
// index. Additionally it *cannot* be `NULL`. While the list can be
// empty, Protobuf does not allow `repeated` along with `optional`.
//
// `fdb_type` of `versionstamp` is only supported for secondary index
// and not primary key. Additionally it *cannot* be `NULL`. It has the
// following structure for `fdb_value`.
//
// ```
// { 'incarnation_version': <number>|NULL, 'local_version': <number> }
// ```
//
// Since `incarnation_version` is passed into index function as an
// `Option<u64>` you can create `<number>|NULL` by doing the following
// in the index function query.
//
// ```
// incarnation_version
//     .map(|x| format!("{}", x))
//     .unwrap_or_else(|| "NULL".to_string())
// ```

/// TODO

//
// The form that we expect is as follows.
//
// ```
// <<[{ 'fdb_type': '...', 'fdb_value': '...' }, { ... }]>>
// ```
//
// It is a bag of arrays. The bag must contain atleast one array.
// Within the array, there must be a tuples. Each tuple must have two
// attributes - `fdb_type` and `fdb_value`.
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
    .and_then(|vec| if vec.len() == 0 { None } else { Some(vec) })
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

            match fdb_type.as_str() {
                "string" => {
                    let tuple_string = if let Value::String(boxed_string) = fdb_value {
                        Some(boxed_string)
                    } else {
                        None
                    }
                    .map(|boxed_string| *boxed_string)
                    .ok_or_else(|| FdbError::new(PARTIQL_FDB_TUPLE_INVALID_PRIMARY_KEY_VALUE))?;

                    fdb_tuple.push_back(tuple_string);
                }
                "double" => {
                    // While we do not create `Value::Real` variant in
                    // `partiql_value_double`, it may get created when
                    // using macros. So, we check for it as well.
                    let f = match fdb_value {
                        Value::Real(ordered_float) => Ok(ordered_float.into_inner()),
                        Value::Decimal(boxed_decimal) => {
                            f64::try_from(*boxed_decimal).map_err(|_| {
                                FdbError::new(PARTIQL_FDB_TUPLE_INVALID_PRIMARY_KEY_VALUE)
                            })
                        }
                        _ => Err(FdbError::new(PARTIQL_FDB_TUPLE_INVALID_PRIMARY_KEY_VALUE)),
                    }?;

                    fdb_tuple.push_back(f);
                }
                "float" => {
                    // While we do not create `Value::Real` variant in
                    // `partiql_value_float`, it may get created when
                    // using macros. So, we check for it as well.
                    let f = match fdb_value {
                        Value::Real(ordered_float) => Ok(ordered_float.into_inner() as f32),
                        Value::Decimal(boxed_decimal) => {
                            f32::try_from(*boxed_decimal).map_err(|_| {
                                FdbError::new(PARTIQL_FDB_TUPLE_INVALID_PRIMARY_KEY_VALUE)
                            })
                        }
                        _ => Err(FdbError::new(PARTIQL_FDB_TUPLE_INVALID_PRIMARY_KEY_VALUE)),
                    }?;

                    fdb_tuple.push_back(f);
                }
                "integer" => {
                    let i = if let Value::Integer(i) = fdb_value {
                        Some(i)
                    } else {
                        None
                    }
                    .ok_or_else(|| FdbError::new(PARTIQL_FDB_TUPLE_INVALID_PRIMARY_KEY_VALUE))?;

                    fdb_tuple.push_back(i);
                }
                "bool" => {
                    let b = if let Value::Boolean(i) = fdb_value {
                        Some(i)
                    } else {
                        None
                    }
                    .ok_or_else(|| FdbError::new(PARTIQL_FDB_TUPLE_INVALID_PRIMARY_KEY_VALUE))?;

                    fdb_tuple.push_back(b);
                }
                "bytes" => {
                    let b = if let Value::Blob(boxed_bytes) = fdb_value {
                        Some(boxed_bytes)
                    } else {
                        None
                    }
                    .map(|boxed_bytes| *boxed_bytes)
                    .map(Bytes::from)
                    .ok_or_else(|| FdbError::new(PARTIQL_FDB_TUPLE_INVALID_PRIMARY_KEY_VALUE))?;

                    fdb_tuple.push_back(b);
                }
                "v1_uuid" => {
                    let uuid = if let Value::Tuple(boxed_tuple) = fdb_value {
                        Some(boxed_tuple)
                    } else {
                        None
                    }
                    .map(|boxed_tuple| *boxed_tuple)
                    .and_then(|tuple| if tuple.len() == 1 { Some(tuple) } else { None })
                    .and_then(|t| t.take_val(&BindingsName::CaseSensitive("uuid_value".into())))
                    .and_then(|v| {
                        if let Value::Blob(boxed_bytes) = v {
                            Some(boxed_bytes)
                        } else {
                            None
                        }
                    })
                    .map(|boxed_bytes| *boxed_bytes)
                    .and_then(|b| Uuid::from_slice(b.as_slice()).ok())
                    .ok_or_else(|| FdbError::new(PARTIQL_FDB_TUPLE_INVALID_PRIMARY_KEY_VALUE))?;

                    fdb_tuple.push_back(uuid);
                }
                _ => return Err(FdbError::new(PARTIQL_FDB_TUPLE_INVALID_PRIMARY_KEY_VALUE)),
            }
        } else {
            return Err(FdbError::new(PARTIQL_FDB_TUPLE_INVALID_PRIMARY_KEY_VALUE));
        }
    }

    Ok(fdb_tuple)
}

/// TODO

// There are two forms that we can accept.  Both forms are bag of
// tuples. This bag must contain atleast one tuple. It is an error if
// you call with an empty bag.
//
// The first form is as follows.
//
// ```
// <<
//   { key: [{ 'fdb_type': '...', 'fdb_value': '...' }, { ... }] },
//   { key: [{ 'fdb_type': '...', 'fdb_value': '...' }, { ... }] },
//   { key: [{ 'fdb_type': '...', 'fdb_value': '...' }, { ... }] },
//   { ... },
// >>
// ```
//
// In this form only the key part of the index is generated, and the
// value is empty (It would be an empty FoundationDB Tuple).
//
// The second form is similar to the first from but now there will be
// an additional attribute `value`.
//
// ```
// <<
//   { key: [{ 'fdb_type': '...', 'fdb_value': '...' }, { ... }],
//     value: [{ 'fdb_type': '...', 'fdb_value': '...' }, { ... }] },
//   { key: [{ 'fdb_type': '...', 'fdb_value': '...' }, { ... }] },
//     value: [{ 'fdb_type': '...', 'fdb_value': '...' }, { ... }] },
//   { key: [{ 'fdb_type': '...', 'fdb_value': '...' }, { ... }] },
//     value: [{ 'fdb_type': '...', 'fdb_value': '...' }, { ... }] },
//   { ... },
// >>
// ```
//
// The second form can be used to build "covering index".
//
// We allow the bag to be empty. We will get an empty bag when we
// attempt to build an index by unnesting a repeated field that is
// empty. In protobuf we are not allowed to have `optional repeated`.
pub fn index_value(value: Value) -> FdbResult<Vec<(FdbTuple, Option<FdbTuple>)>> {
    let partiql_bag_vec = if let Value::Bag(boxed_bag) = value {
        Some(boxed_bag)
    } else {
        None
    }
    .map(|boxed_bag| *boxed_bag)
    .map(Bag::to_vec)
    .ok_or_else(|| FdbError::new(PARTIQL_FDB_TUPLE_INVALID_INDEX_VALUE))?;

    let mut res = Vec::new();

    for val in partiql_bag_vec {
        let (partiql_list_key, maybe_partiql_list_value) = if let Value::Tuple(boxed_tuple) = val {
            Some(boxed_tuple)
        } else {
            None
        }
        .map(|boxed_tuple| *boxed_tuple)
        .map(|tuple| HashMap::<String, Value>::from_iter(tuple.into_pairs()))
        .and_then(|mut tuple_hash_map| {
            // Returning `None` would indicate an error. We can have
            // either `Some((partiql_list, None))` when we have only
            // `key` or `Some((partiql_list, Some(partiql_List)))`
            // when we have `key` *and* `value`.
            //
            // First we check if `tuple_hash_map` is well formed.
            //
            // Then we attempt to extract `partiql_list`.
            if tuple_hash_map.len() == 1 && tuple_hash_map.contains_key("key") {
                Some((tuple_hash_map.remove("key")?, None))
            } else if tuple_hash_map.len() == 2
                && tuple_hash_map.contains_key("key")
                && tuple_hash_map.contains_key("value")
            {
                tuple_hash_map.remove("key").and_then(|key| {
                    tuple_hash_map
                        .remove("value")
                        .map(|value| (key, Some(value)))
                })
            } else {
                None
            }
            .and_then(|(key, maybe_value)| {
                // Returning `None` would be an error. Otherwise we
                // would need to return `Some((partiql_list,
                // maybe_partial_list))`. The `maybe_partial_list` can
                // be a `Some(partiql_list)` or `None`.
                //
                // We use `.ok_or_else(...)` to temporarily create a
                // value of `Result` type and then use `.ok()` to
                // convert back to a value of `Option` type.
                if let Value::List(boxed_list) = key {
                    Some(boxed_list)
                } else {
                    None
                }
                .map(|boxed_list| *boxed_list)
                .ok_or_else(|| ())
                .and_then(|key_list| match maybe_value {
                    Some(value) => if let Value::List(boxed_list) = value {
                        Some(boxed_list)
                    } else {
                        None
                    }
                    .map(|boxed_list| *boxed_list)
                    .map(|value_list| (key_list, Some(value_list)))
                    .ok_or_else(|| ()),
                    None => Ok((key_list, None)),
                })
                .ok()
            })
        })
        .ok_or_else(|| FdbError::new(PARTIQL_FDB_TUPLE_INVALID_INDEX_VALUE))?;

        let fdb_tuple_key = index_value_inner(partiql_list_key)?;

        let maybe_fdb_tuple_value = match maybe_partiql_list_value {
            Some(partiql_list_value) => Some(index_value_inner(partiql_list_value)?),
            None => None,
        };

        res.push((fdb_tuple_key, maybe_fdb_tuple_value));
    }

    Ok(res)
}

fn index_value_inner(list: List) -> FdbResult<FdbTuple> {
    if list.len() == 0 {
        return Err(FdbError::new(PARTIQL_FDB_TUPLE_INVALID_INDEX_VALUE));
    }

    let mut fdb_tuple = FdbTuple::new();

    for val in list {
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
        .ok_or_else(|| FdbError::new(PARTIQL_FDB_TUPLE_INVALID_INDEX_VALUE))?;

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
                .ok_or_else(|| FdbError::new(PARTIQL_FDB_TUPLE_INVALID_INDEX_VALUE))?;

            match fdb_type.as_str() {
                "string" => match fdb_value {
                    Value::String(boxed_string) => fdb_tuple.push_back(*boxed_string),
                    Value::Null => fdb_tuple.push_back(FdbTupleNull),
                    _ => return Err(FdbError::new(PARTIQL_FDB_TUPLE_INVALID_INDEX_VALUE)),
                },
                // While we do not create `Value::Real` variant in
                // `partiql_value_double`, it may get created when
                // using macros. So, we check for it as well.
                "double" => match fdb_value {
                    Value::Real(ordered_float) => fdb_tuple.push_back(ordered_float.into_inner()),
                    Value::Decimal(boxed_decimal) => fdb_tuple.push_back(
                        f64::try_from(*boxed_decimal)
                            .map_err(|_| FdbError::new(PARTIQL_FDB_TUPLE_INVALID_INDEX_VALUE))?,
                    ),
                    Value::Null => fdb_tuple.push_back(FdbTupleNull),
                    _ => return Err(FdbError::new(PARTIQL_FDB_TUPLE_INVALID_INDEX_VALUE)),
                },
                "float" => match fdb_value {
                    Value::Real(ordered_float) => {
                        fdb_tuple.push_back(ordered_float.into_inner() as f32)
                    }
                    Value::Decimal(boxed_decimal) => fdb_tuple.push_back(
                        f32::try_from(*boxed_decimal)
                            .map_err(|_| FdbError::new(PARTIQL_FDB_TUPLE_INVALID_INDEX_VALUE))?,
                    ),
                    Value::Null => fdb_tuple.push_back(FdbTupleNull),
                    _ => return Err(FdbError::new(PARTIQL_FDB_TUPLE_INVALID_INDEX_VALUE)),
                },
                "integer" => match fdb_value {
                    Value::Integer(i) => fdb_tuple.push_back(i),
                    Value::Null => fdb_tuple.push_back(FdbTupleNull),
                    _ => return Err(FdbError::new(PARTIQL_FDB_TUPLE_INVALID_INDEX_VALUE)),
                },
                "bool" => match fdb_value {
                    Value::Boolean(i) => fdb_tuple.push_back(i),
                    Value::Null => fdb_tuple.push_back(FdbTupleNull),
                    _ => return Err(FdbError::new(PARTIQL_FDB_TUPLE_INVALID_INDEX_VALUE)),
                },
                "bytes" => match fdb_value {
                    Value::Blob(boxed_bytes) => fdb_tuple.push_back(Bytes::from(*boxed_bytes)),
                    Value::Null => fdb_tuple.push_back(FdbTupleNull),
                    _ => return Err(FdbError::new(PARTIQL_FDB_TUPLE_INVALID_INDEX_VALUE)),
                },
                "v1_uuid" => match fdb_value {
                    Value::Tuple(boxed_tuple) => {
                        let uuid = Some(*boxed_tuple)
                            .and_then(|tuple| if tuple.len() == 1 { Some(tuple) } else { None })
                            .and_then(|t| {
                                t.take_val(&BindingsName::CaseSensitive("uuid_value".into()))
                            })
                            .and_then(|v| {
                                if let Value::Blob(boxed_bytes) = v {
                                    Some(boxed_bytes)
                                } else {
                                    None
                                }
                            })
                            .map(|boxed_bytes| *boxed_bytes)
                            .and_then(|b| Uuid::from_slice(b.as_slice()).ok())
                            .ok_or_else(|| FdbError::new(PARTIQL_FDB_TUPLE_INVALID_INDEX_VALUE))?;
                        fdb_tuple.push_back(uuid);
                    }
                    Value::Null => fdb_tuple.push_back(FdbTupleNull),
                    _ => return Err(FdbError::new(PARTIQL_FDB_TUPLE_INVALID_INDEX_VALUE)),
                },
                "versionstamp" => match fdb_value {
                    Value::Tuple(boxed_tuple) => {
                        let (incarnation_version, local_version) = {
                            // We attempt to return
                            // `Option<(Option<i64>, u16)>`. Returning
                            // `None` would indicate an error. `None`
                            // is converted into an
                            // `Err(FdbError::new(PARTIQL_FDB_TUPLE_INVALID_INDEX_VALUE))`
                            // at the end.
                            //
                            // When we return
                            // `Some((incarnation_version,
                            // local_version))`, `incarnation_version`
                            // has to be a value of type
                            // `Option<i64>`. `local_version` has to
                            // be a value of type `u16`.
                            Some(*boxed_tuple)
                        }
                        .map(|tuple| HashMap::<String, Value>::from_iter(tuple.into_pairs()))
                        .and_then(|mut tuple_hash_map| {
                            if tuple_hash_map.len() == 2
                                && tuple_hash_map.contains_key("incarnation_version")
                                && tuple_hash_map.contains_key("local_version")
                            {
                                tuple_hash_map
                                    .remove("incarnation_version")
                                    .and_then(|incarnation_version| match incarnation_version {
                                        Value::Integer(i) => Some(Some(i)),
                                        Value::Null => Some(None),
                                        _ => None,
                                    })
                                    .and_then(|incarnation_version| {
                                        tuple_hash_map
                                            .remove("local_version")
                                            .and_then(|local_version| {
                                                if let Value::Integer(i) = local_version {
                                                    u16::try_from(i).ok()
                                                } else {
                                                    None
                                                }
                                            })
                                            .map(|local_version| {
                                                (incarnation_version, local_version)
                                            })
                                    })
                            } else {
                                None
                            }
                        })
                        .ok_or_else(|| FdbError::new(PARTIQL_FDB_TUPLE_INVALID_INDEX_VALUE))?;

                        match incarnation_version {
                            Some(iv) => {
                                let tup: (i64, Versionstamp) =
                                    (iv, Versionstamp::incomplete(local_version));
                                fdb_tuple.push_back(tup.0);
                                fdb_tuple.push_back(tup.1);
                            }
                            None => {
                                let tup: (FdbTupleNull, Versionstamp) =
                                    (FdbTupleNull, Versionstamp::incomplete(local_version));
                                fdb_tuple.push_back(tup.0);
                                fdb_tuple.push_back(tup.1);
                            }
                        }
                    }
                    _ => return Err(FdbError::new(PARTIQL_FDB_TUPLE_INVALID_INDEX_VALUE)),
                },
                // TODO:
                // `list_of_string`
                // `list_of_double`
                // `list_of_float`
                // `list_of_integer`
                // `list_of_bool`
                // `list_of_bytes`
                // `list_of_v1_uuid`
                _ => return Err(FdbError::new(PARTIQL_FDB_TUPLE_INVALID_INDEX_VALUE)),
            }
        } else {
            return Err(FdbError::new(PARTIQL_FDB_TUPLE_INVALID_INDEX_VALUE));
        }
    }

    Ok(fdb_tuple)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use fdb::error::FdbError;
    use fdb::tuple::{Null as FdbTupleNull, Tuple as FdbTuple, Versionstamp};

    use partiql_value::{bag, list, tuple, Value};

    use uuid::Uuid;

    use super::super::error::{
        PARTIQL_FDB_TUPLE_INVALID_INDEX_VALUE, PARTIQL_FDB_TUPLE_INVALID_PRIMARY_KEY_VALUE,
    };

    #[test]
    fn primary_key_value() {
        // Valid cases.
        {
            // string type
            {
                // single
                {
                    let result = super::primary_key_value(
                        bag![list![tuple![
                            ("fdb_type", "string"),
                            ("fdb_value", "hello"),
                        ],],]
                        .into(),
                    )
                    .unwrap();

                    let expected = {
                        let tup: (&'static str,) = ("hello",);

                        let mut t = FdbTuple::new();
                        t.push_back(tup.0.to_string());
                        t
                    };

                    assert_eq!(result, expected);
                }
                // multiple
                {
                    let result = super::primary_key_value(
                        bag![list![
                            tuple![("fdb_type", "string"), ("fdb_value", "hello"),],
                            tuple![("fdb_type", "string"), ("fdb_value", "world"),]
                        ],]
                        .into(),
                    )
                    .unwrap();

                    let expected = {
                        let tup: (&'static str, &'static str) = ("hello", "world");

                        let mut t = FdbTuple::new();
                        t.push_back(tup.0.to_string());
                        t.push_back(tup.1.to_string());
                        t
                    };

                    assert_eq!(result, expected);
                }
                // change order inside `tuple!`
                {
                    let result = super::primary_key_value(
                        bag![list![tuple![
                            ("fdb_value", "hello"),
                            ("fdb_type", "string"),
                        ],],]
                        .into(),
                    )
                    .unwrap();

                    let expected = {
                        let tup: (&'static str,) = ("hello",);

                        let mut t = FdbTuple::new();
                        t.push_back(tup.0.to_string());
                        t
                    };

                    assert_eq!(result, expected);
                }
            }
            // double type
            {
                // single
                {
                    let result = super::primary_key_value(
                        bag![list![tuple![("fdb_type", "double"), ("fdb_value", 3.14),],],].into(),
                    )
                    .unwrap();

                    let expected = {
                        let tup: (f64,) = (3.14,);

                        let mut t = FdbTuple::new();
                        t.push_back(tup.0);
                        t
                    };

                    assert_eq!(result, expected);
                }
                // multiple
                {
                    let result = super::primary_key_value(
                        bag![list![
                            tuple![("fdb_type", "double"), ("fdb_value", 3.14),],
                            tuple![("fdb_type", "double"), ("fdb_value", 6.28),]
                        ],]
                        .into(),
                    )
                    .unwrap();

                    let expected = {
                        let tup: (f64, f64) = (3.14, 6.28);

                        let mut t = FdbTuple::new();
                        t.push_back(tup.0);
                        t.push_back(tup.1);
                        t
                    };

                    assert_eq!(result, expected);
                }
                // mixed
                {
                    let result = super::primary_key_value(
                        bag![list![
                            tuple![("fdb_type", "string"), ("fdb_value", "hello"),],
                            tuple![("fdb_type", "double"), ("fdb_value", 3.14),]
                        ],]
                        .into(),
                    )
                    .unwrap();

                    let expected = {
                        let tup: (&'static str, f64) = ("hello", 3.14);

                        let mut t = FdbTuple::new();
                        t.push_back(tup.0.to_string());
                        t.push_back(tup.1);
                        t
                    };

                    assert_eq!(result, expected);
                }
            }
            // float type
            {
                // single
                {
                    let result = super::primary_key_value(
                        bag![list![tuple![("fdb_type", "float"), ("fdb_value", 3.14),],],].into(),
                    )
                    .unwrap();

                    let expected = {
                        let tup: (f32,) = (3.14,);

                        let mut t = FdbTuple::new();
                        t.push_back(tup.0);
                        t
                    };

                    assert_eq!(result, expected);
                }
                // multiple
                {
                    let result = super::primary_key_value(
                        bag![list![
                            tuple![("fdb_type", "float"), ("fdb_value", 3.14),],
                            tuple![("fdb_type", "float"), ("fdb_value", 6.28),]
                        ],]
                        .into(),
                    )
                    .unwrap();

                    let expected = {
                        let tup: (f32, f32) = (3.14, 6.28);

                        let mut t = FdbTuple::new();
                        t.push_back(tup.0);
                        t.push_back(tup.1);
                        t
                    };

                    assert_eq!(result, expected);
                }
                // mixed
                {
                    let result = super::primary_key_value(
                        bag![list![
                            tuple![("fdb_type", "float"), ("fdb_value", 3.14),],
                            tuple![("fdb_type", "double"), ("fdb_value", 3.14),]
                        ],]
                        .into(),
                    )
                    .unwrap();

                    let expected = {
                        let tup: (f32, f64) = (3.14, 3.14);

                        let mut t = FdbTuple::new();
                        t.push_back(tup.0);
                        t.push_back(tup.1);
                        t
                    };

                    assert_eq!(result, expected);
                }
            }
            // integer type
            {
                // single
                {
                    let result = super::primary_key_value(
                        bag![list![tuple![("fdb_type", "integer"), ("fdb_value", 108),],],].into(),
                    )
                    .unwrap();

                    let expected = {
                        let tup: (i8,) = (108,);

                        let mut t = FdbTuple::new();
                        t.push_back(tup.0);
                        t
                    };

                    assert_eq!(result, expected);
                }
                // multiple
                {
                    let result = super::primary_key_value(
                        bag![list![
                            tuple![("fdb_type", "integer"), ("fdb_value", 108),],
                            tuple![("fdb_type", "integer"), ("fdb_value", 216),]
                        ],]
                        .into(),
                    )
                    .unwrap();

                    let expected = {
                        let tup: (i16, i32) = (108, 216);

                        let mut t = FdbTuple::new();
                        t.push_back(tup.0);
                        t.push_back(tup.1);
                        t
                    };

                    assert_eq!(result, expected);
                }
                // mixed
                {
                    let result = super::primary_key_value(
                        bag![list![
                            tuple![("fdb_type", "integer"), ("fdb_value", 108),],
                            tuple![("fdb_type", "double"), ("fdb_value", 3.14),]
                        ],]
                        .into(),
                    )
                    .unwrap();

                    let expected = {
                        let tup: (i64, f64) = (108, 3.14);

                        let mut t = FdbTuple::new();
                        t.push_back(tup.0);
                        t.push_back(tup.1);
                        t
                    };

                    assert_eq!(result, expected);
                }
            }
            // bool type
            {
                // single
                {
                    let result = super::primary_key_value(
                        bag![list![tuple![("fdb_type", "bool"), ("fdb_value", true),],],].into(),
                    )
                    .unwrap();

                    let expected = {
                        let tup: (bool,) = (true,);

                        let mut t = FdbTuple::new();
                        t.push_back(tup.0);
                        t
                    };

                    assert_eq!(result, expected);
                }
                // multiple
                {
                    let result = super::primary_key_value(
                        bag![list![
                            tuple![("fdb_type", "bool"), ("fdb_value", true),],
                            tuple![("fdb_type", "bool"), ("fdb_value", false),]
                        ],]
                        .into(),
                    )
                    .unwrap();

                    let expected = {
                        let tup: (bool, bool) = (true, false);

                        let mut t = FdbTuple::new();
                        t.push_back(tup.0);
                        t.push_back(tup.1);
                        t
                    };

                    assert_eq!(result, expected);
                }
                // mixed
                {
                    let result = super::primary_key_value(
                        bag![list![
                            tuple![("fdb_type", "integer"), ("fdb_value", 108),],
                            tuple![("fdb_type", "bool"), ("fdb_value", true),]
                        ],]
                        .into(),
                    )
                    .unwrap();

                    let expected = {
                        let tup: (i64, bool) = (108, true);

                        let mut t = FdbTuple::new();
                        t.push_back(tup.0);
                        t.push_back(tup.1);
                        t
                    };

                    assert_eq!(result, expected);
                }
            }
            // bytes type
            {
                // single
                {
                    let result = super::primary_key_value(
                        bag![list![tuple![
                            ("fdb_type", "bytes"),
                            (
                                "fdb_value",
                                Value::Blob(Vec::<u8>::from(Bytes::from_static(b"hello")).into())
                            ),
                        ],],]
                        .into(),
                    )
                    .unwrap();

                    let expected = {
                        let tup: (Bytes,) = (Bytes::from_static(b"hello"),);

                        let mut t = FdbTuple::new();
                        t.push_back(tup.0);
                        t
                    };

                    assert_eq!(result, expected);
                }
                // multiple
                {
                    let result = super::primary_key_value(
                        bag![list![
                            tuple![
                                ("fdb_type", "bytes"),
                                (
                                    "fdb_value",
                                    Value::Blob(
                                        Vec::<u8>::from(Bytes::from_static(b"hello")).into()
                                    )
                                ),
                            ],
                            tuple![
                                ("fdb_type", "bytes"),
                                (
                                    "fdb_value",
                                    Value::Blob(
                                        Vec::<u8>::from(Bytes::from_static(b"world")).into()
                                    )
                                ),
                            ]
                        ],]
                        .into(),
                    )
                    .unwrap();

                    let expected = {
                        let tup: (Bytes, Bytes) =
                            (Bytes::from_static(b"hello"), Bytes::from_static(b"world"));

                        let mut t = FdbTuple::new();
                        t.push_back(tup.0);
                        t.push_back(tup.1);
                        t
                    };

                    assert_eq!(result, expected);
                }
                // mixed
                {
                    let result = super::primary_key_value(
                        bag![list![
                            tuple![("fdb_type", "integer"), ("fdb_value", 108),],
                            tuple![
                                ("fdb_type", "bytes"),
                                (
                                    "fdb_value",
                                    Value::Blob(
                                        Vec::<u8>::from(Bytes::from_static(b"hello")).into()
                                    )
                                ),
                            ]
                        ],]
                        .into(),
                    )
                    .unwrap();

                    let expected = {
                        let tup: (i64, Bytes) = (108, Bytes::from_static(b"hello"));

                        let mut t = FdbTuple::new();
                        t.push_back(tup.0);
                        t.push_back(tup.1);
                        t
                    };

                    assert_eq!(result, expected);
                }
            }
            // v1_uuid type
            {
                // single
                {
                    let result = super::primary_key_value(
                        bag![list![tuple![
                            ("fdb_type", "v1_uuid"),
                            (
                                "fdb_value",
                                tuple![(
                                    "uuid_value",
                                    Value::Blob(
                                        Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e")
                                            .unwrap()
                                            .as_bytes()
                                            .to_vec()
                                            .into(),
                                    )
                                )]
                            ),
                        ],],]
                        .into(),
                    )
                    .unwrap();

                    let expected = {
                        let tup: (Uuid,) =
                            (Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap(),);

                        let mut t = FdbTuple::new();
                        t.push_back(tup.0);
                        t
                    };

                    assert_eq!(result, expected);
                }
                // multiple
                {
                    let result = super::primary_key_value(
                        bag![list![
                            tuple![
                                ("fdb_type", "v1_uuid"),
                                (
                                    "fdb_value",
                                    tuple![(
                                        "uuid_value",
                                        Value::Blob(
                                            Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e")
                                                .unwrap()
                                                .as_bytes()
                                                .to_vec()
                                                .into(),
                                        )
                                    )]
                                ),
                            ],
                            tuple![
                                ("fdb_type", "v1_uuid"),
                                (
                                    "fdb_value",
                                    tuple![(
                                        "uuid_value",
                                        Value::Blob(
                                            Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e")
                                                .unwrap()
                                                .as_bytes()
                                                .to_vec()
                                                .into(),
                                        )
                                    )]
                                ),
                            ]
                        ],]
                        .into(),
                    )
                    .unwrap();

                    let expected = {
                        let tup: (Uuid, Uuid) = (
                            Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap(),
                            Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap(),
                        );

                        let mut t = FdbTuple::new();
                        t.push_back(tup.0);
                        t.push_back(tup.1);
                        t
                    };

                    assert_eq!(result, expected);
                }
                // mixed
                {
                    let result = super::primary_key_value(
                        bag![list![
                            tuple![("fdb_type", "integer"), ("fdb_value", 108),],
                            tuple![
                                ("fdb_type", "v1_uuid"),
                                (
                                    "fdb_value",
                                    tuple![(
                                        "uuid_value",
                                        Value::Blob(
                                            Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e")
                                                .unwrap()
                                                .as_bytes()
                                                .to_vec()
                                                .into(),
                                        )
                                    )]
                                ),
                            ]
                        ],]
                        .into(),
                    )
                    .unwrap();

                    let expected = {
                        let tup: (i64, Uuid) = (
                            108,
                            Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap(),
                        );

                        let mut t = FdbTuple::new();
                        t.push_back(tup.0);
                        t.push_back(tup.1);
                        t
                    };

                    assert_eq!(result, expected);
                }
            }
        }
        // Invaild cases
        {
            let table: Vec<Value> = vec![
                // Passed value not a bag
                //
                // Pass a tuple instead.
                tuple![("fdb_type", "string"), ("fdb_value", "hello"),].into(),
                // Empty bag
                bag![].into(),
                // Bag with more than one item.
                bag![
                    list![tuple![("fdb_type", "string"), ("fdb_value", "hello"),],],
                    list![tuple![("fdb_type", "string"), ("fdb_value", "world"),],]
                ]
                .into(),
                // Bag without a list
                bag![tuple![("fdb_type", "string"), ("fdb_value", "hello"),]].into(),
                // Bag with empty list (this list *must* have atleast one tuple)
                bag![list![]].into(),
                // List with non-tuple element.
                bag![list![
                    tuple![("fdb_type", "string"), ("fdb_value", "hello"),],
                    "world"
                ]]
                .into(),
                // Missing tuple attributes
                bag![list![tuple![]]].into(),
                bag![list![tuple![("fdb_type", "string")]]].into(),
                bag![list![tuple![("fdb_value", "hello")]]].into(),
                // Superfluous tuple attributes
                bag![list![tuple![
                    ("fdb_type", "string"),
                    ("fdb_value", "hello"),
                    ("abcd", "efgh")
                ],]]
                .into(),
                // Invalid tuple type
                bag![list![tuple![
                    ("fdb_type", "unknown"),
                    ("fdb_value", "hello"),
                ],]]
                .into(),
                // Invalid string value
                bag![list![tuple![("fdb_type", "string"), ("fdb_value", 3.14),],]].into(),
                // Invalid double value
                bag![list![tuple![
                    ("fdb_type", "double"),
                    ("fdb_value", "hello"),
                ],]]
                .into(),
                // Invalid float value
                bag![list![
                    tuple![("fdb_type", "float"), ("fdb_value", "hello"),],
                ]]
                .into(),
                // Invalid integer value
                bag![list![tuple![
                    ("fdb_type", "integer"),
                    ("fdb_value", "hello"),
                ],]]
                .into(),
                // Invalid bool value
                bag![list![tuple![("fdb_type", "bool"), ("fdb_value", "hello"),],]].into(),
                // Invalid bytes value
                bag![list![
                    tuple![("fdb_type", "bytes"), ("fdb_value", "hello"),],
                ]]
                .into(),
                // Invalid v1_uuid value
                bag![list![tuple![
                    ("fdb_type", "v1_uuid"),
                    ("fdb_value", "hello"),
                ],]]
                .into(),
            ];

            for value in table {
                assert_eq!(
                    super::primary_key_value(value.into()),
                    Err(FdbError::new(PARTIQL_FDB_TUPLE_INVALID_PRIMARY_KEY_VALUE))
                );
            }
        }
    }

    #[test]
    fn index_value() {
        // Valid cases.
        {
            // empty bag
            {
                let result = super::index_value(bag![].into()).unwrap();

                let expected = vec![];

                assert_eq!(result, expected);
            }
            // multiple inner tuple elements
            {
                let result = super::index_value(
                    bag![tuple![(
                        "key",
                        list![
                            tuple![("fdb_type", "string"), ("fdb_value", "hello"),],
                            tuple![("fdb_type", "string"), ("fdb_value", "world"),],
                            tuple![("fdb_type", "string"), ("fdb_value", Value::Null),]
                        ]
                    )],]
                    .into(),
                )
                .unwrap();

                let expected = vec![(
                    {
                        let tup: (&'static str, &'static str, FdbTupleNull) =
                            ("hello", "world", FdbTupleNull);

                        let mut t = FdbTuple::new();
                        t.push_back(tup.0.to_string());
                        t.push_back(tup.1.to_string());
                        t.push_back(tup.2);
                        t
                    },
                    Option::<FdbTuple>::None,
                )];

                assert_eq!(result, expected);
            }
            // string
            {
                // with `key` only
                {
                    let result = super::index_value(
                        bag![
                            tuple![(
                                "key",
                                list![tuple![("fdb_type", "string"), ("fdb_value", "hello"),]]
                            )],
                            tuple![(
                                "key",
                                list![tuple![("fdb_type", "string"), ("fdb_value", Value::Null),]]
                            )],
                        ]
                        .into(),
                    )
                    .unwrap();

                    let expected = vec![
                        (
                            {
                                let tup: (&'static str,) = ("hello",);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0.to_string());
                                t
                            },
                            Option::<FdbTuple>::None,
                        ),
                        (
                            {
                                let tup: (FdbTupleNull,) = (FdbTupleNull,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Option::<FdbTuple>::None,
                        ),
                    ];

                    assert_eq!(result, expected);
                }
                // with `key` and `value` value
                {
                    let result = super::index_value(
                        bag![
                            tuple![
                                (
                                    "key",
                                    list![tuple![("fdb_type", "string"), ("fdb_value", "hello"),]]
                                ),
                                (
                                    "value",
                                    list![tuple![
                                        ("fdb_type", "string"),
                                        ("fdb_value", Value::Null),
                                    ]]
                                )
                            ],
                            tuple![
                                (
                                    "key",
                                    list![tuple![
                                        ("fdb_type", "string"),
                                        ("fdb_value", Value::Null),
                                    ]]
                                ),
                                (
                                    "value",
                                    list![tuple![("fdb_type", "string"), ("fdb_value", "world"),]]
                                )
                            ],
                        ]
                        .into(),
                    )
                    .unwrap();

                    let expected = vec![
                        (
                            {
                                let tup: (&'static str,) = ("hello",);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0.to_string());
                                t
                            },
                            Some({
                                let tup: (FdbTupleNull,) = (FdbTupleNull,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            }),
                        ),
                        (
                            {
                                let tup: (FdbTupleNull,) = (FdbTupleNull,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Some({
                                let tup: (&'static str,) = ("world",);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0.to_string());
                                t
                            }),
                        ),
                    ];

                    assert_eq!(result, expected);
                }
            }
            // double
            {
                // with `key` only
                {
                    let result = super::index_value(
                        bag![
                            tuple![(
                                "key",
                                list![tuple![("fdb_type", "double"), ("fdb_value", 3.14),]]
                            )],
                            tuple![(
                                "key",
                                list![tuple![("fdb_type", "double"), ("fdb_value", Value::Null),]]
                            )],
                        ]
                        .into(),
                    )
                    .unwrap();

                    let expected = vec![
                        (
                            {
                                let tup: (f64,) = (3.14,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Option::<FdbTuple>::None,
                        ),
                        (
                            {
                                let tup: (FdbTupleNull,) = (FdbTupleNull,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Option::<FdbTuple>::None,
                        ),
                    ];

                    assert_eq!(result, expected);
                }
                // with `key` and `value` value
                {
                    let result = super::index_value(
                        bag![
                            tuple![
                                (
                                    "key",
                                    list![tuple![("fdb_type", "double"), ("fdb_value", 3.14),]]
                                ),
                                (
                                    "value",
                                    list![tuple![
                                        ("fdb_type", "double"),
                                        ("fdb_value", Value::Null),
                                    ]]
                                )
                            ],
                            tuple![
                                (
                                    "key",
                                    list![tuple![
                                        ("fdb_type", "double"),
                                        ("fdb_value", Value::Null),
                                    ]]
                                ),
                                (
                                    "value",
                                    list![tuple![("fdb_type", "double"), ("fdb_value", 6.28),]]
                                )
                            ],
                        ]
                        .into(),
                    )
                    .unwrap();

                    let expected = vec![
                        (
                            {
                                let tup: (f64,) = (3.14,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Some({
                                let tup: (FdbTupleNull,) = (FdbTupleNull,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            }),
                        ),
                        (
                            {
                                let tup: (FdbTupleNull,) = (FdbTupleNull,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Some({
                                let tup: (f64,) = (6.28,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            }),
                        ),
                    ];

                    assert_eq!(result, expected);
                }
            }
            // float
            {
                // with `key` only
                {
                    let result = super::index_value(
                        bag![
                            tuple![(
                                "key",
                                list![tuple![("fdb_type", "float"), ("fdb_value", 3.14),]]
                            )],
                            tuple![(
                                "key",
                                list![tuple![("fdb_type", "float"), ("fdb_value", Value::Null),]]
                            )],
                        ]
                        .into(),
                    )
                    .unwrap();

                    let expected = vec![
                        (
                            {
                                let tup: (f32,) = (3.14,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Option::<FdbTuple>::None,
                        ),
                        (
                            {
                                let tup: (FdbTupleNull,) = (FdbTupleNull,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Option::<FdbTuple>::None,
                        ),
                    ];

                    assert_eq!(result, expected);
                }
                // with `key` and `value` value
                {
                    let result = super::index_value(
                        bag![
                            tuple![
                                (
                                    "key",
                                    list![tuple![("fdb_type", "float"), ("fdb_value", 3.14),]]
                                ),
                                (
                                    "value",
                                    list![tuple![
                                        ("fdb_type", "float"),
                                        ("fdb_value", Value::Null),
                                    ]]
                                )
                            ],
                            tuple![
                                (
                                    "key",
                                    list![tuple![
                                        ("fdb_type", "float"),
                                        ("fdb_value", Value::Null),
                                    ]]
                                ),
                                (
                                    "value",
                                    list![tuple![("fdb_type", "float"), ("fdb_value", 6.28),]]
                                )
                            ],
                        ]
                        .into(),
                    )
                    .unwrap();

                    let expected = vec![
                        (
                            {
                                let tup: (f32,) = (3.14,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Some({
                                let tup: (FdbTupleNull,) = (FdbTupleNull,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            }),
                        ),
                        (
                            {
                                let tup: (FdbTupleNull,) = (FdbTupleNull,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Some({
                                let tup: (f32,) = (6.28,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            }),
                        ),
                    ];

                    assert_eq!(result, expected);
                }
            }
            // integer
            {
                // with `key` only
                {
                    let result = super::index_value(
                        bag![
                            tuple![(
                                "key",
                                list![tuple![("fdb_type", "integer"), ("fdb_value", 108),]]
                            )],
                            tuple![(
                                "key",
                                list![tuple![("fdb_type", "integer"), ("fdb_value", Value::Null),]]
                            )],
                        ]
                        .into(),
                    )
                    .unwrap();

                    let expected = vec![
                        (
                            {
                                let tup: (i8,) = (108,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Option::<FdbTuple>::None,
                        ),
                        (
                            {
                                let tup: (FdbTupleNull,) = (FdbTupleNull,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Option::<FdbTuple>::None,
                        ),
                    ];

                    assert_eq!(result, expected);
                }
                // with `key` and `value` value
                {
                    let result = super::index_value(
                        bag![
                            tuple![
                                (
                                    "key",
                                    list![tuple![("fdb_type", "integer"), ("fdb_value", 108),]]
                                ),
                                (
                                    "value",
                                    list![tuple![
                                        ("fdb_type", "float"),
                                        ("fdb_value", Value::Null),
                                    ]]
                                )
                            ],
                            tuple![
                                (
                                    "key",
                                    list![tuple![
                                        ("fdb_type", "float"),
                                        ("fdb_value", Value::Null),
                                    ]]
                                ),
                                (
                                    "value",
                                    list![tuple![("fdb_type", "integer"), ("fdb_value", 216),]]
                                )
                            ],
                        ]
                        .into(),
                    )
                    .unwrap();

                    let expected = vec![
                        (
                            {
                                let tup: (i16,) = (108,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Some({
                                let tup: (FdbTupleNull,) = (FdbTupleNull,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            }),
                        ),
                        (
                            {
                                let tup: (FdbTupleNull,) = (FdbTupleNull,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Some({
                                let tup: (i32,) = (216,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            }),
                        ),
                    ];

                    assert_eq!(result, expected);
                }
            }
            // bool
            {
                // with `key` only
                {
                    let result = super::index_value(
                        bag![
                            tuple![(
                                "key",
                                list![tuple![("fdb_type", "bool"), ("fdb_value", true),]]
                            )],
                            tuple![(
                                "key",
                                list![tuple![("fdb_type", "bool"), ("fdb_value", Value::Null),]]
                            )],
                        ]
                        .into(),
                    )
                    .unwrap();

                    let expected = vec![
                        (
                            {
                                let tup: (bool,) = (true,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Option::<FdbTuple>::None,
                        ),
                        (
                            {
                                let tup: (FdbTupleNull,) = (FdbTupleNull,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Option::<FdbTuple>::None,
                        ),
                    ];

                    assert_eq!(result, expected);
                }
                // with `key` and `value` value
                {
                    let result = super::index_value(
                        bag![
                            tuple![
                                (
                                    "key",
                                    list![tuple![("fdb_type", "bool"), ("fdb_value", true),]]
                                ),
                                (
                                    "value",
                                    list![
                                        tuple![("fdb_type", "bool"), ("fdb_value", Value::Null),]
                                    ]
                                )
                            ],
                            tuple![
                                (
                                    "key",
                                    list![
                                        tuple![("fdb_type", "bool"), ("fdb_value", Value::Null),]
                                    ]
                                ),
                                (
                                    "value",
                                    list![tuple![("fdb_type", "bool"), ("fdb_value", false),]]
                                )
                            ],
                        ]
                        .into(),
                    )
                    .unwrap();

                    let expected = vec![
                        (
                            {
                                let tup: (bool,) = (true,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Some({
                                let tup: (FdbTupleNull,) = (FdbTupleNull,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            }),
                        ),
                        (
                            {
                                let tup: (FdbTupleNull,) = (FdbTupleNull,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Some({
                                let tup: (bool,) = (false,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            }),
                        ),
                    ];

                    assert_eq!(result, expected);
                }
            }
            // bytes
            {
                // with `key` only
                {
                    let result = super::index_value(
                        bag![
                            tuple![(
                                "key",
                                list![tuple![
                                    ("fdb_type", "bytes"),
                                    (
                                        "fdb_value",
                                        Value::Blob(
                                            Vec::<u8>::from(Bytes::from_static(b"hello")).into()
                                        )
                                    ),
                                ]]
                            )],
                            tuple![(
                                "key",
                                list![tuple![("fdb_type", "bytes"), ("fdb_value", Value::Null),]]
                            )],
                        ]
                        .into(),
                    )
                    .unwrap();

                    let expected = vec![
                        (
                            {
                                let tup: (Bytes,) = (Bytes::from_static(b"hello"),);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Option::<FdbTuple>::None,
                        ),
                        (
                            {
                                let tup: (FdbTupleNull,) = (FdbTupleNull,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Option::<FdbTuple>::None,
                        ),
                    ];

                    assert_eq!(result, expected);
                }
                // with `key` and `value` value
                {
                    let result = super::index_value(
                        bag![
                            tuple![
                                (
                                    "key",
                                    list![tuple![
                                        ("fdb_type", "bytes"),
                                        (
                                            "fdb_value",
                                            Value::Blob(
                                                Vec::<u8>::from(Bytes::from_static(b"hello"))
                                                    .into()
                                            )
                                        ),
                                    ]]
                                ),
                                (
                                    "value",
                                    list![tuple![
                                        ("fdb_type", "bytes"),
                                        ("fdb_value", Value::Null),
                                    ]]
                                )
                            ],
                            tuple![
                                (
                                    "key",
                                    list![tuple![
                                        ("fdb_type", "bytes"),
                                        ("fdb_value", Value::Null),
                                    ]]
                                ),
                                (
                                    "value",
                                    list![tuple![
                                        ("fdb_type", "bytes"),
                                        (
                                            "fdb_value",
                                            Value::Blob(
                                                Vec::<u8>::from(Bytes::from_static(b"world"))
                                                    .into()
                                            )
                                        ),
                                    ]]
                                )
                            ],
                        ]
                        .into(),
                    )
                    .unwrap();

                    let expected = vec![
                        (
                            {
                                let tup: (Bytes,) = (Bytes::from_static(b"hello"),);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Some({
                                let tup: (FdbTupleNull,) = (FdbTupleNull,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            }),
                        ),
                        (
                            {
                                let tup: (FdbTupleNull,) = (FdbTupleNull,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Some({
                                let tup: (Bytes,) = (Bytes::from_static(b"world"),);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            }),
                        ),
                    ];

                    assert_eq!(result, expected);
                }
            }
            // v1_uuid
            {
                // with `key` only
                {
                    let result = super::index_value(
                        bag![
                            tuple![(
                                "key",
                                list![tuple![
                                    ("fdb_type", "v1_uuid"),
                                    (
                                        "fdb_value",
                                        tuple![(
                                            "uuid_value",
                                            Value::Blob(
                                                Uuid::parse_str(
                                                    "ffffffff-ba5e-ba11-0000-00005ca1ab1e"
                                                )
                                                .unwrap()
                                                .as_bytes()
                                                .to_vec()
                                                .into(),
                                            )
                                        )]
                                    ),
                                ]]
                            )],
                            tuple![(
                                "key",
                                list![tuple![("fdb_type", "v1_uuid"), ("fdb_value", Value::Null),]]
                            )],
                        ]
                        .into(),
                    )
                    .unwrap();

                    let expected = vec![
                        (
                            {
                                let tup: (Uuid,) =
                                    (Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e")
                                        .unwrap(),);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Option::<FdbTuple>::None,
                        ),
                        (
                            {
                                let tup: (FdbTupleNull,) = (FdbTupleNull,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Option::<FdbTuple>::None,
                        ),
                    ];

                    assert_eq!(result, expected);
                }
                // with `key` and `value` value
                {
                    let result = super::index_value(
                        bag![
                            tuple![
                                (
                                    "key",
                                    list![tuple![
                                        ("fdb_type", "v1_uuid"),
                                        (
                                            "fdb_value",
                                            tuple![(
                                                "uuid_value",
                                                Value::Blob(
                                                    Uuid::parse_str(
                                                        "ffffffff-ba5e-ba11-0000-00005ca1ab1e"
                                                    )
                                                    .unwrap()
                                                    .as_bytes()
                                                    .to_vec()
                                                    .into(),
                                                )
                                            )]
                                        ),
                                    ]]
                                ),
                                (
                                    "value",
                                    list![tuple![
                                        ("fdb_type", "v1_uuid"),
                                        ("fdb_value", Value::Null),
                                    ]]
                                )
                            ],
                            tuple![
                                (
                                    "key",
                                    list![tuple![
                                        ("fdb_type", "v1_uuid"),
                                        ("fdb_value", Value::Null),
                                    ]]
                                ),
                                (
                                    "value",
                                    list![tuple![
                                        ("fdb_type", "v1_uuid"),
                                        (
                                            "fdb_value",
                                            tuple![(
                                                "uuid_value",
                                                Value::Blob(
                                                    Uuid::parse_str(
                                                        "ffffffff-ba5e-ba11-0000-00005ca1ab1e"
                                                    )
                                                    .unwrap()
                                                    .as_bytes()
                                                    .to_vec()
                                                    .into(),
                                                )
                                            )]
                                        ),
                                    ]]
                                )
                            ],
                        ]
                        .into(),
                    )
                    .unwrap();

                    let expected = vec![
                        (
                            {
                                let tup: (Uuid,) =
                                    (Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e")
                                        .unwrap(),);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Some({
                                let tup: (FdbTupleNull,) = (FdbTupleNull,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            }),
                        ),
                        (
                            {
                                let tup: (FdbTupleNull,) = (FdbTupleNull,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Some({
                                let tup: (Uuid,) =
                                    (Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e")
                                        .unwrap(),);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            }),
                        ),
                    ];

                    assert_eq!(result, expected);
                }
            }
            // versionstamp
            {
                // with `key` only
                {
                    // In the following example we are specifying
                    // `incarnation_version` to be non-null and
                    // null. At this layer we are allowing this, but
                    // we should not see such a scenario. This needs
                    // to get checked at the higher layer.
                    //
                    // Also in most cases we should only see one one
                    // tuple entry in the bag.
                    let result = super::index_value(
                        bag![
                            tuple![(
                                "key",
                                list![tuple![
                                    ("fdb_type", "versionstamp"),
                                    (
                                        "fdb_value",
                                        tuple![("incarnation_version", 10), ("local_version", 20),]
                                    ),
                                ]]
                            )],
                            tuple![(
                                "key",
                                list![tuple![
                                    ("fdb_type", "versionstamp"),
                                    (
                                        "fdb_value",
                                        tuple![
                                            ("incarnation_version", Value::Null),
                                            ("local_version", 20),
                                        ]
                                    ),
                                ]]
                            )],
                        ]
                        .into(),
                    )
                    .unwrap();

                    let expected = vec![
                        (
                            {
                                let tup: (i64, Versionstamp) = (10, Versionstamp::incomplete(20));

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t.push_back(tup.1);
                                t
                            },
                            Option::<FdbTuple>::None,
                        ),
                        (
                            {
                                let tup: (FdbTupleNull, Versionstamp) =
                                    (FdbTupleNull, Versionstamp::incomplete(20));

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t.push_back(tup.1);
                                t
                            },
                            Option::<FdbTuple>::None,
                        ),
                    ];

                    assert_eq!(result, expected);
                }
                // with `key` and `value` value
                {
                    let result = super::index_value(
                        bag![
                            tuple![
                                (
                                    "key",
                                    list![tuple![("fdb_type", "string"), ("fdb_value", "hello"),]]
                                ),
                                (
                                    "value",
                                    list![tuple![
                                        ("fdb_type", "versionstamp"),
                                        (
                                            "fdb_value",
                                            tuple![
                                                ("incarnation_version", 10),
                                                ("local_version", 20),
                                            ]
                                        ),
                                    ]]
                                )
                            ],
                            tuple![
                                (
                                    "key",
                                    list![tuple![
                                        ("fdb_type", "string"),
                                        ("fdb_value", Value::Null),
                                    ]]
                                ),
                                (
                                    "value",
                                    list![tuple![
                                        ("fdb_type", "versionstamp"),
                                        (
                                            "fdb_value",
                                            tuple![
                                                ("incarnation_version", Value::Null),
                                                ("local_version", 20),
                                            ]
                                        ),
                                    ]]
                                )
                            ],
                        ]
                        .into(),
                    )
                    .unwrap();

                    let expected = vec![
                        (
                            {
                                let tup: (&'static str,) = ("hello",);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0.to_string());
                                t
                            },
                            Some({
                                let tup: (i64, Versionstamp) = (10, Versionstamp::incomplete(20));

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t.push_back(tup.1);
                                t
                            }),
                        ),
                        (
                            {
                                let tup: (FdbTupleNull,) = (FdbTupleNull,);

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t
                            },
                            Some({
                                let tup: (FdbTupleNull, Versionstamp) =
                                    (FdbTupleNull, Versionstamp::incomplete(20));

                                let mut t = FdbTuple::new();
                                t.push_back(tup.0);
                                t.push_back(tup.1);
                                t
                            }),
                        ),
                    ];

                    assert_eq!(result, expected);
                }
            }
            // // wip (remove later)
            // {
            //     let value = bag![
            //         tuple![(
            //             "key",
            //             list![tuple![("fdb_type", "string"), ("fdb_value", "hello"),]]
            //         )],
            //         tuple![(
            //             "key",
            //             list![tuple![("fdb_type", "string"), ("fdb_value", Value::Null),]]
            //         )]
            //     ];
            //     println!("value: {:?}", value);
            //     println!("index_value: {:?}", super::index_value(value.into()));

            //     let value = bag![
            //         tuple![
            //             (
            //                 "key",
            //                 list![tuple![("fdb_type", "string"), ("fdb_value", "hello"),]]
            //             ),
            //             (
            //                 "value",
            //                 list![tuple![("fdb_type", "string"), ("fdb_value", "world"),]]
            //             ),
            //         ],
            //         tuple![(
            //             "key",
            //             list![tuple![("fdb_type", "string"), ("fdb_value", Value::Null),]]
            //         )]
            //     ];
            //     println!("value: {:?}", value);
            //     println!("index_value: {:?}", super::index_value(value.into()));
            // }
        }
        // Invalid cases
        {
            let table: Vec<Value> = vec![
                // Passed value not a bag
                //
                // Pass a tuple instead.
                tuple![("fdb_type", "string"), ("fdb_value", "hello"),].into(),
                // Bag with a item that is not a tuple
                bag![list![tuple![
                    ("fdb_type", "string"),
                    ("fdb_value", "hello"),
                ],],]
                .into(),
                // Mislabeled `key`
                bag![tuple![(
                    "mislabled_key",
                    list![tuple![("fdb_type", "string"), ("fdb_value", "hello"),]]
                )],]
                .into(),
                // Mislabeled `value`
                bag![tuple![
                    (
                        "key",
                        list![tuple![("fdb_type", "string"), ("fdb_value", "hello"),]]
                    ),
                    (
                        "mislabeled_value",
                        list![tuple![("fdb_type", "string"), ("fdb_value", "world"),]]
                    ),
                ],]
                .into(),
                // Spurious attribute
                bag![tuple![
                    (
                        "key",
                        list![tuple![("fdb_type", "string"), ("fdb_value", "hello"),]]
                    ),
                    (
                        "value",
                        list![tuple![("fdb_type", "string"), ("fdb_value", "world"),]]
                    ),
                    (
                        "spurious_attribute",
                        list![tuple![("fdb_type", "string"), ("fdb_value", "world"),]]
                    ),
                ],]
                .into(),
                // No attribute
                bag![tuple![],].into(),
                // `key` attribute not a list
                bag![tuple![(
                    "key",
                    tuple![("fdb_type", "string"), ("fdb_value", "hello"),]
                ),],]
                .into(),
                // `value` attribute not a list
                bag![tuple![
                    (
                        "key",
                        list![tuple![("fdb_type", "string"), ("fdb_value", "hello"),]]
                    ),
                    (
                        "value",
                        tuple![("fdb_type", "string"), ("fdb_value", "world"),]
                    ),
                ],]
                .into(),
                // Empty `key` attribute
                bag![tuple![
                    ("key", list![]),
                    (
                        "value",
                        list![tuple![("fdb_type", "string"), ("fdb_value", "world"),]]
                    ),
                ],]
                .into(),
                // Empty `value` attribute
                bag![tuple![
                    (
                        "key",
                        list![tuple![("fdb_type", "string"), ("fdb_value", "world"),]]
                    ),
                    ("value", list![]),
                ],]
                .into(),
                // Missing `fdb_type` and `fdb_value` attributes
                bag![tuple![("key", list![tuple![]]),],].into(),
                bag![tuple![("key", list![tuple![("fdb_type", "string")]])],].into(),
                bag![tuple![("key", list![tuple![("fdb_value", "world"),]])],].into(),
                // Superfluous tuple attributes
                bag![tuple![(
                    "key",
                    list![tuple![
                        ("fdb_type", "string"),
                        ("fdb_value", "world"),
                        ("abcd", "efgh")
                    ]]
                )],]
                .into(),
                // Invalid tuple type
                bag![tuple![(
                    "key",
                    list![tuple![("fdb_type", "unknown"), ("fdb_value", "world")]]
                )],]
                .into(),
                // Invalid string value
                bag![tuple![(
                    "key",
                    list![tuple![("fdb_type", "string"), ("fdb_value", 3.14)]]
                )],]
                .into(),
                // Invalid double value
                bag![tuple![(
                    "key",
                    list![tuple![("fdb_type", "double"), ("fdb_value", "hello")]]
                )],]
                .into(),
                // Invalid float value
                bag![tuple![(
                    "key",
                    list![tuple![("fdb_type", "float"), ("fdb_value", "hello")]]
                )],]
                .into(),
                // Invalid integer value
                bag![tuple![(
                    "key",
                    list![tuple![("fdb_type", "integer"), ("fdb_value", "hello")]]
                )],]
                .into(),
                // Invalid bool value
                bag![tuple![(
                    "key",
                    list![tuple![("fdb_type", "bool"), ("fdb_value", "hello")]]
                )],]
                .into(),
                // Invalid bytes value
                bag![tuple![(
                    "key",
                    list![tuple![("fdb_type", "bytes"), ("fdb_value", "hello")]]
                )],]
                .into(),
                // Invalid v1_uuid value
                bag![tuple![(
                    "key",
                    list![tuple![("fdb_type", "v1_uuid"), ("fdb_value", "hello")]]
                )],]
                .into(),
                // Invalid versionstamp value (not a tuple)
                bag![tuple![(
                    "key",
                    list![tuple![("fdb_type", "versionstamp"), ("fdb_value", "hello")]]
                )],]
                .into(),
                // Invalid versionstamp value (tuple, but with NULL)
                bag![tuple![(
                    "key",
                    list![tuple![
                        ("fdb_type", "versionstamp"),
                        ("fdb_value", Value::Null)
                    ]]
                )],]
                .into(),
                // Invalid versionstamp value (mis-spelling incarnation_version)
                bag![tuple![(
                    "key",
                    list![tuple![
                        ("fdb_type", "versionstamp"),
                        (
                            "fdb_value",
                            tuple![("incarnation_version1", 10), ("local_version", 20),]
                        )
                    ]]
                )],]
                .into(),
                // Invalid versionstamp value (mis-spelling local_version)
                bag![tuple![(
                    "key",
                    list![tuple![
                        ("fdb_type", "versionstamp"),
                        (
                            "fdb_value",
                            tuple![("incarnation_version", 10), ("local_version1", 20),]
                        )
                    ]]
                )],]
                .into(),
                // Invalid versionstamp value (invalid incarnation_version type)
                bag![tuple![(
                    "key",
                    list![tuple![
                        ("fdb_type", "versionstamp"),
                        (
                            "fdb_value",
                            tuple![("incarnation_version", "hello"), ("local_version", 20),]
                        )
                    ]]
                )],]
                .into(),
                // Invalid versionstamp value (invalid local_version type)
                bag![tuple![(
                    "key",
                    list![tuple![
                        ("fdb_type", "versionstamp"),
                        (
                            "fdb_value",
                            tuple![("incarnation_version", 10), ("local_version", "hello"),]
                        )
                    ]]
                )],]
                .into(),
                // Invalid versionstamp value (invalid local_version value)
                bag![tuple![(
                    "key",
                    list![tuple![
                        ("fdb_type", "versionstamp"),
                        (
                            "fdb_value",
                            tuple![("incarnation_version", 10), ("local_version", -1),]
                        )
                    ]]
                )],]
                .into(),
                // Invalid versionstamp value (missing incarnation_version)
                bag![tuple![(
                    "key",
                    list![tuple![
                        ("fdb_type", "versionstamp"),
                        ("fdb_value", tuple![("local_version", 20),])
                    ]]
                )],]
                .into(),
                // Invalid versionstamp value (missing local_version)
                bag![tuple![(
                    "key",
                    list![tuple![
                        ("fdb_type", "versionstamp"),
                        ("fdb_value", tuple![("incarnation_version", 10),])
                    ]]
                )],]
                .into(),
                // Invalid versionstamp value (spurious attribute)
                bag![tuple![(
                    "key",
                    list![tuple![
                        ("fdb_type", "versionstamp"),
                        (
                            "fdb_value",
                            tuple![
                                ("incarnation_version", 10),
                                ("local_version", 20),
                                ("spurious_attribute", 30),
                            ]
                        )
                    ]]
                )],]
                .into(),
                // TODO: Add additional invalid types here.
            ];

            for value in table {
                assert_eq!(
                    super::index_value(value.into()),
                    Err(FdbError::new(PARTIQL_FDB_TUPLE_INVALID_INDEX_VALUE))
                );
            }
        }
    }
}
