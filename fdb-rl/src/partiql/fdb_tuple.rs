//! TODO
use bytes::Bytes;

use fdb::error::{FdbError, FdbResult};
use fdb::tuple::Tuple as FdbTuple;

use partiql_value::{Bag, BindingsName, List, Value};

use uuid::Uuid;

use std::collections::HashMap;
use std::convert::TryFrom;
use std::iter::FromIterator;

use super::error::PARTIQL_FDB_TUPLE_INVALID_PRIMARY_KEY_VALUE;

// Following is the mapping between an `fdb_rl_type` (from protobuf)
// and `fdb_type`.
//
// ```
// |------------------------------+--------------------+----------|
// | fdb_rl_type                  | TupleSchemaElement | fdb_type |
// |------------------------------+--------------------+----------|
// | string                       | String             | string   |
// | double                       | Double             | double   |
// | float                        | Float              | float    |
// | int32                        | Integer            | integer  |
// | int64                        | Integer            | integer  |
// | sint32                       | Integer            | integer  |
// | sint64                       | Integer            | integer  |
// | sfixed32                     | Integer            | integer  |
// | sfixed64                     | Integer            | integer  |
// | bool                         | Boolean            | bool     |
// | bytes                        | Bytes              | bytes    |
// | message_fdb_rl.field.v1.UUID | Uuid               | v1_uuid  |
// |------------------------------+--------------------+----------|
// ```

/// TODO
pub fn primary_key_value(value: Value) -> FdbResult<FdbTuple> {
    // The form that we expect is as follows.
    //
    // ```
    // <<[{ 'fdb_type': '...', 'fdb_value': '...' }, { ... }]>>
    // ```
    //
    // It needs to be a bag. The bag must have one item. This item
    // must be an array. Within the array, there must be a
    // tuples. Each tuple must have two attributes - `fdb_type` and
    // `fdb_value`.

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
pub fn index_value(value: Value) -> FdbResult<Vec<FdbTuple>> {
    // TODO: Continue from here.
    todo!();
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use fdb::error::FdbError;
    use fdb::tuple::Tuple as FdbTuple;

    use partiql_value::{bag, list, tuple, Value};

    use uuid::Uuid;

    use super::super::error::PARTIQL_FDB_TUPLE_INVALID_PRIMARY_KEY_VALUE;

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
}
