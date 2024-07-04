//! TODO

use fdb::error::{FdbError, FdbResult};
use fdb_rl_proto::fdb_rl::field::v1::Uuid as FdbRLWktV1UuidProto;

use prost_reflect::{
    DynamicMessage, EnumDescriptor, Kind, MapKey, MessageDescriptor, ReflectMessage,
    Value as ProstReflectValue,
};

// We do not rename `Value` to `PartiqlValue`. If we did that `tuple!`
// macro fails.
use partiql_value::{tuple, List, Tuple, Value};

use rust_decimal::Decimal;

use uuid::Uuid;

use std::borrow::Cow;
use std::convert::TryFrom;
use std::ops::Deref;

use super::error::{
    PROTOBUF_ILL_FORMED_MESSAGE, PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
};
use super::{WellFormedMessageDescriptor, FDB_RL_WKT_V1_UUID};

/// Describes a valid `DynamicMessage`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct WellFormedDynamicMessage {
    inner: DynamicMessage,
}

impl WellFormedDynamicMessage {
    fn validate_wtk_prost_reflect_value(prost_reflect_value_ref: &ProstReflectValue) -> bool {
        match prost_reflect_value_ref {
            // We should not be seeing `ProstReflectValue::{U32,U64}`
            // variants. If we see it, return an error.
            ProstReflectValue::U32(_) | ProstReflectValue::U64(_) => false,
            ProstReflectValue::Bool(_)
            | ProstReflectValue::I32(_)
            | ProstReflectValue::I64(_)
            | ProstReflectValue::F32(_)
            | ProstReflectValue::F64(_)
            | ProstReflectValue::String(_)
            | ProstReflectValue::Bytes(_)
            | ProstReflectValue::EnumNumber(_) => true,
            ProstReflectValue::Message(dynamic_message_ref) => {
                let message_descriptor = dynamic_message_ref.descriptor();

                // Check if message descriptor is a well known
                // type. If so, perform additional checks specific to
                // the well known type to ensure that it is well
                // formed.
                //
                // If it is not a well known type, then it could be a
                // nested type that could contain a well known type.
                if message_descriptor.full_name() == FDB_RL_WKT_V1_UUID.deref().as_str() {
                    // Transcode the value of `DynamicMessage` into a
                    // value of `FdbRLWktV1UuidProto`, and use the
                    // `try_from(...)`.
                    dynamic_message_ref
                        .transcode_to::<FdbRLWktV1UuidProto>()
                        .map_err(|_| ())
                        .and_then(|x| Uuid::try_from(x).map_err(|_| ()))
                        .is_ok()
                } else {
                    Self::validate_wkt(dynamic_message_ref)
                }
            }
            ProstReflectValue::List(value_list_ref) => {
                for value_ref in value_list_ref {
                    // If any of the values returns `false` return
                    // `false`. Otherwise, `true`.
                    if !Self::validate_wtk_prost_reflect_value(value_ref) {
                        return false;
                    }
                }
                true
            }
            ProstReflectValue::Map(value_map_ref) => {
                for value_ref in value_map_ref.values() {
                    // If any of the values returns `false` return
                    // `false`. Otherwise, `true`.
                    if !Self::validate_wtk_prost_reflect_value(value_ref) {
                        return false;
                    }
                }
                true
            }
        }
    }

    /// This is an internal method. Here we are only checking to make
    /// sure that any well known types inside the dynamic message is
    /// well formed. We are *not* checking if the message descriptor
    /// backing the dynamic message is well formed. It is the caller's
    /// responsibility to do that.
    fn validate_wkt(dynamic_message_ref: &DynamicMessage) -> bool {
        let message_descriptor = dynamic_message_ref.descriptor();

        for field_descriptor in message_descriptor.fields() {
            if field_descriptor.is_map() {
                // ```
                // map<string, SomeType> some_field = X;
                // ```
                //
                // The default value is an empty map, which
                // `get_field` returns.
                if !Self::validate_wtk_prost_reflect_value(
                    dynamic_message_ref.get_field(&field_descriptor).deref(),
                ) {
                    return false;
                }
            } else if field_descriptor.is_list() {
                // ```
                // repeated SomeType some_field = X;
                // ```
                //
                // The default value is an empty list, which
                // `get_field` returns.
                if !Self::validate_wtk_prost_reflect_value(
                    dynamic_message_ref.get_field(&field_descriptor).deref(),
                ) {
                    return false;
                }
            } else {
                // ```
                // optional SomeType some_field = X;
                // ```
                //
                // or
                //
                // ```
                // oneof some_oneof {
                //   SomeType some_field = X;
                // }
                // ```
                //
                // Ignore if the field is `null`.
                if dynamic_message_ref.has_field(&field_descriptor) {
                    if !Self::validate_wtk_prost_reflect_value(
                        dynamic_message_ref.get_field(&field_descriptor).deref(),
                    ) {
                        return false;
                    }
                }
            }
        }

        true
    }

    fn partiql_value_double(
        maybe_prost_reflect_value: Option<ProstReflectValue>,
    ) -> FdbResult<Tuple> {
        match maybe_prost_reflect_value {
            Some(prost_reflect_value) => if let ProstReflectValue::F64(f) = prost_reflect_value {
                Ok(f)
            } else {
                Err(FdbError::new(
                    PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                ))
            }
            .and_then(|f| {
                Decimal::try_from(f).map_err(|_| {
                    FdbError::new(PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR)
                })
            })
            .map(Box::<Decimal>::from)
            .map(Value::Decimal)
            .map(|partiql_value_decimal| {
                tuple![
                    ("fdb_rl_type", "double"),
                    ("fdb_rl_value", partiql_value_decimal)
                ]
            }),
            None => Ok(tuple![
                ("fdb_rl_type", "double"),
                ("fdb_rl_value", Value::Null)
            ]),
        }
    }

    fn partiql_value_float(
        maybe_prost_reflect_value: Option<ProstReflectValue>,
    ) -> FdbResult<Tuple> {
        match maybe_prost_reflect_value {
            Some(prost_reflect_value) => if let ProstReflectValue::F32(f) = prost_reflect_value {
                Ok(f)
            } else {
                Err(FdbError::new(
                    PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                ))
            }
            .and_then(|f| {
                Decimal::try_from(f).map_err(|_| {
                    FdbError::new(PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR)
                })
            })
            .map(Box::<Decimal>::from)
            .map(Value::Decimal)
            .map(|partiql_value_decimal| {
                tuple![
                    ("fdb_rl_type", "float"),
                    ("fdb_rl_value", partiql_value_decimal)
                ]
            }),
            None => Ok(tuple![
                ("fdb_rl_type", "float"),
                ("fdb_rl_value", Value::Null)
            ]),
        }
    }

    fn partiql_value_int32(
        maybe_prost_reflect_value: Option<ProstReflectValue>,
    ) -> FdbResult<Tuple> {
        match maybe_prost_reflect_value {
            Some(prost_reflect_value) => if let ProstReflectValue::I32(i) = prost_reflect_value {
                Ok(i)
            } else {
                Err(FdbError::new(
                    PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                ))
            }
            .map(i64::from)
            .map(Value::Integer)
            .map(|partiql_value_integer| {
                tuple![
                    ("fdb_rl_type", "int32"),
                    ("fdb_rl_value", partiql_value_integer)
                ]
            }),
            None => Ok(tuple![
                ("fdb_rl_type", "int32"),
                ("fdb_rl_value", Value::Null)
            ]),
        }
    }

    fn partiql_value_int64(
        maybe_prost_reflect_value: Option<ProstReflectValue>,
    ) -> FdbResult<Tuple> {
        match maybe_prost_reflect_value {
            Some(prost_reflect_value) => if let ProstReflectValue::I64(i) = prost_reflect_value {
                Ok(i)
            } else {
                Err(FdbError::new(
                    PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                ))
            }
            .map(Value::Integer)
            .map(|partiql_value_integer| {
                tuple![
                    ("fdb_rl_type", "int64"),
                    ("fdb_rl_value", partiql_value_integer)
                ]
            }),
            None => Ok(tuple![
                ("fdb_rl_type", "int64"),
                ("fdb_rl_value", Value::Null)
            ]),
        }
    }

    fn partiql_value_sint32(
        maybe_prost_reflect_value: Option<ProstReflectValue>,
    ) -> FdbResult<Tuple> {
        match maybe_prost_reflect_value {
            Some(prost_reflect_value) => if let ProstReflectValue::I32(i) = prost_reflect_value {
                Ok(i)
            } else {
                Err(FdbError::new(
                    PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                ))
            }
            .map(i64::from)
            .map(Value::Integer)
            .map(|partiql_value_integer| {
                tuple![
                    ("fdb_rl_type", "sint32"),
                    ("fdb_rl_value", partiql_value_integer)
                ]
            }),
            None => Ok(tuple![
                ("fdb_rl_type", "sint32"),
                ("fdb_rl_value", Value::Null)
            ]),
        }
    }

    fn partiql_value_sint64(
        maybe_prost_reflect_value: Option<ProstReflectValue>,
    ) -> FdbResult<Tuple> {
        match maybe_prost_reflect_value {
            Some(prost_reflect_value) => if let ProstReflectValue::I64(i) = prost_reflect_value {
                Ok(i)
            } else {
                Err(FdbError::new(
                    PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                ))
            }
            .map(Value::Integer)
            .map(|partiql_value_integer| {
                tuple![
                    ("fdb_rl_type", "sint64"),
                    ("fdb_rl_value", partiql_value_integer)
                ]
            }),
            None => Ok(tuple![
                ("fdb_rl_type", "sint64"),
                ("fdb_rl_value", Value::Null)
            ]),
        }
    }

    fn partiql_value_sfixed32(
        maybe_prost_reflect_value: Option<ProstReflectValue>,
    ) -> FdbResult<Tuple> {
        match maybe_prost_reflect_value {
            Some(prost_reflect_value) => if let ProstReflectValue::I32(i) = prost_reflect_value {
                Ok(i)
            } else {
                Err(FdbError::new(
                    PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                ))
            }
            .map(i64::from)
            .map(Value::Integer)
            .map(|partiql_value_integer| {
                tuple![
                    ("fdb_rl_type", "sfixed32"),
                    ("fdb_rl_value", partiql_value_integer)
                ]
            }),
            None => Ok(tuple![
                ("fdb_rl_type", "sfixed32"),
                ("fdb_rl_value", Value::Null)
            ]),
        }
    }

    fn partiql_value_sfixed64(
        maybe_prost_reflect_value: Option<ProstReflectValue>,
    ) -> FdbResult<Tuple> {
        match maybe_prost_reflect_value {
            Some(prost_reflect_value) => if let ProstReflectValue::I64(i) = prost_reflect_value {
                Ok(i)
            } else {
                Err(FdbError::new(
                    PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                ))
            }
            .map(Value::Integer)
            .map(|partiql_value_integer| {
                tuple![
                    ("fdb_rl_type", "sfixed64"),
                    ("fdb_rl_value", partiql_value_integer)
                ]
            }),
            None => Ok(tuple![
                ("fdb_rl_type", "sfixed64"),
                ("fdb_rl_value", Value::Null)
            ]),
        }
    }

    fn partiql_value_bool(
        maybe_prost_reflect_value: Option<ProstReflectValue>,
    ) -> FdbResult<Tuple> {
        match maybe_prost_reflect_value {
            Some(prost_reflect_value) => if let ProstReflectValue::Bool(b) = prost_reflect_value {
                Ok(b)
            } else {
                Err(FdbError::new(
                    PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                ))
            }
            .map(Value::Boolean)
            .map(|partiql_value_bool| {
                tuple![
                    ("fdb_rl_type", "bool"),
                    ("fdb_rl_value", partiql_value_bool)
                ]
            }),
            None => Ok(tuple![
                ("fdb_rl_type", "bool"),
                ("fdb_rl_value", Value::Null)
            ]),
        }
    }

    fn partiql_value_string(
        maybe_prost_reflect_value: Option<ProstReflectValue>,
    ) -> FdbResult<Tuple> {
        match maybe_prost_reflect_value {
            Some(prost_reflect_value) => {
                if let ProstReflectValue::String(s) = prost_reflect_value {
                    Ok(s)
                } else {
                    Err(FdbError::new(
                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                    ))
                }
                .map(Box::<String>::from)
                .map(Value::String)
                .map(|partiql_value_string| {
                    tuple![
                        ("fdb_rl_type", "string"),
                        ("fdb_rl_value", partiql_value_string)
                    ]
                })
            }
            None => Ok(tuple![
                ("fdb_rl_type", "string"),
                ("fdb_rl_value", Value::Null)
            ]),
        }
    }

    fn partiql_value_bytes(
        maybe_prost_reflect_value: Option<ProstReflectValue>,
    ) -> FdbResult<Tuple> {
        match maybe_prost_reflect_value {
            Some(prost_reflect_value) => if let ProstReflectValue::Bytes(b) = prost_reflect_value {
                Ok(Vec::<u8>::from(b))
            } else {
                Err(FdbError::new(
                    PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                ))
            }
            .map(Box::<Vec<u8>>::from)
            .map(Value::Blob)
            .map(|partiql_value_blob| {
                tuple![
                    ("fdb_rl_type", "bytes"),
                    ("fdb_rl_value", partiql_value_blob)
                ]
            }),
            None => Ok(tuple![
                ("fdb_rl_type", "bytes"),
                ("fdb_rl_value", Value::Null)
            ]),
        }
    }

    fn partiql_value_message(
        message_descriptor: MessageDescriptor,
        maybe_prost_reflect_value: Option<ProstReflectValue>,
    ) -> FdbResult<Tuple> {
        // First check if message descriptor is a well
        // known type.
        if message_descriptor.full_name() == FDB_RL_WKT_V1_UUID.deref().as_str() {
            match maybe_prost_reflect_value {
                Some(prost_reflect_value) => {
                    if let ProstReflectValue::Message(dm) = prost_reflect_value {
                        Ok(dm)
                    } else {
                        Err(FdbError::new(
                            PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                        ))
                    }
                    .and_then(|dm| {
                        dm.get_field_by_name("value")
                            .ok_or(FdbError::new(
                                PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                            ))
                            .map(Cow::into_owned)
                    })
                    .and_then(|v| {
                        if let ProstReflectValue::Bytes(b) = v {
                            Ok(b)
                        } else {
                            Err(FdbError::new(
                                PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                            ))
                        }
                    })
                    .map(|uuid_bytes| {
                        tuple![
                            (
                                "fdb_rl_type",
                                format!("message_{}", FDB_RL_WKT_V1_UUID.deref().as_str())
                            ),
                            (
                                "fdb_rl_value",
                                tuple![(
                                    "uuid_value",
                                    Value::Blob(Vec::<u8>::from(uuid_bytes).into())
                                )]
                            )
                        ]
                    })
                }
                None => Ok(tuple![
                    (
                        "fdb_rl_type",
                        format!("message_{}", FDB_RL_WKT_V1_UUID.deref().as_str())
                    ),
                    ("fdb_rl_value", Value::Null)
                ]),
            }
        } else {
            match maybe_prost_reflect_value {
                Some(prost_reflect_value) => {
                    if let ProstReflectValue::Message(dm) = prost_reflect_value {
                        Ok(dm)
                    } else {
                        Err(FdbError::new(
                            PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                        ))
                    }
                    .and_then(Self::try_from_inner)
                    .map(|partiql_value| {
                        tuple![
                            (
                                "fdb_rl_type",
                                format!("message_{}", message_descriptor.name())
                            ),
                            ("fdb_rl_value", partiql_value)
                        ]
                    })
                }
                None => Ok(tuple![
                    (
                        "fdb_rl_type",
                        format!("message_{}", message_descriptor.name())
                    ),
                    ("fdb_rl_value", Value::Null)
                ]),
            }
        }
    }

    fn partiql_value_enum(
        enum_descriptor: EnumDescriptor,
        maybe_prost_reflect_value: Option<ProstReflectValue>,
    ) -> FdbResult<Tuple> {
        match maybe_prost_reflect_value {
            Some(prost_reflect_value) => {
                if let ProstReflectValue::EnumNumber(number) = prost_reflect_value {
                    Ok(number)
                } else {
                    Err(FdbError::new(
                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                    ))
                }
                .and_then(|number| {
                    enum_descriptor
                        .get_value(number)
                        .map(|e| (e.name().to_string(), number))
                        .ok_or(FdbError::new(
                            PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                        ))
                })
                .map(|(name, number)| tuple![("name", name), ("number", number),])
                .map(Box::<Tuple>::from)
                .map(Value::Tuple)
                .map(|partiql_value_tuple| {
                    tuple![
                        ("fdb_rl_type", format!("enum_{}", enum_descriptor.name())),
                        ("fdb_rl_value", partiql_value_tuple)
                    ]
                })
            }
            None => Ok(tuple![
                ("fdb_rl_type", format!("enum_{}", enum_descriptor.name())),
                ("fdb_rl_value", Value::Null)
            ]),
        }
    }

    // We define this so that we can work with nested message without
    // having to check if the inner dynamic message is well formed or
    // not.
    //
    // When we have a value of `WellFormedDynamicMessage`, then the
    // whole message, including inner message is well formed. A well
    // formed dynamic message by definition also means that any well
    // known types within the dynamic message is also well
    // formed. Therefore we can safely convert well known types to
    // their PartiQL equivalent value.
    fn try_from_inner(dynamic_message: DynamicMessage) -> FdbResult<Value> {
        let message_descriptor = dynamic_message.descriptor();

        let mut message_tuple = Tuple::new();

        message_tuple.insert(
            "fdb_rl_type",
            Value::String(format!("message_{}", message_descriptor.name()).into()),
        );

        let mut message_fdb_rl_value_tuple = Tuple::new();

        for field_descriptor in message_descriptor.fields() {
            if field_descriptor.is_map() {
                // ```
                // map<string, SomeType> some_field = X;
                // ```

                // We need to get the type of `SomeType` and (if
                // applicable) value field message descriptor or enum
                // descriptor.
                let (
                    fdb_rl_type,
                    maybe_value_field_message_descriptor,
                    maybe_value_field_enum_descriptor,
                ) = if let Kind::Message(map_entry_message_descriptor) = field_descriptor.kind() {
                    // `map_entry_key_field` in our case is always a
                    // string`.
                    let map_entry_value_field_descriptor =
                        if map_entry_message_descriptor.is_map_entry() {
                            map_entry_message_descriptor.map_entry_value_field()
                        } else {
                            return Err(FdbError::new(
                                PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                            ));
                        };

                    match map_entry_value_field_descriptor.kind() {
                        Kind::Uint32 | Kind::Uint64 | Kind::Fixed32 | Kind::Fixed64 => {
                            return Err(FdbError::new(
                                PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                            ))
                        }
                        Kind::Double => ("double".to_string(), None, None),
                        Kind::Float => ("float".to_string(), None, None),
                        Kind::Int32 => ("int32".to_string(), None, None),
                        Kind::Int64 => ("int64".to_string(), None, None),
                        Kind::Sint32 => ("sint32".to_string(), None, None),
                        Kind::Sint64 => ("sint64".to_string(), None, None),
                        Kind::Sfixed32 => ("sfixed32".to_string(), None, None),
                        Kind::Sfixed64 => ("sfixed64".to_string(), None, None),
                        Kind::Bool => ("bool".to_string(), None, None),
                        Kind::String => ("string".to_string(), None, None),
                        Kind::Bytes => ("bytes".to_string(), None, None),
                        Kind::Message(value_field_message_descriptor) => {
                            // First check if message descriptor is a well
                            // known type.
                            if value_field_message_descriptor.full_name()
                                == FDB_RL_WKT_V1_UUID.deref().as_str()
                            {
                                (
                                    format!("message_{}", FDB_RL_WKT_V1_UUID.deref().as_str()),
                                    Some(value_field_message_descriptor),
                                    None,
                                )
                            } else {
                                (
                                    format!("message_{}", value_field_message_descriptor.name()),
                                    Some(value_field_message_descriptor),
                                    None,
                                )
                            }
                        }
                        Kind::Enum(value_field_enum_descriptor) => (
                            format!("enum_{}", value_field_enum_descriptor.name()),
                            None,
                            Some(value_field_enum_descriptor),
                        ),
                    }
                } else {
                    return Err(FdbError::new(
                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                    ));
                };

                let mut map_fdb_rl_value_tuple = Tuple::new();

                if let ProstReflectValue::Map(mut hash_map) =
                    dynamic_message.get_field(&field_descriptor).into_owned()
                {
                    for (key, val) in hash_map.drain() {
                        let tuple_key = if let MapKey::String(s) = key {
                            s
                        } else {
                            return Err(FdbError::new(
                                PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                            ));
                        };

                        // `prost_reflect::Value` does not capture
                        // `prost_reflect::Kind`
                        // information. Therefore we need to use
                        // `fdb_rl_type`.
                        //
                        // You cannot define the following in protobuf [1].
                        //
                        // ```
                        // map<string, repeated SomeType> some_field = X;
                        // map<string, map<string, SomeType>> some_field = X;
                        // ```
                        //
                        // These forms needs to be wrapped inside
                        // another message type.
                        //
                        // [1]: https://protobuf.dev/programming-guides/proto3/#maps
                        if fdb_rl_type.as_str() == "double" {
                            map_fdb_rl_value_tuple.insert(
                                tuple_key.as_str(),
                                Value::Tuple(Self::partiql_value_double(Some(val))?.into()),
                            );
                        } else if fdb_rl_type.as_str() == "float" {
                            map_fdb_rl_value_tuple.insert(
                                tuple_key.as_str(),
                                Value::Tuple(Self::partiql_value_float(Some(val))?.into()),
                            );
                        } else if fdb_rl_type.as_str() == "int32" {
                            map_fdb_rl_value_tuple.insert(
                                tuple_key.as_str(),
                                Value::Tuple(Self::partiql_value_int32(Some(val))?.into()),
                            );
                        } else if fdb_rl_type.as_str() == "int64" {
                            map_fdb_rl_value_tuple.insert(
                                tuple_key.as_str(),
                                Value::Tuple(Self::partiql_value_int64(Some(val))?.into()),
                            );
                        } else if fdb_rl_type.as_str() == "sint32" {
                            map_fdb_rl_value_tuple.insert(
                                tuple_key.as_str(),
                                Value::Tuple(Self::partiql_value_sint32(Some(val))?.into()),
                            );
                        } else if fdb_rl_type.as_str() == "sint64" {
                            map_fdb_rl_value_tuple.insert(
                                tuple_key.as_str(),
                                Value::Tuple(Self::partiql_value_sint64(Some(val))?.into()),
                            );
                        } else if fdb_rl_type.as_str() == "sfixed32" {
                            map_fdb_rl_value_tuple.insert(
                                tuple_key.as_str(),
                                Value::Tuple(Self::partiql_value_sfixed32(Some(val))?.into()),
                            );
                        } else if fdb_rl_type.as_str() == "sfixed64" {
                            map_fdb_rl_value_tuple.insert(
                                tuple_key.as_str(),
                                Value::Tuple(Self::partiql_value_sfixed64(Some(val))?.into()),
                            );
                        } else if fdb_rl_type.as_str() == "bool" {
                            map_fdb_rl_value_tuple.insert(
                                tuple_key.as_str(),
                                Value::Tuple(Self::partiql_value_bool(Some(val))?.into()),
                            );
                        } else if fdb_rl_type.as_str() == "string" {
                            map_fdb_rl_value_tuple.insert(
                                tuple_key.as_str(),
                                Value::Tuple(Self::partiql_value_string(Some(val))?.into()),
                            );
                        } else if fdb_rl_type.as_str() == "bytes" {
                            map_fdb_rl_value_tuple.insert(
                                tuple_key.as_str(),
                                Value::Tuple(Self::partiql_value_bytes(Some(val))?.into()),
                            );
                        } else if fdb_rl_type.as_str().starts_with("message_") {
                            match maybe_value_field_message_descriptor {
                                Some(ref value_field_message_descriptor) => {
                                    map_fdb_rl_value_tuple.insert(
                                        tuple_key.as_str(),
                                        Value::Tuple(
                                            Self::partiql_value_message(
                                                value_field_message_descriptor.clone(),
                                                Some(val),
                                            )?
                                            .into(),
                                        ),
                                    );
                                }
                                None => {
                                    return Err(FdbError::new(
                                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                                    ))
                                }
                            }
                        } else if fdb_rl_type.as_str().starts_with("enum_") {
                            match maybe_value_field_enum_descriptor {
                                Some(ref value_field_enum_descriptor) => {
                                    map_fdb_rl_value_tuple.insert(
                                        tuple_key.as_str(),
                                        Value::Tuple(
                                            Self::partiql_value_enum(
                                                value_field_enum_descriptor.clone(),
                                                Some(val),
                                            )?
                                            .into(),
                                        ),
                                    );
                                }
                                None => {
                                    return Err(FdbError::new(
                                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                                    ))
                                }
                            }
                        } else {
                            return Err(FdbError::new(
                                PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                            ));
                        }
                    }
                } else {
                    return Err(FdbError::new(
                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                    ));
                };

                message_fdb_rl_value_tuple.insert(
                    field_descriptor.name(),
                    Value::Tuple(
                        tuple![
                            ("fdb_rl_type", format!("map_{}", fdb_rl_type)),
                            ("fdb_rl_value", map_fdb_rl_value_tuple),
                        ]
                        .into(),
                    ),
                );
            } else if field_descriptor.is_list() {
                // ```
                // repeated SomeType some_field = X;
                // ```

                // We need to get the type of `SomeType` and (if
                // applicable) message descriptor or enum descriptor.
                let (
                    fdb_rl_type,
                    maybe_repeated_field_message_descriptor,
                    maybe_repeated_field_enum_descriptor,
                ) = match field_descriptor.kind() {
                    Kind::Uint32 | Kind::Uint64 | Kind::Fixed32 | Kind::Fixed64 => {
                        return Err(FdbError::new(
                            PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                        ))
                    }
                    Kind::Double => ("double".to_string(), None, None),
                    Kind::Float => ("float".to_string(), None, None),
                    Kind::Int32 => ("int32".to_string(), None, None),
                    Kind::Int64 => ("int64".to_string(), None, None),
                    Kind::Sint32 => ("sint32".to_string(), None, None),
                    Kind::Sint64 => ("sint64".to_string(), None, None),
                    Kind::Sfixed32 => ("sfixed32".to_string(), None, None),
                    Kind::Sfixed64 => ("sfixed64".to_string(), None, None),
                    Kind::Bool => ("bool".to_string(), None, None),
                    Kind::String => ("string".to_string(), None, None),
                    Kind::Bytes => ("bytes".to_string(), None, None),
                    Kind::Message(repeated_field_message_descriptor) => {
                        // First check if message descriptor is a well
                        // known type.
                        if repeated_field_message_descriptor.full_name()
                            == FDB_RL_WKT_V1_UUID.deref().as_str()
                        {
                            (
                                format!("message_{}", FDB_RL_WKT_V1_UUID.deref().as_str()),
                                Some(repeated_field_message_descriptor),
                                None,
                            )
                        } else {
                            (
                                format!("message_{}", repeated_field_message_descriptor.name()),
                                Some(repeated_field_message_descriptor),
                                None,
                            )
                        }
                    }
                    Kind::Enum(repeated_field_enum_descriptor) => (
                        format!("enum_{}", repeated_field_enum_descriptor.name()),
                        None,
                        Some(repeated_field_enum_descriptor),
                    ),
                };

                let mut repeated_fdb_rl_value_list = Vec::<Value>::new();

                if let ProstReflectValue::List(prost_reflect_value_list) =
                    dynamic_message.get_field(&field_descriptor).into_owned()
                {
                    // `prost_reflect::Value` does not capture
                    // `prost_reflect::Kind`
                    // information. Therefore we need to use
                    // `fdb_rl_type`.
                    //
                    // You cannot define the following in protobuf.
                    //
                    // ```
                    // repeated map<string, SomeType> some_field = X;
                    // repeated repeated double some_field = X;
                    // ```
                    //
                    // These forms needs to be wrapped inside
                    // another message type.
                    for val in prost_reflect_value_list {
                        if fdb_rl_type.as_str() == "double" {
                            repeated_fdb_rl_value_list
                                .push(Self::partiql_value_double(Some(val))?.into());
                        } else if fdb_rl_type.as_str() == "float" {
                            repeated_fdb_rl_value_list
                                .push(Self::partiql_value_float(Some(val))?.into());
                        } else if fdb_rl_type.as_str() == "int32" {
                            repeated_fdb_rl_value_list
                                .push(Self::partiql_value_int32(Some(val))?.into());
                        } else if fdb_rl_type.as_str() == "int64" {
                            repeated_fdb_rl_value_list
                                .push(Self::partiql_value_int64(Some(val))?.into());
                        } else if fdb_rl_type.as_str() == "sint32" {
                            repeated_fdb_rl_value_list
                                .push(Self::partiql_value_sint32(Some(val))?.into());
                        } else if fdb_rl_type.as_str() == "sint64" {
                            repeated_fdb_rl_value_list
                                .push(Self::partiql_value_sint64(Some(val))?.into());
                        } else if fdb_rl_type.as_str() == "sfixed32" {
                            repeated_fdb_rl_value_list
                                .push(Self::partiql_value_sfixed32(Some(val))?.into());
                        } else if fdb_rl_type.as_str() == "sfixed64" {
                            repeated_fdb_rl_value_list
                                .push(Self::partiql_value_sfixed64(Some(val))?.into());
                        } else if fdb_rl_type.as_str() == "bool" {
                            repeated_fdb_rl_value_list
                                .push(Self::partiql_value_bool(Some(val))?.into());
                        } else if fdb_rl_type.as_str() == "string" {
                            repeated_fdb_rl_value_list
                                .push(Self::partiql_value_string(Some(val))?.into());
                        } else if fdb_rl_type.as_str() == "bytes" {
                            repeated_fdb_rl_value_list
                                .push(Self::partiql_value_bytes(Some(val))?.into());
                        } else if fdb_rl_type.as_str().starts_with("message_") {
                            match maybe_repeated_field_message_descriptor {
                                Some(ref repeated_field_message_descriptor) => {
                                    repeated_fdb_rl_value_list.push(
                                        Self::partiql_value_message(
                                            repeated_field_message_descriptor.clone(),
                                            Some(val),
                                        )?
                                        .into(),
                                    );
                                }
                                None => {
                                    return Err(FdbError::new(
                                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                                    ))
                                }
                            }
                        } else if fdb_rl_type.as_str().starts_with("enum_") {
                            match maybe_repeated_field_enum_descriptor {
                                Some(ref repeated_field_enum_descriptor) => {
                                    repeated_fdb_rl_value_list.push(
                                        Self::partiql_value_enum(
                                            repeated_field_enum_descriptor.clone(),
                                            Some(val),
                                        )?
                                        .into(),
                                    );
                                }
                                None => {
                                    return Err(FdbError::new(
                                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                                    ))
                                }
                            }
                        } else {
                            return Err(FdbError::new(
                                PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                            ));
                        }
                    }
                } else {
                    return Err(FdbError::new(
                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                    ));
                }

                message_fdb_rl_value_tuple.insert(
                    field_descriptor.name(),
                    Value::Tuple(
                        tuple![
                            ("fdb_rl_type", format!("repeated_{}", fdb_rl_type)),
                            (
                                "fdb_rl_value",
                                Value::List(List::from(repeated_fdb_rl_value_list).into())
                            ),
                        ]
                        .into(),
                    ),
                );
            } else {
                // ```
                // optional SomeType some_field = X;
                // ```
                //
                // or
                //
                // ```
                // oneof some_oneof {
                //   SomeType some_field = X;
                // }
                // ```
                let partiql_value_tuple = match field_descriptor.kind() {
                    Kind::Uint32 | Kind::Uint64 | Kind::Fixed32 | Kind::Fixed64 => {
                        return Err(FdbError::new(
                            PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                        ))
                    }
                    // For `Kind::Double` and `Kind::Float`, we are
                    // converting to `Value::Decimal` instead of
                    // `Value::Real`.
                    //
                    // See [1] and [2].
                    //
                    // [1]: https://github.com/partiql/partiql-lang/issues/41
                    // [2]: https://github.com/partiql/partiql-lang-rust/blob/v0.6.0/partiql-parser/src/parse/partiql.lalrpop#L1185-L1213
                    Kind::Double => {
                        if dynamic_message.has_field(&field_descriptor) {
                            Self::partiql_value_double(Some(
                                dynamic_message.get_field(&field_descriptor).into_owned(),
                            ))?
                        } else {
                            Self::partiql_value_double(None)?
                        }
                    }
                    Kind::Float => {
                        if dynamic_message.has_field(&field_descriptor) {
                            Self::partiql_value_float(Some(
                                dynamic_message.get_field(&field_descriptor).into_owned(),
                            ))?
                        } else {
                            Self::partiql_value_float(None)?
                        }
                    }
                    Kind::Int32 => {
                        if dynamic_message.has_field(&field_descriptor) {
                            Self::partiql_value_int32(Some(
                                dynamic_message.get_field(&field_descriptor).into_owned(),
                            ))?
                        } else {
                            Self::partiql_value_int32(None)?
                        }
                    }
                    Kind::Int64 => {
                        if dynamic_message.has_field(&field_descriptor) {
                            Self::partiql_value_int64(Some(
                                dynamic_message.get_field(&field_descriptor).into_owned(),
                            ))?
                        } else {
                            Self::partiql_value_int64(None)?
                        }
                    }
                    Kind::Sint32 => {
                        if dynamic_message.has_field(&field_descriptor) {
                            Self::partiql_value_sint32(Some(
                                dynamic_message.get_field(&field_descriptor).into_owned(),
                            ))?
                        } else {
                            Self::partiql_value_sint32(None)?
                        }
                    }
                    Kind::Sint64 => {
                        if dynamic_message.has_field(&field_descriptor) {
                            Self::partiql_value_sint64(Some(
                                dynamic_message.get_field(&field_descriptor).into_owned(),
                            ))?
                        } else {
                            Self::partiql_value_sint64(None)?
                        }
                    }
                    Kind::Sfixed32 => {
                        if dynamic_message.has_field(&field_descriptor) {
                            Self::partiql_value_sfixed32(Some(
                                dynamic_message.get_field(&field_descriptor).into_owned(),
                            ))?
                        } else {
                            Self::partiql_value_sfixed32(None)?
                        }
                    }
                    Kind::Sfixed64 => {
                        if dynamic_message.has_field(&field_descriptor) {
                            Self::partiql_value_sfixed64(Some(
                                dynamic_message.get_field(&field_descriptor).into_owned(),
                            ))?
                        } else {
                            Self::partiql_value_sfixed64(None)?
                        }
                    }
                    Kind::Bool => {
                        if dynamic_message.has_field(&field_descriptor) {
                            Self::partiql_value_bool(Some(
                                dynamic_message.get_field(&field_descriptor).into_owned(),
                            ))?
                        } else {
                            Self::partiql_value_bool(None)?
                        }
                    }
                    Kind::String => {
                        if dynamic_message.has_field(&field_descriptor) {
                            Self::partiql_value_string(Some(
                                dynamic_message.get_field(&field_descriptor).into_owned(),
                            ))?
                        } else {
                            Self::partiql_value_string(None)?
                        }
                    }
                    Kind::Bytes => {
                        if dynamic_message.has_field(&field_descriptor) {
                            Self::partiql_value_bytes(Some(
                                dynamic_message.get_field(&field_descriptor).into_owned(),
                            ))?
                        } else {
                            Self::partiql_value_bytes(None)?
                        }
                    }
                    Kind::Message(message_descriptor) => {
                        if dynamic_message.has_field(&field_descriptor) {
                            Self::partiql_value_message(
                                message_descriptor,
                                Some(dynamic_message.get_field(&field_descriptor).into_owned()),
                            )?
                        } else {
                            Self::partiql_value_message(message_descriptor, None)?
                        }
                    }
                    Kind::Enum(enum_descriptor) => {
                        if dynamic_message.has_field(&field_descriptor) {
                            Self::partiql_value_enum(
                                enum_descriptor,
                                Some(dynamic_message.get_field(&field_descriptor).into_owned()),
                            )?
                        } else {
                            Self::partiql_value_enum(enum_descriptor, None)?
                        }
                    }
                };

                message_fdb_rl_value_tuple.insert(
                    field_descriptor.name(),
                    Value::Tuple(partiql_value_tuple.into()),
                );
            }
        }

        message_tuple.insert(
            "fdb_rl_value",
            Value::Tuple(message_fdb_rl_value_tuple.into()),
        );

        Ok(Value::Tuple(message_tuple.into()))
    }
}

impl<T> TryFrom<(WellFormedMessageDescriptor, &T)> for WellFormedDynamicMessage
where
    T: ReflectMessage,
{
    type Error = FdbError;

    fn try_from(
        (well_formed_message_descriptor, message): (WellFormedMessageDescriptor, &T),
    ) -> FdbResult<WellFormedDynamicMessage> {
        WellFormedMessageDescriptor::try_from(message.descriptor())
            .map_err(|_| {
                // `message`'s message descriptor is not valid.
                FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE)
            })
            .and_then(|message_well_formed_message_descriptor| {
                if well_formed_message_descriptor
                    .is_evolvable_to(message_well_formed_message_descriptor)
                {
                    // `message` is compatible to provided message
                    // descriptor.
                    Ok(())
                } else {
                    Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE))
                }
            })
            .and_then(|_| {
                // Create a new dynamic message. If transcoding fails,
                // return an error.
                let mut dynamic_message =
                    DynamicMessage::new(well_formed_message_descriptor.into());

                dynamic_message
                    .transcode_from(message)
                    .map(|_| dynamic_message)
                    .map_err(|_| FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE))
            })
            .and_then(|dynamic_message| {
                // Unlike Java RecordLayer [1], we no not allow the
                // dynamic message to have unknown fields.
                //
                // In our case, when adding a new field, first the
                // clients will need to be upgraded with the new
                // field.
                //
                // Even though the field has been added to the client,
                // its value *must* be `None`. If the client were to
                // set a `Some(...)` value, that would result in
                // unknown field in dynamic message, which is not
                // allowed.
                //
                // Then using a transaction, the new field can be
                // added to the metadata. At that point, all clients
                // would need to refresh their metadata.
                //
                // We can then roll out the code that adds `Some(...)`
                // value to the field, build indexes etc.,
                //
                // We also never remove a field.
                //
                // [1] https://github.com/FoundationDB/fdb-record-layer/blob/3.4.473.0/docs/SchemaEvolution.md#add-a-field-to-an-existing-record-type
                if dynamic_message.unknown_fields().count() == 0 {
                    Ok(dynamic_message)
                } else {
                    Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE))
                }
            })
            .and_then(|dynamic_message| {
                // Check to make sure that any well known types
                // that might be inside dynamic message is well
                // formed. If any WKT is ill formed, do not return
                // a value of well formed dynamic message.
                if Self::validate_wkt(&dynamic_message) {
                    Ok(WellFormedDynamicMessage {
                        inner: dynamic_message,
                    })
                } else {
                    Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE))
                }
            })
    }
}

impl TryFrom<WellFormedDynamicMessage> for Value {
    type Error = FdbError;

    fn try_from(value: WellFormedDynamicMessage) -> FdbResult<Value> {
        let dynamic_message = value.inner;

        WellFormedDynamicMessage::try_from_inner(dynamic_message)
    }
}

#[cfg(test)]
mod tests {
    mod well_formed_dynamic_message {
        use bytes::Bytes;

        use fdb::error::FdbError;

        use partiql_value::{list, tuple, Value};

        use prost_reflect::ReflectMessage;

        use uuid::Uuid;

        use std::collections::HashMap;
        use std::convert::TryFrom;

        use super::super::super::error::PROTOBUF_ILL_FORMED_MESSAGE;
        use super::super::super::WellFormedMessageDescriptor;
        use super::super::WellFormedDynamicMessage;

        #[test]
        fn validate_wkt() {
            // fdb_rl_proto::fdb_rl::field::v1::Uuid
            {
                use fdb_rl_proto::fdb_rl::field::v1::Uuid as FdbRLWktV1UuidProto;

                // `WktV1UuidOptional`
                {
                    use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::WktV1UuidOptional;

                    assert!(WellFormedMessageDescriptor::try_from(
                        WktV1UuidOptional::default().descriptor()
                    )
                    .is_ok());

                    // optional valid wkt field
                    {
                        let dynamic_message = WktV1UuidOptional {
                            optional_field: Some(FdbRLWktV1UuidProto::from(
                                Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap(),
                            )),
                            hello: Some("hello".to_string()),
                        }
                        .transcode_to_dynamic();

                        assert!(WellFormedDynamicMessage::validate_wkt(&dynamic_message));
                    }
                    // optional invalid wkt field
                    {
                        let dynamic_message = WktV1UuidOptional {
                            optional_field: Some(FdbRLWktV1UuidProto {
                                value: Bytes::from([4, 54, 67, 12, 43, 2, 98, 76].as_ref()),
                            }),
                            hello: Some("hello".to_string()),
                        }
                        .transcode_to_dynamic();

                        assert!(!WellFormedDynamicMessage::validate_wkt(&dynamic_message));
                    }
                    // optional null wkt field
                    {
                        let dynamic_message = WktV1UuidOptional {
                            optional_field: None,
                            hello: Some("hello".to_string()),
                        }
                        .transcode_to_dynamic();

                        assert!(WellFormedDynamicMessage::validate_wkt(&dynamic_message));
                    }
                }
                // `WktV1UuidRepeated`
                {
                    use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::WktV1UuidRepeated;

                    assert!(WellFormedMessageDescriptor::try_from(
                        WktV1UuidRepeated::default().descriptor()
                    )
                    .is_ok());

                    // repeated with valid wkt entries
                    {
                        let dynamic_message = WktV1UuidRepeated {
                            repeated_field: vec![
                                FdbRLWktV1UuidProto::from(
                                    Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e")
                                        .unwrap(),
                                ),
                                FdbRLWktV1UuidProto::from(
                                    Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e")
                                        .unwrap(),
                                ),
                                FdbRLWktV1UuidProto::from(
                                    Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e")
                                        .unwrap(),
                                ),
                            ],
                            hello: None,
                        }
                        .transcode_to_dynamic();

                        assert!(WellFormedDynamicMessage::validate_wkt(&dynamic_message));
                    }
                    // repeated with some invalid wkt entries
                    {
                        let dynamic_message = WktV1UuidRepeated {
                            repeated_field: vec![
                                FdbRLWktV1UuidProto::from(
                                    Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e")
                                        .unwrap(),
                                ),
                                // invalid
                                FdbRLWktV1UuidProto {
                                    value: Bytes::from([4, 54, 67, 12, 43, 2, 98, 76].as_ref()),
                                },
                                FdbRLWktV1UuidProto::from(
                                    Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e")
                                        .unwrap(),
                                ),
                            ],
                            hello: None,
                        }
                        .transcode_to_dynamic();

                        assert!(!WellFormedDynamicMessage::validate_wkt(&dynamic_message));
                    }
                    // repeated with no entry
                    {
                        let dynamic_message = WktV1UuidRepeated {
                            repeated_field: vec![],
                            hello: None,
                        }
                        .transcode_to_dynamic();

                        assert!(WellFormedDynamicMessage::validate_wkt(&dynamic_message));
                    }
                }
                // `WktV1UuidMap`
                {
                    use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::WktV1UuidMap;

                    assert!(WellFormedMessageDescriptor::try_from(
                        WktV1UuidMap::default().descriptor()
                    )
                    .is_ok());

                    // map with valid wkt entries
                    {
                        let dynamic_message = WktV1UuidMap {
                            map_field: HashMap::from([
                                (
                                    "key1".to_string(),
                                    FdbRLWktV1UuidProto::from(
                                        Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e")
                                            .unwrap(),
                                    ),
                                ),
                                (
                                    "key2".to_string(),
                                    FdbRLWktV1UuidProto::from(
                                        Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e")
                                            .unwrap(),
                                    ),
                                ),
                                (
                                    "key3".to_string(),
                                    FdbRLWktV1UuidProto::from(
                                        Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e")
                                            .unwrap(),
                                    ),
                                ),
                            ]),
                            hello: Some("hello".to_string()),
                        }
                        .transcode_to_dynamic();

                        assert!(WellFormedDynamicMessage::validate_wkt(&dynamic_message));
                    }
                    // map with some invalid wkt entries
                    {
                        let dynamic_message = WktV1UuidMap {
                            map_field: HashMap::from([
                                (
                                    "key1".to_string(),
                                    FdbRLWktV1UuidProto::from(
                                        Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e")
                                            .unwrap(),
                                    ),
                                ),
                                (
                                    "key2".to_string(),
                                    // invalid
                                    FdbRLWktV1UuidProto {
                                        value: Bytes::from([4, 54, 67, 12, 43, 2, 98, 76].as_ref()),
                                    },
                                ),
                                (
                                    "key3".to_string(),
                                    FdbRLWktV1UuidProto::from(
                                        Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e")
                                            .unwrap(),
                                    ),
                                ),
                            ]),
                            hello: Some("hello".to_string()),
                        }
                        .transcode_to_dynamic();

                        assert!(!WellFormedDynamicMessage::validate_wkt(&dynamic_message));
                    }
                    // map with no entry
                    {
                        let dynamic_message = WktV1UuidMap {
                            map_field: HashMap::from([]),
                            hello: Some("hello".to_string()),
                        }
                        .transcode_to_dynamic();

                        assert!(WellFormedDynamicMessage::validate_wkt(&dynamic_message));
                    }
                }
                // `WktV1UuidNestedOuter`
                {
                    use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::{
                        WktV1UuidNestedInner, WktV1UuidNestedOuter,
                    };

                    assert!(WellFormedMessageDescriptor::try_from(
                        WktV1UuidNestedOuter::default().descriptor()
                    )
                    .is_ok());

                    // inner valid wkt field
                    {
                        let dynamic_message = WktV1UuidNestedOuter {
                            nested_inner: Some(WktV1UuidNestedInner {
                                optional_field: Some(FdbRLWktV1UuidProto::from(
                                    Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e")
                                        .unwrap(),
                                )),
                                world: Some("world".to_string()),
                            }),
                            hello: None,
                        }
                        .transcode_to_dynamic();

                        assert!(WellFormedDynamicMessage::validate_wkt(&dynamic_message));
                    }
                    // inner invalid wkt field
                    {
                        let dynamic_message = WktV1UuidNestedOuter {
                            nested_inner: Some(WktV1UuidNestedInner {
                                optional_field: Some(FdbRLWktV1UuidProto {
                                    value: Bytes::from([4, 54, 67, 12, 43, 2, 98, 76].as_ref()),
                                }),
                                world: Some("world".to_string()),
                            }),
                            hello: None,
                        }
                        .transcode_to_dynamic();

                        assert!(!WellFormedDynamicMessage::validate_wkt(&dynamic_message));
                    }
                    // inner null wkt field
                    {
                        let dynamic_message = WktV1UuidNestedOuter {
                            nested_inner: Some(WktV1UuidNestedInner {
                                optional_field: None,
                                world: Some("world".to_string()),
                            }),
                            hello: None,
                        }
                        .transcode_to_dynamic();

                        assert!(WellFormedDynamicMessage::validate_wkt(&dynamic_message));
                    }
                }
                // `WktV1UuidRecursiveOuter`
                {
                    use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::{
                        WktV1UuidRecursiveInner, WktV1UuidRecursiveOuter,
                    };

                    assert!(WellFormedMessageDescriptor::try_from(
                        WktV1UuidRecursiveOuter::default().descriptor()
                    )
                    .is_ok());

                    // outer valid, inner valid, outer valid
                    {
                        let dynamic_message = WktV1UuidRecursiveOuter {
                            recursive_inner: Some(
                                WktV1UuidRecursiveInner {
                                    recursive_outer: Some(
                                        WktV1UuidRecursiveOuter {
                                            recursive_inner: None,
                                            optional_field: Some(FdbRLWktV1UuidProto::from(
                                                Uuid::parse_str(
                                                    "ffffffff-ba5e-ba11-0000-00005ca1ab1e",
                                                )
                                                .unwrap(),
                                            )),
                                        }
                                        .into(),
                                    ),
                                    optional_field: Some(FdbRLWktV1UuidProto::from(
                                        Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e")
                                            .unwrap(),
                                    )),
                                }
                                .into(),
                            ),
                            optional_field: Some(FdbRLWktV1UuidProto::from(
                                Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap(),
                            )),
                        }
                        .transcode_to_dynamic();

                        assert!(WellFormedDynamicMessage::validate_wkt(&dynamic_message));
                    }
                    // outer valid, inner valid, outer invalid
                    {
                        let dynamic_message = WktV1UuidRecursiveOuter {
                            recursive_inner: Some(
                                WktV1UuidRecursiveInner {
                                    recursive_outer: Some(
                                        WktV1UuidRecursiveOuter {
                                            recursive_inner: None,
                                            optional_field: Some(FdbRLWktV1UuidProto {
                                                value: Bytes::from(
                                                    [4, 54, 67, 12, 43, 2, 98, 76].as_ref(),
                                                ),
                                            }),
                                        }
                                        .into(),
                                    ),
                                    optional_field: Some(FdbRLWktV1UuidProto::from(
                                        Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e")
                                            .unwrap(),
                                    )),
                                }
                                .into(),
                            ),
                            optional_field: Some(FdbRLWktV1UuidProto::from(
                                Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap(),
                            )),
                        }
                        .transcode_to_dynamic();

                        assert!(!WellFormedDynamicMessage::validate_wkt(&dynamic_message));
                    }
                    // outer valid, inner invalid, outer valid
                    {
                        let dynamic_message = WktV1UuidRecursiveOuter {
                            recursive_inner: Some(
                                WktV1UuidRecursiveInner {
                                    recursive_outer: Some(
                                        WktV1UuidRecursiveOuter {
                                            recursive_inner: None,
                                            optional_field: Some(FdbRLWktV1UuidProto::from(
                                                Uuid::parse_str(
                                                    "ffffffff-ba5e-ba11-0000-00005ca1ab1e",
                                                )
                                                .unwrap(),
                                            )),
                                        }
                                        .into(),
                                    ),
                                    optional_field: Some(FdbRLWktV1UuidProto {
                                        value: Bytes::from([4, 54, 67, 12, 43, 2, 98, 76].as_ref()),
                                    }),
                                }
                                .into(),
                            ),
                            optional_field: Some(FdbRLWktV1UuidProto::from(
                                Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap(),
                            )),
                        }
                        .transcode_to_dynamic();

                        assert!(!WellFormedDynamicMessage::validate_wkt(&dynamic_message));
                    }
                    // outer invalid, inner valid, outer valid
                    {
                        let dynamic_message = WktV1UuidRecursiveOuter {
                            recursive_inner: Some(
                                WktV1UuidRecursiveInner {
                                    recursive_outer: Some(
                                        WktV1UuidRecursiveOuter {
                                            recursive_inner: None,
                                            optional_field: Some(FdbRLWktV1UuidProto::from(
                                                Uuid::parse_str(
                                                    "ffffffff-ba5e-ba11-0000-00005ca1ab1e",
                                                )
                                                .unwrap(),
                                            )),
                                        }
                                        .into(),
                                    ),
                                    optional_field: Some(FdbRLWktV1UuidProto::from(
                                        Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e")
                                            .unwrap(),
                                    )),
                                }
                                .into(),
                            ),
                            optional_field: Some(FdbRLWktV1UuidProto {
                                value: Bytes::from([4, 54, 67, 12, 43, 2, 98, 76].as_ref()),
                            }),
                        }
                        .transcode_to_dynamic();

                        assert!(!WellFormedDynamicMessage::validate_wkt(&dynamic_message));
                    }
                    // outer null, inner null, outer null
                    {
                        let dynamic_message = WktV1UuidRecursiveOuter {
                            recursive_inner: Some(
                                WktV1UuidRecursiveInner {
                                    recursive_outer: Some(
                                        WktV1UuidRecursiveOuter {
                                            recursive_inner: None,
                                            optional_field: None,
                                        }
                                        .into(),
                                    ),
                                    optional_field: None,
                                }
                                .into(),
                            ),
                            optional_field: None,
                        }
                        .transcode_to_dynamic();

                        assert!(WellFormedDynamicMessage::validate_wkt(&dynamic_message));
                    }
                }
                // `WktV1UuidOneof`
                {
                    use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::WktV1UuidOneof;
		    use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::wkt_v1_uuid_oneof::WktV1UuidOneof as WktV1UuidOneofEnum;

                    assert!(WellFormedMessageDescriptor::try_from(
                        WktV1UuidOneof::default().descriptor()
                    )
                    .is_ok());

                    // `wkt_v1_uuid_oneof` uuid valid
                    {
                        let dynamic_message = WktV1UuidOneof {
                            hello: Some("hello".to_string()),
                            wkt_v1_uuid_oneof: Some(WktV1UuidOneofEnum::UuidField(
                                FdbRLWktV1UuidProto::from(
                                    Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e")
                                        .unwrap(),
                                ),
                            )),
                        }
                        .transcode_to_dynamic();

                        assert!(WellFormedDynamicMessage::validate_wkt(&dynamic_message));
                    }
                    // `wkt_v1_uuid_oneof` uuid invalid
                    {
                        let dynamic_message = WktV1UuidOneof {
                            hello: Some("hello".to_string()),
                            wkt_v1_uuid_oneof: Some(WktV1UuidOneofEnum::UuidField(
                                FdbRLWktV1UuidProto {
                                    value: Bytes::from([4, 54, 67, 12, 43, 2, 98, 76].as_ref()),
                                },
                            )),
                        }
                        .transcode_to_dynamic();

                        assert!(!WellFormedDynamicMessage::validate_wkt(&dynamic_message));
                    }
                    // `wkt_v1_uuid_oneof` string
                    {
                        let dynamic_message = WktV1UuidOneof {
                            hello: Some("hello".to_string()),
                            wkt_v1_uuid_oneof: Some(WktV1UuidOneofEnum::World("world".to_string())),
                        }
                        .transcode_to_dynamic();

                        assert!(WellFormedDynamicMessage::validate_wkt(&dynamic_message));
                    }
                    // `wkt_v1_uuid_oneof` null
                    {
                        let dynamic_message = WktV1UuidOneof {
                            hello: Some("hello".to_string()),
                            wkt_v1_uuid_oneof: None,
                        }
                        .transcode_to_dynamic();

                        assert!(WellFormedDynamicMessage::validate_wkt(&dynamic_message));
                    }
                }
            }
        }

        #[test]
        fn try_from_well_formed_message_descriptor_ref_t_try_from() {
            // Valid message
            {
                // Same message descriptor
                {
                    use fdb_rl_proto::fdb_rl::field::v1::Uuid as FdbRLWktV1UuidProto;
                    use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_message_descriptor::good::v1::HelloWorld;

                    let hello_world = HelloWorld {
                        primary_key: Some(FdbRLWktV1UuidProto::from(
                            Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap(),
                        )),
                        hello: Some("hello".to_string()),
                        world: Some("world".to_string()),
                    };

                    let well_formed_message_descriptor =
                        WellFormedMessageDescriptor::try_from(hello_world.descriptor()).unwrap();

                    let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                        well_formed_message_descriptor,
                        &hello_world,
                    ));

                    assert_eq!(
                        Ok(WellFormedDynamicMessage {
                            inner: hello_world.transcode_to_dynamic()
                        }),
                        well_formed_dynamic_message
                    );
                }
                // Evolved message descriptor (optional)
                {
                    use fdb_rl_proto::fdb_rl::field::v1::Uuid as FdbRLWktV1UuidProto;
                    use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_message_descriptor::good::v1::HelloWorld as MetadataHelloWorld;
		    use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_message_descriptor::good::v2::HelloWorld as CodeHelloWorld;

                    let well_formed_message_descriptor = WellFormedMessageDescriptor::try_from(
                        MetadataHelloWorld::default().descriptor(),
                    )
                    .unwrap();

                    // Does not have the field `hello_world`.
                    let metadata_hello_world = MetadataHelloWorld {
                        primary_key: Some(FdbRLWktV1UuidProto::from(
                            Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap(),
                        )),
                        hello: Some("hello".to_string()),
                        world: Some("world".to_string()),
                    };

                    let code_hello_world = CodeHelloWorld {
                        primary_key: Some(FdbRLWktV1UuidProto::from(
                            Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap(),
                        )),
                        hello: Some("hello".to_string()),
                        world: Some("world".to_string()),
                        hello_world: Some("hello_world".to_string()),
                    };

                    // The `DynamicMessageFieldSet` would have unknown
                    // fields [1] [2] for `hello_world`, as it is not
                    // present in `MetadataHelloWorld` message
                    // descriptor.
                    //
                    // ```
                    // 4: Unknown(UnknownFieldSet {
                    //    fields: [UnknownField { number: 4, value: LengthDelimited(b"hello_world") }]
                    // })
                    // ```
                    //
                    // We do not allow unknown fields in dynamic
                    // messages created using message descriptors
                    // stored in metadata. Therefore it results in an
                    // error.
                    //
                    // [1] https://protobuf.dev/programming-guides/proto3/#unknowns
                    // [2] https://protobuf.dev/programming-guides/proto2/#unknowns

                    let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                        well_formed_message_descriptor.clone(),
                        &code_hello_world,
                    ));

                    assert_eq!(
                        Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE)),
                        well_formed_dynamic_message
                    );

                    // The correct way to this is to first set
                    // `hello_world` to `None` and then update the
                    // metadata.

                    let code_hello_world = CodeHelloWorld {
                        primary_key: Some(FdbRLWktV1UuidProto::from(
                            Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap(),
                        )),
                        hello: Some("hello".to_string()),
                        world: Some("world".to_string()),
                        hello_world: None,
                    };

                    let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                        well_formed_message_descriptor,
                        &code_hello_world,
                    ))
                    .unwrap();

                    let dynamic_message = well_formed_dynamic_message.inner;

                    assert_eq!(
                        dynamic_message.transcode_to::<MetadataHelloWorld>(),
                        Ok(metadata_hello_world)
                    );

                    assert_eq!(
                        dynamic_message.transcode_to::<CodeHelloWorld>(),
                        Ok(code_hello_world)
                    );
                }
                // Evolved message descriptor (repeated)
                {
                    use fdb_rl_proto::fdb_rl::field::v1::Uuid as FdbRLWktV1UuidProto;
                    use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_message_descriptor::good::v1::HelloWorld as MetadataHelloWorld;
		    use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_message_descriptor::good::v3::HelloWorld as CodeHelloWorld;

                    let well_formed_message_descriptor = WellFormedMessageDescriptor::try_from(
                        MetadataHelloWorld::default().descriptor(),
                    )
                    .unwrap();

                    // Does not have the repeated field `hello_world`.
                    let metadata_hello_world = MetadataHelloWorld {
                        primary_key: Some(FdbRLWktV1UuidProto::from(
                            Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap(),
                        )),
                        hello: Some("hello".to_string()),
                        world: Some("world".to_string()),
                    };

                    let code_hello_world = CodeHelloWorld {
                        primary_key: Some(FdbRLWktV1UuidProto::from(
                            Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap(),
                        )),
                        hello: Some("hello".to_string()),
                        world: Some("world".to_string()),
                        hello_world: vec!["hello".to_string(), "world".to_string()],
                    };

                    // The `DynamicMessageFieldSet` would have unknown
                    // fields [1] [2] for `hello_world`, as it is not
                    // present in `MetadataHelloWorld` message
                    // descriptor.
                    //
                    // ```
                    // 4: Unknown(UnknownFieldSet {
                    //    fields: [
                    //      UnknownField { number: 4, value: LengthDelimited(b"hello") },
                    //      UnknownField { number: 4, value: LengthDelimited(b"world") }
                    //    ]
                    // })
                    // ```
                    //
                    // We do not allow unknown fields in dynamic
                    // messages created using message descriptors
                    // stored in metadata. Therefore it results in an
                    // error.
                    //
                    // [1] https://protobuf.dev/programming-guides/proto3/#unknowns
                    // [2] https://protobuf.dev/programming-guides/proto2/#unknowns

                    let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                        well_formed_message_descriptor.clone(),
                        &code_hello_world,
                    ));

                    assert_eq!(
                        Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE)),
                        well_formed_dynamic_message
                    );

                    // The correct way to this is to first set
                    // `hello_world` to `vec![]` (empty vector) and
                    // then update the metadata.

                    let code_hello_world = CodeHelloWorld {
                        primary_key: Some(FdbRLWktV1UuidProto::from(
                            Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap(),
                        )),
                        hello: Some("hello".to_string()),
                        world: Some("world".to_string()),
                        hello_world: vec![],
                    };

                    let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                        well_formed_message_descriptor,
                        &code_hello_world,
                    ))
                    .unwrap();

                    let dynamic_message = well_formed_dynamic_message.inner;

                    assert_eq!(
                        dynamic_message.transcode_to::<MetadataHelloWorld>(),
                        Ok(metadata_hello_world)
                    );

                    assert_eq!(
                        dynamic_message.transcode_to::<CodeHelloWorld>(),
                        Ok(code_hello_world)
                    );
                }
            }
            // Invalid message
            {
                {
                    use fdb_rl_proto::fdb_rl::field::v1::Uuid as FdbRLWktV1UuidProto;
                    use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_message_descriptor::good::v1::{HelloWorld, HelloWorldMap};

                    use std::collections::HashMap;

                    let well_formed_message_descriptor =
                        WellFormedMessageDescriptor::try_from(HelloWorld::default().descriptor())
                            .unwrap();

                    let hello_world_map = HelloWorldMap {
                        hello_world_map: HashMap::from([(
                            "hello_world".to_string(),
                            HelloWorld {
                                primary_key: Some(FdbRLWktV1UuidProto::from(
                                    Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e")
                                        .unwrap(),
                                )),
                                hello: Some("hello".to_string()),
                                world: Some("world".to_string()),
                            },
                        )]),
                    };

                    let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                        well_formed_message_descriptor,
                        &hello_world_map,
                    ));

                    // Even though `hello_world_map` has a message
                    // descriptor that is well formed, it is not
                    // compatible with `HelloWorld`'s message descriptor.
                    assert_eq!(
                        Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE)),
                        well_formed_dynamic_message
                    );
                }
                {
                    use fdb_rl_proto::fdb_rl::field::v1::Uuid as FdbRLWktV1UuidProto;
                    use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_message_descriptor::good::v1::HelloWorld;

                    let well_formed_message_descriptor =
                        WellFormedMessageDescriptor::try_from(HelloWorld::default().descriptor())
                            .unwrap();

                    let hello_world = HelloWorld {
                        primary_key: Some(FdbRLWktV1UuidProto {
                            value: Bytes::from([4, 54, 67, 12, 43, 2, 98, 76].as_ref()),
                        }),
                        hello: Some("hello".to_string()),
                        world: None,
                    };

                    let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                        well_formed_message_descriptor,
                        &hello_world,
                    ));

                    // We have an invalid `FdbRLWktV1UuidProto` value.
                    assert_eq!(
                        Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE)),
                        well_formed_dynamic_message
                    );
                }
            }
        }

        #[test]
        fn try_from_well_formed_message_descriptor_try_from() {
            // `Empty`
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::Empty;
                let empty = Empty {};

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(empty.descriptor()).unwrap();

                let well_formed_dynamic_message =
                    WellFormedDynamicMessage::try_from((well_formed_message_descriptor, &empty))
                        .unwrap();

                let result = Value::try_from(well_formed_dynamic_message).unwrap();
                let expected =
                    tuple![("fdb_rl_type", "message_Empty"), ("fdb_rl_value", tuple![]),];

                assert_eq!(result, expected.into(),);
            }
            // `HelloWorldDouble`
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::HelloWorldDouble;

                let hello_world_double = HelloWorldDouble {
                    hello: Some(3.14),
                    world: None,
                };

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(hello_world_double.descriptor()).unwrap();

                let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                    well_formed_message_descriptor,
                    &hello_world_double,
                ))
                .unwrap();

                let result = Value::try_from(well_formed_dynamic_message).unwrap();
                let expected = tuple![
                    ("fdb_rl_type", "message_HelloWorldDouble"),
                    (
                        "fdb_rl_value",
                        tuple![
                            (
                                "hello",
                                tuple![("fdb_rl_type", "double"), ("fdb_rl_value", 3.14),]
                            ),
                            (
                                "world",
                                tuple![("fdb_rl_type", "double"), ("fdb_rl_value", Value::Null),]
                            ),
                        ]
                    ),
                ];

                assert_eq!(result, expected.into(),);
            }
            // `HelloWorldFloat`
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::HelloWorldFloat;

                let hello_world_float = HelloWorldFloat {
                    hello: Some(3.14),
                    world: None,
                };

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(hello_world_float.descriptor()).unwrap();

                let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                    well_formed_message_descriptor,
                    &hello_world_float,
                ))
                .unwrap();

                let result = Value::try_from(well_formed_dynamic_message).unwrap();
                let expected = tuple![
                    ("fdb_rl_type", "message_HelloWorldFloat"),
                    (
                        "fdb_rl_value",
                        tuple![
                            (
                                "hello",
                                tuple![("fdb_rl_type", "float"), ("fdb_rl_value", 3.14),]
                            ),
                            (
                                "world",
                                tuple![("fdb_rl_type", "float"), ("fdb_rl_value", Value::Null),]
                            ),
                        ]
                    ),
                ];

                assert_eq!(result, expected.into(),);
            }
            // `HelloWorldInt32`
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::HelloWorldInt32;

                let hello_world_int32 = HelloWorldInt32 {
                    hello: Some(108),
                    world: None,
                };

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(hello_world_int32.descriptor()).unwrap();

                let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                    well_formed_message_descriptor,
                    &hello_world_int32,
                ))
                .unwrap();

                let result = Value::try_from(well_formed_dynamic_message).unwrap();
                let expected = tuple![
                    ("fdb_rl_type", "message_HelloWorldInt32"),
                    (
                        "fdb_rl_value",
                        tuple![
                            (
                                "hello",
                                tuple![("fdb_rl_type", "int32"), ("fdb_rl_value", 108),]
                            ),
                            (
                                "world",
                                tuple![("fdb_rl_type", "int32"), ("fdb_rl_value", Value::Null),]
                            ),
                        ]
                    ),
                ];

                assert_eq!(result, expected.into(),);
            }
            // `HelloWorldInt64`
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::HelloWorldInt64;

                let hello_world_int64 = HelloWorldInt64 {
                    hello: Some(108),
                    world: None,
                };

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(hello_world_int64.descriptor()).unwrap();

                let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                    well_formed_message_descriptor,
                    &hello_world_int64,
                ))
                .unwrap();

                let result = Value::try_from(well_formed_dynamic_message).unwrap();
                let expected = tuple![
                    ("fdb_rl_type", "message_HelloWorldInt64"),
                    (
                        "fdb_rl_value",
                        tuple![
                            (
                                "hello",
                                tuple![("fdb_rl_type", "int64"), ("fdb_rl_value", 108),]
                            ),
                            (
                                "world",
                                tuple![("fdb_rl_type", "int64"), ("fdb_rl_value", Value::Null),]
                            ),
                        ]
                    ),
                ];

                assert_eq!(result, expected.into(),);
            }
            // `HelloWorldSint32`
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::HelloWorldSint32;

                let hello_world_sint32 = HelloWorldSint32 {
                    hello: Some(108),
                    world: None,
                };

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(hello_world_sint32.descriptor()).unwrap();

                let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                    well_formed_message_descriptor,
                    &hello_world_sint32,
                ))
                .unwrap();

                let result = Value::try_from(well_formed_dynamic_message).unwrap();
                let expected = tuple![
                    ("fdb_rl_type", "message_HelloWorldSint32"),
                    (
                        "fdb_rl_value",
                        tuple![
                            (
                                "hello",
                                tuple![("fdb_rl_type", "sint32"), ("fdb_rl_value", 108),]
                            ),
                            (
                                "world",
                                tuple![("fdb_rl_type", "sint32"), ("fdb_rl_value", Value::Null),]
                            ),
                        ]
                    ),
                ];

                assert_eq!(result, expected.into(),);
            }
            // `HelloWorldSint64`
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::HelloWorldSint64;

                let hello_world_sint64 = HelloWorldSint64 {
                    hello: Some(108),
                    world: None,
                };

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(hello_world_sint64.descriptor()).unwrap();

                let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                    well_formed_message_descriptor,
                    &hello_world_sint64,
                ))
                .unwrap();

                let result = Value::try_from(well_formed_dynamic_message).unwrap();
                let expected = tuple![
                    ("fdb_rl_type", "message_HelloWorldSint64"),
                    (
                        "fdb_rl_value",
                        tuple![
                            (
                                "hello",
                                tuple![("fdb_rl_type", "sint64"), ("fdb_rl_value", 108),]
                            ),
                            (
                                "world",
                                tuple![("fdb_rl_type", "sint64"), ("fdb_rl_value", Value::Null),]
                            ),
                        ]
                    ),
                ];

                assert_eq!(result, expected.into(),);
            }
            // `HelloWorldSfixed32`
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::HelloWorldSfixed32;

                let hello_world_sfixed32 = HelloWorldSfixed32 {
                    hello: Some(108),
                    world: None,
                };

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(hello_world_sfixed32.descriptor())
                        .unwrap();

                let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                    well_formed_message_descriptor,
                    &hello_world_sfixed32,
                ))
                .unwrap();

                let result = Value::try_from(well_formed_dynamic_message).unwrap();
                let expected = tuple![
                    ("fdb_rl_type", "message_HelloWorldSfixed32"),
                    (
                        "fdb_rl_value",
                        tuple![
                            (
                                "hello",
                                tuple![("fdb_rl_type", "sfixed32"), ("fdb_rl_value", 108),]
                            ),
                            (
                                "world",
                                tuple![("fdb_rl_type", "sfixed32"), ("fdb_rl_value", Value::Null),]
                            ),
                        ]
                    ),
                ];

                assert_eq!(result, expected.into(),);
            }
            // `HelloWorldSfixed64`
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::HelloWorldSfixed64;

                let hello_world_sfixed64 = HelloWorldSfixed64 {
                    hello: Some(108),
                    world: None,
                };

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(hello_world_sfixed64.descriptor())
                        .unwrap();

                let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                    well_formed_message_descriptor,
                    &hello_world_sfixed64,
                ))
                .unwrap();

                let result = Value::try_from(well_formed_dynamic_message).unwrap();
                let expected = tuple![
                    ("fdb_rl_type", "message_HelloWorldSfixed64"),
                    (
                        "fdb_rl_value",
                        tuple![
                            (
                                "hello",
                                tuple![("fdb_rl_type", "sfixed64"), ("fdb_rl_value", 108),]
                            ),
                            (
                                "world",
                                tuple![("fdb_rl_type", "sfixed64"), ("fdb_rl_value", Value::Null),]
                            ),
                        ]
                    ),
                ];

                assert_eq!(result, expected.into(),);
            }
            // `HelloWorldBool`
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::HelloWorldBool;

                let hello_world_bool = HelloWorldBool {
                    hello: Some(true),
                    world: None,
                };

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(hello_world_bool.descriptor()).unwrap();

                let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                    well_formed_message_descriptor,
                    &hello_world_bool,
                ))
                .unwrap();

                let result = Value::try_from(well_formed_dynamic_message).unwrap();
                let expected = tuple![
                    ("fdb_rl_type", "message_HelloWorldBool"),
                    (
                        "fdb_rl_value",
                        tuple![
                            (
                                "hello",
                                tuple![
                                    ("fdb_rl_type", "bool"),
                                    ("fdb_rl_value", Value::Boolean(true)),
                                ]
                            ),
                            (
                                "world",
                                tuple![("fdb_rl_type", "bool"), ("fdb_rl_value", Value::Null),]
                            ),
                        ]
                    ),
                ];

                assert_eq!(result, expected.into(),);
            }
            // `HelloWorldString`
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::HelloWorldString;

                let hello_world_string = HelloWorldString {
                    hello: Some("hello".to_string()),
                    world: None,
                };

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(hello_world_string.descriptor()).unwrap();

                let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                    well_formed_message_descriptor,
                    &hello_world_string,
                ))
                .unwrap();

                let result = Value::try_from(well_formed_dynamic_message).unwrap();
                let expected = tuple![
                    ("fdb_rl_type", "message_HelloWorldString"),
                    (
                        "fdb_rl_value",
                        tuple![
                            (
                                "hello",
                                tuple![("fdb_rl_type", "string"), ("fdb_rl_value", "hello"),]
                            ),
                            (
                                "world",
                                tuple![("fdb_rl_type", "string"), ("fdb_rl_value", Value::Null),]
                            ),
                        ]
                    ),
                ];

                assert_eq!(result, expected.into(),);
            }
            // `HelloWorldBytes`
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::HelloWorldBytes;

                let hello_world_bytes = HelloWorldBytes {
                    hello: Some(Bytes::from_static(b"hello")),
                    world: None,
                };

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(hello_world_bytes.descriptor()).unwrap();

                let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                    well_formed_message_descriptor,
                    &hello_world_bytes,
                ))
                .unwrap();

                let result = Value::try_from(well_formed_dynamic_message).unwrap();
                let expected = tuple![
                    ("fdb_rl_type", "message_HelloWorldBytes"),
                    (
                        "fdb_rl_value",
                        tuple![
                            (
                                "hello",
                                tuple![
                                    ("fdb_rl_type", "bytes"),
                                    (
                                        "fdb_rl_value",
                                        Value::Blob(
                                            Vec::<u8>::from(Bytes::from_static(b"hello")).into()
                                        )
                                    ),
                                ]
                            ),
                            (
                                "world",
                                tuple![("fdb_rl_type", "bytes"), ("fdb_rl_value", Value::Null),]
                            ),
                        ]
                    ),
                ];

                assert_eq!(result, expected.into(),);
            }
            // `HelloWorldEnum`
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::HelloWorldEnum;
		use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::hello_world_enum::Size;

                let hello_world_enum = HelloWorldEnum {
                    hello: Some(Size::Large.into()),
                    world: None,
                };

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(hello_world_enum.descriptor()).unwrap();

                let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                    well_formed_message_descriptor,
                    &hello_world_enum,
                ))
                .unwrap();

                let result = Value::try_from(well_formed_dynamic_message).unwrap();
                let expected = tuple![
                    ("fdb_rl_type", "message_HelloWorldEnum"),
                    (
                        "fdb_rl_value",
                        tuple![
                            (
                                "hello",
                                tuple![
                                    ("fdb_rl_type", "enum_Size"),
                                    (
                                        "fdb_rl_value",
                                        tuple![("name", "SIZE_LARGE"), ("number", 3),]
                                    )
                                ]
                            ),
                            (
                                "world",
                                tuple![("fdb_rl_type", "enum_Size"), ("fdb_rl_value", Value::Null),]
                            )
                        ]
                    )
                ];

                assert_eq!(result, expected.into(),);
            }
            // `HelloWorldOneof, hello_world: Some(...)`
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::HelloWorldOneof;
		use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::hello_world_oneof::HelloWorld;

                let hello_world_oneof = HelloWorldOneof {
                    some_field: Some("some_field".to_string()),
                    hello_world: Some(HelloWorld::World(108)),
                };

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(hello_world_oneof.descriptor()).unwrap();

                let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                    well_formed_message_descriptor,
                    &hello_world_oneof,
                ))
                .unwrap();

                let result = Value::try_from(well_formed_dynamic_message).unwrap();
                let expected = tuple![
                    ("fdb_rl_type", "message_HelloWorldOneof"),
                    (
                        "fdb_rl_value",
                        tuple![
                            (
                                "some_field",
                                tuple![("fdb_rl_type", "string"), ("fdb_rl_value", "some_field"),]
                            ),
                            (
                                "hello",
                                tuple![("fdb_rl_type", "string"), ("fdb_rl_value", Value::Null)]
                            ),
                            (
                                "world",
                                tuple![("fdb_rl_type", "int64"), ("fdb_rl_value", 108)]
                            )
                        ]
                    )
                ];

                assert_eq!(result, expected.into(),);
            }
            // `HelloWorldOneof, hello_world: None`
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::HelloWorldOneof;

                let hello_world_oneof = HelloWorldOneof {
                    some_field: Some("some_field".to_string()),
                    hello_world: None,
                };

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(hello_world_oneof.descriptor()).unwrap();

                let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                    well_formed_message_descriptor,
                    &hello_world_oneof,
                ))
                .unwrap();

                let result = Value::try_from(well_formed_dynamic_message).unwrap();
                let expected = tuple![
                    ("fdb_rl_type", "message_HelloWorldOneof"),
                    (
                        "fdb_rl_value",
                        tuple![
                            (
                                "some_field",
                                tuple![("fdb_rl_type", "string"), ("fdb_rl_value", "some_field"),]
                            ),
                            (
                                "hello",
                                tuple![("fdb_rl_type", "string"), ("fdb_rl_value", Value::Null)]
                            ),
                            (
                                "world",
                                tuple![("fdb_rl_type", "int64"), ("fdb_rl_value", Value::Null)]
                            )
                        ]
                    )
                ];

                assert_eq!(result, expected.into(),);
            }
            // `HelloWorldWktV1Uuid, primary_key: None`
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::HelloWorldWktV1Uuid;

                let hello_world_wkt_v1_uuid = HelloWorldWktV1Uuid {
                    primary_key: None,
                    hello: Some("hello".to_string()),
                    world: None,
                };

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(hello_world_wkt_v1_uuid.descriptor())
                        .unwrap();

                let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                    well_formed_message_descriptor,
                    &hello_world_wkt_v1_uuid,
                ))
                .unwrap();

                let result = Value::try_from(well_formed_dynamic_message).unwrap();
                let expected = tuple![
                    ("fdb_rl_type", "message_HelloWorldWktV1Uuid"),
                    (
                        "fdb_rl_value",
                        tuple![
                            (
                                "primary_key",
                                tuple![
                                    ("fdb_rl_type", "message_fdb_rl.field.v1.UUID"),
                                    ("fdb_rl_value", Value::Null)
                                ]
                            ),
                            (
                                "hello",
                                tuple![("fdb_rl_type", "string"), ("fdb_rl_value", "hello")]
                            ),
                            (
                                "world",
                                tuple![("fdb_rl_type", "string"), ("fdb_rl_value", Value::Null)]
                            )
                        ]
                    )
                ];

                assert_eq!(result, expected.into(),);
            }
            // `HelloWorldWktV1Uuid, primary_key:
            // Some(valid_uuid)`. We do not test for `primary_key:
            // Some(invalid_uuid)` because we will not get a value of
            // `well_formed_dynamic_message` if WKTs are invalid.
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::HelloWorldWktV1Uuid;

                use fdb_rl_proto::fdb_rl::field::v1::Uuid as FdbRLWktV1UuidProto;

                let hello_world_wkt_v1_uuid = HelloWorldWktV1Uuid {
                    primary_key: Some(FdbRLWktV1UuidProto::from(
                        Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap(),
                    )),
                    hello: Some("hello".to_string()),
                    world: None,
                };

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(hello_world_wkt_v1_uuid.descriptor())
                        .unwrap();

                let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                    well_formed_message_descriptor,
                    &hello_world_wkt_v1_uuid,
                ))
                .unwrap();

                let result = Value::try_from(well_formed_dynamic_message).unwrap();
                let expected = tuple![
                    ("fdb_rl_type", "message_HelloWorldWktV1Uuid"),
                    (
                        "fdb_rl_value",
                        tuple![
                            (
                                "primary_key",
                                tuple![
                                    ("fdb_rl_type", "message_fdb_rl.field.v1.UUID"),
                                    (
                                        "fdb_rl_value",
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
                                    )
                                ]
                            ),
                            (
                                "hello",
                                tuple![("fdb_rl_type", "string"), ("fdb_rl_value", "hello")]
                            ),
                            (
                                "world",
                                tuple![("fdb_rl_type", "string"), ("fdb_rl_value", Value::Null)]
                            )
                        ]
                    )
                ];

                assert_eq!(result, expected.into(),);
            }
            // `HelloWorldNestedOuter, nested_inner: None`
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::HelloWorldNestedOuter;

                let hello_world_nested_outer = HelloWorldNestedOuter {
                    nested_inner: None,
                    hello: Some("hello".to_string()),
                };

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(hello_world_nested_outer.descriptor())
                        .unwrap();

                let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                    well_formed_message_descriptor,
                    &hello_world_nested_outer,
                ))
                .unwrap();

                let result = Value::try_from(well_formed_dynamic_message).unwrap();
                let expected = tuple![
                    ("fdb_rl_type", "message_HelloWorldNestedOuter"),
                    (
                        "fdb_rl_value",
                        tuple![
                            (
                                "nested_inner",
                                tuple![
                                    ("fdb_rl_type", "message_HelloWorldNestedInner"),
                                    ("fdb_rl_value", Value::Null)
                                ]
                            ),
                            (
                                "hello",
                                tuple![("fdb_rl_type", "string"), ("fdb_rl_value", "hello"),]
                            )
                        ]
                    )
                ];

                assert_eq!(result, expected.into(),);
            }
            // `HelloWorldNestedOuter, nested_inner: Some(...)`
            //
            // In this case, `("fdb_rl_type",
            // "message_HelloWorldNestedInner")` appears twice. Once
            // as a part of the value and another time to describe the
            // field.
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::{
                    HelloWorldNestedInner, HelloWorldNestedOuter,
                };

                let hello_world_nested_outer = HelloWorldNestedOuter {
                    nested_inner: Some(HelloWorldNestedInner {
                        world: Some("world".to_string()),
                    }),
                    hello: Some("hello".to_string()),
                };

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(hello_world_nested_outer.descriptor())
                        .unwrap();

                let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                    well_formed_message_descriptor,
                    &hello_world_nested_outer,
                ))
                .unwrap();

                let result = Value::try_from(well_formed_dynamic_message).unwrap();
                let expected = tuple![
                    ("fdb_rl_type", "message_HelloWorldNestedOuter"),
                    (
                        "fdb_rl_value",
                        tuple![
                            (
                                "nested_inner",
                                tuple![
                                    ("fdb_rl_type", "message_HelloWorldNestedInner"),
                                    (
                                        "fdb_rl_value",
                                        tuple![
                                            ("fdb_rl_type", "message_HelloWorldNestedInner"),
                                            (
                                                "fdb_rl_value",
                                                tuple![(
                                                    "world",
                                                    tuple![
                                                        ("fdb_rl_type", "string"),
                                                        ("fdb_rl_value", "world")
                                                    ]
                                                )]
                                            )
                                        ]
                                    )
                                ]
                            ),
                            (
                                "hello",
                                tuple![("fdb_rl_type", "string"), ("fdb_rl_value", "hello"),]
                            )
                        ]
                    )
                ];

                assert_eq!(result, expected.into(),);
            }
            // `HelloWorldMap, empty`
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::HelloWorldMap;

                let hello_world_map = HelloWorldMap::default();

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(hello_world_map.descriptor()).unwrap();

                let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                    well_formed_message_descriptor,
                    &hello_world_map,
                ))
                .unwrap();

                let result = Value::try_from(well_formed_dynamic_message).unwrap();
                let expected = tuple![
                    ("fdb_rl_type", "message_HelloWorldMap"),
                    (
                        "fdb_rl_value",
                        tuple![
                            (
                                "field_double",
                                tuple![("fdb_rl_type", "map_double"), ("fdb_rl_value", tuple![]),]
                            ),
                            (
                                "field_float",
                                tuple![("fdb_rl_type", "map_float"), ("fdb_rl_value", tuple![]),]
                            ),
                            (
                                "field_int32",
                                tuple![("fdb_rl_type", "map_int32"), ("fdb_rl_value", tuple![]),]
                            ),
                            (
                                "field_int64",
                                tuple![("fdb_rl_type", "map_int64"), ("fdb_rl_value", tuple![]),]
                            ),
                            (
                                "field_sint32",
                                tuple![("fdb_rl_type", "map_sint32"), ("fdb_rl_value", tuple![]),]
                            ),
                            (
                                "field_sint64",
                                tuple![("fdb_rl_type", "map_sint64"), ("fdb_rl_value", tuple![]),]
                            ),
                            (
                                "field_sfixed32",
                                tuple![("fdb_rl_type", "map_sfixed32"), ("fdb_rl_value", tuple![]),]
                            ),
                            (
                                "field_sfixed64",
                                tuple![("fdb_rl_type", "map_sfixed64"), ("fdb_rl_value", tuple![]),]
                            ),
                            (
                                "field_bool",
                                tuple![("fdb_rl_type", "map_bool"), ("fdb_rl_value", tuple![]),]
                            ),
                            (
                                "field_string",
                                tuple![("fdb_rl_type", "map_string"), ("fdb_rl_value", tuple![]),]
                            ),
                            (
                                "field_bytes",
                                tuple![("fdb_rl_type", "map_bytes"), ("fdb_rl_value", tuple![]),]
                            ),
                            (
                                "field_enum",
                                tuple![
                                    ("fdb_rl_type", "map_enum_Size"),
                                    ("fdb_rl_value", tuple![]),
                                ]
                            ),
                            (
                                "field_message",
                                tuple![
                                    ("fdb_rl_type", "map_message_HelloWorldString"),
                                    ("fdb_rl_value", tuple![]),
                                ]
                            ),
                            (
                                "field_message_wkt_v1_uuid",
                                tuple![
                                    ("fdb_rl_type", "map_message_fdb_rl.field.v1.UUID"),
                                    ("fdb_rl_value", tuple![]),
                                ]
                            ),
                        ]
                    )
                ];

                assert_eq!(result, expected.into(),);
            }
            // `HelloWorldMap, not empty`
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::{HelloWorldMap, HelloWorldString};
		use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::hello_world_map::Size;
		use fdb_rl_proto::fdb_rl::field::v1::Uuid as FdbRLWktV1UuidProto;

                let hello_world_map = HelloWorldMap {
                    field_double: HashMap::from([
                        ("hello".to_string(), 3.14),
                        ("world".to_string(), 3.14),
                    ]),
                    field_float: HashMap::from([
                        ("hello".to_string(), 3.14),
                        ("world".to_string(), 3.14),
                    ]),
                    field_int32: HashMap::from([
                        ("hello".to_string(), 108),
                        ("world".to_string(), 108),
                    ]),
                    field_int64: HashMap::from([
                        ("hello".to_string(), 108),
                        ("world".to_string(), 108),
                    ]),
                    field_sint32: HashMap::from([
                        ("hello".to_string(), 108),
                        ("world".to_string(), 108),
                    ]),
                    field_sint64: HashMap::from([
                        ("hello".to_string(), 108),
                        ("world".to_string(), 108),
                    ]),
                    field_sfixed32: HashMap::from([
                        ("hello".to_string(), 108),
                        ("world".to_string(), 108),
                    ]),
                    field_sfixed64: HashMap::from([
                        ("hello".to_string(), 108),
                        ("world".to_string(), 108),
                    ]),
                    field_bool: HashMap::from([
                        ("hello".to_string(), true),
                        ("world".to_string(), false),
                    ]),
                    field_string: HashMap::from([
                        ("hello".to_string(), "abcd".to_string()),
                        ("world".to_string(), "efgh".to_string()),
                    ]),
                    field_bytes: HashMap::from([
                        ("hello".to_string(), Bytes::from_static(b"abcd")),
                        ("world".to_string(), Bytes::from_static(b"efgh")),
                    ]),
                    field_enum: HashMap::from([
                        ("hello".to_string(), Size::Small.into()),
                        ("world".to_string(), Size::Medium.into()),
                    ]),
                    field_message: HashMap::from([
                        (
                            "hello".to_string(),
                            HelloWorldString {
                                hello: Some("hello".to_string()),
                                world: None,
                            },
                        ),
                        (
                            "world".to_string(),
                            HelloWorldString {
                                hello: None,
                                world: Some("world".to_string()),
                            },
                        ),
                    ]),
                    field_message_wkt_v1_uuid: HashMap::from([
                        (
                            "hello".to_string(),
                            FdbRLWktV1UuidProto::from(
                                Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap(),
                            ),
                        ),
                        (
                            "world".to_string(),
                            FdbRLWktV1UuidProto::from(
                                Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap(),
                            ),
                        ),
                    ]),
                };

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(hello_world_map.descriptor()).unwrap();

                let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                    well_formed_message_descriptor,
                    &hello_world_map,
                ))
                .unwrap();

                let result = Value::try_from(well_formed_dynamic_message).unwrap();
                let expected = tuple![
                    ("fdb_rl_type", "message_HelloWorldMap"),
                    (
                        "fdb_rl_value",
                        tuple![
                            (
                                "field_double",
                                tuple![
                                    ("fdb_rl_type", "map_double"),
                                    (
                                        "fdb_rl_value",
                                        tuple![
                                            (
                                                "hello",
                                                tuple![
                                                    ("fdb_rl_type", "double"),
                                                    ("fdb_rl_value", 3.14),
                                                ]
                                            ),
                                            (
                                                "world",
                                                tuple![
                                                    ("fdb_rl_type", "double"),
                                                    ("fdb_rl_value", 3.14),
                                                ]
                                            ),
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_float",
                                tuple![
                                    ("fdb_rl_type", "map_float"),
                                    (
                                        "fdb_rl_value",
                                        tuple![
                                            (
                                                "hello",
                                                tuple![
                                                    ("fdb_rl_type", "float"),
                                                    ("fdb_rl_value", 3.14),
                                                ]
                                            ),
                                            (
                                                "world",
                                                tuple![
                                                    ("fdb_rl_type", "float"),
                                                    ("fdb_rl_value", 3.14),
                                                ]
                                            ),
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_int32",
                                tuple![
                                    ("fdb_rl_type", "map_int32"),
                                    (
                                        "fdb_rl_value",
                                        tuple![
                                            (
                                                "hello",
                                                tuple![
                                                    ("fdb_rl_type", "int32"),
                                                    ("fdb_rl_value", 108),
                                                ]
                                            ),
                                            (
                                                "world",
                                                tuple![
                                                    ("fdb_rl_type", "int32"),
                                                    ("fdb_rl_value", 108),
                                                ]
                                            ),
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_int64",
                                tuple![
                                    ("fdb_rl_type", "map_int64"),
                                    (
                                        "fdb_rl_value",
                                        tuple![
                                            (
                                                "hello",
                                                tuple![
                                                    ("fdb_rl_type", "int64"),
                                                    ("fdb_rl_value", 108),
                                                ]
                                            ),
                                            (
                                                "world",
                                                tuple![
                                                    ("fdb_rl_type", "int64"),
                                                    ("fdb_rl_value", 108),
                                                ]
                                            )
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_sint32",
                                tuple![
                                    ("fdb_rl_type", "map_sint32"),
                                    (
                                        "fdb_rl_value",
                                        tuple![
                                            (
                                                "hello",
                                                tuple![
                                                    ("fdb_rl_type", "sint32"),
                                                    ("fdb_rl_value", 108),
                                                ]
                                            ),
                                            (
                                                "world",
                                                tuple![
                                                    ("fdb_rl_type", "sint32"),
                                                    ("fdb_rl_value", 108),
                                                ]
                                            )
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_sint64",
                                tuple![
                                    ("fdb_rl_type", "map_sint64"),
                                    (
                                        "fdb_rl_value",
                                        tuple![
                                            (
                                                "hello",
                                                tuple![
                                                    ("fdb_rl_type", "sint64"),
                                                    ("fdb_rl_value", 108),
                                                ]
                                            ),
                                            (
                                                "world",
                                                tuple![
                                                    ("fdb_rl_type", "sint64"),
                                                    ("fdb_rl_value", 108),
                                                ]
                                            )
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_sfixed32",
                                tuple![
                                    ("fdb_rl_type", "map_sfixed32"),
                                    (
                                        "fdb_rl_value",
                                        tuple![
                                            (
                                                "hello",
                                                tuple![
                                                    ("fdb_rl_type", "sfixed32"),
                                                    ("fdb_rl_value", 108),
                                                ]
                                            ),
                                            (
                                                "world",
                                                tuple![
                                                    ("fdb_rl_type", "sfixed32"),
                                                    ("fdb_rl_value", 108),
                                                ]
                                            )
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_sfixed64",
                                tuple![
                                    ("fdb_rl_type", "map_sfixed64"),
                                    (
                                        "fdb_rl_value",
                                        tuple![
                                            (
                                                "hello",
                                                tuple![
                                                    ("fdb_rl_type", "sfixed64"),
                                                    ("fdb_rl_value", 108),
                                                ]
                                            ),
                                            (
                                                "world",
                                                tuple![
                                                    ("fdb_rl_type", "sfixed64"),
                                                    ("fdb_rl_value", 108),
                                                ]
                                            )
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_bool",
                                tuple![
                                    ("fdb_rl_type", "map_bool"),
                                    (
                                        "fdb_rl_value",
                                        tuple![
                                            (
                                                "hello",
                                                tuple![
                                                    ("fdb_rl_type", "bool"),
                                                    ("fdb_rl_value", Value::Boolean(true)),
                                                ]
                                            ),
                                            (
                                                "world",
                                                tuple![
                                                    ("fdb_rl_type", "bool"),
                                                    ("fdb_rl_value", Value::Boolean(false)),
                                                ]
                                            )
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_string",
                                tuple![
                                    ("fdb_rl_type", "map_string"),
                                    (
                                        "fdb_rl_value",
                                        tuple![
                                            (
                                                "hello",
                                                tuple![
                                                    ("fdb_rl_type", "string"),
                                                    ("fdb_rl_value", "abcd"),
                                                ]
                                            ),
                                            (
                                                "world",
                                                tuple![
                                                    ("fdb_rl_type", "string"),
                                                    ("fdb_rl_value", "efgh"),
                                                ]
                                            )
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_bytes",
                                tuple![
                                    ("fdb_rl_type", "map_bytes"),
                                    (
                                        "fdb_rl_value",
                                        tuple![
                                            (
                                                "hello",
                                                tuple![
                                                    ("fdb_rl_type", "bytes"),
                                                    (
                                                        "fdb_rl_value",
                                                        Value::Blob(
                                                            Vec::<u8>::from(Bytes::from_static(
                                                                b"abcd"
                                                            ))
                                                            .into()
                                                        )
                                                    )
                                                ]
                                            ),
                                            (
                                                "world",
                                                tuple![
                                                    ("fdb_rl_type", "bytes"),
                                                    (
                                                        "fdb_rl_value",
                                                        Value::Blob(
                                                            Vec::<u8>::from(Bytes::from_static(
                                                                b"efgh"
                                                            ))
                                                            .into()
                                                        )
                                                    ),
                                                ]
                                            )
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_enum",
                                tuple![
                                    ("fdb_rl_type", "map_enum_Size"),
                                    (
                                        "fdb_rl_value",
                                        tuple![
                                            (
                                                "hello",
                                                tuple![
                                                    ("fdb_rl_type", "enum_Size"),
                                                    (
                                                        "fdb_rl_value",
                                                        tuple![
                                                            ("name", "SIZE_SMALL"),
                                                            ("number", 1),
                                                        ]
                                                    )
                                                ]
                                            ),
                                            (
                                                "world",
                                                tuple![
                                                    ("fdb_rl_type", "enum_Size"),
                                                    (
                                                        "fdb_rl_value",
                                                        tuple![
                                                            ("name", "SIZE_MEDIUM"),
                                                            ("number", 2),
                                                        ]
                                                    )
                                                ]
                                            )
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_message",
                                tuple![
                                    ("fdb_rl_type", "map_message_HelloWorldString"),
                                    (
                                        "fdb_rl_value",
                                        tuple![
                                            (
                                                "hello",
                                                tuple![
                                                    ("fdb_rl_type", "message_HelloWorldString"),
                                                    (
                                                        "fdb_rl_value",
                                                        tuple![
                                                            (
                                                                "fdb_rl_type",
                                                                "message_HelloWorldString"
                                                            ),
                                                            (
                                                                "fdb_rl_value",
                                                                tuple![
                                                                    (
                                                                        "hello",
                                                                        tuple![
                                                                            (
                                                                                "fdb_rl_type",
                                                                                "string"
                                                                            ),
                                                                            (
                                                                                "fdb_rl_value",
                                                                                "hello"
                                                                            ),
                                                                        ]
                                                                    ),
                                                                    (
                                                                        "world",
                                                                        tuple![
                                                                            (
                                                                                "fdb_rl_type",
                                                                                "string"
                                                                            ),
                                                                            (
                                                                                "fdb_rl_value",
                                                                                Value::Null
                                                                            ),
                                                                        ]
                                                                    ),
                                                                ]
                                                            )
                                                        ]
                                                    )
                                                ]
                                            ),
                                            (
                                                "world",
                                                tuple![
                                                    ("fdb_rl_type", "message_HelloWorldString"),
                                                    (
                                                        "fdb_rl_value",
                                                        tuple![
                                                            (
                                                                "fdb_rl_type",
                                                                "message_HelloWorldString"
                                                            ),
                                                            (
                                                                "fdb_rl_value",
                                                                tuple![
                                                                    (
                                                                        "hello",
                                                                        tuple![
                                                                            (
                                                                                "fdb_rl_type",
                                                                                "string"
                                                                            ),
                                                                            (
                                                                                "fdb_rl_value",
                                                                                Value::Null
                                                                            ),
                                                                        ]
                                                                    ),
                                                                    (
                                                                        "world",
                                                                        tuple![
                                                                            (
                                                                                "fdb_rl_type",
                                                                                "string"
                                                                            ),
                                                                            (
                                                                                "fdb_rl_value",
                                                                                "world"
                                                                            ),
                                                                        ]
                                                                    ),
                                                                ]
                                                            )
                                                        ]
                                                    )
                                                ]
                                            )
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_message_wkt_v1_uuid",
                                tuple![
                                    ("fdb_rl_type", "map_message_fdb_rl.field.v1.UUID"),
                                    (
                                        "fdb_rl_value",
                                        tuple![
                                            (
                                                "hello",
                                                tuple![
                                                    ("fdb_rl_type", "message_fdb_rl.field.v1.UUID"),
                                                    (
                                                        "fdb_rl_value",
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
                                                    )
                                                ]
                                            ),
                                            (
                                                "world",
                                                tuple![
                                                    ("fdb_rl_type", "message_fdb_rl.field.v1.UUID"),
                                                    (
                                                        "fdb_rl_value",
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
                                                    )
                                                ]
                                            )
                                        ]
                                    ),
                                ]
                            ),
                        ]
                    )
                ];

                assert_eq!(result, expected.into(),);
            }
            // `HelloWorldRepeated, empty`
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::HelloWorldRepeated;

                let hello_world_repeated = HelloWorldRepeated::default();

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(hello_world_repeated.descriptor())
                        .unwrap();

                let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                    well_formed_message_descriptor,
                    &hello_world_repeated,
                ))
                .unwrap();

                let result = Value::try_from(well_formed_dynamic_message).unwrap();
                let expected = tuple![
                    ("fdb_rl_type", "message_HelloWorldRepeated"),
                    (
                        "fdb_rl_value",
                        tuple![
                            (
                                "field_double",
                                tuple![
                                    ("fdb_rl_type", "repeated_double"),
                                    ("fdb_rl_value", list![])
                                ]
                            ),
                            (
                                "field_float",
                                tuple![
                                    ("fdb_rl_type", "repeated_float"),
                                    ("fdb_rl_value", list![])
                                ]
                            ),
                            (
                                "field_int32",
                                tuple![
                                    ("fdb_rl_type", "repeated_int32"),
                                    ("fdb_rl_value", list![])
                                ]
                            ),
                            (
                                "field_int64",
                                tuple![
                                    ("fdb_rl_type", "repeated_int64"),
                                    ("fdb_rl_value", list![])
                                ]
                            ),
                            (
                                "field_sint32",
                                tuple![
                                    ("fdb_rl_type", "repeated_sint32"),
                                    ("fdb_rl_value", list![])
                                ]
                            ),
                            (
                                "field_sint64",
                                tuple![
                                    ("fdb_rl_type", "repeated_sint64"),
                                    ("fdb_rl_value", list![])
                                ]
                            ),
                            (
                                "field_sfixed32",
                                tuple![
                                    ("fdb_rl_type", "repeated_sfixed32"),
                                    ("fdb_rl_value", list![])
                                ]
                            ),
                            (
                                "field_sfixed64",
                                tuple![
                                    ("fdb_rl_type", "repeated_sfixed64"),
                                    ("fdb_rl_value", list![])
                                ]
                            ),
                            (
                                "field_bool",
                                tuple![("fdb_rl_type", "repeated_bool"), ("fdb_rl_value", list![])]
                            ),
                            (
                                "field_string",
                                tuple![
                                    ("fdb_rl_type", "repeated_string"),
                                    ("fdb_rl_value", list![])
                                ]
                            ),
                            (
                                "field_bytes",
                                tuple![
                                    ("fdb_rl_type", "repeated_bytes"),
                                    ("fdb_rl_value", list![])
                                ]
                            ),
                            (
                                "field_enum",
                                tuple![
                                    ("fdb_rl_type", "repeated_enum_Size"),
                                    ("fdb_rl_value", list![])
                                ]
                            ),
                            (
                                "field_message",
                                tuple![
                                    ("fdb_rl_type", "repeated_message_HelloWorldString"),
                                    ("fdb_rl_value", list![])
                                ]
                            ),
                            (
                                "field_message_wkt_v1_uuid",
                                tuple![
                                    ("fdb_rl_type", "repeated_message_fdb_rl.field.v1.UUID"),
                                    ("fdb_rl_value", list![])
                                ]
                            ),
                        ]
                    )
                ];

                assert_eq!(result, expected.into(),);
            }
            // `HelloWorldRepeated, not empty`
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::{HelloWorldRepeated, HelloWorldString};
		use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::hello_world_repeated::Size;
		use fdb_rl_proto::fdb_rl::field::v1::Uuid as FdbRLWktV1UuidProto;

                let hello_world_repeated = HelloWorldRepeated {
                    field_double: vec![3.14, 3.14],
                    field_float: vec![3.14, 3.14],
                    field_int32: vec![108, 108],
                    field_int64: vec![108, 108],
                    field_sint32: vec![108, 108],
                    field_sint64: vec![108, 108],
                    field_sfixed32: vec![108, 108],
                    field_sfixed64: vec![108, 108],
                    field_bool: vec![true, false],
                    field_string: vec!["abcd".to_string(), "efgh".to_string()],
                    field_bytes: vec![Bytes::from_static(b"abcd"), Bytes::from_static(b"efgh")],
                    field_enum: vec![Size::Small.into(), Size::Small.into()],
                    field_message: vec![
                        HelloWorldString {
                            hello: Some("hello".to_string()),
                            world: None,
                        },
                        HelloWorldString {
                            hello: None,
                            world: Some("world".to_string()),
                        },
                    ],
                    field_message_wkt_v1_uuid: vec![
                        FdbRLWktV1UuidProto::from(
                            Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap(),
                        ),
                        FdbRLWktV1UuidProto::from(
                            Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap(),
                        ),
                    ],
                };

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(hello_world_repeated.descriptor())
                        .unwrap();

                let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                    well_formed_message_descriptor,
                    &hello_world_repeated,
                ))
                .unwrap();

                let result = Value::try_from(well_formed_dynamic_message).unwrap();
                let expected = tuple![
                    ("fdb_rl_type", "message_HelloWorldRepeated"),
                    (
                        "fdb_rl_value",
                        tuple![
                            (
                                "field_double",
                                tuple![
                                    ("fdb_rl_type", "repeated_double"),
                                    (
                                        "fdb_rl_value",
                                        list![
                                            tuple![
                                                ("fdb_rl_type", "double"),
                                                ("fdb_rl_value", 3.14),
                                            ],
                                            tuple![
                                                ("fdb_rl_type", "double"),
                                                ("fdb_rl_value", 3.14),
                                            ]
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_float",
                                tuple![
                                    ("fdb_rl_type", "repeated_float"),
                                    (
                                        "fdb_rl_value",
                                        list![
                                            tuple![
                                                ("fdb_rl_type", "float"),
                                                ("fdb_rl_value", 3.14),
                                            ],
                                            tuple![
                                                ("fdb_rl_type", "float"),
                                                ("fdb_rl_value", 3.14),
                                            ]
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_int32",
                                tuple![
                                    ("fdb_rl_type", "repeated_int32"),
                                    (
                                        "fdb_rl_value",
                                        list![
                                            tuple![("fdb_rl_type", "int32"), ("fdb_rl_value", 108),],
                                            tuple![("fdb_rl_type", "int32"), ("fdb_rl_value", 108),]
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_int64",
                                tuple![
                                    ("fdb_rl_type", "repeated_int64"),
                                    (
                                        "fdb_rl_value",
                                        list![
                                            tuple![("fdb_rl_type", "int64"), ("fdb_rl_value", 108),],
                                            tuple![("fdb_rl_type", "int64"), ("fdb_rl_value", 108),]
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_sint32",
                                tuple![
                                    ("fdb_rl_type", "repeated_sint32"),
                                    (
                                        "fdb_rl_value",
                                        list![
                                            tuple![
                                                ("fdb_rl_type", "sint32"),
                                                ("fdb_rl_value", 108),
                                            ],
                                            tuple![
                                                ("fdb_rl_type", "sint32"),
                                                ("fdb_rl_value", 108),
                                            ]
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_sint64",
                                tuple![
                                    ("fdb_rl_type", "repeated_sint64"),
                                    (
                                        "fdb_rl_value",
                                        list![
                                            tuple![
                                                ("fdb_rl_type", "sint64"),
                                                ("fdb_rl_value", 108),
                                            ],
                                            tuple![
                                                ("fdb_rl_type", "sint64"),
                                                ("fdb_rl_value", 108),
                                            ]
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_sfixed32",
                                tuple![
                                    ("fdb_rl_type", "repeated_sfixed32"),
                                    (
                                        "fdb_rl_value",
                                        list![
                                            tuple![
                                                ("fdb_rl_type", "sfixed32"),
                                                ("fdb_rl_value", 108),
                                            ],
                                            tuple![
                                                ("fdb_rl_type", "sfixed32"),
                                                ("fdb_rl_value", 108),
                                            ]
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_sfixed64",
                                tuple![
                                    ("fdb_rl_type", "repeated_sfixed64"),
                                    (
                                        "fdb_rl_value",
                                        list![
                                            tuple![
                                                ("fdb_rl_type", "sfixed64"),
                                                ("fdb_rl_value", 108),
                                            ],
                                            tuple![
                                                ("fdb_rl_type", "sfixed64"),
                                                ("fdb_rl_value", 108),
                                            ]
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_bool",
                                tuple![
                                    ("fdb_rl_type", "repeated_bool"),
                                    (
                                        "fdb_rl_value",
                                        list![
                                            tuple![
                                                ("fdb_rl_type", "bool"),
                                                ("fdb_rl_value", Value::Boolean(true)),
                                            ],
                                            tuple![
                                                ("fdb_rl_type", "bool"),
                                                ("fdb_rl_value", Value::Boolean(false)),
                                            ]
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_string",
                                tuple![
                                    ("fdb_rl_type", "repeated_string"),
                                    (
                                        "fdb_rl_value",
                                        list![
                                            tuple![
                                                ("fdb_rl_type", "string"),
                                                ("fdb_rl_value", "abcd"),
                                            ],
                                            tuple![
                                                ("fdb_rl_type", "string"),
                                                ("fdb_rl_value", "efgh"),
                                            ]
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_bytes",
                                tuple![
                                    ("fdb_rl_type", "repeated_bytes"),
                                    (
                                        "fdb_rl_value",
                                        list![
                                            tuple![
                                                ("fdb_rl_type", "bytes"),
                                                (
                                                    "fdb_rl_value",
                                                    Value::Blob(
                                                        Vec::<u8>::from(Bytes::from_static(
                                                            b"abcd"
                                                        ))
                                                        .into()
                                                    )
                                                )
                                            ],
                                            tuple![
                                                ("fdb_rl_type", "bytes"),
                                                (
                                                    "fdb_rl_value",
                                                    Value::Blob(
                                                        Vec::<u8>::from(Bytes::from_static(
                                                            b"efgh"
                                                        ))
                                                        .into()
                                                    )
                                                ),
                                            ]
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_enum",
                                tuple![
                                    ("fdb_rl_type", "repeated_enum_Size"),
                                    (
                                        "fdb_rl_value",
                                        list![
                                            tuple![
                                                ("fdb_rl_type", "enum_Size"),
                                                (
                                                    "fdb_rl_value",
                                                    tuple![("name", "SIZE_SMALL"), ("number", 1),]
                                                )
                                            ],
                                            tuple![
                                                ("fdb_rl_type", "enum_Size"),
                                                (
                                                    "fdb_rl_value",
                                                    tuple![("name", "SIZE_SMALL"), ("number", 1),]
                                                )
                                            ]
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_message",
                                tuple![
                                    ("fdb_rl_type", "repeated_message_HelloWorldString"),
                                    (
                                        "fdb_rl_value",
                                        list![
                                            tuple![
                                                ("fdb_rl_type", "message_HelloWorldString"),
                                                (
                                                    "fdb_rl_value",
                                                    tuple![
                                                        ("fdb_rl_type", "message_HelloWorldString"),
                                                        (
                                                            "fdb_rl_value",
                                                            tuple![
                                                                (
                                                                    "hello",
                                                                    tuple![
                                                                        ("fdb_rl_type", "string"),
                                                                        ("fdb_rl_value", "hello"),
                                                                    ]
                                                                ),
                                                                (
                                                                    "world",
                                                                    tuple![
                                                                        ("fdb_rl_type", "string"),
                                                                        (
                                                                            "fdb_rl_value",
                                                                            Value::Null
                                                                        ),
                                                                    ]
                                                                ),
                                                            ]
                                                        )
                                                    ]
                                                )
                                            ],
                                            tuple![
                                                ("fdb_rl_type", "message_HelloWorldString"),
                                                (
                                                    "fdb_rl_value",
                                                    tuple![
                                                        ("fdb_rl_type", "message_HelloWorldString"),
                                                        (
                                                            "fdb_rl_value",
                                                            tuple![
                                                                (
                                                                    "hello",
                                                                    tuple![
                                                                        ("fdb_rl_type", "string"),
                                                                        (
                                                                            "fdb_rl_value",
                                                                            Value::Null
                                                                        ),
                                                                    ]
                                                                ),
                                                                (
                                                                    "world",
                                                                    tuple![
                                                                        ("fdb_rl_type", "string"),
                                                                        ("fdb_rl_value", "world"),
                                                                    ]
                                                                ),
                                                            ]
                                                        )
                                                    ]
                                                )
                                            ]
                                        ]
                                    ),
                                ]
                            ),
                            (
                                "field_message_wkt_v1_uuid",
                                tuple![
                                    ("fdb_rl_type", "repeated_message_fdb_rl.field.v1.UUID"),
                                    (
                                        "fdb_rl_value",
                                        list![
                                            tuple![
                                                ("fdb_rl_type", "message_fdb_rl.field.v1.UUID"),
                                                (
                                                    "fdb_rl_value",
                                                    tuple![
							(
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
							)
						    ]
                                                )
                                            ],
                                            tuple![
                                                ("fdb_rl_type", "message_fdb_rl.field.v1.UUID"),
                                                (
                                                    "fdb_rl_value",
                                                    tuple![
							(
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
							)
						    ]
                                                )
                                            ]
                                        ]
                                    ),
                                ]
                            ),
                        ]
                    )
                ];

                assert_eq!(result, expected.into(),);
            }
        }
    }
}
