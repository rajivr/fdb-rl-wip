//! TODO

use fdb::error::{FdbError, FdbResult};
use fdb_rl_proto::fdb_rl::field::v1::Uuid as FdbRLWktV1UuidProto;

use prost_reflect::{DynamicMessage, Kind, ReflectMessage, Value as ProstReflectValue};

// We do not rename `Value` to `PartiqlValue`. If we did that `tuple!`
// macro fails.
use partiql_value::{tuple, Tuple, Value};

use rust_decimal::Decimal;

use uuid::Uuid;

use std::convert::TryFrom;
use std::ops::Deref;

use super::error::{
    PROTOBUF_ILL_FORMED_MESSAGE, PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
};
use super::{WellFormedMessageDescriptor, FDB_RL_WKT_V1_UUID};

/// Describes a valid `DynamicMessage`.
///
/// TODO
#[derive(Debug, PartialEq)]
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

    // TODO: write test case to for valid/invalid uuid in a regular
    // field, list with some invalid, and map with some invalid. list
    // empty, map empty, nested test, nested recursion test. Also test
    // oneof.
    //
    // TODO: Continue from here.
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

    // We define this so that we can work with nested message without
    // having to check if the inner dynamic message is well formed or
    // not.
    //
    // When we have a value of `WellFormedDynamicMessage`, then the
    // whole message, including inner message is well formed.
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
                todo!();
            } else if field_descriptor.is_list() {
                // ```
                // repeated SomeType some_field = X;
                // ```
                todo!();
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
                            let partiql_value_decimal = Value::Decimal(
                                Decimal::try_from(
                                    if let ProstReflectValue::F64(f) =
                                        dynamic_message.get_field(&field_descriptor).deref()
                                    {
                                        *f
                                    } else {
                                        return Err(FdbError::new(
				    PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR
				));
                                    },
                                )
                                .map_err(|_| {
                                    FdbError::new(
                                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                                    )
                                })?
                                .into(),
                            );

                            tuple![
                                ("fdb_rl_type", "double"),
                                ("fdb_rl_value", partiql_value_decimal)
                            ]
                        } else {
                            tuple![("fdb_rl_type", "double"), ("fdb_rl_value", Value::Null)]
                        }
                    }
                    Kind::Float => {
                        if dynamic_message.has_field(&field_descriptor) {
                            let partiql_value_decimal = Value::Decimal(
                                Decimal::try_from(
                                    if let ProstReflectValue::F32(f) =
                                        dynamic_message.get_field(&field_descriptor).deref()
                                    {
                                        *f
                                    } else {
                                        return Err(FdbError::new(
				    PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR
				));
                                    },
                                )
                                .map_err(|_| {
                                    FdbError::new(
                                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                                    )
                                })?
                                .into(),
                            );

                            tuple![
                                ("fdb_rl_type", "float"),
                                ("fdb_rl_value", partiql_value_decimal)
                            ]
                        } else {
                            tuple![("fdb_rl_type", "float"), ("fdb_rl_value", Value::Null)]
                        }
                    }
                    Kind::Int32 => {
                        if dynamic_message.has_field(&field_descriptor) {
                            let partiql_value_integer = Value::Integer(
                                if let ProstReflectValue::I32(i) =
                                    dynamic_message.get_field(&field_descriptor).deref()
                                {
                                    *i
                                } else {
                                    return Err(FdbError::new(
                                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                                    ));
                                }
                                .into(),
                            );

                            tuple![
                                ("fdb_rl_type", "int32"),
                                ("fdb_rl_value", partiql_value_integer)
                            ]
                        } else {
                            tuple![("fdb_rl_type", "int32"), ("fdb_rl_value", Value::Null)]
                        }
                    }
                    Kind::Int64 => {
                        if dynamic_message.has_field(&field_descriptor) {
                            let partiql_value_integer = Value::Integer(
                                if let ProstReflectValue::I64(i) =
                                    dynamic_message.get_field(&field_descriptor).deref()
                                {
                                    *i
                                } else {
                                    return Err(FdbError::new(
                                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                                    ));
                                }
                                .into(),
                            );

                            tuple![
                                ("fdb_rl_type", "int64"),
                                ("fdb_rl_value", partiql_value_integer)
                            ]
                        } else {
                            tuple![("fdb_rl_type", "int64"), ("fdb_rl_value", Value::Null)]
                        }
                    }
                    Kind::Sint32 => {
                        if dynamic_message.has_field(&field_descriptor) {
                            let partiql_value_integer = Value::Integer(
                                if let ProstReflectValue::I32(i) =
                                    dynamic_message.get_field(&field_descriptor).deref()
                                {
                                    *i
                                } else {
                                    return Err(FdbError::new(
                                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                                    ));
                                }
                                .into(),
                            );

                            tuple![
                                ("fdb_rl_type", "sint32"),
                                ("fdb_rl_value", partiql_value_integer)
                            ]
                        } else {
                            tuple![("fdb_rl_type", "sint32"), ("fdb_rl_value", Value::Null)]
                        }
                    }
                    Kind::Sint64 => {
                        if dynamic_message.has_field(&field_descriptor) {
                            let partiql_value_integer = Value::Integer(
                                if let ProstReflectValue::I64(i) =
                                    dynamic_message.get_field(&field_descriptor).deref()
                                {
                                    *i
                                } else {
                                    return Err(FdbError::new(
                                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                                    ));
                                }
                                .into(),
                            );

                            tuple![
                                ("fdb_rl_type", "sint64"),
                                ("fdb_rl_value", partiql_value_integer)
                            ]
                        } else {
                            tuple![("fdb_rl_type", "sint64"), ("fdb_rl_value", Value::Null)]
                        }
                    }
                    Kind::Sfixed32 => {
                        if dynamic_message.has_field(&field_descriptor) {
                            let partiql_value_integer = Value::Integer(
                                if let ProstReflectValue::I32(i) =
                                    dynamic_message.get_field(&field_descriptor).deref()
                                {
                                    *i
                                } else {
                                    return Err(FdbError::new(
                                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                                    ));
                                }
                                .into(),
                            );

                            tuple![
                                ("fdb_rl_type", "sfixed32"),
                                ("fdb_rl_value", partiql_value_integer)
                            ]
                        } else {
                            tuple![("fdb_rl_type", "sfixed32"), ("fdb_rl_value", Value::Null)]
                        }
                    }
                    Kind::Sfixed64 => {
                        if dynamic_message.has_field(&field_descriptor) {
                            let partiql_value_integer = Value::Integer(
                                if let ProstReflectValue::I64(i) =
                                    dynamic_message.get_field(&field_descriptor).deref()
                                {
                                    *i
                                } else {
                                    return Err(FdbError::new(
                                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                                    ));
                                }
                                .into(),
                            );

                            tuple![
                                ("fdb_rl_type", "sfixed64"),
                                ("fdb_rl_value", partiql_value_integer)
                            ]
                        } else {
                            tuple![("fdb_rl_type", "sfixed64"), ("fdb_rl_value", Value::Null)]
                        }
                    }
                    Kind::Bool => {
                        if dynamic_message.has_field(&field_descriptor) {
                            let partiql_value_bool = Value::Boolean(
                                if let ProstReflectValue::Bool(b) =
                                    dynamic_message.get_field(&field_descriptor).deref()
                                {
                                    *b
                                } else {
                                    return Err(FdbError::new(
                                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                                    ));
                                },
                            );

                            tuple![
                                ("fdb_rl_type", "bool"),
                                ("fdb_rl_value", partiql_value_bool)
                            ]
                        } else {
                            tuple![("fdb_rl_type", "bool"), ("fdb_rl_value", Value::Null)]
                        }
                    }
                    Kind::String => {
                        if dynamic_message.has_field(&field_descriptor) {
                            let partiql_value_string = Value::String(
                                if let ProstReflectValue::String(s) =
                                    dynamic_message.get_field(&field_descriptor).into_owned()
                                {
                                    s
                                } else {
                                    return Err(FdbError::new(
                                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                                    ));
                                }
                                .into(),
                            );

                            tuple![
                                ("fdb_rl_type", "string"),
                                ("fdb_rl_value", partiql_value_string)
                            ]
                        } else {
                            tuple![("fdb_rl_type", "string"), ("fdb_rl_value", Value::Null)]
                        }
                    }
                    Kind::Bytes => {
                        if dynamic_message.has_field(&field_descriptor) {
                            let partiql_value_blob = Value::Blob(
                                if let ProstReflectValue::Bytes(b) =
                                    dynamic_message.get_field(&field_descriptor).into_owned()
                                {
                                    Vec::<u8>::from(b)
                                } else {
                                    return Err(FdbError::new(
                                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                                    ));
                                }
                                .into(),
                            );

                            tuple![
                                ("fdb_rl_type", "bytes"),
                                ("fdb_rl_value", partiql_value_blob)
                            ]
                        } else {
                            tuple![("fdb_rl_type", "bytes"), ("fdb_rl_value", Value::Null)]
                        }
                    }
                    Kind::Message(message_descriptor) => {
                        // First check if message descriptor is a well
                        // known type.
                        if message_descriptor.full_name() == FDB_RL_WKT_V1_UUID.deref().as_str() {
                            if dynamic_message.has_field(&field_descriptor) {
                                if let ProstReflectValue::Message(dm) =
                                    dynamic_message.get_field(&field_descriptor).as_ref()
                                {
                                    // `dm` is a dynamic message value
                                    // of type `fdb_rl.field.v1.UUID`.
                                    let uuid_bytes = if let ProstReflectValue::Bytes(b) = dm
                                        .get_field_by_name("value")
                                        .ok_or(FdbError::new(
                                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
					))?.as_ref() {
					    b.clone()
					} else {
					    return Err(FdbError::new(
						PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
					    ));
					};

                                    // TODO: Remove this check.
                                    //
                                    // If we have a value of
                                    // `WellFormedDynamicMessage`
                                    // type, then we will assume all
                                    // values of well known types
                                    // within it are valid.

                                    // Check that UUID bytes is well
                                    // formed. Since
                                    // `fdb_rl_proto::fdb_rl::field::v1::Uuid`
                                    // is an open struct, this is to
                                    // ensure that we do not
                                    // accidentally convert an invalid
                                    // value.
                                    Uuid::try_from(FdbRLWktV1UuidProto {
                                        value: uuid_bytes.clone(),
                                    })
                                    .map(|_| {
                                        tuple![
                                            (
                                                "fdb_rl_type",
                                                format!(
                                                    "message_{}",
                                                    FDB_RL_WKT_V1_UUID.deref().as_str()
                                                )
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
                                    .map_err(|_| {
                                        FdbError::new(
						PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR)
                                    })?
                                } else {
                                    return Err(FdbError::new(
                                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                                    ));
                                }
                            } else {
                                tuple![
                                    (
                                        "fdb_rl_type",
                                        format!("message_{}", FDB_RL_WKT_V1_UUID.deref().as_str())
                                    ),
                                    ("fdb_rl_value", Value::Null)
                                ]
                            }
                        } else {
                            // TODO: Write test case for this.
                            if dynamic_message.has_field(&field_descriptor) {
                                let partiql_value = if let ProstReflectValue::Message(dm) =
                                    dynamic_message.get_field(&field_descriptor).into_owned()
                                {
                                    WellFormedDynamicMessage::try_from_inner(dm)?
                                } else {
                                    return Err(FdbError::new(
                                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                                    ));
                                };

                                tuple![
                                    (
                                        "fdb_rl_type",
                                        format!("message_{}", message_descriptor.name())
                                    ),
                                    ("fdb_rl_value", partiql_value)
                                ]
                            } else {
                                tuple![
                                    (
                                        "fdb_rl_type",
                                        format!("message_{}", message_descriptor.name())
                                    ),
                                    ("fdb_rl_value", Value::Null)
                                ]
                            }
                        }
                    }
                    Kind::Enum(enum_descriptor) => {
                        if dynamic_message.has_field(&field_descriptor) {
                            let partiql_value_tuple = Value::Tuple(
                                if let ProstReflectValue::EnumNumber(i) =
                                    dynamic_message.get_field(&field_descriptor).deref()
                                {
                                    let number = *i;
                                    let name = enum_descriptor
                                    .get_value(number)
                                    .ok_or(FdbError::new(
                                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                                    ))?
                                    .name()
                                    .to_string();

                                    tuple![("name", name), ("number", number),]
                                } else {
                                    return Err(FdbError::new(
                                        PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
                                    ));
                                }
                                .into(),
                            );

                            tuple![
                                ("fdb_rl_type", format!("enum_{}", enum_descriptor.name())),
                                ("fdb_rl_value", partiql_value_tuple)
                            ]
                        } else {
                            tuple![
                                ("fdb_rl_type", format!("enum_{}", enum_descriptor.name())),
                                ("fdb_rl_value", Value::Null)
                            ]
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
        // TODO: Logic below needs to be rewritten.

        // Ensure that `message` is compatible with
        // `well_formed_message_descriptor`.
        if well_formed_message_descriptor.is_evolvable_to(
            WellFormedMessageDescriptor::try_from(message.descriptor())
                .map_err(|_| FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE))?,
        ) {
            let mut dynamic_message = DynamicMessage::new(well_formed_message_descriptor.into());

            if let Err(_) = dynamic_message.transcode_from(message) {
                Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE))
            } else {
                // TODO: Check to make sure that any well known types
                // that might be inside dynamic message is well
                // formed. Basically if any WKT is ill formed, do not
                // return a value of well formed dynamic message.

                Ok(WellFormedDynamicMessage {
                    inner: dynamic_message,
                })
            }
        } else {
            Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE))
        }
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

        use partiql_value::{tuple, Value};

        use prost_reflect::ReflectMessage;

        use uuid::Uuid;

        use std::convert::TryFrom;

        use super::super::super::error::PROTOBUF_ILL_FORMED_MESSAGE;
        use super::super::super::WellFormedMessageDescriptor;
        use super::super::WellFormedDynamicMessage;

        #[test]
        fn try_from_well_formed_message_descriptor_ref_t_try_from() {
            // Valid message
            {
                // Same message descriptor
                {
                    use fdb_rl_proto::fdb_rl::field::v1::Uuid as FdbRLWktUuidProto;
                    use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_message_descriptor::good::v1::HelloWorld;

                    let hello_world = HelloWorld {
                        primary_key: Some(FdbRLWktUuidProto::from(
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
                // Evolved message descriptor
                {
                    use fdb_rl_proto::fdb_rl::field::v1::Uuid as FdbRLWktUuidProto;
                    use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_message_descriptor::good::v1::HelloWorld as MetadataHelloWorld;
		    use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_message_descriptor::good::v2::HelloWorld as CodeHelloWorld;

                    let well_formed_message_descriptor = WellFormedMessageDescriptor::try_from(
                        MetadataHelloWorld::default().descriptor(),
                    )
                    .unwrap();

                    // Does not have the field `hello_world`.
                    let metadata_hello_world = MetadataHelloWorld {
                        primary_key: Some(FdbRLWktUuidProto::from(
                            Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap(),
                        )),
                        hello: Some("hello".to_string()),
                        world: Some("world".to_string()),
                    };

                    let code_hello_world = CodeHelloWorld {
                        primary_key: Some(FdbRLWktUuidProto::from(
                            Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap(),
                        )),
                        hello: Some("hello".to_string()),
                        world: Some("world".to_string()),
                        hello_world: Some("hello_world".to_string()),
                    };

                    let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
                        well_formed_message_descriptor,
                        &code_hello_world,
                    ))
                    .unwrap();

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
                    // When the dynamic message is "merged" into
                    // `CodeHelloWorld`, then unknown field is
                    // properly resolved.
                    //
                    // [1] https://protobuf.dev/programming-guides/proto3/#unknowns
                    // [2] https://protobuf.dev/programming-guides/proto2/#unknowns
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
                use fdb_rl_proto::fdb_rl::field::v1::Uuid as FdbRLWktUuidProto;
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_message_descriptor::good::v1::{HelloWorld, HelloWorldMap};

                use std::collections::HashMap;

                let well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(HelloWorld::default().descriptor())
                        .unwrap();

                let hello_world_map = HelloWorldMap {
                    hello_world_map: HashMap::from([(
                        "hello_world".to_string(),
                        HelloWorld {
                            primary_key: Some(FdbRLWktUuidProto::from(
                                Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap(),
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

                let hello_world_bool = HelloWorldBytes {
                    hello: Some(Bytes::from_static(b"hello")),
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
            // `HelloWorldWktV1Uuid, primary_key: Some(invalid_uuid)`
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::HelloWorldWktV1Uuid;

                use fdb_rl_proto::fdb_rl::field::v1::Uuid as FdbRLWktV1UuidProto;

                let hello_world_wkt_v1_uuid = HelloWorldWktV1Uuid {
                    primary_key: Some(FdbRLWktV1UuidProto {
                        value: Bytes::from([4, 54, 67, 12, 43, 2, 98, 76].as_ref()),
                    }),
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
            }
        }
    }
}
