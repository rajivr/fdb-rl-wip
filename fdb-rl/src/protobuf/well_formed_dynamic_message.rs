//! TODO

use fdb::error::{FdbError, FdbResult};

use prost_reflect::{DynamicMessage, Kind, ReflectMessage, Value as ProstReflectValue};

// We do not rename `Value` to `PartiqlValue`. If we did that `tuple!`
// macro fails.
use partiql_value::{tuple, Tuple, Value};

use rust_decimal::Decimal;

use std::convert::TryFrom;
use std::ops::Deref;

use super::error::{
    PROTOBUF_ILL_FORMED_MESSAGE, PROTOBUF_WELL_FORMED_DYNAMIC_MESSAGE_TO_PARTIQL_VALUE_ERROR,
};
use super::WellFormedMessageDescriptor;

/// Describes a valid `DynamicMessage`.
///
/// TODO

#[derive(Debug, PartialEq)]
pub(crate) struct WellFormedDynamicMessage {
    inner: DynamicMessage,
}

impl<T> TryFrom<(WellFormedMessageDescriptor, &T)> for WellFormedDynamicMessage
where
    T: ReflectMessage,
{
    type Error = FdbError;

    fn try_from(
        (well_formed_message_descriptor, message): (WellFormedMessageDescriptor, &T),
    ) -> FdbResult<WellFormedDynamicMessage> {
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
                    Kind::Message(_) => todo!(),
                    Kind::Enum(_) => todo!(),
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
        }
    }
}
