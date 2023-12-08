//! TODO

use fdb::error::{FdbError, FdbResult};
use prost_reflect::{DynamicMessage, ReflectMessage};

use std::convert::TryFrom;

use super::error::PROTOBUF_ILL_FORMED_MESSAGE;
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

#[cfg(test)]
mod tests {
    mod well_formed_dynamic_message {
        use fdb::error::FdbError;

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
    }
}
