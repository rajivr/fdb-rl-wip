//! TODO

use fdb::error::{FdbError, FdbResult};
use prost::Message;
use prost_reflect::DynamicMessage;

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
    T: Message,
{
    type Error = FdbError;

    fn try_from(
        (well_formed_message_descriptor, message): (WellFormedMessageDescriptor, &T),
    ) -> FdbResult<WellFormedDynamicMessage> {
        let mut dynamic_message = DynamicMessage::new(well_formed_message_descriptor.into());

        if let Err(_) = dynamic_message.transcode_from(message) {
            Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE))
        } else {
            Ok(WellFormedDynamicMessage {
                inner: dynamic_message,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    mod well_formed_dynamic_message {
        use bytes::BytesMut;

        use prost::Message;
        use prost_reflect::ReflectMessage;
        use uuid::Uuid;

        use std::convert::TryFrom;

        use super::super::super::WellFormedMessageDescriptor;
        use super::super::WellFormedDynamicMessage;

        #[test]
        fn try_from_well_formed_message_descriptor_ref_t_try_from() {
            // Valid message
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
                ))
                .unwrap();

                // TODO: it looks like
                // `DynamicMessage::`transcode_from` and
                // `DynamicMessage::`transcode_to` is very
                // liberal. So, it seems like it is our responsiblity
                // to ensure that we correctly pass the right `T:
                // Message`, with compatible message descriptors. In
                // this case, `HelloWorld` and `HelloWorldMap` ase
                // clearly incompatible, even though both their
                // message descriptors are "well formed".

                let dynamic_message = well_formed_dynamic_message.inner;

                let mut bytes = BytesMut::new();
                let _ = dynamic_message.encode(&mut bytes).unwrap();

                println!("{:?}", HelloWorld::decode(bytes.clone()));
                println!("{:?}", HelloWorldMap::decode(bytes.clone()));

                println!("-------");

                println!("{:?}", dynamic_message.transcode_to::<HelloWorld>());
                println!("{:?}", dynamic_message.transcode_to::<HelloWorldMap>());
            }
        }
    }
}
