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
