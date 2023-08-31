//! TODO

use fdb::error::{FdbError, FdbResult};
use prost_reflect::{FileDescriptor, MessageDescriptor, Syntax};

use std::convert::TryFrom;

use super::error::KEY_EXPRESSION_ILL_FORMED_MESSAGE_DESCRIPTOR;

// TODO: Remove later
//
// Takes a `prost_reflect::MessageDescriptor`, and performs checks to
// ensure that message descriptor is well formed. If it is well
// formed, it wraps the message descriptor and returns a value of type
// `WellFormedMessageDescriptor`.
//
// If you have a value of type `WellFormedMessageDescriptor`, you can
// be sure that the provided `prost_reflect::MessageDescriptor` is
// well formed.

/// TODO
#[derive(Debug, PartialEq)]
pub(crate) struct WellFormedMessageDescriptor {
    inner: MessageDescriptor,
}

impl TryFrom<MessageDescriptor> for WellFormedMessageDescriptor {
    type Error = FdbError;

    fn try_from(message_descriptor: MessageDescriptor) -> FdbResult<WellFormedMessageDescriptor> {
        if walk_message_descriptor(&MessageDescriptorValidatorVisitor, &message_descriptor) {
            Ok(WellFormedMessageDescriptor {
                inner: message_descriptor,
            })
        } else {
            Err(FdbError::new(KEY_EXPRESSION_ILL_FORMED_MESSAGE_DESCRIPTOR))
        }
    }
}

trait Visitor {
    fn visit_parent_file_descriptor(&self, file_descriptor: FileDescriptor) -> bool;
}

fn walk_message_descriptor(visitor: &dyn Visitor, message_descriptor: &MessageDescriptor) -> bool {
    if !visitor.visit_parent_file_descriptor(message_descriptor.parent_file()) {
        return false;
    }
    true
}

/// TODO
#[derive(Debug)]
struct MessageDescriptorValidatorVisitor;

impl Visitor for MessageDescriptorValidatorVisitor {
    /// Returns `true` if `.proto` file was compiled using `syntax =
    /// proto3`.
    fn visit_parent_file_descriptor(&self, file_descriptor: FileDescriptor) -> bool {
        matches!(file_descriptor.syntax(), Syntax::Proto3)
    }
}

#[cfg(test)]
mod tests {
    mod well_formed_message_descriptor {
        use fdb::error::FdbError;
        use fdb_rl_proto::fdb_rl_test::key_expression::well_formed_message_descriptor::bad::v1::HelloWorld as HelloWorldProto2;
        use prost_reflect::ReflectMessage;

        use std::convert::TryFrom;

        use super::super::super::error::KEY_EXPRESSION_ILL_FORMED_MESSAGE_DESCRIPTOR;
        use super::super::WellFormedMessageDescriptor;

        #[test]
        fn try_from_message_descriptor_try_from() {
            // Invalid message descriptor
            {
                let hello_world = HelloWorldProto2 {
                    hello: "hello".to_string(),
                    world: Some("world".to_string()),
                };

                let message_descriptor = hello_world.descriptor();

                assert_eq!(
                    WellFormedMessageDescriptor::try_from(message_descriptor),
                    Err(FdbError::new(KEY_EXPRESSION_ILL_FORMED_MESSAGE_DESCRIPTOR))
                );
            }
        }
    }
}
