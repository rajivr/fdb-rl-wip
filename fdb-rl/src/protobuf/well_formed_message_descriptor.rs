//! TODO

use fdb::error::{FdbError, FdbResult};
use fdb_rl_proto::fdb_rl::field::v1::Uuid as FdbRLWktUuidProto;
use prost::Message;
use prost_reflect::{
    Cardinality, EnumDescriptor, FieldDescriptor, FileDescriptor, Kind, MessageDescriptor,
    ReflectMessage, Syntax,
};

use std::collections::HashSet;
use std::convert::TryFrom;
use std::ops::Deref;
use std::sync::LazyLock;

use super::error::PROTOBUF_ILL_FORMED_MESSAGE_DESCRIPTOR;

/// Well known types that are known to FDB Record Layer.
///
/// We check based on full name. This is so that we do not get an
/// error in case `protoc` produces descriptor pool in sligtly
/// different ways when different options are used.
static FDB_RL_WKT_FULL_NAME: LazyLock<Vec<String>> = LazyLock::new(|| {
    vec![FdbRLWktUuidProto::default()
        .descriptor()
        .full_name()
        .to_string()]
});

// Wrapper types so we do not accidentally mixup old and new
// descriptors.

#[derive(PartialEq, Eq, Hash)]
struct OldMessageDescriptorProtoBytes(Vec<u8>);

#[derive(PartialEq, Eq, Hash)]
struct NewMessageDescriptorProtoBytes(Vec<u8>);

struct OldMessageDescriptor(MessageDescriptor);
struct NewMessageDescriptor(MessageDescriptor);

struct OldFieldDescriptor(FieldDescriptor);
struct NewFieldDescriptor(FieldDescriptor);

struct OldEnumDescriptor(EnumDescriptor);
struct NewEnumDescriptor(EnumDescriptor);

/// Describes a valid `MessageDescriptor`.
///
/// Takes a `prost_reflect::MessageDescriptor`, and performs checks to
/// ensure that message descriptor is well formed. If it is well
/// formed, it wraps the message descriptor and returns a value of
/// type `WellFormedMessageDescriptor`.
///
/// If you have a value of type `WellFormedMessageDescriptor`, you can
/// be sure that the provided `prost_reflect::MessageDescriptor` is
/// well formed.
#[derive(Debug, PartialEq)]
pub(crate) struct WellFormedMessageDescriptor {
    inner: MessageDescriptor,
}

impl WellFormedMessageDescriptor {
    pub(crate) fn is_evolvable_to(
        &self,
        well_formed_message_descriptor: WellFormedMessageDescriptor,
    ) -> bool {
        let old_message_descriptor = self.inner.clone();
        let new_message_descriptor = well_formed_message_descriptor.into();
        let mut seen_descriptors = HashSet::new();

        Self::validate_message(
            OldMessageDescriptor(old_message_descriptor),
            NewMessageDescriptor(new_message_descriptor),
            &mut seen_descriptors,
        )
    }

    // Adapted from [1].
    //
    // [1]: https://github.com/FoundationDB/fdb-record-layer/blob/3.3.433.0/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/metadata/MetaDataEvolutionValidator.java#L218
    fn validate_message(
        OldMessageDescriptor(old_message_descriptor): OldMessageDescriptor,
        NewMessageDescriptor(new_message_descriptor): NewMessageDescriptor,
        seen_descriptors: &mut HashSet<(
            OldMessageDescriptorProtoBytes,
            NewMessageDescriptorProtoBytes,
        )>,
    ) -> bool {
        // In Java RecordLayer there is the following code.
        //
        // ```java
        // if (oldDescriptor == newDescriptor) {
        //     // Don't bother validating message types that are the same.
        //     return;
        // }
        // ```
        //
        // In our case, we do not have a `Hash` trait on
        // `MessageDescriptor` and `PartiqlEq` is derived. It is not
        // entirely clear if we did the following, we will get the
        // required benefit as we could end up doing a deep
        // comparison. For now we are not implementing this logic, and
        // just letting `MessageDescriptor` comparisons happen using
        // `FieldDescriptor`.
        //
        // ```rust
        // if old_message_descriptor == new_message_descriptor {
        //     // Don't bother validating message types that are the
        //     // same.
        //     return true;
        // }
        // ```

        // Do not allow `MessageDescriptor` to be renamed. They can be
        // in different packages.
        if old_message_descriptor.name() != new_message_descriptor.name() {
            return false;
        }

        let old_message_descriptor_proto_bytes = OldMessageDescriptorProtoBytes(
            old_message_descriptor.descriptor_proto().encode_to_vec(),
        );
        let new_message_descriptor_proto_bytes = NewMessageDescriptorProtoBytes(
            new_message_descriptor.descriptor_proto().encode_to_vec(),
        );

        if !seen_descriptors.insert((
            old_message_descriptor_proto_bytes,
            new_message_descriptor_proto_bytes,
        )) {
            // Note that because messages can contain fields that are
            // of the same type as the containing message, if this
            // check to make sure the pair hadn't already been
            // validated weren't present, this might recurse
            // infinitely on some inputs.
            return true;
        }

        // We do not have to validate `proto2` or `proto3` because we
        // only support `proto3`. A value of
        // `WellFormedMessageDescriptor` type would not be `proto3`.

        // Validate that every field in the old descriptor is still in
        // the new descriptor.
        for old_field_descriptor in old_message_descriptor.fields() {
            if let Some(new_field_descriptor) =
                new_message_descriptor.get_field(old_field_descriptor.number())
            {
                if !Self::validate_field(
                    OldFieldDescriptor(old_field_descriptor),
                    NewFieldDescriptor(new_field_descriptor),
                    seen_descriptors,
                ) {
                    return false;
                }
            } else {
                return false;
            }
        }

        // Since, we only support `proto3`, we also do not need to
        // check if new fields are marked `required`.
        true
    }

    // Adapted from [1].
    //
    // [1]: https://github.com/FoundationDB/fdb-record-layer/blob/3.3.433.0/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/metadata/MetaDataEvolutionValidator.java#L254
    fn validate_field(
        OldFieldDescriptor(old_field_descriptor): OldFieldDescriptor,
        NewFieldDescriptor(new_field_descriptor): NewFieldDescriptor,
        seen_descriptors: &mut HashSet<(
            OldMessageDescriptorProtoBytes,
            NewMessageDescriptorProtoBytes,
        )>,
    ) -> bool {
        // We do not allow field renaming.
        if old_field_descriptor.name() != new_field_descriptor.name() {
            return false;
        }

        match old_field_descriptor.cardinality() {
            t @ (Cardinality::Optional | Cardinality::Repeated) => {
                if t != new_field_descriptor.cardinality() {
                    return false;
                }
            }
            Cardinality::Required => {
                // We should not be seeing `Required`.
                return false;
            }
        }

        let new_field_descriptor_kind = new_field_descriptor.kind();

        // Match all kinds explicitly so that in the
        // unlikely event a new kind gets introduced in
        // the future, we do not miss it.
        match old_field_descriptor.kind() {
            // We are not suppose to see these types.
            Kind::Uint32 | Kind::Uint64 | Kind::Fixed32 | Kind::Fixed64 => false,
            t @ (Kind::Double
            | Kind::Float
            | Kind::Int32
            | Kind::Int64
            | Kind::Sint32
            | Kind::Sint64
            | Kind::Sfixed32
            | Kind::Sfixed64
            | Kind::Bool
            | Kind::String
            | Kind::Bytes) => {
                if t == new_field_descriptor_kind {
                    true
                } else {
                    false
                }
            }
            Kind::Message(old_inner_message_descriptor) => {
                if let Kind::Message(new_inner_message_descriptor) = new_field_descriptor_kind {
                    if FDB_RL_WKT_FULL_NAME
                        .deref()
                        .contains(&old_inner_message_descriptor.full_name().to_string())
                    {
                        // It looks like we have FDB Record Layer well
                        // know type. FDB well known types cannot be
                        // evolved. So, we check if their full names
                        // match. If it does, then it is
                        // okay. Otherwise it is an error.
                        if old_inner_message_descriptor.full_name()
                            == new_inner_message_descriptor.full_name()
                        {
                            true
                        } else {
                            false
                        }
                    } else {
                        Self::validate_message(
                            OldMessageDescriptor(old_inner_message_descriptor),
                            NewMessageDescriptor(new_inner_message_descriptor),
                            seen_descriptors,
                        )
                    }
                } else {
                    false
                }
            }
            Kind::Enum(old_inner_enum_descriptor) => {
                if let Kind::Enum(new_inner_enum_descriptor) = new_field_descriptor_kind {
                    Self::validate_enum(
                        OldEnumDescriptor(old_inner_enum_descriptor),
                        NewEnumDescriptor(new_inner_enum_descriptor),
                    )
                } else {
                    false
                }
            }
        }
    }

    // Adapted from [1].
    //
    // [1]: https://github.com/FoundationDB/fdb-record-layer/blob/3.3.433.0/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/metadata/MetaDataEvolutionValidator.java#L299
    fn validate_enum(
        OldEnumDescriptor(old_enum_descriptor): OldEnumDescriptor,
        NewEnumDescriptor(new_enum_descriptor): NewEnumDescriptor,
    ) -> bool {
        // Do not allow `EnumDescriptor` to be renamed. They can be in
        // different packages.
        if old_enum_descriptor.name() != new_enum_descriptor.name() {
            return false;
        }

        for old_enum_value_descriptor in old_enum_descriptor.values() {
            if let Some(new_enum_value_descriptor) =
                new_enum_descriptor.get_value(old_enum_value_descriptor.number())
            {
                // Ensure that short names are the same.
                if old_enum_value_descriptor.name() != new_enum_value_descriptor.name() {
                    return false;
                }
            } else {
                return false;
            }
        }

        true
    }
}

impl From<WellFormedMessageDescriptor> for MessageDescriptor {
    fn from(well_formed_message_descriptor: WellFormedMessageDescriptor) -> MessageDescriptor {
        let WellFormedMessageDescriptor { inner } = well_formed_message_descriptor;

        inner
    }
}

impl TryFrom<MessageDescriptor> for WellFormedMessageDescriptor {
    type Error = FdbError;

    fn try_from(message_descriptor: MessageDescriptor) -> FdbResult<WellFormedMessageDescriptor> {
        // Before walking the message descriptor, make sure that the
        // message descriptor is not a Protobuf generated map
        // entry. While the message descriptor can contain map fields,
        // if the user is trying to use Protobuf generated map entry
        // as a record, then something is amiss.
        if message_descriptor.is_map_entry() {
            return Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE_DESCRIPTOR));
        }

        let mut message_descriptor_validator_visitor = MessageDescriptorValidatorVisitor::new();

        if walk_message_descriptor(
            &mut message_descriptor_validator_visitor,
            &message_descriptor,
        ) {
            Ok(WellFormedMessageDescriptor {
                inner: message_descriptor,
            })
        } else {
            Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE_DESCRIPTOR))
        }
    }
}

trait Visitor {
    fn check_fdb_wkt(&self, message_descriptor: &MessageDescriptor) -> bool;

    fn previously_walked_check_or_update(&mut self, message_descriptor: &MessageDescriptor)
        -> bool;

    fn visit_parent_file_descriptor(&self, file_descriptor: FileDescriptor) -> bool;

    fn visit_field_descriptor(&mut self, field_descriptor: FieldDescriptor) -> bool;

    fn visit_map_entry_key_type_field_descriptor(&self, field_descriptor: FieldDescriptor) -> bool;

    fn visit_map_entry_value_type_field_descriptor(
        &mut self,
        field_descriptor: FieldDescriptor,
    ) -> bool;
}

fn walk_message_descriptor(
    visitor: &mut dyn Visitor,
    message_descriptor: &MessageDescriptor,
) -> bool {
    if visitor.previously_walked_check_or_update(message_descriptor) {
        // Have we seen this message descriptor before? If so, it is a
        // recursive message. Just return `true` here, and let message
        // validation happen at the place where it was first seen.
        true
    } else {
        // If message descriptor is a FDB Record Layer well known
        // type, there is nothing to be done. We return `true`.
        if visitor.check_fdb_wkt(message_descriptor) {
            return true;
        }

        // To understand the motivation for the checks we perform
        // below, see the documentation for type
        // `MessageDescriptorValidatorVisitor`.
        //
        // We just visit various parts of the message descriptor
        // below. The actual logic is implemented inside the visitor.

        if !visitor.visit_parent_file_descriptor(message_descriptor.parent_file()) {
            return false;
        }

        if message_descriptor.is_map_entry() {
            // Ensure that there are no unexpected field
            // descriptors. We should only see two field descriptors,
            // one for `key` another for `value`.
            if message_descriptor.fields().len() != 2 {
                return false;
            }

            let res = message_descriptor
                .get_field_by_name("key")
                .and_then(|field_descriptor| {
                    if visitor.visit_map_entry_key_type_field_descriptor(field_descriptor) {
                        Some(())
                    } else {
                        None
                    }
                })
                .and_then(|_| message_descriptor.get_field_by_name("value"))
                .and_then(|field_descriptor| {
                    if visitor.visit_map_entry_value_type_field_descriptor(field_descriptor) {
                        Some(())
                    } else {
                        None
                    }
                });

            match res {
                Some(()) => true,
                None => false,
            }
        } else {
            for field_descriptor in message_descriptor.fields() {
                if !visitor.visit_field_descriptor(field_descriptor) {
                    return false;
                }
            }
            true
        }
    }
}

/// There are five valid forms of field descriptor that we want. They
/// are numbered (1), (2), (a), (b.i), (b.ii). We also follow Java
/// RecordLayer restrictions on unsigned types.
///
/// The form (a) is a special case of form (2).
///
/// Forms (1) and (2) describe most common field descriptor forms that
/// we want, which are fields containing `optional` and `repeated`.
///
/// 1. `cardinality: Optional`, `supports_presence: true`, `is_list:
///    false`, `is_map: false`, `default_value: None`,
///    `containing_oneof: Some("...")`, where `...` is the name of the
///    oneof. `...` can be explicitly provided when `oneof` keyword is
///    used. Alternatively, when `optional` is specified, it is
///    generated by the protobuf compiler.
///
/// 2. `cardinality: Repeated`, `supports_presence: false`,
///    `default_value: None`, `containing_oneof: None`. When `is_list:
///    true` then the field is a `repeated`. When `is_map: true`, the
///    field is a `map` (with a compiler generated map entry message
///    type).
///
///    In proto3, there is no concept of `optional repeated` [1] or
///    `optional map`.
///
/// When `cardinality: Optional`, `support_presence: false` and
/// `is_list: false`, and `is_map: false` that is a round about way of
/// saying "required" with default value. We do not allow that for
/// form (1) described above.
///
/// The only way to specify that a field is "required" is by using key
/// expression.
///
/// For all valid forms (i.e., form (1) and (2) described above and
/// `map` form described below) and we outright reject `cardinality:
/// Required`. If we see `cardinality: Required`, there is some
/// serious bug, as we are checking for proto3 from file descriptor.
///
/// For all valid forms, we reject `is_group: true`, `default_value:
/// Some(...)`. There is no API from `FileDescriptor` type to check
/// `default_value`. Therefore we need to check using
/// `FileDescriptorProto` type. Hence you will see the following code.
///
/// ```
/// field_descriptor
///   .field_descriptor_proto()
///   .default_value
///   .is_some()
/// ```
///
/// Protobuf packing can vary depending on the type. So, we do not
/// check `is_packed`.
///
/// Protobuf message can have no fields. Together with `oneof` this is
/// very useful in building up discriminated unions (tagged union, sum
/// types). We use this technique in `cursor.proto` in
/// `KeyValueContinuation` message.
///
/// We support `oneof` keyword. Oneof fields are just fields in the
/// enclosing message descriptor [2] and they have `containing_oneof:
/// Some("...")`, where `...` specified in `.proto` file rather than
/// being compiler generated. Fields that are part of `oneof` keyword
/// have `supports_presence: true`, `cardinality: Optional`, which is
/// form (1) described above.
///
/// Additionally, within oneof, there cannot be a `repeated` field
/// [3]. This ensures that we won't see an empty `repeated` field
/// within oneof.
///
/// Following message type gives us the correct field descriptor for
/// form (1).
///
/// ```
/// message HelloWorld {
///   optional fdb_rl.field.v1.UUID primary_key = 1;
///   optional string hello = 2;
///   optional string world = 3;
/// }
/// ```
///
/// If we were to remove the `optional` keyword, for example:
///
/// ```
/// message HelloWorld {
///   fdb_rl.field.v1.UUID non_optional_primary_key = 4;
///   string non_optional_string = 5;
/// }
/// ```
///
/// Then the field `non_optional_primary_key` would have
/// `containing_oneof: None`. The field `non_optional_string` would
/// have `containing_oneof: None` *and* `supports_presence: false`.
///
/// The logic for checking form (1) and (2) is implemented in
/// `visit_field_descriptor`.
///
/// Protobuf map type is a special case. The `map` field is
/// implemented using with a protobuf generated message type. We will
/// refer to the generated protobuf message type as
/// `GeneratedMapEntryMessage`.
///
/// In case of `map` fields, there are three field descriptors to be
/// aware of.
///
/// a. The field descriptor where `map` field is defined. This field
///    descriptor will have the kind of protobuf generated message
///    type (i.e., `GeneratedMapEntryMessage`)
///
/// b. Within `GeneratedMapEntryMessage`, there would be two field
///    descriptors whose names are:
///    i.  `key`
///    ii. `value`
///
/// The `map` field's field descriptor (i.e., (a) above) will have
/// `cardinality: Repeated`, `supports_presence: false`,
/// `containing_oneof: None`. It will have `is_map: true`. This
/// scenario is taken care of by (2) above in
/// `visit_field_descriptor`.
///
/// The `GeneratedMapEntryMessage` message descriptor has
/// `is_map_entry: true`. It has two field descriptors with name `key`
/// (`key_type`) and `value` (`value_type`). How we walk a message
/// descriptor depends on if `is_map_entry` is `true` or `false`. This
/// is handled in `walk_message_descriptor`.
///
/// The field descriptor with name `key` ((b.i) above) has
/// `cardinality: Optional`, `supports_presence: false`, `is_list:
/// false`, `is_map: false`, `default_value: None`, `containing_oneof:
/// None`.
///
/// The field descriptor with name `value` ((b.ii) above) has
/// `cardinality: Optional`, `is_list: false`, `is_map: false`,
/// `default_value: None`, `containing_oneof: None`. *Note:*
/// `support_presence` would be `false` if the `value_type` is scalar
/// and it would be `true` if the `value_type` is a `message`.
///
/// Protobuf allows `key_type` to be any integral or string type
/// [4]. We however want to limit `key_type` to be a `string`. The
/// motivation for this is when necessary we want the key value to be
/// PartiQL tuple attribute, which is a `string`.
///
/// The logic for checking field descriptor with name `key` ((b.i)
/// above) is implemented in
/// `visit_map_entry_key_type_field_descriptor` and the logic for
/// checking field descriptor with name `value` ((b.ii) above) is
/// implemented in `visit_map_entry_value_type_field_descriptor`.
///
/// We do not allow field names to begin with `fdb_`. This is because
/// field names starting with `fdb_` is used to represent metadata
/// when converting to PartiQL.
///
/// Unsigned types (`uint32`, `uint64`, `fixed32` and `fixed64`) are
/// invalid [5].
///
/// [1]: https://github.com/protocolbuffers/protobuf/issues/10489
/// [2]: https://protobuf.com/docs/language-spec#fully-qualified-names
/// [3]: https://protobuf.dev/programming-guides/proto3/#using-oneof
/// [4]: https://protobuf.dev/programming-guides/proto3/#maps
/// [5]: https://github.com/FoundationDB/fdb-record-layer/blob/3.2.283.0/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/RecordMetaDataBuilder.java#L603-L610
#[derive(Debug)]
struct MessageDescriptorValidatorVisitor {
    walked_message_descriptor: Vec<MessageDescriptor>,
}

impl MessageDescriptorValidatorVisitor {
    fn new() -> MessageDescriptorValidatorVisitor {
        MessageDescriptorValidatorVisitor {
            walked_message_descriptor: Vec::new(),
        }
    }
}

impl Visitor for MessageDescriptorValidatorVisitor {
    /// Checks if the message descriptor is a FDB Record Layer well
    /// known type.
    fn check_fdb_wkt(&self, message_descriptor: &MessageDescriptor) -> bool {
        FDB_RL_WKT_FULL_NAME
            .deref()
            .contains(&message_descriptor.full_name().to_string())
    }

    /// Checks if we have seen the message descriptor before. If not,
    /// we update that we have seen it and returns `false`. Therefore,
    /// next time we are called, we can return `true`.
    fn previously_walked_check_or_update(
        &mut self,
        message_descriptor: &MessageDescriptor,
    ) -> bool {
        if self.walked_message_descriptor.contains(message_descriptor) {
            true
        } else {
            self.walked_message_descriptor
                .push(message_descriptor.clone());
            false
        }
    }

    /// Returns `true` if `.proto` file was compiled using `syntax =
    /// proto3`.
    fn visit_parent_file_descriptor(&self, file_descriptor: FileDescriptor) -> bool {
        matches!(file_descriptor.syntax(), Syntax::Proto3)
    }

    /// Returns `true` if the field descriptor is considered
    /// "valid".
    ///
    /// See documentation on type
    /// [`MessageDescriptorValidatorVisitor`] for details.
    fn visit_field_descriptor(&mut self, field_descriptor: FieldDescriptor) -> bool {
        if field_descriptor.is_group()
            || field_descriptor
                .field_descriptor_proto()
                .default_value
                .is_some()
        {
            return false;
        }

        if field_descriptor.name().starts_with("fdb_") {
            return false;
        }

        match field_descriptor.cardinality() {
            Cardinality::Optional => {
                if field_descriptor.supports_presence()
                    && !field_descriptor.is_list()
                    && !field_descriptor.is_map()
                    && field_descriptor.containing_oneof().is_some()
                {
                    // Match all kinds explicitly so that in the
                    // unlikely event a new kind gets introduced in
                    // the future, we do not miss it.
                    match field_descriptor.kind() {
                        Kind::Message(inner_message_descriptor) => {
                            walk_message_descriptor(self, &inner_message_descriptor)
                        }
                        Kind::Uint32 | Kind::Uint64 | Kind::Fixed32 | Kind::Fixed64 => false,
                        Kind::Double
                        | Kind::Float
                        | Kind::Int32
                        | Kind::Int64
                        | Kind::Sint32
                        | Kind::Sint64
                        | Kind::Sfixed32
                        | Kind::Sfixed64
                        | Kind::Bool
                        | Kind::String
                        | Kind::Bytes
                        | Kind::Enum(_) => true,
                    }
                } else {
                    false
                }
            }
            Cardinality::Required => false,
            Cardinality::Repeated => {
                if !field_descriptor.supports_presence()
                    && ((field_descriptor.is_list() && !field_descriptor.is_map())
                        || (!field_descriptor.is_list() && field_descriptor.is_map()))
                    && field_descriptor.containing_oneof().is_none()
                {
                    // Match all kinds explicitly so that in the
                    // unlikely event a new kind gets introduced in
                    // the future, we do not miss it.
                    match field_descriptor.kind() {
                        Kind::Message(inner_message_descriptor) => {
                            walk_message_descriptor(self, &inner_message_descriptor)
                        }
                        Kind::Uint32 | Kind::Uint64 | Kind::Fixed32 | Kind::Fixed64 => false,
                        Kind::Double
                        | Kind::Float
                        | Kind::Int32
                        | Kind::Int64
                        | Kind::Sint32
                        | Kind::Sint64
                        | Kind::Sfixed32
                        | Kind::Sfixed64
                        | Kind::Bool
                        | Kind::String
                        | Kind::Bytes
                        | Kind::Enum(_) => true,
                    }
                } else {
                    false
                }
            }
        }
    }

    /// Returns `true` if the field descriptor is considered
    /// "valid".
    ///
    /// See documentation on type
    /// [`MessageDescriptorValidatorVisitor`] for details.
    fn visit_map_entry_key_type_field_descriptor(&self, field_descriptor: FieldDescriptor) -> bool {
        if field_descriptor.is_group()
            || field_descriptor
                .field_descriptor_proto()
                .default_value
                .is_some()
        {
            return false;
        }

        match field_descriptor.cardinality() {
            Cardinality::Optional => {
                if !field_descriptor.supports_presence()
                    && !field_descriptor.is_list()
                    && !field_descriptor.is_map()
                    && field_descriptor.containing_oneof().is_none()
                {
                    // Match all kinds explicitly so that in the
                    // unlikely event a new kind gets introduced in
                    // the future, we do not miss it.
                    match field_descriptor.kind() {
                        Kind::String => true,
                        Kind::Double
                        | Kind::Float
                        | Kind::Int32
                        | Kind::Int64
                        | Kind::Uint32
                        | Kind::Uint64
                        | Kind::Sint32
                        | Kind::Sint64
                        | Kind::Fixed32
                        | Kind::Fixed64
                        | Kind::Sfixed32
                        | Kind::Sfixed64
                        | Kind::Bool
                        | Kind::Bytes
                        | Kind::Message(_)
                        | Kind::Enum(_) => false,
                    }
                } else {
                    false
                }
            }
            Cardinality::Required | Cardinality::Repeated => false,
        }
    }

    /// Returns `true` if the field descriptor is considered
    /// "valid".
    ///
    /// See documentation on type
    /// [`MessageDescriptorValidatorVisitor`] for details.
    fn visit_map_entry_value_type_field_descriptor(
        &mut self,
        field_descriptor: FieldDescriptor,
    ) -> bool {
        if field_descriptor.is_group()
            || field_descriptor
                .field_descriptor_proto()
                .default_value
                .is_some()
        {
            return false;
        }

        match field_descriptor.cardinality() {
            Cardinality::Optional => {
                // We do not check `supports_presence` here.
                // `support_presence` would be `false` if the
                // `value_type` is scalar and it would be `true` if
                // the `value_type` is a `message`.
                if !field_descriptor.is_list()
                    && !field_descriptor.is_map()
                    && field_descriptor.containing_oneof().is_none()
                {
                    // Match all kinds explicitly so that in the
                    // unlikely event a new kind gets introduced in
                    // the future, we do not miss it.
                    match field_descriptor.kind() {
                        Kind::Message(inner_message_descriptor) => {
                            // Before walking the inner message
                            // descriptor, make sure that it is not a
                            // Protobuf generated map entry.
                            if inner_message_descriptor.is_map_entry() {
                                false
                            } else {
                                walk_message_descriptor(self, &inner_message_descriptor)
                            }
                        }
                        Kind::Uint32 | Kind::Uint64 | Kind::Fixed32 | Kind::Fixed64 => false,
                        Kind::Double
                        | Kind::Float
                        | Kind::Int32
                        | Kind::Int64
                        | Kind::Sint32
                        | Kind::Sint64
                        | Kind::Sfixed32
                        | Kind::Sfixed64
                        | Kind::Bool
                        | Kind::String
                        | Kind::Bytes
                        | Kind::Enum(_) => true,
                    }
                } else {
                    false
                }
            }
            Cardinality::Required | Cardinality::Repeated => false,
        }
    }
}

#[cfg(test)]
mod tests {
    mod well_formed_message_descriptor {
        use fdb::error::FdbError;
        use prost_reflect::ReflectMessage;

        use std::convert::TryFrom;

        use super::super::super::error::PROTOBUF_ILL_FORMED_MESSAGE_DESCRIPTOR;
        use super::super::WellFormedMessageDescriptor;

        #[test]
        fn is_evolvable_to() {
            // Tests adapted from Java RecordLayer is reasonably
            // exhaustive. So, our tests only test well known type
            // evolution.

            // Evolution when a well known type is present (in this
            // case a UUID).
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_message_descriptor::evolution::v1::HelloWorld as OldHelloWorld;
		use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_message_descriptor::evolution::v2::HelloWorld as NewHelloWorld;

                let old_well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(OldHelloWorld::default().descriptor())
                        .unwrap();
                let new_well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(NewHelloWorld::default().descriptor())
                        .unwrap();

                assert!(old_well_formed_message_descriptor
                    .is_evolvable_to(new_well_formed_message_descriptor));
            }

            // Evolution when well known type is replaced with a fake
            // one.
            {
                use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_message_descriptor::evolution::v1::HelloWorld as OldHelloWorld;
		use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_message_descriptor::evolution::v3::HelloWorld as NewHelloWorld;

                let old_well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(OldHelloWorld::default().descriptor())
                        .unwrap();
                let new_well_formed_message_descriptor =
                    WellFormedMessageDescriptor::try_from(NewHelloWorld::default().descriptor())
                        .unwrap();

                assert!(!old_well_formed_message_descriptor
                    .is_evolvable_to(new_well_formed_message_descriptor));
            }

            // Java
            {
                // `dropField()`
                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_1::v1::MySimpleRecord as OldMySimpleRecord;
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_1::v2::MySimpleRecord as NewMySimpleRecord;

                    let old_well_formed_message_descriptor = WellFormedMessageDescriptor::try_from(
                        OldMySimpleRecord::default().descriptor(),
                    )
                    .unwrap();
                    let new_well_formed_message_descriptor = WellFormedMessageDescriptor::try_from(
                        NewMySimpleRecord::default().descriptor(),
                    )
                    .unwrap();

                    assert!(!old_well_formed_message_descriptor
                        .is_evolvable_to(new_well_formed_message_descriptor));
                }
                // `renameField()`
                {
                    {
                        use fdb_rl_proto::fdb_rl_test::java::proto::test_records_1::v1::MySimpleRecord as OldMySimpleRecord;
                        use fdb_rl_proto::fdb_rl_test::java::proto::test_records_1::v3::MySimpleRecord as NewMySimpleRecord;

                        let old_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                OldMySimpleRecord::default().descriptor(),
                            )
                            .unwrap();
                        let new_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                NewMySimpleRecord::default().descriptor(),
                            )
                            .unwrap();

                        assert!(!old_well_formed_message_descriptor
                            .is_evolvable_to(new_well_formed_message_descriptor));
                    }
                    {
                        use fdb_rl_proto::fdb_rl_test::java::proto::test_records_1::v1::MySimpleRecord as OldMySimpleRecord;
                        use fdb_rl_proto::fdb_rl_test::java::proto::test_records_1::v4::MySimpleRecord as NewMySimpleRecord;

                        let old_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                OldMySimpleRecord::default().descriptor(),
                            )
                            .unwrap();
                        let new_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                NewMySimpleRecord::default().descriptor(),
                            )
                            .unwrap();

                        assert!(!old_well_formed_message_descriptor
                            .is_evolvable_to(new_well_formed_message_descriptor));
                    }
                }
                // `fieldTypeChanged()`
                {
                    {
                        use fdb_rl_proto::fdb_rl_test::java::proto::test_records_1::v1::MySimpleRecord as OldMySimpleRecord;
                        use fdb_rl_proto::fdb_rl_test::java::proto::test_records_1::v5::MySimpleRecord as NewMySimpleRecord;

                        let old_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                OldMySimpleRecord::default().descriptor(),
                            )
                            .unwrap();
                        let new_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                NewMySimpleRecord::default().descriptor(),
                            )
                            .unwrap();

                        assert!(!old_well_formed_message_descriptor
                            .is_evolvable_to(new_well_formed_message_descriptor));
                    }
                    // Unlike Java RecordLayer, we do not allow `int32
                    // -> int64` conversion. Skipping tests for
                    // `sint32 -> sint64`, `sfixed32 -> sfixed64` as
                    // the code path is the same.
                    {
                        use fdb_rl_proto::fdb_rl_test::java::proto::test_records_1::v1::MySimpleRecord as OldMySimpleRecord;
                        use fdb_rl_proto::fdb_rl_test::java::proto::test_records_1::v6::MySimpleRecord as NewMySimpleRecord;

                        let old_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                OldMySimpleRecord::default().descriptor(),
                            )
                            .unwrap();
                        let new_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                NewMySimpleRecord::default().descriptor(),
                            )
                            .unwrap();

                        assert!(!old_well_formed_message_descriptor
                            .is_evolvable_to(new_well_formed_message_descriptor));
                    }
                }
                // There are not tests from
                // `fieldChangedFromMessageToGroup()` as we do not
                // have group in `proto3`.
                //
                // `enumFieldChanged()`
                {
                    {
                        use fdb_rl_proto::fdb_rl_test::java::proto::test_records_enum::v1::MyShapeRecord as OldMyShapeRecord;
                        use fdb_rl_proto::fdb_rl_test::java::proto::test_records_enum::v2::MyShapeRecord as NewMyShapeRecord;

                        let old_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                OldMyShapeRecord::default().descriptor(),
                            )
                            .unwrap();
                        let new_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                NewMyShapeRecord::default().descriptor(),
                            )
                            .unwrap();

                        assert!(old_well_formed_message_descriptor
                            .is_evolvable_to(new_well_formed_message_descriptor));
                    }
                    // The way to test dropping of enum field is to
                    // reverse the message descriptor in the above
                    // test. We would effectively be dropping
                    // `SIZE_X_LARGE` variant.
                    {
                        use fdb_rl_proto::fdb_rl_test::java::proto::test_records_enum::v1::MyShapeRecord as OldMyShapeRecord;
                        use fdb_rl_proto::fdb_rl_test::java::proto::test_records_enum::v2::MyShapeRecord as NewMyShapeRecord;

                        let old_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                OldMyShapeRecord::default().descriptor(),
                            )
                            .unwrap();
                        let new_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                NewMyShapeRecord::default().descriptor(),
                            )
                            .unwrap();

                        assert!(!new_well_formed_message_descriptor
                            .is_evolvable_to(old_well_formed_message_descriptor));
                    }
                    // Unlike Java RecordLayer, we do not allow
                    // renaming `enum`.
                    {
                        use fdb_rl_proto::fdb_rl_test::java::proto::test_records_enum::v1::MyShapeRecord as OldMyShapeRecord;
                        use fdb_rl_proto::fdb_rl_test::java::proto::test_records_enum::v3::MyShapeRecord as NewMyShapeRecord;

                        let old_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                OldMyShapeRecord::default().descriptor(),
                            )
                            .unwrap();
                        let new_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                NewMyShapeRecord::default().descriptor(),
                            )
                            .unwrap();

                        assert!(!old_well_formed_message_descriptor
                            .is_evolvable_to(new_well_formed_message_descriptor));
                    }
                }
                // `selfReferenceChanged()`
                {
                    {
                        use fdb_rl_proto::fdb_rl_test::java::proto::evolution::test_self_reference::v1::LinkedListRecord as OldLinkedListRecord;
			use fdb_rl_proto::fdb_rl_test::java::proto::evolution::test_self_reference_unspooled::v1::LinkedListRecord as NewLinkedListRecord;

                        let old_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                OldLinkedListRecord::default().descriptor(),
                            )
                            .unwrap();
                        let new_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                NewLinkedListRecord::default().descriptor(),
                            )
                            .unwrap();

                        assert!(!old_well_formed_message_descriptor
                            .is_evolvable_to(new_well_formed_message_descriptor));
                    }
                    {
                        use fdb_rl_proto::fdb_rl_test::java::proto::evolution::test_self_reference_unspooled::v1::LinkedListRecord as OldLinkedListRecord;
			use fdb_rl_proto::fdb_rl_test::java::proto::evolution::test_self_reference_unspooled::v2::LinkedListRecord as NewLinkedListRecord;

                        let old_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                OldLinkedListRecord::default().descriptor(),
                            )
                            .unwrap();
                        let new_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                NewLinkedListRecord::default().descriptor(),
                            )
                            .unwrap();

                        assert!(!old_well_formed_message_descriptor
                            .is_evolvable_to(new_well_formed_message_descriptor));
                    }
                }
                // Unlike Java RecordLayer, we do not allow renaming
                // of types.
                //
                // `nestedTypeChangesName()`
                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_with_header::v1::MyRecord as OldMyRecord;
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_with_header::v2::MyRecord as NewMyRecord;

                    let old_well_formed_message_descriptor =
                        WellFormedMessageDescriptor::try_from(OldMyRecord::default().descriptor())
                            .unwrap();
                    let new_well_formed_message_descriptor =
                        WellFormedMessageDescriptor::try_from(NewMyRecord::default().descriptor())
                            .unwrap();

                    assert!(!old_well_formed_message_descriptor
                        .is_evolvable_to(new_well_formed_message_descriptor));
                }
                // `nestedTypeChangesFieldName()
                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_with_header::v1::MyRecord as OldMyRecord;
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_with_header::v3::MyRecord as NewMyRecord;

                    let old_well_formed_message_descriptor =
                        WellFormedMessageDescriptor::try_from(OldMyRecord::default().descriptor())
                            .unwrap();
                    let new_well_formed_message_descriptor =
                        WellFormedMessageDescriptor::try_from(NewMyRecord::default().descriptor())
                            .unwrap();

                    assert!(!old_well_formed_message_descriptor
                        .is_evolvable_to(new_well_formed_message_descriptor));
                }
                // `nestedTypeChangesFieldType()`
                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_with_header::v1::MyRecord as OldMyRecord;
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_with_header::v4::MyRecord as NewMyRecord;

                    let old_well_formed_message_descriptor =
                        WellFormedMessageDescriptor::try_from(OldMyRecord::default().descriptor())
                            .unwrap();
                    let new_well_formed_message_descriptor =
                        WellFormedMessageDescriptor::try_from(NewMyRecord::default().descriptor())
                            .unwrap();

                    assert!(!old_well_formed_message_descriptor
                        .is_evolvable_to(new_well_formed_message_descriptor));
                }
                // nestedTypesMerged()
                {
                    // In Java RecordLayer,
                    // `TestUnmergedNestedTypesProto.getDescriptor()`
                    // is used. Here we are using merged nested type.
                    use fdb_rl_proto::fdb_rl_test::java::proto::evolution::test_merged_nested_types::v1::MyRecord as OldMyRecord;
		    use fdb_rl_proto::fdb_rl_test::java::proto::evolution::test_merged_nested_types::v2::MyRecord as NewMyRecord;

                    let old_well_formed_message_descriptor =
                        WellFormedMessageDescriptor::try_from(OldMyRecord::default().descriptor())
                            .unwrap();
                    let new_well_formed_message_descriptor =
                        WellFormedMessageDescriptor::try_from(NewMyRecord::default().descriptor())
                            .unwrap();

                    assert!(!old_well_formed_message_descriptor
                        .is_evolvable_to(new_well_formed_message_descriptor));
                }
                // nestedTypesSplit()
                {
                    {
                        // In Java RecordLayer supports evolution of
                        // `TestMergedNestedTypesProto.RecordTypeUnion.getDescriptor()`
                        // as old and
                        // `TestSplitNestedTypesProto.RecordTypeUnion.getDescriptor()`
                        // as new. We do not support that.
                        use fdb_rl_proto::fdb_rl_test::java::proto::evolution::test_merged_nested_types::v1::MyRecord as OldMyRecord;
			use fdb_rl_proto::fdb_rl_test::java::proto::evolution::test_split_nested_types::v1::MyRecord as NewMyRecord;

                        let old_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                OldMyRecord::default().descriptor(),
                            )
                            .unwrap();
                        let new_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                NewMyRecord::default().descriptor(),
                            )
                            .unwrap();

                        assert!(!old_well_formed_message_descriptor
                            .is_evolvable_to(new_well_formed_message_descriptor));
                    }
                    {
                        // In Java RecordLayer, the comparision is
                        // between
                        // `TestUnmergedNestedTypesProto.getDescriptor()`
                        // and
                        // `TestSplitNestedTypesProto.getDescriptor()`. In
                        // our case, we compare
                        // `TestSplitNestedTypesProto.getDescriptor()`
                        // and the mutated
                        // `TestSplitNestedTypesProto.getDescriptor()`.
                        use fdb_rl_proto::fdb_rl_test::java::proto::evolution::test_split_nested_types::v1::MyRecord as OldMyRecord;
			use fdb_rl_proto::fdb_rl_test::java::proto::evolution::test_split_nested_types::v2::MyRecord as NewMyRecord;

                        let old_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                OldMyRecord::default().descriptor(),
                            )
                            .unwrap();
                        let new_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                NewMyRecord::default().descriptor(),
                            )
                            .unwrap();

                        assert!(!old_well_formed_message_descriptor
                            .is_evolvable_to(new_well_formed_message_descriptor));
                    }
                }
                // `fieldLabelChanged()`
                {
                    {
                        use fdb_rl_proto::fdb_rl_test::java::proto::test_records_1::v1::MySimpleRecord as OldMySimpleRecord;
                        use fdb_rl_proto::fdb_rl_test::java::proto::test_records_1::v7::MySimpleRecord as NewMySimpleRecord;

                        let old_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                OldMySimpleRecord::default().descriptor(),
                            )
                            .unwrap();
                        let new_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                NewMySimpleRecord::default().descriptor(),
                            )
                            .unwrap();

                        assert!(!old_well_formed_message_descriptor
                            .is_evolvable_to(new_well_formed_message_descriptor));
                    }
                    {
                        use fdb_rl_proto::fdb_rl_test::java::proto::test_records_1::v1::MySimpleRecord as OldMySimpleRecord;
                        use fdb_rl_proto::fdb_rl_test::java::proto::test_records_1::v8::MySimpleRecord as NewMySimpleRecord;

                        let old_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                OldMySimpleRecord::default().descriptor(),
                            )
                            .unwrap();
                        let new_well_formed_message_descriptor =
                            WellFormedMessageDescriptor::try_from(
                                NewMySimpleRecord::default().descriptor(),
                            )
                            .unwrap();

                        assert!(!old_well_formed_message_descriptor
                            .is_evolvable_to(new_well_formed_message_descriptor));
                    }
                }
                // Java RecordLayer `addRequiredField()` test does not
                // make sense for us as we do not support
                // `proto2`. However, it should be possible to add an
                // optional field without any issue.
                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_1::v1::MySimpleRecord as OldMySimpleRecord;
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_1::v9::MySimpleRecord as NewMySimpleRecord;

                    let old_well_formed_message_descriptor = WellFormedMessageDescriptor::try_from(
                        OldMySimpleRecord::default().descriptor(),
                    )
                    .unwrap();
                    let new_well_formed_message_descriptor = WellFormedMessageDescriptor::try_from(
                        NewMySimpleRecord::default().descriptor(),
                    )
                    .unwrap();

                    assert!(old_well_formed_message_descriptor
                        .is_evolvable_to(new_well_formed_message_descriptor));
                }
            }
        }

        #[test]
        fn try_from_message_descriptor_try_from() {
            // Invalid message descriptor
            {
                {
                    use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_message_descriptor::bad::proto_2::v1::HelloWorld;

                    let message_descriptor = HelloWorld::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor),
                        Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE_DESCRIPTOR))
                    )
                }

                {
                    use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_message_descriptor::bad::proto_3::v1::{RecursiveInner, RecursiveOuter, GeneratedMapEntry, InvalidMap, UnsignedRecordUint32, UnsignedRecordRepeatedUint32, UnsignedRecordUint64, UnsignedRecordRepeatedUint64, UnsignedRecordFixed32, UnsignedRecordRepeatedFixed32, UnsignedRecordFixed64, UnsignedRecordRepeatedFixed64, InvalidFieldName, InvalidMapUnsignedRecordUint32, InvalidMapUnsignedRecordUint64, InvalidMapUnsignedRecordFixed32, InvalidMapUnsignedRecordFixed64};

                    for message_descriptor in vec![
                        InvalidMap::default().descriptor(),
                        RecursiveInner::default().descriptor(),
                        RecursiveOuter::default().descriptor(),
                        UnsignedRecordUint32::default().descriptor(),
                        UnsignedRecordRepeatedUint32::default().descriptor(),
                        UnsignedRecordUint64::default().descriptor(),
                        UnsignedRecordRepeatedUint64::default().descriptor(),
                        UnsignedRecordFixed32::default().descriptor(),
                        UnsignedRecordRepeatedFixed32::default().descriptor(),
                        UnsignedRecordFixed64::default().descriptor(),
                        UnsignedRecordRepeatedFixed64::default().descriptor(),
                        InvalidFieldName::default().descriptor(),
                        InvalidMapUnsignedRecordUint32::default().descriptor(),
                        InvalidMapUnsignedRecordUint64::default().descriptor(),
                        InvalidMapUnsignedRecordFixed32::default().descriptor(),
                        InvalidMapUnsignedRecordFixed64::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor),
                            Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE_DESCRIPTOR))
                        );
                    }

                    // Protobuf generated map entry message.
                    let message_descriptor = GeneratedMapEntry::default()
			.descriptor()
			.parent_pool()
			.get_message_by_name("fdb_rl_test.protobuf.well_formed_message_descriptor.bad.proto_3.v1.GeneratedMapEntry.HelloWorldEntry")
			.unwrap();

                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor),
                        Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE_DESCRIPTOR))
                    );
                }

                // Java RecordLayer `proto`
                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_unsigned_1::v1::SimpleUnsignedRecord;

                    let message_descriptor = SimpleUnsignedRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor),
                        Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE_DESCRIPTOR))
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_unsigned_2::v1::{
                        NestedWithUnsigned, UnsignedInNestedRecord,
                    };

                    for message_descriptor in vec![
                        UnsignedInNestedRecord::default().descriptor(),
                        NestedWithUnsigned::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE_DESCRIPTOR))
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_unsigned_3::v1::Fixed32UnsignedRecord;

                    let message_descriptor = Fixed32UnsignedRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE_DESCRIPTOR))
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_unsigned_4::v1::Fixed64UnsignedRecord;

                    let message_descriptor = Fixed64UnsignedRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE_DESCRIPTOR))
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_unsigned_5::v1::ReferencesUnsignedRecord;

                    let message_descriptor = ReferencesUnsignedRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE_DESCRIPTOR))
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto3::evolution::test_nested_proto3::v1::NestedRecord;

                    let message_descriptor = NestedRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor),
                        Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE_DESCRIPTOR))
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto3::evolution::test_records_3_proto3::v1::MyHierarchicalRecord;

                    let message_descriptor = MyHierarchicalRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor),
                        Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE_DESCRIPTOR))
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto3::evolution::test_records_enum_proto3::v1::MyShapeRecord;

                    let message_descriptor = MyShapeRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor),
                        Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE_DESCRIPTOR))
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto3::evolution::test_records_nested_proto2::v1::MyRecord;

                    let message_descriptor = MyRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor),
                        Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE_DESCRIPTOR))
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto3::evolution::test_records_nested_proto3::v1::MyRecord;

                    let message_descriptor = MyRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor),
                        Err(FdbError::new(PROTOBUF_ILL_FORMED_MESSAGE_DESCRIPTOR))
                    );
                }
            }

            // Valid Message descriptor
            {
                {
                    use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_message_descriptor::good::v1::{HelloWorld, RecursiveInner, RecursiveOuter, HelloWorldOneof, HelloWorldMap};

                    for message_descriptor in vec![
                        HelloWorld::default().descriptor(),
                        RecursiveInner::default().descriptor(),
                        RecursiveOuter::default().descriptor(),
                        HelloWorldOneof::default().descriptor(),
                        HelloWorldMap::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                // Java RecordLayer `proto`
                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::evolution::test_field_type_change::v1::MySimpleRecord;

                    let message_descriptor = MySimpleRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::evolution::test_merged_nested_types::v1::{OneTrueNested, MyRecord};

                    for message_descriptor in vec![
                        OneTrueNested::default().descriptor(),
                        MyRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::evolution::test_new_record_type::v1::NewRecord;

                    let message_descriptor = NewRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::evolution::test_self_reference::v1::LinkedListRecord;

                    let message_descriptor = LinkedListRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::evolution::test_self_reference_unspooled::v1::{Node, LinkedListRecord};

                    for message_descriptor in vec![
                        Node::default().descriptor(),
                        LinkedListRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::evolution::test_split_nested_types::v1::{NestedA, NestedB, MyRecord};

                    for message_descriptor in vec![
                        NestedA::default().descriptor(),
                        NestedB::default().descriptor(),
                        MyRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::evolution::test_unmerged_nested_types::v1::{NestedA, NestedB, MyRecord};

                    for message_descriptor in vec![
                        NestedA::default().descriptor(),
                        NestedB::default().descriptor(),
                        MyRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::expression_tests::v1::{
                        Customer, NestedField, SubString, SubStrings, TestScalarFieldAccess,
                    };

                    for message_descriptor in vec![
                        TestScalarFieldAccess::default().descriptor(),
                        NestedField::default().descriptor(),
                        SubString::default().descriptor(),
                        SubStrings::default().descriptor(),
                        Customer::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_no_indexes::v1::MySimpleRecord;

                    let message_descriptor = MySimpleRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_no_union::v1::MySimpleRecord;

                    let message_descriptor = MySimpleRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_no_union_evolved::v1::{
                        MyOtherRecord, MySimpleRecord,
                    };

                    for message_descriptor in vec![
                        MyOtherRecord::default().descriptor(),
                        MySimpleRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_no_union_evolved_illegal::v1::MySimpleRecord;

                    let message_descriptor = MySimpleRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_no_union_evolved_renamed_type::v1::MySimpleRecordRenamed;

                    let message_descriptor = MySimpleRecordRenamed::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_1::v1::{
                        MyOtherRecord, MySimpleRecord,
                    };
                    for message_descriptor in vec![
                        MyOtherRecord::default().descriptor(),
                        MySimpleRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_1_evolved::v1::{
                        AnotherRecord, MyOtherRecord, MySimpleRecord,
                    };

                    for message_descriptor in vec![
                        MySimpleRecord::default().descriptor(),
                        MyOtherRecord::default().descriptor(),
                        AnotherRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_1_evolved_again::v1::{AnotherRecord, MyOtherRecord, MySimpleRecord, OneMoreRecord, Record};

                    for message_descriptor in vec![
                        MySimpleRecord::default().descriptor(),
                        MyOtherRecord::default().descriptor(),
                        AnotherRecord::default().descriptor(),
                        OneMoreRecord::default().descriptor(),
                        Record::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_2::v1::MyLongRecord;

                    let message_descriptor = MyLongRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_3::v1::MyHierarchicalRecord;

                    let message_descriptor = MyHierarchicalRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_4::v1::{
                        RestaurantRecord, RestaurantReview, RestaurantReviewer, RestaurantTag,
                        ReviewerStats,
                    };

                    for message_descriptor in vec![
                        RestaurantReviewer::default().descriptor(),
                        ReviewerStats::default().descriptor(),
                        RestaurantReview::default().descriptor(),
                        RestaurantTag::default().descriptor(),
                        RestaurantRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_4_wrapper::v1::{
                        RestaurantComplexRecord, RestaurantComplexReview,
                        RestaurantComplexReviewList, RestaurantRecord, RestaurantReview,
                        RestaurantReviewList, RestaurantReviewer, RestaurantTag, RestaurantTagList,
                        ReviewerEndorsements, ReviewerEndorsementsList, ReviewerStats, StringList,
                    };

                    for message_descriptor in vec![
                        ReviewerEndorsements::default().descriptor(),
                        ReviewerEndorsementsList::default().descriptor(),
                        RestaurantComplexReview::default().descriptor(),
                        RestaurantReviewer::default().descriptor(),
                        ReviewerStats::default().descriptor(),
                        RestaurantReview::default().descriptor(),
                        RestaurantTag::default().descriptor(),
                        StringList::default().descriptor(),
                        RestaurantTagList::default().descriptor(),
                        RestaurantReviewList::default().descriptor(),
                        RestaurantComplexReviewList::default().descriptor(),
                        RestaurantRecord::default().descriptor(),
                        RestaurantComplexRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_5::v1::{
                        AlarmIndex, CalendarEvent, CalendarEventIndex, Recurrence,
                    };

                    for message_descriptor in vec![
                        CalendarEvent::default().descriptor(),
                        AlarmIndex::default().descriptor(),
                        CalendarEventIndex::default().descriptor(),
                        Recurrence::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_6::v1::MyRepeatedRecord;

                    let message_descriptor = MyRepeatedRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_7::v1::{
                        MyRecord1, MyRecord2,
                    };

                    for message_descriptor in vec![
                        MyRecord1::default().descriptor(),
                        MyRecord2::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_8::v1::StringRecordId;

                    let message_descriptor = StringRecordId::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_bad_union_2::v1::MySimpleRecord;

                    let message_descriptor = MySimpleRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_bitmap::v1::{
                        MyNestedRecord, MySimpleRecord,
                    };

                    for message_descriptor in vec![
                        MySimpleRecord::default().descriptor(),
                        MyNestedRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_bytes::v1::ByteStringRecord;

                    let message_descriptor = ByteStringRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_chained_2::v1::MyChainedRecord2;

                    let message_descriptor = MyChainedRecord2::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_datatypes::v1::TypesRecord;

                    let message_descriptor = TypesRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_double_nested::v1::{
                        MiddleRecord, OtherRecord, OuterRecord,
                    };

                    for message_descriptor in vec![
                        OuterRecord::default().descriptor(),
                        OtherRecord::default().descriptor(),
                        MiddleRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_enum::v1::MyShapeRecord;

                    let message_descriptor = MyShapeRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_implicit_usage::v1::{
                        MyOtherRecord, MySimpleRecord,
                    };

                    for message_descriptor in vec![
                        MySimpleRecord::default().descriptor(),
                        MyOtherRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_implicit_usage_no_union::v1::{MySimpleRecord, MyOtherRecord};

                    for message_descriptor in vec![
                        MySimpleRecord::default().descriptor(),
                        MyOtherRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_import_flat::v1::{
                        MyLongRecord, MySimpleRecord,
                    };

                    for message_descriptor in vec![
                        MySimpleRecord::default().descriptor(),
                        MyLongRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_imported_and_new::v1::{MySimpleRecord, MyOtherRecord};

                    for message_descriptor in vec![
                        MySimpleRecord::default().descriptor(),
                        MyOtherRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_index_compat::v1::{
                        MyCompatRecord, MyModernRecord,
                    };

                    for message_descriptor in vec![
                        MyCompatRecord::default().descriptor(),
                        MyModernRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_index_filtering::v1::MyBasicRecord;

                    let message_descriptor = MyBasicRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_join_index::v1::{
                        Customer, CustomerWithHeader, Header, Item, JoiningRecord, MyOtherRecord,
                        MySimpleRecord, NestedA, NestedB, Order, OrderWithHeader, Ref, TypeA,
                        TypeB, TypeC,
                    };

                    for message_descriptor in vec![
                        MySimpleRecord::default().descriptor(),
                        MyOtherRecord::default().descriptor(),
                        JoiningRecord::default().descriptor(),
                        TypeA::default().descriptor(),
                        TypeB::default().descriptor(),
                        TypeC::default().descriptor(),
                        NestedA::default().descriptor(),
                        NestedB::default().descriptor(),
                        Customer::default().descriptor(),
                        Order::default().descriptor(),
                        Item::default().descriptor(),
                        Header::default().descriptor(),
                        Ref::default().descriptor(),
                        CustomerWithHeader::default().descriptor(),
                        OrderWithHeader::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_leaderboard::v1::{
                        FlatLeaderboardRecord, NestedLeaderboardEntry, NestedLeaderboardRecord,
                    };

                    for message_descriptor in vec![
                        NestedLeaderboardRecord::default().descriptor(),
                        NestedLeaderboardEntry::default().descriptor(),
                        FlatLeaderboardRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_map::v1::{
                        MapRecord, OuterRecord,
                    };

                    for message_descriptor in vec![
                        OuterRecord::default().descriptor(),
                        MapRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_marked_unmarked::v1::{MyMarkedRecord, MyUnmarkedRecord1, MyUnmarkedRecord2};

                    for message_descriptor in vec![
                        MyMarkedRecord::default().descriptor(),
                        MyUnmarkedRecord1::default().descriptor(),
                        MyUnmarkedRecord2::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_multi::v1::{
                        MultiRecordOne, MultiRecordThree, MultiRecordTwo,
                    };

                    for message_descriptor in vec![
                        MultiRecordOne::default().descriptor(),
                        MultiRecordTwo::default().descriptor(),
                        MultiRecordThree::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_multidimensional::v1::MyMultidimensionalRecord;

                    let message_descriptor = MyMultidimensionalRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_name_clash::v1::MySimpleRecord;

                    let message_descriptor = MySimpleRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_nested_as_record::v1::{OuterRecord, OtherRecord};

                    for message_descriptor in vec![
                        OuterRecord::default().descriptor(),
                        OtherRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_no_primary_key::v1::MyNoPrimaryKeyRecord;

                    let message_descriptor = MyNoPrimaryKeyRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_nulls_2::v1::MyNullRecord;

                    let message_descriptor = MyNullRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_oneof::v1::{
                        MyOtherRecord, MySimpleRecord,
                    };

                    for message_descriptor in vec![
                        MySimpleRecord::default().descriptor(),
                        MyOtherRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_parent_child::v1::{
                        MyChildRecord, MyParentRecord,
                    };

                    for message_descriptor in vec![
                        MyParentRecord::default().descriptor(),
                        MyChildRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_rank::v1::{
                        BasicRankedRecord, HeaderRankedRecord, NestedRankedRecord,
                        RepeatedRankedRecord,
                    };

                    for message_descriptor in vec![
                        BasicRankedRecord::default().descriptor(),
                        NestedRankedRecord::default().descriptor(),
                        HeaderRankedRecord::default().descriptor(),
                        RepeatedRankedRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_text::v1::{
                        ComplexDocument, ManyFieldsDocument, MapDocument, MultiDocument,
                        NestedMapDocument, SimpleDocument,
                    };

                    for message_descriptor in vec![
                        SimpleDocument::default().descriptor(),
                        ComplexDocument::default().descriptor(),
                        MapDocument::default().descriptor(),
                        MultiDocument::default().descriptor(),
                        NestedMapDocument::default().descriptor(),
                        ManyFieldsDocument::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_transform::v1::{
                        DefaultTransformMessage, TransformMessageMaxTypes,
                    };

                    for message_descriptor in vec![
                        DefaultTransformMessage::default().descriptor(),
                        TransformMessageMaxTypes::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_tuple_fields::v1::MyFieldsRecord;

                    let message_descriptor = MyFieldsRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_union_missing_record::v1::{MyMissingRecord, MyUsedRecord};

                    for message_descriptor in vec![
                        MyMissingRecord::default().descriptor(),
                        MyUsedRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_union_with_imported_nested::v1::MyUsedRecord;

                    let message_descriptor = MyUsedRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_union_with_nested::v1::{MyNestedRecord, MyUsedRecord};

                    for message_descriptor in vec![
                        MyNestedRecord::default().descriptor(),
                        MyUsedRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_with_header::v1::{
                        HeaderRecord, MyRecord,
                    };

                    for message_descriptor in vec![
                        HeaderRecord::default().descriptor(),
                        MyRecord::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto::test_records_with_union::v1::{
                        MySimpleRecord, MySimpleRecord2, MySimpleRecord3, Nested, NotInUnion,
                    };

                    for message_descriptor in vec![
                        MySimpleRecord::default().descriptor(),
                        MySimpleRecord2::default().descriptor(),
                        MySimpleRecord3::default().descriptor(),
                        NotInUnion::default().descriptor(),
                        Nested::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto2::test_records_maps::v1::{
                        StringToInt, StringToString,
                    };

                    for message_descriptor in vec![
                        StringToString::default().descriptor(),
                        StringToInt::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto3::evolution::test_nested_proto2::v1::NestedRecord;

                    let message_descriptor = NestedRecord::default().descriptor();
                    assert_eq!(
                        WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                        Ok(WellFormedMessageDescriptor {
                            inner: message_descriptor,
                        })
                    );
                }

                {
                    use fdb_rl_proto::fdb_rl_test::java::proto3::test_records_maps::v1::{
                        StringToInt, StringToString,
                    };

                    for message_descriptor in vec![
                        StringToString::default().descriptor(),
                        StringToInt::default().descriptor(),
                    ] {
                        assert_eq!(
                            WellFormedMessageDescriptor::try_from(message_descriptor.clone()),
                            Ok(WellFormedMessageDescriptor {
                                inner: message_descriptor,
                            })
                        );
                    }
                }
            }
        }
    }
}
