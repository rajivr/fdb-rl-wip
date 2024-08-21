// TODO (write tests)

use fdb::error::{FdbError, FdbResult};

use std::convert::TryFrom;

use crate::metadata::{
    IndexSchema, IndexSchemaElement, IndexSchemaKey, IndexSchemaValue, PrimaryKeySchema,
    PrimaryKeySchemaElement,
};

use super::error::PROTOBUF_TUPLE_SCHEMA_INVALID_PROTO;

/// Protobuf types
pub(crate) mod pb {
    use fdb::error::{FdbError, FdbResult};

    use std::convert::TryFrom;

    use super::super::error::PROTOBUF_TUPLE_SCHEMA_INVALID_PROTO;

    /// Protobuf generated types renamed to prepend `Proto` and append
    /// version (and add `Enum` suffix).
    pub(crate) use fdb_rl_proto::fdb_rl::tuple_schema::v1::{
        IndexSchema as ProtoIndexSchemaV1, IndexSchemaElement as ProtoIndexSchemaElementEnumV1,
        IndexSchemaKey as ProtoIndexSchemaKeyV1, IndexSchemaValue as ProtoIndexSchemaValueV1,
        PrimaryKeySchema as ProtoPrimaryKeySchemaV1,
        PrimaryKeySchemaElement as ProtoPrimaryKeySchemaElementEnumV1,
    };

    /// Protobuf message `fdb_rl.tuple_schema.v1.IndexSchema` contains
    /// a `Required` field. So, we need to define this type.
    pub(crate) struct IndexSchemaInternalV1 {
        pub(crate) key_schema: ProtoIndexSchemaKeyV1,
        pub(crate) value_schema: Option<ProtoIndexSchemaValueV1>,
    }

    impl TryFrom<ProtoIndexSchemaV1> for IndexSchemaInternalV1 {
        type Error = FdbError;

        fn try_from(proto_index_schema_v1: ProtoIndexSchemaV1) -> FdbResult<IndexSchemaInternalV1> {
            let ProtoIndexSchemaV1 {
                key_schema,
                value_schema,
            } = proto_index_schema_v1;

            let maybe_key_schema = key_schema;

            maybe_key_schema
                .map(|key_schema| IndexSchemaInternalV1 {
                    key_schema,
                    value_schema,
                })
                .ok_or_else(|| FdbError::new(PROTOBUF_TUPLE_SCHEMA_INVALID_PROTO))
        }
    }

    impl From<IndexSchemaInternalV1> for ProtoIndexSchemaV1 {
        fn from(index_schema_internal_v1: IndexSchemaInternalV1) -> ProtoIndexSchemaV1 {
            let IndexSchemaInternalV1 {
                key_schema,
                value_schema,
            } = index_schema_internal_v1;

            ProtoIndexSchemaV1 {
                key_schema: Some(key_schema),
                value_schema,
            }
        }
    }
}

/// Internal representation of `PrimaryKeySchema`
///
/// We do not do any validation checks on this type. Do not assume it
/// to be well formed. Instead use this *only* as an intermediary type
/// for serializing and deserializing from protobuf.
pub(crate) enum PrimaryKeySchemaInternal {
    V1(pb::ProtoPrimaryKeySchemaV1),
}

// internal function used within `From` impls below.
fn from_primary_key_schema_element(
    primary_key_schema_element: PrimaryKeySchemaElement,
) -> pb::ProtoPrimaryKeySchemaElementEnumV1 {
    match primary_key_schema_element {
        PrimaryKeySchemaElement::String => pb::ProtoPrimaryKeySchemaElementEnumV1::String,
        PrimaryKeySchemaElement::Double => pb::ProtoPrimaryKeySchemaElementEnumV1::Double,
        PrimaryKeySchemaElement::Float => pb::ProtoPrimaryKeySchemaElementEnumV1::Float,
        PrimaryKeySchemaElement::Integer => pb::ProtoPrimaryKeySchemaElementEnumV1::Integer,
        PrimaryKeySchemaElement::Boolean => pb::ProtoPrimaryKeySchemaElementEnumV1::Boolean,
        PrimaryKeySchemaElement::Bytes => pb::ProtoPrimaryKeySchemaElementEnumV1::Bytes,
        PrimaryKeySchemaElement::Uuid => pb::ProtoPrimaryKeySchemaElementEnumV1::Uuid,
    }
}

impl From<PrimaryKeySchema> for PrimaryKeySchemaInternal {
    // When we have a value of type `PrimaryKeySchema`, we assume it
    // is well formed. So, we convert it directly into a value of type
    // `PrimaryKeySchemaInternal` without any additional checks.
    fn from(primary_key_schema: PrimaryKeySchema) -> PrimaryKeySchemaInternal {
        let key_schema = primary_key_schema.into_part();

        let proto_key_schema = key_schema
            .into_iter()
            .map(from_primary_key_schema_element)
            .map(i32::from)
            .collect::<Vec<i32>>();

        PrimaryKeySchemaInternal::V1(pb::ProtoPrimaryKeySchemaV1 {
            key_schema: proto_key_schema,
        })
    }
}

// internal function used within `TryFrom` impl below.
fn try_from_proto_primary_key_schema_element_enum_v1(
    proto_primary_key_schema_element_enum_v1: pb::ProtoPrimaryKeySchemaElementEnumV1,
) -> FdbResult<PrimaryKeySchemaElement> {
    Ok(match proto_primary_key_schema_element_enum_v1 {
        pb::ProtoPrimaryKeySchemaElementEnumV1::Unspecified => {
            return Err(FdbError::new(PROTOBUF_TUPLE_SCHEMA_INVALID_PROTO))
        }
        pb::ProtoPrimaryKeySchemaElementEnumV1::String => PrimaryKeySchemaElement::String,
        pb::ProtoPrimaryKeySchemaElementEnumV1::Double => PrimaryKeySchemaElement::Double,
        pb::ProtoPrimaryKeySchemaElementEnumV1::Float => PrimaryKeySchemaElement::Float,
        pb::ProtoPrimaryKeySchemaElementEnumV1::Integer => PrimaryKeySchemaElement::Integer,
        pb::ProtoPrimaryKeySchemaElementEnumV1::Boolean => PrimaryKeySchemaElement::Boolean,
        pb::ProtoPrimaryKeySchemaElementEnumV1::Bytes => PrimaryKeySchemaElement::Bytes,
        pb::ProtoPrimaryKeySchemaElementEnumV1::Uuid => PrimaryKeySchemaElement::Uuid,
    })
}

impl TryFrom<PrimaryKeySchemaInternal> for PrimaryKeySchema {
    type Error = FdbError;

    fn try_from(
        primary_key_schema_internal: PrimaryKeySchemaInternal,
    ) -> FdbResult<PrimaryKeySchema> {
        let PrimaryKeySchemaInternal::V1(pb::ProtoPrimaryKeySchemaV1 { key_schema }) =
            primary_key_schema_internal;

        let metadata_key_schema = key_schema.into_iter().try_fold(
            Vec::<PrimaryKeySchemaElement>::new(),
            |mut acc, x| {
                // First check to make sure that an `i32` can be
                // properly converted into a
                // `pb::ProtoPrimaryKeySchemaElementEnumV1`. Then
                // attempt to convert a value of type
                // `pb::ProtoPrimaryKeySchemaElementEnumV1` to
                // `PrimaryKeySchemaElement`.
                let y = pb::ProtoPrimaryKeySchemaElementEnumV1::try_from(x)
                    .map_err(|_| FdbError::new(PROTOBUF_TUPLE_SCHEMA_INVALID_PROTO))
                    .and_then(try_from_proto_primary_key_schema_element_enum_v1)?;

                acc.push(y);

                Ok::<Vec<PrimaryKeySchemaElement>, FdbError>(acc)
            },
        )?;

        // Check to make sure vec of `PrimaryKeySchemaElement` is
        // well-formed.
        PrimaryKeySchema::try_from(metadata_key_schema)
            .map_err(|_| FdbError::new(PROTOBUF_TUPLE_SCHEMA_INVALID_PROTO))
    }
}

/// Internal representation of `IndexSchema`
///
/// We do not do any validation checks on this type. Do not assume it
/// to be well formed. Instead use this *only* as an intermediary type
/// for serializing and deserializing from protobuf.
pub(crate) enum IndexSchemaInternal {
    V1(pb::IndexSchemaInternalV1),
}

// internal function used within `From` impl below.
fn from_index_schema_element(
    index_schema_element: IndexSchemaElement,
) -> pb::ProtoIndexSchemaElementEnumV1 {
    match index_schema_element {
        IndexSchemaElement::String => pb::ProtoIndexSchemaElementEnumV1::String,
        IndexSchemaElement::MaybeString => pb::ProtoIndexSchemaElementEnumV1::MaybeString,
        IndexSchemaElement::Double => pb::ProtoIndexSchemaElementEnumV1::Double,
        IndexSchemaElement::MaybeDouble => pb::ProtoIndexSchemaElementEnumV1::MaybeDouble,
        IndexSchemaElement::Float => pb::ProtoIndexSchemaElementEnumV1::Float,
        IndexSchemaElement::MaybeFloat => pb::ProtoIndexSchemaElementEnumV1::MaybeFloat,
        IndexSchemaElement::Integer => pb::ProtoIndexSchemaElementEnumV1::Integer,
        IndexSchemaElement::MaybeInteger => pb::ProtoIndexSchemaElementEnumV1::MaybeInteger,
        IndexSchemaElement::Boolean => pb::ProtoIndexSchemaElementEnumV1::Boolean,
        IndexSchemaElement::MaybeBoolean => pb::ProtoIndexSchemaElementEnumV1::MaybeBoolean,
        IndexSchemaElement::Bytes => pb::ProtoIndexSchemaElementEnumV1::Bytes,
        IndexSchemaElement::MaybeBytes => pb::ProtoIndexSchemaElementEnumV1::MaybeBytes,
        IndexSchemaElement::Uuid => pb::ProtoIndexSchemaElementEnumV1::Uuid,
        IndexSchemaElement::MaybeUuid => pb::ProtoIndexSchemaElementEnumV1::MaybeUuid,
        IndexSchemaElement::Versionstamp => pb::ProtoIndexSchemaElementEnumV1::Versionstamp,
        IndexSchemaElement::ListOfString => pb::ProtoIndexSchemaElementEnumV1::ListOfString,
        IndexSchemaElement::ListOfDouble => pb::ProtoIndexSchemaElementEnumV1::ListOfDouble,
        IndexSchemaElement::ListOfFloat => pb::ProtoIndexSchemaElementEnumV1::ListOfFloat,
        IndexSchemaElement::ListOfInteger => pb::ProtoIndexSchemaElementEnumV1::ListOfInteger,
        IndexSchemaElement::ListOfBoolean => pb::ProtoIndexSchemaElementEnumV1::ListOfBoolean,
        IndexSchemaElement::ListOfBytes => pb::ProtoIndexSchemaElementEnumV1::ListOfBytes,
        IndexSchemaElement::ListOfUuid => pb::ProtoIndexSchemaElementEnumV1::ListOfUuid,
    }
}

impl From<IndexSchema> for IndexSchemaInternal {
    // When we have a value of type `IndexSchema`, we assume it is
    // well formed. So, we convert it directly into a value of type
    // `IndexSchemaInternal` without any additional checks.
    fn from(index_schema: IndexSchema) -> IndexSchemaInternal {
        let (key_schema, value_schema) = index_schema.into_parts();

        let proto_key_schema = {
            let (index_schema_elements, primary_key_schema_elements) = key_schema.into_parts();

            let proto_index_schema = index_schema_elements
                .into_iter()
                .map(from_index_schema_element)
                .map(i32::from)
                .collect::<Vec<i32>>();

            let proto_primary_key_schema = primary_key_schema_elements
                .into_iter()
                .map(from_primary_key_schema_element)
                .map(i32::from)
                .collect::<Vec<i32>>();

            pb::ProtoIndexSchemaKeyV1 {
                index_schema: proto_index_schema,
                primary_key_schema: proto_primary_key_schema,
            }
        };

        let maybe_proto_value_schema =
            value_schema
                .map(IndexSchemaValue::into_part)
                .map(|index_schema_elements| pb::ProtoIndexSchemaValueV1 {
                    index_schema: index_schema_elements
                        .into_iter()
                        .map(from_index_schema_element)
                        .map(i32::from)
                        .collect::<Vec<i32>>(),
                });

        IndexSchemaInternal::V1(pb::IndexSchemaInternalV1 {
            key_schema: proto_key_schema,
            value_schema: maybe_proto_value_schema,
        })
    }
}

// internal function used within `TryFrom` impl below.
fn try_from_proto_index_schema_element_enum_v1(
    proto_index_schema_element_enum_v1: pb::ProtoIndexSchemaElementEnumV1,
) -> FdbResult<IndexSchemaElement> {
    Ok(match proto_index_schema_element_enum_v1 {
        pb::ProtoIndexSchemaElementEnumV1::Unspecified => {
            return Err(FdbError::new(PROTOBUF_TUPLE_SCHEMA_INVALID_PROTO))
        }
        pb::ProtoIndexSchemaElementEnumV1::String => IndexSchemaElement::String,
        pb::ProtoIndexSchemaElementEnumV1::MaybeString => IndexSchemaElement::MaybeString,
        pb::ProtoIndexSchemaElementEnumV1::Double => IndexSchemaElement::Double,
        pb::ProtoIndexSchemaElementEnumV1::MaybeDouble => IndexSchemaElement::MaybeDouble,
        pb::ProtoIndexSchemaElementEnumV1::Float => IndexSchemaElement::Float,
        pb::ProtoIndexSchemaElementEnumV1::MaybeFloat => IndexSchemaElement::MaybeFloat,
        pb::ProtoIndexSchemaElementEnumV1::Integer => IndexSchemaElement::Integer,
        pb::ProtoIndexSchemaElementEnumV1::MaybeInteger => IndexSchemaElement::MaybeInteger,
        pb::ProtoIndexSchemaElementEnumV1::Boolean => IndexSchemaElement::Boolean,
        pb::ProtoIndexSchemaElementEnumV1::MaybeBoolean => IndexSchemaElement::MaybeBoolean,
        pb::ProtoIndexSchemaElementEnumV1::Bytes => IndexSchemaElement::Bytes,
        pb::ProtoIndexSchemaElementEnumV1::MaybeBytes => IndexSchemaElement::MaybeBytes,
        pb::ProtoIndexSchemaElementEnumV1::Uuid => IndexSchemaElement::Uuid,
        pb::ProtoIndexSchemaElementEnumV1::MaybeUuid => IndexSchemaElement::MaybeUuid,
        pb::ProtoIndexSchemaElementEnumV1::Versionstamp => IndexSchemaElement::Versionstamp,
        pb::ProtoIndexSchemaElementEnumV1::ListOfString => IndexSchemaElement::ListOfString,
        pb::ProtoIndexSchemaElementEnumV1::ListOfDouble => IndexSchemaElement::ListOfDouble,
        pb::ProtoIndexSchemaElementEnumV1::ListOfFloat => IndexSchemaElement::ListOfFloat,
        pb::ProtoIndexSchemaElementEnumV1::ListOfInteger => IndexSchemaElement::ListOfInteger,
        pb::ProtoIndexSchemaElementEnumV1::ListOfBoolean => IndexSchemaElement::ListOfBoolean,
        pb::ProtoIndexSchemaElementEnumV1::ListOfBytes => IndexSchemaElement::ListOfBytes,
        pb::ProtoIndexSchemaElementEnumV1::ListOfUuid => IndexSchemaElement::ListOfUuid,
    })
}

impl TryFrom<IndexSchemaInternal> for IndexSchema {
    type Error = FdbError;

    fn try_from(index_schema_internal: IndexSchemaInternal) -> FdbResult<IndexSchema> {
        // By using `pb::IndexSchemaInternalV1`, we are ensuring that
        // `key_schema` being a `Required` field is taken care of. We
        // still do not know if `key_schema` and `value_schema` are
        // well formed.
        let IndexSchemaInternal::V1(pb::IndexSchemaInternalV1 {
            key_schema,
            value_schema,
        }) = index_schema_internal;

        let metadata_key_schema = {
            let metadata_index_schema = key_schema.index_schema.into_iter().try_fold(
                Vec::<IndexSchemaElement>::new(),
                |mut acc, x| {
                    // First check to make sure that an `i32` can be
                    // properly converted into a
                    // `pb::ProtoIndexSchemaElementEnumV1`. Then
                    // attempt to convert a value of type
                    // `pb::ProtoIndexSchemaElementEnumV1` to
                    // `IndexSchemaElement`.
                    let y = pb::ProtoIndexSchemaElementEnumV1::try_from(x)
                        .map_err(|_| FdbError::new(PROTOBUF_TUPLE_SCHEMA_INVALID_PROTO))
                        .and_then(try_from_proto_index_schema_element_enum_v1)?;

                    acc.push(y);

                    Ok::<Vec<IndexSchemaElement>, FdbError>(acc)
                },
            )?;

            let metdata_primary_key_schema = key_schema.primary_key_schema.into_iter().try_fold(
                Vec::<PrimaryKeySchemaElement>::new(),
                |mut acc, x| {
                    // First check to make sure that an `i32` can be
                    // properly converted into a
                    // `pb::ProtoPrimaryKeySchemaElementEnumV1`. Then
                    // attempt to convert a value of type
                    // `pb::ProtoPrimaryKeySchemaElementEnumV1` to
                    // `PrimaryKeySchemaElement`.
                    let y = pb::ProtoPrimaryKeySchemaElementEnumV1::try_from(x)
                        .map_err(|_| FdbError::new(PROTOBUF_TUPLE_SCHEMA_INVALID_PROTO))
                        .and_then(try_from_proto_primary_key_schema_element_enum_v1)?;

                    acc.push(y);

                    Ok::<Vec<PrimaryKeySchemaElement>, FdbError>(acc)
                },
            )?;

            // Check to make sure vec of `IndexSchemaElement` and vec
            // of `PrimaryKeySchemaElement` is well-formed.
            IndexSchemaKey::try_from((metadata_index_schema, metdata_primary_key_schema))
                .map_err(|_| FdbError::new(PROTOBUF_TUPLE_SCHEMA_INVALID_PROTO))?
        };

        let metadata_value_schema = match value_schema {
            None => None,
            Some(value_schema) => Some(
                value_schema
                    .index_schema
                    .into_iter()
                    .try_fold(Vec::<IndexSchemaElement>::new(), |mut acc, x| {
                        // First check to make sure that an `i32` can be
                        // properly converted into a
                        // `pb::ProtoIndexSchemaElementEnumV1`. Then
                        // attempt to convert a value of type
                        // `pb::ProtoIndexSchemaElementEnumV1` to
                        // `IndexSchemaElement`.
                        let y = pb::ProtoIndexSchemaElementEnumV1::try_from(x)
                            .map_err(|_| FdbError::new(PROTOBUF_TUPLE_SCHEMA_INVALID_PROTO))
                            .and_then(try_from_proto_index_schema_element_enum_v1)?;

                        acc.push(y);

                        Ok::<Vec<IndexSchemaElement>, FdbError>(acc)
                    })
                    .and_then(IndexSchemaValue::try_from)
                    .map_err(|_| FdbError::new(PROTOBUF_TUPLE_SCHEMA_INVALID_PROTO))?,
            ),
        };

        IndexSchema::try_from((metadata_key_schema, metadata_value_schema))
            .map_err(|_| FdbError::new(PROTOBUF_TUPLE_SCHEMA_INVALID_PROTO))
    }
}
