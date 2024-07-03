// TODO (write tests)

use fdb::error::{FdbError, FdbResult};

use std::convert::TryFrom;

use crate::metadata::{
    IndexSchema, IndexSchemaElement, IndexSchemaFdbKey, IndexSchemaFdbValue, PrimaryKeySchema,
    PrimaryKeySchemaElement,
};

use super::error::PROTOBUF_TUPLE_SCHEMA_INVALID_PROTO;

/// Protobuf types
pub(crate) mod pb {
    /// Protobuf generated types renamed to prepend `Proto` and append
    /// version (and add `Enum` suffix).
    pub(crate) use fdb_rl_proto::fdb_rl::tuple_schema::v1::{
        IndexSchema as ProtoIndexSchemaV1, IndexSchemaElement as ProtoIndexSchemaElementEnumV1,
        IndexSchemaValue as ProtoIndexSchemaValueV1, PrimaryKeySchema as ProtoPrimaryKeySchemaV1,
        PrimaryKeySchemaElement as ProtoPrimaryKeySchemaElementEnumV1,
    };
}

/// Internal representation of `PrimaryKeySchema`
///
/// We do not do any validation checks on this type. Do not assume it
/// to be well formed. Instead use this *only* as an intermediary type
/// for serializing and deserializing from protobuf.
pub(crate) enum PrimaryKeySchemaInternal {
    V1(pb::ProtoPrimaryKeySchemaV1),
}

impl From<PrimaryKeySchema> for PrimaryKeySchemaInternal {
    // When we have a value of type `PrimaryKeySchema`, we assume it
    // is well formed. So, we convert it directly into a value of type
    // `PrimaryKeySchemaInternal` without any additional checks.
    fn from(primary_key_schema: PrimaryKeySchema) -> PrimaryKeySchemaInternal {
        let (fdb_key_schema,) = primary_key_schema.into_parts();

        let mut proto_fdb_key_schema = Vec::<i32>::new();

        for x in fdb_key_schema {
            proto_fdb_key_schema.push(i32::from(match x {
                PrimaryKeySchemaElement::String => pb::ProtoPrimaryKeySchemaElementEnumV1::String,
                PrimaryKeySchemaElement::Double => pb::ProtoPrimaryKeySchemaElementEnumV1::Double,
                PrimaryKeySchemaElement::Float => pb::ProtoPrimaryKeySchemaElementEnumV1::Float,
                PrimaryKeySchemaElement::Integer => pb::ProtoPrimaryKeySchemaElementEnumV1::Integer,
                PrimaryKeySchemaElement::Boolean => pb::ProtoPrimaryKeySchemaElementEnumV1::Boolean,
                PrimaryKeySchemaElement::Bytes => pb::ProtoPrimaryKeySchemaElementEnumV1::Bytes,
                PrimaryKeySchemaElement::Uuid => pb::ProtoPrimaryKeySchemaElementEnumV1::Uuid,
            }));
        }

        PrimaryKeySchemaInternal::V1(pb::ProtoPrimaryKeySchemaV1 {
            fdb_key_schema: proto_fdb_key_schema,
        })
    }
}

impl TryFrom<PrimaryKeySchemaInternal> for PrimaryKeySchema {
    type Error = FdbError;

    fn try_from(
        primary_key_schema_internal: PrimaryKeySchemaInternal,
    ) -> FdbResult<PrimaryKeySchema> {
        let PrimaryKeySchemaInternal::V1(pb::ProtoPrimaryKeySchemaV1 { fdb_key_schema }) =
            primary_key_schema_internal;

        let mut metadata_fdb_key_schema = Vec::<PrimaryKeySchemaElement>::new();

        for x in fdb_key_schema {
            let primary_key_schema_element =
                match pb::ProtoPrimaryKeySchemaElementEnumV1::try_from(x)
                    .map_err(|_| FdbError::new(PROTOBUF_TUPLE_SCHEMA_INVALID_PROTO))?
                {
                    pb::ProtoPrimaryKeySchemaElementEnumV1::Unspecified => {
                        return Err(FdbError::new(PROTOBUF_TUPLE_SCHEMA_INVALID_PROTO))
                    }
                    pb::ProtoPrimaryKeySchemaElementEnumV1::String => {
                        PrimaryKeySchemaElement::String
                    }
                    pb::ProtoPrimaryKeySchemaElementEnumV1::Double => {
                        PrimaryKeySchemaElement::Double
                    }
                    pb::ProtoPrimaryKeySchemaElementEnumV1::Float => PrimaryKeySchemaElement::Float,
                    pb::ProtoPrimaryKeySchemaElementEnumV1::Integer => {
                        PrimaryKeySchemaElement::Integer
                    }
                    pb::ProtoPrimaryKeySchemaElementEnumV1::Boolean => {
                        PrimaryKeySchemaElement::Boolean
                    }
                    pb::ProtoPrimaryKeySchemaElementEnumV1::Bytes => PrimaryKeySchemaElement::Bytes,
                    pb::ProtoPrimaryKeySchemaElementEnumV1::Uuid => PrimaryKeySchemaElement::Uuid,
                };

            metadata_fdb_key_schema.push(primary_key_schema_element);
        }

        PrimaryKeySchema::try_from(metadata_fdb_key_schema)
            .map_err(|_| FdbError::new(PROTOBUF_TUPLE_SCHEMA_INVALID_PROTO))
    }
}

/// Internal representation of `IndexSchema`
///
/// We do not do any validation checks on this type. Do not assume it
/// to be well formed. Instead use this *only* as an intermediary type
/// for serializing and deserializing from protobuf.
pub(crate) enum IndexSchemaInternal {
    V1(pb::ProtoIndexSchemaV1),
}

impl From<IndexSchema> for IndexSchemaInternal {
    // When we have a value of type `IndexSchema`, we assume it is
    // well formed. So, we convert it directly into a value of type
    // `IndexSchemaInternal` without any additional checks.
    fn from(index_schema: IndexSchema) -> IndexSchemaInternal {
        let (fdb_key_schema, fdb_value_schema) = index_schema.into_parts();

        let mut proto_fdb_key_schema = Vec::<i32>::new();

        for x in fdb_key_schema {
            proto_fdb_key_schema.push(i32::from(match x {
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
                IndexSchemaElement::ListOfInteger => {
                    pb::ProtoIndexSchemaElementEnumV1::ListOfInteger
                }
                IndexSchemaElement::ListOfBoolean => {
                    pb::ProtoIndexSchemaElementEnumV1::ListOfBoolean
                }
                IndexSchemaElement::ListOfBytes => pb::ProtoIndexSchemaElementEnumV1::ListOfBytes,
                IndexSchemaElement::ListOfUuid => pb::ProtoIndexSchemaElementEnumV1::ListOfUuid,
            }));
        }

        let proto_fdb_value_schema = fdb_value_schema
            .map(|list_index_schema_element| {
                let mut proto_fdb_value_schema = Vec::<i32>::new();

                for x in list_index_schema_element {
                    proto_fdb_value_schema.push(i32::from(match x {
                        IndexSchemaElement::String => pb::ProtoIndexSchemaElementEnumV1::String,
                        IndexSchemaElement::MaybeString => {
                            pb::ProtoIndexSchemaElementEnumV1::MaybeString
                        }
                        IndexSchemaElement::Double => pb::ProtoIndexSchemaElementEnumV1::Double,
                        IndexSchemaElement::MaybeDouble => {
                            pb::ProtoIndexSchemaElementEnumV1::MaybeDouble
                        }
                        IndexSchemaElement::Float => pb::ProtoIndexSchemaElementEnumV1::Float,
                        IndexSchemaElement::MaybeFloat => {
                            pb::ProtoIndexSchemaElementEnumV1::MaybeFloat
                        }
                        IndexSchemaElement::Integer => pb::ProtoIndexSchemaElementEnumV1::Integer,
                        IndexSchemaElement::MaybeInteger => {
                            pb::ProtoIndexSchemaElementEnumV1::MaybeInteger
                        }
                        IndexSchemaElement::Boolean => pb::ProtoIndexSchemaElementEnumV1::Boolean,
                        IndexSchemaElement::MaybeBoolean => {
                            pb::ProtoIndexSchemaElementEnumV1::MaybeBoolean
                        }
                        IndexSchemaElement::Bytes => pb::ProtoIndexSchemaElementEnumV1::Bytes,
                        IndexSchemaElement::MaybeBytes => {
                            pb::ProtoIndexSchemaElementEnumV1::MaybeBytes
                        }
                        IndexSchemaElement::Uuid => pb::ProtoIndexSchemaElementEnumV1::Uuid,
                        IndexSchemaElement::MaybeUuid => {
                            pb::ProtoIndexSchemaElementEnumV1::MaybeUuid
                        }
                        IndexSchemaElement::Versionstamp => {
                            pb::ProtoIndexSchemaElementEnumV1::Versionstamp
                        }
                        IndexSchemaElement::ListOfString => {
                            pb::ProtoIndexSchemaElementEnumV1::ListOfString
                        }
                        IndexSchemaElement::ListOfDouble => {
                            pb::ProtoIndexSchemaElementEnumV1::ListOfDouble
                        }
                        IndexSchemaElement::ListOfFloat => {
                            pb::ProtoIndexSchemaElementEnumV1::ListOfFloat
                        }
                        IndexSchemaElement::ListOfInteger => {
                            pb::ProtoIndexSchemaElementEnumV1::ListOfInteger
                        }
                        IndexSchemaElement::ListOfBoolean => {
                            pb::ProtoIndexSchemaElementEnumV1::ListOfBoolean
                        }
                        IndexSchemaElement::ListOfBytes => {
                            pb::ProtoIndexSchemaElementEnumV1::ListOfBytes
                        }
                        IndexSchemaElement::ListOfUuid => {
                            pb::ProtoIndexSchemaElementEnumV1::ListOfUuid
                        }
                    }));
                }

                proto_fdb_value_schema
            })
            .map(|value| pb::ProtoIndexSchemaValueV1 { value });

        IndexSchemaInternal::V1(pb::ProtoIndexSchemaV1 {
            fdb_key_schema: proto_fdb_key_schema,
            fdb_value_schema: proto_fdb_value_schema,
        })
    }
}

impl TryFrom<IndexSchemaInternal> for IndexSchema {
    type Error = FdbError;

    fn try_from(index_schema_internal: IndexSchemaInternal) -> FdbResult<IndexSchema> {
        let IndexSchemaInternal::V1(pb::ProtoIndexSchemaV1 {
            fdb_key_schema,
            fdb_value_schema,
        }) = index_schema_internal;

        let mut metadata_fdb_key_schema = Vec::<IndexSchemaElement>::new();

        for x in fdb_key_schema {
            let index_schema_element = match pb::ProtoIndexSchemaElementEnumV1::try_from(x)
                .map_err(|_| FdbError::new(PROTOBUF_TUPLE_SCHEMA_INVALID_PROTO))?
            {
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
                pb::ProtoIndexSchemaElementEnumV1::ListOfInteger => {
                    IndexSchemaElement::ListOfInteger
                }
                pb::ProtoIndexSchemaElementEnumV1::ListOfBoolean => {
                    IndexSchemaElement::ListOfBoolean
                }
                pb::ProtoIndexSchemaElementEnumV1::ListOfBytes => IndexSchemaElement::ListOfBytes,
                pb::ProtoIndexSchemaElementEnumV1::ListOfUuid => IndexSchemaElement::ListOfUuid,
            };

            metadata_fdb_key_schema.push(index_schema_element)
        }

        let metadata_fdb_value_schema = FdbResult::<_>::Ok(fdb_value_schema).and_then(|x| {
            Ok(match x {
                None => None,
                Some(index_schema_value) => {
                    let pb::ProtoIndexSchemaValueV1 { value } = index_schema_value;

                    let mut metadata_fdb_value_schema_inner = Vec::<IndexSchemaElement>::new();

                    for x in value {
                        let index_schema_element =
                            match pb::ProtoIndexSchemaElementEnumV1::try_from(x)
                                .map_err(|_| FdbError::new(PROTOBUF_TUPLE_SCHEMA_INVALID_PROTO))?
                            {
                                pb::ProtoIndexSchemaElementEnumV1::Unspecified => {
                                    return Err(FdbError::new(PROTOBUF_TUPLE_SCHEMA_INVALID_PROTO))
                                }
                                pb::ProtoIndexSchemaElementEnumV1::String => {
                                    IndexSchemaElement::String
                                }
                                pb::ProtoIndexSchemaElementEnumV1::MaybeString => {
                                    IndexSchemaElement::MaybeString
                                }
                                pb::ProtoIndexSchemaElementEnumV1::Double => {
                                    IndexSchemaElement::Double
                                }
                                pb::ProtoIndexSchemaElementEnumV1::MaybeDouble => {
                                    IndexSchemaElement::MaybeDouble
                                }
                                pb::ProtoIndexSchemaElementEnumV1::Float => {
                                    IndexSchemaElement::Float
                                }
                                pb::ProtoIndexSchemaElementEnumV1::MaybeFloat => {
                                    IndexSchemaElement::MaybeFloat
                                }
                                pb::ProtoIndexSchemaElementEnumV1::Integer => {
                                    IndexSchemaElement::Integer
                                }
                                pb::ProtoIndexSchemaElementEnumV1::MaybeInteger => {
                                    IndexSchemaElement::MaybeInteger
                                }
                                pb::ProtoIndexSchemaElementEnumV1::Boolean => {
                                    IndexSchemaElement::Boolean
                                }
                                pb::ProtoIndexSchemaElementEnumV1::MaybeBoolean => {
                                    IndexSchemaElement::MaybeBoolean
                                }
                                pb::ProtoIndexSchemaElementEnumV1::Bytes => {
                                    IndexSchemaElement::Bytes
                                }
                                pb::ProtoIndexSchemaElementEnumV1::MaybeBytes => {
                                    IndexSchemaElement::MaybeBytes
                                }
                                pb::ProtoIndexSchemaElementEnumV1::Uuid => IndexSchemaElement::Uuid,
                                pb::ProtoIndexSchemaElementEnumV1::MaybeUuid => {
                                    IndexSchemaElement::MaybeUuid
                                }
                                pb::ProtoIndexSchemaElementEnumV1::Versionstamp => {
                                    IndexSchemaElement::Versionstamp
                                }
                                pb::ProtoIndexSchemaElementEnumV1::ListOfString => {
                                    IndexSchemaElement::ListOfString
                                }
                                pb::ProtoIndexSchemaElementEnumV1::ListOfDouble => {
                                    IndexSchemaElement::ListOfDouble
                                }
                                pb::ProtoIndexSchemaElementEnumV1::ListOfFloat => {
                                    IndexSchemaElement::ListOfFloat
                                }
                                pb::ProtoIndexSchemaElementEnumV1::ListOfInteger => {
                                    IndexSchemaElement::ListOfInteger
                                }
                                pb::ProtoIndexSchemaElementEnumV1::ListOfBoolean => {
                                    IndexSchemaElement::ListOfBoolean
                                }
                                pb::ProtoIndexSchemaElementEnumV1::ListOfBytes => {
                                    IndexSchemaElement::ListOfBytes
                                }
                                pb::ProtoIndexSchemaElementEnumV1::ListOfUuid => {
                                    IndexSchemaElement::ListOfUuid
                                }
                            };

                        metadata_fdb_value_schema_inner.push(index_schema_element);
                    }

                    Some(metadata_fdb_value_schema_inner)
                }
            })
        })?;

        IndexSchema::try_from((
            IndexSchemaFdbKey(metadata_fdb_key_schema),
            IndexSchemaFdbValue(metadata_fdb_value_schema),
        ))
        .map_err(|_| FdbError::new(PROTOBUF_TUPLE_SCHEMA_INVALID_PROTO))
    }
}
