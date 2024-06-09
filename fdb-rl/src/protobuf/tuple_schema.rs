// TODO: Continue from here.

/// Protobuf types
pub(crate) mod pb {
    use fdb::error::{FdbError, FdbResult};

    use super::super::error::PROTOBUF_TUPLE_SCHEMA_INVALID_PROTO;

    use std::convert::{TryFrom, TryInto};

    /// Protobuf generated types renamed to prepend `Proto` and append
    /// version (and add `Enum` suffix).
    pub(crate) use fdb_rl_proto::fdb_rl::tuple_schema::v1::{
        Boolean as ProtoBooleanV1, Bytes as ProtoBytesV1, Double as ProtoDoubleV1,
        Float as ProtoFloatV1, IndexSchema as ProtoIndexSchemaV1,
        IndexSchemaElement as ProtoIndexSchemaElementV1, Integer as ProtoIntegerV1,
        ListOfBoolean as ProtoListOfBooleanV1, ListOfBytes as ProtoListOfBytesV1,
        ListOfDouble as ProtoListOfDoubleV1, ListOfFloat as ProtoListOfFloatV1,
        ListOfInteger as ProtoListOfIntegerV1, ListOfString as ProtoListOfStringV1,
        ListOfUuid as ProtoListOfUuidV1, MaybeBoolean as ProtoMaybeBooleanV1,
        MaybeDouble as ProtoMaybeDoubleV1, MaybeFloat as ProtoMaybeFloatV1,
        MaybeInteger as ProtoMaybeIntegerV1, MaybeString as ProtoMaybeStringV1,
        MaybeUuid as ProtoMaybeUuidV1, PrimaryKeySchema as ProtoPrimaryKeySchemaV1,
        PrimaryKeySchemaElement as ProtoPrimaryKeyElementV1, String as ProtoStringV1,
        Uuid as ProtoUuidV1, Versionstamp as ProtoVersionstampV1,
    };

    /// Protobuf generated types renamed to prepend `Proto` and append
    /// version (and add `Enum` suffix).
    pub(crate) use fdb_rl_proto::fdb_rl::tuple_schema::v1::index_schema_element::IndexSchemaElement as ProtoIndexSchemaElementEnumV1;

    /// Protobuf generated types renamed to prepend `Proto` and append
    /// version (and add `Enum` suffix).
    pub(crate) use fdb_rl_proto::fdb_rl::tuple_schema::v1::primary_key_schema_element::PrimaryKeySchemaElement as ProtoPrimaryKeySchemaElementEnumV1;

    /// Protobuf message
    /// `fdb_rl.tuple_schema.v1.PrimaryKeySchemaElement.primary_key_schema_element`
    /// is a `Required` field.
    ///
    /// So, we need to define this type.
    #[derive(Clone, Debug, PartialEq)]
    pub(crate) struct PrimaryKeySchemaElementInternalV1 {
        pub(crate) primary_key_schema_element: ProtoPrimaryKeySchemaElementEnumV1,
    }

    impl TryFrom<ProtoPrimaryKeyElementV1> for PrimaryKeySchemaElementInternalV1 {
        type Error = FdbError;

        fn try_from(
            proto_primary_key_element_v1: ProtoPrimaryKeyElementV1,
        ) -> FdbResult<PrimaryKeySchemaElementInternalV1> {
            let ProtoPrimaryKeyElementV1 {
                primary_key_schema_element,
            } = proto_primary_key_element_v1;

            primary_key_schema_element
                .map(|x| PrimaryKeySchemaElementInternalV1 {
                    primary_key_schema_element: x,
                })
                .ok_or_else(|| FdbError::new(PROTOBUF_TUPLE_SCHEMA_INVALID_PROTO))
        }
    }

    impl From<PrimaryKeySchemaElementInternalV1> for ProtoPrimaryKeyElementV1 {
        fn from(
            primary_key_schema_element_internal_v1: PrimaryKeySchemaElementInternalV1,
        ) -> ProtoPrimaryKeyElementV1 {
            let PrimaryKeySchemaElementInternalV1 {
                primary_key_schema_element,
            } = primary_key_schema_element_internal_v1;

            ProtoPrimaryKeyElementV1 {
                primary_key_schema_element: Some(primary_key_schema_element),
            }
        }
    }

    /// Protobuf message
    /// `fdb_rl.tuple_schema.v1.PrimaryKeySchemaElement.primary_key_schema_element`
    /// is a `Required` field (transitive requirement).
    ///
    /// Protobuf message
    /// `fdb_rl.tuple_schema.v1.PrimaryKeySchema.primary_key_schema_elements`
    /// has a requirment that there *must* be at-least one element
    /// indicating the primary key. But we won't check this
    /// here. Instead we will do it outside of the the protobuf layer.
    ///
    /// So, we need to define this type.
    #[derive(Clone, Debug, PartialEq)]
    pub(crate) struct PrimaryKeySchemaInternalV1 {
        pub(crate) primary_key_schema_elements: Vec<PrimaryKeySchemaElementInternalV1>,
    }

    impl TryFrom<ProtoPrimaryKeySchemaV1> for PrimaryKeySchemaInternalV1 {
        type Error = FdbError;

        fn try_from(
            proto_primary_key_schema_v1: ProtoPrimaryKeySchemaV1,
        ) -> FdbResult<PrimaryKeySchemaInternalV1> {
            let ProtoPrimaryKeySchemaV1 {
                primary_key_schema_elements,
            } = proto_primary_key_schema_v1;

            let mut res = Vec::<PrimaryKeySchemaElementInternalV1>::new();

            for x in primary_key_schema_elements {
                res.push(TryInto::<PrimaryKeySchemaElementInternalV1>::try_into(x)?);
            }

            Ok(PrimaryKeySchemaInternalV1 {
                primary_key_schema_elements: res,
            })
        }
    }

    impl From<PrimaryKeySchemaInternalV1> for ProtoPrimaryKeySchemaV1 {
        fn from(
            primary_key_schema_internal_v1: PrimaryKeySchemaInternalV1,
        ) -> ProtoPrimaryKeySchemaV1 {
            let PrimaryKeySchemaInternalV1 {
                primary_key_schema_elements,
            } = primary_key_schema_internal_v1;

            let primary_key_schema_elements = primary_key_schema_elements
                .into_iter()
                .map(ProtoPrimaryKeyElementV1::from)
                .collect::<Vec<ProtoPrimaryKeyElementV1>>();

            ProtoPrimaryKeySchemaV1 {
                primary_key_schema_elements,
            }
        }
    }

    /// Protobuf message
    /// `fdb_rl.tuple_schema.v1.IndexSchemaElement.index_schema_element`
    /// is a `Required` field.
    ///
    /// So, we need to define this type.
    #[derive(Clone, Debug, PartialEq)]
    pub(crate) struct IndexSchemaElementInternalV1 {
        pub(crate) index_schema_element: ProtoIndexSchemaElementEnumV1,
    }

    impl TryFrom<ProtoIndexSchemaElementV1> for IndexSchemaElementInternalV1 {
        type Error = FdbError;

        fn try_from(
            proto_index_schema_element_v1: ProtoIndexSchemaElementV1,
        ) -> FdbResult<IndexSchemaElementInternalV1> {
            todo!();
        }
    }

    impl From<IndexSchemaElementInternalV1> for ProtoIndexSchemaElementV1 {
        fn from(
            index_schema_element_internal_v1: IndexSchemaElementInternalV1,
        ) -> ProtoIndexSchemaElementV1 {
            todo!();
        }
    }

    // TODO: Continue from here.
}
