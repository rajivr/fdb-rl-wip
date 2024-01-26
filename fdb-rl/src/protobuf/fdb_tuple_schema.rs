use fdb::tuple::TupleSchema;

/// Protobuf types
pub(crate) mod pb {
    use fdb::error::{FdbError, FdbResult};
    use fdb::tuple::{TupleSchema, TupleSchemaElement};

    use std::convert::TryFrom;

    use super::super::error::PROTOBUF_FDB_TUPLE_SCHEMA_INVALID_PROTO;

    /// Protobuf generated types renamed to prepend `Proto` and append
    /// version (and add `Enum` suffix).
    pub(crate) use fdb_rl_proto::fdb_rl::fdb_tuple_schema::v1::{
        FdbTupleSchema as ProtoFdbTupleSchemaV1,
        FdbTupleSchemaElement as ProtoFdbTupleSchemaElementV1,
    };

    /// Protobuf generated types renamed to prepend `Proto` and append
    /// version (and add `Enum` suffix).
    pub(crate) use fdb_rl_proto::fdb_rl::fdb_tuple_schema::v1::fdb_tuple_schema_element::{
        Boolean as ProtoBooleanV1, Bytes as ProtoBytesV1, Double as ProtoDoubleV1,
        FdbTupleSchemaElement as ProtoFdbTupleSchemaElementEnumV1, Float as ProtoFloatV1,
        Integer as ProtoIntegerV1, Null as ProtoNullV1, String as ProtoStringV1,
        Uuid as ProtoUuidV1, Versionstamp as ProtoVersionstampV1,
    };

    /// Protobuf message
    /// `fdb_rl.fdb_tuple_schema.v1.FdbTupleSchemaElement.fdb_tuple_schema_element`
    /// transitively contains a `Required` field. So we need to define
    /// this type.
    #[derive(Clone, Debug, PartialEq)]
    pub(crate) enum FdbTupleSchemaElementInternalEnumV1 {
        Null(ProtoNullV1),
        Bytes(ProtoBytesV1),
        String(ProtoStringV1),
        Tuple(FdbTupleSchemaInternalV1),
        Integer(ProtoIntegerV1),
        Float(ProtoFloatV1),
        Double(ProtoDoubleV1),
        Boolean(ProtoBooleanV1),
        Uuid(ProtoUuidV1),
        Versionstamp(ProtoVersionstampV1),
        MaybeBytes(ProtoBytesV1),
        MaybeString(ProtoStringV1),
        MaybeTuple(FdbTupleSchemaInternalV1),
        MaybeInteger(ProtoIntegerV1),
        MaybeFloat(ProtoFloatV1),
        MaybeDouble(ProtoDoubleV1),
        MaybeBoolean(ProtoBooleanV1),
        MaybeUuid(ProtoUuidV1),
        MaybeVersionstamp(ProtoVersionstampV1),
        ListOfBytes(ProtoBytesV1),
        ListOfString(ProtoStringV1),
        ListOfTuple(FdbTupleSchemaInternalV1),
        ListOfInteger(ProtoIntegerV1),
        ListOfFloat(ProtoFloatV1),
        ListOfDouble(ProtoDoubleV1),
        ListOfBoolean(ProtoBooleanV1),
        ListOfUuid(ProtoUuidV1),
        ListOfVersionstamp(ProtoVersionstampV1),
    }

    impl TryFrom<ProtoFdbTupleSchemaElementEnumV1> for FdbTupleSchemaElementInternalEnumV1 {
        type Error = FdbError;

        fn try_from(
            proto_fdb_tuple_schema_element_enum_v1: ProtoFdbTupleSchemaElementEnumV1,
        ) -> FdbResult<FdbTupleSchemaElementInternalEnumV1> {
            match proto_fdb_tuple_schema_element_enum_v1 {
                ProtoFdbTupleSchemaElementEnumV1::Null(ProtoNullV1 {}) => {
                    Ok(FdbTupleSchemaElementInternalEnumV1::Null(ProtoNullV1 {}))
                }
                ProtoFdbTupleSchemaElementEnumV1::Bytes(ProtoBytesV1 {}) => {
                    Ok(FdbTupleSchemaElementInternalEnumV1::Bytes(ProtoBytesV1 {}))
                }
                ProtoFdbTupleSchemaElementEnumV1::String(ProtoStringV1 {}) => Ok(
                    FdbTupleSchemaElementInternalEnumV1::String(ProtoStringV1 {}),
                ),
                ProtoFdbTupleSchemaElementEnumV1::Tuple(proto_fdb_tuple_schema_v1) => {
                    FdbTupleSchemaInternalV1::try_from(proto_fdb_tuple_schema_v1)
                        .map(FdbTupleSchemaElementInternalEnumV1::Tuple)
                }
                ProtoFdbTupleSchemaElementEnumV1::Integer(ProtoIntegerV1 {}) => Ok(
                    FdbTupleSchemaElementInternalEnumV1::Integer(ProtoIntegerV1 {}),
                ),
                ProtoFdbTupleSchemaElementEnumV1::Float(ProtoFloatV1 {}) => {
                    Ok(FdbTupleSchemaElementInternalEnumV1::Float(ProtoFloatV1 {}))
                }
                ProtoFdbTupleSchemaElementEnumV1::Double(ProtoDoubleV1 {}) => Ok(
                    FdbTupleSchemaElementInternalEnumV1::Double(ProtoDoubleV1 {}),
                ),
                ProtoFdbTupleSchemaElementEnumV1::Boolean(ProtoBooleanV1 {}) => Ok(
                    FdbTupleSchemaElementInternalEnumV1::Boolean(ProtoBooleanV1 {}),
                ),
                ProtoFdbTupleSchemaElementEnumV1::Uuid(ProtoUuidV1 {}) => {
                    Ok(FdbTupleSchemaElementInternalEnumV1::Uuid(ProtoUuidV1 {}))
                }
                ProtoFdbTupleSchemaElementEnumV1::Versionstamp(ProtoVersionstampV1 {}) => Ok(
                    FdbTupleSchemaElementInternalEnumV1::Versionstamp(ProtoVersionstampV1 {}),
                ),
                ProtoFdbTupleSchemaElementEnumV1::MaybeBytes(ProtoBytesV1 {}) => Ok(
                    FdbTupleSchemaElementInternalEnumV1::MaybeBytes(ProtoBytesV1 {}),
                ),
                ProtoFdbTupleSchemaElementEnumV1::MaybeString(ProtoStringV1 {}) => Ok(
                    FdbTupleSchemaElementInternalEnumV1::MaybeString(ProtoStringV1 {}),
                ),
                ProtoFdbTupleSchemaElementEnumV1::MaybeTuple(proto_fdb_tuple_schema_v1) => {
                    FdbTupleSchemaInternalV1::try_from(proto_fdb_tuple_schema_v1)
                        .map(FdbTupleSchemaElementInternalEnumV1::MaybeTuple)
                }
                ProtoFdbTupleSchemaElementEnumV1::MaybeInteger(ProtoIntegerV1 {}) => Ok(
                    FdbTupleSchemaElementInternalEnumV1::MaybeInteger(ProtoIntegerV1 {}),
                ),
                ProtoFdbTupleSchemaElementEnumV1::MaybeFloat(ProtoFloatV1 {}) => Ok(
                    FdbTupleSchemaElementInternalEnumV1::MaybeFloat(ProtoFloatV1 {}),
                ),
                ProtoFdbTupleSchemaElementEnumV1::MaybeDouble(ProtoDoubleV1 {}) => Ok(
                    FdbTupleSchemaElementInternalEnumV1::MaybeDouble(ProtoDoubleV1 {}),
                ),
                ProtoFdbTupleSchemaElementEnumV1::MaybeBoolean(ProtoBooleanV1 {}) => Ok(
                    FdbTupleSchemaElementInternalEnumV1::MaybeBoolean(ProtoBooleanV1 {}),
                ),
                ProtoFdbTupleSchemaElementEnumV1::MaybeUuid(ProtoUuidV1 {}) => Ok(
                    FdbTupleSchemaElementInternalEnumV1::MaybeUuid(ProtoUuidV1 {}),
                ),
                ProtoFdbTupleSchemaElementEnumV1::MaybeVersionstamp(ProtoVersionstampV1 {}) => Ok(
                    FdbTupleSchemaElementInternalEnumV1::MaybeVersionstamp(ProtoVersionstampV1 {}),
                ),
                ProtoFdbTupleSchemaElementEnumV1::ListOfBytes(ProtoBytesV1 {}) => Ok(
                    FdbTupleSchemaElementInternalEnumV1::ListOfBytes(ProtoBytesV1 {}),
                ),
                ProtoFdbTupleSchemaElementEnumV1::ListOfString(ProtoStringV1 {}) => Ok(
                    FdbTupleSchemaElementInternalEnumV1::ListOfString(ProtoStringV1 {}),
                ),
                ProtoFdbTupleSchemaElementEnumV1::ListOfTuple(proto_fdb_tuple_schema_v1) => {
                    FdbTupleSchemaInternalV1::try_from(proto_fdb_tuple_schema_v1)
                        .map(FdbTupleSchemaElementInternalEnumV1::ListOfTuple)
                }
                ProtoFdbTupleSchemaElementEnumV1::ListOfInteger(ProtoIntegerV1 {}) => Ok(
                    FdbTupleSchemaElementInternalEnumV1::ListOfInteger(ProtoIntegerV1 {}),
                ),
                ProtoFdbTupleSchemaElementEnumV1::ListOfFloat(ProtoFloatV1 {}) => Ok(
                    FdbTupleSchemaElementInternalEnumV1::ListOfFloat(ProtoFloatV1 {}),
                ),
                ProtoFdbTupleSchemaElementEnumV1::ListOfDouble(ProtoDoubleV1 {}) => Ok(
                    FdbTupleSchemaElementInternalEnumV1::ListOfDouble(ProtoDoubleV1 {}),
                ),
                ProtoFdbTupleSchemaElementEnumV1::ListOfBoolean(ProtoBooleanV1 {}) => Ok(
                    FdbTupleSchemaElementInternalEnumV1::ListOfBoolean(ProtoBooleanV1 {}),
                ),
                ProtoFdbTupleSchemaElementEnumV1::ListOfUuid(ProtoUuidV1 {}) => Ok(
                    FdbTupleSchemaElementInternalEnumV1::ListOfUuid(ProtoUuidV1 {}),
                ),
                ProtoFdbTupleSchemaElementEnumV1::ListOfVersionstamp(ProtoVersionstampV1 {}) => Ok(
                    FdbTupleSchemaElementInternalEnumV1::ListOfVersionstamp(ProtoVersionstampV1 {}),
                ),
            }
        }
    }

    impl From<FdbTupleSchemaElementInternalEnumV1> for ProtoFdbTupleSchemaElementEnumV1 {
        fn from(
            fdb_tuple_schema_element_internal_enum_v1: FdbTupleSchemaElementInternalEnumV1,
        ) -> ProtoFdbTupleSchemaElementEnumV1 {
            match fdb_tuple_schema_element_internal_enum_v1 {
                FdbTupleSchemaElementInternalEnumV1::Null(ProtoNullV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::Null(ProtoNullV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::Bytes(ProtoBytesV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::Bytes(ProtoBytesV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::String(ProtoStringV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::String(ProtoStringV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::Tuple(fdb_tupleschema_internal_v1) => {
                    ProtoFdbTupleSchemaElementEnumV1::Tuple(ProtoFdbTupleSchemaV1::from(
                        fdb_tupleschema_internal_v1,
                    ))
                }
                FdbTupleSchemaElementInternalEnumV1::Integer(ProtoIntegerV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::Integer(ProtoIntegerV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::Float(ProtoFloatV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::Float(ProtoFloatV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::Double(ProtoDoubleV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::Double(ProtoDoubleV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::Boolean(ProtoBooleanV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::Boolean(ProtoBooleanV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::Uuid(ProtoUuidV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::Uuid(ProtoUuidV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::Versionstamp(ProtoVersionstampV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::Versionstamp(ProtoVersionstampV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::MaybeBytes(ProtoBytesV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::MaybeBytes(ProtoBytesV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::MaybeString(ProtoStringV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::MaybeString(ProtoStringV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::MaybeTuple(fdb_tupleschema_internal_v1) => {
                    ProtoFdbTupleSchemaElementEnumV1::MaybeTuple(ProtoFdbTupleSchemaV1::from(
                        fdb_tupleschema_internal_v1,
                    ))
                }
                FdbTupleSchemaElementInternalEnumV1::MaybeInteger(ProtoIntegerV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::MaybeInteger(ProtoIntegerV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::MaybeFloat(ProtoFloatV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::MaybeFloat(ProtoFloatV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::MaybeDouble(ProtoDoubleV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::MaybeDouble(ProtoDoubleV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::MaybeBoolean(ProtoBooleanV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::MaybeBoolean(ProtoBooleanV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::MaybeUuid(ProtoUuidV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::MaybeUuid(ProtoUuidV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::MaybeVersionstamp(ProtoVersionstampV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::MaybeVersionstamp(ProtoVersionstampV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::ListOfBytes(ProtoBytesV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::ListOfBytes(ProtoBytesV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::ListOfString(ProtoStringV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::ListOfString(ProtoStringV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::ListOfTuple(fdb_tupleschema_internal_v1) => {
                    ProtoFdbTupleSchemaElementEnumV1::ListOfTuple(ProtoFdbTupleSchemaV1::from(
                        fdb_tupleschema_internal_v1,
                    ))
                }
                FdbTupleSchemaElementInternalEnumV1::ListOfInteger(ProtoIntegerV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::ListOfInteger(ProtoIntegerV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::ListOfFloat(ProtoFloatV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::ListOfFloat(ProtoFloatV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::ListOfDouble(ProtoDoubleV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::ListOfDouble(ProtoDoubleV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::ListOfBoolean(ProtoBooleanV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::ListOfBoolean(ProtoBooleanV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::ListOfUuid(ProtoUuidV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::ListOfUuid(ProtoUuidV1 {})
                }
                FdbTupleSchemaElementInternalEnumV1::ListOfVersionstamp(ProtoVersionstampV1 {}) => {
                    ProtoFdbTupleSchemaElementEnumV1::ListOfVersionstamp(ProtoVersionstampV1 {})
                }
            }
        }
    }

    /// Protobuf message
    /// `fdb_rl.fdb_tuple_schema.v1.FdbTupleSchemaElement` contains a
    /// `Required` field. So, we need to define this type.
    #[derive(Clone, Debug, PartialEq)]
    pub(crate) struct FdbTupleSchemaElementInternalV1 {
        pub(crate) fdb_tuple_schema_element: FdbTupleSchemaElementInternalEnumV1,
    }

    impl TryFrom<ProtoFdbTupleSchemaElementV1> for FdbTupleSchemaElementInternalV1 {
        type Error = FdbError;

        fn try_from(
            proto_fdb_tuple_schema_element_v1: ProtoFdbTupleSchemaElementV1,
        ) -> FdbResult<FdbTupleSchemaElementInternalV1> {
            proto_fdb_tuple_schema_element_v1
                .fdb_tuple_schema_element
                .ok_or_else(|| FdbError::new(PROTOBUF_FDB_TUPLE_SCHEMA_INVALID_PROTO))
                .and_then(FdbTupleSchemaElementInternalEnumV1::try_from)
                .map(|fdb_tuple_schema_element| FdbTupleSchemaElementInternalV1 {
                    fdb_tuple_schema_element,
                })
        }
    }

    impl From<FdbTupleSchemaElementInternalV1> for ProtoFdbTupleSchemaElementV1 {
        fn from(
            fdb_tuple_schema_element_internal_v1: FdbTupleSchemaElementInternalV1,
        ) -> ProtoFdbTupleSchemaElementV1 {
            ProtoFdbTupleSchemaElementV1 {
                fdb_tuple_schema_element: Some(ProtoFdbTupleSchemaElementEnumV1::from(
                    fdb_tuple_schema_element_internal_v1.fdb_tuple_schema_element,
                )),
            }
        }
    }

    /// Protobuf message
    /// `fdb_rl.fdb_tuple_schema.v1.FdbTupleSchema` transitively via
    /// `fdb_rl.fdb_tuple_schema.v1.FdbTupleSchemaElement` contains a
    /// `Required` field. So we need to define this type.
    #[derive(Clone, Debug, PartialEq)]
    pub(crate) struct FdbTupleSchemaInternalV1 {
        pub(crate) fdb_tuple_schema_elements: Vec<FdbTupleSchemaElementInternalV1>,
    }

    impl TryFrom<ProtoFdbTupleSchemaV1> for FdbTupleSchemaInternalV1 {
        type Error = FdbError;

        fn try_from(
            proto_fdb_tuple_schema_v1: ProtoFdbTupleSchemaV1,
        ) -> FdbResult<FdbTupleSchemaInternalV1> {
            let mut fdb_tuple_schema_elements = Vec::new();

            for fdb_tuple_schema_element in proto_fdb_tuple_schema_v1.fdb_tuple_schema_elements {
                fdb_tuple_schema_elements.push(FdbTupleSchemaElementInternalV1::try_from(
                    fdb_tuple_schema_element,
                )?);
            }

            Ok(FdbTupleSchemaInternalV1 {
                fdb_tuple_schema_elements,
            })
        }
    }

    impl From<FdbTupleSchemaInternalV1> for ProtoFdbTupleSchemaV1 {
        fn from(fdb_tuple_schema_internal_v1: FdbTupleSchemaInternalV1) -> ProtoFdbTupleSchemaV1 {
            let fdb_tuple_schema_elements = fdb_tuple_schema_internal_v1
                .fdb_tuple_schema_elements
                .into_iter()
                .map(ProtoFdbTupleSchemaElementV1::from)
                .collect::<Vec<ProtoFdbTupleSchemaElementV1>>();

            ProtoFdbTupleSchemaV1 {
                fdb_tuple_schema_elements,
            }
        }
    }

    impl From<FdbTupleSchemaInternalV1> for TupleSchema {
        fn from(fdb_tuple_schema_internal_v1: FdbTupleSchemaInternalV1) -> TupleSchema {
            let mut tuple_schema = TupleSchema::new();

            for fdb_tuple_schema_element_internal_v1 in
                fdb_tuple_schema_internal_v1.fdb_tuple_schema_elements
            {
                let fdb_tuple_schema_element_internal_enum_v1 =
                    fdb_tuple_schema_element_internal_v1.fdb_tuple_schema_element;

                match fdb_tuple_schema_element_internal_enum_v1 {
                    FdbTupleSchemaElementInternalEnumV1::Null(ProtoNullV1 {}) => {
                        tuple_schema.push_back(TupleSchemaElement::Null)
                    }
                    FdbTupleSchemaElementInternalEnumV1::Bytes(ProtoBytesV1 {}) => {
                        tuple_schema.push_back(TupleSchemaElement::Bytes)
                    }
                    FdbTupleSchemaElementInternalEnumV1::String(ProtoStringV1 {}) => {
                        tuple_schema.push_back(TupleSchemaElement::String)
                    }
                    FdbTupleSchemaElementInternalEnumV1::Tuple(
                        inner_fdb_tuple_schema_internal_v1,
                    ) => tuple_schema.push_back(TupleSchemaElement::Tuple(
                        inner_fdb_tuple_schema_internal_v1.into(),
                    )),
                    FdbTupleSchemaElementInternalEnumV1::Integer(ProtoIntegerV1 {}) => {
                        tuple_schema.push_back(TupleSchemaElement::Integer)
                    }
                    FdbTupleSchemaElementInternalEnumV1::Float(ProtoFloatV1 {}) => {
                        tuple_schema.push_back(TupleSchemaElement::Float)
                    }
                    FdbTupleSchemaElementInternalEnumV1::Double(ProtoDoubleV1 {}) => {
                        tuple_schema.push_back(TupleSchemaElement::Double)
                    }
                    FdbTupleSchemaElementInternalEnumV1::Boolean(ProtoBooleanV1 {}) => {
                        tuple_schema.push_back(TupleSchemaElement::Boolean)
                    }
                    FdbTupleSchemaElementInternalEnumV1::Uuid(ProtoUuidV1 {}) => {
                        tuple_schema.push_back(TupleSchemaElement::Uuid)
                    }
                    FdbTupleSchemaElementInternalEnumV1::Versionstamp(ProtoVersionstampV1 {}) => {
                        tuple_schema.push_back(TupleSchemaElement::Versionstamp)
                    }
                    FdbTupleSchemaElementInternalEnumV1::MaybeBytes(ProtoBytesV1 {}) => {
                        tuple_schema.push_back(TupleSchemaElement::MaybeBytes)
                    }
                    FdbTupleSchemaElementInternalEnumV1::MaybeString(ProtoStringV1 {}) => {
                        tuple_schema.push_back(TupleSchemaElement::MaybeString)
                    }
                    FdbTupleSchemaElementInternalEnumV1::MaybeTuple(
                        inner_fdb_tuple_schema_internal_v1,
                    ) => tuple_schema.push_back(TupleSchemaElement::MaybeTuple(
                        inner_fdb_tuple_schema_internal_v1.into(),
                    )),
                    FdbTupleSchemaElementInternalEnumV1::MaybeInteger(ProtoIntegerV1 {}) => {
                        tuple_schema.push_back(TupleSchemaElement::MaybeInteger)
                    }
                    FdbTupleSchemaElementInternalEnumV1::MaybeFloat(ProtoFloatV1 {}) => {
                        tuple_schema.push_back(TupleSchemaElement::MaybeFloat)
                    }
                    FdbTupleSchemaElementInternalEnumV1::MaybeDouble(ProtoDoubleV1 {}) => {
                        tuple_schema.push_back(TupleSchemaElement::MaybeDouble)
                    }
                    FdbTupleSchemaElementInternalEnumV1::MaybeBoolean(ProtoBooleanV1 {}) => {
                        tuple_schema.push_back(TupleSchemaElement::MaybeBoolean)
                    }
                    FdbTupleSchemaElementInternalEnumV1::MaybeUuid(ProtoUuidV1 {}) => {
                        tuple_schema.push_back(TupleSchemaElement::MaybeUuid)
                    }
                    FdbTupleSchemaElementInternalEnumV1::MaybeVersionstamp(
                        ProtoVersionstampV1 {},
                    ) => tuple_schema.push_back(TupleSchemaElement::MaybeVersionstamp),
                    FdbTupleSchemaElementInternalEnumV1::ListOfBytes(ProtoBytesV1 {}) => {
                        tuple_schema.push_back(TupleSchemaElement::ListOfBytes)
                    }
                    FdbTupleSchemaElementInternalEnumV1::ListOfString(ProtoStringV1 {}) => {
                        tuple_schema.push_back(TupleSchemaElement::ListOfString)
                    }
                    FdbTupleSchemaElementInternalEnumV1::ListOfTuple(
                        inner_fdb_tuple_schema_internal_v1,
                    ) => tuple_schema.push_back(TupleSchemaElement::ListOfTuple(
                        inner_fdb_tuple_schema_internal_v1.into(),
                    )),
                    FdbTupleSchemaElementInternalEnumV1::ListOfInteger(ProtoIntegerV1 {}) => {
                        tuple_schema.push_back(TupleSchemaElement::ListOfInteger)
                    }
                    FdbTupleSchemaElementInternalEnumV1::ListOfFloat(ProtoFloatV1 {}) => {
                        tuple_schema.push_back(TupleSchemaElement::ListOfFloat)
                    }
                    FdbTupleSchemaElementInternalEnumV1::ListOfDouble(ProtoDoubleV1 {}) => {
                        tuple_schema.push_back(TupleSchemaElement::ListOfDouble)
                    }
                    FdbTupleSchemaElementInternalEnumV1::ListOfBoolean(ProtoBooleanV1 {}) => {
                        tuple_schema.push_back(TupleSchemaElement::ListOfBoolean)
                    }
                    FdbTupleSchemaElementInternalEnumV1::ListOfUuid(ProtoUuidV1 {}) => {
                        tuple_schema.push_back(TupleSchemaElement::ListOfUuid)
                    }
                    FdbTupleSchemaElementInternalEnumV1::ListOfVersionstamp(
                        ProtoVersionstampV1 {},
                    ) => tuple_schema.push_back(TupleSchemaElement::ListOfVersionstamp),
                }
            }

            tuple_schema
        }
    }

    impl From<TupleSchema> for FdbTupleSchemaInternalV1 {
        fn from(tuple_schema: TupleSchema) -> FdbTupleSchemaInternalV1 {
            let mut fdb_tuple_schema_elements: Vec<FdbTupleSchemaElementInternalV1> = Vec::new();

            for tuple_schema_element in tuple_schema {
                let fdb_tuple_schema_element = match tuple_schema_element {
                    TupleSchemaElement::Null => {
                        FdbTupleSchemaElementInternalEnumV1::Null(ProtoNullV1 {})
                    }
                    TupleSchemaElement::Bytes => {
                        FdbTupleSchemaElementInternalEnumV1::Bytes(ProtoBytesV1 {})
                    }
                    TupleSchemaElement::String => {
                        FdbTupleSchemaElementInternalEnumV1::String(ProtoStringV1 {})
                    }
                    TupleSchemaElement::Tuple(inner_tuple_schema) => {
                        FdbTupleSchemaElementInternalEnumV1::Tuple(inner_tuple_schema.into())
                    }
                    TupleSchemaElement::Integer => {
                        FdbTupleSchemaElementInternalEnumV1::Integer(ProtoIntegerV1 {})
                    }
                    TupleSchemaElement::Float => {
                        FdbTupleSchemaElementInternalEnumV1::Float(ProtoFloatV1 {})
                    }
                    TupleSchemaElement::Double => {
                        FdbTupleSchemaElementInternalEnumV1::Double(ProtoDoubleV1 {})
                    }
                    TupleSchemaElement::Boolean => {
                        FdbTupleSchemaElementInternalEnumV1::Boolean(ProtoBooleanV1 {})
                    }
                    TupleSchemaElement::Uuid => {
                        FdbTupleSchemaElementInternalEnumV1::Uuid(ProtoUuidV1 {})
                    }
                    TupleSchemaElement::Versionstamp => {
                        FdbTupleSchemaElementInternalEnumV1::Versionstamp(ProtoVersionstampV1 {})
                    }
                    TupleSchemaElement::MaybeBytes => {
                        FdbTupleSchemaElementInternalEnumV1::MaybeBytes(ProtoBytesV1 {})
                    }
                    TupleSchemaElement::MaybeString => {
                        FdbTupleSchemaElementInternalEnumV1::MaybeString(ProtoStringV1 {})
                    }
                    TupleSchemaElement::MaybeTuple(inner_tuple_schema) => {
                        FdbTupleSchemaElementInternalEnumV1::MaybeTuple(inner_tuple_schema.into())
                    }
                    TupleSchemaElement::MaybeInteger => {
                        FdbTupleSchemaElementInternalEnumV1::MaybeInteger(ProtoIntegerV1 {})
                    }
                    TupleSchemaElement::MaybeFloat => {
                        FdbTupleSchemaElementInternalEnumV1::MaybeFloat(ProtoFloatV1 {})
                    }
                    TupleSchemaElement::MaybeDouble => {
                        FdbTupleSchemaElementInternalEnumV1::MaybeDouble(ProtoDoubleV1 {})
                    }
                    TupleSchemaElement::MaybeBoolean => {
                        FdbTupleSchemaElementInternalEnumV1::MaybeBoolean(ProtoBooleanV1 {})
                    }
                    TupleSchemaElement::MaybeUuid => {
                        FdbTupleSchemaElementInternalEnumV1::MaybeUuid(ProtoUuidV1 {})
                    }
                    TupleSchemaElement::MaybeVersionstamp => {
                        FdbTupleSchemaElementInternalEnumV1::MaybeVersionstamp(
                            ProtoVersionstampV1 {},
                        )
                    }
                    TupleSchemaElement::ListOfBytes => {
                        FdbTupleSchemaElementInternalEnumV1::ListOfBytes(ProtoBytesV1 {})
                    }
                    TupleSchemaElement::ListOfString => {
                        FdbTupleSchemaElementInternalEnumV1::ListOfString(ProtoStringV1 {})
                    }
                    TupleSchemaElement::ListOfTuple(inner_tuple_schema) => {
                        FdbTupleSchemaElementInternalEnumV1::ListOfTuple(inner_tuple_schema.into())
                    }
                    TupleSchemaElement::ListOfInteger => {
                        FdbTupleSchemaElementInternalEnumV1::ListOfInteger(ProtoIntegerV1 {})
                    }
                    TupleSchemaElement::ListOfFloat => {
                        FdbTupleSchemaElementInternalEnumV1::ListOfFloat(ProtoFloatV1 {})
                    }
                    TupleSchemaElement::ListOfDouble => {
                        FdbTupleSchemaElementInternalEnumV1::ListOfDouble(ProtoDoubleV1 {})
                    }
                    TupleSchemaElement::ListOfBoolean => {
                        FdbTupleSchemaElementInternalEnumV1::ListOfBoolean(ProtoBooleanV1 {})
                    }
                    TupleSchemaElement::ListOfUuid => {
                        FdbTupleSchemaElementInternalEnumV1::ListOfUuid(ProtoUuidV1 {})
                    }
                    TupleSchemaElement::ListOfVersionstamp => {
                        FdbTupleSchemaElementInternalEnumV1::ListOfVersionstamp(
                            ProtoVersionstampV1 {},
                        )
                    }
                };

                fdb_tuple_schema_elements.push(FdbTupleSchemaElementInternalV1 {
                    fdb_tuple_schema_element,
                })
            }

            FdbTupleSchemaInternalV1 {
                fdb_tuple_schema_elements,
            }
        }
    }
}

/// Internal representation of `FdbTupleSchema`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FdbTupleSchemaInternal {
    V1(pb::FdbTupleSchemaInternalV1),
}

impl From<FdbTupleSchemaInternal> for TupleSchema {
    fn from(fdb_tuple_schema_internal: FdbTupleSchemaInternal) -> TupleSchema {
        match fdb_tuple_schema_internal {
            FdbTupleSchemaInternal::V1(pb_fdb_tuple_schema_internal_v1) => {
                TupleSchema::from(pb_fdb_tuple_schema_internal_v1)
            }
        }
    }
}

impl From<TupleSchema> for FdbTupleSchemaInternal {
    fn from(tuple_schema: TupleSchema) -> FdbTupleSchemaInternal {
        FdbTupleSchemaInternal::V1(pb::FdbTupleSchemaInternalV1::from(tuple_schema))
    }
}
