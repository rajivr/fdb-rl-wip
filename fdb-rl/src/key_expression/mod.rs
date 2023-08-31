//! Provides types for Key Expression.

pub(crate) mod error;
pub(crate) mod record_type_key_expression;
pub(crate) mod well_formed_message_descriptor;

use fdb::error::{FdbError, FdbResult};

use std::convert::TryFrom;

use error::{KEY_EXPRESSION_CONCAT_FIELDS_CANNOT_NEST, KEY_EXPRESSION_INVALID_VARIANT};

/// Protobuf types.
pub(crate) mod pb {
    use fdb::error::{FdbError, FdbResult};

    use std::convert::TryFrom;

    use super::error::KEY_EXPRESSION_INVALID_PROTO;

    /// Protobuf generated types renamed to prepend `Proto` and append
    /// version (and add `Enum` suffix).
    pub(crate) use fdb_rl_proto::fdb_rl::key_expression::v1::field::field_fan_type::{
        Concatenate as ProtoConcatenateV1, FanOut as ProtoFanOutV1,
        FieldFanType as ProtoFieldFanTypeEnumV1,
    };

    /// Protobuf generated types renamed to prepend `Proto` and append
    /// version (and add `Enum` suffix).
    pub(crate) use fdb_rl_proto::fdb_rl::key_expression::v1::field::field_null_interpretation::{
        Distinct as ProtoDistinctV1, FieldNullInterpretation as ProtoFieldNullInterpretationEnumV1,
        NotDistinct as ProtoNotDistinctV1, NotNull as ProtoNotNullV1,
    };

    /// Protobuf generated types renamed to prepend `Proto` and append
    /// version (and add `Enum` suffix).
    pub(crate) use fdb_rl_proto::fdb_rl::key_expression::v1::field::{
        FieldFanType as ProtoFieldFanTypeV1, FieldLabel as ProtoFieldLabelEnumV1,
        FieldNullInterpretation as ProtoFieldNullInterpretationV1,
    };

    /// Protobuf message `fdb_rl.key_expression.v1.Field.FieldFanType`
    /// contains a `Required` field. So, we need to define this type.
    ///
    /// We have conversion functions to and from
    /// [`ProtoFieldFanTypeV1`].
    #[derive(Clone, Debug, PartialEq)]
    pub(crate) struct FieldFanTypeInternalV1 {
        pub(crate) field_fan_type: ProtoFieldFanTypeEnumV1,
    }

    impl TryFrom<ProtoFieldFanTypeV1> for FieldFanTypeInternalV1 {
        type Error = FdbError;

        fn try_from(
            proto_field_fan_type_v1: ProtoFieldFanTypeV1,
        ) -> FdbResult<FieldFanTypeInternalV1> {
            proto_field_fan_type_v1
                .field_fan_type
                .ok_or_else(|| FdbError::new(KEY_EXPRESSION_INVALID_PROTO))
                .map(|field_fan_type| FieldFanTypeInternalV1 { field_fan_type })
        }
    }

    impl From<FieldFanTypeInternalV1> for ProtoFieldFanTypeV1 {
        fn from(field_fan_type_internal_v1: FieldFanTypeInternalV1) -> ProtoFieldFanTypeV1 {
            ProtoFieldFanTypeV1 {
                field_fan_type: Some(field_fan_type_internal_v1.field_fan_type),
            }
        }
    }

    /// Protobuf message `fdb_rl.key_expression.v1.Field.field_label`
    /// enum indirectly contains a `Required` field (via
    /// `FieldNullInterpretation` and `FieldFanType`. So, we need to
    /// define this type.
    ///
    /// We have conversion functions to and from
    /// [`ProtoFieldLabelEnumV1`].
    #[derive(Clone, Debug, PartialEq)]
    pub(crate) enum FieldLabelInternalEnumV1 {
        NullInterpretation(FieldNullInterpretationInternalV1),
        FanType(FieldFanTypeInternalV1),
    }

    impl TryFrom<ProtoFieldLabelEnumV1> for FieldLabelInternalEnumV1 {
        type Error = FdbError;

        fn try_from(
            proto_field_label_enum_v1: ProtoFieldLabelEnumV1,
        ) -> FdbResult<FieldLabelInternalEnumV1> {
            match proto_field_label_enum_v1 {
                ProtoFieldLabelEnumV1::NullInterpretation(proto_field_null_interpretation_v1) => {
                    FieldNullInterpretationInternalV1::try_from(proto_field_null_interpretation_v1)
                        .map(FieldLabelInternalEnumV1::NullInterpretation)
                }
                ProtoFieldLabelEnumV1::FanType(proto_field_fan_type_v1) => {
                    FieldFanTypeInternalV1::try_from(proto_field_fan_type_v1)
                        .map(FieldLabelInternalEnumV1::FanType)
                }
            }
        }
    }

    impl From<FieldLabelInternalEnumV1> for ProtoFieldLabelEnumV1 {
        fn from(field_label_internal_enum_v1: FieldLabelInternalEnumV1) -> ProtoFieldLabelEnumV1 {
            match field_label_internal_enum_v1 {
                FieldLabelInternalEnumV1::NullInterpretation(
                    field_null_interpretation_internal_v1,
                ) => ProtoFieldLabelEnumV1::NullInterpretation(
                    ProtoFieldNullInterpretationV1::from(field_null_interpretation_internal_v1),
                ),
                FieldLabelInternalEnumV1::FanType(field_fan_type_internal_v1) => {
                    ProtoFieldLabelEnumV1::FanType(ProtoFieldFanTypeV1::from(
                        field_fan_type_internal_v1,
                    ))
                }
            }
        }
    }

    /// Protobuf message
    /// `fdb_rl.key_expression.v1.Field.FieldNullInterpretation`
    /// contains a `Required` field. So, we need to define this type.
    #[derive(Clone, Debug, PartialEq)]
    pub(crate) struct FieldNullInterpretationInternalV1 {
        pub(crate) field_null_interpretation: ProtoFieldNullInterpretationEnumV1,
    }

    impl TryFrom<ProtoFieldNullInterpretationV1> for FieldNullInterpretationInternalV1 {
        type Error = FdbError;

        fn try_from(
            proto_field_null_interpretation_v1: ProtoFieldNullInterpretationV1,
        ) -> FdbResult<FieldNullInterpretationInternalV1> {
            proto_field_null_interpretation_v1
                .field_null_interpretation
                .ok_or_else(|| FdbError::new(KEY_EXPRESSION_INVALID_PROTO))
                .map(
                    |field_null_interpretation| FieldNullInterpretationInternalV1 {
                        field_null_interpretation,
                    },
                )
        }
    }

    impl From<FieldNullInterpretationInternalV1> for ProtoFieldNullInterpretationV1 {
        fn from(
            field_null_interpretation_internal_v1: FieldNullInterpretationInternalV1,
        ) -> ProtoFieldNullInterpretationV1 {
            ProtoFieldNullInterpretationV1 {
                field_null_interpretation: Some(
                    field_null_interpretation_internal_v1.field_null_interpretation,
                ),
            }
        }
    }

    /// Protobuf generated types renamed to prepend `Proto` and append
    /// version (and add `Enum` suffix).
    pub(crate) use fdb_rl_proto::fdb_rl::key_expression::v1::key_expression::KeyExpression as ProtoKeyExpressionEnumV1;

    /// Protobuf oneof
    /// `fdb_rl.key_expression.v1.KeyExpression.key_expression` uses
    /// protos that contain a `Required` field. So, we need to define
    /// this type.
    #[derive(Clone, Debug, PartialEq)]
    pub(crate) enum KeyExpressionInternalEnumV1 {
        Field(FieldInternalV1),
        Nest(Box<NestInternalV1>),
        Concat(ConcatInternalV1),
    }

    impl TryFrom<ProtoKeyExpressionEnumV1> for KeyExpressionInternalEnumV1 {
        type Error = FdbError;

        fn try_from(
            proto_key_expression_enum_v1: ProtoKeyExpressionEnumV1,
        ) -> FdbResult<KeyExpressionInternalEnumV1> {
            match proto_key_expression_enum_v1 {
                ProtoKeyExpressionEnumV1::Field(proto_field_v1) => {
                    FieldInternalV1::try_from(proto_field_v1)
                        .map(KeyExpressionInternalEnumV1::Field)
                }
                ProtoKeyExpressionEnumV1::Nest(boxed_proto_nest_v1) => {
                    NestInternalV1::try_from(*boxed_proto_nest_v1)
                        .map(Box::new)
                        .map(KeyExpressionInternalEnumV1::Nest)
                }
                ProtoKeyExpressionEnumV1::Concat(proto_concat_v1) => {
                    ConcatInternalV1::try_from(proto_concat_v1)
                        .map(KeyExpressionInternalEnumV1::Concat)
                }
            }
        }
    }

    impl From<KeyExpressionInternalEnumV1> for ProtoKeyExpressionEnumV1 {
        fn from(
            key_expression_internal_enum_v1: KeyExpressionInternalEnumV1,
        ) -> ProtoKeyExpressionEnumV1 {
            match key_expression_internal_enum_v1 {
                KeyExpressionInternalEnumV1::Field(field_internal_v1) => {
                    ProtoKeyExpressionEnumV1::Field(ProtoFieldV1::from(field_internal_v1))
                }
                KeyExpressionInternalEnumV1::Nest(boxed_nest_internal_v1) => {
                    ProtoKeyExpressionEnumV1::Nest(Box::new(ProtoNestV1::from(
                        *boxed_nest_internal_v1,
                    )))
                }
                KeyExpressionInternalEnumV1::Concat(concat_internal_v1) => {
                    ProtoKeyExpressionEnumV1::Concat(ProtoConcatV1::from(concat_internal_v1))
                }
            }
        }
    }

    /// Protobuf generated types renamed to prepend `Proto` and append
    /// version.
    pub(crate) use fdb_rl_proto::fdb_rl::key_expression::v1::{
        Concat as ProtoConcatV1, Field as ProtoFieldV1, KeyExpression as ProtoKeyExpressionV1,
        Nest as ProtoNestV1,
    };

    /// Protobuf message `fdb_rl.key_expression.v1.Field`
    /// contains a `Required` field. So, we need to define this type.
    #[derive(Clone, Debug, PartialEq)]
    pub(crate) struct FieldInternalV1 {
        pub(crate) field_number: u32,
        pub(crate) field_label: FieldLabelInternalEnumV1,
    }

    impl TryFrom<ProtoFieldV1> for FieldInternalV1 {
        type Error = FdbError;

        fn try_from(proto_field_v1: ProtoFieldV1) -> FdbResult<FieldInternalV1> {
            let ProtoFieldV1 {
                field_number,
                field_label,
            } = proto_field_v1;

            field_number
                .and_then(|field_number| {
                    field_label
                        .and_then(|proto_field_label_enumv1| {
                            FieldLabelInternalEnumV1::try_from(proto_field_label_enumv1).ok()
                        })
                        .map(|field_label| (field_number, field_label))
                })
                .map(|(field_number, field_label)| FieldInternalV1 {
                    field_number,
                    field_label,
                })
                .ok_or_else(|| FdbError::new(KEY_EXPRESSION_INVALID_PROTO))
        }
    }

    impl From<FieldInternalV1> for ProtoFieldV1 {
        fn from(field_internal_v1: FieldInternalV1) -> ProtoFieldV1 {
            let FieldInternalV1 {
                field_number,
                field_label,
            } = field_internal_v1;

            ProtoFieldV1 {
                field_number: Some(field_number),
                field_label: Some(field_label.into()),
            }
        }
    }

    /// Protobuf message `fdb_rl.key_expression.v1.Nest` contains a
    /// `Required` field. So, we need to define this type.
    #[derive(Clone, Debug, PartialEq)]
    pub(crate) struct NestInternalV1 {
        pub(crate) parent: FieldInternalV1,
        pub(crate) child: Box<KeyExpressionInternalV1>,
    }

    impl TryFrom<ProtoNestV1> for NestInternalV1 {
        type Error = FdbError;

        fn try_from(proto_nest_v1: ProtoNestV1) -> FdbResult<NestInternalV1> {
            let ProtoNestV1 { parent, child } = proto_nest_v1;

            parent
                .and_then(|x| FieldInternalV1::try_from(x).ok())
                .and_then(|field_internal_v1| {
                    child.and_then(|b| {
                        KeyExpressionInternalV1::try_from(*b)
                            .ok()
                            .map(Box::new)
                            .map(|boxed_key_expression_internal_v1| {
                                (field_internal_v1, boxed_key_expression_internal_v1)
                            })
                    })
                })
                .and_then(|(parent, child)| {
                    // For a nested expression, the parent field
                    // *cannot* be
                    // `ProtoFieldFanTypeEnumV1::Concatenate(ProtoConcatenateV1
                    // {})`
                    //
                    // See `Field::try_nest(...)` for additional
                    // comments.
                    if matches!(
                        parent.field_label,
                        FieldLabelInternalEnumV1::FanType(FieldFanTypeInternalV1 {
                            field_fan_type: ProtoFieldFanTypeEnumV1::Concatenate(
                                ProtoConcatenateV1 {}
                            )
                        })
                    ) {
                        None
                    } else {
                        Some(NestInternalV1 { parent, child })
                    }
                })
                .ok_or_else(|| FdbError::new(KEY_EXPRESSION_INVALID_PROTO))
        }
    }

    impl From<NestInternalV1> for ProtoNestV1 {
        fn from(nest_internal_v1: NestInternalV1) -> ProtoNestV1 {
            let NestInternalV1 { parent, child } = nest_internal_v1;

            ProtoNestV1 {
                parent: Some(ProtoFieldV1::from(parent)),
                child: Some(Box::new(ProtoKeyExpressionV1::from(*child))),
            }
        }
    }

    /// Protobuf message `fdb_rl.key_expression.v1.Concat` uses protos
    /// that contain a `Required` field. So, we need to define this
    /// type.
    #[derive(Clone, Debug, PartialEq)]
    pub(crate) struct ConcatInternalV1 {
        pub(crate) children: Vec<KeyExpressionInternalV1>,
    }

    impl TryFrom<ProtoConcatV1> for ConcatInternalV1 {
        type Error = FdbError;

        fn try_from(proto_concat_v1: ProtoConcatV1) -> FdbResult<ConcatInternalV1> {
            let ProtoConcatV1 { children } = proto_concat_v1;

            let mut c = Vec::with_capacity(children.len());

            let mut loop_err = false;

            for x in children {
                match KeyExpressionInternalV1::try_from(x) {
                    Ok(key_expression_internal_v1) => c.push(key_expression_internal_v1),
                    Err(_) => {
                        loop_err = true;
                        break;
                    }
                }
            }

            if loop_err {
                Err(FdbError::new(KEY_EXPRESSION_INVALID_PROTO))
            } else {
                Ok(ConcatInternalV1 { children: c })
            }
        }
    }

    impl From<ConcatInternalV1> for ProtoConcatV1 {
        fn from(concat_internal_v1: ConcatInternalV1) -> ProtoConcatV1 {
            let ConcatInternalV1 { children } = concat_internal_v1;

            let c = children
                .into_iter()
                .map(ProtoKeyExpressionV1::from)
                .collect::<Vec<ProtoKeyExpressionV1>>();

            ProtoConcatV1 { children: c }
        }
    }

    /// Protobuf message `fdb_rl.key_expression.v1.KeyExpression`
    /// contains a `Required` field. So, we need to define this type.
    #[derive(Clone, Debug, PartialEq)]
    pub(crate) struct KeyExpressionInternalV1 {
        pub(crate) key_expression: KeyExpressionInternalEnumV1,
    }

    impl TryFrom<ProtoKeyExpressionV1> for KeyExpressionInternalV1 {
        type Error = FdbError;

        fn try_from(
            proto_key_expression_v1: ProtoKeyExpressionV1,
        ) -> FdbResult<KeyExpressionInternalV1> {
            proto_key_expression_v1
                .key_expression
                .ok_or_else(|| FdbError::new(KEY_EXPRESSION_INVALID_PROTO))
                .and_then(KeyExpressionInternalEnumV1::try_from)
                .map(|key_expression| KeyExpressionInternalV1 { key_expression })
        }
    }

    impl From<KeyExpressionInternalV1> for ProtoKeyExpressionV1 {
        fn from(key_expression_internal_v1: KeyExpressionInternalV1) -> ProtoKeyExpressionV1 {
            ProtoKeyExpressionV1 {
                key_expression: Some(ProtoKeyExpressionEnumV1::from(
                    key_expression_internal_v1.key_expression,
                )),
            }
        }
    }
}

/// Internal representation of [`KeyExpression`].
///
/// If we need to introduce a new version of `key_expression.proto`,
/// we can handle it here.
///
/// We currently do not directly seralize [`KeyExpressionInternal`],
/// so we do not have `<Bytes as KeyExpressionInternal>::try_from` and
/// `<KeyExpressionInternal as Bytes>::try_from` impls.
///
/// Even though there is `From` trait implementation to convert
/// `pb::*` types to [`KeyExpression`], they are not meant to be
/// directly used. All protobuf access **must** be proxied using this
/// type.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum KeyExpressionInternal {
    V1(pb::KeyExpressionInternalV1),
}

impl From<KeyExpression> for KeyExpressionInternal {
    fn from(key_expression: KeyExpression) -> KeyExpressionInternal {
        KeyExpressionInternal::V1(pb::KeyExpressionInternalV1::from(key_expression))
    }
}

impl From<KeyExpressionInternal> for KeyExpression {
    fn from(key_expression_internal: KeyExpressionInternal) -> KeyExpression {
        match key_expression_internal {
            KeyExpressionInternal::V1(pb_key_expression_internal_v1) => {
                KeyExpression::from(pb_key_expression_internal_v1)
            }
        }
    }
}

/// How to handle repeated field.
pub(crate) enum FieldFanType {
    /// Create an index value for each value of the field.
    FanOut,
    /// Convert a repeated field into a single list.
    ///
    /// This does not cause the number of index values to increase.
    Concatenate,
}

impl From<pb::FieldFanTypeInternalV1> for FieldFanType {
    fn from(pb_field_fan_type_internal_v1: pb::FieldFanTypeInternalV1) -> FieldFanType {
        match pb_field_fan_type_internal_v1.field_fan_type {
            pb::ProtoFieldFanTypeEnumV1::FanOut(pb::ProtoFanOutV1 {}) => FieldFanType::FanOut,
            pb::ProtoFieldFanTypeEnumV1::Concatenate(pb::ProtoConcatenateV1 {}) => {
                FieldFanType::Concatenate
            }
        }
    }
}

impl From<FieldFanType> for pb::FieldFanTypeInternalV1 {
    fn from(field_fan_type: FieldFanType) -> pb::FieldFanTypeInternalV1 {
        let field_fan_type = match field_fan_type {
            FieldFanType::FanOut => pb::ProtoFieldFanTypeEnumV1::FanOut(pb::ProtoFanOutV1 {}),
            FieldFanType::Concatenate => {
                pb::ProtoFieldFanTypeEnumV1::Concatenate(pb::ProtoConcatenateV1 {})
            }
        };

        pb::FieldFanTypeInternalV1 { field_fan_type }
    }
}

/// How to interpret missing value (`NULL`) or assert that value must
/// always be present.
pub(crate) enum FieldNullInterpretation {
    /// Missing value (`NULL`) are allowed multiple times in unique
    /// index (PostgreSQL `NULLS DISTINCT`, Java RecordLayer
    /// `NOT_UNIQUE`).
    ///
    /// This is the default for PostgreSQL and Java RecordLayer.
    Distinct,
    /// Missing value (`NULL`) *cannot* be repeated in a unique
    /// index. This is very restrictive as a column can have only one
    /// `NULL` value. (PostgreSQL `NULLS NOT DISTINCT`, Java
    /// RecordLayer `UNIQUE`).
    NotDistinct,
    /// Field is not `NULL`.
    NotNull,
}

impl From<pb::FieldNullInterpretationInternalV1> for FieldNullInterpretation {
    fn from(
        pb_field_null_interpretation_internal_v1: pb::FieldNullInterpretationInternalV1,
    ) -> FieldNullInterpretation {
        match pb_field_null_interpretation_internal_v1.field_null_interpretation {
            pb::ProtoFieldNullInterpretationEnumV1::Distinct(pb::ProtoDistinctV1 {}) => {
                FieldNullInterpretation::Distinct
            }
            pb::ProtoFieldNullInterpretationEnumV1::NotDistinct(pb::ProtoNotDistinctV1 {}) => {
                FieldNullInterpretation::NotDistinct
            }
            pb::ProtoFieldNullInterpretationEnumV1::NotNull(pb::ProtoNotNullV1 {}) => {
                FieldNullInterpretation::NotNull
            }
        }
    }
}

impl From<FieldNullInterpretation> for pb::FieldNullInterpretationInternalV1 {
    fn from(
        field_null_interpretation: FieldNullInterpretation,
    ) -> pb::FieldNullInterpretationInternalV1 {
        let field_null_interpretation = match field_null_interpretation {
            FieldNullInterpretation::Distinct => {
                pb::ProtoFieldNullInterpretationEnumV1::Distinct(pb::ProtoDistinctV1 {})
            }
            FieldNullInterpretation::NotDistinct => {
                pb::ProtoFieldNullInterpretationEnumV1::NotDistinct(pb::ProtoNotDistinctV1 {})
            }
            FieldNullInterpretation::NotNull => {
                pb::ProtoFieldNullInterpretationEnumV1::NotNull(pb::ProtoNotNullV1 {})
            }
        };

        pb::FieldNullInterpretationInternalV1 {
            field_null_interpretation,
        }
    }
}

/// TODO
pub(crate) enum FieldLabel {
    NullInterpretation(FieldNullInterpretation),
    FanType(FieldFanType),
}

impl From<pb::FieldLabelInternalEnumV1> for FieldLabel {
    fn from(pb_field_label_internal_enumv1: pb::FieldLabelInternalEnumV1) -> FieldLabel {
        match pb_field_label_internal_enumv1 {
            pb::FieldLabelInternalEnumV1::NullInterpretation(
                pb_field_null_interpretation_internal_v1,
            ) => FieldLabel::NullInterpretation(FieldNullInterpretation::from(
                pb_field_null_interpretation_internal_v1,
            )),
            pb::FieldLabelInternalEnumV1::FanType(pb_field_fantype_internal_v1) => {
                FieldLabel::FanType(FieldFanType::from(pb_field_fantype_internal_v1))
            }
        }
    }
}

impl From<FieldLabel> for pb::FieldLabelInternalEnumV1 {
    fn from(field_label: FieldLabel) -> pb::FieldLabelInternalEnumV1 {
        match field_label {
            FieldLabel::NullInterpretation(field_null_interpretation) => {
                pb::FieldLabelInternalEnumV1::NullInterpretation(
                    pb::FieldNullInterpretationInternalV1::from(field_null_interpretation),
                )
            }
            FieldLabel::FanType(field_fan_type) => pb::FieldLabelInternalEnumV1::FanType(
                pb::FieldFanTypeInternalV1::from(field_fan_type),
            ),
        }
    }
}

/// TODO
pub(crate) struct Field {
    /// TODO
    field_number: u32,
    /// TODO
    field_label: FieldLabel,
}

impl Field {
    /// TODO
    //
    // For a nested expression, the parent field *cannot*
    // be`FieldFanType::Concatenate`.
    //
    // In Java RecordLayer, there is a comment [1] that says it is
    // possible to allow `FieldFanType::Concatenate`. We can revisit
    // this once it is implemented upstream.
    //
    // [1]: https://github.com/FoundationDB/fdb-record-layer/blob/3.3.406.0/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/metadata/expressions/FieldKeyExpression.java#L284-L286
    pub(crate) fn try_nest(self, child: KeyExpression) -> FdbResult<Nest> {
        if matches!(
            self.field_label,
            FieldLabel::FanType(FieldFanType::Concatenate)
        ) {
            Err(FdbError::new(KEY_EXPRESSION_CONCAT_FIELDS_CANNOT_NEST))
        } else {
            Ok(Nest {
                parent: self,
                child: Box::new(child),
            })
        }
    }
}

impl From<pb::FieldInternalV1> for Field {
    fn from(pb_field_internal_v1: pb::FieldInternalV1) -> Field {
        let pb::FieldInternalV1 {
            field_number,
            field_label,
        } = pb_field_internal_v1;

        Field {
            field_number,
            field_label: FieldLabel::from(field_label),
        }
    }
}

impl From<Field> for pb::FieldInternalV1 {
    fn from(field: Field) -> pb::FieldInternalV1 {
        let Field {
            field_number,
            field_label,
        } = field;

        pb::FieldInternalV1 {
            field_number,
            field_label: pb::FieldLabelInternalEnumV1::from(field_label),
        }
    }
}

/// TODO
pub(crate) struct Nest {
    parent: Field,
    child: Box<KeyExpression>,
}

impl From<pb::NestInternalV1> for Nest {
    fn from(pb_nest_internal_v1: pb::NestInternalV1) -> Nest {
        let pb::NestInternalV1 { parent, child } = pb_nest_internal_v1;

        Nest {
            parent: Field::from(parent),
            child: Box::new(KeyExpression::from(*child)),
        }
    }
}

impl From<Nest> for pb::NestInternalV1 {
    fn from(nest: Nest) -> pb::NestInternalV1 {
        let Nest { parent, child } = nest;

        pb::NestInternalV1 {
            parent: pb::FieldInternalV1::from(parent),
            child: Box::new(pb::KeyExpressionInternalV1::from(*child)),
        }
    }
}

/// TODO
pub(crate) struct Concat {
    children: Vec<KeyExpression>,
}

impl From<pb::ConcatInternalV1> for Concat {
    fn from(pb_concat_internal_v1: pb::ConcatInternalV1) -> Concat {
        let pb::ConcatInternalV1 { children } = pb_concat_internal_v1;

        Concat {
            children: children
                .into_iter()
                .map(KeyExpression::from)
                .collect::<Vec<KeyExpression>>(),
        }
    }
}

impl From<Concat> for pb::ConcatInternalV1 {
    fn from(concat: Concat) -> pb::ConcatInternalV1 {
        let Concat { children } = concat;

        pb::ConcatInternalV1 {
            children: children
                .into_iter()
                .map(pb::KeyExpressionInternalV1::from)
                .collect::<Vec<pb::KeyExpressionInternalV1>>(),
        }
    }
}

/// TODO
pub(crate) enum KeyExpression {
    /// TODO
    Field(Field),
    /// TODO
    Nest(Box<Nest>),
    /// TODO
    Concat(Concat),
}

impl From<pb::KeyExpressionInternalV1> for KeyExpression {
    fn from(pb_key_expression_internal_v1: pb::KeyExpressionInternalV1) -> KeyExpression {
        match pb_key_expression_internal_v1.key_expression {
            pb::KeyExpressionInternalEnumV1::Field(pb_field_internal_v1) => {
                KeyExpression::Field(Field::from(pb_field_internal_v1))
            }
            pb::KeyExpressionInternalEnumV1::Nest(boxed_pb_nest_internal_v1) => {
                KeyExpression::Nest(Box::new(Nest::from(*boxed_pb_nest_internal_v1)))
            }
            pb::KeyExpressionInternalEnumV1::Concat(pb_concat_internal_v1) => {
                KeyExpression::Concat(Concat::from(pb_concat_internal_v1))
            }
        }
    }
}

impl From<KeyExpression> for pb::KeyExpressionInternalV1 {
    fn from(key_expression: KeyExpression) -> pb::KeyExpressionInternalV1 {
        let key_expression = match key_expression {
            KeyExpression::Field(field) => {
                pb::KeyExpressionInternalEnumV1::Field(pb::FieldInternalV1::from(field))
            }
            KeyExpression::Nest(boxed_nest) => pb::KeyExpressionInternalEnumV1::Nest(Box::new(
                pb::NestInternalV1::from(*boxed_nest),
            )),
            KeyExpression::Concat(concat) => {
                pb::KeyExpressionInternalEnumV1::Concat(pb::ConcatInternalV1::from(concat))
            }
        };

        pb::KeyExpressionInternalV1 { key_expression }
    }
}

impl From<Field> for KeyExpression {
    fn from(field: Field) -> KeyExpression {
        KeyExpression::Field(field)
    }
}

impl TryFrom<KeyExpression> for Field {
    type Error = FdbError;

    fn try_from(key_expression: KeyExpression) -> FdbResult<Field> {
        if let KeyExpression::Field(field) = key_expression {
            Ok(field)
        } else {
            Err(FdbError::new(KEY_EXPRESSION_INVALID_VARIANT))
        }
    }
}

impl From<Nest> for KeyExpression {
    fn from(nest: Nest) -> KeyExpression {
        KeyExpression::Nest(Box::new(nest))
    }
}

impl TryFrom<KeyExpression> for Nest {
    type Error = FdbError;

    fn try_from(key_expression: KeyExpression) -> FdbResult<Nest> {
        if let KeyExpression::Nest(boxed_nest) = key_expression {
            Ok(*boxed_nest)
        } else {
            Err(FdbError::new(KEY_EXPRESSION_INVALID_VARIANT))
        }
    }
}

impl From<Concat> for KeyExpression {
    fn from(concat: Concat) -> KeyExpression {
        KeyExpression::Concat(concat)
    }
}

impl TryFrom<KeyExpression> for Concat {
    type Error = FdbError;

    fn try_from(key_expression: KeyExpression) -> FdbResult<Concat> {
        if let KeyExpression::Concat(concat) = key_expression {
            Ok(concat)
        } else {
            Err(FdbError::new(KEY_EXPRESSION_INVALID_VARIANT))
        }
    }
}

// TODO: Semi-public apis to generate key expressions.

/// TODO
pub(crate) fn field(field_number: u32, field_label: FieldLabel) -> Field {
    Field {
        field_number,
        field_label,
    }
}

/// TODO
pub(crate) fn concat(children: Vec<KeyExpression>) -> Concat {
    Concat { children }
}
