//! TODO

use fdb::error::{FdbError, FdbResult};

use std::convert::TryFrom;

use super::error::{METADATA_INVALID_INDEX_SCHEMA, METADATA_INVALID_PRIMARY_KEY_SCHEMA};

/// Represents the elements that a [`PrimaryKeySchema`] may contain.
#[derive(Debug, Clone, PartialEq)]
pub enum PrimaryKeySchemaElement {
    /// [`String`] value
    String,
    /// [`f64`] value
    Double,
    /// [`f32`] value
    Float,
    /// Integer value
    ///
    /// ## Note
    ///
    /// Integer value can be a [`i8`], [`i16`], [`i32`], [`i64`].
    ///
    /// Even though FDB Tuple Layer supports [`BigInt`], there is no
    /// Protobuf primitive [`BigInt`] type.
    ///
    /// [`BigInt`]: num_bigint::BigInt
    Integer,
    /// [`bool`] value
    Boolean,
    /// [`Bytes`] value
    ///
    /// [`Bytes`]: bytes::Bytes
    Bytes,
    /// [`Uuid`] value
    ///
    /// [`Uuid`]: uuid::Uuid
    Uuid,
}

/// Represents the schema for primary key.
//
// When we have a value of type `PrimaryKeySchema`, we can assume that
// it represents a well-formed `PrimaryKeySchema`.
#[derive(Debug, Clone, PartialEq)]
pub struct PrimaryKeySchema {
    key_schema: Vec<PrimaryKeySchemaElement>,
}

impl PrimaryKeySchema {
    pub(crate) fn into_part(self) -> Vec<PrimaryKeySchemaElement> {
        let PrimaryKeySchema { key_schema } = self;

        key_schema
    }
}

impl TryFrom<Vec<PrimaryKeySchemaElement>> for PrimaryKeySchema {
    type Error = FdbError;

    fn try_from(
        primary_key_schema_elements: Vec<PrimaryKeySchemaElement>,
    ) -> FdbResult<PrimaryKeySchema> {
        if primary_key_schema_elements.len() == 0 {
            Err(FdbError::new(METADATA_INVALID_PRIMARY_KEY_SCHEMA))
        } else {
            Ok(PrimaryKeySchema {
                key_schema: primary_key_schema_elements,
            })
        }
    }
}

/// Represents the elements that a [`IndexSchema`] may contain.
#[derive(Debug, Clone, PartialEq)]
pub enum IndexSchemaElement {
    /// [`String`] value
    String,
    /// Optional [`String`] value
    MaybeString,
    /// [`f64`] value
    Double,
    /// Optional [`f64`] value
    MaybeDouble,
    /// [`f32`] value
    Float,
    /// Optional [`f32`] value
    MaybeFloat,
    /// Integer value
    ///
    /// ## Note
    ///
    /// Integer value can be a [`i8`], [`i16`], [`i32`], [`i64`].
    ///
    /// Even though FDB Tuple Layer supports [`BigInt`], there is no
    /// Protobuf primitive [`BigInt`] type.
    ///
    /// [`BigInt`]: num_bigint::BigInt
    Integer,
    /// Optional integer value
    ///
    /// ## Note
    ///
    /// Integer value can be a [`i8`], [`i16`], [`i32`], [`i64`].
    /// [`BigInt`].
    ///
    /// Even though FDB Tuple Layer supports [`BigInt`], there is no
    /// Protobuf primitive [`BigInt`] type.
    ///
    /// [`BigInt`]: num_bigint::BigInt
    MaybeInteger,
    /// [`bool`] value
    Boolean,
    /// Optional [`bool`] value
    MaybeBoolean,
    /// [`Bytes`] value
    ///
    /// [`Bytes`]: bytes::Bytes
    Bytes,
    /// Optional [`Bytes`] value
    ///
    /// [`Bytes`]: bytes::Bytes
    MaybeBytes,
    /// [`Uuid`] value
    ///
    /// [`Uuid`]: uuid::Uuid
    Uuid,
    /// Optional [`Uuid`] value
    ///
    /// [`Uuid`]: uuid::Uuid
    MaybeUuid,
    /// [`Versionstamp`] value
    ///
    /// [`Versionstamp`]: crate::tuple::Versionstamp
    Versionstamp,
    /// List of [`String`] value
    ListOfString,
    /// List of [`f64`] value
    ListOfDouble,
    /// List of [`f32`] value
    ListOfFloat,
    /// List of integer value
    ///
    /// ## Note
    ///
    /// Integer value can be a [`i8`], [`i16`], [`i32`], [`i64`].
    /// [`BigInt`].
    ///
    /// Even though FDB Tuple Layer supports [`BigInt`], there is no
    /// Protobuf primitive [`BigInt`] type.
    ///
    /// [`BigInt`]: num_bigint::BigInt
    ListOfInteger,
    /// List of [`bool`] value
    ListOfBoolean,
    /// List of [`Bytes`] value
    ///
    /// [`Bytes`]: bytes::Bytes
    ListOfBytes,
    /// List of [`Uuid`] value
    ///
    /// [`Uuid`]: uuid::Uuid
    ListOfUuid,
}

/// Represents the schema for the key part of FDB key-value that
/// represents [`IndexSchema`].
//
// When we have a value of type `IndexSchemaKey`, we can assume that
// it represents a well formed `IndexSchemaKey`. It needs to match
// following contraints.
//
// 1. There needs to be at-least one element in both `index_schema`
//    and `primary_key_schema`.
//
// 2. There *cannot* be multiple `IndexSchemaElement::Versionstamp`
//    entries in `index_schema`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IndexSchemaKey {
    index_schema: Vec<IndexSchemaElement>,
    primary_key_schema: Vec<PrimaryKeySchemaElement>,
}

impl IndexSchemaKey {
    pub(crate) fn into_parts(self) -> (Vec<IndexSchemaElement>, Vec<PrimaryKeySchemaElement>) {
        let IndexSchemaKey {
            index_schema,
            primary_key_schema,
        } = self;
        (index_schema, primary_key_schema)
    }

    pub(crate) fn has_versionstamp(&self) -> bool {
        for x in &self.index_schema {
            if x == &IndexSchemaElement::Versionstamp {
                return true;
            }
        }
        false
    }
}

impl TryFrom<(Vec<IndexSchemaElement>, Vec<PrimaryKeySchemaElement>)> for IndexSchemaKey {
    type Error = FdbError;

    fn try_from(
        (index_schema_elements, primay_key_schema_elements): (
            Vec<IndexSchemaElement>,
            Vec<PrimaryKeySchemaElement>,
        ),
    ) -> FdbResult<IndexSchemaKey> {
        // Reuse `PrimaryKeySchema` validation logic.
        let primary_key_schema = PrimaryKeySchema::try_from(primay_key_schema_elements)
            .map_err(|_| FdbError::new(METADATA_INVALID_INDEX_SCHEMA))?
            .into_part();

        if index_schema_elements.len() == 0 {
            return Err(FdbError::new(METADATA_INVALID_INDEX_SCHEMA));
        }

        // If present, there can only be one
        // `IndexSchemaElement::Versionstamp`.
        let mut num_versionstamp = 0;

        for x in &index_schema_elements {
            if x == &IndexSchemaElement::Versionstamp {
                if num_versionstamp == 1 {
                    return Err(FdbError::new(METADATA_INVALID_INDEX_SCHEMA));
                } else {
                    num_versionstamp += 1;
                }
            }
        }

        Ok(IndexSchemaKey {
            index_schema: index_schema_elements,
            primary_key_schema,
        })
    }
}

/// Represents the schema for the value part of FDB key-value that
/// represents [`IndexSchema`].
//
// When we have a value of type `IndexSchemaValue`, we can assume that
// it represents a well formed `IndexSchemaValue`. It needs to match
// following contraints.
//
// 1. There needs to be at-least one element in both `index_schema`.
//
// 2. There *cannot* be multiple `IndexSchemaElement::Versionstamp`
//    entries in `index_schema`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IndexSchemaValue {
    index_schema: Vec<IndexSchemaElement>,
}

impl IndexSchemaValue {
    pub(crate) fn into_part(self) -> Vec<IndexSchemaElement> {
        let IndexSchemaValue { index_schema } = self;

        index_schema
    }

    pub(crate) fn has_versionstamp(&self) -> bool {
        for x in &self.index_schema {
            if x == &IndexSchemaElement::Versionstamp {
                return true;
            }
        }
        false
    }
}

impl TryFrom<Vec<IndexSchemaElement>> for IndexSchemaValue {
    type Error = FdbError;

    fn try_from(index_schema_elements: Vec<IndexSchemaElement>) -> FdbResult<IndexSchemaValue> {
        if index_schema_elements.len() == 0 {
            return Err(FdbError::new(METADATA_INVALID_INDEX_SCHEMA));
        }

        // If present, there can only be one
        // `IndexSchemaElement::Versionstamp`.
        let mut num_versionstamp = 0;

        for x in &index_schema_elements {
            if x == &IndexSchemaElement::Versionstamp {
                if num_versionstamp == 1 {
                    return Err(FdbError::new(METADATA_INVALID_INDEX_SCHEMA));
                } else {
                    num_versionstamp += 1;
                }
            }
        }

        Ok(IndexSchemaValue {
            index_schema: index_schema_elements,
        })
    }
}

/// Represents the schema for Index entry.
//
// When we have a value of type `IndexSchema`, we can assume that it
// represents a well formed `IndexSchema`.
//
// If `key_schema` contains `IndexSchemaElement::Versionstamp`, then
// `value_schema` *cannot* contain `IndexSchemaElement::Versionstamp`
// and vice-versa.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexSchema {
    key_schema: IndexSchemaKey,
    value_schema: Option<IndexSchemaValue>,
}

impl IndexSchema {
    pub(crate) fn into_parts(self) -> (IndexSchemaKey, Option<IndexSchemaValue>) {
        let IndexSchema {
            key_schema,
            value_schema,
        } = self;

        (key_schema, value_schema)
    }
}

impl TryFrom<(IndexSchemaKey, Option<IndexSchemaValue>)> for IndexSchema {
    type Error = FdbError;

    fn try_from(
        (index_schema_key, maybe_index_schema_value): (IndexSchemaKey, Option<IndexSchemaValue>),
    ) -> FdbResult<IndexSchema> {
        match maybe_index_schema_value {
            Some(index_schema_value) => {
                if index_schema_key.has_versionstamp() && index_schema_value.has_versionstamp() {
                    Err(FdbError::new(METADATA_INVALID_INDEX_SCHEMA))
                } else {
                    Ok(IndexSchema {
                        key_schema: index_schema_key,
                        value_schema: Some(index_schema_value),
                    })
                }
            }
            None => Ok(IndexSchema {
                key_schema: index_schema_key,
                value_schema: None,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    mod primary_key_schema {
        use fdb::error::FdbError;

        use std::convert::TryFrom;

        use super::super::super::error::METADATA_INVALID_PRIMARY_KEY_SCHEMA;
        use super::super::{PrimaryKeySchema, PrimaryKeySchemaElement};

        #[test]
        fn try_from_vec_primary_key_schema_element_try_from() {
            // Valid
            {
                // Single
                {
                    assert_eq!(
                        PrimaryKeySchema::try_from(vec![PrimaryKeySchemaElement::Uuid,]),
                        Ok(PrimaryKeySchema {
                            key_schema: vec![PrimaryKeySchemaElement::Uuid]
                        })
                    );
                }
                // Multiple
                {
                    assert_eq!(
                        PrimaryKeySchema::try_from(vec![
                            PrimaryKeySchemaElement::String,
                            PrimaryKeySchemaElement::Integer,
                        ]),
                        Ok(PrimaryKeySchema {
                            key_schema: vec![
                                PrimaryKeySchemaElement::String,
                                PrimaryKeySchemaElement::Integer,
                            ]
                        })
                    );
                }
            }
            // Invalid
            {
                assert_eq!(
                    Err(FdbError::new(METADATA_INVALID_PRIMARY_KEY_SCHEMA)),
                    PrimaryKeySchema::try_from(vec![]),
                );
            }
        }
    }

    mod index_schema_key {
        use fdb::error::FdbError;

        use std::convert::TryFrom;

        use super::super::super::error::METADATA_INVALID_INDEX_SCHEMA;
        use super::super::{IndexSchemaElement, IndexSchemaKey, PrimaryKeySchemaElement};

        #[test]
        fn has_versionstamp() {
            // true
            {
                let index_schema_key = IndexSchemaKey::try_from((
                    vec![IndexSchemaElement::String, IndexSchemaElement::Versionstamp],
                    vec![PrimaryKeySchemaElement::Uuid],
                ))
                .unwrap();

                assert!(index_schema_key.has_versionstamp());
            }
            // false
            {
                let index_schema_key = IndexSchemaKey::try_from((
                    vec![IndexSchemaElement::String],
                    vec![PrimaryKeySchemaElement::Uuid],
                ))
                .unwrap();

                assert!(!index_schema_key.has_versionstamp());
            }
        }

        #[test]
        fn try_from_tuple_vec_index_schema_element_vec_primary_key_schema_element_try_from() {
            // invalid
            {
                // empty `Vec<PrimaryKeySchemaElement>`
                {
                    assert_eq!(
                        Err(FdbError::new(METADATA_INVALID_INDEX_SCHEMA)),
                        IndexSchemaKey::try_from((vec![IndexSchemaElement::String], vec![],))
                    );
                }
                // empty `Vec<IndexSchemaElement>`
                {
                    assert_eq!(
                        Err(FdbError::new(METADATA_INVALID_INDEX_SCHEMA)),
                        IndexSchemaKey::try_from((vec![], vec![PrimaryKeySchemaElement::Uuid])),
                    );
                }
                // multiple `IndexSchemaElement::Versionstamp`
                {
                    assert_eq!(
                        Err(FdbError::new(METADATA_INVALID_INDEX_SCHEMA)),
                        IndexSchemaKey::try_from((
                            vec![
                                IndexSchemaElement::Versionstamp,
                                IndexSchemaElement::String,
                                IndexSchemaElement::Versionstamp
                            ],
                            vec![PrimaryKeySchemaElement::Uuid],
                        ))
                    );
                }
            }
            // valid
            {
                // no `IndexSchemaElement::Versionstamp`
                {
                    assert_eq!(
                        IndexSchemaKey::try_from((
                            vec![IndexSchemaElement::String, IndexSchemaElement::Double],
                            vec![PrimaryKeySchemaElement::Uuid],
                        )),
                        Ok(IndexSchemaKey {
                            index_schema: vec![
                                IndexSchemaElement::String,
                                IndexSchemaElement::Double
                            ],
                            primary_key_schema: vec![PrimaryKeySchemaElement::Uuid]
                        })
                    );
                }
                // one `IndexSchemaElement::Versionstamp`
                {
                    assert_eq!(
                        IndexSchemaKey::try_from((
                            vec![
                                IndexSchemaElement::String,
                                IndexSchemaElement::Double,
                                IndexSchemaElement::Versionstamp,
                            ],
                            vec![PrimaryKeySchemaElement::Uuid],
                        )),
                        Ok(IndexSchemaKey {
                            index_schema: vec![
                                IndexSchemaElement::String,
                                IndexSchemaElement::Double,
                                IndexSchemaElement::Versionstamp,
                            ],
                            primary_key_schema: vec![PrimaryKeySchemaElement::Uuid]
                        })
                    );
                }
            }
        }
    }

    mod index_schema_value {
        use fdb::error::FdbError;

        use std::convert::TryFrom;

        use super::super::super::error::METADATA_INVALID_INDEX_SCHEMA;

        use super::super::{IndexSchemaElement, IndexSchemaValue};

        #[test]
        fn has_versionstamp() {
            // true
            {
                let index_schema_value = IndexSchemaValue::try_from(vec![
                    IndexSchemaElement::String,
                    IndexSchemaElement::Versionstamp,
                ])
                .unwrap();

                assert!(index_schema_value.has_versionstamp());
            }
            // false
            {
                let index_schema_value =
                    IndexSchemaValue::try_from(vec![IndexSchemaElement::String]).unwrap();

                assert!(!index_schema_value.has_versionstamp());
            }
        }

        #[test]
        fn try_from_vec_index_schema_element_try_from() {
            // invalid
            {
                // empty `Vec<IndexSchemaElement>`
                {
                    assert_eq!(
                        Err(FdbError::new(METADATA_INVALID_INDEX_SCHEMA)),
                        IndexSchemaValue::try_from(vec![]),
                    );
                }
                // multiple `IndexSchemaElement::Versionstamp`
                {
                    assert_eq!(
                        Err(FdbError::new(METADATA_INVALID_INDEX_SCHEMA)),
                        IndexSchemaValue::try_from(vec![
                            IndexSchemaElement::Versionstamp,
                            IndexSchemaElement::String,
                            IndexSchemaElement::Versionstamp
                        ])
                    );
                }
            }
            // valid
            {
                // no `IndexSchemaElement::Versionstamp`
                {
                    assert_eq!(
                        IndexSchemaValue::try_from(vec![
                            IndexSchemaElement::String,
                            IndexSchemaElement::Double
                        ]),
                        Ok(IndexSchemaValue {
                            index_schema: vec![
                                IndexSchemaElement::String,
                                IndexSchemaElement::Double
                            ]
                        })
                    );
                }
                // one `IndexSchemaElement::Versionstamp`
                {
                    assert_eq!(
                        IndexSchemaValue::try_from(vec![
                            IndexSchemaElement::String,
                            IndexSchemaElement::Double,
                            IndexSchemaElement::Versionstamp,
                        ]),
                        Ok(IndexSchemaValue {
                            index_schema: vec![
                                IndexSchemaElement::String,
                                IndexSchemaElement::Double,
                                IndexSchemaElement::Versionstamp,
                            ],
                        })
                    );
                }
            }
        }
    }

    mod index_schema {
        use fdb::error::FdbError;

        use std::convert::TryFrom;

        use super::super::super::error::METADATA_INVALID_INDEX_SCHEMA;
        use super::super::{
            IndexSchema, IndexSchemaElement, IndexSchemaKey, IndexSchemaValue,
            PrimaryKeySchemaElement,
        };

        #[test]
        fn try_from_tuple_index_schema_key_option_index_schema_value_try_from() {
            // invalid
            {
                // both `key_schema` and `value_schema` contains
                // `IndexSchemaElement::Versionstamp`
                {
                    let key_schema = IndexSchemaKey::try_from((
                        vec![
                            IndexSchemaElement::String,
                            IndexSchemaElement::Double,
                            IndexSchemaElement::Versionstamp,
                        ],
                        vec![PrimaryKeySchemaElement::Uuid],
                    ))
                    .unwrap();

                    let value_schema =
                        IndexSchemaValue::try_from(vec![IndexSchemaElement::Versionstamp]).unwrap();

                    assert_eq!(
                        Err(FdbError::new(METADATA_INVALID_INDEX_SCHEMA)),
                        IndexSchema::try_from((key_schema, Some(value_schema)))
                    );
                }
            }
            // valid
            {
                // `value_schema` is `None` and `key_schema` does not
                // have `IndexSchemaElement::Versionstamp`
                {
                    let key_schema = IndexSchemaKey::try_from((
                        vec![IndexSchemaElement::String, IndexSchemaElement::Double],
                        vec![PrimaryKeySchemaElement::Uuid],
                    ))
                    .unwrap();

                    assert_eq!(
                        Ok(IndexSchema {
                            key_schema: IndexSchemaKey {
                                index_schema: vec![
                                    IndexSchemaElement::String,
                                    IndexSchemaElement::Double,
                                ],
                                primary_key_schema: vec![PrimaryKeySchemaElement::Uuid]
                            },
                            value_schema: None
                        }),
                        IndexSchema::try_from((key_schema, None))
                    );
                }
                // `value_schema` is `None` and `key_schema` has
                // `IndexSchemaElement::Versionstamp`
                {
                    let key_schema = IndexSchemaKey::try_from((
                        vec![
                            IndexSchemaElement::String,
                            IndexSchemaElement::Double,
                            IndexSchemaElement::Versionstamp,
                        ],
                        vec![PrimaryKeySchemaElement::Uuid],
                    ))
                    .unwrap();

                    assert_eq!(
                        Ok(IndexSchema {
                            key_schema: IndexSchemaKey {
                                index_schema: vec![
                                    IndexSchemaElement::String,
                                    IndexSchemaElement::Double,
                                    IndexSchemaElement::Versionstamp,
                                ],
                                primary_key_schema: vec![PrimaryKeySchemaElement::Uuid]
                            },
                            value_schema: None
                        }),
                        IndexSchema::try_from((key_schema, None))
                    );
                }
                // `value_schema` is `Some(...)`, where `...` has
                // `IndexSchemaElement::Versionstamp`, and
                // `key_schema` does not have
                // `IndexSchemaElement::Versionstamp`
                {
                    let key_schema = IndexSchemaKey::try_from((
                        vec![IndexSchemaElement::String, IndexSchemaElement::Double],
                        vec![PrimaryKeySchemaElement::Uuid],
                    ))
                    .unwrap();

                    let value_schema =
                        IndexSchemaValue::try_from(vec![IndexSchemaElement::Versionstamp]).unwrap();

                    assert_eq!(
                        Ok(IndexSchema {
                            key_schema: IndexSchemaKey {
                                index_schema: vec![
                                    IndexSchemaElement::String,
                                    IndexSchemaElement::Double,
                                ],
                                primary_key_schema: vec![PrimaryKeySchemaElement::Uuid]
                            },
                            value_schema: Some(IndexSchemaValue {
                                index_schema: vec![IndexSchemaElement::Versionstamp],
                            }),
                        }),
                        IndexSchema::try_from((key_schema, Some(value_schema)))
                    );
                }
                // `value_schema` is `Some(...)`, where `...` does not
                // have `IndexSchemaElement::Versionstamp`, and
                // `key_schema` does not have
                // `IndexSchemaElement::Versionstamp`
                {
                    let key_schema = IndexSchemaKey::try_from((
                        vec![IndexSchemaElement::String, IndexSchemaElement::Double],
                        vec![PrimaryKeySchemaElement::Uuid],
                    ))
                    .unwrap();

                    let value_schema =
                        IndexSchemaValue::try_from(vec![IndexSchemaElement::Integer]).unwrap();

                    assert_eq!(
                        Ok(IndexSchema {
                            key_schema: IndexSchemaKey {
                                index_schema: vec![
                                    IndexSchemaElement::String,
                                    IndexSchemaElement::Double,
                                ],
                                primary_key_schema: vec![PrimaryKeySchemaElement::Uuid]
                            },
                            value_schema: Some(IndexSchemaValue {
                                index_schema: vec![IndexSchemaElement::Integer],
                            }),
                        }),
                        IndexSchema::try_from((key_schema, Some(value_schema)))
                    );
                }
            }
        }
    }
}
