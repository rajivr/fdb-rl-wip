//! TODO

use fdb::error::{FdbError, FdbResult};

use std::convert::TryFrom;

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

/// Represents a schema for primary key.
//
// When we have a value of type `PrimaryKeySchema`, we can assume that
// it represents a well-formed `PrimaryKeySchema`
#[derive(Debug, Clone, PartialEq)]
pub struct PrimaryKeySchema {
    fdb_key_schema: Vec<PrimaryKeySchemaElement>,
}

impl PrimaryKeySchema {
    pub(crate) fn into_parts(self) -> (Vec<PrimaryKeySchemaElement>,) {
        let PrimaryKeySchema { fdb_key_schema } = self;

        (fdb_key_schema,)
    }
}

impl TryFrom<Vec<PrimaryKeySchemaElement>> for PrimaryKeySchema {
    type Error = FdbError;

    fn try_from(
        list_of_primary_key_schema_element: Vec<PrimaryKeySchemaElement>,
    ) -> FdbResult<PrimaryKeySchema> {
        todo!();
    }
}

/// Represents [`IndexSchema`]'s FDB Key schema
#[derive(Debug)]
pub struct IndexSchemaFdbKey(pub Vec<IndexSchemaElement>);

/// Represents [`IndexSchema`]'s FDB Value schema
#[derive(Debug)]
pub struct IndexSchemaFdbValue(pub Option<Vec<IndexSchemaElement>>);

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

/// Represents a schema for Index entry.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexSchema {
    fdb_key_schema: Vec<IndexSchemaElement>,
    fdb_value_schema: Option<Vec<IndexSchemaElement>>,
}

impl IndexSchema {
    pub(crate) fn into_parts(self) -> (Vec<IndexSchemaElement>, Option<Vec<IndexSchemaElement>>) {
        let IndexSchema {
            fdb_key_schema,
            fdb_value_schema,
        } = self;

        (fdb_key_schema, fdb_value_schema)
    }
}

impl TryFrom<(IndexSchemaFdbKey, IndexSchemaFdbValue)> for IndexSchema {
    type Error = FdbError;

    fn try_from(
        (IndexSchemaFdbKey(fdb_key_schema), IndexSchemaFdbValue(maybe_fdb_value_schema)): (
            IndexSchemaFdbKey,
            IndexSchemaFdbValue,
        ),
    ) -> FdbResult<IndexSchema> {
        todo!();
    }
}
