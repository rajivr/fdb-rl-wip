//! Provides [`RawRecord`] type and associated items.
use bytes::Bytes;

use fdb::error::{FdbError, FdbResult};
use fdb::tuple::{Tuple, TupleSchema, TupleSchemaElement};

use std::convert::TryFrom;

use crate::cursor::{Cursor, CursorResult};
use crate::error::{
    RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA, RAW_RECORD_PRIMARY_KEY_TUPLE_SCHEMA_MISMATCH,
};
use crate::RecordVersion;

trait Visitor {
    fn visit_tuple_schema_element(&self, tuple_schema_element: &TupleSchemaElement) -> bool;
}

/// We do not allow the primary key's [`TupleSchema`] of [`RawRecord`]
/// to be empty or to have a `Null` or `Versionstamp` or a nullable
/// type (such as `MaybeXYZ`) or a empty nested tuple.
#[derive(Debug)]
struct PrimaryKeySchemaValidatorVisitor;

impl Visitor for PrimaryKeySchemaValidatorVisitor {
    fn visit_tuple_schema_element(&self, tuple_schema_element: &TupleSchemaElement) -> bool {
        match tuple_schema_element {
            TupleSchemaElement::Null
            | TupleSchemaElement::Versionstamp
            | TupleSchemaElement::MaybeBytes
            | TupleSchemaElement::MaybeString
            | TupleSchemaElement::MaybeTuple(_)
            | TupleSchemaElement::MaybeInteger
            | TupleSchemaElement::MaybeFloat
            | TupleSchemaElement::MaybeDouble
            | TupleSchemaElement::MaybeBoolean
            | TupleSchemaElement::MaybeUuid
            | TupleSchemaElement::MaybeVersionstamp => false,
            TupleSchemaElement::Bytes
            | TupleSchemaElement::String
            | TupleSchemaElement::Integer
            | TupleSchemaElement::Float
            | TupleSchemaElement::Double
            | TupleSchemaElement::Boolean
            | TupleSchemaElement::Uuid => true,
            TupleSchemaElement::Tuple(ts) => walk_tuple_schema(self, ts),
        }
    }
}

fn walk_tuple_schema(visitor: &dyn Visitor, tuple_schema: &TupleSchema) -> bool {
    if tuple_schema.len() == 0 {
        false
    } else {
        for tuple_schema_element in tuple_schema.iter() {
            if !walk_tuple_schema_element(visitor, tuple_schema_element) {
                return false;
            }
        }
        true
    }
}

fn walk_tuple_schema_element(
    visitor: &dyn Visitor,
    tuple_schema_element: &TupleSchemaElement,
) -> bool {
    visitor.visit_tuple_schema_element(tuple_schema_element)
}

/// Represents the schema for a [`RawRecordPrimaryKey`].
///
/// It consists of a [`TupleSchema`]. When we have a value of
/// [`RawRecordPrimaryKeySchema`], that means that the [`TupleSchema`]
/// satisfies the constraints to be a primary key schema.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RawRecordPrimaryKeySchema {
    inner: TupleSchema,
}

impl RawRecordPrimaryKeySchema {
    /// Get reference to [`TupleSchema].
    fn get_tuple_schema_ref(&self) -> &TupleSchema {
        &self.inner
    }
}

impl TryFrom<TupleSchema> for RawRecordPrimaryKeySchema {
    type Error = FdbError;

    fn try_from(tuple_schema: TupleSchema) -> FdbResult<RawRecordPrimaryKeySchema> {
        if walk_tuple_schema(&PrimaryKeySchemaValidatorVisitor, &tuple_schema) {
            Ok(RawRecordPrimaryKeySchema {
                inner: tuple_schema,
            })
        } else {
            Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
        }
    }
}

/// Represents a [`RawRecord`]'s primary key which is a [`Tuple`].
///
/// When we have a value of [`RawRecordPrimaryKey`], that means that
/// it conforms to [`RawRecordPrimaryKeySchema`].
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RawRecordPrimaryKey {
    schema: RawRecordPrimaryKeySchema,
    key: Tuple,
}

impl TryFrom<(RawRecordPrimaryKeySchema, Tuple)> for RawRecordPrimaryKey {
    type Error = FdbError;

    fn try_from(
        (schema, key): (RawRecordPrimaryKeySchema, Tuple),
    ) -> FdbResult<RawRecordPrimaryKey> {
        if schema.get_tuple_schema_ref().validate(&key) {
            Ok(RawRecordPrimaryKey { schema, key })
        } else {
            Err(FdbError::new(RAW_RECORD_PRIMARY_KEY_TUPLE_SCHEMA_MISMATCH))
        }
    }
}

/// A wrapper around all information that can be determined about a
/// record before serializing and deserializing it.
pub(crate) struct RawRecord {
    primary_key: RawRecordPrimaryKey,
    version: RecordVersion,
    record_bytes: Bytes,
}

// No tests for this because we are just constructing a [`RawRecord`].
impl From<(RawRecordPrimaryKey, RecordVersion, Bytes)> for RawRecord {
    fn from(
        (primary_key, version, record_bytes): (RawRecordPrimaryKey, RecordVersion, Bytes),
    ) -> RawRecord {
        RawRecord {
            primary_key,
            version,
            record_bytes,
        }
    }
}

/// TODO
pub(crate) struct RawRecordCursor {}

impl Cursor<RawRecord> for RawRecordCursor {
    /// TODO

    // (remove later): `NoNextReason::ReturnLimitReached` would be
    // specific number of `RawRecord`.
    //
    // Mandatorially specify:
    // - Primary key tuple length
    //
    // And optionally
    // - `Subspace`
    // - `StreamingMode` (default would be `StreamingMode::Iterator`)
    // - `limit` (this would be number of `RawRecord`s)
    // - `reverse`
    // - `from_primary_key`
    //
    // TODO:
    // -----
    //
    // `TupleSchema` and `TupleSchemaValue`
    //
    //  - `TupleSchemaValue::Null`
    //  - `TupleSchemaValue::Bytes`
    //  - `TupleSchemaValue::Tuple`
    //  - `TupleSchemaValue::Integer`
    //  - `TupleSchemaValue::Float`
    //  - `TupleSchemaValue::Double`
    //  - `TupleSchemaValue::Bool`
    //  - `TupleSchemaValue::UUid`
    //  - `TupleSchemaValue::Versionstamp`
    //  - `TupleSchemaValue::MaybeBytes`
    //  - `TupleSchemaValue::MaybeTuple`
    //  - `TupleSchemaValue::MaybeInteger`
    //
    // TODO: continue from here.
    // tuple_schema.validate(&tuple)
    // TUPLE_SCHEMA_MISMATCH

    async fn next(&mut self) -> CursorResult<RawRecord> {
        todo!();
    }
}

#[cfg(test)]
mod tests {
    mod raw_record_primary_key_schema {
        use fdb::error::FdbError;
        use fdb::tuple::{TupleSchema, TupleSchemaElement};

        use std::convert::TryFrom;

        use crate::error::RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA;

        use super::super::RawRecordPrimaryKeySchema;

        #[test]
        fn try_from_tuple_schema_try_from() {
            // Valid schema
            {
                let mut ts = TupleSchema::new();
                ts.push_back(TupleSchemaElement::Bytes);
                ts.push_back(TupleSchemaElement::String);
                ts.push_back(TupleSchemaElement::Integer);
                ts.push_back(TupleSchemaElement::Float);
                ts.push_back(TupleSchemaElement::Double);
                ts.push_back(TupleSchemaElement::Uuid);
                ts.push_back(TupleSchemaElement::Tuple({
                    let mut ts_inner = TupleSchema::new();
                    ts_inner.push_back(TupleSchemaElement::String);
                    ts_inner
                }));

                assert_eq!(
                    RawRecordPrimaryKeySchema::try_from(ts.clone()),
                    Ok(RawRecordPrimaryKeySchema { inner: ts })
                );
            }

            // Empty schema is invalid
            {
                let ts = TupleSchema::new();

                assert_eq!(
                    RawRecordPrimaryKeySchema::try_from(ts),
                    Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                );
            }

            // Invalid schema elements
            {
                {
                    let mut ts = TupleSchema::new();
                    ts.push_back(TupleSchemaElement::Null);

                    assert_eq!(
                        RawRecordPrimaryKeySchema::try_from(ts),
                        Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                    );
                }
                {
                    let mut ts = TupleSchema::new();
                    ts.push_back(TupleSchemaElement::Versionstamp);

                    assert_eq!(
                        RawRecordPrimaryKeySchema::try_from(ts),
                        Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                    );
                }
                {
                    let mut ts = TupleSchema::new();
                    ts.push_back(TupleSchemaElement::MaybeString);

                    assert_eq!(
                        RawRecordPrimaryKeySchema::try_from(ts),
                        Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                    );
                }
                // Valid nested tuple, but within a `MaybeTuple`.
                {
                    let mut ts = TupleSchema::new();
                    ts.push_back(TupleSchemaElement::MaybeTuple({
                        let mut ts_inner = TupleSchema::new();
                        ts_inner.push_back(TupleSchemaElement::String);
                        ts_inner
                    }));

                    assert_eq!(
                        RawRecordPrimaryKeySchema::try_from(ts),
                        Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                    );
                }
                // Empty tuple, in an otherwise valid schema.
                // Valid nested tuple, but within a `MaybeTuple`.
                {
                    let mut ts = TupleSchema::new();
                    ts.push_back(TupleSchemaElement::Bytes);
                    ts.push_back(TupleSchemaElement::Tuple({
                        let ts_inner = TupleSchema::new();
                        ts_inner
                    }));

                    assert_eq!(
                        RawRecordPrimaryKeySchema::try_from(ts),
                        Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                    );
                }
                {
                    let mut ts = TupleSchema::new();
                    ts.push_back(TupleSchemaElement::MaybeInteger);

                    assert_eq!(
                        RawRecordPrimaryKeySchema::try_from(ts),
                        Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                    );
                }
                {
                    let mut ts = TupleSchema::new();
                    ts.push_back(TupleSchemaElement::MaybeFloat);

                    assert_eq!(
                        RawRecordPrimaryKeySchema::try_from(ts),
                        Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                    );
                }
                {
                    let mut ts = TupleSchema::new();
                    ts.push_back(TupleSchemaElement::MaybeDouble);

                    assert_eq!(
                        RawRecordPrimaryKeySchema::try_from(ts),
                        Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                    );
                }
                {
                    let mut ts = TupleSchema::new();
                    ts.push_back(TupleSchemaElement::MaybeBoolean);

                    assert_eq!(
                        RawRecordPrimaryKeySchema::try_from(ts),
                        Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                    );
                }
                {
                    let mut ts = TupleSchema::new();
                    ts.push_back(TupleSchemaElement::MaybeUuid);

                    assert_eq!(
                        RawRecordPrimaryKeySchema::try_from(ts),
                        Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                    );
                }
                {
                    let mut ts = TupleSchema::new();
                    ts.push_back(TupleSchemaElement::MaybeVersionstamp);

                    assert_eq!(
                        RawRecordPrimaryKeySchema::try_from(ts),
                        Err(FdbError::new(RAW_RECORD_INVALID_PRIMARY_KEY_SCHEMA))
                    );
                }
            }
        }
    }

    mod raw_record_primary_key {
        use bytes::Bytes;

        use fdb::error::FdbError;
        use fdb::tuple::{Tuple, TupleSchema, TupleSchemaElement};

        use std::convert::TryFrom;

        use crate::error::RAW_RECORD_PRIMARY_KEY_TUPLE_SCHEMA_MISMATCH;

        use super::super::{RawRecordPrimaryKey, RawRecordPrimaryKeySchema};

        #[test]
        fn try_from_raw_record_primary_key_schema_tuple_try_from() {
            // `Tuple` matches `RawRecordPrimaryKeySchema`
            {
                let mut ts = TupleSchema::new();
                ts.push_back(TupleSchemaElement::Bytes);
                ts.push_back(TupleSchemaElement::String);
                ts.push_back(TupleSchemaElement::Integer);

                let schema = RawRecordPrimaryKeySchema::try_from(ts).unwrap();

                let mut key = Tuple::new();
                key.push_back::<Bytes>(Bytes::from_static(b"hello"));
                key.push_back::<String>("world".to_string());
                key.push_back::<i8>(0);

                assert_eq!(
                    RawRecordPrimaryKey::try_from((schema.clone(), key.clone())),
                    Ok(RawRecordPrimaryKey { schema, key })
                );
            }

            // `Tuple` does not match `RawRecordPrimaryKeySchema`
            {
                let mut ts = TupleSchema::new();
                ts.push_back(TupleSchemaElement::Bytes);
                ts.push_back(TupleSchemaElement::String);
                ts.push_back(TupleSchemaElement::Integer);

                let schema = RawRecordPrimaryKeySchema::try_from(ts).unwrap();

                let mut key = Tuple::new();
                key.push_back::<i8>(0);
                key.push_back::<String>("world".to_string());
                key.push_back::<Bytes>(Bytes::from_static(b"hello"));

                assert_eq!(
                    RawRecordPrimaryKey::try_from((schema, key)),
                    Err(FdbError::new(RAW_RECORD_PRIMARY_KEY_TUPLE_SCHEMA_MISMATCH))
                );
            }
        }
    }
}
