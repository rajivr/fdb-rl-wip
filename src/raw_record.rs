//! Provides [`RawRecord`] type and associated items.
use bytes::Bytes;

use fdb::tuple::Tuple;

use crate::cursor::{Cursor, CursorResult};
use crate::RecordVersion;

/// A wrapper around all information that can be determined about a
/// record before serializing and deserializing it.
pub(crate) struct RawRecord {
    primary_key: Tuple,
    version: RecordVersion,
    raw_record: Bytes,
}

impl RawRecord {
    /// Extract primary key [`Tuple`], [`RecordVersion`] and record
    /// [`Bytes`] from [`RawRecord`].
    pub(crate) fn into_parts(self) -> (Tuple, RecordVersion, Bytes) {
        let RawRecord {
            primary_key,
            version,
            raw_record,
        } = self;
        (primary_key, version, raw_record)
    }
}

impl From<(Tuple, RecordVersion, Bytes)> for RawRecord {
    fn from(value: (Tuple, RecordVersion, Bytes)) -> RawRecord {
        let (primary_key, version, raw_record) = value;

        RawRecord {
            primary_key,
            version,
            raw_record,
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
    // TODO: check if the following still holds.
    //
    // No tests here as we are just defining types.
}
