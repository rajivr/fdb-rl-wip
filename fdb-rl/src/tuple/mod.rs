//! TODO

// An abstraction layer for tuples within the record layer.
//
//
// We will attempt to extend the Tuple Layer to support custom types
// like Date, Time, etc., All keys that we generate from PartiQL would
// translate to `RecordTuple` and `RecordTupleSchema`.

pub(crate) mod record_tuple;
pub(crate) mod record_tuple_schema;

pub(crate) use record_tuple::{
    RecordTuple, Timestamp, UTCTimeWithMaybeOffset, UTCTimestampWithMaybeOffset,
};
pub(crate) use record_tuple_schema::{RecordTupleSchema, RecordTupleSchemaElement};
