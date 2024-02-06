use std::collections::VecDeque;

// TODO
// https://github.com/cockroachdb/cockroach/blob/v23.1.14/pkg/util/encoding/encoding.go#L42-L135

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_BYTES: i32 = 0;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_MAYBE_BYTES: i32 = 1;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_LIST_OF_BYTES: i32 = 2;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_STRING: i32 = 3;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_MAYBE_STRING: i32 = 4;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_LIST_OF_STRING: i32 = 5;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_RECORD_TUPLE: i32 = 6;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_MAYBE_RECORD_TUPLE: i32 = 7;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_LIST_OF_RECORD_TUPLE: i32 = 8;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_INTEGER: i32 = 9;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_MAYBE_INTEGER: i32 = 10;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_LIST_OF_INTEGER: i32 = 11;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_FLOAT: i32 = 12;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_MAYBE_FLOAT: i32 = 13;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_LIST_OF_FLOAT: i32 = 14;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_DOUBLE: i32 = 15;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_MAYBE_DOUBLE: i32 = 16;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_LIST_OF_DOUBLE: i32 = 17;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_BOOLEAN: i32 = 18;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_MAYBE_BOOLEAN: i32 = 19;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_LIST_OF_BOOLEAN: i32 = 20;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_UUID: i32 = 21;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_MAYBE_UUID: i32 = 22;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_LIST_OF_UUID: i32 = 23;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_VERSIONSTAMP: i32 = 24;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_MAYBE_VERSIONSTAMP: i32 = 25;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_LIST_OF_VERSIONSTAMP: i32 = 26;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_DATE: i32 = 27;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_MAYBE_DATE: i32 = 28;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_LIST_OF_DATE: i32 = 29;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_TIME: i32 = 30;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_MAYBE_TIME: i32 = 31;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_LIST_OF_TIME: i32 = 32;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_UTC_TIME_WITH_OFFSET: i32 = 33;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_MAYBE_UTC_TIME_WITH_OFFSET: i32 = 34;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_LIST_OF_UTC_TIME_WITH_OFFSET: i32 = 35;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_TIMESTAMP: i32 = 36;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_MAYBE_TIMESTAMP: i32 = 37;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_LIST_OF_TIMESTAMP: i32 = 38;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_UTC_TIMESTAMP_WITH_OFFSET: i32 = 39;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_MAYBE_UTC_TIMESTAMP_WITH_OFFSET: i32 = 40;

/// TODO
pub(crate) const RECORD_TUPLE_MARKER_LIST_OF_UTC_TIMESTAMP_WITH_OFFSET: i32 = 41;

/// TODO
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RecordTupleSchema {
    elements: VecDeque<RecordTupleSchemaElement>,
}

/// TODO
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RecordTupleSchemaElement {
    Bytes,
    String,
    RecordTuple(RecordTupleSchema),
    Integer,
    Float,
    Double,
    Boolean,
    Uuid,
    Versionstamp,
    Date,
    Time,
    UTCTimeWithOffset,
    Timestamp,
    UTCTimestampWithOffset,
    MaybeBytes,
    MaybeString,
    MaybeRecordTuple(RecordTupleSchema),
    MaybeInteger,
    MaybeFloat,
    MaybeDouble,
    MaybeBoolean,
    MaybeUuid,
    MaybeVersionstamp,
    MaybeDate,
    MaybeTime,
    MaybeUTCTimeWithOffset,
    MaybeTimestamp,
    MaybeUTCTimestampWithOffset,
    ListOfBytes,
    ListOfString,
    ListOfRecordTuple(RecordTupleSchema),
    ListOfInteger,
    ListOfFloat,
    ListOfDouble,
    ListOfBoolean,
    ListOfUuid,
    ListOfVersionstamp,
    ListOfDate,
    ListOfTime,
    ListOfUTCTimeWithOffset,
    ListOfTimestamp,
    ListOfUTCTimestampWithOffset,
}
