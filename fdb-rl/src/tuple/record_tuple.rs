use bytes::Bytes;
use fdb::tuple::Versionstamp;
use num_bigint::BigInt;
use time::{Date, Time, UtcOffset};
use uuid::Uuid;

use std::collections::VecDeque;

/// Needed for [`RecordTupleSchemaElement::Integer`] variant because
/// we do not have anonymous enums.
enum RecordTupleValueInteger {
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    BigInt(BigInt),
}

/// TODO
pub(crate) enum RecordTupleValue {
    Bytes(Bytes),
    String(String),
    RecordTuple(RecordTuple),
    Integer(RecordTupleValueInteger),
    Float(f32),
    Double(f64),
    Boolean(bool),
    Uuid(Uuid),
    Versionstamp(Versionstamp),
    Date(Date),
    Time(Time),
    UTCTimeWithOffset(Time, UtcOffset),
    Timestamp(Date, Time),
    UTCTimestampWithOffset(Date, Time, UtcOffset),
    MaybeBytes(Option<Bytes>),
    MaybeString(Option<String>),
    MaybeRecordTuple(Option<RecordTuple>),
    MaybeInteger(Option<RecordTupleValueInteger>),
    MaybeFloat(Option<f32>),
    MaybeDouble(Option<f64>),
    MaybeBoolean(Option<bool>),
    MaybeUuid(Option<Uuid>),
    MaybeVersionstamp(Option<Versionstamp>),
    MaybeDate(Option<Date>),
    MaybeTime(Option<Time>),
    MaybeUTCTimeWithOffset(Option<(Time, UtcOffset)>),
    MaybeTimestamp(Option<(Date, Time)>),
    MaybeUTCTimestampWithOffset(Option<(Date, Time, UtcOffset)>),
    ListOfBytes(Vec<Bytes>),
    ListOfString(Vec<String>),
    ListOfRecordTuple(Vec<RecordTuple>),
    ListOfInteger(Vec<RecordTupleValueInteger>),
    ListOfFloat(Vec<f32>),
    ListOfDouble(Vec<f64>),
    ListOfBoolean(Vec<bool>),
    ListOfUuid(Vec<Uuid>),
    ListOfVersionstamp(Vec<Versionstamp>),
    ListOfDate(Vec<Date>),
    ListOfTime(Vec<Time>),
    ListOfUTCTimeWithOffset(Vec<(Time, UtcOffset)>),
    ListOfTimestamp(Vec<(Date, Time)>),
    ListOfUTCTimestampWithOffset(Vec<(Date, Time, UtcOffset)>),
}

/// TODO
pub(crate) struct RecordTuple {
    elements: VecDeque<RecordTupleValue>,
}
