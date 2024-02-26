use bytes::Bytes;
use fdb::tuple::Versionstamp;
use num_bigint::BigInt;
use time::{Date, Time, UtcOffset};
use uuid::Uuid;

use std::collections::VecDeque;
use std::convert::TryFrom;

/// Prevent users from implementing private trait.
mod private {
    use bytes::Bytes;
    use fdb::tuple::Versionstamp;
    use num_bigint::BigInt;
    use time::{Date, Time, UtcOffset};
    use uuid::Uuid;

    use crate::tuple::{
        RecordTuple, Timestamp, UTCTimeWithMaybeOffset, UTCTimestampWithMaybeOffset,
    };

    pub(crate) trait SealedGet {}

    impl SealedGet for &Bytes {}
    impl SealedGet for &String {}
    impl SealedGet for &RecordTuple {}
    impl SealedGet for BigInt {}
    impl SealedGet for i64 {}
    impl SealedGet for i32 {}
    impl SealedGet for i16 {}
    impl SealedGet for i8 {}
    impl SealedGet for f32 {}
    impl SealedGet for f64 {}
    impl SealedGet for bool {}
    impl SealedGet for &Uuid {}
    impl SealedGet for &Versionstamp {}
    impl SealedGet for &Date {}
    impl SealedGet for &Time {}
    impl SealedGet for &UTCTimeWithMaybeOffset {}
    impl SealedGet for &Timestamp {}
    impl SealedGet for &UTCTimestampWithMaybeOffset {}

    impl SealedGet for &Option<Bytes> {}
    impl SealedGet for &Option<String> {}
    impl SealedGet for &Option<RecordTuple> {}
    impl SealedGet for Option<BigInt> {}
    impl SealedGet for Option<i64> {}
    impl SealedGet for Option<i32> {}
    impl SealedGet for Option<i16> {}
    impl SealedGet for Option<i8> {}
    impl SealedGet for Option<f32> {}
    impl SealedGet for Option<f64> {}
    impl SealedGet for Option<bool> {}
    impl SealedGet for &Option<Uuid> {}
    impl SealedGet for &Option<Versionstamp> {}
    impl SealedGet for &Option<Date> {}
    impl SealedGet for &Option<Time> {}
    impl SealedGet for &Option<UTCTimeWithMaybeOffset> {}
    impl SealedGet for &Option<Timestamp> {}
    impl SealedGet for &Option<UTCTimestampWithMaybeOffset> {}

    impl SealedGet for &Vec<Bytes> {}
    impl SealedGet for &Vec<String> {}
    impl SealedGet for &Vec<RecordTuple> {}
    impl SealedGet for Vec<BigInt> {}
    impl SealedGet for Vec<i64> {}
    impl SealedGet for Vec<i32> {}
    impl SealedGet for Vec<i16> {}
    impl SealedGet for Vec<i8> {}
    impl SealedGet for Vec<f32> {}
    impl SealedGet for Vec<f64> {}
    impl SealedGet for Vec<bool> {}
    impl SealedGet for &Vec<Uuid> {}
    impl SealedGet for &Vec<Versionstamp> {}
    impl SealedGet for &Vec<Date> {}
    impl SealedGet for &Vec<Time> {}
    impl SealedGet for &Vec<UTCTimeWithMaybeOffset> {}
    impl SealedGet for &Vec<Timestamp> {}
    impl SealedGet for &Vec<UTCTimestampWithMaybeOffset> {}

    pub(crate) trait SealedPush {}

    impl SealedPush for Bytes {}
    impl SealedPush for String {}
    impl SealedPush for RecordTuple {}
    impl SealedPush for BigInt {}
    impl SealedPush for i64 {}
    impl SealedPush for i32 {}
    impl SealedPush for i16 {}
    impl SealedPush for i8 {}
    impl SealedPush for f32 {}
    impl SealedPush for f64 {}
    impl SealedPush for bool {}
    impl SealedPush for Uuid {}
    impl SealedPush for Versionstamp {}
    impl SealedPush for Date {}
    impl SealedPush for Time {}
    impl SealedPush for UTCTimeWithMaybeOffset {}
    impl SealedPush for Timestamp {}
    impl SealedPush for UTCTimestampWithMaybeOffset {}

    impl SealedPush for Option<Bytes> {}
    impl SealedPush for Option<String> {}
    impl SealedPush for Option<RecordTuple> {}
    impl SealedPush for Option<BigInt> {}
    impl SealedPush for Option<i64> {}
    impl SealedPush for Option<i32> {}
    impl SealedPush for Option<i16> {}
    impl SealedPush for Option<i8> {}
    impl SealedPush for Option<f32> {}
    impl SealedPush for Option<f64> {}
    impl SealedPush for Option<bool> {}
    impl SealedPush for Option<Uuid> {}
    impl SealedPush for Option<Versionstamp> {}
    impl SealedPush for Option<Date> {}
    impl SealedPush for Option<Time> {}
    impl SealedPush for Option<UTCTimeWithMaybeOffset> {}
    impl SealedPush for Option<Timestamp> {}
    impl SealedPush for Option<UTCTimestampWithMaybeOffset> {}

    impl SealedPush for Vec<Bytes> {}
    impl SealedPush for Vec<String> {}
    impl SealedPush for Vec<RecordTuple> {}
    impl SealedPush for Vec<BigInt> {}
    impl SealedPush for Vec<i64> {}
    impl SealedPush for Vec<i32> {}
    impl SealedPush for Vec<i16> {}
    impl SealedPush for Vec<i8> {}
    impl SealedPush for Vec<f32> {}
    impl SealedPush for Vec<f64> {}
    impl SealedPush for Vec<bool> {}
    impl SealedPush for Vec<Uuid> {}
    impl SealedPush for Vec<Versionstamp> {}
    impl SealedPush for Vec<Date> {}
    impl SealedPush for Vec<Time> {}
    impl SealedPush for Vec<UTCTimeWithMaybeOffset> {}
    impl SealedPush for Vec<Timestamp> {}
    impl SealedPush for Vec<UTCTimestampWithMaybeOffset> {}

    pub(crate) trait SealedPop {}

    impl SealedPop for Bytes {}
    impl SealedPop for String {}
    impl SealedPop for RecordTuple {}
    impl SealedPop for BigInt {}
    impl SealedPop for i64 {}
    impl SealedPop for i32 {}
    impl SealedPop for i16 {}
    impl SealedPop for i8 {}
    impl SealedPop for f32 {}
    impl SealedPop for f64 {}
    impl SealedPop for bool {}
    impl SealedPop for Uuid {}
    impl SealedPop for Versionstamp {}
    impl SealedPop for Date {}
    impl SealedPop for Time {}
    impl SealedPop for UTCTimeWithMaybeOffset {}
    impl SealedPop for Timestamp {}
    impl SealedPop for UTCTimestampWithMaybeOffset {}

    impl SealedPop for Option<Bytes> {}
    impl SealedPop for Option<String> {}
    impl SealedPop for Option<RecordTuple> {}
    impl SealedPop for Option<BigInt> {}
    impl SealedPop for Option<i64> {}
    impl SealedPop for Option<i32> {}
    impl SealedPop for Option<i16> {}
    impl SealedPop for Option<i8> {}
    impl SealedPop for Option<f32> {}
    impl SealedPop for Option<f64> {}
    impl SealedPop for Option<bool> {}
    impl SealedPop for Option<Uuid> {}
    impl SealedPop for Option<Versionstamp> {}
    impl SealedPop for Option<Date> {}
    impl SealedPop for Option<Time> {}
    impl SealedPop for Option<UTCTimeWithMaybeOffset> {}
    impl SealedPop for Option<Timestamp> {}
    impl SealedPop for Option<UTCTimestampWithMaybeOffset> {}

    impl SealedPop for Vec<Bytes> {}
    impl SealedPop for Vec<String> {}
    impl SealedPop for Vec<RecordTuple> {}
    impl SealedPop for Vec<BigInt> {}
    impl SealedPop for Vec<i64> {}
    impl SealedPop for Vec<i32> {}
    impl SealedPop for Vec<i16> {}
    impl SealedPop for Vec<i8> {}
    impl SealedPop for Vec<f32> {}
    impl SealedPop for Vec<f64> {}
    impl SealedPop for Vec<bool> {}
    impl SealedPop for Vec<Uuid> {}
    impl SealedPop for Vec<Versionstamp> {}
    impl SealedPop for Vec<Date> {}
    impl SealedPop for Vec<Time> {}
    impl SealedPop for Vec<UTCTimeWithMaybeOffset> {}
    impl SealedPop for Vec<Timestamp> {}
    impl SealedPop for Vec<UTCTimestampWithMaybeOffset> {}
}

/// TODO
pub(crate) trait RecordTupleElementGet<'a>: private::SealedGet {
    #[doc(hidden)]
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<Self>
    where
        Self: Sized + 'a;
}

impl<'a> RecordTupleElementGet<'a> for &'a Bytes {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Bytes> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::Bytes(ref b) => Some(b),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a String {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a String> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::String(ref s) => Some(s),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a RecordTuple {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a RecordTuple> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::RecordTuple(ref rt) => Some(rt),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for BigInt {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<BigInt> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::Integer(ref i) => Some(BigInt::from(i)),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for i64 {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<i64> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::Integer(ref i) => i64::try_from(i).ok(),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for i32 {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<i32> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::Integer(ref i) => i32::try_from(i).ok(),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for i16 {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<i16> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::Integer(ref i) => i16::try_from(i).ok(),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for i8 {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<i8> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::Integer(ref i) => i8::try_from(i).ok(),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for f32 {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<f32> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::Float(f) => Some(f),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for f64 {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<f64> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::Double(d) => Some(d),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for bool {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<bool> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::Boolean(b) => Some(b),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Uuid {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Uuid> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::Uuid(ref u) => Some(u),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Versionstamp {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Versionstamp> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::Versionstamp(ref v) => Some(v),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Date {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Date> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::Date(ref d) => Some(d),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Time {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Time> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::Time(ref t) => Some(t),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a UTCTimeWithMaybeOffset {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a UTCTimeWithMaybeOffset> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::UTCTimeWithMaybeOffset(ref t) => Some(t),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Timestamp {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Timestamp> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::Timestamp(ref ts) => Some(ts),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a UTCTimestampWithMaybeOffset {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a UTCTimestampWithMaybeOffset> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::UTCTimestampWithMaybeOffset(ref ts) => Some(ts),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Option<Bytes> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Option<Bytes>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::MaybeBytes(ref maybe_b) => Some(maybe_b),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Option<String> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Option<String>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::MaybeString(ref maybe_s) => Some(maybe_s),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Option<RecordTuple> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Option<RecordTuple>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::MaybeRecordTuple(ref maybe_rt) => Some(maybe_rt),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for Option<BigInt> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<Option<BigInt>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::MaybeInteger(ref maybe_i) => Some(maybe_i.as_ref().map(BigInt::from)),
            _ => None,
        })
    }
}

// None: Missing or BigInt instead of i64
// Some(None): Null
// Some(Some(x)): Okay case.
//
// TODO: Write tests for this!
impl<'a> RecordTupleElementGet<'a> for Option<i64> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<Option<i64>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::MaybeInteger(ref maybe_i) => {
                match maybe_i.as_ref().map(i64::try_from) {
                    Some(res) => res.ok().map(Option::Some),
                    None => Some(None),
                }
            }
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for Option<i32> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<Option<i32>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::MaybeInteger(ref maybe_i) => {
                match maybe_i.as_ref().map(i32::try_from) {
                    Some(res) => res.ok().map(Option::Some),
                    None => Some(None),
                }
            }
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for Option<i16> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<Option<i16>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::MaybeInteger(ref maybe_i) => {
                match maybe_i.as_ref().map(i16::try_from) {
                    Some(res) => res.ok().map(Option::Some),
                    None => Some(None),
                }
            }
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for Option<i8> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<Option<i8>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::MaybeInteger(ref maybe_i) => {
                match maybe_i.as_ref().map(i8::try_from) {
                    Some(res) => res.ok().map(Option::Some),
                    None => Some(None),
                }
            }
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for Option<f32> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<Option<f32>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::MaybeFloat(maybe_f) => Some(maybe_f),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for Option<f64> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<Option<f64>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::MaybeDouble(maybe_d) => Some(maybe_d),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for Option<bool> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<Option<bool>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::MaybeBoolean(maybe_b) => Some(maybe_b),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Option<Uuid> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Option<Uuid>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::MaybeUuid(ref maybe_u) => Some(maybe_u),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Option<Versionstamp> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Option<Versionstamp>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::MaybeVersionstamp(ref maybe_v) => Some(maybe_v),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Option<Date> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Option<Date>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::MaybeDate(ref maybe_d) => Some(maybe_d),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Option<Time> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Option<Time>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::MaybeTime(ref maybe_t) => Some(maybe_t),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Option<UTCTimeWithMaybeOffset> {
    fn get(
        record_tuple: &'a RecordTuple,
        index: usize,
    ) -> Option<&'a Option<UTCTimeWithMaybeOffset>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::MaybeUTCTimeWithMaybeOffset(ref maybe_t) => Some(maybe_t),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Option<Timestamp> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Option<Timestamp>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::MaybeTimestamp(ref maybe_ts) => Some(maybe_ts),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Option<UTCTimestampWithMaybeOffset> {
    fn get(
        record_tuple: &'a RecordTuple,
        index: usize,
    ) -> Option<&'a Option<UTCTimestampWithMaybeOffset>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::MaybeUTCTimestampWithMaybeOffset(ref maybe_ts) => Some(maybe_ts),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Vec<Bytes> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Vec<Bytes>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::ListOfBytes(ref vec_b) => Some(vec_b),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Vec<String> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Vec<String>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::ListOfString(ref vec_s) => Some(vec_s),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Vec<RecordTuple> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Vec<RecordTuple>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::ListOfRecordTuple(ref vec_rt) => Some(vec_rt),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for Vec<BigInt> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<Vec<BigInt>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::ListOfInteger(ref vec_i) => {
                Some(vec_i.iter().map(BigInt::from).collect::<Vec<BigInt>>())
            }
            _ => None,
        })
    }
}

// TODO: write tests!
impl<'a> RecordTupleElementGet<'a> for Vec<i64> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<Vec<i64>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::ListOfInteger(ref vec_i) => vec_i
                .iter()
                .try_fold(Vec::<i64>::new(), |mut v, i| {
                    v.push(i64::try_from(i)?);

                    Ok::<Vec<i64>, ()>(v)
                })
                .ok(),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for Vec<i32> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<Vec<i32>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::ListOfInteger(ref vec_i) => vec_i
                .iter()
                .try_fold(Vec::<i32>::new(), |mut v, i| {
                    v.push(i32::try_from(i)?);

                    Ok::<Vec<i32>, ()>(v)
                })
                .ok(),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for Vec<i16> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<Vec<i16>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::ListOfInteger(ref vec_i) => vec_i
                .iter()
                .try_fold(Vec::<i16>::new(), |mut v, i| {
                    v.push(i16::try_from(i)?);

                    Ok::<Vec<i16>, ()>(v)
                })
                .ok(),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for Vec<i8> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<Vec<i8>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::ListOfInteger(ref vec_i) => vec_i
                .iter()
                .try_fold(Vec::<i8>::new(), |mut v, i| {
                    v.push(i8::try_from(i)?);

                    Ok::<Vec<i8>, ()>(v)
                })
                .ok(),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for Vec<f32> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<Vec<f32>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::ListOfFloat(ref vec_f) => Some(vec_f.clone()),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for Vec<f64> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<Vec<f64>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::ListOfDouble(ref vec_d) => Some(vec_d.clone()),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for Vec<bool> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<Vec<bool>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::ListOfBoolean(ref vec_b) => Some(vec_b.clone()),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Vec<Uuid> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Vec<Uuid>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::ListOfUuid(ref vec_u) => Some(vec_u),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Vec<Versionstamp> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Vec<Versionstamp>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::ListOfVersionstamp(ref vec_v) => Some(vec_v),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Vec<Date> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Vec<Date>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::ListOfDate(ref vec_d) => Some(vec_d),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Vec<Time> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Vec<Time>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::ListOfTime(ref vec_t) => Some(vec_t),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Vec<UTCTimeWithMaybeOffset> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Vec<UTCTimeWithMaybeOffset>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::ListOfUTCTimeWithMaybeOffset(ref vec_t) => Some(vec_t),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Vec<Timestamp> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Vec<Timestamp>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::ListOfTimestamp(ref vec_ts) => Some(vec_ts),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Vec<UTCTimestampWithMaybeOffset> {
    fn get(
        record_tuple: &'a RecordTuple,
        index: usize,
    ) -> Option<&'a Vec<UTCTimestampWithMaybeOffset>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::ListOfUTCTimestampWithMaybeOffset(ref vec_ts) => Some(vec_ts),
            _ => None,
        })
    }
}

/// TODO
pub(crate) trait RecordTupleElementPush: private::SealedPush {
    #[doc(hidden)]
    fn push_back(record_tuple: &mut RecordTuple, value: Self);

    #[doc(hidden)]
    fn push_front(record_tuple: &mut RecordTuple, value: Self);
}

impl RecordTupleElementPush for Bytes {
    fn push_back(record_tuple: &mut RecordTuple, value: Bytes) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::Bytes(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Bytes) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::Bytes(value));
    }
}

impl RecordTupleElementPush for String {
    fn push_back(record_tuple: &mut RecordTuple, value: String) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::String(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: String) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::String(value));
    }
}

impl RecordTupleElementPush for RecordTuple {
    fn push_back(record_tuple: &mut RecordTuple, value: RecordTuple) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::RecordTuple(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: RecordTuple) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::RecordTuple(value));
    }
}

impl RecordTupleElementPush for BigInt {
    fn push_back(record_tuple: &mut RecordTuple, value: BigInt) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::Integer(RecordTupleValueInteger::from(
                value,
            )));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: BigInt) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::Integer(RecordTupleValueInteger::from(
                value,
            )));
    }
}

impl RecordTupleElementPush for i64 {
    fn push_back(record_tuple: &mut RecordTuple, value: i64) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::Integer(RecordTupleValueInteger::from(
                value,
            )));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: i64) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::Integer(RecordTupleValueInteger::from(
                value,
            )));
    }
}

impl RecordTupleElementPush for i32 {
    fn push_back(record_tuple: &mut RecordTuple, value: i32) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::Integer(RecordTupleValueInteger::from(
                value,
            )));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: i32) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::Integer(RecordTupleValueInteger::from(
                value,
            )));
    }
}

impl RecordTupleElementPush for i16 {
    fn push_back(record_tuple: &mut RecordTuple, value: i16) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::Integer(RecordTupleValueInteger::from(
                value,
            )));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: i16) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::Integer(RecordTupleValueInteger::from(
                value,
            )));
    }
}

impl RecordTupleElementPush for i8 {
    fn push_back(record_tuple: &mut RecordTuple, value: i8) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::Integer(RecordTupleValueInteger::from(
                value,
            )));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: i8) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::Integer(RecordTupleValueInteger::from(
                value,
            )));
    }
}

impl RecordTupleElementPush for f32 {
    fn push_back(record_tuple: &mut RecordTuple, value: f32) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::Float(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: f32) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::Float(value));
    }
}

impl RecordTupleElementPush for f64 {
    fn push_back(record_tuple: &mut RecordTuple, value: f64) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::Double(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: f64) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::Double(value));
    }
}

impl RecordTupleElementPush for bool {
    fn push_back(record_tuple: &mut RecordTuple, value: bool) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::Boolean(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: bool) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::Boolean(value));
    }
}

impl RecordTupleElementPush for Uuid {
    fn push_back(record_tuple: &mut RecordTuple, value: Uuid) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::Uuid(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Uuid) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::Uuid(value));
    }
}

impl RecordTupleElementPush for Versionstamp {
    fn push_back(record_tuple: &mut RecordTuple, value: Versionstamp) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::Versionstamp(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Versionstamp) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::Versionstamp(value));
    }
}

impl RecordTupleElementPush for Date {
    fn push_back(record_tuple: &mut RecordTuple, value: Date) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::Date(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Date) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::Date(value));
    }
}

impl RecordTupleElementPush for Time {
    fn push_back(record_tuple: &mut RecordTuple, value: Time) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::Time(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Time) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::Time(value));
    }
}

impl RecordTupleElementPush for UTCTimeWithMaybeOffset {
    fn push_back(record_tuple: &mut RecordTuple, value: UTCTimeWithMaybeOffset) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::UTCTimeWithMaybeOffset(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: UTCTimeWithMaybeOffset) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::UTCTimeWithMaybeOffset(value));
    }
}

impl RecordTupleElementPush for Timestamp {
    fn push_back(record_tuple: &mut RecordTuple, value: Timestamp) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::Timestamp(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Timestamp) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::Timestamp(value));
    }
}

impl RecordTupleElementPush for UTCTimestampWithMaybeOffset {
    fn push_back(record_tuple: &mut RecordTuple, value: UTCTimestampWithMaybeOffset) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::UTCTimestampWithMaybeOffset(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: UTCTimestampWithMaybeOffset) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::UTCTimestampWithMaybeOffset(value));
    }
}

impl RecordTupleElementPush for Option<Bytes> {
    fn push_back(record_tuple: &mut RecordTuple, value: Option<Bytes>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::MaybeBytes(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Option<Bytes>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::MaybeBytes(value));
    }
}

impl RecordTupleElementPush for Option<String> {
    fn push_back(record_tuple: &mut RecordTuple, value: Option<String>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::MaybeString(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Option<String>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::MaybeString(value));
    }
}

impl RecordTupleElementPush for Option<RecordTuple> {
    fn push_back(record_tuple: &mut RecordTuple, value: Option<RecordTuple>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::MaybeRecordTuple(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Option<RecordTuple>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::MaybeRecordTuple(value));
    }
}

impl RecordTupleElementPush for Option<BigInt> {
    fn push_back(record_tuple: &mut RecordTuple, value: Option<BigInt>) {
        match value {
            Some(v) => record_tuple
                .elements
                .push_back(RecordTupleValue::MaybeInteger(Some(
                    RecordTupleValueInteger::from(v),
                ))),
            None => record_tuple
                .elements
                .push_back(RecordTupleValue::MaybeInteger(None)),
        }
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Option<BigInt>) {
        match value {
            Some(v) => record_tuple
                .elements
                .push_front(RecordTupleValue::MaybeInteger(Some(
                    RecordTupleValueInteger::from(v),
                ))),
            None => record_tuple
                .elements
                .push_front(RecordTupleValue::MaybeInteger(None)),
        }
    }
}

impl RecordTupleElementPush for Option<i64> {
    fn push_back(record_tuple: &mut RecordTuple, value: Option<i64>) {
        match value {
            Some(v) => record_tuple
                .elements
                .push_back(RecordTupleValue::MaybeInteger(Some(
                    RecordTupleValueInteger::from(v),
                ))),
            None => record_tuple
                .elements
                .push_back(RecordTupleValue::MaybeInteger(None)),
        }
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Option<i64>) {
        match value {
            Some(v) => record_tuple
                .elements
                .push_front(RecordTupleValue::MaybeInteger(Some(
                    RecordTupleValueInteger::from(v),
                ))),
            None => record_tuple
                .elements
                .push_front(RecordTupleValue::MaybeInteger(None)),
        }
    }
}

impl RecordTupleElementPush for Option<i32> {
    fn push_back(record_tuple: &mut RecordTuple, value: Option<i32>) {
        match value {
            Some(v) => record_tuple
                .elements
                .push_back(RecordTupleValue::MaybeInteger(Some(
                    RecordTupleValueInteger::from(v),
                ))),
            None => record_tuple
                .elements
                .push_back(RecordTupleValue::MaybeInteger(None)),
        }
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Option<i32>) {
        match value {
            Some(v) => record_tuple
                .elements
                .push_front(RecordTupleValue::MaybeInteger(Some(
                    RecordTupleValueInteger::from(v),
                ))),
            None => record_tuple
                .elements
                .push_front(RecordTupleValue::MaybeInteger(None)),
        }
    }
}

impl RecordTupleElementPush for Option<i16> {
    fn push_back(record_tuple: &mut RecordTuple, value: Option<i16>) {
        match value {
            Some(v) => record_tuple
                .elements
                .push_back(RecordTupleValue::MaybeInteger(Some(
                    RecordTupleValueInteger::from(v),
                ))),
            None => record_tuple
                .elements
                .push_back(RecordTupleValue::MaybeInteger(None)),
        }
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Option<i16>) {
        match value {
            Some(v) => record_tuple
                .elements
                .push_front(RecordTupleValue::MaybeInteger(Some(
                    RecordTupleValueInteger::from(v),
                ))),
            None => record_tuple
                .elements
                .push_front(RecordTupleValue::MaybeInteger(None)),
        }
    }
}

impl RecordTupleElementPush for Option<i8> {
    fn push_back(record_tuple: &mut RecordTuple, value: Option<i8>) {
        match value {
            Some(v) => record_tuple
                .elements
                .push_back(RecordTupleValue::MaybeInteger(Some(
                    RecordTupleValueInteger::from(v),
                ))),
            None => record_tuple
                .elements
                .push_back(RecordTupleValue::MaybeInteger(None)),
        }
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Option<i8>) {
        match value {
            Some(v) => record_tuple
                .elements
                .push_front(RecordTupleValue::MaybeInteger(Some(
                    RecordTupleValueInteger::from(v),
                ))),
            None => record_tuple
                .elements
                .push_front(RecordTupleValue::MaybeInteger(None)),
        }
    }
}

impl RecordTupleElementPush for Option<f32> {
    fn push_back(record_tuple: &mut RecordTuple, value: Option<f32>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::MaybeFloat(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Option<f32>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::MaybeFloat(value));
    }
}

impl RecordTupleElementPush for Option<f64> {
    fn push_back(record_tuple: &mut RecordTuple, value: Option<f64>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::MaybeDouble(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Option<f64>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::MaybeDouble(value));
    }
}

impl RecordTupleElementPush for Option<bool> {
    fn push_back(record_tuple: &mut RecordTuple, value: Option<bool>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::MaybeBoolean(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Option<bool>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::MaybeBoolean(value));
    }
}

impl RecordTupleElementPush for Option<Uuid> {
    fn push_back(record_tuple: &mut RecordTuple, value: Option<Uuid>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::MaybeUuid(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Option<Uuid>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::MaybeUuid(value));
    }
}

impl RecordTupleElementPush for Option<Versionstamp> {
    fn push_back(record_tuple: &mut RecordTuple, value: Option<Versionstamp>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::MaybeVersionstamp(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Option<Versionstamp>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::MaybeVersionstamp(value));
    }
}

impl RecordTupleElementPush for Option<Date> {
    fn push_back(record_tuple: &mut RecordTuple, value: Option<Date>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::MaybeDate(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Option<Date>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::MaybeDate(value));
    }
}

impl RecordTupleElementPush for Option<Time> {
    fn push_back(record_tuple: &mut RecordTuple, value: Option<Time>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::MaybeTime(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Option<Time>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::MaybeTime(value));
    }
}

impl RecordTupleElementPush for Option<UTCTimeWithMaybeOffset> {
    fn push_back(record_tuple: &mut RecordTuple, value: Option<UTCTimeWithMaybeOffset>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::MaybeUTCTimeWithMaybeOffset(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Option<UTCTimeWithMaybeOffset>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::MaybeUTCTimeWithMaybeOffset(value));
    }
}

impl RecordTupleElementPush for Option<Timestamp> {
    fn push_back(record_tuple: &mut RecordTuple, value: Option<Timestamp>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::MaybeTimestamp(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Option<Timestamp>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::MaybeTimestamp(value));
    }
}

impl RecordTupleElementPush for Option<UTCTimestampWithMaybeOffset> {
    fn push_back(record_tuple: &mut RecordTuple, value: Option<UTCTimestampWithMaybeOffset>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::MaybeUTCTimestampWithMaybeOffset(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Option<UTCTimestampWithMaybeOffset>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::MaybeUTCTimestampWithMaybeOffset(value));
    }
}

impl RecordTupleElementPush for Vec<Bytes> {
    fn push_back(record_tuple: &mut RecordTuple, value: Vec<Bytes>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::ListOfBytes(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Vec<Bytes>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::ListOfBytes(value));
    }
}

impl RecordTupleElementPush for Vec<String> {
    fn push_back(record_tuple: &mut RecordTuple, value: Vec<String>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::ListOfString(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Vec<String>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::ListOfString(value));
    }
}

impl RecordTupleElementPush for Vec<RecordTuple> {
    fn push_back(record_tuple: &mut RecordTuple, value: Vec<RecordTuple>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::ListOfRecordTuple(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Vec<RecordTuple>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::ListOfRecordTuple(value));
    }
}

impl RecordTupleElementPush for Vec<BigInt> {
    fn push_back(record_tuple: &mut RecordTuple, value: Vec<BigInt>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::ListOfInteger(
                value
                    .into_iter()
                    .map(RecordTupleValueInteger::from)
                    .collect::<Vec<RecordTupleValueInteger>>(),
            ));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Vec<BigInt>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::ListOfInteger(
                value
                    .into_iter()
                    .map(RecordTupleValueInteger::from)
                    .collect::<Vec<RecordTupleValueInteger>>(),
            ));
    }
}

impl RecordTupleElementPush for Vec<i64> {
    fn push_back(record_tuple: &mut RecordTuple, value: Vec<i64>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::ListOfInteger(
                value
                    .into_iter()
                    .map(RecordTupleValueInteger::from)
                    .collect::<Vec<RecordTupleValueInteger>>(),
            ));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Vec<i64>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::ListOfInteger(
                value
                    .into_iter()
                    .map(RecordTupleValueInteger::from)
                    .collect::<Vec<RecordTupleValueInteger>>(),
            ));
    }
}

impl RecordTupleElementPush for Vec<i32> {
    fn push_back(record_tuple: &mut RecordTuple, value: Vec<i32>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::ListOfInteger(
                value
                    .into_iter()
                    .map(RecordTupleValueInteger::from)
                    .collect::<Vec<RecordTupleValueInteger>>(),
            ));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Vec<i32>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::ListOfInteger(
                value
                    .into_iter()
                    .map(RecordTupleValueInteger::from)
                    .collect::<Vec<RecordTupleValueInteger>>(),
            ));
    }
}

impl RecordTupleElementPush for Vec<i16> {
    fn push_back(record_tuple: &mut RecordTuple, value: Vec<i16>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::ListOfInteger(
                value
                    .into_iter()
                    .map(RecordTupleValueInteger::from)
                    .collect::<Vec<RecordTupleValueInteger>>(),
            ));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Vec<i16>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::ListOfInteger(
                value
                    .into_iter()
                    .map(RecordTupleValueInteger::from)
                    .collect::<Vec<RecordTupleValueInteger>>(),
            ));
    }
}

impl RecordTupleElementPush for Vec<i8> {
    fn push_back(record_tuple: &mut RecordTuple, value: Vec<i8>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::ListOfInteger(
                value
                    .into_iter()
                    .map(RecordTupleValueInteger::from)
                    .collect::<Vec<RecordTupleValueInteger>>(),
            ));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Vec<i8>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::ListOfInteger(
                value
                    .into_iter()
                    .map(RecordTupleValueInteger::from)
                    .collect::<Vec<RecordTupleValueInteger>>(),
            ));
    }
}

impl RecordTupleElementPush for Vec<f32> {
    fn push_back(record_tuple: &mut RecordTuple, value: Vec<f32>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::ListOfFloat(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Vec<f32>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::ListOfFloat(value));
    }
}

impl RecordTupleElementPush for Vec<f64> {
    fn push_back(record_tuple: &mut RecordTuple, value: Vec<f64>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::ListOfDouble(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Vec<f64>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::ListOfDouble(value));
    }
}

impl RecordTupleElementPush for Vec<bool> {
    fn push_back(record_tuple: &mut RecordTuple, value: Vec<bool>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::ListOfBoolean(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Vec<bool>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::ListOfBoolean(value));
    }
}

impl RecordTupleElementPush for Vec<Uuid> {
    fn push_back(record_tuple: &mut RecordTuple, value: Vec<Uuid>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::ListOfUuid(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Vec<Uuid>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::ListOfUuid(value));
    }
}

impl RecordTupleElementPush for Vec<Versionstamp> {
    fn push_back(record_tuple: &mut RecordTuple, value: Vec<Versionstamp>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::ListOfVersionstamp(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Vec<Versionstamp>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::ListOfVersionstamp(value));
    }
}

impl RecordTupleElementPush for Vec<Date> {
    fn push_back(record_tuple: &mut RecordTuple, value: Vec<Date>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::ListOfDate(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Vec<Date>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::ListOfDate(value));
    }
}

impl RecordTupleElementPush for Vec<Time> {
    fn push_back(record_tuple: &mut RecordTuple, value: Vec<Time>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::ListOfTime(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Vec<Time>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::ListOfTime(value));
    }
}

impl RecordTupleElementPush for Vec<UTCTimeWithMaybeOffset> {
    fn push_back(record_tuple: &mut RecordTuple, value: Vec<UTCTimeWithMaybeOffset>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::ListOfUTCTimeWithMaybeOffset(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Vec<UTCTimeWithMaybeOffset>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::ListOfUTCTimeWithMaybeOffset(value));
    }
}

impl RecordTupleElementPush for Vec<Timestamp> {
    fn push_back(record_tuple: &mut RecordTuple, value: Vec<Timestamp>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::ListOfTimestamp(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Vec<Timestamp>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::ListOfTimestamp(value));
    }
}

impl RecordTupleElementPush for Vec<UTCTimestampWithMaybeOffset> {
    fn push_back(record_tuple: &mut RecordTuple, value: Vec<UTCTimestampWithMaybeOffset>) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::ListOfUTCTimestampWithMaybeOffset(value));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: Vec<UTCTimestampWithMaybeOffset>) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::ListOfUTCTimestampWithMaybeOffset(value));
    }
}

pub(crate) trait RecordTupleElementPop: private::SealedPop {
    #[doc(hidden)]
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Self>
    where
        Self: Sized;

    #[doc(hidden)]
    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Self>
    where
        Self: Sized;
}

impl RecordTupleElementPop for Bytes {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Bytes> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Bytes`. In that
            // case, we will need to push the `RecordTupleValue` back
            // before returning `None`.
            Bytes::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Bytes> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Bytes`. In that
            // case, we will need to push the `RecordTupleValue` to
            // the front before returning `None`.
            Bytes::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for String {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<String> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `String`. In that
            // case, we will need to push the `RecordTupleValue` back
            // before returning `None`.
            String::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<String> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `String`. In that
            // case, we will need to push the `RecordTupleValue` to
            // the front before returning `None`.
            String::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for RecordTuple {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<RecordTuple> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `RecordTuple`. In
            // that case, we will need to push the `RecordTupleValue`
            // back before returning `None`.
            RecordTuple::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<RecordTuple> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `RecordTuple`. In
            // that case, we will need to push the `RecordTupleValue`
            // to the front before returning `None`.
            RecordTuple::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for BigInt {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<BigInt> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `BigInt`. In that
            // case, we will need to push the `RecordTupleValue` back
            // before returning `None`.
            BigInt::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<BigInt> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `BigInt`. In that
            // case, we will need to push the `RecordTupleValue` to
            // the front before returning `None`.
            BigInt::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for i64 {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<i64> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `i64`. In that case,
            // we will need to push the `RecordTupleValue` back before
            // returning `None`.
            i64::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<i64> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `i64`. In that case,
            // we will need to push the `RecordTupleValue` to the
            // front before returning `None`.
            i64::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for i32 {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<i32> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `i32`. In that case,
            // we will need to push the `RecordTupleValue` back before
            // returning `None`.
            i32::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<i32> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `i32`. In that case,
            // we will need to push the `RecordTupleValue` to the
            // front before returning `None`.
            i32::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for i16 {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<i16> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `i16`. In that case,
            // we will need to push the `RecordTupleValue` back before
            // returning `None`.
            i16::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<i16> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `i16`. In that case,
            // we will need to push the `RecordTupleValue` to the
            // front before returning `None`.
            i16::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for i8 {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<i8> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `i8`. In that case,
            // we will need to push the `RecordTupleValue` back before
            // returning `None`.
            i8::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<i8> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `i8`. In that case,
            // we will need to push the `RecordTupleValue` to the
            // front before returning `None`.
            i8::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for f32 {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<f32> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `f32`. In that case,
            // we will need to push the `RecordTupleValue` back before
            // returning `None`.
            f32::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<f32> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `f32`. In that case,
            // we will need to push the `RecordTupleValue` to the
            // front before returning `None`.
            f32::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for f64 {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<f64> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `f64`. In that case,
            // we will need to push the `RecordTupleValue` back before
            // returning `None`.
            f64::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<f64> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `f64`. In that case,
            // we will need to push the `RecordTupleValue` to the
            // front before returning `None`.
            f64::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for bool {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<bool> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `bool`. In that case,
            // we will need to push the `RecordTupleValue` back before
            // returning `None`.
            bool::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<bool> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `bool`. In that case,
            // we will need to push the `RecordTupleValue` to the
            // front before returning `None`.
            bool::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Uuid {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Uuid> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Uuid`. In that case,
            // we will need to push the `RecordTupleValue` back before
            // returning `None`.
            Uuid::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Uuid> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Uuid`. In that case,
            // we will need to push the `RecordTupleValue` to the
            // front before returning `None`.
            Uuid::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Versionstamp {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Versionstamp> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Versionstamp`. In
            // that case, we will need to push the `RecordTupleValue`
            // back before returning `None`.
            Versionstamp::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Versionstamp> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Versionstamp`. In
            // that case, we will need to push the `RecordTupleValue`
            // to the front before returning `None`.
            Versionstamp::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Date {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Date> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Date`. In that case,
            // we will need to push the `RecordTupleValue` back before
            // returning `None`.
            Date::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Date> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Date`. In that case,
            // we will need to push the `RecordTupleValue` to the
            // front before returning `None`.
            Date::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Time {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Time> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Time`. In that case,
            // we will need to push the `RecordTupleValue` back before
            // returning `None`.
            Time::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Time> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Time`. In that case,
            // we will need to push the `RecordTupleValue` to the
            // front before returning `None`.
            Time::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for UTCTimeWithMaybeOffset {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<UTCTimeWithMaybeOffset> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a
            // `UTCTimeWithMaybeOffset`. In that case, we will need to
            // push the `RecordTupleValue` back before returning
            // `None`.
            UTCTimeWithMaybeOffset::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<UTCTimeWithMaybeOffset> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a
            // `UTCTimeWithMaybeOffset`. In that case, we will need to
            // push the `RecordTupleValue` to the front before
            // returning `None`.
            UTCTimeWithMaybeOffset::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Timestamp {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Timestamp> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Timestamp`. In that
            // case, we will need to push the `RecordTupleValue` back
            // before returning `None`.
            Timestamp::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Timestamp> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Timestamp`. In that
            // case, we will need to push the `RecordTupleValue` to
            // the front before returning `None`.
            Timestamp::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for UTCTimestampWithMaybeOffset {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<UTCTimestampWithMaybeOffset> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `UTCTimestampWithMaybeOffset`. In that
            // case, we will need to push the `RecordTupleValue` back
            // before returning `None`.
            UTCTimestampWithMaybeOffset::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<UTCTimestampWithMaybeOffset> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `UTCTimestampWithMaybeOffset`. In that
            // case, we will need to push the `RecordTupleValue` to
            // the front before returning `None`.
            UTCTimestampWithMaybeOffset::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Option<Bytes> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Option<Bytes>> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<Bytes>`. In
            // that case, we will need to push the `RecordTupleValue`
            // back before returning `None`.
            Option::<Bytes>::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Option<Bytes>> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<Bytes>`. In
            // that case, we will need to push the `RecordTupleValue`
            // to the front before returning `None`.
            Option::<Bytes>::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Option<String> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Option<String>> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<String>`. In
            // that case, we will need to push the `RecordTupleValue`
            // back before returning `None`.
            Option::<String>::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Option<String>> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<String>`. In
            // that case, we will need to push the `RecordTupleValue`
            // to the front before returning `None`.
            Option::<String>::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Option<RecordTuple> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Option<RecordTuple>> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a
            // `Option<RecordTuple>`. In that case, we will need to
            // push the `RecordTupleValue` back before returning
            // `None`.
            Option::<RecordTuple>::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Option<RecordTuple>> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a
            // `Option<RecordTuple>`. In that case, we will need to
            // push the `RecordTupleValue` to the front before
            // returning `None`.
            Option::<RecordTuple>::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Option<BigInt> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Option<BigInt>> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<BigInt>`. In
            // that case, we will need to push the `RecordTupleValue`
            // back before returning `None`.
            Option::<BigInt>::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Option<BigInt>> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<BigInt>`. In
            // that case, we will need to push the `RecordTupleValue`
            // to the front before returning `None`.
            Option::<BigInt>::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Option<i64> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Option<i64>> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<i64>`. In
            // that case, we will need to push the `RecordTupleValue`
            // back before returning `None`.
            Option::<i64>::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Option<i64>> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<i64>`. In
            // that case, we will need to push the `RecordTupleValue`
            // to the front before returning `None`.
            Option::<i64>::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Option<i32> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Option<i32>> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<i32>`. In
            // that case, we will need to push the `RecordTupleValue`
            // back before returning `None`.
            Option::<i32>::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Option<i32>> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<i32>`. In
            // that case, we will need to push the `RecordTupleValue`
            // to the front before returning `None`.
            Option::<i32>::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Option<i16> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Option<i16>> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<i16>`. In
            // that case, we will need to push the `RecordTupleValue`
            // back before returning `None`.
            Option::<i16>::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Option<i16>> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<i16>`. In
            // that case, we will need to push the `RecordTupleValue`
            // to the front before returning `None`.
            Option::<i16>::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Option<i8> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Option<i8>> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<i8>`. In that
            // case, we will need to push the `RecordTupleValue` back
            // before returning `None`.
            Option::<i8>::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Option<i8>> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<i8>`. In that
            // case, we will need to push the `RecordTupleValue` to
            // the front before returning `None`.
            Option::<i8>::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Option<f32> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Option<f32>> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<f32>`. In
            // that case, we will need to push the `RecordTupleValue`
            // back before returning `None`.
            Option::<f32>::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Option<f32>> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<f32>`. In
            // that case, we will need to push the `RecordTupleValue`
            // to the front before returning `None`.
            Option::<f32>::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Option<f64> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Option<f64>> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<f64>`. In
            // that case, we will need to push the `RecordTupleValue`
            // back before returning `None`.
            Option::<f64>::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Option<f64>> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<f64>`. In
            // that case, we will need to push the `RecordTupleValue`
            // to the front before returning `None`.
            Option::<f64>::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Option<bool> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Option<bool>> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<bool>`. In
            // that case, we will need to push the `RecordTupleValue`
            // back before returning `None`.
            Option::<bool>::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Option<bool>> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<bool>`. In
            // that case, we will need to push the `RecordTupleValue`
            // to the front before returning `None`.
            Option::<bool>::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Option<Uuid> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Option<Uuid>> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<Uuid>`. In
            // that case, we will need to push the `RecordTupleValue`
            // back before returning `None`.
            Option::<Uuid>::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Option<Uuid>> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<Uuid>`. In
            // that case, we will need to push the `RecordTupleValue`
            // to the front before returning `None`.
            Option::<Uuid>::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Option<Versionstamp> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Option<Versionstamp>> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a
            // `Option<Versionstamp>`. In that case, we will need to
            // push the `RecordTupleValue` back before returning
            // `None`.
            Option::<Versionstamp>::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Option<Versionstamp>> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a
            // `Option<Versionstamp>`. In that case, we will need to
            // push the `RecordTupleValue` to the front before
            // returning `None`.
            Option::<Versionstamp>::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Option<Date> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Option<Date>> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<Date>`. In
            // that case, we will need to push the `RecordTupleValue`
            // back before returning `None`.
            Option::<Date>::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Option<Date>> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<Date>`. In
            // that case, we will need to push the `RecordTupleValue`
            // to the front before returning `None`.
            Option::<Date>::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Option<Time> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Option<Time>> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<Time>`. In
            // that case, we will need to push the `RecordTupleValue`
            // back before returning `None`.
            Option::<Time>::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Option<Time>> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a `Option<Time>`. In
            // that case, we will need to push the `RecordTupleValue`
            // to the front before returning `None`.
            Option::<Time>::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Option<UTCTimeWithMaybeOffset> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Option<UTCTimeWithMaybeOffset>> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a
            // `Option<UTCTimeWithMaybeOffset>`. In that case, we will
            // need to push the `RecordTupleValue` back before
            // returning `None`.
            Option::<UTCTimeWithMaybeOffset>::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Option<UTCTimeWithMaybeOffset>> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a
            // `Option<UTCTimeWithMaybeOffset>`. In that case, we will
            // need to push the `RecordTupleValue` to the front before
            // returning `None`.
            Option::<UTCTimeWithMaybeOffset>::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Option<Timestamp> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Option<Timestamp>> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a
            // `Option<Timestamp>`. In that case, we will need to push
            // the `RecordTupleValue` back before returning `None`.
            Option::<Timestamp>::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Option<Timestamp>> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a
            // `Option<Timestamp>`. In that case, we will need to push
            // the `RecordTupleValue` to the front before returning
            // `None`.
            Option::<Timestamp>::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Option<UTCTimestampWithMaybeOffset> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Option<UTCTimestampWithMaybeOffset>> {
        record_tuple.elements.pop_back().and_then(|tail| {
            // `.pop_back()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a
            // `Option<UTCTimestampWithMaybeOffset>`. In that case, we
            // will need to push the `RecordTupleValue` back before
            // returning `None`.
            Option::<UTCTimestampWithMaybeOffset>::try_from(tail.clone())
                .map_err(|_| record_tuple.elements.push_back(tail))
                .ok()
        })
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Option<UTCTimestampWithMaybeOffset>> {
        record_tuple.elements.pop_front().and_then(|head| {
            // `.pop_front()` mutates the `VecDeque`. The returned
            // `RecordTupleValue` might not be a
            // `Option<UTCTimestampWithMaybeOffset>`. In that case, we
            // will need to push the `RecordTupleValue` to the front
            // before returning `None`.
            Option::<UTCTimestampWithMaybeOffset>::try_from(head.clone())
                .map_err(|_| record_tuple.elements.push_front(head))
                .ok()
        })
    }
}

impl RecordTupleElementPop for Vec<Bytes> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Vec<Bytes>> {
        todo!();
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Vec<Bytes>> {
        todo!();
    }
}

impl RecordTupleElementPop for Vec<String> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Vec<String>> {
        todo!();
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Vec<String>> {
        todo!();
    }
}

impl RecordTupleElementPop for Vec<RecordTuple> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Vec<RecordTuple>> {
        todo!();
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Vec<RecordTuple>> {
        todo!();
    }
}

impl RecordTupleElementPop for Vec<BigInt> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Vec<BigInt>> {
        todo!();
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Vec<BigInt>> {
        todo!();
    }
}

impl RecordTupleElementPop for Vec<i64> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Vec<i64>> {
        todo!();
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Vec<i64>> {
        todo!();
    }
}

impl RecordTupleElementPop for Vec<i32> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Vec<i32>> {
        todo!();
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Vec<i32>> {
        todo!();
    }
}

impl RecordTupleElementPop for Vec<i16> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Vec<i16>> {
        todo!();
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Vec<i16>> {
        todo!();
    }
}

impl RecordTupleElementPop for Vec<i8> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Vec<i8>> {
        todo!();
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Vec<i8>> {
        todo!();
    }
}

impl RecordTupleElementPop for Vec<f32> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Vec<f32>> {
        todo!();
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Vec<f32>> {
        todo!();
    }
}

impl RecordTupleElementPop for Vec<f64> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Vec<f64>> {
        todo!();
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Vec<f64>> {
        todo!();
    }
}

impl RecordTupleElementPop for Vec<bool> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Vec<bool>> {
        todo!();
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Vec<bool>> {
        todo!();
    }
}

impl RecordTupleElementPop for Vec<Uuid> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Vec<Uuid>> {
        todo!();
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Vec<Uuid>> {
        todo!();
    }
}

impl RecordTupleElementPop for Vec<Versionstamp> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Vec<Versionstamp>> {
        todo!();
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Vec<Versionstamp>> {
        todo!();
    }
}

impl RecordTupleElementPop for Vec<Date> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Vec<Date>> {
        todo!();
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Vec<Date>> {
        todo!();
    }
}

impl RecordTupleElementPop for Vec<Time> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Vec<Time>> {
        todo!();
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Vec<Time>> {
        todo!();
    }
}

impl RecordTupleElementPop for Vec<UTCTimeWithMaybeOffset> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Vec<UTCTimeWithMaybeOffset>> {
        todo!();
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Vec<UTCTimeWithMaybeOffset>> {
        todo!();
    }
}

impl RecordTupleElementPop for Vec<Timestamp> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Vec<Timestamp>> {
        todo!();
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Vec<Timestamp>> {
        todo!();
    }
}

impl RecordTupleElementPop for Vec<UTCTimestampWithMaybeOffset> {
    fn pop_back(record_tuple: &mut RecordTuple) -> Option<Vec<UTCTimestampWithMaybeOffset>> {
        todo!();
    }

    fn pop_front(record_tuple: &mut RecordTuple) -> Option<Vec<UTCTimestampWithMaybeOffset>> {
        todo!();
    }
}

/// Needed for [`RecordTupleSchemaElement::Integer`] variant because
/// we do not have anonymous enums.
///
/// `RecordTupleValue::Integer` is stored in the most optimal
/// variant. So, a value of `0` is stored as an `i8` and not as `i16`.
#[derive(Clone, PartialEq, Debug)]
enum RecordTupleValueInteger {
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    BigInt(BigInt),
}

impl From<&RecordTupleValueInteger> for BigInt {
    fn from(record_tuple_value_integer_ref: &RecordTupleValueInteger) -> BigInt {
        match *record_tuple_value_integer_ref {
            RecordTupleValueInteger::I8(i) => BigInt::from(i),
            RecordTupleValueInteger::I16(i) => BigInt::from(i),
            RecordTupleValueInteger::I32(i) => BigInt::from(i),
            RecordTupleValueInteger::I64(i) => BigInt::from(i),
            RecordTupleValueInteger::BigInt(ref i) => i.clone(),
        }
    }
}

impl TryFrom<&RecordTupleValueInteger> for i64 {
    type Error = ();

    fn try_from(record_tuple_value_integer_ref: &RecordTupleValueInteger) -> Result<i64, ()> {
        match *record_tuple_value_integer_ref {
            RecordTupleValueInteger::I8(i) => Ok(i64::from(i)),
            RecordTupleValueInteger::I16(i) => Ok(i64::from(i)),
            RecordTupleValueInteger::I32(i) => Ok(i64::from(i)),
            RecordTupleValueInteger::I64(i) => Ok(i),
            RecordTupleValueInteger::BigInt(_) => Err(()),
        }
    }
}

impl TryFrom<&RecordTupleValueInteger> for i32 {
    type Error = ();

    fn try_from(record_tuple_value_integer_ref: &RecordTupleValueInteger) -> Result<i32, ()> {
        match *record_tuple_value_integer_ref {
            RecordTupleValueInteger::I8(i) => Ok(i32::from(i)),
            RecordTupleValueInteger::I16(i) => Ok(i32::from(i)),
            RecordTupleValueInteger::I32(i) => Ok(i),
            RecordTupleValueInteger::I64(_) | RecordTupleValueInteger::BigInt(_) => Err(()),
        }
    }
}

impl TryFrom<&RecordTupleValueInteger> for i16 {
    type Error = ();

    fn try_from(record_tuple_value_integer_ref: &RecordTupleValueInteger) -> Result<i16, ()> {
        match *record_tuple_value_integer_ref {
            RecordTupleValueInteger::I8(i) => Ok(i16::from(i)),
            RecordTupleValueInteger::I16(i) => Ok(i),
            RecordTupleValueInteger::I32(_)
            | RecordTupleValueInteger::I64(_)
            | RecordTupleValueInteger::BigInt(_) => Err(()),
        }
    }
}

impl TryFrom<&RecordTupleValueInteger> for i8 {
    type Error = ();

    fn try_from(record_tuple_value_integer_ref: &RecordTupleValueInteger) -> Result<i8, ()> {
        match *record_tuple_value_integer_ref {
            RecordTupleValueInteger::I8(i) => Ok(i),
            RecordTupleValueInteger::I16(_)
            | RecordTupleValueInteger::I32(_)
            | RecordTupleValueInteger::I64(_)
            | RecordTupleValueInteger::BigInt(_) => Err(()),
        }
    }
}

impl From<BigInt> for RecordTupleValueInteger {
    fn from(value: BigInt) -> RecordTupleValueInteger {
        i64::try_from(value.clone())
            .map(RecordTupleValueInteger::from)
            .unwrap_or_else(|_| RecordTupleValueInteger::BigInt(value))
    }
}

impl From<i64> for RecordTupleValueInteger {
    fn from(value: i64) -> RecordTupleValueInteger {
        i32::try_from(value)
            .map(RecordTupleValueInteger::from)
            .unwrap_or_else(|_| RecordTupleValueInteger::I64(value))
    }
}

impl From<i32> for RecordTupleValueInteger {
    fn from(value: i32) -> RecordTupleValueInteger {
        i16::try_from(value)
            .map(RecordTupleValueInteger::from)
            .unwrap_or_else(|_| RecordTupleValueInteger::I32(value))
    }
}

impl From<i16> for RecordTupleValueInteger {
    fn from(value: i16) -> RecordTupleValueInteger {
        i8::try_from(value)
            .map(RecordTupleValueInteger::from)
            .unwrap_or_else(|_| RecordTupleValueInteger::I16(value))
    }
}

impl From<i8> for RecordTupleValueInteger {
    fn from(value: i8) -> RecordTupleValueInteger {
        RecordTupleValueInteger::I8(value)
    }
}

/// TODO
#[derive(Clone, PartialEq, Debug)]
pub(crate) struct UTCTimeWithMaybeOffset(Time, Option<UtcOffset>);

/// TODO
#[derive(Clone, PartialEq, Debug)]
pub(crate) struct Timestamp(Date, Time);

/// TODO
#[derive(Clone, PartialEq, Debug)]
pub(crate) struct UTCTimestampWithMaybeOffset(Date, Time, Option<UtcOffset>);

/// TODO
#[derive(Clone, PartialEq, Debug)]
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
    UTCTimeWithMaybeOffset(UTCTimeWithMaybeOffset),
    Timestamp(Timestamp),
    UTCTimestampWithMaybeOffset(UTCTimestampWithMaybeOffset),
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
    MaybeUTCTimeWithMaybeOffset(Option<UTCTimeWithMaybeOffset>),
    MaybeTimestamp(Option<Timestamp>),
    MaybeUTCTimestampWithMaybeOffset(Option<UTCTimestampWithMaybeOffset>),
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
    ListOfUTCTimeWithMaybeOffset(Vec<UTCTimeWithMaybeOffset>),
    ListOfTimestamp(Vec<Timestamp>),
    ListOfUTCTimestampWithMaybeOffset(Vec<UTCTimestampWithMaybeOffset>),
}

impl TryFrom<RecordTupleValue> for Bytes {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Bytes, ()> {
        match value {
            RecordTupleValue::Bytes(b) => Ok(b),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for String {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<String, ()> {
        match value {
            RecordTupleValue::String(s) => Ok(s),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for RecordTuple {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<RecordTuple, ()> {
        match value {
            RecordTupleValue::RecordTuple(t) => Ok(t),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for BigInt {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<BigInt, ()> {
        match value {
            RecordTupleValue::Integer(i) => Ok(BigInt::from(&i)),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for i64 {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<i64, ()> {
        match value {
            RecordTupleValue::Integer(i) => i64::try_from(&i),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for i32 {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<i32, ()> {
        match value {
            RecordTupleValue::Integer(i) => i32::try_from(&i),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for i16 {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<i16, ()> {
        match value {
            RecordTupleValue::Integer(i) => i16::try_from(&i),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for i8 {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<i8, ()> {
        match value {
            RecordTupleValue::Integer(i) => i8::try_from(&i),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for f32 {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<f32, ()> {
        match value {
            RecordTupleValue::Float(f) => Ok(f),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for f64 {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<f64, ()> {
        match value {
            RecordTupleValue::Double(d) => Ok(d),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for bool {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<bool, ()> {
        match value {
            RecordTupleValue::Boolean(b) => Ok(b),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for Uuid {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Uuid, ()> {
        match value {
            RecordTupleValue::Uuid(u) => Ok(u),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for Versionstamp {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Versionstamp, ()> {
        match value {
            RecordTupleValue::Versionstamp(vs) => Ok(vs),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for Date {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Date, ()> {
        match value {
            RecordTupleValue::Date(d) => Ok(d),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for Time {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Time, ()> {
        match value {
            RecordTupleValue::Time(t) => Ok(t),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for UTCTimeWithMaybeOffset {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<UTCTimeWithMaybeOffset, ()> {
        match value {
            RecordTupleValue::UTCTimeWithMaybeOffset(t) => Ok(t),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for Timestamp {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Timestamp, ()> {
        match value {
            RecordTupleValue::Timestamp(dt) => Ok(dt),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for UTCTimestampWithMaybeOffset {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<UTCTimestampWithMaybeOffset, ()> {
        match value {
            RecordTupleValue::UTCTimestampWithMaybeOffset(dt) => Ok(dt),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for Option<Bytes> {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Option<Bytes>, ()> {
        match value {
            RecordTupleValue::MaybeBytes(maybe_b) => Ok(maybe_b),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for Option<String> {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Option<String>, ()> {
        match value {
            RecordTupleValue::MaybeString(maybe_s) => Ok(maybe_s),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for Option<RecordTuple> {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Option<RecordTuple>, ()> {
        match value {
            RecordTupleValue::MaybeRecordTuple(maybe_t) => Ok(maybe_t),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for Option<BigInt> {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Option<BigInt>, ()> {
        match value {
            RecordTupleValue::MaybeInteger(maybe_i) => Ok(maybe_i.as_ref().map(BigInt::from)),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for Option<i64> {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Option<i64>, ()> {
        match value {
            RecordTupleValue::MaybeInteger(maybe_i) => match maybe_i {
                Some(i) => i64::try_from(&i).map(Option::Some),
                None => Ok(None),
            },
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for Option<i32> {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Option<i32>, ()> {
        match value {
            RecordTupleValue::MaybeInteger(maybe_i) => match maybe_i {
                Some(i) => i32::try_from(&i).map(Option::Some),
                None => Ok(None),
            },
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for Option<i16> {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Option<i16>, ()> {
        match value {
            RecordTupleValue::MaybeInteger(maybe_i) => match maybe_i {
                Some(i) => i16::try_from(&i).map(Option::Some),
                None => Ok(None),
            },
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for Option<i8> {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Option<i8>, ()> {
        match value {
            RecordTupleValue::MaybeInteger(maybe_i) => match maybe_i {
                Some(i) => i8::try_from(&i).map(Option::Some),
                None => Ok(None),
            },
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for Option<f32> {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Option<f32>, ()> {
        match value {
            RecordTupleValue::MaybeFloat(maybe_f) => Ok(maybe_f),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for Option<f64> {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Option<f64>, ()> {
        match value {
            RecordTupleValue::MaybeDouble(maybe_d) => Ok(maybe_d),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for Option<bool> {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Option<bool>, ()> {
        match value {
            RecordTupleValue::MaybeBoolean(maybe_b) => Ok(maybe_b),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for Option<Uuid> {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Option<Uuid>, ()> {
        match value {
            RecordTupleValue::MaybeUuid(maybe_u) => Ok(maybe_u),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for Option<Versionstamp> {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Option<Versionstamp>, ()> {
        match value {
            RecordTupleValue::MaybeVersionstamp(maybe_vs) => Ok(maybe_vs),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for Option<Date> {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Option<Date>, ()> {
        match value {
            RecordTupleValue::MaybeDate(maybe_d) => Ok(maybe_d),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for Option<Time> {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Option<Time>, ()> {
        match value {
            RecordTupleValue::MaybeTime(maybe_t) => Ok(maybe_t),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for Option<UTCTimeWithMaybeOffset> {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Option<UTCTimeWithMaybeOffset>, ()> {
        match value {
            RecordTupleValue::MaybeUTCTimeWithMaybeOffset(maybe_t) => Ok(maybe_t),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for Option<Timestamp> {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Option<Timestamp>, ()> {
        match value {
            RecordTupleValue::MaybeTimestamp(maybe_dt) => Ok(maybe_dt),
            _ => Err(()),
        }
    }
}

impl TryFrom<RecordTupleValue> for Option<UTCTimestampWithMaybeOffset> {
    type Error = ();

    fn try_from(value: RecordTupleValue) -> Result<Option<UTCTimestampWithMaybeOffset>, ()> {
        match value {
            RecordTupleValue::MaybeUTCTimestampWithMaybeOffset(maybe_dt) => Ok(maybe_dt),
            _ => Err(()),
        }
    }
}

// TODO: Continue from here.

/// TODO
#[derive(Clone, PartialEq, Debug)]
pub(crate) struct RecordTuple {
    elements: VecDeque<RecordTupleValue>,
}

impl RecordTuple {
    /// TODO
    pub(crate) fn new() -> RecordTuple {
        RecordTuple {
            elements: VecDeque::new(),
        }
    }

    /// TODO
    pub(crate) fn get<'a, T>(&'a self, index: usize) -> Option<T>
    where
        T: RecordTupleElementGet<'a> + 'a,
    {
        RecordTupleElementGet::get(self, index)
    }

    /// TODO
    pub(crate) fn pop_back<T>(&mut self) -> Option<T>
    where
        T: RecordTupleElementPop,
    {
        RecordTupleElementPop::pop_back(self)
    }

    /// TODO
    pub(crate) fn pop_front<T>(&mut self) -> Option<T>
    where
        T: RecordTupleElementPop,
    {
        RecordTupleElementPop::pop_front(self)
    }

    /// TODO
    pub(crate) fn push_back<T>(&mut self, value: T)
    where
        T: RecordTupleElementPush,
    {
        RecordTupleElementPush::push_back(self, value)
    }

    /// TODO
    pub(crate) fn push_front<T>(&mut self, value: T)
    where
        T: RecordTupleElementPush,
    {
        RecordTupleElementPush::push_front(self, value)
    }

    /// TODO
    pub(crate) fn append(&mut self, other: &mut RecordTuple) {
        self.elements.append(&mut other.elements);
    }

    /// TODO
    pub(crate) fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// TODO
    pub(crate) fn len(&self) -> usize {
        self.elements.len()
    }
}
