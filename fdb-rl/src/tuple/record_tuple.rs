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
        let _ = i64::try_from(value.clone())
            .map(|x| record_tuple.push_back::<i64>(x))
            .map_err(|_| {
                record_tuple.elements.push_back(RecordTupleValue::Integer(
                    RecordTupleValueInteger::BigInt(value),
                ));
            });
    }

    fn push_front(record_tuple: &mut RecordTuple, value: BigInt) {
        let _ = i64::try_from(value.clone())
            .map(|x| record_tuple.push_front::<i64>(x))
            .map_err(|_| {
                record_tuple.elements.push_front(RecordTupleValue::Integer(
                    RecordTupleValueInteger::BigInt(value),
                ));
            });
    }
}

impl RecordTupleElementPush for i64 {
    fn push_back(record_tuple: &mut RecordTuple, value: i64) {
        let _ = i32::try_from(value)
            .map(|x| record_tuple.push_back::<i32>(x))
            .map_err(|_| {
                record_tuple.elements.push_back(RecordTupleValue::Integer(
                    RecordTupleValueInteger::I64(value),
                ));
            });
    }

    fn push_front(record_tuple: &mut RecordTuple, value: i64) {
        let _ = i32::try_from(value)
            .map(|x| record_tuple.push_front::<i32>(x))
            .map_err(|_| {
                record_tuple.elements.push_front(RecordTupleValue::Integer(
                    RecordTupleValueInteger::I64(value),
                ));
            });
    }
}

impl RecordTupleElementPush for i32 {
    fn push_back(record_tuple: &mut RecordTuple, value: i32) {
        let _ = i16::try_from(value)
            .map(|x| record_tuple.push_back::<i16>(x))
            .map_err(|_| {
                record_tuple.elements.push_back(RecordTupleValue::Integer(
                    RecordTupleValueInteger::I32(value),
                ));
            });
    }

    fn push_front(record_tuple: &mut RecordTuple, value: i32) {
        let _ = i16::try_from(value)
            .map(|x| record_tuple.push_back::<i16>(x))
            .map_err(|_| {
                record_tuple.elements.push_front(RecordTupleValue::Integer(
                    RecordTupleValueInteger::I32(value),
                ));
            });
    }
}

impl RecordTupleElementPush for i16 {
    fn push_back(record_tuple: &mut RecordTuple, value: i16) {
        let _ = i8::try_from(value)
            .map(|x| record_tuple.push_back::<i8>(x))
            .map_err(|_| {
                record_tuple.elements.push_back(RecordTupleValue::Integer(
                    RecordTupleValueInteger::I16(value),
                ));
            });
    }

    fn push_front(record_tuple: &mut RecordTuple, value: i16) {
        let _ = i8::try_from(value)
            .map(|x| record_tuple.push_back::<i8>(x))
            .map_err(|_| {
                record_tuple.elements.push_front(RecordTupleValue::Integer(
                    RecordTupleValueInteger::I16(value),
                ));
            });
    }
}

impl RecordTupleElementPush for i8 {
    fn push_back(record_tuple: &mut RecordTuple, value: i8) {
        record_tuple
            .elements
            .push_back(RecordTupleValue::Integer(RecordTupleValueInteger::I8(
                value,
            )));
    }

    fn push_front(record_tuple: &mut RecordTuple, value: i8) {
        record_tuple
            .elements
            .push_front(RecordTupleValue::Integer(RecordTupleValueInteger::I8(
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

// TODO: continue from here...

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
