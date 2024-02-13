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

    use crate::tuple::RecordTuple;

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
    impl SealedGet for &(Time, Option<UtcOffset>) {}
    impl SealedGet for &(Date, Time) {}
    impl SealedGet for &(Date, Time, Option<UtcOffset>) {}

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
    impl SealedGet for &Option<(Time, Option<UtcOffset>)> {}
    impl SealedGet for &Option<(Date, Time)> {}
    impl SealedGet for &Option<(Date, Time, Option<UtcOffset>)> {}

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
    impl SealedGet for &Vec<(Time, Option<UtcOffset>)> {}
    impl SealedGet for &Vec<(Date, Time)> {}
    impl SealedGet for &Vec<(Date, Time, Option<UtcOffset>)> {}

    pub(crate) trait SealedPush {}

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

impl<'a> RecordTupleElementGet<'a> for &'a (Time, Option<UtcOffset>) {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a (Time, Option<UtcOffset>)> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::UTCTimeWithMaybeOffset(ref t) => Some(t),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a (Date, Time) {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a (Date, Time)> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::Timestamp(ref dt) => Some(dt),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a (Date, Time, Option<UtcOffset>) {
    fn get(
        record_tuple: &'a RecordTuple,
        index: usize,
    ) -> Option<&'a (Date, Time, Option<UtcOffset>)> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::UTCTimestampWithMaybeOffset(ref dt) => Some(dt),
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

impl<'a> RecordTupleElementGet<'a> for &'a Option<(Time, Option<UtcOffset>)> {
    fn get(
        record_tuple: &'a RecordTuple,
        index: usize,
    ) -> Option<&'a Option<(Time, Option<UtcOffset>)>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::MaybeUTCTimeWithMaybeOffset(ref maybe_t) => Some(maybe_t),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Option<(Date, Time)> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Option<(Date, Time)>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::MaybeTimestamp(ref maybe_dt) => Some(maybe_dt),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Option<(Date, Time, Option<UtcOffset>)> {
    fn get(
        record_tuple: &'a RecordTuple,
        index: usize,
    ) -> Option<&'a Option<(Date, Time, Option<UtcOffset>)>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::MaybeUTCTimestampWithMaybeOffset(ref maybe_dt) => Some(maybe_dt),
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

impl<'a> RecordTupleElementGet<'a> for &'a Vec<(Time, Option<UtcOffset>)> {
    fn get(
        record_tuple: &'a RecordTuple,
        index: usize,
    ) -> Option<&'a Vec<(Time, Option<UtcOffset>)>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::ListOfUTCTimeWithMaybeOffset(ref vec_t) => Some(vec_t),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Vec<(Date, Time)> {
    fn get(record_tuple: &'a RecordTuple, index: usize) -> Option<&'a Vec<(Date, Time)>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::ListOfTimestamp(ref vec_dt) => Some(vec_dt),
            _ => None,
        })
    }
}

impl<'a> RecordTupleElementGet<'a> for &'a Vec<(Date, Time, Option<UtcOffset>)> {
    fn get(
        record_tuple: &'a RecordTuple,
        index: usize,
    ) -> Option<&'a Vec<(Date, Time, Option<UtcOffset>)>> {
        record_tuple.elements.get(index).and_then(|x| match *x {
            RecordTupleValue::ListOfUTCTimestampWithMaybeOffset(ref vec_dt) => Some(vec_dt),
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
    UTCTimeWithMaybeOffset((Time, Option<UtcOffset>)),
    Timestamp((Date, Time)),
    UTCTimestampWithMaybeOffset((Date, Time, Option<UtcOffset>)),
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
    MaybeUTCTimeWithMaybeOffset(Option<(Time, Option<UtcOffset>)>),
    MaybeTimestamp(Option<(Date, Time)>),
    MaybeUTCTimestampWithMaybeOffset(Option<(Date, Time, Option<UtcOffset>)>),
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
    ListOfUTCTimeWithMaybeOffset(Vec<(Time, Option<UtcOffset>)>),
    ListOfTimestamp(Vec<(Date, Time)>),
    ListOfUTCTimestampWithMaybeOffset(Vec<(Date, Time, Option<UtcOffset>)>),
}

/// TODO
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
