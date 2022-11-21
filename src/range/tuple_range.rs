use fdb::subspace::Subspace;
use fdb::tuple::Tuple;
use fdb::Key;

use crate::range::{HighEndpoint, KeyRange, LowEndpoint};

/// Low endpoint of a [`TupleRange`].
pub type TupleLowEndpoint = LowEndpoint<Tuple>;

/// High endpoint of a [`TupleRange`].
pub type TupleHighEndpoint = HighEndpoint<Tuple>;

/// A range within an optional subspace specified by two [`Tuple`]
/// endpoints.
///
/// A [`TupleRange`] can be converted into a [`KeyRange`].
#[derive(Debug, Clone, PartialEq)]
pub struct TupleRange {
    low_endpoint: TupleLowEndpoint,
    high_endpoint: TupleHighEndpoint,
}

impl TupleRange {
    /// Create a [`TupleRange`]
    pub fn new(low_endpoint: TupleLowEndpoint, high_endpoint: TupleHighEndpoint) -> TupleRange {
        TupleRange {
            low_endpoint,
            high_endpoint,
        }
    }

    /// Create a [`TupleRange`] of all the tuples.
    pub fn all() -> TupleRange {
        let low_endpoint = LowEndpoint::Start;
        let high_endpoint = HighEndpoint::End;

        TupleRange::new(low_endpoint, high_endpoint)
    }

    /// Create a [`TupleRange`] over all keys beginning with a given
    /// [`Tuple`].
    ///
    /// This is a shortcut for creating a [`TupleRange`] with `prefix`
    /// as both the low-endpoint and high-endpoint and setting the
    /// type to range inclusive.
    pub fn all_of(prefix: Tuple) -> TupleRange {
        let low_endpoint = LowEndpoint::RangeInclusive(prefix.clone());
        let high_endpoint = HighEndpoint::RangeInclusive(prefix);

        TupleRange::new(low_endpoint, high_endpoint)
    }

    /// Create a [`TupleRange`] over all keys between the given [`Tuple`]s.
    ///
    /// `low` is the *inclusive* start of the range. `None` indicates
    /// the beginning.
    ///
    /// `high` is the *exclusive* end of the range. `None` indicates
    /// the end.
    pub fn between(low: Option<Tuple>, high: Option<Tuple>) -> TupleRange {
        let low_endpoint = match low {
            None => LowEndpoint::Start,
            Some(low_tuple) => LowEndpoint::RangeInclusive(low_tuple),
        };

        let high_endpoint = match high {
            Some(high_tuple) => HighEndpoint::RangeExclusive(high_tuple),
            None => HighEndpoint::End,
        };

        TupleRange::new(low_endpoint, high_endpoint)
    }

    /// Create a [`TupleRange`] over the same keys as this range but
    /// prepended by the supplied [`Tuple`].
    ///
    /// For example, if this range is over all [`Tuple`]'s from `("a",
    /// 3,)` exclusive to `("b", 4,)` inclusive, and one calls this
    /// method with `(0, null)` as the argument, this will create a
    /// range from `(0, null, "a", 3,)` exclusive to `(0, null, "b",
    /// 4,)` inclusive.
    pub fn prepend(self, mut beginning: Tuple) -> TupleRange {
        let TupleRange {
            low_endpoint,
            high_endpoint,
        } = self;

        let new_low_endpoint = match &low_endpoint {
            LowEndpoint::Start => LowEndpoint::RangeInclusive(beginning.clone()),
            LowEndpoint::RangeInclusive(_) | LowEndpoint::RangeExclusive(_) => {
                low_endpoint.map(|mut low_tuple| {
                    let mut b = beginning.clone();
                    b.append(&mut low_tuple);
                    b
                })
            }
        };

        let new_high_endpoint = match &high_endpoint {
            HighEndpoint::RangeInclusive(_) | HighEndpoint::RangeExclusive(_) => {
                high_endpoint.map(|mut high_tuple| {
                    beginning.append(&mut high_tuple);
                    beginning
                })
            }
            HighEndpoint::End => HighEndpoint::RangeInclusive(beginning),
        };

        TupleRange::new(new_low_endpoint, new_high_endpoint)
    }

    /// Convert to a [`KeyRange`].
    ///
    /// Takes an optional [`Subspace`] that the key range should be
    /// prefixed by.
    pub fn into_key_range(self, maybe_subspace_ref: &Option<Subspace>) -> KeyRange {
        let TupleRange {
            low_endpoint,
            high_endpoint,
        } = self;

        let (key_low_endpoint, key_high_endpoint) = if let Some(subspace_ref) =
            maybe_subspace_ref.as_ref()
        {
            let key_low_endpoint = match &low_endpoint {
                LowEndpoint::Start => LowEndpoint::RangeInclusive(Key::from(subspace_ref.pack())),
                LowEndpoint::RangeInclusive(_) | LowEndpoint::RangeExclusive(_) => low_endpoint
                    .map(|low_tuple| Key::from(subspace_ref.subspace(&low_tuple).pack())),
            };

            let key_high_endpoint = match &high_endpoint {
                HighEndpoint::RangeInclusive(_) | HighEndpoint::RangeExclusive(_) => high_endpoint
                    .map(|high_tuple| Key::from(subspace_ref.subspace(&high_tuple).pack())),

                HighEndpoint::End => HighEndpoint::RangeInclusive(Key::from(subspace_ref.pack())),
            };

            (key_low_endpoint, key_high_endpoint)
        } else {
            // Non-subspace case
            let key_low_endpoint = low_endpoint.map(|t| Key::from(t.pack()));
            let key_high_endpoint = high_endpoint.map(|t| Key::from(t.pack()));

            (key_low_endpoint, key_high_endpoint)
        };

        KeyRange::new(key_low_endpoint, key_high_endpoint)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use fdb::range::Range;
    use fdb::subspace::Subspace;
    use fdb::tuple::{key_util, Null, Tuple};

    use std::convert::TryFrom;

    use crate::range::{TupleHighEndpoint, TupleLowEndpoint};

    use super::TupleRange;

    // The `into_key_range()` test below tests `TupleRange::all`,
    // `TupleRange::all_of` and `TupleRange::between`. So, we do not
    // have tests for those seperately.

    #[test]
    fn prepend() {
        let mut tup_low_endpoint = {
            let t: (&'static str, i8) = ("a", 3);

            let mut tup = Tuple::new();
            tup.push_back::<String>((t.0).to_string());
            tup.push_back::<i8>(t.1);
            tup
        };

        let mut tup_high_endpoint = {
            let t: (&'static str, i8) = ("b", 4);

            let mut tup = Tuple::new();
            tup.push_back::<String>((t.0).to_string());
            tup.push_back::<i8>(t.1);
            tup
        };

        let prepend_tup = {
            let t: (i8, Null) = (0, Null);

            let mut tup = Tuple::new();
            tup.push_back::<i8>(t.0);
            tup.push_back::<Null>(t.1);
            tup
        };

        let tuple_range_1 = TupleRange::new(
            TupleLowEndpoint::RangeExclusive(tup_low_endpoint.clone()),
            TupleHighEndpoint::RangeInclusive(tup_high_endpoint.clone()),
        )
        .prepend(prepend_tup.clone());

        let tup_low_endpoint_prepended = {
            let mut tup = prepend_tup.clone();
            tup.append(&mut tup_low_endpoint);
            tup
        };

        let tup_high_endpoint_prepended = {
            let mut tup = prepend_tup;
            tup.append(&mut tup_high_endpoint);
            tup
        };

        let tuple_range_2 = TupleRange::new(
            TupleLowEndpoint::RangeExclusive(tup_low_endpoint_prepended),
            TupleHighEndpoint::RangeInclusive(tup_high_endpoint_prepended),
        );

        assert_eq!(tuple_range_1, tuple_range_2);
    }

    // *Note:* Unlike Java RecordLayer, there are no `illegalRanges`
    //         tests because we take care of that in the type
    //         system. Also in Java RecordLayer this test is called
    //         `TupleRangeTest::toRange`.
    #[test]
    fn into_key_range() {
        let prefix_tuple = {
            let prefix_tup: (&'static str,) = ("prefix",);

            let mut tup = Tuple::new();
            tup.push_back::<String>((prefix_tup.0).to_string());
            tup
        };

        let prefix_subspace = Subspace::new(Bytes::new()).subspace(&prefix_tuple);

        let prefix_bytes = prefix_subspace.pack();

        let a = {
            let a_tup: (&'static str,) = ("a",);

            let mut tup = Tuple::new();
            tup.push_back::<String>((a_tup.0).to_string());
            tup
        };

        let b = {
            let b_tup: (&'static str,) = ("b",);

            let mut tup = Tuple::new();
            tup.push_back::<String>((b_tup.0).to_string());
            tup
        };

        let test_cases = vec![
            (
                TupleRange::all(),
                Range::new(Bytes::new(), Bytes::from_static(b"\xFF")),
            ),
            (
                TupleRange::all_of(prefix_tuple.clone()),
                Range::starts_with(prefix_bytes.clone()),
            ),
            (
                TupleRange::new(
                    TupleLowEndpoint::RangeInclusive(a.clone()),
                    TupleHighEndpoint::RangeExclusive(b.clone()),
                ),
                Range::new(a.clone().pack(), b.clone().pack()),
            ),
            (
                TupleRange::new(
                    TupleLowEndpoint::RangeInclusive(a.clone()),
                    TupleHighEndpoint::RangeInclusive(b.clone()),
                ),
                Range::new(a.clone().pack(), {
                    // Returns the first key that does not have tuple
                    // `b` as prefix.
                    key_util::strinc(b.clone().pack()).unwrap()
                }),
            ),
            (
                TupleRange::new(
                    TupleLowEndpoint::RangeExclusive(a.clone()),
                    TupleHighEndpoint::RangeExclusive(b.clone()),
                ),
                Range::new(
                    {
                        // Returns the first key that does not have tuple
                        // `a` as prefix.
                        key_util::strinc(a.clone().pack()).unwrap()
                    },
                    b.clone().pack(),
                ),
            ),
            (
                TupleRange::between(None, Some(b.clone())),
                Range::new(Bytes::new(), b.clone().pack()),
            ),
            (
                TupleRange::between(Some(a.clone()), None),
                Range::new(a.clone().pack(), Bytes::from_static(b"\xFF")),
            ),
            // Unlike Java RecordLayer, `TupleRange` cannot be built
            // using a "continuation". We do not support that in the
            // type system. We also do not support `PREFIX_STRING`.
        ];

        for test_case in test_cases {
            let (tuple_range, range) = test_case;

            {
                // Safety: Safe to unwrap because we are constructing
                //         a proper tuple above.
                let tuple_range =
                    Range::try_from(tuple_range.clone().into_key_range(&None)).unwrap();
                let range = range.clone();

                assert_eq!(tuple_range, range);
            }

            // Prepend the prefix tuple
            let range_with_tuple_range_prepend = {
                // Safety: Safe to unwrap because we are constructing
                //         a proper tuple above.
                Range::try_from(
                    tuple_range
                        .clone()
                        .prepend(prefix_tuple.clone())
                        .into_key_range(&None),
                )
                .unwrap()
            };

            // Prepend the prefix subspace
            let range_with_tuple_range_subspace = {
                // Safety: Safe to unwrap because we are constructing
                //         a proper tuple above.
                Range::try_from(tuple_range.into_key_range(&Some(prefix_subspace.clone()))).unwrap()
            };

            assert_eq!(
                range_with_tuple_range_prepend,
                range_with_tuple_range_subspace
            );
        }
    }
}
