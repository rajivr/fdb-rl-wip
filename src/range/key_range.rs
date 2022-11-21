use fdb::error::{FdbError, FdbResult};
use fdb::range::Range;
use fdb::Key;

use std::convert::TryFrom;

use crate::range::{bytes_endpoint, HighEndpoint, LowEndpoint};

/// Low endpoint of a [`Key`].
pub type KeyLowEndpoint = LowEndpoint<Key>;

/// High endpoint of a [`Key`].
pub type KeyHighEndpoint = HighEndpoint<Key>;

/// A range specified by two [`Key`] endpoints.
///
/// A [`KeyRange`] does not have a [`Subspace`] and can be converted
/// into a FDB [`Range`].
///
/// [`Subspace`]: fdb::subspace::Subspace
#[derive(Debug, Clone, PartialEq)]
pub struct KeyRange {
    low_endpoint: KeyLowEndpoint,
    high_endpoint: KeyHighEndpoint,
}

impl KeyRange {
    /// Create a [`KeyRange`]
    pub fn new(low_endpoint: KeyLowEndpoint, high_endpoint: KeyHighEndpoint) -> KeyRange {
        KeyRange {
            low_endpoint,
            high_endpoint,
        }
    }

    /// Create a [`KeyRange`] from [`Key`]s.
    ///
    /// In the constructed [`KeyRange`], the `low_key` would be
    /// inclusive and `high_key` would be exclusive.
    pub fn from_keys(low_key: Key, high_key: Key) -> KeyRange {
        let low_endpoint = LowEndpoint::RangeInclusive(low_key);
        let high_endpoint = HighEndpoint::RangeExclusive(high_key);

        KeyRange::new(low_endpoint, high_endpoint)
    }

    pub(crate) fn into_parts(self) -> (KeyLowEndpoint, KeyHighEndpoint) {
        let KeyRange {
            low_endpoint,
            high_endpoint,
        } = self;

        (low_endpoint, high_endpoint)
    }
}

impl TryFrom<KeyRange> for Range {
    type Error = FdbError;

    fn try_from(value: KeyRange) -> FdbResult<Range> {
        bytes_endpoint::build_range_continuation(
            &None, // maybe_subspace_ref
            value, // key_range
            None,  // maybe_continuation_key
            false, // reverse
        )
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use fdb::Key;

    use crate::range::{HighEndpoint, LowEndpoint};

    use super::KeyRange;

    #[test]
    fn from_keys() {
        let low_key = Key::from(Bytes::from_static(b"abcd"));
        let high_key = Key::from(Bytes::from_static(b"efgh"));

        let (low_key_endpoint, high_key_endpoint) =
            KeyRange::from_keys(low_key, high_key).into_parts();

        assert!(
            matches!(low_key_endpoint, LowEndpoint::RangeInclusive(key) if key == Key::from(Bytes::from_static(b"abcd")))
        );
        assert!(
            matches!(high_key_endpoint, HighEndpoint::RangeExclusive(key) if key == Key::from(Bytes::from_static(b"efgh")))
        );
    }
}
