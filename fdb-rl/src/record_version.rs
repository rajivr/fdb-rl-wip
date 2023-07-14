//! Provides [`RecordVersion`] type and associated items.
use bytes::Bytes;

use fdb::tuple::Versionstamp;

/// Type representing a specific version within FDB.
///
/// This is designed to inter-operate with the [`Versionstamp`] type
/// used to serialize versions within FoundationDB. The version
/// consists of three parts: a incarnation version, a global version,
/// a local version.
///
/// The incarnation version must be set by the user, and must be
/// incremented by the user each time the record is moved to a
/// different FoundationDB cluster.
///
/// The global version is a 10 byte version that is set by the
/// database, and is used to impose an ordering between different
/// transactions within a FoundationDB cluster.
///
/// The local version should be set by the user, and it must be used
/// to impose an order on different records within a single
/// transaction.
///
/// Together the incarnation version, global version and local version
/// are used to impose a total order across all records. Incarnation
/// version and local version can be managed using [`RecordContext`].
///
/// [`RecordContext`]: crate::RecordContext
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct RecordVersion {
    incarnation_version: Option<u64>,
    // *Note:*: The ordering of `global_version`, `local_version` and
    //          `complete` is based on `Versionstamp` type's
    //          `tr_version`, `user_version` and `complete`. Do not
    //          change this ordering.
    /// Corresponds to `tr_version` field in the [`Versionstamp`]
    /// struct. In RecordLayer `FDBRecordVersion` class, this field is
    /// known as `versionBytes`.
    global_version: Bytes,
    /// Corresponds to `user_version` field in the [`Versionstamp`]
    /// struct.
    local_version: u16,
    complete: bool,
}

impl RecordVersion {
    /// Get incarnation version. When no incarnation version is
    /// present, then `None` is returned.
    pub fn get_incarnation_version(&self) -> Option<u64> {
        self.incarnation_version
    }

    /// Get [`Versionstamp`].
    pub fn get_versionstamp(&self) -> Versionstamp {
        if self.complete {
            // Safety: In the `From::from` implementation, we take in
            //         a properly constructed versionstamp, which we
            //         then deconstruct to create a value of
            //         `RecordVersion` type. We can therefore safely
            //         put back a value of Versionstamp without
            //         panicing.
            Versionstamp::complete(self.global_version.clone(), self.local_version)
        } else {
            Versionstamp::incomplete(self.local_version)
        }
    }

    pub(crate) fn into_parts(self) -> (Option<u64>, Bytes, u16, bool) {
        let RecordVersion {
            incarnation_version,
            global_version,
            local_version,
            complete,
        } = self;

        (incarnation_version, global_version, local_version, complete)
    }
}

impl From<(u64, Versionstamp)> for RecordVersion {
    fn from((i, vs): (u64, Versionstamp)) -> RecordVersion {
        let incarnation_version = Some(i);

        let (global_version, local_version, complete) = vs.into_parts();

        RecordVersion {
            incarnation_version,
            global_version,
            local_version,
            complete,
        }
    }
}

impl From<Versionstamp> for RecordVersion {
    fn from(value: Versionstamp) -> RecordVersion {
        let (global_version, local_version, complete) = value.into_parts();

        RecordVersion {
            incarnation_version: None,
            global_version,
            local_version,
            complete,
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, Bytes, BytesMut};

    use fdb::tuple::Versionstamp;

    use super::RecordVersion;

    #[test]
    fn get_incarnation_version() {
        let rv = RecordVersion::from(Versionstamp::incomplete(10));
        assert_eq!(rv.get_incarnation_version(), None);

        let rv = RecordVersion::from((0, Versionstamp::incomplete(10)));
        assert_eq!(rv.get_incarnation_version(), Some(0));
    }

    #[test]
    fn get_versionstamp() {
        let tr_version = {
            let mut b = BytesMut::new();
            b.put_u64(1066);
            b.put_u16(1);
            Bytes::from(b)
        };

        let rv = RecordVersion::from(Versionstamp::incomplete(10));
        assert_eq!(rv.get_versionstamp(), Versionstamp::incomplete(10));

        let rv = RecordVersion::from(Versionstamp::complete(tr_version.clone(), 10));
        assert_eq!(
            rv.get_versionstamp(),
            Versionstamp::complete(tr_version, 10)
        );
    }

    #[test]
    fn comparisons() {
        // In 10 bytes transaction version below, `1066` and `1766`
        // are the 8 byte commit version and `1` and `2` are the
        // transaction batch order.
        let version_bytes_one = {
            let mut b = BytesMut::new();
            b.put_u64(1066);
            b.put_u16(1);
            Bytes::from(b)
        };

        let version_bytes_two = {
            let mut b = BytesMut::new();
            b.put_u64(1066);
            b.put_u16(2);
            Bytes::from(b)
        };

        let version_bytes_three = {
            let mut b = BytesMut::new();
            b.put_u64(1766);
            b.put_u16(1);
            Bytes::from(b)
        };

        let version_bytes_four = {
            let mut b = BytesMut::new();
            b.put_u64(1766);
            b.put_u16(2);
            Bytes::from(b)
        };

        // Each block corresponds to similar tests in Java
        // RecordLayer.
        {
            assert_eq!(
                RecordVersion::from(Versionstamp::incomplete(5)),
                RecordVersion::from(Versionstamp::incomplete(5))
            );
        }

        {
            assert_ne!(
                RecordVersion::from(Versionstamp::incomplete(5)),
                RecordVersion::from(Versionstamp::incomplete(6))
            );
            assert!(
                RecordVersion::from(Versionstamp::incomplete(5))
                    < RecordVersion::from(Versionstamp::incomplete(6))
            );
            assert!(
                RecordVersion::from(Versionstamp::incomplete(5))
                    > RecordVersion::from(Versionstamp::incomplete(0))
            );
        }

        {
            assert_eq!(
                RecordVersion::from(Versionstamp::complete(version_bytes_two.clone(), 10)),
                RecordVersion::from(Versionstamp::complete(version_bytes_two.clone(), 10)),
            );
        }

        {
            assert_ne!(
                RecordVersion::from(Versionstamp::complete(version_bytes_three.clone(), 10)),
                RecordVersion::from(Versionstamp::complete(version_bytes_two.clone(), 10)),
            );
            assert!(
                RecordVersion::from(Versionstamp::complete(version_bytes_three.clone(), 10))
                    > RecordVersion::from(Versionstamp::complete(version_bytes_two.clone(), 10))
            );
            assert!(
                RecordVersion::from(Versionstamp::complete(version_bytes_two.clone(), 10))
                    < RecordVersion::from(Versionstamp::complete(version_bytes_three.clone(), 10))
            );
        }

        {
            assert_ne!(
                RecordVersion::from(Versionstamp::complete(version_bytes_one.clone(), 10)),
                RecordVersion::from(Versionstamp::complete(version_bytes_two.clone(), 10)),
            );
            assert!(
                RecordVersion::from(Versionstamp::complete(version_bytes_one.clone(), 10))
                    < RecordVersion::from(Versionstamp::complete(version_bytes_two.clone(), 10))
            );
            assert!(
                RecordVersion::from(Versionstamp::complete(version_bytes_two.clone(), 10))
                    > RecordVersion::from(Versionstamp::complete(version_bytes_one.clone(), 10))
            );
        }

        {
            assert_ne!(
                RecordVersion::from(Versionstamp::complete(version_bytes_two.clone(), 15)),
                RecordVersion::from(Versionstamp::complete(version_bytes_two.clone(), 10)),
            );
            assert!(
                RecordVersion::from(Versionstamp::complete(version_bytes_two.clone(), 15))
                    > RecordVersion::from(Versionstamp::complete(version_bytes_two.clone(), 10))
            );
            assert!(
                RecordVersion::from(Versionstamp::complete(version_bytes_two.clone(), 10))
                    < RecordVersion::from(Versionstamp::complete(version_bytes_two.clone(), 15))
            );
        }

        {
            assert_ne!(
                RecordVersion::from(Versionstamp::incomplete(10)),
                RecordVersion::from(Versionstamp::complete(version_bytes_one.clone(), 10)),
            );
            assert!(
                RecordVersion::from(Versionstamp::incomplete(10))
                    > RecordVersion::from(Versionstamp::complete(version_bytes_one.clone(), 10)),
            );
            assert!(
                RecordVersion::from(Versionstamp::complete(version_bytes_one.clone(), 10))
                    < RecordVersion::from(Versionstamp::incomplete(10))
            );
        }

        // Incarnation version tests. Not present in Java RecordLayer.
        {
            assert_ne!(
                RecordVersion::from(Versionstamp::complete(version_bytes_four.clone(), 11)),
                RecordVersion::from((0, Versionstamp::complete(version_bytes_one.clone(), 10)))
            );
            assert!(
                RecordVersion::from(Versionstamp::complete(version_bytes_four.clone(), 11))
                    < RecordVersion::from((
                        0,
                        Versionstamp::complete(version_bytes_one.clone(), 10)
                    ))
            );
            assert!(
                RecordVersion::from((0, Versionstamp::complete(version_bytes_one.clone(), 10)))
                    > RecordVersion::from(Versionstamp::complete(version_bytes_four.clone(), 11))
            );
            assert!(
                RecordVersion::from((0, Versionstamp::complete(version_bytes_four.clone(), 11)))
                    < RecordVersion::from((
                        1,
                        Versionstamp::complete(version_bytes_one.clone(), 10)
                    ))
            );
            assert!(
                RecordVersion::from((1, Versionstamp::complete(version_bytes_one.clone(), 10)))
                    > RecordVersion::from((
                        0,
                        Versionstamp::complete(version_bytes_four.clone(), 11)
                    ))
            );
        }
    }
}
