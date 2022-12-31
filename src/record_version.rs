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
#[derive(Debug)]
pub struct RecordVersion {
    incarnation_version: Option<u64>,
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
    /// TODO: Documentation + tests
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
    /// TODO: Documentation + Tests
    fn from(value: (u64, Versionstamp)) -> RecordVersion {
        let (i, vs) = value;

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
    /// TODO: Documentation + Tests
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
    // TODO
}
