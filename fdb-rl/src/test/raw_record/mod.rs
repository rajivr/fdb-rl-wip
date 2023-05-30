use bytes::Bytes;

use fdb::error::FdbResult;
use fdb::range::StreamingMode;
use fdb::subspace::Subspace;
use fdb::transaction::ReadTransaction;

use crate::raw_record::{RawRecordCursor, RawRecordCursorBuilder, RawRecordPrimaryKeySchema};
use crate::scan::ScanLimiter;

/// Expose [`RawRecordCursorBuilder::build`] method.
pub fn raw_record_cursor_builder_build<Tr>(
    primary_key_schema: Option<RawRecordPrimaryKeySchema>,
    subspace: Option<Subspace>,
    scan_limiter: Option<ScanLimiter>,
    streaming_mode: Option<StreamingMode>,
    limit: Option<usize>,
    reverse: Option<bool>,
    continuation: Option<Bytes>,
    read_transaction: &Tr,
) -> FdbResult<RawRecordCursor>
where
    Tr: ReadTransaction,
{
    let raw_record_cursor_builder = RawRecordCursorBuilder::from((
        primary_key_schema,
        subspace,
        scan_limiter,
        streaming_mode,
        limit,
        reverse,
        continuation,
    ));
    raw_record_cursor_builder.build(read_transaction)
}
