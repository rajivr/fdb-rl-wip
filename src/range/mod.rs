//! Provides types for Record Layer range.
//!
//! Record Layer range module provides a higher level of abstraction
//! for working FDB [range] and [cursors].
//!
//! Please consider using [`TupleRange`] and only if the APIs are not
//! suitable, then use [`KeyRange`].
//!
//! [range]: fdb::range
//! [cursors]: crate::cursor

mod endpoint;
mod key_range;
mod tuple_range;

pub(crate) mod bytes_endpoint;

pub use endpoint::{HighEndpoint, LowEndpoint};
pub use key_range::{KeyHighEndpoint, KeyLowEndpoint, KeyRange};
pub use tuple_range::{TupleHighEndpoint, TupleLowEndpoint, TupleRange};
