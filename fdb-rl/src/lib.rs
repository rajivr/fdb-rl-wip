#![allow(incomplete_features)]
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![feature(type_alias_impl_trait)]
#![feature(async_fn_in_trait)]
#![feature(return_position_impl_trait_in_trait)]
#![feature(once_cell)]
// TODO: Remove this later.
#![allow(rustdoc::broken_intra_doc_links)]
#![allow(unused_variables)]
#![allow(dead_code)]

//! FoundationDB Record Layer
//!
//! **This crate is work-in-progress.**
//!
//! At this time, the thinking with Record Layer is that it will
//! consist of a schema (`RecordSchema`) containing multiple
//! `RecordEntity`s.
//!
//! Each `RecordEntity` will map to seralized byte array such as an
//! Avro Record and will have a primary key tuple and optionally one
//! or more secondary indexes.
//!
//! This crate will provide:
//! - APIs for Schema and Index management.
//!
//! - Ensure transactionally consistent secondary indexes during
//!   inserts, updates and deletes.
//!
//! - A very simple API for querying based on primary key and
//!   secondary indexes.
//!
//! In addition, a simple transactionally consistent queuing system
//! similar to QuiCK will also be implemented.
//!
//! *There are no plans to implement a query execution engine or other
//! higher level database features.*
mod raw_record;
mod record_context;
mod record_version;

pub mod cursor;
pub mod error;
pub mod range;
pub mod scan;
pub mod split_helper;

#[doc(hidden)]
pub mod test;

pub(crate) use crate::raw_record::RawRecord;

pub use crate::record_context::RecordContext;
pub use crate::record_version::RecordVersion;
