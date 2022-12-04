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
#![feature(atomic_mut_ptr)]
// TODO: Remove this later.
#![allow(rustdoc::broken_intra_doc_links)]
#![allow(unused_variables)]
#![allow(dead_code)]

//! FoundationDB Record Layer
//!
//! At this time, the thinking with Record Layer would be that it will
//! consist of a schema containing multiple `RecordType`s.
//!
//! A `RecordType` maps to a Avro Record and has a primary key tuple
//! and one or more secondary indexes.

pub mod cursor;
pub mod error;
pub mod range;
pub mod scan;

#[doc(hidden)]
pub mod test;
