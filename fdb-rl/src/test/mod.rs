#![allow(rustdoc::private_intra_doc_links)]

//! Types and Functions meant **only** for integration testing.
//!
//! This module exposes internal types and functions that we would
//! like to test against a live database. These types and functions
//! have *absolutely no* API stability guarantee and you should
//! *never* rely upon them.
//!
//! The module structure in this module is arranged to correspond with
//! the crate module structure.

pub mod cursor;
pub mod raw_record;
