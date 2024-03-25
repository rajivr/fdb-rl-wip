//! TODO

pub(crate) mod error;
pub(crate) mod fdb_tuple_schema;
pub(crate) mod well_formed_dynamic_message;
pub(crate) mod well_formed_message_descriptor;

pub(crate) use well_formed_dynamic_message::WellFormedDynamicMessage;
pub(crate) use well_formed_message_descriptor::{WellFormedMessageDescriptor, FDB_RL_WKT_V1_UUID};
