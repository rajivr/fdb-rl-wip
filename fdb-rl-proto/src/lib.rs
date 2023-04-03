#![warn(missing_debug_implementations, rust_2018_idioms, unreachable_pub)]

pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("file_descriptor_set");

pub mod cursor {
    pub mod v1 {
        tonic::include_proto!("fdb_rl.cursor.v1");
    }
}
