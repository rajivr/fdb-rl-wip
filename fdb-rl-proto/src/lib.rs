#![warn(missing_debug_implementations, rust_2018_idioms, unreachable_pub)]

// `tonic::include_proto`
macro_rules! include_proto {
    ($package: tt) => {
        include!(concat!(env!("OUT_DIR"), concat!("/", $package, ".rs")));
    };
}

// `tonic::include_file_descriptor_set`
macro_rules! include_file_descriptor_set {
    ($package: tt) => {
        include_bytes!(concat!(env!("OUT_DIR"), concat!("/", $package, ".bin")))
    };
}

pub(crate) const FILE_DESCRIPTOR_SET: &[u8] = include_file_descriptor_set!("file_descriptor_set");

pub mod cursor {
    pub mod v1 {
        include_proto!("fdb_rl.cursor.v1");
    }
}

pub mod key_expression {
    pub mod v1 {
        include_proto!("fdb_rl.key_expression.v1");
    }
}

pub mod record_metadata {
    pub mod v1 {
        include_proto!("fdb_rl.record_metadata.v1");
    }
}

pub mod tuple_fields {
    pub mod v1 {
        include_proto!("fdb_rl.tuple_fields.v1");
    }
}
