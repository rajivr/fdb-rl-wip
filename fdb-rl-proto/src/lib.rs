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

pub mod fdb_rl {
    pub mod cursor {
        pub mod v1 {
            include_proto!("fdb_rl.cursor.v1");
        }
    }

    pub mod field {
        pub mod v1 {
            include_proto!("fdb_rl.field.v1");
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
}

pub(crate) const TEST_FILE_DESCRIPTOR_SET: &[u8] =
    include_file_descriptor_set!("test_file_descriptor_set");

pub mod fdb_rl_test {
    pub mod key_expression {
        pub mod well_formed_message_descriptor {
            pub mod bad {
                pub mod v1 {
                    include_proto!(
                        "fdb_rl_test.key_expression.well_formed_message_descriptor.bad.v1"
                    );
                }
            }
            pub mod good {
                pub mod v1 {
                    include_proto!(
                        "fdb_rl_test.key_expression.well_formed_message_descriptor.good.v1"
                    );
                }
            }
        }
    }
}
