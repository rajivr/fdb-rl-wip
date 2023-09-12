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

    pub mod java {
        pub mod proto {
            pub mod expression_tests {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.expression_tests.v1");
                }
            }

            pub mod test_no_indexes {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_no_indexes.v1");
                }
            }

            pub mod test_no_record_types {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_no_record_types.v1");
                }
            }

            pub mod test_no_union {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_no_union.v1");
                }
            }

            pub mod test_no_union_evolved {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_no_union_evolved.v1");
                }
            }

            pub mod test_no_union_evolved_illegal {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_no_union_evolved_illegal.v1");
                }
            }

            pub mod test_no_union_evolved_renamed_type {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_no_union_evolved_renamed_type.v1");
                }
            }

            pub mod test_records_1 {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_1.v1");
                }
            }

            pub mod test_records_1_evolved {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_1_evolved.v1");
                }
            }

            pub mod test_records_1_evolved_again {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_1_evolved_again.v1");
                }
            }

            pub mod test_records_2 {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_2.v1");
                }
            }
        }
    }
}
