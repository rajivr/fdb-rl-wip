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

pub mod error;

pub mod fdb_rl {
    pub mod cursor {
        pub mod v1 {
            include_proto!("fdb_rl.cursor.v1");
        }
    }

    pub mod field {
        pub mod v1 {
            use std::convert::TryFrom;

            include_proto!("fdb_rl.field.v1");

            impl From<uuid::Uuid> for crate::fdb_rl::field::v1::Uuid {
                fn from(uuid: uuid::Uuid) -> crate::fdb_rl::field::v1::Uuid {
                    crate::fdb_rl::field::v1::Uuid {
                        value: bytes::Bytes::from(uuid.as_bytes().to_vec()),
                    }
                }
            }

            impl TryFrom<crate::fdb_rl::field::v1::Uuid> for uuid::Uuid {
                type Error = fdb::error::FdbError;

                fn try_from(
                    uuid: crate::fdb_rl::field::v1::Uuid,
                ) -> fdb::error::FdbResult<uuid::Uuid> {
                    uuid::Uuid::from_slice(uuid.value.as_ref())
                        .map_err(|_| fdb::error::FdbError::new(crate::error::FIELD_V1_INVALID_UUID))
                }
            }

            #[cfg(test)]
            mod tests {
                mod uuid {
                    use bytes::Bytes;
                    use fdb::error::FdbError;
                    use uuid::Uuid;

                    use std::convert::TryFrom;

                    use crate::error::FIELD_V1_INVALID_UUID;

                    use super::super::Uuid as FdbRLWktUuidProto;

                    #[test]
                    fn from_uuid_from() {
                        let uuid = Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap();

                        let wkt_uuid_proto = FdbRLWktUuidProto::from(uuid.clone());

                        assert_eq!(uuid, Uuid::try_from(wkt_uuid_proto).unwrap());
                    }

                    #[test]
                    fn try_from_uuid_try_from() {
                        // Valid case is checked in `From` trait unit
                        // test. We check invalid case below.
                        let wkt_uuid_proto = FdbRLWktUuidProto {
                            value: Bytes::from([4, 54, 67, 12, 43, 2, 98, 76].as_ref()),
                        };

                        assert_eq!(
                            Uuid::try_from(wkt_uuid_proto),
                            Err(FdbError::new(FIELD_V1_INVALID_UUID))
                        );
                    }
                }
            }
        }
    }

    pub mod record_metadata {
        pub mod v1 {
            include_proto!("fdb_rl.record_metadata.v1");
        }
    }

    pub mod tuple_schema {
        pub mod v1 {
            include_proto!("fdb_rl.tuple_schema.v1");
        }
    }
}

pub(crate) const TEST_FILE_DESCRIPTOR_SET: &[u8] =
    include_file_descriptor_set!("test_file_descriptor_set");

pub mod fdb_rl_test {
    pub mod protobuf {
        pub mod well_formed_dynamic_message {
            pub mod v1 {
                include_proto!("fdb_rl_test.protobuf.well_formed_dynamic_message.v1");
            }
        }

        pub mod well_formed_message_descriptor {
            pub mod bad {
                pub mod proto_2 {
                    pub mod v1 {
                        include_proto!(
                            "fdb_rl_test.protobuf.well_formed_message_descriptor.bad.proto_2.v1"
                        );
                    }
                }

                pub mod proto_3 {
                    pub mod v1 {
                        include_proto!(
                            "fdb_rl_test.protobuf.well_formed_message_descriptor.bad.proto_3.v1"
                        );
                    }
                }
            }

            pub mod evolution {
                pub mod v1 {
                    include_proto!(
                        "fdb_rl_test.protobuf.well_formed_message_descriptor.evolution.v1"
                    );
                }
                pub mod v2 {
                    include_proto!(
                        "fdb_rl_test.protobuf.well_formed_message_descriptor.evolution.v2"
                    );
                }
                pub mod v3 {
                    include_proto!(
                        "fdb_rl_test.protobuf.well_formed_message_descriptor.evolution.v3"
                    );
                }
                pub mod v4 {
                    include_proto!(
                        "fdb_rl_test.protobuf.well_formed_message_descriptor.evolution.v4"
                    );
                }
                pub mod v5 {
                    include_proto!(
                        "fdb_rl_test.protobuf.well_formed_message_descriptor.evolution.v5"
                    );
                }
            }

            pub mod good {
                pub mod v1 {
                    include_proto!("fdb_rl_test.protobuf.well_formed_message_descriptor.good.v1");
                }
                pub mod v2 {
                    include_proto!("fdb_rl_test.protobuf.well_formed_message_descriptor.good.v2");
                }
                pub mod v3 {
                    include_proto!("fdb_rl_test.protobuf.well_formed_message_descriptor.good.v3");
                }
            }
        }
    }

    pub mod java {
        pub mod proto {
            pub mod evolution {
                pub mod test_field_type_change {
                    pub mod v1 {
                        include_proto!(
                            "fdb_rl_test.java.proto.evolution.test_field_type_change.v1"
                        );
                    }
                }

                pub mod test_merged_nested_types {
                    pub mod v1 {
                        include_proto!(
                            "fdb_rl_test.java.proto.evolution.test_merged_nested_types.v1"
                        );
                    }
                    pub mod v2 {
                        include_proto!(
                            "fdb_rl_test.java.proto.evolution.test_merged_nested_types.v2"
                        );
                    }
                }

                pub mod test_new_record_type {
                    pub mod v1 {
                        include_proto!("fdb_rl_test.java.proto.evolution.test_new_record_type.v1");
                    }
                }

                pub mod test_self_reference {
                    pub mod v1 {
                        include_proto!("fdb_rl_test.java.proto.evolution.test_self_reference.v1");
                    }
                }

                pub mod test_self_reference_unspooled {
                    pub mod v1 {
                        include_proto!(
                            "fdb_rl_test.java.proto.evolution.test_self_reference_unspooled.v1"
                        );
                    }
                    pub mod v2 {
                        include_proto!(
                            "fdb_rl_test.java.proto.evolution.test_self_reference_unspooled.v2"
                        );
                    }
                }

                pub mod test_split_nested_types {
                    pub mod v1 {
                        include_proto!(
                            "fdb_rl_test.java.proto.evolution.test_split_nested_types.v1"
                        );
                    }
                    pub mod v2 {
                        include_proto!(
                            "fdb_rl_test.java.proto.evolution.test_split_nested_types.v2"
                        );
                    }
                }

                pub mod test_swap_union_fields {
                    pub mod v1 {
                        include_proto!(
                            "fdb_rl_test.java.proto.evolution.test_swap_union_fields.v1"
                        );
                    }
                }

                pub mod test_unmerged_nested_types {
                    pub mod v1 {
                        include_proto!(
                            "fdb_rl_test.java.proto.evolution.test_unmerged_nested_types.v1"
                        );
                    }
                }
            }

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
                pub mod v2 {
                    include_proto!("fdb_rl_test.java.proto.test_records_1.v2");
                }
                pub mod v3 {
                    include_proto!("fdb_rl_test.java.proto.test_records_1.v3");
                }
                pub mod v4 {
                    include_proto!("fdb_rl_test.java.proto.test_records_1.v4");
                }
                pub mod v5 {
                    include_proto!("fdb_rl_test.java.proto.test_records_1.v5");
                }
                pub mod v6 {
                    include_proto!("fdb_rl_test.java.proto.test_records_1.v6");
                }
                pub mod v7 {
                    include_proto!("fdb_rl_test.java.proto.test_records_1.v7");
                }
                pub mod v8 {
                    include_proto!("fdb_rl_test.java.proto.test_records_1.v8");
                }
                pub mod v9 {
                    include_proto!("fdb_rl_test.java.proto.test_records_1.v9");
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

            pub mod test_records_3 {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_3.v1");
                }
            }

            pub mod test_records_4 {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_4.v1");
                }
            }

            pub mod test_records_4_wrapper {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_4_wrapper.v1");
                }
            }

            pub mod test_records_5 {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_5.v1");
                }
            }

            pub mod test_records_6 {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_6.v1");
                }
            }

            pub mod test_records_7 {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_7.v1");
                }
            }

            pub mod test_records_8 {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_8.v1");
                }
            }

            pub mod test_records_bad_union_1 {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_bad_union_1.v1");
                }
            }

            pub mod test_records_bad_union_2 {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_bad_union_2.v1");
                }
            }

            pub mod test_records_bitmap {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_bitmap.v1");
                }
            }

            pub mod test_records_bytes {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_bytes.v1");
                }
            }

            pub mod test_records_chained_1 {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_chained_1.v1");
                }
            }

            pub mod test_records_chained_2 {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_chained_2.v1");
                }
            }

            pub mod test_records_datatypes {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_datatypes.v1");
                }
            }

            pub mod test_records_double_nested {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_double_nested.v1");
                }
            }

            pub mod test_records_duplicate_union_fields {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_duplicate_union_fields.v1");
                }
            }

            pub mod test_records_duplicate_union_fields_reordered {
                pub mod v1 {
                    include_proto!(
                        "fdb_rl_test.java.proto.test_records_duplicate_union_fields_reordered.v1"
                    );
                }
            }

            pub mod test_records_enum {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_enum.v1");
                }
                pub mod v2 {
                    include_proto!("fdb_rl_test.java.proto.test_records_enum.v2");
                }
                pub mod v3 {
                    include_proto!("fdb_rl_test.java.proto.test_records_enum.v3");
                }
            }

            pub mod test_records_implicit_usage {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_implicit_usage.v1");
                }
            }

            pub mod test_records_implicit_usage_no_union {
                pub mod v1 {
                    include_proto!(
                        "fdb_rl_test.java.proto.test_records_implicit_usage_no_union.v1"
                    );
                }
            }

            pub mod test_records_import {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_import.v1");
                }
            }

            pub mod test_records_import_flat {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_import_flat.v1");
                }
            }

            pub mod test_records_imported_and_new {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_imported_and_new.v1");
                }
            }

            pub mod test_records_index_compat {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_index_compat.v1");
                }
            }

            pub mod test_records_index_filtering {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_index_filtering.v1");
                }
            }

            pub mod test_records_join_index {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_join_index.v1");
                }
            }

            pub mod test_records_leaderboard {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_leaderboard.v1");
                }
            }

            pub mod test_records_map {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_map.v1");
                }
            }

            pub mod test_records_marked_unmarked {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_marked_unmarked.v1");
                }
            }

            pub mod test_records_multi {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_multi.v1");
                }
            }

            pub mod test_records_multidimensional {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_multidimensional.v1");
                }
            }

            pub mod test_records_name_clash {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_name_clash.v1");
                }
            }

            pub mod test_records_nested_as_record {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_nested_as_record.v1");
                }
            }

            pub mod test_records_no_primary_key {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_no_primary_key.v1");
                }
            }

            pub mod test_records_nulls_2 {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_nulls_2.v1");
                }
            }

            pub mod test_records_oneof {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_oneof.v1");
                }
            }

            pub mod test_records_parent_child {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_parent_child.v1");
                }
            }

            pub mod test_records_rank {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_rank.v1");
                }
            }

            pub mod test_records_text {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_text.v1");
                }
            }

            pub mod test_records_transform {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_transform.v1");
                }
            }

            pub mod test_records_tuple_fields {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_tuple_fields.v1");
                }
            }

            pub mod test_records_two_unions {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_two_unions.v1");
                }
            }

            pub mod test_records_union_default_name {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_union_default_name.v1");
                }
            }

            pub mod test_records_union_missing_record {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_union_missing_record.v1");
                }
            }

            pub mod test_records_union_with_imported_nested {
                pub mod v1 {
                    include_proto!(
                        "fdb_rl_test.java.proto.test_records_union_with_imported_nested.v1"
                    );
                }
            }

            pub mod test_records_union_with_nested {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_union_with_nested.v1");
                }
            }

            pub mod test_records_unsigned_1 {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_unsigned_1.v1");
                }
            }

            pub mod test_records_unsigned_2 {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_unsigned_2.v1");
                }
            }

            pub mod test_records_unsigned_3 {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_unsigned_3.v1");
                }
            }

            pub mod test_records_unsigned_4 {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_unsigned_4.v1");
                }
            }

            pub mod test_records_unsigned_5 {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_unsigned_5.v1");
                }
            }

            pub mod test_records_with_header {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_with_header.v1");
                }
                pub mod v2 {
                    include_proto!("fdb_rl_test.java.proto.test_records_with_header.v2");
                }
                pub mod v3 {
                    include_proto!("fdb_rl_test.java.proto.test_records_with_header.v3");
                }
                pub mod v4 {
                    include_proto!("fdb_rl_test.java.proto.test_records_with_header.v4");
                }
            }

            pub mod test_records_with_union {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto.test_records_with_union.v1");
                }
            }
        }

        pub mod proto2 {
            pub mod test_records_maps {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto2.test_records_maps.v1");
                }
            }
        }

        pub mod proto3 {
            pub mod evolution {
                pub mod test_nested_proto2 {
                    pub mod v1 {
                        include_proto!("fdb_rl_test.java.proto3.evolution.test_nested_proto2.v1");
                    }
                }

                pub mod test_nested_proto3 {
                    pub mod v1 {
                        include_proto!("fdb_rl_test.java.proto3.evolution.test_nested_proto3.v1");
                    }
                }

                pub mod test_records_1_imported {
                    pub mod v1 {
                        include_proto!(
                            "fdb_rl_test.java.proto3.evolution.test_records_1_imported.v1"
                        );
                    }
                }

                pub mod test_records_3_proto3 {
                    pub mod v1 {
                        include_proto!(
                            "fdb_rl_test.java.proto3.evolution.test_records_3_proto3.v1"
                        );
                    }
                }

                pub mod test_records_enum_proto3 {
                    pub mod v1 {
                        include_proto!(
                            "fdb_rl_test.java.proto3.evolution.test_records_enum_proto3.v1"
                        );
                    }
                }

                pub mod test_records_nested_proto2 {
                    pub mod v1 {
                        include_proto!(
                            "fdb_rl_test.java.proto3.evolution.test_records_nested_proto2.v1"
                        );
                    }
                }

                pub mod test_records_nested_proto3 {
                    pub mod v1 {
                        include_proto!(
                            "fdb_rl_test.java.proto3.evolution.test_records_nested_proto3.v1"
                        );
                    }
                }
            }

            pub mod test_records_maps {
                pub mod v1 {
                    include_proto!("fdb_rl_test.java.proto3.test_records_maps.v1");
                }
            }
        }
    }
}
