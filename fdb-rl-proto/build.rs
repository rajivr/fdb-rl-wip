use std::env;
use std::io;
use std::path::PathBuf;

fn main() -> io::Result<()> {
    // It looks like we cannot use `#[cfg(test)]` to selectively
    // exclude the compilation of protos used for testing. Therefore
    // we compile all protos and organize them in different packages -
    // `fdb_rl` and `fdb_rl_test`.
    {
        let mut prost_build_config = prost_build::Config::new();

        // Use `Bytes` types
        prost_build_config.bytes(&["."]);

        let mut prost_reflect_build_builder = prost_reflect_build::Builder::new();

        prost_reflect_build_builder
            .file_descriptor_set_bytes("crate::FILE_DESCRIPTOR_SET")
            .compile_protos_with_config(
                prost_build_config,
                &[
                    "proto/fdb_rl/cursor/v1/cursor.proto",
                    "proto/fdb_rl/field/v1/field.proto",
                    "proto/fdb_rl/key_expression/v1/key_expression.proto",
                    "proto/fdb_rl/record_metadata/v1/record_type.proto",
                ],
                &["proto/"],
            )?;
    }

    {
        let mut prost_build_config = prost_build::Config::new();

        // Use `Bytes` types
        prost_build_config.bytes(&["."]);

        let mut prost_reflect_build_builder = prost_reflect_build::Builder::new();

        prost_reflect_build_builder
            .file_descriptor_set_path(
                PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR environment variable not set"))
                    .join("test_file_descriptor_set.bin"),
            )
            .file_descriptor_set_bytes("crate::TEST_FILE_DESCRIPTOR_SET")
            .compile_protos_with_config(
                prost_build_config,
                &[
                    "proto-test/fdb_rl_test/key_expression/well_formed_message_descriptor/bad/v1/version_2.proto",
		    "proto-test/fdb_rl_test/key_expression/well_formed_message_descriptor/good/v1/version_3.proto",
		    // Java RecordLayer Protos
		    "proto-test/fdb_rl_test/java/proto/expression_tests/v1/expression_tests.proto",
		    "proto-test/fdb_rl_test/java/proto/test_no_indexes/v1/test_no_indexes.proto",
		    "proto-test/fdb_rl_test/java/proto/test_no_record_types/v1/test_no_record_types.proto",
		    "proto-test/fdb_rl_test/java/proto/test_no_union/v1/test_no_union.proto",
		    "proto-test/fdb_rl_test/java/proto/test_no_union_evolved/v1/test_no_union_evolved.proto",
		    "proto-test/fdb_rl_test/java/proto/test_no_union_evolved_illegal/v1/test_no_union_evolved_illegal.proto",
		    "proto-test/fdb_rl_test/java/proto/test_no_union_evolved_renamed_type/v1/test_no_union_evolved_renamed_type.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_1/v1/test_records_1.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_1_evolved/v1/test_records_1_evolved.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_1_evolved_again/v1/test_records_1_evolved_again.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_2/v1/test_records_2.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_3/v1/test_records_3.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_4/v1/test_records_4.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_4_wrapper/v1/test_records_4_wrapper.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_5/v1/test_records_5.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_6/v1/test_records_6.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_7/v1/test_records_7.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_8/v1/test_records_8.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_bad_union_1/v1/test_records_bad_union_1.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_bad_union_2/v1/test_records_bad_union_2.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_bitmap/v1/test_records_bitmap.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_bytes/v1/test_records_bytes.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_chained_1/v1/test_records_chained_1.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_chained_2/v1/test_records_chained_2.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_datatypes/v1/test_records_datatypes.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_double_nested/v1/test_records_double_nested.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_duplicate_union_fields/v1/test_records_duplicate_union_fields.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_duplicate_union_fields_reordered/v1/test_records_duplicate_union_fields_reordered.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_enum/v1/test_records_enum.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_implicit_usage/v1/test_records_implicit_usage.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_implicit_usage_no_union/v1/test_records_implicit_usage_no_union.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_import/v1/test_records_import.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_import_flat/v1/test_records_import_flat.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_imported_and_new/v1/test_records_imported_and_new.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_index_compat/v1/test_records_index_compat.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_index_filtering/v1/test_records_index_filtering.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_join_index/v1/test_records_join_index.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_leaderboard/v1/test_records_leaderboard.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_map/v1/test_records_map.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_marked_unmarked/v1/test_records_marked_unmarked.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_multi/v1/test_records_multi.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_multidimensional/v1/test_records_multidimensional.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_name_clash/v1/test_records_name_clash.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_nested_as_record/v1/test_records_nested_as_record.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_no_primary_key/v1/test_records_no_primary_key.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_nulls_2/v1/test_records_nulls_2.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_oneof/v1/test_records_oneof.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_parent_child/v1/test_records_parent_child.proto",
		    "proto-test/fdb_rl_test/java/proto/test_records_rank/v1/test_records_rank.proto",
                ],
                &[
		    "proto",
		    "proto-test/",
		],
            )?;
    }
    Ok(())
}
