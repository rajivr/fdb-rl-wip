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
                ],
                &[
		    "proto",
		    "proto-test/",
		],
            )?;
    }
    Ok(())
}
