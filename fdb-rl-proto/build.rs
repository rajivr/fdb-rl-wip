use std::io;

fn main() -> io::Result<()> {
    let mut prost_build_config = prost_build::Config::new();

    // Use `Bytes` types
    prost_build_config.bytes(&["."]);

    let mut prost_reflect_build_builder = prost_reflect_build::Builder::new();

    prost_reflect_build_builder
        .file_descriptor_set_bytes("crate::FILE_DESCRIPTOR_SET")
        .compile_protos_with_config(
            prost_build_config,
            &["proto/fdb_rl/cursor/v1/cursor.proto"],
            &["proto/"],
        )?;

    Ok(())
}
