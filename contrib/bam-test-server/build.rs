fn main() -> Result<(), std::io::Error> {
    const PROTOC_ENVAR: &str = "PROTOC";
    if std::env::var(PROTOC_ENVAR).is_err() {
        #[cfg(not(windows))]
        std::env::set_var(PROTOC_ENVAR, protobuf_src::protoc());
    }

    let proto_base_path = std::path::PathBuf::from(
        std::env::var("CARGO_MANIFEST_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::path::PathBuf::from(".")),
    )
    .join("../../src/disco/bam/proto/bam-protos");
    let proto_files = ["bam_types.proto", "bam_api.proto"];
    let mut protos = Vec::new();
    for proto_file in &proto_files {
        let proto = proto_base_path.join(proto_file);
        println!("cargo:rerun-if-changed={}", proto.display());
        protos.push(proto);
    }

    tonic_build::configure()
        .build_client(false)
        .build_server(true)
        .compile_protos(&protos, &[proto_base_path])
}
