use std::path::PathBuf;

fn main() -> Result<(), std::io::Error> {
    const PROTOC_ENVAR: &str = "PROTOC";

    if std::env::var(PROTOC_ENVAR).is_err() {
        #[cfg(not(windows))]
        std::env::set_var(PROTOC_ENVAR, protobuf_src::protoc());
    }

    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."));
    let proto_base_path = manifest_dir.join("../../src/disco/bam/proto/bam-protos");
    let types_proto = proto_base_path.join("bam_types.proto");
    if !types_proto.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "failed to locate {} (is the bam-protos submodule initialized?)",
                types_proto.display()
            ),
        ));
    }
    let fault_api = manifest_dir.join("proto/bam_api.proto");
    println!("cargo:rerun-if-changed={}", types_proto.display());
    println!("cargo:rerun-if-changed={}", fault_api.display());

    tonic_build::configure()
        .build_client(false)
        .build_server(true)
        .extern_path(
            ".bam_api.RawSchedulerResponse",
            "crate::scenario::RawSchedulerResponse",
        )
        .extern_path(
            ".bam_api.RawAuthChallengeResponse",
            "crate::scenario::RawAuthChallengeResponse",
        )
        .extern_path(
            ".bam_api.RawConfigResponse",
            "crate::scenario::RawConfigResponse",
        )
        .compile_protos(
            &[types_proto, fault_api],
            &[manifest_dir.join("proto"), proto_base_path],
        )
}
