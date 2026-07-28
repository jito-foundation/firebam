use std::path::PathBuf;

fn main() -> Result<(), std::io::Error> {
    const PROTOC_ENVAR: &str = "PROTOC";
    const PROTO_FILES: [&str; 1] = ["bam_types.proto"];
    const PROTO_DIR_CANDIDATES: [&str; 4] = [
        "../../src/disco/bam/proto/bam-protos",
        "../../src/disco/bam/proto",
        "../../bam-client/jito-protos/bam-protos",
        "../../bam/jito-protos/jss-protos",
    ];

    if std::env::var(PROTOC_ENVAR).is_err() {
        #[cfg(not(windows))]
        std::env::set_var(PROTOC_ENVAR, protobuf_src::protoc());
    }

    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."));
    let proto_base_path = PROTO_DIR_CANDIDATES
        .into_iter()
        .map(|dir| manifest_dir.join(dir))
        .find(|candidate| {
            PROTO_FILES
                .iter()
                .all(|file| candidate.join(file).is_file())
        })
        .ok_or(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "failed to locate BAM proto files {PROTO_FILES:?} from {}",
                manifest_dir.display()
            ),
        ))?;

    let mut protos = PROTO_FILES
        .map(|file| {
            let proto = proto_base_path.join(file);
            println!("cargo:rerun-if-changed={}", proto.display());
            proto
        })
        .to_vec();
    let fault_api = manifest_dir.join("proto/bam_api.proto");
    println!("cargo:rerun-if-changed={}", fault_api.display());
    protos.push(fault_api);

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
        .compile_protos(&protos, &[manifest_dir.join("proto"), proto_base_path])
}
