use std::path::PathBuf;

fn main() -> Result<(), std::io::Error> {
    const PROTOC_ENVAR: &str = "PROTOC";
    const PROTO_DIR: &str = "../bam-test-server/proto/solana";
    const PROTO_FILES: [&str; 4] = [
        "context.proto",
        "metadata.proto",
        "txn.proto",
        "invoke.proto",
    ];

    if std::env::var(PROTOC_ENVAR).is_err() {
        #[cfg(not(windows))]
        std::env::set_var(PROTOC_ENVAR, protobuf_src::protoc());
    }

    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."));
    let proto_base = manifest_dir.join(PROTO_DIR);
    if !PROTO_FILES
        .iter()
        .all(|file| proto_base.join(file).is_file())
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "failed to locate txn bridge proto files {PROTO_FILES:?} under {}",
                proto_base.display()
            ),
        ));
    }

    let protos = PROTO_FILES.map(|file| {
        let proto = proto_base.join(file);
        println!("cargo:rerun-if-changed={}", proto.display());
        proto
    });

    prost_build::Config::new().compile_protos(&protos, &[proto_base])
}
