use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};

use anyhow::{bail, Result};
use clap::Parser;
use solana_pubkey::Pubkey;
use tonic::transport::{Identity, Server, ServerTlsConfig};

mod proto;
mod scenario;

use crate::{
    proto::bam_api::bam_node_api_server::BamNodeApiServer,
    scenario::{MockBamNode, ScenarioKind},
};

#[derive(Parser, Debug, Clone)]
#[command(author, version, about)]
pub struct Args {
    /// Scenario name for the BAM controller.
    #[arg(long, value_enum, default_value_t = ScenarioKind::Benign)]
    scenario: ScenarioKind,

    /// Optional scripted scenario file (.toml or .json). Overrides --scenario delivery behavior.
    #[arg(long)]
    scenario_file: Option<PathBuf>,

    /// Address to bind the BAM test server (gRPC)
    #[arg(long, default_value = "0.0.0.0:50055")]
    bind: SocketAddr,

    /// PEM certificate used to serve gRPC over TLS.
    #[arg(long, requires = "tls_key")]
    tls_cert: Option<PathBuf>,

    /// PEM private key used to serve gRPC over TLS.
    #[arg(long, requires = "tls_cert")]
    tls_key: Option<PathBuf>,

    /// TPU address to advertise to the validator
    #[arg(long, default_value = "127.0.0.1")]
    tpu_ip: IpAddr,

    /// TPU port to advertise to the validator
    #[arg(long, default_value_t = 5004)]
    tpu_port: u16,

    /// TPU forward address to advertise to the validator
    #[arg(long, default_value = "127.0.0.1")]
    tpu_fwd_ip: IpAddr,

    /// TPU forward port to advertise to the validator
    #[arg(long, default_value_t = 5005)]
    tpu_fwd_port: u16,

    /// Shred address to advertise to the validator
    #[arg(long, default_value = "127.0.0.1")]
    shred_ip: IpAddr,

    /// Shred port to advertise to the validator
    #[arg(long, default_value_t = 5006)]
    shred_port: u16,

    /// Block Engine builder commission as a percentage in [0,100].
    #[arg(long, default_value_t = 3)]
    builder_commission_pct: u32,

    /// Validator commission in basis points for BamConfig metadata.
    #[arg(long, default_value_t = 300)]
    commission_bps: u32,

    /// Builder pubkey to advertise (all-zero pubkey if omitted)
    #[arg(long)]
    builder_pubkey: Option<Pubkey>,

    /// Omit BlockEngineBuilderConfig from GetBuilderConfig responses.
    #[arg(long, default_value_t = false)]
    omit_block_engine_config: bool,

    /// Target compute units per benign bundle
    #[arg(long, default_value_t = 60_000_000)]
    target_cus: u64,

    /// Compute unit limit per transaction
    #[arg(long, default_value_t = 1_000_000)]
    cu_per_tx: u32,

    /// Heartbeat interval in seconds
    #[arg(long, default_value_t = 2)]
    heartbeat_secs: u8,

    /// Bundle interval in seconds
    #[arg(long, default_value_t = 1)]
    bundle_interval_secs: u8,

    /// Scheduler ping interval in seconds; 0 disables BAM proto ping emission
    #[arg(long, default_value_t = 0)]
    ping_interval_secs: u8,

    /// Seq id used by the duplicate-seq-id overrun scenario
    #[arg(long, default_value_t = 42)]
    h02_seq_id: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let svc = MockBamNode::from_args(&args)?;

    println!("BAM test server listening on {}", args.bind);
    for line in svc.summary_lines() {
        println!("{line}");
    }

    let mut server = Server::builder();
    match (&args.tls_cert, &args.tls_key) {
        (Some(cert_path), Some(key_path)) => {
            let cert = std::fs::read(cert_path)?;
            let key = std::fs::read(key_path)?;
            server = server
                .tls_config(ServerTlsConfig::new().identity(Identity::from_pem(cert, key)))?;
            println!("BAM test server TLS enabled");
        }
        (None, None) => {}
        _ => bail!("--tls-cert and --tls-key must be supplied together"),
    }

    server
        .add_service(BamNodeApiServer::new(svc))
        .serve(args.bind)
        .await?;

    Ok(())
}
