use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::proto::{
    bam_api::{
        bam_node_api_server::{BamNodeApi, BamNodeApiServer},
        scheduler_message_v0::Msg as SchedulerMsg,
        scheduler_response::VersionedMsg as SchedulerRespVersioned,
        scheduler_response_v0::Resp as SchedulerResp,
        AuthChallengeRequest, AuthChallengeResponse, ConfigRequest, ConfigResponse,
        SchedulerMessage, SchedulerResponse, SchedulerResponseV0,
    },
    bam_types::{
        AtomicTxnBatch, BamConfig, BlockEngineBuilderConfig, BuilderHeartBeat, Meta,
        MultipleAtomicTxnBatch, Packet, PacketFlags, Socket,
    },
};
use anyhow::Result;
use clap::Parser;
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_keypair::Keypair;
use solana_signer::Signer;
use solana_system_interface::instruction::transfer;
use solana_transaction::{Hash, Transaction};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

pub(crate) mod proto {
    pub(crate) mod bam_api {
        tonic::include_proto!("bam_api");
    }

    pub(crate) mod bam_types {
        tonic::include_proto!("bam_types");
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Address to bind the BAM test server (gRPC)
    #[arg(long, default_value = "0.0.0.0:50055")]
    bind: String,

    /// TPU address to advertise to the validator
    #[arg(long, default_value = "127.0.0.1")]
    tpu_ip: String,

    /// TPU port to advertise to the validator
    #[arg(long, default_value_t = 5004)]
    tpu_port: u16,

    /// TPU forward address to advertise to the validator
    #[arg(long, default_value = "127.0.0.1")]
    tpu_fwd_ip: String,

    /// TPU forward port to advertise to the validator
    #[arg(long, default_value_t = 5005)]
    tpu_fwd_port: u16,

    /// Builder commission in basis points (default: 300 = 3%)
    #[arg(long, default_value_t = 300)]
    builder_commission_bps: u32,

    /// Builder pubkey to advertise (random if omitted)
    #[arg(long, default_value = "")]
    builder_pubkey: String,

    /// Target compute units per bundle
    #[arg(long, default_value_t = 60_000_000)]
    target_cus: u64,

    /// Compute unit limit per transaction
    #[arg(long, default_value_t = 1_000_000)]
    cu_per_tx: u32,

    /// Heartbeat interval in seconds
    #[arg(long, default_value_t = 2)]
    heartbeat_secs: u64,

    /// Bundle interval in seconds
    #[arg(long, default_value_t = 1)]
    bundle_interval_secs: u64,
}

#[derive(Clone)]
struct MockBamNode {
    challenge: String,
    config: Arc<ConfigResponse>,
    bundle_packets: Arc<Vec<Packet>>,
    next_seq_id: Arc<AtomicU32>,
    heartbeat_interval: Duration,
    bundle_interval: Duration,
}

#[tonic::async_trait]
impl BamNodeApi for MockBamNode {
    type InitSchedulerStreamStream = ReceiverStream<Result<SchedulerResponse, Status>>;

    async fn get_auth_challenge(
        &self,
        _request: Request<AuthChallengeRequest>,
    ) -> Result<Response<AuthChallengeResponse>, Status> {
        Ok(Response::new(AuthChallengeResponse {
            challenge_to_sign: self.challenge.clone(),
        }))
    }

    async fn get_builder_config(
        &self,
        _request: Request<ConfigRequest>,
    ) -> Result<Response<ConfigResponse>, Status> {
        Ok(Response::new((*self.config).clone()))
    }

    async fn init_scheduler_stream(
        &self,
        request: Request<tonic::Streaming<SchedulerMessage>>,
    ) -> Result<Response<Self::InitSchedulerStreamStream>, Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(1024);

        // Drain inbound messages; accept anything.
        tokio::spawn(async move {
            while let Ok(Some(msg)) = inbound.message().await {
                let _ = msg.versioned_msg.as_ref().and_then(|v| match v {
                    crate::proto::bam_api::scheduler_message::VersionedMsg::V0(v0) => {
                        v0.msg.as_ref().map(|m| match m {
                            SchedulerMsg::AuthProof(_) => (),
                            SchedulerMsg::HeartBeat(_) => (),
                            SchedulerMsg::LeaderState(_) => (),
                            SchedulerMsg::MultipleAtomicTxnBatchResult(_) => (),
                        })
                    }
                });
            }
        });

        // Heartbeat sender
        let heartbeat_tx = tx.clone();
        let heartbeat_interval = self.heartbeat_interval;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(heartbeat_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let now_us = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_micros() as u64;
                let resp = SchedulerResponse {
                    versioned_msg: Some(SchedulerRespVersioned::V0(SchedulerResponseV0 {
                        resp: Some(SchedulerResp::HeartBeat(BuilderHeartBeat {
                            time_sent_microseconds: now_us,
                        })),
                    })),
                };
                if heartbeat_tx.send(Ok(resp)).await.is_err() {
                    break;
                }
            }
        });

        // Bundle sender
        let bundle_tx = tx.clone();
        let packets = self.bundle_packets.clone();
        let next_seq_id = self.next_seq_id.clone();
        let bundle_interval = self.bundle_interval;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(bundle_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let seq_id = next_seq_id.fetch_add(1, Ordering::Relaxed);
                let batch = AtomicTxnBatch {
                    seq_id,
                    max_schedule_slot: u64::MAX,
                    packets: packets.as_ref().clone(),
                };
                let resp = SchedulerResponse {
                    versioned_msg: Some(SchedulerRespVersioned::V0(SchedulerResponseV0 {
                        resp: Some(SchedulerResp::MultipleAtomicTxnBatch(
                            MultipleAtomicTxnBatch {
                                batches: vec![batch],
                            },
                        )),
                    })),
                };
                if bundle_tx.send(Ok(resp)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

fn build_packets(target_cus: u64, cu_per_tx: u32) -> Vec<Packet> {
    let keypair = Keypair::new();
    let blockhash = Hash::new_from_array(keypair.pubkey().to_bytes());
    let per_tx = u64::from(cu_per_tx.max(1));
    let tx_count = ((target_cus + per_tx - 1) / per_tx).max(1);

    let mut packets = Vec::with_capacity(tx_count as usize);
    for _ in 0..tx_count {
        let tx = Transaction::new_signed_with_payer(
            &[
                ComputeBudgetInstruction::set_compute_unit_limit(cu_per_tx),
                transfer(&keypair.pubkey(), &keypair.pubkey(), 1),
            ],
            Some(&keypair.pubkey()),
            &[&keypair],
            blockhash.clone(),
        );
        let data = bincode::serialize(&tx).expect("serialize transaction");
        let size = data.len() as u64;
        packets.push(Packet {
            data,
            meta: Some(Meta {
                size,
                flags: Some(PacketFlags {
                    simple_vote_tx: false,
                    revert_on_error: true,
                }),
            }),
        });
    }
    packets
}

fn build_config(args: &Args, builder_pubkey: String) -> ConfigResponse {
    let block_engine_config = BlockEngineBuilderConfig {
        builder_pubkey: builder_pubkey.clone(),
        builder_commission: args.builder_commission_bps,
    };

    let bam_config = BamConfig {
        tpu_sock: Some(Socket {
            ip: args.tpu_ip.clone(),
            port: u32::from(args.tpu_port),
        }),
        tpu_fwd_sock: Some(Socket {
            ip: args.tpu_fwd_ip.clone(),
            port: u32::from(args.tpu_fwd_port),
        }),
        commission_bps: args.builder_commission_bps,
        prio_fee_recipient_pubkey: builder_pubkey,
    };

    ConfigResponse {
        block_engine_config: Some(block_engine_config),
        bam_config: Some(bam_config),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let builder_pubkey = if args.builder_pubkey.is_empty() {
        solana_pubkey::Pubkey::new_unique().to_string()
    } else {
        args.builder_pubkey.clone()
    };

    let config = build_config(&args, builder_pubkey);
    let bundle_packets = build_packets(args.target_cus, args.cu_per_tx);

    let svc = MockBamNode {
        challenge: "test-challenge".to_string(),
        config: Arc::new(config),
        bundle_packets: Arc::new(bundle_packets),
        next_seq_id: Arc::new(AtomicU32::new(1)),
        heartbeat_interval: Duration::from_secs(args.heartbeat_secs),
        bundle_interval: Duration::from_secs(args.bundle_interval_secs),
    };

    let addr: SocketAddr = args.bind.parse()?;
    println!("BAM test server listening on {addr}");
    println!(
        "Bundles: target_cus={}, cu_per_tx={}",
        args.target_cus, args.cu_per_tx
    );

    tonic::transport::Server::builder()
        .add_service(BamNodeApiServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}
