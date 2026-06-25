use std::{
    collections::HashSet,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
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
        MultipleAtomicTxnBatch, Packet, PacketFlags, Ping, Socket,
    },
};
use anyhow::Result;
use clap::Parser;
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
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
    bind: SocketAddr,

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
    #[arg(long, default_value_t = 8003)]
    shred_port: u16,

    /// Builder commission in basis points (default: 300 = 3%)
    #[arg(long, default_value_t = 300)]
    builder_commission_bps: u32,

    /// Builder pubkey to advertise (random if omitted)
    #[arg(long)]
    builder_pubkey: Option<Pubkey>,

    /// Target compute units per bundle
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
}

struct MockBamNode {
    challenge: String,
    config: ConfigResponse,
    bundle_packets: Vec<Packet>,
    heartbeat_interval: Duration,
    bundle_interval: Duration,
    ping_interval: Option<Duration>,
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
        Ok(Response::new(self.config.clone()))
    }

    async fn init_scheduler_stream(
        &self,
        request: Request<tonic::Streaming<SchedulerMessage>>,
    ) -> Result<Response<Self::InitSchedulerStreamStream>, Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(1024);
        let outstanding_pings = Arc::new(Mutex::new(HashSet::new()));

        // Drain inbound messages and validate scheduler Pong replies when ping
        // emission is enabled.
        let outstanding_pings_rx = Arc::clone(&outstanding_pings);
        tokio::spawn(async move {
            while let Ok(Some(msg)) = inbound.message().await {
                if let Some(crate::proto::bam_api::scheduler_message::VersionedMsg::V0(v0)) =
                    msg.versioned_msg
                {
                    if let Some(msg) = v0.msg {
                        match msg {
                            SchedulerMsg::AuthProof(_)
                            | SchedulerMsg::HeartBeat(_)
                            | SchedulerMsg::LeaderState(_)
                            | SchedulerMsg::MultipleAtomicTxnBatchResult(_) => (),
                            SchedulerMsg::Pong(pong) => {
                                let mut pending = outstanding_pings_rx
                                    .lock()
                                    .expect("scheduler ping state mutex poisoned");
                                if pending.remove(&pong.id) {
                                    println!("validated scheduler pong id={}", pong.id);
                                } else {
                                    eprintln!("unexpected scheduler pong id={}", pong.id);
                                }
                            }
                        }
                    }
                }
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
                if heartbeat_tx
                    .send(Ok(SchedulerResponse {
                        versioned_msg: Some(SchedulerRespVersioned::V0(SchedulerResponseV0 {
                            resp: Some(SchedulerResp::HeartBeat(BuilderHeartBeat {
                                time_sent_microseconds: now_us,
                            })),
                        })),
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        // Bundle sender
        let bundle_tx = tx.clone();
        let packets = self.bundle_packets.clone();
        let bundle_interval = self.bundle_interval;
        tokio::spawn(async move {
            let mut next_seq_id = 1_u32;
            let mut interval = tokio::time::interval(bundle_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let seq_id = next_seq_id;
                next_seq_id = next_seq_id.wrapping_add(1);
                if bundle_tx
                    .send(Ok(SchedulerResponse {
                        versioned_msg: Some(SchedulerRespVersioned::V0(SchedulerResponseV0 {
                            resp: Some(SchedulerResp::MultipleAtomicTxnBatch(
                                MultipleAtomicTxnBatch {
                                    batches: vec![AtomicTxnBatch {
                                        seq_id,
                                        max_schedule_slot: u64::MAX,
                                        packets: packets.clone(),
                                    }],
                                },
                            )),
                        })),
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        if let Some(ping_interval) = self.ping_interval {
            let ping_tx = tx.clone();
            let outstanding_pings_tx = Arc::clone(&outstanding_pings);
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(ping_interval);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                let mut next_ping_id = 1_u32;
                loop {
                    interval.tick().await;
                    let ping_id = next_ping_id;
                    next_ping_id = next_ping_id.wrapping_add(1);
                    {
                        let mut pending = outstanding_pings_tx
                            .lock()
                            .expect("scheduler ping state mutex poisoned");
                        pending.insert(ping_id);
                    }
                    if ping_tx
                        .send(Ok(SchedulerResponse {
                            versioned_msg: Some(SchedulerRespVersioned::V0(SchedulerResponseV0 {
                                resp: Some(SchedulerResp::Ping(Ping { id: ping_id })),
                            })),
                        }))
                        .await
                        .is_err()
                    {
                        let mut pending = outstanding_pings_tx
                            .lock()
                            .expect("scheduler ping state mutex poisoned");
                        pending.remove(&ping_id);
                        break;
                    }
                    println!("sent scheduler ping id={ping_id}");
                }
            });
        }

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
            ip: args.tpu_ip.to_string(),
            port: u32::from(args.tpu_port),
        }),
        tpu_fwd_sock: Some(Socket {
            ip: args.tpu_fwd_ip.to_string(),
            port: u32::from(args.tpu_fwd_port),
        }),
        shred_sock: vec![Socket {
            ip: args.shred_ip.to_string(),
            port: u32::from(args.shred_port),
        }],
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
    let builder_pubkey = args
        .builder_pubkey
        .clone()
        .unwrap_or_else(Pubkey::new_unique)
        .to_string();

    let config = build_config(&args, builder_pubkey);
    let bundle_packets = build_packets(args.target_cus, args.cu_per_tx);

    let svc = MockBamNode {
        challenge: "test-challenge".to_string(),
        config,
        bundle_packets,
        heartbeat_interval: Duration::from_secs(u64::from(args.heartbeat_secs)),
        bundle_interval: Duration::from_secs(u64::from(args.bundle_interval_secs)),
        ping_interval: (args.ping_interval_secs != 0)
            .then(|| Duration::from_secs(u64::from(args.ping_interval_secs))),
    };

    let addr = args.bind;
    println!("BAM test server listening on {addr}");
    println!(
        "Bundles: target_cus={}, cu_per_tx={}",
        args.target_cus, args.cu_per_tx
    );
    if args.ping_interval_secs == 0 {
        println!("Scheduler proto ping: disabled");
    } else {
        println!("Scheduler proto ping: every {}s", args.ping_interval_secs);
    }

    tonic::transport::Server::builder()
        .add_service(BamNodeApiServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}
