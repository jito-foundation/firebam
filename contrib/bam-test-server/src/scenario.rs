macro_rules! println {
    ($($arg:tt)*) => {{
        std::eprintln!($($arg)*);
    }};
}

use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context, Result};
use base64::prelude::*;
use clap::ValueEnum;
use prost::{
    bytes::{Buf, BufMut},
    encoding::{DecodeContext, WireType},
    DecodeError, Message,
};
use serde::Deserialize;
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_keypair::Keypair;
use solana_signer::Signer;
use solana_system_interface::instruction::transfer;
use solana_transaction::{Hash, Transaction};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request, Response, Status};

use crate::{
    proto::{
        bam_api::{
            bam_node_api_server::BamNodeApi, scheduler_message_v0::Msg as SchedulerMsg,
            scheduler_response::VersionedMsg as SchedulerRespVersioned,
            scheduler_response_v0::Resp as SchedulerResp, AuthChallengeRequest,
            AuthChallengeResponse, ConfigRequest, ConfigResponse, SchedulerMessage,
            SchedulerResponse, SchedulerResponseV0,
        },
        bam_types::{
            atomic_txn_batch_result, not_committed, AtomicTxnBatch, AtomicTxnBatchResult,
            BamConfig, BlockEngineBuilderConfig, BuilderHeartBeat, DeserializationErrorReason,
            Meta, MultipleAtomicTxnBatch, Packet, PacketFlags, Ping, SchedulingError, Socket,
            TransactionCommittedResult, TransactionErrorReason,
        },
    },
    Args,
};

const MAX_ATOMIC_BATCH_PACKETS: u64 = 5;
const SCHEDULE_SLOT_LATEST_LEADER_MAX_OFFSET: u64 = 1024;
const SCHEDULE_SLOT_LATEST_LEADER_BASE: u64 = u64::MAX - SCHEDULE_SLOT_LATEST_LEADER_MAX_OFFSET;

#[derive(Clone, Debug, Default)]
pub struct RawSchedulerResponse {
    bytes: Vec<u8>,
}

#[derive(Clone, Debug, Default)]
pub struct RawAuthChallengeResponse {
    bytes: Vec<u8>,
}

impl RawAuthChallengeResponse {
    fn from_message(message: AuthChallengeResponse) -> Self {
        Self {
            bytes: message.encode_to_vec(),
        }
    }

    fn from_bytes(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }
}

#[derive(Clone, Debug, Default)]
pub struct RawConfigResponse {
    bytes: Vec<u8>,
}

impl RawConfigResponse {
    fn from_message(message: ConfigResponse) -> Self {
        Self {
            bytes: message.encode_to_vec(),
        }
    }

    fn from_bytes(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }
}

impl RawSchedulerResponse {
    fn from_message(message: SchedulerResponse) -> Self {
        Self {
            bytes: message.encode_to_vec(),
        }
    }

    fn from_bytes(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }
}

impl Message for RawSchedulerResponse {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        buf.put_slice(&self.bytes);
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        prost::encoding::skip_field(wire_type, tag, buf, ctx)
    }

    fn encoded_len(&self) -> usize {
        self.bytes.len()
    }

    fn clear(&mut self) {
        self.bytes.clear();
    }
}

macro_rules! impl_raw_message {
    ($name:ty) => {
        impl Message for $name {
            fn encode_raw(&self, buf: &mut impl BufMut) {
                buf.put_slice(&self.bytes);
            }

            fn merge_field(
                &mut self,
                tag: u32,
                wire_type: WireType,
                buf: &mut impl Buf,
                ctx: DecodeContext,
            ) -> Result<(), DecodeError> {
                prost::encoding::skip_field(wire_type, tag, buf, ctx)
            }

            fn encoded_len(&self) -> usize {
                self.bytes.len()
            }

            fn clear(&mut self) {
                self.bytes.clear();
            }
        }
    };
}

impl_raw_message!(RawAuthChallengeResponse);
impl_raw_message!(RawConfigResponse);

#[derive(Clone, Debug)]
enum ScenarioPlan {
    Named(ScenarioKind),
    Scripted(ScriptedScenario),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum ScenarioKind {
    #[default]
    Benign,
    C01AuthAbort,
    H01ConfigRedirect,
    H02SeqIdOverrun,
    H03BamOverride,
}

impl ScenarioKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Benign => "Benign",
            Self::C01AuthAbort => "C01AuthAbort",
            Self::H01ConfigRedirect => "H01ConfigRedirect",
            Self::H02SeqIdOverrun => "H02SeqIdOverrun",
            Self::H03BamOverride => "H03BamOverride",
        }
    }
}

#[derive(Clone, Debug)]
struct ScriptedScenario {
    source_path: PathBuf,
    description: Option<String>,
    heartbeat_interval: Option<Duration>,
    ping_interval: Option<Duration>,
    replay_on_reconnect: bool,
    resume_on_reconnect: bool,
    auth_status_sequence: Vec<RpcStatusSpec>,
    auth_delay_ms_sequence: Vec<u64>,
    auth_challenge_sequence: Vec<String>,
    auth_raw_hex_sequence: Vec<String>,
    config_status_sequence: Vec<RpcStatusSpec>,
    config_delay_ms_sequence: Vec<u64>,
    config_variant_sequence: Vec<String>,
    config_raw_hex_sequence: Vec<String>,
    stream_status_sequence: Vec<RpcStatusSpec>,
    stream_delay_ms_sequence: Vec<u64>,
    runtime: Arc<ScriptedRuntime>,
    events: Vec<ScriptedEvent>,
}

#[derive(Debug)]
struct ScriptedRuntime {
    next_event_idx: AtomicUsize,
    next_seq_id: AtomicU32,
    next_ping_id: AtomicU32,
    next_auth_status: AtomicUsize,
    next_auth_delay: AtomicUsize,
    next_auth_challenge: AtomicUsize,
    next_auth_raw_hex: AtomicUsize,
    next_config_status: AtomicUsize,
    next_config_delay: AtomicUsize,
    next_config_variant: AtomicUsize,
    next_config_raw_hex: AtomicUsize,
    next_stream_status: AtomicUsize,
    next_stream_delay: AtomicUsize,
}

#[derive(Clone, Debug)]
struct RpcStatusSpec {
    code: Code,
    message: String,
}

#[derive(Clone, Debug, Deserialize)]
struct RpcStatusSpecDef {
    code: String,
    message: Option<String>,
}

#[derive(Clone, Debug)]
enum ScriptedEvent {
    Sleep {
        ms: u64,
    },
    SleepResumeSafe {
        ms: u64,
    },
    SendBatch(BatchSpec),
    SendBatchFlood(BatchFloodSpec),
    SendMultiBatch(Vec<BatchSpec>),
    SendSplitBatch(SplitBatchSpec),
    SendPing {
        id: Option<u32>,
    },
    SendHeartbeat {
        time_sent_microseconds: u64,
    },
    SendEmptyEnvelope,
    SendEmptyV0,
    SendUnsupportedVersion,
    SendUnsupportedV0,
    SendEmptyMultiBatch,
    SendRawResponse {
        name: String,
        bytes: Vec<u8>,
    },
    SendRawOverflowFaults {
        start_seq_id: u32,
    },
    SendStatus {
        code: Code,
        message: String,
    },
    ConfigUpdate(ConfigUpdateSpec),
    WaitInbound {
        kind: WaitInboundKind,
        min_count: u64,
        timeout_ms: u64,
    },
    CloseStream,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum WaitInboundKind {
    AuthProof,
    Heartbeat,
    LeaderState,
    BatchResultMessage,
    BatchResult,
    CommittedBatch,
    NotCommittedBatch,
    Pong,
}

impl WaitInboundKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::AuthProof => "auth_proof",
            Self::Heartbeat => "heartbeat",
            Self::LeaderState => "leader_state",
            Self::BatchResultMessage => "batch_result_message",
            Self::BatchResult => "batch_result",
            Self::CommittedBatch => "committed_batch",
            Self::NotCommittedBatch => "not_committed_batch",
            Self::Pong => "pong",
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct InboundCounts {
    auth_proof: u64,
    heartbeat: u64,
    leader_state: u64,
    last_leader_slot: Option<u64>,
    batch_result_message: u64,
    batch_result: u64,
    committed_batch: u64,
    not_committed_batch: u64,
    pong: u64,
}

impl InboundCounts {
    fn count(self, kind: WaitInboundKind) -> u64 {
        match kind {
            WaitInboundKind::AuthProof => self.auth_proof,
            WaitInboundKind::Heartbeat => self.heartbeat,
            WaitInboundKind::LeaderState => self.leader_state,
            WaitInboundKind::BatchResultMessage => self.batch_result_message,
            WaitInboundKind::BatchResult => self.batch_result,
            WaitInboundKind::CommittedBatch => self.committed_batch,
            WaitInboundKind::NotCommittedBatch => self.not_committed_batch,
            WaitInboundKind::Pong => self.pong,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ScriptedEventDef {
    Sleep {
        ms: u64,
    },
    SleepResumeSafe {
        ms: u64,
    },
    SendBatch {
        seq_id: Option<u32>,
        expect_result: Option<bool>,
        packet_count: Option<u64>,
        packets_base64: Option<Vec<String>>,
        packets_base64_file: Option<PathBuf>,
        max_schedule_slot: Option<ScheduleSlotDef>,
        simple_vote_tx: Option<bool>,
        revert_on_error: Option<bool>,
        revert_on_error_sequence: Option<Vec<bool>>,
        packet_meta_sequence: Option<Vec<PacketMetaModeDef>>,
        packet_data_size: Option<u64>,
        cu_per_tx: Option<u32>,
    },
    SendBatchFlood {
        batch_count: u64,
        start_seq_id: Option<u32>,
        packet_count: Option<u64>,
        packets_base64: Option<Vec<String>>,
        packets_base64_file: Option<PathBuf>,
        max_schedule_slot: Option<ScheduleSlotDef>,
        simple_vote_tx: Option<bool>,
        revert_on_error: Option<bool>,
        revert_on_error_sequence: Option<Vec<bool>>,
        packet_meta_sequence: Option<Vec<PacketMetaModeDef>>,
        packet_data_size: Option<u64>,
        cu_per_tx: Option<u32>,
    },
    SendMultiBatch {
        batches: Vec<BatchEventDef>,
    },
    SendSplitBatch {
        seq_id: u32,
        splits: Vec<u64>,
        packet_count: Option<u64>,
        packets_base64: Option<Vec<String>>,
        packets_base64_file: Option<PathBuf>,
        max_schedule_slot: Option<ScheduleSlotDef>,
        simple_vote_tx: Option<bool>,
        revert_on_error: Option<bool>,
        revert_on_error_sequence: Option<Vec<bool>>,
        packet_meta_sequence: Option<Vec<PacketMetaModeDef>>,
        packet_data_size: Option<u64>,
        cu_per_tx: Option<u32>,
    },
    SendPing {
        id: Option<u32>,
    },
    SendHeartbeat {
        time_sent_microseconds: Option<u64>,
    },
    SendEmptyEnvelope,
    SendEmptyV0,
    SendUnsupportedVersion,
    SendUnsupportedV0,
    SendEmptyMultiBatch,
    SendRawResponse {
        name: String,
        hex: String,
    },
    SendRawOverflowFaults {
        start_seq_id: Option<u32>,
    },
    SendStatus {
        code: String,
        message: Option<String>,
    },
    ConfigUpdate {
        builder_pubkey: Option<String>,
        builder_commission_pct: Option<u32>,
        prio_fee_recipient_pubkey: Option<String>,
        commission_bps: Option<u32>,
        include_block_engine_config: Option<bool>,
        include_bam_config: Option<bool>,
    },
    WaitInbound {
        kind: WaitInboundKind,
        min_count: Option<u64>,
        timeout_ms: u64,
    },
    CloseStream,
}

#[derive(Clone, Debug, Deserialize)]
struct ScriptedScenarioDef {
    description: Option<String>,
    heartbeat_interval_ms: Option<u64>,
    ping_interval_ms: Option<u64>,
    replay_on_reconnect: Option<bool>,
    resume_on_reconnect: Option<bool>,
    #[serde(default)]
    auth_status_sequence: Vec<RpcStatusSpecDef>,
    #[serde(default)]
    auth_delay_ms_sequence: Vec<u64>,
    #[serde(default)]
    auth_challenge_sequence: Vec<String>,
    #[serde(default)]
    auth_raw_hex_sequence: Vec<String>,
    #[serde(default)]
    config_status_sequence: Vec<RpcStatusSpecDef>,
    #[serde(default)]
    config_delay_ms_sequence: Vec<u64>,
    #[serde(default)]
    config_variant_sequence: Vec<String>,
    #[serde(default)]
    config_raw_hex_sequence: Vec<String>,
    #[serde(default)]
    stream_status_sequence: Vec<RpcStatusSpecDef>,
    #[serde(default)]
    stream_delay_ms_sequence: Vec<u64>,
    events: Vec<ScriptedEventDef>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum ScheduleSlotDef {
    Number(u64),
    Keyword(String),
}

#[derive(Clone, Debug, Deserialize)]
struct BatchEventDef {
    seq_id: Option<u32>,
    expect_result: Option<bool>,
    packet_count: Option<u64>,
    packets_base64: Option<Vec<String>>,
    packets_base64_file: Option<PathBuf>,
    max_schedule_slot: Option<ScheduleSlotDef>,
    simple_vote_tx: Option<bool>,
    revert_on_error: Option<bool>,
    revert_on_error_sequence: Option<Vec<bool>>,
    packet_meta_sequence: Option<Vec<PacketMetaModeDef>>,
    packet_data_size: Option<u64>,
    cu_per_tx: Option<u32>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PacketMetaModeDef {
    Full,
    MissingMeta,
    MissingFlags,
}

#[derive(Clone, Debug)]
struct BatchSpec {
    seq_id: Option<u32>,
    expect_result: bool,
    packets: Vec<Packet>,
    max_schedule_slot: u64,
}

#[derive(Clone, Debug)]
struct BatchFloodSpec {
    batch_count: u64,
    start_seq_id: u32,
    batch: BatchSpec,
}

#[derive(Clone, Debug)]
struct SplitBatchSpec {
    seq_id: u32,
    packet_slices: Vec<Vec<Packet>>,
    max_schedule_slot: u64,
}

#[derive(Clone, Debug)]
struct ConfigUpdateSpec {
    builder_pubkey: Option<String>,
    builder_commission_pct: Option<u32>,
    prio_fee_recipient_pubkey: Option<String>,
    commission_bps: Option<u32>,
    include_block_engine_config: Option<bool>,
    include_bam_config: Option<bool>,
}

pub struct MockBamNode {
    plan: ScenarioPlan,
    challenge: String,
    config: Arc<Mutex<ConfigResponse>>,
    benign_bundle_packets: Vec<Packet>,
    default_cu_per_tx: u32,
    default_heartbeat_interval: Duration,
    default_bundle_interval: Duration,
    default_ping_interval: Option<Duration>,
    h02_seq_id: u32,
    scheduler_connection_counter: AtomicUsize,
}

impl MockBamNode {
    pub fn from_args(args: &Args) -> Result<Self> {
        let builder_pubkey = args
            .builder_pubkey
            .clone()
            .unwrap_or_default()
            .to_string();
        let plan = if let Some(path) = args.scenario_file.as_ref() {
            ScenarioPlan::Scripted(load_scripted_scenario(path, args.cu_per_tx)?)
        } else {
            ScenarioPlan::Named(args.scenario)
        };

        Ok(Self {
            challenge: match plan {
                ScenarioPlan::Named(ScenarioKind::C01AuthAbort) => "12345678".to_string(),
                _ => "test-challenge".to_string(),
            },
            plan,
            config: Arc::new(Mutex::new(build_config(args, builder_pubkey))),
            benign_bundle_packets: build_packets(
                target_packet_count(args.target_cus, args.cu_per_tx).min(MAX_ATOMIC_BATCH_PACKETS),
                args.cu_per_tx,
                false,
                true,
            ),
            default_cu_per_tx: args.cu_per_tx,
            default_heartbeat_interval: Duration::from_secs(u64::from(args.heartbeat_secs)),
            default_bundle_interval: Duration::from_secs(u64::from(args.bundle_interval_secs)),
            default_ping_interval: (args.ping_interval_secs != 0)
                .then(|| Duration::from_secs(u64::from(args.ping_interval_secs))),
            h02_seq_id: args.h02_seq_id,
            scheduler_connection_counter: AtomicUsize::new(0),
        })
    }

    pub fn summary_lines(&self) -> Vec<String> {
        let config = self
            .config
            .lock()
            .expect("BAM config mutex poisoned")
            .clone();
        let mut lines = vec![
            format!(
                "Auth challenge: len={} value={:?}",
                self.challenge.len(),
                self.challenge
            ),
            format!(
                "Advertised sockets: tpu={} fwd={} shred={}",
                format_socket(
                    config
                        .bam_config
                        .as_ref()
                        .and_then(|cfg| cfg.tpu_sock.as_ref())
                ),
                format_socket(
                    config
                        .bam_config
                        .as_ref()
                        .and_then(|cfg| cfg.tpu_fwd_sock.as_ref())
                ),
                format_socket_list(
                    config
                        .bam_config
                        .as_ref()
                        .map(|cfg| cfg.shred_sock.as_slice())
                        .unwrap_or(&[])
                ),
            ),
            format!(
                "Bundle cadence: every {:?}, benign packet_count={}",
                self.default_bundle_interval,
                self.benign_bundle_packets.len()
            ),
        ];

        match &self.plan {
            ScenarioPlan::Named(scenario) => {
                lines.insert(0, format!("Scenario: {}", scenario.as_str()));
                if *scenario == ScenarioKind::H02SeqIdOverrun {
                    lines.push(format!(
                        "H-02 payload: duplicate seq_id={} split=[5,1]",
                        self.h02_seq_id
                    ));
                }

                if let Some(interval) = self.default_ping_interval {
                    lines.push(format!("Scheduler ping cadence: every {:?}", interval));
                } else {
                    lines.push("Scheduler ping cadence: disabled".to_string());
                }
            }
            ScenarioPlan::Scripted(script) => {
                lines.insert(
                    0,
                    format!("Scenario: scripted ({})", script.source_path.display()),
                );
                if let Some(description) = script.description.as_ref() {
                    lines.insert(1, format!("Description: {description}"));
                }
                lines.push(format!("Scripted events: {}", script.events.len()));
                lines.push(format!(
                    "Replay scripted events on reconnect: {}",
                    script.replay_on_reconnect
                ));
                lines.push(format!(
                    "Resume scripted events on reconnect: {}",
                    script.resume_on_reconnect
                ));
                if let Some(interval) = script.ping_interval {
                    lines.push(format!("Scheduler ping cadence: every {:?}", interval));
                } else {
                    lines.push("Scheduler ping cadence: disabled".to_string());
                }
            }
        }

        lines
    }
}

#[tonic::async_trait]
impl BamNodeApi for MockBamNode {
    type InitSchedulerStreamStream = ReceiverStream<Result<RawSchedulerResponse, Status>>;

    async fn get_auth_challenge(
        &self,
        _request: Request<AuthChallengeRequest>,
    ) -> Result<Response<RawAuthChallengeResponse>, Status> {
        let challenge = match &self.plan {
            ScenarioPlan::Scripted(script) => {
                if let Some(status) = next_rpc_status(
                    &script.auth_status_sequence,
                    &script.runtime.next_auth_status,
                ) {
                    println!(
                        "GetAuthChallenge: scenario={} scripted_status={:?} message={:?}",
                        self.plan_name(),
                        status.code,
                        status.message
                    );
                    return Err(build_rpc_status(status));
                }
                if let Some(delay_ms) = next_u64_value(
                    &script.auth_delay_ms_sequence,
                    &script.runtime.next_auth_delay,
                ) {
                    println!("GetAuthChallenge: scripted_delay_ms={delay_ms}");
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
                if let Some(hex) = next_string_value(
                    &script.auth_raw_hex_sequence,
                    &script.runtime.next_auth_raw_hex,
                ) {
                    let bytes = decode_hex(&hex).map_err(|err| {
                        Status::internal(format!("invalid scripted auth raw hex: {err:#}"))
                    })?;
                    println!(
                        "GetAuthChallenge: scenario={} scripted_raw_bytes={} hex={}",
                        self.plan_name(),
                        bytes.len(),
                        encode_hex(&bytes)
                    );
                    return Ok(Response::new(RawAuthChallengeResponse::from_bytes(bytes)));
                }
                next_string_value(
                    &script.auth_challenge_sequence,
                    &script.runtime.next_auth_challenge,
                )
                .unwrap_or_else(|| self.challenge.clone())
            }
            ScenarioPlan::Named(_) => self.challenge.clone(),
        };
        println!(
            "GetAuthChallenge: scenario={} challenge_len={} challenge={:?}",
            self.plan_name(),
            challenge.len(),
            challenge
        );

        Ok(Response::new(RawAuthChallengeResponse::from_message(
            AuthChallengeResponse {
                challenge_to_sign: challenge,
            },
        )))
    }

    async fn get_builder_config(
        &self,
        _request: Request<ConfigRequest>,
    ) -> Result<Response<RawConfigResponse>, Status> {
        let mut config = self
            .config
            .lock()
            .expect("BAM config mutex poisoned")
            .clone();
        if let ScenarioPlan::Scripted(script) = &self.plan {
            if let Some(status) = next_rpc_status(
                &script.config_status_sequence,
                &script.runtime.next_config_status,
            ) {
                println!(
                    "GetBuilderConfig: scenario={} scripted_status={:?} message={:?}",
                    self.plan_name(),
                    status.code,
                    status.message
                );
                return Err(build_rpc_status(status));
            }
            if let Some(delay_ms) = next_u64_value(
                &script.config_delay_ms_sequence,
                &script.runtime.next_config_delay,
            ) {
                println!("GetBuilderConfig: scripted_delay_ms={delay_ms}");
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            if let Some(hex) = next_string_value(
                &script.config_raw_hex_sequence,
                &script.runtime.next_config_raw_hex,
            ) {
                let bytes = decode_hex(&hex).map_err(|err| {
                    Status::internal(format!("invalid scripted config raw hex: {err:#}"))
                })?;
                println!(
                    "GetBuilderConfig: scenario={} scripted_raw_bytes={} hex={}",
                    self.plan_name(),
                    bytes.len(),
                    encode_hex(&bytes)
                );
                return Ok(Response::new(RawConfigResponse::from_bytes(bytes)));
            }
            if let Some(variant) = next_string_value(
                &script.config_variant_sequence,
                &script.runtime.next_config_variant,
            ) {
                apply_config_variant(&mut config, &variant).map_err(|err| {
                    Status::internal(format!("invalid scripted config variant: {err:#}"))
                })?;
                println!("GetBuilderConfig: scripted_variant={variant:?}");
            }
        }
        println!(
            "GetBuilderConfig: scenario={} {}",
            self.plan_name(),
            format_config_summary(&config),
        );
        Ok(Response::new(RawConfigResponse::from_message(config)))
    }

    async fn init_scheduler_stream(
        &self,
        request: Request<tonic::Streaming<SchedulerMessage>>,
    ) -> Result<Response<Self::InitSchedulerStreamStream>, Status> {
        if let ScenarioPlan::Scripted(script) = &self.plan {
            if let Some(status) = next_rpc_status(
                &script.stream_status_sequence,
                &script.runtime.next_stream_status,
            ) {
                println!(
                    "InitSchedulerStream: scenario={} scripted_status={:?} message={:?}",
                    self.plan_name(),
                    status.code,
                    status.message
                );
                return Err(build_rpc_status(status));
            }
            if let Some(delay_ms) = next_u64_value(
                &script.stream_delay_ms_sequence,
                &script.runtime.next_stream_delay,
            ) {
                println!("InitSchedulerStream: scripted_delay_ms={delay_ms}");
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        }
        let conn_id = self
            .scheduler_connection_counter
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        println!(
            "InitSchedulerStream: scenario={} conn={}",
            self.plan_name(),
            conn_id
        );

        let inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(1024);
        let outstanding_pings = Arc::new(Mutex::new(HashSet::new()));
        let (inbound_updates_tx, inbound_updates_rx) = watch::channel(InboundCounts::default());
        let (stop_tx, stop_rx) = watch::channel(false);

        tokio::spawn(observe_inbound(
            inbound,
            Arc::clone(&outstanding_pings),
            inbound_updates_tx,
            stop_tx.clone(),
            conn_id,
        ));
        match &self.plan {
            ScenarioPlan::Named(scenario) => {
                tokio::spawn(heartbeat_loop(
                    tx.clone(),
                    self.default_heartbeat_interval,
                    stop_rx.clone(),
                ));

                match scenario {
                    ScenarioKind::H02SeqIdOverrun => tokio::spawn(h02_overrun_once(
                        tx.clone(),
                        build_packets(6, self.default_cu_per_tx, false, true),
                        self.h02_seq_id,
                        self.default_bundle_interval,
                        stop_rx.clone(),
                    )),
                    _ => tokio::spawn(periodic_bundle_loop(
                        tx.clone(),
                        self.benign_bundle_packets.clone(),
                        self.default_bundle_interval,
                        stop_rx.clone(),
                    )),
                };

                if let Some(ping_interval) = self.default_ping_interval {
                    tokio::spawn(ping_loop(
                        tx.clone(),
                        Arc::clone(&outstanding_pings),
                        ping_interval,
                        stop_rx.clone(),
                    ));
                }
            }
            ScenarioPlan::Scripted(script) => {
                if let Some(heartbeat_interval) = script.heartbeat_interval {
                    tokio::spawn(heartbeat_loop(
                        tx.clone(),
                        heartbeat_interval,
                        stop_rx.clone(),
                    ));
                }

                if let Some(ping_interval) = script.ping_interval {
                    tokio::spawn(ping_loop(
                        tx.clone(),
                        Arc::clone(&outstanding_pings),
                        ping_interval,
                        stop_rx.clone(),
                    ));
                }

                let start_event_idx = if conn_id == 1 || script.replay_on_reconnect {
                    0
                } else if script.resume_on_reconnect {
                    script.runtime.next_event_idx.load(Ordering::SeqCst)
                } else {
                    usize::MAX
                };

                if start_event_idx != usize::MAX {
                    if script.resume_on_reconnect && conn_id != 1 {
                        println!(
                            "scripted scenario conn={} resuming at event {} of {}",
                            conn_id,
                            start_event_idx,
                            script.events.len()
                        );
                    }
                    tokio::spawn(scripted_event_loop(
                        tx.clone(),
                        script.events.clone(),
                        stop_tx.clone(),
                        stop_rx.clone(),
                        inbound_updates_rx.clone(),
                        Arc::clone(&self.config),
                        conn_id,
                        start_event_idx,
                        script
                            .resume_on_reconnect
                            .then(|| Arc::clone(&script.runtime)),
                    ));
                } else {
                    println!(
                        "scripted scenario conn={} replay disabled; not replaying {} events",
                        conn_id,
                        script.events.len()
                    );
                }
            }
        }

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

impl MockBamNode {
    fn plan_name(&self) -> String {
        match &self.plan {
            ScenarioPlan::Named(scenario) => scenario.as_str().to_string(),
            ScenarioPlan::Scripted(script) => {
                format!("scripted:{}", script.source_path.display())
            }
        }
    }
}

fn build_rpc_status(spec: &RpcStatusSpec) -> Status {
    Status::new(spec.code, spec.message.clone())
}

fn next_rpc_status<'a>(
    values: &'a [RpcStatusSpec],
    next: &AtomicUsize,
) -> Option<&'a RpcStatusSpec> {
    let idx = next.fetch_add(1, Ordering::SeqCst);
    values.get(idx)
}

fn next_string_value(values: &[String], next: &AtomicUsize) -> Option<String> {
    let idx = next.fetch_add(1, Ordering::SeqCst);
    values.get(idx).cloned()
}

fn next_u64_value(values: &[u64], next: &AtomicUsize) -> Option<u64> {
    let idx = next.fetch_add(1, Ordering::SeqCst);
    values.get(idx).copied()
}

fn apply_config_variant(config: &mut ConfigResponse, variant: &str) -> Result<()> {
    if !matches!(
        variant,
        "normal" | "bad_builder_commission" | "bad_builder_pubkey" | "missing_bam"
    ) {
        config.block_engine_config = None;
    }
    match variant {
        "normal" => {}
        "empty_config" => {
            config.block_engine_config = None;
            config.bam_config = None;
        }
        "missing_block_engine" => config.block_engine_config = None,
        "missing_bam" => {
            config
                .block_engine_config
                .as_mut()
                .context("base config has no block_engine_config")?
                .builder_commission = 101;
            config.bam_config = None;
        }
        "bad_builder_commission" => {
            config
                .block_engine_config
                .as_mut()
                .context("base config has no block_engine_config")?
                .builder_commission = 101;
        }
        "bad_builder_pubkey" => {
            config
                .block_engine_config
                .as_mut()
                .context("base config has no block_engine_config")?
                .builder_pubkey = "not-a-pubkey".to_string();
        }
        "empty_priority_fee_recipient" => {
            config
                .bam_config
                .as_mut()
                .context("base config has no bam_config")?
                .prio_fee_recipient_pubkey
                .clear();
        }
        "bad_priority_fee_recipient" => {
            config
                .bam_config
                .as_mut()
                .context("base config has no bam_config")?
                .prio_fee_recipient_pubkey = "not-a-pubkey".to_string();
        }
        "missing_tpu_socket" => {
            config
                .bam_config
                .as_mut()
                .context("base config has no bam_config")?
                .tpu_sock = None;
        }
        "bad_tpu_ip" => {
            config
                .bam_config
                .as_mut()
                .context("base config has no bam_config")?
                .tpu_sock
                .as_mut()
                .context("base config has no tpu_sock")?
                .ip = "not-an-ip".to_string();
        }
        "zero_tpu_port" => {
            config
                .bam_config
                .as_mut()
                .context("base config has no bam_config")?
                .tpu_sock
                .as_mut()
                .context("base config has no tpu_sock")?
                .port = 0;
        }
        "oversized_tpu_port" => {
            config
                .bam_config
                .as_mut()
                .context("base config has no bam_config")?
                .tpu_sock
                .as_mut()
                .context("base config has no tpu_sock")?
                .port = u32::from(u16::MAX);
        }
        "missing_tpu_fwd_socket" => {
            config
                .bam_config
                .as_mut()
                .context("base config has no bam_config")?
                .tpu_fwd_sock = None;
        }
        "bad_tpu_fwd_ip" => {
            config
                .bam_config
                .as_mut()
                .context("base config has no bam_config")?
                .tpu_fwd_sock
                .as_mut()
                .context("base config has no tpu_fwd_sock")?
                .ip = "not-an-ip".to_string();
        }
        "zero_tpu_fwd_port" => {
            config
                .bam_config
                .as_mut()
                .context("base config has no bam_config")?
                .tpu_fwd_sock
                .as_mut()
                .context("base config has no tpu_fwd_sock")?
                .port = 0;
        }
        "oversized_tpu_fwd_port" => {
            config
                .bam_config
                .as_mut()
                .context("base config has no bam_config")?
                .tpu_fwd_sock
                .as_mut()
                .context("base config has no tpu_fwd_sock")?
                .port = u32::from(u16::MAX);
        }
        "empty_shred" => {
            config
                .bam_config
                .as_mut()
                .context("base config has no bam_config")?
                .shred_sock
                .clear();
        }
        "duplicate_shred" => {
            let bam = config
                .bam_config
                .as_mut()
                .context("base config has no bam_config")?;
            let first = bam
                .shred_sock
                .first()
                .context("base config has no shred_sock")?
                .clone();
            bam.shred_sock.push(first);
        }
        "bad_shred_ip" => {
            config
                .bam_config
                .as_mut()
                .context("base config has no bam_config")?
                .shred_sock
                .push(Socket {
                    ip: "not-an-ip".to_string(),
                    port: 9009,
                });
        }
        "zero_shred_port" => {
            config
                .bam_config
                .as_mut()
                .context("base config has no bam_config")?
                .shred_sock
                .push(Socket {
                    ip: "127.0.0.2".to_string(),
                    port: 0,
                });
        }
        "oversized_shred_port" => {
            config
                .bam_config
                .as_mut()
                .context("base config has no bam_config")?
                .shred_sock
                .push(Socket {
                    ip: "127.0.0.2".to_string(),
                    port: u32::from(u16::MAX) + 1,
                });
        }
        "shred_overflow" => {
            let bam = config
                .bam_config
                .as_mut()
                .context("base config has no bam_config")?;
            bam.shred_sock = (0..33)
                .map(|idx| Socket {
                    ip: format!("127.0.1.{}", idx + 1),
                    port: 9009 + idx,
                })
                .collect();
        }
        other => bail!("unsupported config variant {other:?}"),
    }
    Ok(())
}

async fn observe_inbound(
    mut inbound: tonic::Streaming<SchedulerMessage>,
    outstanding_pings: Arc<Mutex<HashSet<u32>>>,
    inbound_updates: watch::Sender<InboundCounts>,
    stop_tx: watch::Sender<bool>,
    conn_id: usize,
) {
    let mut inbound_counts = InboundCounts::default();
    while let Ok(Some(msg)) = inbound.message().await {
        if let Some(crate::proto::bam_api::scheduler_message::VersionedMsg::V0(v0)) =
            msg.versioned_msg
        {
            if let Some(msg) = v0.msg {
                match msg {
                    SchedulerMsg::AuthProof(proof) => {
                        inbound_counts.auth_proof += 1;
                        let _ = inbound_updates.send(inbound_counts);
                        println!(
                            "scheduler<-validator auth proof validator_pubkey={} challenge_len={} sig_len={} conn={}",
                            proof.validator_pubkey,
                            proof.challenge_to_sign.len(),
                            proof.signature.len(),
                            conn_id
                        );
                    }
                    SchedulerMsg::HeartBeat(_) => {
                        inbound_counts.heartbeat += 1;
                        let _ = inbound_updates.send(inbound_counts);
                        println!("scheduler<-validator heartbeat conn={conn_id}");
                    }
                    SchedulerMsg::LeaderState(leader_state) => {
                        inbound_counts.leader_state += 1;
                        inbound_counts.last_leader_slot = Some(leader_state.slot);
                        let _ = inbound_updates.send(inbound_counts);
                        println!(
                            "scheduler<-validator leader state slot={} conn={}",
                            leader_state.slot, conn_id
                        );
                    }
                    SchedulerMsg::MultipleAtomicTxnBatchResult(result) => {
                        inbound_counts.batch_result_message += 1;
                        let _ = inbound_updates.send(inbound_counts);
                        println!(
                            "scheduler<-validator batch results count={} conn={}",
                            result.results.len(),
                            conn_id
                        );
                        for batch_result in result.results {
                            inbound_counts.batch_result += 1;
                            match batch_result.result.as_ref() {
                                Some(atomic_txn_batch_result::Result::Committed(_)) => {
                                    inbound_counts.committed_batch += 1;
                                }
                                Some(atomic_txn_batch_result::Result::NotCommitted(_)) => {
                                    inbound_counts.not_committed_batch += 1;
                                }
                                None => {}
                            }
                            let _ = inbound_updates.send(inbound_counts);
                            for line in format_batch_result_lines(&batch_result, conn_id) {
                                println!("{line}");
                            }
                        }
                    }
                    SchedulerMsg::Pong(pong) => {
                        let mut pending = outstanding_pings
                            .lock()
                            .expect("scheduler ping state mutex poisoned");
                        if pending.remove(&pong.id) {
                            inbound_counts.pong += 1;
                            let _ = inbound_updates.send(inbound_counts);
                            println!("validated scheduler pong id={} conn={}", pong.id, conn_id);
                        } else {
                            eprintln!("unexpected scheduler pong id={} conn={}", pong.id, conn_id);
                        }
                    }
                }
            }
        }
    }

    println!("scheduler stream closed by validator conn={conn_id}");
    let _ = stop_tx.send(true);
}

fn format_batch_result_lines(result: &AtomicTxnBatchResult, conn_id: usize) -> Vec<String> {
    match result.result.as_ref() {
        Some(atomic_txn_batch_result::Result::Committed(committed)) => {
            let mut lines = vec![format!(
                "scheduler<-validator batch_result seq_id={} status=committed txns={} conn={}",
                result.seq_id,
                committed.transaction_results.len(),
                conn_id
            )];
            lines.extend(committed.transaction_results.iter().enumerate().map(
                |(tx_index, tx_result)| {
                    format_committed_tx_result(result.seq_id, tx_index, tx_result, conn_id)
                },
            ));
            lines
        }
        Some(atomic_txn_batch_result::Result::NotCommitted(not_committed)) => {
            vec![format_not_committed_result(
                result.seq_id,
                not_committed,
                conn_id,
            )]
        }
        None => vec![format!(
            "scheduler<-validator batch_result seq_id={} status=missing_result conn={}",
            result.seq_id, conn_id
        )],
    }
}

fn format_committed_tx_result(
    seq_id: u32,
    tx_index: usize,
    tx_result: &TransactionCommittedResult,
    conn_id: usize,
) -> String {
    format!(
        concat!(
            "scheduler<-validator batch_result_tx ",
            "seq_id={} tx_index={} execution_success={} ",
            "cus_consumed={} feepayer_balance_lamports={} ",
            "loaded_accounts_data_size={} conn={}"
        ),
        seq_id,
        tx_index,
        tx_result.execution_success,
        tx_result.cus_consumed,
        tx_result.feepayer_balance_lamports,
        tx_result.loaded_accounts_data_size,
        conn_id,
    )
}

fn format_not_committed_result(
    seq_id: u32,
    not_committed: &crate::proto::bam_types::NotCommitted,
    conn_id: usize,
) -> String {
    match not_committed.reason.as_ref() {
        Some(not_committed::Reason::TransactionError(err)) => format!(
            "scheduler<-validator batch_result seq_id={} status=not_committed reason=transaction_error index={} detail={} conn={}",
            seq_id,
            err.index,
            transaction_error_reason_name(err.reason),
            conn_id
        ),
        Some(not_committed::Reason::SchedulingError(err)) => format!(
            "scheduler<-validator batch_result seq_id={} status=not_committed reason=scheduling_error detail={} conn={}",
            seq_id,
            scheduling_error_name(*err),
            conn_id
        ),
        Some(not_committed::Reason::GenericInvalid(err)) => format!(
            "scheduler<-validator batch_result seq_id={} status=not_committed reason=generic_invalid detail={:?} conn={}",
            seq_id,
            err.message,
            conn_id
        ),
        Some(not_committed::Reason::DeserializationError(err)) => format!(
            "scheduler<-validator batch_result seq_id={} status=not_committed reason=deserialization_error index={} detail={} conn={}",
            seq_id,
            err.index,
            deserialization_error_reason_name(err.reason),
            conn_id
        ),
        None => format!(
            "scheduler<-validator batch_result seq_id={} status=not_committed reason=missing conn={}",
            seq_id,
            conn_id
        ),
    }
}

fn transaction_error_reason_name(raw: i32) -> &'static str {
    TransactionErrorReason::try_from(raw)
        .map(|reason| reason.as_str_name())
        .unwrap_or("UNKNOWN_TRANSACTION_ERROR")
}

fn scheduling_error_name(raw: i32) -> &'static str {
    SchedulingError::try_from(raw)
        .map(|reason| reason.as_str_name())
        .unwrap_or("UNKNOWN_SCHEDULING_ERROR")
}

fn deserialization_error_reason_name(raw: i32) -> &'static str {
    DeserializationErrorReason::try_from(raw)
        .map(|reason| reason.as_str_name())
        .unwrap_or("UNKNOWN_DESERIALIZATION_ERROR")
}

async fn heartbeat_loop(
    tx: mpsc::Sender<Result<RawSchedulerResponse, Status>>,
    heartbeat_interval: Duration,
    mut stop_rx: watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(heartbeat_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = interval.tick() => {}
            changed = stop_rx.changed() => {
                if changed.is_ok() && *stop_rx.borrow() {
                    break;
                }
                continue;
            }
        }
        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        if tx
            .send(Ok(scheduler_response(SchedulerResp::HeartBeat(
                BuilderHeartBeat {
                    time_sent_microseconds: now_us,
                },
            ))))
            .await
            .is_err()
        {
            break;
        }
    }
}

async fn periodic_bundle_loop(
    tx: mpsc::Sender<Result<RawSchedulerResponse, Status>>,
    packets: Vec<Packet>,
    bundle_interval: Duration,
    mut stop_rx: watch::Receiver<bool>,
) {
    let mut next_seq_id = 1_u32;
    let mut interval = tokio::time::interval(bundle_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = interval.tick() => {}
            changed = stop_rx.changed() => {
                if changed.is_ok() && *stop_rx.borrow() {
                    break;
                }
                continue;
            }
        }
        let seq_id = next_seq_id;
        next_seq_id = next_non_terminal_seq_id(next_seq_id);

        let response = scheduler_response(SchedulerResp::MultipleAtomicTxnBatch(
            MultipleAtomicTxnBatch {
                batches: vec![AtomicTxnBatch {
                    seq_id,
                    max_schedule_slot: u64::MAX,
                    packets: packets.clone(),
                }],
            },
        ));

        println!(
            "sending benign bundle seq_id={} packets={}",
            seq_id,
            packets.len()
        );

        if tx.send(Ok(response)).await.is_err() {
            break;
        }
    }
}

async fn h02_overrun_once(
    tx: mpsc::Sender<Result<RawSchedulerResponse, Status>>,
    packets: Vec<Packet>,
    seq_id: u32,
    delay: Duration,
    mut stop_rx: watch::Receiver<bool>,
) {
    tokio::select! {
        _ = tokio::time::sleep(delay) => {}
        changed = stop_rx.changed() => {
            if changed.is_ok() && *stop_rx.borrow() {
                return;
            }
        }
    }

    let first = packets.iter().take(5).cloned().collect::<Vec<_>>();
    let second = packets.iter().skip(5).take(1).cloned().collect::<Vec<_>>();

    println!(
        "sending H02 duplicate seq_id={} batches=[{} packets, {} packets]",
        seq_id,
        first.len(),
        second.len()
    );

    let response = scheduler_response(SchedulerResp::MultipleAtomicTxnBatch(
        MultipleAtomicTxnBatch {
            batches: vec![
                AtomicTxnBatch {
                    seq_id,
                    max_schedule_slot: u64::MAX,
                    packets: first,
                },
                AtomicTxnBatch {
                    seq_id,
                    max_schedule_slot: u64::MAX,
                    packets: second,
                },
            ],
        },
    ));

    let _ = tx.send(Ok(response)).await;
}

async fn ping_loop(
    tx: mpsc::Sender<Result<RawSchedulerResponse, Status>>,
    outstanding_pings: Arc<Mutex<HashSet<u32>>>,
    ping_interval: Duration,
    mut stop_rx: watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(ping_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut next_ping_id = 1_u32;

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            changed = stop_rx.changed() => {
                if changed.is_ok() && *stop_rx.borrow() {
                    break;
                }
                continue;
            }
        }
        let ping_id = next_ping_id;
        next_ping_id = next_non_terminal_seq_id(next_ping_id);

        {
            let mut pending = outstanding_pings
                .lock()
                .expect("scheduler ping state mutex poisoned");
            pending.insert(ping_id);
        }

        if tx
            .send(Ok(scheduler_response(SchedulerResp::Ping(Ping {
                id: ping_id,
            }))))
            .await
            .is_err()
        {
            let mut pending = outstanding_pings
                .lock()
                .expect("scheduler ping state mutex poisoned");
            pending.remove(&ping_id);
            break;
        }

        println!("sent scheduler ping id={ping_id}");
    }
}

fn next_script_seq_id(runtime: Option<&ScriptedRuntime>, next_seq_id: &mut u32) -> u32 {
    if let Some(runtime) = runtime {
        runtime
            .next_seq_id
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                Some(next_non_terminal_seq_id(current))
            })
            .expect("scripted next_seq_id fetch_update should succeed")
    } else {
        let seq_id = *next_seq_id;
        *next_seq_id = next_non_terminal_seq_id(*next_seq_id);
        seq_id
    }
}

fn next_script_ping_id(runtime: Option<&ScriptedRuntime>, next_ping_id: &mut u32) -> u32 {
    if let Some(runtime) = runtime {
        runtime
            .next_ping_id
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                Some(next_non_terminal_seq_id(current))
            })
            .expect("scripted next_ping_id fetch_update should succeed")
    } else {
        let ping_id = *next_ping_id;
        *next_ping_id = next_non_terminal_seq_id(*next_ping_id);
        ping_id
    }
}

fn mark_script_event_complete(runtime: Option<&ScriptedRuntime>, event_idx: usize) {
    if let Some(runtime) = runtime {
        runtime
            .next_event_idx
            .store(event_idx + 1, Ordering::SeqCst);
    }
}

fn validate_pubkey_string(field: &str, value: &str) -> bool {
    if value.parse::<solana_pubkey::Pubkey>().is_ok() {
        true
    } else {
        eprintln!("scripted config_update ignored invalid {field}: {value:?}");
        false
    }
}

fn apply_config_update(
    config: &Arc<Mutex<ConfigResponse>>,
    spec: ConfigUpdateSpec,
    conn_id: usize,
) {
    let mut cfg = config.lock().expect("BAM config mutex poisoned");

    if let Some(include) = spec.include_block_engine_config {
        if include {
            if cfg.block_engine_config.is_none() {
                let builder_pubkey = spec
                    .builder_pubkey
                    .as_ref()
                    .filter(|value| validate_pubkey_string("builder_pubkey", value))
                    .cloned()
                    .or_else(|| {
                        cfg.bam_config
                            .as_ref()
                            .map(|bam| bam.prio_fee_recipient_pubkey.clone())
                    })
                    .unwrap_or_else(|| solana_pubkey::Pubkey::new_unique().to_string());
                cfg.block_engine_config = Some(BlockEngineBuilderConfig {
                    builder_pubkey,
                    builder_commission: spec.builder_commission_pct.unwrap_or(0),
                });
            }
        } else {
            cfg.block_engine_config = None;
        }
    }

    if let Some(include) = spec.include_bam_config {
        if !include {
            cfg.bam_config = None;
        } else if cfg.bam_config.is_none() {
            eprintln!("scripted config_update cannot synthesize BamConfig sockets after removal");
        }
    }

    if let Some(builder_pubkey) = spec.builder_pubkey.as_ref() {
        if validate_pubkey_string("builder_pubkey", builder_pubkey) {
            if let Some(block_engine_config) = cfg.block_engine_config.as_mut() {
                block_engine_config.builder_pubkey = builder_pubkey.clone();
            } else {
                eprintln!("scripted config_update builder_pubkey ignored because block_engine_config is omitted");
            }
        }
    }

    if let Some(builder_commission) = spec.builder_commission_pct {
        if let Some(block_engine_config) = cfg.block_engine_config.as_mut() {
            block_engine_config.builder_commission = builder_commission;
        } else {
            eprintln!("scripted config_update builder_commission_pct ignored because block_engine_config is omitted");
        }
    }

    if let Some(prio_fee_recipient) = spec.prio_fee_recipient_pubkey.as_ref() {
        if validate_pubkey_string("prio_fee_recipient_pubkey", prio_fee_recipient) {
            if let Some(bam_config) = cfg.bam_config.as_mut() {
                bam_config.prio_fee_recipient_pubkey = prio_fee_recipient.clone();
            } else {
                eprintln!("scripted config_update prio_fee_recipient_pubkey ignored because bam_config is omitted");
            }
        }
    }

    if let Some(commission_bps) = spec.commission_bps {
        if let Some(bam_config) = cfg.bam_config.as_mut() {
            bam_config.commission_bps = commission_bps;
        } else {
            eprintln!(
                "scripted config_update commission_bps ignored because bam_config is omitted"
            );
        }
    }

    println!(
        "scripted config_update conn={} {}",
        conn_id,
        format_config_summary(&cfg),
    );
}

async fn latest_leader_slot_or_wait(
    inbound_updates: &mut watch::Receiver<InboundCounts>,
    stop_rx: &mut watch::Receiver<bool>,
    conn_id: usize,
) -> Option<u64> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(slot) = inbound_updates.borrow().last_leader_slot {
            return Some(slot);
        }
        if *stop_rx.borrow() {
            return None;
        }
        let now = Instant::now();
        if now >= deadline {
            eprintln!("scripted dynamic leader slot timed out conn={conn_id}");
            return None;
        }
        tokio::select! {
            changed = inbound_updates.changed() => {
                if changed.is_err() {
                    return None;
                }
            }
            changed = stop_rx.changed() => {
                if changed.is_err() || *stop_rx.borrow() {
                    return None;
                }
            }
            _ = tokio::time::sleep(deadline.saturating_duration_since(now)) => {
                eprintln!("scripted dynamic leader slot timed out conn={conn_id}");
                return None;
            }
        }
    }
}

async fn resolve_script_schedule_slot(
    raw_slot: u64,
    inbound_updates: &mut watch::Receiver<InboundCounts>,
    stop_rx: &mut watch::Receiver<bool>,
    conn_id: usize,
) -> Option<u64> {
    if !(SCHEDULE_SLOT_LATEST_LEADER_BASE..u64::MAX).contains(&raw_slot) {
        return Some(raw_slot);
    }
    let offset = raw_slot - SCHEDULE_SLOT_LATEST_LEADER_BASE;
    latest_leader_slot_or_wait(inbound_updates, stop_rx, conn_id)
        .await
        .map(|slot| slot.saturating_add(offset))
}

async fn scripted_event_loop(
    tx: mpsc::Sender<Result<RawSchedulerResponse, Status>>,
    events: Vec<ScriptedEvent>,
    stop_tx: watch::Sender<bool>,
    mut stop_rx: watch::Receiver<bool>,
    mut inbound_updates: watch::Receiver<InboundCounts>,
    config: Arc<Mutex<ConfigResponse>>,
    conn_id: usize,
    start_event_idx: usize,
    runtime: Option<Arc<ScriptedRuntime>>,
) {
    let mut next_seq_id = 1_u32;
    let mut next_ping_id = 1_u32;

    for (event_idx, event) in events.into_iter().enumerate().skip(start_event_idx) {
        let runtime_ref = runtime.as_deref();
        if *stop_rx.borrow() {
            break;
        }
        match event {
            ScriptedEvent::Sleep { ms } => {
                if !sleep_or_stop(&mut stop_rx, Duration::from_millis(ms)).await {
                    break;
                }
                mark_script_event_complete(runtime_ref, event_idx);
            }
            ScriptedEvent::SleepResumeSafe { ms } => {
                println!("scripted sleep_resume_safe ms={ms}");
                mark_script_event_complete(runtime_ref, event_idx);
                if !sleep_or_stop(&mut stop_rx, Duration::from_millis(ms)).await {
                    break;
                }
            }
            ScriptedEvent::SendBatch(spec) => {
                let seq_id = spec
                    .seq_id
                    .unwrap_or_else(|| next_script_seq_id(runtime_ref, &mut next_seq_id));
                let Some(max_schedule_slot) = resolve_script_schedule_slot(
                    spec.max_schedule_slot,
                    &mut inbound_updates,
                    &mut stop_rx,
                    conn_id,
                )
                .await
                else {
                    break;
                };

                println!(
                    "scripted send_batch seq_id={} packets={} max_schedule_slot={} expect_result={}",
                    seq_id,
                    spec.packets.len(),
                    max_schedule_slot,
                    spec.expect_result
                );

                let response = scheduler_response(SchedulerResp::MultipleAtomicTxnBatch(
                    MultipleAtomicTxnBatch {
                        batches: vec![AtomicTxnBatch {
                            seq_id,
                            max_schedule_slot,
                            packets: spec.packets,
                        }],
                    },
                ));

                if tx.send(Ok(response)).await.is_err() {
                    break;
                }
                mark_script_event_complete(runtime_ref, event_idx);
            }
            ScriptedEvent::SendBatchFlood(spec) => {
                let Some(max_schedule_slot) = resolve_script_schedule_slot(
                    spec.batch.max_schedule_slot,
                    &mut inbound_updates,
                    &mut stop_rx,
                    conn_id,
                )
                .await
                else {
                    break;
                };

                println!(
                    "scripted send_batch_flood batch_count={} start_seq_id={} packets_per_batch={} max_schedule_slot={}",
                    spec.batch_count,
                    spec.start_seq_id,
                    spec.batch.packets.len(),
                    max_schedule_slot
                );

                let batches = (0..spec.batch_count)
                    .map(|offset| AtomicTxnBatch {
                        seq_id: spec.start_seq_id.wrapping_add(offset as u32),
                        max_schedule_slot,
                        packets: spec.batch.packets.clone(),
                    })
                    .collect();
                let response = scheduler_response(SchedulerResp::MultipleAtomicTxnBatch(
                    MultipleAtomicTxnBatch { batches },
                ));

                if tx.send(Ok(response)).await.is_err() {
                    break;
                }
                mark_script_event_complete(runtime_ref, event_idx);
            }
            ScriptedEvent::SendMultiBatch(specs) => {
                let mut seq_ids = Vec::with_capacity(specs.len());
                let mut packet_counts = Vec::with_capacity(specs.len());
                let mut batches = Vec::with_capacity(specs.len());
                for spec in specs {
                    let seq_id = spec
                        .seq_id
                        .unwrap_or_else(|| next_script_seq_id(runtime_ref, &mut next_seq_id));
                    let Some(max_schedule_slot) = resolve_script_schedule_slot(
                        spec.max_schedule_slot,
                        &mut inbound_updates,
                        &mut stop_rx,
                        conn_id,
                    )
                    .await
                    else {
                        return;
                    };
                    seq_ids.push(seq_id);
                    packet_counts.push(spec.packets.len());
                    batches.push(AtomicTxnBatch {
                        seq_id,
                        max_schedule_slot,
                        packets: spec.packets,
                    });
                }

                println!(
                    "scripted send_multi_batch seq_ids={:?} packet_counts={:?}",
                    seq_ids, packet_counts
                );

                let response = scheduler_response(SchedulerResp::MultipleAtomicTxnBatch(
                    MultipleAtomicTxnBatch { batches },
                ));

                if tx.send(Ok(response)).await.is_err() {
                    break;
                }
                mark_script_event_complete(runtime_ref, event_idx);
            }
            ScriptedEvent::SendSplitBatch(spec) => {
                let Some(max_schedule_slot) = resolve_script_schedule_slot(
                    spec.max_schedule_slot,
                    &mut inbound_updates,
                    &mut stop_rx,
                    conn_id,
                )
                .await
                else {
                    break;
                };
                println!(
                    "scripted send_split_batch seq_id={} splits={:?} max_schedule_slot={}",
                    spec.seq_id,
                    spec.packet_slices
                        .iter()
                        .map(|slice| slice.len())
                        .collect::<Vec<_>>(),
                    max_schedule_slot
                );

                let response = scheduler_response(SchedulerResp::MultipleAtomicTxnBatch(
                    MultipleAtomicTxnBatch {
                        batches: spec
                            .packet_slices
                            .into_iter()
                            .map(|packets| AtomicTxnBatch {
                                seq_id: spec.seq_id,
                                max_schedule_slot,
                                packets,
                            })
                            .collect(),
                    },
                ));

                if tx.send(Ok(response)).await.is_err() {
                    break;
                }
                mark_script_event_complete(runtime_ref, event_idx);
            }
            ScriptedEvent::SendPing { id } => {
                let ping_id =
                    id.unwrap_or_else(|| next_script_ping_id(runtime_ref, &mut next_ping_id));

                println!("scripted send_ping id={ping_id}");
                if tx
                    .send(Ok(scheduler_response(SchedulerResp::Ping(Ping {
                        id: ping_id,
                    }))))
                    .await
                    .is_err()
                {
                    break;
                }
                mark_script_event_complete(runtime_ref, event_idx);
            }
            ScriptedEvent::SendHeartbeat {
                time_sent_microseconds,
            } => {
                println!("scripted send_heartbeat time_sent_microseconds={time_sent_microseconds}");
                if tx
                    .send(Ok(scheduler_response(SchedulerResp::HeartBeat(
                        BuilderHeartBeat {
                            time_sent_microseconds,
                        },
                    ))))
                    .await
                    .is_err()
                {
                    break;
                }
                mark_script_event_complete(runtime_ref, event_idx);
            }
            ScriptedEvent::SendEmptyEnvelope => {
                println!("scripted send_empty_envelope");
                if tx
                    .send(Ok(RawSchedulerResponse::from_message(SchedulerResponse {
                        versioned_msg: None,
                    })))
                    .await
                    .is_err()
                {
                    break;
                }
                mark_script_event_complete(runtime_ref, event_idx);
            }
            ScriptedEvent::SendEmptyV0 => {
                println!("scripted send_empty_v0");
                if tx
                    .send(Ok(RawSchedulerResponse::from_message(SchedulerResponse {
                        versioned_msg: Some(SchedulerRespVersioned::V0(SchedulerResponseV0 {
                            resp: None,
                        })),
                    })))
                    .await
                    .is_err()
                {
                    break;
                }
                mark_script_event_complete(runtime_ref, event_idx);
            }
            ScriptedEvent::SendUnsupportedVersion => {
                println!("scripted send_unsupported_version");
                if tx
                    .send(Ok(RawSchedulerResponse::from_message(SchedulerResponse {
                        versioned_msg: Some(SchedulerRespVersioned::UnsupportedVersion(vec![
                            0x08, 0x01,
                        ])),
                    })))
                    .await
                    .is_err()
                {
                    break;
                }
                mark_script_event_complete(runtime_ref, event_idx);
            }
            ScriptedEvent::SendUnsupportedV0 => {
                println!("scripted send_unsupported_v0");
                if tx
                    .send(Ok(RawSchedulerResponse::from_message(SchedulerResponse {
                        versioned_msg: Some(SchedulerRespVersioned::V0(SchedulerResponseV0 {
                            resp: Some(SchedulerResp::UnsupportedResponse(vec![0x08, 0x01])),
                        })),
                    })))
                    .await
                    .is_err()
                {
                    break;
                }
                mark_script_event_complete(runtime_ref, event_idx);
            }
            ScriptedEvent::SendRawResponse { name, bytes } => {
                println!(
                    "scripted send_raw_response name={name:?} bytes={} hex={}",
                    bytes.len(),
                    encode_hex(&bytes)
                );
                if tx
                    .send(Ok(RawSchedulerResponse::from_bytes(bytes)))
                    .await
                    .is_err()
                {
                    break;
                }
                mark_script_event_complete(runtime_ref, event_idx);
            }
            ScriptedEvent::SendRawOverflowFaults { start_seq_id } => {
                let expected_result_count = 257_u32;
                println!(
                    "scripted send_raw_overflow_faults start_seq_id={} expected_result_count={}",
                    start_seq_id, expected_result_count
                );
                let first = raw_overflow_response(start_seq_id, false);
                let second = raw_overflow_response(start_seq_id.wrapping_add(128), true);
                if tx.send(Ok(first)).await.is_err() || tx.send(Ok(second)).await.is_err() {
                    break;
                }
                mark_script_event_complete(runtime_ref, event_idx);
            }
            ScriptedEvent::SendEmptyMultiBatch => {
                println!("scripted send_empty_multi_batch");
                if tx
                    .send(Ok(scheduler_response(
                        SchedulerResp::MultipleAtomicTxnBatch(MultipleAtomicTxnBatch {
                            batches: Vec::new(),
                        }),
                    )))
                    .await
                    .is_err()
                {
                    break;
                }
                mark_script_event_complete(runtime_ref, event_idx);
            }
            ScriptedEvent::SendStatus { code, message } => {
                println!("scripted send_status code={code:?} message={message:?}");
                mark_script_event_complete(runtime_ref, event_idx);
                let _ = tx.send(Err(Status::new(code, message))).await;
                let _ = stop_tx.send(true);
                break;
            }
            ScriptedEvent::ConfigUpdate(spec) => {
                apply_config_update(&config, spec, conn_id);
                mark_script_event_complete(runtime_ref, event_idx);
            }
            ScriptedEvent::WaitInbound {
                kind,
                min_count,
                timeout_ms,
            } => {
                println!(
                    "scripted wait_inbound conn={} kind={} min_count={} timeout_ms={}",
                    conn_id,
                    kind.as_str(),
                    min_count,
                    timeout_ms
                );
                if !wait_for_inbound_count(
                    &mut inbound_updates,
                    &mut stop_rx,
                    kind,
                    min_count,
                    Duration::from_millis(timeout_ms),
                )
                .await
                {
                    if *stop_rx.borrow() {
                        break;
                    }
                    let observed = inbound_updates.borrow().count(kind);
                    eprintln!(
                        "scripted wait_inbound timeout conn={} kind={} observed={} expected_at_least={}",
                        conn_id,
                        kind.as_str(),
                        observed,
                        min_count
                    );
                    break;
                }
                let observed = inbound_updates.borrow().count(kind);
                println!(
                    "scripted wait_inbound satisfied conn={} kind={} observed={}",
                    conn_id,
                    kind.as_str(),
                    observed
                );
                mark_script_event_complete(runtime_ref, event_idx);
            }
            ScriptedEvent::CloseStream => {
                println!("scripted close_stream");
                mark_script_event_complete(runtime_ref, event_idx);
                let _ = stop_tx.send(true);
                break;
            }
        }
    }
}

async fn sleep_or_stop(stop_rx: &mut watch::Receiver<bool>, duration: Duration) -> bool {
    if *stop_rx.borrow() {
        return false;
    }

    tokio::select! {
        _ = tokio::time::sleep(duration) => true,
        changed = stop_rx.changed() => {
            changed.is_ok() && !*stop_rx.borrow()
        }
    }
}

async fn wait_for_inbound_count(
    inbound_updates: &mut watch::Receiver<InboundCounts>,
    stop_rx: &mut watch::Receiver<bool>,
    kind: WaitInboundKind,
    min_count: u64,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let observed = inbound_updates.borrow().count(kind);
        if observed >= min_count {
            return true;
        }
        if *stop_rx.borrow() {
            return false;
        }
        let now = Instant::now();
        if now >= deadline {
            return false;
        }
        let remaining = deadline.saturating_duration_since(now);
        tokio::select! {
            _ = tokio::time::sleep(remaining) => return false,
            changed = inbound_updates.changed() => {
                if changed.is_err() {
                    return inbound_updates.borrow().count(kind) >= min_count;
                }
            }
            changed = stop_rx.changed() => {
                if changed.is_err() {
                    return false;
                }
                if *stop_rx.borrow() {
                    return false;
                }
            }
        }
    }
}

fn scheduler_response(resp: SchedulerResp) -> RawSchedulerResponse {
    RawSchedulerResponse::from_message(SchedulerResponse {
        versioned_msg: Some(SchedulerRespVersioned::V0(SchedulerResponseV0 {
            resp: Some(resp),
        })),
    })
}

fn build_packets(
    tx_count: u64,
    cu_per_tx: u32,
    simple_vote_tx: bool,
    revert_on_error: bool,
) -> Vec<Packet> {
    let keypair = Keypair::new();
    let blockhash = Hash::new_from_array(keypair.pubkey().to_bytes());
    let mut packets = Vec::with_capacity(tx_count as usize);

    for idx in 0..tx_count {
        let tx = Transaction::new_signed_with_payer(
            &[
                ComputeBudgetInstruction::set_compute_unit_limit(cu_per_tx),
                transfer(&keypair.pubkey(), &keypair.pubkey(), idx + 1),
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
                    simple_vote_tx,
                    revert_on_error,
                }),
            }),
        });
    }

    packets
}

fn build_config(args: &Args, builder_pubkey: String) -> ConfigResponse {
    let block_engine_config = if args.omit_block_engine_config {
        None
    } else {
        Some(BlockEngineBuilderConfig {
            builder_pubkey: builder_pubkey.clone(),
            builder_commission: args.builder_commission_pct,
        })
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
        commission_bps: args.commission_bps,
        prio_fee_recipient_pubkey: builder_pubkey,
    };

    ConfigResponse {
        block_engine_config,
        bam_config: Some(bam_config),
    }
}

fn format_config_summary(config: &ConfigResponse) -> String {
    let block_engine = config
        .block_engine_config
        .as_ref()
        .map(|cfg| {
            format!(
                "block_engine=present builder_pubkey={} builder_commission={}",
                cfg.builder_pubkey, cfg.builder_commission
            )
        })
        .unwrap_or_else(|| "block_engine=omitted".to_string());
    let bam = config
        .bam_config
        .as_ref()
        .map(|cfg| {
            format!(
                "bam=present prio_fee_recipient_pubkey={} commission_bps={} tpu={} fwd={} shred={}",
                cfg.prio_fee_recipient_pubkey,
                cfg.commission_bps,
                format_socket(cfg.tpu_sock.as_ref()),
                format_socket(cfg.tpu_fwd_sock.as_ref()),
                format_socket_list(&cfg.shred_sock),
            )
        })
        .unwrap_or_else(|| "bam=omitted".to_string());
    format!("{block_engine} {bam}")
}

fn target_packet_count(target_cus: u64, cu_per_tx: u32) -> u64 {
    let per_tx = u64::from(cu_per_tx.max(1));
    ((target_cus + per_tx - 1) / per_tx).max(1)
}

fn next_non_terminal_seq_id(current: u32) -> u32 {
    let next = current.wrapping_add(1);
    if next == u32::MAX {
        1
    } else {
        next
    }
}

fn load_scripted_scenario(path: &Path, default_cu_per_tx: u32) -> Result<ScriptedScenario> {
    let source = fs::read_to_string(path)
        .with_context(|| format!("failed to read scenario file {}", path.display()))?;
    let ext = path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or_default();
    let def: ScriptedScenarioDef = match ext {
        "json" => serde_json::from_str(&source)
            .with_context(|| format!("failed to parse JSON scenario {}", path.display()))?,
        "toml" | "tml" | "" => toml::from_str(&source)
            .with_context(|| format!("failed to parse TOML scenario {}", path.display()))?,
        other => bail!(
            "unsupported scenario file extension .{other} for {}",
            path.display()
        ),
    };

    let resume_on_reconnect = def.resume_on_reconnect.unwrap_or(false);
    let replay_on_reconnect = def.replay_on_reconnect.unwrap_or(!resume_on_reconnect);
    if replay_on_reconnect && resume_on_reconnect {
        bail!(
            "scenario {} cannot enable both replay_on_reconnect and resume_on_reconnect",
            path.display()
        );
    }
    let auth_status_sequence = build_rpc_status_specs(def.auth_status_sequence)?;
    let config_status_sequence = build_rpc_status_specs(def.config_status_sequence)?;
    let stream_status_sequence = build_rpc_status_specs(def.stream_status_sequence)?;

    Ok(ScriptedScenario {
        source_path: path.to_path_buf(),
        description: def.description,
        heartbeat_interval: def.heartbeat_interval_ms.map(Duration::from_millis),
        ping_interval: def.ping_interval_ms.map(Duration::from_millis),
        replay_on_reconnect,
        resume_on_reconnect,
        auth_status_sequence,
        auth_delay_ms_sequence: def.auth_delay_ms_sequence,
        auth_challenge_sequence: def.auth_challenge_sequence,
        auth_raw_hex_sequence: def.auth_raw_hex_sequence,
        config_status_sequence,
        config_delay_ms_sequence: def.config_delay_ms_sequence,
        config_variant_sequence: def.config_variant_sequence,
        config_raw_hex_sequence: def.config_raw_hex_sequence,
        stream_status_sequence,
        stream_delay_ms_sequence: def.stream_delay_ms_sequence,
        runtime: Arc::new(ScriptedRuntime {
            next_event_idx: AtomicUsize::new(0),
            next_seq_id: AtomicU32::new(1),
            next_ping_id: AtomicU32::new(1),
            next_auth_status: AtomicUsize::new(0),
            next_auth_delay: AtomicUsize::new(0),
            next_auth_challenge: AtomicUsize::new(0),
            next_auth_raw_hex: AtomicUsize::new(0),
            next_config_status: AtomicUsize::new(0),
            next_config_delay: AtomicUsize::new(0),
            next_config_variant: AtomicUsize::new(0),
            next_config_raw_hex: AtomicUsize::new(0),
            next_stream_status: AtomicUsize::new(0),
            next_stream_delay: AtomicUsize::new(0),
        }),
        events: def
            .events
            .into_iter()
            .map(|event| build_scripted_event(event, default_cu_per_tx))
            .collect::<Result<Vec<_>>>()?,
    })
}

fn build_scripted_event(event: ScriptedEventDef, default_cu_per_tx: u32) -> Result<ScriptedEvent> {
    match event {
        ScriptedEventDef::Sleep { ms } => Ok(ScriptedEvent::Sleep { ms }),
        ScriptedEventDef::SleepResumeSafe { ms } => Ok(ScriptedEvent::SleepResumeSafe { ms }),
        ScriptedEventDef::SendBatch {
            seq_id,
            expect_result,
            packet_count,
            packets_base64,
            packets_base64_file,
            max_schedule_slot,
            simple_vote_tx,
            revert_on_error,
            revert_on_error_sequence,
            packet_meta_sequence,
            packet_data_size,
            cu_per_tx,
        } => Ok(ScriptedEvent::SendBatch(BatchSpec {
            seq_id,
            expect_result: expect_result.unwrap_or(true),
            packets: build_script_packets(
                packet_count,
                packets_base64,
                packets_base64_file,
                cu_per_tx.unwrap_or(default_cu_per_tx),
                simple_vote_tx.unwrap_or(false),
                revert_on_error.unwrap_or(true),
                revert_on_error_sequence,
                packet_meta_sequence,
                packet_data_size,
            )?,
            max_schedule_slot: parse_schedule_slot(max_schedule_slot)?,
        })),
        ScriptedEventDef::SendBatchFlood {
            batch_count,
            start_seq_id,
            packet_count,
            packets_base64,
            packets_base64_file,
            max_schedule_slot,
            simple_vote_tx,
            revert_on_error,
            revert_on_error_sequence,
            packet_meta_sequence,
            packet_data_size,
            cu_per_tx,
        } => {
            if !(1..=4096).contains(&batch_count) {
                bail!("send_batch_flood batch_count must be in 1..=4096");
            }
            Ok(ScriptedEvent::SendBatchFlood(BatchFloodSpec {
                batch_count,
                start_seq_id: start_seq_id.unwrap_or(0),
                batch: BatchSpec {
                    seq_id: None,
                    expect_result: true,
                    packets: build_script_packets(
                        packet_count,
                        packets_base64,
                        packets_base64_file,
                        cu_per_tx.unwrap_or(default_cu_per_tx),
                        simple_vote_tx.unwrap_or(false),
                        revert_on_error.unwrap_or(true),
                        revert_on_error_sequence,
                        packet_meta_sequence,
                        packet_data_size,
                    )?,
                    max_schedule_slot: parse_schedule_slot(max_schedule_slot)?,
                },
            }))
        }
        ScriptedEventDef::SendMultiBatch { batches } => Ok(ScriptedEvent::SendMultiBatch(
            batches
                .into_iter()
                .map(|batch| build_batch_spec(batch, default_cu_per_tx))
                .collect::<Result<Vec<_>>>()?,
        )),
        ScriptedEventDef::SendSplitBatch {
            seq_id,
            splits,
            packet_count,
            packets_base64,
            packets_base64_file,
            max_schedule_slot,
            simple_vote_tx,
            revert_on_error,
            revert_on_error_sequence,
            packet_meta_sequence,
            packet_data_size,
            cu_per_tx,
        } => {
            let packets = build_script_packets(
                packet_count,
                packets_base64,
                packets_base64_file,
                cu_per_tx.unwrap_or(default_cu_per_tx),
                simple_vote_tx.unwrap_or(false),
                revert_on_error.unwrap_or(true),
                revert_on_error_sequence,
                packet_meta_sequence,
                packet_data_size,
            )?;
            let split_total = splits.iter().copied().sum::<u64>() as usize;
            if split_total != packets.len() {
                bail!(
                    "split packet count mismatch: splits total {} but {} packets were built",
                    split_total,
                    packets.len()
                );
            }

            let mut offset = 0usize;
            let packet_slices = splits
                .into_iter()
                .map(|len| {
                    let len = len as usize;
                    let end = offset + len;
                    let slice = packets[offset..end].to_vec();
                    offset = end;
                    slice
                })
                .collect::<Vec<_>>();

            Ok(ScriptedEvent::SendSplitBatch(SplitBatchSpec {
                seq_id,
                packet_slices,
                max_schedule_slot: parse_schedule_slot(max_schedule_slot)?,
            }))
        }
        ScriptedEventDef::SendPing { id } => Ok(ScriptedEvent::SendPing { id }),
        ScriptedEventDef::SendHeartbeat {
            time_sent_microseconds,
        } => Ok(ScriptedEvent::SendHeartbeat {
            time_sent_microseconds: time_sent_microseconds.unwrap_or(0),
        }),
        ScriptedEventDef::SendEmptyEnvelope => Ok(ScriptedEvent::SendEmptyEnvelope),
        ScriptedEventDef::SendEmptyV0 => Ok(ScriptedEvent::SendEmptyV0),
        ScriptedEventDef::SendUnsupportedVersion => Ok(ScriptedEvent::SendUnsupportedVersion),
        ScriptedEventDef::SendUnsupportedV0 => Ok(ScriptedEvent::SendUnsupportedV0),
        ScriptedEventDef::SendEmptyMultiBatch => Ok(ScriptedEvent::SendEmptyMultiBatch),
        ScriptedEventDef::SendRawResponse { name, hex } => Ok(ScriptedEvent::SendRawResponse {
            name,
            bytes: decode_hex(&hex)?,
        }),
        ScriptedEventDef::SendRawOverflowFaults { start_seq_id } => {
            Ok(ScriptedEvent::SendRawOverflowFaults {
                start_seq_id: start_seq_id.unwrap_or(5600),
            })
        }
        ScriptedEventDef::SendStatus { code, message } => Ok(ScriptedEvent::SendStatus {
            code: parse_status_code(&code)?,
            message: message.unwrap_or_else(|| format!("scripted {code}")),
        }),
        ScriptedEventDef::ConfigUpdate {
            builder_pubkey,
            builder_commission_pct,
            prio_fee_recipient_pubkey,
            commission_bps,
            include_block_engine_config,
            include_bam_config,
        } => Ok(ScriptedEvent::ConfigUpdate(ConfigUpdateSpec {
            builder_pubkey,
            builder_commission_pct,
            prio_fee_recipient_pubkey,
            commission_bps,
            include_block_engine_config,
            include_bam_config,
        })),
        ScriptedEventDef::WaitInbound {
            kind,
            min_count,
            timeout_ms,
        } => Ok(ScriptedEvent::WaitInbound {
            kind,
            min_count: min_count.unwrap_or(1),
            timeout_ms,
        }),
        ScriptedEventDef::CloseStream => Ok(ScriptedEvent::CloseStream),
    }
}

fn decode_hex(value: &str) -> Result<Vec<u8>> {
    let compact = value
        .chars()
        .filter(|ch| !ch.is_ascii_whitespace() && *ch != '_')
        .collect::<String>();
    if compact.len() % 2 != 0 {
        bail!("raw response hex must contain an even number of digits");
    }
    (0..compact.len())
        .step_by(2)
        .map(|idx| {
            u8::from_str_radix(&compact[idx..idx + 2], 16)
                .with_context(|| format!("invalid raw response hex at byte {}", idx / 2))
        })
        .collect()
}

fn encode_hex(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn append_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        out.push(byte | if value == 0 { 0 } else { 0x80 });
        if value == 0 {
            break;
        }
    }
}

fn append_len_field(out: &mut Vec<u8>, tag: u8, payload: &[u8]) {
    out.push((tag << 3) | 2);
    append_varint(out, payload.len() as u64);
    out.extend_from_slice(payload);
}

fn raw_overflow_response(start_seq_id: u32, attribute_malformed: bool) -> RawSchedulerResponse {
    let mut multi = Vec::new();
    for offset in 0..128_u32 {
        let mut batch = vec![0x08];
        append_varint(&mut batch, u64::from(start_seq_id.wrapping_add(offset)));
        append_len_field(&mut multi, 3, &batch);
    }

    let mut malformed = Vec::new();
    if attribute_malformed {
        malformed.push(0x08);
        append_varint(&mut malformed, u64::from(start_seq_id.wrapping_add(128)));
    }
    malformed.push(0x00);
    append_len_field(&mut multi, 3, &malformed);

    let mut v0 = Vec::new();
    append_len_field(&mut v0, 2, &multi);
    let mut response = Vec::new();
    append_len_field(&mut response, 1, &v0);
    RawSchedulerResponse::from_bytes(response)
}

fn parse_status_code(value: &str) -> Result<Code> {
    match value {
        "cancelled" => Ok(Code::Cancelled),
        "unknown" => Ok(Code::Unknown),
        "invalid_argument" => Ok(Code::InvalidArgument),
        "deadline_exceeded" => Ok(Code::DeadlineExceeded),
        "not_found" => Ok(Code::NotFound),
        "already_exists" => Ok(Code::AlreadyExists),
        "permission_denied" => Ok(Code::PermissionDenied),
        "resource_exhausted" => Ok(Code::ResourceExhausted),
        "failed_precondition" => Ok(Code::FailedPrecondition),
        "aborted" => Ok(Code::Aborted),
        "out_of_range" => Ok(Code::OutOfRange),
        "unimplemented" => Ok(Code::Unimplemented),
        "internal" => Ok(Code::Internal),
        "unavailable" => Ok(Code::Unavailable),
        "data_loss" => Ok(Code::DataLoss),
        "unauthenticated" => Ok(Code::Unauthenticated),
        _ => bail!("unsupported scripted gRPC status code {value:?}"),
    }
}

fn build_rpc_status_specs(defs: Vec<RpcStatusSpecDef>) -> Result<Vec<RpcStatusSpec>> {
    defs.into_iter()
        .map(|def| {
            let code = parse_status_code(&def.code)?;
            Ok(RpcStatusSpec {
                code,
                message: def
                    .message
                    .unwrap_or_else(|| format!("scripted {}", def.code)),
            })
        })
        .collect()
}

fn build_batch_spec(batch: BatchEventDef, default_cu_per_tx: u32) -> Result<BatchSpec> {
    Ok(BatchSpec {
        seq_id: batch.seq_id,
        expect_result: batch.expect_result.unwrap_or(true),
        packets: build_script_packets(
            batch.packet_count,
            batch.packets_base64,
            batch.packets_base64_file,
            batch.cu_per_tx.unwrap_or(default_cu_per_tx),
            batch.simple_vote_tx.unwrap_or(false),
            batch.revert_on_error.unwrap_or(true),
            batch.revert_on_error_sequence,
            batch.packet_meta_sequence,
            batch.packet_data_size,
        )?,
        max_schedule_slot: parse_schedule_slot(batch.max_schedule_slot)?,
    })
}

fn parse_schedule_slot(slot: Option<ScheduleSlotDef>) -> Result<u64> {
    match slot {
        None => Ok(u64::MAX),
        Some(ScheduleSlotDef::Number(slot)) => Ok(slot),
        Some(ScheduleSlotDef::Keyword(keyword)) => parse_schedule_slot_keyword(&keyword),
    }
}

fn parse_schedule_slot_keyword(keyword: &str) -> Result<u64> {
    match keyword {
        "max" | "u64_max" | "u64::MAX" => return Ok(u64::MAX),
        "leader" | "leader_slot" | "current_leader" => {
            return Ok(SCHEDULE_SLOT_LATEST_LEADER_BASE);
        }
        _ => {}
    }

    for prefix in ["leader+", "leader_slot+", "current_leader+"] {
        if let Some(offset) = keyword.strip_prefix(prefix) {
            let offset = offset
                .parse::<u64>()
                .with_context(|| format!("invalid max_schedule_slot keyword {keyword:?}"))?;
            if offset >= SCHEDULE_SLOT_LATEST_LEADER_MAX_OFFSET {
                bail!(
                    "max_schedule_slot keyword {keyword:?} offset must be below {SCHEDULE_SLOT_LATEST_LEADER_MAX_OFFSET}"
                );
            }
            return Ok(SCHEDULE_SLOT_LATEST_LEADER_BASE + offset);
        }
    }

    bail!("unsupported max_schedule_slot keyword {keyword:?}")
}

fn build_script_packets(
    packet_count: Option<u64>,
    packets_base64: Option<Vec<String>>,
    packets_base64_file: Option<PathBuf>,
    cu_per_tx: u32,
    simple_vote_tx: bool,
    revert_on_error: bool,
    revert_on_error_sequence: Option<Vec<bool>>,
    packet_meta_sequence: Option<Vec<PacketMetaModeDef>>,
    packet_data_size: Option<u64>,
) -> Result<Vec<Packet>> {
    let encoded_from_file = match packets_base64_file {
        Some(path) => Some(load_base64_packet_file(&path)?),
        None => None,
    };

    let mut packets = match (packet_count, packets_base64, encoded_from_file) {
        (Some(_), Some(_), _) => bail!("scenario event cannot specify both packet_count and packets_base64"),
        (Some(_), _, Some(_)) => bail!("scenario event cannot specify both packet_count and packets_base64_file"),
        (None, Some(_), Some(_)) => bail!("scenario event cannot specify both packets_base64 and packets_base64_file"),
        (None, None, None) => bail!("scenario event must specify one of packet_count, packets_base64, or packets_base64_file"),
        (Some(count), None, None) => Ok(build_packets(
            count,
            cu_per_tx,
            simple_vote_tx,
            revert_on_error,
        )),
        (None, Some(encoded_packets), None) => encoded_packets
            .into_iter()
            .map(|packet| decode_base64_packet(&packet, simple_vote_tx, revert_on_error))
            .collect(),
        (None, None, Some(encoded_packets)) => encoded_packets
            .into_iter()
            .map(|packet| decode_base64_packet(&packet, simple_vote_tx, revert_on_error))
            .collect(),
    }?;

    if let Some(sequence) = revert_on_error_sequence {
        if sequence.len() != packets.len() {
            bail!(
                "revert_on_error_sequence has {} entries for {} packets",
                sequence.len(),
                packets.len()
            );
        }
        for (packet, revert_on_error) in packets.iter_mut().zip(sequence) {
            let meta = packet.meta.get_or_insert_with(Meta::default);
            meta.flags
                .get_or_insert_with(PacketFlags::default)
                .revert_on_error = revert_on_error;
        }
    }

    if let Some(size) = packet_data_size {
        if size > 1_048_576 {
            bail!("packet_data_size must not exceed 1048576");
        }
        let size = usize::try_from(size).map_err(|_| anyhow!("packet_data_size overflow"))?;
        for packet in &mut packets {
            packet.data.resize(size, 0);
            if let Some(meta) = packet.meta.as_mut() {
                meta.size = size as u64;
            }
        }
    }

    if let Some(sequence) = packet_meta_sequence {
        if sequence.len() != packets.len() {
            bail!(
                "packet_meta_sequence has {} entries for {} packets",
                sequence.len(),
                packets.len()
            );
        }
        for (packet, mode) in packets.iter_mut().zip(sequence) {
            match mode {
                PacketMetaModeDef::Full => {}
                PacketMetaModeDef::MissingMeta => packet.meta = None,
                PacketMetaModeDef::MissingFlags => {
                    let meta = packet.meta.get_or_insert_with(Meta::default);
                    meta.flags = None;
                }
            }
        }
    }

    Ok(packets)
}

fn load_base64_packet_file(path: &Path) -> Result<Vec<String>> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read packet base64 file {}", path.display()))?;
    let packets = contents
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_string)
        .collect::<Vec<_>>();
    if packets.is_empty() {
        bail!(
            "packet base64 file {} did not contain any packets",
            path.display()
        );
    }
    Ok(packets)
}

fn decode_base64_packet(
    encoded: &str,
    simple_vote_tx: bool,
    revert_on_error: bool,
) -> Result<Packet> {
    let data = BASE64_STANDARD
        .decode(encoded)
        .map_err(|err| anyhow!("failed to decode packet base64: {err}"))?;
    let size = u64::try_from(data.len()).map_err(|_| anyhow!("packet size overflow"))?;
    Ok(Packet {
        data,
        meta: Some(Meta {
            size,
            flags: Some(PacketFlags {
                simple_vote_tx,
                revert_on_error,
            }),
        }),
    })
}

fn format_socket(sock: Option<&Socket>) -> String {
    sock.map(|sock| format!("{}:{}", sock.ip, sock.port))
        .unwrap_or_else(|| "<none>".to_string())
}

fn format_socket_list(socks: &[Socket]) -> String {
    if socks.is_empty() {
        return "<none>".to_string();
    }

    socks
        .iter()
        .map(|sock| format!("{}:{}", sock.ip, sock.port))
        .collect::<Vec<_>>()
        .join(",")
}
