# BAM Validator Client (jito-solana) <-> BAM Node (bam repo) Clean-Room Specification

This document specifies the on-the-wire protocol and observed behavioral contract between:

- The validator-side BAM client in the separate `jito-solana` repo.
- The BAM Node service in `bam` (the "bam-node").

It is written so an engineer can reimplement BAM support clean-room, without reading either codebase.

Last verified against:

- [jito-solana git commit
  `bc49b3903412d4e06ef274b9b18530b1ece4f24b`](https://github.com/jito-labs/jito-solana/tree/bc49b3903412d4e06ef274b9b18530b1ece4f24b)
- bam git commit `d79a06c77dd55bd673583e1870ac5eb61cd1381f`

## Terminology

- Node: The BAM Node gRPC server.
- Validator, Client: A Solana validator process with BAM enabled; it connects to Node over gRPC and executes
  Node-supplied batches.
- AtomicTxnBatch: Protobuf message `bam_types.AtomicTxnBatch { seq_id, max_schedule_slot, packets[] }`. Can be a single
  transaction or a bundle.
- Batch packet: Protobuf message `bam_types.Packet { data, meta }`, where `data` is the serialized Solana transaction.
- revert_on_error batch: All packets in the batch have `PacketFlags.revert_on_error = true`. Execution is atomic:
  all-or-nothing for inclusion/recording.
- non-revert batch: `PacketFlags.revert_on_error = false`; atomic rollback is not requested.
- Leader bank (leader working bank): The active bank from PoH recorder state. Used for `LeaderState` emission and BAM
  execution.
- BankForks working bank: Used for parsing and scheduling checks, and as the current-slot fallback.
- Root bank: `bank_forks.root_bank()`. Used for LUT resolution and reserved account key checks during parsing.
- Leader slot: A slot where the validator has an unfrozen leader bank and is producing entries.

## High-Level Behavior

1. Validator connects to Node via gRPC, authenticates by signing a Node-provided challenge, and fetches config.
2. While connected, validator sends leader state updates while it is leader, receives `AtomicTxnBatch` messages from
   Node, validates and schedules them with account-aware conflict tracking, executes them against the active leader
   bank, and returns batch results to Node.
3. Connection state and received config control validator-side non-vote scheduling, Block Engine streaming, TPU
   selection, and shred-routing overrides as detailed below.
4. Node handles validator execution results by correlating them with forwarded batches by `seq_id` and applying
   retry/non-retry policy based on the returned outcome.

## Protobuf and gRPC API

The Node exposes `bam_api.BamNodeApi`:

1. `GetAuthChallenge(AuthChallengeRequest) -> AuthChallengeResponse`
2. `GetBuilderConfig(ConfigRequest) -> ConfigResponse`
3. `InitSchedulerStream(stream SchedulerMessage) -> stream SchedulerResponse`

Messages are protobuf3 and versioned via `oneof versioned_msg { v0 }`. Current implementations only accept `v0`.
The Node requires both the `v0` wrapper and a populated `SchedulerMessageV0.msg` on every validator-to-Node message;
otherwise it returns `invalid_argument`. The validator disconnects on a missing or unsupported Node-to-validator version
wrapper, but silently ignores a valid `v0` `SchedulerResponseV0` whose `resp` is unset. Such an empty v0 response still
counts as the first successfully decoded inbound message that starts periodic config refresh.

### Stream Message Types (v0)

Validator -> Node (`SchedulerMessageV0.msg`):

- `AuthProof { challenge_to_sign, validator_pubkey, signature }`
- `ValidatorHeartBeat { time_sent_microseconds }`
- `LeaderState { slot, tick, slot_cu_budget_remaining }`
- `MultipleAtomicTxnBatchResult { results: [AtomicTxnBatchResult] }`
- `Pong { id }` (gRPC RTT telemetry)

Node -> Validator (`SchedulerResponseV0.resp`):

- `BuilderHeartBeat { time_sent_microseconds }`
- `MultipleAtomicTxnBatch { batches: [AtomicTxnBatch] }`
- `Ping { id }` (gRPC RTT telemetry)

## Authentication

### Label and Signed Bytes

Both Node and Validator use the same label prefix:

- `AUTH_LABEL = b"X_OFF_CHAIN_JITO_BAM_V1\0"` (note the trailing NUL byte)

Signing/verification input:

1. Node issues an opaque UTF-8 `challenge_to_sign`.
2. Validator forms `labeled_bytes = AUTH_LABEL || challenge_to_sign.as_bytes()`.
3. Validator signs `labeled_bytes` with the validator identity keypair (ed25519).
4. Validator sends `AuthProof` with `validator_pubkey` as a base58 pubkey string and `signature` as a base58 signature
   string

### Validator Requirements

- The first message sent on `InitSchedulerStream` MUST be `AuthProof`.
- If the validator identity changes at runtime, the validator MUST reconnect and re-authenticate (auth is tied to the
  identity key).

### Node Requirements (Observed)

Node accepts `AuthProof` only if:

- The connection is not already authenticated (node accepts `AuthProof` only once per stream).
- `validator_pubkey` parses, and signature verifies for `labeled_bytes`.
- The challenge is known and unused. Node consumes it before age, leader-schedule, and signature checks, so a later
  failure still makes it unusable.
- The validator is on leader schedule and not disabled by policy.
- The validator sends `AuthProof` within 2s of starting `InitSchedulerStream`.
- The challenge age does not exceed `max_ping_rtt_us` (default 30ms).
- Except in test-node mode, the current Node rejects authentication when the validator is "too close" to its next leader
  slots (within the inclusive configurable lookahead).

## Connection Liveness and Health

### Initial Connection Refusal (Observed)

Before authentication, the Node may refuse to start the scheduler stream:

- If the Node is not healthy, `InitSchedulerStream` returns `unavailable` ("Node is not healthy").
- If the Node is shutting down, `InitSchedulerStream` returns `unavailable` ("Node is shutting down").
- A client IP recently disconnected for high RTT may receive
  `failed_precondition` until the configured reconnect backoff expires (default 5 minutes).

### Heartbeats

- Node sends `BuilderHeartBeat` every 2s; the validator reconnects after 6s without one.
- Validator sends `ValidatorHeartBeat` every 5s; Node disconnects it after 10s without one.
- Payload timestamps are informational. Liveness uses local receive time, so synchronized clocks are unnecessary.

### Ping/Pong

- Node may send `Ping`; the validator echoes its `id` in `Pong`.
- Node sends at most one outstanding ping on its 2s heartbeat cadence. A matching `Pong` records elapsed application
  round-trip time, which includes stream queueing and validator response delay.
- A missing `Pong` records a capped timeout sample; an unexpected or mismatched `Pong` is reported but does not itself
  disconnect the stream.
- Every configured policy interval (default 60s), Node reports one aggregate gRPC RTT datapoint and resets the
  reporting histogram. The disconnect decision uses the cumulative mean for the connection, which is retained until
  disconnect. If that mean exceeds the threshold (default 30ms), Node disconnects. The near-leader exception delays
  the decision without discarding RTT evidence.

### Other Node Disconnect Conditions (Observed)

In addition to heartbeat timeouts and explicit RPC errors, the node may terminate the scheduler stream for policy/health
reasons, including:

- Not authenticating within 2s.
- The validator becoming disabled, banned by the optional Autobahn enforcer policy, or absent from the leader schedule.
  Autobahn enforcement is disabled by default.
- A newer authenticated connection replacing an existing one for the same validator pubkey (the replaced stream
  terminates with "Connection replaced").
- Node health degradation or shutdown, subject to leader-lookahead exceptions.
- Mean gRPC Ping/Pong RTT above the configured threshold at a periodic policy check, subject to a near-leader exception.

## Configuration (GetBuilderConfig) and Validator Effects

`ConfigResponse` contains:

- `block_engine_config { builder_pubkey, builder_commission }`
- `bam_config { prio_fee_recipient_pubkey, commission_bps, tpu_sock, tpu_fwd_sock, shred_socks[] }`

`ConfigRequest` is empty and unauthenticated. Node currently returns its builder identity/commission, public TPU and
shred endpoints, `commission_bps = 300`, and the builder pubkey as `prio_fee_recipient_pubkey`.

Validator applies config live:

1. Apply BAM TPU config only when both sockets parse as IPv4; otherwise preserve the prior pair. The pair is retained
   across disconnects but selected only while `Connected`. Fetch stage prefers usable BAM sockets, then a healthy
   relayer, then original TPU sockets. It adds `+6` to BAM ports when gossiping QUIC addresses and restores the prior
   selection if a gossip update fails.
2. Accept `builder_commission` only in the range 0-100. An invalid commission preserves both builder fields. With a
   valid commission, an invalid `builder_pubkey` preserves only the prior pubkey while the new commission applies.
3. Apply `prio_fee_recipient_pubkey` only if it parses. Ignore `commission_bps`.
4. For `shred_socks`, retain at most 32 valid unique IPv4 destinations with nonzero ports. Clear them on disconnect or
   missing `bam_config`. The validator sends produced shreds to them while leading and during retransmit shortly before
   its next consecutive leader run (1-4 slots).

Config polling starts after the first decoded inbound stream message and runs every second. A successful response is
required to enter `Connected`; subsequent polling failures preserve the last config and do not by themselves
disconnect an otherwise healthy session. Individual config fields need not parse successfully.

A successfully received response counts as config even when either optional protobuf submessage is absent. Missing
`block_engine_config` preserves the prior builder pubkey and commission. Missing `bam_config` preserves the prior
TPU/TPU-forward pair and priority-fee recipient, while explicitly clearing the installed BAM shred destinations. Such
a response can still satisfy the config prerequisite for `Connected`.

## BAM URL Configuration (Validator)

- `--bam-url` enables BAM. The CLI accepts `http` and `https`, supplies a missing scheme, and defaults to port 50055 for
  HTTP or 50056 for HTTPS.
- Admin RPC `setBamUrl` changes or disables BAM at runtime. Unlike the CLI, a non-empty value must already be a
  fully-qualified `tonic::transport::Endpoint`; null or blank disables BAM.
- Any runtime URL change disconnects the current session and, when a new URL is present, reconnects and
  re-authenticates.

## Leader State (Validator -> Node)

`LeaderState` reports the validator's view that it is currently producing a leader slot, together with its intra-slot
progress and remaining compute budget. In the current Node, this message is informational: the Node range-checks its
`slot` and stores the latest value on the connection, but live scheduler slot targeting and pacing do not consume
`LeaderState`. They use the Node's bank and leader-schedule state instead.

### When It Is Sent (Observed)

While the gRPC stream is up, the validator emits `LeaderState` only when:

- A `working_bank` exists in the shared leader state, and
- That bank is not frozen.

### Field Semantics (Observed)

For the active unfrozen leader `Bank`:

- `LeaderState.slot = bank.slot()`
- `LeaderState.tick = (bank.tick_height() % bank.ticks_per_slot()) as u32`
- `LeaderState.slot_cu_budget_remaining = block_cost_limit.saturating_sub(block_cost) as u32`, where
  `block_cost_limit = bank.read_cost_tracker().block_cost_limit()` and
  `block_cost = bank.read_cost_tracker().block_cost()`.

### Node Validation (Observed)

On receiving `LeaderState`, the Node validates that `leader_state.slot` is within a leeway window of its own
`current_slot`:

- Valid range: `[current_slot - 5, current_slot + 5]` (inclusive, saturating at 0).
- If outside that range, Node returns an `invalid_argument` error and disconnects the validator stream.
- `tick` and `slot_cu_budget_remaining` are accepted as-is. The handler also does not independently verify that the
  authenticated validator is the scheduled leader of the reported slot.

## BAM Overrides Inside the Validator

When `bam_enabled == Connected`, validator-side non-vote scheduling switches to BAM-supplied batches:

- The dedicated BAM scheduler replaces the normal non-vote scheduler.
- Block Engine streaming is suppressed only in `Connected`, not `Connecting`.
- TPU forwarding is controlled separately by fetch-stage selection; `Connected` alone does not disable it.
- Vote processing and validator-internal maintenance work (for example tip-program upkeep bundles) still continue.

### bam_enabled State Machine (Observed)

- `Disconnected`: BAM inactive; normal non-vote scheduling enabled.
- `Connecting`: stream/config setup in progress; normal scheduling remains enabled and BAM scheduling disabled.
- `Connected`: heartbeat-healthy with at least one config response; BAM scheduling enabled and normal scheduling
  disabled.

An unhealthy connection, URL change, or identity change returns the validator to `Disconnected`. After an identity
change it waits until the new identity appears in `ClusterInfo` rather than authenticating with the old identity.
Successfully parsed batches received before `Connected` may be dropped without results during `Consume`/`Hold`; during
`Forward`/`ForwardAndHold` they receive `OUTSIDE_LEADER_SLOT`.

Gossip advertises `ClientId::AgaveBam` while connected and restores `ClientId::JitoLabs` otherwise.

## Node -> Validator: AtomicTxnBatch Delivery

Node sends:

- `SchedulerResponseV0.resp = MultipleAtomicTxnBatch { batches: [...] }`

Each `AtomicTxnBatch` contains:

- `seq_id: u32` (Node-generated result-correlation identifier; validator FIFO priority is independent of it)
- `max_schedule_slot: u64` (latest slot accepted by the validator's parsing and scheduling stages)
- `packets: repeated Packet`

- Standalone transactions use one packet with `revert_on_error = false`.
- Bundles use one to five packets with `revert_on_error = true`.
- Outer grouping reflects worker packing and may mix standalone and bundle
  batches.
- Message grouping is a transport/implementation detail and MUST NOT be used to infer scheduling or atomicity. Atomicity
  is defined per `AtomicTxnBatch`.
- An empty outer group is ignored and produces no result.
- Node assigns each batch a `seq_id` from a process-lifetime wrapping counter and sets `max_schedule_slot` to the
  speculative bank's slot. The current implementation does not check for a pending or late-result collision when the
  counter wraps; results carry no auction-generation identifier.
- The protocol permits multi-packet non-revert batches, but current Node output and validator result mapping assume one
  packet.

## Node: Intake, Prioritization, and Dispatch Path (Observed)

This section covers node policy that materially affects validator-visible work.

### Ingress and Parsing

Node ingests three logical work classes:

- Single transactions from TPU and Block Engine packet streams.
- Maker transactions from the plugin TPU (PTPU) path.
- Bundles from the Block Engine bundle stream.

Block Engine connection ownership is tied to authenticated validator streams. After a successful `AuthProof`, the Node
starts one client per configured URL for that validator identity and subscribes to packet and bundle streams. These
clients feed shared ingress and terminate when that validator stream is replaced or disconnected. Their non-blocking
handoff can drop packets or whole bundles under pressure.

The Node can also enable ZMQ subscribers independently of validator authentication. Configured transaction endpoints
feed the packet lane; every configured bundle class feeds the same unverified bundle lane and filtering stages.
Handoffs are non-blocking and lossy under pressure. High/low labels and tip/compute metadata encoded in a bundle UUID
do not override scheduler priority; it is recomputed from transactions and configured tip accounts.

Block Engine bundle prefilter path:

- CPU-verifies every signature before parsing; one failed packet drops the entire bundle.
- Parses and applies the bundle admission checks described below into a separate priority-bounded container.
- Pre-simulates bundles asynchronously against the current working bank.
- Forwards a reserialized bundle only when it is nonempty and every flattened transaction processing result succeeds.
  The later `simple_forwarder` path parses and checks the bundle again, then performs the slot-scoped speculative
  execution used for dispatch.

Transaction receive path:

- Parses bytes into sanitized and then resolved runtime transaction views.
- Treats Legacy and V1 views as no-LUT transactions; resolves V0 address lookup tables against the root bank. All
  versions use root-bank reserved account keys when producing the resolved view. The later bank check rejects V1 with
  `UnsupportedVersion` until the bank's `enable_tx_v1` feature is active.
- Rejects simple vote transactions, blacklisted accounts, invalid account-lock sets, and invalid compute-budget
  instructions.
- Computes transaction priority as `reward * 1_000_000 / (cost + 1)`, where reward is the validator deposit from the
  transaction fee calculation and cost is the cost-model estimate.
- Runs dynamic `check_transactions` plus fee-payer solvency checks before queueing.

Bundle receive path:

- Rejects an empty bundle, a bundle with more than five packets, any packet already marked discarded, or the entire
  bundle when any contained packet fails parsing or admission checks.
- Parses each packet into a runtime transaction view using the same root-bank LUT resolution machinery.
- Rejects bundle-local duplicate message hashes before bundle admission.
- Rejects blacklisted accounts, invalid locks, and invalid compute-budget instructions.
- Computes bundle priority as `(sum(reward) + sum(static system-transfer tips to configured tip accounts)) *
  1_000_000 / (sum(cost) + 1)`. A static tip is recognized only when a system-program instruction has exactly 12 bytes
  of data that decodes via wincode as `(2u32, lamports: u64)`, and its second instruction account resolves to a configured
  tip account. Recognized amounts within a transaction are accumulated with saturating addition. The scorer itself does
  not validate the transfer source, authorization, balance, or eventual execution success; later admission and
  simulation may still reject the transaction.
- Does not reject simple vote transactions on this Node bundle path; they may reach validator ingestion.
- Applies dynamic `builder_bank.check_transactions` across the bundle before queueing.
- Only checks the fee payer of the first transaction in the bundle on this node-side path.

PTPU maker receive path:

- Enrollment policy loads from `bam.mpp_configs`, falling back through configured peer HTTP endpoints. No valid initial
  policy disables PTPU. Refresh failures retain the last good state. A strictly older version is rejected, as is a
  versionless update after a versioned state has been stored. A changed payload with the same version is accepted. When
  the payload is unchanged, an otherwise accepted version change updates the stored version, while an unchanged version
  and payload is a no-op.
- The program configuration list is append-only. If a signer appears under
  multiple program entries, the lowest-indexed (earliest) entry remains active
  and later entries for that signer are ignored. Each policy load or refresh
  reports the ignored count as
  `ptpu_mpp_config.num_ignored_duplicate_signer_program_mappings`; operators can
  alert on a nonzero value to detect ignored program configurations.
- UDP packets pass byte deduplication, metadata validation, and CPU signature verification.
- Metadata requires exactly one enrolled signer, its active signer-to-program
  mapping, and at least one enrolled writable market update. Every writable
  instruction account other than the signer must be an enrolled market;
  single-market mode rejects updates to multiple markets.
- Metadata validation uses static account keys only; address-table keys are not resolved. Out-of-range program or
  writable-account indexes are rejected.
- Instructions may target only the enrolled program, the compute-budget program, or the system program.
- The configured instruction range supplies a masked 32- or 64-bit little-endian sequence number. Every matching
  instruction must contain the range; the last supplies the stored sequence. The last `SetComputeUnitPrice`, converted
  to lamports, must meet the maker minimum.
- Work must contain plugin metadata and pass the ordinary standalone transaction checks before entering the dedicated
  maker container.
- For 32-bit sequence numbers, freshness uses wrapping RFC-1982-style comparison; other widths use ordinary numeric
  comparison.
- Before scheduling, an older transaction is dropped when it is stale for every market it updates. For a shared
  same-sequence scope, only the most recently inserted transaction for that scope and sequence remains eligible.
- The per-market highest-sequence values are high-water marks: removing or capacity-evicting the transaction that set a
  mark does not recompute it. The marks reset when the maker container is cleared.
- During auction fill, all currently queued maker transactions are drained before regular transactions and bundles,
  without the ordinary per-fill item cap. They otherwise execute and dispatch as standalone non-revert transactions.
- Refreshed policy applies to later packets; already-buffered maker transactions are not revalidated or evicted.

### Buffering and Ordering

- Buffered work is capacity-bounded.
- Transaction, maker-transaction, and bundle queues drop the current lowest-priority queued work when over capacity.
- When choosing between the highest-priority ordinary transaction and bundle, a transaction wins only when its priority
  is strictly greater. An exact priority tie therefore selects the bundle. Maker transactions are handled by their
  earlier, drain-first rule rather than this comparison.
- Auction admission adds work to an account-conflict DAG in priority order.
- Auction-local deduplication covers bundle ID, message hash, and certain durable-nonce conflicts.
- A shared bounded rejection cache can reject, before address-table resolution, signatures already processed, expired
  or unadvanceable durable nonces, nonce transactions missing a cached authority signature, and non-nonce transactions
  with a currently invalid blockhash. Signature and nonce state is cached; blockhash validity is checked live.
- Already-processed and durable-nonce entries are slot-local and derive from committed state or validator results. A
  speculative nonce write invalidates its cached authority so a same-slot authority change does not poison later work.

### Leader-Window Intake and Auction Policy

Except in explicit always-auction mode, receive workers and the state machine gate work using the connected-validator
leader schedule:

- The bundle gate applies before prefilter simulation and again at
  `simple_forwarder`. The upstream Block Engine signature-verification stage
  continues independently. When the gate is closed, raw bundles are discarded
  before simulation and any prepared output is discarded downstream.
- Transaction and PTPU ingress is enabled only when a connected leader exists within `buffer_slot_lookahead`.
- Bundle ingress uses the separate, normally tighter `bundle_buffer_slot_lookahead`.
- Configuration clamps the effective `bundle_buffer_slot_lookahead` to at most `buffer_slot_lookahead`.
- A closed receive gate drains pending and raw ingress rather than accumulating stale work.

The state machine applies these phase-specific policies:

- `NoLeaderSoon`: drain transaction, PTPU, and bundle ingress; stop any auction; clear ordinary, maker, and bundle
  containers; and clear both the slot-scoped and shared speculative bank.
- `LeaderSoon`: clear the speculative bank, receive and priority-buffer transaction and PTPU work, and receive bundles
  only after entering the tighter bundle lookahead. Otherwise bundle ingress and the bundle container are drained.
  Ordinary transactions and bundles are periodically rechecked for duplicates, cached rejection, and bank validity.
  Maker freshness and same-sequence eligibility are checked when an auction fills.
- `LeaderNow*`: create or reuse the slot-scoped speculative bank and run auction start, active, cleanup, and between-round
  states. Only these states simulate and dispatch work.

### Speculative Execution and Dispatch

The scheduler:

- Simulates ready DAG items against a slot-scoped speculative bank.
- Dispatches only successfully committed speculative results to the validator.
- Sends committed standalone transactions as one-packet, `revert_on_error = false` batches.
- Sends committed bundles as grouped `revert_on_error = true` batches.
- Targets the pubkey identified as leader by the current speculative working bank and looks up that exact identity's
  active stream in the connected-leader schedule; work is not broadcast to all connected validators.
- Serializes each packet with:
  - `meta.size = packet.data.len()`
  - `meta.flags.revert_on_error = <batch atomicity>`
  - `meta.flags.simple_vote_tx = false`

Auction cost pacing comes from Node bank timing, not `LeaderState`:

- A slot pacer treats parent-bank freeze time as slot start and linearly releases the working bank's block and
  per-account cost limits over the estimated slot.
- Each round budgets through its projected end. Candidate fill applies a packing multiplier while speculative
  admission and commit remain bounded by released block and account limits.
- Once at least half of a round has elapsed, the auction compares committed block-cost progress with the linear
  progress expected toward that round's released target. If actual progress is below 85% of expected progress, bundle
  admission is disabled for the rest of that round and deferred bundles are requeued while standalone transaction
  work continues. This transaction-only decision is sticky only for that auction round.

The auction uses Wavefront: graph workers pop ready nodes, commit directly into
the speculative bank, and cascade newly ready dependents. Bundles execute alone
inside a worker batch, while compatible standalone items may be packed together.
These packing details do not change the per-batch wire and result contract.

Retryable work is split into two buckets:

- `RetryableThisSlot`: may be retried again in the current slot. Work that was never sent to a validator uses this bucket
  during cleanup.
- `RetryableNextSlot`: must wait until the next slot. Work that was already sent to the validator is intentionally
  deferred here on retry/cleanup so late validator results do not corrupt speculative status handling.

## Validator: Batch Ingestion, Checks, and Filters

The validator runs a dedicated parsing pipeline before any batch is eligible for execution.

### Current Slot for Validation

Validator computes `current_slot` for batch validation as:

- If shared leader state contains a `working_bank`, use that bank's `slot` (no frozen/complete check at this stage).
- Otherwise use the BankForks working bank's `slot`.

### Batch-Level Prevalidation

For each `AtomicTxnBatch`, validator enforces:

1. Slot window:
   If `max_schedule_slot < current_slot`, reject with `SchedulingError::OUTSIDE_LEADER_SLOT`.
2. Non-empty:
   If `packets.is_empty()`, reject with `DeserializationErrorReason::EMPTY` at index 0.
3. Packet count:
   If `packets.len() > 5`, reject with `DeserializationErrorReason::SANITIZE_ERROR` at index 0.
4. Consistent revert flag:
   All packets must have the same `meta.flags.revert_on_error`. Missing `meta`/`flags` is treated as
   `revert_on_error = false`. If not consistent, reject with `DeserializationErrorReason::INCONSISTENT_BUNDLE` at index
   0.

### Signature Verification

After prevalidation, the validator verifies every packet signature. Oversized data is rejected rather than truncated,
including V1 data; proto `meta.size` is ignored. `simple_vote_tx` is propagated into local sigverify metadata only; it is
not trusted as the semantic vote classification. Any discard rejects the entire batch with `SANITIZE_ERROR` at the
failing packet index.

### Per-Transaction Parsing and Bank-Front-Run Checks

For each packet in the batch, validator performs the following checks in order.
Any failure rejects the entire batch, with the failing transaction index in the reason.

1. Vote-only mode reject:
   If the BankForks working bank is vote-only, reject with `DeserializationErrorReason::SANITIZE_ERROR`.
2. Basic sanitization:
   Parse packet bytes using the root-bank `limit_instruction_accounts` feature. Failures map to
   `DeserializationErrorReason::SANITIZE_ERROR`. The validator constructs the runtime transaction without supplying the
   protobuf vote hint, so `is_simple_vote_transaction` is recomputed from the transaction bytes.
3. Reject vote transactions:
   If `is_simple_vote_transaction == true`, reject with `DeserializationErrorReason::VOTE_TRANSACTION_FAILURE`.
4. Resolve address lookup tables:
   `Legacy` and `V1` transaction views take the no-LUT path. For `V0`, load LUT addresses from the root bank using the
   transaction's address table lookups. Convert every version to a resolved transaction view using the root bank's
   reserved account keys. Failure maps to `DeserializationErrorReason::SANITIZE_ERROR`.
5. Validate account locks per transaction:
   Enforce "no duplicate accounts" and the per-transaction account lock limit (limit is read from the working bank).
   Failure maps to `NotCommitted.reason = TransactionError` with the mapped `TransactionErrorReason`.
6. Validate compute budget instructions:
   Extract and sanitize compute budget limits using the working bank feature set. Failure maps to
   `TransactionErrorReason` as above.
7. Bank checks:
   Run `working_bank.check_transactions(..., MAX_PROCESSING_AGE, ...)`, covering V1 feature gating,
   blockhash/durable-nonce age, status-cache duplicates, and execution/fee-limit derivation. Failure maps to
   `TransactionErrorReason`.
8. Fee payer solvency:
   On the current BAM batch parsing path, only the first transaction's fee payer is checked. The validator verifies
   that the first transaction's fee payer can pay fee and required rent; later transactions in the same BAM bundle are
   not fee-payer-checked here. Failure maps to `TransactionErrorReason`.
9. Blacklisted accounts:
   If any account key is in the validator-provided blacklist, including the tip-payment program ID, reject with
   `TransactionErrorReason::SANITIZE_FAILURE`.

Parsing records the root-bank epoch and each transaction's LUT invalidation horizon for execution-time revalidation.
It does not compute cost-model estimates: admitted transaction states use `cost = 0`, and batch metadata has no
aggregate cost.

### Batch Priority

Validator assigns monotonically decreasing local priority to successfully admitted batches. Earlier batches therefore
win independently of `seq_id`, preserving FIFO order across `seq_id` wrap.

### Buffering Behavior

Validated batches are inserted into a bounded container as one batch entry plus N transaction entries.

The container admission rule is all-or-nothing for a batch:

- A batch consumes one header plus one entry per transaction. If all entries do not fit, reject it with
  `SchedulingError::CONTAINER_FULL`.
- Unlike single-transaction insertion in normal scheduling, batch insertion does not evict lower-priority entries to
  make room.

### Non-Leader Phases

If the validator is not in a leader consume/hold phase (decision is Forward or ForwardAndHold):

- Any buffered batches are flushed with `SchedulingError::OUTSIDE_LEADER_SLOT`.
- Successfully parsed new batches also receive `OUTSIDE_LEADER_SLOT`; earlier prevalidation, signature, and parse
  failures retain their specific errors.

## Validator Scheduling: Priority Graph and Slot Boundaries

### Scheduling Unit

The scheduler treats each atomic batch as one conflict-graph node and one worker unit.

### Slot Tracking and Bank Boundaries

On a slot change, the scheduler releases prior-slot in-flight graph locks, rejects remaining queued or graph work as
`OUTSIDE_LEADER_SLOT`, and clears the graph. Losing the active bank has the same effect.

The incoming `max_schedule_slot` is checked during prevalidation, while pulling work into the priority graph, and again
before dispatch. Immediately before worker dispatch, however, the validator overwrites the work's
`max_schedule_slot` with the scheduler's current slot. The worker therefore checks the dispatch slot, not the original
wire deadline.

### Prio-Graph Resource Model

The resource set is the union of read/write account accesses across every transaction in the batch:

- A Write access conflicts with any prior Read or Write access on the same pubkey.
- A Read access conflicts only with a prior Write access on the same pubkey.

Work enters the graph in descending local batch priority.

### Observed Scheduling Algorithm

1. Pop buffered batches in descending priority, reject stale batches, and run the optional second
   `working_bank.check_transactions` pass.
2. Insert accepted batches into the conflict graph with their combined account accesses.
3. Repeat slot and bank checks before dispatch because state may change while work waits.
4. Dispatch every currently unblocked node. The BAM scheduler ignores the controller's cost budget and worker
   availability for admission; account conflicts still block dependent nodes.
5. Keep dispatched nodes blocked until completion. Map completion to the original `seq_id`, release dependents only in
   the same slot, and remove the batch from the container.

## Validator Execution: Bank and PoH Interaction

### Worker Entry Conditions

- Workers require a present, incomplete leader working bank. This differs from `LeaderState` emission, which requires
  an unfrozen bank.
- The worker checks once and does not wait. The eight BAM workers share one unbounded work receiver. If any worker finds
  no active bank, it drains its current work plus all then-unclaimed work from that shared queue as retryable
  `SchedulingError::POH_TIMEOUT`.
- If the scheduler-stamped dispatch slot is older than the bank slot, return `POH_TIMEOUT` without execution.

### Tip Program Maintenance (BAM Workers)

When enabled, a worker that encounters known tip accounts first attempts the once-per-slot tip-program initialization
and crank bundles. These maintenance bundles are atomic and separate from Node work. Their failure does not prevent the
Node batch from executing; unsuccessful maintenance may be attempted again in the same slot.

### Core Execution Call

Execution uses the normal banking-stage consumer:

1. Revalidate reserved keys and LUTs if the epoch or recorded invalidation horizon changed.
2. Apply QoS, cost tracking, and account locking. Atomic batches use relaxed intra-batch locking, so their transactions
   may share accounts, while conflicts with other in-flight work still fail. Duplicate message hashes fail as
   `AlreadyProcessed`.
3. Execute transactions sequentially in input order.
4. Collect processed transactions in input order, record them together as one PoH transaction batch, and then commit.

### revert_on_error Semantics (Atomicity)

When `revert_on_error == true`, atomicity is enforced at two points:

1. Lock stage:
   If any transaction fails account locking, the entire work is treated as not committed. The failing transaction
   carries its lock error; others are `CommitCancelled`.
2. Execute stage:
   If any transaction does not execute successfully (including instruction error), the entire work is treated as not
   committed. The failing transaction carries its execution error; others are `CommitCancelled`.

When `revert_on_error == false`, transactions can be partially committed.

## Validator Results and Delivery

### Result Shape

Validator attempts to return one `AtomicTxnBatchResult` per received `AtomicTxnBatch`, keyed by `seq_id`.
Due to internal best-effort drops (bounded channels + `try_send`), a clean-room implementation must tolerate missing
results (see Backpressure and Drop Semantics).

`AtomicTxnBatchResult.result` is one of:

- `Committed { transaction_results: [TransactionCommittedResult] }`
- `NotCommitted { reason: oneof(TransactionError, SchedulingError, DeserializationError, GenericInvalid) }`

### TransactionCommittedResult Fields

For each committed transaction, validator reports:

- `cus_consumed` (u32)
- `feepayer_balance_lamports` (u64, post-balance)
- `loaded_accounts_data_size` (u32)
- `execution_success` (bool, true iff execution returned Ok)

A committed transaction may have `execution_success = false`: fees are paid and it is recorded despite its execution
error. For atomic batches, `Committed` requires every transaction to execute successfully.

### NotCommitted Reasons

Scheduling errors:

- `OUTSIDE_LEADER_SLOT`: validator not in leader consume/hold, or batch schedule window stale.
- `CONTAINER_FULL`: local buffer capacity could not admit the batch.
- `POH_TIMEOUT`: catch-all for "bank not available", expired schedule slot at worker, or PoH recording failure.
  Internal recording failures also collapse to `POH_TIMEOUT`.

Transaction errors:

- Reported as `TransactionError { index, reason }`.
- `index` is the 0-based transaction index within the batch.
- `reason` is a mapped `TransactionErrorReason` derived from the validator's internal `TransactionError`.

#### TransactionErrorReason Mapping (Observed)

Validator maps `solana_transaction_error::TransactionError` to the protobuf enum as follows:

- `AccountInUse` -> `ACCOUNT_IN_USE`
- `AccountLoadedTwice` -> `ACCOUNT_LOADED_TWICE`
- `AccountNotFound` -> `ACCOUNT_NOT_FOUND`
- `ProgramAccountNotFound` -> `PROGRAM_ACCOUNT_NOT_FOUND`
- `InsufficientFundsForFee` -> `INSUFFICIENT_FUNDS_FOR_FEE`
- `InvalidAccountForFee` -> `INVALID_ACCOUNT_FOR_FEE`
- `AlreadyProcessed` -> `ALREADY_PROCESSED`
- `BlockhashNotFound` -> `BLOCKHASH_NOT_FOUND`
- `InstructionError(_, _)` -> `INSTRUCTION_ERROR`
- `CallChainTooDeep` -> `CALL_CHAIN_TOO_DEEP`
- `MissingSignatureForFee` -> `MISSING_SIGNATURE_FOR_FEE`
- `InvalidAccountIndex` -> `INVALID_ACCOUNT_INDEX`
- `SignatureFailure` -> `SIGNATURE_FAILURE`
- `InvalidProgramForExecution` -> `INVALID_PROGRAM_FOR_EXECUTION`
- `SanitizeFailure` -> `SANITIZE_FAILURE`
- `ClusterMaintenance` -> `CLUSTER_MAINTENANCE`
- `AccountBorrowOutstanding` -> `ACCOUNT_BORROW_OUTSTANDING`
- `WouldExceedMaxBlockCostLimit` -> `WOULD_EXCEED_MAX_BLOCK_COST_LIMIT`
- `UnsupportedVersion` -> `UNSUPPORTED_VERSION`
- `InvalidWritableAccount` -> `INVALID_WRITABLE_ACCOUNT`
- `WouldExceedMaxAccountCostLimit` -> `WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT`
- `WouldExceedAccountDataBlockLimit` -> `WOULD_EXCEED_ACCOUNT_DATA_BLOCK_LIMIT`
- `TooManyAccountLocks` -> `TOO_MANY_ACCOUNT_LOCKS`
- `AddressLookupTableNotFound` -> `ADDRESS_LOOKUP_TABLE_NOT_FOUND`
- `InvalidAddressLookupTableOwner` -> `INVALID_ADDRESS_LOOKUP_TABLE_OWNER`
- `InvalidAddressLookupTableData` -> `INVALID_ADDRESS_LOOKUP_TABLE_DATA`
- `InvalidAddressLookupTableIndex` -> `INVALID_ADDRESS_LOOKUP_TABLE_INDEX`
- `InvalidRentPayingAccount` -> `INVALID_RENT_PAYING_ACCOUNT`
- `WouldExceedMaxVoteCostLimit` -> `WOULD_EXCEED_MAX_VOTE_COST_LIMIT`
- `WouldExceedAccountDataTotalLimit` -> `WOULD_EXCEED_ACCOUNT_DATA_TOTAL_LIMIT`
- `DuplicateInstruction(_)` -> `DUPLICATE_INSTRUCTION`
- `InsufficientFundsForRent { .. }` -> `INSUFFICIENT_FUNDS_FOR_RENT`
- `MaxLoadedAccountsDataSizeExceeded` -> `MAX_LOADED_ACCOUNTS_DATA_SIZE_EXCEEDED`
- `InvalidLoadedAccountsDataSizeLimit` -> `INVALID_LOADED_ACCOUNTS_DATA_SIZE_LIMIT`
- `ResanitizationNeeded` -> `RESANITIZATION_NEEDED`
- `ProgramExecutionTemporarilyRestricted { .. }` -> `PROGRAM_EXECUTION_TEMPORARILY_RESTRICTED`
- `UnbalancedTransaction` -> `UNBALANCED_TRANSACTION`
- `ProgramCacheHitMaxLimit` -> `PROGRAM_CACHE_HIT_MAX_LIMIT`
- `CommitCancelled` -> `COMMIT_CANCELLED`

Payload-bearing variants collapse to their enum category; their payload is not transmitted.

Deserialization errors:

- Reported as `DeserializationError { index, reason }`.
- Used for signature verify failures and sanitize/parse failures that occur before bank checks.

### Aggregation Rules

For `revert_on_error == true` batches:

1. If all transactions are committed, return `Committed` and include all `TransactionCommittedResult` entries in order.
2. Otherwise return `NotCommitted`:
   Scan `processed_results` left-to-right. Ignore `CommitCancelled` when picking the "primary" error. Select the first
   occurrence of either a non-cancelled `TransactionError` or a `POH_TIMEOUT` (whichever appears first). If neither is
   found, report `POH_TIMEOUT` at index 0.

For `revert_on_error == false` batches:

- Current system behavior assumes 1 transaction per batch and reports index 0 only.

### Backpressure and Drop Semantics (Observed)

Validator may coalesce results; Node accepts any group size, including an empty no-op group. Delivery is best-effort,
not exactly-once:

- Validator receive overflow drops an entire outer batch group without results. Its outbound queues may independently
  drop `LeaderState` or results.
- Node batch dispatch blocks on its per-validator stream queue. Heartbeat enqueue failure disconnects the validator;
  ping enqueue failure is logged without immediately terminating the stream.
- The graph payload is already `SentToValidator` before the message callback. A
  failed enqueue may therefore already have forwarded telemetry and follows the
  sent-work `RetryableNextSlot` policy above rather than being immediately
  requeued.
- Node result-ingress overflow may drop individual results without disconnecting the stream.

Node-side code tolerates missing results by resending the underlying work under a new `seq_id` on later sends/slot
boundaries.

## Node: Result Handling and Retry Policy (Observed)

Node correlates results by `seq_id` to the forwarded batch.

- Unknown `seq_id` results and results from a validator other than the pending batch's expected validator are ignored
  without consuming the pending entry. Once both values match, Node removes and consumes the pending entry even if a
  later auction-generation or DAG check fails.
- After that match, Node updates its slot-local rejection cache and emits validator-result callbacks before checking the
  auction generation or DAG node. `Committed` records every pending transaction signature as already processed;
  `NotCommitted::TransactionError::AlreadyProcessed` records the indexed signature when the index is in range.
- A stale-auction result or a result whose DAG node has already been cleaned up is ignored only for DAG-state mutation;
  the pending-entry removal, rejection-cache update, and result callbacks have already occurred.
- For a result that passes the auction-generation and DAG-node checks, any present `Committed` value marks the DAG item
  processed; Node does not validate the result count against the pending batch.
- For such a current result, retryable `NotCommitted` recovers the sent payload under the `RetryableNextSlot` policy
  above; non-retryable `NotCommitted` marks the DAG node not processed/dropped.

Retryability decision:

1. Any `SchedulingError`, including an unrecognized numeric value, is retryable.
2. If `NotCommitted.reason` is `TransactionError`, retryability depends on the specific error.
   Retryable set includes: `AccountInUse`, `WouldExceedMaxBlockCostLimit`,
   `WouldExceedMaxVoteCostLimit`, `WouldExceedMaxAccountCostLimit`, `WouldExceedAccountDataBlockLimit`,
   `WouldExceedAccountDataTotalLimit`.
3. If `NotCommitted.reason` is `DeserializationError` or `GenericInvalid`, it is not retryable.
4. Missing result/reason fields and unrecognized transaction-error values are not retryable.
