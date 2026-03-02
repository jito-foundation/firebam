# BAM Validator Client (jito-solana) <-> BAM Node (bam repo) Clean-Room Specification

This document specifies the on-the-wire protocol and observed behavioral contract between:

- The validator-side BAM client in this repo (`/home/eric/dev/jito-solana`).
- The BAM Node service in `/home/eric/dev/bam` (the "bam-node").

It is written so an engineer can reimplement BAM support clean-room, without reading either codebase.

Observed from:

- jito-solana git commit `9a566139dade105211e144555192460add9f56cc`
- bam git commit `fbd2f5d8c3bb1cb5e4d850ae32079e80d46a4794`

## Terminology

- Node: The BAM Node gRPC server.
- Validator, Client: A Solana validator process with BAM enabled; it connects to Node over gRPC and executes
  Node-supplied batches.
- AtomicTxnBatch: Protobuf message `bam_types.AtomicTxnBatch { seq_id, max_schedule_slot, packets[] }`. Can be a single transaction, or a bundle.
- Batch packet: Protobuf message `bam_types.Packet { data, meta }`, where `data` is the serialized Solana transaction.
- revert_on_error batch: All packets in the batch have `PacketFlags.revert_on_error = true`. Execution is atomic (
  all-or-nothing for inclusion/recording).
- non-revert batch: `PacketFlags.revert_on_error = false`. In current node implementations, these are single-transaction
  batches.
- Leader bank (leader working bank): The bank from the PoH recorder shared leader state for the currently produced
  leader slot. Used for `LeaderState` emission and for execution in BAM workers.
- BankForks working bank: `bank_forks.working_bank()`. Used for parsing-time/scheduling-time bank checks (e.g.,
  `check_transactions`, fee payer checks), and as the `current_slot` fallback when there is no active leader bank.
- Root bank: `bank_forks.root_bank()`. Used for LUT resolution and reserved account key checks during parsing.
- Leader slot: A slot where the validator has an unfrozen leader bank and is producing entries.

## High-Level Behavior

1. Validator connects to Node via gRPC, authenticates by signing a Node-provided challenge, and fetches config.
2. While connected, validator updates its advertised TPU/TPU-forward addresses to point at Node-provided sockets, sends
   leader state updates while it is leader, receives `AtomicTxnBatch` messages from Node, validates and schedules them
   with account-aware conflict tracking, executes them against the active leader bank, and sends an
   `AtomicTxnBatchResult` per received batch back to Node.
3. When BAM is connected, the validator disables/bypasses normal transaction ingestion paths and executes only
   Node-supplied batches.

## Protobuf and gRPC API

The Node exposes `bam_api.BamNodeApi`:

1. `GetAuthChallenge(AuthChallengeRequest) -> AuthChallengeResponse`
2. `GetBuilderConfig(ConfigRequest) -> ConfigResponse`
3. `InitSchedulerStream(stream SchedulerMessage) -> stream SchedulerResponse`

Messages are protobuf3 and versioned via `oneof versioned_msg { v0 }`. Current implementations only accept `v0`.

### Stream Message Types (v0)

Validator -> Node (`SchedulerMessageV0.msg`):

- `AuthProof { challenge_to_sign, validator_pubkey, signature }`
- `ValidatorHeartBeat { time_sent_microseconds }`
- `LeaderState { slot, tick, slot_cu_budget_remaining }`
- `MultipleAtomicTxnBatchResult { results: [AtomicTxnBatchResult] }`
- `Pong { id }` (gRPC RTT telemetry; current validator does not send because it ignores `Ping`)

Node -> Validator (`SchedulerResponseV0.resp`):

- `BuilderHeartBeat { time_sent_microseconds }`
- `MultipleAtomicTxnBatch { batches: [AtomicTxnBatch] }`
- `Ping { id }` (gRPC RTT telemetry; current validator client ignores it)

## Authentication

### Label and Signed Bytes

Both Node and Validator use the same label prefix:

- `AUTH_LABEL = b"X_OFF_CHAIN_JITO_BAM_V1\\0"` (note the trailing NUL byte)

Signing/verification input:

1. Node issues `challenge_to_sign` as a UTF-8 string.
2. Validator forms `labeled_bytes = AUTH_LABEL || challenge_to_sign.as_bytes()`.
3. Validator signs `labeled_bytes` with the validator identity keypair (ed25519).
4. Validator sends `AuthProof` with `validator_pubkey` as a base58 pubkey string and `signature` as a base58 signature
   string (`Signature::to_string()` equivalent).

### Validator Requirements

- The first message sent on `InitSchedulerStream` MUST be `AuthProof`.
- If the validator identity changes at runtime, the validator MUST reconnect and re-authenticate (auth is tied to the
  identity key).

Observed validator connect/auth ordering:

1. Establish gRPC channel.
2. Start `InitSchedulerStream` (bi-di stream).
3. Call `GetAuthChallenge` (unary).
4. Send `AuthProof` as the first stream message.

### Node Requirements (Observed)

Node accepts `AuthProof` only if:

- The connection is not already authenticated (node accepts `AuthProof` only once per stream).
- `validator_pubkey` parses, and signature verifies for `labeled_bytes`.
- The challenge is known (previously issued by `GetAuthChallenge`) and not reused (challenge is removed on first use).
- The validator is on leader schedule and not disabled by policy.
- The auth response passes Node freshness checks. The validator MUST send `AuthProof` within
  `WAIT_FOR_AUTH_DURATION = 2s` of starting `InitSchedulerStream` (else Node disconnects). The challenge MUST be
  answered quickly: `now - challenge_creation_time <= max_ping_rtt_us` (microseconds). With Node defaults,
  `max_ping_rtt_us = 30_000us` so the default challenge freshness limit is `30_000us` (30ms).
- Node MAY reject new connections that are "too close" to the validator's next leader slots (within a configurable
  lookahead) unless running in a test-node mode.

If auth is not completed shortly after stream start, Node terminates the stream.

## Connection Liveness and Health

### Initial Connection Refusal (Observed)

Before authentication, the Node may refuse to start the scheduler stream:

- If the Node is not healthy, `InitSchedulerStream` returns `unavailable` ("Node is not healthy").
- If the Node is shutting down, `InitSchedulerStream` returns `unavailable` ("Node is shutting down").
- If the Node has recently disconnected the same client IP for high ping RTT, it may refuse reconnection for
  `interval_backoff_after_disconnect_on_max_ping_rtt` (default 5 minutes) by returning `failed_precondition` (high RTT
  message). After the backoff window expires, the Node clears the stored RTT entry and allows the connection attempt.

### Heartbeats

Node -> Validator:

- Node periodically sends `BuilderHeartBeat` on the stream.

Validator -> Node:

- Validator periodically sends `ValidatorHeartBeat` on the stream.

Validator side:

- Validator tracks the time since the last received Node heartbeat.
- If heartbeat is not received within a threshold, the connection is considered unhealthy and the validator reconnects.

Node side:

- Node tracks the time since the last received validator heartbeat.
- If validator heartbeat is not received within a threshold, Node disconnects.

### Ping/Pong

- The node MAY send `Ping { id }` for RTT telemetry (in practice it is attempted on the same cadence as node
  heartbeats).
- Observed node behavior: ping `id` is an incrementing, wrapping `u32`, and the node tracks at most one pending ping at
  a time per connection.
- The current validator implementation ignores/drops `Ping` in its gRPC inbound handler and therefore never sends
  `Pong` (despite `Ping`/`Pong` being defined in the proto).
- The node does not require `Pong` for correctness: if a `Pong` is not received within a timeout, the node records a
  timeout RTT datapoint and continues.
- Observed ping timeout: `ping_timeout = max(1us, 2 * max_ping_rtt_us)`. With defaults (`max_ping_rtt_us = 30ms`),
  `ping_timeout = 60ms`.
- If a `Pong` arrives with no pending ping or with a mismatched `id`, the node records telemetry and continues (it does
  not disconnect).
- The node's disconnect policy for "high RTT" is based on its network-canary mean ping RTT when available; the gRPC
  `Ping`/`Pong` RTT is recorded separately and is currently telemetry-only.
- A clean-room client MAY implement `Pong` by echoing the received `Ping.id` as `Pong.id` to improve RTT
  measurement/telemetry.

### Other Node Disconnect Conditions (Observed)

In addition to heartbeat timeouts and explicit RPC errors, the node may terminate the scheduler stream for policy/health
reasons, including:

- Not authenticating within `WAIT_FOR_AUTH_DURATION` (2s).
- The validator becoming disabled by policy, or no longer being on the leader schedule.
- A newer authenticated connection replacing an existing one for the same validator pubkey (the older stream terminates
  with "Connection replaced").
- The node reporting itself unhealthy, unless the validator is within `buffer_slot_lookahead` slots of an upcoming
  leader slot.
- Sustained high mean network ping RTT above `max_ping_rtt_us` (default 30ms) for
  `interval_before_disconnect_on_max_ping_rtt` (default 60s), unless the validator will be leader within
  `min_slots_before_leader_for_disconnect` (default 10 slots).
- If the node is shutting down, disconnecting validators whose next leader slot is farther than `buffer_slot_lookahead`
  slots away.

## Configuration (GetBuilderConfig) and Validator Effects

`ConfigResponse` contains:

- `block_engine_config { builder_pubkey, builder_commission }`
- `bam_config { prio_fee_recipient_pubkey, commission_bps, tpu_sock, tpu_fwd_sock }`

Validator applies config live:

1. TPU sockets:
   If `bam_config.tpu_sock` is present, update the validator's advertised TPU address in gossip (ClusterInfo). If
   `bam_config.tpu_fwd_sock` is present, update the validator's advertised TPU-forward address in gossip.
   Observed parsing behavior: the current validator parses `Socket.ip` as an IPv4 literal (`Ipv4Addr::from_str`);
   non-IPv4 values (including hostnames and IPv6 literals) are ignored.
2. Builder identity/commission:
   Parse `builder_pubkey` and store it. `builder_commission` is a percent (0-100); values > 100 are rejected. Stored
   builder info is used by tip-program maintenance.
3. Priority fee recipient:
   Parse and store `prio_fee_recipient_pubkey`. (Current validator stores it but does not otherwise use it.)
4. `commission_bps`:
   Currently ignored by the validator.

The validator only reports itself as fully "Connected" when it is both heartbeat-healthy and has received at least one
`ConfigResponse` (it does not require that all config fields parse successfully).

Implementation notes:

- If `builder_pubkey` fails to parse, the validator logs an error and resets the stored builder pubkey to
  `Pubkey::default()`.
- If `builder_commission > 100`, the validator logs an error and ignores that commission value (leaving the prior value
  unchanged).

## BAM URL Configuration (Validator)

This spec is primarily the validator<->node contract, but clean-room implementations typically need the validator-side
configuration surface as well.

Observed validator behavior:

- Startup flag: `--bam-url <URL>` enables BAM. If omitted, BAM remains disabled.
- URL normalization (CLI path): If the URL has no scheme, default scheme is `http`.
- URL normalization (CLI path): Supported schemes are `http` and `https` only.
- URL normalization (CLI path): Default port if missing is `50055` for `http`, `50056` for `https`.
- URL normalization (CLI path): Host must be non-empty.
- URL normalization (CLI path): Trailing `/` is trimmed unless the input ended with `/` (path is preserved).
- Runtime update: admin RPC method `setBamUrl` accepts an optional string.
- Runtime update: `null` disables BAM.
- Runtime update: `""` (empty/whitespace) disables BAM and is treated as a manual disconnect.
- Runtime update: non-empty values must parse as a `tonic::transport::Endpoint`.
- Runtime update: Unlike the CLI path, `setBamUrl` does *not* apply scheme/port defaults; callers should provide a
  fully-qualified URI compatible with `Endpoint::from_str`.
- When the BAM URL is changed or cleared at runtime, the validator transitions to `Disconnected` and (if a new URL is
  set) reconnects and re-authenticates.

## Leader State (Validator -> Node)

### Purpose

`LeaderState` is how the validator tells the Node "I am currently producing a leader slot and here is my intra-slot
progress and remaining compute budget." The Node uses it for slot targeting and pacing.

### When It Is Sent (Observed)

While the gRPC stream is up, the validator emits `LeaderState` only when:

- A `working_bank` exists in the shared leader state, and
- That bank is not frozen.

The validator emits `LeaderState` frequently (approximately every 5ms while leader, based on a tight loop plus a
`sleep(5ms)`).

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

## BAM Overrides Inside the Validator

When `bam_enabled == Connected`, the validator bypasses normal transaction intake and block-engine intake:

- TPU packet forwarding from the network is disabled and drained/dropped.
- Block Engine streaming is suppressed/terminated when `bam_enabled == Connected` (the Block Engine stage exits its
  consume loop and idles). During `Connecting`, the Block Engine stage can still run normally; however, Block Engine
  stage errors are mapped to a `BamEnabled` sentinel error and suppressed when `bam_enabled != Disconnected`.
- The normal non-vote scheduler stops scheduling.
- A dedicated BAM scheduler/controller becomes the only active non-vote scheduler.

Net effect: when BAM is connected, non-vote transaction execution is driven by Node-supplied `AtomicTxnBatch` messages (
plus validator-internal maintenance work such as tip-program bundles, and independent vote processing).

### bam_enabled State Machine (Observed)

`bam_enabled` is a shared atomic state with these observed values:

- `Disconnected` (0): BAM is inactive; the normal non-vote scheduler is enabled.
- `Connecting` (1): BAM connection is being established and/or config has not yet been received. The normal non-vote
  scheduler is still enabled and the BAM scheduler is disabled; successfully parsed BAM batches may be dropped without
  producing results. Block Engine ingestion is not actively suppressed until `Connected` (see override notes above).
- `Connected` (2): BAM connection is heartbeat-healthy and at least one `ConfigResponse` has been received. The BAM
  scheduler is enabled, the normal non-vote scheduler is disabled, and TPU packet forwarding is drained/dropped.

Observed transitions:

- `Disconnected` -> `Connecting`: when a BAM URL is configured and a connection attempt begins.
- `Connecting` -> `Connected`: once the connection is healthy and config has been fetched.
- Any -> `Disconnected`: on unhealthy connection, URL change, identity change, etc.; reconnect attempts back off for ~
  1s.

## Node -> Validator: AtomicTxnBatch Delivery

Node sends:

- `SchedulerResponseV0.resp = MultipleAtomicTxnBatch { batches: [...] }`

Each `AtomicTxnBatch` contains:

- `seq_id: u32` (Node-generated identifier used for both ordering and correlation of results. In the current node
  implementation it is a single monotonically increasing counter over the node process lifetime (not per slot/leader
  rotation) and must be globally unique across all in-flight batches because the node correlates results by `seq_id`
  alone.)
- `max_schedule_slot: u64` (slot for which the batch is valid)
- `packets: repeated Packet`

Observed Node behavior:

- TPU and Block Engine "packet stream": `packets.len() == 1`, `revert_on_error == false`.
- Block Engine "bundle stream": `1 <= packets.len() <= 5`, `revert_on_error == true`.

Node sets:

- `seq_id` as a monotonically increasing counter per forwarded batch.
- `max_schedule_slot` to the leader slot it is currently targeting for that validator.

## Node: Bundle Intake, Validation, Priority, and Forwarding (Observed)

This section summarizes node-side behaviors that materially affect what the validator receives. It is not required to
implement a validator-side client, but is useful for end-to-end clean-room reimplementations.

### Ingress Normalization

Node ingests from multiple sources and normalizes each input into `PacketBatchWithMeta`:

- Fields: `PacketBatchWithMeta { batch: AtomicTxnBatch, revert_on_error: bool, simulated_tip_and_cu: Option<(u64,u32)>, entrypoint }`.
- Sources (observed):
  - Block Engine bundles: `revert_on_error = true`, `1 <= packets.len() <= 5`, may have `simulated_tip_and_cu`.
  - Block Engine packets: `revert_on_error = false`, `packets.len() == 1`.
  - TPU packets: `revert_on_error = false`, `packets.len() == 1`.

When the node (re)serializes packets into an `AtomicTxnBatch`, it sets each `Packet.meta` as:

- `meta.size = packet.data.len()` (u64)
- `meta.flags.revert_on_error = <batch revert_on_error>`
- `meta.flags.simple_vote_tx = false`

### Bundle Construction and Deduplication

Node converts the incoming packet bytes into an internal `Bundle` by:

1. Deserializing each packet into a sanitized transaction view and then a runtime transaction.
2. Resolving v0 address lookup tables against cluster state (finalized slot), using the cluster state's reserved account
   keys. LUT resolution rejects lookup failures and rejects any loaded address that is in the node's
   `blacklisted_accounts` set.
3. Rejecting intra-bundle duplicates by message hash: if two transactions have the same message hash, the bundle is
   rejected (`DuplicateTransaction`).
4. Extracting durable-nonce usage if present.
5. Computing per-transaction fee and cost estimates:
   - Fee = signature fee (`5000 lamports * total_signatures`) + prioritization fee (from compute-budget limits).
   - Cost = cost-model estimate sum.
6. Computing `bundle_id = SHA256(concat(first_signature(tx_0), ..., first_signature(tx_n)))` for deduplication.

Node maintains a `seen` set of `bundle_id` and drops already-seen bundles (it does not enqueue them).

### Node-Side Validation

Node applies two layers of validation before enqueueing:

Static validation:

- Enforce per-transaction account key count <= max (64, or `MAX_TX_ACCOUNT_LOCKS` when the corresponding feature is
  active).
- Reject vote transactions (node runs with `only_non_votes = true`).
- Reject if any static account key is in the node's `blacklisted_accounts` set. (Loaded LUT addresses are checked
  earlier during bundle construction.)

Dynamic validation (against the node's highest BankForks view):

- Blockhash / nonce validity:
  - For non-nonce transactions: require recent blockhash to be valid for age (`MAX_PROCESSING_AGE`).
  - For nonce transactions: perform a weak nonce-account check (if the nonce account is missing, the check is skipped;
    if present, it validates nonce-account owner/state/authority and nonce value).
- Reject if any transaction's first signature is already processed (node calls `any_signature_processed` over
  `tx.signatures().first()` for each transaction).
- Fee payer solvency ("weak"): only the first transaction's fee payer is checked; if the account is missing, the check
  is skipped.

Only bundles passing all checks are eligible for forwarding.

### Priority and Queue Ordering

For valid bundles, node computes:

- `priority = (total_fees_lamports + simulated_tip_lamports) * 1_000_000 / total_cost`
- If `total_cost == 0`, priority is 0 (checked division).

Node queues bundles in a max-heap keyed by:

1. `revert_on_error` (true sorts above false)
2. `priority` (higher first)
3. `bundle_id` (tie-breaker)

### Forwarding to Leaders

On each send cycle, node:

1. Selects a connected leader in the range `local_current_slot..=local_current_slot+1` (one-slot fall-behind leeway).
2. Computes `slot_to_target = max(leader_state.slot, leader_schedule_slot)` and only sends if at least one of
   `local_current_slot >= leader_schedule_slot` or `leader_state.slot >= leader_schedule_slot` holds.
3. On target-slot change: if the chosen `leader_schedule_slot` differs from the last targeted slot (tracked as
   `last_send_slot`), spills retryable bundles back to the ready heap and copies pending-without-result bundles back to
   the ready heap (both gated by dynamic revalidation).
4. Pops best-ready bundles, reruns dynamic validation, serializes to `AtomicTxnBatch` if needed, and sets:
   - `seq_id = next_seq_id` (monotonic u32 counter, incremented with `saturating_add(1)` after each send)
   - `max_schedule_slot = leader_schedule_slot` (the slot chosen for forwarding; typically equals `slot_to_target`)
5. Emits `MultipleAtomicTxnBatch` messages containing up to 8 independent `AtomicTxnBatch` entries, and tracks pending
   results by `seq_id` until a result is received (or the entry is copied back on a later slot boundary).

Result handling and retry policy are described in "Node: Result Handling and Retry Policy (Observed)" below.

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

### Signature Verification (CPU)

Validator signature-verifies all packets in the received batches using CPU sigverify:

1. Convert each proto `Packet` to a `solana_packet::Packet` buffer.
2. Run `ed25519_verify_cpu` over packet batches.
3. For any packet flagged discard by sigverify:
   Reject the batch with `DeserializationErrorReason::SANITIZE_ERROR` for the failing packet index.

Packet-to-`solana_packet::Packet` conversion details (observed):

- Copy length is `min(PACKET_DATA_SIZE, packet.data.len())`.
- `solana_packet.meta.size` is set from proto `meta.size` if present (else defaults to `packet.data.len()`).
- If proto `meta.flags.simple_vote_tx == true`, `solana_packet.meta.flags` includes `SIMPLE_VOTE_TX` for sigverify
  purposes.

### Per-Transaction Parsing and Bank-Front-Run Checks

For each packet in the batch, validator performs the following checks in order.
Any failure rejects the entire batch, with the failing transaction index in the reason.

1. Vote-only mode reject:
   If the BankForks working bank is vote-only, reject with `DeserializationErrorReason::SANITIZE_ERROR`.
2. Basic sanitization:
   Parse packet bytes to a sanitized transaction view. Sanitization is parameterized by the root bank feature flag
   `static_instruction_limit`. Failures map to `DeserializationErrorReason::SANITIZE_ERROR`.
3. Reject vote transactions:
   If `is_simple_vote_transaction == true`, reject with `DeserializationErrorReason::VOTE_TRANSACTION_FAILURE`.
4. Resolve address lookup tables (v0 only):
   Load LUT addresses from the root bank using the transaction's address table lookups, then convert to a resolved
   transaction view using the root bank's reserved account keys. Failure maps to
   `DeserializationErrorReason::SANITIZE_ERROR`.
5. Validate account locks per transaction:
   Enforce "no duplicate accounts" and the per-transaction account lock limit (limit is read from the working bank).
   Failure maps to `NotCommitted.reason = TransactionError` with the mapped `TransactionErrorReason`.
6. Validate compute budget instructions:
   Extract and sanitize compute budget limits using the working bank feature set. Failure maps to
   `TransactionErrorReason` as above.
7. Bank `check_transactions`:
   Run `working_bank.check_transactions(..., MAX_PROCESSING_AGE, ...)`, which (in this codebase) re-checks compute
   budget limits, validates blockhash age / nonce validity, and checks the status cache (`AlreadyProcessed`). Failure
   maps to `TransactionErrorReason`.
8. Fee payer solvency:
   Verify the fee payer can pay fee and required rent. Failure maps to `TransactionErrorReason`.
9. Blacklisted accounts:
   If any account key is in the validator-provided blacklist, reject. In this codebase the blacklist includes the
   tip-payment program id (`tip_manager.tip_payment_program_id()`). Failure maps to
   `TransactionErrorReason::SANITIZE_FAILURE`.

Derived values computed during parsing:

- MaxAge for each transaction: `sanitized_epoch = root_bank.epoch()` and
  `alt_invalidation_slot = estimate_last_valid_slot(min(deactivation_slot, root_bank.slot()))`.
- Legacy transactions: `deactivation_slot` is treated as `u64::MAX`, so
  `alt_invalidation_slot = estimate_last_valid_slot(root_bank.slot())`.
- Cost estimate: compute via cost model; sum across transactions to form a batch cost. Current BAM scheduler does not
  use this for admission control.

### Batch Priority

Validator priority for BAM batches is derived solely from `seq_id`:

- `priority = u64::MAX - seq_id`

Smaller `seq_id` means higher priority. Node controls ordering by assigning `seq_id`.

### Buffering Behavior

Validated batches are inserted into a bounded container as one batch entry plus N transaction entries.

The container admission rule is all-or-nothing for a batch:

- A batch consumes `1 + packets.len()` container entries (1 batch header + 1 entry per transaction). Admission requires
  `current_len + (1 + packets.len()) <= slab_capacity` (exact-fit is allowed). Equivalently, admission rejects only
  when `entries_required > available_entries`. If it cannot be admitted, the batch is rejected with
  `SchedulingError::CONTAINER_FULL`.
- Unlike single-transaction insertion in normal scheduling, batch insertion does not evict lower-priority entries to
  make room.
- The container is instantiated with nominal capacity `TOTAL_BUFFERED_PACKETS = 100_000`, but internally allocates
  `EXTRA_CAPACITY = 64` additional slab entries. BAM batch admission uses the underlying slab capacity, not the nominal
  capacity.

### Non-Leader Phases

If the validator is not in a leader consume/hold phase (decision is Forward or ForwardAndHold):

- Any buffered batches are flushed with `SchedulingError::OUTSIDE_LEADER_SLOT`.
- Any newly received batches during that phase are also responded to as `OUTSIDE_LEADER_SLOT` (observed: the
  implementation drains for ~100ms to avoid backlog buildup).

## Validator Scheduling: Priority Graph and Slot Boundaries

### Scheduling Unit

The scheduler treats the batch as the scheduling unit:

- One prio-graph node per batch.
- One scheduler-to-worker message per scheduled prio-graph node (current implementation).

### Slot Tracking and Bank Boundaries

The scheduler tracks `slot: Option<Slot>` derived from the banking decision:

- If decision includes a bank, `slot = Some(bank.slot())`.
- Otherwise `slot = None`.

On any slot change:

1. If `slot` becomes None, drain and reject all buffered work as `OUTSIDE_LEADER_SLOT`.
2. Unblock any nodes held by in-flight batches from the prior slot.
3. Drain the prio-graph and reject remaining work as `OUTSIDE_LEADER_SLOT`.
4. Clear prio-graph state.

### Prio-Graph Resource Model

Resources are account pubkeys.

For each transaction:

- For each `account_keys()[i]`: AccessKind is Write if `is_writable(i)` else Read.

For a batch:

- The resource access set is the concatenation of the above across all transactions in the batch.

Prio-graph conflict semantics (as used by the validator):

- A Write access conflicts with any prior Read or Write access on the same pubkey.
- A Read access conflicts only with a prior Write access on the same pubkey.

Insertion order requirement:

- Work must be inserted into the prio-graph in descending priority order; among conflicting work, earlier insertion
  wins. The current implementation satisfies this by popping from the container's max-priority queue and inserting
  immediately.

### Observed Scheduling Algorithm

Scheduling is performed as two phases on each pass.

#### Phase 1: Pull Into Prio-Graph

1. While `container.pop()` yields a next batch (highest `priority` first), load the batch's transaction ids and its
   `max_schedule_slot`.
2. If `batch.max_schedule_slot < current_slot`, respond `NotCommitted(SchedulingError::OUTSIDE_LEADER_SLOT)` for that
   batch and remove it from the container.
3. Extra bank checks (enabled by default): run `BankForks.working_bank.check_transactions` over the batch transactions
   with `MAX_PROCESSING_AGE`. If any transaction fails, respond `NotCommitted(TransactionError { index, reason })` using
   the first failing index, then remove the batch from the container.
4. Insert the batch into the prio-graph as one node, with its account access set computed as the union of read/write
   accesses across all transactions in the batch.

#### Phase 2: Schedule From Prio-Graph

1. While the prio-graph has an unblocked node, pop the highest-priority unblocked batch node id.
2. Re-check the slot window: if `batch.max_schedule_slot < current_slot`, respond `NotCommitted(OUTSIDE_LEADER_SLOT)`,
   `prio_graph.unblock(batch_node)`, and remove it from the container.
3. Extra bank checks (enabled by default): re-run `BankForks.working_bank.check_transactions` on the batch. If any
   transaction fails, respond `NotCommitted(TransactionError { index, reason })`, `prio_graph.unblock(batch_node)`, and
   remove it from the container.
4. Create and send a `ConsumeWork` to a worker, with `transactions[]` equal to the batch transactions (moved out of the
   container for scheduling), `max_ages[]` equal to the per-transaction `MaxAge` values computed at parse time,
   `revert_on_error` copied from the batch, `respond_with_extra_info = true`, and
   `max_schedule_slot = Some(current_slot)` (note: set to the scheduler's current bank slot, not to
   `batch.max_schedule_slot`).
5. Record the batch as in-flight so completion can be mapped back to its original `seq_id` and so prio-graph
   dependencies can be released on completion.

#### Completion Mapping

1. When a worker finishes, the scheduler maps the worker output to a single `AtomicTxnBatchResult` for the original
   `seq_id`.
2. If still in the same slot, the scheduler calls `prio_graph.unblock(batch_node)` to release dependent work.
3. The scheduler removes the batch and all its transactions from the container.

## Validator Execution: Bank and PoH Interaction

### Worker Entry Conditions

Workers obtain the active leader bank from shared leader state.

Observed definition of "active leader bank" in workers:

- `shared_leader_state.load().working_bank()` must exist, and
- `working_bank.is_complete() == false` (if complete, treat as no active bank).

Note: This differs slightly from `LeaderState` emission, which checks `!bank.is_frozen()`.

If no active leader bank is available within 50ms (spin-wait timeout), the work is returned as not committed; reporting
treats this as `SchedulingError::POH_TIMEOUT`.

Observed implementation detail: when this happens, the worker uses a "retry drain" path that marks the current work
item and all currently queued work items on that worker's input channel as retryable `POH_TIMEOUT`.

If `work.max_schedule_slot < bank.slot()` at execution time:

Work is not executed; it is returned as not committed; reporting treats this as `POH_TIMEOUT`.

### Tip Program Maintenance (BAM Workers)

When tip-processing is enabled for the BAM workers, they may run tip-program maintenance bundles before executing the
Node-supplied work batch if:

- Any work transaction touches any known tip accounts, and
- Tip programs have not been run yet for the current slot.

Tip maintenance details (observed):

- It is guarded by a shared `last_tip_updated_slot`; tip maintenance is skipped when
  `bank.slot() == last_tip_updated_slot`.
- `last_tip_updated_slot` is updated only when tip maintenance completes successfully or when a crank-bundle generation
  error is logged and ignored. Failures (init/crank not fully committed, or missing builder pubkey) do not update the
  slot and can cause tip maintenance to be attempted again within the same slot.
- It may execute an "initialize tip programs" bundle and then a "crank tip programs" bundle.
- Both maintenance bundles are executed with `revert_on_error = true` and are separate from the Node-supplied batch.
- If tip maintenance fails, the worker logs an error but still proceeds to execute the Node-supplied work batch.

### Core Execution Call

Execution uses the standard banking stage consumer pipeline:

1. MaxAge prechecks:
   If `bank.epoch != max_age.sanitized_epoch`, verify reserved key constraints. If
   `bank.slot > max_age.alt_invalidation_slot`, attempt to re-resolve LUT addresses; failure drops the transaction.
2. QoS/cost model selection and cost tracker accounting.
3. Account locking with `bank.prepare_sanitized_batch_with_results(..., batched_locking = revert_on_error)`.
   Note: `batched_locking = true` (and/or the `relax_intrabatch_account_locks` feature) enables "relaxed intrabatch
   locks" semantics:
   transactions within the same work item may touch the same accounts without *intrabatch* `AccountInUse` lock
   failures (conflicts with already-locked accounts in other in-flight work can still fail), and duplicate message
   hashes within the batch are rejected as `AlreadyProcessed` (SIMD83 behavior).
4. `bank.load_and_execute_transactions(...)`.
   Execution order note: the current runtime executes the transactions sequentially in the input order (not parallel),
   because transactions in the same batch may modify the same accounts (SIMD83 behavior).
5. Record to PoH:
   Transactions are partitioned into sequential non-conflicting entry batches for recording (splitting on contention,
   without reordering).
6. Commit:
   Commit results are generated per transaction.

### revert_on_error Semantics (Atomicity)

When `revert_on_error == true`, atomicity is enforced at two points:

1. Lock stage:
   If any transaction fails account locking, the entire work is treated as not committed. The failing transaction
   carries its lock error; others are `CommitCancelled`.
2. Execute stage:
   If any transaction does not execute successfully (including instruction error), the entire work is treated as not
   committed. The failing transaction carries its execution error; others are `CommitCancelled`.

When `revert_on_error == false`, transactions can be partially committed; in current Node usage, these batches are
single-transaction.

## Construction of AtomicTxnBatchResult

### Result Shape

Validator returns exactly one `AtomicTxnBatchResult` per received `AtomicTxnBatch`, keyed by `seq_id`.
Note: the validator *attempts* to produce one result per batch, but due to internal best-effort drops (bounded
channels + `try_send`) a clean-room implementation must tolerate missing results (see Backpressure and Drop Semantics).

`AtomicTxnBatchResult.result` is one of:

- `Committed { transaction_results: [TransactionCommittedResult] }`
- `NotCommitted { reason: oneof(TransactionError, SchedulingError, DeserializationError, GenericInvalid) }`

### TransactionCommittedResult Fields

For each committed transaction, validator reports:

- `cus_consumed` (u32)
- `feepayer_balance_lamports` (u64, post-balance)
- `loaded_accounts_data_size` (u32)
- `execution_success` (bool, true iff execution returned Ok)

Important: a transaction can be committed with `execution_success = false` (fees are paid and the transaction is
recorded even if it errors at execution time).
Note: for `revert_on_error == true` batches, the validator only commits if *all* transactions execute successfully, so
`execution_success` is effectively always `true` in the `Committed` case.

### NotCommitted Reasons

Scheduling errors:

- `OUTSIDE_LEADER_SLOT`: validator not in leader consume/hold, or batch schedule window stale.
- `CONTAINER_FULL`: local buffer capacity could not admit the batch.
- `POH_TIMEOUT`: catch-all for "bank not available", expired schedule slot at worker, or PoH recording/commit failure.
  Observed: internal commit/recording failures (ChannelFull, ChannelDisconnected, MaxHeightReached) are also reported as
  `POH_TIMEOUT`.

Transaction errors:

- Reported as `TransactionError { index, reason }`.
- `index` is the 0-based transaction index within the batch.
- `reason` is a mapped `TransactionErrorReason` derived from the validator's internal `TransactionError`.

#### TransactionErrorReason Mapping (Observed)

Validator-side mapping from `solana_transaction_error::TransactionError` to proto `TransactionErrorReason`:

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

## Validator -> Node: Outbound Delivery

Validator sends results over the scheduler stream as:

- `SchedulerMessageV0.msg = MultipleAtomicTxnBatchResult { results: [...] }`

Validator MAY coalesce multiple results before sending.

Validator also sends:

- `LeaderState` while in an active leader slot.
- `ValidatorHeartBeat` periodically regardless of leader status.

### Backpressure and Drop Semantics (Observed)

The validator-side implementation is best-effort at several internal boundaries; clean-room reimplementations should
assume the wire contract is not exactly-once:

- Node -> Validator delivery is not backpressured end-to-end:
  The gRPC receive loop forwards each received `AtomicTxnBatch` into a bounded channel (`capacity = 100_000`) using a
  non-blocking `try_send`. If the channel is full, the batch is dropped locally and no `AtomicTxnBatchResult` is
  produced for it.
- Validator -> Node delivery is also best-effort:
  `LeaderState` and `AtomicTxnBatchResult` are enqueued into bounded channels using non-blocking `try_send`. If these
  channels are full, updates/results can be dropped.
- gRPC outbound also uses non-blocking sends:
  the BAM connection's outbound task uses a bounded tokio mpsc channel and `try_send`, so even after results are
  produced they may be dropped before reaching the wire.

Node-side code tolerates missing results by resending the underlying work under a new `seq_id` on later sends/slot
boundaries.

## Node: Result Handling and Retry Policy (Observed)

Node correlates results by `seq_id` to the forwarded batch.

Result lifecycle and correlation details (observed):

- The node stores sent bundles in `pending_result_bundles` keyed only by `seq_id`. When a result arrives, it removes the
  pending entry by `seq_id` (the validator pubkey in the result is not used for lookup).
- The node computes `same_slot = (forwarded_slot == last_send_slot)`, where `forwarded_slot` is the slot used when the
  bundle was sent (the leader schedule slot) and `last_send_slot` tracks the most recently targeted `slot_to_target`.
- When `same_slot == true`, retryable results are queued into the node's retryable queue, and non-retryable results
  (including committed) cause the bundle id to be removed from the node's `seen` set. When `same_slot == false`, the
  node assumes the bundle has already been copied back into the ready heap on a slot boundary and does not update the
  `seen` set or retryable queue in response to the result.

Retryability decision:

1. If `NotCommitted.reason` is `SchedulingError`, it is retryable.
2. If `NotCommitted.reason` is `TransactionError`, retryability depends on the specific error.
   Retryable set includes: `AccountInUse`, `ResanitizationNeeded`, `WouldExceedMaxBlockCostLimit`,
   `WouldExceedMaxVoteCostLimit`, `WouldExceedMaxAccountCostLimit`, `WouldExceedAccountDataBlockLimit`,
   `CommitCancelled`, `ClusterMaintenance`.
3. If `NotCommitted.reason` is `DeserializationError` or `GenericInvalid`, it is not retryable.

Node may requeue retryable work for resend, subject to its slot-targeting logic.

## Observed Defaults and Capacities

Validator-side (observed):

- gRPC connect timeout: 5s
- send validator heartbeat interval: 5s
- Node heartbeat health threshold: 6s
- connection metrics/health check interval: 25ms
- refresh builder config interval: 1s
- leader state emission loop sleep (while leader): 5ms
- outbound flush tick: 1ms
- batch result coalesce threshold: 24 results
- inbound batch channel capacity: 100,000
- outbound message channel capacity: 100,000
- gRPC outbound channel capacity (tokio mpsc): 100,000
- parsing burst: up to 128 AtomicTxnBatch per receive window
- parsing receive timeout: 1ms
- max packets per AtomicTxnBatch: 5
- BAM consume worker threads: 8
- worker active leader bank acquisition timeout: 50ms
- scheduler container nominal capacity: 100,000 entries (batches plus per-transaction entries), plus
  `EXTRA_CAPACITY = 64` internal slab headroom
- reconnect backoff (after disconnect/unhealthy): 1s
- wait loop sleep (during initial "healthy + config" wait): 10ms

Node-side (observed):

- send node heartbeat interval: 2s
- validator heartbeat timeout: 10s
- auth challenge cleanup TTL: 5s
- validator must authenticate within 2s of stream start
- auth challenge freshness limit: `max_ping_rtt_us` (defaults to 30ms)
- max_ping_rtt_us (RTT policy default): 30ms
- interval_before_disconnect_on_max_ping_rtt (RTT policy default): 60s
- interval_backoff_after_disconnect_on_max_ping_rtt (RTT reconnect backoff default): 5m
- min_slots_before_leader_for_disconnect (RTT policy default): 10 slots
- leader state slot validity window: current_slot +/- 5 slots
- max batches per MultipleAtomicTxnBatch message: 8
- commission_bps returned in config: 300 (3%)

## Clean-Room Implementation Checklist

To implement validator-side BAM support compatible with current bam-node behavior:

1. Implement gRPC client for BamNodeApi:
   GetAuthChallenge, GetBuilderConfig, and bidirectional InitSchedulerStream.
2. Implement AuthProof signing exactly:
   Use AUTH_LABEL with trailing NUL and sign AUTH_LABEL || challenge_bytes with validator identity keypair.
3. Maintain connection health based on Node BuilderHeartBeat.
4. Refresh config and apply it:
   Advertise Node-provided TPU sockets and store builder pubkey and commission.
5. When connected, disable non-BAM transaction ingestion and block-engine ingestion.
6. Receive AtomicTxnBatch and enforce:
   Slot window and batch constraints, CPU signature verification, and sanitize + LUT resolution + bank checks +
   blacklist filter.
7. Derive batch priority from seq_id (u64::MAX - seq_id) and buffer batches atomically.
8. Schedule with account-aware conflict tracking (prio-graph or equivalent).
9. Execute against the active leader bank (from shared leader state) with revert_on_error atomic semantics.
10. Emit exactly one AtomicTxnBatchResult per batch and send it back (optionally coalesced).
11. Send LeaderState while leader and send ValidatorHeartBeat periodically.

## Compatibility Notes

- `Ping`/`Pong` are defined in the protos on both sides, but the current validator client ignores inbound `Ping` and
  never sends `Pong`. Implementing `Pong` in a clean-room client is safe and may improve telemetry; the node does not
  require `Pong` for correctness.
- Protocol permits non-revert batches with multiple packets, but current Node sends single-packet non-revert batches.
  Validator result mapping assumes that invariant.
- Node seq_id generation must avoid reuse while a previous seq_id could still produce a result.

## Appendix: Traceability (Non-Normative)

This spec was derived by tracing behavior in these sources.

jito-solana:

- `core/src/bam_connection.rs`
- `core/src/bam_manager.rs`
- `core/src/bam_dependencies.rs`
- `core/src/tpu.rs`
- `validator/src/commands/bam/mod.rs`
- `validator/src/admin_rpc_service.rs`
- `core/src/proxy/fetch_stage_manager.rs`
- `core/src/proxy/block_engine_stage.rs`
- `core/src/banking_stage.rs`
- `core/src/banking_stage/transaction_scheduler/scheduler_controller.rs`
- `core/src/banking_stage/transaction_scheduler/bam_receive_and_buffer.rs`
- `core/src/banking_stage/transaction_scheduler/bam_scheduler.rs`
- `core/src/banking_stage/transaction_scheduler/bam_utils.rs`
- `core/src/banking_stage/transaction_scheduler/transaction_state_container.rs`
- `core/src/banking_stage/consumer.rs`
- `core/src/banking_stage/consume_worker.rs`
- `jito-protos/bam-protos/bam_api.proto`
- `jito-protos/bam-protos/bam_types.proto`

bam:

- `node/src/validator_service.rs`
- `node/src/simple_forwarder.rs`
- `node/src/tpu.rs`
- `node/src/blockengine_connection.rs`
- `node/src/utils/connected_leader_schedule.rs`
- `bundle/src/bundle.rs`
- `jito-protos/jss-protos/bam_api.proto`
- `jito-protos/jss-protos/bam_types.proto`
