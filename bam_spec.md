# BAM Validator Client (jito-solana) <-> BAM Node (bam repo) Clean-Room Specification

This document specifies the on-the-wire protocol and observed behavioral contract between:

- The validator-side BAM client in the separate `jito-solana` repo.
- The BAM Node service in `bam` (the "bam-node").

It is written so an engineer can reimplement BAM support clean-room, without reading either codebase.

Source review: 2026-10-30, against:

- [jito-solana git commit
  `5ead20530340ab5b5d354d2487207a2ec84c8b82`](https://github.com/jito-labs/jito-solana/tree/5ead20530340ab5b5d354d2487207a2ec84c8b82)
- bam git commit `ab0fbc3a7a6fa619409c50f065c1885d41dec65f`
- Shared `bam-protos` git commit `642c49ae6da23dcbc6c0c621288d4479799a8910` in both trees.

Validator-side observations below describe the Jito-Solana reference implementation. Node policy, validator internals,
and configurable defaults are identified separately from wire requirements; they are not claims about other validators'
implementation. BAM's embedded Agave/Solana dependencies separately pin `bam-node-validator` revision
`3cf0320fbfbfab0ee472e73fcf4fa8c53ff42012`; the validator checkout above is the behavioral reference, not that dependency
revision. This review traced source and existing tests; it did not run a cross-validator conformance suite.

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

### Message Limits and Optional Fields

- Node limits serialized protobuf request payloads to 16 KiB and response payloads to 4 MiB; these are per-message
  codec limits, not Rust object-memory limits or aggregate stream budgets. Oversize requests fail at the codec layer,
  separately from the `invalid_argument` count checks below.
- A `MultipleAtomicTxnBatchResult` may contain at most 24 results; each `Committed` may contain at most five transaction
  results. Exceeding either count closes the stream with `invalid_argument`. An empty results group is accepted.
- Current Node output contains at most eight `AtomicTxnBatch` entries per outer group. This is a Node packing limit,
  not an eight-result limit or a schema-wide limit on the repeated field.
- `MultipleAtomicTxnBatch.mss_padding` is protobuf bytes field 15. Receivers ignore its contents. Node pads the first
  message claimed for each leader window to at least 3,000 encoded bytes to avoid an idle-connection BBR delay; padding
  carries no work and changes no batch semantics. Older protobuf receivers can skip the unknown field.
- The reference validator limits `GetBuilderConfig` responses to 64 KiB before decoding. This separate limit does not
  reduce the scheduler stream's response limit.
- Proto3 scalar omission supplies zero/false. In particular, omitted `max_schedule_slot` means slot 0, not an unlimited
  deadline; omitted `seq_id` means 0, which is a usable correlation identifier. A present error reason whose enum field
  is omitted also uses numeric zero (`ACCOUNT_IN_USE` or `POH_TIMEOUT`), unlike an absent result/reason oneof.

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
- The challenge is known and unused. Node consumes it before age, leader-schedule-membership, and signature checks,
  so a failure at those later checks still makes it unusable. Pubkey parsing, disabled/ban policy, and the
  too-close-to-leader check precede consumption; their failures do not themselves consume it.
- The validator is on leader schedule and not disabled by policy.
- Node waits up to 2s for the first ordered stream message at its connection handler, and that message must
  authenticate. This timeout covers waiting for the message, not the subsequent synchronous proof validation.
- The challenge age does not exceed `max_ping_rtt_us` (default 30ms).
- The Node is `Running/Accepting` at authentication time. If it stops accepting after the stream opens, an
  `AuthProof` returns `unavailable`; leader lookahead does not exempt it. Admission is checked both before proof
  validation and again before publishing the authenticated connection. If draining wins the final check, authentication
  fails. If authentication wins, its connection-count reservation makes shutdown wait for it even if publication is
  delayed by contention.
- Except in test-node mode, the current Node rejects authentication when the validator is "too close" to its next leader
  slots (within the inclusive configurable lookahead).

## Connection Liveness and Health

### Process Status and Admission

- The Node's lifecycle states are `Initializing`, `Running/Accepting`, `Running/Shedding`, `Draining`, and `Exiting`.
  Validator health requires caught up, leader schedule ready, and startup ready; startup ready latches once set and
  moves the Node from `Initializing` to `Running`. Unhealthy validator health and maintenance each cause
  `Running/Shedding`; admission resumes only when both clear. Maintenance keeps the Node running and cannot change
  after shutdown begins. Shutdown enters `Draining` before `Exiting`.
- Catch-up compares the local optimistically confirmed slot with Votor's highest finalized certificate slot.
  Without that reference, or when the finalized slot is unchanged for more than 10 s, catch-up is unknown and unhealthy,
  lag reads `u64::MAX`, and the lag window restarts. With a reference, the instantaneous check uses the smaller of the
  last two recent lag samples against the configured threshold (the current sample alone if there is no recent prior
  sample). After a full 60 s window, mean lag must also be at most one slot. A sample gap over 5 s restarts that window.
- Maintenance and shutdown do not change validator health. `GET /api/v1/health/validator` and admin
  `isHealthy`/`isValidatorHealthy` report validator health; `GET /api/v1/health` returns 503 unless the Node is
  `Running/Accepting`, including during healthy maintenance.
- `GET /api/v1/health` does not track listener readiness; the HTTPS listener binds only after TLS is ready, possibly
  after the Node is `Running/Accepting`. Accept errors on either listener are logged and accepting continues. Losing
  either validator listener (bind or server failure, or the accept loop ending) starts a failed
  drain: the Node enters `Draining`, drains established streams as for shutdown, enters `Exiting` once no validators
  remain or after 600s, and exits nonzero so the service manager restarts it. A requested shutdown exits successfully.

### Initial Connection Refusal (Observed)

- `InitSchedulerStream` returns `unavailable` unless the Node is `Running/Accepting` ("Node is not healthy", "Node is
  in maintenance mode", "Node is shutting down", or "Node is exiting").
- A client IP recently disconnected for high RTT may receive
  `failed_precondition` until the configured reconnect backoff expires (default 5 minutes).

### Heartbeats

- Node sends `BuilderHeartBeat` every 2s; the validator reconnects after 6s without one.
- Validator sends `ValidatorHeartBeat` every 5s; Node disconnects it after 10s without one.
- Payload timestamps are informational. Liveness uses local receive time, so synchronized clocks are unnecessary.

### Ping/Pong

- Node may send `Ping`; the validator echoes its `id` in `Pong`.
- The protobuf comment calls for random IDs to resist anticipatory `Pong`s, but current Node code starts at 0 and
  increments a wrapping counter. This is an observed schema-comment/implementation disagreement; RTT policy is not
  evidence that an adversarial authenticated validator honestly waited for each ping.
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

- No first ordered message reaching the connection handler within 2s, or that message failing authentication.
- The validator becoming disabled, banned by the optional Autobahn enforcer policy, or absent from the leader schedule.
  Autobahn enforcement is disabled by default.
- A newer authenticated connection replacing an existing one for the same validator pubkey (the replaced stream
  terminates with "Connection replaced").
- For authenticated streams, unhealthy validator health at 2s heartbeat checks ("Node is unhealthy"), or maintenance or
  draining at 300ms drain checks ("Node is in maintenance mode" or "Node is shutting down"), subject to the inclusive
  leader-lookahead exemption. The stream stays connected through its leader window; maintenance ends this protection
  600s after it is first enabled (re-enabling does not reset the timer) without disabling maintenance. Draining ignores
  the maintenance cap; a requested shutdown is bounded externally by the service manager's stop timeout, and a failed
  drain by the Node's 600s exit deadline.
- Mean gRPC Ping/Pong RTT above the configured threshold at a periodic policy check, subject to a near-leader exception.

## Configuration (GetBuilderConfig) and Validator Effects

`ConfigResponse` contains:

- `block_engine_config { builder_pubkey, builder_commission }`
- `bam_config { prio_fee_recipient_pubkey, commission_bps, tpu_sock, tpu_fwd_sock, shred_socks[] }`

`ConfigRequest` is empty and unauthenticated. Node currently returns its builder identity/commission, public TPU and
shred endpoints, `commission_bps = 300`, and the builder pubkey as `prio_fee_recipient_pubkey`.

Validator applies config live:

1. Apply BAM TPU config only when both sockets parse as IPv4 with ports in `1..=65529`; otherwise preserve the prior
   pair. The upper bound leaves room for the `+6` QUIC port offset. The pair is retained across disconnects but selected
   only while `Connected`. Fetch stage prefers usable BAM sockets, then a healthy relayer, then original TPU sockets.
   It restores the prior selection if a gossip update fails.
2. Publish the builder pubkey and commission together as one immutable snapshot only when `builder_pubkey` parses to a
   non-default pubkey and `builder_commission` is in `0..=100`. Either invalid field, or missing `block_engine_config`,
   preserves the prior snapshot and reports failure to apply builder metadata.
3. Apply `prio_fee_recipient_pubkey` only if it parses. Ignore `commission_bps`.
4. For `shred_socks`, retain at most 32 valid unique IPv4 destinations with nonzero ports. Clear them on disconnect or
   missing `bam_config`. The validator sends produced shreds to them while leading and during retransmit shortly before
   its next consecutive leader run (1-4 slots).

Config polling starts after the first decoded inbound stream message and runs every second. Equal responses do not
republish metadata. A successful response plus a healthy heartbeat permits the Block Engine drain to begin; entering
`Connected` additionally requires the drain acknowledgement and successfully applying builder metadata. An invalid or
missing initial builder snapshot causes disconnect and fallback. Subsequent polling failures or invalid builder
updates preserve the last snapshot and do not by themselves disconnect an otherwise healthy session. Other config
fields may fail to parse independently.

A successfully received response counts as a fetched config even when either optional protobuf submessage is absent,
but missing `block_engine_config` cannot satisfy initial activation. Missing `bam_config` preserves the prior
TPU/TPU-forward pair and priority-fee recipient, while explicitly clearing the installed BAM shred destinations. Such
a response can still activate BAM if it contains valid builder metadata. Disconnect after cutover begins resets the
builder snapshot to its default so Block Engine must publish fresh metadata; a failed attempt still in `Connecting`
preserves Block Engine's snapshot.

## BAM URL Configuration (Validator)

- `--bam-url` and admin RPC `setBamUrl` share URL normalization: trim whitespace, accept `http` and `https`, supply
  `http` when the scheme is missing, and reject explicit port 0. Null or blank disables BAM.
- A URL whose parsed path is `/` names one Node. An omitted port defaults to 50055 for HTTP or 50056 for HTTPS;
  explicit ports, including 80 and 443, are preserved.
- A URL whose parsed path differs from `/` names a registry node-list document. It keeps ordinary HTTP(S) port
  behavior instead of receiving a BAM default port. Discovery fetches the list and selects a Node; this is validator
  discovery policy, not another `BamNodeApi` RPC.
- A change in the effective selected Node disconnects the current session, reconnects, and re-authenticates. Changes
  during connection setup cancel the stale attempt. A configured registry URL change rebuilds discovery; a direct URL
  that normalizes to the existing URL need not reconnect.

## Leader State (Validator -> Node)

`LeaderState` reports the validator's view that it is currently producing a leader slot, together with its intra-slot
progress and remaining compute budget. The Node range-checks its `slot` and stores the latest value on the connection.
Before queueing speculative work, the Node rejects the message and closes dispatch for the target slot if that
validator has already reported a later slot. This check can race with a state update. Slot targeting, the Alpenglow
record deadline, and cost pacing use the Node's bank and leader schedule (see Speculative Execution and Dispatch); tick
and compute budget remain informational.

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

- Valid range: `[current_slot - 5, current_slot + 5]` (inclusive; arithmetic saturates at the `u64` bounds).
- If outside that range, Node records the violation and drops the snapshot.
- `tick` and `slot_cu_budget_remaining` are accepted as-is. The handler also does not independently verify that the
  authenticated validator is the scheduled leader of the reported slot.
- After the first ordered stream message, valid leader snapshots use a separate latest-value channel; they may be
  coalesced and handled out of order relative to results or heartbeats. Range checking happens when the stream drain
  receives the snapshot. There is no monotonic-slot check: a later arrival with a lower in-range slot can replace it.
  Results and heartbeats use the ordered control queue instead.

## BAM Overrides Inside the Validator

When `bam_enabled == Connected`, validator-side non-vote scheduling switches to BAM-supplied batches:

- The dedicated BAM scheduler replaces the normal non-vote scheduler.
- Block Engine streaming and bundle processing are suppressed from `DrainingBlockEngine` onward; they continue during
  `Connecting`.
- TPU forwarding is controlled separately by fetch-stage selection; `Connected` alone does not disable it.
- Vote processing and validator-internal maintenance work (for example tip-program upkeep bundles) still continue.

### bam_enabled State Machine (Observed)

- `Disconnected`: BAM inactive; normal non-vote scheduling enabled.
- `Connecting`: stream/config setup in progress; normal scheduling remains enabled and BAM scheduling disabled.
- `DrainingBlockEngine`: heartbeat-healthy with at least one config response; stop Block Engine ingress and wait for
  BundleStage to finish its current processing pass and clear buffered bundle storage. BAM scheduling stays disabled.
- `BlockEngineDrained`: BundleStage acknowledged the drain; the manager must successfully publish the initial BAM
  builder snapshot before completing cutover. BAM scheduling stays disabled.
- `Connected`: cutover complete; BAM scheduling enabled and normal scheduling disabled. Normal non-vote scheduling
  remains enabled throughout the preceding states.

An unhealthy connection, URL change, or identity change returns the validator to `Disconnected`. After an identity
change it waits until the new identity appears in `ClusterInfo` rather than authenticating with the old identity.
Successfully parsed batches may be buffered during either drain state. In `Disconnected`/`Connecting`, otherwise
bufferable parsed batches may be dropped without results. When not buffering for a leader or retained replacement
slot, parsed batches receive `OUTSIDE_LEADER_SLOT` regardless of connection state.

Gossip advertises `ClientId::AgaveBam` once the manager is maintaining a healthy post-handshake connection, including
the drain states, and restores `ClientId::JitoLabs` on its disconnected path. This client ID alone does not prove that
BAM scheduling is active.

## Node -> Validator: AtomicTxnBatch Delivery

Node sends:

- `SchedulerResponseV0.resp = MultipleAtomicTxnBatch { batches: [...] }`

Each `AtomicTxnBatch` contains:

- `seq_id: u32` (Node-generated result-correlation identifier; validator FIFO priority is independent of it)
- `max_schedule_slot: u64` (latest slot accepted by the validator's parsing and scheduling stages)
- `packets: repeated Packet`

- Standalone transactions use one packet with `revert_on_error = false`.
- Bundles use one to five packets with `revert_on_error = true`.
- Outer grouping packs available committed notices and may mix standalone and bundle batches. It need not match a
  worker's execution batch.
- Message grouping is a transport/implementation detail and MUST NOT be used to infer scheduling or atomicity. Atomicity
  is defined per `AtomicTxnBatch`.
- An empty outer group is ignored and produces no result.
- Node assigns each batch a `seq_id` from a process-lifetime wrapping counter and sets `max_schedule_slot` to the
  speculative bank's slot. The current implementation does not check for a pending or late-result collision when the
  counter wraps; results carry no auction-generation identifier.
- The schema can encode multi-packet non-revert batches, but the reference validator rejects them with
  `INCONSISTENT_BUNDLE` at index 0. Send exactly one packet when `revert_on_error = false`.

## Node: Intake, Prioritization, and Dispatch Path (Observed)

Node policy selects validator-visible work; it does not replace validator validation or execution:

- Intake is bounded and lossy. Transactions, maker transactions, and bundles pass source-specific checks before
  entering priority containers. Validator-stream authentication is separate from HTTP/BAMB bundle-access credentials.
- Makers enter the graph before regular work. Regular priority is validator reward divided by estimated cost;
  bundles also include recognized tips. Account conflicts determine execution order after insertion.
- Auctions run for a connected current leader, except in test-only always-auction mode. Node bank timing determines
  pacing and dispatch deadlines; validator `LeaderState` tick/CU reports do not supply those budgets.
- Ready work executes against a slot-scoped speculative bank. Standalone instruction failures or fees-only results
  can still commit speculative state and be forwarded. Bundles and optional generated clocks require success.
- Dispatch targets the selected leader's stream. Bundles execute alone in worker batches; standalone packing does
  not change per-`AtomicTxnBatch` wire atomicity. Optional generated clocks use ordinary non-revert delivery.
- Retryable sent work waits for the next slot. Work rejected before dispatch may retry in the same slot, subject to
  admission, expiry, and capacity limits. Validator rejection does not roll back prior speculative effects.

The scheduler guide owns [ingress and maker policy](bam-speculative-execution-scheduler-context.md#ingress-validation-and-buffering),
[leader-window behavior](bam-speculative-execution-scheduler-context.md#leader-state-machine),
[priority and bundle pacing](bam-speculative-execution-scheduler-context.md#common-auction-admission),
[speculative state and cost budgets](bam-speculative-execution-scheduler-context.md#slot-scoped-speculative-state-and-pacing),
and [generated clocks](bam-speculative-execution-scheduler-context.md#generated-clock-transactions).

## Validator: Batch Ingestion, Checks, and Filters

The validator runs a dedicated parsing pipeline before any batch is eligible for execution.

### Current Slot for Validation

Validator selects a bank for batch validation as:

- If shared leader state contains a `working_bank`, use that bank (no frozen/complete check at this stage).
- During a bankless same-slot replacement with a retained leader slot, hold the received burst until a replacement
  bank is available rather than validating against an unrelated BankForks bank.
- Otherwise use the BankForks working bank. In the live pipeline this fallback bank may be provisional: fork-dependent
  blockhash/status and seed fee-payer checks wait until atomic batch execution is enabled on the resolved leader bank.
  The selected bank's slot still supplies `current_slot` for prevalidation.

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
5. Representable non-revert result:
   If `revert_on_error = false` and there is more than one packet, reject with
   `DeserializationErrorReason::INCONSISTENT_BUNDLE` at index 0. This also rejects multi-packet batches whose flags are
   all omitted.

### Signature Verification

After prevalidation, the validator sanitizes each packet view and verifies every signature. Oversized data is rejected
rather than truncated: Legacy/V0 use the 1,232-byte packet limit; V1 uses the 4,096-byte `MAX_TRANSACTION_SIZE` limit.
Proto `meta.size` and
`simple_vote_tx` are ignored. Signature verification derives the vote classification from the bytes and retains it
with the sanitized view for parsing. A size, sanitize, or signature failure rejects the entire batch with
`SANITIZE_ERROR` at the first failing packet index. These failures take precedence over later bank-dependent parsing
errors anywhere in that batch.

### Per-Transaction Parsing and Bank-Front-Run Checks

For each packet in the batch, validator performs the following checks in order.
Any failure rejects the entire batch, with the failing transaction index in the reason.

1. Vote-only mode reject:
   If the selected parsing bank is vote-only, reject at index 0 with `DeserializationErrorReason::SANITIZE_ERROR`.
2. Basic sanitization:
   Construct runtime metadata from the sanitized view and byte-derived vote classification retained by sigverify.
   Sanitization uses the runtime's canonical `SanitizeConfig`, not the root bank's former
   `limit_instruction_accounts` feature flag. Failures map to `DeserializationErrorReason::SANITIZE_ERROR`.
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
   When the selected bank is resolved for atomic batches, run its `check_transactions_external` with
   `MAX_PROCESSING_AGE`, covering V1 feature gating, blockhash/durable-nonce age, status-cache duplicates, and
   execution/fee-limit derivation. Otherwise defer these fork-dependent checks. Failure maps to `TransactionErrorReason`.
8. Fee payer solvency:
   When bank checks are enabled, only the first transaction's fee payer is checked. The validator verifies
   that the first transaction's fee payer can pay fee and required rent; later transactions in the same BAM bundle are
   not fee-payer-checked here because preceding bundle transactions may fund them. Failure maps to
   `TransactionErrorReason`.
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

If the validator is not in a leader consume/hold phase and shared leader state retains no leader bank slot:

- Any buffered batches are flushed with `SchedulingError::OUTSIDE_LEADER_SLOT`.
- Successfully parsed new batches also receive `OUTSIDE_LEADER_SLOT`; earlier prevalidation, signature, and parse
  failures retain their specific errors.

A retained leader slot during replacement is a buffering exception even if the current decision is Forward or
ForwardAndHold. This protects work through a same-slot bankless gap; it does not authorize execution without a bank.

## Validator Scheduling: Priority Graph and Slot Boundaries

### Scheduling Unit

The scheduler treats each atomic batch as one conflict-graph node and one worker unit.

### Slot Tracking and Bank Boundaries

On a slot change, the scheduler releases prior-slot in-flight graph dependencies, rejects graph-resident and
deferred-admission work as `OUTSIDE_LEADER_SLOT`, and clears the graph. Work still queued in the receive container is
not unconditionally rejected on a direct transition to another leader slot: it is checked against its own
`max_schedule_slot` when pulled into the new graph. Losing the active slot also drains that queue. A retained slot
during same-slot replacement (fast-leader-handover `UpdateParent`) preserves buffered and graph work through the
bankless gap; reserved in-flight work returned entirely retryable can be admitted again on the replacement.

The incoming `max_schedule_slot` is checked during prevalidation and while pulling work into the priority graph. A slot
change drains graph and deferred-admission work. Dispatch stamps a separate `target_slot` with the selected leader
bank's slot. The worker requires an exact slot match and, for reserved work, an exact bank identity match. It does not
carry the original wire deadline into execution as permission to run on a later bank.

### Prio-Graph Resource Model

The resource set is the union of read/write account accesses across every transaction in the batch:

- A Write access conflicts with any prior Read or Write access on the same pubkey.
- A Read access conflicts only with a prior Write access on the same pubkey.

Work enters the graph in descending local batch priority.

### Observed Scheduling Algorithm

1. Select one resolved, incomplete leader bank with atomic batches enabled. Pin validation and cost admission to that
   bank even if BankForks advances. Pop buffered batches in descending priority, reject stale batches, and run the
   optional scheduling transaction checks.
2. Insert accepted batches into the conflict graph with their combined account accesses.
3. Before admission on a new bank or builder snapshot, wait for old in-flight work to drain and prepare tip programs.
   Recheck transactions and minimally resanitize reserved keys/LUTs before computing costs.
4. Reserve estimated cost in graph-pop order before worker dispatch, with at most eight batches in flight (the BAM
   worker count). The controller's separate cost budget is ignored, but the bank's block/account cost limits apply.
   Account conflicts continue to block dependent nodes. Atomic admission failures release successful prefix
   reservations and mark those entries `CommitCancelled`.
5. When a block-cost rejection can be explained by outstanding estimated reservations and the candidate can otherwise
   fit, defer that batch at the head of admission until reservations settle or the bank changes. Later work does not
   bypass that deferred head. Other admission failures travel to workers for result construction.
6. Keep dispatched nodes blocked until completion. Settle or release cost reservations on their owning bank; map
   completion to the original `seq_id` and release dependents in the active slot. Same-slot replacement work whose
   entire returned batch is retryable retains its graph node and admission order for another attempt.

## Validator Execution: Bank and PoH Interaction

### Worker Entry Conditions

- Workers require a present, incomplete leader working bank. This differs from `LeaderState` emission, which requires
  an unfrozen bank.
- The worker checks once and does not wait. The eight BAM workers share one unbounded work receiver. If any worker finds
  no active bank, it drains its current work plus all then-unclaimed work from that shared queue as retryable
  `SchedulingError::POH_TIMEOUT`.
- The scheduler-stamped `target_slot` must equal the bank slot; either an older or a newer slot fails without execution.
- If a cost reservation belongs to a different bank identity, return the work to the scheduler for release and ordered
  admission on the replacement. A reservation on one bank is never consumed on a same-slot replacement bank.

### Tip Program Preparation (BAM Scheduler)

When configured, the scheduler prepares tip-program initialization and crank bundles before admitting work on a new
bank or newly published builder snapshot. It waits for prior in-flight work to finish before changing preparation
state. These atomic maintenance bundles are separate from Node work; a construction or commit failure blocks further
BAM admission and is retried after a short delay. A default builder key also blocks preparation. The reference code
skips preparation for free-rent test banks. Successfully prepared state is scoped to bank identity and the immutable
builder snapshot, not merely to slot number or whether a Node batch touches known tip accounts.

### Core Execution Call

Execution uses the normal banking-stage consumer:

1. Revalidate reserved keys and LUTs if the epoch or recorded invalidation horizon changed.
2. Consume scheduler cost-admission results without reserving the same cost twice, and apply account locking. Atomic
   batches use relaxed intra-batch locking, so their transactions may share accounts, while conflicts with other
   in-flight work still fail. Duplicate message hashes fail as
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

When `revert_on_error == false`, the underlying consumer permits partial commits, but BAM ingress accepts only one
transaction in such a batch. Multi-transaction partial-commit responses are therefore outside the supported contract.

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
- `feepayer_balance_lamports` (u64, post-balance; the reference reports 0 when internal commit metadata omits it)
- `loaded_accounts_data_size` (u32)
- `execution_success` (bool, true iff execution returned Ok)

A committed transaction may have `execution_success = false`: fees are paid and it is recorded despite its execution
error. For `revert_on_error = true` batches, `Committed` requires every transaction to execute successfully.

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
Internal variants without a dedicated protobuf reason, including `BailOut` and future variants handled by the
reference's fallback arm, map to `INSTRUCTION_ERROR`. A receiver must not infer a particular instruction error payload
from that enum alone.

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

- Ingress accepts only one transaction, and result construction reports index 0 only.

### Backpressure and Drop Semantics (Observed)

Validator may coalesce up to 24 results; Node also accepts an empty no-op group. A committed result may contain up to
five transaction results. Delivery is best-effort, not exactly-once:

- Validator receive overflow drops an entire outer batch group without results. Its outbound queues may independently
  drop `LeaderState` or results.
- Node batch dispatch blocks on its per-validator stream queue; a closed queue, or a target leader with no connected
  stream, rejects the outer message, removes its pending result-correlation entries, and closes dispatch for the target
  slot. Heartbeat enqueue failure disconnects the validator; ping enqueue failure is logged without immediately
  terminating the stream.
- Slot/deadline and bank-freeze checks precede enqueue. A blocking enqueue can outlast those checks, and already
  queued work is not recalled when the bank changes or the deadline passes. Validator-side slot and bank checks still
  govern whether it executes; enqueue success is not an execution acknowledgement.
- The graph payload is already `SentToValidator` before the message callback. A
  failed enqueue may therefore already have forwarded telemetry and follows the
  sent-work `RetryableNextSlot` policy above rather than being immediately
  requeued.
- Node result-ingress overflow may drop individual results without disconnecting the stream.
- Node's inbound control queue is bounded to 2,048 messages and backpressures its stream drain when full; that queue
  does not intentionally drop results. This is separate from the later lossy result handoff above. Leader snapshots
  use their own one-value channel, so no ordered relationship to a result group is promised.

Node can retry still-buffered underlying work under a new `seq_id` when cleanup and slot policy permit. Expiration,
rejection, and capacity eviction still apply; this recovery does not guarantee eventual delivery or recording.
Late worker retries are also fenced by container generation and discarded after the corresponding container is
cleared, preventing a stale payload from targeting a reused slab ID.

## Node: Result Handling and Retry Policy (Observed)

Node correlates results by `seq_id` to the forwarded batch.

- Unknown `seq_id` results and results from a validator other than the pending batch's expected validator are ignored
  without consuming the pending entry. Once both values match, Node removes and consumes the pending entry even if a
  later auction-generation or DAG check fails.
- After that match, Node updates its bank-local rejection cache and emits validator-result callbacks before checking the
  auction generation or DAG node. `Committed` records every pending transaction signature as already processed;
  `NotCommitted::TransactionError::AlreadyProcessed` records the indexed signature when the index is in range.
- Independently of DAG-state mutation, Node retains evidence needed for same-slot replacement recovery until replay
  observes the forwarded work. A known scheduling rejection, or a transaction/deserialization rejection with an in-range
  index, is treated as proof of no recording. `GenericInvalid`, missing fields, and unrecognized scheduling values do
  not provide that proof; after the replacement wait reaches 100 ms, only committed evidence continues to hold it.
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

## Implementation References

Node authentication, liveness, and message limits are in [validator_service.rs](../node/src/validator_service.rs).
Wire batching and enqueue behavior are in [dispatch_speculative.rs](../node/src/simple_forwarder/dispatch_speculative.rs);
result correlation and retry policy are in [auction.rs](../scheduler/src/auction.rs).

The validator-side boundary is implemented in
[`jito-labs/jito-solana`](https://github.com/jito-labs/jito-solana) at the
revision pinned above:

| Area | jito-solana source |
|---|---|
| Stream connection and result transport | [core/src/bam_connection.rs](https://github.com/jito-labs/jito-solana/blob/5ead20530340ab5b5d354d2487207a2ec84c8b82/core/src/bam_connection.rs) |
| Receive, validation, queueing, and conflict scheduling | [bam_receive_and_buffer.rs](https://github.com/jito-labs/jito-solana/blob/5ead20530340ab5b5d354d2487207a2ec84c8b82/core/src/banking_stage/transaction_scheduler/bam_receive_and_buffer.rs), [bam_scheduler.rs](https://github.com/jito-labs/jito-solana/blob/5ead20530340ab5b5d354d2487207a2ec84c8b82/core/src/banking_stage/transaction_scheduler/bam_scheduler.rs), [transaction_state_container.rs](https://github.com/jito-labs/jito-solana/blob/5ead20530340ab5b5d354d2487207a2ec84c8b82/core/src/banking_stage/transaction_scheduler/transaction_state_container.rs) |
| Cutover, builder snapshots, and tip preparation | [core/src/bam_manager.rs](https://github.com/jito-labs/jito-solana/blob/5ead20530340ab5b5d354d2487207a2ec84c8b82/core/src/bam_manager.rs), [core/src/bam_dependencies.rs](https://github.com/jito-labs/jito-solana/blob/5ead20530340ab5b5d354d2487207a2ec84c8b82/core/src/bam_dependencies.rs) |
| Canonical execution | [core/src/banking_stage/consume_worker.rs](https://github.com/jito-labs/jito-solana/blob/5ead20530340ab5b5d354d2487207a2ec84c8b82/core/src/banking_stage/consume_worker.rs) |
