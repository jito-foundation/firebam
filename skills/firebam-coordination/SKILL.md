---
name: firebam-coordination
description: Analyze or modify FireBAM connection coordination, BAM health, block-engine fallback, bundle/QUIC suppression, bam_status_fseq override behavior, BAM reconnect/reset paths, or BAM node/block-engine duplicate-pubkey interactions in this repo.
---

# FireBAM Coordination

Use this when the task is about who owns validator ingress: BAM, direct block-engine bundles, or TPU/QUIC.

## Core Model

The coordination point in Firedancer is `bam_status_fseq`, not a direct BAM-to-bundle call.

- BAM requests `FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE` only when `fd_bam_client_status(...) == CONNECTED_HEALTHY`. During deactivation, an already-active bit may remain set until the ownership handoff completes.
- Bundle tile treats that bit as "pause direct block-engine gRPC"; on the transition to active it resets the bundle client.
- Pack reads the bit and admits/schedules BAM work while suppressing ordinary and Block Engine work.
- QUIC and verify do not join this fseq in the current topologies. Full Firedancer redirects advertised TPU contact information through `bam_gossip`; fdctl performs the equivalent handoff through the Agave admin RPC.

`CONNECTED_HEALTHY` is intentionally stronger than "TCP/gRPC is connected". It requires a live scheduler stream, a successfully decoded config response, HTTP/2 keepalive health, and a recent `BuilderHeartBeat`.

## High-Risk Distinctions

- `bam_config_received` is not equivalent to `builder_info_valid_until`.
  Fresh builder metadata must not suppress a config fetch after reset because `bam_config_received` gates healthy status.
- Scheduler proto `Ping` and scheduled work are not builder activity. Stream start initializes the watchdog, but health requires a received `BuilderHeartBeat`, which is the only scheduler message that refreshes it.
- BAM reset requests deactivation immediately. The ownership bit can remain set until pack acknowledges retirement of prior-generation BAM work and, in fdctl, until the default contact information is restored.
- Ordinary bundle-client resets can leave decoded transactions to drain, but BAM activation explicitly clears the bundle tile's pending queue. A bundle publication that already holds `BUNDLE_PUBLISHING` completes before BAM can activate.
- Pausing bundle only at `CONNECTED_HEALTHY` leaves a startup/reconnect window where direct block-engine and BAM node block-engine subscriptions can compete.

## Cross-Repo Block-Engine Chain

When investigating BAM node vs block-engine interactions, inspect the sibling repos directly if present:

- `../block-engine/src/validator_interface_service/src/server.rs`
- `../block-engine/src/core/src/middleware/token_authenticator.rs`
- `../block-engine/src/core/src/grpc/subscription_stream.rs`
- `../bam/node/src/blockengine_connection.rs`
- `../bam/node/src/validator_service.rs`
- `../bam/core/src/process_status.rs`
- `../bam/node/src/node_liveness.rs`

Known chain to verify against current code:

- Block-engine `SubscribePackets` and `SubscribeBundles` are keyed by validator pubkey.
- A new same-pubkey stream replaces the old stream and sends the old side `RESOURCE_EXHAUSTED`.
- BAM node authenticates to block-engine as `api_key:validator_pubkey`, so it collides with a direct validator bundle client using the same identity.
- BAM node block-engine stream loss is normally local to `BlockEngineConnection`: it logs/metrics and reconnects. It does not by itself prove the validator-facing BAM scheduler stream drops.
- BAM node scheduler-stream replacement cleanup needs connection-id guarding; otherwise an old replaced validator task can remove the new pubkey entry.
- Block-engine subscription cleanup uses a subscription id guard; use that as the comparison point when reviewing BAM node cleanup.

## Review Checklist

For connection/health changes, explicitly answer:

- What exact state writes or clears `FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE`?
- Can bundle reconnect while BAM is configured but not yet healthy?
- Does reset clear enough state to avoid stale health, while preserving durable bundle results?
- Is `bam_config_received` forced to refresh after reconnect even if builder info is fresh?
- Are stream-end paths passive reconnects, health downgrades, or hard resets?
- Are duplicate-connection cleanup paths guarded by a stream/connection id?

## Targeted Validation

Prefer tests that exercise the real tile/client path:

- `make -j4 test_bam_tile`
- `./build/native/gcc/unit-test/test_bam_tile`
- From `../bam`: `cargo test -p bam-node validator_service::tests::<test_name>`

For a BAM config/health regression, include a reset-like case with:

- `builder_info_valid_until` in the future
- `bam_config_received == 0`
- no config request in flight
- expected immediate or short-throttle `GetBuilderConfig` retry
