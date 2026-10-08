---
name: firebam-coordination
description: Analyze or change FireBAM ingress ownership, connection health/recovery, and BAM/Block Engine subscription conflicts.
---

# FireBAM Coordination

Use this when the task is about who owns validator ingress: BAM, direct block-engine bundles, or TPU/QUIC.

## Core Model

The coordination point in Firedancer is `bam_status_fseq`, not a direct BAM-to-bundle call.

- BAM requests `FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE` only when `fd_bam_client_status(...) == CONNECTED_HEALTHY`. During deactivation, an already-active bit may remain set until the ownership handoff completes.
- Bundle tile treats that bit as "pause direct block-engine gRPC"; on the transition to active it resets the bundle client.
- Pack reads the bit and schedules only BAM work and simple vote transactions; as in jito-solana it still buffers ordinary TPU transactions (scheduled once BAM deactivates) and drops direct Block Engine bundles.
- QUIC and verify do not join this fseq in the current topologies. Full Firedancer redirects advertised TPU contact information through `bam_gossip`; fdctl performs the equivalent handoff through the Agave admin RPC.

`CONNECTED_HEALTHY` is intentionally stronger than "TCP/gRPC is connected". It requires a live scheduler stream, a successfully decoded config response, a valid (nonzero) builder pubkey from this session, HTTP/2 keepalive health, and a recent `BuilderHeartBeat`.

## High-Risk Distinctions

- `bam_config_received` and the builder are per session: reset clears both, so a new session is healthy only after its own config response.
- Scheduler proto `Ping` and scheduled work are not builder activity. Stream start initializes the watchdog, but health requires a received `BuilderHeartBeat`, which is the only scheduler message that refreshes it.
- BAM reset requests deactivation immediately. The ownership bit can remain set until pack acknowledges retirement of prior-generation BAM work and, in fdctl, until the default contact information is restored.
- Ordinary bundle-client resets can leave decoded transactions to drain, but BAM activation explicitly clears the bundle tile's pending queue. A bundle publication that already holds `BUNDLE_PUBLISHING` completes before BAM can activate.
- Pausing bundle only at `CONNECTED_HEALTHY` leaves a startup/reconnect window where direct block-engine and BAM node block-engine subscriptions can compete.

For same-validator Block Engine subscription conflicts or sibling connection
cleanup, read the [cross-repository subscription chain](references/block-engine.md).

## Review Checklist

For connection/health changes, check the relevant questions:

- What exact state writes or clears `FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE`?
- Can bundle reconnect while BAM is configured but not yet healthy?
- Does reset clear enough state to avoid stale health, while preserving durable bundle results?
- Is `bam_config_received` forced to refresh after reconnect?
- Are stream-end paths passive reconnects, health downgrades, or hard resets?
- Are duplicate-connection cleanup paths guarded by a stream/connection id?

## Targeted Validation

Prefer tests that exercise the real tile/client path:

- `make -j4 test_bam_tile`
- `"$(make --silent objdir)/unit-test/test_bam_tile"`
- From `../bam`: `cargo test -p bam-node validator_service::tests::<test_name>`

Use the same build parameters when locating artifacts; see
[FireBAM testing](../../doc/firebam-coordination.md#testing) for local setup.

When validating config refetch after reset, include a case with:

- `bam_config_received == 0`
- no config request in flight
- expected immediate or short-throttle `GetBuilderConfig` retry
