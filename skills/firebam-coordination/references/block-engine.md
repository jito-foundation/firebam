# Cross-repository Block Engine subscription chain

Use this when investigating competing subscriptions for the same validator or
connection cleanup across BAM and Block Engine. Sibling paths below are
relative to the FireBAM repository root; inspect them when present.

## Source leads

- `../block-engine/src/validator_interface_service/src/server.rs`
- `../block-engine/src/core/src/middleware/token_authenticator.rs`
- `../block-engine/src/core/src/grpc/subscription_stream.rs`
- `../bam/node/src/blockengine_connection.rs`
- `../bam/node/src/validator_service.rs`
- `../bam/core/src/process_status.rs`
- `../bam/node/src/node_liveness.rs`

## Chain to verify against current code

- Block-engine `SubscribePackets` and `SubscribeBundles` are keyed by validator pubkey.
- A new same-pubkey stream replaces the old stream and sends the old side `RESOURCE_EXHAUSTED`.
- BAM node authenticates to block-engine as `api_key:validator_pubkey`, so it collides with a direct validator bundle client using the same identity.
- BAM node block-engine stream loss is normally local to `BlockEngineConnection`: it logs/metrics and reconnects. It does not by itself prove the validator-facing BAM scheduler stream drops.
- BAM node scheduler-stream replacement cleanup needs connection-id guarding; otherwise an old replaced validator task can remove the new pubkey entry.
- Block-engine subscription cleanup uses a subscription id guard; use that as the comparison point when reviewing BAM node cleanup.
