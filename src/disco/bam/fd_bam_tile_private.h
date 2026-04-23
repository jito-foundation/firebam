#ifndef HEADER_fd_src_disco_bam_fd_bam_tile_private_h
#define HEADER_fd_src_disco_bam_fd_bam_tile_private_h

#include "../bundle/fd_keepalive.h"
#include "../stem/fd_stem.h"
#include "../keyguard/fd_keyswitch.h"
#include "../topo/fd_topo.h"
#include "../bam/fd_bam_types.h"
#include "fd_bam_ctrl.h"
#include "../metrics/fd_metrics.h"
#include "../plugin/fd_plugin.h"
#include "../../waltz/grpc/fd_grpc_client.h"
#include "../../waltz/resolv/fd_netdb.h"
#include "../../waltz/fd_rtt_est.h"
#include "../../util/net/fd_net_headers.h"
#include "proto/bam_api.pb.h"
#include "proto/bam_types.pb.h"

struct fd_bam_tile;
typedef struct fd_bam_tile fd_bam_tile_t;

#define FD_BAM_ACTIVITY_TIMEOUT_NS ((long)6e9) /* 6 seconds */
#if FD_HAS_OPENSSL
#include <openssl/ssl.h> /* SSL_CTX */
#endif

struct fd_bam_out_ctx {
  ulong       idx;
  fd_wksp_t * mem;
  ulong       chunk0;
  ulong       wmark;
  ulong       chunk;
};

typedef struct fd_bam_out_ctx fd_bam_out_ctx_t;

struct fd_bam_in_ctx {
  fd_wksp_t * mem;
  ulong       chunk0;
  ulong       wmark;
};

typedef struct fd_bam_in_ctx fd_bam_in_ctx_t;

/* fd_bam_metrics_t contains private metric counters.  These get
   published to fd_metrics periodically.
   Counters are cumulative over the BAM tile lifetime and are not reset
   by reconnects (fd_bam_client_reset). */

struct fd_bam_metrics {
  ulong transaction_published_cnt;
  ulong atomic_batch_published_cnt;
  ulong feedback_results_dropped_cnt;
  ulong ingress_packet_oversize_cnt;
  ulong keepalive_acks_cnt;
  ulong builder_heartbeats_decoded_cnt;
  ulong healthy_connects_cnt;
  ulong healthy_disconnects_cnt;

  ulong failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_CNT ];

  /* Ingress diagnostics for BAM scheduler responses. */
  ulong ingress_multi_message_received_cnt;

  ulong ingress_batch_commit_attempt_cnt;
  ulong ingress_batch_published_cnt;
  ulong ingress_batch_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_CNT ];
  ulong ingress_message_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_MESSAGE_REJECT_REASON_CNT ];

  /* Enum counters staged locally and flushed during housekeeping. */
  ulong outbound_enqueue_outcome_cnt[ FD_METRICS_ENUM_BAM_ENQUEUE_OUTCOME_CNT ];
  ulong stream_transition_cnt[ FD_METRICS_ENUM_BAM_STREAM_TRANSITION_CNT ];
  ulong leader_pending_dropped_cnt[ FD_METRICS_ENUM_BAM_LEADER_PENDING_DROP_REASON_CNT ];
  ulong leader_pending_replaced_cnt;
  ulong slot_ingress_result_cnt[ FD_METRICS_ENUM_BAM_SLOT_INGRESS_RESULT_CNT ];
  ulong slot_ingress_transactions_cnt[ FD_METRICS_ENUM_BAM_SLOT_INGRESS_TXN_TIMING_CNT ];
  ulong leader_slot_end_status_cnt[ FD_METRICS_ENUM_BAM_LEADER_SLOT_END_STATUS_CNT ];
  ulong healthy_leader_slot_result_cnt[ FD_METRICS_ENUM_BAM_HEALTHY_LEADER_SLOT_RESULT_CNT ];
  ulong scheduler_pong_send_outcome_cnt[ FD_METRICS_ENUM_BAM_SCHEDULER_PONG_SEND_OUTCOME_CNT ];

  fd_histf_t builder_heartbeat_arrival_delta_nanos[1];
  fd_histf_t scheduler_pong_send_nanos[1];
};

typedef struct fd_bam_metrics fd_bam_metrics_t;

#define FD_BAM_SLOT_INGRESS_TIMING_CNT 128UL
#define FD_BAM_SLOT_INGRESS_RETENTION_SLOTS 2UL
typedef struct {
  ulong slot;
  long  first_rx_ts_ns;
  long  slot_end_ns;
  ulong txn_before_slot_end;
  ulong txn_after_slot_end;
  ulong txn_unknown_slot_end;
  uchar first_rx_after_slot_end;
  uchar summary_emitted;
  uchar valid;
} fd_bam_slot_ingress_timing_t;

#define FD_BAM_LEADER_SLOT_END_TRACKER_CNT 64UL
typedef struct {
  ulong                         slot;
  long                          slot_end_ns;
  fd_plugin_bam_update_status_t status_at_end;
  uchar                         fresh_seen_before_end;
  uchar                         counted;
  uchar                         valid;
} fd_bam_leader_slot_end_tracker_t;

typedef struct {
  fd_bam_tile_t * ctx;                                         /* owning tile context; non-NULL while batch is processed */
  long                ingress_rx_ts_ns;                        /* fd_bam_now() timestamp from the scheduler receive callback. */
  long                ingress_slot_end_ns;                     /* slot_end_ns snapshot for batch max_schedule_slot when known at receive time, else 0. */
  uint                ingress_rx_tspub;                        /* Compact fd_tickcount() timestamp from the scheduler receive callback. 0 means unknown. */
  bam_types_Packet    packets[ FD_PACK_MAX_TXN_PER_BUNDLE ];   /* decoded packet cache; indices [0,packet_cnt) valid */
  uchar               packet_cnt;                              /* number of packets collected; [0,FD_PACK_MAX_TXN_PER_BUNDLE) */
  uchar               revert_on_error;                         /* 0/1 value for the most recently collected packet; missing flags default to 0 */
  uchar               has_deser_err;                           /* 0/1 value if we have batch-level not-committed reason */
  uchar               deser_index;                             /* zero-based transaction index tied to deserialization error */
  uchar               deser_reason;                            /* bam_types_DeserializationErrorReason enum value */
} fd_bam_batch_ctx_t;

/* fd_bam_tpu_update_state_t tracks what Frankendancer Agave's TPU value, and if update attempt is required.
   Issue:
   - Two independent code paths can call fd_bam_gossip_update() around the same
     time (e.g. ConfigResponse arrival + BAM status edge). Without
     dedupe, both paths can immediately invoke admin RPC updates back-to-back.

   How it is used:
   - fd_bam_gossip_update() compares the desired "applied" state
     (BAM vs default TPU) to ctx->tpu_update_state. If it already matches,
     it skips the admin RPC update, preventing duplicate updates.
   - On failure (admin RPC unavailable, apply failure, or readback mismatch),
     it records a PENDING_* state so fd_bam_tile_housekeeping() will retry later.
   - When BAM TPU sockets change (new ConfigResponse), we set UNKNOWN so the
     next call will re-apply even if we're already in BAM mode. */

typedef enum {
  FD_BAM_TPU_UPDATE_STATE_UNKNOWN         = 0, /* No known applied state; next update should attempt to apply desired. */
  FD_BAM_TPU_UPDATE_STATE_APPLIED_DEFAULT = 1, /* Last successful update applied default TPU. */
  FD_BAM_TPU_UPDATE_STATE_APPLIED_BAM     = 2, /* Last successful update applied BAM-provided TPU. */
  FD_BAM_TPU_UPDATE_STATE_PENDING_DEFAULT = 3, /* Needs an update attempt to apply default TPU; housekeeping retries. */
  FD_BAM_TPU_UPDATE_STATE_PENDING_BAM     = 4, /* Needs an update attempt to apply BAM TPU; housekeeping retries. */
} fd_bam_tpu_update_state_t;

/* fd_bam_tile_t is the context object provided to callbacks from
   stem, and contains all state needed to progress the tile. */

/* fd_bam_tile aggregates the long-lived state required to operate the BAM
   scheduler client: networking, authentication, subscriptions, result queues,
   and topology bindings. */

struct fd_bam_tile {
  fd_keyswitch_t * keyswitch;                     /* Manages the identity keypair */
  fd_keyguard_client_t keyguard_client[1];        /* Keyguard client used to request signatures */

  ulong            bank_bam_in_idx;               /* First polled bank_bam input index in stem callback space */
  ulong            bank_bam_in_cnt;               /* Count of contiguous bank_bam durable result inputs */
  ulong            pack_bam_leader_in_idx;        /* Polled input index for pack_bam_ldr snapshot/control updates */
  ulong            pack_bam_result_in_idx;        /* Polled input index for pack_bam_res durable bundle feedback */
  fd_bam_in_ctx_t  bank_in[ FD_PACK_MAX_BANK_TILES ]; /* Bank->BAM result ingress dcache contexts */
  fd_bam_in_ctx_t  pack_leader_in;                /* Pack->BAM latest-value-wins leader-state ingress */
  fd_bam_in_ctx_t  pack_result_in;                /* Pack->BAM durable result ingress */
  uchar            frag_staged_kind;              /* FD_BAM_FRAG_STAGED_* marker set by during_frag and consumed by after_frag; NONE means "drop/no-op". */
  ulong            frag_staged_chunk;             /* during_frag staged source dcache chunk for after_frag commit */
  fd_wksp_t *      frag_staged_mem;               /* during_frag staged source dcache workspace for after_frag commit */

  uchar is_ssl : 1;                                /* Non-zero when TLS is negotiated */
  int  keylog_fd;                                 /* TLS key log output fd (-1 when disabled) */
# if FD_HAS_OPENSSL
  /* OpenSSL */
  SSL_CTX *    ssl_ctx;                           /* Owning TLS context for BAM connection */
  SSL *        ssl;                               /* TLS session bound to tcp_sock */
  fd_alloc_t * ssl_alloc;                         /* TODO: use this when BAM owns OpenSSL allocator teardown/reset lifecycle */
# endif /* FD_HAS_OPENSSL */

  /* Currently running config, values loaded via TOML and updated by set_bam admin control */
  fd_bam_ctrl_t * ctrl;                  /* Runtime control shared object (NULL when tile launched without admin support) */
  uchar  enabled;                        /* Whether BAM runtime is enabled by the operator */
  uchar  dump_bam_mode;                  /* FD_BAM_DEBUG_DUMP_MODE_* controlling BAM debug logging. */
  char   server_fqdn[ FD_FQDN_BUF_MAX ]; /* cstr; hostname configured for BAM endpoint */
  ushort server_fqdn_len;                /* Length of server_fqdn (no terminator) */
  char   server_sni[ FD_SNI_BUF_MAX ];   /* cstr; optional override for TLS SNI */
  ushort server_sni_len;                 /* Length of server_sni (no terminator) */
  ushort server_tcp_port;                /* Remote TCP port for gRPC */

  /* Resolver */
  fd_netdb_fds_t netdb_fds[1];                    /* fd_netdb handles for async DNS lookups */
  uint server_ip4_addr; /* last DNS lookup result; cached IPv4 addr from most recent resolve */

  /* TCP socket */
  int  tcp_sock;                                  /* Non-blocking socket for gRPC transport */
  int  so_rcvbuf;                                 /* Desired receive buffer size */
  uchar tcp_sock_connected : 1;                   /* Set once connect handshake completes */
  uchar defer_reset : 1;                          /* Delay reset until after current iteration */

  /* Keepalive via HTTP/2 PINGs (randomized) */
  long              keepalive_interval;           /* Target interval for PING dispatch */
  fd_keepalive_t    keepalive[1];                 /* HTTP/2 keepalive state machine */
  fd_rtt_estimate_t rtt[1];                       /* RTT estimator fed by keepalive replies */

  /* gRPC client */
  void *                   grpc_client_mem;       /* Scratch backing storage for fd_grpc_client */
  ulong                    grpc_buf_max;          /* Maximum payload size allocated for gRPC */
  fd_grpc_client_t *       grpc_client;           /* Active gRPC client driving HTTP/2 */
  fd_grpc_client_metrics_t grpc_metrics[1];       /* Per-client metrics exported to fd_metrics */
  ulong                    map_seed;              /* Random seed used for header hashing */
  fd_wksp_t *              heap_wksp;             /* Workspace containing this tile, cached for heap gauges. */
  ulong                    heap_size_cached;      /* Cached workspace capacity exported to bam_heap_size. */
  ulong                    heap_free_bytes_cached;/* Cached workspace free bytes exported to bam_heap_free_bytes. */
  long                     heap_usage_last_update_ns; /* Last fd_wksp_usage sample time (fd_log_wallclock), 0 if never sampled. */

  /* ConfigResponse BlockEngineBuilderConfig values */
  uchar builder_pubkey[ 32 ];                     /* Builder identity fetched from BAM */
  uchar builder_commission;                       /* commission as a percentage (0-100) */
  long  builder_info_valid_until;                 /* Expiry timestamp for builder info */
  uchar prio_fee_recipient[ 32 ];                 /* Recipient pubkey of the priority fee commission */
  ushort commission_bps;                          /* commission basis points */
  uchar prio_fee_recipient_set;                   /* Flag indicating prio_fee_recipient populated */

  /* ConfigResponse BamConfig values */
  fd_bam_fee_cfg_t * fee_cfg;          /* Shared fee configuration exported to peers */
  uint               fee_cfg_version;  /* Last version published to fee_cfg */
  fd_ip4_port_t bam_tpu;               /* Latest TPU socket advertised by BAM */
  fd_ip4_port_t bam_tpu_fwd;           /* Latest TPU Forward socket advertised by BAM */
  ulong         bam_shred_sock_cnt;    /* Latest shred receiver count advertised by BAM */
  fd_ip4_port_t bam_shred_sock[ FD_BAM_SHRED_SOCK_MAX ]; /* Latest shred receivers advertised by BAM */
  ulong         published_shred_sock_cnt; /* Last effective shred receiver count published to shred tiles */
  fd_ip4_port_t published_shred_sock[ FD_BAM_SHRED_SOCK_MAX ]; /* Last effective shred receivers published to shred tiles */
  fd_ip4_port_t default_tpu;           /* TPU socket Agave booted with (non-BAM) */
  fd_ip4_port_t default_tpu_fwd;       /* TPU Forward socket Agave booted with */
  fd_ip4_port_t configured_default_tpu; /* Startup TPU advert derived from local Frankendancer config when the advertised IP is locally knowable. Firedancer Agave uses the same base UDP socket for tpu and tpu_forwards. */
  char admin_rpc_path[ PATH_MAX ];     /* Frankendancer Agave admin socket path; empty disables the cross-process admin path. */
  fd_bam_tpu_update_state_t tpu_update_state; /* Dedupe/retry state for admin-RPC TPU advert updates (Frankendancer) */

  /* Bundle state */
  uint  bundle_seq;                               /* Monotonic bundle identifier (0 before first bundle). */
  ulong bundle_max_schedule_slot;                 /* Highest slot allowed by scheduler, or 0 when no BAM bundle is active. */
  fd_bam_slot_ingress_timing_t slot_ingress_timing[ FD_BAM_SLOT_INGRESS_TIMING_CNT ]; /* Recent BAM ingress timing by max_schedule_slot for debug captures. */
  ulong dump_bam_last_slot;                       /* Most recent max_schedule_slot dumped under FD_BAM_DEBUG_DUMP_MODE_SLOT_FIRST. */
  uchar dump_bam_last_slot_valid;                 /* Whether dump_bam_last_slot has been initialized */

  /* BAM specific */
  fd_grpc_h2_stream_t * bam_stream;                      /* Current scheduler stream; NULL while unsubscribed or reconnecting */
  long                  bam_last_builder_activity_ns;    /* fd_bam_now() timestamp of last scheduler-stream liveness refresh from stream start, BuilderHeartBeat, or bundle work; scheduler proto Ping is intentionally excluded (0 if none received) */
  long                  bam_last_validator_heartbeat_ns; /* fd_bam_now() timestamp of last validator heartbeat (0 if never sent) */
  long                  bam_last_config_poll_ns;         /* fd_bam_now() timestamp of last config poll attempt (0 if never polled) */
  ushort                feedback_queue_depth;             /* Queue depth of bam_results (0 <= cnt < FD_BAM_MAX_PENDING_RESULTS) */
  ushort                bam_results_head;                /* Index of next result to flush (wraps modulo FD_BAM_MAX_PENDING_RESULTS) */
  ushort                bam_results_tail;                /* Index of next slot to fill (wraps modulo FD_BAM_MAX_PENDING_RESULTS) */
  fd_bam_bundle_result_t bam_results[ FD_BAM_MAX_PENDING_RESULTS ]; /* Durable FIFO result ring fed by pack_bam_res and bank_bam; preserved across reconnect/reset until flushed */
  fd_bam_leader_state_t  bam_leader_state;               /* Latest pack_bam_ldr snapshot awaiting publication; newer unsent snapshots supersede older ones */
  fd_bam_leader_slot_end_tracker_t leader_slot_end[ FD_BAM_LEADER_SLOT_END_TRACKER_CNT ]; /* Per-slot metric tracker used to record BAM status and fresh-work state at slot end without relying on transition-history reconstruction. */
  uchar                 bam_identity_pubkey[ 32 ];       /* validator pubkey from the identity keypair */
  char                  bam_identity_pubkey_b58[ FD_BASE58_ENCODED_32_SZ ]; /* Base58-encoded validator pubkey string (NUL-terminated) */
  char                  challenge_to_sign[ sizeof(bam_api_AuthChallengeResponse) ]; /* Latest auth challenge from AuthChallengeResponse.challenge_to_sign field */
  char                  bam_auth_signature[ FD_BASE58_ENCODED_64_SZ ]; /* Base58-encoded Ed25519 signature for BAM auth (NUL-terminated) */
  uint                  bam_stream_live        : 1;      /* set once bam_stream is established and delivering messages */
  uint                  bam_stream_connecting  : 1;      /* set during gRPC stream handshake before bam_stream_live */
  uint                  bam_auth_ready         : 1;      /* set when challenge_to_sign contains a fresh, NUL-terminated challenge to sign */
  uint                  bam_auth_inflight      : 1;      /* true while GetAuthChallenge GRPC call is pending */
  uint                  bam_config_inflight    : 1;      /* true while GetBuilderConfig GRPC call is pending */
  uint                  bam_config_received    : 1;      /* set after a valid ConfigResponse lands on the current connection */
  uint                  bam_leader_pending     : 1;      /* set when a coalesced leader snapshot awaits send; not durable like bam_results */

  /* Error backoff */
  fd_rng_t rng[1];                                /* RNG used to randomize reconnects */
  uint     backoff_iter;                          /* Backoff iteration counter */
  long     backoff_until;                         /* Earliest ts to retry connection */
  long     backoff_reset;                         /* Errors before this ts reset backoff_iter */

  /* Stem publish */
  fd_stem_context_t * stem;                          /* Cached stem context handed to callbacks */
  fd_bam_out_ctx_t    verify_out;                    /* Output ring for transaction verification */
  fd_bam_out_ctx_t    plugin_out;                    /* Output ring for plugin status updates */
  fd_bam_out_ctx_t    gossip_out;       /* Stem output buffer used for BAM gossip updates (Full firedancer, not Frankendncer) */
  fd_bam_out_ctx_t    shred_out;        /* Stem output buffer used for BAM shred receiver updates */
  ulong *             bam_status_fseq; /* Shared latch written with BAM status bits (bit 0 = override active, bit 1 = current slot has BAM work) */

  /* App metrics */
  fd_bam_metrics_t metrics;                         /* Tile-local counters flushed to metrics */

  /* Check engine light */
  fd_plugin_bam_update_status_t bam_status_recent;  /* most recently observed 'check engine light' */ //TODO: update this for bam
  fd_plugin_bam_update_status_t bam_status_plugin;  /* last 'plugin' update written */
  fd_plugin_bam_update_status_t bam_status_counted; /* last status used for healthy-edge counters */
  fd_plugin_bam_update_status_t bam_status_logged;  /* last logged bundle status */
  long  last_bam_status_log_nanos;
  long  last_gui_publish_nanos;
  uchar               gui_dirty;       /* Forces a GUI/plugin update on next publish */
};

typedef struct fd_bam_tile fd_bam_tile_t;

/* Result feedback is durable: append to the local FIFO ring and keep it
   across reconnect/reset until the scheduler stream accepts it. */
FD_FN_UNUSED static inline void
fd_bam_enqueue_result( fd_bam_tile_t *               ctx,
                       fd_bam_bundle_result_t const * res ) {
  if( FD_UNLIKELY( res->bundle_txn_cnt > FD_PACK_MAX_TXN_PER_BUNDLE ) ) {
    FD_LOG_WARNING(( "Dropping malformed BAM bundle result txn_cnt=%u exceeds max=%lu (seq_id=%u slot=%lu)",
                     res->bundle_txn_cnt, FD_PACK_MAX_TXN_PER_BUNDLE, res->seq_id, res->slot ));
    ctx->metrics.feedback_results_dropped_cnt++;
    return;
  }
  if( FD_UNLIKELY( res->bundle_err > FD_BAM_BUNDLE_ERR_GENERIC_INVALID ) ) {
    FD_LOG_WARNING(( "Malformed BAM bundle result bundle_err=%u (seq_id=%u slot=%lu)",
                     res->bundle_err, res->seq_id, res->slot ));
  } else if( FD_UNLIKELY( res->bundle_err==FD_BAM_BUNDLE_ERR_GENERIC_INVALID &&
                           (res->generic_invalid_reason==FD_BAM_ERR_GENERIC_INVALID_NONE ||
                            res->generic_invalid_reason>=FD_BAM_ERR_GENERIC_INVALID_CNT) ) ) {
    FD_LOG_WARNING(( "Malformed BAM generic-invalid reason=%u (seq_id=%u slot=%lu)",
                     res->generic_invalid_reason, res->seq_id, res->slot ));
  } else if( FD_UNLIKELY( res->bundle_err==FD_BAM_BUNDLE_ERR_DESER &&
                           res->deser_reason>_bam_types_DeserializationErrorReason_MAX ) ) {
    FD_LOG_WARNING(( "Malformed BAM deser reason=%u (seq_id=%u slot=%lu)",
                     res->deser_reason, res->seq_id, res->slot ));
  }

  if( FD_UNLIKELY( ctx->feedback_queue_depth>=FD_BAM_MAX_PENDING_RESULTS ) ) {
    FD_LOG_WARNING(( "Dropping BAM bundle result (bam tile queue full): seq_id=%u slot=%lu bundle_txn_cnt=%u exec_success=%u sched_err=%u",
                     res->seq_id, res->slot, res->bundle_txn_cnt, (uint)res->execution_success, res->scheduling_error ));
    ctx->metrics.feedback_results_dropped_cnt++;
    return;
  }
  ctx->bam_results[ ctx->bam_results_tail ] = *res;
  ctx->bam_results_tail = (ushort)((ctx->bam_results_tail + 1U) % FD_BAM_MAX_PENDING_RESULTS);
  ctx->feedback_queue_depth = (ushort)( ctx->feedback_queue_depth + 1U );
}

void
fd_bam_try_emit_slot_ingress_timing_summary( fd_bam_tile_t *                ctx,
                                             fd_bam_slot_ingress_timing_t * entry,
                                             ulong                          current_leader_slot );

FD_FN_UNUSED static inline void
fd_bam_finalize_slot_ingress_rollup( fd_bam_tile_t *                ctx,
                                     fd_bam_slot_ingress_timing_t *  entry,
                                     ulong                           current_leader_slot ) {
  if( FD_UNLIKELY( !entry->valid ) ) return;

  ulong tx_before  = entry->txn_before_slot_end;
  ulong tx_after   = entry->txn_after_slot_end;
  ulong tx_unknown = entry->txn_unknown_slot_end;
  ulong result_idx = entry->slot_end_ns
    ? FD_METRICS_ENUM_BAM_SLOT_INGRESS_RESULT_V_NO_INGRESS_IDX
    : FD_METRICS_ENUM_BAM_SLOT_INGRESS_RESULT_V_UNKNOWN_SLOT_END_IDX;

  fd_bam_try_emit_slot_ingress_timing_summary( ctx, entry, current_leader_slot );

  if( FD_LIKELY( entry->first_rx_ts_ns ) && FD_LIKELY( entry->slot_end_ns ) ) {
    result_idx = entry->first_rx_ts_ns<=entry->slot_end_ns
      ? FD_METRICS_ENUM_BAM_SLOT_INGRESS_RESULT_V_FIRST_BEFORE_END_IDX
      : FD_METRICS_ENUM_BAM_SLOT_INGRESS_RESULT_V_FIRST_AFTER_END_IDX;
  } else if( FD_UNLIKELY( entry->first_rx_ts_ns ) ) {
    tx_unknown += tx_before + tx_after;
    tx_before   = 0UL;
    tx_after    = 0UL;
  }

  ctx->metrics.slot_ingress_result_cnt[ result_idx ]++;
  ctx->metrics.slot_ingress_transactions_cnt[ FD_METRICS_ENUM_BAM_SLOT_INGRESS_TXN_TIMING_V_BEFORE_END_IDX ]       += tx_before;
  ctx->metrics.slot_ingress_transactions_cnt[ FD_METRICS_ENUM_BAM_SLOT_INGRESS_TXN_TIMING_V_AFTER_END_IDX ]        += tx_after;
  ctx->metrics.slot_ingress_transactions_cnt[ FD_METRICS_ENUM_BAM_SLOT_INGRESS_TXN_TIMING_V_UNKNOWN_SLOT_END_IDX ] += tx_unknown;
  fd_memset( entry, 0, sizeof(*entry) );
}

FD_FN_UNUSED static inline fd_bam_slot_ingress_timing_t const *
fd_bam_slot_ingress_timing_query_const( fd_bam_tile_t const * ctx,
                                        ulong                 slot ) {
  ulong start = slot & ( FD_BAM_SLOT_INGRESS_TIMING_CNT - 1UL );
  for( ulong probe=0UL; probe<FD_BAM_SLOT_INGRESS_TIMING_CNT; probe++ ) {
    fd_bam_slot_ingress_timing_t const * entry = &ctx->slot_ingress_timing[ ( start + probe ) & ( FD_BAM_SLOT_INGRESS_TIMING_CNT - 1UL ) ];
    if( FD_LIKELY( entry->valid && entry->slot==slot ) ) return entry;
  }
  return NULL;
}

FD_FN_UNUSED static inline fd_bam_slot_ingress_timing_t *
fd_bam_slot_ingress_timing_query_or_insert( fd_bam_tile_t * ctx,
                                            ulong           slot,
                                            ulong           current_leader_slot ) {
  ulong start = slot & ( FD_BAM_SLOT_INGRESS_TIMING_CNT - 1UL );
  fd_bam_slot_ingress_timing_t * free_entry = NULL;
  fd_bam_slot_ingress_timing_t * aged_entry = NULL;
  for( ulong probe=0UL; probe<FD_BAM_SLOT_INGRESS_TIMING_CNT; probe++ ) {
    fd_bam_slot_ingress_timing_t * entry = &ctx->slot_ingress_timing[ ( start + probe ) & ( FD_BAM_SLOT_INGRESS_TIMING_CNT - 1UL ) ];
    if( FD_LIKELY( entry->valid ) ) {
      if( FD_LIKELY( entry->slot==slot ) ) return entry;
      if( FD_UNLIKELY( current_leader_slot!=ULONG_MAX &&
                       entry->slot<current_leader_slot &&
                       current_leader_slot-entry->slot>=FD_BAM_SLOT_INGRESS_RETENTION_SLOTS &&
                       ( !aged_entry || entry->slot<aged_entry->slot ) ) ) {
        aged_entry = entry;
      }
    } else if( FD_UNLIKELY( !free_entry ) ) {
      free_entry = entry;
    }
  }

  if( FD_UNLIKELY( !free_entry ) ) {
    if( FD_UNLIKELY( !aged_entry ) ) {
      FD_LOG_WARNING(( "BAM slot-ingress timing table full while tracking slot %lu", slot ));
      return NULL;
    }
    fd_bam_finalize_slot_ingress_rollup( ctx, aged_entry, current_leader_slot );
    free_entry = aged_entry;
  }

  *free_entry = (fd_bam_slot_ingress_timing_t){ .slot = slot, .valid = 1U };
  return free_entry;
}

FD_FN_UNUSED static inline void
fd_bam_stage_leader_state( fd_bam_tile_t *                ctx,
                           fd_bam_leader_state_t const *  state ) {
  _Bool track_slot_end = ( state->slot!=ULONG_MAX && state->slot_end_ns );
  if( FD_UNLIKELY( ctx->bam_leader_pending &&
                   !fd_bam_leader_state_eq( &ctx->bam_leader_state, state ) ) ) {
    ctx->metrics.leader_pending_replaced_cnt++;
  }

  fd_bam_slot_ingress_timing_t * entry = track_slot_end
    ? fd_bam_slot_ingress_timing_query_or_insert( ctx, state->slot, state->slot )
    : NULL;
  if( FD_LIKELY( entry ) ) {
    entry->slot_end_ns = state->slot_end_ns;
    entry->first_rx_after_slot_end = (uchar)( entry->first_rx_ts_ns > state->slot_end_ns );
  }

  if( FD_LIKELY( track_slot_end ) ) {
    fd_bam_leader_slot_end_tracker_t * tracker = NULL;
    fd_bam_leader_slot_end_tracker_t * free_tracker = NULL;
    fd_bam_leader_slot_end_tracker_t * counted_tracker = NULL;
    for( ulong i=0UL; i<FD_BAM_LEADER_SLOT_END_TRACKER_CNT; i++ ) {
      fd_bam_leader_slot_end_tracker_t * candidate = &ctx->leader_slot_end[ i ];
      if( FD_UNLIKELY( !candidate->valid ) ) {
        if( FD_LIKELY( !free_tracker ) ) free_tracker = candidate;
        continue;
      }
      if( FD_LIKELY( candidate->slot==state->slot ) ) {
        tracker = candidate;
        break;
      }
      if( FD_UNLIKELY( candidate->counted && ( !counted_tracker || candidate->slot<counted_tracker->slot ) ) ) {
        counted_tracker = candidate;
      }
    }

    if( FD_UNLIKELY( !tracker ) ) tracker = free_tracker ? free_tracker : counted_tracker;
    if( FD_UNLIKELY( !tracker ) ) {
      FD_LOG_WARNING(( "BAM leader-slot-end tracker table full while tracking slot %lu", state->slot ));
    } else {
      if( FD_UNLIKELY( !tracker->valid || tracker->slot!=state->slot ) ) {
        *tracker = (fd_bam_leader_slot_end_tracker_t){
          .slot          = state->slot,
          .slot_end_ns   = state->slot_end_ns,
          .status_at_end = ctx->bam_status_counted,
          .valid         = 1U
        };
      }
      tracker->slot_end_ns = state->slot_end_ns;
      if( FD_LIKELY( !tracker->counted ) ) tracker->fresh_seen_before_end |= (uchar)!!state->current_slot_has_bam_work;
    }
  }

  ctx->bam_leader_state = *state;
  ctx->bam_leader_pending = 1U;
}

FD_FN_PURE static inline _Bool
fd_bam_current_slot_has_bam_work( fd_bam_tile_t const * ctx,
                                  long                   now_ns ) {
  return !!( ctx->bam_leader_state.slot_end_ns &&
             now_ns < ctx->bam_leader_state.slot_end_ns &&
             ctx->bam_leader_state.current_slot_has_bam_work );
}

/* Define 'request_ctx' IDs to identify different types of gRPC calls */

#define FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge               0
#define FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig               1
#define FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream            2

FD_PROTOTYPES_BEGIN

/* fd_bam_now is an externally linked function wrapping
   fd_log_wallclock.  This is backed by a weak symbol, allowing tests to
   override the clock source. */

long
fd_bam_now( void );

/* Admin-RPC helpers used by Frankendancer BAM mode.  The low-level
   request hook is weak so unit tests can override it without touching
   the production Unix-socket transport. */

int
fd_bam_admin_rpc_request( char const * admin_rpc_path,
                          char const * request,
                          char *       response,
                          ulong        response_max );

int
fd_bam_admin_rpc_get_contact_info( char const *   admin_rpc_path,
                                   fd_ip4_port_t * out_tpu,
                                   fd_ip4_port_t * out_tpu_fwd );

int
fd_bam_admin_rpc_set_public_tpu( char const *       admin_rpc_path,
                                 fd_ip4_port_t const tpu,
                                 fd_ip4_port_t const tpu_fwd );

/* fd_bam_client_grpc_callbacks provides callbacks for grpc_client. */

extern fd_grpc_client_callbacks_t fd_bam_client_grpc_callbacks;

/* fd_bam_client_step is an all-in-one routine to drive client logic.
   As long as the tile calls this periodically, the client will
   reconnect to the bundle server, authenticate, and subscribe to
   packets and bundles. */

void
fd_bam_client_step( fd_bam_tile_t * bundle,
                       int *              charge_busy );

/* fd_bam_client_step_reconnect drives the BAM protocol state machine
   once the HTTP/2 connection is established. It handles auth, stream
   setup, config polling, heartbeats, leader state updates, keepalives,
   and result flushing. */

int
fd_bam_client_step_reconnect( fd_bam_tile_t * ctx,
                                 long               now );

/* Expose reconnect step logic for unit tests. Returns 1 if any work was
   performed, 0 otherwise. Not used in production. */

int
fd_bam_test_client_step_reconnect( fd_bam_tile_t * ctx,
                                      long               now );

/* Expose internal result flushing logic for unit tests. Returns 1 if any
   results were flushed (busy), 0 otherwise. Not used in production. */

int
fd_bam_test_flush_results( fd_bam_tile_t * ctx );

int
fd_bam_send_leader_state( fd_bam_tile_t *               ctx,
                          fd_bam_leader_state_t const * state );

/* fd_bam_tile_backoff is called whenever an error occurs.  Stalls
   forward progress for a randomized amount of time to prevent error
   floods. */

void
fd_bam_tile_backoff( fd_bam_tile_t * ctx,
                        long               now );

void
fd_bam_gossip_update( fd_bam_tile_t *    ctx,
                              fd_stem_context_t * stem,
                              _Bool use_bam);

void
fd_bam_shred_update( fd_bam_tile_t *    ctx,
                     fd_stem_context_t * stem,
                     _Bool               use_bam );

/* fd_bam_tile_should_stall returns 1 if forward progress should be
   temporarily prevented due to an error. */

FD_FN_PURE static inline int
fd_bam_tile_should_stall( fd_bam_tile_t const * ctx,
                             long                     now ) {
  return now < ctx->backoff_until;
}

/* fd_bam_tile_housekeeping runs periodically at a low frequency. */

void
fd_bam_tile_housekeeping( fd_bam_tile_t * ctx );

/* fd_bam_client_grpc_rx_start is the first RX callback of a stream. */

void
fd_bam_client_grpc_rx_start(
    void * app_ctx,
    ulong  request_ctx
) ;

/* fd_bam_client_grpc_rx_msg is called by grpc_client when a gRPC
   message arrives (unary or server-streaming response). */

void
fd_bam_client_grpc_rx_msg(
    void *       app_ctx,      /* (fd_bam_tile_t *) */
    void const * protobuf,
    ulong        protobuf_sz,
    ulong        request_ctx   /* FD_BAM_CLIENT_REQ_{...} */
);

/* fd_bam_client_grpc_rx_end is called by grpc_client when a gRPC
   server-streaming response finishes. */

void
fd_bam_client_grpc_rx_end(
    void *                app_ctx,
    ulong                 request_ctx,
    fd_grpc_resp_hdrs_t * resp
);

/* fd_bam_client_grpc_rx_timeout is called by grpc_client when a
   gRPC request deadline gets exceeded. */

void
fd_bam_client_grpc_rx_timeout(
    void * app_ctx,
    ulong  request_ctx, /* FD_BAM_CLIENT_REQ_{...} */
    int    deadline_kind /* FD_GRPC_DEADLINE_{HEADER|RX_END} */
);

void
fd_bam_tile_publish_bundle_txn(
    fd_bam_tile_t * ctx,
    void const *       txn,
    ushort             txn_sz,
    uchar              bundle_txn_cnt,
    uchar              batch_idx,
    uint               scheduler_arrival_tspub,
    uint               source_ipv4 );

void
fd_bam_tile_publish_txn(
    fd_bam_tile_t * ctx,
    void const *       txn,
    ulong              txn_sz,
    ulong              max_schedule_slot,
    uint               seq_id,
    uchar              batch_idx,
    uchar              batch_cnt,
    uchar              revert_on_error,
    uint               scheduler_arrival_tspub,
    uint               source_ipv4 );

void
fd_bam_publish_batch( fd_bam_tile_t *            ctx,
                      fd_bam_batch_ctx_t *       state,
                      bam_types_AtomicTxnBatch const * batch );

int
fd_bam_should_dump_batch( fd_bam_tile_t * ctx,
                          ulong           max_schedule_slot );

void
fd_bam_handle_scheduler_response( fd_bam_tile_t * ctx,
                                  void const *    data,
                                  ulong           data_sz,
                                  long            rx_ts_ns,
                                  uint            rx_tspub );

/* fd_bam_client_status provides a "check engine light".

   Return codes are FD_PLUGIN_MSG_BAM_UPDATE_STATUS_{...}. */

fd_plugin_bam_update_status_t
fd_bam_client_status( fd_bam_tile_t const * ctx );

/* fd_bam_request_ctx_cstr returns the gRPC method name for a
   FD_BAM_CLIENT_REQ_* ID.  Returns "unknown" the ID is not
   recognized. */

FD_FN_CONST char const *
fd_bam_request_ctx_cstr( ulong request_ctx );

/* fd_bam_client_reset frees all connection-related resources. */

void
fd_bam_client_reset( fd_bam_tile_t * ctx );

/* fd_bam_client_ping_tx enqueues an HTTP/2 keepalive PING frame for
   sending.  This is transport-level keepalive, not BAM scheduler proto
   Ping/Pong. */

void
fd_bam_client_send_ping( fd_bam_tile_t * ctx );

FD_PROTOTYPES_END

#endif /* HEADER_fd_src_disco_bam_fd_bam_tile_private_h */
