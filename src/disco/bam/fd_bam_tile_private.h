#ifndef HEADER_fd_src_disco_bam_fd_bam_tile_private_h
#define HEADER_fd_src_disco_bam_fd_bam_tile_private_h

#include "../bundle/fd_keepalive.h"
#include "../stem/fd_stem.h"
#include "../keyguard/fd_keyswitch.h"
#include "../bam/fd_bam_types.h"
#include "fd_bam_ctrl.h"
#include "../metrics/fd_metrics.h"
#include "../../waltz/grpc/fd_grpc_client.h"
#include "../../waltz/resolv/fd_netdb.h"
#include "../../waltz/fd_rtt_est.h"
#include "proto/bam_api.pb.h"

#define FD_BAM_HEARTBEAT_TIMEOUT_NS ((long)6e9) /* 6 seconds */

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
   published to fd_metrics periodically. */

struct fd_bam_metrics {
  ulong txn_received_cnt;
  ulong bundle_received_cnt;
  ulong bundle_result_drop_cnt;
  ulong packet_drop_cnt;
  ulong ping_ack_cnt;
  ulong heartbeat_sent_cnt;
  ulong heartbeat_recv_cnt;
  ulong connection_cnt;
  ulong disconnect_cnt;

  ulong decode_fail_cnt;
  ulong transport_fail_cnt;
  ulong timeout_fail_cnt;
  ulong missing_builder_info_fail_cnt;

  ulong result_sent_cnt;
  ulong leader_state_sent_cnt;

  fd_histf_t msg_rx_delay[1];
};

typedef struct fd_bam_metrics fd_bam_metrics_t;

/* fd_bam_tile_t is the context object provided to callbacks from
   stem, and contains all state needed to progress the tile. */

/* fd_bam_tile aggregates the long-lived state required to operate the BAM
   scheduler client: networking, authentication, subscriptions, result queues,
   and topology bindings. */

struct fd_bam_tile {
  fd_keyswitch_t * keyswitch;                     /* Manages the identity keypair */
  fd_keyguard_client_t keyguard_client[1];        /* Keyguard client used to request signatures */

  ulong            bank_bam_in_idx;               /* Topology link index for bank->bam input */
  ulong            pack_leader_in_idx;            /* Topology link index for leader->bam input */
  fd_bam_in_ctx_t  bank_in;                       /* Bank bundle ingress dcache context */
  fd_bam_in_ctx_t  leader_in;                     /* Pack tile ingress for leader state/results */

  uchar is_ssl : 1;                                /* Non-zero when TLS is negotiated */
  int  keylog_fd;                                 /* TLS key log output fd (-1 when disabled) */
# if FD_HAS_OPENSSL
  /* OpenSSL */
  SSL_CTX *    ssl_ctx;                           /* Owning TLS context for BAM connection */
  SSL *        ssl;                               /* TLS session bound to tcp_sock */
  fd_alloc_t * ssl_alloc;                         /* Allocator backing OpenSSL init */
# endif /* FD_HAS_OPENSSL */

  /* Currently running config, values loaded via TOML and updated by set_bam admin control */
  fd_bam_ctrl_t * ctrl;                  /* Runtime control shared object (NULL when tile launched without admin support) */
  uchar  enabled;                        /* Whether BAM runtime is enabled by the operator */
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
  long cached_ts;                                 /* Last fd_bam_now() sample for metrics */

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
  fd_ip4_port_t bam_tpu_addr;          /* Latest TPU endpoint advertised by BAM */
  fd_ip4_port_t bam_tpu_fwd_addr;      /* Latest TPU Forward endpoint advertised by BAM */

  /* Bundle state */
  uint  bundle_seq;                               /* Monotonic bundle identifier (0 before first bundle).
                                                     Scheduler batches copy the sender-provided seq_id so
                                                     every txn in the bundle shares the same
                                                     block_engine.bundle_id. */
  uchar bundle_txn_cnt;                           /* Number of txns in current bundle */
  ulong bundle_max_schedule_slot;                 /* Highest slot allowed by scheduler, FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT as default */

  /* BAM specific */
  fd_grpc_h2_stream_t * bam_stream;               /* Current scheduler stream; NULL while unsubscribed or reconnecting */
  long                  bam_last_builder_heartbeat_ns;  /* fd_bam_now() timestamp of last builder heartbeat (0 if none received) */
  long                  bam_last_validator_heartbeat_ns;/* fd_bam_now() timestamp of last validator heartbeat (0 if none received) */
  long                  bam_last_config_poll_ns;         /* fd_bam_now() timestamp of last config poll attempt (0 if never polled) */
  ushort                bam_pending_results;             /* Queue depth of bam_results (0 <= cnt < FD_BAM_MAX_PENDING_RESULTS) */
  ushort                bam_results_head;                /* Index of next result to flush (wraps modulo FD_BAM_MAX_PENDING_RESULTS) */
  ushort                bam_results_tail;                /* Index of next slot to fill (wraps modulo FD_BAM_MAX_PENDING_RESULTS) */
  fd_bam_bundle_result_t bam_results[ FD_BAM_MAX_PENDING_RESULTS ]; /* Ring buffer of bundle outcomes awaiting publication */
  fd_bam_leader_state_t  bam_leader_state;        /* Cached leader-schedule budget received from the BAM node */
  uchar                 bam_identity_pubkey[ 32 ];   /* validator pubkey from the identity keypair */
  char                  bam_identity_pubkey_b58[ FD_BASE58_ENCODED_32_SZ ]; /* Base58-encoded validator pubkey string (NUL-terminated) */
  char                  challenge_to_sign[ sizeof(bam_api_AuthChallengeResponse) ]; /* Latest auth challenge from AuthChallengeResponse.challenge_to_sign field */
  uchar                 bam_challenge_to_sign_len;           /* Length of current auth challenge */
  char                  bam_auth_signature[ FD_BASE58_ENCODED_64_SZ ]; /* Base58-encoded Ed25519 signature for BAM auth (NUL-terminated) */
  uint                  bam_stream_live        : 1;  /* set once bam_stream is established and delivering messages */
  uint                  bam_stream_connecting  : 1;  /* set during gRPC stream handshake before bam_stream_live */
  uint                  bam_auth_ready         : 1;  /* set when bam_auth_challenge/_len contain a fresh challenge to sign */
  uint                  bam_auth_inflight      : 1;  /* true while GetAuthChallenge GRPC call is pending */
  uint                  bam_config_inflight    : 1;  /* true while GetBuilderConfig GRPC call is pending */
  uint                  bam_leader_pending     : 1;  /* set when awaiting a scheduler leader-state response */

  /* Error backoff */
  fd_rng_t rng[1];                                /* RNG used to randomize reconnects */
  uint     backoff_iter;                          /* Backoff iteration counter */
  long     backoff_until;                         /* Earliest ts to retry connection */
  long     backoff_reset;                         /* Errors before this ts reset backoff_iter */

  /* Stem publish */
  fd_stem_context_t * stem;                          /* Cached stem context handed to callbacks */
  fd_bam_out_ctx_t    verify_out;                    /* Output ring for transaction verification */
  fd_bam_out_ctx_t    plugin_out;                    /* Output ring for plugin status updates */
  fd_bam_out_ctx_t    gossip_out;       /* Stem output buffer used for BAM gossip updates */
  ulong *             bam_status_fseq; /* Shared latch written with BAM status (0=inactive,1=active) */
  uchar               gui_dirty;       /* Forces a GUI/plugin update on next publish */

  /* App metrics */
  fd_bam_metrics_t metrics;                         /* Tile-local counters flushed to metrics */

  /* Check engine light */
  uchar bundle_status_recent;  /* most recently observed 'check engine light' */
  uchar bundle_status_plugin;  /* last 'plugin' update written */
  uchar bundle_status_logged;
  long  last_bundle_status_log_nanos;
  long  last_gui_publish_nanos;
};

typedef struct fd_bam_tile fd_bam_tile_t;

FD_FN_UNUSED static inline void
fd_bam_enqueue_result( fd_bam_tile_t *               ctx,
                       fd_bam_bundle_result_t const * res ) {
  if( FD_UNLIKELY( ctx->bam_pending_results>=FD_BAM_MAX_PENDING_RESULTS ) ) {
    FD_LOG_WARNING(( "Dropping BAM bundle result (bam tile queue full): seq_id=%u slot=%lu txn_cnt=%u exec_success=%u sched_err=%u",
                     res->seq_id, res->slot, res->txn_cnt, res->execution_success, res->scheduling_error ));
    ctx->metrics.bundle_result_drop_cnt++;
    FD_MCNT_INC( BAM, BUNDLE_RESULTS_DROPPED, 1UL );
    return;
  }
  ctx->bam_results[ ctx->bam_results_tail ] = *res;
  ctx->bam_results_tail = (ushort)((ctx->bam_results_tail + 1U) % FD_BAM_MAX_PENDING_RESULTS);
  ctx->bam_pending_results = (ushort)( ctx->bam_pending_results + 1U );
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

/* fd_bam_client_grpc_callbacks provides callbacks for grpc_client. */

extern fd_grpc_client_callbacks_t fd_bam_client_grpc_callbacks;

/* fd_bam_client_step is an all-in-one routine to drive client logic.
   As long as the tile calls this periodically, the client will
   reconnect to the bundle server, authenticate, and subscribe to
   packets and bundles. */

void
fd_bam_client_step( fd_bam_tile_t * bundle,
                       int *              charge_busy );

/* fd_bam_client_step_reconnect drives the 'reconnect' state machine.
   Once the HTTP/2 conn is established (SETTINGS exchanged), this
   function drives the auth logic, requests block builder info, sets up
   packet and bundle subscriptions, and PINGs. */

int
fd_bam_client_step_reconnect( fd_bam_tile_t * ctx,
                                 long               now );

/* Expose internal result flushing logic for unit tests. Returns 1 if any
   results were flushed (busy), 0 otherwise. Not used in production. */

int
fd_bam_test_flush_results( fd_bam_tile_t * ctx );

int
fd_bam_test_drive( fd_bam_tile_t * ctx,
                   long             now );

/* fd_bam_tile_backoff is called whenever an error occurs.  Stalls
   forward progress for a randomized amount of time to prevent error
   floods. */

void
fd_bam_tile_backoff( fd_bam_tile_t * ctx,
                        long               now );

void
fd_bam_publish_gossip_update( fd_bam_tile_t *    ctx,
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

/* fd_bam_client_status provides a "check engine light".

   Returns 0 if the client has recently failed and is currently backing
   off from a reconnect attempt.

   Returns 1 if the client is currently reconnecting.

   Returns 2 if all of the following conditions are met:
   - TCP socket is alive
   - SSL session is not in an error state
   - HTTP/2 connection is established (SETTINGS exchange done)
   - gRPC bundle and packet subscriptions are live
   - HTTP/2 PING exchange was done recently

   Return codes are FD_PLUGIN_MSG_BAM_UPDATE_STATUS_{...}. */

int
fd_bam_client_status( fd_bam_tile_t const * ctx );

/* fd_bam_request_ctx_cstr returns the gRPC method name for a
   FD_BAM_CLIENT_REQ_* ID.  Returns "unknown" the ID is not
   recognized. */

FD_FN_CONST char const *
fd_bam_request_ctx_cstr( ulong request_ctx );

/* fd_bam_client_reset frees all connection-related resources. */

void
fd_bam_client_reset( fd_bam_tile_t * ctx );

/* fd_bam_client_ping_tx enqueues a PING frame for sending.  Returns
   1 on success and 0 on failure (occurs when frame_tx buf is full). */

void
fd_bam_client_send_ping( fd_bam_tile_t * ctx );

FD_PROTOTYPES_END

#endif /* HEADER_fd_src_disco_bam_fd_bam_tile_private_h */
