#ifndef HEADER_fd_src_disco_bam_fd_bam_tile_private_h
#define HEADER_fd_src_disco_bam_fd_bam_tile_private_h

#include "../bundle/fd_bundle_auth.h"
#include "../bundle/fd_keepalive.h"
#include "../stem/fd_stem.h"
#include "../keyguard/fd_keyswitch.h"
#include "../keyguard/fd_keyguard_client.h"
#include "../bam/fd_bam_types.h"
#include "../../waltz/grpc/fd_grpc_client.h"
#include "../../waltz/resolv/fd_netdb.h"
#include "../../waltz/fd_rtt_est.h"
#include "../../util/alloc/fd_alloc.h"
#include "../../util/hist/fd_histf.h"
#include "../../ballet/base58/fd_base58.h"

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
  ulong packet_received_cnt;
  ulong shredstream_heartbeat_cnt;
  ulong ping_ack_cnt;

  ulong decode_fail_cnt;
  ulong transport_fail_cnt;
  ulong missing_builder_info_fail_cnt;

  fd_histf_t msg_rx_delay[1];
};

typedef struct fd_bam_metrics fd_bam_metrics_t;

/* fd_bam_tile_t is the context object provided to callbacks from
   stem, and contains all state needed to progress the tile. */

struct fd_bam_tile {
  /* Key switch */
  fd_keyswitch_t * keyswitch;

  /* Key guard */
  fd_keyguard_client_t keyguard_client[1];

  ulong            bank_bam_in_idx;
  ulong            pack_leader_in_idx;
  fd_bam_in_ctx_t  bank_in;
  fd_bam_in_ctx_t  leader_in;

  uint is_ssl : 1;
  int  keylog_fd;
# if FD_HAS_OPENSSL
  /* OpenSSL */
  SSL_CTX *    ssl_ctx;
  SSL *        ssl;
  fd_alloc_t * ssl_alloc;
# endif /* FD_HAS_OPENSSL */

  /* Config */
  char   server_fqdn[ 256 ]; /* cstr */
  ulong  server_fqdn_len;
  char   server_sni[ 256 ]; /* cstr */
  ulong  server_sni_len;
  ushort server_tcp_port;

  /* Resolver */
  fd_netdb_fds_t netdb_fds[1];
  uint server_ip4_addr; /* last DNS lookup result */

  /* TCP socket */
  int  tcp_sock;
  int  so_rcvbuf;
  uint tcp_sock_connected : 1;
  uint defer_reset : 1;
  long cached_ts;

  /* Keepalive via HTTP/2 PINGs (randomized) */
  long              keepalive_interval;
  fd_keepalive_t    keepalive[1];
  fd_rtt_estimate_t rtt[1];

  /* gRPC client */
  void *                   grpc_client_mem;
  ulong                    grpc_buf_max;
  fd_grpc_client_t *       grpc_client;
  fd_grpc_client_metrics_t grpc_metrics[1];
  ulong                    map_seed;

  /* Bundle authenticator */
  fd_bundle_auther_t auther;

  /* Bundle block builder info */
  uchar builder_pubkey[ 32 ];
  uchar builder_commission;  /* in [0,100] (percent) */
  uchar builder_info_avail : 1;  /* Block builder info available? (potentially stale) */
  uchar builder_info_wait  : 1;  /* Request already in-flight? */
  long  builder_info_valid_until;

  /* Bundle subscriptions */
  uchar packet_subscription_live : 1;  /* Want to subscribe to a stream? */
  uchar packet_subscription_wait : 1;  /* Request already in-flight? */
  uchar bundle_subscription_live : 1;
  uchar bundle_subscription_wait : 1;

  /* Bundle state */
  ulong bundle_seq;                               /* Monotonic bundle identifier (0 before first bundle).
                                                     BAM AtomicTxnBatch updates copy the sender-provided
                                                     seq_id here, while SubscribeBundles falls back to
                                                     incrementing this counter so every txn in the bundle
                                                     shares the same block_engine.bundle_id. */
  ulong bundle_txn_cnt;                           /* Number of txns in current bundle */
  ulong bundle_max_schedule_slot;                 /* Highest slot allowed by scheduler */

  /* BAM specific */
  fd_grpc_h2_stream_t * bam_stream;
  long                  bam_last_builder_heartbeat_ns;
  long                  bam_last_validator_heartbeat_ns;
  long                  bam_last_config_poll_ns;
  ulong                 bam_pending_results;
  ulong                 bam_results_head;
  ulong                 bam_results_tail;
  fd_bam_bundle_result_t bam_results[ FD_BAM_MAX_PENDING_RESULTS ];
  fd_bam_leader_state_t  bam_leader_state;
  uchar                 bam_url_pubkey[ 32 ];
  char                  bam_validator_pubkey[ FD_BASE58_ENCODED_32_SZ ];
  char                  bam_auth_challenge[ 256 ];
  uint                  bam_auth_challenge_len;
  char                  bam_auth_signature[ FD_BASE58_ENCODED_64_SZ ];
  uint                  bam_stream_live : 1;
  uint                  bam_stream_connecting : 1;
  uint                  bam_auth_ready : 1;
  uint                  bam_auth_inflight : 1;
  uint                  bam_config_inflight : 1;
  uint                  bam_leader_pending : 1;

  /* Error backoff */
  fd_rng_t rng[1];
  uint     backoff_iter;
  long     backoff_until;
  long     backoff_reset;

  /* Stem publish */
  fd_stem_context_t * stem;
  fd_bam_out_ctx_t verify_out;
  fd_bam_out_ctx_t plugin_out;

  /* App metrics */
  fd_bam_metrics_t metrics;

  /* Check engine light */
  uchar bundle_status_recent;  /* most recently observed 'check engine light' */
  uchar bundle_status_plugin;  /* last 'plugin' update written */
  uchar bundle_status_logged;
  long  last_bundle_status_log_nanos;
};

typedef struct fd_bam_tile fd_bam_tile_t;

/* Define 'request_ctx' IDs to identify different types of gRPC calls */

#define FD_BAM_CLIENT_REQ_Auth_GenerateAuthChallenge         0
#define FD_BAM_CLIENT_REQ_Auth_GenerateAuthTokens            1
#define FD_BAM_CLIENT_REQ_Bundle_SubscribePackets            4
#define FD_BAM_CLIENT_REQ_Bundle_SubscribeBundles            5
#define FD_BAM_CLIENT_REQ_Bundle_GetBlockBuilderFeeInfo      6
#define FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge               7
#define FD_BAM_CLIENT_REQ_BAM_GetConfig                      8
#define FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream            9

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

/* fd_bam_tile_backoff is called whenever an error occurs.  Stalls
   forward progress for a randomized amount of time to prevent error
   floods. */

void
fd_bam_tile_backoff( fd_bam_tile_t * ctx,
                        long               now );

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

   Return codes are compatible with FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_{...}. */

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
