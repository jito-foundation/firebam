/* fd_bam_client.c steps gRPC related tasks. */

#define _GNU_SOURCE /* SOL_TCP */
#include "fd_bam_tile_private.h"
#include "proto/bam_api.pb.h"
#include "proto/bam_types.pb.h"
#include "fd_bam_tile.h"
#include "fd_bam_types.h"
#include "../keyguard/fd_keyguard.h"
#include "../fd_txn_m.h"
#include "../plugin/fd_plugin.h"
#include "../metrics/fd_metrics.h"
#include "../../tango/fseq/fd_fseq.h"
#include "../../waltz/h2/fd_h2_conn.h"
#include "../../waltz/http/fd_url.h" /* fd_url_unescape */
#include "../../ballet/base58/fd_base58.h"
#include "../../ballet/nanopb/pb_decode.h"
#include "../../ballet/nanopb/pb_encode.h"
#include "../../util/net/fd_ip4.h"
#include "../../util/fd_util.h"

#include <fcntl.h>
#include <errno.h>
#include <unistd.h> /* close */
#include <poll.h> /* poll */
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h> /* snprintf */

typedef struct {
  fd_bam_bundle_result_t const * res;
} fd_bam_encode_ctx_t;

typedef struct {
  bam_types_AtomicTxnBatchResult const * atomic_res;
} fd_bam_encode_batch_ctx_t;

__attribute__((weak)) long
fd_bam_now( void ) {
  return fd_log_wallclock();
}

void
fd_bam_tile_backoff( fd_bam_tile_t * ctx,
                        long               now ) {
  uint iter = ctx->backoff_iter;
  if( now < ctx->backoff_reset ) iter = 0U;
  iter++;

  /* FIXME proper backoff */
  long wait_ns = (long)2e9;
  wait_ns = (long)( fd_rng_ulong( ctx->rng ) & ( (1UL<<fd_ulong_find_msb_w_default( (ulong)wait_ns, 0 ))-1UL ) );

  ctx->backoff_until = now +   wait_ns;
  ctx->backoff_reset = now + 2*wait_ns;

  ctx->backoff_iter = iter;
}

static double
fd_bam_client_retry_ms( fd_bam_tile_t * ctx ) {
  long wait_ns = ctx->backoff_until - fd_bam_now();
  if( wait_ns < 0L ) wait_ns = 0L;
  return (double)wait_ns / 1e6;
}

typedef enum {
  FD_BAM_LEADER_PENDING_DROP_CLIENT_RESET = 0,    /* dropped by full client reset */
  FD_BAM_LEADER_PENDING_DROP_REQUEST_FAILED,      /* dropped when stream request fails */
  FD_BAM_LEADER_PENDING_DROP_STREAM_ENDED,        /* dropped when stream ends */
  FD_BAM_LEADER_PENDING_DROP_STREAM_TIMEOUT       /* dropped when stream times out */
} fd_bam_leader_pending_drop_reason_t;

static inline void
fd_bam_set_stream_live( fd_bam_tile_t * ctx,
                        _Bool           live ) {
  if( FD_UNLIKELY( live != ctx->bam_stream_live ) ) {
    ctx->metrics.stream_transition_cnt[ live
      ? FD_METRICS_ENUM_BAM_STREAM_TRANSITION_V_DOWN_TO_LIVE_IDX
      : FD_METRICS_ENUM_BAM_STREAM_TRANSITION_V_LIVE_TO_DOWN_IDX ]++;
  }
  ctx->bam_stream_live = live;
}

static inline void
fd_bam_drop_pending_leader_state( fd_bam_tile_t *                       ctx,
                                  fd_bam_leader_pending_drop_reason_t    reason ) {
  if( FD_LIKELY( !ctx->bam_leader_pending ) ) return;

  switch( reason ) {
  case FD_BAM_LEADER_PENDING_DROP_CLIENT_RESET:
    ctx->metrics.leader_pending_dropped_cnt[ FD_METRICS_ENUM_BAM_LEADER_PENDING_DROP_REASON_V_CLIENT_RESET_IDX ]++;
    break;
  case FD_BAM_LEADER_PENDING_DROP_REQUEST_FAILED:
    ctx->metrics.leader_pending_dropped_cnt[ FD_METRICS_ENUM_BAM_LEADER_PENDING_DROP_REASON_V_REQUEST_FAILED_IDX ]++;
    break;
  case FD_BAM_LEADER_PENDING_DROP_STREAM_ENDED:
    ctx->metrics.leader_pending_dropped_cnt[ FD_METRICS_ENUM_BAM_LEADER_PENDING_DROP_REASON_V_STREAM_ENDED_IDX ]++;
    break;
  case FD_BAM_LEADER_PENDING_DROP_STREAM_TIMEOUT:
    ctx->metrics.leader_pending_dropped_cnt[ FD_METRICS_ENUM_BAM_LEADER_PENDING_DROP_REASON_V_STREAM_TIMEOUT_IDX ]++;
    break;
  default:
    break;
  }

  ctx->bam_leader_pending = 0U;
}

void
fd_bam_client_reset( fd_bam_tile_t * ctx ) {
  long now = fd_bam_now();

  /* Drop the shared BAM-status latch immediately so downstream tiles
     can resume QUIC/bundle ingestion without waiting for housekeeping. */
  if( FD_LIKELY( ctx->bam_status_fseq ) ) fd_fseq_update( ctx->bam_status_fseq, 0UL );
  fd_bam_shred_update( ctx, ctx->stem, 0 );

  if( FD_UNLIKELY( ctx->tcp_sock >= 0 ) ) {
    if( FD_UNLIKELY( 0 != close( ctx->tcp_sock ) ) ) {
      FD_LOG_ERR(( "close(tcp_sock=%i) failed (%i-%s)", ctx->tcp_sock, errno, fd_io_strerror( errno ) ));
    }
    ctx->tcp_sock = -1;
    ctx->tcp_sock_connected = 0;
  }
  /* Leave the last good BAM contact info intact here; the tile decides
     when to fall back to the default ports after seeing the status
     transition so gossip never advertises a half-cleared override. */
  ctx->defer_reset = 0;

  if( FD_UNLIKELY( ctx->builder_info_valid_until && now >= ctx->builder_info_valid_until ) ) {
    ctx->builder_info_valid_until = 0L;
  }
  ctx->bundle_max_schedule_slot = 0UL;

  memset( ctx->rtt, 0, sizeof(fd_rtt_estimate_t) );

# if FD_HAS_OPENSSL
  if( FD_UNLIKELY( ctx->ssl ) ) {
    SSL_free( ctx->ssl );
    ctx->ssl = NULL;
  }
# endif

  fd_bam_tile_backoff( ctx, now );

  fd_grpc_client_reset( ctx->grpc_client );

  ctx->bam_stream                 = NULL;
  fd_bam_set_stream_live( ctx, 0U );
  ctx->bam_stream_connecting      = 0;
  ctx->bam_leader_started_slot    = ULONG_MAX;
  ctx->bam_auth_ready             = 0;
  ctx->challenge_to_sign[ 0 ]     = '\0';
  ctx->bam_auth_inflight          = 0;
  ctx->bam_config_inflight        = 0;
  ctx->bam_config_received        = 0;
  ctx->bam_last_builder_activity_ns = 0L;
  ctx->bam_last_validator_heartbeat_ns = 0L;
  ctx->bam_last_config_poll_ns    = 0L;
  /* Preserve any buffered bundle results so they flush once the next
     scheduler stream comes up.  The server expects every dispatched
     bundle to eventually produce a result; dropping them here would lose
     that guarantee. */
  /* ctx->feedback_queue_depth        = 0UL; */
  /* ctx->bam_results_head           = 0UL; */
  /* ctx->bam_results_tail           = 0UL; */
  fd_bam_drop_pending_leader_state( ctx, FD_BAM_LEADER_PENDING_DROP_CLIENT_RESET );
}

static int
fd_bam_client_do_connect( fd_bam_tile_t const * ctx,
                             uint               ip4_addr ) {
  struct sockaddr_in addr = {
    .sin_family      = AF_INET,
    .sin_addr.s_addr = ip4_addr,
    .sin_port        = fd_ushort_bswap( ctx->server_tcp_port )
  };
  int err = connect( ctx->tcp_sock, fd_type_pun_const( &addr ), sizeof(struct sockaddr_in) );
  /* FD_LIKELY is used here as EINPROGRESS is expected even to local tcp ports */
  if( FD_LIKELY( err==-1 ) ) {
    return errno;
  }
  return 0;
}

static void
fd_bam_client_create_conn( fd_bam_tile_t * ctx ) {
  fd_bam_client_reset( ctx );

  /* FIXME IPv6 support */
  fd_addrinfo_t hints = {0};
  hints.ai_family = AF_INET;
  fd_addrinfo_t * res = NULL;
  uchar scratch[ 4096 ];
  void * pscratch = scratch;
  int err = fd_getaddrinfo( ctx->server_fqdn, &hints, &res, &pscratch, sizeof(scratch) );
  if( FD_UNLIKELY( err ) ) {
    fd_bam_client_reset( ctx );
    ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_RESOLVE_IDX ]++;
    FD_LOG_WARNING(( "fd_getaddrinfo `%s` failed (%d-%s); backing off for %.3f ms",
                     ctx->server_fqdn, err, fd_gai_strerror( err ), fd_bam_client_retry_ms( ctx ) ));
    return;
  }
  uint const ip4_addr = ((struct sockaddr_in *)res->ai_addr)->sin_addr.s_addr;
  ctx->server_ip4_addr = ip4_addr;

  int tcp_sock = socket( AF_INET, SOCK_STREAM|SOCK_CLOEXEC, 0 );
  if( FD_UNLIKELY( tcp_sock < 0 ) ) {
    FD_LOG_ERR(( "socket(AF_INET,SOCK_STREAM|SOCK_CLOEXEC,0) failed (%i-%s)", errno, fd_io_strerror( errno ) ));
  }
  ctx->tcp_sock = tcp_sock;

  if( FD_UNLIKELY( 0 != setsockopt( tcp_sock, SOL_SOCKET, SO_RCVBUF, &ctx->so_rcvbuf, sizeof(int) ) ) ) {
    FD_LOG_ERR(( "setsockopt(SOL_SOCKET,SO_RCVBUF,%i) failed (%i-%s)", ctx->so_rcvbuf, errno, fd_io_strerror( errno ) ));
  }

  int tcp_nodelay = 1;
  if( FD_UNLIKELY( 0 != setsockopt( tcp_sock, SOL_TCP, TCP_NODELAY, &tcp_nodelay, sizeof(int) ) ) ) {
    FD_LOG_ERR(( "setsockopt failed (%d-%s)", errno, fd_io_strerror( errno ) ));
  }

  if( FD_UNLIKELY( fcntl( tcp_sock, F_SETFL, O_NONBLOCK ) == -1 ) ) {
    FD_LOG_ERR(( "fcntl(tcp_sock,F_SETFL,O_NONBLOCK) failed (%i-%s)", errno, fd_io_strerror( errno ) ));
  }

  char const * scheme = "http";
# if FD_HAS_OPENSSL
  if( ctx->is_ssl ) scheme = "https";
# endif

  FD_LOG_INFO(( "Connecting to %s://" FD_IP4_ADDR_FMT ":%hu (%.*s)",
                scheme,
                FD_IP4_ADDR_FMT_ARGS( ip4_addr ), ctx->server_tcp_port,
                (int)ctx->server_sni_len, ctx->server_sni ));

  int connect_err = fd_bam_client_do_connect( ctx, ip4_addr );
  if( FD_LIKELY( connect_err ) ) {
    if( FD_UNLIKELY( connect_err != EINPROGRESS ) ) {
      fd_bam_client_reset( ctx );
      ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_CONNECT_IDX ]++;
      FD_LOG_WARNING(( "connect(tcp_sock," FD_IP4_ADDR_FMT ":%u) failed (%i-%s) (fqdn=%s sni=%.*s); retrying in %.3f ms",
                       FD_IP4_ADDR_FMT_ARGS( ip4_addr ), ctx->server_tcp_port,
                       connect_err, fd_io_strerror( connect_err ),
                       ctx->server_fqdn, (int)ctx->server_sni_len, ctx->server_sni,
                       fd_bam_client_retry_ms( ctx ) ));
      return;
    }
  }

# if FD_HAS_OPENSSL
  if( ctx->is_ssl ) {
    BIO * bio = BIO_new_socket( ctx->tcp_sock, BIO_NOCLOSE );
    if( FD_UNLIKELY( !bio ) ) {
      FD_LOG_ERR(( "BIO_new_socket failed" ));
    }

    SSL * ssl = SSL_new( ctx->ssl_ctx );
    if( FD_UNLIKELY( !ssl ) ) {
      FD_LOG_ERR(( "SSL_new failed" ));
    }

    SSL_set_bio( ssl, bio, bio ); /* moves ownership of bio */
    SSL_set_connect_state( ssl );

    /* Indicate to endpoint which server name we want */
    if( FD_UNLIKELY( !SSL_set_tlsext_host_name( ssl, ctx->server_sni ) ) ) {
      FD_LOG_ERR(( "SSL_set_tlsext_host_name failed" ));
    }

    /* Enable hostname verification */
    if( FD_UNLIKELY( !SSL_set1_host( ssl, ctx->server_sni ) ) ) {
      FD_LOG_ERR(( "SSL_set1_host failed" ));
    }

    ctx->ssl = ssl;
  }
# endif /* FD_HAS_OPENSSL */

  fd_grpc_client_reset( ctx->grpc_client );
  fd_keepalive_init( ctx->keepalive, ctx->rng, ctx->keepalive_interval, ctx->keepalive_interval, fd_bam_now() );
}

static int
fd_bam_client_drive_io( fd_bam_tile_t * ctx,
                           int *              charge_busy ) {
# if FD_HAS_OPENSSL
  if( ctx->is_ssl ) {
    return fd_grpc_client_rxtx_ossl( ctx->grpc_client, ctx->ssl, charge_busy );
  }
# endif /* FD_HAS_OPENSSL */

  return fd_grpc_client_rxtx_socket( ctx->grpc_client, ctx->tcp_sock, charge_busy );
}

/* Forwards a bundle transaction to the tango message bus. */
void
fd_bam_tile_publish_bundle_txn(
    fd_bam_tile_t * ctx,
    void const *       txn,
    ushort             txn_sz,  /* <= FD_TXN_MTU */
    uchar              bundle_txn_cnt,
    uchar              batch_idx,
    uint               scheduler_arrival_tspub,
    uint               source_ipv4
) {
  fd_txn_m_t * txnm = fd_chunk_to_laddr( ctx->verify_out.mem, ctx->verify_out.chunk );
  *txnm = (fd_txn_m_t) {
    .reference_slot = 0UL,
    .payload_sz     = txn_sz,
    .txn_t_sz       = 0U,
    .source_ipv4    = source_ipv4,
    .source_tpu     = FD_TXN_M_TPU_SOURCE_BAM,
    .scheduler_arrival_tspub = scheduler_arrival_tspub,
    .block_engine   = {0},
    .bam = {
      .max_schedule_slot = ctx->bundle_max_schedule_slot,
      .seq_id            = ctx->bundle_seq,
      .txn_cnt         = bundle_txn_cnt,
      .batch_idx         = batch_idx,
      .revert_on_error   = 1, // FIXME: check if this is correct
    },
  };
  fd_memcpy( fd_txn_m_payload( txnm ), txn, txn_sz );

  ulong sz  = fd_txn_m_realized_footprint( txnm, 0, 0 );

  if( FD_UNLIKELY( !ctx->stem ) ) {
    FD_LOG_CRIT(( "ctx->stem not set. This is a bug." ));
  }

  fd_stem_publish( ctx->stem, ctx->verify_out.idx, 1, ctx->verify_out.chunk, sz, 0UL, 0UL, fd_frag_meta_ts_comp( fd_bam_now() ) );
  ctx->verify_out.chunk = fd_dcache_compact_next( ctx->verify_out.chunk, sz, ctx->verify_out.chunk0, ctx->verify_out.wmark );
  ctx->metrics.transaction_published_cnt++;
}

/* Forwards a regular transaction to the tango message bus. */
void
fd_bam_tile_publish_txn(
    fd_bam_tile_t * ctx,
    void const *       txn,
    ulong              txn_sz,  /* <= FD_TXN_MTU */
    ulong              max_schedule_slot,
    uint               seq_id,
    uchar              batch_idx,
    uchar              batch_cnt,
    uchar              revert_on_error,
    uint               scheduler_arrival_tspub,
    uint               source_ipv4
) {
  fd_txn_m_t * txnm = fd_chunk_to_laddr( ctx->verify_out.mem, ctx->verify_out.chunk );
  *txnm = (fd_txn_m_t) {
    .reference_slot = 0UL,
    .payload_sz     = (ushort)txn_sz,
    .txn_t_sz       = 0U,
    .source_ipv4    = source_ipv4,
    .source_tpu     = FD_TXN_M_TPU_SOURCE_BAM,
    .scheduler_arrival_tspub = scheduler_arrival_tspub,
    .block_engine   = {0},
    .bam = {
      .max_schedule_slot = max_schedule_slot,
      .seq_id            = seq_id,
      .txn_cnt         = batch_cnt,
      .batch_idx         = batch_idx,
      .revert_on_error   = !!revert_on_error,
    },
  };
  fd_memcpy( fd_txn_m_payload( txnm ), txn, txn_sz );

  ulong sz  = fd_txn_m_realized_footprint( txnm, 0, 0 );

  if( FD_UNLIKELY( !ctx->stem ) ) {
    FD_LOG_CRIT(( "ctx->stem not set. This is a bug." ));
  }

  fd_stem_publish( ctx->stem, ctx->verify_out.idx, 0, ctx->verify_out.chunk, sz, 0UL, 0UL, fd_frag_meta_ts_comp( fd_bam_now() ) );
  ctx->verify_out.chunk = fd_dcache_compact_next( ctx->verify_out.chunk, sz, ctx->verify_out.chunk0, ctx->verify_out.wmark );
  ctx->metrics.transaction_published_cnt++;
}

static bool
fd_bam_encode_committed_cb( pb_ostream_t *          stream,
                            pb_field_t const *       field,
                            void * const *           arg ) {
  fd_bam_encode_ctx_t const * ctx = (fd_bam_encode_ctx_t const *)*arg;
  if( FD_UNLIKELY( !ctx || !ctx->res ) ) return false;
  fd_bam_bundle_result_t const * res = ctx->res;
  for( uchar i=0U; i<res->bundle_txn_cnt; i++ ) {
    bam_types_TransactionCommittedResult txn_res = bam_types_TransactionCommittedResult_init_default;
    txn_res.cus_consumed               = res->consumed_cus[ i ];
    txn_res.feepayer_balance_lamports  = res->feepayer_balance_lamports[ i ];
    txn_res.loaded_accounts_data_size  = res->loaded_accounts_data_size[ i ];
    txn_res.execution_success          = ( res->sanitize_success[ i ] && !res->transaction_err_count );
    if( FD_UNLIKELY( !pb_encode_tag_for_field( stream, field ) ) ) return false;
    if( FD_UNLIKELY( !pb_encode_submessage( stream, bam_types_TransactionCommittedResult_fields, &txn_res ) ) ) return false;
  }
  return true;
}

static bool
fd_bam_encode_batch_results_cb( pb_ostream_t *          stream,
                                pb_field_t const *       field,
                                void * const *           arg ) {
  fd_bam_encode_batch_ctx_t const * ctx = (fd_bam_encode_batch_ctx_t const *)*arg;
  if( FD_UNLIKELY( !ctx || !ctx->atomic_res ) ) return false;
  if( FD_UNLIKELY( !pb_encode_tag_for_field( stream, field ) ) ) return false;
  return pb_encode_submessage( stream, bam_types_AtomicTxnBatchResult_fields, ctx->atomic_res );
}

static void
fd_bam_request_auth_challenge( fd_bam_tile_t * ctx ) {
  if( FD_UNLIKELY( fd_grpc_client_request_is_blocked( ctx->grpc_client ) ) ) return;

  bam_api_AuthChallengeRequest req = bam_api_AuthChallengeRequest_init_default;
  static char const path[] = "/bam_api.BamNodeApi/GetAuthChallenge";
  fd_grpc_h2_stream_t * request = fd_grpc_client_request_start_ex(
      ctx->grpc_client,
      path, sizeof(path)-1,
      FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge,
      &bam_api_AuthChallengeRequest_msg, &req,
      NULL, 0,
      1
  );
  if( FD_UNLIKELY( !request ) ) return;

  ctx->bam_auth_inflight = 1;
  fd_grpc_client_deadline_set( request,
                               FD_GRPC_DEADLINE_RX_END,
                               fd_bam_now() + FD_BAM_CLIENT_REQUEST_TIMEOUT );
}

static void
fd_bam_request_config( fd_bam_tile_t * ctx,
                        long               now ) {
  if( FD_UNLIKELY( fd_grpc_client_request_is_blocked( ctx->grpc_client ) ) ) return;

  bam_api_ConfigRequest req = bam_api_ConfigRequest_init_default;
  static char const path[] = "/bam_api.BamNodeApi/GetBuilderConfig";
  fd_grpc_h2_stream_t * request = fd_grpc_client_request_start_ex(
      ctx->grpc_client,
      path, sizeof(path)-1,
      FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig,
      &bam_api_ConfigRequest_msg, &req,
      NULL, 0,
      1
  );
  if( FD_UNLIKELY( !request ) ) return;

  ctx->bam_last_config_poll_ns = now;
  ctx->bam_config_inflight     = 1;
  fd_grpc_client_deadline_set( request,
                               FD_GRPC_DEADLINE_RX_END,
                               fd_bam_now() + FD_BAM_CLIENT_REQUEST_TIMEOUT );
}

/* Decodes bam_api.AuthChallengeResponse. Returns 1 after storing the challenge
   and preparing the auth signature; returns 0 on protobuf or validation
   failure (clearing inflight state so the caller can retry). */
static int
fd_bam_handle_auth_challenge( fd_bam_tile_t * ctx,
                              void const *      data,
                              ulong             data_sz ) {
  pb_istream_t istream = pb_istream_from_buffer( data, data_sz );
  bam_api_AuthChallengeResponse resp = bam_api_AuthChallengeResponse_init_default;
  if( FD_UNLIKELY( !pb_decode( &istream, &bam_api_AuthChallengeResponse_msg, &resp ) ) ) {
    ctx->bam_auth_inflight = 0;
    ctx->bam_auth_ready    = 0;
    ctx->challenge_to_sign[ 0 ] = '\0';
    FD_LOG_WARNING(( "Protobuf decode of (bam_api.AuthChallengeResponse) failed" ));
    return 0;
  }

  size_t challenge_len = strnlen( resp.challenge_to_sign, sizeof(resp.challenge_to_sign) );
  if( FD_UNLIKELY( challenge_len == sizeof(resp.challenge_to_sign) ) ) {
    ctx->bam_auth_inflight = 0;
    ctx->bam_auth_ready    = 0;
    ctx->challenge_to_sign[ 0 ] = '\0';
    FD_LOG_WARNING(( "AuthChallengeResponse challenge not NUL terminated" ));
    return 0;
  }

  ctx->bam_auth_inflight = 0;
  fd_memcpy( ctx->challenge_to_sign, resp.challenge_to_sign, sizeof(ctx->challenge_to_sign) );

  uchar  sign_payload[ FD_BAM_AUTH_LABEL_LEN + sizeof(bam_api_AuthChallengeResponse) ]; // the null is to be included
  fd_memcpy( sign_payload, FD_BAM_AUTH_LABEL, FD_BAM_AUTH_LABEL_LEN );
  fd_memcpy( sign_payload + FD_BAM_AUTH_LABEL_LEN, resp.challenge_to_sign, sizeof(bam_api_AuthChallengeResponse) );

  uchar signature[ 64 ];
  fd_keyguard_client_sign( ctx->keyguard_client, signature, sign_payload, FD_BAM_AUTH_LABEL_LEN + challenge_len, FD_KEYGUARD_SIGN_TYPE_ED25519 );

  fd_base58_encode_64( signature, NULL, ctx->bam_auth_signature );
  ctx->bam_auth_ready = 1;
  return 1;
}

/* Decodes bam_api.ConfigResponse. On protobuf failure it increments the
   config-decode failure metric and returns early; otherwise it updates cached
   BAM config in place and applies builder config atomically (all-or-nothing). */
static void
fd_bam_handle_config( fd_bam_tile_t * ctx,
                      void const *    protobuf,
                      ulong           protobuf_sz ) {
  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  pb_istream_t istream = pb_istream_from_buffer( protobuf, protobuf_sz );
  if( FD_UNLIKELY( !pb_decode( &istream, &bam_api_ConfigResponse_msg, &resp ) ) ) {
    ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_CONFIG_DECODE_IDX ]++;
    FD_LOG_WARNING(( "Protobuf decode of (bam_api.ConfigResponse) failed" ));
    return;
  }

  if( FD_LIKELY( resp.has_block_engine_config ) ) {
    bam_types_BlockEngineBuilderConfig const * cfg = &resp.block_engine_config;
    _Bool commission_ok = FD_LIKELY( cfg->builder_commission <= 100UL );
    uchar decoded_builder_pubkey[ 32 ];
    _Bool pubkey_ok = FD_LIKELY( fd_base58_decode_32( cfg->builder_pubkey, decoded_builder_pubkey ) );

    if( FD_UNLIKELY( !commission_ok ) ) {
      FD_LOG_WARNING(( "BlockEngine builder commission out of range (0-100): %u", cfg->builder_commission ));
    }
    if( FD_UNLIKELY( !pubkey_ok ) ) {
      FD_LOG_HEXDUMP_WARNING(( "Invalid builder pubkey in ConfigResponse",
                               cfg->builder_pubkey,
                               strnlen( cfg->builder_pubkey, sizeof( cfg->builder_pubkey ) ) ));
    }

    /* Apply builder info atomically to avoid mixed old/new state. */
    if( FD_LIKELY( commission_ok && pubkey_ok ) ) {
      ctx->builder_commission = (uchar)cfg->builder_commission;
      fd_memcpy( ctx->builder_pubkey, decoded_builder_pubkey, sizeof(ctx->builder_pubkey) );
      ctx->builder_info_valid_until = fd_bam_now() + (long)( 60e9 * 5. );
    }
  }

  if( FD_UNLIKELY( !resp.has_bam_config ) ) {
    FD_LOG_WARNING(( "Missing BAM config in ConfigResponse" ));
    return;
  }
  ctx->bam_config_received = 1U;

  bam_types_BamConfig const * cfg = &resp.bam_config;
  fd_ip4_port_t prev_tpu     = ctx->bam_tpu;
  fd_ip4_port_t prev_tpu_fwd = ctx->bam_tpu_fwd;
  ulong         prev_shred_sock_cnt = ctx->bam_shred_sock_cnt;
  fd_ip4_port_t new_tpu     = {0};
  fd_ip4_port_t new_tpu_fwd = {0};
  fd_ip4_port_t new_shred_sock[ FD_BAM_SHRED_SOCK_MAX ] = {0};
  ulong         new_shred_sock_cnt = 0UL;

  if( cfg->has_tpu_sock ) {
    uint ip4;
    if( FD_LIKELY( fd_cstr_to_ip4_addr( cfg->tpu_sock.ip, &ip4 ) ) &&
        FD_LIKELY( cfg->tpu_sock.port > 0 && cfg->tpu_sock.port <= USHORT_MAX ) ) {
      new_tpu = (fd_ip4_port_t){ .addr = ip4, .port = fd_ushort_bswap( (ushort)cfg->tpu_sock.port ) };
    } else {
      FD_LOG_WARNING(( "Invalid BAM TPU socket in ConfigResponse: " FD_IP4_ADDR_FMT ":%hu",
                    FD_IP4_ADDR_FMT_ARGS( new_tpu.addr ),
                    fd_ushort_bswap( new_tpu.port )
                    ));
    }
  }

  if( cfg->has_tpu_fwd_sock ) {
    uint ip4;
    if( FD_LIKELY( fd_cstr_to_ip4_addr( cfg->tpu_fwd_sock.ip, &ip4 ) ) &&
        FD_LIKELY( cfg->tpu_fwd_sock.port > 0 && cfg->tpu_fwd_sock.port <= USHORT_MAX ) ) {
      new_tpu_fwd = (fd_ip4_port_t){ .addr = ip4, .port = fd_ushort_bswap( (ushort)cfg->tpu_fwd_sock.port ) };
    } else {
      FD_LOG_WARNING(( "Invalid BAM TPU forward socket in ConfigResponse: " FD_IP4_ADDR_FMT ":%hu",
                    FD_IP4_ADDR_FMT_ARGS( new_tpu_fwd.addr ),
                    fd_ushort_bswap( new_tpu_fwd.port )
                    ));
    }
  }

  for( ulong i=0UL; i<(ulong)cfg->shred_sock_count; i++ ) {
    bam_types_Socket const * sock = &cfg->shred_sock[ i ];
    uint ip4;
    if( FD_LIKELY( fd_cstr_to_ip4_addr( sock->ip, &ip4 ) ) &&
        FD_LIKELY( sock->port > 0 && sock->port <= USHORT_MAX ) ) {
      new_shred_sock[ new_shred_sock_cnt++ ] = (fd_ip4_port_t){ .addr = ip4, .port = fd_ushort_bswap( (ushort)sock->port ) };
    } else {
      FD_LOG_WARNING(( "Dropping invalid BAM shred receiver socket in ConfigResponse: %s:%u", sock->ip, sock->port ));
    }
  }

  fd_plugin_bam_update_status_t status = fd_bam_client_status( ctx );
  /* A disconnect means Firedancer should resume advertising its local
     TPU ports so TPU clients do not get stuck targeting the BAM host. */
  _Bool has_valid_contact = !!new_tpu.addr && !!new_tpu.port && !!new_tpu_fwd.addr && !!new_tpu_fwd.port;
  if ( FD_LIKELY( has_valid_contact ) ) {
    ctx->bam_tpu     = new_tpu;
    ctx->bam_tpu_fwd = new_tpu_fwd;
    _Bool tpu_changed = ( prev_tpu.l != ctx->bam_tpu.l || prev_tpu_fwd.l != ctx->bam_tpu_fwd.l );
    if( FD_UNLIKELY( tpu_changed ) ) ctx->tpu_update_state = FD_BAM_TPU_UPDATE_STATE_UNKNOWN;
  } else {
    FD_LOG_WARNING(( "Received incomplete or invalid TPU config; preserving prior BAM TPU config" ));
  }

  _Bool shred_changed = ( prev_shred_sock_cnt != new_shred_sock_cnt ) ||
                        ( new_shred_sock_cnt &&
                          0!=memcmp( ctx->bam_shred_sock, new_shred_sock, new_shred_sock_cnt * sizeof(fd_ip4_port_t) ) );
  if( FD_UNLIKELY( shred_changed ) ) {
    ctx->bam_shred_sock_cnt = new_shred_sock_cnt;
    fd_memcpy( ctx->bam_shred_sock, new_shred_sock, new_shred_sock_cnt * sizeof(fd_ip4_port_t) );
  }

  ctx->gui_dirty = 1U;
  /* Only switch TPU adverts to BAM when the client is actually healthy.
     Config responses can arrive while connecting or after an admin disable,
     and should not force a BAM TPU override. */
  fd_bam_gossip_update( ctx, ctx->stem, ( status == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY ) && has_valid_contact );
  if( FD_UNLIKELY( shred_changed ) ) {
    fd_bam_shred_update( ctx, ctx->stem, status == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
  }

  // update fee config
  _Bool bam_config_fee_updated = false;
  ushort new_commission_bps = (ushort)fd_uint_min( cfg->commission_bps, 10000U );
  if( FD_UNLIKELY( ctx->commission_bps != new_commission_bps ) ) {
    ctx->commission_bps = new_commission_bps;
    bam_config_fee_updated = 1;
  }

  if( cfg->prio_fee_recipient_pubkey[0] ) {
    uchar decoded[ 32 ];
    if( FD_LIKELY( fd_base58_decode_32( cfg->prio_fee_recipient_pubkey, decoded ) ) ) {
      /* Either we have not seen a key before, or the pubkey changed. */
      if( FD_UNLIKELY( !ctx->prio_fee_recipient_set || !!memcmp( ctx->prio_fee_recipient, decoded, sizeof( decoded ) ) ) ) {
        fd_memcpy( ctx->prio_fee_recipient, decoded, sizeof( decoded ) );
        ctx->prio_fee_recipient_set = 1U;
        bam_config_fee_updated = true;
      }
    } else {
      FD_LOG_HEXDUMP_WARNING(( "Invalid priority fee recipient pubkey in ConfigResponse",
                               cfg->prio_fee_recipient_pubkey,
                               strnlen( cfg->prio_fee_recipient_pubkey, sizeof( cfg->prio_fee_recipient_pubkey ) ) ));
    }
  } else if( FD_UNLIKELY( ctx->prio_fee_recipient_set ) ) {
    /* BAM sent no pubkey, so update the state to match. */
    fd_memset( ctx->prio_fee_recipient, 0, sizeof( ctx->prio_fee_recipient ) );
    ctx->prio_fee_recipient_set = 0U;
    bam_config_fee_updated = true;
  }

  if( FD_UNLIKELY( bam_config_fee_updated ) ) {
    /* Broadcast the new validator fee settings to shared memory readers. */
    ctx->fee_cfg_version++;
    fd_memcpy( ctx->fee_cfg->prio_fee_recipient, ctx->prio_fee_recipient, sizeof( ctx->fee_cfg->prio_fee_recipient ) );
    ctx->fee_cfg->commission_bps         = ctx->commission_bps;
    ctx->fee_cfg->has_prio_fee_recipient = ctx->prio_fee_recipient_set;
    FD_COMPILER_MFENCE();
    ctx->fee_cfg->version = ctx->fee_cfg_version;
    FD_COMPILER_MFENCE();
  }
}

static void
fd_bam_try_start_stream( fd_bam_tile_t * ctx ) {
  if( FD_UNLIKELY( !ctx->bam_auth_ready ) ) return;
  if( FD_UNLIKELY( ctx->bam_stream || ctx->bam_stream_connecting ) ) return;
  if( FD_UNLIKELY( fd_grpc_client_request_is_blocked( ctx->grpc_client ) ) ) return;

  bam_types_AuthProof proof = bam_types_AuthProof_init_default;
  fd_memcpy( proof.challenge_to_sign, ctx->challenge_to_sign, sizeof(proof.challenge_to_sign) );
  strlcpy( proof.validator_pubkey, ctx->bam_identity_pubkey_b58, sizeof(proof.validator_pubkey) );
  strlcpy( proof.signature, ctx->bam_auth_signature, sizeof(proof.signature ) );

  bam_api_SchedulerMessage msg = bam_api_SchedulerMessage_init_default;
  msg.which_versioned_msg               = bam_api_SchedulerMessage_v0_tag;
  msg.versioned_msg.v0.which_msg        = bam_api_SchedulerMessageV0_auth_proof_tag;
  msg.versioned_msg.v0.msg.auth_proof   = proof;

  static char const path[] = "/bam_api.BamNodeApi/InitSchedulerStream";
  fd_grpc_h2_stream_t * stream = fd_grpc_client_request_start_ex(
      ctx->grpc_client,
      path, sizeof(path)-1,
      FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream,
      &bam_api_SchedulerMessage_msg, &msg,
      NULL, 0,
      0
  );
  if( FD_UNLIKELY( !stream ) ) {
    size_t challenge_len = strnlen( proof.challenge_to_sign, sizeof( proof.challenge_to_sign ) );
    size_t pubkey_len    = strnlen( proof.validator_pubkey, sizeof( proof.validator_pubkey ) );
    size_t sig_len       = strnlen( proof.signature, sizeof( proof.signature ) );
    FD_LOG_WARNING(( "Failed BAM GRPC call `InitSchedulerStream` with auth proof (challenge_len=%lu pubkey_len=%lu sig_len=%lu) challenge=\"%.*s\" validator_pubkey=\"%.*s\" signature=\"%.*s\"",
                  (ulong)challenge_len, (ulong)pubkey_len, (ulong)sig_len,
                  (int)challenge_len, proof.challenge_to_sign,
                  (int)pubkey_len, proof.validator_pubkey,
                  (int)sig_len, proof.signature ));
    return;
  }
  ctx->bam_stream            = stream;
  ctx->bam_stream_connecting = 1;
  ctx->bam_auth_ready        = 0;
  ctx->challenge_to_sign[ 0 ] = '\0';
}

static void
fd_bam_send_heartbeat( fd_bam_tile_t * ctx,
                        long               now ) {
  if( FD_UNLIKELY( !ctx->bam_stream || !ctx->bam_stream_live ) ) return;
  bam_api_SchedulerMessage msg = bam_api_SchedulerMessage_init_default;
  msg.which_versioned_msg        = bam_api_SchedulerMessage_v0_tag;
  msg.versioned_msg.v0.which_msg = bam_api_SchedulerMessageV0_heart_beat_tag;
  bam_types_ValidatorHeartBeat hb = bam_types_ValidatorHeartBeat_init_default;
  hb.time_sent_microseconds = (ulong)fd_long_max(now / 1000, 0);
  msg.versioned_msg.v0.msg.heart_beat = hb;
  int send_res = fd_grpc_client_stream_send( ctx->grpc_client, ctx->bam_stream, &bam_api_SchedulerMessage_msg, &msg, 0 );
  if( FD_LIKELY( send_res ) ) ctx->bam_last_validator_heartbeat_ns = now;
  ctx->metrics.outbound_enqueue_outcome_cnt[
      send_res
      ? FD_METRICS_ENUM_BAM_ENQUEUE_OUTCOME_V_HEARTBEAT_ENQUEUED_IDX
      : FD_METRICS_ENUM_BAM_ENQUEUE_OUTCOME_V_HEARTBEAT_ENQUEUE_FAIL_IDX ]++;
}

static int
fd_bam_send_result( fd_bam_tile_t *               ctx,
                    fd_bam_bundle_result_t const * res ) {
  if( FD_UNLIKELY( !ctx->bam_stream || !ctx->bam_stream_live ) ) {
    ctx->metrics.outbound_enqueue_outcome_cnt[ FD_METRICS_ENUM_BAM_ENQUEUE_OUTCOME_V_RESULT_NO_STREAM_IDX ]++;
    return 0;
  }

  bam_types_AtomicTxnBatchResult atomic_res = bam_types_AtomicTxnBatchResult_init_default;
  atomic_res.seq_id = res->seq_id;

  fd_bam_encode_ctx_t encode_ctx = { .res = res };

  if( FD_LIKELY( res->execution_success ) ) {
    atomic_res.which_result = bam_types_AtomicTxnBatchResult_committed_tag;
    atomic_res.result.committed.transaction_results.funcs.encode = fd_bam_encode_committed_cb;
    atomic_res.result.committed.transaction_results.arg          = &encode_ctx;
  } else {
    atomic_res.which_result = bam_types_AtomicTxnBatchResult_not_committed_tag;
    bam_types_NotCommitted * out = &atomic_res.result.not_committed;
    *out = (bam_types_NotCommitted)bam_types_NotCommitted_init_default;

    switch( res->bundle_err ) {
    case FD_BAM_BUNDLE_ERR_NONE:
      break;
    case FD_BAM_BUNDLE_ERR_DESER:
      if( FD_UNLIKELY( res->deser_reason > _bam_types_DeserializationErrorReason_MAX ) ) {
        out->which_reason = bam_types_NotCommitted_generic_invalid_tag;
        snprintf( out->reason.generic_invalid.message,
                  sizeof(out->reason.generic_invalid.message),
                  "invalid deserialization error %u",
                  res->deser_reason );
        break;
      }
      out->which_reason                        = bam_types_NotCommitted_deserialization_error_tag;
      out->reason.deserialization_error.index  = res->deser_index;
      out->reason.deserialization_error.reason = (bam_types_DeserializationErrorReason)res->deser_reason;
      break;
    case FD_BAM_BUNDLE_ERR_GENERIC_INVALID:
      out->which_reason = bam_types_NotCommitted_generic_invalid_tag;
      if( FD_UNLIKELY( res->generic_invalid_reason==FD_BAM_ERR_GENERIC_INVALID_NONE ||
                       res->generic_invalid_reason>=FD_BAM_ERR_GENERIC_INVALID_CNT ||
                       !FD_BAM_ERR_GENERIC_INVALID_STRINGS[ res->generic_invalid_reason ] ) ) {
        snprintf( out->reason.generic_invalid.message,
                  sizeof(out->reason.generic_invalid.message),
                  "invalid generic-invalid reason %u",
                  res->generic_invalid_reason );
        break;
      }
      strlcpy( out->reason.generic_invalid.message,
               FD_BAM_ERR_GENERIC_INVALID_STRINGS[ res->generic_invalid_reason ],
               sizeof(out->reason.generic_invalid.message) );
      break;
    default:
      FD_LOG_WARNING(( "Invalid error type %u, value out of range.", res->bundle_err ));
      out->which_reason = bam_types_NotCommitted_generic_invalid_tag;
      snprintf( out->reason.generic_invalid.message,
                sizeof(out->reason.generic_invalid.message),
                "invalid bundle error %u",
                res->bundle_err );
      break;
    }

    if( FD_UNLIKELY( !out->which_reason && res->scheduling_error != FD_BAM_SCHED_ERR_NONE ) ) {
      if( FD_LIKELY( res->scheduling_error <= _bam_types_SchedulingError_MAX ) ) {
        out->which_reason            = bam_types_NotCommitted_scheduling_error_tag;
        out->reason.scheduling_error = (bam_types_SchedulingError)res->scheduling_error;
      } else {
        out->which_reason = bam_types_NotCommitted_generic_invalid_tag;
        snprintf( out->reason.generic_invalid.message,
                  sizeof(out->reason.generic_invalid.message),
                  FD_BAM_ERR_FMT_INVALID_SCHEDULING_ERROR,
                  res->scheduling_error );
      }
    }

    if( FD_UNLIKELY( !out->which_reason ) ) {
      for( uchar i=0U; i<res->bundle_txn_cnt; i++ ) {
        if( FD_UNLIKELY( !res->sanitize_success[ i ] ) ) {
          out->which_reason                        = bam_types_NotCommitted_deserialization_error_tag;
          out->reason.deserialization_error.index  = i;
          out->reason.deserialization_error.reason = bam_types_DeserializationErrorReason_SANITIZE_ERROR;
          break;
        }
      }
    }

    if( FD_UNLIKELY( !out->which_reason && res->transaction_err_count ) ) {
      uchar err_idx = 0U;
      _Bool found_non_cancelled = 0;
      for( uchar i=0U; i<res->bundle_txn_cnt; i++ ) {
        if( FD_LIKELY( res->transaction_err[ i ] != bam_types_TransactionErrorReason_COMMIT_CANCELLED ) ) {
          err_idx = i;
          found_non_cancelled = 1;
          break;
        }
      }

      if( FD_UNLIKELY( !found_non_cancelled && res->bundle_txn_cnt>1U ) ) {
        out->which_reason            = bam_types_NotCommitted_scheduling_error_tag;
        out->reason.scheduling_error = bam_types_SchedulingError_POH_TIMEOUT;
      } else if( FD_LIKELY( res->transaction_err[ err_idx ] < _bam_types_TransactionErrorReason_ARRAYSIZE ) ) {
        out->which_reason                    = bam_types_NotCommitted_transaction_error_tag;
        out->reason.transaction_error.index  = err_idx;
        out->reason.transaction_error.reason = res->transaction_err[ err_idx ];
      } else {
        out->which_reason = bam_types_NotCommitted_generic_invalid_tag;
        snprintf( out->reason.generic_invalid.message,
                  sizeof(out->reason.generic_invalid.message),
                  FD_BAM_ERR_FMT_TRANSACTION_ERROR,
                  res->transaction_err[ err_idx ] );
      }
    }

    if( FD_UNLIKELY( !out->which_reason ) ) {
      out->which_reason = bam_types_NotCommitted_generic_invalid_tag;
      strlcpy( out->reason.generic_invalid.message,
               FD_BAM_ERR_MSG_BUNDLE_EXECUTION_FAILED,
               sizeof(out->reason.generic_invalid.message) );
      out->reason.generic_invalid.message[ sizeof(out->reason.generic_invalid.message)-1UL ] = '\0';
    }
  }

  bam_types_MultipleAtomicTxnBatchResult multi = bam_types_MultipleAtomicTxnBatchResult_init_default;
  fd_bam_encode_batch_ctx_t batch_ctx = { .atomic_res = &atomic_res };
  multi.results.funcs.encode = fd_bam_encode_batch_results_cb;
  multi.results.arg          = &batch_ctx;

  bam_api_SchedulerMessage msg = bam_api_SchedulerMessage_init_default;
  msg.which_versioned_msg                        = bam_api_SchedulerMessage_v0_tag;
  msg.versioned_msg.v0.which_msg                 = bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag;
  msg.versioned_msg.v0.msg.multiple_atomic_txn_batch_result = multi;

  int send_res = fd_grpc_client_stream_send( ctx->grpc_client, ctx->bam_stream, &bam_api_SchedulerMessage_msg, &msg, 0 );
  ctx->metrics.outbound_enqueue_outcome_cnt[
      send_res
      ? FD_METRICS_ENUM_BAM_ENQUEUE_OUTCOME_V_RESULT_ENQUEUED_IDX
      : FD_METRICS_ENUM_BAM_ENQUEUE_OUTCOME_V_RESULT_ENQUEUE_FAIL_IDX ]++;
  return send_res;
}

int
fd_bam_send_leader_state( fd_bam_tile_t *                ctx,
                          fd_bam_leader_state_t const *  state ) {
  if( FD_UNLIKELY( !ctx->bam_stream || !ctx->bam_stream_live ) ) {
    ctx->metrics.outbound_enqueue_outcome_cnt[ FD_METRICS_ENUM_BAM_ENQUEUE_OUTCOME_V_LEADER_STATE_NO_STREAM_IDX ]++;
    return 0;
  }

  bam_types_LeaderState ls = bam_types_LeaderState_init_default;
  ls.slot                    = state->slot;
  ls.tick                    = state->tick;
  ls.slot_cu_budget_remaining = state->slot_cu_budget_remaining;

  bam_api_SchedulerMessage msg = bam_api_SchedulerMessage_init_default;
  msg.which_versioned_msg        = bam_api_SchedulerMessage_v0_tag;
  msg.versioned_msg.v0.which_msg = bam_api_SchedulerMessageV0_leader_state_tag;
  msg.versioned_msg.v0.msg.leader_state = ls;

  int send_res = fd_grpc_client_stream_send( ctx->grpc_client, ctx->bam_stream, &bam_api_SchedulerMessage_msg, &msg, 0 );
  ctx->metrics.outbound_enqueue_outcome_cnt[
      send_res
      ? FD_METRICS_ENUM_BAM_ENQUEUE_OUTCOME_V_LEADER_STATE_ENQUEUED_IDX
      : FD_METRICS_ENUM_BAM_ENQUEUE_OUTCOME_V_LEADER_STATE_ENQUEUE_FAIL_IDX ]++;
  return send_res;
}

static int
fd_bam_flush_results( fd_bam_tile_t * ctx ) {
  int busy = 0;
  while( ctx->feedback_queue_depth ) {
    fd_bam_bundle_result_t const * res =
        &ctx->bam_results[ ctx->bam_results_head ];
    if( FD_UNLIKELY( !fd_bam_send_result( ctx, res ) ) ) break;
    ctx->bam_results_head = (ctx->bam_results_head + 1) % FD_BAM_MAX_PENDING_RESULTS;
    ctx->feedback_queue_depth--;
    busy = 1;
  }
  return busy;
}

int
fd_bam_test_flush_results( fd_bam_tile_t * ctx ) {
  return fd_bam_flush_results( ctx );
}

void
fd_bam_client_send_ping( fd_bam_tile_t * ctx ) {
  if( FD_UNLIKELY( !ctx->grpc_client ) ) return; /* no client */
  fd_h2_conn_t * conn = fd_grpc_client_h2_conn( ctx->grpc_client );
  if( FD_UNLIKELY( !conn ) ) return; /* no conn */
  if( FD_UNLIKELY( conn->flags ) ) return; /* conn busy */
  fd_h2_rbuf_t * rbuf_tx = fd_grpc_client_rbuf_tx( ctx->grpc_client );

  if( FD_LIKELY( fd_h2_tx_ping( conn, rbuf_tx ) ) ) {
    long now = fd_bam_now();
    fd_keepalive_tx( ctx->keepalive, ctx->rng, now );
    FD_LOG_DEBUG(( "Keepalive TX (deadline=+%gs)", (double)( ctx->keepalive->ts_deadline-now )/1e9 ));
  }
}

int
fd_bam_client_step_reconnect( fd_bam_tile_t * ctx,
                              long            now ) {
  if( FD_UNLIKELY( !fd_grpc_client_is_connected( ctx->grpc_client ) ) ) return 0;

  int busy = 0;
  if( FD_UNLIKELY( ctx->builder_info_valid_until &&
                   now >= ctx->builder_info_valid_until ) ) {
    ctx->builder_info_valid_until = 0L;
    ctx->bam_last_config_poll_ns  = 0L;
  }

  /* Request auth challenge before opening the scheduler stream. */
  if( FD_UNLIKELY( !ctx->bam_auth_ready && !ctx->bam_auth_inflight && !ctx->bam_stream ) ) {
    fd_bam_request_auth_challenge( ctx );
    busy = 1;
  }

  /* Start scheduler stream. */
  if( FD_LIKELY( ctx->bam_auth_ready ) ) {
    fd_bam_try_start_stream( ctx );
  }

  /* Poll builder config on a throttle to keep stream settings fresh. */
  long const throttle_ns = ctx->builder_info_valid_until ? (long)5e9 : (long)1e9;
  _Bool const never_polled = ctx->bam_last_config_poll_ns == 0L;
  _Bool const poll_due = never_polled || now - ctx->bam_last_config_poll_ns >= throttle_ns;
  _Bool const refresh_needed = !ctx->builder_info_valid_until ||
                               ( now + (long)5e9 >= ctx->builder_info_valid_until );
  if( FD_UNLIKELY( !ctx->bam_config_inflight && refresh_needed && poll_due ) ) {
    fd_bam_request_config( ctx, now );
    busy = 1;
  }

  /* Heartbeat to keep validator session live. */
  if( FD_LIKELY( ctx->bam_stream && ctx->bam_stream_live ) ) {
    if( FD_UNLIKELY( ctx->bam_last_validator_heartbeat_ns == 0L ||
                     now - ctx->bam_last_validator_heartbeat_ns >= (long)5e9 ) ) {
      fd_bam_send_heartbeat( ctx, now );
      busy = 1;
    }
  }

  /* Push leader state updates once BAM has started the current slot by
     delivering at least one scheduler batch for it. */
  if( FD_UNLIKELY( ctx->bam_leader_pending &&
                   fd_bam_leader_state_send_allowed( ctx, &ctx->bam_leader_state ) ) ) {
    if( FD_LIKELY( fd_bam_send_leader_state( ctx, &ctx->bam_leader_state ) ) ) {
      ctx->bam_leader_pending = 0U;
      busy = 1;
    }
  }

  /* Send a PING */
  if( FD_UNLIKELY( fd_keepalive_should_tx( ctx->keepalive, now ) ) ) {
    fd_bam_client_send_ping( ctx );
    busy = 1;
  }

  /* Flush queued execution results back to the BAM node. */
  if( FD_LIKELY( ctx->bam_stream_live ) ) busy |= fd_bam_flush_results( ctx );

  return busy;
}

int
fd_bam_test_client_step_reconnect( fd_bam_tile_t * ctx,
                   long             now ) {
  return fd_bam_client_step_reconnect( ctx, now );
}

static void
fd_bam_client_step1( fd_bam_tile_t * ctx,
                       int *              charge_busy ) {

  if( FD_UNLIKELY( !FD_VOLATILE_CONST( ctx->enabled ) ) ) {
    /* Admin can pause BAM, skip reconnect until re-enabled. */
    return;
  }

  if( FD_UNLIKELY( ctx->defer_reset ) ) {
    FD_LOG_WARNING(( "BAM client reset requested; retrying %s/" FD_IP4_ADDR_FMT ":%hu in %.3f ms",
      ctx->server_fqdn,
      FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
      ctx->server_tcp_port,
      fd_bam_client_retry_ms( ctx ) ));
    fd_bam_client_reset( ctx );
    *charge_busy = 1;
    return;
  }

  /* Wait for TCP socket to connect */
  if( FD_UNLIKELY( !ctx->tcp_sock_connected ) ) {
    if( FD_UNLIKELY( ctx->tcp_sock < 0 ) ) goto reconnect;

    struct pollfd pfds[1] = {
      { .fd = ctx->tcp_sock, .events = POLLOUT }
    };
    int poll_res = fd_syscall_poll( pfds, 1, 0 );
    if( FD_UNLIKELY( poll_res < 0 ) ) {
      FD_LOG_ERR(( "fd_syscall_poll(tcp_sock) failed (%i-%s)", errno, fd_io_strerror( errno ) ));
    }
    if( poll_res == 0 ) return;

    if( pfds[0].revents & (POLLERR|POLLHUP) ) {
      int connect_err = fd_bam_client_do_connect( ctx, 0U );
      FD_LOG_WARNING(( "BAM gRPC connect attempt failed (%i-%s) while dialing %s/" FD_IP4_ADDR_FMT ":%hu; retrying in %.3f ms",
        connect_err, fd_io_strerror( connect_err ),
        ctx->server_fqdn,
        FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
        ctx->server_tcp_port,
        fd_bam_client_retry_ms( ctx ) ));
      ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_CONNECT_IDX ]++;
      fd_bam_client_reset( ctx );
      *charge_busy = 1;
      return;
    }
    if( pfds[0].revents & POLLOUT ) {
      int connect_err = fd_bam_client_do_connect( ctx, 0U );
      if( connect_err==EINPROGRESS || connect_err==EALREADY ) return;
      if( FD_UNLIKELY( connect_err ) ) {
        FD_LOG_WARNING(( "BAM TCP socket reported writable but connect failed (%i-%s) to %s/" FD_IP4_ADDR_FMT ":%hu; retrying in %.3f ms",
          connect_err, fd_io_strerror( connect_err ),
          ctx->server_fqdn,
          FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
          ctx->server_tcp_port,
          fd_bam_client_retry_ms( ctx ) ));
        fd_bam_client_reset( ctx );
        ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_CONNECT_IDX ]++;
        *charge_busy = 1;
        return;
      }
      FD_LOG_DEBUG(( "BAM TCP socket connected" ));
      ctx->tcp_sock_connected = 1;
      *charge_busy = 1;
      return;
    }
    return;
  }

  /* gRPC conn died? */
  if( FD_UNLIKELY( !ctx->grpc_client ) ) {
    long sleep_start;
  reconnect:
    sleep_start = fd_bam_now();
    if( FD_UNLIKELY( fd_bam_tile_should_stall( ctx, sleep_start ) ) ) {
      long wait_dur = ctx->backoff_until - sleep_start;
      fd_log_sleep( fd_long_min( wait_dur, 1e6 ) );
      return;
    }
    fd_bam_client_create_conn( ctx );
    *charge_busy = 1;
    return;
  }

  /* Did a HTTP/2 PING time out */
  long check_ts = fd_bam_now();
  if( FD_UNLIKELY( fd_keepalive_is_timeout( ctx->keepalive, check_ts ) ) ) {
    FD_LOG_WARNING(( "BAM gRPC timed out (HTTP/2 PING went unanswered for %.2f seconds); retrying %s/" FD_IP4_ADDR_FMT ":%hu in %.3f ms",
                     (double)( check_ts - ctx->keepalive->ts_last_tx )/1e9,
                     ctx->server_fqdn,
                     FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
                     ctx->server_tcp_port,
                     fd_bam_client_retry_ms( ctx ) ));
    ctx->keepalive->inflight = 0;
    fd_bam_client_reset( ctx );
    ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_KEEPALIVE_TIMEOUT_IDX ]++;
    *charge_busy = 1;
    return;
  }

  /* Did scheduler-stream builder activity time out */
  if( FD_UNLIKELY( ctx->bam_stream_live &&
                   ctx->bam_last_builder_activity_ns != 0L &&
                   check_ts - ctx->bam_last_builder_activity_ns >= FD_BAM_ACTIVITY_TIMEOUT_NS ) ) {
    FD_LOG_WARNING(( "BAM builder activity timed out (no scheduler activity for %.2f seconds); retrying %s/" FD_IP4_ADDR_FMT ":%hu in %.3f ms",
      (double)( check_ts - ctx->bam_last_builder_activity_ns )/1e9,
      ctx->server_fqdn,
      FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
      ctx->server_tcp_port,
      fd_bam_client_retry_ms( ctx ) ));
    ctx->keepalive->inflight = 0;
    fd_bam_client_reset( ctx );
    ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_BUILDER_ACTIVITY_TIMEOUT_IDX ]++;
    *charge_busy = 1;
    return;
  }

  /* Drive I/O, SSL handshake, and any inflight requests */
  if( FD_UNLIKELY( !fd_bam_client_drive_io( ctx, charge_busy ) ) ) {
    FD_LOG_WARNING(( "BAM client reset; retrying %s/" FD_IP4_ADDR_FMT ":%hu in %.3f ms",
                     ctx->server_fqdn,
                     FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
                     ctx->server_tcp_port,
                     fd_bam_client_retry_ms( ctx ) ));
    fd_bam_client_reset( ctx );
    ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_IO_IDX ]++;
    *charge_busy = 1;
    return;
  }
  if( FD_UNLIKELY( ctx->defer_reset ) ) {
    FD_LOG_WARNING(( "BAM client reset; retrying %s/" FD_IP4_ADDR_FMT ":%hu in %.3f ms",
                     ctx->server_fqdn,
                     FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
                     ctx->server_tcp_port,
                     fd_bam_client_retry_ms( ctx ) ));
    fd_bam_client_reset( ctx );
    *charge_busy = 1;
    return;
  }

  /* Are we ready to issue a new request? */
  if( FD_UNLIKELY( fd_grpc_client_request_is_blocked( ctx->grpc_client ) ) ) return;
  long io_ts = fd_bam_now();
  if( FD_UNLIKELY( fd_bam_tile_should_stall( ctx, io_ts ) ) ) return;

  *charge_busy |= fd_bam_client_step_reconnect( ctx, io_ts );
}

void
fd_bam_client_step( fd_bam_tile_t * ctx,
                       int *              charge_busy ) {
  /* Edge-trigger logging with rate limiting */
  fd_bam_client_step1( ctx, charge_busy );
  fd_plugin_bam_update_status_t status = fd_bam_client_status( ctx );
  int const healthy_now    = ( status == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
  int const healthy_before = ( ctx->bam_status_counted == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
  if( FD_UNLIKELY( status != ctx->bam_status_counted ) ) {
    long ts_ns = fd_bam_now();
    for( ulong i=0UL; i<FD_BAM_LEADER_SLOT_END_TRACKER_CNT; i++ ) {
      fd_bam_leader_slot_end_tracker_t * tracker = &ctx->leader_slot_end[ i ];
      if( FD_UNLIKELY( !tracker->valid || tracker->counted || ts_ns>tracker->slot_end_ns ) ) continue;
      tracker->status_at_end = status;
    }
  }
  if( FD_UNLIKELY( healthy_now != healthy_before ) ) {
    if( healthy_now ) ctx->metrics.healthy_connects_cnt++;
    else              ctx->metrics.healthy_disconnects_cnt++;
  }
  ctx->bam_status_counted = status;
  if( FD_UNLIKELY( healthy_now != ( ctx->bam_status_logged == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY ) ) ) {
    long ts = fd_log_wallclock();
    if( FD_LIKELY( ts-(ctx->last_bam_status_log_nanos) >= (long)1e6 ) ) {
      if( healthy_now ) {
        char const * scheme = "http";
# if FD_HAS_OPENSSL
        if( ctx->is_ssl ) scheme = "https";
# endif
        FD_LOG_NOTICE(( "Connected to BAM node at %s://%s/ (" FD_IP4_ADDR_FMT ":%hu)",
                        scheme,
                        ctx->server_fqdn,
                        FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
                        ctx->server_tcp_port ));
      } else {
        FD_LOG_WARNING(( "Disconnected from BAM node" ));
      }
      ctx->last_bam_status_log_nanos = ts;
      ctx->bam_status_logged = status;
    }
  }
}

static void
fd_bam_client_grpc_conn_established( void * app_ctx ) {
  (void)app_ctx;
  FD_LOG_INFO(( "BAM gRPC connection established" ));
}

static void
fd_bam_client_grpc_conn_dead( void * app_ctx,
                                 uint   h2_err,
                                 int    closed_by ) {
  fd_bam_tile_t * ctx = app_ctx;
  FD_LOG_INFO(( "BAM gRPC connection closed %s (%u-%s) while connected to %s/" FD_IP4_ADDR_FMT ":%hu",
                closed_by ? "by peer" : "due to error",
                h2_err, fd_h2_strerror( h2_err ),
                ctx->server_fqdn,
                FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
                ctx->server_tcp_port ));
  ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_IO_IDX ]++;
  ctx->defer_reset = 1;
}

static void
fd_bam_client_grpc_tx_complete(
    void * app_ctx,
    ulong  request_ctx
) {
  (void)app_ctx; (void)request_ctx;
}

void
fd_bam_client_grpc_rx_start(
    void * app_ctx,
    ulong  request_ctx
) {
  fd_bam_tile_t * ctx = app_ctx;
  switch( request_ctx ) {
  case FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream: {
    long now = fd_bam_now();
    fd_bam_set_stream_live( ctx, 1U );
    ctx->bam_stream_connecting  = 0;
    ctx->bam_last_validator_heartbeat_ns = now;
    ctx->bam_last_builder_activity_ns    = now;
    break;
  }
  }
}

void
fd_bam_client_grpc_rx_msg(
    void *       app_ctx,
    void const * protobuf,
    ulong        protobuf_sz,
    ulong        request_ctx
) {
  fd_bam_tile_t * ctx = app_ctx;
  long rx_ts_ns = fd_bam_now();
  switch( request_ctx ) {
  case FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge:
    if( FD_UNLIKELY( !fd_bam_handle_auth_challenge( ctx, protobuf, protobuf_sz ) ) ) {
      ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_AUTH_CHALLENGE_DECODE_IDX ]++;
      fd_bam_tile_backoff( ctx, fd_bam_now() );
    }
    break;
  case FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig:
    fd_bam_handle_config( ctx, protobuf, protobuf_sz );
    ctx->bam_config_inflight = 0;
    break;
  case FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream:
    fd_bam_handle_scheduler_response( ctx,
                                      protobuf,
                                      protobuf_sz,
                                      rx_ts_ns,
                                      (uint)fd_frag_meta_ts_comp( fd_tickcount() ) );
    break;
  default:
    FD_LOG_ERR(( "Received unexpected gRPC message (request_ctx=%lu)", request_ctx ));
  }
}

static void
fd_bam_client_request_failed( fd_bam_tile_t * ctx,
                                 ulong              request_ctx ) {
  ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_REQUEST_FAILED_IDX ]++;
  fd_bam_tile_backoff( ctx, fd_bam_now() );
  switch( request_ctx ) {
  case FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge:
    ctx->bam_auth_inflight      = 0;
    ctx->bam_auth_ready         = 0;
    ctx->challenge_to_sign[ 0 ] = '\0';
    break;
  case FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig:
    ctx->bam_config_inflight = 0;
    break;
  case FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream:
    ctx->bam_stream             = NULL;
    fd_bam_set_stream_live( ctx, 0U );
    ctx->bam_stream_connecting  = 0;
    ctx->bam_auth_ready         = 0;
    ctx->challenge_to_sign[ 0 ] = '\0';
    fd_bam_drop_pending_leader_state( ctx, FD_BAM_LEADER_PENDING_DROP_REQUEST_FAILED );
    break;
  }
}

void
fd_bam_client_grpc_rx_end(
    void *                app_ctx,
    ulong                 request_ctx,
    fd_grpc_resp_hdrs_t * resp
) {
  fd_bam_tile_t * ctx = app_ctx;
  if( FD_UNLIKELY( resp->h2_status != 200 ) ) {
    FD_LOG_WARNING(( "gRPC request failed (HTTP status %u)", resp->h2_status ));
    fd_bam_client_request_failed( ctx, request_ctx );
    return;
  }

  resp->grpc_msg_len = (uint)fd_url_unescape( resp->grpc_msg, resp->grpc_msg_len );
  if( !resp->grpc_msg_len ) {
    fd_memcpy( resp->grpc_msg, "unknown error", 13 );
    resp->grpc_msg_len = 13;
  }

  switch( request_ctx ) {
  case FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge:
    ctx->bam_auth_inflight = 0;
    break;
  case FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig:
    ctx->bam_config_inflight = 0;
    break;
  case FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream:
    ctx->bam_stream            = NULL;
    fd_bam_set_stream_live( ctx, 0U );
    ctx->bam_stream_connecting = 0;
    fd_bam_drop_pending_leader_state( ctx, FD_BAM_LEADER_PENDING_DROP_STREAM_ENDED );
    break;
  default:
    break;
  }

  if( FD_UNLIKELY( resp->grpc_status != FD_GRPC_STATUS_OK ) ) {
    FD_LOG_WARNING(( "gRPC request %s failed (gRPC status %u-%s): %.*s",
                     fd_bam_request_ctx_cstr( request_ctx ),
                     resp->grpc_status, fd_grpc_status_cstr( resp->grpc_status ),
                     (int)resp->grpc_msg_len, resp->grpc_msg ));
    fd_bam_client_request_failed( ctx, request_ctx );
    if( resp->grpc_status == FD_GRPC_STATUS_UNAUTHENTICATED ||
        resp->grpc_status == FD_GRPC_STATUS_PERMISSION_DENIED ) {
      ctx->bam_auth_ready         = 0;
      ctx->challenge_to_sign[ 0 ] = '\0';
    }
    return;
  }
}

void
fd_bam_client_grpc_rx_timeout(
    void * app_ctx,
    ulong  request_ctx,  /* FD_BAM_CLIENT_REQ_{...} */
    int    deadline_kind /* FD_GRPC_DEADLINE_{HEADER|RX_END} */
) {
  (void)deadline_kind;
  FD_LOG_WARNING(( "Request timed out: %s", fd_bam_request_ctx_cstr( request_ctx ) ));
  fd_bam_tile_t * ctx = app_ctx;
  ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_REQUEST_TIMEOUT_IDX ]++;
  ctx->defer_reset = 1;
  switch( request_ctx ) {
  case FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge:
    ctx->bam_auth_inflight      = 0;
    ctx->bam_auth_ready         = 0;
    ctx->challenge_to_sign[ 0 ] = '\0';
    break;
  case FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig:
    ctx->bam_config_inflight = 0;
    break;
  case FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream:
    ctx->bam_stream            = NULL;
    fd_bam_set_stream_live( ctx, 0U );
    ctx->bam_stream_connecting = 0;
    fd_bam_drop_pending_leader_state( ctx, FD_BAM_LEADER_PENDING_DROP_STREAM_TIMEOUT );
    break;
  default:
    break;
  }
}

static void
fd_bam_client_grpc_ping_ack( void * app_ctx ) {
  fd_bam_tile_t * ctx = app_ctx;
  long rtt_sample = fd_keepalive_rx( ctx->keepalive, fd_bam_now() );
  if( FD_LIKELY( rtt_sample ) ) {
    fd_rtt_sample( ctx->rtt, (float)rtt_sample, 0 );
    FD_LOG_DEBUG(( "Keepalive ACK" ));
  }
  ctx->metrics.keepalive_acks_cnt++;
}

fd_grpc_client_callbacks_t fd_bam_client_grpc_callbacks = {
  .conn_established = fd_bam_client_grpc_conn_established,
  .conn_dead        = fd_bam_client_grpc_conn_dead,
  .tx_complete      = fd_bam_client_grpc_tx_complete,
  .rx_start         = fd_bam_client_grpc_rx_start,
  .rx_msg           = fd_bam_client_grpc_rx_msg,
  .rx_end           = fd_bam_client_grpc_rx_end,
  .rx_timeout       = fd_bam_client_grpc_rx_timeout,
  .ping_ack         = fd_bam_client_grpc_ping_ack,
};

fd_plugin_bam_update_status_t
fd_bam_client_status( fd_bam_tile_t const * ctx ) {
  if( FD_UNLIKELY( !FD_VOLATILE_CONST( ctx->enabled ) ) )
    return FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISABLED;

  /* Treat the connection as "owned" only when every layer (TCP socket,
     HTTP/2 session, bundle auth, scheduler stream, and keepalive) is
     healthy.  Downstream tiles key off this switch to stop ingesting
     QUIC/bundle traffic, so any premature CONNECTED state would cause a
     data gap. */
  if( FD_UNLIKELY( ( !ctx->tcp_sock_connected ) |
                   ( !ctx->grpc_client        ) ) ) {
    return FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED;
  }

  fd_h2_conn_t * conn = fd_grpc_client_h2_conn( ctx->grpc_client );
  if( FD_UNLIKELY( !conn ) ) {
    return FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED; /* no conn */
  }
  if( FD_UNLIKELY( conn->flags &
      ( FD_H2_CONN_FLAGS_DEAD |
        FD_H2_CONN_FLAGS_SEND_GOAWAY ) ) ) {
    return FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED;
  }

  if( FD_UNLIKELY( conn->flags &
      ( FD_H2_CONN_FLAGS_CLIENT_INITIAL      |
        FD_H2_CONN_FLAGS_WAIT_SETTINGS_ACK_0 |
        FD_H2_CONN_FLAGS_WAIT_SETTINGS_0     |
        FD_H2_CONN_FLAGS_SERVER_INITIAL ) ) ) {
    return FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTING; /* connection is not ready */
  }

  if( FD_UNLIKELY( !ctx->bam_stream_live ) ) {
    return FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTING; // TODO: check if correct, differs from bundle client
  }

  long now = fd_bam_now();

  if( FD_UNLIKELY( fd_keepalive_is_timeout( ctx->keepalive, now ) ) ) {
    return FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED; /* possible timeout */
  }

  if( FD_UNLIKELY( !fd_grpc_client_is_connected( ctx->grpc_client ) ) ) {
    return FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTING;
  }

  if( FD_UNLIKELY( !ctx->bam_config_received ) ) {
    return FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY;
  }

  if( FD_UNLIKELY(
    ( ctx->bam_last_builder_activity_ns<=0L ) ||
    ( now - ctx->bam_last_builder_activity_ns >= FD_BAM_ACTIVITY_TIMEOUT_NS ) ) ) {
    return FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY;
  }

  return FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY;
}

FD_FN_CONST char const *
fd_bam_request_ctx_cstr( ulong request_ctx ) {
  switch( request_ctx ) {
  case FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge:
    return "BamGetAuthChallenge";
  case FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig:
    return "BamGetBuilderConfig";
  case FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream:
    return "BamInitSchedulerStream";
  default:
    return "unknown";
  }
}
