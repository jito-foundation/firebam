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
#include "../../waltz/h2/fd_h2_conn.h"
#include "../../waltz/http/fd_url.h" /* fd_url_unescape */
#include "../../ballet/base58/fd_base58.h"
#include "../../ballet/nanopb/pb_decode.h"
#include "../../ballet/nanopb/pb_encode.h"
#include "../../util/net/fd_ip4.h"

#include <fcntl.h>
#include <errno.h>
#include <unistd.h> /* close */
#include <poll.h> /* poll */
#include <netinet/in.h>
#include <netinet/tcp.h>

typedef struct {
  fd_bam_tile_t * ctx;                                         /* owning tile context; non-NULL while batch is processed */
  bam_types_Packet    packets[ FD_PACK_MAX_TXN_PER_BUNDLE ];   /* decoded packet cache; indices [0,packet_cnt) valid */
  uchar               packet_cnt;                              /* number of packets collected; [0,FD_PACK_MAX_TXN_PER_BUNDLE) */
  uchar               revert_on_error;                         /* 0/1 flag mirrored from packet meta; only meaningful when revert_flag_set != 0 */
  uchar               revert_flag_set;                         /* 0 before first flag observed, 1 after; prevents defaulting to revert_on_error=0 */
  uchar               has_deser_err;                           /* 0/1 value if we have batch-level not-committed reason */
  uchar               deser_index;                             /* zero-based transaction index tied to deserialization error */
  uchar               deser_reason;                            /* bam_types_DeserializationErrorReason enum value */
} fd_bam_batch_ctx_t;

typedef struct {
  fd_bam_bundle_result_t const * res;
} fd_bam_encode_ctx_t;

typedef struct {
  bam_types_AtomicTxnBatchResult const * atomic_res;
} fd_bam_encode_batch_ctx_t;

static int
fd_bam_drive( fd_bam_tile_t * ctx,
              long             now );

static void
fd_bam_tile_publish_bundle_txn( fd_bam_tile_t * ctx,
                                void const *    txn,
                                ushort          txn_sz,
                                uchar           bundle_txn_cnt,
                                uchar           batch_idx,
                                uint            source_ipv4 );

static void
fd_bam_tile_publish_txn( fd_bam_tile_t * ctx,
                         void const *    txn,
                         ulong           txn_sz,
                         ulong           max_schedule_slot,
                         uint            scheduler_seq_id,
                         uchar           batch_idx,
                         uchar           batch_cnt,
                         uchar           revert_on_error,
                         uint            source_ipv4 );

static void
fd_bam_client_sample_heartbeat_delay( fd_bam_tile_t * ctx,
                                      uint64_t        time_sent_microseconds );

__attribute__((weak)) long
fd_bam_now( void ) {
  return fd_log_wallclock();
}

void
fd_bam_client_reset( fd_bam_tile_t * ctx ) {
  long now = fd_bam_now();
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
  ctx->bundle_max_schedule_slot = FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT;

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
  ctx->bam_stream_live            = 0;
  ctx->bam_stream_connecting      = 0;
  ctx->bam_auth_ready             = 0;
  ctx->bam_auth_inflight          = 0;
  ctx->bam_config_inflight        = 0;
  ctx->bam_auth_challenge_len     = 0U;
  ctx->bam_last_builder_heartbeat_ns = 0L;
  ctx->bam_last_validator_heartbeat_ns = 0L;
  ctx->bam_last_config_poll_ns    = 0L;
  /* Preserve any buffered bundle results so they flush once the next
     scheduler stream comes up.  The server expects every dispatched
     bundle to eventually produce a result; dropping them here would lose
     that guarantee. */
  /* ctx->bam_pending_results        = 0UL; */
  /* ctx->bam_results_head           = 0UL; */
  /* ctx->bam_results_tail           = 0UL; */
  ctx->bam_leader_pending         = 0U;
}

static double
fd_bam_client_retry_ms( fd_bam_tile_t * ctx ) {
  long wait_ns = ctx->backoff_until - fd_bam_now();
  if( wait_ns < 0L ) wait_ns = 0L;
  return (double)wait_ns / 1e6;
}

static int
fd_bam_client_do_connect( fd_bam_tile_t const * ctx,
                             uint                     ip4_addr ) {
  if( FD_UNLIKELY( ctx->tcp_sock < 0 ) ) return EBADF;

  if( FD_UNLIKELY( ip4_addr == 0U ) ) {
    int so_err = 0;
    socklen_t so_err_sz = sizeof(so_err);
    if( FD_UNLIKELY( getsockopt( ctx->tcp_sock, SOL_SOCKET, SO_ERROR, &so_err, &so_err_sz ) ) ) {
      return errno;
    }
    return so_err;
  }

  struct sockaddr_in addr = {
    .sin_family      = AF_INET,
    .sin_addr.s_addr = ip4_addr,
    .sin_port        = fd_ushort_bswap( ctx->server_tcp_port )
  };
  errno = 0;
  connect( ctx->tcp_sock, fd_type_pun_const( &addr ), sizeof(struct sockaddr_in) );
  return errno;
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
    ctx->metrics.transport_fail_cnt++;
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
  if( FD_UNLIKELY( connect_err ) ) {
    if( FD_UNLIKELY( connect_err != EINPROGRESS ) ) {
      fd_bam_client_reset( ctx );
      ctx->metrics.transport_fail_cnt++;
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

static bool
fd_bam_encode_committed_cb( pb_ostream_t *          stream,
                            pb_field_t const *       field,
                            void * const *           arg ) {
  fd_bam_encode_ctx_t const * ctx = (fd_bam_encode_ctx_t const *)*arg;
  if( FD_UNLIKELY( !ctx || !ctx->res ) ) return false;
  fd_bam_bundle_result_t const * res = ctx->res;
  for( uchar i=0U; i<res->txn_cnt; i++ ) {
    bam_types_TransactionCommittedResult txn_res = bam_types_TransactionCommittedResult_init_default;
    txn_res.cus_consumed               = res->consumed_cus[ i ];
    txn_res.feepayer_balance_lamports  = 0UL;
    txn_res.loaded_accounts_data_size  = 0U;
    txn_res.execution_success          = ( res->sanitize_success[ i ] && res->transaction_err[ i ] == 0U );
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
fd_bam_fill_not_committed( bam_types_NotCommitted *           out,
                           fd_bam_bundle_result_t const *     res ) {
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
      return;
    }
    out->which_reason                        = bam_types_NotCommitted_deserialization_error_tag;
    out->reason.deserialization_error.index  = res->deser_index;
    out->reason.deserialization_error.reason = (bam_types_DeserializationErrorReason)res->deser_reason;
    return;
  case FD_BAM_BUNDLE_ERR_GENERIC_INVALID:
    out->which_reason = bam_types_NotCommitted_generic_invalid_tag;
    if( FD_UNLIKELY( res->generic_invalid_reason >= FD_BAM_ERR_GENERIC_INVALID_CNT ) ) {
      FD_LOG_ERR(( "Invalid FD_BAM_ERR_GENERIC_INVALID %u, value out of range.", res->generic_invalid_reason ));
    }
    strlcpy( out->reason.generic_invalid.message,
             FD_BAM_ERR_GENERIC_INVALID_STRINGS[ res->generic_invalid_reason ],
             sizeof(out->reason.generic_invalid.message) );
    return;
  default:
    FD_LOG_ERR(( "Invalid error type %u, value out of range.", res->bundle_err));
  }

  if( FD_UNLIKELY( res->scheduling_error != FD_BAM_SCHED_ERR_NONE ) ) {
    if( FD_LIKELY( res->scheduling_error <= _bam_types_SchedulingError_MAX ) ) {
      out->which_reason                 = bam_types_NotCommitted_scheduling_error_tag;
      out->reason.scheduling_error      = (bam_types_SchedulingError)res->scheduling_error;
    } else {
      out->which_reason                 = bam_types_NotCommitted_generic_invalid_tag;
      snprintf( out->reason.generic_invalid.message,
                sizeof(out->reason.generic_invalid.message),
                FD_BAM_ERR_FMT_INVALID_SCHEDULING_ERROR,
                res->scheduling_error );
    }
    return;
  }

    for( uchar i=0U; i<res->txn_cnt; i++ ) {
    if( FD_UNLIKELY( !res->sanitize_success[ i ] ) ) {
      out->which_reason                             = bam_types_NotCommitted_deserialization_error_tag;
      out->reason.deserialization_error.index       = i;
      out->reason.deserialization_error.reason      = bam_types_DeserializationErrorReason_SANITIZE_ERROR;
      return;
    }
  }

    for( uchar i=0U; i<res->txn_cnt; i++ ) {
    int err = res->transaction_err[ i ];
    if( FD_UNLIKELY( err ) ) {
      if( FD_LIKELY( err < (int)_bam_types_TransactionErrorReason_ARRAYSIZE ) ) {
        out->which_reason                      = bam_types_NotCommitted_transaction_error_tag;
        out->reason.transaction_error.index    = i;
        out->reason.transaction_error.reason   = (bam_types_TransactionErrorReason)err;
      } else {
        out->which_reason                      = bam_types_NotCommitted_generic_invalid_tag;
        snprintf( out->reason.generic_invalid.message,
                  sizeof(out->reason.generic_invalid.message),
                  FD_BAM_ERR_FMT_TRANSACTION_ERROR,
                  err );
      }
      return;
    }
  }

  out->which_reason = bam_types_NotCommitted_generic_invalid_tag;
  strlcpy( out->reason.generic_invalid.message,
           FD_BAM_ERR_MSG_BUNDLE_EXECUTION_FAILED,
           sizeof(out->reason.generic_invalid.message) );
  out->reason.generic_invalid.message[ sizeof(out->reason.generic_invalid.message)-1UL ] = '\0';
}

/* Collects a single Packet from the protobuf stream. Returns true while the
   packet parsed and passed basic validation; returns false to abort the
   surrounding AtomicTxnBatch decode and surface the bundle error. */
static bool
fd_bam_collect_packet( pb_istream_t *         stream,
                       pb_field_t const *     field,
                       void **                arg ) {
  (void)field;
  fd_bam_batch_ctx_t * state = *arg;
  if( FD_UNLIKELY( state->packet_cnt > FD_PACK_MAX_TXN_PER_BUNDLE ) ) {
    FD_LOG_WARNING(( "Received AtomicTxnBatch exceeding max bundle size" ));
    state->has_deser_err   = true;
    state->deser_reason    = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
    state->deser_index     = state->packet_cnt;
    return false;
  }

  bam_types_Packet packet = bam_types_Packet_init_default;
  if( FD_UNLIKELY( !pb_decode( stream, &bam_types_Packet_msg, &packet ) ) ) {
    state->has_deser_err   = true;
    state->deser_reason    = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
    state->deser_index     = state->packet_cnt;
    return false;
  }

  if( packet.has_meta && packet.meta.has_flags ) {
    if( FD_UNLIKELY( packet.meta.flags.simple_vote_tx ) ) {
      state->has_deser_err = true;
      state->deser_reason = bam_types_DeserializationErrorReason_VOTE_TRANSACTION_FAILURE;
      state->deser_index  = state->packet_cnt;
      return false;
    }

    if( state->revert_flag_set && state->revert_on_error != packet.meta.flags.revert_on_error ) {
      FD_LOG_WARNING(( "AtomicTxnBatch contains mixed revert_on_error flags" ));
    state->has_deser_err = true;
      state->deser_reason = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
      state->deser_index  = state->packet_cnt;
      state->revert_on_error = state->revert_on_error | packet.meta.flags.revert_on_error;
      state->revert_flag_set = 1;
      return false;
    }
    state->revert_on_error = packet.meta.flags.revert_on_error;
    state->revert_flag_set = 1;
  }

  ulong declared_sz = fd_ulong_max( packet.data.size, packet.meta.size );
  if( FD_UNLIKELY( declared_sz > FD_TXN_MTU ) ) {
    state->ctx->metrics.packet_drop_cnt++;
    FD_MCNT_INC( BAM, PACKETS_DROPPED, 1UL );
    FD_LOG_WARNING(( "Received AtomicTxnBatch packet exceeding MTU (%lu>%lu); dropping batch",
                     declared_sz, FD_TXN_MTU ));
    state->has_deser_err = true;
    state->deser_reason = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
    state->deser_index  = state->packet_cnt;
    return false;
  }

  state->packets[ state->packet_cnt ] = packet;
  state->packet_cnt++;
  return true;
}

static void
fd_bam_publish_batch( fd_bam_tile_t *            ctx,
                      fd_bam_batch_ctx_t *       state,
                      bam_types_AtomicTxnBatch const * batch ) {
  if ( FD_UNLIKELY( state->has_deser_err ) ) {
    fd_bam_bundle_result_t res = {
      .seq_id            = batch->seq_id,
      .slot              = batch->max_schedule_slot,
      .bundle_txn_cnt    = state->packet_cnt,
      .txn_cnt           = state->packet_cnt,
      .execution_success = 0,
      .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
      .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
      .deser_reason      = state->deser_reason,
      .deser_index       = state->deser_index
    };
    fd_bam_enqueue_result( ctx, &res );
    ctx->bundle_max_schedule_slot = FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT;
    return;
  }

  if( FD_UNLIKELY( state->packet_cnt == 0U ) ) {
    fd_bam_bundle_result_t res = {
      .seq_id            = batch->seq_id,
      .slot              = batch->max_schedule_slot,
      .bundle_txn_cnt    = state->packet_cnt,
      .txn_cnt           = state->packet_cnt,
      .execution_success = 0,
      .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
      .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
      .deser_reason      = bam_types_DeserializationErrorReason_EMPTY,
      .deser_index       = 0
    };
    fd_bam_enqueue_result( ctx, &res );
    ctx->bundle_max_schedule_slot = FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT;
    return;
  }

  if( state->revert_on_error ) {
    if( FD_UNLIKELY( !ctx->builder_info_valid_until ) ) {
      fd_bam_bundle_result_t res = {
        .seq_id                 = batch->seq_id,
        .slot                   = batch->max_schedule_slot,
        .bundle_txn_cnt         = state->packet_cnt,
        .txn_cnt                = state->packet_cnt,
        .execution_success      = 0,
        .scheduling_error       = FD_BAM_SCHED_ERR_NONE,
        .bundle_err             = FD_BAM_BUNDLE_ERR_GENERIC_INVALID,
        .generic_invalid_reason = FD_BAM_ERR_GENERIC_INVALID_BUILDER_INFO_UNAVAILABLE,
        .generic_invalid_index  = 0,
      };
      fd_bam_enqueue_result( ctx, &res );
      ctx->metrics.missing_builder_info_fail_cnt++;
      return;
    }
    ctx->bundle_seq                = batch->seq_id;
    ctx->bundle_txn_cnt            = state->packet_cnt;
    ctx->bundle_max_schedule_slot  = batch->max_schedule_slot;

    for( uchar i=0; i<state->packet_cnt; i++ ) {
      fd_bam_tile_publish_bundle_txn( ctx,
                                      state->packets[i].data.bytes,
                                      (ushort)state->packets[i].data.size,
                                      state->packet_cnt,
                                      i,
                                      0 );
    }
    ctx->metrics.bundle_received_cnt++;
  } else {
    for( uchar i=0; i<state->packet_cnt; i++ ) {
      fd_bam_tile_publish_txn( ctx,
                               state->packets[i].data.bytes,
                               state->packets[i].data.size,
                               batch->max_schedule_slot,
                               batch->seq_id,
                               0, // treat as individual transactions
                               1,
                               0,
                               0 );
    }
  }
  ctx->bundle_max_schedule_slot = FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT;
}

/* Decodes one bam_types.AtomicTxnBatch message. Returns 1 when the batch was
   fully consumed (success path publishes to tiles); returns 0 when protobuf
   decoding failed after logging/enqueuing the appropriate bundle error. */
static _Bool
fd_bam_decode_batch( fd_bam_tile_t * ctx,
                     pb_istream_t *  stream ) {
  fd_bam_batch_ctx_t state = { .ctx = ctx };
  bam_types_AtomicTxnBatch batch = bam_types_AtomicTxnBatch_init_default;
  batch.packets = (pb_callback_t){
    .funcs.decode = fd_bam_collect_packet,
    .arg          = &state
  };

  if( FD_UNLIKELY( !pb_decode( stream, &bam_types_AtomicTxnBatch_msg, &batch ) ) ) {
    ctx->metrics.decode_fail_cnt++;
    FD_MCNT_INC( BAM, ERRORS_PROTOBUF, 1UL );
    char const * err = PB_GET_ERROR( stream );
    FD_LOG_WARNING(( "Protobuf decode of (bam_types.AtomicTxnBatch) failed (%s)", err ));
    fd_bam_bundle_result_t res = {
      .seq_id            = batch.seq_id,
      .slot              = batch.max_schedule_slot,
      .bundle_txn_cnt    = state.packet_cnt,
      .txn_cnt           = state.packet_cnt,
      .execution_success = 0,
      .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
      .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
      .deser_reason      = state.deser_reason,
      .deser_index       = state.deser_index
    };
    fd_bam_enqueue_result( ctx, &res );
    return 0;
  }

  fd_bam_publish_batch( ctx, &state, &batch );
  return 1;
}

/* Decodes a bam_types.MultipleAtomicTxnBatch wrapper. Returns 1 on successful
   parsing of the outer message (including the empty-batch case where an error
   result is enqueued); returns 0 on protobuf failures while walking the tag
   stream. */
static int
fd_bam_decode_multiple_atomic_txn_batch( fd_bam_tile_t * ctx,
                                         pb_istream_t *   stream ) {
  uint32_t      tag;
  pb_wire_type_t wire_type;
  bool          eof = false;
  int           seen_batch_count = 0;
  while( pb_decode_tag( stream, &wire_type, &tag, &eof ) ) {
    if( FD_UNLIKELY( tag != bam_types_MultipleAtomicTxnBatch_batches_tag ) ) {
      if( FD_UNLIKELY( !pb_skip_field( stream, wire_type ) ) ) return 0;
      continue;
    }
    if( FD_UNLIKELY( wire_type != PB_WT_STRING ) ) {
      if( FD_UNLIKELY( !pb_skip_field( stream, wire_type ) ) ) return 0;
      continue;
    }
    pb_istream_t substream;
    if( FD_UNLIKELY( !pb_make_string_substream( stream, &substream ) ) ) return 0;
    seen_batch_count++;
    const _Bool ok = fd_bam_decode_batch( ctx, &substream );
    pb_close_string_substream( stream, &substream );
    if( FD_UNLIKELY( !ok ) ) return 0;
  }

  if( FD_UNLIKELY( !eof ) ) return 0;
  if( FD_UNLIKELY( seen_batch_count == 0 ) ) {
    FD_LOG_WARNING(( "MultipleAtomicTxnBatch contained no AtomicTxnBatch entries" ));
    bam_types_AtomicTxnBatch batch = bam_types_AtomicTxnBatch_init_default;
    fd_bam_bundle_result_t res = {
      .seq_id            = batch.seq_id,
      .slot              = batch.max_schedule_slot,
      .bundle_txn_cnt    = 0,
      .txn_cnt           = 0,
      .execution_success = 0,
      .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
      .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
      .deser_reason      = bam_types_DeserializationErrorReason_EMPTY,
      .deser_index       = 0
    };
    fd_bam_enqueue_result( ctx, &res );
  }
  return 1;
}

/* Decodes the v0 scheduler response payload (heartbeats and nested batches).
   Returns 1 once the message was consumed; returns 0 on protobuf failures so
   the caller can reset/tear down the stream. */
static int
fd_bam_decode_scheduler_response_v0( fd_bam_tile_t * ctx,
                                     pb_istream_t *   stream ) {
  uint32_t      tag;
  pb_wire_type_t wire_type;
  bool          eof = false;
  while( pb_decode_tag( stream, &wire_type, &tag, &eof ) ) {
    switch( tag ) {
    case bam_api_SchedulerResponseV0_heart_beat_tag: {
      if( FD_UNLIKELY( wire_type != PB_WT_STRING ) ) {
        if( FD_UNLIKELY( !pb_skip_field( stream, wire_type ) ) ) return 0;
        break;
      }
      pb_istream_t hb_stream;
      if( FD_UNLIKELY( !pb_make_string_substream( stream, &hb_stream ) ) ) return 0;
      bam_types_BuilderHeartBeat hb = bam_types_BuilderHeartBeat_init_default;
      int ok = pb_decode( &hb_stream, &bam_types_BuilderHeartBeat_msg, &hb );
      pb_close_string_substream( stream, &hb_stream );
      if( FD_UNLIKELY( !ok ) ) {
        FD_LOG_WARNING(( "heartbeat decode failed: %s", PB_GET_ERROR( &hb_stream ) ));
        return 0;
      }
      ctx->bam_last_builder_heartbeat_ns = fd_bam_now();
      fd_bam_client_sample_heartbeat_delay( ctx, hb.time_sent_microseconds );
      ctx->metrics.heartbeat_recv_cnt++;
      break;
    }
    case bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag: {
      if( FD_UNLIKELY( wire_type != PB_WT_STRING ) ) {
        if( FD_UNLIKELY( !pb_skip_field( stream, wire_type ) ) ) return 0;
        break;
      }
      pb_istream_t substream;
      if( FD_UNLIKELY( !pb_make_string_substream( stream, &substream ) ) ) return 0;
      int ok = fd_bam_decode_multiple_atomic_txn_batch( ctx, &substream );
      pb_close_string_substream( stream, &substream );
      if( FD_UNLIKELY( !ok ) ) return 0;
      ctx->bam_last_builder_heartbeat_ns = fd_bam_now();
      break;
    }
    default:
      if( FD_UNLIKELY( !pb_skip_field( stream, wire_type ) ) ) return 0;
      break;
    }
  }
  if( FD_UNLIKELY( !eof ) ) return 0;
  return 1;
}

static void
fd_bam_client_sample_heartbeat_delay( fd_bam_tile_t * ctx,
                                      uint64_t        time_sent_microseconds ) {
  if( FD_UNLIKELY( !time_sent_microseconds ) ) return;
  ulong tsorig_ns = time_sent_microseconds * 1000UL;
  long  now_ns    = fd_bam_now();
  ulong now_u     = fd_ulong_if( now_ns >= 0L, (ulong)now_ns, 0UL );
  fd_histf_sample( ctx->metrics.msg_rx_delay, fd_ulong_sat_sub( now_u, tsorig_ns ) );
}

static void
fd_bam_request_auth_challenge( fd_bam_tile_t * ctx ) {
  if( FD_UNLIKELY( ctx->bam_auth_inflight ) ) return;
  if( FD_UNLIKELY( !ctx->grpc_client ) ) return;
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
    FD_LOG_WARNING(( "Protobuf decode of (bam_api.AuthChallengeResponse) failed" ));
    return 0;
  }

  size_t challenge_len = strnlen( resp.challenge_to_sign, sizeof(resp.challenge_to_sign) );
  if( FD_UNLIKELY( challenge_len == sizeof(resp.challenge_to_sign) ) ) {
    ctx->bam_auth_inflight = 0;
    FD_LOG_WARNING(( "AuthChallengeResponse challenge not NUL terminated" ));
    return 0;
  }

  ctx->bam_auth_inflight      = 0;
  ctx->bam_auth_challenge_len = (uchar)challenge_len;
  fd_memcpy( ctx->challenge_to_sign, resp.challenge_to_sign, sizeof(ctx->challenge_to_sign) );

  uchar  sign_payload[ FD_BAM_AUTH_LABEL_LEN + sizeof(bam_api_AuthChallengeResponse) ]; // the null is to be included
  fd_memcpy( sign_payload, FD_BAM_AUTH_LABEL, FD_BAM_AUTH_LABEL_LEN );
  fd_memcpy( sign_payload + FD_BAM_AUTH_LABEL_LEN, ctx->challenge_to_sign, challenge_len );

  uchar signature[ 64 ];
  fd_keyguard_client_sign( ctx->keyguard_client, signature, sign_payload, FD_BAM_AUTH_LABEL_LEN + challenge_len, FD_KEYGUARD_SIGN_TYPE_ED25519 );

  fd_base58_encode_64( signature, NULL, ctx->bam_auth_signature );
  ctx->bam_auth_ready = 1;
  return 1;
}

static void
fd_bam_request_config( fd_bam_tile_t * ctx,
                        long               now ) {
  if( FD_UNLIKELY( ctx->bam_config_inflight ) ) return;
  if( FD_UNLIKELY( !ctx->grpc_client ) ) return;
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

/* Decodes bam_api.ConfigResponse. On protobuf failure it increments decode
   metrics and returns early; otherwise it updates cached builder/BAM config in
   place. */
static void
fd_bam_handle_config( fd_bam_tile_t * ctx,
                       pb_istream_t *     istream ) {
  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  if( FD_UNLIKELY( !pb_decode( istream, &bam_api_ConfigResponse_msg, &resp ) ) ) {
    ctx->metrics.decode_fail_cnt++;
    FD_LOG_WARNING(( "Protobuf decode of (bam_api.ConfigResponse) failed" ));
    return;
  }

  if( FD_LIKELY( resp.has_block_engine_config ) ) {
    bam_types_BlockEngineBuilderConfig const * cfg = &resp.block_engine_config;
    if( FD_UNLIKELY( cfg->builder_commission > 100UL ) ) {
      FD_LOG_WARNING(( "BlockEngine builder commission out of range (0-100): %u", cfg->builder_commission ));
    } else {
      ctx->builder_commission = (uchar)cfg->builder_commission;
    }

    if( FD_UNLIKELY( !fd_base58_decode_32( cfg->builder_pubkey, ctx->builder_pubkey ) ) ) {
      FD_LOG_HEXDUMP_WARNING(( "Invalid builder pubkey in ConfigResponse",
                               cfg->builder_pubkey,
                               strnlen( cfg->builder_pubkey, sizeof(cfg->builder_pubkey) ) ));
    } else {
      ctx->builder_info_valid_until  = fd_bam_now() + (long)( 60e9 * 5. );
    }
  }

  if( FD_UNLIKELY( !resp.has_bam_config ) ) {
    FD_LOG_WARNING(( "Missing BAM config in ConfigResponse" ));
    return;
  }

  bam_types_BamConfig const * cfg = &resp.bam_config;
  fd_ip4_port_t new_tpu_addr = {0};
  fd_ip4_port_t new_tpu_fwd_addr = {0};

  if( cfg->has_tpu_sock ) {
    uint ip4;
    if( FD_LIKELY( fd_cstr_to_ip4_addr( cfg->tpu_sock.ip, &ip4 ) ) &&
        FD_LIKELY( cfg->tpu_sock.port > 0 && cfg->tpu_sock.port <= USHORT_MAX ) ) {
      new_tpu_addr.addr = ip4;
      new_tpu_addr.port = fd_ushort_bswap( (ushort)cfg->tpu_sock.port );
    } else {
      FD_LOG_WARNING(( "Invalid BAM TPU socket in ConfigResponse (ip=%s port=%u)",
                       cfg->tpu_sock.ip,
                       cfg->tpu_sock.port ));
    }
  }

  if( cfg->has_tpu_fwd_sock ) {
    uint ip4;
    if( FD_LIKELY( fd_cstr_to_ip4_addr( cfg->tpu_fwd_sock.ip, &ip4 ) ) &&
        FD_LIKELY( cfg->tpu_fwd_sock.port > 0 && cfg->tpu_fwd_sock.port <= USHORT_MAX ) ) {
      new_tpu_fwd_addr.addr = ip4;
      new_tpu_fwd_addr.port = fd_ushort_bswap( (ushort)cfg->tpu_fwd_sock.port );
    } else {
      FD_LOG_WARNING(( "Invalid BAM TPU forward socket in ConfigResponse (ip=%s port=%u)",
                       cfg->tpu_fwd_sock.ip,
                       cfg->tpu_fwd_sock.port ));
    }
  }

  _Bool contact_changed = ( new_tpu_addr.l != ctx->bam_tpu_addr.l ) ||
      ( new_tpu_addr.port != ctx->bam_tpu_addr.port ) ||
      ( new_tpu_fwd_addr.l != ctx->bam_tpu_fwd_addr.l ) ||
      ( new_tpu_fwd_addr.port != ctx->bam_tpu_fwd_addr.port );

  if( FD_UNLIKELY( contact_changed ) ) {
    /* A disconnect means Firedancer should resume advertising its local
       TPU ports so TPU clients do not get stuck targeting the BAM host. */
    _Bool has_contact = !!new_tpu_addr.l && new_tpu_addr.port > 0 && !!new_tpu_fwd_addr.l && new_tpu_fwd_addr.port > 0;
    if ( FD_UNLIKELY( !has_contact ) ) {
      FD_LOG_WARNING(( "Reverting BAM TPU config, due to BAM reset" ));
    }
    // TODO: verify if we successfully connected to BAM at this point before gossiping out to the solana cluster
    ctx->bam_tpu_addr     = new_tpu_addr;
    ctx->bam_tpu_fwd_addr = new_tpu_fwd_addr;

    ctx->gui_dirty = 1U;
    fd_bam_publish_gossip_update( ctx, ctx->stem, has_contact );
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
    fd_bam_fee_cfg_t * fee_cfg = ctx->fee_cfg;
    fd_memcpy( fee_cfg->prio_fee_recipient, ctx->prio_fee_recipient, sizeof( fee_cfg->prio_fee_recipient ) );
    fee_cfg->commission_bps         = ctx->commission_bps;
    fee_cfg->has_prio_fee_recipient = ctx->prio_fee_recipient_set;
    FD_COMPILER_MFENCE();
    fee_cfg->version = ctx->fee_cfg_version;
    FD_COMPILER_MFENCE();
  }
}

static void
fd_bam_try_start_stream( fd_bam_tile_t * ctx ) {
  if( FD_UNLIKELY( !ctx->bam_auth_ready ) ) return;
  if( FD_UNLIKELY( ctx->bam_stream || ctx->bam_stream_connecting ) ) return;
  if( FD_UNLIKELY( !ctx->builder_info_valid_until ) ) return;
  if( FD_UNLIKELY( fd_grpc_client_request_is_blocked( ctx->grpc_client ) ) ) return;

  bam_types_AuthProof proof = bam_types_AuthProof_init_default;
  fd_memcpy( proof.challenge_to_sign, ctx->challenge_to_sign, sizeof(proof.challenge_to_sign) );
  fd_memcpy( proof.validator_pubkey, ctx->bam_identity_pubkey_b58, sizeof(proof.validator_pubkey) );
  fd_memcpy( proof.signature,
             ctx->bam_auth_signature,
             fd_ulong_min( sizeof(ctx->bam_auth_signature), sizeof(proof.signature) ) );

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
  if( FD_UNLIKELY( !stream ) ) return;

  ctx->bam_stream            = stream;
  ctx->bam_stream_connecting = 1;
  ctx->bam_auth_ready        = 0;
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
  if( FD_UNLIKELY( !fd_grpc_client_stream_send( ctx->grpc_client, ctx->bam_stream, &bam_api_SchedulerMessage_msg, &msg, 0 ) ) ) {
    return;
  }
  ctx->bam_last_validator_heartbeat_ns = now;
  ctx->metrics.heartbeat_sent_cnt++;
}

static int
fd_bam_send_result( fd_bam_tile_t *               ctx,
                    fd_bam_bundle_result_t const * res ) {
  if( FD_UNLIKELY( !ctx->bam_stream || !ctx->bam_stream_live ) ) return 0;

  bam_types_AtomicTxnBatchResult atomic_res = bam_types_AtomicTxnBatchResult_init_default;
  atomic_res.seq_id = res->seq_id;

  fd_bam_encode_ctx_t encode_ctx = { .res = res };

  if( FD_LIKELY( res->execution_success ) ) {
    atomic_res.which_result = bam_types_AtomicTxnBatchResult_committed_tag;
    atomic_res.result.committed.transaction_results.funcs.encode = fd_bam_encode_committed_cb;
    atomic_res.result.committed.transaction_results.arg          = &encode_ctx;
  } else {
    atomic_res.which_result = bam_types_AtomicTxnBatchResult_not_committed_tag;
    fd_bam_fill_not_committed( &atomic_res.result.not_committed, res );
  }

  bam_types_MultipleAtomicTxnBatchResult multi = bam_types_MultipleAtomicTxnBatchResult_init_default;
  fd_bam_encode_batch_ctx_t batch_ctx = { .atomic_res = &atomic_res };
  multi.results.funcs.encode = fd_bam_encode_batch_results_cb;
  multi.results.arg          = &batch_ctx;

  bam_api_SchedulerMessage msg = bam_api_SchedulerMessage_init_default;
  msg.which_versioned_msg                        = bam_api_SchedulerMessage_v0_tag;
  msg.versioned_msg.v0.which_msg                 = bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag;
  msg.versioned_msg.v0.msg.multiple_atomic_txn_batch_result = multi;

  if( FD_UNLIKELY( !fd_grpc_client_stream_send( ctx->grpc_client, ctx->bam_stream, &bam_api_SchedulerMessage_msg, &msg, 0 ) ) ) {
    return 0;
  }
  ctx->metrics.result_sent_cnt++;
  FD_MCNT_INC( BAM, RESULTS_SENT, 1UL );
  return 1;
}

static int
fd_bam_send_leader_state( fd_bam_tile_t *                ctx,
                          fd_bam_leader_state_t const *  state ) {
  if( FD_UNLIKELY( !ctx->bam_stream || !ctx->bam_stream_live ) ) return 0;

  bam_types_LeaderState ls = bam_types_LeaderState_init_default;
  ls.slot                    = state->slot;
  ls.tick                    = state->tick;
  ls.slot_cu_budget_remaining = state->slot_cu_budget_remaining;

  bam_api_SchedulerMessage msg = bam_api_SchedulerMessage_init_default;
  msg.which_versioned_msg        = bam_api_SchedulerMessage_v0_tag;
  msg.versioned_msg.v0.which_msg = bam_api_SchedulerMessageV0_leader_state_tag;
  msg.versioned_msg.v0.msg.leader_state = ls;

  const int send_res = fd_grpc_client_stream_send( ctx->grpc_client, ctx->bam_stream, &bam_api_SchedulerMessage_msg, &msg, 0 );
  if( FD_UNLIKELY( send_res ) ) {
    ctx->metrics.leader_state_sent_cnt++;
    FD_MCNT_INC( BAM, LEADER_STATE_SENT, 1UL );
  }

  return send_res;
}

static int
fd_bam_flush_results( fd_bam_tile_t * ctx ) {
  int busy = 0;
  while( ctx->bam_pending_results ) {
    fd_bam_bundle_result_t const * res =
        &ctx->bam_results[ ctx->bam_results_head ];
    if( FD_UNLIKELY( !fd_bam_send_result( ctx, res ) ) ) break;
    ctx->bam_results_head = (ctx->bam_results_head + 1) % FD_BAM_MAX_PENDING_RESULTS;
    ctx->bam_pending_results--;
    busy = 1;
  }
  return busy;
}

int
fd_bam_test_flush_results( fd_bam_tile_t * ctx ) {
  return fd_bam_flush_results( ctx );
}

int
fd_bam_test_drive( fd_bam_tile_t * ctx,
                   long             now ) {
  return fd_bam_drive( ctx, now );
}

/* Decodes bam_api.SchedulerResponse (versioned envelope). On decode failure or
   unsupported version it bumps metrics and may request a reset; otherwise it
   dispatches nested payloads to versioned handlers. */
static void
fd_bam_handle_scheduler_response( fd_bam_tile_t * ctx,
                                   pb_istream_t *   istream ) {
  uint32_t      tag;
  pb_wire_type_t wire_type;
  bool          eof         = false;
  uint32_t      version_tag = 0U;
  int           seen_v0     = 0;

  while( pb_decode_tag( istream, &wire_type, &tag, &eof ) ) {
    version_tag = tag;
    if( tag == bam_api_SchedulerResponse_v0_tag ) {
      if( FD_UNLIKELY( wire_type != PB_WT_STRING ) ) {
        if( FD_UNLIKELY( !pb_skip_field( istream, wire_type ) ) ) goto fail;
        continue;
      }
      pb_istream_t substream;
      if( FD_UNLIKELY( !pb_make_string_substream( istream, &substream ) ) ) goto fail;
      int ok = fd_bam_decode_scheduler_response_v0( ctx, &substream );
      pb_close_string_substream( istream, &substream );
      if( FD_UNLIKELY( !ok ) ) goto fail;
      seen_v0 = 1;
    } else {
      if( FD_UNLIKELY( !pb_skip_field( istream, wire_type ) ) ) goto fail;
    }
  }

  if( FD_UNLIKELY( !eof ) ) goto fail;
  if( FD_UNLIKELY( !seen_v0 ) ) {
    if( version_tag && version_tag != bam_api_SchedulerResponse_v0_tag ) {
      FD_LOG_WARNING(( "Unsupported SchedulerResponse version (tag=%u); scheduling reset", version_tag ));
      ctx->metrics.transport_fail_cnt++;
      ctx->defer_reset = 1;
    } else {
      ctx->metrics.decode_fail_cnt++;
      FD_LOG_WARNING(( "Protobuf decode of (bam_api.SchedulerResponse) missing version" ));
    }
  }
  return;

fail:
  ctx->metrics.decode_fail_cnt++;
  FD_LOG_WARNING(( "Protobuf decode of (bam_api.SchedulerResponse) failed (%s)", PB_GET_ERROR( istream ) ));
}

static int
fd_bam_drive( fd_bam_tile_t * ctx,
              long             now ) {
  int busy = 0;
  if( FD_UNLIKELY( !ctx->grpc_client ) ) return busy;
  if( FD_UNLIKELY( !fd_grpc_client_is_connected( ctx->grpc_client ) ) ) return busy;

  long builder_info_valid_until = ctx->builder_info_valid_until;
  int  builder_info_ready       = builder_info_valid_until != 0L;
  if( FD_UNLIKELY( builder_info_ready && now >= builder_info_valid_until ) ) {
    ctx->builder_info_valid_until = 0L;
    ctx->bam_last_config_poll_ns  = 0L;
    builder_info_valid_until      = 0L;
    builder_info_ready            = 0;
  }

  if( FD_UNLIKELY( !ctx->bam_auth_ready && !ctx->bam_auth_inflight && !ctx->bam_stream ) ) {
    fd_bam_request_auth_challenge( ctx );
    busy = 1;
  }

  if( FD_LIKELY( ctx->bam_auth_ready ) ) {
    fd_bam_try_start_stream( ctx );
  }

  long const config_refresh_margin_ns = (long)5e9;
  int need_config = builder_info_ready ? 0 : 1;
  if( FD_UNLIKELY( builder_info_ready ) ) {
    if( FD_UNLIKELY( now + config_refresh_margin_ns >= builder_info_valid_until ) ) {
      need_config = 1;
    }
  }
  if( FD_UNLIKELY( ctx->bam_last_config_poll_ns == 0L ) ) need_config = 1;
  if( FD_UNLIKELY( need_config && !ctx->bam_config_inflight ) ) {
    long const throttle_ns = builder_info_ready ? (long)5e9 : (long)1e9;
    if( FD_UNLIKELY( ctx->bam_last_config_poll_ns == 0L ||
                     now - ctx->bam_last_config_poll_ns >= throttle_ns ) ) {
      fd_bam_request_config( ctx, now );
      busy = 1;
    }
  }

  long const heartbeat_ns = (long)5e9;
  if( FD_LIKELY( ctx->bam_stream && ctx->bam_stream_live ) ) {
    if( FD_UNLIKELY( ( ctx->bam_last_validator_heartbeat_ns == 0L ) ||
                     ( now - ctx->bam_last_validator_heartbeat_ns >= heartbeat_ns ) ) ) {
      fd_bam_send_heartbeat( ctx, now );
      busy = 1;
    }
  }

  if( FD_UNLIKELY( ctx->bam_leader_pending ) ) {
    if( FD_LIKELY( fd_bam_send_leader_state( ctx, &ctx->bam_leader_state ) ) ) {
      ctx->bam_leader_pending = 0U;
      busy = 1;
    }
  }

  if( FD_UNLIKELY( fd_keepalive_should_tx( ctx->keepalive, now ) ) ) {
    fd_bam_client_send_ping( ctx );
    busy = 1;
  }

  if( FD_LIKELY( ctx->bam_stream_live ) ) busy |= fd_bam_flush_results( ctx );

  return busy;
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
                                 long               now ) {
  /* Send a PING */
  if( FD_UNLIKELY( fd_keepalive_should_tx( ctx->keepalive, now ) ) ) {
    fd_bam_client_send_ping( ctx );
    return 1;
  }

  return 0;
}

static void
fd_bam_client_step1( fd_bam_tile_t * ctx,
                       int *              charge_busy ) {

  if( FD_UNLIKELY( !FD_VOLATILE_CONST( ctx->enabled ) ) ) {
    /* Admin can pause BAM without tearing the tile down; skip reconnect/IO until re-enabled. */
    return;
  }

  if( FD_UNLIKELY( ctx->defer_reset ) ) {
    fd_bam_client_reset( ctx );
    *charge_busy = 1;
    FD_LOG_WARNING(( "BAM client reset requested; retrying %s/" FD_IP4_ADDR_FMT ":%hu in %.3f ms",
                     ctx->server_fqdn,
                     FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
                     ctx->server_tcp_port,
                     fd_bam_client_retry_ms( ctx ) ));
    return;
  }

  /* Wait for TCP socket to connect */
  if( FD_UNLIKELY( !ctx->tcp_sock_connected ) ) {
    if( FD_UNLIKELY( ctx->tcp_sock < 0 ) ) goto reconnect;

    struct pollfd pfds[1] = {
      { .fd = ctx->tcp_sock, .events = POLLOUT }
    };
    int poll_res = poll( pfds, 1, 0 );
    if( FD_UNLIKELY( poll_res < 0 ) ) {
      FD_LOG_ERR(( "poll(tcp_sock) failed (%i-%s)", errno, fd_io_strerror( errno ) ));
    }
    if( poll_res == 0 ) return;

    if( pfds[0].revents & (POLLERR|POLLHUP) ) {
      int connect_err = fd_bam_client_do_connect( ctx, 0U );
      fd_bam_client_reset( ctx );
      ctx->metrics.transport_fail_cnt++;
      FD_LOG_WARNING(( "BAM gRPC connect attempt failed (%i-%s) while dialing %s/" FD_IP4_ADDR_FMT ":%hu; retrying in %.3f ms",
                       connect_err, fd_io_strerror( connect_err ),
                       ctx->server_fqdn,
                       FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
                       ctx->server_tcp_port,
                       fd_bam_client_retry_ms( ctx ) ));
      *charge_busy = 1;
      return;
    }
    if( pfds[0].revents & POLLOUT ) {
      int connect_err = fd_bam_client_do_connect( ctx, 0U );
      if( FD_UNLIKELY( connect_err ) ) {
        fd_bam_client_reset( ctx );
        ctx->metrics.transport_fail_cnt++;
        FD_LOG_WARNING(( "BAM TCP socket reported writable but connect failed (%i-%s) to %s/" FD_IP4_ADDR_FMT ":%hu; retrying in %.3f ms",
                         connect_err, fd_io_strerror( connect_err ),
                         ctx->server_fqdn,
                         FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
                         ctx->server_tcp_port,
                         fd_bam_client_retry_ms( ctx ) ));
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
  reconnect:
    if( FD_UNLIKELY( fd_bam_tile_should_stall( ctx, fd_bam_now() ) ) ) {
      return;
    }
    fd_bam_client_create_conn( ctx );
    *charge_busy = 1;
    return;
  }

  /* Did a HTTP/2 PING time out */
  long check_ts = ctx->cached_ts = fd_bam_now();
  if( FD_UNLIKELY( fd_keepalive_is_timeout( ctx->keepalive, check_ts ) ) ) {
    double const timeout_sec = (double)( check_ts - ctx->keepalive->ts_last_tx )/1e9;
    ctx->keepalive->inflight = 0;
    fd_bam_client_reset( ctx );
    ctx->metrics.transport_fail_cnt++;
    FD_LOG_WARNING(( "BAM gRPC timed out (HTTP/2 PING went unanswered for %.2f seconds); retrying %s/" FD_IP4_ADDR_FMT ":%hu in %.3f ms",
                     timeout_sec,
                     ctx->server_fqdn,
                     FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
                     ctx->server_tcp_port,
                     fd_bam_client_retry_ms( ctx ) ));
    *charge_busy = 1;
    return;
  }

  /* Did BAM heartbeat time out */
  if( FD_UNLIKELY( ctx->bam_stream_live &&
                   ctx->bam_last_builder_heartbeat_ns != 0L &&
                   check_ts - ctx->bam_last_builder_heartbeat_ns >= FD_BAM_HEARTBEAT_TIMEOUT_NS ) ) {
    double const missing_sec = (double)( check_ts - ctx->bam_last_builder_heartbeat_ns )/1e9;
    ctx->keepalive->inflight = 0;
    fd_bam_client_reset( ctx );
    ctx->metrics.transport_fail_cnt++;
    FD_LOG_WARNING(( "BAM heartbeat timed out (no heartbeat for %.2f seconds); retrying %s/" FD_IP4_ADDR_FMT ":%hu in %.3f ms",
                     missing_sec,
                     ctx->server_fqdn,
                     FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
                     ctx->server_tcp_port,
                     fd_bam_client_retry_ms( ctx ) ));
    *charge_busy = 1;
    return;
  }

  /* Drive I/O, SSL handshake, and any inflight requests */
  if( FD_UNLIKELY( !fd_bam_client_drive_io( ctx, charge_busy ) ) ) {
    fd_bam_client_reset( ctx );
    ctx->metrics.transport_fail_cnt++;
    *charge_busy = 1;
    FD_LOG_WARNING(( "BAM gRPC transport I/O failed; reconnecting to %s/" FD_IP4_ADDR_FMT ":%hu in %.3f ms (see preceding socket/TLS log for root cause)",
                     ctx->server_fqdn,
                     FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
                     ctx->server_tcp_port,
                     fd_bam_client_retry_ms( ctx ) ));
    return;
  }

  if( FD_UNLIKELY( ctx->defer_reset /* new error? */ ) ) {
    fd_bam_client_reset( ctx );
    ctx->metrics.transport_fail_cnt++;
    *charge_busy = 1;
    FD_LOG_WARNING(( "BAM client requested reset; retrying %s/" FD_IP4_ADDR_FMT ":%hu in %.3f ms",
                     ctx->server_fqdn,
                     FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
                     ctx->server_tcp_port,
                     fd_bam_client_retry_ms( ctx ) ));
    return;
  }

  /* Are we ready to issue a new request? */
  if( FD_UNLIKELY( fd_grpc_client_request_is_blocked( ctx->grpc_client ) ) ) return;
  long io_ts = fd_bam_now();
  if( FD_UNLIKELY( fd_bam_tile_should_stall( ctx, io_ts ) ) ) return;

  *charge_busy |= fd_bam_client_step_reconnect( ctx, io_ts );
  *charge_busy |= fd_bam_drive( ctx, io_ts );
}

static void
fd_bam_client_log_status( fd_bam_tile_t * ctx ) {
  int status = fd_bam_client_status( ctx );

  int const connected_now    = ( status == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED );
  int const connected_before = ( ctx->bundle_status_logged == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED );

  if( FD_UNLIKELY( connected_now != connected_before ) ) {
    long ts = fd_log_wallclock();
    if( FD_LIKELY( ts-(ctx->last_bundle_status_log_nanos) >= (long)1e6 ) ) {
      if( connected_now ) {
        char const * scheme = "http";
# if FD_HAS_OPENSSL
        if( ctx->is_ssl ) scheme = "https";
# endif
        FD_LOG_NOTICE(( "Connected to BAM node at %s://%s/ (" FD_IP4_ADDR_FMT ":%hu)",
                        scheme,
                        ctx->server_fqdn,
                        FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
                        ctx->server_tcp_port ));
        ctx->metrics.connection_cnt++;
        FD_MCNT_INC( BAM, CONNECTIONS, 1UL );
      } else {
        FD_LOG_WARNING(( "Disconnected from BAM node" ));
        ctx->metrics.disconnect_cnt++;
        FD_MCNT_INC( BAM, DISCONNECTS, 1UL );
      }
      ctx->last_bundle_status_log_nanos = ts;
      ctx->bundle_status_logged = (uchar)status;
    }
  }
}

void
fd_bam_client_step( fd_bam_tile_t * ctx,
                       int *              charge_busy ) {
  /* Edge-trigger logging with rate limiting */
  fd_bam_client_step1( ctx, charge_busy );
  fd_bam_client_log_status( ctx );
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
  ctx->defer_reset = 1;
}

/* Forwards a bundle transaction to the tango message bus. */

static void
fd_bam_tile_publish_bundle_txn(
    fd_bam_tile_t * ctx,
    void const *       txn,
    ushort             txn_sz,  /* <= FD_TXN_MTU */
    uchar              bundle_txn_cnt,
    uchar              batch_idx,
    uint               source_ipv4
) {
  if( FD_UNLIKELY( !ctx->builder_info_valid_until ) ) {
    ctx->metrics.missing_builder_info_fail_cnt++; /* unreachable */
    return;
  }

  fd_txn_m_t * txnm = fd_chunk_to_laddr( ctx->verify_out.mem, ctx->verify_out.chunk );
  *txnm = (fd_txn_m_t) {
    .reference_slot = 0UL,
    .payload_sz     = txn_sz,
    .txn_t_sz       = 0U,
    .source_ipv4    = source_ipv4,
    .source_tpu     = FD_TXN_M_TPU_SOURCE_BAM,
    .block_engine   = {0},
    .bam = {
      .max_schedule_slot = ctx->bundle_max_schedule_slot,
      .seq_id            = ctx->bundle_seq,
      .batch_cnt         = bundle_txn_cnt,
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
  ctx->metrics.txn_received_cnt++;
}

/* Forwards a regular transaction to the tango message bus. */

static void
fd_bam_tile_publish_txn(
    fd_bam_tile_t * ctx,
    void const *       txn,
    ulong              txn_sz,  /* <= FD_TXN_MTU */
    ulong              max_schedule_slot,
    uint               seq_id,
    uchar              batch_idx,
    uchar              batch_cnt,
    uchar              revert_on_error,
    uint               source_ipv4
) {
  fd_txn_m_t * txnm = fd_chunk_to_laddr( ctx->verify_out.mem, ctx->verify_out.chunk );
  *txnm = (fd_txn_m_t) {
    .reference_slot = 0UL,
    .payload_sz     = (ushort)txn_sz,
    .txn_t_sz       = 0U,
    .source_ipv4    = source_ipv4,
    .source_tpu     = FD_TXN_M_TPU_SOURCE_BAM,
    .block_engine   = {0},
    .bam = {
      .max_schedule_slot = max_schedule_slot,
      .seq_id            = seq_id,
      .batch_cnt         = batch_cnt,
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
  ctx->metrics.txn_received_cnt++;
}

/* Called for each transaction in a bundle.  Simply counts up
   bundle_txn_cnt, but does not publish anything. */

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
    ctx->bam_stream_live        = 1;
    ctx->bam_stream_connecting  = 0;
    ctx->bam_last_validator_heartbeat_ns = now;
    ctx->bam_last_builder_heartbeat_ns   = now;
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
  pb_istream_t istream = pb_istream_from_buffer( protobuf, protobuf_sz );
  switch( request_ctx ) {
  case FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge:
    if( FD_UNLIKELY( !fd_bam_handle_auth_challenge( ctx, protobuf, protobuf_sz ) ) ) {
      ctx->metrics.decode_fail_cnt++;
      fd_bam_tile_backoff( ctx, fd_bam_now() );
    }
    break;
  case FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig:
    fd_bam_handle_config( ctx, &istream );
    ctx->bam_config_inflight = 0;
    break;
  case FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream:
    fd_bam_handle_scheduler_response( ctx, &istream );
    break;
  default:
    FD_LOG_ERR(( "Received unexpected gRPC message (request_ctx=%lu)", request_ctx ));
  }
}

static void
fd_bam_client_request_failed( fd_bam_tile_t * ctx,
                                 ulong              request_ctx ) {
  fd_bam_tile_backoff( ctx, fd_bam_now() );
  switch( request_ctx ) {
  case FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge:
    ctx->bam_auth_inflight       = 0;
    ctx->bam_auth_ready          = 0;
    ctx->bam_auth_challenge_len  = 0;
    break;
  case FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig:
    ctx->bam_config_inflight = 0;
    break;
  case FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream:
    ctx->bam_stream             = NULL;
    ctx->bam_stream_live        = 0;
    ctx->bam_stream_connecting  = 0;
    ctx->bam_auth_ready         = 0;
    ctx->bam_auth_challenge_len = 0;
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
    ctx->bam_stream_live       = 0;
    ctx->bam_stream_connecting = 0;
    ctx->bam_leader_pending    = 0U;
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
      ctx->bam_auth_challenge_len = 0;
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
  ctx->metrics.timeout_fail_cnt++;
  FD_MCNT_INC( BAM, ERRORS_TIMEOUT, 1UL );
  ctx->defer_reset = 1;
  switch( request_ctx ) {
  case FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge:
    ctx->bam_auth_inflight       = 0;
    ctx->bam_auth_ready          = 0;
    ctx->bam_auth_challenge_len  = 0;
    break;
  case FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig:
    ctx->bam_config_inflight = 0;
    break;
  case FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream:
    ctx->bam_stream            = NULL;
    ctx->bam_stream_live       = 0;
    ctx->bam_stream_connecting = 0;
    ctx->bam_leader_pending    = 0U;
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
  ctx->metrics.ping_ack_cnt++;
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

/* Decrease verbosity */
#define DISCONNECTED FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED
#define CONNECTING   FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTING
#define CONNECTED    FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED

int
fd_bam_client_status( fd_bam_tile_t const * ctx ) {
  /* Treat the connection as "owned" only when every layer (TCP socket,
     HTTP/2 session, bundle auth, scheduler stream, and keepalive) is
     healthy.  Downstream tiles key off this switch to stop ingesting
     QUIC/bundle traffic, so any premature CONNECTED state would cause a
     data gap. */
  if( FD_UNLIKELY( ( !ctx->tcp_sock_connected ) |
                   ( !ctx->grpc_client        ) ) ) {
    return DISCONNECTED;
  }

  fd_h2_conn_t * conn = fd_grpc_client_h2_conn( ctx->grpc_client );
  if( FD_UNLIKELY( !conn ) ) {
    return DISCONNECTED; /* no conn */
  }
  if( FD_UNLIKELY( conn->flags &
      ( FD_H2_CONN_FLAGS_DEAD |
        FD_H2_CONN_FLAGS_SEND_GOAWAY ) ) ) {
    return DISCONNECTED;
  }

  if( FD_UNLIKELY( conn->flags &
      ( FD_H2_CONN_FLAGS_CLIENT_INITIAL      |
        FD_H2_CONN_FLAGS_WAIT_SETTINGS_ACK_0 |
        FD_H2_CONN_FLAGS_WAIT_SETTINGS_0     |
        FD_H2_CONN_FLAGS_SERVER_INITIAL ) ) ) {
    return CONNECTING; /* connection is not ready */
  }

  if( FD_UNLIKELY( !ctx->bam_stream_live ) ) {
    return CONNECTING;
  }

  if( FD_UNLIKELY( fd_keepalive_is_timeout( ctx->keepalive, fd_bam_now() ) ) ) {
    return DISCONNECTED; /* possible timeout */
  }

  if( FD_UNLIKELY( !fd_grpc_client_is_connected( ctx->grpc_client ) ) ) {
    return CONNECTING;
  }

  return CONNECTED;
}

#undef DISCONNECTED
#undef CONNECTING
#undef CONNECTED

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
