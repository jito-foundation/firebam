/* fd_bam_client.c steps gRPC related tasks. */

#define _GNU_SOURCE /* SOL_TCP */
#include "fd_bam_tile_private.h"
#include "proto/bam_api.pb.h"
#include "proto/bam_types.pb.h"
#include "../fd_txn_m_t.h"
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
#include <sys/socket.h> /* socket */
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>

#define FD_BAM_CLIENT_REQUEST_TIMEOUT ((long)8e9) /* 8 seconds */

#define FD_BAM_AUTH_LABEL "X_OFF_CHAIN_JITO_BAM_V1\0"
static char const fd_bam_auth_label[] = FD_BAM_AUTH_LABEL;

typedef struct {
  fd_bam_tile_t * ctx;
  bam_types_Packet    packets[ FD_PACK_MAX_TXN_PER_BUNDLE ];
  ulong               packet_cnt;
  int                 revert_on_error;
  int                 revert_flag_set;
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
                                ulong           txn_sz,
                                ulong           bundle_txn_cnt,
                                uint            source_ipv4 );

static void
fd_bam_tile_publish_txn( fd_bam_tile_t * ctx,
                         void const *    txn,
                         ulong           txn_sz,
                         uint            source_ipv4 );

__attribute__((weak)) long
fd_bam_now( void ) {
  return fd_log_wallclock();
}

void
fd_bam_client_reset( fd_bam_tile_t * ctx ) {
  if( FD_UNLIKELY( ctx->tcp_sock >= 0 ) ) {
    if( FD_UNLIKELY( 0!=close( ctx->tcp_sock ) ) ) {
      FD_LOG_ERR(( "close(tcp_sock=%i) failed (%i-%s)", ctx->tcp_sock, errno, fd_io_strerror( errno ) ));
    }
    ctx->tcp_sock = -1;
    ctx->tcp_sock_connected = 0;
  }
  /* Leave the last good BAM contact info intact here; the tile decides
     when to fall back to the default ports after seeing the status
     transition so gossip never advertises a half-cleared override. */
  ctx->defer_reset = 0;

  ctx->builder_info_avail       = 0;
  ctx->builder_info_wait        = 0;
  ctx->builder_info_valid_until = 0L;
  ctx->bundle_max_schedule_slot = ULONG_MAX;

  memset( ctx->rtt, 0, sizeof(fd_rtt_estimate_t) );

# if FD_HAS_OPENSSL
  if( FD_UNLIKELY( ctx->ssl ) ) {
    SSL_free( ctx->ssl );
    ctx->ssl = NULL;
  }
# endif

  fd_bam_tile_backoff( ctx, fd_bam_now() );

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
  // ctx->bam_pending_results        = 0UL;
  // ctx->bam_results_head           = 0UL;
  // ctx->bam_results_tail           = 0UL;
  ctx->bam_leader_pending         = 0U;
}

static int
fd_bam_client_do_connect( fd_bam_tile_t const * ctx,
                             uint                     ip4_addr ) {
  if( FD_UNLIKELY( ctx->tcp_sock<0 ) ) return EBADF;

  if( FD_UNLIKELY( ip4_addr==0U ) ) {
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
    FD_LOG_WARNING(( "fd_getaddrinfo `%s` failed (%d-%s)", ctx->server_fqdn, err, fd_gai_strerror( err ) ));
    fd_bam_client_reset( ctx );
    ctx->metrics.transport_fail_cnt++;
    return;
  }
  uint const ip4_addr = ((struct sockaddr_in *)res->ai_addr)->sin_addr.s_addr;
  ctx->server_ip4_addr = ip4_addr;

  int tcp_sock = socket( AF_INET, SOCK_STREAM|SOCK_CLOEXEC, 0 );
  if( FD_UNLIKELY( tcp_sock<0 ) ) {
    FD_LOG_ERR(( "socket(AF_INET,SOCK_STREAM|SOCK_CLOEXEC,0) failed (%i-%s)", errno, fd_io_strerror( errno ) ));
  }
  ctx->tcp_sock = tcp_sock;

  if( FD_UNLIKELY( 0!=setsockopt( tcp_sock, SOL_SOCKET, SO_RCVBUF, &ctx->so_rcvbuf, sizeof(int) ) ) ) {
    FD_LOG_ERR(( "setsockopt(SOL_SOCKET,SO_RCVBUF,%i) failed (%i-%s)", ctx->so_rcvbuf, errno, fd_io_strerror( errno ) ));
  }

  int tcp_nodelay = 1;
  if( FD_UNLIKELY( 0!=setsockopt( tcp_sock, SOL_TCP, TCP_NODELAY, &tcp_nodelay, sizeof(int) ) ) ) {
    FD_LOG_ERR(( "setsockopt failed (%d-%s)", errno, fd_io_strerror( errno ) ));
  }

  if( FD_UNLIKELY( fcntl( tcp_sock, F_SETFL, O_NONBLOCK )==-1 ) ) {
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
    if( FD_UNLIKELY( connect_err!=EINPROGRESS ) ) {
      FD_LOG_WARNING(( "connect(tcp_sock," FD_IP4_ADDR_FMT ":%u) failed (%i-%s)",
                      FD_IP4_ADDR_FMT_ARGS( ip4_addr ), ctx->server_tcp_port,
                      connect_err, fd_io_strerror( connect_err ) ));
      fd_bam_client_reset( ctx );
      ctx->metrics.transport_fail_cnt++;
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
  for( uint i=0U; i<res->txn_cnt; i++ ) {
    bam_types_TransactionCommittedResult txn_res = bam_types_TransactionCommittedResult_init_default;
    txn_res.cus_consumed               = res->consumed_cus[ i ];
    txn_res.feepayer_balance_lamports  = 0UL;
    txn_res.loaded_accounts_data_size  = 0U;
    txn_res.execution_success          = ( res->sanitize_success[ i ] && res->transaction_err[ i ]==0U );
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

  if( FD_UNLIKELY( res->scheduling_error!=FD_BAM_SCHED_ERR_NONE ) ) {
    if( FD_LIKELY( res->scheduling_error<=_bam_types_SchedulingError_MAX ) ) {
      out->which_reason                 = bam_types_NotCommitted_scheduling_error_tag;
      out->reason.scheduling_error      = (bam_types_SchedulingError)res->scheduling_error;
    } else {
      out->which_reason                 = bam_types_NotCommitted_generic_invalid_tag;
      snprintf( out->reason.generic_invalid.message,
                sizeof(out->reason.generic_invalid.message),
                "invalid scheduling error %u",
                res->scheduling_error );
    }
    return;
  }

  for( uint i=0U; i<res->txn_cnt; i++ ) {
    if( FD_UNLIKELY( !res->sanitize_success[ i ] ) ) {
      out->which_reason                             = bam_types_NotCommitted_deserialization_error_tag;
      out->reason.deserialization_error.index       = i;
      out->reason.deserialization_error.reason      = bam_types_DeserializationErrorReason_SANITIZE_ERROR;
      return;
    }
  }

  for( uint i=0U; i<res->txn_cnt; i++ ) {
    uint err = res->transaction_err[ i ];
    if( FD_UNLIKELY( err ) ) {
      if( FD_LIKELY( err < _bam_types_TransactionErrorReason_ARRAYSIZE ) ) {
        out->which_reason                      = bam_types_NotCommitted_transaction_error_tag;
        out->reason.transaction_error.index    = i;
        out->reason.transaction_error.reason   = (bam_types_TransactionErrorReason)err;
      } else {
        out->which_reason                      = bam_types_NotCommitted_generic_invalid_tag;
        snprintf( out->reason.generic_invalid.message,
                  sizeof(out->reason.generic_invalid.message),
                  "transaction error %u",
                  err );
      }
      return;
    }
  }

  out->which_reason = bam_types_NotCommitted_generic_invalid_tag;
  strncpy( out->reason.generic_invalid.message,
           "bundle execution failed",
           sizeof(out->reason.generic_invalid.message)-1UL );
  out->reason.generic_invalid.message[ sizeof(out->reason.generic_invalid.message)-1UL ] = '\0';
}

static bool
fd_bam_collect_packet( pb_istream_t *         stream,
                       pb_field_t const *     field,
                       void **                arg ) {
  (void)field;
  fd_bam_batch_ctx_t * state = *arg;
  if( FD_UNLIKELY( state->packet_cnt >= FD_PACK_MAX_TXN_PER_BUNDLE ) ) {
    FD_LOG_WARNING(( "Received AtomicTxnBatch exceeding max bundle size" ));
    return false;
  }

  bam_types_Packet packet = bam_types_Packet_init_default;
  if( FD_UNLIKELY( !pb_decode( stream, &bam_types_Packet_msg, &packet ) ) ) {
    return false;
  }

  if( FD_UNLIKELY( packet.data.size > FD_TXN_MTU ) ) {
    if( FD_LIKELY( state->ctx ) ) {
      state->ctx->metrics.packet_drop_cnt++;
      FD_MCNT_INC( BAM, PACKETS_DROPPED, 1UL );
    }
    FD_LOG_WARNING(( "Received AtomicTxnBatch packet exceeding MTU (%lu>%lu); dropping batch",
                     (ulong)packet.data.size, FD_TXN_MTU ));
    return false;
  }

  state->packets[ state->packet_cnt ] = packet;
  if( packet.has_meta && packet.meta.has_flags ) {
    int flag = packet.meta.flags.revert_on_error;
    if( state->revert_flag_set && state->revert_on_error!=flag ) {
      FD_LOG_WARNING(( "AtomicTxnBatch contains mixed revert_on_error flags" ));
    }
    state->revert_on_error = flag;
    state->revert_flag_set = 1;
  }

  state->packet_cnt++;
  return true;
}

static void
fd_bam_publish_batch( fd_bam_tile_t *            ctx,
                      fd_bam_batch_ctx_t *       state,
                      bam_types_AtomicTxnBatch const * batch ) {
  if( FD_UNLIKELY( state->packet_cnt==0 ) ) return;

  if( state->revert_on_error ) {
    if( FD_UNLIKELY( !ctx->builder_info_avail ) ) {
      ctx->metrics.missing_builder_info_fail_cnt++;
      return;
    }
    ctx->bundle_seq                = batch->seq_id;
    ctx->bundle_txn_cnt            = state->packet_cnt;
    ctx->bundle_max_schedule_slot  = batch->max_schedule_slot ? batch->max_schedule_slot : ULONG_MAX;

    for( ulong i=0UL; i<state->packet_cnt; i++ ) {
      bam_types_Packet const * pkt = &state->packets[ i ];
      fd_bam_tile_publish_bundle_txn( ctx, pkt->data.bytes, pkt->data.size, state->packet_cnt, 0U );
    }
    ctx->metrics.bundle_received_cnt++;
  } else {
    for( ulong i=0UL; i<state->packet_cnt; i++ ) {
      bam_types_Packet const * pkt = &state->packets[ i ];
      fd_bam_tile_publish_txn( ctx, pkt->data.bytes, pkt->data.size, 0U );
    }
  }
  ctx->bundle_max_schedule_slot = ULONG_MAX;
}

static int
fd_bam_decode_batch( fd_bam_tile_t * ctx,
                     pb_istream_t *   stream ) {
  fd_bam_batch_ctx_t state = {
    .ctx              = ctx,
    .packet_cnt       = 0UL,
    .revert_on_error  = 0,
    .revert_flag_set  = 0
  };

  bam_types_AtomicTxnBatch batch = bam_types_AtomicTxnBatch_init_default;
  batch.packets = (pb_callback_t){
    .funcs.decode = fd_bam_collect_packet,
    .arg          = &state
  };

  if( FD_UNLIKELY( !pb_decode( stream, &bam_types_AtomicTxnBatch_msg, &batch ) ) ) {
    FD_LOG_WARNING(( "Protobuf decode of (bam_types.AtomicTxnBatch) failed" ));
    return 0;
  }

  fd_bam_publish_batch( ctx, &state, &batch );
  return 1;
}

static bool
fd_bam_visit_batches( pb_istream_t *       stream,
                      pb_field_t const *   field,
                      void **              arg ) {
  (void)field;
  fd_bam_tile_t * ctx = *arg;
  pb_istream_t substream;
  if( FD_UNLIKELY( !pb_make_string_substream( stream, &substream ) ) ) return false;

  int ok = fd_bam_decode_batch( ctx, &substream );
  pb_close_string_substream( stream, &substream );
  return ok;
}

static void
fd_bam_client_sample_heartbeat_delay( fd_bam_tile_t * ctx,
                                      uint64_t        time_sent_microseconds ) {
  if( FD_UNLIKELY( !time_sent_microseconds ) ) return;
  ulong tsorig_ns = time_sent_microseconds * 1000UL;
  long  now_ns    = fd_bam_now();
  ulong now_u     = fd_ulong_if( now_ns>=0L, (ulong)now_ns, 0UL );
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
  if( FD_UNLIKELY( challenge_len==sizeof(resp.challenge_to_sign) ) ) {
    ctx->bam_auth_inflight = 0;
    FD_LOG_WARNING(( "AuthChallengeResponse challenge not NUL terminated" ));
    return 0;
  }

  ctx->bam_auth_inflight      = 0;
  ctx->bam_auth_challenge_len = (uint)challenge_len;
  fd_memset( ctx->bam_auth_challenge, 0, sizeof(ctx->bam_auth_challenge) );
  fd_memcpy( ctx->bam_auth_challenge, resp.challenge_to_sign, challenge_len );

  size_t label_len      = sizeof( fd_bam_auth_label ) - 1UL;
  ulong  sign_payload_sz = label_len + challenge_len;
  uchar  sign_payload[ sizeof(fd_bam_auth_label) + 256UL ];
  fd_memcpy( sign_payload, fd_bam_auth_label, label_len );
  fd_memcpy( sign_payload + label_len, ctx->bam_auth_challenge, challenge_len );

  uchar signature[ 64 ];
  fd_keyguard_client_sign( ctx->keyguard_client, signature, sign_payload, sign_payload_sz, FD_KEYGUARD_SIGN_TYPE_ED25519 );

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
      FD_BAM_CLIENT_REQ_BAM_GetConfig,
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
      ctx->builder_info_avail        = 1;
      ctx->builder_info_valid_until  = fd_bam_now() + (long)( 60e9 * 5. );
    }
  }

  if( FD_LIKELY( resp.has_bam_config ) ) {
    bam_types_BamConfig const * cfg = &resp.bam_config;

    fd_ip4_port_t new_tpu_addr = ctx->bam_tpu_addr;
    fd_ip4_port_t new_tpu_quic_addr = ctx->bam_tpu_quic_addr;
    int have_tpu = 0;
    int have_tpu_quic = 0;

    if( cfg->has_tpu_sock ) {
      uint ip4;
      if( FD_LIKELY( fd_cstr_to_ip4_addr( cfg->tpu_sock.ip, &ip4 ) ) &&
          FD_LIKELY( cfg->tpu_sock.port>0 && cfg->tpu_sock.port<=USHORT_MAX ) ) {
        new_tpu_addr.addr = ip4;
        new_tpu_addr.port = fd_ushort_bswap( (ushort)cfg->tpu_sock.port );
        have_tpu = 1;
      } else {
        FD_LOG_WARNING(( "Invalid BAM TPU socket in ConfigResponse (ip=%s port=%u)",
                         cfg->tpu_sock.ip,
                         cfg->tpu_sock.port ));
      }
    }

    if( cfg->has_tpu_fwd_sock ) {
      uint ip4;
      if( FD_LIKELY( fd_cstr_to_ip4_addr( cfg->tpu_fwd_sock.ip, &ip4 ) ) &&
          FD_LIKELY( cfg->tpu_fwd_sock.port>0 && cfg->tpu_fwd_sock.port<=USHORT_MAX ) ) {
        new_tpu_quic_addr.addr = ip4;
        new_tpu_quic_addr.port = fd_ushort_bswap( (ushort)cfg->tpu_fwd_sock.port );
        have_tpu_quic = 1;
      } else {
        FD_LOG_WARNING(( "Invalid BAM TPU forward socket in ConfigResponse (ip=%s port=%u)",
                         cfg->tpu_fwd_sock.ip,
                         cfg->tpu_fwd_sock.port ));
      }
    }

    if( FD_LIKELY( have_tpu ) ) {
      /* Treat the QUIC tuple as optional: if BAM stops advertising it we
         revert to the Firedancer default (0) and still flag the contact
         info as changed so gossip releases the override cleanly. */
      int quic_changed = have_tpu_quic ? ( ctx->bam_tpu_quic_addr.l!=new_tpu_quic_addr.l )
                                       : ( ctx->bam_tpu_quic_addr.l!=0UL );
      /* Signal the tile loop to republish once the connection comes up.
         We keep the active flag separate so a reconnect without new
         config still reuses the last good endpoints. */
      if( FD_UNLIKELY( !ctx->bam_contact_avail || ctx->bam_tpu_addr.l!=new_tpu_addr.l || quic_changed ) ) {
        ctx->bam_contact_dirty = 1U;
      }
      ctx->bam_tpu_addr      = new_tpu_addr;
      ctx->bam_tpu_quic_addr = have_tpu_quic ? new_tpu_quic_addr : (fd_ip4_port_t){ .l = 0UL };
      /* Record that BAM supplied usable endpoints.  The tile clears
         bam_contact_dirty after it republishes, so reconnects without new
         config continue advertising the last known override. */
      ctx->bam_contact_avail = 1U;
    } else {
      /* BAM withdrew its TPU override; fall back to Firedancer defaults and
         prompt gossip to restore the original contact info. */
      if( FD_UNLIKELY( ctx->bam_contact_avail ) ) {
        ctx->bam_contact_dirty = 1U;
      }
      ctx->bam_contact_avail = 0U;
      ctx->bam_tpu_addr.l      = 0UL;
      ctx->bam_tpu_quic_addr.l = 0UL;
    }
  }
}

static void
fd_bam_try_start_stream( fd_bam_tile_t * ctx ) {
  if( FD_UNLIKELY( !ctx->bam_auth_ready ) ) return;
  if( FD_UNLIKELY( ctx->bam_stream || ctx->bam_stream_connecting ) ) return;
  if( FD_UNLIKELY( !ctx->grpc_client ) ) return;
  if( FD_UNLIKELY( fd_grpc_client_request_is_blocked( ctx->grpc_client ) ) ) return;

  bam_types_AuthProof proof = bam_types_AuthProof_init_default;
  fd_memset( proof.challenge_to_sign, 0, sizeof(proof.challenge_to_sign) );
  fd_memcpy( proof.challenge_to_sign,
             ctx->bam_auth_challenge,
             fd_ulong_min( (ulong)ctx->bam_auth_challenge_len, sizeof(proof.challenge_to_sign)-1UL ) );
  fd_memset( proof.validator_pubkey, 0, sizeof(proof.validator_pubkey) );
  fd_memcpy( proof.validator_pubkey,
             ctx->bam_validator_pubkey,
             fd_ulong_min( (ulong)strlen( ctx->bam_validator_pubkey ), sizeof(proof.validator_pubkey)-1UL ) );
  fd_memset( proof.signature, 0, sizeof(proof.signature) );
  fd_memcpy( proof.signature,
             ctx->bam_auth_signature,
             fd_ulong_min( (ulong)strlen( ctx->bam_auth_signature ), sizeof(proof.signature)-1UL ) );

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
  hb.time_sent_microseconds = (uint64_t)( fd_ulong_if( now>=0L, (ulong)now, 0UL ) / 1000UL );
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
  atomic_res.seq_id = (uint32_t)res->bundle_id;

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

  return fd_grpc_client_stream_send( ctx->grpc_client, ctx->bam_stream, &bam_api_SchedulerMessage_msg, &msg, 0 );
}

static int
fd_bam_flush_results( fd_bam_tile_t * ctx ) {
  int busy = 0;
  while( ctx->bam_pending_results ) {
    fd_bam_bundle_result_t const * res =
        &ctx->bam_results[ ctx->bam_results_head ];
    if( FD_UNLIKELY( !fd_bam_send_result( ctx, res ) ) ) break;
    ctx->bam_results_head = ( ctx->bam_results_head + 1UL ) % FD_BAM_MAX_PENDING_RESULTS;
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

static void
fd_bam_handle_scheduler_response( fd_bam_tile_t * ctx,
                                   pb_istream_t *   istream ) {
  bam_api_SchedulerResponse resp = bam_api_SchedulerResponse_init_default;
  resp.versioned_msg.v0.resp.multiple_atomic_txn_batch.batches = (pb_callback_t){
    .funcs.decode = fd_bam_visit_batches,
    .arg          = ctx
  };

  if( FD_UNLIKELY( !pb_decode( istream, &bam_api_SchedulerResponse_msg, &resp ) ) ) {
    ctx->metrics.decode_fail_cnt++;
    FD_LOG_WARNING(( "Protobuf decode of (bam_api.SchedulerResponse) failed" ));
    return;
  }

  if( FD_UNLIKELY( resp.which_versioned_msg != bam_api_SchedulerResponse_v0_tag ) ) {
    FD_LOG_WARNING(( "Unsupported SchedulerResponse version (tag=%u); scheduling reset", (unsigned)resp.which_versioned_msg ));
    ctx->metrics.transport_fail_cnt++;
    ctx->defer_reset = 1;
    return;
  }

  bam_api_SchedulerResponseV0 * v0 = &resp.versioned_msg.v0;
  switch( v0->which_resp ) {
  case bam_api_SchedulerResponseV0_heart_beat_tag:
    ctx->bam_last_builder_heartbeat_ns = fd_bam_now();
    fd_bam_client_sample_heartbeat_delay( ctx, v0->resp.heart_beat.time_sent_microseconds );
    ctx->metrics.heartbeat_recv_cnt++;
    break;
  case bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag:
    ctx->bam_last_builder_heartbeat_ns = fd_bam_now();
    break;
  default:
    break;
  }
}

static int
fd_bam_drive( fd_bam_tile_t * ctx,
              long             now ) {
  int busy = 0;
  if( FD_UNLIKELY( !ctx->grpc_client ) ) return busy;
  if( FD_UNLIKELY( !fd_grpc_client_is_connected( ctx->grpc_client ) ) ) return busy;

  if( FD_UNLIKELY( ctx->builder_info_avail ) ) {
    long const valid_until = ctx->builder_info_valid_until;
    if( FD_UNLIKELY( valid_until && now >= valid_until ) ) {
      ctx->builder_info_avail      = 0;
      ctx->bam_last_config_poll_ns = 0L;
    }
  }

  if( FD_UNLIKELY( !ctx->bam_auth_ready && !ctx->bam_auth_inflight && !ctx->bam_stream ) ) {
    fd_bam_request_auth_challenge( ctx );
    busy = 1;
  }

  if( FD_LIKELY( ctx->bam_auth_ready ) ) {
    fd_bam_try_start_stream( ctx );
  }

  if( FD_UNLIKELY( !ctx->bam_config_inflight ) &&
      ( ctx->bam_last_config_poll_ns==0L || now - ctx->bam_last_config_poll_ns >= (long)1e9 ) ) {
    fd_bam_request_config( ctx, now );
    busy = 1;
  }

  long const heartbeat_ns = (long)5e9;
  if( FD_LIKELY( ctx->bam_stream && ctx->bam_stream_live ) ) {
    if( FD_UNLIKELY( ( ctx->bam_last_validator_heartbeat_ns==0L ) ||
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

  if( FD_UNLIKELY( !FD_VOLATILE_CONST( ctx->runtime_enabled ) ) ) {
    /* Admin can pause BAM without tearing the tile down; skip reconnect/IO until re-enabled. */
    return;
  }

  if( FD_UNLIKELY( ctx->defer_reset ) ) {
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
    int poll_res = poll( pfds, 1, 0 );
    if( FD_UNLIKELY( poll_res<0 ) ) {
      FD_LOG_ERR(( "poll(tcp_sock) failed (%i-%s)", errno, fd_io_strerror( errno ) ));
    }
    if( poll_res==0 ) return;

    if( pfds[0].revents & (POLLERR|POLLHUP) ) {
      int connect_err = fd_bam_client_do_connect( ctx, 0U );
      FD_LOG_INFO(( "BAM gRPC connect attempt failed (%i-%s)", connect_err, fd_io_strerror( connect_err ) ));
      fd_bam_client_reset( ctx );
      ctx->metrics.transport_fail_cnt++;
      *charge_busy = 1;
      return;
    }
    if( pfds[0].revents & POLLOUT ) {
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
    FD_LOG_WARNING(( "BAM gRPC timed out (HTTP/2 PING went unanswered for %.2f seconds)",
                     (double)( check_ts - ctx->keepalive->ts_last_tx )/1e9 ));
    ctx->keepalive->inflight = 0;
    fd_bam_client_reset( ctx );
    ctx->metrics.transport_fail_cnt++;
    *charge_busy = 1;
    return;
  }

  /* Did BAM heartbeat time out */
  if( FD_UNLIKELY( ctx->bam_stream_live &&
                   ctx->bam_last_builder_heartbeat_ns != 0L &&
                   check_ts - ctx->bam_last_builder_heartbeat_ns >= FD_BAM_HEARTBEAT_TIMEOUT_NS ) ) {
    FD_LOG_WARNING(( "BAM heartbeat timed out (no heartbeat for %.2f seconds)",
                     (double)( check_ts - ctx->bam_last_builder_heartbeat_ns )/1e9 ));
    ctx->defer_reset = 1;
    *charge_busy = 1;
    return;
  }

  /* Drive I/O, SSL handshake, and any inflight requests */
  if( FD_UNLIKELY( !fd_bam_client_drive_io( ctx, charge_busy ) ||
                   ctx->defer_reset /* new error? */ ) ) {
    fd_bam_client_reset( ctx );
    ctx->metrics.transport_fail_cnt++;
    *charge_busy = 1;
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

  int const connected_now    = ( status==FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTED );
  int const connected_before = ( ctx->bundle_status_logged==FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTED );

  if( FD_UNLIKELY( connected_now!=connected_before ) ) {
    long ts = fd_log_wallclock();
    if( FD_LIKELY( ts-(ctx->last_bundle_status_log_nanos) >= (long)1e6 ) ) {
      if( connected_now ) {
        FD_LOG_NOTICE(( "Connected to BAM server" ));
      } else {
        FD_LOG_WARNING(( "Disconnected from BAM server" ));
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
  FD_LOG_INFO(( "BAM gRPC connection closed %s (%u-%s)",
                closed_by ? "by peer" : "due to error",
                h2_err, fd_h2_strerror( h2_err ) ));
  ctx->defer_reset = 1;
}

/* Forwards a bundle transaction to the tango message bus. */

static void
fd_bam_tile_publish_bundle_txn(
    fd_bam_tile_t * ctx,
    void const *       txn,
    ulong              txn_sz,  /* <=FD_TXN_MTU */
    ulong              bundle_txn_cnt,
    uint               source_ipv4
) {
  if( FD_UNLIKELY( !ctx->builder_info_avail ) ) {
    ctx->metrics.missing_builder_info_fail_cnt++; /* unreachable */
    return;
  }

  fd_txn_m_t * txnm = fd_chunk_to_laddr( ctx->verify_out.mem, ctx->verify_out.chunk );
  *txnm = (fd_txn_m_t) {
    .reference_slot = 0UL,
    .payload_sz     = (ushort)txn_sz,
    .txn_t_sz       = 0U,
    .source_ipv4    = source_ipv4,
    .source_tpu     = FD_TXN_M_TPU_SOURCE_BUNDLE,
    .block_engine   = {
      .bundle_id         = ctx->bundle_seq,
      .bundle_txn_cnt    = bundle_txn_cnt,
      .commission        = (uchar)ctx->builder_commission,
      .max_schedule_slot = ctx->bundle_max_schedule_slot
    },
  };
  memcpy( txnm->block_engine.commission_pubkey, ctx->builder_pubkey, 32UL );
  fd_memcpy( fd_txn_m_payload( txnm ), txn, txn_sz );

  ulong sz  = fd_txn_m_realized_footprint( txnm, 0, 0 );
  ulong sig = 1UL;

  if( FD_UNLIKELY( !ctx->stem ) ) {
    FD_LOG_CRIT(( "ctx->stem not set. This is a bug." ));
  }

  ulong tspub = (ulong)fd_frag_meta_ts_comp( fd_bam_now() );
  fd_stem_publish( ctx->stem, ctx->verify_out.idx, sig, ctx->verify_out.chunk, sz, 0UL, 0UL, tspub );
  ctx->verify_out.chunk = fd_dcache_compact_next( ctx->verify_out.chunk, sz, ctx->verify_out.chunk0, ctx->verify_out.wmark );
  ctx->metrics.txn_received_cnt++;
}

/* Forwards a regular transaction to the tango message bus. */

static void
fd_bam_tile_publish_txn(
    fd_bam_tile_t * ctx,
    void const *       txn,
    ulong              txn_sz,  /* <=FD_TXN_MTU */
    uint               source_ipv4
) {
  fd_txn_m_t * txnm = fd_chunk_to_laddr( ctx->verify_out.mem, ctx->verify_out.chunk );
  *txnm = (fd_txn_m_t) {
    .reference_slot = 0UL,
    .payload_sz     = (ushort)txn_sz,
    .txn_t_sz       = 0U,
    .source_ipv4    = source_ipv4,
    .source_tpu     = FD_TXN_M_TPU_SOURCE_BUNDLE,
    .block_engine   = {
      .bundle_id         = 0UL,
      .bundle_txn_cnt    = 1UL,
      .commission        = 0U,
      .commission_pubkey = {0U},
      .max_schedule_slot = 0UL
    },
  };
  fd_memcpy( fd_txn_m_payload( txnm ), txn, txn_sz );

  ulong sz  = fd_txn_m_realized_footprint( txnm, 0, 0 );
  ulong sig = 0UL;

  if( FD_UNLIKELY( !ctx->stem ) ) {
    FD_LOG_CRIT(( "ctx->stem not set. This is a bug." ));
  }

  ulong tspub = (ulong)fd_frag_meta_ts_comp( fd_bam_now() );
  fd_stem_publish( ctx->stem, ctx->verify_out.idx, sig, ctx->verify_out.chunk, sz, 0UL, 0UL, tspub );
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
  case FD_BAM_CLIENT_REQ_BAM_GetConfig:
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
  case FD_BAM_CLIENT_REQ_BAM_GetConfig:
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
  if( FD_UNLIKELY( resp->h2_status!=200 ) ) {
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
  case FD_BAM_CLIENT_REQ_BAM_GetConfig:
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

  if( FD_UNLIKELY( resp->grpc_status!=FD_GRPC_STATUS_OK ) ) {
    FD_LOG_INFO(( "gRPC request failed (gRPC status %u-%s): %.*s",
                  resp->grpc_status, fd_grpc_status_cstr( resp->grpc_status ),
                  (int)resp->grpc_msg_len, resp->grpc_msg ));
    fd_bam_client_request_failed( ctx, request_ctx );
    if( resp->grpc_status==FD_GRPC_STATUS_UNAUTHENTICATED ||
        resp->grpc_status==FD_GRPC_STATUS_PERMISSION_DENIED ) {
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
  ctx->defer_reset = 1;
  switch( request_ctx ) {
  case FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge:
    ctx->bam_auth_inflight       = 0;
    ctx->bam_auth_ready          = 0;
    ctx->bam_auth_challenge_len  = 0;
    break;
  case FD_BAM_CLIENT_REQ_BAM_GetConfig:
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
#define DISCONNECTED FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_DISCONNECTED
#define CONNECTING   FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTING
#define CONNECTED    FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTED

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
  case FD_BAM_CLIENT_REQ_BAM_GetConfig:
    return "BamGetBuilderConfig";
  case FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream:
    return "BamInitSchedulerStream";
  default:
    return "unknown";
  }
}
