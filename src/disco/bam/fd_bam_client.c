/* fd_bam_client.c steps gRPC related tasks. */

#define _GNU_SOURCE /* SOL_TCP */
#include "fd_bam_tile_private.h"
#include "fd_bam_tile.h"
#include "../keyguard/fd_keyguard.h"
#include "../../waltz/h2/fd_h2_conn.h"
#include "../../waltz/http/fd_url.h" /* fd_url_unescape */
#include "../../waltz/openssl/fd_openssl.h" /* fd_openssl_bio_new_socket */
#include "../../ballet/base58/fd_base58.h"
#include "../../ballet/nanopb/pb_decode.h"
#include "../../ballet/nanopb/pb_encode.h"
#include "../../util/fd_util.h"

#include <fcntl.h>
#include <errno.h>
#include <limits.h>
#include <unistd.h> /* close */
#include <poll.h> /* poll */
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h> /* snprintf */

__attribute__((weak)) long
fd_bam_now( void ) {
  return fd_log_wallclock();
}

void
fd_bam_tile_backoff( fd_bam_tile_t * ctx,
                     long            now ) {
  long wait_ns = (long)2e9;
  wait_ns = (long)( fd_rng_ulong( ctx->rng ) & ( (1UL<<fd_ulong_find_msb_w_default( (ulong)wait_ns, 0 ))-1UL ) );
  ctx->backoff_until = now + wait_ns;
}

static double
fd_bam_client_retry_ms( fd_bam_tile_t * ctx ) {
  long wait_ns = ctx->backoff_until - fd_bam_now();
  if( wait_ns < 0L ) wait_ns = 0L;
  return (double)wait_ns / 1e6;
}

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
fd_bam_clear_auth_state( fd_bam_tile_t * ctx ) {
  ctx->bam_auth_inflight      = 0;
  ctx->bam_auth_ready         = 0;
  ctx->challenge_to_sign[ 0 ] = '\0';
}

static int
fd_bam_parse_scheduler_leader_state_reject( char const * msg,
                                            uint         msg_len,
                                            ulong *      rejected_slot,
                                            ulong *      valid_min_slot,
                                            ulong *      valid_max_slot ) {
  char cstr[ 1009 ];
  ulong const len = fd_ulong_min( (ulong)msg_len, sizeof(cstr)-1UL );
  fd_memcpy( cstr, msg, len );
  cstr[ len ] = '\0';

  return 3==sscanf( cstr,
                    "Leader state slot %lu is outside of valid range %lu..=%lu",
                    rejected_slot,
                    valid_min_slot,
                    valid_max_slot );
}

static inline void
fd_bam_clear_scheduler_rejected_leader_state( fd_bam_tile_t * ctx,
                                              ulong           rejected_slot,
                                              ulong           valid_min_slot,
                                              ulong           valid_max_slot ) {
  if( FD_LIKELY( ctx->bam_leader_state.slot!=rejected_slot ) ) return;

  long now = fd_bam_now();
  fd_bam_note_leader_state_suppressed( ctx,
                                       &ctx->bam_leader_state,
                                       FD_BAM_LEADER_STATE_SUPPRESS_SCHEDULER_REJECTED,
                                       now );
  FD_LOG_WARNING(( "clearing retained BAM leader state slot=%lu after scheduler rejected valid_range=%lu..=%lu",
                   rejected_slot,
                   valid_min_slot,
                   valid_max_slot ));

  ctx->bam_leader_state = (fd_bam_leader_state_t){ .slot = ULONG_MAX };
  ctx->bam_leader_pending = 0U;
}

static inline void
fd_bam_drop_pending_leader_state( fd_bam_tile_t * ctx,
                                  uint            reason_idx ) {
  if( FD_LIKELY( !ctx->bam_leader_pending ) ) return;
  ctx->metrics.leader_pending_dropped_cnt[ reason_idx ]++;
  ctx->bam_leader_pending = 0U;
}

static inline void
fd_bam_clear_stream_state( fd_bam_tile_t * ctx,
                           uint            reason_idx ) {
  ctx->bam_stream            = NULL;
  fd_bam_set_stream_live( ctx, 0U );
  ctx->bam_stream_connecting = 0;
  fd_bam_drop_pending_leader_state( ctx, reason_idx );
}

void
fd_bam_client_reset( fd_bam_tile_t * ctx ) {
  long now = fd_bam_now();

  /* Publish BAM inactive immediately so downstream tiles can resume
     QUIC/bundle ingestion without waiting for housekeeping. */
  fd_bam_publish_active_state( ctx, ctx->stem, 0 );

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
  ctx->leader_schedule_gate_start_ns = 0L;
  ctx->leader_schedule_recheck_slot = FD_BAM_LEADER_SCHEDULE_RECHECK_NONE_SLOT;

  if( FD_UNLIKELY( ctx->builder_info_valid_until && now >= ctx->builder_info_valid_until ) ) {
    ctx->builder_info_valid_until = 0L;
  }
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
  fd_bam_clear_auth_state( ctx );
  ctx->bam_config_inflight        = 0;
  ctx->bam_config_received        = 0;
  ctx->bam_last_builder_activity_ns = 0L;
  ctx->bam_last_validator_heartbeat_ns = 0L;
  ctx->bam_last_config_poll_ns    = 0L;
  /* Preserve any buffered BAM results so they flush once the next
     scheduler stream comes up.  The server expects every dispatched
     atomic batch to eventually produce a result; dropping them here
     would lose that guarantee. */
  fd_bam_drop_pending_leader_state( ctx, FD_METRICS_ENUM_BAM_LEADER_PENDING_DROP_REASON_V_CLIENT_RESET_IDX );
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

  struct sockaddr_in addr = {
    .sin_family      = AF_INET,
    .sin_addr.s_addr = ip4_addr,
    .sin_port        = fd_ushort_bswap( ctx->server_tcp_port )
  };
  int connect_err = connect( ctx->tcp_sock, fd_type_pun_const( &addr ), sizeof(addr) );
  /* FD_LIKELY is used here as EINPROGRESS is expected even to local tcp ports */
  if( FD_LIKELY( connect_err==-1 ) ) connect_err = errno;
  if( FD_UNLIKELY( connect_err && connect_err != EINPROGRESS ) ) {
    fd_bam_client_reset( ctx );
    ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_CONNECT_IDX ]++;
    FD_LOG_WARNING(( "connect(tcp_sock," FD_IP4_ADDR_FMT ":%u) failed (%i-%s) (fqdn=%s sni=%.*s); retrying in %.3f ms",
                     FD_IP4_ADDR_FMT_ARGS( ip4_addr ), ctx->server_tcp_port,
                     connect_err, fd_io_strerror( connect_err ),
                     ctx->server_fqdn, (int)ctx->server_sni_len, ctx->server_sni,
                     fd_bam_client_retry_ms( ctx ) ));
    return;
  }

# if FD_HAS_OPENSSL
  if( ctx->is_ssl ) {
    BIO * bio = fd_openssl_bio_new_socket( ctx->tcp_sock, BIO_NOCLOSE );
    if( FD_UNLIKELY( !bio ) ) {
      FD_LOG_ERR(( "fd_openssl_bio_new_socket failed" ));
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
  fd_bam_bundle_result_t const * res = (fd_bam_bundle_result_t const *)*arg;
  if( FD_UNLIKELY( !res ) ) return false;
  for( uchar i=0U; i<res->bundle_txn_cnt; i++ ) {
    bam_types_TransactionCommittedResult txn_res = bam_types_TransactionCommittedResult_init_default;
    txn_res.cus_consumed               = res->consumed_cus[ i ];
    txn_res.feepayer_balance_lamports  = res->feepayer_balance_lamports[ i ];
    txn_res.loaded_accounts_data_size  = res->loaded_accounts_data_size[ i ];
    txn_res.execution_success          = ( res->sanitize_success[ i ] && !res->transaction_err[ i ] );
    if( FD_UNLIKELY( !pb_encode_tag_for_field( stream, field ) ) ) return false;
    if( FD_UNLIKELY( !pb_encode_submessage( stream, bam_types_TransactionCommittedResult_fields, &txn_res ) ) ) return false;
  }
  return true;
}

static bool
fd_bam_encode_batch_results_cb( pb_ostream_t *          stream,
                                pb_field_t const *       field,
                                void * const *           arg ) {
  bam_types_AtomicTxnBatchResult const * atomic_res = (bam_types_AtomicTxnBatchResult const *)*arg;
  if( FD_UNLIKELY( !atomic_res ) ) return false;
  if( FD_UNLIKELY( !pb_encode_tag_for_field( stream, field ) ) ) return false;
  return pb_encode_submessage( stream, bam_types_AtomicTxnBatchResult_fields, atomic_res );
}

static void
fd_bam_request_auth_challenge( fd_bam_tile_t * ctx ) {
  if( FD_UNLIKELY( fd_grpc_client_request_is_blocked( ctx->grpc_client ) ) ) return;

  bam_api_AuthChallengeRequest req = bam_api_AuthChallengeRequest_init_default;
  static char const path[] = "/bam_api.BamNodeApi/GetAuthChallenge";
  fd_grpc_h2_stream_t * request = fd_grpc_client_request_start(
      ctx->grpc_client,
      path, sizeof(path)-1,
      FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge,
      &bam_api_AuthChallengeRequest_msg, &req,
      NULL, 0,
      0
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
  fd_grpc_h2_stream_t * request = fd_grpc_client_request_start(
      ctx->grpc_client,
      path, sizeof(path)-1,
      FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig,
      &bam_api_ConfigRequest_msg, &req,
      NULL, 0,
      0
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
    fd_bam_clear_auth_state( ctx );
    FD_LOG_WARNING(( "Protobuf decode of (bam_api.AuthChallengeResponse) failed" ));
    return 0;
  }

  size_t challenge_len = strnlen( resp.challenge_to_sign, sizeof(resp.challenge_to_sign) );
  if( FD_UNLIKELY( !challenge_len ) ) {
    fd_bam_clear_auth_state( ctx );
    FD_LOG_WARNING(( "AuthChallengeResponse challenge is empty" ));
    return 0;
  }
  if( FD_UNLIKELY( challenge_len == sizeof(resp.challenge_to_sign) ) ) {
    fd_bam_clear_auth_state( ctx );
    FD_LOG_WARNING(( "AuthChallengeResponse challenge not NUL terminated" ));
    return 0;
  }

  ctx->bam_auth_inflight = 0;
  fd_memcpy( ctx->challenge_to_sign, resp.challenge_to_sign, sizeof(ctx->challenge_to_sign) );

  uchar  sign_payload[ FD_BAM_AUTH_LABEL_LEN + sizeof(resp.challenge_to_sign) ];
  fd_memcpy( sign_payload, FD_BAM_AUTH_LABEL, FD_BAM_AUTH_LABEL_LEN );
  fd_memcpy( sign_payload + FD_BAM_AUTH_LABEL_LEN, resp.challenge_to_sign, challenge_len );
  ulong sign_payload_sz = FD_BAM_AUTH_LABEL_LEN + challenge_len;

  ulong payload_mask = fd_keyguard_payload_match( sign_payload, sign_payload_sz,
                                                  FD_KEYGUARD_SIGN_TYPE_ED25519 );
  if( FD_UNLIKELY( payload_mask != FD_KEYGUARD_PAYLOAD_BAM_AUTH ) ) {
    fd_bam_clear_auth_state( ctx );
    FD_LOG_WARNING(( "AuthChallengeResponse challenge yields ambiguous or invalid BAM auth payload (challenge_len=%lu mask=%#lx)",
                     (ulong)challenge_len, payload_mask ));
    return 0;
  }

  uchar signature[ 64 ];
  fd_keyguard_client_sign( ctx->keyguard_client, signature, sign_payload, sign_payload_sz,
                           FD_KEYGUARD_SIGN_TYPE_ED25519 );

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
  fd_ip4_port_t new_tpu     = {0};
  fd_ip4_port_t new_tpu_fwd = {0};
  fd_ip4_port_t new_shred_sock[ FD_BAM_SHRED_SOCK_MAX ] = {0};
  uchar         new_shred_sock_cnt = 0U;

  if( cfg->has_tpu_sock ) {
    uint ip4;
    if( FD_LIKELY( fd_cstr_to_ip4_addr( cfg->tpu_sock.ip, &ip4 ) ) &&
        FD_LIKELY( cfg->tpu_sock.port > 0 && cfg->tpu_sock.port <= (uint)(USHRT_MAX-6U) ) ) {
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
        FD_LIKELY( cfg->tpu_fwd_sock.port > 0 && cfg->tpu_fwd_sock.port <= (uint)(USHRT_MAX-6U) ) ) {
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

  /* A disconnect means Firedancer should resume advertising its local
     TPU ports so TPU clients do not get stuck targeting the BAM host. */
  _Bool has_valid_new_contact = !!new_tpu.addr && !!new_tpu.port && !!new_tpu_fwd.addr && !!new_tpu_fwd.port;
  if( FD_LIKELY( has_valid_new_contact ) ) {
    if( FD_UNLIKELY( ctx->bam_tpu.l != new_tpu.l || ctx->bam_tpu_fwd.l != new_tpu_fwd.l ) )
      ctx->tpu_update_state = FD_BAM_TPU_UPDATE_STATE_UNKNOWN;
    ctx->bam_tpu     = new_tpu;
    ctx->bam_tpu_fwd = new_tpu_fwd;
  } else {
    FD_LOG_WARNING(( "Received incomplete or invalid TPU config; preserving prior BAM TPU config" ));
  }

  if( FD_UNLIKELY( ctx->bam_shred_sock_cnt != new_shred_sock_cnt ||
                   0!=memcmp( ctx->bam_shred_sock, new_shred_sock, new_shred_sock_cnt * sizeof(fd_ip4_port_t) ) ) ) {
    ctx->bam_shred_sock_cnt = new_shred_sock_cnt;
    fd_memcpy( ctx->bam_shred_sock, new_shred_sock, new_shred_sock_cnt * sizeof(fd_ip4_port_t) );
  }

  ctx->gui_dirty = 1U;
  /* Only publish BAM active when the client is actually healthy and has
     complete effective contact data. Config responses can arrive while
     connecting or after an admin disable, and incomplete refreshes should
     not force a BAM TPU override unless a prior valid contact is cached. */
  fd_plugin_bam_update_status_t status = fd_bam_client_status( ctx );
  fd_bam_publish_active_state( ctx,
                               ctx->stem,
                               status == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );

  _Bool bam_config_fee_updated = false;
  ushort new_commission_bps = (ushort)fd_uint_min( cfg->commission_bps, 10000U );
  if( FD_UNLIKELY( ctx->commission_bps != new_commission_bps ) ) {
    ctx->commission_bps = new_commission_bps;
    bam_config_fee_updated = true;
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
    fd_bam_fee_cfg_t * fee_cfg = ctx->fee_cfg;
    if( FD_LIKELY( fee_cfg ) ) {
      /* Publish to the pack tile via a seqlock: set the version's
         write-in-progress marker, store the fields, then commit the next
         counter value. A reader therefore never sees a torn
         prio_fee_recipient. x86-64/TSO, so compiler fences suffice. */
      FD_VOLATILE( fee_cfg->version ) = fd_uint_set_bit( ctx->fee_cfg_version, 31 );
      FD_COMPILER_MFENCE();
      fd_memcpy( fee_cfg->prio_fee_recipient, ctx->prio_fee_recipient, sizeof( fee_cfg->prio_fee_recipient ) );
      fee_cfg->commission_bps         = ctx->commission_bps;
      fee_cfg->has_prio_fee_recipient = ctx->prio_fee_recipient_set;
      FD_COMPILER_MFENCE();

      ctx->fee_cfg_version = fd_uint_clear_bit( ctx->fee_cfg_version + 1U, 31 );
      if( FD_UNLIKELY( !ctx->fee_cfg_version ) ) ctx->fee_cfg_version = 1U;
      FD_VOLATILE( fee_cfg->version ) = ctx->fee_cfg_version;
    }
  }
}

static void
fd_bam_try_start_stream( fd_bam_tile_t * ctx ) {
  if( FD_UNLIKELY( !ctx->bam_auth_ready ) ) return;
  if( FD_UNLIKELY( ctx->bam_stream || ctx->bam_stream_connecting ) ) return;
  if( FD_UNLIKELY( fd_grpc_client_request_is_blocked( ctx->grpc_client ) ) ) return;

  bam_types_AuthProof proof = bam_types_AuthProof_init_default;
  fd_memcpy( proof.challenge_to_sign, ctx->challenge_to_sign, sizeof(proof.challenge_to_sign) );
  fd_cstr_ncpy( proof.validator_pubkey, ctx->bam_identity_pubkey_b58, sizeof( proof.validator_pubkey ) );
  fd_cstr_ncpy( proof.signature, ctx->bam_auth_signature, sizeof( proof.signature ) );

  bam_api_SchedulerMessage msg = bam_api_SchedulerMessage_init_default;
  msg.which_versioned_msg               = bam_api_SchedulerMessage_v0_tag;
  msg.versioned_msg.v0.which_msg        = bam_api_SchedulerMessageV0_auth_proof_tag;
  msg.versioned_msg.v0.msg.auth_proof   = proof;

  static char const path[] = "/bam_api.BamNodeApi/InitSchedulerStream";
  fd_grpc_h2_stream_t * stream = fd_grpc_client_request_start(
      ctx->grpc_client,
      path, sizeof(path)-1,
      FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream,
      &bam_api_SchedulerMessage_msg, &msg,
      NULL, 0,
      1
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
  int send_res = fd_grpc_client_stream_send_msg( ctx->grpc_client, ctx->bam_stream, &bam_api_SchedulerMessage_msg, &msg );
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

  if( FD_LIKELY( res->execution_success ) ) {
    atomic_res.which_result = bam_types_AtomicTxnBatchResult_committed_tag;
    atomic_res.result.committed.transaction_results.funcs.encode = fd_bam_encode_committed_cb;
    atomic_res.result.committed.transaction_results.arg          = (void *)res;
  } else {
    atomic_res.which_result = bam_types_AtomicTxnBatchResult_not_committed_tag;
    bam_types_NotCommitted * out = &atomic_res.result.not_committed;
    *out = (bam_types_NotCommitted)bam_types_NotCommitted_init_default;

    switch( res->bundle_err ) {
    case FD_BAM_BUNDLE_ERR_NONE:
      break;
    case FD_BAM_BUNDLE_ERR_DESER:
      out->which_reason                        = bam_types_NotCommitted_deserialization_error_tag;
      out->reason.deserialization_error.index  = res->deser_index;
      out->reason.deserialization_error.reason = (bam_types_DeserializationErrorReason)res->deser_reason;
      break;
    }

    if( FD_UNLIKELY( !out->which_reason && res->scheduling_error != FD_BAM_SCHED_ERR_NONE ) ) {
      out->which_reason            = bam_types_NotCommitted_scheduling_error_tag;
      out->reason.scheduling_error = (bam_types_SchedulingError)res->scheduling_error;
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
      } else {
        out->which_reason                    = bam_types_NotCommitted_transaction_error_tag;
        out->reason.transaction_error.index  = err_idx;
        out->reason.transaction_error.reason = (bam_types_TransactionErrorReason)res->transaction_err[ err_idx ];
      }
    }

    if( FD_UNLIKELY( !out->which_reason ) ) {
      out->which_reason = bam_types_NotCommitted_generic_invalid_tag;
      fd_cstr_ncpy( out->reason.generic_invalid.message,
                    FD_BAM_ERR_MSG_BUNDLE_EXECUTION_FAILED,
                    sizeof( out->reason.generic_invalid.message ) );
    }
  }

  bam_types_MultipleAtomicTxnBatchResult multi = bam_types_MultipleAtomicTxnBatchResult_init_default;
  multi.results.funcs.encode = fd_bam_encode_batch_results_cb;
  multi.results.arg          = &atomic_res;

  bam_api_SchedulerMessage msg = bam_api_SchedulerMessage_init_default;
  msg.which_versioned_msg                        = bam_api_SchedulerMessage_v0_tag;
  msg.versioned_msg.v0.which_msg                 = bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag;
  msg.versioned_msg.v0.msg.multiple_atomic_txn_batch_result = multi;

  int send_res = fd_grpc_client_stream_send_msg( ctx->grpc_client, ctx->bam_stream, &bam_api_SchedulerMessage_msg, &msg );
  ctx->metrics.outbound_enqueue_outcome_cnt[
      send_res
      ? FD_METRICS_ENUM_BAM_ENQUEUE_OUTCOME_V_RESULT_ENQUEUED_IDX
      : FD_METRICS_ENUM_BAM_ENQUEUE_OUTCOME_V_RESULT_ENQUEUE_FAIL_IDX ]++;
  return send_res;
}

int
fd_bam_send_leader_state( fd_bam_tile_t *                ctx,
                          fd_bam_leader_state_t const *  state ) {
  long now = fd_bam_now();
  fd_bam_leader_state_suppress_reason_t reason;
  if( FD_UNLIKELY( fd_bam_leader_state_suppress_reason( ctx, state, now, 0, &reason ) ) ) {
    fd_bam_note_leader_state_suppressed( ctx, state, reason, now );
    ctx->bam_leader_pending = 0U;
    if( FD_UNLIKELY( reason==FD_BAM_LEADER_STATE_SUPPRESS_NOT_LEADER ||
                     reason==FD_BAM_LEADER_STATE_SUPPRESS_EXPIRED ) ) {
      ctx->bam_leader_state = (fd_bam_leader_state_t){ .slot = ULONG_MAX };
    }
    return 0;
  }

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

  int send_res = fd_grpc_client_stream_send_msg( ctx->grpc_client, ctx->bam_stream, &bam_api_SchedulerMessage_msg, &msg );
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

  /* Poll config on a throttle to keep stream settings fresh.  The
     builder-info TTL only covers block-engine builder metadata; missing BAM
     config must force polling independently because it gates healthy status. */
  _Bool const missing_bam_config    = !ctx->bam_config_received;
  _Bool const missing_builder_info  = !ctx->builder_info_valid_until;
  _Bool const builder_info_expiring = ctx->builder_info_valid_until &&
                                      now + (long)5e9 >= ctx->builder_info_valid_until;
  _Bool const refresh_needed = missing_bam_config ||
                               missing_builder_info ||
                               builder_info_expiring;
  long const throttle_ns = ( missing_bam_config || missing_builder_info ) ? (long)1e9 : (long)5e9;
  _Bool const poll_due = !ctx->bam_last_config_poll_ns ||
                         now - ctx->bam_last_config_poll_ns >= throttle_ns;
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

  /* Push leader state updates while the scheduler stream is live. */
  if( FD_UNLIKELY( ctx->bam_leader_pending &&
                   ctx->bam_stream &&
                   ctx->bam_stream_live ) ) {
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
    ctx->leader_schedule_gate_start_ns = 0L;
    ctx->leader_schedule_recheck_slot = FD_BAM_LEADER_SCHEDULE_RECHECK_NONE_SLOT;
    return;
  }

  if( FD_UNLIKELY( ( !ctx->server_fqdn_len || !ctx->server_tcp_port ) &&
                   !ctx->tcp_sock_connected &&
                   ctx->tcp_sock<0 ) ) {
    /* set-bam --enable may arrive before any URL is configured.  With no
       endpoint and no existing socket to service, stay idle; a later non-empty
       URL update will populate server_fqdn/server_tcp_port and allow dialing. */
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

  int leader_schedule_gate_active = fd_bam_tile_leader_schedule_gate_active( ctx );
  if( FD_LIKELY( !leader_schedule_gate_active ) ) {
    ctx->leader_schedule_gate_start_ns = 0L;
  } else if( FD_UNLIKELY( ctx->leader_schedule_recheck_slot!=FD_BAM_LEADER_SCHEDULE_RECHECK_DUE_SLOT ) ) {
    long gate_now = fd_bam_now();
    if( FD_UNLIKELY( !ctx->leader_schedule_gate_start_ns ) ) ctx->leader_schedule_gate_start_ns = gate_now;
    if( FD_LIKELY( gate_now - ctx->leader_schedule_gate_start_ns < FD_BAM_LEADER_SCHEDULE_RECHECK_WALLCLOCK_NS ) ) return;
    ctx->leader_schedule_recheck_slot = FD_BAM_LEADER_SCHEDULE_RECHECK_DUE_SLOT;
    ctx->leader_schedule_gate_start_ns = 0L;
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

    short const revents = pfds[0].revents;
    int so_err = 0;
    socklen_t so_err_sz = sizeof(so_err);
    int connect_result = getsockopt( ctx->tcp_sock, SOL_SOCKET, SO_ERROR, &so_err, &so_err_sz )
                         ? errno
                         : so_err;
    if( FD_UNLIKELY( ( revents & (POLLERR|POLLHUP|POLLNVAL) ) || connect_result ) ) {
      if( FD_UNLIKELY( !connect_result ) ) connect_result = ECONNABORTED;
      FD_LOG_WARNING(( "BAM gRPC connect attempt failed (%i-%s) while dialing %s/" FD_IP4_ADDR_FMT ":%hu; retrying in %.3f ms",
        connect_result, fd_io_strerror( connect_result ),
        ctx->server_fqdn,
        FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
        ctx->server_tcp_port,
        fd_bam_client_retry_ms( ctx ) ));
      fd_bam_client_reset( ctx );
      ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_CONNECT_IDX ]++;
      *charge_busy = 1;
      return;
    }
    if( FD_LIKELY( revents & POLLOUT ) ) {
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
    if( FD_UNLIKELY( leader_schedule_gate_active &&
                     ctx->leader_schedule_recheck_slot==FD_BAM_LEADER_SCHEDULE_RECHECK_DUE_SLOT ) ) {
      ctx->leader_schedule_recheck_slot = FD_BAM_LEADER_SCHEDULE_RECHECK_NONE_SLOT;
      ctx->leader_schedule_gate_start_ns = 0L;
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
  if( FD_UNLIKELY( -1==fd_bam_client_drive_io( ctx, charge_busy ) ) ) {
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
  _Bool const healthy_now    = ( status == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
  _Bool const healthy_before = ( ctx->bam_status_counted == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
  if( FD_UNLIKELY( status != ctx->bam_status_counted ) ) {
    long ts_ns = fd_bam_now();
    for( ulong i=0UL; i<FD_BAM_LEADER_SLOT_END_TRACKER_CNT; i++ ) {
      fd_bam_leader_slot_end_tracker_t * tracker = &ctx->leader_slot_end[ i ];
      if( FD_UNLIKELY( !tracker->valid || tracker->counted || ts_ns>tracker->slot_end_ns ) ) continue;
      tracker->healthy_at_end = healthy_now;
    }
  }
  if( FD_UNLIKELY( healthy_now != healthy_before ) ) {
    if( healthy_now ) ctx->metrics.healthy_connects_cnt++;
    else              ctx->metrics.healthy_disconnects_cnt++;
  }
  ctx->bam_status_counted = status;
  if( FD_UNLIKELY( status!=ctx->bam_status_logged ) ) {
    long ts = fd_log_wallclock();
    if( FD_LIKELY( ts-(ctx->last_bam_status_log_nanos) >= (long)1e6 ) ) {
      if( FD_UNLIKELY( status==FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED ) ) {
        FD_LOG_WARNING(( "Disconnected from BAM node" ));
      } else if( FD_UNLIKELY( status==FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTING ) ) {
        FD_LOG_INFO(( "BAM connection state CONNECTING" ));
      } else if( FD_UNLIKELY( status==FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY ) ) {
        FD_LOG_INFO(( "BAM connection state CONNECTED_UNHEALTHY" ));
      } else if( FD_UNLIKELY( status==FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY ) ) {
        char const * scheme = "http";
# if FD_HAS_OPENSSL
        if( ctx->is_ssl ) scheme = "https";
# endif
        FD_LOG_NOTICE(( "Connected to BAM node at %s://%s/ (" FD_IP4_ADDR_FMT ":%hu)",
                        scheme,
                        ctx->server_fqdn,
                        FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
                        ctx->server_tcp_port ));
      }

      ctx->last_bam_status_log_nanos = ts;
      ctx->bam_status_logged         = status;
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
    fd_bam_leader_state_suppress_reason_t reason;
    if( ctx->bam_leader_state.slot != ULONG_MAX &&
        fd_bam_leader_state_suppress_reason( ctx, &ctx->bam_leader_state, now, 0, &reason ) ) {
      fd_bam_note_leader_state_suppressed( ctx, &ctx->bam_leader_state, reason, now );
      ctx->bam_leader_state = (fd_bam_leader_state_t){ .slot = ULONG_MAX };
      ctx->bam_leader_pending = 0U;
    } else if( ctx->bam_leader_state.slot != ULONG_MAX &&
               ctx->bam_leader_state.slot_end_ns &&
               fd_long_sat_add( ctx->bam_leader_state.slot_end_ns, FD_BAM_LEADER_STATE_EXPIRY_GRACE_NS ) > now ) {
      ctx->bam_leader_pending = 1U;
    } else {
      ctx->bam_leader_pending = 0U;
    }
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
  case FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig: {
    FD_LOG_INFO(( "GetBuilderConfig response received in %.3f ms",
      (double)(rx_ts_ns - ctx->bam_last_config_poll_ns) / 1e6 ));
    fd_bam_handle_config( ctx, protobuf, protobuf_sz );
    ctx->bam_config_inflight = 0;
    break;
  }
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
    fd_bam_clear_auth_state( ctx );
    break;
  case FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig:
    ctx->bam_config_inflight = 0;
    break;
  case FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream:
    fd_bam_clear_stream_state( ctx, FD_METRICS_ENUM_BAM_LEADER_PENDING_DROP_REASON_V_REQUEST_FAILED_IDX );
    ctx->bam_auth_ready = 0;
    ctx->challenge_to_sign[ 0 ] = '\0';
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

  if( FD_UNLIKELY( resp->grpc_status != FD_GRPC_STATUS_OK ) ) {
    FD_LOG_WARNING(( "gRPC request %s failed (gRPC status %u-%s): %.*s",
                     fd_bam_request_ctx_cstr( request_ctx ),
                     resp->grpc_status, fd_grpc_status_cstr( resp->grpc_status ),
                     (int)resp->grpc_msg_len, resp->grpc_msg ));

    ulong rejected_slot;
    ulong valid_min_slot;
    ulong valid_max_slot;
    if( FD_UNLIKELY( request_ctx==FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream &&
                     resp->grpc_status==FD_GRPC_STATUS_INVALID_ARGUMENT &&
                     fd_bam_parse_scheduler_leader_state_reject( resp->grpc_msg,
                                                                 resp->grpc_msg_len,
                                                                 &rejected_slot,
                                                                 &valid_min_slot,
                                                                 &valid_max_slot ) ) ) {
      fd_bam_clear_scheduler_rejected_leader_state( ctx, rejected_slot, valid_min_slot, valid_max_slot );
    }

    fd_bam_client_request_failed( ctx, request_ctx );
    if( resp->grpc_status == FD_GRPC_STATUS_UNAUTHENTICATED ||
        resp->grpc_status == FD_GRPC_STATUS_PERMISSION_DENIED ) {
      ctx->bam_auth_ready         = 0;
      ctx->challenge_to_sign[ 0 ] = '\0';
    }
    if( request_ctx==FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream ) {
      ctx->defer_reset = 1;
      FD_LOG_INFO(( "BAM scheduler stream failed (gRPC status %u-%s). Reconnecting ...",
                    resp->grpc_status, fd_grpc_status_cstr( resp->grpc_status ) ));
    }
    return;
  }

  switch( request_ctx ) {
  case FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge:
    ctx->bam_auth_inflight = 0;
    break;
  case FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig:
    ctx->bam_config_inflight = 0;
    break;
  case FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream:
    fd_bam_clear_stream_state( ctx, FD_METRICS_ENUM_BAM_LEADER_PENDING_DROP_REASON_V_STREAM_ENDED_IDX );
    fd_bam_tile_backoff( ctx, fd_bam_now() );
    ctx->defer_reset = 1;
    FD_LOG_INFO(( "BAM scheduler stream ended cleanly. Reconnecting ..." ));
    break;
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
    fd_bam_clear_auth_state( ctx );
    break;
  case FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig:
    ctx->bam_config_inflight = 0;
    break;
  case FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream:
    fd_bam_clear_stream_state( ctx, FD_METRICS_ENUM_BAM_LEADER_PENDING_DROP_REASON_V_STREAM_TIMEOUT_IDX );
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
     HTTP/2 session, BAM auth, scheduler stream, and keepalive) is
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
    return FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTING; /* scheduler stream not live yet */
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
  if( FD_UNLIKELY( !fd_bam_has_effective_contact( ctx ) ) ) {
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
