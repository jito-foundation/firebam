/* fd_bam_client.c steps gRPC related tasks. */

#define _GNU_SOURCE /* SOL_TCP */
#include "fd_bam_tile_private.h"
#include "../keyguard/fd_keyguard.h"
#include "../../waltz/h2/fd_h2_conn.h"
#include "../../waltz/http/fd_url.h" /* fd_url_unescape */
#include "../../ballet/base58/fd_base58.h"
#include "../../ballet/ed25519/fd_ed25519.h"
#include "../../ballet/ed25519/fd_x25519.h"
#include "../waker/fd_waker.h"
#include "../../third_party/nanopb/pb_decode.h"
#include "../../third_party/nanopb/pb_encode.h"

#include <fcntl.h>
#include <errno.h>
#include <limits.h>
#include <unistd.h> /* close */
#include <poll.h> /* poll */
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h> /* sscanf */
#include <sys/epoll.h>

__attribute__((weak)) long
fd_bam_now( void ) {
  return fd_log_wallclock();
}

/* The stem calls next_deadline after a client step that reported no work.
   A due obligation may remain unsatisfied while HTTP/2 is blocked on
   transport or stream capacity.  Poll at the stem park cap until socket
   readiness changes instead of keeping the tile continuously runnable. */
#define FD_BAM_CLIENT_NO_PROGRESS_RETRY_NS ((long)20e6)

static inline long
fd_bam_client_no_progress_deadline( long deadline,
                                     long now ) {
  return FD_UNLIKELY( deadline<=now )
       ? fd_long_sat_add( now, FD_BAM_CLIENT_NO_PROGRESS_RETRY_NS )
       : deadline;
}

static fd_plugin_bam_update_status_t
fd_bam_client_status_at( fd_bam_tile_t const * ctx,
                         long                  now );

FD_FN_CONST static char const *
fd_bam_request_ctx_cstr( ulong request_ctx ) {
  switch( request_ctx ) {
  case FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge:      return "BamGetAuthChallenge";
  case FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig:      return "BamGetBuilderConfig";
  case FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream:   return "BamInitSchedulerStream";
  default:                                          return "unknown";
  }
}

/* fd_bam_tile_backoff is called whenever an error occurs.  Stalls
   forward progress for a random time below 2^30 ns (~1.07 s) to prevent
   error floods. */

static void
fd_bam_tile_backoff( fd_bam_tile_t * ctx,
                     long            now ) {
  ctx->backoff_until = now + (long)( fd_rng_ulong( ctx->rng ) & ((1UL<<30)-1UL) );
}

/* The BAM node answers the scheduler stream with HTTP 200 before it
   checks the auth proof, so a live stream counts as accepted only from
   the node's first BuilderHeartBeat (counted DOWN_TO_LIVE on decode).
   Only an accepted stream counts LIVE_TO_DOWN. */
static inline void
fd_bam_set_stream_live( fd_bam_tile_t * ctx,
                        _Bool           live ) {
  if( FD_UNLIKELY( !live && ctx->bam_stream_live && ctx->bam_builder_heartbeat_received ) ) {
    ctx->metrics.stream_transition_cnt[ FD_METRICS_ENUM_BAM_STREAM_TRANSITION_V_LIVE_TO_DOWN_IDX ]++;
  }
  ctx->bam_stream_live = live;
}

static inline void
fd_bam_clear_auth_state( fd_bam_tile_t * ctx ) {
  ctx->bam_auth_inflight      = 0;
  ctx->bam_auth_ready         = 0;
  ctx->challenge_to_sign[ 0 ] = '\0';
}

/* fd_bam_handle_leader_state_reject clears the retained leader state if
   the scheduler stream's INVALID_ARGUMENT status rejected its slot. */

static void
fd_bam_handle_leader_state_reject( fd_bam_tile_t *             ctx,
                                   fd_grpc_resp_hdrs_t const * resp ) {
  char cstr[ sizeof(resp->grpc_msg)+1UL ];
  ulong const len = fd_ulong_min( (ulong)resp->grpc_msg_len, sizeof(cstr)-1UL );
  fd_memcpy( cstr, resp->grpc_msg, len );
  cstr[ len ] = '\0';

  ulong rejected_slot, valid_min_slot, valid_max_slot;
  if( FD_LIKELY( 3!=sscanf( cstr, "Leader state slot %lu is outside of valid range %lu..=%lu",
                            &rejected_slot, &valid_min_slot, &valid_max_slot ) ||
                 ctx->bam_leader_state.slot!=rejected_slot ) ) return;

  fd_bam_note_leader_state_suppressed( ctx,
                                       &ctx->bam_leader_state,
                                       FD_METRICS_ENUM_BAM_LEADER_STATE_SUPPRESS_REASON_V_SCHEDULER_REJECTED_IDX,
                                       fd_bam_now() );
  FD_LOG_WARNING(( "clearing retained BAM leader state slot=%lu after scheduler rejected valid_range=%lu..=%lu",
                   rejected_slot, valid_min_slot, valid_max_slot ));
  ctx->bam_leader_state = (fd_bam_leader_state_t){ .slot = ULONG_MAX };
  ctx->bam_leader_pending = 0U;
}

/* fd_bam_clear_stream_state forgets the scheduler stream and drops a
   pending leader state, counted under drop_reason_idx. */

static inline void
fd_bam_clear_stream_state( fd_bam_tile_t * ctx,
                           uint            drop_reason_idx ) {
  ctx->bam_stream = NULL;
  fd_bam_set_stream_live( ctx, 0U );
  if( FD_UNLIKELY( ctx->bam_leader_pending ) ) ctx->metrics.leader_pending_dropped_cnt[ drop_reason_idx ]++;
  ctx->bam_leader_pending = 0U;
}

void
fd_bam_client_reset( fd_bam_tile_t * ctx ) {
  long now = fd_bam_now();

  /* Request BAM deactivation immediately rather than waiting for
     housekeeping.  Downstream ownership remains active until pack
     acknowledges retirement of pending BAM work. */
  fd_bam_publish_active_state( ctx, ctx->stem, 0 );

  if( FD_UNLIKELY( ctx->tcp_sock >= 0 ) ) {
    if( FD_UNLIKELY( 0 != close( ctx->tcp_sock ) ) ) {
      FD_LOG_ERR(( "close(tcp_sock=%i) failed (%i-%s)", ctx->tcp_sock, errno, fd_io_strerror( errno ) ));
    }
    ctx->tcp_sock = -1;
    ctx->tcp_sock_connected = 0;
    ctx->sock_in_epoll = 0;
    ctx->epoll_out_armed = 0;
  }
  /* BAM contact info is not cleared here.  If BAM was active,
     fd_bam_publish_active_state above began a new ownership generation,
     which drops the BAM contact; the default contact is requested once
     pack acknowledges that generation, by this or a later refresh. */
  ctx->defer_reset = 0;
  ctx->leader_schedule_gate_start_ns = 0L;
  ctx->leader_schedule_recheck_slot = FD_BAM_LEADER_SCHEDULE_RECHECK_NONE_SLOT;

  /* As in jito-solana, every session must publish its own builder
     before it is healthy.  Pack keeps the last published fee
     configuration, which only BAM mode uses. */
  memset( ctx->builder_pubkey, 0, sizeof(ctx->builder_pubkey) );
  ctx->builder_commission = 0U;
  memset( ctx->rtt, 0, sizeof(fd_rtt_estimate_t) );

  fd_bam_tile_backoff( ctx, now );

  fd_grpc_client_reset( ctx->grpc_client );

  /* Forget the scheduler stream and any pending leader state, but keep
     buffered BAM results so they flush once the next scheduler stream
     comes up.  The server expects every dispatched atomic batch to
     eventually produce a result. */
  fd_bam_clear_stream_state( ctx, FD_METRICS_ENUM_BAM_LEADER_PENDING_DROP_REASON_V_CLIENT_RESET_IDX );
  fd_bam_clear_auth_state( ctx );
  ctx->bam_config_inflight         = 0;
  ctx->bam_config_pending_received = 0;
  ctx->bam_config_pending_invalid  = 0;
  ctx->bam_config_received         = 0;
  ctx->bam_builder_heartbeat_received = 0;
  ctx->bam_last_builder_activity_ns = 0L;
  ctx->bam_last_validator_heartbeat_ns = 0L;
  ctx->bam_last_config_poll_ns    = 0L;
}

static void
fd_bam_client_create_conn( fd_bam_tile_t * ctx ) {
  fd_bam_client_reset( ctx );
  ctx->backoff_until = 0L; /* the dial already waited out the backoff */

  /* FIXME IPv6 support */
  fd_addrinfo_t hints = { .ai_family = AF_INET };
  fd_addrinfo_t * res = NULL;
  uchar scratch[ 4096 ];
  void * pscratch = scratch;
  int err = fd_getaddrinfo( ctx->server_fqdn, &hints, &res, &pscratch, sizeof(scratch) );
  if( FD_UNLIKELY( err ) ) {
    fd_bam_client_reset( ctx );
    ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_RESOLVE_IDX ]++;
    FD_LOG_WARNING(( "fd_getaddrinfo `%s` failed (%d-%s)",
                     ctx->server_fqdn, err, fd_gai_strerror( err ) ));
    return;
  }
  uint const ip4_addr = ((struct sockaddr_in *)res->ai_addr)->sin_addr.s_addr;
  ctx->server_ip4_addr = ip4_addr;

  int tcp_sock = socket( AF_INET, SOCK_STREAM|SOCK_CLOEXEC, 0 );
  if( FD_UNLIKELY( tcp_sock < 0 ) ) {
    FD_LOG_ERR(( "socket(AF_INET,SOCK_STREAM|SOCK_CLOEXEC,0) failed (%i-%s)", errno, fd_io_strerror( errno ) ));
  }
  ctx->tcp_sock = tcp_sock;

  if( FD_UNLIKELY( ctx->waker_fseq ) ) {
    struct epoll_event ev = { .events = EPOLLIN|EPOLLOUT, .data.fd = tcp_sock };
    if( FD_UNLIKELY( -1==epoll_ctl( FD_WAKER_INNER_FD( ctx->waker_client_idx ), EPOLL_CTL_ADD, tcp_sock, &ev ) ) )
      FD_LOG_ERR(( "epoll_ctl(ADD,bam tcp_sock) failed (%i-%s)", errno, fd_io_strerror( errno ) ));
    ctx->sock_in_epoll   = 1;
    ctx->epoll_out_armed = 1;
  }

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

  FD_LOG_INFO(( "Connecting to %s://" FD_IP4_ADDR_FMT ":%hu (%.*s)",
                ctx->is_ssl ? "https" : "http",
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
    FD_LOG_WARNING(( "connect(tcp_sock," FD_IP4_ADDR_FMT ":%u) failed (%i-%s) (fqdn=%s sni=%.*s)",
                     FD_IP4_ADDR_FMT_ARGS( ip4_addr ), ctx->server_tcp_port,
                     connect_err, fd_io_strerror( connect_err ),
                     ctx->server_fqdn, (int)ctx->server_sni_len, ctx->server_sni ));
    return;
  }

  if( ctx->is_ssl ) {
    fd_tls_t * tls = ctx->tls;
    ulong sni_len = ctx->server_sni_len;
    FD_TEST( sni_len<sizeof(tls->server_name) );
    fd_memcpy( tls->server_name, ctx->server_sni, sni_len );
    tls->server_name[ sni_len ] = '\0';
    tls->server_name_len = (ushort)sni_len;
    if( FD_UNLIKELY( !fd_rng_secure( tls->kex_private_key, 32UL ) ) ) FD_LOG_CRIT(( "fd_rng_secure failed" ));
    fd_x25519_public( tls->kex_public_key, tls->kex_private_key );
    fd_tlsrec_conn_init( ctx->tls_conn, tls, 0 );
  }

  fd_keepalive_init( ctx->keepalive, ctx->rng, ctx->keepalive_interval, ctx->keepalive_interval, fd_bam_now() );
}

static bool
fd_bam_encode_committed_cb( pb_ostream_t *          stream,
                            pb_field_t const *       field,
                            void * const *           arg ) {
  fd_bam_bundle_result_t const * res = (fd_bam_bundle_result_t const *)*arg;
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

/* fd_bam_request_unary starts a unary BamNodeApi call.  Returns 1 if
   the request was started, 0 if the client is busy. */

static int
fd_bam_request_unary( fd_bam_tile_t *      ctx,
                      char const *         path,
                      ulong                request_ctx,
                      pb_msgdesc_t const * fields,
                      void const *         req ) {
  fd_grpc_h2_stream_t * request = fd_grpc_client_request_start( ctx->grpc_client, path, strlen( path ), request_ctx,
                                                                fields, req, NULL, 0, 0 );
  if( FD_UNLIKELY( !request ) ) return 0;
  fd_grpc_client_deadline_set( request, FD_GRPC_DEADLINE_RX_END, fd_bam_now() + FD_BAM_CLIENT_REQUEST_TIMEOUT );
  return 1;
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

  /* nanopb NUL-terminates decoded strings */
  size_t challenge_len = strlen( resp.challenge_to_sign );
  if( FD_UNLIKELY( !challenge_len ) ) {
    fd_bam_clear_auth_state( ctx );
    FD_LOG_WARNING(( "AuthChallengeResponse challenge is empty" ));
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

  fd_sha512_t _sha[1];
  fd_sha512_t * sha = fd_sha512_join( fd_sha512_new( _sha ) );
  int verify_res = fd_ed25519_verify( sign_payload,
                                      sign_payload_sz,
                                      signature,
                                      ctx->bam_identity_pubkey,
                                      sha );
  if( FD_UNLIKELY( verify_res != FD_ED25519_SUCCESS ) ) {
    fd_bam_clear_auth_state( ctx );
    FD_LOG_WARNING(( "AuthChallengeResponse signature does not verify against cached BAM identity (%s)",
                     fd_ed25519_strerror( verify_res ) ));
    return 1;
  }

  fd_base58_encode_64( signature, NULL, ctx->bam_auth_signature );
  ctx->bam_auth_ready = 1;
  return 1;
}

/* fd_bam_config_sock returns sock as an fd_ip4_port_t, or one with a
   zero port if sock is invalid or missing. */

static fd_ip4_port_t
fd_bam_config_sock( bam_types_Socket const * sock,
                    uint                     port_max ) {
  uint ip4;
  if( FD_UNLIKELY( !fd_cstr_to_ip4_addr( sock->ip, &ip4 ) || !sock->port || sock->port>port_max ) ) return (fd_ip4_port_t){0};
  return (fd_ip4_port_t){ .addr = ip4, .port = fd_ushort_bswap( (ushort)sock->port ) };
}

/* Apply a successful ConfigResponse.  As in jito-solana, the block
   builder configuration applies as a whole or not at all: a commission
   above 100 or an undecodable or all-zero builder pubkey leaves the
   previous builder and commission in place. */
static void
fd_bam_apply_config( fd_bam_tile_t *               ctx,
                     bam_api_ConfigResponse const * resp ) {
  /* Config is polled repeatedly; warn only when the response changed. */
  _Bool quiet = ctx->bam_config_received && ctx->bam_config_pending_hash==ctx->bam_config_applied_hash;
  ctx->bam_config_applied_hash = ctx->bam_config_pending_hash;
  ctx->bam_config_received = 1U;

  if( FD_LIKELY( resp->has_block_engine_config ) ) {
    bam_types_BlockEngineBuilderConfig const * cfg = &resp->block_engine_config;
    if( FD_UNLIKELY( cfg->builder_commission > 100UL ) ) {
      if( !quiet ) FD_LOG_WARNING(( "BlockEngine builder commission out of range (0-100): %u", cfg->builder_commission ));
    } else {
      uchar decoded_builder_pubkey[ 32 ];
      if( FD_UNLIKELY( !fd_base58_decode_32( cfg->builder_pubkey, decoded_builder_pubkey ) ||
                       fd_mem_iszero( decoded_builder_pubkey, 32UL ) ) ) {
        if( !quiet ) FD_LOG_HEXDUMP_WARNING(( "Invalid builder pubkey in ConfigResponse",
                                 cfg->builder_pubkey,
                                 strnlen( cfg->builder_pubkey, sizeof( cfg->builder_pubkey ) ) ));
      } else {
        ctx->builder_commission = (uchar)cfg->builder_commission;
        fd_memcpy( ctx->builder_pubkey, decoded_builder_pubkey, sizeof(ctx->builder_pubkey) );

        /* As in jito-solana, pack sees a new fee configuration version
           only when the builder or commission changed. */
        fd_bam_fee_cfg_t * fee_cfg = ctx->fee_cfg;
        if( FD_UNLIKELY( !ctx->fee_cfg_version ||
                         fee_cfg->builder_commission!=ctx->builder_commission ||
                         memcmp( fee_cfg->builder_pubkey, ctx->builder_pubkey, 32UL ) ) ) {
          FD_LOG_INFO(( "BAM builder %s, commission %u%%", cfg->builder_pubkey, (uint)ctx->builder_commission ));
          FD_VOLATILE( fee_cfg->version ) = fd_uint_set_bit( ctx->fee_cfg_version, 31 );
          FD_HW_MFENCE_ST();
          fd_memcpy( fee_cfg->builder_pubkey, ctx->builder_pubkey, sizeof( fee_cfg->builder_pubkey ) );
          fee_cfg->builder_commission = ctx->builder_commission;
          FD_HW_MFENCE_ST();
          ctx->fee_cfg_version = fd_uint_clear_bit( ctx->fee_cfg_version + 1U, 31 );
          if( FD_UNLIKELY( !ctx->fee_cfg_version ) ) ctx->fee_cfg_version = 1U;
          FD_VOLATILE( fee_cfg->version ) = ctx->fee_cfg_version;
        }
      }
    }
  } else if( !quiet && fd_mem_iszero( ctx->builder_pubkey, 32UL ) ) {
    FD_LOG_WARNING(( "Missing block engine builder config in ConfigResponse; BAM stays inactive" ));
  }

  /* pb_decode leaves a missing BamConfig empty: no TPU, no shred receivers */
  if( FD_UNLIKELY( !quiet && !resp->has_bam_config ) ) FD_LOG_WARNING(( "Missing BAM config in ConfigResponse" ));
  bam_types_BamConfig const * cfg = &resp->bam_config;
  fd_ip4_port_t new_tpu     = fd_bam_config_sock( &cfg->tpu_sock,     USHRT_MAX-6U );
  fd_ip4_port_t new_tpu_fwd = fd_bam_config_sock( &cfg->tpu_fwd_sock, USHRT_MAX-6U );
  fd_ip4_port_t new_shred_sock[ FD_BAM_SHRED_SOCK_MAX ] = {0};
  uchar         new_shred_sock_cnt = 0U;
  for( ulong i=0UL; i<(ulong)cfg->shred_socks_count; i++ ) {
    fd_ip4_port_t new_sock = fd_bam_config_sock( &cfg->shred_socks[ i ], USHORT_MAX );
    if( FD_UNLIKELY( !new_sock.port ) ) {
      if( !quiet ) FD_LOG_WARNING(( "Dropping invalid BAM shred receiver socket in ConfigResponse: %s:%u",
                                    cfg->shred_socks[ i ].ip, cfg->shred_socks[ i ].port ));
      continue;
    }
    _Bool duplicate = 0;
    for( ulong j=0UL; j<(ulong)new_shred_sock_cnt; j++ ) duplicate |= new_shred_sock[ j ].l==new_sock.l;
    if( FD_LIKELY( !duplicate ) ) new_shred_sock[ new_shred_sock_cnt++ ] = new_sock;
  }

  /* A disconnect means Firedancer should resume advertising its local
     TPU ports so TPU clients do not get stuck targeting the BAM host. */
  _Bool has_valid_new_contact = !!new_tpu.addr && !!new_tpu.port && !!new_tpu_fwd.addr && !!new_tpu_fwd.port;
  if( FD_LIKELY( has_valid_new_contact ) ) {
    /* A new BAM TPU makes only an applied BAM TPU stale.  UNKNOWN would
       read as the default and drop a pending default restore. */
    if( FD_UNLIKELY( ( ctx->bam_tpu.l != new_tpu.l || ctx->bam_tpu_fwd.l != new_tpu_fwd.l ) &&
                     ctx->tpu_update_state==FD_BAM_CONTACT_UPDATE_STATE_APPLIED_BAM ) )
      ctx->tpu_update_state = FD_BAM_CONTACT_UPDATE_STATE_PENDING_BAM;
    ctx->bam_tpu     = new_tpu;
    ctx->bam_tpu_fwd = new_tpu_fwd;
  } else if( !quiet && resp->has_bam_config ) {
    FD_LOG_WARNING(( "Received incomplete or invalid TPU config (tpu=%s:%u tpu_fwd=%s:%u); preserving prior BAM TPU config",
                     cfg->tpu_sock.ip, cfg->tpu_sock.port, cfg->tpu_fwd_sock.ip, cfg->tpu_fwd_sock.port ));
  }
  ctx->bam_shred_sock_cnt = new_shred_sock_cnt;
  fd_memcpy( ctx->bam_shred_sock, new_shred_sock, new_shred_sock_cnt * sizeof(fd_ip4_port_t) );

  /* Connection health owns scheduler work independently of whether a usable
     BAM TPU contact is cached.  Active-state publication selects the BAM TPU
     override only when that contact is complete. */
  fd_plugin_bam_update_status_t status = fd_bam_client_status( ctx );
  fd_bam_publish_active_state( ctx,
                               ctx->stem,
                               status == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
}

static void
fd_bam_try_start_stream( fd_bam_tile_t * ctx ) {
  if( FD_UNLIKELY( ctx->bam_stream ) ) return;
  if( FD_UNLIKELY( fd_grpc_client_request_is_blocked( ctx->grpc_client ) ) ) return;

  bam_api_SchedulerMessage msg = bam_api_SchedulerMessage_init_default;
  msg.which_versioned_msg        = bam_api_SchedulerMessage_v0_tag;
  msg.versioned_msg.v0.which_msg = bam_api_SchedulerMessageV0_auth_proof_tag;
  bam_types_AuthProof * proof = &msg.versioned_msg.v0.msg.auth_proof;
  fd_memcpy( proof->challenge_to_sign, ctx->challenge_to_sign, sizeof(proof->challenge_to_sign) );
  fd_cstr_ncpy( proof->validator_pubkey, ctx->bam_identity_pubkey_b58, sizeof(proof->validator_pubkey) );
  fd_cstr_ncpy( proof->signature,        ctx->bam_auth_signature,      sizeof(proof->signature)        );

  static char const path[] = "/bam_api.BamNodeApi/InitSchedulerStream";
  fd_grpc_h2_stream_t * stream = fd_grpc_client_request_start( ctx->grpc_client, path, sizeof(path)-1, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream,
                                                               &bam_api_SchedulerMessage_msg, &msg, NULL, 0, 1 );
  if( FD_UNLIKELY( !stream ) ) {
    FD_LOG_WARNING(( "Failed BAM GRPC call `InitSchedulerStream`" ));
    return;
  }
  /* The stream is long-lived; only its response headers have a deadline. */
  fd_grpc_client_deadline_set( stream,
                               FD_GRPC_DEADLINE_HEADER,
                               fd_bam_now() + FD_BAM_CLIENT_REQUEST_TIMEOUT );
  ctx->bam_stream            = stream;
  ctx->bam_auth_ready        = 0;
  ctx->challenge_to_sign[ 0 ] = '\0';
}

/* fd_bam_stream_send sends msg as a v0 SchedulerMessage on the live
   scheduler stream and counts the outcome as ok_idx or fail_idx.
   Returns 0 without sending while the stream is down or its TX path is
   busy; the caller retries on a later step. */

static int
fd_bam_stream_send( fd_bam_tile_t *            ctx,
                    bam_api_SchedulerMessage * msg,
                    ulong                      ok_idx,
                    ulong                      fail_idx ) {
  if( FD_UNLIKELY( !ctx->bam_stream || !ctx->bam_stream_live ) ) return 0;
  if( FD_UNLIKELY( fd_grpc_client_stream_send_is_blocked( ctx->grpc_client ) ) ) return 0;
  msg->which_versioned_msg = bam_api_SchedulerMessage_v0_tag;
  int send_res = fd_grpc_client_stream_send_msg( ctx->grpc_client, ctx->bam_stream, &bam_api_SchedulerMessage_msg, msg );
  ctx->metrics.outbound_enqueue_outcome_cnt[ send_res ? ok_idx : fail_idx ]++;
  return send_res;
}

static bool
fd_bam_encode_batch_results_cb( pb_ostream_t *    stream,
                                pb_field_t const * field,
                                void * const *     arg ) {
  fd_bam_tile_t const * ctx = (fd_bam_tile_t const *)*arg;

  uint result_cnt = fd_uint_min( (uint)ctx->feedback_queue_depth, FD_BAM_RESULTS_PER_MESSAGE );
  for( uint result_i=0U; result_i<result_cnt; result_i++ ) {
    ushort result_idx = (ushort)(((uint)ctx->bam_results_head+result_i) % FD_BAM_MAX_PENDING_RESULTS);
    fd_bam_bundle_result_t const * res = &ctx->bam_results[ result_idx ];
    bam_types_AtomicTxnBatchResult atomic_res = bam_types_AtomicTxnBatchResult_init_default;
    atomic_res.seq_id = res->seq_id;

    if( FD_LIKELY( res->execution_success ) ) {
      atomic_res.which_result = bam_types_AtomicTxnBatchResult_committed_tag;
      atomic_res.result.committed.transaction_results.funcs.encode = fd_bam_encode_committed_cb;
      atomic_res.result.committed.transaction_results.arg          = (void *)res;
    } else {
      atomic_res.which_result = bam_types_AtomicTxnBatchResult_not_committed_tag;
      bam_types_NotCommitted * out = &atomic_res.result.not_committed;

      if( FD_UNLIKELY( res->bundle_err==FD_BAM_BUNDLE_ERR_DESER ) ) {
        out->which_reason                        = bam_types_NotCommitted_deserialization_error_tag;
        out->reason.deserialization_error.index  = res->deser_index;
        out->reason.deserialization_error.reason = (bam_types_DeserializationErrorReason)res->deser_reason;
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
        /* Report the first error that is not a cancellation */
        uchar err_idx = 0U;
        while( err_idx<res->bundle_txn_cnt && res->transaction_err[ err_idx ]==bam_types_TransactionErrorReason_COMMIT_CANCELLED ) err_idx++;
        if( FD_UNLIKELY( err_idx==res->bundle_txn_cnt && res->bundle_txn_cnt>1U ) ) {
          out->which_reason            = bam_types_NotCommitted_scheduling_error_tag;
          out->reason.scheduling_error = bam_types_SchedulingError_POH_TIMEOUT;
        } else {
          if( FD_UNLIKELY( err_idx==res->bundle_txn_cnt ) ) err_idx = 0U;
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

    if( FD_UNLIKELY( !pb_encode_tag_for_field( stream, field ) ) ) return false;
    if( FD_UNLIKELY( !pb_encode_submessage( stream,
                                            bam_types_AtomicTxnBatchResult_fields,
                                            &atomic_res ) ) ) return false;
  }
  return true;
}

int
fd_bam_send_leader_state( fd_bam_tile_t *                ctx,
                          fd_bam_leader_state_t const *  state ) {
  long now = fd_bam_now();
  uint reason;
  if( FD_UNLIKELY( fd_bam_leader_state_suppress_reason( state, now, &reason ) ) ) {
    fd_bam_note_leader_state_suppressed( ctx, state, reason, now );
    ctx->bam_leader_pending = 0U;
    ctx->bam_leader_state = (fd_bam_leader_state_t){ .slot = ULONG_MAX };
    return 0;
  }

  bam_api_SchedulerMessage msg = bam_api_SchedulerMessage_init_default;
  msg.versioned_msg.v0.which_msg = bam_api_SchedulerMessageV0_leader_state_tag;
  msg.versioned_msg.v0.msg.leader_state = (bam_types_LeaderState) {
    .slot                     = state->slot,
    .tick                     = state->tick,
    .slot_cu_budget_remaining = state->slot_cu_budget_remaining
  };
  return fd_bam_stream_send( ctx, &msg, FD_METRICS_ENUM_BAM_ENQUEUE_OUTCOME_V_LEADER_STATE_ENQUEUED_IDX,
                                        FD_METRICS_ENUM_BAM_ENQUEUE_OUTCOME_V_LEADER_STATE_ENQUEUE_FAIL_IDX );
}

int
fd_bam_flush_results( fd_bam_tile_t * ctx ) {
  if( FD_LIKELY( !ctx->feedback_queue_depth ) ) return 0;

  bam_api_SchedulerMessage msg = bam_api_SchedulerMessage_init_default;
  msg.versioned_msg.v0.which_msg = bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag;
  msg.versioned_msg.v0.msg.multiple_atomic_txn_batch_result.results = (pb_callback_t) {
    .funcs.encode = fd_bam_encode_batch_results_cb,
    .arg          = ctx
  };
  int busy = 0;
  while( ctx->feedback_queue_depth ) {
    uint result_cnt = fd_uint_min( (uint)ctx->feedback_queue_depth, FD_BAM_RESULTS_PER_MESSAGE );
    if( FD_UNLIKELY( !fd_bam_stream_send( ctx, &msg, FD_METRICS_ENUM_BAM_ENQUEUE_OUTCOME_V_RESULT_ENQUEUED_IDX,
                                                     FD_METRICS_ENUM_BAM_ENQUEUE_OUTCOME_V_RESULT_ENQUEUE_FAIL_IDX ) ) ) break;
    ctx->bam_results_head = (ushort)(((uint)ctx->bam_results_head + result_cnt) % FD_BAM_MAX_PENDING_RESULTS);
    ctx->feedback_queue_depth = (ushort)((uint)ctx->feedback_queue_depth - result_cnt);
    busy = 1;
  }
  return busy;
}

int
fd_bam_client_step_reconnect( fd_bam_tile_t * ctx,
                              long            now ) {
  if( FD_UNLIKELY( !fd_grpc_client_is_connected( ctx->grpc_client ) ) ) return 0;

  int busy = 0;

  /* Request auth challenge before opening the scheduler stream. */
  if( FD_UNLIKELY( !ctx->bam_auth_ready && !ctx->bam_auth_inflight && !ctx->bam_stream ) ) {
    ctx->bam_auth_inflight = !!fd_bam_request_unary( ctx, "/bam_api.BamNodeApi/GetAuthChallenge", FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge,
                                                     &bam_api_AuthChallengeRequest_msg, &(bam_api_AuthChallengeRequest){0} );
    busy = 1;
  }

  /* Start scheduler stream. */
  if( FD_UNLIKELY( ctx->bam_auth_ready ) ) fd_bam_try_start_stream( ctx );

  /* Poll the config every second until this session has a builder, then
     every 60 s (jito-solana bam_connection.rs REFRESH_CONFIG_INTERVAL: 1 s). */
  _Bool const poll_due = !ctx->bam_last_config_poll_ns ||
                         now - ctx->bam_last_config_poll_ns >= ( fd_mem_iszero( ctx->builder_pubkey, 32UL ) ? (long)1e9 : (long)60e9 );
  if( FD_UNLIKELY( !ctx->bam_config_inflight && poll_due ) ) {
    if( FD_LIKELY( fd_bam_request_unary( ctx, "/bam_api.BamNodeApi/GetBuilderConfig", FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig,
                                         &bam_api_ConfigRequest_msg, &(bam_api_ConfigRequest){0} ) ) ) {
      ctx->bam_last_config_poll_ns     = now;
      ctx->bam_config_inflight         = 1;
      ctx->bam_config_pending_received = 0;
      ctx->bam_config_pending_invalid  = 0;
    }
    busy = 1;
  }

  /* Heartbeat to keep validator session live.  A busy TX path keeps it
     due for a later step. */
  if( FD_LIKELY( ctx->bam_stream && ctx->bam_stream_live ) &&
      FD_UNLIKELY( !ctx->bam_last_validator_heartbeat_ns || now - ctx->bam_last_validator_heartbeat_ns >= (long)5e9 ) ) {
    bam_api_SchedulerMessage msg = bam_api_SchedulerMessage_init_default;
    msg.versioned_msg.v0.which_msg = bam_api_SchedulerMessageV0_heart_beat_tag;
    msg.versioned_msg.v0.msg.heart_beat.time_sent_microseconds = (ulong)fd_long_max( now/1000, 0 );
    if( FD_LIKELY( fd_bam_stream_send( ctx, &msg, FD_METRICS_ENUM_BAM_ENQUEUE_OUTCOME_V_HEARTBEAT_ENQUEUED_IDX,
                                                  FD_METRICS_ENUM_BAM_ENQUEUE_OUTCOME_V_HEARTBEAT_ENQUEUE_FAIL_IDX ) ) ) {
      ctx->bam_last_validator_heartbeat_ns = now;
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

  /* Send an HTTP/2 PING (transport keepalive, not the scheduler proto
     Ping) unless the connection is busy. */
  if( FD_UNLIKELY( fd_keepalive_should_tx( ctx->keepalive, now ) ) ) {
    fd_h2_conn_t * conn = fd_grpc_client_h2_conn( ctx->grpc_client );
    if( FD_LIKELY( !conn->flags && fd_h2_tx_ping( conn, fd_grpc_client_rbuf_tx( ctx->grpc_client ) ) ) ) {
      long ping_now = fd_bam_now();
      fd_keepalive_tx( ctx->keepalive, ctx->rng, ping_now );
      FD_LOG_DEBUG(( "Keepalive TX (deadline=+%gs)", (double)( ctx->keepalive->ts_deadline-ping_now )/1e9 ));
    }
    busy = 1;
  }

  /* Flush queued execution results back to the BAM node once it has
     accepted the scheduler stream.  Results sent on a stream it then
     rejects would be lost. */
  if( FD_LIKELY( ctx->bam_stream_live && ctx->bam_builder_heartbeat_received ) ) busy |= fd_bam_flush_results( ctx );

  return busy;
}

static void
fd_bam_client_step1( fd_bam_tile_t * ctx,
                     int *           charge_busy,
                     long            now ) {

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
    if( FD_LIKELY( !ctx->bam_stream_reject_repeat ) ) {
      FD_LOG_WARNING(( "BAM client reset requested; retrying %s/" FD_IP4_ADDR_FMT ":%hu",
        ctx->server_fqdn,
        FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
        ctx->server_tcp_port ));
    }
    fd_bam_client_reset( ctx );
    *charge_busy = 1;
    return;
  }

  int leader_schedule_gate_active = fd_bam_tile_leader_schedule_gate_active( ctx );
  if( FD_LIKELY( !leader_schedule_gate_active ) ) {
    ctx->leader_schedule_gate_start_ns = 0L;
  } else if( FD_UNLIKELY( ctx->leader_schedule_recheck_slot!=FD_BAM_LEADER_SCHEDULE_RECHECK_DUE_SLOT ) ) {
    if( FD_UNLIKELY( !ctx->leader_schedule_gate_start_ns ) ) ctx->leader_schedule_gate_start_ns = now;
    if( FD_LIKELY( now - ctx->leader_schedule_gate_start_ns < FD_BAM_LEADER_SCHEDULE_RECHECK_WALLCLOCK_NS ) ) return;
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
      FD_LOG_WARNING(( "BAM gRPC connect attempt failed (%i-%s) while dialing %s/" FD_IP4_ADDR_FMT ":%hu",
        connect_result, fd_io_strerror( connect_result ),
        ctx->server_fqdn,
        FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
        ctx->server_tcp_port ));
      fd_bam_client_reset( ctx );
      ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_CONNECT_IDX ]++;
      *charge_busy = 1;
      return;
    }
    if( FD_LIKELY( revents & POLLOUT ) ) {
      FD_LOG_DEBUG(( "BAM TCP socket connected" ));
      ctx->tcp_sock_connected = 1;
      *charge_busy = 1;
    }
    return;
  }

  /* gRPC conn died? */
  if( FD_UNLIKELY( !ctx->grpc_client ) ) {
  reconnect:
    /* next_deadline schedules the retry at backoff_until. */
    if( FD_UNLIKELY( fd_bam_tile_should_stall( ctx, now ) ) ) return;
    if( FD_UNLIKELY( leader_schedule_gate_active &&
                     ctx->leader_schedule_recheck_slot==FD_BAM_LEADER_SCHEDULE_RECHECK_DUE_SLOT ) ) {
      ctx->leader_schedule_recheck_slot = FD_BAM_LEADER_SCHEDULE_RECHECK_NONE_SLOT;
      ctx->leader_schedule_gate_start_ns = 0L;
    }
    fd_bam_client_create_conn( ctx );
    *charge_busy = 1;
    return;
  }

  /* Did a HTTP/2 PING time out.  The reconnect reinitializes keepalive. */
  if( FD_UNLIKELY( fd_keepalive_is_timeout( ctx->keepalive, now ) ) ) {
    FD_LOG_WARNING(( "BAM gRPC timed out (HTTP/2 PING went unanswered for %.2f seconds); retrying %s/" FD_IP4_ADDR_FMT ":%hu",
                     (double)( now - ctx->keepalive->ts_last_tx )/1e9,
                     ctx->server_fqdn,
                     FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
                     ctx->server_tcp_port ));
    fd_bam_client_reset( ctx );
    ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_KEEPALIVE_TIMEOUT_IDX ]++;
    *charge_busy = 1;
    return;
  }

  /* Did the scheduler-stream BuilderHeartBeat time out */
  if( FD_UNLIKELY( ctx->bam_stream_live &&
                   ctx->bam_last_builder_activity_ns != 0L &&
                   now - ctx->bam_last_builder_activity_ns >= FD_BAM_ACTIVITY_TIMEOUT_NS ) ) {
    FD_LOG_WARNING(( "BAM node heartbeat timed out (no BuilderHeartBeat for %.2f seconds); retrying %s/" FD_IP4_ADDR_FMT ":%hu",
      (double)( now - ctx->bam_last_builder_activity_ns )/1e9,
      ctx->server_fqdn,
      FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
      ctx->server_tcp_port ));
    fd_bam_client_reset( ctx );
    ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_BUILDER_ACTIVITY_TIMEOUT_IDX ]++;
    *charge_busy = 1;
    return;
  }

  /* Drive I/O, SSL handshake, and any inflight requests.  Callbacks may
     request a reset. */
  int io_err = -1==( ctx->is_ssl ? fd_grpc_client_rxtx_tls( ctx->grpc_client, ctx->tls_conn, ctx->tcp_sock, now, charge_busy )
                                 : fd_grpc_client_rxtx_socket( ctx->grpc_client, ctx->tcp_sock, now, charge_busy ) );
  if( FD_UNLIKELY( io_err || ctx->defer_reset ) ) {
    if( FD_LIKELY( !ctx->bam_stream_reject_repeat ) ) {
      FD_LOG_WARNING(( "BAM client reset; retrying %s/" FD_IP4_ADDR_FMT ":%hu",
                       ctx->server_fqdn,
                       FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
                       ctx->server_tcp_port ));
    }
    fd_bam_client_reset( ctx );
    if( io_err ) ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_IO_IDX ]++;
    *charge_busy = 1;
    return;
  }

  /* Are we ready to issue a new request? */
  if( FD_UNLIKELY( fd_grpc_client_request_is_blocked( ctx->grpc_client ) ) ) return;
  if( FD_UNLIKELY( fd_bam_tile_should_stall( ctx, now ) ) ) return;

  *charge_busy |= fd_bam_client_step_reconnect( ctx, now );
}

void
fd_bam_client_step( fd_bam_tile_t * ctx,
                       int *              charge_busy ) {
  long now = fd_bam_now();
  /* Edge-trigger logging with rate limiting */
  fd_bam_client_step1( ctx, charge_busy, now );
  fd_plugin_bam_update_status_t status = fd_bam_client_status_at( ctx, now );
  _Bool const healthy_now    = ( status == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
  _Bool const healthy_before = ( ctx->bam_status_counted == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
  if( FD_UNLIKELY( status != ctx->bam_status_counted ) ) {
    for( ulong i=0UL; i<FD_BAM_LEADER_SLOT_END_TRACKER_CNT; i++ ) {
      fd_bam_leader_slot_end_tracker_t * tracker = &ctx->leader_slot_end[ i ];
      if( FD_UNLIKELY( !tracker->valid || tracker->counted || now>tracker->slot_end_ns ) ) continue;
      tracker->healthy_at_end = healthy_now;
    }
  }
  if( FD_UNLIKELY( healthy_now != healthy_before ) ) {
    if( healthy_now ) {
      ctx->metrics.healthy_connects_cnt++;
      ctx->bam_stream_reject_hash = 0UL; /* a healthy session ends a rejection streak */
    } else {
      ctx->metrics.healthy_disconnects_cnt++;
    }
  }
  ctx->bam_status_counted = status;
  if( FD_UNLIKELY( status!=ctx->bam_status_logged ) ) {
    long ts = fd_log_wallclock();
    if( FD_LIKELY( ts-(ctx->last_bam_status_log_nanos) >= (long)1e6 ) ) {
      if( FD_UNLIKELY( status==FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED ) ) {
        if( FD_UNLIKELY( ctx->bam_stream_reject_repeat ) ) FD_LOG_INFO(( "BAM connection state DISCONNECTED" ));
        else                                               FD_LOG_WARNING(( "Disconnected from BAM node" ));
      } else if( FD_UNLIKELY( status==FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTING ) ) {
        FD_LOG_INFO(( "BAM connection state CONNECTING" ));
      } else if( FD_UNLIKELY( status==FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY ) ) {
        FD_LOG_INFO(( "BAM connection state CONNECTED_UNHEALTHY" ));
      } else if( FD_LIKELY( status==FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY ) ) {
        FD_LOG_NOTICE(( "Connected to BAM node at %s://%s/ (" FD_IP4_ADDR_FMT ":%hu)",
                        ctx->is_ssl ? "https" : "http",
                        ctx->server_fqdn,
                        FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ),
                        ctx->server_tcp_port ));
      }

      ctx->last_bam_status_log_nanos = ts;
      ctx->bam_status_logged         = status;
      ctx->bam_stream_reject_repeat  = 0U;
    }
  }

  if( FD_UNLIKELY( ctx->tcp_sock_connected && fd_grpc_client_tx_pending( ctx->grpc_client ) ) ) {
    int flush_err = ctx->is_ssl ? fd_grpc_client_tls_flush( ctx->grpc_client, ctx->tcp_sock )
                                : fd_grpc_client_tx_flush_socket( ctx->grpc_client, ctx->tcp_sock );
    if( FD_UNLIKELY( flush_err<0 ) ) {
      fd_bam_client_reset( ctx );
      ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_IO_IDX ]++;
      *charge_busy = 1;
      return;
    }
  }

  if( FD_LIKELY( ctx->tcp_sock_connected && ctx->sock_in_epoll ) ) {
    int want = fd_grpc_client_tx_pending( ctx->grpc_client );
    want |= ctx->is_ssl && fd_grpc_client_tls_tx_pending( ctx->grpc_client );
    if( FD_UNLIKELY( want!=(int)ctx->epoll_out_armed ) ) {
      struct epoll_event ev = { .events = EPOLLIN | (want ? EPOLLOUT : 0U), .data.fd = ctx->tcp_sock };
      if( FD_UNLIKELY( -1==epoll_ctl( FD_WAKER_INNER_FD( ctx->waker_client_idx ), EPOLL_CTL_MOD, ctx->tcp_sock, &ev ) ) )
        FD_LOG_ERR(( "epoll_ctl(MOD,bam tcp_sock) failed (%i-%s)", errno, fd_io_strerror( errno ) ));
      ctx->epoll_out_armed = !!want;
    }
  }
}

long
fd_bam_client_next_deadline( fd_bam_tile_t const * ctx,
                             long                  now ) {
  if( FD_UNLIKELY( !ctx->enabled || !ctx->server_fqdn_len || !ctx->server_tcp_port ) ) return LONG_MAX;
  if( FD_UNLIKELY( ctx->defer_reset ) ) return now;
  if( FD_UNLIKELY( ctx->leader_schedule_gate_start_ns ) )
    return fd_bam_client_no_progress_deadline( ctx->leader_schedule_gate_start_ns + FD_BAM_LEADER_SCHEDULE_RECHECK_WALLCLOCK_NS, now );
  if( FD_UNLIKELY( ctx->tcp_sock<0 ) ) return fd_bam_client_no_progress_deadline( ctx->backoff_until, now );
  if( FD_UNLIKELY( !ctx->tcp_sock_connected ) ) return LONG_MAX;

  if( FD_UNLIKELY( ctx->is_ssl &&
                   fd_grpc_client_tls_rx_pending( ctx->grpc_client ) &&
                   !fd_grpc_client_tls_tx_pending( ctx->grpc_client ) ) ) return now;

  long deadline = fd_grpc_client_next_deadline( ctx->grpc_client );
  if( FD_UNLIKELY( !fd_grpc_client_is_connected( ctx->grpc_client ) ) ) return fd_bam_client_no_progress_deadline( deadline, now );
  if( FD_LIKELY( ctx->keepalive->interval ) )
    deadline = fd_long_min( deadline, ctx->keepalive->inflight ? ctx->keepalive->ts_deadline
                                                               : ctx->keepalive->ts_next_tx );
  if( FD_UNLIKELY( ctx->bam_stream_live ) ) {
    if( ctx->bam_last_builder_activity_ns )
      deadline = fd_long_min( deadline, ctx->bam_last_builder_activity_ns+FD_BAM_ACTIVITY_TIMEOUT_NS );
    if( ctx->bam_last_validator_heartbeat_ns )
      deadline = fd_long_min( deadline, ctx->bam_last_validator_heartbeat_ns+(long)5e9 );
  }

  if( FD_UNLIKELY( !ctx->bam_config_inflight ) )
    deadline = fd_long_min( deadline, ctx->bam_last_config_poll_ns ? ctx->bam_last_config_poll_ns+( fd_mem_iszero( ctx->builder_pubkey, 32UL ) ? (long)1e9 : (long)60e9 ) : now );

  if( FD_UNLIKELY( !fd_grpc_client_request_is_blocked( ctx->grpc_client ) &&
                   ctx->bam_auth_ready && !ctx->bam_stream ) )
    deadline = now;
  deadline = fd_bam_client_no_progress_deadline( deadline, now );
  if( FD_UNLIKELY( ctx->backoff_until>now ) )
    deadline = fd_long_min( deadline, ctx->backoff_until );
  return deadline;
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
fd_bam_client_grpc_tx_complete( void * app_ctx,
                                ulong  request_ctx ) {
  (void)app_ctx; (void)request_ctx;
}

static void
fd_bam_client_grpc_rx_start( void * app_ctx,
                             ulong  request_ctx ) {
  fd_bam_tile_t * ctx = app_ctx;
  if( FD_UNLIKELY( request_ctx!=FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream ) ) return;
  long now = fd_bam_now();
  fd_bam_set_stream_live( ctx, 1U );
  ctx->bam_last_validator_heartbeat_ns = now;
  ctx->bam_last_builder_activity_ns    = now;
  ctx->bam_builder_heartbeat_received  = 0U;
  uint reason;
  if( FD_UNLIKELY( ctx->bam_leader_state.slot != ULONG_MAX &&
                   fd_bam_leader_state_suppress_reason( &ctx->bam_leader_state, now, &reason ) ) ) {
    fd_bam_note_leader_state_suppressed( ctx, &ctx->bam_leader_state, reason, now );
    ctx->bam_leader_state = (fd_bam_leader_state_t){ .slot = ULONG_MAX };
  }
  ctx->bam_leader_pending = ctx->bam_leader_state.slot!=ULONG_MAX && ctx->bam_leader_state.slot_end_ns;
}

static void
fd_bam_client_grpc_rx_msg( void *       app_ctx,
                           void const * protobuf,
                           ulong        protobuf_sz,
                           ulong        request_ctx ) {
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
    /* Decode without changing the live configuration; rx_end applies it
       after OK trailers. */
    if( FD_UNLIKELY( ctx->bam_config_pending_received || ctx->bam_config_pending_invalid ) ) {
      ctx->bam_config_pending_invalid = 1;
      ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_CONFIG_DECODE_IDX ]++;
      FD_LOG_WARNING(( "GetBuilderConfig sent multiple response messages" ));
      break;
    }
    pb_istream_t istream = pb_istream_from_buffer( protobuf, protobuf_sz );
    if( FD_UNLIKELY( !pb_decode( &istream, &bam_api_ConfigResponse_msg, &ctx->bam_config_pending ) ) ) {
      ctx->bam_config_pending_invalid = 1;
      ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_CONFIG_DECODE_IDX ]++;
      FD_LOG_WARNING(( "Protobuf decode of (bam_api.ConfigResponse) failed" ));
      break;
    }
    ctx->bam_config_pending_hash     = fd_hash( 0UL, protobuf, protobuf_sz );
    ctx->bam_config_pending_received = 1;
    break;
  }
  case FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream:
    fd_bam_handle_scheduler_response( ctx,
                                      protobuf,
                                      protobuf_sz,
                                      rx_ts_ns );
    break;
  default:
    FD_LOG_ERR(( "Received unexpected gRPC message (request_ctx=%lu)", request_ctx ));
  }
}

static void
fd_bam_client_request_failed( fd_bam_tile_t * ctx,
                                 ulong              request_ctx ) {
  ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_REQUEST_FAILED_IDX ]++;
  if( FD_UNLIKELY( request_ctx==FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig ) ) {
    /* A config poll is independent of the healthy scheduler stream.  Pace
       its retry locally so heartbeats and queued results can still send. */
    ctx->bam_config_pending_received = 0;
    ctx->bam_config_pending_invalid  = 0;
    ctx->bam_config_inflight         = 0;
    ctx->bam_last_config_poll_ns     = fd_bam_now();
    return;
  }
  fd_bam_tile_backoff( ctx, fd_bam_now() );
  switch( request_ctx ) {
  case FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge:
    fd_bam_clear_auth_state( ctx );
    break;
  case FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream:
    fd_bam_clear_stream_state( ctx, FD_METRICS_ENUM_BAM_LEADER_PENDING_DROP_REASON_V_REQUEST_FAILED_IDX );
    ctx->bam_auth_ready = 0;
    ctx->challenge_to_sign[ 0 ] = '\0';
    break;
  }
}

/* fd_bam_note_scheduler_reject records a scheduler stream rejection and
   returns 1 if it repeats the previous one while next_leader_slot is
   unknown.  Outside the leader schedule, every leader schedule recheck
   dials BAM and gets the same PERMISSION_DENIED.  The first rejection of
   such a streak warns as usual; repeats skip the WARNING lines and leave
   only the INFO trail.  A known leader slot, any other rejection, or
   the first rejection in a new epoch always warns. */
static int
fd_bam_note_scheduler_reject( fd_bam_tile_t *             ctx,
                              fd_grpc_resp_hdrs_t const * resp ) {
  ulong reject_hash = 0UL;
  if( resp->grpc_status==FD_GRPC_STATUS_PERMISSION_DENIED ) ctx->bam_admitted_epoch_end = 0UL; /* no longer admitted */
  if( resp->grpc_status==FD_GRPC_STATUS_PERMISSION_DENIED &&
      ctx->replay_in_idx!=ULONG_MAX &&
      !fd_bam_tile_leader_slot_known( ctx ) ) {
    reject_hash = fd_hash( 0UL, resp->grpc_msg, resp->grpc_msg_len ) | 1UL; /* nonzero */
  }
  int repeat = reject_hash && reject_hash==ctx->bam_stream_reject_hash;
  ctx->bam_stream_reject_hash   = reject_hash;
  ctx->bam_stream_reject_repeat = !!repeat;
  return repeat;
}

static void
fd_bam_client_grpc_rx_end( void *                app_ctx,
                           ulong                 request_ctx,
                           fd_grpc_resp_hdrs_t * resp ) {
  fd_bam_tile_t * ctx = app_ctx;
  if( FD_UNLIKELY( resp->h2_status != 200 ) ) {
    FD_LOG_WARNING(( "gRPC request failed (HTTP status %u)", resp->h2_status ));
    fd_bam_client_request_failed( ctx, request_ctx );
    /* The scheduler's bidirectional stream can be RX closed while its TX
       half remains open.  Reset H2 before retrying to release its quota. */
    if( FD_UNLIKELY( request_ctx==FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream ) ) ctx->defer_reset = 1;
    return;
  }

  resp->grpc_msg_len = (uint)fd_url_unescape( resp->grpc_msg, resp->grpc_msg_len );
  if( FD_LIKELY( !resp->grpc_msg_len ) ) {
    fd_memcpy( resp->grpc_msg, "unknown error", 13 );
    resp->grpc_msg_len = 13;
  }

  if( FD_UNLIKELY( resp->grpc_status != FD_GRPC_STATUS_OK ) ) {
    int repeat = request_ctx==FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream &&
                 fd_bam_note_scheduler_reject( ctx, resp );
    if( FD_LIKELY( !repeat ) ) {
      FD_LOG_WARNING(( "gRPC request %s failed (gRPC status %u-%s): %.*s",
                       fd_bam_request_ctx_cstr( request_ctx ),
                       resp->grpc_status, fd_grpc_status_cstr( resp->grpc_status ),
                       (int)resp->grpc_msg_len, resp->grpc_msg ));
    }

    if( FD_UNLIKELY( request_ctx==FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream &&
                     resp->grpc_status==FD_GRPC_STATUS_INVALID_ARGUMENT ) ) fd_bam_handle_leader_state_reject( ctx, resp );

    /* Also discards the auth challenge of a failed auth or stream request */
    fd_bam_client_request_failed( ctx, request_ctx );
    if( request_ctx==FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream ) {
      ctx->defer_reset = 1;
      FD_LOG_INFO(( "BAM scheduler stream failed (gRPC status %u-%s). Reconnecting ...",
                    resp->grpc_status, fd_grpc_status_cstr( resp->grpc_status ) ));
      /* A rejection repeated while no leader slot is known, e.g. not on
         the node's leader schedule, usually will not clear soon (but
         transient ones can repeat, e.g. "Connection blocked").  The node
         takes no results meanwhile, and queued ones would keep the leader
         schedule gate from holding off redials. */
      if( FD_UNLIKELY( repeat && ctx->feedback_queue_depth ) ) {
        FD_LOG_WARNING(( "Dropping %u queued BAM bundle results: the BAM node keeps rejecting the scheduler stream",
                         (uint)ctx->feedback_queue_depth ));
        ctx->metrics.feedback_results_dropped_cnt += (ulong)ctx->feedback_queue_depth;
        ctx->bam_results_head     = ctx->bam_results_tail;
        ctx->feedback_queue_depth = 0U;
      }
    }
    return;
  }

  switch( request_ctx ) {
  case FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge:
    ctx->bam_auth_inflight = 0;
    break;
  case FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig:
    if( FD_UNLIKELY( !ctx->bam_config_pending_received || ctx->bam_config_pending_invalid ) ) {
      FD_LOG_WARNING(( "GetBuilderConfig completed without exactly one valid response" ));
      fd_bam_client_request_failed( ctx, request_ctx );
      break;
    }
    fd_bam_apply_config( ctx, &ctx->bam_config_pending );
    ctx->bam_config_pending_received = 0;
    ctx->bam_config_inflight         = 0;
    break;
  case FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream:
    fd_bam_clear_stream_state( ctx, FD_METRICS_ENUM_BAM_LEADER_PENDING_DROP_REASON_V_STREAM_ENDED_IDX );
    fd_bam_tile_backoff( ctx, fd_bam_now() );
    ctx->defer_reset = 1;
    FD_LOG_INFO(( "BAM scheduler stream ended cleanly. Reconnecting ..." ));
    break;
  }
}

static void
fd_bam_client_grpc_rx_timeout( void * app_ctx,
                               ulong  request_ctx,
                               int    deadline_kind ) {
  (void)deadline_kind;
  FD_LOG_WARNING(( "Request timed out: %s", fd_bam_request_ctx_cstr( request_ctx ) ));
  fd_bam_tile_t * ctx = app_ctx;
  ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_REQUEST_TIMEOUT_IDX ]++;
  if( FD_UNLIKELY( request_ctx==FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig ) ) {
    /* gRPC cancels only this request.  Keep a healthy scheduler stream and
       retry config on its own poll throttle, without stalling feedback.
       Before the first config, reconnect, possibly to another node. */
    ctx->bam_config_pending_received = 0;
    ctx->bam_config_pending_invalid  = 0;
    ctx->bam_config_inflight         = 0;
    ctx->bam_last_config_poll_ns     = fd_bam_now();
    if( FD_LIKELY( ctx->bam_config_received ) ) return;
  }
  ctx->defer_reset = 1;
  switch( request_ctx ) {
  case FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge:
    fd_bam_clear_auth_state( ctx );
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

static fd_plugin_bam_update_status_t
fd_bam_client_status_at( fd_bam_tile_t const * ctx,
                         long                  now ) {
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
  if( FD_UNLIKELY( conn->flags &
      ( FD_H2_CONN_FLAGS_DEAD |
        FD_H2_CONN_FLAGS_SEND_GOAWAY ) ) ) {
    return FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED;
  }

  /* A live stream implies a completed HTTP/2 handshake: only
     fd_bam_client_reset, which clears bam_stream_live, resets HTTP/2. */
  if( FD_UNLIKELY( !ctx->bam_stream_live ) ) {
    return FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTING; /* scheduler stream not live yet */
  }

  if( FD_UNLIKELY( fd_keepalive_is_timeout( ctx->keepalive, now ) ) ) {
    return FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED; /* possible timeout */
  }

  if( FD_UNLIKELY( !fd_grpc_client_is_connected( ctx->grpc_client ) ) ) {
    return FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTING;
  }

  /* As in jito-solana, a session without a valid builder stays on Block
     Engine and TPU scheduling. */
  if( FD_UNLIKELY( !ctx->bam_config_received || fd_mem_iszero( ctx->builder_pubkey, 32UL ) ) ) {
    return FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY;
  }

  if( FD_UNLIKELY(
    !ctx->bam_builder_heartbeat_received ||
    ( now - ctx->bam_last_builder_activity_ns >= FD_BAM_ACTIVITY_TIMEOUT_NS ) ) ) {
    return FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY;
  }

  return FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY;
}

fd_plugin_bam_update_status_t
fd_bam_client_status( fd_bam_tile_t const * ctx ) {
  return fd_bam_client_status_at( ctx, fd_bam_now() );
}
