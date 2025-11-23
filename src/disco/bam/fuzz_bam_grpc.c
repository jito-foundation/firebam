#define _GNU_SOURCE

#if !FD_HAS_HOSTED
#error "This target requires FD_HAS_HOSTED"
#endif

#include "fd_bam_tile_private.h"
#include "fd_bam_ctrl.h"
#include "fd_bam_types.h"
#include "proto/bam_api.pb.h"
#include "../plugin/fd_plugin.h"
#include "../metrics/fd_metrics.h"
#include "../stem/fd_stem.h"
#include "../fd_txn_m.h"
#include "../../util/fd_util.h"
#include "../../util/net/fd_net_headers.h"

#include <stdlib.h>
#include <string.h>

char const fdctl_version_string[] = "fuzz";

#define BAM_FUZZ_OUT_VERIFY (0UL)
#define BAM_FUZZ_OUT_GOSSIP (1UL)
#define BAM_FUZZ_OUT_MAX    (2UL)

#define BAM_FUZZ_MTU FD_TPU_PARSED_MTU

/* Simple BAM gRPC fuzzer.  The fuzzer drives ConfigResponse, SchedulerResponse,
   and AuthChallengeResponse decode paths while also exercising the runtime
   enable/disable control block.  When the tile is considered healthy and
   enabled, we assert that gossip publishes the TPU sockets learned from
   the BamConfig protobuf. */

static fd_wksp_t * g_wksp;

static struct {
  fd_bam_tile_t * tile;

  fd_bam_out_ctx_t out_verify;
  fd_bam_out_ctx_t out_gossip;

  fd_frag_meta_t * mcaches[ BAM_FUZZ_OUT_MAX ];
  void *           mcache_mem[ BAM_FUZZ_OUT_MAX ];
  ulong            depths [ BAM_FUZZ_OUT_MAX ];

  uchar * dcaches[ BAM_FUZZ_OUT_MAX ];
  void *  dcache_mem[ BAM_FUZZ_OUT_MAX ];

  ulong seqs    [ BAM_FUZZ_OUT_MAX ];
  ulong cr_avail[ BAM_FUZZ_OUT_MAX ];
  ulong min_cr_avail;

  fd_stem_context_t stem;

  fd_bam_ctrl_t     ctrl;
  fd_bam_fee_cfg_t  fee_cfg;
  fd_histf_t        rx_delay[1];

  /* Keyguard request/response wiring to avoid fd_keyguard_client_sign hanging */
  fd_frag_meta_t * key_req_mcache;
  fd_frag_meta_t * key_resp_mcache;
  void *           key_req_mcache_mem;
  void *           key_resp_mcache_mem;
  uchar *          key_req_dcache;
  uchar *          key_resp_dcache;
  void *           key_req_dcache_mem;
  void *           key_resp_dcache_mem;
  ulong            key_req_depth;
  ulong            key_resp_depth;
  ulong            key_req_mtu;

  fd_bam_tile_t tile_storage[1];
} bam_fuzz_ctx;

static void
bam_fuzz_setup_out( ulong out_idx,
                    ulong depth,
                    ulong mtu ) {
  bam_fuzz_ctx.depths[ out_idx ] = depth;

  ulong mcache_foot = fd_mcache_footprint( depth, 0UL );
  bam_fuzz_ctx.mcache_mem[ out_idx ] = fd_wksp_alloc_laddr( g_wksp, fd_mcache_align(), mcache_foot, 1UL );
  FD_TEST( bam_fuzz_ctx.mcache_mem[ out_idx ] );
  fd_frag_meta_t * mcache = fd_mcache_join( fd_mcache_new( bam_fuzz_ctx.mcache_mem[ out_idx ], depth, 0UL, 0UL ) );
  FD_TEST( mcache );
  bam_fuzz_ctx.mcaches[ out_idx ] = mcache;

  ulong dcache_data_sz = fd_dcache_req_data_sz( mtu, depth, 1UL, 1 );
  ulong dcache_foot    = fd_dcache_footprint( dcache_data_sz, 0UL );
  bam_fuzz_ctx.dcache_mem[ out_idx ] = fd_wksp_alloc_laddr( g_wksp, fd_dcache_align(), dcache_foot, 1UL );
  FD_TEST( bam_fuzz_ctx.dcache_mem[ out_idx ] );
  uchar * dcache = fd_dcache_join( fd_dcache_new( bam_fuzz_ctx.dcache_mem[ out_idx ], dcache_data_sz, 0UL ) );
  FD_TEST( dcache );
  bam_fuzz_ctx.dcaches[ out_idx ] = dcache;

  bam_fuzz_ctx.seqs    [ out_idx ] = 0UL;
  bam_fuzz_ctx.cr_avail[ out_idx ] = ULONG_MAX;
}

static void
bam_fuzz_seed_keyguard_response( fd_bam_tile_t * ctx ) {
  /* Always leave the response mcache "ready" so fd_keyguard_client_sign
     returns immediately instead of hanging the fuzzer. */
  fd_frag_meta_t * meta = bam_fuzz_ctx.key_resp_mcache +
      fd_mcache_line_idx( ctx->keyguard_client->response_seq, bam_fuzz_ctx.key_resp_depth );
  meta->seq   = ctx->keyguard_client->response_seq;
  meta->sig   = 0UL;
  ulong chunk = fd_ulong_min( ctx->keyguard_client->response_chunk0, ctx->keyguard_client->response_wmark );
  meta->chunk = (uint)chunk;
  meta->sz    = (ushort)64U;
  meta->ctl   = 0U;
  meta->tsorig = 0U;
  meta->tspub  = 0U;

  uchar * dst = fd_chunk_to_laddr( ctx->keyguard_client->response_mem, chunk );
  fd_memset( dst, 0, 64UL );
}

static void
bam_fuzz_setup_keyguard( void ) {
  bam_fuzz_ctx.key_req_depth = 1UL;
  bam_fuzz_ctx.key_resp_depth = 1UL;
  bam_fuzz_ctx.key_req_mtu = 512UL;

  ulong req_mcache_foot = fd_mcache_footprint( bam_fuzz_ctx.key_req_depth, 0UL );
  bam_fuzz_ctx.key_req_mcache_mem = fd_wksp_alloc_laddr( g_wksp, fd_mcache_align(), req_mcache_foot, 1UL );
  FD_TEST( bam_fuzz_ctx.key_req_mcache_mem );
  bam_fuzz_ctx.key_req_mcache = fd_mcache_join( fd_mcache_new( bam_fuzz_ctx.key_req_mcache_mem, bam_fuzz_ctx.key_req_depth, 0UL, 0UL ) );
  FD_TEST( bam_fuzz_ctx.key_req_mcache );

  ulong req_dcache_data_sz = fd_dcache_req_data_sz( bam_fuzz_ctx.key_req_mtu, bam_fuzz_ctx.key_req_depth, 1UL, 1 );
  ulong req_dcache_foot    = fd_dcache_footprint( req_dcache_data_sz, 0UL );
  bam_fuzz_ctx.key_req_dcache_mem = fd_wksp_alloc_laddr( g_wksp, fd_dcache_align(), req_dcache_foot, 1UL );
  FD_TEST( bam_fuzz_ctx.key_req_dcache_mem );
  bam_fuzz_ctx.key_req_dcache = fd_dcache_join( fd_dcache_new( bam_fuzz_ctx.key_req_dcache_mem, req_dcache_data_sz, 0UL ) );
  FD_TEST( bam_fuzz_ctx.key_req_dcache );

  ulong resp_mcache_foot = fd_mcache_footprint( bam_fuzz_ctx.key_resp_depth, 0UL );
  bam_fuzz_ctx.key_resp_mcache_mem = fd_wksp_alloc_laddr( g_wksp, fd_mcache_align(), resp_mcache_foot, 1UL );
  FD_TEST( bam_fuzz_ctx.key_resp_mcache_mem );
  bam_fuzz_ctx.key_resp_mcache = fd_mcache_join( fd_mcache_new( bam_fuzz_ctx.key_resp_mcache_mem, bam_fuzz_ctx.key_resp_depth, 0UL, 0UL ) );
  FD_TEST( bam_fuzz_ctx.key_resp_mcache );

  ulong resp_dcache_data_sz = fd_dcache_req_data_sz( 64UL, bam_fuzz_ctx.key_resp_depth, 1UL, 1 );
  ulong resp_dcache_foot    = fd_dcache_footprint( resp_dcache_data_sz, 0UL );
  bam_fuzz_ctx.key_resp_dcache_mem = fd_wksp_alloc_laddr( g_wksp, fd_dcache_align(), resp_dcache_foot, 1UL );
  FD_TEST( bam_fuzz_ctx.key_resp_dcache_mem );
  bam_fuzz_ctx.key_resp_dcache = fd_dcache_join( fd_dcache_new( bam_fuzz_ctx.key_resp_dcache_mem, resp_dcache_data_sz, 0UL ) );
  FD_TEST( bam_fuzz_ctx.key_resp_dcache );
}

static void
bam_fuzz_env_init( int *    pargc,
                   char *** pargv ) {
  static int initialized = 0;
  if( FD_LIKELY( initialized ) ) return;

  fd_memset( &bam_fuzz_ctx, 0, sizeof( bam_fuzz_ctx ) );

  ulong cpu_idx = fd_tile_cpu_id( fd_tile_idx() );
  if( cpu_idx>fd_shmem_cpu_cnt() ) cpu_idx = 0UL;

  char const * _page_sz = fd_env_strip_cmdline_cstr ( pargc, pargv, "--page-sz",  NULL, "normal"                     );
  ulong        page_cnt = fd_env_strip_cmdline_ulong( pargc, pargv, "--page-cnt", NULL, 256UL                        );
  ulong        numa_idx = fd_env_strip_cmdline_ulong( pargc, pargv, "--numa-idx", NULL, fd_shmem_numa_idx( cpu_idx ) );

  g_wksp = fd_wksp_new_anonymous( fd_cstr_to_shmem_page_sz( _page_sz ), page_cnt, fd_shmem_cpu_idx( numa_idx ), "bam-fuzz", 16UL );
  FD_TEST( g_wksp );

  bam_fuzz_ctx.tile = bam_fuzz_ctx.tile_storage;
  fd_memset( bam_fuzz_ctx.tile, 0, sizeof( fd_bam_tile_t ) );

  /* Build two outputs: verify (0) and gossip (1) */
  bam_fuzz_setup_out( BAM_FUZZ_OUT_VERIFY, 128UL, BAM_FUZZ_MTU );
  bam_fuzz_setup_out( BAM_FUZZ_OUT_GOSSIP, 64UL, sizeof(fd_bam_contact_update_t) );

  bam_fuzz_ctx.out_verify = (fd_bam_out_ctx_t) {
      .idx    = BAM_FUZZ_OUT_VERIFY,
      .mem    = (fd_wksp_t *)bam_fuzz_ctx.dcaches[ BAM_FUZZ_OUT_VERIFY ],
      .chunk0 = 0UL,
      .chunk  = 0UL,
      .wmark  = fd_dcache_compact_wmark( bam_fuzz_ctx.dcaches[ BAM_FUZZ_OUT_VERIFY ],
                                         bam_fuzz_ctx.dcaches[ BAM_FUZZ_OUT_VERIFY ],
                                         BAM_FUZZ_MTU )
  };

  bam_fuzz_ctx.out_gossip = (fd_bam_out_ctx_t) {
      .idx    = BAM_FUZZ_OUT_GOSSIP,
      .mem    = (fd_wksp_t *)bam_fuzz_ctx.dcaches[ BAM_FUZZ_OUT_GOSSIP ],
      .chunk0 = 0UL,
      .chunk  = 0UL,
      .wmark  = fd_dcache_compact_wmark( bam_fuzz_ctx.dcaches[ BAM_FUZZ_OUT_GOSSIP ],
                                         bam_fuzz_ctx.dcaches[ BAM_FUZZ_OUT_GOSSIP ],
                                         sizeof(fd_bam_contact_update_t) )
  };

  bam_fuzz_ctx.min_cr_avail = ULONG_MAX;
  bam_fuzz_ctx.stem = (fd_stem_context_t) {
      .mcaches             = bam_fuzz_ctx.mcaches,
      .seqs                = bam_fuzz_ctx.seqs,
      .depths              = bam_fuzz_ctx.depths,
      .cr_avail            = bam_fuzz_ctx.cr_avail,
      .min_cr_avail        = &bam_fuzz_ctx.min_cr_avail,
      .cr_decrement_amount = 0UL
  };

  fd_histf_new( bam_fuzz_ctx.rx_delay,
      FD_MHIST_MIN( BAM, MESSAGE_RX_DELAY_NANOS ),
      FD_MHIST_MAX( BAM, MESSAGE_RX_DELAY_NANOS ) );

  bam_fuzz_setup_keyguard();

  initialized = 1;
}

static ulong
bam_fuzz_varint_put( ulong val,
                     uchar * buf,
                     ulong   buf_sz ) {
  ulong idx = 0UL;
  do {
    if( FD_UNLIKELY( idx>=buf_sz ) ) return 0UL;
    uchar byte = (uchar)( val & 0x7fUL );
    val >>= 7;
    if( val ) byte |= 0x80U;
    buf[ idx++ ] = byte;
  } while( val );
  return idx;
}

static ulong
bam_fuzz_build_auth_challenge_payload( uchar selector,
                                       uchar * buf,
                                       ulong   buf_sz ) {
  bam_api_AuthChallengeResponse tmp = bam_api_AuthChallengeResponse_init_default;
  ulong max_len = sizeof( tmp.challenge_to_sign );
  ulong challenge_len;
  switch( selector & 0x3U ) {
  case 0: challenge_len = 0UL; break;                     /* Empty challenge */
  case 1: challenge_len = fd_ulong_sat_sub( max_len, 1UL ); break; /* Max valid size */
  case 2: challenge_len = max_len; break;                 /* Forces decode failure */
  default: challenge_len = fd_ulong_min( max_len/2UL + (ulong)(selector&0x0fU), max_len ); break;
  }

  ulong idx = 0UL;
  if( FD_UNLIKELY( !buf_sz ) ) return 0UL;
  buf[ idx++ ] = 0x0AU; /* tag for field 1, wire type length-delimited */

  uchar varint[ 10 ];
  ulong varint_len = bam_fuzz_varint_put( challenge_len, varint, sizeof( varint ) );
  if( FD_UNLIKELY( !varint_len ) ) return 0UL;
  if( FD_UNLIKELY( idx + varint_len + challenge_len > buf_sz ) ) return 0UL;
  fd_memcpy( buf + idx, varint, varint_len );
  idx += varint_len;

  for( ulong i=0UL; i<challenge_len; i++ ) buf[ idx++ ] = (uchar)( selector + i );
  return idx;
}

static void
bam_fuzz_reset_tile( void ) {
  fd_bam_tile_t * ctx = bam_fuzz_ctx.tile;
  fd_memset( ctx, 0, sizeof( fd_bam_tile_t ) );

  /* Wiring for publish paths */
  ctx->stem       = &bam_fuzz_ctx.stem;
  ctx->verify_out = bam_fuzz_ctx.out_verify;
  ctx->gossip_out = bam_fuzz_ctx.out_gossip;

  bam_fuzz_ctx.seqs[ BAM_FUZZ_OUT_VERIFY ] = 0UL;
  bam_fuzz_ctx.seqs[ BAM_FUZZ_OUT_GOSSIP ] = 0UL;
  bam_fuzz_ctx.cr_avail[ BAM_FUZZ_OUT_VERIFY ] = ULONG_MAX;
  bam_fuzz_ctx.cr_avail[ BAM_FUZZ_OUT_GOSSIP ] = ULONG_MAX;
  bam_fuzz_ctx.min_cr_avail = ULONG_MAX;
  ctx->verify_out.chunk = ctx->verify_out.chunk0;
  ctx->gossip_out.chunk = ctx->gossip_out.chunk0;

  /* Runtime control / fee config */
  fd_memset( &bam_fuzz_ctx.ctrl, 0, sizeof( bam_fuzz_ctx.ctrl ) );
  bam_fuzz_ctx.ctrl.state  = FD_BAM_CTRL_STATE_IDLE;
  bam_fuzz_ctx.ctrl.enable = 1U;

  fd_memset( &bam_fuzz_ctx.fee_cfg, 0, sizeof( bam_fuzz_ctx.fee_cfg ) );
  ctx->ctrl    = &bam_fuzz_ctx.ctrl;
  ctx->fee_cfg = &bam_fuzz_ctx.fee_cfg;

  ctx->metrics.msg_rx_delay[0] = bam_fuzz_ctx.rx_delay[0];
  ctx->keepalive_interval = (long)1e9;
  long now = fd_log_wallclock();
  FD_TEST( fd_rng_new( ctx->rng, 1234U, 0UL ) );
  FD_TEST( fd_keepalive_init( ctx->keepalive, ctx->rng, ctx->keepalive_interval, (long)2e9, now ) );
  ctx->keepalive->ts_last_tx = now;
  ctx->keepalive->ts_last_rx = now;
  ctx->keepalive->ts_deadline = now + ctx->keepalive_interval;
  ctx->keepalive->inflight = 0U;
  fd_memset( ctx->rtt, 0, sizeof( fd_rtt_estimate_t ) );

  /* Default endpoint so runtime control can toggle cleanly */
  static char const default_host[] = "bam.example.com";
  fd_memset( ctx->server_fqdn, 0, sizeof( ctx->server_fqdn ) );
  fd_memcpy( ctx->server_fqdn, default_host, fd_ulong_min( strlen( default_host ), sizeof( ctx->server_fqdn )-1UL ) );
  ctx->server_fqdn_len = (ushort)strlen( ctx->server_fqdn );
  fd_memset( ctx->server_sni, 0, sizeof( ctx->server_sni ) );
  fd_memcpy( ctx->server_sni, default_host, fd_ulong_min( strlen( default_host ), sizeof( ctx->server_sni )-1UL ) );
  ctx->server_sni_len = ctx->server_fqdn_len;
  ctx->server_tcp_port = 443U;
  ctx->is_ssl = 1;

  /* Identity for auth */
  for( ulong i=0UL; i<sizeof( ctx->bam_url_pubkey ); i++ ) ctx->bam_url_pubkey[ i ] = (uchar)( i+1U );
  fd_base58_encode_32( ctx->bam_url_pubkey, NULL, ctx->bam_validator_pubkey );

  /* Keyguard client stubbed with in-memory request/response rings */
  fd_keyguard_client_new( ctx->keyguard_client,
                          bam_fuzz_ctx.key_req_mcache,
                          bam_fuzz_ctx.key_req_dcache,
                          bam_fuzz_ctx.key_resp_mcache,
                          bam_fuzz_ctx.key_resp_dcache,
                          bam_fuzz_ctx.key_req_mtu );
  bam_fuzz_seed_keyguard_response( ctx );

  ctx->keylog_fd             = -1;
  ctx->grpc_buf_max          = 4096UL;
  ctx->tcp_sock              = -1;
  ctx->bundle_status_logged  = (uchar)FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED;
  ctx->bundle_status_recent  = (uchar)FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED;
  ctx->enabled               = 1U;
  ctx->bam_pending_results   = 0U;
  ctx->bundle_max_schedule_slot = FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT;

  /* Assume a valid builder config was fetched so bundle publish paths don't abort */
  ctx->builder_info_valid_until = fd_bam_now() + (long)5e9;
}

static void
bam_fuzz_seed_stream_state( fd_bam_tile_t * ctx,
                            ulong           request_ctx ) {
  if( FD_UNLIKELY( request_ctx==FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream ) ) {
    ctx->bam_stream            = (fd_grpc_h2_stream_t *)ctx; /* sentinel non-NULL */
    ctx->bam_stream_live       = 1U;
    ctx->bam_stream_connecting = 1U;
    ctx->bam_auth_ready        = 1U;
    ctx->bam_auth_challenge_len = 8U;
  } else if( FD_UNLIKELY( request_ctx==FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge ) ) {
    ctx->bam_auth_inflight      = 1U;
    ctx->bam_auth_ready         = 1U;
    ctx->bam_auth_challenge_len = 8U;
  }
}

static void
bam_fuzz_drive_grpc_end( fd_bam_tile_t * ctx,
                         ulong           request_ctx,
                         uchar const *   payload,
                         ulong           payload_sz,
                         uchar           selector ) {
  fd_grpc_resp_hdrs_t resp;
  fd_memset( &resp, 0, sizeof( resp ) );

  static uint const http_status_map[ 4 ] = { 200U, 500U, 403U, 0U };
  resp.h2_status   = http_status_map[ selector & 0x3U ];
  static uint const grpc_status_map[ 4 ] = {
      FD_GRPC_STATUS_OK,
      FD_GRPC_STATUS_UNAUTHENTICATED,
      FD_GRPC_STATUS_PERMISSION_DENIED,
      FD_GRPC_STATUS_UNAVAILABLE
  };
  resp.grpc_status = grpc_status_map[ (selector>>2) & 0x3U ];
  resp.grpc_msg_len = (uint)fd_ulong_min( payload_sz, sizeof( resp.grpc_msg ) );
  fd_memcpy( resp.grpc_msg, payload, resp.grpc_msg_len );

  bam_fuzz_seed_stream_state( ctx, request_ctx );
  fd_bam_client_grpc_rx_end( ctx, request_ctx, &resp );
}

static void
bam_fuzz_drive_timeout( fd_bam_tile_t * ctx,
                        ulong           request_ctx,
                        uchar           selector ) {
  int deadline_kind = (selector & 1U) ? FD_GRPC_DEADLINE_HEADER : FD_GRPC_DEADLINE_RX_END;
  bam_fuzz_seed_stream_state( ctx, request_ctx );
  fd_bam_client_grpc_rx_timeout( ctx, request_ctx, deadline_kind );
}

static void
bam_fuzz_apply_ctrl( uchar enable_flag ) {
  fd_bam_tile_t * ctx = bam_fuzz_ctx.tile;
  ctx->ctrl->enable  = enable_flag ? 1U : 0U;
  ctx->enabled       = ctx->ctrl->enable;
  FD_VOLATILE( ctx->ctrl->state ) = FD_BAM_CTRL_STATE_SUCCESS;
}

static void
bam_fuzz_publish_and_check( _Bool healthy ) {
  fd_bam_tile_t * ctx = bam_fuzz_ctx.tile;
  ulong chunk_before = ctx->gossip_out.chunk;
  fd_bam_publish_gossip_update( ctx, ctx->stem, healthy );
  fd_bam_contact_update_t const * msg =
      (fd_bam_contact_update_t const *)fd_chunk_to_laddr( ctx->gossip_out.mem, chunk_before );

  if( healthy ) {
    FD_TEST( msg->use_bam == FD_BAM_CONTACT_USE_BAM );
    FD_TEST( msg->tpu_addr.addr     == ctx->bam_tpu_addr.addr );
    FD_TEST( msg->tpu_addr.port     == ctx->bam_tpu_addr.port );
    FD_TEST( msg->tpu_fwd_addr.addr == ctx->bam_tpu_fwd_addr.addr );
    FD_TEST( msg->tpu_fwd_addr.port == ctx->bam_tpu_fwd_addr.port );
  } else {
    FD_TEST( msg->use_bam != FD_BAM_CONTACT_USE_BAM );
  }
}

int
LLVMFuzzerInitialize( int *argc,
                      char ***argv ) {
  putenv( "FD_LOG_BACKTRACE=0" );
  fd_boot( argc, argv );
  fd_log_level_core_set( 3 ); /* fail fast on warnings */
  bam_fuzz_env_init( argc, argv );
  atexit( fd_halt );
  return 0;
}

int
LLVMFuzzerTestOneInput( uchar const * data,
                        ulong         size ) {
  if( FD_UNLIKELY( size<1UL ) ) return 0;
  bam_fuzz_reset_tile();

  uchar selector     = data[0];
  uchar msg_kind     = (uchar)( selector & 0x3U );        /* 0=config,1=scheduler,2=auth */
  uchar apply_ctrl   = (uchar)( ( selector>>2 ) & 1U );   /* bit2: drive runtime control */
  uchar enable_ok    = (uchar)( ( selector>>3 ) & 1U );   /* bit3: desired enable state */
  uchar stream_ok    = (uchar)( ( selector>>4 ) & 1U );   /* bit4: mark stream live */
  uchar drive_end    = (uchar)( ( selector>>5 ) & 1U );   /* bit5: drive rx_end */
  uchar drive_to     = (uchar)( ( selector>>6 ) & 1U );   /* bit6: drive timeout */
  uchar structured   = (uchar)( ( selector>>7 ) & 1U );   /* bit7: build structured auth */

  uchar const * payload    = data+1;
  ulong         payload_sz = size-1UL;

  fd_bam_tile_t * ctx = bam_fuzz_ctx.tile;

  if( stream_ok ) {
    ctx->bam_stream_live = 1U;
    ctx->bundle_status_recent = FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED;
  }

  if( apply_ctrl ) {
    bam_fuzz_apply_ctrl( enable_ok );
  }

  ulong request_ctx = FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig;
  if( msg_kind==1 ) request_ctx = FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream;
  else if( msg_kind==2 ) request_ctx = FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge;

  uchar auth_buf[ 512 ];
  if( structured && msg_kind==2 ) {
    ulong enc_sz = bam_fuzz_build_auth_challenge_payload( payload_sz ? payload[0] : 0U, auth_buf, sizeof( auth_buf ) );
    if( enc_sz ) {
      payload    = auth_buf;
      payload_sz = enc_sz;
      ctx->bam_auth_inflight = 1U; /* mirror real request lifecycle */
    }
  }

  switch( msg_kind ) {
  case 0:
    fd_bam_client_grpc_rx_msg( ctx,
                               payload,
                               payload_sz,
                               FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );
    break;
  case 1:
    fd_bam_client_grpc_rx_msg( ctx,
                               payload,
                               payload_sz,
                               FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
    break;
  case 2:
    fd_bam_client_grpc_rx_msg( ctx,
                               payload,
                               payload_sz,
                               FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge );
    break;
  default:
    break;
  }

  if( drive_end && (msg_kind==1U || msg_kind==2U) ) {
    bam_fuzz_drive_grpc_end( ctx, request_ctx, payload, payload_sz, selector );
  }

  if( drive_to && (msg_kind==1U || msg_kind==2U) ) {
    bam_fuzz_drive_timeout( ctx, request_ctx, selector );
  }

  _Bool healthy = (!!ctx->enabled) &&
                  (!!ctx->bam_stream_live) &&
                  (!!ctx->bam_tpu_addr.addr) &&
                  (!!ctx->bam_tpu_fwd_addr.addr);

  bam_fuzz_publish_and_check( healthy );
  return 0;
}
