#define _GNU_SOURCE

#if !FD_HAS_HOSTED
#error "This target requires FD_HAS_HOSTED"
#endif

#include "fd_bam_tile_private.h"
#include "fd_bam_ctrl.h"
#include "../fd_txn_m.h" // FD_TPU_PARSED_MTU
#include "../../flamenco/gossip/fd_gossip_message.h"
#include "../../ballet/nanopb/pb_encode.h"
#include "../../waltz/grpc/fd_grpc_client_private.h"
#include "../../util/net/fd_ip4.h"

#include <stdio.h>
#include <string.h>

char const fdctl_version_string[] = "fuzz";

#define BAM_FUZZ_OUT_VERIFY (0UL)
#define BAM_FUZZ_OUT_GOSSIP (1UL)
#define BAM_FUZZ_OUT_MAX    (2UL)
#define BAM_FUZZ_NOW        (1700000000000000000L)

/* Simple BAM gRPC fuzzer.  The fuzzer drives ConfigResponse, SchedulerResponse,
   and AuthChallengeResponse decode paths while also exercising the runtime
   enable/disable control block.  When the tile is considered healthy and
   enabled, we assert that gossip publishes the TPU sockets learned from
   the BamConfig protobuf. */

static fd_wksp_t * g_wksp;
static long        bam_fuzz_now = BAM_FUZZ_NOW;

static long
bam_fuzz_wallclock( void const * _ ) {
  (void)_;
  return bam_fuzz_now;
}

long
fd_bam_now( void ) {
  return bam_fuzz_now;
}

static struct {
  fd_bam_tile_t * tile; /* Active tile under test */

  fd_bam_out_ctx_t out_verify; /* Verify output ring (MTU = FD_TPU_PARSED_MTU) */
  fd_bam_out_ctx_t out_gossip; /* Gossip output ring for contact-info updates */

  fd_frag_meta_t * mcaches[ BAM_FUZZ_OUT_MAX ];
  void *           mcache_mem[ BAM_FUZZ_OUT_MAX ];
  ulong            depths [ BAM_FUZZ_OUT_MAX ];

  uchar * dcaches[ BAM_FUZZ_OUT_MAX ]; /* Backing dcaches for outputs */
  void *  dcache_mem[ BAM_FUZZ_OUT_MAX ];

  ulong seqs    [ BAM_FUZZ_OUT_MAX ]; /* Current publish seq per out (monotonic) */
  ulong cr_avail[ BAM_FUZZ_OUT_MAX ]; /* Credit counters; seeded to ULONG_MAX */
  int out_reliable[ BAM_FUZZ_OUT_MAX ]; /* Fuzzer outputs use reliable stem accounting */
  ulong min_cr_avail;                 /* Min credit observed (tracks exhaustion) */

  fd_stem_context_t stem; /* Stem wiring for fd_stem_publish */

  fd_bam_ctrl_t     ctrl;    /* Runtime enable/disable switch */
  fd_bam_fee_cfg_t  fee_cfg; /* Shared fee cfg buffer */
  fd_keyswitch_t     keyswitch[1];
  fd_histf_t        builder_heartbeat_arrival_delta[1]; /* Histogram backing for the BuilderHeartBeat arrival-delta metric */
  fd_histf_t        scheduler_pong_send_nanos[1];        /* Histogram backing for scheduler Ping->Pong latency */

  void * pending_txn_mem;
  ulong  pending_txn_max;

  /* Backing storage for fd_grpc_client */
  void * grpc_client_mem;
  ulong  grpc_buf_max;

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
  ulong            key_req_mtu; /* Max sign payload size; kept <= 512 */

  fd_bam_decoded_multi_batch_t decoded_multi_storage[1];
  fd_bam_tile_t tile_storage[1]; /* Backing storage for tile pointer */
} bam_fuzz_ctx;

typedef struct {
  ushort payload_sz;      /* Bytes written into buf (0 when generation failed) */
  uchar  expected_len;    /* Expected decoded challenge length when valid (<256) */
  uchar  start_byte;      /* First byte used to fill challenge_to_sign */
  uchar  expect_decode_ok;/* 1 when decoder should succeed, 0 to force failure */
} bam_fuzz_auth_payload_t;

static uint const bam_fuzz_http_status_map[ 4 ] = { 200U, 401U, 403U, 503U };
static uint const bam_fuzz_grpc_status_map[ 5 ] = {
    FD_GRPC_STATUS_OK,
    FD_GRPC_STATUS_UNAUTHENTICATED,
    FD_GRPC_STATUS_PERMISSION_DENIED,
    FD_GRPC_STATUS_UNAVAILABLE,
    FD_GRPC_STATUS_INVALID_ARGUMENT
};

/* Build a single-producer ring for publishing fragments.
   depth>0 and mtu>0 are required; test aborts if footprint alloc fails. */
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
bam_fuzz_reset_out_storage( ulong out_idx,
                            ulong mtu ) {
  ulong depth = bam_fuzz_ctx.depths[ out_idx ];

  fd_frag_meta_t * mcache = fd_mcache_join( fd_mcache_new( bam_fuzz_ctx.mcache_mem[ out_idx ], depth, 0UL, 0UL ) );
  FD_TEST( mcache );
  bam_fuzz_ctx.mcaches[ out_idx ] = mcache;

  ulong dcache_data_sz = fd_dcache_req_data_sz( mtu, depth, 1UL, 1 );
  uchar * dcache = fd_dcache_join( fd_dcache_new( bam_fuzz_ctx.dcache_mem[ out_idx ], dcache_data_sz, 0UL ) );
  FD_TEST( dcache );
  bam_fuzz_ctx.dcaches[ out_idx ] = dcache;
}

static fd_bam_out_ctx_t
bam_fuzz_out_ctx( ulong out_idx,
                  ulong mtu ) {
  uchar * dcache = bam_fuzz_ctx.dcaches[ out_idx ];
  return (fd_bam_out_ctx_t) {
      .idx    = out_idx,
      .mem    = (fd_wksp_t *)dcache,
      .chunk0 = 0UL,
      .chunk  = 0UL,
      .wmark  = fd_dcache_compact_wmark( dcache, dcache, mtu )
  };
}

/* Always leave the response mcache "ready" so fd_keyguard_client_sign
   returns immediately instead of hanging the fuzzer. Response chunk is
   always 64 bytes of zeroed data. */
static void
bam_fuzz_seed_keyguard_response( fd_bam_tile_t * ctx ) {
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

/* Build in-memory request/response rings for keyguard. Depth 1 is
   sufficient because the fuzzer never pipelines signatures. */
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
bam_fuzz_reset_reusable_storage( void ) {
  bam_fuzz_reset_out_storage( BAM_FUZZ_OUT_VERIFY, FD_TPU_PARSED_MTU );
  bam_fuzz_reset_out_storage( BAM_FUZZ_OUT_GOSSIP, sizeof(fd_bam_contact_update_t) );

  bam_fuzz_ctx.out_verify = bam_fuzz_out_ctx( BAM_FUZZ_OUT_VERIFY, FD_TPU_PARSED_MTU );
  bam_fuzz_ctx.out_gossip = bam_fuzz_out_ctx( BAM_FUZZ_OUT_GOSSIP, sizeof(fd_bam_contact_update_t) );

  bam_fuzz_ctx.key_req_mcache = fd_mcache_join( fd_mcache_new( bam_fuzz_ctx.key_req_mcache_mem,
                                                               bam_fuzz_ctx.key_req_depth,
                                                               0UL,
                                                               0UL ) );
  FD_TEST( bam_fuzz_ctx.key_req_mcache );
  bam_fuzz_ctx.key_resp_mcache = fd_mcache_join( fd_mcache_new( bam_fuzz_ctx.key_resp_mcache_mem,
                                                                bam_fuzz_ctx.key_resp_depth,
                                                                0UL,
                                                                0UL ) );
  FD_TEST( bam_fuzz_ctx.key_resp_mcache );

  ulong req_dcache_data_sz = fd_dcache_req_data_sz( bam_fuzz_ctx.key_req_mtu, bam_fuzz_ctx.key_req_depth, 1UL, 1 );
  bam_fuzz_ctx.key_req_dcache = fd_dcache_join( fd_dcache_new( bam_fuzz_ctx.key_req_dcache_mem, req_dcache_data_sz, 0UL ) );
  FD_TEST( bam_fuzz_ctx.key_req_dcache );
  ulong resp_dcache_data_sz = fd_dcache_req_data_sz( 64UL, bam_fuzz_ctx.key_resp_depth, 1UL, 1 );
  bam_fuzz_ctx.key_resp_dcache = fd_dcache_join( fd_dcache_new( bam_fuzz_ctx.key_resp_dcache_mem, resp_dcache_data_sz, 0UL ) );
  FD_TEST( bam_fuzz_ctx.key_resp_dcache );

  fd_memset( bam_fuzz_ctx.pending_txn_mem, 0, bam_pending_txn_footprint( bam_fuzz_ctx.pending_txn_max ) );
  fd_memset( bam_fuzz_ctx.grpc_client_mem, 0, fd_grpc_client_footprint( bam_fuzz_ctx.grpc_buf_max ) );

  fd_histf_new( bam_fuzz_ctx.builder_heartbeat_arrival_delta,
      FD_MHIST_MIN( BAM, BUILDER_HEARTBEAT_ARRIVAL_DELTA_NANOS ),
      FD_MHIST_MAX( BAM, BUILDER_HEARTBEAT_ARRIVAL_DELTA_NANOS ) );
  fd_histf_new( bam_fuzz_ctx.scheduler_pong_send_nanos,
      FD_MHIST_MIN( BAM, SCHEDULER_PONG_SEND_NANOS ),
      FD_MHIST_MAX( BAM, SCHEDULER_PONG_SEND_NANOS ) );
}

/* One-time environment bring-up: allocate workspace, create output rings,
   initialize metrics backing, and wire the dummy keyguard channels. */
static void
bam_fuzz_env_init( int *    pargc,
                   char *** pargv ) {
  static int initialized = 0;
  if( FD_LIKELY( initialized ) ) return;

  fd_memset( &bam_fuzz_ctx, 0, sizeof( bam_fuzz_ctx ) );

  ulong cpu_idx = fd_tile_cpu_id( fd_tile_idx() );
  if( cpu_idx>fd_shmem_cpu_cnt() ) cpu_idx = 0UL;

  /* Allow basic workspace sizing overrides via CLI; defaults are modest, so
     the fuzzer can run in constrained sandboxes. */
  char const * _page_sz = fd_env_strip_cmdline_cstr ( pargc, pargv, "--page-sz",  NULL, "normal"                     );
  ulong        page_cnt = fd_env_strip_cmdline_ulong( pargc, pargv, "--page-cnt", NULL, 256UL                        );
  ulong        numa_idx = fd_env_strip_cmdline_ulong( pargc, pargv, "--numa-idx", NULL, fd_shmem_numa_idx( cpu_idx ) );

  g_wksp = fd_wksp_new_anonymous( fd_cstr_to_shmem_page_sz( _page_sz ), page_cnt, fd_shmem_cpu_idx( numa_idx ), "bam-fuzz", 16UL );
  FD_TEST( g_wksp );

  bam_fuzz_ctx.tile = bam_fuzz_ctx.tile_storage;
  fd_memset( bam_fuzz_ctx.tile, 0, sizeof( fd_bam_tile_t ) );
  bam_fuzz_ctx.tile->decoded_multi = bam_fuzz_ctx.decoded_multi_storage;
  bam_fuzz_ctx.tile->bam_leader_state.slot = ULONG_MAX;

  /* Build two outputs: verify (0) and gossip (1) */
  bam_fuzz_setup_out( BAM_FUZZ_OUT_VERIFY, 128UL, FD_TPU_PARSED_MTU);
  bam_fuzz_setup_out( BAM_FUZZ_OUT_GOSSIP, 64UL, sizeof(fd_bam_contact_update_t) );

  bam_fuzz_ctx.pending_txn_max = bam_fuzz_ctx.depths[ BAM_FUZZ_OUT_VERIFY ];
  bam_fuzz_ctx.pending_txn_mem = fd_wksp_alloc_laddr( g_wksp,
                                                       bam_pending_txn_align(),
                                                       bam_pending_txn_footprint( bam_fuzz_ctx.pending_txn_max ),
                                                       1UL );
  FD_TEST( bam_fuzz_ctx.pending_txn_mem );

  bam_fuzz_ctx.out_verify = bam_fuzz_out_ctx( BAM_FUZZ_OUT_VERIFY, FD_TPU_PARSED_MTU );
  bam_fuzz_ctx.out_gossip = bam_fuzz_out_ctx( BAM_FUZZ_OUT_GOSSIP, sizeof(fd_bam_contact_update_t) );

  /* Metrics macros expect fd_metrics_tl to point at a formatted buffer.
     Create a minimal metrics region so timeout/error paths can update counters. */
  void * metrics_mem = fd_wksp_alloc_laddr( g_wksp, FD_METRICS_ALIGN, FD_METRICS_FOOTPRINT( 0UL ), 1UL );
  FD_TEST( metrics_mem );
  fd_metrics_new( metrics_mem, 0UL );
  fd_metrics_register( metrics_mem );

  bam_fuzz_ctx.min_cr_avail = ULONG_MAX;
  for( ulong out_idx=0UL; out_idx<BAM_FUZZ_OUT_MAX; out_idx++ ) {
    bam_fuzz_ctx.out_reliable[ out_idx ] = 1;
  }
  bam_fuzz_ctx.stem = (fd_stem_context_t) {
      .mcaches             = bam_fuzz_ctx.mcaches,
      .seqs                = bam_fuzz_ctx.seqs,
      .depths              = bam_fuzz_ctx.depths,
      .cr_avail            = bam_fuzz_ctx.cr_avail,
      .min_cr_avail        = &bam_fuzz_ctx.min_cr_avail,
      .cr_decrement_amount = 0UL,
      .out_reliable        = bam_fuzz_ctx.out_reliable
  };

  fd_histf_new( bam_fuzz_ctx.builder_heartbeat_arrival_delta,
      FD_MHIST_MIN( BAM, BUILDER_HEARTBEAT_ARRIVAL_DELTA_NANOS ),
      FD_MHIST_MAX( BAM, BUILDER_HEARTBEAT_ARRIVAL_DELTA_NANOS ) );
  fd_histf_new( bam_fuzz_ctx.scheduler_pong_send_nanos,
      FD_MHIST_MIN( BAM, SCHEDULER_PONG_SEND_NANOS ),
      FD_MHIST_MAX( BAM, SCHEDULER_PONG_SEND_NANOS ) );

  bam_fuzz_setup_keyguard();

  bam_fuzz_ctx.grpc_buf_max = 4096UL;
  ulong grpc_foot = fd_grpc_client_footprint( bam_fuzz_ctx.grpc_buf_max );
  bam_fuzz_ctx.grpc_client_mem = fd_wksp_alloc_laddr( g_wksp, fd_grpc_client_align(), grpc_foot, 1UL );
  FD_TEST( bam_fuzz_ctx.grpc_client_mem );

  initialized = 1;
}

static void
bam_fuzz_enqueue_results( fd_bam_tile_t * ctx,
                          uchar const *   data,
                          ulong           size ) {
  /* Enqueue up to two small results to exercise both committed and not-committed
     SchedulerMessage encoders without exceeding gRPC buffer limits. */

  uchar seed0 = (uchar)( size ? data[0] : 0U );
  uchar seed1 = (uchar)( size>1 ? data[1] : (uchar)(seed0^0x5aU) );

  /* 1) Committed bundle */
  {
    fd_bam_bundle_result_t res = {0};
    res.seq_id            = (uint)seed0;
    res.scheduler_gen     = ctx->scheduler_gen;
    res.slot              = (ulong)seed0;
    res.bundle_txn_cnt    = (uchar)( 1U + ( seed0 % FD_PACK_MAX_TXN_PER_BUNDLE ) );
    res.execution_success = 1U;
    for( uchar i=0U; i<res.bundle_txn_cnt; i++ ) {
      res.sanitize_success[ i ] = 1U;
      res.consumed_cus    [ i ] = (seed0 + i) & 0xffU;
    }
    fd_bam_enqueue_result( ctx, &res );
  }

  /* 2) Not-committed bundle (vary reason) */
  {
    fd_bam_bundle_result_t res = {0};
    res.seq_id            = seed0 | ((uint)seed1<<8);
    res.scheduler_gen     = ctx->scheduler_gen;
    res.slot              = (ulong)seed1;
    res.bundle_txn_cnt    = (uchar)( 1U + ( seed1 % FD_PACK_MAX_TXN_PER_BUNDLE ) );

    uchar reason_sel = (uchar)( (seed1>>3) & 0x3U );
    if( reason_sel==0U ) {
      res.scheduling_error = FD_BAM_SCHED_ERR_POH_TIMEOUT;
    } else if( reason_sel==1U ) {
      res.bundle_err   = FD_BAM_BUNDLE_ERR_DESER;
      res.deser_index  = (uchar)(seed1 & 0x7U);
      res.deser_reason = (uchar)bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
    } else if( reason_sel==2U ) {
      res.sanitize_success[ 0 ] = 0U;
    } else {
      fd_bam_result_add_txn_error( &res, 0UL, bam_types_TransactionErrorReason_ACCOUNT_NOT_FOUND );
      res.sanitize_success[ 0 ] = 1U;
    }

    for( uchar i=0U; i<res.bundle_txn_cnt; i++ ) {
      res.consumed_cus[ i ] = (seed1 + i) & 0xffU;
      if( i ) {
        res.sanitize_success[ i ] = 1U;
      }
    }
    fd_bam_enqueue_result( ctx, &res );
  }
}

static void
bam_fuzz_prepare_connected_grpc( fd_bam_tile_t * ctx ) {
  ctx->grpc_client->h2_hs_done  = 1U;
  ctx->grpc_client->ssl_hs_done = 1U;
  fd_h2_conn_t * conn = fd_grpc_client_h2_conn( ctx->grpc_client );
  if( conn ) {
    conn->flags = 0U;
    conn->peer_settings.max_concurrent_streams = FD_GRPC_CLIENT_MAX_STREAMS;
  }
  ctx->tcp_sock_connected = 1;

  long now = fd_bam_now();
  ctx->keepalive->ts_next_tx  = now + ctx->keepalive_interval;
  ctx->keepalive->ts_last_tx  = now;
  ctx->keepalive->ts_last_rx  = now;
  ctx->keepalive->ts_deadline = now + ctx->keepalive_interval;
  ctx->keepalive->inflight    = 0U;
}

static void
bam_fuzz_clear_grpc_tx( fd_bam_tile_t * ctx ) {
  fd_h2_rbuf_t * rbuf_tx = fd_grpc_client_rbuf_tx( ctx->grpc_client );
  fd_h2_rbuf_init( rbuf_tx, rbuf_tx->buf0, rbuf_tx->bufsz );
  ctx->grpc_client->request_stream = NULL;
  *ctx->grpc_client->request_tx_op = (fd_h2_tx_op_t){0};
}

static void
bam_fuzz_open_scheduler_stream( fd_bam_tile_t * ctx,
                                uchar const *   data,
                                ulong           size ) {
  bam_fuzz_prepare_connected_grpc( ctx );
  if( FD_UNLIKELY( !ctx->bam_stream ) ) {
    uchar seed = size ? data[0] : 0U;
    ulong len = fd_ulong_min( (ulong)( seed & 0x3fU ), sizeof( ctx->challenge_to_sign )-1UL );
    for( ulong i=0UL; i<len; i++ ) ctx->challenge_to_sign[ i ] = (char)('A' + (seed % 26U));
    ctx->challenge_to_sign[ len ] = '\0';
    fd_cstr_ncpy( ctx->bam_auth_signature, "1111111111111111111111111111111111", sizeof( ctx->bam_auth_signature ) );
    ctx->bam_auth_ready    = 1U;
    ctx->bam_auth_inflight = 0U;

    bam_fuzz_clear_grpc_tx( ctx );
    (void)fd_bam_test_client_step_reconnect( ctx, fd_bam_now() );
  }

  if( FD_LIKELY( ctx->bam_stream && !ctx->bam_stream_live ) ) {
    fd_bam_client_grpc_rx_start( ctx, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  }
}

static void
bam_fuzz_exercise_outbound( fd_bam_tile_t * ctx,
                            uchar const *   data,
                            ulong           size ) {
  if( FD_UNLIKELY( !FD_VOLATILE_CONST( ctx->enabled ) ) ) return;

  bam_fuzz_open_scheduler_stream( ctx, data, size );

  if( FD_UNLIKELY( !ctx->bam_stream || !ctx->bam_stream_live ) ) return;

  /* Discard any queued frames so stream sends don't get stuck behind a full TX ring. */
  bam_fuzz_clear_grpc_tx( ctx );

  /* Force a heartbeat, a leader-state update, and a couple bundle results. */
  long now = fd_bam_now();
  ctx->bam_last_validator_heartbeat_ns = now - (long)10e9;

  ctx->bam_leader_state = (fd_bam_leader_state_t){
    .slot                    = (ulong)( size>2 ? data[2] : 0U ),
    .tick                    = (ushort)( size>3 ? data[3] : 0U ),
    .slot_cu_budget_remaining = (uint)( size>4 ? data[4] : 0U ),
  };
  ctx->bam_leader_pending = 1U;

  bam_fuzz_enqueue_results( ctx, data, size );
  (void)fd_bam_test_client_step_reconnect( ctx, now );
}

/* Synthesize an AuthChallengeResponse payload. selector drives length and
   truncation patterns to cover decode successes and failures. buf must be
   large enough for tag+varint+payload (<=sizeof(challenge_to_sign)). */
static bam_fuzz_auth_payload_t
bam_fuzz_build_auth_challenge_payload( uchar selector,
                                       uchar * buf,
                                       ulong   buf_sz ) {
  bam_fuzz_auth_payload_t info = {0};
  bam_api_AuthChallengeResponse tmp = bam_api_AuthChallengeResponse_init_default;
  uchar case_id = selector & 0x7U;
  if( case_id==3U ) { /* Truncated varint to force decode failure */
    if( FD_UNLIKELY( buf_sz<2UL ) ) return info;
    buf[0] = 0x0AU; /* tag */
    buf[1] = 0x80U; /* unterminated length varint */
    info.payload_sz       = 2U;
    info.expect_decode_ok = 0U;
    info.start_byte       = selector;
    return info;
  }

  uchar challenge_len;
  switch( case_id & 0x3U ) {
  case 0: challenge_len = 0UL; info.expect_decode_ok = 0; break;                     /* Empty challenge */
  case 1: challenge_len = (uchar)fd_ulong_sat_sub( sizeof( tmp.challenge_to_sign ), 1UL ); info.expect_decode_ok = 1; break; /* Max valid size */
  case 2: challenge_len = sizeof( tmp.challenge_to_sign ); info.expect_decode_ok = 0; break;                 /* Forces decode failure / NUL check */
  default: challenge_len = fd_uchar_min( sizeof( tmp.challenge_to_sign )/2 + (selector&0x0fU), sizeof( tmp.challenge_to_sign )-1UL ); info.expect_decode_ok = 1; break;
  }

  uchar fill = (uchar)( selector | 0x1U );
  ushort idx = 0UL;
  if( FD_UNLIKELY( !buf_sz ) ) return info;
  buf[ idx++ ] = 0x0AU; /* tag for field 1, wire type length-delimited */

  pb_byte_t varint[ 10 ];
  pb_ostream_t varint_stream = pb_ostream_from_buffer( varint, sizeof( varint ) );
  if( FD_UNLIKELY( !pb_encode_varint( &varint_stream, challenge_len ) ) ) return info;
  if( FD_UNLIKELY( idx + varint_stream.bytes_written + challenge_len > buf_sz ) ) return info;
  fd_memcpy( buf + idx, varint, varint_stream.bytes_written );
  idx += (uchar)varint_stream.bytes_written;

  for( ulong i=0UL; i<challenge_len; i++ ) buf[ idx++ ] = fill;
  info.payload_sz    = idx;
  info.expected_len  = info.expect_decode_ok ? challenge_len : 0U;
  info.start_byte    = fill;
  return info;
}

static void bam_fuzz_assert_auth_cleared( fd_bam_tile_t * ctx );

/* Verify auth state mirrors the decoded payload when a structured auth
   message was generated. Expects bam_auth_ready=1 with matching challenge
   contents when decode succeeds; otherwise expects cleared auth state. */
static void
bam_fuzz_assert_auth_state( fd_bam_tile_t *          ctx,
                            bam_fuzz_auth_payload_t  info ) {
  if( FD_UNLIKELY( !info.payload_sz ) ) return;

  if( FD_UNLIKELY( !info.expect_decode_ok ) ) {
    bam_fuzz_assert_auth_cleared( ctx );
    return;
  }

  FD_TEST( ctx->bam_auth_ready==1U );
  FD_TEST( ctx->bam_auth_inflight==0U );
  ulong challenge_len = strnlen( ctx->challenge_to_sign, sizeof( ctx->challenge_to_sign ) );
  FD_TEST( challenge_len==info.expected_len );
  /* Generator fills challenge_to_sign with a single repeated byte; ensure decode preserved it. */
  for( ulong i=0UL; i<info.expected_len; i++ ) {
    FD_TEST( ((uchar)ctx->challenge_to_sign[ i ])==info.start_byte );
  }
}

static void
bam_fuzz_assert_auth_cleared( fd_bam_tile_t * ctx ) {
  /* Assert auth bookkeeping is cleared (ready=0, inflight=0, empty challenge). */
  FD_TEST( ctx->bam_auth_ready==0U );
  FD_TEST( ctx->challenge_to_sign[ 0 ]=='\0' );
  FD_TEST( ctx->bam_auth_inflight==0U );
}

/* Reinitialize tile state for each fuzz input. Populates default endpoints,
   seeds keepalive/rng, and wires ctrl/fee_cfg/outputs to local buffers. */
static void
bam_fuzz_reset_tile( void ) {
  fd_bam_tile_t * ctx = bam_fuzz_ctx.tile;
  fd_memset( ctx, 0, sizeof( fd_bam_tile_t ) );
  bam_fuzz_reset_reusable_storage();
  ctx->decoded_multi = bam_fuzz_ctx.decoded_multi_storage;

  fd_memset( bam_fuzz_ctx.keyswitch, 0, sizeof( bam_fuzz_ctx.keyswitch ) );
  bam_fuzz_ctx.keyswitch->magic = FD_KEYSWITCH_MAGIC;
  bam_fuzz_ctx.keyswitch->state = FD_KEYSWITCH_STATE_COMPLETED;
  ctx->keyswitch = bam_fuzz_ctx.keyswitch;

  ctx->bam_leader_state.slot  = ULONG_MAX;

  /* Wiring for publish paths */
  ctx->stem       = &bam_fuzz_ctx.stem;
  ctx->verify_out = bam_fuzz_ctx.out_verify;
  ctx->gossip_out = bam_fuzz_ctx.out_gossip;
  ctx->pending_txns = bam_pending_txn_join( bam_pending_txn_new( bam_fuzz_ctx.pending_txn_mem,
                                                                 bam_fuzz_ctx.pending_txn_max ) );
  FD_TEST( ctx->pending_txns );

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

  ctx->metrics.builder_heartbeat_arrival_delta_nanos[0] = bam_fuzz_ctx.builder_heartbeat_arrival_delta[0];
  ctx->metrics.scheduler_pong_send_nanos[0]              = bam_fuzz_ctx.scheduler_pong_send_nanos[0];
  ctx->keepalive_interval = (long)1e9;
  long now = fd_log_wallclock();
  FD_TEST( fd_rng_new( ctx->rng, 1234U, 0UL ) );
  FD_TEST( fd_keepalive_init( ctx->keepalive, ctx->rng, ctx->keepalive_interval, (long)2e9, now ) );
  ctx->keepalive->ts_last_tx = now;
  ctx->keepalive->ts_last_rx = now;
  ctx->keepalive->ts_deadline = now + ctx->keepalive_interval;
  ctx->keepalive->inflight = 0U;
  fd_memset( ctx->rtt, 0, sizeof( fd_rtt_estimate_t ) );

  ctx->default_tpu     = (fd_ip4_port_t){ .addr = FD_IP4_ADDR( 127, 0, 0, 1 ), .port = fd_ushort_bswap( (ushort)9007U ) };
  ctx->default_tpu_fwd = (fd_ip4_port_t){ .addr = FD_IP4_ADDR( 127, 0, 0, 1 ), .port = fd_ushort_bswap( (ushort)9008U ) };

  /* Default endpoint so runtime control can toggle cleanly */
  static char const default_host[] = "bam.example.com";
  fd_cstr_ncpy( ctx->server_fqdn, default_host, sizeof( ctx->server_fqdn ) );
  ctx->server_fqdn_len = (ushort)fd_cstr_nlen( ctx->server_fqdn, sizeof( ctx->server_fqdn ) );
  fd_cstr_ncpy( ctx->server_sni, default_host, sizeof( ctx->server_sni ) );
  ctx->server_sni_len = ctx->server_fqdn_len;
  ctx->server_tcp_port = 443U;
  ctx->is_ssl = 1;

  /* gRPC client in "connected" state to exercise outbound encode paths */
  ctx->grpc_buf_max      = bam_fuzz_ctx.grpc_buf_max;
  ctx->grpc_client_mem   = bam_fuzz_ctx.grpc_client_mem;
  ctx->map_seed          = 1UL;
  ctx->grpc_client = fd_grpc_client_new( ctx->grpc_client_mem,
                                         &fd_bam_client_grpc_callbacks,
                                         ctx->grpc_metrics,
                                         ctx,
                                         ctx->grpc_buf_max,
                                         ctx->map_seed );
  FD_TEST( ctx->grpc_client );
  fd_grpc_client_set_version( ctx->grpc_client, fdctl_version_string, strlen( fdctl_version_string ) );
  fd_grpc_client_set_authority( ctx->grpc_client, ctx->server_sni, ctx->server_sni_len, ctx->server_tcp_port );
  bam_fuzz_prepare_connected_grpc( ctx );

  /* Identity for auth */
  for( ulong i=0UL; i<sizeof( ctx->bam_identity_pubkey ); i++ ) ctx->bam_identity_pubkey[ i ] = (uchar)( i+1U );
  fd_base58_encode_32( ctx->bam_identity_pubkey, NULL, ctx->bam_identity_pubkey_b58 );

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
  ctx->bam_status_logged    = FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED;
  ctx->bam_status_recent    = FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED;
  ctx->enabled               = 1U;

  /* Assume a valid builder config was fetched so bundle publish paths don't abort */
  long bam_now = fd_bam_now();
  ctx->builder_info_valid_until = bam_now + (long)5e9;
  ctx->bam_last_config_poll_ns  = bam_now;
}

/* Pre-mark request lifecycle state so rx_end/rx_timeout can exercise
   cleanup paths. request_ctx selects which inflight flags to raise. */
static void
bam_fuzz_seed_stream_state( fd_bam_tile_t * ctx,
                            ulong           request_ctx ) {
  if( FD_UNLIKELY( request_ctx==FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge ) ) {
    ctx->bam_auth_inflight      = 1U;
    ctx->bam_auth_ready         = 0U;
    ctx->challenge_to_sign[ 0 ] = '\0';
  } else if( FD_UNLIKELY( request_ctx==FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig ) ) {
    ctx->bam_config_inflight = 1U;
  }
}

/* Deliver a terminal gRPC response to the client handler. Carries the raw
   payload as the grpc_msg body and the supplied HTTP/gRPC status codes. */
static void
bam_fuzz_drive_grpc_end( fd_bam_tile_t * ctx,
                         ulong           request_ctx,
                         uchar const *   payload,
                         ulong           payload_sz,
                         uint            http_status,
                         uint            grpc_status ) {
  fd_grpc_resp_hdrs_t resp;
  fd_memset( &resp, 0, sizeof( resp ) );

  resp.h2_status   = http_status;
  resp.grpc_status = grpc_status;
  resp.grpc_msg_len = (uint)fd_ulong_min( payload_sz, sizeof( resp.grpc_msg ) );
  fd_memcpy( resp.grpc_msg, payload, resp.grpc_msg_len );

  if( FD_UNLIKELY( request_ctx==FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream &&
                   grpc_status==FD_GRPC_STATUS_INVALID_ARGUMENT ) ) {
    ulong rejected_slot  = payload_sz ? (ulong)payload[0] : 7UL;
    ulong valid_min_slot = rejected_slot + 1UL;
    ulong valid_max_slot = rejected_slot + 8UL;
    ctx->bam_leader_state = (fd_bam_leader_state_t){
      .slot                    = rejected_slot,
      .tick                    = 1U,
      .slot_cu_budget_remaining = 1U,
      .slot_end_ns             = fd_bam_now() + (long)1e9
    };
    ctx->bam_leader_pending = 1U;
    snprintf( resp.grpc_msg,
              sizeof( resp.grpc_msg ),
              "Leader state slot %lu is outside of valid range %lu..=%lu",
              rejected_slot,
              valid_min_slot,
              valid_max_slot );
    resp.grpc_msg_len = (uint)strnlen( resp.grpc_msg, sizeof( resp.grpc_msg ) );
  }

  fd_bam_client_grpc_rx_end( ctx, request_ctx, &resp );
}

static void
bam_fuzz_apply_ctrl_request( fd_bam_tile_t * ctx,
                             uchar           ctrl_selector,
                             uchar           enable_ok ) {
  fd_bam_ctrl_t * ctrl = ctx->ctrl;
  uchar ctrl_case = (uchar)( ctrl_selector & 0x3U );

  char old_host[ FD_FQDN_BUF_MAX ];
  char old_sni [ FD_SNI_BUF_MAX  ];
  ushort old_port    = 0U;
  uchar  old_ssl     = 0U;
  uchar  old_enabled = 0U;
  if( ctrl_case==3U ) {
    fd_cstr_ncpy( old_host, ctx->server_fqdn, sizeof( old_host ) );
    fd_cstr_ncpy( old_sni,  ctx->server_sni,  sizeof( old_sni  ) );
    old_port    = ctx->server_tcp_port;
    old_ssl     = ctx->is_ssl;
    old_enabled = ctx->enabled;
  }

  ctrl->enable = !!enable_ok;

  switch( ctrl_case ) {
  case 0U: /* enable-only */
    ctrl->command = FD_BAM_CTRL_CMD_ENABLE;
    break;
  case 1U: /* valid URL, optional explicit SNI */
    ctrl->command = FD_BAM_CTRL_CMD_ENABLE | FD_BAM_CTRL_CMD_URL;
    fd_cstr_ncpy( ctrl->url, "http://new.example.com:8899", sizeof( ctrl->url ) );
    if( ctrl_selector & 0x4U ) {
      ctrl->command |= FD_BAM_CTRL_CMD_SNI;
      fd_cstr_ncpy( ctrl->sni, "custom.sni.invalid", sizeof( ctrl->sni ) );
    }
    break;
  case 2U: /* blank URL clears endpoint and disables */
    ctrl->command = FD_BAM_CTRL_CMD_URL;
    fd_cstr_ncpy( ctrl->url, "   \t\n", sizeof( ctrl->url ) );
    break;
  default: /* invalid URL should fail without mutating config */
    ctrl->command = FD_BAM_CTRL_CMD_URL;
    fd_cstr_ncpy( ctrl->url, "not a url", sizeof( ctrl->url ) );
    break;
  }

  FD_VOLATILE( ctrl->state ) = FD_BAM_CTRL_STATE_REQUEST;
  fd_bam_tile_housekeeping( ctx );

  FD_TEST( ctrl->state == (uchar)( ctrl_case==3U ? FD_BAM_CTRL_STATE_ERROR : FD_BAM_CTRL_STATE_SUCCESS ) );
  switch( ctrl_case ) {
  case 0U:
    FD_TEST( ctx->enabled == !!enable_ok );
    break;
  case 1U: {
    char const * expected_sni = ( ctrl_selector & 0x4U ) ? "custom.sni.invalid" : "new.example.com";
    FD_TEST( ctx->enabled == !!enable_ok );
    FD_TEST( strcmp( ctx->server_fqdn, "new.example.com" )==0 );
    FD_TEST( ctx->server_tcp_port == 8899U );
    FD_TEST( ctx->is_ssl == 0U );
    FD_TEST( strcmp( ctx->server_sni, expected_sni )==0 );
    break;
  }
  case 2U:
    FD_TEST( ctx->enabled == 0U );
    FD_TEST( ctx->server_fqdn[0] == '\0' );
    FD_TEST( ctx->server_tcp_port == 0U );
    break;
  default:
    FD_TEST( ctx->enabled == old_enabled );
    FD_TEST( strcmp( ctx->server_fqdn, old_host )==0 );
    FD_TEST( strcmp( ctx->server_sni, old_sni )==0 );
    FD_TEST( ctx->server_tcp_port == old_port );
    FD_TEST( ctx->is_ssl == old_ssl );
    break;
  }

  bam_fuzz_prepare_connected_grpc( ctx );
}

/* Publish a gossip update and assert the emitted fields match the tile
   state. Requires both TPU addresses/ports to match */
static void
bam_fuzz_publish_and_check(_Bool use_bam) {
  fd_bam_tile_t * ctx = bam_fuzz_ctx.tile;
  ulong chunk_before = ctx->gossip_out.chunk;
  FD_TEST( fd_bam_gossip_update( ctx, ctx->stem, use_bam ) );
  fd_bam_contact_update_t const * msg = fd_chunk_to_laddr( ctx->gossip_out.mem, chunk_before );

  fd_ip4_port_t expected_tpu     = use_bam ? ctx->bam_tpu     : ctx->default_tpu;
  fd_ip4_port_t expected_tpu_fwd = use_bam ? ctx->bam_tpu_fwd : ctx->default_tpu_fwd;

  FD_TEST( msg->tpu.l     == expected_tpu.l );
  FD_TEST( msg->tpu_fwd.l == expected_tpu_fwd.l );
  FD_TEST( msg->version_client_id == (ushort)( use_bam ? FD_GOSSIP_CONTACT_INFO_CLIENT_BAM : FD_GOSSIP_CONTACT_INFO_CLIENT_FIREDANCER ) );
}

/* Standard libFuzzer entry: disable backtraces, boot Firedancer core, and
   allocate the fuzz workspace once. */
int
LLVMFuzzerInitialize( int *argc,
                      char ***argv ) {
  putenv( "FD_LOG_BACKTRACE=0" );
  putenv( "FD_LOG_COLORIZE=0" );
  putenv( "FD_LOG_DEDUP=0" );
  fd_log_wallclock_set( bam_fuzz_wallclock, NULL );
  fd_boot( argc, argv );
  fd_log_level_core_set( 4 ); /* fail fast on errors */
  bam_fuzz_env_init( argc, argv );
  atexit( fd_halt );
  return 0;
}

/* Split the input into control byte + status selector + raw payload. The
   control byte drives which RPC is decoded and which callbacks fire. */
int
LLVMFuzzerTestOneInput( uchar const * data,
                        ulong         size ) {
  if( FD_UNLIKELY( size<1UL ) ) return 0;
  bam_fuzz_now = BAM_FUZZ_NOW;
  bam_fuzz_reset_tile();

  uchar selector     = data[0];
  uchar msg_kind     = (uchar)( selector & 0x3U );        /* 0=config,1=scheduler,2=auth,3=no inbound RPC */
  uchar apply_ctrl   = (uchar)( ( selector>>2 ) & 1U );   /* bit2: drive runtime control */
  uchar enable_ok    = (uchar)( ( selector>>3 ) & 1U );   /* bit3: desired enable state */
  uchar stream_ok    = (uchar)( ( selector>>4 ) & 1U );   /* bit4: open scheduler stream */
  uchar drive_end    = (uchar)( ( selector>>5 ) & 1U );   /* bit5: drive rx_end */
  uchar drive_to     = (uchar)( ( selector>>6 ) & 1U );   /* bit6: drive timeout */
  uchar structured   = (uchar)( ( selector>>7 ) & 1U );   /* bit7: build structured auth */
  uchar status_selector = size>1 ? data[1] : (uchar)( selector ^ 0x5a ); /* Second byte chooses status; fallback mixes bits for small inputs */
  uint  http_status  = bam_fuzz_http_status_map[ status_selector & 0x3 ];      /* Maps into {200,401,403,503} */
  uint  grpc_status  = bam_fuzz_grpc_status_map[
      ((ulong)status_selector>>2) % (sizeof(bam_fuzz_grpc_status_map)/sizeof(bam_fuzz_grpc_status_map[0])) ];

  uchar const * payload    = size>1 ? data+2 : data+1;
  ulong         payload_sz = size>1 ? size-2 : size-1;
  bam_fuzz_auth_payload_t auth_info = {0};
  uchar structured_ping = 0U;
  uchar expect_bam_gossip = 0U;

  fd_bam_tile_t * ctx = bam_fuzz_ctx.tile;

  uchar has_rpc = (uchar)( msg_kind!=3U );
  ulong request_ctx = FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig;
  if( msg_kind==1 ) request_ctx = FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream;
  else if( msg_kind==2 ) request_ctx = FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge;

  if( apply_ctrl ) {
    uchar ctrl_selector = size>2UL ? data[2] : status_selector;
    bam_fuzz_apply_ctrl_request( ctx, ctrl_selector, enable_ok );
  }

  if( stream_ok && ctx->enabled ) {
    bam_fuzz_open_scheduler_stream( ctx, data, size );
  }

  uchar structured_buf[ 512 ];
  if( structured && msg_kind==1 ) {
    static uchar const ping_payload[] = { 0x0a, 0x04, 0x1a, 0x02, 0x08, 0x07 };
    payload = ping_payload;
    payload_sz = sizeof( ping_payload );
    structured_ping = 1U;
  } else if( structured && msg_kind==2 ) {
    auth_info = bam_fuzz_build_auth_challenge_payload( payload_sz ? payload[0] : 0U, structured_buf, sizeof( structured_buf ) );
    if( auth_info.payload_sz ) {
      payload    = structured_buf;
      payload_sz = auth_info.payload_sz;
    }
  }

  if( has_rpc ) bam_fuzz_seed_stream_state( ctx, request_ctx );

  uchar expect_pong = (uchar)( structured_ping && stream_ok && ctx->enabled );
  ulong pong_samples_before  = 0UL;
  ulong pong_outcome_idx     = FD_METRICS_ENUM_BAM_SCHEDULER_PONG_SEND_OUTCOME_V_ENQUEUED_IDX;
  ulong pong_outcome_before  = 0UL;
  if( expect_pong ) {
    FD_TEST( ctx->bam_stream && ctx->bam_stream_live );
    bam_fuzz_clear_grpc_tx( ctx );
    switch( status_selector & 0x3U ) {
    case 1U:
      FD_TEST( fd_h2_tx_ping( fd_grpc_client_h2_conn( ctx->grpc_client ),
                              fd_grpc_client_rbuf_tx( ctx->grpc_client ) ) );
      pong_outcome_idx = FD_METRICS_ENUM_BAM_SCHEDULER_PONG_SEND_OUTCOME_V_FRAME_TX_BUSY_IDX;
      break;
    case 2U:
      ctx->grpc_client->request_stream = ctx->bam_stream;
      ctx->grpc_client->request_tx_op->chunk_sz = 1UL;
      pong_outcome_idx = FD_METRICS_ENUM_BAM_SCHEDULER_PONG_SEND_OUTCOME_V_REQUEST_BUSY_IDX;
      break;
    case 3U:
      ctx->bam_stream->s.state = FD_H2_STREAM_STATE_CLOSED;
      pong_outcome_idx = FD_METRICS_ENUM_BAM_SCHEDULER_PONG_SEND_OUTCOME_V_SEND_FAIL_IDX;
      break;
    default:
      break;
    }
    for( ulong i=0UL; i<FD_HISTF_BUCKET_CNT; i++ ) {
      pong_samples_before += fd_histf_cnt( ctx->metrics.scheduler_pong_send_nanos, i );
    }
    pong_outcome_before = ctx->metrics.scheduler_pong_send_outcome_cnt[ pong_outcome_idx ];
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

  if( expect_pong ) {
    ulong pong_samples_after = 0UL;
    for( ulong i=0UL; i<FD_HISTF_BUCKET_CNT; i++ ) {
      pong_samples_after += fd_histf_cnt( ctx->metrics.scheduler_pong_send_nanos, i );
    }
    FD_TEST( pong_samples_after == pong_samples_before + 1UL );
    FD_TEST( ctx->metrics.scheduler_pong_send_outcome_cnt[ pong_outcome_idx ] == pong_outcome_before + 1UL );
  }

  if( has_rpc && drive_end ) {
    bam_fuzz_drive_grpc_end( ctx, request_ctx, payload, payload_sz, http_status, grpc_status );
    if( request_ctx==FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge ) {
      if( http_status==200U && grpc_status==FD_GRPC_STATUS_OK ) {
        if( structured ) bam_fuzz_assert_auth_state( ctx, auth_info );
      } else {
        bam_fuzz_assert_auth_cleared( ctx );
      }
    }
    if( request_ctx==FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream &&
        http_status==200U && grpc_status==FD_GRPC_STATUS_INVALID_ARGUMENT ) {
      FD_TEST( ctx->bam_leader_state.slot == ULONG_MAX );
      FD_TEST( ctx->bam_leader_pending == 0U );
    }
  }

  if( has_rpc && drive_to ) {
    int deadline_kind = (status_selector & 1U) ? FD_GRPC_DEADLINE_HEADER : FD_GRPC_DEADLINE_RX_END;
    bam_fuzz_seed_stream_state( ctx, request_ctx );
    fd_bam_client_grpc_rx_timeout( ctx, request_ctx, deadline_kind );
    if( request_ctx==FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge ) {
      bam_fuzz_assert_auth_cleared( ctx );
    }
  }

  bam_fuzz_exercise_outbound( ctx, data, size );
  if( msg_kind==0U && stream_ok && ctx->enabled &&
      ctx->bam_config_received && fd_bam_has_effective_contact( ctx ) ) {
    static uchar const heartbeat_payload[] = { 0x0a, 0x04, 0x0a, 0x02, 0x08, 0x01 };
    FD_TEST( ctx->bam_stream && ctx->bam_stream_live );
    fd_bam_client_grpc_rx_msg( ctx,
                               heartbeat_payload,
                               sizeof( heartbeat_payload ),
                               FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
    expect_bam_gossip = 1U;
  }

  ctx->bam_status_recent = fd_bam_client_status( ctx );
  _Bool use_bam = ctx->bam_status_recent == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY &&
                  fd_bam_has_effective_contact( ctx );
  if( expect_bam_gossip ) FD_TEST( use_bam );
  bam_fuzz_publish_and_check( use_bam );
  return 0;
}
