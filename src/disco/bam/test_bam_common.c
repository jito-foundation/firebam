#include "fd_bam_tile_private.h"
#include "../metrics/fd_metrics.h"
#include "../plugin/fd_plugin.h"
#include "../fd_txn_m.h"
#include "../../waltz/grpc/fd_grpc_client_private.h"
#include "../../ballet/nanopb/pb_encode.h"
#include <sys/socket.h>
#include <unistd.h>

typedef struct {
  bam_types_Packet * packets;
  size_t             packet_cnt;
} test_bam_packet_encode_ctx_t;

FD_FN_UNUSED static bool
test_bam_encode_packets_cb( pb_ostream_t *       stream,
                            pb_field_t const *   field,
                            void * const *       arg ) {
  test_bam_packet_encode_ctx_t * ctx = (test_bam_packet_encode_ctx_t *)(*arg);
  for( size_t i=0UL; i<ctx->packet_cnt; i++ ) {
    if( FD_UNLIKELY( !pb_encode_tag_for_field( stream, field ) ) ) return false;
    if( FD_UNLIKELY( !pb_encode_submessage( stream, bam_types_Packet_fields, &ctx->packets[ i ] ) ) ) return false;
  }
  return true;
}

typedef struct {
  bam_types_AtomicTxnBatch * batches;
  size_t                     batch_cnt;
} test_bam_batch_encode_ctx_t;

FD_FN_UNUSED static bool
test_bam_encode_batches_cb( pb_ostream_t *       stream,
                            pb_field_t const *   field,
                            void * const *       arg ) {
  test_bam_batch_encode_ctx_t * ctx = (test_bam_batch_encode_ctx_t *)(*arg);
  for( size_t i=0UL; i<ctx->batch_cnt; i++ ) {
    if( FD_UNLIKELY( !pb_encode_tag_for_field( stream, field ) ) ) return false;
    if( FD_UNLIKELY( !pb_encode_submessage( stream, bam_types_AtomicTxnBatch_fields, &ctx->batches[ i ] ) ) ) return false;
  }
  return true;
}

struct test_bam_env {
  fd_stem_context_t stem[1];
  ulong             stem_seqs[1];
  ulong             stem_depths[1];
  ulong             stem_cr_avail[1];
  ulong             stem_min_cr_avail[1];
  fd_frag_meta_t *  out_mcache;
  uchar *           out_dcache;
  int               server_sock;

  fd_bam_tile_t state[1];
};

typedef struct test_bam_env test_bam_env_t;

static test_bam_env_t *
test_bam_env_create( test_bam_env_t * env,
                     fd_wksp_t *      wksp ) {
  fd_memset( env, 0, sizeof(test_bam_env_t) );

  ulong const mcache_depth = 128UL;
  fd_frag_meta_t * mcache = fd_mcache_join( fd_mcache_new(
      fd_wksp_alloc_laddr( wksp, fd_mcache_align(), fd_mcache_footprint( mcache_depth, 0UL ), 1UL ),
      128UL, 0UL, 0UL ) );
  FD_TEST( mcache );

  ulong const mtu = FD_TPU_PARSED_MTU;
  ulong const dcache_data_sz = fd_dcache_req_data_sz( mtu, mcache_depth, 1UL, 1 );
  void * dcache = fd_dcache_join( fd_dcache_new(
      fd_wksp_alloc_laddr( wksp, fd_dcache_align(), fd_dcache_footprint( dcache_data_sz, 0UL ), 1UL ),
      dcache_data_sz, 0UL ) );
  FD_TEST( dcache );

  env->out_mcache       = mcache;
  env->out_dcache       = dcache;
  env->stem_seqs    [0] = 0UL;
  env->stem_depths  [0] = mcache_depth;
  env->stem_cr_avail[0] = ULONG_MAX;
  env->stem_min_cr_avail[0] = 0UL;
  *env->stem = (fd_stem_context_t) {
    .mcaches              = &env->out_mcache,
    .seqs                 = env->stem_seqs,
    .depths               = env->stem_depths,
    .cr_avail             = env->stem_cr_avail,
    .min_cr_avail         = env->stem_min_cr_avail,
    .cr_decrement_amount  = 0UL
  };
  env->server_sock = -1;

  fd_bam_tile_t * state = env->state;
  fd_memset( state, 0, sizeof(fd_bam_tile_t) );
  for( ulong i=0UL; i<sizeof(state->bam_identity_pubkey); i++ ) state->bam_identity_pubkey[ i ] = (uchar)( i + 1U );
  fd_base58_encode_32( state->bam_identity_pubkey, NULL, state->bam_identity_pubkey_b58 );
  state->stem = env->stem;
  state->enabled = 1;
  state->verify_out = (fd_bam_out_ctx_t) {
    .idx    = 0UL,
    .mem    = dcache,
    .chunk0 = 0UL,
    .chunk  = 0UL,
    .wmark  = fd_dcache_compact_wmark( dcache, dcache, FD_TPU_PARSED_MTU )
  };
  state->plugin_out = (fd_bam_out_ctx_t){ .idx    = ULONG_MAX };
  state->gossip_out = (fd_bam_out_ctx_t){ .idx    = ULONG_MAX };
  state->tcp_sock        = -1;
  state->keylog_fd       = -1;
  state->so_rcvbuf       = 4096;
  state->grpc_buf_max    = 4096UL;
  state->map_seed        = 1UL;

  state->grpc_client_mem = fd_wksp_alloc_laddr( wksp, fd_grpc_client_align(), fd_grpc_client_footprint( state->grpc_buf_max ), 1UL );
  FD_TEST( state->grpc_client_mem );
  state->grpc_client = fd_grpc_client_new( state->grpc_client_mem, &fd_bam_client_grpc_callbacks, state->grpc_metrics, state, state->grpc_buf_max, state->map_seed );
  FD_TEST( state->grpc_client );
  fd_h2_conn_t * h2_conn = fd_grpc_client_h2_conn( state->grpc_client );
  h2_conn->flags = 0;
  state->grpc_client->conn->peer_settings.max_concurrent_streams = 8;

  state->fee_cfg = fd_wksp_alloc_laddr( wksp, alignof(fd_bam_fee_cfg_t), sizeof(fd_bam_fee_cfg_t), 1UL );
  FD_TEST( state->fee_cfg );
  fd_memset( state->fee_cfg, 0, sizeof(fd_bam_fee_cfg_t) );
  state->fee_cfg_version = 0U;
  state->commission_bps = 0U;
  state->prio_fee_recipient_set = 0U;
  fd_memset( state->prio_fee_recipient, 0, sizeof( state->prio_fee_recipient ) );

  FD_TEST( fd_rng_new( state->rng, 0U, 0UL ) );
  long ka_interval = (long)1e9;
  long ka_timeout  = (long)1e9;
  long now = fd_bam_now();
  state->keepalive_interval = ka_interval;
  FD_TEST( fd_keepalive_init( state->keepalive, state->rng, ka_interval, ka_timeout, now ) );
  state->keepalive->ts_last_tx = now;
  state->keepalive->ts_last_rx = now;

  fd_histf_new( state->metrics.msg_rx_delay,
      FD_MHIST_MIN( BAM, MESSAGE_RX_DELAY_NANOS ),
      FD_MHIST_MAX( BAM, MESSAGE_RX_DELAY_NANOS ) );
  state->cached_ts = now;

  return env;
}

FD_FN_UNUSED static void
test_bam_env_mock_builder_info( fd_bam_tile_t * state ) {
  long now = fd_bam_now();
  state->builder_commission       = 7;
  state->builder_info_valid_until = now + (long)1e9;
  state->bam_last_config_poll_ns  = now;
  for( ulong i=0UL; i<sizeof(state->builder_pubkey); i++ ) state->builder_pubkey[ i ] = (uchar)(i+1U);
}

FD_FN_UNUSED static void
test_bam_env_mock_conn_empty( test_bam_env_t * env ) {
  fd_bam_tile_t * state = env->state;
  long const ts_start = fd_bam_now();
  fd_rng_new( state->rng, 42U, 42UL );
  state->tcp_sock_connected    = 1;
  state->keepalive->ts_last_tx = ts_start;
  state->keepalive->ts_last_rx = ts_start;
  state->keepalive->ts_deadline = ts_start + (long)1e9;

  if( FD_UNLIKELY( env->server_sock >= 0 ) ) {
    FD_TEST( 0 == close( env->server_sock ) );
    env->server_sock = -1;
  }
  if( FD_UNLIKELY( state->tcp_sock >= 0 ) ) {
    FD_TEST( 0 == close( state->tcp_sock ) );
    state->tcp_sock = -1;
  }

  int sockpair[2] = { -1, -1 };
  FD_TEST( 0 == socketpair( AF_UNIX, SOCK_STREAM, 0, sockpair ) );
  env->server_sock    = sockpair[0];
  state->tcp_sock     = sockpair[1];
}

FD_FN_UNUSED static void
test_bam_env_mock_h2_hs( fd_bam_tile_t * state ) {
  fd_h2_conn_t * conn = fd_grpc_client_h2_conn( state->grpc_client );
  conn->flags = 0; /* Simulate completed HTTP/2 handshake */
  state->grpc_client->h2_hs_done = 1;
  FD_TEST( !fd_grpc_client_request_is_blocked( state->grpc_client ) );
}

FD_FN_UNUSED static void
test_bam_env_mock_conn( test_bam_env_t * env ) {
  test_bam_env_mock_conn_empty( env );
  fd_bam_tile_t * state = env->state;
  test_bam_env_mock_builder_info( state );
  test_bam_env_mock_h2_hs( state );
  long now = fd_bam_now();
  state->bam_last_builder_heartbeat_ns   = now;
  state->bam_last_validator_heartbeat_ns = now;
  state->bam_stream_live = 1;
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
}

static void
test_bam_env_destroy( test_bam_env_t * env ) {
  if( env->server_sock >= 0 ) {
    FD_TEST( 0 == close( env->server_sock ) );
    env->server_sock = -1;
  }
  if( env->state->tcp_sock >= 0 ) {
    FD_TEST( 0 == close( env->state->tcp_sock ) );
    env->state->tcp_sock = -1;
  }
  if( env->state->fee_cfg ) {
    fd_wksp_free_laddr( env->state->fee_cfg );
    env->state->fee_cfg = NULL;
  }
  fd_wksp_free_laddr( fd_mcache_delete( fd_mcache_leave( env->out_mcache ) ) );
  void * dcache_shmem = fd_dcache_leave( env->out_dcache );
  FD_TEST( dcache_shmem );
  FD_TEST( fd_dcache_join( dcache_shmem ) ); /* sanity check header before delete */
  void * dcache_deleted = fd_dcache_delete( dcache_shmem );
  FD_TEST( dcache_deleted );
  fd_wksp_free_laddr( dcache_deleted );
  fd_wksp_free_laddr( env->state->grpc_client_mem );
  fd_memset( env, 0, sizeof(test_bam_env_t) );
}
