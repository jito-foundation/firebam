#ifndef FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTED
#define FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_DISCONNECTED (0)
#define FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTING   (1)
#define FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTED    (2)
#endif

#include "test_bam_common.c"
#include "../bundle/proto/block_engine.pb.h"
#include "../../ballet/base58/fd_base58.h"
#include "../../ballet/nanopb/pb_encode.h"
#include <string.h>

FD_IMPORT_BINARY( test_bundle_response, "src/disco/bundle/test_bundle_response.binpb" );

__attribute__((weak)) char const fdctl_version_string[] = "0.0.0";

static long g_clock = 1L;

__attribute__((weak)) long
fd_bam_now( void ) {
  return g_clock;
}

static void
zero_meta_ts( fd_frag_meta_t * meta,
              ulong            depth ) {
  for( ulong i=0UL; i<depth; i++ ) {
    meta[ i ].tsorig = 0U;
    meta[ i ].tspub  = 0U;
  }
}

static void
test_bam_packets_forwarded( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  static uchar subscribe_packets_msg[] = {
    0x12, 0x13, 0x0a, 0x07, 0x0a, 0x01, 0x48, 0x12,
    0x02, 0x08, 0x01, 0x0a, 0x08, 0x0a, 0x02, 0x48,
    0x48, 0x12, 0x02, 0x08, 0x02
  };

  FD_TEST( state->metrics.packet_received_cnt==0UL );
  FD_TEST( state->metrics.txn_received_cnt==0UL );

  fd_bam_client_grpc_rx_msg( state,
                             subscribe_packets_msg,
                             sizeof(subscribe_packets_msg),
                             FD_BAM_CLIENT_REQ_Bundle_SubscribePackets );

  FD_TEST( state->metrics.packet_received_cnt==2UL );
  FD_TEST( state->metrics.txn_received_cnt  ==2UL );

  zero_meta_ts( env->out_mcache, 2UL );
  fd_frag_meta_t expected[2] = {
    { .seq=0UL, .sig=0UL, .chunk=0UL, .sz=(ushort)(sizeof(fd_txn_m_t)+1UL), .ctl=0U },
    { .seq=1UL, .sig=0UL, .chunk=2UL, .sz=(ushort)(sizeof(fd_txn_m_t)+2UL), .ctl=0U }
  };
  FD_TEST( fd_memeq( env->out_mcache, expected, sizeof(expected) ) );

  fd_txn_m_t * first = (fd_txn_m_t *)fd_chunk_to_laddr( state->verify_out.mem, 0UL );
  FD_TEST( first->source_tpu    == FD_TXN_M_TPU_SOURCE_BUNDLE );
  FD_TEST( first->block_engine.bundle_id == 0UL );
  FD_TEST( first->block_engine.bundle_txn_cnt==1UL );

  test_bam_env_destroy( env );
}

static void
test_bam_bundle_forwarded( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;
  test_bam_env_mock_builder_info( state );

  FD_TEST( state->metrics.bundle_received_cnt==0UL );

  fd_bam_client_grpc_rx_msg( state,
                             test_bundle_response,
                             test_bundle_response_sz,
                             FD_BAM_CLIENT_REQ_Bundle_SubscribeBundles );

  FD_TEST( state->metrics.bundle_received_cnt==1UL );
  FD_TEST( state->bundle_seq==1UL );
  FD_TEST( state->metrics.txn_received_cnt>0UL );

  fd_txn_m_t * first = (fd_txn_m_t *)fd_chunk_to_laddr( state->verify_out.mem, 0UL );
  FD_TEST( first->block_engine.bundle_id==1UL );
  FD_TEST( first->block_engine.bundle_txn_cnt>=1UL );
  FD_TEST( first->block_engine.commission==state->builder_commission );
  FD_TEST( 0==memcmp( first->block_engine.commission_pubkey, state->builder_pubkey, 32UL ) );

  test_bam_env_destroy( env );
}

static void
test_bam_bundle_requires_builder_info( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  FD_TEST( state->metrics.bundle_received_cnt==0UL );
  fd_bam_client_grpc_rx_msg( state,
                             test_bundle_response,
                             test_bundle_response_sz,
                             FD_BAM_CLIENT_REQ_Bundle_SubscribeBundles );
  FD_TEST( state->metrics.bundle_received_cnt==0UL );
  FD_TEST( state->metrics.missing_builder_info_fail_cnt==1UL );
  FD_TEST( state->metrics.txn_received_cnt==0UL );

  test_bam_env_destroy( env );
}

static void
test_bam_grpc_end_handling( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  fd_grpc_client_t * client = state->grpc_client;
  fd_grpc_h2_stream_t * stream = fd_grpc_client_stream_acquire( client, FD_BAM_CLIENT_REQ_Bundle_SubscribeBundles );
  FD_TEST( stream );
  stream->hdrs.h2_status     = 200;
  stream->hdrs.is_grpc_proto = 1;
  fd_bam_client_grpc_rx_start( state, FD_BAM_CLIENT_REQ_Bundle_SubscribeBundles );
  FD_TEST( state->bundle_subscription_live==1U );

  fd_grpc_resp_hdrs_t hdrs = {
    .h2_status   = 200,
    .grpc_status = FD_GRPC_STATUS_UNAVAILABLE
  };
  fd_bam_client_grpc_rx_end( state, FD_BAM_CLIENT_REQ_Bundle_SubscribeBundles, &hdrs );
  FD_TEST( state->bundle_subscription_live==0U );
  FD_TEST( state->defer_reset==1U );

  state->defer_reset = 0;
  stream = fd_grpc_client_stream_acquire( client, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( stream );
  stream->hdrs.h2_status     = 200;
  stream->hdrs.is_grpc_proto = 1;
  state->bam_stream = stream;
  fd_bam_client_grpc_rx_start( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( state->bam_stream_live==1U );
  hdrs.grpc_status = FD_GRPC_STATUS_OK;
  fd_bam_client_grpc_rx_end( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream, &hdrs );
  FD_TEST( state->bam_stream_live==0U );
  FD_TEST( state->bam_stream==NULL );
  FD_TEST( state->defer_reset==0U );

  test_bam_env_destroy( env );
}

static void
test_bam_grpc_timeout( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  state->bam_auth_inflight      = 1U;
  state->bam_auth_ready         = 1U;
  state->bam_auth_challenge_len = 16U;
  fd_bam_client_grpc_rx_timeout( state, FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge, FD_GRPC_DEADLINE_HEADER );
  FD_TEST( state->bam_auth_inflight==0U );
  FD_TEST( state->bam_auth_ready==0U );
  FD_TEST( state->bam_auth_challenge_len==0U );
  FD_TEST( state->defer_reset==1U );

  state->defer_reset = 0U;
  state->bam_stream_live       = 1U;
  state->bam_stream_connecting = 1U;
  state->bam_leader_pending    = 1U;
  fd_grpc_h2_stream_t * timeout_stream = fd_grpc_client_stream_acquire( state->grpc_client, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( timeout_stream );
  state->bam_stream = timeout_stream;
  fd_bam_client_grpc_rx_timeout( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream, FD_GRPC_DEADLINE_RX_END );
  FD_TEST( state->bam_stream_live==0U );
  FD_TEST( state->bam_stream_connecting==0U );
  FD_TEST( state->bam_leader_pending==0U );
  FD_TEST( state->defer_reset==1U );

  test_bam_env_destroy( env );
}

static void
test_bam_client_status( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;
  fd_bam_tile_t state_backup = *state;
  fd_grpc_client_t client_backup = *state->grpc_client;

  FD_TEST( fd_bam_client_status( state )==FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTED );

  state->tcp_sock_connected = 0U;
  FD_TEST( fd_bam_client_status( state )==FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_DISCONNECTED );
  *state = state_backup;

  ushort const conn_dead_flags[] = {
    FD_H2_CONN_FLAGS_DEAD,
    FD_H2_CONN_FLAGS_SEND_GOAWAY
  };
  for( ulong i=0UL; i<sizeof(conn_dead_flags)/sizeof(conn_dead_flags[0]); i++ ) {
    FD_TEST( fd_bam_client_status( state )==FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTED );
    state->grpc_client->conn->flags |= conn_dead_flags[ i ];
    FD_TEST( fd_bam_client_status( state )==FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_DISCONNECTED );
    *state->grpc_client = client_backup;
  }

  ushort const conn_prog_flags[] = {
    FD_H2_CONN_FLAGS_CLIENT_INITIAL,
    FD_H2_CONN_FLAGS_WAIT_SETTINGS_ACK_0,
    FD_H2_CONN_FLAGS_WAIT_SETTINGS_0,
    FD_H2_CONN_FLAGS_SERVER_INITIAL
  };
  for( ulong i=0UL; i<sizeof(conn_prog_flags)/sizeof(conn_prog_flags[0]); i++ ) {
    FD_TEST( fd_bam_client_status( state )==FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTED );
    state->grpc_client->conn->flags |= conn_prog_flags[ i ];
    FD_TEST( fd_bam_client_status( state )==FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTING );
    *state->grpc_client = client_backup;
  }

  FD_TEST( fd_bam_client_status( state )==FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTED );
  state->auther.state = FD_BUNDLE_AUTH_STATE_REQ_TOKENS;
  FD_TEST( fd_bam_client_status( state )==FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTING );
  state->auther.state = FD_BUNDLE_AUTH_STATE_DONE_WAIT;

  FD_TEST( fd_bam_client_status( state )==FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTED );
  state->bam_stream_live = 0U;
  FD_TEST( fd_bam_client_status( state )==FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTING );
  state->bam_stream_live = 1U;

  FD_TEST( fd_bam_client_status( state )==FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTED );
  state->keepalive->inflight = 1U;
  state->keepalive->ts_deadline = state->keepalive->ts_last_tx;
  FD_TEST( fd_bam_client_status( state )==FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_DISCONNECTED );
  *state = state_backup;
  *state->grpc_client = client_backup;

  FD_TEST( fd_bam_client_status( state )==FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTED );
  state->grpc_client->h2_hs_done = 0;
  FD_TEST( fd_bam_client_status( state )==FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTING );

  test_bam_env_destroy( env );
}

static void
test_bam_request_ctx_labels( void ) {
  FD_TEST( 0==strcmp( fd_bam_request_ctx_cstr( FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge ), "BamGetAuthChallenge" ) );
  FD_TEST( 0==strcmp( fd_bam_request_ctx_cstr( FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream ), "BamInitSchedulerStream" ) );
  FD_TEST( 0==strcmp( fd_bam_request_ctx_cstr( FD_BAM_CLIENT_REQ_Bundle_SubscribePackets ), "SubscribePackets" ) );
  FD_TEST( 0==strcmp( fd_bam_request_ctx_cstr( 99UL ), "unknown" ) );
}

static void
test_bam_builder_fee_info( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  uchar pb_buf[ 256 ];
  block_engine_BlockBuilderFeeInfoResponse resp = block_engine_BlockBuilderFeeInfoResponse_init_default;
  uchar pubkey[32] = {0};
  pubkey[0] = 1U;
  pubkey[1] = 2U;
  pubkey[2] = 3U;
  pubkey[3] = 4U;
  pubkey[4] = 5U;
  FD_TEST( fd_base58_encode_32( pubkey, NULL, resp.pubkey ) );
  resp.pubkey[ FD_BASE58_ENCODED_32_SZ-1 ] = '\0';
  resp.commission = 5UL;
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  if( FD_UNLIKELY( !pb_encode( &ostream, &block_engine_BlockBuilderFeeInfoResponse_msg, &resp ) ) ) {
    FD_LOG_ERR(( "pb_encode fee info failed: %s", PB_GET_ERROR( &ostream ) ));
  }

  FD_TEST( state->builder_info_avail==0U );
  fd_bam_client_grpc_rx_msg( state, pb_buf, ostream.bytes_written, FD_BAM_CLIENT_REQ_Bundle_GetBlockBuilderFeeInfo );
  FD_TEST( state->builder_info_avail==1U );
  FD_TEST( state->builder_commission==5U );
  uchar decoded[32];
  FD_TEST( fd_base58_decode_32( resp.pubkey, decoded ) );
  FD_TEST( 0==memcmp( state->builder_pubkey, decoded, 32UL ) );

  test_bam_env_destroy( env );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  ulong cpu_idx = fd_tile_cpu_id( fd_tile_idx() );
  if( cpu_idx>fd_shmem_cpu_cnt() ) cpu_idx = 0UL;

  char const * _page_sz = fd_env_strip_cmdline_cstr ( &argc, &argv, "--page-sz",     NULL, "normal"                     );
  ulong        page_cnt = fd_env_strip_cmdline_ulong( &argc, &argv, "--page-cnt",    NULL, 256UL                        );
  ulong        numa_idx = fd_env_strip_cmdline_ulong( &argc, &argv, "--numa-idx",    NULL, fd_shmem_numa_idx( cpu_idx ) );

  fd_wksp_t * wksp = fd_wksp_new_anonymous( fd_cstr_to_shmem_page_sz( _page_sz ), page_cnt, fd_shmem_cpu_idx( numa_idx ), "bam-test", 16UL );
  FD_TEST( wksp );

  test_bam_packets_forwarded( wksp );
  test_bam_bundle_forwarded( wksp );
  test_bam_bundle_requires_builder_info( wksp );
  test_bam_grpc_end_handling( wksp );
  test_bam_grpc_timeout( wksp );
  test_bam_client_status( wksp );
  test_bam_request_ctx_labels();
  test_bam_builder_fee_info( wksp );

  fd_wksp_usage_t wksp_usage;
  FD_TEST( fd_wksp_usage( wksp, NULL, 0UL, &wksp_usage ) );
  FD_TEST( wksp_usage.free_cnt==wksp_usage.total_cnt );

  fd_wksp_delete_anonymous( wksp );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
