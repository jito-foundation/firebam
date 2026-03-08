#define _GNU_SOURCE

#include "fd_bam_tile.h"
#include "test_bam_common.c"

static uchar metrics_scratch[ FD_METRICS_FOOTPRINT( 0UL, 0UL ) ] __attribute__((aligned( FD_METRICS_ALIGN )));

__attribute__((weak)) char const fdctl_version_string[] = "0.0.0";

static long g_clock = 1L;

__attribute__((weak)) long
fd_bam_now( void ) {
  return g_clock;
}

static void
test_bam_e2e_committed_result_flushes_wire_message( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  test_bam_prepare_scheduler_stream( state );

  g_clock = (long)9e9;
  test_bam_keepalive_sync( state, g_clock );

  fd_bam_bundle_result_t result = test_make_bundle_result( 900U, 1900UL, 2U );
  test_enqueue_bundle_result( state, &result );

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->bam_pending_results == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.which_versioned_msg == bam_api_SchedulerMessage_v0_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  FD_TEST( decoded.multi.results[0].seq_id == 900U );
  FD_TEST( decoded.multi.results[0].which_result == bam_types_AtomicTxnBatchResult_committed_tag );
  FD_TEST( decoded.multi.committed[0].txn_cnt == result.bundle_txn_cnt );
  FD_TEST( decoded.multi.committed[0].txns[0].cus_consumed == result.consumed_cus[0] );
  FD_TEST( decoded.multi.committed[0].txns[0].feepayer_balance_lamports == result.feepayer_balance_lamports[0] );
  FD_TEST( decoded.multi.committed[0].txns[0].loaded_accounts_data_size == result.loaded_accounts_data_size[0] );

  test_bam_env_destroy( env );
}

static void
test_bam_e2e_inbound_mixed_flags_flushes_inconsistent_bundle( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  test_bam_prepare_scheduler_stream( state );

  bam_types_Packet packets[ 2 ];
  fd_memset( packets, 0, sizeof( packets ) );
  test_bam_init_simple_vote_packet( &packets[0], 1U );
  packets[1].data.size = 1U;
  packets[1].data.bytes[0] = (uchar)'x';
  packets[1].has_meta = 1U;
  packets[1].meta.has_flags = 1U;
  packets[1].meta.flags.revert_on_error = 0U;

  uchar protobuf[ 2048 ];
  size_t protobuf_sz = test_bam_encode_scheduler_response( packets, 2UL, 901U, protobuf, sizeof( protobuf ) );
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->bam_pending_results == 1UL );
  g_clock = (long)10e9;
  test_bam_keepalive_sync( state, g_clock );

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->bam_pending_results == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->seq_id == 901U );
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_deserialization_error_tag );
  FD_TEST( result->result.not_committed.reason.deserialization_error.reason == bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE );
  FD_TEST( result->result.not_committed.reason.deserialization_error.index == 0U );

  test_bam_env_destroy( env );
}

static void
test_bam_e2e_real_vote_payload_flushes_vote_rejection( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  test_bam_prepare_scheduler_stream( state );

  bam_types_Packet packet[ 1 ];
  test_bam_init_simple_vote_packet( &packet[0], 1U );

  uchar protobuf[ 2048 ];
  size_t protobuf_sz = test_bam_encode_scheduler_response( packet, 1UL, 902U, protobuf, sizeof( protobuf ) );
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->bam_pending_results == 1UL );
  g_clock = (long)11e9;
  test_bam_keepalive_sync( state, g_clock );

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->bam_pending_results == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->seq_id == 902U );
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_deserialization_error_tag );
  FD_TEST( result->result.not_committed.reason.deserialization_error.reason == bam_types_DeserializationErrorReason_VOTE_TRANSACTION_FAILURE );
  FD_TEST( result->result.not_committed.reason.deserialization_error.index == 0U );

  test_bam_env_destroy( env );
}

int
main( int argc,
      char ** argv ) {
  fd_boot( &argc, &argv );
  fd_metrics_register( (ulong *)fd_metrics_new( metrics_scratch, 0UL, 0UL ) );

  ulong cpu_idx = fd_tile_cpu_id( fd_tile_idx() );
  if( cpu_idx > fd_shmem_cpu_cnt() ) cpu_idx = 0UL;

  char const * _page_sz = fd_env_strip_cmdline_cstr ( &argc, &argv, "--page-sz", NULL, "normal"                     );
  ulong        page_cnt = fd_env_strip_cmdline_ulong( &argc, &argv, "--page-cnt", NULL, 256UL                        );
  ulong        numa_idx = fd_env_strip_cmdline_ulong( &argc, &argv, "--numa-idx", NULL, fd_shmem_numa_idx( cpu_idx ) );

  fd_wksp_t * wksp = fd_wksp_new_anonymous( fd_cstr_to_shmem_page_sz( _page_sz ),
                                            page_cnt,
                                            fd_shmem_cpu_idx( numa_idx ),
                                            "bam-e2e-test",
                                            16UL );
  FD_TEST( wksp );

  test_bam_e2e_committed_result_flushes_wire_message( wksp );
  test_bam_e2e_inbound_mixed_flags_flushes_inconsistent_bundle( wksp );
  test_bam_e2e_real_vote_payload_flushes_vote_rejection( wksp );

  fd_wksp_usage_t usage;
  FD_TEST( fd_wksp_usage( wksp, NULL, 0UL, &usage ) );
  FD_TEST( usage.free_cnt == usage.total_cnt );
  fd_wksp_delete_anonymous( wksp );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
