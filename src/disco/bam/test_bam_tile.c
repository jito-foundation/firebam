#ifndef FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTED
#define FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_DISCONNECTED (0)
#define FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTING   (1)
#define FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTED    (2)
#endif

#include "test_bam_common.c"
#include "proto/bam_api.pb.h"
#include "proto/bam_types.pb.h"
#include "../../ballet/base58/fd_base58.h"
#include "../../ballet/nanopb/pb_encode.h"
#include "../../ballet/nanopb/pb_decode.h"
#include "../../waltz/grpc/fd_grpc_codec.h"
#include "../../util/fd_util.h"
#include <stdbool.h>
#include "../../tango/fseq/fd_fseq.h"
#include <limits.h>
#include <string.h>


__attribute__((weak)) char const fdctl_version_string[] = "0.0.0";

static long g_clock = 1L;

__attribute__((weak)) long
fd_bam_now( void ) {
  return g_clock;
}

static void
test_bam_keepalive_sync( fd_bam_tile_t * state,
                         long            now ) {
  state->keepalive->ts_next_tx = now + state->keepalive_interval;
  state->keepalive->ts_deadline = 0L;
  state->keepalive->ts_last_tx  = now;
  state->keepalive->ts_last_rx  = now;
  state->keepalive->inflight    = 0U;
}

static void
test_bam_prepare_scheduler_stream( fd_bam_tile_t * state ) {
  fd_grpc_h2_stream_t * stream = fd_grpc_client_stream_acquire( state->grpc_client, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( stream );
  state->bam_stream            = stream;
  state->bam_stream_live       = 1U;
  state->bam_stream_connecting = 0U;
  state->grpc_client->request_stream = NULL;
  *state->grpc_client->request_tx_op = (fd_h2_tx_op_t){0};
  state->bam_auth_ready        = 1U;
  state->bam_auth_inflight     = 0U;
}

static void
zero_meta_ts( fd_frag_meta_t * meta,
              ulong            depth ) {
  for( ulong i=0UL; i<depth; i++ ) {
    meta[ i ].tsorig = 0U;
    meta[ i ].tspub  = 0U;
  }
}

static fd_bam_bundle_result_t
test_make_bundle_result( ulong bundle_id ) {
  fd_bam_bundle_result_t res = {0};
  res.bundle_id        = bundle_id;
  res.slot             = bundle_id + 1000UL;
  res.bundle_txn_cnt   = 2UL;
  res.txn_cnt          = 2U;
  res.execution_success = 1U;
  res.scheduling_error  = FD_BAM_SCHED_ERR_NONE;
  for( uint i=0U; i<FD_PACK_MAX_TXN_PER_BUNDLE; i++ ) {
    res.transaction_err[ i ]   = 0U;
    res.consumed_cus[ i ]      = (uint)( i + 1U );
    res.sanitize_success[ i ]  = 1U;
  }
  return res;
}

static void
test_enqueue_bundle_result( fd_bam_tile_t *               state,
                            fd_bam_bundle_result_t const * res ) {
  FD_TEST( state->bam_pending_results < FD_BAM_MAX_PENDING_RESULTS );
  state->bam_results[ state->bam_results_tail ] = *res;
  state->bam_results_tail = ( state->bam_results_tail + 1UL ) % FD_BAM_MAX_PENDING_RESULTS;
  state->bam_pending_results++;
}

typedef struct {
  bam_types_Packet * packets;
  size_t             packet_cnt;
} test_bam_packet_encode_ctx_t;

static bool
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

static bool
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

static size_t
test_bam_encode_scheduler_response( bam_types_Packet * packets,
                                    size_t             packet_cnt,
                                    uint32_t           seq_id,
                                    uchar *            out,
                                    size_t             out_sz ) {
  test_bam_packet_encode_ctx_t packets_ctx = {
      .packets    = packets,
      .packet_cnt = packet_cnt
  };

  bam_types_AtomicTxnBatch batch = bam_types_AtomicTxnBatch_init_default;
  batch.seq_id = seq_id;
  batch.max_schedule_slot = 0UL;
  batch.packets.funcs.encode = test_bam_encode_packets_cb;
  batch.packets.arg          = &packets_ctx;

  test_bam_batch_encode_ctx_t batches_ctx = {
      .batches   = &batch,
      .batch_cnt = 1UL
  };

  bam_types_MultipleAtomicTxnBatch multi = bam_types_MultipleAtomicTxnBatch_init_default;
  multi.batches.funcs.encode = test_bam_encode_batches_cb;
  multi.batches.arg          = &batches_ctx;

  bam_api_SchedulerResponse resp = bam_api_SchedulerResponse_init_default;
  resp.which_versioned_msg = bam_api_SchedulerResponse_v0_tag;
  resp.versioned_msg.v0.which_resp = bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag;
  resp.versioned_msg.v0.resp.multiple_atomic_txn_batch = multi;

  pb_ostream_t ostream = pb_ostream_from_buffer( out, out_sz );
  FD_TEST( pb_encode( &ostream, bam_api_SchedulerResponse_fields, &resp ) );
  return (size_t)ostream.bytes_written;
}

static size_t
test_bam_build_scheduler_batch_msg( uchar *  out,
                                    size_t   out_sz,
                                    uint32_t seq_id,
                                    int      revert_on_error ) {
  bam_types_Packet packets[ 2 ];
  fd_memset( packets, 0, sizeof( packets ) );
  for( size_t i=0UL; i<2UL; i++ ) {
    packets[ i ].data.size = (pb_size_t)( i + 1UL );
    for( pb_size_t j=0U; j<packets[ i ].data.size; j++ ) {
      packets[ i ].data.bytes[ j ] = (uchar)( 'A' + (int)i + (int)j );
    }
    if( revert_on_error ) {
      packets[ i ].has_meta        = 1;
      packets[ i ].meta.has_flags  = 1;
      packets[ i ].meta.flags.revert_on_error = 1;
    }
  }
  return test_bam_encode_scheduler_response( packets, 2UL, seq_id, out, out_sz );
}

typedef struct {
  bam_types_TransactionCommittedResult txns[ FD_PACK_MAX_TXN_PER_BUNDLE ];
  ulong                                txn_cnt;
} test_bam_committed_results_t;

typedef struct {
  bam_types_AtomicTxnBatchResult results[ 8 ];
  ulong                          result_cnt;
  test_bam_committed_results_t   committed[ 8 ];
} test_bam_multi_results_t;

typedef struct {
  bam_api_SchedulerMessage msg;
  test_bam_multi_results_t multi;
} test_bam_decoded_message_t;

static void
test_bam_decode_multi_results( pb_istream_t * stream,
                               test_bam_decoded_message_t * out );

static void
test_bam_decode_scheduler_message_v0( pb_istream_t * stream,
                                      test_bam_decoded_message_t * out );

static void
test_bam_decode_committed_results( pb_istream_t * stream,
                                   test_bam_committed_results_t * out ) {
  uint32_t tag;
  pb_wire_type_t wire_type;
  bool eof = false;
  while( pb_decode_tag( stream, &wire_type, &tag, &eof ) ) {
    switch( tag ) {
    case bam_types_Committed_transaction_results_tag: {
      FD_TEST( wire_type==PB_WT_STRING );
      FD_TEST( out->txn_cnt < FD_PACK_MAX_TXN_PER_BUNDLE );
      pb_istream_t substream;
      FD_TEST( pb_make_string_substream( stream, &substream ) );
      bam_types_TransactionCommittedResult * txn = &out->txns[ out->txn_cnt ];
      *txn = (bam_types_TransactionCommittedResult)bam_types_TransactionCommittedResult_init_default;
      FD_TEST( pb_decode( &substream, bam_types_TransactionCommittedResult_fields, txn ) );
      pb_close_string_substream( stream, &substream );
      out->txn_cnt++;
      break;
    }
    default:
      FD_TEST( pb_skip_field( stream, wire_type ) );
      break;
    }
  }
}

static void
test_bam_decode_atomic_result( pb_istream_t * stream,
                               bam_types_AtomicTxnBatchResult * out,
                               test_bam_committed_results_t * committed ) {
  uint32_t tag;
  pb_wire_type_t wire_type;
  bool eof = false;
  while( pb_decode_tag( stream, &wire_type, &tag, &eof ) ) {
    switch( tag ) {
    case bam_types_AtomicTxnBatchResult_seq_id_tag: {
      FD_TEST( wire_type==PB_WT_VARINT );
      uint64_t val = 0;
      FD_TEST( pb_decode_varint( stream, &val ) );
      out->seq_id = (uint32_t)val;
      break;
    }
    case bam_types_AtomicTxnBatchResult_committed_tag: {
      FD_TEST( wire_type==PB_WT_STRING );
      pb_istream_t substream;
      FD_TEST( pb_make_string_substream( stream, &substream ) );
      out->which_result = bam_types_AtomicTxnBatchResult_committed_tag;
      test_bam_decode_committed_results( &substream, committed );
      pb_close_string_substream( stream, &substream );
      break;
    }
    case bam_types_AtomicTxnBatchResult_not_committed_tag: {
      FD_TEST( wire_type==PB_WT_STRING );
      pb_istream_t substream;
      FD_TEST( pb_make_string_substream( stream, &substream ) );
      out->which_result = bam_types_AtomicTxnBatchResult_not_committed_tag;
      out->result.not_committed = (bam_types_NotCommitted)bam_types_NotCommitted_init_default;
      FD_TEST( pb_decode( &substream, bam_types_NotCommitted_fields, &out->result.not_committed ) );
      pb_close_string_substream( stream, &substream );
      break;
    }
    default:
      FD_TEST( pb_skip_field( stream, wire_type ) );
      break;
    }
  }
}

static bool
test_bam_decode_atomic_result_cb( pb_istream_t * stream,
                                  const pb_field_t * field,
                                  void ** arg ) {
  (void)field;
  test_bam_multi_results_t * multi = (test_bam_multi_results_t *)(*arg);
  FD_TEST( multi->result_cnt < 8UL );

  bam_types_AtomicTxnBatchResult * res = &multi->results[ multi->result_cnt ];
  *res = (bam_types_AtomicTxnBatchResult)bam_types_AtomicTxnBatchResult_init_default;
  test_bam_committed_results_t * committed = &multi->committed[ multi->result_cnt ];
  fd_memset( committed, 0, sizeof(test_bam_committed_results_t) );

  test_bam_decode_atomic_result( stream, res, committed );

  multi->result_cnt++;
  return true;
}

static void
test_bam_decode_multi_results( pb_istream_t * stream,
                               test_bam_decoded_message_t * out ) {
  bam_types_MultipleAtomicTxnBatchResult msg = bam_types_MultipleAtomicTxnBatchResult_init_default;
  msg.results.funcs.decode = test_bam_decode_atomic_result_cb;
  msg.results.arg          = &out->multi;
  FD_TEST( pb_decode( stream, bam_types_MultipleAtomicTxnBatchResult_fields, &msg ) );
}

static void
test_bam_decode_scheduler_message_v0( pb_istream_t * stream,
                                      test_bam_decoded_message_t * out ) {
  uint32_t tag;
  pb_wire_type_t wire_type;
  bool eof = false;
  while( pb_decode_tag( stream, &wire_type, &tag, &eof ) ) {
    switch( tag ) {
    case bam_api_SchedulerMessageV0_heart_beat_tag:
      FD_TEST( wire_type==PB_WT_STRING );
      pb_istream_t hb_stream;
      FD_TEST( pb_make_string_substream( stream, &hb_stream ) );
      out->msg.versioned_msg.v0.which_msg = bam_api_SchedulerMessageV0_heart_beat_tag;
      FD_TEST( pb_decode( &hb_stream, bam_types_ValidatorHeartBeat_fields,
                          &out->msg.versioned_msg.v0.msg.heart_beat ) );
      pb_close_string_substream( stream, &hb_stream );
      break;
    case bam_api_SchedulerMessageV0_auth_proof_tag: {
      FD_TEST( wire_type==PB_WT_STRING );
      pb_istream_t substream;
      FD_TEST( pb_make_string_substream( stream, &substream ) );
      out->msg.versioned_msg.v0.which_msg = bam_api_SchedulerMessageV0_auth_proof_tag;
      out->msg.versioned_msg.v0.msg.auth_proof = (bam_types_AuthProof)bam_types_AuthProof_init_default;
      FD_TEST( pb_decode( &substream, bam_types_AuthProof_fields,
                          &out->msg.versioned_msg.v0.msg.auth_proof ) );
      pb_close_string_substream( stream, &substream );
      break;
    }
    case bam_api_SchedulerMessageV0_leader_state_tag:
      FD_TEST( wire_type==PB_WT_STRING );
      pb_istream_t ls_stream;
      FD_TEST( pb_make_string_substream( stream, &ls_stream ) );
      out->msg.versioned_msg.v0.which_msg = bam_api_SchedulerMessageV0_leader_state_tag;
      FD_TEST( pb_decode( &ls_stream, bam_types_LeaderState_fields,
                          &out->msg.versioned_msg.v0.msg.leader_state ) );
      pb_close_string_substream( stream, &ls_stream );
      break;
    case bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag: {
      FD_TEST( wire_type==PB_WT_STRING );
      pb_istream_t substream;
      FD_TEST( pb_make_string_substream( stream, &substream ) );
      out->msg.versioned_msg.v0.which_msg = bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag;
      test_bam_decode_multi_results( &substream, out );
      pb_close_string_substream( stream, &substream );
      break;
    }
    default:
      FD_TEST( pb_skip_field( stream, wire_type ) );
      break;
    }
  }
}

static void
test_bam_decode_last_message( fd_bam_tile_t *              state,
                              test_bam_decoded_message_t * out ) {
  fd_grpc_client_t * client = state->grpc_client;
  fd_grpc_hdr_t hdr;
  memcpy( &hdr, client->nanopb_tx, sizeof(fd_grpc_hdr_t) );
  uint msg_sz = fd_uint_bswap( hdr.msg_sz );
  FD_TEST( msg_sz <= client->nanopb_tx_max - sizeof(fd_grpc_hdr_t) );

  out->msg = (bam_api_SchedulerMessage)bam_api_SchedulerMessage_init_default;
  fd_memset( &out->multi, 0, sizeof(out->multi) );
  pb_istream_t stream = pb_istream_from_buffer( client->nanopb_tx + sizeof(fd_grpc_hdr_t), msg_sz );
  uint32_t tag;
  pb_wire_type_t wire_type;
  bool eof = false;
  while( pb_decode_tag( &stream, &wire_type, &tag, &eof ) ) {
    if( tag==bam_api_SchedulerMessage_v0_tag && wire_type==PB_WT_STRING ) {
      pb_istream_t substream;
      FD_TEST( pb_make_string_substream( &stream, &substream ) );
      out->msg.which_versioned_msg = bam_api_SchedulerMessage_v0_tag;
      test_bam_decode_scheduler_message_v0( &substream, out );
      pb_close_string_substream( &stream, &substream );
    } else {
      FD_TEST( pb_skip_field( &stream, wire_type ) );
    }
  }

  client->request_stream = NULL;
  *client->request_tx_op = (fd_h2_tx_op_t){0};
}

static void
test_bam_packets_forwarded( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  uchar protobuf[256];
  size_t protobuf_sz = test_bam_build_scheduler_batch_msg( protobuf, sizeof(protobuf), 0U, 0 );

  FD_TEST( state->metrics.txn_received_cnt==0UL );
  FD_TEST( state->metrics.bundle_received_cnt==0UL );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.txn_received_cnt==2UL );
  FD_TEST( state->metrics.bundle_received_cnt==0UL );

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

  uchar protobuf[512];
  size_t protobuf_sz = test_bam_build_scheduler_batch_msg( protobuf, sizeof(protobuf), 1U, 1 );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

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
  uchar protobuf[512];
  size_t protobuf_sz = test_bam_build_scheduler_batch_msg( protobuf, sizeof(protobuf), 2U, 1 );
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
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
  fd_grpc_h2_stream_t * stream = fd_grpc_client_stream_acquire( client, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( stream );
  stream->hdrs.h2_status     = 200;
  stream->hdrs.is_grpc_proto = 1;
  state->bam_stream = stream;
  fd_bam_client_grpc_rx_start( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( state->bam_stream_live==1U );

  fd_grpc_resp_hdrs_t hdrs_fail = {
      .h2_status   = 200,
      .grpc_status = FD_GRPC_STATUS_UNAVAILABLE
  };
  fd_bam_client_grpc_rx_end( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream, &hdrs_fail );
  FD_TEST( state->bam_stream_live==0U );
  FD_TEST( state->bam_stream==NULL );
  FD_TEST( state->defer_reset==0U );

  stream = fd_grpc_client_stream_acquire( client, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( stream );
  stream->hdrs.h2_status     = 200;
  stream->hdrs.is_grpc_proto = 1;
  state->bam_stream = stream;
  fd_bam_client_grpc_rx_start( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( state->bam_stream_live==1U );

  fd_grpc_resp_hdrs_t hdrs_ok = {
      .h2_status   = 200,
      .grpc_status = FD_GRPC_STATUS_OK
  };
  fd_bam_client_grpc_rx_end( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream, &hdrs_ok );
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

static fd_bam_tile_t *
test_bam_heartbeat_env_start( test_bam_env_t * env,
                              fd_wksp_t *      wksp ) {
  g_clock = (long)1e9;
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;
  state->runtime_enabled = 1;
  state->keepalive->ts_deadline = LONG_MAX;
  state->keepalive->ts_last_tx = g_clock;
  state->keepalive->ts_last_rx = g_clock;
  state->keepalive->inflight   = 0;
  state->defer_reset = 0;
  fd_bam_client_grpc_rx_start( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( state->bam_last_builder_heartbeat_ns==g_clock );
  FD_TEST( state->bam_last_validator_heartbeat_ns==g_clock );
  return state;
}

static void
test_bam_heartbeat_timeout_forces_disconnect( fd_wksp_t * wksp ) {
  /* Test 1: Timeout triggers when heartbeat exceeds 6 seconds */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    int charge_busy = 0;
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS + (long)1e8;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->defer_reset==1 );
    FD_TEST( charge_busy==1 );
    test_bam_env_destroy( env );
  }

  /* Test 2: No timeout when heartbeat timestamp is 0L (uninitialized) */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    state->bam_last_builder_heartbeat_ns = 0L;
    int charge_busy = 0;
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS + (long)1e9;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->defer_reset==0 );
    test_bam_env_destroy( env );
  }

  /* Test 3: No timeout when stream is not live */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    state->bam_stream_live = 0;
    int charge_busy = 0;
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS + (long)1e9;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->defer_reset==0 );
    test_bam_env_destroy( env );
  }

  /* Test 4: No timeout just before the threshold */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    int charge_busy = 0;
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS - (long)1e6; /* 1ms before timeout */
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->defer_reset==0 );
    test_bam_env_destroy( env );
  }

  /* Test 5: Timeout at exact boundary (>= 6 seconds should trigger) */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    int charge_busy = 0;
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->defer_reset==1 );
    FD_TEST( charge_busy==1 );
    test_bam_env_destroy( env );
  }

  /* Test 6: Timeout at boundary + 1ns */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    int charge_busy = 0;
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS + 1L;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->defer_reset==1 );
    FD_TEST( charge_busy==1 );
    test_bam_env_destroy( env );
  }

  /* Test 7: No timeout when runtime is disabled */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    state->runtime_enabled = 0;
    int charge_busy = 0;
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS + (long)1e9;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->defer_reset==0 );
    test_bam_env_destroy( env );
  }
}

static void
test_bam_heartbeat_reset_extends_timeout( fd_wksp_t * wksp ) {
  // Pre-encoded bam_api.SchedulerResponse messages (nanopb framing) that
  // drive the heartbeat timestamping logic, derived from src/disco/bam/proto/bam_api.proto

  // SchedulerResponse { v0 { heart_beat { time_sent_microseconds: 1 } } }
  static uchar heartbeat_msg[] = { 0x0a, 0x04, 0x0a, 0x02, 0x08, 0x01 };
  // SchedulerResponse { v0 { multiple_atomic_txn_batch { /* field #1 (reserved/unused) set to 0 so the message is non-empty */ } } }
  static uchar bundle_msg[]    = { 0x0a, 0x04, 0x12, 0x02, 0x08, 0x00 };

  /* Heartbeat message updates timestamp */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    fd_bam_client_grpc_rx_msg( state, heartbeat_msg, sizeof(heartbeat_msg),
                               FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
    long expected_ts = g_clock;
    FD_TEST( state->bam_last_builder_heartbeat_ns==expected_ts );
    FD_TEST( state->metrics.heartbeat_recv_cnt==1UL );
    test_bam_env_destroy( env );
  }

  /* 5.9 seconds after heartbeat should NOT timeout */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    int charge_busy = 0;
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS - (long)2e8;
    fd_bam_client_grpc_rx_msg( state, heartbeat_msg, sizeof(heartbeat_msg),
                               FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS - (long)1e8;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->defer_reset==0 );
    test_bam_env_destroy( env );
  }

  /* 6.1 seconds after heartbeat SHOULD timeout */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    int charge_busy = 0;
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS - (long)2e8;
    fd_bam_client_grpc_rx_msg( state, heartbeat_msg, sizeof(heartbeat_msg),
                               FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS + (long)2e8;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->defer_reset==1 );
    FD_TEST( charge_busy==1 );
    test_bam_env_destroy( env );
  }

  /* Bundle batches update timestamp */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    fd_bam_client_grpc_rx_msg( state, bundle_msg, sizeof(bundle_msg),
                               FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
    long expected_ts = g_clock;
    FD_TEST( state->bam_last_builder_heartbeat_ns==expected_ts );
    test_bam_env_destroy( env );
  }

  /* 5.9 seconds after bundle should NOT timeout */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    int charge_busy = 0;
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS - (long)2e8;
    fd_bam_client_grpc_rx_msg( state, bundle_msg, sizeof(bundle_msg),
                               FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS - (long)1e8;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->defer_reset==0 );
    test_bam_env_destroy( env );
  }

  /* 6.1 seconds after bundle SHOULD timeout */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    int charge_busy = 0;
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS - (long)2e8;
    fd_bam_client_grpc_rx_msg( state, bundle_msg, sizeof(bundle_msg),
                               FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS + (long)2e8;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->defer_reset==1 );
    FD_TEST( charge_busy==1 );
    test_bam_env_destroy( env );
  }
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
test_bam_scheduler_auth_proof_publishes_message( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  state->bam_stream            = NULL;
  state->bam_stream_live       = 0U;
  state->bam_stream_connecting = 0U;
  state->bam_config_inflight   = 1U;
  state->bam_auth_ready        = 1U;
  state->bam_auth_inflight     = 0U;
  state->grpc_client->request_stream = NULL;
  *state->grpc_client->request_tx_op = (fd_h2_tx_op_t){0};

  g_clock = (long)5e9;
  test_bam_keepalive_sync( state, g_clock );
  state->bam_last_config_poll_ns = g_clock;

  char const challenge[] = "challenge-123";
  fd_memset( state->bam_auth_challenge, 0, sizeof(state->bam_auth_challenge) );
  memcpy( state->bam_auth_challenge, challenge, sizeof(challenge) );
  state->bam_auth_challenge_len = (uint)sizeof(challenge) - 1U;

  char const signature[] = "sig-abcdef";
  fd_memset( state->bam_auth_signature, 0, sizeof(state->bam_auth_signature) );
  memcpy( state->bam_auth_signature, signature, sizeof(signature) );

  char const validator_key[] = "validator-key-test";
  fd_memset( state->bam_validator_pubkey, 0, sizeof(state->bam_validator_pubkey) );
  memcpy( state->bam_validator_pubkey, validator_key, sizeof(validator_key) );

  fd_bam_test_drive( state, g_clock );

  FD_TEST( state->bam_stream!=NULL );
  FD_TEST( state->bam_stream_connecting==1U );
  FD_TEST( state->bam_auth_ready==0U );
  FD_TEST( state->bam_stream_live==0U );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.which_versioned_msg==bam_api_SchedulerMessage_v0_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg==bam_api_SchedulerMessageV0_auth_proof_tag );
  FD_TEST( 0==strcmp( decoded.msg.versioned_msg.v0.msg.auth_proof.challenge_to_sign, challenge ) );
  FD_TEST( 0==strcmp( decoded.msg.versioned_msg.v0.msg.auth_proof.signature, signature ) );
  FD_TEST( 0==strcmp( decoded.msg.versioned_msg.v0.msg.auth_proof.validator_pubkey, validator_key ) );

  test_bam_env_destroy( env );
}

static void
test_bam_auth_challenge_response_sets_signature( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  ulong const depth = 8UL;

  void * request_mem = fd_wksp_alloc_laddr(
      wksp, fd_mcache_align(), fd_mcache_footprint( depth, 0UL ), 1UL );
  FD_TEST( request_mem );
  fd_frag_meta_t * request_mcache = fd_mcache_join(
      fd_mcache_new( request_mem, depth, 0UL, 0UL ) );
  FD_TEST( request_mcache );

  void * response_mem = fd_wksp_alloc_laddr(
      wksp, fd_mcache_align(), fd_mcache_footprint( depth, 0UL ), 1UL );
  FD_TEST( response_mem );
  fd_frag_meta_t * response_mcache = fd_mcache_join(
      fd_mcache_new( response_mem, depth, 0UL, 0UL ) );
  FD_TEST( response_mcache );

  uchar * request_data = fd_wksp_alloc_laddr( wksp, FD_WKSP_ALIGN_DEFAULT, 256UL, 1UL );
  uchar * response_data = fd_wksp_alloc_laddr( wksp, FD_WKSP_ALIGN_DEFAULT, 64UL, 1UL );
  FD_TEST( request_data && response_data );

  fd_memset( request_data, 0, 256UL );
  fd_memset( response_data, 0, 64UL );

  FD_TEST( fd_keyguard_client_new( state->keyguard_client,
                                   request_mcache, request_data,
                                   response_mcache, response_data ) );

  uchar signature[ 64 ];
  for( ulong i=0UL; i<64UL; i++ ) signature[ i ] = (uchar)( i + 1UL );
  fd_memcpy( response_data, signature, sizeof(signature) );
  fd_mcache_publish( response_mcache,
                     depth,
                     state->keyguard_client->response_seq,
                     0UL,
                     0UL,
                     sizeof(signature),
                     0UL,
                     0UL,
                     0UL );

  bam_api_AuthChallengeResponse resp = bam_api_AuthChallengeResponse_init_default;
  char const challenge[] = "unit-test-challenge";
  FD_TEST( strlen( challenge ) < sizeof( resp.challenge_to_sign ) );
  fd_memset( resp.challenge_to_sign, 0, sizeof( resp.challenge_to_sign ) );
  fd_memcpy( resp.challenge_to_sign, challenge, strlen( challenge ) );

  uchar pb_buf[ 128 ];
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_AuthChallengeResponse_fields, &resp ) );

  state->bam_auth_inflight = 1U;
  char const validator_key[] = "validator-pubkey-test";
  fd_memset( state->bam_validator_pubkey, 0, sizeof( state->bam_validator_pubkey ) );
  fd_memcpy( state->bam_validator_pubkey, validator_key, strlen( validator_key ) );

  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             (ulong)ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge );

  FD_TEST( state->bam_auth_inflight==0U );
  FD_TEST( state->bam_auth_ready==1U );
  FD_TEST( state->bam_auth_challenge_len==strlen( challenge ) );
  FD_TEST( 0==memcmp( state->bam_auth_challenge, challenge, strlen( challenge ) ) );

  char expected_sig[ FD_BASE58_ENCODED_64_SZ ];
  FD_TEST( fd_base58_encode_64( signature, NULL, expected_sig ) );
  FD_TEST( 0==strcmp( state->bam_auth_signature, expected_sig ) );
  FD_TEST( state->keyguard_client->request_seq==1UL );

  static char const label[] = "X_OFF_CHAIN_JITO_BAM_V1\0";
  size_t const label_len = sizeof( label ) - 1UL;
  size_t const challenge_len = strlen( challenge );
  uchar expected_payload[ 256 ];
  FD_TEST( label_len + challenge_len <= sizeof( expected_payload ) );
  fd_memcpy( expected_payload, label, label_len );
  fd_memcpy( expected_payload + label_len, challenge, challenge_len );
  FD_TEST( 0==memcmp( request_data, expected_payload, label_len + challenge_len ) );

  fd_frag_meta_t const * req_meta =
      request_mcache + fd_mcache_line_idx( 0UL, depth );
  FD_TEST( req_meta->sz==(ushort)( label_len + challenge_len ) );

  fd_wksp_free_laddr( request_data );
  fd_wksp_free_laddr( response_data );
  void * request_shmem = fd_mcache_leave( request_mcache );
  FD_TEST( request_shmem );
  fd_wksp_free_laddr( fd_mcache_delete( request_shmem ) );
  void * response_shmem = fd_mcache_leave( response_mcache );
  FD_TEST( response_shmem );
  fd_wksp_free_laddr( fd_mcache_delete( response_shmem ) );

  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_heartbeat_publishes_message( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  test_bam_prepare_scheduler_stream( state );

  g_clock = (long)7e9;
  long now = g_clock;
  state->bam_last_validator_heartbeat_ns = 0L;
  state->bam_last_config_poll_ns = now;
  test_bam_keepalive_sync( state, now );
  int busy = fd_bam_test_drive( state, now );
  FD_TEST( busy==1 );
  FD_TEST( state->bam_last_validator_heartbeat_ns==now );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.which_versioned_msg==bam_api_SchedulerMessage_v0_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg==bam_api_SchedulerMessageV0_heart_beat_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.msg.heart_beat.time_sent_microseconds==(uint64_t)(now/1000L) );
  FD_TEST( state->metrics.heartbeat_sent_cnt==1UL );

  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_leader_state_publishes_message( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  test_bam_prepare_scheduler_stream( state );

  state->bam_leader_pending = 1U;
  state->bam_leader_state = (fd_bam_leader_state_t){
    .slot = 42UL,
    .tick = 7U,
    .slot_cu_budget_remaining = 123U
  };

  g_clock = (long)8e9;
  long now = g_clock;
  state->bam_last_config_poll_ns = now;
  state->bam_last_validator_heartbeat_ns = now;
  test_bam_keepalive_sync( state, now );
  int busy = fd_bam_test_drive( state, now );
  FD_TEST( busy==1 );
  FD_TEST( state->bam_leader_pending==0U );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.which_versioned_msg==bam_api_SchedulerMessage_v0_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg==bam_api_SchedulerMessageV0_leader_state_tag );
  bam_types_LeaderState const * ls = &decoded.msg.versioned_msg.v0.msg.leader_state;
  FD_TEST( ls->slot==42UL );
  FD_TEST( ls->tick==7U );
  FD_TEST( ls->slot_cu_budget_remaining==123U );

  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_result_publishes_message( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  test_bam_prepare_scheduler_stream( state );

  g_clock = (long)9e9;
  test_bam_keepalive_sync( state, g_clock );

  fd_bam_bundle_result_t res = test_make_bundle_result( 900UL );
  test_enqueue_bundle_result( state, &res );

  int flushed = fd_bam_test_flush_results( state );
  FD_TEST( flushed==1 );
  FD_TEST( state->bam_pending_results==0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.which_versioned_msg==bam_api_SchedulerMessage_v0_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg==bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt==1UL );
  FD_TEST( decoded.multi.results[0].seq_id==900U );
  FD_TEST( decoded.multi.results[0].which_result==bam_types_AtomicTxnBatchResult_committed_tag );
  FD_TEST( decoded.multi.committed[0].txn_cnt==res.txn_cnt );
  FD_TEST( decoded.multi.committed[0].txns[0].cus_consumed==(uint32_t)res.consumed_cus[0] );

  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_result_not_committed_publishes_message( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  test_bam_prepare_scheduler_stream( state );

  g_clock = (long)10e9;
  test_bam_keepalive_sync( state, g_clock );
  state->bam_last_config_poll_ns = g_clock;

  fd_bam_bundle_result_t res = test_make_bundle_result( 901UL );
  res.execution_success = 0U;
  res.scheduling_error  = FD_BAM_SCHED_ERR_OUTSIDE_SLOT;
  test_enqueue_bundle_result( state, &res );

  int flushed = fd_bam_test_flush_results( state );
  FD_TEST( flushed==1 );
  FD_TEST( state->bam_pending_results==0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.which_versioned_msg==bam_api_SchedulerMessage_v0_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg==bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt==1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result==bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason==bam_types_NotCommitted_scheduling_error_tag );
  FD_TEST( result->result.not_committed.reason.scheduling_error==bam_types_SchedulingError_OUTSIDE_LEADER_SLOT );

  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_result_not_committed_sanitize_error_reason( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  test_bam_prepare_scheduler_stream( state );

  g_clock = (long)11e9;
  test_bam_keepalive_sync( state, g_clock );

  fd_bam_bundle_result_t res = test_make_bundle_result( 902UL );
  res.execution_success   = 0U;
  res.sanitize_success[1] = 0U;
  test_enqueue_bundle_result( state, &res );

  int flushed = fd_bam_test_flush_results( state );
  FD_TEST( flushed==1 );
  FD_TEST( state->bam_pending_results==0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg==bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt==1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result==bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason==bam_types_NotCommitted_deserialization_error_tag );
  FD_TEST( result->result.not_committed.reason.deserialization_error.index==1U );
  FD_TEST( result->result.not_committed.reason.deserialization_error.reason==bam_types_DeserializationErrorReason_SANITIZE_ERROR );

  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_result_not_committed_transaction_error_reason( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  test_bam_prepare_scheduler_stream( state );

  g_clock = (long)12e9;
  test_bam_keepalive_sync( state, g_clock );

  fd_bam_bundle_result_t res = test_make_bundle_result( 903UL );
  res.execution_success  = 0U;
  res.transaction_err[1] = bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND;
  test_enqueue_bundle_result( state, &res );

  int flushed = fd_bam_test_flush_results( state );
  FD_TEST( flushed==1 );
  FD_TEST( state->bam_pending_results==0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg==bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt==1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result==bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason==bam_types_NotCommitted_transaction_error_tag );
  FD_TEST( result->result.not_committed.reason.transaction_error.index==1U );
  FD_TEST( result->result.not_committed.reason.transaction_error.reason==bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND );

  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_result_not_committed_generic_failure_reason( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  test_bam_prepare_scheduler_stream( state );

  g_clock = (long)13e9;
  test_bam_keepalive_sync( state, g_clock );

  fd_bam_bundle_result_t generic = test_make_bundle_result( 904UL );
  generic.execution_success = 0U;
  test_enqueue_bundle_result( state, &generic );

  FD_TEST( fd_bam_test_flush_results( state )==1 );
  FD_TEST( state->bam_pending_results==0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg==bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt==1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result==bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason==bam_types_NotCommitted_generic_invalid_tag );
  FD_TEST( 0==strcmp( result->result.not_committed.reason.generic_invalid.message, "bundle execution failed" ) );

  fd_bam_bundle_result_t invalid = test_make_bundle_result( 905UL );
  invalid.execution_success = 0U;
  invalid.transaction_err[0] = (uint)_bam_types_TransactionErrorReason_ARRAYSIZE;
  test_enqueue_bundle_result( state, &invalid );

  FD_TEST( fd_bam_test_flush_results( state )==1 );
  FD_TEST( state->bam_pending_results==0UL );

  fd_memset( &decoded, 0, sizeof(decoded) );
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg==bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt==1UL );
  result = &decoded.multi.results[0];
  FD_TEST( result->which_result==bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason==bam_types_NotCommitted_generic_invalid_tag );
  FD_TEST( 0==strcmp( result->result.not_committed.reason.generic_invalid.message, "transaction error 39" ) );

  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_result_not_committed_invalid_scheduling_error_reason( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  test_bam_prepare_scheduler_stream( state );

  g_clock = (long)14e9;
  test_bam_keepalive_sync( state, g_clock );

  fd_bam_bundle_result_t res = test_make_bundle_result( 906UL );
  res.execution_success = 0U;
  res.scheduling_error  = (uint)(_bam_types_SchedulingError_MAX + 1);
  test_enqueue_bundle_result( state, &res );

  FD_TEST( fd_bam_test_flush_results( state )==1 );
  FD_TEST( state->bam_pending_results==0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg==bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt==1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result==bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason==bam_types_NotCommitted_generic_invalid_tag );
  FD_TEST( 0==strcmp( result->result.not_committed.reason.generic_invalid.message, "invalid scheduling error 3" ) );

  test_bam_env_destroy( env );
}

static void
test_bam_request_ctx_labels( void ) {
  FD_TEST( 0==strcmp( fd_bam_request_ctx_cstr( FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge ), "BamGetAuthChallenge" ) );
  FD_TEST( 0==strcmp( fd_bam_request_ctx_cstr( FD_BAM_CLIENT_REQ_BAM_GetConfig ), "BamGetBuilderConfig" ) );
  FD_TEST( 0==strcmp( fd_bam_request_ctx_cstr( FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream ), "BamInitSchedulerStream" ) );
  FD_TEST( 0==strcmp( fd_bam_request_ctx_cstr( 99UL ), "unknown" ) );
}

static void
setup_ctrl_defaults( fd_bam_tile_t * ctx,
                     fd_bam_ctrl_t * ctrl ) {
  ctx->ctrl = ctrl;
  fd_memset( ctrl, 0, sizeof(fd_bam_ctrl_t) );

  /* Mirror the on-startup state: control block idle, HTTPS endpoint configured, BAM enabled. */
  ctx->runtime_enabled = 1;
  char const * host = "initial.builder.test";
  ulong host_len = strlen( host );
  FD_TEST( host_len < sizeof( ctx->server_fqdn ) );
  strcpy( ctx->server_fqdn, host );
  ctx->server_fqdn_len = host_len;
  strcpy( ctx->server_sni, host );
  ctx->server_sni_len = host_len;
  ctx->server_tcp_port = 443;
  ctx->is_ssl          = 1;

  ctrl->current_enable = 1U;
  ctrl->enable         = 1U;
  fd_bam_ctrl_copy_str( ctrl->current_url, FD_BAM_CTRL_URL_MAX, "https://initial.builder.test:443" );
  fd_bam_ctrl_copy_str( ctrl->current_sni, FD_BAM_CTRL_SNI_MAX, host );
  ctrl->state = FD_BAM_CTRL_STATE_IDLE;
}

FD_FN_UNUSED static void
test_bam_ctrl_updates_url_and_sni( fd_wksp_t * wksp ) {
  /* Ensure runtime URL/SNI updates reconfigure the client and publish success to the control block. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * ctx = env->state;

  fd_bam_ctrl_t ctrl;
  setup_ctrl_defaults( ctx, &ctrl );

  static fd_keyswitch_t keyswitch = {0};
  keyswitch.magic = FD_KEYSWITCH_MAGIC;
  keyswitch.state = FD_KEYSWITCH_STATE_COMPLETED;
  keyswitch.param = 0UL;
  ctx->keyswitch = &keyswitch;

  ctrl.command = FD_BAM_CTRL_CMD_URL | FD_BAM_CTRL_CMD_SNI;
  ctrl.enable  = 1U;
  fd_bam_ctrl_copy_str( ctrl.url, FD_BAM_CTRL_URL_MAX, "http://new.example.com:8899" );
  fd_bam_ctrl_copy_str( ctrl.sni, FD_BAM_CTRL_SNI_MAX, "custom.sni.invalid" );
  ctrl.state = FD_BAM_CTRL_STATE_REQUEST;

  fd_bam_tile_housekeeping( ctx );

  FD_TEST( ctrl.state==FD_BAM_CTRL_STATE_SUCCESS );
  FD_TEST( ctrl.current_enable==1U );
  FD_TEST( !strcmp( ctrl.current_url, "http://new.example.com:8899" ) );
  FD_TEST( !strcmp( ctrl.current_sni, "custom.sni.invalid" ) );
  FD_TEST( ctx->runtime_enabled==1 );
  FD_TEST( ctx->server_tcp_port==8899 );
  FD_TEST( !strcmp( ctx->server_fqdn, "new.example.com" ) );
  FD_TEST( ctx->server_fqdn_len==strlen( "new.example.com" ) );
  FD_TEST( ctx->is_ssl==0 );
  FD_TEST( !strcmp( ctx->server_sni, "custom.sni.invalid" ) );
  FD_TEST( ctx->server_sni_len==strlen( "custom.sni.invalid" ) );
  FD_TEST( !strcmp( ctx->ctrl->error, "" ) );
  FD_TEST( !strcmp( ctx->grpc_client->host, "custom.sni.invalid" ) );
  FD_TEST( ctx->grpc_client->port==8899 );

  ctx->keyswitch = NULL;
  test_bam_env_destroy( env );
}

FD_FN_UNUSED static void
test_bam_ctrl_toggle_enable_updates_runtime_state( fd_wksp_t * wksp ) {
  /* Validate that toggling enable pauses connectivity and clears the status latch. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * ctx = env->state;

  fd_bam_ctrl_t ctrl;
  setup_ctrl_defaults( ctx, &ctrl );

  static fd_keyswitch_t keyswitch = {0};
  keyswitch.magic = FD_KEYSWITCH_MAGIC;
  keyswitch.state = FD_KEYSWITCH_STATE_COMPLETED;
  keyswitch.param = 0UL;
  ctx->keyswitch = &keyswitch;

  uchar fseq_mem[ FD_FSEQ_FOOTPRINT ] __attribute__((aligned(FD_FSEQ_ALIGN)));
  fd_memset( fseq_mem, 0, sizeof(fseq_mem) );
  void * fseq_shmem = fd_fseq_new( fseq_mem, 0UL );
  FD_TEST( fseq_shmem );
  ulong * fseq = fd_fseq_join( fseq_shmem );
  FD_TEST( fseq );
  fd_fseq_update( fseq, 1UL );
  ctx->bam_status_fseq = fseq;

  ctrl.command = FD_BAM_CTRL_CMD_ENABLE;
  ctrl.enable  = 0U;
  ctrl.state   = FD_BAM_CTRL_STATE_REQUEST;

  fd_bam_tile_housekeeping( ctx );

  FD_TEST( ctrl.state==FD_BAM_CTRL_STATE_SUCCESS );
  FD_TEST( ctrl.current_enable==0U );
  FD_TEST( ctx->runtime_enabled==0 );
  FD_TEST( fd_fseq_query( fseq )==0UL );
  FD_TEST( !strcmp( ctrl.current_url, "https://initial.builder.test:443" ) );

  FD_TEST( fd_fseq_leave( fseq )==fseq_shmem );
  FD_TEST( fd_fseq_delete( fseq_shmem )==fseq_shmem );
  ctx->bam_status_fseq = NULL;
  ctx->keyswitch = NULL;

  test_bam_env_destroy( env );
}

FD_FN_UNUSED static void
test_bam_ctrl_invalid_url_sets_error_and_preserves_config( fd_wksp_t * wksp ) {
  /* Invalid runtime URLs should surface an error without mutating existing configuration. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * ctx = env->state;

  fd_bam_ctrl_t ctrl;
  setup_ctrl_defaults( ctx, &ctrl );

  static fd_keyswitch_t keyswitch = {0};
  keyswitch.magic = FD_KEYSWITCH_MAGIC;
  keyswitch.state = FD_KEYSWITCH_STATE_COMPLETED;
  keyswitch.param = 0UL;
  ctx->keyswitch = &keyswitch;

  ctrl.command = FD_BAM_CTRL_CMD_URL;
  ctrl.enable  = 1U;
  fd_bam_ctrl_copy_str( ctrl.url, FD_BAM_CTRL_URL_MAX, "not a url" );
  ctrl.state = FD_BAM_CTRL_STATE_REQUEST;

  fd_bam_tile_housekeeping( ctx );

  FD_TEST( ctrl.state==FD_BAM_CTRL_STATE_ERROR );
  FD_TEST( strstr( ctrl.error, "Invalid BAM URL" )!=NULL );
  FD_TEST( !strcmp( ctrl.current_url, "https://initial.builder.test:443" ) );
  FD_TEST( !strcmp( ctx->server_fqdn, "initial.builder.test" ) );
  FD_TEST( ctx->runtime_enabled==1 );

  ctx->keyswitch = NULL;
  test_bam_env_destroy( env );
}

static void
test_bam_config_updates_contact_info( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  FD_TEST( state->bam_contact_avail==0U );
  FD_TEST( state->bam_contact_dirty==0U );
  FD_TEST( state->bam_tpu_addr.l==0UL );
  FD_TEST( state->bam_tpu_quic_addr.l==0UL );

  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  resp.has_bam_config = true;
  resp.bam_config.has_tpu_sock = true;
  fd_memset( resp.bam_config.tpu_sock.ip, 0, sizeof( resp.bam_config.tpu_sock.ip ) );
  fd_memcpy( resp.bam_config.tpu_sock.ip, "1.2.3.4", 7UL );
  resp.bam_config.tpu_sock.port = 9000U;
  resp.bam_config.has_tpu_fwd_sock = true;
  fd_memset( resp.bam_config.tpu_fwd_sock.ip, 0, sizeof( resp.bam_config.tpu_fwd_sock.ip ) );
  fd_memcpy( resp.bam_config.tpu_fwd_sock.ip, "5.6.7.8", 7UL );
  resp.bam_config.tpu_fwd_sock.port = 10001U;

  uchar pb_buf[ 256 ];
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             (ulong)ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetConfig );

  FD_TEST( state->bam_contact_avail==1U );
  FD_TEST( state->bam_contact_dirty==1U );

  fd_ip4_port_t expected_tpu = {0};
  FD_TEST( fd_cstr_to_ip4_addr( "1.2.3.4", &expected_tpu.addr ) );
  expected_tpu.port = fd_ushort_bswap( (ushort)9000U );
  FD_TEST( state->bam_tpu_addr.l==expected_tpu.l );

  fd_ip4_port_t expected_quic = {0};
  FD_TEST( fd_cstr_to_ip4_addr( "5.6.7.8", &expected_quic.addr ) );
  expected_quic.port = fd_ushort_bswap( (ushort)10001U );
  FD_TEST( state->bam_tpu_quic_addr.l==expected_quic.l );

  state->bam_contact_dirty = 0U;
  ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             (ulong)ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetConfig );
  FD_TEST( state->bam_contact_dirty==0U );
  FD_TEST( state->bam_tpu_addr.l==expected_tpu.l );
  FD_TEST( state->bam_tpu_quic_addr.l==expected_quic.l );

  resp.bam_config.has_tpu_fwd_sock = false;
  fd_memset( resp.bam_config.tpu_fwd_sock.ip, 0, sizeof( resp.bam_config.tpu_fwd_sock.ip ) );
  resp.bam_config.tpu_fwd_sock.port = 0U;
  ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             (ulong)ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetConfig );
  FD_TEST( state->bam_contact_dirty==1U );
  FD_TEST( state->bam_contact_avail==1U );
  FD_TEST( state->bam_tpu_addr.l==expected_tpu.l );
  FD_TEST( state->bam_tpu_quic_addr.l==0UL );

  test_bam_env_destroy( env );
}

static void
test_bam_builder_fee_info( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  uchar pb_buf[ 256 ];
  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  resp.has_block_engine_config = true;
  uchar pubkey[32] = {0};
  pubkey[0] = 1U;
  pubkey[1] = 2U;
  pubkey[2] = 3U;
  pubkey[3] = 4U;
  pubkey[4] = 5U;
  FD_TEST( fd_base58_encode_32( pubkey, NULL, resp.block_engine_config.builder_pubkey ) );
  resp.block_engine_config.builder_pubkey[ FD_BASE58_ENCODED_32_SZ-1 ] = '\0';
  resp.block_engine_config.builder_commission = 5U;
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  if( FD_UNLIKELY( !pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) ) ) {
    FD_LOG_ERR(( "pb_encode fee info failed: %s", PB_GET_ERROR( &ostream ) ));
  }

  FD_TEST( state->builder_info_avail==0U );
  fd_bam_client_grpc_rx_msg( state, pb_buf, ostream.bytes_written, FD_BAM_CLIENT_REQ_BAM_GetConfig );
  FD_TEST( state->builder_info_avail==1U );
  FD_TEST( state->builder_commission==5U );
  uchar decoded[32];
  FD_TEST( fd_base58_decode_32( resp.block_engine_config.builder_pubkey, decoded ) );
  FD_TEST( 0==memcmp( state->builder_pubkey, decoded, 32UL ) );

  test_bam_env_destroy( env );
}

/* Verifies that bundle results buffered in the queue survive
 * fd_bam_client_reset and remain available for flushing after reconnect.
 * Ensure that bundle results aren't lost during temporary disconnections. */

static void
test_bam_bundle_result_queue_survives_reset( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  for( ulong i=0UL; i<3UL; i++ ) {
    fd_bam_bundle_result_t res = test_make_bundle_result( 100UL + i );
    test_enqueue_bundle_result( state, &res );
  }

  ulong expected_tail = state->bam_results_tail;
  fd_bam_client_reset( state );

  FD_TEST( state->bam_pending_results==3UL );
  FD_TEST( state->bam_results_head==0UL );
  FD_TEST( state->bam_results_tail==expected_tail );
  FD_TEST( state->bam_results[0].bundle_id==100UL );
  FD_TEST( state->bam_results[1].bundle_id==101UL );
  FD_TEST( state->bam_results[2].bundle_id==102UL );

  test_bam_env_destroy( env );
}

/* Verifies that buffered bundle results flush cleanly once a new scheduler stream is
   established. Tests that the queue drains completely and head/tail
   pointers are properly updated after successful flush. */

static void
test_bam_bundle_result_queue_flushes_after_reconnect( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  for( ulong i=0UL; i<2UL; i++ ) {
    fd_bam_bundle_result_t res = test_make_bundle_result( 200UL + i );
    test_enqueue_bundle_result( state, &res );
  }

  fd_bam_client_reset( state );
  FD_TEST( state->bam_pending_results==2UL );

  test_bam_env_mock_conn( env );
  state->bam_stream_live = 0U;

  fd_grpc_h2_stream_t * stream = fd_grpc_client_stream_acquire( state->grpc_client, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( stream );
  stream->hdrs.h2_status     = 200;
  stream->hdrs.is_grpc_proto = 1;
  state->bam_stream = stream;
  state->bam_stream_live = 1U;
  state->grpc_client->request_stream = NULL;
  *state->grpc_client->request_tx_op = (fd_h2_tx_op_t){0};

  int flushed = fd_bam_test_flush_results( state );
  FD_TEST( flushed==1 );
  FD_TEST( state->bam_pending_results==0UL );
  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg==bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt==1UL );
  FD_TEST( decoded.multi.results[0].seq_id==201U );
  FD_TEST( state->bam_results_head==state->bam_results_tail );

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
  test_bam_scheduler_auth_proof_publishes_message( wksp );
  test_bam_scheduler_heartbeat_publishes_message( wksp );
  test_bam_scheduler_leader_state_publishes_message( wksp );
  test_bam_scheduler_result_publishes_message( wksp );
  test_bam_scheduler_result_not_committed_publishes_message( wksp );
  test_bam_scheduler_result_not_committed_sanitize_error_reason( wksp );
  test_bam_scheduler_result_not_committed_transaction_error_reason( wksp );
  test_bam_scheduler_result_not_committed_generic_failure_reason( wksp );
  test_bam_scheduler_result_not_committed_invalid_scheduling_error_reason( wksp );
  test_bam_heartbeat_timeout_forces_disconnect( wksp );
  test_bam_heartbeat_reset_extends_timeout( wksp );
  test_bam_client_status( wksp );
  test_bam_auth_challenge_response_sets_signature( wksp );
  test_bam_request_ctx_labels();
  test_bam_ctrl_updates_url_and_sni( wksp );
  test_bam_ctrl_toggle_enable_updates_runtime_state( wksp );
  test_bam_ctrl_invalid_url_sets_error_and_preserves_config( wksp );
  test_bam_config_updates_contact_info( wksp );
  test_bam_builder_fee_info( wksp );
  test_bam_bundle_result_queue_survives_reset( wksp );
  test_bam_bundle_result_queue_flushes_after_reconnect( wksp );

  fd_wksp_usage_t wksp_usage;
  FD_TEST( fd_wksp_usage( wksp, NULL, 0UL, &wksp_usage ) );
  FD_TEST( wksp_usage.free_cnt==wksp_usage.total_cnt );

  fd_wksp_delete_anonymous( wksp );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
