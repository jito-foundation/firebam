#define _GNU_SOURCE

#include "test_bam_common.c"
#include "proto/bam_api.pb.h"
#include "proto/bam_types.pb.h"
#include "fd_bam_types.h"
#include "../../ballet/base58/fd_base58.h"
#include "../../ballet/nanopb/pb_encode.h"
#include "../../ballet/nanopb/pb_decode.h"
#include "../../waltz/grpc/fd_grpc_codec.h"
#include "../pack/fd_pack_tile_bam_fee.h"
#include "../bundle/fd_bundle_crank.h"
#include "../../util/fd_util.h"
#include <stdbool.h>
#include <stdint.h>
#include "../../tango/fseq/fd_fseq.h"
#include <limits.h>
#include <string.h>

#include "fd_bam_tile.h"

static uchar metrics_scratch[ FD_METRICS_FOOTPRINT( 0UL, 0UL ) ] __attribute__((aligned( FD_METRICS_ALIGN )));


__attribute__((weak)) char const fdctl_version_string[] = "0.0.0";

static long g_clock = 1L;

__attribute__((weak)) long
fd_bam_now( void ) {
  return g_clock;
}

static fd_bam_contact_update_t
test_bam_read_gossip_update( fd_wksp_t * mem,
                             ulong       chunk ) {
  fd_bam_contact_update_t msg;
  fd_memcpy( &msg, fd_chunk_to_laddr( mem, chunk ), sizeof(fd_bam_contact_update_t) );
  return msg;
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
  res.seq_id        = (uint) bundle_id; /* FIXME: broken! */
  res.slot             = bundle_id + 1000UL;
  res.bundle_txn_cnt   = 2;
  res.txn_cnt          = 2;
  res.execution_success = 1;
  res.scheduling_error  = FD_BAM_SCHED_ERR_NONE;
  for( uint i=0U; i<FD_PACK_MAX_TXN_PER_BUNDLE; i++ ) {
    res.transaction_err[ i ]   = 0;
    res.consumed_cus[ i ]      = i + 1;
    res.sanitize_success[ i ]  = 1;
  }
  return res;
}

static void
test_enqueue_bundle_result( fd_bam_tile_t *               state,
                            fd_bam_bundle_result_t const * res ) {
  FD_TEST( state->bam_pending_results < FD_BAM_MAX_PENDING_RESULTS );
  state->bam_results[ state->bam_results_tail ] = *res;
  state->bam_results_tail = (ushort)((state->bam_results_tail + 1U) % FD_BAM_MAX_PENDING_RESULTS);
  state->bam_pending_results = (ushort)( state->bam_pending_results + 1U );
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
test_bam_encode_scheduler_multi_batch_response( bam_types_AtomicTxnBatch * batches,
                                                size_t                     batch_cnt,
                                                uchar *                    out,
                                                size_t                     out_sz ) {
  test_bam_batch_encode_ctx_t batches_ctx = {
      .batches   = batches,
      .batch_cnt = batch_cnt
  };
  bam_api_SchedulerResponse resp = bam_api_SchedulerResponse_init_default;
  resp.which_versioned_msg = bam_api_SchedulerResponse_v0_tag;
  resp.versioned_msg.v0.which_resp = bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag;
  resp.versioned_msg.v0.resp.multiple_atomic_txn_batch.batches.funcs.encode = test_bam_encode_batches_cb;
  resp.versioned_msg.v0.resp.multiple_atomic_txn_batch.batches.arg          = &batches_ctx;

  pb_ostream_t ostream = pb_ostream_from_buffer( out, out_sz );
  if( FD_UNLIKELY( !pb_encode( &ostream, bam_api_SchedulerResponse_fields, &resp ) ) ) {
    FD_LOG_ERR(( "SchedulerResponse encode failed (batch_cnt=%lu, out_sz=%lu): %s", batch_cnt, out_sz, PB_GET_ERROR( &ostream ) ));
  }
  return ostream.bytes_written;
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
  batch.max_schedule_slot = FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT;
  batch.packets.funcs.encode = test_bam_encode_packets_cb;
  batch.packets.arg          = &packets_ctx;

  return test_bam_encode_scheduler_multi_batch_response( &batch, 1UL, out, out_sz );
}

static size_t
test_bam_build_scheduler_batch_msg(uchar *  out,
                                   size_t   out_sz,
                                   uint32_t seq_id,
                                   uchar    packet_cnt,
                                   int      revert_on_error) {
  bam_types_Packet packets[ packet_cnt ];
  fd_memset( packets, 0, sizeof( packets ) );
  for( size_t i=0; i<packet_cnt; i++ ) {
    packets[ i ].data.size = (pb_size_t)(i + 1);
    for( pb_size_t j=0; j<packets[ i ].data.size; j++ ) {
      packets[ i ].data.bytes[ j ] = (uchar)( 'A' + (int)i + (int)j );
    }
    if( revert_on_error ) {
      packets[ i ].has_meta        = 1;
      packets[ i ].meta.has_flags  = 1;
      packets[ i ].meta.flags.revert_on_error = 1;
    }
  }
  return test_bam_encode_scheduler_response( packets, packet_cnt, seq_id, out, out_sz );
}

static void
test_bam_send_scheduler_heartbeat( fd_bam_tile_t * state,
                                   ulong          time_sent_microseconds ) {
  uchar protobuf[64];
  bam_api_SchedulerResponse resp = bam_api_SchedulerResponse_init_default;
  resp.which_versioned_msg = bam_api_SchedulerResponse_v0_tag;
  resp.versioned_msg.v0.which_resp = bam_api_SchedulerResponseV0_heart_beat_tag;
  resp.versioned_msg.v0.resp.heart_beat.time_sent_microseconds = time_sent_microseconds;

  pb_ostream_t ostream = pb_ostream_from_buffer( protobuf, sizeof(protobuf) );
  FD_TEST( pb_encode( &ostream, bam_api_SchedulerResponse_fields, &resp ) );
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
}

static void
test_bam_send_scheduler_bundle( fd_bam_tile_t * state,
                                uint32_t        seq_id,
                                int             revert_on_error ) {
  uchar protobuf[256];
  size_t protobuf_sz = test_bam_build_scheduler_batch_msg( protobuf, sizeof(protobuf), seq_id, 2, revert_on_error);
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
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
      FD_TEST( wire_type == PB_WT_STRING );
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
      FD_TEST( wire_type == PB_WT_VARINT );
      uint64_t val = 0;
      FD_TEST( pb_decode_varint( stream, &val ) );
      out->seq_id = (uint32_t)val;
      break;
    }
    case bam_types_AtomicTxnBatchResult_committed_tag: {
      FD_TEST( wire_type == PB_WT_STRING );
      pb_istream_t substream;
      FD_TEST( pb_make_string_substream( stream, &substream ) );
      out->which_result = bam_types_AtomicTxnBatchResult_committed_tag;
      test_bam_decode_committed_results( &substream, committed );
      pb_close_string_substream( stream, &substream );
      break;
    }
    case bam_types_AtomicTxnBatchResult_not_committed_tag: {
      FD_TEST( wire_type == PB_WT_STRING );
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
      FD_TEST( wire_type == PB_WT_STRING );
      pb_istream_t hb_stream;
      FD_TEST( pb_make_string_substream( stream, &hb_stream ) );
      out->msg.versioned_msg.v0.which_msg = bam_api_SchedulerMessageV0_heart_beat_tag;
      FD_TEST( pb_decode( &hb_stream, bam_types_ValidatorHeartBeat_fields,
                          &out->msg.versioned_msg.v0.msg.heart_beat ) );
      pb_close_string_substream( stream, &hb_stream );
      break;
    case bam_api_SchedulerMessageV0_auth_proof_tag: {
      FD_TEST( wire_type == PB_WT_STRING );
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
      FD_TEST( wire_type == PB_WT_STRING );
      pb_istream_t ls_stream;
      FD_TEST( pb_make_string_substream( stream, &ls_stream ) );
      out->msg.versioned_msg.v0.which_msg = bam_api_SchedulerMessageV0_leader_state_tag;
      FD_TEST( pb_decode( &ls_stream, bam_types_LeaderState_fields,
                          &out->msg.versioned_msg.v0.msg.leader_state ) );
      pb_close_string_substream( stream, &ls_stream );
      break;
    case bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag: {
      FD_TEST( wire_type == PB_WT_STRING );
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
    if( tag == bam_api_SchedulerMessage_v0_tag && wire_type == PB_WT_STRING ) {
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

/* Verify that scheduler batches without revert_on_error are fanned out
 * as individual bundle-sourced transactions with fragment metadata.
 */
static void
test_bam_packets_forwarded( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  uchar protobuf[256];
  size_t protobuf_sz = test_bam_build_scheduler_batch_msg( protobuf, sizeof(protobuf), 0U, 2, 0);

  FD_TEST( state->metrics.txn_received_cnt == 0UL );
  FD_TEST( state->metrics.bundle_received_cnt == 0UL );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.txn_received_cnt == 2UL );
  FD_TEST( state->metrics.bundle_received_cnt == 0UL );

  zero_meta_ts( env->out_mcache, 2UL );
  fd_frag_meta_t expected[2] = {
    { .seq=0UL, .sig=0UL, .chunk=0UL, .sz=(ushort)(sizeof(fd_txn_m_t)+1UL), .ctl=0U },
    { .seq=1UL, .sig=0UL, .chunk=2UL, .sz=(ushort)(sizeof(fd_txn_m_t)+2UL), .ctl=0U }
  };
  FD_TEST( fd_memeq( env->out_mcache, expected, sizeof(expected) ) );

  fd_txn_m_t * first = (fd_txn_m_t *)fd_chunk_to_laddr( state->verify_out.mem, 0UL );
  FD_TEST( first->source_tpu    == FD_TXN_M_TPU_SOURCE_BAM );
  FD_TEST( first->bam.seq_id    == 0U );
  FD_TEST( first->bam.batch_cnt == 1UL );

  test_bam_env_destroy( env );
}

static void
/* Validates that a scheduler response carrying multiple AtomicTxnBatch entries
   fans out into sequential fragments with the correct metadata for each batch. */
test_bam_multiple_batches_forwarded( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;
  test_bam_env_mock_builder_info( state );

  zero_meta_ts( env->out_mcache, 3UL );

  bam_types_Packet first_packets[1];
  fd_memset( first_packets, 0, sizeof(first_packets) );
  first_packets[0].data.size     = 1U;
  first_packets[0].data.bytes[0] = (uchar)'m';

  test_bam_packet_encode_ctx_t first_ctx = {
      .packets    = first_packets,
      .packet_cnt = 1UL
  };

  bam_types_Packet second_packets[2];
  fd_memset( second_packets, 0, sizeof(second_packets) );
  for( size_t i=0UL; i<2UL; i++ ) {
    second_packets[ i ].data.size     = 1U;
    second_packets[ i ].data.bytes[0] = (uchar)('n' + (int)i);
    second_packets[ i ].has_meta = 1U;
    second_packets[ i ].meta.has_flags = 1U;
    second_packets[ i ].meta.flags.revert_on_error = 1U;
  }

  test_bam_packet_encode_ctx_t second_ctx = {
      .packets    = second_packets,
      .packet_cnt = 2UL
  };

  bam_types_AtomicTxnBatch batches[2];
  batches[0] = (bam_types_AtomicTxnBatch)bam_types_AtomicTxnBatch_init_default;
  batches[0].seq_id = 6U;
  batches[0].max_schedule_slot = FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT;
  batches[0].packets.funcs.encode = test_bam_encode_packets_cb;
  batches[0].packets.arg          = &first_ctx;

  batches[1] = (bam_types_AtomicTxnBatch)bam_types_AtomicTxnBatch_init_default;
  batches[1].seq_id = 7U;
  batches[1].max_schedule_slot = FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT;
  batches[1].packets.funcs.encode = test_bam_encode_packets_cb;
  batches[1].packets.arg          = &second_ctx;

  uchar protobuf[512];
  size_t protobuf_sz = test_bam_encode_scheduler_multi_batch_response( batches, 2UL, protobuf, sizeof(protobuf) );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.txn_received_cnt == 3UL );
  FD_TEST( state->metrics.bundle_received_cnt == 1UL );
  FD_TEST( state->bundle_seq == 7U );
  FD_TEST( state->bundle_txn_cnt == 2U );

  fd_frag_meta_t * meta = env->out_mcache;
  FD_TEST( meta[0].seq == 0UL );
  FD_TEST( meta[1].seq == 1UL );
  FD_TEST( meta[2].seq == 2UL );

  fd_txn_m_t * tx0 = fd_chunk_to_laddr( state->verify_out.mem, meta[0].chunk );
  fd_txn_m_t * tx1 = fd_chunk_to_laddr( state->verify_out.mem, meta[1].chunk );
  fd_txn_m_t * tx2 = fd_chunk_to_laddr( state->verify_out.mem, meta[2].chunk );

  FD_TEST( tx0->bam.seq_id == 6U );
  FD_TEST( tx0->bam.revert_on_error == 0U );
  FD_TEST( tx0->bam.batch_cnt == 1U );

  FD_TEST( tx1->bam.seq_id == 7U );
  FD_TEST( tx1->bam.revert_on_error == 1U );
  FD_TEST( tx1->bam.batch_cnt == 2U );
  FD_TEST( tx1->bam.batch_idx == 0U );
  FD_TEST( tx1->source_tpu == FD_TXN_M_TPU_SOURCE_BAM );

  FD_TEST( tx2->bam.seq_id == 7U );
  FD_TEST( tx2->bam.revert_on_error == 1U );
  FD_TEST( tx2->bam.batch_cnt == 2U );
  FD_TEST( tx2->bam.batch_idx == 1U );
  FD_TEST( tx2->source_tpu == FD_TXN_M_TPU_SOURCE_BAM );

  uchar const * payload0 = fd_txn_m_payload( tx0 );
  uchar const * payload1 = fd_txn_m_payload( tx1 );
  uchar const * payload2 = fd_txn_m_payload( tx2 );
  FD_TEST( payload0[0] == 'm' );
  FD_TEST( payload1[0] == 'n' );
  FD_TEST( payload2[0] == 'o' );

  test_bam_env_destroy( env );
}

static void
test_bam_bundle_forwarded( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;
  test_bam_env_mock_builder_info( state );

  FD_TEST( state->metrics.bundle_received_cnt == 0UL );

  uchar protobuf[512];
  size_t protobuf_sz = test_bam_build_scheduler_batch_msg( protobuf, sizeof(protobuf), 1U, 2, 1);

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.bundle_received_cnt == 1UL );
  FD_TEST( state->bundle_seq == 1U );
  FD_TEST( state->metrics.txn_received_cnt > 0UL );

  fd_txn_m_t * first = (fd_txn_m_t *)fd_chunk_to_laddr( state->verify_out.mem, 0UL );
  FD_TEST( first->source_tpu == FD_TXN_M_TPU_SOURCE_BAM );
  FD_TEST( first->bam.seq_id == 1U );
  FD_TEST( first->bam.revert_on_error == 1U );
  FD_TEST( first->bam.batch_cnt >= 1U );
  FD_TEST( first->bam.batch_idx == 0U );

  test_bam_env_destroy( env );
}

static void
/* Ensures truncated scheduler responses trigger decode_fail accounting and
   drop the message without emitting any downstream fragments. */
test_bam_scheduler_truncated_message_dropped( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  fd_memset( env->out_mcache, 0, 3UL * sizeof(fd_frag_meta_t) );

  uchar protobuf[256];
  size_t protobuf_sz = test_bam_build_scheduler_batch_msg( protobuf, sizeof(protobuf), 5U, 2, 0);
  FD_TEST( protobuf_sz > 1UL );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz - 1UL,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.decode_fail_cnt == 1UL );
  FD_TEST( state->metrics.txn_received_cnt == 0UL );
  FD_TEST( state->metrics.bundle_received_cnt == 0UL );
  FD_TEST( state->metrics.packet_drop_cnt == 0UL );
  FD_TEST( env->stem_seqs[0] == 0UL );
  FD_TEST( env->out_mcache[0].seq == 0UL );
  FD_TEST( env->out_mcache[0].sz == 0 );

  test_bam_env_destroy( env );
}

static void
test_bam_bundle_requires_builder_info( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  FD_TEST( state->metrics.bundle_received_cnt == 0UL );
  uchar protobuf[512];
  size_t protobuf_sz = test_bam_build_scheduler_batch_msg( protobuf, sizeof(protobuf), 2U, 2, 1);
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( state->metrics.bundle_received_cnt == 0UL );
  FD_TEST( state->metrics.missing_builder_info_fail_cnt == 1UL );
  FD_TEST( state->metrics.txn_received_cnt == 0UL );
  FD_TEST( state->bam_pending_results == 1UL );
  fd_bam_bundle_result_t const * res = &state->bam_results[ state->bam_results_head ];
  FD_TEST( res->seq_id == 2UL );
  FD_TEST( res->execution_success == 0U );
  FD_TEST( res->txn_cnt == 2U );

  test_bam_env_mock_conn_empty( env );
  test_bam_env_mock_h2_hs( state );
  test_bam_prepare_scheduler_stream( state );
  g_clock = (long)14e9;
  test_bam_keepalive_sync( state, g_clock );
  state->bam_last_config_poll_ns = g_clock;

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->bam_pending_results == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.which_versioned_msg == bam_api_SchedulerMessage_v0_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_generic_invalid_tag );
  FD_TEST( 0 == strcmp( result->result.not_committed.reason.generic_invalid.message,
                      FD_BAM_ERR_MSG_BUILDER_INFO_UNAVAILABLE ) );

  test_bam_env_destroy( env );
}

static void
test_bam_bundle_rejects_mixed_revert_flags( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  bam_types_Packet packets[ 2 ];
  fd_memset( packets, 0, sizeof( packets ) );
  for( size_t i=0UL; i<2UL; i++ ) {
    packets[ i ].data.size = 1U;
    packets[ i ].data.bytes[0] = (uchar)( 'Z' + (int)i );
    packets[ i ].has_meta = 1;
    packets[ i ].meta.has_flags = 1;
    packets[ i ].meta.flags.revert_on_error = (i == 0UL);
  }

  uchar protobuf[256];
  size_t protobuf_sz = test_bam_encode_scheduler_response( packets, 2UL, 42U, protobuf, sizeof( protobuf ) );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.bundle_received_cnt == 0UL );
  FD_TEST( state->bam_pending_results == 1UL );

  test_bam_prepare_scheduler_stream( state );
  g_clock = (long)15e9;
  test_bam_keepalive_sync( state, g_clock );
  state->bam_last_config_poll_ns = g_clock;

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->bam_pending_results == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_deserialization_error_tag );
  FD_TEST( result->result.not_committed.reason.deserialization_error.reason == bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE );

  test_bam_env_destroy( env );
}

static void
test_bam_bundle_rejects_vote_transactions( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  bam_types_Packet packets[ 2 ];
  fd_memset( packets, 0, sizeof( packets ) );
  for( size_t i=0UL; i<2UL; i++ ) {
    packets[ i ].data.size = 1U;
    packets[ i ].data.bytes[0] = (uchar)( 'a' + (int)i );
    packets[ i ].has_meta = 1;
    packets[ i ].meta.has_flags = 1;
    packets[ i ].meta.flags.revert_on_error = (i == 0UL); /* vary to ensure vote rejection independent of flag */
    packets[ i ].meta.flags.simple_vote_tx  = (i == 0UL);
  }

  uchar protobuf[256];
  size_t protobuf_sz = test_bam_encode_scheduler_response( packets, 2UL, 43U, protobuf, sizeof( protobuf ) );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.bundle_received_cnt == 0UL );
  FD_TEST( state->bam_pending_results == 1UL );

  test_bam_prepare_scheduler_stream( state );
  g_clock = (long)16e9;
  test_bam_keepalive_sync( state, g_clock );
  state->bam_last_config_poll_ns = g_clock;

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->bam_pending_results == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_deserialization_error_tag );
  FD_TEST( result->result.not_committed.reason.deserialization_error.reason == bam_types_DeserializationErrorReason_VOTE_TRANSACTION_FAILURE );

  test_bam_env_destroy( env );
}

static void
test_bam_bundle_rejects_excess_packet_count( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  size_t packet_cnt = FD_PACK_MAX_TXN_PER_BUNDLE + 1UL;
  bam_types_Packet packets[ FD_PACK_MAX_TXN_PER_BUNDLE + 1UL ];
  fd_memset( packets, 0, sizeof( packets ) );
  for( size_t i=0UL; i<packet_cnt; i++ ) {
    packets[ i ].data.size = 1U;
    packets[ i ].data.bytes[0] = (uchar)( 'a' + (int)( i % 26UL ) );
  }

  uchar protobuf[ 2048 ];
  size_t protobuf_sz = test_bam_encode_scheduler_response( packets, packet_cnt, 50U, protobuf, sizeof( protobuf ) );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.bundle_received_cnt == 0UL );
  FD_TEST( state->metrics.txn_received_cnt == 0UL );
  FD_TEST( state->bam_pending_results == 1UL );

  test_bam_prepare_scheduler_stream( state );
  g_clock = (long)17e9;
  test_bam_keepalive_sync( state, g_clock );
  state->bam_last_config_poll_ns = g_clock;

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->bam_pending_results == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_deserialization_error_tag );
  FD_TEST( result->result.not_committed.reason.deserialization_error.reason ==
           bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE );
  FD_TEST( result->result.not_committed.reason.deserialization_error.index ==
           (uint32_t)FD_PACK_MAX_TXN_PER_BUNDLE );

  test_bam_env_destroy( env );
}

static void
test_bam_bundle_rejects_oversized_packet( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  bam_types_Packet packets[1];
  packets[0].data.size = FD_TXN_MTU; // can't do + 1 here for overflow, otherwise test will panic
  packets[0].has_meta = 1U;
  packets[0].meta.size = FD_TXN_MTU + 1;
  for( pb_size_t i=0U; i<FD_TXN_MTU; i++ ) {
    packets[0].data.bytes[ i ] = (uchar)i;
  }

  uchar protobuf[ 4096 ];
  size_t protobuf_sz = test_bam_encode_scheduler_response( packets, 1UL, 51U, protobuf, sizeof( protobuf ) );

  FD_TEST( state->metrics.packet_drop_cnt == 0UL );
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.packet_drop_cnt == 1UL );
  FD_TEST( state->metrics.txn_received_cnt == 0UL );
  FD_TEST( state->bam_pending_results == 1UL );

  test_bam_prepare_scheduler_stream( state );
  g_clock = (long)18e9;
  test_bam_keepalive_sync( state, g_clock );
  state->bam_last_config_poll_ns = g_clock;

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->bam_pending_results == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_generic_invalid_tag );
  FD_TEST( strstr( result->result.not_committed.reason.generic_invalid.message, "exceeds MTU" ) != NULL );

  test_bam_env_destroy( env );
}

/* Ensure a scheduler batch containing zero packets is surfaced as an EMPTY
   deserialization error while leaving bundle/txn metrics untouched. */
static void
test_bam_bundle_rejects_empty_batch( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  uchar protobuf[256];
  test_bam_packet_encode_ctx_t packets_ctx = {
    .packets    = NULL,
    .packet_cnt = 0UL
  };

  bam_types_AtomicTxnBatch batch = bam_types_AtomicTxnBatch_init_default;
  batch.seq_id = 55;
  batch.max_schedule_slot = FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT;
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

  pb_ostream_t ostream = pb_ostream_from_buffer( protobuf, sizeof( protobuf ) );
  FD_TEST( pb_encode( &ostream, bam_api_SchedulerResponse_fields, &resp ) );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.bundle_received_cnt == 0UL );
  FD_TEST( state->metrics.txn_received_cnt == 0UL );
  FD_TEST( state->bam_pending_results == 1UL );

  test_bam_prepare_scheduler_stream( state );
  g_clock = (long)19e9;
  test_bam_keepalive_sync( state, g_clock );
  state->bam_last_config_poll_ns = g_clock;

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->bam_pending_results == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->seq_id == 55U );
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_deserialization_error_tag );
  FD_TEST( result->result.not_committed.reason.deserialization_error.reason == bam_types_DeserializationErrorReason_EMPTY );
  FD_TEST( result->result.not_committed.reason.deserialization_error.index == 0U );

  test_bam_env_destroy( env );
}

/* Ensure an InitSchedulerStream response that omits the batches array entirely
   is also treated as an EMPTY deserialization error and returns a not-committed
   result back to the scheduler. */
static void
test_bam_bundle_rejects_missing_batches( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  uchar protobuf[256];
  size_t out_sz = sizeof( protobuf );
  test_bam_batch_encode_ctx_t batches_ctx = {
    .batches   = NULL,
    .batch_cnt = 0UL
  };

  bam_types_MultipleAtomicTxnBatch multi = bam_types_MultipleAtomicTxnBatch_init_default;
  multi.batches.funcs.encode = test_bam_encode_batches_cb;
  multi.batches.arg          = &batches_ctx;

  bam_api_SchedulerResponse resp = bam_api_SchedulerResponse_init_default;
  resp.which_versioned_msg = bam_api_SchedulerResponse_v0_tag;
  resp.versioned_msg.v0.which_resp = bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag;
  resp.versioned_msg.v0.resp.multiple_atomic_txn_batch = multi;

  pb_ostream_t ostream = pb_ostream_from_buffer( protobuf, out_sz );
  FD_TEST( pb_encode( &ostream, bam_api_SchedulerResponse_fields, &resp ) );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.bundle_received_cnt == 0UL );
  FD_TEST( state->metrics.txn_received_cnt == 0UL );
  FD_TEST( state->bam_pending_results == 1UL );

  test_bam_prepare_scheduler_stream( state );
  g_clock = (long)20e9;
  test_bam_keepalive_sync( state, g_clock );
  state->bam_last_config_poll_ns = g_clock;

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->bam_pending_results == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->seq_id == 0U );
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_deserialization_error_tag );
  FD_TEST( result->result.not_committed.reason.deserialization_error.reason == bam_types_DeserializationErrorReason_EMPTY );
  FD_TEST( result->result.not_committed.reason.deserialization_error.index == 0U );

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
  FD_TEST( state->bam_stream_live == 1U );

  fd_grpc_resp_hdrs_t hdrs_fail = {
      .h2_status   = 200,
      .grpc_status = FD_GRPC_STATUS_UNAVAILABLE
  };
  fd_bam_client_grpc_rx_end( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream, &hdrs_fail );
  FD_TEST( state->bam_stream_live == 0U );
  FD_TEST( state->bam_stream == NULL );
  FD_TEST( state->defer_reset == 0U );

  stream = fd_grpc_client_stream_acquire( client, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( stream );
  stream->hdrs.h2_status     = 200;
  stream->hdrs.is_grpc_proto = 1;
  state->bam_stream = stream;
  fd_bam_client_grpc_rx_start( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( state->bam_stream_live == 1U );

  fd_grpc_resp_hdrs_t hdrs_ok = {
      .h2_status   = 200,
      .grpc_status = FD_GRPC_STATUS_OK
  };
  fd_bam_client_grpc_rx_end( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream, &hdrs_ok );
  FD_TEST( state->bam_stream_live == 0U );
  FD_TEST( state->bam_stream == NULL );
  FD_TEST( state->defer_reset == 0U );

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
  state->bam_challenge_to_sign_len = 16U;
  fd_bam_client_grpc_rx_timeout( state, FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge, FD_GRPC_DEADLINE_HEADER );
  FD_TEST( state->bam_auth_inflight == 0U );
  FD_TEST( state->bam_auth_ready == 0U );
  FD_TEST( state->bam_challenge_to_sign_len == 0U );
  FD_TEST( state->defer_reset == 1U );

  state->defer_reset = 0U;
  state->bam_stream_live       = 1U;
  state->bam_stream_connecting = 1U;
  state->bam_leader_pending    = 1U;
  fd_grpc_h2_stream_t * timeout_stream = fd_grpc_client_stream_acquire( state->grpc_client, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( timeout_stream );
  state->bam_stream = timeout_stream;
  fd_bam_client_grpc_rx_timeout( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream, FD_GRPC_DEADLINE_RX_END );
  FD_TEST( state->bam_stream_live == 0U );
  FD_TEST( state->bam_stream_connecting == 0U );
  FD_TEST( state->bam_leader_pending == 0U );
  FD_TEST( state->defer_reset == 1U );

  test_bam_env_destroy( env );
}

static fd_bam_tile_t *
test_bam_heartbeat_env_start( test_bam_env_t * env,
                              fd_wksp_t *      wksp ) {
  g_clock = (long)1e9;
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;
  state->enabled = 1;
  state->keepalive->ts_deadline = LONG_MAX;
  state->keepalive->ts_last_tx = g_clock;
  state->keepalive->ts_last_rx = g_clock;
  state->keepalive->inflight   = 0;
  state->defer_reset = 0;
  test_bam_keepalive_sync( state, g_clock );
  state->keepalive_interval    = LONG_MAX;
  state->keepalive->interval   = 0L;
  state->keepalive->timeout    = LONG_MAX;
  state->keepalive->ts_next_tx = LONG_MAX;
  state->keepalive->ts_deadline = LONG_MAX;
  state->keepalive->inflight   = 0U;
  fd_bam_client_grpc_rx_start( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( state->bam_last_builder_heartbeat_ns == g_clock );
  FD_TEST( state->bam_last_validator_heartbeat_ns == g_clock );
  return state;
}

static void
test_bam_heartbeat_timeout_forces_disconnect( fd_wksp_t * wksp ) {
  /* Test 1: Watchdog drops the connection once the builder heartbeat is stale (>6s) while streams are live. */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    int charge_busy = 0;
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS + (long)1e8;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->tcp_sock == -1 );
    FD_TEST( state->tcp_sock_connected == 0U );
    FD_TEST( charge_busy == 1 );
    test_bam_env_destroy( env );
  }

  /* Test 2: An uninitialized heartbeat timestamp (0L) should never trip the watchdog. */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    state->bam_last_builder_heartbeat_ns = 0L;
    int charge_busy = 0;
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS + (long)1e9;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->tcp_sock >= 0 );
    FD_TEST( state->tcp_sock_connected == 1U );
    test_bam_env_destroy( env );
  }

  /* Test 3: If the scheduler stream is down, the watchdog should stay idle even with stale timestamps. */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    state->bam_stream_live = 0;
    int charge_busy = 0;
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS + (long)1e9;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->tcp_sock >= 0 );
    test_bam_env_destroy( env );
  }

  /* Test 4: Just before the 6s boundary we should not disconnect (off-by-one guard). */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    int charge_busy = 0;
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS - (long)1e6; /* 1ms before timeout */
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->tcp_sock >= 0 );
    test_bam_env_destroy( env );
  }

  /* Test 5: The exact 6s boundary should still trigger a disconnect. */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    int charge_busy = 0;
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->tcp_sock == -1 );
    FD_TEST( charge_busy == 1 );
    test_bam_env_destroy( env );
  }

  /* Test 6: Boundary +1ns also triggers, proving the comparison is inclusive. */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    int charge_busy = 0;
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS + 1L;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->tcp_sock == -1 );
    FD_TEST( charge_busy == 1 );
    test_bam_env_destroy( env );
  }

  /* Test 7: Disabling BAM runtime should bypass the watchdog entirely. */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    state->enabled = 0;
    int charge_busy = 0;
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS + (long)1e9;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->tcp_sock >= 0 );
    test_bam_env_destroy( env );
  }
}

static void
test_bam_heartbeat_reset_extends_timeout( fd_wksp_t * wksp ) {
  /* Uses helper-generated scheduler responses to drive heartbeat timestamping
     and ensure batches refresh the watchdog deadline. */

  /* Heartbeat message updates timestamp to prove unsolicited pings refresh the deadline. */
  {
    /* Subtest: direct heartbeat bumps the builder heartbeat timestamp. */
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    test_bam_send_scheduler_heartbeat( state, 1UL );
    long expected_ts = g_clock;
    FD_TEST( state->bam_last_builder_heartbeat_ns == expected_ts );
    FD_TEST( state->metrics.heartbeat_recv_cnt == 1UL );
    test_bam_env_destroy( env );
  }

  /* 5.9 seconds after heartbeat should NOT timeout: verifies a recent heartbeat defers disconnect. */
  {
    /* Subtest: watchdog stays armed-but-waiting when heartbeat is recent. */
    test_bam_env_t env[1];
   fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    int charge_busy = 0;
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS - (long)2e8;
    test_bam_send_scheduler_heartbeat( state, 1UL );
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS - (long)1e8;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->tcp_sock >= 0 );
    test_bam_env_destroy( env );
  }

  /* 6.1 seconds after heartbeat SHOULD timeout: ensures the refreshed deadline still enforces the same limit. */
  {
    /* Subtest: once heartbeat ages past the limit we disconnect even after refresh. */
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    int charge_busy = 0;
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS - (long)2e8;
    test_bam_send_scheduler_heartbeat( state, 1UL );
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS + (long)2e8;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->tcp_sock == -1 );
    FD_TEST( charge_busy == 1 );
    test_bam_env_destroy( env );
  }

  /* Bundle batches update timestamp because executing work should also count as liveness. */
  {
    /* Subtest: executing a bundle refreshes watchdog like a heartbeat. */
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    test_bam_send_scheduler_bundle( state, 0U, 0 );
    long expected_ts = g_clock;
    FD_TEST( state->bam_last_builder_heartbeat_ns == expected_ts );
    test_bam_env_destroy( env );
  }

  /* 5.9 seconds after bundle should NOT timeout: confirms bundle-driven refresh behaves like heartbeats. */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    int charge_busy = 0;
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS - (long)2e8;
    test_bam_send_scheduler_bundle( state, 0U, 0 );
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS - (long)1e8;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->tcp_sock >= 0 );
    test_bam_env_destroy( env );
  }

  /* 6.1 seconds after bundle SHOULD timeout: the refreshed deadline still enforces the same bound. */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    int charge_busy = 0;
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS - (long)2e8;
    test_bam_send_scheduler_bundle( state, 0U, 0 );
    g_clock += FD_BAM_HEARTBEAT_TIMEOUT_NS + (long)2e8;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->tcp_sock == -1 );
    FD_TEST( charge_busy == 1 );
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

  // FIXME: update these tests to check for unhealthy -> healthy new state tracking

  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY );

  state->tcp_sock_connected = 0U;
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED );
  *state = state_backup;

  ushort const conn_dead_flags[] = {
    FD_H2_CONN_FLAGS_DEAD,
    FD_H2_CONN_FLAGS_SEND_GOAWAY
  };
  for( ulong i=0UL; i<sizeof(conn_dead_flags)/sizeof(conn_dead_flags[0]); i++ ) {
    FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY );
    state->grpc_client->conn->flags |= conn_dead_flags[ i ];
    FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED );
    *state->grpc_client = client_backup;
  }

  ushort const conn_prog_flags[] = {
    FD_H2_CONN_FLAGS_CLIENT_INITIAL,
    FD_H2_CONN_FLAGS_WAIT_SETTINGS_ACK_0,
    FD_H2_CONN_FLAGS_WAIT_SETTINGS_0,
    FD_H2_CONN_FLAGS_SERVER_INITIAL
  };
  for( ulong i=0UL; i<sizeof(conn_prog_flags)/sizeof(conn_prog_flags[0]); i++ ) {
    FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY );
    state->grpc_client->conn->flags |= conn_prog_flags[ i ];
    FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTING );
    *state->grpc_client = client_backup;
  }

  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY );
  state->bam_stream_live = 0U;
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTING );
  state->bam_stream_live = 1U;

  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY );
  state->keepalive->inflight = 1U;
  state->keepalive->ts_deadline = state->keepalive->ts_last_tx;
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED );
  *state = state_backup;
  *state->grpc_client = client_backup;

  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY );
  state->grpc_client->h2_hs_done = 0;
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTING );

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
  state->bam_challenge_to_sign_len = (uchar)strlcpy( state->challenge_to_sign, challenge, sizeof(challenge) );

  char const signature[] = "sig-abcdef";
  strlcpy( state->bam_auth_signature, signature, sizeof(signature) );

  char const validator_key[] = "validator-key-test";
  strlcpy( state->bam_identity_pubkey_b58, validator_key, sizeof(validator_key) );

  fd_bam_test_drive( state, g_clock );

  FD_TEST( state->bam_stream != NULL );
  FD_TEST( state->bam_stream_connecting == 1U );
  FD_TEST( state->bam_auth_ready == 0U );
  FD_TEST( state->bam_stream_live == 0U );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.which_versioned_msg == bam_api_SchedulerMessage_v0_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_auth_proof_tag );
  FD_TEST( 0 == strcmp( decoded.msg.versioned_msg.v0.msg.auth_proof.challenge_to_sign, challenge ) );
  FD_TEST( 0 == strcmp( decoded.msg.versioned_msg.v0.msg.auth_proof.signature, signature ) );
  FD_TEST( 0 == strcmp( decoded.msg.versioned_msg.v0.msg.auth_proof.validator_pubkey, validator_key ) );

  test_bam_env_destroy( env );
}

static void
test_bam_auth_challenge_response_sets_signature( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  ulong const depth        = 8UL;
  ulong const request_mtu  = 256UL;
  ulong const response_mtu = 64UL;

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

  ulong request_data_sz = fd_dcache_req_data_sz( request_mtu, depth, 1UL, 1 );
  void * request_dcache_shmem = fd_wksp_alloc_laddr( wksp, fd_dcache_align(), fd_dcache_footprint( request_data_sz, 0UL ), 1UL );
  FD_TEST( request_dcache_shmem );
  uchar * request_data = fd_dcache_join( fd_dcache_new( request_dcache_shmem, request_data_sz, 0UL ) );
  FD_TEST( request_data );

  ulong response_data_sz = fd_dcache_req_data_sz( response_mtu, depth, 1UL, 1 );
  void * response_dcache_shmem = fd_wksp_alloc_laddr( wksp, fd_dcache_align(), fd_dcache_footprint( response_data_sz, 0UL ), 1UL );
  FD_TEST( response_dcache_shmem );
  uchar * response_data = fd_dcache_join( fd_dcache_new( response_dcache_shmem, response_data_sz, 0UL ) );
  FD_TEST( response_data );

  FD_TEST( fd_keyguard_client_new( state->keyguard_client,
                                   request_mcache, request_data,
                                   response_mcache, response_data, request_mtu ) );

  uchar signature[ 64 ];
  for( uchar i=0; i<64; i++ ) signature[ i ] = (uchar)( i + 1 );
  ulong resp_chunk = state->keyguard_client->response_chunk0;
  fd_memcpy( fd_chunk_to_laddr( state->keyguard_client->response_mem, resp_chunk ),
             signature, sizeof(signature) );
  fd_mcache_publish( response_mcache,
                     depth,
                     state->keyguard_client->response_seq,
                     0UL,
                     resp_chunk,
                     sizeof(signature),
                     0UL,
                     0UL,
                     0UL );

  bam_api_AuthChallengeResponse resp = bam_api_AuthChallengeResponse_init_default;
  char const challenge[] = "unit-test-challenge";
  const size_t challenge_len = strlen(challenge);
  FD_TEST( challenge_len < sizeof( resp.challenge_to_sign ) );
  strlcpy( resp.challenge_to_sign, challenge, sizeof( challenge ) );

  uchar pb_buf[ 128 ];
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_AuthChallengeResponse_fields, &resp ) );

  state->bam_auth_inflight = 1U;
  char const validator_key[] = "validator-pubkey-test";
  strlcpy( state->bam_identity_pubkey_b58, validator_key, sizeof( validator_key ) );

  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge );

  FD_TEST( state->bam_auth_inflight == 0U );
  FD_TEST( state->bam_auth_ready == 1U );
  FD_TEST( state->bam_challenge_to_sign_len == challenge_len );
  FD_TEST( 0 == memcmp( state->challenge_to_sign, challenge, challenge_len ) );

  char expected_sig[ FD_BASE58_ENCODED_64_SZ ];
  FD_TEST( fd_base58_encode_64( signature, NULL, expected_sig ) );
  FD_TEST( 0 == strcmp( state->bam_auth_signature, expected_sig ) );
  FD_TEST( state->keyguard_client->request_seq == 1UL );

  uchar  expected_payload[ FD_BAM_AUTH_LABEL_LEN + sizeof(bam_api_AuthChallengeResponse) ];
  FD_TEST( FD_BAM_AUTH_LABEL_LEN + challenge_len <= sizeof( expected_payload ) );
  fd_memcpy( expected_payload, FD_BAM_AUTH_LABEL, FD_BAM_AUTH_LABEL_LEN );
  fd_memcpy( expected_payload + FD_BAM_AUTH_LABEL_LEN, challenge, challenge_len );
  FD_TEST( 0 == memcmp( request_data, expected_payload, FD_BAM_AUTH_LABEL_LEN + challenge_len ) );

  fd_frag_meta_t const * req_meta =
      request_mcache + fd_mcache_line_idx( 0UL, depth );
  FD_TEST( req_meta->sz == (ushort)( FD_BAM_AUTH_LABEL_LEN + challenge_len ) );

  fd_wksp_free_laddr( fd_dcache_delete( fd_dcache_leave( request_data ) ) );
  fd_wksp_free_laddr( fd_dcache_delete( fd_dcache_leave( response_data ) ) );
  fd_wksp_free_laddr( fd_mcache_delete( fd_mcache_leave( request_mcache ) ) );
  fd_wksp_free_laddr( fd_mcache_delete( fd_mcache_leave( response_mcache ) ) );

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
  FD_TEST( busy == 1 );
  FD_TEST( state->bam_last_validator_heartbeat_ns == now );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.which_versioned_msg == bam_api_SchedulerMessage_v0_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_heart_beat_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.msg.heart_beat.time_sent_microseconds == (uint64_t)(now/1000L) );
  FD_TEST( state->metrics.heartbeat_sent_cnt == 1UL );

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
  FD_TEST( busy == 1 );
  FD_TEST( state->bam_leader_pending == 0U );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.which_versioned_msg == bam_api_SchedulerMessage_v0_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_leader_state_tag );
  bam_types_LeaderState const * ls = &decoded.msg.versioned_msg.v0.msg.leader_state;
  FD_TEST( ls->slot == 42UL );
  FD_TEST( ls->tick == 7U );
  FD_TEST( ls->slot_cu_budget_remaining == 123U );

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
  FD_TEST( flushed == 1 );
  FD_TEST( state->bam_pending_results == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.which_versioned_msg == bam_api_SchedulerMessage_v0_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  FD_TEST( decoded.multi.results[0].seq_id == 900U );
  FD_TEST( decoded.multi.results[0].which_result == bam_types_AtomicTxnBatchResult_committed_tag );
  FD_TEST( decoded.multi.committed[0].txn_cnt == res.txn_cnt );
  FD_TEST( decoded.multi.committed[0].txns[0].cus_consumed == (uint32_t)res.consumed_cus[0] );

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
  res.execution_success = 0;
  res.scheduling_error  = FD_BAM_SCHED_ERR_OUTSIDE_SLOT;
  test_enqueue_bundle_result( state, &res );

  int flushed = fd_bam_test_flush_results( state );
  FD_TEST( flushed == 1 );
  FD_TEST( state->bam_pending_results == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.which_versioned_msg == bam_api_SchedulerMessage_v0_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_scheduling_error_tag );
  FD_TEST( result->result.not_committed.reason.scheduling_error == bam_types_SchedulingError_OUTSIDE_LEADER_SLOT );

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
  res.execution_success   = 0;
  res.sanitize_success[1] = 0;
  test_enqueue_bundle_result( state, &res );

  int flushed = fd_bam_test_flush_results( state );
  FD_TEST( flushed == 1 );
  FD_TEST( state->bam_pending_results == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_deserialization_error_tag );
  FD_TEST( result->result.not_committed.reason.deserialization_error.index == 1U );
  FD_TEST( result->result.not_committed.reason.deserialization_error.reason == bam_types_DeserializationErrorReason_SANITIZE_ERROR );

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
  res.execution_success  = 0;
  res.transaction_err[1] = (uchar)bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND;
  test_enqueue_bundle_result( state, &res );

  int flushed = fd_bam_test_flush_results( state );
  FD_TEST( flushed == 1 );
  FD_TEST( state->bam_pending_results == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_transaction_error_tag );
  FD_TEST( result->result.not_committed.reason.transaction_error.index == 1U );
  FD_TEST( result->result.not_committed.reason.transaction_error.reason == bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND );

  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_result_not_committed_transaction_error_high_index( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  test_bam_prepare_scheduler_stream( state );

  g_clock = (long)12e9;
  test_bam_keepalive_sync( state, g_clock );

  fd_bam_bundle_result_t res = test_make_bundle_result( 907UL );
  res.bundle_txn_cnt   = 3;
  res.txn_cnt          = 3;
  res.execution_success = 0;
  for( uint i=0U; i<res.txn_cnt; i++ ) res.sanitize_success[ i ] = 1;
  res.transaction_err[ 2 ] = (uchar)bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND;
  test_enqueue_bundle_result( state, &res );

  int flushed = fd_bam_test_flush_results( state );
  FD_TEST( flushed == 1 );
  FD_TEST( state->bam_pending_results == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_transaction_error_tag );
  FD_TEST( result->result.not_committed.reason.transaction_error.index == 2U );
  FD_TEST( result->result.not_committed.reason.transaction_error.reason == bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND );

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

  /* Case 1: generic execution failure yields generic_invalid */
  fd_bam_bundle_result_t generic = test_make_bundle_result( 904UL );
  generic.execution_success = 0;
  test_enqueue_bundle_result( state, &generic );

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->bam_pending_results == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_generic_invalid_tag );
  FD_TEST( 0 == strcmp( result->result.not_committed.reason.generic_invalid.message,
                      FD_BAM_ERR_MSG_BUNDLE_EXECUTION_FAILED ) );

  /* Case 2: out-of-range transaction error falls back to generic_invalid with prefix */
  fd_bam_bundle_result_t invalid = test_make_bundle_result( 905UL );
  invalid.execution_success = 0;
  invalid.transaction_err[0] = (uchar)_bam_types_TransactionErrorReason_ARRAYSIZE;
  test_enqueue_bundle_result( state, &invalid );

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->bam_pending_results == 0UL );

  fd_memset( &decoded, 0, sizeof(decoded) );
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  result = &decoded.multi.results[0];
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_generic_invalid_tag );
  char const * generic_msg = result->result.not_committed.reason.generic_invalid.message;
  size_t const txn_prefix_len = strlen( FD_BAM_ERR_PREFIX_TRANSACTION_ERROR );
  FD_TEST( 0 == strncmp( generic_msg, FD_BAM_ERR_PREFIX_TRANSACTION_ERROR, txn_prefix_len ) );
  FD_TEST( strlen( generic_msg )>txn_prefix_len );

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
  res.execution_success = 0;
  res.scheduling_error  = (ushort)(_bam_types_SchedulingError_MAX + 1);
  test_enqueue_bundle_result( state, &res );

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->bam_pending_results == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_generic_invalid_tag );
  char const * sched_msg = result->result.not_committed.reason.generic_invalid.message;
  size_t const sched_prefix_len = strlen( FD_BAM_ERR_PREFIX_INVALID_SCHEDULING );
  FD_TEST( 0 == strncmp( sched_msg, FD_BAM_ERR_PREFIX_INVALID_SCHEDULING, sched_prefix_len ) );
  FD_TEST( strlen( sched_msg )>sched_prefix_len );

  test_bam_env_destroy( env );
}

static void
test_bam_request_ctx_labels( void ) {
  FD_TEST( 0 == strcmp( fd_bam_request_ctx_cstr( FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge ), "BamGetAuthChallenge" ) );
  FD_TEST( 0 == strcmp( fd_bam_request_ctx_cstr( FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig ), "BamGetBuilderConfig" ) );
  FD_TEST( 0 == strcmp( fd_bam_request_ctx_cstr( FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream ), "BamInitSchedulerStream" ) );
  FD_TEST( 0 == strcmp( fd_bam_request_ctx_cstr( 99UL ), "unknown" ) );
}

static void
setup_ctrl_defaults( fd_bam_tile_t * ctx,
                     fd_bam_ctrl_t * ctrl ) {
  ctx->ctrl = ctrl;
  fd_memset( ctrl, 0, sizeof(fd_bam_ctrl_t) );

  /* Mirror the on-startup state: control block idle, HTTPS endpoint configured, BAM enabled. */
  ctx->enabled = 1;
  char const * host = "initial.builder.test";
  ulong host_len = strlen( host );
  FD_TEST( host_len < sizeof( ctx->server_fqdn ) );
  strcpy( ctx->server_fqdn, host );
  ctx->server_fqdn_len = (ushort)host_len;
  strcpy( ctx->server_sni, host );
  ctx->server_sni_len = (ushort)host_len;
  ctx->server_tcp_port = 443;
  ctx->is_ssl          = 1;

  ctrl->enable         = 1U;
  strlcpy( ctrl->url, "https://initial.builder.test:443", FD_URL_MAX );
  strlcpy( ctrl->sni, host, FD_SNI_BUF_MAX );
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
  strlcpy( ctrl.url, "http://new.example.com:8899", FD_URL_MAX );
  strlcpy( ctrl.sni, "custom.sni.invalid", FD_SNI_BUF_MAX );
  ctrl.state = FD_BAM_CTRL_STATE_REQUEST;

  fd_bam_tile_housekeeping( ctx );

  FD_TEST( ctrl.state == FD_BAM_CTRL_STATE_SUCCESS );
  FD_TEST( ctrl.enable == 1U );
  FD_TEST( !strcmp( ctrl.url, "http://new.example.com:8899" ) );
  FD_TEST( !strcmp( ctrl.sni, "custom.sni.invalid" ) );
  FD_TEST( ctx->enabled == 1 );
  FD_TEST( ctx->server_tcp_port == 8899 );
  FD_TEST( !strcmp( ctx->server_fqdn, "new.example.com" ) );
  FD_TEST( ctx->server_fqdn_len == strlen( "new.example.com" ) );
  FD_TEST( ctx->is_ssl == 0 );
  FD_TEST( !strcmp( ctx->server_sni, "custom.sni.invalid" ) );
  FD_TEST( ctx->server_sni_len == strlen( "custom.sni.invalid" ) );
  FD_TEST( !strcmp( ctx->ctrl->error, "" ) );
  FD_TEST( !strcmp( ctx->grpc_client->host, "custom.sni.invalid" ) );
  FD_TEST( ctx->grpc_client->port == 8899 );

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

  FD_TEST( ctrl.state == FD_BAM_CTRL_STATE_SUCCESS );
  FD_TEST( ctrl.enable == 0U );
  FD_TEST( ctx->enabled == 0 );
  FD_TEST( fd_fseq_query( fseq ) == 0UL );
  FD_TEST( !strcmp( ctrl.url, "https://initial.builder.test:443" ) );

  FD_TEST( fd_fseq_leave( fseq ) == fseq_shmem );
  FD_TEST( fd_fseq_delete( fseq_shmem ) == fseq_shmem );
  ctx->bam_status_fseq = NULL;
  ctx->keyswitch = NULL;

  test_bam_env_destroy( env );
}

FD_FN_UNUSED static void
test_bam_ctrl_enable_from_disabled_start( fd_wksp_t * wksp ) {
  /* Ensure a tile launched with BAM disabled can be enabled via runtime control. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * ctx = env->state;

  fd_bam_ctrl_t ctrl;
  setup_ctrl_defaults( ctx, &ctrl );

  ctx->enabled = 0;
  ctrl.enable  = 0U;

  static fd_keyswitch_t keyswitch = {0};
  keyswitch.magic = FD_KEYSWITCH_MAGIC;
  keyswitch.state = FD_KEYSWITCH_STATE_COMPLETED;
  keyswitch.param = 0UL;
  ctx->keyswitch = &keyswitch;

  ctrl.command = FD_BAM_CTRL_CMD_ENABLE;
  ctrl.enable  = 1U;
  ctrl.state   = FD_BAM_CTRL_STATE_REQUEST;

  fd_bam_tile_housekeeping( ctx );

  FD_TEST( ctrl.state == FD_BAM_CTRL_STATE_SUCCESS );
  FD_TEST( ctrl.enable == 1U );
  FD_TEST( ctx->enabled == 1 );
  FD_TEST( !strcmp( ctrl.url, "https://initial.builder.test:443" ) );
  FD_TEST( !strcmp( ctx->ctrl->error, "" ) );

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
  strlcpy( ctrl.url, "not a url", FD_URL_MAX );
  ctrl.state = FD_BAM_CTRL_STATE_REQUEST;

  fd_bam_tile_housekeeping( ctx );

  FD_TEST( ctrl.state == FD_BAM_CTRL_STATE_ERROR );
  FD_TEST( strstr( ctrl.error, "Invalid BAM URL" ) != NULL );
  FD_TEST( !strcmp( ctrl.url, "https://initial.builder.test:443" ) );
  FD_TEST( !strcmp( ctx->server_fqdn, "initial.builder.test" ) );
  FD_TEST( ctx->enabled == 1 );

  ctx->keyswitch = NULL;
  test_bam_env_destroy( env );
}

static void
test_bam_gossip_publishes_bam_config_contact( fd_wksp_t * wksp ) {
  /* A healthy BAM tile should advertise the TPU endpoints supplied by BamConfig. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  fd_wksp_t * gossip_mem = fd_wksp_containing( env->out_dcache );
  state->gossip_out = (fd_bam_out_ctx_t){
      .idx    = 0UL,
      .mem    = gossip_mem,
      .chunk0 = fd_dcache_compact_chunk0( gossip_mem, env->out_dcache ),
      .chunk  = fd_dcache_compact_chunk0( gossip_mem, env->out_dcache ),
      .wmark  = fd_dcache_compact_wmark( gossip_mem, env->out_dcache, FD_TPU_PARSED_MTU )
  };
  state->bundle_status_recent = FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY;

  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  resp.has_bam_config = true;
  resp.bam_config.has_tpu_sock = true;
  strlcpy( resp.bam_config.tpu_sock.ip, "10.20.30.40", sizeof( resp.bam_config.tpu_sock.ip ) );
  resp.bam_config.tpu_sock.port = 1122U;
  resp.bam_config.has_tpu_fwd_sock = true;
  strlcpy( resp.bam_config.tpu_fwd_sock.ip, "11.12.13.14", sizeof( resp.bam_config.tpu_fwd_sock.ip ) );
  resp.bam_config.tpu_fwd_sock.port = 3344U;

  uchar pb_buf[ 256 ];
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );

  ulong publish_chunk = state->gossip_out.chunk;
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );

  fd_bam_contact_update_t update = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  FD_TEST( update.use_bam == FD_BAM_CONTACT_USE_BAM );

  fd_ip4_port_t expected_tpu = {0};
  FD_TEST( fd_cstr_to_ip4_addr( "10.20.30.40", &expected_tpu.addr ) );
  expected_tpu.port = fd_ushort_bswap( 1122 );
  FD_TEST( update.tpu_addr.l == expected_tpu.l );

  fd_ip4_port_t expected_tpu_fwd = {0};
  FD_TEST( fd_cstr_to_ip4_addr( "11.12.13.14", &expected_tpu_fwd.addr ) );
  expected_tpu_fwd.port = fd_ushort_bswap( 3344 );
  FD_TEST( update.tpu_fwd_addr.l == expected_tpu_fwd.l );

  test_bam_env_destroy( env );
}

static void
test_bam_gossip_resets_when_contact_missing( fd_wksp_t * wksp ) {
  /* If either BamConfig address is absent, the tile should force gossip back to defaults. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  fd_wksp_t * gossip_mem = fd_wksp_containing( env->out_dcache );
  state->gossip_out = (fd_bam_out_ctx_t){
      .idx    = 0UL,
      .mem    = gossip_mem,
      .chunk0 = fd_dcache_compact_chunk0( gossip_mem, env->out_dcache ),
      .chunk  = fd_dcache_compact_chunk0( gossip_mem, env->out_dcache ),
      .wmark  = fd_dcache_compact_wmark( gossip_mem, env->out_dcache, FD_TPU_PARSED_MTU )
  };
  state->bundle_status_recent = FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY;
  state->enabled = 1;

  fd_ip4_port_t bam_tpu = {0};
  FD_TEST( fd_cstr_to_ip4_addr( "12.34.56.78", &bam_tpu.addr ) );
  bam_tpu.port = fd_ushort_bswap( 2222 );
  fd_ip4_port_t bam_tpu_fwd = {0};
  FD_TEST( fd_cstr_to_ip4_addr( "98.76.54.32", &bam_tpu_fwd.addr ) );
  bam_tpu_fwd.port = fd_ushort_bswap( 3333 );
  state->bam_tpu_addr     = bam_tpu;
  state->bam_tpu_fwd_addr = bam_tpu_fwd;

  fd_bam_contact_update_t updates[3];
  ulong update_cnt = 0UL;

  ulong publish_chunk = state->gossip_out.chunk;
  fd_bam_gossip_update( state, state->stem );
  updates[ update_cnt++ ] = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  FD_TEST( updates[0].use_bam == FD_BAM_CONTACT_USE_BAM );
  FD_TEST( updates[0].tpu_addr.l == bam_tpu.l );
  FD_TEST( updates[0].tpu_fwd_addr.l == bam_tpu_fwd.l );

  /* Missing TPU endpoint should revert to Firedancer defaults. */
  state->bam_tpu_addr = (fd_ip4_port_t){0};
  publish_chunk = state->gossip_out.chunk;
  fd_bam_gossip_update( state, state->stem );
  updates[ update_cnt++ ] = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  FD_TEST( updates[1].use_bam == FD_BAM_CONTACT_USE_DEFAULT );
  FD_TEST( updates[1].tpu_addr.l == 0UL );
  FD_TEST( updates[1].tpu_fwd_addr.l == 0UL );

  /* Missing forward endpoint should also revert to defaults. */
  state->bam_tpu_addr     = bam_tpu;
  state->bam_tpu_fwd_addr = (fd_ip4_port_t){0};
  publish_chunk = state->gossip_out.chunk;
  fd_bam_gossip_update( state, state->stem );
  updates[ update_cnt++ ] = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  FD_TEST( updates[2].use_bam == FD_BAM_CONTACT_USE_DEFAULT );
  FD_TEST( updates[2].tpu_addr.l == 0UL );
  FD_TEST( updates[2].tpu_fwd_addr.l == 0UL );

  test_bam_env_destroy( env );
}

static void
test_bam_gossip_reconnect_without_contact( fd_wksp_t * wksp ) {
  /* Regression: if BAM withdraws its TPU override while the client is disconnected,
     reconnecting should not re-publish stale contact info. Also exercises the
     connected→disconnect→connected handshake to ensure we emit the expected
     use_bam transitions. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  test_bam_env_mock_conn( env );
  fd_wksp_t * gossip_mem = fd_wksp_containing( env->out_dcache );
  state->gossip_out = (fd_bam_out_ctx_t){
      .idx    = 0UL,
      .mem    = gossip_mem,
      .chunk0 = fd_dcache_compact_chunk0( gossip_mem, env->out_dcache ),
      .chunk  = fd_dcache_compact_chunk0( gossip_mem, env->out_dcache ),
      .wmark  = fd_dcache_compact_wmark( gossip_mem, env->out_dcache, FD_TPU_PARSED_MTU )
  };
  state->bundle_status_recent = FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY;

  fd_bam_contact_update_t updates[4];
  ulong                   update_cnt = 0UL;

  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  resp.has_bam_config = true;
  resp.bam_config.has_tpu_sock = true;
  strlcpy( resp.bam_config.tpu_sock.ip, "9.8.7.6", sizeof( resp.bam_config.tpu_sock.ip ) );
  resp.bam_config.tpu_sock.port = 5000U;
  resp.bam_config.has_tpu_fwd_sock = true;
  strlcpy( resp.bam_config.tpu_fwd_sock.ip, "4.3.2.1", sizeof( resp.bam_config.tpu_fwd_sock.ip ) );
  resp.bam_config.tpu_fwd_sock.port = 6000U;

  uchar pb_buf[ 256 ];
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
  ulong publish_chunk = state->gossip_out.chunk;
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );

  /* Disconnected path: publish default contact so gossip falls back to Firedancer TPU. */
  updates[ update_cnt++ ] = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  FD_TEST( update_cnt == 1UL );
  FD_TEST( updates[0].use_bam == FD_BAM_CONTACT_USE_BAM );

  fd_ip4_port_t expected_tpu = {0};
  FD_TEST( fd_cstr_to_ip4_addr( "9.8.7.6", &expected_tpu.addr ) );
  expected_tpu.port = fd_ushort_bswap( 5000 );
  FD_TEST( updates[0].tpu_addr.l == expected_tpu.l );

  fd_ip4_port_t expected_tpu_fwd = {0};
  FD_TEST( fd_cstr_to_ip4_addr( "4.3.2.1", &expected_tpu_fwd.addr ) );
  expected_tpu_fwd.port = fd_ushort_bswap( 6000 );
  FD_TEST( updates[0].tpu_fwd_addr.l == expected_tpu_fwd.l );

  publish_chunk = state->gossip_out.chunk;
  fd_bam_gossip_update( state, state->stem );
  updates[ update_cnt++ ] = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  FD_TEST( update_cnt == 2UL );
  FD_TEST( updates[1].use_bam == FD_BAM_CONTACT_USE_DEFAULT );

  state->bundle_status_recent = FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTING;

  resp = (bam_api_ConfigResponse)bam_api_ConfigResponse_init_default;
  resp.has_bam_config = true;
  resp.bam_config.has_tpu_sock = false;
  resp.bam_config.has_tpu_fwd_sock = false;
  ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
  publish_chunk = state->gossip_out.chunk;
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );

  FD_TEST( state->bam_tpu_addr.l == 0UL );
  FD_TEST( state->bam_tpu_fwd_addr.l == 0UL );
  updates[ update_cnt++ ] = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  FD_TEST( update_cnt == 3UL );
  FD_TEST( updates[2].use_bam == FD_BAM_CONTACT_USE_DEFAULT );

  /* If BAM contact is absent, should publish empty ip:port for tpu+tpu_fwd to revert */
  publish_chunk = state->gossip_out.chunk;
  fd_bam_gossip_update( state, state->stem );
  updates[ update_cnt++ ] = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  FD_TEST( update_cnt == 4UL );
  FD_TEST( updates[3].use_bam == FD_BAM_CONTACT_USE_DEFAULT );
  FD_TEST( updates[3].tpu_addr.l == 0UL );
  FD_TEST( updates[3].tpu_fwd_addr.l == 0UL );

  test_bam_env_destroy( env );
}

static void
test_bam_runtime_toggle_updates_gossip( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  fd_wksp_t * gossip_mem = fd_wksp_containing( env->out_dcache );
  state->gossip_out = (fd_bam_out_ctx_t){
      .idx    = 0UL,
      .mem    = gossip_mem,
      .chunk0 = fd_dcache_compact_chunk0( gossip_mem, env->out_dcache ),
      .chunk  = fd_dcache_compact_chunk0( gossip_mem, env->out_dcache ),
      .wmark  = fd_dcache_compact_wmark( gossip_mem, env->out_dcache, FD_TPU_PARSED_MTU )
  };
  state->bundle_status_recent = FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY;

  fd_bam_contact_update_t updates[3];
  ulong                   update_cnt = 0UL;

  /* Initial BamConfig should publish the override while runtime is enabled. */
  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  resp.has_bam_config = true;
  resp.bam_config.has_tpu_sock = true;
  strlcpy( resp.bam_config.tpu_sock.ip, "9.9.9.9", sizeof( resp.bam_config.tpu_sock.ip ) );
  resp.bam_config.tpu_sock.port = 7000U;
  resp.bam_config.has_tpu_fwd_sock = true;
  strlcpy( resp.bam_config.tpu_fwd_sock.ip, "8.8.8.8", sizeof( resp.bam_config.tpu_fwd_sock.ip ) );
  resp.bam_config.tpu_fwd_sock.port = 7001U;

  uchar pb_buf[ 256 ];
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
  ulong publish_chunk = state->gossip_out.chunk;
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );

  updates[ update_cnt++ ] = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  FD_TEST( update_cnt == 1UL );
  FD_TEST( updates[0].use_bam == FD_BAM_CONTACT_USE_BAM );

  fd_ip4_port_t expected_tpu = {0};
  FD_TEST( fd_cstr_to_ip4_addr( "9.9.9.9", &expected_tpu.addr ) );
  expected_tpu.port = fd_ushort_bswap( 7000 );
  FD_TEST( updates[0].tpu_addr.l == expected_tpu.l );

  fd_ip4_port_t expected_tpu_fwd = {0};
  FD_TEST( fd_cstr_to_ip4_addr( "8.8.8.8", &expected_tpu_fwd.addr ) );
  expected_tpu_fwd.port = fd_ushort_bswap( 7001 );
  FD_TEST( updates[0].tpu_fwd_addr.l == expected_tpu_fwd.l );

  /* Disabling runtime should immediately revert gossip to the Firedancer defaults. */
  publish_chunk = state->gossip_out.chunk;
  state->enabled = 0;
  fd_bam_gossip_update( state, state->stem);
  updates[ update_cnt++ ] = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  FD_TEST( update_cnt == 2UL );
  FD_TEST( updates[1].use_bam == FD_BAM_CONTACT_USE_DEFAULT );

  /* Re-enabling runtime while still connected should republish the BamConfig address. */
  publish_chunk = state->gossip_out.chunk;
  state->enabled = 1;
  fd_bam_gossip_update( state, state->stem);
  updates[ update_cnt++ ] = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  FD_TEST( update_cnt == 3UL );
  FD_TEST( updates[2].use_bam == FD_BAM_CONTACT_USE_BAM );
  FD_TEST( updates[2].tpu_addr.l == expected_tpu.l );
  FD_TEST( updates[2].tpu_fwd_addr.l == expected_tpu_fwd.l );

  test_bam_env_destroy( env );
}

static void
test_bam_gossip_update_requires_full_contact( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  fd_wksp_t * gossip_mem = fd_wksp_containing( env->out_dcache );
  state->gossip_out = (fd_bam_out_ctx_t){
      .idx    = 0UL,
      .mem    = gossip_mem,
      .chunk0 = fd_dcache_compact_chunk0( gossip_mem, env->out_dcache ),
      .chunk  = fd_dcache_compact_chunk0( gossip_mem, env->out_dcache ),
      .wmark  = fd_dcache_compact_wmark( gossip_mem, env->out_dcache, FD_TPU_PARSED_MTU )
  };

  fd_ip4_port_t bam_tpu = {0};
  FD_TEST( fd_cstr_to_ip4_addr( "7.7.7.7", &bam_tpu.addr ) );
  bam_tpu.port = fd_ushort_bswap( 8899 );

  /* Missing forward address should not advertise BAM contact info. */
  state->bam_tpu_addr     = bam_tpu;
  state->bam_tpu_fwd_addr = (fd_ip4_port_t){0};
  ulong publish_chunk = state->gossip_out.chunk;
  fd_bam_gossip_update( state, state->stem );
  fd_bam_contact_update_t update = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  FD_TEST( update.use_bam == FD_BAM_CONTACT_USE_DEFAULT );

  /* Missing both sockets should also fall back to defaults. */
  state->bam_tpu_addr     = (fd_ip4_port_t){0};
  state->bam_tpu_fwd_addr = (fd_ip4_port_t){0};
  publish_chunk = state->gossip_out.chunk;
  fd_bam_gossip_update( state, state->stem );
  update = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  FD_TEST( update.use_bam == FD_BAM_CONTACT_USE_DEFAULT );

  /* Full contact info should publish BAM overrides. */
  fd_ip4_port_t bam_tpu_fwd = {0};
  FD_TEST( fd_cstr_to_ip4_addr( "6.6.6.6", &bam_tpu_fwd.addr ) );
  bam_tpu_fwd.port = fd_ushort_bswap( 9999 );
  state->bam_tpu_addr     = bam_tpu;
  state->bam_tpu_fwd_addr = bam_tpu_fwd;
  publish_chunk = state->gossip_out.chunk;
  fd_bam_gossip_update( state, state->stem );
  update = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  FD_TEST( update.use_bam == FD_BAM_CONTACT_USE_BAM );
  FD_TEST( update.tpu_addr.l == bam_tpu.l );
  FD_TEST( update.tpu_fwd_addr.l == bam_tpu_fwd.l );

  test_bam_env_destroy( env );
}

static void
test_bam_config_updates_contact_info( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  FD_TEST( state->bam_tpu_addr.l == 0UL );
  FD_TEST( state->bam_tpu_fwd_addr.l == 0UL );

  /* Initial config populates TPU endpoints and fee recipient. */
  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  resp.has_bam_config = true;
  resp.bam_config.has_tpu_sock = true;
  strlcpy( resp.bam_config.tpu_sock.ip, "1.2.3.4", sizeof( resp.bam_config.tpu_sock.ip ) );
  resp.bam_config.tpu_sock.port = 9000U;
  resp.bam_config.has_tpu_fwd_sock = true;
  strlcpy( resp.bam_config.tpu_fwd_sock.ip, "5.6.7.8", sizeof( resp.bam_config.tpu_fwd_sock.ip ) );
  resp.bam_config.tpu_fwd_sock.port = 10001U;
  uchar prio_fee_raw[ 32 ];
  for( ulong i=0UL; i<32UL; i++ ) prio_fee_raw[ i ] = (uchar)( i + 7U );
  FD_TEST( fd_base58_encode_32( prio_fee_raw, NULL, resp.bam_config.prio_fee_recipient_pubkey ) );
  resp.bam_config.prio_fee_recipient_pubkey[ FD_BASE58_ENCODED_32_SZ-1 ] = '\0';
  resp.bam_config.commission_bps = 2750U;

  uchar pb_buf[ 256 ];
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );

  FD_TEST( state->bam_tpu_addr.l != 0UL );

  fd_ip4_port_t expected_tpu = {0};
  FD_TEST( fd_cstr_to_ip4_addr( "1.2.3.4", &expected_tpu.addr ) );
  expected_tpu.port = fd_ushort_bswap( 9000 );
  FD_TEST( state->bam_tpu_addr.l == expected_tpu.l );

  fd_ip4_port_t expected_tpu_fwd = {0};
  FD_TEST( fd_cstr_to_ip4_addr( "5.6.7.8", &expected_tpu_fwd.addr ) );
  expected_tpu_fwd.port = fd_ushort_bswap( 10001 );
  FD_TEST( state->bam_tpu_fwd_addr.l == expected_tpu_fwd.l );
  FD_TEST( state->prio_fee_recipient_set == 1U );
  FD_TEST( state->commission_bps == 2750 );
  FD_TEST( 0 == memcmp( state->prio_fee_recipient, prio_fee_raw, sizeof( prio_fee_raw ) ) );
  FD_TEST( state->fee_cfg != NULL );
  FD_TEST( state->fee_cfg->has_prio_fee_recipient == 1U );
  FD_TEST( state->fee_cfg->commission_bps == 2750 );
  FD_TEST( 0 == memcmp( state->fee_cfg->prio_fee_recipient, prio_fee_raw, sizeof( prio_fee_raw ) ) );
  FD_TEST( state->fee_cfg->version == 1UL );

  ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );
  FD_TEST( state->bam_tpu_addr.l == expected_tpu.l );
  FD_TEST( state->bam_tpu_fwd_addr.l == expected_tpu_fwd.l );
  FD_TEST( state->fee_cfg->version == 1UL );

  /* Dropping only the forward socket should keep TPU and fee config intact. */
  resp.bam_config.has_tpu_fwd_sock = false;
  fd_memset( resp.bam_config.tpu_fwd_sock.ip, 0, sizeof( resp.bam_config.tpu_fwd_sock.ip ) );
  resp.bam_config.tpu_fwd_sock.port = 0U;
  ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );
  FD_TEST( state->bam_tpu_addr.l != 0UL );
  FD_TEST( state->bam_tpu_addr.l == expected_tpu.l );
  FD_TEST( state->bam_tpu_fwd_addr.l == 0UL );
  FD_TEST( state->fee_cfg->version == 1UL );

  /* Clearing both sockets resets contact info but leaves fee config versioned. */
  resp.bam_config.has_tpu_sock = false;
  fd_memset( resp.bam_config.tpu_sock.ip, 0, sizeof( resp.bam_config.tpu_sock.ip ) );
  resp.bam_config.tpu_sock.port = 0U;
  ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );
  FD_TEST( state->bam_tpu_addr.l == 0UL );
  FD_TEST( state->bam_tpu_fwd_addr.l == 0UL );
  FD_TEST( state->fee_cfg->version == 1UL );

  test_bam_env_destroy( env );
}

static void
test_bam_fee_cfg_propagates_to_pack( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * bam_state = env->state;

  FD_TEST( bam_state->fee_cfg != NULL );
  fd_bam_fee_cfg_t * shared_cfg = bam_state->fee_cfg;
  fd_memset( shared_cfg, 0, sizeof(fd_bam_fee_cfg_t) );

  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  resp.has_bam_config = true;
  resp.bam_config.has_tpu_sock = true;
  strlcpy( resp.bam_config.tpu_sock.ip, "1.1.1.1", sizeof( resp.bam_config.tpu_sock.ip ) );
  resp.bam_config.tpu_sock.port = 8000U;
  resp.bam_config.has_tpu_fwd_sock = true;
  strlcpy( resp.bam_config.tpu_fwd_sock.ip, "2.2.2.2", sizeof( resp.bam_config.tpu_fwd_sock.ip ) );
  resp.bam_config.tpu_fwd_sock.port = 9000U;
  uchar prio_fee_raw[ 32 ];
  for( ulong i=0UL; i<32UL; i++ ) prio_fee_raw[ i ] = (uchar)( i + 11U );
  FD_TEST( fd_base58_encode_32( prio_fee_raw, NULL, resp.bam_config.prio_fee_recipient_pubkey ) );
  resp.bam_config.prio_fee_recipient_pubkey[ FD_BASE58_ENCODED_32_SZ-1 ] = '\0';
  resp.bam_config.commission_bps = 3500U;

  uchar pb_buf[ 256 ];
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
  fd_bam_client_grpc_rx_msg( bam_state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );

  FD_TEST( shared_cfg->version == 1UL );
  FD_TEST( shared_cfg->has_prio_fee_recipient == 1U );
  FD_TEST( shared_cfg->commission_bps == 3500U );
  FD_TEST( 0 == memcmp( shared_cfg->prio_fee_recipient, prio_fee_raw, sizeof( prio_fee_raw ) ) );

  fd_bundle_crank_gen_t crank_gen_mem[1];
  fd_memset( crank_gen_mem, 0, sizeof( crank_gen_mem ) );
  fd_bundle_crank_gen_t * crank_gen = crank_gen_mem;
  ulong pack_cfg_version = 0UL;
  int crank_enabled = 1;

  fd_memset( crank_gen->crank3->new_tip_receiver, 0xAA, sizeof( crank_gen->crank3->new_tip_receiver ) );
  fd_memset( crank_gen->crank2->new_tip_receiver, 0xBB, sizeof( crank_gen->crank2->new_tip_receiver ) );
  crank_gen->crank3->init_tip_distribution_acct.commission_bps = (ushort)777U;

  /* New BAM fee config should propagate into both cranks and bump version. */
  fd_pack_apply_bam_fee_cfg_impl( shared_cfg,
                                  &pack_cfg_version,
                                  crank_enabled,
                                  crank_gen->crank3,
                                  crank_gen->crank2 );

  FD_TEST( pack_cfg_version == shared_cfg->version );
  FD_TEST( crank_gen->crank3->init_tip_distribution_acct.commission_bps == 3500U );
  FD_TEST( 0 == memcmp( crank_gen->crank3->new_tip_receiver, prio_fee_raw, sizeof( prio_fee_raw ) ) );
  FD_TEST( 0 == memcmp( crank_gen->crank2->new_tip_receiver, prio_fee_raw, sizeof( prio_fee_raw ) ) );

  uchar sentinel3[32];
  uchar sentinel2[32];
  fd_memset( sentinel3, 0xCC, sizeof( sentinel3 ) );
  fd_memset( sentinel2, 0xDD, sizeof( sentinel2 ) );
  fd_memcpy( crank_gen->crank3->new_tip_receiver, sentinel3, sizeof( sentinel3 ) );
  fd_memcpy( crank_gen->crank2->new_tip_receiver, sentinel2, sizeof( sentinel2 ) );
  crank_gen->crank3->init_tip_distribution_acct.commission_bps = (ushort)1234U;

  /* Without a version bump pack tile should ignore the shared config. */
  shared_cfg->commission_bps = 9000U;
  fd_pack_apply_bam_fee_cfg_impl( shared_cfg,
                                  &pack_cfg_version,
                                  crank_enabled,
                                  crank_gen->crank3,
                                  crank_gen->crank2 );
  FD_TEST( pack_cfg_version == 1UL );
  FD_TEST( crank_gen->crank3->init_tip_distribution_acct.commission_bps == 1234U );
  FD_TEST( 0 == memcmp( crank_gen->crank3->new_tip_receiver, sentinel3, sizeof( sentinel3 ) ) );
  FD_TEST( 0 == memcmp( crank_gen->crank2->new_tip_receiver, sentinel2, sizeof( sentinel2 ) ) );

  /* Second config update bumps version and replaces fee settings everywhere. */
  bam_api_ConfigResponse resp_update = bam_api_ConfigResponse_init_default;
  resp_update.has_bam_config = true;
  resp_update.bam_config.commission_bps = 15000U;
  uchar prio_fee_raw2[ 32 ];
  for( ulong i=0UL; i<32UL; i++ ) prio_fee_raw2[ i ] = (uchar)( i + 39U );
  FD_TEST( fd_base58_encode_32( prio_fee_raw2, NULL, resp_update.bam_config.prio_fee_recipient_pubkey ) );
  resp_update.bam_config.prio_fee_recipient_pubkey[ FD_BASE58_ENCODED_32_SZ-1 ] = '\0';
  ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp_update ) );
  fd_bam_client_grpc_rx_msg( bam_state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );

  FD_TEST( shared_cfg->version == 2UL );
  FD_TEST( shared_cfg->commission_bps == 10000U );
  FD_TEST( shared_cfg->has_prio_fee_recipient == 1U );
  FD_TEST( 0 == memcmp( shared_cfg->prio_fee_recipient, prio_fee_raw2, sizeof( prio_fee_raw2 ) ) );

  fd_pack_apply_bam_fee_cfg_impl( shared_cfg,
                                  &pack_cfg_version,
                                  crank_enabled,
                                  crank_gen->crank3,
                                  crank_gen->crank2 );

  FD_TEST( pack_cfg_version == shared_cfg->version );
  FD_TEST( crank_gen->crank3->init_tip_distribution_acct.commission_bps == 10000U );
  FD_TEST( 0 == memcmp( crank_gen->crank3->new_tip_receiver, prio_fee_raw2, sizeof( prio_fee_raw2 ) ) );
  FD_TEST( 0 == memcmp( crank_gen->crank2->new_tip_receiver, prio_fee_raw2, sizeof( prio_fee_raw2 ) ) );

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
  uchar pubkey[32] = {1,2,3,4,5};
  FD_TEST( fd_base58_encode_32( pubkey, NULL, resp.block_engine_config.builder_pubkey ) );
  resp.block_engine_config.builder_pubkey[ FD_BASE58_ENCODED_32_SZ-1 ] = '\0';
  resp.block_engine_config.builder_commission = 5U;
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  if( FD_UNLIKELY( !pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) ) ) {
    FD_LOG_ERR(( "pb_encode fee info failed: %s", PB_GET_ERROR( &ostream ) ));
  }

  FD_TEST( state->builder_info_valid_until == 0L );
  fd_bam_client_grpc_rx_msg( state, pb_buf, ostream.bytes_written, FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );
  FD_TEST( state->builder_info_valid_until != 0L );
  FD_TEST( state->builder_commission == 5U );
  uchar decoded[32];
  FD_TEST( fd_base58_decode_32( resp.block_engine_config.builder_pubkey, decoded ) );
  FD_TEST( 0 == memcmp( state->builder_pubkey, decoded, 32UL ) );

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

  FD_TEST( state->bam_pending_results == 3UL );
  FD_TEST( state->bam_results_head == 0UL );
  FD_TEST( state->bam_results_tail == expected_tail );
  FD_TEST( state->bam_results[0].seq_id == 100UL );
  FD_TEST( state->bam_results[1].seq_id == 101UL );
  FD_TEST( state->bam_results[2].seq_id == 102UL );

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
  FD_TEST( state->bam_pending_results == 2UL );

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
  FD_TEST( flushed == 1 );
  FD_TEST( state->bam_pending_results == 0UL );
  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  FD_TEST( decoded.multi.results[0].seq_id == 201U );
  FD_TEST( state->bam_results_head == state->bam_results_tail );

  test_bam_env_destroy( env );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );
  fd_metrics_register( (ulong *)fd_metrics_new( metrics_scratch, 0UL, 0UL ) );

  ulong cpu_idx = fd_tile_cpu_id( fd_tile_idx() );
  if( cpu_idx > fd_shmem_cpu_cnt() ) cpu_idx = 0UL;

  char const * _page_sz = fd_env_strip_cmdline_cstr ( &argc, &argv, "--page-sz",     NULL, "normal"                     );
  ulong        page_cnt = fd_env_strip_cmdline_ulong( &argc, &argv, "--page-cnt",    NULL, 256UL                        );
  ulong        numa_idx = fd_env_strip_cmdline_ulong( &argc, &argv, "--numa-idx",    NULL, fd_shmem_numa_idx( cpu_idx ) );

  fd_wksp_t * wksp = fd_wksp_new_anonymous( fd_cstr_to_shmem_page_sz( _page_sz ), page_cnt, fd_shmem_cpu_idx( numa_idx ), "bam-test", 16UL );
  FD_TEST( wksp );

  test_bam_packets_forwarded( wksp );
  test_bam_bundle_forwarded( wksp );
  test_bam_multiple_batches_forwarded( wksp );
  test_bam_scheduler_truncated_message_dropped( wksp );
  test_bam_bundle_requires_builder_info( wksp );
  test_bam_bundle_rejects_mixed_revert_flags( wksp );
  test_bam_bundle_rejects_vote_transactions( wksp );
  test_bam_bundle_rejects_excess_packet_count( wksp );
  test_bam_bundle_rejects_oversized_packet( wksp );
  test_bam_bundle_rejects_empty_batch( wksp );
  test_bam_bundle_rejects_missing_batches( wksp );
  test_bam_grpc_end_handling( wksp );
  test_bam_grpc_timeout( wksp );
  test_bam_scheduler_auth_proof_publishes_message( wksp );
  test_bam_scheduler_heartbeat_publishes_message( wksp );
  test_bam_scheduler_leader_state_publishes_message( wksp );
  test_bam_scheduler_result_publishes_message( wksp );
  test_bam_scheduler_result_not_committed_publishes_message( wksp );
  test_bam_scheduler_result_not_committed_sanitize_error_reason( wksp );
  test_bam_scheduler_result_not_committed_transaction_error_reason( wksp );
  test_bam_scheduler_result_not_committed_transaction_error_high_index( wksp );
  test_bam_scheduler_result_not_committed_generic_failure_reason( wksp );
  test_bam_scheduler_result_not_committed_invalid_scheduling_error_reason( wksp );
  test_bam_heartbeat_timeout_forces_disconnect( wksp );
  test_bam_heartbeat_reset_extends_timeout( wksp );
  test_bam_client_status( wksp );
  test_bam_auth_challenge_response_sets_signature( wksp );
  test_bam_request_ctx_labels();
  test_bam_ctrl_updates_url_and_sni( wksp );
  test_bam_ctrl_toggle_enable_updates_runtime_state( wksp );
  test_bam_ctrl_enable_from_disabled_start( wksp );
  test_bam_ctrl_invalid_url_sets_error_and_preserves_config( wksp );
  test_bam_gossip_publishes_bam_config_contact( wksp );
  test_bam_gossip_resets_when_contact_missing( wksp );
  test_bam_gossip_reconnect_without_contact( wksp );
  test_bam_runtime_toggle_updates_gossip( wksp );
  test_bam_gossip_update_requires_full_contact( wksp );
  test_bam_config_updates_contact_info( wksp );
  test_bam_fee_cfg_propagates_to_pack( wksp );
  test_bam_builder_fee_info( wksp );
  test_bam_bundle_result_queue_survives_reset( wksp );
  test_bam_bundle_result_queue_flushes_after_reconnect( wksp );

  fd_wksp_usage_t wksp_usage;
  FD_TEST( fd_wksp_usage( wksp, NULL, 0UL, &wksp_usage ) );
  if( wksp_usage.free_cnt!=wksp_usage.total_cnt ) {
    FD_LOG_WARNING(( "wksp leak: free_cnt=%lu total_cnt=%lu", wksp_usage.free_cnt, wksp_usage.total_cnt ));
  }
  FD_TEST( wksp_usage.free_cnt == wksp_usage.total_cnt );

  fd_wksp_delete_anonymous( wksp );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
