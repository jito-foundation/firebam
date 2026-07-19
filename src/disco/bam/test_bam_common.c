#include "fd_bam_tile_private.h"
#include "../metrics/fd_metrics.h"
#include "../../discoh/plugin/fd_plugin.h"
#include "../fd_txn_m.h"
#include "../../waltz/grpc/fd_grpc_client_private.h"
#include "../../third_party/nanopb/pb_decode.h"
#include "../../third_party/nanopb/pb_encode.h"
#include <sys/socket.h>
#include <unistd.h>

extern void
fd_bam_test_after_credit( fd_bam_tile_t *    ctx,
                          fd_stem_context_t * stem,
                          int *               opt_poll_in,
                          int *               charge_busy );

FD_IMPORT_BINARY( test_bam_sample_vote, "src/disco/pack/sample_vote.bin" );

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

FD_FN_UNUSED static size_t
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
  FD_TEST( pb_encode( &ostream, bam_api_SchedulerResponse_fields, &resp ) );
  return ostream.bytes_written;
}

FD_FN_UNUSED static size_t
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
  batch.max_schedule_slot = 1UL;
  batch.packets.funcs.encode = test_bam_encode_packets_cb;
  batch.packets.arg          = &packets_ctx;

  return test_bam_encode_scheduler_multi_batch_response( &batch, 1UL, out, out_sz );
}

typedef struct {
  bam_types_TransactionCommittedResult txns[ FD_PACK_MAX_TXN_PER_BUNDLE ];
  ulong                                txn_cnt;
} test_bam_committed_results_t;

#define TEST_BAM_MAX_DECODED_RESULTS 8UL
#define TEST_BAM_PROTOBUF_BUF_SZ     (64UL*1024UL)

typedef struct {
  bam_types_AtomicTxnBatchResult results[ TEST_BAM_MAX_DECODED_RESULTS ];
  ulong                          result_cnt;
  test_bam_committed_results_t   committed[ TEST_BAM_MAX_DECODED_RESULTS ];
} test_bam_multi_results_t;

typedef struct {
  bam_api_SchedulerMessage msg;
  test_bam_multi_results_t multi;
} test_bam_decoded_message_t;

FD_FN_UNUSED static void
test_bam_keepalive_sync( fd_bam_tile_t * state,
                         long            now ) {
  state->keepalive->ts_next_tx = now + state->keepalive_interval;
  state->keepalive->ts_deadline = 0L;
  state->keepalive->ts_last_tx  = now;
  state->keepalive->ts_last_rx  = now;
  state->keepalive->inflight    = 0U;
}

FD_FN_UNUSED static void
test_bam_prepare_scheduler_stream( fd_bam_tile_t * state ) {
  fd_grpc_h2_stream_t * stream = fd_grpc_client_stream_acquire( state->grpc_client, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( stream );
  state->bam_stream            = stream;
  state->bam_stream_live       = 1U;
  state->bam_stream_connecting = 0U;
  state->grpc_client->request_stream = NULL;
  *state->grpc_client->request_tx_op = (fd_h2_tx_op_t){0};
  state->grpc_client->frame_tx->lo = state->grpc_client->frame_tx->hi;
  state->grpc_client->frame_tx->lo_off = state->grpc_client->frame_tx->hi_off;
  state->bam_auth_ready        = 1U;
  state->bam_auth_inflight     = 0U;
}

FD_FN_UNUSED static fd_bam_bundle_result_t
test_make_bundle_result( uint  seq_id,
                         ulong slot,
                         uchar bundle_txn_cnt ) {
  fd_bam_bundle_result_t res = {0};
  res.seq_id             = seq_id;
  res.slot               = slot;
  res.bundle_txn_cnt     = bundle_txn_cnt;
  res.execution_success  = 1;
  res.scheduling_error   = FD_BAM_SCHED_ERR_NONE;
  res.transaction_err_count = 0U;
  for( uint i=0U; i<bundle_txn_cnt; i++ ) {
    res.consumed_cus[ i ]              = i + 1U;
    res.feepayer_balance_lamports[ i ] = 1000UL + i;
    res.loaded_accounts_data_size[ i ] = 2000U + i;
    res.sanitize_success[ i ]          = 1U;
  }
  return res;
}

FD_FN_UNUSED static void
test_enqueue_bundle_result( fd_bam_tile_t *               state,
                            fd_bam_bundle_result_t const * res ) {
  FD_TEST( state->feedback_queue_depth < FD_BAM_MAX_PENDING_RESULTS );
  fd_bam_bundle_result_t copy = *res;
  copy.scheduler_gen = state->scheduler_gen;
  state->bam_results[ state->bam_results_tail ] = copy;
  state->bam_results_tail = (ushort)(( state->bam_results_tail + 1U ) % FD_BAM_MAX_PENDING_RESULTS);
  state->feedback_queue_depth = (ushort)( state->feedback_queue_depth + 1U );
}

FD_FN_UNUSED static void
test_bam_decode_committed_results( pb_istream_t *              stream,
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

FD_FN_UNUSED static void
test_bam_decode_atomic_result( pb_istream_t *                 stream,
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

FD_FN_UNUSED static bool
test_bam_decode_atomic_result_cb( pb_istream_t *      stream,
                                  pb_field_t const *  field,
                                  void **             arg ) {
  (void)field;
  test_bam_multi_results_t * multi = (test_bam_multi_results_t *)(*arg);
  FD_TEST( multi->result_cnt < TEST_BAM_MAX_DECODED_RESULTS );

  bam_types_AtomicTxnBatchResult * res = &multi->results[ multi->result_cnt ];
  *res = (bam_types_AtomicTxnBatchResult)bam_types_AtomicTxnBatchResult_init_default;
  test_bam_committed_results_t * committed = &multi->committed[ multi->result_cnt ];
  fd_memset( committed, 0, sizeof(test_bam_committed_results_t) );

  test_bam_decode_atomic_result( stream, res, committed );

  multi->result_cnt++;
  return true;
}

FD_FN_UNUSED static void
test_bam_decode_multi_results( pb_istream_t *             stream,
                               test_bam_decoded_message_t * out ) {
  bam_types_MultipleAtomicTxnBatchResult msg = bam_types_MultipleAtomicTxnBatchResult_init_default;
  msg.results.funcs.decode = test_bam_decode_atomic_result_cb;
  msg.results.arg          = &out->multi;
  FD_TEST( pb_decode( stream, bam_types_MultipleAtomicTxnBatchResult_fields, &msg ) );
}

FD_FN_UNUSED static void
test_bam_decode_scheduler_message_v0( pb_istream_t *             stream,
                                      test_bam_decoded_message_t * out ) {
  uint32_t tag;
  pb_wire_type_t wire_type;
  bool eof = false;
  while( pb_decode_tag( stream, &wire_type, &tag, &eof ) ) {
    switch( tag ) {
    case bam_api_SchedulerMessageV0_heart_beat_tag: {
      FD_TEST( wire_type == PB_WT_STRING );
      pb_istream_t hb_stream;
      FD_TEST( pb_make_string_substream( stream, &hb_stream ) );
      out->msg.versioned_msg.v0.which_msg = bam_api_SchedulerMessageV0_heart_beat_tag;
      FD_TEST( pb_decode( &hb_stream, bam_types_ValidatorHeartBeat_fields,
                          &out->msg.versioned_msg.v0.msg.heart_beat ) );
      pb_close_string_substream( stream, &hb_stream );
      break;
    }
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
    case bam_api_SchedulerMessageV0_pong_tag: {
      FD_TEST( wire_type == PB_WT_STRING );
      pb_istream_t substream;
      FD_TEST( pb_make_string_substream( stream, &substream ) );
      out->msg.versioned_msg.v0.which_msg = bam_api_SchedulerMessageV0_pong_tag;
      out->msg.versioned_msg.v0.msg.pong = (bam_types_Pong)bam_types_Pong_init_default;
      FD_TEST( pb_decode( &substream, bam_types_Pong_fields,
                          &out->msg.versioned_msg.v0.msg.pong ) );
      pb_close_string_substream( stream, &substream );
      break;
    }
    case bam_api_SchedulerMessageV0_leader_state_tag: {
      FD_TEST( wire_type == PB_WT_STRING );
      pb_istream_t ls_stream;
      FD_TEST( pb_make_string_substream( stream, &ls_stream ) );
      out->msg.versioned_msg.v0.which_msg = bam_api_SchedulerMessageV0_leader_state_tag;
      FD_TEST( pb_decode( &ls_stream, bam_types_LeaderState_fields,
                          &out->msg.versioned_msg.v0.msg.leader_state ) );
      pb_close_string_substream( stream, &ls_stream );
      break;
    }
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

FD_FN_UNUSED static void
test_bam_decode_last_message( fd_bam_tile_t *             state,
                              test_bam_decoded_message_t * out ) {
  fd_grpc_client_t * client = state->grpc_client;
  fd_grpc_hdr_t hdr;
  fd_memcpy( &hdr, client->nanopb_tx, sizeof(fd_grpc_hdr_t) );
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
  client->frame_tx->lo = client->frame_tx->hi;
  client->frame_tx->lo_off = client->frame_tx->hi_off;
}

FD_FN_UNUSED static void
test_bam_init_simple_vote_packet( bam_types_Packet * packet,
                                  _Bool              revert_on_error ) {
  fd_memset( packet, 0, sizeof(*packet) );
  packet->data.size = (pb_size_t)test_bam_sample_vote_sz;
  fd_memcpy( packet->data.bytes, test_bam_sample_vote, test_bam_sample_vote_sz );
  packet->has_meta = 1U;
  packet->meta.size = (pb_size_t)test_bam_sample_vote_sz;
  packet->meta.has_flags = 1U;
  packet->meta.flags.revert_on_error = revert_on_error;
  packet->meta.flags.simple_vote_tx  = 1U;
}

FD_FN_UNUSED static void
test_bam_env_inject_config_response( fd_bam_tile_t * state ) {
  uchar pubkey[32];
  for( ulong i=0UL; i<sizeof(pubkey); i++ ) pubkey[ i ] = (uchar)( i + 1U );

  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  resp.has_block_engine_config = true;
  resp.block_engine_config.builder_commission = 7U;
  FD_TEST( fd_base58_encode_32( pubkey, NULL, resp.block_engine_config.builder_pubkey ) );
  resp.block_engine_config.builder_pubkey[ FD_BASE58_ENCODED_32_SZ-1 ] = '\0';

  resp.has_bam_config = true;
  resp.bam_config.has_tpu_sock = true;
  fd_cstr_ncpy( resp.bam_config.tpu_sock.ip, "127.0.0.1", sizeof( resp.bam_config.tpu_sock.ip ) );
  resp.bam_config.tpu_sock.port = 9007U;
  resp.bam_config.has_tpu_fwd_sock = true;
  fd_cstr_ncpy( resp.bam_config.tpu_fwd_sock.ip, "127.0.0.2", sizeof( resp.bam_config.tpu_fwd_sock.ip ) );
  resp.bam_config.tpu_fwd_sock.port = 9008U;

  uchar pb_buf[ 256 ];
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
  state->bam_config_inflight = 1U;
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );
}

struct test_bam_env {
  fd_stem_context_t stem[1];
  ulong             stem_seqs[1];
  ulong             stem_depths[1];
  ulong             stem_cr_avail[1];
  ulong             stem_min_cr_avail[1];
  int              stem_out_reliable[1];
  fd_frag_meta_t *  out_mcache;
  uchar *           out_dcache;
  void *            pending_txn_mem;
  int               server_sock;

  fd_bam_tile_t state[1];
};

typedef struct test_bam_env test_bam_env_t;

static test_bam_env_t *
test_bam_env_create( test_bam_env_t * env,
                     fd_wksp_t *      wksp ) {
  fd_memset( env, 0, sizeof(test_bam_env_t) );

  ulong const mcache_depth = fd_ulong_pow2_up( fd_ulong_max( FD_BAM_STEM_BURST + 1UL,
                                                             FD_BAM_MAX_TXN_PER_MESSAGE + 1UL ) );
  fd_frag_meta_t * mcache = fd_mcache_join( fd_mcache_new(
      fd_wksp_alloc_laddr( wksp, fd_mcache_align(), fd_mcache_footprint( mcache_depth, 0UL ), 1UL ),
      mcache_depth, 0UL, 0UL ) );
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
  env->stem_out_reliable[0] = 1;
  *env->stem = (fd_stem_context_t) {
    .mcaches              = &env->out_mcache,
    .seqs                 = env->stem_seqs,
    .depths               = env->stem_depths,
    .cr_avail             = env->stem_cr_avail,
    .min_cr_avail         = env->stem_min_cr_avail,
    .cr_decrement_amount  = 0UL,
    .out_reliable         = env->stem_out_reliable
  };
  env->server_sock = -1;

  fd_bam_tile_t * state = env->state;
  fd_memset( state, 0, sizeof(fd_bam_tile_t) );
  for( ulong i=0UL; i<sizeof(state->bam_identity_pubkey); i++ ) state->bam_identity_pubkey[ i ] = (uchar)( i + 1U );
  fd_base58_encode_32( state->bam_identity_pubkey, NULL, state->bam_identity_pubkey_b58 );
  state->stem = env->stem;
  state->enabled = 1;
  ulong const pending_max = mcache_depth;
  env->pending_txn_mem = fd_wksp_alloc_laddr( wksp, bam_pending_txn_align(), bam_pending_txn_footprint( pending_max ), 1UL );
  FD_TEST( env->pending_txn_mem );
  state->pending_txns = bam_pending_txn_join( bam_pending_txn_new( env->pending_txn_mem, pending_max ) );
  FD_TEST( state->pending_txns );
  state->decoded_multi = fd_wksp_alloc_laddr( wksp,
                                              alignof(fd_bam_decoded_multi_batch_t),
                                              sizeof(fd_bam_decoded_multi_batch_t),
                                              1UL );
  FD_TEST( state->decoded_multi );
  state->verify_out = (fd_bam_out_ctx_t) {
    .idx    = 0UL,
    .mem    = dcache,
    .chunk0 = 0UL,
    .chunk  = 0UL,
    .wmark  = fd_dcache_compact_wmark( dcache, dcache, FD_TPU_PARSED_MTU )
  };
  state->plugin_out = (fd_bam_out_ctx_t){ .idx    = ULONG_MAX };
  state->gossip_out = (fd_bam_out_ctx_t){ .idx    = ULONG_MAX };
  state->shred_out  = (fd_bam_out_ctx_t){ .idx    = ULONG_MAX };
  state->bam_leader_state.slot = ULONG_MAX;
  state->pack_bam_leader_in_idx = ULONG_MAX;
  state->pack_bam_result_in_idx = ULONG_MAX;
  state->replay_out_in_idx = ULONG_MAX;
  state->next_leader_slot = ULONG_MAX;
  state->leader_schedule_recheck_slot = FD_BAM_LEADER_SCHEDULE_RECHECK_NONE_SLOT;
  state->heap_wksp       = wksp;
  state->tcp_sock        = -1;
  state->admin_rpc_fd    = -1;
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

  fd_histf_new( state->metrics.builder_heartbeat_arrival_delta_nanos,
      FD_MHIST_MIN( BAM, BUILDER_HEARTBEAT_ARRIVAL_DELTA_NANOS ),
      FD_MHIST_MAX( BAM, BUILDER_HEARTBEAT_ARRIVAL_DELTA_NANOS ) );
  fd_histf_new( state->metrics.scheduler_pong_send_nanos,
      FD_MHIST_MIN( BAM, SCHEDULER_PONG_SEND_NANOS ),
      FD_MHIST_MAX( BAM, SCHEDULER_PONG_SEND_NANOS ) );
  return env;
}

FD_FN_UNUSED static ulong
test_bam_env_drain_pending_txns( test_bam_env_t * env ) {
  fd_bam_tile_t * state = env->state;
  ulong out_idx = state->verify_out.idx;
  ulong seq_before = env->stem_seqs[ out_idx ];
  ulong pending_before = bam_pending_txn_cnt( state->pending_txns );
  ulong * prev_fseq = state->bam_status_fseq;
  ulong   prev_fseq_val = prev_fseq ? fd_fseq_query( prev_fseq ) : 0UL;
  uchar fseq_mem[ FD_FSEQ_FOOTPRINT ] __attribute__((aligned(FD_FSEQ_ALIGN)));
  void * fseq_shmem = NULL;
  if( FD_LIKELY( prev_fseq ) ) {
    fd_fseq_update( prev_fseq, FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
  } else {
    fseq_shmem = fd_fseq_new( fseq_mem, FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
    FD_TEST( fseq_shmem );
    state->bam_status_fseq = fd_fseq_join( fseq_shmem );
    FD_TEST( state->bam_status_fseq );
  }
  int opt_poll_in = 1;
  int charge_busy = 0;
  fd_bam_test_after_credit( state, env->stem, &opt_poll_in, &charge_busy );
  if( FD_LIKELY( prev_fseq ) ) {
    fd_fseq_update( prev_fseq, prev_fseq_val );
  } else {
    FD_TEST( fd_fseq_leave( state->bam_status_fseq ) == fseq_shmem );
    FD_TEST( fd_fseq_delete( fseq_shmem ) == fseq_shmem );
    state->bam_status_fseq = NULL;
  }
  ulong published = env->stem_seqs[ out_idx ] - seq_before;
  if( FD_UNLIKELY( pending_before ) ) {
    FD_TEST( published>0UL );
    FD_TEST( !opt_poll_in );
    FD_TEST( charge_busy );
  }
  return published;
}

FD_FN_UNUSED static ulong
test_bam_env_drain_all_pending_txns( test_bam_env_t * env ) {
  ulong published = 0UL;
  while( !bam_pending_txn_empty( env->state->pending_txns ) ) published += test_bam_env_drain_pending_txns( env );
  return published;
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
  test_bam_env_mock_h2_hs( state );
  test_bam_env_inject_config_response( state );
  long now = fd_bam_now();
  state->bam_last_builder_activity_ns    = now;
  state->bam_builder_heartbeat_received  = 1U;
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
  if( env->state->pending_txns ) {
    fd_wksp_free_laddr( bam_pending_txn_delete( bam_pending_txn_leave( env->state->pending_txns ) ) );
    env->state->pending_txns = NULL;
    env->pending_txn_mem = NULL;
  }
  if( env->state->decoded_multi ) {
    fd_wksp_free_laddr( env->state->decoded_multi );
    env->state->decoded_multi = NULL;
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
