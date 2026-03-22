#define _GNU_SOURCE

#include "fd_bam_tile.h"
#include "fd_bam_tile_private.h"
#include "test_bam_common.c"
#include "../../ballet/nanopb/pb_encode.h"
#include "../../ballet/nanopb/pb_decode.h"

static uchar metrics_scratch[ FD_METRICS_FOOTPRINT( 0UL, 0UL ) ] __attribute__((aligned( FD_METRICS_ALIGN )));

__attribute__((weak)) char const fdctl_version_string[] = "0.0.0";

/* Deterministic clock for the synthetic scheduler/bank model. */
static long g_clock = 1L;

__attribute__((weak)) long
fd_bam_now( void ) {
  return g_clock;
}

/* ---------- Node-side protobuf helpers ---------- */

static size_t
bam_model_encode_scheduler_batches( bam_types_AtomicTxnBatch * batches,
                                  size_t                     batch_cnt,
                                  uchar *                    out,
                                  size_t                     out_sz ) {
  test_bam_batch_encode_ctx_t ctx = {
    .batches   = batches,
    .batch_cnt = batch_cnt
  };

  bam_api_SchedulerResponse resp = bam_api_SchedulerResponse_init_default;
  resp.which_versioned_msg = bam_api_SchedulerResponse_v0_tag;
  resp.versioned_msg.v0.which_resp = bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag;
  resp.versioned_msg.v0.resp.multiple_atomic_txn_batch.batches.funcs.encode = test_bam_encode_batches_cb;
  resp.versioned_msg.v0.resp.multiple_atomic_txn_batch.batches.arg          = &ctx;

  pb_ostream_t ostream = pb_ostream_from_buffer( out, out_sz );
  FD_TEST( pb_encode( &ostream, bam_api_SchedulerResponse_fields, &resp ) );
  return ostream.bytes_written;
}

static size_t
bam_model_encode_scheduler_heartbeat( ulong time_sent_microseconds,
                                    uchar * out,
                                    size_t  out_sz ) {
  bam_api_SchedulerResponse resp = bam_api_SchedulerResponse_init_default;
  resp.which_versioned_msg = bam_api_SchedulerResponse_v0_tag;
  resp.versioned_msg.v0.which_resp = bam_api_SchedulerResponseV0_heart_beat_tag;
  resp.versioned_msg.v0.resp.heart_beat.time_sent_microseconds = time_sent_microseconds;

  pb_ostream_t ostream = pb_ostream_from_buffer( out, out_sz );
  FD_TEST( pb_encode( &ostream, bam_api_SchedulerResponse_fields, &resp ) );
  return ostream.bytes_written;
}

static size_t
bam_model_encode_auth_challenge( char const * challenge,
                               uchar *      out,
                               size_t       out_sz ) {
  bam_api_AuthChallengeResponse resp = bam_api_AuthChallengeResponse_init_default;
  strlcpy( resp.challenge_to_sign, challenge, sizeof( resp.challenge_to_sign ) );
  pb_ostream_t ostream = pb_ostream_from_buffer( out, out_sz );
  FD_TEST( pb_encode( &ostream, bam_api_AuthChallengeResponse_fields, &resp ) );
  return ostream.bytes_written;
}

static size_t
bam_model_encode_config( uint commission_bps,
                       uchar const * prio_fee_recipient,
                       uchar * out,
                       size_t  out_sz ) {
  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  resp.has_bam_config = true;
  resp.bam_config.commission_bps = commission_bps;
  if( prio_fee_recipient ) {
    FD_TEST( fd_base58_encode_32( prio_fee_recipient, NULL, resp.bam_config.prio_fee_recipient_pubkey ) );
    resp.bam_config.prio_fee_recipient_pubkey[ FD_BASE58_ENCODED_32_SZ-1 ] = '\0';
  }

  /* Keep TPU contact valid so gossip/control path behaves like real connected mode. */
  resp.bam_config.has_tpu_sock = true;
  strlcpy( resp.bam_config.tpu_sock.ip, "127.0.0.1", sizeof( resp.bam_config.tpu_sock.ip ) );
  resp.bam_config.tpu_sock.port = 9007U;
  resp.bam_config.has_tpu_fwd_sock = true;
  strlcpy( resp.bam_config.tpu_fwd_sock.ip, "127.0.0.1", sizeof( resp.bam_config.tpu_fwd_sock.ip ) );
  resp.bam_config.tpu_fwd_sock.port = 9008U;

  resp.has_block_engine_config = true;
  resp.block_engine_config.builder_commission = 5U;
  FD_TEST( fd_base58_encode_32( prio_fee_recipient ? prio_fee_recipient : (uchar const[32]){1}, NULL,
                                resp.block_engine_config.builder_pubkey ) );

  pb_ostream_t ostream = pb_ostream_from_buffer( out, out_sz );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
  return ostream.bytes_written;
}

/* ---------- Outbound decode helper (collect AtomicTxnBatchResult stream) ---------- */

typedef struct {
  uint  seq_id;
  uchar committed;
  uchar which_reason;
  bam_types_SchedulingError          scheduling_error;
  bam_types_DeserializationErrorReason deser_reason;
  bam_types_TransactionErrorReason   txn_reason;
  uchar idx;
} bam_model_wire_result_t;

FD_FN_UNUSED static _Bool
bam_model_decode_last_scheduler_message( fd_bam_tile_t *          state,
                                       bam_api_SchedulerMessage * out ) {
  fd_grpc_client_t * client = state->grpc_client;
  fd_grpc_hdr_t hdr;
  fd_memcpy( &hdr, client->nanopb_tx, sizeof(fd_grpc_hdr_t) );
  uint msg_sz = fd_uint_bswap( hdr.msg_sz );
  if( FD_UNLIKELY( !msg_sz ) ) return 0;
  if( FD_UNLIKELY( msg_sz > state->grpc_buf_max ) ) return 0;

  *out = (bam_api_SchedulerMessage)bam_api_SchedulerMessage_init_default;
  pb_istream_t stream = pb_istream_from_buffer( client->nanopb_tx + sizeof(fd_grpc_hdr_t), msg_sz );
  return pb_decode( &stream, bam_api_SchedulerMessage_fields, out );
}

FD_FN_UNUSED static _Bool
bam_model_decode_last_wire_result( fd_bam_tile_t *         state,
                                 bam_model_wire_result_t * out ) {
  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  if( FD_UNLIKELY( decoded.msg.which_versioned_msg != bam_api_SchedulerMessage_v0_tag ) ) {
    FD_LOG_WARNING(( "unexpected wire versioned_msg=%u", decoded.msg.which_versioned_msg ));
    return 0;
  }
  if( FD_UNLIKELY( decoded.msg.versioned_msg.v0.which_msg != bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag ) ) {
    FD_LOG_WARNING(( "unexpected wire msg kind=%u", decoded.msg.versioned_msg.v0.which_msg ));
    return 0;
  }
  if( FD_UNLIKELY( decoded.multi.result_cnt != 1UL ) ) {
    FD_LOG_WARNING(( "unexpected wire result_cnt=%lu", decoded.multi.result_cnt ));
    return 0;
  }

  bam_types_AtomicTxnBatchResult const * res = &decoded.multi.results[0];
  fd_memset( out, 0, sizeof(*out) );
  out->seq_id = res->seq_id;
  if( res->which_result == bam_types_AtomicTxnBatchResult_committed_tag ) {
    out->committed = 1U;
    out->which_reason = 0U;
    return 1;
  }

  out->committed = 0U;
  out->which_reason = (uchar)res->result.not_committed.which_reason;
  if( res->result.not_committed.which_reason == bam_types_NotCommitted_scheduling_error_tag ) {
    out->scheduling_error = res->result.not_committed.reason.scheduling_error;
  } else if( res->result.not_committed.which_reason == bam_types_NotCommitted_deserialization_error_tag ) {
    out->deser_reason = res->result.not_committed.reason.deserialization_error.reason;
    out->idx          = (uchar)res->result.not_committed.reason.deserialization_error.index;
  } else if( res->result.not_committed.which_reason == bam_types_NotCommitted_transaction_error_tag ) {
    out->txn_reason = res->result.not_committed.reason.transaction_error.reason;
    out->idx        = (uchar)res->result.not_committed.reason.transaction_error.index;
  }
  return 1;
}

/* ---------- Synthetic scheduler/bank model ---------- */

typedef enum {
  BAM_MODEL_TXN_OK = 0,
  BAM_MODEL_TXN_VERIFY_SIG_FAIL = 1,
  BAM_MODEL_TXN_SANITIZE_FAIL = 2,
  BAM_MODEL_TXN_LUT_FAIL = 3,
  BAM_MODEL_TXN_BANK_FRONT_RUN_FAIL = 4,
  BAM_MODEL_TXN_LOCK_FAIL = 5,
  BAM_MODEL_TXN_EXEC_FAIL = 6,
  BAM_MODEL_TXN_POH_TIMEOUT = 7,
  BAM_MODEL_TXN_DUPLICATE = 8,
} bam_model_txn_mode_t;

typedef struct {
  uchar mode;
  uchar fee_lamports;
  uchar requested_cu;
  uchar actual_cu;
  uchar work_id;
} bam_model_txn_spec_t;

typedef struct {
  uint  seq_id;
  ulong max_schedule_slot;
  uchar batch_idx;
  uchar batch_cnt;
  uchar revert_on_error;
  bam_model_txn_spec_t spec;
} bam_model_stage_txn_t;

typedef struct {
  uint  seq_id;
  ulong max_schedule_slot;
  uchar revert_on_error;
  uchar txn_cnt;
  bam_model_stage_txn_t txn[ FD_PACK_MAX_TXN_PER_BUNDLE ];
} bam_model_batch_t;

typedef struct {
  uint  seq_id;
  ulong max_schedule_slot;
  uchar revert_on_error;
  uchar txn_cnt;
  uchar seen[ FD_PACK_MAX_TXN_PER_BUNDLE ];
  bam_model_stage_txn_t txn[ FD_PACK_MAX_TXN_PER_BUNDLE ];
  uchar active;
} bam_model_partial_batch_t;

typedef struct {
  uint seq_id;
  uchar intent_cnt;
} bam_model_intent_t;

typedef enum {
  BAM_TRACE_VERIFY = 0,
  BAM_TRACE_DEDUP  = 1,
  BAM_TRACE_RESOLV = 2,
  BAM_TRACE_PACK   = 3,
  BAM_TRACE_BANK   = 4,
} bam_model_trace_stage_t;

typedef struct {
  bam_model_trace_stage_t stage;
  uint  seq_id;
  uchar batch_idx;
  uchar batch_cnt;
  uchar revert_on_error;
  ulong max_schedule_slot;
  uchar work_id;
} bam_model_trace_t;

#define BAM_MODEL_MAX_READY_BATCHES 1024UL
#define BAM_MODEL_MAX_RESULTS       4096UL
#define BAM_MODEL_MAX_TRACES        16384UL
#define BAM_MODEL_MAX_INTENTS       4096UL

typedef struct {
  test_bam_env_t env[1];
  fd_bam_tile_t * state;

  ulong out_read_seq;

  /* Synthetic keyguard channels used by auth challenge / proof flow. */
  fd_frag_meta_t * keyguard_req_mcache;
  fd_frag_meta_t * keyguard_rsp_mcache;
  uchar *          keyguard_req_dcache;
  uchar *          keyguard_rsp_dcache;

  /* Leader/slot model */
  ulong leader_working_slot;
  ulong bankforks_working_slot;
  uchar leader_working_present;
  uchar leader_on;
  uchar bank_available;

  /* Admission limits */
  uint slot_cu_limit;
  uint slot_cu_used;
  uint slot_microblock_limit;
  uint slot_microblock_used;

  /* Replay / dedup model */
  uchar work_committed[ 256 ];

  /* Fee/CU accounting */
  ulong fee_lamports_total;
  ulong charged_txn_cnt;
  ulong consumed_cu_total;

  /* staging */
  bam_model_partial_batch_t partial[1];
  bam_model_batch_t ready[ BAM_MODEL_MAX_READY_BATCHES ];
  ulong ready_cnt;

  bam_model_intent_t intents[ BAM_MODEL_MAX_INTENTS ];
  ulong intent_cnt;

  fd_bam_bundle_result_t model_results[ BAM_MODEL_MAX_RESULTS ];
  ulong model_result_cnt;

  bam_model_trace_t traces[ BAM_MODEL_MAX_TRACES ];
  ulong trace_cnt;

  bam_model_wire_result_t wire_results[ BAM_MODEL_MAX_RESULTS ];
  ulong wire_result_cnt;

  ulong drop_cnt;
} bam_model_harness_t;

/* batch definitions pushed by node emulator */
typedef struct {
  uint  seq_id;
  ulong max_schedule_slot;
  uchar revert_on_error;
  uchar txn_cnt;
  bam_model_txn_spec_t txn[ FD_PACK_MAX_TXN_PER_BUNDLE ];
} bam_model_batch_def_t;

static void
bam_model_trace( bam_model_harness_t *      h,
               bam_model_trace_stage_t    stage,
               bam_model_stage_txn_t const * txn ) {
  if( FD_UNLIKELY( h->trace_cnt >= BAM_MODEL_MAX_TRACES ) ) return;
  bam_model_trace_t * tr = &h->traces[ h->trace_cnt++ ];
  tr->stage             = stage;
  tr->seq_id            = txn->seq_id;
  tr->batch_idx         = txn->batch_idx;
  tr->batch_cnt         = txn->batch_cnt;
  tr->revert_on_error   = txn->revert_on_error;
  tr->max_schedule_slot = txn->max_schedule_slot;
  tr->work_id           = txn->spec.work_id;
}

static inline ulong
bam_model_current_slot( bam_model_harness_t const * h ) {
  return h->leader_working_present ? h->leader_working_slot : h->bankforks_working_slot;
}

static void
bam_model_record_intent( bam_model_harness_t * h,
                       uint                 seq_id ) {
  for( ulong i=0UL; i<h->intent_cnt; i++ ) {
    if( h->intents[i].seq_id==seq_id ) {
      h->intents[i].intent_cnt++;
      return;
    }
  }
  if( FD_UNLIKELY( h->intent_cnt >= BAM_MODEL_MAX_INTENTS ) ) return;
  h->intents[ h->intent_cnt ].seq_id     = seq_id;
  h->intents[ h->intent_cnt ].intent_cnt = 1U;
  h->intent_cnt++;
}

static void
bam_model_try_enqueue_result( bam_model_harness_t *            h,
                            fd_bam_bundle_result_t const * res ) {
  fd_bam_tile_t * state = h->state;
  if( FD_UNLIKELY( state->feedback_queue_depth >= FD_BAM_MAX_PENDING_RESULTS ) ) {
    state->metrics.feedback_results_dropped_cnt++;
    h->drop_cnt++;
    return;
  }

  state->bam_results[ state->bam_results_tail ] = *res;
  state->bam_results_tail = (ushort)(( state->bam_results_tail + 1U ) % FD_BAM_MAX_PENDING_RESULTS );
  state->feedback_queue_depth = (ushort)( state->feedback_queue_depth + 1U );
}

static void
bam_model_emit_model_result( bam_model_harness_t *               h,
                         fd_bam_bundle_result_t const *    res ) {
  bam_model_record_intent( h, res->seq_id );
  if( FD_LIKELY( h->model_result_cnt < BAM_MODEL_MAX_RESULTS ) ) h->model_results[ h->model_result_cnt++ ] = *res;
  bam_model_try_enqueue_result( h, res );
}

static bam_model_txn_spec_t
bam_model_parse_payload( uchar const * payload,
                       ulong         payload_sz ) {
  bam_model_txn_spec_t spec = {0};
  if( FD_UNLIKELY( payload_sz<5UL ) ) {
    spec.mode         = BAM_MODEL_TXN_SANITIZE_FAIL;
    spec.fee_lamports = 0U;
    spec.requested_cu = 1U;
    spec.actual_cu    = 0U;
    spec.work_id      = 0U;
    return spec;
  }
  spec.mode         = payload[0];
  spec.fee_lamports = payload[1];
  spec.requested_cu = payload[2];
  spec.actual_cu    = payload[3];
  spec.work_id      = payload[4];
  return spec;
}

static void
bam_model_prepare_scheduler_stream( fd_bam_tile_t * state ) {
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
bam_model_keepalive_sync( fd_bam_tile_t * state,
                        long            now ) {
  state->keepalive->ts_next_tx  = now + state->keepalive_interval;
  state->keepalive->ts_deadline = 0L;
  state->keepalive->ts_last_tx  = now;
  state->keepalive->ts_last_rx  = now;
  state->keepalive->inflight    = 0U;
}

static void
bam_model_node_deliver_heartbeat( bam_model_harness_t * h ) {
  uchar pb[64];
  size_t pb_sz = bam_model_encode_scheduler_heartbeat( (ulong)fd_long_max( g_clock/1000L, 0L ), pb, sizeof(pb) );
  fd_bam_client_grpc_rx_msg( h->state, pb, pb_sz, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
}

static void
bam_model_node_push_config( bam_model_harness_t * h,
                          uint                 commission_bps ) {
  uchar pb[512];
  uchar pk[32];
  for( ulong i=0UL; i<32UL; i++ ) pk[i]=(uchar)(i+1U);
  size_t pb_sz = bam_model_encode_config( commission_bps, pk, pb, sizeof(pb) );
  fd_bam_client_grpc_rx_msg( h->state, pb, pb_sz, FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );
}

static void
bam_model_node_send_auth_challenge( bam_model_harness_t * h,
                                  char const *        challenge ) {
  FD_TEST( h->state->keyguard_client->request_mtu >=
           FD_BAM_AUTH_LABEL_LEN + strlen( challenge ) );

  uchar signature[ 64 ];
  for( uchar i=0U; i<64U; i++ ) signature[ i ] = (uchar)( i + 1U );
  ulong rsp_chunk = h->state->keyguard_client->response_chunk0;
  fd_memcpy( fd_chunk_to_laddr( h->state->keyguard_client->response_mem, rsp_chunk ),
             signature, sizeof(signature) );
  fd_mcache_publish( h->keyguard_rsp_mcache,
                     h->state->keyguard_client->response_depth,
                     h->state->keyguard_client->response_seq,
                     0UL,
                     rsp_chunk,
                     sizeof(signature),
                     0UL,
                     0UL,
                     0UL );

  uchar pb[256];
  size_t pb_sz = bam_model_encode_auth_challenge( challenge, pb, sizeof(pb) );
  h->state->bam_auth_inflight = 1U;
  fd_bam_client_grpc_rx_msg( h->state, pb, pb_sz, FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge );
}

static void
bam_model_node_deliver_batches( bam_model_harness_t *      h,
                              bam_model_batch_def_t const * defs,
                              ulong                     def_cnt,
                              uchar                     drop_last_idx ) {
  bam_types_AtomicTxnBatch batches[ 32 ];
  test_bam_packet_encode_ctx_t packet_ctx[ 32 ];
  bam_types_Packet packets[ 32 ][ FD_PACK_MAX_TXN_PER_BUNDLE ];
  FD_TEST( def_cnt <= 32UL );

  for( ulong i=0UL; i<def_cnt; i++ ) {
    bam_model_batch_def_t const * def = &defs[ i ];
    batches[ i ] = (bam_types_AtomicTxnBatch)bam_types_AtomicTxnBatch_init_default;
    batches[ i ].seq_id            = def->seq_id;
    batches[ i ].max_schedule_slot = def->max_schedule_slot;

    packet_ctx[ i ].packet_cnt = def->txn_cnt;
    packet_ctx[ i ].packets    = packets[ i ];

    for( ulong j=0UL; j<def->txn_cnt; j++ ) {
      bam_types_Packet * pkt = &packets[ i ][ j ];
      *pkt = (bam_types_Packet)bam_types_Packet_init_default;
      pkt->data.size    = 5U;
      pkt->data.bytes[0] = def->txn[ j ].mode;
      pkt->data.bytes[1] = def->txn[ j ].fee_lamports;
      pkt->data.bytes[2] = def->txn[ j ].requested_cu;
      pkt->data.bytes[3] = def->txn[ j ].actual_cu;
      pkt->data.bytes[4] = def->txn[ j ].work_id;
      pkt->has_meta = 1U;
      pkt->meta.size = 5U;
      pkt->meta.has_flags = 1U;
      pkt->meta.flags.revert_on_error = def->revert_on_error;
      if( FD_UNLIKELY( drop_last_idx && j==def->txn_cnt-1U ) ) {
        /* Incomplete delivery injection for missing-index scenarios. */
        packet_ctx[ i ].packet_cnt = def->txn_cnt - 1U;
      }
    }

    batches[ i ].packets.funcs.encode = test_bam_encode_packets_cb;
    batches[ i ].packets.arg          = &packet_ctx[ i ];
  }

  uchar pb[4096];
  size_t pb_sz = bam_model_encode_scheduler_batches( batches, def_cnt, pb, sizeof(pb) );
  fd_bam_client_grpc_rx_msg( h->state, pb, pb_sz, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
}

static void
bam_model_set_partial( bam_model_partial_batch_t * p,
                     bam_model_stage_txn_t const * t ) {
  fd_memset( p, 0, sizeof(*p) );
  p->active            = 1U;
  p->seq_id            = t->seq_id;
  p->max_schedule_slot = t->max_schedule_slot;
  p->revert_on_error   = t->revert_on_error;
  p->txn_cnt           = t->batch_cnt;
}

static void
bam_model_emit_partial_missing( bam_model_harness_t * h,
                              bam_model_partial_batch_t * p ) {
  if( FD_UNLIKELY( !p->active ) ) return;
  if( FD_UNLIKELY( !p->revert_on_error ) ) {
    p->active = 0U;
    return;
  }

  fd_bam_bundle_result_t res = {0};
  res.seq_id            = p->seq_id;
  res.slot              = p->max_schedule_slot;
  res.bundle_txn_cnt    = p->txn_cnt;
  res.execution_success = 0U;
  res.scheduling_error  = FD_BAM_SCHED_ERR_NONE;
  res.bundle_err        = FD_BAM_BUNDLE_ERR_NONE;
  for( uchar i=0U; i<p->txn_cnt; i++ ) {
    res.sanitize_success[ i ] = 1U;
    if( FD_UNLIKELY( !p->seen[ i ] ) ) {
      res.transaction_err[ i ] = bam_types_TransactionErrorReason_SIGNATURE_FAILURE;
      res.transaction_err_count++;
    }
  }

  bam_model_emit_model_result( h, &res );
  p->active = 0U;
}

static void
bam_model_ready_push( bam_model_harness_t * h,
                    bam_model_batch_t const * b ) {
  if( FD_UNLIKELY( h->ready_cnt>=BAM_MODEL_MAX_READY_BATCHES ) ) return;
  h->ready[ h->ready_cnt++ ] = *b;
}

static inline int
bam_model_has_slot_hint( ulong max_schedule_slot ) {
  return max_schedule_slot!=FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT &&
         max_schedule_slot!=0UL;
}

static int
bam_model_batch_precheck_fail( bam_model_batch_t const * b,
                             ulong                   current_slot,
                             fd_bam_bundle_result_t * out ) {
  if( FD_UNLIKELY( !b->txn_cnt ) ) {
    *out = (fd_bam_bundle_result_t){
      .seq_id            = b->seq_id,
      .slot              = b->max_schedule_slot,
      .bundle_txn_cnt    = 0U,
      .execution_success = 0U,
      .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
      .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
      .deser_reason      = bam_types_DeserializationErrorReason_EMPTY,
      .deser_index       = 0U,
    };
    return 1;
  }

  if( FD_UNLIKELY( b->txn_cnt>FD_PACK_MAX_TXN_PER_BUNDLE ) ) {
    *out = (fd_bam_bundle_result_t){
      .seq_id            = b->seq_id,
      .slot              = b->max_schedule_slot,
      .bundle_txn_cnt    = b->txn_cnt,
      .execution_success = 0U,
      .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
      .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
      .deser_reason      = bam_types_DeserializationErrorReason_SANITIZE_ERROR,
      .deser_index       = 0U,
    };
    return 1;
  }

  if( FD_UNLIKELY( bam_model_has_slot_hint( b->max_schedule_slot ) &&
                   b->max_schedule_slot<current_slot ) ) {
    *out = (fd_bam_bundle_result_t){
      .seq_id            = b->seq_id,
      .slot              = b->max_schedule_slot,
      .bundle_txn_cnt    = b->txn_cnt,
      .execution_success = 0U,
      .scheduling_error  = FD_BAM_SCHED_ERR_OUTSIDE_SLOT,
      .bundle_err        = FD_BAM_BUNDLE_ERR_NONE,
    };
    return 1;
  }

  return 0;
}

static void
bam_model_sort_ready_by_priority( bam_model_harness_t * h ) {
  for( ulong i=0UL; i<h->ready_cnt; i++ ) {
    for( ulong j=i+1UL; j<h->ready_cnt; j++ ) {
      ulong prio_i = ULONG_MAX - (ulong)h->ready[i].seq_id;
      ulong prio_j = ULONG_MAX - (ulong)h->ready[j].seq_id;
      if( FD_UNLIKELY( prio_j > prio_i ) ) {
        bam_model_batch_t t = h->ready[i];
        h->ready[i] = h->ready[j];
        h->ready[j] = t;
      }
    }
  }
}

static fd_bam_bundle_result_t
bam_model_execute_batch( bam_model_harness_t * h,
                       bam_model_batch_t const * b ) {
  fd_bam_bundle_result_t res = {0};
  res.seq_id            = b->seq_id;
  res.slot              = bam_model_current_slot( h );
  res.bundle_txn_cnt    = b->txn_cnt;
  res.execution_success = 1U;
  res.scheduling_error  = FD_BAM_SCHED_ERR_NONE;
  res.bundle_err        = FD_BAM_BUNDLE_ERR_NONE;

  uint requested_total = 0U;
  uint actual_total    = 0U;

  for( uchar i=0U; i<b->txn_cnt; i++ ) {
    bam_model_txn_spec_t const * spec = &b->txn[i].spec;
    requested_total += spec->requested_cu;
    actual_total    += spec->actual_cu;
  }

  if( FD_UNLIKELY( h->slot_microblock_used + b->txn_cnt > h->slot_microblock_limit ) ) {
    res.execution_success = 0U;
    res.scheduling_error  = FD_BAM_SCHED_ERR_CONTAINER_FULL;
    return res;
  }

  if( FD_UNLIKELY( h->slot_cu_used + requested_total > h->slot_cu_limit ) ) {
    res.execution_success = 0U;
    if( b->revert_on_error ) {
      res.transaction_err_count = b->txn_cnt;
      for( uchar i=0U; i<b->txn_cnt; i++ ) {
        res.sanitize_success[i] = 1U;
        res.transaction_err[i]  = bam_types_TransactionErrorReason_COMMIT_CANCELLED;
      }
      res.transaction_err[0] = bam_types_TransactionErrorReason_WOULD_EXCEED_MAX_BLOCK_COST_LIMIT;
    } else {
      res.transaction_err_count = 1U;
      res.sanitize_success[0]   = 1U;
      res.transaction_err[0]    = bam_types_TransactionErrorReason_WOULD_EXCEED_MAX_BLOCK_COST_LIMIT;
    }
    return res;
  }

  /* Replay/idempotence guard: already committed work must not commit twice. */
  for( uchar i=0U; i<b->txn_cnt; i++ ) {
    if( FD_UNLIKELY( h->work_committed[ b->txn[i].spec.work_id ] ) ) {
      res.execution_success = 0U;
      if( b->revert_on_error ) {
        res.transaction_err_count = b->txn_cnt;
        for( uchar j=0U; j<b->txn_cnt; j++ ) {
          res.sanitize_success[j] = 1U;
          res.transaction_err[j]  = bam_types_TransactionErrorReason_COMMIT_CANCELLED;
        }
      } else {
        res.transaction_err_count = 1U;
        res.sanitize_success[0]   = 1U;
      }
      res.transaction_err[i] = bam_types_TransactionErrorReason_ALREADY_PROCESSED;
      return res;
    }
  }

  ulong first_exec_fail_idx = ULONG_MAX;
  ulong first_lock_fail_idx = ULONG_MAX;
  ulong first_timeout_idx   = ULONG_MAX;

  for( uchar i=0U; i<b->txn_cnt; i++ ) {
    bam_model_txn_spec_t const * spec = &b->txn[i].spec;
    res.sanitize_success[i] = 1U;

    if( FD_UNLIKELY( spec->actual_cu > spec->requested_cu ) ) {
      if( first_exec_fail_idx==ULONG_MAX ) first_exec_fail_idx = i;
      res.transaction_err[i] = bam_types_TransactionErrorReason_WOULD_EXCEED_MAX_BLOCK_COST_LIMIT;
      continue;
    }

    switch( (bam_model_txn_mode_t)spec->mode ) {
      case BAM_MODEL_TXN_LOCK_FAIL:
        if( first_lock_fail_idx==ULONG_MAX ) first_lock_fail_idx = i;
        res.transaction_err[i] = bam_types_TransactionErrorReason_ACCOUNT_IN_USE;
        break;
      case BAM_MODEL_TXN_EXEC_FAIL:
        if( first_exec_fail_idx==ULONG_MAX ) first_exec_fail_idx = i;
        res.transaction_err[i] = bam_types_TransactionErrorReason_INSTRUCTION_ERROR;
        break;
      case BAM_MODEL_TXN_POH_TIMEOUT:
        if( first_timeout_idx==ULONG_MAX ) first_timeout_idx = i;
        break;
      default:
        break;
    }
  }

  if( FD_UNLIKELY( !h->bank_available ) ) {
    res.execution_success = 0U;
    res.scheduling_error = FD_BAM_SCHED_ERR_POH_TIMEOUT;
    return res;
  }

  if( b->revert_on_error ) {
    if( FD_UNLIKELY( first_timeout_idx!=ULONG_MAX && first_exec_fail_idx==ULONG_MAX && first_lock_fail_idx==ULONG_MAX ) ) {
      res.execution_success = 0U;
      res.scheduling_error = FD_BAM_SCHED_ERR_POH_TIMEOUT;
      return res;
    }

    if( FD_UNLIKELY( first_exec_fail_idx!=ULONG_MAX || first_lock_fail_idx!=ULONG_MAX ) ) {
      ulong fail_idx = first_lock_fail_idx!=ULONG_MAX ? first_lock_fail_idx : first_exec_fail_idx;
      bam_types_TransactionErrorReason fail_reason = res.transaction_err[ fail_idx ];
      res.execution_success = 0U;
      res.transaction_err_count = b->txn_cnt;
      for( uchar i=0U; i<b->txn_cnt; i++ ) res.transaction_err[i] = bam_types_TransactionErrorReason_COMMIT_CANCELLED;
      res.transaction_err[ fail_idx ] = fail_reason;
      return res;
    }

    /* Atomic all-success commit. */
    for( uchar i=0U; i<b->txn_cnt; i++ ) {
      h->work_committed[ b->txn[i].spec.work_id ] = 1U;
      h->fee_lamports_total += b->txn[i].spec.fee_lamports;
      h->charged_txn_cnt++;
      h->consumed_cu_total += b->txn[i].spec.actual_cu;
      res.consumed_cus[i] = b->txn[i].spec.actual_cu;
    }
    h->slot_cu_used += requested_total;
    h->slot_microblock_used += b->txn_cnt;
    return res;
  }

  /* Non-atomic (single-txn expected). */
  FD_TEST( b->txn_cnt==1U );
  bam_model_txn_spec_t const * spec0 = &b->txn[0].spec;
  if( FD_UNLIKELY( first_timeout_idx!=ULONG_MAX ) ) {
    res.execution_success = 0U;
    res.scheduling_error  = FD_BAM_SCHED_ERR_POH_TIMEOUT;
    return res;
  }

  if( FD_UNLIKELY( first_lock_fail_idx!=ULONG_MAX ) ) {
    res.execution_success = 0U;
    res.transaction_err_count = 1U;
    res.transaction_err[0] = bam_types_TransactionErrorReason_ACCOUNT_IN_USE;
    return res;
  }

  if( FD_UNLIKELY( first_exec_fail_idx!=ULONG_MAX ) ) {
    /* Fee-only commit semantics for non-atomic execution error. */
    res.execution_success = 1U;
    res.transaction_err_count = 1U;
    res.transaction_err[0] = bam_types_TransactionErrorReason_INSTRUCTION_ERROR;
    res.consumed_cus[0] = spec0->actual_cu;
    h->work_committed[ spec0->work_id ] = 1U;
    h->fee_lamports_total += spec0->fee_lamports;
    h->charged_txn_cnt++;
    h->consumed_cu_total += spec0->actual_cu;
    h->slot_cu_used += requested_total;
    h->slot_microblock_used += 1U;
    return res;
  }

  res.execution_success = 1U;
  res.consumed_cus[0] = spec0->actual_cu;
  h->work_committed[ spec0->work_id ] = 1U;
  h->fee_lamports_total += spec0->fee_lamports;
  h->charged_txn_cnt++;
  h->consumed_cu_total += spec0->actual_cu;
  h->slot_cu_used += requested_total;
  h->slot_microblock_used += 1U;
  return res;
}

static fd_bam_bundle_result_t
bam_model_make_outside_slot_result( uint  seq_id,
                                    ulong slot,
                                    uchar txn_cnt ) {
  return (fd_bam_bundle_result_t){
    .seq_id            = seq_id,
    .slot              = slot,
    .bundle_txn_cnt    = txn_cnt,
    .execution_success = 0U,
    .scheduling_error  = FD_BAM_SCHED_ERR_OUTSIDE_SLOT,
    .bundle_err        = FD_BAM_BUNDLE_ERR_NONE,
  };
}

static void
bam_model_process_ready( bam_model_harness_t * h ) {
  if( FD_UNLIKELY( !h->ready_cnt ) ) return;
  bam_model_sort_ready_by_priority( h );
  ulong current_slot = bam_model_current_slot( h );

  for( ulong i=0UL; i<h->ready_cnt; i++ ) {
    bam_model_batch_t const * b = &h->ready[ i ];
    fd_bam_bundle_result_t res;

    if( FD_UNLIKELY( bam_model_has_slot_hint( b->max_schedule_slot ) &&
                     b->max_schedule_slot<current_slot ) ) {
      res = bam_model_make_outside_slot_result( b->seq_id, b->max_schedule_slot, b->txn_cnt );
      bam_model_emit_model_result( h, &res );
      continue;
    }

    if( FD_UNLIKELY( !h->leader_on ) ) {
      res = bam_model_make_outside_slot_result( b->seq_id, current_slot, b->txn_cnt );
      bam_model_emit_model_result( h, &res );
      continue;
    }

    res = bam_model_execute_batch( h, b );
    bam_model_emit_model_result( h, &res );
  }

  h->ready_cnt = 0UL;
}

static void
bam_model_absorb_verify_output( bam_model_harness_t * h ) {
  ulong published = h->env->stem_seqs[0];
  ulong depth     = h->env->stem_depths[0];
  while( h->out_read_seq < published ) {
    fd_frag_meta_t const * meta = &h->env->out_mcache[ fd_mcache_line_idx( h->out_read_seq, depth ) ];
    fd_txn_m_t const * txnm = (fd_txn_m_t const *)fd_chunk_to_laddr_const( h->state->verify_out.mem, meta->chunk );

    bam_model_stage_txn_t t = {
      .seq_id            = txnm->bam.seq_id,
      .max_schedule_slot = txnm->bam.max_schedule_slot,
      .batch_idx         = txnm->bam.batch_idx,
      .batch_cnt         = txnm->bam.txn_cnt,
      .revert_on_error   = txnm->bam.revert_on_error,
      .spec              = bam_model_parse_payload( fd_txn_m_payload_const( txnm ), txnm->payload_sz ),
    };

    bam_model_trace( h, BAM_TRACE_VERIFY, &t );
    bam_model_trace( h, BAM_TRACE_DEDUP,  &t );
    bam_model_trace( h, BAM_TRACE_RESOLV, &t );

    bam_model_partial_batch_t * p = h->partial;

    if( FD_UNLIKELY( p->active && p->revert_on_error && p->seq_id!=t.seq_id ) ) {
      bam_model_emit_partial_missing( h, p );
    }

    if( FD_UNLIKELY( p->active && p->seq_id==t.seq_id && p->revert_on_error!=t.revert_on_error ) ) {
      fd_bam_bundle_result_t rej = {
        .seq_id            = t.seq_id,
        .slot              = t.max_schedule_slot,
        .bundle_txn_cnt    = t.batch_cnt,
        .execution_success = 0U,
        .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
        .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
        .deser_reason      = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE,
        .deser_index       = t.batch_idx,
      };
      bam_model_emit_model_result( h, &rej );
      p->active = 0U;
      h->out_read_seq++;
      continue;
    }

    if( FD_UNLIKELY( !p->active ) ) bam_model_set_partial( p, &t );
    if( FD_UNLIKELY( p->seq_id!=t.seq_id ) ) bam_model_set_partial( p, &t );

    /* enforce per-batch prevalidation at first observed packet */
    if( FD_UNLIKELY( t.batch_idx==0U ) ) {
      bam_model_batch_t tmp = {
        .seq_id            = t.seq_id,
        .max_schedule_slot = t.max_schedule_slot,
        .revert_on_error   = t.revert_on_error,
        .txn_cnt           = t.batch_cnt,
      };
      fd_bam_bundle_result_t rej;
      if( FD_UNLIKELY( bam_model_batch_precheck_fail( &tmp, bam_model_current_slot( h ), &rej ) ) ) {
        bam_model_emit_model_result( h, &rej );
        p->active = 0U;
        h->out_read_seq++;
        continue;
      }
    }

    if( FD_UNLIKELY( t.batch_idx>=FD_PACK_MAX_TXN_PER_BUNDLE || t.batch_idx>=t.batch_cnt ) ) {
      fd_bam_bundle_result_t rej = {
        .seq_id            = t.seq_id,
        .slot              = t.max_schedule_slot,
        .bundle_txn_cnt    = t.batch_cnt,
        .execution_success = 0U,
        .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
        .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
        .deser_reason      = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE,
        .deser_index       = t.batch_idx,
      };
      bam_model_emit_model_result( h, &rej );
      p->active = 0U;
      h->out_read_seq++;
      continue;
    }

    p->txn[ t.batch_idx ]  = t;
    p->seen[ t.batch_idx ] = 1U;

    if( FD_UNLIKELY( t.spec.mode==BAM_MODEL_TXN_VERIFY_SIG_FAIL ||
                     t.spec.mode==BAM_MODEL_TXN_SANITIZE_FAIL   ||
                     t.spec.mode==BAM_MODEL_TXN_LUT_FAIL ) ) {
      fd_bam_bundle_result_t rej = {
        .seq_id            = t.seq_id,
        .slot              = t.max_schedule_slot,
        .bundle_txn_cnt    = t.batch_cnt,
        .execution_success = 0U,
        .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
        .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
        .deser_reason      = bam_types_DeserializationErrorReason_SANITIZE_ERROR,
        .deser_index       = t.batch_idx,
      };
      bam_model_emit_model_result( h, &rej );
      p->active = 0U;
      h->out_read_seq++;
      continue;
    }

    if( FD_UNLIKELY( t.spec.mode==BAM_MODEL_TXN_BANK_FRONT_RUN_FAIL ) ) {
      fd_bam_bundle_result_t rej = {
        .seq_id            = t.seq_id,
        .slot              = t.max_schedule_slot,
        .bundle_txn_cnt    = t.batch_cnt,
        .execution_success = 0U,
        .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
        .bundle_err        = FD_BAM_BUNDLE_ERR_NONE,
      };
      if( t.revert_on_error ) {
        rej.transaction_err_count = t.batch_cnt;
        for( uchar j=0U; j<t.batch_cnt; j++ ) {
          rej.sanitize_success[j] = 1U;
          rej.transaction_err[j]  = bam_types_TransactionErrorReason_COMMIT_CANCELLED;
        }
      } else {
        rej.transaction_err_count = 1U;
        rej.sanitize_success[0] = 1U;
      }
      rej.transaction_err[ t.batch_idx ] = bam_types_TransactionErrorReason_ALREADY_PROCESSED;
      bam_model_emit_model_result( h, &rej );
      p->active = 0U;
      h->out_read_seq++;
      continue;
    }

    bam_model_trace( h, BAM_TRACE_PACK, &t );

    uchar complete = 1U;
    for( uchar j=0U; j<p->txn_cnt; j++ ) complete &= p->seen[j];

    if( FD_UNLIKELY( complete ) ) {
      bam_model_batch_t b = {
        .seq_id            = p->seq_id,
        .max_schedule_slot = p->max_schedule_slot,
        .revert_on_error   = p->revert_on_error,
        .txn_cnt           = p->txn_cnt,
      };
      for( uchar j=0U; j<p->txn_cnt; j++ ) {
        b.txn[j] = p->txn[j];
        bam_model_trace( h, BAM_TRACE_BANK, &b.txn[j] );
      }
      bam_model_ready_push( h, &b );
      p->active = 0U;
    }

    h->out_read_seq++;
  }
}

FD_FN_UNUSED static void
bam_model_flush_wire_results( bam_model_harness_t * h ) {
  while( h->state->feedback_queue_depth ) {
    ushort pending = h->state->feedback_queue_depth;
    h->state->feedback_queue_depth = 1U;
    FD_TEST( fd_bam_test_flush_results( h->state )==1 );
    if( FD_UNLIKELY( h->wire_result_cnt >= BAM_MODEL_MAX_RESULTS ) ) continue;
    bam_model_wire_result_t * wr = &h->wire_results[ h->wire_result_cnt ];
    if( FD_UNLIKELY( bam_model_decode_last_wire_result( h->state, wr ) ) ) h->wire_result_cnt++;
    h->state->feedback_queue_depth = (ushort)( pending-1U );
  }
}

static void
bam_model_apply_pipeline( bam_model_harness_t * h ) {
  bam_model_absorb_verify_output( h );
  bam_model_process_ready( h );
}

static void
bam_model_keyguard_setup( bam_model_harness_t * h,
                        fd_wksp_t *         wksp ) {
  ulong const depth        = 8UL;
  ulong const request_mtu  = 256UL;
  ulong const response_mtu = 64UL;

  void * request_mem = fd_wksp_alloc_laddr(
      wksp, fd_mcache_align(), fd_mcache_footprint( depth, 0UL ), 1UL );
  FD_TEST( request_mem );
  h->keyguard_req_mcache = fd_mcache_join(
      fd_mcache_new( request_mem, depth, 0UL, 0UL ) );
  FD_TEST( h->keyguard_req_mcache );

  void * response_mem = fd_wksp_alloc_laddr(
      wksp, fd_mcache_align(), fd_mcache_footprint( depth, 0UL ), 1UL );
  FD_TEST( response_mem );
  h->keyguard_rsp_mcache = fd_mcache_join(
      fd_mcache_new( response_mem, depth, 0UL, 0UL ) );
  FD_TEST( h->keyguard_rsp_mcache );

  ulong request_data_sz = fd_dcache_req_data_sz( request_mtu, depth, 1UL, 1UL );
  void * request_dcache_shmem = fd_wksp_alloc_laddr(
      wksp, fd_dcache_align(), fd_dcache_footprint( request_data_sz, 0UL ), 1UL );
  FD_TEST( request_dcache_shmem );
  h->keyguard_req_dcache = fd_dcache_join(
      fd_dcache_new( request_dcache_shmem, request_data_sz, 0UL ) );
  FD_TEST( h->keyguard_req_dcache );

  ulong response_data_sz = fd_dcache_req_data_sz( response_mtu, depth, 1UL, 1UL );
  void * response_dcache_shmem = fd_wksp_alloc_laddr(
      wksp, fd_dcache_align(), fd_dcache_footprint( response_data_sz, 0UL ), 1UL );
  FD_TEST( response_dcache_shmem );
  h->keyguard_rsp_dcache = fd_dcache_join(
      fd_dcache_new( response_dcache_shmem, response_data_sz, 0UL ) );
  FD_TEST( h->keyguard_rsp_dcache );

  FD_TEST( fd_keyguard_client_new( h->state->keyguard_client,
                                   h->keyguard_req_mcache,
                                   h->keyguard_req_dcache,
                                   h->keyguard_rsp_mcache,
                                   h->keyguard_rsp_dcache,
                                   request_mtu ) );
}

static void
bam_model_keyguard_teardown( bam_model_harness_t * h ) {
  if( h->keyguard_req_dcache ) {
    fd_wksp_free_laddr( fd_dcache_delete( fd_dcache_leave( h->keyguard_req_dcache ) ) );
    h->keyguard_req_dcache = NULL;
  }
  if( h->keyguard_rsp_dcache ) {
    fd_wksp_free_laddr( fd_dcache_delete( fd_dcache_leave( h->keyguard_rsp_dcache ) ) );
    h->keyguard_rsp_dcache = NULL;
  }
  if( h->keyguard_req_mcache ) {
    fd_wksp_free_laddr( fd_mcache_delete( fd_mcache_leave( h->keyguard_req_mcache ) ) );
    h->keyguard_req_mcache = NULL;
  }
  if( h->keyguard_rsp_mcache ) {
    fd_wksp_free_laddr( fd_mcache_delete( fd_mcache_leave( h->keyguard_rsp_mcache ) ) );
    h->keyguard_rsp_mcache = NULL;
  }
}

static void
bam_model_init( bam_model_harness_t * h,
              fd_wksp_t *         wksp ) {
  fd_memset( h, 0, sizeof(*h) );
  test_bam_env_create( h->env, wksp );
  h->state = h->env->state;
  bam_model_keyguard_setup( h, wksp );

  test_bam_env_mock_conn( h->env );
  bam_model_prepare_scheduler_stream( h->state );
  g_clock = (long)9e9;
  bam_model_keepalive_sync( h->state, g_clock );

  h->leader_working_slot    = 100UL;
  h->bankforks_working_slot = 99UL;
  h->leader_working_present = 1U;
  h->leader_on              = 1U;
  h->bank_available         = 1U;
  h->slot_cu_limit          = 1000U;
  h->slot_microblock_limit  = 32U;
  h->slot_cu_used           = 0U;
  h->slot_microblock_used   = 0U;

  bam_model_node_push_config( h, 300U );
  bam_model_node_deliver_heartbeat( h );
}

static void
bam_model_fini( bam_model_harness_t * h ) {
  bam_model_keyguard_teardown( h );
  test_bam_env_destroy( h->env );
}

static void
bam_model_assert_trace_metadata( bam_model_harness_t const * h ) {
  for( ulong i=0UL; i<h->trace_cnt; i++ ) {
    bam_model_trace_t const * tr = &h->traces[i];
    FD_TEST( tr->batch_cnt>0U && tr->batch_cnt<=FD_PACK_MAX_TXN_PER_BUNDLE );
    FD_TEST( tr->batch_idx < tr->batch_cnt );
    (void)tr;
  }
}

static void
bam_model_assert_intents( bam_model_harness_t const * h ) {
  for( ulong i=0UL; i<h->intent_cnt; i++ ) {
    FD_TEST( h->intents[i].intent_cnt==1U );
  }
}

static void
bam_model_expected_wire_result( fd_bam_bundle_result_t const * res,
                                bam_model_wire_result_t *      out ) {
  fd_memset( out, 0, sizeof(*out) );
  out->seq_id = res->seq_id;

  if( FD_LIKELY( res->execution_success ) ) {
    out->committed = 1U;
    return;
  }

  if( FD_UNLIKELY( res->bundle_err==FD_BAM_BUNDLE_ERR_DESER ) ) {
    out->which_reason = bam_types_NotCommitted_deserialization_error_tag;
    out->deser_reason = (bam_types_DeserializationErrorReason)res->deser_reason;
    out->idx          = res->deser_index;
    return;
  }

  if( FD_UNLIKELY( res->scheduling_error != FD_BAM_SCHED_ERR_NONE ) ) {
    if( FD_LIKELY( res->scheduling_error <= _bam_types_SchedulingError_MAX ) ) {
      out->which_reason     = bam_types_NotCommitted_scheduling_error_tag;
      out->scheduling_error = (bam_types_SchedulingError)res->scheduling_error;
    } else {
      out->which_reason = bam_types_NotCommitted_generic_invalid_tag;
    }
    return;
  }

  for( uchar i=0U; i<res->bundle_txn_cnt; i++ ) {
    if( FD_UNLIKELY( !res->sanitize_success[ i ] ) ) {
      out->which_reason = bam_types_NotCommitted_deserialization_error_tag;
      out->deser_reason = bam_types_DeserializationErrorReason_SANITIZE_ERROR;
      out->idx          = i;
      return;
    }
  }

  if( FD_UNLIKELY( res->transaction_err_count ) ) {
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
      out->which_reason     = bam_types_NotCommitted_scheduling_error_tag;
      out->scheduling_error = bam_types_SchedulingError_POH_TIMEOUT;
      return;
    }

    if( FD_LIKELY( res->transaction_err[ err_idx ] < _bam_types_TransactionErrorReason_ARRAYSIZE ) ) {
      out->which_reason = bam_types_NotCommitted_transaction_error_tag;
      out->txn_reason   = res->transaction_err[ err_idx ];
      out->idx          = err_idx;
    } else {
      out->which_reason = bam_types_NotCommitted_generic_invalid_tag;
    }
    return;
  }

  out->which_reason = bam_types_NotCommitted_generic_invalid_tag;
}

static void
bam_model_assert_wire_matches_model( bam_model_harness_t * h ) {
  bam_model_flush_wire_results( h );
  if( FD_UNLIKELY( h->wire_result_cnt != h->model_result_cnt ) ) {
    FD_LOG_WARNING(( "wire/model result count mismatch: wire=%lu model=%lu",
                     h->wire_result_cnt, h->model_result_cnt ));
    for( ulong i=0UL; i<h->model_result_cnt; i++ ) {
      FD_LOG_WARNING(( "model[%lu]: seq=%u success=%d sched=%u bundle_err=%u txn_err_cnt=%u",
                       i,
                       h->model_results[i].seq_id,
                       h->model_results[i].execution_success,
                       h->model_results[i].scheduling_error,
                       h->model_results[i].bundle_err,
                       h->model_results[i].transaction_err_count ));
    }
    for( ulong i=0UL; i<h->wire_result_cnt; i++ ) {
      FD_LOG_WARNING(( "wire[%lu]: seq=%u committed=%u which_reason=%u",
                       i,
                       h->wire_results[i].seq_id,
                       h->wire_results[i].committed,
                       h->wire_results[i].which_reason ));
    }
  }
  FD_TEST( h->wire_result_cnt == h->model_result_cnt );
  for( ulong i=0UL; i<h->model_result_cnt; i++ ) {
    bam_model_wire_result_t expected;
    bam_model_expected_wire_result( &h->model_results[i], &expected );

    bam_model_wire_result_t const * actual = &h->wire_results[i];
    FD_TEST( actual->seq_id       == expected.seq_id );
    FD_TEST( actual->committed    == expected.committed );
    FD_TEST( actual->which_reason == expected.which_reason );

    if( expected.which_reason == bam_types_NotCommitted_scheduling_error_tag ) {
      FD_TEST( actual->scheduling_error == expected.scheduling_error );
    } else if( expected.which_reason == bam_types_NotCommitted_deserialization_error_tag ) {
      FD_TEST( actual->deser_reason == expected.deser_reason );
      FD_TEST( actual->idx          == expected.idx );
    } else if( expected.which_reason == bam_types_NotCommitted_transaction_error_tag ) {
      FD_TEST( actual->txn_reason == expected.txn_reason );
      FD_TEST( actual->idx        == expected.idx );
    }
  }
}

/* ---------- Deterministic scenario matrix ---------- */

static bam_model_batch_def_t
bam_model_make_batch( uint  seq_id,
                    ulong max_schedule_slot,
                    uchar revert_on_error,
                    uchar txn_cnt ) {
  bam_model_batch_def_t def;
  fd_memset( &def, 0, sizeof(def) );
  def.seq_id            = seq_id;
  def.max_schedule_slot = max_schedule_slot;
  def.revert_on_error   = revert_on_error;
  def.txn_cnt           = txn_cnt;
  for( uchar i=0U; i<txn_cnt; i++ ) {
    def.txn[i].mode         = BAM_MODEL_TXN_OK;
    def.txn[i].fee_lamports = 10U + i;
    def.txn[i].requested_cu = 20U;
    def.txn[i].actual_cu    = 15U;
    def.txn[i].work_id      = (uchar)( seq_id + i );
  }
  return def;
}

static void
bam_model_run_scenario_atomic_success( bam_model_harness_t * h ) {
  bam_model_batch_def_t b = bam_model_make_batch( 10U, 100UL, 1U, 3U );
  bam_model_node_deliver_batches( h, &b, 1UL, 0U );
  bam_model_apply_pipeline( h );

  FD_TEST( h->model_result_cnt==1UL );
  FD_TEST( h->model_results[0].execution_success==1U );
  FD_TEST( h->charged_txn_cnt==3UL );
}

static void
bam_model_run_scenario_prevalidation_too_many_packets( bam_model_harness_t * h ) {
  bam_model_batch_t b = {
    .seq_id            = 901U,
    .max_schedule_slot = bam_model_current_slot( h ),
    .revert_on_error   = 1U,
    .txn_cnt           = (uchar)( FD_PACK_MAX_TXN_PER_BUNDLE + 1U ),
  };
  fd_bam_bundle_result_t rej = {0};
  FD_TEST( bam_model_batch_precheck_fail( &b, bam_model_current_slot( h ), &rej ) );
  FD_TEST( rej.bundle_err   == FD_BAM_BUNDLE_ERR_DESER );
  FD_TEST( rej.deser_reason == bam_types_DeserializationErrorReason_SANITIZE_ERROR );
  FD_TEST( rej.deser_index  == 0U );
}

static void
bam_model_run_scenario_prevalidation_inconsistent_revert( bam_model_harness_t * h ) {
  /* Seed a partial atomic batch and then inject a same-seq packet with a
     different revert flag to enforce INCONSISTENT_BUNDLE handling. */
  fd_memset( h->partial, 0, sizeof(*h->partial) );
  h->partial->active            = 1U;
  h->partial->seq_id            = 902U;
  h->partial->max_schedule_slot = 100UL;
  h->partial->revert_on_error   = 1U;
  h->partial->txn_cnt           = 2U;
  h->partial->seen[0]           = 1U;
  h->partial->txn[0] = (bam_model_stage_txn_t){
    .seq_id            = 902U,
    .max_schedule_slot = 100UL,
    .batch_idx         = 0U,
    .batch_cnt         = 2U,
    .revert_on_error   = 1U,
    .spec              = {
      .mode         = BAM_MODEL_TXN_OK,
      .fee_lamports = 10U,
      .requested_cu = 20U,
      .actual_cu    = 15U,
      .work_id      = 1U,
    },
  };

  bam_model_batch_def_t b = bam_model_make_batch( 902U, 100UL, 0U, 1U );
  bam_model_node_deliver_batches( h, &b, 1UL, 0U );
  bam_model_apply_pipeline( h );

  FD_TEST( h->model_result_cnt==1UL );
  FD_TEST( h->model_results[0].seq_id       == 902U );
  FD_TEST( h->model_results[0].bundle_err   == FD_BAM_BUNDLE_ERR_DESER );
  FD_TEST( h->model_results[0].deser_reason == bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE );
  FD_TEST( h->model_results[0].deser_index  == 0U );
}

static void
bam_model_run_scenario_prevalidation_stale_slot( bam_model_harness_t * h ) {
  h->leader_working_present = 1U;
  h->leader_working_slot    = 200UL;
  h->bankforks_working_slot = 150UL;

  bam_model_batch_def_t b = bam_model_make_batch( 903U, 199UL, 1U, 2U );
  bam_model_node_deliver_batches( h, &b, 1UL, 0U );
  bam_model_apply_pipeline( h );

  FD_TEST( h->model_result_cnt==1UL );
  FD_TEST( !h->model_results[0].execution_success );
  FD_TEST( h->model_results[0].scheduling_error==FD_BAM_SCHED_ERR_OUTSIDE_SLOT );
}

static void
bam_model_run_scenario_non_atomic_stale_slot( bam_model_harness_t * h ) {
  h->leader_working_present = 1U;
  h->leader_working_slot    = 220UL;
  h->bankforks_working_slot = 220UL;

  bam_model_batch_def_t b = bam_model_make_batch( 930U, 219UL, 0U, 1U );
  bam_model_node_deliver_batches( h, &b, 1UL, 0U );
  bam_model_apply_pipeline( h );

  FD_TEST( h->model_result_cnt==1UL );
  FD_TEST( !h->model_results[0].execution_success );
  FD_TEST( h->model_results[0].scheduling_error==FD_BAM_SCHED_ERR_OUTSIDE_SLOT );
}

static void
bam_model_run_scenario_slot_source_fallback( bam_model_harness_t * h ) {
  h->leader_working_present = 0U;
  h->bankforks_working_slot = 140UL;

  bam_model_batch_def_t b = bam_model_make_batch( 904U, 139UL, 1U, 1U );
  bam_model_node_deliver_batches( h, &b, 1UL, 0U );
  bam_model_apply_pipeline( h );

  FD_TEST( h->model_result_cnt==1UL );
  FD_TEST( !h->model_results[0].execution_success );
  FD_TEST( h->model_results[0].scheduling_error==FD_BAM_SCHED_ERR_OUTSIDE_SLOT );
}

static void
bam_model_run_scenario_non_leader_rejects_buffered_and_new_work( bam_model_harness_t * h ) {
  h->leader_on = 0U;

  bam_model_batch_def_t buffered = bam_model_make_batch( 905U, 100UL, 1U, 2U );
  bam_model_node_deliver_batches( h, &buffered, 1UL, 0U );
  bam_model_apply_pipeline( h );

  FD_TEST( h->model_result_cnt==1UL );
  FD_TEST( h->model_results[0].seq_id==905U );
  FD_TEST( !h->model_results[0].execution_success );
  FD_TEST( h->model_results[0].scheduling_error==FD_BAM_SCHED_ERR_OUTSIDE_SLOT );
  FD_TEST( h->ready_cnt==0UL );

  bam_model_batch_def_t fresh = bam_model_make_batch( 906U, 100UL, 1U, 1U );
  bam_model_node_deliver_batches( h, &fresh, 1UL, 0U );
  bam_model_apply_pipeline( h );
  FD_TEST( h->model_result_cnt==2UL );
  FD_TEST( h->model_results[1].seq_id==906U );
  FD_TEST( !h->model_results[1].execution_success );
  FD_TEST( h->model_results[1].scheduling_error==FD_BAM_SCHED_ERR_OUTSIDE_SLOT );
  FD_TEST( h->ready_cnt==0UL );

  h->leader_on = 1U;
  bam_model_process_ready( h );
  FD_TEST( h->model_result_cnt==2UL );
  FD_TEST( h->ready_cnt==0UL );
}

static void
bam_model_run_scenario_non_leader_rejects_work_without_slot_hint( bam_model_harness_t * h ) {
  h->leader_on = 0U;

  bam_model_batch_def_t buffered = bam_model_make_batch( 908U, 0UL, 1U, 1U );
  bam_model_node_deliver_batches( h, &buffered, 1UL, 0U );
  bam_model_apply_pipeline( h );
  FD_TEST( h->model_result_cnt==1UL );
  FD_TEST( h->model_results[0].seq_id==908U );
  FD_TEST( !h->model_results[0].execution_success );
  FD_TEST( h->model_results[0].scheduling_error==FD_BAM_SCHED_ERR_OUTSIDE_SLOT );
  FD_TEST( h->ready_cnt==0UL );

  h->leader_on = 1U;
  bam_model_process_ready( h );
  FD_TEST( h->model_result_cnt==1UL );
  FD_TEST( h->ready_cnt==0UL );
}

static void
bam_model_run_scenario_bank_unavailable_timeout( bam_model_harness_t * h ) {
  h->bank_available = 0U;
  bam_model_batch_def_t b = bam_model_make_batch( 907U, 100UL, 1U, 2U );
  bam_model_node_deliver_batches( h, &b, 1UL, 0U );
  bam_model_apply_pipeline( h );

  FD_TEST( h->model_result_cnt==1UL );
  FD_TEST( !h->model_results[0].execution_success );
  FD_TEST( h->model_results[0].scheduling_error==FD_BAM_SCHED_ERR_POH_TIMEOUT );
}

static void
bam_model_run_scenario_auth_and_control( bam_model_harness_t * h ) {
  char const challenge[] = "model-auth-challenge";
  bam_model_node_send_auth_challenge( h, challenge );

  /* Verify keyguard signing input is AUTH_LABEL || challenge bytes. */
  uchar expected_sign_payload[ FD_BAM_AUTH_LABEL_LEN + 64U ];
  ulong challenge_len = strlen( challenge );
  FD_TEST( challenge_len <= 64U );
  fd_memcpy( expected_sign_payload, FD_BAM_AUTH_LABEL, FD_BAM_AUTH_LABEL_LEN );
  fd_memcpy( expected_sign_payload + FD_BAM_AUTH_LABEL_LEN, challenge, challenge_len );
  fd_frag_meta_t const * req_meta =
      h->keyguard_req_mcache + fd_mcache_line_idx( 0UL, h->state->keyguard_client->request_depth );
  FD_TEST( req_meta->sz == (ushort)( FD_BAM_AUTH_LABEL_LEN + challenge_len ) );
  FD_TEST( 0==memcmp( fd_chunk_to_laddr_const( h->state->keyguard_client->request_mem, req_meta->chunk ),
                      expected_sign_payload,
                      FD_BAM_AUTH_LABEL_LEN + challenge_len ) );

  FD_TEST( h->state->bam_auth_ready==1U );
  FD_TEST( 0==strcmp( h->state->challenge_to_sign, challenge ) );
  FD_TEST( h->state->bam_auth_signature[0] != '\0' );

  uchar expected_sig_raw[ 64 ];
  for( uchar i=0U; i<64U; i++ ) expected_sig_raw[i] = (uchar)( i + 1U );
  char expected_sig[ FD_BASE58_ENCODED_64_SZ ];
  FD_TEST( fd_base58_encode_64( expected_sig_raw, NULL, expected_sig ) );
  FD_TEST( 0==strcmp( h->state->bam_auth_signature, expected_sig ) );

  /* Force reconnect state to ensure the next outbound stream message is AuthProof. */
  h->state->bam_stream            = NULL;
  h->state->bam_stream_live       = 0U;
  h->state->bam_stream_connecting = 0U;
  h->state->bam_auth_inflight     = 0U;
  h->state->bam_config_inflight   = 1U;
  h->state->builder_info_valid_until = g_clock + (long)1e9;
  h->state->bam_last_config_poll_ns  = g_clock;
  h->state->grpc_client->request_stream = NULL;
  *h->state->grpc_client->request_tx_op = (fd_h2_tx_op_t){0};

  (void)fd_bam_test_client_step_reconnect( h->state, g_clock );
  FD_TEST( h->state->bam_stream_connecting==1U );

  bam_api_SchedulerMessage out_msg = bam_api_SchedulerMessage_init_default;
  FD_TEST( bam_model_decode_last_scheduler_message( h->state, &out_msg ) );
  FD_TEST( out_msg.which_versioned_msg==bam_api_SchedulerMessage_v0_tag );
  FD_TEST( out_msg.versioned_msg.v0.which_msg==bam_api_SchedulerMessageV0_auth_proof_tag );
  FD_TEST( 0==strcmp( out_msg.versioned_msg.v0.msg.auth_proof.challenge_to_sign, challenge ) );
  FD_TEST( 0==strcmp( out_msg.versioned_msg.v0.msg.auth_proof.signature, expected_sig ) );
  FD_TEST( 0==strcmp( out_msg.versioned_msg.v0.msg.auth_proof.validator_pubkey,
                      h->state->bam_identity_pubkey_b58 ) );
}

static void
bam_model_run_scenario_atomic_verify_fail( bam_model_harness_t * h ) {
  bam_model_batch_def_t b = bam_model_make_batch( 11U, 100UL, 1U, 2U );
  b.txn[0].mode = BAM_MODEL_TXN_VERIFY_SIG_FAIL;
  bam_model_node_deliver_batches( h, &b, 1UL, 0U );
  bam_model_apply_pipeline( h );

  FD_TEST( h->model_result_cnt==1UL );
  FD_TEST( h->model_results[0].bundle_err==FD_BAM_BUNDLE_ERR_DESER );
  FD_TEST( h->model_results[0].deser_reason==bam_types_DeserializationErrorReason_SANITIZE_ERROR );
}

static void
bam_model_run_scenario_atomic_lut_fail( bam_model_harness_t * h ) {
  bam_model_batch_def_t b = bam_model_make_batch( 12U, 100UL, 1U, 2U );
  b.txn[1].mode = BAM_MODEL_TXN_LUT_FAIL;
  bam_model_node_deliver_batches( h, &b, 1UL, 0U );
  bam_model_apply_pipeline( h );

  FD_TEST( h->model_result_cnt==1UL );
  FD_TEST( h->model_results[0].bundle_err==FD_BAM_BUNDLE_ERR_DESER );
  FD_TEST( h->model_results[0].deser_index==1U );
}

static void
bam_model_run_scenario_atomic_exec_fail( bam_model_harness_t * h ) {
  bam_model_batch_def_t b = bam_model_make_batch( 13U, 100UL, 1U, 3U );
  b.txn[1].mode = BAM_MODEL_TXN_EXEC_FAIL;
  bam_model_node_deliver_batches( h, &b, 1UL, 0U );
  bam_model_apply_pipeline( h );

  FD_TEST( h->model_result_cnt==1UL );
  FD_TEST( !h->model_results[0].execution_success );
  FD_TEST( h->model_results[0].transaction_err_count==3U );
  FD_TEST( h->model_results[0].transaction_err[1]==bam_types_TransactionErrorReason_INSTRUCTION_ERROR );
  FD_TEST( h->model_results[0].transaction_err[0]==bam_types_TransactionErrorReason_COMMIT_CANCELLED );
}

static void
bam_model_run_scenario_partial_then_seq_switch( bam_model_harness_t * h ) {
  bam_model_batch_def_t b0 = bam_model_make_batch( 14U, 100UL, 1U, 3U );
  bam_model_batch_def_t b1 = bam_model_make_batch( 15U, 100UL, 1U, 2U );

  /* BAM decode currently canonicalizes batch_cnt to packets.len(), so a wire
     drop cannot represent "missing index within declared batch". Seed a
     partially buffered atomic batch directly, then force seq switch. */
  fd_memset( h->partial, 0, sizeof(*h->partial) );
  h->partial->active            = 1U;
  h->partial->seq_id            = b0.seq_id;
  h->partial->max_schedule_slot = b0.max_schedule_slot;
  h->partial->revert_on_error   = b0.revert_on_error;
  h->partial->txn_cnt           = b0.txn_cnt;
  for( uchar i=0U; i<2U; i++ ) {
    h->partial->seen[ i ] = 1U;
    h->partial->txn[ i ]  = (bam_model_stage_txn_t){
      .seq_id            = b0.seq_id,
      .max_schedule_slot = b0.max_schedule_slot,
      .batch_idx         = i,
      .batch_cnt         = b0.txn_cnt,
      .revert_on_error   = b0.revert_on_error,
      .spec              = b0.txn[ i ],
    };
  }

  bam_model_node_deliver_batches( h, &b1, 1UL, 0U );
  bam_model_apply_pipeline( h );

  fd_bam_bundle_result_t const * seq14 = NULL;
  fd_bam_bundle_result_t const * seq15 = NULL;
  for( ulong i=0UL; i<h->model_result_cnt; i++ ) {
    if( h->model_results[i].seq_id==14U ) seq14 = &h->model_results[i];
    if( h->model_results[i].seq_id==15U ) seq15 = &h->model_results[i];
  }

  FD_TEST( h->model_result_cnt==2UL );
  FD_TEST( !!seq14 );
  FD_TEST( !!seq15 );
  FD_TEST( seq14->transaction_err_count>0U );
}

static void
bam_model_run_scenario_non_atomic_success_fail( bam_model_harness_t * h ) {
  bam_model_batch_def_t ok = bam_model_make_batch( 16U, 100UL, 0U, 1U );
  bam_model_batch_def_t fail = bam_model_make_batch( 17U, 100UL, 0U, 1U );
  fail.txn[0].mode = BAM_MODEL_TXN_EXEC_FAIL;
  bam_model_node_deliver_batches( h, &ok, 1UL, 0U );
  bam_model_node_deliver_batches( h, &fail, 1UL, 0U );
  bam_model_apply_pipeline( h );

  FD_TEST( h->model_result_cnt==2UL );
  FD_TEST( h->model_results[0].execution_success==1U );
  FD_TEST( h->model_results[1].execution_success==1U ); /* fee-only commit */
  FD_TEST( h->model_results[1].transaction_err_count==1U );
}

static void
bam_model_run_scenario_non_atomic_sanitize_fail( bam_model_harness_t * h ) {
  bam_model_batch_def_t bad = bam_model_make_batch( 931U, 100UL, 0U, 1U );
  bad.txn[0].mode = BAM_MODEL_TXN_SANITIZE_FAIL;
  bam_model_node_deliver_batches( h, &bad, 1UL, 0U );
  bam_model_apply_pipeline( h );

  FD_TEST( h->model_result_cnt==1UL );
  FD_TEST( h->model_results[0].bundle_err==FD_BAM_BUNDLE_ERR_DESER );
  FD_TEST( h->model_results[0].deser_reason==bam_types_DeserializationErrorReason_SANITIZE_ERROR );
}

static void
bam_model_run_scenario_replay_same_seq( bam_model_harness_t * h ) {
  bam_model_batch_def_t b = bam_model_make_batch( 18U, 100UL, 1U, 2U );
  bam_model_node_deliver_batches( h, &b, 1UL, 0U );
  bam_model_apply_pipeline( h );
  ulong fee_after_first = h->fee_lamports_total;

  bam_model_node_deliver_batches( h, &b, 1UL, 0U );
  bam_model_apply_pipeline( h );

  FD_TEST( h->model_result_cnt==2UL );
  FD_TEST( h->fee_lamports_total==fee_after_first );
  FD_TEST( h->model_results[1].execution_success==0U );
}

static void
bam_model_run_scenario_replay_new_seq_after_missing_result( bam_model_harness_t * h ) {
  bam_model_batch_def_t b0 = bam_model_make_batch( 19U, 100UL, 1U, 2U );
  bam_model_batch_def_t b1 = b0;
  b1.seq_id = 20U;

  bam_model_node_deliver_batches( h, &b0, 1UL, 0U );
  bam_model_apply_pipeline( h );
  ulong fee_after_first = h->fee_lamports_total;

  /* Simulate bounded channel pressure: second result intent is dropped. */
  h->state->feedback_queue_depth = FD_BAM_MAX_PENDING_RESULTS;
  ulong drop_before = h->state->metrics.feedback_results_dropped_cnt;
  bam_model_node_deliver_batches( h, &b1, 1UL, 0U );
  bam_model_apply_pipeline( h );

  FD_TEST( h->state->metrics.feedback_results_dropped_cnt > drop_before );
  FD_TEST( h->fee_lamports_total==fee_after_first );
}

static void
bam_model_run_scenario_disconnect_reconnect( bam_model_harness_t * h ) {
  bam_model_batch_def_t b = bam_model_make_batch( 21U, 100UL, 1U, 2U );
  bam_model_node_deliver_batches( h, &b, 1UL, 0U );
  bam_model_apply_pipeline( h );

  FD_TEST( h->state->feedback_queue_depth>=1U );
  fd_bam_client_reset( h->state );
  FD_TEST( h->state->feedback_queue_depth>=1U );

  test_bam_env_mock_conn( h->env );
  bam_model_prepare_scheduler_stream( h->state );
  bam_model_keepalive_sync( h->state, g_clock );
  FD_TEST( h->state->bam_stream != NULL );
  FD_TEST( h->state->feedback_queue_depth>=1U );
}

static void
bam_model_run_scenario_limit_edges( bam_model_harness_t * h ) {
  h->slot_cu_limit = 30U;
  h->slot_microblock_limit = 2U;

  bam_model_batch_def_t ok = bam_model_make_batch( 22U, 100UL, 1U, 1U );
  ok.txn[0].requested_cu = 20U;
  ok.txn[0].actual_cu    = 20U;

  bam_model_batch_def_t overflow = bam_model_make_batch( 23U, 100UL, 1U, 2U );
  overflow.txn[0].requested_cu = 20U;
  overflow.txn[1].requested_cu = 20U;

  bam_model_node_deliver_batches( h, &ok, 1UL, 0U );
  bam_model_node_deliver_batches( h, &overflow, 1UL, 0U );
  bam_model_apply_pipeline( h );

  FD_TEST( h->model_result_cnt==2UL );
  FD_TEST( h->model_results[1].execution_success==0U );
}

static void
bam_model_run_scenario_fee_accounting( bam_model_harness_t * h ) {
  bam_model_batch_def_t a = bam_model_make_batch( 24U, 100UL, 0U, 1U );
  bam_model_batch_def_t b = bam_model_make_batch( 25U, 100UL, 1U, 2U );
  b.txn[0].mode = BAM_MODEL_TXN_EXEC_FAIL;
  bam_model_batch_def_t c = bam_model_make_batch( 26U, 100UL, 0U, 1U );

  bam_model_node_deliver_batches( h, &a, 1UL, 0U );
  bam_model_node_deliver_batches( h, &b, 1UL, 0U );
  bam_model_node_deliver_batches( h, &c, 1UL, 0U );
  bam_model_apply_pipeline( h );

  /* Only non-atomic commits should charge in this mix. */
  FD_TEST( h->charged_txn_cnt==2UL );
}

static void
bam_model_run_scenario_forced_saturation( bam_model_harness_t * h ) {
  fd_bam_bundle_result_t fill = {
    .seq_id=700000U,
    .slot=100UL,
    .bundle_txn_cnt=1U,
    .execution_success=0U,
    .scheduling_error=FD_BAM_SCHED_ERR_OUTSIDE_SLOT,
    .bundle_err=FD_BAM_BUNDLE_ERR_NONE,
  };
  ulong before_drop = h->state->metrics.feedback_results_dropped_cnt;
  h->state->feedback_queue_depth = FD_BAM_MAX_PENDING_RESULTS;
  bam_model_try_enqueue_result( h, &fill );
  FD_TEST( h->state->metrics.feedback_results_dropped_cnt > before_drop );
}

static void
bam_model_run_scenarios( fd_wksp_t * wksp ) {
  bam_model_harness_t h[1];

  bam_model_init( h, wksp );
  bam_model_run_scenario_auth_and_control( h );
  bam_model_fini( h );

  bam_model_init( h, wksp );
  bam_model_run_scenario_prevalidation_too_many_packets( h );
  bam_model_fini( h );

  bam_model_init( h, wksp );
  bam_model_run_scenario_prevalidation_inconsistent_revert( h );
  bam_model_assert_wire_matches_model( h );
  bam_model_fini( h );

  bam_model_init( h, wksp );
  bam_model_run_scenario_prevalidation_stale_slot( h );
  bam_model_assert_wire_matches_model( h );
  bam_model_fini( h );

  bam_model_init( h, wksp );
  bam_model_run_scenario_non_atomic_stale_slot( h );
  bam_model_assert_wire_matches_model( h );
  bam_model_fini( h );

  bam_model_init( h, wksp );
  bam_model_run_scenario_slot_source_fallback( h );
  bam_model_assert_wire_matches_model( h );
  bam_model_fini( h );

  bam_model_init( h, wksp );
  bam_model_run_scenario_non_leader_rejects_buffered_and_new_work( h );
  bam_model_assert_wire_matches_model( h );
  bam_model_fini( h );

  bam_model_init( h, wksp );
  bam_model_run_scenario_non_leader_rejects_work_without_slot_hint( h );
  bam_model_assert_wire_matches_model( h );
  bam_model_fini( h );

  bam_model_init( h, wksp );
  bam_model_run_scenario_bank_unavailable_timeout( h );
  bam_model_assert_wire_matches_model( h );
  bam_model_fini( h );

  bam_model_init( h, wksp );
  bam_model_run_scenario_atomic_success( h );
  bam_model_assert_trace_metadata( h );
  bam_model_assert_intents( h );
  bam_model_assert_wire_matches_model( h );
  bam_model_fini( h );

  bam_model_init( h, wksp );
  bam_model_run_scenario_atomic_verify_fail( h );
  bam_model_assert_wire_matches_model( h );
  bam_model_fini( h );

  bam_model_init( h, wksp );
  bam_model_run_scenario_atomic_lut_fail( h );
  bam_model_assert_wire_matches_model( h );
  bam_model_fini( h );

  bam_model_init( h, wksp );
  bam_model_run_scenario_atomic_exec_fail( h );
  bam_model_assert_wire_matches_model( h );
  bam_model_fini( h );

  bam_model_init( h, wksp );
  bam_model_run_scenario_partial_then_seq_switch( h );
  bam_model_assert_wire_matches_model( h );
  bam_model_fini( h );

  bam_model_init( h, wksp );
  bam_model_run_scenario_non_atomic_success_fail( h );
  bam_model_assert_wire_matches_model( h );
  bam_model_fini( h );

  bam_model_init( h, wksp );
  bam_model_run_scenario_non_atomic_sanitize_fail( h );
  bam_model_assert_wire_matches_model( h );
  bam_model_fini( h );

  bam_model_init( h, wksp );
  bam_model_run_scenario_replay_same_seq( h );
  bam_model_assert_wire_matches_model( h );
  bam_model_fini( h );

  bam_model_init( h, wksp );
  bam_model_run_scenario_replay_new_seq_after_missing_result( h );
  bam_model_fini( h );

  bam_model_init( h, wksp );
  bam_model_run_scenario_disconnect_reconnect( h );
  bam_model_fini( h );

  bam_model_init( h, wksp );
  bam_model_run_scenario_limit_edges( h );
  bam_model_assert_wire_matches_model( h );
  bam_model_fini( h );

  bam_model_init( h, wksp );
  bam_model_run_scenario_fee_accounting( h );
  bam_model_assert_wire_matches_model( h );
  bam_model_fini( h );

  bam_model_init( h, wksp );
  bam_model_run_scenario_forced_saturation( h );
  bam_model_fini( h );
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
                                            "bam-model-test",
                                            16UL );
  FD_TEST( wksp );

  bam_model_run_scenarios( wksp );

  fd_wksp_usage_t usage;
  FD_TEST( fd_wksp_usage( wksp, NULL, 0UL, &usage ) );
  FD_TEST( usage.free_cnt == usage.total_cnt );

  fd_wksp_delete_anonymous( wksp );
  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
