/* protobuf decoding for the BAM client. */

#include "fd_bam_tile_private.h"
#include "proto/bam_types.pb.h"
#include "../../ballet/base58/fd_base58.h"
#include "../../ballet/txn/fd_txn.h"
#include "../../ballet/nanopb/pb_decode.h"

#define FD_BAM_MAX_TXN_PER_ATOMIC_BATCH        5U
#define FD_BAM_MAX_ATOMIC_BATCHES_PER_PACKET   8U

FD_STATIC_ASSERT( FD_BAM_MAX_TXN_PER_ATOMIC_BATCH <= FD_PACK_MAX_TXN_PER_BUNDLE,
                  bam_decode_packet_limit_fits_staging );

typedef struct {
  bam_types_AtomicTxnBatch batch;
  fd_bam_batch_ctx_t       state;
} fd_bam_decoded_batch_t;

typedef struct {
  fd_bam_decoded_batch_t batches[ FD_BAM_MAX_ATOMIC_BATCHES_PER_PACKET ];
  uint                   batch_cnt;
  _Bool                  has_err_result;
  fd_bam_bundle_result_t err_result;
} fd_bam_decoded_multi_batch_t;

typedef enum {
  FD_BAM_V0_STAGED_NONE      = 0,
  FD_BAM_V0_STAGED_HEARTBEAT = 1,
  FD_BAM_V0_STAGED_MULTI     = 2
} fd_bam_v0_staged_kind_t;

typedef struct {
  fd_bam_v0_staged_kind_t kind;
  uint64_t                heartbeat_time_sent_microseconds;
  fd_bam_decoded_multi_batch_t multi;
} fd_bam_decoded_v0_t;

static void
fd_bam_dump_inbound_txn( uint                 seq_id,
                         ulong                max_schedule_slot,
                         uchar                batch_idx,
                         uchar                batch_cnt,
                         uchar                revert_on_error,
                         bam_types_Packet const * packet ) {
  uchar const * payload    = packet->data.bytes;

  uchar txn_buf[ FD_TXN_MAX_SZ ];
  ulong txn_sz = fd_txn_parse( payload, packet->data.size, txn_buf, NULL );
  if( FD_UNLIKELY( !txn_sz ) ) {
    FD_LOG_NOTICE(( "BAM rx txn (PARSE FAILED): seq_id=%u max_schedule_slot=%lu batch_idx=%u batch_cnt=%u revert_on_error=%u payload_sz=%u",
                    seq_id, max_schedule_slot, (uint)batch_idx, (uint)batch_cnt, (uint)revert_on_error, packet->data.size ));
    FD_LOG_HEXDUMP_NOTICE(( "BAM rx txn bytes (prefix)", payload, fd_ulong_min( packet->data.size, 1232 ) ));
    return;
  }

  fd_txn_t const * txn  = (fd_txn_t const *)txn_buf;
  fd_ed25519_sig_t const * sigs = fd_txn_get_signatures( txn, payload );
  fd_acct_addr_t const *  keys = fd_txn_get_acct_addrs( txn, payload );

  FD_LOG_NOTICE(( "BAM rx txn: seq_id=%u max_schedule_slot=%lu batch_idx=%u batch_cnt=%u revert_on_error=%u payload_sz=%u version=%u sig_cnt=%u acct_cnt=%u instr_cnt=%u addr_lut_cnt=%u addr_lut_acct_cnt=%u",
                  seq_id,
                  max_schedule_slot,
                  (uint)batch_idx,
                  (uint)batch_cnt,
                  (uint)revert_on_error,
                  packet->data.size,
                  (uint)txn->transaction_version,
                  (uint)txn->signature_cnt,
                  (uint)txn->acct_addr_cnt,
                  (uint)txn->instr_cnt,
                  (uint)txn->addr_table_lookup_cnt,
                  (uint)txn->addr_table_adtl_cnt ));

  if( FD_LIKELY( txn->acct_addr_cnt ) ) {
    char fee_payer[ FD_BASE58_ENCODED_32_SZ ];
    fd_base58_encode_32( keys[0].b, NULL, fee_payer );

    char recent_blockhash[ FD_BASE58_ENCODED_32_SZ ];
    fd_base58_encode_32( fd_txn_get_recent_blockhash( txn, payload ), NULL, recent_blockhash );

    FD_LOG_NOTICE(( "BAM rx txn meta: fee_payer=%s recent_blockhash=%s", fee_payer, recent_blockhash ));
  }

  for( ulong i=0; i<txn->signature_cnt; i++ ) {
    char sig_b58[ FD_BASE58_ENCODED_64_SZ ];
    fd_base58_encode_64( sigs[i], NULL, sig_b58 );
    FD_LOG_NOTICE(( "BAM rx sig: sig_index=%lu sig=%s", i, sig_b58 ));
  }

  if( txn->transaction_version==FD_TXN_V0 && txn->addr_table_lookup_cnt ) {
    fd_txn_acct_addr_lut_t const * luts = fd_txn_get_address_tables_const( txn );
    for( ulong i=0; i<txn->addr_table_lookup_cnt; i++ ) {
      fd_acct_addr_t const * addr = (fd_acct_addr_t const *)( (ulong)payload + (ulong)luts[i].addr_off );
      char addr_b58[ FD_BASE58_ENCODED_32_SZ ];
      fd_base58_encode_32( addr->b, NULL, addr_b58 );
      FD_LOG_NOTICE(( "BAM rx lut: lut_index=%lu addr=%s writable_cnt=%u readonly_cnt=%u",
                      i, addr_b58, (uint)luts[i].writable_cnt, (uint)luts[i].readonly_cnt ));

      uchar const * w_idx = (uchar const *)( (ulong)payload + (ulong)luts[i].writable_off );
      for( ulong j=0; j<luts[i].writable_cnt; j++ ) {
        FD_LOG_NOTICE(( "BAM rx lut writable: lut_index=%lu j=%lu idx=%u", i, j, (uint)w_idx[j] ));
      }

      uchar const * r_idx = (uchar const *)( (ulong)payload + (ulong)luts[i].readonly_off );
      for( ulong j=0; j<luts[i].readonly_cnt; j++ ) {
        FD_LOG_NOTICE(( "BAM rx lut readonly: lut_index=%lu j=%lu idx=%u", i, j, (uint)r_idx[j] ));
      }
    }
  }

  for( ulong ix_idx=0UL; ix_idx<(ulong)txn->instr_cnt; ix_idx++ ) {
    fd_txn_instr_t const * instr = &txn->instr[ ix_idx ];

    char prog_b58[ FD_BASE58_ENCODED_32_SZ ];
    char const * prog_str = "<lookup>";
    if( FD_LIKELY( instr->program_id < txn->acct_addr_cnt ) ) {
      fd_base58_encode_32( keys[ instr->program_id ].b, NULL, prog_b58 );
      prog_str = prog_b58;
    }

    FD_LOG_NOTICE(( "BAM rx ix: ix_index=%lu program_id_index=%u program=%s acct_cnt=%u data_sz=%u",
                    ix_idx,
                    (uint)instr->program_id,
                    prog_str,
                    (uint)instr->acct_cnt,
                    (uint)instr->data_sz ));

    uchar const * acct_idx = fd_txn_get_instr_accts( instr, payload );
    for( ulong j=0; j<instr->acct_cnt; j++ ) {
      uchar acct = acct_idx[ j ];
      char  acct_b58[ FD_BASE58_ENCODED_32_SZ ];
      char const * acct_str = "<lookup>";
      if( FD_LIKELY( acct < txn->acct_addr_cnt ) ) {
        fd_base58_encode_32( keys[ acct ].b, NULL, acct_b58 );
        acct_str = acct_b58;
      }
      FD_LOG_NOTICE(( "BAM rx ix acct: ix_index=%lu j=%lu acct_index=%u acct=%s",
                      ix_idx, j, (uint)acct, acct_str ));
    }

    uchar const * data = fd_txn_get_instr_data( instr, payload );
    FD_LOG_HEXDUMP_NOTICE(( "BAM rx ix data (prefix)", data, fd_ulong_min( (ulong)instr->data_sz, 64UL ) ));
  }
}

/* Collects a single Packet from the protobuf stream. Returns true while the
   packet parsed and passed basic validation; returns false to abort the
   surrounding AtomicTxnBatch decode and surface the bundle error. */
static bool
fd_bam_collect_packet( pb_istream_t *         stream,
                       pb_field_t const *     field,
                       void **                arg ) {
  (void)field;
  fd_bam_batch_ctx_t * state = *arg;
  if( FD_UNLIKELY( state->packet_cnt >= FD_BAM_MAX_TXN_PER_ATOMIC_BATCH ) ) {
    FD_LOG_WARNING(( "Received AtomicTxnBatch exceeding max bundle size, already have %u txns", state->packet_cnt ));
    state->has_deser_err   = true;
    state->deser_reason    = bam_types_DeserializationErrorReason_SANITIZE_ERROR;
    state->deser_index     = 0U;
    return false;
  }

  bam_types_Packet packet = bam_types_Packet_init_default;
  if( FD_UNLIKELY( !pb_decode( stream, &bam_types_Packet_msg, &packet ) ) ) {
    state->has_deser_err   = true;
    state->deser_reason    = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
    state->deser_index     = state->packet_cnt;
    return false;
  }

  uchar packet_revert_on_error = 0U;
  if( packet.has_meta && packet.meta.has_flags ) {
    if( FD_UNLIKELY( packet.meta.flags.simple_vote_tx ) ) {
      state->has_deser_err = true;
      state->deser_reason = bam_types_DeserializationErrorReason_VOTE_TRANSACTION_FAILURE;
      state->deser_index  = state->packet_cnt;
      return false;
    }
    packet_revert_on_error = packet.meta.flags.revert_on_error;
  }

  if( FD_UNLIKELY( state->packet_cnt && state->revert_on_error != packet_revert_on_error ) ) {
    FD_LOG_WARNING(( "AtomicTxnBatch contains mixed revert_on_error flags" ));
    state->has_deser_err = true;
    state->deser_reason  = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
    state->deser_index   = 0U;
    return false;
  }
  state->revert_on_error = packet_revert_on_error;

  ulong declared_sz = fd_ulong_max( packet.data.size, packet.meta.size );
  if( FD_UNLIKELY( declared_sz > FD_TXN_MTU ) ) {
    state->ctx->metrics.packet_drop_cnt++;
    FD_MCNT_INC( BAM, PACKETS_DROPPED, 1UL );
    FD_LOG_WARNING(( "Received AtomicTxnBatch packet exceeding MTU (%lu>%lu); dropping batch",
                     declared_sz, FD_TXN_MTU ));
    state->has_deser_err = true;
    state->deser_reason = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
    state->deser_index  = state->packet_cnt;
    return false;
  }

  state->packets[ state->packet_cnt ] = packet;
  state->packet_cnt++;
  return true;
}

static _Bool
fd_bam_validate_batch( fd_bam_tile_t *                  ctx,
                       fd_bam_batch_ctx_t const *       state,
                       bam_types_AtomicTxnBatch const * batch ) {
  if( FD_UNLIKELY( state->has_deser_err ) ) {
    ctx->metrics.ingress_reject_deser_cnt++;
    fd_bam_bundle_result_t res = {
      .seq_id            = batch->seq_id,
      .slot              = batch->max_schedule_slot,
      .bundle_txn_cnt    = state->packet_cnt,
      .execution_success = 0,
      .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
      .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
      .deser_reason      = state->deser_reason,
      .deser_index       = state->deser_index
    };
    fd_bam_enqueue_result( ctx, &res );
    ctx->bundle_max_schedule_slot = FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT;
    return 0;
  }

  if( FD_UNLIKELY( state->packet_cnt == 0U ) ) {
    ctx->metrics.ingress_reject_empty_cnt++;
    fd_bam_bundle_result_t res = {
      .seq_id            = batch->seq_id,
      .slot              = batch->max_schedule_slot,
      .bundle_txn_cnt    = state->packet_cnt,
      .execution_success = 0,
      .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
      .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
      .deser_reason      = bam_types_DeserializationErrorReason_EMPTY,
      .deser_index       = 0
    };
    fd_bam_enqueue_result( ctx, &res );
    ctx->bundle_max_schedule_slot = FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT;
    return 0;
  }

  if( FD_UNLIKELY( (!state->revert_on_error) && state->packet_cnt>1U ) ) {
    ctx->metrics.ingress_reject_non_revert_multi_packet_cnt++;
    /* For now, revert_on_error=0 batches are assumed to contain exactly one
       packet so we can return one result per seq_id without BAM-node changes. */
    fd_bam_bundle_result_t res = {
      .seq_id            = batch->seq_id,
      .slot              = batch->max_schedule_slot,
      .bundle_txn_cnt    = state->packet_cnt,
      .execution_success = 0,
      .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
      .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
      .deser_reason      = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE,
      .deser_index       = 0
    };
    fd_bam_enqueue_result( ctx, &res );
    ctx->bundle_max_schedule_slot = FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT;
    return 0;
  }

  if( FD_UNLIKELY( state->revert_on_error && !ctx->builder_info_valid_until ) ) {
    ctx->metrics.ingress_reject_missing_builder_info_cnt++;
    fd_bam_bundle_result_t res = {
      .seq_id                 = batch->seq_id,
      .slot                   = batch->max_schedule_slot,
      .bundle_txn_cnt         = state->packet_cnt,
      .execution_success      = 0,
      .scheduling_error       = FD_BAM_SCHED_ERR_NONE,
      .bundle_err             = FD_BAM_BUNDLE_ERR_GENERIC_INVALID,
      .generic_invalid_reason = FD_BAM_ERR_GENERIC_INVALID_BUILDER_INFO_UNAVAILABLE,
      .generic_invalid_index  = 0,
    };
    fd_bam_enqueue_result( ctx, &res );
    ctx->metrics.missing_builder_info_fail_cnt++;
    return 0;
  }

  return 1;
}

void
fd_bam_publish_batch( fd_bam_tile_t *            ctx,
                      fd_bam_batch_ctx_t *       state,
                      bam_types_AtomicTxnBatch const * batch ) {
  if( FD_UNLIKELY( !!ctx->dump_txns ) ) {
    FD_LOG_NOTICE(( "BAM rx bundle begin >>> seq_id=%u max_schedule_slot=%lu packet_cnt=%u revert_on_error=%u has_deser_err=%u deser_reason=%u deser_index=%u",
                    batch->seq_id,
                    batch->max_schedule_slot,
                    (uint)state->packet_cnt,
                    (uint)state->revert_on_error,
                    (uint)state->has_deser_err,
                    (uint)state->deser_reason,
                    (uint)state->deser_index ));
    for( uchar i=0U; i<state->packet_cnt; i++ ) {
      fd_bam_dump_inbound_txn( batch->seq_id,
                               batch->max_schedule_slot,
                               i,
                               state->packet_cnt,
                               state->revert_on_error,
                               &state->packets[ i ] );
    }
    FD_LOG_NOTICE(( "BAM rx bundle end <<< seq_id=%u max_schedule_slot=%lu packet_cnt=%u",
                    batch->seq_id,
                    batch->max_schedule_slot,
                    (uint)state->packet_cnt ));
  }

  if( state->revert_on_error ) {
    ctx->bundle_seq                = batch->seq_id;
    ctx->bundle_txn_cnt            = state->packet_cnt;
    ctx->bundle_max_schedule_slot  = batch->max_schedule_slot;

    for( uchar i=0; i<state->packet_cnt; i++ ) {
      fd_bam_tile_publish_bundle_txn( ctx,
                                      state->packets[i].data.bytes,
                                      (ushort)state->packets[i].data.size,
                                      state->packet_cnt,
                                      i,
                                      0 );
    }
    ctx->metrics.bundle_received_cnt++;
  } else {
    for( uchar i=0; i<state->packet_cnt; i++ ) {
      fd_bam_tile_publish_txn( ctx,
                               state->packets[i].data.bytes,
                               state->packets[i].data.size,
                               batch->max_schedule_slot,
                               batch->seq_id,
                               i,
                               state->packet_cnt,
                               state->revert_on_error,
                               0 );
    }
  }
  ctx->bundle_max_schedule_slot = FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT;
}

/* Decodes one bam_types.AtomicTxnBatch message into staged state only.
   This function never publishes transactions. Returns 1 when the batch was
   fully consumed. Returns 0 when protobuf decoding failed after
   logging/enqueuing the appropriate bundle error. */
static _Bool
fd_bam_decode_batch( fd_bam_tile_t *          ctx,
                     pb_istream_t *           stream,
                     fd_bam_decoded_batch_t * decoded ) {
  fd_memset( decoded, 0, sizeof(fd_bam_decoded_batch_t) );
  decoded->state.ctx          = ctx;
  decoded->state.deser_reason = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
  decoded->state.deser_index  = 0U;
  decoded->batch = (bam_types_AtomicTxnBatch)bam_types_AtomicTxnBatch_init_default;
  decoded->batch.packets = (pb_callback_t){
    .funcs.decode = fd_bam_collect_packet,
    .arg          = &decoded->state
  };

  if( FD_UNLIKELY( !pb_decode( stream, &bam_types_AtomicTxnBatch_msg, &decoded->batch ) ) ) {
    uchar deser_reason = decoded->state.has_deser_err
                       ? decoded->state.deser_reason
                       : (uchar)bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
    uchar deser_index  = decoded->state.has_deser_err
                       ? decoded->state.deser_index
                       : decoded->state.packet_cnt;
    FD_MCNT_INC( BAM, ERRORS_PROTOBUF, 1UL );
    /* Decode failure still produces a not-committed result, so count this as
       a BAM ingress reject in addition to the protobuf error bucket. */
    ctx->metrics.ingress_batch_reject_cnt++;
    ctx->metrics.ingress_reject_deser_cnt++;
    char const * err = PB_GET_ERROR( stream );
    FD_LOG_WARNING(( "Protobuf decode of (bam_types.AtomicTxnBatch) failed (%s)", err ));
    fd_bam_bundle_result_t res = {
      .seq_id            = decoded->batch.seq_id,
      .slot              = decoded->batch.max_schedule_slot,
      .bundle_txn_cnt    = decoded->state.packet_cnt,
      .execution_success = 0,
      .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
      .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
      .deser_reason      = deser_reason,
      .deser_index       = deser_index
    };
    fd_bam_enqueue_result( ctx, &res );
    return 0;
  }

  return 1;
}

/* Decodes a bam_types.MultipleAtomicTxnBatch wrapper into staged state.
   Returns 1 once the message bytes were consumed, including semantic error
   cases that stage a not-committed result. Returns 0 on protobuf failures.
   No transaction publish occurs in this stage. */
static int
fd_bam_decode_multiple_atomic_txn_batch( fd_bam_tile_t * ctx,
                                         pb_istream_t *   stream,
                                         fd_bam_decoded_multi_batch_t * decoded_multi ) {
  fd_memset( decoded_multi, 0, sizeof(fd_bam_decoded_multi_batch_t) );

  uint32_t       tag;
  pb_wire_type_t wire_type;
  bool           eof = false;
  uint           seen_batch_count = 0U;

  while( pb_decode_tag( stream, &wire_type, &tag, &eof ) ) {
    if( FD_UNLIKELY( tag != bam_types_MultipleAtomicTxnBatch_batches_tag ) ) {
      if( FD_UNLIKELY( !pb_skip_field( stream, wire_type ) ) ) return 0;
      continue;
    }
    if( FD_UNLIKELY( wire_type != PB_WT_STRING ) ) {
      if( FD_UNLIKELY( !pb_skip_field( stream, wire_type ) ) ) return 0;
      continue;
    }

    pb_istream_t substream;
    if( FD_UNLIKELY( !pb_make_string_substream( stream, &substream ) ) ) return 0;

    if( FD_UNLIKELY( seen_batch_count >= FD_BAM_MAX_ATOMIC_BATCHES_PER_PACKET ) ) {
      fd_bam_decoded_batch_t overflow_batch;
      _Bool const ok = fd_bam_decode_batch( ctx, &substream, &overflow_batch );
      pb_close_string_substream( stream, &substream );
      if( FD_UNLIKELY( !ok ) ) return 0;

      FD_LOG_WARNING(( "MultipleAtomicTxnBatch exceeded max batch count (%u>%u)",
                       seen_batch_count + 1U,
                       FD_BAM_MAX_ATOMIC_BATCHES_PER_PACKET ));
      ctx->metrics.ingress_multi_overflow_msg_cnt++;
      ctx->metrics.ingress_batch_reject_cnt++;
      ctx->metrics.ingress_reject_deser_cnt++;
      decoded_multi->batch_cnt = seen_batch_count;
      decoded_multi->has_err_result = 1;
      decoded_multi->err_result = (fd_bam_bundle_result_t){
        .seq_id            = overflow_batch.batch.seq_id,
        .slot              = overflow_batch.batch.max_schedule_slot,
        .bundle_txn_cnt    = overflow_batch.state.packet_cnt,
        .execution_success = 0,
        .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
        .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
        .deser_reason      = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE,
        .deser_index       = 0
      };
      return 1;
    }

    _Bool const ok = fd_bam_decode_batch( ctx, &substream, &decoded_multi->batches[ seen_batch_count ] );
    pb_close_string_substream( stream, &substream );
    if( FD_UNLIKELY( !ok ) ) return 0;
    seen_batch_count++;
  }

  if( FD_UNLIKELY( !eof ) ) return 0;
  decoded_multi->batch_cnt = seen_batch_count;

  if( FD_UNLIKELY( seen_batch_count == 0U ) ) {
    FD_LOG_WARNING(( "MultipleAtomicTxnBatch contained no AtomicTxnBatch entries" ));
    ctx->metrics.ingress_multi_empty_msg_cnt++;
    ctx->metrics.ingress_batch_reject_cnt++;
    ctx->metrics.ingress_reject_empty_cnt++;
    bam_types_AtomicTxnBatch batch = bam_types_AtomicTxnBatch_init_default;
    decoded_multi->has_err_result = 1;
    decoded_multi->err_result = (fd_bam_bundle_result_t){
      .seq_id            = batch.seq_id,
      .slot              = batch.max_schedule_slot,
      .bundle_txn_cnt    = 0,
      .execution_success = 0,
      .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
      .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
      .deser_reason      = bam_types_DeserializationErrorReason_EMPTY,
      .deser_index       = 0
    };
  }
  return 1;
}

static void
fd_bam_commit_multiple_atomic_txn_batch( fd_bam_tile_t *                      ctx,
                                         fd_bam_decoded_multi_batch_t *       decoded_multi ) {
  ctx->metrics.ingress_batch_commit_attempt_cnt += decoded_multi->batch_cnt;

  for( uint i=0U; i<decoded_multi->batch_cnt; i++ ) {
    if( FD_UNLIKELY( !fd_bam_validate_batch( ctx,
                                             &decoded_multi->batches[ i ].state,
                                             &decoded_multi->batches[ i ].batch ) ) ) {
      ctx->metrics.ingress_batch_reject_cnt++;
      continue;
    }
    fd_bam_publish_batch( ctx,
                          &decoded_multi->batches[ i ].state,
                          &decoded_multi->batches[ i ].batch );
    ctx->metrics.ingress_batch_publish_cnt++;
  }

  if( FD_UNLIKELY( decoded_multi->has_err_result ) ) {
    /* Reject counters are recorded at the decode site where err_result was staged
       (overflow/empty wrapper) so the reason taxonomy remains accurate. */
    fd_bam_enqueue_result( ctx, &decoded_multi->err_result );
    ctx->bundle_max_schedule_slot = FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT;
  }
}

/* Decodes the v0 scheduler response payload (heartbeats and nested batches)
   into staged state. Returns 1 once the message was consumed; returns 0 on
   protobuf failures so the caller can reject the message before commit. */
static int
fd_bam_decode_scheduler_response_v0( fd_bam_tile_t * ctx,
                                     pb_istream_t *   stream,
                                     fd_bam_decoded_v0_t * decoded_v0 ) {
  fd_memset( decoded_v0, 0, sizeof(fd_bam_decoded_v0_t) );

  uint32_t      tag;
  pb_wire_type_t wire_type;
  bool          eof = false;
  while( pb_decode_tag( stream, &wire_type, &tag, &eof ) ) {
    switch( tag ) {
    case bam_api_SchedulerResponseV0_heart_beat_tag: {
      if( FD_UNLIKELY( wire_type != PB_WT_STRING ) ) {
        if( FD_UNLIKELY( !pb_skip_field( stream, wire_type ) ) ) return 0;
        break;
      }
      pb_istream_t hb_stream;
      if( FD_UNLIKELY( !pb_make_string_substream( stream, &hb_stream ) ) ) return 0;
      bam_types_BuilderHeartBeat hb = bam_types_BuilderHeartBeat_init_default;
      int ok = pb_decode( &hb_stream, &bam_types_BuilderHeartBeat_msg, &hb );
      pb_close_string_substream( stream, &hb_stream );
      if( FD_UNLIKELY( !ok ) ) {
        FD_LOG_WARNING(( "BuilderHeartBeat decode failed: %s", PB_GET_ERROR( &hb_stream ) ));
        return 0;
      }
      ctx->metrics.ingress_v0_heartbeat_msg_cnt++;
      decoded_v0->kind = FD_BAM_V0_STAGED_HEARTBEAT;
      decoded_v0->heartbeat_time_sent_microseconds = hb.time_sent_microseconds;
      break;
    }
    case bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag: {
      if( FD_UNLIKELY( wire_type != PB_WT_STRING ) ) {
        if( FD_UNLIKELY( !pb_skip_field( stream, wire_type ) ) ) return 0;
        break;
      }
      pb_istream_t substream;
      if( FD_UNLIKELY( !pb_make_string_substream( stream, &substream ) ) ) return 0;
      fd_bam_decoded_multi_batch_t decoded_multi;
      int ok = fd_bam_decode_multiple_atomic_txn_batch( ctx, &substream, &decoded_multi );
      pb_close_string_substream( stream, &substream );
      if( FD_UNLIKELY( !ok ) ) return 0;
      ctx->metrics.ingress_v0_multi_msg_cnt++;
      ctx->metrics.ingress_multi_batch_total_cnt += decoded_multi.batch_cnt;
      decoded_v0->kind  = FD_BAM_V0_STAGED_MULTI;
      decoded_v0->multi = decoded_multi;
      break;
    }
    default:
      if( FD_UNLIKELY( !pb_skip_field( stream, wire_type ) ) ) return 0;
      break;
    }
  }
  if( FD_UNLIKELY( !eof ) ) return 0;
  return 1;
}

/* Decodes bam_api.SchedulerResponse (versioned envelope) using a two-phase
   stage/commit flow. On decode failure or unsupported version it bumps metrics
   and may request a reset; otherwise it commits the staged versioned payload. */
void
fd_bam_handle_scheduler_response( fd_bam_tile_t * ctx,
                                   void const *      data,
                                   ulong             data_sz ) {
  pb_istream_t istream = pb_istream_from_buffer( data, data_sz );

  uint32_t      tag;
  pb_wire_type_t wire_type;
  bool          eof         = false;
  uint32_t      version_tag = 0U;
  int           seen_v0     = 0;
  fd_bam_decoded_v0_t decoded_v0 = {0};

  while( pb_decode_tag( &istream, &wire_type, &tag, &eof ) ) {
    version_tag = tag;
    if( FD_UNLIKELY( tag != bam_api_SchedulerResponse_v0_tag ) ) {
      if( FD_UNLIKELY( !pb_skip_field( &istream, wire_type ) ) ) goto fail;
      continue;
    }

    if( FD_UNLIKELY( wire_type != PB_WT_STRING ) ) {
      if( FD_UNLIKELY( !pb_skip_field( &istream, wire_type ) ) ) goto fail;
      continue;
    }

    pb_istream_t substream;
    if( FD_UNLIKELY( !pb_make_string_substream( &istream, &substream ) ) ) goto fail;
    fd_bam_decoded_v0_t staged_v0;
    int ok = fd_bam_decode_scheduler_response_v0( ctx, &substream, &staged_v0 );
    pb_close_string_substream( &istream, &substream );
    if( FD_UNLIKELY( !ok ) ) goto fail;
    decoded_v0 = staged_v0;
    seen_v0 = 1;
  }

  if( FD_UNLIKELY( !eof ) ) goto fail;
  if( FD_UNLIKELY( !seen_v0 ) ) {
    if( version_tag && version_tag != bam_api_SchedulerResponse_v0_tag ) {
      FD_LOG_WARNING(( "Unsupported SchedulerResponse version (tag=%u); scheduling reset", version_tag ));
      ctx->metrics.transport_fail_cnt++;
      ctx->defer_reset = 1;
    } else {
      ctx->metrics.decode_fail_cnt++;
      FD_LOG_WARNING(( "Protobuf decode of (bam_api.SchedulerResponse) missing version" ));
    }
    return;
  }

  switch( decoded_v0.kind ) {
  case FD_BAM_V0_STAGED_HEARTBEAT:
    ctx->bam_last_builder_heartbeat_ns = fd_bam_now();
    fd_bam_client_sample_heartbeat_delay( ctx, decoded_v0.heartbeat_time_sent_microseconds );
    ctx->metrics.heartbeat_recv_cnt++;
    break;
  case FD_BAM_V0_STAGED_MULTI:
    fd_bam_commit_multiple_atomic_txn_batch( ctx, &decoded_v0.multi );
    ctx->bam_last_builder_heartbeat_ns = fd_bam_now();
    break;
  default:
    break;
  }
  return;

fail:
  ctx->metrics.decode_fail_cnt++;
  FD_LOG_WARNING(( "Protobuf decode of (bam_api.SchedulerResponse) failed (%s)", PB_GET_ERROR( &istream ) ));
}
