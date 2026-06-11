#define _GNU_SOURCE

#if !FD_HAS_HOSTED
#error "This target requires FD_HAS_HOSTED"
#endif

#define FD_BAM_MODEL_NO_MAIN 1
#include "test_bam_model.c"

FD_IMPORT_BINARY( bam_fuzz_txn1, "src/ballet/txn/fixtures/transaction1.bin" );
FD_IMPORT_BINARY( bam_fuzz_txn2, "src/ballet/txn/fixtures/transaction2.bin" );
FD_IMPORT_BINARY( bam_fuzz_txn3, "src/ballet/txn/fixtures/transaction3.bin" );
FD_IMPORT_BINARY( bam_fuzz_txn4, "src/ballet/txn/fixtures/transaction4.bin" );
FD_IMPORT_BINARY( bam_fuzz_txn6, "src/ballet/txn/fixtures/transaction6.bin" );

/* Stateful BAM end-to-end fuzzer.

   The byte grammar is intentionally stable with the old target, but each
   event now drives the BAM tile/model harness:
   SEND_BATCH, REPLAY, DUP_SEQ, NEW_SEQ_SAME_PAYLOAD,
   ADVANCE_SLOT, LEADER_ON/OFF, DISCONNECT, RECONNECT,
   FILL_QUEUE, DRAIN_QUEUE. */

typedef enum {
  BAM_EVT_SEND_BATCH           = 0,
  BAM_EVT_REPLAY               = 1,
  BAM_EVT_DUP_SEQ              = 2,
  BAM_EVT_NEW_SEQ_SAME_PAYLOAD = 3,
  BAM_EVT_ADVANCE_SLOT         = 4,
  BAM_EVT_LEADER_OFF           = 5,
  BAM_EVT_LEADER_ON            = 6,
  BAM_EVT_DISCONNECT           = 7,
  BAM_EVT_RECONNECT            = 8,
  BAM_EVT_FILL_QUEUE           = 9,
  BAM_EVT_DRAIN_QUEUE          = 10,
  BAM_EVT_KIND_CNT
} bam_evt_kind_t;

#define BAM_FUZZ_MAX_EVENTS 128UL
#define BAM_FUZZ_RAW_REJECT_SUBMODE_CNT 6U
#define BAM_FUZZ_MAX_RAW_BATCHES (FD_BAM_MAX_ATOMIC_BATCHES_PER_PACKET + 1U)

typedef struct {
  uint next_seq;
  uint direct_result_seq;

  bam_model_batch_def_t last_batch;
  uchar                 has_last_batch;
} bam_fuzz_state_t;

static fd_wksp_t * g_bam_fuzz_wksp;

static void
bam_fuzz_ensure_scheduler_stream( bam_model_harness_t * h ) {
  if( FD_LIKELY( h->state->bam_stream && h->state->bam_stream_live ) ) return;

  test_bam_env_mock_conn( h->env );
  bam_model_prepare_scheduler_stream( h->state );
  g_clock += (long)1000000L;
  bam_model_keepalive_sync( h->state, g_clock );
  bam_model_node_deliver_heartbeat( h );
}

static void
bam_fuzz_assert_queue_state( fd_bam_tile_t const * state ) {
  FD_TEST( state->feedback_queue_depth <= FD_BAM_MAX_PENDING_RESULTS );
  FD_TEST( state->bam_results_head     <  FD_BAM_MAX_PENDING_RESULTS );
  FD_TEST( state->bam_results_tail     <  FD_BAM_MAX_PENDING_RESULTS );

  ushort expected_tail = (ushort)(( (uint)state->bam_results_head +
                                    (uint)state->feedback_queue_depth ) %
                                  FD_BAM_MAX_PENDING_RESULTS);
  FD_TEST( state->bam_results_tail==expected_tail );
}

static void
bam_fuzz_assert_invariants( bam_model_harness_t const * h ) {
  bam_fuzz_assert_queue_state( h->state );

  ulong committed_work_cnt = 0UL;
  for( ulong i=0UL; i<sizeof(h->work_committed); i++ ) committed_work_cnt += !!h->work_committed[ i ];
  FD_TEST( h->charged_txn_cnt == committed_work_cnt );
  FD_TEST( h->slot_cu_used <= h->slot_cu_limit );
  FD_TEST( h->slot_microblock_used <= h->slot_microblock_limit );
  FD_TEST( h->drop_cnt == h->state->metrics.feedback_results_dropped_cnt );
}

static bam_model_txn_spec_t
bam_fuzz_make_txn_spec( uint  seq_id,
                        uchar idx,
                        uchar a,
                        uchar b,
                        uchar c ) {
  uint selector = ( (uint)a + 17U*(uint)idx + ( (uint)b>>1 ) + (uint)c ) % 10U;

  bam_model_txn_spec_t spec = {
    .mode         = BAM_MODEL_TXN_OK,
    .fee_lamports = (uchar)( 1U + ((uint)a + (uint)c + (uint)idx) % 100U ),
    .requested_cu = (uchar)( 1U + ((uint)b + (uint)(3U*idx)) % 80U ),
    .actual_cu    = 0U,
    .work_id      = (uchar)( seq_id + 31U*(uint)idx + (uint)c ),
  };
  uint requested_cu = (uint)spec.requested_cu;
  uint cu_delta     = (uint)a & 7U;
  spec.actual_cu = (uchar)( requested_cu > cu_delta ? requested_cu - cu_delta : 1U );

  switch( selector ) {
    case 0U:
    case 1U:
    case 2U:
      spec.mode = BAM_MODEL_TXN_OK;
      break;
    case 3U:
      spec.mode = BAM_MODEL_TXN_VERIFY_SIG_FAIL;
      break;
    case 4U:
      spec.mode = BAM_MODEL_TXN_SANITIZE_FAIL;
      break;
    case 5U:
      spec.mode = BAM_MODEL_TXN_LUT_FAIL;
      break;
    case 6U:
      spec.mode = BAM_MODEL_TXN_BANK_FRONT_RUN_FAIL;
      break;
    case 7U:
      spec.mode = BAM_MODEL_TXN_LOCK_FAIL;
      break;
    case 8U:
      spec.mode = BAM_MODEL_TXN_EXEC_FAIL;
      break;
    case 9U:
      spec.mode = BAM_MODEL_TXN_POH_TIMEOUT;
      break;
  }

  if( FD_UNLIKELY( b & 0x40U ) ) spec.actual_cu = (uchar)( spec.requested_cu + 1U );
  return spec;
}

static bam_model_batch_def_t
bam_fuzz_make_batch( bam_model_harness_t const * h,
                     bam_fuzz_state_t *          f,
                     uchar                       a,
                     uchar                       b,
                     uchar                       c ) {
  uint seq_id = f->next_seq++;
  ulong current_slot = bam_model_current_slot( h );

  _Bool revert_on_error = !!( b & 1U );
  uchar txn_cnt = (uchar)( 1U + ((uint)a % FD_BAM_MAX_TXN_PER_ATOMIC_BATCH) );
  if( FD_UNLIKELY( !revert_on_error ) ) txn_cnt = 1U;

  ulong max_schedule_slot;
  if( FD_UNLIKELY( b & 0x20U ) ) {
    max_schedule_slot = 0UL;
  } else if( FD_UNLIKELY( b & 0x10U ) ) {
    max_schedule_slot = current_slot ? current_slot-1UL : 0UL;
  } else {
    max_schedule_slot = current_slot + (ulong)(( b>>6 ) & 3U);
  }

  bam_model_batch_def_t def;
  fd_memset( &def, 0, sizeof(def) );
  def.seq_id            = seq_id;
  def.max_schedule_slot = max_schedule_slot;
  def.revert_on_error   = revert_on_error;
  def.txn_cnt           = txn_cnt;
  for( uchar i=0U; i<txn_cnt; i++ ) def.txn[ i ] = bam_fuzz_make_txn_spec( seq_id, i, a, b, c );
  return def;
}

static void
bam_fuzz_deliver_batches( bam_model_harness_t *          h,
                          bam_model_batch_def_t const * defs,
                          ulong                        def_cnt ) {
  if( FD_UNLIKELY( !h->state->bam_stream || !h->state->bam_stream_live ) ) return;

  bam_model_node_deliver_batches( h, defs, def_cnt, 0U );
  bam_model_apply_pipeline( h );
}

static void
bam_fuzz_init_real_txn_packet( bam_types_Packet * pkt,
                               uint               payload_seed,
                               _Bool              revert_on_error ) {
  uchar const * const payloads[ 5 ] = {
    bam_fuzz_txn1,
    bam_fuzz_txn2,
    bam_fuzz_txn3,
    bam_fuzz_txn4,
    bam_fuzz_txn6,
  };
  ulong const sizes[ 5 ] = {
    bam_fuzz_txn1_sz,
    bam_fuzz_txn2_sz,
    bam_fuzz_txn3_sz,
    bam_fuzz_txn4_sz,
    bam_fuzz_txn6_sz,
  };
  ulong idx = (ulong)( payload_seed % 5U );
  uchar const * payload    = payloads[ idx ];
  ulong         payload_sz = sizes[ idx ];
  FD_TEST( payload_sz <= sizeof(pkt->data.bytes) );

  uchar txn_buf[ FD_TXN_MAX_SZ ];
  FD_TEST( fd_txn_parse( payload, payload_sz, txn_buf, NULL ) );

  *pkt = (bam_types_Packet)bam_types_Packet_init_default;
  pkt->data.size = (pb_size_t)payload_sz;
  fd_memcpy( pkt->data.bytes, payload, payload_sz );
  pkt->has_meta = 1U;
  pkt->meta.size = payload_sz;
  pkt->meta.has_flags = 1U;
  pkt->meta.flags.revert_on_error = revert_on_error;
}

static void
bam_fuzz_deliver_raw_batches( bam_model_harness_t * h,
                              uint const *          seq_ids,
                              bam_types_Packet *    packets,
                              size_t                packet_stride,
                              size_t const *        packet_cnts,
                              size_t                batch_cnt ) {
  test_bam_packet_encode_ctx_t packet_ctx[ BAM_FUZZ_MAX_RAW_BATCHES ];
  bam_types_AtomicTxnBatch     batches   [ BAM_FUZZ_MAX_RAW_BATCHES ];

  for( size_t i=0UL; i<batch_cnt; i++ ) {
    packet_ctx[ i ] = (test_bam_packet_encode_ctx_t) {
      .packets    = packets ? packets + i*packet_stride : NULL,
      .packet_cnt = packet_cnts[ i ],
    };

    batches[ i ] = (bam_types_AtomicTxnBatch)bam_types_AtomicTxnBatch_init_default;
    batches[ i ].seq_id            = seq_ids[ i ];
    batches[ i ].max_schedule_slot = bam_model_current_slot( h );
    batches[ i ].packets.funcs.encode = test_bam_encode_packets_cb;
    batches[ i ].packets.arg          = &packet_ctx[ i ];
  }

  uchar pb[ 16384 ];
  size_t pb_sz = bam_model_encode_scheduler_batches( batches, batch_cnt, pb, sizeof(pb) );
  bam_model_node_deliver_scheduler_response( h, pb, pb_sz );
  bam_model_apply_pipeline( h );
}

static void
bam_fuzz_deliver_raw_packets( bam_model_harness_t * h,
                              uint                  seq_id,
                              bam_types_Packet *    packets,
                              size_t                packet_cnt ) {
  uint   seq_ids[1]    = { seq_id };
  size_t packet_cnts[1] = { packet_cnt };
  bam_fuzz_deliver_raw_batches( h,
                                seq_ids,
                                packets,
                                packet_cnt ? packet_cnt : 1UL,
                                packet_cnts,
                                1UL );
}

static fd_bam_bundle_result_t
bam_fuzz_make_direct_result( bam_model_harness_t const * h,
                             uint                        seq_id,
                             uchar                       variant ) {
  fd_bam_bundle_result_t res = {
    .seq_id            = seq_id,
    .slot              = bam_model_current_slot( h ),
    .bundle_txn_cnt    = 1U,
    .execution_success = 0U,
    .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
    .bundle_err        = FD_BAM_BUNDLE_ERR_NONE,
  };

  switch( variant % 4U ) {
    case 0U:
      res.scheduling_error = FD_BAM_SCHED_ERR_OUTSIDE_SLOT;
      break;
    case 1U:
      res.scheduling_error = FD_BAM_SCHED_ERR_POH_TIMEOUT;
      break;
    case 2U:
      res.bundle_err   = FD_BAM_BUNDLE_ERR_DESER;
      res.deser_reason = bam_types_DeserializationErrorReason_SANITIZE_ERROR;
      res.deser_index  = 0U;
      break;
    case 3U:
      res.sanitize_success[0]     = 1U;
      res.transaction_err_count   = 1U;
      res.transaction_err[0]      = bam_types_TransactionErrorReason_ACCOUNT_IN_USE;
      break;
  }

  return res;
}

static void
bam_fuzz_apply_event( bam_model_harness_t * h,
                      bam_fuzz_state_t *    f,
                      bam_evt_kind_t        kind,
                      uchar                 a,
                      uchar                 b,
                      uchar                 c ) {
  if( FD_UNLIKELY( h->model_result_cnt >= BAM_MODEL_MAX_RESULTS-16UL ) ) return;

  switch( kind ) {
    case BAM_EVT_SEND_BATCH: {
      if( FD_UNLIKELY( c & 0x80U ) ) {
        if( FD_UNLIKELY( !h->state->bam_stream || !h->state->bam_stream_live ) ) break;

        if( FD_UNLIKELY( b & 0x40U ) ) {
          _Bool multi_batch = !!( b & 0x80U );
          size_t batch_cnt = multi_batch ? 2UL + (size_t)( a & 1U ) : 1UL;
          if( FD_UNLIKELY( (ulong)h->state->feedback_queue_depth + batch_cnt > FD_BAM_MAX_PENDING_RESULTS ) ) break;

          uint             seq_ids    [ BAM_FUZZ_MAX_RAW_BATCHES ];
          size_t           packet_cnts[ BAM_FUZZ_MAX_RAW_BATCHES ];
          bam_types_Packet packets    [ BAM_FUZZ_MAX_RAW_BATCHES ][ FD_BAM_MAX_TXN_PER_ATOMIC_BATCH ];

          for( size_t i=0UL; i<batch_cnt; i++ ) {
            seq_ids[ i ] = f->next_seq++;
            _Bool revert_on_error = !multi_batch
                                  ? !!( a & 1U )
                                  : !!( ( (uint)a >> ( (uint)i + 1U ) ) & 1U );
            packet_cnts[ i ] = revert_on_error
                             ? ( !multi_batch
                               ? 2UL + (size_t)(( (uint)a >> 1U ) % ( FD_BAM_MAX_TXN_PER_ATOMIC_BATCH-1U ))
                               : 1UL + (size_t)(( (uint)b + (uint)i ) & 1U ) )
                             : 1UL;

            uint seed0 = !multi_batch ? (uint)c : (uint)a + 17U*(uint)i;
            for( size_t j=0UL; j<packet_cnts[ i ]; j++ ) {
              bam_fuzz_init_real_txn_packet( &packets[ i ][ j ], seed0 + (uint)j, revert_on_error );
            }
          }
          if( FD_UNLIKELY( multi_batch && ( a & 0x80U ) ) ) {
            uint t = seq_ids[0];
            seq_ids[0] = seq_ids[ batch_cnt-1UL ];
            seq_ids[ batch_cnt-1UL ] = t;
          }

          bam_fuzz_deliver_raw_batches( h,
                                        seq_ids,
                                        &packets[0][0],
                                        FD_BAM_MAX_TXN_PER_ATOMIC_BATCH,
                                        packet_cnts,
                                        batch_cnt );
          break;
        }

        if( FD_UNLIKELY( b & 0x80U ) ) {
          ulong result_cnt = FD_BAM_MAX_ATOMIC_BATCHES_PER_PACKET + 1UL;
          if( FD_UNLIKELY( (ulong)h->state->feedback_queue_depth + result_cnt > FD_BAM_MAX_PENDING_RESULTS ) ) break;

          bam_model_batch_def_t defs[ FD_BAM_MAX_ATOMIC_BATCHES_PER_PACKET + 1U ];
          for( ulong i=0UL; i<FD_BAM_MAX_ATOMIC_BATCHES_PER_PACKET + 1UL; i++ ) {
            defs[ i ] = bam_fuzz_make_batch( h,
                                             f,
                                             (uchar)((uint)a + (uint)i),
                                             (uchar)( (uint)b & 0x0fU ),
                                             (uchar)((uint)c + (uint)i) );
          }
          f->last_batch     = defs[0];
          f->has_last_batch = 1U;
          bam_fuzz_deliver_batches( h, defs, FD_BAM_MAX_ATOMIC_BATCHES_PER_PACKET + 1UL );
          break;
        }

        if( FD_UNLIKELY( h->state->feedback_queue_depth >= FD_BAM_MAX_PENDING_RESULTS ) ) break;

        uint seq_id = f->next_seq++;
        uint submode = ( (uint)a + 3U*(uint)b + (uint)c ) % BAM_FUZZ_RAW_REJECT_SUBMODE_CNT;
        switch( submode ) {
          case 0U:
            bam_fuzz_deliver_raw_packets( h, seq_id, NULL, 0UL );
            break;
          case 1U: {
            bam_types_Packet packets[ FD_BAM_MAX_TXN_PER_ATOMIC_BATCH + 1U ];
            for( ulong i=0UL; i<sizeof(packets)/sizeof(packets[0]); i++ ) {
              bam_fuzz_init_real_txn_packet( &packets[i], (uint)a + (uint)i, 1 );
            }
            bam_fuzz_deliver_raw_packets( h, seq_id, packets, sizeof(packets)/sizeof(packets[0]) );
            break;
          }
          case 2U: {
            bam_types_Packet packets[2];
            bam_fuzz_init_real_txn_packet( &packets[0], a, 0 );
            bam_fuzz_init_real_txn_packet( &packets[1], b, 0 );
            bam_fuzz_deliver_raw_packets( h, seq_id, packets, 2UL );
            break;
          }
          case 3U: {
            bam_types_Packet packets[2];
            bam_fuzz_init_real_txn_packet( &packets[0], a, 1 );
            bam_fuzz_init_real_txn_packet( &packets[1], b, 0 );
            bam_fuzz_deliver_raw_packets( h, seq_id, packets, 2UL );
            break;
          }
          case 4U: {
            bam_types_Packet packet[1];
            bam_fuzz_init_real_txn_packet( &packet[0], a, 1 );
            packet[0].data.size = FD_TXN_MTU;
            packet[0].meta.size = FD_TXN_MTU + 1UL;
            for( pb_size_t i=0U; i<FD_TXN_MTU; i++ ) packet[0].data.bytes[i] = (uchar)( i + (pb_size_t)b );
            bam_fuzz_deliver_raw_packets( h, seq_id, packet, 1UL );
            break;
          }
          default: {
            bam_types_Packet packet[1];
            test_bam_init_simple_vote_packet( &packet[0], 1 );
            bam_fuzz_deliver_raw_packets( h, seq_id, packet, 1UL );
            break;
          }
        }
        break;
      }
      bam_model_batch_def_t def = bam_fuzz_make_batch( h, f, a, b, c );
      f->last_batch     = def;
      f->has_last_batch = 1U;
      bam_fuzz_deliver_batches( h, &def, 1UL );
      break;
    }
    case BAM_EVT_REPLAY: {
      if( FD_UNLIKELY( !f->has_last_batch ) ) {
        bam_model_batch_def_t def = bam_fuzz_make_batch( h, f, a, b, c );
        f->last_batch     = def;
        f->has_last_batch = 1U;
      }
      bam_fuzz_deliver_batches( h, &f->last_batch, 1UL );
      break;
    }
    case BAM_EVT_DUP_SEQ: {
      if( FD_UNLIKELY( c & 0x80U ) ) {
        if( FD_UNLIKELY( !h->state->bam_stream || !h->state->bam_stream_live ) ) break;

        bam_model_batch_def_t partial = bam_fuzz_make_batch( h, f, a, (uchar)( b | 1U ), c );
        if( FD_UNLIKELY( partial.txn_cnt<2U ) ) {
          partial.txn_cnt = 2U;
          partial.txn[1] = bam_fuzz_make_txn_spec( partial.seq_id, 1U, a, b, c );
        }

        fd_memset( h->partial, 0, sizeof(*h->partial) );
        h->partial->active            = 1U;
        h->partial->seq_id            = partial.seq_id;
        h->partial->max_schedule_slot = partial.max_schedule_slot;
        h->partial->revert_on_error   = 1U;
        h->partial->txn_cnt           = partial.txn_cnt;

        for( uchar i=0U; i<(uchar)( partial.txn_cnt-1U ); i++ ) {
          h->partial->seen[ i ] = 1U;
          h->partial->txn[ i ] = (bam_model_stage_txn_t) {
            .seq_id            = partial.seq_id,
            .max_schedule_slot = partial.max_schedule_slot,
            .batch_idx         = i,
            .batch_cnt         = partial.txn_cnt,
            .revert_on_error   = 1U,
            .spec              = partial.txn[ i ],
          };
        }

        f->last_batch     = partial;
        f->has_last_batch = 1U;

        bam_model_batch_def_t next = bam_fuzz_make_batch( h, f, (uchar)( a + 1U ), b, (uchar)( c + 1U ) );
        bam_fuzz_deliver_batches( h, &next, 1UL );
        break;
      }

      if( FD_UNLIKELY( !f->has_last_batch ) ) {
        bam_model_batch_def_t def = bam_fuzz_make_batch( h, f, a, b, c );
        f->last_batch     = def;
        f->has_last_batch = 1U;
      }

      bam_model_batch_def_t def = f->last_batch;
      if( FD_UNLIKELY( a & 1U ) ) {
        def.revert_on_error = (_Bool)!def.revert_on_error;
        if( FD_UNLIKELY( !def.revert_on_error ) ) def.txn_cnt = 1U;
      }
      def.txn[0] = bam_fuzz_make_txn_spec( def.seq_id, 0U, a, b, c );
      bam_fuzz_deliver_batches( h, &def, 1UL );
      break;
    }
    case BAM_EVT_NEW_SEQ_SAME_PAYLOAD: {
      if( FD_UNLIKELY( !f->has_last_batch ) ) {
        bam_model_batch_def_t def = bam_fuzz_make_batch( h, f, a, b, c );
        f->last_batch     = def;
        f->has_last_batch = 1U;
      }

      bam_model_batch_def_t def = f->last_batch;
      def.seq_id = f->next_seq++;
      if( FD_UNLIKELY( a & 0x80U ) ) def.max_schedule_slot = bam_model_current_slot( h );
      f->last_batch = def;
      bam_fuzz_deliver_batches( h, &def, 1UL );
      break;
    }
    case BAM_EVT_ADVANCE_SLOT: {
      ulong delta = 1UL + (ulong)( a & 3U );
      h->leader_working_slot += delta;
      h->bankforks_working_slot = h->leader_working_slot - (ulong)( !!( b & 1U ) );
      h->leader_working_present = (uchar)( !( b & 0x80U ) );
      h->slot_cu_used = 0U;
      h->slot_microblock_used = 0U;
      g_clock += (long)( delta * 1000000UL );
      break;
    }
    case BAM_EVT_LEADER_OFF:
      h->leader_on = 0U;
      if( FD_UNLIKELY( a & 1U ) ) h->bank_available = 0U;
      if( FD_UNLIKELY( a & 2U ) ) h->leader_working_present = 0U;
      break;
    case BAM_EVT_LEADER_ON:
      h->leader_on = 1U;
      h->bank_available = (uchar)( !( a & 1U ) );
      h->leader_working_present = (uchar)( !( a & 2U ) );
      break;
    case BAM_EVT_DISCONNECT:
      fd_bam_client_reset( h->state );
      break;
    case BAM_EVT_RECONNECT:
      bam_fuzz_ensure_scheduler_stream( h );
      if( FD_UNLIKELY( a & 0x80U ) ) bam_model_node_send_auth_challenge( h, "fuzz-auth-challenge" );
      break;
    case BAM_EVT_FILL_QUEUE: {
      ulong queue_avail = FD_BAM_MAX_PENDING_RESULTS - (ulong)h->state->feedback_queue_depth;
      if( FD_UNLIKELY( a & 0x80U ) ) {
        ulong model_avail = BAM_MODEL_MAX_RESULTS - h->model_result_cnt;
        if( FD_UNLIKELY( queue_avail > model_avail ) ) break;

        for( ulong i=0UL; i<queue_avail; i++ ) {
          fd_bam_bundle_result_t res = bam_fuzz_make_direct_result( h, f->direct_result_seq++, (uchar)( b + (uchar)i ) );
          bam_model_emit_model_result( h, &res );
        }

        ulong drop_before = h->state->metrics.feedback_results_dropped_cnt;
        fd_bam_bundle_result_t res = bam_fuzz_make_direct_result( h, f->direct_result_seq++, b );
        bam_model_emit_model_result( h, &res );
        FD_TEST( h->state->metrics.feedback_results_dropped_cnt > drop_before );
        break;
      }

      ulong n = 1UL + (ulong)( a & 7U );
      n = fd_ulong_min( n, queue_avail );

      for( ulong i=0UL; i<n; i++ ) {
        fd_bam_bundle_result_t res = bam_fuzz_make_direct_result( h, f->direct_result_seq++, (uchar)( b + (uchar)i ) );
        bam_model_emit_model_result( h, &res );
      }
      break;
    }
    case BAM_EVT_DRAIN_QUEUE: {
      bam_fuzz_ensure_scheduler_stream( h );
      ulong max_cnt = 1UL + (ulong)( a & 15U );

      while( max_cnt-- && h->state->feedback_queue_depth ) {
        ushort pending = h->state->feedback_queue_depth;
        h->state->feedback_queue_depth = 1U;
        FD_TEST( fd_bam_test_flush_results( h->state )==1 );

        FD_TEST( h->wire_result_cnt < BAM_MODEL_MAX_RESULTS );
        bam_model_wire_result_t * wr = &h->wire_results[ h->wire_result_cnt ];
        FD_TEST( bam_model_decode_last_wire_result( h->state, wr ) );
        h->wire_result_cnt++;

        h->state->feedback_queue_depth = (ushort)( pending-1U );
        bam_fuzz_assert_queue_state( h->state );
      }
      break;
    }
    case BAM_EVT_KIND_CNT:
      break;
  }

  bam_fuzz_assert_invariants( h );
}

int
LLVMFuzzerInitialize( int *    argc,
                      char *** argv ) {
  putenv( "FD_LOG_BACKTRACE=0" );
  fd_boot( argc, argv );
  fd_log_level_logfile_set( 4 );
  fd_log_level_stderr_set( 4 );
  fd_log_level_flush_set( 4 );
  fd_log_level_core_set( 4 ); /* fail fast on errors */
  fd_metrics_register( (ulong *)fd_metrics_new( metrics_scratch, 0UL ) );

  ulong cpu_idx = fd_tile_cpu_id( fd_tile_idx() );
  if( cpu_idx >= fd_shmem_cpu_cnt() ) cpu_idx = 0UL;

  char const * _page_sz = fd_env_strip_cmdline_cstr ( argc, argv, "--page-sz", NULL, "normal"                     );
  ulong        page_cnt = fd_env_strip_cmdline_ulong( argc, argv, "--page-cnt", NULL, 256UL                        );
  ulong        numa_idx = fd_env_strip_cmdline_ulong( argc, argv, "--numa-idx", NULL, fd_shmem_numa_idx( cpu_idx ) );

  g_bam_fuzz_wksp = fd_wksp_new_anonymous( fd_cstr_to_shmem_page_sz( _page_sz ),
                                           page_cnt,
                                           fd_shmem_cpu_idx( numa_idx ),
                                           "bam-e2e-fuzz",
                                           16UL );
  FD_TEST( g_bam_fuzz_wksp );
  return 0;
}

int
LLVMFuzzerTestOneInput( uchar const * data,
                        ulong         size ) {
  if( FD_UNLIKELY( !size ) ) return 0;
  FD_TEST( g_bam_fuzz_wksp );

  uint seed = 0xC0DEC0DEU;
  for( ulong i=0UL; i<fd_ulong_min( size, 4UL ); i++ ) seed = ( seed << 8 ) ^ data[ i ];

  bam_model_harness_t h[1];
  bam_model_init( h, g_bam_fuzz_wksp );

  bam_fuzz_state_t f[1];
  fd_memset( f, 0, sizeof(*f) );
  f->next_seq          = ( seed << 1 ) | 1U;
  f->direct_result_seq = 0x80000000U | ( seed & 0x00ffffffU );

  h->leader_working_slot    = 100UL + (ulong)( seed & 31U );
  h->bankforks_working_slot = h->leader_working_slot ? h->leader_working_slot-1UL : 0UL;
  h->leader_working_present = 1U;
  h->leader_on              = 1U;
  h->bank_available         = 1U;
  h->slot_cu_limit          = 200U + (uint)( seed & 0xffU );
  h->slot_microblock_limit  = 4U + (uint)(( seed>>8 ) & 31U);
  h->slot_cu_used           = 0U;
  h->slot_microblock_used   = 0U;

  for( ulong i=0UL, event_cnt=0UL; i<size && event_cnt<BAM_FUZZ_MAX_EVENTS; event_cnt++ ) {
    uchar k = data[ i++ ];
    uchar a = 0U;
    uchar b = 0U;
    uchar c = 0U;
    if( i<size ) a = data[ i++ ];
    if( i<size ) b = data[ i++ ];
    if( i<size ) c = data[ i++ ];

    bam_fuzz_apply_event( h, f, (bam_evt_kind_t)((uint)k % (uint)BAM_EVT_KIND_CNT), a, b, c );
  }

  bam_fuzz_ensure_scheduler_stream( h );
  bam_model_apply_pipeline( h );
  bam_fuzz_assert_invariants( h );
  bam_model_assert_trace_metadata( h );
  bam_model_assert_wire_matches_model( h );
  bam_fuzz_assert_invariants( h );

  bam_model_fini( h );

  fd_wksp_usage_t usage;
  FD_TEST( fd_wksp_usage( g_bam_fuzz_wksp, NULL, 0UL, &usage ) );
  FD_TEST( usage.free_cnt == usage.total_cnt );
  return 0;
}
