#define _GNU_SOURCE

#if !FD_HAS_HOSTED
#error "This target requires FD_HAS_HOSTED"
#endif

#include "fd_bam_tile.h"
#include "fd_bam_tile_private.h"
#include "fuzz_bam_pipeline_stage_dedup.h"
#include "fuzz_bam_pipeline_stage_execle.h"
#include "fuzz_bam_pipeline_stage_pack.h"
#include "fuzz_bam_pipeline_stage_resolv.h"
#include "fuzz_bam_pipeline_stage_verify.h"
#include "../tiles.h"
#include "test_bam_common.c"
#include <errno.h>
#include <limits.h>
#include <sys/mman.h>

static uchar metrics_scratch[ FD_METRICS_FOOTPRINT( 0UL ) ] __attribute__((aligned( FD_METRICS_ALIGN )));

__attribute__((weak)) char const fdctl_version_string[] = "fuzz";

FD_IMPORT_BINARY( bam_fuzz_txn1, "src/ballet/txn/fixtures/transaction1.bin" );
FD_IMPORT_BINARY( bam_fuzz_txn4, "src/ballet/txn/fixtures/transaction4.bin" );
FD_IMPORT_BINARY( bam_fuzz_txn6, "src/ballet/txn/fixtures/transaction6.bin" );
FD_IMPORT_BINARY( bam_fuzz_pack_txn1, "src/disco/pack/fixtures/txn1.bin" );
FD_IMPORT_BINARY( bam_fuzz_pack_txn3, "src/disco/pack/fixtures/txn3.bin" );
FD_IMPORT_BINARY( bam_fuzz_pack_txn4, "src/disco/pack/fixtures/txn4.bin" );
FD_IMPORT_BINARY( bam_fuzz_pack_txn5, "src/disco/pack/fixtures/txn5.bin" );
FD_IMPORT_BINARY( bam_fuzz_pack_txn6, "src/disco/pack/fixtures/txn6.bin" );
FD_IMPORT_BINARY( bam_fuzz_pack_txn7, "src/disco/pack/fixtures/txn7.bin" );

#define BAM_FUZZ_FIXTURE_CNT (9U)

/* Stateful BAM pipeline fuzzer, staged real-path conversion.

   Current stages exercise the real BAM scheduler protobuf decode,
   pending transaction queue, bam_verif publication, verify/dedup/resolv tile
   callbacks, pack tile callbacks through pack_execle and pack_bam_*, execle
   tile callbacks through native Firedancer runtime execution, durable feedback
   queue, and outbound scheduler protobuf result encoding.  It has no model
   oracle, and all scheduler transactions are real signature-valid fixture
   bytes. */

extern void
fd_bam_test_receive_ingress_frag( fd_bam_tile_t * ctx,
                                  ulong           in_idx,
                                  ulong           sig,
                                  ulong           chunk,
                                  ulong           sz );

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
  BAM_EVT_RESULT_BURST         = 9,
  BAM_EVT_DRAIN_QUEUE          = 10
} bam_evt_kind_t;

/* Corpus records are fixed 4-byte events: kind, a, b, c.  The kind byte is a
   literal bam_evt_kind_t; invalid kinds are ignored instead of remapped. */

#define BAM_EVT_KIND_CNT 11U
#define BAM_FUZZ_MAX_EVENTS 128UL
#define BAM_FUZZ_MAX_BATCHES FD_BAM_FULL_ATOMIC_BATCHES_PER_STEM_BURST
#define BAM_FUZZ_PB_BUF_SZ  (64UL*1024UL)
#define BAM_FUZZ_WKSP_FOOTPRINT (8UL<<30)

#define BAM_FUZZ_BANK_IN_EXECLE_IDX (0UL)
#define BAM_FUZZ_BANK_IN_BASE_IDX   (3UL)
static fd_wksp_t * g_bam_fuzz_wksp;
static long        g_bam_fuzz_now = 1L;

long
fd_bam_now( void ) {
  return g_bam_fuzz_now;
}

static fd_wksp_t *
bam_fuzz_wksp_new_lazy( ulong footprint ) {
  footprint = fd_ulong_align_up( footprint, FD_SHMEM_NORMAL_PAGE_SZ );
  void * mem = mmap( NULL, footprint, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0 );
  if( FD_UNLIKELY( mem==MAP_FAILED ) ) {
    FD_LOG_ERR(( "mmap(NULL,%lu KiB,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS) failed (%i-%s)",
                 footprint>>10, errno, fd_io_strerror( errno ) ));
  }

  ulong part_max = fd_wksp_part_max_est( footprint, 64UL<<10 );
  FD_TEST( part_max );
  ulong data_max = fd_wksp_data_max_est( footprint, part_max );
  FD_TEST( data_max );

  fd_wksp_t * wksp = fd_wksp_join( fd_wksp_new( mem, "bam-pipeline-fuzz", 1U, part_max, data_max ) );
  FD_TEST( wksp );
  FD_TEST( !fd_shmem_join_anonymous( "bam-pipeline-fuzz",
                                     FD_SHMEM_JOIN_MODE_READ_WRITE,
                                     wksp,
                                     mem,
                                     FD_SHMEM_NORMAL_PAGE_SZ,
                                     footprint>>FD_SHMEM_NORMAL_LG_PAGE_SZ ) );
  return wksp;
}

typedef struct {
  uint  seq_id;
  ulong max_schedule_slot;
  uchar txn_cnt;
  uchar revert_on_error;
  uint  payload_seed[ FD_BAM_MAX_TXN_PER_ATOMIC_BATCH ];
} bam_fuzz_batch_spec_t;

typedef struct {
  uint next_seq;
  ulong current_slot;
  fd_bam_leader_state_t leader_state;

  bam_fuzz_batch_spec_t last_batch[ BAM_FUZZ_MAX_BATCHES ];
  ulong                 last_batch_cnt;
  uchar                 has_last_batch;

  fd_bam_bundle_result_t durable_results[ FD_BAM_MAX_PENDING_RESULTS ];
  ushort durable_results_head;
  ushort durable_results_tail;
  ushort durable_results_depth;

  uint observed_outside_slot;
  uint observed_txn_error;
  uint observed_result_drop;

  uint required_outside_slot;
  uint required_txn_error;
  uint required_result_drop;
} bam_fuzz_state_t;

typedef struct {
  bam_fuzz_link_t verify_out;
  bam_fuzz_link_t dedup_out;
  bam_fuzz_link_t resolv_pack;
  bam_fuzz_link_t pack_execle;
  bam_fuzz_link_t pack_bam_leader;
  bam_fuzz_link_t pack_bam_result;
  bam_fuzz_link_t execle_bank_bam;
  ulong *         pack_execle_busy_fseq;
} bam_fuzz_links_t;

static void
bam_fuzz_ensure_scheduler_stream( test_bam_env_t * env ) {
  fd_bam_tile_t * state = env->state;
  if( FD_LIKELY( state->bam_stream && state->bam_stream_live ) ) return;

  test_bam_env_mock_conn( env );
  test_bam_prepare_scheduler_stream( state );
  test_bam_keepalive_sync( state, g_bam_fuzz_now );

  uchar pb[ 64 ];
  bam_api_SchedulerResponse resp = bam_api_SchedulerResponse_init_default;
  resp.which_versioned_msg = bam_api_SchedulerResponse_v0_tag;
  resp.versioned_msg.v0.which_resp = bam_api_SchedulerResponseV0_heart_beat_tag;
  resp.versioned_msg.v0.resp.heart_beat.time_sent_microseconds =
      (ulong)fd_long_max( g_bam_fuzz_now/1000L, 0L );

  pb_ostream_t ostream = pb_ostream_from_buffer( pb, sizeof(pb) );
  FD_TEST( pb_encode( &ostream, bam_api_SchedulerResponse_fields, &resp ) );
  fd_bam_client_grpc_rx_msg( state, pb, ostream.bytes_written, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
}

static uint
bam_fuzz_next_seq( bam_fuzz_state_t * f ) {
  return f->next_seq++;
}

static void
bam_fuzz_assert_result_summary_eq( fd_bam_bundle_result_t const * actual,
                                   fd_bam_bundle_result_t const * expected ) {
  FD_TEST( actual->seq_id                == expected->seq_id );
  FD_TEST( actual->scheduler_gen         == expected->scheduler_gen );
  FD_TEST( actual->slot                  == expected->slot );
  FD_TEST( actual->bundle_txn_cnt        == expected->bundle_txn_cnt );
  FD_TEST( actual->execution_success     == expected->execution_success );
  FD_TEST( actual->scheduling_error      == expected->scheduling_error );
  FD_TEST( actual->bundle_err            == expected->bundle_err );
  FD_TEST( actual->deser_index           == expected->deser_index );
  FD_TEST( actual->deser_reason          == expected->deser_reason );
  FD_TEST( actual->transaction_err_count == expected->transaction_err_count );
  for( uchar i=0U; i<expected->bundle_txn_cnt; i++ ) {
    FD_TEST( actual->sanitize_success[ i ]             == expected->sanitize_success[ i ] );
    FD_TEST( actual->transaction_err[ i ]              == expected->transaction_err[ i ] );
    FD_TEST( actual->consumed_cus[ i ]                 == expected->consumed_cus[ i ] );
    FD_TEST( actual->feepayer_balance_lamports[ i ]    == expected->feepayer_balance_lamports[ i ] );
    FD_TEST( actual->loaded_accounts_data_size[ i ]    == expected->loaded_accounts_data_size[ i ] );
  }
}

static void
bam_fuzz_shadow_push_result( bam_fuzz_state_t *              f,
                             fd_bam_bundle_result_t const * res ) {
  FD_TEST( f->durable_results_depth < FD_BAM_MAX_PENDING_RESULTS );
  for( ushort i=0U; i<f->durable_results_depth; i++ ) {
    ushort idx = (ushort)(((uint)f->durable_results_head + (uint)i) % FD_BAM_MAX_PENDING_RESULTS);
    fd_bam_bundle_result_t const * pending = &f->durable_results[ idx ];
    FD_TEST( pending->scheduler_gen!=res->scheduler_gen ||
             pending->slot         !=res->slot          ||
             pending->seq_id       !=res->seq_id );
  }

  if( FD_UNLIKELY( res->scheduling_error==FD_BAM_SCHED_ERR_OUTSIDE_SLOT ) ) f->observed_outside_slot = 1U;
  if( FD_UNLIKELY( res->transaction_err_count ) ) {
    for( uchar i=0U; i<res->bundle_txn_cnt; i++ ) {
      if( FD_UNLIKELY( res->transaction_err[ i ]!=bam_types_TransactionErrorReason_COMMIT_CANCELLED ) ) {
        f->observed_txn_error = 1U;
      }
    }
  }

  f->durable_results[ f->durable_results_tail ] = *res;
  f->durable_results_tail = (ushort)(( f->durable_results_tail + 1U ) % FD_BAM_MAX_PENDING_RESULTS);
  f->durable_results_depth = (ushort)( f->durable_results_depth + 1U );
}

static void
bam_fuzz_assert_shadow_queue( bam_fuzz_state_t const * f,
                              fd_bam_tile_t const *    state ) {
  FD_TEST( state->feedback_queue_depth <= FD_BAM_MAX_PENDING_RESULTS );
  FD_TEST( state->bam_results_head     <  FD_BAM_MAX_PENDING_RESULTS );
  FD_TEST( state->feedback_queue_depth==f->durable_results_depth );

  ushort expected_tail = (ushort)(((uint)state->bam_results_head +
                                   (uint)state->feedback_queue_depth) %
                                  FD_BAM_MAX_PENDING_RESULTS);
  FD_TEST( state->bam_results_tail==expected_tail );

  for( ushort i=0U; i<f->durable_results_depth; i++ ) {
    ushort state_idx = (ushort)(((uint)state->bam_results_head + (uint)i) % FD_BAM_MAX_PENDING_RESULTS);
    ushort shadow_idx = (ushort)(((uint)f->durable_results_head + (uint)i) % FD_BAM_MAX_PENDING_RESULTS);
    bam_fuzz_assert_result_summary_eq( &state->bam_results[ state_idx ],
                                       &f->durable_results[ shadow_idx ] );
  }
}

static void
bam_fuzz_mirror_queue_growth( bam_fuzz_state_t *    f,
                              fd_bam_tile_t const * state,
                              ushort                depth_before,
                              ushort                tail_before ) {
  FD_TEST( state->feedback_queue_depth>=depth_before );

  ushort added = (ushort)( state->feedback_queue_depth - depth_before );
  for( ushort i=0U; i<added; i++ ) {
    ushort state_idx = (ushort)(((uint)tail_before + (uint)i) % FD_BAM_MAX_PENDING_RESULTS);
    bam_fuzz_shadow_push_result( f, &state->bam_results[ state_idx ] );
  }
  bam_fuzz_assert_shadow_queue( f, state );
}

static void
bam_fuzz_assert_outbound_leader_state( fd_bam_tile_t *               state,
                                       fd_bam_leader_state_t const * expected ) {
  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.which_versioned_msg==bam_api_SchedulerMessage_v0_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg==bam_api_SchedulerMessageV0_leader_state_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.msg.leader_state.slot==expected->slot );
  FD_TEST( decoded.msg.versioned_msg.v0.msg.leader_state.tick==expected->tick );
  FD_TEST( decoded.msg.versioned_msg.v0.msg.leader_state.slot_cu_budget_remaining==
           expected->slot_cu_budget_remaining );
}

static void
bam_fuzz_assert_frag_bounds( fd_frag_meta_t const * meta,
                             ulong                  chunk0,
                             ulong                  wmark,
                             ulong                  min_sz,
                             ulong                  max_sz ) {
  FD_TEST( meta->sz>=min_sz );
  FD_TEST( meta->sz<=max_sz );
  FD_TEST( meta->chunk>=chunk0 );
  FD_TEST( meta->chunk<=wmark );
}

static fd_txn_m_t const *
bam_fuzz_txnm_from_link( bam_fuzz_link_t const * link,
                         ulong                   seq,
                         ulong                   max_sz,
                         fd_frag_meta_t const **  meta_out ) {
  fd_frag_meta_t const * meta = link->mcache + fd_mcache_line_idx( seq, link->depth );
  bam_fuzz_assert_frag_bounds( meta,
                               link->chunk0,
                               link->wmark,
                               sizeof(fd_txn_m_t),
                               max_sz );
  fd_txn_m_t const * txnm = (fd_txn_m_t const *)fd_chunk_to_laddr_const( link->mem, meta->chunk );
  FD_TEST( txnm->txn_t_sz>0U );
  *meta_out = meta;
  return txnm;
}

static void
bam_fuzz_ingest_result_link( test_bam_env_t *       env,
                             bam_fuzz_state_t *     f,
                             ulong                  in_idx,
                             bam_fuzz_link_t const * link,
                             ulong                  seq ) {
  fd_frag_meta_t const * meta = link->mcache + fd_mcache_line_idx( seq, link->depth );
  bam_fuzz_assert_frag_bounds( meta,
                               link->chunk0,
                               link->wmark,
                               sizeof(fd_bam_bundle_result_t),
                               sizeof(fd_bam_bundle_result_t) );
  fd_bam_bundle_result_t const * result =
      (fd_bam_bundle_result_t const *)fd_chunk_to_laddr_const( link->mem, meta->chunk );

  fd_bam_tile_t * state = env->state;
  ushort depth_before = state->feedback_queue_depth;
  ushort tail_before  = state->bam_results_tail;
  ulong drops_before  = state->metrics.feedback_results_dropped_cnt;
  fd_bam_test_receive_ingress_frag( state, in_idx, meta->sig, meta->chunk, meta->sz );

  if( FD_UNLIKELY( state->feedback_queue_depth==depth_before &&
                   state->metrics.feedback_results_dropped_cnt==drops_before ) ) {
    FD_TEST( state->bam_results_tail==tail_before );
    bam_fuzz_assert_shadow_queue( f, state );
    return;
  }

  if( FD_UNLIKELY( depth_before>=FD_BAM_MAX_PENDING_RESULTS ) ) {
    FD_TEST( state->feedback_queue_depth==depth_before );
    FD_TEST( state->metrics.feedback_results_dropped_cnt==drops_before+1UL );
    f->observed_result_drop = 1U;
    return;
  }

  FD_TEST( state->metrics.feedback_results_dropped_cnt==drops_before );
  FD_TEST( state->feedback_queue_depth==(ushort)( depth_before+1U ) );
  FD_TEST( state->bam_results_tail==(ushort)(( (uint)tail_before+1U ) % FD_BAM_MAX_PENDING_RESULTS) );
  bam_fuzz_assert_result_summary_eq( &state->bam_results[ tail_before ], result );
  bam_fuzz_shadow_push_result( f, result );
  bam_fuzz_assert_shadow_queue( f, state );
}

static void
bam_fuzz_reset_preserving_feedback( bam_fuzz_state_t * f,
                                    fd_bam_tile_t *    state ) {
  bam_fuzz_assert_shadow_queue( f, state );

  fd_bam_client_reset( state );

  bam_fuzz_assert_shadow_queue( f, state );
}

static fd_txn_m_t const *
bam_fuzz_assert_bam_publish( test_bam_env_t const * env,
                             fd_frag_meta_t const * meta ) {
  fd_bam_tile_t const * state = env->state;
  FD_TEST( meta->sz >= sizeof(fd_txn_m_t) );
  FD_TEST( meta->sz <= FD_TPU_PARSED_MTU );
  FD_TEST( meta->chunk >= state->verify_out.chunk0 );
  FD_TEST( meta->chunk <= state->verify_out.wmark );

  fd_txn_m_t const * txnm = (fd_txn_m_t const *)fd_chunk_to_laddr_const( state->verify_out.mem, meta->chunk );
  FD_TEST( txnm->source_tpu==FD_TXN_M_TPU_SOURCE_BAM );
  FD_TEST( txnm->payload_sz>0U );
  FD_TEST( txnm->payload_sz<=FD_TXN_MTU );
  FD_TEST( txnm->bam.txn_cnt<=FD_BAM_MAX_TXN_PER_ATOMIC_BATCH );
  FD_TEST( txnm->bam.batch_idx<txnm->bam.txn_cnt );
  FD_TEST( txnm->bam.max_schedule_slot!=ULONG_MAX );

  if( txnm->bam.revert_on_error ) {
    FD_TEST( txnm->block_engine.bundle_id==((ulong)txnm->bam.seq_id + 1UL) );
    FD_TEST( txnm->block_engine.bundle_txn_cnt==
             (txnm->bam.batch_idx ? 0UL : (ulong)txnm->bam.txn_cnt) );
  } else {
    FD_TEST( txnm->block_engine.bundle_id==0UL );
    FD_TEST( txnm->block_engine.bundle_txn_cnt==0UL );
  }

  uchar txn_buf[ FD_TXN_MAX_SZ ];
  FD_TEST( fd_txn_parse( fd_txn_m_payload_const( txnm ), txnm->payload_sz, txn_buf, NULL ) );
  return txnm;
}

static void
bam_fuzz_ingest_pack_outputs( test_bam_env_t *              env,
                              bam_fuzz_state_t *            f,
                              bam_fuzz_links_t const *       links,
                              bam_fuzz_execle_t *           execle,
                              bam_fuzz_pack_result_t const * res ) {
  fd_bam_tile_t * state = env->state;

  for( ulong seq=res->bam_leader_before; seq<res->bam_leader_after; seq++ ) {
    fd_frag_meta_t const * meta = links->pack_bam_leader.mcache +
                                  fd_mcache_line_idx( seq, links->pack_bam_leader.depth );
    bam_fuzz_assert_frag_bounds( meta,
                                 links->pack_bam_leader.chunk0,
                                 links->pack_bam_leader.wmark,
                                 sizeof(fd_bam_leader_state_t),
                                 sizeof(fd_bam_leader_state_t) );
    fd_bam_leader_state_t const * leader =
        (fd_bam_leader_state_t const *)fd_chunk_to_laddr_const( links->pack_bam_leader.mem, meta->chunk );
    FD_TEST( leader->slot!=ULONG_MAX );
    fd_bam_test_receive_ingress_frag( state,
                                      state->pack_bam_leader_in_idx,
                                      meta->sig,
                                      meta->chunk,
                                      meta->sz );
    FD_TEST( state->bam_leader_state.slot==leader->slot );
    FD_TEST( state->bam_leader_state.tick==leader->tick );
    FD_TEST( state->bam_leader_state.slot_cu_budget_remaining==leader->slot_cu_budget_remaining );
    FD_TEST( state->bam_leader_state.current_slot_has_bam_work==leader->current_slot_has_bam_work );
    fd_grpc_client_t * client = state->grpc_client;
    if( FD_UNLIKELY( client &&
                     ( !fd_h2_rbuf_is_empty( client->frame_tx ) ||
                       ( client->request_stream && client->request_tx_op->chunk_sz ) ) ) ) {
      bam_fuzz_assert_outbound_leader_state( state, leader );
    }
  }

  for( ulong seq=res->bam_result_before; seq<res->bam_result_after; seq++ ) {
    bam_fuzz_ingest_result_link( env, f, state->pack_bam_result_in_idx, &links->pack_bam_result, seq );
  }

  for( ulong seq=res->execle_before; seq<res->execle_after; seq++ ) {
    fd_frag_meta_t const * meta = links->pack_execle.mcache +
                                  fd_mcache_line_idx( seq, links->pack_execle.depth );
    bam_fuzz_assert_frag_bounds( meta,
                                 links->pack_execle.chunk0,
                                 links->pack_execle.wmark,
                                 sizeof(fd_microblock_execle_trailer_t)+1UL,
                                 MAX_MICROBLOCK_SZ );
    bam_fuzz_execle_result_t execle_res = bam_fuzz_execle_frag( execle, meta, seq );
    FD_TEST( execle_res.poh_after>=execle_res.poh_before );
    for( ulong bank_seq=execle_res.bank_bam_before; bank_seq<execle_res.bank_bam_after; bank_seq++ ) {
      bam_fuzz_ingest_result_link( env,
                                   f,
                                   state->bank_bam_in_idx + BAM_FUZZ_BANK_IN_EXECLE_IDX,
                                   &links->execle_bank_bam,
                                   bank_seq );
    }
  }

  FD_TEST( res->poh_after>=res->poh_before );
}

static void
bam_fuzz_pump_pack( test_bam_env_t *  env,
                    bam_fuzz_state_t * f,
                    bam_fuzz_links_t const * links,
                    bam_fuzz_pack_t * pack,
                    bam_fuzz_execle_t * execle,
                    ulong             max_iter ) {
  for( ulong i=0UL; i<max_iter; i++ ) {
    bam_fuzz_pack_result_t res = bam_fuzz_pack_credit( pack );
    bam_fuzz_ingest_pack_outputs( env, f, links, execle, &res );
    if( FD_UNLIKELY( res.execle_after==res.execle_before &&
                     res.bam_leader_after==res.bam_leader_before &&
                     res.bam_result_after==res.bam_result_before &&
                     res.poh_after==res.poh_before ) ) {
      break;
    }
  }
}

static void
bam_fuzz_sync_pack_leader( test_bam_env_t *  env,
                           bam_fuzz_state_t * f,
                           bam_fuzz_links_t const * links,
                           bam_fuzz_resolv_t * resolv,
                           bam_fuzz_pack_t * pack,
                           bam_fuzz_execle_t * execle ) {
  (void)env;
  ulong slot = f->leader_state.slot;
  fd_banks_t * banks = NULL;
  fd_accdb_t * accdb = NULL;
  void const * bank = NULL;
  ulong bank_idx = ULONG_MAX;
  bam_fuzz_execle_prepare_slot( execle, slot, &banks, &accdb, &bank, &bank_idx );
  bam_fuzz_resolv_prepare_slot( resolv,
                                slot,
                                banks,
                                accdb,
                                (fd_bank_t *)bank );
  bam_fuzz_pack_result_t res = bam_fuzz_pack_set_leader_slot( pack, slot, bank, bank_idx, g_bam_fuzz_now );
  bam_fuzz_ingest_pack_outputs( env, f, links, execle, &res );
  bam_fuzz_pump_pack( env, f, links, pack, execle, 4UL );
}

static void
bam_fuzz_assert_txnm_preserved( fd_txn_m_t const * txnm,
                                fd_txn_m_t const * bam_txnm ) {
  FD_TEST( txnm->source_tpu==FD_TXN_M_TPU_SOURCE_BAM );
  FD_TEST( txnm->payload_sz==bam_txnm->payload_sz );
  FD_TEST( 0==memcmp( fd_txn_m_payload_const( txnm ),
                      fd_txn_m_payload_const( bam_txnm ),
                      bam_txnm->payload_sz ) );
  FD_TEST( txnm->bam.seq_id==bam_txnm->bam.seq_id );
  FD_TEST( txnm->bam.txn_cnt==bam_txnm->bam.txn_cnt );
  FD_TEST( txnm->bam.batch_idx==bam_txnm->bam.batch_idx );
  FD_TEST( txnm->bam.max_schedule_slot==bam_txnm->bam.max_schedule_slot );
  FD_TEST( txnm->bam.revert_on_error==bam_txnm->bam.revert_on_error );
}

static void
bam_fuzz_pump_resolv( test_bam_env_t *    env,
                      bam_fuzz_state_t *  f,
                      bam_fuzz_links_t const * links,
                      bam_fuzz_resolv_t * resolv,
                      bam_fuzz_pack_t *   pack,
                      bam_fuzz_execle_t * execle,
                      ulong               max_iter ) {
  for( ulong i=0UL; i<max_iter; i++ ) {
    bam_fuzz_resolv_result_t res = bam_fuzz_resolv_credit( resolv );
    for( ulong pack_seq=res.pack_before; pack_seq<res.pack_after; pack_seq++ ) {
      fd_frag_meta_t const * pack_meta = links->resolv_pack.mcache +
                                         fd_mcache_line_idx( pack_seq, links->resolv_pack.depth );
      bam_fuzz_assert_frag_bounds( pack_meta,
                                   links->resolv_pack.chunk0,
                                   links->resolv_pack.wmark,
                                   sizeof(fd_txn_m_t),
                                   FD_TPU_RESOLVED_MTU );
      bam_fuzz_pack_result_t pack_res = bam_fuzz_pack_frag( pack, pack_meta, pack_seq );
      bam_fuzz_ingest_pack_outputs( env, f, links, execle, &pack_res );
    }
    if( FD_UNLIKELY( res.pack_after==res.pack_before ) ) break;
  }
}

static void
bam_fuzz_run_pipeline( test_bam_env_t *       env,
                       bam_fuzz_state_t *     f,
                       bam_fuzz_links_t const * links,
                       bam_fuzz_verify_t *    verify,
                       bam_fuzz_dedup_t *     dedup,
                       bam_fuzz_resolv_t *    resolv,
                       bam_fuzz_pack_t *      pack,
                       bam_fuzz_execle_t *    execle,
                       fd_frag_meta_t const * bam_meta,
                       fd_txn_m_t const *     bam_txnm,
                       ulong                  bam_seq ) {
  bam_fuzz_verify_result_t verify_res = bam_fuzz_verify_frag( verify, bam_meta, bam_seq );

  for( ulong verify_seq=verify_res.verify_before; verify_seq<verify_res.verify_after; verify_seq++ ) {
    fd_frag_meta_t const * verify_meta = NULL;
    fd_txn_m_t const * verify_txnm =
        bam_fuzz_txnm_from_link( &links->verify_out, verify_seq, FD_TPU_PARSED_MTU, &verify_meta );
    bam_fuzz_assert_txnm_preserved( verify_txnm, bam_txnm );

    bam_fuzz_dedup_result_t dedup_res = bam_fuzz_dedup_frag( dedup, verify_meta, verify_seq );
    for( ulong dedup_seq=dedup_res.dedup_before; dedup_seq<dedup_res.dedup_after; dedup_seq++ ) {
      fd_frag_meta_t const * dedup_meta = NULL;
      fd_txn_m_t const * dedup_txnm =
          bam_fuzz_txnm_from_link( &links->dedup_out, dedup_seq, FD_TPU_PARSED_MTU, &dedup_meta );
      bam_fuzz_assert_txnm_preserved( dedup_txnm, bam_txnm );
      FD_TEST( dedup_meta->sig==1UL );

      bam_fuzz_resolv_result_t resolv_res = bam_fuzz_resolv_frag( resolv, dedup_meta, dedup_seq );
      for( ulong pack_seq=resolv_res.pack_before; pack_seq<resolv_res.pack_after; pack_seq++ ) {
        fd_frag_meta_t const * pack_meta = NULL;
        fd_txn_m_t const * pack_txnm =
            bam_fuzz_txnm_from_link( &links->resolv_pack, pack_seq, FD_TPU_RESOLVED_MTU, &pack_meta );
        bam_fuzz_assert_txnm_preserved( pack_txnm, bam_txnm );

        bam_fuzz_pack_result_t pack_res = bam_fuzz_pack_frag( pack, pack_meta, pack_seq );
        bam_fuzz_ingest_pack_outputs( env, f, links, execle, &pack_res );
        bam_fuzz_pump_pack( env, f, links, pack, execle, 8UL );
      }
    }
  }

  bam_fuzz_pump_resolv( env, f, links, resolv, pack, execle, 4UL );
}

static void
bam_fuzz_drain_pending_txns( test_bam_env_t *    env,
                             bam_fuzz_state_t *  f,
                             bam_fuzz_links_t const * links,
                             bam_fuzz_verify_t * verify,
                             bam_fuzz_dedup_t *  dedup,
                             bam_fuzz_resolv_t * resolv,
                             bam_fuzz_pack_t *   pack,
                             bam_fuzz_execle_t * execle,
                             ulong               max_credit ) {
  if( FD_UNLIKELY( !max_credit ) ) return;

  fd_bam_tile_t * state = env->state;
  ulong out_idx = state->verify_out.idx;
  ulong seq_before = env->stem_seqs[ out_idx ];

  if( FD_UNLIKELY( max_credit==ULONG_MAX ) ) {
    (void)test_bam_env_drain_all_pending_txns( env );
  } else {
    for( ulong i=0UL; i<max_credit && !bam_pending_txn_empty( state->pending_txns ); i++ ) {
      if( FD_UNLIKELY( !test_bam_env_drain_pending_txns( env ) ) ) break;
    }
  }

  ulong seq_after = env->stem_seqs[ out_idx ];
  FD_TEST( seq_after>=seq_before );
  for( ulong seq=seq_before; seq<seq_after; seq++ ) {
    fd_frag_meta_t const * meta = env->out_mcache + fd_mcache_line_idx( seq, env->stem_depths[ out_idx ] );
    fd_txn_m_t const * txnm = bam_fuzz_assert_bam_publish( env, meta );
    bam_fuzz_run_pipeline( env, f, links, verify, dedup, resolv, pack, execle, meta, txnm, seq );
  }
}

static void
bam_fuzz_assert_invariants( test_bam_env_t const * env,
                            bam_fuzz_state_t const * f ) {
  fd_bam_tile_t const * state = env->state;
  bam_fuzz_assert_shadow_queue( f, state );
  if( FD_UNLIKELY( f->required_outside_slot ) ) FD_TEST( f->observed_outside_slot );
  if( FD_UNLIKELY( f->required_txn_error   ) ) FD_TEST( f->observed_txn_error   );
  if( FD_UNLIKELY( f->required_result_drop ) ) FD_TEST( f->observed_result_drop );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) <= bam_pending_txn_max( state->pending_txns ) );
  FD_TEST( state->verify_out.chunk >= state->verify_out.chunk0 );
  FD_TEST( state->verify_out.chunk <= state->verify_out.wmark );
}

static void
bam_fuzz_make_batch_specs( bam_fuzz_state_t *      f,
                           bam_fuzz_batch_spec_t * specs,
                           ulong *                 spec_cnt,
                           uchar                   a,
                           uchar                   b,
                           uchar                   c ) {
  ulong batch_cnt = (b & 0x80U)
                  ? (ulong)BAM_FUZZ_MAX_BATCHES
                  : 1UL + (ulong)( (a>>5) & 3U );
  batch_cnt = fd_ulong_min( batch_cnt, (ulong)BAM_FUZZ_MAX_BATCHES );

  for( ulong i=0UL; i<batch_cnt; i++ ) {
    _Bool revert_on_error = !!( (b >> (i & 3UL)) & 1U );
    uchar txn_cnt = revert_on_error
                  ? (uchar)( 1U + (uchar)(((uint)a + (uint)i) % FD_BAM_MAX_TXN_PER_ATOMIC_BATCH) )
                  : 1U;

    ulong max_schedule_slot;
    if( FD_UNLIKELY( b & 0x20U ) ) max_schedule_slot = f->current_slot ? f->current_slot-1UL : 0UL;
    else                           max_schedule_slot = f->current_slot + (ulong)((c >> 5) & 3U);

    specs[ i ] = (bam_fuzz_batch_spec_t) {
      .seq_id            = bam_fuzz_next_seq( f ),
      .max_schedule_slot = max_schedule_slot,
      .txn_cnt           = txn_cnt,
      .revert_on_error   = (uchar)revert_on_error,
    };
    for( uchar j=0U; j<txn_cnt; j++ ) {
      specs[ i ].payload_seed[ j ] = (uint)c + 17U*(uint)i + 5U*(uint)j;
    }
  }

  *spec_cnt = batch_cnt;
}

static void
bam_fuzz_ensure_last_batch( bam_fuzz_state_t * f,
                            uchar              a,
                            uchar              b,
                            uchar              c ) {
  if( FD_LIKELY( f->has_last_batch ) ) return;

  bam_fuzz_batch_spec_t specs[ BAM_FUZZ_MAX_BATCHES ];
  ulong spec_cnt = 0UL;
  bam_fuzz_make_batch_specs( f, specs, &spec_cnt, a, b, c );
  fd_memcpy( f->last_batch, specs, spec_cnt*sizeof(bam_fuzz_batch_spec_t) );
  f->last_batch_cnt = spec_cnt;
  f->has_last_batch = 1U;
}

static void
bam_fuzz_deliver_specs( test_bam_env_t *              env,
                        bam_fuzz_state_t *            f,
                        bam_fuzz_links_t const *       links,
                        bam_fuzz_verify_t *           verify,
                        bam_fuzz_dedup_t *            dedup,
                        bam_fuzz_resolv_t *           resolv,
                        bam_fuzz_pack_t *             pack,
                        bam_fuzz_execle_t *           execle,
                        bam_fuzz_batch_spec_t const * specs,
                        ulong                         spec_cnt,
                        ulong                         drain_credit ) {
  if( FD_UNLIKELY( !( env->state->bam_stream && env->state->bam_stream_live ) ) ) return;

  bam_fuzz_sync_pack_leader( env, f, links, resolv, pack, execle );

  uchar const * const payloads[ BAM_FUZZ_FIXTURE_CNT ] = {
    bam_fuzz_txn1,
    bam_fuzz_txn4,
    bam_fuzz_pack_txn1,
    bam_fuzz_pack_txn3,
    bam_fuzz_pack_txn4,
    bam_fuzz_pack_txn5,
    bam_fuzz_txn6,
    bam_fuzz_pack_txn6,
    bam_fuzz_pack_txn7,
  };
  ulong const sizes[ BAM_FUZZ_FIXTURE_CNT ] = {
    bam_fuzz_txn1_sz,
    bam_fuzz_txn4_sz,
    bam_fuzz_pack_txn1_sz,
    bam_fuzz_pack_txn3_sz,
    bam_fuzz_pack_txn4_sz,
    bam_fuzz_pack_txn5_sz,
    bam_fuzz_txn6_sz,
    bam_fuzz_pack_txn6_sz,
    bam_fuzz_pack_txn7_sz,
  };
  test_bam_packet_encode_ctx_t packet_ctx[ BAM_FUZZ_MAX_BATCHES ];
  bam_types_AtomicTxnBatch     batches   [ BAM_FUZZ_MAX_BATCHES ];
  bam_types_Packet             packets   [ BAM_FUZZ_MAX_BATCHES ][ FD_BAM_MAX_TXN_PER_ATOMIC_BATCH ];

  FD_TEST( spec_cnt<=BAM_FUZZ_MAX_BATCHES );
  for( ulong i=0UL; i<spec_cnt; i++ ) {
    FD_TEST( specs[ i ].txn_cnt<=FD_BAM_MAX_TXN_PER_ATOMIC_BATCH );
    for( uchar j=0U; j<specs[ i ].txn_cnt; j++ ) {
      bam_types_Packet * pkt = &packets[ i ][ j ];
      ulong idx = (ulong)( specs[ i ].payload_seed[ j ] % BAM_FUZZ_FIXTURE_CNT );
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
      pkt->meta.flags.revert_on_error = specs[ i ].revert_on_error;
    }

    packet_ctx[ i ] = (test_bam_packet_encode_ctx_t) {
      .packets    = packets[ i ],
      .packet_cnt = specs[ i ].txn_cnt,
    };

    batches[ i ] = (bam_types_AtomicTxnBatch)bam_types_AtomicTxnBatch_init_default;
    batches[ i ].seq_id            = specs[ i ].seq_id;
    batches[ i ].max_schedule_slot = specs[ i ].max_schedule_slot;
    batches[ i ].packets.funcs.encode = test_bam_encode_packets_cb;
    batches[ i ].packets.arg          = &packet_ctx[ i ];
  }

  uchar pb[ BAM_FUZZ_PB_BUF_SZ ];
  size_t pb_sz = test_bam_encode_scheduler_multi_batch_response( batches, spec_cnt, pb, sizeof(pb) );
  ushort depth_before = env->state->feedback_queue_depth;
  ushort tail_before  = env->state->bam_results_tail;
  ulong drops_before  = env->state->metrics.feedback_results_dropped_cnt;
  uint require_outside_slot = 0U;
  if( FD_LIKELY( f->leader_state.slot==f->current_slot ) ) {
    ulong accept_capacity = (ulong)FD_BAM_MAX_PENDING_RESULTS - (ulong)depth_before;
    for( ulong i=0UL; i<spec_cnt && i<accept_capacity; i++ ) {
      if( FD_UNLIKELY( specs[ i ].max_schedule_slot<f->current_slot ) ) {
        require_outside_slot = 1U;
        break;
      }
    }
  }
  fd_bam_client_grpc_rx_msg( env->state, pb, pb_sz, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( env->state->metrics.feedback_results_dropped_cnt>=drops_before );
  if( FD_UNLIKELY( env->state->metrics.feedback_results_dropped_cnt>drops_before ) ) {
    f->observed_result_drop = 1U;
  }
  bam_fuzz_mirror_queue_growth( f, env->state, depth_before, tail_before );
  if( FD_UNLIKELY( require_outside_slot ) ) {
    f->required_outside_slot = 1U;
    FD_TEST( f->observed_outside_slot );
  }
  bam_fuzz_drain_pending_txns( env, f, links, verify, dedup, resolv, pack, execle, drain_credit );
}

/* Validates results[ idx ] of a (possibly batched) outbound result message
   against the expected shadow-queue entry. */

static void
bam_fuzz_assert_decoded_result_at( fd_bam_bundle_result_t const *     expected,
                                   test_bam_decoded_message_t const * decoded,
                                   ulong                              idx ) {
  FD_TEST( decoded->msg.which_versioned_msg==bam_api_SchedulerMessage_v0_tag );
  FD_TEST( decoded->msg.versioned_msg.v0.which_msg==
           bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( idx<decoded->multi.result_cnt );

  bam_types_AtomicTxnBatchResult const * result = &decoded->multi.results[ idx ];
  FD_TEST( result->seq_id==expected->seq_id );
  if( expected->execution_success ) {
    FD_TEST( result->which_result==bam_types_AtomicTxnBatchResult_committed_tag );
    test_bam_committed_results_t const * committed = &decoded->multi.committed[ idx ];
    FD_TEST( committed->txn_cnt==expected->bundle_txn_cnt );
    for( uchar i=0U; i<expected->bundle_txn_cnt; i++ ) {
      bam_types_TransactionCommittedResult const * txn = &committed->txns[ i ];
      FD_TEST( txn->execution_success==( expected->sanitize_success[ i ] && !expected->transaction_err[ i ] ) );
      FD_TEST( txn->cus_consumed==expected->consumed_cus[ i ] );
      FD_TEST( txn->feepayer_balance_lamports==expected->feepayer_balance_lamports[ i ] );
      FD_TEST( txn->loaded_accounts_data_size==expected->loaded_accounts_data_size[ i ] );
    }
  } else {
    FD_TEST( result->which_result==bam_types_AtomicTxnBatchResult_not_committed_tag );
    bam_types_NotCommitted const * not_committed = &result->result.not_committed;
    FD_TEST( not_committed->which_reason!=0U );

    switch( expected->bundle_err ) {
    case FD_BAM_BUNDLE_ERR_NONE:
      break;
    case FD_BAM_BUNDLE_ERR_DESER:
      FD_TEST( not_committed->which_reason==bam_types_NotCommitted_deserialization_error_tag );
      FD_TEST( not_committed->reason.deserialization_error.index==expected->deser_index );
      FD_TEST( not_committed->reason.deserialization_error.reason==
               (bam_types_DeserializationErrorReason)expected->deser_reason );
      return;
    }

    if( FD_UNLIKELY( expected->scheduling_error!=FD_BAM_SCHED_ERR_NONE ) ) {
      FD_TEST( not_committed->which_reason==bam_types_NotCommitted_scheduling_error_tag );
      FD_TEST( not_committed->reason.scheduling_error==
               (bam_types_SchedulingError)expected->scheduling_error );
      return;
    }

    for( uchar i=0U; i<expected->bundle_txn_cnt; i++ ) {
      if( FD_UNLIKELY( !expected->sanitize_success[ i ] ) ) {
        FD_TEST( not_committed->which_reason==bam_types_NotCommitted_deserialization_error_tag );
        FD_TEST( not_committed->reason.deserialization_error.index==i );
        FD_TEST( not_committed->reason.deserialization_error.reason==
                 bam_types_DeserializationErrorReason_SANITIZE_ERROR );
        return;
      }
    }

    if( FD_UNLIKELY( !expected->transaction_err_count ) ) {
      FD_TEST( not_committed->which_reason==bam_types_NotCommitted_generic_invalid_tag );
      FD_TEST( !strcmp( not_committed->reason.generic_invalid.message,
                        FD_BAM_ERR_MSG_BUNDLE_EXECUTION_FAILED ) );
      return;
    }

    uchar err_idx = 0U;
    _Bool found_non_cancelled = 0;
    for( uchar i=0U; i<expected->bundle_txn_cnt; i++ ) {
      if( FD_LIKELY( expected->transaction_err[ i ]!=bam_types_TransactionErrorReason_COMMIT_CANCELLED ) ) {
        err_idx = i;
        found_non_cancelled = 1;
        break;
      }
    }

    if( FD_UNLIKELY( !found_non_cancelled && expected->bundle_txn_cnt>1U ) ) {
      FD_TEST( not_committed->which_reason==bam_types_NotCommitted_scheduling_error_tag );
      FD_TEST( not_committed->reason.scheduling_error==bam_types_SchedulingError_POH_TIMEOUT );
    } else {
      FD_TEST( not_committed->which_reason==bam_types_NotCommitted_transaction_error_tag );
      FD_TEST( not_committed->reason.transaction_error.index==err_idx );
      FD_TEST( not_committed->reason.transaction_error.reason==
               (bam_types_TransactionErrorReason)expected->transaction_err[ err_idx ] );
    }
  }
}

static void
bam_fuzz_drain_leader_state( test_bam_env_t * env ) {
  fd_bam_tile_t * state = env->state;
  if( FD_LIKELY( !state->bam_leader_pending ) ) return;

  bam_fuzz_ensure_scheduler_stream( env );
  int sent = fd_bam_send_leader_state( state, &state->bam_leader_state );
  if( FD_UNLIKELY( !sent ) ) {
    FD_TEST( !state->bam_leader_pending );
    return;
  }

  state->bam_leader_pending = 0U;
  bam_fuzz_assert_outbound_leader_state( state, &state->bam_leader_state );
}

static void
bam_fuzz_drain_result_queue( test_bam_env_t * env,
                             bam_fuzz_state_t * f,
                             ulong            max_cnt ) {
  fd_bam_tile_t * state = env->state;
  bam_fuzz_ensure_scheduler_stream( env );
  bam_fuzz_drain_leader_state( env );

  bam_fuzz_assert_shadow_queue( f, state );
  for( ulong i=0UL; i<max_cnt && state->feedback_queue_depth; i++ ) {
    ushort pending     = state->feedback_queue_depth;
    ushort head_before = state->bam_results_head;

    /* One flush drains up to FD_BAM_RESULTS_PER_MESSAGE queued results into a
       single repeated-result message, so snapshot the whole expected batch
       before flushing. */
    uint expected_cnt = fd_uint_min( (uint)pending, FD_BAM_RESULTS_PER_MESSAGE );
    fd_bam_bundle_result_t expected[ FD_BAM_RESULTS_PER_MESSAGE ];
    for( uint k=0U; k<expected_cnt; k++ ) {
      expected[ k ] = f->durable_results[ ( f->durable_results_head + k ) % FD_BAM_MAX_PENDING_RESULTS ];
    }

    int flush_busy = fd_bam_test_flush_results( state );
    if( FD_UNLIKELY( !flush_busy ) ) {
      bam_fuzz_assert_shadow_queue( f, state );
      return;
    }

    test_bam_decoded_message_t decoded;
    test_bam_decode_last_message( state, &decoded );
    FD_TEST( decoded.multi.result_cnt==(ulong)expected_cnt );
    for( uint k=0U; k<expected_cnt; k++ ) bam_fuzz_assert_decoded_result_at( &expected[ k ], &decoded, k );

    FD_TEST( state->feedback_queue_depth==(ushort)( (uint)pending-expected_cnt ) );
    FD_TEST( state->bam_results_head==(ushort)(( (uint)head_before+expected_cnt ) % FD_BAM_MAX_PENDING_RESULTS) );
    f->durable_results_head  = (ushort)(( (uint)f->durable_results_head + expected_cnt ) % FD_BAM_MAX_PENDING_RESULTS);
    f->durable_results_depth = (ushort)( (uint)f->durable_results_depth - expected_cnt );
    bam_fuzz_assert_shadow_queue( f, state );
  }
}

static void
bam_fuzz_enqueue_synthetic_result( test_bam_env_t *   env,
                                   bam_fuzz_state_t * f,
                                   uchar              b,
                                   uchar              c ) {
  fd_bam_tile_t * state = env->state;
  ushort depth_before = state->feedback_queue_depth;
  ushort tail_before  = state->bam_results_tail;
  ulong drops_before  = state->metrics.feedback_results_dropped_cnt;

  uchar txn_cnt = (uchar)( 1U + (uchar)( c % FD_BAM_MAX_TXN_PER_ATOMIC_BATCH ) );
  fd_bam_bundle_result_t res =
      test_make_bundle_result( bam_fuzz_next_seq( f ),
                               f->current_slot + (ulong)( b & 7U ),
                               txn_cnt );
  res.scheduler_gen = state->scheduler_gen;
  fd_bam_enqueue_result( state, &res );

  if( FD_UNLIKELY( depth_before>=FD_BAM_MAX_PENDING_RESULTS ) ) {
    FD_TEST( state->feedback_queue_depth==depth_before );
    FD_TEST( state->bam_results_tail==tail_before );
    FD_TEST( state->metrics.feedback_results_dropped_cnt==drops_before+1UL );
    f->observed_result_drop = 1U;
    return;
  }

  FD_TEST( state->metrics.feedback_results_dropped_cnt==drops_before );
  FD_TEST( state->feedback_queue_depth==(ushort)( depth_before+1U ) );
  FD_TEST( state->bam_results_tail==(ushort)(( (uint)tail_before+1U ) % FD_BAM_MAX_PENDING_RESULTS) );
  bam_fuzz_shadow_push_result( f, &res );
  bam_fuzz_assert_shadow_queue( f, state );
}

static void
bam_fuzz_force_result_drop( test_bam_env_t *   env,
                            bam_fuzz_state_t * f,
                            uchar              b,
                            uchar              c ) {
  bam_fuzz_ensure_scheduler_stream( env );

  fd_bam_tile_t * state = env->state;
  while( state->feedback_queue_depth<FD_BAM_MAX_PENDING_RESULTS ) {
    bam_fuzz_enqueue_synthetic_result( env, f, b, c );
  }

  ulong drops_before = state->metrics.feedback_results_dropped_cnt;
  bam_fuzz_enqueue_synthetic_result( env, f, b, c );
  FD_TEST( state->metrics.feedback_results_dropped_cnt==drops_before+1UL );
  FD_TEST( state->feedback_queue_depth==FD_BAM_MAX_PENDING_RESULTS );
  f->required_result_drop = 1U;
  FD_TEST( f->observed_result_drop );
  bam_fuzz_assert_shadow_queue( f, state );
}

static void
bam_fuzz_require_outside_slot_path( test_bam_env_t *    env,
                                    bam_fuzz_verify_t * verify,
                                    bam_fuzz_dedup_t *  dedup,
                                    bam_fuzz_resolv_t * resolv,
                                    bam_fuzz_pack_t *   pack,
                                    bam_fuzz_execle_t * execle,
                                    bam_fuzz_state_t *  f,
                                    bam_fuzz_links_t const * links,
                                    uchar               c ) {
  bam_fuzz_ensure_scheduler_stream( env );
  bam_fuzz_drain_pending_txns( env, f, links, verify, dedup, resolv, pack, execle, ULONG_MAX );
  bam_fuzz_drain_result_queue( env, f, ULONG_MAX );

  f->leader_state = (fd_bam_leader_state_t) {
    .slot = f->current_slot,
    .slot_end_ns = g_bam_fuzz_now + (long)1000000L,
    .slot_cu_budget_remaining = 1000000U,
    .tick = (ushort)c,
    .current_slot_has_bam_work = 0U,
  };
  bam_fuzz_sync_pack_leader( env, f, links, resolv, pack, execle );

  f->observed_outside_slot = 0U;
  bam_fuzz_batch_spec_t spec = {
    .seq_id            = bam_fuzz_next_seq( f ),
    .max_schedule_slot = f->current_slot ? f->current_slot-1UL : 0UL,
    .txn_cnt           = 1U,
    .revert_on_error   = 0U,
  };
  spec.payload_seed[ 0 ] = (uint)c;
  bam_fuzz_deliver_specs( env, f, links, verify, dedup, resolv, pack, execle, &spec, 1UL, 0UL );
  f->required_outside_slot = 1U;
  FD_TEST( f->observed_outside_slot );
}

static void
bam_fuzz_require_txn_error_path( test_bam_env_t *    env,
                                 bam_fuzz_verify_t * verify,
                                 bam_fuzz_dedup_t *  dedup,
                                 bam_fuzz_resolv_t * resolv,
                                 bam_fuzz_pack_t *   pack,
                                 bam_fuzz_execle_t * execle,
                                 bam_fuzz_state_t *  f,
                                 bam_fuzz_links_t const * links,
                                 uchar               c ) {
  bam_fuzz_ensure_scheduler_stream( env );
  bam_fuzz_drain_pending_txns( env, f, links, verify, dedup, resolv, pack, execle, ULONG_MAX );
  bam_fuzz_drain_result_queue( env, f, ULONG_MAX );

  if( FD_UNLIKELY( f->leader_state.slot==ULONG_MAX ) ) {
    f->leader_state = (fd_bam_leader_state_t) {
      .slot = f->current_slot,
      .slot_end_ns = g_bam_fuzz_now + (long)1000000L,
      .slot_cu_budget_remaining = 1000000U,
      .tick = (ushort)c,
      .current_slot_has_bam_work = 0U,
    };
    bam_fuzz_sync_pack_leader( env, f, links, resolv, pack, execle );
  }

  f->observed_txn_error = 0U;
  bam_fuzz_batch_spec_t specs[ BAM_FUZZ_MAX_BATCHES ];
  ulong spec_cnt = 0UL;
  bam_fuzz_make_batch_specs( f, specs, &spec_cnt, 0x42U, 0x43U, c );
  bam_fuzz_deliver_specs( env, f, links, verify, dedup, resolv, pack, execle, specs, spec_cnt, ULONG_MAX );
  bam_fuzz_drain_pending_txns( env, f, links, verify, dedup, resolv, pack, execle, ULONG_MAX );
  bam_fuzz_pump_pack( env, f, links, pack, execle, 16UL );

  f->required_txn_error = 1U;
  FD_TEST( f->observed_txn_error );
}

static void
bam_fuzz_deliver_result_burst( test_bam_env_t *    env,
                               bam_fuzz_verify_t * verify,
                               bam_fuzz_dedup_t *  dedup,
                               bam_fuzz_resolv_t * resolv,
                               bam_fuzz_pack_t *   pack,
                               bam_fuzz_execle_t * execle,
                               bam_fuzz_state_t *  f,
                               bam_fuzz_links_t const * links,
                               uchar               a,
                               uchar               b,
                               uchar               c ) {
  fd_bam_tile_t * state = env->state;
  if( FD_UNLIKELY( a & 0x80U ) ) {
    bam_fuzz_force_result_drop( env, f, b, c );
    if( FD_UNLIKELY( a & 0x40U ) ) {
      bam_fuzz_reset_preserving_feedback( f, state );
      bam_fuzz_ensure_scheduler_stream( env );
    }
    return;
  }

  if( FD_LIKELY( !( a & 0x20U ) && f->leader_state.slot==ULONG_MAX ) ) {
    f->leader_state = (fd_bam_leader_state_t) {
      .slot = f->current_slot,
      .slot_end_ns = g_bam_fuzz_now + (long)1000000L,
      .slot_cu_budget_remaining = 1000000U,
      .tick = (ushort)c,
      .current_slot_has_bam_work = 0U,
    };
    bam_fuzz_sync_pack_leader( env, f, links, resolv, pack, execle );
  }

  bam_fuzz_ensure_scheduler_stream( env );
  ulong burst_cnt = 1UL + (ulong)( a & 7U );
  for( ulong i=0UL; i<burst_cnt && state->feedback_queue_depth<FD_BAM_MAX_PENDING_RESULTS; i++ ) {
    uchar variant = (uchar)( b + (uchar)i );
    _Bool stale = !!( variant & 2U );
    _Bool revert_on_error = !!( variant & 1U );
    uchar txn_cnt = revert_on_error
                  ? (uchar)( 1U + (uchar)(( (uint)c + (uint)i ) % FD_BAM_MAX_TXN_PER_ATOMIC_BATCH) )
                  : 1U;

    bam_fuzz_batch_spec_t spec = {
      .seq_id            = bam_fuzz_next_seq( f ),
      .max_schedule_slot = stale && f->current_slot ? f->current_slot-1UL : f->current_slot + (ulong)((c>>5) & 3U),
      .txn_cnt           = txn_cnt,
      .revert_on_error   = (uchar)revert_on_error,
    };
    for( uchar j=0U; j<txn_cnt; j++ ) spec.payload_seed[ j ] = (uint)c + 31U*(uint)i + 7U*(uint)j;
    ulong drain_credit = ( variant & 0x40U ) ? 0UL : 1UL;
    bam_fuzz_deliver_specs( env, f, links, verify, dedup, resolv, pack, execle, &spec, 1UL, drain_credit );
  }

  if( FD_UNLIKELY( a & 0x40U ) ) {
    bam_fuzz_reset_preserving_feedback( f, state );
    bam_fuzz_ensure_scheduler_stream( env );
  }
}

static void
bam_fuzz_apply_event( test_bam_env_t *    env,
                      bam_fuzz_verify_t * verify,
                      bam_fuzz_dedup_t *  dedup,
                      bam_fuzz_resolv_t * resolv,
                      bam_fuzz_pack_t *   pack,
                      bam_fuzz_execle_t * execle,
                      bam_fuzz_state_t *  f,
                      bam_fuzz_links_t const * links,
                      bam_evt_kind_t      kind,
                      uchar               a,
                      uchar               b,
                      uchar               c ) {
  fd_bam_tile_t * state = env->state;

  switch( kind ) {
    case BAM_EVT_SEND_BATCH: {
      bam_fuzz_ensure_scheduler_stream( env );
      bam_fuzz_batch_spec_t specs[ BAM_FUZZ_MAX_BATCHES ];
      ulong spec_cnt = 0UL;
      bam_fuzz_make_batch_specs( f, specs, &spec_cnt, a, b, c );
      fd_memcpy( f->last_batch, specs, spec_cnt*sizeof(bam_fuzz_batch_spec_t) );
      f->last_batch_cnt = spec_cnt;
      f->has_last_batch = 1U;
      ulong drain_credit = ( b & 0x40U ) ? 0UL : 1UL + (ulong)( c & 3U );
      bam_fuzz_deliver_specs( env, f, links, verify, dedup, resolv, pack, execle, specs, spec_cnt, drain_credit );
      break;
    }
    case BAM_EVT_REPLAY:
      bam_fuzz_ensure_last_batch( f, a, b, c );
      bam_fuzz_ensure_scheduler_stream( env );
      bam_fuzz_deliver_specs( env, f, links, verify, dedup, resolv, pack, execle,
                              f->last_batch, f->last_batch_cnt, ( a & 0x40U ) ? 0UL : 1UL );
      break;
    case BAM_EVT_DUP_SEQ: {
      bam_fuzz_ensure_scheduler_stream( env );
      bam_fuzz_ensure_last_batch( f, a, b, c );

      bam_fuzz_batch_spec_t dup = f->last_batch[ 0 ];
      dup.max_schedule_slot = (b & 0x20U) && f->current_slot ? f->current_slot-1UL : f->current_slot;
      dup.revert_on_error   = (uchar)!!( a & 1U );
      dup.txn_cnt           = dup.revert_on_error
                            ? (uchar)( 1U + (uchar)((b>>1) % FD_BAM_MAX_TXN_PER_ATOMIC_BATCH) )
                            : 1U;
      if( FD_UNLIKELY( c & 0x80U ) ) {
        dup.revert_on_error = 1U;
        dup.txn_cnt = (uchar)( 2U + (uchar)((b>>1) % (FD_BAM_MAX_TXN_PER_ATOMIC_BATCH-1U)) );
      }
      for( uchar i=0U; i<dup.txn_cnt; i++ ) dup.payload_seed[ i ] = (uint)c + 5U*(uint)i;
      bam_fuzz_deliver_specs( env, f, links, verify, dedup, resolv, pack, execle,
                              &dup, 1UL, ( c & 0x80U ) ? 1UL : ( ( a & 0x40U ) ? 0UL : 1UL ) );
      if( FD_UNLIKELY( c & 0x80U ) ) {
        bam_fuzz_batch_spec_t switched = dup;
        switched.seq_id = bam_fuzz_next_seq( f );
        for( uchar i=0U; i<switched.txn_cnt; i++ ) switched.payload_seed[ i ] += 11U;
        fd_memcpy( f->last_batch, &switched, sizeof(switched) );
        f->last_batch_cnt = 1UL;
        f->has_last_batch = 1U;
        bam_fuzz_deliver_specs( env, f, links, verify, dedup, resolv, pack, execle,
                                &switched, 1UL, ( a & 0x40U ) ? 0UL : 1UL );
      }
      break;
    }
    case BAM_EVT_NEW_SEQ_SAME_PAYLOAD: {
      bam_fuzz_ensure_scheduler_stream( env );
      bam_fuzz_ensure_last_batch( f, a, b, c );

      bam_fuzz_batch_spec_t specs[ BAM_FUZZ_MAX_BATCHES ];
      fd_memcpy( specs, f->last_batch, f->last_batch_cnt*sizeof(bam_fuzz_batch_spec_t) );
      for( ulong i=0UL; i<f->last_batch_cnt; i++ ) {
        specs[ i ].seq_id = bam_fuzz_next_seq( f );
        if( FD_UNLIKELY( a & 0x80U ) ) specs[ i ].max_schedule_slot = f->current_slot;
      }
      fd_memcpy( f->last_batch, specs, f->last_batch_cnt*sizeof(bam_fuzz_batch_spec_t) );
      bam_fuzz_deliver_specs( env, f, links, verify, dedup, resolv, pack, execle,
                              specs, f->last_batch_cnt, ( b & 0x40U ) ? 0UL : 1UL );
      break;
    }
    case BAM_EVT_ADVANCE_SLOT: {
      ulong delta = 1UL + (ulong)( a & 3U );
      f->current_slot += delta;
      g_bam_fuzz_now += (long)( delta * 1000000UL );
      if( FD_LIKELY( !( b & 0x80U ) ) ) {
        f->leader_state = (fd_bam_leader_state_t) {
          .slot = f->current_slot,
          .slot_end_ns = g_bam_fuzz_now + (long)1000000L,
          .slot_cu_budget_remaining = 1000000U - (uint)( c * 1000U ),
          .tick = (ushort)( a + b ),
          .current_slot_has_bam_work = 0U,
        };
      } else {
        f->leader_state = (fd_bam_leader_state_t){ .slot = ULONG_MAX };
      }
      bam_fuzz_sync_pack_leader( env, f, links, resolv, pack, execle );
      break;
    }
    case BAM_EVT_LEADER_OFF:
      f->leader_state = (fd_bam_leader_state_t){ .slot = ULONG_MAX };
      bam_fuzz_sync_pack_leader( env, f, links, resolv, pack, execle );
      break;
    case BAM_EVT_LEADER_ON:
      f->leader_state = (fd_bam_leader_state_t) {
        .slot = f->current_slot,
        .slot_end_ns = g_bam_fuzz_now + (long)( 1000000UL + (ulong)a*1000UL ),
        .slot_cu_budget_remaining = 1000000U,
        .tick = (ushort)b,
        .current_slot_has_bam_work = 0U,
      };
      bam_fuzz_sync_pack_leader( env, f, links, resolv, pack, execle );
      break;
    case BAM_EVT_DISCONNECT:
      bam_fuzz_reset_preserving_feedback( f, state );
      break;
    case BAM_EVT_RECONNECT:
      bam_fuzz_ensure_scheduler_stream( env );
      break;
    case BAM_EVT_RESULT_BURST:
      bam_fuzz_deliver_result_burst( env, verify, dedup, resolv, pack, execle, f, links, a, b, c );
      break;
    case BAM_EVT_DRAIN_QUEUE:
      bam_fuzz_drain_pending_txns( env, f, links, verify, dedup, resolv, pack, execle,
                                   1UL + (ulong)( b & 7U ) );
      bam_fuzz_drain_result_queue( env, f, 1UL + (ulong)( a & 15U ) );
      if( FD_UNLIKELY( c & 0x80U ) ) {
        uchar coverage = (uchar)( c & 0x7fU );
        if( FD_UNLIKELY( coverage & 0x01U ) ) {
          bam_fuzz_require_outside_slot_path( env, verify, dedup, resolv, pack, execle, f, links, b );
        }
        if( FD_UNLIKELY( coverage & 0x02U ) ) {
          bam_fuzz_require_txn_error_path( env, verify, dedup, resolv, pack, execle, f, links, 0x37U );
        }
        if( FD_UNLIKELY( coverage & 0x08U ) ) {
          bam_fuzz_require_txn_error_path( env, verify, dedup, resolv, pack, execle, f, links, 0x32U );
        }
        if( FD_UNLIKELY( coverage & 0x04U ) ) {
          bam_fuzz_force_result_drop( env, f, a, b );
        }
      }
      break;
  }

  bam_fuzz_assert_invariants( env, f );
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

  char const * _page_sz = fd_env_strip_cmdline_cstr ( argc, argv, "--page-sz", NULL, NULL                         );
  ulong        page_cnt = fd_env_strip_cmdline_ulong( argc, argv, "--page-cnt", NULL, 1048576UL                    );
  ulong        numa_idx = fd_env_strip_cmdline_ulong( argc, argv, "--numa-idx", NULL, fd_shmem_numa_idx( cpu_idx ) );

  g_bam_fuzz_wksp = _page_sz
      ? fd_wksp_new_anonymous( fd_cstr_to_shmem_page_sz( _page_sz ),
                               page_cnt,
                               fd_shmem_cpu_idx( numa_idx ),
                               "bam-pipeline-fuzz",
                               512UL )
      : bam_fuzz_wksp_new_lazy( BAM_FUZZ_WKSP_FOOTPRINT );
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

  g_bam_fuzz_now = 1000000000L + (long)( seed & 0xffffU );

  test_bam_env_t env[1];
  test_bam_env_create( env, g_bam_fuzz_wksp );
  bam_fuzz_links_t links[1];
  fd_memset( links, 0, sizeof(*links) );

  bam_fuzz_verify_t * verify = bam_fuzz_verify_new( g_bam_fuzz_wksp,
                                                    env->state->verify_out.mem,
                                                    env->state->verify_out.chunk0,
                                                    env->state->verify_out.wmark,
                                                    &links->verify_out );
  bam_fuzz_dedup_t * dedup = bam_fuzz_dedup_new( g_bam_fuzz_wksp,
                                                 links->verify_out.mem,
                                                 links->verify_out.chunk0,
                                                 links->verify_out.wmark,
                                                 &links->dedup_out );
  bam_fuzz_resolv_t * resolv = bam_fuzz_resolv_new( g_bam_fuzz_wksp,
                                                    links->dedup_out.mem,
                                                    links->dedup_out.chunk0,
                                                    links->dedup_out.wmark,
                                                    &links->resolv_pack );
  bam_fuzz_pack_t * pack = bam_fuzz_pack_new( g_bam_fuzz_wksp,
                                              links->resolv_pack.mem,
                                              links->resolv_pack.chunk0,
                                              links->resolv_pack.wmark,
                                              &links->pack_execle,
                                              &links->pack_bam_leader,
                                              &links->pack_bam_result,
                                              &links->pack_execle_busy_fseq );
  bam_fuzz_execle_t * execle = bam_fuzz_execle_new( g_bam_fuzz_wksp,
                                                    links->pack_execle.mem,
                                                    links->pack_execle.chunk0,
                                                    links->pack_execle.wmark,
                                                    links->pack_execle_busy_fseq,
                                                    &links->execle_bank_bam );
  env->state->bank_bam_in_idx = BAM_FUZZ_BANK_IN_BASE_IDX;
  env->state->bank_bam_in_cnt = 1UL;
  env->state->bank_in[ BAM_FUZZ_BANK_IN_EXECLE_IDX ] = (fd_bam_in_ctx_t) {
    .mem    = links->execle_bank_bam.mem,
    .chunk0 = links->execle_bank_bam.chunk0,
    .wmark  = links->execle_bank_bam.wmark,
  };
  env->state->pack_bam_leader_in_idx = 1UL;
  env->state->pack_leader_in = (fd_bam_in_ctx_t) {
    .mem    = links->pack_bam_leader.mem,
    .chunk0 = links->pack_bam_leader.chunk0,
    .wmark  = links->pack_bam_leader.wmark,
  };
  env->state->pack_bam_result_in_idx = 2UL;
  env->state->pack_result_in = (fd_bam_in_ctx_t) {
    .mem    = links->pack_bam_result.mem,
    .chunk0 = links->pack_bam_result.chunk0,
    .wmark  = links->pack_bam_result.wmark,
  };

  bam_fuzz_state_t f[1];
  fd_memset( f, 0, sizeof(*f) );
  f->next_seq = ( seed << 1 ) | 1U;
  f->current_slot = 100UL + (ulong)( seed & 31U );

  f->leader_state = (fd_bam_leader_state_t) {
    .slot = f->current_slot,
    .slot_end_ns = g_bam_fuzz_now + (long)1000000L,
    .slot_cu_budget_remaining = 1000000U,
    .tick = 0U,
    .current_slot_has_bam_work = 0U,
  };
  bam_fuzz_sync_pack_leader( env, f, links, resolv, pack, execle );

  bam_fuzz_ensure_scheduler_stream( env );

  for( ulong i=0UL, event_cnt=0UL; i+4UL<=size && event_cnt<BAM_FUZZ_MAX_EVENTS; event_cnt++ ) {
    uchar k = data[ i++ ];
    uchar a = data[ i++ ];
    uchar b = data[ i++ ];
    uchar c = data[ i++ ];

    if( FD_LIKELY( k<BAM_EVT_KIND_CNT ) ) {
      bam_fuzz_apply_event( env, verify, dedup, resolv, pack, execle, f, links, (bam_evt_kind_t)k, a, b, c );
    }
  }

  bam_fuzz_ensure_scheduler_stream( env );
  bam_fuzz_sync_pack_leader( env, f, links, resolv, pack, execle );
  bam_fuzz_drain_pending_txns( env, f, links, verify, dedup, resolv, pack, execle, ULONG_MAX );
  bam_fuzz_pump_pack( env, f, links, pack, execle, 8UL );
  bam_fuzz_drain_result_queue( env, f, ULONG_MAX );
  bam_fuzz_assert_invariants( env, f );

  bam_fuzz_execle_delete( execle );
  bam_fuzz_pack_delete( pack );
  bam_fuzz_resolv_delete( resolv );
  bam_fuzz_dedup_delete( dedup );
  bam_fuzz_verify_delete( verify );
  test_bam_env_destroy( env );

  fd_wksp_usage_t usage;
  FD_TEST( fd_wksp_usage( g_bam_fuzz_wksp, NULL, 0UL, &usage ) );
  FD_TEST( usage.free_cnt == usage.total_cnt );
  return 0;
}
