#include "../../ballet/fd_ballet.h"
#include "../../ballet/txn/fd_compact_u16.h"
#include "../metrics/fd_metrics.h"

#include <stddef.h>
#include <stdlib.h>

#if FD_USING_GCC && __GNUC__ >= 15
#pragma GCC diagnostic ignored "-Wunterminated-string-initialization"
#endif

FD_IMPORT_BINARY( test_pack_tile_sample_vote, "src/disco/pack/sample_vote.bin" );
FD_IMPORT_BINARY( test_pack_tile_non_vote,    "src/ballet/txn/fixtures/transaction2.bin" );

/* Stub fd_pack entry points so BAM tile tests can exercise pack-tile
   bookkeeping without building a full fd_pack instance. */
#define fd_pack_delete_transaction test_fd_pack_delete_transaction
#define fd_pack_insert_txn_init test_fd_pack_insert_txn_init
#define fd_pack_insert_txn_fini test_fd_pack_insert_txn_fini
#define fd_pack_insert_txn_cancel test_fd_pack_insert_txn_cancel
#define fd_pack_insert_bundle_fini test_fd_pack_insert_bundle_fini
#define fd_pack_insert_bundle_cancel test_fd_pack_insert_bundle_cancel
#include "fd_pack_tile.c"
#undef fd_pack_insert_bundle_cancel
#undef fd_pack_insert_bundle_fini
#undef fd_pack_insert_txn_cancel
#undef fd_pack_insert_txn_fini
#undef fd_pack_insert_txn_init
#undef fd_pack_delete_transaction

int fd_pack_insert_bundle_fini( fd_pack_t *, fd_txn_e_t * const *, ulong, ulong,
                                int, void const *, ulong *, ulong * );

#include "../bam/test_bam_common.c"

__attribute__((weak)) char const fdctl_version_string[] = "0.0.0";

static uchar metrics_scratch[ FD_METRICS_FOOTPRINT( 0UL ) ] __attribute__((aligned( FD_METRICS_ALIGN )));

static ulong             test_delete_call_cnt;
static fd_ed25519_sig_t  test_delete_last_sig[1];
static ulong             test_bundle_cancel_call_cnt;
static ulong             test_bundle_cancel_last_txn_cnt;
static ulong             test_insert_fini_call_cnt;
static int               test_insert_fini_result;
static int               test_insert_fini_use_real_pack;
static int               test_delete_before_insert_fini;
static ulong             test_insert_txn_init_call_cnt;
static ulong             test_insert_txn_fini_call_cnt;
static ulong             test_insert_txn_cancel_call_cnt;
static fd_txn_e_t        test_insert_txn_slot[1];

#define TEST_PACK_TILE_DCACHE_CHUNKS 16UL
#define TEST_PACK_TILE_MCACHE_DEPTH  16UL
#define TEST_PACK_TILE_BAM_WORK_CAP   8UL
#define TEST_D18_INSTR_ACCT_CNT       (FD_PACK_MAX_ACCOUNTS_PER_INSTRUCTION+1UL)

ulong
test_fd_pack_delete_transaction( fd_pack_t *                 pack,
                                 fd_ed25519_sig_t const *    sig0 ) {
  (void)pack;
  test_delete_call_cnt++;
  if( FD_UNLIKELY( !sig0 ) ) return 1UL;
  fd_memcpy( test_delete_last_sig, sig0, sizeof(fd_ed25519_sig_t) );
  return 1UL;
}

fd_txn_e_t *
test_fd_pack_insert_txn_init( fd_pack_t * pack ) {
  (void)pack;
  test_insert_txn_init_call_cnt++;
  fd_memset( test_insert_txn_slot, 0, sizeof(test_insert_txn_slot) );
  return test_insert_txn_slot;
}

int
test_fd_pack_insert_txn_fini( fd_pack_t  * pack,
                              fd_txn_e_t * txn,
                              ulong        expires_at,
                              ulong *      delete_cnt ) {
  (void)pack;
  (void)expires_at;
  FD_TEST( txn==test_insert_txn_slot );
  test_insert_txn_fini_call_cnt++;
  *delete_cnt = 0UL;
  return fd_txn_is_simple_vote_transaction( TXN( txn->txnp ), txn->txnp->payload )
         ? FD_PACK_INSERT_ACCEPT_VOTE_ADD
         : FD_PACK_INSERT_ACCEPT_NONVOTE_ADD;
}

void
test_fd_pack_insert_txn_cancel( fd_pack_t *  pack,
                                fd_txn_e_t * txn ) {
  (void)pack;
  (void)txn;
  test_insert_txn_cancel_call_cnt++;
}

int
test_fd_pack_insert_bundle_fini( fd_pack_t          * pack,
                                 fd_txn_e_t * const * bundle,
                                 ulong                txn_cnt,
                                 ulong                expires_at,
                                 int                  initializer_bundle,
                                 void const *         bundle_meta,
                                 ulong *              delete_cnt,
                                 ulong *              reject_txn_idx ) {
  (void)pack;
  (void)bundle;
  (void)txn_cnt;
  (void)expires_at;
  (void)initializer_bundle;
  (void)bundle_meta;
  test_insert_fini_call_cnt++;
  test_delete_before_insert_fini = !!test_delete_call_cnt;
  if( FD_UNLIKELY( test_insert_fini_use_real_pack ) ) {
    return fd_pack_insert_bundle_fini( pack,
                                       bundle,
                                       txn_cnt,
                                       expires_at,
                                       initializer_bundle,
                                       bundle_meta,
                                       delete_cnt,
                                       reject_txn_idx );
  }
  *delete_cnt = 0UL;
  if( reject_txn_idx ) *reject_txn_idx = ULONG_MAX;
  return test_insert_fini_result;
}

void
test_fd_pack_insert_bundle_cancel( fd_pack_t *          pack,
                                   fd_txn_e_t * const * bundle,
                                   ulong                txn_cnt ) {
  (void)pack;
  (void)bundle;
  test_bundle_cancel_call_cnt++;
  test_bundle_cancel_last_txn_cnt = txn_cnt;
}

typedef struct {
  fd_frag_meta_t *  mcache;
  uchar *           dcache;
  fd_frag_meta_t *  mcaches[ 1 ];
  ulong             seqs[ 1 ];
  ulong             depths[ 1 ];
  ulong             cr_avail[ 1 ];
  ulong             min_cr_avail;
  int              out_reliable[ 1 ];
  fd_stem_context_t stem;
} test_pack_tile_out_t;

typedef struct {
  uchar          pad[ FD_PACK_PENDING_TXN_CNT_OFF ];
  ulong          pending_txn_cnt;
  pack_bam_work_t bam_work[ TEST_PACK_TILE_BAM_WORK_CAP ];
  fd_bam_bundle_result_t bam_result_queue[ 2UL*TEST_PACK_TILE_BAM_WORK_CAP ];
} test_fake_pack_t;

FD_STATIC_ASSERT( offsetof( test_fake_pack_t, pending_txn_cnt )==FD_PACK_PENDING_TXN_CNT_OFF,
                  test_fake_pack_pending_txn_cnt_off );

typedef struct {
  test_pack_tile_out_t out[1];
  test_fake_pack_t     fake_pack[1];
  fd_bam_fee_cfg_t     fee_cfg[1];
  fd_pack_ctx_t        ctx[1];
} test_pack_tile_harness_t;

static void
test_pack_tile_out_new( test_pack_tile_out_t * out ) {
  fd_memset( out, 0, sizeof(*out) );

  out->mcache = aligned_alloc( alignof(fd_frag_meta_t), TEST_PACK_TILE_MCACHE_DEPTH * sizeof(fd_frag_meta_t) );
  FD_TEST( out->mcache );
  fd_memset( out->mcache, 0, TEST_PACK_TILE_MCACHE_DEPTH * sizeof(fd_frag_meta_t) );

  out->dcache = aligned_alloc( FD_CHUNK_ALIGN, TEST_PACK_TILE_DCACHE_CHUNKS * FD_CHUNK_SZ );
  FD_TEST( out->dcache );
  fd_memset( out->dcache, 0, TEST_PACK_TILE_DCACHE_CHUNKS * FD_CHUNK_SZ );

  out->mcaches[ 0 ]      = out->mcache;
  out->seqs[ 0 ]         = 0UL;
  out->depths[ 0 ]       = TEST_PACK_TILE_MCACHE_DEPTH;
  out->cr_avail[ 0 ]     = ULONG_MAX;
  out->min_cr_avail      = ULONG_MAX;
  out->out_reliable[ 0 ] = 1;
  out->stem = (fd_stem_context_t){
    .mcaches             = out->mcaches,
    .seqs                = out->seqs,
    .depths              = out->depths,
    .cr_avail            = out->cr_avail,
    .min_cr_avail        = &out->min_cr_avail,
    .cr_decrement_amount = 0UL,
    .out_reliable        = out->out_reliable,
  };
}

static void
test_pack_tile_out_delete( test_pack_tile_out_t * out ) {
  free( out->dcache );
  free( out->mcache );
  fd_memset( out, 0, sizeof(*out) );
}

static fd_bam_bundle_result_t const *
test_pack_tile_last_result( test_pack_tile_harness_t const * h ) {
  test_pack_tile_out_t const * out = h->out;
  FD_TEST( out->seqs[ 0 ] > 0UL );
  fd_frag_meta_t const * meta = &out->mcache[ fd_mcache_line_idx( out->seqs[ 0 ] - 1UL, out->depths[ 0 ] ) ];
  return (fd_bam_bundle_result_t const *)fd_chunk_to_laddr_const( out->dcache, meta->chunk );
}

static pack_bam_out_ctx_t
test_pack_tile_result_out( test_pack_tile_out_t const * out ) {
  fd_wksp_t * mem    = (fd_wksp_t *)out->dcache;
  ulong       chunk0 = fd_dcache_compact_chunk0( mem, out->dcache );
  return (pack_bam_out_ctx_t){
    .idx    = 0UL,
    .mem    = mem,
    .chunk0 = chunk0,
    .wmark  = fd_dcache_compact_wmark( mem, out->dcache, sizeof(fd_bam_bundle_result_t) ),
    .chunk  = chunk0,
  };
}

static fd_bam_bundle_result_t const *
test_pack_tile_assert_last_result( test_pack_tile_harness_t const * h,
                                   uint                             seq_id,
                                   ulong                            slot,
                                   uchar                            txn_cnt,
                                   uint                             scheduling_error,
                                   uchar                            transaction_err_count ) {
  fd_bam_bundle_result_t const * res = test_pack_tile_last_result( h );
  FD_TEST( res->seq_id == seq_id );
  FD_TEST( res->slot == slot );
  FD_TEST( res->bundle_txn_cnt == txn_cnt );
  FD_TEST( res->scheduling_error == scheduling_error );
  FD_TEST( res->transaction_err_count == transaction_err_count );
  return res;
}

static void
test_pack_tile_assert_deleted_sig( void const * expected ) {
  FD_TEST( test_delete_call_cnt == 1UL );
  FD_TEST( 0 == memcmp( test_delete_last_sig, expected, sizeof(fd_ed25519_sig_t) ) );
}

static void
test_pack_tile_harness_new( test_pack_tile_harness_t * h ) {
  fd_memset( h, 0, sizeof(*h) );
  test_delete_call_cnt = 0UL;
  fd_memset( test_delete_last_sig, 0, sizeof(test_delete_last_sig) );
  test_bundle_cancel_call_cnt     = 0UL;
  test_bundle_cancel_last_txn_cnt = 0UL;
  test_insert_fini_call_cnt       = 0UL;
  test_insert_fini_result         = FD_PACK_INSERT_ACCEPT_NONVOTE_ADD;
  test_insert_fini_use_real_pack  = 0;
  test_delete_before_insert_fini  = 0;
  test_insert_txn_init_call_cnt   = 0UL;
  test_insert_txn_fini_call_cnt   = 0UL;
  test_insert_txn_cancel_call_cnt = 0UL;
  fd_memset( test_insert_txn_slot, 0, sizeof(test_insert_txn_slot) );
  test_pack_tile_out_new( h->out );

  for( ulong i=0UL; i<FD_PACK_BAM_RECENT_SLOT_CNT; i++ ) h->ctx->bam_recent_slot[ i ].slot = ULONG_MAX;

  h->ctx->pack                     = fd_type_pun( h->fake_pack );
  h->ctx->bam_work                 = h->fake_pack->bam_work;
  h->ctx->bam_result_queue         = h->fake_pack->bam_result_queue;
  h->ctx->max_pending_transactions = TEST_PACK_TILE_BAM_WORK_CAP;
  h->ctx->approx_wallclock_ns      = 1000L;
  h->ctx->ticks_per_ns             = 0.;
  h->ctx->leader_slot              = ULONG_MAX;
  h->ctx->highest_observed_slot    = 0UL;
  h->ctx->bam_result_out           = test_pack_tile_result_out( h->out );
  h->ctx->bam_fee_cfg              = h->fee_cfg;
}

static void
test_pack_tile_harness_delete( test_pack_tile_harness_t * h ) {
  test_pack_tile_out_delete( h->out );
  fd_memset( h, 0, sizeof(*h) );
}

static void
test_pack_tile_bam_fee_meta_seqlock_keeps_last_snapshot( void ) {
  test_pack_tile_harness_t h[1];
  test_pack_tile_harness_new( h );

  fd_bam_fee_cfg_t cfg[1];
  fd_memset( cfg, 0, sizeof(cfg) );
  h->ctx->bam_fee_cfg = cfg;
  pack_tile_refresh_bam_fee_meta( h->ctx );

  uchar builder_pubkey0[ 32 ];
  uchar builder_pubkey1[ 32 ];
  for( ulong i=0UL; i<sizeof(builder_pubkey0); i++ ) {
    builder_pubkey0[ i ] = (uchar)( 0x10U + i );
    builder_pubkey1[ i ] = (uchar)( 0x80U + i );
  }

  fd_memcpy( cfg->builder_pubkey, builder_pubkey0, sizeof(builder_pubkey0) );
  cfg->builder_commission = 35U;
  cfg->version            = 1U;
  pack_tile_refresh_bam_fee_meta( h->ctx );
  FD_TEST( h->ctx->bam_fee_cfg_version == 1U );
  FD_TEST( h->ctx->bam_fee_meta->commission == 35UL );
  FD_TEST( 0==memcmp( h->ctx->bam_fee_meta->commission_pubkey->b, builder_pubkey0, sizeof(builder_pubkey0) ) );

  fd_memcpy( cfg->builder_pubkey, builder_pubkey1, sizeof(builder_pubkey1) );
  cfg->builder_commission = 99U;
  cfg->version            = fd_uint_set_bit( 1U, 31 );
  pack_tile_refresh_bam_fee_meta( h->ctx );
  FD_TEST( h->ctx->bam_fee_cfg_version == 1U );
  FD_TEST( h->ctx->bam_fee_meta->commission == 35UL );
  FD_TEST( 0==memcmp( h->ctx->bam_fee_meta->commission_pubkey->b, builder_pubkey0, sizeof(builder_pubkey0) ) );

  cfg->version = 2U;
  pack_tile_refresh_bam_fee_meta( h->ctx );
  FD_TEST( h->ctx->bam_fee_cfg_version == 2U );
  FD_TEST( h->ctx->bam_fee_meta->commission == 99UL );
  FD_TEST( 0==memcmp( h->ctx->bam_fee_meta->commission_pubkey->b, builder_pubkey1, sizeof(builder_pubkey1) ) );

  fd_memset( cfg->builder_pubkey, 0, sizeof(cfg->builder_pubkey) );
  cfg->version = 3U;
  pack_tile_refresh_bam_fee_meta( h->ctx );
  FD_TEST( h->ctx->bam_fee_cfg_version == 3U );
  FD_TEST( h->ctx->bam_fee_meta->commission == 99UL );
  FD_TEST( fd_mem_iszero( h->ctx->bam_fee_meta->commission_pubkey->b, sizeof(builder_pubkey1) ) );

  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_fill_sig( uchar sig[ static FD_ED25519_SIG_SZ ],
                         uchar seed ) {
  for( ulong i=0UL; i<sizeof(fd_ed25519_sig_t); i++ ) sig[ i ] = (uchar)( seed + i );
}

static pack_bam_work_t *
test_pack_tile_mark_bam_work_scheduled( test_pack_tile_harness_t * h,
                                        void const *                sig0 ) {
  ulong work_idx = pack_tile_bam_work_find_by_sig0( h->ctx, sig0 );
  FD_TEST( work_idx<h->ctx->bam_work_cnt );

  pack_bam_work_t * item = &h->ctx->bam_work[ work_idx ];
  FD_TEST( item->state==PACK_BAM_WORK_STATE_PENDING );

  h->ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_SCHEDULED_IDX ]++;
  item->state             = PACK_BAM_WORK_STATE_SCHEDULED;
  item->remaining_txn_cnt = item->txn_cnt;
  h->ctx->bam_pending_work_cnt--;
  h->ctx->bam_scheduled_work_cnt++;

  return item;
}

static void
test_pack_tile_send_executed_txn( test_pack_tile_harness_t * h,
                                  void const *                txn_sig,
                                  ulong                       event_kind ) {
  h->ctx->in_kind[ 0 ] = IN_KIND_EXECUTED_TXN;
  fd_memcpy( h->ctx->executed_txn_sig, txn_sig, FD_TXN_SIGNATURE_SZ );
  after_frag( h->ctx, 0UL, 0UL, event_kind, FD_TXN_SIGNATURE_SZ, 0UL, 0UL, &h->out->stem );
}

static void
test_pack_tile_bam_completion_outcomes( void ) {
  struct {
    ulong event_kind[ 3 ];
    ulong completed_stage;
    ulong delete_call_cnt;
    uchar txn_cnt;
  } const cases[] = {
    {
      { FD_EXECUTED_TXN_KIND_LANDED, FD_EXECUTED_TXN_KIND_LANDED, 0UL },
      FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_LANDED_IDX, 2UL, 2U
    },
    {
      { FD_EXECUTED_TXN_KIND_LANDED, FD_EXECUTED_TXN_KIND_BAM_COMPLETED_UNLANDED, FD_EXECUTED_TXN_KIND_LANDED },
      FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_COMPLETED_UNLANDED_IDX, 2UL, 3U
    },
    {
      { FD_EXECUTED_TXN_KIND_BAM_COMPLETED_UNLANDED, FD_EXECUTED_TXN_KIND_BAM_COMPLETED_UNLANDED, FD_EXECUTED_TXN_KIND_BAM_COMPLETED_UNLANDED },
      FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_COMPLETED_UNLANDED_IDX, 0UL, 3U
    },
  };

  for( ulong case_idx=0UL; case_idx<sizeof(cases)/sizeof(*cases); case_idx++ ) {
    test_pack_tile_harness_t h[1];
    uchar                    sigs[ 3 ][ sizeof(fd_ed25519_sig_t) ];
    uchar                    txn_cnt = cases[ case_idx ].txn_cnt;

    test_pack_tile_harness_new( h );
    for( uchar i=0U; i<txn_cnt; i++ ) test_pack_tile_fill_sig( sigs[ i ], (uchar)( 10UL*case_idx + i + 1U ) );
    FD_TEST( pack_tile_track_bam_work( h->ctx, sigs, 0L, (uint)( case_idx+1UL ), 0U, 100UL, 100UL, 100UL, 0U, txn_cnt ) );
    (void)test_pack_tile_mark_bam_work_scheduled( h, sigs[ 0 ] );

    for( uchar i=0U; i<txn_cnt; i++ ) {
      test_pack_tile_send_executed_txn( h, sigs[ i ], cases[ case_idx ].event_kind[ i ] );
      if( i+1U<txn_cnt ) FD_TEST( h->ctx->bam_work_cnt == 1UL );
    }

    ulong other_stage = cases[ case_idx ].completed_stage==FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_LANDED_IDX
                      ? FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_COMPLETED_UNLANDED_IDX
                      : FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_LANDED_IDX;
    FD_TEST( h->ctx->bam_work_cnt == 0UL );
    FD_TEST( test_delete_call_cnt == cases[ case_idx ].delete_call_cnt );
    FD_TEST( h->ctx->bam_pending_result_cnt == 0UL );
    FD_TEST( h->ctx->bam_work_item_stage_cnt[ cases[ case_idx ].completed_stage ] == 1UL );
    FD_TEST( h->ctx->bam_work_item_stage_cnt[ other_stage ] == 0UL );
    test_pack_tile_harness_delete( h );
  }
}

static void
test_pack_tile_bam_completion_tracking_reuses_capacity( void ) {
  test_pack_tile_harness_t h[1];
  uchar                    sigs[ 3 ][ sizeof(fd_ed25519_sig_t) ];

  test_pack_tile_harness_new( h );
  for( uchar i=0U; i<3U; i++ ) test_pack_tile_fill_sig( sigs[ i ], (uchar)( 70U + i ) );
  FD_TEST( pack_tile_track_bam_work( h->ctx, sigs, 0L, 5U, 0U, 100UL, 100UL, 100UL, 0U, 2U ) );
  (void)test_pack_tile_mark_bam_work_scheduled( h, sigs[ 0 ] );

  test_pack_tile_send_executed_txn( h, sigs[ 0 ], FD_EXECUTED_TXN_KIND_BAM_COMPLETED_UNLANDED );
  test_pack_tile_send_executed_txn( h, sigs[ 0 ], FD_EXECUTED_TXN_KIND_BAM_COMPLETED_UNLANDED );
  test_pack_tile_send_executed_txn( h, sigs[ 2 ], FD_EXECUTED_TXN_KIND_BAM_COMPLETED_UNLANDED );
  FD_TEST( h->ctx->bam_work[ 0 ].remaining_txn_cnt == 1U );
  test_pack_tile_send_executed_txn( h, sigs[ 1 ], FD_EXECUTED_TXN_KIND_BAM_COMPLETED_UNLANDED );
  test_pack_tile_send_executed_txn( h, sigs[ 1 ], FD_EXECUTED_TXN_KIND_BAM_COMPLETED_UNLANDED );
  FD_TEST( h->ctx->bam_work_cnt == 0UL );

  for( ulong i=0UL; i<3UL*TEST_PACK_TILE_BAM_WORK_CAP; i++ ) {
    test_pack_tile_fill_sig( sigs[ 0 ], (uchar)( 100UL + i ) );
    FD_TEST( pack_tile_track_bam_work( h->ctx, sigs[ 0 ], 0L, (uint)( 100UL + i ), 0U, 100UL, 100UL, 100UL, 0U, 1U ) );
    (void)test_pack_tile_mark_bam_work_scheduled( h, sigs[ 0 ] );
    test_pack_tile_send_executed_txn( h, sigs[ 0 ], FD_EXECUTED_TXN_KIND_BAM_COMPLETED_UNLANDED );
    FD_TEST( h->ctx->bam_work_cnt == 0UL );
  }

  FD_TEST( h->ctx->bam_scheduled_work_cnt == 0UL );
  FD_TEST( h->ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_COMPLETED_UNLANDED_IDX ] == 1UL+3UL*TEST_PACK_TILE_BAM_WORK_CAP );
  test_pack_tile_harness_delete( h );
}

static ulong
test_pack_tile_prepare_resolv_frag( test_pack_tile_harness_t * h,
                                    uchar *                    resolved_buf,
                                    uchar const *              payload,
                                    ulong                      payload_sz,
                                    uchar                      source_tpu,
                                    ulong                      slot ) {
  fd_memset( resolved_buf, 0, FD_TPU_RESOLVED_MTU );

  fd_txn_m_t * txnm = (fd_txn_m_t *)resolved_buf;
  txnm->reference_slot = slot;
  txnm->payload_sz     = (ushort)payload_sz;
  txnm->source_tpu     = source_tpu;
  fd_memcpy( fd_txn_m_payload( txnm ), payload, payload_sz );

  fd_txn_t * txn      = fd_txn_m_txn_t( txnm );
  ulong      txn_t_sz = fd_txn_parse( fd_txn_m_payload( txnm ), payload_sz, txn, NULL );
  FD_TEST( txn_t_sz );
  txnm->txn_t_sz = (ushort)txn_t_sz;

  ulong sz = fd_txn_m_realized_footprint( txnm, 1, 1 );
  h->ctx->in_kind[ 0 ]   = IN_KIND_RESOLV;
  h->ctx->in[ 0 ].mem    = (fd_wksp_t *)resolved_buf;
  h->ctx->in[ 0 ].chunk0 = 0UL;
  h->ctx->in[ 0 ].wmark  = (sz + FD_CHUNK_SZ - 1UL) >> FD_CHUNK_LG_SZ;
  h->ctx->leader_slot    = slot;
  return sz;
}

static void
test_pack_tile_bam_override_allows_votes_only_from_normal_ingress( void ) {
  test_pack_tile_harness_t h[1];
  uchar                    resolved_buf[ FD_TPU_RESOLVED_MTU ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  ulong                    bam_status_fseq = FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE;

  test_pack_tile_harness_new( h );

  struct {
    uchar const * payload;
    ulong         payload_sz;
    uchar         source_tpu;
    int           is_vote;
  } cases[] = {
    { test_pack_tile_non_vote,    test_pack_tile_non_vote_sz,    FD_TXN_M_TPU_SOURCE_UDP,  0 },
    { test_pack_tile_sample_vote, test_pack_tile_sample_vote_sz, FD_TXN_M_TPU_SOURCE_UDP,  1 },
    { test_pack_tile_non_vote,    test_pack_tile_non_vote_sz,    FD_TXN_M_TPU_SOURCE_QUIC, 0 },
    { test_pack_tile_sample_vote, test_pack_tile_sample_vote_sz, FD_TXN_M_TPU_SOURCE_QUIC, 1 },
  };

  ulong expected_insert_cnt = 0UL;
  for( ulong i=0UL; i<sizeof(cases)/sizeof(cases[0]); i++ ) {
    ulong sz = test_pack_tile_prepare_resolv_frag( h, resolved_buf, cases[ i ].payload, cases[ i ].payload_sz, cases[ i ].source_tpu, 100UL );

    fd_txn_m_t * txnm = (fd_txn_m_t *)resolved_buf;
    fd_txn_t *   txn  = fd_txn_m_txn_t( txnm );
    FD_TEST( fd_txn_is_simple_vote_transaction( txn, fd_txn_m_payload( txnm ) )==cases[ i ].is_vote );

    h->ctx->bam_status_fseq            = &bam_status_fseq;
    h->ctx->current_bundle->bundle     = NULL;
    h->ctx->current_bundle_bam->is_bam = 0;

    during_frag( h->ctx, 0UL, 0UL, 100UL, 0UL, sz, 0UL );
    after_frag( h->ctx, 0UL, 0UL, 100UL, sz, 0UL, 0UL, &h->out->stem );

    expected_insert_cnt += (ulong)cases[ i ].is_vote;
    FD_TEST( test_insert_txn_init_call_cnt == expected_insert_cnt );
    FD_TEST( test_insert_txn_fini_call_cnt == expected_insert_cnt );
    FD_TEST( h->ctx->cur_spot == NULL );
  }

  FD_TEST( test_insert_txn_slot->txnp->source_tpu == FD_TXN_M_TPU_SOURCE_QUIC );
  FD_TEST( test_insert_txn_slot->txnp->payload_sz == test_pack_tile_sample_vote_sz );
  FD_TEST( fd_txn_is_simple_vote_transaction( TXN( test_insert_txn_slot->txnp ), test_insert_txn_slot->txnp->payload ) );

  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_bam_override_after_frag_preserves_votes_only( void ) {
  struct {
    uchar const * payload;
    ulong         payload_sz;
    int           is_vote;
  } cases[] = {
    { test_pack_tile_non_vote,    test_pack_tile_non_vote_sz,    0 },
    { test_pack_tile_sample_vote, test_pack_tile_sample_vote_sz, 1 },
  };

  for( ulong i=0UL; i<sizeof(cases)/sizeof(cases[0]); i++ ) {
    test_pack_tile_harness_t h[1];
    uchar                    resolved_buf[ FD_TPU_RESOLVED_MTU ] __attribute__((aligned(FD_CHUNK_ALIGN)));
    ulong                    bam_status_fseq = 0UL;

    test_pack_tile_harness_new( h );
    ulong sz = test_pack_tile_prepare_resolv_frag( h, resolved_buf, cases[ i ].payload, cases[ i ].payload_sz, FD_TXN_M_TPU_SOURCE_QUIC, 100UL );
    h->ctx->bam_status_fseq = &bam_status_fseq;

    during_frag( h->ctx, 0UL, 0UL, 100UL, 0UL, sz, 0UL );
    FD_TEST( test_insert_txn_init_call_cnt == 1UL );
    FD_TEST( h->ctx->cur_spot );

    bam_status_fseq = FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE;
    after_frag( h->ctx, 0UL, 0UL, 100UL, sz, 0UL, 0UL, &h->out->stem );

    FD_TEST( test_insert_txn_fini_call_cnt == (ulong)cases[ i ].is_vote );
    FD_TEST( test_insert_txn_cancel_call_cnt == (ulong)!cases[ i ].is_vote );
    FD_TEST( h->ctx->cur_spot == NULL );

    test_pack_tile_harness_delete( h );
  }
}

static void
test_pack_tile_bam_override_drops_block_engine_bundles( void ) {
  test_pack_tile_harness_t h[1];
  uchar                    resolved_buf[ FD_TPU_RESOLVED_MTU ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  ulong                    bam_status_fseq = FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE;

  test_pack_tile_harness_new( h );
  ulong sz = test_pack_tile_prepare_resolv_frag( h, resolved_buf, test_pack_tile_non_vote, test_pack_tile_non_vote_sz, FD_TXN_M_TPU_SOURCE_BUNDLE, 100UL );

  fd_txn_m_t * txnm = (fd_txn_m_t *)resolved_buf;
  txnm->block_engine.bundle_id      = 44UL;
  txnm->block_engine.bundle_txn_cnt = 2UL;

  h->ctx->bam_status_fseq = &bam_status_fseq;

  h->ctx->current_bundle->id           = 43UL;
  h->ctx->current_bundle->txn_cnt      = 3UL;
  h->ctx->current_bundle->txn_received = 1UL;
  h->ctx->current_bundle->bundle       = h->ctx->current_bundle->_txn;
  h->ctx->current_bundle_bam->is_bam   = 0;

  during_frag( h->ctx, 0UL, 0UL, 100UL, 0UL, sz, 0UL );
  after_frag( h->ctx, 0UL, 0UL, 100UL, sz, 0UL, 0UL, &h->out->stem );

  FD_TEST( h->ctx->bundle_kind == PACK_TILE_BUNDLE_KIND_NONE );
  FD_TEST( h->ctx->current_bundle->bundle == NULL );
  FD_TEST( test_bundle_cancel_call_cnt == 1UL );
  FD_TEST( test_bundle_cancel_last_txn_cnt == 3UL );
  FD_TEST( test_insert_fini_call_cnt == 0UL );

  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_bam_override_after_frag_cancels_block_engine_bundle( void ) {
  test_pack_tile_harness_t h[1];
  fd_txn_e_t               staged_txns[ 2 ];
  ulong                    bam_status_fseq = FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE;

  test_pack_tile_harness_new( h );
  fd_memset( staged_txns, 0, sizeof(staged_txns) );

  h->ctx->in_kind[ 0 ]           = IN_KIND_RESOLV;
  h->ctx->bam_status_fseq        = &bam_status_fseq;
  h->ctx->bundle_kind            = PACK_TILE_BUNDLE_KIND_BLOCK_ENGINE;
  h->ctx->current_bundle->id     = 55UL;
  h->ctx->current_bundle->txn_cnt      = 2UL;
  h->ctx->current_bundle->txn_received = 1UL;
  h->ctx->current_bundle->_txn[ 0 ]    = &staged_txns[ 0 ];
  h->ctx->current_bundle->_txn[ 1 ]    = &staged_txns[ 1 ];
  h->ctx->current_bundle->bundle       = h->ctx->current_bundle->_txn;
  h->ctx->current_bundle_bam->is_bam   = 0;
  h->ctx->cur_spot = &staged_txns[ 1 ];

  after_frag( h->ctx, 0UL, 0UL, 100UL, 0UL, 0UL, 0UL, &h->out->stem );

  FD_TEST( h->ctx->bundle_kind == PACK_TILE_BUNDLE_KIND_NONE );
  FD_TEST( h->ctx->current_bundle->bundle == NULL );
  FD_TEST( h->ctx->current_bundle->txn_received == 0UL );
  FD_TEST( h->ctx->cur_spot == NULL );
  FD_TEST( test_bundle_cancel_call_cnt == 1UL );
  FD_TEST( test_bundle_cancel_last_txn_cnt == 2UL );
  FD_TEST( test_insert_fini_call_cnt == 0UL );

  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_complete_bam_bundle( test_pack_tile_harness_t * h,
                                    fd_txn_e_t * const *       txns,
                                    uchar                      txn_cnt,
                                    uint                       seq_id,
                                    ulong                      max_schedule_slot,
                                    ulong                      min_blockhash_slot,
                                    uchar                      min_blockhash_slot_txn_idx ) {
  FD_TEST( txn_cnt );
  h->ctx->in_kind[ 0 ]                         = IN_KIND_RESOLV;
  h->ctx->bundle_kind                          = PACK_TILE_BUNDLE_KIND_BAM;
  h->ctx->current_bundle->id                   = (ulong)seq_id + 1UL;
  h->ctx->current_bundle->txn_cnt              = txn_cnt;
  h->ctx->current_bundle->txn_received         = (ulong)txn_cnt - 1UL;
  h->ctx->current_bundle->min_blockhash_slot   = min_blockhash_slot;
  h->ctx->current_bundle->bundle               = h->ctx->current_bundle->_txn;
  h->ctx->current_bundle_bam->max_schedule_slot = max_schedule_slot;
  h->ctx->current_bundle_bam->is_bam            = 1;
  h->ctx->current_bundle_bam->min_blockhash_slot_txn_idx = min_blockhash_slot_txn_idx;

  for( uchar i=0U; i<txn_cnt; i++ ) {
    txns[ i ]->txnp->bam.batch_idx = i;
    txns[ i ]->txnp->source_tpu    = FD_TXN_M_TPU_SOURCE_BAM;
    h->ctx->current_bundle->_txn[ i ] = txns[ i ];
  }
  h->ctx->cur_spot = txns[ txn_cnt-1U ];

  after_frag( h->ctx, 0UL, 0UL, min_blockhash_slot, 0UL, 0UL, 0UL, &h->out->stem );
}

static void
test_pack_tile_bam_missing_builder_cfg_accepted( void ) {
  test_pack_tile_harness_t h[1];
  fd_txn_e_t               txn[1];

  test_pack_tile_harness_new( h );
  fd_memset( txn, 0, sizeof(txn) );
  test_pack_tile_fill_sig( txn->txnp->payload + 1UL, 91U );
  test_pack_tile_complete_bam_bundle( h, (fd_txn_e_t *[1]){ txn }, 1U, 90U, 100UL, 0UL, 0U );

  FD_TEST( test_insert_fini_call_cnt == 1UL );
  FD_TEST( test_bundle_cancel_call_cnt == 0UL );
  FD_TEST( h->ctx->current_bundle->bundle == NULL );
  FD_TEST( h->ctx->bam_work_cnt == 1UL );
  FD_TEST( h->ctx->bam_pending_result_cnt == 0UL );
  FD_TEST( h->ctx->blk_engine_cfg->is_bam );
  FD_TEST( h->ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_ACCEPTED_IDX ] == 1UL );
  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_d18_sign_and_parse( fd_txn_p_t * txnp,
                                   uchar *      payload_end,
                                   uchar const  public_key[ 32 ],
                                   uchar const  private_key[ 32 ],
                                   fd_sha512_t * sha,
                                   ulong         expected_payload_sz ) {
  uchar * message    = txnp->payload + 1UL + FD_TXN_SIGNATURE_SZ;
  ulong   message_sz = (ulong)( payload_end-message );
  fd_ed25519_sign( txnp->payload+1UL, message, message_sz, public_key, private_key, sha );
  FD_TEST( fd_ed25519_verify( message, message_sz, txnp->payload+1UL, public_key, sha )==FD_ED25519_SUCCESS );

  txnp->payload_sz = (ulong)( payload_end-txnp->payload );
  FD_TEST( txnp->payload_sz==expected_payload_sz );
  FD_TEST( fd_txn_parse( txnp->payload, txnp->payload_sz, TXN( txnp ), NULL ) );
}

/* C port of D18_LIVE_VALIDATOR_POC.diff's build_d18_over_limit_packet
   and valid control Transaction::new_signed_with_payer. */
static void
test_pack_tile_make_d18_poc_txns( fd_txn_p_t * bad_txnp,
                                  fd_txn_p_t * control_txnp ) {
  uchar private_key[ 32 ] = {1U};
  uchar public_key [ 32 ];
  uchar blockhash  [ FD_TXN_BLOCKHASH_SZ ] = {2U};

  fd_sha512_t sha[1];
  FD_TEST( fd_sha512_join( fd_sha512_new( sha ) ) );
  fd_ed25519_public_from_private( public_key, private_key, sha );

  uchar const system_transfer_one_lamport[ 12 ] = {
    2U, 0U, 0U, 0U, /* SystemInstruction::Transfer */
    1U, 0U, 0U, 0U, 0U, 0U, 0U, 0U,
  };
  uchar const set_compute_unit_limit[ 5 ] = { 2U, 0x40U, 0x42U, 0x0fU, 0U }; /* 1,000,000 CUs */

  fd_memset( bad_txnp, 0, sizeof(*bad_txnp) );
  uchar * p = bad_txnp->payload;
  *p++ = 1U; /* signature count */
  p += FD_TXN_SIGNATURE_SZ;
  *p++ = 1U; /* num_required_signatures */
  *p++ = 0U; /* num_readonly_signed_accounts */
  *p++ = 1U; /* num_readonly_unsigned_accounts: system program */
  p += fd_cu16_enc( 2U, p );
  fd_memcpy( p, public_key, FD_TXN_ACCT_ADDR_SZ );
  p += FD_TXN_ACCT_ADDR_SZ;
  fd_memset( p, 0, FD_TXN_ACCT_ADDR_SZ ); /* System Program ID */
  p += FD_TXN_ACCT_ADDR_SZ;
  fd_memcpy( p, blockhash, sizeof(blockhash) );
  p += sizeof(blockhash);
  p += fd_cu16_enc( 1U, p ); /* instruction count */
  *p++ = 1U; /* program_id_index: system program */
  p += fd_cu16_enc( (ushort)TEST_D18_INSTR_ACCT_CNT, p );
  fd_memset( p, 0, TEST_D18_INSTR_ACCT_CNT );
  p += TEST_D18_INSTR_ACCT_CNT;
  p += fd_cu16_enc( (ushort)sizeof(system_transfer_one_lamport), p );
  fd_memcpy( p, system_transfer_one_lamport, sizeof(system_transfer_one_lamport) );
  p += sizeof(system_transfer_one_lamport);
  test_pack_tile_d18_sign_and_parse( bad_txnp, p, public_key, private_key, sha, 438UL );

  fd_txn_t const * bad_txn = TXN( bad_txnp );
  FD_TEST( bad_txn->signature_cnt==1U && bad_txn->acct_addr_cnt==2U && bad_txn->instr_cnt==1U );
  FD_TEST( bad_txn->instr[ 0 ].program_id==1U && bad_txn->instr[ 0 ].acct_cnt==TEST_D18_INSTR_ACCT_CNT &&
           bad_txn->instr[ 0 ].data_sz==sizeof(system_transfer_one_lamport) );

  fd_memset( control_txnp, 0, sizeof(*control_txnp) );
  p = control_txnp->payload;
  *p++ = 1U; /* signature count */
  p += FD_TXN_SIGNATURE_SZ;
  *p++ = 1U; /* num_required_signatures */
  *p++ = 0U; /* num_readonly_signed_accounts */
  *p++ = 2U; /* num_readonly_unsigned_accounts: system and compute budget */
  p += fd_cu16_enc( 3U, p );
  fd_memcpy( p, public_key, FD_TXN_ACCT_ADDR_SZ );
  p += FD_TXN_ACCT_ADDR_SZ;
  fd_memset( p, 0, FD_TXN_ACCT_ADDR_SZ ); /* System Program ID sorts first */
  p += FD_TXN_ACCT_ADDR_SZ;
  fd_memcpy( p, FD_COMPUTE_BUDGET_PROGRAM_ID, FD_TXN_ACCT_ADDR_SZ );
  p += FD_TXN_ACCT_ADDR_SZ;
  fd_memcpy( p, blockhash, sizeof(blockhash) );
  p += sizeof(blockhash);
  p += fd_cu16_enc( 2U, p ); /* instruction count */
  *p++ = 2U; /* Compute Budget Program */
  p += fd_cu16_enc( 0U, p );
  p += fd_cu16_enc( (ushort)sizeof(set_compute_unit_limit), p );
  fd_memcpy( p, set_compute_unit_limit, sizeof(set_compute_unit_limit) );
  p += sizeof(set_compute_unit_limit);
  *p++ = 1U; /* System Program */
  p += fd_cu16_enc( 2U, p );
  *p++ = 0U; /* fee payer / transfer source */
  *p++ = 0U; /* fee payer / transfer destination */
  p += fd_cu16_enc( (ushort)sizeof(system_transfer_one_lamport), p );
  fd_memcpy( p, system_transfer_one_lamport, sizeof(system_transfer_one_lamport) );
  p += sizeof(system_transfer_one_lamport);
  test_pack_tile_d18_sign_and_parse( control_txnp, p, public_key, private_key, sha, 223UL );

  fd_txn_t const * control_txn = TXN( control_txnp );
  FD_TEST( control_txn->signature_cnt==1U && control_txn->acct_addr_cnt==3U && control_txn->instr_cnt==2U );
  FD_TEST( control_txn->instr[ 0 ].program_id==2U && control_txn->instr[ 0 ].acct_cnt==0U &&
           control_txn->instr[ 0 ].data_sz==sizeof(set_compute_unit_limit) );
}

static void
test_pack_tile_bam_stale_max_schedule_slot_rejected( void ) {
  test_pack_tile_harness_t h[1];
  uchar                    sigs[ 2UL * sizeof(fd_ed25519_sig_t) ];

  test_pack_tile_harness_new( h );

  test_pack_tile_fill_sig( sigs + 0UL, 11U );
  test_pack_tile_fill_sig( sigs + sizeof(fd_ed25519_sig_t), 22U );

  FD_TEST( pack_tile_track_bam_work( h->ctx, sigs, 0L, 55U, 0U, 100UL, 100UL, 100UL, 0U, 2U ) );
  FD_TEST( h->ctx->bam_work_cnt == 1UL );
  FD_TEST( h->ctx->bam_pending_work_cnt == 1UL );

  pack_tile_evict_invalid_pending_bam_work( h->ctx, 101UL );

  test_pack_tile_assert_deleted_sig( (fd_ed25519_sig_t const *)sigs );
  FD_TEST( h->out->seqs[ 0 ] == 0UL );
  FD_TEST( h->ctx->bam_work_cnt == 0UL );
  FD_TEST( h->ctx->bam_pending_work_cnt == 0UL );
  FD_TEST( h->ctx->bam_scheduled_work_cnt == 0UL );
  FD_TEST( h->ctx->bam_pending_result_cnt == 1UL );

  FD_TEST( h->ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_PENDING_ENTERED_IDX ] == 1UL );
  FD_TEST( h->ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_PENDING_EVICTED_IDX ] == 1UL );

  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  FD_TEST( h->ctx->bam_pending_result_cnt == 0UL );
  test_pack_tile_assert_last_result( h, 55U, 100UL, 2U, FD_BAM_SCHED_ERR_OUTSIDE_SLOT, 0U );

  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_bam_expired_blockhash_reports_member_idx( void ) {
  for( ulong stage=0UL; stage<3UL; stage++ ) {
    test_pack_tile_harness_t h[1];
    fd_txn_e_t               txns[ 2 ];

    test_pack_tile_harness_new( h );
    fd_memset( txns, 0, sizeof(txns) );
    test_pack_tile_fill_sig( txns[ 0 ].txnp->payload + 1UL, (uchar)( 41UL+2UL*stage ) );
    test_pack_tile_fill_sig( txns[ 1 ].txnp->payload + 1UL, (uchar)( 42UL+2UL*stage ) );

    ulong min_blockhash_slot;
    ulong max_schedule_slot;
    if( FD_UNLIKELY( stage==0UL ) ) {
      h->ctx->leader_slot = TRANSACTION_LIFETIME_SLOTS + 1UL;
      min_blockhash_slot  = 0UL;
      max_schedule_slot   = h->ctx->leader_slot;
    } else if( FD_UNLIKELY( stage==1UL ) ) {
      h->ctx->leader_slot   = 100UL;
      min_blockhash_slot    = 100UL;
      max_schedule_slot     = 100UL;
      test_insert_fini_result = FD_PACK_INSERT_REJECT_EXPIRED;
    } else {
      min_blockhash_slot  = 100UL;
      h->ctx->leader_slot = min_blockhash_slot + TRANSACTION_LIFETIME_SLOTS;
      max_schedule_slot   = h->ctx->leader_slot + 1UL;
    }

    test_pack_tile_complete_bam_bundle( h, (fd_txn_e_t *[2]){ txns, txns+1 }, 2U,
                                        (uint)( 56UL+stage ), max_schedule_slot, min_blockhash_slot, 1U );

    if( FD_UNLIKELY( stage==2UL ) ) {
      pack_tile_evict_invalid_pending_bam_work( h->ctx, max_schedule_slot );
    }
    FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
    fd_bam_bundle_result_t const * expired = test_pack_tile_assert_last_result( h,
                                                                                (uint)( 56UL+stage ),
                                                                                max_schedule_slot,
                                                                                2U,
                                                                                FD_BAM_SCHED_ERR_NONE,
                                                                                2U );
    FD_TEST( expired->transaction_err[ 0 ] == bam_types_TransactionErrorReason_COMMIT_CANCELLED );
    FD_TEST( expired->transaction_err[ 1 ] == bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND );
    test_pack_tile_harness_delete( h );
  }
}

static void
test_pack_tile_bam_stale_results_drain_without_drop( void ) {
  test_pack_tile_harness_t h[1];
  uchar                    sigs[ 3 ][ sizeof(fd_ed25519_sig_t) ];

  test_pack_tile_harness_new( h );

  for( uchar i=0U; i<3U; i++ ) {
    test_pack_tile_fill_sig( sigs[ i ], (uchar)( 40U + 10U*i ) );
    FD_TEST( pack_tile_track_bam_work( h->ctx, sigs[ i ], 0L, (uint)( 100U + i ), 0U, 100UL, 100UL, 100UL, 0U, 1U ) );
  }

  pack_tile_evict_invalid_pending_bam_work( h->ctx, 101UL );

  FD_TEST( test_delete_call_cnt == 3UL );
  FD_TEST( h->out->seqs[ 0 ] == 0UL );
  FD_TEST( h->ctx->bam_work_cnt == 0UL );
  FD_TEST( h->ctx->bam_pending_work_cnt == 0UL );
  FD_TEST( h->ctx->bam_pending_result_cnt == 3UL );

  for( uchar i=0U; i<3U; i++ ) {
    if( FD_LIKELY( i ) ) h->ctx->bam_result_publish_cnt = 0UL;
    FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
    test_pack_tile_assert_last_result( h, (uint)( 100U + i ), 100UL, 1U, FD_BAM_SCHED_ERR_OUTSIDE_SLOT, 0U );
    FD_TEST( h->out->seqs[ 0 ] == (ulong)i + 1UL );
    FD_TEST( h->ctx->bam_pending_result_cnt == 2UL - (ulong)i );
  }

  FD_TEST( h->ctx->bam_work_cnt == 0UL );

  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_bam_same_seq_pending_duplicate_replaces_before_insert( void ) {
  test_pack_tile_harness_t h[1];
  uchar                    old_sigs[ 2UL * sizeof(fd_ed25519_sig_t) ];
  fd_txn_e_t               new_txn[1];

  test_pack_tile_harness_new( h );
  fd_memset( new_txn, 0, sizeof(new_txn) );

  test_pack_tile_fill_sig( old_sigs + 0UL, 21U );
  test_pack_tile_fill_sig( old_sigs + sizeof(fd_ed25519_sig_t), 31U );
  fd_memcpy( new_txn[ 0 ].txnp->payload + 1UL, old_sigs, sizeof(fd_ed25519_sig_t) );
  new_txn[ 0 ].txnp->scheduler_arrival_time_nanos = 20L;

  FD_TEST( pack_tile_track_bam_work( h->ctx, old_sigs, 10L, 10U, 0U, 100UL, 100UL, 100UL, 0U, 2U ) );
  FD_TEST( h->ctx->bam_work_cnt == 1UL );
  FD_TEST( h->ctx->bam_pending_work_cnt == 1UL );
  FD_TEST( h->ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_PENDING_ENTERED_IDX ] == 1UL );

  test_pack_tile_complete_bam_bundle( h, (fd_txn_e_t *[1]){ new_txn }, 1U, 10U, 100UL, 100UL, 0U );

  test_pack_tile_assert_deleted_sig( new_txn[ 0 ].txnp->payload + 1UL );
  FD_TEST( test_insert_fini_call_cnt == 1UL );
  FD_TEST( test_delete_before_insert_fini );
  FD_TEST( test_bundle_cancel_call_cnt == 0UL );
  FD_TEST( h->ctx->current_bundle->bundle == NULL );
  FD_TEST( h->ctx->current_bundle_bam->is_bam == 0 );
  FD_TEST( h->ctx->bam_work_cnt == 1UL );
  FD_TEST( h->ctx->bam_pending_work_cnt == 1UL );
  FD_TEST( h->ctx->bam_scheduled_work_cnt == 0UL );
  FD_TEST( h->ctx->bam_pending_result_cnt == 0UL );
  FD_TEST( h->ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_PENDING_ENTERED_IDX ] == 2UL );
  FD_TEST( h->ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_ACCEPTED_IDX ] == 1UL );

  ulong work_idx = pack_tile_bam_work_find_by_sig0( h->ctx, new_txn[ 0 ].txnp->payload + 1UL );
  FD_TEST( work_idx<h->ctx->bam_work_cnt );
  FD_TEST( h->ctx->bam_work[ work_idx ].seq_id == 10U );
  FD_TEST( h->ctx->bam_work[ work_idx ].state == PACK_BAM_WORK_STATE_PENDING );
  FD_TEST( h->ctx->bam_work[ work_idx ].txn_cnt == 1U );

  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_bam_same_seq_different_slot_pending_duplicate_rejected_before_insert( void ) {
  test_pack_tile_harness_t h[1];
  uchar                    old_sigs[ 2UL * sizeof(fd_ed25519_sig_t) ];
  fd_txn_e_t               new_txn[1];

  test_pack_tile_harness_new( h );
  fd_memset( new_txn, 0, sizeof(new_txn) );

  test_pack_tile_fill_sig( old_sigs + 0UL, 21U );
  test_pack_tile_fill_sig( old_sigs + sizeof(fd_ed25519_sig_t), 31U );
  fd_memcpy( new_txn[ 0 ].txnp->payload + 1UL, old_sigs, sizeof(fd_ed25519_sig_t) );
  new_txn[ 0 ].txnp->scheduler_arrival_time_nanos = 20L;

  FD_TEST( pack_tile_track_bam_work( h->ctx, old_sigs, 10L, 10U, 0U, 100UL, 100UL, 100UL, 0U, 2U ) );
  FD_TEST( h->ctx->bam_work_cnt == 1UL );
  FD_TEST( h->ctx->bam_pending_work_cnt == 1UL );
  FD_TEST( h->ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_PENDING_ENTERED_IDX ] == 1UL );

  test_pack_tile_complete_bam_bundle( h, (fd_txn_e_t *[1]){ new_txn }, 1U, 10U, 101UL, 101UL, 0U );

  FD_TEST( test_delete_call_cnt == 0UL );
  FD_TEST( test_insert_fini_call_cnt == 0UL );
  FD_TEST( test_bundle_cancel_call_cnt == 1UL );
  FD_TEST( test_bundle_cancel_last_txn_cnt == 1UL );
  FD_TEST( h->ctx->current_bundle->bundle == NULL );
  FD_TEST( h->ctx->current_bundle_bam->is_bam == 0 );

  FD_TEST( h->ctx->bam_work_cnt == 1UL );
  FD_TEST( h->ctx->bam_pending_work_cnt == 1UL );
  FD_TEST( h->ctx->bam_scheduled_work_cnt == 0UL );
  FD_TEST( h->ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_REJECTED_PRE_PENDING_IDX ] == 1UL );

  ulong work_idx = pack_tile_bam_work_find_by_sig0( h->ctx, old_sigs );
  FD_TEST( work_idx<h->ctx->bam_work_cnt );
  FD_TEST( h->ctx->bam_work[ work_idx ].seq_id == 10U );
  FD_TEST( h->ctx->bam_work[ work_idx ].max_schedule_slot == 100UL );
  FD_TEST( h->ctx->bam_work[ work_idx ].state == PACK_BAM_WORK_STATE_PENDING );
  FD_TEST( h->ctx->bam_work[ work_idx ].txn_cnt == 2U );

  FD_TEST( h->ctx->bam_pending_result_cnt == 1UL );
  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  fd_bam_bundle_result_t const * dup = test_pack_tile_assert_last_result( h, 10U, 101UL, 1U, FD_BAM_SCHED_ERR_NONE, 1U );
  FD_TEST( dup->transaction_err[ 0 ] == bam_types_TransactionErrorReason_ALREADY_PROCESSED );
  FD_TEST( dup->sanitize_success[ 0 ] == 1U );
  FD_TEST( h->ctx->bam_pending_result_cnt == 0UL );

  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_bam_different_seq_pending_duplicate_rejected_before_insert( void ) {
  test_pack_tile_harness_t h[1];
  uchar                    old_sigs[ 2UL * sizeof(fd_ed25519_sig_t) ];
  fd_txn_e_t               new_txn[1];

  test_pack_tile_harness_new( h );
  fd_memset( new_txn, 0, sizeof(new_txn) );

  test_pack_tile_fill_sig( old_sigs + 0UL, 21U );
  test_pack_tile_fill_sig( old_sigs + sizeof(fd_ed25519_sig_t), 31U );
  fd_memcpy( new_txn[ 0 ].txnp->payload + 1UL, old_sigs, sizeof(fd_ed25519_sig_t) );
  new_txn[ 0 ].txnp->scheduler_arrival_time_nanos = 20L;

  FD_TEST( pack_tile_track_bam_work( h->ctx, old_sigs, 10L, 10U, 0U, 100UL, 100UL, 100UL, 0U, 2U ) );
  FD_TEST( h->ctx->bam_work_cnt == 1UL );
  FD_TEST( h->ctx->bam_pending_work_cnt == 1UL );
  FD_TEST( h->ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_PENDING_ENTERED_IDX ] == 1UL );

  test_pack_tile_complete_bam_bundle( h, (fd_txn_e_t *[1]){ new_txn }, 1U, 11U, 101UL, 101UL, 0U );

  FD_TEST( test_delete_call_cnt == 0UL );
  FD_TEST( test_insert_fini_call_cnt == 0UL );
  FD_TEST( test_bundle_cancel_call_cnt == 1UL );
  FD_TEST( test_bundle_cancel_last_txn_cnt == 1UL );
  FD_TEST( h->ctx->current_bundle->bundle == NULL );
  FD_TEST( h->ctx->current_bundle_bam->is_bam == 0 );

  FD_TEST( h->ctx->bam_work_cnt == 1UL );
  FD_TEST( h->ctx->bam_pending_work_cnt == 1UL );
  FD_TEST( h->ctx->bam_scheduled_work_cnt == 0UL );
  FD_TEST( h->ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_REJECTED_PRE_PENDING_IDX ] == 1UL );

  ulong work_idx = pack_tile_bam_work_find_by_sig0( h->ctx, old_sigs );
  FD_TEST( work_idx<h->ctx->bam_work_cnt );
  FD_TEST( h->ctx->bam_work[ work_idx ].seq_id == 10U );
  FD_TEST( h->ctx->bam_work[ work_idx ].state == PACK_BAM_WORK_STATE_PENDING );
  FD_TEST( h->ctx->bam_work[ work_idx ].txn_cnt == 2U );

  FD_TEST( h->ctx->bam_pending_result_cnt == 1UL );
  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  fd_bam_bundle_result_t const * dup = test_pack_tile_assert_last_result( h, 11U, 101UL, 1U, FD_BAM_SCHED_ERR_NONE, 1U );
  FD_TEST( dup->transaction_err[ 0 ] == bam_types_TransactionErrorReason_ALREADY_PROCESSED );
  FD_TEST( dup->sanitize_success[ 0 ] == 1U );
  FD_TEST( h->ctx->bam_pending_result_cnt == 0UL );

  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_bam_scheduled_duplicate_rejected_before_insert( void ) {
  test_pack_tile_harness_t h[1];
  uchar                    sig[ sizeof(fd_ed25519_sig_t) ];
  fd_txn_e_t               new_txns[2];

  test_pack_tile_harness_new( h );
  fd_memset( new_txns, 0, sizeof(new_txns) );

  test_pack_tile_fill_sig( sig, 41U );
  FD_TEST( pack_tile_track_bam_work( h->ctx, sig, 10L, 20U, 0U, 100UL, 100UL, 100UL, 0U, 1U ) );
  pack_bam_work_t * scheduled = test_pack_tile_mark_bam_work_scheduled( h, sig );
  FD_TEST( scheduled->seq_id == 20U );
  FD_TEST( h->ctx->bam_pending_work_cnt == 0UL );
  FD_TEST( h->ctx->bam_scheduled_work_cnt == 1UL );

  fd_memcpy( new_txns[ 0 ].txnp->payload + 1UL, sig, sizeof(fd_ed25519_sig_t) );
  test_pack_tile_fill_sig( new_txns[ 1 ].txnp->payload + 1UL, 51U );
  new_txns[ 0 ].txnp->scheduler_arrival_time_nanos = 20L;
  new_txns[ 1 ].txnp->scheduler_arrival_time_nanos = 21L;
  test_pack_tile_complete_bam_bundle( h, (fd_txn_e_t *[2]){ new_txns, new_txns+1 },
                                      2U, 21U, 101UL, 101UL, 0U );

  FD_TEST( test_delete_call_cnt == 0UL );
  FD_TEST( test_insert_fini_call_cnt == 0UL );
  FD_TEST( test_bundle_cancel_call_cnt == 1UL );
  FD_TEST( test_bundle_cancel_last_txn_cnt == 2UL );
  FD_TEST( h->ctx->current_bundle->bundle == NULL );
  FD_TEST( h->ctx->current_bundle_bam->is_bam == 0 );

  FD_TEST( h->ctx->bam_work_cnt == 1UL );
  FD_TEST( h->ctx->bam_pending_work_cnt == 0UL );
  FD_TEST( h->ctx->bam_scheduled_work_cnt == 1UL );
  ulong work_idx = pack_tile_bam_work_find_by_sig0( h->ctx, sig );
  FD_TEST( work_idx<h->ctx->bam_work_cnt );
  FD_TEST( h->ctx->bam_work[ work_idx ].seq_id == 20U );
  FD_TEST( h->ctx->bam_work[ work_idx ].state == PACK_BAM_WORK_STATE_SCHEDULED );

  FD_TEST( h->ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_SCHEDULED_IDX ] == 1UL );
  FD_TEST( h->ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_REJECTED_PRE_PENDING_IDX ] == 1UL );

  FD_TEST( h->ctx->bam_pending_result_cnt == 1UL );
  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  fd_bam_bundle_result_t const * dup = test_pack_tile_assert_last_result( h, 21U, 101UL, 2U, FD_BAM_SCHED_ERR_NONE, 2U );
  FD_TEST( dup->transaction_err[ 0 ] == bam_types_TransactionErrorReason_ALREADY_PROCESSED );
  FD_TEST( dup->sanitize_success[ 0 ] == 1U );
  FD_TEST( dup->sanitize_success[ 1 ] == 1U );

  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_bam_pending_result_does_not_shadow_new_work( void ) {
  test_pack_tile_harness_t h[1];
  uchar                    sig[ sizeof(fd_ed25519_sig_t) ];

  test_pack_tile_harness_new( h );

  test_pack_tile_fill_sig( sig, 70U );
  FD_TEST( pack_tile_track_bam_work( h->ctx, sig, 0L, 200U, 0U, 100UL, 100UL, 100UL, 0U, 1U ) );
  pack_tile_evict_invalid_pending_bam_work( h->ctx, 101UL );

  FD_TEST( h->ctx->bam_work_cnt == 0UL );
  FD_TEST( h->ctx->bam_pending_result_cnt == 1UL );

  FD_TEST( pack_tile_track_bam_work( h->ctx, sig, 0L, 201U, 0U, 102UL, 102UL, 102UL, 0U, 1U ) );

  FD_TEST( h->ctx->bam_work_cnt == 1UL );
  FD_TEST( h->ctx->bam_pending_result_cnt == 1UL );
  FD_TEST( h->ctx->bam_pending_work_cnt == 1UL );
  ulong work_idx = pack_tile_bam_work_find_by_sig0( h->ctx, sig );
  FD_TEST( work_idx < h->ctx->bam_work_cnt );
  FD_TEST( h->ctx->bam_work[ work_idx ].state == PACK_BAM_WORK_STATE_PENDING );
  FD_TEST( h->ctx->bam_work[ work_idx ].seq_id == 201U );

  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_bam_queued_results_preserve_fifo_before_direct_publish( void ) {
  test_pack_tile_harness_t h[1];
  uchar                    sig[ sizeof(fd_ed25519_sig_t) ];

  test_pack_tile_harness_new( h );

  test_pack_tile_fill_sig( sig, 80U );
  FD_TEST( pack_tile_track_bam_work( h->ctx, sig, 0L, 100U, 0U, 100UL, 100UL, 100UL, 0U, 1U ) );
  pack_tile_evict_invalid_pending_bam_work( h->ctx, 101UL );

  pack_tile_publish_bam_insert_reject( h->ctx, 200U, 0U, 200UL, 1U, 0UL, FD_PACK_INSERT_REJECT_DUPLICATE );

  FD_TEST( h->out->seqs[ 0 ] == 0UL );
  FD_TEST( h->ctx->bam_pending_result_cnt == 2UL );

  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  test_pack_tile_assert_last_result( h, 100U, 100UL, 1U, FD_BAM_SCHED_ERR_OUTSIDE_SLOT, 0U );

  h->ctx->bam_result_publish_cnt = 0UL;
  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  test_pack_tile_assert_last_result( h, 200U, 200UL, 1U, FD_BAM_SCHED_ERR_NONE, 1U );
  FD_TEST( h->ctx->bam_pending_result_cnt == 0UL );

  pack_tile_publish_bam_insert_reject( h->ctx, 201U, 0U, 201UL, 1U, 0UL, FD_PACK_INSERT_REJECT_DUPLICATE );

  FD_TEST( h->out->seqs[ 0 ] == 2UL );
  FD_TEST( h->ctx->bam_pending_result_cnt == 1UL );

  h->ctx->bam_result_publish_cnt = 0UL;
  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  test_pack_tile_assert_last_result( h, 201U, 201UL, 1U, FD_BAM_SCHED_ERR_NONE, 1U );
  FD_TEST( h->ctx->bam_pending_result_cnt == 0UL );

  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_bam_pending_results_reserve_direct_result_headroom( void ) {
  test_pack_tile_harness_t h[1];
  uchar                    sigs[ TEST_PACK_TILE_BAM_WORK_CAP ][ sizeof(fd_ed25519_sig_t) ];

  test_pack_tile_harness_new( h );

  for( uchar i=0U; i<TEST_PACK_TILE_BAM_WORK_CAP; i++ ) {
    test_pack_tile_fill_sig( sigs[ i ], (uchar)( 90U + i ) );
    FD_TEST( pack_tile_track_bam_work( h->ctx, sigs[ i ], 0L, (uint)( 300U + i ), 0U, 100UL, 100UL, 100UL, 0U, 1U ) );
  }
  FD_TEST( h->ctx->bam_work_cnt == TEST_PACK_TILE_BAM_WORK_CAP );

  pack_tile_evict_invalid_pending_bam_work( h->ctx, 101UL );

  FD_TEST( h->ctx->bam_work_cnt == 0UL );
  FD_TEST( h->ctx->bam_pending_result_cnt == TEST_PACK_TILE_BAM_WORK_CAP );

  for( uchar i=0U; i<TEST_PACK_TILE_BAM_WORK_CAP-1U; i++ ) {
    test_pack_tile_fill_sig( sigs[ i ], (uchar)( 120U + i ) );
    FD_TEST( pack_tile_track_bam_work( h->ctx, sigs[ i ], 0L, (uint)( 400U + i ), 0U, 102UL, 102UL, 102UL, 0U, 1U ) );
  }
  test_pack_tile_fill_sig( sigs[ TEST_PACK_TILE_BAM_WORK_CAP-1U ], 130U );
  FD_TEST( !pack_tile_track_bam_work( h->ctx, sigs[ TEST_PACK_TILE_BAM_WORK_CAP-1U ], 0L, 499U, 0U, 102UL, 102UL, 102UL, 0U, 1U ) );
  FD_TEST( h->ctx->bam_work_cnt == TEST_PACK_TILE_BAM_WORK_CAP-1UL );
  FD_TEST( h->ctx->bam_pending_result_cnt == TEST_PACK_TILE_BAM_WORK_CAP );

  fd_bam_bundle_result_t res = { .seq_id = 700U, .slot = 700UL, .bundle_txn_cnt = 1U };
  h->ctx->bam_pending_result_cnt = 2UL*TEST_PACK_TILE_BAM_WORK_CAP;
  FD_TEST( !pack_tile_enqueue_bam_result( h->ctx, &res ) );

  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_bam_scheduled_work_does_not_consume_result_headroom( void ) {
  test_pack_tile_harness_t h[1];
  uchar                    sigs[ 5 ][ sizeof(fd_ed25519_sig_t) ];

  test_pack_tile_harness_new( h );

  for( uchar i=0U; i<4U; i++ ) {
    test_pack_tile_fill_sig( sigs[ i ], (uchar)( 150U + i ) );
    FD_TEST( pack_tile_track_bam_work( h->ctx, sigs[ i ], 0L, (uint)( 500U + i ), 0U, ULONG_MAX, 200UL, 200UL, 0U, 1U ) );
    (void)test_pack_tile_mark_bam_work_scheduled( h, sigs[ i ] );
  }

  FD_TEST( h->ctx->bam_work_cnt == 4UL );
  FD_TEST( h->ctx->bam_pending_work_cnt == 0UL );
  FD_TEST( h->ctx->bam_scheduled_work_cnt == 4UL );

  h->ctx->bam_pending_result_cnt = 2UL*TEST_PACK_TILE_BAM_WORK_CAP - h->ctx->bam_scheduled_work_cnt;

  test_pack_tile_fill_sig( sigs[ 4 ], 190U );
  FD_TEST( pack_tile_track_bam_work( h->ctx, sigs[ 4 ], 0L, 600U, 0U, 201UL, 201UL, 201UL, 0U, 1U ) );
  FD_TEST( h->ctx->bam_work_cnt == 5UL );
  FD_TEST( h->ctx->bam_pending_work_cnt == 1UL );
  FD_TEST( h->ctx->bam_scheduled_work_cnt == 4UL );

  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_bam_instr_acct_reject_serializes_exact_member( void ) {
  fd_pack_limits_t limits = {
    .max_cost_per_block           = FD_PACK_MAX_COST_PER_BLOCK_LOWER_BOUND,
    .max_vote_cost_per_block      = FD_PACK_MAX_VOTE_COST_PER_BLOCK_LOWER_BOUND,
    .max_write_cost_per_acct      = FD_PACK_MAX_WRITE_COST_PER_ACCT_LOWER_BOUND,
    .max_data_bytes_per_block     = FD_PACK_MAX_DATA_PER_BLOCK,
    .max_txn_per_microblock       = FD_PACK_MAX_TXN_PER_BUNDLE,
    .max_microblocks_per_block    = 1UL,
    .max_allocated_data_per_block = FD_PACK_MAX_ALLOCATED_DATA_PER_BLOCK,
  };

  fd_rng_t pack_rng[1];
  FD_TEST( fd_rng_join( fd_rng_new( pack_rng, 0U, 0UL ) ) );

  void * pack_mem = aligned_alloc( fd_pack_align(),
                                   fd_ulong_align_up( fd_pack_footprint( FD_PACK_MAX_TXN_PER_BUNDLE, 1UL, 1UL, &limits ),
                                                      fd_pack_align() ) );
  FD_TEST( pack_mem );
  fd_pack_t * pack = fd_pack_join( fd_pack_new( pack_mem, FD_PACK_MAX_TXN_PER_BUNDLE, 1UL, 1UL, &limits, NULL, 0UL, pack_rng ) );
  FD_TEST( pack );

  fd_txn_e_t * bundle[ 2 ];
  FD_TEST( fd_pack_insert_bundle_init( pack, bundle, 2UL )==bundle );
  test_pack_tile_make_d18_poc_txns( bundle[ 0 ]->txnp, bundle[ 1 ]->txnp );

  test_pack_tile_harness_t h[1];
  test_pack_tile_harness_new( h );
  h->ctx->pack                   = pack;
  test_insert_fini_use_real_pack = 1;

  test_pack_tile_complete_bam_bundle( h, bundle, 2U, 0U, 0UL, 0UL, 0U );

  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  fd_bam_bundle_result_t pack_result = *test_pack_tile_last_result( h );
  FD_TEST( pack_result.bundle_err   == FD_BAM_BUNDLE_ERR_DESER );
  FD_TEST( pack_result.deser_index  == 0U );
  FD_TEST( pack_result.deser_reason == bam_types_DeserializationErrorReason_SANITIZE_ERROR );
  test_pack_tile_harness_delete( h );

  FD_TEST( fd_pack_delete( fd_pack_leave( pack ) )==pack_mem );
  free( pack_mem );
  FD_TEST( fd_rng_delete( fd_rng_leave( pack_rng ) )==pack_rng );

  ulong wksp_footprint = 16UL<<20;
  void * wksp_mem = aligned_alloc( FD_SHMEM_NORMAL_PAGE_SZ, wksp_footprint );
  FD_TEST( wksp_mem );
  ulong part_max = fd_wksp_part_max_est( wksp_footprint, 64UL<<10 );
  fd_wksp_t * wksp = fd_wksp_join( fd_wksp_new( wksp_mem, "bam-wire-test", 1U, part_max,
                                                fd_wksp_data_max_est( wksp_footprint, part_max ) ) );
  FD_TEST( wksp );
  FD_TEST( !fd_shmem_join_anonymous( "bam-wire-test",
                                     FD_SHMEM_JOIN_MODE_READ_WRITE,
                                     wksp,
                                     wksp_mem,
                                     FD_SHMEM_NORMAL_PAGE_SZ,
                                     wksp_footprint>>FD_SHMEM_NORMAL_LG_PAGE_SZ ) );

  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  test_bam_prepare_scheduler_stream( env->state );
  test_bam_keepalive_sync( env->state, fd_bam_now() );
  test_enqueue_bundle_result( env->state, &pack_result );

  FD_TEST( fd_bam_test_flush_results( env->state )==1 );
  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( env->state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg==
           bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt==1UL );
  bam_types_AtomicTxnBatchResult const * wire_result = &decoded.multi.results[ 0 ];
  FD_TEST( wire_result->which_result==bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( wire_result->result.not_committed.which_reason==
           bam_types_NotCommitted_deserialization_error_tag );
  FD_TEST( wire_result->result.not_committed.reason.deserialization_error.index==0U );
  FD_TEST( wire_result->result.not_committed.reason.deserialization_error.reason==
           bam_types_DeserializationErrorReason_SANITIZE_ERROR );

  test_bam_env_destroy( env );
  FD_TEST( !fd_shmem_leave_anonymous( wksp_mem, NULL ) );
  FD_TEST( fd_wksp_delete( fd_wksp_leave( wksp ) )==wksp_mem );
  free( wksp_mem );
}

static void
test_pack_tile_bam_result_mapping_insert_reject( void ) {
  test_pack_tile_harness_t h[1];

  test_pack_tile_harness_new( h );

  pack_tile_publish_bam_insert_reject( h->ctx, 77U, 0U, 123UL, 2U, 0UL, FD_PACK_INSERT_REJECT_DUPLICATE );

  FD_TEST( h->out->seqs[ 0 ] == 0UL );
  FD_TEST( h->ctx->bam_pending_result_cnt == 1UL );
  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  fd_bam_bundle_result_t const * dup = test_pack_tile_assert_last_result( h, 77U, 123UL, 2U, FD_BAM_SCHED_ERR_NONE, 2U );
  FD_TEST( dup->transaction_err[ 0 ] == bam_types_TransactionErrorReason_ALREADY_PROCESSED );
  FD_TEST( dup->transaction_err[ 1 ] == bam_types_TransactionErrorReason_COMMIT_CANCELLED );
  FD_TEST( dup->sanitize_success[ 0 ] == 1U );
  FD_TEST( dup->sanitize_success[ 1 ] == 1U );

  h->ctx->bam_result_publish_cnt = 0UL;
  pack_tile_publish_bam_insert_reject( h->ctx, 79U, 0U, 125UL, 2U, 1UL, FD_PACK_INSERT_REJECT_INVALID_NONCE );

  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  fd_bam_bundle_result_t const * nonce = test_pack_tile_assert_last_result( h, 79U, 125UL, 2U, FD_BAM_SCHED_ERR_NONE, 2U );
  FD_TEST( nonce->transaction_err[ 0 ] == bam_types_TransactionErrorReason_COMMIT_CANCELLED );
  FD_TEST( nonce->transaction_err[ 1 ] == bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND );

  h->ctx->bam_result_publish_cnt = 0UL;
  pack_tile_publish_bam_insert_reject( h->ctx, 81U, 0U, 127UL, 2U, 1UL, FD_PACK_INSERT_REJECT_INSTR_ACCT_CNT );

  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  fd_bam_bundle_result_t const * instr_acct_cnt = test_pack_tile_assert_last_result( h, 81U, 127UL, 2U, FD_BAM_SCHED_ERR_NONE, 0U );
  FD_TEST( instr_acct_cnt->bundle_err   == FD_BAM_BUNDLE_ERR_DESER );
  FD_TEST( instr_acct_cnt->deser_index  == 1U );
  FD_TEST( instr_acct_cnt->deser_reason == bam_types_DeserializationErrorReason_SANITIZE_ERROR );

  h->ctx->bam_result_publish_cnt = 0UL;
  pack_tile_publish_bam_insert_reject( h->ctx, 78U, 0U, 124UL, 1U, ULONG_MAX, FD_PACK_INSERT_REJECT_PRIORITY );

  FD_TEST( h->ctx->bam_pending_result_cnt == 1UL );
  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  test_pack_tile_assert_last_result( h, 78U, 124UL, 1U, FD_BAM_SCHED_ERR_CONTAINER_FULL, 0U );

  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_bam_result_mapping_tracking_reject( void ) {
  test_pack_tile_harness_t h[1];
  fd_ed25519_sig_t         sig0[1];

  test_pack_tile_harness_new( h );
  h->ctx->highest_observed_slot = 200UL;
  test_pack_tile_fill_sig( (uchar *)sig0, 33U );

  pack_tile_publish_bam_tracking_reject( h->ctx,
                                         sig0,
                                         0L,
                                         88U,
                                         0U,
                                         200UL,
                                         201UL,
                                         199UL,
                                         1U,
                                         1U,
                                         2U );

  test_pack_tile_assert_deleted_sig( sig0 );
  FD_TEST( h->ctx->bam_tracking_rejected_cnt == 1UL );
  FD_TEST( h->ctx->bam_tracking_rejected_txn_cnt == 2UL );

  FD_TEST( h->ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_REJECTED_PRE_PENDING_IDX ] == 1UL );

  FD_TEST( h->ctx->bam_pending_result_cnt == 1UL );
  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  test_pack_tile_assert_last_result( h, 88U, 201UL, 2U, FD_BAM_SCHED_ERR_CONTAINER_FULL, 0U );

  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_bam_atomic_abandon_reports_missing_deser( void ) {
  pack_tile_bam_bundle_assembly_abandon_reason_t reasons[] = {
    PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_NEW_SEQ_BEFORE_COMPLETE,
    PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_POH_TIMEOUT,
  };

  for( ulong reason_idx=0UL; reason_idx<sizeof(reasons)/sizeof(reasons[0]); reason_idx++ ) {
    test_pack_tile_harness_t h[1];
    fd_txn_e_t               old_txns[ 2 ];

    test_pack_tile_harness_new( h );
    fd_memset( old_txns, 0, sizeof(old_txns) );

    test_pack_tile_fill_sig( old_txns[ 0 ].txnp->payload + 1UL, 1U );
    test_pack_tile_fill_sig( old_txns[ 1 ].txnp->payload + 1UL, 2U );
    old_txns[ 0 ].txnp->scheduler_arrival_time_nanos = 0L;
    old_txns[ 1 ].txnp->scheduler_arrival_time_nanos = 0L;

    h->ctx->current_bundle->id                 = 321UL + reason_idx + 1UL;
    h->ctx->current_bundle->txn_cnt            = 3UL;
    h->ctx->current_bundle->txn_received       = 2UL;
    h->ctx->current_bundle->min_blockhash_slot = 500UL;
    h->ctx->current_bundle->_txn[ 0 ]          = &old_txns[ 0 ];
    h->ctx->current_bundle->_txn[ 1 ]          = &old_txns[ 1 ];
    h->ctx->current_bundle->bundle             = h->ctx->current_bundle->_txn;
    h->ctx->current_bundle_bam->max_schedule_slot = 600UL;
    h->ctx->current_bundle_bam->is_bam            = 1;

    pack_tile_abandon_current_bam_bundle( h->ctx, reasons[ reason_idx ] );

    FD_TEST( test_bundle_cancel_call_cnt == 1UL );
    FD_TEST( test_bundle_cancel_last_txn_cnt == 3UL );
    FD_TEST( h->ctx->current_bundle->bundle == NULL );
    FD_TEST( h->ctx->current_bundle_bam->is_bam == 0 );
    FD_TEST( h->ctx->bam_pending_result_cnt == 1UL );

    fd_bam_bundle_result_t const * queued = &h->ctx->bam_result_queue[ h->ctx->bam_result_queue_head ];
    FD_TEST( queued->seq_id                == 321U + reason_idx );
    FD_TEST( queued->slot                  == 600UL );
    FD_TEST( queued->bundle_txn_cnt        == 3U );
    FD_TEST( queued->scheduling_error      == FD_BAM_SCHED_ERR_NONE );
    FD_TEST( queued->bundle_err            == FD_BAM_BUNDLE_ERR_DESER );
    FD_TEST( queued->deser_index           == 2U );
    FD_TEST( queued->deser_reason          == bam_types_DeserializationErrorReason_SANITIZE_ERROR );
    FD_TEST( queued->transaction_err_count == 0U );

    FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
    fd_bam_bundle_result_t const * published = test_pack_tile_last_result( h );
    FD_TEST( published->seq_id                == 321U + reason_idx );
    FD_TEST( published->bundle_err            == FD_BAM_BUNDLE_ERR_DESER );
    FD_TEST( published->deser_index           == 2U );
    FD_TEST( published->transaction_err_count == 0U );
    FD_TEST( h->ctx->bam_pending_result_cnt   == 0UL );
    FD_TEST( h->out->seqs[ 0 ]                == 1UL );

    h->ctx->bam_result_publish_cnt = 0UL;
    FD_TEST( !pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
    FD_TEST( h->out->seqs[ 0 ] == 1UL );

    test_pack_tile_harness_delete( h );
  }
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );
  fd_metrics_register( (ulong *)fd_metrics_new( metrics_scratch, 0UL ) );

  test_pack_tile_bam_completion_outcomes();
  test_pack_tile_bam_completion_tracking_reuses_capacity();
  test_pack_tile_bam_stale_max_schedule_slot_rejected();
  test_pack_tile_bam_expired_blockhash_reports_member_idx();
  test_pack_tile_bam_stale_results_drain_without_drop();
  test_pack_tile_bam_same_seq_pending_duplicate_replaces_before_insert();
  test_pack_tile_bam_same_seq_different_slot_pending_duplicate_rejected_before_insert();
  test_pack_tile_bam_different_seq_pending_duplicate_rejected_before_insert();
  test_pack_tile_bam_scheduled_duplicate_rejected_before_insert();
  test_pack_tile_bam_pending_result_does_not_shadow_new_work();
  test_pack_tile_bam_queued_results_preserve_fifo_before_direct_publish();
  test_pack_tile_bam_pending_results_reserve_direct_result_headroom();
  test_pack_tile_bam_scheduled_work_does_not_consume_result_headroom();
  test_pack_tile_bam_instr_acct_reject_serializes_exact_member();
  test_pack_tile_bam_result_mapping_insert_reject();
  test_pack_tile_bam_result_mapping_tracking_reject();
  test_pack_tile_bam_atomic_abandon_reports_missing_deser();
  test_pack_tile_bam_override_allows_votes_only_from_normal_ingress();
  test_pack_tile_bam_override_after_frag_preserves_votes_only();
  test_pack_tile_bam_override_drops_block_engine_bundles();
  test_pack_tile_bam_override_after_frag_cancels_block_engine_bundle();
  test_pack_tile_bam_fee_meta_seqlock_keeps_last_snapshot();
  test_pack_tile_bam_missing_builder_cfg_accepted();

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
