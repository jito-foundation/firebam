#include "../../ballet/fd_ballet.h"
#include "../metrics/fd_metrics.h"

#include <stddef.h>
#include <stdlib.h>

/* Stub pack deletion so pack-tile BAM helper tests can exercise result
   mapping without building a full fd_pack instance. */
#define fd_pack_delete_transaction test_fd_pack_delete_transaction
#include "fd_pack_tile.c"
#undef fd_pack_delete_transaction

static uchar metrics_scratch[ FD_METRICS_FOOTPRINT( 0UL ) ] __attribute__((aligned( FD_METRICS_ALIGN )));

static ulong             test_delete_call_cnt;
static fd_ed25519_sig_t  test_delete_last_sig[1];

#define TEST_PACK_TILE_DCACHE_CHUNKS 16UL
#define TEST_PACK_TILE_MCACHE_DEPTH  16UL
#define TEST_PACK_TILE_BAM_WORK_CAP   8UL

ulong
test_fd_pack_delete_transaction( fd_pack_t *                 pack,
                                 fd_ed25519_sig_t const *    sig0 ) {
  (void)pack;
  test_delete_call_cnt++;
  if( FD_UNLIKELY( !sig0 ) ) return 1UL;
  fd_memcpy( test_delete_last_sig, sig0, sizeof(fd_ed25519_sig_t) );
  return 1UL;
}

typedef struct {
  fd_frag_meta_t *  mcache;
  uchar *           dcache;
  fd_frag_meta_t *  mcaches[ 1 ];
  ulong             seqs[ 1 ];
  ulong             depths[ 1 ];
  ulong             cr_avail[ 1 ];
  ulong             min_cr_avail;
  _Bool            out_reliable[ 1 ];
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

static void
test_pack_tile_reset_delete_state( void ) {
  test_delete_call_cnt = 0UL;
  fd_memset( test_delete_last_sig, 0, sizeof(test_delete_last_sig) );
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

static pack_bam_recent_slot_t const *
test_pack_tile_recent_slot( test_pack_tile_harness_t const * h,
                            ulong                            slot ) {
  return &h->ctx->bam_recent_slot[ slot & ( FD_PACK_BAM_RECENT_SLOT_CNT - 1UL ) ];
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
  test_pack_tile_reset_delete_state();
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
}

static void
test_pack_tile_harness_delete( test_pack_tile_harness_t * h ) {
  test_pack_tile_out_delete( h->out );
  fd_memset( h, 0, sizeof(*h) );
}

static void
test_pack_tile_fill_sig( uchar sig[ static FD_ED25519_SIG_SZ ],
                         uchar seed ) {
  for( ulong i=0UL; i<sizeof(fd_ed25519_sig_t); i++ ) sig[ i ] = (uchar)( seed + i );
}

static void
test_pack_tile_bam_stale_max_schedule_slot_rejected( void ) {
  test_pack_tile_harness_t h[1];
  uchar                    sigs[ 2UL * sizeof(fd_ed25519_sig_t) ];

  test_pack_tile_harness_new( h );

  test_pack_tile_fill_sig( sigs + 0UL, 11U );
  test_pack_tile_fill_sig( sigs + sizeof(fd_ed25519_sig_t), 22U );

  FD_TEST( pack_tile_track_bam_work( h->ctx, sigs, 0L, 55U, 100UL, 100UL, 100UL, 2U ) );
  FD_TEST( h->ctx->bam_work_cnt == 1UL );
  FD_TEST( h->ctx->bam_pending_work_cnt == 1UL );

  pack_tile_evict_invalid_pending_bam_work( h->ctx, 101UL );

  test_pack_tile_assert_deleted_sig( (fd_ed25519_sig_t const *)sigs );
  FD_TEST( h->out->seqs[ 0 ] == 0UL );
  FD_TEST( h->ctx->bam_work_cnt == 0UL );
  FD_TEST( h->ctx->bam_pending_work_cnt == 0UL );
  FD_TEST( h->ctx->bam_scheduled_work_cnt == 0UL );
  FD_TEST( h->ctx->bam_pending_result_cnt == 1UL );

  pack_bam_recent_slot_t const * recent = test_pack_tile_recent_slot( h, 100UL );
  FD_TEST( recent->slot == 100UL );
  FD_TEST( recent->pending_items == 0UL );
  FD_TEST( recent->pending_txns == 0UL );
  FD_TEST( recent->evicted_post_pending_items == 1UL );
  FD_TEST( recent->evicted_post_pending_txns == 2UL );

  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  FD_TEST( h->ctx->bam_pending_result_cnt == 0UL );
  test_pack_tile_assert_last_result( h, 55U, 100UL, 2U, FD_BAM_SCHED_ERR_OUTSIDE_SLOT, 0U );

  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_bam_stale_results_drain_without_drop( void ) {
  test_pack_tile_harness_t h[1];
  uchar                    sigs[ 3 ][ sizeof(fd_ed25519_sig_t) ];

  test_pack_tile_harness_new( h );

  for( uchar i=0U; i<3U; i++ ) {
    test_pack_tile_fill_sig( sigs[ i ], (uchar)( 40U + 10U*i ) );
    FD_TEST( pack_tile_track_bam_work( h->ctx, sigs[ i ], 0L, (uint)( 100U + i ), 100UL, 100UL, 100UL, 1U ) );
  }

  pack_tile_evict_invalid_pending_bam_work( h->ctx, 101UL );

  FD_TEST( test_delete_call_cnt == 3UL );
  FD_TEST( h->out->seqs[ 0 ] == 0UL );
  FD_TEST( h->ctx->bam_work_cnt == 0UL );
  FD_TEST( h->ctx->bam_pending_work_cnt == 0UL );
  FD_TEST( h->ctx->bam_pending_result_cnt == 3UL );

  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  test_pack_tile_assert_last_result( h, 100U, 100UL, 1U, FD_BAM_SCHED_ERR_OUTSIDE_SLOT, 0U );
  FD_TEST( h->out->seqs[ 0 ] == 1UL );
  FD_TEST( h->ctx->bam_pending_result_cnt == 2UL );

  h->ctx->bam_result_publish_cnt = 0UL;
  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  test_pack_tile_assert_last_result( h, 101U, 100UL, 1U, FD_BAM_SCHED_ERR_OUTSIDE_SLOT, 0U );
  FD_TEST( h->out->seqs[ 0 ] == 2UL );
  FD_TEST( h->ctx->bam_pending_result_cnt == 1UL );

  h->ctx->bam_result_publish_cnt = 0UL;
  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  test_pack_tile_assert_last_result( h, 102U, 100UL, 1U, FD_BAM_SCHED_ERR_OUTSIDE_SLOT, 0U );
  FD_TEST( h->out->seqs[ 0 ] == 3UL );
  FD_TEST( h->ctx->bam_work_cnt == 0UL );
  FD_TEST( h->ctx->bam_pending_result_cnt == 0UL );

  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_bam_pending_result_does_not_shadow_new_work( void ) {
  test_pack_tile_harness_t h[1];
  uchar                    sig[ sizeof(fd_ed25519_sig_t) ];

  test_pack_tile_harness_new( h );

  test_pack_tile_fill_sig( sig, 70U );
  FD_TEST( pack_tile_track_bam_work( h->ctx, sig, 0L, 200U, 100UL, 100UL, 100UL, 1U ) );
  pack_tile_evict_invalid_pending_bam_work( h->ctx, 101UL );

  FD_TEST( h->ctx->bam_work_cnt == 0UL );
  FD_TEST( h->ctx->bam_pending_result_cnt == 1UL );

  FD_TEST( pack_tile_track_bam_work( h->ctx, sig, 0L, 201U, 102UL, 102UL, 102UL, 1U ) );

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
  FD_TEST( pack_tile_track_bam_work( h->ctx, sig, 0L, 100U, 100UL, 100UL, 100UL, 1U ) );
  pack_tile_evict_invalid_pending_bam_work( h->ctx, 101UL );

  pack_tile_publish_bam_insert_reject( h->ctx, 200U, 200UL, 1U, FD_PACK_INSERT_REJECT_DUPLICATE );

  FD_TEST( h->out->seqs[ 0 ] == 0UL );
  FD_TEST( h->ctx->bam_pending_result_cnt == 2UL );

  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  test_pack_tile_assert_last_result( h, 100U, 100UL, 1U, FD_BAM_SCHED_ERR_OUTSIDE_SLOT, 0U );

  h->ctx->bam_result_publish_cnt = 0UL;
  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  test_pack_tile_assert_last_result( h, 200U, 200UL, 1U, FD_BAM_SCHED_ERR_NONE, 1U );
  FD_TEST( h->ctx->bam_pending_result_cnt == 0UL );

  pack_tile_publish_bam_insert_reject( h->ctx, 201U, 201UL, 1U, FD_PACK_INSERT_REJECT_DUPLICATE );

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
    FD_TEST( pack_tile_track_bam_work( h->ctx, sigs[ i ], 0L, (uint)( 300U + i ), 100UL, 100UL, 100UL, 1U ) );
  }
  FD_TEST( h->ctx->bam_work_cnt == TEST_PACK_TILE_BAM_WORK_CAP );

  pack_tile_evict_invalid_pending_bam_work( h->ctx, 101UL );

  FD_TEST( h->ctx->bam_work_cnt == 0UL );
  FD_TEST( h->ctx->bam_pending_result_cnt == TEST_PACK_TILE_BAM_WORK_CAP );

  for( uchar i=0U; i<TEST_PACK_TILE_BAM_WORK_CAP-1U; i++ ) {
    test_pack_tile_fill_sig( sigs[ i ], (uchar)( 120U + i ) );
    FD_TEST( pack_tile_track_bam_work( h->ctx, sigs[ i ], 0L, (uint)( 400U + i ), 102UL, 102UL, 102UL, 1U ) );
  }
  test_pack_tile_fill_sig( sigs[ TEST_PACK_TILE_BAM_WORK_CAP-1U ], 130U );
  FD_TEST( !pack_tile_track_bam_work( h->ctx, sigs[ TEST_PACK_TILE_BAM_WORK_CAP-1U ], 0L, 499U, 102UL, 102UL, 102UL, 1U ) );
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
    FD_TEST( pack_tile_track_bam_work( h->ctx, sigs[ i ], 0L, (uint)( 500U + i ), ULONG_MAX, 200UL, 200UL, 1U ) );

    pack_bam_work_t * item = &h->ctx->bam_work[ i ];
    pack_tile_bam_recent_slot_sub_pending( h->ctx, item->slot, 1UL, item->txn_cnt );
    item->state = PACK_BAM_WORK_STATE_SCHEDULED;
    item->remaining_txn_cnt = item->txn_cnt;
    h->ctx->bam_pending_work_cnt--;
    h->ctx->bam_scheduled_work_cnt++;
  }

  FD_TEST( h->ctx->bam_work_cnt == 4UL );
  FD_TEST( h->ctx->bam_pending_work_cnt == 0UL );
  FD_TEST( h->ctx->bam_scheduled_work_cnt == 4UL );

  h->ctx->bam_pending_result_cnt = 2UL*TEST_PACK_TILE_BAM_WORK_CAP - h->ctx->bam_scheduled_work_cnt;

  test_pack_tile_fill_sig( sigs[ 4 ], 190U );
  FD_TEST( pack_tile_track_bam_work( h->ctx, sigs[ 4 ], 0L, 600U, 201UL, 201UL, 201UL, 1U ) );
  FD_TEST( h->ctx->bam_work_cnt == 5UL );
  FD_TEST( h->ctx->bam_pending_work_cnt == 1UL );
  FD_TEST( h->ctx->bam_scheduled_work_cnt == 4UL );

  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_bam_result_mapping_insert_reject( void ) {
  test_pack_tile_harness_t h[1];

  test_pack_tile_harness_new( h );

  pack_tile_publish_bam_insert_reject( h->ctx, 77U, 123UL, 2U, FD_PACK_INSERT_REJECT_DUPLICATE );

  FD_TEST( h->out->seqs[ 0 ] == 0UL );
  FD_TEST( h->ctx->bam_pending_result_cnt == 1UL );
  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  fd_bam_bundle_result_t const * dup = test_pack_tile_assert_last_result( h, 77U, 123UL, 2U, FD_BAM_SCHED_ERR_NONE, 1U );
  FD_TEST( dup->transaction_err[ 0 ] == bam_types_TransactionErrorReason_ALREADY_PROCESSED );
  FD_TEST( dup->sanitize_success[ 0 ] == 1U );
  FD_TEST( dup->sanitize_success[ 1 ] == 1U );

  h->ctx->bam_result_publish_cnt = 0UL;
  pack_tile_publish_bam_insert_reject( h->ctx, 78U, 124UL, 1U, FD_PACK_INSERT_REJECT_PRIORITY );

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
                                         200UL,
                                         201UL,
                                         199UL,
                                         1U,
                                         1U,
                                         2U );

  test_pack_tile_assert_deleted_sig( sig0 );
  FD_TEST( h->ctx->bam_tracking_rejected_cnt == 1UL );
  FD_TEST( h->ctx->bam_tracking_rejected_txn_cnt == 2UL );

  pack_bam_recent_slot_t const * recent = test_pack_tile_recent_slot( h, 200UL );
  FD_TEST( recent->slot == 200UL );
  FD_TEST( recent->rejected_pre_pending_items == 1UL );
  FD_TEST( recent->rejected_pre_pending_txns == 2UL );

  FD_TEST( h->ctx->bam_pending_result_cnt == 1UL );
  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  test_pack_tile_assert_last_result( h, 88U, 201UL, 2U, FD_BAM_SCHED_ERR_CONTAINER_FULL, 0U );

  test_pack_tile_harness_delete( h );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );
  fd_metrics_register( (ulong *)fd_metrics_new( metrics_scratch, 0UL ) );

  test_pack_tile_bam_stale_max_schedule_slot_rejected();
  test_pack_tile_bam_stale_results_drain_without_drop();
  test_pack_tile_bam_pending_result_does_not_shadow_new_work();
  test_pack_tile_bam_queued_results_preserve_fifo_before_direct_publish();
  test_pack_tile_bam_pending_results_reserve_direct_result_headroom();
  test_pack_tile_bam_scheduled_work_does_not_consume_result_headroom();
  test_pack_tile_bam_result_mapping_insert_reject();
  test_pack_tile_bam_result_mapping_tracking_reject();

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
