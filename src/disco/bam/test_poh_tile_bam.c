/* Regression coverage for BAM feedback at the Full Firedancer PoH
   boundary.  Include the tile implementation so the test exercises the
   live returnable-fragment callback. */

#include "../../discof/poh/fd_poh_tile.c"
#include "test_bam_poh_common.c"

static void
test_hold_execle_until_leader_then_record_bam_microblock( fd_wksp_t * wksp,
                                                          int         executed_txn ) {
  ulong const depth          = 4UL;
  ulong const completed_slot = 135UL;
  ulong const leader_slot    = completed_slot+1UL;
  uint  const pack_idx       = 0U;

  /* An execle microblock can reach PoH before replay's reset and
     become_leader for its slot, while PoH still has a later
     next_leader_slot.  PoH must hold it, not consume it, and mix it in
     once the leader bank arrives.  The executed_txn link exists only
     with BAM, so PoH also runs without it. */
  static fd_poh_tile_t ctx[1];
  fd_memset( ctx, 0, sizeof(ctx) );

  test_stem_t ts[1];
  fd_stem_context_t *      stem    = test_stem_new( ts, wksp, depth );
  ulong const *            seqs    = ts->seqs;
  fd_frag_meta_t * const * mcaches = ts->mcaches;

  *ctx->shred_out        = test_out_new( wksp, 0UL, depth, MAX_MICROBLOCK_SZ );
  *ctx->replay_out       = test_out_new( wksp, 1UL, depth, sizeof(fd_poh_leader_slot_ended_t) );
  *ctx->executed_txn_out = executed_txn ? test_out_new( wksp, 2UL, depth, FD_TXN_SIGNATURE_SZ ) : (fd_poh_out_t){ .idx = ULONG_MAX };
  *ctx->bam_out          = test_out_new( wksp, 3UL, depth, sizeof(fd_bam_bundle_result_t) );
  static uchar timing_table_storage[ FD_LEADER_TXN_TIMING_TABLE_CNT *
                                     ( sizeof(fd_leader_txn_timing_table_t)+sizeof(fd_leader_txn_timing_rec_t) ) ]
    __attribute__((aligned(alignof(fd_leader_txn_timing_table_t))));
  fd_leader_txn_timing_table_t * timing_tables = (fd_leader_txn_timing_table_t *)timing_table_storage;
  fd_memset( timing_table_storage, 0, sizeof(timing_table_storage) );
  FD_TEST( fd_poh_join( fd_poh_new( ctx->poh ), ctx->shred_out, ctx->replay_out, timing_tables, 1UL ) );

  uchar completed_hash[ 32 ] = {0};
  uchar completed_id  [ 32 ] = {1};
  fd_poh_reset( ctx->poh, stem, 0L, 62500UL, 64UL, 6250UL, completed_slot,
                completed_hash, 200UL, 1024UL, completed_id );
  FD_TEST( !fd_poh_have_leader_bank( ctx->poh ) );
  FD_TEST( ctx->poh->hashcnt_duration_ns==(double)6250UL/(double)62500UL );

  ctx->expect_pack_idx = pack_idx;
  ctx->in_kind[ 0 ]    = IN_KIND_EXECLE;
  void * in_dcache = test_dcache_new( wksp, depth, MAX_MICROBLOCK_SZ );
  ctx->in[ 0 ].mem    = wksp;
  ctx->in[ 0 ].chunk0 = fd_dcache_compact_chunk0( wksp, in_dcache );
  ctx->in[ 0 ].wmark  = fd_dcache_compact_wmark( wksp, in_dcache, MAX_MICROBLOCK_SZ );
  ctx->in[ 0 ].mtu    = MAX_MICROBLOCK_SZ;

  ulong in_chunk = ctx->in[ 0 ].chunk0;
  void * fragment = fd_chunk_to_laddr( wksp, in_chunk );
  fd_bam_bundle_result_t result = test_result( 12345U, 7U, leader_slot, 1U );
  ulong fragment_sz = test_microblock( fragment, leader_slot, result.seq_id, 1UL, &result );
  ulong sig = fd_disco_execle_sig( leader_slot, pack_idx );

  FD_TEST( returnable_frag( ctx, 0UL, 0UL, sig, in_chunk, fragment_sz, 0UL, 0UL, 0UL, stem )==1 );
  FD_TEST( ctx->expect_pack_idx==pack_idx );
  FD_TEST( seqs[ 0 ]==0UL && seqs[ 1 ]==0UL && seqs[ 2 ]==0UL && seqs[ 3 ]==0UL );

  /* Runtime BAM changes alter tick duration without changing hashes per
     tick.  Reset and leader-start must both refresh the derived clock. */
  completed_id[ 0 ] = 2U; /* main rejects duplicate reset block IDs */
  fd_poh_reset( ctx->poh, stem, 0L, 62500UL, 64UL, 5000UL, completed_slot,
                completed_hash, leader_slot, 1024UL, completed_id );
  FD_TEST( ctx->poh->hashcnt_duration_ns==(double)5000UL/(double)62500UL );
  FD_TEST( returnable_frag( ctx, 0UL, 0UL, sig, in_chunk, fragment_sz, 0UL, 0UL, 0UL, stem )==1 );
  FD_TEST( seqs[ 0 ]==0UL && seqs[ 2 ]==0UL && seqs[ 3 ]==0UL );

  fd_poh_begin_leader( ctx->poh, leader_slot, 62500UL, 64UL, 6250UL, 1024UL, 0L );
  FD_TEST( ctx->poh->hashcnt_duration_ns==(double)6250UL/(double)62500UL );

  FD_TEST( !returnable_frag( ctx, 0UL, 0UL, sig, in_chunk, fragment_sz, 0UL, 0UL, 0UL, stem ) );
  FD_TEST( ctx->expect_pack_idx==pack_idx+1U );
  FD_TEST( seqs[ 0 ]==1UL ); /* microblock recorded */
  FD_TEST( seqs[ 2 ]==(ulong)!!executed_txn );
  FD_TEST( seqs[ 3 ]==1UL );
  fd_leader_txn_timing_table_t const * timing_table = fd_leader_txn_timing_table_const( timing_tables, ctx->poh->timing_table_idx, 1UL );
  FD_TEST( timing_table->cnt==1UL );
  FD_TEST( timing_table->rec[0].received_ns==123456789L );

  fd_frag_meta_t const * meta = mcaches[ 3 ] + fd_mcache_line_idx( 0UL, depth );
  fd_bam_bundle_result_t const * accepted = fd_chunk_to_laddr_const( wksp, meta->chunk );
  FD_TEST( accepted->seq_id==result.seq_id );
  FD_TEST( accepted->execution_success );
  FD_TEST( accepted->scheduling_error==FD_BAM_SCHED_ERR_NONE );

  /* executed_txn carries only BAM completions: a landed TPU
     transaction publishes nothing on it, and an unlanded BAM
     transaction still releases pack's tracking. */
  fragment_sz = test_microblock( fragment, leader_slot, 0U, 1UL, NULL );
  ((fd_txn_p_t *)fragment)->source_tpu = FD_TXN_M_TPU_SOURCE_QUIC;
  FD_TEST( !returnable_frag( ctx, 0UL, 1UL, fd_disco_execle_sig( leader_slot, pack_idx+1U ),
                             in_chunk, fragment_sz, 0UL, 0UL, 0UL, stem ) );
  FD_TEST( seqs[ 0 ]==2UL && seqs[ 2 ]==(ulong)!!executed_txn && seqs[ 3 ]==1UL );

  fragment_sz = test_microblock( fragment, leader_slot, result.seq_id+1U, 1UL, NULL );
  ((fd_txn_p_t *)fragment)->flags = FD_TXN_P_FLAGS_SANITIZE_SUCCESS;
  FD_TEST( !returnable_frag( ctx, 0UL, 2UL, fd_disco_execle_sig( leader_slot, pack_idx+2U ),
                             in_chunk, fragment_sz, 0UL, 0UL, 0UL, stem ) );
  FD_TEST( ctx->expect_pack_idx==pack_idx+3U );
  FD_TEST( seqs[ 0 ]==2UL && seqs[ 2 ]==2UL*(ulong)!!executed_txn && seqs[ 3 ]==1UL );
  if( executed_txn ) {
    FD_TEST( mcaches[ 2 ][ fd_mcache_line_idx( 0UL, depth ) ].sig==FD_EXECUTED_TXN_KIND_LANDED );
    FD_TEST( mcaches[ 2 ][ fd_mcache_line_idx( 1UL, depth ) ].sig==FD_EXECUTED_TXN_KIND_BAM_COMPLETED_UNLANDED );
  }
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  fd_wksp_t * wksp = fd_wksp_new_anonymous( FD_SHMEM_NORMAL_PAGE_SZ, 2048UL,
                                             fd_shmem_cpu_idx( 0UL ), "poh-test", 0UL );
  FD_TEST( wksp );

  test_hold_execle_until_leader_then_record_bam_microblock( wksp, 1 );
  test_hold_execle_until_leader_then_record_bam_microblock( wksp, 0 );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
