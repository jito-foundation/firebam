/* Regression coverage for stale BAM feedback at the Full Firedancer PoH
   boundary.  Include the tile implementation so the test exercises the
   live returnable-fragment callback. */

#include "fd_poh_tile.c"
#include "../../util/tmpl/fd_unit_test.c"

int volatile const fd_startup_skip_checks = 1; /* fd_startup.c */

static void *
test_dcache_new( fd_wksp_t * wksp,
                 ulong       depth,
                 ulong       mtu ) {
  ulong data_sz = fd_dcache_req_data_sz( mtu, depth, 1UL, 1 );
  void * mem = fd_wksp_alloc_laddr( wksp, fd_dcache_align(), fd_dcache_footprint( data_sz, 0UL ), 1UL );
  FD_TEST( mem );
  void * dcache = fd_dcache_join( fd_dcache_new( mem, data_sz, 0UL ) );
  FD_TEST( dcache );
  return dcache;
}

static fd_frag_meta_t *
test_mcache_new( fd_wksp_t * wksp,
                 ulong       depth ) {
  void * mem = fd_wksp_alloc_laddr( wksp, fd_mcache_align(), fd_mcache_footprint( depth, 0UL ), 1UL );
  FD_TEST( mem );
  fd_frag_meta_t * mcache = fd_mcache_join( fd_mcache_new( mem, depth, 0UL, 0UL ) );
  FD_TEST( mcache );
  return mcache;
}

static fd_poh_out_t
test_out_new( fd_wksp_t * wksp,
              ulong       idx,
              ulong       depth,
              ulong       mtu ) {
  void * dcache = test_dcache_new( wksp, depth, mtu );
  ulong chunk0 = fd_dcache_compact_chunk0( wksp, dcache );
  return (fd_poh_out_t) {
    .idx    = idx,
    .mem    = wksp,
    .chunk0 = chunk0,
    .wmark  = fd_dcache_compact_wmark( wksp, dcache, mtu ),
    .chunk  = chunk0,
  };
}

static ulong
test_microblock( void *                         fragment,
                 ulong                          slot,
                 uint                           seq_id,
                 fd_bam_bundle_result_t const * result ) {
  fd_txn_p_t * txn = fragment;
  fd_memset( txn, 0, sizeof(fd_txn_p_t) );
  txn->payload_sz = FD_TXN_SIGNATURE_SZ;
  txn->source_tpu = FD_TXN_M_TPU_SOURCE_BAM;
  txn->flags      = FD_TXN_P_FLAGS_SANITIZE_SUCCESS | FD_TXN_P_FLAGS_EXECUTE_SUCCESS;
  txn->bam.seq_id = seq_id;
  TXN(txn)->signature_off = 0U;
  fd_memset( txn->payload, (int)seq_id, FD_TXN_SIGNATURE_SZ );

  fd_microblock_trailer_t * trailer = fd_bam_microblock_prepare_trailer( fragment, 1UL, result );
  fd_memset( trailer, 0, sizeof(fd_microblock_trailer_t) );
  trailer->hash[ 0 ] = (uchar)slot;
  return fd_bam_microblock_footprint( 1UL, !!result );
}

static void
test_reset_rejects_stale_then_records_live_bam_microblock( fd_wksp_t * wksp ) {
  ulong const depth      = 4UL;
  ulong const stale_slot = 136UL;
  ulong const live_slot  = stale_slot+1UL;
  uint  const stale_idx  = 0U;
  uint  const live_idx   = stale_idx+1U;

  /* A reset can overtake an executed BAM microblock.  PoH must reject
     that stale success, then accept and record the next live fragment. */
  static fd_poh_tile_t ctx[1];
  fd_memset( ctx, 0, sizeof(ctx) );

  fd_frag_meta_t * mcaches[ 4 ];
  for( ulong i=0UL; i<4UL; i++ ) mcaches[ i ] = test_mcache_new( wksp, depth );
  ulong seqs[ 4 ]         = { 0UL, 0UL, 0UL, 0UL };
  ulong depths[ 4 ]       = { depth, depth, depth, depth };
  ulong cr_avail[ 4 ]     = { ULONG_MAX, ULONG_MAX, ULONG_MAX, ULONG_MAX };
  ulong min_cr_avail      = ULONG_MAX;
  int out_reliable[ 4 ]   = { 0, 0, 0, 0 };
  fd_stem_context_t stem[1] = {{
    .mcaches             = mcaches,
    .seqs                = seqs,
    .depths              = depths,
    .cr_avail            = cr_avail,
    .min_cr_avail        = &min_cr_avail,
    .cr_decrement_amount = 1UL,
    .out_reliable        = out_reliable,
  }};

  *ctx->shred_out        = test_out_new( wksp, 0UL, depth, MAX_MICROBLOCK_SZ );
  *ctx->replay_out       = test_out_new( wksp, 1UL, depth, sizeof(fd_poh_leader_slot_ended_t) );
  *ctx->executed_txn_out = test_out_new( wksp, 2UL, depth, FD_TXN_SIGNATURE_SZ );
  *ctx->bam_out          = test_out_new( wksp, 3UL, depth, sizeof(fd_bam_bundle_result_t) );
  FD_TEST( fd_poh_join( fd_poh_new( ctx->poh ), ctx->shred_out, ctx->replay_out, NULL ) );

  uchar completed_hash[ 32 ] = {0};
  uchar completed_id  [ 32 ] = {1};
  fd_poh_reset( ctx->poh, stem, 0L, 62500UL, 64UL, 6250UL, stale_slot,
                completed_hash, live_slot, 1024UL, completed_id );
  FD_TEST( !fd_poh_have_leader_bank( ctx->poh ) );

  ctx->expect_pack_idx = UINT_MAX; /* stale consumption crosses the uint wrap */
  ctx->in_kind[ 0 ]    = IN_KIND_EXECLE;
  void * in_dcache = test_dcache_new( wksp, depth, MAX_MICROBLOCK_SZ );
  ctx->in[ 0 ].mem    = wksp;
  ctx->in[ 0 ].chunk0 = fd_dcache_compact_chunk0( wksp, in_dcache );
  ctx->in[ 0 ].wmark  = fd_dcache_compact_wmark( wksp, in_dcache, MAX_MICROBLOCK_SZ );
  ctx->in[ 0 ].mtu    = MAX_MICROBLOCK_SZ;

  ulong in_chunk = ctx->in[ 0 ].chunk0;
  void * fragment = fd_chunk_to_laddr( wksp, in_chunk );
  fd_bam_bundle_result_t stale_result = fd_bam_result_base( 12345U, 7U, stale_slot, 1U );
  stale_result.execution_success = 1U;
  fd_bam_result_mark_sanitize_success_all( &stale_result );
  ulong fragment_sz = test_microblock( fragment, stale_slot, stale_result.seq_id, &stale_result );

  FD_TEST( !returnable_frag( ctx, 0UL, 0UL, fd_disco_execle_sig( stale_slot, stale_idx ),
                             in_chunk, fragment_sz, 0UL, 0UL, 0UL, stem ) );
  FD_TEST( ctx->expect_pack_idx==live_idx );
  FD_TEST( seqs[ 0 ]==0UL ); /* no stale ledger publication */
  FD_TEST( seqs[ 2 ]==0UL ); /* no stale executed notification */
  FD_TEST( seqs[ 3 ]==1UL );

  fd_frag_meta_t const * stale_meta = mcaches[ 3 ] + fd_mcache_line_idx( 0UL, depth );
  fd_bam_bundle_result_t const * rejected = fd_chunk_to_laddr_const( wksp, stale_meta->chunk );
  FD_TEST( rejected->seq_id==stale_result.seq_id );
  FD_TEST( !rejected->execution_success );
  FD_TEST( rejected->scheduling_error==FD_BAM_SCHED_ERR_POH_TIMEOUT );

  fd_poh_begin_leader( ctx->poh, live_slot, 62500UL, 64UL, 6250UL, 1024UL, 0L );
  fd_bam_bundle_result_t live_result = fd_bam_result_base( 12346U, 7U, live_slot, 1U );
  live_result.execution_success = 1U;
  fd_bam_result_mark_sanitize_success_all( &live_result );
  fragment_sz = test_microblock( fragment, live_slot, live_result.seq_id, &live_result );

  FD_TEST( !returnable_frag( ctx, 0UL, 1UL, fd_disco_execle_sig( live_slot, live_idx ),
                             in_chunk, fragment_sz, 0UL, 0UL, 0UL, stem ) );
  FD_TEST( ctx->expect_pack_idx==live_idx+1U );
  FD_TEST( seqs[ 0 ]==1UL ); /* live microblock recorded */
  FD_TEST( seqs[ 2 ]==1UL );
  FD_TEST( seqs[ 3 ]==2UL );

  fd_frag_meta_t const * live_meta = mcaches[ 3 ] + fd_mcache_line_idx( 1UL, depth );
  fd_bam_bundle_result_t const * accepted = fd_chunk_to_laddr_const( wksp, live_meta->chunk );
  FD_TEST( accepted->seq_id==live_result.seq_id );
  FD_TEST( accepted->execution_success );
  FD_TEST( accepted->scheduling_error==FD_BAM_SCHED_ERR_NONE );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  fd_wksp_t * wksp = fd_wksp_new_anonymous( FD_SHMEM_NORMAL_PAGE_SZ, 2048UL,
                                             fd_shmem_cpu_idx( 0UL ), "poh-test", 0UL );
  FD_TEST( wksp );

  test_reset_rejects_stale_then_records_live_bam_microblock( wksp );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
