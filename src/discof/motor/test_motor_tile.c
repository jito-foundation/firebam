/* Exercise BAM feedback through the Alpenglow entry publication boundary. */
#include "fd_motor_tile.c"
#include "../../util/tmpl/fd_unit_test.c"

int volatile const fd_startup_skip_checks = 1;

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
                 ulong                          txn_cnt,
                 fd_bam_bundle_result_t const * result ) {
  fd_txn_p_t * txns = fragment;
  for( ulong i=0UL; i<txn_cnt; i++ ) {
    fd_txn_p_t * txn = txns+i;
    fd_memset( txn, 0, sizeof(fd_txn_p_t) );
    txn->payload_sz = FD_TXN_SIGNATURE_SZ;
    txn->source_tpu = FD_TXN_M_TPU_SOURCE_BAM;
    txn->flags      = FD_TXN_P_FLAGS_SANITIZE_SUCCESS | FD_TXN_P_FLAGS_EXECUTE_SUCCESS;
    txn->bam.seq_id = seq_id;
    TXN(txn)->signature_off = 0U;
    fd_memset( txn->payload, (int)(seq_id+i), FD_TXN_SIGNATURE_SZ );
  }

  fd_microblock_trailer_t * trailer = fd_bam_microblock_prepare_trailer( fragment, txn_cnt, result );
  fd_memset( trailer, 0, sizeof(fd_microblock_trailer_t) );
  trailer->hash[ 0 ] = (uchar)slot;
  return fd_bam_microblock_footprint( txn_cnt, !!result );
}

static void
test_bam_and_tpu_microblocks( fd_wksp_t * wksp,
                             ulong       txn_cnt ) {
  ulong const depth = 8UL;
  static fd_motor_tile_t ctx[1];
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

  *ctx->shred_out        = test_out_new( wksp, 0UL, depth, FD_POH_SHRED_MTU );
  *ctx->replay_out       = test_out_new( wksp, 1UL, depth, sizeof(fd_poh_leader_slot_ended_t) );
  *ctx->executed_txn_out = test_out_new( wksp, 2UL, depth, FD_TXN_SIGNATURE_SZ );
  *ctx->bam_out          = test_out_new( wksp, 3UL, depth, sizeof(fd_bam_bundle_result_t) );
  ctx->slot = 137UL;
  ctx->parent_slot = 136UL;
  ctx->in_kind[ 0 ] = IN_KIND_EXECLE;
  void * in_dcache = test_dcache_new( wksp, depth, FD_EXECLE_POH_MTU );
  ctx->in[ 0 ].mem    = wksp;
  ctx->in[ 0 ].chunk0 = fd_dcache_compact_chunk0( wksp, in_dcache );
  ctx->in[ 0 ].wmark  = fd_dcache_compact_wmark( wksp, in_dcache, FD_EXECLE_POH_MTU );
  ctx->in[ 0 ].mtu    = FD_EXECLE_POH_MTU;
  ulong in_chunk = ctx->in[ 0 ].chunk0;
  void * fragment = fd_chunk_to_laddr( wksp, in_chunk );

  /* An extended BAM microblock must publish its entry, durable result,
     and landed signature.  A subsequent rejected BAM transaction only
     releases pack/dedup ownership, without creating an empty entry. */
  fd_bam_bundle_result_t result = fd_bam_result_base( 12346U, 7U, ctx->slot, (uchar)txn_cnt );
  result.execution_success = 1U;
  fd_bam_result_mark_sanitize_success_all( &result );
  ulong sz = test_microblock( fragment, ctx->slot, result.seq_id, txn_cnt, &result );
  FD_TEST( !returnable_frag( ctx, 0UL, 0UL, fd_disco_execle_sig( ctx->slot, 0UL ),
                             in_chunk, sz, 0UL, 0UL, 0UL, stem ) );
  FD_TEST( seqs[ 0 ]==1UL && seqs[ 2 ]==txn_cnt && seqs[ 3 ]==1UL );
  fd_frag_meta_t const * result_meta = mcaches[ 3 ] + fd_mcache_line_idx( 0UL, depth );
  fd_bam_bundle_result_t const * accepted = fd_chunk_to_laddr_const( wksp, result_meta->chunk );
  FD_TEST( fd_memeq( accepted, &result, sizeof(result) ) );
  fd_frag_meta_t const * entry_meta = mcaches[ 0 ] + fd_mcache_line_idx( 0UL, depth );
  fd_entry_batch_meta_t const * entry_batch = fd_chunk_to_laddr_const( wksp, entry_meta->chunk );
  fd_entry_batch_header_t const * entry = (fd_entry_batch_header_t const *)(entry_batch+1);
  FD_TEST( entry->txn_cnt==txn_cnt );
  for( ulong i=0UL; i<txn_cnt; i++ ) {
    uchar const * payload = ((fd_txn_p_t *)fragment)[ i ].payload;
    FD_TEST( fd_memeq( (uchar const *)(entry+1) + i*FD_TXN_SIGNATURE_SZ, payload, FD_TXN_SIGNATURE_SZ ) );
    fd_frag_meta_t const * landed_meta = mcaches[ 2 ] + fd_mcache_line_idx( i, depth );
    FD_TEST( landed_meta->sig==FD_EXECUTED_TXN_KIND_LANDED );
    FD_TEST( fd_memeq( fd_chunk_to_laddr_const( wksp, landed_meta->chunk ), payload, FD_TXN_SIGNATURE_SZ ) );
  }

  sz = test_microblock( fragment, ctx->slot, result.seq_id+1U, 1UL, NULL );
  ((fd_txn_p_t *)fragment)->flags = FD_TXN_P_FLAGS_SANITIZE_SUCCESS;
  FD_TEST( !returnable_frag( ctx, 0UL, 1UL, fd_disco_execle_sig( ctx->slot, 1UL ),
                             in_chunk, sz, 0UL, 0UL, 0UL, stem ) );
  FD_TEST( seqs[ 0 ]==1UL && seqs[ 2 ]==txn_cnt+1UL && seqs[ 3 ]==1UL );
  FD_TEST( mcaches[ 2 ][ fd_mcache_line_idx( txn_cnt, depth ) ].sig==FD_EXECUTED_TXN_KIND_BAM_COMPLETED_UNLANDED );

  /* Ordinary TPU microblocks use the same parser without BAM feedback. */
  sz = test_microblock( fragment, ctx->slot, result.seq_id+2U, 1UL, NULL );
  ((fd_txn_p_t *)fragment)->source_tpu = FD_TXN_M_TPU_SOURCE_QUIC;
  FD_TEST( !returnable_frag( ctx, 0UL, 2UL, fd_disco_execle_sig( ctx->slot, 2UL ),
                             in_chunk, sz, 0UL, 0UL, 0UL, stem ) );
  FD_TEST( seqs[ 0 ]==2UL && seqs[ 2 ]==txn_cnt+2UL && seqs[ 3 ]==1UL );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  fd_wksp_t * wksp = fd_wksp_new_anonymous( FD_SHMEM_NORMAL_PAGE_SZ, 2048UL,
                                             fd_shmem_cpu_idx( 0UL ), "motor-test", 0UL );
  FD_TEST( wksp );

  test_bam_and_tpu_microblocks( wksp, 1UL );
  /* A full non-revert BAM batch carries all transactions and its result
     in one fragment.  Exercise the actual execle link capacity. */
  test_bam_and_tpu_microblocks( wksp, FD_BAM_MAX_TXN_PER_ATOMIC_BATCH );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
