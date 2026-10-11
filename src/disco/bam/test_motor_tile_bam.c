/* Exercise BAM feedback through the Alpenglow entry publication boundary. */
#include "../../discof/motor/fd_motor_tile.c"
#include "test_bam_poh_common.c"

/* Motor initialized from a topology with its execle, pack and replay
   inputs and its shred, replay, executed_txn and BAM outputs. */
static fd_motor_tile_t *
test_motor_new( fd_wksp_t * wksp ) {
  static fd_topo_t topo[1];
  fd_memset( topo, 0, sizeof(topo) );
  fd_topo_tile_t * tile = topo->tiles;
  topo->workspaces[0].wksp = wksp;
  FD_TEST( fd_pod_new( topo->props, sizeof(topo->props) ) );

  void * scratch = fd_wksp_alloc_laddr( wksp, scratch_align(), scratch_footprint( tile ), 1UL );
  FD_TEST( scratch );
  fd_memset( scratch, 0xA5, scratch_footprint( tile ) );
  topo->objs[0].offset = fd_wksp_gaddr( wksp, scratch );

  char const * names[7] = { "execle_poh", "pack_poh", "replay_slot", "poh_shred", "poh_replay", "executed_txn", "poh_bam" };
  ulong const mtus[7] = { FD_EXECLE_POH_MTU, sizeof(fd_done_packing_t), sizeof(fd_replay_message_t),
                         FD_POH_SHRED_MTU, sizeof(fd_poh_leader_slot_ended_t), FD_TXN_SIGNATURE_SZ, sizeof(fd_bam_bundle_result_t) };
  for( ulong i=0UL; i<7UL; i++ ) {
    fd_topo_link_t * link = topo->links+i;
    fd_cstr_ncpy( link->name, names[i], sizeof(link->name) );
    link->mtu           = mtus[i];
    link->dcache_obj_id = i+1UL;
    link->dcache        = test_dcache_new( wksp, 16UL, mtus[i] );
    topo->objs[i+1UL].id = i+1UL;
    if( i<3UL ) tile->in_link_id[i] = i;
    else        tile->out_link_id[i-3UL] = i;
  }
  tile->in_cnt  = 3UL;
  tile->out_cnt = 4UL;
  unprivileged_init( topo, tile );
  return scratch;
}

static void
test_bam_and_tpu_microblocks( fd_wksp_t * wksp,
                             ulong       txn_cnt ) {
  ulong const depth = 8UL;
  static fd_motor_tile_t ctx[1];
  fd_memset( ctx, 0, sizeof(ctx) );
  test_stem_t ts[1];
  fd_stem_context_t *      stem    = test_stem_new( ts, wksp, depth );
  ulong const *            seqs    = ts->seqs;
  fd_frag_meta_t * const * mcaches = ts->mcaches;

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
  fd_bam_bundle_result_t result = test_result( 12346U, 7U, ctx->slot, (uchar)txn_cnt );
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

  /* Ordinary TPU microblocks use the same parser without BAM feedback
     or executed_txn completions. */
  sz = test_microblock( fragment, ctx->slot, result.seq_id+2U, 1UL, NULL );
  ((fd_txn_p_t *)fragment)->source_tpu = FD_TXN_M_TPU_SOURCE_QUIC;
  FD_TEST( !returnable_frag( ctx, 0UL, 2UL, fd_disco_execle_sig( ctx->slot, 2UL ),
                             in_chunk, sz, 0UL, 0UL, 0UL, stem ) );
  FD_TEST( seqs[ 0 ]==2UL && seqs[ 2 ]==txn_cnt+1UL && seqs[ 3 ]==1UL );
}

/* Pack reads replay_out and Motor replay_slot, independently.
   Exercise work that reaches Motor before its leader notice, then
   retry after the notice. */
static void
test_future_slot_handoff( fd_wksp_t * wksp ) {
  fd_motor_tile_t * ctx = test_motor_new( wksp );
  FD_TEST( ctx->slot==ULONG_MAX && !ctx->expect_pack_idx );

  test_stem_t ts[1];
  fd_stem_context_t *      stem    = test_stem_new( ts, wksp, 16UL );
  ulong const *            seqs    = ts->seqs;
  fd_frag_meta_t * const * mcaches = ts->mcaches;

  ulong execle_chunk = ctx->in[0].chunk0;
  ulong pack_chunk   = ctx->in[1].chunk0;
  ulong replay_chunk = ctx->in[2].chunk0;
  fd_txn_p_t * txn           = fd_chunk_to_laddr( wksp, execle_chunk );
  fd_done_packing_t * done   = fd_chunk_to_laddr( wksp, pack_chunk );
  void * replay_frag        = fd_chunk_to_laddr( wksp, replay_chunk );

  /* Slot zero must also wait for the first leader notice. */
  FD_TEST( before_frag( ctx, 0UL, 0UL, fd_disco_execle_sig( 0UL, 0UL ) )==-1 );
  FD_TEST( before_frag( ctx, 1UL, 0UL, fd_disco_execle_sig( 0UL, 0UL ) )==-1 );

  for( ulong slot=1UL; slot<=2UL; slot++ ) {
    uint pack_idx = ctx->expect_pack_idx;
    ulong execle_sig = fd_disco_execle_sig( slot, pack_idx );
    ulong pack_sig   = fd_disco_execle_sig( slot, pack_idx+1U );
    FD_TEST( before_frag( ctx, 0UL, 0UL, execle_sig )==-1 );
    FD_TEST( before_frag( ctx, 1UL, 0UL, pack_sig )==-1 );
    FD_TEST( before_frag( ctx, 1UL, 0UL, FD_PACK_MSG_DONE_DRAINING )==1 );
    FD_TEST( before_frag( ctx, 1UL, 0UL, FD_PACK_MSG_REDUCE_MB_BOUND )==1 );

    fd_poh_reset_t * reset = replay_frag;
    fd_memset( reset, 0, sizeof(*reset) );
    reset->completed_slot = slot-1UL;
    FD_TEST( !returnable_frag( ctx, 2UL, 0UL, REPLAY_SIG_RESET, replay_chunk, sizeof(*reset), 0UL, 0UL, 0UL, stem ) );
    fd_became_leader_t * leader = replay_frag;
    fd_memset( leader, 0, sizeof(*leader) );
    leader->slot = slot;
    FD_TEST( !returnable_frag( ctx, 2UL, 0UL, REPLAY_SIG_BECAME_LEADER, replay_chunk, sizeof(*leader), 0UL, 0UL, 0UL, stem ) );
    FD_TEST( ctx->slot==slot && ctx->expect_pack_idx==pack_idx );

    fd_memset( done, 0, sizeof(*done) );
    done->end_slot_reason = FD_PACK_END_SLOT_REASON_TIME;
    done->microblocks_in_slot = 1UL;
    FD_TEST( !before_frag( ctx, 1UL, 0UL, pack_sig ) );
    FD_TEST( returnable_frag( ctx, 1UL, 0UL, pack_sig, pack_chunk, sizeof(*done), 0UL, 0UL, 0UL, stem )==1 );
    FD_TEST( ctx->expect_pack_idx==pack_idx && seqs[1]==slot-1UL );

    fd_bam_bundle_result_t result = test_result( 90U+(uint)slot, 1U, slot, 1U );
    ulong sz = test_microblock( txn, slot, result.seq_id, 1UL, &result );
    FD_TEST( !before_frag( ctx, 0UL, 0UL, execle_sig ) );
    FD_TEST( !returnable_frag( ctx, 0UL, 0UL, execle_sig, execle_chunk, sz, 0UL, 0UL, 0UL, stem ) );
    FD_TEST( ctx->expect_pack_idx==pack_idx+1U && seqs[0]==4UL*(slot-1UL)+2UL );
    FD_TEST( seqs[2]==slot && seqs[3]==slot );
    fd_bam_bundle_result_t const * accepted = fd_chunk_to_laddr_const( wksp, mcaches[3][slot-1UL].chunk );
    FD_TEST( fd_memeq( accepted, &result, sizeof(result) ) );
    FD_TEST( !returnable_frag( ctx, 1UL, 0UL, pack_sig, pack_chunk, sizeof(*done), 0UL, 0UL, 0UL, stem ) );
    FD_TEST( ctx->expect_pack_idx==pack_idx+2U && seqs[1]==slot );
    fd_poh_leader_slot_ended_t const * ended = fd_chunk_to_laddr_const( wksp, mcaches[1][slot-1UL].chunk );
    FD_TEST( ended->completed && ended->slot==slot && ended->microblock_count==1UL );

    fd_replay_leader_footer_t * footer = replay_frag;
    fd_memset( footer, 0, sizeof(*footer) );
    footer->slot = slot;
    FD_TEST( !returnable_frag( ctx, 2UL, 0UL, REPLAY_SIG_LEADER_FOOTER, replay_chunk, sizeof(*footer), 0UL, 0UL, 0UL, stem ) );
    FD_TEST( seqs[0]==4UL*slot && seqs[1]==slot );
    fd_entry_batch_meta_t const * meta = fd_chunk_to_laddr_const( wksp, mcaches[0][4UL*slot-1UL].chunk );
    FD_TEST( meta->block_complete==1 );
  }

}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  fd_wksp_t * wksp = fd_wksp_new_anonymous( FD_SHMEM_NORMAL_PAGE_SZ, 4096UL,
                                             fd_shmem_cpu_idx( 0UL ), "motor-test", 0UL );
  FD_TEST( wksp );

  test_bam_and_tpu_microblocks( wksp, 1UL );
  test_future_slot_handoff( wksp );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
