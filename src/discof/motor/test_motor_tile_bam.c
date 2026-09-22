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

  /* A microblock from an abandoned leader slot must settle provisional
     BAM success without publishing an entry or consuming the new slot's
     pack index.  The same applies after Pack closes the current slot. */
  ctx->slot = 138UL;
  uint const expect_pack_idx = ctx->expect_pack_idx;
  result = fd_bam_result_base( 12347U, 8U, 137UL, 1U );
  result.execution_success = 1U;
  fd_bam_result_mark_sanitize_success_all( &result );
  sz = test_microblock( fragment, 137UL, result.seq_id, 1UL, &result );
  ulong sig = fd_disco_execle_sig( 137UL, 0UL );
  FD_TEST( !before_frag( ctx, 0UL, 3UL, sig ) );
  FD_TEST( !returnable_frag( ctx, 0UL, 3UL, sig, in_chunk, sz, 0UL, 0UL, 0UL, stem ) );
  FD_TEST( seqs[ 0 ]==2UL && seqs[ 2 ]==txn_cnt+2UL && seqs[ 3 ]==2UL );
  FD_TEST( ctx->expect_pack_idx==expect_pack_idx );
  result_meta = mcaches[ 3 ] + fd_mcache_line_idx( 1UL, depth );
  fd_bam_bundle_result_t const * rejected = fd_chunk_to_laddr_const( wksp, result_meta->chunk );
  FD_TEST( !rejected->execution_success && rejected->scheduling_error==FD_BAM_SCHED_ERR_POH_TIMEOUT );
  FD_TEST( rejected->seq_id==result.seq_id && rejected->slot==result.slot );

  ctx->slot_closed = 1;
  result = fd_bam_result_base( 12348U, 9U, ctx->slot, 1U );
  result.execution_success = 1U;
  fd_bam_result_mark_sanitize_success_all( &result );
  sz = test_microblock( fragment, ctx->slot, result.seq_id, 1UL, &result );
  sig = fd_disco_execle_sig( ctx->slot, expect_pack_idx );
  FD_TEST( !before_frag( ctx, 0UL, 4UL, sig ) );
  FD_TEST( !returnable_frag( ctx, 0UL, 4UL, sig, in_chunk, sz, 0UL, 0UL, 0UL, stem ) );
  FD_TEST( seqs[ 0 ]==2UL && seqs[ 2 ]==txn_cnt+2UL && seqs[ 3 ]==3UL );
  FD_TEST( ctx->expect_pack_idx==expect_pack_idx+1U ); /* consumed, so later work cannot wait on it */
  result_meta = mcaches[ 3 ] + fd_mcache_line_idx( 2UL, depth );
  rejected = fd_chunk_to_laddr_const( wksp, result_meta->chunk );
  FD_TEST( !rejected->execution_success && rejected->scheduling_error==FD_BAM_SCHED_ERR_POH_TIMEOUT );
  FD_TEST( rejected->seq_id==result.seq_id && rejected->slot==result.slot );
}

/* Pack reads replay_out and Motor replay_slot, independently.
   Exercise work that reaches Motor before its leader notice, then
   retry after the notice. */
static void
test_future_slot_handoff( fd_wksp_t * wksp ) {
  ulong const depth = 16UL;
  static fd_topo_t topo[1];
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
    link->dcache        = test_dcache_new( wksp, depth, mtus[i] );
    topo->objs[i+1UL].id = i+1UL;
    if( i<3UL ) tile->in_link_id[i] = i;
    else        tile->out_link_id[i-3UL] = i;
  }
  tile->in_cnt  = 3UL;
  tile->out_cnt = 4UL;
  unprivileged_init( topo, tile );
  fd_motor_tile_t * ctx = scratch;
  FD_TEST( ctx->slot==ULONG_MAX && !ctx->expect_pack_idx );

  fd_frag_meta_t * mcaches[4];
  for( ulong i=0UL; i<4UL; i++ ) mcaches[i] = test_mcache_new( wksp, depth );
  ulong seqs[4]             = { 0UL, 0UL, 0UL, 0UL };
  ulong depths[4]           = { depth, depth, depth, depth };
  int out_reliable[4]       = { 0, 0, 0, 0 };
  fd_stem_context_t stem[1] = {{ .mcaches=mcaches, .seqs=seqs, .depths=depths, .out_reliable=out_reliable }};

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
    FD_TEST( ctx->slot==slot && !ctx->slot_closed && ctx->expect_pack_idx==pack_idx );

    /* Stale Pack is filtered; Execle still settles any BAM result. */
    FD_TEST( before_frag( ctx, 0UL, 0UL, fd_disco_execle_sig( slot-1UL, pack_idx ) )==0 );
    FD_TEST( before_frag( ctx, 1UL, 0UL, fd_disco_execle_sig( slot-1UL, pack_idx ) )==1 );

    fd_memset( done, 0, sizeof(*done) );
    done->end_slot_reason = FD_PACK_END_SLOT_REASON_TIME;
    done->microblocks_in_slot = 1UL;
    FD_TEST( !before_frag( ctx, 1UL, 0UL, pack_sig ) );
    FD_TEST( returnable_frag( ctx, 1UL, 0UL, pack_sig, pack_chunk, sizeof(*done), 0UL, 0UL, 0UL, stem )==1 );
    FD_TEST( ctx->expect_pack_idx==pack_idx && seqs[1]==slot-1UL );

    fd_bam_bundle_result_t result = fd_bam_result_base( 90U+(uint)slot, 1U, slot, 1U );
    result.execution_success = 1U;
    fd_bam_result_mark_sanitize_success_all( &result );
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

/* A closed leader slot must not wedge Pack ordering.  With BAM, a
   dropped Execle fragment settles its result and consumes its pack
   index, so Pack's done_packing still completes the slot.  Without
   BAM, Motor keeps upstream's ordering and does not drop the frag. */
static void
test_closed_slot_pack_order( fd_wksp_t * wksp,
                             int         bam ) {
  ulong const depth = 16UL;
  static fd_topo_t topo[1];
  fd_memset( topo, 0, sizeof(topo) );
  fd_topo_tile_t * tile = topo->tiles;
  topo->workspaces[0].wksp = wksp;
  FD_TEST( fd_pod_new( topo->props, sizeof(topo->props) ) );

  void * scratch = fd_wksp_alloc_laddr( wksp, scratch_align(), scratch_footprint( tile ), 1UL );
  FD_TEST( scratch );
  fd_memset( scratch, 0xA5, scratch_footprint( tile ) );
  topo->objs[0].offset = fd_wksp_gaddr( wksp, scratch );

  char const * names[6] = { "execle_poh", "pack_poh", "replay_slot", "poh_shred", "poh_replay", "poh_bam" };
  ulong const mtus[6] = { FD_EXECLE_POH_MTU, sizeof(fd_done_packing_t), sizeof(fd_replay_message_t),
                          FD_POH_SHRED_MTU, sizeof(fd_poh_leader_slot_ended_t), sizeof(fd_bam_bundle_result_t) };
  ulong link_cnt = bam ? 6UL : 5UL;
  for( ulong i=0UL; i<link_cnt; i++ ) {
    fd_topo_link_t * link = topo->links+i;
    fd_cstr_ncpy( link->name, names[i], sizeof(link->name) );
    link->mtu           = mtus[i];
    link->dcache_obj_id = i+1UL;
    link->dcache        = test_dcache_new( wksp, depth, mtus[i] );
    topo->objs[i+1UL].id = i+1UL;
    if( i<3UL ) tile->in_link_id[i] = i;
    else        tile->out_link_id[i-3UL] = i;
  }
  tile->in_cnt  = 3UL;
  tile->out_cnt = link_cnt-3UL;
  unprivileged_init( topo, tile );
  fd_motor_tile_t * ctx = scratch;
  FD_TEST( (ctx->bam_out->idx!=ULONG_MAX)==bam );

  fd_frag_meta_t * mcaches[3];
  for( ulong i=0UL; i<3UL; i++ ) mcaches[i] = test_mcache_new( wksp, depth );
  ulong seqs[3]             = { 0UL, 0UL, 0UL };
  ulong depths[3]           = { depth, depth, depth };
  int out_reliable[3]       = { 0, 0, 0 };
  fd_stem_context_t stem[1] = {{ .mcaches=mcaches, .seqs=seqs, .depths=depths, .out_reliable=out_reliable }};

  ulong execle_chunk = ctx->in[0].chunk0;
  ulong pack_chunk   = ctx->in[1].chunk0;
  ulong replay_chunk = ctx->in[2].chunk0;
  fd_txn_p_t * txn         = fd_chunk_to_laddr( wksp, execle_chunk );
  fd_done_packing_t * done = fd_chunk_to_laddr( wksp, pack_chunk );
  void * replay_frag       = fd_chunk_to_laddr( wksp, replay_chunk );

  fd_poh_reset_t * reset = replay_frag;
  fd_memset( reset, 0, sizeof(*reset) );
  FD_TEST( !returnable_frag( ctx, 2UL, 0UL, REPLAY_SIG_RESET, replay_chunk, sizeof(*reset), 0UL, 0UL, 0UL, stem ) );
  fd_became_leader_t * leader = replay_frag;
  fd_memset( leader, 0, sizeof(*leader) );
  leader->slot = 1UL;
  FD_TEST( !returnable_frag( ctx, 2UL, 0UL, REPLAY_SIG_BECAME_LEADER, replay_chunk, sizeof(*leader), 0UL, 0UL, 0UL, stem ) );
  FD_TEST( ctx->slot==1UL && !ctx->slot_closed && !ctx->expect_pack_idx && seqs[0]==1UL );

  /* Replay closes the open slot while pack indices 0 and 1 are still in
     flight. */
  fd_memset( reset, 0, sizeof(*reset) );
  FD_TEST( !returnable_frag( ctx, 2UL, 0UL, REPLAY_SIG_RESET, replay_chunk, sizeof(*reset), 0UL, 0UL, 0UL, stem ) );
  FD_TEST( ctx->slot_closed );

  fd_bam_bundle_result_t result = fd_bam_result_base( 77U, 1U, 1UL, 1U );
  result.execution_success = 1U;
  fd_bam_result_mark_sanitize_success_all( &result );
  ulong sz = test_microblock( txn, 1UL, result.seq_id, 1UL, bam ? &result : NULL );
  ulong sig = fd_disco_execle_sig( 1UL, 1UL );
  FD_TEST( !before_frag( ctx, 0UL, 0UL, sig ) );
  if( !bam ) {
    /* Upstream ordering: index 1 waits for index 0. */
    FD_TEST( returnable_frag( ctx, 0UL, 0UL, sig, execle_chunk, sz, 0UL, 0UL, 0UL, stem )==1 );
    FD_TEST( !ctx->expect_pack_idx && seqs[0]==1UL );
    return;
  }

  /* Arrival order across Execle links is arbitrary: index 1 first. */
  FD_TEST( !returnable_frag( ctx, 0UL, 0UL, sig, execle_chunk, sz, 0UL, 0UL, 0UL, stem ) );
  FD_TEST( ctx->expect_pack_idx==2U && seqs[0]==1UL && seqs[2]==1UL );
  fd_bam_bundle_result_t const * settled = fd_chunk_to_laddr_const( wksp, mcaches[2][0].chunk );
  FD_TEST( !settled->execution_success && settled->scheduling_error==FD_BAM_SCHED_ERR_POH_TIMEOUT );

  fd_memset( done, 0, sizeof(*done) );
  sig = fd_disco_execle_sig( 1UL, 2UL );
  FD_TEST( !before_frag( ctx, 1UL, 0UL, sig ) );
  FD_TEST( !returnable_frag( ctx, 1UL, 0UL, sig, pack_chunk, sizeof(*done), 0UL, 0UL, 0UL, stem ) );
  FD_TEST( ctx->expect_pack_idx==3U && seqs[1]==1UL );

  /* Index 0 arrives late and is settled without an ordering fault. */
  sz = test_microblock( txn, 1UL, result.seq_id+1U, 1UL, &result );
  sig = fd_disco_execle_sig( 1UL, 0UL );
  FD_TEST( !returnable_frag( ctx, 0UL, 0UL, sig, execle_chunk, sz, 0UL, 0UL, 0UL, stem ) );
  FD_TEST( ctx->expect_pack_idx==3U && seqs[0]==1UL && seqs[2]==2UL );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  fd_wksp_t * wksp = fd_wksp_new_anonymous( FD_SHMEM_NORMAL_PAGE_SZ, 4096UL,
                                             fd_shmem_cpu_idx( 0UL ), "motor-test", 0UL );
  FD_TEST( wksp );

  test_bam_and_tpu_microblocks( wksp, 1UL );
  /* A full non-revert BAM batch carries all transactions and its result
     in one fragment.  Exercise the actual execle link capacity. */
  test_bam_and_tpu_microblocks( wksp, FD_BAM_MAX_TXN_PER_ATOMIC_BATCH );
  test_future_slot_handoff( wksp );
  test_closed_slot_pack_order( wksp, 1 );
  test_closed_slot_pack_order( wksp, 0 );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
