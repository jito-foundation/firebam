/* Exercise BAM feedback through the Alpenglow entry publication boundary.
   test_motor_tile.c is included verbatim for its fixtures; its main is
   renamed and not run here. */
#define main test_motor_tile_upstream_main
int test_motor_tile_upstream_main( int argc, char ** argv );
#include "../../discof/motor/test_motor_tile.c"
#undef main

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

/* Every output is unreliable, so publishing never touches credits. */

typedef struct {
  fd_frag_meta_t *  mcaches[ 4 ];
  ulong             seqs[ 4 ];
  ulong             depths[ 4 ];
  int               out_reliable[ 4 ];
  fd_stem_context_t stem[1];
} test_stem_t;

static fd_stem_context_t *
test_stem_new( test_stem_t * s,
               fd_wksp_t *   wksp,
               ulong         depth ) {
  fd_memset( s, 0, sizeof(test_stem_t) );
  for( ulong i=0UL; i<4UL; i++ ) {
    s->mcaches[ i ] = test_mcache_new( wksp, depth );
    s->depths[ i ] = depth;
  }
  *s->stem = (fd_stem_context_t){ .mcaches=s->mcaches, .seqs=s->seqs, .depths=s->depths, .out_reliable=s->out_reliable };
  return s->stem;
}

/* A provisional all-success BAM result, as execle attaches it. */

static fd_bam_bundle_result_t
test_result( uint   seq_id,
             ushort scheduler_gen,
             ulong  slot,
             uchar  txn_cnt ) {
  fd_bam_bundle_result_t result = fd_bam_result_base( seq_id, scheduler_gen, slot, txn_cnt );
  result.execution_success = 1U;
  fd_bam_result_mark_sanitize_success_all( &result );
  return result;
}

/* Writes an executed microblock of txn_cnt BAM transactions, with
   result attached if non-NULL, and returns its size. */

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
    txn->first_seen_nanos             = 123456789L;
    txn->scheduler_arrival_time_nanos = 987654321L;
    TXN(txn)->signature_off = 0U;
    fd_memset( txn->payload, (int)(seq_id+i), FD_TXN_SIGNATURE_SZ );
  }

  fd_microblock_trailer_t * trailer = (fd_microblock_trailer_t *)( txns+txn_cnt );
  fd_memset( trailer, 0, sizeof(fd_microblock_trailer_t) );
  trailer->hash[ 0 ] = (uchar)slot;
  if( result ) fd_bam_microblock_attach_result( trailer, result );
  return fd_bam_microblock_footprint( txn_cnt, !!result );
}

/* Motor initialized from a topology with its execle, pack and replay
   inputs and its shred, replay and BAM outputs. */
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

  char const * names[6] = { "execle_poh", "pack_poh", "replay_slot", "poh_shred", "poh_replay", "motor_bam" };
  ulong const mtus[6] = { FD_EXECLE_POH_MTU, sizeof(fd_done_packing_t), sizeof(fd_replay_message_t),
                         FD_POH_SHRED_MTU, sizeof(fd_poh_leader_slot_ended_t), sizeof(fd_bam_bundle_result_t) };
  for( ulong i=0UL; i<6UL; i++ ) {
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
  tile->out_cnt = 3UL;
  unprivileged_init( topo, tile );
  return scratch;
}

/* A not-committed result, as execle attaches it to a failed batch. */

static fd_bam_bundle_result_t
test_failed_result( uint   seq_id,
                    ushort scheduler_gen,
                    ulong  slot ) {
  fd_bam_bundle_result_t result = fd_bam_result_base( seq_id, scheduler_gen, slot, 1U );
  fd_bam_result_mark_not_committed_txn_error( &result, 0UL, bam_types_TransactionErrorReason_INSUFFICIENT_FUNDS_FOR_FEE );
  fd_bam_result_mark_sanitize_success_all( &result );
  return result;
}

/* Motor with its execle input 0, a pack input 1 sharing its dcache,
   and its shred, replay and BAM outputs 0, 1 and 2 (bam_out_idx). */

static fd_motor_tile_t *
test_motor_manual_new( fd_wksp_t *   wksp,
                       test_stem_t * ts,
                       ulong         depth,
                       ulong         bam_out_idx ) {
  static fd_motor_tile_t ctx[1];
  fd_memset( ctx, 0, sizeof(ctx) );
  test_stem_new( ts, wksp, depth );
  *ctx->shred_out  = test_out_new( wksp, 0UL, depth, FD_POH_SHRED_MTU );
  *ctx->replay_out = test_out_new( wksp, 1UL, depth, sizeof(fd_poh_leader_slot_ended_t) );
  *ctx->bam_out    = test_out_new( wksp, bam_out_idx, depth, sizeof(fd_bam_bundle_result_t) );
  static uchar timing_storage[ FD_LEADER_TXN_TIMING_TABLE_CNT *
                               ( sizeof(fd_leader_txn_timing_table_t)+sizeof(fd_leader_txn_timing_rec_t) ) ]
    __attribute__((aligned(alignof(fd_leader_txn_timing_table_t))));
  fd_memset( timing_storage, 0, sizeof(timing_storage) );
  ctx->timing_tables = (fd_leader_txn_timing_table_t *)timing_storage;
  ctx->timing_table_max = 1UL;
  ctx->slot = 137UL;
  ctx->parent_slot = 136UL;
  void * in_dcache = test_dcache_new( wksp, depth, FD_EXECLE_POH_MTU );
  for( ulong i=0UL; i<2UL; i++ ) {
    ctx->in_kind[ i ] = i ? IN_KIND_PACK : IN_KIND_EXECLE;
    ctx->in[ i ].mem    = wksp;
    ctx->in[ i ].chunk0 = fd_dcache_compact_chunk0( wksp, in_dcache );
    ctx->in[ i ].wmark  = fd_dcache_compact_wmark( wksp, in_dcache, FD_EXECLE_POH_MTU );
    ctx->in[ i ].mtu    = FD_EXECLE_POH_MTU;
  }
  return ctx;
}

static fd_bam_bundle_result_t const *
test_published_result( test_stem_t const * ts,
                       fd_wksp_t *         wksp,
                       ulong               depth,
                       ulong               seq ) {
  fd_frag_meta_t const * meta = ts->mcaches[ 2 ] + fd_mcache_line_idx( seq, depth );
  FD_TEST( meta->sz==sizeof(fd_bam_bundle_result_t) );
  return fd_chunk_to_laddr_const( wksp, meta->chunk );
}

static void
test_bam_and_tpu_microblocks( fd_wksp_t * wksp ) {
  ulong const depth = 8UL;
  test_stem_t ts[1];
  fd_motor_tile_t * ctx = test_motor_manual_new( wksp, ts, depth, 2UL );
  fd_stem_context_t * stem = ts->stem;
  ulong const *       seqs = ts->seqs;
  ulong in_chunk = ctx->in[ 0 ].chunk0;
  void * fragment = fd_chunk_to_laddr( wksp, in_chunk );

  /* An extended BAM microblock must publish its entry and durable
     result. */
  fd_bam_bundle_result_t result = test_result( 12346U, 7U, ctx->slot, 1U );
  ulong sz = test_microblock( fragment, ctx->slot, result.seq_id, 1UL, &result );
  FD_TEST( !returnable_frag( ctx, 0UL, 0UL, fd_disco_execle_sig( ctx->slot, 0UL ),
                             in_chunk, sz, 0UL, 0UL, 0UL, stem ) );
  FD_TEST( seqs[ 0 ]==1UL && seqs[ 2 ]==1UL );
  fd_leader_txn_timing_table_t const * timing = fd_leader_txn_timing_table_const( ctx->timing_tables, 0UL, 1UL );
  FD_TEST( timing->cnt==1UL && timing->rec[0].received_ns==123456789L );
  FD_TEST( fd_memeq( test_published_result( ts, wksp, depth, 0UL ), &result, sizeof(result) ) );
  fd_entry_batch_meta_t const * entry_batch = fd_chunk_to_laddr_const( wksp, ts->mcaches[ 0 ][ fd_mcache_line_idx( 0UL, depth ) ].chunk );
  fd_entry_batch_header_t const * entry = (fd_entry_batch_header_t const *)(entry_batch+1);
  FD_TEST( entry->txn_cnt==1UL );
  FD_TEST( fd_memeq( (uchar const *)(entry+1), ((fd_txn_p_t *)fragment)->payload, FD_TXN_SIGNATURE_SZ ) );

  /* A failed BAM transaction publishes its failure result after motor
     releases its pack ownership, without creating an empty entry. */
  fd_bam_bundle_result_t failed = test_failed_result( result.seq_id+1U, 7U, ctx->slot );
  sz = test_microblock( fragment, ctx->slot, failed.seq_id, 1UL, &failed );
  ((fd_txn_p_t *)fragment)->flags = FD_TXN_P_FLAGS_SANITIZE_SUCCESS;
  FD_TEST( !returnable_frag( ctx, 0UL, 1UL, fd_disco_execle_sig( ctx->slot, 1UL ),
                             in_chunk, sz, 0UL, 0UL, 0UL, stem ) );
  FD_TEST( seqs[ 0 ]==1UL && seqs[ 2 ]==2UL && ctx->expect_pack_idx==2U );
  FD_TEST( fd_memeq( test_published_result( ts, wksp, depth, 1UL ), &failed, sizeof(failed) ) );
  /* It went to a new chunk: the BAM tile may not have read the first. */
  FD_TEST( fd_memeq( test_published_result( ts, wksp, depth, 0UL ), &result, sizeof(result) ) );

  /* Ordinary TPU microblocks use the same parser without BAM feedback. */
  sz = test_microblock( fragment, ctx->slot, result.seq_id+2U, 1UL, NULL );
  ((fd_txn_p_t *)fragment)->source_tpu = FD_TXN_M_TPU_SOURCE_QUIC;
  FD_TEST( !returnable_frag( ctx, 0UL, 2UL, fd_disco_execle_sig( ctx->slot, 2UL ),
                             in_chunk, sz, 0UL, 0UL, 0UL, stem ) );
  FD_TEST( seqs[ 0 ]==2UL && seqs[ 2 ]==2UL );
}

/* Motor drops a microblock of a slot before its current one without
   recording it.  The result it carries, committed or failed, becomes a
   POH_TIMEOUT scheduling error; a microblock without a result, a pack
   frag or a tile without a BAM output publishes nothing. */
static void
test_dropped_microblock_results( fd_wksp_t * wksp,
                                 int         bam_wired ) {
  ulong const depth = 8UL;
  test_stem_t ts[1];
  fd_motor_tile_t * ctx = test_motor_manual_new( wksp, ts, depth, bam_wired ? 2UL : ULONG_MAX );
  fd_stem_context_t * stem = ts->stem;
  ulong const *       seqs = ts->seqs;
  ulong in_chunk = ctx->in[ 0 ].chunk0;
  void * fragment = fd_chunk_to_laddr( wksp, in_chunk );
  ulong stale = ctx->slot-1UL;

  fd_bam_bundle_result_t committed = test_result( 500U, 9U, stale, 1U );
  committed.consumed_cus[ 0 ] = 1234U;
  ulong sz = test_microblock( fragment, stale, committed.seq_id, 1UL, &committed );
  FD_TEST( !returnable_frag( ctx, 0UL, 0UL, fd_disco_execle_sig( stale, 0UL ), in_chunk, sz, 0UL, 0UL, 0UL, stem ) );
  FD_TEST( ctx->expect_pack_idx==1U && !seqs[ 0 ] && !seqs[ 1 ] && seqs[ 2 ]==(ulong)bam_wired );
  fd_bam_bundle_result_t expected = fd_bam_result_base( committed.seq_id, 9U, stale, 1U );
  expected.scheduling_error = FD_BAM_SCHED_ERR_POH_TIMEOUT;
  if( bam_wired ) {
    fd_bam_bundle_result_t const * dropped = test_published_result( ts, wksp, depth, 0UL );
    FD_TEST( fd_memeq( dropped, &expected, sizeof(expected) ) );
    FD_TEST( !dropped->execution_success && !dropped->consumed_cus[ 0 ] );
  }

  fd_bam_bundle_result_t failed = test_failed_result( 501U, 9U, stale );
  sz = test_microblock( fragment, stale, failed.seq_id, 1UL, &failed );
  ((fd_txn_p_t *)fragment)->flags = FD_TXN_P_FLAGS_SANITIZE_SUCCESS;
  FD_TEST( !returnable_frag( ctx, 0UL, 1UL, fd_disco_execle_sig( stale, 1UL ), in_chunk, sz, 0UL, 0UL, 0UL, stem ) );
  FD_TEST( ctx->expect_pack_idx==2U && !seqs[ 0 ] && seqs[ 2 ]==2UL*(ulong)bam_wired );
  expected.seq_id = failed.seq_id;
  if( bam_wired ) FD_TEST( fd_memeq( test_published_result( ts, wksp, depth, 1UL ), &expected, sizeof(expected) ) );

  sz = test_microblock( fragment, stale, 502U, 1UL, NULL );
  FD_TEST( !returnable_frag( ctx, 0UL, 2UL, fd_disco_execle_sig( stale, 2UL ), in_chunk, sz, 0UL, 0UL, 0UL, stem ) );

  /* A pack frag of the result-carrying size is never read as one. */
  sz = test_microblock( fragment, stale, 503U, 1UL, &committed );
  FD_TEST( !returnable_frag( ctx, 1UL, 3UL, fd_disco_execle_sig( stale, 3UL ), in_chunk, sz, 0UL, 0UL, 0UL, stem ) );
  FD_TEST( ctx->expect_pack_idx==4U && !seqs[ 0 ] && !seqs[ 1 ] && seqs[ 2 ]==2UL*(ulong)bam_wired );
}

/* Motor built from a topology with a motor_bam link forwards the
   result an execle microblock carries once its leader slot starts.
   The upstream test covers the rest of the slot handoff without it. */
static void
test_topology_bam_out( fd_wksp_t * wksp ) {
  fd_motor_tile_t * ctx = test_motor_new( wksp );
  FD_TEST( ctx->bam_out->idx==2UL );

  test_stem_t ts[1];
  fd_stem_context_t * stem = test_stem_new( ts, wksp, 16UL );

  ulong replay_chunk = ctx->in[2].chunk0;
  fd_became_leader_t * leader = fd_chunk_to_laddr( wksp, replay_chunk );
  fd_memset( leader, 0, sizeof(*leader) );
  leader->slot = 1UL;
  FD_TEST( !returnable_frag( ctx, 2UL, 0UL, REPLAY_SIG_BECAME_LEADER, replay_chunk, sizeof(*leader), 0UL, 0UL, 0UL, stem ) );
  FD_TEST( ctx->slot==1UL );

  ulong execle_chunk = ctx->in[0].chunk0;
  ulong execle_sig   = fd_disco_execle_sig( 1UL, 0UL );
  fd_bam_bundle_result_t result = test_result( 91U, 1U, 1UL, 1U );
  ulong sz = test_microblock( fd_chunk_to_laddr( wksp, execle_chunk ), 1UL, result.seq_id, 1UL, &result );
  FD_TEST( !before_frag( ctx, 0UL, 0UL, execle_sig ) );
  FD_TEST( !returnable_frag( ctx, 0UL, 0UL, execle_sig, execle_chunk, sz, 0UL, 0UL, 0UL, stem ) );
  FD_TEST( ctx->expect_pack_idx==1U && ts->seqs[2]==1UL );
  FD_TEST( fd_memeq( fd_chunk_to_laddr_const( wksp, ts->mcaches[2][0].chunk ), &result, sizeof(result) ) );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  fd_wksp_t * wksp = fd_wksp_new_anonymous( FD_SHMEM_NORMAL_PAGE_SZ, 4096UL,
                                             fd_shmem_cpu_idx( 0UL ), "motor-test", 0UL );
  FD_TEST( wksp );

  test_bam_and_tpu_microblocks( wksp );
  test_dropped_microblock_results( wksp, 1 );
  test_dropped_microblock_results( wksp, 0 );
  test_topology_bam_out( wksp );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
