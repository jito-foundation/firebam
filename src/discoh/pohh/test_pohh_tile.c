/* Regression coverage for BAM feedback at the Frankendancer PoH
   boundary.  Include the tile implementation so the test exercises the
   same before_frag/during_frag/after_frag callbacks as the live stem. */

#define _GNU_SOURCE
#include "../../flamenco/leaders/fd_multi_epoch_leaders.h"

static ulong
test_next_leader_slot( fd_multi_epoch_leaders_t const * mleaders,
                       ulong                            slot,
                       fd_pubkey_t const *              identity_key );

#define fd_multi_epoch_leaders_get_next_slot test_next_leader_slot
#include "fd_pohh_tile.c"
#undef fd_multi_epoch_leaders_get_next_slot

#include "../../util/tmpl/fd_unit_test.c"

int volatile const fd_startup_skip_checks = 1; /* fd_startup.c */

void
fd_ext_bank_acquire( void const * bank FD_PARAM_UNUSED ) {
}

void
fd_ext_bank_release( void const * bank FD_PARAM_UNUSED ) {
}

void
fd_ext_poh_signal_leader_change( void * sender FD_PARAM_UNUSED ) {
}

void
fd_ext_poh_register_tick( void const * bank FD_PARAM_UNUSED,
                          uchar const * hash FD_PARAM_UNUSED ) {
}

int
fd_ext_bank_load_account( void const *  bank       FD_PARAM_UNUSED,
                          int           fixed_root FD_PARAM_UNUSED,
                          uchar const * address    FD_PARAM_UNUSED,
                          uchar *       owner      FD_PARAM_UNUSED,
                          uchar *       data       FD_PARAM_UNUSED,
                          ulong *       data_sz    FD_PARAM_UNUSED ) {
  return 0;
}

int
fd_ext_admin_rpc_set_identity( uchar const * identity_keypair FD_PARAM_UNUSED,
                               int           require_tower    FD_PARAM_UNUSED,
                               int           tower_check      FD_PARAM_UNUSED ) {
  return 0;
}

static ulong
test_next_leader_slot( fd_multi_epoch_leaders_t const * mleaders     FD_PARAM_UNUSED,
                       ulong                            slot         FD_PARAM_UNUSED,
                       fd_pubkey_t const *              identity_key FD_PARAM_UNUSED ) {
  return ULONG_MAX;
}

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

/* Accepting a prior-slot microblock after PoH resets can acknowledge work that
   PoH discarded.  Reject the stale microblock before reporting success. */
static void
test_reset_rejects_stale_bam_microblock( fd_wksp_t * wksp ) {
  ulong const depth      = 4UL;
  ulong const stale_slot = 136UL;
  uint  const pack_idx   = 94U;

  /* fd_pohh_tile_t contains the skipped-tick hash cache and is much
     larger than a normal thread stack. */
  static fd_pohh_tile_t ctx[1];
  static fd_keyswitch_t keyswitch[1];
  fd_memset( ctx, 0, sizeof(ctx) );
  fd_memset( keyswitch, 0, sizeof(keyswitch) );

  /* A BAM batch can finish execution just as replay abandons its leader
     slot.  The reset must make that in-flight microblock stale, prevent
     ledger publication, and publish one retryable POH_TIMEOUT result on
     poh_bam instead of reporting the provisional success. */
  ctx->slot                   = stale_slot;
  ctx->next_leader_slot       = stale_slot;
  ctx->current_leader_bank    = (void const *)1UL;
  ctx->store_leader_bank_slot = ULONG_MAX;
  ctx->highwater_leader_slot  = ULONG_MAX;
  ctx->tick_duration_ns       = 6250UL;
  ctx->hashcnt_per_tick       = 62500UL;
  ctx->ticks_per_slot         = 64UL;
  ctx->slot_duration_ns       = (double)ctx->tick_duration_ns*(double)ctx->ticks_per_slot;
  ctx->keyswitch              = keyswitch;

  fd_pohh_global_ctx   = ctx;
  fd_poh_waiting_lock  = 0UL;
  fd_poh_returned_lock = 1UL;
  uchar reset_hash[ 32 ] = {0};
  ulong features_activation[ (sizeof(fd_shred_features_activation_t)+sizeof(ulong)-1UL)/sizeof(ulong) ] = {0};
  ulong shred_slot_limits [ (sizeof(fd_shred_slot_limits_t)        +sizeof(ulong)-1UL)/sizeof(ulong) ] = {0};
  fd_ext_poh_reset( stale_slot-5UL,
                    reset_hash,
                    ctx->hashcnt_per_tick,
                    ctx->tick_duration_ns,
                    NULL,
                    features_activation,
                    shred_slot_limits );
  FD_TEST( ctx->highwater_leader_slot==stale_slot+1UL );
  FD_TEST( !ctx->current_leader_bank );

  ctx->expect_pack_idx = pack_idx;
  ctx->in_kind[ 0 ]    = IN_KIND_BANK;

  void * in_dcache = test_dcache_new( wksp, depth, MAX_MICROBLOCK_SZ );
  ctx->in[ 0 ].mem    = wksp;
  ctx->in[ 0 ].chunk0 = fd_dcache_compact_chunk0( wksp, in_dcache );
  ctx->in[ 0 ].wmark  = fd_dcache_compact_wmark( wksp, in_dcache, MAX_MICROBLOCK_SZ );

  ulong in_chunk = ctx->in[ 0 ].chunk0;
  uchar * fragment = fd_chunk_to_laddr( wksp, in_chunk );
  fd_txn_p_t * txn = (fd_txn_p_t *)fragment;
  fd_memset( txn, 0, sizeof(fd_txn_p_t) );
  txn->source_tpu = FD_TXN_M_TPU_SOURCE_BAM;
  txn->flags      = FD_TXN_P_FLAGS_SANITIZE_SUCCESS | FD_TXN_P_FLAGS_EXECUTE_SUCCESS;
  txn->bam.seq_id        = 12345U;
  txn->bam.scheduler_gen = 7U;
  txn->bam.batch_idx     = 0U;

  fd_bam_bundle_result_t provisional = fd_bam_result_base( txn->bam.seq_id,
                                                           txn->bam.scheduler_gen,
                                                           stale_slot,
                                                           1U );
  provisional.execution_success = 1U;
  fd_bam_result_mark_sanitize_success_all( &provisional );
  fd_microblock_trailer_t * trailer = fd_bam_microblock_prepare_trailer( fragment, 1UL, &provisional );
  fd_memset( trailer, 0, sizeof(fd_microblock_trailer_t) );
  ulong const fragment_sz = fd_bam_microblock_footprint( 1UL, 1 );

  fd_frag_meta_t * shred_mcache = test_mcache_new( wksp, depth );
  void * shred_dcache = test_dcache_new( wksp, depth, MAX_MICROBLOCK_SZ );
  *ctx->shred_out = (fd_pohh_out_t) {
    .idx    = 0UL,
    .mem    = wksp,
    .chunk0 = fd_dcache_compact_chunk0( wksp, shred_dcache ),
    .wmark  = fd_dcache_compact_wmark( wksp, shred_dcache, MAX_MICROBLOCK_SZ ),
    .chunk  = fd_dcache_compact_chunk0( wksp, shred_dcache ),
  };

  fd_frag_meta_t * poh_bam_mcache = test_mcache_new( wksp, depth );
  void * poh_bam_dcache = test_dcache_new( wksp, depth, sizeof(fd_bam_bundle_result_t) );
  *ctx->bam_out = (fd_pohh_out_t) {
    .idx    = 1UL,
    .mem    = wksp,
    .chunk0 = fd_dcache_compact_chunk0( wksp, poh_bam_dcache ),
    .wmark  = fd_dcache_compact_wmark( wksp, poh_bam_dcache, sizeof(fd_bam_bundle_result_t) ),
    .chunk  = fd_dcache_compact_chunk0( wksp, poh_bam_dcache ),
  };

  fd_frag_meta_t * mcaches[ 2 ] = { shred_mcache, poh_bam_mcache };
  ulong seqs[ 2 ]                = { 0UL, 0UL };
  ulong depths[ 2 ]              = { depth, depth };
  ulong cr_avail[ 2 ]            = { ULONG_MAX, ULONG_MAX };
  ulong min_cr_avail             = ULONG_MAX;
  int out_reliable[ 2 ]          = { 0, 0 };
  fd_stem_context_t stem[1] = {{
    .mcaches             = mcaches,
    .seqs                = seqs,
    .depths              = depths,
    .cr_avail            = cr_avail,
    .min_cr_avail        = &min_cr_avail,
    .cr_decrement_amount = 1UL,
    .out_reliable        = out_reliable,
  }};

  ulong sig = fd_disco_execle_sig( stale_slot, pack_idx );
  FD_TEST( before_frag( ctx, 0UL, 0UL, sig )==0 );
  FD_TEST( ctx->expect_pack_idx==pack_idx+1U );

  during_frag( ctx, 0UL, 0UL, sig, in_chunk, fragment_sz, 0UL );
  FD_TEST( ctx->skip_frag );
  FD_TEST( ctx->_bam_result_valid );

  after_frag( ctx, 0UL, 0UL, sig, fragment_sz, 0UL, 0UL, stem );

  FD_TEST( seqs[ 0 ]==0UL ); /* no shred/ledger publication */
  FD_TEST( seqs[ 1 ]==1UL );
  fd_frag_meta_t const * meta = poh_bam_mcache + fd_mcache_line_idx( 0UL, depth );
  FD_TEST( fd_frag_meta_seq_query( meta )==0UL );
  FD_TEST( meta->sz==sizeof(fd_bam_bundle_result_t) );

  fd_bam_bundle_result_t const * result = fd_chunk_to_laddr_const( wksp, meta->chunk );
  FD_TEST( result->seq_id==provisional.seq_id );
  FD_TEST( result->scheduler_gen==provisional.scheduler_gen );
  FD_TEST( result->slot==provisional.slot );
  FD_TEST( !result->execution_success );
  FD_TEST( result->scheduling_error==FD_BAM_SCHED_ERR_POH_TIMEOUT );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  fd_wksp_t * wksp = fd_wksp_new_anonymous( FD_SHMEM_NORMAL_PAGE_SZ, 2048UL,
                                             fd_shmem_cpu_idx( 0UL ), "pohh-test", 0UL );
  FD_TEST( wksp );

  test_reset_rejects_stale_bam_microblock( wksp );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
