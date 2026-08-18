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
#include <sys/wait.h>

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

static void
test_full_slot_timing( fd_wksp_t * wksp ) {
  fd_slot_params_t const * regimes[] = {
    &FD_SLOT_PARAMS_400MS,
    &FD_SLOT_PARAMS_350MS,
    &FD_SLOT_PARAMS_300MS,
    &FD_SLOT_PARAMS_250MS,
    &FD_SLOT_PARAMS_200MS,
  };

  for( ulong i=0UL; i<sizeof(regimes)/sizeof(regimes[0]); i++ ) {
    fd_slot_params_t const * params = regimes[ i ];
    FD_TEST( params->ns_per_slot-params->ns_per_slot_adjusted==FD_TARGET_SLOT_ADJUSTMENT_NS );

    ulong adjusted_tick_ns = params->ns_per_slot_adjusted/64UL;
    FD_TEST( pohh_effective_tick_duration_ns( adjusted_tick_ns, 64UL, 0 )==adjusted_tick_ns );
    FD_TEST( pohh_effective_tick_duration_ns( adjusted_tick_ns, 64UL, 1 )==params->ns_per_slot/64UL );
  }

  ulong const custom_ticks_per_slot   = 63UL;
  ulong const custom_nominal_tick_ns  = FD_SLOT_PARAMS_400MS.ns_per_slot/custom_ticks_per_slot;
  ulong const custom_adjusted_tick_ns = fd_ulong_sat_sub( custom_nominal_tick_ns,
                                                          FD_TARGET_SLOT_ADJUSTMENT_NS/custom_ticks_per_slot );
  FD_TEST( pohh_effective_tick_duration_ns( custom_adjusted_tick_ns, custom_ticks_per_slot, 0 )==custom_adjusted_tick_ns );
  FD_TEST( pohh_effective_tick_duration_ns( custom_adjusted_tick_ns, custom_ticks_per_slot, 1 )==custom_nominal_tick_ns );

  /* The zero-tick guard is process-fatal, so exercise it in a child. */
  pid_t pid = fork();
  FD_TEST( pid>=0 );
  if( FD_UNLIKELY( !pid ) ) {
    fd_log_level_logfile_set( 6 );
    (void)pohh_effective_tick_duration_ns( 1UL, 0UL, 1 );
    _exit( 0 );
  }
  int status = 0;
  FD_TEST( waitpid( pid, &status, 0 )==pid );
  FD_TEST( ( WIFEXITED( status ) && WEXITSTATUS( status )==1 ) ||
           ( WIFSIGNALED( status ) && WTERMSIG( status )==SIGABRT ) );

  /* Exercise all three fresh-value boundaries.  Each receives the same
     adjusted tick; BAM mode must restore it once, never cumulatively. */
  static fd_pohh_tile_t ctx[1];
  static fd_keyswitch_t keyswitch[1];
  static fd_bam_ctrl_t bam_ctrl[1];
  fd_memset( ctx, 0, sizeof(ctx) );
  fd_memset( keyswitch, 0, sizeof(keyswitch) );
  fd_memset( bam_ctrl, 0, sizeof(bam_ctrl) );
  bam_ctrl->applied_enable          = 1U;
  ctx->bam_ctrl                     = bam_ctrl;
  ctx->use_nominal_slot_duration = 1;
  ctx->highwater_leader_slot     = ULONG_MAX;
  ctx->store_leader_bank_slot    = ULONG_MAX;
  ctx->keyswitch                 = keyswitch;

  fd_pohh_global_ctx   = ctx;
  fd_poh_waiting_lock  = 0UL;
  fd_poh_returned_lock = 1UL;
  uchar initial_hash[32] = {0};
  ulong adjusted_tick_ns = FD_SLOT_PARAMS_400MS.ns_per_slot_adjusted/64UL;
  fd_ext_poh_initialize( adjusted_tick_ns, 62500UL, 64UL, 64UL, initial_hash, NULL );
  FD_TEST( ctx->tick_duration_ns==FD_SLOT_PARAMS_400MS.ns_per_slot/64UL );
  FD_TEST( (ulong)ctx->slot_duration_ns==FD_SLOT_PARAMS_400MS.ns_per_slot );

  ulong const depth = 4UL;
  fd_frag_meta_t * pack_mcache  = test_mcache_new( wksp, depth );
  fd_frag_meta_t * shred_mcache = test_mcache_new( wksp, depth );
  void * pack_dcache  = test_dcache_new( wksp, depth, sizeof(fd_became_leader_t) );
  void * shred_dcache = test_dcache_new( wksp, depth, sizeof(void const *) );

  *ctx->pack_out = (fd_pohh_out_t) {
    .idx    = 0UL,
    .mem    = wksp,
    .chunk0 = fd_dcache_compact_chunk0( wksp, pack_dcache ),
    .wmark  = fd_dcache_compact_wmark( wksp, pack_dcache, sizeof(fd_became_leader_t) ),
    .chunk  = fd_dcache_compact_chunk0( wksp, pack_dcache ),
  };
  *ctx->shred_out = (fd_pohh_out_t) {
    .idx    = 1UL,
    .mem    = wksp,
    .chunk0 = fd_dcache_compact_chunk0( wksp, shred_dcache ),
    .wmark  = fd_dcache_compact_wmark( wksp, shred_dcache, sizeof(void const *) ),
    .chunk  = fd_dcache_compact_chunk0( wksp, shred_dcache ),
  };

  fd_frag_meta_t * mcaches[2] = { pack_mcache, shred_mcache };
  ulong seqs[2]                = { 0UL, 0UL };
  ulong depths[2]              = { depth, depth };
  ulong cr_avail[2]            = { ULONG_MAX, ULONG_MAX };
  ulong min_cr_avail           = ULONG_MAX;
  int out_reliable[2]          = { 0, 0 };
  fd_stem_context_t stem[1] = {{
    .mcaches             = mcaches,
    .seqs                = seqs,
    .depths              = depths,
    .cr_avail            = cr_avail,
    .min_cr_avail        = &min_cr_avail,
    .cr_decrement_amount = 1UL,
    .out_reliable        = out_reliable,
  }};
  ctx->stem = stem;

  fd_histf_join( fd_histf_new( ctx->begin_leader_delay, FD_MHIST_SECONDS_MIN( POHH, BEGIN_LEADER_DELAY_SECONDS ),
                                                       FD_MHIST_SECONDS_MAX( POHH, BEGIN_LEADER_DELAY_SECONDS ) ) );

  ctx->slot                  = 2UL;
  ctx->reset_slot            = 2UL;
  ctx->last_slot             = 2UL;
  ctx->last_hashcnt          = 0UL;
  ctx->next_leader_slot      = 2UL;
  ctx->reset_slot_start_ns   = fd_log_wallclock();
  ctx->current_leader_bank   = NULL;

  ulong leader_chunk = ctx->pack_out->chunk;
  fd_poh_waiting_lock  = 0UL;
  fd_poh_returned_lock = 1UL;
  fd_ext_poh_begin_leader( (void const *)1UL, 2UL, 0UL, 62500UL, adjusted_tick_ns,
                           48000000UL, 36000000UL, 12000000UL, 100000000UL, 32768UL );
  FD_TEST( ctx->tick_duration_ns==FD_SLOT_PARAMS_400MS.ns_per_slot/64UL );
  FD_TEST( (ulong)ctx->slot_duration_ns==FD_SLOT_PARAMS_400MS.ns_per_slot );

  fd_became_leader_t const * leader = fd_chunk_to_laddr_const( wksp, leader_chunk );
  FD_TEST( leader->tick_duration_ns==FD_SLOT_PARAMS_400MS.ns_per_slot/64UL );
  FD_TEST( (ulong)(leader->slot_end_ns-leader->slot_start_ns)==FD_SLOT_PARAMS_400MS.ns_per_slot );

  uchar reset_hash[32] = {1};
  ulong features_activation[ (sizeof(fd_shred_features_activation_t)+sizeof(ulong)-1UL)/sizeof(ulong) ] = {0};
  ulong shred_slot_limits [ (sizeof(fd_shred_slot_limits_t)        +sizeof(ulong)-1UL)/sizeof(ulong) ] = {0};
  fd_poh_waiting_lock  = 0UL;
  fd_poh_returned_lock = 1UL;
  fd_ext_poh_reset( 2UL, reset_hash, 62500UL, adjusted_tick_ns, NULL, features_activation, shred_slot_limits );
  FD_TEST( ctx->tick_duration_ns==FD_SLOT_PARAMS_400MS.ns_per_slot/64UL );
  FD_TEST( (ulong)ctx->slot_duration_ns==FD_SLOT_PARAMS_400MS.ns_per_slot );

  bam_ctrl->applied_enable = 0U;
  fd_poh_waiting_lock  = 0UL;
  fd_poh_returned_lock = 1UL;
  fd_ext_poh_reset( 3UL, reset_hash, 62500UL, adjusted_tick_ns, NULL, features_activation, shred_slot_limits );
  FD_TEST( !ctx->use_nominal_slot_duration );
  FD_TEST( ctx->tick_duration_ns==adjusted_tick_ns );
  FD_TEST( (ulong)ctx->slot_duration_ns==FD_SLOT_PARAMS_400MS.ns_per_slot_adjusted );

  bam_ctrl->applied_enable = 1U;
  fd_poh_waiting_lock  = 0UL;
  fd_poh_returned_lock = 1UL;
  fd_ext_poh_reset( 4UL, reset_hash, 62500UL, adjusted_tick_ns, NULL, features_activation, shred_slot_limits );
  FD_TEST( ctx->use_nominal_slot_duration );
  FD_TEST( ctx->tick_duration_ns==FD_SLOT_PARAMS_400MS.ns_per_slot/64UL );
  FD_TEST( (ulong)ctx->slot_duration_ns==FD_SLOT_PARAMS_400MS.ns_per_slot );

  FD_LOG_NOTICE(( "pass: test_full_slot_timing" ));
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

  test_full_slot_timing( wksp );
  test_reset_rejects_stale_bam_microblock( wksp );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
