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
#define fd_pack_contains_transaction test_fd_pack_contains_transaction
#define fd_pack_contains_bam_bundle test_fd_pack_contains_bam_bundle
#define fd_pack_delete_bam_bundle test_fd_pack_delete_bam_bundle
#include "fd_pack_tile.c"
#undef fd_pack_delete_bam_bundle
#undef fd_pack_contains_bam_bundle
#undef fd_pack_contains_transaction
#undef fd_pack_insert_bundle_cancel
#undef fd_pack_insert_bundle_fini
#undef fd_pack_insert_txn_cancel
#undef fd_pack_insert_txn_fini
#undef fd_pack_insert_txn_init
#undef fd_pack_delete_transaction

int fd_pack_insert_bundle_fini( fd_pack_t *, fd_txn_e_t * const *, ulong, ulong,
                                int, void const *, ulong *, ulong * );
ulong fd_pack_delete_transaction( fd_pack_t *, fd_ed25519_sig_t const * );
int fd_pack_contains_transaction( fd_pack_t const *, fd_ed25519_sig_t const * );
int fd_pack_contains_bam_bundle( fd_pack_t const *, fd_ed25519_sig_t const *, uint, ushort );
ulong fd_pack_delete_bam_bundle( fd_pack_t *, fd_ed25519_sig_t const *, uint, ushort );

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
static int               test_delete_use_real_pack;
static int               test_delete_before_insert_fini;
static ulong             test_insert_txn_init_call_cnt;
static ulong             test_insert_txn_fini_call_cnt;
static ulong             test_insert_txn_cancel_call_cnt;
static fd_txn_e_t        test_insert_txn_slot[1];
static fd_pack_ctx_t *    test_query_ctx;

/* Models fd_pack's signature map for the fake pack.  Everything reachable
   through the fake pack got there by being tracked, so the base model is
   "some pending bam_work item has this sig0" -- the invariant the tile
   relies on.  The one state that cannot express is pack having evicted a
   bundle the tile still tracks, so evictions are staged explicitly and
   override the base.

     (pending bam_work sig0s) & ~staged evictions

   Occupancy the tile never tracked -- a mempool copy of a bundle's leading
   transaction, or another bundle carrying it in a non-leading position --
   is covered by the tests that attach a real fd_pack, where
   fd_pack_contains_transaction runs for real. */
#define TEST_PACK_STAGED_SIG_CAP 8UL
static fd_ed25519_sig_t  test_pack_evicted_sigs[ TEST_PACK_STAGED_SIG_CAP ];
static ulong             test_pack_evicted_sig_cnt;

/* An insert puts the signature back into pack, so it stops reading as
   evicted. */
static void
test_pack_clear_evicted_sig( void const * sig ) {
  for( ulong i=0UL; i<test_pack_evicted_sig_cnt; i++ ) {
    if( FD_LIKELY( memcmp( test_pack_evicted_sigs[ i ], sig, sizeof(fd_ed25519_sig_t) ) ) ) continue;
    fd_memcpy( test_pack_evicted_sigs[ i ],
               test_pack_evicted_sigs[ --test_pack_evicted_sig_cnt ],
               sizeof(fd_ed25519_sig_t) );
    return;
  }
}

#define TEST_PACK_TILE_DCACHE_CHUNKS 16UL
#define TEST_PACK_TILE_MCACHE_DEPTH  16UL
#define TEST_PACK_TILE_BAM_WORK_CAP   8UL
#define TEST_D18_INSTR_ACCT_CNT       (FD_PACK_MAX_ACCOUNTS_PER_INSTRUCTION+1UL)

int
test_fd_pack_contains_transaction( fd_pack_t const *        pack,
                                   fd_ed25519_sig_t const * sig0 ) {
  if( FD_UNLIKELY( test_insert_fini_use_real_pack ) )
    return fd_pack_contains_transaction( pack, sig0 );
  for( ulong i=0UL; i<test_pack_evicted_sig_cnt; i++ )
    if( FD_UNLIKELY( !memcmp( test_pack_evicted_sigs[ i ], sig0, sizeof(fd_ed25519_sig_t) ) ) ) return 0;
  if( FD_UNLIKELY( !test_query_ctx ) ) return 0;
  return pack_tile_bam_work_find_by_sig0_state( test_query_ctx,
                                                sig0,
                                                PACK_BAM_WORK_STATE_PENDING )<test_query_ctx->bam_work_cnt;
}

int
test_fd_pack_contains_bam_bundle( fd_pack_t const *        pack,
                                  fd_ed25519_sig_t const * sig0,
                                  uint                     seq_id,
                                  ushort                   scheduler_gen ) {
  if( FD_UNLIKELY( test_insert_fini_use_real_pack ) )
    return fd_pack_contains_bam_bundle( pack, sig0, seq_id, scheduler_gen );
  for( ulong i=0UL; i<test_pack_evicted_sig_cnt; i++ )
    if( FD_UNLIKELY( !memcmp( test_pack_evicted_sigs[ i ], sig0, sizeof(fd_ed25519_sig_t) ) ) ) return 0;
  if( FD_UNLIKELY( !test_query_ctx ) ) return 0;
  for( ulong i=test_query_ctx->bam_scheduled_work_cnt; i<test_query_ctx->bam_work_cnt; i++ ) {
    pack_bam_work_t const * work = &test_query_ctx->bam_work[ i ];
    if( FD_LIKELY( memcmp( work->sig[ 0 ], sig0, sizeof(fd_ed25519_sig_t) ) ) ) continue;
    if( FD_LIKELY( work->seq_id==seq_id && work->scheduler_gen==scheduler_gen ) ) return 1;
  }
  return 0;
}

ulong
test_fd_pack_delete_transaction( fd_pack_t *                 pack,
                                 fd_ed25519_sig_t const *    sig0 ) {
  test_delete_call_cnt++;
  if( FD_UNLIKELY( !sig0 ) ) return 1UL;
  fd_memcpy( test_delete_last_sig, sig0, sizeof(fd_ed25519_sig_t) );
  if( FD_UNLIKELY( test_delete_use_real_pack ) )
    return fd_pack_delete_transaction( pack, sig0 );
  return 1UL;
}

ulong
test_fd_pack_delete_bam_bundle( fd_pack_t *              pack,
                                fd_ed25519_sig_t const * sig0,
                                uint                     seq_id,
                                ushort                   scheduler_gen ) {
  test_delete_call_cnt++;
  if( FD_UNLIKELY( !sig0 ) ) return 1UL;
  fd_memcpy( test_delete_last_sig, sig0, sizeof(fd_ed25519_sig_t) );
  if( FD_UNLIKELY( test_delete_use_real_pack ) )
    return fd_pack_delete_bam_bundle( pack, sig0, seq_id, scheduler_gen );
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
                                 int                  initializer_bundle_kind,
                                 void const *         bundle_meta,
                                 ulong *              delete_cnt,
                                 ulong *              reject_txn_idx ) {
  (void)pack;
  (void)bundle;
  (void)txn_cnt;
  (void)expires_at;
  (void)bundle_meta;
  test_insert_fini_call_cnt++;
  test_delete_before_insert_fini = !!test_delete_call_cnt;
  /* A successful insert puts the bundle back into pack, so it must stop
     reading as evicted -- otherwise the model would keep reporting a
     transaction that is demonstrably there as missing. */
  if( FD_LIKELY( bundle && txn_cnt ) ) test_pack_clear_evicted_sig( bundle[ 0 ]->txnp->payload+1UL );
  if( FD_UNLIKELY( test_insert_fini_use_real_pack ) ) {
    return fd_pack_insert_bundle_fini( pack,
                                       bundle,
                                       txn_cnt,
                                       expires_at,
                                       initializer_bundle_kind,
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
  ulong          bundle_evicted_cnt;
  pack_bam_work_t bam_work[ TEST_PACK_TILE_BAM_WORK_CAP ];
  fd_bam_bundle_result_t bam_result_queue[ 2UL*TEST_PACK_TILE_BAM_WORK_CAP ];
} test_fake_pack_t;

FD_STATIC_ASSERT( offsetof( test_fake_pack_t, pending_txn_cnt )==FD_PACK_PENDING_TXN_CNT_OFF,
                  test_fake_pack_pending_txn_cnt_off );
FD_STATIC_ASSERT( offsetof( test_fake_pack_t, bundle_evicted_cnt )==FD_PACK_BUNDLE_EVICTED_CNT_OFF,
                  test_fake_pack_bundle_evicted_cnt_off );

static void
test_pack_stage_missing_sig( void const * sig ) {
  FD_TEST( test_pack_evicted_sig_cnt<TEST_PACK_STAGED_SIG_CAP );
  fd_memcpy( test_pack_evicted_sigs[ test_pack_evicted_sig_cnt++ ], sig, sizeof(fd_ed25519_sig_t) );
}

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
  test_delete_use_real_pack       = 0;
  test_delete_before_insert_fini  = 0;
  test_insert_txn_init_call_cnt   = 0UL;
  test_insert_txn_fini_call_cnt   = 0UL;
  test_insert_txn_cancel_call_cnt = 0UL;
  test_pack_evicted_sig_cnt       = 0UL;
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
  test_query_ctx                   = h->ctx;
}

static void
test_pack_tile_harness_delete( test_pack_tile_harness_t * h ) {
  if( FD_LIKELY( test_query_ctx==h->ctx ) ) test_query_ctx = NULL;
  test_pack_tile_out_delete( h->out );
  fd_memset( h, 0, sizeof(*h) );
}

/* Searches all of bam_work regardless of state.  The tile itself always
   knows which half of the partition it wants and so uses the state-scoped
   lookup; tests use this to assert across both halves at once. */

static inline ulong
pack_tile_bam_work_find_by_sig0( fd_pack_ctx_t const * ctx,
                                 void const *          sig0 ) {
  for( ulong i=0UL; i<ctx->bam_work_cnt; i++ ) {
    if( FD_LIKELY( memcmp( ctx->bam_work[ i ].sig[ 0 ], sig0, sizeof(fd_ed25519_sig_t) ) ) ) continue;
    return i;
  }
  return ctx->bam_work_cnt;
}

/* Appends BAM work, refusing to create a second entry for a sig0 already
   tracked.  The tile's own append does not check this -- see the comment
   on pack_tile_append_bam_work -- so this exists purely so tests can stage
   work items without accidentally building an impossible bam_work state. */

static inline int
pack_tile_track_bam_work( fd_pack_ctx_t * ctx,
                          void const *    sigs,
                          long            first_rx_ts_ns,
                          uint            seq_id,
                          ushort          scheduler_gen,
                          ulong           slot,
                          ulong           max_schedule_slot,
                          ulong           blockhash_slot,
                          uchar           min_blockhash_slot_txn_idx,
                          uchar           txn_cnt ) {
  if( FD_UNLIKELY( pack_tile_bam_work_find_by_sig0( ctx, sigs )<ctx->bam_work_cnt ) ) return 0;
  return pack_tile_append_bam_work( ctx, sigs, first_rx_ts_ns, seq_id, scheduler_gen,
                                    slot, max_schedule_slot, blockhash_slot,
                                    min_blockhash_slot_txn_idx, txn_cnt );
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

/* Retaining selected initializer metadata across a mode switch can apply the
   old source's fee configuration to new work.  Retire it at both mode edges. */
static void
test_pack_tile_bam_mode_edges_retire_pending_initializer( void ) {
  test_pack_tile_harness_t h[ 1 ];
  ulong                    bam_status = 0UL;
  fd_ed25519_sig_t         normal_initializer_sig[ 1 ];
  fd_ed25519_sig_t         bam_initializer_sig[ 1 ];

  test_pack_tile_harness_new( h );
  test_pack_tile_fill_sig( *normal_initializer_sig, 41U );
  test_pack_tile_fill_sig( *bam_initializer_sig,    81U );

  h->ctx->bam_status_fseq          = &bam_status;
  h->ctx->bam_override_snapshot    = 0;
  h->ctx->crank->enabled           = 1;
  h->ctx->crank->ib_inserted       = 1;
  h->ctx->crank->prev_config_before_ib->discriminator = 1UL;
  h->ctx->crank->prev_config->discriminator           = 2UL;
  fd_memcpy( h->ctx->crank->last_sig, normal_initializer_sig, sizeof(fd_ed25519_sig_t) );

  /* Signature-wide initializer deletion can also remove a pending BAM copy
     without advancing fd_pack's autonomous-eviction counter. */
  FD_TEST( pack_tile_track_bam_work( h->ctx, normal_initializer_sig, 0L,
                                     71U, 3U, 100UL, 100UL, 100UL, 0U, 1U ) );
  test_pack_stage_missing_sig( normal_initializer_sig );

  /* Re-observing one mode leaves its pending initializer alone. */
  FD_TEST( pack_tile_snapshot_bam_override( h->ctx )==0 );
  FD_TEST( test_delete_call_cnt==0UL );
  FD_TEST( h->ctx->bam_pending_work_cnt==1UL );

  /* Activation retires the pending normal/Block Engine initializer and
     reconciles the BAM copy deleted as collateral. */
  bam_status = FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE;
  FD_TEST( pack_tile_snapshot_bam_override( h->ctx )==1 );
  FD_TEST( test_delete_call_cnt==1UL );
  FD_TEST( !memcmp( test_delete_last_sig, normal_initializer_sig, sizeof(fd_ed25519_sig_t) ) );
  FD_TEST( !h->ctx->crank->ib_inserted );
  FD_TEST( h->ctx->crank->prev_config->discriminator==1UL );
  FD_TEST( h->ctx->bam_work_cnt==0UL );
  FD_TEST( h->ctx->bam_pending_work_cnt==0UL );
  FD_TEST( h->ctx->bam_pending_result_cnt==1UL );
  fd_bam_bundle_result_t const * res = &h->ctx->bam_result_queue[ h->ctx->bam_result_queue_head ];
  FD_TEST( res->seq_id==71U );
  FD_TEST( res->scheduler_gen==3U );
  FD_TEST( res->scheduling_error==FD_BAM_SCHED_ERR_CONTAINER_FULL );

  /* Deactivation applies the same rule to a pending BAM initializer. */
  h->ctx->crank->ib_inserted = 1;
  h->ctx->crank->prev_config_before_ib->discriminator = 3UL;
  h->ctx->crank->prev_config->discriminator           = 4UL;
  fd_memcpy( h->ctx->crank->last_sig, bam_initializer_sig, sizeof(fd_ed25519_sig_t) );
  bam_status = 0UL;
  FD_TEST( pack_tile_snapshot_bam_override( h->ctx )==0 );
  FD_TEST( test_delete_call_cnt==2UL );
  FD_TEST( !memcmp( test_delete_last_sig, bam_initializer_sig, sizeof(fd_ed25519_sig_t) ) );
  FD_TEST( !h->ctx->crank->ib_inserted );
  FD_TEST( h->ctx->crank->prev_config->discriminator==3UL );

  test_pack_tile_harness_delete( h );
}

static pack_bam_work_t *
test_pack_tile_mark_bam_work_scheduled( test_pack_tile_harness_t * h,
                                        void const *                sig0 ) {
  ulong work_idx = pack_tile_bam_work_find_by_sig0( h->ctx, sig0 );
  FD_TEST( work_idx<h->ctx->bam_work_cnt );

  pack_bam_work_t * item = &h->ctx->bam_work[ work_idx ];
  FD_TEST( item->state==PACK_BAM_WORK_STATE_PENDING );

  h->ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_SCHEDULED_IDX ]++;
  return pack_tile_bam_work_mark_scheduled( h->ctx, work_idx );
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
test_pack_tile_bam_work_partition_mutations( void ) {
  test_pack_tile_harness_t h[1];
  uchar sigs[ 3 ][ sizeof(fd_ed25519_sig_t) ];
  test_pack_tile_harness_new( h );

  for( uchar i=0U; i<3U; i++ ) {
    test_pack_tile_fill_sig( sigs[ i ], (uchar)( 200U+i ) );
    FD_TEST( pack_tile_track_bam_work( h->ctx, sigs[ i ], 0L, (uint)i, 0U,
                                      100UL, 100UL, 100UL, 0U, 1U ) );
  }

  (void)test_pack_tile_mark_bam_work_scheduled( h, sigs[ 1 ] );
  (void)test_pack_tile_mark_bam_work_scheduled( h, sigs[ 2 ] );
  FD_TEST( h->ctx->bam_scheduled_work_cnt==2UL );
  FD_TEST( h->ctx->bam_pending_work_cnt==1UL );
  for( ulong i=0UL; i<h->ctx->bam_scheduled_work_cnt; i++ )
    FD_TEST( h->ctx->bam_work[ i ].state==PACK_BAM_WORK_STATE_SCHEDULED );
  for( ulong i=h->ctx->bam_scheduled_work_cnt; i<h->ctx->bam_work_cnt; i++ )
    FD_TEST( h->ctx->bam_work[ i ].state==PACK_BAM_WORK_STATE_PENDING );

  ulong work_idx = pack_tile_bam_work_find_by_sig0_state( h->ctx, sigs[ 1 ], PACK_BAM_WORK_STATE_SCHEDULED );
  FD_TEST( work_idx<h->ctx->bam_work_cnt );
  (void)pack_tile_bam_work_swap_remove( h->ctx, work_idx );
  FD_TEST( h->ctx->bam_scheduled_work_cnt==1UL );
  FD_TEST( h->ctx->bam_pending_work_cnt==1UL );
  FD_TEST( h->ctx->bam_work[ 0 ].state==PACK_BAM_WORK_STATE_SCHEDULED );
  FD_TEST( h->ctx->bam_work[ 1 ].state==PACK_BAM_WORK_STATE_PENDING );

  work_idx = pack_tile_bam_work_find_by_sig0_state( h->ctx, sigs[ 0 ], PACK_BAM_WORK_STATE_PENDING );
  FD_TEST( work_idx<h->ctx->bam_work_cnt );
  (void)pack_tile_bam_work_swap_remove( h->ctx, work_idx );
  FD_TEST( h->ctx->bam_work_cnt==1UL );
  FD_TEST( h->ctx->bam_scheduled_work_cnt==1UL );
  FD_TEST( h->ctx->bam_pending_work_cnt==0UL );

  (void)pack_tile_bam_work_swap_remove( h->ctx, 0UL );
  FD_TEST( !h->ctx->bam_work_cnt );
  FD_TEST( !h->ctx->bam_scheduled_work_cnt );
  FD_TEST( !h->ctx->bam_pending_work_cnt );
  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_bam_signature_prefix_collision( void ) {
  test_pack_tile_harness_t h[1];
  uchar                    sigs[ 2 ][ sizeof(fd_ed25519_sig_t) ];

  test_pack_tile_harness_new( h );
  test_pack_tile_fill_sig( sigs[ 0 ], 10U );
  fd_memcpy( sigs[ 1 ], sigs[ 0 ], sizeof(fd_ed25519_sig_t) );
  sigs[ 1 ][ sizeof(fd_ed25519_sig_t)-1UL ]++;

  FD_TEST( pack_tile_track_bam_work( h->ctx, sigs[ 0 ], 0L, 100U, 0U,
                                     100UL, 100UL, 100UL, 0U, 1U ) );
  FD_TEST( pack_tile_track_bam_work( h->ctx, sigs[ 1 ], 0L, 101U, 0U,
                                     100UL, 100UL, 100UL, 0U, 1U ) );

  ulong work_idx = pack_tile_bam_work_find_by_sig0_state( h->ctx, sigs[ 1 ], PACK_BAM_WORK_STATE_PENDING );
  FD_TEST( work_idx<h->ctx->bam_work_cnt );
  FD_TEST( h->ctx->bam_work[ work_idx ].seq_id==101U );

  uchar matched_idx = UCHAR_MAX;
  work_idx = pack_tile_bam_work_find_by_any_sig( h->ctx, sigs[ 1 ], PACK_BAM_WORK_STATE_PENDING, &matched_idx );
  FD_TEST( work_idx<h->ctx->bam_work_cnt );
  FD_TEST( h->ctx->bam_work[ work_idx ].seq_id==101U );
  FD_TEST( matched_idx==0U );

  pack_tile_retire_all_pending_bam_work_by_sig( h->ctx, sigs[ 0 ] );
  FD_TEST( h->ctx->bam_work_cnt==1UL );
  FD_TEST( h->ctx->bam_pending_work_cnt==1UL );
  FD_TEST( h->ctx->bam_pending_result_cnt==1UL );
  FD_TEST( h->ctx->bam_work[ 0 ].seq_id==101U );

  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_assert_pending_duplicate_results( test_pack_tile_harness_t * h,
                                                 uint                       first_seq_id,
                                                 ulong                      slot ) {
  uchar seen[ 3 ] = { 0U };
  for( uchar i=0U; i<3U; i++ ) {
    if( FD_LIKELY( i ) ) h->ctx->bam_result_publish_cnt = 0UL;
    FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
    uint seq_id = test_pack_tile_last_result( h )->seq_id;
    FD_TEST( seq_id>=first_seq_id && seq_id<first_seq_id+3U );
    uchar seq_idx = (uchar)( seq_id-first_seq_id );
    FD_TEST( !seen[ seq_idx ] );
    seen[ seq_idx ] = 1U;

    fd_bam_bundle_result_t const * res = test_pack_tile_assert_last_result(
        h, seq_id, slot, 2U, FD_BAM_SCHED_ERR_NONE, 2U );
    FD_TEST( res->transaction_err[ 0 ]==bam_types_TransactionErrorReason_COMMIT_CANCELLED );
    FD_TEST( res->transaction_err[ 1 ]==bam_types_TransactionErrorReason_ALREADY_PROCESSED );
    FD_TEST( res->sanitize_success[ 0 ] && res->sanitize_success[ 1 ] );
  }
  FD_TEST( !h->ctx->bam_pending_result_cnt );
}

/* Ignoring landed or completed-unlanded feedback leaks scheduled BAM records
   until slot end.  Retire either outcome exactly once. */
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

/* Keeping completed BAM records consumes the fixed tracking table and rejects
   later same-slot batches.  Completion must immediately free capacity. */
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

static void
test_pack_tile_bam_overlapping_replay_reconciliation( void ) {
  test_pack_tile_harness_t h[1];
  uchar                    sigs[ 2 ][ sizeof(fd_ed25519_sig_t) ];

  test_pack_tile_harness_new( h );
  test_pack_tile_fill_sig( sigs[ 1 ], 90U );
  for( uchar i=0U; i<3U; i++ ) {
    test_pack_tile_fill_sig( sigs[ 0 ], (uchar)( 10U+i ) );
    FD_TEST( pack_tile_track_bam_work( h->ctx, sigs, 0L, 110U+i, 8U,
                                      101UL, 101UL, 101UL, 0U, 2U ) );
  }

  h->ctx->in_kind[ 0 ] = IN_KIND_REPLAY;
  h->ctx->txn_committed = 1U;
  fd_memcpy( h->ctx->executed_txn_sig, sigs[ 1 ], sizeof(fd_ed25519_sig_t) );
  after_frag( h->ctx, 0UL, 0UL, REPLAY_SIG_TXN_EXECUTED, 0UL, 0UL, 0UL, &h->out->stem );

  FD_TEST( h->ctx->bam_work_cnt==0UL );
  test_pack_tile_assert_pending_duplicate_results( h, 110U, 101UL );
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
test_pack_tile_send_bam_resolv_frag( test_pack_tile_harness_t * h,
                                     uint                       seq_id,
                                     ushort                     scheduler_gen,
                                     ushort                     ownership_gen,
                                     ulong                      max_schedule_slot,
                                     uchar                      batch_idx,
                                     uchar                      txn_cnt,
                                     _Bool                      revert_on_error,
                                     _Bool                      blockhash_expired,
                                     ulong                      blockhash_slot,
                                     uchar                      signature_seed ) {
  uchar resolved_buf[ FD_TPU_RESOLVED_MTU ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  ulong sz = test_pack_tile_prepare_resolv_frag( h,
                                                 resolved_buf,
                                                 test_pack_tile_non_vote,
                                                 test_pack_tile_non_vote_sz,
                                                 FD_TXN_M_TPU_SOURCE_BAM,
                                                 blockhash_slot );
  fd_txn_m_t * txnm = (fd_txn_m_t *)resolved_buf;
  txnm->bam.max_schedule_slot = max_schedule_slot;
  txnm->bam.seq_id            = seq_id;
  txnm->bam.scheduler_gen      = scheduler_gen;
  txnm->bam.ownership_gen      = ownership_gen;
  txnm->bam.txn_cnt            = txn_cnt;
  txnm->bam.batch_idx          = batch_idx;
  txnm->bam.revert_on_error    = revert_on_error;
  txnm->bam.blockhash_expired  = blockhash_expired;
  txnm->block_engine.bundle_id = fd_ulong_if( revert_on_error, (ulong)seq_id+1UL, 0UL );
  txnm->block_engine.bundle_txn_cnt = fd_ulong_if( revert_on_error && !batch_idx, (ulong)txn_cnt, 0UL );
  test_pack_tile_fill_sig( fd_txn_m_payload( txnm )+1UL, signature_seed );

  during_frag( h->ctx, 0UL, 0UL, blockhash_slot, 0UL, sz, 0UL );
  after_frag( h->ctx, 0UL, 0UL, blockhash_slot, sz, 0UL, 0UL, &h->out->stem );
}

static void
test_pack_tile_bam_ownership_generation_retirement_barrier( void ) {
  test_pack_tile_harness_t h[1];
  test_pack_tile_harness_new( h );

  ulong generation = 7UL<<1;
  h->ctx->bam_gen_fseq = &generation;
  h->ctx->bam_ownership_gen = 7U;

  fd_ed25519_sig_t pending_old_sig[1];
  fd_ed25519_sig_t scheduled_old_sig[1];
  test_pack_tile_fill_sig( *pending_old_sig,   10U );
  test_pack_tile_fill_sig( *scheduled_old_sig, 20U );

  FD_TEST( pack_tile_track_bam_work( h->ctx, pending_old_sig,   0L, 10U, 7U, 100UL, ULONG_MAX, 100UL, 0U, 1U ) );
  FD_TEST( pack_tile_track_bam_work( h->ctx, scheduled_old_sig, 0L, 11U, 7U, 100UL, ULONG_MAX, 100UL, 0U, 1U ) );
  (void)test_pack_tile_mark_bam_work_scheduled( h, scheduled_old_sig );

  generation = (8UL<<1) | 1UL;
  pack_tile_sync_bam_ownership_generation( h->ctx );

  FD_TEST( generation==(8UL<<1) );
  FD_TEST( h->ctx->bam_ownership_gen==8U );
  FD_TEST( h->ctx->bam_work_cnt==1UL );
  FD_TEST( h->ctx->bam_pending_work_cnt==0UL );
  FD_TEST( h->ctx->bam_scheduled_work_cnt==1UL );
  FD_TEST( pack_tile_bam_work_find_by_sig0( h->ctx, pending_old_sig )==h->ctx->bam_work_cnt );
  FD_TEST( pack_tile_bam_work_find_by_sig0( h->ctx, scheduled_old_sig )<h->ctx->bam_work_cnt );
  test_pack_tile_assert_deleted_sig( pending_old_sig );

  /* An old fragment that was already upstream when pack acknowledged
     the barrier cannot recreate retired work. */
  test_pack_tile_send_bam_resolv_frag( h, 13U, 7U, 7U, ULONG_MAX, 0U, 1U, 0, 0, 100UL, 40U );
  FD_TEST( h->ctx->bam_work_cnt==1UL );
  FD_TEST( test_insert_fini_call_cnt==0UL );

  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_complete_bam_bundle( test_pack_tile_harness_t *,
                                    fd_txn_e_t * const *,
                                    uchar,
                                    uint,
                                    ulong,
                                    ulong,
                                    uchar );

static void
test_pack_tile_make_d18_poc_txns( fd_txn_p_t *,
                                  fd_txn_p_t * );

/* Clearing override before old BAM work is retired makes that work eligible
   for ordinary scheduling.  Delete the generation and wait for pack's ack. */
static void
test_pack_tile_bam_disable_retires_pending_before_override_clear( void ) {
  fd_pack_limits_t limits = {
    .max_cost_per_block           = FD_PACK_MAX_COST_PER_BLOCK_LOWER_BOUND,
    .max_vote_cost_per_block      = FD_PACK_MAX_VOTE_COST_PER_BLOCK_LOWER_BOUND,
    .max_write_cost_per_acct      = FD_PACK_MAX_WRITE_COST_PER_ACCT_LOWER_BOUND,
    .max_data_bytes_per_block     = FD_PACK_MAX_DATA_PER_BLOCK,
    .max_txn_per_microblock       = FD_PACK_MAX_TXN_PER_BUNDLE,
    .max_microblocks_per_block    = 8UL,
    .max_allocated_data_per_block = FD_PACK_MAX_ALLOCATED_DATA_PER_BLOCK,
  };

  /* The control case demonstrates the leak without retirement; the guarded
     case proves the same queued work remains unschedulable. */
  for( int retire_before_clear=0; retire_before_clear<2; retire_before_clear++ ) {
    fd_rng_t pack_rng[1];
    FD_TEST( fd_rng_join( fd_rng_new( pack_rng, 0U, (ulong)retire_before_clear ) ) );

    ulong pack_footprint = fd_pack_footprint( TEST_PACK_TILE_BAM_WORK_CAP,
                                              BUNDLE_META_SZ,
                                              1UL,
                                              &limits );
    void * pack_mem = aligned_alloc( fd_pack_align(),
                                     fd_ulong_align_up( pack_footprint, fd_pack_align() ) );
    FD_TEST( pack_mem );
    fd_pack_t * pack = fd_pack_join( fd_pack_new( pack_mem,
                                                  TEST_PACK_TILE_BAM_WORK_CAP,
                                                  BUNDLE_META_SZ,
                                                  1UL,
                                                  &limits,
                                                  NULL,
                                                  0UL,
                                                  pack_rng ) );
    FD_TEST( pack );
    fd_pack_set_initializer_bundles_ready( pack );

    test_pack_tile_harness_t h[1];
    test_pack_tile_harness_new( h );

    ulong generation = 7UL<<1;
    ulong bam_status = FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE;
    h->ctx->pack              = pack;
    h->ctx->bam_gen_fseq      = &generation;
    h->ctx->bam_status_fseq   = &bam_status;
    h->ctx->bam_ownership_gen = 7U;
    test_insert_fini_use_real_pack = 1;
    test_delete_use_real_pack      = 1;
    FD_TEST( pack_tile_bam_override_active( h->ctx ) );

    fd_txn_e_t * bundle[ 1 ];
    FD_TEST( fd_pack_insert_bundle_init( pack, bundle, 1UL )==bundle );
    fd_txn_p_t unused_invalid_txn[ 1 ];
    test_pack_tile_make_d18_poc_txns( unused_invalid_txn, bundle[ 0 ]->txnp );
    bundle[ 0 ]->txnp->bam.seq_id          = 360001U;
    bundle[ 0 ]->txnp->bam.scheduler_gen   = 9U;
    bundle[ 0 ]->txnp->bam.revert_on_error = 1U;
    h->ctx->current_bundle_bam->scheduler_gen = 9U;
    h->ctx->current_bundle_bam->ownership_gen = 7U;
    test_pack_tile_complete_bam_bundle( h,
                                        bundle,
                                        1U,
                                        360001U,
                                        ULONG_MAX,
                                        100UL,
                                        0U );

    FD_TEST( test_insert_fini_call_cnt==1UL );
    FD_TEST( h->ctx->bam_work_cnt==1UL );
    FD_TEST( h->ctx->bam_pending_work_cnt==1UL );
    FD_TEST( fd_pack_avail_txn_cnt( pack )==1UL );

    if( FD_LIKELY( retire_before_clear ) ) {
      generation = (8UL<<1) | 1UL;
      pack_tile_sync_bam_ownership_generation( h->ctx );

      FD_TEST( generation==(8UL<<1) );
      FD_TEST( h->ctx->bam_ownership_gen==8U );
      FD_TEST( h->ctx->bam_work_cnt==0UL );
      FD_TEST( h->ctx->bam_pending_work_cnt==0UL );
      FD_TEST( test_delete_call_cnt==1UL );
      FD_TEST( fd_pack_avail_txn_cnt( pack )==0UL );
    }

    bam_status = 0UL;
    FD_TEST( !pack_tile_bam_override_active( h->ctx ) );
    fd_txn_e_t scheduled[ FD_PACK_MAX_TXN_PER_BUNDLE ];
    ulong scheduled_cnt = fd_pack_schedule_next_microblock(
        pack,
        FD_PACK_MAX_COST_PER_BLOCK_LOWER_BOUND,
        0.0f,
        0UL,
        FD_PACK_SCHEDULE_VOTE | FD_PACK_SCHEDULE_BUNDLE | FD_PACK_SCHEDULE_TXN,
        scheduled );

    if( FD_UNLIKELY( !retire_before_clear ) ) {
      FD_TEST( scheduled_cnt==1UL );
      FD_TEST( scheduled[ 0 ].txnp->source_tpu==FD_TXN_M_TPU_SOURCE_BAM );
      FD_TEST( scheduled[ 0 ].txnp->bam.seq_id==360001U );
      FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );
    } else {
      FD_TEST( scheduled_cnt==0UL );
    }

    test_pack_tile_harness_delete( h );
    FD_TEST( fd_pack_delete( fd_pack_leave( pack ) )==pack_mem );
    free( pack_mem );
    FD_TEST( fd_rng_delete( fd_rng_leave( pack_rng ) )==pack_rng );
  }
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
  h->ctx->current_bundle_bam->resolver_blockhash_expired_txn_idx = FD_PACK_MAX_TXN_PER_BUNDLE;

  for( uchar i=0U; i<txn_cnt; i++ ) {
    txns[ i ]->txnp->bam.seq_id        = seq_id;
    txns[ i ]->txnp->bam.scheduler_gen = h->ctx->current_bundle_bam->scheduler_gen;
    txns[ i ]->txnp->bam.batch_idx     = i;
    txns[ i ]->txnp->source_tpu        = FD_TXN_M_TPU_SOURCE_BAM;
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
test_pack_tile_sign_and_parse( fd_txn_p_t * txnp,
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

/* A 256-index instruction must fail sanitization before its valid sibling can
   contribute a competing error, so construct both in the same batch. */
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
  test_pack_tile_sign_and_parse( bad_txnp, p, public_key, private_key, sha, 438UL );

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
  test_pack_tile_sign_and_parse( control_txnp, p, public_key, private_key, sha, 223UL );

  fd_txn_t const * control_txn = TXN( control_txnp );
  FD_TEST( control_txn->signature_cnt==1U && control_txn->acct_addr_cnt==3U && control_txn->instr_cnt==2U );
  FD_TEST( control_txn->instr[ 0 ].program_id==2U && control_txn->instr[ 0 ].acct_cnt==0U &&
           control_txn->instr[ 0 ].data_sz==sizeof(set_compute_unit_limit) );
}

/* Making the nonce authority a nonsigner distinguishes invalid-nonce handling
   from ordinary transaction-signature verification. */
static void
test_pack_tile_make_d26_poc_txn( fd_txn_p_t * txnp ) {
  uchar private_key[ 32 ] = {3U};
  uchar public_key [ 32 ];
  uchar blockhash  [ FD_TXN_BLOCKHASH_SZ ] = {4U};
  uchar nonce_acct [ FD_TXN_ACCT_ADDR_SZ ] = {5U};
  uchar nonce_auth [ FD_TXN_ACCT_ADDR_SZ ] = {6U};

  fd_sha512_t sha[1];
  FD_TEST( fd_sha512_join( fd_sha512_new( sha ) ) );
  fd_ed25519_public_from_private( public_key, private_key, sha );

  fd_memset( txnp, 0, sizeof(*txnp) );
  uchar * p = txnp->payload;
  *p++ = 1U; /* signature count: fee payer only */
  p += FD_TXN_SIGNATURE_SZ;
  *p++ = 1U; /* num_required_signatures */
  *p++ = 0U; /* num_readonly_signed_accounts */
  *p++ = 3U; /* recent blockhashes sysvar, nonce authority, system program */
  p += fd_cu16_enc( 5U, p );
  fd_memcpy( p, public_key, FD_TXN_ACCT_ADDR_SZ ); /* fee payer */
  p += FD_TXN_ACCT_ADDR_SZ;
  fd_memcpy( p, nonce_acct, FD_TXN_ACCT_ADDR_SZ );
  p += FD_TXN_ACCT_ADDR_SZ;
  fd_memcpy( p, (uchar const [ FD_TXN_ACCT_ADDR_SZ ]){ SYSVAR_RECENT_BLKHASH_ID }, FD_TXN_ACCT_ADDR_SZ );
  p += FD_TXN_ACCT_ADDR_SZ;
  fd_memcpy( p, nonce_auth, FD_TXN_ACCT_ADDR_SZ );
  p += FD_TXN_ACCT_ADDR_SZ;
  fd_memset( p, 0, FD_TXN_ACCT_ADDR_SZ ); /* System Program ID */
  p += FD_TXN_ACCT_ADDR_SZ;
  fd_memcpy( p, blockhash, sizeof(blockhash) );
  p += sizeof(blockhash);
  p += fd_cu16_enc( 1U, p ); /* instruction count */
  *p++ = 4U; /* System Program */
  p += fd_cu16_enc( 3U, p );
  *p++ = 1U; /* nonce account */
  *p++ = 2U; /* recent blockhashes sysvar */
  *p++ = 3U; /* nonce authority: intentionally not a signer */
  p += fd_cu16_enc( 4U, p );
  FD_STORE( uint, p, 4U ); /* SystemInstruction::AdvanceNonceAccount */
  p += sizeof(uint);
  test_pack_tile_sign_and_parse( txnp, p, public_key, private_key, sha, 272UL );

  fd_txn_t const * txn = TXN( txnp );
  FD_TEST( txn->signature_cnt==1U && txn->acct_addr_cnt==5U && txn->instr_cnt==1U );
  FD_TEST( txn->instr[ 0 ].program_id==4U && txn->instr[ 0 ].acct_cnt==3U && txn->instr[ 0 ].data_sz==4U );
  FD_TEST( FD_LOAD( uint, txnp->payload+txn->instr[ 0 ].data_off )==4U );
  FD_TEST( txnp->payload[ txn->instr[ 0 ].acct_off+2UL ]==3U );
  FD_TEST( !fd_txn_is_signer( txn, 3U ) );
}

/* Cleanup that stops at the first bundle containing landed X leaves orphaned
   records for [B,X] and [C,X]; following all overlaps must not delete X twice. */
static void
test_pack_tile_bam_overlapping_signature_poc_regression( void ) {
  fd_pack_limits_t limits = {
    .max_cost_per_block           = FD_PACK_MAX_COST_PER_BLOCK_LOWER_BOUND,
    .max_vote_cost_per_block      = FD_PACK_MAX_VOTE_COST_PER_BLOCK_LOWER_BOUND,
    .max_write_cost_per_acct      = FD_PACK_MAX_WRITE_COST_PER_ACCT_LOWER_BOUND,
    .max_data_bytes_per_block     = FD_PACK_MAX_DATA_PER_BLOCK,
    .max_txn_per_microblock       = FD_PACK_MAX_TXN_PER_BUNDLE,
    .max_microblocks_per_block    = TEST_PACK_TILE_BAM_WORK_CAP,
    .max_allocated_data_per_block = FD_PACK_MAX_ALLOCATED_DATA_PER_BLOCK,
  };

  fd_rng_t pack_rng[1];
  FD_TEST( fd_rng_join( fd_rng_new( pack_rng, 0U, 0UL ) ) );

  ulong pack_footprint = fd_pack_footprint( TEST_PACK_TILE_BAM_WORK_CAP,
                                            BUNDLE_META_SZ,
                                            1UL,
                                            &limits );
  void * pack_mem = aligned_alloc( fd_pack_align(),
                                   fd_ulong_align_up( pack_footprint, fd_pack_align() ) );
  FD_TEST( pack_mem );
  fd_pack_t * pack = fd_pack_join( fd_pack_new( pack_mem,
                                                TEST_PACK_TILE_BAM_WORK_CAP,
                                                BUNDLE_META_SZ,
                                                1UL,
                                                &limits,
                                                NULL,
                                                0UL,
                                                pack_rng ) );
  FD_TEST( pack );

  test_pack_tile_harness_t h[1];
  test_pack_tile_harness_new( h );
  h->ctx->pack = pack;
  test_insert_fini_use_real_pack = 1;
  test_delete_use_real_pack      = 1;

  uchar sigs[ 2 ][ sizeof(fd_ed25519_sig_t) ];
  test_pack_tile_fill_sig( sigs[ 1 ], 90U );

  fd_txn_p_t invalid_txn[1];
  fd_txn_p_t valid_txn[1];
  test_pack_tile_make_d18_poc_txns( invalid_txn, valid_txn );

  uint const  first_seq_id = 140U;
  ulong const slot         = 104UL;
  for( uchar i=0U; i<3U; i++ ) {
    test_pack_tile_fill_sig( sigs[ 0 ], (uchar)( 10U+i ) );

    fd_txn_e_t * bundle[ 2 ];
    FD_TEST( fd_pack_insert_bundle_init( pack, bundle, 2UL )==bundle );
    for( uchar txn_idx=0U; txn_idx<2U; txn_idx++ ) {
      *bundle[ txn_idx ]->txnp = *valid_txn;
      fd_memcpy( bundle[ txn_idx ]->txnp->payload+1UL,
                 sigs[ txn_idx ],
                 sizeof(fd_ed25519_sig_t) );
    }

    test_pack_tile_complete_bam_bundle( h, bundle, 2U, first_seq_id+i, slot, slot, 0U );
  }
  FD_TEST( fd_pack_avail_txn_cnt( pack )==6UL );

  test_pack_tile_send_executed_txn( h, sigs[ 1 ], FD_EXECUTED_TXN_KIND_LANDED );
  FD_TEST( fd_pack_avail_txn_cnt( pack )==0UL );
  FD_TEST( h->ctx->bam_work_cnt==0UL );
  test_pack_tile_assert_pending_duplicate_results( h, first_seq_id, slot );

  test_pack_tile_harness_delete( h );
  FD_TEST( fd_pack_delete( fd_pack_leave( pack ) )==pack_mem );
  free( pack_mem );
  FD_TEST( fd_rng_delete( fd_rng_leave( pack_rng ) )==pack_rng );
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

/* Builds a real fd_pack and points the harness at it.  Returns the backing
   allocation, which the caller frees after test_pack_tile_harness_delete. */
static void *
test_pack_tile_bam_attach_real_pack( test_pack_tile_harness_t * h,
                                     fd_rng_t *                 rng,
                                     ulong                      pack_depth ) {
  fd_pack_limits_t limits = {
    .max_cost_per_block           = FD_PACK_MAX_COST_PER_BLOCK_LOWER_BOUND,
    .max_vote_cost_per_block      = FD_PACK_MAX_VOTE_COST_PER_BLOCK_LOWER_BOUND,
    .max_write_cost_per_acct      = FD_PACK_MAX_WRITE_COST_PER_ACCT_LOWER_BOUND,
    .max_data_bytes_per_block     = FD_PACK_MAX_DATA_PER_BLOCK,
    .max_txn_per_microblock       = FD_PACK_MAX_TXN_PER_BUNDLE,
    .max_microblocks_per_block    = TEST_PACK_TILE_BAM_WORK_CAP,
    .max_allocated_data_per_block = FD_PACK_MAX_ALLOCATED_DATA_PER_BLOCK,
  };

  FD_TEST( fd_rng_join( fd_rng_new( rng, 0U, 0UL ) ) );

  ulong  pack_footprint = fd_pack_footprint( pack_depth, BUNDLE_META_SZ, 1UL, &limits );
  void * pack_mem       = aligned_alloc( fd_pack_align(),
                                         fd_ulong_align_up( pack_footprint, fd_pack_align() ) );
  FD_TEST( pack_mem );
  fd_pack_t * pack = fd_pack_join( fd_pack_new( pack_mem, pack_depth, BUNDLE_META_SZ, 1UL,
                                                &limits, NULL, 0UL, rng ) );
  FD_TEST( pack );

  h->ctx->pack                        = pack;
  h->ctx->bam_pack_bundle_evicted_cnt = fd_pack_bundle_evicted_cnt( pack );
  test_insert_fini_use_real_pack      = 1;
  test_delete_use_real_pack           = 1;
  return pack_mem;
}

/* Inserting a bundle into a full pack makes fd_pack call delete_worst with
   a FLT_MAX threshold, which is free to evict a bundle we are still
   tracking as pending work.  Pack reports only a count, so without
   reconciliation that work item lingers with no transaction behind it --
   and because the after_frag duplicate check uses fd_pack's signature map
   as the index over pending work, a resend of the same sig0 would then be
   tracked a second time.  Uses a real pack shallower than the BAM work cap
   so the eviction is genuine rather than simulated. */

static void
test_pack_tile_bam_pack_evicted_bundle_is_reconciled( void ) {
  ulong const pack_depth = 4UL;
  test_pack_tile_harness_t h[1];
  fd_rng_t                 pack_rng[1];
  test_pack_tile_harness_new( h );
  void * pack_mem = test_pack_tile_bam_attach_real_pack( h, pack_rng, pack_depth );
  fd_pack_t * pack = h->ctx->pack;

  fd_txn_p_t invalid_txn[1];
  fd_txn_p_t valid_txn[1];
  test_pack_tile_make_d18_poc_txns( invalid_txn, valid_txn );

  uchar       sigs[ 5 ][ sizeof(fd_ed25519_sig_t) ];
  uint  const first_seq_id = 700U;
  ulong const slot         = 104UL;

  /* Fill pack exactly to depth.  Nothing is evicted yet. */
  for( uchar i=0U; i<(uchar)pack_depth; i++ ) {
    test_pack_tile_fill_sig( sigs[ i ], (uchar)( 30U+i ) );

    fd_txn_e_t * bundle[ 1 ];
    FD_TEST( fd_pack_insert_bundle_init( pack, bundle, 1UL )==bundle );
    *bundle[ 0 ]->txnp = *valid_txn;
    fd_memcpy( bundle[ 0 ]->txnp->payload+1UL, sigs[ i ], sizeof(fd_ed25519_sig_t) );

    test_pack_tile_complete_bam_bundle( h, bundle, 1U, first_seq_id+i, slot, slot, 0U );
  }
  FD_TEST( h->ctx->bam_work_cnt==pack_depth );
  FD_TEST( h->ctx->bam_pending_work_cnt==pack_depth );
  FD_TEST( fd_pack_avail_txn_cnt( pack )==pack_depth );
  FD_TEST( !fd_pack_bundle_evicted_cnt( pack ) );

  ulong const evicted_stage_before =
      h->ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_PENDING_EVICTED_IDX ];
  ulong const pending_results_before = h->ctx->bam_pending_result_cnt;

  /* The pool is now entirely bundles, so the forced delete lands on one. */
  test_pack_tile_fill_sig( sigs[ 4 ], 30U+(uchar)pack_depth );
  fd_txn_e_t * overflow[ 1 ];
  FD_TEST( fd_pack_insert_bundle_init( pack, overflow, 1UL )==overflow );
  *overflow[ 0 ]->txnp = *valid_txn;
  fd_memcpy( overflow[ 0 ]->txnp->payload+1UL, sigs[ 4 ], sizeof(fd_ed25519_sig_t) );
  test_pack_tile_complete_bam_bundle( h, overflow, 1U, first_seq_id+(uint)pack_depth, slot, slot, 0U );

  FD_TEST( fd_pack_bundle_evicted_cnt( pack )==1UL );
  FD_TEST( h->ctx->bam_pack_bundle_evicted_cnt==1UL );

  /* One work item was appended and the silently evicted one was retired. */
  FD_TEST( h->ctx->bam_work_cnt==pack_depth );
  FD_TEST( h->ctx->bam_pending_work_cnt==pack_depth );
  FD_TEST( !h->ctx->bam_scheduled_work_cnt );
  FD_TEST( h->ctx->bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_PENDING_EVICTED_IDX ]
           ==evicted_stage_before+1UL );
  FD_TEST( h->ctx->bam_pending_result_cnt==pending_results_before+1UL );

  /* The invariant the duplicate check depends on holds again, and no sig0
     is tracked twice. */
  for( ulong i=0UL; i<h->ctx->bam_work_cnt; i++ ) {
    FD_TEST( fd_pack_contains_transaction( pack,
                                           (fd_ed25519_sig_t const *)(void const *)&h->ctx->bam_work[ i ].sig[ 0 ] ) );
    for( ulong j=i+1UL; j<h->ctx->bam_work_cnt; j++ ) {
      FD_TEST( memcmp( h->ctx->bam_work[ i ].sig[ 0 ],
                       h->ctx->bam_work[ j ].sig[ 0 ],
                       sizeof(fd_ed25519_sig_t) ) );
    }
  }

  /* The retired sequence is reported as losing the priority contest, which
     is what actually happened to it. */
  ulong result_idx = ( h->ctx->bam_result_queue_head + pending_results_before ) %
                     ( 2UL*h->ctx->max_pending_transactions );
  fd_bam_bundle_result_t const * res = &h->ctx->bam_result_queue[ result_idx ];
  FD_TEST( res->scheduling_error==FD_BAM_SCHED_ERR_CONTAINER_FULL );
  FD_TEST( res->seq_id>=first_seq_id && res->seq_id<first_seq_id+(uint)pack_depth );

  /* Replaying the evicted sequence must not produce a second entry for a
     sig0 that is already tracked. */
  uchar evicted_i = (uchar)( res->seq_id - first_seq_id );
  FD_TEST( pack_tile_bam_work_find_by_sig0( h->ctx, sigs[ evicted_i ] )==h->ctx->bam_work_cnt );

  ulong const work_cnt_before_replay = h->ctx->bam_work_cnt;
  fd_txn_e_t * replay[ 1 ];
  FD_TEST( fd_pack_insert_bundle_init( pack, replay, 1UL )==replay );
  *replay[ 0 ]->txnp = *valid_txn;
  fd_memcpy( replay[ 0 ]->txnp->payload+1UL, sigs[ evicted_i ], sizeof(fd_ed25519_sig_t) );
  test_pack_tile_complete_bam_bundle( h, replay, 1U, res->seq_id, slot, slot, 0U );

  FD_TEST( h->ctx->bam_work_cnt==work_cnt_before_replay );
  for( ulong i=0UL; i<h->ctx->bam_work_cnt; i++ ) {
    for( ulong j=i+1UL; j<h->ctx->bam_work_cnt; j++ ) {
      FD_TEST( memcmp( h->ctx->bam_work[ i ].sig[ 0 ],
                       h->ctx->bam_work[ j ].sig[ 0 ],
                       sizeof(fd_ed25519_sig_t) ) );
    }
  }

  test_pack_tile_harness_delete( h );
  free( pack_mem );
}

/* fd_pack accepts duplicate signatures -- it never returns
   FD_PACK_INSERT_REJECT_DUPLICATE -- so a bundle's leading transaction can
   already sit in pack under a signature the tile does not track as BAM
   work.  Runs with the BAM override active, because that is the mode where
   it matters and where it is least obvious: the override refuses new
   non-vote non-BAM transactions, but it does not purge the ones already in
   pack, and those stay in fd_pack's signature map until they expire or
   land.  A hit in that map is therefore only a hint that the pending suffix
   is worth scanning, never grounds to reject the bundle. */

static void
test_pack_tile_bam_sig0_shared_with_untracked_pack_txn_is_accepted( void ) {
  test_pack_tile_harness_t h[1];
  fd_rng_t                 pack_rng[1];
  ulong                    bam_status_fseq = FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE;

  test_pack_tile_harness_new( h );
  h->ctx->bam_status_fseq = &bam_status_fseq;
  void * pack_mem = test_pack_tile_bam_attach_real_pack( h, pack_rng, 8UL );
  fd_pack_t * pack = h->ctx->pack;

  fd_txn_p_t invalid_txn[1];
  fd_txn_p_t valid_txn[1];
  test_pack_tile_make_d18_poc_txns( invalid_txn, valid_txn );

  uchar shared_sig[ sizeof(fd_ed25519_sig_t) ];
  test_pack_tile_fill_sig( shared_sig, 61U );

  /* Put shared_sig into pack without telling the tile about it. */
  fd_txn_e_t * untracked[ 1 ];
  FD_TEST( fd_pack_insert_bundle_init( pack, untracked, 1UL )==untracked );
  *untracked[ 0 ]->txnp = *valid_txn;
  fd_memcpy( untracked[ 0 ]->txnp->payload+1UL, shared_sig, sizeof(fd_ed25519_sig_t) );
  ulong deleted = 0UL;
  FD_TEST( fd_pack_insert_bundle_fini( pack, untracked, 1UL, 100UL, 0, NULL, &deleted, NULL )>=0 );
  FD_TEST( fd_pack_contains_transaction( pack, (fd_ed25519_sig_t const *)(void const *)shared_sig ) );
  FD_TEST( !h->ctx->bam_work_cnt );

  /* A BAM bundle whose sig0 collides with it must still be inserted. */
  ulong const cancel_before = test_bundle_cancel_call_cnt;
  fd_txn_e_t * bundle[ 1 ];
  FD_TEST( fd_pack_insert_bundle_init( pack, bundle, 1UL )==bundle );
  *bundle[ 0 ]->txnp = *valid_txn;
  fd_memcpy( bundle[ 0 ]->txnp->payload+1UL, shared_sig, sizeof(fd_ed25519_sig_t) );
  test_pack_tile_complete_bam_bundle( h, bundle, 1U, 900U, 104UL, 104UL, 0U );

  FD_TEST( test_bundle_cancel_call_cnt==cancel_before );
  FD_TEST( h->ctx->bam_pending_work_cnt==1UL );
  FD_TEST( h->ctx->bam_work_cnt==1UL );
  FD_TEST( pack_tile_bam_work_find_by_sig0( h->ctx, shared_sig )<h->ctx->bam_work_cnt );
  FD_TEST( !h->ctx->insert_result[ FD_PACK_INSERT_REJECT_DUPLICATE + FD_PACK_INSERT_RETVAL_OFF ] );

  test_pack_tile_harness_delete( h );
  free( pack_mem );
}

/* Same premise, reached without any non-BAM transaction being involved:
   bundle A carries Y in a non-leading position, so pack holds Y while no
   bam_work has sig[0]==Y.  Bundle B leading with Y is not a duplicate of A
   and must be accepted.  Reachable with the BAM override active and pack
   holding nothing but BAM work, so it is purely a BAM-to-BAM overlap. */

static void
test_pack_tile_bam_overlapping_bundle_leading_sig_is_accepted( void ) {
  test_pack_tile_harness_t h[1];
  fd_rng_t                 pack_rng[1];
  ulong                    bam_status_fseq = FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE;

  test_pack_tile_harness_new( h );
  h->ctx->bam_status_fseq = &bam_status_fseq;
  void * pack_mem = test_pack_tile_bam_attach_real_pack( h, pack_rng, 8UL );
  fd_pack_t * pack = h->ctx->pack;

  fd_txn_p_t invalid_txn[1];
  fd_txn_p_t valid_txn[1];
  test_pack_tile_make_d18_poc_txns( invalid_txn, valid_txn );

  uchar sig_x[ sizeof(fd_ed25519_sig_t) ];
  uchar sig_y[ sizeof(fd_ed25519_sig_t) ];
  uchar sig_z[ sizeof(fd_ed25519_sig_t) ];
  test_pack_tile_fill_sig( sig_x, 71U );
  test_pack_tile_fill_sig( sig_y, 72U );
  test_pack_tile_fill_sig( sig_z, 73U );

  /* Bundle A = [X,Y], tracked.  bam_work holds sig[0]==X, sig[1]==Y. */
  fd_txn_e_t * bundle_a[ 2 ];
  FD_TEST( fd_pack_insert_bundle_init( pack, bundle_a, 2UL )==bundle_a );
  *bundle_a[ 0 ]->txnp = *valid_txn;
  *bundle_a[ 1 ]->txnp = *valid_txn;
  fd_memcpy( bundle_a[ 0 ]->txnp->payload+1UL, sig_x, sizeof(fd_ed25519_sig_t) );
  fd_memcpy( bundle_a[ 1 ]->txnp->payload+1UL, sig_y, sizeof(fd_ed25519_sig_t) );
  test_pack_tile_complete_bam_bundle( h, bundle_a, 2U, 910U, 104UL, 104UL, 0U );
  FD_TEST( h->ctx->bam_pending_work_cnt==1UL );
  FD_TEST( fd_pack_contains_transaction( pack, (fd_ed25519_sig_t const *)(void const *)sig_y ) );
  FD_TEST( pack_tile_bam_work_find_by_sig0( h->ctx, sig_y )==h->ctx->bam_work_cnt );

  /* Bundle B = [Y,Z] leads with Y. */
  ulong const cancel_before = test_bundle_cancel_call_cnt;
  fd_txn_e_t * bundle_b[ 2 ];
  FD_TEST( fd_pack_insert_bundle_init( pack, bundle_b, 2UL )==bundle_b );
  *bundle_b[ 0 ]->txnp = *valid_txn;
  *bundle_b[ 1 ]->txnp = *valid_txn;
  fd_memcpy( bundle_b[ 0 ]->txnp->payload+1UL, sig_y, sizeof(fd_ed25519_sig_t) );
  fd_memcpy( bundle_b[ 1 ]->txnp->payload+1UL, sig_z, sizeof(fd_ed25519_sig_t) );
  test_pack_tile_complete_bam_bundle( h, bundle_b, 2U, 911U, 104UL, 104UL, 0U );

  FD_TEST( test_bundle_cancel_call_cnt==cancel_before );
  FD_TEST( h->ctx->bam_pending_work_cnt==2UL );
  FD_TEST( pack_tile_bam_work_find_by_sig0( h->ctx, sig_y )<h->ctx->bam_work_cnt );

  /* Replacing B must delete B exactly.  Signature-wide deletion by Y would
     also delete A because Y is A's non-leading transaction. */
  fd_txn_e_t * bundle_b_resend[ 2 ];
  FD_TEST( fd_pack_insert_bundle_init( pack, bundle_b_resend, 2UL )==bundle_b_resend );
  *bundle_b_resend[ 0 ]->txnp = *valid_txn;
  *bundle_b_resend[ 1 ]->txnp = *valid_txn;
  fd_memcpy( bundle_b_resend[ 0 ]->txnp->payload+1UL, sig_y, sizeof(fd_ed25519_sig_t) );
  fd_memcpy( bundle_b_resend[ 1 ]->txnp->payload+1UL, sig_z, sizeof(fd_ed25519_sig_t) );
  test_pack_tile_complete_bam_bundle( h, bundle_b_resend, 2U, 911U, 104UL, 104UL, 0U );

  FD_TEST( h->ctx->bam_pending_work_cnt==2UL );
  FD_TEST( fd_pack_avail_txn_cnt( pack )==4UL );
  FD_TEST( fd_pack_contains_bam_bundle( pack,
                                        (fd_ed25519_sig_t const *)(void const *)sig_x,
                                        910U,
                                        0U ) );
  FD_TEST( fd_pack_contains_bam_bundle( pack,
                                        (fd_ed25519_sig_t const *)(void const *)sig_y,
                                        911U,
                                        0U ) );

  /* Simulate pack having evicted B.  A still makes the generic signature
     query for Y succeed, so reconciliation must use the exact BAM identity
     and retire only B's work item. */
  FD_TEST( fd_pack_delete_bam_bundle( pack,
                                      (fd_ed25519_sig_t const *)(void const *)sig_y,
                                      911U,
                                      0U )==2UL );
  FD_TEST( fd_pack_contains_transaction( pack,
                                         (fd_ed25519_sig_t const *)(void const *)sig_y ) );
  FD_TEST( !fd_pack_contains_bam_bundle( pack,
                                         (fd_ed25519_sig_t const *)(void const *)sig_y,
                                         911U,
                                         0U ) );

  ulong pending_results_before = h->ctx->bam_pending_result_cnt;
  pack_tile_reconcile_pending_bam_work( h->ctx );
  FD_TEST( h->ctx->bam_pending_work_cnt==1UL );
  FD_TEST( h->ctx->bam_work_cnt==1UL );
  FD_TEST( pack_tile_bam_work_find_by_sig0( h->ctx, sig_x )<h->ctx->bam_work_cnt );
  FD_TEST( fd_pack_avail_txn_cnt( pack )==2UL );
  FD_TEST( h->ctx->bam_pending_result_cnt==pending_results_before+1UL );

  ulong result_idx = ( h->ctx->bam_result_queue_head + pending_results_before ) %
                     ( 2UL*h->ctx->max_pending_transactions );
  fd_bam_bundle_result_t const * result = &h->ctx->bam_result_queue[ result_idx ];
  FD_TEST( result->seq_id==911U );
  FD_TEST( result->scheduling_error==FD_BAM_SCHED_ERR_CONTAINER_FULL );

  test_pack_tile_harness_delete( h );
  free( pack_mem );
}

/* Hardcoding reject index zero blames the wrong transaction when a later
   member's blockhash expires.  Preserve the index through every pack stage. */
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

/* Dropping an expired marker during resolver handoff lets a sibling's later
   error replace it.  Preserve its index and BLOCKHASH_NOT_FOUND reason. */
static void
test_pack_tile_bam_resolver_expired_member_reports_exact_idx( void ) {
  for( ulong revert_on_error=0UL; revert_on_error<2UL; revert_on_error++ ) {
    test_pack_tile_harness_t h[1];
    fd_txn_e_t               txns[ 3 ];
    test_pack_tile_harness_new( h );
    fd_memset( txns, 0, sizeof(txns) );

    uint   seq_id             = (uint)( 600UL+revert_on_error );
    ushort scheduler_gen      = 7U;
    ulong  blockhash_slot     = 500UL;
    ulong  max_schedule_slot  = 600UL;

    test_pack_tile_fill_sig( txns[ 0 ].txnp->payload+1UL, (uchar)( 140U+revert_on_error ) );
    txns[ 0 ].txnp->source_tpu                         = FD_TXN_M_TPU_SOURCE_BAM;
    txns[ 0 ].txnp->bam.batch_idx                      = 0U;
    h->ctx->current_bundle->id                         = (ulong)seq_id+1UL;
    h->ctx->current_bundle->txn_cnt                    = 3UL;
    h->ctx->current_bundle->txn_received               = 1UL;
    h->ctx->current_bundle->min_blockhash_slot         = blockhash_slot;
    h->ctx->current_bundle->bundle                     = h->ctx->current_bundle->_txn;
    h->ctx->current_bundle_bam->max_schedule_slot      = max_schedule_slot;
    h->ctx->current_bundle_bam->scheduler_gen          = scheduler_gen;
    h->ctx->current_bundle_bam->ownership_gen          = 0U;
    h->ctx->current_bundle_bam->is_bam                 = 1;
    h->ctx->current_bundle_bam->min_blockhash_slot_txn_idx = 0U;
    h->ctx->current_bundle_bam->resolver_blockhash_expired_txn_idx = FD_PACK_MAX_TXN_PER_BUNDLE;
    for( uchar batch_idx=0U; batch_idx<3U; batch_idx++ ) h->ctx->current_bundle->_txn[ batch_idx ] = &txns[ batch_idx ];

    for( uchar batch_idx=1U; batch_idx<3U; batch_idx++ ) {
      test_pack_tile_send_bam_resolv_frag( h,
                                           seq_id,
                                           scheduler_gen,
                                           0U,
                                           max_schedule_slot,
                                           batch_idx,
                                           3U,
                                           (_Bool)revert_on_error,
                                           batch_idx==1U || ( revert_on_error && batch_idx==2U ),
                                           blockhash_slot,
                                           (uchar)( 150U+10U*revert_on_error+batch_idx ) );
      if( FD_LIKELY( batch_idx==1U ) ) {
        FD_TEST( h->ctx->current_bundle->txn_received==(ulong)batch_idx+1UL );
        FD_TEST( h->ctx->bam_pending_result_cnt==0UL );
      }
    }

    FD_TEST( test_insert_fini_call_cnt==0UL );
    FD_TEST( test_bundle_cancel_call_cnt==1UL );
    FD_TEST( test_bundle_cancel_last_txn_cnt==3UL );
    FD_TEST( h->ctx->current_bundle->bundle==NULL );
    FD_TEST( !h->ctx->current_bundle_bam->is_bam );
    FD_TEST( h->ctx->bam_pending_result_cnt==1UL );

    FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
    fd_bam_bundle_result_t const * expired = test_pack_tile_assert_last_result( h,
                                                                                seq_id,
                                                                                max_schedule_slot,
                                                                                3U,
                                                                                FD_BAM_SCHED_ERR_NONE,
                                                                                3U );
    FD_TEST( expired->transaction_err[ 0 ]==bam_types_TransactionErrorReason_COMMIT_CANCELLED );
    FD_TEST( expired->transaction_err[ 1 ]==bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND );
    FD_TEST( expired->transaction_err[ 2 ]==bam_types_TransactionErrorReason_COMMIT_CANCELLED );
    FD_TEST( expired->sanitize_success[ 0 ] );
    FD_TEST( expired->sanitize_success[ 1 ] );
    FD_TEST( expired->sanitize_success[ 2 ] );
    FD_TEST( h->ctx->bam_pending_result_cnt==0UL );

    ulong published_result_cnt = h->out->seqs[ 0 ];
    pack_tile_abandon_current_bam_bundle( h->ctx, PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_NEW_SEQ_BEFORE_COMPLETE );
    pack_tile_abandon_current_bam_bundle( h->ctx, PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_LEADER_SLOT_END );
    FD_TEST( h->ctx->bam_pending_result_cnt==0UL );
    FD_TEST( h->out->seqs[ 0 ]==published_result_cnt );

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

/* Reusing seq_id without clearing its old pending group carries stale indices
   into the replacement and can overflow bundle_idx.  Reset before insertion. */
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
test_pack_tile_bam_pending_duplicate_rejected_before_insert( uint  new_seq_id,
                                                              ulong new_slot ) {
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

  test_pack_tile_complete_bam_bundle( h, (fd_txn_e_t *[1]){ new_txn }, 1U,
                                      new_seq_id, new_slot, new_slot, 0U );

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
  fd_bam_bundle_result_t const * dup = test_pack_tile_assert_last_result( h, new_seq_id, new_slot, 1U,
                                                                         FD_BAM_SCHED_ERR_NONE, 1U );
  FD_TEST( dup->transaction_err[ 0 ] == bam_types_TransactionErrorReason_ALREADY_PROCESSED );
  FD_TEST( dup->sanitize_success[ 0 ] == 1U );
  FD_TEST( h->ctx->bam_pending_result_cnt == 0UL );

  test_pack_tile_harness_delete( h );
}

static void
test_pack_tile_bam_same_seq_different_slot_pending_duplicate_rejected_before_insert( void ) {
  test_pack_tile_bam_pending_duplicate_rejected_before_insert( 10U, 101UL );
}

static void
test_pack_tile_bam_different_seq_pending_duplicate_rejected_before_insert( void ) {
  test_pack_tile_bam_pending_duplicate_rejected_before_insert( 11U, 101UL );
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
test_pack_tile_bam_result_queue_wrap_preserves_fifo( void ) {
  test_pack_tile_harness_t h[1];
  test_pack_tile_harness_new( h );

  ulong const cap = 2UL*h->ctx->max_pending_transactions;
  h->ctx->bam_result_queue_head = cap-1UL;

  fd_bam_bundle_result_t first  = fd_bam_result_base( 100U, 0U, 100UL, 1U );
  fd_bam_bundle_result_t second = fd_bam_result_base( 101U, 0U, 101UL, 1U );
  FD_TEST( pack_tile_enqueue_bam_result( h->ctx, &first  ) );
  FD_TEST( pack_tile_enqueue_bam_result( h->ctx, &second ) );
  FD_TEST( h->ctx->bam_result_queue[ cap-1UL ].seq_id==100U );
  FD_TEST( h->ctx->bam_result_queue[ 0UL     ].seq_id==101U );

  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  test_pack_tile_assert_last_result( h, 100U, 100UL, 1U, FD_BAM_SCHED_ERR_NONE, 0U );
  FD_TEST( h->ctx->bam_result_queue_head==0UL );

  h->ctx->bam_result_publish_cnt = 0UL;
  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  test_pack_tile_assert_last_result( h, 101U, 101UL, 1U, FD_BAM_SCHED_ERR_NONE, 0U );
  FD_TEST( h->ctx->bam_result_queue_head==1UL );
  FD_TEST( !h->ctx->bam_pending_result_cnt );

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

/* If pack continues past transaction zero's 256 account indices, the valid
   sibling can mask the real failure.  Serialize SANITIZE_ERROR at index zero. */
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

/* Classifying a nonsigner nonce authority as SIGNATURE_FAILURE emits the wrong
   scheduler result.  Real pack must serialize BLOCKHASH_NOT_FOUND at index zero. */
static void
test_pack_tile_bam_invalid_nonce_poc_serializes_blockhash_not_found( void ) {
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

  fd_txn_e_t * bundle[ 1 ];
  FD_TEST( fd_pack_insert_bundle_init( pack, bundle, 1UL )==bundle );
  test_pack_tile_make_d26_poc_txn( bundle[ 0 ]->txnp );

  test_pack_tile_harness_t h[1];
  test_pack_tile_harness_new( h );
  h->ctx->pack                   = pack;
  test_insert_fini_use_real_pack = 1;

  test_pack_tile_complete_bam_bundle( h, bundle, 1U, 26U, 1UL, 0UL, 0U );

  FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
  fd_bam_bundle_result_t pack_result = *test_pack_tile_assert_last_result( h,
                                                                           26U,
                                                                           1UL,
                                                                           1U,
                                                                           FD_BAM_SCHED_ERR_NONE,
                                                                           1U );
  FD_TEST( pack_result.bundle_err         == FD_BAM_BUNDLE_ERR_NONE );
  FD_TEST( pack_result.transaction_err[0] == bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND );
  FD_TEST( pack_result.sanitize_success[0] == 1U );
  test_pack_tile_harness_delete( h );

  FD_TEST( fd_pack_delete( fd_pack_leave( pack ) )==pack_mem );
  free( pack_mem );
  FD_TEST( fd_rng_delete( fd_rng_leave( pack_rng ) )==pack_rng );

  /* A correct internal reject is insufficient if wire mapping changes its
     reason or index, so decode the emitted scheduler result as well. */
  ulong wksp_footprint = 16UL<<20;
  void * wksp_mem = aligned_alloc( FD_SHMEM_NORMAL_PAGE_SZ, wksp_footprint );
  FD_TEST( wksp_mem );
  ulong part_max = fd_wksp_part_max_est( wksp_footprint, 64UL<<10 );
  fd_wksp_t * wksp = fd_wksp_join( fd_wksp_new( wksp_mem, "bam-d26-wire-test", 1U, part_max,
                                                fd_wksp_data_max_est( wksp_footprint, part_max ) ) );
  FD_TEST( wksp );
  FD_TEST( !fd_shmem_join_anonymous( "bam-d26-wire-test",
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
           bam_types_NotCommitted_transaction_error_tag );
  FD_TEST( wire_result->result.not_committed.reason.transaction_error.index==0U );
  FD_TEST( wire_result->result.not_committed.reason.transaction_error.reason==
           bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND );

  test_bam_env_destroy( env );
  FD_TEST( !fd_shmem_leave_anonymous( wksp_mem, NULL ) );
  FD_TEST( fd_wksp_delete( fd_wksp_leave( wksp ) )==wksp_mem );
  free( wksp_mem );
}

/* Defaulting every pack reject to index zero misattributes later-member
   failures.  Carry reject_txn_idx to the wire and cancel only unaffected peers. */
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

/* Reprocessing an abandoned partial batch can overwrite OUTSIDE_LEADER_SLOT
   with deserialization failure or emit twice.  Pack must remain sole owner. */
static void
test_pack_tile_bam_atomic_abandon_result_mapping( void ) {
  pack_tile_bam_bundle_assembly_abandon_reason_t const reasons[] = {
    PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_NEW_SEQ_BEFORE_COMPLETE,
    PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_LEADER_SLOT_END,
    PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_POH_TIMEOUT,
  };

  for( ulong reason_idx=0UL; reason_idx<sizeof(reasons)/sizeof(reasons[0]); reason_idx++ ) {
    test_pack_tile_harness_t h[1];
    fd_txn_e_t               old_txns[ 2 ];

    test_pack_tile_harness_new( h );
    fd_memset( old_txns, 0, sizeof(old_txns) );
    uint  seq_id             = 321U + (uint)reason_idx;
    _Bool is_new_seq_abandon = reasons[ reason_idx ]==PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_NEW_SEQ_BEFORE_COMPLETE;

    test_pack_tile_fill_sig( old_txns[ 0 ].txnp->payload + 1UL, 1U );
    test_pack_tile_fill_sig( old_txns[ 1 ].txnp->payload + 1UL, 2U );
    old_txns[ 0 ].txnp->scheduler_arrival_time_nanos = 0L;
    old_txns[ 1 ].txnp->scheduler_arrival_time_nanos = 0L;

    h->ctx->current_bundle->id                 = (ulong)seq_id + 1UL;
    h->ctx->current_bundle->txn_cnt            = 3UL;
    h->ctx->current_bundle->txn_received       = 2UL;
    h->ctx->current_bundle->min_blockhash_slot = 500UL;
    h->ctx->current_bundle->_txn[ 0 ]          = &old_txns[ 0 ];
    h->ctx->current_bundle->_txn[ 1 ]          = &old_txns[ 1 ];
    h->ctx->current_bundle->bundle             = h->ctx->current_bundle->_txn;
    h->ctx->current_bundle_bam->max_schedule_slot = 600UL;
    h->ctx->current_bundle_bam->is_bam            = 1;
    h->ctx->current_bundle_bam->resolver_blockhash_expired_txn_idx = FD_PACK_MAX_TXN_PER_BUNDLE;

    pack_tile_abandon_current_bam_bundle( h->ctx, reasons[ reason_idx ] );

    FD_TEST( test_bundle_cancel_call_cnt == 1UL );
    FD_TEST( test_bundle_cancel_last_txn_cnt == 3UL );
    FD_TEST( h->ctx->current_bundle->bundle == NULL );
    FD_TEST( h->ctx->current_bundle_bam->is_bam == (uchar)!is_new_seq_abandon );
    FD_TEST( h->ctx->bam_pending_result_cnt == (ulong)is_new_seq_abandon );

    if( FD_UNLIKELY( !is_new_seq_abandon ) ) {
      uchar resolved_buf[ FD_TPU_RESOLVED_MTU ] __attribute__((aligned(FD_CHUNK_ALIGN)));
      ulong sz = test_pack_tile_prepare_resolv_frag( h, resolved_buf, test_pack_tile_non_vote, test_pack_tile_non_vote_sz, FD_TXN_M_TPU_SOURCE_BAM, 500UL );
      fd_txn_m_t * txnm = (fd_txn_m_t *)resolved_buf;
      txnm->bam.max_schedule_slot          = 600UL;
      txnm->bam.seq_id                     = seq_id;
      txnm->bam.txn_cnt                    = 3U;
      txnm->bam.batch_idx                  = 2U;
      txnm->bam.revert_on_error            = 1;
      txnm->block_engine.bundle_id         = (ulong)txnm->bam.seq_id + 1UL;

      during_frag( h->ctx, 0UL, 0UL, 500UL, 0UL, sz, 0UL );
      FD_TEST( h->ctx->bundle_kind == PACK_TILE_BUNDLE_KIND_BAM );
      after_frag( h->ctx, 0UL, 0UL, 500UL, sz, 0UL, 0UL, &h->out->stem );

      FD_TEST( h->ctx->current_bundle->bundle == NULL );
      FD_TEST( !h->ctx->current_bundle_bam->is_bam );
      FD_TEST( test_bundle_cancel_call_cnt == 1UL );
      FD_TEST( h->ctx->bam_pending_result_cnt == 1UL );
    }

    FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
    fd_bam_bundle_result_t const * published = test_pack_tile_assert_last_result( h, seq_id, 600UL, 3U,
                                                                                 is_new_seq_abandon ? FD_BAM_SCHED_ERR_NONE : FD_BAM_SCHED_ERR_OUTSIDE_SLOT, 0U );
    if( FD_LIKELY( is_new_seq_abandon ) ) {
      FD_TEST( published->bundle_err   == FD_BAM_BUNDLE_ERR_DESER );
      FD_TEST( published->deser_index  == 2U );
      FD_TEST( published->deser_reason == bam_types_DeserializationErrorReason_SANITIZE_ERROR );
    } else {
      FD_TEST( published->bundle_err == FD_BAM_BUNDLE_ERR_NONE );
    }

    test_pack_tile_harness_delete( h );
  }
}

/* If verify or resolver publishes an incomplete-batch failure directly, pack
   publishes a second terminal result.  Preprocessing stages must defer to pack. */
static void
test_pack_tile_bam_preprocess_marker_has_one_terminal_owner( void ) {
  for( ulong timeout_first=0UL; timeout_first<2UL; timeout_first++ ) {
    test_pack_tile_harness_t h[1];
    fd_txn_e_t               tx0[1];
    uchar                    resolved_buf[ FD_TPU_RESOLVED_MTU ] __attribute__((aligned(FD_CHUNK_ALIGN)));
    uint                     seq_id = (uint)( 400UL+timeout_first );

    test_pack_tile_harness_new( h );
    fd_memset( tx0, 0, sizeof(tx0) );
    test_pack_tile_fill_sig( tx0->txnp->payload+1UL, 9U );

    h->ctx->current_bundle->id                 = (ulong)seq_id+1UL;
    h->ctx->current_bundle->txn_cnt            = 3UL;
    h->ctx->current_bundle->txn_received       = 1UL;
    h->ctx->current_bundle->min_blockhash_slot = 500UL;
    h->ctx->current_bundle->_txn[ 0 ]          = tx0;
    h->ctx->current_bundle->bundle             = h->ctx->current_bundle->_txn;
    h->ctx->current_bundle_bam->max_schedule_slot = 600UL;
    h->ctx->current_bundle_bam->scheduler_gen     = 7U;
    h->ctx->current_bundle_bam->is_bam            = 1U;
    h->ctx->current_bundle_bam->resolver_blockhash_expired_txn_idx = FD_PACK_MAX_TXN_PER_BUNDLE;

    if( FD_UNLIKELY( timeout_first ) ) {
      pack_tile_abandon_current_bam_bundle( h->ctx, PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_POH_TIMEOUT );
      FD_TEST( h->ctx->bam_pending_result_cnt==0UL );
      FD_TEST( h->ctx->current_bundle_bam->is_bam );
    }

    ulong sz = test_pack_tile_prepare_resolv_frag( h, resolved_buf, test_pack_tile_non_vote,
                                                   test_pack_tile_non_vote_sz, FD_TXN_M_TPU_SOURCE_BAM, 500UL );
    fd_txn_m_t * txnm = (fd_txn_m_t *)resolved_buf;
    txnm->bam.max_schedule_slot = 600UL;
    txnm->bam.seq_id            = seq_id;
    txnm->bam.scheduler_gen      = 7U;
    txnm->bam.txn_cnt            = 3U;
    txnm->bam.batch_idx          = 1U;
    txnm->bam.revert_on_error    = 1U;
    txnm->bam.preprocess_failed  = 1U;
    txnm->block_engine.bundle_id = (ulong)seq_id+1UL;

    during_frag( h->ctx, 0UL, 0UL, 500UL, 0UL, sz, 0UL );
    after_frag( h->ctx, 0UL, 0UL, 500UL, sz, 0UL, 0UL, &h->out->stem );

    FD_TEST( !h->ctx->current_bundle_bam->is_bam );
    FD_TEST( h->ctx->current_bundle->bundle==NULL );
    FD_TEST( h->ctx->bam_pending_result_cnt==1UL );
    FD_TEST( test_bundle_cancel_call_cnt==1UL );

    pack_tile_abandon_current_bam_bundle( h->ctx, PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_POH_TIMEOUT );
    FD_TEST( h->ctx->bam_pending_result_cnt==1UL );

    FD_TEST( pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );
    fd_bam_bundle_result_t const * published =
        test_pack_tile_assert_last_result( h, seq_id, 600UL, 3U, FD_BAM_SCHED_ERR_NONE, 0U );
    FD_TEST( published->bundle_err   == FD_BAM_BUNDLE_ERR_DESER );
    FD_TEST( published->deser_index  == 1U );
    FD_TEST( published->deser_reason == bam_types_DeserializationErrorReason_SANITIZE_ERROR );
    FD_TEST( !pack_tile_drain_one_pending_bam_result( h->ctx, &h->out->stem ) );

    test_pack_tile_harness_delete( h );
  }
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );
  fd_metrics_register( (ulong *)fd_metrics_new( metrics_scratch, 0UL ) );

  test_pack_tile_bam_work_partition_mutations();
  test_pack_tile_bam_signature_prefix_collision();
  test_pack_tile_bam_pack_evicted_bundle_is_reconciled();
  test_pack_tile_bam_sig0_shared_with_untracked_pack_txn_is_accepted();
  test_pack_tile_bam_overlapping_bundle_leading_sig_is_accepted();
  test_pack_tile_bam_completion_outcomes();
  test_pack_tile_bam_completion_tracking_reuses_capacity();
  test_pack_tile_bam_overlapping_replay_reconciliation();
  test_pack_tile_bam_overlapping_signature_poc_regression();
  test_pack_tile_bam_stale_max_schedule_slot_rejected();
  test_pack_tile_bam_expired_blockhash_reports_member_idx();
  test_pack_tile_bam_resolver_expired_member_reports_exact_idx();
  test_pack_tile_bam_stale_results_drain_without_drop();
  test_pack_tile_bam_same_seq_pending_duplicate_replaces_before_insert();
  test_pack_tile_bam_same_seq_different_slot_pending_duplicate_rejected_before_insert();
  test_pack_tile_bam_different_seq_pending_duplicate_rejected_before_insert();
  test_pack_tile_bam_scheduled_duplicate_rejected_before_insert();
  test_pack_tile_bam_pending_result_does_not_shadow_new_work();
  test_pack_tile_bam_queued_results_preserve_fifo_before_direct_publish();
  test_pack_tile_bam_result_queue_wrap_preserves_fifo();
  test_pack_tile_bam_pending_results_reserve_direct_result_headroom();
  test_pack_tile_bam_scheduled_work_does_not_consume_result_headroom();
  test_pack_tile_bam_instr_acct_reject_serializes_exact_member();
  test_pack_tile_bam_invalid_nonce_poc_serializes_blockhash_not_found();
  test_pack_tile_bam_result_mapping_insert_reject();
  test_pack_tile_bam_result_mapping_tracking_reject();
  test_pack_tile_bam_atomic_abandon_result_mapping();
  test_pack_tile_bam_preprocess_marker_has_one_terminal_owner();
  test_pack_tile_bam_ownership_generation_retirement_barrier();
  test_pack_tile_bam_disable_retires_pending_before_override_clear();
  test_pack_tile_bam_override_allows_votes_only_from_normal_ingress();
  test_pack_tile_bam_override_after_frag_preserves_votes_only();
  test_pack_tile_bam_override_drops_block_engine_bundles();
  test_pack_tile_bam_override_after_frag_cancels_block_engine_bundle();
  test_pack_tile_bam_fee_meta_seqlock_keeps_last_snapshot();
  test_pack_tile_bam_mode_edges_retire_pending_initializer();
  test_pack_tile_bam_missing_builder_cfg_accepted();

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
