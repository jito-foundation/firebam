#define fd_tile_execle bam_fuzz_execle_tile_unused
#include "../../discof/execle/fd_execle_tile.c"
#undef fd_tile_execle

#include "fuzz_bam_pipeline_stage.h"
#include "../../flamenco/log_collector/fd_log_collector.h"
#include "../../flamenco/runtime/fd_blockhashes.h"
#include "../../flamenco/runtime/tests/fd_svm_mini.h"
#include <sys/mman.h>
#include <unistd.h>

#define BAM_FUZZ_EXECLE_MCACHE_DEPTH          (256UL)
#define BAM_FUZZ_EXECLE_LIVE_SLOTS            (2UL)
#define BAM_FUZZ_EXECLE_TXNCACHE_LIVE_SLOTS   (8UL)
#define BAM_FUZZ_EXECLE_TXNCACHE_TXN_PER_SLOT (256UL)
#define BAM_FUZZ_EXECLE_ACCOUNT_LAMPORTS      (1000000000000000UL)

struct bam_fuzz_execle {
  fd_execle_tile_t * ctx;
  void *             storage_mem;
  ulong              storage_footprint;
  fd_txncache_t *    txncache;

  fd_svm_mini_t *    mini;
  ulong              active_slot;
  ulong              bank_idx;
  fd_bank_t *        bank;

  bam_fuzz_outs_t    outs;
};

static void
bam_fuzz_execle_runtime_bind( bam_fuzz_execle_t * h ) {
  h->ctx->banks                    = h->mini->banks;
  h->ctx->accdb                    = h->mini->runtime->accdb;
  h->ctx->progcache->metrics       = h->mini->progcache->metrics;
  h->ctx->runtime->accdb           = h->mini->runtime->accdb;
  h->ctx->runtime->progcache       = h->mini->progcache;
  h->ctx->runtime->status_cache    = h->txncache;
  fd_memset( &h->ctx->runtime->log, 0, sizeof(h->ctx->runtime->log) );
  fd_log_collector_init( h->ctx->log_collector, 0 );
  h->ctx->runtime->log.log_collector       = h->ctx->log_collector;
  h->ctx->runtime->fuzz.enabled            = 0;
  h->ctx->runtime->fuzz.reclaim_accounts   = 0;
  h->ctx->runtime->accounts.executable_cnt = 0UL;
  fd_memset( &h->ctx->runtime->metrics, 0, sizeof(h->ctx->runtime->metrics) );
}

static fd_execle_out_t
bam_fuzz_execle_out( bam_fuzz_execle_t * h,
                     ulong               idx,
                     fd_wksp_t *         wksp,
                     ulong               mtu ) {
  bam_fuzz_link_t const * o = bam_fuzz_outs_add( &h->outs, idx, wksp, BAM_FUZZ_EXECLE_MCACHE_DEPTH, mtu );
  return (fd_execle_out_t){ .idx = idx, .mem = o->mem, .chunk0 = o->chunk0, .wmark = o->wmark, .chunk = o->chunk0 };
}

bam_fuzz_execle_t *
bam_fuzz_execle_new( fd_wksp_t *             wksp,
                     bam_fuzz_link_t const * in,
                     bam_fuzz_link_t         out[ BAM_FUZZ_OUT_MAX ],
                     ulong *                 busy_fseq ) {
  bam_fuzz_execle_t * h = fd_wksp_alloc_laddr( wksp, alignof(bam_fuzz_execle_t), sizeof(bam_fuzz_execle_t), 1UL );
  FD_TEST( h );
  fd_memset( h, 0, sizeof(*h) );
  h->active_slot = ULONG_MAX;
  h->bank_idx    = ULONG_MAX;

  /* Fresh anonymous pages: the large tile context starts zeroed
     without being touched. */
  ulong txncache_shmem_footprint = fd_txncache_shmem_footprint( BAM_FUZZ_EXECLE_TXNCACHE_LIVE_SLOTS, BAM_FUZZ_EXECLE_TXNCACHE_TXN_PER_SLOT );
  ulong l = FD_LAYOUT_INIT;
  l = FD_LAYOUT_APPEND( l, alignof(fd_execle_tile_t), sizeof(fd_execle_tile_t) );
  l = FD_LAYOUT_APPEND( l, FD_BLAKE3_ALIGN,           FD_BLAKE3_FOOTPRINT );
  l = FD_LAYOUT_APPEND( l, FD_BMTREE_COMMIT_ALIGN,    FD_BMTREE_COMMIT_FOOTPRINT(0) );
  l = FD_LAYOUT_APPEND( l, fd_txncache_shmem_align(), txncache_shmem_footprint );
  l = FD_LAYOUT_APPEND( l, fd_txncache_align(),       fd_txncache_footprint( BAM_FUZZ_EXECLE_TXNCACHE_LIVE_SLOTS ) );
  h->storage_footprint = FD_LAYOUT_FINI( l, 4096UL );
  h->storage_mem = mmap( NULL, h->storage_footprint, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0 );
  FD_TEST( h->storage_mem!=MAP_FAILED );

  FD_SCRATCH_ALLOC_INIT( alloc, h->storage_mem );
  fd_execle_tile_t * ctx = h->ctx = FD_SCRATCH_ALLOC_APPEND( alloc, alignof(fd_execle_tile_t), sizeof(fd_execle_tile_t) );
  ctx->blake3 = fd_blake3_join( fd_blake3_new( FD_SCRATCH_ALLOC_APPEND( alloc, FD_BLAKE3_ALIGN, FD_BLAKE3_FOOTPRINT ) ) );
  FD_TEST( ctx->blake3 );
  ctx->bmtree = FD_SCRATCH_ALLOC_APPEND( alloc, FD_BMTREE_COMMIT_ALIGN, FD_BMTREE_COMMIT_FOOTPRINT(0) );
  fd_txncache_shmem_t * txncache_shmem = fd_txncache_shmem_join( fd_txncache_shmem_new(
      FD_SCRATCH_ALLOC_APPEND( alloc, fd_txncache_shmem_align(), txncache_shmem_footprint ),
      BAM_FUZZ_EXECLE_TXNCACHE_LIVE_SLOTS, BAM_FUZZ_EXECLE_TXNCACHE_TXN_PER_SLOT, 0UL ) );
  FD_TEST( txncache_shmem );
  h->txncache = fd_txncache_join( fd_txncache_new(
      FD_SCRATCH_ALLOC_APPEND( alloc, fd_txncache_align(), fd_txncache_footprint( BAM_FUZZ_EXECLE_TXNCACHE_LIVE_SLOTS ) ), txncache_shmem ) );
  FD_TEST( h->txncache );
  FD_TEST( FD_SCRATCH_ALLOC_FINI( alloc, 4096UL )<=(ulong)h->storage_mem+h->storage_footprint );

  fd_svm_mini_limits_t limits[1];
  fd_svm_mini_limits_default( limits );
  limits->max_live_slots           = BAM_FUZZ_EXECLE_LIVE_SLOTS;
  limits->max_fork_width           = 2UL;
  limits->max_vote_accounts        = 8UL;
  limits->max_stake_accounts       = 8UL;
  limits->max_accounts             = 256UL;
  limits->max_account_space_bytes  = 4UL<<20;
  limits->max_txn_write_locks      = 128UL;
  limits->max_txn_per_slot         = BAM_FUZZ_EXECLE_TXNCACHE_TXN_PER_SLOT;
  h->mini = fd_svm_mini_create( wksp, limits );
  FD_TEST( h->mini );

  ctx->busy_fseq        = busy_fseq;
  ctx->pack_in_mem      = in->mem;
  ctx->pack_in_chunk0   = in->chunk0;
  ctx->pack_in_wmark    = in->wmark;
  ctx->rebates_for_slot = ULONG_MAX;
  ctx->ns_per_tick      = 1.f / (float)fd_tempo_tick_per_ns( NULL );
  FD_TEST( fd_pack_rebate_sum_join( fd_pack_rebate_sum_new( ctx->rebater, ctx->rebate_seed ) ) );

  for( ulong i=0UL; i<FD_PACK_MAX_TXN_PER_BUNDLE; i++ ) {
    ctx->txn_in[ i ].bundle.prev_txn_cnt = i;
    for( ulong j=0UL; j<i; j++ ) ctx->txn_in[ i ].bundle.prev_txn_outs[ j ] = &ctx->txn_out[ j ];
  }

  *ctx->out_poh  = bam_fuzz_execle_out( h, BAM_FUZZ_EXECLE_OUT_POH,      wksp, MAX_MICROBLOCK_SZ              );
  *ctx->out_bam  = bam_fuzz_execle_out( h, BAM_FUZZ_EXECLE_OUT_BANK_BAM, wksp, sizeof(fd_bam_bundle_result_t) );
  *ctx->out_pack = (fd_execle_out_t){ .idx = ULONG_MAX };

  bam_fuzz_execle_runtime_bind( h );
  fd_memcpy( out, h->outs.link, sizeof(h->outs.link) );
  return h;
}

void
bam_fuzz_execle_delete( bam_fuzz_execle_t * h ) {
  /* svm-mini's destructor leaves its accdb allocations to callers that
     destroy the whole workspace.  This persistent fuzz workspace must
     release them after every input. */
  void * accdb_shmem_mem = h->mini->accdb_shmem_mem;
  void * accdb_join_mem  = h->mini->accdb_join_mem;
  int    accdb_fd        = h->mini->accdb_fd;
  fd_svm_mini_destroy( h->mini );
  FD_TEST( !close( accdb_fd ) );
  fd_wksp_free_laddr( accdb_join_mem  );
  fd_wksp_free_laddr( accdb_shmem_mem );

  FD_TEST( fd_blake3_delete( fd_blake3_leave( h->ctx->blake3 ) ) );
  bam_fuzz_outs_delete( &h->outs );
  FD_TEST( !munmap( h->storage_mem, h->storage_footprint ) );
  fd_wksp_free_laddr( h );
}

static fd_txncache_fork_id_t
bam_fuzz_execle_reset_txncache( bam_fuzz_execle_t * h ) {
  fd_txncache_reset( h->txncache );

  fd_txncache_fork_id_t parent = { .val = USHORT_MAX };
  for( ulong i=0UL; i<=BAM_FUZZ_FIXTURE_CNT; i++ ) {
    ulong         payload_sz;
    uchar const * payload = bam_fuzz_fixture( i, &payload_sz );
    uchar txn_buf[ FD_TXN_MAX_SZ ];
    FD_TEST( fd_txn_parse( payload, payload_sz, txn_buf, NULL ) );
    fd_hash_t const * blockhash = (fd_hash_t const *)fd_txn_get_recent_blockhash( (fd_txn_t const *)txn_buf, payload );
    fd_txncache_fork_id_t fork = fd_txncache_attach_child( h->txncache, parent );
    fd_txncache_finalize_fork( h->txncache, fork, 0UL, blockhash->uc );
    parent = fork;
  }

  return fd_txncache_attach_child( h->txncache, parent );
}

void
bam_fuzz_execle_prepare_slot( bam_fuzz_execle_t * h,
                              ulong               slot,
                              fd_banks_t **       banks,
                              fd_accdb_t **       accdb,
                              void const **       bank,
                              ulong *             bank_idx ) {
  *banks = h->mini->banks;
  *accdb = h->mini->runtime->accdb;

  if( FD_UNLIKELY( slot==ULONG_MAX ) ) {
    h->active_slot = ULONG_MAX;
    h->bank        = NULL;
    h->bank_idx    = ULONG_MAX;
  } else if( FD_UNLIKELY( h->active_slot!=slot || !h->bank ) ) {
    fd_svm_mini_params_t params[1];
    fd_svm_mini_params_default( params );
    params->hash_seed             = 0xBADC0DEUL;
    params->root_slot             = slot ? slot-1UL : 0UL;
    params->slots_per_epoch       = 16UL;
    params->init_sysvars          = 1;
    params->init_feature_accounts = 0;
    params->init_builtins         = 1;
    params->mock_validator_cnt    = 1UL;

    ulong root_bank_idx = fd_svm_mini_reset( h->mini, params );
    fd_txncache_fork_id_t txncache_fork = bam_fuzz_execle_reset_txncache( h );
    fd_bank_t * root_bank = fd_svm_mini_bank( h->mini, root_bank_idx );
    FD_TEST( root_bank );
    root_bank->txncache_fork_id = txncache_fork;

    if( FD_LIKELY( slot>params->root_slot ) ) {
      h->bank_idx = fd_svm_mini_attach_child( h->mini, root_bank_idx, slot );
      h->bank = fd_svm_mini_bank( h->mini, h->bank_idx );
      FD_TEST( h->bank );
      h->bank->txncache_fork_id = txncache_fork;
    } else {
      h->bank_idx = root_bank_idx;
      h->bank     = root_bank;
    }

    FD_TEST( h->bank->f.slot==slot );
    h->bank->f.features.enable_tx_v1 = 0UL;
    h->active_slot = slot;
    bam_fuzz_execle_runtime_bind( h );
  }

  *bank     = h->bank;
  *bank_idx = h->bank_idx;
}

static void
bam_fuzz_execle_seed_account( bam_fuzz_execle_t *    h,
                              fd_accdb_fork_id_t     fork_id,
                              fd_acct_addr_t const * acct ) {
  if( FD_LIKELY( fd_accdb_exists( h->mini->runtime->accdb, fork_id, acct->b ) ) ) return;

  fd_svm_mini_add_lamports( h->mini, fork_id, (fd_pubkey_t const *)acct, BAM_FUZZ_EXECLE_ACCOUNT_LAMPORTS );
}

static void
bam_fuzz_execle_seed_txn( bam_fuzz_execle_t * h,
                          fd_txn_e_t const *  txne ) {
  fd_txn_p_t const * txnp = txne->txnp;
  fd_txn_t const *   txn  = TXN( txnp );
  fd_hash_t const *  blockhash = (fd_hash_t const *)fd_txn_get_recent_blockhash( txn, txnp->payload );

  fd_blockhashes_t * blockhashes = &h->bank->f.block_hash_queue;
  if( FD_LIKELY( !fd_blockhashes_check_age( blockhashes, blockhash, 151UL ) ) ) {
    fd_blockhash_info_t * info = fd_blockhashes_push_new( blockhashes, blockhash );
    info->lamports_per_signature = 0UL;
  }

  fd_accdb_fork_id_t fork_id = h->bank->accdb_fork_id;
  fd_acct_addr_t const * imm_accts = fd_txn_get_acct_addrs( txn, txnp->payload );
  for( ulong i=0UL; i<(ulong)txn->acct_addr_cnt; i++ ) {
    bam_fuzz_execle_seed_account( h, fork_id, &imm_accts[ i ] );
  }

  ulong alt_cnt = fd_txn_account_cnt( txn, FD_TXN_ACCT_CAT_ALT );
  for( ulong i=0UL; i<alt_cnt; i++ ) {
    bam_fuzz_execle_seed_account( h, fork_id, &txne->alt_accts[ i ] );
  }
}

static void
bam_fuzz_execle_seed_microblock( bam_fuzz_execle_t *    h,
                                 fd_frag_meta_t const * meta ) {
  FD_TEST( h->bank );
  FD_TEST( meta->sz>=sizeof(fd_microblock_execle_trailer_t) );
  FD_TEST( (meta->sz-sizeof(fd_microblock_execle_trailer_t))%sizeof(fd_txn_e_t)==0UL );

  fd_txn_e_t const * txne = (fd_txn_e_t const *)fd_chunk_to_laddr_const( h->ctx->pack_in_mem, meta->chunk );
  ulong txn_cnt = (meta->sz-sizeof(fd_microblock_execle_trailer_t))/sizeof(fd_txn_e_t);
  for( ulong i=0UL; i<txn_cnt; i++ ) bam_fuzz_execle_seed_txn( h, &txne[ i ] );
}

bam_fuzz_res_t
bam_fuzz_execle_frag( bam_fuzz_execle_t *    h,
                      fd_frag_meta_t const * meta,
                      ulong                  seq ) {
  bam_fuzz_res_t res = bam_fuzz_outs_begin( &h->outs );
  FD_TEST( !before_frag( h->ctx, 0UL, seq, meta->sig ) );
  bam_fuzz_execle_seed_microblock( h, meta );
  during_frag( h->ctx, 0UL, seq, meta->sig, meta->chunk, meta->sz, meta->ctl );
  after_frag( h->ctx, 0UL, seq, meta->sig, meta->sz, meta->tsorig, meta->tspub, h->outs.stem );
  return bam_fuzz_outs_end( &h->outs, res );
}
