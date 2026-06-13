#define FD_TILE_TEST 1
#define fd_tile_execle bam_fuzz_execle_tile_unused
#include "../../discof/execle/fd_execle_tile.c"
#undef fd_tile_execle

#include "fuzz_bam_e2e_execle.h"
#include "../../flamenco/accdb/fd_accdb_sync.h"
#include "../../flamenco/log_collector/fd_log_collector.h"
#include "../../flamenco/runtime/fd_blockhashes.h"
#include "../../flamenco/runtime/tests/fd_svm_mini.h"
#include <sys/mman.h>

FD_IMPORT_BINARY( bam_fuzz_execle_txn1, "src/ballet/txn/fixtures/transaction1.bin" );
FD_IMPORT_BINARY( bam_fuzz_execle_txn4, "src/ballet/txn/fixtures/transaction4.bin" );
FD_IMPORT_BINARY( bam_fuzz_execle_txn6, "src/ballet/txn/fixtures/transaction6.bin" );
FD_IMPORT_BINARY( bam_fuzz_execle_pack_txn1, "src/disco/pack/fixtures/txn1.bin" );
FD_IMPORT_BINARY( bam_fuzz_execle_pack_txn3, "src/disco/pack/fixtures/txn3.bin" );
FD_IMPORT_BINARY( bam_fuzz_execle_pack_txn4, "src/disco/pack/fixtures/txn4.bin" );
FD_IMPORT_BINARY( bam_fuzz_execle_pack_txn5, "src/disco/pack/fixtures/txn5.bin" );
FD_IMPORT_BINARY( bam_fuzz_execle_pack_txn6, "src/disco/pack/fixtures/txn6.bin" );
FD_IMPORT_BINARY( bam_fuzz_execle_pack_txn7, "src/disco/pack/fixtures/txn7.bin" );

#define BAM_FUZZ_EXECLE_OUT_POH_IDX      (0UL)
#define BAM_FUZZ_EXECLE_OUT_BANK_BAM_IDX (1UL)
#define BAM_FUZZ_EXECLE_OUT_CNT          (2UL)
#define BAM_FUZZ_EXECLE_MCACHE_DEPTH     (256UL)
#define BAM_FUZZ_EXECLE_LIVE_SLOTS       (2UL)
#define BAM_FUZZ_EXECLE_TXNCACHE_LIVE_SLOTS (8UL)
#define BAM_FUZZ_EXECLE_TXNCACHE_TXN_PER_SLOT (256UL)
#define BAM_FUZZ_EXECLE_ACCOUNT_LAMPORTS (1000000000000000UL)

struct bam_fuzz_execle {
  fd_wksp_t * wksp;

  fd_execle_tile_t * ctx;

  void * storage_mem;
  ulong  storage_footprint;

  void * blake3_mem;
  void * bmtree_mem;
  void * txncache_shmem_mem;
  fd_txncache_shmem_t * txncache_shmem;
  void * txncache_mem;
  fd_txncache_t * txncache;

  fd_svm_mini_t * mini;
  ulong           active_slot;
  ulong           root_bank_idx;
  ulong           bank_idx;
  fd_bank_t *     bank;

  void *           poh_mcache_mem;
  fd_frag_meta_t * poh_mcache;
  void *           poh_dcache_mem;
  uchar *          poh_dcache;

  void *           bank_bam_mcache_mem;
  fd_frag_meta_t * bank_bam_mcache;
  void *           bank_bam_dcache_mem;
  uchar *          bank_bam_dcache;

  fd_stem_context_t stem[1];
  fd_frag_meta_t *  stem_mcaches[ BAM_FUZZ_EXECLE_OUT_CNT ];
  ulong             stem_seqs[ BAM_FUZZ_EXECLE_OUT_CNT ];
  ulong             stem_depths[ BAM_FUZZ_EXECLE_OUT_CNT ];
  ulong             stem_cr_avail[ BAM_FUZZ_EXECLE_OUT_CNT ];
  ulong             stem_min_cr_avail[1];
  int               stem_out_reliable[ BAM_FUZZ_EXECLE_OUT_CNT ];
};

static void
bam_fuzz_execle_runtime_bind( bam_fuzz_execle_t * h ) {
  h->ctx->banks                    = h->mini->banks;
  h->ctx->runtime->accdb           = h->mini->accdb;
  h->ctx->runtime->progcache       = h->mini->progcache;
  h->ctx->runtime->status_cache    = h->txncache;
  h->ctx->runtime->acc_pool        = h->mini->acc_pool;
  fd_memset( &h->ctx->runtime->log, 0, sizeof(h->ctx->runtime->log) );
  fd_log_collector_init( h->ctx->log_collector, 0 );
  h->ctx->runtime->log.log_collector       = h->ctx->log_collector;
  h->ctx->runtime->fuzz.enabled            = 0;
  h->ctx->runtime->fuzz.reclaim_accounts   = 0;
  h->ctx->runtime->accounts.executable_cnt = 0UL;
  fd_memset( &h->ctx->runtime->metrics, 0, sizeof(h->ctx->runtime->metrics) );
}

bam_fuzz_execle_t *
bam_fuzz_execle_new( fd_wksp_t * wksp,
                     fd_wksp_t * in_mem,
                     ulong       in_chunk0,
                     ulong       in_wmark,
                     ulong *     busy_fseq,
                     bam_fuzz_link_t * bank_bam_out ) {
  bam_fuzz_execle_t * h = fd_wksp_alloc_laddr( wksp, alignof(bam_fuzz_execle_t), sizeof(bam_fuzz_execle_t), 1UL );
  FD_TEST( h );
  fd_memset( h, 0, sizeof(*h) );
  h->wksp        = wksp;
  h->active_slot = ULONG_MAX;
  h->root_bank_idx = ULONG_MAX;
  h->bank_idx    = ULONG_MAX;

  ulong l = FD_LAYOUT_INIT;
  l = FD_LAYOUT_APPEND( l, alignof(fd_execle_tile_t), sizeof(fd_execle_tile_t) );
  l = FD_LAYOUT_APPEND( l, FD_BLAKE3_ALIGN,           FD_BLAKE3_FOOTPRINT );
  l = FD_LAYOUT_APPEND( l, FD_BMTREE_COMMIT_ALIGN,    FD_BMTREE_COMMIT_FOOTPRINT(0) );
  l = FD_LAYOUT_APPEND( l, fd_txncache_shmem_align(), fd_txncache_shmem_footprint( BAM_FUZZ_EXECLE_TXNCACHE_LIVE_SLOTS,
                                                                                    BAM_FUZZ_EXECLE_TXNCACHE_TXN_PER_SLOT ) );
  l = FD_LAYOUT_APPEND( l, fd_txncache_align(),       fd_txncache_footprint( BAM_FUZZ_EXECLE_TXNCACHE_LIVE_SLOTS ) );
  l = FD_LAYOUT_APPEND( l, fd_mcache_align(),         fd_mcache_footprint( BAM_FUZZ_EXECLE_MCACHE_DEPTH, 0UL ) );
  l = FD_LAYOUT_APPEND( l, fd_dcache_align(),         fd_dcache_footprint( fd_dcache_req_data_sz( MAX_MICROBLOCK_SZ, BAM_FUZZ_EXECLE_MCACHE_DEPTH, 1UL, 1 ), 0UL ) );
  l = FD_LAYOUT_APPEND( l, fd_mcache_align(),         fd_mcache_footprint( BAM_FUZZ_EXECLE_MCACHE_DEPTH, 0UL ) );
  l = FD_LAYOUT_APPEND( l, fd_dcache_align(),         fd_dcache_footprint( fd_dcache_req_data_sz( sizeof(fd_bam_bundle_result_t), BAM_FUZZ_EXECLE_MCACHE_DEPTH, 1UL, 1 ), 0UL ) );
  h->storage_footprint = FD_LAYOUT_FINI( l, fd_dcache_align() );
  h->storage_mem = mmap( NULL, h->storage_footprint, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0 );
  FD_TEST( h->storage_mem!=MAP_FAILED );

  FD_SCRATCH_ALLOC_INIT( alloc, h->storage_mem );
  ulong poh_data_sz      = fd_dcache_req_data_sz( MAX_MICROBLOCK_SZ, BAM_FUZZ_EXECLE_MCACHE_DEPTH, 1UL, 1 );
  ulong bank_bam_data_sz = fd_dcache_req_data_sz( sizeof(fd_bam_bundle_result_t), BAM_FUZZ_EXECLE_MCACHE_DEPTH, 1UL, 1 );

  h->ctx = FD_SCRATCH_ALLOC_APPEND( alloc, alignof(fd_execle_tile_t), sizeof(fd_execle_tile_t) );
  FD_TEST( h->ctx );

  h->blake3_mem = FD_SCRATCH_ALLOC_APPEND( alloc, FD_BLAKE3_ALIGN, FD_BLAKE3_FOOTPRINT );
  h->ctx->blake3 = fd_blake3_join( fd_blake3_new( h->blake3_mem ) );
  FD_TEST( h->ctx->blake3 );

  h->bmtree_mem = FD_SCRATCH_ALLOC_APPEND( alloc, FD_BMTREE_COMMIT_ALIGN, FD_BMTREE_COMMIT_FOOTPRINT(0) );
  FD_TEST( h->bmtree_mem );
  h->ctx->bmtree = h->bmtree_mem;

  h->txncache_shmem_mem = FD_SCRATCH_ALLOC_APPEND( alloc,
                                                   fd_txncache_shmem_align(),
                                                   fd_txncache_shmem_footprint( BAM_FUZZ_EXECLE_TXNCACHE_LIVE_SLOTS,
                                                                                BAM_FUZZ_EXECLE_TXNCACHE_TXN_PER_SLOT ) );
  h->txncache_shmem = fd_txncache_shmem_join( fd_txncache_shmem_new( h->txncache_shmem_mem,
                                                                     BAM_FUZZ_EXECLE_TXNCACHE_LIVE_SLOTS,
                                                                     BAM_FUZZ_EXECLE_TXNCACHE_TXN_PER_SLOT ) );
  FD_TEST( h->txncache_shmem );

  h->txncache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_txncache_align(), fd_txncache_footprint( BAM_FUZZ_EXECLE_TXNCACHE_LIVE_SLOTS ) );
  h->txncache = fd_txncache_join( fd_txncache_new( h->txncache_mem, h->txncache_shmem ) );
  FD_TEST( h->txncache );

  h->poh_mcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_mcache_align(), fd_mcache_footprint( BAM_FUZZ_EXECLE_MCACHE_DEPTH, 0UL ) );
  h->poh_mcache     = fd_mcache_join( fd_mcache_new( h->poh_mcache_mem, BAM_FUZZ_EXECLE_MCACHE_DEPTH, 0UL, 0UL ) );
  FD_TEST( h->poh_mcache );
  h->poh_dcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_dcache_align(), fd_dcache_footprint( poh_data_sz, 0UL ) );
  h->poh_dcache     = fd_dcache_join( fd_dcache_new( h->poh_dcache_mem, poh_data_sz, 0UL ) );
  FD_TEST( h->poh_dcache );

  h->bank_bam_mcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_mcache_align(), fd_mcache_footprint( BAM_FUZZ_EXECLE_MCACHE_DEPTH, 0UL ) );
  h->bank_bam_mcache     = fd_mcache_join( fd_mcache_new( h->bank_bam_mcache_mem, BAM_FUZZ_EXECLE_MCACHE_DEPTH, 0UL, 0UL ) );
  FD_TEST( h->bank_bam_mcache );
  h->bank_bam_dcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_dcache_align(), fd_dcache_footprint( bank_bam_data_sz, 0UL ) );
  h->bank_bam_dcache     = fd_dcache_join( fd_dcache_new( h->bank_bam_dcache_mem, bank_bam_data_sz, 0UL ) );
  FD_TEST( h->bank_bam_dcache );

  ulong storage_top = FD_SCRATCH_ALLOC_FINI( alloc, fd_dcache_align() );
  FD_TEST( storage_top <= (ulong)h->storage_mem + h->storage_footprint );

  fd_svm_mini_limits_t limits[1];
  fd_svm_mini_limits_default( limits );
  limits->max_live_slots           = BAM_FUZZ_EXECLE_LIVE_SLOTS;
  limits->max_fork_width           = 2UL;
  limits->max_vote_accounts        = 8UL;
  limits->max_stake_accounts       = 8UL;
  limits->max_accounts             = 256UL;
  limits->max_account_space_bytes  = 4UL<<20;
  limits->max_progcache_recs       = 128UL;
  limits->max_progcache_heap_bytes = 65536UL;
  limits->max_txn_write_locks      = 128UL;
  h->mini = fd_svm_mini_create( wksp, limits );
  FD_TEST( h->mini );

  h->stem_mcaches[ BAM_FUZZ_EXECLE_OUT_POH_IDX      ] = h->poh_mcache;
  h->stem_mcaches[ BAM_FUZZ_EXECLE_OUT_BANK_BAM_IDX ] = h->bank_bam_mcache;
  for( ulong i=0UL; i<BAM_FUZZ_EXECLE_OUT_CNT; i++ ) {
    h->stem_depths[ i ] = BAM_FUZZ_EXECLE_MCACHE_DEPTH;
    h->stem_cr_avail[ i ] = ULONG_MAX;
    h->stem_out_reliable[ i ] = 0;
  }
  h->stem_min_cr_avail[0] = ULONG_MAX;
  *h->stem = (fd_stem_context_t) {
    .mcaches             = h->stem_mcaches,
    .seqs                = h->stem_seqs,
    .depths              = h->stem_depths,
    .cr_avail            = h->stem_cr_avail,
    .min_cr_avail        = h->stem_min_cr_avail,
    .cr_decrement_amount = 0UL,
    .out_reliable        = h->stem_out_reliable,
  };

  h->ctx->kind_id        = 0UL;
  h->ctx->busy_fseq      = busy_fseq;
  h->ctx->pack_in_mem    = in_mem;
  h->ctx->pack_in_chunk0 = in_chunk0;
  h->ctx->pack_in_wmark  = in_wmark;
  h->ctx->rebates_for_slot = ULONG_MAX;
  h->ctx->enable_rebates = 0;
  h->ctx->ns_per_tick    = 1.f / (float)fd_tempo_tick_per_ns( NULL );

  FD_TEST( fd_pack_rebate_sum_join( fd_pack_rebate_sum_new( h->ctx->rebater ) ) );

  for( ulong i=0UL; i<FD_PACK_MAX_TXN_PER_BUNDLE; i++ ) {
    h->ctx->txn_in[ i ].bundle.prev_txn_cnt = i;
    for( ulong j=0UL; j<i; j++ ) h->ctx->txn_in[ i ].bundle.prev_txn_outs[ j ] = &h->ctx->txn_out[ j ];
  }

  *h->ctx->out_poh = (fd_execle_out_t) {
    .idx    = BAM_FUZZ_EXECLE_OUT_POH_IDX,
    .mem    = (fd_wksp_t *)h->poh_dcache,
    .chunk0 = fd_dcache_compact_chunk0( h->poh_dcache, h->poh_dcache ),
    .wmark  = fd_dcache_compact_wmark ( h->poh_dcache, h->poh_dcache, MAX_MICROBLOCK_SZ ),
    .chunk  = fd_dcache_compact_chunk0( h->poh_dcache, h->poh_dcache ),
  };
  *h->ctx->out_pack = (fd_execle_out_t) {
    .idx    = ULONG_MAX,
    .mem    = NULL,
    .chunk0 = 0UL,
    .wmark  = 0UL,
    .chunk  = 0UL,
  };
  *h->ctx->out_bam = (fd_execle_out_t) {
    .idx    = BAM_FUZZ_EXECLE_OUT_BANK_BAM_IDX,
    .mem    = (fd_wksp_t *)h->bank_bam_dcache,
    .chunk0 = fd_dcache_compact_chunk0( h->bank_bam_dcache, h->bank_bam_dcache ),
    .wmark  = fd_dcache_compact_wmark ( h->bank_bam_dcache, h->bank_bam_dcache, sizeof(fd_bam_bundle_result_t) ),
    .chunk  = fd_dcache_compact_chunk0( h->bank_bam_dcache, h->bank_bam_dcache ),
  };

  bam_fuzz_execle_runtime_bind( h );
  if( FD_LIKELY( bank_bam_out ) ) {
    *bank_bam_out = (bam_fuzz_link_t) {
      .mem    = (fd_wksp_t *)h->bank_bam_dcache,
      .mcache = h->bank_bam_mcache,
      .chunk0 = h->ctx->out_bam->chunk0,
      .wmark  = h->ctx->out_bam->wmark,
      .depth  = BAM_FUZZ_EXECLE_MCACHE_DEPTH,
    };
  }
  return h;
}

void
bam_fuzz_execle_delete( bam_fuzz_execle_t * h ) {
  if( FD_UNLIKELY( !h ) ) return;

  if( h->mini ) {
    fd_svm_mini_destroy( h->mini );
    h->mini = NULL;
  }
  if( h->ctx && h->ctx->blake3 ) {
    void * mem = fd_blake3_delete( fd_blake3_leave( h->ctx->blake3 ) );
    FD_TEST( mem==h->blake3_mem );
    h->ctx->blake3 = NULL;
  }

  bam_fuzz_delete_mcache( &h->poh_mcache,      h->poh_mcache_mem      );
  bam_fuzz_delete_dcache( &h->poh_dcache,      h->poh_dcache_mem      );
  bam_fuzz_delete_mcache( &h->bank_bam_mcache, h->bank_bam_mcache_mem );
  bam_fuzz_delete_dcache( &h->bank_bam_dcache, h->bank_bam_dcache_mem );

  if( h->storage_mem ) {
    FD_TEST( !munmap( h->storage_mem, h->storage_footprint ) );
    h->storage_mem = NULL;
  }
  fd_wksp_free_laddr( h );
}

static fd_txncache_fork_id_t
bam_fuzz_execle_reset_txncache( bam_fuzz_execle_t * h ) {
  fd_txncache_reset( h->txncache );

  uchar const * const payloads[] = {
    bam_fuzz_execle_txn1,
    bam_fuzz_execle_txn4,
    bam_fuzz_execle_txn6,
    bam_fuzz_execle_pack_txn1,
    bam_fuzz_execle_pack_txn3,
    bam_fuzz_execle_pack_txn4,
    bam_fuzz_execle_pack_txn5,
    bam_fuzz_execle_pack_txn6,
    bam_fuzz_execle_pack_txn7,
  };
  ulong const payload_szs[] = {
    bam_fuzz_execle_txn1_sz,
    bam_fuzz_execle_txn4_sz,
    bam_fuzz_execle_txn6_sz,
    bam_fuzz_execle_pack_txn1_sz,
    bam_fuzz_execle_pack_txn3_sz,
    bam_fuzz_execle_pack_txn4_sz,
    bam_fuzz_execle_pack_txn5_sz,
    bam_fuzz_execle_pack_txn6_sz,
    bam_fuzz_execle_pack_txn7_sz,
  };

  fd_txncache_fork_id_t parent = { .val = USHORT_MAX };
  for( ulong i=0UL; i<sizeof(payloads)/sizeof(payloads[0]); i++ ) {
    uchar txn_buf[ FD_TXN_MAX_SZ ];
    ulong parsed_sz = fd_txn_parse( payloads[ i ], payload_szs[ i ], txn_buf, NULL );
    FD_TEST( parsed_sz );
    fd_hash_t const * blockhash =
        (fd_hash_t const *)fd_txn_get_recent_blockhash( (fd_txn_t const *)txn_buf, payloads[ i ] );
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
                              fd_accdb_user_t **  accdb,
                              void const **       bank,
                              ulong *             bank_idx ) {
  if( banks ) *banks = h->mini->banks;
  if( accdb ) *accdb = h->mini->accdb;

  if( FD_UNLIKELY( slot==ULONG_MAX ) ) {
    h->active_slot = ULONG_MAX;
    h->bank        = NULL;
    h->bank_idx    = ULONG_MAX;
    if( bank     ) *bank     = NULL;
    if( bank_idx ) *bank_idx = ULONG_MAX;
    return;
  }

  if( FD_LIKELY( h->active_slot==slot && h->bank ) ) {
    if( bank     ) *bank     = h->bank;
    if( bank_idx ) *bank_idx = h->bank_idx;
    return;
  }

  fd_svm_mini_params_t params[1];
  fd_svm_mini_params_default( params );
  params->hash_seed             = 0xBADC0DEUL;
  params->root_slot             = slot ? slot-1UL : 0UL;
  params->slots_per_epoch       = 16UL;
  params->init_sysvars          = 1;
  params->init_feature_accounts = 0;
  params->init_builtins         = 1;
  params->mock_validator_cnt    = 1UL;

  h->root_bank_idx = fd_svm_mini_reset( h->mini, params );
  fd_txncache_fork_id_t txncache_fork = bam_fuzz_execle_reset_txncache( h );
  fd_bank_t * root_bank = fd_svm_mini_bank( h->mini, h->root_bank_idx );
  FD_TEST( root_bank );
  root_bank->txncache_fork_id = txncache_fork;

  if( FD_LIKELY( slot>params->root_slot ) ) {
    h->bank_idx = fd_svm_mini_attach_child( h->mini, h->root_bank_idx, slot );
    h->bank = fd_svm_mini_bank( h->mini, h->bank_idx );
    FD_TEST( h->bank );
    h->bank->txncache_fork_id = txncache_fork;
  } else {
    h->bank_idx = h->root_bank_idx;
    h->bank     = root_bank;
  }

  FD_TEST( h->bank->f.slot==slot );
  h->active_slot = slot;
  bam_fuzz_execle_runtime_bind( h );

  if( bank     ) *bank     = h->bank;
  if( bank_idx ) *bank_idx = h->bank_idx;
}

static void
bam_fuzz_execle_seed_account( bam_fuzz_execle_t *     h,
                              fd_funk_txn_xid_t const * xid,
                              fd_acct_addr_t const *  acct ) {
  fd_accdb_ro_t ro[1];
  if( FD_LIKELY( fd_accdb_open_ro( h->mini->accdb, ro, xid, acct ) ) ) {
    fd_accdb_close_ro( h->mini->accdb, ro );
    return;
  }

  fd_svm_mini_add_lamports( h->mini, xid, (fd_pubkey_t const *)acct, BAM_FUZZ_EXECLE_ACCOUNT_LAMPORTS );
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

  fd_funk_txn_xid_t xid = fd_bank_xid( h->bank );
  fd_acct_addr_t const * imm_accts = fd_txn_get_acct_addrs( txn, txnp->payload );
  for( ulong i=0UL; i<(ulong)txn->acct_addr_cnt; i++ ) {
    bam_fuzz_execle_seed_account( h, &xid, &imm_accts[ i ] );
  }

  ulong alt_cnt = fd_txn_account_cnt( txn, FD_TXN_ACCT_CAT_ALT );
  for( ulong i=0UL; i<alt_cnt; i++ ) {
    bam_fuzz_execle_seed_account( h, &xid, &txne->alt_accts[ i ] );
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

bam_fuzz_execle_result_t
bam_fuzz_execle_frag( bam_fuzz_execle_t *    h,
                      fd_frag_meta_t const * meta,
                      ulong                  seq ) {
  bam_fuzz_execle_result_t res = {
    .poh_before      = h->stem_seqs[ BAM_FUZZ_EXECLE_OUT_POH_IDX ],
    .poh_after       = h->stem_seqs[ BAM_FUZZ_EXECLE_OUT_POH_IDX ],
    .bank_bam_before = h->stem_seqs[ BAM_FUZZ_EXECLE_OUT_BANK_BAM_IDX ],
    .bank_bam_after  = h->stem_seqs[ BAM_FUZZ_EXECLE_OUT_BANK_BAM_IDX ],
  };

  int filtered = before_frag( h->ctx, 0UL, seq, meta->sig );
  FD_TEST( !filtered );
  bam_fuzz_execle_seed_microblock( h, meta );
  during_frag( h->ctx, 0UL, seq, meta->sig, meta->chunk, meta->sz, meta->ctl );
  after_frag( h->ctx, 0UL, seq, meta->sig, meta->sz, meta->tsorig, meta->tspub, h->stem );

  res.poh_after      = h->stem_seqs[ BAM_FUZZ_EXECLE_OUT_POH_IDX ];
  res.bank_bam_after = h->stem_seqs[ BAM_FUZZ_EXECLE_OUT_BANK_BAM_IDX ];
  return res;
}
