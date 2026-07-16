#define fd_tile_resolv bam_fuzz_resolv_tile_unused
#include "../../discof/resolv/fd_resolv_tile.c"
#undef fd_tile_resolv

#include "fuzz_bam_pipeline_stage_resolv.h"

static void (* const bam_fuzz_resolv_init_refs[])( fd_topo_t const *, fd_topo_tile_t const * ) __attribute__((unused)) = {
  unprivileged_init,
};

static ulong (* const bam_fuzz_resolv_policy_refs[])( fd_topo_t const *, fd_topo_tile_t const *, ulong, struct sock_filter * ) __attribute__((unused)) = {
  populate_allowed_seccomp,
};

static ulong (* const bam_fuzz_resolv_fds_refs[])( fd_topo_t const *, fd_topo_tile_t const *, ulong, int * ) __attribute__((unused)) = {
  populate_allowed_fds,
};

#define BAM_FUZZ_RESOLV_IN_DEDUP_IDX     (0UL)
#define BAM_FUZZ_RESOLV_OUT_PACK_IDX     (0UL)
#define BAM_FUZZ_RESOLV_OUT_BANK_BAM_IDX (1UL)
#define BAM_FUZZ_RESOLV_OUT_CNT          (2UL)
#define BAM_FUZZ_RESOLV_MCACHE_DEPTH     (256UL)
#define BAM_FUZZ_RESOLV_POOL_CNT         (256UL)
#define BAM_FUZZ_RESOLV_MAP_CHAIN_CNT    (512UL)

struct bam_fuzz_resolv {
  fd_wksp_t * wksp;

  fd_resolv_ctx_t * ctx;

  void * storage_mem;

  void * pool_mem;
  void * map_chain_mem;
  void * map_mem;

  void *           pack_mcache_mem;
  fd_frag_meta_t * pack_mcache;
  void *           pack_dcache_mem;
  uchar *          pack_dcache;

  void *           bank_bam_mcache_mem;
  fd_frag_meta_t * bank_bam_mcache;
  void *           bank_bam_dcache_mem;
  uchar *          bank_bam_dcache;

  fd_stem_context_t stem[1];
  fd_frag_meta_t *  stem_mcaches[ BAM_FUZZ_RESOLV_OUT_CNT ];
  ulong             stem_seqs[ BAM_FUZZ_RESOLV_OUT_CNT ];
  ulong             stem_depths[ BAM_FUZZ_RESOLV_OUT_CNT ];
  ulong             stem_cr_avail[ BAM_FUZZ_RESOLV_OUT_CNT ];
  ulong             stem_min_cr_avail[1];
  int               stem_out_reliable[ BAM_FUZZ_RESOLV_OUT_CNT ];
};

bam_fuzz_resolv_t *
bam_fuzz_resolv_new( fd_wksp_t * wksp,
                     fd_wksp_t * in_mem,
                     ulong       in_chunk0,
                     ulong       in_wmark,
                     bam_fuzz_link_t * pack_out,
                     bam_fuzz_link_t * bank_bam_out ) {
  bam_fuzz_resolv_t * h = fd_wksp_alloc_laddr( wksp, alignof(bam_fuzz_resolv_t), sizeof(bam_fuzz_resolv_t), 1UL );
  FD_TEST( h );
  fd_memset( h, 0, sizeof(*h) );
  h->wksp = wksp;

  ulong l = FD_LAYOUT_INIT;
  l = FD_LAYOUT_APPEND( l, alignof(fd_resolv_ctx_t), sizeof(fd_resolv_ctx_t) );
  l = FD_LAYOUT_APPEND( l, pool_align(),             pool_footprint( BAM_FUZZ_RESOLV_POOL_CNT ) );
  l = FD_LAYOUT_APPEND( l, map_chain_align(),        map_chain_footprint( BAM_FUZZ_RESOLV_MAP_CHAIN_CNT ) );
  l = FD_LAYOUT_APPEND( l, map_align(),              map_footprint() );
  l = FD_LAYOUT_APPEND( l, fd_mcache_align(),        fd_mcache_footprint( BAM_FUZZ_RESOLV_MCACHE_DEPTH, 0UL ) );
  l = FD_LAYOUT_APPEND( l, fd_dcache_align(),        fd_dcache_footprint( fd_dcache_req_data_sz( FD_TPU_RESOLVED_MTU, BAM_FUZZ_RESOLV_MCACHE_DEPTH, 1UL, 1 ), 0UL ) );
  l = FD_LAYOUT_APPEND( l, fd_mcache_align(),        fd_mcache_footprint( BAM_FUZZ_RESOLV_MCACHE_DEPTH, 0UL ) );
  l = FD_LAYOUT_APPEND( l, fd_dcache_align(),        fd_dcache_footprint( fd_dcache_req_data_sz( sizeof(fd_bam_bundle_result_t), BAM_FUZZ_RESOLV_MCACHE_DEPTH, 1UL, 1 ), 0UL ) );
  ulong storage_footprint = FD_LAYOUT_FINI( l, fd_dcache_align() );
  h->storage_mem = fd_wksp_alloc_laddr( wksp, fd_dcache_align(), storage_footprint, 1UL );
  FD_TEST( h->storage_mem );

  FD_SCRATCH_ALLOC_INIT( alloc, h->storage_mem );
  ulong pack_data_sz     = fd_dcache_req_data_sz( FD_TPU_RESOLVED_MTU, BAM_FUZZ_RESOLV_MCACHE_DEPTH, 1UL, 1 );
  ulong bank_bam_data_sz = fd_dcache_req_data_sz( sizeof(fd_bam_bundle_result_t), BAM_FUZZ_RESOLV_MCACHE_DEPTH, 1UL, 1 );

  h->ctx = FD_SCRATCH_ALLOC_APPEND( alloc, alignof(fd_resolv_ctx_t), sizeof(fd_resolv_ctx_t) );
  FD_TEST( h->ctx );
  fd_memset( h->ctx, 0, sizeof(fd_resolv_ctx_t) );

  h->pool_mem = FD_SCRATCH_ALLOC_APPEND( alloc, pool_align(), pool_footprint( BAM_FUZZ_RESOLV_POOL_CNT ) );
  h->ctx->pool = pool_join( pool_new( h->pool_mem, BAM_FUZZ_RESOLV_POOL_CNT ) );
  FD_TEST( h->ctx->pool );

  h->map_chain_mem = FD_SCRATCH_ALLOC_APPEND( alloc, map_chain_align(), map_chain_footprint( BAM_FUZZ_RESOLV_MAP_CHAIN_CNT ) );
  h->ctx->map_chain = map_chain_join( map_chain_new( h->map_chain_mem, BAM_FUZZ_RESOLV_MAP_CHAIN_CNT, 0UL ) );
  FD_TEST( h->ctx->map_chain );

  h->map_mem = FD_SCRATCH_ALLOC_APPEND( alloc, map_align(), map_footprint() );
  h->ctx->blockhash_map = map_join( map_new( h->map_mem ) );
  FD_TEST( h->ctx->blockhash_map );

  FD_TEST( h->ctx->lru_list==lru_list_join( lru_list_new( h->ctx->lru_list ) ) );

  h->pack_mcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_mcache_align(), fd_mcache_footprint( BAM_FUZZ_RESOLV_MCACHE_DEPTH, 0UL ) );
  h->pack_mcache = fd_mcache_join( fd_mcache_new( h->pack_mcache_mem, BAM_FUZZ_RESOLV_MCACHE_DEPTH, 0UL, 0UL ) );
  FD_TEST( h->pack_mcache );
  h->pack_dcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_dcache_align(), fd_dcache_footprint( pack_data_sz, 0UL ) );
  h->pack_dcache = fd_dcache_join( fd_dcache_new( h->pack_dcache_mem, pack_data_sz, 0UL ) );
  FD_TEST( h->pack_dcache );

  h->bank_bam_mcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_mcache_align(), fd_mcache_footprint( BAM_FUZZ_RESOLV_MCACHE_DEPTH, 0UL ) );
  h->bank_bam_mcache = fd_mcache_join( fd_mcache_new( h->bank_bam_mcache_mem, BAM_FUZZ_RESOLV_MCACHE_DEPTH, 0UL, 0UL ) );
  FD_TEST( h->bank_bam_mcache );
  h->bank_bam_dcache_mem = FD_SCRATCH_ALLOC_APPEND( alloc, fd_dcache_align(), fd_dcache_footprint( bank_bam_data_sz, 0UL ) );
  h->bank_bam_dcache = fd_dcache_join( fd_dcache_new( h->bank_bam_dcache_mem, bank_bam_data_sz, 0UL ) );
  FD_TEST( h->bank_bam_dcache );

  ulong storage_top = FD_SCRATCH_ALLOC_FINI( alloc, fd_dcache_align() );
  FD_TEST( storage_top <= (ulong)h->storage_mem + storage_footprint );

  h->stem_mcaches[ BAM_FUZZ_RESOLV_OUT_PACK_IDX     ] = h->pack_mcache;
  h->stem_mcaches[ BAM_FUZZ_RESOLV_OUT_BANK_BAM_IDX ] = h->bank_bam_mcache;
  for( ulong i=0UL; i<BAM_FUZZ_RESOLV_OUT_CNT; i++ ) {
    h->stem_depths[ i ] = BAM_FUZZ_RESOLV_MCACHE_DEPTH;
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

  h->ctx->round_robin_cnt = 1UL;
  h->ctx->round_robin_idx = 0UL;
  h->ctx->flush_pool_idx = ULONG_MAX;
  h->ctx->completed_slot = 0UL;
  h->ctx->in[ BAM_FUZZ_RESOLV_IN_DEDUP_IDX ] = (fd_resolv_in_ctx_t) {
    .kind   = IN_KIND_DEDUP,
    .mem    = in_mem,
    .chunk0 = in_chunk0,
    .wmark  = in_wmark,
    .mtu    = FD_TPU_PARSED_MTU,
  };
  h->ctx->out_pack->idx    = BAM_FUZZ_RESOLV_OUT_PACK_IDX;
  h->ctx->out_pack->mem    = (fd_wksp_t *)h->pack_dcache;
  h->ctx->out_pack->chunk0 = fd_dcache_compact_chunk0( h->pack_dcache, h->pack_dcache );
  h->ctx->out_pack->wmark  = fd_dcache_compact_wmark ( h->pack_dcache, h->pack_dcache, FD_TPU_RESOLVED_MTU );
  h->ctx->out_pack->chunk  = h->ctx->out_pack->chunk0;

  *h->ctx->out_replay = (fd_resolv_out_ctx_t){ .idx = ULONG_MAX };

  h->ctx->out_bam->idx    = BAM_FUZZ_RESOLV_OUT_BANK_BAM_IDX;
  h->ctx->out_bam->mem    = (fd_wksp_t *)h->bank_bam_dcache;
  h->ctx->out_bam->chunk0 = fd_dcache_compact_chunk0( h->bank_bam_dcache, h->bank_bam_dcache );
  h->ctx->out_bam->wmark  = fd_dcache_compact_wmark ( h->bank_bam_dcache, h->bank_bam_dcache, sizeof(fd_bam_bundle_result_t) );
  h->ctx->out_bam->chunk  = h->ctx->out_bam->chunk0;

  if( FD_LIKELY( pack_out ) ) {
    *pack_out = (bam_fuzz_link_t) {
      .mem    = (fd_wksp_t *)h->pack_dcache,
      .mcache = h->pack_mcache,
      .chunk0 = h->ctx->out_pack->chunk0,
      .wmark  = h->ctx->out_pack->wmark,
      .depth  = BAM_FUZZ_RESOLV_MCACHE_DEPTH,
    };
  }
  if( FD_LIKELY( bank_bam_out ) ) {
    *bank_bam_out = (bam_fuzz_link_t) {
      .mem    = (fd_wksp_t *)h->bank_bam_dcache,
      .mcache = h->bank_bam_mcache,
      .chunk0 = h->ctx->out_bam->chunk0,
      .wmark  = h->ctx->out_bam->wmark,
      .depth  = BAM_FUZZ_RESOLV_MCACHE_DEPTH,
    };
  }

  return h;
}

void
bam_fuzz_resolv_delete( bam_fuzz_resolv_t * h ) {
  if( FD_UNLIKELY( !h ) ) return;

  bam_fuzz_delete_mcache( &h->pack_mcache,     h->pack_mcache_mem     );
  bam_fuzz_delete_dcache( &h->pack_dcache,     h->pack_dcache_mem     );
  bam_fuzz_delete_mcache( &h->bank_bam_mcache, h->bank_bam_mcache_mem );
  bam_fuzz_delete_dcache( &h->bank_bam_dcache, h->bank_bam_dcache_mem );

  if( h->storage_mem ) {
    fd_wksp_free_laddr( h->storage_mem );
    h->storage_mem = NULL;
  }
  fd_wksp_free_laddr( h );
}

void
bam_fuzz_resolv_prepare_slot( bam_fuzz_resolv_t * h,
                              ulong               slot,
                              fd_banks_t *        banks,
                              fd_accdb_t *        accdb,
                              fd_bank_t *         bank ) {
  h->ctx->banks = banks;
  h->ctx->bank  = bank;
  if( FD_LIKELY( accdb ) ) h->ctx->accdb = accdb;
  if( FD_LIKELY( slot!=ULONG_MAX ) ) h->ctx->completed_slot = slot;
}

static void
bam_fuzz_resolv_seed_blockhash( bam_fuzz_resolv_t * h,
                                fd_frag_meta_t const * meta ) {
  fd_txn_m_t const * txnm = (fd_txn_m_t const *)fd_chunk_to_laddr_const( h->ctx->in[ BAM_FUZZ_RESOLV_IN_DEDUP_IDX ].mem, meta->chunk );
  if( FD_UNLIKELY( txnm->txn_t_sz==0U ) ) return;

  fd_txn_t const * txnt = fd_txn_m_txn_t_const( txnm );
  blockhash_t const * hash = (blockhash_t const *)( fd_txn_m_payload_const( txnm ) + txnt->recent_blockhash_off );
  if( FD_UNLIKELY( map_key_inval( *hash ) ) ) return;

  blockhash_map_t * entry = map_query( h->ctx->blockhash_map, *hash, NULL );
  if( FD_UNLIKELY( !entry ) ) entry = map_insert( h->ctx->blockhash_map, *hash );
  entry->slot = h->ctx->completed_slot;
}

bam_fuzz_resolv_result_t
bam_fuzz_resolv_frag( bam_fuzz_resolv_t * h,
                      fd_frag_meta_t const * meta,
                      ulong seq ) {
  bam_fuzz_resolv_result_t res = {
    .pack_before     = h->stem_seqs[ BAM_FUZZ_RESOLV_OUT_PACK_IDX ],
    .pack_after      = h->stem_seqs[ BAM_FUZZ_RESOLV_OUT_PACK_IDX ],
    .bank_bam_before = h->stem_seqs[ BAM_FUZZ_RESOLV_OUT_BANK_BAM_IDX ],
    .bank_bam_after  = h->stem_seqs[ BAM_FUZZ_RESOLV_OUT_BANK_BAM_IDX ],
  };

  int filtered = before_frag( h->ctx, BAM_FUZZ_RESOLV_IN_DEDUP_IDX, seq, meta->sig );
  FD_TEST( !filtered );
  bam_fuzz_resolv_seed_blockhash( h, meta );
  during_frag( h->ctx, BAM_FUZZ_RESOLV_IN_DEDUP_IDX, seq, meta->sig, meta->chunk, meta->sz, meta->ctl );
  after_frag( h->ctx, BAM_FUZZ_RESOLV_IN_DEDUP_IDX, seq, meta->sig, meta->sz, meta->tsorig, meta->tspub, h->stem );

  res.pack_after     = h->stem_seqs[ BAM_FUZZ_RESOLV_OUT_PACK_IDX ];
  res.bank_bam_after = h->stem_seqs[ BAM_FUZZ_RESOLV_OUT_BANK_BAM_IDX ];
  return res;
}

bam_fuzz_resolv_result_t
bam_fuzz_resolv_credit( bam_fuzz_resolv_t * h ) {
  bam_fuzz_resolv_result_t res = {
    .pack_before     = h->stem_seqs[ BAM_FUZZ_RESOLV_OUT_PACK_IDX ],
    .pack_after      = h->stem_seqs[ BAM_FUZZ_RESOLV_OUT_PACK_IDX ],
    .bank_bam_before = h->stem_seqs[ BAM_FUZZ_RESOLV_OUT_BANK_BAM_IDX ],
    .bank_bam_after  = h->stem_seqs[ BAM_FUZZ_RESOLV_OUT_BANK_BAM_IDX ],
  };
  int opt_poll_in = 1;
  int charge_busy = 0;
  after_credit( h->ctx, h->stem, &opt_poll_in, &charge_busy );
  res.pack_after     = h->stem_seqs[ BAM_FUZZ_RESOLV_OUT_PACK_IDX ];
  res.bank_bam_after = h->stem_seqs[ BAM_FUZZ_RESOLV_OUT_BANK_BAM_IDX ];
  return res;
}
