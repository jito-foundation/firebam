#define fd_tile_resolv bam_fuzz_resolv_tile_unused
#include "../../discof/resolv/fd_resolv_tile.c"
#undef fd_tile_resolv

#include "fuzz_bam_pipeline_stage.h"

#define BAM_FUZZ_RESOLV_IN_DEDUP_IDX  (0UL)
#define BAM_FUZZ_RESOLV_IN_REPLAY_IDX (1UL)
#define BAM_FUZZ_RESOLV_MCACHE_DEPTH  (256UL)
#define BAM_FUZZ_RESOLV_POOL_CNT      (256UL)
#define BAM_FUZZ_RESOLV_MAP_CHAIN_CNT (512UL)

struct bam_fuzz_resolv {
  fd_resolv_ctx_t     ctx[1];
  void *              storage;
  fd_replay_message_t replay_msg[1];
  bam_fuzz_outs_t     outs;
};

bam_fuzz_resolv_t *
bam_fuzz_resolv_new( fd_wksp_t *             wksp,
                     bam_fuzz_link_t const * in,
                     bam_fuzz_link_t         out[ BAM_FUZZ_OUT_MAX ] ) {
  bam_fuzz_resolv_t * h = fd_wksp_alloc_laddr( wksp, alignof(bam_fuzz_resolv_t), sizeof(bam_fuzz_resolv_t), 1UL );
  FD_TEST( h );
  fd_memset( h, 0, sizeof(*h) );
  fd_resolv_ctx_t * ctx = h->ctx;

  ulong map_cnt = 2UL*map_chain_cnt_est( BLOCKHASH_RING_LEN );
  ulong l = FD_LAYOUT_INIT;
  l = FD_LAYOUT_APPEND( l, pool_align(),      pool_footprint( BAM_FUZZ_RESOLV_POOL_CNT ) );
  l = FD_LAYOUT_APPEND( l, map_chain_align(), map_chain_footprint( BAM_FUZZ_RESOLV_MAP_CHAIN_CNT ) );
  l = FD_LAYOUT_APPEND( l, map_align(),       map_footprint( map_cnt ) );
  l = FD_LAYOUT_APPEND( l, map_align(),       map_footprint( map_cnt ) );
  ulong footprint = FD_LAYOUT_FINI( l, 4096UL );
  h->storage = fd_wksp_alloc_laddr( wksp, 4096UL, footprint, 1UL );
  FD_TEST( h->storage );

  FD_SCRATCH_ALLOC_INIT( alloc, h->storage );
  ctx->pool                = pool_join( pool_new( FD_SCRATCH_ALLOC_APPEND( alloc, pool_align(), pool_footprint( BAM_FUZZ_RESOLV_POOL_CNT ) ), BAM_FUZZ_RESOLV_POOL_CNT ) );
  ctx->map_chain           = map_chain_join( map_chain_new( FD_SCRATCH_ALLOC_APPEND( alloc, map_chain_align(), map_chain_footprint( BAM_FUZZ_RESOLV_MAP_CHAIN_CNT ) ),
                                                            BAM_FUZZ_RESOLV_MAP_CHAIN_CNT, 0UL ) );
  ctx->blockhash_map       = map_join( map_new( FD_SCRATCH_ALLOC_APPEND( alloc, map_align(), map_footprint( map_cnt ) ), map_cnt, 0UL ) );
  ctx->nonce_blockhash_map = map_join( map_new( FD_SCRATCH_ALLOC_APPEND( alloc, map_align(), map_footprint( map_cnt ) ), map_cnt, 0UL ) );
  FD_TEST( FD_SCRATCH_ALLOC_FINI( alloc, 4096UL )<=(ulong)h->storage+footprint );
  FD_TEST( ctx->pool && ctx->map_chain && ctx->blockhash_map && ctx->nonce_blockhash_map );
  FD_TEST( ctx->lru_list==lru_list_join( lru_list_new( ctx->lru_list ) ) );

  bam_fuzz_link_t const * o = bam_fuzz_outs_add( &h->outs, 0UL, wksp, BAM_FUZZ_RESOLV_MCACHE_DEPTH, FD_TPU_RESOLVED_MTU );

  /* The standalone harness has no replay tile to gate startup on. */
  ctx->startup_gate->started = 1;
  ctx->round_robin_cnt = 1UL;
  ctx->round_robin_idx = 0UL;
  ctx->flush_pool_idx  = ULONG_MAX;
  ctx->completed_slot  = 0UL;
  ctx->in[ BAM_FUZZ_RESOLV_IN_DEDUP_IDX  ] = (fd_resolv_in_ctx_t){ .kind = IN_KIND_DEDUP, .mem = in->mem, .chunk0 = in->chunk0, .wmark = in->wmark,
                                                                   .mtu = FD_TPU_PARSED_MTU };
  ctx->in[ BAM_FUZZ_RESOLV_IN_REPLAY_IDX ] = (fd_resolv_in_ctx_t){ .kind = IN_KIND_REPLAY, .mem = fd_type_pun( h->replay_msg ),
                                                                   .mtu = sizeof(fd_replay_message_t) };
  ctx->out_pack->mem    = o->mem;
  ctx->out_pack->chunk0 = o->chunk0;
  ctx->out_pack->wmark  = o->wmark;
  ctx->out_pack->chunk  = o->chunk0;

  fd_memcpy( out, h->outs.link, sizeof(h->outs.link) );
  return h;
}

void
bam_fuzz_resolv_delete( bam_fuzz_resolv_t * h ) {
  bam_fuzz_outs_delete( &h->outs );
  fd_wksp_free_laddr( h->storage );
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
  if( FD_LIKELY( slot!=ULONG_MAX ) ) {
    h->ctx->completed_slot = slot;
    h->ctx->completed_block_height = bank ? bank->f.block_height : slot;
  }
}

/* Resolv learns blockhashes only from replay's SLOT_COMPLETED, which
   fills blockhash_ring and the parallel nonce_blockhash_ring.  Deliver
   one through the tile callbacks for each unknown recent blockhash, as
   if its block just completed.  A durable nonce value is a hash of a
   blockhash, so nonce transactions cannot be made known this way. */

static void
bam_fuzz_resolv_seed_blockhash( bam_fuzz_resolv_t *    h,
                                fd_frag_meta_t const * meta ) {
  fd_txn_m_t const * txnm = (fd_txn_m_t const *)fd_chunk_to_laddr_const( h->ctx->in[ BAM_FUZZ_RESOLV_IN_DEDUP_IDX ].mem, meta->chunk );
  if( FD_UNLIKELY( txnm->txn_t_sz==0U || txnm->bam.preprocess_failed ) ) return;

  fd_txn_t const * txnt = fd_txn_m_txn_t_const( txnm );
  if( FD_UNLIKELY( fd_disco_tpu_is_durable_nonce( txnt, fd_txn_m_payload_const( txnm ) ) ) ) return;

  /* The tile ignores (and warns about) a completed slot whose
     blockhash it already holds. */
  blockhash_t const * hash = (blockhash_t const *)( fd_txn_m_payload_const( txnm ) + txnt->recent_blockhash_off );
  if( FD_LIKELY( map_ele_query_const( h->ctx->blockhash_map, hash, NULL, h->ctx->blockhash_ring ) ) ) return;

  fd_replay_slot_completed_t * msg = &h->replay_msg->slot_completed;
  fd_memset( msg, 0, sizeof(fd_replay_slot_completed_t) );
  msg->slot         = h->ctx->completed_slot;
  msg->block_height = h->ctx->completed_block_height;
  fd_memcpy( msg->block_hash.uc, hash->b, 32UL );

  ulong in_idx = BAM_FUZZ_RESOLV_IN_REPLAY_IDX;
  ulong sig    = REPLAY_SIG_SLOT_COMPLETED;
  FD_TEST( !before_frag( h->ctx, in_idx, 0UL, sig ) );
  during_frag( h->ctx, in_idx, 0UL, sig, 0UL, sizeof(fd_replay_slot_completed_t), 0UL );
  after_frag( h->ctx, in_idx, 0UL, sig, sizeof(fd_replay_slot_completed_t), 0UL, 0UL, h->outs.stem );
}

bam_fuzz_res_t
bam_fuzz_resolv_frag( bam_fuzz_resolv_t *    h,
                      fd_frag_meta_t const * meta,
                      ulong                  seq ) {
  bam_fuzz_res_t res = bam_fuzz_outs_begin( &h->outs );
  bam_fuzz_resolv_seed_blockhash( h, meta );
  FD_TEST( !before_frag( h->ctx, BAM_FUZZ_RESOLV_IN_DEDUP_IDX, seq, meta->sig ) );
  during_frag( h->ctx, BAM_FUZZ_RESOLV_IN_DEDUP_IDX, seq, meta->sig, meta->chunk, meta->sz, meta->ctl );
  after_frag( h->ctx, BAM_FUZZ_RESOLV_IN_DEDUP_IDX, seq, meta->sig, meta->sz, meta->tsorig, meta->tspub, h->outs.stem );
  return bam_fuzz_outs_end( &h->outs, res );
}

bam_fuzz_res_t
bam_fuzz_resolv_credit( bam_fuzz_resolv_t * h ) {
  bam_fuzz_res_t res = bam_fuzz_outs_begin( &h->outs );
  int opt_poll_in = 1;
  int charge_busy = 0;
  after_credit( h->ctx, h->outs.stem, &opt_poll_in, &charge_busy );
  return bam_fuzz_outs_end( &h->outs, res );
}
