#define fd_tile_dedup bam_fuzz_dedup_tile_unused
#include "../dedup/fd_dedup_tile.c"
#undef fd_tile_dedup

#include "fuzz_bam_pipeline_stage.h"

#define BAM_FUZZ_DEDUP_MCACHE_DEPTH  (256UL)
#define BAM_FUZZ_DEDUP_TCACHE_DEPTH  (128UL)

struct bam_fuzz_dedup {
  fd_dedup_ctx_t  ctx[1];
  fd_tcache_t *   tcache;
  bam_fuzz_outs_t outs;
};

bam_fuzz_dedup_t *
bam_fuzz_dedup_new( fd_wksp_t *             wksp,
                    bam_fuzz_link_t const * in,
                    ulong                   in_cnt,
                    bam_fuzz_link_t         out[ BAM_FUZZ_OUT_MAX ] ) {
  bam_fuzz_dedup_t * h = fd_wksp_alloc_laddr( wksp, alignof(bam_fuzz_dedup_t), sizeof(bam_fuzz_dedup_t), 1UL );
  FD_TEST( h );
  fd_memset( h, 0, sizeof(*h) );
  fd_dedup_ctx_t * ctx = h->ctx;

  h->tcache = fd_tcache_join( fd_tcache_new( fd_wksp_alloc_laddr( wksp, fd_tcache_align(), fd_tcache_footprint( BAM_FUZZ_DEDUP_TCACHE_DEPTH, 0UL ), 1UL ),
                                             BAM_FUZZ_DEDUP_TCACHE_DEPTH, 0UL ) );
  FD_TEST( h->tcache );

  bam_fuzz_link_t const * o = bam_fuzz_outs_add( &h->outs, 0UL, wksp, BAM_FUZZ_DEDUP_MCACHE_DEPTH, FD_TPU_PARSED_MTU );

  FD_TEST( in_cnt<=sizeof(ctx->in)/sizeof(ctx->in[ 0 ]) );
  for( ulong i=0UL; i<in_cnt; i++ ) {
    ctx->in_kind[ i ] = IN_KIND_VERIFY;
    ctx->in     [ i ] = (fd_dedup_in_ctx_t){ .mem = in[ i ].mem, .chunk0 = in[ i ].chunk0, .wmark = in[ i ].wmark, .mtu = FD_TPU_PARSED_MTU };
  }
  fd_dedup_tile_bam_init( ctx, in_cnt );
  ctx->out_mem    = o->mem;
  ctx->out_chunk0 = o->chunk0;
  ctx->out_wmark  = o->wmark;
  ctx->out_chunk  = o->chunk0;

  ctx->hashmap_seed   = 0xD00DUL;
  ctx->tcache_depth   = fd_tcache_depth       ( h->tcache );
  ctx->tcache_map_cnt = fd_tcache_map_cnt     ( h->tcache );
  ctx->tcache_sync    = fd_tcache_oldest_laddr( h->tcache );
  ctx->tcache_ring    = fd_tcache_ring_laddr  ( h->tcache );
  ctx->tcache_map     = fd_tcache_map_laddr   ( h->tcache );

  fd_memcpy( out, h->outs.link, sizeof(h->outs.link) );
  return h;
}

void
bam_fuzz_dedup_delete( bam_fuzz_dedup_t * h ) {
  fd_wksp_free_laddr( fd_tcache_delete( fd_tcache_leave( h->tcache ) ) );
  bam_fuzz_outs_delete( &h->outs );
  fd_wksp_free_laddr( h );
}

bam_fuzz_res_t
bam_fuzz_dedup_frag( bam_fuzz_dedup_t *     h,
                     ulong                  in_idx,
                     fd_frag_meta_t const * meta,
                     ulong                  seq,
                     int *                  held ) {
  bam_fuzz_res_t res = bam_fuzz_outs_begin( &h->outs );
  int filter = fd_dedup_tile_bam_before_frag( h->ctx, in_idx, seq, meta->sig );
  FD_TEST( filter<=0 );
  *held = filter<0;
  if( *held ) return res;
  during_frag( h->ctx, in_idx, seq, meta->sig, meta->chunk, meta->sz, meta->ctl );
  after_frag( h->ctx, in_idx, seq, meta->sig, meta->sz, meta->tsorig, meta->tspub, h->outs.stem );
  return bam_fuzz_outs_end( &h->outs, res );
}
