#include "fd_verify_tile.h"
#include "../fd_txn_m.h"
#include "../bam/fd_bam_types.h"
#include "../metrics/fd_metrics.h"
#include "generated/fd_verify_tile_seccomp.h"
#include "../../flamenco/gossip/fd_gossip_message.h"

#define IN_KIND_QUIC   (0UL)
#define IN_KIND_BUNDLE (1UL)
#define IN_KIND_GOSSIP (2UL)
#define IN_KIND_TXSEND (3UL)
#define IN_KIND_BAM    (4UL)

FD_FN_CONST static inline ulong
scratch_align( void ) {
  return FD_TCACHE_ALIGN;
}

FD_FN_PURE static inline ulong
scratch_footprint( fd_topo_tile_t const * tile ) {
  ulong l = FD_LAYOUT_INIT;
  l = FD_LAYOUT_APPEND( l, alignof( fd_verify_ctx_t ), sizeof( fd_verify_ctx_t ) );
  l = FD_LAYOUT_APPEND( l, fd_tcache_align(), fd_tcache_footprint( tile->verify.tcache_depth, 0UL ) );
  for( ulong i=0; i<FD_TXN_ACTUAL_SIG_MAX; i++ ) {
    l = FD_LAYOUT_APPEND( l, fd_sha512_align(), fd_sha512_footprint() );
  }
  return FD_LAYOUT_FINI( l, scratch_align() );
}

static inline void
metrics_write( fd_verify_ctx_t * ctx ) {
  FD_MCNT_ENUM_COPY( VERIFY, TRANSACTION_RESULT, ctx->metrics.verify_tile_result );
  FD_MCNT_SET( VERIFY, GOSSIPED_VOTES_RECEIVED,  ctx->metrics.gossiped_votes_cnt );
}

static int
before_frag( fd_verify_ctx_t * ctx,
             ulong             in_idx,
             ulong             seq,
             ulong             sig ) {
  /* Bundle tile can produce both "bundles" and "packets", a packet is a
     regular transaction and should be round-robined between verify
     tiles, while bundles need to go through verify:0 currently to
     prevent interleaving of bundle streams. */
  int is_bundle_packet = ctx->in_kind[ in_idx ]==IN_KIND_BUNDLE && !sig;

  if( FD_LIKELY( is_bundle_packet || ctx->in_kind[ in_idx ]==IN_KIND_QUIC ) ) {
    return (seq % ctx->round_robin_cnt) != ctx->round_robin_idx;
  } else if( FD_LIKELY( ctx->in_kind[ in_idx ]==IN_KIND_BUNDLE || ctx->in_kind[ in_idx ]==IN_KIND_BAM ) ) {
    return ctx->round_robin_idx!=0UL;
  } else if( FD_LIKELY( ctx->in_kind[ in_idx ]==IN_KIND_GOSSIP ) ) {
      return (seq % ctx->round_robin_cnt) != ctx->round_robin_idx ||
             sig!=FD_GOSSIP_UPDATE_TAG_VOTE;
  }

  return 0;
}

/* during_frag is called between pairs for sequence number checks, as
   we are reading incoming frags.  We don't actually need to copy the
   fragment here, see fd_dedup.c for why we do this.*/

static inline void
during_frag( fd_verify_ctx_t * ctx,
             ulong             in_idx,
             ulong             seq FD_PARAM_UNUSED,
             ulong             sig FD_PARAM_UNUSED,
             ulong             chunk,
             ulong             sz,
             ulong             ctl FD_PARAM_UNUSED ) {

  ulong in_kind = ctx->in_kind[ in_idx ];
  if( FD_UNLIKELY( in_kind==IN_KIND_BUNDLE || in_kind==IN_KIND_BAM || in_kind==IN_KIND_QUIC || in_kind==IN_KIND_TXSEND ) ) {
    if( FD_UNLIKELY( chunk<ctx->in[in_idx].chunk0 || chunk>ctx->in[in_idx].wmark || sz>FD_TPU_RAW_MTU ) )
      FD_LOG_ERR(( "chunk %lu %lu corrupt, not in range [%lu,%lu,%lu]", chunk, sz, ctx->in[in_idx].chunk0, ctx->in[in_idx].wmark, FD_TPU_RAW_MTU ));

    uchar * src = fd_chunk_to_laddr( ctx->in[in_idx].mem, chunk );
    uchar * dst = fd_chunk_to_laddr( ctx->out_mem, ctx->out_chunk );
    fd_memcpy( dst, src, sz );

    fd_txn_m_t const * txnm = (fd_txn_m_t const *)dst;
    if( FD_UNLIKELY( txnm->payload_sz>FD_TPU_MTU ) ) {
      FD_LOG_ERR(( "fd_verify: txn payload size %hu exceeds max %lu", txnm->payload_sz, FD_TPU_MTU ));
    }
  } else if( FD_UNLIKELY( ctx->in_kind[ in_idx ]==IN_KIND_GOSSIP ) ) {
    if( FD_UNLIKELY( chunk<ctx->in[in_idx].chunk0 || chunk>ctx->in[in_idx].wmark || sz>2048UL ) )
      FD_LOG_ERR(( "chunk %lu %lu corrupt, not in range [%lu,%lu]", chunk, sz, ctx->in[in_idx].chunk0, ctx->in[in_idx].wmark ));

    fd_gossip_update_message_t const * msg = fd_chunk_to_laddr_const( ctx->in[in_idx].mem, chunk );
    fd_txn_m_t * dst = fd_chunk_to_laddr( ctx->out_mem, ctx->out_chunk );

    dst->payload_sz = (ushort)msg->vote->value->transaction_len;
    dst->block_engine.bundle_id = 0UL;
    dst->source_ipv4 = msg->vote->socket->is_ipv6 ? 0U : msg->vote->socket->ip4;
    dst->source_tpu = FD_TXN_M_TPU_SOURCE_GOSSIP;
    fd_memcpy( fd_txn_m_payload( dst ), msg->vote->value->transaction, msg->vote->value->transaction_len );
  }
}

static inline void
after_frag( fd_verify_ctx_t *   ctx,
            ulong               in_idx,
            ulong               seq,
            ulong               sig,
            ulong               sz,
            ulong               tsorig,
            ulong               _tspub,
            fd_stem_context_t * stem ) {
  (void)in_idx;
  (void)seq;
  (void)sig;
  (void)sz;
  (void)_tspub;

  if( FD_UNLIKELY( ctx->in_kind[ in_idx ]==IN_KIND_GOSSIP || ctx->in_kind[ in_idx ]==IN_KIND_TXSEND ) ) ctx->metrics.gossiped_votes_cnt++;

  fd_txn_m_t * txnm = (fd_txn_m_t *)fd_chunk_to_laddr( ctx->out_mem, ctx->out_chunk );
  fd_txn_t *  txnt = fd_txn_m_txn_t( txnm );
  int is_bam    = txnm->source_tpu==FD_TXN_M_TPU_SOURCE_BAM;
  int is_bundle = !!txnm->block_engine.bundle_id;

  txnm->txn_t_sz = (ushort)fd_txn_parse( fd_txn_m_payload( txnm ), txnm->payload_sz, txnt, NULL );

  if( FD_UNLIKELY( is_bundle & (txnm->block_engine.bundle_id!=ctx->bundle_id) ) ) {
    ctx->bundle_failed = 0;
    ctx->bundle_id     = txnm->block_engine.bundle_id;
  }

  if( FD_UNLIKELY( is_bundle & (!!ctx->bundle_failed) ) ) {
    ctx->metrics.verify_tile_result[ FD_METRICS_ENUM_VERIFY_TILE_RESULT_V_BUNDLE_PEER_FAILURE_IDX ]++;
    return;
  }

  ulong failure_idx = ULONG_MAX;
  if( FD_UNLIKELY( !txnm->txn_t_sz ) ) {
    failure_idx = FD_METRICS_ENUM_VERIFY_TILE_RESULT_V_PARSE_FAILURE_IDX;
  } else {
    /* Users sometimes send transactions as part of a bundle (with a tip)
       and via the normal path (without a tip).  Regardless of which
       arrives first, we want to pack the one with the tip.  Thus, we
       exempt bundles from the normal HA dedup checks.  Likewise, BAM
       traffic is already sequenced by the BAM node and may legitimately
       resend signatures, so it bypasses early signature dedup entirely. */
    int do_sig_dedup = fd_txn_m_use_prepack_sig_dedup( txnm );
    ulong _txn_sig;
    int res = fd_txn_verify( ctx, fd_txn_m_payload( txnm ), txnm->payload_sz, txnt, do_sig_dedup, &_txn_sig );
    if( FD_UNLIKELY( res!=FD_TXN_VERIFY_SUCCESS ) )
      failure_idx = fd_ulong_if( res==FD_TXN_VERIFY_DEDUP,
                                 FD_METRICS_ENUM_VERIFY_TILE_RESULT_V_DEDUP_FAILURE_IDX,
                                 FD_METRICS_ENUM_VERIFY_TILE_RESULT_V_VERIFY_FAILURE_IDX );
  }

  if( FD_UNLIKELY( failure_idx!=ULONG_MAX ) ) {
    if( FD_UNLIKELY( is_bundle ) ) ctx->bundle_failed = 1;
    if( FD_UNLIKELY( is_bam & (ctx->bam_result_out_idx!=ULONG_MAX) ) ) {
      fd_bam_bundle_result_t * bam_res = fd_chunk_to_laddr( ctx->bam_result_out_mem, ctx->bam_result_out_chunk );
      *bam_res = (fd_bam_bundle_result_t) {
        .seq_id           = txnm->bam.seq_id,
        .slot             = txnm->bam.max_schedule_slot,
        .bundle_txn_cnt   = txnm->bam.txn_cnt,
        .scheduling_error = FD_BAM_SCHED_ERR_NONE,
        .bundle_err       = FD_BAM_BUNDLE_ERR_DESER,
        .deser_index      = txnm->bam.batch_idx,
        .deser_reason     = bam_types_DeserializationErrorReason_SANITIZE_ERROR
      };
      fd_stem_publish( stem, ctx->bam_result_out_idx, 0UL, ctx->bam_result_out_chunk, sizeof(fd_bam_bundle_result_t), 0UL, 0UL, fd_frag_meta_ts_comp( fd_tickcount() ) );
      ctx->bam_result_out_chunk = fd_dcache_compact_next( ctx->bam_result_out_chunk, sizeof(fd_bam_bundle_result_t), ctx->bam_result_out_chunk0, ctx->bam_result_out_wmark );
    }
    ctx->metrics.verify_tile_result[ failure_idx ]++;
    return;
  }

  ulong realized_sz = fd_txn_m_realized_footprint( txnm, 1, 0 );
  ulong tspub = (ulong)fd_frag_meta_ts_comp( fd_tickcount() );
  fd_stem_publish( stem, 0UL, 0UL, ctx->out_chunk, realized_sz, 0UL, tsorig, tspub );
  ctx->out_chunk = fd_dcache_compact_next( ctx->out_chunk, realized_sz, ctx->out_chunk0, ctx->out_wmark );

  ctx->metrics.verify_tile_result[ FD_METRICS_ENUM_VERIFY_TILE_RESULT_V_SUCCESS_IDX ]++;
}

static void
privileged_init( fd_topo_t *      topo,
                 fd_topo_tile_t * tile ) {
  void * scratch = fd_topo_obj_laddr( topo, tile->tile_obj_id );

  FD_SCRATCH_ALLOC_INIT( l, scratch );
  fd_verify_ctx_t * ctx = FD_SCRATCH_ALLOC_APPEND( l, alignof( fd_verify_ctx_t ), sizeof( fd_verify_ctx_t ) );
  FD_TEST( fd_rng_secure( &ctx->hashmap_seed, 8U ) );
}

static void
unprivileged_init( fd_topo_t *      topo,
                   fd_topo_tile_t * tile ) {
  void * scratch = fd_topo_obj_laddr( topo, tile->tile_obj_id );

  FD_SCRATCH_ALLOC_INIT( l, scratch );
  fd_verify_ctx_t * ctx = FD_SCRATCH_ALLOC_APPEND( l, alignof( fd_verify_ctx_t ), sizeof( fd_verify_ctx_t ) );
  fd_tcache_t * tcache = fd_tcache_join( fd_tcache_new( FD_SCRATCH_ALLOC_APPEND( l, FD_TCACHE_ALIGN, FD_TCACHE_FOOTPRINT( tile->verify.tcache_depth, 0UL ) ), tile->verify.tcache_depth, 0UL ) );
  if( FD_UNLIKELY( !tcache ) ) FD_LOG_ERR(( "fd_tcache_join failed" ));

  ctx->round_robin_cnt = fd_topo_tile_name_cnt( topo, tile->name );
  ctx->round_robin_idx = tile->kind_id;

  for ( ulong i=0; i<FD_TXN_ACTUAL_SIG_MAX; i++ ) {
    fd_sha512_t * sha = fd_sha512_join( fd_sha512_new( FD_SCRATCH_ALLOC_APPEND( l, alignof( fd_sha512_t ), sizeof( fd_sha512_t ) ) ) );
    if( FD_UNLIKELY( !sha ) ) FD_LOG_ERR(( "fd_sha512_join failed" ));
    ctx->sha[i] = sha;
  }

  ctx->bundle_failed = 0;
  ctx->bundle_id     = 0UL;

  memset( &ctx->metrics, 0, sizeof( ctx->metrics ) );

  ctx->tcache_depth   = fd_tcache_depth       ( tcache );
  ctx->tcache_map_cnt = fd_tcache_map_cnt     ( tcache );
  ctx->tcache_sync    = fd_tcache_oldest_laddr( tcache );
  ctx->tcache_ring    = fd_tcache_ring_laddr  ( tcache );
  ctx->tcache_map     = fd_tcache_map_laddr   ( tcache );

  for( ulong i=0UL; i<tile->in_cnt; i++ ) {
    fd_topo_link_t * link = &topo->links[ tile->in_link_id[ i ] ];

    fd_topo_wksp_t * link_wksp = &topo->workspaces[ topo->objs[ link->dcache_obj_id ].wksp_id ];
    ctx->in[i].mem = link_wksp->wksp;
    ctx->in[i].chunk0 = fd_dcache_compact_chunk0( ctx->in[i].mem, link->dcache );
    ctx->in[i].wmark  = fd_dcache_compact_wmark ( ctx->in[i].mem, link->dcache, link->mtu );

    if(      !strcmp( link->name, "quic_verify"  ) ) ctx->in_kind[ i ] = IN_KIND_QUIC;
    else if( !strcmp( link->name, "bundle_verif" ) ) ctx->in_kind[ i ] = IN_KIND_BUNDLE;
    else if( !strcmp( link->name, "txsend_out"   ) ) ctx->in_kind[ i ] = IN_KIND_TXSEND;
    else if( !strcmp( link->name, "bam_verif"    ) ) ctx->in_kind[ i ] = IN_KIND_BAM;
    else if( !strcmp( link->name, "gossip_out"   ) ) ctx->in_kind[ i ] = IN_KIND_GOSSIP;
    else FD_LOG_ERR(( "unexpected link name %s", link->name ));
  }

  ctx->out_mem    = topo->workspaces[ topo->objs[ topo->links[ tile->out_link_id[ 0 ] ].dcache_obj_id ].wksp_id ].wksp;
  ctx->out_chunk0 = fd_dcache_compact_chunk0( ctx->out_mem, topo->links[ tile->out_link_id[ 0 ] ].dcache );
  ctx->out_wmark  = fd_dcache_compact_wmark ( ctx->out_mem, topo->links[ tile->out_link_id[ 0 ] ].dcache, topo->links[ tile->out_link_id[ 0 ] ].mtu );
  ctx->out_chunk  = ctx->out_chunk0;

  ctx->bam_result_out_idx = ULONG_MAX;
  ulong bam_result_out_idx = ULONG_MAX;
  for( ulong i=0UL; i<tile->out_cnt; i++ ) {
    if( !strcmp( topo->links[ tile->out_link_id[ i ] ].name, "bank_bam" ) ) {
      bam_result_out_idx = i;
      break;
    }
  }
  if( FD_LIKELY( bam_result_out_idx!=ULONG_MAX ) ) {
    fd_topo_link_t const * bam_result_out = &topo->links[ tile->out_link_id[ bam_result_out_idx ] ];
    ctx->bam_result_out_idx    = bam_result_out_idx;
    ctx->bam_result_out_mem    = topo->workspaces[ topo->objs[ bam_result_out->dcache_obj_id ].wksp_id ].wksp;
    ctx->bam_result_out_chunk0 = fd_dcache_compact_chunk0( ctx->bam_result_out_mem, bam_result_out->dcache );
    ctx->bam_result_out_wmark  = fd_dcache_compact_wmark ( ctx->bam_result_out_mem, bam_result_out->dcache, bam_result_out->mtu );
    ctx->bam_result_out_chunk  = ctx->bam_result_out_chunk0;
  }

  ulong scratch_top = FD_SCRATCH_ALLOC_FINI( l, 1UL );
  if( FD_UNLIKELY( scratch_top > (ulong)scratch + scratch_footprint( tile ) ) )
    FD_LOG_ERR(( "scratch overflow %lu %lu %lu", scratch_top - (ulong)scratch - scratch_footprint( tile ), scratch_top, (ulong)scratch + scratch_footprint( tile ) ));
}

static ulong
populate_allowed_seccomp( fd_topo_t const *      topo,
                          fd_topo_tile_t const * tile,
                          ulong                  out_cnt,
                          struct sock_filter *   out ) {
  (void)topo;
  (void)tile;

  populate_sock_filter_policy_fd_verify_tile( out_cnt, out, (uint)fd_log_private_logfile_fd() );
  return sock_filter_policy_fd_verify_tile_instr_cnt;
}

static ulong
populate_allowed_fds( fd_topo_t const *      topo,
                      fd_topo_tile_t const * tile,
                      ulong                  out_fds_cnt,
                      int *                  out_fds ) {
  (void)topo;
  (void)tile;

  if( FD_UNLIKELY( out_fds_cnt<2UL ) ) FD_LOG_ERR(( "out_fds_cnt %lu", out_fds_cnt ));

  ulong out_cnt = 0UL;
  out_fds[ out_cnt++ ] = 2; /* stderr */
  if( FD_LIKELY( -1!=fd_log_private_logfile_fd() ) )
    out_fds[ out_cnt++ ] = fd_log_private_logfile_fd(); /* logfile */
  return out_cnt;
}

#define STEM_BURST (1UL)

#define STEM_CALLBACK_CONTEXT_TYPE  fd_verify_ctx_t
#define STEM_CALLBACK_CONTEXT_ALIGN alignof(fd_verify_ctx_t)

#define STEM_CALLBACK_METRICS_WRITE metrics_write
#define STEM_CALLBACK_BEFORE_FRAG   before_frag
#define STEM_CALLBACK_DURING_FRAG   during_frag
#define STEM_CALLBACK_AFTER_FRAG    after_frag

#include "../stem/fd_stem.c"

#ifndef FD_TILE_TEST
fd_topo_run_tile_t fd_tile_verify = {
  .name                     = "verify",
  .populate_allowed_seccomp = populate_allowed_seccomp,
  .populate_allowed_fds     = populate_allowed_fds,
  .scratch_align            = scratch_align,
  .scratch_footprint        = scratch_footprint,
  .privileged_init          = privileged_init,
  .unprivileged_init        = unprivileged_init,
  .run                      = stem_run,
};
#endif
