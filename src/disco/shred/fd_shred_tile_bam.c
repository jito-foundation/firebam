/* FireBAM part of the shred tile: forwarding shreds to the BAM node's
   shred receivers.  Included once by fd_shred_tile.c after
   fd_shred_ctx_t; send_shred is defined later in that file. */

static inline void
send_shred( fd_shred_ctx_t                 * ctx,
            fd_stem_context_t              * stem,
            fd_shred_t const               * shred,
            fd_shred_dest_weighted_t const * dest,
            ulong                            tsorig );

/* fd_shred_bam_update_during_frag copies in a bam_shred update; a
   malformed one is skipped. */
static inline void
fd_shred_bam_update_during_frag( fd_shred_ctx_t * ctx,
                                 ulong            in_idx,
                                 ulong            chunk,
                                 ulong            sz ) {
  if( FD_UNLIKELY( chunk<ctx->in[ in_idx ].chunk0 || chunk>ctx->in[ in_idx ].wmark || sz!=sizeof(fd_bam_shred_update_t) ) ) {
    FD_LOG_WARNING(( "Malformed BAM shred update chunk=%lu sz=%lu range=[%lu,%lu]",
                     chunk, sz, ctx->in[ in_idx ].chunk0, ctx->in[ in_idx ].wmark ));
    ctx->skip_frag = 1;
    return;
  }
  fd_memcpy( ctx->bam_shred_upd_buf, fd_chunk_to_laddr_const( ctx->in[ in_idx ].mem, chunk ), sizeof(fd_bam_shred_update_t) );
  if( FD_UNLIKELY( ctx->bam_shred_upd_buf->shred_sock_cnt>FD_BAM_SHRED_SOCK_MAX ) ) {
    FD_LOG_WARNING(( "Malformed BAM shred update receiver count=%u max=%lu",
                     (uint)ctx->bam_shred_upd_buf->shred_sock_cnt, FD_BAM_SHRED_SOCK_MAX ));
    ctx->skip_frag = 1;
  }
}

/* fd_shred_bam_update_after_frag installs the BAM shred receivers. */
static inline void
fd_shred_bam_update_after_frag( fd_shred_ctx_t * ctx ) {
  ctx->bam_dests_cnt = ctx->bam_shred_upd_buf->shred_sock_cnt;
  for( ulong i=0UL; i<ctx->bam_dests_cnt; i++ ) {
    ctx->bam_dests[ i ].ip4  = ctx->bam_shred_upd_buf->shred_sock[ i ].addr;
    ctx->bam_dests[ i ].port = fd_ushort_bswap( ctx->bam_shred_upd_buf->shred_sock[ i ].port );
  }
}

/* fd_shred_send_bam_shred forwards a shred to the BAM shred receivers if
   this validator leads its slot (its own shreds) or a leader rotation
   starting within the next four slots (retransmitted shreds).

   ctx->bam_leader_soon* memoize "is this validator leader in
   shred->slot+1..+4" for the retransmit check.  The answer only changes
   once per slot, but the check sits in the per-shred retransmit path, so
   recomputing it for every shred would cost four leader-schedule lookups
   each.  bam_leader_soon_slot is the shred->slot the cached answer
   applies to, or ULONG_MAX when no answer is cached. */
static inline void
fd_shred_send_bam_shred( fd_shred_ctx_t *    ctx,
                         fd_stem_context_t * stem,
                         fd_shred_t const *  shred,
                         int                 is_retransmit,
                         ulong               tsorig ) {
  if( FD_UNLIKELY( !ctx->bam_dests_cnt ) ) return;

  int should_send = 0;
  if( is_retransmit ) {
    /* Stop forwarding older retransmits once local leader shredding starts. */
    if( FD_UNLIKELY( ctx->slot!=ULONG_MAX && shred->slot<ctx->slot ) ) return;

    if( FD_LIKELY( shred->slot==ctx->bam_leader_soon_slot ) ) {
      should_send = ctx->bam_leader_soon;
    } else {
      for( ulong off=1UL; off<5UL; off++ ) {
        ulong slot = shred->slot + off;
        fd_epoch_leaders_t const * lsched = fd_stake_ci_get_lsched_for_slot( ctx->stake_ci, slot );
        if( FD_LIKELY( !lsched || (slot-lsched->slot0)%FD_EPOCH_SLOTS_PER_ROTATION ) ) continue;
        fd_pubkey_t const * leader = fd_epoch_leaders_get( lsched, slot );
        if( FD_LIKELY( !leader || !fd_memeq( leader, ctx->identity_key, sizeof(fd_pubkey_t) ) ) ) continue;
        should_send = 1;
        break;
      }
      ctx->bam_leader_soon_slot = shred->slot;
      ctx->bam_leader_soon      = should_send;
    }
  } else {
    fd_epoch_leaders_t const * lsched = fd_stake_ci_get_lsched_for_slot( ctx->stake_ci, shred->slot );
    if( FD_UNLIKELY( !lsched ) ) return;
    fd_pubkey_t const * leader = fd_epoch_leaders_get( lsched, shred->slot );
    should_send = !!( leader && fd_memeq( leader, ctx->identity_key, sizeof(fd_pubkey_t) ) );
  }

  if( FD_UNLIKELY( !should_send ) ) return;

  for( ulong i=0UL; i<ctx->bam_dests_cnt; i++ ) send_shred( ctx, stem, shred, ctx->bam_dests + i, tsorig );
}
