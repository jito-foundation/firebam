/* FireBAM replay metrics.  BAM does not change Alpenglow leader timing. */

/* Report the adjusted duration of the slot replay last refreshed its
   epoch info from, which Alpenglow ticks use. */

static inline void
replay_bam_metrics_write( fd_replay_tile_t const * ctx ) {
  FD_MGAUGE_SET( REPLAY, SLOT_DURATION_NANOS, fd_ulong_sat_sub( ctx->ns_per_slot, FD_TARGET_SLOT_ADJUSTMENT_NS ) );
}

static void
replay_bam_init( fd_replay_tile_t * ctx,
                 fd_topo_t const *  topo ) {
  ctx->bam_enabled = fd_topo_find_tile( topo, "bam", 0UL )!=ULONG_MAX;
}
