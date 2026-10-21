/* FireBAM part of the replay tile, included once by fd_replay_tile.c:
   under Tower, while BAM is enabled PoH keeps the nominal, unadjusted
   slot duration.  Alpenglow leader timing ignores BAM. */

#include "../../disco/bam/fd_bam_ctrl.h"

/* replay_poh_slot_duration_ns returns the PoH slot duration for params
   under the latched timing mode, and records it as the value of the
   SlotDurationNanos gauge. */

static inline ulong
replay_poh_slot_duration_ns( fd_replay_tile_t *       ctx,
                             fd_slot_params_t const * params ) {
  ctx->metrics.slot_duration_ns = ctx->use_nominal_slot_duration ? params->ns_per_slot : params->ns_per_slot_adjusted;
  return ctx->metrics.slot_duration_ns;
}

/* replay_bam_metrics_write sets the SlotDurationNanos gauge.  Alpenglow
   leader timing does not use replay_poh_slot_duration_ns, so under
   Alpenglow the gauge reports the adjusted duration of the slot replay
   last refreshed its epoch info from, which Alpenglow PoH ticks use. */

static inline void
replay_bam_metrics_write( fd_replay_tile_t const * ctx ) {
  ulong slot_duration_ns = ctx->alpenglow ? fd_ulong_sat_sub( ctx->ns_per_slot, FD_TARGET_SLOT_ADJUSTMENT_NS ) : ctx->metrics.slot_duration_ns;
  FD_MGAUGE_SET( REPLAY, SLOT_DURATION_NANOS, slot_duration_ns );
}

static void
replay_bam_init( fd_replay_tile_t * ctx,
                 fd_topo_t const *  topo ) {
  ulong bam_ctrl_obj_id = fd_pod_query_ulong( topo->props, "bam_ctrl", ULONG_MAX );
  ctx->bam_ctrl = bam_ctrl_obj_id==ULONG_MAX ? NULL : fd_topo_obj_laddr( topo, bam_ctrl_obj_id );
  ctx->use_nominal_slot_duration = !ctx->alpenglow && fd_topo_find_tile( topo, "bam", 0UL )!=ULONG_MAX;
  if( FD_UNLIKELY( ctx->use_nominal_slot_duration ) ) FD_LOG_NOTICE(( "PoH slot timing mode: nominal (BAM startup mode)" ));
}
