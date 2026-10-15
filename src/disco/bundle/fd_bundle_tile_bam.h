#ifndef HEADER_fd_src_disco_bundle_fd_bundle_tile_bam_h
#define HEADER_fd_src_disco_bundle_fd_bundle_tile_bam_h

/* FireBAM part of the bundle tile: while BAM is active the tile sleeps
   as it does without upcoming leader slots, which closes the Block
   Engine connection, and activation discards the pending queue.
   Included once by fd_bundle_tile.c. */

#include "fd_bundle_tile.h"
#include "fd_bundle_tile_private.h"
#include "../bam/fd_bam_types.h"
#include "../topo/fd_topo.h"
#include "../../util/pod/fd_pod.h"

static inline void
fd_bundle_tile_sync_bam_override( fd_bundle_tile_t * ctx ) {
  /* bam_status boots at ULONG_MAX until BAM init writes 0 */
  _Bool bam_active = ctx->bam_status_fseq && fd_fseq_query( ctx->bam_status_fseq )==FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE;
  if( FD_UNLIKELY( bam_active ) ) {
    /* Housekeeping wakes the tile for a near leader slot or an unhalt.
       Keep it asleep and skip the leader schedule check. */
    ctx->sleep_mode     = 1;
    ctx->sleep_check_ns = LONG_MAX;
  }
  if( FD_LIKELY( bam_active==ctx->bam_override_active ) ) return;

  ctx->bam_override_active = bam_active;
  if( bam_active ) {
    pending_txn_remove_all( ctx->pending_txns );
    FD_LOG_NOTICE(( "BAM active; pausing bundle gRPC connection" ));
  } else {
    /* Recheck the leader schedule now, or connect without one */
    ctx->sleep_mode                   = !!ctx->replay_in.mem;
    ctx->sleep_check_ns               = 0L;
    ctx->next_step_deadline           = 0L;
    ctx->last_bundle_status_log_nanos = fd_log_wallclock();
    FD_LOG_NOTICE(( "BAM inactive; resuming bundle gRPC connection" ));
  }
}

static inline void
fd_bundle_tile_bam_init( fd_bundle_tile_t * ctx,
                         fd_topo_t const *  topo ) {
  ulong bam_status_obj_id = fd_pod_query_ulong( topo->props, "bam_status", ULONG_MAX );
  if( FD_LIKELY( bam_status_obj_id!=ULONG_MAX ) ) { /* else NULL, ctx is zeroed */
    ctx->bam_status_fseq = fd_fseq_join( fd_topo_obj_laddr( topo, bam_status_obj_id ) );
    if( FD_UNLIKELY( !ctx->bam_status_fseq ) ) FD_LOG_ERR(( "bundle tile missing bam_status fseq" ));
  }
}

#endif /* HEADER_fd_src_disco_bundle_fd_bundle_tile_bam_h */
