#ifndef HEADER_fd_src_disco_bundle_fd_bundle_tile_bam_h
#define HEADER_fd_src_disco_bundle_fd_bundle_tile_bam_h

/* FireBAM part of the bundle tile: pause the Block Engine connection
   while BAM is active, and claim bundle publication so BAM cannot
   activate mid-publish.  Included once by fd_bundle_tile.c. */

#include "fd_bundle_tile.h"
#include "fd_bundle_tile_private.h"
#include "../bam/fd_bam_types.h"
#include "../topo/fd_topo.h"
#include "../../util/pod/fd_pod.h"

static inline void
fd_bundle_tile_sync_bam_override( fd_bundle_tile_t * ctx ) {
  _Bool bam_active = ctx->bam_status_fseq && ( fd_fseq_query( ctx->bam_status_fseq ) & FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
  if( FD_LIKELY( bam_active==ctx->bam_override_active ) ) return;

  ctx->bam_override_active          = bam_active;
  ctx->last_bundle_status_log_nanos = fd_log_wallclock();
  if( bam_active ) {
    fd_bundle_client_reset( ctx );
    pending_txn_remove_all( ctx->pending_txns );
    ctx->bundle_status_plugin = 127;
    ctx->bundle_status_recent = FD_BUNDLE_STATE_DISCONNECTED;
    ctx->bundle_status_logged = ctx->bundle_status_recent;
    FD_LOG_NOTICE(( "BAM active; pausing bundle gRPC connection" ));
  } else {
    ctx->backoff_until = 0;
    ctx->defer_reset   = 0;
    ctx->next_step_deadline = 0L;
    FD_LOG_NOTICE(( "BAM inactive; resuming bundle gRPC connection" ));
  }
}

/* Claims bundle publication so BAM cannot activate mid-publish.
   Returns 0, after syncing the override, if BAM activated since
   before_credit. */
static inline int
fd_bundle_tile_claim_publish( fd_bundle_tile_t * ctx ) {
  if( FD_UNLIKELY( ctx->bam_status_fseq &&
                   FD_ATOMIC_CAS( ctx->bam_status_fseq, 0UL, FD_BAM_STATUS_FSEQ_BUNDLE_PUBLISHING ) ) ) {
    fd_bundle_tile_sync_bam_override( ctx );
    return 0;
  }
  return 1;
}

static inline void
fd_bundle_tile_release_publish( fd_bundle_tile_t * ctx ) {
  if( FD_LIKELY( ctx->bam_status_fseq ) ) {
    ulong released = FD_ATOMIC_CAS( ctx->bam_status_fseq, FD_BAM_STATUS_FSEQ_BUNDLE_PUBLISHING, 0UL );
    if( FD_UNLIKELY( released!=FD_BAM_STATUS_FSEQ_BUNDLE_PUBLISHING ) )
      FD_LOG_ERR(( "BAM status changed while bundle publication was claimed (status=%lu)", released ));
  }
}

static inline void
fd_bundle_tile_bam_init( fd_bundle_tile_t * ctx,
                         fd_topo_t const *  topo ) {
  ulong bam_status_obj_id = fd_pod_query_ulong( topo->props, "bam_status", ULONG_MAX );
  if( FD_LIKELY( bam_status_obj_id!=ULONG_MAX ) ) {
    ctx->bam_status_fseq = fd_fseq_join( fd_topo_obj_laddr( topo, bam_status_obj_id ) );
    if( FD_UNLIKELY( !ctx->bam_status_fseq ) ) FD_LOG_ERR(( "bundle tile missing bam_status fseq" ));
    ctx->bam_override_active = !!( fd_fseq_query( ctx->bam_status_fseq ) & FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
  } else {
    ctx->bam_status_fseq = NULL;
  }
}

#endif /* HEADER_fd_src_disco_bundle_fd_bundle_tile_bam_h */
