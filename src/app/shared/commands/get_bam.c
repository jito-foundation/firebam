#define _GNU_SOURCE

#include "../fd_config.h"
#include "../fd_action.h"
#include "../../../disco/bam/fd_bam_ctrl.h"
#include "../../../util/pod/fd_pod.h"
#include "../../../util/fd_util.h"

#include <string.h>
#include <unistd.h>

void
get_bam_cmd_fn( args_t *   args FD_PARAM_UNUSED,
                config_t * config ) {
  fd_topo_t * topo = &config->topo;
  ulong bam_ctrl_obj_id = fd_pod_query_ulong( topo->props, "bam_ctrl", ULONG_MAX );
  if( FD_UNLIKELY( bam_ctrl_obj_id == ULONG_MAX ) )
    FD_LOG_ERR(( "BAM runtime control is unavailable (was BAM enabled when Firedancer was started?)" ));

  fd_topo_obj_t * obj = &topo->objs[ bam_ctrl_obj_id ];
  fd_topo_join_workspace( topo, &topo->workspaces[ obj->wksp_id ], FD_SHMEM_JOIN_MODE_READ_ONLY );

  fd_bam_ctrl_t const * ctrl = fd_topo_obj_laddr( topo, bam_ctrl_obj_id );
  if( FD_UNLIKELY( !ctrl ) ) {
    fd_topo_leave_workspaces( topo );
    FD_LOG_ERR(( "Failed to join BAM control workspace" ));
  }

  long const timeout_ns = (long)2e9;
  long start_ns = fd_log_wallclock();
  /* When a set-bam request is inflight the tile is mutating current_* fields; wait for it to
     settle so this command prints a consistent snapshot. */
  for( ;; ) {
    uchar st = FD_VOLATILE_CONST( ctrl->state );
    if( st != FD_BAM_CTRL_STATE_REQUEST && st != FD_BAM_CTRL_STATE_APPLYING )
      break;
    if( FD_UNLIKELY( fd_log_wallclock() - start_ns > timeout_ns ) ) {
      fd_topo_leave_workspaces( topo );
      FD_LOG_ERR(( "Timed out waiting for BAM control to become idle" ));
    }
    usleep( 5000 );
  }

  int enabled = (int)FD_VOLATILE_CONST( ctrl->current_enable );
  char url_buf[ FD_URL_MAX ];
  strlcpy( url_buf, ctrl->current_url, sizeof(url_buf) );
  char sni_buf[ FD_SNI_BUF_MAX ];
  strlcpy( sni_buf, ctrl->current_sni, sizeof(sni_buf) );

  fd_topo_leave_workspaces( topo );

  FD_LOG_STDOUT(( "enabled=%s\n", enabled ? "true" : "false" ));
  FD_LOG_STDOUT(( "url=%s\n", url_buf[0] ? url_buf : "(unset)" ));
  if( sni_buf[0] )
    FD_LOG_STDOUT(( "sni=%s\n", sni_buf ));
  else
    FD_LOG_STDOUT(( "sni=(default)\n" ));
}

action_t fd_action_get_bam = {
  .name           = "get-bam",
  .args           = NULL,
  .fn             = get_bam_cmd_fn,
  .require_config = 1,
  .perm           = NULL,
  .description    = "Show BAM runtime configuration",
};
