#define _GNU_SOURCE
#include "set_bam.h"

#include "../fd_action.h"
#include "../../../disco/bam/fd_bam_ctrl.h"
#include "../../../util/pod/fd_pod.h"
#include "../../../util/fd_util.h"
#include "../../../util/net/fd_net_headers.h"

#include <limits.h>
#include <unistd.h>

void
set_bam_cmd_args( int *    pargc,
                  char *** pargv,
                  args_t * args ) {
  args->set_bam.has_enable = 0;
  args->set_bam.has_url    = 0;
  args->set_bam.has_sni    = 0;
  args->set_bam.enable     = 0;
  fd_memset( args->set_bam.url, 0, sizeof(args->set_bam.url) );
  fd_memset( args->set_bam.sni, 0, sizeof(args->set_bam.sni) );

  int enable_flag  = fd_env_strip_cmdline_contains( pargc, pargv, "--enable" );
  int disable_flag = fd_env_strip_cmdline_contains( pargc, pargv, "--disable" );
  if( FD_UNLIKELY( enable_flag && disable_flag ) )
    FD_LOG_ERR(( "Cannot pass both --enable and --disable" ));

  if( enable_flag ) {
    args->set_bam.has_enable = 1;
    args->set_bam.enable     = 1;
  } else if( disable_flag ) {
    args->set_bam.has_enable = 1;
    args->set_bam.enable     = 0;
  }

  char const * url = fd_env_strip_cmdline_cstr( pargc, pargv, "--url", NULL, NULL );
  if( url ) {
    args->set_bam.has_url = 1;
    fd_bam_ctrl_copy_str( args->set_bam.url, sizeof(args->set_bam.url), url );
  }

  char const * sni = fd_env_strip_cmdline_cstr( pargc, pargv, "--sni", NULL, NULL );
  if( sni ) {
    args->set_bam.has_sni = 1;
    fd_bam_ctrl_copy_str( args->set_bam.sni, sizeof(args->set_bam.sni), sni );
  }

  if( FD_UNLIKELY( *pargc ) )
    FD_LOG_ERR(( "Usage: fdctl set-bam [--enable|--disable] [--url <url>] [--sni <domain>]" ));

  if( FD_UNLIKELY( !args->set_bam.has_enable &&
                   !args->set_bam.has_url &&
                   !args->set_bam.has_sni ) )
    FD_LOG_ERR(( "Usage: fdctl set-bam [--enable|--disable] [--url <url>] [--sni <domain>]" ));
}

static fd_bam_ctrl_t *
join_bam_ctrl( config_t * config ) {
  fd_topo_t * topo = &config->topo;
  ulong bam_ctrl_obj_id = fd_pod_query_ulong( topo->props, "bam_ctrl", ULONG_MAX );
  if( FD_UNLIKELY( bam_ctrl_obj_id==ULONG_MAX ) )
    FD_LOG_ERR(( "BAM runtime control is unavailable (was BAM enabled when Firedancer was started?)" ));

  fd_topo_obj_t * obj = &topo->objs[ bam_ctrl_obj_id ];
  /* Control lives in shared memory next to the bam tile; we join with RW access so the CLI can
     hand the tile a configuration change request. */
  fd_topo_join_workspace( topo, &topo->workspaces[ obj->wksp_id ], FD_SHMEM_JOIN_MODE_READ_WRITE );

  fd_bam_ctrl_t * ctrl = fd_topo_obj_laddr( topo, bam_ctrl_obj_id );
  if( FD_UNLIKELY( !ctrl ) ) {
    fd_topo_leave_workspaces( topo );
    FD_LOG_ERR(( "Failed to join BAM control workspace" ));
  }
  return ctrl;
}

static void
set_bam_apply_request( args_t *   args,
                       config_t * config,
                       fd_bam_ctrl_t * ctrl ) {
  long state = FD_VOLATILE_CONST( ctrl->state );
  if( FD_UNLIKELY( state==FD_BAM_CTRL_STATE_REQUEST ||
                   state==FD_BAM_CTRL_STATE_APPLYING ) )
    FD_LOG_ERR(( "Another BAM configuration update is in progress" ));

  if( state==FD_BAM_CTRL_STATE_SUCCESS || state==FD_BAM_CTRL_STATE_ERROR )
    /* Make sure the tile sees a fresh request after a previous completion/error path. */
    FD_VOLATILE( ctrl->state ) = FD_BAM_CTRL_STATE_IDLE;

  uchar command = 0;
  if( args->set_bam.has_enable ) command |= FD_BAM_CTRL_CMD_ENABLE;
  if( args->set_bam.has_url    ) command |= FD_BAM_CTRL_CMD_URL;
  if( args->set_bam.has_sni    ) command |= FD_BAM_CTRL_CMD_SNI;
  if( FD_UNLIKELY( !command ) )
    FD_LOG_ERR(( "No BAM configuration changes requested" ));

  ctrl->command = command;
  ctrl->enable  = args->set_bam.has_enable ? (uchar)args->set_bam.enable : ctrl->current_enable;

  if( args->set_bam.has_url ) fd_bam_ctrl_copy_str( ctrl->url, sizeof(ctrl->url), args->set_bam.url );
  else                        fd_bam_ctrl_copy_str( ctrl->url, sizeof(ctrl->url), ctrl->current_url );

  if( args->set_bam.has_sni ) fd_bam_ctrl_copy_str( ctrl->sni, sizeof(ctrl->sni), args->set_bam.sni );
  else                        fd_bam_ctrl_copy_str( ctrl->sni, sizeof(ctrl->sni), ctrl->current_sni );

  ctrl->error[0] = '\0';

  FD_COMPILER_MFENCE();
  FD_VOLATILE( ctrl->state ) = FD_BAM_CTRL_STATE_REQUEST;

  long const timeout_ns = (long)5e9;
  long start_ns = fd_log_wallclock();
  /* Busy-wait until the bam tile processes the request.  The tile only touches state after
     taking ownership via CAS, so polling here is sufficient for synchronization. */
  for( ;; ) {
    long st = FD_VOLATILE_CONST( ctrl->state );
    if( st==FD_BAM_CTRL_STATE_SUCCESS || st==FD_BAM_CTRL_STATE_ERROR )
      break;
    if( FD_UNLIKELY( fd_log_wallclock() - start_ns > timeout_ns ) ) {
      FD_VOLATILE( ctrl->state ) = FD_BAM_CTRL_STATE_IDLE;
      FD_LOG_ERR(( "Timed out waiting for BAM runtime update. Is Firedancer currently running?" ));
    }
    usleep( 10000 ); /* 10ms */
  }

  if( FD_UNLIKELY( ctrl->state==FD_BAM_CTRL_STATE_ERROR ) ) {
    char err_buf[ FD_BAM_CTRL_ERR_MAX ];
    fd_bam_ctrl_copy_str( err_buf, sizeof(err_buf), ctrl->error );
    FD_VOLATILE( ctrl->state ) = FD_BAM_CTRL_STATE_IDLE;
    FD_LOG_ERR(( "Failed to update BAM configuration: %s", err_buf[0] ? err_buf : "unknown error" ));
  }

  int enabled = (int)FD_VOLATILE_CONST( ctrl->current_enable );
  char url_buf[ FD_BAM_CTRL_URL_MAX ];
  fd_bam_ctrl_copy_str( url_buf, sizeof(url_buf), ctrl->current_url );
  char sni_buf[ FD_BAM_CTRL_SNI_MAX ];
  fd_bam_ctrl_copy_str( sni_buf, sizeof(sni_buf), ctrl->current_sni );
  FD_VOLATILE( ctrl->state ) = FD_BAM_CTRL_STATE_IDLE;

  if( sni_buf[0] )
    FD_LOG_NOTICE(( "BAM runtime configuration updated (enabled=%s, url=%s, sni=%s)",
                    enabled ? "true" : "false",
                    url_buf,
                    sni_buf ));
  else
    FD_LOG_NOTICE(( "BAM runtime configuration updated (enabled=%s, url=%s)",
                    enabled ? "true" : "false",
                    url_buf ));

  (void)config;
}

void
set_bam_cmd_fn( args_t *   args,
                config_t * config ) {
  fd_bam_ctrl_t * ctrl = join_bam_ctrl( config );
  set_bam_apply_request( args, config, ctrl );
  fd_topo_leave_workspaces( &config->topo );
}

action_t fd_action_set_bam = {
  .name           = "set-bam",
  .args           = set_bam_cmd_args,
  .fn             = set_bam_cmd_fn,
  .require_config = 1,
  .perm           = NULL,
  .description    = "Update BAM runtime configuration",
};
