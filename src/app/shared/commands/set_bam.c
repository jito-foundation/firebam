#define _GNU_SOURCE
#include "../fd_config.h"
#include "../fd_action.h"
#include "../../../util/pod/fd_pod.h"
#include <stdio.h> // snprintf
#include <unistd.h> // usleep

void
set_bam_cmd_args( int *    pargc,
                  char *** pargv,
                  args_t * args ) {
  /* Start with sentinel "no change" values.  enable stays at -1 until the user requests
     --enable/--disable, while NULL pointers mean URL/SNI should not be modified. */
  args->set_bam.enable = -1;
  args->set_bam.url    = NULL;
  args->set_bam.sni    = NULL;

  int enable_flag  = fd_env_strip_cmdline_contains( pargc, pargv, "--enable" );
  int disable_flag = fd_env_strip_cmdline_contains( pargc, pargv, "--disable" );
  if( FD_UNLIKELY( enable_flag && disable_flag ) )
    FD_LOG_ERR(( "Cannot pass both --enable and --disable" ));

  if( enable_flag ) {
    args->set_bam.enable = 1;
  } else if( disable_flag ) {
    args->set_bam.enable = 0;
  }

  char const * url = fd_env_strip_cmdline_cstr( pargc, pargv, "--url", NULL, NULL );
  if( url ) {
    /* Empty string is a valid input; it clears the URL when copied into the control struct. */
    args->set_bam.url = url;
  }

  char const * sni = fd_env_strip_cmdline_cstr( pargc, pargv, "--sni", NULL, NULL );
  if( sni ) {
    /* Likewise, an empty string erases an existing SNI override. */
    args->set_bam.sni = sni;
  }

  if( FD_UNLIKELY( *pargc ) )
    FD_LOG_ERR(( "Usage: fdctl set-bam [--enable|--disable] [--url <url>] [--sni <domain>]" ));

  if( FD_UNLIKELY( args->set_bam.enable<0 &&
                   !args->set_bam.url &&
                   !args->set_bam.sni ) )
    FD_LOG_ERR(( "Usage: fdctl set-bam [--enable|--disable] [--url <url>] [--sni <domain>]" ));
}

static fd_bam_ctrl_t *
join_bam_ctrl( config_t * config ) {
  fd_topo_t * topo = &config->topo;
  ulong bam_ctrl_obj_id = fd_pod_query_ulong( topo->props, "bam_ctrl", ULONG_MAX );
  if( FD_UNLIKELY( bam_ctrl_obj_id == ULONG_MAX ) )
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
  (void)config;

  /* Acquire exclusive ownership so request fields cannot be written by two CLI instances. */
  for( ;; ) {
    uchar state = FD_VOLATILE_CONST( ctrl->state );
    if( FD_UNLIKELY( state == FD_BAM_CTRL_STATE_REQUEST  ||
                     state == FD_BAM_CTRL_STATE_APPLYING ||
                     state == FD_BAM_CTRL_STATE_SUCCESS  ||
                     state == FD_BAM_CTRL_STATE_ERROR    ||
                     state == FD_BAM_CTRL_STATE_LOCKED ) )
      FD_LOG_ERR(( "Another BAM configuration update is in progress" ));
    if( FD_UNLIKELY( state != FD_BAM_CTRL_STATE_IDLE ) )
      FD_LOG_ERR(( "Unexpected BAM configuration state: %u", (uint)state ));
    if( FD_ATOMIC_CAS( &ctrl->state, FD_BAM_CTRL_STATE_IDLE, FD_BAM_CTRL_STATE_LOCKED ) == FD_BAM_CTRL_STATE_IDLE )
      break;
  }

  uchar command = 0;
  if( args->set_bam.enable>=0 ) command |= FD_BAM_CTRL_CMD_ENABLE;
  if( args->set_bam.url     ) command |= FD_BAM_CTRL_CMD_URL;
  if( args->set_bam.sni     ) command |= FD_BAM_CTRL_CMD_SNI;
  if( FD_UNLIKELY( !command ) )
    FD_LOG_ERR(( "No BAM configuration changes requested" ));

  ctrl->command = command;
  if( args->set_bam.enable>=0 ) ctrl->enable = (uchar)args->set_bam.enable;

  if( args->set_bam.url ) strlcpy( ctrl->url, args->set_bam.url, sizeof(ctrl->url) );

  if( args->set_bam.sni ) strlcpy( ctrl->sni, args->set_bam.sni, sizeof(ctrl->sni) );

  ctrl->error[0] = '\0';

  FD_COMPILER_MFENCE();
  FD_VOLATILE( ctrl->state ) = FD_BAM_CTRL_STATE_REQUEST;

  long const timeout_ns = (long)5e9;
  long start_ns = fd_log_wallclock();
  /* Busy-wait until the bam tile processes the request.  The tile only touches state after
     taking ownership via CAS, so polling here is sufficient for synchronization. */
  for( ;; ) {
    uchar st = FD_VOLATILE_CONST( ctrl->state );
    if( st == FD_BAM_CTRL_STATE_SUCCESS || st == FD_BAM_CTRL_STATE_ERROR )
      break;
    if( FD_UNLIKELY( fd_log_wallclock() - start_ns > timeout_ns ) ) {
      FD_VOLATILE( ctrl->state ) = FD_BAM_CTRL_STATE_IDLE;
      FD_LOG_ERR(( "Timed out waiting for BAM runtime update. Is Firedancer currently running?" ));
    }
    usleep( 10000 ); /* 10ms */
  }

  if( FD_UNLIKELY( ctrl->state == FD_BAM_CTRL_STATE_ERROR ) ) {
    char err_buf[ FD_BAM_CTRL_ERR_MAX ];
    fd_memcpy( err_buf, ctrl->error, sizeof(err_buf) );
    (void)FD_ATOMIC_CAS( &ctrl->state, FD_BAM_CTRL_STATE_ERROR, FD_BAM_CTRL_STATE_IDLE );
    FD_LOG_ERR(( "Failed to update BAM configuration: %s", err_buf[0] ? err_buf : "unknown error" ));
  }

  char out_buf[ FD_URL_MAX + FD_SNI_BUF_MAX + 96 ];
  int  out_sz = snprintf( out_buf, sizeof( out_buf ),
                     "BAM runtime configuration updated (enabled=%s, url=%s, sni=%s)",
                     FD_VOLATILE_CONST( ctrl->enable ) ? "true" : "false",
                     ctrl->url[ 0 ] ? ctrl->url : "(unset)",
                     ctrl->sni[ 0 ] ? ctrl->sni : "(default)" );
  (void)FD_ATOMIC_CAS( &ctrl->state, FD_BAM_CTRL_STATE_SUCCESS, FD_BAM_CTRL_STATE_IDLE );

  if( FD_UNLIKELY( out_sz<0 || (ulong)out_sz>=sizeof( out_buf ) ) )
    FD_LOG_ERR(( "Failed to format BAM runtime configuration output" ));

  FD_LOG_NOTICE(( "%s", out_buf ));
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
