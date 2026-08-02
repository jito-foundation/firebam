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
  char const * usage = "Usage: fdctl set-bam [--enable|--disable] [--url <url>] [--sni <domain>]";

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
    FD_LOG_ERR(( "%s", usage ));

  if( FD_UNLIKELY( args->set_bam.enable<0 &&
                   !args->set_bam.url &&
                   !args->set_bam.sni ) )
    FD_LOG_ERR(( "%s", usage ));
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
  fd_topo_join_workspace( topo, &topo->workspaces[ obj->wksp_id ], FD_SHMEM_JOIN_MODE_READ_WRITE, FD_TOPO_CORE_DUMP_LEVEL_DISABLED );

  fd_bam_ctrl_t * ctrl = fd_topo_obj_laddr( topo, bam_ctrl_obj_id );
  if( FD_UNLIKELY( !ctrl ) ) {
    fd_topo_leave_workspaces( topo );
    FD_LOG_ERR(( "Failed to join BAM control workspace" ));
  }
  return ctrl;
}

static void
set_bam_apply_request( args_t *   args,
                       fd_bam_ctrl_t * ctrl ) {
  /* Acquire exclusive ownership so request fields cannot be written by two CLI instances. */
  for( ;; ) {
    uchar state = FD_VOLATILE_CONST( ctrl->state );
    if( FD_UNLIKELY( state == FD_BAM_CTRL_STATE_REQUEST  ||
                     state == FD_BAM_CTRL_STATE_APPLYING ||
                     state == FD_BAM_CTRL_STATE_SUCCESS  ||
                     state == FD_BAM_CTRL_STATE_ERROR    ||
                     state == FD_BAM_CTRL_STATE_LOCKED ) )
      FD_LOG_ERR(( "Another BAM configuration update is in progress" ));
    else if( FD_UNLIKELY( state != FD_BAM_CTRL_STATE_IDLE ) )
      FD_LOG_ERR(( "Unexpected BAM configuration state: %u", (uint)state ));
    if( FD_LIKELY( FD_ATOMIC_CAS( &ctrl->state, FD_BAM_CTRL_STATE_IDLE, FD_BAM_CTRL_STATE_LOCKED ) == FD_BAM_CTRL_STATE_IDLE ) )
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

  if( args->set_bam.url ) fd_cstr_ncpy( ctrl->url, args->set_bam.url, sizeof( ctrl->url ) );

  if( args->set_bam.sni ) fd_cstr_ncpy( ctrl->sni, args->set_bam.sni, sizeof( ctrl->sni ) );

  ctrl->error[0] = '\0';

  FD_COMPILER_MFENCE();
  FD_VOLATILE( ctrl->state ) = FD_BAM_CTRL_STATE_REQUEST;

  long const timeout_ns = (long)15e9;
  long const start_ns = fd_log_wallclock();
  long       last_wait_log_ns = start_ns;
  /* Busy-wait until the bam tile processes the request.  The tile only touches state after
     taking ownership via CAS, so polling here is sufficient for synchronization. */
  for( ;; ) {
    long now = fd_log_wallclock();
    uchar st = FD_VOLATILE_CONST( ctrl->state );
    if( st == FD_BAM_CTRL_STATE_SUCCESS || st == FD_BAM_CTRL_STATE_ERROR )
      break;
    if( FD_UNLIKELY( now - start_ns >= timeout_ns ) ) {
      if( st == FD_BAM_CTRL_STATE_APPLYING ) {
        if( FD_UNLIKELY( now - last_wait_log_ns >= timeout_ns ) ) {
          FD_LOG_WARNING(( "BAM runtime update is still applying; waiting for BAM tile to finish" ));
          last_wait_log_ns = now;
        }
      } else if( st == FD_BAM_CTRL_STATE_REQUEST ) {
        if( FD_LIKELY( FD_ATOMIC_CAS( &ctrl->state, FD_BAM_CTRL_STATE_REQUEST, FD_BAM_CTRL_STATE_IDLE )==FD_BAM_CTRL_STATE_REQUEST ) )
          FD_LOG_ERR(( "Timed out waiting for BAM runtime update to be claimed. Is Firedancer currently running?" ));
      } else {
        FD_LOG_ERR(( "Timed out waiting for BAM runtime update (state=%u). Is Firedancer currently running?",
                     (uint)st ));
      }
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
  set_bam_apply_request( args, ctrl );
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
