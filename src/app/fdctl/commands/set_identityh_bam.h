#ifndef HEADER_fd_src_app_fdctl_commands_set_identityh_bam_h
#define HEADER_fd_src_app_fdctl_commands_set_identityh_bam_h

/* BAM signs with the identity key like the bundle tile, so set-identity
   halts, switches and unhalts it together with bundle.  These helpers
   apply the bundle steps to both tiles; either may be absent. */

#include "../../../disco/topo/fd_topo.h"
#include "../../../disco/keyguard/fd_keyswitch.h"

static inline int
set_identityh_is_bundle( char const * tile_name ) {
  return !strcmp( tile_name, "bundle" ) || !strcmp( tile_name, "bam" );
}

static inline int
set_identityh_bundle_exists( fd_topo_t const * topo ) {
  return fd_topo_find_tile( topo, "bundle", 0UL )!=ULONG_MAX ||
         fd_topo_find_tile( topo, "bam",    0UL )!=ULONG_MAX;
}

static inline void
set_identityh_bundle_unhalt( fd_topo_t const * topo ) {
  for( ulong i=0UL; i<topo->tile_cnt; i++ ) {
    if( !set_identityh_is_bundle( topo->tiles[ i ].name ) ) continue;
    fd_keyswitch_t * keyswitch = fd_topo_obj_laddr( topo, topo->tiles[ i ].id_keyswitch_obj_id );
    FD_TEST( keyswitch );
    FD_COMPILER_MFENCE();
    fd_keyswitch_state( keyswitch, FD_KEYSWITCH_STATE_UNHALT_PENDING );
    FD_COMPILER_MFENCE();
  }
}

/* set_identityh_bundle_state returns COMPLETED once every bundle and
   BAM tile has completed its unhalt, otherwise the first other state
   observed.  Each state is read once, so a tile that completes during
   the scan cannot make a still-pending tile look completed. */

static inline ulong
set_identityh_bundle_state( fd_topo_t const * topo ) {
  int found = 0;
  for( ulong i=0UL; i<topo->tile_cnt; i++ ) {
    if( !set_identityh_is_bundle( topo->tiles[ i ].name ) ) continue;
    fd_keyswitch_t const * keyswitch = fd_topo_obj_laddr( topo, topo->tiles[ i ].id_keyswitch_obj_id );
    FD_TEST( keyswitch );
    found = 1;
    ulong state = fd_keyswitch_state_query( keyswitch );
    if( state!=FD_KEYSWITCH_STATE_COMPLETED ) return state;
  }
  FD_TEST( found );
  return FD_KEYSWITCH_STATE_COMPLETED;
}

#endif /* HEADER_fd_src_app_fdctl_commands_set_identityh_bam_h */
