#ifndef HEADER_fd_src_disco_pack_fd_pack_tile_bam_fee_h
#define HEADER_fd_src_disco_pack_fd_pack_tile_bam_fee_h

#include "../../util/fd_util.h"
#include "../bam/fd_bam_types.h"
#include "../bundle/fd_bundle_crank.h"

static inline void
fd_pack_apply_bam_fee_cfg_impl( fd_bam_fee_cfg_t *      cfg,
                                ulong *                 cfg_version,
                                int                     crank_enabled,
                                fd_bundle_crank_3_t *   crank3,
                                fd_bundle_crank_2_t *   crank2 ) {
  if( FD_UNLIKELY( !cfg ) ) return;
  if( FD_UNLIKELY( !crank_enabled ) ) return;

  ulong version = FD_VOLATILE_CONST( cfg->version );
  if( FD_UNLIKELY( !version || version==*cfg_version ) ) return;

  *cfg_version = version;

  if( FD_LIKELY( cfg->has_prio_fee_recipient ) ) {
    fd_memcpy( crank3->new_tip_receiver,
               cfg->prio_fee_recipient,
               sizeof( cfg->prio_fee_recipient ) );
    fd_memcpy( crank2->new_tip_receiver,
               cfg->prio_fee_recipient,
               sizeof( cfg->prio_fee_recipient ) );
  }

  crank3->init_tip_distribution_acct.commission_bps =
      (ushort)fd_uint_min( cfg->commission_bps, 10000U );
}

#endif /* HEADER_fd_src_disco_pack_fd_pack_tile_bam_fee_h */
