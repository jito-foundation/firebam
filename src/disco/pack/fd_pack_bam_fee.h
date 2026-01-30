#ifndef HEADER_fd_src_disco_pack_fd_pack_bam_fee_h
#define HEADER_fd_src_disco_pack_fd_pack_bam_fee_h

#include "../bundle/fd_bundle_crank.h"
#include "../bam/fd_bam_types.h"

/* Applies a BAM fee configuration to the pack crank state. Updates
   tip-receiver destinations stored in |gen| and writes a clamped copy
   of commission_bps into |commission_bps_field| when a new version is
   observed. |version_tracker| is updated to the applied version and is
   used to detect duplicates. If |crank_enabled| is zero, the call is a
   no-op. */
void
fd_pack_bam_fee_cfg_apply( fd_bundle_crank_gen_t *           gen,
                           ushort *                          commission_bps_field,
                           fd_bam_fee_cfg_t const *          cfg,
                           ulong *                           version_tracker,
                           int                               crank_enabled );

#endif /* HEADER_fd_src_disco_pack_fd_pack_bam_fee_h */
