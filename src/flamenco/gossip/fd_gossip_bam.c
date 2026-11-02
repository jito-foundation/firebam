/* FireBAM part of fd_gossip, declared by the gossip tile
   (discof/gossip/fd_gossip_tile_bam.h).  Included once by fd_gossip.c. */

void
fd_gossip_set_my_contact_info( fd_gossip_t *                    gossip,
                               fd_gossip_contact_info_t const * contact_info,
                               long                             now FD_PARAM_UNUSED ) {
  *gossip->my_contact_info.ci->contact_info = *contact_info;
  refresh_contact_info( gossip );
}

fd_gossip_contact_info_t const *
fd_gossip_my_crds_contact_info( fd_gossip_t const * gossip ) {
  ulong ci_idx = fd_crds_ci_idx( gossip->crds, gossip->identity_pubkey );
  if( FD_UNLIKELY( ci_idx==ULONG_MAX ) ) return NULL;
  return fd_crds_ci( gossip->crds, ci_idx );
}
