#ifndef HEADER_fd_src_discof_gossip_fd_gossip_tile_bam_h
#define HEADER_fd_src_discof_gossip_fd_gossip_tile_bam_h

/* BAM contact handling for the gossip tile.  Included once by
   fd_gossip_tile.c.  The BAM tile publishes the TPU sockets and client
   id to advertise on the bam_gossip link.  Gossip applies them to its
   ContactInfo and acknowledges each update on the bam_gossip_signed
   fseq once the local CRDS table holds a signed ContactInfo from this
   instance advertising it, so BAM activates only after gossip pushed
   the BAM contact. */

#include "fd_gossip_tile.h"
#include "../../disco/bam/fd_bam_types.h"
#include "../../util/pod/fd_pod.h"
#include <limits.h>

/* In kind of the bam_gossip link.  Outside the upstream IN_KIND_*
   range so a new upstream kind never collides with it. */

#define IN_KIND_BAM_GOSSIP (64)

/* fd_gossip_tile_bam_init joins the bam_gossip_signed fseq for the
   bam_gossip input link and returns its in kind. */

static int
fd_gossip_tile_bam_init( fd_gossip_tile_ctx_t * ctx,
                         fd_topo_t const *      topo ) {
  ulong obj_id = fd_pod_query_ulong( topo->props, "bam_gossip_signed", ULONG_MAX );
  FD_TEST( obj_id!=ULONG_MAX );
  ctx->bam_contact.signed_fseq = fd_fseq_join( fd_topo_obj_laddr( topo, obj_id ) );
  FD_TEST( ctx->bam_contact.signed_fseq );
  fd_fseq_update( ctx->bam_contact.signed_fseq, 0UL );
  return IN_KIND_BAM_GOSSIP;
}

/* fd_gossip_tile_bam_set_identity keeps the tile's ContactInfo copy,
   which BAM contact updates install wholesale, on the switched identity
   and its new outset. */

static inline void
fd_gossip_tile_bam_set_identity( fd_gossip_tile_ctx_t * ctx ) {
  ulong identity_outset = (ulong)FD_NANOSEC_TO_MICRO( ctx->keyswitch->param );
  fd_memcpy( ctx->identity_key->uc, ctx->keyswitch->bytes, 32UL );
  ctx->my_contact_info->outset = identity_outset;
}

static _Bool
fd_gossip_tile_apply_bam_contact( fd_gossip_tile_ctx_t *          ctx,
                                  fd_bam_contact_update_t const * update,
                                  ulong                           seq,
                                  long                            now,
                                  fd_stem_context_t *             stem ) {
  /* A newer update supersedes any older contact still waiting on signing,
     including if the newer update proves invalid below. */
  ctx->bam_contact.ack_pending = 0U;
  ushort tpu_port     = fd_ushort_bswap( update->tpu.port );
  ushort tpu_fwd_port = fd_ushort_bswap( update->tpu_fwd.port );
  if( FD_UNLIKELY( tpu_port>(ushort)(USHRT_MAX-6U) || tpu_fwd_port>(ushort)(USHRT_MAX-6U) ) ) {
    FD_LOG_WARNING(( "Ignoring BAM contact update with TPU base port overflow: tpu=" FD_IP4_ADDR_FMT ":%hu fwd=" FD_IP4_ADDR_FMT ":%hu",
                     FD_IP4_ADDR_FMT_ARGS( update->tpu.addr ),
                     tpu_port,
                     FD_IP4_ADDR_FMT_ARGS( update->tpu_fwd.addr ),
                     tpu_fwd_port ));
    return 0;
  }
  /* BAM supplies base TPU ports. Gossip advertises the paired QUIC sockets at base+6. */
  ctx->my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU ] = (fd_gossip_socket_t){
    .is_ipv6 = 0,
    .ip4     = update->tpu.addr,
    .port    = update->tpu.port
  };
  ctx->my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_FORWARDS ] = (fd_gossip_socket_t){
    .is_ipv6 = 0,
    .ip4     = update->tpu_fwd.addr,
    .port    = update->tpu_fwd.port
  };
  ctx->my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_QUIC ] = (fd_gossip_socket_t){
    .is_ipv6 = 0,
    .ip4     = update->tpu.addr,
    .port    = fd_ushort_bswap( (ushort)( tpu_port + 6U ) )
  };
  ctx->my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_FORWARDS_QUIC ] = (fd_gossip_socket_t){
    .is_ipv6 = 0,
    .ip4     = update->tpu_fwd.addr,
    .port    = fd_ushort_bswap( (ushort)( tpu_fwd_port + 6U ) )
  };
  ctx->my_contact_info->version.client = update->version_client_id;
  fd_gossip_set_my_contact_info( ctx->gossip, ctx->my_contact_info, now );
  if( FD_LIKELY( stem ) ) fd_gossip_advance( ctx->gossip, now, stem, NULL );
  if( FD_UNLIKELY( ctx->bam_contact.signed_fseq ) ) {
    ctx->bam_contact.expected    = *update;
    ctx->bam_contact.ack_target  = fd_seq_inc( seq, 1UL );
    ctx->bam_contact.ack_pending = 1U;
  }
  return 1;
}

/* fd_gossip_tile_bam_contact_frag handles a frag from the bam_gossip
   link. */

static void
fd_gossip_tile_bam_contact_frag( fd_gossip_tile_ctx_t * ctx,
                                 ulong                  in_idx,
                                 ulong                  seq,
                                 ulong                  chunk,
                                 ulong                  sz,
                                 fd_stem_context_t *    stem ) {
  if( FD_UNLIKELY( sz!=sizeof(fd_bam_contact_update_t) ) ) {
    FD_LOG_WARNING(( "Unexpected BAM gossip update size %lu", sz ));
    return;
  }
  fd_bam_contact_update_t const * update = fd_chunk_to_laddr_const( ctx->in[ in_idx ].mem, chunk );
  long now = fd_clock_tile_now( ctx->clock );
  (void)fd_gossip_tile_apply_bam_contact( ctx, update, seq, now, stem );
}

static inline int
fd_gossip_tile_bam_contact_matches( fd_bam_contact_update_t const *  expected,
                                    fd_gossip_contact_info_t const * ci ) {
  ushort tpu_port = fd_ushort_bswap( expected->tpu.port );
  ushort fwd_port = fd_ushort_bswap( expected->tpu_fwd.port );
  fd_gossip_socket_t const * tpu  = &ci->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU ];
  fd_gossip_socket_t const * fwd  = &ci->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_FORWARDS ];
  fd_gossip_socket_t const * quic = &ci->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_QUIC ];
  fd_gossip_socket_t const * fquic= &ci->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_FORWARDS_QUIC ];
  return ci->version.client==expected->version_client_id &&
         !tpu->is_ipv6 && tpu->ip4==expected->tpu.addr && tpu->port==expected->tpu.port &&
         !fwd->is_ipv6 && fwd->ip4==expected->tpu_fwd.addr && fwd->port==expected->tpu_fwd.port &&
         !quic->is_ipv6 && quic->ip4==expected->tpu.addr && quic->port==fd_ushort_bswap( (ushort)(tpu_port+6U) ) &&
         !fquic->is_ipv6 && fquic->ip4==expected->tpu_fwd.addr && fquic->port==fd_ushort_bswap( (ushort)(fwd_port+6U) );
}

/* Acknowledges the pending BAM contact update once the local CRDS table
   holds a ContactInfo from this instance advertising it.  Gossip pushed
   that value when it installed it.  A contact that loses the insert
   (same-millisecond hash tiebreak, clock stepped back) is installed by
   a later contact info refresh.  A value signed under the old identity
   while halting cannot acknowledge. */

static void
fd_gossip_tile_bam_contact_ack( fd_gossip_tile_ctx_t * ctx ) {
  if( FD_UNLIKELY( !ctx->bam_contact.ack_pending || ctx->is_halting_signing ) ) return;
  fd_gossip_contact_info_t const * ci = fd_gossip_my_crds_contact_info( ctx->gossip );
  if( FD_UNLIKELY( !ci || ci->outset!=ctx->my_contact_info->outset ||
                   !fd_gossip_tile_bam_contact_matches( &ctx->bam_contact.expected, ci ) ) ) return;
  fd_fseq_update( ctx->bam_contact.signed_fseq, ctx->bam_contact.ack_target );
  ctx->bam_contact.ack_pending = 0U;
}

#endif /* HEADER_fd_src_discof_gossip_fd_gossip_tile_bam_h */
