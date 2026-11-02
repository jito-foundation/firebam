/* test_bam_gossip_glue checks the gossip tile's sign response dispatch:
   a sign response acknowledges a pending BAM contact update once the
   local CRDS table holds our contact advertising it.  CRDS and signing
   are mocked; test_bam_tile covers the acknowledgement rules. */

#include "../../discof/gossip/fd_gossip_tile.h"

static fd_gossip_contact_info_t mock_crds_ci[1];
static int                      mock_crds_installed;

static void
mock_sign_response( fd_gossip_t *       gossip,
                    uchar const *       signature,
                    fd_stem_context_t * stem,
                    long                now ) {
  (void)gossip; (void)signature; (void)stem; (void)now;
  mock_crds_installed = 1;
}

static fd_gossip_contact_info_t const *
mock_my_crds_contact_info( fd_gossip_t const * gossip ) {
  (void)gossip;
  return mock_crds_installed ? mock_crds_ci : NULL;
}

#define fd_gossip_sign_response        mock_sign_response
#define fd_gossip_my_crds_contact_info mock_my_crds_contact_info
#define fd_clock_tile_now( clock )     ((void)(clock), 0L)
#include "../../discof/gossip/fd_gossip_tile.c"

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  static fd_gossip_tile_ctx_t ctx[1];
  static uchar fseq_mem[ FD_FSEQ_FOOTPRINT ] __attribute__((aligned(FD_FSEQ_ALIGN)));
  ctx->in[ 0 ].kind               = IN_KIND_SIGN;
  ctx->bam_contact.signed_fseq    = fd_fseq_join( fd_fseq_new( fseq_mem, 0UL ) );
  ctx->my_contact_info->outset    = 7UL;
  ctx->bam_contact.expected       = (fd_bam_contact_update_t){
    .tpu               = { .addr = FD_IP4_ADDR( 10, 0, 0, 1 ), .port = fd_ushort_bswap( 5000 ) },
    .tpu_fwd           = { .addr = FD_IP4_ADDR( 10, 0, 0, 1 ), .port = fd_ushort_bswap( 5001 ) },
    .version_client_id = 4U
  };
  ctx->bam_contact.ack_target     = 5UL;
  ctx->bam_contact.ack_pending    = 1U;

  *mock_crds_ci = *ctx->my_contact_info;
  mock_crds_ci->version.client = 4U;
  mock_crds_ci->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU                ] = (fd_gossip_socket_t){ .ip4 = FD_IP4_ADDR( 10, 0, 0, 1 ), .port = fd_ushort_bswap( 5000 ) };
  mock_crds_ci->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_FORWARDS       ] = (fd_gossip_socket_t){ .ip4 = FD_IP4_ADDR( 10, 0, 0, 1 ), .port = fd_ushort_bswap( 5001 ) };
  mock_crds_ci->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_QUIC           ] = (fd_gossip_socket_t){ .ip4 = FD_IP4_ADDR( 10, 0, 0, 1 ), .port = fd_ushort_bswap( 5006 ) };
  mock_crds_ci->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_FORWARDS_QUIC  ] = (fd_gossip_socket_t){ .ip4 = FD_IP4_ADDR( 10, 0, 0, 1 ), .port = fd_ushort_bswap( 5007 ) };

  after_frag( ctx, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL, NULL );
  FD_TEST( mock_crds_installed );
  FD_TEST( fd_fseq_query( ctx->bam_contact.signed_fseq )==5UL );
  FD_TEST( !ctx->bam_contact.ack_pending );

  /* BAM contact updates wait out an identity switch like other inputs;
     only sign responses pass while signing is halted. */
  ctx->in[ 1 ].kind                   = IN_KIND_BAM_GOSSIP;
  ctx->my_contact_info->shred_version = 1U;
  FD_TEST( before_frag( ctx, 1UL, 0UL, 0UL )==0 );
  ctx->is_halting_signing             = 1;
  FD_TEST( before_frag( ctx, 1UL, 0UL, 0UL )==-1 );
  FD_TEST( before_frag( ctx, 0UL, 0UL, 0UL )==0 );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
