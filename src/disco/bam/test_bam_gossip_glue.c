/* test_bam_gossip_glue checks the gossip tile's input and sign response dispatch:
   a sign response acknowledges a pending BAM contact update once the
   local CRDS table holds our contact advertising it.  CRDS and signing
   are mocked; test_bam_tile covers the acknowledgement rules.  Identity
   and contact installation are mocked below to exercise the real tile
   callbacks without constructing a gossip protocol instance. */

#include "../../discof/gossip/fd_gossip_tile.h"

static fd_gossip_contact_info_t mock_crds_ci[1];
static int                      mock_crds_installed;
static ulong                    mock_sign_pending;
static ulong                    mock_identity_installed;
static ulong                    mock_contact_installed;
static ulong                    mock_advance_cnt;

static void
mock_sign_response( fd_gossip_t *       gossip,
                    uchar const *       signature,
                    fd_stem_context_t * stem,
                    long                now ) {
  (void)gossip; (void)signature; (void)stem; (void)now;
  if( mock_sign_pending ) mock_sign_pending--;
  mock_crds_installed = 1;
}

static ulong
mock_sign_pend_cnt( fd_gossip_t const * gossip ) {
  (void)gossip;
  return mock_sign_pending;
}

static void
mock_set_identity( fd_gossip_t * gossip, uchar const * identity, long now, ulong outset ) {
  (void)gossip; (void)identity; (void)now;
  FD_TEST( outset==11UL );
  mock_identity_installed++;
}

static void
mock_set_contact_info( fd_gossip_t * gossip, fd_gossip_contact_info_t const * contact, long now ) {
  (void)gossip; (void)now;
  FD_TEST( mock_identity_installed==1UL && contact->outset==11UL );
  *mock_crds_ci = *contact;
  mock_crds_installed = 0;
  mock_contact_installed++;
}

static void
mock_advance( fd_gossip_t * gossip, long now, fd_stem_context_t * stem, int * charge_busy ) {
  (void)gossip; (void)now; (void)stem; (void)charge_busy;
  FD_TEST( mock_identity_installed==1UL && mock_contact_installed==1UL );
  mock_sign_pending++;
  mock_advance_cnt++;
}

static fd_gossip_contact_info_t const *
mock_my_crds_contact_info( fd_gossip_t const * gossip ) {
  (void)gossip;
  return mock_crds_installed ? mock_crds_ci : NULL;
}

#define fd_gossip_sign_response        mock_sign_response
#define fd_gossip_sign_pend_cnt        mock_sign_pend_cnt
#define fd_gossip_set_identity         mock_set_identity
#define fd_gossip_set_my_contact_info  mock_set_contact_info
#define fd_gossip_advance              mock_advance
#define fd_gossip_my_crds_contact_info mock_my_crds_contact_info
#define fd_clock_tile_now( clock )     ((void)(clock), 0L)
#include "../../discof/gossip/fd_gossip_tile.c"

static void
test_halted_bam_input_dispatch( void ) {
  static fd_gossip_tile_ctx_t ctx[1];
  static uchar fseq_mem[ FD_FSEQ_FOOTPRINT ] __attribute__((aligned(FD_FSEQ_ALIGN)));
  static uchar keyswitch_mem[ FD_KEYSWITCH_FOOTPRINT ] __attribute__((aligned(FD_KEYSWITCH_ALIGN)));
  fd_bam_contact_update_t update = {
    .tpu               = { .addr = FD_IP4_ADDR( 10, 0, 0, 2 ), .port = fd_ushort_bswap( 6000 ) },
    .tpu_fwd           = { .addr = FD_IP4_ADDR( 10, 0, 0, 2 ), .port = fd_ushort_bswap( 6001 ) },
    .version_client_id = 4U
  };
  /* This callback uses mem only as a chunk-zero byte address. */
  uchar ingress_mem[ sizeof(update) ] __attribute__((aligned(FD_WKSP_ALIGN)));
  fd_memcpy( ingress_mem, &update, sizeof(update) );
  ctx->in[0].kind = IN_KIND_SIGN;
  ctx->in[1] = (fd_gossip_in_ctx_t){ .kind = IN_KIND_BAM_GOSSIP, .mem = (fd_wksp_t *)(void *)ingress_mem,
                                  .chunk0 = 0UL, .wmark = 0UL, .mtu = sizeof(update) };
  ctx->in[2].kind = IN_KIND_GOSSVF;
  ctx->my_contact_info->shred_version = 1U;
  ctx->bam_contact.signed_fseq = fd_fseq_join( fd_fseq_new( fseq_mem, 0UL ) );
  ctx->bam_contact.ack_pending = 1U;
  ctx->keyswitch = fd_keyswitch_join( fd_keyswitch_new( keyswitch_mem, FD_KEYSWITCH_STATE_SWITCH_PENDING ) );
  FD_TEST( ctx->keyswitch );
  ctx->is_halting_signing = 1;
  mock_sign_pending = 1UL;

  /* fd_stem_run1.c calls before_frag before returnable_frag.  A negative
     filter retains the input sequence and skips both fragment callbacks,
     so a delayed restore cannot reach the contact helper during halt. */
  FD_TEST( before_frag( ctx, 1UL, 6UL, 0UL )==-1 );
  FD_TEST( before_frag( ctx, 2UL, 0UL, 0UL )==-1 );
  FD_TEST( before_frag( ctx, 0UL, 0UL, 0UL )==0 );
  after_frag( ctx, 0UL, 0UL, 0UL, 64UL, 0UL, 0UL, NULL );
  FD_TEST( !mock_sign_pending && fd_keyswitch_state_query( ctx->keyswitch )==FD_KEYSWITCH_STATE_COMPLETED );
  FD_TEST( ctx->is_halting_signing && ctx->bam_contact.ack_pending );
  FD_TEST( !fd_fseq_query( ctx->bam_contact.signed_fseq ) && !mock_contact_installed && !mock_advance_cnt );

  /* Housekeeping sets this pending flag on UNHALT_PENDING; the real
     credited callback installs the new identity before releasing inputs. */
  fd_memset( ctx->keyswitch->bytes, 0x17, 32UL );
  ctx->keyswitch->param = 11000UL;
  fd_keyswitch_state( ctx->keyswitch, FD_KEYSWITCH_STATE_UNHALT_PENDING );
  ctx->is_pending_set_identity = 1;
  int poll_in = 1, busy = 0;
  after_credit( ctx, NULL, &poll_in, &busy );
  FD_TEST( busy && !ctx->is_halting_signing && !ctx->is_pending_set_identity );
  FD_TEST( fd_keyswitch_state_query( ctx->keyswitch )==FD_KEYSWITCH_STATE_COMPLETED );
  FD_TEST( mock_identity_installed==1UL && ctx->identity_key->uc[0]==0x17 && ctx->my_contact_info->outset==11UL );
  FD_TEST( before_frag( ctx, 1UL, 6UL, 0UL )==0 && before_frag( ctx, 2UL, 0UL, 0UL )==0 );
  fd_stem_context_t stem = {0};
  FD_TEST( !returnable_frag( ctx, 1UL, 6UL, 0UL, 0UL, sizeof(update), 0UL, 0UL, 0UL, &stem ) );
  FD_TEST( mock_contact_installed==1UL && mock_advance_cnt==1UL && mock_sign_pending==1UL );
  FD_TEST( ctx->bam_contact.ack_pending && ctx->bam_contact.ack_target==7UL );
  after_frag( ctx, 0UL, 1UL, 0UL, 64UL, 0UL, 0UL, NULL );
  FD_TEST( !mock_sign_pending && !ctx->bam_contact.ack_pending );
  FD_TEST( fd_fseq_query( ctx->bam_contact.signed_fseq )==7UL );
}

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

  test_halted_bam_input_dispatch();

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
