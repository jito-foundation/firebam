/* FireBAM keyload tests.  test_keyload.c is included verbatim so these
   tests share its includes; its main is renamed and not run here. */

#define main test_keyload_upstream_main
int test_keyload_upstream_main( int argc, char ** argv );
#include "../keyguard/test_keyload.c"
#undef main

#include "../keyguard/fd_keyguard.h"
#include "../../flamenco/gossip/fd_gossip_message.h"

/* Keep this test tied to the exact feature-set constant checked by
   fd_keyguard_authorize_gossip without pulling in the wider runtime. */
#define HEADER_fd_src_flamenco_features_fd_features_h
#include "../../flamenco/features/fd_features_generated.h"
#undef HEADER_fd_src_flamenco_features_fd_features_h

static int
authorize_contact_info_client_id( ushort client_id ) {
  fd_keyguard_authority_t authority = {0};

  fd_gossip_value_t value;
  fd_memset( &value, 0, sizeof(value) );

  for( uchar i=0U; i<32U; i++ ) {
    authority.identity_pubkey[ i ] = i;
    value.origin[ i ]              = i;
  }

  value.tag = FD_GOSSIP_VALUE_CONTACT_INFO;
  value.wallclock = 0UL;
  value.contact_info->outset              = 1UL;
  value.contact_info->shred_version       = 1U;
  value.contact_info->version.feature_set = FD_FEATURE_SET_ID;
  value.contact_info->version.client      = client_id;

  uchar crds_value[ FD_GOSSIP_VALUE_MAX_SZ ];
  long crds_value_sz = fd_gossip_value_serialize( &value, crds_value, sizeof(crds_value) );
  FD_TEST( crds_value_sz>64L );

  return fd_keyguard_payload_authorize( &authority,
                                        crds_value+64UL,
                                        (ulong)(crds_value_sz-64L),
                                        FD_KEYGUARD_ROLE_GOSSIP,
                                        FD_KEYGUARD_SIGN_TYPE_ED25519 );
}

static void
test_contact_info_client_id_authorization( void ) {
  FD_TEST( authorize_contact_info_client_id( FD_GOSSIP_CONTACT_INFO_CLIENT_FIREDANCER ) );
  FD_TEST( authorize_contact_info_client_id( FD_GOSSIP_CONTACT_INFO_CLIENT_FIRE_BAM   ) );
  FD_TEST( !authorize_contact_info_client_id( FD_GOSSIP_CONTACT_INFO_CLIENT_AGAVE_BAM ) );
}

int
main( int     argc,
      char ** argv ) {
  fd_log_private_boot( &argc, &argv );
  test_contact_info_client_id_authorization();
  FD_LOG_NOTICE(( "pass" ));
  return 0;
}
