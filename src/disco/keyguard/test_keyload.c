#include "fd_keyload.h"
#include "fd_keyguard.h"
#include "../../flamenco/gossip/fd_gossip_message.h"

/* Keep this test tied to the exact feature-set constant checked by
   fd_keyguard_authorize_gossip without pulling in the wider runtime. */
#define HEADER_fd_src_flamenco_features_fd_features_h
#include "../../flamenco/features/fd_features_generated.h"
#undef HEADER_fd_src_flamenco_features_fd_features_h

#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <sys/wait.h>

#define TEST_FORK_OK(child) do {                            \
    pid_t pid = fork();                                     \
    if ( pid ) {                                            \
      int wstatus;                                          \
      FD_TEST( -1 != waitpid( pid, &wstatus, WUNTRACED ) ); \
      FD_TEST( WIFEXITED( wstatus ) );                      \
      FD_TEST( !WEXITSTATUS( wstatus ) );                   \
      FD_TEST( !WIFSIGNALED( wstatus ) );                   \
      FD_TEST( !WIFSTOPPED( wstatus ) );                    \
    } else {                                                \
      do { child } while ( 0 );                             \
      exit( EXIT_SUCCESS );                                 \
    }                                                       \
} while( 0 )

void
test_protected_pages( void ) {
  pid_t pid = fork();
  if ( pid ) {
    int wstatus;
    FD_TEST( -1 != waitpid( pid, &wstatus, WUNTRACED ) );
    FD_TEST( WIFSIGNALED( wstatus ) && WTERMSIG( wstatus ) == SIGSEGV );
  } else { // child
    uchar * allocated = fd_keyload_alloc_protected_pages( 1UL, 1UL );
    /* This should trigger a segfault */
    uchar c = FD_VOLATILE_CONST( allocated[ 4096 ] );
    (void)c;
    exit( EXIT_FAILURE );
  }

  pid = fork();
  if ( pid ) {
    int wstatus;
    FD_TEST( -1 != waitpid( pid, &wstatus, WUNTRACED ) );
    FD_TEST( WIFSIGNALED( wstatus ) && WTERMSIG( wstatus ) == SIGSEGV );
  } else { // child
    uchar * allocated = fd_keyload_alloc_protected_pages( 1UL, 1UL );
    /* This should trigger a segfault */
    uchar c = FD_VOLATILE_CONST( allocated[ -1 ] );
    (void)c;
    exit( EXIT_FAILURE );
  }

  uchar * allocated = fd_keyload_alloc_protected_pages( 1UL, 1UL );
  for( ulong i=0UL; i<4096UL; i++ ) FD_TEST( allocated[i]==0 );
  for( ulong i=0UL; i<4096UL; i++ ) allocated[i]=1;

  /* Wiped on fork */
  TEST_FORK_OK( for( ulong i=0UL; i<4096UL; i++ ) FD_TEST( allocated[i]==0 ); );
  /* But not in parent */
  for( ulong i=0UL; i<4096UL; i++ ) FD_TEST( allocated[i]==1 );
}


void test_vote_txn_oob( void ) {
  uchar data[172];
  memset( data, 0, sizeof(data) );

  data[0] = 2;    /* signer_cnt */
  data[1] = 1;    /* ro_signed_cnt = signer_cnt - 1 */
  data[2] = 1;    /* ro_unsigned_cnt */
  data[3] = 4;    /* acc_cnt (compact_u16, 1 byte) */

  fd_keyguard_authority_t authority;
  memset( &authority, 0xAA, sizeof(authority) );
  memcpy( data + 4, authority.identity_pubkey, 32 );

  /* account 3, vote program id */
  uchar vote_prog_id[32] = {
    0x07, 0x61, 0x48, 0x1d, 0x35, 0x74, 0x74, 0xbb,
    0x7c, 0x4d, 0x76, 0x24, 0xeb, 0xd3, 0xbd, 0xb3,
    0xd8, 0x35, 0x5e, 0x73, 0xd1, 0x10, 0x43, 0xfc,
    0x0d, 0xa3, 0x53, 0x80, 0x00, 0x00, 0x00, 0x00
  };
  memcpy( data + 100, vote_prog_id, 32 );

  /* recent blockhash */

  data[164] = 1;  /* instr_cnt = 1 (compact_u16, 1 byte) */
  data[165] = 3;  /* index of vote program = acc_cnt - 1 */
  data[166] = 2;  /* compact_u16 = 2, 1 byte */

  /* account indices for instruction (offsets 167, 168) */
  data[167] = 0;
  data[168] = 1;

  data[169] = 0x80;  /* bit 7 set -> need at least 2 bytes */
  data[170] = 0x80;  /* bit 7 set -> need 3 bytes */
  data[171] = 0x01;  /* non-zero, upper bits clear -> valid 3-byte cu16 */

  int res = fd_keyguard_payload_authorize(
      &authority, data, sizeof(data),
      FD_KEYGUARD_ROLE_TXSEND,
      FD_KEYGUARD_SIGN_TYPE_ED25519 );

  (void)res;
}

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
  FD_TEST( authorize_contact_info_client_id( FD_GOSSIP_CONTACT_INFO_CLIENT_BAM        ) );
  FD_TEST( !authorize_contact_info_client_id( FD_GOSSIP_CONTACT_INFO_CLIENT_AGAVE_BAM ) );
}

int
main( int     argc,
      char ** argv ) {
  fd_log_private_boot( &argc, &argv );

  FD_LOG_NOTICE(( "test_protected_pages.." ));
  test_protected_pages();
  test_vote_txn_oob();
  test_contact_info_client_id_authorization();
  FD_LOG_NOTICE(( "pass" ));
  return 0;
}
