#include "fd_keyload.h"
#include "fd_keyguard.h"
#include "../../flamenco/gossip/fd_gossip_message.h"

/* Keep this test tied to the exact feature-set constant checked by
   fd_keyguard_authorize_gossip without pulling in the wider runtime. */
#define HEADER_fd_src_flamenco_features_fd_features_h
#include "../../flamenco/features/fd_features_generated.h"
#undef HEADER_fd_src_flamenco_features_fd_features_h

#include <setjmp.h>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <sys/wait.h>

#define TEST_FORK_OK(child) do {                            \
    pid_t pid = fork();                                     \
    if( pid ) {                                             \
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
  if( pid ) {
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

void
test_vote_txn_oob( void ) {
  uchar data[172];
  memset( data, 0, sizeof(data) );

  data[0] = 2;    /* signer_cnt */
  data[1] = 1;    /* ro_signed_cnt = signer_cnt - 1 */
  data[2] = 1;    /* ro_unsigned_cnt */
  data[3] = 4;    /* acc_cnt (compact_u16, 1 byte) */

  fd_keyguard_authority_t authority;
  memset( &authority, 0xAA, sizeof(authority) );
  memcpy( data + 4, authority.identity_pubkey, 32 );

  uchar vote_prog_id[32] = {
    0x07, 0x61, 0x48, 0x1d, 0x35, 0x74, 0x74, 0xbb,
    0x7c, 0x4d, 0x76, 0x24, 0xeb, 0xd3, 0xbd, 0xb3,
    0xd8, 0x35, 0x5e, 0x73, 0xd1, 0x10, 0x43, 0xfc,
    0x0d, 0xa3, 0x53, 0x80, 0x00, 0x00, 0x00, 0x00
  };
  memcpy( data + 100, vote_prog_id, 32 );

  data[164] = 1;  /* instr_cnt = 1 (compact_u16, 1 byte) */
  data[165] = 3;  /* index of vote program = acc_cnt - 1 */
  data[166] = 2;  /* compact_u16 = 2, 1 byte */

  data[167] = 0;
  data[168] = 1;

  data[169] = 0x80;  /* bit 7 set -> need at least 2 bytes */
  data[170] = 0x80;  /* bit 7 set -> need 3 bytes */
  data[171] = 0x01;  /* non-zero, upper bits clear -> valid 3-byte cu16 */

  int res = fd_keyguard_payload_authorize( &authority,
                                           data,
                                           sizeof(data),
                                           FD_KEYGUARD_ROLE_TXSEND,
                                           FD_KEYGUARD_SIGN_TYPE_ED25519 );

  (void)res;
}

/* tmp_key_file creates a new (unprotected) key file with random bytes.
   tmp_key_file1 is a variant with chosen bytes.

   Returns a pointer to the cstr file path, which is valid until the
   next call to tmp_key_file or tmp_key_file1.

   The caller is responsible for deleting this file. */

static char const *
tmp_key_file1( uchar const content[ 64 ] ) {
  char json[ 512 ];
  char * p = fd_cstr_init( json );
  p = fd_cstr_append_char( p, '[' );
  for( ulong i=0UL; i<64UL; i++ ) {
    if( i ) p = fd_cstr_append_char( p, ',' );
    p = fd_cstr_append_uchar_as_text( p, 0, 0, (uchar)content[ i ], fd_uint_base10_dig_cnt( content[ i ] ) );
  }
  p = fd_cstr_append_char( p, ']' );
  ulong len = (ulong)( p-json );
  fd_cstr_fini( p );

  static char path[ 28 ];
  strcpy( path, "/tmp/fd_keyload_test_XXXXXX" );
  int fd = mkstemp( path );
  FD_TEST( fd>=0 );
  FILE * f = fdopen( fd, "wb" );
  FD_TEST( f );
  FD_TEST( fwrite( json, 1, len, f )==len );
  FD_TEST( !fclose( f ) );
  return path;
}

static char const *
tmp_key_file( void ) {
  uchar content[ 64 ];
  FD_TEST( fd_rng_secure( content, 32 ) );
  fd_sha512_t sha[1];
  fd_ed25519_public_from_private( content+32, content, sha );
  return tmp_key_file1( content );
}

static sigjmp_buf segv_jmpbuf;

static void
segfault_handler( int         signo,
                  siginfo_t * info,
                  void *      context ) {
  (void)signo; (void)info; (void)context;
  siglongjmp( segv_jmpbuf, 1 );
}

void
test_readonly( void ) {
  char const * path = tmp_key_file();
  uchar * key = (uchar *)fd_keyload_load( path, 0 );
  FD_TEST( key );
  FD_TEST( !unlink( path ) );

  struct sigaction sa = {0};
  sa.sa_sigaction = segfault_handler;
  sa.sa_flags     = SA_SIGINFO;
  FD_TEST( !sigaction( SIGSEGV, &sa, NULL ) );

  if( sigsetjmp( segv_jmpbuf, 0 )==0 ) {
    FD_VOLATILE( key[0] ) = 1;
    FD_COMPILER_MFENCE();
    FD_LOG_ERR(( "Write to readonly key page did not segfault" ));
  }

  FD_TEST( signal( SIGSEGV, SIG_DFL )!=SIG_ERR );
  fd_keyload_unload( key, 0 );
}

void
test_madvise( void ) {
  /* Query /proc/self/smaps to verify that madvise applied as intended

     smaps format is as follows:

     55555555a000-55555555c000 r--p 00006000 fd:01 537351138                  /usr/bin/bla
     Size:                  8 kB
     KernelPageSize:        4 kB
     MMUPageSize:           4 kB
     ...
     VmFlags: rd mr mw me sd */

  char const * path = tmp_key_file();
  uchar const * key = fd_keyload_load( path, 0 );
  FD_TEST( key );
  FD_TEST( !unlink( path ) );

  FILE * maps = fopen( "/proc/self/smaps", "r" );
  FD_TEST( maps );
  char line[ 4096 ];

  /* Scan until we find a matching region */
  for(;;) {
    FD_TEST( fgets( line, sizeof(line), maps ) );
    ulong start, end;
    if( FD_UNLIKELY( sscanf( line, "%lx-%lx", &start, &end )!=2 ) ) continue;
    if( (void *)start==key ) break;
  }

  /* Now scan for VmFlags */
  struct {
    uint dd:1;
    uint wf:1;
  } flags = {0};
  for(;;) {
    FD_TEST( fgets( line, sizeof(line), maps ) );
    if( strncmp( line, "VmFlags: ", 9 )!=0 ) continue;
    char * tokens[ 16 ];
    ulong flag_cnt = fd_cstr_tokenize( tokens, 16, line+9, ' ' );
    for( ulong i=0UL; i<flag_cnt; i++ ) {
      if( strncmp( tokens[ i ], "dd", 2 )==0 ) flags.dd = 1;
      if( strncmp( tokens[ i ], "wf", 2 )==0 ) flags.wf = 1;
    }
    break;
  }
  if( FD_UNLIKELY( !flags.dd ) ) FD_LOG_ERR(( "key page missing MADV_DONTDUMP" ));
  if( FD_UNLIKELY( !flags.wf ) ) FD_LOG_ERR(( "key page missing MADV_WIPEONFORK" ));

  fd_keyload_unload( key, 0 );
  FD_TEST( !fclose( maps ) );
}

static void
test_keyguard_identity( uchar identity[ static 32 ] ) {
  for( uchar i=0U; i<32U; i++ ) identity[ i ] = i;
}

static ulong
build_contact_info_sign_payload( uchar * out,
                                 ulong   out_sz,
                                 ushort  client_id ) {
  fd_gossip_value_t value;
  fd_memset( &value, 0, sizeof(value) );

  value.tag = FD_GOSSIP_VALUE_CONTACT_INFO;
  test_keyguard_identity( value.origin );
  value.wallclock = 0UL;
  value.contact_info->outset              = 1UL;
  value.contact_info->shred_version       = 1U;
  value.contact_info->version.feature_set = FD_FEATURE_SET_ID;
  value.contact_info->version.client      = client_id;

  uchar crds_value[ FD_GOSSIP_VALUE_MAX_SZ ];
  long crds_value_sz = fd_gossip_value_serialize( &value, crds_value, sizeof(crds_value) );
  FD_TEST( crds_value_sz>64L );
  FD_TEST( (ulong)(crds_value_sz-64L)<=out_sz );

  fd_memcpy( out, crds_value+64UL, (ulong)(crds_value_sz-64L) );
  return (ulong)(crds_value_sz-64L);
}

static int
authorize_contact_info_client_id( ushort client_id ) {
  fd_keyguard_authority_t authority;
  test_keyguard_identity( authority.identity_pubkey );

  uchar payload[ FD_GOSSIP_VALUE_MAX_SZ ];
  ulong payload_sz = build_contact_info_sign_payload( payload, sizeof(payload), client_id );

  return fd_keyguard_payload_authorize( &authority,
                                        payload,
                                        payload_sz,
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
  test_protected_pages();
  test_readonly();
  test_madvise();
  test_vote_txn_oob();
  test_contact_info_client_id_authorization();
  FD_LOG_NOTICE(( "pass" ));
  return 0;
}
