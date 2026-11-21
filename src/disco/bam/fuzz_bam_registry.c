#if !FD_HAS_HOSTED
#error "This target requires FD_HAS_HOSTED"
#endif

#include "fd_bam_registry.h"
#include "../../waltz/http/fd_url.h"
#include "../../util/fd_util.h"

#include <stdlib.h>

int
LLVMFuzzerInitialize( int *    argc,
                      char *** argv ) {
  putenv( "FD_LOG_BACKTRACE=0" );
  setenv( "FD_LOG_PATH", "", 0 );
  fd_boot( argc, argv );
  fd_log_level_core_set( 0 );
  fd_log_level_stderr_set( 4 );
  return 0;
}

/* check_parse checks what the follower relies on: a failed parse
   leaves cnt alone, and a list is non-empty, within cap, sorted and
   free of duplicates. */

static void
check_parse( char const * json,
             ulong        sz ) {
  fd_bam_registry_node_t nodes[ 4 ];
  ulong cnt = 777UL;
  int rc = fd_bam_registry_parse( json, sz, nodes, 4UL, &cnt );
  if( rc ) {
    FD_TEST( ( rc==-1 || rc==-2 ) && cnt==777UL );
    return;
  }
  FD_TEST( cnt>=1UL && cnt<=4UL );
  for( ulong i=1UL; i<cnt; i++ ) {
    uint a = fd_uint_bswap( nodes[ i-1UL ].ip4 );
    uint b = fd_uint_bswap( nodes[ i     ].ip4 );
    FD_TEST( a<b || ( a==b && nodes[ i-1UL ].grpc_port<nodes[ i ].grpc_port ) );
  }
  for( ulong i=0UL; i<cnt; i++ ) { /* a region is a string that logs on one line */
    char const * r = nodes[ i ].region;
    ulong        n = 0UL;
    while( n<sizeof(nodes[ i ].region) && r[ n ] ) FD_TEST( (uchar)r[ n++ ]>=0x20U );
    FD_TEST( n<sizeof(nodes[ i ].region) );
  }
}

static char buf [ FD_BAM_REGISTRY_BODY_MAX ];
static char buf2[ FD_BAM_REGISTRY_BODY_MAX ];
static char loc [ FD_URL_MAX+FD_BAM_REGISTRY_PATH_MAX ];
static char loc2[ FD_URL_MAX+FD_BAM_REGISTRY_PATH_MAX ];

static int
response( uchar const * data,
          ulong         size,
          int           eof,
          char *        b,
          ulong *       len,
          char *        l ) {
  fd_memcpy( b, data, size );
  *len = size;
  l[0] = '\0';
  return fd_bam_registry_http_response( b, len, eof, l, sizeof(loc) );
}

int
LLVMFuzzerTestOneInput( uchar const * data,
                        ulong         size ) {
  check_parse( (char const *)data, size );

  fd_url_t url[1];
  if( fd_url_parse_cstr( url, (char const *)data, size, NULL ) )
    fd_bam_registry_is_url( url );

  if( size>sizeof(buf) ) return 0;

  /* The fetch reads until the response decides, and at EOF it must. */
  ulong len0, len1;
  int r0 = response( data, size, 0, buf,  &len0, loc  );
  int r1 = response( data, size, 1, buf2, &len1, loc2 );
  FD_TEST( r1 );
  if( r0 ) {
    FD_TEST( r0==r1 );
    if( r0==FD_BAM_REGISTRY_HTTP_BODY     ) FD_TEST( len0==len1 && !memcmp( buf, buf2, len0 ) );
    if( r0==FD_BAM_REGISTRY_HTTP_REDIRECT ) FD_TEST( !strcmp( loc, loc2 ) );
  }

  if( r1==FD_BAM_REGISTRY_HTTP_BODY ) {
    FD_TEST( len1<=size );
    check_parse( buf2, len1 );
  } else if( r1==FD_BAM_REGISTRY_HTTP_REDIRECT ) {
    FD_TEST( loc2[0] && strlen( loc2 )<sizeof(loc2) );
    static fd_bam_registry_t reg[1];
    strcpy( reg->f_host, "a.example" ); reg->f_host_len = 9;
    strcpy( reg->f_path, "/v1/nodes?x=1" ); reg->f_path_len = 13;
    reg->f_port = 443; reg->f_ssl = 1;
    if( !fd_bam_registry_redirect_target( reg, loc2 ) ) {
      FD_TEST( reg->f_path_len<FD_BAM_REGISTRY_PATH_MAX && reg->f_path[0]=='/' );
      FD_TEST( strlen( reg->f_path )==reg->f_path_len && !strchr( reg->f_path, '#' ) );
      FD_TEST( reg->f_host_len && strlen( reg->f_host )==reg->f_host_len && reg->f_port );
      if( loc2[0]=='/' ) FD_TEST( !strcmp( reg->f_host, "a.example" ) && reg->f_port==443 && reg->f_ssl );
    }
  }

  /* However TCP splits the bytes, the first decision is the one the
     whole response gives. */
  for( ulong s=0UL; size>1UL && s<8UL; s++ ) {
    ulong k = 1UL + fd_ulong_hash( s ^ size ) % (size-1UL);
    ulong lk;
    int rk = response( data, k, 0, buf2, &lk, loc2 );
    if( !rk ) continue;
    FD_TEST( rk==r0 );
    if( rk==FD_BAM_REGISTRY_HTTP_BODY     ) FD_TEST( lk==len0 && !memcmp( buf2, buf, lk ) );
    if( rk==FD_BAM_REGISTRY_HTTP_REDIRECT ) FD_TEST( !strcmp( loc2, loc ) );
  }
  return 0;
}
