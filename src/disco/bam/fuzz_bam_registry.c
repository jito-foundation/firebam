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
  return 0;
}

int
LLVMFuzzerTestOneInput( uchar const * data,
                        ulong         size ) {
  fd_bam_registry_node_t nodes[ 4 ];
  ulong cnt = 0UL;
  fd_bam_registry_parse( (char const *)data, size, nodes, 4UL, &cnt );

  fd_url_t url[1];
  if( fd_url_parse_cstr( url, (char const *)data, size, NULL ) )
    fd_bam_registry_is_url( url );

  static char buf[ FD_BAM_REGISTRY_BODY_MAX ];
  static char loc[ FD_URL_MAX+FD_BAM_REGISTRY_PATH_MAX ];
  if( size>sizeof(buf) ) return 0;
  for( int eof=0; eof<2; eof++ ) {
    ulong len = size;
    fd_memcpy( buf, data, size );
    int rc = fd_bam_registry_http_response( buf, &len, eof, loc, sizeof(loc) );
    if( rc==FD_BAM_REGISTRY_HTTP_BODY ) {
      FD_TEST( len<=size );
      fd_bam_registry_parse( buf, len, nodes, 4UL, &cnt );
    } else if( rc==FD_BAM_REGISTRY_HTTP_REDIRECT ) {
      FD_TEST( loc[0] && strlen( loc )<sizeof(loc) );
      static fd_bam_registry_t reg[1];
      strcpy( reg->f_host, "a.example" ); reg->f_host_len = 9;
      strcpy( reg->f_path, "/v1/nodes?x=1" ); reg->f_path_len = 13;
      reg->f_port = 443; reg->f_ssl = 1;
      if( !fd_bam_registry_redirect_target( reg, loc ) ) {
        FD_TEST( reg->f_path_len<FD_BAM_REGISTRY_PATH_MAX && reg->f_path[0]=='/' );
        FD_TEST( strlen( reg->f_path )==reg->f_path_len );
        FD_TEST( reg->f_host_len && strlen( reg->f_host )==reg->f_host_len );
      }
    }
  }
  return 0;
}
