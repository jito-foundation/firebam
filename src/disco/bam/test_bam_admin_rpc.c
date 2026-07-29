#include "fd_bam_tile_private.h"

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

static void
test_bam_admin_rpc_setup( fd_bam_tile_t * ctx,
                          int             sock[2] ) {
  FD_TEST( !socketpair( AF_UNIX, SOCK_STREAM, 0, sock ) );
  int flags = fcntl( sock[ 0 ], F_GETFL, 0 );
  FD_TEST( flags>=0 );
  FD_TEST( !fcntl( sock[ 0 ], F_SETFL, flags|O_NONBLOCK ) );
  fd_memset( ctx, 0, sizeof(fd_bam_tile_t) );
  ctx->admin_rpc_fd = sock[ 0 ];
  fd_cstr_ncpy( ctx->admin_rpc_path, "/ignored/admin.rpc", sizeof(ctx->admin_rpc_path) );
}

static void
test_bam_admin_rpc_soft_timeout_drains_late_response( void ) {
  int sock[ 2 ];
  static fd_bam_tile_t ctx[1];
  test_bam_admin_rpc_setup( ctx, sock );

  char const partial_response[] = "{\"jsonrpc\":\"2.0\",\"result\":\"late\"";
  FD_TEST( write( sock[ 1 ], partial_response, strlen( partial_response ) )==(ssize_t)strlen( partial_response ) );

  char response[ FD_BAM_ADMIN_RPC_RESPONSE_BUF_SZ ];
  char const request_one[] = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"one\",\"params\":[]}\n";
  FD_TEST( fd_bam_admin_rpc_request( ctx, request_one, response, sizeof(response) ) );
  FD_TEST( ctx->admin_rpc_fd==sock[ 0 ] );
  FD_TEST( ctx->admin_rpc_response_pending );
  FD_TEST( ctx->admin_rpc_response_len==strlen( partial_response ) );
  FD_TEST( !memcmp( ctx->admin_rpc_response_buf, partial_response, strlen( partial_response ) ) );

  FD_TEST( recv( sock[ 1 ], response, sizeof(request_one)-1UL, MSG_WAITALL )==(ssize_t)(sizeof(request_one)-1UL) );

  char const late_response_tail[] = ",\"id\":1}\n";
  char const request_two[] = "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"two\",\"params\":[]}\n";
  pid_t pid = fork();
  FD_TEST( pid>=0 );
  if( FD_UNLIKELY( !pid ) ) {
    close( sock[ 0 ] );
    struct pollfd pfd = { .fd = sock[ 1 ], .events = POLLIN };
    /* The second request must not be sent while the first response is still
       incomplete.  Release the late tail only after observing that quiet. */
    if( poll( &pfd, 1UL, 100 ) ) _exit( 1 );
    if( write( sock[ 1 ], late_response_tail, strlen( late_response_tail ) )!=(ssize_t)strlen( late_response_tail ) ) _exit( 1 );
    pfd.revents = 0;
    if( poll( &pfd, 1UL, 2000 )!=1 ) _exit( 1 );
    char child_request[ sizeof(request_two) ];
    if( recv( sock[ 1 ], child_request, sizeof(request_two)-1UL, MSG_WAITALL )!=(ssize_t)(sizeof(request_two)-1UL) ) _exit( 1 );
    char const fresh_response[] = "{\"jsonrpc\":\"2.0\",\"result\":\"fresh\",\"id\":2}\n";
    if( write( sock[ 1 ], fresh_response, strlen( fresh_response ) )!=(ssize_t)strlen( fresh_response ) ) _exit( 1 );
    if( shutdown( sock[ 1 ], SHUT_WR ) ) _exit( 1 );
    pfd.revents = 0;
    if( poll( &pfd, 1UL, 2000 )!=1 || read( sock[ 1 ], child_request, sizeof(child_request) )<=0 ) _exit( 1 );
    _exit( 0 );
  }

  close( sock[ 1 ] );
  FD_TEST( !fd_bam_admin_rpc_request( ctx, request_two, response, sizeof(response) ) );
  FD_TEST( strstr( response, "\"result\":\"fresh\"" ) );
  FD_TEST( fd_bam_admin_rpc_request( ctx, request_one, response, sizeof(response) ) );
  FD_TEST( ctx->admin_rpc_fd==FD_BAM_ADMIN_RPC_FD_DEAD );

  int status = 0;
  FD_TEST( waitpid( pid, &status, 0 )==pid );
  FD_TEST( WIFEXITED( status ) && !WEXITSTATUS( status ) );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );
  test_bam_admin_rpc_soft_timeout_drains_late_response();
  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
