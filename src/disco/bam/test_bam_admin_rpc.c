#include "fd_bam_tile_private.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
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

static void
test_bam_admin_rpc_connect_pathname( void ) {
  char path[ 64 ];
  FD_TEST( fd_cstr_printf_check( path, sizeof(path), NULL, "/tmp/fd_bam_admin_rpc_%i.sock", (int)getpid() ) );
  unlink( path );

  /* Nothing is listening yet. */
  FD_TEST( fd_bam_admin_rpc_connect( path )<0 );

  struct sockaddr_un addr = { .sun_family = AF_UNIX };
  fd_cstr_ncpy( addr.sun_path, path, sizeof(addr.sun_path) );
  int srv = socket( AF_UNIX, SOCK_STREAM|SOCK_CLOEXEC, 0 );
  FD_TEST( srv>=0 );
  FD_TEST( !bind( srv, (struct sockaddr *)&addr, sizeof(addr) ) );
  FD_TEST( !listen( srv, 1 ) );

  /* A connected stream comes back non-blocking and immediately usable. */
  int fd = fd_bam_admin_rpc_connect( path );
  FD_TEST( fd>=0 );
  FD_TEST( fcntl( fd, F_GETFL, 0 )&O_NONBLOCK );
  FD_TEST( send( fd, "x", 1UL, MSG_NOSIGNAL )==1L );
  int accepted = accept( srv, NULL, NULL );
  FD_TEST( accepted>=0 );
  FD_TEST( !close( accepted ) );
  FD_TEST( !close( fd ) );

  /* Saturate the listen backlog.  A non-blocking AF_UNIX connect reports
     that as EAGAIN, and unlike TCP there is no deferred completion: such a
     socket polls writable and reads SO_ERROR==0 while send() still fails
     ENOTCONN.  It must be reported as a failed attempt, never handed back
     as connected, so the caller retries instead of caching a dead fd. */
  int   pending[ 64 ];
  ulong pending_cnt = 0UL;
  int   connect_errno = 0;
  for( ulong i=0UL; i<sizeof(pending)/sizeof(pending[0]); i++ ) {
    int p = fd_bam_admin_rpc_connect( path );
    if( FD_UNLIKELY( p<0 ) ) { connect_errno = errno; break; }
    pending[ pending_cnt++ ] = p;
  }
  FD_TEST( connect_errno==EAGAIN );

  for( ulong i=0UL; i<pending_cnt; i++ ) FD_TEST( !close( pending[ i ] ) );
  FD_TEST( !close( srv ) );
  FD_TEST( !unlink( path ) );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );
  test_bam_admin_rpc_soft_timeout_drains_late_response();
  test_bam_admin_rpc_connect_pathname();
  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
