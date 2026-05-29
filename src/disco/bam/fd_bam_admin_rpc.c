#define _GNU_SOURCE
#include "fd_bam_tile_private.h"
#include "../../util/io/fd_io.h"
#include "../../util/log/fd_log.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stddef.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#define FD_BAM_ADMIN_RPC_POLL_MS       (250)
#define FD_BAM_ADMIN_RPC_MAX_POLLS     (4)

__attribute__((weak)) int
fd_bam_admin_rpc_request( fd_bam_tile_t * ctx,
                          char const * request,
                          char *       response,
                          ulong        response_max ) {
  if( FD_UNLIKELY( !ctx || !request || !response || response_max<2UL ) ) return -1;
  char const * admin_rpc_path = ctx->admin_rpc_path;
  if( FD_UNLIKELY( !admin_rpc_path[0] ) ) return -1;

  int fd = ctx->admin_rpc_fd;
  int close_fd = 0;
  int rc = -1;
  char const * fail_phase = NULL;
  int          fail_errno = 0;
  if( FD_UNLIKELY( fd<0 ) ) {
    union {
      struct sockaddr    sa;
      struct sockaddr_un un;
    } addr = { .un = { .sun_family = AF_UNIX } };
    ulong path_len = strnlen( admin_rpc_path, sizeof(addr.un.sun_path) );
    if( FD_UNLIKELY( !path_len || path_len>=sizeof(addr.un.sun_path) ) ) {
      FD_LOG_WARNING(( "BAM admin RPC request rejected: invalid Unix socket path `%s`", admin_rpc_path ));
      return -1;
    }
    fd_memcpy( addr.un.sun_path, admin_rpc_path, path_len );
    addr.un.sun_path[ path_len ] = '\0';

    fd = socket( AF_UNIX, SOCK_STREAM|SOCK_CLOEXEC, 0 );
    if( FD_UNLIKELY( fd<0 ) ) {
      int err = errno;
      FD_LOG_WARNING(( "BAM admin RPC socket(AF_UNIX,SOCK_STREAM|SOCK_CLOEXEC) failed for `%s` (%i-%s)",
                       admin_rpc_path, err, fd_io_strerror( err ) ));
      return -1;
    }
    close_fd = 1;

    if( FD_UNLIKELY( connect( fd, &addr.sa, (socklen_t)( offsetof( struct sockaddr_un, sun_path ) + path_len + 1UL ) ) ) ) {
      fail_phase = "connect";
      fail_errno = errno;
      goto out;
    }
  }

  ulong request_len = strlen( request );
  ulong request_off = 0UL;
  while( request_off<request_len ) {
    ssize_t wr = send( fd, request+request_off, request_len-request_off, MSG_NOSIGNAL );
    if( FD_UNLIKELY( wr<0 ) ) {
      if( errno==EINTR ) continue;
      if( errno==EAGAIN || errno==EWOULDBLOCK ) {
        struct pollfd pfd = { .fd = fd, .events = POLLOUT };
        int poll_rc = ppoll( &pfd, 1UL, &(struct timespec){ .tv_sec = 0L, .tv_nsec = FD_BAM_ADMIN_RPC_POLL_MS * (long)1000000L }, NULL );
        if( FD_LIKELY( poll_rc>0 ) ) continue;
        if( FD_UNLIKELY( poll_rc<0 && errno==EINTR ) ) continue;
        fail_phase = poll_rc<0 ? "ppoll(POLLOUT)" : "waiting for writable stream";
        fail_errno = poll_rc<0 ? errno : 0;
        goto out;
      }
      fail_phase = "send";
      fail_errno = errno;
      goto out;
    }
    if( FD_UNLIKELY( !wr ) ) {
      fail_phase = "send";
      goto out;
    }
    request_off += (ulong)wr;
  }

  /* jsonrpc-ipc-server expects a live bidirectional stream while it
     processes the request.  Half-closing the write side causes the
     server to terminate the session without sending a response. */
  if( FD_UNLIKELY( close_fd && fcntl( fd, F_SETFL, O_NONBLOCK ) ) ) {
    fail_phase = "fcntl(F_SETFL,O_NONBLOCK)";
    fail_errno = errno;
    goto out;
  }

  ulong response_len = 0UL;
  response[ 0 ] = '\0';
  for( int poll_idx=0; poll_idx<FD_BAM_ADMIN_RPC_MAX_POLLS; poll_idx++ ) {
    struct pollfd pfd = { .fd = fd, .events = POLLIN };
    int poll_rc = ppoll( &pfd, 1UL, &(struct timespec){ .tv_sec = 0L, .tv_nsec = FD_BAM_ADMIN_RPC_POLL_MS * (long)1000000L }, NULL );
    if( FD_UNLIKELY( poll_rc<0 ) ) {
      if( errno==EINTR ) {
        poll_idx--;
        continue;
      }
      fail_phase = "ppoll";
      fail_errno = errno;
      goto out;
    }
    if( FD_UNLIKELY( !poll_rc ) ) continue;

    for(;;) {
      if( FD_UNLIKELY( response_len+1UL>=response_max ) ) {
        fail_phase = "response overflow";
        goto out;
      }
      ssize_t rd = read( fd, response+response_len, response_max-response_len-1UL );
      if( FD_LIKELY( rd>0 ) ) {
        response_len += (ulong)rd;
        response[ response_len ] = '\0';
        int depth     = 0;
        int in_string = 0;
        int escaped   = 0;
        int saw_obj   = 0;
        for( ulong i=0UL; i<response_len; i++ ) {
          char c = response[ i ];
          if( in_string ) {
            if( escaped ) escaped = 0;
            else if( c=='\\' ) escaped = 1;
            else if( c=='"' )  in_string = 0;
            continue;
          }

          if( c=='"' ) {
            in_string = 1;
            continue;
          }

          if( FD_UNLIKELY( !saw_obj && isspace( (uchar)c ) ) ) continue;
          if( c=='{' ) {
            saw_obj = 1;
            depth++;
          } else if( FD_UNLIKELY( c=='}' && saw_obj && !--depth ) ) {
            rc = 0;
            goto out;
          }
        }
        continue;
      }

      if( FD_LIKELY( !rd ) ) break;
      if( errno==EINTR ) continue;
      if( errno==EAGAIN || errno==EWOULDBLOCK ) break;
      fail_phase = "read";
      fail_errno = errno;
      goto out;
    }
  }
  fail_phase = "waiting for a complete response";

out:
  if( FD_UNLIKELY( rc && fail_phase ) ) {
    if( FD_LIKELY( fail_errno ) ) FD_LOG_WARNING(( "BAM admin RPC request failed while %s on `%s` (%i-%s)",
                                                   fail_phase, admin_rpc_path, fail_errno, fd_io_strerror( fail_errno ) ));
    else                          FD_LOG_WARNING(( "BAM admin RPC request failed while %s on `%s`",
                                                   fail_phase, admin_rpc_path ));
  }
  if( FD_UNLIKELY( rc && !close_fd && fd>=0 ) ) {
    close( fd );
    ctx->admin_rpc_fd = -1;
  } else if( FD_LIKELY( close_fd ) ) {
    close( fd );
  }
  return rc;
}
