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

int
fd_bam_admin_rpc_connect( char const * admin_rpc_path ) {
  if( FD_UNLIKELY( !admin_rpc_path ) ) {
    errno = EINVAL;
    return -1;
  }

  union {
    struct sockaddr    sa;
    struct sockaddr_un un;
  } addr = { .un = { .sun_family = AF_UNIX } };
  ulong path_len = strnlen( admin_rpc_path, sizeof(addr.un.sun_path) );
  if( FD_UNLIKELY( !path_len || path_len>=sizeof(addr.un.sun_path) ) ) {
    errno = EINVAL;
    return -1;
  }
  fd_memcpy( addr.un.sun_path, admin_rpc_path, path_len );
  addr.un.sun_path[ path_len ] = '\0';

  int fd = socket( AF_UNIX, SOCK_STREAM|SOCK_CLOEXEC, 0 );
  if( FD_UNLIKELY( fd<0 ) ) return -1;

  if( FD_UNLIKELY( connect( fd, &addr.sa, (socklen_t)( offsetof( struct sockaddr_un, sun_path ) + path_len + 1UL ) ) ) ) {
    int err = errno;
    close( fd );
    errno = err;
    return -1;
  }

  int flags = fcntl( fd, F_GETFL, 0 );
  if( FD_UNLIKELY( flags<0 || fcntl( fd, F_SETFL, flags|O_NONBLOCK ) ) ) {
    int err = errno;
    close( fd );
    errno = err;
    return -1;
  }

  return fd;
}

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
  int soft_timeout = 0;
  int discard_response = 0;
  char const * fail_phase = NULL;
  int          fail_errno = 0;
  if( FD_UNLIKELY( fd<FD_BAM_ADMIN_RPC_FD_NONE ) ) return -1;
  if( FD_UNLIKELY( fd==FD_BAM_ADMIN_RPC_FD_NONE ) ) {
    fd = fd_bam_admin_rpc_connect( admin_rpc_path );
    if( FD_UNLIKELY( fd<0 ) ) {
      fail_phase = "opening stream";
      fail_errno = errno;
      goto out;
    }
    close_fd = 1;
  }

  discard_response = ctx->admin_rpc_response_pending;
  if( FD_UNLIKELY( discard_response ) ) goto read_response;

send_request:;
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

  ctx->admin_rpc_response_pending = 1U;

read_response:
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
      if( FD_UNLIKELY( ctx->admin_rpc_response_len+1UL>=FD_BAM_ADMIN_RPC_RESPONSE_BUF_SZ ) ) {
        fail_phase = "response overflow";
        goto out;
      }
      ssize_t rd = read( fd,
                         ctx->admin_rpc_response_buf+ctx->admin_rpc_response_len,
                         FD_BAM_ADMIN_RPC_RESPONSE_BUF_SZ-ctx->admin_rpc_response_len-1UL );
      if( FD_LIKELY( rd>0 ) ) {
        ctx->admin_rpc_response_len += (ulong)rd;
        ctx->admin_rpc_response_buf[ ctx->admin_rpc_response_len ] = '\0';
        int depth     = 0;
        int in_string = 0;
        int escaped   = 0;
        int saw_obj   = 0;
        for( ulong i=0UL; i<ctx->admin_rpc_response_len; i++ ) {
          char c = ctx->admin_rpc_response_buf[ i ];
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
            if( FD_UNLIKELY( !discard_response &&
                             ctx->admin_rpc_response_len+1UL>response_max ) ) {
              fail_phase = "response overflow";
              goto out;
            }
            if( FD_LIKELY( !discard_response ) )
              fd_memcpy( response, ctx->admin_rpc_response_buf, ctx->admin_rpc_response_len+1UL );
            ctx->admin_rpc_response_len     = 0UL;
            ctx->admin_rpc_response_pending = 0U;
            if( FD_UNLIKELY( discard_response ) ) {
              discard_response = 0;
              goto send_request;
            }
            rc = 0;
            goto out;
          }
        }
        continue;
      }

      if( FD_UNLIKELY( !rd ) ) {
        fail_phase = "reading response EOF";
        goto out;
      }
      if( errno==EINTR ) continue;
      if( errno==EAGAIN || errno==EWOULDBLOCK ) break;
      fail_phase = "read";
      fail_errno = errno;
      goto out;
    }
  }
  fail_phase = discard_response ? "waiting for outstanding response" : "waiting for a complete response";
  soft_timeout = 1;

out:
  if( FD_UNLIKELY( rc && fail_phase ) ) {
    if( FD_LIKELY( fail_errno ) ) FD_LOG_WARNING(( "BAM admin RPC request failed while %s on `%s` (%i-%s)",
                                                   fail_phase, admin_rpc_path, fail_errno, fd_io_strerror( fail_errno ) ));
    else                          FD_LOG_WARNING(( "BAM admin RPC request failed while %s on `%s`",
                                                   fail_phase, admin_rpc_path ));
  }
  if( FD_UNLIKELY( rc && ( !soft_timeout || close_fd ) ) ) {
    ctx->admin_rpc_response_len     = 0UL;
    ctx->admin_rpc_response_pending = 0U;
  }
  if( FD_UNLIKELY( rc && !soft_timeout && !close_fd && fd>=0 ) ) {
    close( fd );
    ctx->admin_rpc_fd = FD_BAM_ADMIN_RPC_FD_DEAD;
  } else if( FD_LIKELY( close_fd ) ) {
    close( fd );
  }
  return rc;
}
