#define _GNU_SOURCE
#include "fd_bam_tile_private.h"
#include "../../util/cstr/fd_cstr.h"
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

#define FD_BAM_ADMIN_RPC_REQ_MAX       (512UL)
#define FD_BAM_ADMIN_RPC_RESP_MAX      (4096UL)
#define FD_BAM_ADMIN_RPC_LOG_RESP_MAX  (256UL)
#define FD_BAM_ADMIN_RPC_POLL_MS       (250)
#define FD_BAM_ADMIN_RPC_MAX_POLLS     (4)
#define FD_BAM_ADMIN_RPC_SLEEP_AFTER_SET_NS ((long)2e6)

static int
fd_bam_admin_rpc_extract_string_field( char const * json,
                                       char const * key,
                                       char *       out,
                                       ulong        out_sz ) {
  char const * cursor = strstr( json, key );
  if( FD_UNLIKELY( !cursor ) ) return -1;
  cursor += strlen( key );
  while( *cursor && isspace( (uchar)*cursor ) ) cursor++;
  if( FD_UNLIKELY( *cursor++!=':' ) ) return -1;
  while( *cursor && isspace( (uchar)*cursor ) ) cursor++;
  if( FD_UNLIKELY( *cursor++!='"' ) ) return -1;

  ulong out_idx = 0UL;
  for( ; *cursor; cursor++ ) {
    if( FD_UNLIKELY( *cursor=='"' ) ) {
      out[ out_idx ] = '\0';
      return 0;
    }
    if( FD_UNLIKELY( *cursor=='\\' ) ) return -1;
    if( FD_UNLIKELY( out_idx+1UL>=out_sz ) ) return -1;
    out[ out_idx++ ] = *cursor;
  }

  return -1;
}

static int
fd_bam_admin_rpc_parse_socket( char const *   socket_cstr,
                               fd_ip4_port_t * out ) {
  char buf[ sizeof("255.255.255.255:65535") ];
  strlcpy( buf, socket_cstr, sizeof(buf) );

  char * colon = strrchr( buf, ':' );
  if( FD_UNLIKELY( !colon ) ) return -1;
  *colon = '\0';

  uint ip4 = 0U;
  ushort port = fd_cstr_to_ushort( colon+1 );
  if( FD_UNLIKELY( !fd_cstr_to_ip4_addr( buf, &ip4 ) || !port ) ) return -1;

  *out = (fd_ip4_port_t){ .addr = ip4, .port = fd_ushort_bswap( port ) };
  return 0;
}

static int
fd_bam_admin_rpc_response_complete( char const * buf,
                                    ulong        buf_sz ) {
  int depth     = 0;
  int in_string = 0;
  int escaped   = 0;
  int saw_obj   = 0;

  for( ulong i=0UL; i<buf_sz; i++ ) {
    char c = buf[ i ];
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
      return 1;
    }
  }

  return 0;
}

__attribute__((weak)) int
fd_bam_admin_rpc_request( char const * admin_rpc_path,
                          char const * request,
                          char *       response,
                          ulong        response_max ) {
  if( FD_UNLIKELY( !admin_rpc_path || !admin_rpc_path[0] || !request || !response || response_max<2UL ) ) return -1;

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

  int fd = socket( AF_UNIX, SOCK_STREAM|SOCK_CLOEXEC, 0 );
  if( FD_UNLIKELY( fd<0 ) ) {
    int err = errno;
    FD_LOG_WARNING(( "BAM admin RPC socket(AF_UNIX,SOCK_STREAM|SOCK_CLOEXEC) failed for `%s` (%i-%s)",
                     admin_rpc_path, err, fd_io_strerror( err ) ));
    return -1;
  }

  int rc = -1;
  char const * fail_phase = NULL;
  int          fail_errno = 0;
  if( FD_UNLIKELY( connect( fd, &addr.sa, (socklen_t)( offsetof( struct sockaddr_un, sun_path ) + path_len + 1UL ) ) ) ) {
    fail_phase = "connect";
    fail_errno = errno;
    goto out;
  }

  ulong request_len = strlen( request );
  ulong request_off = 0UL;
  while( request_off<request_len ) {
    ssize_t wr = write( fd, request+request_off, request_len-request_off );
    if( FD_UNLIKELY( wr<0 ) ) {
      if( errno==EINTR ) continue;
      fail_phase = "write";
      fail_errno = errno;
      goto out;
    }
    request_off += (ulong)wr;
  }

  /* jsonrpc-ipc-server expects a live bidirectional stream while it
     processes the request.  Half-closing the write side causes the
     server to terminate the session without sending a response. */
  if( FD_UNLIKELY( fcntl( fd, F_SETFL, O_NONBLOCK ) ) ) {
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
        if( FD_UNLIKELY( fd_bam_admin_rpc_response_complete( response, response_len ) ) ) {
          rc = 0;
          goto out;
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
  close( fd );
  return rc;
}

int
fd_bam_admin_rpc_get_contact_info( char const *    admin_rpc_path,
                                   fd_ip4_port_t * out_tpu,
                                   fd_ip4_port_t * out_tpu_fwd ) {
  char response[ FD_BAM_ADMIN_RPC_RESP_MAX ];
  if( FD_UNLIKELY( fd_bam_admin_rpc_request( admin_rpc_path,
                                             "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"contactInfo\",\"params\":[]}\n",
                                             response,
                                             sizeof(response) ) ) ) return -1;
  if( FD_UNLIKELY( strstr( response, "\"error\"" ) ) ) {
    FD_LOG_WARNING(( "BAM admin RPC contactInfo returned error response: %.*s",
                     (int)fd_ulong_min( strnlen( response, sizeof(response) ), FD_BAM_ADMIN_RPC_LOG_RESP_MAX ),
                     response ));
    return -1;
  }

  char socket_cstr[ 2 ][ sizeof("255.255.255.255:65535") ];
  fd_ip4_port_t socket[ 2 ] = {0};
  char const * keys[ 2 ] = { "\"tpu\"", "\"tpu_forwards\"" };
  for( ulong i=0UL; i<2UL; i++ ) {
    if( FD_UNLIKELY( fd_bam_admin_rpc_extract_string_field( response, keys[ i ], socket_cstr[ i ], sizeof(socket_cstr[ i ]) ) ) ) return -1;
    if( FD_UNLIKELY( fd_bam_admin_rpc_parse_socket( socket_cstr[ i ], &socket[ i ] ) ) ) return -1;
  }

  if( FD_LIKELY( out_tpu ) )     *out_tpu     = socket[ 0 ];
  if( FD_LIKELY( out_tpu_fwd ) ) *out_tpu_fwd = socket[ 1 ];
  return 0;
}

int
fd_bam_admin_rpc_set_public_tpu( char const *        admin_rpc_path,
                                 fd_ip4_port_t const tpu,
                                 fd_ip4_port_t const tpu_fwd ) {
  char request[ FD_BAM_ADMIN_RPC_REQ_MAX ];
  char response[ FD_BAM_ADMIN_RPC_RESP_MAX ];
  for( ulong i=0UL; i<2UL; i++ ) {
    fd_ip4_port_t socket = i ? tpu_fwd : tpu;
    char const * method = i ? "setPublicTpuForwardsAddress" : "setPublicTpuAddress";
    if( FD_UNLIKELY( !fd_cstr_printf_check( request,
                                            sizeof(request),
                                            NULL,
                                            "{\"jsonrpc\":\"2.0\",\"id\":%u,\"method\":\"%s\",\"params\":[\"" FD_IP4_ADDR_FMT ":%hu\"]}\n",
                                            (uint)( 2UL+i ),
                                            method,
                                            FD_IP4_ADDR_FMT_ARGS( socket.addr ),
                                            fd_ushort_bswap( socket.port ) ) ) ) {
      FD_LOG_WARNING(( "BAM admin RPC failed to format %s request for target=" FD_IP4_ADDR_FMT ":%hu",
                       method,
                       FD_IP4_ADDR_FMT_ARGS( socket.addr ),
                       fd_ushort_bswap( socket.port ) ));
      return -1;
    }
    if( FD_UNLIKELY( fd_bam_admin_rpc_request( admin_rpc_path, request, response, sizeof(response) ) ) ) {
      FD_LOG_WARNING(( "BAM admin RPC %s request failed for target=" FD_IP4_ADDR_FMT ":%hu via `%s`",
                       method,
                       FD_IP4_ADDR_FMT_ARGS( socket.addr ),
                       fd_ushort_bswap( socket.port ),
                       admin_rpc_path ));
      return -1;
    }
    if( FD_UNLIKELY( strstr( response, "\"error\"" ) ) ) {
      FD_LOG_WARNING(( "BAM admin RPC %s returned error for target=" FD_IP4_ADDR_FMT ":%hu: %.*s",
                       method,
                       FD_IP4_ADDR_FMT_ARGS( socket.addr ),
                       fd_ushort_bswap( socket.port ),
                       (int)fd_ulong_min( strnlen( response, sizeof(response) ), FD_BAM_ADMIN_RPC_LOG_RESP_MAX ),
                       response ));
      return -1;
    }

    if( !i ) {
      /* Agave refreshes contact-info on each setter.  A short delay avoids
         same-millisecond CRDS tie-breaks between the TPU and TPU-forwards writes. */
      fd_log_wait_until( fd_log_wallclock() + FD_BAM_ADMIN_RPC_SLEEP_AFTER_SET_NS );
    }
  }

  return 0;
}
