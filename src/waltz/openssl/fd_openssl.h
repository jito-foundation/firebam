#ifndef HEADER_fd_src_waltz_openssl_fd_openssl_h
#define HEADER_fd_src_waltz_openssl_fd_openssl_h

#include "../../util/fd_util_base.h"
#include "../../util/alloc/fd_alloc.h"

#if FD_HAS_OPENSSL

FD_PROTOTYPES_BEGIN

/* fd_openssl_ssl_strerror returns a human-readable string for SSL error
   codes like SSL_ERROR_ZERO_RETURN.  Unfortunately, no such strerror
   API exists in OpenSSL itself, for APIs that don't append to the error
   queue. */

FD_FN_CONST char const *
fd_openssl_ssl_strerror( int ssl_err );

/* fd_openssl_set_thread_alloc installs the OpenSSL allocators once per
   process and sets the thread-local allocator context for the caller. */
void
fd_openssl_set_thread_alloc( fd_alloc_t * alloc );

FD_PROTOTYPES_END

#endif /* FD_HAS_OPENSSL */

#endif /* HEADER_fd_src_waltz_openssl_fd_openssl_h */
