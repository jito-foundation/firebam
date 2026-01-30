#include "fd_openssl.h"

#if !FD_HAS_OPENSSL
#error "fd_openssl.c requires FD_HAS_OPENSSL"
#endif

#include <openssl/ssl.h>
#include <openssl/crypto.h>

static FD_TL fd_alloc_t * fd_openssl_alloc_ctx = NULL;

static void *
fd_openssl_crypto_malloc( ulong        num,
                          char const * file,
                          int          line ) {
  (void)file; (void)line;
  void * result = fd_alloc_malloc( fd_openssl_alloc_ctx, 16UL, num + 8UL );
  if( FD_UNLIKELY( !result ) ) return NULL;
  *(ulong *)result = num;
  return (uchar *)result + 8UL;
}

static void
fd_openssl_crypto_free( void *       addr,
                        char const * file,
                        int          line ) {
  (void)file;
  (void)line;

  if( FD_UNLIKELY( !addr ) ) return;
  fd_alloc_free( fd_openssl_alloc_ctx, (uchar *)addr - 8UL );
}

static void *
fd_openssl_crypto_realloc( void *       addr,
                           ulong        num,
                           char const * file,
                           int          line ) {
  if( FD_UNLIKELY( !addr ) ) return fd_openssl_crypto_malloc( num, file, line );
  if( FD_UNLIKELY( !num ) ) {
    fd_openssl_crypto_free( addr, file, line );
    return NULL;
  }

  void * new = fd_alloc_malloc( fd_openssl_alloc_ctx, 16UL, num + 8UL );
  if( FD_UNLIKELY( !new ) ) return NULL;

  ulong old_num = *(ulong *)( (uchar *)addr - 8UL );
  fd_memcpy( (uchar *)new + 8UL, (uchar *)addr, fd_ulong_min( old_num, num ) );
  fd_alloc_free( fd_openssl_alloc_ctx, (uchar *)addr - 8UL );
  *(ulong *)new = num;
  return (uchar *)new + 8UL;
}

FD_FN_CONST char const *
fd_openssl_ssl_strerror( int ssl_err ) {
  switch( ssl_err ) {
  case SSL_ERROR_NONE:                 return "SSL_ERROR_NONE";
  case SSL_ERROR_SSL:                  return "SSL_ERROR_SSL";
  case SSL_ERROR_WANT_READ:            return "SSL_ERROR_WANT_READ";
  case SSL_ERROR_WANT_WRITE:           return "SSL_ERROR_WANT_WRITE";
  case SSL_ERROR_WANT_X509_LOOKUP:     return "SSL_ERROR_WANT_X509_LOOKUP";
  case SSL_ERROR_SYSCALL:              return "SSL_ERROR_SYSCALL";
  case SSL_ERROR_ZERO_RETURN:          return "SSL_ERROR_ZERO_RETURN";
  case SSL_ERROR_WANT_CONNECT:         return "SSL_ERROR_WANT_CONNECT";
  case SSL_ERROR_WANT_ACCEPT:          return "SSL_ERROR_WANT_ACCEPT";
  case SSL_ERROR_WANT_ASYNC:           return "SSL_ERROR_WANT_ASYNC";
  case SSL_ERROR_WANT_ASYNC_JOB:       return "SSL_ERROR_WANT_ASYNC_JOB";
  case SSL_ERROR_WANT_CLIENT_HELLO_CB: return "SSL_ERROR_WANT_CLIENT_HELLO_CB";
  case SSL_ERROR_WANT_RETRY_VERIFY:    return "SSL_ERROR_WANT_RETRY_VERIFY";
  default: return "unknown";
  }
}

void
fd_openssl_set_thread_alloc( fd_alloc_t * alloc ) {
  fd_openssl_alloc_ctx = alloc;

  FD_ONCE_BEGIN {
    if( FD_UNLIKELY( !CRYPTO_set_mem_functions( fd_openssl_crypto_malloc,
                                                fd_openssl_crypto_realloc,
                                                fd_openssl_crypto_free ) ) ) {
      FD_LOG_ERR(( "CRYPTO_set_mem_functions failed" ));
    }
  } FD_ONCE_END;
}
