#if FD_HAS_OPENSSL
#include "../../util/bits/fd_bits.h"
#include "fd_openssl.h"
#include "fd_openssl_tile.h"

#include <dirent.h>
#include <unistd.h>
#include <errno.h>

/* Thread-local alloc object for each tile that uses OpenSSL. */
FD_TL fd_alloc_t * fd_ossl_alloc        = NULL;
FD_TL ulong        fd_ossl_alloc_errors = 0UL;

void
fd_ossl_tile_init( fd_alloc_t * alloc ) {
  FD_TEST( alloc );
  fd_ossl_alloc = alloc;
  fd_openssl_set_thread_alloc( alloc );

  FD_ONCE_BEGIN {
    OPENSSL_init_ssl(
      OPENSSL_INIT_LOAD_SSL_STRINGS |
      OPENSSL_INIT_LOAD_CRYPTO_STRINGS |
      OPENSSL_INIT_NO_LOAD_CONFIG,
      NULL );
  } FD_ONCE_END;
}

void
fd_ossl_load_certs( SSL_CTX * ssl_ctx ) {
  X509_STORE * ca_certs = X509_STORE_new();
  if( FD_UNLIKELY( !ca_certs ) ) {
    FD_LOG_ERR(( "X509_STORE_new failed" ));
  }

  static char const default_dir[] = "/etc/ssl/certs/";
  DIR * dir = opendir( default_dir );
  if( FD_UNLIKELY( !dir ) ) {
    FD_LOG_ERR(( "opendir(%s) failed (%i-%s)", default_dir, errno, fd_io_strerror( errno ) ));
  }

  struct dirent * entry;
  for(;;) {
    errno = 0;
    entry = readdir( dir );
    if( FD_UNLIKELY( !entry ) ) break;
    if( !strcmp( entry->d_name, "." ) || !strcmp( entry->d_name, ".." ) ) continue;

    char cert_path[ PATH_MAX ];
    char * p = fd_cstr_init( cert_path );
    p = fd_cstr_append_text( p, default_dir, sizeof(default_dir)-1 );
    p = fd_cstr_append_cstr_safe( p, entry->d_name, (ulong)(cert_path+sizeof(cert_path)-1) - (ulong)p );
    fd_cstr_fini( p );

    if( !X509_STORE_load_locations( ca_certs, cert_path, NULL ) ) {
      /* Not all files in /etc/ssl/certs are valid certs, so ignore errors */
      continue;
    }
  }

  if( FD_UNLIKELY( errno ) ) {
    FD_LOG_ERR(( "readdir(%s) failed (%i-%s)", default_dir, errno, fd_io_strerror( errno ) ));
  }

  STACK_OF(X509) * cert_list = X509_STORE_get1_all_certs( ca_certs );
  FD_LOG_INFO(( "Loaded %d CA certs from %s into OpenSSL", sk_X509_num( cert_list ), default_dir ));
  if( fd_log_level_logfile()==0 ) {
    for( int i=0; i<sk_X509_num( cert_list ); i++ ) {
      X509 * cert = sk_X509_value( cert_list, i );
      FD_LOG_DEBUG(( "Loaded CA cert \"%s\"", X509_NAME_oneline( X509_get_subject_name( cert ), NULL, 0 ) ));
    }
  }
  sk_X509_pop_free( cert_list, X509_free );

  SSL_CTX_set_cert_store( ssl_ctx, ca_certs );

  if( FD_UNLIKELY( 0!=closedir( dir ) ) ) {
    FD_LOG_ERR(( "closedir(%s) failed (%i-%s)", default_dir, errno, fd_io_strerror( errno ) ));
  }

  SSL_CTX_set_verify( ssl_ctx, SSL_VERIFY_PEER, NULL );
}

#endif /* FD_HAS_OPENSSL */
