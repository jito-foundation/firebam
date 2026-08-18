#ifndef HEADER_fd_src_waltz_fd_fqdn_h
#define HEADER_fd_src_waltz_fd_fqdn_h

/* Cstr storage capacity for host names used by Waltz URL and resolver
   APIs: up to 255 bytes plus the terminating NUL. */
#define FD_FQDN_BUF_MAX (255UL+1UL)

/* TLS SNI contains a host name. */
#define FD_SNI_BUF_MAX FD_FQDN_BUF_MAX

#endif /* HEADER_fd_src_waltz_fd_fqdn_h */
