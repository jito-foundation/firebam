#define _GNU_SOURCE /* memfd_create */
#include "fd_bam_tile_private.h"
#include "fd_bam_registry.h"
#include "../../util/fd_util.h"
#include "../../util/net/fd_ip4.h"
#include "../../waltz/resolv/fd_adns.h"
#include "../../waltz/resolv/fd_lookup.h"
#include "../waker/fd_waker.h"
#include "../../ballet/ed25519/fd_x25519.h"
#include "../../waltz/tls/test_tls_helper.h"
#include "../../waltz/tlsrec/fd_tlsrec_sock.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/mman.h>
#include <sys/socket.h>

static int
is_registry( char const * raw ) {
  fd_url_t url[1];
  if( !fd_url_parse_cstr( url, raw, strlen( raw ), NULL ) ) return 0;
  return fd_bam_registry_is_url( url );
}

/* ctx_init readies a tile with a system CA bundle and BAM enabled,
   as a waker client (the efficient layout) if waker is set. */

static uchar waker_fseq_mem[ FD_FSEQ_FOOTPRINT ] __attribute__((aligned(FD_FSEQ_ALIGN)));

static void
ctx_init( fd_bam_tile_t * ctx,
          uint            seed,
          int             waker ) {
  fd_bam_tile_ctx_defaults( ctx );
  FD_TEST( fd_rng_join( fd_rng_new( ctx->rng, seed, 0UL ) ) );
  ctx->ca_store->cnt = 1UL;
  ctx->enabled       = 1U;
  if( waker ) {
    ctx->waker_fseq       = fd_fseq_join( fd_fseq_new( waker_fseq_mem, 0UL ) );
    ctx->waker_client_idx = 0UL;
  }
}

/* waker_ready reports whether the tile's waker set has a ready socket
   within timeout_ms, as the waker tile would see it. */

static int
waker_ready( int timeout_ms ) {
  struct pollfd pfd = { .fd = FD_WAKER_INNER_FD( 0UL ), .events = POLLIN };
  int rc = poll( &pfd, 1, timeout_ms );
  FD_TEST( rc>=0 );
  return rc;
}

/* park waits, as a parked tile would, until one of fds or the waker
   set is ready or deadline passes (at most 1 s). */

static void
park( int const * fds,
      ulong       fd_cnt,
      long        deadline ) {
  struct pollfd pfd[ 8 ];
  ulong cnt = 0UL;
  pfd[ cnt++ ] = (struct pollfd){ .fd = FD_WAKER_INNER_FD( 0UL ), .events = POLLIN };
  for( ulong i=0UL; i<fd_cnt; i++ ) if( fds[ i ]>=0 ) pfd[ cnt++ ] = (struct pollfd){ .fd = fds[ i ], .events = POLLIN };
  long wait_ms = ( deadline-fd_bam_now()+999999L )/1000000L;
  FD_TEST( poll( pfd, (nfds_t)cnt, (int)fd_long_max( 0L, fd_long_min( wait_ms, 1000L ) ) )>=0 );
}

static void
take( fd_bam_tile_t * ctx,
      char const *    raw ) {
  fd_url_t url[1];
  ushort   port = 0;
  _Bool    ssl  = 0;
  FD_TEST( !fd_url_parse_endpoint( url, raw, strlen( raw ), &port, &ssl, NULL ) );
  FD_TEST( fd_bam_registry_take_url( ctx, raw, url, ssl, port, NULL, 0UL )==1 );
}

/* listener returns a TCP socket bound to a free loopback port, and
   listening if listening is set. */

static int
listener( int      listening,
          ushort * port ) {
  int fd = socket( AF_INET, SOCK_STREAM, 0 );
  FD_TEST( fd>=0 );
  struct sockaddr_in addr = { .sin_family = AF_INET, .sin_addr.s_addr = FD_IP4_ADDR( 127, 0, 0, 1 ) };
  socklen_t addr_sz = sizeof(addr);
  FD_TEST( !bind( fd, fd_type_pun( &addr ), addr_sz ) );
  if( listening ) FD_TEST( !listen( fd, 4 ) );
  FD_TEST( !getsockname( fd, fd_type_pun( &addr ), &addr_sz ) );
  *port = fd_ushort_bswap( addr.sin_port );
  return fd;
}

/* Each case is what serde_json does with jito's ServedNodes. */

static int
parse( char const *             json,
       fd_bam_registry_node_t * nodes,
       ulong *                  cnt ) {
  return fd_bam_registry_parse( json, strlen( json ), nodes, 8UL, cnt );
}

static int
parses( char const * json ) {
  fd_bam_registry_node_t nodes[ 8 ];
  ulong cnt = 0UL;
  return !parse( json, nodes, &cnt );
}

static void
test_parse( void ) {
  fd_bam_registry_node_t nodes[ 8 ];
  ulong cnt = 0UL;
  FD_TEST( !parse( "{\"generated_at\":\"2026-09-09T12:34:56Z\",\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":50056,\"region\":\"fra\"}]}", nodes, &cnt ) );
  FD_TEST( cnt==1UL && nodes[0].ip4==FD_IP4_ADDR( 203, 0, 113, 1 ) && nodes[0].grpc_port==50056 && !strcmp( nodes[0].region, "fra" ) );

  /* a repeated node is listed once */
  FD_TEST( !parse( "{\"nodes\":[{\"region\":\"a\",\"grpc_port\":50056,\"ip\":\"203.0.113.1\"},{\"ip\":\"203.0.113.9\",\"grpc_port\":50056,\"region\":\"b\"},{\"ip\":\"203.0.113.1\",\"grpc_port\":50056,\"region\":\"c\"}]}", nodes, &cnt ) );
  FD_TEST( cnt==2UL && nodes[0].ip4==FD_IP4_ADDR( 203, 0, 113, 1 ) && nodes[1].ip4==FD_IP4_ADDR( 203, 0, 113, 9 ) );

  /* sorted by octets, not by the in-memory word */
  FD_TEST( !parse( "{\"nodes\":[{\"ip\":\"2.0.0.1\",\"grpc_port\":1,\"region\":\"a\"},{\"ip\":\"1.0.0.2\",\"grpc_port\":1,\"region\":\"b\"}]}", nodes, &cnt ) );
  FD_TEST( cnt==2UL && !strcmp( nodes[0].region, "b" ) );

  /* -2: no IPv4 node; -1: malformed */
  FD_TEST( parse( "{\"nodes\":[]}", nodes, &cnt )==-2 );
  FD_TEST( parse( "{\"nodes\":[{\"ip\":\"2001:db8::1\",\"grpc_port\":50056,\"region\":\"fra\"}]}", nodes, &cnt )==-2 );
  FD_TEST( parse( "{", nodes, &cnt )==-1 );

  /* IpAddr: valid IPv6 is skipped, IPv4-mapped included; anything else
     fails the document */
  FD_TEST( !parse( "{\"nodes\":[{\"ip\":\"2001:db8::1\",\"grpc_port\":50056,\"region\":\"fra\"},{\"ip\":\"203.0.113.9\",\"grpc_port\":50056,\"region\":\"ams\"}]}", nodes, &cnt ) );
  FD_TEST( cnt==1UL && nodes[0].ip4==FD_IP4_ADDR( 203, 0, 113, 9 ) );
  FD_TEST( parses( "{\"nodes\":[{\"ip\":\"::ffff:1.2.3.4\",\"grpc_port\":1,\"region\":\"a\"},{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"zz:zz\",\"grpc_port\":1,\"region\":\"a\"},{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"203.0.113.01\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":1,\"grpc_port\":1,\"region\":\"a\"}]}" ) );

  /* u16 port: 0 is valid (the probe drops it later) */
  FD_TEST( parses( "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":0,\"region\":\"fra\"}]}" ) );
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":65536,\"region\":\"fra\"}]}" ) );
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":050056,\"region\":\"fra\"}]}" ) );
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1.0,\"region\":\"a\"}]}" ) );
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":-1,\"region\":\"a\"}]}" ) );

  /* every field is required, even on a skipped IPv6 node */
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1}]}" ) );
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"2001:db8::1\",\"region\":\"a\"},{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );
  FD_TEST( !parses( "{\"generated_at\":null}" ) );
  FD_TEST( !parses( "[]" ) );

  /* repeated known fields fail; unknown fields are skipped */
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"ip\":\"203.0.113.2\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );
  FD_TEST( !parses( "{\"nodes\":[],\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );
  FD_TEST( !parses( "{\"generated_at\":null,\"generated_at\":null,\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );
  FD_TEST( parses( "{\"x\":1,\"x\":2,\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\",\"added_later\":true}]}" ) );

  /* lenient generated_at: scalars pass, containers fail */
  FD_TEST( parses( "{\"generated_at\":7,\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );
  FD_TEST( !parses( "{\"generated_at\":{},\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );
  FD_TEST( !parses( "{\"generated_at\":[],\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );

  /* escapes are checked, and the region is kept as sent */
  FD_TEST( !parse( "{\"nodes\":[{\"\\u0069p\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\\u00e9\\ud83d\\ude00\\b\\f\\r\"}]}", nodes, &cnt ) );
  FD_TEST( !strcmp( nodes[0].region, "a\\u00e9\\ud83d\\ude00\\b\\f\\r" ) );
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"\\ud83d\"}]}" ) );
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"\xc0\xaf\"}]}" ) );
  /* serde_json ignores a lone surrogate in a skipped value; fd_jtok
     does not, and serde_json never writes one. */
  FD_TEST( !parses( "{\"skip\":\"\\ud83d\",\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );

  /* a long region is kept, truncated at a character boundary */
  FD_TEST( !parse( "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"0123456789012345678901234567890123456789012345678901234567890123456789\"}]}", nodes, &cnt ) );
  FD_TEST( strlen( nodes[0].region )==sizeof(nodes[0].region)-1UL );
  /* 62 bytes, then a 2-byte character */
  FD_TEST( !parse( "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"01234567890123456789012345678901234567890123456789012345678901\xc3\xa9z\"}]}", nodes, &cnt ) );
  FD_TEST( strlen( nodes[0].region )==62UL );
}

static int
http( char const * raw,
      int          eof,
      char *       out,
      ulong *      out_len ) {
  static char buf[ FD_BAM_REGISTRY_BODY_MAX ];
  ulong len = strlen( raw );
  memcpy( buf, raw, len );
  char loc[ 64 ];
  int rc = fd_bam_registry_http_response( buf, &len, eof, loc, sizeof(loc) );
  if( rc==FD_BAM_REGISTRY_HTTP_BODY ) { memcpy( out, buf, len ); out[ len ] = '\0'; *out_len = len; }
  return rc;
}

static void
test_http( void ) {
  char  body[ 256 ];
  ulong n = 0UL;
  FD_TEST( http( "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}", 0, body, &n )==1 && !strcmp( body, "{}" ) );
  FD_TEST( http( "HTTP/1.1 203 OK\r\ncontent-length:2\r\n\r\n{}", 0, body, &n )==1 );
  FD_TEST( http( "HTTP/1.1 200 OK\nContent-Length: 2\n\n{}", 0, body, &n )==1 && !strcmp( body, "{}" ) ); /* bare LF, as httparse allows */
  FD_TEST( http( "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n{}", 0, body, &n )==0 );
  FD_TEST( http( "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n{}", 1, body, &n )==-1 );
  FD_TEST( http( "HTTP/1.1 200 OK\r\n\r\n{}", 1, body, &n )==1 && n==2UL );
  FD_TEST( http( "HTTP/1.1 301 Moved\r\nLocation: /x\r\n\r\n", 1, body, &n )==FD_BAM_REGISTRY_HTTP_REDIRECT );
  FD_TEST( http( "HTTP/1.1 404 Not Found\r\n\r\n", 1, body, &n )==-1 );

  /* An HTTP/1.0 client gets no transfer coding. */
  FD_TEST( http( "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n0\r\n\r\n", 1, body, &n )==-1 );
  FD_TEST( http( "HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\n{}junk", 0, body, &n )==1 && !strcmp( body, "{}" ) );
}

static void
test_rank_and_choose( void ) {
  static fd_bam_registry_t reg[1];
  memset( reg, 0, sizeof(*reg) );
  fd_rng_t rng[1];
  FD_TEST( fd_rng_join( fd_rng_new( rng, 1U, 0UL ) ) );

  fd_bam_registry_node_t nodes[ 2 ];
  memset( nodes, 0, sizeof(nodes) );
  nodes[0].ip4 = 1U; nodes[0].grpc_port = 50056; strcpy( nodes[0].region, "a" );
  nodes[1].ip4 = 2U; nodes[1].grpc_port = 50056; strcpy( nodes[1].region, "b" );
  long now = 100000000000L;
  fd_bam_registry_on_fetch( reg, nodes, 2UL, now, 0, rng );
  FD_TEST( reg->node_cnt==2UL );
  FD_TEST( reg->resync_at==now+FD_BAM_REGISTRY_RESYNC_STALLED_NS );
  FD_TEST( reg->probe_at==now );

  fd_bam_registry_on_fetch( reg, nodes, 2UL, now+3000000000L, 1, rng );
  FD_TEST( reg->probe_at==now ); /* unchanged list keeps the ranking clock */
  FD_TEST( reg->resync_at>=now+3000000000L+FD_BAM_REGISTRY_RESYNC_LIVE_NS/2 );
  FD_TEST( reg->resync_at<=now+3000000000L+FD_BAM_REGISTRY_RESYNC_LIVE_NS );

  /* jito compares whole ServedNodes, so a region change re-probes */
  reg->ranked_cnt = 1UL;
  nodes[0].region[0] = 'z';
  fd_bam_registry_on_fetch( reg, nodes, 2UL, now+3000000000L, 1, rng );
  FD_TEST( reg->ranked_cnt==0UL );
  FD_TEST( reg->probe_at==now+3000000000L );
  FD_TEST( reg->nodes[0].region[0]=='z' );

  reg->ranked_cnt = 2UL;
  reg->ranked[0].node = nodes[0]; reg->ranked[0].rtt_us = 1000UL;
  reg->ranked[1].node = nodes[1]; reg->ranked[1].rtt_us = 45000UL;
  fd_bam_registry_finish_round( reg );
  FD_TEST( reg->ranked_cnt==1UL );
  FD_TEST( reg->ranked[0].rtt_us==1000UL );

  reg->ranked_cnt = 2UL;
  reg->ranked[0].node = nodes[0]; reg->ranked[0].rtt_us = 1000UL;
  reg->ranked[1].node = nodes[1]; reg->ranked[1].rtt_us = 2000UL;
  reg->cursor = 0UL;
  reg->have_dial = 1;
  reg->dial_ip4 = nodes[0].ip4;
  reg->dial_port = nodes[0].grpc_port;
  reg->stalled_ns = FD_BAM_REGISTRY_CONNECT_GRACE_NS;
  reg->stalled_at = now;
  fd_bam_registry_node_t out;
  FD_TEST( fd_bam_registry_choose( reg, 0, 0, now, &out ) );
  FD_TEST( out.ip4==nodes[1].ip4 );

  reg->ranked_cnt = 1UL;
  reg->ranked[0].node = nodes[0];
  reg->cursor = 0UL;
  reg->stalled_ns = FD_BAM_REGISTRY_CONNECT_GRACE_NS;
  FD_TEST( !fd_bam_registry_choose( reg, 0, 0, now, &out ) );
  FD_TEST( !reg->ranked_cnt );
  FD_TEST( reg->stalled_ns==FD_BAM_REGISTRY_CONNECT_GRACE_NS );

  /* Still stuck after the re-probe: a new best publishes at once. */
  reg->ranked_cnt = 1UL;
  reg->ranked[0].node = nodes[1];
  reg->cursor = 0UL;
  FD_TEST( fd_bam_registry_choose( reg, 0, 0, now+1000000000L, &out ) );
  FD_TEST( out.ip4==nodes[1].ip4 );

  /* Healthy resets the clock; nothing to do. */
  reg->ranked[0].node = nodes[0];
  FD_TEST( !fd_bam_registry_choose( reg, 1, 0, now+2000000000L, &out ) );
  FD_TEST( !reg->stalled_ns );

  /* The leader-schedule wait does not count toward the grace period;
     dialing time does. */
  long t = now+2000000000L;
  FD_TEST( !fd_bam_registry_choose( reg, 0, 1, t+30000000000L, &out ) );
  FD_TEST( !reg->stalled_ns );
  FD_TEST( !fd_bam_registry_choose( reg, 0, 0, t+30000000000L+FD_BAM_REGISTRY_CONNECT_GRACE_NS-1L, &out ) );
  FD_TEST( !fd_bam_registry_choose( reg, 0, 1, t+90000000000L, &out ) );
  FD_TEST( reg->stalled_ns==FD_BAM_REGISTRY_CONNECT_GRACE_NS-1L );
  FD_TEST( !fd_bam_registry_choose( reg, 0, 0, t+90000000001L, &out ) ); /* stuck, but already on the only ranked node */
  FD_TEST( !reg->ranked_cnt );
  reg->ranked_cnt = 1UL;
  FD_TEST( !fd_bam_registry_choose( reg, 1, 0, t+90000000002L, &out ) );

  /* Healthy but delisted: move to the best ranked node, or hold the
     live connection when nothing ranked. */
  reg->dial_ip4 = FD_IP4_ADDR( 198, 51, 100, 9 );
  FD_TEST( fd_bam_registry_choose( reg, 1, 0, t+93000000000L, &out ) );
  FD_TEST( out.ip4==nodes[0].ip4 && out.grpc_port==nodes[0].grpc_port );
  reg->ranked_cnt = 0UL;
  FD_TEST( !fd_bam_registry_choose( reg, 1, 0, t+93000000000L, &out ) );
}

static void
test_take_url( void ) {
  /* Only a URL with a path is a registry. */
  FD_TEST( !is_registry( "https://203.0.113.1:50056" ) );
  FD_TEST( !is_registry( "https://203.0.113.1:50056/" ) );
  FD_TEST( !is_registry( "https://203.0.113.1:50056?x=1" ) );

  static fd_bam_tile_t ctx[1];
  fd_bam_tile_ctx_defaults( ctx );
  fd_url_t url[1];
  char const * reg = "https://dev.testnet.registry.bam.jito.wtf/v1/nodes";
  FD_TEST( fd_url_parse_cstr( url, reg, strlen( reg ), NULL ) );
  char err[ 128 ];

  /* Registry nodes are always verified, so no system bundle, no
     registry. */
  FD_TEST( fd_bam_registry_take_url( ctx, reg, url, 1, 50056, err, sizeof(err) )==-1 );
  FD_TEST( !ctx->registry.on && strstr( err, "CA certificate bundle" ) );
  ctx->ca_store->cnt = 1UL;

  char const * plain = "http://registry.example/v1/nodes";
  FD_TEST( fd_url_parse_cstr( url, plain, strlen( plain ), NULL ) );
  FD_TEST( fd_bam_registry_take_url( ctx, plain, url, 0, 50056, err, sizeof(err) )==1 );
  FD_TEST( ctx->registry.on && !ctx->registry.ssl && ctx->registry.port==80 );

  /* The caller passes the BAM default; the registry uses reqwest's. */
  FD_TEST( fd_url_parse_cstr( url, reg, strlen( reg ), NULL ) );
  FD_TEST( fd_bam_registry_take_url( ctx, reg, url, 1, 50056, err, sizeof(err) )==1 );
  FD_TEST( ctx->registry.on && ctx->registry.ssl );
  FD_TEST( ctx->registry.port==443 );
  FD_TEST( ctx->registry.stalled_at && !ctx->registry.stalled_ns );
  FD_TEST( ctx->tls->ca_store==ctx->ca_store );

  char const * reg_port = "https://registry.example:8443/v1/nodes";
  FD_TEST( fd_url_parse_cstr( url, reg_port, strlen( reg_port ), NULL ) );
  FD_TEST( fd_bam_registry_take_url( ctx, reg_port, url, 1, 8443, err, sizeof(err) )==1 );
  FD_TEST( ctx->registry.port==8443 );

  char const * direct = "https://203.0.113.1:50056";
  FD_TEST( fd_url_parse_cstr( url, direct, strlen( direct ), NULL ) );
  FD_TEST( !fd_bam_registry_take_url( ctx, direct, url, 1, 50056, err, sizeof(err) ) );
  FD_TEST( ctx->registry.on ); /* the caller leaves registry mode */
  fd_bam_registry_disable( ctx );
  FD_TEST( !ctx->registry.on );
  FD_TEST( !ctx->tls->ca_store );
}

static int
redirect( char const * raw,
          char *       loc,
          ulong        loc_max ) {
  char  buf[ 256 ];
  ulong len = strlen( raw );
  memcpy( buf, raw, len );
  return fd_bam_registry_http_response( buf, &len, 0, loc, loc_max )==FD_BAM_REGISTRY_HTTP_REDIRECT;
}

static void
test_redirect( void ) {
  char loc[ 64 ];
  FD_TEST( redirect( "HTTP/1.1 302 Found\r\nlocation:  /v2 \r\n\r\n", loc, sizeof(loc) ) );
  FD_TEST( !strcmp( loc, "/v2" ) );
  FD_TEST( !redirect( "HTTP/1.1 302 Found\r\nLocation: /v2\r\n", loc, sizeof(loc) ) );
  FD_TEST( !redirect( "HTTP/1.1 300 Multi\r\nLocation: /v2\r\n\r\n", loc, sizeof(loc) ) );
  FD_TEST( !redirect( "HTTP/1.1 301 Moved\r\n\r\n", loc, sizeof(loc) ) );
  FD_TEST( !redirect( "HTTP/1.1 200 OK\r\nLocation: /v2\r\n\r\n", loc, sizeof(loc) ) );

  static fd_bam_registry_t reg[1];
  memset( reg, 0, sizeof(*reg) );
  strcpy( reg->f_host, "a.example" ); reg->f_host_len = 9;
  strcpy( reg->f_path, "/v1/nodes?x=1" ); reg->f_path_len = 13;
  reg->f_port = 443; reg->f_ssl = 1;

  FD_TEST( !fd_bam_registry_redirect_target( reg, "/root#frag" ) );
  FD_TEST( !strcmp( reg->f_path, "/root" ) && reg->f_path_len==5 && !strcmp( reg->f_host, "a.example" ) );
  FD_TEST( !fd_bam_registry_redirect_target( reg, "https://b.example:8443/n?x=1" ) );
  FD_TEST( !strcmp( reg->f_host, "b.example" ) && reg->f_port==8443 && reg->f_ssl && !strcmp( reg->f_path, "/n?x=1" ) );
  FD_TEST( !fd_bam_registry_redirect_target( reg, "http://c.example" ) );
  FD_TEST( !strcmp( reg->f_host, "c.example" ) && reg->f_port==80 && !reg->f_ssl && !strcmp( reg->f_path, "/" ) );
  FD_TEST( !fd_bam_registry_redirect_target( reg, "http://c.example?y" ) );
  FD_TEST( !strcmp( reg->f_path, "/?y" ) );
  /* Registries answer with absolute URLs or paths; nothing else is
     followed. */
  FD_TEST( fd_bam_registry_redirect_target( reg, "list" )==-1 );
  FD_TEST( fd_bam_registry_redirect_target( reg, "//d.example/n" )==-1 );
  FD_TEST( fd_bam_registry_redirect_target( reg, "ftp://d.example/" )==-1 );
  /* Nor a bad port, which must not log: the failed fetch's warning
     already names the target. */
  FD_TEST( fd_bam_registry_redirect_target( reg, "https://d.example:99999/n" )==-1 );
  FD_TEST( fd_bam_registry_redirect_target( reg, "https://d.example:123456/n" )==-1 );
  FD_TEST( fd_bam_registry_redirect_target( reg, "https://d.example:0/n" )==-1 );
  FD_TEST( fd_bam_registry_redirect_target( reg, "https://d.example:/n" )==-1 );
  FD_TEST( fd_bam_registry_redirect_target( reg, "https://d.example:8x/n" )==-1 );
  FD_TEST( !strcmp( reg->f_host, "c.example" ) && reg->f_port==80 );
  FD_TEST( !fd_bam_registry_redirect_target( reg, "http://d.example:65535/n" ) );
  FD_TEST( !strcmp( reg->f_host, "d.example" ) && reg->f_port==65535 && !reg->f_ssl );
}

static ulong
fetch_total( fd_bam_registry_t const * reg ) {
  ulong total = 0UL;
  for( ulong i=0UL; i<FD_METRICS_ENUM_BAM_REGISTRY_FETCH_RESULT_CNT; i++ ) total += reg->fetch_cnt[ i ];
  return total;
}

/* Fake nameserver answers, by the host's first label. */
#define DNS_HOSTS  (0) /* registry.hosts: /etc/hosts, no query */
#define DNS_ANSWER (1) /* A 127.0.0.1 */
#define DNS_NXDOMAIN (2)
#define DNS_SILENT (3)

static int
memfd_with( char const * data ) {
  int fd = memfd_create( "test_bam_registry", 0 );
  FD_TEST( fd>=0 );
  FD_TEST( (long)strlen( data )==write( fd, data, strlen( data ) ) );
  return fd;
}

/* test_fetch_loopback fetches http://host:port/path from a local
   server that answers /old with a redirect to /v1/nodes and anything
   else with status and body, one request per connection.  Names other
   than 127.0.0.1 resolve through a fake nameserver acting as dns says.
   No step may block.  With waker set the tile parks whenever a step
   reports no progress, and each answer must wake its waker set. */

static void
test_fetch_loopback( char const * host,
                     int          dns,
                     char const * path,
                     char const * status,
                     char const * body,
                     ulong        result,
                     int          waker ) {
  static fd_bam_tile_t ctx[1];
  ctx_init( ctx, 1U, waker );

  ushort port;
  int lfd = listener( 1, &port );
  FD_TEST( fcntl( lfd, F_SETFL, O_NONBLOCK )==0 );

  int ufd = socket( AF_INET, SOCK_DGRAM|SOCK_NONBLOCK, 0 );
  FD_TEST( ufd>=0 );
  struct sockaddr_in uaddr = { .sin_family = AF_INET, .sin_addr.s_addr = FD_IP4_ADDR( 127, 0, 0, 1 ) };
  socklen_t uaddr_sz = sizeof(uaddr);
  FD_TEST( !bind( ufd, fd_type_pun( &uaddr ), uaddr_sz ) );
  FD_TEST( !getsockname( ufd, fd_type_pun( &uaddr ), &uaddr_sz ) );
  fd_adns_ns_port       = fd_ushort_bswap( uaddr.sin_port );
  fd_etc_hosts_fd       = memfd_with( "127.0.0.1 registry.hosts\n" );
  fd_etc_resolv_conf_fd = memfd_with( "nameserver 127.0.0.1\noptions timeout:1 attempts:2\n" );

  char raw[ 96 ];
  FD_TEST( fd_cstr_printf_check( raw, sizeof(raw), NULL, "http://%s:%hu%s", host, port, path ) );
  take( ctx, raw );

  char resp[ 256 ];
  ulong resp_len = 0UL;
  FD_TEST( fd_cstr_printf_check( resp, sizeof(resp), &resp_len, "HTTP/1.0 %s\r\nContent-Length: %lu\r\n\r\n%s", status, strlen( body ), body ) );
  char moved[ 128 ];
  ulong moved_len = 0UL;
  FD_TEST( fd_cstr_printf_check( moved, sizeof(moved), &moved_len, "HTTP/1.1 301 Moved\r\nLocation: http://%s:%hu/v1/nodes\r\n\r\n", host, port ) );

  int   cfd = -1;
  ulong served = 0UL;
  ulong queries = 0UL;
  long  step_max = 0L;
  char  req[ 1024 ];
  ulong req_len = 0UL;
  fd_bam_registry_t * reg = &ctx->registry;
  ulong steps = 0UL;
  ulong busy_steps = 0UL;
  int   busy = 1;
  long  dl   = 0L;
  long  end = fd_log_wallclock()+(long)5e9;
  while( !fetch_total( reg ) && fd_log_wallclock()<end ) {
    if( waker && !busy ) park( (int const[]){ lfd, ufd, cfd }, 3UL, dl );
    busy = 0;
    long t0   = fd_log_wallclock();
    fd_bam_registry_step( ctx, &busy, &dl );
    step_max = fd_long_max( step_max, fd_log_wallclock()-t0 );
    steps++;
    busy_steps += !!busy;
    if( waker && !busy ) FD_TEST( dl>t0 );

    uchar q[ 512 ];
    struct sockaddr_in from;
    socklen_t from_sz = sizeof(from);
    long qn = recvfrom( ufd, q, sizeof(q)-16UL, 0, fd_type_pun( &from ), &from_sz );
    if( qn>=12L ) {
      queries++;
      FD_TEST( q[5]==1 && !(q[2]&0x80) ); /* one question, a query */
      /* The silent case gives up well before FETCH_TIMEOUT. */
      if( dns==DNS_SILENT ) reg->deadline = fd_long_min( reg->deadline, fd_bam_now()+(long)300e6 );
      if( dns==DNS_ANSWER || dns==DNS_NXDOMAIN ) {
        q[2] = (uchar)(q[2]|0x80U);
        q[3] = (uchar)(0x80U|( dns==DNS_NXDOMAIN ? 3U : 0U ));
        if( dns==DNS_ANSWER ) {
          static uchar const rr[] = { 0xC0,0x0C, 0,1, 0,1, 0,0,0,60, 0,4, 127,0,0,1 };
          q[7] = 1; /* ANCOUNT */
          memcpy( q+qn, rr, sizeof(rr) );
          qn += (long)sizeof(rr);
        }
        if( waker ) FD_TEST( !waker_ready( 0 ) );
        FD_TEST( sendto( ufd, q, (ulong)qn, 0, fd_type_pun( &from ), from_sz )==qn );
        if( waker ) FD_TEST( waker_ready( 1000 ) );
      }
    }

    if( cfd<0 ) {
      cfd = accept( lfd, NULL, NULL );
      req_len = 0UL;
      continue;
    }
    long n = recv( cfd, req+req_len, sizeof(req)-1UL-req_len, MSG_DONTWAIT );
    if( n>0 ) req_len += (ulong)n;
    req[ req_len ] = '\0';
    if( !strstr( req, "\r\n\r\n" ) ) continue;
    FD_TEST( !strncmp( req, "GET ", 4UL ) );
    char want[ 96 ];
    FD_TEST( fd_cstr_printf_check( want, sizeof(want), NULL, " HTTP/1.0\r\nHost: %s:%hu\r\n", host, port ) );
    FD_TEST( strstr( req, want ) );
    int old = !strncmp( req+4, "/old ", 5UL );
    FD_TEST( old || !strncmp( req+4, "/v1/nodes ", 10UL ) );
    if( waker ) FD_TEST( !waker_ready( 0 ) ); /* no writable wake once the request is out */
    FD_TEST( send( cfd, old ? moved : resp, old ? moved_len : resp_len, MSG_NOSIGNAL )==(long)( old ? moved_len : resp_len ) );
    if( waker ) FD_TEST( waker_ready( 1000 ) );
    close( cfd );
    cfd = -1;
    served++;
  }
  /* A resolver that blocked would wait out its 1 s timeout (resolv.conf
     above).  Logging inside a step can stall on the log file for
     100+ ms under write load. */
  FD_TEST( step_max<(long)500e6 );
  FD_LOG_INFO(( "fetch %s%s %s: %lu steps, %lu busy", host, path, waker ? "parked" : "polled", steps, busy_steps ));
  if( waker ) FD_TEST( busy_steps<=12UL && steps<=40UL );
  else        FD_TEST( busy_steps==steps );
  int named = !!strcmp( host, "127.0.0.1" );
  int via_dns = named && dns!=DNS_HOSTS;
  FD_TEST( via_dns ? queries>=1UL : !queries );
  FD_TEST( fetch_total( reg )==1UL && reg->fetch_cnt[ result ]==1UL );
  if( result==FD_METRICS_ENUM_BAM_REGISTRY_FETCH_RESULT_V_ACCEPTED_IDX ) {
    FD_TEST( served==( strcmp( path, "/old" ) ? 1UL : 2UL ) );
    FD_TEST( reg->node_cnt==1UL );
    FD_TEST( reg->nodes[0].ip4==FD_IP4_ADDR( 203, 0, 113, 1 ) && reg->nodes[0].grpc_port==50056 );
  } else {
    FD_TEST( !reg->node_cnt );
  }

  fd_bam_registry_disable( ctx );
  FD_TEST( reg->fd<0 );
  close( fd_etc_hosts_fd );
  close( fd_etc_resolv_conf_fd );
  fd_etc_hosts_fd       = -1;
  fd_etc_resolv_conf_fd = -1;
  fd_adns_ns_port       = 53;
  close( ufd );
  close( lfd );
}

static void
test_fetch_refused( void ) {
  static fd_bam_tile_t ctx[1];
  ctx_init( ctx, 2U, 0 );

  /* A port that was just bound and closed refuses connections. */
  ushort port;
  close( listener( 0, &port ) );
  char raw[ 64 ];
  FD_TEST( fd_cstr_printf_check( raw, sizeof(raw), NULL, "http://127.0.0.1:%hu/v1/nodes", port ) );
  take( ctx, raw );

  fd_bam_registry_t * reg = &ctx->registry;
  long end = fd_log_wallclock()+(long)5e9;
  while( !fetch_total( reg ) && fd_log_wallclock()<end ) {
    int  busy = 0;
    long dl   = 0L;
    fd_bam_registry_step( ctx, &busy, &dl );
  }
  ulong const refused = FD_METRICS_ENUM_BAM_REGISTRY_FETCH_RESULT_V_CONNECT_IDX;
  FD_TEST( fetch_total( reg )==1UL && reg->fetch_cnt[ refused ]==1UL );
  FD_TEST( reg->fetch_warn->reason==refused );

  /* The same failure again within a minute does not warn again; a
     minute later it does. */
  long warned_at = reg->fetch_warn->at;
  for( ulong want=2UL; want<=3UL; want++ ) {
    reg->resync_at   = 0L;
    reg->fetch_after = 0L;
    while( fetch_total( reg )<want && fd_log_wallclock()<end ) {
      int  busy = 0;
      long dl   = 0L;
      fd_bam_registry_step( ctx, &busy, &dl );
    }
    FD_TEST( fetch_total( reg )==want && reg->fetch_warn->reason==refused );
    if( want==2UL ) {
      FD_TEST( reg->fetch_warn->at==warned_at );
      reg->fetch_warn->at -= FD_BAM_REGISTRY_WARN_INTERVAL_NS;
    } else {
      FD_TEST( reg->fetch_warn->at>warned_at );
    }
  }
  fd_bam_registry_disable( ctx );
}

/* --- TLS node ------------------------------------------------------- */

/* An Ed25519 test CA, and a leaf it signed for 127.0.0.1 and
   registry.hosts with the private key test_leaf_key, both valid from
   2025 to 2125.  Made with openssl genpkey -algorithm ed25519, then
   req -x509 (basicConstraints critical CA:TRUE, keyUsage critical
   keyCertSign) and x509 -req (keyUsage critical digitalSignature,
   extendedKeyUsage serverAuth, subjectAltName IP:127.0.0.1,
   DNS:registry.hosts). */

static char const test_ca_pem[] =
  "-----BEGIN CERTIFICATE-----\n"
  "MIIBODCB66ADAgECAgEBMAUGAytlcDAjMSEwHwYDVQQDDBhGaXJlQkFNIHJlZ2lz\n"
  "dHJ5IHRlc3QgQ0EwIBcNMjUwMTAxMDAwMDAwWhgPMjEyNTAxMDEwMDAwMDBaMCMx\n"
  "ITAfBgNVBAMMGEZpcmVCQU0gcmVnaXN0cnkgdGVzdCBDQTAqMAUGAytlcAMhALub\n"
  "LmujA2bfikQ5GtUsHtjOd0Dg2pY5TD4h4UO+qCggo0IwQDAPBgNVHRMBAf8EBTAD\n"
  "AQH/MA4GA1UdDwEB/wQEAwICBDAdBgNVHQ4EFgQUyGllqhbCYJekqEyKdZ8mzJdc\n"
  "B3gwBQYDK2VwA0EAXPE/T4imodSocdZCCNFirVeiz2sHYdgFbTcYS8gz208KfLh3\n"
  "2BnMxyprO9RxwhPT95Htf8JvkDP0dQewk0m4BA==\n"
  "-----END CERTIFICATE-----\n";

static uchar const test_leaf_der[ 326 ] = {
  0x30,0x82,0x01,0x42,0x30,0x81,0xf5,0xa0,0x03,0x02,0x01,0x02,0x02,0x01,0x02,0x30,
  0x05,0x06,0x03,0x2b,0x65,0x70,0x30,0x23,0x31,0x21,0x30,0x1f,0x06,0x03,0x55,0x04,
  0x03,0x0c,0x18,0x46,0x69,0x72,0x65,0x42,0x41,0x4d,0x20,0x72,0x65,0x67,0x69,0x73,
  0x74,0x72,0x79,0x20,0x74,0x65,0x73,0x74,0x20,0x43,0x41,0x30,0x20,0x17,0x0d,0x32,
  0x35,0x30,0x31,0x30,0x31,0x30,0x30,0x30,0x30,0x30,0x30,0x5a,0x18,0x0f,0x32,0x31,
  0x32,0x35,0x30,0x31,0x30,0x31,0x30,0x30,0x30,0x30,0x30,0x30,0x5a,0x30,0x19,0x31,
  0x17,0x30,0x15,0x06,0x03,0x55,0x04,0x03,0x0c,0x0e,0x72,0x65,0x67,0x69,0x73,0x74,
  0x72,0x79,0x2e,0x68,0x6f,0x73,0x74,0x73,0x30,0x2a,0x30,0x05,0x06,0x03,0x2b,0x65,
  0x70,0x03,0x21,0x00,0x82,0xce,0x1c,0x2a,0xb5,0xaa,0x87,0x30,0xec,0xf9,0xe4,0xe5,
  0x3c,0x3c,0xdb,0xc4,0x6e,0x37,0x2b,0x87,0xcc,0x96,0xaf,0xda,0x83,0xb4,0x78,0x63,
  0x32,0xbe,0x79,0x43,0xa3,0x56,0x30,0x54,0x30,0x0c,0x06,0x03,0x55,0x1d,0x13,0x01,
  0x01,0xff,0x04,0x02,0x30,0x00,0x30,0x0e,0x06,0x03,0x55,0x1d,0x0f,0x01,0x01,0xff,
  0x04,0x04,0x03,0x02,0x07,0x80,0x30,0x13,0x06,0x03,0x55,0x1d,0x25,0x04,0x0c,0x30,
  0x0a,0x06,0x08,0x2b,0x06,0x01,0x05,0x05,0x07,0x03,0x01,0x30,0x1f,0x06,0x03,0x55,
  0x1d,0x11,0x04,0x18,0x30,0x16,0x87,0x04,0x7f,0x00,0x00,0x01,0x82,0x0e,0x72,0x65,
  0x67,0x69,0x73,0x74,0x72,0x79,0x2e,0x68,0x6f,0x73,0x74,0x73,0x30,0x05,0x06,0x03,
  0x2b,0x65,0x70,0x03,0x41,0x00,0x92,0x43,0x7c,0x17,0xd2,0xcb,0x94,0x18,0xaf,0x35,
  0x79,0xbf,0xe8,0x0e,0x88,0x69,0x15,0xed,0xa7,0x02,0x5b,0xa7,0x47,0x62,0x57,0x74,
  0x9d,0xaf,0xe2,0x39,0x5d,0xf8,0xd0,0x61,0x7d,0xef,0x18,0xaa,0x2d,0x12,0x2f,0x01,
  0x70,0x8f,0xf7,0xda,0x8a,0xeb,0xdd,0xd9,0x4f,0xcc,0x9c,0xb2,0x7c,0xa5,0x9c,0x6f,
  0x4e,0x76,0x03,0x59,0x10,0x03,
};

static uchar const test_leaf_key[ 32 ] = {
  0x1f,0x84,0x07,0xd6,0x8d,0xdb,0xff,0x0e,0xab,0x37,0x05,0xec,0xe1,0x72,0x1e,0x02,
  0x66,0xe0,0x41,0xdb,0xdb,0x23,0x1b,0xf0,0x67,0x76,0xc9,0x0b,0xc9,0xeb,0xb2,0xc1,
};

/* tls_init sets up the registry's TLS client as the tile's init does.
   With trusted set, ctx->ca_store holds just the test CA; without it
   the store ctx_init fills trusts nothing. */

static void
tls_init( fd_bam_tile_t * ctx,
          int             trusted ) {
  fd_tls_t * tls = fd_tls_join( fd_tls_new( ctx->registry.tls ) );
  FD_TEST( tls );
  uchar key[ FD_CHACHA_KEY_SZ ] = {1};
  fd_chacha_rng_init( ctx->registry.rng, key, FD_CHACHA_RNG_ALGO_CHACHA8 );
  tls->rng = ctx->registry.rng;
  if( !trusted ) return;
  int  fd = memfd_with( test_ca_pem );
  char path[ 32 ];
  FD_TEST( fd_cstr_printf_check( path, sizeof(path), NULL, "/proc/self/fd/%d", fd ) );
  FD_TEST( fd_x509_ca_store_load( ctx->ca_store, path )==1L );
  close( fd );
}

/* test_node_t serves TLS with the test leaf, one connection at a time:
   HTTP/1.0 answers the first request with resp and closes, H2_SILENT
   settles HTTP/2 settings and never answers a call, H2_ANSWER answers
   each call with an empty message and grpc-status 0, delay_ns after
   it.  calls counts the requests it received. */

#define NODE_HTTP      (0)
#define NODE_H2_SILENT (1)
#define NODE_H2_ANSWER (2)

typedef struct {
  int                    lfd;
  int                    fd;
  int                    mode;
  int                    preface;
  char const *           resp;
  long                   delay_ns;    /* H2_ANSWER: answer this long after a call */
  uint                   pending_sid; /* call to answer at due, 0 if none */
  long                   due;
  ulong                  calls;
  ulong                  rx_len;
  uchar                  rx[ 4096 ];
  fd_tls_t               tls[1];
  fd_tls_test_sign_ctx_t sign[1];
  fd_chacha_rng_t        chacha[1];
  fd_tlsrec_conn_t       conn[1];
  fd_tlsrec_sock_t       sock[1];
} test_node_t;

static void
node_init( test_node_t * n,
           int           mode,
           ushort *      port ) {
  static fd_rng_t _rng[1];
  fd_rng_t * rng = fd_rng_join( fd_rng_new( _rng, 17U, 0UL ) );
  memset( n, 0, sizeof(*n) );
  n->lfd  = listener( 1, port );
  n->fd   = -1;
  n->mode = mode;
  FD_TEST( fcntl( n->lfd, F_SETFL, O_NONBLOCK )==0 );
  FD_TEST( fd_sha512_join( fd_sha512_new( n->sign->sha512 ) ) );
  memcpy( n->sign->private_key, test_leaf_key, 32UL );
  fd_ed25519_public_from_private( n->sign->public_key, n->sign->private_key, n->sign->sha512 );
  n->tls->rng  = fd_tls_test_rand( n->chacha, rng );
  n->tls->sign = fd_tls_test_sign( n->sign );
  for( ulong i=0UL; i<32UL; i++ ) n->tls->kex_private_key[ i ] = fd_rng_uchar( rng );
  fd_x25519_public( n->tls->kex_public_key, n->tls->kex_private_key );
  memcpy( n->tls->cert_public_key, n->sign->public_key, 32UL );
  memcpy( n->tls->cert_x509, test_leaf_der, sizeof(test_leaf_der) );
  n->tls->cert_x509_sz = sizeof(test_leaf_der);
  char const * alpn = mode==NODE_HTTP ? "\x08http/1.1" : "\x02h2";
  n->tls->alpn_sz = strlen( alpn );
  memcpy( n->tls->alpn, alpn, n->tls->alpn_sz );
  fd_rng_delete( fd_rng_leave( rng ) );
}

static void
node_drop( test_node_t * n ) {
  if( n->fd>=0 ) close( n->fd );
  n->fd = -1;
}

static void
node_fini( test_node_t * n ) {
  node_drop( n );
  close( n->lfd );
}

static void
node_send( test_node_t * n,
           void const *  data,
           ulong         sz ) {
  uchar const * p = data;
  while( sz ) {
    if( fd_tlsrec_sock_tx_pending( n->sock ) ) {
      FD_TEST( fd_tlsrec_sock_flush( n->sock, n->fd )>=0 );
      continue;
    }
    ulong done = 0UL;
    FD_TEST( !fd_tlsrec_sock_tx( n->sock, n->conn, n->fd, p, sz, &done ) );
    p  += done;
    sz -= done;
  }
}

static void
node_frame( test_node_t * n,
            uint          type,
            uint          flags,
            uint          stream_id,
            void const *  payload,
            ulong         payload_sz ) {
  uchar f[ 9+64 ];
  FD_TEST( payload_sz<=64UL );
  f[0] = (uchar)(payload_sz>>16); f[1] = (uchar)(payload_sz>>8); f[2] = (uchar)payload_sz;
  f[3] = (uchar)type;
  f[4] = (uchar)flags;
  FD_STORE( uint, f+5, fd_uint_bswap( stream_id ) );
  if( payload_sz ) memcpy( f+9, payload, payload_sz );
  node_send( n, f, 9UL+payload_sz );
}

static void
node_answer( test_node_t * n,
             uint          sid ) {
  static uchar const hdrs[] = { 0x88, 0x00, 12, 'c','o','n','t','e','n','t','-','t','y','p','e',
                                16, 'a','p','p','l','i','c','a','t','i','o','n','/','g','r','p','c' };
  static uchar const msg[5] = {0};
  static uchar const trailers[] = { 0x00, 11, 'g','r','p','c','-','s','t','a','t','u','s', 1, '0' };
  node_frame( n, 1U, 4U, sid, hdrs, sizeof(hdrs) );         /* HEADERS, END_HEADERS */
  node_frame( n, 0U, 0U, sid, msg, sizeof(msg) );           /* DATA: one empty message */
  node_frame( n, 1U, 5U, sid, trailers, sizeof(trailers) ); /* HEADERS, END_HEADERS|END_STREAM */
}

static void
node_h2( test_node_t * n ) {
  ulong off = 0UL;
  if( !n->preface ) {
    if( n->rx_len<24UL ) return;
    FD_TEST( !memcmp( n->rx, "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n", 24UL ) );
    n->preface = 1;
    off = 24UL;
    node_frame( n, 4U, 0U, 0U, NULL, 0UL ); /* SETTINGS */
  }
  while( n->rx_len-off>=9UL ) {
    uchar const * f   = n->rx+off;
    ulong         len = (ulong)f[0]<<16 | (ulong)f[1]<<8 | (ulong)f[2];
    if( n->rx_len-off<9UL+len ) break;
    uint sid = fd_uint_bswap( FD_LOAD( uint, f+5 ) ) & 0x7fffffffU;
    if( f[3]==4U && !( f[4]&1U ) ) node_frame( n, 4U, 1U, 0U, NULL, 0UL ); /* SETTINGS ACK */
    if( f[3]<=1U && ( f[4]&1U ) && sid ) { /* DATA or HEADERS ending a request */
      n->calls++;
      if( n->mode==NODE_H2_ANSWER ) {
        FD_TEST( !n->pending_sid ); /* a probe sends one call at a time */
        n->pending_sid = sid;
        n->due         = fd_bam_now()+n->delay_ns;
      }
    }
    off += 9UL+len;
  }
  memmove( n->rx, n->rx+off, n->rx_len-off );
  n->rx_len -= off;
}

/* node_step accepts, handshakes and answers without blocking. */

static void
node_step( test_node_t * n ) {
  if( n->fd<0 ) {
    int fd = accept( n->lfd, NULL, NULL );
    if( fd<0 ) return;
    FD_TEST( fcntl( fd, F_SETFL, O_NONBLOCK )==0 );
    int nodelay = 1; /* as gRPC servers do; else Nagle holds each reply for an ACK */
    FD_TEST( !setsockopt( fd, SOL_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay) ) );
    n->fd      = fd;
    n->preface = 0;
    n->rx_len  = 0UL;
    n->pending_sid = 0U;
    fd_tlsrec_sock_init( n->sock );
    FD_TEST( fd_tlsrec_conn_init( n->conn, n->tls, 1 )==n->conn );
  }
  if( fd_tlsrec_sock_tx_pending( n->sock ) && fd_tlsrec_sock_flush( n->sock, n->fd )<0 ) {
    node_drop( n );
    return;
  }
  if( fd_tlsrec_sock_rx( n->sock, n->conn, n->fd, NULL ) ) {
    node_drop( n );
    return;
  }
  n->rx_len += fd_tlsrec_sock_rx_pop( n->sock, n->rx+n->rx_len, sizeof(n->rx)-n->rx_len );
  if( !fd_tlsrec_conn_is_ready( n->conn ) ) return;
  if( n->mode!=NODE_HTTP ) {
    node_h2( n );
    if( n->pending_sid && fd_bam_now()>=n->due ) {
      node_answer( n, n->pending_sid );
      n->pending_sid = 0U;
    }
  } else if( !n->calls && memmem( n->rx, n->rx_len, "\r\n\r\n", 4UL ) ) {
    n->calls++;
    node_send( n, n->resp, strlen( n->resp ) );
    FD_TEST( fd_tlsrec_sock_close( n->sock, n->conn, n->fd )>=0 );
  }
}

/* test_fetch_tls fetches https://host:port/v1/nodes from a TLS node.
   The fetch is rejected unless the store trusts the test CA and host
   is a name the leaf carries; /etc/hosts maps other.hosts there too. */

static void
test_fetch_tls( char const * host,
                int          trusted,
                int          waker ) {
  static fd_bam_tile_t ctx[1];
  ctx_init( ctx, 4U, waker );
  tls_init( ctx, trusted );
  fd_etc_hosts_fd = memfd_with( "127.0.0.1 registry.hosts other.hosts\n" );
  static test_node_t node[1];
  ushort port;
  node_init( node, NODE_HTTP, &port );
  char const * body = "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":50056,\"region\":\"a\"}]}";
  char resp[ 256 ];
  FD_TEST( fd_cstr_printf_check( resp, sizeof(resp), NULL, "HTTP/1.0 200 OK\r\nContent-Length: %lu\r\n\r\n%s", strlen( body ), body ) );
  node->resp = resp;
  char raw[ 64 ];
  FD_TEST( fd_cstr_printf_check( raw, sizeof(raw), NULL, "https://%s:%hu/v1/nodes", host, port ) );
  take( ctx, raw );

  fd_bam_registry_t * reg = &ctx->registry;
  ulong steps = 0UL;
  ulong busy_steps = 0UL;
  int   busy = 1;
  long  dl   = 0L;
  long  end  = fd_log_wallclock()+(long)5e9;
  while( !fetch_total( reg ) && fd_log_wallclock()<end ) {
    if( waker && !busy ) park( (int const[]){ node->lfd, node->fd }, 2UL, dl );
    busy = 0;
    long t1 = fd_bam_now();
    fd_bam_registry_step( ctx, &busy, &dl );
    if( waker && !busy ) FD_TEST( dl>t1 );
    steps++;
    busy_steps += !!busy;
    node_step( node );
  }
  FD_LOG_INFO(( "fetch over TLS from %s %s %s: %lu steps, %lu busy", host, trusted ? "trusted" : "untrusted",
                waker ? "parked" : "polled", steps, busy_steps ));
  if( waker ) FD_TEST( busy_steps<=12UL && steps<=40UL );
  else        FD_TEST( busy_steps==steps );
  FD_TEST( fetch_total( reg )==1UL );
  if( trusted && strcmp( host, "other.hosts" ) ) {
    FD_TEST( reg->fetch_cnt[ FD_METRICS_ENUM_BAM_REGISTRY_FETCH_RESULT_V_ACCEPTED_IDX ]==1UL );
    FD_TEST( node->calls==1UL && reg->node_cnt==1UL );
    FD_TEST( reg->nodes[0].ip4==FD_IP4_ADDR( 203, 0, 113, 1 ) && reg->nodes[0].grpc_port==50056 );
  } else {
    char why[ 64 ];
    FD_TEST( fd_cstr_printf_check( why, sizeof(why), NULL, "TLS handshake with %s failed", host ) );
    FD_TEST( reg->fetch_cnt[ FD_METRICS_ENUM_BAM_REGISTRY_FETCH_RESULT_V_CERT_REJECTED_IDX ]==1UL );
    FD_TEST( !node->calls && !reg->node_cnt );
    FD_TEST( strstr( reg->why, why ) );
  }
  fd_bam_registry_disable( ctx );
  node_fini( node );
  close( fd_etc_hosts_fd );
  fd_etc_hosts_fd = -1;
}

/* test_probe_tls probes one TLS node: it answers each sample, after
   delay_ms, answers none, or presents a certificate the store does not
   trust.  With waker set the tile parks whenever a step reports no
   progress. */

static void
test_probe_tls( int  mode,
                int  trusted,
                int  waker,
                long delay_ms ) {
  static fd_bam_tile_t ctx[1];
  ctx_init( ctx, 5U, waker );
  tls_init( ctx, trusted );
  fd_bam_registry_t * reg = &ctx->registry;
  ulong align = fd_bam_registry_probe_align();
  void * mem  = aligned_alloc( align, fd_ulong_align_up( fd_bam_registry_probe_footprint(), align ) );
  FD_TEST( mem );
  reg->probe_mem = mem;
  fd_bam_registry_join_probe( ctx, 7UL );

  static test_node_t node[1];
  fd_bam_registry_node_t nodes[1];
  memset( nodes, 0, sizeof(nodes) );
  node_init( node, mode, &nodes[0].grpc_port );
  node->delay_ns = delay_ms*(long)1e6;
  nodes[0].ip4 = FD_IP4_ADDR( 127, 0, 0, 1 );
  take( ctx, "http://127.0.0.1:1/v1/nodes" );
  fd_bam_registry_on_fetch( reg, nodes, 1UL, fd_bam_now(), 0, ctx->rng ); /* next fetch in 2 s */

  int  busy = 0;
  long dl   = 0L;
  long t0   = fd_bam_now();
  fd_bam_registry_step( ctx, &busy, &dl );
  FD_TEST( reg->phase==2 && reg->probe_active );
  ulong steps = 0UL;
  ulong busy_steps = 0UL;
  long  end = fd_log_wallclock()+(long)3e9;
  while( reg->phase==2 && fd_log_wallclock()<end ) {
    if( waker && !busy ) park( (int const[]){ node->lfd, node->fd }, 2UL, node->pending_sid ? fd_long_min( dl, node->due ) : dl );
    busy = 0;
    long t1 = fd_bam_now();
    fd_bam_registry_step( ctx, &busy, &dl );
    if( waker && !busy ) FD_TEST( dl>t1 );
    steps++;
    busy_steps += !!busy;
    node_step( node );
  }
  long took = fd_bam_now()-t0;
  ulong rtt_us = reg->ranked_cnt ? reg->ranked[0].rtt_us : 0UL;
  FD_LOG_INFO(( "probe over TLS mode %d %s %s, %ld ms delay: %lu steps, %lu busy, %ld us, rtt %lu us", mode,
                trusted ? "trusted" : "untrusted", waker ? "parked" : "polled", delay_ms, steps, busy_steps, took/1000L, rtt_us ));
  FD_TEST( !reg->phase && !reg->probe_active );
  if( waker ) FD_TEST( busy_steps<=8UL && steps<=24UL );
  else        FD_TEST( busy_steps==steps );
  ulong const usable = FD_METRICS_ENUM_BAM_REGISTRY_PROBE_NODE_RESULT_V_USABLE_IDX;
  ulong const silent = FD_METRICS_ENUM_BAM_REGISTRY_PROBE_NODE_RESULT_V_UNANSWERED_IDX;
  if( !trusted ) {
    FD_TEST( reg->probe_cert_cnt==1UL && !node->calls );
    FD_TEST( !reg->ranked_cnt && reg->last_round[ silent ]==1UL );
  } else if( mode==NODE_H2_ANSWER ) {
    FD_TEST( !reg->probe_cert_cnt && node->calls==FD_BAM_REGISTRY_PROBE_SAMPLES );
    FD_TEST( reg->ranked_cnt==1UL && reg->last_round[ usable ]==1UL );
    FD_TEST( reg->ranked[0].node.grpc_port==nodes[0].grpc_port && rtt_us<=FD_BAM_REGISTRY_MAX_RTT_US );
    FD_TEST( rtt_us>=(ulong)delay_ms*1000UL );
  } else {
    /* Each sample waits out its request deadline. */
    FD_TEST( !reg->probe_cert_cnt && node->calls==FD_BAM_REGISTRY_PROBE_SAMPLES );
    FD_TEST( !reg->ranked_cnt && reg->last_round[ silent ]==1UL );
    FD_TEST( took>=(long)FD_BAM_REGISTRY_PROBE_SAMPLES*FD_BAM_REGISTRY_PROBE_REQUEST_NS );
  }
  fd_bam_registry_disable( ctx );
  node_fini( node );
  free( mem );
}

/* test_probe_round probes a refused port and one that accepts TCP but
   never answers TLS.  Disabling BAM mid-round idles the follower;
   enabling it reruns the round, which settles with no usable node,
   warns, and dials nothing.  With waker set the rerun parks between
   steps: the refusal and the connect wake the waker set, and the
   silent node's connect deadline ends the round. */

static void
test_probe_round( int waker ) {
  static fd_bam_tile_t ctx[1];
  ctx_init( ctx, 3U, waker );
  fd_bam_registry_t * reg = &ctx->registry;
  ulong align = fd_bam_registry_probe_align();
  ulong fp    = fd_bam_registry_probe_footprint();
  FD_TEST( fp>=FD_BAM_REGISTRY_PROBE_FANOUT*sizeof(fd_bam_registry_slot_t) );
  void * mem  = aligned_alloc( align, fd_ulong_align_up( fp, align ) );
  FD_TEST( mem );
  reg->probe_mem = mem;
  fd_bam_registry_join_probe( ctx, 7UL );
  FD_TEST( reg->slot_cnt==FD_BAM_REGISTRY_PROBE_FANOUT );
  for( ulong i=0UL; i<reg->slot_cnt; i++ ) {
    FD_TEST( reg->slots[ i ].fd==-1 && reg->slots[ i ].grpc && reg->slots[ i ].best_us==ULONG_MAX );
  }

  fd_bam_registry_node_t nodes[2];
  memset( nodes, 0, sizeof(nodes) );
  int lfd[2];
  for( ulong i=0UL; i<2UL; i++ ) {
    lfd[ i ]      = listener( 1, &nodes[ i ].grpc_port );
    nodes[ i ].ip4 = FD_IP4_ADDR( 127, 0, 0, 1 );
  }
  close( lfd[0] ); /* refused */

  take( ctx, "http://127.0.0.1:1/v1/nodes" );
  fd_bam_registry_on_fetch( reg, nodes, 2UL, fd_bam_now(), 0, ctx->rng ); /* next fetch in 2 s */

  int  busy = 0;
  long dl   = 0L;
  fd_bam_registry_step( ctx, &busy, &dl );
  FD_TEST( reg->phase==2 && reg->probe_active );
  for( ulong i=0UL; i<64UL; i++ ) fd_bam_registry_step( ctx, &busy, &dl );
  FD_TEST( reg->phase==2 );

  ctx->enabled = 0U;
  for( ulong i=0UL; i<64UL; i++ ) {
    fd_bam_registry_step( ctx, &busy, &dl );
    FD_TEST( reg->paused && !reg->phase && !reg->probe_active && reg->fd<0 && dl==LONG_MAX );
  }
  FD_TEST( !reg->ranked_cnt && !reg->probe_at && !reg->have_dial );
  for( ulong i=0UL; i<reg->slot_cnt; i++ ) FD_TEST( reg->slots[ i ].fd<0 );

  reg->stalled_ns = 1L;
  ctx->enabled = 1U;
  fd_bam_registry_step( ctx, &busy, &dl );
  FD_TEST( !reg->paused && !reg->stalled_ns );
  FD_TEST( reg->phase==2 && reg->probe_active );
  /* A connecting slot waits to become writable; on loopback the
     connect finishes before the tile would park, so check directly. */
  for( ulong i=0UL; waker && i<reg->slot_cnt; i++ ) {
    fd_bam_registry_slot_t const * s = &reg->slots[ i ];
    if( s->fd>=0 ) FD_TEST( s->ep==( s->io ? 1 : 2 ) );
  }

  ulong steps = 0UL;
  ulong busy_steps = 0UL;
  long  end = fd_log_wallclock()+(long)3e9;
  while( reg->phase==2 && fd_log_wallclock()<end ) {
    if( waker && !busy ) {
      long t0 = fd_bam_now();
      FD_TEST( dl>t0 );
      park( NULL, 0UL, dl );
    }
    busy = 0;
    fd_bam_registry_step( ctx, &busy, &dl );
    steps++;
    busy_steps += !!busy;
  }
  FD_LOG_INFO(( "probe round %s: %lu steps, %lu busy", waker ? "parked" : "polled", steps, busy_steps ));
  if( waker ) FD_TEST( busy_steps<=8UL && steps<=24UL );
  else        FD_TEST( busy_steps==steps );
  FD_TEST( !reg->phase && !reg->probe_active );
  FD_TEST( reg->round_cnt[ FD_METRICS_ENUM_BAM_REGISTRY_PROBE_ROUND_RESULT_V_SETTLED_IDX ]==1UL );
  FD_TEST( reg->last_round[ FD_METRICS_ENUM_BAM_REGISTRY_PROBE_NODE_RESULT_V_UNANSWERED_IDX ]==2UL );
  FD_TEST( !reg->ranked_cnt && reg->probe_warn->reason==1UL );
  FD_TEST( reg->probe_at>fd_bam_now()+FD_BAM_REGISTRY_PROBE_COOLDOWN_NS-(long)5e9 );
  fd_bam_registry_step( ctx, &busy, &dl );
  FD_TEST( !reg->have_dial && !reg->phase );

  fd_bam_registry_disable( ctx );
  close( lfd[1] );
  free( mem );
}

static void
test_near_leader( void ) {
  ulong const g = FD_BAM_REGISTRY_LEADER_GUARD_SLOTS;
  FD_TEST( !fd_bam_registry_near_leader( ULONG_MAX, 1000UL ) );
  FD_TEST( !fd_bam_registry_near_leader( 1000UL+g+1UL, 1000UL ) );
  FD_TEST(  fd_bam_registry_near_leader( 1000UL+g,     1000UL ) );
  FD_TEST(  fd_bam_registry_near_leader( 1000UL,       1000UL+g ) );
  FD_TEST( !fd_bam_registry_near_leader( 1000UL,       1000UL+g+1UL ) ); /* stale hint */
  FD_TEST( !fd_bam_registry_near_leader( 1000UL,       0UL ) ); /* no slot yet */
  FD_TEST(  fd_bam_registry_near_leader( ULONG_MAX-1UL, ULONG_MAX-1UL ) );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );
  /* test_fetch_loopback bounds how long a registry step takes.  Do not
     also fsync the log file on every warning a step logs. */
  fd_log_level_flush_set( 4 );
  fd_waker_install( 1UL );
  test_parse();
  test_http();
  test_rank_and_choose();
  test_take_url();
  test_redirect();
  char const * one = "{\"generated_at\":\"2026-01-01T00:00:00Z\",\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":50056,\"region\":\"a\"}]}";
  ulong const ok    = FD_METRICS_ENUM_BAM_REGISTRY_FETCH_RESULT_V_ACCEPTED_IDX;
  ulong const nodns = FD_METRICS_ENUM_BAM_REGISTRY_FETCH_RESULT_V_RESOLVE_IDX;
  /* The performance layout polls; the efficient one parks. */
  for( int waker=0; waker<2; waker++ ) {
    test_fetch_loopback( "registry.hosts", DNS_HOSTS,    "/v1/nodes", "200 OK", one, ok, waker );
    test_fetch_loopback( "registry.dns",   DNS_ANSWER,   "/old",      "200 OK", one, ok, waker );
    test_fetch_loopback( "registry.dns",   DNS_NXDOMAIN, "/v1/nodes", "200 OK", one, nodns, waker );
    test_fetch_loopback( "registry.dns",   DNS_SILENT,   "/v1/nodes", "200 OK", one, nodns, waker );
    test_fetch_loopback( "127.0.0.1",      DNS_HOSTS,    "/v1/nodes", "200 OK", "{\"nodes\":[]}", FD_METRICS_ENUM_BAM_REGISTRY_FETCH_RESULT_V_EMPTY_LIST_IDX, waker );
    test_fetch_loopback( "127.0.0.1",      DNS_HOSTS,    "/v1/nodes", "200 OK", "{\"nodes\":", FD_METRICS_ENUM_BAM_REGISTRY_FETCH_RESULT_V_JSON_IDX, waker );
    test_fetch_loopback( "127.0.0.1",      DNS_HOSTS,    "/v1/nodes", "503 Service Unavailable", one, FD_METRICS_ENUM_BAM_REGISTRY_FETCH_RESULT_V_HTTP_IDX, waker );
  }
  test_fetch_refused();
  for( int waker=0; waker<2; waker++ ) {
    test_fetch_tls( "registry.hosts", 1, waker );
    test_fetch_tls( "registry.hosts", 0, waker );
  }
  test_fetch_tls( "127.0.0.1",   1, 0 ); /* the IP SAN, as probes check */
  test_fetch_tls( "other.hosts", 1, 0 ); /* a name the leaf lacks */
  test_probe_tls( NODE_H2_ANSWER, 1, 0,  0L );
  test_probe_tls( NODE_H2_ANSWER, 1, 0, 10L );
  test_probe_tls( NODE_H2_SILENT, 1, 0,  0L );
  test_probe_tls( NODE_H2_ANSWER, 0, 0,  0L );
  test_probe_round( 0 );
  test_probe_round( 1 );
  test_near_leader();
  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
