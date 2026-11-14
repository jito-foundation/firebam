#define _GNU_SOURCE /* memfd_create */
#include "fd_bam_tile_private.h"
#include "fd_bam_registry.h"
#include "../../util/fd_util.h"
#include "../../util/net/fd_ip4.h"
#include "../../waltz/resolv/fd_adns.h"
#include "../../waltz/resolv/fd_lookup.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/mman.h>
#include <sys/socket.h>

static int
is_registry( char const * raw ) {
  fd_url_t url[1];
  if( !fd_url_parse_cstr( url, raw, strlen( raw ), NULL ) ) return 0;
  return fd_bam_registry_is_url( url );
}

/* ctx_init readies a tile with a system CA bundle and BAM enabled. */

static void
ctx_init( fd_bam_tile_t * ctx,
          uint            seed ) {
  memset( ctx, 0, sizeof(*ctx) );
  ctx->registry.fd = -1;
  FD_TEST( fd_rng_join( fd_rng_new( ctx->rng, seed, 0UL ) ) );
  ctx->ca_store->cnt = 1UL;
  ctx->enabled       = 1U;
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
  int rc = fd_bam_registry_http_body( buf, &len, eof );
  if( rc==1 ) { memcpy( out, buf, len ); out[ len ] = '\0'; *out_len = len; }
  return rc;
}

static void
test_http( void ) {
  char  body[ 256 ];
  ulong n = 0UL;
  FD_TEST( http( "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}", 0, body, &n )==1 && !strcmp( body, "{}" ) );
  FD_TEST( http( "HTTP/1.1 203 OK\r\ncontent-length:2\r\n\r\n{}", 0, body, &n )==1 );
  FD_TEST( http( "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n{}", 0, body, &n )==0 );
  FD_TEST( http( "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n{}", 1, body, &n )==-1 );
  FD_TEST( http( "HTTP/1.1 200 OK\r\n\r\n{}", 1, body, &n )==1 && n==2UL );
  FD_TEST( http( "HTTP/1.1 301 Moved\r\nLocation: /x\r\n\r\n", 1, body, &n )==-1 );
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
  memset( ctx, 0, sizeof(*ctx) );
  ctx->registry.fd = -1;
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
  return fd_bam_registry_http_redirect( raw, strlen( raw ), loc, loc_max );
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
  FD_TEST( !strcmp( reg->f_host, "c.example" ) );
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
   No step may block. */

static void
test_fetch_loopback( char const * host,
                     int          dns,
                     char const * path,
                     char const * status,
                     char const * body,
                     ulong        result ) {
  static fd_bam_tile_t ctx[1];
  ctx_init( ctx, 1U );

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
  long  end = fd_log_wallclock()+(long)5e9;
  while( !fetch_total( reg ) && fd_log_wallclock()<end ) {
    int  busy = 0;
    long dl   = 0L;
    long t0   = fd_log_wallclock();
    fd_bam_registry_step( ctx, &busy, &dl );
    step_max = fd_long_max( step_max, fd_log_wallclock()-t0 );

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
        FD_TEST( sendto( ufd, q, (ulong)qn, 0, fd_type_pun( &from ), from_sz )==qn );
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
    FD_TEST( send( cfd, old ? moved : resp, old ? moved_len : resp_len, MSG_NOSIGNAL )==(long)( old ? moved_len : resp_len ) );
    close( cfd );
    cfd = -1;
    served++;
  }
  FD_TEST( step_max<(long)50e6 );
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
  ctx_init( ctx, 2U );

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

/* test_probe_round probes a refused port and one that accepts TCP but
   never answers TLS.  Disabling BAM mid-round idles the follower;
   enabling it reruns the round, which settles with no usable node,
   warns, and dials nothing. */

static void
test_probe_round( void ) {
  static fd_bam_tile_t ctx[1];
  ctx_init( ctx, 3U );
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

  long end = fd_log_wallclock()+(long)3e9;
  while( reg->phase==2 && fd_log_wallclock()<end ) fd_bam_registry_step( ctx, &busy, &dl );
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
  test_parse();
  test_http();
  test_rank_and_choose();
  test_take_url();
  test_redirect();
  char const * one = "{\"generated_at\":\"2026-01-01T00:00:00Z\",\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":50056,\"region\":\"a\"}]}";
  ulong const ok    = FD_METRICS_ENUM_BAM_REGISTRY_FETCH_RESULT_V_ACCEPTED_IDX;
  ulong const nodns = FD_METRICS_ENUM_BAM_REGISTRY_FETCH_RESULT_V_RESOLVE_IDX;
  test_fetch_loopback( "registry.hosts", DNS_HOSTS,    "/v1/nodes", "200 OK", one, ok );
  test_fetch_loopback( "registry.dns",   DNS_ANSWER,   "/old",      "200 OK", one, ok );
  test_fetch_loopback( "registry.dns",   DNS_NXDOMAIN, "/v1/nodes", "200 OK", one, nodns );
  test_fetch_loopback( "registry.dns",   DNS_SILENT,   "/v1/nodes", "200 OK", one, nodns );
  test_fetch_loopback( "127.0.0.1",      DNS_HOSTS,    "/v1/nodes", "200 OK", "{\"nodes\":[]}", FD_METRICS_ENUM_BAM_REGISTRY_FETCH_RESULT_V_EMPTY_LIST_IDX );
  test_fetch_loopback( "127.0.0.1",      DNS_HOSTS,    "/v1/nodes", "200 OK", "{\"nodes\":", FD_METRICS_ENUM_BAM_REGISTRY_FETCH_RESULT_V_JSON_IDX );
  test_fetch_loopback( "127.0.0.1",      DNS_HOSTS,    "/v1/nodes", "503 Service Unavailable", one, FD_METRICS_ENUM_BAM_REGISTRY_FETCH_RESULT_V_HTTP_IDX );
  test_fetch_refused();
  test_probe_round();
  test_near_leader();
  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
