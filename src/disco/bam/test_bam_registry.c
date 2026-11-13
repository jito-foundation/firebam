#include "fd_bam_tile_private.h"
#include "fd_bam_registry.h"
#include "../../ballet/x509/fd_x509_verify.h"
#include "../../util/fd_util.h"
#include "../../util/net/fd_ip4.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/socket.h>

static int
is_registry( char const * raw ) {
  fd_url_t url[1];
  if( !fd_url_parse_cstr( url, raw, strlen( raw ), NULL ) ) return 0;
  return fd_bam_registry_is_url( url );
}

static void
test_url_class( void ) {
  FD_TEST( is_registry( "https://dev.testnet.registry.bam.jito.wtf/v1/nodes" ) );
  FD_TEST( is_registry( "https://registry.jito.wtf/nodes" ) );
  FD_TEST( !is_registry( "https://203.0.113.1:50056" ) );
  FD_TEST( !is_registry( "https://203.0.113.1:50056/" ) );
  FD_TEST( !is_registry( "https://203.0.113.1:50056?x=1" ) );
}

static void
test_parse( void ) {
  fd_bam_registry_node_t nodes[ 8 ];
  ulong cnt = 0UL;
  char const * golden =
    "{\"generated_at\":\"2026-09-09T12:34:56Z\",\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":50056,\"region\":\"fra\"}]}";
  FD_TEST( !fd_bam_registry_parse( golden, strlen( golden ), nodes, 8UL, &cnt ) );
  FD_TEST( cnt==1UL );
  FD_TEST( nodes[0].grpc_port==50056 );
  FD_TEST( !strcmp( nodes[0].region, "fra" ) );
  FD_TEST( nodes[0].ip4==(203U | (0U<<8) | (113U<<16) | (1U<<24)) );

  char const * bad_ts =
    "{\"generated_at\":\"not-a-timestamp\",\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":50056,\"region\":\"fra\",\"added_later\":true}],\"extra\":1}";
  FD_TEST( !fd_bam_registry_parse( bad_ts, strlen( bad_ts ), nodes, 8UL, &cnt ) );
  FD_TEST( cnt==1UL );

  char const * dup =
    "{\"nodes\":[{\"region\":\"a\",\"grpc_port\":50056,\"ip\":\"203.0.113.1\"},{\"ip\":\"203.0.113.9\",\"grpc_port\":50056,\"region\":\"b\"},{\"ip\":\"203.0.113.1\",\"grpc_port\":50056,\"region\":\"c\"}]}";
  FD_TEST( !fd_bam_registry_parse( dup, strlen( dup ), nodes, 8UL, &cnt ) );
  FD_TEST( cnt==2UL );
  FD_TEST( nodes[0].ip4==(203U | (0U<<8) | (113U<<16) | (1U<<24)) );
  FD_TEST( nodes[1].ip4==(203U | (0U<<8) | (113U<<16) | (9U<<24)) );

  char const * empty = "{\"nodes\":[]}";
  FD_TEST( fd_bam_registry_parse( empty, strlen( empty ), nodes, 8UL, &cnt )==-2 );
  char const * junk = "{";
  FD_TEST( fd_bam_registry_parse( junk, strlen( junk ), nodes, 8UL, &cnt )==-1 );
  char const * v6 =
    "{\"nodes\":[{\"ip\":\"2001:db8::1\",\"grpc_port\":50056,\"region\":\"fra\"}]}";
  FD_TEST( fd_bam_registry_parse( v6, strlen( v6 ), nodes, 8UL, &cnt )==-2 );
  char const * mixed =
    "{\"nodes\":[{\"ip\":\"2001:db8::1\",\"grpc_port\":50056,\"region\":\"fra\"},{\"ip\":\"203.0.113.9\",\"grpc_port\":50056,\"region\":\"ams\"}]}";
  FD_TEST( !fd_bam_registry_parse( mixed, strlen( mixed ), nodes, 8UL, &cnt ) );
  FD_TEST( cnt==1UL );
  FD_TEST( nodes[0].ip4==(203U | (0U<<8) | (113U<<16) | (9U<<24)) );
  char const * long_key =
    "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":50056,\"region\":\"fra\",\"this_key_is_longer_than_thirty_one_chars\":1}]}";
  FD_TEST( !fd_bam_registry_parse( long_key, strlen( long_key ), nodes, 8UL, &cnt ) );
  FD_TEST( cnt==1UL );
}

/* Each case is what serde_json does with jito's ServedNodes. */

static int
parses( char const * json ) {
  fd_bam_registry_node_t nodes[ 8 ];
  ulong cnt = 0UL;
  return !fd_bam_registry_parse( json, strlen( json ), nodes, 8UL, &cnt );
}

static void
test_parse_serde( void ) {
  fd_bam_registry_node_t nodes[ 8 ];
  ulong cnt = 0UL;

  /* port 0 is a valid u16; the probe drops it later */
  FD_TEST( parses( "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":0,\"region\":\"fra\"}]}" ) );
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":65536,\"region\":\"fra\"}]}" ) );
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":050056,\"region\":\"fra\"}]}" ) );

  /* repeated known fields */
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"ip\":\"203.0.113.2\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );
  FD_TEST( !parses( "{\"nodes\":[],\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );
  FD_TEST( !parses( "{\"generated_at\":null,\"generated_at\":null,\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );
  FD_TEST( parses( "{\"x\":1,\"x\":2,\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );

  /* lenient generated_at: scalars pass, containers fail */
  FD_TEST( parses( "{\"generated_at\":7,\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );
  FD_TEST( parses( "{\"generated_at\":true,\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );
  FD_TEST( !parses( "{\"generated_at\":{},\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );
  FD_TEST( !parses( "{\"generated_at\":[],\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );

  /* escapes */
  char const * esc = "{\"nodes\":[{\"\\u0069p\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\\u00e9\\ud83d\\ude00\\b\\f\\r\"}]}";
  FD_TEST( !fd_bam_registry_parse( esc, strlen( esc ), nodes, 8UL, &cnt ) );
  FD_TEST( !strcmp( nodes[0].region, "a\\u00e9\\ud83d\\ude00\\b\\f\\r" ) ); /* logged as sent */
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"\\ud83d\"}]}" ) );
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"\\ude00\"}]}" ) );
  /* serde_json ignores these in skipped values; fd_jtok does not, and
     serde_json never writes them. */
  FD_TEST( !parses( "{\"skip\":\"\\ud83d\",\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );
  FD_TEST( !parses( "{\"skip\":\"\\x\",\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );
  FD_TEST( !parses( "{\"skip\":01,\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );

  /* strict UTF-8 */
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"\xc0\xaf\"}]}" ) );
  FD_TEST( !parses( "{\"skip\":\"\xff\",\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );

  /* a long region is kept, truncated */
  char const * long_region =
    "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"0123456789012345678901234567890123456789012345678901234567890123456789\"}]}";
  FD_TEST( !fd_bam_registry_parse( long_region, strlen( long_region ), nodes, 8UL, &cnt ) );
  FD_TEST( strlen( nodes[0].region )==sizeof(nodes[0].region)-1UL );
  /* ... at a character boundary: 62 bytes, then a 2-byte character */
  char const * long_utf8 =
    "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"01234567890123456789012345678901234567890123456789012345678901\xc3\xa9z\"}]}";
  FD_TEST( !fd_bam_registry_parse( long_utf8, strlen( long_utf8 ), nodes, 8UL, &cnt ) );
  FD_TEST( strlen( nodes[0].region )==62UL );

  /* every field is required, even on a skipped IPv6 node */
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1}]}" ) );
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"2001:db8::1\",\"region\":\"a\"},{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );
  FD_TEST( !parses( "{\"generated_at\":null}" ) );
  FD_TEST( !parses( "[]" ) );
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]} x" ) );
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":1.0,\"region\":\"a\"}]}" ) );
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":-1,\"region\":\"a\"}]}" ) );
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":1,\"grpc_port\":1,\"region\":\"a\"}]}" ) );

  /* IpAddr: invalid IPv6 fails the document, valid IPv6 is skipped */
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"zz:zz\",\"grpc_port\":1,\"region\":\"a\"},{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );
  FD_TEST( !parses( "{\"nodes\":[{\"ip\":\"203.0.113.01\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );
  FD_TEST( parses( "{\"nodes\":[{\"ip\":\"::ffff:1.2.3.4\",\"grpc_port\":1,\"region\":\"a\"},{\"ip\":\"203.0.113.1\",\"grpc_port\":1,\"region\":\"a\"}]}" ) );

  /* sorted by octets, not by the in-memory word */
  char const * order =
    "{\"nodes\":[{\"ip\":\"2.0.0.1\",\"grpc_port\":1,\"region\":\"a\"},{\"ip\":\"1.0.0.2\",\"grpc_port\":1,\"region\":\"b\"}]}";
  FD_TEST( !fd_bam_registry_parse( order, strlen( order ), nodes, 8UL, &cnt ) );
  FD_TEST( cnt==2UL && !strcmp( nodes[0].region, "b" ) );
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
  FD_TEST( http( "HTTP/1.1 503 Busy\r\n\r\n", 1, body, &n )==-1 );

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
  reg->stalled_since = now-FD_BAM_REGISTRY_CONNECT_GRACE_NS;
  fd_bam_registry_node_t out;
  FD_TEST( fd_bam_registry_choose( reg, 0, now, &out ) );
  FD_TEST( out.ip4==nodes[1].ip4 );

  reg->ranked_cnt = 1UL;
  reg->ranked[0].node = nodes[0];
  reg->cursor = 0UL;
  reg->stalled_since = now-FD_BAM_REGISTRY_CONNECT_GRACE_NS;
  FD_TEST( !fd_bam_registry_choose( reg, 0, now, &out ) );
  FD_TEST( !reg->ranked_cnt );
  FD_TEST( reg->stalled_since==now-FD_BAM_REGISTRY_CONNECT_GRACE_NS );

  /* Still stuck after the re-probe: a new best publishes at once. */
  reg->ranked_cnt = 1UL;
  reg->ranked[0].node = nodes[1];
  reg->cursor = 0UL;
  FD_TEST( fd_bam_registry_choose( reg, 0, now+1000000000L, &out ) );
  FD_TEST( out.ip4==nodes[1].ip4 );

  /* Healthy resets the clock; nothing to do. */
  reg->ranked[0].node = nodes[0];
  FD_TEST( !fd_bam_registry_choose( reg, 1, now+2000000000L, &out ) );
  FD_TEST( reg->stalled_since==now+2000000000L );

  /* Healthy but delisted: move to the best ranked node, or hold the
     live connection when nothing ranked. */
  reg->dial_ip4 = FD_IP4_ADDR( 198, 51, 100, 9 );
  FD_TEST( fd_bam_registry_choose( reg, 1, now+3000000000L, &out ) );
  FD_TEST( out.ip4==nodes[0].ip4 && out.grpc_port==nodes[0].grpc_port );
  reg->ranked_cnt = 0UL;
  FD_TEST( !fd_bam_registry_choose( reg, 1, now+3000000000L, &out ) );
}

static void
test_reject_bad_cert( void ) {
  /* Same verifier fd_tls runs when ca_store is set.  A registry session
     always sets it, so a malformed or untrusted chain fails the handshake
     with FD_TLS_REASON_CERT_VERIFY. */
  static fd_x509_ca_store_t store;
  memset( &store, 0, sizeof(store) );
  uchar msg[ 8 ] = {0};
  FD_TEST( fd_x509_verify_tls_cert_msg( msg, sizeof(msg), &store, "203.0.113.1", 11UL, 1700000000L )!=FD_X509_VERIFY_OK );
  FD_TEST( FD_TLS_REASON_CERT_VERIFY==607 );
}

static void
test_take_url( void ) {
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
  FD_TEST( ctx->registry.stalled_since );
  FD_TEST( ctx->tls->ca_store==ctx->ca_store );

  char const * reg_port = "https://registry.example:8443/v1/nodes";
  FD_TEST( fd_url_parse_cstr( url, reg_port, strlen( reg_port ), NULL ) );
  FD_TEST( fd_bam_registry_take_url( ctx, reg_port, url, 1, 8443, err, sizeof(err) )==1 );
  FD_TEST( ctx->registry.port==8443 );

  char const * direct = "https://203.0.113.1:50056";
  FD_TEST( fd_url_parse_cstr( url, direct, strlen( direct ), NULL ) );
  FD_TEST( !fd_bam_registry_take_url( ctx, direct, url, 1, 50056, err, sizeof(err) ) );
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
  FD_TEST( fd_bam_registry_redirect_target( reg, "?y=2" )==-1 );
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

/* test_fetch_loopback fetches path from a local server that answers
   /old with a redirect to /v1/nodes and anything else with status and
   body, one request per connection. */

static void
test_fetch_loopback( char const * path,
                     char const * status,
                     char const * body,
                     ulong        result ) {
  static fd_bam_tile_t ctx[1];
  memset( ctx, 0, sizeof(*ctx) );
  ctx->registry.fd = -1;
  FD_TEST( fd_rng_join( fd_rng_new( ctx->rng, 1U, 0UL ) ) );
  ctx->ca_store->cnt = 1UL;

  int lfd = socket( AF_INET, SOCK_STREAM, 0 );
  FD_TEST( lfd>=0 );
  struct sockaddr_in addr = { .sin_family = AF_INET, .sin_addr.s_addr = FD_IP4_ADDR( 127, 0, 0, 1 ) };
  socklen_t addr_sz = sizeof(addr);
  FD_TEST( !bind( lfd, fd_type_pun( &addr ), addr_sz ) );
  FD_TEST( !listen( lfd, 4 ) );
  FD_TEST( !getsockname( lfd, fd_type_pun( &addr ), &addr_sz ) );
  FD_TEST( fcntl( lfd, F_SETFL, O_NONBLOCK )==0 );
  ushort port = fd_ushort_bswap( addr.sin_port );

  char raw[ 64 ];
  FD_TEST( fd_cstr_printf_check( raw, sizeof(raw), NULL, "http://127.0.0.1:%hu%s", port, path ) );
  fd_url_t url[1];
  ushort   url_port = 0;
  _Bool    ssl      = 0;
  FD_TEST( !fd_url_parse_endpoint( url, raw, strlen( raw ), &url_port, &ssl, NULL ) );
  char err[ 128 ];
  FD_TEST( fd_bam_registry_take_url( ctx, raw, url, ssl, url_port, err, sizeof(err) )==1 );

  char resp[ 256 ];
  ulong resp_len = 0UL;
  FD_TEST( fd_cstr_printf_check( resp, sizeof(resp), &resp_len, "HTTP/1.0 %s\r\nContent-Length: %lu\r\n\r\n%s", status, strlen( body ), body ) );
  char moved[ 128 ];
  ulong moved_len = 0UL;
  FD_TEST( fd_cstr_printf_check( moved, sizeof(moved), &moved_len, "HTTP/1.1 301 Moved\r\nLocation: http://127.0.0.1:%hu/v1/nodes\r\n\r\n", port ) );

  /* Answer each request, then close. */
  int   cfd = -1;
  ulong served = 0UL;
  char  req[ 1024 ];
  ulong req_len = 0UL;
  fd_bam_registry_t * reg = &ctx->registry;
  long  end = fd_log_wallclock()+(long)5e9;
  while( !fetch_total( reg ) && fd_log_wallclock()<end ) {
    int  busy = 0;
    long dl   = 0L;
    fd_bam_registry_step( ctx, &busy, &dl );
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
    FD_TEST( strstr( req, " HTTP/1.0\r\nHost: 127.0.0.1:" ) );
    int old = !strncmp( req+4, "/old ", 5UL );
    FD_TEST( old || !strncmp( req+4, "/v1/nodes ", 10UL ) );
    FD_TEST( send( cfd, old ? moved : resp, old ? moved_len : resp_len, MSG_NOSIGNAL )==(long)( old ? moved_len : resp_len ) );
    close( cfd );
    cfd = -1;
    served++;
  }
  FD_TEST( served==( strcmp( path, "/old" ) ? 1UL : 2UL ) );
  FD_TEST( fetch_total( reg )==1UL && reg->fetch_cnt[ result ]==1UL );
  if( result==FD_METRICS_ENUM_BAM_REGISTRY_FETCH_RESULT_V_ACCEPTED_IDX ) {
    FD_TEST( reg->node_cnt==1UL );
    FD_TEST( reg->nodes[0].ip4==FD_IP4_ADDR( 203, 0, 113, 1 ) && reg->nodes[0].grpc_port==50056 );
  } else {
    FD_TEST( !reg->node_cnt );
  }

  fd_bam_registry_disable( ctx );
  close( lfd );
}

static void
test_fetch_refused( void ) {
  static fd_bam_tile_t ctx[1];
  memset( ctx, 0, sizeof(*ctx) );
  ctx->registry.fd = -1;
  FD_TEST( fd_rng_join( fd_rng_new( ctx->rng, 2U, 0UL ) ) );
  ctx->ca_store->cnt = 1UL;

  /* A port that was just bound and closed refuses connections. */
  int lfd = socket( AF_INET, SOCK_STREAM, 0 );
  FD_TEST( lfd>=0 );
  struct sockaddr_in addr = { .sin_family = AF_INET, .sin_addr.s_addr = FD_IP4_ADDR( 127, 0, 0, 1 ) };
  socklen_t addr_sz = sizeof(addr);
  FD_TEST( !bind( lfd, fd_type_pun( &addr ), addr_sz ) );
  FD_TEST( !getsockname( lfd, fd_type_pun( &addr ), &addr_sz ) );
  close( lfd );

  char raw[ 64 ];
  FD_TEST( fd_cstr_printf_check( raw, sizeof(raw), NULL, "http://127.0.0.1:%hu/v1/nodes", fd_ushort_bswap( addr.sin_port ) ) );
  fd_url_t url[1];
  ushort   port = 0;
  _Bool    ssl  = 0;
  FD_TEST( !fd_url_parse_endpoint( url, raw, strlen( raw ), &port, &ssl, NULL ) );
  char err[ 128 ];
  FD_TEST( fd_bam_registry_take_url( ctx, raw, url, ssl, port, err, sizeof(err) )==1 );

  fd_bam_registry_t * reg = &ctx->registry;
  long end = fd_log_wallclock()+(long)5e9;
  while( !fetch_total( reg ) && fd_log_wallclock()<end ) {
    int  busy = 0;
    long dl   = 0L;
    fd_bam_registry_step( ctx, &busy, &dl );
  }
  FD_TEST( fetch_total( reg )==1UL && reg->fetch_cnt[ FD_METRICS_ENUM_BAM_REGISTRY_FETCH_RESULT_V_CONNECT_IDX ]==1UL );
  fd_bam_registry_disable( ctx );
}

static void
test_probe_join( void ) {
  static fd_bam_tile_t ctx[1];
  memset( ctx, 0, sizeof(*ctx) );
  ctx->registry.fd = -1;
  ulong align = fd_bam_registry_probe_align();
  ulong fp    = fd_bam_registry_probe_footprint();
  FD_TEST( fd_ulong_is_pow2( align ) );
  FD_TEST( fp>=FD_BAM_REGISTRY_PROBE_FANOUT*sizeof(fd_bam_registry_slot_t) );
  void * mem = aligned_alloc( align, fd_ulong_align_up( fp, align ) );
  FD_TEST( mem );
  ctx->registry.probe_mem = mem;
  fd_bam_registry_join_probe( ctx, 7UL );
  FD_TEST( ctx->registry.slot_cnt==FD_BAM_REGISTRY_PROBE_FANOUT );
  for( ulong i=0UL; i<ctx->registry.slot_cnt; i++ ) {
    fd_bam_registry_slot_t const * s = &ctx->registry.slots[ i ];
    FD_TEST( s->fd==-1 && s->grpc && s->best_us==ULONG_MAX );
    FD_TEST( (ulong)s->grpc+1UL<=(ulong)mem+fp );
  }
  fd_bam_registry_disable( ctx );
  FD_TEST( !ctx->registry.probe_active );
  free( mem );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );
  test_url_class();
  test_parse();
  test_parse_serde();
  test_http();
  test_rank_and_choose();
  test_reject_bad_cert();
  test_take_url();
  test_redirect();
  char const * one = "{\"generated_at\":\"2026-01-01T00:00:00Z\",\"nodes\":[{\"ip\":\"203.0.113.1\",\"grpc_port\":50056,\"region\":\"a\"}]}";
  test_fetch_loopback( "/v1/nodes", "200 OK", one, FD_METRICS_ENUM_BAM_REGISTRY_FETCH_RESULT_V_ACCEPTED_IDX );
  test_fetch_loopback( "/old", "200 OK", one, FD_METRICS_ENUM_BAM_REGISTRY_FETCH_RESULT_V_ACCEPTED_IDX );
  test_fetch_loopback( "/v1/nodes", "200 OK", "{\"nodes\":[]}", FD_METRICS_ENUM_BAM_REGISTRY_FETCH_RESULT_V_EMPTY_LIST_IDX );
  test_fetch_loopback( "/v1/nodes", "200 OK", "{\"nodes\":", FD_METRICS_ENUM_BAM_REGISTRY_FETCH_RESULT_V_JSON_IDX );
  test_fetch_loopback( "/v1/nodes", "503 Service Unavailable", one, FD_METRICS_ENUM_BAM_REGISTRY_FETCH_RESULT_V_HTTP_IDX );
  test_fetch_refused();
  test_probe_join();
  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
