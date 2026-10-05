#include "fd_grpc_codec.h"
#include "../h2/fd_h2_rbuf.h"
#include "../h2/fd_hpack.h"
#include "../../util/tmpl/fd_unit_test.c"

FD_UNIT_TEST( h2_gen_request_hdr ) {
  fd_grpc_req_hdrs_t req = {
    .https    = 1,
    .host     = "example.org",
    .host_len = 11,
    .port     = 443,
    .path     = "/auth.AuthService/GenerateAuthChallenge",
    .path_len = 39,
  };
  uchar buf[ 2048 ];
  fd_h2_rbuf_t rbuf_tx[1];
  fd_h2_rbuf_init( rbuf_tx, buf, sizeof(buf) );
  FD_TEST( fd_grpc_h2_gen_request_hdrs( &req, rbuf_tx, "1.2.3", 5 )==1 );
  FD_TEST( rbuf_tx->lo_off==0 && rbuf_tx->lo==buf );
# define EXPECT_HDR( nam, val )                                        \
  do {                                                                 \
    FD_TEST( !fd_hpack_rd_done( hpack_rd ) );                          \
    FD_TEST( !fd_hpack_rd_next( hpack_rd, hdr, &scratch, 0UL ) );      \
    FD_TEST( hdr->name_len==sizeof(nam)-1 );                           \
    FD_TEST( fd_memeq( hdr->name, nam, sizeof(nam)-1 ) );              \
    FD_TEST( hdr->value_len==sizeof(val)-1 );                          \
    FD_TEST( fd_memeq( hdr->value, val, sizeof(val)-1 ) );             \
  } while(0)

  fd_hpack_rd_t hpack_rd[1];
  fd_hpack_rd_init( hpack_rd, buf, rbuf_tx->hi_off );
  fd_h2_hdr_t hdr[1];
  uchar * scratch = NULL;
  EXPECT_HDR( ":method", "POST" );
  EXPECT_HDR( ":scheme", "https" );
  EXPECT_HDR( ":path", "/auth.AuthService/GenerateAuthChallenge" );
  EXPECT_HDR( ":authority", "example.org:443" );
  EXPECT_HDR( "te", "trailers" );
  EXPECT_HDR( "content-type", "application/grpc+proto" );
  EXPECT_HDR( "user-agent", "grpc-firedancer/1.2.3" );
  FD_TEST( fd_hpack_rd_done( hpack_rd ) );

  char const example_jwt[] = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0.KMUFsIDTnFmyG3nMiGM6H9FNFUROf3wh7SmqJp-QV30";
  fd_grpc_req_hdrs_t req2 = {
    .https           = 1,
    .host            = "example.org",
    .host_len        = 11,
    .port            = 443,
    .path            = "/block_engine.BlockEngineValidator/SubscribePackets",
    .path_len        = 51,
    .bearer_auth     = example_jwt, /* example invalid JWT */
    .bearer_auth_len = sizeof(example_jwt)-1,
  };
  fd_h2_rbuf_init( rbuf_tx, buf, sizeof(buf) );
  FD_TEST( fd_grpc_h2_gen_request_hdrs( &req2, rbuf_tx, "1.2.3", 5 )==1 );
  FD_TEST( rbuf_tx->lo_off==0 && rbuf_tx->lo==buf );

  fd_hpack_rd_init( hpack_rd, buf, rbuf_tx->hi_off );
  EXPECT_HDR( ":method", "POST" );
  EXPECT_HDR( ":scheme", "https" );
  EXPECT_HDR( ":path", "/block_engine.BlockEngineValidator/SubscribePackets" );
  EXPECT_HDR( ":authority", "example.org:443" );
  EXPECT_HDR( "te", "trailers" );
  EXPECT_HDR( "content-type", "application/grpc+proto" );
  EXPECT_HDR( "user-agent", "grpc-firedancer/1.2.3" );
  FD_TEST( !fd_hpack_rd_done( hpack_rd ) );
  FD_TEST( !fd_hpack_rd_next( hpack_rd, hdr, &scratch, 0UL ) );
  FD_TEST( hdr->name_len==13 );
  FD_TEST( fd_memeq( hdr->name, "authorization", 13 ) );
  FD_TEST( hdr->value_len==7+req2.bearer_auth_len );
  FD_TEST( fd_memeq( hdr->value,   "Bearer ",        7                    ) );
  FD_TEST( fd_memeq( hdr->value+7, req2.bearer_auth, req2.bearer_auth_len ) );
  FD_TEST( fd_hpack_rd_done( hpack_rd ) );

# undef EXPECT_HDR
}

/* Build an HPACK literal-without-indexing header (new name).
   Returns number of bytes written. */
static ulong
hpack_literal( uchar * out, char const * name, ulong name_len,
                             char const * val,  ulong val_len ) {
  uchar * p = out;
  *p++ = 0x00;
  FD_TEST( name_len<127UL );
  *p++ = (uchar)name_len;
  fd_memcpy( p, name, name_len ); p += name_len;
  *p++ = (uchar)fd_ulong_min( val_len, 127UL );
  if( val_len>=127UL ) {
    ulong remain = val_len-127UL;
    while( remain>=128UL ) { *p++ = (uchar)((remain&127UL)|128UL); remain >>= 7; }
    *p++ = (uchar)remain;
  }
  fd_memcpy( p, val, val_len );   p += val_len;
  return (ulong)(p - out);
}

FD_UNIT_TEST( read_response_hdrs ) {
  fd_h2_hdr_matcher_t matcher[1];
  FD_TEST( fd_h2_hdr_matcher_init( matcher, 1UL )==matcher );
  fd_h2_hdr_matcher_insert_literal( matcher, FD_GRPC_HDR_STATUS,  "grpc-status"  );
  fd_h2_hdr_matcher_insert_literal( matcher, FD_GRPC_HDR_MESSAGE, "grpc-message" );

  /* Helper: parse HPACK payload into resp_hdrs */
# define PARSE( payload, payload_sz ) do {                            \
    memset( &resp, 0, sizeof(resp) );                                 \
    resp.grpc_status = FD_GRPC_STATUS_UNKNOWN;                        \
    rc = fd_grpc_h2_read_response_hdrs( &resp, matcher,               \
                                        (payload), (payload_sz) );    \
  } while(0)

  fd_grpc_resp_hdrs_t resp;
  int rc;
  uchar buf[ 256 ];
  ulong off;

  /* ---- Valid cases ---- */

  /* :status: 200 via indexed representation (static table index 8) */
  { uchar hpack[] = { 0x88 };
    PARSE( hpack, sizeof(hpack) );
    FD_TEST( rc==FD_H2_SUCCESS );
    FD_TEST( resp.h2_status==200 ); }

  /* :status: 200 + grpc-status: 0 */
  { off = 0;
    uchar indexed_200[] = { 0x88 };
    fd_memcpy( buf, indexed_200, 1 ); off += 1;
    off += hpack_literal( buf+off, "grpc-status", 11, "0", 1 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_SUCCESS );
    FD_TEST( resp.h2_status==200 );
    FD_TEST( resp.grpc_status==FD_GRPC_STATUS_OK ); }

  /* :status: 200 + grpc-status: 16 (UNAUTHENTICATED, max valid) */
  { off = 0;
    buf[off++] = 0x88;
    off += hpack_literal( buf+off, "grpc-status", 11, "16", 2 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_SUCCESS );
    FD_TEST( resp.grpc_status==FD_GRPC_STATUS_UNAUTHENTICATED ); }

  /* :status: 100 (lowest valid HTTP status) */
  { off = 0;
    off += hpack_literal( buf, ":status", 7, "100", 3 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_SUCCESS );
    FD_TEST( resp.h2_status==100 ); }

  /* :status: 599 */
  { off = 0;
    off += hpack_literal( buf, ":status", 7, "599", 3 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_SUCCESS );
    FD_TEST( resp.h2_status==599 ); }

  /* :status: 600 (valid per http crate, non-standard but accepted) */
  { off = 0;
    off += hpack_literal( buf, ":status", 7, "600", 3 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_SUCCESS );
    FD_TEST( resp.h2_status==600 ); }

  /* :status: 999 (highest 3-digit HTTP status) */
  { off = 0;
    off += hpack_literal( buf, ":status", 7, "999", 3 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_SUCCESS );
    FD_TEST( resp.h2_status==999 ); }

  /* No :status or grpc-status headers => success with defaults */
  { uchar empty[] = "";
    PARSE( empty, 0 );
    FD_TEST( rc==FD_H2_SUCCESS );
    FD_TEST( resp.h2_status==0 );
    FD_TEST( resp.grpc_status==FD_GRPC_STATUS_UNKNOWN ); }

  /* grpc-message preserved on success */
  { off = 0;
    buf[off++] = 0x88;
    off += hpack_literal( buf+off, "grpc-status", 11, "2", 1 );
    off += hpack_literal( buf+off, "grpc-message", 12, "something broke", 15 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_SUCCESS );
    FD_TEST( resp.grpc_msg_len==15 );
    FD_TEST( fd_memeq( resp.grpc_msg, "something broke", 15 ) ); }

  /* ---- content-type cases ---- */

  /* content-type: application/grpc => is_grpc_proto==1 */
  { off = 0;
    buf[off++] = 0x88;
    off += hpack_literal( buf+off, "content-type", 12, "application/grpc", 16 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_SUCCESS );
    FD_TEST( resp.is_grpc_proto==1 ); }

  /* content-type: application/grpc+proto => is_grpc_proto==1 */
  { off = 0;
    buf[off++] = 0x88;
    off += hpack_literal( buf+off, "content-type", 12, "application/grpc+proto", 22 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_SUCCESS );
    FD_TEST( resp.is_grpc_proto==1 ); }

  /* content-type: application/grpc+json => is_grpc_proto==0 (not supported) */
  { off = 0;
    buf[off++] = 0x88;
    off += hpack_literal( buf+off, "content-type", 12, "application/grpc+json", 21 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_SUCCESS );
    FD_TEST( resp.is_grpc_proto==0 ); }

  /* content-type: text/plain => is_grpc_proto==0 */
  { off = 0;
    buf[off++] = 0x88;
    off += hpack_literal( buf+off, "content-type", 12, "text/plain", 10 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_SUCCESS );
    FD_TEST( resp.is_grpc_proto==0 ); }

  /* content-type: application/grpc; charset=utf-8 => is_grpc_proto==0 (params rejected) */
  { off = 0;
    buf[off++] = 0x88;
    off += hpack_literal( buf+off, "content-type", 12, "application/grpc; charset=utf-8", sizeof("application/grpc; charset=utf-8")-1 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_SUCCESS );
    FD_TEST( resp.is_grpc_proto==0 ); }

  /* no content-type header => is_grpc_proto==0 (default from memset) */
  { off = 0;
    buf[off++] = 0x88;
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_SUCCESS );
    FD_TEST( resp.is_grpc_proto==0 ); }

  /* ---- h2_status rejection cases ---- */

  /* :status: 0 (below 100) */
  { off = hpack_literal( buf, ":status", 7, "0", 1 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_ERR_PROTOCOL ); }

  /* :status: 99 (below 100) */
  { off = hpack_literal( buf, ":status", 7, "99", 2 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_ERR_PROTOCOL ); }

  /* :status: (empty) */
  { off = hpack_literal( buf, ":status", 7, "", 0 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_ERR_PROTOCOL ); }

  /* :status: OK (non-numeric) */
  { off = hpack_literal( buf, ":status", 7, "OK", 2 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_ERR_PROTOCOL ); }

  /* :status: 0x1F4 (hex, must be rejected under strict decimal parsing) */
  { off = hpack_literal( buf, ":status", 7, "0x1F4", 5 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_ERR_PROTOCOL ); }

  /* :status: 0310 (4 digits, rejected per HTTP/2 3-digit requirement) */
  { off = hpack_literal( buf, ":status", 7, "0310", 4 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_ERR_PROTOCOL ); }

  /* :status: 0200 (non-canonical encoding of 200, must be rejected) */
  { off = hpack_literal( buf, ":status", 7, "0200", 4 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_ERR_PROTOCOL ); }

  /* :status: 20 (2 digits, too short) */
  { off = hpack_literal( buf, ":status", 7, "20", 2 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_ERR_PROTOCOL ); }

  /* :status: 2000 (4 digits, too long) */
  { off = hpack_literal( buf, ":status", 7, "2000", 4 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_ERR_PROTOCOL ); }

  /* :status: 200abc (trailing junk) */
  { off = hpack_literal( buf, ":status", 7, "200abc", 6 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_ERR_PROTOCOL ); }

  /* ---- grpc_status unknown/malformed cases (mapped to UNKNOWN, matching tonic) ---- */

  /* grpc-status: 17 (above UNAUTHENTICATED=16, mapped to UNKNOWN) */
  { off = 0;
    buf[off++] = 0x88;
    off += hpack_literal( buf+off, "grpc-status", 11, "17", 2 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_SUCCESS );
    FD_TEST( resp.grpc_status==FD_GRPC_STATUS_UNKNOWN ); }

  /* grpc-status: 99 (non-standard, mapped to UNKNOWN) */
  { off = 0;
    buf[off++] = 0x88;
    off += hpack_literal( buf+off, "grpc-status", 11, "99", 2 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_SUCCESS );
    FD_TEST( resp.grpc_status==FD_GRPC_STATUS_UNKNOWN ); }

  /* grpc-status: (empty, mapped to UNKNOWN) */
  { off = 0;
    buf[off++] = 0x88;
    off += hpack_literal( buf+off, "grpc-status", 11, "", 0 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_SUCCESS );
    FD_TEST( resp.grpc_status==FD_GRPC_STATUS_UNKNOWN ); }

  /* grpc-status: OK (non-numeric, mapped to UNKNOWN) */
  { off = 0;
    buf[off++] = 0x88;
    off += hpack_literal( buf+off, "grpc-status", 11, "OK", 2 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_SUCCESS );
    FD_TEST( resp.grpc_status==FD_GRPC_STATUS_UNKNOWN ); }

  /* grpc-status: 0x10 (hex, mapped to UNKNOWN) */
  { off = 0;
    buf[off++] = 0x88;
    off += hpack_literal( buf+off, "grpc-status", 11, "0x10", 4 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_SUCCESS );
    FD_TEST( resp.grpc_status==FD_GRPC_STATUS_UNKNOWN ); }

  /* grpc-status: -1 (negative, mapped to UNKNOWN) */
  { off = 0;
    buf[off++] = 0x88;
    off += hpack_literal( buf+off, "grpc-status", 11, "-1", 2 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_SUCCESS );
    FD_TEST( resp.grpc_status==FD_GRPC_STATUS_UNKNOWN ); }

  /* grpc-status: 999999 (large number, mapped to UNKNOWN) */
  { off = 0;
    buf[off++] = 0x88;
    off += hpack_literal( buf+off, "grpc-status", 11, "999999", 6 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_SUCCESS );
    FD_TEST( resp.grpc_status==FD_GRPC_STATUS_UNKNOWN ); }

  /* :status: " 200" (leading whitespace, strtoul would accept) */
  { off = hpack_literal( buf, ":status", 7, " 200", 4 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_ERR_PROTOCOL ); }

  /* :status: +200 (leading plus sign, strtoul would accept) */
  { off = hpack_literal( buf, ":status", 7, "+200", 4 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_ERR_PROTOCOL ); }

  /* grpc-status: " 0" violates generic HTTP field validity. */
  { off = 0;
    buf[off++] = 0x88;
    off += hpack_literal( buf+off, "grpc-status", 11, " 0", 2 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_ERR_PROTOCOL );
    FD_TEST( resp.grpc_status==FD_GRPC_STATUS_UNKNOWN ); }

  /* grpc-status: +0 (leading plus, mapped to UNKNOWN) */
  { off = 0;
    buf[off++] = 0x88;
    off += hpack_literal( buf+off, "grpc-status", 11, "+0", 2 );
    PARSE( buf, off );
    FD_TEST( rc==FD_H2_SUCCESS );
    FD_TEST( resp.grpc_status==FD_GRPC_STATUS_UNKNOWN ); }

  /* Corrupt HPACK payload */
  { uchar corrupt[] = { 0xff, 0xff, 0xff };
    PARSE( corrupt, sizeof(corrupt) );
    FD_TEST( rc==FD_H2_ERR_COMPRESSION ); }

# undef PARSE
}

FD_UNIT_TEST( compression_error_overrides_semantic_error ) {
  fd_h2_hdr_matcher_t matcher[1];
  fd_h2_hdr_matcher_init( matcher, 1UL );
  uchar block[] = {0x08,3,'x','y','z',0x80};
  fd_grpc_resp_hdrs_t resp = {.grpc_status=FD_GRPC_STATUS_UNKNOWN};
  FD_TEST( fd_grpc_h2_read_response_hdrs( &resp, matcher, block, sizeof(block) )==FD_H2_ERR_COMPRESSION );
  fd_hpack_skip_t skip[1];
  for( ulong split=0UL; split<=sizeof(block); split++ ) {
    fd_hpack_skip_init( skip );
    uint err = fd_hpack_skip_feed( skip, block, split, 0 );
    if( !err ) err = fd_hpack_skip_feed( skip, block+split, sizeof(block)-split, 1 );
    FD_TEST( err==FD_H2_ERR_COMPRESSION );
  }
}

FD_UNIT_TEST( pseudo_order_and_singleton_metadata_preserve_compression_scope ) {
  fd_h2_hdr_matcher_t matcher[1];
  FD_TEST(fd_h2_hdr_matcher_init(matcher,1UL)==matcher);
  fd_h2_hdr_matcher_insert_literal(matcher,FD_GRPC_HDR_STATUS,"grpc-status");
  uchar buf[256]; uchar scratch[256]; fd_grpc_resp_hdrs_t resp; uint status_cnt;
  for( int fault=0; fault<3; fault++ ) {
    ulong off=0UL;
    if( !fault ) {
      off+=hpack_literal(buf+off,"x",1,"v",1); buf[off++]=0x88;
    } else {
      buf[off++]=0x88;
      char const * name=fault==1 ? "grpc-status" : "content-type";
      ulong name_len=fault==1 ? 11UL : 12UL;
      off+=hpack_literal(buf+off,name,name_len,"0",1);
      off+=hpack_literal(buf+off,name,name_len,"0",1);
    }
    memset(&resp,0,sizeof(resp));
    FD_TEST(fd_grpc_h2_read_response_hdrs_ex(&resp,matcher,buf,off,scratch,sizeof(scratch),0,&status_cnt)==FD_H2_ERR_PROTOCOL);
    /* A semantic error never hides a subsequent malformed HPACK instruction. */
    buf[off++]=0x80;
    FD_TEST(fd_grpc_h2_read_response_hdrs_ex(&resp,matcher,buf,off,scratch,sizeof(scratch),0,&status_cnt)==FD_H2_ERR_COMPRESSION);
  }
  ulong off=hpack_literal(buf,"content-type",12,"text/plain",10);
  resp=(fd_grpc_resp_hdrs_t){.h2_status=200U,.is_grpc_proto=1};
  FD_TEST(fd_grpc_h2_read_response_hdrs_ex(&resp,matcher,buf,off,scratch,sizeof(scratch),1,&status_cnt)==FD_H2_SUCCESS);
  FD_TEST(resp.h2_status==200U && resp.is_grpc_proto);
}

static int
parse_literal_field( char const * name, ulong name_len,
                     char const * value, ulong value_len,
                     int trailers, int corrupt_suffix,
                     uint * status_cnt, fd_grpc_resp_hdrs_t * resp ) {
  fd_h2_hdr_matcher_t matcher[1];
  FD_TEST( fd_h2_hdr_matcher_init( matcher, 1UL )==matcher );
  fd_h2_hdr_matcher_insert_literal( matcher, FD_GRPC_HDR_STATUS,  "grpc-status" );
  fd_h2_hdr_matcher_insert_literal( matcher, FD_GRPC_HDR_MESSAGE, "grpc-message" );
  uchar block[256], scratch[512];
  FD_TEST( name_len<127UL && value_len<127UL );
  ulong sz = hpack_literal( block, name, name_len, value, value_len );
  if( corrupt_suffix ) block[sz++] = 0x80; /* nonexistent indexed field0 */
  *resp = (fd_grpc_resp_hdrs_t){ .grpc_status=FD_GRPC_STATUS_UNKNOWN };
  return fd_grpc_h2_read_response_hdrs_ex( resp, matcher, block, sz,
                                         scratch, sizeof(scratch), trailers, status_cnt );
}

FD_UNIT_TEST( decoded_field_octets_and_connection_fields ) {
  /* Exercise each octet in unknown names and values, including both value
     edges.  HTTP token punctuation/obs-text must survive stricter checking. */
  char const * token = "!#$%&'*+-.^_`|~0123456789abcdefghijklmnopqrstuvwxyz";
  uint count; fd_grpc_resp_hdrs_t resp; ulong cases = 0UL;
  for( uint c=0U; c<256U; c++ ) {
    char name[3] = {'x',(char)c,'x'};
    int valid = c && strchr( token, (int)c )!=NULL;
    FD_TEST( parse_literal_field( name, 3UL, "v", 1UL, 0, 0, &count, &resp )==
             (valid ? FD_H2_SUCCESS : FD_H2_ERR_PROTOCOL) ); cases++;
    char value[3] = {'x',(char)c,'x'};
    valid = (c>=32U && c!=127U) || c==9U;
    FD_TEST( parse_literal_field( "x", 1UL, value, 3UL, 1, 0, &count, &resp )==
             (valid ? FD_H2_SUCCESS : FD_H2_ERR_PROTOCOL) ); cases++;
    for( uint edge=0U; edge<2U; edge++ ) {
      char edge_value[2] = {'x','x'}; edge_value[edge] = (char)c;
      valid = c>=33U && c!=127U;
      FD_TEST( parse_literal_field( "x", 1UL, edge_value, 2UL, 0, 0, &count, &resp )==
               (valid ? FD_H2_SUCCESS : FD_H2_ERR_PROTOCOL) ); cases++;
    }
  }
  FD_TEST( parse_literal_field( "", 0UL, "", 0UL, 0, 0, &count, &resp )==FD_H2_ERR_PROTOCOL ); cases++;
  FD_TEST( parse_literal_field( "x", 1UL, "", 0UL, 1, 0, &count, &resp )==FD_H2_SUCCESS ); cases++;
  char const * forbidden[] = {"connection","proxy-connection","keep-alive","transfer-encoding","upgrade","te"};
  for( ulong i=0UL; i<sizeof(forbidden)/sizeof(forbidden[0]); i++ ) for( int trailer=0; trailer<2; trailer++ ) {
    FD_TEST( parse_literal_field( forbidden[i], strlen(forbidden[i]), "trailers", 8UL,
                                 trailer, 0, &count, &resp )==FD_H2_ERR_PROTOCOL ); cases++;
    FD_TEST( parse_literal_field( forbidden[i], strlen(forbidden[i]), "trailers", 8UL,
                                 trailer, 1, &count, &resp )==FD_H2_ERR_COMPRESSION ); cases++;
  }
  /* A recognized field is also checked before copying invalid value bytes. */
  for( uint c=0U; c<256U; c++ ) {
    char value[3] = {'x',(char)c,'x'};
    int valid = (c>=32U && c!=127U) || c==9U;
    FD_TEST( parse_literal_field( "grpc-message", 12UL, value, 3UL, 1, 0, &count, &resp )==
             (valid ? FD_H2_SUCCESS : FD_H2_ERR_PROTOCOL) );
    FD_TEST( resp.grpc_msg_len==(valid ? 3U : 0U) );
    if( valid ) FD_TEST( fd_memeq( resp.grpc_msg, value, 3UL ) );
    cases++;
  }
  FD_TEST( cases==1306UL );
  FD_LOG_NOTICE(( "decoded field validity checks=%lu", cases ));
}

FD_UNIT_TEST( invalid_status_count_and_huffman ) {
  static char const values[][4] = {{'2',0,'0',0},{'2','\r','0',0},{'2','\n','0',0},{' ','2','0','0'},{'2','0','0','\t'}};
  uint count; fd_grpc_resp_hdrs_t resp; ulong cases = 0UL;
  for( ulong i=0UL; i<sizeof(values)/sizeof(values[0]); i++ ) for( int trailer=0; trailer<2; trailer++ ) {
    ulong len = i<3UL ? 3UL : 4UL;
    FD_TEST( parse_literal_field( ":status", 7UL, values[i], len, trailer, 0, &count, &resp )==FD_H2_ERR_PROTOCOL );
    FD_TEST( count==1U ); cases++;
    FD_TEST( parse_literal_field( ":status", 7UL, values[i], len, trailer, 1, &count, &resp )==FD_H2_ERR_COMPRESSION );
    FD_TEST( count==1U ); cases++;
  }
  FD_TEST( parse_literal_field( ":Status", 7UL, "200", 3UL, 0, 0, &count, &resp )==FD_H2_ERR_PROTOCOL );
  FD_TEST( !count ); cases++;
  fd_h2_hdr_matcher_t matcher[1]; FD_TEST( fd_h2_hdr_matcher_init( matcher, 1UL )==matcher );
  uchar block[256], scratch[512];
  ulong sz = hpack_literal( block, ":status", 7UL, " 200", 4UL ); block[sz++] = 0x88;
  FD_TEST( fd_grpc_h2_read_response_hdrs_ex( &resp, matcher, block, sz, scratch, sizeof(scratch), 0, &count )==FD_H2_ERR_PROTOCOL );
  FD_TEST( count==2U ); cases++;
  /* Duplicate valid statuses retain codec count2; the client rejects cardinality. */
  block[0]=0x88; block[1]=0x88;
  FD_TEST( fd_grpc_h2_read_response_hdrs_ex( &resp, matcher, block, 2UL, scratch, sizeof(scratch), 0, &count )==FD_H2_SUCCESS );
  FD_TEST( count==2U ); cases++;
  /* HPACK Huffman name=:status, value="2\n0": count after decoding, before skip. */
  static uchar const huffman[] = {0,0x85,0xb8,0x84,0x8d,0x36,0xa3,0x84,0x17,0xfe,0x3c,0xff};
  memcpy( block, huffman, sizeof(huffman) );
  for( int trailer=0; trailer<2; trailer++ ) {
    FD_TEST( fd_grpc_h2_read_response_hdrs_ex( &resp, matcher, block, sizeof(huffman), scratch, sizeof(scratch), trailer, &count )==FD_H2_ERR_PROTOCOL );
    FD_TEST( count==1U ); cases++;
    block[sizeof(huffman)] = 0x80;
    FD_TEST( fd_grpc_h2_read_response_hdrs_ex( &resp, matcher, block, sizeof(huffman)+1UL, scratch, sizeof(scratch), trailer, &count )==FD_H2_ERR_COMPRESSION );
    FD_TEST( count==1U ); cases++;
  }
  FD_LOG_NOTICE(( "invalid status count checks=%lu", cases ));
}

FD_UNIT_TEST( canonical_grpc_statuses ) {
  uint count; fd_grpc_resp_hdrs_t resp;
  for( uint code=0U; code<=16U; code++ ) {
    char value[2]; ulong len = code<10U ? 1UL : 2UL;
    value[0] = code<10U ? (char)('0'+code) : '1'; value[1] = (char)('0'+code%10U);
    FD_TEST( parse_literal_field( "grpc-status", 11UL, value, len, 0, 0, &count, &resp )==FD_H2_SUCCESS );
    FD_TEST( resp.grpc_status==code );
  }
  char const * noncanonical[] = {"00","03","010","016","0000000000","17","+0","-1","abc","","1\t0"};
  for( ulong i=0UL; i<sizeof(noncanonical)/sizeof(noncanonical[0]); i++ ) {
    FD_TEST( parse_literal_field( "grpc-status", 11UL, noncanonical[i], strlen(noncanonical[i]), 1, 0, &count, &resp )==FD_H2_SUCCESS );
    FD_TEST( resp.grpc_status==FD_GRPC_STATUS_UNKNOWN );
  }
  FD_LOG_NOTICE(( "canonical grpc statuses=17 noncanonical=11" ));
}

FD_UNIT_TEST( decoded_resource_limit_precedes_field_scans ) {
  fd_h2_hdr_matcher_t matcher[1]; FD_TEST( fd_h2_hdr_matcher_init( matcher, 1UL )==matcher );
  static uchar block[FD_GRPC_HEADER_LIST_MAX+16UL];
  static char value[FD_GRPC_HEADER_LIST_MAX]; memset( value, 'x', sizeof(value) );
  uchar scratch[512]; uint count; fd_grpc_resp_hdrs_t resp = {0};
  ulong len = FD_GRPC_HEADER_LIST_MAX-33UL; /* one-byte name plus accounting overhead */
  ulong sz = hpack_literal( block, "x", 1UL, value, len );
  FD_TEST( fd_grpc_h2_read_response_hdrs_ex( &resp, matcher, block, sz, scratch, sizeof(scratch), 0, &count )==FD_H2_SUCCESS );
  sz = hpack_literal( block, "x", 1UL, value, len+1UL ); block[sz++] = 0x80;
  FD_TEST( fd_grpc_h2_read_response_hdrs_ex( &resp, matcher, block, sz, scratch, sizeof(scratch), 0, &count )==FD_H2_ERR_ENHANCE_YOUR_CALM );
  /* A prior semantic failure must not prevent accounting for the next field,
     nor require parsing an already connection-fatal oversized block's tail. */
  ulong off = hpack_literal( block, "X", 1UL, "v", 1UL );
  sz = off+hpack_literal( block+off, "x", 1UL, value, len-33UL ); block[sz++] = 0x80;
  FD_TEST( fd_grpc_h2_read_response_hdrs_ex( &resp, matcher, block, sz, scratch, sizeof(scratch), 0, &count )==FD_H2_ERR_ENHANCE_YOUR_CALM );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );
  fd_unit_tests( argc, argv );
  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
