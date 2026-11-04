#define _GNU_SOURCE
#include "fd_bam_tile_private.h"

#include "../../ballet/ed25519/fd_x25519.h"
#include "../../util/net/fd_ip4.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

/* phase: idle, fetching the document, probing nodes. */
#define FD_BAM_REG_IDLE  (0)
#define FD_BAM_REG_FETCH (1)
#define FD_BAM_REG_PROBE (2)

/* io: TCP connect, TLS handshake (not for http), then HTTP body or
   gRPC samples. */
#define FD_BAM_REG_IO_CONNECT (0)
#define FD_BAM_REG_IO_TLS     (1)
#define FD_BAM_REG_IO_BODY    (2)

static char const FD_BAM_REG_CONFIG_PATH[] = "/bam_api.BamNodeApi/GetBuilderConfig";

static void
reg_close( fd_bam_registry_t * reg ) {
  if( reg->fd>=0 ) {
    close( reg->fd );
    reg->fd = -1;
  }
  reg->io       = FD_BAM_REG_IO_CONNECT;
  reg->http_off = 0UL;
  reg->body_len = 0UL;
}

static void
slot_close( fd_bam_registry_slot_t * s ) {
  if( s->fd>=0 ) {
    close( s->fd );
    s->fd = -1;
  }
  fd_grpc_client_reset( s->grpc );
  s->io       = FD_BAM_REG_IO_CONNECT;
  s->inflight = 0;
  s->dead     = 0;
  s->samples  = 0U;
  s->best_us  = ULONG_MAX;
}

static void
reg_close_all( fd_bam_registry_t * reg ) {
  reg_close( reg );
  for( ulong i=0UL; i<reg->slot_cnt; i++ ) slot_close( &reg->slots[ i ] );
  reg->probe_active = 0UL;
}

static void
reg_fail_msg( char *       err,
              ulong        err_sz,
              char const * msg ) {
  if( err && err_sz ) fd_cstr_printf( err, err_sz, NULL, "%s", msg );
  FD_LOG_WARNING(( "%s", msg ));
}

static void
reg_schedule( fd_bam_registry_t * reg,
              long                now,
              int                 live,
              fd_rng_t *          rng ) {
  long const half = FD_BAM_REGISTRY_RESYNC_LIVE_NS/2;
  long jitter = half;
  if( rng ) jitter += (long)fd_rng_ulong_roll( rng, (ulong)half+1UL );
  reg->resync_at = now+jitter;
  if( !live ) {
    long stalled = now+FD_BAM_REGISTRY_RESYNC_STALLED_NS;
    if( stalled<reg->resync_at ) reg->resync_at = stalled;
  }
}

int
fd_bam_registry_is_url( fd_url_t const * url ) {
  if( !url || !url->tail_len || url->tail[0]!='/' ) return 0;
  ulong n = 0UL;
  while( n<url->tail_len && url->tail[ n ]!='?' && url->tail[ n ]!='#' ) n++;
  return n>1UL;
}

/* ---- JSON ---------------------------------------------------------- */

struct reg_cur {
  char const * p;
  char const * end;
};

static int
reg_ws( struct reg_cur * c ) {
  while( c->p<c->end ) {
    char ch = *c->p;
    if( ch!=' ' && ch!='\t' && ch!='\n' && ch!='\r' ) break;
    c->p++;
  }
  return c->p<c->end;
}

struct reg_str {
  char * dst;
  ulong  max;
  ulong  n;
  int    cut;
};

static void
reg_put( struct reg_str * s,
         uint             ch ) {
  if( s->cut ) return;
  if( !ch || s->n+1UL>=s->max ) { s->cut = 1; return; }
  s->dst[ s->n++ ] = (char)ch;
}

static void
reg_put_cp( struct reg_str * s,
            uint             cp ) {
  if( cp<0x80U ) {
    reg_put( s, cp );
  } else if( cp<0x800U ) {
    reg_put( s, 0xC0U|(cp>>6) );
    reg_put( s, 0x80U|(cp&0x3FU) );
  } else if( cp<0x10000U ) {
    reg_put( s, 0xE0U|(cp>>12) );
    reg_put( s, 0x80U|((cp>>6)&0x3FU) );
    reg_put( s, 0x80U|(cp&0x3FU) );
  } else {
    reg_put( s, 0xF0U|(cp>>18) );
    reg_put( s, 0x80U|((cp>>12)&0x3FU) );
    reg_put( s, 0x80U|((cp>>6)&0x3FU) );
    reg_put( s, 0x80U|(cp&0x3FU) );
  }
}

static int
reg_hex4( struct reg_cur * c,
          uint *           out ) {
  if( c->end-c->p<4L ) return -1;
  uint v = 0U;
  for( ulong i=0UL; i<4UL; i++ ) {
    char h = *c->p++;
    uint d;
    if(      h>='0' && h<='9' ) d = (uint)(h-'0');
    else if( h>='a' && h<='f' ) d = (uint)(h-'a'+10);
    else if( h>='A' && h<='F' ) d = (uint)(h-'A'+10);
    else return -1;
    v = (v<<4)|d;
  }
  *out = v;
  return 0;
}

/* Strict UTF-8, as Rust's str: no overlongs, surrogates, or >U+10FFFF. */

static int
reg_utf8( struct reg_cur * c,
          uchar            lead,
          struct reg_str * s ) {
  ulong more;
  uint  cp, min;
  if(      (lead&0xE0U)==0xC0U ) { more = 1UL; cp = lead&0x1FU; min = 0x80U;    }
  else if( (lead&0xF0U)==0xE0U ) { more = 2UL; cp = lead&0x0FU; min = 0x800U;   }
  else if( (lead&0xF8U)==0xF0U ) { more = 3UL; cp = lead&0x07U; min = 0x10000U; }
  else return -1;
  reg_put( s, lead );
  for( ulong i=0UL; i<more; i++ ) {
    if( c->p>=c->end ) return -1;
    uchar b = (uchar)*c->p++;
    if( (b&0xC0U)!=0x80U ) return -1;
    cp = (cp<<6)|(b&0x3FU);
    reg_put( s, b );
  }
  if( cp<min || cp>0x10FFFFU || (cp>=0xD800U && cp<=0xDFFFU) ) return -1;
  return 0;
}

/* reg_string reads a JSON string.  With dst it decodes like serde_json
   parse_str: escapes, surrogate pairs, and UTF-8 are checked.  Returns
   1 when the value was cut (too long, or an embedded NUL); dst then
   holds the prefix.  Without dst it checks what serde_json checks when
   ignoring a value: escape syntax and control characters. */

static int
reg_string( struct reg_cur * c,
            char *           dst,
            ulong            dst_max ) {
  if( !reg_ws( c ) || *c->p!='"' ) return -1;
  c->p++;
  struct reg_str s = { dst, dst_max, 0UL, 0 };
  for(;;) {
    if( c->p>=c->end ) return -1;
    uchar ch = (uchar)*c->p++;
    if( ch=='"' ) break;
    if( ch<0x20U ) return -1;
    if( ch!='\\' ) {
      if( !dst ) continue;
      if( ch<0x80U ) reg_put( &s, ch );
      else if( reg_utf8( c, ch, &s ) ) return -1;
      continue;
    }
    if( c->p>=c->end ) return -1;
    char e = *c->p++;
    uint cp;
    switch( e ) {
    case '"': case '\\': case '/': cp = (uint)(uchar)e; break;
    case 'b': cp = '\b'; break;
    case 'f': cp = '\f'; break;
    case 'n': cp = '\n'; break;
    case 'r': cp = '\r'; break;
    case 't': cp = '\t'; break;
    case 'u':
      if( reg_hex4( c, &cp ) ) return -1;
      if( !dst ) break;
      if( cp>=0xDC00U && cp<=0xDFFFU ) return -1;
      if( cp>=0xD800U && cp<=0xDBFFU ) {
        uint lo;
        if( c->end-c->p<2L || c->p[0]!='\\' || c->p[1]!='u' ) return -1;
        c->p += 2;
        if( reg_hex4( c, &lo ) || lo<0xDC00U || lo>0xDFFFU ) return -1;
        cp = 0x10000U+((cp-0xD800U)<<10)+(lo-0xDC00U);
      }
      break;
    default:
      return -1;
    }
    if( dst ) reg_put_cp( &s, cp );
  }
  if( dst ) dst[ s.n ] = '\0';
  return s.cut;
}

static int
reg_skip( struct reg_cur * c );

static int
reg_skip_lit( struct reg_cur * c,
              char const *     lit ) {
  ulong n = strlen( lit );
  if( (ulong)(c->end-c->p)<n || memcmp( c->p, lit, n ) ) return -1;
  c->p += n;
  return 0;
}

static int
reg_skip( struct reg_cur * c ) {
  if( !reg_ws( c ) ) return -1;
  char ch = *c->p;
  if( ch=='"' ) return reg_string( c, NULL, 0UL );
  if( ch=='t' ) return reg_skip_lit( c, "true" );
  if( ch=='f' ) return reg_skip_lit( c, "false" );
  if( ch=='n' ) return reg_skip_lit( c, "null" );
  if( ch=='-' || (ch>='0' && ch<='9') ) {
    if( ch=='-' ) c->p++;
    if( c->p>=c->end || *c->p<'0' || *c->p>'9' ) return -1;
    if( *c->p=='0' ) c->p++;
    else while( c->p<c->end && *c->p>='0' && *c->p<='9' ) c->p++;
    if( c->p<c->end && *c->p=='.' ) {
      c->p++;
      if( c->p>=c->end || *c->p<'0' || *c->p>'9' ) return -1;
      while( c->p<c->end && *c->p>='0' && *c->p<='9' ) c->p++;
    }
    if( c->p<c->end && (*c->p=='e' || *c->p=='E') ) {
      c->p++;
      if( c->p<c->end && (*c->p=='+' || *c->p=='-') ) c->p++;
      if( c->p>=c->end || *c->p<'0' || *c->p>'9' ) return -1;
      while( c->p<c->end && *c->p>='0' && *c->p<='9' ) c->p++;
    }
    return 0;
  }
  if( ch=='{' || ch=='[' ) {
    char open = ch;
    char shut = (char)(open=='{' ? '}' : ']');
    c->p++;
    if( !reg_ws( c ) ) return -1;
    if( *c->p==shut ) { c->p++; return 0; }
    for( ;; ) {
      if( open=='{' ) {
        if( reg_string( c, NULL, 0UL ) ) return -1;
        if( !reg_ws( c ) || *c->p!=':' ) return -1;
        c->p++;
      }
      if( reg_skip( c ) ) return -1;
      if( !reg_ws( c ) ) return -1;
      if( *c->p==shut ) { c->p++; return 0; }
      if( *c->p!=',' ) return -1;
      c->p++;
    }
  }
  return -1;
}

static int
reg_ipv4( char const * s,
          uint *       ip ) {
  uint oct[ 4 ] = {0};
  ulong i = 0UL;
  char const * p = s;
  while( i<4UL ) {
    if( *p<'0' || *p>'9' ) return -1;
    uint v = 0U;
    ulong digits = 0UL;
    while( *p>='0' && *p<='9' ) {
      if( digits==1UL && v==0U ) return -1;
      v = v*10U+(uint)(*p-'0');
      if( v>255U || ++digits>3UL ) return -1;
      p++;
    }
    if( !digits ) return -1;
    oct[ i++ ] = v;
    if( i==4UL ) break;
    if( *p!='.' ) return -1;
    p++;
  }
  if( *p ) return -1;
  *ip = oct[0] | (oct[1]<<8) | (oct[2]<<16) | (oct[3]<<24);
  return 0;
}

static int
reg_u16( struct reg_cur * c,
         ushort *         out ) {
  if( !reg_ws( c ) || *c->p<'0' || *c->p>'9' ) return -1;
  ulong v = 0UL;
  ulong digits = 0UL;
  while( c->p<c->end && *c->p>='0' && *c->p<='9' ) {
    if( digits==1UL && v==0UL ) return -1;
    v = v*10UL+(ulong)(*c->p-'0');
    if( v>65535UL || ++digits>5UL ) return -1;
    c->p++;
  }
  *out = (ushort)v;
  return 0;
}

static int
reg_node( struct reg_cur *         c,
          fd_bam_registry_node_t * node ) {
  if( !reg_ws( c ) || *c->p!='{' ) return -1;
  c->p++;
  memset( node, 0, sizeof(*node) );
  int got_ip = 0, got_port = 0, got_region = 0, skip = 0;
  if( !reg_ws( c ) ) return -1;
  if( *c->p=='}' ) return -1;
  for( ;; ) {
    char key[ 32 ];
    int kr = reg_string( c, key, sizeof(key) );
    if( kr<0 ) return -1;
    if( !reg_ws( c ) || *c->p!=':' ) return -1;
    c->p++;
    /* serde derive rejects a repeated known field. */
    if( kr>0 ) {
      if( reg_skip( c ) ) return -1;
    } else if( !strcmp( key, "ip" ) ) {
      if( got_ip ) return -1;
      got_ip = 1;
      char ip[ 64 ];
      uchar ip6[ 16 ];
      if( reg_string( c, ip, sizeof(ip) ) ) return -1;
      if( reg_ipv4( ip, &node->ip4 ) ) {
        if( inet_pton( AF_INET6, ip, ip6 )!=1 ) return -1;
        skip = 1;
      }
    } else if( !strcmp( key, "grpc_port" ) ) {
      if( got_port ) return -1;
      got_port = 1;
      if( reg_u16( c, &node->grpc_port ) ) return -1;
    } else if( !strcmp( key, "region" ) ) {
      if( got_region ) return -1;
      got_region = 1;
      if( reg_string( c, node->region, sizeof(node->region) )<0 ) return -1;
    } else if( reg_skip( c ) ) return -1;
    if( !reg_ws( c ) ) return -1;
    if( *c->p=='}' ) { c->p++; break; }
    if( *c->p!=',' ) return -1;
    c->p++;
  }
  if( skip ) return 1;
  if( !got_ip || !got_port || !got_region ) return -1;
  return 0;
}

static void
reg_dedupe( fd_bam_registry_node_t * nodes,
            ulong *                  cnt ) {
  /* Stable sort by (octets, port) like Rust's IpAddr ordering, so the
     first listed duplicate keeps its region. */
  for( ulong i=1UL; i<*cnt; i++ ) {
    fd_bam_registry_node_t key = nodes[ i ];
    uint  key_be = fd_uint_bswap( key.ip4 );
    ulong j = i;
    while( j && ( fd_uint_bswap( nodes[ j-1UL ].ip4 )>key_be ||
                  (nodes[ j-1UL ].ip4==key.ip4 && nodes[ j-1UL ].grpc_port>key.grpc_port) ) ) {
      nodes[ j ] = nodes[ j-1UL ];
      j--;
    }
    nodes[ j ] = key;
  }
  ulong w = 0UL;
  for( ulong i=0UL; i<*cnt; i++ ) {
    if( w && nodes[ w-1UL ].ip4==nodes[ i ].ip4 && nodes[ w-1UL ].grpc_port==nodes[ i ].grpc_port ) continue;
    nodes[ w++ ] = nodes[ i ];
  }
  *cnt = w;
}

int
fd_bam_registry_parse( char const *             json,
                       ulong                    json_sz,
                       fd_bam_registry_node_t * nodes,
                       ulong                    cap,
                       ulong *                  cnt ) {
  struct reg_cur c = { json, json+json_sz };
  if( !reg_ws( &c ) || *c.p!='{' ) return -1;
  c.p++;
  int saw_nodes = 0, saw_ts = 0;
  ulong n = 0UL;
  if( !reg_ws( &c ) ) return -1;
  if( *c.p!='}' ) {
    for( ;; ) {
      char key[ 32 ];
      int kr = reg_string( &c, key, sizeof(key) );
      if( kr<0 ) return -1;
      if( !reg_ws( &c ) || *c.p!=':' ) return -1;
      c.p++;
      if( kr>0 ) {
        if( reg_skip( &c ) ) return -1;
      } else if( !strcmp( key, "generated_at" ) ) {
        /* lenient_rfc3339 swallows a bad scalar, but serde_json does not
           consume an object or array there, so the document fails. */
        if( saw_ts ) return -1;
        saw_ts = 1;
        if( !reg_ws( &c ) || *c.p=='{' || *c.p=='[' ) return -1;
        if( reg_skip( &c ) ) return -1;
      } else if( !strcmp( key, "nodes" ) ) {
        if( saw_nodes ) return -1;
        if( !reg_ws( &c ) || *c.p!='[' ) return -1;
        c.p++;
        saw_nodes = 1;
        if( !reg_ws( &c ) ) return -1;
        if( *c.p!=']' ) {
          for( ;; ) {
            fd_bam_registry_node_t node;
            int nr = reg_node( &c, &node );
            if( nr<0 ) return -1;
            if( !nr ) {
              if( n>=cap ) return -1;
              nodes[ n++ ] = node;
            }
            if( !reg_ws( &c ) ) return -1;
            if( *c.p==']' ) break;
            if( *c.p!=',' ) return -1;
            c.p++;
          }
        }
        if( !reg_ws( &c ) || *c.p!=']' ) return -1;
        c.p++;
      } else if( reg_skip( &c ) ) return -1;
      if( !reg_ws( &c ) ) return -1;
      if( *c.p=='}' ) break;
      if( *c.p!=',' ) return -1;
      c.p++;
    }
  }
  if( *c.p!='}' ) return -1;
  c.p++;
  if( reg_ws( &c ) ) return -1;
  if( !saw_nodes || !n ) return -1;
  reg_dedupe( nodes, &n );
  if( !n ) return -1;
  *cnt = n;
  return 0;
}

/* ---- follower ------------------------------------------------------ */

static int
reg_nodes_match( fd_bam_registry_t const *      reg,
                 fd_bam_registry_node_t const * nodes,
                 ulong                          cnt ) {
  if( cnt!=reg->node_cnt || !cnt ) return 0;
  for( ulong i=0UL; i<cnt; i++ ) {
    if( reg->nodes[ i ].ip4!=nodes[ i ].ip4 || reg->nodes[ i ].grpc_port!=nodes[ i ].grpc_port ||
        strcmp( reg->nodes[ i ].region, nodes[ i ].region ) ) return 0;
  }
  return 1;
}

void
fd_bam_registry_on_fetch( fd_bam_registry_t *            reg,
                          fd_bam_registry_node_t const * nodes,
                          ulong                          cnt,
                          long                           now,
                          int                            live,
                          fd_rng_t *                     rng ) {
  if( !reg_nodes_match( reg, nodes, cnt ) ) {
    memcpy( reg->nodes, nodes, cnt*sizeof(*nodes) );
    reg->node_cnt   = cnt;
    reg->ranked_cnt = 0UL;
    reg->cursor     = 0UL;
    reg->probe_at   = now;
  }
  reg_schedule( reg, now, live, rng );
}

void
fd_bam_registry_finish_round( fd_bam_registry_t * reg ) {
  for( ulong i=1UL; i<reg->ranked_cnt; i++ ) {
    fd_bam_registry_rank_t key = reg->ranked[ i ];
    ulong j = i;
    while( j && reg->ranked[ j-1UL ].rtt_us>key.rtt_us ) {
      reg->ranked[ j ] = reg->ranked[ j-1UL ];
      j--;
    }
    reg->ranked[ j ] = key;
  }
  ulong keep = 0UL;
  while( keep<reg->ranked_cnt && reg->ranked[ keep ].rtt_us<=FD_BAM_REGISTRY_MAX_RTT_US ) keep++;
  reg->ranked_cnt = keep;
  reg->cursor     = 0UL;
}

int
fd_bam_registry_choose( fd_bam_registry_t *      reg,
                        int                      live,
                        long                     now,
                        fd_bam_registry_node_t * out ) {
  if( live ) reg->stalled_since = now;
  int stuck = !live && now-reg->stalled_since>=FD_BAM_REGISTRY_CONNECT_GRACE_NS;

  int on_pick = 0;
  if( reg->have_dial && reg->cursor<reg->ranked_cnt ) {
    fd_bam_registry_node_t const * n = &reg->ranked[ reg->cursor ].node;
    on_pick = n->ip4==reg->dial_ip4 && n->grpc_port==reg->dial_port;
  }
  if( stuck && on_pick && reg->ranked_cnt ) {
    reg->cursor = (reg->cursor+1UL)%reg->ranked_cnt;
    if( !reg->cursor ) {
      reg->ranked_cnt = 0UL;
      return 0;
    }
  }

  int in_list = 0;
  if( reg->have_dial ) {
    for( ulong i=0UL; i<reg->node_cnt; i++ ) {
      if( reg->nodes[ i ].ip4==reg->dial_ip4 && reg->nodes[ i ].grpc_port==reg->dial_port ) {
        in_list = 1;
        break;
      }
    }
  }
  if( (stuck || !reg->have_dial || !in_list) && reg->cursor<reg->ranked_cnt ) {
    *out = reg->ranked[ reg->cursor ].node;
    return 1;
  }
  return 0;
}

void
fd_bam_registry_disable( fd_bam_tile_t * ctx ) {
  fd_bam_registry_t * reg = &ctx->registry;
  reg_close_all( reg );
  reg->on         = 0;
  reg->phase      = FD_BAM_REG_IDLE;
  reg->node_cnt   = 0UL;
  reg->ranked_cnt = 0UL;
  reg->have_dial  = 0;
  if( !reg->cert_verify_cfg ) ctx->tls->ca_store = NULL;
}

int
fd_bam_registry_take_url( fd_bam_tile_t *  ctx,
                          char const *     raw,
                          fd_url_t const * url,
                          int              is_ssl,
                          ushort           port,
                          char *           err,
                          ulong            err_sz ) {
  if( !fd_bam_registry_is_url( url ) ) {
    if( ctx->registry.on ) fd_bam_registry_disable( ctx );
    return 0;
  }
  /* reqwest's default ports, not the BAM node ports the caller
     filled in. */
  if( !url->port_len ) port = is_ssl ? (ushort)443 : (ushort)80;
  ulong path_len = 0UL;
  while( path_len<url->tail_len && url->tail[ path_len ]!='#' ) path_len++;
  if( !path_len || path_len>=FD_BAM_REGISTRY_PATH_MAX || url->host_len>=sizeof(ctx->registry.host) ) {
    reg_fail_msg( err, err_sz, "BAM registry URL path or host is too long" );
    return -1;
  }

  fd_bam_registry_t * reg = &ctx->registry;
  int same = reg->on && reg->ssl==!!is_ssl && reg->port==port &&
             reg->host_len==(ushort)url->host_len && reg->path_len==(ushort)path_len &&
             !memcmp( reg->host, url->host, url->host_len ) && !memcmp( reg->path, url->tail, path_len );
  if( !same ) {
    reg_close_all( reg );
    reg->phase      = FD_BAM_REG_IDLE;
    reg->node_cnt   = 0UL;
    reg->ranked_cnt = 0UL;
    reg->have_dial  = 0;
    reg->resync_at  = 0L;
    reg->probe_at   = 0L;
    reg->fetch_after   = 0L;
    reg->stalled_since = fd_bam_now();
    memcpy( reg->host, url->host, url->host_len );
    reg->host[ url->host_len ] = '\0';
    reg->host_len = (ushort)url->host_len;
    memcpy( reg->path, url->tail, path_len );
    reg->path[ path_len ] = '\0';
    reg->path_len = (ushort)path_len;
    reg->port = port;
    reg->ssl  = (uchar)!!is_ssl;
  }
  fd_cstr_ncpy( reg->url, raw, sizeof(reg->url) );
  reg->on = 1;
  ctx->tls->ca_store = ctx->ca_store;
  return 1;
}

/* ---- TLS sessions -------------------------------------------------- */

static int
reg_cert_failed( fd_bam_registry_t * reg,
                 fd_tlsrec_conn_t *  conn,
                 fd_tls_t const *    tls ) {
  if( !fd_tlsrec_conn_is_failed( conn ) ) return 0;
  if( conn->hs.base.reason==FD_TLS_REASON_CERT_VERIFY ) {
    reg->cert_fail++;
    FD_LOG_WARNING(( "BAM registry TLS certificate rejected for %s", tls->server_name ));
  }
  return 1;
}

/* reg_tls_init starts a client handshake verified against ca. */

static int
reg_tls_init( fd_x509_ca_store_t const * ca,
              fd_tls_t *         tls,
              fd_tlsrec_conn_t * conn,
              char const *       sni,
              ulong              sni_len,
              int                http1 ) {
  if( http1 ) {
    static uchar const alpn[] = { 8, 'h','t','t','p','/','1','.','1' };
    memcpy( tls->alpn, alpn, sizeof(alpn) );
    tls->alpn_sz = sizeof(alpn);
  } else {
    static uchar const alpn[] = { 2, 'h', '2' };
    memcpy( tls->alpn, alpn, sizeof(alpn) );
    tls->alpn_sz = sizeof(alpn);
  }
  if( sni_len>=sizeof(tls->server_name) ) return -1;
  memcpy( tls->server_name, sni, sni_len );
  tls->server_name[ sni_len ] = '\0';
  tls->server_name_len = (ushort)sni_len;
  tls->ca_store = ca;
  if( FD_UNLIKELY( !fd_rng_secure( tls->kex_private_key, 32UL ) ) ) return -1;
  fd_x25519_public( tls->kex_public_key, tls->kex_private_key );
  fd_tlsrec_conn_init( conn, tls, 0 );
  return 0;
}

/* reg_socket returns a nonblocking socket with connect in flight, or
   -1.  quiet drops the connect-failure warning for probes. */

static int
reg_socket( fd_bam_tile_t * ctx,
            uint            ip4,
            ushort          port,
            int             quiet ) {
  int fd = socket( AF_INET, SOCK_STREAM|SOCK_CLOEXEC, 0 );
  if( FD_UNLIKELY( fd<0 ) ) {
    FD_LOG_WARNING(( "BAM registry socket failed (%i-%s)", errno, fd_io_strerror( errno ) ));
    return -1;
  }
  int rcv = ctx->so_rcvbuf>0 ? ctx->so_rcvbuf : 65536;
  if( FD_UNLIKELY( setsockopt( fd, SOL_SOCKET, SO_RCVBUF, &rcv, sizeof(rcv) ) ) ) {
    close( fd );
    FD_LOG_WARNING(( "BAM registry setsockopt(SO_RCVBUF) failed (%i-%s)", errno, fd_io_strerror( errno ) ));
    return -1;
  }
  int nodelay = 1;
  if( FD_UNLIKELY( setsockopt( fd, SOL_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay) ) ) ) {
    close( fd );
    FD_LOG_WARNING(( "BAM registry setsockopt(TCP_NODELAY) failed (%i-%s)", errno, fd_io_strerror( errno ) ));
    return -1;
  }
  if( FD_UNLIKELY( fcntl( fd, F_SETFL, O_NONBLOCK )==-1 ) ) {
    close( fd );
    FD_LOG_WARNING(( "BAM registry fcntl(O_NONBLOCK) failed (%i-%s)", errno, fd_io_strerror( errno ) ));
    return -1;
  }
  struct sockaddr_in addr = {
    .sin_family      = AF_INET,
    .sin_addr.s_addr = ip4,
    .sin_port        = fd_ushort_bswap( port )
  };
  int connect_err = connect( fd, fd_type_pun_const( &addr ), sizeof(addr) );
  if( connect_err==-1 ) connect_err = errno;
  if( FD_UNLIKELY( connect_err && connect_err!=EINPROGRESS ) ) {
    close( fd );
    if( !quiet ) FD_LOG_WARNING(( "BAM registry connect failed (%i-%s)", connect_err, fd_io_strerror( connect_err ) ));
    return -1;
  }
  return fd;
}

/* reg_resolve fills addrs with the fetch host's IPv4 addresses.
   TODO: AAAA records too, as hyper does (see fd_bam_registry_parse). */

static int
reg_resolve( fd_bam_registry_t * reg ) {
  fd_addrinfo_t hints = { .ai_family = AF_INET };
  fd_addrinfo_t * res = NULL;
  uchar scratch[ 4096 ];
  void * pscratch = scratch;
  int err = fd_getaddrinfo( reg->f_host, &hints, &res, &pscratch, sizeof(scratch) );
  if( FD_UNLIKELY( err || !res ) ) {
    FD_LOG_WARNING(( "BAM registry resolve `%s` failed (%d-%s)",
                     reg->f_host, err, fd_gai_strerror( err ) ));
    return -1;
  }
  reg->addr_cnt = 0UL;
  reg->addr_i   = 0UL;
  for( fd_addrinfo_t * ai=res; ai && reg->addr_cnt<FD_BAM_REGISTRY_ADDR_MAX; ai=ai->ai_next ) {
    if( ai->ai_family!=AF_INET ) continue;
    reg->addrs[ reg->addr_cnt++ ] = ((struct sockaddr_in *)ai->ai_addr)->sin_addr.s_addr;
  }
  return reg->addr_cnt ? 0 : -1;
}

/* reg_fetch_connect opens the next resolved address.  As in hyper, a
   failed TCP connect moves to the next address, and a TLS failure
   does not.  All attempts share FETCH_TIMEOUT. */

static int
reg_fetch_connect( fd_bam_tile_t * ctx ) {
  fd_bam_registry_t * reg = &ctx->registry;
  reg_close( reg );
  while( reg->addr_i<reg->addr_cnt ) {
    int fd = reg_socket( ctx, reg->addrs[ reg->addr_i++ ], reg->f_port, 0 );
    if( fd<0 ) continue;
    reg->fd = fd;
    if( reg->f_ssl ) {
      if( reg_tls_init( reg->ca_store, reg->tls, reg->conn, reg->f_host, reg->f_host_len, 1 ) ) {
        reg_close( reg );
        return -1;
      }
      fd_tlsrec_sock_init( reg->sock );
    }
    return 0;
  }
  return -1;
}

static int
reg_fetch_begin( fd_bam_tile_t * ctx ) {
  if( reg_resolve( &ctx->registry ) ) return -1;
  return reg_fetch_connect( ctx );
}

/* reg_tcp_check returns 1 when a polled connect finished cleanly and
   -1 when it failed. */

static int
reg_tcp_check( int   fd,
               short revents,
               int   quiet ) {
  int so_err = 0;
  socklen_t sz = sizeof(so_err);
  if( FD_UNLIKELY( getsockopt( fd, SOL_SOCKET, SO_ERROR, &so_err, &sz ) || so_err ||
                   (revents & (POLLERR|POLLHUP|POLLNVAL)) ) ) {
    if( !so_err ) so_err = ECONNABORTED;
    if( !quiet ) FD_LOG_WARNING(( "BAM registry connect failed (%i-%s)", so_err, fd_io_strerror( so_err ) ));
    return -1;
  }
  return 1;
}

static int
reg_tcp_ready( fd_bam_registry_t * reg ) {
  struct pollfd pfd = { .fd = reg->fd, .events = POLLOUT };
  int pr = fd_syscall_poll( &pfd, 1, 0 );
  if( FD_UNLIKELY( pr<0 ) ) {
    FD_LOG_WARNING(( "BAM registry poll failed (%i-%s)", errno, fd_io_strerror( errno ) ));
    return -1;
  }
  if( !pr ) return 0;
  return reg_tcp_check( reg->fd, pfd.revents, 0 );
}

/* jito counts a session live only after wait_until_healthy_and_config_received
   and drops it to Disconnected once unhealthy.  CONNECTED_UNHEALTHY is
   neither, so it must age toward CONNECT_GRACE. */

static int
reg_live( fd_bam_tile_t * ctx ) {
  return fd_bam_client_status( ctx )==FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY;
}

static void
reg_fetch_done( fd_bam_tile_t * ctx,
                int             ok ) {
  fd_bam_registry_t * reg = &ctx->registry;
  ulong body_len = reg->body_len; /* reg_close clears it */
  reg_close( reg );
  reg->phase = FD_BAM_REG_IDLE;
  /* jito schedules from the start of the step that fetched. */
  long now = reg->fetch_start;
  reg->fetch_after = fd_bam_now()+FD_BAM_REGISTRY_POLL_NS;
  if( ok ) {
    ulong cnt = 0UL;
    if( !fd_bam_registry_parse( reg->body, body_len, reg->parsed, FD_BAM_REGISTRY_NODE_CAP, &cnt ) ) {
      fd_bam_registry_on_fetch( reg, reg->parsed, cnt, now, reg_live( ctx ), ctx->rng );
      reg->fetch_ok++;
      FD_LOG_INFO(( "BAM registry list: %lu nodes from %s", cnt, reg->url ));
      return;
    }
  }
  reg->fetch_fail++;
  reg_schedule( reg, now, reg_live( ctx ), ctx->rng );
  FD_LOG_WARNING(( "BAM registry fetch failed; keeping %lu cached nodes", reg->node_cnt ));
}

static int
reg_lower_eq( char const * p,
              char const * lit,
              ulong        n ) {
  for( ulong k=0UL; k<n; k++ ) {
    char a = p[ k ];
    if( a>='A' && a<='Z' ) a = (char)(a-'A'+'a');
    if( a!=lit[ k ] ) return 0;
  }
  return 1;
}

/* reg_hdr returns the trimmed value of header name (lowercase, with
   ':') on line [p,nl), or NULL. */

static char const *
reg_hdr( char const *   p,
         char const *   nl,
         char const *   name,
         char const * * val_end ) {
  ulong n = strlen( name );
  if( (ulong)(nl-p)<n || !reg_lower_eq( p, name, n ) ) return NULL;
  char const * v = p+n;
  char const * e = nl;
  while( v<e && (*v==' ' || *v=='\t') ) v++;
  while( e>v && (e[-1]==' ' || e[-1]=='\t' || e[-1]=='\r') ) e--;
  *val_end = e;
  return v;
}

/* reg_chunked decodes buf[0,len) in place.  Returns 1 with *out_len
   once the last chunk and trailers are in, 0 if more bytes are needed,
   -1 if malformed.  With write==0 it only scans. */

static int
reg_chunked( char * buf,
             ulong  len,
             int    write,
             ulong * out_len ) {
  ulong r = 0UL, w = 0UL;
  for(;;) {
    ulong sz = 0UL, digits = 0UL;
    while( r<len ) {
      char h = buf[ r ];
      ulong d;
      if(      h>='0' && h<='9' ) d = (ulong)(h-'0');
      else if( h>='a' && h<='f' ) d = (ulong)(h-'a'+10);
      else if( h>='A' && h<='F' ) d = (ulong)(h-'A'+10);
      else break;
      sz = (sz<<4)|d;
      if( ++digits>8UL || sz>FD_BAM_REGISTRY_BODY_MAX ) return -1;
      r++;
    }
    if( r>=len ) return 0;
    if( !digits ) return -1;
    while( r<len && buf[ r ]!='\n' ) r++; /* chunk extensions */
    if( r>=len ) return 0;
    if( buf[ r-1UL ]!='\r' ) return -1;
    r++;
    if( !sz ) break;
    if( len-r<sz+2UL ) return 0;
    if( buf[ r+sz ]!='\r' || buf[ r+sz+1UL ]!='\n' ) return -1;
    if( write ) memmove( buf+w, buf+r, sz );
    w += sz;
    r += sz+2UL;
  }
  for(;;) { /* trailers end at an empty line */
    char const * nl = memchr( buf+r, '\n', len-r );
    if( !nl ) return 0;
    ulong line = (ulong)(nl-(buf+r));
    r += line+1UL;
    if( line==0UL || (line==1UL && nl[-1]=='\r') ) break;
  }
  *out_len = w;
  return 1;
}

static char const *
reg_hdr_end( char const * buf,
             ulong        len ) {
  for( ulong i=0UL; i+3UL<len; i++ ) {
    if( buf[ i ]=='\r' && buf[ i+1 ]=='\n' && buf[ i+2 ]=='\r' && buf[ i+3 ]=='\n' ) return buf+i;
  }
  return NULL;
}

/* Any 2xx passes, as with reqwest's error_for_status.  Unlike reqwest,
   1xx interim responses fail the fetch. */

int
fd_bam_registry_http_body( char *  buf,
                           ulong * len,
                           int     eof ) {
  char const * hdr_end = reg_hdr_end( buf, *len );
  if( !hdr_end ) return eof ? -1 : 0;
  ulong hdr_len = (ulong)(hdr_end-buf);
  if( *len<12UL || memcmp( buf, "HTTP/1.", 7 ) || buf[8]!=' ' ||
      buf[9]!='2' || buf[10]<'0' || buf[10]>'9' ||
      buf[11]<'0' || buf[11]>'9' ) {
    return -1;
  }
  long content_len = -1L;
  int  chunked     = 0;
  char const * p = buf;
  while( p<hdr_end ) {
    char const * nl = memchr( p, '\n', (ulong)(hdr_end-p) );
    if( !nl ) nl = hdr_end;
    char const * ve = NULL;
    char const * v  = reg_hdr( p, nl, "content-length:", &ve );
    if( v ) {
      content_len = 0L;
      if( v>=ve ) return -1;
      for( ; v<ve; v++ ) {
        if( *v<'0' || *v>'9' ) return -1;
        content_len = content_len*10L+(*v-'0');
        if( content_len>(long)FD_BAM_REGISTRY_BODY_MAX ) return -1;
      }
    }
    v = reg_hdr( p, nl, "transfer-encoding:", &ve );
    if( v ) {
      if( (ulong)(ve-v)!=7UL || !reg_lower_eq( v, "chunked", 7UL ) ) return -1;
      chunked = 1;
    }
    p = nl+1;
  }
  ulong body_at = hdr_len+4UL;
  if( chunked ) {
    ulong n = 0UL;
    int rc = reg_chunked( buf+body_at, *len-body_at, 0, &n );
    if( rc<=0 ) return ( rc<0 || eof ) ? -1 : 0;
    reg_chunked( buf+body_at, *len-body_at, 1, &n );
    memmove( buf, buf+body_at, n );
    *len = n;
    return 1;
  }
  if( content_len>=0L ) {
    if( *len<body_at+(ulong)content_len ) return eof ? -1 : 0;
    memmove( buf, buf+body_at, (ulong)content_len );
    *len = (ulong)content_len;
    return 1;
  }
  if( !eof ) return 0;
  ulong n = *len-body_at;
  memmove( buf, buf+body_at, n );
  *len = n;
  return 1;
}

int
fd_bam_registry_http_redirect( char const * buf,
                               ulong        len,
                               char *       loc,
                               ulong        loc_max ) {
  char const * hdr_end = reg_hdr_end( buf, len );
  if( !hdr_end || len<12UL || memcmp( buf, "HTTP/1.", 7 ) || buf[8]!=' ' ) return 0;
  for( ulong i=9UL; i<12UL; i++ ) if( buf[ i ]<'0' || buf[ i ]>'9' ) return 0;
  int code = (buf[9]-'0')*100 + (buf[10]-'0')*10 + (buf[11]-'0');
  if( code!=301 && code!=302 && code!=303 && code!=307 && code!=308 ) return 0;
  for( char const * p=buf; p<hdr_end; ) {
    char const * nl = memchr( p, '\n', (ulong)(hdr_end-p) );
    if( !nl ) nl = hdr_end;
    char const * ve = NULL;
    char const * v  = reg_hdr( p, nl, "location:", &ve );
    if( v ) {
      ulong n = (ulong)(ve-v);
      if( !n || n>=loc_max ) return 0;
      memcpy( loc, v, n );
      loc[ n ] = '\0';
      return 1;
    }
    p = nl+1;
  }
  return 0;
}

static int
reg_set_path( fd_bam_registry_t * reg,
              char const *        a,
              ulong               a_len,
              char const *        b,
              ulong               b_len ) {
  ulong bn = 0UL;
  while( bn<b_len && b[ bn ]!='#' ) bn++;
  if( a_len+bn+1UL>=FD_BAM_REGISTRY_PATH_MAX ) return -1;
  char path[ FD_BAM_REGISTRY_PATH_MAX ];
  if( a_len ) memcpy( path, a, a_len );
  memcpy( path+a_len, b, bn );
  ulong n = a_len+bn;
  if( !n || path[0]!='/' ) {
    memmove( path+1, path, n );
    path[0] = '/';
    n++;
  }
  memcpy( reg->f_path, path, n );
  reg->f_path[ n ] = '\0';
  reg->f_path_len = (ushort)n;
  return 0;
}

int
fd_bam_registry_redirect_target( fd_bam_registry_t * reg,
                                 char const *        loc ) {
  ulong n = strlen( loc );
  char abs[ FD_URL_MAX+FD_BAM_REGISTRY_PATH_MAX+8UL ];
  if( n>=2UL && loc[0]=='/' && loc[1]=='/' ) {
    if( !fd_cstr_printf_check( abs, sizeof(abs), &n, "%s:%s", reg->f_ssl ? "https" : "http", loc ) ) return -1;
    loc = abs;
  }
  if( !strncmp( loc, "http://", 7UL ) || !strncmp( loc, "https://", 8UL ) ) {
    fd_url_t url[1];
    ushort port = 0;
    _Bool  ssl  = 0;
    if( fd_url_parse_endpoint( url, loc, n, &port, &ssl, "BAM registry redirect" ) ) return -1;
    if( !url->host_len || url->host_len>=sizeof(reg->f_host) ) return -1;
    if( reg_set_path( reg, NULL, 0UL, url->tail, url->tail_len ) ) return -1;
    memcpy( reg->f_host, url->host, url->host_len );
    reg->f_host[ url->host_len ] = '\0';
    reg->f_host_len = (ushort)url->host_len;
    reg->f_port     = port;
    reg->f_ssl      = (uchar)ssl;
    return 0;
  }
  if( loc[0]=='/' ) return reg_set_path( reg, NULL, 0UL, loc, n );
  /* Any other scheme is an error in reqwest. */
  for( char const * c=loc; *c && *c!='/' && *c!='?' && *c!='#'; c++ ) if( *c==':' ) return -1;
  /* Relative reference: keep the base up to its last '/', or the whole
     path when only the query changes. */
  ulong q = 0UL;
  while( q<reg->f_path_len && reg->f_path[ q ]!='?' ) q++;
  ulong base = q;
  if( loc[0]!='?' ) {
    base = 0UL;
    for( ulong i=0UL; i<q; i++ ) if( reg->f_path[ i ]=='/' ) base = i+1UL;
  }
  char cur[ FD_BAM_REGISTRY_PATH_MAX ];
  memcpy( cur, reg->f_path, base );
  return reg_set_path( reg, cur, base, loc, n );
}

/* ---- probe ------------------------------------------------------- */

/* Mirrors jito probe_and_rank: a shuffled pool, at most PROBE_FANOUT
   channels at once, PROBE_SAMPLES sequential GetBuilderConfig calls
   per channel, and a round budget that drops whatever is in flight. */

static void
reg_round_end( fd_bam_tile_t * ctx,
               long            now ) {
  fd_bam_registry_t * reg = &ctx->registry;
  for( ulong i=0UL; i<reg->slot_cnt; i++ ) slot_close( &reg->slots[ i ] );
  reg->probe_active = 0UL;
  if( now-reg->round_start>=FD_BAM_REGISTRY_PROBE_BUDGET_NS ) {
    FD_LOG_WARNING(( "BAM probe round hit its budget, ranking the %lu nodes that answered", reg->probe_answered ));
  }
  fd_bam_registry_finish_round( reg );
  reg->phase       = FD_BAM_REG_IDLE;
  reg->fetch_after = now+FD_BAM_REGISTRY_POLL_NS;
  FD_LOG_INFO(( "BAM probe round: %lu/%lu nodes answered, %lu usable",
                reg->probe_answered, reg->order_cnt, reg->ranked_cnt ));
}

static void
slot_done( fd_bam_tile_t *          ctx,
           fd_bam_registry_slot_t * s ) {
  fd_bam_registry_t * reg = &ctx->registry;
  if( s->best_us!=ULONG_MAX && reg->ranked_cnt<FD_BAM_REGISTRY_NODE_CAP ) {
    fd_bam_registry_rank_t * rank = &reg->ranked[ reg->ranked_cnt++ ];
    rank->node   = reg->nodes[ s->node_i ];
    rank->rtt_us = s->best_us;
    reg->probe_answered++;
  }
  slot_close( s );
  reg->probe_active--;
}

static int
slot_open( fd_bam_tile_t *          ctx,
           fd_bam_registry_slot_t * s,
           ulong                    node_i ) {
  fd_bam_registry_node_t const * node = &ctx->registry.nodes[ node_i ];
  char ip[ 16 ];
  ulong ip_len = 0UL;
  fd_cstr_printf( ip, sizeof(ip), &ip_len, FD_IP4_ADDR_FMT, FD_IP4_ADDR_FMT_ARGS( node->ip4 ) );
  int fd = reg_socket( ctx, node->ip4, node->grpc_port, 1 );
  if( fd<0 ) return -1;
  s->fd = fd;
  if( reg_tls_init( ctx->ca_store, s->tls, s->conn, ip, ip_len, 0 ) ) {
    slot_close( s );
    return -1;
  }
  fd_grpc_client_set_authority( s->grpc, ip, ip_len, node->grpc_port );
  s->node_i   = node_i;
  s->deadline = fd_bam_now()+FD_BAM_REGISTRY_PROBE_CONNECT_NS;
  return 0;
}

/* A node that cannot be opened scores nothing and its slot takes the
   next node, as one failed future does under buffer_unordered. */

static void
reg_probe_fill( fd_bam_tile_t * ctx ) {
  fd_bam_registry_t * reg = &ctx->registry;
  for( ulong i=0UL; i<reg->slot_cnt && reg->order_i<reg->order_cnt; i++ ) {
    fd_bam_registry_slot_t * s = &reg->slots[ i ];
    while( s->fd<0 && reg->order_i<reg->order_cnt ) {
      if( !slot_open( ctx, s, reg->order[ reg->order_i++ ] ) ) reg->probe_active++;
    }
  }
}

static void
slot_sample_done( fd_bam_registry_slot_t * s,
                  int                      ok ) {
  if( !s->inflight ) return;
  if( ok ) {
    long dt = fd_bam_now()-s->sample_ns;
    ulong us = dt>0L ? (ulong)(dt/1000L) : 0UL;
    if( us<s->best_us ) s->best_us = us;
  }
  s->inflight = 0;
  s->samples++;
}

static void
probe_rx_end( void *                app,
              ulong                 request_ctx,
              fd_grpc_resp_hdrs_t * resp ) {
  (void)request_ctx;
  slot_sample_done( app, resp && resp->grpc_status==FD_GRPC_STATUS_OK );
}

static void
probe_rx_timeout( void * app,
                  ulong  request_ctx,
                  int    deadline_kind ) {
  (void)request_ctx; (void)deadline_kind;
  slot_sample_done( app, 0 );
}

static void
probe_dead( void * app,
            uint   h2_err,
            int    closed_by ) {
  (void)h2_err; (void)closed_by;
  ((fd_bam_registry_slot_t *)app)->dead = 1;
}

static void
probe_nop( void * app ) {
  (void)app;
}

static void
probe_nop_u( void * app,
             ulong  request_ctx ) {
  (void)app; (void)request_ctx;
}

static void
probe_nop_msg( void *       app,
               void const * protobuf,
               ulong        protobuf_sz,
               ulong        request_ctx ) {
  (void)app; (void)protobuf; (void)protobuf_sz; (void)request_ctx;
}

static fd_grpc_client_callbacks_t const probe_cbs = {
  .conn_established = probe_nop,
  .conn_dead        = probe_dead,
  .tx_complete      = probe_nop_u,
  .rx_start         = probe_nop_u,
  .rx_msg           = probe_nop_msg,
  .rx_end           = probe_rx_end,
  .rx_timeout       = probe_rx_timeout,
  .ping_ack         = probe_nop,
};

ulong
fd_bam_registry_probe_align( void ) {
  return fd_ulong_max( alignof(fd_bam_registry_slot_t), fd_grpc_client_align() );
}

ulong
fd_bam_registry_probe_footprint( void ) {
  ulong l = FD_LAYOUT_INIT;
  l = FD_LAYOUT_APPEND( l, alignof(fd_bam_registry_slot_t), FD_BAM_REGISTRY_PROBE_FANOUT*sizeof(fd_bam_registry_slot_t) );
  for( ulong i=0UL; i<FD_BAM_REGISTRY_PROBE_FANOUT; i++ ) {
    l = FD_LAYOUT_APPEND( l, fd_grpc_client_align(), fd_grpc_client_footprint( FD_BAM_REGISTRY_PROBE_BUF ) );
  }
  return FD_LAYOUT_FINI( l, fd_bam_registry_probe_align() );
}

void
fd_bam_registry_join_probe( fd_bam_tile_t * ctx,
                            ulong           seed ) {
  fd_bam_registry_t * reg = &ctx->registry;
  if( !reg->probe_mem ) return;
  FD_SCRATCH_ALLOC_INIT( l, reg->probe_mem );
  reg->slots = FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_bam_registry_slot_t),
                                        FD_BAM_REGISTRY_PROBE_FANOUT*sizeof(fd_bam_registry_slot_t) );
  for( ulong i=0UL; i<FD_BAM_REGISTRY_PROBE_FANOUT; i++ ) {
    fd_bam_registry_slot_t * s = &reg->slots[ i ];
    void * grpc_mem = FD_SCRATCH_ALLOC_APPEND( l, fd_grpc_client_align(), fd_grpc_client_footprint( FD_BAM_REGISTRY_PROBE_BUF ) );
    memset( s, 0, sizeof(*s) );
    s->fd      = -1;
    s->best_us = ULONG_MAX;
    fd_tls_t * tls = fd_tls_join( fd_tls_new( s->tls ) );
    if( FD_UNLIKELY( !tls ) ) FD_LOG_CRIT(( "BAM registry probe tls failed" ));
    tls->rng = reg->rng;
    s->grpc = fd_grpc_client_new( grpc_mem, &probe_cbs, s->metrics, s, FD_BAM_REGISTRY_PROBE_BUF, seed+i );
    if( FD_UNLIKELY( !s->grpc ) ) FD_LOG_CRIT(( "BAM registry probe client failed" ));
  }
  reg->slot_cnt = FD_BAM_REGISTRY_PROBE_FANOUT;
}

static void
reg_start_fetch( fd_bam_tile_t * ctx ) {
  fd_bam_registry_t * reg = &ctx->registry;
  reg->fetch_start = fd_bam_now();
  reg->deadline    = reg->fetch_start+FD_BAM_REGISTRY_FETCH_TIMEOUT_NS;
  reg->phase       = FD_BAM_REG_FETCH;
  memcpy( reg->f_host, reg->host, reg->host_len+1UL );
  reg->f_host_len = reg->host_len;
  memcpy( reg->f_path, reg->path, reg->path_len+1UL );
  reg->f_path_len = reg->path_len;
  reg->f_port     = reg->port;
  reg->f_ssl      = reg->ssl;
  reg->redirects  = 0U;
  if( reg_fetch_begin( ctx ) ) reg_fetch_done( ctx, 0 );
}

static void
reg_start_round( fd_bam_tile_t * ctx ) {
  fd_bam_registry_t * reg = &ctx->registry;
  long now = fd_bam_now();
  if( !reg->slot_cnt ) {
    reg->probe_at = now+FD_BAM_REGISTRY_PROBE_COOLDOWN_NS;
    return;
  }
  reg->order_cnt      = reg->node_cnt;
  reg->order_i        = 0UL;
  reg->ranked_cnt     = 0UL;
  reg->probe_active   = 0UL;
  reg->probe_answered = 0UL;
  reg->round_start    = now;
  reg->probe_at       = now+FD_BAM_REGISTRY_PROBE_COOLDOWN_NS;
  for( ulong i=0UL; i<reg->order_cnt; i++ ) reg->order[ i ] = i;
  for( ulong i=reg->order_cnt; i>1UL; i-- ) {
    ulong j = fd_rng_ulong_roll( ctx->rng, i );
    ulong tmp = reg->order[ j ];
    reg->order[ j ] = reg->order[ i-1UL ];
    reg->order[ i-1UL ] = tmp;
  }
  reg->phase = FD_BAM_REG_PROBE;
  reg_probe_fill( ctx );
  if( !reg->probe_active ) reg_round_end( ctx, now );
}

static void
reg_drive_tls( fd_bam_tile_t * ctx,
               int *           failed ) {
  fd_bam_registry_t * reg = &ctx->registry;
  ulong rx_sz = 0UL;
  int rc = fd_tlsrec_sock_rx( reg->sock, reg->conn, reg->fd, &rx_sz );
  if( rc==FD_TLSREC_SOCK_ERR_EOF && !fd_tlsrec_conn_is_ready( reg->conn ) ) *failed = 1;
  else if( rc && rc!=FD_TLSREC_SOCK_ERR_EOF ) *failed = 1;
  if( reg_cert_failed( reg, reg->conn, reg->tls ) ) *failed = 1;
}

/* reg_fetch_rx appends received plaintext to body.  Returns 1 on EOF,
   0 otherwise, -1 on error or overflow. */

static int
reg_fetch_rx( fd_bam_registry_t * reg ) {
  if( !reg->f_ssl ) {
    ulong room = FD_BAM_REGISTRY_BODY_MAX-reg->body_len;
    if( !room ) return -1;
    long sz = recv( reg->fd, reg->body+reg->body_len, room, MSG_NOSIGNAL|MSG_DONTWAIT );
    if( !sz ) return 1;
    if( sz<0 ) return ( errno==EAGAIN || errno==EWOULDBLOCK ) ? 0 : -1;
    reg->body_len += (ulong)sz;
    return 0;
  }
  ulong avail = fd_tlsrec_sock_rx_avail( reg->sock );
  if( reg->body_len+avail>FD_BAM_REGISTRY_BODY_MAX ) return -1;
  if( avail ) {
    fd_tlsrec_sock_rx_pop( reg->sock, reg->body+reg->body_len, avail );
    reg->body_len += avail;
  }
  int eof = 0;
  ulong ignored = 0UL;
  int rx = fd_tlsrec_sock_rx( reg->sock, reg->conn, reg->fd, &ignored );
  if( rx==FD_TLSREC_SOCK_ERR_EOF ) eof = 1;
  else if( rx ) return -1;
  avail = fd_tlsrec_sock_rx_avail( reg->sock );
  if( reg->body_len+avail>FD_BAM_REGISTRY_BODY_MAX ) return -1;
  if( avail ) {
    fd_tlsrec_sock_rx_pop( reg->sock, reg->body+reg->body_len, avail );
    reg->body_len += avail;
  }
  return eof;
}

/* reg_fetch_tx sends the rest of req.  Returns 1 once all of it is
   out, 0 to retry, -1 on error. */

static int
reg_fetch_tx( fd_bam_registry_t * reg,
              char const *        req,
              ulong               req_len ) {
  if( !reg->f_ssl ) {
    if( reg->http_off>=req_len ) return 1;
    long sz = send( reg->fd, req+reg->http_off, req_len-reg->http_off, MSG_NOSIGNAL|MSG_DONTWAIT );
    if( sz<0 ) return ( errno==EAGAIN || errno==EWOULDBLOCK ) ? 0 : -1;
    reg->http_off += (ulong)sz;
    return reg->http_off>=req_len;
  }
  if( fd_tlsrec_sock_tx_pending( reg->sock ) ) {
    int fl = fd_tlsrec_sock_flush( reg->sock, reg->fd );
    if( fl<0 ) return -1;
    if( fl==1 ) return 0;
  }
  if( reg->http_off>=req_len ) return 1;
  ulong consumed = 0UL;
  if( fd_tlsrec_sock_tx( reg->sock, reg->conn, reg->fd, req+reg->http_off, req_len-reg->http_off, &consumed ) ) return -1;
  reg->http_off += consumed;
  return 0;
}

static void
reg_drive_fetch( fd_bam_tile_t * ctx ) {
  fd_bam_registry_t * reg = &ctx->registry;
  long now = fd_bam_now();
  if( now>=reg->deadline ) { reg_fetch_done( ctx, 0 ); return; }

  if( reg->io==FD_BAM_REG_IO_CONNECT ) {
    int ready = reg_tcp_ready( reg );
    if( ready<0 ) {
      if( reg_fetch_connect( ctx ) ) reg_fetch_done( ctx, 0 );
      return;
    }
    if( !ready ) return;
    reg->io = reg->f_ssl ? FD_BAM_REG_IO_TLS : FD_BAM_REG_IO_BODY;
  }

  if( reg->f_ssl ) {
    int failed = 0;
    reg_drive_tls( ctx, &failed );
    if( failed ) { reg_fetch_done( ctx, 0 ); return; }
    if( !fd_tlsrec_conn_is_ready( reg->conn ) ) return;
    if( reg->io==FD_BAM_REG_IO_TLS ) reg->io = FD_BAM_REG_IO_BODY;
  }

  char port[ 8 ] = {0};
  if( reg->f_port!=( reg->f_ssl ? 443 : 80 ) ) fd_cstr_printf( port, sizeof(port), NULL, ":%hu", reg->f_port );
  char req[ FD_BAM_REGISTRY_PATH_MAX+FD_FQDN_BUF_MAX+128UL ];
  ulong req_len = 0UL;
  if( !fd_cstr_printf_check( req, sizeof(req), &req_len,
                             "GET %s HTTP/1.1\r\nHost: %s%s\r\nConnection: close\r\nAccept: */*\r\n\r\n",
                             reg->f_path, reg->f_host, port ) ) {
    reg_fetch_done( ctx, 0 );
    return;
  }
  int tx = reg_fetch_tx( reg, req, req_len );
  if( tx<0 ) { reg_fetch_done( ctx, 0 ); return; }
  if( !tx ) return;

  int eof = reg_fetch_rx( reg );
  if( eof<0 ) { reg_fetch_done( ctx, 0 ); return; }

  char loc[ FD_URL_MAX+FD_BAM_REGISTRY_PATH_MAX ];
  if( fd_bam_registry_http_redirect( reg->body, reg->body_len, loc, sizeof(loc) ) ) {
    if( ++reg->redirects>FD_BAM_REGISTRY_REDIRECT_MAX ||
        fd_bam_registry_redirect_target( reg, loc ) ||
        reg_fetch_begin( ctx ) ) {
      reg_fetch_done( ctx, 0 );
    }
    return;
  }
  int done = fd_bam_registry_http_body( reg->body, &reg->body_len, eof );
  if( !done && reg->body_len>=FD_BAM_REGISTRY_BODY_MAX ) done = -1;
  if( !done ) return;
  if( done<0 ) { reg_fetch_done( ctx, 0 ); return; }
  reg_fetch_done( ctx, 1 );
}

/* slot_drive returns 1 once the slot is finished: connect timeout,
   channel failure, or PROBE_SAMPLES calls made. */

static int
slot_drive( fd_bam_tile_t *          ctx,
            fd_bam_registry_slot_t * s,
            long                     now ) {
  int busy = 0;
  int io   = fd_grpc_client_rxtx_tls( s->grpc, s->conn, s->fd, now, &busy );
  int cert = reg_cert_failed( &ctx->registry, s->conn, s->tls ); /* counted even when io failed */
  if( s->dead || io || cert ) return 1;
  if( !fd_grpc_client_is_connected( s->grpc ) ) return now>=s->deadline;

  if( s->inflight && now-s->sample_ns>=FD_BAM_REGISTRY_PROBE_REQUEST_NS ) slot_sample_done( s, 0 );
  if( s->inflight ) return 0;
  if( s->samples>=FD_BAM_REGISTRY_PROBE_SAMPLES ) return 1;

  bam_api_ConfigRequest req = bam_api_ConfigRequest_init_default;
  long started = fd_bam_now();
  fd_grpc_h2_stream_t * stream = fd_grpc_client_request_start( s->grpc, FD_BAM_REG_CONFIG_PATH,
                                                              sizeof(FD_BAM_REG_CONFIG_PATH)-1UL, 1UL,
                                                              &bam_api_ConfigRequest_msg, &req, NULL, 0UL, 0 );
  if( FD_UNLIKELY( !stream ) ) {
    s->samples++;
    return s->samples>=FD_BAM_REGISTRY_PROBE_SAMPLES;
  }
  fd_grpc_client_deadline_set( stream, FD_GRPC_DEADLINE_RX_END, now+FD_BAM_REGISTRY_PROBE_REQUEST_NS );
  s->inflight  = 1;
  s->sample_ns = started;
  /* Put the request on the wire now so the sample starts at send. */
  if( fd_grpc_client_rxtx_tls( s->grpc, s->conn, s->fd, now, &busy ) || s->dead ) return 1;
  return 0;
}

static void
reg_drive_probe( fd_bam_tile_t * ctx ) {
  fd_bam_registry_t * reg = &ctx->registry;
  long now = fd_bam_now();
  if( now-reg->round_start>=FD_BAM_REGISTRY_PROBE_BUDGET_NS ) {
    reg_round_end( ctx, now );
    return;
  }

  /* One poll for every slot still in TCP connect. */
  struct pollfd pfd[ FD_BAM_REGISTRY_PROBE_FANOUT ];
  ulong         pidx[ FD_BAM_REGISTRY_PROBE_FANOUT ];
  ulong         pcnt = 0UL;
  for( ulong i=0UL; i<reg->slot_cnt; i++ ) {
    fd_bam_registry_slot_t * s = &reg->slots[ i ];
    if( s->fd<0 || s->io!=FD_BAM_REG_IO_CONNECT ) continue;
    if( now>=s->deadline ) { slot_done( ctx, s ); continue; }
    pfd[ pcnt ]  = (struct pollfd){ .fd = s->fd, .events = POLLOUT };
    pidx[ pcnt ] = i;
    pcnt++;
  }
  if( pcnt ) {
    int pr = fd_syscall_poll( pfd, (uint)pcnt, 0 );
    if( FD_UNLIKELY( pr<0 ) ) {
      FD_LOG_WARNING(( "BAM registry poll failed (%i-%s)", errno, fd_io_strerror( errno ) ));
    } else {
      for( ulong k=0UL; pr>0 && k<pcnt; k++ ) {
        if( !pfd[ k ].revents ) continue;
        fd_bam_registry_slot_t * s = &reg->slots[ pidx[ k ] ];
        if( reg_tcp_check( s->fd, pfd[ k ].revents, 1 )<0 ) slot_done( ctx, s );
        else s->io = FD_BAM_REG_IO_TLS;
      }
    }
  }

  for( ulong i=0UL; i<reg->slot_cnt; i++ ) {
    fd_bam_registry_slot_t * s = &reg->slots[ i ];
    if( s->fd<0 || s->io==FD_BAM_REG_IO_CONNECT ) continue;
    if( slot_drive( ctx, s, now ) ) slot_done( ctx, s );
  }

  reg_probe_fill( ctx );
  if( !reg->probe_active && reg->order_i>=reg->order_cnt ) reg_round_end( ctx, now );
}

static void
reg_idle( fd_bam_tile_t * ctx,
          int *           charge_busy,
          long *          deadline ) {
  fd_bam_registry_t * reg = &ctx->registry;
  long now = fd_bam_now();
  int live = reg_live( ctx );
  if( !live ) reg->resync_at = fd_long_min( reg->resync_at, now+FD_BAM_REGISTRY_RESYNC_STALLED_NS );
  if( now>=reg->resync_at && now>=reg->fetch_after ) {
    reg_start_fetch( ctx );
    *charge_busy = 1;
    *deadline = now;
    return;
  }
  if( !reg->ranked_cnt && reg->node_cnt && now>=reg->probe_at ) {
    reg_start_round( ctx );
    *charge_busy = 1;
    *deadline = now;
    return;
  }
  fd_bam_registry_node_t node;
  if( fd_bam_registry_choose( reg, live, now, &node ) && fd_bam_tile_dial_node( ctx, &node ) ) {
    reg->stalled_since = now;
  }
  *deadline = fd_long_max( reg->resync_at, reg->fetch_after );
  if( !reg->ranked_cnt && reg->node_cnt ) *deadline = fd_long_min( *deadline, reg->probe_at );
  *deadline = fd_long_min( *deadline, now+FD_BAM_REGISTRY_POLL_NS );
}

void
fd_bam_registry_step( fd_bam_tile_t * ctx,
                      int *           charge_busy,
                      long *          deadline ) {
  *deadline = LONG_MAX;
  if( !ctx->registry.on ) return;
  if( ctx->registry.fd<0 && ctx->registry.phase==FD_BAM_REG_IDLE ) {
    reg_idle( ctx, charge_busy, deadline );
    return;
  }
  *charge_busy = 1;
  *deadline = fd_bam_now();
  if( ctx->registry.phase==FD_BAM_REG_FETCH ) reg_drive_fetch( ctx );
  else if( ctx->registry.phase==FD_BAM_REG_PROBE ) reg_drive_probe( ctx );
  else reg_idle( ctx, charge_busy, deadline );
}
