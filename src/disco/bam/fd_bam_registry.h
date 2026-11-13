#ifndef HEADER_fd_src_disco_bam_fd_bam_registry_h
#define HEADER_fd_src_disco_bam_fd_bam_registry_h

/* BAM registry follower.  A --bam-url whose path is not "/" is a node
   list (jito-solana bam_discovery).  FireBAM GETs that document over
   HTTP(S) and dials the chosen node with the existing gRPC client. */

#include "fd_bam_tile.h"
#include "../../waltz/http/fd_url.h"
#include "../../waltz/grpc/fd_grpc_client.h"
#include "../../waltz/tls/fd_tls.h"
#include "../../waltz/tlsrec/fd_tlsrec.h"
#include "../../waltz/tlsrec/fd_tlsrec_sock.h"
#include "../../util/rng/fd_rng.h"
#include "../metrics/fd_metrics.h"

/* jito-solana has no such limits; these sit far above any real
   registry (BODY_MAX fits NODE_CAP nodes of 256 bytes each). */
#define FD_BAM_REGISTRY_NODE_CAP   (1024UL)
#define FD_BAM_REGISTRY_BODY_MAX   (1UL<<18)
#define FD_BAM_REGISTRY_PATH_MAX   (1024UL)
#define FD_BAM_REGISTRY_ADDR_MAX   (16UL)
#define FD_BAM_REGISTRY_REDIRECT_MAX (10U)  /* reqwest Policy::default() */
#define FD_BAM_REGISTRY_PROBE_BUF  (8192UL)
#define FD_BAM_REGISTRY_MAX_RTT_US (30000UL)

#define FD_BAM_REGISTRY_RESYNC_LIVE_NS    ((long)60e9)
#define FD_BAM_REGISTRY_RESYNC_STALLED_NS ((long)2e9)
#define FD_BAM_REGISTRY_CONNECT_GRACE_NS  ((long)20e9)
#define FD_BAM_REGISTRY_PROBE_COOLDOWN_NS ((long)30e9)
#define FD_BAM_REGISTRY_FETCH_TIMEOUT_NS  ((long)5e9)
#define FD_BAM_REGISTRY_PROBE_CONNECT_NS  ((long)500e6)
#define FD_BAM_REGISTRY_PROBE_REQUEST_NS  ((long)60e6)
#define FD_BAM_REGISTRY_PROBE_BUDGET_NS   ((long)10e9)
#define FD_BAM_REGISTRY_PROBE_SAMPLES     (3U)
/* jito probes 32 nodes at once.  Each slot holds a TLS and a gRPC
   client (~300 KB), and a reply that lands while the tile handshakes
   with other slots reads late, so FireBAM probes 8: a round still
   covers ~160 unreachable nodes (PROBE_CONNECT_NS each) in budget. */
#define FD_BAM_REGISTRY_PROBE_FANOUT      (8UL)
#define FD_BAM_REGISTRY_POLL_NS           ((long)1e9)

struct fd_bam_registry_node {
  uint   ip4;       /* IPv4, FD_IP4_ADDR_FMT order */
  ushort grpc_port;
  char   region[ 64 ]; /* longer names are truncated */
};

typedef struct fd_bam_registry_node fd_bam_registry_node_t;

struct fd_bam_registry_rank {
  fd_bam_registry_node_t node;
  ulong                  rtt_us;
};

typedef struct fd_bam_registry_rank fd_bam_registry_rank_t;

/* One in-flight probe: a TLS channel to one node and its RTT samples.
   fd<0 marks a free slot.  Each slot owns its fd_tls_t because the
   ephemeral key and SNI live there for the whole handshake. */

struct fd_bam_registry_slot {
  int                fd;
  uchar              io;
  uchar              inflight;
  uchar              dead;
  uint               samples;
  ulong              node_i;
  ulong              best_us;
  long               deadline;
  long               sample_ns;
  fd_grpc_client_t * grpc;
  fd_grpc_client_metrics_t metrics[1];
  fd_tls_t           tls[1];
  fd_tlsrec_conn_t   conn[1];
};

typedef struct fd_bam_registry_slot fd_bam_registry_slot_t;

struct fd_bam_registry {
  uchar  on;
  uchar  cert_verify_cfg; /* tiles.bam.tls_cert_verify; kept for direct URLs */
  uchar  have_dial;
  uchar  ssl;
  char   url[ FD_URL_MAX ];
  char   host[ FD_FQDN_BUF_MAX ];
  ushort host_len;
  ushort port;
  char   path[ FD_BAM_REGISTRY_PATH_MAX ];
  ushort path_len;

  /* Where the current fetch points after redirects. */
  char   f_host[ FD_FQDN_BUF_MAX ];
  ushort f_host_len;
  ushort f_port;
  char   f_path[ FD_BAM_REGISTRY_PATH_MAX ];
  ushort f_path_len;
  uchar  f_ssl;
  uint   redirects;
  uint   addrs[ FD_BAM_REGISTRY_ADDR_MAX ];
  ulong  addr_cnt;
  ulong  addr_i;
  uint   dns_ns[ 3 ];    /* MAXNS */
  ulong  dns_ns_cnt;
  uchar  dns_query[ 280 ]; /* FD_DNS_QUERY_MTU */
  int    dns_qlen;
  long   dns_retry;
  long   dns_send_at;

  fd_bam_registry_node_t nodes[ FD_BAM_REGISTRY_NODE_CAP ];
  ulong                  node_cnt;
  fd_bam_registry_rank_t ranked[ FD_BAM_REGISTRY_NODE_CAP ];
  ulong                  ranked_cnt;
  ulong                  cursor;
  uint                   dial_ip4;
  ushort                 dial_port;

  long resync_at;
  long probe_at;
  long stalled_since;
  long deadline;
  long round_start;
  long fetch_start;
  long fetch_after; /* jito sleeps POLL_INTERVAL between steps */

  ulong order[ FD_BAM_REGISTRY_NODE_CAP ];
  ulong order_i;      /* next node to launch */
  ulong order_cnt;
  ulong probe_active; /* slots with an open fd */
  ulong probe_answered;

  uchar phase; /* 0 idle, 1 fetch, 2 probe */
  uchar io;    /* 0 connect, 1 tls, 2 payload */
  int   fd;
  ulong http_off;
  ulong body_len;
  char  body[ FD_BAM_REGISTRY_BODY_MAX ];
  fd_bam_registry_node_t parsed[ FD_BAM_REGISTRY_NODE_CAP ];

  fd_tls_t           tls[1];
  fd_chacha_rng_t    rng[1];
  fd_tlsrec_conn_t   conn[1];
  fd_tlsrec_sock_t   sock[1];
  void *                   probe_mem;
  fd_bam_registry_slot_t * slots;
  ulong                    slot_cnt;

  ulong fetch_cnt[ FD_METRICS_ENUM_BAM_REGISTRY_FETCH_RESULT_CNT ];
  ulong round_cnt[ FD_METRICS_ENUM_BAM_REGISTRY_PROBE_ROUND_RESULT_CNT ];
  ulong last_round[ FD_METRICS_ENUM_BAM_REGISTRY_PROBE_NODE_RESULT_CNT ];
  ulong probe_cert_cnt;
  ulong select_cnt;
  ulong dial_rtt_us;
};

typedef struct fd_bam_registry fd_bam_registry_t;

FD_PROTOTYPES_BEGIN

/* fd_bam_registry_is_url returns 1 when the path is longer than "/". */

int
fd_bam_registry_is_url( fd_url_t const * url );

/* fd_bam_registry_parse reads a registry document into nodes with
   serde_json's accept/reject rules for jito's ServedNodes (fd_jtok is
   stricter only inside values serde_json skips, which its own output
   never has).  Valid IPv6 nodes are skipped.  Returns 0 and sets *cnt
   on a non-empty IPv4 list.  Returns -2 when the document lists no
   IPv4 node, -1 when it is malformed or over cap; the caller keeps its
   cache either way.

   TODO: IPv6 nodes and registry hosts, which jito-solana supports.
   Needs IPv6 in fd_bam_client, bracketed hosts in fd_url, and IPv6
   iPAddress SAN matching in fd_x509. */

int
fd_bam_registry_parse( char const *             json,
                       ulong                    json_sz,
                       fd_bam_registry_node_t * nodes,
                       ulong                    cap,
                       ulong *                  cnt );

/* fd_bam_registry_http_body strips the response to an HTTP/1.0
   request in buf[0,*len) to its body in place.  The body is
   Content-Length bytes, or runs to EOF.  Returns 1 with *len set, 0 if
   more bytes are needed, -1 on a non-2xx status, a transfer coding, or
   malformed framing. */

int
fd_bam_registry_http_body( char *  buf,
                           ulong * len,
                           int     eof );

/* fd_bam_registry_http_redirect returns 1 once the headers in
   buf[0,len) are complete and carry a 301/302/303/307/308 with a
   Location that fits in loc, which receives it NUL-terminated.
   Returns 0 otherwise; fd_bam_registry_http_body then fails any 3xx. */

int
fd_bam_registry_http_redirect( char const * buf,
                               ulong        len,
                               char *       loc,
                               ulong        loc_max );

/* fd_bam_registry_redirect_target points the fetch at loc: an
   absolute http(s) URL, or a path on the current host, without the
   fragment.  Returns -1 for anything else (relative and
   scheme-relative references, other schemes) and for an oversized
   host or path. */

int
fd_bam_registry_redirect_target( fd_bam_registry_t * reg,
                                 char const *        loc );

/* fd_bam_registry_take_url returns 1 when raw is now the registry
   target, 0 when raw is a direct node (registry mode cleared), and
   -1 when its host or path is too long or no system CA bundle was
   loaded (registry fetches and nodes are always verified). */

int
fd_bam_registry_take_url( fd_bam_tile_t *  ctx,
                          char const *     raw,
                          fd_url_t const * url,
                          int              is_ssl,
                          ushort           port,
                          char *           err,
                          ulong            err_sz );

/* fd_bam_registry_disable drops registry mode and closes its socket.
   When cert_verify_cfg is off, the main client stops verifying too. */

void
fd_bam_registry_disable( fd_bam_tile_t * ctx );

/* fd_bam_registry_step advances one fetch, probe, or idle action.
   *charge_busy is set when the socket needs another poll immediately.
   *deadline is the next wake when the follower is idle. */

void
fd_bam_registry_step( fd_bam_tile_t * ctx,
                      int *           charge_busy,
                      long *          deadline );

/* fd_bam_registry_on_fetch replaces the cached list when any node
   differs, region included, and drops the ranking.  live is non-zero
   when the session is CONNECTED_HEALTHY. */

void
fd_bam_registry_on_fetch( fd_bam_registry_t *            reg,
                          fd_bam_registry_node_t const * nodes,
                          ulong                          cnt,
                          long                           now,
                          int                            live,
                          fd_rng_t *                     rng );

/* fd_bam_registry_finish_round sorts by RTT and drops nodes slower
   than FD_BAM_REGISTRY_MAX_RTT_US. */

void
fd_bam_registry_finish_round( fd_bam_registry_t * reg );

/* fd_bam_registry_choose returns 1 if the caller should dial *out.
   Wrapping the cursor clears the ranking but keeps the stall clock, so
   the next ranking publishes its best node at once. */

int
fd_bam_registry_choose( fd_bam_registry_t *      reg,
                        int                      live,
                        long                     now,
                        fd_bam_registry_node_t * out );

/* fd_bam_tile_dial_node returns 1 if the gRPC client was pointed at
   node, or 0 if it already was. */

int
fd_bam_tile_dial_node( fd_bam_tile_t *                ctx,
                       fd_bam_registry_node_t const * node );

/* fd_bam_registry_probe_{align,footprint} size the scratch region for
   FD_BAM_REGISTRY_PROBE_FANOUT probe slots and their gRPC clients. */

ulong
fd_bam_registry_probe_align( void );

ulong
fd_bam_registry_probe_footprint( void );

/* fd_bam_registry_join_probe lays out the probe slots in probe_mem,
   which must already be set. */

void
fd_bam_registry_join_probe( fd_bam_tile_t * ctx,
                            ulong           seed );

FD_PROTOTYPES_END

#endif /* HEADER_fd_src_disco_bam_fd_bam_registry_h */
