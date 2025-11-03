#define _GNU_SOURCE
#include "fd_bam_tile_private.h"
#include "../metrics/fd_metrics.h"
#include "../topo/fd_topo.h"
#include "../keyguard/fd_keyload.h"
#include "../plugin/fd_plugin.h"
#include "../../waltz/http/fd_url.h"
#include "../../tango/fseq/fd_fseq.h"
#include "../../util/pod/fd_pod_format.h"

#include <errno.h>
#include <dirent.h> /* opendir */
#include <stdio.h> /* snprintf */
#include <string.h>
#include <fcntl.h> /* F_SETFL */
#include <unistd.h> /* close */
#include <sys/mman.h> /* PROT_READ (seccomp) */
#include <sys/uio.h> /* writev */
#include <limits.h>
#include <netinet/in.h> /* AF_INET */
#include <netinet/tcp.h> /* TCP_FASTOPEN_CONNECT (seccomp) */
#include "../../waltz/resolv/fd_netdb.h"

#include "../bundle/generated/fd_bundle_tile_seccomp.h"

/* Provided by fdctl/firedancer version.c */
extern char const fdctl_version_string[];

FD_FN_CONST static ulong
scratch_align( void ) {
  return alignof(fd_bam_tile_t);
}

FD_FN_CONST static ulong
scratch_footprint( fd_topo_tile_t const * tile ) {
  (void)tile;
  ulong l = FD_LAYOUT_INIT;
  l = FD_LAYOUT_APPEND( l, alignof(fd_bam_tile_t), sizeof(fd_bam_tile_t)                        );
  l = FD_LAYOUT_APPEND( l, fd_grpc_client_align(),    fd_grpc_client_footprint( tile->bam.buf_sz ) );
  l = FD_LAYOUT_APPEND( l, fd_alloc_align(),          fd_alloc_footprint()                            );
  return FD_LAYOUT_FINI( l, 32 );
}

FD_FN_CONST static inline ulong
loose_footprint( fd_topo_tile_t const * tile ) {
  (void)tile;
  /* Leftover space for OpenSSL allocations */
  return 1UL<<26; /* 64 MiB */
}

static inline void
metrics_write( fd_bam_tile_t * ctx ) {
  FD_MCNT_SET( BAM, TRANSACTION_RECEIVED,   ctx->metrics.txn_received_cnt          );
  FD_MCNT_SET( BAM, BUNDLE_RECEIVED,        ctx->metrics.bundle_received_cnt       );
  FD_MCNT_SET( BAM, BUNDLE_RESULTS_DROPPED, ctx->metrics.bundle_result_drop_cnt    );
  FD_MCNT_SET( BAM, PACKETS_DROPPED,        ctx->metrics.packet_drop_cnt           );
  FD_MCNT_SET( BAM, KEEPALIVES,             ctx->metrics.ping_ack_cnt              );
  FD_MCNT_SET( BAM, HEARTBEATS_SENT,        ctx->metrics.heartbeat_sent_cnt        );
  FD_MCNT_SET( BAM, HEARTBEATS_RECEIVED,    ctx->metrics.heartbeat_recv_cnt        );
  FD_MCNT_SET( BAM, CONNECTIONS,            ctx->metrics.connection_cnt            );
  FD_MCNT_SET( BAM, DISCONNECTS,            ctx->metrics.disconnect_cnt            );
  FD_MCNT_SET( BAM, ERRORS_PROTOBUF,        ctx->metrics.decode_fail_cnt           );
  FD_MCNT_SET( BAM, ERRORS_TRANSPORT,       ctx->metrics.transport_fail_cnt        );
  FD_MCNT_SET( BAM, ERRORS_TIMEOUT,         ctx->metrics.timeout_fail_cnt          );
  FD_MCNT_SET( BAM, ERRORS_NO_FEE_INFO,     ctx->metrics.missing_builder_info_fail_cnt );
  FD_MCNT_SET( BAM, RESULTS_SENT,           ctx->metrics.result_sent_cnt           );
  FD_MCNT_SET( BAM, LEADER_STATE_SENT,      ctx->metrics.leader_state_sent_cnt     );

  FD_MGAUGE_SET( BAM, RTT_SAMPLE,   (ulong)ctx->rtt->latest_rtt   );
  FD_MGAUGE_SET( BAM, RTT_SMOOTHED, (ulong)ctx->rtt->smoothed_rtt );
  FD_MGAUGE_SET( BAM, RTT_VAR,      (ulong)ctx->rtt->var_rtt      );
  FD_MGAUGE_SET( BAM, RESULTS_QUEUE_DEPTH, (ulong)ctx->bam_pending_results );
  FD_MGAUGE_SET( BAM, RUNTIME_ENABLED,     (ulong)ctx->runtime_enabled );

  FD_MHIST_COPY( BAM, MESSAGE_RX_DELAY_NANOS, ctx->metrics.msg_rx_delay );

  fd_wksp_t * wksp = fd_wksp_containing( ctx );
  fd_wksp_usage_t usage[1];
  ulong const free_tag = 0UL;
  if( FD_UNLIKELY( !fd_wksp_usage( wksp, &free_tag, 1UL, usage ) ) ) {
    FD_LOG_ERR(( "fd_wksp_usage failed" )); /* unreachable */
  }
  FD_MGAUGE_SET( BAM, HEAP_SIZE,       usage->total_sz );
  FD_MGAUGE_SET( BAM, HEAP_FREE_BYTES, usage->used_sz  );

  int bundle_status = fd_bam_client_status( ctx );
  FD_MGAUGE_SET( BAM, CONNECTED, bundle_status == FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTED );
  ctx->bundle_status_recent = (uchar)bundle_status;
}

void
fd_bam_publish_gossip_update( fd_bam_tile_t *    ctx,
                              fd_stem_context_t * stem,
                              uint                use_bam ) {
  if( FD_UNLIKELY( !ctx->gossip_out.mem ) ) return;

  /* The gossip tile reads these control messages and mutates its local
     contact-info state.  Keep the payload minimal so the reliable bus
     round-trips quickly. */
  fd_bam_contact_update_t * msg =
      fd_chunk_to_laddr( ctx->gossip_out.mem, ctx->gossip_out.chunk );
  fd_memset( msg, 0, sizeof(fd_bam_contact_update_t) );
  msg->use_bam = use_bam ? FD_BAM_CONTACT_USE_BAM : FD_BAM_CONTACT_USE_DEFAULT;
  if( FD_LIKELY( use_bam ) ) {
    msg->tpu_addr      = ctx->bam_tpu_addr;
    msg->tpu_quic_addr = ctx->bam_tpu_quic_addr;
  }

  ulong tspub = (ulong)fd_frag_meta_ts_comp( fd_bam_now() );
  fd_stem_publish( stem,
                   ctx->gossip_out.idx,
                   FD_BAM_STEM_SIG_GOSSIP_UPDATE,
                   ctx->gossip_out.chunk,
                   sizeof(fd_bam_contact_update_t),
                   0UL,
                   0UL,
                   tspub );
  ctx->gossip_out.chunk = fd_dcache_compact_next( ctx->gossip_out.chunk,
                                                  sizeof(fd_bam_contact_update_t),
                                                  ctx->gossip_out.chunk0,
                                                  ctx->gossip_out.wmark );
}

void
fd_bam_update_contact_info( fd_bam_tile_t *    ctx,
                            fd_stem_context_t * stem,
                            int                 status,
                            int                 prev_status ) {
  if( FD_UNLIKELY( !ctx->gossip_out.mem ) ) return;

  int const connected     = ( status == FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTED );
  int const was_connected = ( prev_status == FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTED );
  int const have_contact  = ctx->bam_tpu_addr.l != 0UL;

  if( FD_UNLIKELY( connected && have_contact && !was_connected ) ) {
    fd_bam_publish_gossip_update( ctx, stem, 1U );
    return;
  }

  if( FD_UNLIKELY( was_connected && !connected && have_contact ) ) {
    /* A disconnect means Firedancer should resume advertising its local
       TPU ports so TPU clients do not get stuck targeting the BAM host. */
    fd_bam_publish_gossip_update( ctx, stem, 0U );
  }
}

static void fd_bam_tile_handle_ctrl( fd_bam_tile_t * ctx );

void
fd_bam_tile_housekeeping( fd_bam_tile_t * ctx ) {
  fd_bam_tile_handle_ctrl( ctx );

  long log_interval_ns = (long)30e9;
  int  status          = fd_bam_client_status( ctx );
  long log_next_ns     = ctx->last_bundle_status_log_nanos + log_interval_ns;
  long now_ns          = fd_log_wallclock();
  if( FD_UNLIKELY( status != FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTED && now_ns > log_next_ns ) ) {
    FD_LOG_WARNING(( "No bundle server connection in the last %ld seconds", log_interval_ns/(long)1e9 ) );
    ctx->last_bundle_status_log_nanos = now_ns;
  }

  if( FD_UNLIKELY( fd_keyswitch_state_query( ctx->keyswitch ) == FD_KEYSWITCH_STATE_SWITCH_PENDING ) ) {
    fd_memcpy( ctx->bam_url_pubkey, ctx->keyswitch->bytes, 32UL );
    fd_base58_encode_32( ctx->keyswitch->bytes, NULL, ctx->bam_validator_pubkey );
    fd_keyswitch_state( ctx->keyswitch, FD_KEYSWITCH_STATE_COMPLETED );
    ctx->defer_reset = 1;
  }
}

static void
bam_during_frag( fd_bam_tile_t * ctx,
                 ulong           in_idx,
                 ulong           seq,
                 ulong           sig,
                 ulong           chunk,
                 ulong           sz,
                 ulong           ctl ) {
  (void)seq;
  (void)sig;
  (void)ctl;

  if( FD_LIKELY( in_idx == ctx->bank_bam_in_idx ) ) {
    if( FD_UNLIKELY( sz != sizeof(fd_bam_bundle_result_t) ) ) {
      FD_LOG_WARNING(( "Unexpected BAM bundle result size %lu", sz ));
      return;
    }
    if( FD_UNLIKELY( ( chunk < ctx->bank_in.chunk0 ) | ( chunk > ctx->bank_in.wmark ) ) ) {
      FD_LOG_WARNING(( "BAM bundle result chunk %lu out of range [%lu,%lu]", chunk, ctx->bank_in.chunk0, ctx->bank_in.wmark ));
      return;
    }
    fd_bam_enqueue_result( ctx, fd_chunk_to_laddr( ctx->bank_in.mem, chunk ) );
    return;
  }

  if( FD_UNLIKELY( in_idx == ctx->pack_leader_in_idx ) ) {
    if( FD_LIKELY( sz == sizeof(fd_bam_leader_state_t) ) ) {
      if( FD_UNLIKELY( ( chunk < ctx->leader_in.chunk0 ) | ( chunk > ctx->leader_in.wmark ) ) ) {
        FD_LOG_WARNING(( "BAM leader state chunk %lu out of range [%lu,%lu]", chunk, ctx->leader_in.chunk0, ctx->leader_in.wmark ));
        return;
      }
      fd_bam_leader_state_t const * state = (fd_bam_leader_state_t const *)fd_chunk_to_laddr( ctx->leader_in.mem, chunk );
      ctx->bam_leader_state  = *state;
      ctx->bam_leader_pending = 1U;
      return;
    }
    if( FD_LIKELY( sz == sizeof(fd_bam_bundle_result_t) ) ) {
      if( FD_UNLIKELY( ( chunk < ctx->leader_in.chunk0 ) | ( chunk > ctx->leader_in.wmark ) ) ) {
        FD_LOG_WARNING(( "BAM bundle result chunk %lu out of range [%lu,%lu]", chunk, ctx->leader_in.chunk0, ctx->leader_in.wmark ));
        return;
      }
      fd_bam_bundle_result_t const * res = (fd_bam_bundle_result_t const *)fd_chunk_to_laddr( ctx->leader_in.mem, chunk );
      fd_bam_enqueue_result( ctx, res );
      return;
    }
    FD_LOG_WARNING(( "Unexpected pack->bam fragment size %lu", sz ));
  }
}

static void
fd_bam_tile_publish_block_engine_update(
    fd_bam_tile_t *  ctx,
    fd_stem_context_t * stem
) {
  fd_plugin_msg_block_engine_update_t * update =
      fd_chunk_to_laddr( ctx->plugin_out.mem, ctx->plugin_out.chunk );
  memset( update, 0, sizeof(fd_plugin_msg_block_engine_update_t) );

  strncpy( update->name, "bam", sizeof(update->name) );

  /* Deliberately silently truncates */
  snprintf( update->url, sizeof(update->url), "%s://%.*s:%u",
            ctx->is_ssl ? "https" : "http",
            (int)ctx->server_fqdn_len,
            ctx->server_fqdn,
            ctx->server_tcp_port );

  /* Format IPv4 string */
  snprintf( update->ip_cstr, sizeof(update->ip_cstr),
            FD_IP4_ADDR_FMT,
            FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ) );

  update->status = (uchar)ctx->bundle_status_recent;

  ulong tspub = (ulong)fd_frag_meta_ts_comp( fd_bam_now() );
  fd_stem_publish(
      stem,
      ctx->plugin_out.idx,
      FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE,
      ctx->plugin_out.chunk,
      sizeof(fd_plugin_msg_block_engine_update_t),
      0UL, /* ctl */
      0UL, /* seq */
      tspub
  );
  ctx->plugin_out.chunk = fd_dcache_compact_next( ctx->plugin_out.chunk, sizeof(fd_plugin_msg_block_engine_update_t), ctx->plugin_out.chunk0, ctx->plugin_out.wmark );
}

static void
after_credit( fd_bam_tile_t *  ctx,
              fd_stem_context_t * stem,
              int *               opt_poll_in,
              int *               charge_busy ) {
  (void)opt_poll_in;
  if( FD_UNLIKELY( !ctx->stem ) ) ctx->stem = stem;
  fd_bam_client_step( ctx, charge_busy );

  int bundle_status = fd_bam_client_status( ctx );
  int prev_status   = ctx->bundle_status_recent;
  ctx->bundle_status_recent = (uchar)bundle_status;
  if( FD_LIKELY( ctx->bam_status_fseq ) ) {
    /* Expose BAM connectivity via a shared latch.  The verify tile uses
       this to pause QUIC/bundle traffic when BAM has taken over leader
       duties.  fd_bam_client_status only returns CONNECTED once the
       transport, auth, and scheduler stream are fully live, so toggling
       the latch here guarantees peers see a consistent "BAM owns TPU"
       window. */
    ulong is_connected = (ulong)( bundle_status == FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTED );
    fd_fseq_update( ctx->bam_status_fseq, is_connected );
  }
  fd_bam_update_contact_info( ctx, stem, bundle_status, prev_status );

  if( ctx->plugin_out.mem ) {
    if( FD_UNLIKELY( ctx->bundle_status_recent != ctx->bundle_status_plugin ) ) {
      fd_bam_tile_publish_block_engine_update( ctx, stem );
      ctx->bundle_status_plugin = (uchar)ctx->bundle_status_recent;
      *charge_busy = 1;
    }
  }
}

static int
parse_url( fd_url_t *   url_,
           char const * url_str,
           ulong        url_str_len,
           ushort *     tcp_port,
           _Bool *      is_ssl,
           char const * context,
           int          fatal ) {

  /* Parse URL */

  int url_err[1];
  fd_url_t * url = fd_url_parse_cstr( url_, url_str, url_str_len, url_err );
  if( FD_UNLIKELY( !url ) ) {
    switch( *url_err ) {
    scheme_err:
    case FD_URL_ERR_SCHEME:
      if( fatal ) FD_LOG_ERR(( "Invalid %s `%.*s`: must start with `http://` or `https://`", context, (int)url_str_len, url_str ));
      FD_LOG_WARNING(( "Invalid %s `%.*s`: must start with `http://` or `https://`", context, (int)url_str_len, url_str ));
      return -1;
      break;
    case FD_URL_ERR_HOST_OVERSZ:
      if( fatal ) FD_LOG_ERR(( "Invalid %s `%.*s`: domain name is too long", context, (int)url_str_len, url_str ));
      FD_LOG_WARNING(( "Invalid %s `%.*s`: domain name is too long", context, (int)url_str_len, url_str ));
      return -1;
      break;
    default:
      if( fatal ) FD_LOG_ERR(( "Invalid %s `%.*s`", context, (int)url_str_len, url_str ));
      FD_LOG_WARNING(( "Invalid %s `%.*s`", context, (int)url_str_len, url_str ));
      return -1;
      break;
    }
  }

  /* FIXME the URL scheme path technically shouldn't contain slashes */
  if( url->scheme_len == 8UL && fd_memeq( url->scheme, "https://", 8UL ) ) {
    *is_ssl = 1;
  } else if( url->scheme_len == 7UL && fd_memeq( url->scheme, "http://", 7UL ) ) {
    *is_ssl = 0;
  } else {
    goto scheme_err;
  }

  /* Parse port number */

  *tcp_port = 443;
  if( url->port_len ) {
    if( FD_UNLIKELY( url->port_len > 5 ) ) {
    invalid_port:
      if( fatal ) FD_LOG_ERR(( "Invalid %s `%.*s`: invalid port number", context, (int)url_str_len, url_str ));
      FD_LOG_WARNING(( "Invalid %s `%.*s`: invalid port number", context, (int)url_str_len, url_str ));
      return -1;
    }

    char port_cstr[6];
    fd_cstr_fini( fd_cstr_append_text( fd_cstr_init( port_cstr ), url->port, url->port_len ) );
    ulong port_no = fd_cstr_to_ulong( port_cstr );
    if( FD_UNLIKELY( !port_no || port_no > USHORT_MAX ) ) goto invalid_port;

    *tcp_port = (ushort)port_no;
  }

  /* Resolve domain */

  if( FD_UNLIKELY( url->host_len > 255 ) ) {
    if( fatal ) FD_LOG_CRIT(( "Invalid url->host_len" )); /* unreachable */
    FD_LOG_WARNING(( "Invalid %s `%.*s`: domain name is too long", context, (int)url_str_len, url_str ));
    return -1;
  }
  return 0;
}

static int
fd_bam_tile_parse_runtime_endpoint( char const * url_cstr,
                                    ushort *     tcp_port,
                                    int *        is_ssl,
                                    char *       host_buf,
                                    ulong *      host_len,
                                    char *       err,
                                    ulong        err_sz ) {
  ulong url_len = strlen( url_cstr );
  if( FD_UNLIKELY( !url_len ) ) {
    if( err_sz ) fd_cstr_printf( err, err_sz, NULL, "BAM URL must be non-empty" );
    return -1;
  }

  fd_url_t url[1];
  ushort tmp_port = *tcp_port;
  _Bool tmp_ssl = (_Bool)(*is_ssl);
  if( FD_UNLIKELY( parse_url( url, url_cstr, url_len, &tmp_port, &tmp_ssl, "runtime BAM url", 0 ) ) ) {
    if( err_sz ) fd_cstr_printf( err, err_sz, NULL, "Invalid BAM URL `%s`", url_cstr );
    return -1;
  }
  if( FD_UNLIKELY( !url->host_len ) ) {
    if( err_sz ) fd_cstr_printf( err, err_sz, NULL, "BAM URL `%s` missing host", url_cstr );
    return -1;
  }
  if( FD_UNLIKELY( url->host_len >= FD_BAM_CTRL_URL_MAX ) ) {
    if( err_sz ) fd_cstr_printf( err, err_sz, NULL, "BAM host name too long" );
    return -1;
  }

  fd_memcpy( host_buf, url->host, url->host_len );
  host_buf[ url->host_len ] = '\0';
  *host_len = url->host_len;
  *tcp_port = tmp_port;
  *is_ssl   = (int)tmp_ssl;
  return 0;
}

static void
fd_bam_tile_format_url( fd_bam_tile_t const * ctx,
                        char *                dst,
                        ulong                 dst_sz ) {
  if( FD_UNLIKELY( !dst || !dst_sz ) ) return;
  if( FD_UNLIKELY( ctx->server_fqdn_len >= dst_sz ) ) {
    fd_memset( dst, 0, dst_sz );
    return;
  }
  int n = snprintf( dst, dst_sz, "%s://%.*s:%u",
                    ctx->is_ssl ? "https" : "http",
                    (int)ctx->server_fqdn_len,
                    ctx->server_fqdn,
                    (uint)ctx->server_tcp_port );
  if( FD_UNLIKELY( n < 0 ) ) dst[0] = '\0';
}

static void
fd_bam_tile_ctrl_update_current( fd_bam_tile_t * ctx ) {
  if( FD_UNLIKELY( !ctx->ctrl ) ) return;
  ctx->ctrl->current_enable = ctx->runtime_enabled;
  ctx->ctrl->enable         = ctx->runtime_enabled;
  char url_buf[ FD_BAM_CTRL_URL_MAX + FD_BAM_CTRL_URL_FORMAT_OVERHEAD ];
  fd_bam_tile_format_url( ctx, url_buf, sizeof(url_buf) );
  fd_bam_ctrl_copy_str( ctx->ctrl->current_url, FD_BAM_CTRL_URL_MAX, url_buf );
  fd_bam_ctrl_copy_str( ctx->ctrl->current_sni, FD_BAM_CTRL_SNI_MAX, ctx->server_sni );
}

static int
fd_bam_tile_apply_ctrl_request( fd_bam_tile_t * ctx,
                                uint            command,
                                uint            enable,
                                char const *    url,
                                char const *    sni,
                                char *          err,
                                ulong           err_sz ) {
  if( FD_UNLIKELY( !command ) ) {
    if( err_sz ) fd_cstr_printf( err, err_sz, NULL, "No BAM update requested" );
    return -1;
  }

  ushort new_port = ctx->server_tcp_port;
  int    new_ssl  = ctx->is_ssl;
  char   new_host[ 256 ];
  ulong  new_host_len = ctx->server_fqdn_len;
  fd_memcpy( new_host, ctx->server_fqdn, fd_ulong_min( sizeof(new_host)-1UL, new_host_len ) );
  new_host[ new_host_len ] = '\0';

  if( command & FD_BAM_CTRL_CMD_URL ) {
    if( FD_UNLIKELY( fd_bam_tile_parse_runtime_endpoint( url, &new_port, &new_ssl, new_host, &new_host_len, err, err_sz ) ) ) {
      return -1;
    }
#if !FD_HAS_OPENSSL
    if( FD_UNLIKELY( new_ssl ) ) {
      /* CLI can introduce TLS endpoints at runtime; without OpenSSL we must refuse early
         so the live tile stays on its previous HTTP configuration instead of flailing. */
      if( err_sz )
        fd_cstr_printf( err, err_sz, NULL,
                        "This build does not include OpenSSL. Re-run ./deps.sh and rebuild before using https URLs." );
      return -1;
    }
#endif
  }

  char new_sni[ FD_BAM_CTRL_SNI_MAX ];
  if( command & FD_BAM_CTRL_CMD_SNI ) {
    fd_bam_ctrl_copy_str( new_sni, sizeof(new_sni), sni );
    if( FD_UNLIKELY( !new_sni[0] ) )
      fd_bam_ctrl_copy_str( new_sni, sizeof(new_sni), new_host );
  } else if( command & FD_BAM_CTRL_CMD_URL ) {
    fd_bam_ctrl_copy_str( new_sni, sizeof(new_sni), new_host );
  } else {
    fd_bam_ctrl_copy_str( new_sni, sizeof(new_sni), ctx->server_sni );
  }

  uchar new_enable = ctx->runtime_enabled;
  if( command & FD_BAM_CTRL_CMD_ENABLE )
    new_enable = !!enable;

  int need_reset = 0;
  int clear_backoff_after_reset = 0; /* Track whether we should nuke backoff state after reset so re-enable connects immediately. */
  if( command & FD_BAM_CTRL_CMD_URL ) {
    fd_memset( ctx->server_fqdn, 0, sizeof(ctx->server_fqdn) );
    fd_memcpy( ctx->server_fqdn, new_host, new_host_len );
    ctx->server_fqdn_len = (ushort)fd_ulong_min( new_host_len, (ulong)USHORT_MAX );
    ctx->server_tcp_port = new_port;
    ctx->is_ssl          = !!new_ssl;
    need_reset = 1;
  }

  if( command & (FD_BAM_CTRL_CMD_URL | FD_BAM_CTRL_CMD_SNI) ) {
    fd_memset( ctx->server_sni, 0, sizeof(ctx->server_sni) );
    ulong sni_len = strlen( new_sni );
    fd_memcpy( ctx->server_sni, new_sni, sni_len );
    ctx->server_sni_len = (ushort)fd_ulong_min( sni_len, (ulong)USHORT_MAX );
    fd_grpc_client_set_authority( ctx->grpc_client, ctx->server_sni, ctx->server_sni_len, ctx->server_tcp_port );
    need_reset = 1;
  }

  if( (command & FD_BAM_CTRL_CMD_ENABLE) && (new_enable != ctx->runtime_enabled) ) {
    ctx->runtime_enabled = new_enable;
    need_reset = 1;
    if( new_enable ) clear_backoff_after_reset = 1;
  }

  if( need_reset ) {
    fd_bam_client_reset( ctx );
    if( FD_UNLIKELY( clear_backoff_after_reset ) ) {
      /* The reset routine re-establishes the randomized pause. Clearing it post-reset lets
         admin-triggered re-enables take effect immediately instead of waiting out the old backoff. */
      ctx->backoff_until = 0L;
      ctx->backoff_reset = 0L;
      ctx->backoff_iter  = 0U;
    }
    if( FD_UNLIKELY( !ctx->runtime_enabled && ctx->bam_status_fseq ) )
      /* Force the shared status latch low immediately when BAM is
         disabled so downstream tiles resume QUIC/bundle input without
         waiting for TCP timeouts. */
      fd_fseq_update( ctx->bam_status_fseq, 0UL );
  }

  fd_bam_tile_ctrl_update_current( ctx );
  if( err_sz ) err[0] = '\0';
  return 0;
}

static void
fd_bam_tile_handle_ctrl( fd_bam_tile_t * ctx ) {
  if( FD_UNLIKELY( !ctx->ctrl ) ) return;

  /* Wait until we receive a new request. */
  for( ;; ) {
    long state = FD_VOLATILE_CONST( ctx->ctrl->state );
    if( FD_LIKELY( state != FD_BAM_CTRL_STATE_REQUEST ) ) return;
    if( FD_ATOMIC_CAS( &ctx->ctrl->state, FD_BAM_CTRL_STATE_REQUEST, FD_BAM_CTRL_STATE_APPLYING ) == FD_BAM_CTRL_STATE_REQUEST )
      break;
  }

  uint command = ctx->ctrl->command;
  uint enable  = ctx->ctrl->enable;
  char url[ FD_BAM_CTRL_URL_MAX ];
  char sni[ FD_BAM_CTRL_SNI_MAX ];
  fd_memcpy( url, ctx->ctrl->url, sizeof(url) );
  fd_memcpy( sni, ctx->ctrl->sni, sizeof(sni) );

  char err[ FD_BAM_CTRL_ERR_MAX ];
  err[0] = '\0';
  int rc = fd_bam_tile_apply_ctrl_request( ctx, command, enable, url, sni, err, sizeof(err) );
  if( FD_UNLIKELY( rc ) ) {
    fd_bam_ctrl_copy_str( ctx->ctrl->error, FD_BAM_CTRL_ERR_MAX, err );
    FD_COMPILER_MFENCE();
    FD_VOLATILE( ctx->ctrl->state ) = FD_BAM_CTRL_STATE_ERROR;
    return;
  }

  fd_bam_ctrl_copy_str( ctx->ctrl->error, FD_BAM_CTRL_ERR_MAX, "" );
  FD_COMPILER_MFENCE();
  FD_VOLATILE( ctx->ctrl->state ) = FD_BAM_CTRL_STATE_SUCCESS;
}

static void
fd_bam_tile_parse_endpoint( fd_bam_tile_t *     ctx,
                               fd_topo_tile_t const * tile ) {
  fd_url_t url[1];
  _Bool is_ssl = 0;
  parse_url(
      url,
      tile->bam.url, tile->bam.url_len,
      &ctx->server_tcp_port,
      &is_ssl,
      "[tiles.bam.url]",
      1
  );
  if( FD_UNLIKELY( url->host_len > 255 ) ) {
    FD_LOG_CRIT(( "Invalid url->host_len" )); /* unreachable */
  }
  fd_cstr_fini( fd_cstr_append_text( fd_cstr_init( ctx->server_fqdn ), url->host, url->host_len ) );
  ctx->server_fqdn_len = (ushort)fd_ulong_min( url->host_len, (ulong)USHORT_MAX );

  if( FD_UNLIKELY( tile->bam.sni_len ) ) {
    fd_cstr_fini( fd_cstr_append_text( fd_cstr_init( ctx->server_sni ), tile->bam.sni, tile->bam.sni_len ) );
    ctx->server_sni_len = (ushort)fd_ulong_min( tile->bam.sni_len, (ulong)USHORT_MAX );
  } else {
    fd_cstr_fini( fd_cstr_append_text( fd_cstr_init( ctx->server_sni ), url->host, url->host_len ) );
    ctx->server_sni_len = (ushort)fd_ulong_min( url->host_len, (ulong)USHORT_MAX );
  }

  ctx->is_ssl = !!is_ssl;
#if !FD_HAS_OPENSSL
  if( FD_UNLIKELY( is_ssl ) ) {
    FD_LOG_ERR(( "This build does not include OpenSSL. To install OpenSSL, re-run ./deps.sh and do a clean re build." ));
  }
#endif
  fd_bam_tile_ctrl_update_current( ctx );
}

#if FD_HAS_OPENSSL

/* OpenSSL allows us to specify custom memory allocation functions,
   which we want to point to an fd_alloc_t, but it does not let us use a
   context object.  Instead we stash it in this thread local, which is
   OK because the parent workspace exists for the duration of the SSL
   context, and the process only has one thread.

   Currently fd_alloc doesn't support realloc, so it's implemented on
   top of malloc and free, and then also it doesn't support getting the
   size of an allocation from the pointer, which we need for realloc, so
   we pad each alloc by 8 bytes and stuff the size into the first 8
   bytes. */
static FD_TL fd_alloc_t * fd_quic_ssl_mem_function_ctx = NULL;

static void *
crypto_malloc( ulong        num,
               char const * file,
               int          line ) {
  (void)file; (void)line;
  void * result = fd_alloc_malloc( fd_quic_ssl_mem_function_ctx, 16UL, num + 8UL );
  if( FD_UNLIKELY( !result ) ) {
    FD_MCNT_INC( BAM, ERRORS_SSL_ALLOC, 1UL );
    return NULL;
  }
  *(ulong *)result = num;
  return (uchar *)result + 8UL;
}

static void
crypto_free( void *       addr,
             char const * file,
             int          line ) {
  (void)file;
  (void)line;

  if( FD_UNLIKELY( !addr ) ) return;
  fd_alloc_free( fd_quic_ssl_mem_function_ctx, (uchar *)addr - 8UL );
}

static void *
crypto_realloc( void *       addr,
                ulong        num,
                char const * file,
                int          line ) {
  if( FD_UNLIKELY( !addr ) ) return crypto_malloc( num, file, line );
  if( FD_UNLIKELY( !num ) ) {
    crypto_free( addr, file, line );
    return NULL;
  }

  void * new = fd_alloc_malloc( fd_quic_ssl_mem_function_ctx, 16UL, num + 8UL );
  if( FD_UNLIKELY( !new ) ) return NULL;

  ulong old_num = *(ulong *)( (uchar *)addr - 8UL );
  fd_memcpy( (uchar*)new + 8, (uchar*)addr, fd_ulong_min( old_num, num ) );
  fd_alloc_free( fd_quic_ssl_mem_function_ctx, (uchar *)addr - 8UL );
  *(ulong *)new = num;
  return (uchar*)new + 8UL;
}

static void
fd_ossl_keylog_callback( SSL const *  ssl,
                         char const * line ) {
  SSL_CTX * ssl_ctx = SSL_get_SSL_CTX( ssl );
  fd_bam_tile_t * ctx = SSL_CTX_get_ex_data( ssl_ctx, 0 );
  ulong line_len = strlen( line );
  struct iovec iovs[2] = {
    { .iov_base=(void *)line, .iov_len=line_len },
    { .iov_base=(void *)"\n", .iov_len=1UL }
  };
  if( FD_UNLIKELY( writev( ctx->keylog_fd, iovs, 2 ) != (long)line_len+1 ) ) {
    FD_LOG_WARNING(( "write(keylog) failed (%i-%s)", errno, fd_io_strerror( errno ) ));
  }
}

static void
fd_bam_tile_load_certs( SSL_CTX * ssl_ctx ) {
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
  while( (entry = readdir( dir )) ) {
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

  if( FD_UNLIKELY( errno && errno != ENOENT ) ) {
    FD_LOG_ERR(( "readdir(%s) failed (%i-%s)", default_dir, errno, fd_io_strerror( errno ) ));
  }

  STACK_OF(X509) * cert_list = X509_STORE_get1_all_certs( ca_certs );
  FD_LOG_INFO(( "Loaded %d CA certs from %s into OpenSSL", sk_X509_num( cert_list ), default_dir ));
  if( fd_log_level_logfile() == 0 ) {
    for( int i=0; i < sk_X509_num( cert_list ); i++ ) {
      X509 * cert = sk_X509_value( cert_list, i );
      FD_LOG_DEBUG(( "Loaded CA cert \"%s\"", X509_NAME_oneline( X509_get_subject_name( cert ), NULL, 0 ) ));
    }
  }
  sk_X509_pop_free( cert_list, X509_free );

  SSL_CTX_set_cert_store( ssl_ctx, ca_certs );

  if( FD_UNLIKELY( 0 != closedir( dir ) ) ) {
    FD_LOG_ERR(( "closedir(%s) failed (%i-%s)", default_dir, errno, fd_io_strerror( errno ) ));
  }
}

static void
fd_bam_tile_init_openssl( fd_bam_tile_t * ctx,
                             void *             alloc_mem,
                             int                tls_cert_verify ) {
  fd_alloc_t * alloc = fd_alloc_join( fd_alloc_new( alloc_mem, 1UL ), 1UL );
  if( FD_UNLIKELY( !alloc ) ) {
    FD_LOG_ERR(( "fd_alloc_new failed" ));
  }
  ctx->ssl_alloc               = alloc;
  fd_quic_ssl_mem_function_ctx = alloc;

  if( FD_UNLIKELY( !CRYPTO_set_mem_functions( crypto_malloc, crypto_realloc, crypto_free ) ) ) {
    FD_LOG_ERR(( "CRYPTO_set_mem_functions failed" ));
  }

  OPENSSL_init_ssl(
      OPENSSL_INIT_LOAD_SSL_STRINGS |
      OPENSSL_INIT_LOAD_CRYPTO_STRINGS |
      OPENSSL_INIT_NO_LOAD_CONFIG,
      NULL
  );

  SSL_CTX * ssl_ctx = SSL_CTX_new( TLS_client_method() );
  if( FD_UNLIKELY( !ssl_ctx ) ) {
    FD_LOG_ERR(( "SSL_CTX_new failed" ));
  }

  if( FD_UNLIKELY( !SSL_CTX_set_ex_data( ssl_ctx, 0, ctx ) ) ) {
    FD_LOG_ERR(( "SSL_CTX_set_ex_data failed" ));
  }

  if( FD_UNLIKELY( !SSL_CTX_set_mode( ssl_ctx, SSL_MODE_ENABLE_PARTIAL_WRITE|SSL_MODE_AUTO_RETRY ) ) ) {
    FD_LOG_ERR(( "SSL_CTX_set_mode failed" ));
  }

  if( FD_UNLIKELY( !SSL_CTX_set_min_proto_version( ssl_ctx, TLS1_3_VERSION ) ) ) {
    FD_LOG_ERR(( "SSL_CTX_set_min_proto_version(ssl_ctx,TLS1_3_VERSION) failed" ));
  }

  if( FD_UNLIKELY( 0 != SSL_CTX_set_alpn_protos( ssl_ctx, (const unsigned char *)"\x02h2", 3 ) ) ) {
    FD_LOG_ERR(( "SSL_CTX_set_alpn_protos failed" ));
  }

  if( tls_cert_verify ) {
    fd_bam_tile_load_certs( ssl_ctx );
    SSL_CTX_set_verify( ssl_ctx, SSL_VERIFY_PEER, NULL );
  }

  if( FD_LIKELY( ctx->keylog_fd >= 0 ) ) {
    SSL_CTX_set_keylog_callback( ssl_ctx, fd_ossl_keylog_callback );
  }

  ctx->ssl_ctx = ssl_ctx;
}

#endif /* FD_HAS_OPENSSL */

static void
privileged_init( fd_topo_t *      topo,
                 fd_topo_tile_t * tile ) {
  void * scratch = fd_topo_obj_laddr( topo, tile->tile_obj_id );

  FD_SCRATCH_ALLOC_INIT( l, scratch );
  fd_bam_tile_t * ctx         = FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_bam_tile_t), sizeof(fd_bam_tile_t)                        );
  void *             grpc_mem    = FD_SCRATCH_ALLOC_APPEND( l, fd_grpc_client_align(),    fd_grpc_client_footprint( tile->bam.buf_sz ) );
  void *             alloc_mem   = FD_SCRATCH_ALLOC_APPEND( l, fd_alloc_align(),          fd_alloc_footprint()                            );
  ulong              scratch_end = FD_SCRATCH_ALLOC_FINI( l, scratch_align() );
  (void)alloc_mem; /* potentially unused */

  if( FD_UNLIKELY( (ulong)ctx != (ulong)scratch ) ) {
    FD_LOG_CRIT(( "Invalid bundle tile scratch alignment" )); /* unreachable */
  }
  if( FD_UNLIKELY( scratch_end - (ulong)scratch > scratch_footprint( tile ) ) ) {
    FD_LOG_CRIT(( "Bundle tile scratch overflow" )); /* unreachable */
  }

  memset( ctx, 0, sizeof(fd_bam_tile_t) );
  ctx->grpc_client_mem = grpc_mem;
  ctx->grpc_buf_max    = tile->bam.buf_sz;
  ctx->tcp_sock        = -1;
  ctx->bank_bam_in_idx = ULONG_MAX;
  ctx->pack_leader_in_idx = ULONG_MAX;
  ctx->bundle_max_schedule_slot = FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT;

  uchar const * public_key = fd_keyload_load( tile->bam.identity_key_path, 1 /* public key only */ );
  fd_memcpy( ctx->bam_url_pubkey, public_key, 32UL );
  fd_base58_encode_32( public_key, NULL, ctx->bam_validator_pubkey );

  ctx->keylog_fd = -1;

# if FD_HAS_OPENSSL

  if( FD_UNLIKELY( tile->bam.key_log_path[0] ) ) {
    ctx->keylog_fd = open( tile->bam.key_log_path, O_WRONLY|O_APPEND|O_CREAT, 0644 );
    if( FD_UNLIKELY( ctx->keylog_fd < 0 ) ) {
      FD_LOG_ERR(( "open(%s) failed (%i-%s)", tile->bam.key_log_path, errno, fd_io_strerror( errno ) ));
    }
  }

  /* OpenSSL goes and tries to read files and allocate memory and
     other dumb things on a thread local basis, so we need a special
     initializer to do it before seccomp happens in the process. */
  fd_bam_tile_init_openssl( ctx, alloc_mem, tile->bam.tls_cert_verify );

# endif /* FD_HAS_OPENSSL */

  /* Init resolver */
  if( FD_UNLIKELY( !fd_netdb_open_fds( ctx->netdb_fds ) ) ) {
    FD_LOG_ERR(( "fd_netdb_open_fds failed" ));
  }

  /* Random seed for header hashmap */
  if( FD_UNLIKELY( !fd_rng_secure( &ctx->map_seed, sizeof(ulong) ) ) ) {
    FD_LOG_CRIT(( "fd_rng_secure failed" ));
  }

  /* Random seed for timing RNG */
  uint rng_seed;
  if( FD_UNLIKELY( !fd_rng_secure( &rng_seed, sizeof(uint) ) ) ) {
    FD_LOG_CRIT(( "fd_rng_secure failed" ));
  }
  if( FD_UNLIKELY( !fd_rng_join( fd_rng_new( &ctx->rng, rng_seed, 0UL ) ) ) ) {
    FD_LOG_CRIT(( "fd_rng_join failed" )); /* unreachable */
  }

  ctx->runtime_enabled = 1;
  ctx->fee_cfg_version = 0UL;
  ctx->validator_commission_bps = 0U;
  ctx->prio_fee_recipient_set   = 0U;
  fd_memset( ctx->prio_fee_recipient, 0, sizeof( ctx->prio_fee_recipient ) );

  ulong bam_fee_cfg_obj_id = fd_pod_query_ulong( topo->props, "bam_fee_cfg", ULONG_MAX );
  if( FD_LIKELY( bam_fee_cfg_obj_id != ULONG_MAX ) ) {
    ctx->fee_cfg = fd_topo_obj_laddr( topo, bam_fee_cfg_obj_id );
    fd_memset( ctx->fee_cfg, 0, sizeof(fd_bam_fee_cfg_t) );
  } else {
    ctx->fee_cfg = NULL;
  }

  ulong bam_ctrl_obj_id = fd_pod_query_ulong( topo->props, "bam_ctrl", ULONG_MAX );
  if( FD_LIKELY( bam_ctrl_obj_id != ULONG_MAX ) ) {
    ctx->ctrl = fd_topo_obj_laddr( topo, bam_ctrl_obj_id );
    fd_memset( ctx->ctrl, 0, sizeof(fd_bam_ctrl_t) );
    ctx->ctrl->state          = FD_BAM_CTRL_STATE_IDLE;
    ctx->ctrl->enable         = 1;
    ctx->ctrl->current_enable = 1;
    fd_bam_ctrl_copy_str( ctx->ctrl->current_url, FD_BAM_CTRL_URL_MAX, tile->bam.url );
    fd_bam_ctrl_copy_str( ctx->ctrl->current_sni, FD_BAM_CTRL_SNI_MAX, tile->bam.sni );
  } else {
    ctx->ctrl = NULL;
  }
}

static fd_bam_out_ctx_t
bam_out_link( fd_topo_t const *      topo,
                 fd_topo_link_t const * link,
                 ulong                  out_link_idx ) {
  fd_bam_out_ctx_t out = {0};
  out.idx    = out_link_idx;
  out.mem    = topo->workspaces[ topo->objs[ link->dcache_obj_id ].wksp_id ].wksp;
  out.chunk0 = fd_dcache_compact_chunk0( out.mem, link->dcache );
  out.wmark  = fd_dcache_compact_wmark ( out.mem, link->dcache, link->mtu );
  out.chunk  = out.chunk0;
  return out;
}

static void
unprivileged_init( fd_topo_t *      topo,
                   fd_topo_tile_t * tile ) {
  fd_bam_tile_t * ctx = fd_topo_obj_laddr( topo, tile->tile_obj_id );
  if( FD_UNLIKELY( tile->kind_id != 0 ) ) {
    FD_LOG_ERR(( "There can only be one bam tile" ));
  }

  ulong sign_in_idx = fd_topo_find_tile_in_link( topo, tile, "sign_bam", tile->kind_id );
  if( FD_UNLIKELY( sign_in_idx == ULONG_MAX ) ) FD_LOG_ERR(( "Missing sign_bam link" ));
  fd_topo_link_t const * sign_in  = &topo->links[ tile->in_link_id[ sign_in_idx ] ];

  ulong sign_out_idx = fd_topo_find_tile_out_link( topo, tile, "bam_sign", tile->kind_id );
  if( FD_UNLIKELY( sign_out_idx == ULONG_MAX ) ) FD_LOG_ERR(( "Missing bam_sign link" ));
  fd_topo_link_t const * sign_out = &topo->links[ tile->out_link_id[ sign_out_idx ] ];

  if( FD_UNLIKELY( !fd_keyguard_client_join( fd_keyguard_client_new(
      ctx->keyguard_client,
      sign_out->mcache,
      sign_out->dcache,
      sign_in->mcache,
      sign_in->dcache
  ) ) ) ) {
    FD_LOG_ERR(( "fd_keyguard_client_join failed" )); /* unreachable */
  }

  ctx->keyswitch = fd_keyswitch_join( fd_topo_obj_laddr( topo, tile->keyswitch_obj_id ) );
  FD_TEST( ctx->keyswitch );

  ulong bank_in_idx = fd_topo_find_tile_in_link( topo, tile, "bank_bam", tile->kind_id );
  if( bank_in_idx != ULONG_MAX ) {
    ctx->bank_bam_in_idx = bank_in_idx;
    fd_topo_link_t const * bank_in = &topo->links[ tile->in_link_id[ bank_in_idx ] ];
    ctx->bank_in.mem    = topo->workspaces[ topo->objs[ bank_in->dcache_obj_id ].wksp_id ].wksp;
    ctx->bank_in.chunk0 = fd_dcache_compact_chunk0( ctx->bank_in.mem, bank_in->dcache );
    ctx->bank_in.wmark  = fd_dcache_compact_wmark ( ctx->bank_in.mem, bank_in->dcache, bank_in->mtu );
  }

  ulong leader_in_idx = fd_topo_find_tile_in_link( topo, tile, "pack_bam", tile->kind_id );
  if( leader_in_idx != ULONG_MAX ) {
    ctx->pack_leader_in_idx = leader_in_idx;
    fd_topo_link_t const * leader_in = &topo->links[ tile->in_link_id[ leader_in_idx ] ];
    ctx->leader_in.mem    = topo->workspaces[ topo->objs[ leader_in->dcache_obj_id ].wksp_id ].wksp;
    ctx->leader_in.chunk0 = fd_dcache_compact_chunk0( ctx->leader_in.mem, leader_in->dcache );
    ctx->leader_in.wmark  = fd_dcache_compact_wmark ( ctx->leader_in.mem, leader_in->dcache, leader_in->mtu );
  } else {
    ctx->pack_leader_in_idx = ULONG_MAX;
  }

  ulong verify_out_idx = fd_topo_find_tile_out_link( topo, tile, "bam_verif", tile->kind_id );
  if( FD_UNLIKELY( verify_out_idx == ULONG_MAX ) ) FD_LOG_ERR(( "Missing bam_verif link" ));
  ctx->verify_out = bam_out_link( topo, &topo->links[ tile->out_link_id[ verify_out_idx ] ], verify_out_idx );

  ulong plugin_out_idx = fd_topo_find_tile_out_link( topo, tile, "bam_plugi", tile->kind_id );
  if( plugin_out_idx != ULONG_MAX ) {
    ctx->plugin_out = bam_out_link( topo, &topo->links[ tile->out_link_id[ plugin_out_idx ] ], plugin_out_idx );
  } else {
    ctx->plugin_out = (fd_bam_out_ctx_t){ .idx    = ULONG_MAX };
  }

  ulong gossip_out_idx = fd_topo_find_tile_out_link( topo, tile, "bam_gossip", tile->kind_id );
  if( gossip_out_idx != ULONG_MAX ) {
    ctx->gossip_out = bam_out_link( topo, &topo->links[ tile->out_link_id[ gossip_out_idx ] ], gossip_out_idx );
  } else {
    ctx->gossip_out = (fd_bam_out_ctx_t){ .idx    = ULONG_MAX };
  }

  /* Set socket receive buffer size */
  ulong so_rcvbuf = tile->bam.buf_sz;
  if( FD_UNLIKELY( so_rcvbuf < 2048UL  ) ) FD_LOG_ERR(( "Invalid [development.bundle.buffer_size_kib]: too small" ));
  if( FD_UNLIKELY( so_rcvbuf > INT_MAX ) ) FD_LOG_ERR(( "Invalid [development.bundle.buffer_size_kib]: too large" ));
  ctx->so_rcvbuf = (int)so_rcvbuf;

  /* Set idle ping timer */
  ctx->keepalive_interval = (long)tile->bam.keepalive_interval_nanos;

  ctx->bundle_status_plugin = 127;
  ctx->bundle_status_recent = FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_DISCONNECTED;
  ctx->last_bundle_status_log_nanos = fd_log_wallclock();

  ctx->bam_tpu_addr.l       = 0UL;
  ctx->bam_tpu_quic_addr.l  = 0UL;

  ulong bam_status_obj_id = fd_pod_query_ulong( topo->props, "bam_status", ULONG_MAX );
  if( FD_LIKELY( bam_status_obj_id != ULONG_MAX ) ) {
    ctx->bam_status_fseq = fd_fseq_join( fd_topo_obj_laddr( topo, bam_status_obj_id ) );
    if( FD_UNLIKELY( !ctx->bam_status_fseq ) ) FD_LOG_ERR(( "bam tile missing bam_status fseq" ));
    /* Start disconnected so a late BAM connect transitions the flag to 1
       and wakes up peers waiting for the override. */
    fd_fseq_update( ctx->bam_status_fseq, 0UL );
  } else {
    ctx->bam_status_fseq = NULL;
  }

  fd_bam_tile_parse_endpoint( ctx, tile );

  ctx->grpc_client = fd_grpc_client_new( ctx->grpc_client_mem, &fd_bam_client_grpc_callbacks, ctx->grpc_metrics, ctx, ctx->grpc_buf_max, ctx->map_seed );
  if( FD_UNLIKELY( !ctx->grpc_client ) ) {
    FD_LOG_CRIT(( "fd_grpc_client_new failed" )); /* unreachable */
  }
  fd_grpc_client_set_version( ctx->grpc_client, fdctl_version_string, strlen( fdctl_version_string ) );
  fd_grpc_client_set_authority( ctx->grpc_client, ctx->server_sni, ctx->server_sni_len, ctx->server_tcp_port );

  fd_histf_new( ctx->metrics.msg_rx_delay,
      FD_MHIST_MIN( BAM, MESSAGE_RX_DELAY_NANOS ),
      FD_MHIST_MAX( BAM, MESSAGE_RX_DELAY_NANOS ) );
}

static ulong
populate_allowed_seccomp( fd_topo_t const *      topo,
                          fd_topo_tile_t const * tile,
                          ulong                  out_cnt,
                          struct sock_filter *   out ) {
  fd_bam_tile_t * ctx = fd_topo_obj_laddr( topo, tile->tile_obj_id );

  populate_sock_filter_policy_fd_bundle_tile(
      out_cnt, out,
      (uint)fd_log_private_logfile_fd(),
      (uint)ctx->keylog_fd,
      (uint)ctx->netdb_fds->etc_hosts,
      (uint)ctx->netdb_fds->etc_resolv_conf
  );
  return sock_filter_policy_fd_bundle_tile_instr_cnt;
}

static ulong
populate_allowed_fds( fd_topo_t const *      topo,
                      fd_topo_tile_t const * tile,
                      ulong                  out_fds_cnt,
                      int *                  out_fds ) {
  fd_bam_tile_t * ctx = fd_topo_obj_laddr( topo, tile->tile_obj_id );

  if( FD_UNLIKELY( out_fds_cnt < 5UL ) ) FD_LOG_ERR(( "out_fds_cnt %lu", out_fds_cnt ));

  ulong out_cnt = 0UL;
  out_fds[ out_cnt++ ] = 2; /* stderr */
  if( FD_LIKELY( -1 != fd_log_private_logfile_fd() ) )
    out_fds[ out_cnt++ ] = fd_log_private_logfile_fd(); /* logfile */
  if( FD_LIKELY( ctx->netdb_fds->etc_hosts >= 0 ) )
    out_fds[ out_cnt++ ] = ctx->netdb_fds->etc_hosts;
  out_fds[ out_cnt++ ] = ctx->netdb_fds->etc_resolv_conf;
  if( FD_UNLIKELY( ctx->keylog_fd >= 0 ) )
    out_fds[ out_cnt++ ] = ctx->keylog_fd;
  return out_cnt;
}

#define STEM_BURST (5UL)
#define STEM_LAZY ((long)10e6)

#define STEM_CALLBACK_CONTEXT_TYPE  fd_bam_tile_t
#define STEM_CALLBACK_CONTEXT_ALIGN alignof(fd_bam_tile_t)

#define STEM_CALLBACK_DURING_HOUSEKEEPING fd_bam_tile_housekeeping
#define STEM_CALLBACK_METRICS_WRITE       metrics_write
#define STEM_CALLBACK_AFTER_CREDIT        after_credit
#define STEM_CALLBACK_DURING_FRAG         bam_during_frag

#include "../stem/fd_stem.c"

fd_topo_run_tile_t fd_tile_bam = {
  .name                     = "bam",
  .populate_allowed_seccomp = populate_allowed_seccomp,
  .populate_allowed_fds     = populate_allowed_fds,
  .scratch_align            = scratch_align,
  .scratch_footprint        = scratch_footprint,
  .loose_footprint          = loose_footprint,
  .privileged_init          = privileged_init,
  .unprivileged_init        = unprivileged_init,
  .run                      = stem_run,
  .rlimit_file_cnt          = 64,
  .keep_host_networking     = 1,
  .allow_connect            = 1
};
