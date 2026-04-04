#define _GNU_SOURCE
#include "fd_bam_tile_private.h"
#include "../metrics/fd_metrics.h"
#include "../topo/fd_topo.h"
#include "../keyguard/fd_keyload.h"
#include "../plugin/fd_plugin.h"
#include "../../waltz/http/fd_url.h"
#include "../../waltz/openssl/fd_openssl.h"
#include "../../tango/fseq/fd_fseq.h"
#include "../../util/pod/fd_pod_format.h"
#include "../../util/net/fd_ip4.h"

#include <errno.h>
#include <ctype.h> /* isspace */
#include <dirent.h> /* opendir */
#include <stdio.h> /* snprintf */
#include <fcntl.h> /* F_SETFL */
#include <unistd.h> /* close */
#include <sys/mman.h> /* PROT_READ (seccomp) */
#include <sys/uio.h> /* writev */
#include <netinet/in.h> /* AF_INET */
#include <netinet/tcp.h> /* TCP_FASTOPEN_CONNECT (seccomp) */
#include "../../waltz/resolv/fd_netdb.h"

#include "../bundle/generated/fd_bundle_tile_seccomp.h"

/* Provided by fdctl/firedancer version.c */
extern char const fdctl_version_string[];

FD_FN_CONST static ulong
scratch_align( void ) {
  return fd_ulong_max( fd_ulong_max( alignof(fd_bam_tile_t), fd_grpc_client_align() ), fd_alloc_align() );
}

FD_FN_CONST static ulong
scratch_footprint( fd_topo_tile_t const * tile ) {
  (void)tile;
  ulong l = FD_LAYOUT_INIT;
  l = FD_LAYOUT_APPEND( l, alignof(fd_bam_tile_t), sizeof(fd_bam_tile_t)                        );
  l = FD_LAYOUT_APPEND( l, fd_grpc_client_align(),    fd_grpc_client_footprint( tile->bam.buf_sz ) );
  l = FD_LAYOUT_APPEND( l, fd_alloc_align(),          fd_alloc_footprint()                            );
  return FD_LAYOUT_FINI( l, scratch_align() );
}

FD_FN_CONST static inline ulong
loose_footprint( fd_topo_tile_t const * tile ) {
  (void)tile;
  /* Leftover space for OpenSSL allocations */
  return 1UL<<26; /* 64 MiB */
}

void
fd_bam_try_emit_slot_ingress_timing_summary( fd_bam_tile_t *                ctx,
                                             fd_bam_slot_ingress_timing_t * entry,
                                             ulong                          current_leader_slot ) {
  if( FD_UNLIKELY( !( ctx->dump_bam_txns || ctx->dump_bam_first_slot_txn ) || entry->summary_emitted ) ) return;
  long first_rx_minus_slot_end_ns = 0L;
  if( FD_UNLIKELY( entry->first_rx_ts_ns && entry->slot_end_ns ) ) first_rx_minus_slot_end_ns = entry->first_rx_ts_ns - entry->slot_end_ns;

  FD_LOG_NOTICE(( "BAM slot ingress summary: slot=%lu first_rx_ns=%ld first_rx_minus_slot_end_ns=%ld first_rx_after_slot_end=%u txns_before_slot_end=%lu txns_after_slot_end=%lu txns_unknown_slot_end=%lu current_leader_slot=%lu",
                  entry->slot,
                  entry->first_rx_ts_ns,
                  first_rx_minus_slot_end_ns,
                  (uint)entry->first_rx_after_slot_end,
                  entry->txn_before_slot_end,
                  entry->txn_after_slot_end,
                  entry->txn_unknown_slot_end,
                  current_leader_slot ));
  entry->summary_emitted = 1U;
}

static inline void
metrics_write( fd_bam_tile_t * ctx ) {
  fd_plugin_bam_update_status_t status = fd_bam_client_status( ctx );
  _Bool healthy = status == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY;
  ulong current_slot_first_ingress_recorded       = 0UL;
  ulong current_slot_first_ingress_slot_end_known = 0UL;
  ulong current_slot_first_ingress_distance_nanos = 0UL;
  ulong current_slot_first_ingress_after_end      = 0UL;
  ulong leader_slot = ctx->bam_leader_state.slot;
  fd_bam_slot_ingress_timing_t const * entry =
      leader_slot==ULONG_MAX ? NULL : fd_bam_slot_ingress_timing_query_const( ctx, leader_slot );
  if( FD_LIKELY( entry && entry->first_rx_ts_ns ) ) {
    current_slot_first_ingress_recorded = 1UL;
    long slot_end_ns = entry->slot_end_ns ? entry->slot_end_ns : ctx->bam_leader_state.slot_end_ns;
    if( FD_LIKELY( slot_end_ns ) ) {
      long delta = entry->first_rx_ts_ns - slot_end_ns;
      current_slot_first_ingress_slot_end_known = 1UL;
      current_slot_first_ingress_distance_nanos = fd_long_abs( delta );
      current_slot_first_ingress_after_end      = (ulong)( delta>0L );
    }
  }
  FD_MCNT_SET( BAM, TRANSACTION_PUBLISHED,   ctx->metrics.transaction_published_cnt          );
  FD_MCNT_SET( BAM, ATOMIC_BATCH_PUBLISHED,  ctx->metrics.atomic_batch_published_cnt );
  FD_MCNT_SET( BAM, FEEDBACK_RESULTS_DROPPED, ctx->metrics.feedback_results_dropped_cnt   );
  FD_MCNT_SET( BAM, INGRESS_PACKET_OVERSIZE, ctx->metrics.ingress_packet_oversize_cnt );
  FD_MCNT_SET( BAM, KEEPALIVE_ACKS,          ctx->metrics.keepalive_acks_cnt           );
  FD_MCNT_SET( BAM, BUILDER_HEARTBEATS_DECODED,    ctx->metrics.builder_heartbeats_decoded_cnt       );
  FD_MCNT_SET( BAM, HEALTHY_CONNECTS,              ctx->metrics.healthy_connects_cnt      );
  FD_MCNT_SET( BAM, HEALTHY_DISCONNECTS,           ctx->metrics.healthy_disconnects_cnt   );
  FD_MCNT_ENUM_COPY( BAM, FAILURE,          ctx->metrics.failure_cnt               );
  FD_MCNT_SET( BAM, INGRESS_MULTI_MESSAGE_RECEIVED,         ctx->metrics.ingress_multi_message_received_cnt );
  FD_MCNT_SET( BAM, INGRESS_BATCH_COMMIT_ATTEMPT,           ctx->metrics.ingress_batch_commit_attempt_cnt );
  FD_MCNT_SET( BAM, INGRESS_BATCH_PUBLISHED,                ctx->metrics.ingress_batch_published_cnt );
  FD_MCNT_ENUM_COPY( BAM, INGRESS_BATCH_REJECTED, ctx->metrics.ingress_batch_rejected_cnt );
  FD_MCNT_ENUM_COPY( BAM, INGRESS_MESSAGE_REJECTED, ctx->metrics.ingress_message_rejected_cnt );
  FD_MCNT_ENUM_COPY( BAM, OUTBOUND_ENQUEUE_OUTCOME,  ctx->metrics.outbound_enqueue_outcome_cnt );
  FD_MCNT_ENUM_COPY( BAM, STREAM_TRANSITION,      ctx->metrics.stream_transition_cnt     );
  FD_MCNT_ENUM_COPY( BAM, LEADER_PENDING_DROPPED, ctx->metrics.leader_pending_dropped_cnt );
  FD_MCNT_SET( BAM, LEADER_PENDING_REPLACED,      ctx->metrics.leader_pending_replaced_cnt );
  FD_MCNT_ENUM_COPY( BAM, SLOT_INGRESS_RESULT,       ctx->metrics.slot_ingress_result_cnt );
  FD_MCNT_ENUM_COPY( BAM, SLOT_INGRESS_TRANSACTIONS, ctx->metrics.slot_ingress_transactions_cnt );
  FD_MCNT_ENUM_COPY( BAM, LEADER_SLOT_END_STATUS,     ctx->metrics.leader_slot_end_status_cnt );
  FD_MCNT_ENUM_COPY( BAM, HEALTHY_LEADER_SLOT_RESULT, ctx->metrics.healthy_leader_slot_result_cnt );

  FD_MGAUGE_SET( BAM, KEEPALIVE_RTT_SAMPLE,    (ulong)ctx->rtt->latest_rtt   );
  FD_MGAUGE_SET( BAM, KEEPALIVE_RTT_SMOOTHED,  (ulong)ctx->rtt->smoothed_rtt );
  FD_MGAUGE_SET( BAM, KEEPALIVE_RTT_DEVIATION, (ulong)ctx->rtt->var_rtt      );
  FD_MGAUGE_SET( BAM, KEEPALIVE_RTT_VALID,     (ulong)ctx->rtt->is_rtt_valid );
  FD_MGAUGE_SET( BAM, FEEDBACK_QUEUE_DEPTH, (ulong)ctx->feedback_queue_depth );
  FD_MGAUGE_SET( BAM, ENABLED,              (ulong)ctx->enabled );
  FD_MGAUGE_SET( BAM, HEALTHY,              (ulong)healthy );
  FD_MGAUGE_SET( BAM, STREAM_LIVE,          (ulong)ctx->bam_stream_live );
  FD_MGAUGE_SET( BAM, LEADER_STATE_SLOT,    ctx->bam_leader_state.slot==ULONG_MAX ? 0UL : ctx->bam_leader_state.slot );
  FD_MGAUGE_SET( BAM, LEADER_STATE_TICK,    (ulong)ctx->bam_leader_state.tick );
  FD_MGAUGE_SET( BAM, LEADER_STATE_SLOT_END_NANOS,
                 ctx->bam_leader_state.slot_end_ns>0L ? (ulong)ctx->bam_leader_state.slot_end_ns : 0UL );
  FD_MGAUGE_SET( BAM, CURRENT_LEADER_SLOT_FIRST_INGRESS_RECORDED,       current_slot_first_ingress_recorded );
  FD_MGAUGE_SET( BAM, CURRENT_LEADER_SLOT_FIRST_INGRESS_SLOT_END_KNOWN, current_slot_first_ingress_slot_end_known );
  FD_MGAUGE_SET( BAM, CURRENT_LEADER_SLOT_FIRST_INGRESS_DISTANCE_NANOS, current_slot_first_ingress_distance_nanos );
  FD_MGAUGE_SET( BAM, CURRENT_LEADER_SLOT_FIRST_INGRESS_AFTER_END,      current_slot_first_ingress_after_end );
  FD_MHIST_COPY( BAM, BUILDER_HEARTBEAT_ARRIVAL_DELTA_NANOS, ctx->metrics.builder_heartbeat_arrival_delta_nanos );
  FD_MHIST_COPY( BAM, SCHEDULER_PONG_ENQUEUE_NANOS, ctx->metrics.scheduler_pong_enqueue_nanos );

  fd_wksp_t * wksp = fd_wksp_containing( ctx );
  fd_wksp_usage_t usage[1];
  ulong const free_tag = 0UL;
  if( FD_UNLIKELY( !fd_wksp_usage( wksp, &free_tag, 1UL, usage ) ) ) {
    FD_LOG_ERR(( "fd_wksp_usage failed" )); /* unreachable */
  }
  FD_MGAUGE_SET( BAM, HEAP_SIZE,       usage->total_sz );
  FD_MGAUGE_SET( BAM, HEAP_FREE_BYTES, usage->free_sz  );

}

static inline void
fd_bam_record_leader_slot_end( fd_bam_tile_t *               ctx,
                               fd_plugin_bam_update_status_t status_fallback,
                               fd_bam_leader_state_t const * leader_state,
                               long                          now_ns ) {
  if( FD_UNLIKELY( leader_state->slot==ULONG_MAX ||
                   !leader_state->slot_end_ns ||
                   now_ns < leader_state->slot_end_ns ||
                   ctx->leader_slot_end_last_slot == leader_state->slot ) ) return;

  fd_plugin_bam_update_status_t status = status_fallback;
  if( FD_LIKELY( ctx->bam_status_history_cnt ) ) {
    ulong oldest_idx = ( ctx->bam_status_history_next - ctx->bam_status_history_cnt ) & ( FD_BAM_STATUS_HISTORY_CNT - 1UL );
    status = ctx->bam_status_history[ oldest_idx ].status;
    for( ulong i=0UL; i<ctx->bam_status_history_cnt; i++ ) {
      ulong idx = ( ctx->bam_status_history_next - 1UL - i ) & ( FD_BAM_STATUS_HISTORY_CNT - 1UL );
      fd_bam_status_history_t const * entry = &ctx->bam_status_history[ idx ];
      if( FD_LIKELY( entry->ts_ns <= leader_state->slot_end_ns ) ) {
        status = entry->status;
        break;
      }
    }
  }

  ulong status_idx = (ulong)status;
  if( FD_UNLIKELY( status_idx >= FD_METRICS_ENUM_BAM_LEADER_SLOT_END_STATUS_CNT ) ) {
    FD_LOG_ERR(( "unknown BAM status code %u", (uint)status ));
  }

  ctx->metrics.leader_slot_end_status_cnt[ status_idx ]++;
  if( FD_LIKELY( status==FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY ) ) {
    ulong result_idx = leader_state->current_slot_fresh
      ? FD_METRICS_ENUM_BAM_HEALTHY_LEADER_SLOT_RESULT_V_FRESH_WORK_IDX
      : FD_METRICS_ENUM_BAM_HEALTHY_LEADER_SLOT_RESULT_V_NO_FRESH_WORK_IDX;
    ctx->metrics.healthy_leader_slot_result_cnt[ result_idx ]++;
  }
  ctx->leader_slot_end_last_slot = leader_state->slot;
}

// Updates ContactInfo to BAM or default TPU based on use_bam
void
fd_bam_gossip_update( fd_bam_tile_t *    ctx,
                      fd_stem_context_t * stem,
                      _Bool use_bam) {
  fd_bam_tpu_update_state_t desired_applied = use_bam
    ? FD_BAM_TPU_UPDATE_STATE_APPLIED_BAM
    : FD_BAM_TPU_UPDATE_STATE_APPLIED_DEFAULT;
  fd_bam_tpu_update_state_t desired_pending = use_bam
    ? FD_BAM_TPU_UPDATE_STATE_PENDING_BAM
    : FD_BAM_TPU_UPDATE_STATE_PENDING_DEFAULT;

  if( FD_UNLIKELY( ctx->tpu_update_state != desired_applied ) ) {
    if( FD_UNLIKELY( !ctx->admin_rpc_path[0] ) ) {
      ctx->tpu_update_state = desired_applied;
    } else {
      fd_ip4_port_t current_tpu     = (fd_ip4_port_t){0};
      fd_ip4_port_t current_tpu_fwd = (fd_ip4_port_t){0};
      int current_rc = fd_bam_admin_rpc_get_contact_info( ctx->admin_rpc_path, &current_tpu, &current_tpu_fwd );
      if( FD_UNLIKELY( current_rc ) ) {
        ctx->tpu_update_state = desired_pending;
      } else {
        _Bool have_default_tpu = !!( ctx->default_tpu.addr     &&
                                     ctx->default_tpu.port     &&
                                     ctx->default_tpu_fwd.addr &&
                                     ctx->default_tpu_fwd.port );

        /* Cache the non-BAM TPU to restore it if BAM is disabled/disconnects.
           This can't be done in init() since agave takes a long time to start. */
        if( FD_UNLIKELY( !have_default_tpu ) ) {
          if( FD_UNLIKELY( !current_tpu.addr || !current_tpu_fwd.addr || !current_tpu.port || !current_tpu_fwd.port ) ) {
            FD_LOG_WARNING(( "Failed to cache default TPU, invalid ip/port. tpu=" FD_IP4_ADDR_FMT ":%hu, tpu_fwd=" FD_IP4_ADDR_FMT ":%hu)",
                             FD_IP4_ADDR_FMT_ARGS( current_tpu.addr ), fd_ushort_bswap( current_tpu.port ),
                             FD_IP4_ADDR_FMT_ARGS( current_tpu_fwd.addr ), fd_ushort_bswap( current_tpu_fwd.port ) ) );
          } else if( FD_UNLIKELY( current_tpu.l==ctx->bam_tpu.l &&
                                  current_tpu_fwd.l==ctx->bam_tpu_fwd.l ) ) {
            if( FD_LIKELY( ctx->configured_default_tpu.addr &&
                           ctx->configured_default_tpu.port &&
                           ( ctx->configured_default_tpu.l!=ctx->bam_tpu.l ||
                             ctx->configured_default_tpu.l!=ctx->bam_tpu_fwd.l ) ) ) {
              ctx->default_tpu     = ctx->configured_default_tpu;
              ctx->default_tpu_fwd = ctx->configured_default_tpu;
              have_default_tpu     = true;
              FD_LOG_NOTICE(( "Using configured default TPU addresses because current admin RPC readback already matches BAM TPU: tpu=" FD_IP4_ADDR_FMT ":%hu fwd=" FD_IP4_ADDR_FMT ":%hu.",
                              FD_IP4_ADDR_FMT_ARGS( ctx->default_tpu.addr ),
                              fd_ushort_bswap( ctx->default_tpu.port ),
                              FD_IP4_ADDR_FMT_ARGS( ctx->default_tpu_fwd.addr ),
                              fd_ushort_bswap( ctx->default_tpu_fwd.port ) ));
            } else {
              FD_LOG_WARNING(( "Default TPU cache deferred because current admin RPC readback already matches BAM TPU and no safe C-side default TPU advert is available" ));
            }
          } else {
            ctx->default_tpu     = current_tpu;
            ctx->default_tpu_fwd = current_tpu_fwd;
            have_default_tpu     = true;
            FD_LOG_NOTICE(( "Agave default TPU addresses: tpu=" FD_IP4_ADDR_FMT ":%hu fwd=" FD_IP4_ADDR_FMT ":%hu.",
                            FD_IP4_ADDR_FMT_ARGS( current_tpu.addr ), fd_ushort_bswap( current_tpu.port ),
                            FD_IP4_ADDR_FMT_ARGS( current_tpu_fwd.addr ), fd_ushort_bswap( current_tpu_fwd.port ) ));
          }
        }

        fd_ip4_port_t tpu     = use_bam ? ctx->bam_tpu     : ctx->default_tpu;
        fd_ip4_port_t tpu_fwd = use_bam ? ctx->bam_tpu_fwd : ctx->default_tpu_fwd;

        if( FD_UNLIKELY( !use_bam && !have_default_tpu ) ) {
          FD_LOG_WARNING(( "Attempted to revert TPU before agave finished initializing" ));
          ctx->tpu_update_state = desired_pending;
        } else if( FD_UNLIKELY( !tpu.addr || !tpu.port || !tpu_fwd.addr || !tpu_fwd.port ) ) {
          FD_LOG_WARNING(( "Failed to update TPU addresses: target incomplete tpu=" FD_IP4_ADDR_FMT ":%hu fwd=" FD_IP4_ADDR_FMT ":%hu",
                           FD_IP4_ADDR_FMT_ARGS( tpu.addr ),
                           fd_ushort_bswap( tpu.port ),
                           FD_IP4_ADDR_FMT_ARGS( tpu_fwd.addr ),
                           fd_ushort_bswap( tpu_fwd.port ) ));
          ctx->tpu_update_state = desired_pending;
        } else if( FD_UNLIKELY( current_tpu.l==tpu.l && current_tpu_fwd.l==tpu_fwd.l ) ) {
          FD_LOG_NOTICE(( "TPU addresses already match desired %s state: tpu=" FD_IP4_ADDR_FMT ":%hu fwd=" FD_IP4_ADDR_FMT ":%hu",
                          use_bam ? "BAM" : "default",
                          FD_IP4_ADDR_FMT_ARGS( tpu.addr ),
                          fd_ushort_bswap( tpu.port ),
                          FD_IP4_ADDR_FMT_ARGS( tpu_fwd.addr ),
                          fd_ushort_bswap( tpu_fwd.port ) ));
          ctx->tpu_update_state = desired_applied;
        } else {
          FD_LOG_NOTICE(( "Prepare to set TPU addresses: tpu=" FD_IP4_ADDR_FMT ":%hu fwd=" FD_IP4_ADDR_FMT ":%hu, use_bam: %d", /* FIXME: change to INFO level */
                          FD_IP4_ADDR_FMT_ARGS( tpu.addr ),
                          fd_ushort_bswap( tpu.port ),
                          FD_IP4_ADDR_FMT_ARGS( tpu_fwd.addr ),
                          fd_ushort_bswap( tpu_fwd.port ),
                          use_bam ));

          int set_rc = fd_bam_admin_rpc_set_public_tpu( ctx->admin_rpc_path, tpu, tpu_fwd );
          if( FD_UNLIKELY( set_rc ) ) {
            FD_LOG_WARNING(( "Failed to update TPU addresses: tpu=" FD_IP4_ADDR_FMT ":%hu fwd=" FD_IP4_ADDR_FMT ":%hu",
                             FD_IP4_ADDR_FMT_ARGS( tpu.addr ),
                             fd_ushort_bswap( tpu.port ),
                             FD_IP4_ADDR_FMT_ARGS( tpu_fwd.addr ),
                             fd_ushort_bswap( tpu_fwd.port ) ));
            ctx->tpu_update_state = desired_pending;
          } else {
            fd_ip4_port_t readback_tpu     = (fd_ip4_port_t){0};
            fd_ip4_port_t readback_tpu_fwd = (fd_ip4_port_t){0};
            int readback_rc = fd_bam_admin_rpc_get_contact_info( ctx->admin_rpc_path, &readback_tpu, &readback_tpu_fwd );
            if( FD_LIKELY( !readback_rc && readback_tpu.l==tpu.l && readback_tpu_fwd.l==tpu_fwd.l ) ) {
              FD_LOG_NOTICE(( "Updated TPU addresses: tpu=" FD_IP4_ADDR_FMT ":%hu fwd=" FD_IP4_ADDR_FMT ":%hu", /* FIXME: change to INFO level */
                              FD_IP4_ADDR_FMT_ARGS( tpu.addr ),
                              fd_ushort_bswap( tpu.port ),
                              FD_IP4_ADDR_FMT_ARGS( tpu_fwd.addr ),
                              fd_ushort_bswap( tpu_fwd.port ) ));
              ctx->tpu_update_state = desired_applied;
            } else {
              if( FD_UNLIKELY( readback_rc ) ) {
                FD_LOG_WARNING(( "Failed to verify TPU addresses after apply: tpu=" FD_IP4_ADDR_FMT ":%hu fwd=" FD_IP4_ADDR_FMT ":%hu",
                                 FD_IP4_ADDR_FMT_ARGS( tpu.addr ),
                                 fd_ushort_bswap( tpu.port ),
                                 FD_IP4_ADDR_FMT_ARGS( tpu_fwd.addr ),
                                 fd_ushort_bswap( tpu_fwd.port ) ));
              } else {
                FD_LOG_WARNING(( "Failed to verify TPU addresses after apply: expected tpu=" FD_IP4_ADDR_FMT ":%hu fwd=" FD_IP4_ADDR_FMT ":%hu, readback tpu=" FD_IP4_ADDR_FMT ":%hu fwd=" FD_IP4_ADDR_FMT ":%hu",
                                 FD_IP4_ADDR_FMT_ARGS( tpu.addr ),
                                 fd_ushort_bswap( tpu.port ),
                                 FD_IP4_ADDR_FMT_ARGS( tpu_fwd.addr ),
                                 fd_ushort_bswap( tpu_fwd.port ),
                                 FD_IP4_ADDR_FMT_ARGS( readback_tpu.addr ),
                                 fd_ushort_bswap( readback_tpu.port ),
                                 FD_IP4_ADDR_FMT_ARGS( readback_tpu_fwd.addr ),
                                 fd_ushort_bswap( readback_tpu_fwd.port ) ));
              }
              ctx->tpu_update_state = desired_pending;
            }
          }
        }
      }
    }
  }

  if( FD_UNLIKELY( !ctx->gossip_out.mem ) ) return;
  fd_ip4_port_t tpu     = use_bam ? ctx->bam_tpu     : ctx->default_tpu;
  fd_ip4_port_t tpu_fwd = use_bam ? ctx->bam_tpu_fwd : ctx->default_tpu_fwd;
  /* Full firedancer uses Gossip tile, it consumes these messages and mutates its local contact-info state. */
  fd_bam_contact_update_t * msg = fd_chunk_to_laddr( ctx->gossip_out.mem, ctx->gossip_out.chunk );
  msg->tpu     = tpu;
  msg->tpu_fwd = tpu_fwd;

  fd_stem_publish( stem,
                   ctx->gossip_out.idx,
                   FD_BAM_STEM_SIG_GOSSIP_UPDATE,
                   ctx->gossip_out.chunk,
                   sizeof(fd_bam_contact_update_t),
                   0UL,
                   0UL,
                   fd_frag_meta_ts_comp( fd_bam_now() ) );
  ctx->gossip_out.chunk = fd_dcache_compact_next( ctx->gossip_out.chunk,
                                                  sizeof(fd_bam_contact_update_t),
                                                  ctx->gossip_out.chunk0,
                                                  ctx->gossip_out.wmark );
}

static void fd_bam_tile_handle_ctrl( fd_bam_tile_t * ctx );

/* Two-phase fragment staging kind.
   - bam_during_frag validates size/range and stores chunk + kind.
   - bam_after_frag consumes that staged chunk based on kind.
   Edge cases / invariants:
   - NONE: fail-closed state used for unknown sizes, bad chunks, or unexpected
     in_idx. after_frag must no-op.
   - RESULT: staged chunk points to fd_bam_bundle_result_t. This can originate
     from either bank->bam or pack->bam links, so after_frag must still branch
     on in_idx to choose the right dcache base.
   - LEADER: staged chunk points to fd_bam_leader_state_t and is only valid
     from pack->bam. Any other in_idx is malformed and dropped. */
enum {
  FD_BAM_FRAG_STAGED_NONE   = 0U, /* No staged payload (or staged payload was rejected). */
  FD_BAM_FRAG_STAGED_RESULT = 1U, /* fd_bam_bundle_result_t staged for enqueue_result. */
  FD_BAM_FRAG_STAGED_LEADER = 2U  /* fd_bam_leader_state_t staged for bam_leader_state update. */
};

void
fd_bam_tile_housekeeping( fd_bam_tile_t * ctx ) {
  fd_bam_tile_handle_ctrl( ctx );
  long now_ns = fd_log_wallclock();
  ulong leader_slot = ctx->bam_leader_state.slot;
  if( FD_LIKELY( leader_slot!=ULONG_MAX ) ) {
    for( ulong i=0UL; i<FD_BAM_SLOT_INGRESS_TIMING_CNT; i++ ) {
      fd_bam_slot_ingress_timing_t * entry = &ctx->slot_ingress_timing[ i ];
      if( FD_LIKELY( !entry->valid ) ) continue;
      if( FD_LIKELY( leader_slot<=entry->slot ) ) continue;
      if( FD_UNLIKELY( !entry->summary_emitted ) ) {
        fd_bam_try_emit_slot_ingress_timing_summary( ctx, entry, leader_slot );
      }
      if( FD_LIKELY( leader_slot-entry->slot<FD_BAM_SLOT_INGRESS_RETENTION_SLOTS ) ) continue;
      fd_bam_finalize_slot_ingress_rollup( ctx, entry, leader_slot );
    }
  }

  if( FD_LIKELY( ctx->plugin_out.mem ) ) {
    long next_gui_refresh = ctx->last_gui_publish_nanos + (long)5e9;
    if( FD_UNLIKELY( now_ns > next_gui_refresh ) )
      ctx->gui_dirty = 1U;
  }

  long log_interval_ns = (long)30e9;
  long log_next_ns     = ctx->last_bam_status_log_nanos + log_interval_ns;
  fd_plugin_bam_update_status_t status = fd_bam_client_status( ctx );
  if( FD_UNLIKELY( (
    status == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED ||
    status == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTING ) && now_ns > log_next_ns ) ) {
    FD_LOG_WARNING(( "No BAM node connection in the last %ld seconds", log_interval_ns/(long)1e9 ) );
    ctx->last_bam_status_log_nanos = now_ns;
  }

  fd_bam_leader_state_t const * leader_state = &ctx->bam_leader_state;
  fd_bam_record_leader_slot_end( ctx, status, leader_state, now_ns );
  _Bool use_bam = status==FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY;
  _Bool current_slot_fresh = use_bam && fd_bam_current_slot_fresh( ctx, now_ns );
  _Bool tpu_update_pending = ( ctx->tpu_update_state == FD_BAM_TPU_UPDATE_STATE_PENDING_BAM ) |
                            ( ctx->tpu_update_state == FD_BAM_TPU_UPDATE_STATE_PENDING_DEFAULT );
  if( FD_UNLIKELY( ctx->bam_status_recent != status || tpu_update_pending ) ) {
    fd_bam_gossip_update( ctx, ctx->stem, use_bam );
  }
  ctx->bam_status_recent = status;
  if( FD_LIKELY( ctx->bam_status_fseq ) ) {
    /* Expose BAM connectivity via a shared latch. The verify tile uses
       this to pause QUIC/bundle traffic when BAM has taken over leader
       duties.  fd_bam_client_status only returns CONNECTED once the
       transport, auth, and scheduler stream are fully live, else
       immediately release the TPU back to default Firedancer behaviour */
    ulong bam_status =
        fd_ulong_if( use_bam, FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE, 0UL ) |
        fd_ulong_if( current_slot_fresh,
                     FD_BAM_STATUS_FSEQ_CURRENT_SLOT_FRESH,
                     0UL );
    fd_fseq_update( ctx->bam_status_fseq, bam_status );
  }

  if( FD_UNLIKELY( fd_keyswitch_state_query( ctx->keyswitch ) == FD_KEYSWITCH_STATE_SWITCH_PENDING ) ) {
    fd_memcpy( ctx->bam_identity_pubkey, ctx->keyswitch->bytes, 32UL );
    fd_base58_encode_32( ctx->keyswitch->bytes, NULL, ctx->bam_identity_pubkey_b58 );
    fd_keyswitch_state( ctx->keyswitch, FD_KEYSWITCH_STATE_COMPLETED );
    ctx->defer_reset = 1;
    FD_LOG_NOTICE(( "BAM identity pubkey updated to %s", ctx->bam_identity_pubkey_b58 ));
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

  ctx->frag_staged_kind = FD_BAM_FRAG_STAGED_NONE;
  ctx->frag_staged_chunk = 0UL;

  if( FD_LIKELY( in_idx == ctx->bank_bam_in_idx ) ) {
    if( FD_UNLIKELY( sz != sizeof(fd_bam_bundle_result_t) ) ) {
      FD_LOG_WARNING(( "Unexpected BAM bundle result size %lu", sz ));
      return;
    }
    if( FD_UNLIKELY( chunk < ctx->bank_in.chunk0 || chunk > ctx->bank_in.wmark ) ) {
      FD_LOG_WARNING(( "BAM bundle result chunk %lu out of range [%lu,%lu]", chunk, ctx->bank_in.chunk0, ctx->bank_in.wmark ));
      return;
    }
    ctx->frag_staged_chunk = chunk;
    ctx->frag_staged_kind = FD_BAM_FRAG_STAGED_RESULT;
    return;
  }

  fd_bam_in_ctx_t const * pack_in;
  ulong                   expected_sz;
  uchar                   staged_kind;
  char const *            size_what;
  char const *            chunk_what;

  if( FD_LIKELY( in_idx == ctx->pack_bam_leader_in_idx ) ) {
    pack_in     = &ctx->pack_leader_in;
    expected_sz = sizeof(fd_bam_leader_state_t);
    staged_kind = FD_BAM_FRAG_STAGED_LEADER;
    size_what   = "pack->bam leader fragment";
    chunk_what  = "BAM leader state";
  } else if( FD_LIKELY( in_idx == ctx->pack_bam_result_in_idx ) ) {
    pack_in     = &ctx->pack_result_in;
    expected_sz = sizeof(fd_bam_bundle_result_t);
    staged_kind = FD_BAM_FRAG_STAGED_RESULT;
    size_what   = "pack->bam result fragment";
    chunk_what  = "BAM bundle result";
  } else {
    return;
  }

  if( FD_UNLIKELY( sz != expected_sz ) ) {
    FD_LOG_WARNING(( "Unexpected %s size %lu", size_what, sz ));
    return;
  }
  if( FD_UNLIKELY( chunk < pack_in->chunk0 || chunk > pack_in->wmark ) ) {
    FD_LOG_WARNING(( "%s chunk %lu out of range [%lu,%lu]", chunk_what, chunk, pack_in->chunk0, pack_in->wmark ));
    return;
  }
  ctx->frag_staged_chunk = chunk;
  ctx->frag_staged_kind = staged_kind;
}

static void
bam_after_frag( fd_bam_tile_t *     ctx,
                ulong               in_idx,
                ulong               seq    FD_PARAM_UNUSED,
                ulong               sig    FD_PARAM_UNUSED,
                ulong               sz     FD_PARAM_UNUSED,
                ulong               tsorig FD_PARAM_UNUSED,
                ulong               tspub  FD_PARAM_UNUSED,
                fd_stem_context_t * stem   FD_PARAM_UNUSED ) {
  switch( ctx->frag_staged_kind ) {
  case FD_BAM_FRAG_STAGED_RESULT: {
    fd_bam_bundle_result_t const * res = NULL;
    if( FD_LIKELY( in_idx==ctx->bank_bam_in_idx ) ) {
      res = (fd_bam_bundle_result_t const *)fd_chunk_to_laddr( ctx->bank_in.mem, ctx->frag_staged_chunk );
    } else if( FD_LIKELY( in_idx==ctx->pack_bam_result_in_idx ) ) {
      res = (fd_bam_bundle_result_t const *)fd_chunk_to_laddr( ctx->pack_result_in.mem, ctx->frag_staged_chunk );
    } else {
      FD_LOG_WARNING(( "Unexpected in_idx=%lu for staged BAM bundle result", in_idx ));
      break;
    }
    fd_bam_enqueue_result( ctx, res );
    break;
  }
  case FD_BAM_FRAG_STAGED_LEADER:
    if( FD_UNLIKELY( in_idx!=ctx->pack_bam_leader_in_idx ) ) {
      FD_LOG_WARNING(( "Unexpected in_idx=%lu for staged BAM leader state", in_idx ));
      break;
    }
    fd_bam_leader_state_t const * leader_state = (fd_bam_leader_state_t const *)fd_chunk_to_laddr( ctx->pack_leader_in.mem, ctx->frag_staged_chunk );
    fd_bam_leader_state_t const * prev_leader_state = &ctx->bam_leader_state;
    if( FD_LIKELY( prev_leader_state->slot!=ULONG_MAX &&
                   prev_leader_state->slot!=leader_state->slot &&
                   prev_leader_state->slot_end_ns ) ) {
      long now_ns = fd_log_wallclock();
      if( FD_LIKELY( now_ns >= prev_leader_state->slot_end_ns || leader_state->slot > prev_leader_state->slot ) ) {
        fd_bam_record_leader_slot_end( ctx,
                                       fd_bam_client_status( ctx ),
                                       prev_leader_state,
                                       fd_long_max( now_ns, prev_leader_state->slot_end_ns ) );
      }
    }
    fd_bam_stage_leader_state( ctx, leader_state );
    break;
  default:
    /* Unknown staged kind (e.g. memory corruption) is ignored to avoid
       dereferencing an unvalidated chunk pointer. */
    break;
  }
  ctx->frag_staged_kind = FD_BAM_FRAG_STAGED_NONE;
  ctx->frag_staged_chunk = 0UL;
}

void
fd_bam_test_receive_ingress_frag( fd_bam_tile_t * ctx,
                                  ulong           in_idx,
                                  ulong           chunk,
                                  ulong           sz ) {
  bam_during_frag( ctx, in_idx, 0UL, 0UL, chunk, sz, 0UL );
  bam_after_frag( ctx, in_idx, 0UL, 0UL, sz, 0UL, 0UL, NULL );
}

void
fd_bam_test_metrics_write( fd_bam_tile_t * ctx ) {
  metrics_write( ctx );
}

static void
after_credit( fd_bam_tile_t *  ctx,
              fd_stem_context_t * stem,
              int *               opt_poll_in,
              int *               charge_busy ) {
  (void)opt_poll_in;
  if( FD_UNLIKELY( !ctx->stem ) ) ctx->stem = stem;
  fd_bam_client_step( ctx, charge_busy );

  if( FD_UNLIKELY( !ctx->plugin_out.mem ) ) return;
  if( FD_LIKELY( !ctx->gui_dirty && ctx->bam_status_recent == ctx->bam_status_plugin ) ) return;

  fd_plugin_msg_bam_update_t * update =
      fd_chunk_to_laddr( ctx->plugin_out.mem, ctx->plugin_out.chunk );
  fd_bam_metrics_t const * metrics = &ctx->metrics;
  ulong const * failure_cnt = metrics->failure_cnt;
  ulong const * ingress_batch_rejected_cnt = metrics->ingress_batch_rejected_cnt;
  ulong const * ingress_message_rejected_cnt = metrics->ingress_message_rejected_cnt;
  ulong const * leader_slot_end_status_cnt = metrics->leader_slot_end_status_cnt;
  memset( update, 0, sizeof(fd_plugin_msg_bam_update_t) );

  strlcpy( update->name, "bam", sizeof( update->name ));

  if( FD_LIKELY( ctx->server_fqdn_len && ctx->server_tcp_port ) ) {
    snprintf( update->url, sizeof( update->url ), "%s://%.*s:%u",
              ctx->is_ssl ? "https" : "http",
              (int)ctx->server_fqdn_len,
              ctx->server_fqdn,
              ctx->server_tcp_port );
  }

  strlcpy( update->sni, ctx->server_sni, sizeof( update->sni ) );
  snprintf( update->ip_cstr, sizeof( update->ip_cstr ),
            FD_IP4_ADDR_FMT,
            FD_IP4_ADDR_FMT_ARGS( ctx->server_ip4_addr ) );

  if( FD_LIKELY( ctx->bam_tpu.addr && ctx->bam_tpu.port ) ) {
    snprintf( update->tpu_cstr, sizeof( update->tpu_cstr ),
              FD_IP4_ADDR_FMT ":%hu",
              FD_IP4_ADDR_FMT_ARGS( ctx->bam_tpu.addr ),
              fd_ushort_bswap( ctx->bam_tpu.port ) );
  }

  if( FD_LIKELY( ctx->bam_tpu_fwd.addr && ctx->bam_tpu_fwd.port ) ) {
    snprintf( update->tpu_fwd_cstr, sizeof( update->tpu_fwd_cstr ),
              FD_IP4_ADDR_FMT ":%hu",
              FD_IP4_ADDR_FMT_ARGS( ctx->bam_tpu_fwd.addr ),
              fd_ushort_bswap( ctx->bam_tpu_fwd.port ) );
  }

  update->status_code  = ctx->bam_status_recent;
  update->enabled = ctx->enabled;
  update->keepalive_rtt_sample    = ctx->rtt->latest_rtt;
  update->keepalive_rtt_smoothed  = ctx->rtt->smoothed_rtt;
  update->keepalive_rtt_deviation = ctx->rtt->var_rtt;
  update->feedback_queue_depth = ctx->feedback_queue_depth;
  update->validator_heartbeats_enqueued =
      metrics->outbound_enqueue_outcome_cnt[ FD_METRICS_ENUM_BAM_ENQUEUE_OUTCOME_V_HEARTBEAT_ENQUEUED_IDX ];
  update->builder_heartbeats_decoded = metrics->builder_heartbeats_decoded_cnt;
  update->transaction_published  = metrics->transaction_published_cnt;
  update->atomic_batch_published = metrics->atomic_batch_published_cnt;
  update->ingress_packet_oversize = metrics->ingress_packet_oversize_cnt;
  update->failure_auth_challenge_decode = failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_AUTH_CHALLENGE_DECODE_IDX ];
  update->failure_config_decode = failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_CONFIG_DECODE_IDX ];
  update->failure_scheduler_envelope_decode = failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_SCHEDULER_ENVELOPE_DECODE_IDX ];
  update->failure_request_failed = failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_REQUEST_FAILED_IDX ];
  update->failure_transport =
      failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_RESOLVE_IDX ] +
      failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_CONNECT_IDX ] +
      failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_IO_IDX ];
  update->failure_unsupported_version = failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_UNSUPPORTED_VERSION_IDX ];
  update->failure_timeout =
      failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_REQUEST_TIMEOUT_IDX ] +
      failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_KEEPALIVE_TIMEOUT_IDX ] +
      failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_BUILDER_HEARTBEAT_TIMEOUT_IDX ];
  update->ingress_multi_message_received = metrics->ingress_multi_message_received_cnt;
  update->ingress_batch_commit_attempt = metrics->ingress_batch_commit_attempt_cnt;
  update->ingress_batch_published = metrics->ingress_batch_published_cnt;
  update->ingress_batch_rejected_invalid_batch = ingress_batch_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_INVALID_BATCH_IDX ];
  update->ingress_batch_rejected_empty_batch = ingress_batch_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_EMPTY_BATCH_IDX ];
  update->ingress_batch_rejected_vote_transaction = ingress_batch_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_VOTE_TRANSACTION_IDX ];
  update->ingress_batch_rejected_non_revert_multi_packet = ingress_batch_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_NON_REVERT_MULTI_PACKET_IDX ];
  update->ingress_message_rejected_empty_message = ingress_message_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_MESSAGE_REJECT_REASON_V_EMPTY_MESSAGE_IDX ];
  update->ingress_message_rejected_overflow_message = ingress_message_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_MESSAGE_REJECT_REASON_V_OVERFLOW_MESSAGE_IDX ];
  update->leader_slot_end_status_disabled = leader_slot_end_status_cnt[ FD_METRICS_ENUM_BAM_LEADER_SLOT_END_STATUS_V_DISABLED_IDX ];
  update->leader_slot_end_status_disconnected = leader_slot_end_status_cnt[ FD_METRICS_ENUM_BAM_LEADER_SLOT_END_STATUS_V_DISCONNECTED_IDX ];
  update->leader_slot_end_status_connecting = leader_slot_end_status_cnt[ FD_METRICS_ENUM_BAM_LEADER_SLOT_END_STATUS_V_CONNECTING_IDX ];
  update->leader_slot_end_status_connected_unhealthy = leader_slot_end_status_cnt[ FD_METRICS_ENUM_BAM_LEADER_SLOT_END_STATUS_V_CONNECTED_UNHEALTHY_IDX ];
  update->leader_slot_end_status_connected_healthy = leader_slot_end_status_cnt[ FD_METRICS_ENUM_BAM_LEADER_SLOT_END_STATUS_V_CONNECTED_HEALTHY_IDX ];

  ulong tspub = fd_frag_meta_ts_comp( fd_bam_now() );
  fd_stem_publish(
      stem,
      ctx->plugin_out.idx,
      FD_PLUGIN_MSG_BAM_UPDATE,
      ctx->plugin_out.chunk,
      sizeof(fd_plugin_msg_bam_update_t),
      0UL,
      0UL,
      tspub
  );
  ctx->last_gui_publish_nanos = fd_log_wallclock();
  ctx->plugin_out.chunk = fd_dcache_compact_next( ctx->plugin_out.chunk, sizeof(fd_plugin_msg_bam_update_t), ctx->plugin_out.chunk0, ctx->plugin_out.wmark );
  ctx->bam_status_plugin = ctx->bam_status_recent;
  ctx->gui_dirty = 0U;
  *charge_busy = 1;
}

static int
fd_bam_tile_ctrl_update_current( fd_bam_tile_t * ctx ) {
  if( FD_UNLIKELY( !ctx->ctrl ) ) return -1;
  /* Surface an unset URL/SNI when BAM has no configured runtime endpoint. */
  ctx->ctrl->url[0] = '\0';
  ctx->ctrl->sni[0] = '\0';
  ctx->ctrl->enable = ctx->enabled;
  if( FD_UNLIKELY( !ctx->server_fqdn_len || !ctx->server_tcp_port ) ) {
    return 0;
  }
  char buf[FD_URL_MAX];
  // FIXME: check if `http` already in server_fqdn, if so, dont prepend
  int n = snprintf( buf, FD_URL_MAX, "%s://%.*s:%u",
                    ctx->is_ssl ? "https" : "http",
                    ctx->server_fqdn_len,
                    ctx->server_fqdn,
                    ctx->server_tcp_port );
  if( FD_UNLIKELY( n < 0 ) ) {
    ctx->ctrl->url[0] = '\0';
    return -1;
  }
  strlcpy(ctx->ctrl->url, buf, (size_t)n+1);
  strlcpy( ctx->ctrl->sni, ctx->server_sni, FD_SNI_BUF_MAX );
  return 0;
}

static char
fd_bam_tile_apply_ctrl_request( fd_bam_tile_t * ctx,
                                char *          err,
                                ulong           err_sz ) {
  uchar command = ctx->ctrl->command;
  if( FD_UNLIKELY( !command ) ) {
    fd_cstr_printf( err, err_sz, NULL, "No BAM update requested" );
    return -1;
  }

  ushort new_port = ctx->server_tcp_port;
  uchar  new_ssl  = ctx->is_ssl;
  char   new_host[ FD_FQDN_BUF_MAX ];
  ushort new_host_len;
  _Bool  need_reset = 0;

  if( command & FD_BAM_CTRL_CMD_URL ) {
    char const * p = ctx->ctrl->url;
    while( *p && fd_isspace( (int)*p ) ) p++;

    if( FD_UNLIKELY( !*p ) ) {
      /* Runtime blank URL means "clear BAM URL" and disable the client. */
      need_reset = !!ctx->server_fqdn_len || !!ctx->server_sni_len || !!ctx->server_tcp_port || !!ctx->is_ssl || !!ctx->enabled;
      ctx->server_fqdn[0]   = '\0';
      ctx->server_fqdn_len  = 0U;
      ctx->server_sni[0]    = '\0';
      ctx->server_sni_len   = 0U;
      ctx->server_tcp_port  = 0U;
      ctx->is_ssl           = 0U;
      ctx->enabled          = 0U;
      fd_grpc_client_set_authority( ctx->grpc_client, ctx->server_sni, ctx->server_sni_len, ctx->server_tcp_port );
      goto finalize;
    }

    ulong url_len = strlen( ctx->ctrl->url );
    fd_url_t runtime_url;
    ushort parse_port = new_port;
    _Bool  parse_ssl  = new_ssl;
    if( FD_UNLIKELY( fd_url_parse_endpoint( &runtime_url, ctx->ctrl->url, url_len, &parse_port, &parse_ssl, "runtime BAM url" ) < 0 ) ) {
      fd_cstr_printf( err, err_sz, NULL, "Invalid BAM URL `%s`", ctx->ctrl->url );
      return -1;
    }
    if( FD_UNLIKELY( !runtime_url.host_len ) ) {
      fd_cstr_printf( err, err_sz, NULL, "BAM URL `%s` missing host", ctx->ctrl->url );
      return -1;
    }
    if( FD_UNLIKELY( runtime_url.host_len >= FD_FQDN_BUF_MAX ) ) {
      fd_cstr_printf( err, err_sz, NULL, "BAM host name too long" );
      return -1;
    }

    fd_memcpy( new_host, runtime_url.host, runtime_url.host_len );
    new_host[ runtime_url.host_len ] = '\0';
    new_host_len = (ushort)runtime_url.host_len;
    new_port = parse_port;
    new_ssl  = parse_ssl;
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
  } else {
    new_host_len = (ushort)strlcpy( new_host, ctx->server_fqdn, sizeof(new_host) );
  }

  char new_sni[ FD_SNI_BUF_MAX ];
  if( command & FD_BAM_CTRL_CMD_SNI ) {
    strlcpy( new_sni, ctx->ctrl->sni, sizeof(new_sni) );
    if( FD_UNLIKELY( !new_sni[0] ) )
      strlcpy( new_sni, new_host, sizeof(new_sni) );
  } else if( command & FD_BAM_CTRL_CMD_URL ) {
    strlcpy( new_sni, new_host, sizeof(new_sni) );
  } else {
    strlcpy( new_sni, ctx->server_sni, sizeof(new_sni) );
  }

  uchar new_enable = (uchar)( ( command & FD_BAM_CTRL_CMD_ENABLE ) ? !!ctx->ctrl->enable : ctx->enabled );
  if( command & FD_BAM_CTRL_CMD_URL ) {
    strlcpy( ctx->server_fqdn, new_host, sizeof(ctx->server_fqdn) );
    ctx->server_fqdn_len = (ushort)fd_ulong_min( new_host_len, (ulong)USHORT_MAX );
    ctx->server_tcp_port = new_port;
    ctx->is_ssl          = !!new_ssl;
    need_reset = 1;
  }

  if( command & (FD_BAM_CTRL_CMD_URL | FD_BAM_CTRL_CMD_SNI) ) {
    ulong sni_len = strlcpy( ctx->server_sni, new_sni, sizeof(ctx->server_sni) );
    ctx->server_sni_len = (ushort)fd_ulong_min( sni_len, (ulong)USHORT_MAX );
    fd_grpc_client_set_authority( ctx->grpc_client, ctx->server_sni, ctx->server_sni_len, ctx->server_tcp_port );
    need_reset = 1;
  }

  if( ( command & FD_BAM_CTRL_CMD_ENABLE ) && ( new_enable != ctx->enabled ) ) {
    ctx->enabled = new_enable;
    need_reset = 1;
  }

finalize:
  if( need_reset ) {
    fd_bam_client_reset( ctx );
    ctx->backoff_until = 0; /* Clear any backoff so admin-triggered changes take effect immediately. */
    ctx->backoff_reset = 0;
    ctx->backoff_iter  = 0;
    if( FD_UNLIKELY( !ctx->enabled && ctx->bam_status_fseq ) )
      /* Force the shared status latch low immediately when BAM is
         disabled so downstream tiles resume QUIC/bundle input without
         waiting for TCP timeouts. */
      fd_fseq_update( ctx->bam_status_fseq, 0UL );
  }

  if ( FD_UNLIKELY( fd_bam_tile_ctrl_update_current( ctx ) < 0 ) ) {
    FD_LOG_WARNING(( "Failed to update BAM config" ));
    return -1;
  }

  ctx->gui_dirty = 1U;
  err[0] = '\0';
  return 0;
}

static void
fd_bam_tile_handle_ctrl( fd_bam_tile_t * ctx ) {
  if( FD_UNLIKELY( !ctx->ctrl ) ) return;

  /* Wait until we receive a new request. */
  for( ;; ) {
    uchar state = FD_VOLATILE_CONST( ctx->ctrl->state );
    if( FD_LIKELY( state != FD_BAM_CTRL_STATE_REQUEST ) ) return;
    if( FD_ATOMIC_CAS( &ctx->ctrl->state, FD_BAM_CTRL_STATE_REQUEST, FD_BAM_CTRL_STATE_APPLYING ) == FD_BAM_CTRL_STATE_REQUEST )
      break;
  }

  char err[ FD_BAM_CTRL_ERR_MAX ];
  err[0] = '\0';
  char rc = fd_bam_tile_apply_ctrl_request( ctx, err, sizeof(err) );
  if( FD_UNLIKELY( rc ) ) {
    strlcpy( ctx->ctrl->error, err, FD_BAM_CTRL_ERR_MAX );
    /* Revert the request fields to the actual state so set-bam sees the correct values next time */
    fd_bam_tile_ctrl_update_current( ctx );
    FD_COMPILER_MFENCE();
    FD_VOLATILE( ctx->ctrl->state ) = FD_BAM_CTRL_STATE_ERROR;
    return;
  }

  strlcpy( ctx->ctrl->error, "", FD_BAM_CTRL_ERR_MAX );
  FD_COMPILER_MFENCE();
  FD_VOLATILE( ctx->ctrl->state ) = FD_BAM_CTRL_STATE_SUCCESS;
}

static void
fd_bam_tile_parse_endpoint( fd_bam_tile_t *     ctx,
                               fd_topo_tile_t const * tile ) {
  fd_url_t url[1];
  _Bool is_ssl = 0;
  int res = fd_url_parse_endpoint(
      url,
      tile->bam.url, tile->bam.url_len,
      &ctx->server_tcp_port,
      &is_ssl,
      "[tiles.bam.url]"
  );
  if( FD_UNLIKELY( res < 0 ) ) {
    FD_LOG_CRIT(( "Failed to parse BAM endpoint" )); // TODO: dont crash
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

  if ( FD_UNLIKELY( fd_bam_tile_ctrl_update_current( ctx ) < 0 ) ) {
    FD_LOG_WARNING(( "Failed to update BAM config" ));
  }
}

#if FD_HAS_OPENSSL

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
  for( ;; ) {
    errno = 0; /* Track errors from this readdir() call only */
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
  /* TODO: plumb ssl_alloc through OpenSSL teardown/reset instead of keeping it as init-only state. */
  ctx->ssl_alloc = alloc;
  fd_openssl_set_thread_alloc( alloc );

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
  ctx->leader_slot_end_last_slot = ULONG_MAX;
  ctx->grpc_client_mem = grpc_mem;
  ctx->grpc_buf_max    = tile->bam.buf_sz;
  ctx->tcp_sock        = -1;
  ctx->bank_bam_in_idx = ULONG_MAX;
  ctx->pack_bam_leader_in_idx = ULONG_MAX;
  ctx->pack_bam_result_in_idx = ULONG_MAX;
  ctx->bundle_max_schedule_slot = FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT;

  uchar const * public_key = fd_keyload_load( tile->bam.identity_key_path, 1 /* public key only */ );
  fd_memcpy( ctx->bam_identity_pubkey, public_key, 32UL );
  fd_base58_encode_32( public_key, NULL, ctx->bam_identity_pubkey_b58 );
  FD_LOG_NOTICE(( "BAM identity pubkey updated to %s", ctx->bam_identity_pubkey_b58 ));

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

  ctx->enabled = !!tile->bam.enabled;
  ctx->dump_bam_txns = !!tile->bam.dump_bam_txns;
  ctx->dump_bam_first_slot_txn = !!tile->bam.dump_bam_first_slot_txn;
  strlcpy( ctx->admin_rpc_path, tile->bam.admin_rpc_path, sizeof(ctx->admin_rpc_path) );
  ctx->configured_default_tpu = tile->bam.configured_default_tpu;
  ctx->fee_cfg_version = 0U;
  ctx->commission_bps = 0U;
  ctx->prio_fee_recipient_set   = 0U;
  fd_memset( ctx->prio_fee_recipient, 0, sizeof( ctx->prio_fee_recipient ) );

  ulong bam_fee_cfg_obj_id = fd_pod_query_ulong( topo->props, "bam_fee_cfg", ULONG_MAX );
  if( FD_UNLIKELY( bam_fee_cfg_obj_id == ULONG_MAX ) ) FD_LOG_ERR(( "Missing bam_fee_cfg object" ));
  ctx->fee_cfg = fd_topo_obj_laddr( topo, bam_fee_cfg_obj_id );
  fd_memset( ctx->fee_cfg, 0, sizeof(fd_bam_fee_cfg_t) );

  ulong bam_ctrl_obj_id = fd_pod_query_ulong( topo->props, "bam_ctrl", ULONG_MAX );
  if( FD_UNLIKELY( bam_ctrl_obj_id == ULONG_MAX ) ) FD_LOG_ERR(( "Missing bam_ctrl object" ));
  ctx->ctrl = fd_topo_obj_laddr( topo, bam_ctrl_obj_id );
  fd_memset( ctx->ctrl, 0, sizeof(fd_bam_ctrl_t) );
  ctx->ctrl->state          = FD_BAM_CTRL_STATE_IDLE;
  ctx->ctrl->enable = ctx->enabled;
  strlcpy( ctx->ctrl->url, tile->bam.url, FD_URL_MAX );
  strlcpy( ctx->ctrl->sni, tile->bam.sni, FD_SNI_BUF_MAX );
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

static fd_bam_in_ctx_t
bam_in_link( fd_topo_t const *      topo,
             fd_topo_link_t const * link ) {
  fd_bam_in_ctx_t in = {0};
  in.mem    = topo->workspaces[ topo->objs[ link->dcache_obj_id ].wksp_id ].wksp;
  in.chunk0 = fd_dcache_compact_chunk0( in.mem, link->dcache );
  in.wmark  = fd_dcache_compact_wmark ( in.mem, link->dcache, link->mtu );
  return in;
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
      sign_in->dcache,
      sign_out->mtu
  ) ) ) ) {
    FD_LOG_ERR(( "fd_keyguard_client_join failed" )); /* unreachable */
  }

  ctx->keyswitch = fd_keyswitch_join( fd_topo_obj_laddr( topo, tile->keyswitch_obj_id ) );
  FD_TEST( ctx->keyswitch );

  ulong bank_in_idx = fd_topo_find_tile_in_link( topo, tile, "bank_bam", tile->kind_id );
  if( FD_UNLIKELY( bank_in_idx == ULONG_MAX ) ) FD_LOG_ERR(( "Missing bank_bam link" ));
  if( FD_UNLIKELY( !tile->in_link_poll[ bank_in_idx ] ) ) FD_LOG_ERR(( "bank_bam must be polled" ));
  /* stem callback in_idx is compacted over polled links only. */
  ctx->bank_bam_in_idx = 0UL;
  for( ulong i=0UL; i<bank_in_idx; i++ ) ctx->bank_bam_in_idx += (ulong)!!tile->in_link_poll[ i ];
  fd_topo_link_t const * bank_in = &topo->links[ tile->in_link_id[ bank_in_idx ] ];
  ctx->bank_in = bam_in_link( topo, bank_in );

  ulong leader_in_idx = fd_topo_find_tile_in_link( topo, tile, "pack_bam_ldr", tile->kind_id );
  if( FD_UNLIKELY( leader_in_idx == ULONG_MAX ) ) FD_LOG_ERR(( "Missing pack_bam_ldr link" ));
  if( FD_UNLIKELY( !tile->in_link_poll[ leader_in_idx ] ) ) FD_LOG_ERR(( "pack_bam_ldr must be polled" ));
  ctx->pack_bam_leader_in_idx = 0UL;
  for( ulong i=0UL; i<leader_in_idx; i++ ) ctx->pack_bam_leader_in_idx += (ulong)!!tile->in_link_poll[ i ];
  fd_topo_link_t const * leader_in = &topo->links[ tile->in_link_id[ leader_in_idx ] ];
  ctx->pack_leader_in = bam_in_link( topo, leader_in );

  ulong result_in_idx = fd_topo_find_tile_in_link( topo, tile, "pack_bam_res", tile->kind_id );
  if( FD_UNLIKELY( result_in_idx == ULONG_MAX ) ) FD_LOG_ERR(( "Missing pack_bam_res link" ));
  if( FD_UNLIKELY( !tile->in_link_poll[ result_in_idx ] ) ) FD_LOG_ERR(( "pack_bam_res must be polled" ));
  ctx->pack_bam_result_in_idx = 0UL;
  for( ulong i=0UL; i<result_in_idx; i++ ) ctx->pack_bam_result_in_idx += (ulong)!!tile->in_link_poll[ i ];
  fd_topo_link_t const * result_in = &topo->links[ tile->in_link_id[ result_in_idx ] ];
  ctx->pack_result_in = bam_in_link( topo, result_in );

  ulong verify_out_idx = fd_topo_find_tile_out_link( topo, tile, "bam_verif", tile->kind_id );
  if( FD_UNLIKELY( verify_out_idx == ULONG_MAX ) ) FD_LOG_ERR(( "Missing bam_verif link" ));
  ctx->verify_out = bam_out_link( topo, &topo->links[ tile->out_link_id[ verify_out_idx ] ], verify_out_idx );

  ulong plugin_out_idx = fd_topo_find_tile_out_link( topo, tile, "bam_plugi", tile->kind_id );
  if( plugin_out_idx != ULONG_MAX ) {
    ctx->plugin_out = bam_out_link( topo, &topo->links[ tile->out_link_id[ plugin_out_idx ] ], plugin_out_idx );
  } else {
    ctx->plugin_out = (fd_bam_out_ctx_t){ .idx    = ULONG_MAX };
  }

  // for full firedancer, not frankendancer
  ulong gossip_out_idx = fd_topo_find_tile_out_link( topo, tile, "bam_gossip", tile->kind_id );
  if( gossip_out_idx != ULONG_MAX ) {
    ctx->gossip_out = bam_out_link( topo, &topo->links[ tile->out_link_id[ gossip_out_idx ] ], gossip_out_idx );
  } else {
    ctx->gossip_out = (fd_bam_out_ctx_t){ .idx    = ULONG_MAX };
  }

  /* Set socket receive buffer size */
  ulong so_rcvbuf = tile->bam.buf_sz;
  if( FD_UNLIKELY( so_rcvbuf < 2048UL  ) ) FD_LOG_ERR(( "Invalid [development.bam.buffer_size_kib]: too small" ));
  if( FD_UNLIKELY( so_rcvbuf > INT_MAX ) ) FD_LOG_ERR(( "Invalid [development.bam.buffer_size_kib]: too large" ));
  ctx->so_rcvbuf = (int)so_rcvbuf;

  /* Set idle ping timer */
  ctx->keepalive_interval = (long)tile->bam.keepalive_interval_nanos;

  ctx->bam_status_recent = ctx->enabled
      ? FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED
      : FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISABLED;
  ctx->bam_status_plugin = ctx->bam_status_recent;
  ctx->bam_status_counted = ctx->bam_status_recent;
  ctx->bam_status_logged = ctx->bam_status_recent;
  ctx->bam_status_history_next = 0UL;
  ctx->bam_status_history_cnt  = 0UL;
  ctx->bam_status_history[ 0 ] = (fd_bam_status_history_t){ .ts_ns = fd_bam_now(), .status = ctx->bam_status_recent };
  ctx->bam_status_history_next = 1UL & ( FD_BAM_STATUS_HISTORY_CNT - 1UL );
  ctx->bam_status_history_cnt  = 1UL;
  ctx->last_bam_status_log_nanos = fd_log_wallclock();
  ctx->gui_dirty = 1U;

  ctx->bam_tpu         = (fd_ip4_port_t){0};
  ctx->bam_tpu_fwd     = (fd_ip4_port_t){0};
  ctx->default_tpu     = (fd_ip4_port_t){0};
  ctx->default_tpu_fwd = (fd_ip4_port_t){0};
  ctx->tpu_update_state = FD_BAM_TPU_UPDATE_STATE_UNKNOWN;

  ulong bam_status_obj_id = fd_pod_query_ulong( topo->props, "bam_status", ULONG_MAX );
  if( FD_UNLIKELY( bam_status_obj_id == ULONG_MAX ) ) FD_LOG_ERR(( "Missing bam_status object" ));
  ctx->bam_status_fseq = fd_fseq_join( fd_topo_obj_laddr( topo, bam_status_obj_id ) );
  if( FD_UNLIKELY( !ctx->bam_status_fseq ) ) FD_LOG_ERR(( "bam tile missing bam_status fseq" ));
  /* Start disconnected so a late BAM connect transitions the flag to 1
     and wakes up peers waiting for the override. */
  fd_fseq_update( ctx->bam_status_fseq, 0UL );

  fd_bam_tile_parse_endpoint( ctx, tile );

  ctx->grpc_client = fd_grpc_client_new( ctx->grpc_client_mem, &fd_bam_client_grpc_callbacks, ctx->grpc_metrics, ctx, ctx->grpc_buf_max, ctx->map_seed );
  if( FD_UNLIKELY( !ctx->grpc_client ) ) {
    FD_LOG_CRIT(( "fd_grpc_client_new failed" )); /* unreachable */
  }
  fd_grpc_client_set_version( ctx->grpc_client, fdctl_version_string, strlen( fdctl_version_string ) );
  fd_grpc_client_set_authority( ctx->grpc_client, ctx->server_sni, ctx->server_sni_len, ctx->server_tcp_port );

  for( ulong i=0UL; i<FD_BAM_SLOT_INGRESS_TIMING_CNT; i++ )
    fd_memset( &ctx->slot_ingress_timing[ i ], 0, sizeof(ctx->slot_ingress_timing[ i ]) );
  ctx->unresolved_slot_ingress = (fd_bam_unresolved_slot_ingress_timing_t){0};

  fd_histf_new( ctx->metrics.builder_heartbeat_arrival_delta_nanos,
                FD_MHIST_MIN( BAM, BUILDER_HEARTBEAT_ARRIVAL_DELTA_NANOS ),
                FD_MHIST_MAX( BAM, BUILDER_HEARTBEAT_ARRIVAL_DELTA_NANOS ) );
  fd_histf_new( ctx->metrics.scheduler_pong_enqueue_nanos,
      FD_MHIST_MIN( BAM, SCHEDULER_PONG_ENQUEUE_NANOS ),
      FD_MHIST_MAX( BAM, SCHEDULER_PONG_ENQUEUE_NANOS ) );
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
#define STEM_CALLBACK_AFTER_FRAG          bam_after_frag

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
