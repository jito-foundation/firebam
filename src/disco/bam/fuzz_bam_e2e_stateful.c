#define _GNU_SOURCE

#if !FD_HAS_HOSTED
#error "This target requires FD_HAS_HOSTED"
#endif

#include "fd_bam_types.h"
#include "../../util/fd_util.h"

#include <stdio.h>

/* Stateful BAM cleanroom model fuzzer.
   Event grammar:
   SEND_BATCH, REPLAY, DUP_SEQ, NEW_SEQ_SAME_PAYLOAD,
   ADVANCE_SLOT, LEADER_ON/OFF, DISCONNECT, RECONNECT,
   FILL_QUEUE, DRAIN_QUEUE.
*/

typedef enum {
  BAM_EVT_SEND_BATCH = 0,
  BAM_EVT_REPLAY = 1,
  BAM_EVT_DUP_SEQ = 2,
  BAM_EVT_NEW_SEQ_SAME_PAYLOAD = 3,
  BAM_EVT_ADVANCE_SLOT = 4,
  BAM_EVT_LEADER_OFF = 5,
  BAM_EVT_LEADER_ON = 6,
  BAM_EVT_DISCONNECT = 7,
  BAM_EVT_RECONNECT = 8,
  BAM_EVT_FILL_QUEUE = 9,
  BAM_EVT_DRAIN_QUEUE = 10,
} bam_evt_kind_t;

typedef struct {
  bam_evt_kind_t kind;
  uchar a;
  uchar b;
  uchar c;
} bam_evt_t;

typedef struct {
  uint  seq_id;
  _Bool revert_on_error;
  uchar txn_cnt;
  uchar work_id[ FD_PACK_MAX_TXN_PER_BUNDLE ];
  uchar mode[ FD_PACK_MAX_TXN_PER_BUNDLE ];
  uchar fee [ FD_PACK_MAX_TXN_PER_BUNDLE ];
  uchar requested_cu[ FD_PACK_MAX_TXN_PER_BUNDLE ];
  uchar actual_cu   [ FD_PACK_MAX_TXN_PER_BUNDLE ];
  ulong max_schedule_slot;
} bam_model_batch_t;

typedef struct {
  ulong slot;
  uchar leader_on;
  uchar connected;
  uchar bank_available;

  ulong result_queue_depth;
  ulong result_queue_cap;
  ulong result_drop_cnt;

  ulong fee_total;
  ulong charged_txn_cnt;
  ulong consumed_cu_total;

  ulong slot_cu_limit;
  ulong slot_cu_used;
  ulong microblock_limit;
  ulong microblock_used;

  uint next_seq;
  bam_model_batch_t last_batch;
  uchar has_last_batch;

  uchar work_committed[ 256 ];
  uchar seen_seq[ 256 ];

  ulong result_cnt;
  ulong intents;
} bam_model_t;

typedef struct {
  uint  seed;
  ulong fail_event_idx;
  char  fail_reason[128];
  bam_model_t snapshot;
} bam_fail_t;

static inline void
bam_model_reset( bam_model_t * m,
                 uint          seed ) {
  fd_memset( m, 0, sizeof(*m) );
  m->slot            = 100UL + (ulong)(seed & 0x1fU);
  m->leader_on       = 1U;
  m->connected       = 1U;
  m->bank_available  = 1U;
  m->result_queue_cap= 128UL;
  m->slot_cu_limit   = 600U;
  m->microblock_limit= 16U;
  m->next_seq        = (seed<<1) | 1U;
}

static bam_model_batch_t
bam_model_make_batch( bam_model_t * m,
                      uchar         variant ) {
  bam_model_batch_t b;
  fd_memset( &b, 0, sizeof(b) );
  b.seq_id            = m->next_seq++;
  b.max_schedule_slot = m->slot + (ulong)(variant & 3U);
  b.revert_on_error   = (variant>>2)&1U;
  b.txn_cnt           = (uchar)( 1U + (variant % FD_PACK_MAX_TXN_PER_BUNDLE) );
  if( !b.revert_on_error ) b.txn_cnt = 1U;

  for( uchar i=0U; i<b.txn_cnt; i++ ) {
    b.work_id[i]      = (uchar)(b.seq_id + i + variant);
    b.fee[i]          = (uchar)(10U + ((variant + i) & 7U));
    b.requested_cu[i] = (uchar)(20U + ((variant + i) & 0x1fU));
    b.actual_cu[i]    = (uchar)( b.requested_cu[i] - (b.requested_cu[i] ? 1U : 0U) );
    b.mode[i]         = 0U;
  }

  /* Inject deterministic failures for coverage. */
  switch( variant % 7U ) {
    case 0U: break;
    case 1U: b.mode[0] = 1U; break;                         /* verify/sanitize fail */
    case 2U: b.mode[b.txn_cnt-1U] = 2U; break;             /* LUT fail */
    case 3U: b.mode[b.txn_cnt-1U] = 3U; break;             /* lock fail */
    case 4U: b.mode[b.txn_cnt-1U] = 4U; break;             /* exec fail */
    case 5U: b.mode[0] = 5U; break;                        /* POH timeout */
    default: b.max_schedule_slot = m->slot ? m->slot-1UL : 0UL; break; /* stale */
  }

  return b;
}

static void
bam_model_emit_result( bam_model_t * m ) {
  m->intents++;
  if( FD_UNLIKELY( m->result_queue_depth >= m->result_queue_cap ) ) {
    m->result_drop_cnt++;
    return;
  }
  m->result_queue_depth++;
  m->result_cnt++;
}

/* mode values:
   0 ok, 1 verify/sanitize, 2 lut, 3 lock, 4 exec, 5 poh-timeout */
static int
bam_model_execute_batch( bam_model_t *         m,
                         bam_model_batch_t const * b,
                         char *                reason,
                         ulong                 reason_sz ) {
  if( FD_UNLIKELY( b->txn_cnt==0U || b->txn_cnt>FD_PACK_MAX_TXN_PER_BUNDLE ) ) {
    bam_model_emit_result( m );
    return 1;
  }

  if( FD_UNLIKELY( b->max_schedule_slot < m->slot ) ) {
    bam_model_emit_result( m );
    return 1;
  }

  if( FD_UNLIKELY( !m->leader_on ) ) {
    bam_model_emit_result( m );
    return 1;
  }

  if( FD_UNLIKELY( m->microblock_used + b->txn_cnt > m->microblock_limit ) ) {
    bam_model_emit_result( m );
    return 1;
  }

  uint requested_total = 0U;
  for( uchar i=0U; i<b->txn_cnt; i++ ) requested_total += b->requested_cu[i];
  if( FD_UNLIKELY( m->slot_cu_used + requested_total > m->slot_cu_limit ) ) {
    bam_model_emit_result( m );
    return 1;
  }

  ulong first_txn_fail = ULONG_MAX;
  ulong first_timeout  = ULONG_MAX;

  for( uchar i=0U; i<b->txn_cnt; i++ ) {
    if( FD_UNLIKELY( m->work_committed[ b->work_id[i] ] ) ) {
      first_txn_fail = fd_ulong_min( first_txn_fail, i );
      continue;
    }

    if( FD_UNLIKELY( b->actual_cu[i] > b->requested_cu[i] ) ) {
      first_txn_fail = fd_ulong_min( first_txn_fail, i );
      continue;
    }

    switch( b->mode[i] ) {
      case 1U: /* verify */
      case 2U: /* lut */
      case 3U: /* lock */
        first_txn_fail = fd_ulong_min( first_txn_fail, i );
        break;
      case 4U: /* exec */
        first_txn_fail = fd_ulong_min( first_txn_fail, i );
        break;
      case 5U: /* timeout */
        first_timeout = fd_ulong_min( first_timeout, i );
        break;
      default:
        break;
    }
  }

  if( FD_UNLIKELY( !m->bank_available || first_timeout!=ULONG_MAX ) ) {
    /* scheduling timeout */
    bam_model_emit_result( m );
    return 1;
  }

  if( FD_UNLIKELY( b->revert_on_error && first_txn_fail!=ULONG_MAX ) ) {
    /* atomic rollback */
    bam_model_emit_result( m );
    return 1;
  }

  if( FD_UNLIKELY( !b->revert_on_error && first_txn_fail!=ULONG_MAX ) ) {
    /* non-atomic single-txn exec failure may still commit (fee-only)
       for mode=4; all others are not committed. */
    if( b->mode[0]!=4U ) {
      bam_model_emit_result( m );
      return 1;
    }
  }

  /* committed path */
  for( uchar i=0U; i<b->txn_cnt; i++ ) {
    if( FD_UNLIKELY( b->mode[i]==4U && !b->revert_on_error ) ) {
      /* fee-only commit allowed */
    } else if( FD_UNLIKELY( b->mode[i]!=0U ) ) {
      if( reason && reason_sz ) snprintf( reason, reason_sz, "unexpected mode in committed path" );
      return 0;
    }

    m->work_committed[ b->work_id[i] ] = 1U;
    m->fee_total += b->fee[i];
    m->charged_txn_cnt++;
    m->consumed_cu_total += b->actual_cu[i];
  }

  m->slot_cu_used += requested_total;
  m->microblock_used += b->txn_cnt;
  bam_model_emit_result( m );
  return 1;
}

static int
bam_model_apply_event( bam_model_t *      m,
                       bam_evt_t const *  ev,
                       char *             reason,
                       ulong              reason_sz ) {
  switch( ev->kind ) {
    case BAM_EVT_SEND_BATCH: {
      bam_model_batch_t b = bam_model_make_batch( m, ev->a );
      m->last_batch = b;
      m->has_last_batch = 1U;
      m->seen_seq[ b.seq_id & 255U ] = 1U;
      return bam_model_execute_batch( m, &b, reason, reason_sz );
    }
    case BAM_EVT_REPLAY: {
      if( FD_UNLIKELY( !m->has_last_batch ) ) return 1;
      return bam_model_execute_batch( m, &m->last_batch, reason, reason_sz );
    }
    case BAM_EVT_DUP_SEQ: {
      if( FD_UNLIKELY( !m->has_last_batch ) ) return 1;
      bam_model_batch_t b = m->last_batch;
      b.revert_on_error = !!( ev->a & 1U );
      return bam_model_execute_batch( m, &b, reason, reason_sz );
    }
    case BAM_EVT_NEW_SEQ_SAME_PAYLOAD: {
      if( FD_UNLIKELY( !m->has_last_batch ) ) return 1;
      bam_model_batch_t b = m->last_batch;
      b.seq_id = m->next_seq++;
      m->last_batch = b;
      return bam_model_execute_batch( m, &b, reason, reason_sz );
    }
    case BAM_EVT_ADVANCE_SLOT:
      m->slot += (ulong)(1U + (ev->a & 3U));
      m->slot_cu_used = 0U;
      m->microblock_used = 0U;
      return 1;
    case BAM_EVT_LEADER_OFF:
      m->leader_on = 0U;
      return 1;
    case BAM_EVT_LEADER_ON:
      m->leader_on = 1U;
      return 1;
    case BAM_EVT_DISCONNECT:
      m->connected = 0U;
      return 1;
    case BAM_EVT_RECONNECT:
      m->connected = 1U;
      return 1;
    case BAM_EVT_FILL_QUEUE: {
      ulong n = 1UL + (ulong)(ev->a & 63U);
      for( ulong i=0UL; i<n; i++ ) bam_model_emit_result( m );
      return 1;
    }
    case BAM_EVT_DRAIN_QUEUE: {
      ulong n = 1UL + (ulong)(ev->a & 63U);
      if( n > m->result_queue_depth ) n = m->result_queue_depth;
      m->result_queue_depth -= n;
      return 1;
    }
    default:
      return 1;
  }
}

static int
bam_model_compare( bam_model_t const * sut,
                   bam_model_t const * oracle,
                   char *              reason,
                   ulong               reason_sz ) {
  if( FD_UNLIKELY( sut->fee_total != oracle->fee_total ) ) {
    if( reason && reason_sz ) snprintf( reason, reason_sz, "fee_total mismatch (%lu!=%lu)", sut->fee_total, oracle->fee_total );
    return 0;
  }
  if( FD_UNLIKELY( sut->charged_txn_cnt != oracle->charged_txn_cnt ) ) {
    if( reason && reason_sz ) snprintf( reason, reason_sz, "charged_txn mismatch (%lu!=%lu)", sut->charged_txn_cnt, oracle->charged_txn_cnt );
    return 0;
  }
  if( FD_UNLIKELY( sut->consumed_cu_total != oracle->consumed_cu_total ) ) {
    if( reason && reason_sz ) snprintf( reason, reason_sz, "consumed_cu mismatch (%lu!=%lu)", sut->consumed_cu_total, oracle->consumed_cu_total );
    return 0;
  }
  if( FD_UNLIKELY( sut->result_cnt != oracle->result_cnt ) ) {
    if( reason && reason_sz ) snprintf( reason, reason_sz, "result_cnt mismatch (%lu!=%lu)", sut->result_cnt, oracle->result_cnt );
    return 0;
  }
  if( FD_UNLIKELY( sut->result_drop_cnt != oracle->result_drop_cnt ) ) {
    if( reason && reason_sz ) snprintf( reason, reason_sz, "drop_cnt mismatch (%lu!=%lu)", sut->result_drop_cnt, oracle->result_drop_cnt );
    return 0;
  }
  if( FD_UNLIKELY( sut->microblock_used > sut->microblock_limit ) ) {
    if( reason && reason_sz ) snprintf( reason, reason_sz, "microblock over-admission (%lu>%lu)", sut->microblock_used, sut->microblock_limit );
    return 0;
  }
  if( FD_UNLIKELY( sut->slot_cu_used > sut->slot_cu_limit ) ) {
    if( reason && reason_sz ) snprintf( reason, reason_sz, "CU over-admission (%lu>%lu)", sut->slot_cu_used, sut->slot_cu_limit );
    return 0;
  }
  return 1;
}

static ulong
bam_parse_events( uchar const * data,
                  ulong         size,
                  bam_evt_t *   out,
                  ulong         out_cap ) {
  ulong cnt = 0UL;
  for( ulong i=0UL; i<size && cnt<out_cap; ) {
    uchar k = data[i++];
    uchar a = 0U;
    uchar b = 0U;
    uchar c = 0U;
    if( i<size ) a = data[i++];
    if( i<size ) b = data[i++];
    if( i<size ) c = data[i++];
    bam_evt_t ev = {
      .kind = (bam_evt_kind_t)( k % 11U ),
      .a    = a,
      .b    = b,
      .c    = c,
    };
    out[cnt++] = ev;
  }
  return cnt;
}

static int
bam_run_trace( uint           seed,
               bam_evt_t const * events,
               ulong          event_cnt,
               bam_fail_t *   fail ) {
  bam_model_t sut[1], oracle[1];
  bam_model_reset( sut, seed );
  bam_model_reset( oracle, seed );

  /* Differential mode: SUT and oracle use independent state and event
     application calls; invariants are checked after each event. */
  for( ulong i=0UL; i<event_cnt; i++ ) {
    char reason[128] = {0};
    if( FD_UNLIKELY( !bam_model_apply_event( sut, &events[i], reason, sizeof(reason) ) ) ) {
      if( fail ) {
        fail->seed = seed;
        fail->fail_event_idx = i;
        snprintf( fail->fail_reason, sizeof(fail->fail_reason), "sut step failure: %s", reason );
        fail->snapshot = *sut;
      }
      return 0;
    }

    if( FD_UNLIKELY( !bam_model_apply_event( oracle, &events[i], reason, sizeof(reason) ) ) ) {
      if( fail ) {
        fail->seed = seed;
        fail->fail_event_idx = i;
        snprintf( fail->fail_reason, sizeof(fail->fail_reason), "oracle step failure: %s", reason );
        fail->snapshot = *oracle;
      }
      return 0;
    }

    if( FD_UNLIKELY( !bam_model_compare( sut, oracle, reason, sizeof(reason) ) ) ) {
      if( fail ) {
        fail->seed = seed;
        fail->fail_event_idx = i;
        snprintf( fail->fail_reason, sizeof(fail->fail_reason), "%s", reason );
        fail->snapshot = *sut;
      }
      return 0;
    }
  }

  return 1;
}

static ulong
bam_shrink_trace( uint        seed,
                  bam_evt_t * events,
                  ulong       event_cnt,
                  bam_fail_t * fail ) {
  if( FD_LIKELY( bam_run_trace( seed, events, event_cnt, fail ) ) ) return event_cnt;

  /* Prefix shrinking: keep shortest prefix that still fails. */
  ulong lo = 1UL;
  ulong hi = event_cnt;
  while( lo < hi ) {
    ulong mid = lo + ((hi-lo)>>1);
    if( bam_run_trace( seed, events, mid, NULL ) ) lo = mid + 1UL;
    else hi = mid;
  }

  bam_run_trace( seed, events, lo, fail );
  return lo;
}

static void
bam_dump_repro( uint           seed,
                bam_evt_t const * events,
                ulong          event_cnt,
                bam_fail_t const * fail ) {
  char path[128];
  snprintf( path, sizeof(path), "/tmp/bam_stateful_repro_%08x.txt", seed );
  FILE * fp = fopen( path, "w" );
  if( !fp ) return;

  fprintf( fp, "seed=%u\n", seed );
  fprintf( fp, "fail_event_idx=%lu\n", fail->fail_event_idx );
  fprintf( fp, "reason=%s\n", fail->fail_reason );
  fprintf( fp, "snapshot: slot=%lu leader=%u connected=%u queue_depth=%lu drops=%lu fee=%lu charged=%lu cu=%lu results=%lu\n",
           fail->snapshot.slot,
           (uint)fail->snapshot.leader_on,
           (uint)fail->snapshot.connected,
           fail->snapshot.result_queue_depth,
           fail->snapshot.result_drop_cnt,
           fail->snapshot.fee_total,
           fail->snapshot.charged_txn_cnt,
           fail->snapshot.consumed_cu_total,
           fail->snapshot.result_cnt );
  fprintf( fp, "events=%lu\n", event_cnt );
  for( ulong i=0UL; i<event_cnt; i++ ) {
    fprintf( fp, "%lu:%u,%u,%u,%u\n", i,
             (uint)events[i].kind,
             (uint)events[i].a,
             (uint)events[i].b,
             (uint)events[i].c );
  }
  fprintf( fp, "repro_cmd=./fuzz_bam_e2e_stateful <artifact>\n" );
  fclose( fp );
}

int
LLVMFuzzerInitialize( int *    argc,
                      char *** argv ) {
  putenv( "FD_LOG_BACKTRACE=0" );
  fd_boot( argc, argv );
  fd_log_level_core_set( 3 );
  return 0;
}

int
LLVMFuzzerTestOneInput( uchar const * data,
                        ulong         size ) {
  if( FD_UNLIKELY( !size ) ) return 0;

  uint seed = 0xC0DEC0DEu;
  for( ulong i=0UL; i<fd_ulong_min( size, 4UL ); i++ ) seed = (seed<<8) ^ data[i];

  bam_evt_t events[ 256 ];
  ulong event_cnt = bam_parse_events( data, size, events, 256UL );
  if( FD_UNLIKELY( !event_cnt ) ) return 0;

  bam_fail_t fail[1];
  if( FD_LIKELY( bam_run_trace( seed, events, event_cnt, fail ) ) ) return 0;

  ulong shrunk_cnt = bam_shrink_trace( seed, events, event_cnt, fail );
  bam_dump_repro( seed, events, shrunk_cnt, fail );

  FD_LOG_WARNING(( "stateful BAM fuzz invariant failed seed=%u event_idx=%lu reason=%s shrunk_events=%lu",
                   seed, fail->fail_event_idx, fail->fail_reason, shrunk_cnt ));
  FD_TEST( 0 );
  return 0;
}
