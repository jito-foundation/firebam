#include "../tiles.h"

#include "generated/fd_pack_tile_seccomp.h"

#include "../../util/pod/fd_pod_format.h"
#include "../../discof/replay/fd_replay_tile.h" // layering violation
#include "../fd_txn_m.h"
#include "../keyguard/fd_keyload.h"
#include "../keyguard/fd_keyswitch.h"
#include "../keyguard/fd_keyguard.h"
#include "../metrics/fd_metrics.h"
#include "../bam/fd_bam_types.h"
#include "../pack/fd_pack.h"
#include "../pack/fd_pack_cost.h"
#include "../pack/fd_pack_pacing.h"
#include "../../ballet/base58/fd_base58.h"

/* fd_pack is responsible for taking verified transactions, and
   arranging them into "microblocks" (groups) of transactions to
   be executed serially.  It can try to do clever things so that
   multiple microblocks can execute in parallel, if they don't
   write to the same accounts. */

#define IN_KIND_RESOLV       (0UL)
#define IN_KIND_POH          (1UL)
#define IN_KIND_BANK         (2UL)
#define IN_KIND_SIGN         (3UL)
#define IN_KIND_REPLAY       (4UL)
#define IN_KIND_EXECUTED_TXN (5UL)

/* Pace microblocks, but only slightly.  This helps keep performance
   more stable.  This limit is 2,000 microblocks/second/bank.  At 31
   transactions/microblock, that's 62k txn/sec/bank. */
#define MICROBLOCK_DURATION_NS  (0L)

/* There are 151 accepted blockhashes, but those don't include skips.
   This check is neither precise nor accurate, but just good enough.
   The bank tile does the final check.  We give a little margin for a
   few percent skip rate. */
#define TRANSACTION_LIFETIME_SLOTS 160UL

/* Time is normally a long, but pack expects a ulong.  Add -LONG_MIN to
   the time values so that LONG_MIN maps to 0, LONG_MAX maps to
   ULONG_MAX, and everything in between maps linearly with a slope of 1.
   Just subtracting LONG_MIN results in signed integer overflow, which
   is U.B. */
#define TIME_OFFSET 0x8000000000000000UL
FD_STATIC_ASSERT( (ulong)LONG_MIN+TIME_OFFSET==0UL,       time_offset );
FD_STATIC_ASSERT( (ulong)LONG_MAX+TIME_OFFSET==ULONG_MAX, time_offset );

/* 1.6 M cost units, enough for 1 max size transaction */
const ulong CUS_PER_MICROBLOCK = 1600000UL;

#define SMALL_MICROBLOCKS 1

#if SMALL_MICROBLOCKS
const float VOTE_FRACTION = 1.0f; /* schedule all available votes first */
#define EFFECTIVE_TXN_PER_MICROBLOCK 1UL
#else
const float VOTE_FRACTION = 0.75f; /* TODO: Is this the right value? */
#define EFFECTIVE_TXN_PER_MICROBLOCK MAX_TXN_PER_MICROBLOCK
#endif

/* There's overhead associated with each microblock the bank tile tries
   to execute it, so the optimal strategy is not to produce a microblock
   with a single transaction as soon as we receive it.  Basically, if we
   have less than 31 transactions, we want to wait a little to see if we
   receive additional transactions before we schedule a microblock.  We
   can model the optimum amount of time to wait, but the equation is
   complicated enough that we want to compute it before compile time.
   wait_duration[i] for i in [0, 31] gives the time in nanoseconds pack
   should wait after receiving its most recent transaction before
   scheduling if it has i transactions available.  Unsurprisingly,
   wait_duration[31] is 0.  wait_duration[0] is ULONG_MAX, so we'll
   always wait if we have 0 transactions. */
FD_IMPORT( wait_duration, "src/disco/pack/pack_delay.bin", ulong, 6, "" );



#if FD_PACK_USE_EXTRA_STORAGE
/* When we are done being leader for a slot and we are leader in the
   very next slot, it can still take some time to transition.  This is
   because the bank has to be finalized, a hash calculated, and various
   other things done in the replay stage to create the new child bank.

   During that time, pack cannot send transactions to banks so it needs
   to be able to buffer.  Typically, these so called "leader
   transitions" are short (<15 millis), so a low value here would
   suffice.  However, in some cases when there is memory pressure on the
   NUMA node or when the operating system context switches relevant
   threads out, it can take significantly longer.

   To prevent drops in these cases and because we assume banks are fast
   enough to drain this buffer once we do become leader, we set this
   buffer size to be quite large. */

struct fd_pack_ctx {
  fd_txn_e_t txn;
  ulong      bam_max_schedule_slot; /* Sidecar for extra-storage BAM txns; fd_txn_p_t does not carry this hint. */
  ulong      bam_funnel_slot;       /* Slot bucket assigned when the BAM txn first reached pack ingress. */
} fd_pack_extra_txn_t;

#define DEQUE_NAME extra_txn_deq
#define DEQUE_T    fd_pack_extra_txn_t
#define DEQUE_MAX  (128UL*1024UL)
#include "../../../../util/tmpl/fd_deque.c"

#endif

/* Sync with src/app/shared/fd_config.c */
#define FD_PACK_STRATEGY_PERF     0
#define FD_PACK_STRATEGY_BALANCED 1
#define FD_PACK_STRATEGY_BUNDLE   2

static char const * const schedule_strategy_strings[3] = { "PRF", "BAL", "BUN" };

#define FD_PACK_BAM_RECENT_SLOT_CNT 32UL

typedef enum {
  PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_NEW_SEQ_BEFORE_COMPLETE = 0, /* dropped when a newer seq arrives before current BAM bundle is complete */
  PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_LEADER_SLOT_END,             /* dropped at normal leader-slot end */
  PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_POH_TIMEOUT,                 /* dropped when PoH wallclock deadline ends the leader slot */
} pack_tile_bam_bundle_assembly_abandon_reason_t;

typedef enum {
  PACK_TILE_BAM_INVALID_NONE = 0,          /* Work remains valid under the local slot and blockhash view. */
  PACK_TILE_BAM_INVALID_OUTSIDE_SLOT,      /* max_schedule_slot trails the slot required to execute this work. */
  PACK_TILE_BAM_INVALID_BLOCKHASH_EXPIRED, /* blockhash_slot fell outside the recent-blockhash lifetime. */
} pack_tile_bam_invalid_reason_t;

typedef enum {
  PACK_BAM_WORK_STATE_PENDING   = 0,
  PACK_BAM_WORK_STATE_SCHEDULED = 1,
} pack_bam_work_state_t;

typedef struct fd_pack_ctx fd_pack_ctx_t;

static inline void
pack_tile_record_bam_invalid_reason_count( ulong *                        counts,
                                           pack_tile_bam_invalid_reason_t reason,
                                           uchar                          txn_cnt ) {
  FD_TEST( reason==PACK_TILE_BAM_INVALID_OUTSIDE_SLOT || reason==PACK_TILE_BAM_INVALID_BLOCKHASH_EXPIRED );
  ulong base = ( txn_cnt==1U ) ? 0UL : 2UL;
  counts[ base + (ulong)reason - 1UL ]++;
}

static inline void
pack_tile_note_bam_work_stage( fd_pack_ctx_t * ctx,
                               ulong           stage_idx,
                               ulong           item_cnt,
                               ulong           txn_cnt );


typedef struct {
  fd_acct_addr_t commission_pubkey[1];
  ulong          commission;
} block_builder_info_t;

typedef struct {
  fd_ed25519_sig_t sig[ FD_PACK_MAX_TXN_PER_BUNDLE ];
  long             first_rx_ts_ns;
  ulong            slot;
  ulong            max_schedule_slot;
  ulong            blockhash_slot;
  uint             seq_id;
  uchar            txn_cnt;
  uchar            remaining_txn_cnt;
  uchar            state;
} pack_bam_work_t;

typedef struct {
  ulong slot;
  ulong received_items;
  ulong received_txns;
  ulong accepted_items;
  ulong accepted_txns;
  ulong rejected_pre_pending_items;
  ulong rejected_pre_pending_txns;
  ulong pending_items;
  ulong pending_txns;
  ulong evicted_post_pending_items;
  ulong evicted_post_pending_txns;
  ulong scheduled_items;
  ulong scheduled_txns;
  ulong landed_items;
  ulong landed_txns;
  uint  first_debug_seq_id; /* UINT_MAX means unset; otherwise the seq_id of the first BAM batch in this slot whose downstream drop logs should print under dump_bam_slot_first_txn. */
} pack_bam_recent_slot_t;

typedef struct {
  ulong received_items;
  ulong received_txns;
  ulong accepted_items;
  ulong accepted_txns;
  ulong rejected_pre_pending_items;
  ulong rejected_pre_pending_txns;
  ulong pending_items;
  ulong pending_txns;
  ulong evicted_post_pending_items;
  ulong evicted_post_pending_txns;
  ulong scheduled_items;
  ulong scheduled_txns;
  ulong landed_items;
  ulong landed_txns;
} pack_bam_unresolved_work_t;

typedef struct {
  long time;
  ulong sched_results[ FD_METRICS_ENUM_PACK_TXN_SCHEDULE_CNT ];
} fd_pack_sched_results_snap_t;

typedef struct {
  fd_wksp_t * mem;
  ulong       chunk0;
  ulong       wmark;
} fd_pack_in_ctx_t;

typedef struct {
  ulong       idx;
  fd_wksp_t * mem;
  ulong       chunk0;
  ulong       wmark;
  ulong       chunk;
} pack_bam_out_ctx_t;
/* pack_bam_out_ctx_t tracks publication state for one pack->bam link. */

struct fd_pack_ctx {
  fd_pack_t *  pack;
  fd_txn_e_t * cur_spot;
  uchar        bundle_kind; /* PACK_TILE_BUNDLE_KIND_* (none/BE/BAM) */
  uchar        dump_bam_mode;
  ulong        bam_txn_max_schedule_slot; /* Current single-txn BAM slot hint; carried here because fd_txn_p_t.bam has no max_schedule_slot. */

  uchar executed_txn_sig[ 64UL ];

  /* One of the FD_PACK_STRATEGY_* values defined above */
  int      strategy;

  /* The value passed to fd_pack_new, etc. */
  ulong    max_pending_transactions;

  /* The leader slot we are currently packing for, or ULONG_MAX if we
     are not the leader. */
  ulong  leader_slot;
  void const * leader_bank;
  ulong        leader_bank_idx;

  fd_became_leader_t _became_leader[1];

  /* The number of microblocks we have packed for the current leader
     slot.  Will always be <= slot_max_microblocks.  We must track
     this so that when we are done we can tell the PoH tile how many
     microblocks to expect in the slot. */
  ulong slot_microblock_cnt;

  /* Counter which increments when we've finished packing for a slot */
  uint pack_idx;

  ulong pack_txn_cnt; /* total num transactions packed since startup */

  /* The maximum number of microblocks that can be packed in this slot.
     Provided by the PoH tile when we become leader.*/
  ulong slot_max_microblocks;

  /* Cap (in bytes) of the amount of transaction data we produce in each
     block to avoid hitting the shred limits.  See where this is set for
     more explanation. */
  ulong slot_max_data;
  int   larger_shred_limits_per_block;

  /* Consensus critical slot cost limits. */
  struct {
    ulong slot_max_cost;
    ulong slot_max_vote_cost;
    ulong slot_max_write_cost_per_acct;
  } limits;

  /* If drain_banks is non-zero, then the pack tile must wait until all
     banks are idle before scheduling any more microblocks.  This is
     primarily helpful in irregular leader transitions, e.g. while being
     leader for slot N, we switch forks to a slot M (!=N+1) in which we
     are also leader.  We don't want to execute microblocks for
     different slots concurrently. */
  int drain_banks;

  /* Updated during housekeeping and used only for checking if the
     leader slot has ended.  Might be off by one housekeeping duration,
     but that should be small relative to a slot duration. */
  long  approx_wallclock_ns;

  /* approx_tickcount is updated in during_housekeeping() with
     fd_tickcount() and will match approx_wallclock_ns.  This is done
     because we need to include an accurate nanosecond timestamp in
     every fd_txn_p_t but don't want to have to call the expensive
     fd_log_wallclock() in in the critical path. We can use
     fd_tempo_tick_per_ns() to convert from ticks to nanoseconds over
     small periods of time. */
  long  approx_tickcount;

  fd_rng_t * rng;

  /* The end wallclock time of the leader slot we are currently packing
     for, if we are currently packing for a slot.*/
  long slot_end_ns;

  /* pacer and ticks_per_ns are used for pacing CUs through the slot,
     i.e. deciding when to schedule a microblock given the number of CUs
     that have been consumed so far.  pacer is an opaque pacing object,
     which is initialized when the pack tile is packing a slot.
     ticks_per_ns is the cached value from tempo. */
  fd_pack_pacing_t pacer[1];
  double           ticks_per_ns;

  /* last_successful_insert stores the tickcount of the last
     successful transaction insert. */
  long last_successful_insert;

  /* highest_observed_slot stores the highest slot number we've seen
     from any transaction coming from the resolv tile.  When this
     increases, we expire old transactions. */
  ulong highest_observed_slot;
  ulong bam_pending_check_slot;

  /* microblock_duration_ns, and wait_duration
     respectively scaled to be in ticks instead of nanoseconds */
  ulong microblock_duration_ticks;
  ulong wait_duration_ticks[ MAX_TXN_PER_MICROBLOCK+1UL ];

#if FD_PACK_USE_EXTRA_STORAGE
  /* In addition to the available transactions that pack knows about, we
     also store a larger ring buffer for handling cases when pack is
     full.  This is an fd_deque. */
  fd_pack_extra_txn_t * extra_txn_deq;
  int                  insert_to_extra; /* whether the last insert was into pack or the extra deq */
#endif

  fd_pack_in_ctx_t in[ 32 ];
  int              in_kind[ 32 ];

  ulong    bank_cnt;
  ulong    bank_idle_bitset; /* bit i is 1 if we've observed *bank_current[i]==bank_expect[i] */
  int      poll_cursor; /* in [0, bank_cnt), the next bank to poll */
  int      use_consumed_cus;
  long     skip_cnt;
  ulong *  bank_current[ FD_PACK_MAX_BANK_TILES ];
  ulong    bank_expect[ FD_PACK_MAX_BANK_TILES  ];
  /* bank_ready_at[x] means don't check bank x until tickcount is at
     least bank_ready_at[x]. */
  long     bank_ready_at[ FD_PACK_MAX_BANK_TILES  ];

  fd_wksp_t * bank_out_mem;
  ulong       bank_out_chunk0;
  ulong       bank_out_wmark;
  ulong       bank_out_chunk;

  fd_wksp_t * poh_out_mem;
  ulong       poh_out_chunk0;
  ulong       poh_out_wmark;
  ulong       poh_out_chunk;

  /* pack->bam outputs are split by semantic contract:
       - pack_bam_ldr carries fd_bam_leader_state_t snapshots. The BAM
         tile coalesces these latest-value-wins before sending upstream.
       - pack_bam_res carries fd_bam_bundle_result_t feedback. The BAM
         tile queues these durably FIFO across reconnect/reset.
     Keeping them separate removes the internal size-based mux. */
  pack_bam_out_ctx_t bam_leader_out;
  pack_bam_out_ctx_t bam_result_out;

  ulong      insert_result[ FD_PACK_INSERT_RETVAL_CNT ];
  ulong      bam_bundle_assembly_abandon_cnt[ FD_METRICS_ENUM_PACK_BAM_BUNDLE_ASSEMBLY_ABANDON_REASON_CNT ];
  ulong      bam_work_rejected_pre_pending_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_INVALID_REASON_CNT ];
  ulong      bam_pending_work_evicted_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_INVALID_REASON_CNT ];
  ulong      bam_work_item_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_CNT ];
  ulong      bam_work_transaction_stage_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_CNT ];
  ulong      bam_work_first_outcome_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_FIRST_OUTCOME_CNT ];
  ulong      bam_tracking_rejected_cnt;
  ulong      bam_tracking_rejected_txn_cnt;
  pack_bam_recent_slot_t bam_recent_slot[ FD_PACK_BAM_RECENT_SLOT_CNT ];
  pack_bam_unresolved_work_t bam_unresolved_work;
  uint       bam_current_slot_fresh;
  uchar      bam_first_insert_seen;
  uchar      bam_first_schedule_seen;
  long       bam_first_insert_minus_slot_end_ns;
  long       bam_first_schedule_minus_slot_end_ns;
  ulong      bam_first_insert_result_cnt[ FD_METRICS_ENUM_PACK_BAM_FIRST_EVENT_RESULT_CNT ];
  ulong      bam_first_schedule_result_cnt[ FD_METRICS_ENUM_PACK_BAM_FIRST_EVENT_RESULT_CNT ];
  fd_histf_t bam_first_insert_before_end_distance_nanos[ 1 ];
  fd_histf_t bam_first_insert_after_end_distance_nanos[ 1 ];
  fd_histf_t bam_first_schedule_before_end_distance_nanos[ 1 ];
  fd_histf_t bam_first_schedule_after_end_distance_nanos[ 1 ];
  fd_histf_t bam_work_rx_to_first_outcome_nanos[ 1 ];
  fd_histf_t schedule_duration[ 1 ];
  fd_histf_t no_sched_duration[ 1 ];
  fd_histf_t insert_duration  [ 1 ];
  fd_histf_t complete_duration[ 1 ];

  struct {
    uint metric_state;
    long metric_state_begin;
    long metric_timing[ 16 ];
  };

  /* last_sched_metrics is a snapshot of the schedule outcome counters
     during the last schedule which included at least one successful
     outcome. */
  fd_pack_sched_results_snap_t last_sched_metrics[ 1 ];

    /* last_sched_metrics is a snapshot of the schedule outcome counters
       at the last start-of-leader-block event. */
  fd_pack_sched_results_snap_t start_block_sched_metrics[ 1 ];

  struct {
    ulong id;
    ulong txn_cnt;
    ulong txn_received;
    ulong min_blockhash_slot;
    fd_txn_e_t * _txn[ FD_PACK_MAX_TXN_PER_BUNDLE ];
    fd_txn_e_t * const * bundle; /* points to _txn when non-NULL */
  } current_bundle[1];

  /* State for assembling a BAM AtomicTxnBatch with revert_on_error==true.

     BAM bundles arrive to pack as *individual* transactions (one frag per
     transaction) with BAM metadata in fd_txn_m_t.bam:
       - seq_id / batch_cnt / batch_idx identify the batch and position
       - max_schedule_slot is a scheduler-provided slot hint

     Pack must buffer until all indices [0,batch_cnt) have arrived, then
     insert the batch as a pack "bundle" so the bank tile executes it
     atomically (bundle semantics / revert-on-error behaviour).

     Important notes / edge cases:
       - seq_id==0 is valid, so |bundle!=NULL| is the only reliable
         "this accumulator is active" sentinel.
       - Transactions may arrive out of order (we index by batch_idx).
       - If a new seq_id arrives before the current batch completes,
         pack abandons the partial batch and reports missing indices as
         SIGNATURE_FAILURE (upstream sigverify/resolv likely dropped them).
       - max_schedule_slot is a concrete scheduler-provided slot limit.
         Pack drops the bundle if it becomes leader *after*
         max_schedule_slot. */
  struct {
    /* BAM AtomicTxnBatch.seq_id. Unique only within one leader rotation.
       Used for correlating bundle results back to the scheduler. */
    uint  seq_id;

    /* Expected number of transactions in this batch (batch_cnt).
       Range: [1,FD_PACK_MAX_TXN_PER_BUNDLE]. Constant for the lifetime
       of an active batch. */
    uchar txn_cnt;

    /* Number of unique batch indices received so far. Incremented only
       the first time each batch_idx is observed (see received[]).
       When txn_received==txn_cnt, the bundle is complete and can be
       inserted into fd_pack via fd_pack_insert_bundle_fini(). */
    uchar txn_received;

    /* Minimum reference_blockhash slot across all transactions received
       in this batch. This is derived from the incoming frag |sig| (the
       blockhash slot computed upstream by resolv).

       Used as the |expires_at| argument to fd_pack_insert_bundle_fini()
       so the bundle expires as soon as the "earliest" blockhash in the
       batch would expire. Initialised to ULONG_MAX when starting a new
       batch. */
    ulong min_blockhash_slot;

    /* BAM AtomicTxnBatch.max_schedule_slot. Pack enforces that the
       bundle only schedules in slots <= this value. */
    ulong max_schedule_slot;

    /* Slot bucket used for recent-slot BAM funnel accounting. This is
       derived when the first transaction for the bundle reaches pack. */
    ulong slot;

    /* Per-index receipt tracker (size is the max bundle size, but only
       indices [0,txn_cnt) are meaningful). A value of 1 means we have
       received and populated _txn[ i ] for that batch_idx. */
    uchar received[ FD_PACK_MAX_TXN_PER_BUNDLE ];

    /* Backing storage for fd_pack_insert_bundle_init(). Indices correspond
       to batch_idx (not arrival order). The array itself is inline and
       never NULL; it is zeroed at init, then _init populates [0,txn_cnt)
       while bundle!=NULL. After _fini/_cancel entries are stale and must
       not be used (they are not cleared). */
    fd_txn_e_t * _txn[ FD_PACK_MAX_TXN_PER_BUNDLE ];

    /* Pointer returned by fd_pack_insert_bundle_init(). When non-NULL,
       points to _txn and indicates this accumulator currently owns txn_cnt
       pool entries. Must be cancelled with fd_pack_insert_bundle_cancel()
       on abandon or after insertion failures. */
    fd_txn_e_t * const * bundle;
  } current_bam_bundle[1];

  /* Scratch bundle metadata passed into fd_pack_insert_bundle_fini().
     Stored by fd_pack alongside the bundle head txn and later consumed
     by the bundle crank logic.

     pack fills this for:
       - Block engine bundles (builder commission from txnm->block_engine)
       - BAM bundles (builder info best-effort derived from bam_fee_cfg)

     Defaults: overwritten before each bundle insertion; contents are only
     meaningful for the immediate fd_pack_insert_bundle_fini() call that
     consumes it. */
  block_builder_info_t bundle_meta[1];

  /* Optional shared-memory BAM fee configuration (populated by the BAM
     tile via gRPC config). When present and has_prio_fee_recipient!=0,
     pack uses it to populate bundle_meta for BAM bundles so the
     bundle crank can direct priority-fee commission correctly.

     Defaults / edge cases:
       - NULL when BAM is disabled or the topology does not provide the
         bam_fee_cfg object.
       - Values may update asynchronously; reads are treated best-effort. */
  fd_bam_fee_cfg_t const * bam_fee_cfg;

  /* Last leader-state message published to pack_bam_ldr. Used to avoid spamming
     identical updates.

     Sentinel: slot==ULONG_MAX means "no cached publish yet", which is safe
     because leader-state publishes only happen while leader_slot is live. */
  fd_bam_leader_state_t last_bam_leader_state;

  struct {
    int                   enabled;
    int                   ib_inserted; /* in this slot */
    fd_acct_addr_t        vote_pubkey[1];
    fd_acct_addr_t        identity_pubkey[1];
    fd_bundle_crank_gen_t gen[1];
    fd_acct_addr_t        tip_receiver_owner[1];
    ulong                 epoch;
    fd_bundle_crank_tip_payment_config_t prev_config[1]; /* as of start of slot, then updated */
    uchar                 recent_blockhash[32];
    fd_ed25519_sig_t      last_sig[1];

    fd_keyswitch_t *      keyswitch;
    fd_keyguard_client_t  keyguard_client[1];

    ulong                 metrics[4];
  } crank[1];


  /* Used between during_frag and after_frag */
  ulong pending_rebate_sz;
  union{ fd_pack_rebate_t rebate[1]; uchar footprint[USHORT_MAX]; } rebate[1];

  /* Single queued BAM result used when a partial bundle must be abandoned
     from during_frag, where we cannot publish immediately.

     Sentinel: bundle_txn_cnt==0 means "no queued result". This is safe
     because only non-empty BAM batches are ever queued here. */
  fd_bam_bundle_result_t pending_bam_result[1];

  /* BAM batches accepted into fd_pack but not yet handed off to bank.
     Used to clean up buffered BAM work when slot or blockhash validity changes. */
  pack_bam_work_t * bam_work;
  ulong             bam_work_cnt;
  ulong             bam_pending_work_cnt;
  ulong             bam_scheduled_work_cnt;
};

static inline void
pack_tile_note_bam_work_stage( fd_pack_ctx_t * ctx,
                               ulong           stage_idx,
                               ulong           item_cnt,
                               ulong           txn_cnt ) {
  ctx->bam_work_item_stage_cnt[ stage_idx ] += item_cnt;
  ctx->bam_work_transaction_stage_cnt[ stage_idx ] += txn_cnt;
}

/* Bundle metadata must fit both block engine and BAM uses. */
#define BUNDLE_META_SZ 40U
FD_STATIC_ASSERT( sizeof(block_builder_info_t)==BUNDLE_META_SZ, block_builder_info_t );

#define PACK_TILE_BUNDLE_KIND_NONE         (0)
#define PACK_TILE_BUNDLE_KIND_BLOCK_ENGINE (1)
#define PACK_TILE_BUNDLE_KIND_BAM          (2)

#define FD_PACK_METRIC_STATE_TRANSACTIONS 0
#define FD_PACK_METRIC_STATE_BANKS        1
#define FD_PACK_METRIC_STATE_LEADER       2
#define FD_PACK_METRIC_STATE_MICROBLOCKS  3

static inline void
pack_tile_publish_bam_leader_state( fd_pack_ctx_t *     ctx,
                                    fd_stem_context_t * stem );

/* Updates one component of the metric state.  If the state has changed,
   records the change. */
static inline void
update_metric_state( fd_pack_ctx_t * ctx,
                     long            effective_as_of,
                     int             type,
                     int             status ) {
  uint current_state = fd_uint_insert_bit( ctx->metric_state, type, status );
  if( FD_UNLIKELY( current_state!=ctx->metric_state ) ) {
    ctx->metric_timing[ ctx->metric_state ] += effective_as_of - ctx->metric_state_begin;
    ctx->metric_state_begin = effective_as_of;
    ctx->metric_state = current_state;
  }
}

static inline long
pack_tile_wallclock_from_ticks( fd_pack_ctx_t const * ctx,
                                long                  now_ticks ) {
  if( FD_UNLIKELY( !ctx->ticks_per_ns ) ) return ctx->approx_wallclock_ns;
  return ctx->approx_wallclock_ns + (long)((double)(now_ticks - ctx->approx_tickcount) / ctx->ticks_per_ns);
}

static inline long
pack_tile_now_ns( fd_pack_ctx_t const * ctx ) {
  return pack_tile_wallclock_from_ticks( ctx, fd_tickcount() );
}

static inline ulong
pack_tile_bam_fresh_slot_for_hint( fd_pack_ctx_t const * ctx,
                                   ulong                 max_schedule_slot ) {
  if( FD_UNLIKELY( ctx->leader_slot==ULONG_MAX ) ) return ULONG_MAX;
  return fd_ulong_if( max_schedule_slot==ctx->leader_slot,
                      ctx->leader_slot,
                      ULONG_MAX );
}

static inline pack_bam_recent_slot_t *
pack_tile_bam_recent_slot_prepare( fd_pack_ctx_t * ctx,
                                   ulong           slot ) {
  FD_TEST( slot!=ULONG_MAX );
  pack_bam_recent_slot_t * entry = &ctx->bam_recent_slot[ slot & ( FD_PACK_BAM_RECENT_SLOT_CNT - 1UL ) ];
  if( FD_LIKELY( entry->slot==slot ) ) return entry;
  *entry = (pack_bam_recent_slot_t){ .slot = slot, .first_debug_seq_id = UINT_MAX };
  return entry;
}

static inline void
pack_tile_bam_recent_slot_sub_pending( fd_pack_ctx_t * ctx,
                                       ulong           slot,
                                       ulong           item_cnt,
                                       ulong           txn_cnt ) {
  if( FD_UNLIKELY( slot==ULONG_MAX ) ) {
    ctx->bam_unresolved_work.pending_items = fd_ulong_sat_sub( ctx->bam_unresolved_work.pending_items, item_cnt );
    ctx->bam_unresolved_work.pending_txns  = fd_ulong_sat_sub( ctx->bam_unresolved_work.pending_txns,  txn_cnt  );
    return;
  }
  pack_bam_recent_slot_t * entry = pack_tile_bam_recent_slot_prepare( ctx, slot );
  entry->pending_items = fd_ulong_sat_sub( entry->pending_items, item_cnt );
  entry->pending_txns  = fd_ulong_sat_sub( entry->pending_txns,  txn_cnt  );
}

static inline void
pack_tile_note_bam_received( fd_pack_ctx_t * ctx,
                             ulong           slot,
                             uint            seq_id,
                             ulong           item_cnt,
                             ulong           txn_cnt ) {
  pack_tile_note_bam_work_stage( ctx, FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_RECEIVED_IDX, item_cnt, txn_cnt );
  if( FD_UNLIKELY( slot==ULONG_MAX ) ) {
    ctx->bam_unresolved_work.received_items += item_cnt;
    ctx->bam_unresolved_work.received_txns  += txn_cnt;
    return;
  }
  pack_bam_recent_slot_t * entry = pack_tile_bam_recent_slot_prepare( ctx, slot );
  if( FD_UNLIKELY( ctx->dump_bam_mode==FD_BAM_DEBUG_DUMP_MODE_SLOT_FIRST && entry->first_debug_seq_id==UINT_MAX ) ) entry->first_debug_seq_id = seq_id;
  entry->received_items += item_cnt;
  entry->received_txns  += txn_cnt;
}

static inline void
pack_tile_note_bam_accepted( fd_pack_ctx_t * ctx,
                             ulong           slot,
                             ulong           item_cnt,
                             ulong           txn_cnt ) {
  pack_tile_note_bam_work_stage( ctx, FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_ACCEPTED_IDX, item_cnt, txn_cnt );
  if( FD_UNLIKELY( slot==ULONG_MAX ) ) {
    ctx->bam_unresolved_work.accepted_items += item_cnt;
    ctx->bam_unresolved_work.accepted_txns  += txn_cnt;
    return;
  }
  pack_bam_recent_slot_t * entry = pack_tile_bam_recent_slot_prepare( ctx, slot );
  entry->accepted_items += item_cnt;
  entry->accepted_txns  += txn_cnt;
}

static inline void
pack_tile_note_bam_rejected_pre_pending( fd_pack_ctx_t * ctx,
                                         ulong           slot,
                                         ulong           item_cnt,
                                         ulong           txn_cnt ) {
  pack_tile_note_bam_work_stage( ctx, FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_REJECTED_PRE_PENDING_IDX, item_cnt, txn_cnt );
  if( FD_UNLIKELY( slot==ULONG_MAX ) ) {
    ctx->bam_unresolved_work.rejected_pre_pending_items += item_cnt;
    ctx->bam_unresolved_work.rejected_pre_pending_txns  += txn_cnt;
    return;
  }
  pack_bam_recent_slot_t * entry = pack_tile_bam_recent_slot_prepare( ctx, slot );
  entry->rejected_pre_pending_items += item_cnt;
  entry->rejected_pre_pending_txns  += txn_cnt;
}

static inline void
pack_tile_note_bam_first_outcome( fd_pack_ctx_t * ctx,
                                  ulong           outcome_idx,
                                  long            first_rx_ts_ns,
                                  long            outcome_ns ) {
  ctx->bam_work_first_outcome_cnt[ outcome_idx ]++;
  if( FD_LIKELY( first_rx_ts_ns>0L && outcome_ns>=first_rx_ts_ns ) ) {
    fd_histf_sample( ctx->bam_work_rx_to_first_outcome_nanos,
                     fd_ulong_sat_sub( (ulong)outcome_ns, (ulong)first_rx_ts_ns ) );
  }
}

static inline long
pack_tile_current_bam_bundle_first_rx_ts_ns( fd_pack_ctx_t const * ctx ) {
  long first_rx_ts_ns = 0L;
  for( uchar i=0U; i<ctx->current_bam_bundle->txn_cnt; i++ ) {
    if( FD_UNLIKELY( !ctx->current_bam_bundle->received[ i ] ) ) continue;
    long rx_ts_ns = ctx->current_bam_bundle->_txn[ i ]->txnp->scheduler_arrival_time_nanos;
    if( FD_UNLIKELY( !first_rx_ts_ns || ( rx_ts_ns && rx_ts_ns<first_rx_ts_ns ) ) ) first_rx_ts_ns = rx_ts_ns;
  }
  return first_rx_ts_ns;
}

static inline void
pack_tile_note_bam_landed( fd_pack_ctx_t * ctx,
                           ulong           slot,
                           ulong           item_cnt,
                           ulong           txn_cnt ) {
  pack_tile_note_bam_work_stage( ctx, FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_LANDED_IDX, item_cnt, txn_cnt );
  if( FD_UNLIKELY( slot==ULONG_MAX ) ) {
    ctx->bam_unresolved_work.landed_items += item_cnt;
    ctx->bam_unresolved_work.landed_txns  += txn_cnt;
    return;
  }
  pack_bam_recent_slot_t * entry = pack_tile_bam_recent_slot_prepare( ctx, slot );
  entry->landed_items += item_cnt;
  entry->landed_txns  += txn_cnt;
}

static inline void
pack_tile_note_first_bam_insert( fd_pack_ctx_t *     ctx,
                                 fd_stem_context_t * stem,
                                 long                now_ns,
                                 ulong               bam_fresh_slot ) {
  if( FD_UNLIKELY( ctx->leader_slot==ULONG_MAX ) ) return;

  long insert_minus_slot_end_ns = now_ns - ctx->slot_end_ns;
  if( FD_UNLIKELY( !ctx->bam_first_insert_seen ) ) {
    ctx->bam_first_insert_seen = 1U;
    ctx->bam_first_insert_minus_slot_end_ns = insert_minus_slot_end_ns;
  }
  if( FD_LIKELY( ctx->bam_current_slot_fresh || insert_minus_slot_end_ns>=0L || bam_fresh_slot!=ctx->leader_slot ) ) return;
  ctx->bam_current_slot_fresh = 1U;
  pack_tile_publish_bam_leader_state( ctx, stem );
}

static inline ulong
pack_tile_bam_first_event_result_idx( uchar seen,
                                      long  event_minus_slot_end_ns ) {
  if( FD_UNLIKELY( !seen ) ) return FD_METRICS_ENUM_PACK_BAM_FIRST_EVENT_RESULT_V_NO_EVENT_IDX;
  if( FD_LIKELY( event_minus_slot_end_ns<0L ) ) return FD_METRICS_ENUM_PACK_BAM_FIRST_EVENT_RESULT_V_BEFORE_END_IDX;
  if( FD_UNLIKELY( event_minus_slot_end_ns>0L ) ) return FD_METRICS_ENUM_PACK_BAM_FIRST_EVENT_RESULT_V_AFTER_END_IDX;
  return FD_METRICS_ENUM_PACK_BAM_FIRST_EVENT_RESULT_V_AT_END_IDX;
}

static inline void
pack_tile_bam_record_first_event_distance( fd_histf_t before_end_hist[ static 1 ],
                                           fd_histf_t after_end_hist[ static 1 ],
                                           long       event_minus_slot_end_ns ) {
  if( FD_LIKELY( event_minus_slot_end_ns<0L ) ) {
    fd_histf_sample( before_end_hist, (ulong)(-event_minus_slot_end_ns) );
  } else if( FD_UNLIKELY( event_minus_slot_end_ns>0L ) ) {
    fd_histf_sample( after_end_hist, (ulong)event_minus_slot_end_ns );
  }
}


static inline void
remove_ib( fd_pack_ctx_t * ctx ) {
  /* It's likely the initializer bundle is long scheduled, but we want to
     try deleting it just in case. */
  if( FD_UNLIKELY( ctx->crank->enabled & ctx->crank->ib_inserted ) ) {
    ulong deleted = fd_pack_delete_transaction( ctx->pack, (fd_ed25519_sig_t const *)ctx->crank->last_sig );
    FD_MCNT_INC( PACK, TRANSACTION_DELETED, deleted );
  }
  ctx->crank->ib_inserted = 0;
}

static inline void
pack_tile_publish_bam_leader_state( fd_pack_ctx_t *     ctx,
                                    fd_stem_context_t * stem ) {
  /* Leader snapshots are latest-value-wins, so pack only publishes when
     the derived state actually changes. */
  if( FD_UNLIKELY( ctx->leader_slot==ULONG_MAX || ctx->bam_leader_out.idx==ULONG_MAX ) ) return;
  long now_ticks = fd_tickcount();
  /* approx_wallclock_ns / approx_tickcount are sampled together during
     housekeeping so hot-path BAM leader-state publishes can cheaply
     project tickcount deltas back into wallclock nanoseconds. */
  long now_ns    = pack_tile_wallclock_from_ticks( ctx, now_ticks );
  fd_became_leader_t const * became_leader = ctx->_became_leader;
  /* BAM leader-state tick uses the same slot-relative notion as pack's
     reference_tick. Clamp pre-slot and post-slot observations into the
     valid [0,ticks_per_slot] range because now_ns is derived from an
     approximate wallclock sample taken during housekeeping. */
  uint tick = FD_LIKELY( became_leader->tick_duration_ns && now_ns>became_leader->slot_start_ns )
            ? fd_uint_min( (uint)((now_ns - became_leader->slot_start_ns) / (long)became_leader->tick_duration_ns),
                           (uint)became_leader->ticks_per_slot )
            : 0U;

  fd_bam_leader_state_t state = { .slot = ctx->leader_slot, .tick = tick,
    .slot_cu_budget_remaining = (uint)fd_ulong_sat_sub( ctx->limits.slot_max_cost, fd_pack_current_block_cost( ctx->pack ) ),
    .slot_end_ns = ctx->slot_end_ns,
    .current_slot_fresh = ctx->bam_current_slot_fresh
  };

  if( FD_LIKELY( fd_bam_leader_state_eq( &state, &ctx->last_bam_leader_state ) ) ) return;
  ctx->last_bam_leader_state = state;

  fd_bam_leader_state_t * out = fd_chunk_to_laddr( ctx->bam_leader_out.mem, ctx->bam_leader_out.chunk );
  *out = state;

  fd_stem_publish( stem,
                   ctx->bam_leader_out.idx,
                   0UL,
                   ctx->bam_leader_out.chunk,
                   sizeof(fd_bam_leader_state_t),
                   0UL,
                   0UL,
                   fd_frag_meta_ts_comp( now_ticks ) );
  ctx->bam_leader_out.chunk = fd_dcache_compact_next( ctx->bam_leader_out.chunk,
                                                      sizeof(fd_bam_leader_state_t),
                                                      ctx->bam_leader_out.chunk0,
                                                      ctx->bam_leader_out.wmark );
}

static inline void
pack_tile_publish_bam_result( fd_pack_ctx_t *             ctx,
                              fd_stem_context_t *         stem,
                              fd_bam_bundle_result_t const * res ) {
  if( FD_UNLIKELY( ctx->bam_result_out.idx==ULONG_MAX ) ) return;
  fd_bam_bundle_result_t * out = fd_chunk_to_laddr( ctx->bam_result_out.mem, ctx->bam_result_out.chunk );
  *out = *res;
  fd_stem_publish( stem,
                   ctx->bam_result_out.idx,
                   0UL,
                   ctx->bam_result_out.chunk,
                   sizeof(fd_bam_bundle_result_t),
                   0UL,
                   0UL,
                   fd_frag_meta_ts_comp( fd_tickcount() ) );
  ctx->bam_result_out.chunk = fd_dcache_compact_next( ctx->bam_result_out.chunk,
                                                      sizeof(fd_bam_bundle_result_t),
                                                      ctx->bam_result_out.chunk0,
                                                      ctx->bam_result_out.wmark );
}

static inline void
pack_tile_publish_bam_invalid_result( fd_pack_ctx_t *                ctx,
                                      fd_stem_context_t *            stem,
                                      uint                           seq_id,
                                      ulong                          max_schedule_slot,
                                      uchar                          txn_cnt,
                                      pack_tile_bam_invalid_reason_t reason ) {
  fd_bam_bundle_result_t res = {
    .seq_id           = seq_id,
    .slot             = max_schedule_slot,
    .bundle_txn_cnt   = txn_cnt,
    .scheduling_error = FD_BAM_SCHED_ERR_NONE,
  };
  if( FD_UNLIKELY( reason==PACK_TILE_BAM_INVALID_OUTSIDE_SLOT ) ) {
    res.scheduling_error = FD_BAM_SCHED_ERR_OUTSIDE_SLOT;
  } else if( FD_UNLIKELY( reason==PACK_TILE_BAM_INVALID_BLOCKHASH_EXPIRED ) ) {
    res.transaction_err[ 0 ] = bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND;
    res.transaction_err_count = 1U;
    for( uchar i=0U; i<res.bundle_txn_cnt; i++ ) res.sanitize_success[ i ] = 1U;
  }
  pack_tile_publish_bam_result( ctx, stem, &res );
}

static inline void
pack_tile_flush_pending_bam_result( fd_pack_ctx_t *     ctx,
                                    fd_stem_context_t * stem ) {
  /* during_frag cannot publish stem output, so partial BAM-bundle failures
     may be staged in pending_bam_result and later flushed from a safe context. */
  if( FD_LIKELY( !ctx->pending_bam_result->bundle_txn_cnt ) ) return;
  pack_tile_publish_bam_result( ctx, stem, ctx->pending_bam_result );
  ctx->pending_bam_result->bundle_txn_cnt = 0U;
}

/* BAM work is validated against the best local slot view pack has:
   the active leader slot if present, otherwise the highest observed slot. */
static inline ulong
pack_tile_bam_best_known_slot( fd_pack_ctx_t const * ctx ) {
  if( FD_LIKELY( ctx->leader_slot!=ULONG_MAX ) ) return ctx->leader_slot;
  if( FD_LIKELY( ctx->highest_observed_slot ) )  return ctx->highest_observed_slot;
  return ULONG_MAX;
}

static inline ulong
pack_tile_bam_funnel_slot( fd_pack_ctx_t const * ctx,
                           ulong                 max_schedule_slot ) {
  (void)ctx;
  return max_schedule_slot;
}

static inline char const *
pack_tile_bam_pack_insert_reason_cstr( int pack_rc ) {
  switch( pack_rc ) {
  case FD_PACK_INSERT_REJECT_DUPLICATE:        return "insert_reject_duplicate";
  case FD_PACK_INSERT_REJECT_UNAFFORDABLE:     return "insert_reject_unaffordable";
  case FD_PACK_INSERT_REJECT_ADDR_LUT:         return "insert_reject_addr_lut";
  case FD_PACK_INSERT_REJECT_EXPIRED:          return "insert_reject_expired";
  case FD_PACK_INSERT_REJECT_TOO_LARGE:        return "insert_reject_too_large";
  case FD_PACK_INSERT_REJECT_ACCOUNT_CNT:      return "insert_reject_account_cnt";
  case FD_PACK_INSERT_REJECT_DUPLICATE_ACCT:   return "insert_reject_duplicate_acct";
  case FD_PACK_INSERT_REJECT_ESTIMATION_FAIL:  return "insert_reject_estimation_fail";
  case FD_PACK_INSERT_REJECT_WRITES_SYSVAR:    return "insert_reject_writes_sysvar";
  case FD_PACK_INSERT_REJECT_INVALID_NONCE:    return "insert_reject_invalid_nonce";
  case FD_PACK_INSERT_REJECT_BUNDLE_BLACKLIST: return "insert_reject_bundle_blacklist";
  case FD_PACK_INSERT_REJECT_NONCE_CONFLICT:   return "insert_reject_nonce_conflict";
  case FD_PACK_INSERT_REJECT_PRIORITY:         return "insert_reject_container_full";
  case FD_PACK_INSERT_REJECT_NONCE_PRIORITY:   return "insert_reject_nonce_container_full";
  default:                                     return "insert_reject_other";
  }
}

static inline void
pack_tile_log_bam_drop( fd_pack_ctx_t const * ctx,
                        char const *          category,
                        char const *          reason,
                        uint                  invalid_reason_known,
                        uint                  invalid_reason_idx,
                        uint                  pack_rc_known,
                        int                   pack_rc,
                        uint                  seq_id,
                        uchar                 txn_cnt,
                        char const *          work_state,
                        ulong                 work_slot,
                        ulong                 max_schedule_slot,
                        ulong                 blockhash_slot,
                        long                  first_rx_ts_ns,
                        uint                  revert_on_error_known,
                        uint                  revert_on_error,
                        uint                  batch_idx_known,
                        uint                  batch_idx,
                        ulong                 txn_received,
                        ulong                 txn_expected,
                        uint                  first_missing_idx_known,
                        uint                  first_missing_idx,
                        void const *          sig0 ) {
  if( FD_UNLIKELY( ctx->dump_bam_mode!=FD_BAM_DEBUG_DUMP_MODE_ALL ) ) {
    /* dump_bam_slot_first_txn should follow only the first BAM batch latched
       for this slot; later batches in the same slot stay quiet. */
    if( FD_LIKELY( ctx->dump_bam_mode!=FD_BAM_DEBUG_DUMP_MODE_SLOT_FIRST || work_slot==ULONG_MAX ) ) return;
    pack_bam_recent_slot_t const * entry = &ctx->bam_recent_slot[ work_slot & ( FD_PACK_BAM_RECENT_SLOT_CNT - 1UL ) ];
    if( FD_UNLIKELY( entry->slot!=work_slot || entry->first_debug_seq_id!=seq_id ) ) return;
  }

  ulong validation_slot                    = pack_tile_bam_best_known_slot( ctx );
  ulong required_min_slot                  = ULONG_MAX;
  long  now_ns                             = pack_tile_now_ns( ctx );
  ulong age_ns                             = ULONG_MAX;
  long  current_leader_slot_end_ns         = ctx->leader_slot==ULONG_MAX ? 0L : ctx->slot_end_ns;
  long  now_minus_current_leader_slot_end  = current_leader_slot_end_ns ? now_ns - current_leader_slot_end_ns : 0L;
  ulong pack_avail_txn_cnt                 = fd_pack_avail_txn_cnt( ctx->pack );
  ulong extra_queue_cnt                    = 0UL;
#if FD_PACK_USE_EXTRA_STORAGE
  extra_queue_cnt = extra_txn_deq_cnt( ctx->extra_txn_deq );
#endif
  if( FD_LIKELY( first_rx_ts_ns>0L && now_ns>=first_rx_ts_ns ) ) age_ns = (ulong)( now_ns - first_rx_ts_ns );
  if( FD_LIKELY( validation_slot!=ULONG_MAX ) ) {
    required_min_slot = blockhash_slot!=ULONG_MAX ? fd_ulong_max( validation_slot, blockhash_slot ) : validation_slot;
  } else if( FD_LIKELY( blockhash_slot!=ULONG_MAX ) ) {
    required_min_slot = blockhash_slot;
  }

  char sig0_b58[ FD_BASE58_ENCODED_64_SZ ] = "<none>";
  if( FD_LIKELY( sig0 ) ) fd_base58_encode_64( (uchar const *)sig0, NULL, sig0_b58 );

  FD_LOG_INFO(( "bam_drop category=%s reason=%s invalid_reason_known=%u invalid_reason_idx=%u pack_rc_known=%u pack_rc=%d seq_id=%u txns=%u sig0=%s work_state=%s validation_slot=%lu validation_slot_known=%u required_min_slot=%lu required_min_slot_known=%u bam_max_schedule_slot=%lu work_slot=%lu work_slot_known=%u blockhash_slot=%lu blockhash_slot_known=%u leader_slot=%lu leader_slot_known=%u current_leader_slot_end_ns=%ld current_leader_slot_end_known=%u now_ns=%ld now_minus_current_leader_slot_end_ns=%ld highest_observed_slot=%lu first_rx_ts_ns=%ld first_rx_known=%u age_ns=%lu age_known=%u revert_on_error_known=%u revert_on_error=%u batch_idx_known=%u batch_idx=%u txn_received=%lu txn_expected=%lu first_missing_idx_known=%u first_missing_idx=%u pack_avail_txn_cnt=%lu extra_queue_cnt=%lu bam_work_cnt=%lu bam_pending_work_cnt=%lu bam_scheduled_work_cnt=%lu max_pending_transactions=%lu",
                category,
                reason,
                invalid_reason_known,
                invalid_reason_idx,
                pack_rc_known,
                pack_rc,
                seq_id,
                (uint)txn_cnt,
                sig0_b58,
                work_state,
                validation_slot,
                (uint)( validation_slot!=ULONG_MAX ),
                required_min_slot,
                (uint)( required_min_slot!=ULONG_MAX ),
                max_schedule_slot,
                work_slot,
                (uint)( work_slot!=ULONG_MAX ),
                blockhash_slot,
                (uint)( blockhash_slot!=ULONG_MAX ),
                ctx->leader_slot,
                (uint)( ctx->leader_slot!=ULONG_MAX ),
                current_leader_slot_end_ns,
                (uint)( !!current_leader_slot_end_ns ),
                now_ns,
                now_minus_current_leader_slot_end,
                ctx->highest_observed_slot,
                first_rx_ts_ns,
                (uint)( first_rx_ts_ns>0L ),
                age_ns,
                (uint)( age_ns!=ULONG_MAX ),
                revert_on_error_known,
                revert_on_error,
                batch_idx_known,
                batch_idx,
                txn_received,
                txn_expected,
                first_missing_idx_known,
                first_missing_idx,
                pack_avail_txn_cnt,
                extra_queue_cnt,
                ctx->bam_work_cnt,
                ctx->bam_pending_work_cnt,
                ctx->bam_scheduled_work_cnt,
                ctx->max_pending_transactions ));
}

static inline pack_tile_bam_invalid_reason_t
pack_tile_bam_invalid_reason( ulong current_slot,
                              ulong max_schedule_slot,
                              ulong blockhash_slot ) {
  /* current_slot is the best local execution slot known to pack. If it is not
     known yet, only an explicit slot hint that already trails the blockhash
     slot can be rejected immediately.

     Once current_slot is known, BAM matches the model contract: blockhash
     lifetime is checked against current_slot, and max_schedule_slot
     must still be >= both current_slot and blockhash_slot. */
  if( FD_UNLIKELY( current_slot==ULONG_MAX ) ) {
    if( FD_UNLIKELY( max_schedule_slot<blockhash_slot ) ) {
      return PACK_TILE_BAM_INVALID_OUTSIDE_SLOT;
    }
    return PACK_TILE_BAM_INVALID_NONE;
  }

  ulong oldest_live_slot = fd_ulong_max( current_slot, TRANSACTION_LIFETIME_SLOTS )-TRANSACTION_LIFETIME_SLOTS;
  if( FD_UNLIKELY( blockhash_slot<oldest_live_slot ) ) return PACK_TILE_BAM_INVALID_BLOCKHASH_EXPIRED;
  if( FD_UNLIKELY( max_schedule_slot<fd_ulong_max( current_slot, blockhash_slot ) ) ) {
    return PACK_TILE_BAM_INVALID_OUTSIDE_SLOT;
  }
  return PACK_TILE_BAM_INVALID_NONE;
}


static inline pack_bam_work_t
pack_tile_bam_work_swap_remove( fd_pack_ctx_t * ctx,
                                ulong           idx ) {
  FD_TEST( idx<ctx->bam_work_cnt );
  pack_bam_work_t item = ctx->bam_work[ idx ];
  ulong last_idx = --ctx->bam_work_cnt;
  if( FD_LIKELY( idx<last_idx ) ) ctx->bam_work[ idx ] = ctx->bam_work[ last_idx ];
  if( FD_LIKELY( item.state==PACK_BAM_WORK_STATE_PENDING ) ) ctx->bam_pending_work_cnt--;
  else                                                        ctx->bam_scheduled_work_cnt--;
  return item;
}

static inline ulong
pack_tile_bam_work_find_by_sig0( fd_pack_ctx_t const *       ctx,
                                 void const *                sig0 ) {
  for( ulong i=0UL; i<ctx->bam_work_cnt; i++ ) {
    if( FD_LIKELY( memcmp( ctx->bam_work[ i ].sig[ 0 ], sig0, sizeof(fd_ed25519_sig_t) ) ) ) continue;
    return i;
  }
  return ctx->bam_work_cnt;
}

static inline ulong
pack_tile_bam_work_find_by_any_sig( fd_pack_ctx_t const * ctx,
                                    uchar const           sig[ static 64 ],
                                    int                   state_filter,
                                    uchar *               matched_idx ) {
  for( ulong i=0UL; i<ctx->bam_work_cnt; i++ ) {
    pack_bam_work_t const * item = &ctx->bam_work[ i ];
    if( FD_UNLIKELY( state_filter>=0 && item->state!=(uchar)state_filter ) ) continue;
    for( uchar j=0U; j<item->txn_cnt; j++ ) {
      if( FD_LIKELY( memcmp( item->sig[ j ], sig, sizeof(fd_ed25519_sig_t) ) ) ) continue;
      if( FD_UNLIKELY( matched_idx ) ) *matched_idx = j;
      return i;
    }
  }
  return ctx->bam_work_cnt;
}

static inline int
pack_tile_track_bam_work( fd_pack_ctx_t *          ctx,
                          void const *             sigs,
                          long                     first_rx_ts_ns,
                          uint                     seq_id,
                          ulong                    slot,
                          ulong                    max_schedule_slot,
                          ulong                    blockhash_slot,
                          uchar                    txn_cnt ) {
  ulong work_idx = pack_tile_bam_work_find_by_sig0( ctx, sigs );
  if( FD_UNLIKELY( work_idx<ctx->bam_work_cnt ) ) {
    pack_bam_work_t old = pack_tile_bam_work_swap_remove( ctx, work_idx );
    if( FD_UNLIKELY( old.state==PACK_BAM_WORK_STATE_PENDING ) ) pack_tile_bam_recent_slot_sub_pending( ctx, old.slot, 1UL, old.txn_cnt );
  }
  if( FD_UNLIKELY( ctx->bam_work_cnt >= ctx->max_pending_transactions ) ) return 0;

  pack_bam_work_t * item = &ctx->bam_work[ ctx->bam_work_cnt++ ];
  *item = (pack_bam_work_t){
    .first_rx_ts_ns    = first_rx_ts_ns,
    .slot              = slot,
    .max_schedule_slot = max_schedule_slot,
    .blockhash_slot    = blockhash_slot,
    .seq_id            = seq_id,
    .txn_cnt           = txn_cnt,
    .state             = PACK_BAM_WORK_STATE_PENDING,
  };
  fd_memcpy( item->sig, sigs, (ulong)txn_cnt * sizeof(fd_ed25519_sig_t) );
  ctx->bam_pending_work_cnt++;
  pack_tile_note_bam_work_stage( ctx, FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_PENDING_ENTERED_IDX, 1UL, txn_cnt );
  if( FD_UNLIKELY( slot==ULONG_MAX ) ) {
    ctx->bam_unresolved_work.pending_items += 1UL;
    ctx->bam_unresolved_work.pending_txns  += txn_cnt;
  } else {
    pack_bam_recent_slot_t * entry = pack_tile_bam_recent_slot_prepare( ctx, slot );
    entry->pending_items += 1UL;
    entry->pending_txns  += txn_cnt;
  }
  return 1;
}

static inline void
pack_tile_evict_invalid_pending_bam_work( fd_pack_ctx_t *     ctx,
                                          fd_stem_context_t * stem,
                                          ulong               current_slot ) {
  for( ulong i=0UL; i<ctx->bam_work_cnt; ) {
    if( FD_UNLIKELY( ctx->bam_work[ i ].state!=PACK_BAM_WORK_STATE_PENDING ) ) {
      i++;
      continue;
    }
    pack_tile_bam_invalid_reason_t invalid_reason =
        pack_tile_bam_invalid_reason( current_slot,
                                      ctx->bam_work[ i ].max_schedule_slot,
                                      ctx->bam_work[ i ].blockhash_slot );
    if( FD_LIKELY( invalid_reason==PACK_TILE_BAM_INVALID_NONE ) ) {
      i++;
      continue;
    }

    /* BAM work accepted into pack can become stale later as slots advance, so
       remove it from both the local tracker and pack when that happens. */
    pack_bam_work_t item = pack_tile_bam_work_swap_remove( ctx, i );
    pack_tile_log_bam_drop( ctx,
                               "post_pending_validation",
                               invalid_reason==PACK_TILE_BAM_INVALID_OUTSIDE_SLOT
                                   ? "pending_evicted_outside_slot"
                                   : "pending_evicted_blockhash_expired",
                               1U,
                               (uint)invalid_reason,
                               0U,
                               0,
                               item.seq_id,
                               item.txn_cnt,
                               "pending",
                               item.slot,
                               item.max_schedule_slot,
                               item.blockhash_slot,
                               item.first_rx_ts_ns,
                               0U,
                               0U,
                               0U,
                               0U,
                               item.txn_cnt,
                               item.txn_cnt,
                               0U,
                               0U,
                               item.sig[ 0 ] );

    pack_tile_record_bam_invalid_reason_count( ctx->bam_pending_work_evicted_cnt, invalid_reason, item.txn_cnt );
    pack_tile_note_bam_work_stage( ctx, FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_PENDING_EVICTED_IDX, 1UL, item.txn_cnt );
    pack_tile_note_bam_first_outcome( ctx,
                                      FD_METRICS_ENUM_PACK_BAM_WORK_FIRST_OUTCOME_V_PENDING_EVICTED_IDX,
                                      item.first_rx_ts_ns,
                                      pack_tile_now_ns( ctx ) );
    if( FD_UNLIKELY( item.slot==ULONG_MAX ) ) {
      ctx->bam_unresolved_work.evicted_post_pending_items += 1UL;
      ctx->bam_unresolved_work.evicted_post_pending_txns  += item.txn_cnt;
    } else {
      pack_bam_recent_slot_t * item_slot = pack_tile_bam_recent_slot_prepare( ctx, item.slot );
      item_slot->evicted_post_pending_items += 1UL;
      item_slot->evicted_post_pending_txns  += item.txn_cnt;
    }
    pack_tile_bam_recent_slot_sub_pending( ctx, item.slot, 1UL, item.txn_cnt );

    ulong deleted = fd_pack_delete_transaction( ctx->pack, (fd_ed25519_sig_t const *)(void const *)&item.sig[ 0 ] );
    FD_MCNT_INC( PACK, TRANSACTION_DELETED, deleted );
    pack_tile_publish_bam_invalid_result( ctx,
                                          stem,
                                          item.seq_id,
                                          item.max_schedule_slot,
                                          item.txn_cnt,
                                          invalid_reason );
  }
}

/* Best-effort mapping from fd_pack insert errors to BAM TransactionErrorReason values. */
static inline bam_types_TransactionErrorReason
pack_tile_bam_txn_err_from_pack_insert( int pack_rc ) {
  switch( pack_rc ) {
  case FD_PACK_INSERT_REJECT_DUPLICATE:        return bam_types_TransactionErrorReason_ALREADY_PROCESSED;
  case FD_PACK_INSERT_REJECT_UNAFFORDABLE:     return bam_types_TransactionErrorReason_INSUFFICIENT_FUNDS_FOR_FEE;
  case FD_PACK_INSERT_REJECT_ADDR_LUT:         return bam_types_TransactionErrorReason_ADDRESS_LOOKUP_TABLE_NOT_FOUND;
  case FD_PACK_INSERT_REJECT_EXPIRED:          return bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND;
  case FD_PACK_INSERT_REJECT_TOO_LARGE:        return bam_types_TransactionErrorReason_WOULD_EXCEED_MAX_BLOCK_COST_LIMIT;
  case FD_PACK_INSERT_REJECT_ACCOUNT_CNT:      return bam_types_TransactionErrorReason_TOO_MANY_ACCOUNT_LOCKS;
  case FD_PACK_INSERT_REJECT_DUPLICATE_ACCT:   return bam_types_TransactionErrorReason_ACCOUNT_LOADED_TWICE;
  case FD_PACK_INSERT_REJECT_ESTIMATION_FAIL:  return bam_types_TransactionErrorReason_SANITIZE_FAILURE;
  case FD_PACK_INSERT_REJECT_WRITES_SYSVAR:    return bam_types_TransactionErrorReason_INVALID_WRITABLE_ACCOUNT;
  case FD_PACK_INSERT_REJECT_INVALID_NONCE:    return bam_types_TransactionErrorReason_SIGNATURE_FAILURE;
  case FD_PACK_INSERT_REJECT_BUNDLE_BLACKLIST: return bam_types_TransactionErrorReason_PROGRAM_EXECUTION_TEMPORARILY_RESTRICTED;
  case FD_PACK_INSERT_REJECT_NONCE_CONFLICT:   return bam_types_TransactionErrorReason_SANITIZE_FAILURE;
  default:                                     return bam_types_TransactionErrorReason_COMMIT_CANCELLED;
  }
}

static inline void
pack_tile_publish_bam_insert_reject( fd_pack_ctx_t *     ctx,
                                     fd_stem_context_t * stem,
                                     uint                seq_id,
                                     ulong               max_schedule_slot,
                                     uchar               txn_cnt,
                                     int                 pack_rc ) {
  fd_bam_bundle_result_t res = {
    .seq_id           = seq_id,
    .slot             = max_schedule_slot,
    .bundle_txn_cnt   = txn_cnt,
    .scheduling_error = FD_BAM_SCHED_ERR_NONE,
  };
  if( FD_UNLIKELY( pack_rc==FD_PACK_INSERT_REJECT_PRIORITY || pack_rc==FD_PACK_INSERT_REJECT_NONCE_PRIORITY ) ) {
    res.scheduling_error = FD_BAM_SCHED_ERR_CONTAINER_FULL;
    pack_tile_publish_bam_result( ctx, stem, &res );
    return;
  }

  res.transaction_err[ 0 ] = pack_tile_bam_txn_err_from_pack_insert( pack_rc );
  res.transaction_err_count = 1U;
  for( uchar i=0U; i<txn_cnt; i++ ) res.sanitize_success[ i ] = 1U;
  pack_tile_publish_bam_result( ctx, stem, &res );
}

static inline void
pack_tile_publish_bam_tracking_reject( fd_pack_ctx_t *          ctx,
                                       fd_stem_context_t *      stem,
                                       void const *             sig0,
                                       long                     first_rx_ts_ns,
                                       uint                     seq_id,
                                       ulong                    slot,
                                       ulong                    max_schedule_slot,
                                       ulong                    blockhash_slot,
                                       uint                     revert_on_error_known,
                                       uint                     revert_on_error,
                                       uchar                    txn_cnt ) {
  ulong deleted = fd_pack_delete_transaction( ctx->pack, (fd_ed25519_sig_t const *)sig0 );
  FD_MCNT_INC( PACK, TRANSACTION_DELETED, deleted );
  pack_tile_log_bam_drop( ctx,
                             "tracking",
                             "tracking_rejected",
                             0U,
                             0U,
                             0U,
                             0,
                             seq_id,
                             txn_cnt,
                             "staging",
                             slot,
                             max_schedule_slot,
                             blockhash_slot,
                             first_rx_ts_ns,
                             revert_on_error_known,
                             revert_on_error,
                             0U,
                             0U,
                             txn_cnt,
                             txn_cnt,
                             0U,
                             0U,
                             sig0 );
  ctx->bam_tracking_rejected_cnt++;
  ctx->bam_tracking_rejected_txn_cnt += txn_cnt;
  pack_tile_note_bam_first_outcome( ctx,
                                    FD_METRICS_ENUM_PACK_BAM_WORK_FIRST_OUTCOME_V_TRACKING_REJECTED_IDX,
                                    first_rx_ts_ns,
                                    pack_tile_now_ns( ctx ) );
  pack_tile_note_bam_rejected_pre_pending( ctx, slot, 1UL, txn_cnt );

  fd_bam_bundle_result_t res = {
    .seq_id           = seq_id,
    .slot             = max_schedule_slot,
    .bundle_txn_cnt   = txn_cnt,
    .scheduling_error = FD_BAM_SCHED_ERR_CONTAINER_FULL,
  };
  pack_tile_publish_bam_result( ctx, stem, &res );
}


FD_FN_CONST static inline ulong
scratch_align( void ) {
  return 4096UL;
}

FD_FN_PURE static inline ulong
scratch_footprint( fd_topo_tile_t const * tile ) {
  fd_pack_limits_t limits[1] = {{
    .max_cost_per_block        = tile->pack.larger_max_cost_per_block ? LARGER_MAX_COST_PER_BLOCK : FD_PACK_MAX_COST_PER_BLOCK_UPPER_BOUND,
    .max_vote_cost_per_block   = FD_PACK_MAX_VOTE_COST_PER_BLOCK_UPPER_BOUND,
    .max_write_cost_per_acct   = FD_PACK_MAX_WRITE_COST_PER_ACCT_UPPER_BOUND,
    .max_data_bytes_per_block  = tile->pack.larger_shred_limits_per_block ? LARGER_MAX_DATA_PER_BLOCK : FD_PACK_MAX_DATA_PER_BLOCK,
    .max_txn_per_microblock    = EFFECTIVE_TXN_PER_MICROBLOCK,
    .max_microblocks_per_block = (ulong)UINT_MAX, /* Limit not known yet */
  }};

  ulong l = FD_LAYOUT_INIT;
  l = FD_LAYOUT_APPEND( l, alignof( fd_pack_ctx_t ), sizeof( fd_pack_ctx_t )                                   );
  l = FD_LAYOUT_APPEND( l, fd_rng_align(),           fd_rng_footprint()                                        );
  l = FD_LAYOUT_APPEND( l, fd_pack_align(),          fd_pack_footprint( tile->pack.max_pending_transactions,
                                                                        BUNDLE_META_SZ,
                                                                        tile->pack.bank_tile_count,
                                                                        limits                               ) );
#if FD_PACK_USE_EXTRA_STORAGE
  l = FD_LAYOUT_APPEND( l, extra_txn_deq_align(),    extra_txn_deq_footprint()                                 );
#endif
  l = FD_LAYOUT_APPEND( l, alignof(pack_bam_work_t), tile->pack.max_pending_transactions*sizeof(pack_bam_work_t) );
  return FD_LAYOUT_FINI( l, scratch_align() );
}

static inline void
log_end_block_metrics( fd_pack_ctx_t * ctx,
                       long            now,
                       char const    * reason,
                       ulong           cus_consumed_in_block ) {
#define DELTA( m ) (fd_metrics_tl[ MIDX(COUNTER, PACK, TRANSACTION_SCHEDULE_##m) ] - ctx->last_sched_metrics->sched_results[ FD_METRICS_ENUM_PACK_TXN_SCHEDULE_V_##m##_IDX ])
#define AVAIL( m ) (fd_metrics_tl[ MIDX(GAUGE, PACK, AVAILABLE_TRANSACTIONS_##m) ])
    FD_LOG_INFO(( "pack_end_block(slot=%lu,%s,%lx,ticks_since_last_schedule=%ld,reasons=%lu,%lu,%lu,%lu,%lu,%lu,%lu;remaining=%lu+%lu+%lu+%lu;smallest=%lu;cus=%lu->%lu)",
          ctx->leader_slot, reason, ctx->bank_idle_bitset, now-ctx->last_sched_metrics->time,
          DELTA( TAKEN ), DELTA( CU_LIMIT ), DELTA( FAST_PATH ), DELTA( BYTE_LIMIT ), DELTA( WRITE_COST ), DELTA( SLOW_PATH ), DELTA( DEFER_SKIP ),
          AVAIL(REGULAR), AVAIL(VOTES), AVAIL(BUNDLES), AVAIL(CONFLICTING),
          (fd_metrics_tl[ MIDX(GAUGE, PACK, SMALLEST_PENDING_TRANSACTION) ]),
          (cus_consumed_in_block),
          (fd_metrics_tl[ MIDX(GAUGE, PACK, CUS_CONSUMED_IN_BLOCK) ])
    ));
#undef AVAIL
#undef DELTA
}

static inline void
get_done_packing( fd_pack_ctx_t * ctx, fd_done_packing_t * done_packing ) {
    done_packing->microblocks_in_slot = ctx->slot_microblock_cnt;
    fd_pack_get_block_limits( ctx->pack, done_packing->limits_usage, done_packing->limits );

#define DELTA( mem, m ) (fd_metrics_tl[ MIDX(COUNTER, PACK, TRANSACTION_SCHEDULE_##m) ] - ctx->mem->sched_results[ FD_METRICS_ENUM_PACK_TXN_SCHEDULE_V_##m##_IDX ])
    done_packing->block_results[ FD_METRICS_ENUM_PACK_TXN_SCHEDULE_V_TAKEN_IDX      ] = DELTA( start_block_sched_metrics, TAKEN      );
    done_packing->block_results[ FD_METRICS_ENUM_PACK_TXN_SCHEDULE_V_CU_LIMIT_IDX   ] = DELTA( start_block_sched_metrics, CU_LIMIT   );
    done_packing->block_results[ FD_METRICS_ENUM_PACK_TXN_SCHEDULE_V_FAST_PATH_IDX  ] = DELTA( start_block_sched_metrics, FAST_PATH  );
    done_packing->block_results[ FD_METRICS_ENUM_PACK_TXN_SCHEDULE_V_BYTE_LIMIT_IDX ] = DELTA( start_block_sched_metrics, BYTE_LIMIT );
    done_packing->block_results[ FD_METRICS_ENUM_PACK_TXN_SCHEDULE_V_WRITE_COST_IDX ] = DELTA( start_block_sched_metrics, WRITE_COST );
    done_packing->block_results[ FD_METRICS_ENUM_PACK_TXN_SCHEDULE_V_SLOW_PATH_IDX  ] = DELTA( start_block_sched_metrics, SLOW_PATH  );
    done_packing->block_results[ FD_METRICS_ENUM_PACK_TXN_SCHEDULE_V_DEFER_SKIP_IDX ] = DELTA( start_block_sched_metrics, DEFER_SKIP );

    done_packing->end_block_results[ FD_METRICS_ENUM_PACK_TXN_SCHEDULE_V_TAKEN_IDX      ] = DELTA( last_sched_metrics, TAKEN      );
    done_packing->end_block_results[ FD_METRICS_ENUM_PACK_TXN_SCHEDULE_V_CU_LIMIT_IDX   ] = DELTA( last_sched_metrics, CU_LIMIT   );
    done_packing->end_block_results[ FD_METRICS_ENUM_PACK_TXN_SCHEDULE_V_FAST_PATH_IDX  ] = DELTA( last_sched_metrics, FAST_PATH  );
    done_packing->end_block_results[ FD_METRICS_ENUM_PACK_TXN_SCHEDULE_V_BYTE_LIMIT_IDX ] = DELTA( last_sched_metrics, BYTE_LIMIT );
    done_packing->end_block_results[ FD_METRICS_ENUM_PACK_TXN_SCHEDULE_V_WRITE_COST_IDX ] = DELTA( last_sched_metrics, WRITE_COST );
    done_packing->end_block_results[ FD_METRICS_ENUM_PACK_TXN_SCHEDULE_V_SLOW_PATH_IDX  ] = DELTA( last_sched_metrics, SLOW_PATH  );
    done_packing->end_block_results[ FD_METRICS_ENUM_PACK_TXN_SCHEDULE_V_DEFER_SKIP_IDX ] = DELTA( last_sched_metrics, DEFER_SKIP );
#undef DELTA

  fd_pack_get_pending_smallest( ctx->pack, done_packing->pending_smallest, done_packing->pending_votes_smallest );
}

static inline void
pack_tile_abandon_current_bam_bundle( fd_pack_ctx_t *              ctx,
                                      fd_stem_context_t *          stem,
                                      _Bool                        queue_missing_result,
                                      pack_tile_bam_bundle_assembly_abandon_reason_t reason ) {
  if( FD_UNLIKELY( !ctx->current_bam_bundle->bundle ) ) return;
  FD_TEST( reason<=PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_POH_TIMEOUT );
  ctx->bam_bundle_assembly_abandon_cnt[ (ulong)reason ]++;
  pack_tile_note_bam_first_outcome( ctx,
                                    FD_METRICS_ENUM_PACK_BAM_WORK_FIRST_OUTCOME_V_BUNDLE_ASSEMBLY_ABANDONED_IDX,
                                    pack_tile_current_bam_bundle_first_rx_ts_ns( ctx ),
                                    pack_tile_now_ns( ctx ) );
  fd_ed25519_sig_t sig0[1] = {{0}};
  int first_missing_idx = -1;
  _Bool have_sig0 = 0;
  for( uchar i=0U; i<ctx->current_bam_bundle->txn_cnt; i++ ) {
    if( FD_UNLIKELY( !ctx->current_bam_bundle->received[ i ] ) ) {
      if( FD_UNLIKELY( first_missing_idx<0 ) ) first_missing_idx = (int)i;
      continue;
    }
    if( FD_UNLIKELY( !have_sig0 ) ) {
      fd_memcpy( sig0, ctx->current_bam_bundle->_txn[ i ]->txnp->payload + 1UL, sizeof(fd_ed25519_sig_t) );
      have_sig0 = 1;
    }
  }
  pack_tile_log_bam_drop( ctx,
                             "bundle_assembly",
                             reason==PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_NEW_SEQ_BEFORE_COMPLETE ? "bundle_assembly_abandon_new_seq_before_complete" :
                             reason==PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_LEADER_SLOT_END          ? "bundle_assembly_abandon_leader_slot_end" :
                                                                                                      "bundle_assembly_abandon_poh_timeout",
                             0U,
                             0U,
                             0U,
                             0,
                             ctx->current_bam_bundle->seq_id,
                             ctx->current_bam_bundle->txn_cnt,
                             "assembling",
                             ctx->current_bam_bundle->slot,
                             ctx->current_bam_bundle->max_schedule_slot,
                             ctx->current_bam_bundle->min_blockhash_slot,
                             pack_tile_current_bam_bundle_first_rx_ts_ns( ctx ),
                             1U,
                             1U,
                             0U,
                             0U,
                             ctx->current_bam_bundle->txn_received,
                             ctx->current_bam_bundle->txn_cnt,
                             (uint)( first_missing_idx>=0 ),
                             (uint)fd_int_if( first_missing_idx>=0, first_missing_idx, 0 ),
                             have_sig0 ? sig0 : NULL );

  FD_TEST( ctx->current_bam_bundle->txn_received!=ctx->current_bam_bundle->txn_cnt );

  fd_bam_bundle_result_t res = {
    .seq_id           = ctx->current_bam_bundle->seq_id,
    .slot             = ctx->current_bam_bundle->max_schedule_slot,
    .bundle_txn_cnt   = ctx->current_bam_bundle->txn_cnt,
    .scheduling_error = FD_BAM_SCHED_ERR_NONE,
  };
  fd_memset( res.sanitize_success, 1, res.bundle_txn_cnt );
  for( uchar i=0U; i<res.bundle_txn_cnt; i++ ) {
    if( FD_UNLIKELY( !ctx->current_bam_bundle->received[ i ] ) ) {
      res.transaction_err[ i ] = bam_types_TransactionErrorReason_SIGNATURE_FAILURE;
      res.transaction_err_count++;
    }
  }
  if( FD_UNLIKELY( queue_missing_result ) ) {
    /* during_frag cannot publish, so a newer seq that cuts off a partially
       received BAM bundle stages the result here for a later safe flush.
       Only partial bundles queue here, so bundle_txn_cnt remains a reliable
       non-zero sentinel until pack_tile_flush_pending_bam_result() runs. */
    ctx->pending_bam_result[0] = res;
  } else {
    pack_tile_publish_bam_result( ctx, stem, &res );
  }

  fd_pack_insert_bundle_cancel( ctx->pack, ctx->current_bam_bundle->bundle, ctx->current_bam_bundle->txn_cnt );
  ctx->current_bam_bundle->bundle = NULL;
}

static inline void
pack_tile_finish_leader_slot( fd_pack_ctx_t *     ctx,
                              fd_stem_context_t * stem,
                              long                now,
                              char const *        reason,
                              pack_tile_bam_bundle_assembly_abandon_reason_t bam_abandon_reason ) {
  if( FD_UNLIKELY( ctx->dump_bam_mode && ctx->leader_slot!=ULONG_MAX ) ) {
    long observed_ns = pack_tile_wallclock_from_ticks( ctx, now );
    FD_LOG_NOTICE(( "Firedancer slot end: pack_current_slot=%lu slot_end_ns=%ld observed_ns=%ld observed_minus_slot_end_ns=%ld reason=%s current_slot_fresh=%u", ctx->leader_slot, ctx->slot_end_ns, observed_ns, observed_ns - ctx->slot_end_ns, reason, (uint)ctx->bam_current_slot_fresh ));
  }

  ulong first_insert_result_idx = pack_tile_bam_first_event_result_idx( ctx->bam_first_insert_seen,
                                                                         ctx->bam_first_insert_minus_slot_end_ns );
  ulong first_schedule_result_idx = pack_tile_bam_first_event_result_idx( ctx->bam_first_schedule_seen,
                                                                           ctx->bam_first_schedule_minus_slot_end_ns );
  ctx->bam_first_insert_result_cnt[ first_insert_result_idx ]++;
  ctx->bam_first_schedule_result_cnt[ first_schedule_result_idx ]++;
  if( FD_LIKELY( ctx->bam_first_insert_seen ) ) {
    pack_tile_bam_record_first_event_distance( ctx->bam_first_insert_before_end_distance_nanos,
                                               ctx->bam_first_insert_after_end_distance_nanos,
                                               ctx->bam_first_insert_minus_slot_end_ns );
  }
  if( FD_LIKELY( ctx->bam_first_schedule_seen ) ) {
    pack_tile_bam_record_first_event_distance( ctx->bam_first_schedule_before_end_distance_nanos,
                                               ctx->bam_first_schedule_after_end_distance_nanos,
                                               ctx->bam_first_schedule_minus_slot_end_ns );
  }

  /* Flush any deferred BAM result staged from during_frag. */
  pack_tile_flush_pending_bam_result( ctx, stem );

  /* Once the slot closes, pending BAM work must survive against the next slot. */
  ulong next_slot = fd_ulong_sat_add( ctx->leader_slot, 1UL );
  pack_tile_evict_invalid_pending_bam_work( ctx, stem, next_slot );

  /* Cancel any bundle assembly that never reached a publishable result. */
  pack_tile_abandon_current_bam_bundle( ctx, stem, 0, bam_abandon_reason );
  if( FD_UNLIKELY( ctx->current_bundle->bundle ) ) {
    fd_pack_insert_bundle_cancel( ctx->pack, ctx->current_bundle->bundle, ctx->current_bundle->txn_cnt );
    ctx->current_bundle->bundle = NULL;
  }

  fd_done_packing_t * done_packing = fd_chunk_to_laddr( ctx->poh_out_mem, ctx->poh_out_chunk );
  get_done_packing( ctx, done_packing );
  fd_pack_end_block( ctx->pack );
  fd_pack_get_top_writers( ctx->pack, done_packing->limits_usage->top_writers );

  fd_stem_publish( stem, 1UL, fd_disco_bank_sig( ctx->leader_slot, ctx->pack_idx ), ctx->poh_out_chunk, sizeof(fd_done_packing_t), 0UL, 0UL, fd_frag_meta_ts_comp( fd_tickcount() ) );
  ctx->poh_out_chunk = fd_dcache_compact_next( ctx->poh_out_chunk, sizeof(fd_done_packing_t), ctx->poh_out_chunk0, ctx->poh_out_wmark );
  ctx->pack_idx++;

  log_end_block_metrics( ctx, now, reason, done_packing->limits_usage->block_cost );
  ctx->drain_banks         = 1;
  ctx->leader_slot         = ULONG_MAX;
  ctx->slot_microblock_cnt = 0UL;
  ctx->bam_current_slot_fresh = 0U;
  ctx->bam_first_insert_seen = 0U;
  ctx->bam_first_schedule_seen = 0U;
  ctx->bam_first_insert_minus_slot_end_ns = 0L;
  ctx->bam_first_schedule_minus_slot_end_ns = 0L;
  remove_ib( ctx );
}

static inline void
metrics_write( fd_pack_ctx_t * ctx ) {
  ulong bam_current_leader_slot_work_items[ FD_METRICS_ENUM_PACK_BAM_CURRENT_LEADER_SLOT_STATE_CNT ] = {0};
  ulong bam_current_leader_slot_work_transactions[ FD_METRICS_ENUM_PACK_BAM_CURRENT_LEADER_SLOT_STATE_CNT ] = {0};
  ulong bam_unresolved_work_items[ FD_METRICS_ENUM_PACK_BAM_UNRESOLVED_STATE_CNT ] = {
    [ FD_METRICS_ENUM_PACK_BAM_UNRESOLVED_STATE_V_RECEIVED_IDX ]             = ctx->bam_unresolved_work.received_items,
    [ FD_METRICS_ENUM_PACK_BAM_UNRESOLVED_STATE_V_ACCEPTED_IDX ]             = ctx->bam_unresolved_work.accepted_items,
    [ FD_METRICS_ENUM_PACK_BAM_UNRESOLVED_STATE_V_REJECTED_PRE_PENDING_IDX ] = ctx->bam_unresolved_work.rejected_pre_pending_items,
    [ FD_METRICS_ENUM_PACK_BAM_UNRESOLVED_STATE_V_PENDING_IDX ]              = ctx->bam_unresolved_work.pending_items,
    [ FD_METRICS_ENUM_PACK_BAM_UNRESOLVED_STATE_V_EVICTED_POST_PENDING_IDX ] = ctx->bam_unresolved_work.evicted_post_pending_items,
    [ FD_METRICS_ENUM_PACK_BAM_UNRESOLVED_STATE_V_SCHEDULED_IDX ]            = ctx->bam_unresolved_work.scheduled_items,
    [ FD_METRICS_ENUM_PACK_BAM_UNRESOLVED_STATE_V_LANDED_IDX ]               = ctx->bam_unresolved_work.landed_items
  };
  ulong bam_unresolved_work_transactions[ FD_METRICS_ENUM_PACK_BAM_UNRESOLVED_STATE_CNT ] = {
    [ FD_METRICS_ENUM_PACK_BAM_UNRESOLVED_STATE_V_RECEIVED_IDX ]             = ctx->bam_unresolved_work.received_txns,
    [ FD_METRICS_ENUM_PACK_BAM_UNRESOLVED_STATE_V_ACCEPTED_IDX ]             = ctx->bam_unresolved_work.accepted_txns,
    [ FD_METRICS_ENUM_PACK_BAM_UNRESOLVED_STATE_V_REJECTED_PRE_PENDING_IDX ] = ctx->bam_unresolved_work.rejected_pre_pending_txns,
    [ FD_METRICS_ENUM_PACK_BAM_UNRESOLVED_STATE_V_PENDING_IDX ]              = ctx->bam_unresolved_work.pending_txns,
    [ FD_METRICS_ENUM_PACK_BAM_UNRESOLVED_STATE_V_EVICTED_POST_PENDING_IDX ] = ctx->bam_unresolved_work.evicted_post_pending_txns,
    [ FD_METRICS_ENUM_PACK_BAM_UNRESOLVED_STATE_V_SCHEDULED_IDX ]            = ctx->bam_unresolved_work.scheduled_txns,
    [ FD_METRICS_ENUM_PACK_BAM_UNRESOLVED_STATE_V_LANDED_IDX ]               = ctx->bam_unresolved_work.landed_txns
  };
  if( FD_LIKELY( ctx->leader_slot!=ULONG_MAX ) ) {
    pack_bam_recent_slot_t const * recent = &ctx->bam_recent_slot[ ctx->leader_slot & ( FD_PACK_BAM_RECENT_SLOT_CNT - 1UL ) ];
    if( FD_LIKELY( recent->slot==ctx->leader_slot ) ) {
      bam_current_leader_slot_work_items[ FD_METRICS_ENUM_PACK_BAM_CURRENT_LEADER_SLOT_STATE_V_RECEIVED_IDX ]             = recent->received_items;
      bam_current_leader_slot_work_items[ FD_METRICS_ENUM_PACK_BAM_CURRENT_LEADER_SLOT_STATE_V_ACCEPTED_IDX ]             = recent->accepted_items;
      bam_current_leader_slot_work_items[ FD_METRICS_ENUM_PACK_BAM_CURRENT_LEADER_SLOT_STATE_V_REJECTED_PRE_PENDING_IDX ] = recent->rejected_pre_pending_items;
      bam_current_leader_slot_work_items[ FD_METRICS_ENUM_PACK_BAM_CURRENT_LEADER_SLOT_STATE_V_PENDING_IDX ]              = recent->pending_items;
      bam_current_leader_slot_work_items[ FD_METRICS_ENUM_PACK_BAM_CURRENT_LEADER_SLOT_STATE_V_EVICTED_POST_PENDING_IDX ] = recent->evicted_post_pending_items;
      bam_current_leader_slot_work_items[ FD_METRICS_ENUM_PACK_BAM_CURRENT_LEADER_SLOT_STATE_V_SCHEDULED_IDX ]            = recent->scheduled_items;
      bam_current_leader_slot_work_items[ FD_METRICS_ENUM_PACK_BAM_CURRENT_LEADER_SLOT_STATE_V_LANDED_IDX ]               = recent->landed_items;

      bam_current_leader_slot_work_transactions[ FD_METRICS_ENUM_PACK_BAM_CURRENT_LEADER_SLOT_STATE_V_RECEIVED_IDX ]             = recent->received_txns;
      bam_current_leader_slot_work_transactions[ FD_METRICS_ENUM_PACK_BAM_CURRENT_LEADER_SLOT_STATE_V_ACCEPTED_IDX ]             = recent->accepted_txns;
      bam_current_leader_slot_work_transactions[ FD_METRICS_ENUM_PACK_BAM_CURRENT_LEADER_SLOT_STATE_V_REJECTED_PRE_PENDING_IDX ] = recent->rejected_pre_pending_txns;
      bam_current_leader_slot_work_transactions[ FD_METRICS_ENUM_PACK_BAM_CURRENT_LEADER_SLOT_STATE_V_PENDING_IDX ]              = recent->pending_txns;
      bam_current_leader_slot_work_transactions[ FD_METRICS_ENUM_PACK_BAM_CURRENT_LEADER_SLOT_STATE_V_EVICTED_POST_PENDING_IDX ] = recent->evicted_post_pending_txns;
      bam_current_leader_slot_work_transactions[ FD_METRICS_ENUM_PACK_BAM_CURRENT_LEADER_SLOT_STATE_V_SCHEDULED_IDX ]            = recent->scheduled_txns;
      bam_current_leader_slot_work_transactions[ FD_METRICS_ENUM_PACK_BAM_CURRENT_LEADER_SLOT_STATE_V_LANDED_IDX ]               = recent->landed_txns;
    }
  }
  FD_MCNT_ENUM_COPY( PACK, TRANSACTION_INSERTED,          ctx->insert_result  );
  FD_MCNT_ENUM_COPY( PACK, BAM_BUNDLE_ASSEMBLY_ABANDON,   ctx->bam_bundle_assembly_abandon_cnt );
  FD_MCNT_ENUM_COPY( PACK, BAM_WORK_REJECTED_PRE_PENDING, ctx->bam_work_rejected_pre_pending_cnt );
  FD_MCNT_ENUM_COPY( PACK, BAM_PENDING_WORK_EVICTED,      ctx->bam_pending_work_evicted_cnt );
  FD_MCNT_ENUM_COPY( PACK, BAM_WORK_ITEMS,                ctx->bam_work_item_stage_cnt );
  FD_MCNT_ENUM_COPY( PACK, BAM_WORK_TRANSACTIONS,         ctx->bam_work_transaction_stage_cnt );
  FD_MCNT_ENUM_COPY( PACK, BAM_WORK_FIRST_OUTCOME,        ctx->bam_work_first_outcome_cnt );
  FD_MCNT_SET(      PACK, BAM_TRACKING_REJECTED,          ctx->bam_tracking_rejected_cnt );
  FD_MCNT_SET(      PACK, BAM_TRACKING_REJECTED_TRANSACTIONS, ctx->bam_tracking_rejected_txn_cnt );
  FD_MGAUGE_SET(     PACK, BAM_PENDING_WORK_COUNT,        ctx->bam_pending_work_cnt );
  FD_MGAUGE_SET(     PACK, LEADER_SLOT,                   ctx->leader_slot==ULONG_MAX ? 0UL : ctx->leader_slot );
  FD_MGAUGE_SET(     PACK, LEADER_SLOT_END_NANOS,         ctx->leader_slot==ULONG_MAX ? 0UL : (ulong)ctx->slot_end_ns );
  FD_MCNT_ENUM_COPY( PACK, BAM_LEADER_SLOT_FIRST_INSERT_RESULT,   ctx->bam_first_insert_result_cnt );
  FD_MCNT_ENUM_COPY( PACK, BAM_LEADER_SLOT_FIRST_SCHEDULE_RESULT, ctx->bam_first_schedule_result_cnt );
  FD_MGAUGE_ENUM_COPY( PACK, BAM_CURRENT_LEADER_SLOT_WORK_ITEMS,        bam_current_leader_slot_work_items );
  FD_MGAUGE_ENUM_COPY( PACK, BAM_CURRENT_LEADER_SLOT_WORK_TRANSACTIONS, bam_current_leader_slot_work_transactions );
  FD_MGAUGE_ENUM_COPY( PACK, BAM_UNRESOLVED_WORK_ITEMS,               bam_unresolved_work_items );
  FD_MGAUGE_ENUM_COPY( PACK, BAM_UNRESOLVED_WORK_TRANSACTIONS,        bam_unresolved_work_transactions );
  FD_MHIST_COPY(     PACK, BAM_LEADER_SLOT_FIRST_INSERT_BEFORE_END_DISTANCE_NANOS,   ctx->bam_first_insert_before_end_distance_nanos );
  FD_MHIST_COPY(     PACK, BAM_LEADER_SLOT_FIRST_INSERT_AFTER_END_DISTANCE_NANOS,    ctx->bam_first_insert_after_end_distance_nanos );
  FD_MHIST_COPY(     PACK, BAM_LEADER_SLOT_FIRST_SCHEDULE_BEFORE_END_DISTANCE_NANOS, ctx->bam_first_schedule_before_end_distance_nanos );
  FD_MHIST_COPY(     PACK, BAM_LEADER_SLOT_FIRST_SCHEDULE_AFTER_END_DISTANCE_NANOS,  ctx->bam_first_schedule_after_end_distance_nanos );
  FD_MHIST_COPY(     PACK, BAM_WORK_RX_TO_FIRST_OUTCOME_NANOS,              ctx->bam_work_rx_to_first_outcome_nanos );
  FD_MCNT_ENUM_COPY( PACK, METRIC_TIMING,        ((ulong*)ctx->metric_timing) );
  FD_MCNT_ENUM_COPY( PACK, BUNDLE_CRANK_STATUS,           ctx->crank->metrics );
  FD_MHIST_COPY( PACK, SCHEDULE_MICROBLOCK_DURATION_SECONDS, ctx->schedule_duration );
  FD_MHIST_COPY( PACK, NO_SCHED_MICROBLOCK_DURATION_SECONDS, ctx->no_sched_duration );
  FD_MHIST_COPY( PACK, INSERT_TRANSACTION_DURATION_SECONDS,  ctx->insert_duration   );
  FD_MHIST_COPY( PACK, COMPLETE_MICROBLOCK_DURATION_SECONDS, ctx->complete_duration );

  fd_pack_metrics_write( ctx->pack );
}

static inline void
during_housekeeping( fd_pack_ctx_t * ctx ) {
  ctx->approx_wallclock_ns = fd_log_wallclock();
  ctx->approx_tickcount = fd_tickcount();

  if( FD_UNLIKELY( ctx->crank->enabled && fd_keyswitch_state_query( ctx->crank->keyswitch )==FD_KEYSWITCH_STATE_SWITCH_PENDING ) ) {
    fd_memcpy( ctx->crank->identity_pubkey, ctx->crank->keyswitch->bytes, 32UL );
    fd_keyswitch_state( ctx->crank->keyswitch, FD_KEYSWITCH_STATE_COMPLETED );
  }
}

static inline void
before_credit( fd_pack_ctx_t *     ctx,
               fd_stem_context_t * stem,
               int *               charge_busy ) {
  (void)stem;

  if( FD_UNLIKELY( (ctx->cur_spot!=NULL) & (ctx->bundle_kind==PACK_TILE_BUNDLE_KIND_NONE) ) ) {
    *charge_busy = 1;
    fd_txn_p_t const * txnp = ctx->cur_spot->txnp;

    /* If we were overrun while processing a frag from an in, then
       cur_spot is left dangling and not cleaned up, so clean it up here
       (by returning the slot to the pool of free slots).  If the last
       transaction was a bundle, then we don't want to return it.  When
       we try to process the first transaction in the next bundle, we'll
       see we never got the full bundle and cancel the whole last
       bundle, returning all the storage to the pool. */
#if FD_PACK_USE_EXTRA_STORAGE
    if( FD_LIKELY( !ctx->insert_to_extra ) ) {
      if( FD_UNLIKELY( txnp->source_tpu==FD_TXN_M_TPU_SOURCE_BAM ) ) {
        pack_tile_log_bam_drop( ctx,
                                   "overrun",
                                   "overrun_cleanup",
                                   0U,
                                   0U,
                                   0U,
                                   0,
                                   txnp->bam.seq_id,
                                   1U,
                                   "staging",
                                   pack_tile_bam_funnel_slot( ctx, ctx->bam_txn_max_schedule_slot ),
                                   ctx->bam_txn_max_schedule_slot,
                                   ULONG_MAX,
                                   txnp->scheduler_arrival_time_nanos,
                                   1U,
                                   (uint)txnp->bam.revert_on_error,
                                   1U,
                                   (uint)txnp->bam.batch_idx,
                                   1UL,
                                   1UL,
                                   0U,
                                   0U,
                                   txnp->payload + 1UL );
      }
      fd_pack_insert_txn_cancel( ctx->pack, ctx->cur_spot );
    } else {
      fd_pack_extra_txn_t const * extra = extra_txn_deq_peek_tail( ctx->extra_txn_deq );
      if( FD_UNLIKELY( extra->txn.txnp->source_tpu==FD_TXN_M_TPU_SOURCE_BAM ) ) {
        pack_tile_log_bam_drop( ctx,
                                   "overrun",
                                   "overrun_cleanup",
                                   0U,
                                   0U,
                                   0U,
                                   0,
                                   extra->txn.txnp->bam.seq_id,
                                   1U,
                                   "extra_buffer",
                                   extra->bam_funnel_slot,
                                   extra->bam_max_schedule_slot,
                                   extra->txn.txnp->blockhash_slot,
                                   extra->txn.txnp->scheduler_arrival_time_nanos,
                                   1U,
                                   (uint)extra->txn.txnp->bam.revert_on_error,
                                   1U,
                                   (uint)extra->txn.txnp->bam.batch_idx,
                                   1UL,
                                   1UL,
                                   0U,
                                   0U,
                                   extra->txn.txnp->payload + 1UL );
      }
      extra_txn_deq_remove_tail( ctx->extra_txn_deq );
    }
#else
    if( FD_UNLIKELY( txnp->source_tpu==FD_TXN_M_TPU_SOURCE_BAM ) ) {
      pack_tile_log_bam_drop( ctx,
                                 "overrun",
                                 "overrun_cleanup",
                                 0U,
                                 0U,
                                 0U,
                                 0,
                                 txnp->bam.seq_id,
                                 1U,
                                 "staging",
                                 pack_tile_bam_funnel_slot( ctx, ctx->bam_txn_max_schedule_slot ),
                                 ctx->bam_txn_max_schedule_slot,
                                 ULONG_MAX,
                                 txnp->scheduler_arrival_time_nanos,
                                 1U,
                                 (uint)txnp->bam.revert_on_error,
                                 1U,
                                 (uint)txnp->bam.batch_idx,
                                 1UL,
                                 1UL,
                                 0U,
                                 0U,
                                 txnp->payload + 1UL );
    }
    fd_pack_insert_txn_cancel( ctx->pack, ctx->cur_spot );
#endif
    ctx->cur_spot = NULL;
  }
}

#if FD_PACK_USE_EXTRA_STORAGE
/* insert_from_extra: helper method to pop the transaction at the head
   off the extra txn deque and insert it into pack.  Requires that
   ctx->extra_txn_deq is non-empty, but it's okay to call it if pack is
   full.  Returns the result of fd_pack_insert_txn_fini or -1 if the
   transaction was invalidated before reinsertion. */
static inline int
insert_from_extra( fd_pack_ctx_t *     ctx,
                   fd_stem_context_t * stem ) {
  fd_txn_e_t             * spot          = fd_pack_insert_txn_init( ctx->pack );
  fd_pack_extra_txn_t const * insert     = extra_txn_deq_peek_head( ctx->extra_txn_deq );
  fd_txn_e_t       const * insert_txne   = &insert->txn;
  fd_txn_t         const * insert_txn    = TXN(insert_txne->txnp);
  fd_txn_p_t       const * insert_txnp   = insert_txne->txnp;
  _Bool                    is_bam        = insert_txnp->source_tpu==FD_TXN_M_TPU_SOURCE_BAM;
  uint                     bam_seq_id    = insert_txnp->bam.seq_id;
  ulong                    bam_slot      = insert->bam_max_schedule_slot;
  ulong                    bam_funnel_slot = insert->bam_funnel_slot;
  ulong                    blockhash_slot = insert_txnp->blockhash_slot;
  fd_ed25519_sig_t         bam_sig[1]    = {0};

  if( FD_UNLIKELY( is_bam ) ) {
    pack_tile_bam_invalid_reason_t invalid_reason =
        pack_tile_bam_invalid_reason( pack_tile_bam_best_known_slot( ctx ), bam_slot, blockhash_slot );
    if( FD_UNLIKELY( invalid_reason!=PACK_TILE_BAM_INVALID_NONE ) ) {
      fd_pack_insert_txn_cancel( ctx->pack, spot );
      extra_txn_deq_remove_head( ctx->extra_txn_deq );
      pack_tile_log_bam_drop( ctx,
                                 "pre_pending_validation",
                                 invalid_reason==PACK_TILE_BAM_INVALID_OUTSIDE_SLOT
                                     ? "rejected_pre_pending_outside_slot"
                                     : "rejected_pre_pending_blockhash_expired",
                                 1U,
                                 (uint)invalid_reason,
                                 0U,
                                 0,
                                 bam_seq_id,
                                 1U,
                                 "extra_buffer",
                                 bam_funnel_slot,
                                 bam_slot,
                                 blockhash_slot,
                                 insert_txnp->scheduler_arrival_time_nanos,
                                 1U,
                                 (uint)insert_txnp->bam.revert_on_error,
                                 1U,
                                 (uint)insert_txnp->bam.batch_idx,
                                 1UL,
                                 1UL,
                                 0U,
                                 0U,
                                 insert_txnp->payload + 1UL );
      pack_tile_record_bam_invalid_reason_count( ctx->bam_work_rejected_pre_pending_cnt, invalid_reason, 1U );
      pack_tile_note_bam_first_outcome( ctx,
                                        FD_METRICS_ENUM_PACK_BAM_WORK_FIRST_OUTCOME_V_REJECTED_PRE_PENDING_IDX,
                                        insert_txnp->scheduler_arrival_time_nanos,
                                        pack_tile_now_ns( ctx ) );
      pack_tile_note_bam_rejected_pre_pending( ctx, bam_funnel_slot, 1UL, 1UL );
      pack_tile_publish_bam_invalid_result( ctx, stem, bam_seq_id, bam_slot, 1U, invalid_reason );
      return -1;
    }
    fd_memcpy( bam_sig, insert_txnp->payload + 1UL, sizeof(fd_ed25519_sig_t) );
  }

  fd_memcpy( spot->txnp->payload, insert_txnp->payload, insert_txnp->payload_sz                                                     );
  fd_memcpy( TXN(spot->txnp),     insert_txn,          fd_txn_footprint( insert_txn->instr_cnt, insert_txn->addr_table_lookup_cnt ) );
  fd_memcpy( spot->alt_accts,     insert_txne->alt_accts, insert_txn->addr_table_adtl_cnt*sizeof(fd_acct_addr_t)                    );
  spot->txnp->payload_sz = insert_txnp->payload_sz;
  spot->txnp->source_tpu  = insert_txnp->source_tpu;
  spot->txnp->source_ipv4 = insert_txnp->source_ipv4;
  spot->txnp->flags       = insert_txnp->flags;
  spot->txnp->bam         = insert_txnp->bam;
  spot->txnp->scheduler_arrival_time_nanos = insert_txnp->scheduler_arrival_time_nanos;
  extra_txn_deq_remove_head( ctx->extra_txn_deq );

  ulong deleted;
  long insert_duration = -fd_tickcount();
  int result = fd_pack_insert_txn_fini( ctx->pack, spot, blockhash_slot, &deleted );
  insert_duration      += fd_tickcount();

  FD_MCNT_INC( PACK, TRANSACTION_DELETED, deleted );
  ctx->insert_result[ result + FD_PACK_INSERT_RETVAL_OFF ]++;
  fd_histf_sample( ctx->insert_duration, (ulong)insert_duration );
  if( FD_LIKELY( !is_bam ) ) {
    FD_MCNT_INC( PACK, TRANSACTION_INSERTED_FROM_EXTRA, 1UL );
    return result;
  }

  if( FD_UNLIKELY( result<0 ) ) {
    pack_tile_log_bam_drop( ctx,
                               "insert",
                               pack_tile_bam_pack_insert_reason_cstr( result ),
                               0U,
                               0U,
                               1U,
                               result,
                               bam_seq_id,
                               1U,
                               "extra_buffer",
                               bam_funnel_slot,
                               bam_slot,
                               blockhash_slot,
                               insert_txnp->scheduler_arrival_time_nanos,
                               1U,
                               (uint)insert_txnp->bam.revert_on_error,
                               1U,
                               (uint)insert_txnp->bam.batch_idx,
                               1UL,
                               1UL,
                               0U,
                               0U,
                               insert_txnp->payload + 1UL );
    pack_tile_note_bam_first_outcome( ctx,
                                      FD_METRICS_ENUM_PACK_BAM_WORK_FIRST_OUTCOME_V_REJECTED_PRE_PENDING_IDX,
                                      insert_txnp->scheduler_arrival_time_nanos,
                                      pack_tile_now_ns( ctx ) );
    pack_tile_note_bam_rejected_pre_pending( ctx, bam_funnel_slot, 1UL, 1UL );
    fd_bam_bundle_result_t res = {
      .seq_id           = bam_seq_id,
      .slot             = bam_slot,
      .bundle_txn_cnt   = 1U,
      .scheduling_error = FD_BAM_SCHED_ERR_NONE,
    };
    res.transaction_err[ 0 ] = pack_tile_bam_txn_err_from_pack_insert( result );
    res.transaction_err_count = 1U;
    res.sanitize_success[ 0 ] = 1U;
    pack_tile_publish_bam_result( ctx, stem, &res );
  } else {
    if( FD_UNLIKELY( !pack_tile_track_bam_work( ctx,
                                                bam_sig[ 0 ],
                                                insert_txnp->scheduler_arrival_time_nanos,
                                                bam_seq_id,
                                                bam_funnel_slot,
                                                bam_slot,
                                                blockhash_slot,
                                                1U ) ) ) {
      pack_tile_publish_bam_tracking_reject( ctx, stem, bam_sig[ 0 ], insert_txnp->scheduler_arrival_time_nanos, bam_seq_id, bam_funnel_slot, bam_slot, blockhash_slot, 1U, (uint)insert_txnp->bam.revert_on_error, 1U );
    } else {
      pack_tile_note_bam_accepted( ctx, bam_funnel_slot, 1UL, 1UL );
      pack_tile_note_first_bam_insert( ctx, stem, pack_tile_now_ns( ctx ), pack_tile_bam_fresh_slot_for_hint( ctx, bam_slot ) );
    }
  }
  FD_MCNT_INC( PACK, TRANSACTION_INSERTED_FROM_EXTRA, 1UL );
  return result;
}
#endif

static inline void
after_credit( fd_pack_ctx_t *     ctx,
              fd_stem_context_t * stem,
              int *               opt_poll_in,
              int *               charge_busy ) {
  (void)opt_poll_in;

  if( FD_UNLIKELY( (ctx->skip_cnt--)>0L ) ) return; /* It would take ages for this to hit LONG_MIN */

  long now = fd_tickcount();

  int pacing_bank_cnt = (int)fd_pack_pacing_enabled_bank_cnt( ctx->pacer, now );

  ulong bank_cnt = ctx->bank_cnt;


  /* If any banks are busy, check one of the busy ones see if it is
     still busy. */
  if( FD_LIKELY( ctx->bank_idle_bitset!=fd_ulong_mask_lsb( (int)bank_cnt ) ) ) {
    int   poll_cursor = ctx->poll_cursor;
    ulong busy_bitset = (~ctx->bank_idle_bitset) & fd_ulong_mask_lsb( (int)bank_cnt );

    /* Suppose bank_cnt is 4 and idle_bitset looks something like this
       (pretending it's a uchar):
                0000 1001
                       ^ busy cursor is 1
       Then busy_bitset is
                0000 0110
       Rotate it right by 2 bits
                1000 0001
       Find lsb returns 0, so busy cursor remains 2, and we poll bank 2.

       If instead idle_bitset were
                0000 1110
                       ^
       The rotated version would be
                0100 0000
       Find lsb will return 6, so busy cursor would be set to 0, and
       we'd poll bank 0, which is the right one. */
    poll_cursor++;
    poll_cursor = (poll_cursor + fd_ulong_find_lsb( fd_ulong_rotate_right( busy_bitset, (poll_cursor&63) ) )) & 63;

    if( FD_UNLIKELY(
        /* if microblock duration is 0, bypass the bank_ready_at check
           to avoid a potential cache miss.  Can't use an ifdef here
           because FD_UNLIKELY is a macro, but the compiler should
           eliminate the check easily. */
        ( (MICROBLOCK_DURATION_NS==0L) || (ctx->bank_ready_at[poll_cursor]<now) ) &&
        (fd_fseq_query( ctx->bank_current[poll_cursor] )==ctx->bank_expect[poll_cursor]) ) ) {
      *charge_busy = 1;
      ctx->bank_idle_bitset |= 1UL<<poll_cursor;

      long complete_duration = -fd_tickcount();
      int completed = fd_pack_microblock_complete( ctx->pack, (ulong)poll_cursor );
      complete_duration      += fd_tickcount();
      if( FD_LIKELY( completed ) ) fd_histf_sample( ctx->complete_duration, (ulong)complete_duration );
    }

    ctx->poll_cursor = poll_cursor;
  }


  /* If we time out on our slot, then stop being leader.  This can only
     happen in the first after_credit after a housekeeping. */
  if( FD_UNLIKELY( ctx->approx_wallclock_ns>=ctx->slot_end_ns && ctx->leader_slot!=ULONG_MAX ) ) {
    *charge_busy = 1;
    pack_tile_finish_leader_slot( ctx, stem, now, "time", PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_POH_TIMEOUT );

    update_metric_state( ctx, now, FD_PACK_METRIC_STATE_LEADER,       0 );
    update_metric_state( ctx, now, FD_PACK_METRIC_STATE_BANKS,        0 );
    update_metric_state( ctx, now, FD_PACK_METRIC_STATE_MICROBLOCKS,  0 );
    return;
  }

  /* Am I leader? If not, see about inserting at most one transaction
     from extra storage.  It's important not to insert too many
     transactions here, or we won't end up servicing dedup_pack enough.
     If extra storage is empty or pack is full, do nothing. */
  if( FD_UNLIKELY( ctx->leader_slot==ULONG_MAX ) ) {
#if FD_PACK_USE_EXTRA_STORAGE
    if( FD_UNLIKELY( !extra_txn_deq_empty( ctx->extra_txn_deq ) &&
         fd_pack_avail_txn_cnt( ctx->pack )<ctx->max_pending_transactions ) ) {
      *charge_busy = 1;

      int result = insert_from_extra( ctx, stem );
      if( FD_LIKELY( result>=0 ) ) ctx->last_successful_insert = now;
    }
#endif
    return;
  }

  pack_tile_publish_bam_leader_state( ctx, stem );

  /* Am I in drain mode?  If so, check if I can exit it */
  if( FD_UNLIKELY( ctx->drain_banks ) ) {
    if( FD_LIKELY( ctx->bank_idle_bitset==fd_ulong_mask_lsb( (int)bank_cnt ) ) ) {
      ctx->drain_banks = 0;

      /* Pack notifies poh when banks are drained so that poh can
         relinquish pack's ownership over the slot bank (by decrementing
         its Arc). We do this by sending a ULONG_MAX sig over the
         pack_poh mcache.

         TODO: This is only needed for Frankendancer, not Firedancer,
         which manages bank lifetime different. */
      fd_stem_publish( stem, 1UL, ULONG_MAX, 0UL, 0UL, 0UL, 0UL, fd_frag_meta_ts_comp( fd_tickcount() ) );
    } else {
      return;
    }
  }

  /* Have I sent the max allowed microblocks? Nothing to do. */
  if( FD_UNLIKELY( ctx->slot_microblock_cnt>=ctx->slot_max_microblocks ) ) return;

  /* Do I have enough transactions and/or have I waited enough time? */
  if( FD_UNLIKELY( (ulong)(now-ctx->last_successful_insert) <
        ctx->wait_duration_ticks[ fd_ulong_min( fd_pack_avail_txn_cnt( ctx->pack ), MAX_TXN_PER_MICROBLOCK ) ] ) ) {
    update_metric_state( ctx, now, FD_PACK_METRIC_STATE_TRANSACTIONS, 0 );
    return;
  }

  int any_ready     = 0;
  int any_scheduled = 0;

  *charge_busy = 1;

  if( FD_LIKELY( ctx->crank->enabled ) ) {
    block_builder_info_t const * top_meta = fd_pack_peek_bundle_meta( ctx->pack );
    if( FD_UNLIKELY( top_meta ) ) {
      /* Have bundles, in a reasonable state to crank. */

      fd_txn_e_t * _bundle[ 1UL ];
      fd_txn_e_t * const * bundle = fd_pack_insert_bundle_init( ctx->pack, _bundle, 1UL );

      ulong txn_sz = fd_bundle_crank_generate( ctx->crank->gen, ctx->crank->prev_config, top_meta->commission_pubkey,
          ctx->crank->identity_pubkey, ctx->crank->tip_receiver_owner, ctx->crank->epoch, top_meta->commission,
          bundle[0]->txnp->payload, TXN( bundle[0]->txnp ) );

      if( FD_LIKELY( txn_sz==0UL ) ) { /* Everything in good shape! */
        fd_pack_insert_bundle_cancel( ctx->pack, bundle, 1UL );
        fd_pack_set_initializer_bundles_ready( ctx->pack );
        ctx->crank->metrics[ 0 ]++; /* BUNDLE_CRANK_STATUS_NOT_NEEDED */
      }
      else if( FD_LIKELY( txn_sz<ULONG_MAX ) ) {
        bundle[0]->txnp->payload_sz  = (ushort)txn_sz;
        bundle[0]->txnp->source_tpu  = FD_TXN_M_TPU_SOURCE_BUNDLE;
        bundle[0]->txnp->source_ipv4 = 0; /* not applicable */
        bundle[0]->txnp->scheduler_arrival_time_nanos = ctx->approx_wallclock_ns + (long)((double)(fd_tickcount() - ctx->approx_tickcount) / ctx->ticks_per_ns);
        memcpy( bundle[0]->txnp->payload+TXN(bundle[0]->txnp)->recent_blockhash_off, ctx->crank->recent_blockhash, 32UL );

        fd_keyguard_client_sign( ctx->crank->keyguard_client, bundle[0]->txnp->payload+1UL,
            bundle[0]->txnp->payload+65UL, txn_sz-65UL, FD_KEYGUARD_SIGN_TYPE_ED25519 );

        memcpy( ctx->crank->last_sig, bundle[0]->txnp->payload+1UL, 64UL );

        ctx->crank->ib_inserted = 1;
        ulong deleted;
        int retval = fd_pack_insert_bundle_fini( ctx->pack, bundle, 1UL, ctx->leader_slot-1UL, 1, NULL, &deleted );
        FD_MCNT_INC( PACK, TRANSACTION_DELETED, deleted );
        ctx->insert_result[ retval + FD_PACK_INSERT_RETVAL_OFF ]++;
        if( FD_UNLIKELY( retval<0 ) ) {
          ctx->crank->metrics[ 3 ]++; /* BUNDLE_CRANK_STATUS_INSERTION_FAILED */
          FD_LOG_WARNING(( "inserting initializer bundle returned %i", retval ));
        } else {
          /* Update the cached copy of the on-chain state.  This seems a
             little dangerous, since we're updating it as if the bundle
             succeeded without knowing if that's true, but here's why
             it's safe:

             From now until we get the rebate call for this initializer
             bundle (which lets us know if it succeeded or failed), pack
             will be in [Pending] state, which means peek_bundle_meta
             will return NULL, so we won't read this state.

             Then, if the initializer bundle failed, we'll go into
             [Failed] IB state until the end of the block, which will
             cause top_meta to remain NULL so we don't read these values
             again.

             Otherwise, the initializer bundle succeeded, which means
             that these are the right values to use. */
          fd_bundle_crank_apply( ctx->crank->gen, ctx->crank->prev_config, top_meta->commission_pubkey,
                                 ctx->crank->tip_receiver_owner, ctx->crank->epoch, top_meta->commission );
          ctx->crank->metrics[ 1 ]++; /* BUNDLE_CRANK_STATUS_INSERTED */
        }
      } else {
        /* Already logged a warning in this case */
        fd_pack_insert_bundle_cancel( ctx->pack, bundle, 1UL );
        ctx->crank->metrics[ 2 ]++; /* BUNDLE_CRANK_STATUS_CREATION_FAILED' */
      }
    }
  }

  /* Try to schedule the next microblock. */
  if( FD_LIKELY( ctx->bank_idle_bitset ) ) { /* Optimize for schedule */
    any_ready = 1;

    int i = fd_ulong_find_lsb( ctx->bank_idle_bitset );

    int flags;

    switch( ctx->strategy ) {
      default:
      case FD_PACK_STRATEGY_PERF:
        flags = FD_PACK_SCHEDULE_VOTE | FD_PACK_SCHEDULE_BUNDLE | FD_PACK_SCHEDULE_TXN;
        break;
      case FD_PACK_STRATEGY_BALANCED:
        /* We want to exempt votes from pacing, so we always allow
           scheduling votes.  It doesn't really make much sense to pace
           bundles, because they get scheduled in FIFO order.  However,
           we keep pacing for normal transactions.  For example, if
           pacing_bank_cnt is 0, then pack won't schedule normal
           transactions to any bank tile. */
        flags = FD_PACK_SCHEDULE_VOTE | fd_int_if( i==0,              FD_PACK_SCHEDULE_BUNDLE, 0 )
                                      | fd_int_if( i<pacing_bank_cnt, FD_PACK_SCHEDULE_TXN,    0 );
        break;
      case FD_PACK_STRATEGY_BUNDLE:
        flags = FD_PACK_SCHEDULE_VOTE | FD_PACK_SCHEDULE_BUNDLE
                                      | fd_int_if( ctx->slot_end_ns - ctx->approx_wallclock_ns<50000000L, FD_PACK_SCHEDULE_TXN,  0 );
        break;
    }

    fd_txn_p_t * microblock_dst = fd_chunk_to_laddr( ctx->bank_out_mem, ctx->bank_out_chunk );
    long schedule_duration = -fd_tickcount();
    ulong schedule_cnt = fd_pack_schedule_next_microblock( ctx->pack, CUS_PER_MICROBLOCK, VOTE_FRACTION, (ulong)i, flags, microblock_dst );
    schedule_duration      += fd_tickcount();
    fd_histf_sample( (schedule_cnt>0UL) ? ctx->schedule_duration : ctx->no_sched_duration, (ulong)schedule_duration );

    if( FD_LIKELY( schedule_cnt ) ) {
      any_scheduled = 1;
      long  now2   = fd_tickcount();
      long  now2_ns = pack_tile_wallclock_from_ticks( ctx, now2 );
      ulong tsorig = (ulong)fd_frag_meta_ts_comp( now  ); /* A bound on when we observed bank was idle */
      ulong tspub  = (ulong)fd_frag_meta_ts_comp( now2 );
      ulong chunk  = ctx->bank_out_chunk;
      ulong msg_sz = schedule_cnt*sizeof(fd_txn_p_t);
      fd_microblock_bank_trailer_t * trailer = (fd_microblock_bank_trailer_t*)(microblock_dst+schedule_cnt);
      trailer->bank = ctx->leader_bank;
      trailer->bank_idx = ctx->leader_bank_idx;
      trailer->microblock_idx = ctx->slot_microblock_cnt;
      trailer->pack_idx = ctx->pack_idx;
      trailer->pack_txn_idx = ctx->pack_txn_cnt;
      trailer->is_bundle = !!(microblock_dst->flags & FD_TXN_P_FLAGS_BUNDLE);

      ulong sig = fd_disco_poh_sig( ctx->leader_slot, POH_PKT_TYPE_MICROBLOCK, (ulong)i );
      fd_stem_publish( stem, 0UL, sig, chunk, msg_sz+sizeof(fd_microblock_bank_trailer_t), 0UL, tsorig, tspub );
      ctx->bank_expect[ i ] = stem->seqs[0]-1UL;
      ctx->bank_ready_at[i] = now2 + (long)ctx->microblock_duration_ticks;
      ctx->bank_out_chunk = fd_dcache_compact_next( ctx->bank_out_chunk, msg_sz+sizeof(fd_microblock_bank_trailer_t), ctx->bank_out_chunk0, ctx->bank_out_wmark );
      ctx->slot_microblock_cnt += fd_ulong_if( trailer->is_bundle, schedule_cnt, 1UL );
      ctx->pack_idx += fd_uint_if( trailer->is_bundle, (uint)schedule_cnt, 1U );
      ctx->pack_txn_cnt += schedule_cnt;

      ctx->bank_idle_bitset = fd_ulong_pop_lsb( ctx->bank_idle_bitset );
      ctx->skip_cnt         = (long)schedule_cnt * fd_long_if( ctx->use_consumed_cus, (long)bank_cnt/2L, 1L );
      fd_pack_pacing_update_consumed_cus( ctx->pacer, fd_pack_current_block_cost( ctx->pack ), now2 );

      ctx->last_sched_metrics->time = now2;
      fd_pack_get_sched_metrics( ctx->pack, ctx->last_sched_metrics->sched_results );

      /* If we're using CU rebates, then we have one in for each bank in
        addition to the two normal ones. We want to skip schedule attempts
        for (bank_cnt + 1) link polls after a successful schedule attempt.
        */
      fd_long_store_if( ctx->use_consumed_cus, &(ctx->skip_cnt), (long)(ctx->bank_cnt + 1) );
      for( ulong j=0UL; j<schedule_cnt; j++ ) {
        fd_txn_p_t const * txnp = microblock_dst + j;
        if( FD_UNLIKELY( txnp->source_tpu!=FD_TXN_M_TPU_SOURCE_BAM ) ) continue;
        if( FD_UNLIKELY( txnp->bam.revert_on_error && txnp->bam.batch_idx ) ) continue;
        if( FD_UNLIKELY( !ctx->bam_first_schedule_seen ) ) {
          ctx->bam_first_schedule_seen = 1U;
          ctx->bam_first_schedule_minus_slot_end_ns = now2_ns - ctx->slot_end_ns;
        }

        ulong work_idx = pack_tile_bam_work_find_by_sig0( ctx, (fd_ed25519_sig_t const *)(txnp->payload + 1UL) );
        if( FD_UNLIKELY( work_idx>=ctx->bam_work_cnt ) ) continue;

        pack_bam_work_t * item = &ctx->bam_work[ work_idx ];
        if( FD_UNLIKELY( item->state!=PACK_BAM_WORK_STATE_PENDING ) ) continue;

        if( FD_UNLIKELY( item->slot==ULONG_MAX ) ) {
          ctx->bam_unresolved_work.scheduled_items += 1UL;
          ctx->bam_unresolved_work.scheduled_txns  += item->txn_cnt;
        } else {
          pack_bam_recent_slot_t * recent = pack_tile_bam_recent_slot_prepare( ctx, item->slot );
          recent->scheduled_items += 1UL;
          recent->scheduled_txns  += item->txn_cnt;
        }
        pack_tile_note_bam_work_stage( ctx, FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_V_SCHEDULED_IDX, 1UL, item->txn_cnt );
        pack_tile_bam_recent_slot_sub_pending( ctx, item->slot, 1UL, item->txn_cnt );
        pack_tile_note_bam_first_outcome( ctx,
                                          FD_METRICS_ENUM_PACK_BAM_WORK_FIRST_OUTCOME_V_SCHEDULED_IDX,
                                          item->first_rx_ts_ns,
                                          now2_ns );

        item->state = PACK_BAM_WORK_STATE_SCHEDULED;
        item->remaining_txn_cnt = item->txn_cnt;
        ctx->bam_pending_work_cnt--;
        ctx->bam_scheduled_work_cnt++;
      }

      pack_tile_publish_bam_leader_state( ctx, stem );
    }
  }

  update_metric_state( ctx, now, FD_PACK_METRIC_STATE_BANKS,       any_ready     );
  update_metric_state( ctx, now, FD_PACK_METRIC_STATE_MICROBLOCKS, any_scheduled );
  now = fd_tickcount();
  update_metric_state( ctx, now, FD_PACK_METRIC_STATE_TRANSACTIONS, fd_pack_avail_txn_cnt( ctx->pack )>0 );

#if FD_PACK_USE_EXTRA_STORAGE
  if( FD_UNLIKELY( !extra_txn_deq_empty( ctx->extra_txn_deq ) ) ) {
    /* Don't start pulling from the extra storage until the available
       transaction count drops below half. */
    ulong avail_space   = (ulong)fd_long_max( 0L, (long)(ctx->max_pending_transactions>>1)-(long)fd_pack_avail_txn_cnt( ctx->pack ) );
    ulong qty_to_insert = fd_ulong_min( 10UL, fd_ulong_min( extra_txn_deq_cnt( ctx->extra_txn_deq ), avail_space ) );
    int any_successes = 0;
    for( ulong i=0UL; i<qty_to_insert; i++ ) any_successes |= (0<=insert_from_extra( ctx, stem ));
    if( FD_LIKELY( any_successes ) ) ctx->last_successful_insert = now;
  }
#endif

  /* Did we send the maximum allowed microblocks? Then end the slot. */
  if( FD_UNLIKELY( ctx->slot_microblock_cnt==ctx->slot_max_microblocks )) {
    update_metric_state( ctx, now, FD_PACK_METRIC_STATE_LEADER,       0 );
    update_metric_state( ctx, now, FD_PACK_METRIC_STATE_BANKS,        0 );
    update_metric_state( ctx, now, FD_PACK_METRIC_STATE_MICROBLOCKS,  0 );
    /* The pack object also does this accounting and increases this
       metric, but we end the slot early so won't see it unless we also
       increment it here. */
    FD_MCNT_INC( PACK, MICROBLOCK_PER_BLOCK_LIMIT, 1UL );
    pack_tile_finish_leader_slot( ctx, stem, now, "microblock", PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_LEADER_SLOT_END );
  }
}


/* At this point, we have started receiving frag seq with details in
    mline at time now.  Speculatively process it here. */

static inline void
during_frag( fd_pack_ctx_t * ctx,
             ulong           in_idx,
             ulong           seq FD_PARAM_UNUSED,
             ulong           sig,
             ulong           chunk,
             ulong           sz,
             ulong           ctl FD_PARAM_UNUSED ) {

  uchar const * dcache_entry = fd_chunk_to_laddr_const( ctx->in[ in_idx ].mem, chunk );

  switch( ctx->in_kind[ in_idx ] ) {
  case IN_KIND_REPLAY: {
    if( FD_LIKELY( sig!=REPLAY_SIG_BECAME_LEADER ) ) return;

    /* There was a leader transition.  Handle it. */
    if( FD_UNLIKELY( chunk<ctx->in[ in_idx ].chunk0 || chunk>ctx->in[ in_idx ].wmark || sz!=sizeof(fd_became_leader_t) ) )
      FD_LOG_ERR(( "chunk %lu %lu corrupt, not in range [%lu,%lu]", chunk, sz, ctx->in[ in_idx ].chunk0, ctx->in[ in_idx ].wmark ));

    fd_memcpy( ctx->_became_leader, dcache_entry, sizeof(fd_became_leader_t) );
    return;
  }
  case IN_KIND_POH: {
      /* Not interested in stamped microblocks, only leader updates. */
    if( fd_disco_poh_sig_pkt_type( sig )!=POH_PKT_TYPE_BECAME_LEADER ) return;

    /* There was a leader transition.  Handle it. */
    if( FD_UNLIKELY( chunk<ctx->in[ in_idx ].chunk0 || chunk>ctx->in[ in_idx ].wmark || sz!=sizeof(fd_became_leader_t) ) )
      FD_LOG_ERR(( "chunk %lu %lu corrupt, not in range [%lu,%lu]", chunk, sz, ctx->in[ in_idx ].chunk0, ctx->in[ in_idx ].wmark ));

    fd_memcpy( ctx->_became_leader, dcache_entry, sizeof(fd_became_leader_t) );
    return;
  }
  case IN_KIND_BANK: {
    FD_TEST( ctx->use_consumed_cus );
      /* For a previous slot */
    if( FD_UNLIKELY( sig!=ctx->leader_slot ) ) return;

    if( FD_UNLIKELY( chunk<ctx->in[ in_idx ].chunk0 || chunk>ctx->in[ in_idx ].wmark || sz<FD_PACK_REBATE_MIN_SZ
          || sz>FD_PACK_REBATE_MAX_SZ ) )
      FD_LOG_ERR(( "chunk %lu %lu corrupt, not in range [%lu,%lu]", chunk, sz, ctx->in[ in_idx ].chunk0, ctx->in[ in_idx ].wmark ));

    ctx->pending_rebate_sz = sz;
    fd_memcpy( ctx->rebate, dcache_entry, sz );
    return;
  }
  case IN_KIND_RESOLV: {
    if( FD_UNLIKELY( chunk<ctx->in[ in_idx ].chunk0 || chunk>ctx->in[ in_idx ].wmark || sz>FD_TPU_RESOLVED_MTU ) )
      FD_LOG_ERR(( "chunk %lu %lu corrupt, not in range [%lu,%lu]", chunk, sz, ctx->in[ in_idx ].chunk0, ctx->in[ in_idx ].wmark ));

    fd_txn_m_t * txnm = (fd_txn_m_t *)dcache_entry;
    ulong payload_sz  = txnm->payload_sz;
    ulong txn_t_sz    = txnm->txn_t_sz;
    uint  source_ipv4 = txnm->source_ipv4;
    uchar source_tpu  = txnm->source_tpu;
    long  now_ticks = fd_tickcount();
    long  arrival_ticks = txnm->scheduler_arrival_tspub
                          ? fd_frag_meta_ts_decomp( (ulong)txnm->scheduler_arrival_tspub, now_ticks )
                          : now_ticks;
    long  scheduler_arrival_time_nanos = pack_tile_wallclock_from_ticks( ctx, arrival_ticks );
    FD_TEST( payload_sz<=FD_TPU_MTU    );
    FD_TEST( txn_t_sz  <=FD_TXN_MAX_SZ );
    fd_txn_t * txn  = fd_txn_m_txn_t( txnm );

    ulong addr_table_sz = 32UL*txn->addr_table_adtl_cnt;
    FD_TEST( addr_table_sz<=32UL*FD_TXN_ACCT_ADDR_MAX );

    if( FD_UNLIKELY( (ctx->leader_slot==ULONG_MAX) & (sig>ctx->highest_observed_slot) ) ) {
      /* Using the resolv tile's knowledge of the current slot is a bit
         of a hack, since we don't get any info if there are no
         transactions and we're not leader.  We're actually in exactly
         the case where that's okay though.  The point of calling
         expire_before long before we become leader is so that we don't
         drop new but low-fee-paying transactions when pack is clogged
         with expired but high-fee-paying transactions.  That can only
         happen if we are getting transactions. */
      ctx->highest_observed_slot = sig;
      ctx->bam_pending_check_slot = sig;
      ulong exp_cnt = fd_pack_expire_before( ctx->pack, fd_ulong_max( ctx->highest_observed_slot, TRANSACTION_LIFETIME_SLOTS )-TRANSACTION_LIFETIME_SLOTS );
      FD_MCNT_INC( PACK, TRANSACTION_EXPIRED, exp_cnt );
    }

    if( FD_UNLIKELY( source_tpu==FD_TXN_M_TPU_SOURCE_BAM && txnm->bam.revert_on_error ) ) {
      FD_TEST( txnm->bam.txn_cnt>0UL && txnm->bam.txn_cnt<=FD_PACK_MAX_TXN_PER_BUNDLE );
      FD_TEST( txnm->bam.batch_idx<txnm->bam.txn_cnt );

      ctx->bundle_kind = PACK_TILE_BUNDLE_KIND_BAM;

      _Bool new_bam_bundle = !ctx->current_bam_bundle->bundle ||
                             ctx->current_bam_bundle->seq_id != txnm->bam.seq_id;

      if( FD_LIKELY( new_bam_bundle ) ) {
        if( FD_UNLIKELY( ctx->current_bam_bundle->bundle &&
                         ctx->current_bam_bundle->txn_received!=ctx->current_bam_bundle->txn_cnt ) ) {
          pack_tile_abandon_current_bam_bundle( ctx, NULL, 1, PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_NEW_SEQ_BEFORE_COMPLETE );
        }

        ctx->current_bam_bundle->seq_id            = txnm->bam.seq_id;
        ctx->current_bam_bundle->txn_cnt           = txnm->bam.txn_cnt;
        ctx->current_bam_bundle->txn_received      = 0U;
        ctx->current_bam_bundle->min_blockhash_slot = ULONG_MAX;
        ctx->current_bam_bundle->max_schedule_slot = txnm->bam.max_schedule_slot;
        ctx->current_bam_bundle->slot             = pack_tile_bam_funnel_slot( ctx, txnm->bam.max_schedule_slot );
        memset( ctx->current_bam_bundle->received, 0, sizeof(ctx->current_bam_bundle->received) );
        ctx->current_bam_bundle->bundle = fd_pack_insert_bundle_init( ctx->pack, ctx->current_bam_bundle->_txn, txnm->bam.txn_cnt );
        pack_tile_note_bam_received( ctx, ctx->current_bam_bundle->slot, txnm->bam.seq_id, 1UL, txnm->bam.txn_cnt );
      }

      ctx->cur_spot                                = ctx->current_bam_bundle->bundle[ txnm->bam.batch_idx ];
      ctx->current_bam_bundle->min_blockhash_slot   = fd_ulong_min( ctx->current_bam_bundle->min_blockhash_slot, sig );
    } else if( FD_UNLIKELY( txnm->block_engine.bundle_id ) ) {
      ulong bundle_id = txnm->block_engine.bundle_id;
      ctx->bundle_kind = PACK_TILE_BUNDLE_KIND_BLOCK_ENGINE;
      if( FD_LIKELY( bundle_id!=ctx->current_bundle->id ) ) {
        if( FD_UNLIKELY( ctx->current_bundle->bundle ) ) {
          FD_MCNT_INC( PACK, TRANSACTION_DROPPED_PARTIAL_BUNDLE, ctx->current_bundle->txn_received );
          fd_pack_insert_bundle_cancel( ctx->pack, ctx->current_bundle->bundle, ctx->current_bundle->txn_cnt );
        }
        ctx->current_bundle->id                 = bundle_id;
        ctx->current_bundle->txn_cnt            = txnm->block_engine.bundle_txn_cnt;
        ctx->current_bundle->min_blockhash_slot = ULONG_MAX;
        ctx->current_bundle->txn_received       = 0UL;

        if( FD_UNLIKELY( ctx->current_bundle->txn_cnt==0UL ) ) {
          FD_MCNT_INC( PACK, TRANSACTION_DROPPED_PARTIAL_BUNDLE, 1UL );
          ctx->current_bundle->id = 0UL;
          return;
        }
        ctx->bundle_meta->commission = txnm->block_engine.commission;
        memcpy( ctx->bundle_meta->commission_pubkey->b, txnm->block_engine.commission_pubkey, 32UL );

        ctx->current_bundle->bundle = fd_pack_insert_bundle_init( ctx->pack, ctx->current_bundle->_txn, ctx->current_bundle->txn_cnt );
      }
      ctx->cur_spot                           = ctx->current_bundle->bundle[ ctx->current_bundle->txn_received ];
      ctx->current_bundle->min_blockhash_slot = fd_ulong_min( ctx->current_bundle->min_blockhash_slot, sig );
    } else {
      ctx->bundle_kind = PACK_TILE_BUNDLE_KIND_NONE;
#if FD_PACK_USE_EXTRA_STORAGE
      if( FD_LIKELY( ctx->leader_slot!=ULONG_MAX || fd_pack_avail_txn_cnt( ctx->pack )<ctx->max_pending_transactions ) ) {
        ctx->cur_spot = fd_pack_insert_txn_init( ctx->pack );
        ctx->insert_to_extra = 0;
      } else {
        if( FD_UNLIKELY( extra_txn_deq_full( ctx->extra_txn_deq ) ) ) {
          fd_pack_extra_txn_t const * evicted = extra_txn_deq_peek_head( ctx->extra_txn_deq );
          if( FD_UNLIKELY( evicted->txn.txnp->source_tpu==FD_TXN_M_TPU_SOURCE_BAM ) ) {
            pack_tile_log_bam_drop( ctx,
                                       "extra_queue",
                                       "extra_queue_head_evicted",
                                       0U,
                                       0U,
                                       0U,
                                       0,
                                       evicted->txn.txnp->bam.seq_id,
                                       1U,
                                       "extra_buffer",
                                       evicted->bam_funnel_slot,
                                       evicted->bam_max_schedule_slot,
                                       evicted->txn.txnp->blockhash_slot,
                                       evicted->txn.txnp->scheduler_arrival_time_nanos,
                                       1U,
                                       (uint)evicted->txn.txnp->bam.revert_on_error,
                                       1U,
                                       (uint)evicted->txn.txnp->bam.batch_idx,
                                       1UL,
                                       1UL,
                                       0U,
                                       0U,
                                       evicted->txn.txnp->payload + 1UL );
          }
          extra_txn_deq_remove_head( ctx->extra_txn_deq );
          FD_MCNT_INC( PACK, TRANSACTION_DROPPED_FROM_EXTRA, 1UL );
        }
        ctx->cur_spot = &extra_txn_deq_peek_tail( extra_txn_deq_insert_tail( ctx->extra_txn_deq ) )->txn;
        ctx->insert_to_extra                = 1;
        FD_MCNT_INC( PACK, TRANSACTION_INSERTED_TO_EXTRA, 1UL );
      }
#else
      ctx->cur_spot = fd_pack_insert_txn_init( ctx->pack );
#endif
    }

    /* We get transactions from the resolv tile.
       The transactions should have been parsed and verified. */
    FD_MCNT_INC( PACK, NORMAL_TRANSACTION_RECEIVED, 1UL );

    fd_memcpy( ctx->cur_spot->txnp->payload, fd_txn_m_payload( txnm ), payload_sz    );
    fd_memcpy( TXN(ctx->cur_spot->txnp),     txn,                      txn_t_sz      );
    fd_memcpy( ctx->cur_spot->alt_accts,     fd_txn_m_alut( txnm ),    addr_table_sz );
    ctx->cur_spot->txnp->scheduler_arrival_time_nanos = scheduler_arrival_time_nanos;
    ctx->cur_spot->txnp->payload_sz  = payload_sz;
    ctx->cur_spot->txnp->source_ipv4 = source_ipv4;
    ctx->cur_spot->txnp->source_tpu  = source_tpu;
    if( FD_LIKELY( source_tpu==FD_TXN_M_TPU_SOURCE_BAM ) ) {
      ctx->cur_spot->txnp->bam.seq_id          = txnm->bam.seq_id;
      ctx->cur_spot->txnp->bam.batch_idx       = txnm->bam.batch_idx;
      ctx->cur_spot->txnp->bam.revert_on_error = txnm->bam.revert_on_error;
      ctx->bam_txn_max_schedule_slot           = txnm->bam.max_schedule_slot;
    } else {
      ctx->cur_spot->txnp->bam.seq_id          = 0U;
      ctx->cur_spot->txnp->bam.batch_idx       = 0U;
      ctx->cur_spot->txnp->bam.revert_on_error = 0U;
    }
#if FD_PACK_USE_EXTRA_STORAGE
    if( FD_UNLIKELY( ctx->insert_to_extra ) ) {
      /* Extra-storage txns bypass fd_pack until they are replayed later, so
         persist the metadata needed to revalidate BAM work on reinsertion. */
      fd_pack_extra_txn_t * extra = extra_txn_deq_peek_tail( ctx->extra_txn_deq );
      ctx->cur_spot->txnp->blockhash_slot = sig;
      extra->bam_max_schedule_slot = 0UL;
      extra->bam_funnel_slot       = ULONG_MAX;
      if( FD_UNLIKELY( source_tpu==FD_TXN_M_TPU_SOURCE_BAM ) ) {
        extra->bam_max_schedule_slot = txnm->bam.max_schedule_slot;
        extra->bam_funnel_slot       = pack_tile_bam_funnel_slot( ctx, txnm->bam.max_schedule_slot );
      }
    }
#endif
    if( FD_UNLIKELY( source_tpu==FD_TXN_M_TPU_SOURCE_BAM ) ) {
      ulong bam_recent_seq_slot = txnm->bam.revert_on_error
                                  ? ctx->current_bam_bundle->slot
                                  : pack_tile_bam_funnel_slot( ctx, txnm->bam.max_schedule_slot );
      if( FD_UNLIKELY( !txnm->bam.revert_on_error ) ) pack_tile_note_bam_received( ctx, bam_recent_seq_slot, txnm->bam.seq_id, 1UL, 1UL );
    }

    break;
  }
  case IN_KIND_EXECUTED_TXN: {
    FD_TEST( sz==64UL );
    fd_memcpy( ctx->executed_txn_sig, dcache_entry, sz );
    break;
  }
  }
}


/* After the transaction has been fully received, and we know we were
   not overrun while reading it, insert it into pack. */

static inline void
after_frag( fd_pack_ctx_t *     ctx,
            ulong               in_idx,
            ulong               seq,
            ulong               sig,
            ulong               sz,
            ulong               tsorig,
            ulong               tspub,
            fd_stem_context_t * stem ) {
  (void)seq;
  (void)sz;
  (void)tsorig;
  (void)tspub;

  long now = fd_tickcount();

  ulong leader_slot = ULONG_MAX;
  switch( ctx->in_kind[ in_idx ] ) {
    case IN_KIND_REPLAY:
      if( FD_UNLIKELY( sig!=REPLAY_SIG_BECAME_LEADER ) ) return;
      leader_slot = ctx->_became_leader->slot;

      ctx->start_block_sched_metrics->time = now;
      fd_pack_get_sched_metrics( ctx->pack, ctx->start_block_sched_metrics->sched_results );
      break;
    case IN_KIND_POH:
      if( fd_disco_poh_sig_pkt_type( sig )!=POH_PKT_TYPE_BECAME_LEADER ) return;
      leader_slot = fd_disco_poh_sig_slot( sig );
      break;
    default:
      break;
  }

  switch( ctx->in_kind[ in_idx ] ) {
  case IN_KIND_REPLAY:
  case IN_KIND_POH: {
    long now_ticks = fd_tickcount();
    long now_ns    = fd_log_wallclock();

    if( FD_UNLIKELY( ctx->leader_slot!=ULONG_MAX ) ) {
      ulong old_leader_slot = ctx->leader_slot;
      FD_LOG_WARNING(( "switching to slot %lu while packing for slot %lu. Draining bank tiles.", leader_slot, old_leader_slot ));
      pack_tile_finish_leader_slot( ctx, stem, now_ticks, "switch", PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_LEADER_SLOT_END );
    }
    ctx->leader_slot = leader_slot;
    ctx->bam_current_slot_fresh = 0U;
    ctx->bam_first_insert_seen = 0U;
    ctx->bam_first_schedule_seen = 0U;
    ctx->bam_first_insert_minus_slot_end_ns = 0L;
    ctx->bam_first_schedule_minus_slot_end_ns = 0L;
    (void)pack_tile_bam_recent_slot_prepare( ctx, ctx->leader_slot );
    pack_tile_evict_invalid_pending_bam_work( ctx, stem, ctx->leader_slot );
    for( ulong i=0UL; i<ctx->bam_work_cnt; ) {
      if( FD_UNLIKELY( ctx->bam_work[ i ].state==PACK_BAM_WORK_STATE_SCHEDULED &&
                       ( ctx->bam_work[ i ].slot==ULONG_MAX || ctx->bam_work[ i ].slot < ctx->leader_slot ) ) ) {
        (void)pack_tile_bam_work_swap_remove( ctx, i );
        continue;
      }
      i++;
    }

    ulong exp_cnt = fd_pack_expire_before( ctx->pack, fd_ulong_max( ctx->leader_slot, TRANSACTION_LIFETIME_SLOTS )-TRANSACTION_LIFETIME_SLOTS );
    FD_MCNT_INC( PACK, TRANSACTION_EXPIRED, exp_cnt );

    ctx->leader_bank          = ctx->_became_leader->bank;
    ctx->leader_bank_idx      = ctx->_became_leader->bank_idx;
    ctx->slot_max_microblocks = ctx->_became_leader->max_microblocks_in_slot;
    /* Reserve some space in the block for ticks */
    ctx->slot_max_data        = (ctx->larger_shred_limits_per_block ? LARGER_MAX_DATA_PER_BLOCK : FD_PACK_MAX_DATA_PER_BLOCK)
                                      - 48UL*(ctx->_became_leader->ticks_per_slot+ctx->_became_leader->total_skipped_ticks);

    ctx->limits.slot_max_cost                = ctx->_became_leader->limits.slot_max_cost;
    ctx->limits.slot_max_vote_cost           = ctx->_became_leader->limits.slot_max_vote_cost;
    ctx->limits.slot_max_write_cost_per_acct = ctx->_became_leader->limits.slot_max_write_cost_per_acct;

    /* ticks_per_ns is probably relatively stable over 400ms, but not
       over several hours, so we need to compute the slot duration in
       milliseconds first and then convert to ticks.  This doesn't need
       to be super accurate, but we don't want it to vary wildly. */
    long end_ticks = now_ticks + (long)((double)fd_long_max( ctx->_became_leader->slot_end_ns - now_ns, 1L )*ctx->ticks_per_ns);
    /* We may still get overrun, but then we'll never use this and just
       reinitialize it the next time when we actually become leader. */
    fd_pack_pacing_init( ctx->pacer, now_ticks, end_ticks, (float)ctx->ticks_per_ns, ctx->limits.slot_max_cost );

    if( FD_UNLIKELY( ctx->crank->enabled ) ) {
      /* If we get overrun, we'll just never use these values, but the
         old values aren't really useful either. */
      ctx->crank->epoch = ctx->_became_leader->epoch;
      *(ctx->crank->prev_config) = *(ctx->_became_leader->bundle->config);
      memcpy( ctx->crank->recent_blockhash,   ctx->_became_leader->bundle->last_blockhash,     32UL );
      memcpy( ctx->crank->tip_receiver_owner, ctx->_became_leader->bundle->tip_receiver_owner, 32UL );
    }

    FD_LOG_INFO(( "pack_became_leader(slot=%lu,ends_at=%ld)", ctx->leader_slot, ctx->_became_leader->slot_end_ns ));

    update_metric_state( ctx, fd_tickcount(), FD_PACK_METRIC_STATE_LEADER, 1 );

    ctx->slot_end_ns = ctx->_became_leader->slot_end_ns;
    if( FD_UNLIKELY( ctx->dump_bam_mode ) ) {
      FD_LOG_NOTICE(( "Firedancer slot start: pack_current_slot=%lu bank_current_slot=%lu observed_ns=%ld slot_end_ns=%ld slot_end_known=%u current_slot_fresh=%u", ctx->leader_slot, ctx->_became_leader->slot, now_ns, ctx->slot_end_ns, (uint)!!ctx->slot_end_ns, (uint)ctx->bam_current_slot_fresh ));
    }
    fd_pack_limits_t limits[ 1 ];
    limits->max_cost_per_block = ctx->limits.slot_max_cost;
    limits->max_data_bytes_per_block = ctx->slot_max_data;
    limits->max_microblocks_per_block = ctx->slot_max_microblocks;
    limits->max_vote_cost_per_block = ctx->limits.slot_max_vote_cost;
    limits->max_write_cost_per_acct = ctx->limits.slot_max_write_cost_per_acct;
    limits->max_txn_per_microblock = ULONG_MAX; /* unused */
    fd_pack_set_block_limits( ctx->pack, limits );
    fd_pack_pacing_update_consumed_cus( ctx->pacer, fd_pack_current_block_cost( ctx->pack ), now );

    if( FD_UNLIKELY( !ctx->crank->enabled ) ) fd_pack_set_initializer_bundles_ready( ctx->pack );
    pack_tile_publish_bam_leader_state( ctx, stem );

    break;
  }
  case IN_KIND_BANK: {
    /* For a previous slot */
    if( FD_UNLIKELY( sig!=ctx->leader_slot ) ) return;

    fd_pack_rebate_cus( ctx->pack, ctx->rebate->rebate );
    ctx->pending_rebate_sz = 0UL;
    fd_pack_pacing_update_consumed_cus( ctx->pacer, fd_pack_current_block_cost( ctx->pack ), now );
    break;
  }
  case IN_KIND_RESOLV: {
    /* While not leader, resolv slot bumps are the only local signal that
       buffered BAM work may have crossed its schedule or blockhash window. */
    if( FD_UNLIKELY( ctx->leader_slot==ULONG_MAX && ctx->bam_pending_check_slot ) ) {
      pack_tile_evict_invalid_pending_bam_work( ctx, stem, ctx->bam_pending_check_slot );
      ctx->bam_pending_check_slot = 0UL;
    }
    pack_tile_flush_pending_bam_result( ctx, stem );

    switch( ctx->bundle_kind ) {
    case PACK_TILE_BUNDLE_KIND_BLOCK_ENGINE: {
      if( FD_UNLIKELY( ctx->current_bundle->txn_cnt==0UL ) ) return;
      if( FD_UNLIKELY( ++(ctx->current_bundle->txn_received)==ctx->current_bundle->txn_cnt ) ) {
        ulong deleted;
        long insert_duration = -fd_tickcount();
        int result = fd_pack_insert_bundle_fini( ctx->pack, ctx->current_bundle->bundle, ctx->current_bundle->txn_cnt, ctx->current_bundle->min_blockhash_slot, 0, ctx->bundle_meta, &deleted );
        insert_duration      += fd_tickcount();
        FD_MCNT_INC( PACK, TRANSACTION_DELETED, deleted );
        ctx->insert_result[ result + FD_PACK_INSERT_RETVAL_OFF ] += ctx->current_bundle->txn_received;
        fd_histf_sample( ctx->insert_duration, (ulong)insert_duration );
        ctx->current_bundle->bundle = NULL;
      }
      break;
    }
    case PACK_TILE_BUNDLE_KIND_BAM: {
      uchar idx = ctx->cur_spot->txnp->bam.batch_idx;
      if( FD_LIKELY( !ctx->current_bam_bundle->received[ idx ] ) ) {
        ctx->current_bam_bundle->received[ idx ] = 1U;
        ctx->current_bam_bundle->txn_received++;
      }

      if( FD_UNLIKELY( ctx->current_bam_bundle->txn_received!=ctx->current_bam_bundle->txn_cnt ) ) break;

      ulong max_schedule_slot = ctx->current_bam_bundle->max_schedule_slot;
      long first_rx_ts_ns = pack_tile_current_bam_bundle_first_rx_ts_ns( ctx );
      pack_tile_bam_invalid_reason_t invalid_reason =
          pack_tile_bam_invalid_reason( pack_tile_bam_best_known_slot( ctx ),
                                        max_schedule_slot,
                                        ctx->current_bam_bundle->min_blockhash_slot );

      if( FD_UNLIKELY( invalid_reason!=PACK_TILE_BAM_INVALID_NONE ) ) {
        pack_tile_log_bam_drop( ctx,
                                   "pre_pending_validation",
                                   invalid_reason==PACK_TILE_BAM_INVALID_OUTSIDE_SLOT
                                       ? "rejected_pre_pending_outside_slot"
                                       : "rejected_pre_pending_blockhash_expired",
                                   1U,
                                   (uint)invalid_reason,
                                   0U,
                                   0,
                                   ctx->current_bam_bundle->seq_id,
                                   ctx->current_bam_bundle->txn_cnt,
                                   "assembling",
                                   ctx->current_bam_bundle->slot,
                                   max_schedule_slot,
                                   ctx->current_bam_bundle->min_blockhash_slot,
                                   first_rx_ts_ns,
                                   1U,
                                   1U,
                                   0U,
                                   0U,
                                   ctx->current_bam_bundle->txn_cnt,
                                   ctx->current_bam_bundle->txn_cnt,
                                   0U,
                                   0U,
                                   ctx->current_bam_bundle->bundle[ 0 ]->txnp->payload + 1UL );
        pack_tile_record_bam_invalid_reason_count( ctx->bam_work_rejected_pre_pending_cnt, invalid_reason, ctx->current_bam_bundle->txn_cnt );
        pack_tile_note_bam_first_outcome( ctx,
                                          FD_METRICS_ENUM_PACK_BAM_WORK_FIRST_OUTCOME_V_REJECTED_PRE_PENDING_IDX,
                                          first_rx_ts_ns,
                                          pack_tile_now_ns( ctx ) );
        pack_tile_note_bam_rejected_pre_pending( ctx, ctx->current_bam_bundle->slot, 1UL, ctx->current_bam_bundle->txn_cnt );
        pack_tile_publish_bam_invalid_result( ctx,
                                              stem,
                                              ctx->current_bam_bundle->seq_id,
                                              max_schedule_slot,
                                              ctx->current_bam_bundle->txn_cnt,
                                              invalid_reason );
        fd_pack_insert_bundle_cancel( ctx->pack, ctx->current_bam_bundle->bundle, ctx->current_bam_bundle->txn_cnt );
        ctx->current_bam_bundle->bundle = NULL;
        break;
      }

      fd_memset( ctx->bundle_meta, 0, sizeof(block_builder_info_t) );
      if( FD_LIKELY( ctx->bam_fee_cfg && FD_VOLATILE_CONST( ctx->bam_fee_cfg->has_prio_fee_recipient ) ) ) {
        fd_memcpy( ctx->bundle_meta->commission_pubkey->b,
                   ctx->bam_fee_cfg->prio_fee_recipient,
                   32UL );
        ctx->bundle_meta->commission = (ulong)fd_uint_min( FD_VOLATILE_CONST( ctx->bam_fee_cfg->commission_bps )/100U, 100U );
      }

      fd_ed25519_sig_t bam_sig[ FD_PACK_MAX_TXN_PER_BUNDLE ];
      for( uchar i=0U; i<FD_PACK_MAX_TXN_PER_BUNDLE; i++ ) {
        if( FD_UNLIKELY( i>=ctx->current_bam_bundle->txn_cnt ) ) break;
        fd_memcpy( &bam_sig[ i ],
                   ctx->current_bam_bundle->bundle[ i ]->txnp->payload + 1UL,
                   sizeof(fd_ed25519_sig_t) );
      }

      ulong deleted;
      long insert_duration = -fd_tickcount();
      int result = fd_pack_insert_bundle_fini( ctx->pack,
                                               ctx->current_bam_bundle->bundle,
                                               ctx->current_bam_bundle->txn_cnt,
                                               ctx->current_bam_bundle->min_blockhash_slot,
                                               0,
                                               ctx->bundle_meta,
                                               &deleted );
      insert_duration      += fd_tickcount();

      FD_MCNT_INC( PACK, TRANSACTION_DELETED, deleted );
      ctx->insert_result[ result + FD_PACK_INSERT_RETVAL_OFF ] += ctx->current_bam_bundle->txn_received;
      fd_histf_sample( ctx->insert_duration, (ulong)insert_duration );

      ctx->current_bam_bundle->bundle = NULL;
      if( FD_UNLIKELY( result<0 ) ) {
        pack_tile_log_bam_drop( ctx,
                                   "insert",
                                   pack_tile_bam_pack_insert_reason_cstr( result ),
                                   0U,
                                   0U,
                                   1U,
                                   result,
                                   ctx->current_bam_bundle->seq_id,
                                   ctx->current_bam_bundle->txn_cnt,
                                   "assembling",
                                   ctx->current_bam_bundle->slot,
                                   max_schedule_slot,
                                   ctx->current_bam_bundle->min_blockhash_slot,
                                   first_rx_ts_ns,
                                   1U,
                                   1U,
                                   0U,
                                   0U,
                                   ctx->current_bam_bundle->txn_cnt,
                                   ctx->current_bam_bundle->txn_cnt,
                                   0U,
                                   0U,
                                   bam_sig[ 0 ] );
        pack_tile_note_bam_first_outcome( ctx,
                                          FD_METRICS_ENUM_PACK_BAM_WORK_FIRST_OUTCOME_V_REJECTED_PRE_PENDING_IDX,
                                          first_rx_ts_ns,
                                          pack_tile_now_ns( ctx ) );
        pack_tile_note_bam_rejected_pre_pending( ctx, ctx->current_bam_bundle->slot, 1UL, ctx->current_bam_bundle->txn_cnt );
        pack_tile_publish_bam_insert_reject( ctx,
                                             stem,
                                             ctx->current_bam_bundle->seq_id,
                                             max_schedule_slot,
                                             ctx->current_bam_bundle->txn_cnt,
                                             result );
        break;
      }
      if( FD_UNLIKELY( !pack_tile_track_bam_work( ctx,
                                                  bam_sig[ 0 ],
                                                  first_rx_ts_ns,
                                                  ctx->current_bam_bundle->seq_id,
                                                  ctx->current_bam_bundle->slot,
                                                  max_schedule_slot,
                                                  ctx->current_bam_bundle->min_blockhash_slot,
                                                  ctx->current_bam_bundle->txn_cnt ) ) ) {
        pack_tile_publish_bam_tracking_reject( ctx,
                                               stem,
                                               bam_sig[ 0 ],
                                               first_rx_ts_ns,
                                               ctx->current_bam_bundle->seq_id,
                                               ctx->current_bam_bundle->slot,
                                               max_schedule_slot,
                                               ctx->current_bam_bundle->min_blockhash_slot,
                                               1U,
                                               1U,
                                               ctx->current_bam_bundle->txn_cnt );
        break;
      }
      pack_tile_note_bam_accepted( ctx, ctx->current_bam_bundle->slot, 1UL, ctx->current_bam_bundle->txn_cnt );
      pack_tile_note_first_bam_insert( ctx,
                                       stem,
                                       pack_tile_now_ns( ctx ),
                                       pack_tile_bam_fresh_slot_for_hint( ctx, max_schedule_slot ) );
      break;
    }
    case PACK_TILE_BUNDLE_KIND_NONE:
    default: {
      fd_txn_p_t const * txnp = ctx->cur_spot->txnp;
      _Bool is_bam            = txnp->source_tpu==FD_TXN_M_TPU_SOURCE_BAM;
      uint  bam_seq_id        = txnp->bam.seq_id;
      ulong bam_slot          = 0UL;
      ulong bam_funnel_slot   = ULONG_MAX;
      pack_tile_bam_invalid_reason_t invalid_reason = PACK_TILE_BAM_INVALID_NONE;
      fd_ed25519_sig_t bam_sig[1] = {0};
      if( FD_UNLIKELY( is_bam ) ) {
        bam_slot = ctx->bam_txn_max_schedule_slot;
        bam_funnel_slot = pack_tile_bam_funnel_slot( ctx, bam_slot );
        fd_memcpy( bam_sig, txnp->payload + 1UL, sizeof(fd_ed25519_sig_t) );
        invalid_reason = pack_tile_bam_invalid_reason( pack_tile_bam_best_known_slot( ctx ), bam_slot, sig );
      }

      if( FD_UNLIKELY( invalid_reason!=PACK_TILE_BAM_INVALID_NONE ) ) {
#if FD_PACK_USE_EXTRA_STORAGE
        if( FD_UNLIKELY( ctx->insert_to_extra ) ) extra_txn_deq_remove_tail( ctx->extra_txn_deq );
        else                                       fd_pack_insert_txn_cancel( ctx->pack, ctx->cur_spot );
#else
        fd_pack_insert_txn_cancel( ctx->pack, ctx->cur_spot );
#endif
        pack_tile_log_bam_drop( ctx,
                                   "pre_pending_validation",
                                   invalid_reason==PACK_TILE_BAM_INVALID_OUTSIDE_SLOT
                                       ? "rejected_pre_pending_outside_slot"
                                       : "rejected_pre_pending_blockhash_expired",
                                   1U,
                                   (uint)invalid_reason,
                                   0U,
                                   0,
                                   bam_seq_id,
                                   1U,
                                   "staging",
                                   bam_funnel_slot,
                                   bam_slot,
                                   sig,
                                   txnp->scheduler_arrival_time_nanos,
                                   1U,
                                   (uint)txnp->bam.revert_on_error,
                                   1U,
                                   (uint)txnp->bam.batch_idx,
                                   1UL,
                                   1UL,
                                   0U,
                                   0U,
                                   txnp->payload + 1UL );
        pack_tile_record_bam_invalid_reason_count( ctx->bam_work_rejected_pre_pending_cnt, invalid_reason, 1U );
        pack_tile_note_bam_first_outcome( ctx,
                                          FD_METRICS_ENUM_PACK_BAM_WORK_FIRST_OUTCOME_V_REJECTED_PRE_PENDING_IDX,
                                          txnp->scheduler_arrival_time_nanos,
                                          pack_tile_now_ns( ctx ) );
        pack_tile_note_bam_rejected_pre_pending( ctx, bam_funnel_slot, 1UL, 1UL );
        pack_tile_publish_bam_invalid_result( ctx, stem, bam_seq_id, bam_slot, 1U, invalid_reason );
        ctx->cur_spot = NULL;
        break;
      }

#if FD_PACK_USE_EXTRA_STORAGE
      if( FD_UNLIKELY( ctx->insert_to_extra ) ) break;
#endif
      ulong deleted;
      long insert_duration = -fd_tickcount();
      int result = fd_pack_insert_txn_fini( ctx->pack, ctx->cur_spot, sig, &deleted );
      insert_duration      += fd_tickcount();
      FD_MCNT_INC( PACK, TRANSACTION_DELETED, deleted );
      ctx->insert_result[ result + FD_PACK_INSERT_RETVAL_OFF ]++;
      fd_histf_sample( ctx->insert_duration, (ulong)insert_duration );
      if( FD_LIKELY( result>=0 ) ) {
        ctx->last_successful_insert = now;
        if( FD_LIKELY( !is_bam ) ) break;
        if( FD_UNLIKELY( !pack_tile_track_bam_work( ctx,
                                                    bam_sig[ 0 ],
                                                    txnp->scheduler_arrival_time_nanos,
                                                    bam_seq_id,
                                                    bam_funnel_slot,
                                                    bam_slot,
                                                    sig,
                                                    1U ) ) ) {
          pack_tile_publish_bam_tracking_reject( ctx, stem, bam_sig[ 0 ], txnp->scheduler_arrival_time_nanos, bam_seq_id, bam_funnel_slot, bam_slot, sig, 1U, (uint)txnp->bam.revert_on_error, 1U );
        } else {
          pack_tile_note_bam_accepted( ctx, bam_funnel_slot, 1UL, 1UL );
          pack_tile_note_first_bam_insert( ctx,
                                           stem,
                                           pack_tile_now_ns( ctx ),
                                           pack_tile_bam_fresh_slot_for_hint( ctx, bam_slot ) );
        }
        break;
      }

      if( FD_UNLIKELY( is_bam ) ) {
        pack_tile_log_bam_drop( ctx,
                                   "insert",
                                   pack_tile_bam_pack_insert_reason_cstr( result ),
                                   0U,
                                   0U,
                                   1U,
                                   result,
                                   bam_seq_id,
                                   1U,
                                   "staging",
                                   bam_funnel_slot,
                                   bam_slot,
                                   sig,
                                   txnp->scheduler_arrival_time_nanos,
                                   1U,
                                   (uint)txnp->bam.revert_on_error,
                                   1U,
                                   (uint)txnp->bam.batch_idx,
                                   1UL,
                                   1UL,
                                   0U,
                                   0U,
                                   txnp->payload + 1UL );
        pack_tile_note_bam_first_outcome( ctx,
                                          FD_METRICS_ENUM_PACK_BAM_WORK_FIRST_OUTCOME_V_REJECTED_PRE_PENDING_IDX,
                                          txnp->scheduler_arrival_time_nanos,
                                          pack_tile_now_ns( ctx ) );
        pack_tile_note_bam_rejected_pre_pending( ctx, bam_funnel_slot, 1UL, 1UL );
        pack_tile_publish_bam_insert_reject( ctx, stem, bam_seq_id, bam_slot, 1U, result );
      }
      break;
    }
    }

    ctx->cur_spot = NULL;
    break;
  }
  case IN_KIND_EXECUTED_TXN: {
    ulong deleted = fd_pack_delete_transaction( ctx->pack, fd_type_pun( ctx->executed_txn_sig ) );
    FD_MCNT_INC( PACK, TRANSACTION_ALREADY_EXECUTED, deleted );
    uchar scheduled_matched_idx = UCHAR_MAX;
    ulong scheduled_work_idx = pack_tile_bam_work_find_by_any_sig( ctx, ctx->executed_txn_sig, PACK_BAM_WORK_STATE_SCHEDULED, &scheduled_matched_idx );
    if( FD_LIKELY( scheduled_work_idx<ctx->bam_work_cnt ) ) {
      pack_bam_work_t * item = &ctx->bam_work[ scheduled_work_idx ];
      fd_memset( item->sig[ scheduled_matched_idx ], 0, sizeof(fd_ed25519_sig_t) );
      item->remaining_txn_cnt = (uchar)fd_ulong_sat_sub( item->remaining_txn_cnt, 1UL );
      pack_tile_note_bam_landed( ctx, item->slot, 0UL, 1UL );
      if( FD_UNLIKELY( !item->remaining_txn_cnt ) ) pack_tile_note_bam_landed( ctx, pack_tile_bam_work_swap_remove( ctx, scheduled_work_idx ).slot, 1UL, 0UL );
    }
    if( FD_LIKELY( !deleted ) ) break;

    uchar matched_idx = UCHAR_MAX;
    ulong work_idx = pack_tile_bam_work_find_by_any_sig( ctx, ctx->executed_txn_sig, PACK_BAM_WORK_STATE_PENDING, &matched_idx );
    if( FD_LIKELY( work_idx>=ctx->bam_work_cnt ) ) break;

    pack_bam_work_t item = pack_tile_bam_work_swap_remove( ctx, work_idx );
    pack_tile_bam_recent_slot_sub_pending( ctx, item.slot, 1UL, item.txn_cnt );
    fd_bam_bundle_result_t res = {
      .seq_id                = item.seq_id,
      .slot                  = item.max_schedule_slot,
      .bundle_txn_cnt        = item.txn_cnt,
      .scheduling_error      = FD_BAM_SCHED_ERR_NONE,
      .transaction_err_count = item.txn_cnt,
    };
    for( uchar j=0U; j<res.bundle_txn_cnt; j++ ) {
      res.sanitize_success[ j ] = 1U;
      res.transaction_err[ j ]  = bam_types_TransactionErrorReason_COMMIT_CANCELLED;
    }
    res.transaction_err[ matched_idx ] = pack_tile_bam_txn_err_from_pack_insert( FD_PACK_INSERT_REJECT_DUPLICATE );
    pack_tile_publish_bam_result( ctx, stem, &res );
    break;
  }
  }

  update_metric_state( ctx, now, FD_PACK_METRIC_STATE_TRANSACTIONS, fd_pack_avail_txn_cnt( ctx->pack )>0 );
}

static void
privileged_init( fd_topo_t *      topo,
                 fd_topo_tile_t * tile ) {
  if( FD_LIKELY( !tile->pack.bundle.enabled ) ) return;
  if( FD_UNLIKELY( !tile->pack.bundle.vote_account_path[0] ) ) {
    FD_LOG_WARNING(( "Disabling bundle crank because no vote account was specified" ));
    return;
  }

  void * scratch = fd_topo_obj_laddr( topo, tile->tile_obj_id );

  FD_SCRATCH_ALLOC_INIT( l, scratch );
  fd_pack_ctx_t * ctx = FD_SCRATCH_ALLOC_APPEND( l, alignof( fd_pack_ctx_t ), sizeof( fd_pack_ctx_t ) );

  if( FD_UNLIKELY( !strcmp( tile->pack.bundle.identity_key_path, "" ) ) )
    FD_LOG_ERR(( "identity_key_path not set" ));

  const uchar * identity_key = fd_keyload_load( tile->pack.bundle.identity_key_path, /* pubkey only: */ 1 );
  fd_memcpy( ctx->crank->identity_pubkey->b, identity_key, 32UL );

  if( FD_UNLIKELY( !fd_base58_decode_32( tile->pack.bundle.vote_account_path, ctx->crank->vote_pubkey->b ) ) ) {
    const uchar * vote_key = fd_keyload_load( tile->pack.bundle.vote_account_path, /* pubkey only: */ 1 );
    fd_memcpy( ctx->crank->vote_pubkey->b, vote_key, 32UL );
  }
}

static void
unprivileged_init( fd_topo_t *      topo,
                   fd_topo_tile_t * tile ) {
  void * scratch = fd_topo_obj_laddr( topo, tile->tile_obj_id );

  if( FD_UNLIKELY( tile->pack.max_pending_transactions >= USHORT_MAX-10UL ) ) FD_LOG_ERR(( "pack tile supports up to %lu pending transactions", USHORT_MAX-11UL ));

  fd_pack_limits_t limits_upper[1] = {{
    .max_cost_per_block        = tile->pack.larger_max_cost_per_block ? LARGER_MAX_COST_PER_BLOCK : FD_PACK_MAX_COST_PER_BLOCK_UPPER_BOUND,
    .max_vote_cost_per_block   = FD_PACK_MAX_VOTE_COST_PER_BLOCK_UPPER_BOUND,
    .max_write_cost_per_acct   = FD_PACK_MAX_WRITE_COST_PER_ACCT_UPPER_BOUND,
    .max_data_bytes_per_block  = tile->pack.larger_shred_limits_per_block ? LARGER_MAX_DATA_PER_BLOCK : FD_PACK_MAX_DATA_PER_BLOCK,
    .max_txn_per_microblock    = EFFECTIVE_TXN_PER_MICROBLOCK,
    .max_microblocks_per_block = (ulong)UINT_MAX, /* Limit not known yet */
  }};

  ulong pack_footprint = fd_pack_footprint( tile->pack.max_pending_transactions, BUNDLE_META_SZ, tile->pack.bank_tile_count, limits_upper );

  FD_SCRATCH_ALLOC_INIT( l, scratch );
  fd_pack_ctx_t * ctx = FD_SCRATCH_ALLOC_APPEND( l, alignof( fd_pack_ctx_t ), sizeof( fd_pack_ctx_t ) );
  fd_rng_t *      rng = fd_rng_join( fd_rng_new( FD_SCRATCH_ALLOC_APPEND( l, fd_rng_align(), fd_rng_footprint() ), 0U, 0UL ) );
  if( FD_UNLIKELY( !rng ) ) FD_LOG_ERR(( "fd_rng_new failed" ));

  fd_pack_limits_t limits_lower[1] = {{
    .max_cost_per_block        = tile->pack.larger_max_cost_per_block ? LARGER_MAX_COST_PER_BLOCK : FD_PACK_MAX_COST_PER_BLOCK_LOWER_BOUND,
    .max_vote_cost_per_block   = FD_PACK_MAX_VOTE_COST_PER_BLOCK_LOWER_BOUND,
    .max_write_cost_per_acct   = FD_PACK_MAX_WRITE_COST_PER_ACCT_LOWER_BOUND,
    .max_data_bytes_per_block  = tile->pack.larger_shred_limits_per_block ? LARGER_MAX_DATA_PER_BLOCK : FD_PACK_MAX_DATA_PER_BLOCK,
    .max_txn_per_microblock    = EFFECTIVE_TXN_PER_MICROBLOCK,
    .max_microblocks_per_block = (ulong)UINT_MAX, /* Limit not known yet */
  }};

  ctx->pack = fd_pack_join( fd_pack_new( FD_SCRATCH_ALLOC_APPEND( l, fd_pack_align(), pack_footprint ),
                                         tile->pack.max_pending_transactions, BUNDLE_META_SZ, tile->pack.bank_tile_count,
                                         limits_lower, rng ) );
  if( FD_UNLIKELY( !ctx->pack ) ) FD_LOG_ERR(( "fd_pack_new failed" ));

  if( FD_UNLIKELY( tile->in_cnt>32UL ) ) FD_LOG_ERR(( "Too many input links (%lu>32) to pack tile", tile->in_cnt ));

  FD_TEST( tile->in_cnt<sizeof( ctx->in_kind )/sizeof( ctx->in_kind[0] ) );
  for( ulong i=0UL; i<tile->in_cnt; i++ ) {
    fd_topo_link_t const * link = &topo->links[ tile->in_link_id[ i ] ];

    if( FD_LIKELY(      !strcmp( link->name, "resolv_pack"  ) ) ) ctx->in_kind[ i ] = IN_KIND_RESOLV;
    else if( FD_LIKELY( !strcmp( link->name, "dedup_pack"   ) ) ) ctx->in_kind[ i ] = IN_KIND_RESOLV;
    else if( FD_LIKELY( !strcmp( link->name, "poh_pack"     ) ) ) ctx->in_kind[ i ] = IN_KIND_POH;
    else if( FD_LIKELY( !strcmp( link->name, "bank_pack"    ) ) ) ctx->in_kind[ i ] = IN_KIND_BANK;
    else if( FD_LIKELY( !strcmp( link->name, "sign_pack"    ) ) ) ctx->in_kind[ i ] = IN_KIND_SIGN;
    else if( FD_LIKELY( !strcmp( link->name, "replay_out"   ) ) ) ctx->in_kind[ i ] = IN_KIND_REPLAY;
    else if( FD_LIKELY( !strcmp( link->name, "executed_txn" ) ) ) ctx->in_kind[ i ] = IN_KIND_EXECUTED_TXN;
    else if( FD_LIKELY( !strcmp( link->name, "exec_sig"     ) ) ) ctx->in_kind[ i ] = IN_KIND_EXECUTED_TXN;
    else FD_LOG_ERR(( "pack tile has unexpected input link %lu %s", i, link->name ));
  }

  ulong bank_cnt = 0UL;
  for( ulong i=0UL; i<topo->tile_cnt; i++ ) {
    fd_topo_tile_t const * consumer_tile = &topo->tiles[ i ];
    if( FD_UNLIKELY( strcmp( consumer_tile->name, "bank" ) && strcmp( consumer_tile->name, "replay" ) ) ) continue;
    for( ulong j=0UL; j<consumer_tile->in_cnt; j++ ) {
      if( FD_UNLIKELY( consumer_tile->in_link_id[ j ]==tile->out_link_id[ 0 ] ) ) bank_cnt++;
    }
  }

  // if( FD_UNLIKELY( !bank_cnt                            ) ) FD_LOG_ERR(( "pack tile connects to no banking tiles" ));
  if( FD_UNLIKELY( bank_cnt>FD_PACK_MAX_BANK_TILES      ) ) FD_LOG_ERR(( "pack tile connects to too many banking tiles" ));
  // if( FD_UNLIKELY( bank_cnt!=tile->pack.bank_tile_count ) ) FD_LOG_ERR(( "pack tile connects to %lu banking tiles, but tile->pack.bank_tile_count is %lu", bank_cnt, tile->pack.bank_tile_count ));

  FD_TEST( (tile->pack.schedule_strategy>=0) & (tile->pack.schedule_strategy<=FD_PACK_STRATEGY_BUNDLE) );

  ctx->crank->enabled = tile->pack.bundle.enabled;
  if( FD_UNLIKELY( tile->pack.bundle.enabled ) ) {
    if( FD_UNLIKELY( !fd_bundle_crank_gen_init( ctx->crank->gen, (fd_acct_addr_t const *)tile->pack.bundle.tip_distribution_program_addr,
            (fd_acct_addr_t const *)tile->pack.bundle.tip_payment_program_addr,
            (fd_acct_addr_t const *)ctx->crank->vote_pubkey->b,
            (fd_acct_addr_t const *)tile->pack.bundle.tip_distribution_authority,
            schedule_strategy_strings[ tile->pack.schedule_strategy ],
            tile->pack.bundle.commission_bps ) ) ) {
      FD_LOG_ERR(( "constructing bundle generator failed" ));
    }

    ulong sign_in_idx  = fd_topo_find_tile_in_link ( topo, tile, "sign_pack", tile->kind_id );
    ulong sign_out_idx = fd_topo_find_tile_out_link( topo, tile, "pack_sign", tile->kind_id );
    FD_TEST( sign_in_idx!=ULONG_MAX );
    fd_topo_link_t * sign_in = &topo->links[ tile->in_link_id[ sign_in_idx ] ];
    fd_topo_link_t * sign_out = &topo->links[ tile->out_link_id[ sign_out_idx ] ];
    if( FD_UNLIKELY( !fd_keyguard_client_join( fd_keyguard_client_new( ctx->crank->keyguard_client,
            sign_out->mcache,
            sign_out->dcache,
            sign_in->mcache,
            sign_in->dcache,
            sign_out->mtu ) ) ) ) {
      FD_LOG_ERR(( "failed to construct keyguard" ));
    }
    /* Initialize enough of the prev config that it produces a
       transaction */
    ctx->crank->prev_config->discriminator       = 0x82ccfa1ee0aa0c9bUL;
    ctx->crank->prev_config->tip_receiver->b[1]  = 1;
    ctx->crank->prev_config->block_builder->b[2] = 1;

    memset( ctx->crank->tip_receiver_owner, '\0', 32UL );
    memset( ctx->crank->recent_blockhash,   '\0', 32UL );
    memset( ctx->crank->last_sig,           '\0', 64UL );
    ctx->crank->ib_inserted    = 0;
    ctx->crank->epoch          = 0UL;
    ctx->crank->keyswitch = fd_keyswitch_join( fd_topo_obj_laddr( topo, tile->keyswitch_obj_id ) );
    FD_TEST( ctx->crank->keyswitch );
  } else {
    memset( ctx->crank, '\0', sizeof(ctx->crank) );
  }


#if FD_PACK_USE_EXTRA_STORAGE
  ctx->extra_txn_deq = extra_txn_deq_join( extra_txn_deq_new( FD_SCRATCH_ALLOC_APPEND( l, extra_txn_deq_align(),
                                                                                          extra_txn_deq_footprint() ) ) );
#endif
  ctx->bam_work = FD_SCRATCH_ALLOC_APPEND( l,
                                           alignof(pack_bam_work_t),
                                           tile->pack.max_pending_transactions*sizeof(pack_bam_work_t) );

  ctx->cur_spot                      = NULL;
  ctx->bundle_kind                   = PACK_TILE_BUNDLE_KIND_NONE;
  ctx->dump_bam_mode                 = tile->pack.dump_bam_mode;
  ctx->bam_txn_max_schedule_slot     = 0UL;
  ctx->bam_work_cnt                  = 0UL;
  ctx->bam_pending_work_cnt          = 0UL;
  ctx->bam_scheduled_work_cnt        = 0UL;
  ctx->strategy                      = tile->pack.schedule_strategy;
  ctx->max_pending_transactions      = tile->pack.max_pending_transactions;
  ctx->leader_slot                   = ULONG_MAX;
  ctx->leader_bank                   = NULL;
  ctx->leader_bank_idx               = ULONG_MAX;
  ctx->pack_idx                      = 0UL;
  ctx->slot_microblock_cnt           = 0UL;
  ctx->pack_txn_cnt                  = 0UL;
  ctx->slot_max_microblocks          = 0UL;
  ctx->slot_max_data                 = 0UL;
  ctx->larger_shred_limits_per_block = tile->pack.larger_shred_limits_per_block;
  ctx->drain_banks                   = 0;
  ctx->approx_wallclock_ns           = fd_log_wallclock();
  ctx->approx_tickcount              = fd_tickcount();
  ctx->rng                           = rng;
  ctx->ticks_per_ns                  = fd_tempo_tick_per_ns( NULL );
  ctx->last_successful_insert        = 0L;
  ctx->highest_observed_slot         = 0UL;
  ctx->bam_pending_check_slot        = 0UL;
  ctx->microblock_duration_ticks     = (ulong)(fd_tempo_tick_per_ns( NULL )*(double)MICROBLOCK_DURATION_NS  + 0.5);
#if FD_PACK_USE_EXTRA_STORAGE
  ctx->insert_to_extra               = 0;
#endif
  ctx->use_consumed_cus              = tile->pack.use_consumed_cus;
  ctx->crank->enabled                = tile->pack.bundle.enabled;

  ctx->wait_duration_ticks[ 0 ] = ULONG_MAX;
  for( ulong i=1UL; i<MAX_TXN_PER_MICROBLOCK+1UL; i++ ) {
    ctx->wait_duration_ticks[ i ]=(ulong)(fd_tempo_tick_per_ns( NULL )*(double)wait_duration[ i ] + 0.5);
  }

  ctx->limits.slot_max_cost                = limits_lower->max_cost_per_block;
  ctx->limits.slot_max_vote_cost           = limits_lower->max_vote_cost_per_block;
  ctx->limits.slot_max_write_cost_per_acct = limits_lower->max_write_cost_per_acct;

  ctx->bank_cnt         = tile->pack.bank_tile_count;
  ctx->poll_cursor      = 0;
  ctx->skip_cnt         = 0L;
  ctx->bank_idle_bitset = fd_ulong_mask_lsb( (int)tile->pack.bank_tile_count );
  for( ulong i=0UL; i<tile->pack.bank_tile_count; i++ ) {
    ulong busy_obj_id = fd_pod_queryf_ulong( topo->props, ULONG_MAX, "bank_busy.%lu", i );
    FD_TEST( busy_obj_id!=ULONG_MAX );
    ctx->bank_current[ i ] = fd_fseq_join( fd_topo_obj_laddr( topo, busy_obj_id ) );
    ctx->bank_expect[ i ] = ULONG_MAX;
    if( FD_UNLIKELY( !ctx->bank_current[ i ] ) ) FD_LOG_ERR(( "banking tile %lu has no busy flag", i ));
    ctx->bank_ready_at[ i ] = 0L;
    FD_TEST( ULONG_MAX==fd_fseq_query( ctx->bank_current[ i ] ) );
  }

  for( ulong i=0UL; i<tile->in_cnt; i++ ) {
    fd_topo_link_t * link = &topo->links[ tile->in_link_id[ i ] ];
    fd_topo_wksp_t * link_wksp = &topo->workspaces[ topo->objs[ link->dcache_obj_id ].wksp_id ];

    ctx->in[ i ].mem    = link_wksp->wksp;
    ctx->in[ i ].chunk0 = fd_dcache_compact_chunk0( ctx->in[ i ].mem, link->dcache );
    ctx->in[ i ].wmark  = fd_dcache_compact_wmark ( ctx->in[ i ].mem, link->dcache, link->mtu );
  }

  ctx->bank_out_mem    = topo->workspaces[ topo->objs[ topo->links[ tile->out_link_id[ 0 ] ].dcache_obj_id ].wksp_id ].wksp;
  ctx->bank_out_chunk0 = fd_dcache_compact_chunk0( ctx->bank_out_mem, topo->links[ tile->out_link_id[ 0 ] ].dcache );
  ctx->bank_out_wmark  = fd_dcache_compact_wmark ( ctx->bank_out_mem, topo->links[ tile->out_link_id[ 0 ] ].dcache, topo->links[ tile->out_link_id[ 0 ] ].mtu );
  ctx->bank_out_chunk  = ctx->bank_out_chunk0;

  ctx->poh_out_mem    = topo->workspaces[ topo->objs[ topo->links[ tile->out_link_id[ 1 ] ].dcache_obj_id ].wksp_id ].wksp;
  ctx->poh_out_chunk0 = fd_dcache_compact_chunk0( ctx->poh_out_mem, topo->links[ tile->out_link_id[ 1 ] ].dcache );
  ctx->poh_out_wmark  = fd_dcache_compact_wmark ( ctx->poh_out_mem, topo->links[ tile->out_link_id[ 1 ] ].dcache, topo->links[ tile->out_link_id[ 1 ] ].mtu );
  ctx->poh_out_chunk  = ctx->poh_out_chunk0;

  ctx->bam_leader_out = (pack_bam_out_ctx_t){ .idx = ULONG_MAX };
  ctx->bam_result_out = (pack_bam_out_ctx_t){ .idx = ULONG_MAX };

  ulong bam_leader_out_idx = fd_topo_find_tile_out_link( topo, tile, "pack_bam_ldr", tile->kind_id );
  if( bam_leader_out_idx!=ULONG_MAX ) {
    fd_topo_link_t const * bam_leader_out = &topo->links[ tile->out_link_id[ bam_leader_out_idx ] ];
    ctx->bam_leader_out.idx    = bam_leader_out_idx;
    ctx->bam_leader_out.mem    = topo->workspaces[ topo->objs[ bam_leader_out->dcache_obj_id ].wksp_id ].wksp;
    ctx->bam_leader_out.chunk0 = fd_dcache_compact_chunk0( ctx->bam_leader_out.mem, bam_leader_out->dcache );
    ctx->bam_leader_out.wmark  = fd_dcache_compact_wmark ( ctx->bam_leader_out.mem, bam_leader_out->dcache, bam_leader_out->mtu );
    ctx->bam_leader_out.chunk  = ctx->bam_leader_out.chunk0;
  }

  ulong bam_result_out_idx = fd_topo_find_tile_out_link( topo, tile, "pack_bam_res", tile->kind_id );
  if( bam_result_out_idx!=ULONG_MAX ) {
    fd_topo_link_t const * bam_result_out = &topo->links[ tile->out_link_id[ bam_result_out_idx ] ];
    ctx->bam_result_out.idx    = bam_result_out_idx;
    ctx->bam_result_out.mem    = topo->workspaces[ topo->objs[ bam_result_out->dcache_obj_id ].wksp_id ].wksp;
    ctx->bam_result_out.chunk0 = fd_dcache_compact_chunk0( ctx->bam_result_out.mem, bam_result_out->dcache );
    ctx->bam_result_out.wmark  = fd_dcache_compact_wmark ( ctx->bam_result_out.mem, bam_result_out->dcache, bam_result_out->mtu );
    ctx->bam_result_out.chunk  = ctx->bam_result_out.chunk0;
  }

  ulong bam_fee_cfg_obj_id = fd_pod_query_ulong( topo->props, "bam_fee_cfg", ULONG_MAX );
  ctx->bam_fee_cfg = FD_LIKELY( bam_fee_cfg_obj_id!=ULONG_MAX )
                   ? (fd_bam_fee_cfg_t const *)fd_topo_obj_laddr( topo, bam_fee_cfg_obj_id )
                   : NULL;

  /* Initialize metrics storage */
  memset( ctx->insert_result, '\0', FD_PACK_INSERT_RETVAL_CNT * sizeof(ulong) );
  memset( ctx->bam_bundle_assembly_abandon_cnt, '\0', sizeof(ctx->bam_bundle_assembly_abandon_cnt) );
  memset( ctx->bam_work_rejected_pre_pending_cnt, '\0', sizeof(ctx->bam_work_rejected_pre_pending_cnt) );
  memset( ctx->bam_pending_work_evicted_cnt, '\0', sizeof(ctx->bam_pending_work_evicted_cnt) );
  memset( ctx->bam_work_item_stage_cnt, '\0', sizeof(ctx->bam_work_item_stage_cnt) );
  memset( ctx->bam_work_transaction_stage_cnt, '\0', sizeof(ctx->bam_work_transaction_stage_cnt) );
  memset( ctx->bam_work_first_outcome_cnt, '\0', sizeof(ctx->bam_work_first_outcome_cnt) );
  ctx->bam_tracking_rejected_cnt     = 0UL;
  ctx->bam_tracking_rejected_txn_cnt = 0UL;
  for( ulong i=0UL; i<FD_PACK_BAM_RECENT_SLOT_CNT; i++ ) ctx->bam_recent_slot[ i ].slot = ULONG_MAX;
  ctx->bam_unresolved_work = (pack_bam_unresolved_work_t){0};
  ctx->bam_current_slot_fresh = 0U;
  ctx->bam_first_insert_seen = 0U;
  ctx->bam_first_schedule_seen = 0U;
  ctx->bam_first_insert_minus_slot_end_ns = 0L;
  ctx->bam_first_schedule_minus_slot_end_ns = 0L;
  memset( ctx->bam_first_insert_result_cnt,   0, sizeof(ctx->bam_first_insert_result_cnt) );
  memset( ctx->bam_first_schedule_result_cnt, 0, sizeof(ctx->bam_first_schedule_result_cnt) );
  fd_histf_join( fd_histf_new( ctx->bam_first_insert_before_end_distance_nanos,
                               FD_MHIST_MIN( PACK, BAM_LEADER_SLOT_FIRST_INSERT_BEFORE_END_DISTANCE_NANOS ),
                               FD_MHIST_MAX( PACK, BAM_LEADER_SLOT_FIRST_INSERT_BEFORE_END_DISTANCE_NANOS ) ) );
  fd_histf_join( fd_histf_new( ctx->bam_first_insert_after_end_distance_nanos,
                               FD_MHIST_MIN( PACK, BAM_LEADER_SLOT_FIRST_INSERT_AFTER_END_DISTANCE_NANOS ),
                               FD_MHIST_MAX( PACK, BAM_LEADER_SLOT_FIRST_INSERT_AFTER_END_DISTANCE_NANOS ) ) );
  fd_histf_join( fd_histf_new( ctx->bam_first_schedule_before_end_distance_nanos,
                               FD_MHIST_MIN( PACK, BAM_LEADER_SLOT_FIRST_SCHEDULE_BEFORE_END_DISTANCE_NANOS ),
                               FD_MHIST_MAX( PACK, BAM_LEADER_SLOT_FIRST_SCHEDULE_BEFORE_END_DISTANCE_NANOS ) ) );
  fd_histf_join( fd_histf_new( ctx->bam_first_schedule_after_end_distance_nanos,
                               FD_MHIST_MIN( PACK, BAM_LEADER_SLOT_FIRST_SCHEDULE_AFTER_END_DISTANCE_NANOS ),
                               FD_MHIST_MAX( PACK, BAM_LEADER_SLOT_FIRST_SCHEDULE_AFTER_END_DISTANCE_NANOS ) ) );
  fd_histf_join( fd_histf_new( ctx->bam_work_rx_to_first_outcome_nanos,
                               FD_MHIST_MIN( PACK, BAM_WORK_RX_TO_FIRST_OUTCOME_NANOS ),
                               FD_MHIST_MAX( PACK, BAM_WORK_RX_TO_FIRST_OUTCOME_NANOS ) ) );
  fd_histf_join( fd_histf_new( ctx->schedule_duration, FD_MHIST_SECONDS_MIN( PACK, SCHEDULE_MICROBLOCK_DURATION_SECONDS ),
                                                       FD_MHIST_SECONDS_MAX( PACK, SCHEDULE_MICROBLOCK_DURATION_SECONDS ) ) );
  fd_histf_join( fd_histf_new( ctx->no_sched_duration, FD_MHIST_SECONDS_MIN( PACK, NO_SCHED_MICROBLOCK_DURATION_SECONDS ),
                                                       FD_MHIST_SECONDS_MAX( PACK, NO_SCHED_MICROBLOCK_DURATION_SECONDS ) ) );
  fd_histf_join( fd_histf_new( ctx->insert_duration,   FD_MHIST_SECONDS_MIN( PACK, INSERT_TRANSACTION_DURATION_SECONDS  ),
                                                       FD_MHIST_SECONDS_MAX( PACK, INSERT_TRANSACTION_DURATION_SECONDS  ) ) );
  fd_histf_join( fd_histf_new( ctx->complete_duration, FD_MHIST_SECONDS_MIN( PACK, COMPLETE_MICROBLOCK_DURATION_SECONDS ),
                                                       FD_MHIST_SECONDS_MAX( PACK, COMPLETE_MICROBLOCK_DURATION_SECONDS ) ) );
  ctx->metric_state = 0;
  ctx->metric_state_begin = fd_tickcount();
  memset( ctx->metric_timing,             '\0', 16*sizeof(long)                        );
  memset( ctx->current_bundle,            '\0', sizeof(ctx->current_bundle)            );
  memset( ctx->current_bam_bundle,        '\0', sizeof(ctx->current_bam_bundle)        );
  memset( ctx->bundle_meta,               '\0', sizeof(ctx->bundle_meta)               );
  memset( ctx->pending_bam_result,        '\0', sizeof(ctx->pending_bam_result)        );
  ctx->bam_work_cnt             = 0UL;
  ctx->bam_pending_work_cnt     = 0UL;
  ctx->bam_scheduled_work_cnt   = 0UL;
  ctx->bam_unresolved_work      = (pack_bam_unresolved_work_t){0};
  ctx->bam_pending_check_slot   = 0UL;
  ctx->last_bam_leader_state.slot = ULONG_MAX;
  memset( ctx->last_sched_metrics,        '\0', sizeof(ctx->last_sched_metrics)        );
  memset( ctx->start_block_sched_metrics, '\0', sizeof(ctx->start_block_sched_metrics) );
  memset( ctx->crank->metrics,            '\0', sizeof(ctx->crank->metrics)            );

  FD_LOG_INFO(( "packing microblocks of at most %lu transactions to %lu bank tiles using strategy %i", EFFECTIVE_TXN_PER_MICROBLOCK, tile->pack.bank_tile_count, ctx->strategy ));

  ulong scratch_top = FD_SCRATCH_ALLOC_FINI( l, 1UL );
  if( FD_UNLIKELY( scratch_top > (ulong)scratch + scratch_footprint( tile ) ) )
    FD_LOG_ERR(( "scratch overflow %lu %lu %lu", scratch_top - (ulong)scratch - scratch_footprint( tile ), scratch_top, (ulong)scratch + scratch_footprint( tile ) ));

}

static ulong
populate_allowed_seccomp( fd_topo_t const *      topo,
                          fd_topo_tile_t const * tile,
                          ulong                  out_cnt,
                          struct sock_filter *   out ) {
  (void)topo;
  (void)tile;

  populate_sock_filter_policy_fd_pack_tile( out_cnt, out, (uint)fd_log_private_logfile_fd() );
  return sock_filter_policy_fd_pack_tile_instr_cnt;
}

static ulong
populate_allowed_fds( fd_topo_t const *      topo,
                      fd_topo_tile_t const * tile,
                      ulong                  out_fds_cnt,
                      int *                  out_fds ) {
  (void)topo;
  (void)tile;

  if( FD_UNLIKELY( out_fds_cnt<2UL ) ) FD_LOG_ERR(( "out_fds_cnt %lu", out_fds_cnt ));

  ulong out_cnt = 0UL;
  out_fds[ out_cnt++ ] = 2; /* stderr */
  if( FD_LIKELY( -1!=fd_log_private_logfile_fd() ) )
    out_fds[ out_cnt++ ] = fd_log_private_logfile_fd(); /* logfile */
  return out_cnt;
}

#define STEM_BURST (1UL)

/* We want lazy (measured in ns) to be small enough that the producer
    and the consumer never have to wait for credits.  For most tango
    links, we use a default worst case speed coming from 100 Gbps
    Ethernet.  That's not very suitable for microblocks that go from
    pack to bank.  Instead we manually estimate the very aggressive
    1000ns per microblock, and then reduce it further (in line with the
    default lazy value computation) to ensure the random value chosen
    based on this won't lead to credit return stalls. */
#define STEM_LAZY  (128L*3000L)

#define STEM_CALLBACK_CONTEXT_TYPE  fd_pack_ctx_t
#define STEM_CALLBACK_CONTEXT_ALIGN alignof(fd_pack_ctx_t)

#define STEM_CALLBACK_DURING_HOUSEKEEPING during_housekeeping
#define STEM_CALLBACK_BEFORE_CREDIT       before_credit
#define STEM_CALLBACK_AFTER_CREDIT        after_credit
#define STEM_CALLBACK_DURING_FRAG         during_frag
#define STEM_CALLBACK_AFTER_FRAG          after_frag
#define STEM_CALLBACK_METRICS_WRITE       metrics_write

#include "../stem/fd_stem.c"

fd_topo_run_tile_t fd_tile_pack = {
  .name                     = "pack",
  .populate_allowed_seccomp = populate_allowed_seccomp,
  .populate_allowed_fds     = populate_allowed_fds,
  .scratch_align            = scratch_align,
  .scratch_footprint        = scratch_footprint,
  .privileged_init          = privileged_init,
  .unprivileged_init        = unprivileged_init,
  .run                      = stem_run,
};
