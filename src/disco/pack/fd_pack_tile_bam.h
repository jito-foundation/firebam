#ifndef HEADER_fd_src_disco_pack_fd_pack_tile_bam_h
#define HEADER_fd_src_disco_pack_fd_pack_tile_bam_h

/* FireBAM types and fd_pack_ctx_t fields (PACK_TILE_BAM_CTX) for the
   pack tile.  Included once by fd_pack_tile.c.  The BAM functions are in
   fd_pack_tile_bam.c, which fd_pack_tile.c includes after
   update_metric_state. */

#include "../bam/fd_bam_types.h"
#include "../bam/fd_bam_microblock.h"
#include "../topo/fd_topo.h"
#include "../../ballet/base58/fd_base58.h"

#define FD_PACK_BAM_RECENT_SLOT_CNT 32UL

typedef enum {
  PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_NEW_SEQ_BEFORE_COMPLETE = 0,
  PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_LEADER_SLOT_END,
  PACK_TILE_BAM_BUNDLE_ASSEMBLY_ABANDON_POH_TIMEOUT,
} pack_tile_bam_bundle_assembly_abandon_reason_t;

typedef struct {
  fd_ed25519_sig_t sig[ FD_PACK_MAX_TXN_PER_BUNDLE ];
  long             first_rx_ts_ns;
  ulong            max_schedule_slot;
  ulong            blockhash_height;
  uint             seq_id;
  ushort           scheduler_gen;
  uchar            txn_cnt;
  uchar            saw_unlanded_completion;
  uchar            indexed_mask; /* bit j set while sig[j] has an entry in bam_sig_map */
} pack_bam_work_t;

/* Cap pending and scheduled BAM batches to bound tracking memory.
   Excess work receives a CONTAINER_FULL result. */

#define PACK_BAM_WORK_MAX (8192UL)

/* Index live member signatures by their first 8 bytes; confirm all 64
   bytes on lookup. Each member uses pool slot
   work_idx*FD_PACK_MAX_TXN_PER_BUNDLE+txn_idx. Update the index when work
   moves or a member completes. */

typedef struct {
  ulong key;      /* first 8 bytes of bam_work[ work_idx ].sig[ txn_idx ] */
  uint  next;
  uint  prev;
  uint  work_idx;
  uchar txn_idx;
} pack_bam_sig_ele_t;

#define MAP_NAME                           pack_bam_sig_map
#define MAP_ELE_T                          pack_bam_sig_ele_t
#define MAP_KEY                            key
#define MAP_IDX_T                          uint
#define MAP_NEXT                           next
#define MAP_PREV                           prev
#define MAP_MULTI                          1
#define MAP_OPTIMIZE_RANDOM_ACCESS_REMOVAL 1
#include "../../util/tmpl/fd_map_chain.c"

#define PACK_BAM_SIG_MAP_NULL (ULONG_MAX)

/* Without a BAM tile no BAM work can arrive, so track none. */
static inline ulong
pack_tile_bam_work_max( fd_topo_tile_t const * tile ) {
  return tile->pack.bam_enabled ? fd_ulong_min( tile->pack.max_pending_transactions, PACK_BAM_WORK_MAX ) : 0UL;
}

static inline ulong
pack_tile_bam_sig_map_chain_cnt( ulong bam_work_max ) {
  return pack_bam_sig_map_chain_cnt_est( bam_work_max*FD_PACK_MAX_TXN_PER_BUNDLE );
}

typedef struct {
  ulong slot;
  uint  first_debug_seq_id;
} pack_bam_recent_slot_t;

/* is_bundle values: whether the current resolv frag is a bundle member,
   and of which kind. */
#define PACK_TILE_BUNDLE_KIND_NONE         (0)
#define PACK_TILE_BUNDLE_KIND_BLOCK_ENGINE (1)
#define PACK_TILE_BUNDLE_KIND_BAM          (2)
#define PACK_BAM_LEADER_STATE_INTERVAL_NS  ((long)5e6)

/* The BAM fields of fd_pack_ctx_t.  The callbacks run a copy without
   BAM unless bam_status_fseq (after_credit) or bam_work_max (the other
   callbacks) is set; pack_tile_bam_init checks BAM is all or nothing. */
#define PACK_TILE_BAM_CTX                                                                                      \
  ulong *                  bam_status_fseq;                                                                    \
  ulong                    bam_work_max; /* capacity of bam_work; result queue holds 2x this */                \
  ulong *                  bam_gen_fseq;                                                                       \
  ushort                   bam_ownership_gen;                                                                  \
  _Bool                    bam_override_snapshot;                                                              \
  uchar                    dump_bam_mode;                                                                      \
  ulong                    bam_min_admission_slot; /* rejects arrivals for closed slots */                     \
  _Bool                    bam_ib_associated; /* crank->last_sig is this slot/gen's BAM IB */                  \
  ulong                    bam_candidate_identity_mismatch_cnt;                                                \
                                                                                                               \
  /* pack_bam_ldr carries latest-value-wins fd_bam_leader_state_t                                              \
     snapshots, pack_bam_res the results the BAM tile queues durably. */                                       \
  fd_pack_out_ctx_t        bam_leader_out;                                                                     \
  fd_pack_out_ctx_t        bam_result_out;                                                                     \
  fd_bam_leader_state_t    last_bam_leader_state; /* last published */                                         \
  long                     bam_leader_state_next_publish_ticks;                                                \
                                                                                                               \
  /* BAM metadata for current_bundle while it assembles BAM work */                                            \
  struct {                                                                                                     \
    ulong  max_schedule_slot;                                                                                  \
    ushort scheduler_gen;                                                                                      \
    ushort ownership_gen;                                                                                      \
    _Bool  is_bam;                                                                                             \
  } current_bundle_bam[1];                                                                                     \
                                                                                                               \
  /* Optional BAM block builder config and its last consistent snapshot */                                     \
  fd_bam_fee_cfg_t const * bam_fee_cfg;                                                                        \
  uint                     bam_fee_cfg_version;                                                                \
  block_builder_info_t     bam_fee_meta[1];                                                                    \
  /* As in jito-solana, BAM work runs only under the current builder's                                         \
     tip configuration.  bam_tip_cfg_version is the version this slot's                                        \
     initializer state certifies (0 if none): the crank found it                                               \
     applied, or its initializer dispatched and Pack holds bundles until                                       \
     it lands. */                                                                                              \
  uint                     bam_tip_cfg_version;                                                                \
  uint                     bam_crank_cfg_version; /* last BAM crank's, so any queued IB's */                   \
  long                     bam_crank_next_ticks;  /* earliest retry of that version */                         \
  /* Restored if an unscheduled initializer is deleted */                                                      \
  fd_bundle_crank_tip_payment_config_t bam_prev_config_before_ib[1];                                           \
                                                                                                               \
  /* Pending and scheduled BAM batches awaiting completion */                                                  \
  pack_bam_work_t *        bam_work;                                                                           \
  pack_bam_sig_ele_t *     bam_sig_pool; /* bam_work_max*FD_PACK_MAX_TXN_PER_BUNDLE */                         \
  pack_bam_sig_map_t *     bam_sig_map;                                                                        \
  fd_bam_bundle_result_t * bam_result_queue;                                                                   \
  ulong                    bam_result_queue_head;                                                              \
  ulong                    bam_work_cnt;                                                                       \
  ulong                    bam_scheduled_work_cnt;                                                             \
  ulong                    bam_pending_result_cnt;                                                             \
  ulong                    bam_pack_bundle_evicted_cnt; /* last fd_pack_bundle_evicted_cnt */                  \
                                                                                                               \
  ulong      bam_bundle_assembly_abandon_cnt  [ FD_METRICS_ENUM_PACK_BAM_BUNDLE_ASSEMBLY_ABANDON_REASON_CNT ]; \
  ulong      bam_work_rejected_pre_pending_cnt[ FD_METRICS_ENUM_PACK_BAM_WORK_INVALID_REASON_CNT ];            \
  ulong      bam_pending_work_evicted_cnt     [ FD_METRICS_ENUM_PACK_BAM_WORK_INVALID_REASON_CNT ];            \
  ulong      bam_work_item_stage_cnt          [ FD_METRICS_ENUM_PACK_BAM_WORK_STAGE_CNT ];                     \
  ulong      bam_work_first_outcome_cnt       [ FD_METRICS_ENUM_PACK_BAM_WORK_FIRST_OUTCOME_CNT ];             \
  ulong      bam_first_insert_result_cnt      [ FD_METRICS_ENUM_PACK_BAM_FIRST_EVENT_RESULT_CNT ];             \
  ulong      bam_first_schedule_result_cnt    [ FD_METRICS_ENUM_PACK_BAM_FIRST_EVENT_RESULT_CNT ];             \
  ulong      bam_tracking_rejected_cnt;                                                                        \
  ulong      bam_tracking_rejected_txn_cnt;                                                                    \
  pack_bam_recent_slot_t bam_recent_slot[ FD_PACK_BAM_RECENT_SLOT_CNT ];                                       \
  uint       bam_current_slot_has_bam_work;                                                                    \
  uchar      bam_first_insert_seen;                                                                            \
  uchar      bam_first_schedule_seen;                                                                          \
  long       bam_first_insert_minus_slot_end_ns;                                                               \
  long       bam_first_schedule_minus_slot_end_ns;                                                             \
  fd_histf_t bam_work_rx_to_first_outcome_nanos[ 1 ];

#endif /* HEADER_fd_src_disco_pack_fd_pack_tile_bam_h */
