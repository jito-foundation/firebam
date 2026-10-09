#ifndef HEADER_fd_src_disco_pack_fd_pack_tile_bam_h
#define HEADER_fd_src_disco_pack_fd_pack_tile_bam_h

/* FireBAM types for the pack tile.  Included once by fd_pack_tile.c
   before fd_pack_ctx_t.  The BAM functions are in fd_pack_tile_bam.c,
   which fd_pack_tile.c includes after update_metric_state. */

#include "../bam/fd_bam_types.h"
#include "../bam/fd_bam_publish.h"
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

#define PACK_TILE_BUNDLE_KIND_NONE         (0)
#define PACK_TILE_BUNDLE_KIND_BLOCK_ENGINE (1)
#define PACK_TILE_BUNDLE_KIND_BAM          (2)
#define PACK_BAM_LEADER_STATE_INTERVAL_NS  ((long)5e6)

#endif /* HEADER_fd_src_disco_pack_fd_pack_tile_bam_h */
