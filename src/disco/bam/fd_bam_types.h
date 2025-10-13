#ifndef HEADER_fd_src_disco_bam_fd_bam_types_h
#define HEADER_fd_src_disco_bam_fd_bam_types_h

#include <limits.h>

#include "../pack/fd_microblock.h"
#include "../pack/fd_pack.h" /* FD_PACK_MAX_TXN_PER_BUNDLE */
#include "proto/bam_types.pb.h"
#include "../../flamenco/types/fd_types.h"
#include "../../util/net/fd_net_headers.h"

#define FD_BAM_MAX_PENDING_RESULTS 64UL

typedef struct {
  ulong bundle_id;
  ulong slot;
  ulong bundle_txn_cnt;
  uint  txn_cnt;
  uint  execution_success; /* treated as bool */
  uint  scheduling_error;  /* bam_types_SchedulingError or FD_BAM_SCHED_ERR_NONE */
  uint  transaction_err[ FD_PACK_MAX_TXN_PER_BUNDLE ];
  uint  consumed_cus    [ FD_PACK_MAX_TXN_PER_BUNDLE ];
  uchar sanitize_success[ FD_PACK_MAX_TXN_PER_BUNDLE ];
} fd_bam_bundle_result_t;

typedef struct {
  ulong slot;
  uint  tick;
  uint  slot_cu_budget_remaining;
} fd_bam_leader_state_t;

typedef struct {
  fd_ip4_port_t tpu_addr;      /* TPU socket (TCP) advertised by BAM */
  fd_ip4_port_t tpu_quic_addr; /* QUIC forwarding socket advertised by BAM */
  uint          use_bam;       /* `FD_BAM_CONTACT_USE_*` selector */
} fd_bam_contact_update_t;

#define FD_BAM_CONTACT_USE_DEFAULT ((uint)0U)
#define FD_BAM_CONTACT_USE_BAM     ((uint)1U)

#define FD_BAM_STEM_SIG_GOSSIP_UPDATE (5UL)

#define FD_BAM_SCHED_ERR_NONE            ((uint)UINT_MAX)
#define FD_BAM_SCHED_ERR_POH_TIMEOUT     ((uint)bam_types_SchedulingError_POH_TIMEOUT)
#define FD_BAM_SCHED_ERR_OUTSIDE_SLOT    ((uint)bam_types_SchedulingError_OUTSIDE_LEADER_SLOT)
#define FD_BAM_SCHED_ERR_CONTAINER_FULL  ((uint)bam_types_SchedulingError_CONTAINER_FULL)

#endif /* HEADER_fd_src_disco_bam_fd_bam_types_h */
