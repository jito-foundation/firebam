#ifndef HEADER_fd_src_discoh_plugin_fd_plugin_h
#define HEADER_fd_src_discoh_plugin_fd_plugin_h

#include "../../waltz/http/fd_url.h"

#define FD_PLUGIN_MSG_SLOT_ROOTED                   ( 0UL)
#define FD_PLUGIN_MSG_SLOT_OPTIMISTICALLY_CONFIRMED ( 1UL)
#define FD_PLUGIN_MSG_SLOT_COMPLETED                ( 2UL)
#define FD_PLUGIN_MSG_SLOT_ESTIMATED                ( 3UL)
#define FD_PLUGIN_MSG_GOSSIP_UPDATE                 ( 4UL)
#define FD_PLUGIN_MSG_VOTE_ACCOUNT_UPDATE           ( 5UL)
#define FD_PLUGIN_MSG_LEADER_SCHEDULE               ( 6UL)
#define FD_PLUGIN_MSG_VALIDATOR_INFO                ( 7UL)
#define FD_PLUGIN_MSG_SLOT_START                    ( 8UL)

typedef struct {
  ulong slot;
  ulong parent_slot;
} fd_plugin_msg_slot_start_t;

#define FD_PLUGIN_MSG_SLOT_END                      ( 9UL)

typedef struct {
  ulong slot;
  ulong cus_used;
} fd_plugin_msg_slot_end_t;

#define FD_PLUGIN_MSG_SLOT_RESET                    (10UL)
#define FD_PLUGIN_MSG_BALANCE                       (11UL)
#define FD_PLUGIN_MSG_START_PROGRESS                (12UL)
#define FD_PLUGIN_MSG_GENESIS_HASH_KNOWN            (13UL)

/* TODO: this needs to be bumped to 13, but that would break
   fd_gui_handle_gossip_update */
#define FD_GOSSIP_UPDATE_MSG_NUM_SOCKETS      (12U)
#define FD_GOSSIP_LINK_MSG_SIZE    (58U + FD_GOSSIP_UPDATE_MSG_NUM_SOCKETS * 6U)
#define FD_VALIDATOR_INFO_MSG_SIZE (          608U)

#define FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE           (14UL)

#define FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_DISCONNECTED (0)
#define FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTING   (1)
#define FD_PLUGIN_MSG_BLOCK_ENGINE_UPDATE_STATUS_CONNECTED    (2)

typedef struct {
  char name[ 16 ];
  char url[ FD_URL_MAX ];
  char ip_cstr[ 40 ]; /* IPv4 or IPv6 cstr */
  int status;
} fd_plugin_msg_block_engine_update_t;

#define FD_PLUGIN_MSG_BAM_UPDATE                    (15UL)

typedef enum {
  FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISABLED            = 0,
  FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED        = 1,
  FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTING          = 2,
  FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY = 3,
  FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY   = 4
} fd_plugin_bam_update_status_t;

typedef struct {
  char  name[ 16 ];
  char  url[ FD_URL_MAX ];
  char  sni[ FD_SNI_BUF_MAX ];
  char  ip_cstr[ 40 ];    /* IPv4 or IPv6 cstr */
  char  tpu_cstr[ 22 ];   /* "a.b.c.d:port" */
  char  tpu_fwd_cstr[ 22 ];
  fd_plugin_bam_update_status_t status_code; /* FD_PLUGIN_MSG_BAM_UPDATE_STATUS_* */
  uchar enabled;          /* Non-zero when operator enabled BAM */

  float  keepalive_rtt_sample;
  float  keepalive_rtt_smoothed;
  float  keepalive_rtt_deviation;
  ushort feedback_queue_depth;
  ulong  outbound_heartbeat_enqueued;
  ulong  outbound_heartbeat_enqueue_fail;
  ulong  builder_heartbeats_decoded;
  ulong  transaction_published;
  ulong  atomic_batch_published;
  ulong  ingress_packet_oversize;
  ulong  failure_auth_challenge_decode;
  ulong  failure_config_decode;
  ulong  failure_scheduler_envelope_decode;
  ulong  failure_request_failed;
  ulong  failure_resolve;
  ulong  failure_connect;
  ulong  failure_io;
  ulong  failure_unsupported_version;
  ulong  failure_request_timeout;
  ulong  failure_keepalive_timeout;
  ulong  failure_builder_activity_timeout;
  ulong  ingress_multi_message_received;
  ulong  ingress_batch_commit_attempt;
  ulong  ingress_batch_published;
  ulong  ingress_batch_rejected_invalid_batch;
  ulong  ingress_batch_rejected_empty_batch;
  ulong  ingress_batch_rejected_vote_transaction;
  ulong  ingress_batch_rejected_non_revert_multi_packet;
  ulong  ingress_message_rejected_empty_message;
  ulong  ingress_message_rejected_overflow_message;
} fd_plugin_msg_bam_update_t;

#endif /* HEADER_fd_src_discoh_plugin_fd_plugin_h */
