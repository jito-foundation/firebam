#ifndef HEADER_fd_src_disco_plugin_fd_plugin_h
#define HEADER_fd_src_disco_plugin_fd_plugin_h

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

struct __attribute__((packed, aligned(8))) fd_replay_complete_msg {
  ulong slot;
  ulong total_txn_count;
  ulong nonvote_txn_count;
  ulong failed_txn_count;
  ulong nonvote_failed_txn_count;
  ulong compute_units;
  ulong transaction_fee;
  ulong priority_fee;
  ulong parent_slot;
};
typedef struct fd_replay_complete_msg fd_replay_complete_msg_t;

#define FD_CLUSTER_NODE_CNT        (200U*201U - 1U)
/* TODO: this needs to be bumped to 13, but that would break
   fd_gui_handle_gossip_update */
#define FD_GOSSIP_UPDATE_MSG_NUM_SOCKETS      (12U)
#define FD_GOSSIP_LINK_MSG_SIZE    (58U + FD_GOSSIP_UPDATE_MSG_NUM_SOCKETS * 6U)
#define FD_VALIDATOR_INFO_MSG_SIZE (          608U)

struct __attribute__((packed)) fd_gossip_update_msg {
  uchar  pubkey[32];			// 0..31
  ulong  wallclock; 			// 32..39
  ushort shred_version;			// 40..41
  uchar  version_type;			// 42
  ushort version_major;			// 43..44
  ushort version_minor;			// 45..46
  ushort version_patch;			// 47..48
  uchar  version_commit_type;		// 49
  uint   version_commit;		// 50..53
  uint   version_feature_set;		// 54..57
    /* gossip_socket,
       rpc_socket,
       rpc_pubsub_socket,
       serve_repair_socket_udp,
       serve_repair_socket_quic,
       tpu_socket_udp,
       tpu_socket_quic,
       tvu_socket_udp,
       tvu_socket_quic,
       tpu_forwards_socket_udp,
       tpu_forwards_socket_quic,
       tpu_vote_socket, */
  struct __attribute__((packed)) {
    uint ip;				// 0..3
    ushort port;			// 4..5
  } addrs[FD_GOSSIP_UPDATE_MSG_NUM_SOCKETS];
};
typedef struct fd_gossip_update_msg fd_gossip_update_msg_t;

FD_STATIC_ASSERT( sizeof(fd_gossip_update_msg_t) == FD_GOSSIP_LINK_MSG_SIZE, fd_gossip_update_msg );

struct __attribute__((packed)) fd_vote_update_msg {
  uchar vote_pubkey[32];	// 0..31
  uchar node_pubkey[32];	// 32..63
  ulong activated_stake;	// 64..71
  ulong last_vote;		// 72..79
  ulong root_slot;		// 80..87
  ulong epoch_credits;		// 88..95
  uchar commission;		// 96
  uchar is_delinquent;		// 97
};
typedef struct fd_vote_update_msg fd_vote_update_msg_t;

FD_STATIC_ASSERT( sizeof(fd_vote_update_msg_t) <= FD_GOSSIP_LINK_MSG_SIZE, fd_vote_update_msg );

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



#define FD_PLUGIN_MSG_BAM_UPDATE           (15UL)

typedef enum {
  FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISABLED            = 0,
  FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED        = 1, // the client has recently failed and is currently backing off from a reconnect attempt
  FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTING          = 2, // the client is currently reconnecting.
  FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY = 3, /* all of below conditions met, but didn't receive heartbeat yet
  - TCP socket is alive
  - SSL session is not in an error state
  - HTTP/2 connection is established (SETTINGS exchange done)
  - gRPC bundle and packet subscriptions are live
  - HTTP/2 PING exchange was done recently */
    FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY   = 4  /* recently exchanged heartbeats within timeout */
} fd_plugin_bam_update_status_t;

typedef struct {
  char  name[ 16 ];
  char  url[ FD_URL_MAX ];
  char  sni[ FD_SNI_BUF_MAX ];
  char  ip_cstr[ 40 ];    /* IPv4 or IPv6 cstr */
  char  tpu_cstr[ 22 ];   /* "a.b.c.d:port" */
  char  tpu_fwd_cstr[ 22 ];
  fd_plugin_bam_update_status_t status_code;           /* FD_PLUGIN_MSG_BAM_UPDATE_STATUS_* */
  uchar enabled;          /* Non-zero when operator enabled BAM */

  /* BAM operator diagnostics mirrored from the BAM tile metrics. */
  float  keepalive_rtt_sample;         /* Latest HTTP/2 keepalive RTT sample (ns) */
  float  keepalive_rtt_smoothed;       /* Smoothed HTTP/2 keepalive RTT estimate (ns) */
  float  keepalive_rtt_deviation;      /* Smoothed HTTP/2 keepalive RTT variation estimate (ns) */
  ushort feedback_queue_depth;         /* Pending BAM feedback results */
  ulong  validator_heartbeats_enqueued; /* Validator heartbeats accepted by gRPC stream_send */
  ulong  builder_heartbeats_decoded;   /* Builder heartbeat messages successfully decoded */
  ulong  transaction_published;        /* Transactions published from BAM to verify */
  ulong  atomic_batch_published;       /* revert_on_error AtomicTxnBatch entries published to verify */
  ulong  ingress_packet_oversize;      /* BAM packets over local MTU */
  ulong  failure_auth_challenge_decode; /* AuthChallengeResponse protobuf/validation failures */
  ulong  failure_config_decode;        /* ConfigResponse protobuf decode failures */
  ulong  failure_scheduler_envelope_decode; /* SchedulerResponse envelope/protobuf decode failures */
  ulong  failure_request_failed;       /* HTTP/gRPC request failures */
  ulong  failure_transport;            /* DNS/socket/connect/I/O failures */
  ulong  failure_unsupported_version;  /* Unsupported BAM scheduler-response versions */
  ulong  failure_timeout;              /* Request/keepalive/heartbeat timeouts */
  ulong  ingress_multi_message_received;
  ulong  ingress_batch_commit_attempt;
  ulong  ingress_batch_published;
  ulong  ingress_batch_rejected_invalid_batch;
  ulong  ingress_batch_rejected_empty_batch;
  ulong  ingress_batch_rejected_vote_transaction;
  ulong  ingress_batch_rejected_non_revert_multi_packet;
  ulong  ingress_message_rejected_empty_message;
  ulong  ingress_message_rejected_overflow_message;
  ulong  leader_slot_end_status_disabled;
  ulong  leader_slot_end_status_disconnected;
  ulong  leader_slot_end_status_connecting;
  ulong  leader_slot_end_status_connected_unhealthy;
  ulong  leader_slot_end_status_connected_healthy;
} fd_plugin_msg_bam_update_t;

#endif /* HEADER_fd_src_disco_plugin_fd_plugin_h */
