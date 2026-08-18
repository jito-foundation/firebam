#ifndef HEADER_fd_src_disco_bam_fd_bam_ctrl_h
#define HEADER_fd_src_disco_bam_fd_bam_ctrl_h

#include "../../waltz/http/fd_url.h"

#define FD_BAM_CTRL_ERR_MAX 128UL

#define FD_BAM_CTRL_CMD_ENABLE (uchar)(1U<<0)
#define FD_BAM_CTRL_CMD_URL    (uchar)(1U<<1)
#define FD_BAM_CTRL_CMD_SNI    (uchar)(1U<<2)

#define FD_BAM_CTRL_STATE_IDLE      (0) /* No in-flight request; ctrl fields reflect current BAM runtime config. */
#define FD_BAM_CTRL_STATE_REQUEST   (1) /* CLI finished populating request fields; bam tile should claim and apply. */
#define FD_BAM_CTRL_STATE_APPLYING  (2) /* Bam tile owns the request and is mutating runtime state. */
#define FD_BAM_CTRL_STATE_SUCCESS   (3) /* Bam tile applied the request and left updated fields for the CLI to read. */
#define FD_BAM_CTRL_STATE_ERROR     (4) /* Bam tile rejected the request; error[] contains details. */
#define FD_BAM_CTRL_STATE_LOCKED    (5) /* CLI holds an exclusive writer lock while filling request fields. */

typedef struct fd_bam_ctrl {
  uchar state;                       /* FD_BAM_CTRL_STATE_* handoff between CLI (producer) and bam tile (consumer). */
  uchar command;                     /* FD_BAM_CTRL_CMD_* bitset of fields the CLI wants applied. */
  uchar enable;                      /* Desired enable state (0/1) when command includes ENABLE; otherwise ignored. */
  char url[ FD_URL_MAX ];            /* Desired gRPC endpoint; empty string clears URL when command includes URL. */
  char sni[ FD_SNI_BUF_MAX ];        /* (Optional) Desired TLS SNI override; empty string clears override when command includes SNI. */
  char error[ FD_BAM_CTRL_ERR_MAX ]; /* Error message when state==ERROR; cleared by CLI for new requests. */
} fd_bam_ctrl_t;

#endif /* HEADER_fd_src_disco_bam_fd_bam_ctrl_h */
