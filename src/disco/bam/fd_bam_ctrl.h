#ifndef HEADER_fd_src_disco_bam_fd_bam_ctrl_h
#define HEADER_fd_src_disco_bam_fd_bam_ctrl_h

#include "../../waltz/http/fd_url.h"

#define FD_BAM_CTRL_ERR_MAX 128UL

#define FD_BAM_CTRL_CMD_ENABLE (uchar)(1U<<0)
#define FD_BAM_CTRL_CMD_URL    (uchar)(1U<<1)
#define FD_BAM_CTRL_CMD_SNI    (uchar)(1U<<2)

#define FD_BAM_CTRL_STATE_IDLE      (0)
#define FD_BAM_CTRL_STATE_REQUEST   (1)
#define FD_BAM_CTRL_STATE_APPLYING  (2)
#define FD_BAM_CTRL_STATE_SUCCESS   (3)
#define FD_BAM_CTRL_STATE_ERROR     (4)

typedef struct fd_bam_ctrl {
  uchar state;                       /* FD_BAM_CTRL_STATE_* */
  uchar command;                     /* FD_BAM_CTRL_CMD_* bitset */
  uchar enable;                      /* Desired enable state (0/1), set by set_bam.c */
  char url[ FD_URL_MAX ];   /* Requested URL */
  char sni[ FD_SNI_BUF_MAX ];   /* Requested SNI override (optional) */
  char error[ FD_BAM_CTRL_ERR_MAX ]; /* Error message returned on failure */

  // cached values for get_bam to read from
  // TODO: try and remove these, to avoid having so much state
  uchar current_enable;              /* Currently applied enable flag */
  char current_url[ FD_URL_MAX ]; /* Currently applied URL */
  char current_sni[ FD_SNI_BUF_MAX ]; /* Currently applied SNI */
} fd_bam_ctrl_t;

#endif /* HEADER_fd_src_disco_bam_fd_bam_ctrl_h */
