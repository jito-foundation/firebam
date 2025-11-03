#ifndef HEADER_fd_src_disco_bam_fd_bam_ctrl_h
#define HEADER_fd_src_disco_bam_fd_bam_ctrl_h

#include "../../util/fd_util.h"
#include <string.h>

#define FD_BAM_CTRL_URL_MAX 256UL
#define FD_BAM_CTRL_URL_FORMAT_OVERHEAD 15UL /* https:// + port separator + 5 digit port + NULL terminator */
#define FD_BAM_CTRL_SNI_MAX 256UL
#define FD_BAM_CTRL_ERR_MAX 128UL

#define FD_BAM_CTRL_CMD_ENABLE (uchar)(1U<<0)
#define FD_BAM_CTRL_CMD_URL    (uchar)(1U<<1)
#define FD_BAM_CTRL_CMD_SNI    (uchar)(1U<<2)

#define FD_BAM_CTRL_STATE_IDLE      (0L)
#define FD_BAM_CTRL_STATE_REQUEST   (1L)
#define FD_BAM_CTRL_STATE_APPLYING  (2L)
#define FD_BAM_CTRL_STATE_SUCCESS   (3L)
#define FD_BAM_CTRL_STATE_ERROR     (4L)

typedef struct fd_bam_ctrl {
  long state;                        /* FD_BAM_CTRL_STATE_* */
  uchar command;                     /* FD_BAM_CTRL_CMD_* bitset */
  uchar enable;                      /* Desired enable state (0/1) */
  char url[ FD_BAM_CTRL_URL_MAX ];   /* Requested URL */
  char sni[ FD_BAM_CTRL_SNI_MAX ];   /* Requested SNI override (optional) */
  char error[ FD_BAM_CTRL_ERR_MAX ]; /* Error message returned on failure */
  uchar current_enable;              /* Currently applied enable flag */
  char current_url[ FD_BAM_CTRL_URL_MAX ]; /* Currently applied URL */
  char current_sni[ FD_BAM_CTRL_SNI_MAX ]; /* Currently applied SNI */
} fd_bam_ctrl_t;

FD_PROTOTYPES_BEGIN

static inline void
fd_bam_ctrl_copy_str( char *        dst,
                      ulong         dst_sz,
                      char const *  src ) {
  if( FD_UNLIKELY( !dst || !dst_sz ) ) return;
  ulong len = src ? strlen( src ) : 0UL;
  if( len>=dst_sz ) len = dst_sz - 1UL;
  if( len ) fd_memcpy( dst, src, len );
  dst[ len ] = '\0';
  if( FD_UNLIKELY( len+1UL < dst_sz ) ) fd_memset( dst + len + 1UL, 0, dst_sz - len - 1UL );
}

FD_PROTOTYPES_END

#endif /* HEADER_fd_src_disco_bam_fd_bam_ctrl_h */
