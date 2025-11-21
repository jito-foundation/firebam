#ifndef HEADER_fd_pack_bam_result_h
#define HEADER_fd_pack_bam_result_h

#include "../../util/fd_util.h"
#include "../bam/fd_bam_types.h"
#include "fd_pack.h"

void
fd_pack_assign_bam_failure_reason( fd_bam_bundle_result_t * res,
                                   uchar                    idx,
                                   int                      result );

#endif /* HEADER_fd_pack_bam_result_h */
