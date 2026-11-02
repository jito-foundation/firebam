#ifndef HEADER_fd_src_app_firedancer_main_bam_h
#define HEADER_fd_src_app_firedancer_main_bam_h

/* FireBAM entries of the firedancer and firedancer-dev main tables. */

#include "../shared/fd_action.h"

extern fd_topo_obj_callbacks_t fd_obj_cb_bam_ctrl;
extern fd_topo_obj_callbacks_t fd_obj_cb_bam_fee_cfg;
extern fd_topo_run_tile_t      fd_tile_bam;
extern action_t                fd_action_set_bam;
extern action_t                fd_action_get_bam;

#endif /* HEADER_fd_src_app_firedancer_main_bam_h */
