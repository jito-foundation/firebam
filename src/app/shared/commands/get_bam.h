#ifndef HEADER_fd_src_app_shared_commands_get_bam_h
#define HEADER_fd_src_app_shared_commands_get_bam_h

#include "../fd_config.h"

union fdctl_args;
struct fd_action;

typedef union fdctl_args args_t;
typedef struct fd_action action_t;

FD_PROTOTYPES_BEGIN

void get_bam_cmd_fn( args_t * args, config_t * config );

FD_PROTOTYPES_END

extern action_t fd_action_get_bam;

#endif /* HEADER_fd_src_app_shared_commands_get_bam_h */
