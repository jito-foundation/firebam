ifdef FD_HAS_INT128
$(call add-hdrs,fd_bam_types.h fd_bam_tile.h fd_bam_publish.h fd_bam_microblock.h)
$(call add-objs,fd_bam_admin_rpc,fd_disco)
$(call add-objs,fd_bam_client,fd_disco)
$(call add-objs,fd_bam_client_decode,fd_disco)
ifdef FD_HAS_DOUBLE
$(call add-objs,fd_bam_tile,fd_disco)
$(call make-unit-test,test_bam_tile,test_bam_tile,fd_discof fd_disco fd_waltz fd_tls fd_flamenco fd_tango fd_ballet fd_util)
$(call run-unit-test,test_bam_tile)
$(call make-unit-test,test_bam_gui_waterfall,test_bam_gui_waterfall,fd_disco fd_choreo fd_flamenco fd_waltz fd_tango fd_ballet fd_util)
$(call run-unit-test,test_bam_gui_waterfall)
endif
ifdef FD_HAS_HOSTED
$(call make-unit-test,test_bam_admin_rpc,test_bam_admin_rpc,fd_disco fd_util)
$(call run-unit-test,test_bam_admin_rpc)
$(call make-unit-test,test_bam_event_tile,test_bam_event_tile,fd_disco fd_waltz fd_tls fd_flamenco fd_tango fd_ballet fd_util)
$(call run-unit-test,test_bam_event_tile)
$(call make-unit-test,test_bam_replay_tile,test_bam_replay_tile,fd_discof fd_choreo fd_disco fd_flamenco fd_vinyl fd_tango fd_ballet fd_util_extra fd_util)
$(call run-unit-test,test_bam_replay_tile)
$(call make-unit-test,test_bam_reward_runtime,test_bam_reward_runtime,fd_discof fd_choreo fd_disco fd_flamenco_test fd_flamenco fd_vinyl fd_tango fd_ballet fd_util_extra fd_util)
$(call run-unit-test,test_bam_reward_runtime)
$(call make-unit-test,test_bam_gossip_glue,test_bam_gossip_glue,fd_discof fd_choreo fd_disco fd_flamenco fd_waltz fd_tango fd_ballet fd_util)
$(call run-unit-test,test_bam_gossip_glue)
$(call make-unit-test,test_bam_verify_tile,test_bam_verify_tile,fd_disco fd_waltz fd_ballet fd_tango fd_util)
$(call run-unit-test,test_bam_verify_tile)
$(call make-fuzz-test,fuzz_bam_client,fuzz_bam_client,fd_disco fd_waltz fd_tls fd_flamenco fd_tango fd_ballet fd_util)

$(call make-fuzz-test,fuzz_bam_pipeline_stateful,fuzz_bam_pipeline_stateful fuzz_bam_pipeline_stage_verify fuzz_bam_pipeline_stage_dedup fuzz_bam_pipeline_stage_resolv fuzz_bam_pipeline_stage_pack fuzz_bam_pipeline_stage_execle,fd_discof fd_disco fd_flamenco_test fd_waltz fd_tls fd_flamenco fd_funk fd_tango fd_ballet fd_util)
endif
endif
