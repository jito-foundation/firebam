ifdef FD_HAS_INT128
$(call add-hdrs,fd_bam_types.h fd_bam_tile.h fd_bam_publish.h fd_bam_microblock.h)
$(call add-objs,fd_bam_admin_rpc fd_bam_client fd_bam_client_decode,fd_disco)

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
$(call make-fuzz-test,fuzz_bam_client,fuzz_bam_client,fd_disco fd_waltz fd_tls fd_flamenco fd_tango fd_ballet fd_util)

# Upstream tiles with BAM traffic, in pipeline order
$(call make-unit-test,test_bam_verify_tile,test_bam_verify_tile,fd_disco fd_waltz fd_ballet fd_tango fd_util)
$(call run-unit-test,test_bam_verify_tile)
$(call make-unit-test,test_dedup_tile_bam,test_dedup_tile_bam,fd_disco fd_ballet fd_tango fd_util)
$(call run-unit-test,test_dedup_tile_bam)
$(call make-unit-test,test_resolv_tile_bam,test_resolv_tile_bam,fd_discof fd_disco fd_flamenco_test fd_flamenco fd_tango fd_ballet fd_util,$(SECP256K1_LIBS))
$(call run-unit-test,test_resolv_tile_bam)
$(call make-unit-test,test_resolh_tile_bam,test_resolh_tile_bam,fd_discoh fd_disco fd_flamenco fd_tango fd_ballet fd_util)
$(call run-unit-test,test_resolh_tile_bam)
$(call make-unit-test,test_pack_bam,test_pack_bam,fd_disco fd_ballet fd_util)
$(call run-unit-test,test_pack_bam)
$(call make-unit-test,test_execle_tile_bam,test_execle_tile_bam test_bam_poh_fixture,fd_discof fd_disco fd_flamenco_test fd_flamenco fd_waltz fd_tango fd_ballet fd_util)
$(call run-unit-test,test_execle_tile_bam)
$(call make-unit-test,test_bundle_exec_bam,test_bundle_exec_bam,fd_flamenco_test fd_flamenco fd_ballet fd_util)
$(call run-unit-test,test_bundle_exec_bam)
$(call make-unit-test,test_poh_tile_bam,test_poh_tile_bam,fd_discof fd_disco fd_flamenco fd_tango fd_ballet fd_util)
$(call run-unit-test,test_poh_tile_bam)
$(call make-unit-test,test_motor_tile_bam,test_motor_tile_bam,fd_discof fd_disco fd_choreo fd_flamenco fd_reedsol fd_waltz fd_tango fd_ballet fd_util)
$(call run-unit-test,test_motor_tile_bam)
$(call make-unit-test,test_pohh_tile_bam,test_pohh_tile_bam,fd_discoh fd_disco fd_flamenco fd_tango fd_ballet fd_util)
$(call run-unit-test,test_pohh_tile_bam)
$(call make-unit-test,test_bam_replay_tile,test_bam_replay_tile,fd_discof fd_choreo fd_disco fd_flamenco fd_vinyl fd_tango fd_ballet fd_util_extra fd_util)
$(call run-unit-test,test_bam_replay_tile)
$(call make-unit-test,test_bam_gossip_glue,test_bam_gossip_glue,fd_discof fd_choreo fd_disco fd_flamenco fd_waltz fd_tango fd_ballet fd_util)
$(call run-unit-test,test_bam_gossip_glue)
$(call make-fuzz-test,fuzz_bam_pipeline_stateful,fuzz_bam_pipeline_stateful fuzz_bam_pipeline_stage_verify fuzz_bam_pipeline_stage_dedup fuzz_bam_pipeline_stage_resolv fuzz_bam_pipeline_stage_pack fuzz_bam_pipeline_stage_execle,fd_discof fd_disco fd_flamenco_test fd_waltz fd_tls fd_flamenco fd_funk fd_tango fd_ballet fd_util)

# Explicit opt-in performance fixture; not part of the automatic unit suite.
$(call make-unit-test,test_bam_latency_bench,test_bam_latency_bench test_bam_poh_fixture,fd_discof fd_disco fd_flamenco_test fd_flamenco fd_waltz fd_tango fd_ballet fd_util)

ifdef FD_HAS_DOUBLE
$(call make-unit-test,test_pack_tile_bam,test_pack_tile_bam,fd_disco fd_tls fd_waltz fd_flamenco fd_tango fd_ballet fd_util)
$(call run-unit-test,test_pack_tile_bam)
$(call make-unit-test,test_bundle_tile_bam,test_bundle_tile_bam,fd_disco fd_waltz fd_tls fd_flamenco fd_tango fd_ballet fd_util)
$(call run-unit-test,test_bundle_tile_bam)
endif

ifdef FD_HAS_ATOMIC
$(call make-unit-test,test_bank_abi_bam,test_bank_abi_bam,fd_flamenco fd_ballet fd_util)
$(call run-unit-test,test_bank_abi_bam)
endif

ifdef FD_HAS_LINUX
$(call make-unit-test,test_genesis_create_bam,test_genesis_create_bam,fddev_shared fd_disco fd_flamenco fd_ballet fd_util)
$(call run-unit-test,test_genesis_create_bam)
$(call make-unit-test,test_keyguard_bam,test_keyguard_bam,fd_disco fd_flamenco fd_tls fd_ballet fd_util)
$(call run-unit-test,test_keyguard_bam)
$(call make-unit-test,test_keyload_bam,test_keyload_bam,fd_disco fd_flamenco fd_tls fd_ballet fd_util)
$(call run-unit-test,test_keyload_bam)
$(call make-unit-test,test_sign_tile_bam,test_sign_tile_bam,fd_disco fd_flamenco fd_tls fd_tango fd_ballet fd_util)
$(call run-unit-test,test_sign_tile_bam)
$(call make-unit-test,test_config_parse_bam,test_config_parse_bam,fd_fdctl fdctl_shared fdctl_platform fd_disco fd_waltz fd_ballet fd_tango fd_util)
$(call run-unit-test,test_config_parse_bam)
endif
endif
endif
