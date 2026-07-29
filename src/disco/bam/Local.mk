ifdef FD_HAS_INT128
$(call add-hdrs,fd_bam_types.h fd_bam_tile.h fd_bam_publish.h)
$(call add-objs,fd_bam_admin_rpc,fd_disco)
$(call add-objs,fd_bam_client,fd_disco)
$(call add-objs,fd_bam_client_decode,fd_disco)
ifdef FD_HAS_DOUBLE
$(call add-objs,fd_bam_tile,fd_disco)
$(call make-unit-test,test_bam_tile,test_bam_tile,fd_discof fd_disco fd_waltz fd_flamenco fd_tango fd_ballet fd_util,$(OPENSSL_LIBS))
$(call run-unit-test,test_bam_tile)
endif
ifdef FD_HAS_HOSTED
$(call make-unit-test,test_bam_admin_rpc,test_bam_admin_rpc,fd_disco fd_util)
$(call run-unit-test,test_bam_admin_rpc)
$(call make-fuzz-test,fuzz_bam_client,fuzz_bam_client,fd_disco fd_waltz fd_flamenco fd_tango fd_ballet fd_util,$(OPENSSL_LIBS))
$(call make-fuzz-test,fuzz_bam_pipeline_stateful,fuzz_bam_pipeline_stateful fuzz_bam_pipeline_stage_verify fuzz_bam_pipeline_stage_dedup fuzz_bam_pipeline_stage_resolv fuzz_bam_pipeline_stage_pack fuzz_bam_pipeline_stage_execle,fd_discof fd_disco fd_flamenco_test fd_waltz fd_flamenco fd_funk fd_tango fd_ballet fd_util,$(OPENSSL_LIBS))
endif
endif
