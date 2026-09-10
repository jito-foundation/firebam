ifdef FD_HAS_ATOMIC
$(call add-objs,fd_execle_tile,fd_discof)
ifdef FD_HAS_HOSTED
$(call make-unit-test,test_execle_tile,test_execle_tile test_bam_poh_fixture,fd_discof fd_disco fd_flamenco_test fd_flamenco fd_tango fd_ballet fd_util)
$(call run-unit-test,test_execle_tile)
# Explicit opt-in performance fixture; not part of the automatic unit suite.
$(call make-unit-test,test_bam_latency_bench,test_bam_latency_bench test_bam_poh_fixture,fd_discof fd_disco fd_flamenco_test fd_flamenco fd_tango fd_ballet fd_util)
endif
endif
