ifdef FD_HAS_HOSTED
ifdef FD_HAS_ALLOCA
$(call add-objs,fd_resolh_tile,fd_discoh)
$(call make-unit-test,test_resolh_tile_bam,test_resolh_tile_bam,fd_discoh fd_disco fd_flamenco fd_tango fd_ballet fd_util)
$(call run-unit-test,test_resolh_tile_bam)
endif
endif
