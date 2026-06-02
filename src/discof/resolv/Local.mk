ifdef FD_HAS_HOSTED
ifdef FD_HAS_ALLOCA
$(call add-objs,fd_resolv_tile,fd_discof)
$(call make-unit-test,test_resolv_tile_bam,test_resolv_tile_bam,fd_discof fd_disco fd_flamenco fd_vinyl fd_funk fd_tango fd_ballet fd_util,$(SECP256K1_LIBS))
$(call run-unit-test,test_resolv_tile_bam)
endif
endif
