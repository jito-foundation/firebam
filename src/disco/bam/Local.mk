ifdef FD_HAS_INT128
$(call add-hdrs,fd_bam_types.h fd_bam_tile.h)
$(call add-objs,fd_bam_client,fd_disco)
$(call add-objs,fd_bam_client_decode,fd_disco)
ifdef FD_HAS_DOUBLE
$(call add-objs,fd_bam_tile,fd_disco)
$(call make-unit-test,test_bam_tile,test_bam_tile,fd_disco fd_waltz fd_flamenco fd_tango fd_ballet fd_util,$(OPENSSL_LIBS))
$(call run-unit-test,test_bam_tile)
endif
ifdef FD_HAS_HOSTED
$(call make-fuzz-test,fuzz_bam_client,fuzz_bam_client,fd_disco fd_waltz fd_flamenco fd_tango fd_ballet fd_util,$(OPENSSL_LIBS))
endif
endif
