ifdef FD_HAS_HOSTED
$(call add-objs,fd_pohh_tile,fd_discoh)
$(call make-unit-test,test_pohh_tile,test_pohh_tile,fd_discoh fd_disco fd_flamenco fd_tango fd_ballet fd_util)
$(call run-unit-test,test_pohh_tile)
endif
