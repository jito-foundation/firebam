ifdef FD_HAS_HOSTED
$(call add-objs,fd_poh_tile fd_poh,fd_discof)
$(call make-unit-test,test_poh_tile,test_poh_tile,fd_discof fd_disco fd_flamenco fd_tango fd_ballet fd_util)
$(call run-unit-test,test_poh_tile)
endif
