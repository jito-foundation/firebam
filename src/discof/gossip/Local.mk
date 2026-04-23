ifdef FD_HAS_HOSTED
ifdef FD_HAS_ALLOCA
$(call add-objs,fd_gossip_tile,fd_discof)
$(call add-objs,fd_gossvf_tile,fd_discof)

$(call make-unit-test,test_gossip_bam_update,test_gossip_bam_update,fd_discof fd_disco fd_flamenco fd_tango fd_waltz fd_ballet fd_reedsol fd_funk fd_util firedancer_version fdctl_shared fdctl_platform)
$(call run-unit-test,test_gossip_bam_update)
endif
endif
