ifdef FD_HAS_HOSTED
ifdef FD_HAS_LINUX
$(call add-objs,set_bam get_bam,fdctl_shared)
endif
endif
