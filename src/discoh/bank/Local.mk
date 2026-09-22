ifdef FD_HAS_ATOMIC
$(call add-hdrs,fd_bank_abi.h)
$(call add-objs,fd_bank_abi fd_bank_tile,fd_discoh)
endif

ifdef FD_HAS_HOSTED
ifdef FD_HAS_ATOMIC
$(call make-unit-test,test_bank_abi,test_bank_abi,fd_flamenco fd_ballet fd_util)
$(call run-unit-test,test_bank_abi)
endif
endif
