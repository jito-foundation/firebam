ifdef FD_HAS_HOSTED
$(call add-objs,fd_resolh_tile,fd_discoh)

$(OBJDIR)/obj/discoh/resolh/test_resolh_tile_bam_from_tile.o: src/discoh/resolh/fd_resolh_tile.c src/disco/bam/test_bam_resolve_common.c $(OBJDIR)/info
	$(MKDIR) $(dir $@) && \
$(CC) $(CPPFLAGS) $(CFLAGS) $(DEPFLAGS) -DFD_TILE_TEST -DFD_RESOLH_TILE_BAM_UNIT_TEST -c $< -o $@

$(call make-unit-test,test_resolh_tile_bam,test_resolh_tile_bam_from_tile,fd_discoh fd_disco fd_flamenco fd_tango fd_ballet fd_util)
$(call run-unit-test,test_resolh_tile_bam)
endif
