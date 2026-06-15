ifdef FD_HAS_HOSTED
$(call add-objs,fd_resolv_tile,fd_discof)

$(OBJDIR)/obj/discof/resolv/test_resolv_tile_bam_from_tile.o: src/discof/resolv/fd_resolv_tile.c src/disco/bam/test_bam_resolve_common.c $(OBJDIR)/info
	$(MKDIR) $(dir $@) && \
$(CC) $(CPPFLAGS) $(CFLAGS) $(DEPFLAGS) -DBLOCKHASH_LG_RING_CNT=4UL -DFD_TILE_TEST -DFD_RESOLV_TILE_BAM_UNIT_TEST -c $< -o $@

$(call make-unit-test,test_resolv_tile_bam,test_resolv_tile_bam_from_tile,fd_discof fd_disco fd_flamenco fd_vinyl fd_funk fd_tango fd_ballet fd_util,$(SECP256K1_LIBS))
$(call run-unit-test,test_resolv_tile_bam)
endif
