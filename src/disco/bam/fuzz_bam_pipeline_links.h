#ifndef HEADER_fd_src_disco_bam_fuzz_bam_pipeline_links_h
#define HEADER_fd_src_disco_bam_fuzz_bam_pipeline_links_h

#include "fd_bam_tile.h"

typedef struct {
  fd_wksp_t *      mem;
  fd_frag_meta_t * mcache;
  ulong            chunk0;
  ulong            wmark;
  ulong            depth;
} bam_fuzz_link_t;

static inline void
bam_fuzz_delete_mcache( fd_frag_meta_t ** mcache,
                        void *            expected_mem ) {
  if( FD_UNLIKELY( !*mcache ) ) return;
  void * mem = fd_mcache_delete( fd_mcache_leave( *mcache ) );
  FD_TEST( mem==expected_mem );
  *mcache = NULL;
}

static inline void
bam_fuzz_delete_dcache( uchar ** dcache,
                        void *   expected_mem ) {
  if( FD_UNLIKELY( !*dcache ) ) return;
  void * shmem = fd_dcache_leave( *dcache );
  FD_TEST( shmem );
  FD_TEST( fd_dcache_delete( shmem )==expected_mem );
  *dcache = NULL;
}

static inline void
bam_fuzz_delete_fseq( ulong ** fseq,
                      void *   expected_mem ) {
  if( FD_UNLIKELY( !*fseq ) ) return;
  void * mem = fd_fseq_delete( fd_fseq_leave( *fseq ) );
  FD_TEST( mem==expected_mem );
  *fseq = NULL;
}

#endif /* HEADER_fd_src_disco_bam_fuzz_bam_pipeline_links_h */
