/* FireBAM resolv tile tests.  The tile is included so the shared BAM
   resolve harness can drive its callbacks. */

#define FD_TILE_TEST 1
#include "../../discof/resolv/fd_resolv_tile.c"

char const *
fd_vinyl_strerror( int err ) {
  (void)err;
  return "test vinyl stub";
}

#define TEST_BAM_RESOLVE_CTX_T       fd_resolv_ctx_t
#define TEST_BAM_RESOLVE_OUT_CNT     2UL
#define TEST_BAM_RESOLVE_HAS_REPLAY  1
#define TEST_BAM_RESOLVE_IN_KIND     IN_KIND_DEDUP
#define TEST_BAM_RESOLVE_OUT( ctx, f ) ((ctx)->out_pack->f)
#include "test_bam_resolve_common.c"
