/* FireBAM resolh tile tests.  The tile is included so the shared BAM
   resolve harness can drive its callbacks. */

#define FD_TILE_TEST 1
#include "../../discoh/resolh/fd_resolh_tile.c"

void
fd_ext_bank_release( void const * bank ) {
  (void)bank;
}

int
fd_ext_bank_load_account( void const *  bank,
                          int           fixed_root,
                          uchar const * addr,
                          uchar *       owner,
                          uchar *       data,
                          ulong *       data_sz ) {
  (void)bank;
  (void)fixed_root;
  (void)addr;
  (void)owner;
  (void)data;
  (void)data_sz;
  return 0;
}

#define TEST_BAM_RESOLVE_CTX_T                    fd_resolh_tile_t
#define TEST_BAM_RESOLVE_OUT_CNT                  1UL
#define TEST_BAM_RESOLVE_IN_KIND                  FD_RESOLH_IN_KIND_FRAGMENT
#define TEST_BAM_RESOLVE_OUT( ctx, f ) ((ctx)->out_##f)
#include "test_bam_resolve_common.c"
