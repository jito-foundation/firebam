#ifndef HEADER_fd_src_discof_execle_test_bam_poh_fixture_h
#define HEADER_fd_src_discof_execle_test_bam_poh_fixture_h

#include "../../disco/bam/fd_bam_types.h"
#include "../../disco/fd_txn_p.h"
#include "../../util/wksp/fd_wksp.h"

/* Test-only adapter around Full Firedancer's real returnable_frag callback.
   Ledger bytes are recovered from PoH's shred output, independently of Pack
   dispatch output.  The fixture does not claim validator root confirmation. */
typedef struct test_bam_poh_fixture test_bam_poh_fixture_t;

struct test_bam_poh_summary {
  ulong slot;
  uint  expect_pack_idx;
  ulong txn_cnt;
  ulong result_cnt;
  fd_txn_p_t txns[16];
  fd_bam_bundle_result_t results[16];
};
typedef struct test_bam_poh_summary test_bam_poh_summary_t;

test_bam_poh_fixture_t * test_bam_poh_fixture_new( fd_wksp_t * wksp, ulong slot, uint pack_idx );
int test_bam_poh_fixture_consume( test_bam_poh_fixture_t * fixture, ulong worker, ulong sig, void const * fragment, ulong sz );
test_bam_poh_summary_t const * test_bam_poh_fixture_summary( test_bam_poh_fixture_t const * fixture );

#endif
