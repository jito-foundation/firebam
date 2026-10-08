#ifndef HEADER_fd_src_disco_pack_fd_pack_bam_h
#define HEADER_fd_src_disco_pack_fd_pack_bam_h

/* FireBAM extensions to the fd_pack API.  Included at the end of
   fd_pack.h and implemented in fd_pack_bam.c, which fd_pack.c
   includes. */

#include "fd_pack.h"

FD_PROTOTYPES_BEGIN

#define FD_PACK_IB_TYPE_NONE   (0)
#define FD_PACK_IB_TYPE_NORMAL (1)
#define FD_PACK_IB_TYPE_BAM    (2)

/* fd_pack_insert_bundle_reject_txn_idx returns the index of the
   transaction that caused the last fd_pack_insert_bundle_fini to reject
   its bundle, or ULONG_MAX if that call accepted the bundle or rejected
   it for a bundle-level reason. */
FD_FN_PURE ulong fd_pack_insert_bundle_reject_txn_idx( fd_pack_t const * pack );

/* Returns the actual first whole candidate for the selected ownership
   mode, including initializers and candidates in Pending/Failed state.
   This is a read-only view, not permission to execute.  Pointer and hint
   expire on insert fini, schedule, delete, expire, end/clear block,
   initializer-state update, or rebate; insert init/cancel leave them valid.
   No candidate yields NULL and ULONG_MAX.  Callers must recompute target-slot
   readiness within this lifetime; scheduling rejects stale or mode-mismatched
   BAM readiness hints.

   bundle_hint receives an opaque value that lets an immediately following
   schedule avoid walking the pending bundle treap a second time.

   When opt_bundle_meta is non-NULL, it receives fd_pack_peek_bundle_meta,
   which describes the head of all pending bundles, not necessarily the
   candidate. */
fd_txn_e_t const * fd_pack_peek_bundle_candidate( fd_pack_t const * pack,
                                                  _Bool             bam_only,
                                                  ulong *           bundle_hint,
                                                  void const **     opt_bundle_meta );

/* Read-only BAM admission check used for each whole group considered by
   bounded conflict lookahead.  Return nonzero only when this exact batch,
   including txn_cnt members, is ready in the current slot and ownership
   generation.  A zero result is a barrier.  This callback must not mutate
   pack or retain any pointer it receives. */
typedef int fd_pack_bam_ready_fn( void const *       ctx,
                                 fd_txn_e_t const * candidate,
                                 ulong              txn_cnt );

/* Tests the queued initializer's full signature and ownership mode, even
   when it is deferred.  A dispatched initializer is no longer queued. */
int fd_pack_contains_initializer_bundle( fd_pack_t const *        pack,
                                         fd_ed25519_sig_t const * sig0,
                                         _Bool                    bam_only );

#define FD_PACK_SCHEDULE_BAM_ONLY 8
#define FD_PACK_SCHEDULE_BAM_SINGLE 16
#define FD_PACK_SCHEDULE_BAM_READY  32

/* The BAM_ONLY bit suppresses normal transactions and filters bundles to
   BAM work, but does not suppress votes.  BAM_SINGLE, like BUNDLE, enables
   bundle scheduling, but grants worker eligibility only for a non-initializer,
   one-transaction BAM batch.  Only bounded conflict lookahead can select
   a younger candidate.  It does not spend earlier batches'
   capacity-deferral budget.
   BUNDLE retains full worker eligibility and ordinary capacity deferral.
   Both require BAM_READY for BAM batches and BAM initializers.  BAM_READY
   certifies that this exact candidate is ready in the active leader bank
   and must be passed with a live mode-matched candidate hint.  The
   unhinted scheduling entry point cannot dispatch BAM work.  Capacity
   failures retain their normal policy; deferred batches
   are never resurrected. */

/* fd_pack_schedule_next_microblock_with_bundle_hint is identical to
   fd_pack_schedule_next_microblock, except that it reuses a bundle candidate
   returned through fd_pack_peek_bundle_candidate.
   Pass ULONG_MAX to opt out of the hint (and BAM readiness).

   When ready is non-NULL, a conflict on the common BAM head may trigger
   lookahead over at most five transactions.  This applies only in BAM_ONLY
   mode with initializer state Ready.  Every complete predecessor and the
   candidate must pass ready(ready_ctx,...).  Skipped batches remain account
   dependencies, including batches this worker cannot execute.  Initializers,
   deferred, incomplete and foreign batches remain barriers.  Capacity
   failures never authorize lookahead and deferred work is never revived.
   The ordinary successful path does not invoke the callback or scan. */
ulong
fd_pack_schedule_next_microblock_with_bundle_hint( fd_pack_t  * pack,
                                                   ulong        total_cus,
                                                   float        vote_fraction,
                                                   ulong        bank_tile,
                                                   int          schedule_flags,
                                                   ulong        bundle_hint,
                                                   fd_pack_bam_ready_fn * ready,
                                                   void const * ready_ctx,
                                                   fd_txn_e_t * out );

/* fd_pack_contains_bam_bundle returns 1 when pack contains the pending
   BAM bundle identified by its leading transaction signature.  When
   match_identity is nonzero, the scheduler generation and sequence ID
   must match as well.  Signatures belonging to unrelated transactions or
   non-leading bundle members do not count as a match. */
int fd_pack_contains_bam_bundle( fd_pack_t const *        pack,
                                 fd_ed25519_sig_t const * sig0,
                                 uint                     seq_id,
                                 ushort                   scheduler_gen,
                                 int                      match_identity );

/* fd_pack_delete_bam_bundle deletes pending BAM bundles matching the leading
   signature, scheduler generation, and sequence ID exactly.  Returns the
   number of transactions deleted.  Unrelated transactions and overlapping
   bundles that happen to carry sig0 are left intact. */
ulong fd_pack_delete_bam_bundle( fd_pack_t *              pack,
                                 fd_ed25519_sig_t const * sig0,
                                 uint                     seq_id,
                                 ushort                   scheduler_gen );

/* fd_pack_delete_non_bam_bundles deletes every pending bundle that is
   neither a BAM bundle nor an initializer bundle, i.e. Block Engine
   bundles.  Returns the number of transactions deleted.  This is an
   explicit delete, so it does not advance fd_pack_bundle_evicted_cnt. */
ulong fd_pack_delete_non_bam_bundles( fd_pack_t * pack );

/* fd_pack_bundle_evicted_cnt returns a monotonically increasing count of
   bundles that pack evicted on its own initiative, i.e. to make room for
   an insert rather than in response to an explicit delete API.  Pack does
   not report which bundle went away, so a caller that tracks pending BAM
   bundles externally should poll this and, when it moves, reconcile its
   own view with fd_pack_contains_bam_bundle.  Never reset, including by
   fd_pack_clear_all, so that a stale snapshot cannot miss an edge. */
#define FD_PACK_BUNDLE_EVICTED_CNT_OFF 88
FD_FN_PURE static inline ulong
fd_pack_bundle_evicted_cnt( fd_pack_t const * pack ) {
  return *((ulong const *)((uchar const *)pack + FD_PACK_BUNDLE_EVICTED_CNT_OFF));
}

FD_PROTOTYPES_END

#endif /* HEADER_fd_src_disco_pack_fd_pack_bam_h */
