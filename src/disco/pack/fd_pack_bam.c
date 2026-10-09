/* FireBAM part of fd_pack.c: the BAM bundle-candidate, lookahead and
   BAM bundle lookup/delete functions and the ordinal rebase.  Included
   once by fd_pack.c after the bundle ordinal macros. */

FD_STATIC_ASSERT( offsetof(fd_txn_e_t,alt_accts)+sizeof(((fd_txn_e_t *)0)->alt_accts)==sizeof(fd_txn_e_t), fd_txn_e_alt_accts_last );

/* Reclaim ordinal bands before an insert past BUNDLE_N without changing
   bundle or member order, so FIFO order holds where upstream underflows.
   Pending transactions bound the number of groups, so assign groups
   descending ordinals from that count, reserving zero for the initializer.
   Forward iteration visits each group's last member first: use insertion's
   priority recurrence, without buffering or rebuilding groups.  Treap
   iteration follows links, which the priority rewrite leaves intact. */
static void
fd_pack_rebase_bundle_ordinals( fd_pack_t * pack ) {
  ulong ordinal = treap_ele_cnt( pack->pending_bundles );
  pack->relative_bundle_idx = ordinal+1UL;
  FD_TEST( pack->relative_bundle_idx<=BUNDLE_N ); /* Pool indices are ushorts. */
  ulong old_ordinal = ULONG_MAX;
  ulong prev_reward = 0UL;
  ulong prev_cost = 0UL;
  for( treap_fwd_iter_t iter=treap_fwd_iter_init( pack->pending_bundles, pack->pool );
       !treap_fwd_iter_done( iter ); iter=treap_fwd_iter_next( iter, pack->pool ) ) {
    fd_pack_ord_txn_t * cur = treap_fwd_iter_ele( iter, pack->pool );
    ulong group = RC_TO_REL_BUNDLE_IDX( cur->rewards, cur->compute_est );
    if( group!=old_ordinal ) {
      old_ordinal = group;
      ulong idx = (cur->txn->flags & FD_TXN_P_FLAGS_INITIALIZER_BUNDLE) ? 0UL : ordinal--;
      prev_reward = BUNDLE_L_PRIME*(BUNDLE_N-idx)-1UL;
      prev_cost = 1UL<<32;
    }
    cur->rewards = (uint)(((ulong)cur->compute_est*(prev_reward+1UL)+prev_cost-1UL)/prev_cost);
    prev_reward = cur->rewards;
    prev_cost = cur->compute_est;
  }
}

/* Returns the first whole bundle eligible for the selected mode. */
static inline __attribute__((always_inline)) treap_rev_iter_t
fd_pack_bundle_candidate( fd_pack_t const * pack,
                          _Bool             bam_only,
                          ulong *           skipped_txn_cnt ) {
  fd_pack_ord_txn_t * pool = pack->pool;
  treap_rev_iter_t   iter = treap_rev_iter_init( pack->pending_bundles, pool );

  while( !treap_rev_iter_done( iter ) ) {
    fd_pack_ord_txn_t * cur = treap_rev_iter_ele( iter, pool );
    /* Deferral marks every member of a bundle alike, so, as upstream,
       skip deferred transactions one at a time without touching their
       txn or decoding their bundle. */
    if( FD_UNLIKELY( cur->skip==pack->compressed_slot_number ) ) {
      (*skipped_txn_cnt)++;
      iter = treap_rev_iter_next( iter, pool );
      continue;
    }
    _Bool is_ib      = !!(cur->txn->flags & FD_TXN_P_FLAGS_INITIALIZER_BUNDLE);
    _Bool mode_match = is_ib
                     ? pack->initializer_bundle_bam==bam_only
                     : !bam_only || cur->txn->source_tpu==FD_TXN_M_TPU_SOURCE_BAM;
    if( FD_LIKELY( mode_match ) ) return iter;

    /* Source filtering operates on whole bundles.  Never return an
       iterator into the middle of a bundle. */
    ulong bundle_idx = RC_TO_REL_BUNDLE_IDX( cur->rewards, cur->compute_est );
    do {
      if( FD_UNLIKELY( cur->skip==pack->compressed_slot_number ) ) (*skipped_txn_cnt)++;
      iter = treap_rev_iter_next( iter, pool );
      if( FD_UNLIKELY( treap_rev_iter_done( iter ) ) ) return iter;
      cur = treap_rev_iter_ele( iter, pool );
    } while( RC_TO_REL_BUNDLE_IDX( cur->rewards, cur->compute_est )==bundle_idx );
  }

  return iter;
}

fd_txn_e_t const *
fd_pack_peek_bundle_candidate( fd_pack_t const * pack,
                               _Bool             bam_only,
                               ulong *           bundle_hint,
                               void const **     opt_bundle_meta ) {
  *bundle_hint = ULONG_MAX;
  if( opt_bundle_meta ) *opt_bundle_meta = fd_pack_peek_bundle_meta( pack );
  ulong skipped = 0UL;
  treap_rev_iter_t _cur = fd_pack_bundle_candidate( pack, bam_only, &skipped );
  if( FD_UNLIKELY( treap_rev_iter_done( _cur ) ) ) return NULL; /* empty */

  fd_pack_ord_txn_t * cur = treap_rev_iter_ele( _cur, pack->pool );
  /* Pool indices and the number of skipped pool elements each fit in a
     ushort.  Bit 32 binds the hint to the mode; upper bits bind its lifetime. */
  *bundle_hint = (ulong)_cur | (skipped<<16) | ((ulong)bam_only<<32) | pack->bundle_hint_generation;
  return cur->txn_e;
}

/* Exact predecessor permissions are required here: the normal fast bitsets
   omit accounts when their finite mapping is exhausted.  This bounded
   scratch index contains every predecessor account and resolves hash
   collisions by comparing full keys.  Each chain has at most 5*64 entries. */
typedef struct {
  fd_acct_addr_t const * key;
  ushort                next;
  ushort                bucket;
  uchar                 writable;
} fd_pack_lookahead_acct_t;

/* Called after a conflict that leaves the pending bundles unchanged.
   Return the first ready successor independent of all preceding groups.
   The exact account index covers at most five txns. */
static ulong
fd_pack_bam_independent_candidate( fd_pack_t const *    pack,
                                    int                  schedule_flags,
                                    ulong                head_hint,
                                    fd_pack_bam_ready_fn * ready,
                                    void const *         ready_ctx ) {
  if( FD_UNLIKELY( !ready || !(schedule_flags & FD_PACK_SCHEDULE_BAM_ONLY) ||
                   pack->initializer_bundle_state!=FD_PACK_IB_STATE_READY ) ) return ULONG_MAX;
  ulong generation = pack->bundle_hint_generation;
  treap_rev_iter_t iter = (treap_rev_iter_t)(head_hint & USHORT_MAX);

  fd_pack_lookahead_acct_t accts[ FD_PACK_MAX_TXN_PER_BUNDLE*FD_TXN_ACCT_ADDR_MAX ];
  ushort heads[512];
  fd_memset( heads, 0xff, sizeof(heads) );
  ulong seed = bitset_map_seed( pack->acct_to_bitset );
  ulong acct_cnt = 0UL;
  ulong scanned = 0UL;
  int is_head = 1;
  while( !treap_rev_iter_done( iter ) ) {
    treap_rev_iter_t begin = iter;
    fd_pack_ord_txn_t const * lead = treap_rev_iter_ele_const( iter, pack->pool );
    fd_txn_e_t const * txn0 = lead->txn_e;
    ulong bundle_idx = RC_TO_REL_BUNDLE_IDX( lead->rewards, lead->compute_est );

    fd_pack_ord_txn_t const * group[ FD_PACK_MAX_TXN_PER_BUNDLE ];
    ulong txn_cnt = 0UL;
    do {
      fd_pack_ord_txn_t const * cur = treap_rev_iter_ele_const( iter, pack->pool );
      if( RC_TO_REL_BUNDLE_IDX( cur->rewards, cur->compute_est )!=bundle_idx ) break;
      /* Never enter a partial group or transfer another batch's readiness. */
      if( FD_UNLIKELY( scanned==FD_PACK_MAX_TXN_PER_BUNDLE ||
                       cur->txn->source_tpu!=FD_TXN_M_TPU_SOURCE_BAM ||
                       cur->txn_e->bam.batch_idx!=txn_cnt ||
                       cur->txn_e->bam.seq_id!=txn0->bam.seq_id ||
                       cur->txn_e->bam.scheduler_gen!=txn0->bam.scheduler_gen ||
                       (cur->txn->flags & FD_TXN_P_FLAGS_INITIALIZER_BUNDLE) ||
                       cur->skip==pack->compressed_slot_number ) ) goto empty;
      group[ txn_cnt++ ] = cur;
      scanned++;
      iter = treap_rev_iter_next( iter, pack->pool );
    } while( !treap_rev_iter_done( iter ) );

    if( FD_UNLIKELY( !ready( ready_ctx, txn0, txn_cnt ) || pack->bundle_hint_generation!=generation ) ) goto empty;

    ulong predecessor_cnt = acct_cnt;
    int blocked = is_head || !( (schedule_flags & FD_PACK_SCHEDULE_BUNDLE) || txn_cnt==1UL );
    for( ulong j=0UL; j<txn_cnt; j++ ) {
      fd_txn_t const * txn = TXN(group[j]->txn);
      ulong imm_cnt = txn->acct_addr_cnt;
      ulong cnt = imm_cnt + txn->addr_table_adtl_cnt;
      fd_acct_addr_t const * imm = fd_txn_get_acct_addrs( txn, group[j]->txn->payload );
      for( ulong k=0UL; k<cnt; k++ ) {
        fd_acct_addr_t const * key = k<imm_cnt ? imm+k : group[j]->txn_e->alt_accts+(k-imm_cnt);
        if( fd_pack_unwritable_contains( key ) ) continue;
        int writable = fd_txn_is_writable( txn, (ushort)k );
        ushort bucket = (ushort)(fd_hash32( key->b, seed ) & 511U);
        accts[ acct_cnt++ ] = (fd_pack_lookahead_acct_t){ key, USHORT_MAX, bucket, (uchar)writable };
        if( !blocked ) {
          fd_pack_addr_use_t const * use = acct_uses_query( pack->acct_in_use, *key, NULL );
          blocked = use && (writable ? !!use->in_use_by : !!(use->in_use_by & FD_PACK_IN_USE_WRITABLE));
          if( !blocked ) for( ushort p=heads[bucket]; p!=USHORT_MAX; p=accts[p].next ) {
            if( (writable || accts[p].writable) && !memcmp( key, accts[p].key, sizeof(fd_acct_addr_t) ) ) {
              blocked = 1;
              break;
            }
          }
        }
      }
    }
    if( !blocked )
      return (ulong)begin | (1UL<<32) | generation; /* Head scan already counted deferred skips. */
    /* Index only after examining the entire group: conflicts between
       members of one atomic batch are allowed.  Retain every skipped
       predecessor, including dependent or worker-ineligible batches. */
    for( ulong p=predecessor_cnt; p<acct_cnt; p++ ) {
      ushort bucket = accts[p].bucket;
      accts[p].next = heads[bucket];
      heads[bucket] = (ushort)p;
    }
    is_head = 0;
  }
empty:
  FD_MCNT_INC( PACK, BAM_LOOKAHEAD_EMPTY, 1UL ); /* repeats until pending bundles or locks change */
  return ULONG_MAX;
}

ulong fd_pack_insert_bundle_reject_txn_idx( fd_pack_t const * pack ) { return pack->reject_txn_idx; }

int
fd_pack_contains_initializer_bundle( fd_pack_t const *        pack,
                                     fd_ed25519_sig_t const * sig0,
                                     _Bool                    bam_only ) {
  treap_rev_iter_t iter = treap_rev_iter_init( pack->pending_bundles, pack->pool );
  if( FD_UNLIKELY( treap_rev_iter_done( iter ) ) ) return 0;
  fd_txn_p_t const * txn = treap_rev_iter_ele( iter, pack->pool )->txn;
  return !!(txn->flags & FD_TXN_P_FLAGS_INITIALIZER_BUNDLE) &&
         pack->initializer_bundle_bam==bam_only &&
         !memcmp( fd_txn_get_signatures( TXN(txn), txn->payload ), sig0, sizeof(fd_ed25519_sig_t) );
}

/* The scheduler body is fd_pack.c's, instantiated twice: the unhinted
   copy folds the hint validation and lookahead away for non-BAM nodes. */
static inline __attribute__((always_inline)) ulong
fd_pack_schedule_next_microblock_impl_bundle_hint( fd_pack_t *  pack,
                                                   ulong        total_cus,
                                                   float        vote_fraction,
                                                   ulong        bank_tile,
                                                   int          schedule_flags,
                                                   ulong        bundle_hint,
                                                   fd_pack_bam_ready_fn * ready,
                                                   void const * ready_ctx,
                                                   fd_txn_e_t * out );

ulong
fd_pack_schedule_next_microblock_with_bundle_hint( fd_pack_t *  pack,
                                                   ulong        total_cus,
                                                   float        vote_fraction,
                                                   ulong        bank_tile,
                                                   int          schedule_flags,
                                                   ulong        bundle_hint,
                                                   fd_pack_bam_ready_fn * ready,
                                                   void const * ready_ctx,
                                                   fd_txn_e_t * out ) {
  return fd_pack_schedule_next_microblock_impl_bundle_hint( pack, total_cus, vote_fraction, bank_tile,
                                                            schedule_flags, bundle_hint, ready, ready_ctx, out );
}

ulong
fd_pack_schedule_next_microblock( fd_pack_t *  pack,
                                  ulong        total_cus,
                                  float        vote_fraction,
                                  ulong        bank_tile,
                                  int          schedule_flags,
                                  fd_txn_e_t * out ) {
  return fd_pack_schedule_next_microblock_impl_bundle_hint( pack, total_cus, vote_fraction, bank_tile,
                                                            schedule_flags, ULONG_MAX, NULL, NULL, out );
}

/* Returns the pool index of the pending BAM bundle's leading transaction.
   sig2txn is a multimap, so every equal-signature entry must be checked:
   the same signature can also belong to an unrelated transaction or to a
   non-leading member of another bundle.  When match_identity is zero,
   seq_id and scheduler_gen are ignored. */
static ulong
fd_pack_find_bam_bundle( fd_pack_t const *        pack,
                         fd_ed25519_sig_t const * sig0,
                         uint                     seq_id,
                         ushort                   scheduler_gen,
                         int                      match_identity ) {
  fd_txn_e_t query_e;
  TXN(query_e.txnp)->signature_off = 0U;
  fd_memcpy( query_e.txnp[0].payload, sig0, FD_TXN_SIGNATURE_SZ );
  ulong idx = sig2txn_idx_query_const( pack->signature_map,
                                       &query_e,
                                       ULONG_MAX,
                                       pack->pool );
  while( idx!=ULONG_MAX ) {
    fd_pack_ord_txn_t const * ord = pack->pool + idx;
    if( FD_LIKELY( ord->root==FD_ORD_TXN_ROOT_PENDING_BUNDLE &&
                   ord->txn->source_tpu==FD_TXN_M_TPU_SOURCE_BAM &&
                   ord->txn_e->bam.batch_idx==0U &&
                   ( !match_identity ||
                     ( ord->txn_e->bam.seq_id==seq_id &&
                       ord->txn_e->bam.scheduler_gen==scheduler_gen ) ) ) ) return idx;
    idx = sig2txn_idx_next_const( idx, ULONG_MAX, pack->pool );
  }
  return ULONG_MAX;
}

int
fd_pack_contains_bam_bundle( fd_pack_t const *        pack,
                             fd_ed25519_sig_t const * sig0,
                             uint                     seq_id,
                             ushort                   scheduler_gen,
                             int                      match_identity ) {
  return fd_pack_find_bam_bundle( pack, sig0, seq_id, scheduler_gen, match_identity )!=ULONG_MAX;
}

ulong
fd_pack_delete_bam_bundle( fd_pack_t *              pack,
                           fd_ed25519_sig_t const * sig0,
                           uint                     seq_id,
                           ushort                   scheduler_gen ) {
  fd_pack_invalidate_bundle_hint( pack );
  ulong cnt = 0UL;
  for(;;) {
    ulong idx = fd_pack_find_bam_bundle( pack, sig0, seq_id, scheduler_gen, 1 );
    if( FD_LIKELY( idx==ULONG_MAX ) ) return cnt;
    cnt += delete_transaction( pack, pack->pool+idx, 1, 1 );
  }
}

ulong
fd_pack_delete_non_bam_bundles( fd_pack_t * pack ) {
  fd_pack_invalidate_bundle_hint( pack );
  fd_pack_ord_txn_t * pool = pack->pool;
  ulong cnt = 0UL;
  treap_fwd_iter_t iter = treap_fwd_iter_init( pack->pending_bundles, pool );
  while( !treap_fwd_iter_done( iter ) ) {
    fd_pack_ord_txn_t * cur = treap_fwd_iter_ele( iter, pool );
    ulong bundle_idx = RC_TO_REL_BUNDLE_IDX( cur->rewards, cur->compute_est );
    /* Step past the whole bundle before deleting it.  Iteration links
       are explicit, so deletion leaves the iterator to the next bundle
       valid. */
    do {
      iter = treap_fwd_iter_next( iter, pool );
    } while( !treap_fwd_iter_done( iter ) &&
             RC_TO_REL_BUNDLE_IDX( pool[ iter ].rewards, pool[ iter ].compute_est )==bundle_idx );
    if( FD_LIKELY( cur->txn->source_tpu==FD_TXN_M_TPU_SOURCE_BAM ||
                   (cur->txn->flags & FD_TXN_P_FLAGS_INITIALIZER_BUNDLE) ) ) continue;
    cnt += delete_transaction( pack, cur, 1, 0 );
  }
  return cnt;
}
