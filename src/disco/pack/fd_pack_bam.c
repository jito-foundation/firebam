/* FireBAM part of fd_pack.c: the BAM hooks fd_pack.c calls, the BAM
   bundle-candidate, lookahead and BAM bundle lookup/delete functions
   and the ordinal rebase.  Included once by fd_pack.c after the bundle
   ordinal macros. */

FD_STATIC_ASSERT( offsetof(fd_txn_e_t,alt_accts)+sizeof(((fd_txn_e_t *)0)->alt_accts)==sizeof(fd_txn_e_t), fd_txn_e_alt_accts_last );
FD_STATIC_ASSERT( offsetof(fd_pack_t, bundle_evicted_cnt)==FD_PACK_BUNDLE_EVICTED_CNT_OFF, bundle_evicted_cnt_off );

/* Called by fd_pack_insert_bundle_fini before validating the bundle.
   BAM slot and generation checks retire BAM work, not blockhash age.
   Returns whether this is a BAM bundle. */
static inline int
fd_pack_bam_insert_bundle_prepare( fd_pack_t          * pack,
                                   fd_txn_e_t * const * bundle,
                                   int                  initializer_bundle,
                                   ulong              * expires_at ) {
  int is_bam = initializer_bundle==FD_PACK_IB_TYPE_BAM || bundle[ 0 ]->txnp->source_tpu==FD_TXN_M_TPU_SOURCE_BAM;
  if( FD_UNLIKELY( is_bam ) ) {
    *expires_at = ULONG_MAX;
    pack->bam_enabled = 1;
  }
  return is_bam;
}

/* Merge active and deferred bundle treaps by descending old ordinal so
   reclaiming ordinal bands preserves global FIFO order across both. */
static void
fd_pack_rebase_bundle_ordinals( fd_pack_t * pack ) {
  ulong ordinal = treap_ele_cnt( pack->pending_bundles ) + treap_ele_cnt( pack->deferred_bundles );
  pack->relative_bundle_idx = ordinal+1UL;
  FD_TEST( pack->relative_bundle_idx<=BUNDLE_N ); /* Pool indices are ushorts. */
  fd_pack_ord_txn_t * pool = pack->pool;
  treap_fwd_iter_t iters[ 2 ] = {
    treap_fwd_iter_init( pack->pending_bundles,  pool ),
    treap_fwd_iter_init( pack->deferred_bundles, pool )
  };
  for(;;) {
    int p_done = treap_fwd_iter_done( iters[ 0 ] );
    int d_done = treap_fwd_iter_done( iters[ 1 ] );
    if( FD_UNLIKELY( p_done & d_done ) ) break;

    ulong pick;
    if( p_done )      pick = 1UL;
    else if( d_done ) pick = 0UL;
    else {
      fd_pack_ord_txn_t const * p = treap_fwd_iter_ele_const( iters[ 0 ], pool );
      fd_pack_ord_txn_t const * d = treap_fwd_iter_ele_const( iters[ 1 ], pool );
      ulong group_p = RC_TO_REL_BUNDLE_IDX( p->rewards, p->compute_est );
      ulong group_d = RC_TO_REL_BUNDLE_IDX( d->rewards, d->compute_est );
      pick = (ulong)(group_d > group_p);
    }

    fd_pack_ord_txn_t * cur = treap_fwd_iter_ele( iters[ pick ], pool );
    ulong group       = RC_TO_REL_BUNDLE_IDX( cur->rewards, cur->compute_est );
    ulong idx         = (cur->txn->flags & FD_TXN_P_FLAGS_INITIALIZER_BUNDLE) ? 0UL : ordinal--;
    ulong prev_reward = BUNDLE_L_PRIME*(BUNDLE_N-idx)-1UL;
    ulong prev_cost   = 1UL<<32;
    do {
      cur->rewards  = (uint)(((ulong)cur->compute_est*(prev_reward+1UL)+prev_cost-1UL)/prev_cost);
      prev_reward   = cur->rewards;
      prev_cost     = cur->compute_est;
      iters[ pick ] = treap_fwd_iter_next( iters[ pick ], pool );
      if( FD_UNLIKELY( treap_fwd_iter_done( iters[ pick ] ) ) ) break;
      cur = treap_fwd_iter_ele( iters[ pick ], pool );
    } while( RC_TO_REL_BUNDLE_IDX( cur->rewards, cur->compute_est )==group );
  }
}

static inline treap_rev_iter_t
fd_pack_peek_bundle_head( fd_pack_t const * pack ) {
  treap_rev_iter_t p_it = treap_rev_iter_init( pack->pending_bundles,  pack->pool );
  treap_rev_iter_t d_it = treap_rev_iter_init( pack->deferred_bundles, pack->pool );
  if( FD_LIKELY( treap_rev_iter_done( d_it ) ) ) return p_it;
  if( FD_UNLIKELY( treap_rev_iter_done( p_it ) ) ) return d_it;
  fd_pack_ord_txn_t const * p = treap_rev_iter_ele_const( p_it, pack->pool );
  fd_pack_ord_txn_t const * d = treap_rev_iter_ele_const( d_it, pack->pool );
  return (RC_TO_REL_BUNDLE_IDX( d->rewards, d->compute_est ) < RC_TO_REL_BUNDLE_IDX( p->rewards, p->compute_est )) ? d_it : p_it;
}

static void
fd_pack_defer_bundle( fd_pack_t *      pack,
                      treap_t *        src_treap,
                      treap_rev_iter_t txn0,
                      ulong            bundle_idx,
                      int              is_pre_rotation,
                      int              is_bam ) {
  fd_pack_ord_txn_t * pool = pack->pool;
  treap_rev_iter_t next;
  for( treap_rev_iter_t cur_it=txn0; !treap_rev_iter_done( cur_it ); cur_it=next ) {
    fd_pack_ord_txn_t * cur = treap_rev_iter_ele( cur_it, pool );
    if( FD_UNLIKELY( RC_TO_REL_BUNDLE_IDX( cur->rewards, cur->compute_est )!=bundle_idx ) ) break;
    next = treap_rev_iter_next( cur_it, pool );

    /* See fd_pack_schedule_impl for this branchless countdown. */
    cur->skip = (ushort)(1+fd_ushort_min( (ushort)(pack->compressed_slot_number-1),
          (ushort)(fd_ushort_min( cur->skip, FD_PACK_BUNDLE_SKIP_CNT )-2) ) );
    if( FD_UNLIKELY( cur->skip==pack->compressed_slot_number ) ) {
      if( FD_UNLIKELY( is_bam && cur_it==txn0 ) ) FD_MCNT_INC( PACK, BAM_CAPACITY_DEFERRED, 1UL );
      if( FD_LIKELY( !is_pre_rotation ) ) {
        treap_ele_remove( src_treap,              cur, pool );
        cur->root = FD_ORD_TXN_ROOT_DEFERRED_BUNDLE;
        treap_ele_insert( pack->deferred_bundles, cur, pool );
      }
    }
  }
}

static void
fd_pack_restore_deferred_bundles( fd_pack_t * pack ) {
  fd_pack_ord_txn_t * pool = pack->pool;
  treap_fwd_iter_t next;
  for( treap_fwd_iter_t it=treap_fwd_iter_init( pack->deferred_bundles, pool );
       !treap_fwd_iter_done( it ); it=next ) {
    next = treap_fwd_iter_next( it, pool );
    fd_pack_ord_txn_t * cur = treap_fwd_iter_ele( it, pool );
    treap_ele_remove( pack->deferred_bundles, cur, pool );
    cur->root = FD_ORD_TXN_ROOT_PENDING_BUNDLE;
    cur->skip = FD_PACK_BUNDLE_SKIP_CNT;
    treap_ele_insert( pack->pending_bundles, cur, pool );
  }
}

/* Expiration is controlled by callers, so a pending transaction can
   outlive the compressed slot counter.  Retire old slot markers before
   reusing them, preserving partially spent capacity-miss budgets. */
static void
fd_pack_retire_skip_markers( fd_pack_t * pack ) {
  for( ulong i=0UL; i<expq_cnt( pack->expiration_q ); i++ ) {
    fd_pack_ord_txn_t * cur = pack->expiration_q[ i ].txn;
    if( cur->skip>FD_PACK_SKIP_CNT ) cur->skip = FD_PACK_SKIP_CNT;
  }
}

static inline void
fd_pack_bam_copy_out( fd_txn_e_t       * out,
                      fd_txn_e_t const * txne ) {
  out->bam = txne->bam;
  if( !out->bam.revert_on_error ) out->txnp->flags &= ~FD_TXN_P_FLAGS_BUNDLE;
}

/* Returns the first whole bundle eligible for the selected mode.
   Deferred bundles live in deferred_bundles, so this never walks them. */
static inline __attribute__((always_inline)) treap_rev_iter_t
fd_pack_bundle_candidate( fd_pack_t const * pack,
                          _Bool             bam_only ) {
  fd_pack_ord_txn_t * pool = pack->pool;
  treap_rev_iter_t   iter = treap_rev_iter_init( pack->pending_bundles, pool );

  while( !treap_rev_iter_done( iter ) ) {
    fd_pack_ord_txn_t * cur = treap_rev_iter_ele( iter, pool );
    _Bool is_ib      = !!(cur->txn->flags & FD_TXN_P_FLAGS_INITIALIZER_BUNDLE);
    _Bool mode_match = is_ib
                     ? pack->initializer_bundle_bam==bam_only
                     : !bam_only || cur->txn->source_tpu==FD_TXN_M_TPU_SOURCE_BAM;
    if( FD_LIKELY( mode_match ) ) return iter;

    /* Source filtering operates on whole bundles.  Never return an
       iterator into the middle of a bundle. */
    ulong bundle_idx = RC_TO_REL_BUNDLE_IDX( cur->rewards, cur->compute_est );
    do {
      iter = treap_rev_iter_next( iter, pool );
      if( FD_UNLIKELY( treap_rev_iter_done( iter ) ) ) return iter;
      cur = treap_rev_iter_ele( iter, pool );
    } while( RC_TO_REL_BUNDLE_IDX( cur->rewards, cur->compute_est )==bundle_idx );
  }

  return iter;
}

/* Returns the hinted or first eligible bundle for
   fd_pack_try_schedule_bundle. */
static inline __attribute__((always_inline)) treap_rev_iter_t
fd_pack_bam_first_bundle( fd_pack_t * pack,
                          int         schedule_flags,
                          ulong       bundle_hint ) {
  if( FD_UNLIKELY( bundle_hint!=ULONG_MAX ) ) return (treap_rev_iter_t)(bundle_hint & USHORT_MAX);
  return fd_pack_bundle_candidate( pack, !!(schedule_flags & FD_PACK_SCHEDULE_BAM_ONLY) );
}

/* Returns whether the bundle led by txn0 is BAM work, or -1 if
   schedule_flags do not permit scheduling it. */
static inline __attribute__((always_inline)) int
fd_pack_bam_bundle_eligible( fd_pack_t const * pack,
                             int               schedule_flags,
                             treap_rev_iter_t  txn0,
                             int               is_ib,
                             ulong             bundle_idx ) {
  fd_pack_ord_txn_t const * lead = treap_rev_iter_ele_const( txn0, pack->pool );
  int is_bam = is_ib ? pack->initializer_bundle_bam : lead->txn->source_tpu==FD_TXN_M_TPU_SOURCE_BAM;
  if( FD_UNLIKELY( is_bam && !(schedule_flags & FD_PACK_SCHEDULE_BAM_READY) ) ) return -1;
  if( FD_LIKELY( schedule_flags & FD_PACK_SCHEDULE_BUNDLE ) ) return is_bam;
  if( FD_UNLIKELY( !is_bam || is_ib ) ) return -1; /* BAM_SINGLE */
  treap_rev_iter_t next = treap_rev_iter_next( txn0, pack->pool );
  if( FD_LIKELY( !treap_rev_iter_done( next ) ) ) {
    fd_pack_ord_txn_t const * successor = treap_rev_iter_ele_const( next, pack->pool );
    if( FD_UNLIKELY( RC_TO_REL_BUNDLE_IDX( successor->rewards, successor->compute_est )==bundle_idx ) ) return -1;
  }
  return 1;
}

/* Start of the scheduler.  Validate the hint before invalidating this
   call's hint lifetime.  In particular, never fall back to a different
   candidate carrying another head's target-slot readiness.  Normal
   transactions remain eligible on an invalid hint. */
static inline __attribute__((always_inline)) int
fd_pack_bam_take_bundle_hint( fd_pack_t * pack,
                              ulong     * bundle_hint,
                              int       * schedule_flags ) {
  int hint_valid = *bundle_hint!=ULONG_MAX &&
                   (*bundle_hint & ~((1UL<<33)-1UL))==pack->bundle_hint_generation &&
                   ((*bundle_hint>>32)&1UL)==(ulong)!!(*schedule_flags & FD_PACK_SCHEDULE_BAM_ONLY) &&
                   (*bundle_hint & USHORT_MAX)<trp_pool_max( pack->pool );
  if( FD_UNLIKELY( !hint_valid ) ) {
    *bundle_hint     = ULONG_MAX;
    *schedule_flags &= ~FD_PACK_SCHEDULE_BAM_READY;
  }
  fd_pack_invalidate_bundle_hint( pack );
  *schedule_flags &= ~fd_int_if( *schedule_flags & FD_PACK_SCHEDULE_BAM_ONLY, FD_PACK_SCHEDULE_TXN, 0 );
  return hint_valid;
}

fd_txn_e_t const *
fd_pack_peek_bundle_candidate( fd_pack_t const * pack,
                               _Bool             bam_only,
                               ulong *           bundle_hint,
                               void const **     opt_bundle_meta ) {
  *bundle_hint = ULONG_MAX;
  if( opt_bundle_meta ) *opt_bundle_meta = fd_pack_peek_bundle_meta( pack );
  treap_rev_iter_t _cur = fd_pack_bundle_candidate( pack, bam_only );
  if( FD_UNLIKELY( treap_rev_iter_done( _cur ) ) ) return NULL; /* empty */

  fd_pack_ord_txn_t * cur = treap_rev_iter_ele( _cur, pack->pool );
  /* Pool indices fit in a ushort.  Bit 32 binds the hint to the mode;
     upper bits bind its lifetime. */
  *bundle_hint = (ulong)_cur | ((ulong)bam_only<<32) | pack->bundle_hint_generation;
  return cur->txn_e;
}

/* Exact predecessor permissions are required here: the normal fast bitsets
   omit accounts when their finite mapping is exhausted.  This bounded
   scratch index contains every predecessor account and resolves hash
   collisions by comparing full keys.  Each chain has at most
   FD_PACK_BAM_LOOKAHEAD_TXN_MAX*64 entries. */
typedef struct {
  fd_acct_addr_t const * key;
  ushort                next;
  ushort                bucket;
  uchar                 writable;
} fd_pack_lookahead_acct_t;

/* Called after a conflict on a valid hint that leaves the pending
   bundles unchanged.  Return the first ready successor independent of
   all preceding groups.  The exact account index covers at most
   FD_PACK_BAM_LOOKAHEAD_TXN_MAX txns. */
FD_STATIC_ASSERT( FD_PACK_BAM_LOOKAHEAD_TXN_MAX*FD_TXN_ACCT_ADDR_MAX<USHORT_MAX, lookahead_acct_idx );
static ulong
fd_pack_bam_independent_candidate( fd_pack_t *          pack,
                                   int                  schedule_flags,
                                   ulong                head_hint,
                                   fd_pack_bam_ready_fn * ready,
                                   void const *         ready_ctx ) {
  if( FD_UNLIKELY( !ready || !(schedule_flags & FD_PACK_SCHEDULE_BAM_ONLY) || !(schedule_flags & FD_PACK_SCHEDULE_BAM_READY) ||
                   pack->initializer_bundle_state!=FD_PACK_IB_STATE_READY ) ) return ULONG_MAX;
  ulong generation = pack->bundle_hint_generation;
  treap_rev_iter_t iter = (treap_rev_iter_t)(head_hint & USHORT_MAX);

  fd_pack_lookahead_acct_t accts[ FD_PACK_BAM_LOOKAHEAD_TXN_MAX*FD_TXN_ACCT_ADDR_MAX ];
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
      if( FD_UNLIKELY( scanned==FD_PACK_BAM_LOOKAHEAD_TXN_MAX ||
                       txn_cnt==FD_PACK_MAX_TXN_PER_BUNDLE ||
                       cur->txn->source_tpu!=FD_TXN_M_TPU_SOURCE_BAM ||
                       cur->txn_e->bam.batch_idx!=txn_cnt ||
                       cur->txn_e->bam.seq_id!=txn0->bam.seq_id ||
                       cur->txn_e->bam.scheduler_gen!=txn0->bam.scheduler_gen ||
                       (cur->txn->flags & FD_TXN_P_FLAGS_INITIALIZER_BUNDLE) ) ) goto empty;
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
    if( !blocked ) {
      /* Retire any views obtained by the readiness callback before
         dispatch.  The head scan already counted deferred skips. */
      fd_pack_invalidate_bundle_hint( pack );
      return (ulong)begin | (1UL<<32) | generation;
    }
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
  FD_MCNT_INC( PACK, BAM_LOOKAHEAD_MISS, 1UL ); /* repeats until pending bundles or locks change */
  return ULONG_MAX;
}

ulong fd_pack_insert_bundle_reject_txn_idx( fd_pack_t const * pack ) { return pack->reject_txn_idx; }

void fd_pack_bam_enable( fd_pack_t * pack ) { pack->bam_enabled = 1; }
int  fd_pack_bam_enabled( fd_pack_t const * pack ) { return pack->bam_enabled; }

int
fd_pack_contains_initializer_bundle( fd_pack_t const *        pack,
                                     fd_ed25519_sig_t const * sig0,
                                     _Bool                    bam_only ) {
  treap_rev_iter_t iter = fd_pack_peek_bundle_head( pack );
  if( FD_UNLIKELY( treap_rev_iter_done( iter ) ) ) return 0;
  fd_txn_p_t const * txn = treap_rev_iter_ele_const( iter, pack->pool )->txn;
  return !!(txn->flags & FD_TXN_P_FLAGS_INITIALIZER_BUNDLE) &&
         pack->initializer_bundle_bam==bam_only &&
         !memcmp( fd_txn_get_signatures( TXN(txn), txn->payload ), sig0, sizeof(fd_ed25519_sig_t) );
}

/* The scheduler body is fd_pack.c's, instantiated twice: the unhinted
   copy folds the hint validation and lookahead away for non-BAM nodes. */
static inline __attribute__((always_inline)) ulong
fd_pack_schedule_next_microblock_impl_bundle_hint( fd_pack_t * pack, ulong total_cus, ulong bank_tile, int schedule_flags,
                                                   ulong bundle_hint, fd_pack_bam_ready_fn * ready, void const * ready_ctx, fd_txn_e_t * out );

ulong
fd_pack_schedule_next_microblock_with_bundle_hint( fd_pack_t *  pack,
                                                   ulong        total_cus,
                                                   ulong        bank_tile,
                                                   int          schedule_flags,
                                                   ulong        bundle_hint,
                                                   fd_pack_bam_ready_fn * ready,
                                                   void const * ready_ctx,
                                                   fd_txn_e_t * out ) {
  return fd_pack_schedule_next_microblock_impl_bundle_hint( pack, total_cus, bank_tile,
                                                            schedule_flags, bundle_hint, ready, ready_ctx, out );
}

ulong
fd_pack_schedule_next_microblock( fd_pack_t *  pack,
                                  ulong        total_cus,
                                  ulong        bank_tile,
                                  int          schedule_flags,
                                  fd_txn_e_t * out ) {
  return fd_pack_schedule_next_microblock_impl_bundle_hint( pack, total_cus, bank_tile,
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
    if( FD_LIKELY( (ord->root==FD_ORD_TXN_ROOT_PENDING_BUNDLE || ord->root==FD_ORD_TXN_ROOT_DEFERRED_BUNDLE) &&
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
  treap_t * treaps[ 2 ] = { pack->pending_bundles, pack->deferred_bundles };
  for( ulong t=0UL; t<2UL; t++ ) {
    treap_fwd_iter_t iter = treap_fwd_iter_init( treaps[ t ], pool );
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
  }
  /* Only Block Engine bundles are pre-rotation bundles. */
  for( ulong i=0UL; i<FD_PACK_PRE_ROTATION_BUNDLE_CAPACITY; i++ ) {
    treap_t * treap = pack->pre_rotation_bundles[ i ].treap;
    if( treap_ele_cnt( treap ) ) cnt += delete_transaction( pack, treap_fwd_iter_ele( treap_fwd_iter_init( treap, pool ), pool ), 1, 0 );
  }
  return cnt;
}
