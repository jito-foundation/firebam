/* FireBAM pack tests.  test_pack.c is included verbatim so these tests
   share its fixtures; its main is renamed and not run here. */

#define main              test_pack_upstream_main
#define make_transaction1 test_pack_upstream_make_transaction1
int test_pack_upstream_main( int argc, char ** argv );
#include "../pack/test_pack.c"
#undef make_transaction1
#undef main

#include "../pack/fd_pack_bitset.h"
#include "../fd_txn_m.h"

/* Pack reuses pool slots across tests.  Clear the source fields that
   upstream's make_transaction1 leaves untouched. */
static void
make_transaction1( fd_txn_p_t * txnp,
                   ulong        i,
                   uint         compute,
                   uint         loaded_data_sz,
                   double       priority,
                   char const * writes,
                   char const * reads,
                   ulong *      priority_fees,
                   ulong *      pack_cost_estimate ) {
  txnp->source_tpu  = FD_TXN_M_TPU_SOURCE_UDP;
  txnp->source_ipv4 = 0U;
  txnp->flags       = 0U;
  test_pack_upstream_make_transaction1( txnp, i, compute, loaded_data_sz, priority, writes, reads, priority_fees, pack_cost_estimate );
}

static fd_pack_t *
init_all_with_meta( ulong pack_depth,
                    ulong bank_tile_cnt,
                    ulong max_txn_per_microblock,
                    ulong bundle_meta_sz,
                    pack_outcome_t * outcome ) {
  fd_pack_limits_t limits[1] = { {
    .max_cost_per_block        = FD_PACK_TEST_MAX_COST_PER_BLOCK,
    .max_vote_cost_per_block   = FD_PACK_TEST_MAX_VOTE_COST_PER_BLOCK,
    .max_write_cost_per_acct   = FD_PACK_TEST_MAX_WRITE_COST_PER_ACCT,
    .max_data_bytes_per_block  = MAX_DATA_PER_BLOCK,
    .max_txn_per_microblock    = max_txn_per_microblock,
    .max_microblocks_per_block = MAX_TEST_TXNS,
    .max_allocated_data_per_block = FD_PACK_MAX_ALLOCATED_DATA_PER_BLOCK,
  } };
  ulong footprint = fd_pack_footprint( pack_depth, bundle_meta_sz, bank_tile_cnt, limits );
  if( footprint>PACK_SCRATCH_SZ ) FD_LOG_ERR(( "Test required %lu bytes, but scratch was only %lu", footprint, PACK_SCRATCH_SZ ));

  fd_pack_t * pack = fd_pack_join( fd_pack_new( pack_scratch, pack_depth, bundle_meta_sz, bank_tile_cnt, limits, NULL, 0UL, rng ) );

  outcome->microblock_cnt = 0UL;
  for( ulong i=0UL; i<FD_PACK_MAX_EXECLE_TILES; i++ ) {
    outcome->r_accts_in_use[ i ] = aset_null( );
    outcome->w_accts_in_use[ i ] = aset_null( );
  }

  return pack;
}

/* Bundle cleanup that follows duplicate signatures can delete the same pack
   object twice and corrupt its maps.  Retire each object exactly once. */
static void
test_duplicate_sig_bam_bundle_delete( void ) {
  FD_LOG_NOTICE(( "TEST DUPLICATE SIGNATURE BAM BUNDLE DELETE" ));
  fd_pack_t * pack = init_all( 128UL, 1UL, 128UL, &outcome );
  fd_pack_set_initializer_bundles_ready( pack );

  fd_txn_e_t * bundle_storage[ FD_PACK_MAX_TXN_PER_BUNDLE ];
  fd_txn_e_t * const * bundle =
      fd_pack_insert_bundle_init( pack, bundle_storage, FD_PACK_MAX_TXN_PER_BUNDLE );

  make_transaction1( bundle[ 0 ]->txnp, 450045UL, 1000U, 500U, 11.0,
                     "AB", "", NULL, NULL );
  bundle[ 0 ]->txnp->source_tpu          = FD_TXN_M_TPU_SOURCE_BAM;
  bundle[ 0 ]->bam = (fd_txn_bam_t){ .seq_id=450045U };

  for( ulong i=1UL; i<FD_PACK_MAX_TXN_PER_BUNDLE; i++ ) {
    *bundle[ i ]->txnp = *bundle[ 0 ]->txnp;
    bundle[ i ]->bam = (fd_txn_bam_t){ .seq_id=450045U, .batch_idx=(uchar)i };
  }

  fd_ed25519_sig_t sig;
  fd_memcpy( sig, txnp_get_signatures( bundle[ 0 ]->txnp ), sizeof(fd_ed25519_sig_t) );

  ulong deleted;
  int insert_result = fd_pack_insert_bundle_fini( pack,
                                                  bundle,
                                                  FD_PACK_MAX_TXN_PER_BUNDLE,
                                                  1000UL,
                                                  0,
                                                  NULL,
                                                  &deleted );
  FD_TEST( insert_result==FD_PACK_INSERT_ACCEPT_NONVOTE_ADD );
  FD_TEST( deleted==0UL );
  FD_TEST( fd_pack_avail_txn_cnt( pack )==FD_PACK_MAX_TXN_PER_BUNDLE );
  FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );

  FD_TEST( fd_pack_delete_transaction( pack, fd_type_pun( &sig ) )==FD_PACK_MAX_TXN_PER_BUNDLE );
  FD_TEST( fd_pack_avail_txn_cnt( pack )==0UL );
  FD_TEST( fd_pack_delete_transaction( pack, fd_type_pun( &sig ) )==0UL );
  FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );
}

/* Test bundle account conflicts */
static void
test_bundle_account_conflicts( void ) {
  fd_pack_t * pack = init_all_with_meta( 64UL, 2UL, 8UL, 64UL, &outcome );
  fd_pack_set_initializer_bundles_ready( pack );

  fd_txn_e_t * _bundle[FD_PACK_MAX_TXN_PER_BUNDLE];
  ulong _deleted;

  /* Bundle 1: writes to accounts a, b, c */
  fd_txn_e_t * const * bundle = fd_pack_insert_bundle_init( pack, _bundle, 3UL );
  make_transaction1( bundle[0]->txnp, 0UL, 2000U, 32U, 10.0, "a", "", NULL, NULL );
  make_transaction1( bundle[1]->txnp, 1UL, 2000U, 32U, 10.0, "b", "", NULL, NULL );
  make_transaction1( bundle[2]->txnp, 2UL, 2000U, 32U, 10.0, "c", "", NULL, NULL );
  FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 3UL, 1000UL, 0, NULL, &_deleted )>=0 );

  /* Bundle 2: writes to different accounts d, e */
  bundle = fd_pack_insert_bundle_init( pack, _bundle, 2UL );
  make_transaction1( bundle[0]->txnp, 3UL, 2000U, 32U, 9.0, "d", "", NULL, NULL );
  make_transaction1( bundle[1]->txnp, 4UL, 2000U, 32U, 9.0, "e", "", NULL, NULL );
  FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 2UL, 1000UL, 0, NULL, &_deleted )>=0 );

  /* Bundle 3: conflicts with bundle 1 (writes to account a) */
  bundle = fd_pack_insert_bundle_init( pack, _bundle, 2UL );
  make_transaction1( bundle[0]->txnp, 5UL, 2000U, 32U, 8.0, "a", "", NULL, NULL );
  make_transaction1( bundle[1]->txnp, 6UL, 2000U, 32U, 8.0, "f", "", NULL, NULL );
  FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 2UL, 1000UL, 0, NULL, &_deleted )>=0 );

  /* Schedule first bundle */
  ulong txn_cnt = fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL, FD_PACK_SCHEDULE_BUNDLE, outcome.results );
  FD_TEST( txn_cnt==3UL );

  /* Try to schedule second bundle on a different bank tile - should succeed (no conflicts) */
  txn_cnt = fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 1UL, FD_PACK_SCHEDULE_BUNDLE, outcome.results );
  FD_TEST( txn_cnt==2UL );
  fd_pack_microblock_complete( pack, 1UL );

  /* Try to schedule third bundle while first is still outstanding - should fail due to conflict */
  txn_cnt = fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 1UL, FD_PACK_SCHEDULE_BUNDLE, outcome.results );
  FD_TEST( txn_cnt==0UL );

  /* Complete first bundle */
  fd_pack_microblock_complete( pack, 0UL );

  /* Now third bundle should schedule */
  txn_cnt = fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL, FD_PACK_SCHEDULE_BUNDLE, outcome.results );
  FD_TEST( txn_cnt==2UL );
  fd_pack_microblock_complete( pack, 0UL );

  fd_pack_delete( fd_pack_leave( pack ) );
}

/* These real-Pack unit fixtures declare all their BAM candidates current-slot
   ready.  The tile tests separately validate the target-slot sidecar.  Always
   obtain a fresh view, including Failed-state and initializer candidates. */
static ulong
schedule_slot_ready_bam( fd_pack_t *  pack,
                         ulong        total_cus,
                         float        vote_fraction,
                         ulong        bank_tile,
                         int          flags,
                         fd_txn_e_t * out ) {
  ulong hint;
  fd_txn_e_t const * candidate = fd_pack_peek_bundle_candidate( pack, !!(flags & FD_PACK_SCHEDULE_BAM_ONLY), &hint, NULL );
  if( candidate ) flags |= FD_PACK_SCHEDULE_BAM_READY;
  return fd_pack_schedule_next_microblock_with_bundle_hint( pack, total_cus, vote_fraction, bank_tile, flags, hint, NULL, NULL, out );
}

/* Ordinary bundles writing reserved accounts are rejected; both BAM
   execution modes demote them.  Nonreserved reads and writes must still
   conflict normally. */
static void
test_reserved_bundle_permissions( void ) {
  for( int alt=0; alt<2; alt++ ) for( int mode=0; mode<3; mode++ ) {
    fd_pack_t * pack = init_all( 64UL, 2UL, 4UL, &outcome );
    fd_pack_set_initializer_bundles_ready( pack );
    fd_txn_e_t * slots[2];
    ulong costs[4];
    for( ulong b=0UL; b<2UL; b++ ) {
      fd_txn_e_t * const * bundle = fd_pack_insert_bundle_init( pack, slots, 2UL );
      for( ulong j=0UL; j<2UL; j++ ) {
        fd_txn_e_t * txne = bundle[j];
        make_transaction1( txne->txnp, 2UL*b+j, 1000U, 500U, 11.0,
                           alt ? (b ? "" : "R") : (b ? "A" : "AR"), "", NULL, NULL );
        fd_txn_t * txn = TXN( txne->txnp );
        if( alt ) {
          txn->transaction_version         = FD_TXN_V0;
          txn->addr_table_adtl_cnt          = 1U;
          txn->addr_table_adtl_writable_cnt = 1U;
          fd_memset( txne->alt_accts[0].b, 0, sizeof(fd_acct_addr_t) );
        } else {
          fd_memset( txne->txnp->payload+txn->acct_addr_off+32UL, 0, 32UL );
        }
        if( mode ) {
          txne->txnp->source_tpu          = FD_TXN_M_TPU_SOURCE_BAM;
          txne->bam.seq_id          = (uint)b;
          txne->bam.scheduler_gen   = 1U;
          txne->bam.batch_idx       = (uchar)j;
          txne->bam.revert_on_error = (uchar)(mode==2);
        }
        uint flags;
        costs[2UL*b+j] = fd_pack_compute_cost( txn, txne->txnp->payload, &flags, NULL, NULL, NULL, NULL, NULL );
      }
      ulong deleted;
      int rc = fd_pack_insert_bundle_fini( pack, bundle, 2UL, 1000UL, 0, NULL, &deleted );
      FD_TEST( mode ? rc>=0 : rc==FD_PACK_INSERT_REJECT_WRITES_SYSVAR );
      FD_TEST( !deleted );
    }
    FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );
    if( !mode ) {
      FD_TEST( !fd_pack_avail_txn_cnt( pack ) );
      continue;
    }
    FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                     0UL, FD_PACK_SCHEDULE_BUNDLE, outcome.results )==2UL );
    FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                     1UL, FD_PACK_SCHEDULE_BUNDLE, outcome.results+2 )==2UL );
    fd_pack_rebate_sum_t _rebater[1];
    fd_pack_rebate_sum_t * rebater = fd_pack_rebate_sum_join( fd_pack_rebate_sum_new( _rebater, 2UL ) );
    union { fd_pack_rebate_t rebate[1]; uchar footprint[USHORT_MAX]; } report;
    for( ulong j=0UL; j<4UL; j++ ) {
      fd_txn_p_t * txnp = outcome.results[j].txnp;
      FD_TEST( !!(txnp->flags & FD_TXN_P_FLAGS_BUNDLE)==(mode!=1) );
      txnp->flags |= FD_TXN_P_FLAGS_EXECUTE_SUCCESS;
      txnp->execle_cu.rebated_cus         = 100U;
      txnp->execle_cu.actual_consumed_cus = (uint)(costs[j]-100UL);
      fd_acct_addr_t const * alt_rebate[1] = { outcome.results[j].alt_accts };
      FD_TEST( !fd_pack_rebate_sum_add_txn( rebater, txnp, alt_rebate, 1UL ) );
    }
    FD_TEST( fd_pack_rebate_sum_report( rebater, report.rebate ) );
    FD_TEST( report.rebate->writer_cnt==5UL ); /* four fee payers and R */
    FD_TEST( report.rebate->total_cost_rebate==400UL );
    fd_pack_rebate_cus( pack, report.rebate );
    FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );

    /* The ordinary writer in bundle zero still excludes readers. */
    make_transaction( 4UL, 1000U, 500U, 11.0, "", "R", NULL, NULL );
    FD_TEST( insert( 4UL, pack )>=0 );
    FD_TEST( fd_pack_microblock_complete( pack, 1UL ) );
    FD_TEST( fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                              1UL, FD_PACK_SCHEDULE_TXN, outcome.results )==0UL );
    FD_TEST( fd_pack_microblock_complete( pack, 0UL ) );
    FD_TEST( fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                              1UL, FD_PACK_SCHEDULE_TXN, outcome.results )==1UL );
    FD_TEST( fd_pack_microblock_complete( pack, 1UL ) );
    FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );
  }
}

/* Reserved writes are only admitted, and demoted, for BAM transactions. */
static fd_txn_e_t * const *
insert_reserved_bam_test( fd_pack_t * pack, ulong i, fd_txn_e_t ** slots ) {
  fd_txn_e_t * const * bundle = fd_pack_insert_bundle_init( pack, slots, 1UL );
  make_transaction1( bundle[0]->txnp, i, 1000U, 500U, 11.0, "", "", NULL, NULL );
  bundle[0]->txnp->source_tpu = FD_TXN_M_TPU_SOURCE_BAM;
  bundle[0]->bam = (fd_txn_bam_t){ .seq_id=(uint)i };
  return bundle;
}

static void
test_reserved_fee_payer_and_program_locks( void ) {
  fd_pack_t * pack = init_all( 64UL, 2UL, 1UL, &outcome );
  fd_pack_set_initializer_bundles_ready( pack );
  fd_txn_e_t * slots[1];
  fd_txn_e_t * const * bundle = insert_reserved_bam_test( pack, 0UL, slots );
  fd_txn_e_t * slot = bundle[0];
  /* Pack leaves fee-payer account validation to runtime.  A reserved
     requested-writable payer must never put the null key in a map. */
  fd_memset( slot->txnp->payload+TXN(slot->txnp)->acct_addr_off, 0, 32UL );
  ulong deleted;
  FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 1UL, 1000UL, 0, NULL, &deleted )>=0 );
  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                   0UL, FD_PACK_SCHEDULE_BUNDLE, outcome.results )==1UL );
  fd_txn_p_t * txnp = outcome.results[0].txnp;
  FD_TEST( fd_txn_is_writable( TXN(txnp), 0 ) );
  ulong cost = (ulong)txnp->pack_cu.non_execution_cus+txnp->pack_cu.requested_exec_plus_acct_data_cus;
  txnp->execle_cu.rebated_cus = (uint)cost; /* runtime rejects this payer */
  txnp->execle_cu.actual_consumed_cus = 0U;
  fd_pack_rebate_sum_t _rebater[1];
  fd_pack_rebate_sum_t * rebater = fd_pack_rebate_sum_join( fd_pack_rebate_sum_new( _rebater, 3UL ) );
  fd_acct_addr_t const * alt[1] = { NULL };
  union { fd_pack_rebate_t rebate[1]; uchar footprint[USHORT_MAX]; } report;
  FD_TEST( !fd_pack_rebate_sum_add_txn( rebater, txnp, alt, 1UL ) );
  FD_TEST( fd_pack_rebate_sum_report( rebater, report.rebate ) );
  FD_TEST( report.rebate->writer_cnt==0UL );
  fd_pack_rebate_cus( pack, report.rebate );
  FD_TEST( fd_pack_current_block_cost( pack )==0UL );
  FD_TEST( fd_pack_microblock_complete( pack, 0UL ) );
  FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );

  /* This repair intentionally preserves conservative requested write
     locks for nonreserved invoked programs.  This remains safe whether
     or not the upgradeable loader makes the runtime permission writable. */
  for( int loader=0; loader<2; loader++ ) {
    fd_pack_clear_all( pack );
    fd_pack_set_initializer_bundles_ready( pack );
    for( ulong i=0UL; i<2UL; i++ ) {
      bundle = insert_reserved_bam_test( pack, i, slots );
      slot = bundle[0];
      TXN(slot->txnp)->readonly_unsigned_cnt = 0U; /* requests CB and work program writable */
      if( loader ) {
        TXN(slot->txnp)->transaction_version = FD_TXN_V0;
        TXN(slot->txnp)->addr_table_adtl_cnt = 1U;
        FD_TEST( fd_base58_decode_32( "BPFLoaderUpgradeab1e11111111111111111111111", slot->alt_accts[0].b ) );
      }
      FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 1UL, 1000UL, 0, NULL, &deleted )>=0 );
    }
    FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                     0UL, FD_PACK_SCHEDULE_BUNDLE, outcome.results )==1UL );
    FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                     1UL, FD_PACK_SCHEDULE_BUNDLE, outcome.results )==0UL );
    FD_TEST( fd_pack_microblock_complete( pack, 0UL ) );
    FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                     1UL, FD_PACK_SCHEDULE_BUNDLE, outcome.results )==1UL );
    FD_TEST( fd_pack_microblock_complete( pack, 1UL ) );
    FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );
  }

  /* Only the bundle path admits reserved writes, even from BAM. */
  fd_pack_clear_all( pack );
  slot = fd_pack_insert_txn_init( pack );
  make_transaction1( slot->txnp, 2UL, 1000U, 500U, 11.0, "A", "", NULL, NULL );
  slot->txnp->source_tpu = FD_TXN_M_TPU_SOURCE_BAM;
  FD_TEST( fd_base58_decode_32( "SysvarC1ock11111111111111111111111111111111",
                                slot->txnp->payload+TXN(slot->txnp)->acct_addr_off+32UL ) );
  FD_TEST( fd_pack_insert_txn_fini( pack, slot, 1000UL, &deleted )==FD_PACK_INSERT_REJECT_WRITES_SYSVAR );
  FD_TEST( !fd_pack_avail_txn_cnt( pack ) );
}

/* BAM-spec regression: non-revert BAM singles use the bundle treap and keep
   FIFO ordering instead of local fee or seq_id priority. */
static void
test_bam_nonrevert_seq_conflict_order( void ) {
  struct {
    ulong  txn_id;
    double priority;
    uint   seq;
  } cases[2] = {
    { 300UL,  3.0, 20U },
    { 301UL, 12.0, 10U },
  };

  fd_pack_t * pack = init_all_with_meta( 64UL, 1UL, 8UL, 64UL, &outcome );
  fd_pack_set_initializer_bundles_ready( pack );
  for( ulong i=0UL; i<2UL; i++ ) {
    ulong _deleted;
    fd_txn_e_t * _bundle[ FD_PACK_MAX_TXN_PER_BUNDLE ];
    fd_txn_e_t * const * bundle = fd_pack_insert_bundle_init( pack, _bundle, 1UL );
    make_transaction1( bundle[0]->txnp, cases[i].txn_id, 2000U, 32U, cases[i].priority, "x", "", NULL, NULL );
    bundle[0]->txnp->source_tpu          = FD_TXN_M_TPU_SOURCE_BAM;
    bundle[0]->bam = (fd_txn_bam_t){ .seq_id=cases[i].seq };
    FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 1UL, 1000UL, 0, NULL, &_deleted )>=0 );
  }

  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL, FD_PACK_SCHEDULE_TXN, outcome.results )==0UL );

  for( ulong i=0UL; i<2UL; i++ ) {
    ulong txn_cnt = schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL, FD_PACK_SCHEDULE_BUNDLE, outcome.results );
    ulong txn_id = 0UL;
    fd_memcpy( &txn_id, outcome.results[0].txnp->payload + 1UL, sizeof(ulong) );
    FD_TEST( txn_cnt==1UL );
    FD_TEST( txn_id==cases[i].txn_id );
    FD_TEST( !( outcome.results[0].txnp->flags & FD_TXN_P_FLAGS_BUNDLE ) );
    FD_TEST( outcome.results[0].txnp->source_tpu==FD_TXN_M_TPU_SOURCE_BAM );
    FD_TEST( outcome.results[0].bam.seq_id==cases[i].seq );
    FD_TEST( outcome.results[0].bam.batch_idx==0U );
    FD_TEST( outcome.results[0].bam.revert_on_error==0U );
    fd_pack_microblock_complete( pack, 0UL );
  }

  FD_TEST( fd_pack_avail_txn_cnt( pack )==0UL );
  fd_pack_delete( fd_pack_leave( pack ) );
}

/* A non-revert BAM batch (always one transaction) enters through the
   bundle path, but leaving the atomic bundle flag on it would route it to
   bundle execution.  Clear the flag before insertion. */
static void
test_bam_nonrevert_clears_bundle_flag( void ) {
  ulong _deleted;

  fd_pack_t * pack = init_all_with_meta( 64UL, 1UL, 8UL, 64UL, &outcome );
  fd_pack_set_initializer_bundles_ready( pack );

  fd_txn_e_t * _bundle[ FD_PACK_MAX_TXN_PER_BUNDLE ];
  fd_txn_e_t * const * bundle = fd_pack_insert_bundle_init( pack, _bundle, 1UL );
  make_transaction1( bundle[0]->txnp, 400UL, 2000U, 32U, 10.0, "x", "", NULL, NULL );
  bundle[0]->txnp->source_tpu          = FD_TXN_M_TPU_SOURCE_BAM;
  bundle[0]->bam = (fd_txn_bam_t){ .seq_id=40U };
  bundle[0]->txnp->first_seen_nanos = 1000L;
  FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 1UL, 1000UL, 0, NULL, &_deleted )>=0 );

  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL, FD_PACK_SCHEDULE_TXN, outcome.results )==0UL );

  ulong txn_cnt = schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL, FD_PACK_SCHEDULE_BUNDLE, outcome.results );
  FD_TEST( txn_cnt==1UL );
  ulong txn_id = 0UL;
  fd_memcpy( &txn_id, outcome.results[0].txnp->payload + 1UL, sizeof(ulong) );
  FD_TEST( txn_id==400UL );
  FD_TEST( !( outcome.results[0].txnp->flags & FD_TXN_P_FLAGS_BUNDLE ) );
  FD_TEST( outcome.results[0].txnp->source_tpu==FD_TXN_M_TPU_SOURCE_BAM );
  FD_TEST( outcome.results[0].bam.seq_id==40U );
  FD_TEST( outcome.results[0].bam.batch_idx==0U );
  FD_TEST( outcome.results[0].bam.revert_on_error==0U );
  FD_TEST( outcome.results[0].txnp->first_seen_nanos==1000L );
  fd_pack_microblock_complete( pack, 0UL );

  FD_TEST( fd_pack_avail_txn_cnt( pack )==0UL );
  fd_pack_delete( fd_pack_leave( pack ) );
}

/* If BAM override still admits ordinary or Block Engine queues, competing work
   can displace the scheduler's selected order.  Admit BAM work only. */
static void
test_bam_only_schedule_filters_non_bam_work( void ) {
  ulong _deleted;

  fd_pack_t * pack = init_all_with_meta( 64UL, 1UL, 8UL, 64UL, &outcome );

  fd_txn_e_t * txn = fd_pack_insert_txn_init( pack );
  make_transaction1( txn->txnp, 500UL, 2000U, 32U, 20.0, "a", "", NULL, NULL );
  txn->txnp->source_tpu = FD_TXN_M_TPU_SOURCE_UDP;
  FD_TEST( fd_pack_insert_txn_fini( pack, txn, 1000UL, &_deleted )>=0 );

  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL, FD_PACK_SCHEDULE_TXN | FD_PACK_SCHEDULE_BAM_ONLY, outcome.results )==0UL );

  txn = fd_pack_insert_txn_init( pack );
  make_transaction1( txn->txnp, 501UL, 2000U, 32U, 1.0, "b", "", NULL, NULL );
  txn->txnp->source_tpu = FD_TXN_M_TPU_SOURCE_BAM;
  FD_TEST( fd_pack_insert_txn_fini( pack, txn, 1000UL, &_deleted )>=0 );

  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL, FD_PACK_SCHEDULE_TXN | FD_PACK_SCHEDULE_BAM_ONLY, outcome.results )==0UL );

  fd_pack_clear_all( pack );

  txn = fd_pack_insert_txn_init( pack );
  make_vote_transaction1( txn->txnp, 520UL );
  txn->txnp->source_tpu = FD_TXN_M_TPU_SOURCE_UDP;
  FD_TEST( fd_pack_insert_txn_fini( pack, txn, 1000UL, &_deleted )>=0 );

  txn = fd_pack_insert_txn_init( pack );
  make_transaction1( txn->txnp, 521UL, 2000U, 32U, 20.0, "a", "", NULL, NULL );
  txn->txnp->source_tpu = FD_TXN_M_TPU_SOURCE_UDP;
  FD_TEST( fd_pack_insert_txn_fini( pack, txn, 1000UL, &_deleted )>=0 );

  ulong txn_cnt = schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 1.0f, 0UL, FD_PACK_SCHEDULE_VOTE | FD_PACK_SCHEDULE_TXN | FD_PACK_SCHEDULE_BAM_ONLY, outcome.results );
  FD_TEST( txn_cnt==1UL );
  FD_TEST( outcome.results[0].txnp->flags==FD_TXN_P_FLAGS_IS_SIMPLE_VOTE );
  FD_TEST( fd_pack_avail_txn_cnt( pack )==1UL );
  fd_pack_microblock_complete( pack, 0UL );
  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 1.0f, 0UL, FD_PACK_SCHEDULE_TXN | FD_PACK_SCHEDULE_BAM_ONLY, outcome.results )==0UL );
  FD_TEST( fd_pack_avail_txn_cnt( pack )==1UL );

  fd_pack_clear_all( pack );
  fd_pack_set_initializer_bundles_ready( pack );

  txn = fd_pack_insert_txn_init( pack );
  make_vote_transaction1( txn->txnp, 530UL );
  txn->txnp->source_tpu = FD_TXN_M_TPU_SOURCE_UDP;
  FD_TEST( fd_pack_insert_txn_fini( pack, txn, 1000UL, &_deleted )>=0 );

  ulong be_meta[ 8 ] = { 11UL };
  ulong bam_meta[ 8 ] = { 22UL };
  fd_txn_e_t * _bundle[ FD_PACK_MAX_TXN_PER_BUNDLE ];
  fd_txn_e_t * const * bundle = fd_pack_insert_bundle_init( pack, _bundle, 1UL );
  make_transaction1( bundle[0]->txnp, 510UL, 2000U, 32U, 20.0, "b", "", NULL, NULL );
  bundle[0]->txnp->source_tpu = FD_TXN_M_TPU_SOURCE_BUNDLE;
  FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 1UL, 1000UL, 0, be_meta, &_deleted )>=0 );

  bundle = fd_pack_insert_bundle_init( pack, _bundle, 1UL );
  make_transaction1( bundle[0]->txnp, 511UL, 2000U, 32U, 1.0, "c", "", NULL, NULL );
  bundle[0]->txnp->source_tpu = FD_TXN_M_TPU_SOURCE_BAM;
  bundle[0]->bam = (fd_txn_bam_t){0};
  FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 1UL, 1000UL, 0, bam_meta, &_deleted )>=0 );

  ulong bundle_hint;
  void const * selected_meta;
  fd_txn_e_t const * selected = fd_pack_peek_bundle_candidate( pack, 1, &bundle_hint, &selected_meta );
  FD_TEST( selected && selected->txnp->source_tpu==FD_TXN_M_TPU_SOURCE_BAM );
  FD_TEST( selected_meta && *(ulong const *)selected_meta==11UL ); /* crank metadata is the head's */

  txn_cnt = schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 1.0f, 0UL, FD_PACK_SCHEDULE_VOTE | FD_PACK_SCHEDULE_BUNDLE | FD_PACK_SCHEDULE_BAM_ONLY, outcome.results );
  FD_TEST( txn_cnt==1UL );
  FD_TEST( outcome.results[0].txnp->flags==FD_TXN_P_FLAGS_IS_SIMPLE_VOTE );
  fd_pack_microblock_complete( pack, 0UL );

  /* Peek again after the vote schedule and verify the hinted path selects
     the same BAM bundle without another traversal. */
  selected = fd_pack_peek_bundle_candidate( pack, 1, &bundle_hint, NULL );
  FD_TEST( selected && selected->txnp->source_tpu==FD_TXN_M_TPU_SOURCE_BAM );
  FD_TEST( fd_pack_schedule_next_microblock_with_bundle_hint( pack,
                                                               FD_PACK_TEST_MAX_COST_PER_BLOCK,
                                                               0.0f,
                                                               0UL,
                                                               FD_PACK_SCHEDULE_BUNDLE | FD_PACK_SCHEDULE_BAM_ONLY | FD_PACK_SCHEDULE_BAM_READY,
                                                               bundle_hint, NULL, NULL,
                                                               outcome.results )==1UL );
  FD_TEST( outcome.results[0].txnp->source_tpu==FD_TXN_M_TPU_SOURCE_BAM );
  fd_pack_microblock_complete( pack, 0UL );

  selected = fd_pack_peek_bundle_candidate( pack, 0, &bundle_hint, &selected_meta );
  FD_TEST( selected && selected->txnp->source_tpu==FD_TXN_M_TPU_SOURCE_BUNDLE );
  FD_TEST( selected_meta && *(ulong const *)selected_meta==11UL );
  FD_TEST( fd_pack_schedule_next_microblock_with_bundle_hint( pack,
                                                               FD_PACK_TEST_MAX_COST_PER_BLOCK,
                                                               0.0f,
                                                               0UL,
                                                               FD_PACK_SCHEDULE_BUNDLE | FD_PACK_SCHEDULE_BAM_ONLY | FD_PACK_SCHEDULE_BAM_READY,
                                                               bundle_hint, NULL, NULL,
                                                               outcome.results )==0UL );

  selected = fd_pack_peek_bundle_candidate( pack, 0, &bundle_hint, &selected_meta );
  FD_TEST( selected && selected->txnp->source_tpu==FD_TXN_M_TPU_SOURCE_BUNDLE );
  FD_TEST( selected_meta && *(ulong const *)selected_meta==11UL );

  fd_pack_clear_all( pack );
  fd_pack_delete( fd_pack_leave( pack ) );
}

static void
insert_mode_test_bundle( fd_pack_t *    pack,
                         fd_txn_e_t *   slots[ static FD_PACK_MAX_TXN_PER_BUNDLE ],
                         ulong          txn_id,
                         uchar          source_tpu,
                         int            initializer_bundle_kind,
                         void const *   meta ) {
  fd_txn_e_t * const * bundle = fd_pack_insert_bundle_init( pack, slots, 1UL );
  uint compute = fd_uint_if( initializer_bundle_kind==FD_PACK_IB_TYPE_NONE, 2000U, 10000U );
  make_transaction1( bundle[ 0 ]->txnp, txn_id, compute, 32U, 10.0, "a", "", NULL, NULL );
  bundle[ 0 ]->txnp->source_tpu = source_tpu;
  bundle[ 0 ]->bam = (fd_txn_bam_t){0};
  ulong deleted;
  FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 1UL, 1000UL, initializer_bundle_kind,
                                       meta, &deleted )>=0 );
}

/* Selecting a queued Block Engine initializer in BAM mode applies the wrong
   builder, recipient, and commission.  Initializers must remain source-bound. */
static void
test_initializer_bundle_mode_selection( void ) {
  typedef struct {
    ulong bundle_id;
  } test_meta_t;

  fd_pack_t * pack = init_all_with_meta( 64UL, 1UL, 8UL, sizeof(test_meta_t), &outcome );
  fd_txn_e_t * slots[ FD_PACK_MAX_TXN_PER_BUNDLE ];

  test_meta_t const be_meta  = { .bundle_id=31UL };
  test_meta_t const bam_meta = { .bundle_id=32UL };

  insert_mode_test_bundle( pack, slots, 610UL, FD_TXN_M_TPU_SOURCE_BUNDLE,
                           FD_PACK_IB_TYPE_NONE, &be_meta );

  /* This models the stale Block Engine-derived initializer inserted just
     before Pack observes BAM activation. */
  insert_mode_test_bundle( pack, slots, 611UL, FD_TXN_M_TPU_SOURCE_BUNDLE,
                           FD_PACK_IB_TYPE_NORMAL, NULL );

  /* BAM work arrives after the stale initializer is already pending. */
  insert_mode_test_bundle( pack, slots, 612UL, FD_TXN_M_TPU_SOURCE_BAM,
                           FD_PACK_IB_TYPE_NONE, &bam_meta );

  ulong bundle_hint;
  void const * meta;
  fd_txn_e_t const * candidate = fd_pack_peek_bundle_candidate( pack, 1, &bundle_hint, &meta );
  FD_TEST( candidate && candidate->txnp->source_tpu==FD_TXN_M_TPU_SOURCE_BAM );
  FD_TEST( !meta ); /* the queued initializer suppresses crank metadata */

  /* A BAM initializer replaces the stale normal initializer and is the
     only initializer admissible while BAM-only scheduling is active. */
  insert_mode_test_bundle( pack, slots, 613UL, FD_TXN_M_TPU_SOURCE_BUNDLE,
                           FD_PACK_IB_TYPE_BAM, NULL );
  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL,
                                             FD_PACK_SCHEDULE_BUNDLE | FD_PACK_SCHEDULE_BAM_ONLY,
                                             outcome.results )==1UL );
  FD_TEST( outcome.results[ 0 ].txnp->flags & FD_TXN_P_FLAGS_INITIALIZER_BUNDLE );
  FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );

  fd_pack_rebate_t rebate[ 1 ] = {{ .ib_result=1 }};
  fd_pack_rebate_cus( pack, rebate );

  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL,
                                             FD_PACK_SCHEDULE_BUNDLE | FD_PACK_SCHEDULE_BAM_ONLY,
                                             outcome.results )==1UL );
  FD_TEST( outcome.results[ 0 ].txnp->source_tpu==FD_TXN_M_TPU_SOURCE_BAM );
  FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );
  FD_TEST( fd_pack_avail_txn_cnt( pack )==1UL ); /* Block Engine retained */

  /* The reverse transition has the same isolation: normal scheduling
     cannot admit a pending BAM initializer. */
  fd_pack_end_block( pack );
  fd_pack_clear_all( pack );

  insert_mode_test_bundle( pack, slots, 615UL, FD_TXN_M_TPU_SOURCE_BUNDLE,
                           FD_PACK_IB_TYPE_BAM, NULL );

  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL,
                                             FD_PACK_SCHEDULE_BUNDLE, outcome.results )==0UL );

  insert_mode_test_bundle( pack, slots, 616UL, FD_TXN_M_TPU_SOURCE_BUNDLE,
                           FD_PACK_IB_TYPE_NORMAL, NULL );
  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL,
                                             FD_PACK_SCHEDULE_BUNDLE, outcome.results )==1UL );
  FD_TEST( outcome.results[ 0 ].txnp->flags & FD_TXN_P_FLAGS_INITIALIZER_BUNDLE );
  FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );

  fd_pack_delete( fd_pack_leave( pack ) );
}

/* As in jito-solana, BAM initializer maintenance is strict: BAM work waits
   for a BAM initializer to commit, and a failed one blocks BAM work for the
   rest of the leader slot, exactly like the normal Block Engine path. */
static void
test_bam_initializer_strict( void ) {
  fd_pack_t * pack = init_all_with_meta( 64UL, 1UL, 8UL, 64UL, &outcome );
  fd_txn_e_t * slots[ FD_PACK_MAX_TXN_PER_BUNDLE ];
  int const bam_full = FD_PACK_SCHEDULE_BUNDLE | FD_PACK_SCHEDULE_BAM_ONLY;

  /* BAM work cannot proceed without an initializer in [Not Initialized]. */
  insert_mode_test_bundle( pack, slots, 620UL, FD_TXN_M_TPU_SOURCE_BAM,
                           FD_PACK_IB_TYPE_NONE, NULL );
  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL,
                                    bam_full, outcome.results )==0UL );

  /* Normal bundles still require a normal initializer. */
  insert_mode_test_bundle( pack, slots, 621UL, FD_TXN_M_TPU_SOURCE_BUNDLE,
                           FD_PACK_IB_TYPE_NONE, NULL );
  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL,
                                    FD_PACK_SCHEDULE_BUNDLE, outcome.results )==0UL );

  insert_mode_test_bundle( pack, slots, 623UL, FD_TXN_M_TPU_SOURCE_BUNDLE,
                           FD_PACK_IB_TYPE_BAM, NULL );

  /* A queued BAM initializer goes ahead of BAM work. */
  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL,
                                    bam_full, outcome.results )==1UL );
  FD_TEST( outcome.results[ 0 ].txnp->flags & FD_TXN_P_FLAGS_INITIALIZER_BUNDLE );
  FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );

  /* [Pending] blocks until execution reports a result. */
  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL,
                                    bam_full, outcome.results )==0UL );

  fd_pack_rebate_t rebate[ 1 ] = {{ .ib_result=-1 }};
  fd_pack_rebate_cus( pack, rebate );

  /* [Failed] blocks BAM and normal bundles for the rest of the slot. */
  for( ulong i=0UL; i<8UL; i++ ) {
    FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL,
                                      bam_full, outcome.results )==0UL );
    FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL,
                                      FD_PACK_SCHEDULE_BUNDLE, outcome.results )==0UL );
  }

  /* The next slot needs a new initializer; a successful one unblocks. */
  fd_pack_end_block( pack );
  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL,
                                    bam_full, outcome.results )==0UL );
  insert_mode_test_bundle( pack, slots, 624UL, FD_TXN_M_TPU_SOURCE_BUNDLE,
                           FD_PACK_IB_TYPE_BAM, NULL );
  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL,
                                    bam_full, outcome.results )==1UL );
  FD_TEST( outcome.results[ 0 ].txnp->flags & FD_TXN_P_FLAGS_INITIALIZER_BUNDLE );
  FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );
  rebate[ 0 ].ib_result = 1;
  fd_pack_rebate_cus( pack, rebate );
  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL,
                                    bam_full, outcome.results )==1UL );
  FD_TEST( outcome.results[ 0 ].txnp->source_tpu==FD_TXN_M_TPU_SOURCE_BAM );
  FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );

  fd_pack_delete( fd_pack_leave( pack ) );
}

static void
insert_bam_candidate_test( fd_pack_t * pack,
                           ulong       txn_id,
                           ulong       txn_cnt,
                           uint        compute,
                           uchar       revert_on_error,
                           char const * writes,
                           char const * reads,
                           int         alt_mode ) {
  fd_txn_e_t * slots[ FD_PACK_MAX_TXN_PER_BUNDLE ];
  fd_txn_e_t * const * bundle = fd_pack_insert_bundle_init( pack, slots, txn_cnt );
  for( ulong i=0UL; i<txn_cnt; i++ ) {
    make_transaction1( bundle[i]->txnp, txn_id+i, compute, 32U,
                       txn_id==700UL ? 1.0 : 12.0, writes, reads, NULL, NULL );
    bundle[i]->txnp->source_tpu          = FD_TXN_M_TPU_SOURCE_BAM;
    /* Deliberately cross numeric wrap in FIFO arrival order. */
    bundle[i]->bam.seq_id          = txn_id==700UL ? UINT_MAX : 0U;
    bundle[i]->bam.scheduler_gen   = 9U;
    bundle[i]->bam.batch_idx       = (uchar)i;
    bundle[i]->bam.revert_on_error = revert_on_error;
    if( alt_mode ) {
      /* The resolved account is appended after static accounts, exactly
         where Pack's address iterator obtains resolved ALT locks. */
      fd_txn_t * txn = TXN( bundle[i]->txnp );
      txn->transaction_version          = FD_TXN_V0;
      txn->addr_table_adtl_cnt           = 1U;
      txn->addr_table_adtl_writable_cnt  = (uchar)(alt_mode==2);
      fd_memset( bundle[i]->alt_accts[0].b, 'X', sizeof(fd_acct_addr_t) );
    }
  }
  ulong deleted;
  FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, txn_cnt, 1000UL, FD_PACK_IB_TYPE_NONE,
                                       NULL, &deleted )>=0 );
  FD_TEST( !deleted );
}

static ulong
candidate_test_txn_id( fd_txn_p_t const * txnp ) {
  return fd_ulong_load_8( fd_txn_get_signatures( TXN(txnp), txnp->payload ) );
}

/* Parallel BAM singles retain the existing account lock ownership and
   atomicity flags.  Cover both static and resolved ALT conflicts. */
static void
test_bam_secondary_account_locks( void ) {
  for( uchar atomic=0U; atomic<2U; atomic++ ) {
    for( int alt=0; alt<2; alt++ ) {
      for( int kind=0; kind<5; kind++ ) {
        fd_pack_t * pack = init_all_with_meta( 64UL, 2UL, 8UL, 48UL, &outcome );
        fd_pack_set_initializer_bundles_ready( pack );
        int writer0 = kind==1 || kind==2;
        int writer1 = kind==1 || kind==3;
        int conflict = kind>=1 && kind<=3;
        for( ulong i=0UL; i<2UL; i++ ) {
          int writer = i ? writer1 : writer0;
          insert_bam_candidate_test( pack, 700UL+i, 1UL, 2000U, atomic,
                                     (!alt && writer) ? "X" : "",
                                     (!alt && !writer && kind) ? "X" : "",
                                     alt && kind ? (writer ? 2 : 1) : 0 );
        }
        int full = FD_PACK_SCHEDULE_BUNDLE | FD_PACK_SCHEDULE_BAM_ONLY;
        int single = FD_PACK_SCHEDULE_BAM_SINGLE | FD_PACK_SCHEDULE_BAM_ONLY;
        FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL, full, outcome.results )==1UL );
        FD_TEST( candidate_test_txn_id( outcome.results[0].txnp )==700UL );
        FD_TEST( !!(outcome.results[0].txnp->flags & FD_TXN_P_FLAGS_BUNDLE)==atomic );
        FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 1UL, single, outcome.results )==(ulong)!conflict );
        if( conflict ) {
          FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );
          FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 1UL, single, outcome.results )==1UL );
        }
        FD_TEST( candidate_test_txn_id( outcome.results[0].txnp )==701UL );
        FD_TEST( !!(outcome.results[0].txnp->flags & FD_TXN_P_FLAGS_BUNDLE)==atomic );
        if( alt && kind ) FD_TEST( outcome.results[0].alt_accts[0].b[0]=='X' );
        /* Independent/read-sharing work completes in reverse dispatch order. */
        FD_TEST( fd_pack_microblock_complete( pack, 1UL )==1 );
        if( !conflict ) FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );
        FD_TEST( !fd_pack_avail_txn_cnt( pack ) );
        FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );
        fd_pack_delete( fd_pack_leave( pack ) );
      }
    }
  }
}

static void
test_bam_secondary_fee_payer_and_nonce_locks( void ) {
  for( int nonce=0; nonce<2; nonce++ ) {
    fd_pack_t * pack = init_all_with_meta( 64UL, 2UL, 8UL, 48UL, &outcome );
    fd_pack_set_initializer_bundles_ready( pack );
    fd_txn_e_t * slots[1];
    fd_txn_e_t * const * bundle = fd_pack_insert_bundle_init( pack, slots, 1UL );
    if( nonce ) make_nonce_transaction1( bundle[0]->txnp, 700UL, 5.0, 5U, 0U, 'b' );
    else        make_transaction1( bundle[0]->txnp, 700UL, 2000U, 32U, 5.0, "", "", NULL, NULL );
    bundle[0]->txnp->source_tpu          = FD_TXN_M_TPU_SOURCE_BAM;
    bundle[0]->bam = (fd_txn_bam_t){ .seq_id=1U, .revert_on_error=1 };
    fd_acct_addr_t payer = fd_txn_get_acct_addrs( TXN(bundle[0]->txnp), bundle[0]->txnp->payload )[0];
    ulong deleted;
    FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 1UL, 1000UL, FD_PACK_IB_TYPE_NONE,
                                         NULL, &deleted )>=0 );
    bundle = fd_pack_insert_bundle_init( pack, slots, 1UL );
    /* Account 5 of the nonce fixture is 'h'; the ordinary successor must
       wait for the nonce writer just as it waits for a common fee payer. */
    make_transaction1( bundle[0]->txnp, 701UL, 2000U, 32U, 10.0, nonce ? "h" : "", "", NULL, NULL );
    if( !nonce ) fd_memcpy( bundle[0]->txnp->payload+TXN(bundle[0]->txnp)->acct_addr_off, &payer, sizeof(payer) );
    bundle[0]->txnp->source_tpu    = FD_TXN_M_TPU_SOURCE_BAM;
    bundle[0]->bam = (fd_txn_bam_t){ .seq_id=2U };
    FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 1UL, 1000UL, FD_PACK_IB_TYPE_NONE,
                                         NULL, &deleted )>=0 );
    FD_TEST( !deleted );
    FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL,
                                     FD_PACK_SCHEDULE_BUNDLE | FD_PACK_SCHEDULE_BAM_ONLY, outcome.results )==1UL );
    FD_TEST( !(outcome.results[0].txnp->flags & FD_TXN_P_FLAGS_DURABLE_NONCE) ); /* the bank checks BAM nonces */
    FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 1UL,
                                     FD_PACK_SCHEDULE_BAM_SINGLE | FD_PACK_SCHEDULE_BAM_ONLY, outcome.results )==0UL );
    FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );
    FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 1UL,
                                     FD_PACK_SCHEDULE_BAM_SINGLE | FD_PACK_SCHEDULE_BAM_ONLY, outcome.results )==1UL );
    FD_TEST( candidate_test_txn_id( outcome.results[0].txnp )==701UL );
    FD_TEST( fd_pack_microblock_complete( pack, 1UL )==1 );
    FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );
    fd_pack_delete( fd_pack_leave( pack ) );
  }
}

/* Ineligibility is not capacity deferral: repeated secondary attempts cannot
   select a higher-reward successor while an older multi waits for worker 0. */
static void
test_bam_secondary_fifo_head( void ) {
  fd_pack_t * pack = init_all_with_meta( 64UL, 2UL, 8UL, 48UL, &outcome );
  fd_pack_set_initializer_bundles_ready( pack );
  insert_bam_candidate_test( pack, 700UL, 2UL, 2000U, 1U, "X", "", 0 );
  insert_bam_candidate_test( pack, 702UL, 1UL, 2000U, 0U, "X", "", 0 );
  int full = FD_PACK_SCHEDULE_BUNDLE | FD_PACK_SCHEDULE_BAM_ONLY;
  int single = FD_PACK_SCHEDULE_BAM_SINGLE | FD_PACK_SCHEDULE_BAM_ONLY;
  for( ulong i=0UL; i<128UL; i++ ) {
    FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 1UL, single, outcome.results )==0UL );
    ulong hint;
    fd_txn_e_t const * candidate = fd_pack_peek_bundle_candidate( pack, 1, &hint, NULL );
    FD_TEST( candidate && candidate_test_txn_id( candidate->txnp )==700UL );
    FD_TEST( fd_pack_avail_txn_cnt( pack )==3UL );
    FD_TEST( !fd_pack_current_block_cost( pack ) );
  }
  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL, full, outcome.results )==2UL );
  FD_TEST( candidate_test_txn_id( outcome.results[0].txnp )==700UL );
  FD_TEST( candidate_test_txn_id( outcome.results[1].txnp )==701UL );
  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 1UL, single, outcome.results )==0UL );
  FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );
  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 1UL, single, outcome.results )==1UL );
  FD_TEST( candidate_test_txn_id( outcome.results[0].txnp )==702UL );
  FD_TEST( fd_pack_microblock_complete( pack, 1UL )==1 );
  FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );
  fd_pack_delete( fd_pack_leave( pack ) );
}

/* A worker never reports its last reservation.  Full-permission BAM capacity
   failures still defer the head and eventually allow a smaller successor. */
static void
test_bam_secondary_capacity_deferral( void ) {
  for( int restricted=0; restricted<2; restricted++ ) {
    fd_pack_t * pack = init_all_with_meta( 64UL, 2UL, 8UL, 48UL, &outcome );
    fd_pack_set_initializer_bundles_ready( pack );
    ulong filler_cost = 0UL;
    ulong deleted;
    for( ulong i=0UL;; i++ ) {
      fd_txn_e_t * txn = fd_pack_insert_txn_init( pack );
      make_transaction1( txn->txnp, i, 1000000U, 32U, 5.0, "", "", NULL, &filler_cost );
      FD_TEST( fd_pack_insert_txn_fini( pack, txn, 1000UL, &deleted )>=0 );
      FD_TEST( fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL,
                                                 FD_PACK_SCHEDULE_TXN, outcome.results )==1UL );
      if( FD_PACK_TEST_MAX_COST_PER_BLOCK-fd_pack_current_block_cost( pack )<filler_cost ) break;
      FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );
    }
    ulong cost_before = fd_pack_current_block_cost( pack );
    insert_bam_candidate_test( pack, 700UL, 1UL, 1400000U, 1U, "X", "", 0 );
    insert_bam_candidate_test( pack, 701UL, 1UL, 2000U, 0U, "X", "", 0 );
    int flags = FD_PACK_SCHEDULE_BAM_ONLY | (restricted ? FD_PACK_SCHEDULE_BAM_SINGLE : FD_PACK_SCHEDULE_BUNDLE);
    if( !restricted ) FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );
    int successor_dispatched = 0;
    for( ulong i=0UL; i<128UL; i++ ) {
      ulong count = schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                            restricted ? 1UL : 0UL, flags, outcome.results );
      if( count ) {
        FD_TEST( !restricted && count==1UL && !successor_dispatched );
        FD_TEST( candidate_test_txn_id( outcome.results[0].txnp )==701UL );
        successor_dispatched = 1;
        FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );
      }
    }
    FD_TEST( successor_dispatched==!restricted );
    if( restricted ) {
      FD_TEST( fd_pack_current_block_cost( pack )==cost_before );
      FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );
    }
    fd_pack_rebate_t rebate = { .total_cost_rebate=filler_cost };
    fd_pack_rebate_cus( pack, &rebate );
    ulong count = schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 1UL,
                                          FD_PACK_SCHEDULE_BAM_SINGLE | FD_PACK_SCHEDULE_BAM_ONLY, outcome.results );
    FD_TEST( count==(ulong)restricted );
    if( restricted ) {
      FD_TEST( candidate_test_txn_id( outcome.results[0].txnp )==700UL );
      FD_TEST( fd_pack_microblock_complete( pack, 1UL )==1 );
    } else {
      /* A rebate must not restore the deferred writer behind its successor. */
      FD_TEST( fd_pack_avail_txn_cnt( pack )==1UL );
      fd_pack_end_block( pack );
      fd_pack_set_initializer_bundles_ready( pack );
      FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 1UL,
                                       FD_PACK_SCHEDULE_BAM_SINGLE | FD_PACK_SCHEDULE_BAM_ONLY, outcome.results )==1UL );
      FD_TEST( candidate_test_txn_id( outcome.results[0].txnp )==700UL );
      FD_TEST( fd_pack_microblock_complete( pack, 1UL )==1 );
    }
    FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );
    fd_pack_delete( fd_pack_leave( pack ) );
  }
}

static void
fill_for_bam_capacity_test( fd_pack_t * pack, pack_outcome_t * outcome ) {
  for( ulong i=0UL;; i++ ) {
    ulong cost;
    ulong deleted;
    fd_txn_e_t * txn = fd_pack_insert_txn_init( pack );
    make_transaction1( txn->txnp, i, 1000000U, 32U, 5.0, "", "", NULL, &cost );
    FD_TEST( fd_pack_insert_txn_fini( pack, txn, 1000UL, &deleted )>=0 );
    FD_TEST( fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL,
                                               FD_PACK_SCHEDULE_TXN, outcome->results )==1UL );
    FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );
    if( FD_PACK_TEST_MAX_COST_PER_BLOCK-fd_pack_current_block_cost( pack )<cost ) return;
  }
}

static void
insert_lookahead_batch( fd_pack_t * pack,
                        ulong id, ulong cnt, char const * writes, char const * reads,
                        int alt, int reserved ) {
  fd_txn_e_t * slots[ FD_PACK_MAX_TXN_PER_BUNDLE ];
  fd_txn_e_t * const * bundle = fd_pack_insert_bundle_init( pack, slots, cnt );
  for( ulong j=0UL; j<cnt; j++ ) {
    make_transaction1( bundle[j]->txnp, id+j, 1000U, 32U, 5.0, writes, reads, NULL, NULL );
    bundle[j]->txnp->source_tpu        = FD_TXN_M_TPU_SOURCE_BAM;
    bundle[j]->bam = (fd_txn_bam_t){ .seq_id=(uint)id, .scheduler_gen=1U, .batch_idx=(uchar)j };
    if( alt ) {
      TXN(bundle[j]->txnp)->transaction_version         = FD_TXN_V0;
      TXN(bundle[j]->txnp)->addr_table_adtl_cnt          = 1U;
      TXN(bundle[j]->txnp)->addr_table_adtl_writable_cnt = 1U;
      fd_memset( bundle[j]->alt_accts[0].b, 'X', 32UL );
    }
    if( reserved ) fd_memset( bundle[j]->txnp->payload+TXN(bundle[j]->txnp)->acct_addr_off+32UL, 0, 32UL );
  }
  ulong deleted;
  FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, cnt, 1000UL, 0, &id, &deleted )>=0 );
  FD_TEST( !deleted );
}

typedef struct {
  ulong barrier_id;
  ulong multi_id;
  ulong multi_cnt;
  ulong * calls;
} test_lookahead_ready_t;

static int
test_lookahead_ready( void const * ctx, fd_txn_e_t const * candidate, ulong txn_cnt ) {
  test_lookahead_ready_t const * r = ctx;
  if( r->calls ) (*r->calls)++;
  ulong id = candidate_test_txn_id( candidate->txnp );
  return id!=r->barrier_id && txn_cnt==(id==r->multi_id ? r->multi_cnt : 1UL);
}

/* Use every bit in the finite conflict accelerator, so dependencies added
   afterward must be resolved from exact static/ALT addresses. */
static void
fill_lookahead_bitsets( fd_pack_t * pack ) {
  for( ulong k=0UL; k<FD_PACK_BITSET_MAX; k++ ) for( ulong j=0UL; j<2UL; j++ ) {
    fd_txn_e_t * slot = fd_pack_insert_txn_init( pack );
    make_transaction1( slot->txnp, 10000UL+2UL*k+j, 1000U, 32U, 5.0, "A", "", NULL, NULL );
    uchar * key = slot->txnp->payload+TXN(slot->txnp)->acct_addr_off+32UL;
    fd_memset( key, 0xcc, 32UL );
    fd_memcpy( key, &k, sizeof(k) );
    ulong deleted;
    FD_TEST( fd_pack_insert_txn_fini( pack, slot, 1000UL, &deleted )>=0 );
  }
}

static void
test_bam_conflict_lookahead( void ) {
  /* Independent work; transitive X->XY->Y; shared reads; ALT; exhausted
     bitsets; reserved System; future predecessor; secondary permission;
     prefix bound; invalidated hint; released conflict; bad group
     count; and a queued initializer barrier. */
  for( int kind=0; kind<13; kind++ ) {
    fd_pack_t * pack = init_all_with_meta( 2048UL, 2UL, 1UL, sizeof(ulong), &outcome );
    fd_pack_set_initializer_bundles_ready( pack );
    if( kind==4 ) fill_lookahead_bitsets( pack );
    fd_txn_e_t * live = fd_pack_insert_txn_init( pack );
    make_transaction1( live->txnp, 900UL, 1000U, 32U, 12.0, kind==2 ? "A" : "X", "", NULL, NULL );
    ulong deleted;
    FD_TEST( fd_pack_insert_txn_fini( pack, live, 1000UL, &deleted )>=0 );
    FD_TEST( fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                              0UL, FD_PACK_SCHEDULE_TXN, outcome.results )==1UL );
    ulong head_cnt = kind==8 ? 5UL : 1UL;
    if( kind==2 ) {
      insert_lookahead_batch( pack, 700UL, 1UL, "A", "X", 0, 0 );
      insert_lookahead_batch( pack, 701UL, 1UL, "Y", "X", 0, 0 );
    } else {
      insert_lookahead_batch( pack, 700UL, head_cnt, kind==3 ? "" : kind==5 ? "RX" : "X", "", kind==3, kind==5 );
      if( kind==1 || kind==4 ) {
        insert_lookahead_batch( pack, 701UL, 1UL, "XY", "", 0, 0 );
        insert_lookahead_batch( pack, 702UL, 1UL, "Y", "", 0, 0 );
        insert_lookahead_batch( pack, 703UL, 1UL, "Z", "", 0, 0 );
      } else {
        insert_lookahead_batch( pack, 705UL, kind==7 ? 2UL : 1UL, kind==5 ? "RY" : "Y", "", 0, kind==5 );
        if( kind==7 ) {
          insert_lookahead_batch( pack, 707UL, 1UL, "Y", "", 0, 0 ); /* Depends on worker-ineligible batch. */
          insert_lookahead_batch( pack, 708UL, 1UL, "Z", "", 0, 0 );
        }
      }
    }
    if( kind==12 ) {
      fd_txn_e_t * slots[1];
      fd_txn_e_t * const * ib = fd_pack_insert_bundle_init( pack, slots, 1UL );
      make_transaction1( ib[0]->txnp, 799UL, 1000U, 32U, 5.0, "X", "", NULL, NULL );
      FD_TEST( fd_pack_insert_bundle_fini( pack, ib, 1UL, 1000UL, FD_PACK_IB_TYPE_BAM, NULL, &deleted )>=0 );
    }
    int flags = FD_PACK_SCHEDULE_BAM_ONLY | FD_PACK_SCHEDULE_BUNDLE;
    ulong calls = 0UL;
    test_lookahead_ready_t ready = { .barrier_id=kind==6 ? 705UL : ULONG_MAX,
                                     .multi_id=kind==7 ? 705UL : 700UL,
                                     .multi_cnt=kind==7 ? 2UL : head_cnt,
                                     .calls=&calls };
    /* The unhinted API and a hinted call without a readiness callback
       still cannot bypass the blocked head. */
    FD_TEST( !fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                               1UL, flags | FD_PACK_SCHEDULE_BAM_READY, outcome.results ) );
    FD_TEST( !schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                      1UL, flags, outcome.results ) );
    ulong hint;
    FD_TEST( fd_pack_peek_bundle_candidate( pack, 1, &hint, NULL ) );
    if( kind==7 ) flags = FD_PACK_SCHEDULE_BAM_ONLY | FD_PACK_SCHEDULE_BAM_SINGLE;
    if( kind==9 ) { fd_pack_rebate_t rebate = {0}; fd_pack_rebate_cus( pack, &rebate ); }
    if( kind==10 ) {
      FD_TEST( fd_pack_microblock_complete( pack, 0UL ) );
      FD_TEST( fd_pack_peek_bundle_candidate( pack, 1, &hint, NULL ) );
    }
    if( kind==11 ) ready.multi_cnt = 2UL; /* incomplete sidecar identity */
    ulong scheduled = fd_pack_schedule_next_microblock_with_bundle_hint( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                          1UL, flags | FD_PACK_SCHEDULE_BAM_READY, hint, test_lookahead_ready, &ready, outcome.results );
    if( kind==6 || (kind>=8 && kind!=10) ) {
      FD_TEST( !scheduled );
      if( kind==9 ) FD_TEST( !calls );
    } else {
      ulong expected = (kind==1 || kind==4) ? 703UL : kind==7 ? 708UL : kind==2 ? 701UL : kind==10 ? 700UL : 705UL;
      FD_TEST( scheduled==1UL && candidate_test_txn_id( outcome.results[0].txnp )==expected );
      if( kind==10 ) FD_TEST( !calls ); /* Released head dispatches without a scan. */
      else FD_TEST( calls>=2UL );
      FD_TEST( fd_pack_microblock_complete( pack, 1UL ) );
    }
    FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );
    fd_pack_delete( fd_pack_leave( pack ) );
  }

  /* Capacity failure provides no lookahead authorization, even if later
     batches are independent and smaller. */
  fd_pack_t * pack = init_all_with_meta( 64UL, 2UL, 8UL, sizeof(ulong), &outcome );
  fd_pack_set_initializer_bundles_ready( pack );
  fill_for_bam_capacity_test( pack, &outcome );
  insert_bam_candidate_test( pack, 700UL, 1UL, 1400000U, 1U, "X", "", 0 );
  insert_lookahead_batch( pack, 705UL, 1UL, "Y", "", 0, 0 );
  int flags = FD_PACK_SCHEDULE_BAM_ONLY | FD_PACK_SCHEDULE_BUNDLE | FD_PACK_SCHEDULE_BAM_READY;
  ulong calls = 0UL;
  test_lookahead_ready_t ready = { .barrier_id=ULONG_MAX, .multi_id=ULONG_MAX, .multi_cnt=1UL, .calls=&calls };
  ulong hint;
  FD_TEST( fd_pack_peek_bundle_candidate( pack, 1, &hint, NULL ) );
  FD_TEST( !fd_pack_schedule_next_microblock_with_bundle_hint( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
              1UL, flags, hint, test_lookahead_ready, &ready, outcome.results ) );
  FD_TEST( !calls );
  fd_pack_delete( fd_pack_leave( pack ) );
}

static void
insert_lookahead_permission_bundle( fd_pack_t * pack,
                                    ulong       id,
                                    char const * writes,
                                    char const * reads,
                                    int         alt_mode,
                                    int         bam ) {
  fd_txn_e_t * slots[1];
  fd_txn_e_t * const * bundle = fd_pack_insert_bundle_init( pack, slots, 1UL );
  make_transaction1( bundle[0]->txnp, id, 1000U, 32U, 5.0, writes, reads, NULL, NULL );
  bundle[0]->txnp->source_tpu = bam ? FD_TXN_M_TPU_SOURCE_BAM : FD_TXN_M_TPU_SOURCE_BUNDLE;
  if( bam ) {
    bundle[0]->bam = (fd_txn_bam_t){ .seq_id=(uint)id, .scheduler_gen=1U };
  }
  if( alt_mode ) {
    fd_txn_t * txn = TXN( bundle[0]->txnp );
    txn->transaction_version          = FD_TXN_V0;
    txn->addr_table_adtl_cnt           = 1U;
    txn->addr_table_adtl_writable_cnt  = (uchar)(alt_mode==2);
    fd_memset( bundle[0]->alt_accts[0].b, 'X', sizeof(fd_acct_addr_t) );
  }
  ulong deleted;
  FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 1UL, 1000UL, FD_PACK_IB_TYPE_NONE,
                                       &id, &deleted )>=0 );
  FD_TEST( !deleted );
}

static void
test_bam_lookahead_permissions( void ) {
  struct permission_case {
    char const * name;
    char const * head_writes;
    char const * head_reads;
    char const * successor_writes;
    char const * successor_reads;
    int head_alt;
    int successor_alt;
    int live_reads_q;
    int extra_successor;
    int be_barrier;
    ulong expected;
  } const cases[9] = {
    { "head_write_successor_read",  "AX", "",  "Y",  "X", 0, 0, 0, 0, 0, 0UL   },
    { "head_read_successor_write",  "A",  "X", "XY", "",  0, 0, 0, 0, 0, 0UL   },
    { "head_write_successor_write", "AX", "",  "X",  "",  0, 0, 0, 0, 0, 0UL   },
    { "head_alt_write",            "A",  "",  "Y",  "X", 2, 0, 0, 0, 0, 0UL   },
    { "successor_alt_write",       "A",  "X", "Y",  "",  0, 2, 0, 0, 0, 0UL   },
    { "successor_alt_read",        "AX", "",  "Y",  "",  0, 1, 0, 0, 0, 0UL   },
    { "shared_alt_read",           "A",  "X", "Y",  "",  0, 1, 0, 0, 0, 705UL },
    { "inflight_read_barrier",     "A",  "",  "Q",  "",  0, 0, 1, 1, 0, 706UL },
    { "block_engine_barrier",      "A",  "",  "Z",  "",  0, 0, 0, 0, 1, 0UL   },
  };
  for( ulong kind=0UL; kind<9UL; kind++ ) for( int worker=0; worker<2; worker++ ) {
    struct permission_case const * c = &cases[kind];
    fd_pack_t * pack = init_all_with_meta( 64UL, 2UL, 1UL, sizeof(ulong), &outcome );
    fd_pack_set_initializer_bundles_ready( pack );
    fd_txn_e_t * live = fd_pack_insert_txn_init( pack );
    make_transaction1( live->txnp, 900UL, 1000U, 32U, 12.0, "A", c->live_reads_q ? "Q" : "", NULL, NULL );
    ulong deleted;
    FD_TEST( fd_pack_insert_txn_fini( pack, live, 1000UL, &deleted )>=0 );
    FD_TEST( fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                               0UL, FD_PACK_SCHEDULE_TXN, outcome.results )==1UL );
    insert_lookahead_permission_bundle( pack, 700UL, c->head_writes, c->head_reads, c->head_alt, 1 );
    if( c->be_barrier ) insert_lookahead_permission_bundle( pack, 704UL, "Y", "", 0, 0 );
    insert_lookahead_permission_bundle( pack, 705UL, c->successor_writes, c->successor_reads,
                                        c->successor_alt, 1 );
    if( c->extra_successor ) insert_lookahead_permission_bundle( pack, 706UL, "Z", "", 0, 1 );
    int flags = FD_PACK_SCHEDULE_BAM_ONLY | (worker ? FD_PACK_SCHEDULE_BAM_SINGLE : FD_PACK_SCHEDULE_BUNDLE) |
                FD_PACK_SCHEDULE_BAM_READY;
    test_lookahead_ready_t ready = { .barrier_id=ULONG_MAX, .multi_id=700UL,
                                     .multi_cnt=1UL, .calls=NULL };
    ulong hint;
    FD_TEST( fd_pack_peek_bundle_candidate( pack, 1, &hint, NULL ) );
    ulong scheduled = fd_pack_schedule_next_microblock_with_bundle_hint( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK,
                        0.0f, 1UL, flags, hint, test_lookahead_ready, &ready, outcome.results );
    ulong got = scheduled ? candidate_test_txn_id( outcome.results[0].txnp ) : 0UL;
    if( got!=c->expected ) FD_LOG_ERR(( "lookahead permission %s worker=%d got=%lu expected=%lu",
                                       c->name, worker, got, c->expected ));
    if( scheduled ) FD_TEST( fd_pack_microblock_complete( pack, 1UL ) );
    FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );
    fd_pack_delete( fd_pack_leave( pack ) );
  }
}

static void
insert_ordinal_test_bundle_capture( fd_pack_t * pack, ulong id, ulong cnt, int bam, int initializer,
                                    char const * writes, fd_ed25519_sig_t * member_sig ) {
  fd_txn_e_t * slots[ FD_PACK_MAX_TXN_PER_BUNDLE ];
  fd_txn_e_t * const * bundle = fd_pack_insert_bundle_init( pack, slots, cnt );
  for( ulong j=0UL; j<cnt; j++ ) {
    /* Exercise integer priority rounding with widely differing costs. */
    make_transaction1( bundle[j]->txnp, id+j, (j&1UL) ? 1U : 1400000U, 32U, 5.0, writes, "", NULL, NULL );
    if( member_sig && j==1UL )
      fd_memcpy( *member_sig, fd_txn_get_signatures( TXN(bundle[j]->txnp), bundle[j]->txnp->payload ), sizeof(*member_sig) );
    if( bam ) {
      bundle[j]->txnp->source_tpu = FD_TXN_M_TPU_SOURCE_BAM;
      bundle[j]->bam = (fd_txn_bam_t){ .seq_id=(uint)id, .scheduler_gen=1U, .batch_idx=(uchar)j };
    }
  }
  ulong deleted;
  FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, cnt, 1000UL, initializer, &id, &deleted )>=0 );
  FD_TEST( !deleted );
}

static void
insert_ordinal_test_bundle( fd_pack_t * pack, ulong id, ulong cnt, int bam, int initializer, char const * writes ) {
  insert_ordinal_test_bundle_capture( pack, id, cnt, bam, initializer, writes, NULL );
}

static void
churn_bundle_ordinals( fd_pack_t * pack, ulong count ) {
  fd_txn_p_t churn[1];
  make_transaction1( churn, 900UL, 1000U, 32U, 5.0, "", "", NULL, NULL );
  churn->source_tpu = FD_TXN_M_TPU_SOURCE_BAM;
  fd_ed25519_sig_t sig;
  fd_memcpy( sig, fd_txn_get_signatures( TXN(churn), churn->payload ), sizeof(sig) );
  for( ulong i=0UL; i<count; i++ ) {
    fd_txn_e_t * slots[1];
    fd_txn_e_t * const * bundle = fd_pack_insert_bundle_init( pack, slots, 1UL );
    *bundle[0]->txnp = *churn;
    bundle[0]->bam = (fd_txn_bam_t){ .seq_id=900U, .scheduler_gen=1U };
    ulong deleted;
    FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 1UL, 1000UL, FD_PACK_IB_TYPE_NONE,
                                         NULL, &deleted )>=0 );
    FD_TEST( !deleted );
    FD_TEST( fd_pack_delete_bam_bundle( pack, (fd_ed25519_sig_t const *)(void const *)&sig,
                                        900U, 1U )==1UL );
  }
}

/* A retained five-member batch plus one reused ordinal used to exceed the
   scheduler/deleter's bounded scratch.  Cross the real insertion counter
   through the public API, keeping original BAM/BE identities and metadata. */
static void
test_bundle_ordinal_rebase( void ) {
  for( int mode=0; mode<4; mode++ ) {
    fd_pack_t * pack = init_all_with_meta( 64UL, 2UL, 8UL, sizeof(ulong), &outcome );
    fd_pack_set_initializer_bundles_ready( pack );
    if( mode==3 ) {
      ulong written_cost = 0UL;
      for( ulong i=0UL;; i++ ) {
        fd_txn_e_t * txn = fd_pack_insert_txn_init( pack );
        ulong cost, deleted;
        make_transaction1( txn->txnp, 10000UL+i, 1000000U, 32U, 5.0, "X", "", NULL, &cost );
        FD_TEST( fd_pack_insert_txn_fini( pack, txn, 1000UL, &deleted )>=0 );
        FD_TEST( fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                                  0UL, FD_PACK_SCHEDULE_TXN, outcome.results )==1UL );
        FD_TEST( fd_pack_microblock_complete( pack, 0UL ) );
        written_cost += cost;
        if( FD_PACK_TEST_MAX_WRITE_COST_PER_ACCT-written_cost<1400000UL ) break;
      }
    }
    if( mode==1 || mode==2 ) {
      insert_ordinal_test_bundle( pack, 600UL, 1UL, 0, FD_PACK_IB_TYPE_BAM, "" );
    }
    if( mode==2 ) {
      FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL,
                FD_PACK_SCHEDULE_BUNDLE | FD_PACK_SCHEDULE_BAM_ONLY, outcome.results )==1UL );
      FD_TEST( fd_pack_microblock_complete( pack, 0UL ) );
    }
    insert_ordinal_test_bundle( pack, 700UL, 5UL, 1, FD_PACK_IB_TYPE_NONE, mode==3 ? "X" : "" );
    insert_ordinal_test_bundle( pack, 800UL, 2UL, 0, FD_PACK_IB_TYPE_NONE, "" );
    if( mode==3 ) {
      for( ulong i=0UL; i<50UL; i++ )
        FD_TEST( !schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL,
                   FD_PACK_SCHEDULE_BUNDLE | FD_PACK_SCHEDULE_BAM_ONLY, outcome.results ) );
    }

    for( ulong round=0UL; round<(mode==0 ? 2UL : 1UL); round++ ) {
      churn_bundle_ordinals( pack, 313721UL );
      FD_TEST( fd_pack_avail_txn_cnt( pack )==(mode==1 ? 8UL : 7UL) );
      FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );
      ulong hint;
      void const * meta;
      fd_txn_e_t const * head = fd_pack_peek_bundle_candidate( pack, 1, &hint, &meta );
      if( mode==3 ) FD_TEST( !head ); /* rebase must not reset deferral */
      else {
        FD_TEST( head && candidate_test_txn_id( head->txnp )==(mode==1 ? 600UL : 700UL) );
        if( mode==1 || mode==2 ) FD_TEST( !meta ); /* queued/Pending initializer */
        else FD_TEST( meta && *(ulong const *)meta==700UL );
      }
    }
    if( mode==1 ) {
      FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL,
                FD_PACK_SCHEDULE_BUNDLE | FD_PACK_SCHEDULE_BAM_ONLY, outcome.results )==1UL );
      FD_TEST( candidate_test_txn_id( outcome.results[0].txnp )==600UL );
      FD_TEST( fd_pack_microblock_complete( pack, 0UL ) );
    }
    if( mode==1 || mode==2 ) {
      FD_TEST( !schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL,
                 FD_PACK_SCHEDULE_BUNDLE | FD_PACK_SCHEDULE_BAM_ONLY, outcome.results ) );
      fd_pack_rebate_t rebate = { .ib_result=1 };
      fd_pack_rebate_cus( pack, &rebate );
    }
    if( mode==3 ) { fd_pack_end_block( pack ); fd_pack_set_initializer_bundles_ready( pack ); }
    for( ulong group=0UL; group<2UL; group++ ) {
      ulong hint;
      void const * meta;
      fd_txn_e_t const * head = fd_pack_peek_bundle_candidate( pack, 0, &hint, &meta );
      ulong id = group ? 800UL : 700UL;
      FD_TEST( head && candidate_test_txn_id( head->txnp )==id && meta && *(ulong const *)meta==id );
      ulong cnt = group ? 2UL : 5UL;
      FD_TEST( fd_pack_schedule_next_microblock_with_bundle_hint( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                0UL, FD_PACK_SCHEDULE_BUNDLE | FD_PACK_SCHEDULE_BAM_READY, hint, NULL, NULL, outcome.results )==cnt );
      for( ulong j=0UL; j<cnt; j++ ) FD_TEST( candidate_test_txn_id( outcome.results[j].txnp )==id+j );
      FD_TEST( fd_pack_microblock_complete( pack, 0UL ) );
    }
    FD_TEST( !fd_pack_avail_txn_cnt( pack ) && !fd_pack_verify( pack, pack_verify_scratch ) );
    fd_pack_delete( fd_pack_leave( pack ) );
  }
}

static void
test_bundle_ordinal_rebase_group_boundaries( void ) {
  enum { ORDINAL_LIMIT = 313721 };
  fd_pack_t * pack = init_all_with_meta( 128UL, 2UL, 8UL, sizeof(ulong), &outcome );
  fd_pack_set_initializer_bundles_ready( pack );
  for( ulong group=0UL; group<12UL; group++ )
    insert_ordinal_test_bundle( pack, 1000UL+10UL*group, group%5UL+1UL,
                                (int)(group&1UL), FD_PACK_IB_TYPE_NONE, "" );
  /* Relative ordinal is now 13.  Place two retained groups at N-1 and N,
     then make the next insert rebase them with every earlier group. */
  churn_bundle_ordinals( pack, ORDINAL_LIMIT-14UL );
  insert_ordinal_test_bundle( pack, 2000UL, 4UL, 1, FD_PACK_IB_TYPE_NONE, "" );
  fd_ed25519_sig_t member_sig;
  insert_ordinal_test_bundle_capture( pack, 2010UL, 5UL, 0, FD_PACK_IB_TYPE_NONE, "", &member_sig );
  insert_ordinal_test_bundle( pack, 2020UL, 1UL, 1, FD_PACK_IB_TYPE_NONE, "" );
  FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );

  FD_TEST( fd_pack_delete_transaction( pack, (fd_ed25519_sig_t const *)(void const *)&member_sig )==5UL );
  FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );

  for( ulong group=0UL; group<14UL; group++ ) {
    ulong id  = group<12UL ? 1000UL+10UL*group : group==12UL ? 2000UL : 2020UL;
    ulong cnt = group<12UL ? group%5UL+1UL      : group==12UL ? 4UL    : 1UL;
    ulong hint;
    void const * meta;
    fd_txn_e_t const * head = fd_pack_peek_bundle_candidate( pack, 0, &hint, &meta );
    FD_TEST( head && candidate_test_txn_id( head->txnp )==id && meta && *(ulong const *)meta==id );
    FD_TEST( fd_pack_schedule_next_microblock_with_bundle_hint( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK,
              0.0f, 0UL, FD_PACK_SCHEDULE_BUNDLE | FD_PACK_SCHEDULE_BAM_READY,
              hint, NULL, NULL, outcome.results )==cnt );
    for( ulong i=0UL; i<cnt; i++ ) FD_TEST( candidate_test_txn_id( outcome.results[i].txnp )==id+i );
    FD_TEST( fd_pack_microblock_complete( pack, 0UL ) );
  }
  FD_TEST( !fd_pack_avail_txn_cnt( pack ) && !fd_pack_verify( pack, pack_verify_scratch ) );
  fd_pack_delete( fd_pack_leave( pack ) );
}

static void
test_bundle_ordinal_rebase_initializer_replacement( void ) {
  enum { ORDINAL_LIMIT = 313721 };
  for( int with_group=0; with_group<2; with_group++ ) {
    fd_pack_t * pack = init_all_with_meta( 64UL, 2UL, 8UL, sizeof(ulong), &outcome );
    fd_pack_set_initializer_bundles_ready( pack );
    insert_ordinal_test_bundle( pack, 3000UL, 3UL, 1, FD_PACK_IB_TYPE_BAM, "" );
    if( with_group ) insert_ordinal_test_bundle( pack, 3200UL, 2UL, 0, FD_PACK_IB_TYPE_NONE, "" );
    churn_bundle_ordinals( pack, ORDINAL_LIMIT-(ulong)with_group );

    fd_txn_e_t * slots[3];
    fd_txn_e_t * const * replacement = fd_pack_insert_bundle_init( pack, slots, 3UL );
    for( ulong i=0UL; i<3UL; i++ ) {
      make_transaction1( replacement[i]->txnp, 3100UL+i, i&1UL ? 1U : 1400000U,
                         32U, 5.0, "", "", NULL, NULL );
      replacement[i]->txnp->source_tpu    = FD_TXN_M_TPU_SOURCE_BAM;
      replacement[i]->bam = (fd_txn_bam_t){ .seq_id=3100U, .batch_idx=(uchar)i };
    }
    fd_ed25519_sig_t ib_sig;
    fd_memcpy( ib_sig, fd_txn_get_signatures( TXN(replacement[0]->txnp), replacement[0]->txnp->payload ),
               sizeof(ib_sig) );
    ulong id = 3100UL;
    ulong deleted;
    FD_TEST( fd_pack_insert_bundle_fini( pack, replacement, 3UL, 1000UL, FD_PACK_IB_TYPE_BAM,
                                         &id, &deleted )>=0 );
    FD_TEST( deleted==3UL && !fd_pack_verify( pack, pack_verify_scratch ) );
    ulong hint;
    fd_txn_e_t const * head = fd_pack_peek_bundle_candidate( pack, 1, &hint, NULL );
    FD_TEST( head && candidate_test_txn_id( head->txnp )==3100UL );
    FD_TEST( fd_pack_delete_transaction( pack, (fd_ed25519_sig_t const *)(void const *)&ib_sig )==3UL );
    if( with_group ) {
      void const * meta;
      head = fd_pack_peek_bundle_candidate( pack, 0, &hint, &meta );
      FD_TEST( head && candidate_test_txn_id( head->txnp )==3200UL && meta && *(ulong const *)meta==3200UL );
    }
    FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );
    fd_pack_delete( fd_pack_leave( pack ) );
  }
}

/* A deferred initializer still heads the bundle queue, so crank metadata
   is never offered for a later bundle (its tip configuration would be
   stale), in either scheduling mode. */
static void
test_deferred_initializer_suppresses_meta( void ) {
  for( int bam=0; bam<2; bam++ ) {
    fd_pack_t * pack = init_all_with_meta( 64UL, 2UL, 8UL, sizeof(ulong), &outcome );
    ulong written_cost = 0UL;
    for( ulong i=0UL;; i++ ) {
      fd_txn_e_t * txn = fd_pack_insert_txn_init( pack );
      ulong cost, deleted;
      make_transaction1( txn->txnp, 10000UL+i, 1000000U, 32U, 5.0, "X", "", NULL, &cost );
      FD_TEST( fd_pack_insert_txn_fini( pack, txn, 1000UL, &deleted )>=0 );
      FD_TEST( fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                                0UL, FD_PACK_SCHEDULE_TXN, outcome.results )==1UL );
      FD_TEST( fd_pack_microblock_complete( pack, 0UL ) );
      written_cost += cost;
      if( FD_PACK_TEST_MAX_WRITE_COST_PER_ACCT-written_cost<1400000UL ) break;
    }
    insert_ordinal_test_bundle( pack, 600UL, 1UL, bam, bam ? FD_PACK_IB_TYPE_BAM : FD_PACK_IB_TYPE_NORMAL, "X" );
    for( ulong i=0UL; i<50UL; i++ )
      FD_TEST( !schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL,
                 FD_PACK_SCHEDULE_BUNDLE | (bam ? FD_PACK_SCHEDULE_BAM_ONLY : 0), outcome.results ) );
    insert_ordinal_test_bundle( pack, 700UL, 1UL, bam, FD_PACK_IB_TYPE_NONE, "" );
    ulong hint;
    void const * meta = &outcome;
    fd_txn_e_t const * head = fd_pack_peek_bundle_candidate( pack, bam, &hint, &meta );
    FD_TEST( head && candidate_test_txn_id( head->txnp )==700UL ); /* the initializer is deferred */
    FD_TEST( !meta && !fd_pack_peek_bundle_meta( pack ) );
    fd_pack_delete( fd_pack_leave( pack ) );
  }
}

static void
test_deferred_slot_rollover( void ) {
  fd_pack_t * pack = init_all_with_meta( 64UL, 2UL, 8UL, 48UL, &outcome );
  fd_pack_set_initializer_bundles_ready( pack );
  ulong written_cost = 0UL;
  ulong deleted;
  for( ulong i=0UL;; i++ ) {
    ulong cost;
    fd_txn_e_t * txn = fd_pack_insert_txn_init( pack );
    make_transaction1( txn->txnp, i, 1000000U, 32U, 5.0, "X", "", NULL, &cost );
    FD_TEST( fd_pack_insert_txn_fini( pack, txn, ULONG_MAX, &deleted )>=0 );
    FD_TEST( fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                               0UL, FD_PACK_SCHEDULE_TXN, outcome.results )==1UL );
    FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );
    written_cost += cost;
    if( FD_PACK_TEST_MAX_WRITE_COST_PER_ACCT-written_cost<1400000UL ) break;
  }
  fd_txn_e_t * txn = fd_pack_insert_txn_init( pack );
  make_transaction1( txn->txnp, 700UL, 1400000U, 32U, 5.0, "X", "", NULL, NULL );
  FD_TEST( fd_pack_insert_txn_fini( pack, txn, ULONG_MAX, &deleted )>=0 );
  insert_bam_candidate_test( pack, 701UL, 1UL, 1400000U, 1U, "X", "", 0 );
  int flags = FD_PACK_SCHEDULE_BAM_ONLY | FD_PACK_SCHEDULE_BUNDLE;
  for( ulong i=0UL; i<64UL; i++ ) {
    FD_TEST( !fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                                0UL, FD_PACK_SCHEDULE_TXN, outcome.results ) );
    FD_TEST( !schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                      0UL, flags, outcome.results ) );
  }
  ulong hint;
  FD_TEST( !fd_pack_peek_bundle_candidate( pack, 1, &hint, NULL ) );
  /* Compressed slots start at 51 and cycle through [51,65535]. */
  for( ulong i=0UL; i<USHORT_MAX-50UL; i++ ) fd_pack_end_block( pack );
  fd_pack_set_initializer_bundles_ready( pack );
  FD_TEST( fd_pack_avail_txn_cnt( pack )==2UL );
  FD_TEST( fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                             0UL, FD_PACK_SCHEDULE_TXN, outcome.results )==1UL );
  FD_TEST( candidate_test_txn_id( outcome.results[0].txnp )==700UL );
  FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );
  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                   0UL, flags, outcome.results )==1UL );
  FD_TEST( candidate_test_txn_id( outcome.results[0].txnp )==701UL );
  FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );
  FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );
  fd_pack_delete( fd_pack_leave( pack ) );

  /* A partially spent budget must survive the same rollover. */
  pack = init_all_with_meta( 64UL, 2UL, 8UL, 48UL, &outcome );
  for( ulong i=0UL; i<USHORT_MAX-51UL; i++ ) fd_pack_end_block( pack );
  fd_pack_set_initializer_bundles_ready( pack );
  fill_for_bam_capacity_test( pack, &outcome );
  insert_bam_candidate_test( pack, 700UL, 1UL, 1400000U, 1U, "X", "", 0 );
  insert_bam_candidate_test( pack, 701UL, 1UL, 2000U, 0U, "X", "", 0 );
  for( ulong i=0UL; i<49UL; i++ )
    FD_TEST( !schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                      0UL, flags, outcome.results ) );
  FD_TEST( candidate_test_txn_id( fd_pack_peek_bundle_candidate( pack, 1, &hint, NULL )->txnp )==700UL );
  fd_pack_end_block( pack );
  fd_pack_set_initializer_bundles_ready( pack );
  fill_for_bam_capacity_test( pack, &outcome );
  FD_TEST( !schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                    0UL, flags, outcome.results ) );
  FD_TEST( candidate_test_txn_id( fd_pack_peek_bundle_candidate( pack, 1, &hint, NULL )->txnp )==701UL );
  FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );
  fd_pack_delete( fd_pack_leave( pack ) );
}

/* As upstream, the candidate walk skips deferred bundle members one at a
   time and counts each once, in both modes.  In BAM mode a non-BAM bundle
   after the deferred one is filtered as a whole and not counted. */
static void
test_deferred_bundle_skip_count( void ) {
  for( int bam=0; bam<2; bam++ ) {
    fd_pack_t * pack = init_all_with_meta( 64UL, 2UL, 8UL, sizeof(ulong), &outcome );
    fd_pack_set_initializer_bundles_ready( pack );
    ulong written_cost = 0UL;
    for( ulong i=0UL;; i++ ) {
      fd_txn_e_t * txn = fd_pack_insert_txn_init( pack );
      ulong cost, deleted;
      make_transaction1( txn->txnp, 10000UL+i, 1000000U, 32U, 5.0, "X", "", NULL, &cost );
      FD_TEST( fd_pack_insert_txn_fini( pack, txn, 1000UL, &deleted )>=0 );
      FD_TEST( fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                                0UL, FD_PACK_SCHEDULE_TXN, outcome.results )==1UL );
      FD_TEST( fd_pack_microblock_complete( pack, 0UL ) );
      written_cost += cost;
      if( FD_PACK_TEST_MAX_WRITE_COST_PER_ACCT-written_cost<1400000UL ) break;
    }
    int flags = FD_PACK_SCHEDULE_BUNDLE | (bam ? FD_PACK_SCHEDULE_BAM_ONLY : 0);
    insert_ordinal_test_bundle( pack, 600UL, 3UL, bam, FD_PACK_IB_TYPE_NONE, "X" );
    for( ulong i=0UL; i<50UL; i++ )
      FD_TEST( !schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL, flags, outcome.results ) );
    insert_ordinal_test_bundle( pack, 650UL, 2UL, 0, FD_PACK_IB_TYPE_NONE, "" );
    insert_ordinal_test_bundle( pack, 700UL, 1UL, 1, FD_PACK_IB_TYPE_NONE, "" );
    ulong hint;
    fd_txn_e_t const * head = fd_pack_peek_bundle_candidate( pack, bam, &hint, NULL );
    FD_TEST( head && candidate_test_txn_id( head->txnp )==(bam ? 700UL : 650UL) );
    FD_TEST( ((hint>>16) & USHORT_MAX)==3UL );
    ulong before[ FD_METRICS_ENUM_PACK_TXN_SCHEDULE_CNT ], after[ FD_METRICS_ENUM_PACK_TXN_SCHEDULE_CNT ];
    fd_pack_get_sched_metrics( pack, before );
    FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL, flags, outcome.results )==(bam ? 1UL : 2UL) );
    FD_TEST( candidate_test_txn_id( outcome.results[0].txnp )==(bam ? 700UL : 650UL) );
    fd_pack_get_sched_metrics( pack, after );
    FD_TEST( after[ FD_METRICS_ENUM_PACK_TXN_SCHEDULE_V_DEFER_SKIP_IDX ]==before[ FD_METRICS_ENUM_PACK_TXN_SCHEDULE_V_DEFER_SKIP_IDX ]+3UL );
    FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );
    fd_pack_delete( fd_pack_leave( pack ) );
  }
}

/* Neither readiness from another mode nor readiness from a stale pool index
   permits dispatch.  Pool reservations alone leave the view valid. */
static void
test_bam_candidate_hint_lifetime( void ) {
  for( int mutation=0; mutation<12; mutation++ ) {
    fd_pack_t * pack = init_all_with_meta( 64UL, 2UL, 8UL, 48UL, &outcome );
    fd_pack_set_initializer_bundles_ready( pack );
    insert_bam_candidate_test( pack, 700UL, 1UL, 2000U, 0U, "X", "", 0 );
    ulong hint;
    fd_txn_e_t const * candidate = fd_pack_peek_bundle_candidate( pack, mutation==0 ? 0 : 1, &hint, NULL );
    FD_TEST( candidate && candidate_test_txn_id( candidate->txnp )==700UL );
    fd_ed25519_sig_t signature;
    fd_memcpy( signature, fd_txn_get_signatures( TXN(candidate->txnp), candidate->txnp->payload ), sizeof(signature) );
    switch( mutation ) {
      case 0: break; /* wrong mode */
      case 1: insert_bam_candidate_test( pack, 701UL, 1UL, 2000U, 0U, "Y", "", 0 ); break;
      case 2: FD_TEST( fd_pack_delete_transaction( pack, (fd_ed25519_sig_t const *)(void const *)signature )==1UL ); break;
      case 3: FD_TEST( fd_pack_delete_bam_bundle( pack, (fd_ed25519_sig_t const *)(void const *)signature, UINT_MAX, 9U )==1UL ); break;
      case 4: (void)fd_pack_expire_before( pack, 0UL ); break;
      case 5: fd_pack_end_block( pack ); break;
      case 6: fd_pack_clear_all( pack ); break;
      case 7: {
        fd_txn_e_t * txn = fd_pack_insert_txn_init( pack );
        make_transaction1( txn->txnp, 701UL, 2000U, 32U, 5.0, "Y", "", NULL, NULL );
        ulong deleted;
        FD_TEST( fd_pack_insert_txn_fini( pack, txn, 1000UL, &deleted )>=0 );
        break;
      }
      case 8: {
        fd_txn_e_t * slots[1];
        fd_pack_insert_bundle_init( pack, slots, 1UL );
        fd_pack_insert_bundle_cancel( pack, slots, 1UL );
        fd_txn_e_t * txn = fd_pack_insert_txn_init( pack );
        fd_pack_insert_txn_cancel( pack, txn );
        break;
      }
      case 9: FD_TEST( fd_pack_schedule_next_microblock( pack, 0UL, 0.0f, 0UL, 0, outcome.results )==0UL ); break;
      case 10: fd_pack_set_initializer_bundles_ready( pack ); break;
      case 11: { fd_pack_rebate_t rebate = {0}; fd_pack_rebate_cus( pack, &rebate ); break; }
    }
    int flags = FD_PACK_SCHEDULE_BUNDLE | FD_PACK_SCHEDULE_BAM_ONLY | FD_PACK_SCHEDULE_BAM_READY;
    FD_TEST( fd_pack_schedule_next_microblock_with_bundle_hint( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                                               0UL, flags, hint, NULL, NULL, outcome.results )==(mutation==8) );
    if( mutation==8 ) FD_TEST( candidate_test_txn_id( outcome.results[0].txnp )==700UL );
    /* No hint and no readiness are separately fail-closed, even with full permission. */
    FD_TEST( fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                               0UL, flags, outcome.results )==0UL );
    (void)fd_pack_peek_bundle_candidate( pack, 1, &hint, NULL );
    FD_TEST( fd_pack_schedule_next_microblock_with_bundle_hint( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f,
                                                               0UL, flags & ~FD_PACK_SCHEDULE_BAM_READY, hint, NULL, NULL, outcome.results )==0UL );
    FD_TEST( !!fd_pack_current_block_cost( pack )==(mutation==8) );
    FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );
    fd_pack_delete( fd_pack_leave( pack ) );
  }
}

static void
test_bam_candidate_initializer_states( void ) {
  fd_pack_t * pack = init_all_with_meta( 64UL, 2UL, 8UL, 48UL, &outcome );
  fd_txn_e_t * slots[ FD_PACK_MAX_TXN_PER_BUNDLE ];
  void const * meta = &outcome;
  ulong hint;
  FD_TEST( !fd_pack_peek_bundle_candidate( pack, 1, &hint, &meta ) );
  FD_TEST( !meta && hint==ULONG_MAX );
  insert_bam_candidate_test( pack, 700UL, 1UL, 2000U, 1U, "X", "", 0 );
  insert_mode_test_bundle( pack, slots, 710UL, FD_TXN_M_TPU_SOURCE_BUNDLE, FD_PACK_IB_TYPE_BAM, NULL );
  int full = FD_PACK_SCHEDULE_BUNDLE | FD_PACK_SCHEDULE_BAM_ONLY;
  int single = FD_PACK_SCHEDULE_BAM_SINGLE | FD_PACK_SCHEDULE_BAM_ONLY;
  FD_TEST( !fd_pack_peek_bundle_meta( pack ) );
  fd_txn_e_t const * candidate = fd_pack_peek_bundle_candidate( pack, 1, &hint, &meta );
  FD_TEST( candidate && (candidate->txnp->flags & FD_TXN_P_FLAGS_INITIALIZER_BUNDLE) );
  FD_TEST( !meta && hint!=ULONG_MAX );
  for( ulong i=0UL; i<128UL; i++ )
    FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 1UL, single, outcome.results )==0UL );
  FD_TEST( !fd_pack_current_block_cost( pack ) );
  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL, full, outcome.results )==1UL );
  FD_TEST( outcome.results[0].txnp->flags & FD_TXN_P_FLAGS_INITIALIZER_BUNDLE );
  FD_TEST( fd_pack_microblock_complete( pack, 0UL )==1 );
  /* Completion releases account locks, but only the rebate releases Pending. */
  meta = &outcome;
  candidate = fd_pack_peek_bundle_candidate( pack, 1, &hint, &meta );
  FD_TEST( candidate && candidate_test_txn_id( candidate->txnp )==700UL );
  FD_TEST( !meta && hint!=ULONG_MAX ); /* Pending suppresses metadata only. */
  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 1UL, single, outcome.results )==0UL );
  fd_pack_rebate_t rebate = { .ib_result=-1 };
  fd_pack_rebate_cus( pack, &rebate );
  FD_TEST( !fd_pack_peek_bundle_meta( pack ) );
  meta = &outcome;
  candidate = fd_pack_peek_bundle_candidate( pack, 1, &hint, &meta );
  FD_TEST( candidate && candidate_test_txn_id( candidate->txnp )==700UL );
  FD_TEST( !meta && hint!=ULONG_MAX ); /* Failed blocks BAM dispatch for the slot. */
  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 1UL, single, outcome.results )==0UL );
  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL, full, outcome.results )==0UL );
  fd_ed25519_sig_t sig;
  fd_memcpy( sig, fd_txn_get_signatures( TXN(candidate->txnp), candidate->txnp->payload ), sizeof(sig) );
  FD_TEST( fd_pack_delete_transaction( pack, (fd_ed25519_sig_t const *)(void const *)&sig )==1UL );

  /* Restricted permission never grants worker eligibility to Block Engine. */
  fd_pack_end_block( pack );
  fd_pack_set_initializer_bundles_ready( pack );
  insert_mode_test_bundle( pack, slots, 711UL, FD_TXN_M_TPU_SOURCE_BUNDLE, FD_PACK_IB_TYPE_NONE, NULL );
  candidate = fd_pack_peek_bundle_candidate( pack, 0, &hint, &meta );
  FD_TEST( candidate && candidate_test_txn_id( candidate->txnp )==711UL && meta );
  FD_TEST( fd_pack_peek_bundle_meta( pack )==meta );
  FD_TEST( schedule_slot_ready_bam( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 1UL,
                                   FD_PACK_SCHEDULE_BAM_SINGLE, outcome.results )==0UL );
  FD_TEST( fd_pack_avail_txn_cnt( pack )==1UL );
  FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );
  fd_pack_delete( fd_pack_leave( pack ) );
}

/* Generic pack regression: initializer-bundle state machine behavior. */
static void
test_initializer_bundle_state_machine( void ) {
  fd_pack_t * pack = init_all_with_meta( 64UL, 1UL, 8UL, 64UL, &outcome );

  fd_txn_e_t * _bundle[FD_PACK_MAX_TXN_PER_BUNDLE];
  ulong _deleted;

  /* State: [Not Initialized] */
  /* Insert a regular bundle */
  fd_txn_e_t * const * bundle = fd_pack_insert_bundle_init( pack, _bundle, 2UL );
  make_transaction1( bundle[0]->txnp, 0UL, 2000U, 32U, 10.0, "a", "", NULL, NULL );
  make_transaction1( bundle[1]->txnp, 1UL, 2000U, 32U, 10.0, "b", "", NULL, NULL );
  FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 2UL, 1000UL, 0, NULL, &_deleted )>=0 );

  /* Cannot schedule regular bundle in [Not Initialized] state */
  ulong txn_cnt = fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL, FD_PACK_SCHEDULE_BUNDLE, outcome.results );
  FD_TEST( txn_cnt==0UL );

  /* Insert an initializer bundle */
  bundle = fd_pack_insert_bundle_init( pack, _bundle, 1UL );
  make_transaction1( bundle[0]->txnp, 2UL, 10000U, 32U, 10.0, "c", "", NULL, NULL );
  FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 1UL, 1000UL, 1, NULL, &_deleted )>=0 );

  /* Now IB should schedule, transitioning to [Pending] */
  txn_cnt = fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL, FD_PACK_SCHEDULE_BUNDLE, outcome.results );
  FD_TEST( txn_cnt==1UL );
  FD_TEST( outcome.results[0].txnp->flags & FD_TXN_P_FLAGS_INITIALIZER_BUNDLE );
  fd_pack_microblock_complete( pack, 0UL );

  /* State: [Pending] - Cannot schedule regular bundle yet */
  txn_cnt = fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL, FD_PACK_SCHEDULE_BUNDLE, outcome.results );
  FD_TEST( txn_cnt==0UL );

  /* Simulate IB success via rebate (transition to [Ready]) */
  fd_pack_rebate_t rebate[1];
  fd_memset( rebate, 0, sizeof(fd_pack_rebate_t) );
  rebate->total_cost_rebate = 5000U;
  rebate->ib_result         = 1;
  fd_pack_rebate_cus( pack, rebate );

  /* State: [Ready] - Now regular bundle should schedule */
  txn_cnt = fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL, FD_PACK_SCHEDULE_BUNDLE, outcome.results );
  FD_TEST( txn_cnt==2UL );
  FD_TEST( !(outcome.results[0].txnp->flags & FD_TXN_P_FLAGS_INITIALIZER_BUNDLE) );
  fd_pack_microblock_complete( pack, 0UL );
  fd_pack_end_block( pack );

  /* Insert first initializer bundle */
  bundle = fd_pack_insert_bundle_init( pack, _bundle, 1UL );
  make_transaction1( bundle[0]->txnp, 3UL, 10000U, 32U, 10.0, "d", "", NULL, NULL );
  FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 1UL, 1000UL, 1, NULL, &_deleted )>=0 );
  FD_TEST( fd_pack_avail_txn_cnt( pack )==1UL );

  /* Insert second initializer bundle - should replace first */
  bundle = fd_pack_insert_bundle_init( pack, _bundle, 1UL );
  make_transaction1( bundle[0]->txnp, 4UL, 10000U, 32U, 10.0, "e", "", NULL, NULL );
  FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 1UL, 1000UL, 1, NULL, &_deleted )>=0 );
  FD_TEST( _deleted==1UL ); /* Previous IB was deleted */
  FD_TEST( fd_pack_avail_txn_cnt( pack )==1UL );

  /* Only the second IB should be scheduled */
  txn_cnt = fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL, FD_PACK_SCHEDULE_BUNDLE, outcome.results );
  FD_TEST( txn_cnt==1UL );
  FD_TEST( outcome.results[0].txnp->flags & FD_TXN_P_FLAGS_INITIALIZER_BUNDLE );
  fd_pack_microblock_complete( pack, 0UL );

  /* End block should reset to [Not Initialized] */
  fd_pack_end_block( pack );

  /* Insert regular bundle */
  bundle = fd_pack_insert_bundle_init( pack, _bundle, 1UL );
  make_transaction1( bundle[0]->txnp, 5UL, 2000U, 32U, 10.0, "f", "", NULL, NULL );
  FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 1UL, 1000UL, 0, NULL, &_deleted )>=0 );

  /* Should not schedule in [Not Initialized] state */
  txn_cnt = fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL, FD_PACK_SCHEDULE_BUNDLE, outcome.results );
  FD_TEST( txn_cnt==0UL );

  fd_pack_clear_all( pack );

  /* Insert and schedule initializer bundle */
  bundle = fd_pack_insert_bundle_init( pack, _bundle, 1UL );
  make_transaction1( bundle[0]->txnp, 6UL, 10000U, 32U, 10.0, "g", "", NULL, NULL );
  FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 1UL, 1000UL, 1, NULL, &_deleted )>=0 );

  txn_cnt = fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL, FD_PACK_SCHEDULE_BUNDLE, outcome.results );
  FD_TEST( txn_cnt==1UL );
  fd_pack_microblock_complete( pack, 0UL );

  /* State: [Pending] - Simulate IB failure via rebate (transition to [Failed]) */
  fd_memset( rebate, 0, sizeof(fd_pack_rebate_t) );
  rebate->total_cost_rebate = 5000U;
  rebate->ib_result         = -1;
  fd_pack_rebate_cus( pack, rebate );

  /* Insert regular bundle */
  bundle = fd_pack_insert_bundle_init( pack, _bundle, 1UL );
  make_transaction1( bundle[0]->txnp, 7UL, 2000U, 32U, 10.0, "h", "", NULL, NULL );
  FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 1UL, 1000UL, 0, NULL, &_deleted )>=0 );

  /* State: [Failed] - Should not schedule regular bundle */
  txn_cnt = fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL, FD_PACK_SCHEDULE_BUNDLE, outcome.results );
  FD_TEST( txn_cnt==0UL );

  fd_pack_delete( fd_pack_leave( pack ) );
}

/* Generic pack regression: opaque bundle metadata storage and lifetime. */
static void
test_bundle_metadata_persistence( void ) {
  typedef struct {
    ulong bundle_id;
    uint  commission;
    ulong custom_field;
  } test_meta_t;

  fd_pack_t * pack = init_all_with_meta( 64UL, 1UL, 8UL, sizeof(test_meta_t), &outcome );
  fd_pack_set_initializer_bundles_ready( pack );

  fd_txn_e_t * _bundle[FD_PACK_MAX_TXN_PER_BUNDLE];
  ulong _deleted;

  test_meta_t meta;
  meta.bundle_id = 12345UL;
  meta.commission = 42;
  meta.custom_field = 0xDEADBEEFUL;

  fd_txn_e_t * const * bundle = fd_pack_insert_bundle_init( pack, _bundle, 1UL );
  make_transaction1( bundle[0]->txnp, 0UL, 2000U, 32U, 10.0, "a", "", NULL, NULL );
  FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 1UL, 1000UL, 0, &meta, &_deleted )>=0 );

  /* Peek metadata before scheduling */
  test_meta_t const * stored = (test_meta_t const *)fd_pack_peek_bundle_meta( pack );
  FD_TEST( stored!=NULL );
  FD_TEST( stored->bundle_id==12345UL );
  FD_TEST( stored->commission==42 );
  FD_TEST( stored->custom_field==0xDEADBEEFUL );

  /* Schedule bundle */
  ulong txn_cnt = fd_pack_schedule_next_microblock( pack, FD_PACK_TEST_MAX_COST_PER_BLOCK, 0.0f, 0UL, FD_PACK_SCHEDULE_BUNDLE, outcome.results );
  FD_TEST( txn_cnt==1UL );
  fd_pack_microblock_complete( pack, 0UL );

  /* Metadata should no longer be accessible after scheduling */
  stored = (test_meta_t const *)fd_pack_peek_bundle_meta( pack );
  FD_TEST( stored==NULL );

  fd_pack_delete( fd_pack_leave( pack ) );
}

/* fd_pack_insert_bundle_fini reports the bundle member that caused a
   rejection through fd_pack_insert_bundle_reject_txn_idx. */
static void
test_bundle_reject_txn_idx( void ) {
  FD_LOG_NOTICE(( "TEST BUNDLE REJECT TXN IDX" ));
  fd_pack_t * pack = init_all( 1024UL, 1UL, 128UL, &outcome );

  fd_txn_e_t * _bundle[ FD_PACK_MAX_TXN_PER_BUNDLE ];
  ulong _deleted;

  fd_txn_e_t * const * bundle = fd_pack_insert_bundle_init( pack, _bundle, 2UL );
  make_transaction1      ( bundle[ 0 ]->txnp, 100UL, 1000U, 100U, 12.0, "A", "B", NULL, NULL );
  make_nonce_transaction1( bundle[ 1 ]->txnp, 101UL, 11.0, 4, 5, 'h' );
  int result = fd_pack_insert_bundle_fini( pack, bundle, 2UL, 1000UL, 0, NULL, &_deleted );
  FD_TEST( result==FD_PACK_INSERT_REJECT_INVALID_NONCE );
  FD_TEST( fd_pack_insert_bundle_reject_txn_idx( pack )==1UL );

  bundle = fd_pack_insert_bundle_init( pack, _bundle, 2UL );
  make_transaction1( bundle[ 0 ]->txnp, 102UL, 1000U, 100U, 12.0, "C", "D", NULL, NULL );
  make_transaction1( bundle[ 1 ]->txnp, 103UL, 1000U, 100U, 11.0, "E", "F", NULL, NULL );
  /* The parser now owns the per-instruction account bound.  Use a
     Pack-owned validation failure to retain coverage that the exact failing
     bundle member is reported. */
  TXN( bundle[ 1 ]->txnp )->addr_table_lookup_cnt        = 1U;
  TXN( bundle[ 1 ]->txnp )->addr_table_adtl_writable_cnt = 20U;
  TXN( bundle[ 1 ]->txnp )->addr_table_adtl_cnt          = 60U;
  result = fd_pack_insert_bundle_fini( pack, bundle, 2UL, 1000UL, 0, NULL, &_deleted );
  FD_TEST( result==FD_PACK_INSERT_REJECT_ACCOUNT_CNT );
  FD_TEST( fd_pack_insert_bundle_reject_txn_idx( pack )==1UL );

  /* As upstream, a nonce conflict (never BAM) rejects the whole bundle
     after the per-member checks. */
  for( ulong txn_cnt=2UL; txn_cnt<=FD_PACK_MAX_TXN_PER_BUNDLE; txn_cnt++ ) {
    for( ulong dup_idx_0=0UL; dup_idx_0<txn_cnt; dup_idx_0++ ) {
      for( ulong dup_idx_1=dup_idx_0+1UL; dup_idx_1<txn_cnt; dup_idx_1++ ) {
        /* All transactions are nonce transactions.  Every other member
           gets a distinct nonce account, so only the pair conflicts. */
        bundle = fd_pack_insert_bundle_init( pack, _bundle, txn_cnt );
        for( ulong i=0UL; i<txn_cnt; i++ ) {
          make_nonce_transaction1( bundle[ i ]->txnp, i, 11.0, 4, 0, (char)( 'a'+i ) );
          bundle[ i ]->txnp->payload[ TXN( bundle[ i ]->txnp )->acct_addr_off+4UL*32UL ] = (uchar)(i+1UL);
        }
        make_nonce_transaction1( bundle[ dup_idx_0 ]->txnp, dup_idx_0, 11.0, 4, 0, 'D' );
        make_nonce_transaction1( bundle[ dup_idx_1 ]->txnp, dup_idx_1, 11.0, 4, 0, 'E' ); /* Same nonce key despite a different hash */
        result = fd_pack_insert_bundle_fini( pack, bundle, txn_cnt, 1000UL, 0, NULL, &_deleted );
        FD_TEST( result==FD_PACK_INSERT_REJECT_NONCE_CONFLICT );
        FD_TEST( fd_pack_insert_bundle_reject_txn_idx( pack )==ULONG_MAX );

        /* Two nonce transactions among non-nonce transactions */
        bundle = fd_pack_insert_bundle_init( pack, _bundle, txn_cnt );
        for( ulong i=0UL; i<txn_cnt; i++ ) make_transaction1( bundle[ i ]->txnp, i, 1000U, 100U, 12.0-(double)i, "A", "B", NULL, NULL );
        make_nonce_transaction1( bundle[ dup_idx_0 ]->txnp, dup_idx_0, 11.0, 4, 0, 'D' );
        make_nonce_transaction1( bundle[ dup_idx_1 ]->txnp, dup_idx_1, 11.0, 4, 0, 'D' );
        result = fd_pack_insert_bundle_fini( pack, bundle, txn_cnt, 1000UL, 0, NULL, &_deleted );
        FD_TEST( result==FD_PACK_INSERT_REJECT_NONCE_CONFLICT );
        FD_TEST( fd_pack_insert_bundle_reject_txn_idx( pack )==ULONG_MAX );
      }
    }
  }
  FD_TEST( fd_pack_avail_txn_cnt( pack )==0UL );
  FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );

  fd_pack_delete( fd_pack_leave( pack ) );
}

/* make_nonce_transaction1 leaves source_tpu as the reused pool entry had
   it.  These fixtures set it, and trim the AdvanceNonce account list to
   acct_cnt.  Accounts 0 and 1 are the fixture's only signers. */
static void
make_nonce_scope_transaction1( fd_txn_p_t * txnp,
                               ulong        i,
                               uchar        nonce_acct_idx,
                               uchar        nonce_auth_idx,
                               uchar        acct_cnt,
                               ulong        source_tpu ) {
  make_nonce_transaction1( txnp, i, 11.0, nonce_acct_idx, nonce_auth_idx, 'n' );
  TXN( txnp )->instr[ 0 ].acct_cnt = acct_cnt;
  txnp->source_tpu = (uchar)source_tpu;
  txnp->flags      = 0U;
}

static void
make_scope_transaction1( fd_txn_p_t * txnp,
                         ulong        i,
                         char const * writes,
                         ulong        source_tpu ) {
  make_transaction1( txnp, i, 1000U, 500U, 11.0, writes, "", NULL, NULL );
  txnp->source_tpu = (uchar)source_tpu;
}

/* As upstream, a bundle member advancing a pending singleton's nonce
   replaces it as soon as it is examined, even if the bundle is later
   rejected for another member. */
static void
test_rejected_bundle_replaces_singleton_nonce( void ) {
  FD_LOG_NOTICE(( "TEST REJECTED BUNDLE REPLACES SINGLETON NONCE" ));
  ulong const quic = FD_TXN_M_TPU_SOURCE_QUIC;
  ulong const jito = FD_TXN_M_TPU_SOURCE_BUNDLE;
  fd_pack_t * pack = init_all( 16UL, 1UL, 16UL, &outcome );
  int const expected[ 3 ] = { FD_PACK_INSERT_REJECT_ACCOUNT_CNT, FD_PACK_INSERT_REJECT_INVALID_NONCE,
                              FD_PACK_INSERT_REJECT_NONCE_CONFLICT };
  for( ulong k=0UL; k<3UL; k++ ) {
    fd_txn_e_t * singleton = fd_pack_insert_txn_init( pack );
    make_nonce_scope_transaction1( singleton->txnp, 9000UL+10UL*k, 4U, 0U, 3U, quic );
    ulong deleted;
    FD_TEST( fd_pack_insert_txn_fini( pack, singleton, 100UL, &deleted )==FD_PACK_INSERT_ACCEPT_NONCE_NONVOTE_ADD );

    fd_txn_e_t * storage[ 2 ];
    fd_txn_e_t * const * bundle = fd_pack_insert_bundle_init( pack, storage, 2UL );
    make_nonce_scope_transaction1( bundle[0]->txnp, 9001UL+10UL*k, 4U, 0U, 3U, jito );
    if( k==0UL ) {
      make_nonce_scope_transaction1( bundle[1]->txnp, 9002UL, 5U, 1U, 3U, jito );
      TXN( bundle[1]->txnp )->addr_table_lookup_cnt        = 1U;
      TXN( bundle[1]->txnp )->addr_table_adtl_writable_cnt = 20U;
      TXN( bundle[1]->txnp )->addr_table_adtl_cnt          = 60U;
    } else {
      make_nonce_scope_transaction1( bundle[1]->txnp, 9002UL+10UL*k, 4U, k==1UL ? 5U : 0U, 3U, jito );
    }
    FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 2UL, 100UL, 0, NULL, &deleted )==expected[ k ] );
    FD_TEST( fd_pack_insert_bundle_reject_txn_idx( pack )==(k<2UL ? 1UL : ULONG_MAX) );
    FD_TEST( deleted==1UL && !fd_pack_avail_txn_cnt( pack ) );
    FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );
  }
}

/* Outside BAM, nonce handling is upstream's.  Short AdvanceNonce forms are
   ordinary work with their own expiry, an unsigned canonical authority is
   INVALID_NONCE, and nonce members do not exempt a bundle from the
   expires_at the pack tile computes from its other members. */
static void
test_non_bam_nonce_matches_upstream( void ) {
  FD_LOG_NOTICE(( "TEST NON-BAM NONCE MATCHES UPSTREAM" ));
  for( ulong source=FD_TXN_M_TPU_SOURCE_QUIC; source<=FD_TXN_M_TPU_SOURCE_TXSEND; source++ ) {
    fd_pack_t * pack = init_all( 128UL, 1UL, 128UL, &outcome );
    FD_TEST( !fd_pack_expire_before( pack, 300UL ) );

    ulong i = 10000UL*source;
    ulong deleted;
    fd_txn_e_t * txn;

    for( uchar acct_cnt=1U; acct_cnt<3U; acct_cnt++ ) {
      txn = fd_pack_insert_txn_init( pack );
      make_nonce_scope_transaction1( txn->txnp, i++, 0U, 0U, acct_cnt, source );
      FD_TEST( fd_pack_insert_txn_fini( pack, txn, 299UL, &deleted )==FD_PACK_INSERT_REJECT_EXPIRED );
      txn = fd_pack_insert_txn_init( pack );
      make_nonce_scope_transaction1( txn->txnp, i++, 0U, 0U, acct_cnt, source );
      FD_TEST( fd_pack_insert_txn_fini( pack, txn, 1000UL, &deleted )==FD_PACK_INSERT_ACCEPT_NONVOTE_ADD );
    }
    for( uchar nonce_acct_idx=0U; nonce_acct_idx<8U; nonce_acct_idx=(uchar)(nonce_acct_idx+4U) ) {
      txn = fd_pack_insert_txn_init( pack );
      make_nonce_scope_transaction1( txn->txnp, i++, nonce_acct_idx, 5U, 3U, source );
      FD_TEST( fd_pack_insert_txn_fini( pack, txn, 1000UL, &deleted )==FD_PACK_INSERT_REJECT_INVALID_NONCE );
    }
    /* A signed canonical nonce ignores a stale expires_at and lives until
       expire_before (300) + FD_PACK_NONCE_SYNTHETIC_LIFETIME */
    txn = fd_pack_insert_txn_init( pack );
    make_nonce_scope_transaction1( txn->txnp, i++, 4U, 0U, 3U, source );
    FD_TEST( fd_pack_insert_txn_fini( pack, txn, 100UL, &deleted )==FD_PACK_INSERT_ACCEPT_NONCE_NONVOTE_ADD );
    FD_TEST( !fd_pack_expire_before( pack, 300UL+FD_PACK_NONCE_SYNTHETIC_LIFETIME ) );
    FD_TEST( fd_pack_expire_before( pack, 301UL+FD_PACK_NONCE_SYNTHETIC_LIFETIME )==1UL );
    FD_TEST( fd_pack_avail_txn_cnt( pack )==2UL );
    /* Not the synthetic nonce lifetime, which would end at 800 */
    FD_TEST( !fd_pack_expire_before( pack, 1000UL ) );
    FD_TEST( fd_pack_expire_before( pack, 1001UL )==2UL );

    fd_txn_e_t * _bundle[ 2 ];
    fd_txn_e_t * const * bundle;
    for( uchar acct_cnt=2U; acct_cnt<4U; acct_cnt++ ) {
      bundle = fd_pack_insert_bundle_init( pack, _bundle, 2UL );
      make_scope_transaction1      ( bundle[ 0 ]->txnp, i++, "A", source );
      make_nonce_scope_transaction1( bundle[ 1 ]->txnp, i++, 0U, 0U, acct_cnt, source );
      FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 2UL, 1000UL, 0, NULL, &deleted )==FD_PACK_INSERT_REJECT_EXPIRED );
      FD_TEST( fd_pack_insert_bundle_reject_txn_idx( pack )==ULONG_MAX );
    }
    bundle = fd_pack_insert_bundle_init( pack, _bundle, 2UL );
    make_scope_transaction1      ( bundle[ 0 ]->txnp, i++, "A", source );
    make_nonce_scope_transaction1( bundle[ 1 ]->txnp, i++, 0U, 5U, 3U, source );
    FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 2UL, 2000UL, 0, NULL, &deleted )==FD_PACK_INSERT_REJECT_INVALID_NONCE );
    FD_TEST( fd_pack_insert_bundle_reject_txn_idx( pack )==1UL );
    FD_TEST( !fd_pack_avail_txn_cnt( pack ) );

    /* A nonce-only bundle expires at the caller's expires_at */
    bundle = fd_pack_insert_bundle_init( pack, _bundle, 2UL );
    make_nonce_scope_transaction1( bundle[ 0 ]->txnp, i++, 4U, 0U, 3U, source );
    make_nonce_scope_transaction1( bundle[ 1 ]->txnp, i++, 5U, 1U, 3U, source );
    FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 2UL, 2000UL, 0, NULL, &deleted )==FD_PACK_INSERT_ACCEPT_NONCE_NONVOTE_ADD );
    FD_TEST( fd_pack_insert_bundle_reject_txn_idx( pack )==ULONG_MAX );
    FD_TEST( fd_pack_avail_txn_cnt( pack )==2UL );
    FD_TEST( !fd_pack_expire_before( pack, 2000UL ) );
    FD_TEST( fd_pack_expire_before( pack, 2001UL )==2UL );
    FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );
  }
}

/* As in jito-solana, BAM leaves durable nonces to the execution bank: no
   AdvanceNonce form is INVALID_NONCE or enters the noncemap, so batches and
   members advancing the same nonce are all admitted (the bank fails the
   later ones), and BAM work never expires by blockhash height. */
static void
test_bam_nonce_exemptions( void ) {
  FD_LOG_NOTICE(( "TEST BAM NONCE EXEMPTIONS" ));
  fd_pack_t * pack = init_all( 128UL, 1UL, 128UL, &outcome );
  FD_TEST( !fd_pack_expire_before( pack, 300UL ) );

  ulong const bam = FD_TXN_M_TPU_SOURCE_BAM;
  ulong i = 90000UL;
  ulong deleted;
  fd_txn_e_t * _bundle[ 3 ];
  fd_txn_e_t * const * bundle;

  /* Short forms, an unsigned authority, then the same signed nonce twice */
  uchar const singles[ 6 ][ 3 ] = { {0,5,1}, {0,5,2}, {0,5,3}, {4,5,3}, {4,0,3}, {4,0,3} };
  for( ulong k=0UL; k<6UL; k++ ) {
    bundle = fd_pack_insert_bundle_init( pack, _bundle, 1UL );
    make_nonce_scope_transaction1( bundle[ 0 ]->txnp, i++, singles[k][0], singles[k][1], singles[k][2], bam );
    bundle[ 0 ]->bam = (fd_txn_bam_t){ .seq_id=(uint)k };
    FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 1UL, 100UL, FD_PACK_IB_TYPE_NONE, NULL, &deleted )==FD_PACK_INSERT_ACCEPT_NONVOTE_ADD );
    FD_TEST( !deleted && fd_pack_insert_bundle_reject_txn_idx( pack )==ULONG_MAX );
    FD_TEST( !(bundle[ 0 ]->txnp->flags & FD_TXN_P_FLAGS_DURABLE_NONCE) );
  }

  /* [A(N),X,B(N)] reaches the bank, which fails B */
  bundle = fd_pack_insert_bundle_init( pack, _bundle, 3UL );
  make_nonce_scope_transaction1( bundle[ 0 ]->txnp, i++, 4U, 0U, 3U, bam );
  make_scope_transaction1      ( bundle[ 1 ]->txnp, i++, "A", bam );
  make_nonce_scope_transaction1( bundle[ 2 ]->txnp, i++, 4U, 0U, 3U, bam );
  for( ulong j=0UL; j<3UL; j++ ) {
    bundle[ j ]->bam = (fd_txn_bam_t){ .seq_id=6U, .batch_idx=(uchar)j, .revert_on_error=1 };
  }
  FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 3UL, 100UL, FD_PACK_IB_TYPE_NONE, NULL, &deleted )==FD_PACK_INSERT_ACCEPT_NONVOTE_ADD );
  FD_TEST( !deleted && fd_pack_insert_bundle_reject_txn_idx( pack )==ULONG_MAX );

  /* A BAM initializer bundle is BAM work whatever its members' source */
  bundle = fd_pack_insert_bundle_init( pack, _bundle, 1UL );
  make_nonce_scope_transaction1( bundle[ 0 ]->txnp, i++, 0U, 5U, 3U, FD_TXN_M_TPU_SOURCE_UDP );
  FD_TEST( fd_pack_insert_bundle_fini( pack, bundle, 1UL, 100UL, FD_PACK_IB_TYPE_BAM, NULL, &deleted )==FD_PACK_INSERT_ACCEPT_NONVOTE_ADD );
  FD_TEST( fd_pack_insert_bundle_reject_txn_idx( pack )==ULONG_MAX );

  FD_TEST( fd_pack_avail_txn_cnt( pack )==10UL );
  FD_TEST( !fd_pack_expire_before( pack, ULONG_MAX ) );
  FD_TEST( fd_pack_avail_txn_cnt( pack )==10UL );
  FD_TEST( !fd_pack_verify( pack, pack_verify_scratch ) );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );
  rng = fd_rng_join( fd_rng_new( _rng, 0U, 0UL ) );
  fd_metrics_register( (ulong *)fd_metrics_new( metrics_scratch, 0UL ) );

  extra_verify = fd_env_strip_cmdline_contains( &argc, &argv, "--extra-verify" );

  test_reserved_bundle_permissions();
  test_reserved_fee_payer_and_program_locks();
  test_duplicate_sig_bam_bundle_delete();
  test_bam_nonrevert_seq_conflict_order();
  test_bam_nonrevert_clears_bundle_flag();
  test_bam_only_schedule_filters_non_bam_work();
  test_initializer_bundle_mode_selection();
  test_bam_initializer_strict();
  test_bam_secondary_account_locks();
  test_bam_secondary_fee_payer_and_nonce_locks();
  test_bam_secondary_fifo_head();
  test_bam_secondary_capacity_deferral();
  test_bam_conflict_lookahead();
  test_bam_lookahead_permissions();
  test_bundle_ordinal_rebase();
  test_bundle_ordinal_rebase_group_boundaries();
  test_bundle_ordinal_rebase_initializer_replacement();
  test_deferred_initializer_suppresses_meta();
  test_deferred_slot_rollover();
  test_deferred_bundle_skip_count();
  test_bam_candidate_hint_lifetime();
  test_bam_candidate_initializer_states();

  /* Generic bundle/initializer-pack regressions */
  test_bundle_account_conflicts();
  test_initializer_bundle_state_machine();
  test_bundle_metadata_persistence();
  test_rejected_bundle_replaces_singleton_nonce();
  test_bundle_reject_txn_idx();
  test_non_bam_nonce_matches_upstream();
  test_bam_nonce_exemptions();


  fd_rng_delete( fd_rng_leave( rng ) );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
