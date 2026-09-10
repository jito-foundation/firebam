/* Opt-in local benchmark.  Reuse the initialized runtime fixtures and real
   execle callbacks without duplicating their account/bank setup.  Their unit
   test registrations are harmless here; this main runs only the benchmark. */
#define main test_execle_fixture_main
#include "test_execle_tile.c"
#undef main

#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <time.h>

#define BENCH_TXNS (16UL)
#define BENCH_MAX_REPETITIONS (32UL)

typedef struct {
  test_env_t * env;
  test_bam_worker_output_t output;
  fd_txn_e_t txn[1];
  ulong txn_index;
  ulong cpu;
  ulong execution_begin;
  ulong execution_end;
  atomic_ulong requested;
  atomic_ulong completed;
  atomic_int ready;
  atomic_int stop;
  uchar metrics[ FD_METRICS_FOOTPRINT(0UL) ] __attribute__((aligned(FD_METRICS_ALIGN)));
} bench_worker_t;

typedef struct {
  ulong latency_ns[BENCH_TXNS];
  ulong elapsed_ns;
  ulong busy_ns[2];
  ulong dispatched[2];
  ulong overlap_ns;
  uchar input_hash[32];
} bench_result_t;

static ulong
bench_now( void ) {
  struct timespec ts;
  FD_TEST( !clock_gettime( CLOCK_MONOTONIC, &ts ) );
  return (ulong)ts.tv_sec*1000000000UL+(ulong)ts.tv_nsec;
}

static void
bench_pin( ulong cpu ) {
  FD_TEST( cpu<CPU_SETSIZE );
  cpu_set_t set;
  CPU_ZERO( &set );
  CPU_SET( cpu, &set );
  FD_TEST( !pthread_setaffinity_np( pthread_self(), sizeof(set), &set ) );
  FD_TEST( (ulong)sched_getcpu()==cpu );
}

static void *
bench_worker_main( void * arg ) {
  bench_worker_t * worker = arg;
  bench_pin( worker->cpu );
  fd_log_private_thread_id_set( worker->env->execle->kind_id+1UL );
  fd_log_thread_set( worker->env->execle->kind_id ? "bam-bench-1" : "bam-bench-0" );
  fd_log_private_cpu_id_set( worker->cpu );
  fd_metrics_register( (ulong *)fd_metrics_new( worker->metrics, 0UL ) );
  ulong seen = 0UL;
  atomic_store_explicit( &worker->ready, 1, memory_order_release );
  while( !atomic_load_explicit( &worker->stop, memory_order_acquire ) ) {
    ulong requested = atomic_load_explicit( &worker->requested, memory_order_acquire );
    if( requested==seen ) { FD_SPIN_PAUSE(); continue; }
    worker->execution_begin = bench_now();
    test_bam_execute_pack_output( worker->env, &worker->output, worker->txn, 1UL,
                                   (uint)worker->txn_index, worker->txn_index );
    worker->execution_end = bench_now();
    seen = requested;
    atomic_store_explicit( &worker->completed, seen, memory_order_release );
  }
  return NULL;
}

/* Both variants use the same two threads and identical signed inputs.  Only
   secondary BAM eligibility changes.  The coordinator is the sole owner of
   Pack and PoH; a worker's job and credit arrays change only while it is idle.
   Release/acquire handshakes publish all callback output before it is read. */
static void
bench_run( int             dual,
            int             hot,
            ulong           instruction_cnt,
            int             atomic,
            ulong const     worker_cpus[2],
            bench_result_t * result ) {
  fd_memset( result, 0, sizeof(*result) );
  test_env_t * env[2] = { test_env_create(), NULL };
  env[1] = test_env_create_worker( env[0] );
  fd_bank_t * bank = fd_svm_mini_bank( mini, env[0]->bank_idx );
  FD_TEST( bank->f.slot==2UL && env[0]->execle->banks==env[1]->execle->banks );
  ((fd_blockhash_info_t *)fd_blockhashes_peek_last( &bank->f.block_hash_queue ))->lamports_per_signature = 5000UL;
  fd_pubkey_t payers[BENCH_TXNS], recipients[BENCH_TXNS];
  uchar private_keys[BENCH_TXNS][32];
  fd_txn_p_t forwarded[BENCH_TXNS];
  fd_sha256_t input_sha[1];
  fd_sha256_init( input_sha );
  FD_TEST( instruction_cnt<=32UL );
  for( ulong i=0UL; i<BENCH_TXNS; i++ ) {
    fd_memset( private_keys[i], (int)(i+1UL), 32UL );
    fd_sha512_t sha[1];
    FD_TEST( fd_ed25519_public_from_private( payers[i].uc, private_keys[i], fd_sha512_join( fd_sha512_new( sha ) ) ) );
    recipients[i] = (fd_pubkey_t){ .ul={ 0xCA0000UL+i } };
    if( !hot || !i ) test_fund_account( env[0], &payers[i], 1000000000UL );
    test_fund_account( env[0], &recipients[i], 1000000UL );
  }
  for( ulong i=0UL; i<BENCH_TXNS; i++ ) {
    fd_pubkey_t to[32];
    ulong lamports[32];
    for( ulong j=0UL; j<instruction_cnt; j++ ) { to[j]=recipients[i]; lamports[j]=1000UL+i; }
    test_build_system_transfer_txns( &forwarded[i], bank, payers[hot ? 0UL : i], to, lamports, instruction_cnt );
    test_bam_pair_sign( &forwarded[i], &payers[hot ? 0UL : i], private_keys[hot ? 0UL : i] );
    test_mark_bam_batch( &forwarded[i], 1UL, (uint)(1000UL+i), atomic );
    forwarded[i].bam.scheduler_gen = 7U;
    fd_sha256_append( input_sha, forwarded[i].payload, forwarded[i].payload_sz );
  }
  fd_sha256_fini( input_sha, result->input_hash );

  fd_pack_limits_t limits = { .max_cost_per_block=48000000UL, .max_vote_cost_per_block=36000000UL,
                             .max_write_cost_per_acct=12000000UL, .max_data_bytes_per_block=5UL<<20,
                             .max_txn_per_microblock=8UL, .max_microblocks_per_block=32UL,
                             .max_allocated_data_per_block=FD_PACK_MAX_ALLOCATED_DATA_PER_BLOCK };
  ulong pack_size = fd_pack_footprint( 64UL, 48UL, 2UL, &limits );
  void * mem = fd_wksp_alloc_laddr( mini->wksp, fd_pack_align(), pack_size, TOPO_TAG );
  FD_TEST( mem );
  fd_rng_t rng[1];
  FD_TEST( fd_rng_join( fd_rng_new( rng, 0UL, 0UL ) ) );
  fd_pack_t * pack = fd_pack_join( fd_pack_new( mem, 64UL, 48UL, 2UL, &limits, NULL, 0UL, rng ) );
  FD_TEST( pack );
  fd_pack_set_initializer_bundles_ready( pack );
  test_bam_poh_fixture_t * poh = test_bam_poh_fixture_new( mini->wksp, 2UL, 0U );
  bench_worker_t * workers = fd_wksp_alloc_laddr( mini->wksp, alignof(bench_worker_t), 2UL*sizeof(bench_worker_t), TOPO_TAG );
  FD_TEST( workers );
  pthread_t threads[2];
  for( ulong i=0UL; i<2UL; i++ ) {
    fd_memset( &workers[i], 0, sizeof(workers[i]) );
    workers[i].env = env[i];
    workers[i].cpu = worker_cpus[i];
    atomic_init( &workers[i].requested, 0UL );
    atomic_init( &workers[i].completed, 0UL );
    atomic_init( &workers[i].ready, 0 );
    atomic_init( &workers[i].stop, 0 );
    test_bam_worker_output_init( &workers[i].output, i );
    FD_TEST( !pthread_create( &threads[i], NULL, bench_worker_main, &workers[i] ) );
  }
  ulong timeout = bench_now()+2000000000UL;
  while( !atomic_load_explicit( &workers[0].ready, memory_order_acquire ) ||
         !atomic_load_explicit( &workers[1].ready, memory_order_acquire ) ) {
    FD_TEST( bench_now()<timeout );
    FD_SPIN_PAUSE();
  }

  ulong admitted[BENCH_TXNS], execution_begin[BENCH_TXNS], execution_end[BENCH_TXNS], worker_for[BENCH_TXNS];
  for( ulong i=0UL; i<BENCH_TXNS; i++ ) {
    fd_txn_e_t * txns[1];
    fd_pack_insert_bundle_init( pack, txns, 1UL );
    fd_memset( txns[0], 0, sizeof(fd_txn_e_t) );
    *txns[0]->txnp = forwarded[i];
    ulong deleted;
    FD_TEST( fd_pack_insert_bundle_fini( pack, txns, 1UL, 2UL, FD_PACK_IB_TYPE_NONE, NULL, &deleted, NULL )>=0 );
    admitted[i] = bench_now();
    FD_TEST( !deleted );
  }
  timeout = admitted[0]+2000000000UL;
  ulong issued[2] = {0}, acknowledged[2] = {0}, published[2] = {0}, consumed[2] = {0}, rebates_consumed[2] = {0};
  ulong idle = 3UL;
  ulong next = 0UL;
  ulong accepted = 0UL;
  while( accepted<BENCH_TXNS ) {
    FD_TEST( bench_now()<timeout );
    for( ulong i=0UL; i<2UL; i++ ) {
      bench_worker_t * worker = &workers[i];
      if( issued[i]!=acknowledged[i] && atomic_load_explicit( &worker->completed, memory_order_acquire )==issued[i] ) {
        FD_TEST( fd_fseq_query( worker->env->execle->busy_fseq )==0UL );
        FD_TEST( fd_pack_microblock_complete( pack, i )==1 );
        while( rebates_consumed[i]<worker->output.seqs[1] ) {
          fd_frag_meta_t const * rebate_meta = worker->output.mcaches[1]+
              fd_mcache_line_idx( rebates_consumed[i]++, worker->output.depths[1] );
          FD_TEST( rebate_meta->sig==2UL && rebate_meta->sz>=FD_PACK_REBATE_MIN_SZ );
          fd_pack_rebate_cus( pack, fd_chunk_to_laddr_const( worker->env->execle->out_pack->mem, rebate_meta->chunk ) );
        }
        ulong txn_index = worker->txn_index;
        execution_begin[txn_index] = worker->execution_begin;
        execution_end[txn_index] = worker->execution_end;
        worker_for[txn_index] = i;
        result->busy_ns[i] += worker->execution_end-worker->execution_begin;
        published[i] = worker->output.seqs[0];
        FD_TEST( !worker->output.seqs[2] ); /* no immediate execution failures */
        acknowledged[i] = issued[i];
        idle |= 1UL<<i;
      }
      if( consumed[i]<published[i] ) {
        fd_frag_meta_t const * m = worker->output.mcaches[0]+fd_mcache_line_idx( consumed[i], worker->output.depths[0] );
        FD_TEST( fd_frag_meta_seq_query( m )==consumed[i] );
        ulong previous_results = test_bam_poh_fixture_summary( poh )->result_cnt;
        int held = test_bam_poh_fixture_consume( poh, i, m->sig,
                     fd_chunk_to_laddr_const( worker->env->execle->out_poh->mem, m->chunk ), m->sz );
        if( !held ) {
          consumed[i]++;
          test_bam_poh_summary_t const * summary = test_bam_poh_fixture_summary( poh );
          FD_TEST( summary->result_cnt==previous_results+1UL );
          fd_bam_bundle_result_t const * terminal = &summary->results[previous_results];
          FD_TEST( terminal->seq_id==1000UL+accepted && terminal->scheduler_gen==7U && terminal->slot==2UL );
          FD_TEST( terminal->execution_success && !terminal->transaction_err_count );
          result->latency_ns[accepted] = bench_now()-admitted[accepted];
          accepted++;
        }
      }
    }
    if( next<BENCH_TXNS && idle ) {
      ulong i = (ulong)fd_ulong_find_lsb( idle );
      bench_worker_t * worker = &workers[i];
      if( published[i]-consumed[i]>=worker->output.depths[0] ) continue;
      int flags = FD_PACK_SCHEDULE_VOTE | FD_PACK_SCHEDULE_BAM_ONLY;
      if( !i ) flags |= FD_PACK_SCHEDULE_BUNDLE;
      else if( dual ) flags |= FD_PACK_SCHEDULE_BAM_SINGLE;
      ulong hint;
      FD_TEST( fd_pack_peek_bundle_candidate( pack, 1, &hint ) );
      ulong count = fd_pack_schedule_next_microblock_with_bundle_hint( pack, 1500000UL, 0.75f, i,
                                        flags | FD_PACK_SCHEDULE_BAM_READY, hint, worker->txn );
      if( count ) {
        FD_TEST( count==1UL && worker->txn[0].txnp->bam.seq_id==1000UL+next );
        worker->txn_index = next++;
        /* Only idle workers' credit arrays can be changed by this thread. */
        worker->output.credits[0] = worker->output.depths[0]-(published[i]-consumed[i]);
        worker->output.credits[1] = worker->output.depths[1];
        worker->output.credits[2] = worker->output.depths[2];
        worker->output.min_credit = worker->output.credits[0];
        result->dispatched[i]++;
        idle &= ~(1UL<<i);
        atomic_store_explicit( &worker->requested, ++issued[i], memory_order_release );
      }
    }
  }
  result->elapsed_ns = bench_now()-admitted[0];
  for( ulong i=0UL; i<2UL; i++ ) atomic_store_explicit( &workers[i].stop, 1, memory_order_release );
  for( ulong i=0UL; i<2UL; i++ ) FD_TEST( !pthread_join( threads[i], NULL ) );
  FD_TEST( idle==3UL && next==BENCH_TXNS && !fd_pack_avail_txn_cnt( pack ) );
  test_bam_poh_summary_t const * summary = test_bam_poh_fixture_summary( poh );
  FD_TEST( summary->txn_cnt==BENCH_TXNS && summary->result_cnt==BENCH_TXNS );
  for( ulong i=0UL; i<BENCH_TXNS; i++ ) {
    FD_TEST( summary->txns[i].payload_sz==forwarded[i].payload_sz );
    FD_TEST( !memcmp( summary->txns[i].payload, forwarded[i].payload, forwarded[i].payload_sz ) );
    ulong transfer = instruction_cnt*(1000UL+i);
    FD_TEST( test_read_lamports( env[0], &recipients[i] )==1000000UL+transfer );
    if( !hot ) FD_TEST( test_read_lamports( env[0], &payers[i] )==1000000000UL-5000UL-transfer );
    for( ulong j=i+1UL; j<BENCH_TXNS; j++ ) {
      if( worker_for[i]==worker_for[j] ) continue;
      ulong start = fd_ulong_max( execution_begin[i], execution_begin[j] );
      ulong end = fd_ulong_min( execution_end[i], execution_end[j] );
      if( end>start ) result->overlap_ns += end-start;
    }
  }
  if( hot ) FD_TEST( test_read_lamports( env[0], &payers[0] )==1000000000UL-5000UL*BENCH_TXNS-
                              instruction_cnt*(1000UL*BENCH_TXNS+(BENCH_TXNS-1UL)*BENCH_TXNS/2UL) );
  if( !dual || hot ) FD_TEST( !result->overlap_ns );
  fd_pack_delete( fd_pack_leave( pack ) );
  fd_rng_delete( fd_rng_leave( rng ) );
  test_env_destroy( env[0] );
}

static ulong
bench_quantile( ulong * values,
                 ulong   count,
                 ulong   percent ) {
  for( ulong i=1UL; i<count; i++ ) {
    ulong value = values[i], j=i;
    while( j && value<values[j-1UL] ) { values[j]=values[j-1UL]; j--; }
    values[j]=value;
  }
  return values[(count-1UL)*percent/100UL];
}

int
main( int argc, char ** argv ) {
  fd_svm_mini_limits_t limits[1];
  fd_svm_mini_limits_default( limits );
  limits->max_live_slots = MAX_LIVE_SLOTS;
  limits->max_txn_per_slot = MAX_TXN_PER_SLOT;
  limits->max_txn_write_locks = MAX_TX_ACCOUNT_LOCKS;
  limits->wksp_addl_sz = 5UL<<30;
  limits->accdb_joiner_cnt = 3UL;
  mini = fd_svm_test_boot( &argc, &argv, limits );
  fd_metrics_register( (ulong *)fd_metrics_new( metrics_scratch, 0UL ) );
  ulong repetitions = fd_env_strip_cmdline_ulong( &argc, &argv, "--repetitions", NULL, 9UL );
  ulong coordinator_cpu = fd_env_strip_cmdline_ulong( &argc, &argv, "--coordinator-cpu", NULL, 14UL );
  ulong worker_cpus[2] = { fd_env_strip_cmdline_ulong( &argc, &argv, "--worker-cpu0", NULL, 10UL ),
                           fd_env_strip_cmdline_ulong( &argc, &argv, "--worker-cpu1", NULL, 12UL ) };
  FD_TEST( repetitions && repetitions<=BENCH_MAX_REPETITIONS );
  FD_TEST( worker_cpus[0]!=worker_cpus[1] && worker_cpus[0]!=coordinator_cpu && worker_cpus[1]!=coordinator_cpu );
  bench_pin( coordinator_cpu );
  FD_LOG_NOTICE(( "BAM_LATENCY_BENCH endpoint=pack_admission_to_local_poh_acceptance clock=CLOCK_MONOTONIC unit=ns rooted=0 network=0 burst=%lu repetitions=%lu coordinator_cpu=%lu worker_cpus=%lu,%lu",
                   BENCH_TXNS, repetitions, coordinator_cpu, worker_cpus[0], worker_cpus[1] ));
  for( int atomic=0; atomic<2; atomic++ ) {
    for( int workload=0; workload<3; workload++ ) {
      ulong instructions = workload ? 24UL : 1UL;
      int hot = workload==2;
      ulong latencies[2][BENCH_MAX_REPETITIONS*BENCH_TXNS], elapsed[2][BENCH_MAX_REPETITIONS];
      ulong overlap[2] = {0}, dispatched[2][2] = {{0}}, busy[2][2] = {{0}};
      for( ulong iteration=0UL; iteration<repetitions+1UL; iteration++ ) {
        bench_result_t pair[2];
        int first = (int)(iteration & 1UL);
        bench_run( first, hot, instructions, atomic, worker_cpus, &pair[first] );
        bench_run( !first, hot, instructions, atomic, worker_cpus, &pair[!first] );
        FD_TEST( !memcmp( pair[0].input_hash, pair[1].input_hash, sizeof(pair[0].input_hash) ) );
        if( !iteration ) continue; /* warm each setup outside reported samples */
        for( ulong mode=0UL; mode<2UL; mode++ ) {
          fd_memcpy( &latencies[mode][(iteration-1UL)*BENCH_TXNS], pair[mode].latency_ns, sizeof(pair[mode].latency_ns) );
          elapsed[mode][iteration-1UL] = pair[mode].elapsed_ns;
          overlap[mode] += pair[mode].overlap_ns;
          for( ulong worker=0UL; worker<2UL; worker++ ) {
            dispatched[mode][worker] += pair[mode].dispatched[worker];
            busy[mode][worker] += pair[mode].busy_ns[worker];
          }
        }
      }
      for( ulong mode=0UL; mode<2UL; mode++ ) {
        ulong count = repetitions*BENCH_TXNS;
        FD_LOG_NOTICE(( "BAM_LATENCY_BENCH workload=%s instructions=%lu atomic=%i mode=%s accepted=%lu omitted=0 terminal=%lu p50_ns=%lu p95_ns=%lu p99_ns=%lu median_burst_ns=%lu overlap_ns=%lu worker0_dispatch=%lu worker1_dispatch=%lu worker0_busy_ns=%lu worker1_busy_ns=%lu",
                         hot ? "shared_writer" : "independent", instructions, atomic, mode ? "primary_plus_single" : "primary_only",
                         count, count, bench_quantile( latencies[mode], count, 50UL ), bench_quantile( latencies[mode], count, 95UL ),
                         bench_quantile( latencies[mode], count, 99UL ), bench_quantile( elapsed[mode], repetitions, 50UL ),
                         overlap[mode], dispatched[mode][0], dispatched[mode][1], busy[mode][0], busy[mode][1] ));
      }
      if( !hot ) FD_TEST( overlap[1] && dispatched[1][1] );
    }
  }
  fd_svm_test_halt( mini );
  FD_LOG_NOTICE(( "pass" ));
  return 0;
}
