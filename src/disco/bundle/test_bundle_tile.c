#define FD_TILE_TEST
#include "fd_bundle_tile.c"
#include "test_bundle_common.c"
#include <stdlib.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include "../bam/generated/fd_bam_tile_seccomp.h"
#include "../events/generated/fd_event_tile_seccomp.h"

static long g_bundle_wall = 1L;
static long g_bundle_mono = 1L;

long
fd_bundle_now( fd_bundle_tile_t const * ctx ) {
  (void)ctx;
  return g_bundle_wall;
}

/* Independent elapsed clock, so rollback cannot renew a partial block. */
long
fd_grpc_client_mono_now( void ) {
  return g_bundle_mono;
}

/* ---- minimal helpers ------------------------------------------------ */

static void *
mock_replay_wksp_new( void ) {
  ulong alloc_sz = fd_ulong_align_up( FD_CHUNK_FOOTPRINT + sizeof(fd_poh_reset_t), FD_CHUNK_ALIGN );
  void * mem = aligned_alloc( FD_CHUNK_ALIGN, alloc_sz );
  FD_TEST( mem );
  memset( mem, 0, alloc_sz );
  return mem;
}

static ulong
mock_replay_write_reset( void * mem,
                         ulong  chunk,
                         ulong  completed_slot,
                         ulong  next_leader_slot ) {
  fd_poh_reset_t * reset = (fd_poh_reset_t *)fd_chunk_to_laddr( mem, chunk );
  memset( reset, 0, sizeof(fd_poh_reset_t) );
  reset->completed_slot   = completed_slot;
  reset->next_leader_slot = next_leader_slot;
  return chunk;
}

static fd_bundle_tile_t test_ctx[1];

static void
inject_replay_reset( fd_bundle_tile_t * ctx,
                     ulong              in_idx,
                     ulong              chunk,
                     ulong              completed_slot,
                     ulong              next_leader_slot ) {
  mock_replay_write_reset( ctx->replay_in.mem, chunk, completed_slot, next_leader_slot );

  during_frag( ctx, in_idx, 0UL, REPLAY_SIG_RESET, chunk, sizeof(fd_poh_reset_t), 0UL );
  after_frag( ctx, in_idx, 0UL, REPLAY_SIG_RESET, sizeof(fd_poh_reset_t), 0UL, 0UL, NULL );
}

/* ---- test: during_frag + after_frag staging/commit ------------------- */

static void
test_replay_frag_ingest( void ) {
  FD_LOG_NOTICE(( "TEST replay frag ingest" ));

  fd_bundle_tile_t * ctx = test_ctx;
  memset( ctx, 0, sizeof(fd_bundle_tile_t) );

  void * wksp = mock_replay_wksp_new();

  ulong const in_idx = 0UL;
  ctx->in_kind[ in_idx ]  = IN_KIND_REPLAY_OUT;
  ctx->replay_in.mem    = wksp;
  ctx->replay_in.chunk0 = 0UL;
  ctx->replay_in.wmark  = 1UL; /* allow chunk 0 and 1 */

  ctx->next_leader_slot = ULONG_MAX;
  ctx->reset_slot       = ULONG_MAX;

  /* Inject a reset: completed_slot=100, next_leader_slot=500 */
  inject_replay_reset( ctx, in_idx, 0UL, 100UL, 500UL );

  FD_TEST( ctx->next_leader_slot==500UL );
  FD_TEST( ctx->reset_slot==100UL );

  /* A non-reset signal should be ignored */
  ulong prev_next = ctx->next_leader_slot;
  ulong prev_rst  = ctx->reset_slot;
  during_frag( ctx, in_idx, 0UL, REPLAY_SIG_RESET+1, 0UL, sizeof(fd_poh_reset_t), 0UL );
  after_frag( ctx, in_idx, 0UL, REPLAY_SIG_RESET+1, sizeof(fd_poh_reset_t), 0UL, 0UL, NULL );
  FD_TEST( ctx->next_leader_slot==prev_next );
  FD_TEST( ctx->reset_slot==prev_rst );

  /* A different in_idx (not replay) should be ignored */
  ctx->in_kind[ 1 ] = 0;
  during_frag( ctx, 1UL, 0UL, REPLAY_SIG_RESET, 0UL, sizeof(fd_poh_reset_t), 0UL );
  after_frag( ctx, 1UL, 0UL, REPLAY_SIG_RESET, sizeof(fd_poh_reset_t), 0UL, 0UL, NULL );
  FD_TEST( ctx->next_leader_slot==prev_next );
  FD_TEST( ctx->reset_slot==prev_rst );

  free( wksp );
}

/* ---- test: maybe_sleep hysteresis ------------------------------------ */

static void
test_maybe_sleep_no_replay( void ) {
  FD_LOG_NOTICE(( "TEST maybe_sleep returns early without replay_in" ));

  fd_bundle_tile_t * ctx = test_ctx;
  memset( ctx, 0, sizeof(fd_bundle_tile_t) );
  ctx->replay_in.mem = NULL;
  ctx->sleep_mode    = 0;

  /* Should be a no-op when replay_in.mem is NULL */
  fd_bundle_tile_maybe_sleep( ctx, 0 );
  FD_TEST( ctx->sleep_mode==0 );
}

static void
test_maybe_sleep_unknown_schedule( void ) {
  FD_LOG_NOTICE(( "TEST maybe_sleep sleeps when leader schedule unknown" ));

  fd_bundle_tile_t * ctx = test_ctx;
  memset( ctx, 0, sizeof(fd_bundle_tile_t) );

  void * wksp = mock_replay_wksp_new();
  ctx->replay_in.mem = wksp;
  ctx->sleep_mode    = 0;
  ctx->sleep_check_ns = 0;

  /* next_leader_slot unknown → should enter sleep */
  ctx->next_leader_slot = ULONG_MAX;
  ctx->reset_slot       = 100UL;
  fd_bundle_tile_maybe_sleep( ctx, 1 );
  FD_TEST( ctx->sleep_mode==1 );

  /* reset_slot unknown → should stay asleep */
  ctx->sleep_mode = 0;
  ctx->sleep_check_ns = 0;
  ctx->next_leader_slot = 100UL;
  ctx->reset_slot       = ULONG_MAX;
  fd_bundle_tile_maybe_sleep( ctx, 1 );
  FD_TEST( ctx->sleep_mode==1 );

  /* Both unknown → should stay asleep */
  ctx->sleep_mode = 0;
  ctx->sleep_check_ns = 0;
  ctx->next_leader_slot = ULONG_MAX;
  ctx->reset_slot       = ULONG_MAX;
  fd_bundle_tile_maybe_sleep( ctx, 1 );
  FD_TEST( ctx->sleep_mode==1 );

  free( wksp );
}

static void
test_maybe_sleep_far_leader( void ) {
  FD_LOG_NOTICE(( "TEST maybe_sleep enters sleep when leader is far" ));

  fd_bundle_tile_t * ctx = test_ctx;
  memset( ctx, 0, sizeof(fd_bundle_tile_t) );

  void * wksp = mock_replay_wksp_new();
  ctx->replay_in.mem    = wksp;
  ctx->sleep_mode       = 0;
  ctx->sleep_check_ns   = 0;

  /* Leader 500 slots away (>450 threshold) → enter sleep */
  ctx->reset_slot       = 100UL;
  ctx->next_leader_slot = 600UL;
  fd_bundle_tile_maybe_sleep( ctx, 1 );
  FD_TEST( ctx->sleep_mode==1 );

  free( wksp );
}

static void
test_maybe_sleep_close_leader( void ) {
  FD_LOG_NOTICE(( "TEST maybe_sleep stays awake when leader is close" ));

  fd_bundle_tile_t * ctx = test_ctx;
  memset( ctx, 0, sizeof(fd_bundle_tile_t) );

  void * wksp = mock_replay_wksp_new();
  ctx->replay_in.mem    = wksp;
  ctx->sleep_mode       = 0;
  ctx->sleep_check_ns   = 0;

  /* Leader 300 slots away (<450 threshold) → stay awake */
  ctx->reset_slot       = 100UL;
  ctx->next_leader_slot = 400UL;
  fd_bundle_tile_maybe_sleep( ctx, 1 );
  FD_TEST( ctx->sleep_mode==0 );

  free( wksp );
}

static void
test_maybe_sleep_hysteresis( void ) {
  FD_LOG_NOTICE(( "TEST maybe_sleep hysteresis between thresholds" ));

  fd_bundle_tile_t * ctx = test_ctx;
  memset( ctx, 0, sizeof(fd_bundle_tile_t) );

  void * wksp = mock_replay_wksp_new();
  ctx->replay_in.mem    = wksp;
  ctx->sleep_mode       = 0;
  ctx->sleep_check_ns   = 0;

  /* Start awake, leader is 425 slots away.
     425 < 450 (sleep threshold) → should stay awake */
  ctx->reset_slot       = 100UL;
  ctx->next_leader_slot = 525UL;
  fd_bundle_tile_maybe_sleep( ctx, 1 );
  FD_TEST( ctx->sleep_mode==0 );

  /* Now push leader to 451 slots away (>450) → enter sleep */
  ctx->sleep_check_ns = 0;
  ctx->next_leader_slot = 551UL;
  fd_bundle_tile_maybe_sleep( ctx, 2 );
  FD_TEST( ctx->sleep_mode==1 );

  /* While sleeping, leader moves to 410 slots away.
     410 > 400 (wake threshold) → stay asleep (hysteresis) */
  ctx->sleep_check_ns = 0;
  ctx->next_leader_slot = 510UL;
  fd_bundle_tile_maybe_sleep( ctx, 3 );
  FD_TEST( ctx->sleep_mode==1 );

  /* Leader moves to 400 slots (<=400 wake threshold) → wake up */
  ctx->sleep_check_ns = 0;
  ctx->next_leader_slot = 500UL;
  fd_bundle_tile_maybe_sleep( ctx, 4 );
  FD_TEST( ctx->sleep_mode==0 );

  /* Now leader is exactly at 450 slots → stay awake (need >450) */
  ctx->sleep_check_ns = 0;
  ctx->next_leader_slot = 550UL;
  fd_bundle_tile_maybe_sleep( ctx, 5 );
  FD_TEST( ctx->sleep_mode==0 );

  /* Leader at 451 → sleep */
  ctx->sleep_check_ns = 0;
  ctx->next_leader_slot = 551UL;
  fd_bundle_tile_maybe_sleep( ctx, 6 );
  FD_TEST( ctx->sleep_mode==1 );

  free( wksp );
}

static void
test_maybe_sleep_check_interval( void ) {
  FD_LOG_NOTICE(( "TEST maybe_sleep respects check interval" ));

  fd_bundle_tile_t * ctx = test_ctx;
  memset( ctx, 0, sizeof(fd_bundle_tile_t) );

  void * wksp = mock_replay_wksp_new();
  ctx->replay_in.mem    = wksp;
  ctx->sleep_mode       = 0;
  ctx->sleep_check_ns   = 0;

  /* Leader far away → should sleep on first check */
  ctx->reset_slot       = 0UL;
  ctx->next_leader_slot = 1000UL;
  fd_bundle_tile_maybe_sleep( ctx, 1 );
  FD_TEST( ctx->sleep_mode==1 );

  /* sleep_check_ns should now be 1 + 5e9 */
  long expected_next = 1 + FD_BUNDLE_SLEEP_CHECK_INTERVAL_NS;
  FD_TEST( ctx->sleep_check_ns==expected_next );

  /* Calling again before interval elapses should be a no-op. */
  ctx->next_leader_slot = 10UL;
  fd_bundle_tile_maybe_sleep( ctx, 2 );
  FD_TEST( ctx->sleep_mode==1 ); /* unchanged, interval not reached */

  /* Advance past interval → check fires, leader close → wake */
  fd_bundle_tile_maybe_sleep( ctx, expected_next + 1 );
  FD_TEST( ctx->sleep_mode==0 );

  free( wksp );
}

static void
test_replay_triggers_sleep_transition( void ) {
  FD_LOG_NOTICE(( "TEST end-to-end: replay reset messages drive sleep" ));

  fd_bundle_tile_t * ctx = test_ctx;
  memset( ctx, 0, sizeof(fd_bundle_tile_t) );

  void * wksp = mock_replay_wksp_new();

  ulong const in_idx = 0UL;
  ctx->in_kind[ in_idx ] = IN_KIND_REPLAY_OUT;
  ctx->replay_in.mem     = wksp;
  ctx->replay_in.chunk0  = 0UL;
  ctx->replay_in.wmark   = 1UL;

  /* Start asleep (mimicking has_replay_in initial state) */
  ctx->next_leader_slot  = ULONG_MAX;
  ctx->reset_slot        = ULONG_MAX;
  ctx->sleep_mode        = 1;
  ctx->sleep_check_ns    = 0;

  /* 1. Replay says: completed_slot=0, next_leader_slot=1000.
        Leader is 1000 slots away (>450) → stay asleep */
  inject_replay_reset( ctx, in_idx, 0UL, 0UL, 1000UL );
  fd_bundle_tile_maybe_sleep( ctx, 1 );
  FD_TEST( ctx->sleep_mode==1 );

  /* 2. Replay says: completed_slot=700, next_leader_slot=1000.
        Leader is 300 slots away (<=400) → wake up */
  inject_replay_reset( ctx, in_idx, 0UL, 700UL, 1000UL );
  ctx->sleep_check_ns = 0;
  fd_bundle_tile_maybe_sleep( ctx, 2 );
  FD_TEST( ctx->sleep_mode==0 );

  /* 3. Replay says: completed_slot=1004, next_leader_slot=2000.
        Leader is 996 slots away (>450) → enter sleep again */
  inject_replay_reset( ctx, in_idx, 0UL, 1004UL, 2000UL );
  ctx->sleep_check_ns = 0;
  fd_bundle_tile_maybe_sleep( ctx, 3 );
  FD_TEST( ctx->sleep_mode==1 );

  /* 4. Replay says: no upcoming leader (ULONG_MAX).
        Should stay asleep. */
  inject_replay_reset( ctx, in_idx, 0UL, 2000UL, ULONG_MAX );
  ctx->sleep_check_ns = 0;
  fd_bundle_tile_maybe_sleep( ctx, 4 );
  FD_TEST( ctx->sleep_mode==1 );

  free( wksp );
}

static void
test_boundary_thresholds( void ) {
  FD_LOG_NOTICE(( "TEST boundary threshold values" ));

  fd_bundle_tile_t * ctx = test_ctx;
  memset( ctx, 0, sizeof(fd_bundle_tile_t) );

  void * wksp = mock_replay_wksp_new();
  ctx->replay_in.mem = wksp;

  /* Exactly at sleep threshold (450): awake should stay awake */
  ctx->sleep_mode       = 0;
  ctx->sleep_check_ns   = 0;
  ctx->reset_slot       = 0;
  ctx->next_leader_slot = FD_BUNDLE_SLEEP_THRESHOLD_SLOTS;
  fd_bundle_tile_maybe_sleep( ctx, 1 );
  FD_TEST( ctx->sleep_mode==0 );

  /* One above sleep threshold (451): awake → sleep */
  ctx->sleep_mode       = 0;
  ctx->sleep_check_ns   = 0;
  ctx->next_leader_slot = FD_BUNDLE_SLEEP_THRESHOLD_SLOTS + 1;
  fd_bundle_tile_maybe_sleep( ctx, 2 );
  FD_TEST( ctx->sleep_mode==1 );

  /* Exactly at wake threshold (400): asleep → wake */
  ctx->sleep_mode       = 1;
  ctx->sleep_check_ns   = 0;
  ctx->next_leader_slot = FD_BUNDLE_WAKE_THRESHOLD_SLOTS;
  fd_bundle_tile_maybe_sleep( ctx, 3 );
  FD_TEST( ctx->sleep_mode==0 );

  /* One above wake threshold (401): asleep → stay asleep */
  ctx->sleep_mode       = 1;
  ctx->sleep_check_ns   = 0;
  ctx->next_leader_slot = FD_BUNDLE_WAKE_THRESHOLD_SLOTS + 1;
  fd_bundle_tile_maybe_sleep( ctx, 4 );
  FD_TEST( ctx->sleep_mode==1 );

  free( wksp );
}

static void
test_saturating_sub( void ) {
  FD_LOG_NOTICE(( "TEST saturating subtraction when reset > leader" ));

  fd_bundle_tile_t * ctx = test_ctx;
  memset( ctx, 0, sizeof(fd_bundle_tile_t) );

  void * wksp = mock_replay_wksp_new();
  ctx->replay_in.mem = wksp;

  /* If reset_slot > next_leader_slot, slots_until_leader saturates
     to 0.  This should wake the tile since 0 <= 400. */
  ctx->sleep_mode       = 1;
  ctx->sleep_check_ns   = 0;
  ctx->reset_slot       = 1000UL;
  ctx->next_leader_slot = 500UL;
  fd_bundle_tile_maybe_sleep( ctx, 1 );
  FD_TEST( ctx->sleep_mode==0 );

  free( wksp );
}

static void
test_tls_keylog( void ) {
  fd_bundle_tile_t * ctx = test_ctx;
  memset( ctx, 0, sizeof(fd_bundle_tile_t) );

  int pipefd[2];
  FD_TEST( !pipe( pipefd ) );
  ctx->keylog_fd = pipefd[1];

  uchar client_random[32]; fd_memset( client_random, 0x11, sizeof(client_random) );
  uchar client_secret[32]; fd_memset( client_secret, 0x22, sizeof(client_secret) );
  uchar server_secret[32]; fd_memset( server_secret, 0x33, sizeof(server_secret) );
  fd_memcpy( ctx->tls_conn->hs.base.client_random, client_random, sizeof(client_random) );

  fd_bundle_tls_keylog( &ctx->tls_conn->hs, server_secret, client_secret, FD_TLS_LEVEL_HANDSHAKE );
  fd_bundle_tls_keylog( &ctx->tls_conn->hs, server_secret, client_secret, FD_TLS_LEVEL_APPLICATION );
  FD_TEST( !close( pipefd[1] ) );

  char random_hex[65];
  char client_hex[65];
  char server_hex[65];
  fd_cstr_fini( fd_hex_encode( fd_cstr_init( random_hex ), client_random, sizeof(client_random) ) );
  fd_cstr_fini( fd_hex_encode( fd_cstr_init( client_hex ), client_secret, sizeof(client_secret) ) );
  fd_cstr_fini( fd_hex_encode( fd_cstr_init( server_hex ), server_secret, sizeof(server_secret) ) );

  char expected[768];
  int expected_sz = snprintf( expected, sizeof(expected),
                              "CLIENT_HANDSHAKE_TRAFFIC_SECRET %s %s\n"
                              "SERVER_HANDSHAKE_TRAFFIC_SECRET %s %s\n"
                              "CLIENT_TRAFFIC_SECRET_0 %s %s\n"
                              "SERVER_TRAFFIC_SECRET_0 %s %s\n",
                              random_hex, client_hex, random_hex, server_hex,
                              random_hex, client_hex, random_hex, server_hex );
  FD_TEST( expected_sz>0 && (ulong)expected_sz<sizeof(expected) );

  char actual[768];
  ulong actual_sz = 0UL;
  for(;;) {
    long read_sz = read( pipefd[0], actual+actual_sz, sizeof(actual)-actual_sz );
    FD_TEST( read_sz>=0L );
    if( !read_sz ) break;
    actual_sz += (ulong)read_sz;
  }
  FD_TEST( !close( pipefd[0] ) );
  FD_TEST( actual_sz==(ulong)expected_sz );
  FD_TEST( !memcmp( actual, expected, actual_sz ) );
}

/* A stale ownership snapshot can publish queued Block Engine work after BAM
   takes exclusive TPU control.  Recheck the override and discard that queue. */
static void
test_bam_override_sync_clears_pending( fd_wksp_t * wksp ) {
  FD_LOG_NOTICE(( "TEST BAM override sync clears pending transactions" ));

  uchar fseq_mem[ FD_FSEQ_FOOTPRINT ] __attribute__((aligned(FD_FSEQ_ALIGN)));
  void * fseq_shmem = fd_fseq_new( fseq_mem, 0UL );
  FD_TEST( fseq_shmem );
  ulong * bam_status_fseq = fd_fseq_join( fseq_shmem );
  FD_TEST( bam_status_fseq );

  /* after_credit must observe an activation that occurred since the last
     housekeeping pass and discard queued Block Engine work. */
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  test_bundle_env_mock_conn( env );
  fd_bundle_tile_t * ctx = env->state;
  ctx->bam_status_fseq    = bam_status_fseq;
  ctx->bam_override_active = 0;
  pending_txn_push_tail( ctx->pending_txns, (fd_bundle_pending_txn_t){ .sig=1UL, .bundle_seq=1UL } );

  long before_pause = fd_log_wallclock();
  fd_fseq_update( bam_status_fseq, FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
  int opt_poll_in = 1;
  int charge_busy = 0;
  after_credit( ctx, env->stem, &opt_poll_in, &charge_busy );

  FD_TEST( env->stem_seqs[ 0 ]==0UL );
  FD_TEST( pending_txn_empty( ctx->pending_txns ) );
  FD_TEST( ctx->bam_override_active );
  FD_TEST( ctx->tcp_sock==-1 );
  FD_TEST( !ctx->tcp_sock_connected );
  FD_TEST( ctx->bundle_status_plugin==127 );
  FD_TEST( ctx->bundle_status_recent==FD_BUNDLE_STATE_DISCONNECTED );
  FD_TEST( ctx->bundle_status_logged==FD_BUNDLE_STATE_DISCONNECTED );
  FD_TEST( ctx->last_bundle_status_log_nanos>=before_pause );

  /* The same boundary handles deactivation without discarding pending work. */
  ctx->backoff_until = fd_log_wallclock() + (long)30e9;
  ctx->defer_reset   = 1;
  pending_txn_push_tail( ctx->pending_txns, (fd_bundle_pending_txn_t){ .sig=1UL, .bundle_seq=2UL } );
  long before_resume = fd_log_wallclock();
  fd_fseq_update( bam_status_fseq, 0UL );
  before_credit( ctx, env->stem, &charge_busy );
  FD_TEST( !ctx->bam_override_active );
  FD_TEST( ctx->backoff_until==0L );
  FD_TEST( !ctx->defer_reset );
  FD_TEST( pending_txn_cnt( ctx->pending_txns )==1UL );
  FD_TEST( ctx->last_bundle_status_log_nanos>=before_resume );

  /* An uncontended publication claims and releases the shared status word. */
  opt_poll_in = 1;
  charge_busy = 0;
  after_credit( ctx, env->stem, &opt_poll_in, &charge_busy );
  FD_TEST( env->stem_seqs[ 0 ]==1UL );
  FD_TEST( pending_txn_empty( ctx->pending_txns ) );
  FD_TEST( fd_fseq_query( bam_status_fseq )==0UL );
  FD_TEST( !opt_poll_in );
  FD_TEST( charge_busy );
  test_bundle_env_destroy( env );

  /* before_credit must perform the same transition even when the pending
     queue is nonempty and the client step would otherwise be skipped. */
  test_bundle_env_create( env, wksp );
  ctx = env->state;
  ctx->bam_status_fseq     = bam_status_fseq;
  ctx->bam_override_active = 0;
  pending_txn_push_tail( ctx->pending_txns, (fd_bundle_pending_txn_t){ .sig=1UL, .bundle_seq=2UL } );

  fd_fseq_update( bam_status_fseq, FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
  charge_busy = 0;
  before_credit( ctx, env->stem, &charge_busy );

  FD_TEST( pending_txn_empty( ctx->pending_txns ) );
  FD_TEST( ctx->bam_override_active );
  test_bundle_env_destroy( env );

  /* A publication claim held by another bundle producer prevents this tile
     from publishing or modifying the queue. */
  test_bundle_env_create( env, wksp );
  ctx = env->state;
  ctx->bam_status_fseq     = bam_status_fseq;
  ctx->bam_override_active = 0;
  pending_txn_push_tail( ctx->pending_txns, (fd_bundle_pending_txn_t){ .sig=1UL, .bundle_seq=3UL } );

  fd_fseq_update( bam_status_fseq, FD_BAM_STATUS_FSEQ_BUNDLE_PUBLISHING );
  opt_poll_in = 1;
  charge_busy = 0;
  after_credit( ctx, env->stem, &opt_poll_in, &charge_busy );

  FD_TEST( env->stem_seqs[ 0 ]==0UL );
  FD_TEST( pending_txn_cnt( ctx->pending_txns )==1UL );
  FD_TEST( !ctx->bam_override_active );
  FD_TEST( fd_fseq_query( bam_status_fseq )==FD_BAM_STATUS_FSEQ_BUNDLE_PUBLISHING );
  FD_TEST( opt_poll_in );
  FD_TEST( !charge_busy );

  fd_fseq_update( bam_status_fseq, 0UL );
  after_credit( ctx, env->stem, &opt_poll_in, &charge_busy );
  FD_TEST( env->stem_seqs[ 0 ]==1UL );
  FD_TEST( pending_txn_empty( ctx->pending_txns ) );
  FD_TEST( fd_fseq_query( bam_status_fseq )==0UL );
  test_bundle_env_destroy( env );

  FD_TEST( fd_fseq_leave( bam_status_fseq )==fseq_shmem );
  FD_TEST( fd_fseq_delete( fseq_shmem )==fseq_shmem );
}

/* Exercise the actual tile pause branch: it must expire a partial field block
   without reading more bundles or waiting for Verify/output credits. */
static void
test_deadline_with_pending_output( fd_wksp_t * wksp ) {
  FD_LOG_NOTICE(( "TEST bundle pending output: monotonic partial-header expiry under wall rollback" ));
  long saved_wall = g_bundle_wall;
  long saved_mono = g_bundle_mono;
  g_bundle_wall = 1L;
  g_bundle_mono = 1000000000L;
  test_bundle_env_t env[1];
  test_bundle_env_create( env, wksp );
  test_bundle_env_mock_conn_empty( env );
  fd_bundle_tile_t * ctx=env->state;
  fd_grpc_client_t * client=ctx->grpc_client;
  FD_TEST( fd_grpc_client_stream_acquire(client,FD_BUNDLE_CLIENT_REQ_Bundle_GetBlockBuilderFeeInfo) );
  fd_h2_frame_hdr_t hdr={.typlen=fd_h2_frame_typlen(FD_H2_FRAME_TYPE_HEADERS,100UL),
                        .r_stream_id=fd_uint_bswap(1U)};
  FD_TEST( write(env->server_sock,&hdr,sizeof(hdr))==(long)sizeof(hdr) );
  int rx_busy=0;
  FD_TEST( fd_grpc_client_rxtx_socket(client,ctx->tcp_sock,g_bundle_wall,&rx_busy)==0 );
  FD_TEST(client->conn->rx_hdrs_observed);
  FD_TEST(fd_grpc_client_next_deadline(client)==g_bundle_wall+5000000000L);
  pending_txn_push_tail(ctx->pending_txns,(fd_bundle_pending_txn_t){.bundle_seq=1UL});
  ctx->next_step_deadline=LONG_MAX;
  FD_TEST(next_deadline(ctx)!=LONG_MAX);
  ulong queued=pending_txn_cnt(ctx->pending_txns);
  g_bundle_wall -= 1000000000L;
  for( int expired=0; expired<2; expired++ ) {
    g_bundle_mono=6000000000L-!expired;
    int busy=0;
    before_credit(ctx,env->stem,&busy);
    FD_TEST(expired ? (busy && ctx->tcp_sock==-1 && !ctx->tcp_sock_connected) : (ctx->tcp_sock>=0 && ctx->tcp_sock_connected));
    FD_TEST(pending_txn_cnt(ctx->pending_txns)==queued && env->stem_seqs[0]==0UL);
  }
  test_bundle_env_destroy(env);
  g_bundle_wall = saved_wall;
  g_bundle_mono = saved_mono;
}

/* Exercise the real generated policies with direct syscalls, avoiding the
   VDSO that normally hides the monotonic clock dependency.  The child reports
   success through the permitted logfile pipe; its final exit syscall is
   intentionally denied.  Other clocks and unrelated fd/architecture gates
   must die before sending that report. */
static void
test_monotonic_clock_seccomp( void ) {
  ulong const counts[] = { sock_filter_policy_fd_bam_tile_instr_cnt,
                          sock_filter_policy_fd_bundle_tile_instr_cnt,
                          sock_filter_policy_fd_event_tile_instr_cnt };
  for( uint policy=0U; policy<3U; policy++ ) for( uint fault=0U; fault<5U; fault++ ) {
    int pipefd[2]; FD_TEST(!pipe(pipefd));
    struct sock_filter filter[256];
    if( policy==0U )
      populate_sock_filter_policy_fd_bam_tile(256UL,filter,(uint)pipefd[1],UINT_MAX,UINT_MAX,UINT_MAX,UINT_MAX,UINT_MAX);
    else if( policy==1U )
      populate_sock_filter_policy_fd_bundle_tile(256UL,filter,(uint)pipefd[1],UINT_MAX,UINT_MAX,UINT_MAX,UINT_MAX,UINT_MAX);
    else
      populate_sock_filter_policy_fd_event_tile(256UL,filter,(uint)pipefd[1],UINT_MAX,UINT_MAX,UINT_MAX,UINT_MAX);
    /* The generated architecture check is at instruction1. */
    if( fault==4U ) filter[1].k^=1U;
    pid_t pid=fork(); FD_TEST(pid>=0);
    if( !pid ) {
      struct rlimit core_limit={0};
      FD_TEST(!setrlimit(RLIMIT_CORE,&core_limit));
      FD_TEST(signal(SIGSYS,SIG_DFL)!=SIG_ERR);
      FD_TEST(!close(pipefd[0]));
      struct sock_fprog prog={.len=(ushort)counts[policy],.filter=filter};
      FD_TEST(!prctl(PR_SET_NO_NEW_PRIVS,1,0,0,0));
      FD_TEST(!prctl(PR_SET_SECCOMP,SECCOMP_MODE_FILTER,&prog));
      clockid_t clock=fault==1U ? CLOCK_REALTIME : fault==2U ? CLOCK_BOOTTIME : CLOCK_MONOTONIC;
      struct timespec ts;
      long rc=syscall(SYS_clock_gettime,clock,&ts);
      if( fault==3U ) (void)syscall(SYS_write,STDOUT_FILENO,"x",1UL);
      (void)syscall(SYS_write,pipefd[1],&rc,sizeof(rc));
      (void)syscall(SYS_exit_group,0); /* not in these production policies */
      __builtin_trap();
    }
    FD_TEST(!close(pipefd[1]));
    long rc=-1L;
    long received=read(pipefd[0],&rc,sizeof(rc));
    FD_TEST(!close(pipefd[0]));
    int status; FD_TEST(waitpid(pid,&status,0)==pid);
    FD_TEST(WIFSIGNALED(status) && WTERMSIG(status)==SIGSYS);
    FD_TEST(fault ? received==0L : (received==(long)sizeof(rc) && !rc));
  }
}

int
main( int     argc,
      char ** argv ) {
  (void)scratch_footprint;
  (void)next_deadline;
  (void)before_credit;
  (void)after_credit;
  (void)metrics_write;
  (void)populate_sock_filter_policy_fd_bundle_tile;

  fd_boot( &argc, &argv );
  test_monotonic_clock_seccomp();

  ulong cpu_idx = fd_tile_cpu_id( fd_tile_idx() );
  if( cpu_idx>fd_shmem_cpu_cnt() ) cpu_idx = 0UL;
  /* Transaction V1 raises FD_TPU_PARSED_MTU enough that the 128-entry test
     dcache and pending deque no longer fit in a 1 MiB workspace. */
  fd_wksp_t * wksp = fd_wksp_new_anonymous( FD_SHMEM_NORMAL_PAGE_SZ, 512UL, fd_shmem_cpu_idx( fd_shmem_numa_idx( cpu_idx ) ), "wksp", 16UL );
  FD_TEST( wksp );

  test_replay_frag_ingest();
  test_maybe_sleep_no_replay();
  test_maybe_sleep_unknown_schedule();
  test_maybe_sleep_far_leader();
  test_maybe_sleep_close_leader();
  test_maybe_sleep_hysteresis();
  test_maybe_sleep_check_interval();
  test_replay_triggers_sleep_transition();
  test_boundary_thresholds();
  test_saturating_sub();
  test_tls_keylog();
  test_bam_override_sync_clears_pending( wksp );
  test_deadline_with_pending_output( wksp );

  fd_wksp_usage_t wksp_usage;
  FD_TEST( fd_wksp_usage( wksp, NULL, 0UL, &wksp_usage ) );
  FD_TEST( wksp_usage.free_cnt==wksp_usage.total_cnt );
  fd_wksp_delete_anonymous( wksp );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
