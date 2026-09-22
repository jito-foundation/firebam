/* test_bam_event_tile checks that the event tile skips BAM transactions
   on dedup_resolv (development.event.report_transactions) instead of
   aborting on their source_tpu, while other transactions, including
   block engine bundles, are still reported. */

#include "../events/fd_event_tile.c"

#include <stdlib.h>

static uchar in_mem[ 4096UL ] __attribute__((aligned(FD_CHUNK_ALIGN)));

static fd_event_tile_t ctx[1];

static fd_txn_m_t *
stage_txn( uchar source_tpu ) {
  fd_memset( in_mem, 0, sizeof(in_mem) );
  fd_txn_m_t * txnm = (fd_txn_m_t *)in_mem;
  txnm->payload_sz = 8;
  txnm->source_tpu = source_tpu;
  fd_memset( fd_txn_m_payload( txnm ), 0xAB, 8UL );
  return txnm;
}

static void
deliver_txn( void ) {
  ctx->chunk = 0UL;
  after_frag( ctx, 0UL, 0UL, 0UL, sizeof(fd_txn_m_t)+8UL, 0UL, 0UL, NULL );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  ulong  circq_sz  = 1UL<<20;
  void * circq_mem = aligned_alloc( FD_CIRCQ_ALIGN, fd_ulong_align_up( fd_circq_footprint( circq_sz ), FD_CIRCQ_ALIGN ) );
  FD_TEST( circq_mem );
  ctx->circq = fd_circq_join( fd_circq_new( circq_mem, circq_sz ) );
  FD_TEST( ctx->circq );

  /* Zero-initialized: the txn path only calls fd_event_client_id_reserve,
     a plain counter. */
  ulong  client_sz  = fd_ulong_align_up( fd_event_client_footprint( GRPC_BUF_MAX ), fd_event_client_align() );
  void * client_mem = aligned_alloc( fd_event_client_align(), client_sz );
  FD_TEST( client_mem );
  fd_memset( client_mem, 0, client_sz );
  ctx->client = (fd_event_client_t *)client_mem;

  fd_clock_tile_init( ctx->clock );

  ctx->in_cnt       = 1UL;
  ctx->in_kind[ 0 ] = IN_KIND_DEDUP;
  ctx->in[ 0 ]      = (fd_event_tile_in_t){ .mem = (fd_wksp_t *)in_mem, .mtu = FD_TPU_PARSED_MTU, .chunk0 = 0UL, .wmark = 0UL };

  /* BAM transactions, whether preprocessing passed or failed and
     whether or not they are atomic, are skipped before a circq slot or
     event id is reserved. */
  stage_txn( FD_TXN_M_TPU_SOURCE_BAM );
  deliver_txn();
  stage_txn( FD_TXN_M_TPU_SOURCE_BAM )->bam.preprocess_failed = 1U;
  deliver_txn();
  fd_txn_m_t * bam_atomic = stage_txn( FD_TXN_M_TPU_SOURCE_BAM );
  bam_atomic->block_engine.bundle_id      = 42UL;
  bam_atomic->block_engine.bundle_txn_cnt = 2UL;
  bam_atomic->bam.revert_on_error         = 1U;
  deliver_txn();
  FD_TEST( !ctx->circq->cnt );
  FD_TEST( !ctx->circq->cursor_push_seq );
  FD_TEST( !fd_event_client_id_reserve( ctx->client ) );

  /* Other sources are still reported, block engine bundles included. */
  stage_txn( FD_TXN_M_TPU_SOURCE_QUIC );
  deliver_txn();
  FD_TEST( ctx->circq->cnt==1UL );

  fd_txn_m_t * bundle = stage_txn( FD_TXN_M_TPU_SOURCE_BUNDLE );
  bundle->block_engine.bundle_id      = 7UL;
  bundle->block_engine.bundle_txn_cnt = 1UL;
  bundle->block_engine.commission     = 5;
  deliver_txn();
  FD_TEST( ctx->circq->cnt==2UL );

  free( client_mem );
  free( fd_circq_delete( fd_circq_leave( ctx->circq ) ) );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
