#include "fd_shred_tile.c"

#include <stdlib.h>

#define TEST_SLOTS_PER_EPOCH (1024UL)

static void
test_bam_forwarding_window_and_send( void ) {
  fd_shred_ctx_t ctx[1];
  fd_memset( ctx, 0, sizeof(ctx) );
  fd_memset( ctx->identity_key->uc, 'I', sizeof(fd_pubkey_t) );
  char const * stakers = "ABCDEFGHIKLMNOPQRSTUVWXYZ";

  fd_stake_ci_t * stake_ci = aligned_alloc( fd_stake_ci_align(), fd_stake_ci_footprint() );
  FD_TEST( stake_ci );
  ctx->stake_ci = fd_stake_ci_join( fd_stake_ci_new( stake_ci, ctx->identity_key ) );

  ulong stake_msg_align = alignof(fd_stake_weight_msg_t);
  ulong stake_msg_footprint = fd_ulong_align_up( fd_stake_weight_msg_sz( strlen( stakers ) ), stake_msg_align );
  fd_stake_weight_msg_t * stake_msg = aligned_alloc( stake_msg_align, stake_msg_footprint );
  FD_TEST( stake_msg );

  stake_msg->epoch              = 0UL;
  stake_msg->start_slot         = 0UL;
  stake_msg->slot_cnt           = TEST_SLOTS_PER_EPOCH;
  stake_msg->staked_cnt         = strlen( stakers );
  stake_msg->excluded_stake     = 0UL;
  stake_msg->vote_keyed_lsched  = 0UL;
  for( ulong i=0UL; stakers[i]; i++ ) {
    fd_memset( stake_msg->weights[i].vote_key.uc, stakers[i], sizeof(fd_pubkey_t) );
    fd_memset( stake_msg->weights[i].id_key.uc,   stakers[i], sizeof(fd_pubkey_t) );
    stake_msg->weights[i].stake = 1000UL/(i+1UL);
  }
  fd_stake_ci_stake_msg_init( ctx->stake_ci, stake_msg );
  fd_stake_ci_stake_msg_fini( ctx->stake_ci );

  ulong run_start = ULONG_MAX;
  ulong run_end   = ULONG_MAX;
  ulong rot_cnt   = TEST_SLOTS_PER_EPOCH / FD_EPOCH_SLOTS_PER_ROTATION;
  for( ulong rot=1UL; rot+1UL<rot_cnt; rot++ ) {
    ulong slot = rot * FD_EPOCH_SLOTS_PER_ROTATION;
    fd_pubkey_t const * prev = fd_epoch_leaders_get( fd_stake_ci_get_lsched_for_slot( ctx->stake_ci, slot-FD_EPOCH_SLOTS_PER_ROTATION ), slot-FD_EPOCH_SLOTS_PER_ROTATION );
    fd_pubkey_t const * curr = fd_epoch_leaders_get( fd_stake_ci_get_lsched_for_slot( ctx->stake_ci, slot                          ), slot                          );
    if( FD_UNLIKELY( !prev || !curr ) ) continue;
    if( fd_memeq( prev, ctx->identity_key, sizeof(fd_pubkey_t) ) ) continue;
    if( !fd_memeq( curr, ctx->identity_key, sizeof(fd_pubkey_t) ) ) continue;

    ulong end_rot = rot;
    for( ; end_rot+1UL<rot_cnt; end_rot++ ) {
      ulong next_slot = (end_rot+1UL) * FD_EPOCH_SLOTS_PER_ROTATION;
      fd_pubkey_t const * next = fd_epoch_leaders_get( fd_stake_ci_get_lsched_for_slot( ctx->stake_ci, next_slot ), next_slot );
      if( FD_UNLIKELY( !next || !fd_memeq( next, ctx->identity_key, sizeof(fd_pubkey_t) ) ) ) break;
    }

    ulong after_slot = (end_rot+1UL) * FD_EPOCH_SLOTS_PER_ROTATION;
    fd_pubkey_t const * after = fd_epoch_leaders_get( fd_stake_ci_get_lsched_for_slot( ctx->stake_ci, after_slot ), after_slot );
    if( FD_LIKELY( after && !fd_memeq( after, ctx->identity_key, sizeof(fd_pubkey_t) ) ) ) {
      run_start = slot;
      run_end   = after_slot - 1UL;
      break;
    }
  }

  FD_TEST( run_start!=ULONG_MAX );

  void * net_out_mem = aligned_alloc( FD_CHUNK_ALIGN, FD_CHUNK_SZ*512UL );
  FD_TEST( net_out_mem );
  fd_frag_meta_t mcache0[ 16 ] = {0};
  fd_frag_meta_t mcache1[ 16 ] = {0};
  fd_frag_meta_t * mcaches[ 2 ] = { mcache0, mcache1 };
  ulong seqs[ 2 ] = { 0UL, 0UL };
  ulong depths[ 2 ] = { 16UL, 16UL };
  ulong cr_avail[ 2 ] = { 128UL, 128UL };
  ulong min_cr_avail = ULONG_MAX;
  fd_stem_context_t stem = {
    .mcaches              = mcaches,
    .seqs                 = seqs,
    .depths               = depths,
    .cr_avail             = cr_avail,
    .min_cr_avail         = &min_cr_avail,
    .cr_decrement_amount  = 0UL
  };

  ctx->net_out_mem    = (fd_wksp_t *)net_out_mem;
  ctx->net_out_chunk0 = 0UL;
  ctx->net_out_chunk  = 0UL;
  ctx->net_out_wmark  = 256UL;
  ctx->tsorig         = 1234UL;
  fd_ip4_udp_hdr_init( ctx->data_shred_net_hdr,   FD_SHRED_MIN_SZ, 0U, 9001U );
  fd_ip4_udp_hdr_init( ctx->parity_shred_net_hdr, FD_SHRED_MAX_SZ, 0U, 9001U );

  ctx->bam_dests_cnt      = 2UL;
  ctx->bam_dests[ 0 ].ip4 = FD_IP4_ADDR( 1, 2, 3, 4 );
  ctx->bam_dests[ 0 ].port= 7001U;
  ctx->bam_dests[ 1 ].ip4 = FD_IP4_ADDR( 5, 6, 7, 8 );
  ctx->bam_dests[ 1 ].port= 7002U;

  uchar shred_mem[ FD_SHRED_MIN_SZ ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  fd_memset( shred_mem, 0, sizeof(shred_mem) );
  fd_shred_t * shred = (fd_shred_t *)shred_mem;
  shred->variant         = fd_shred_variant( FD_SHRED_TYPE_MERKLE_DATA, 2 );
  shred->slot            = run_start-2UL;
  shred->idx             = 7U;
  shred->fec_set_idx     = 7U;
  shred->data.parent_off = 1U;
  shred->data.size       = FD_SHRED_MIN_SZ;

  ulong second_chunk = fd_dcache_compact_next( 0UL, FD_SHRED_MIN_SZ + sizeof(fd_ip4_udp_hdrs_t), 0UL, ctx->net_out_wmark );
  fd_shred_send_bam_shred( ctx, &stem, shred );

  FD_TEST( ctx->bam_obs->cnt.forwarded_shred_cnt        == 1UL );
  FD_TEST( ctx->bam_obs->cnt.forwarded_packet_cnt       == 2UL );
  FD_TEST( ctx->bam_obs->cnt.skipped_no_receivers_cnt   == 0UL );
  FD_TEST( ctx->bam_obs->cnt.skipped_outside_window_cnt == 0UL );
  FD_TEST( stem.seqs[ NET_OUT_IDX ]                 == 2UL );
  FD_TEST( mcache1[ 0 ].chunk                       == 0U );
  FD_TEST( mcache1[ 1 ].chunk                       == (uint)second_chunk );

  fd_ip4_udp_hdrs_t const * pkt0 = fd_chunk_to_laddr_const( net_out_mem, 0UL );
  fd_ip4_udp_hdrs_t const * pkt1 = fd_chunk_to_laddr_const( net_out_mem, second_chunk );
  FD_TEST( pkt0->ip4->daddr                          == ctx->bam_dests[ 0 ].ip4 );
  FD_TEST( fd_ushort_bswap( pkt0->udp->net_dport )  == ctx->bam_dests[ 0 ].port );
  FD_TEST( pkt1->ip4->daddr                          == ctx->bam_dests[ 1 ].ip4 );
  FD_TEST( fd_ushort_bswap( pkt1->udp->net_dport )  == ctx->bam_dests[ 1 ].port );

  shred->slot = run_start-3UL;
  fd_shred_send_bam_shred( ctx, &stem, shred );
  FD_TEST( ctx->bam_obs->cnt.forwarded_shred_cnt        == 1UL );
  FD_TEST( ctx->bam_obs->cnt.forwarded_packet_cnt       == 2UL );
  FD_TEST( ctx->bam_obs->cnt.skipped_outside_window_cnt == 1UL );
  FD_TEST( stem.seqs[ NET_OUT_IDX ]                     == 2UL );

  shred->slot = run_start-1UL;
  fd_shred_send_bam_shred( ctx, &stem, shred );
  FD_TEST( ctx->bam_obs->cnt.forwarded_shred_cnt        == 2UL );
  FD_TEST( ctx->bam_obs->cnt.forwarded_packet_cnt       == 4UL );
  FD_TEST( ctx->bam_obs->cnt.skipped_outside_window_cnt == 1UL );
  FD_TEST( stem.seqs[ NET_OUT_IDX ]                     == 4UL );

  shred->slot = run_start;
  fd_shred_send_bam_shred( ctx, &stem, shred );
  FD_TEST( ctx->bam_obs->cnt.forwarded_shred_cnt        == 3UL );
  FD_TEST( ctx->bam_obs->cnt.forwarded_packet_cnt       == 6UL );
  FD_TEST( stem.seqs[ NET_OUT_IDX ]                     == 6UL );

  shred->slot = run_end;
  fd_shred_send_bam_shred( ctx, &stem, shred );
  FD_TEST( ctx->bam_obs->cnt.forwarded_shred_cnt        == 4UL );
  FD_TEST( ctx->bam_obs->cnt.forwarded_packet_cnt       == 8UL );
  FD_TEST( stem.seqs[ NET_OUT_IDX ]                     == 8UL );

  shred->slot = run_end+1UL;
  fd_shred_send_bam_shred( ctx, &stem, shred );
  FD_TEST( ctx->bam_obs->cnt.forwarded_shred_cnt        == 4UL );
  FD_TEST( ctx->bam_obs->cnt.forwarded_packet_cnt       == 8UL );
  FD_TEST( ctx->bam_obs->cnt.skipped_outside_window_cnt == 2UL );
  FD_TEST( stem.seqs[ NET_OUT_IDX ]                     == 8UL );

  ctx->bam_dests_cnt = 0UL;
  shred->slot = run_start;
  fd_shred_send_bam_shred( ctx, &stem, shred );
  FD_TEST( ctx->bam_obs->cnt.skipped_no_receivers_cnt == 1UL );
  FD_TEST( stem.seqs[ NET_OUT_IDX ]                   == 8UL );

  free( stake_msg );
  free( net_out_mem );
  fd_stake_ci_delete( fd_stake_ci_leave( ctx->stake_ci ) );
  free( stake_ci );
}

static void
test_bam_shred_update_apply_and_truncate( void ) {
  fd_shred_ctx_t ctx[1];
  fd_memset( ctx, 0, sizeof(ctx) );
  ctx->in_kind[ 0 ] = IN_KIND_BAM_SHRED;

  ctx->bam_shred_upd_buf->shred_sock_cnt  = 3UL;
  ctx->bam_shred_upd_buf->shred_sock[ 0 ] = (fd_ip4_port_t){ .addr = FD_IP4_ADDR( 1, 1, 1, 1 ), .port = fd_ushort_bswap( 5001U ) };
  ctx->bam_shred_upd_buf->shred_sock[ 1 ] = (fd_ip4_port_t){ .addr = FD_IP4_ADDR( 2, 2, 2, 2 ), .port = fd_ushort_bswap( 5002U ) };
  ctx->bam_shred_upd_buf->shred_sock[ 2 ] = (fd_ip4_port_t){ .addr = FD_IP4_ADDR( 1, 1, 1, 1 ), .port = fd_ushort_bswap( 5001U ) };
  after_frag( ctx, 0UL, 0UL, FD_BAM_STEM_SIG_SHRED_UPDATE, sizeof(fd_bam_shred_update_t), 0UL, 0UL, NULL );

  FD_TEST( ctx->bam_dests_cnt                            == 3UL );
  FD_TEST( ctx->bam_dests[ 0 ].ip4                       == FD_IP4_ADDR( 1, 1, 1, 1 ) );
  FD_TEST( ctx->bam_dests[ 0 ].port                      == 5001U );
  FD_TEST( ctx->bam_dests[ 1 ].ip4                       == FD_IP4_ADDR( 2, 2, 2, 2 ) );
  FD_TEST( ctx->bam_dests[ 1 ].port                      == 5002U );
  FD_TEST( ctx->bam_dests[ 2 ].ip4                       == FD_IP4_ADDR( 1, 1, 1, 1 ) );
  FD_TEST( ctx->bam_dests[ 2 ].port                      == 5001U );
  FD_TEST( ctx->bam_obs->cnt.receiver_update_applied_cnt     == 1UL );
  FD_TEST( ctx->bam_obs->cnt.receiver_update_truncated_cnt   == 0UL );

  ctx->bam_shred_upd_buf->shred_sock_cnt = FD_BAM_SHRED_SOCK_MAX + 5UL;
  for( ulong i=0UL; i<FD_BAM_SHRED_SOCK_MAX; i++ ) {
    ctx->bam_shred_upd_buf->shred_sock[ i ].addr = FD_IP4_ADDR( 9, 9, 9, (uchar)(i+1UL) );
    ctx->bam_shred_upd_buf->shred_sock[ i ].port = fd_ushort_bswap( (ushort)(6000U+i) );
  }
  after_frag( ctx, 0UL, 0UL, FD_BAM_STEM_SIG_SHRED_UPDATE, sizeof(fd_bam_shred_update_t), 0UL, 0UL, NULL );

  FD_TEST( ctx->bam_dests_cnt                          == FD_BAM_SHRED_SOCK_MAX );
  FD_TEST( ctx->bam_dests[ FD_BAM_SHRED_SOCK_MAX-1UL ].ip4  == FD_IP4_ADDR( 9, 9, 9, (uchar)FD_BAM_SHRED_SOCK_MAX ) );
  FD_TEST( ctx->bam_dests[ FD_BAM_SHRED_SOCK_MAX-1UL ].port == (ushort)(6000U + FD_BAM_SHRED_SOCK_MAX - 1UL) );
  FD_TEST( ctx->bam_obs->cnt.receiver_update_applied_cnt   == 2UL );
  FD_TEST( ctx->bam_obs->cnt.receiver_update_truncated_cnt == 1UL );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  test_bam_forwarding_window_and_send();
  test_bam_shred_update_apply_and_truncate();

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
