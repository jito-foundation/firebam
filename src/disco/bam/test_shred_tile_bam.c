/* test_shred_tile_bam covers the shred tile's BAM forwarding, which no
   upstream shred test reaches: the bam_shred input installing the BAM
   node's shred receivers, and fan_out forwarding each shred of a FEC
   set to them only when this validator leads the shred's slot (its own
   shreds) or one of the next four slots starts its leader rotation
   (retransmitted shreds), with the memoized answer reset on a leader
   schedule change and an identity switch. */

#include "../shred/fd_shred_tile.c"

#define TEST_SLOTS_PER_EPOCH (1000UL)
#define TEST_NET_DEPTH       (512UL)
#define TEST_IN_BAM          (0UL)
#define TEST_IN_EPOCH        (1UL)

static fd_shred_ctx_t     test_ctx[1];
static fd_stake_ci_t      test_stake_ci[1];
static fd_shredder_t      test_shredder[1];
static uchar              test_epoch_msg[ FD_EPOCH_INFO_MAX_MSG_SZ ] __attribute__((aligned(64)));
static fd_stake_weight_t  test_id_scratch[ MAX_STAKE_WEIGHTS ];
static fd_fec_set_t       test_set[1];
static uchar              test_in_buf[ 2UL*FD_CHUNK_SZ+sizeof(fd_bam_shred_update_t) ] __attribute__((aligned(FD_CHUNK_ALIGN)));

static fd_frag_meta_t *   test_mcaches     [ 3 ];
static ulong              test_seqs        [ 3 ];
static ulong              test_depths      [ 3 ];
static ulong              test_cr_avail    [ 3 ];
static ulong              test_min_cr_avail;
static int                test_out_reliable[ 3 ];
static fd_stem_context_t  test_stem[1];

/* One staker leads every slot of the epoch */
static void
test_epoch( ulong epoch,
            uchar leader ) {
  fd_epoch_info_msg_t * msg = fd_type_pun( test_epoch_msg );
  fd_memset( msg, 0, sizeof(fd_epoch_info_msg_t) );
  msg->epoch           = epoch;
  msg->start_slot      = epoch*TEST_SLOTS_PER_EPOCH;
  msg->slot_cnt        = TEST_SLOTS_PER_EPOCH;
  msg->staked_vote_cnt = 1UL;
  msg->features.geo_aware_leader_schedule = FD_FEATURE_DISABLED;
  fd_vote_stake_weight_t * w = fd_epoch_info_msg_stake_weights( msg );
  fd_memset( w->vote_key.uc, leader, sizeof(fd_pubkey_t) );
  fd_memset( w->id_key.uc,   leader, sizeof(fd_pubkey_t) );
  w->stake = 1000UL;
  msg->staked_id_cnt = compute_id_weights_from_vote_weights( test_id_scratch, w, 1UL );
  fd_memcpy( fd_epoch_info_msg_id_weights( msg ), test_id_scratch, msg->staked_id_cnt*sizeof(fd_stake_weight_t) );
  fd_stake_ci_epoch_msg_init( test_ctx->stake_ci, msg );
  /* The tile's epoch after_frag finishes it and drops the memoized answer */
  test_ctx->in_kind[ TEST_IN_EPOCH ] = IN_KIND_EPOCH;
  test_ctx->skip_frag = 0;
  after_frag( test_ctx, TEST_IN_EPOCH, 0UL, 0UL, 0UL, 0UL, 0UL, test_stem );
}

/* Sends a bam_shred update with cnt receivers 10.0.0.(i+1):(9000+i)
   (sz 0 means the right size) through before/during/after_frag, and
   returns whether the tile took it. */
static int
test_bam_update( ulong cnt,
                 ulong sz ) {
  fd_bam_shred_update_t * upd = (fd_bam_shred_update_t *)test_in_buf;
  fd_memset( upd, 0, sizeof(fd_bam_shred_update_t) );
  upd->shred_sock_cnt = (uchar)cnt;
  for( ulong i=0UL; i<fd_ulong_min( cnt, FD_BAM_SHRED_SOCK_MAX ); i++ ) {
    upd->shred_sock[ i ].addr = FD_IP4_ADDR( 10, 0, 0, (uchar)(i+1UL) );
    upd->shred_sock[ i ].port = fd_ushort_bswap( (ushort)(9000UL+i) );
  }
  FD_TEST(  before_frag( test_ctx, TEST_IN_BAM, 0UL, FD_BAM_STEM_SIG_GOSSIP_UPDATE ) );
  FD_TEST( !before_frag( test_ctx, TEST_IN_BAM, 0UL, FD_BAM_STEM_SIG_SHRED_UPDATE  ) );
  during_frag( test_ctx, TEST_IN_BAM, 0UL, FD_BAM_STEM_SIG_SHRED_UPDATE, test_ctx->in[ TEST_IN_BAM ].chunk0,
               sz ? sz : sizeof(fd_bam_shred_update_t), 0UL );
  int took = !test_ctx->skip_frag;
  after_frag( test_ctx, TEST_IN_BAM, 0UL, FD_BAM_STEM_SIG_SHRED_UPDATE, sizeof(fd_bam_shred_update_t), 0UL, 0UL, test_stem );
  return took;
}

/* Counts packets published since seq0 to BAM receivers (10.0.0.x) and
   checks each one's destination port. */
static ulong
test_bam_sent( ulong seq0 ) {
  ulong cnt = 0UL;
  for( ulong seq=seq0; seq<test_seqs[ NET_OUT_IDX ]; seq++ ) {
    fd_frag_meta_t const * meta = test_mcaches[ NET_OUT_IDX ] + fd_mcache_line_idx( seq, test_depths[ NET_OUT_IDX ] );
    FD_TEST( meta->seq==seq );
    fd_ip4_udp_hdrs_t const * hdr = fd_chunk_to_laddr_const( test_ctx->net_out_mem, meta->chunk );
    uint daddr = hdr->ip4->daddr;
    uchar const * b = (uchar const *)&daddr;
    if( b[0]!=10 || b[1]!=0 || b[2]!=0 ) continue;
    FD_TEST( b[3]>=1 && fd_ushort_bswap( hdr->udp->net_dport )==(ushort)(9000U+b[3]-1U) );
    cnt++;
  }
  return cnt;
}

/* Runs fan_out on a set with data shreds 0 and 1 of slot, and returns
   how many packets went to BAM receivers. */
static ulong
test_fan_out( ulong slot,
              int   is_leader ) {
  fd_memset( test_set, 0, sizeof(fd_fec_set_t) );
  test_set->data_shred_rcvd   = ~3U; /* fan_out( received=0 ) takes the clear bits */
  test_set->parity_shred_rcvd = ~0U;
  for( ulong i=0UL; i<2UL; i++ ) {
    test_set->data_shreds[ i ].s->variant = FD_SHRED_TYPE_MERKLE_DATA;
    test_set->data_shreds[ i ].s->slot    = slot;
    test_set->data_shreds[ i ].s->idx     = (uint)i;
  }
  ulong seq0 = test_seqs[ NET_OUT_IDX ];
  fan_out( test_ctx, test_stem, test_set, is_leader, 0, 0UL );
  return test_bam_sent( seq0 );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  fd_pubkey_t me;    fd_memset( me.uc,    'A', sizeof(fd_pubkey_t) );
  fd_pubkey_t other; fd_memset( other.uc, 'C', sizeof(fd_pubkey_t) );

  fd_shred_ctx_t * ctx = test_ctx;
  ctx->identity_key[ 0 ]    = me;
  ctx->stake_ci             = fd_stake_ci_join( fd_stake_ci_new( test_stake_ci, ctx->identity_key, 1234UL ) );
  FD_TEST( ctx->stake_ci );
  ctx->shredder             = test_shredder;
  test_shredder->shred_version = 1;
  ctx->slot                 = ULONG_MAX;
  ctx->bam_dests_cnt        = 0U;
  ctx->bam_leader_soon_slot = ULONG_MAX;
  fd_ip4_udp_hdr_init( ctx->data_shred_net_hdr,   FD_SHRED_MIN_SZ, 0, 8001 );
  fd_ip4_udp_hdr_init( ctx->parity_shred_net_hdr, FD_SHRED_MAX_SZ, 0, 8001 );

  /* Inputs and the net out are malloc'd.  The bam_shred in has a NULL
     base (its chunk is an address >> FD_CHUNK_LG_SZ), the net out its
     dcache as base so published chunks fit the frag meta's 32 bits. */
  ctx->in_kind[ TEST_IN_BAM ]     = IN_KIND_BAM_SHRED;
  ctx->in[ TEST_IN_BAM ].mem      = NULL;
  ctx->in[ TEST_IN_BAM ].chunk0   = (ulong)test_in_buf >> FD_CHUNK_LG_SZ;
  ctx->in[ TEST_IN_BAM ].wmark    = ctx->in[ TEST_IN_BAM ].chunk0+1UL;
  ulong data_sz = fd_dcache_req_data_sz( FD_NET_MTU, TEST_NET_DEPTH, 1UL, 1 );
  void * dcache = fd_dcache_join( fd_dcache_new( aligned_alloc( fd_dcache_align(), fd_dcache_footprint( data_sz, 0UL ) ), data_sz, 0UL ) );
  FD_TEST( dcache );
  ctx->net_out_mem    = (fd_wksp_t *)dcache;
  ctx->net_out_chunk0 = fd_dcache_compact_chunk0( ctx->net_out_mem, dcache );
  ctx->net_out_wmark  = fd_dcache_compact_wmark ( ctx->net_out_mem, dcache, FD_NET_MTU );
  ctx->net_out_chunk  = ctx->net_out_chunk0;
  void * mcache_mem = aligned_alloc( fd_mcache_align(), fd_mcache_footprint( TEST_NET_DEPTH, 0UL ) );
  test_mcaches[ NET_OUT_IDX ] = fd_mcache_join( fd_mcache_new( mcache_mem, TEST_NET_DEPTH, 0UL, 0UL ) );
  FD_TEST( test_mcaches[ NET_OUT_IDX ] );
  test_depths[ NET_OUT_IDX ] = TEST_NET_DEPTH;
  for( ulong i=0UL; i<3UL; i++ ) test_cr_avail[ i ] = ULONG_MAX;
  *test_stem = (fd_stem_context_t){ .mcaches = test_mcaches, .seqs = test_seqs, .depths = test_depths,
                                    .cr_avail = test_cr_avail, .min_cr_avail = &test_min_cr_avail,
                                    .cr_decrement_amount = 1UL, .out_reliable = test_out_reliable };

  /* We lead epoch 0, another validator epoch 1 */
  test_epoch( 0UL, 'A' );
  test_epoch( 1UL, 'C' );

  /* Nothing is forwarded before BAM supplies receivers */
  FD_TEST( !test_fan_out( 100UL, 1 ) );

  /* A malformed update is skipped and keeps the receivers */
  FD_TEST( !test_bam_update( 2UL, sizeof(fd_bam_shred_update_t)-1UL ) );
  FD_TEST( !test_bam_update( FD_BAM_SHRED_SOCK_MAX+1UL, 0UL ) );
  FD_TEST( !ctx->bam_dests_cnt );
  FD_TEST( test_bam_update( 2UL, 0UL ) );
  FD_TEST( ctx->bam_dests_cnt==2U );
  FD_TEST( ctx->bam_dests[ 1 ].ip4==FD_IP4_ADDR( 10, 0, 0, 2 ) && ctx->bam_dests[ 1 ].port==9001 );

  /* Own shreds of a slot we lead go to each receiver, also at the end of
     our epoch, where the following rotation is another validator's */
  FD_TEST( test_fan_out( 100UL, 1 )==4UL );
  FD_TEST( test_fan_out( 999UL, 1 )==4UL );
  FD_TEST( !test_fan_out( 1100UL, 1 ) );

  /* Retransmitted shreds go only ahead of our rotation */
  FD_TEST( test_fan_out( 995UL, 0 )==4UL );  /* 996 starts our rotation */
  FD_TEST( ctx->bam_leader_soon_slot==995UL && ctx->bam_leader_soon );
  FD_TEST( !test_fan_out( 999UL, 0 ) );      /* 1000..1003 are another validator's */
  FD_TEST( !test_fan_out( 1100UL, 0 ) );

  /* Retransmits older than our leader slot in progress stop */
  ctx->slot = 996UL;
  FD_TEST( !test_fan_out( 995UL, 0 ) );
  ctx->slot = ULONG_MAX;

  /* A leader schedule change drops the memoized answer for slot 995 */
  FD_TEST( test_fan_out( 995UL, 0 )==4UL );
  test_epoch( 0UL, 'C' );
  FD_TEST( ctx->bam_leader_soon_slot==ULONG_MAX );
  FD_TEST( !test_fan_out( 995UL, 0 ) );
  test_epoch( 0UL, 'A' );

  /* So does an identity switch */
  FD_TEST( test_fan_out( 995UL, 0 )==4UL );
  fd_keyswitch_t keyswitch[1];
  fd_memset( keyswitch, 0, sizeof(keyswitch) );
  fd_memset( keyswitch->bytes, 'C', 32UL );
  keyswitch->state = FD_KEYSWITCH_STATE_SWITCH_PENDING;
  ctx->keyswitch = keyswitch;
  during_housekeeping( ctx );
  FD_TEST( keyswitch->state==FD_KEYSWITCH_STATE_COMPLETED && fd_memeq( ctx->identity_key->uc, other.uc, 32UL ) );
  FD_TEST( !test_fan_out( 995UL, 0 ) );
  FD_TEST( test_fan_out( 1100UL, 1 )==4UL ); /* 'C' leads epoch 1 */

  free( mcache_mem );
  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
