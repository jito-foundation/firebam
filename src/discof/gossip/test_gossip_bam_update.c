#include "../../util/fd_util.h"
#include "../../flamenco/gossip/fd_gossip.h"
#include "fd_gossip_tile.h"
#include <stdlib.h>

static void
send_stub( void *               ctx,
           fd_stem_context_t *  stem,
           uchar const *        data,
           ulong                sz,
           fd_ip4_port_t const *peer_address,
           ulong                now ) {
  (void)ctx; (void)stem; (void)data; (void)sz; (void)peer_address; (void)now;
}

static void
sign_stub( void *        ctx,
           uchar const * data,
           ulong         sz,
           int           sign_type,
           uchar *       out_signature ) {
  (void)ctx; (void)data; (void)sz; (void)sign_type;
  for( uchar i=0; i<64; i++ ) out_signature[ i ] = i;
}

static void
ping_change_stub( void *        ctx,
                  uchar const * peer_pubkey,
                  fd_ip4_port_t peer_address,
                  long          now,
                  int           change_type ) {
  (void)ctx; (void)peer_pubkey; (void)peer_address; (void)now; (void)change_type;
}

static void *
setup_gossip( fd_gossip_tile_ctx_t * ctx,
              fd_contact_info_t *    my_ci ) {
  ulong max_values      = 128UL;
  ulong entrypoints_len = 1UL;

  fd_ip4_port_t entrypoints[1] = {0};
  entrypoints[0].addr = my_ci->sockets[ FD_CONTACT_INFO_SOCKET_GOSSIP ].addr;
  entrypoints[0].port = my_ci->sockets[ FD_CONTACT_INFO_SOCKET_GOSSIP ].port;

  void * mem = aligned_alloc( fd_gossip_align(), fd_gossip_footprint( max_values, entrypoints_len ) );
  FD_TEST( mem );

  fd_rng_t * rng = fd_rng_join( fd_rng_new( ctx->rng, ctx->rng_seed, ctx->rng_idx ) );
  FD_TEST( rng );

  fd_gossip_out_ctx_t * gossip_out = ctx->gossip_out;
  fd_gossip_out_ctx_t * net_out    = ctx->net_out;
  fd_memset( gossip_out, 0, sizeof(fd_gossip_out_ctx_t) );
  fd_memset( net_out,    0, sizeof(fd_gossip_out_ctx_t) );

  void * shgossip = fd_gossip_new( mem,
                                   rng,
                                   max_values,
                                   entrypoints_len,
                                   entrypoints,
                                   my_ci,
                                   ctx->last_wallclock,
                                   send_stub,
                                   NULL,
                                   sign_stub,
                                   NULL,
                                   ping_change_stub,
                                   NULL,
                                   gossip_out,
                                   net_out );
  FD_TEST( shgossip );
  ctx->gossip = fd_gossip_join( shgossip );
  FD_TEST( ctx->gossip );
  return mem;
}

static void
test_gossip_apply_bam_contact_updates_contact_info( void ) {
  fd_gossip_tile_ctx_t ctx;
  fd_memset( &ctx, 0, sizeof(ctx) );

  ctx.ticks_per_ns   = fd_tempo_tick_per_ns( NULL );
  ctx.last_wallclock = fd_log_wallclock();
  ctx.last_tickcount = fd_tickcount();

  fd_contact_info_t base_ci = {0};
  for( uchar i=0; i<32; i++ ) base_ci.pubkey.uc[ i ] = i;
  base_ci.shred_version                        = 1U;
  base_ci.instance_creation_wallclock_nanos    = ctx.last_wallclock;
  base_ci.wallclock_nanos                      = ctx.last_wallclock;
  base_ci.version.client                       = FD_CONTACT_INFO_VERSION_CLIENT_FIREDANCER;
  base_ci.version.major                        = 0U;
  base_ci.version.minor                        = 0U;
  base_ci.version.patch                        = 0U;

  /* bam
  {
  "featureSet": 3604001754,
  "gossip": "64.130.57.189:8000",
  "pubkey": "23U4mgK9DMCxsv2StC4y2qAptP25Xv5b2cybKCeJ1to3",
  "pubsub": null,
  "rpc": null,
  "serveRepair": "64.130.57.189:8012",
  "shredVersion": 50093,
  "tpu": "64.130.57.242:5004",
  "tpuForwards": "64.130.57.242:5005",
  "tpuForwardsQuic": "64.130.57.242:5011",
  "tpuQuic": "64.130.57.242:5010",
  "tpuVote": "64.130.57.189:8005",
  "tvu": "64.130.57.189:8001",
  "version": "3.0.10"
  }

  // non bam
  {
      "featureSet": 3604001754,
      "gossip": "88.211.250.236:8000",
      "pubkey": "F6SC6vku9XZniYns4mv7Q7eAEETseaL8DpiGW5vrZC14",
      "pubsub": null,
      "rpc": null,
      "serveRepair": "88.211.250.236:8012",
      "shredVersion": 50093,
      "tpu": "88.211.250.236:8003",
      "tpuForwards": "88.211.250.236:8004",
      "tpuForwardsQuic": "88.211.250.236:8010",
      "tpuQuic": "88.211.250.236:8009",
      "tpuVote": "88.211.250.236:8005",
      "tvu": "88.211.250.236:8001",
      "version": "3.0.10"
    },
  */

  fd_ip4_port_t default_gossip = { .addr = 0, .port = fd_ushort_bswap( 8000 ) };
  FD_TEST( fd_cstr_to_ip4_addr( "127.0.0.1", &default_gossip.addr ) );

  fd_ip4_port_t default_tpu = { .addr = 0, .port = fd_ushort_bswap( 8003 ) };
  FD_TEST( fd_cstr_to_ip4_addr( "1.1.1.1", &default_tpu.addr ) );

  fd_ip4_port_t default_tpu_quic = { .addr = 0, .port = fd_ushort_bswap( 8009 ) };
  FD_TEST( fd_cstr_to_ip4_addr( "2.2.2.2", &default_tpu_quic.addr ) );

  base_ci.sockets[ FD_CONTACT_INFO_SOCKET_GOSSIP ]            = default_gossip;
  base_ci.sockets[ FD_CONTACT_INFO_SOCKET_TPU ]               = default_tpu;
  base_ci.sockets[ FD_CONTACT_INFO_SOCKET_TPU_FORWARDS ]      = default_tpu;
  base_ci.sockets[ FD_CONTACT_INFO_SOCKET_TPU_QUIC ]          = default_tpu_quic;
  base_ci.sockets[ FD_CONTACT_INFO_SOCKET_TPU_FORWARDS_QUIC ] = default_tpu_quic;

  *ctx.my_contact_info     = base_ci;

  void * gossip_mem = setup_gossip( &ctx, ctx.my_contact_info );

  uint bam_tpu = 0U;
  FD_TEST( fd_cstr_to_ip4_addr( "9.8.7.6", &bam_tpu ) );
  uint bam_tpu_fwd = 0U;
  FD_TEST( fd_cstr_to_ip4_addr( "4.3.2.1", &bam_tpu_fwd ) );

  fd_bam_contact_update_t contact_update = {
      .tpu     = (fd_ip4_port_t){ .addr = bam_tpu,     .port = fd_ushort_bswap( 5000 ) },
      .tpu_fwd = (fd_ip4_port_t){ .addr = bam_tpu_fwd, .port = fd_ushort_bswap( 6000 ) },
  };
  fd_gossip_tile_apply_bam_contact( &ctx, &contact_update, ctx.last_wallclock + 1L );

  fd_ip4_port_t expected_tpu = { .addr = bam_tpu, .port = fd_ushort_bswap( 5000 ) };
  fd_ip4_port_t expected_tpu_fwd = { .addr = bam_tpu_fwd, .port = fd_ushort_bswap( 6000 ) };

  FD_TEST( ctx.my_contact_info->sockets[ FD_CONTACT_INFO_SOCKET_TPU ].l               == expected_tpu.l );
  FD_TEST( ctx.my_contact_info->sockets[ FD_CONTACT_INFO_SOCKET_TPU_FORWARDS ].l      == expected_tpu_fwd.l );
  FD_TEST( ctx.my_contact_info->sockets[ FD_CONTACT_INFO_SOCKET_TPU_QUIC ].l          == expected_tpu.l );
  FD_TEST( ctx.my_contact_info->sockets[ FD_CONTACT_INFO_SOCKET_TPU_FORWARDS_QUIC ].l == expected_tpu_fwd.l );
  free( gossip_mem );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  test_gossip_apply_bam_contact_updates_contact_info();
  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
