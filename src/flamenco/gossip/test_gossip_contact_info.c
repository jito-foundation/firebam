#include "../../util/fd_util.h"
#include "fd_gossip.h"

#include <stdlib.h>
#include <string.h>

static void
noop_deliver( fd_crds_data_t * data FD_PARAM_UNUSED,
              void *           arg  FD_PARAM_UNUSED ) {
  /* Intentionally empty */
}

static void
noop_send( uchar const *              msg  FD_PARAM_UNUSED,
           size_t                     sz   FD_PARAM_UNUSED,
           fd_gossip_peer_addr_t const * addr FD_PARAM_UNUSED,
           void *                     arg  FD_PARAM_UNUSED ) {
  /* Intentionally empty */
}

static void
noop_sign( void *        ctx        FD_PARAM_UNUSED,
           uchar         signature[ static 64 ],
           uchar const * buffer     FD_PARAM_UNUSED,
           ulong         len        FD_PARAM_UNUSED,
           int           sign_type  FD_PARAM_UNUSED ) {
  memset( signature, 0, 64UL );
}

static void
assert_socket_matches( fd_contact_info_t const *       contact,
                       uchar                           socket_tag,
                       fd_gossip_peer_addr_t const *   expected ) {
  fd_gossip_socket_addr_t actual;
  FD_TEST( fd_contact_info_get_socket_addr( contact, socket_tag, &actual )==0 );
  FD_TEST( actual.discriminant==fd_gossip_socket_addr_enum_ip4 );
  FD_TEST( actual.inner.ip4.addr==expected->addr );
  FD_TEST( actual.inner.ip4.port==expected->port );
}

static void
test_contact_info_switch( void ) {
  fd_pubkey_t identity;
  memset( &identity, 0, sizeof(identity) );

  fd_gossip_peer_addr_t gossip_addr = {
    .addr = FD_IP4_ADDR( 127, 0, 0, 1 ),
    .port = fd_ushort_bswap( (ushort)8000 )
  };

  fd_gossip_config_t config = {
    .public_key    = &identity,
    .node_outset   = 0,
    .my_addr       = gossip_addr,
    .my_version    = (fd_gossip_version_v3_t){ 0 },
    .shred_version = 1U,
    .deliver_fun   = noop_deliver,
    .deliver_arg   = NULL,
    .send_fun      = noop_send,
    .send_arg      = NULL,
    .sign_fun      = noop_sign,
    .sign_arg      = NULL
  };

  fd_gossip_peer_addr_t default_tpu = {
    .addr = FD_IP4_ADDR( 10, 0, 0, 1 ),
    .port = fd_ushort_bswap( (ushort)9000 )
  };
  fd_gossip_peer_addr_t default_tpu_quic = {
    .addr = FD_IP4_ADDR( 10, 0, 0, 1 ),
    .port = fd_ushort_bswap( (ushort)9001 )
  };

  fd_gossip_peer_addr_t bam_tpu = {
    .addr = FD_IP4_ADDR( 192, 168, 10, 11 ),
    .port = fd_ushort_bswap( (ushort)1000 )
  };
  fd_gossip_peer_addr_t bam_tpu_quic = {
    .addr = FD_IP4_ADDR( 192, 168, 10, 12 ),
    .port = fd_ushort_bswap( (ushort)1001 )
  };

  void * mem = NULL;
  FD_TEST( !posix_memalign( &mem, fd_gossip_align(), fd_gossip_footprint() ) );

  fd_gossip_t * gossip = fd_gossip_join( fd_gossip_new( mem, 123UL ) );
  FD_TEST( gossip );

  FD_TEST( !fd_gossip_set_config( gossip, &config ) );
  FD_TEST( !fd_gossip_update_tpu_addr( gossip, &default_tpu, &default_tpu_quic ) );

  fd_contact_info_t const * my_contact = fd_gossip_get_my_contact( gossip );
  FD_TEST( my_contact );
  assert_socket_matches( my_contact, FD_GOSSIP_SOCKET_TAG_TPU, &default_tpu );
  assert_socket_matches( my_contact, FD_GOSSIP_SOCKET_TAG_TPU_QUIC, &default_tpu_quic );

  for( int round = 0; round<3; round++ ) {
    FD_TEST( !fd_gossip_update_tpu_addr( gossip, &bam_tpu, &bam_tpu_quic ) );
    my_contact = fd_gossip_get_my_contact( gossip );
    assert_socket_matches( my_contact, FD_GOSSIP_SOCKET_TAG_TPU, &bam_tpu );
    assert_socket_matches( my_contact, FD_GOSSIP_SOCKET_TAG_TPU_QUIC, &bam_tpu_quic );

    FD_TEST( !fd_gossip_update_tpu_addr( gossip, &default_tpu, &default_tpu_quic ) );
    my_contact = fd_gossip_get_my_contact( gossip );
    assert_socket_matches( my_contact, FD_GOSSIP_SOCKET_TAG_TPU, &default_tpu );
    assert_socket_matches( my_contact, FD_GOSSIP_SOCKET_TAG_TPU_QUIC, &default_tpu_quic );
  }

  fd_gossip_delete( fd_gossip_leave( gossip ) );
  free( mem );
}

int
main( int argc, char ** argv ) {
  fd_boot( &argc, &argv );
  test_contact_info_switch();
  fd_halt();
  return 0;
}
