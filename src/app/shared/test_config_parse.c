#include "fd_config_private.h"
#include "../../ballet/toml/fd_toml.h"

#include <netdb.h>

static struct sockaddr_in test_resolver_addr[ 2 ];
static struct addrinfo    test_resolver_result[ 2 ];
static ulong              test_resolver_call_cnt;
static ulong              test_resolver_free_cnt;

int
getaddrinfo( char const * restrict          node,
             char const * restrict          service,
             struct addrinfo const * restrict hints,
             struct addrinfo ** restrict    result ) {
  FD_TEST( !strcmp( node, "multi-a.test" ) );
  FD_TEST( !service );
  FD_TEST( hints && hints->ai_family==AF_INET );

  fd_memset( test_resolver_addr,   0, sizeof(test_resolver_addr) );
  fd_memset( test_resolver_result, 0, sizeof(test_resolver_result) );
  test_resolver_addr[ 0 ].sin_family      = AF_INET;
  test_resolver_addr[ 0 ].sin_addr.s_addr = FD_IP4_ADDR( 192, 0, 2, 10 );
  test_resolver_addr[ 1 ].sin_family      = AF_INET;
  test_resolver_addr[ 1 ].sin_addr.s_addr = FD_IP4_ADDR( 192, 0, 2, 11 );
  for( ulong i=0UL; i<2UL; i++ ) {
    test_resolver_result[ i ].ai_family  = AF_INET;
    test_resolver_result[ i ].ai_addrlen = sizeof(struct sockaddr_in);
    test_resolver_result[ i ].ai_addr    = (struct sockaddr *)&test_resolver_addr[ i ];
  }
  test_resolver_result[ 0 ].ai_next = &test_resolver_result[ 1 ];
  *result = test_resolver_result;
  test_resolver_call_cnt++;
  return 0;
}

void
freeaddrinfo( struct addrinfo * result ) {
  FD_TEST( result==test_resolver_result );
  test_resolver_free_cnt++;
}

static char const cfg_str_1[] =
  "[gossip]\n"
  "  entrypoints = [\"208.91.106.45:8080\"]";

static char const cfg_str_2[] =
  "wumbo = \"mini\"";

static char const cfg_str_3[] =
  "[development.bundle]\n"
  "  buffer_size_kib = 111\n"
  "  ssl_heap_size_mib = 64\n"
  "  ssl_key_log_file = \"/tmp/bundle.keys\"\n"
  "[development.bam]\n"
  "  buffer_size_kib = 222\n"
  "  ssl_heap_size_mib = 128\n"
  "  ssl_key_log_file = \"/tmp/bam.keys\"\n"
  "  dump_bam_txns = true\n"
  "  dump_bam_slot_first_txn = true\n";

static char const cfg_str_4[] =
  "[tiles.bam]\n"
  "  dump_bam_txns = true\n";

static char const cfg_str_5[] =
  "[tiles.shred]\n"
  "  additional_shred_destinations_retransmit = [\"retransmit-archive.receiver.validator.example.com:12000\"]\n"
  "  additional_shred_destinations_leader = [\"leader-archive.receiver.validator.example.com:13000\"]\n";

extern uchar const fdctl_default_config[];
extern ulong const fdctl_default_config_sz;

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  /* Parse a basic config string */

  static uchar pod_mem[ 1UL<<16 ];
  uchar * pod = fd_pod_join( fd_pod_new( pod_mem, sizeof(pod_mem) ) );

  static uchar scratch[ 4096 ];
  FD_TEST( fd_toml_parse( cfg_str_1, sizeof(cfg_str_1)-1, pod, scratch, sizeof(scratch), NULL ) == FD_TOML_SUCCESS );

  static config_t config[1];
  FD_TEST( fd_config_extract_pod( pod, config ) == config );

  FD_TEST( config->gossip.entrypoints_cnt == 1 );
  FD_TEST( 0==strcmp( config->gossip.entrypoints[0], "208.91.106.45:8080" ) );

  /* Additional shred destinations preserve hostnames longer than the
     previous numeric IPv4 endpoint representation. */

  memset( config, 0, sizeof(config_t) );
  pod = fd_pod_join( fd_pod_new( pod_mem, sizeof(pod_mem) ) );
  FD_TEST( fd_toml_parse( cfg_str_5, sizeof(cfg_str_5)-1, pod, scratch, sizeof(scratch), NULL ) == FD_TOML_SUCCESS );
  FD_TEST( fd_config_extract_pod( pod, config ) == config );
  FD_TEST( config->tiles.shred.additional_shred_destinations_retransmit_cnt==1UL );
  FD_TEST( config->tiles.shred.additional_shred_destinations_leader_cnt==1UL );
  FD_TEST( !strcmp( config->tiles.shred.additional_shred_destinations_retransmit[ 0 ],
                    "retransmit-archive.receiver.validator.example.com:12000" ) );
  FD_TEST( !strcmp( config->tiles.shred.additional_shred_destinations_leader[ 0 ],
                    "leader-archive.receiver.validator.example.com:13000" ) );

  /* Endpoint parsing is strict.  The test resolver returns two IPv4
     records so selection is deterministic and does not require DNS. */

  fd_topo_ip_port_t endpoint;
  FD_TEST( fd_config_resolve_ip4_endpoint( "198.51.100.42:1", &endpoint ) );
  FD_TEST( endpoint.ip==FD_IP4_ADDR( 198, 51, 100, 42 ) );
  FD_TEST( endpoint.port==1U );
  FD_TEST( !test_resolver_call_cnt );

  fd_cstr_ncpy( config->tiles.shred.additional_shred_destinations_retransmit[ 0 ], "multi-a.test:65535",
                sizeof(config->tiles.shred.additional_shred_destinations_retransmit[ 0 ]) );
  fd_cstr_ncpy( config->tiles.shred.additional_shred_destinations_leader[ 0 ], "198.51.100.42:12000",
                sizeof(config->tiles.shred.additional_shred_destinations_leader[ 0 ]) );
  fd_memset( &config->topo, 0, sizeof(config->topo) );
  config->topo.tile_cnt = 2UL;
  fd_cstr_ncpy( config->topo.tiles[ 0 ].name, "shred", sizeof(config->topo.tiles[ 0 ].name) );
  fd_cstr_ncpy( config->topo.tiles[ 1 ].name, "shred", sizeof(config->topo.tiles[ 1 ].name) );
  fd_config_apply_shred_destinations( config, &config->topo );

  FD_TEST( test_resolver_call_cnt==1UL );
  FD_TEST( test_resolver_free_cnt==1UL );
  for( ulong i=0UL; i<2UL; i++ ) {
    FD_TEST( config->topo.tiles[ i ].shred.adtl_dests_retransmit_cnt==1UL );
    FD_TEST( config->topo.tiles[ i ].shred.adtl_dests_retransmit[ 0 ].ip==FD_IP4_ADDR( 192, 0, 2, 10 ) );
    FD_TEST( config->topo.tiles[ i ].shred.adtl_dests_retransmit[ 0 ].port==65535U );
    FD_TEST( config->topo.tiles[ i ].shred.adtl_dests_leader_cnt==1UL );
    FD_TEST( config->topo.tiles[ i ].shred.adtl_dests_leader[ 0 ].ip==FD_IP4_ADDR( 198, 51, 100, 42 ) );
    FD_TEST( config->topo.tiles[ i ].shred.adtl_dests_leader[ 0 ].port==12000U );
  }

  FD_TEST( !fd_config_resolve_ip4_endpoint( "127.0.0.1:0",     &endpoint ) );
  FD_TEST( !fd_config_resolve_ip4_endpoint( "127.0.0.1:65536", &endpoint ) );
  FD_TEST( !fd_config_resolve_ip4_endpoint( "127.0.0.1:12x",   &endpoint ) );
  FD_TEST( !fd_config_resolve_ip4_endpoint( ":1234",           &endpoint ) );
  FD_TEST( !fd_config_resolve_ip4_endpoint( "127.0.0.1:",      &endpoint ) );
  FD_TEST( !fd_config_resolve_ip4_endpoint( "127.0.0.1",       &endpoint ) );
  char oversized_hostname[ 259UL ];
  memset( oversized_hostname, 'a', 256UL );
  fd_memcpy( oversized_hostname+256UL, ":1", 3UL );
  FD_TEST( !fd_config_resolve_ip4_endpoint( oversized_hostname, &endpoint ) );

  /* Reject unrecognized config keys */

  memset( config, 0, sizeof(config_t) );
  pod = fd_pod_join( fd_pod_new( pod_mem, sizeof(pod_mem) ) );
  FD_TEST( fd_toml_parse( cfg_str_2, sizeof(cfg_str_2)-1, pod, scratch, sizeof(scratch), NULL ) == FD_TOML_SUCCESS );
  FD_TEST( !fd_config_extract_pod( pod, config ) );

  /* BAM development settings should be distinct from bundle settings */

  memset( config, 0, sizeof(config_t) );
  pod = fd_pod_join( fd_pod_new( pod_mem, sizeof(pod_mem) ) );
  FD_TEST( fd_toml_parse( cfg_str_3, sizeof(cfg_str_3)-1, pod, scratch, sizeof(scratch), NULL ) == FD_TOML_SUCCESS );
  FD_TEST( fd_config_extract_pod( pod, config ) == config );

  FD_TEST( config->development.bundle.buffer_size_kib == 111U );
  FD_TEST( 0==strcmp( config->development.bundle.ssl_key_log_file, "/tmp/bundle.keys" ) );
  FD_TEST( config->development.bundle.ssl_heap_size_mib == 64U );

  FD_TEST( config->development.bam.buffer_size_kib == 222U );
  FD_TEST( config->development.bam.ssl_heap_size_mib == 128U );
  FD_TEST( 0==strcmp( config->development.bam.ssl_key_log_file, "/tmp/bam.keys" ) );
  FD_TEST( config->development.bam.dump_bam_txns );
  FD_TEST( config->development.bam.dump_bam_slot_first_txn );

  /* BAM dump controls were moved out of [tiles.bam] */

  memset( config, 0, sizeof(config_t) );
  pod = fd_pod_join( fd_pod_new( pod_mem, sizeof(pod_mem) ) );
  FD_TEST( fd_toml_parse( cfg_str_4, sizeof(cfg_str_4)-1, pod, scratch, sizeof(scratch), NULL ) == FD_TOML_SUCCESS );
  FD_TEST( !fd_config_extract_pod( pod, config ) );

  /* The default config must parse fine */

  memset( config, 0, sizeof(config_t) );
  pod = fd_pod_join( fd_pod_new( pod_mem, sizeof(pod_mem) ) );
  FD_TEST( fd_toml_parse( fdctl_default_config, fdctl_default_config_sz, pod, scratch, sizeof(scratch), NULL ) == FD_TOML_SUCCESS );
  FD_TEST( fd_config_extract_pod( pod, config ) == config );
  fd_config_validate( config );  /* exits process with code 1 on failure */

  /* Ensure we can selectively override a field */

  config->gossip.port = 9191;
  config->gossip.entrypoints_cnt = 2;
  strcpy( config->gossip.entrypoints[0], "foo" );
  strcpy( config->gossip.entrypoints[1], "bar" );
  pod = fd_pod_join( fd_pod_new( pod_mem, sizeof(pod_mem) ) );
  FD_TEST( fd_toml_parse( cfg_str_1, sizeof(cfg_str_1)-1, pod, scratch, sizeof(scratch), NULL ) == FD_TOML_SUCCESS );
  FD_TEST( fd_config_extract_pod( pod, config ) == config );
  FD_TEST( config->gossip.entrypoints_cnt == 1 );
  FD_TEST( 0==strcmp( config->gossip.entrypoints[0], "208.91.106.45:8080" ) );
  FD_TEST( config->gossip.port == 9191 );  /* unchanged */

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
}
