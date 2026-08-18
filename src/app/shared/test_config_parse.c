#include "fd_config_private.h"
#include "../../ballet/toml/fd_toml.h"
#include "../../disco/bam/fd_bam_types.h"

static char const cfg_str_1[] =
  "[gossip]\n"
  "  entrypoints = [\"208.91.106.45:8080\"]";

static char const cfg_str_2[] =
  "wumbo = \"mini\"";

/* Auto config specific */
static char const cfg_str_auto[] =
  "[net.xdp]\n  xdp_zero_copy = \"auto\"\n  native_bond = \"auto\"";
static char const cfg_str_auto_invalid[] =
  "[net.xdp]\n  xdp_zero_copy = \"something wrong\"";

static char const cfg_str_bam[] =
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

static char const cfg_str_bam_invalid[] =
  "[tiles.bam]\n"
  "  dump_bam_txns = true\n";

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

  /* Reject unrecognized config keys */

  memset( config, 0, sizeof(config_t) );
  pod = fd_pod_join( fd_pod_new( pod_mem, sizeof(pod_mem) ) );
  FD_TEST( fd_toml_parse( cfg_str_2, sizeof(cfg_str_2)-1, pod, scratch, sizeof(scratch), NULL ) == FD_TOML_SUCCESS );
  FD_TEST( !fd_config_extract_pod( pod, config ) );

  /* BAM development settings should be distinct from bundle settings */

  memset( config, 0, sizeof(config_t) );
  pod = fd_pod_join( fd_pod_new( pod_mem, sizeof(pod_mem) ) );
  FD_TEST( fd_toml_parse( cfg_str_bam, sizeof(cfg_str_bam)-1, pod, scratch, sizeof(scratch), NULL ) == FD_TOML_SUCCESS );
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
  FD_TEST( fd_toml_parse( cfg_str_bam_invalid, sizeof(cfg_str_bam_invalid)-1, pod, scratch, sizeof(scratch), NULL ) == FD_TOML_SUCCESS );
  FD_TEST( !fd_config_extract_pod( pod, config ) );

  /* The default config must parse fine */

  memset( config, 0, sizeof(config_t) );
  pod = fd_pod_join( fd_pod_new( pod_mem, sizeof(pod_mem) ) );
  FD_TEST( fd_toml_parse( fdctl_default_config, fdctl_default_config_sz, pod, scratch, sizeof(scratch), NULL ) == FD_TOML_SUCCESS );
  FD_TEST( fd_config_extract_pod( pod, config ) == config );
  FD_TEST( ((ulong)config->development.bam.buffer_size_kib<<10)==FD_BAM_GRPC_DEFAULT_BUF_SZ );
  fd_config_validate( config );  /* exits process with code 1 on failure */

  /* BAM has a dedicated verify-output ring, independent of TPU receive depth. */
  config->tiles.bam.enabled                = 1;
  config->tiles.verify.receive_buffer_size = 1U;
  fd_config_validate( config );
  FD_TEST( fd_config_extract_pod( pod, config )==config );
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

  /* Test passing "auto" leads to 2 for auto configure fields */

  memset( config, 0, sizeof(config_t) );
  pod = fd_pod_join( fd_pod_new( pod_mem, sizeof(pod_mem) ) );
  FD_TEST( fd_toml_parse( cfg_str_auto, sizeof(cfg_str_auto)-1, pod, scratch, sizeof(scratch), NULL ) == FD_TOML_SUCCESS );
  FD_TEST( fd_config_extract_pod( pod, config ) == config );
  FD_TEST( config->net.xdp.xdp_zero_copy == 2 );
  FD_TEST( config->net.xdp.native_bond   == 2 );

  pod = fd_pod_join( fd_pod_new( pod_mem, sizeof(pod_mem) ) );
  FD_TEST( fd_toml_parse( cfg_str_auto_invalid, sizeof(cfg_str_auto_invalid)-1, pod, scratch, sizeof(scratch), NULL ) == FD_TOML_SUCCESS );
  FD_TEST( !fd_config_extract_pod( pod, config ) );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
}
