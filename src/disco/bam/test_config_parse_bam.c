/* FireBAM config parse tests.  These need no fixture from
   test_config_parse.c (whose main falls off its end, so it cannot be
   included under a renamed main). */

#include "../../app/shared/fd_config_private.h"
#include "../../ballet/toml/fd_toml.h"
#include "fd_bam_types.h"

extern uchar const fdctl_default_config[];
extern ulong const fdctl_default_config_sz;

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

static void
test_bam_config( void ) {
  static uchar    pod_mem[ 1UL<<16 ];
  static uchar    scratch[ 4096 ];
  static config_t config[1];
  uchar * pod;

  /* BAM development settings should be distinct from bundle settings */

  memset( config, 0, sizeof(config_t) );
  pod = fd_pod_join( fd_pod_new( pod_mem, sizeof(pod_mem) ) );
  FD_TEST( fd_toml_parse( cfg_str_bam, sizeof(cfg_str_bam)-1, pod, scratch, sizeof(scratch), NULL ) == FD_TOML_SUCCESS );
  FD_TEST( fd_config_extract_pod( pod, config ) == config );

  FD_TEST( config->development.bundle.buffer_size_kib == 111U );
  FD_TEST( 0==strcmp( config->development.bundle.ssl_key_log_file, "/tmp/bundle.keys" ) );

  FD_TEST( config->development.bam.buffer_size_kib == 222U );
  FD_TEST( 0==strcmp( config->development.bam.ssl_key_log_file, "/tmp/bam.keys" ) );
  FD_TEST( config->development.bam.dump_bam_txns );
  FD_TEST( config->development.bam.dump_bam_slot_first_txn );

  /* BAM dump controls were moved out of [tiles.bam] */

  memset( config, 0, sizeof(config_t) );
  pod = fd_pod_join( fd_pod_new( pod_mem, sizeof(pod_mem) ) );
  FD_TEST( fd_toml_parse( cfg_str_bam_invalid, sizeof(cfg_str_bam_invalid)-1, pod, scratch, sizeof(scratch), NULL ) == FD_TOML_SUCCESS );
  FD_TEST( !fd_config_extract_pod( pod, config ) );

  /* BAM defaults: gRPC buffer floor, and BAM has a dedicated
     verify-output ring, independent of TPU receive depth. */

  memset( config, 0, sizeof(config_t) );
  pod = fd_pod_join( fd_pod_new( pod_mem, sizeof(pod_mem) ) );
  FD_TEST( fd_toml_parse( fdctl_default_config, fdctl_default_config_sz, pod, scratch, sizeof(scratch), NULL ) == FD_TOML_SUCCESS );
  FD_TEST( fd_config_extract_pod( pod, config ) == config );
  FD_TEST( ((ulong)config->development.bam.buffer_size_kib<<10)==FD_BAM_GRPC_MIN_BUF_SZ );
  config->tiles.bam.enabled                = 1;
  config->tiles.verify.receive_buffer_size = 1U;
  fd_config_validate( config );
  FD_TEST( fd_config_extract_pod( pod, config )==config );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );
  test_bam_config();
  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
