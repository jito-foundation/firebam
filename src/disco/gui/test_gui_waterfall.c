#include "fd_gui.h"
#include "fd_gui_printf.h"

#include "../metrics/fd_metrics.h"
#include "../../waltz/http/fd_http_server_private.h"

#include <stdlib.h>

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  void * metrics_mem = aligned_alloc( FD_METRICS_ALIGN, FD_METRICS_FOOTPRINT( 0UL ) );
  FD_TEST( metrics_mem );
  ulong * metrics = fd_metrics_join( fd_metrics_new( metrics_mem, 0UL ) );
  FD_TEST( metrics );
  fd_metrics_tile( metrics )[ MIDX( COUNTER, BAM, TRANSACTION_PUBLISHED ) ] = 23UL;

  static fd_topo_t topo[1]; /* too large for the default thread stack */
  topo->tile_cnt = 1UL;
  fd_memcpy( topo->tiles[ 0 ].name, "bam", 4UL );
  topo->tiles[ 0 ].metrics = metrics;

  fd_gui_t * gui = calloc( 1UL, sizeof(fd_gui_t) );
  FD_TEST( gui );
  gui->topo = topo;

  fd_gui_txn_waterfall_t waterfall = {0};
  fd_gui_txn_waterfall_snap( gui, &waterfall );
  FD_TEST( waterfall.in.bam==23UL );

  fd_gui_tile_stats_t stats = {0};
  fd_gui_tile_stats_snap( gui, &waterfall, &stats, 42L );
  FD_TEST( stats.verify_total_cnt==23UL );

  fd_http_server_params_t params = {
    .max_connection_cnt    = 1UL,
    .max_ws_connection_cnt = 0UL,
    .max_request_len       = 1024UL,
    .max_ws_recv_frame_len = 2048UL,
    .max_ws_send_frame_cnt = 1UL,
    .outgoing_buffer_sz    = 16384UL,
  };
  fd_http_server_callbacks_t callbacks = {0};
  void * http_mem = aligned_alloc( fd_http_server_align(), fd_http_server_footprint( params ) );
  FD_TEST( http_mem );
  fd_http_server_t * http = fd_http_server_join( fd_http_server_new( http_mem, params, callbacks, NULL ) );
  FD_TEST( http );
  gui->http = http;

  fd_gui_txn_waterfall_t prev = {0};
  fd_gui_printf_live_txn_waterfall( gui, &prev, &waterfall, 0UL );

  fd_http_server_response_t response = {0};
  FD_TEST( !fd_http_server_stage_body( http, &response ) );
  FD_TEST( response._body_len<params.outgoing_buffer_sz );
  char * body = malloc( response._body_len+1UL );
  FD_TEST( body );
  fd_memcpy( body, http->oring+(response._body_off%http->oring_sz), response._body_len );
  body[ response._body_len ] = '\0';
  FD_TEST( strstr( body, "\"waterfall\":{\"in\":" ) );
  FD_TEST( strstr( body, "\"bam\":23" ) );

  free( body );
  free( fd_http_server_delete( fd_http_server_leave( http ) ) );
  free( gui );
  free( fd_metrics_delete( fd_metrics_leave( metrics ) ) );

  fd_halt();
  return 0;
}
