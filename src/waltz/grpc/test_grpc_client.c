#if !FD_HAS_HOSTED

#include "../../util/fd_util.h"


int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );
  FD_LOG_WARNING(( "skip: unit test requires FD_HAS_HOSTED" ));
  fd_halt();
  return 0;
}

#else

#include "fd_grpc_client_private.h"
#include "../../util/tmpl/fd_unit_test.c"

#include <errno.h>
#include <sys/socket.h>
#include <unistd.h>

static long g_mono_now;
static uint g_mono_jump_after;
static long g_mono_jump_ns = 5000000000L;
static volatile ulong g_mono_reads;
static int g_after_entry_fd = -1;
static uchar g_after_entry_wire[128];
static ulong g_after_entry_wire_sz;
static uint g_after_entry_cnt;
long fd_grpc_client_mono_now( void ) {
  g_mono_reads++;
  if( g_mono_jump_after && !--g_mono_jump_after ) g_mono_now+=g_mono_jump_ns;
  long sampled = g_mono_now;
  if( g_after_entry_fd>=0 ) {
    int peer = g_after_entry_fd;
    g_after_entry_fd = -1;
    g_mono_now+=6000000000L;
    FD_TEST(write(peer,g_after_entry_wire,g_after_entry_wire_sz)==(long)g_after_entry_wire_sz);
    g_after_entry_cnt++;
  }
  return sampled;
}

typedef struct {
  uchar unused;
} test_empty_msg_t;

#define test_Empty_FIELDLIST(X, a)
#define test_Empty_CALLBACK NULL
#define test_Empty_DEFAULT  NULL
PB_BIND( test_Empty, test_empty_msg_t, AUTO )

static fd_grpc_client_t * client;

/* test_grpc_client_mock_conn injects a fake connection state into the
   gRPC client. */

static void
test_grpc_client_mock_conn( fd_grpc_client_t * client ) {
  client->h2_hs_done  = 1;
  client->conn->flags = 0;
}


static ulong  g_cb_request_ctx;

static ulong g_rx_start_cnt;
static int   g_rx_start_fill_tx;
static int   g_reset_callback;
static fd_grpc_h2_stream_t * g_replacement_stream;

static void
cb_rx_start( void * app_ctx,
             ulong  request_ctx ) {
  (void)app_ctx;
  g_cb_request_ctx = request_ctx;
  g_rx_start_cnt++;
  if( g_reset_callback==1 ) {
    fd_grpc_client_reset( client );
    client->h2_hs_done=1; client->conn->flags=0;
    g_replacement_stream=fd_grpc_client_stream_acquire(client,5678UL);
    return;
  }
  while( g_rx_start_fill_tx && fd_h2_rbuf_free_sz( client->frame_tx ) ) fd_h2_rbuf_push( client->frame_tx, "", 1UL );
}

static ulong g_rx_end_cnt;
static int   g_rx_end_fill_tx;
static fd_grpc_resp_hdrs_t g_cb_resp_hdrs;

static void
cb_rx_end( void * app_ctx,
           ulong  request_ctx,
           fd_grpc_resp_hdrs_t * resp_hdrs ) {
  (void)app_ctx;
  g_cb_request_ctx = request_ctx;
  g_cb_resp_hdrs   = *resp_hdrs;
  g_rx_end_cnt++;
  if( g_reset_callback==3 ) {
    fd_grpc_client_reset(client);
    client->h2_hs_done=1; client->conn->flags=0;
    g_replacement_stream=fd_grpc_client_stream_acquire(client,5678UL);
    return;
  }
  while( g_rx_end_fill_tx && fd_h2_rbuf_free_sz( client->frame_tx ) ) fd_h2_rbuf_push( client->frame_tx, "", 1UL );
}

static ulong g_rx_msg_cnt;
static fd_grpc_h2_stream_t * g_rx_msg_closed_stream;

static void
cb_rx_msg( void *       app_ctx,
           void const * protobuf,
           ulong        protobuf_sz,
           ulong        request_ctx ) {
  (void)app_ctx; (void)protobuf; (void)protobuf_sz; (void)request_ctx;
  g_rx_msg_cnt++;
  if( g_reset_callback==2 ) {
    fd_grpc_client_reset(client);
    client->h2_hs_done=1; client->conn->flags=0;
    g_replacement_stream=fd_grpc_client_stream_acquire(client,5678UL);
    return;
  }
  if( !g_rx_msg_closed_stream ) return;
  FD_TEST( g_rx_msg_closed_stream->s.state==FD_H2_STREAM_STATE_CLOSED );
  ulong const tx_used = fd_h2_rbuf_used_sz( client->frame_tx );
  ulong const pending = client->request_tx_op->chunk_sz;
  client->conn->tx_wnd = g_rx_msg_closed_stream->s.tx_wnd = 100U;
  fd_h2_tx_op_copy( client->conn, &g_rx_msg_closed_stream->s, client->frame_tx, client->request_tx_op );
  FD_TEST( fd_h2_rbuf_used_sz( client->frame_tx )==tx_used && client->request_tx_op->chunk_sz==pending );
  while( fd_h2_rbuf_free_sz( client->frame_tx ) ) fd_h2_rbuf_push( client->frame_tx, "", 1UL );
}

static int g_rx_timeout_fill_tx;
static int g_timeout_acquire_kind;
static int g_timeout_seed_rx;
static ulong g_timeout_acquire_cnt;
static fd_grpc_h2_stream_t * g_timeout_old;
static fd_grpc_h2_stream_t * g_timeout_new;
static void timeout_acquire_request( void );

static struct {
  int    deadline_kind;
} g_timeout_details;

static void
cb_rx_timeout( void * app_ctx,
               ulong  request_ctx,
               int    deadline_kind ) {
  (void)app_ctx;
  g_cb_request_ctx = request_ctx;
  g_timeout_details.deadline_kind = deadline_kind;
  if( g_timeout_acquire_kind ) timeout_acquire_request();
  while( g_rx_timeout_fill_tx && fd_h2_rbuf_free_sz( client->frame_tx ) ) fd_h2_rbuf_push( client->frame_tx, "", 1UL );
}

static ulong g_conn_dead_cnt;
static uint g_conn_dead_error;
static fd_h2_frame_hdr_t
test_incomplete_headers( uint flags, uint id ) {
  return (fd_h2_frame_hdr_t){ .typlen=fd_h2_frame_typlen(FD_H2_FRAME_TYPE_HEADERS,100UL),
                             .flags=(uchar)flags,.r_stream_id=fd_uint_bswap(id) };
}

static void
cb_conn_dead( void * ctx, uint err, int closed_by ) {
  (void)ctx; (void)closed_by;
  g_conn_dead_cnt++;
  g_conn_dead_error = err;
  if( g_reset_callback==4 ) {
    fd_grpc_client_reset( client );
    test_grpc_client_mock_conn( client );
    g_replacement_stream=fd_grpc_client_stream_acquire( client, 5678UL );
    /* Leave a new connection's incomplete frame buffered.  An old padded
       frame must not skip any of these bytes after the reset callback. */
    fd_h2_frame_hdr_t hdr=test_incomplete_headers( 0U, 1U );
    fd_h2_rbuf_push( client->frame_rx, &hdr, sizeof(hdr) );
  }
}

static void
test_rx_frame( uint          type,
               uint          flags,
               uint          stream_id,
               uchar const * data,
               ulong         data_sz ) {
  fd_h2_tx( client->frame_rx, data, data_sz, type, flags, stream_id );
  fd_h2_rx( client->conn, client->frame_rx, client->frame_tx, client->frame_scratch,
            client->frame_scratch_max, &fd_grpc_client_h2_callbacks );
}

FD_UNIT_TEST( header_deadline ) {
  fd_grpc_client_reset( client );
  test_grpc_client_mock_conn( client );
  client->conn->peer_settings.max_concurrent_streams = 1U;

  /* Deadline should not fire prior to expiration */
  FD_TEST( fd_grpc_client_stream_acquire_is_safe( client ) );
  fd_grpc_h2_stream_t * stream = fd_grpc_client_stream_acquire( client, 0UL );
  long const deadline = 1234L;
  fd_grpc_client_deadline_set( stream, FD_GRPC_DEADLINE_HEADER, deadline );
  fd_grpc_client_service_streams( client, deadline-1L );
  FD_TEST( client->stream_cnt==1 );

  /* Deadline should deactivate after headers were received */
  fd_grpc_h2_cb_headers( client->conn, &stream->s, "\x88", 1UL, FD_H2_FLAG_END_HEADERS );
  fd_grpc_client_service_streams( client, deadline+1L );
  FD_TEST( client->stream_cnt==1 );
  fd_h2_stream_reset( &stream->s, client->conn );
  fd_grpc_client_stream_release( client, stream );
  FD_TEST( client->stream_cnt==0 );

  /* Test deadline firing */
  FD_TEST( fd_grpc_client_stream_acquire_is_safe( client ) );
  stream = fd_grpc_client_stream_acquire( client, 0UL );
  ulong const stream_id = stream->s.stream_id;
  fd_grpc_client_deadline_set( stream, FD_GRPC_DEADLINE_HEADER, deadline );
  FD_TEST( client->stream_cnt==1 );
  FD_TEST( !fd_grpc_client_stream_acquire_is_safe( client ) );

  /* Deadlines fire mid field block, but not once the conn is closing */
  uchar const status = 0x88;
  test_rx_frame( FD_H2_FRAME_TYPE_HEADERS, 0U, (uint)stream_id, &status, 1UL );
  client->conn->flags |= FD_H2_CONN_FLAGS_SEND_GOAWAY;
  fd_grpc_client_service_streams( client, deadline+1L );
  FD_TEST( client->stream_cnt==1 );
  client->conn->flags &= (uchar)~FD_H2_CONN_FLAGS_SEND_GOAWAY;

  /* Queue the reset before rx_timeout fills the remaining TX space. */
  static uchar const filler[ 32768 ] = {0};
  ulong const prefix = client->frame_tx_buf_max-sizeof(fd_h2_rst_stream_t);
  fd_h2_rbuf_push( client->frame_tx, filler, prefix );
  g_rx_timeout_fill_tx = 1;
  fd_grpc_client_service_streams( client, deadline+1L );
  g_rx_timeout_fill_tx = 0;
  FD_TEST( client->stream_cnt==0 );
  FD_TEST( client->conn->stream_active_cnt[1]==0U );
  FD_TEST( fd_grpc_client_stream_acquire_is_safe( client ) );
  stream = NULL; /* already freed */

  FD_TEST( !fd_h2_rbuf_free_sz( client->frame_tx ) );
  fd_h2_rbuf_skip( client->frame_tx, prefix );
  fd_h2_rst_stream_t rst_stream;
  fd_h2_rbuf_pop_copy( client->frame_tx, &rst_stream, sizeof(fd_h2_rst_stream_t) );
  FD_TEST( rst_stream.hdr.typlen==fd_h2_frame_typlen( FD_H2_FRAME_TYPE_RST_STREAM, 4UL ) );
  FD_TEST( rst_stream.hdr.flags ==0 );
  FD_TEST( fd_uint_bswap( rst_stream.hdr.r_stream_id )==stream_id );
  FD_TEST( fd_uint_bswap( rst_stream.error_code      )==FD_H2_ERR_CANCEL );
  /* Late frames for the expired stream are dropped */
  test_rx_frame( FD_H2_FRAME_TYPE_CONTINUATION, FD_H2_FLAG_END_HEADERS, (uint)stream_id, &status, 1UL );
  test_rx_frame( FD_H2_FRAME_TYPE_HEADERS,      FD_H2_FLAG_END_HEADERS, (uint)stream_id, &status, 1UL );
  FD_TEST( !client->conn->flags && !client->conn->conn_error && fd_h2_rbuf_is_empty( client->frame_tx ) );
}

FD_UNIT_TEST( rx_end_deadline ) {
  fd_grpc_client_reset( client );
  test_grpc_client_mock_conn( client );
  client->conn->peer_settings.max_concurrent_streams = 1U;

  /* Deadline should not fire prior to expiration */
  FD_TEST( fd_grpc_client_stream_acquire_is_safe( client ) );
  fd_grpc_h2_stream_t * stream = fd_grpc_client_stream_acquire( client, 0UL );
  fd_h2_stream_close_tx( &stream->s, client->conn );
  long const deadline = 1234L;
  fd_grpc_client_deadline_set( stream, FD_GRPC_DEADLINE_RX_END, deadline );
  fd_grpc_client_service_streams( client, deadline-1L );
  FD_TEST( client->stream_cnt==1 );
  FD_TEST( !fd_grpc_client_stream_acquire_is_safe( client ) );

  /* Deadline should still fire after headers were received */
  fd_grpc_h2_cb_headers( client->conn, &stream->s, "\x88", 1UL, FD_H2_FLAG_END_HEADERS );
  fd_grpc_client_service_streams( client, deadline+1L );
  FD_TEST( client->stream_cnt==0 );
  FD_TEST( client->conn->stream_active_cnt[1]==0U );
  FD_TEST( fd_grpc_client_stream_acquire_is_safe( client ) );

  /* No frames on a stream closed by END_STREAM mid field block */
  fd_h2_rbuf_skip( client->frame_tx, fd_h2_rbuf_used_sz( client->frame_tx ) );
  stream = fd_grpc_client_stream_acquire( client, 0UL );
  fd_h2_stream_close_tx( &stream->s, client->conn );
  fd_grpc_client_deadline_set( stream, FD_GRPC_DEADLINE_RX_END, deadline );
  uchar const status = 0x88;
  test_rx_frame( FD_H2_FRAME_TYPE_HEADERS, FD_H2_FLAG_END_STREAM, stream->s.stream_id, &status, 1UL );
  stream->s.rx_wnd = 0U;
  fd_grpc_client_service_streams( client, deadline-1L );
  fd_grpc_client_service_streams( client, deadline+1L );
  FD_TEST( client->stream_cnt==0 && fd_h2_rbuf_is_empty( client->frame_tx ) );
}

FD_UNIT_TEST( rx_stream_quota ) {
  fd_grpc_client_reset( client );
  test_grpc_client_mock_conn( client );

  /* Client should replenish receive quota */
  FD_TEST( fd_grpc_client_stream_acquire_is_safe( client ) );
  fd_grpc_h2_stream_t * stream = fd_grpc_client_stream_acquire( client, 0UL );
  stream->s.rx_wnd = client->conn->self_settings.initial_window_size / 2 - 1;
  fd_grpc_client_service_streams( client, 0L );

  FD_TEST( fd_h2_rbuf_used_sz( client->frame_tx )==sizeof(fd_h2_window_update_t) );
  fd_h2_window_update_t window_update;
  fd_h2_rbuf_pop_copy( client->frame_tx, &window_update, sizeof(fd_h2_window_update_t) );
  FD_TEST( window_update.hdr.typlen==fd_h2_frame_typlen( FD_H2_FRAME_TYPE_WINDOW_UPDATE, 4UL ) );
  FD_TEST( window_update.hdr.flags==0 );
  FD_TEST( fd_uint_bswap( window_update.hdr.r_stream_id )==stream->s.stream_id );
  FD_TEST( fd_uint_bswap( window_update.increment )==client->conn->self_settings.initial_window_size / 2 + 2 );
}

FD_UNIT_TEST( initial_window_update ) {
  fd_grpc_client_reset( client );
  test_grpc_client_mock_conn( client );

  FD_TEST( fd_grpc_client_stream_acquire_is_safe( client ) );
  fd_grpc_h2_stream_t * s1 = fd_grpc_client_stream_acquire( client, 0UL );
  fd_grpc_h2_stream_t * s2 = fd_grpc_client_stream_acquire( client, 0UL );

  /* Positive delta grows every active stream and defers tx resumption
     to after fd_h2_rx */
  s1->s.tx_wnd = 100U;
  s2->s.tx_wnd = 200U;
  fd_grpc_h2_initial_window_update( client->conn, 50L );
  FD_TEST( s1->s.tx_wnd==150U && s1->tx_wnd_debt==0L );
  FD_TEST( s2->s.tx_wnd==250U && s2->tx_wnd_debt==0L );
  FD_TEST( client->window_update_pending==1U );
  client->window_update_pending = 0;

  /* Shrink below consumed credit: window 10, delta -20 -> effective -10.
     A WINDOW_UPDATE of 10 must yield effective 0, not 10. */
  s1->s.tx_wnd = 10U;
  s2->s.tx_wnd = 300U;
  fd_grpc_h2_initial_window_update( client->conn, -20L );
  FD_TEST( s1->s.tx_wnd==0U   && s1->tx_wnd_debt==10L );
  FD_TEST( s2->s.tx_wnd==280U && s2->tx_wnd_debt==0L  );
  FD_TEST( client->window_update_pending==0U ); /* no resumption on shrink */

  s1->s.tx_wnd += 10U; /* as fd_h2 does on WINDOW_UPDATE, before the callback */
  fd_grpc_h2_stream_window_update( client->conn, &s1->s, 10U );
  FD_TEST( s1->s.tx_wnd==0U && s1->tx_wnd_debt==0L );

  s1->s.tx_wnd += 25U;
  fd_grpc_h2_stream_window_update( client->conn, &s1->s, 25U );
  FD_TEST( s1->s.tx_wnd==25U && s1->tx_wnd_debt==0L );

  /* A raise past 2^31-1 on any stream is a connection error */
  s2->s.tx_wnd = 0x7fffffffU;
  fd_grpc_h2_initial_window_update( client->conn, 1L );
  FD_TEST( client->conn->flags & FD_H2_CONN_FLAGS_SEND_GOAWAY );
  FD_TEST( client->conn->conn_error==FD_H2_ERR_FLOW_CONTROL );
}

FD_UNIT_TEST( stream_release ) {
  fd_grpc_client_reset( client );
  test_grpc_client_mock_conn( client );
  fd_grpc_h2_stream_t * stream0 = fd_grpc_client_stream_acquire( client, 0UL );
  fd_grpc_h2_stream_t * stream1 = fd_grpc_client_stream_acquire( client, 1UL );
  fd_grpc_h2_stream_t * stream2 = fd_grpc_client_stream_acquire( client, 2UL );
  fd_grpc_h2_stream_t * stream3 = fd_grpc_client_stream_acquire( client, 3UL );
  FD_TEST( client->stream_cnt==4 );
  fd_grpc_client_stream_release( client, stream1 );
  FD_TEST( client->stream_cnt==3 );
  FD_TEST( client->stream_ids[ 0 ]==stream0->s.stream_id );
  FD_TEST( client->stream_ids[ 1 ]==stream3->s.stream_id );
  FD_TEST( client->stream_ids[ 2 ]==stream2->s.stream_id );
  fd_grpc_client_stream_release( client, stream2 );
  FD_TEST( client->stream_cnt==2 );
  FD_TEST( client->stream_ids[ 0 ]==stream0->s.stream_id );
  FD_TEST( client->stream_ids[ 1 ]==stream3->s.stream_id );
  fd_grpc_client_stream_release( client, stream0 );
  FD_TEST( client->stream_cnt==1 );
  FD_TEST( client->stream_ids[ 0 ]==stream3->s.stream_id );
  fd_grpc_client_stream_release( client, stream3 );
  FD_TEST( client->stream_cnt==0 );
}

FD_UNIT_TEST( stream_send_state ) {
  fd_grpc_client_reset( client );
  test_grpc_client_mock_conn( client );

  fd_grpc_h2_stream_t * stream = fd_grpc_client_stream_acquire( client, 0UL );
  FD_TEST( !fd_grpc_client_request_stream_busy( client ) );

  uchar payload = 0U;
  fd_h2_tx_op_init( client->request_tx_op, &payload, sizeof(payload), 0U );
  FD_TEST( fd_grpc_client_request_stream_busy( client ) );
  *client->request_tx_op = (fd_h2_tx_op_t){0};

  fd_h2_stream_reset( &stream->s, client->conn );
  test_empty_msg_t msg = {0};
  FD_TEST( !fd_grpc_client_stream_send_msg ( client, stream, &test_empty_msg_t_msg, &msg ) );
  FD_TEST( !fd_grpc_client_stream_send_msg1( client, stream, &payload, sizeof(payload) ) );

  stream->s.state = FD_H2_STREAM_STATE_CLOSING_TX;
  FD_TEST( !fd_grpc_client_stream_send_msg ( client, stream, &test_empty_msg_t_msg, &msg ) );
  FD_TEST( !fd_grpc_client_stream_send_msg1( client, stream, &payload, sizeof(payload) ) );

  FD_TEST( fd_h2_rbuf_is_empty( client->frame_tx ) );
  FD_TEST( !client->request_tx_op->chunk_sz );

  fd_grpc_client_stream_release( client, stream );
}

FD_UNIT_TEST( stream_close_state ) {
  fd_grpc_client_reset( client );
  test_grpc_client_mock_conn( client );

  fd_grpc_h2_stream_t * stream = fd_grpc_client_request_start1(
      client, "/test", 5UL, 0UL, NULL, 0UL, NULL, 0UL, 1 );
  FD_TEST( stream );
  FD_TEST( stream->s.state==FD_H2_STREAM_STATE_OPEN );
  FD_TEST( client->conn->stream_active_cnt[1]==1U );
  fd_h2_rbuf_skip( client->frame_tx, fd_h2_rbuf_used_sz( client->frame_tx ) );

  uchar payload = 0U;
  FD_TEST( fd_grpc_client_stream_send_msg1( client, stream, &payload, sizeof(payload) ) );
  FD_TEST( !client->request_stream );
  FD_TEST( !client->request_tx_op->chunk_sz );
  fd_h2_rbuf_skip( client->frame_tx, fd_h2_rbuf_used_sz( client->frame_tx ) );

  FD_TEST( fd_grpc_client_stream_close( client, stream ) );
  FD_TEST( stream->s.state==FD_H2_STREAM_STATE_CLOSING_TX );
  fd_h2_rbuf_skip( client->frame_tx, fd_h2_rbuf_used_sz( client->frame_tx ) );

  FD_TEST( !fd_grpc_client_stream_send_msg1( client, stream, &payload, sizeof(payload) ) );
  FD_TEST( !fd_grpc_client_stream_close( client, stream ) );

  fd_h2_stream_rx_data( &stream->s, client->conn, FD_H2_FLAG_END_STREAM );
  FD_TEST( stream->s.state==FD_H2_STREAM_STATE_CLOSED );
  FD_TEST( client->conn->stream_active_cnt[1]==0U );
  fd_grpc_client_stream_release( client, stream );
}

FD_UNIT_TEST( rx_headers ) {
  /* Header-only response */
  fd_grpc_client_reset( client );
  test_grpc_client_mock_conn( client );
  fd_grpc_h2_stream_t * stream = fd_grpc_client_stream_acquire( client, 0UL );
  FD_TEST( !stream->hdrs_received );
  stream->hdrs.is_grpc_proto = 1;
  stream->hdrs.h2_status = 200;
  fd_grpc_h2_cb_headers( client->conn, &stream->s, "\x88\x5f\x10" "application/grpc", 19UL, FD_H2_FLAG_END_HEADERS|FD_H2_FLAG_END_STREAM );
  FD_TEST( stream->hdrs_received );
  FD_TEST( g_rx_start_cnt==1 );
  FD_TEST( g_rx_end_cnt  ==1 );
  FD_TEST( client->stream_cnt==0 );

  /* Incomplete header frag */
  stream = fd_grpc_client_stream_acquire( client, 0UL );
  FD_TEST( !stream->hdrs_received );
  fd_grpc_h2_cb_headers( client->conn, &stream->s, NULL, 0UL, 0 );
  FD_TEST( !stream->hdrs_received );
  FD_TEST( g_rx_start_cnt==1 );
  FD_TEST( g_rx_end_cnt  ==1 );
  fd_grpc_client_stream_release( client, stream );

  /* Headers complete, data pending */
  stream = fd_grpc_client_stream_acquire( client, 0UL );
  FD_TEST( !stream->hdrs_received );
  stream->hdrs.is_grpc_proto = 1;
  stream->hdrs.h2_status = 200;
  fd_grpc_h2_cb_headers( client->conn, &stream->s, "\x88\x5f\x10" "application/grpc", 19UL, FD_H2_FLAG_END_HEADERS );
  FD_TEST( stream->hdrs_received );
  FD_TEST( g_rx_start_cnt==2 );
  FD_TEST( g_rx_end_cnt  ==1 );
  fd_grpc_client_stream_release( client, stream );

  /* Corrupt header */
  stream = fd_grpc_client_stream_acquire( client, 0UL );
  FD_TEST( !stream->hdrs_received );
  stream->hdrs.is_grpc_proto = 1;
  stream->hdrs.h2_status = 200;
  fd_grpc_h2_cb_headers( client->conn, &stream->s, "\x08\x03xyz", 5UL, FD_H2_FLAG_END_HEADERS );
  FD_TEST( g_rx_start_cnt==2 );
  FD_TEST( g_rx_end_cnt  ==2 ); /* FIXME does it make sense to issue rx_end without rx_start? */
  FD_TEST( client->stream_cnt==0 );
}

FD_UNIT_TEST( error_data_end_stream_releases_stream ) {
  fd_grpc_client_reset( client );
  test_grpc_client_mock_conn( client );

  fd_grpc_h2_stream_t * stream = fd_grpc_client_stream_acquire( client, 1234UL );
  stream->hdrs.h2_status     = 500U;
  stream->hdrs.is_grpc_proto = 0U;
  fd_h2_stream_close_tx( &stream->s, client->conn );
  fd_h2_stream_rx_data( &stream->s, client->conn, FD_H2_FLAG_END_STREAM );
  FD_TEST( client->conn->stream_active_cnt[1]==0U );

  ulong const rx_start_cnt = g_rx_start_cnt;
  ulong const rx_end_cnt   = g_rx_end_cnt;
  uchar const body[] = { 'o', 'o', 'p', 's' };
  fd_grpc_h2_cb_data( client->conn, &stream->s, body, sizeof(body), FD_H2_FLAG_END_STREAM );

  FD_TEST( g_rx_start_cnt==rx_start_cnt );
  FD_TEST( g_rx_end_cnt  ==rx_end_cnt+1UL );
  FD_TEST( g_cb_request_ctx==1234UL );
  FD_TEST( g_cb_resp_hdrs.h2_status==500U );
  FD_TEST( client->stream_cnt==0UL );
  FD_TEST( client->conn->stream_active_cnt[1]==0U );
}

FD_UNIT_TEST( empty_data_end_stream_releases_stream ) {
  fd_grpc_client_reset( client );
  test_grpc_client_mock_conn( client );

  fd_grpc_h2_stream_t * stream = fd_grpc_client_stream_acquire( client, 1234UL );
  stream->hdrs.h2_status     = 200U;
  stream->hdrs.is_grpc_proto = 1U;
  stream->hdrs_received = 1U;
  fd_h2_stream_close_tx( &stream->s, client->conn );
  fd_h2_stream_rx_data( &stream->s, client->conn, FD_H2_FLAG_END_STREAM );

  ulong const rx_end_cnt = g_rx_end_cnt;
  fd_grpc_h2_cb_data( client->conn, &stream->s, NULL, 0UL, FD_H2_FLAG_END_STREAM );

  FD_TEST( g_rx_end_cnt==rx_end_cnt+1UL );
  FD_TEST( client->stream_cnt==0UL );
  FD_TEST( client->conn->stream_active_cnt[1]==0U );
}

/* END_STREAM on a HEADERS frame continued by CONTINUATION ends the
   request once the field block completes; END_STREAM on CONTINUATION
   is undefined and ignored.  A request the server ends before we
   half-closed it gets RST_STREAM(NO_ERROR), freeing its stream slot.
   c==0: split END_STREAM, c==1: same after we half-closed,
   c==2: DATA END_STREAM, c==3: END_STREAM on CONTINUATION,
   c==4: c==0 with rx_start filling all remaining TX space */

FD_UNIT_TEST( split_end_stream_headers ) {
  static uchar const hdrs[] = {0x88,0x5f,0x10,'a','p','p','l','i','c','a','t','i','o','n','/','g','r','p','c'};
  for( int c=0; c<5; c++ ) {
    fd_grpc_client_reset( client );
    test_grpc_client_mock_conn( client );
    client->conn->peer_settings.max_concurrent_streams = 1U;
    fd_grpc_h2_stream_t * stream = fd_grpc_client_stream_acquire( client, 0UL );
    if( c==1 ) fd_h2_stream_close_tx( &stream->s, client->conn );
    g_rx_end_cnt       = 0UL;
    g_rx_start_fill_tx = c==4;
    test_rx_frame( FD_H2_FRAME_TYPE_HEADERS, ( c<2 || c==4 ) ? FD_H2_FLAG_END_STREAM : 0U, 1U, hdrs, 1UL );
    FD_TEST( client->stream_cnt==1UL );
    static uchar const filler[ 32768 ] = {0};
    ulong const tx_prefix = c==4 ? client->frame_tx_buf_max-sizeof(fd_h2_ping_t) : 0UL;
    if( c==4 ) fd_h2_rbuf_push( client->frame_tx, filler, tx_prefix );
    test_rx_frame( FD_H2_FRAME_TYPE_CONTINUATION, FD_H2_FLAG_END_HEADERS|( c==3 ? FD_H2_FLAG_END_STREAM : 0U ),
                   1U, hdrs+1, sizeof(hdrs)-1UL );
    if( c==2 ) test_rx_frame( FD_H2_FRAME_TYPE_DATA, FD_H2_FLAG_END_STREAM, 1U, hdrs, 0UL );
    ulong const live = (ulong)( c==3 );
    FD_TEST( !client->conn->conn_error && g_rx_end_cnt==1UL-live );
    FD_TEST( client->stream_cnt==live && client->conn->stream_active_cnt[1]==(uint)live );
    if( c==4 ) fd_h2_rbuf_skip( client->frame_tx, tx_prefix );
    fd_h2_rst_stream_t rst = {0};
    if( c==0 || c==2 || c==4 ) fd_h2_rbuf_pop_copy( client->frame_tx, &rst, sizeof(rst) );
    if( c==4 ) fd_h2_rbuf_skip( client->frame_tx, fd_h2_rbuf_used_sz( client->frame_tx ) );
    FD_TEST( fd_uint_bswap( rst.error_code )==(c==2 ? FD_H2_ERR_PROTOCOL : FD_H2_SUCCESS) && fd_h2_rbuf_is_empty( client->frame_tx ) );
    FD_TEST( fd_grpc_client_stream_acquire_is_safe( client )==!live );
  }
  g_rx_start_fill_tx = 0;
}

/* A final DATA frame may wrap after a complete message, so its first
   callback can fill TX before the END_STREAM callback.  A pending send
   must not resume after the reset, even when flow-control credit returns. */

FD_UNIT_TEST( data_end_stream_before_callbacks ) {
  static uchar const messages[] = {0,0,0,0,1,'a',0,0,0,0,1,'b'};
  static uchar const request[]  = {0,0,0,0,3,'a','b','c'};
  static uchar const filler[ 32768 ] = {0};
  for( int c=0; c<4; c++ ) {
    fd_grpc_client_reset( client );
    test_grpc_client_mock_conn( client );
    client->conn->peer_settings.max_concurrent_streams = 1U;
    fd_grpc_h2_stream_t * stream = fd_grpc_client_stream_acquire( client, 1UL );
    stream->hdrs.h2_status     = 200U;
    stream->hdrs.is_grpc_proto = 1U;
  stream->hdrs_received = 1U;
    g_rx_msg_closed_stream = stream;
    g_rx_msg_cnt           = 0UL;
    g_rx_end_cnt           = 0UL;
    g_rx_end_fill_tx       = 1;

    /* One complete outbound DATA frame precedes the reset.  The rest of
       this request remains parked until the response cancels it. */
    client->conn->tx_wnd = stream->s.tx_wnd = 1U;
    fd_h2_tx_op_init( client->request_tx_op, request, sizeof(request), 0U );
    fd_h2_tx_op_copy( client->conn, &stream->s, client->frame_tx, client->request_tx_op );
    FD_TEST( client->request_tx_op->chunk_sz==sizeof(request)-1UL );
    ulong const tx_prefix = client->frame_tx_buf_max-2UL*sizeof(fd_h2_window_update_t);
    fd_h2_rbuf_push( client->frame_tx, filler, tx_prefix-fd_h2_rbuf_used_sz( client->frame_tx ) );

    if( c==1 ) {
      ulong const offset = client->frame_rx_buf_max-sizeof(fd_h2_frame_hdr_t)-6UL;
      fd_h2_rbuf_push( client->frame_rx, filler, offset );
      fd_h2_rbuf_skip( client->frame_rx, offset );
    }
    uchar response[ sizeof(messages) ];
    memcpy( response, messages, sizeof(messages) );
    ulong response_sz = sizeof(messages);
    if( c>=2 ) {
      ulong const offset = c==2 ? 0UL : 6UL;
      fd_grpc_hdr_t oversized = { .msg_sz = fd_uint_bswap( (uint)client->frame_rx_buf_max ) };
      memcpy( response+offset, &oversized, sizeof(oversized) );
      response_sz = offset+sizeof(oversized);
    }
    test_rx_frame( FD_H2_FRAME_TYPE_DATA, FD_H2_FLAG_END_STREAM, stream->s.stream_id, response, response_sz );
    FD_TEST( !client->conn->conn_error && !client->stream_cnt && !client->conn->stream_active_cnt[1] );
    FD_TEST( g_rx_msg_cnt==( c==2 ? 0UL : c==3 ? 1UL : 2UL ) && g_rx_end_cnt==1UL );
    FD_TEST( !client->request_stream && !client->request_tx_op->chunk_sz );

    fd_h2_frame_hdr_t data_hdr;
    fd_h2_rbuf_pop_copy( client->frame_tx, &data_hdr, sizeof(data_hdr) );
    FD_TEST( data_hdr.typlen==fd_h2_frame_typlen( FD_H2_FRAME_TYPE_DATA, 1UL ) );
    fd_h2_rbuf_skip( client->frame_tx, tx_prefix-sizeof(data_hdr) );
    fd_h2_rst_stream_t rst;
    fd_h2_rbuf_pop_copy( client->frame_tx, &rst, sizeof(rst) );
    FD_TEST( rst.hdr.typlen==fd_h2_frame_typlen( FD_H2_FRAME_TYPE_RST_STREAM, 4UL ) );
    FD_TEST( fd_uint_bswap( rst.error_code )==( c==2 ? FD_H2_ERR_INTERNAL : FD_H2_SUCCESS ) );
    fd_h2_rbuf_skip( client->frame_tx, fd_h2_rbuf_used_sz( client->frame_tx ) );
    FD_TEST( fd_grpc_client_stream_acquire_is_safe( client ) );
  }
  g_rx_msg_closed_stream = NULL;
  g_rx_end_fill_tx       = 0;
}

FD_UNIT_TEST( grpc_stream_error_releases_h2_quota ) {
  fd_grpc_client_reset( client );
  test_grpc_client_mock_conn( client );
  client->conn->peer_settings.max_concurrent_streams = 1U;

  fd_grpc_h2_stream_t * stream = fd_grpc_client_stream_acquire( client, 0UL );
  ulong stream_id = stream->s.stream_id;
  FD_TEST( client->conn->stream_active_cnt[1]==1U );
  FD_TEST( !fd_grpc_client_stream_acquire_is_safe( client ) );

  fd_grpc_h2_cb_headers( client->conn, &stream->s, "\x08\x03xyz", 5UL, FD_H2_FLAG_END_HEADERS );
  FD_TEST( client->stream_cnt==0UL );
  FD_TEST( client->conn->stream_active_cnt[1]==0U );
  FD_TEST( fd_grpc_client_stream_acquire_is_safe( client ) );
  FD_TEST( g_rx_end_cnt>0UL );

  FD_TEST( fd_h2_rbuf_used_sz( client->frame_tx )==sizeof(fd_h2_rst_stream_t) );
  fd_h2_rst_stream_t rst_stream;
  fd_h2_rbuf_pop_copy( client->frame_tx, &rst_stream, sizeof(fd_h2_rst_stream_t) );
  FD_TEST( rst_stream.hdr.typlen==fd_h2_frame_typlen( FD_H2_FRAME_TYPE_RST_STREAM, 4UL ) );
  FD_TEST( rst_stream.hdr.flags==0 );
  FD_TEST( fd_uint_bswap( rst_stream.hdr.r_stream_id )==stream_id );
  FD_TEST( fd_uint_bswap( rst_stream.error_code )==FD_H2_ERR_PROTOCOL );

  fd_grpc_client_reset( client );
  test_grpc_client_mock_conn( client );
  client->conn->peer_settings.max_concurrent_streams = 1U;

  stream = fd_grpc_client_stream_acquire( client, 1UL );
  stream_id = stream->s.stream_id;
  stream->hdrs.h2_status     = 200U;
  stream->hdrs.is_grpc_proto = 1U;
  stream->hdrs_received = 1U;
  FD_TEST( client->conn->stream_active_cnt[1]==1U );
  FD_TEST( !fd_grpc_client_stream_acquire_is_safe( client ) );

  fd_grpc_hdr_t hdr = {
    .compressed = 0U,
    .msg_sz     = fd_uint_bswap( (uint)client->frame_rx_buf_max )
  };
  fd_grpc_h2_cb_data( client->conn, &stream->s, &hdr, sizeof(hdr), 0UL );
  FD_TEST( client->stream_cnt==0UL );
  FD_TEST( client->conn->stream_active_cnt[1]==0U );
  FD_TEST( fd_grpc_client_stream_acquire_is_safe( client ) );

  FD_TEST( fd_h2_rbuf_used_sz( client->frame_tx )==sizeof(fd_h2_rst_stream_t) );
  fd_h2_rbuf_pop_copy( client->frame_tx, &rst_stream, sizeof(fd_h2_rst_stream_t) );
  FD_TEST( rst_stream.hdr.typlen==fd_h2_frame_typlen( FD_H2_FRAME_TYPE_RST_STREAM, 4UL ) );
  FD_TEST( rst_stream.hdr.flags==0 );
  FD_TEST( fd_uint_bswap( rst_stream.hdr.r_stream_id )==stream_id );
  FD_TEST( fd_uint_bswap( rst_stream.error_code )==FD_H2_ERR_INTERNAL );

  /* Corrupt headers once END_STREAM closed the stream: no RST_STREAM */
  ulong const rx_end_cnt = g_rx_end_cnt;
  stream = fd_grpc_client_stream_acquire( client, 2UL );
  fd_h2_stream_close_tx( &stream->s, client->conn );
  test_rx_frame( FD_H2_FRAME_TYPE_HEADERS, FD_H2_FLAG_END_STREAM|FD_H2_FLAG_END_HEADERS, stream->s.stream_id, (uchar const *)"\x08\x03xyz", 5UL );
  FD_TEST( g_rx_end_cnt==rx_end_cnt+1UL && !client->stream_cnt && fd_h2_rbuf_is_empty( client->frame_tx ) );

  /* Zero stream WINDOW_UPDATE: one RST_STREAM, request ended */
  stream = fd_grpc_client_stream_acquire( client, 3UL );
  uint const zero = 0U;
  test_rx_frame( FD_H2_FRAME_TYPE_WINDOW_UPDATE, 0U, stream->s.stream_id, (uchar const *)&zero, 4UL );
  FD_TEST( g_rx_end_cnt==rx_end_cnt+2UL && !client->stream_cnt && !client->conn->stream_active_cnt[1] && !client->request_stream );
  FD_TEST( !client->conn->conn_error && fd_h2_rbuf_used_sz( client->frame_tx )==sizeof(fd_h2_rst_stream_t) );
  fd_h2_rbuf_pop_copy( client->frame_tx, &rst_stream, sizeof(fd_h2_rst_stream_t) );
  FD_TEST( fd_uint_bswap( rst_stream.error_code )==FD_H2_ERR_PROTOCOL );
}

FD_UNIT_TEST( tls_socket_eof_disconnects ) {
  fd_grpc_client_reset( client );

  fd_tls_t tls = {0};
  fd_tlsrec_conn_t tls_conn[1];
  fd_tlsrec_conn_init( tls_conn, &tls, 0 );
  tls_conn->hs.base.state = FD_TLS_HS_CONNECTED;

  int sock[2];
  FD_TEST( !socketpair( AF_UNIX, SOCK_STREAM, 0, sock ) );
  FD_TEST( !close( sock[1] ) );

  int charge_busy = 0;
  FD_TEST( fd_grpc_client_rxtx_tls( client, tls_conn, sock[0], fd_log_wallclock(), &charge_busy )==-1 );
  FD_TEST( !close( sock[0] ) );
}

FD_UNIT_TEST( tls_record_error_disconnects_during_handshake ) {
  fd_grpc_client_reset( client );

  fd_tls_t tls = {0};
  fd_tlsrec_conn_t tls_conn[1];
  fd_tlsrec_conn_init( tls_conn, &tls, 0 );
  tls_conn->hs.base.state = FD_TLS_HS_WAIT_SH;
  FD_TEST( !fd_tlsrec_conn_is_ready( tls_conn ) );
  FD_TEST( !fd_tlsrec_conn_is_failed( tls_conn ) );

  int sock[2];
  FD_TEST( !socketpair( AF_UNIX, SOCK_STREAM, 0, sock ) );

  uchar const bad_record[] = { FD_TLS_REC_APPLICATION_DATA, 0x03, 0x03, 0x00, 0x00 };
  FD_TEST( write( sock[1], bad_record, sizeof(bad_record) )==(long)sizeof(bad_record) );

  int charge_busy = 0;
  FD_TEST( fd_grpc_client_rxtx_tls( client, tls_conn, sock[0], fd_log_wallclock(), &charge_busy )==-1 );
  FD_TEST( !close( sock[0] ) );
  FD_TEST( !close( sock[1] ) );
}

FD_UNIT_TEST( tls_no_alpn_refuses_h2 ) {
  fd_grpc_client_reset( client );

  fd_tls_t tls = {0};
  fd_tlsrec_conn_t tls_conn[1];
  fd_tlsrec_conn_init( tls_conn, &tls, 0 );
  tls_conn->hs.base.state = FD_TLS_HS_CONNECTED;
  FD_TEST( fd_tlsrec_conn_is_ready( tls_conn ) );
  FD_TEST( !tls_conn->hs.cli.alpn_negotiated );

  int sock[2];
  FD_TEST( !socketpair( AF_UNIX, SOCK_STREAM, 0, sock ) );

  int charge_busy = 0;
  FD_TEST( fd_grpc_client_rxtx_tls( client, tls_conn, sock[0], fd_log_wallclock(), &charge_busy )==-1 );

  tls_conn->hs.cli.alpn_negotiated = 1;
  FD_TEST( fd_grpc_client_rxtx_tls( client, tls_conn, sock[0], fd_log_wallclock(), &charge_busy )==0 );

  FD_TEST( !close( sock[0] ) );
  FD_TEST( !close( sock[1] ) );
}

/* test_tls_install_keys puts conn into the connected state with the
   given application traffic secrets (RFC 8446 section 7.3), skipping
   the handshake. */

static void
test_tls_install_keys( fd_tlsrec_conn_t * conn,
                       uchar const        write_secret[ static 32 ],
                       uchar const        read_secret [ static 32 ] ) {
  fd_tlsrec_keys_t * keys = &conn->keys[1];
  fd_memcpy( keys->write_secret, write_secret, 32UL );
  fd_memcpy( keys->read_secret,  read_secret,  32UL );
  fd_tls_hkdf_expand_label( keys->write_key, 16UL, keys->write_secret, "key", 3UL, NULL, 0UL );
  fd_tls_hkdf_expand_label( keys->write_iv,  12UL, keys->write_secret, "iv",  2UL, NULL, 0UL );
  fd_tls_hkdf_expand_label( keys->read_key,  16UL, keys->read_secret,  "key", 3UL, NULL, 0UL );
  fd_tls_hkdf_expand_label( keys->read_iv,   12UL, keys->read_secret,  "iv",  2UL, NULL, 0UL );
  fd_aes_gcm_init( &keys->write_gcm, keys->write_key, 16UL, keys->write_iv );
  fd_aes_gcm_init( &keys->read_gcm,  keys->read_key,  16UL, keys->read_iv  );
  conn->read_seq  = 0UL;
  conn->write_seq = 0UL;
  conn->hs.base.state = FD_TLS_HS_CONNECTED;
}

/* A send parked on EAGAIN must not stop RX: a KeyUpdate from the peer
   is processed and its reply is appended behind the parked record. */

FD_UNIT_TEST( tls_rx_continues_while_send_blocked ) {
  fd_grpc_client_reset( client );

  fd_tls_t tls = {0};
  fd_tlsrec_conn_t cli[1];
  fd_tlsrec_conn_t srv[1];
  fd_tlsrec_conn_init( cli, &tls, 0 );
  fd_tlsrec_conn_init( srv, &tls, 1 );
  uchar secret_a[ 32 ]; uchar secret_b[ 32 ];
  for( ulong i=0UL; i<32UL; i++ ) { secret_a[i] = (uchar)(0xa0+i); secret_b[i] = (uchar)(0xb0+i); }
  test_tls_install_keys( cli, secret_a, secret_b );
  test_tls_install_keys( srv, secret_b, secret_a );
  cli->hs.cli.alpn_negotiated = 1;
  FD_TEST( fd_tlsrec_conn_is_ready( cli ) );
  FD_TEST( fd_tlsrec_conn_is_ready( srv ) );

  int sock[2];
  FD_TEST( !socketpair( AF_UNIX, SOCK_STREAM, 0, sock ) );
  int sndbuf = 4096;
  FD_TEST( !setsockopt( sock[0], SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(int) ) );

  /* Fill the socket send buffer so the next send blocks */
  static uchar junk[ 4096 ];
  ulong filled = 0UL;
  for(;;) {
    long n = send( sock[0], junk, sizeof(junk), MSG_NOSIGNAL|MSG_DONTWAIT );
    if( n<0L ) { FD_TEST( errno==EAGAIN || errno==EWOULDBLOCK ); break; }
    filled += (ulong)n;
  }
  FD_TEST( filled );

  /* Park an encrypted record */
  fd_tlsrec_sock_t * tls_sock = client->tls_sock;
  uchar const parked_msg[] = "parked";
  ulong consumed;
  FD_TEST( fd_tlsrec_sock_tx( tls_sock, cli, sock[0], parked_msg, sizeof(parked_msg)-1UL, &consumed )==0 );
  FD_TEST( consumed==sizeof(parked_msg)-1UL );
  ulong parked_sz = tls_sock->tx_sz;
  FD_TEST( parked_sz );
  FD_TEST( fd_grpc_client_tls_tx_pending( client ) );
  FD_TEST( tls_sock->tx_off==0UL );

  /* Peer requests a key update */
  uchar ku[ 64 ];
  ulong ku_sz = sizeof(ku);
  FD_TEST( fd_tlsrec_conn_key_update( srv, ku, &ku_sz, 1 )==FD_TLSREC_SUCCESS );
  FD_TEST( ku_sz );
  FD_TEST( write( sock[1], ku, ku_sz )==(long)ku_sz );

  int charge_busy = 0;
  FD_TEST( fd_grpc_client_rxtx_tls( client, cli, sock[0], fd_log_wallclock(), &charge_busy )==0 );
  FD_TEST( charge_busy );
  FD_TEST( !fd_grpc_client_tls_rx_pending( client ) );
  FD_TEST( tls_sock->tx_off==0UL );
  FD_TEST( tls_sock->tx_sz==parked_sz+ku_sz );
  FD_TEST( !cli->write_seq ); /* rotated */

  /* The peer still is not reading.  A step that moves nothing is not
     busy, even with HTTP/2 output queued behind the parked record;
     otherwise the tile would spin on EAGAIN instead of waiting for
     EPOLLOUT. */
  static uchar const h2_junk[] = "h2";
  fd_h2_rbuf_push( client->frame_tx, h2_junk, sizeof(h2_junk) );
  charge_busy = 0;
  FD_TEST( fd_grpc_client_rxtx_tls( client, cli, sock[0], fd_log_wallclock(), &charge_busy )==0 );
  FD_TEST( !charge_busy );
  FD_TEST( tls_sock->tx_off==0UL );
  FD_TEST( tls_sock->tx_sz==parked_sz+ku_sz );
  FD_TEST( fd_h2_rbuf_used_sz( client->frame_tx )>=sizeof(h2_junk) );

  /* Peer drains; parked record then reply go out in order and decrypt
     under the right keys */
  ulong drained = 0UL;
  while( drained<filled ) {
    long n = recv( sock[1], junk, fd_ulong_min( sizeof(junk), filled-drained ), MSG_DONTWAIT );
    FD_TEST( n>0L );
    drained += (ulong)n;
  }
  FD_TEST( fd_grpc_client_tls_flush( client, sock[0] )==0 );
  FD_TEST( !fd_grpc_client_tls_tx_pending( client ) );

  uchar wire[ 256 ];
  long wire_sz = recv( sock[1], wire, sizeof(wire), MSG_DONTWAIT );
  FD_TEST( wire_sz==(long)( parked_sz+ku_sz ) );

  fd_tlsrec_slice_t wire_slice[1];
  fd_tlsrec_slice_init( wire_slice, wire, (ulong)wire_sz );
  uchar srv_tx[ 64 ]; ulong srv_tx_sz = sizeof(srv_tx);
  uchar app_rx[ 64 ]; ulong app_rx_sz = sizeof(app_rx);
  FD_TEST( fd_tlsrec_conn_rx( srv, wire_slice, srv_tx, &srv_tx_sz, app_rx, &app_rx_sz )==FD_TLSREC_SUCCESS );
  FD_TEST( !srv_tx_sz );
  FD_TEST( app_rx_sz==sizeof(parked_msg)-1UL );
  FD_TEST( !memcmp( app_rx, parked_msg, app_rx_sz ) );
  FD_TEST( !srv->read_seq ); /* rotated by the client's reply */

  /* Next record uses the new client write key */
  uchar const next_msg[] = "after";
  fd_tlsrec_slice_t app_tx[1];
  fd_tlsrec_slice_init( app_tx, (uchar *)next_msg, sizeof(next_msg)-1UL );
  ulong next_sz = sizeof(wire);
  FD_TEST( fd_tlsrec_conn_tx( cli, wire, &next_sz, app_tx )==FD_TLSREC_SUCCESS );
  fd_tlsrec_slice_init( wire_slice, wire, next_sz );
  srv_tx_sz = sizeof(srv_tx); app_rx_sz = sizeof(app_rx);
  FD_TEST( fd_tlsrec_conn_rx( srv, wire_slice, srv_tx, &srv_tx_sz, app_rx, &app_rx_sz )==FD_TLSREC_SUCCESS );
  FD_TEST( app_rx_sz==sizeof(next_msg)-1UL );
  FD_TEST( !memcmp( app_rx, next_msg, app_rx_sz ) );

  FD_TEST( !close( sock[0] ) );
  FD_TEST( !close( sock[1] ) );
  fd_grpc_client_reset( client );
}
/* A literal may split inside its name, length, or Huffman code.  No provisional
   metadata reaches the request, including with END_STREAM on its first frame. */
static uchar const complete_headers[] = {
  0x88,0x5f,0x10,'a','p','p','l','i','c','a','t','i','o','n','/','g','r','p','c',
  0x00,0x01,'x',0x8c,0xf1,0xe3,0xc2,0xe5,0xf2,0x3a,0x6b,0xa0,0xab,0x90,0xf4,0xff,
  0x00,11,'g','r','p','c','-','s','t','a','t','u','s',1,'0'
};
static uchar const ok_trailers[] = {0x00,11,'g','r','p','c','-','s','t','a','t','u','s',1,'0'};

static fd_grpc_h2_stream_t *
new_test_stream_at_mono( long mono ) {
  g_mono_now=mono;
  g_mono_jump_after=0U;
  g_mono_jump_ns=5000000000L;
  fd_grpc_client_reset( client );
  test_grpc_client_mock_conn( client );
  fd_grpc_client_service_deadlines( client, 100L );
  g_rx_start_cnt = g_rx_msg_cnt = g_rx_end_cnt = g_conn_dead_cnt = 0UL;
  return fd_grpc_client_stream_acquire( client, 1234UL );
}

static fd_grpc_h2_stream_t *
new_test_stream( void ) {
  return new_test_stream_at_mono( 100L );
}

/* Run every encoded-byte boundary through initial headers or trailers.
   Keep provisional metadata and callback assertions common to raw/Huffman cases. */
static ulong
test_terminal_header_splits( uchar const * block, ulong sz, int trailer, int valid ) {
  for( ulong split=0UL; split<=sz; split++ ) {
    fd_grpc_h2_stream_t * stream=new_test_stream();
    if( trailer ) test_rx_frame(FD_H2_FRAME_TYPE_HEADERS,FD_H2_FLAG_END_HEADERS,1U,complete_headers,sizeof(complete_headers));
    test_rx_frame(FD_H2_FRAME_TYPE_HEADERS,FD_H2_FLAG_END_STREAM,1U,block,split);
    FD_TEST(client->stream_cnt==1UL && !g_rx_end_cnt && g_rx_start_cnt==(ulong)trailer);
    FD_TEST(stream->hdrs_received==(uint)trailer);
    if( !trailer ) FD_TEST(!stream->hdrs.h2_status && stream->hdrs.grpc_status==FD_GRPC_STATUS_UNKNOWN);
    test_rx_frame(FD_H2_FRAME_TYPE_CONTINUATION,FD_H2_FLAG_END_HEADERS,1U,block+split,sz-split);
    FD_TEST(!client->stream_cnt && g_rx_end_cnt==1UL && !client->conn->conn_error);
    FD_TEST(g_rx_start_cnt==(ulong)(trailer || valid));
    FD_TEST(g_cb_resp_hdrs.grpc_status==(valid ? FD_GRPC_STATUS_OK : FD_GRPC_STATUS_INTERNAL));
    if( valid ) FD_TEST(g_cb_resp_hdrs.h2_status==200U);
  }
  return sz+1UL;
}

FD_UNIT_TEST( complete_header_block_every_split ) {
  test_terminal_header_splits( complete_headers, sizeof(complete_headers), 0, 1 );
}

FD_UNIT_TEST( response_phases_and_atomic_failures ) {
  static uchar const info[] = {0x08,3,'1','0','3'};
  static uchar const message[] = {0,0,0,0,1,'a'};
  static uchar const bad[] = {0x08,3,'x','y','z'};
  for( int fault=0; fault<8; fault++ ) {
    fd_grpc_h2_stream_t * stream = new_test_stream();
    fd_grpc_client_deadline_set( stream, FD_GRPC_DEADLINE_HEADER, 1000L );
    test_rx_frame( FD_H2_FRAME_TYPE_HEADERS, FD_H2_FLAG_END_HEADERS, 1U, info, sizeof(info) );
    FD_TEST( !stream->hdrs_received && stream->has_header_deadline && !g_rx_start_cnt );
    if( fault==1 ) {
      test_rx_frame( FD_H2_FRAME_TYPE_DATA, 0U, 1U, message, sizeof(message) );
    } else {
      test_rx_frame( FD_H2_FRAME_TYPE_HEADERS, FD_H2_FLAG_END_HEADERS, 1U,
                     complete_headers, sizeof(complete_headers) );
      FD_TEST( stream->hdrs_received && !stream->has_header_deadline && g_rx_start_cnt==1UL );
      test_rx_frame( FD_H2_FRAME_TYPE_DATA, 0U, 1U, message, sizeof(message) );
      FD_TEST( g_rx_msg_cnt==1UL );
      switch( fault ) {
      case 0: test_rx_frame( FD_H2_FRAME_TYPE_HEADERS, FD_H2_FLAG_END_STREAM|FD_H2_FLAG_END_HEADERS,
                            1U, ok_trailers, sizeof(ok_trailers) ); break;
      case 2: test_rx_frame( FD_H2_FRAME_TYPE_HEADERS, FD_H2_FLAG_END_STREAM|FD_H2_FLAG_END_HEADERS,
                            1U, bad, sizeof(bad) ); break;
      case 3: test_rx_frame( FD_H2_FRAME_TYPE_HEADERS, FD_H2_FLAG_END_HEADERS,
                            1U, ok_trailers, sizeof(ok_trailers) ); break;
      case 4: test_rx_frame( FD_H2_FRAME_TYPE_DATA, FD_H2_FLAG_END_STREAM, 1U, NULL, 0UL ); break;
      case 5: { uint err=fd_uint_bswap(FD_H2_ERR_CANCEL);
                test_rx_frame( FD_H2_FRAME_TYPE_RST_STREAM, 0U, 1U, (uchar const *)&err, 4UL ); break; }
      case 6: test_rx_frame( FD_H2_FRAME_TYPE_HEADERS, FD_H2_FLAG_END_STREAM|FD_H2_FLAG_END_HEADERS,
                            1U, NULL, 0UL ); break; /* missing status does not inherit initial grpc0 */
      case 7: { fd_grpc_hdr_t hdr={.msg_sz=fd_uint_bswap(65536U)};
                test_rx_frame( FD_H2_FRAME_TYPE_DATA, 0U, 1U, (uchar const *)&hdr, sizeof(hdr) ); break; }
      }
    }
    FD_TEST( !client->stream_cnt && g_rx_end_cnt==1UL );
    FD_TEST( (g_cb_resp_hdrs.grpc_status==FD_GRPC_STATUS_OK)==(fault==0) );
    FD_TEST( !client->conn->conn_error );
  }
}

FD_UNIT_TEST( partial_second_message_cannot_complete_with_ok_trailers ) {
  static uchar const two_messages[] = {0,0,0,0,1,'a',0,0,0,0,4,'b','c','d','e'};
  for( ulong second=1UL; second<9UL; second++ ) {
    new_test_stream();
    test_rx_frame( FD_H2_FRAME_TYPE_HEADERS, FD_H2_FLAG_END_HEADERS, 1U, complete_headers, sizeof(complete_headers) );
    test_rx_frame( FD_H2_FRAME_TYPE_DATA, 0U, 1U, two_messages, 6UL+second );
    FD_TEST( g_rx_msg_cnt==1UL && !g_rx_end_cnt );
    test_rx_frame( FD_H2_FRAME_TYPE_HEADERS, FD_H2_FLAG_END_STREAM|FD_H2_FLAG_END_HEADERS,
                   1U, ok_trailers, sizeof(ok_trailers) );
    FD_TEST( g_rx_end_cnt==1UL && !client->stream_cnt && g_cb_resp_hdrs.grpc_status!=FD_GRPC_STATUS_OK );
  }
}

FD_UNIT_TEST( block_deadline_observes_incomplete_first_frame_and_tx_wedge ) {
  new_test_stream();
  fd_h2_frame_hdr_t hdr=test_incomplete_headers( FD_H2_FLAG_PADDED, 1U );
  fd_h2_rbuf_push( client->frame_rx, &hdr, sizeof(hdr) );
  static uchar const filler[4096]={0};
  fd_h2_rbuf_push( client->frame_tx, filler, client->frame_tx_buf_max );
  fd_h2_rx( client->conn, client->frame_rx, client->frame_tx, client->frame_scratch,
            client->frame_scratch_max, &fd_grpc_client_h2_callbacks );
  FD_TEST( client->conn->rx_hdrs_observed && !(client->conn->flags & FD_H2_CONN_FLAGS_CONTINUATION) );
  g_mono_now=200L;
  fd_grpc_client_service_deadlines( client, 200L );
  long deadline=fd_grpc_client_next_deadline( client );
  FD_TEST( deadline==5000000200L );
  g_mono_now=300L;
  fd_grpc_client_service_deadlines( client, 300L );
  FD_TEST( fd_grpc_client_next_deadline(client)==deadline );
  g_mono_now=deadline;
  fd_grpc_client_service_deadlines( client, deadline );
  FD_TEST( g_conn_dead_cnt==1UL && (client->conn->flags & FD_H2_CONN_FLAGS_DEAD) );
  fd_grpc_client_service_deadlines( client, deadline+1L );
  FD_TEST( g_conn_dead_cnt==1UL && !g_rx_end_cnt );
}

FD_UNIT_TEST( header_resource_bounds_and_compression_priority ) {
  new_test_stream();
  static uchar const bad_tail[] = {0x08,3,'x','y','z',0x80};
  test_rx_frame( FD_H2_FRAME_TYPE_HEADERS, FD_H2_FLAG_END_HEADERS, 1U, bad_tail, sizeof(bad_tail) );
  FD_TEST( client->conn->conn_error==FD_H2_ERR_COMPRESSION && !g_rx_end_cnt );
  /* An ignored indexed field still costs its complete name+value+32 bytes. */
  new_test_stream();
  uchar many[2048]; memset(many,0x88,sizeof(many));
  test_rx_frame( FD_H2_FRAME_TYPE_HEADERS, FD_H2_FLAG_END_HEADERS, 1U, many, sizeof(many) );
  FD_TEST( client->conn->conn_error==FD_H2_ERR_ENHANCE_YOUR_CALM && !g_rx_end_cnt );
}

FD_UNIT_TEST( receive_budget_yield_keeps_buffered_work_runnable ) {
  new_test_stream();
  for( int i=0; i<100; i++ )
    fd_h2_tx( client->frame_rx, (uchar const *)"", 0UL, 0xfeU, 0U, 0U );
  fd_h2_rx( client->conn, client->frame_rx, client->frame_tx, client->frame_scratch,
            client->frame_scratch_max, &fd_grpc_client_h2_callbacks );
  FD_TEST( fd_grpc_client_rx_pending(client) && fd_h2_rbuf_used_sz(client->frame_rx)==36UL*9UL );
  fd_h2_rx( client->conn, client->frame_rx, client->frame_tx, client->frame_scratch,
            client->frame_scratch_max, &fd_grpc_client_h2_callbacks );
  FD_TEST( !fd_grpc_client_rx_pending(client) && fd_h2_rbuf_is_empty(client->frame_rx) );
}

FD_UNIT_TEST( callback_reset_cannot_release_reused_stream_or_consume_padding ) {
  static uchar const message[] = {0,0,0,0,1,'a'};
  for( int callback=1; callback<=3; callback++ ) {
    new_test_stream();
    g_reset_callback=callback;
    if( callback==1 ) {
      uchar padded[sizeof(complete_headers)+4UL];
      padded[0]=3U; memcpy(padded+1,complete_headers,sizeof(complete_headers));
      memset(padded+1+sizeof(complete_headers),0,3UL);
      test_rx_frame(FD_H2_FRAME_TYPE_HEADERS,FD_H2_FLAG_PADDED|FD_H2_FLAG_END_STREAM|FD_H2_FLAG_END_HEADERS,
                    1U,padded,sizeof(padded));
    } else {
      test_rx_frame(FD_H2_FRAME_TYPE_HEADERS,FD_H2_FLAG_END_HEADERS,1U,complete_headers,sizeof(complete_headers));
      if( callback==2 ) test_rx_frame(FD_H2_FRAME_TYPE_DATA,0U,1U,message,sizeof(message));
      else test_rx_frame(FD_H2_FRAME_TYPE_HEADERS,FD_H2_FLAG_END_STREAM|FD_H2_FLAG_END_HEADERS,
                         1U,ok_trailers,sizeof(ok_trailers));
    }
    FD_TEST(client->stream_cnt==1UL && client->streams[0]==g_replacement_stream);
    FD_TEST(g_replacement_stream->request_ctx==5678UL && g_replacement_stream->s.stream_id==1U);
    FD_TEST(fd_h2_rbuf_is_empty(client->frame_rx) && client->conn->stream_active_cnt[1]==1U);
    g_reset_callback=0;
  }
}

FD_UNIT_TEST( canceled_block_validation_and_absolute_expiry ) {
  static uchar const bad_tail[] = {0x80};
  for( int malformed=0; malformed<2; malformed++ ) {
    fd_grpc_h2_stream_t * stream=new_test_stream();
    test_rx_frame(FD_H2_FRAME_TYPE_HEADERS,0U,1U,complete_headers,5UL);
    g_mono_now=200L;
    fd_grpc_client_service_deadlines(client,200L);
    long deadline=fd_grpc_client_next_deadline(client);
    fd_grpc_client_deadline_set(stream,FD_GRPC_DEADLINE_HEADER,201L);
    g_mono_now=201L;
    fd_grpc_client_service_deadlines(client,201L);
    FD_TEST(!client->stream_cnt && client->conn->rx_hdrs_discard);
    FD_TEST(fd_grpc_client_next_deadline(client)==deadline);
    test_rx_frame(FD_H2_FRAME_TYPE_CONTINUATION,FD_H2_FLAG_END_HEADERS,1U,
                  malformed ? bad_tail : complete_headers+5UL,
                  malformed ? sizeof(bad_tail) : sizeof(complete_headers)-5UL);
    FD_TEST(!g_rx_end_cnt && client->conn->conn_error==(malformed ? FD_H2_ERR_COMPRESSION : FD_H2_SUCCESS));
  }
  fd_grpc_h2_stream_t * stream=new_test_stream();
  test_rx_frame(FD_H2_FRAME_TYPE_HEADERS,0U,1U,complete_headers,1UL);
  fd_grpc_client_service_deadlines(client,200L);
  /* No caller service intervenes after anchoring: a 5-second parse preemption
     reaches absolute monotonic expiry before metadata can commit. */
  g_mono_now=client->block_deadline_mono;
  test_rx_frame(FD_H2_FRAME_TYPE_CONTINUATION,FD_H2_FLAG_END_HEADERS,1U,
                complete_headers+1UL,sizeof(complete_headers)-1UL);
  FD_TEST(g_conn_dead_cnt==1UL && !g_rx_start_cnt && !g_rx_end_cnt && !stream->hdrs.h2_status);
}

FD_UNIT_TEST( request_timeout_does_not_wait_for_tx_space ) {
  fd_grpc_h2_stream_t * stream=new_test_stream();
  fd_grpc_client_deadline_set(stream,FD_GRPC_DEADLINE_HEADER,123L);
  static uchar const filler[4096]={0};
  fd_h2_rbuf_push(client->frame_tx,filler,client->frame_tx_buf_max);
  fd_grpc_client_service_deadlines(client,123L);
  FD_TEST(!client->stream_cnt && !g_rx_end_cnt && g_timeout_details.deadline_kind==FD_GRPC_DEADLINE_HEADER);
  FD_TEST(g_conn_dead_cnt==1UL && (client->conn->flags & FD_H2_CONN_FLAGS_DEAD));
}

/* Independent nghttp2 1.59.0 zero-table encoding.  Both names and values use
   Huffman coding.  Run it through actual plaintext and encrypted socket I/O,
   including one-byte CONTINUATION fragments and a parser budget yield. */
FD_UNIT_TEST( peer_encoded_headers_resume_without_new_socket_event ) {
  static uchar const peer_headers[] = {
    0x20,0x88,0x0f,0x10,0x8b,0x1d,0x75,0xd0,0x62,0x0d,0x26,0x3d,0x4c,0x4d,0x65,0x64,
    0x00,0x85,0xf2,0xb5,0x65,0x2d,0x9f,0x8c,0xf1,0xe3,0xc2,0xe5,0xf2,0x3a,0x6b,0xa0,
    0xab,0x90,0xf4,0xff
  };
  static uchar const peer_trailers[] = {0x20,0x00,0x88,0x9a,0xca,0xc8,0xb2,0x12,0x34,0xda,0x8f,0x01,0x30};
  static uchar const message[] = {0,0,0,0,1,'a'};
  for( int encrypted=0; encrypted<2; encrypted++ ) {
    new_test_stream();
    uchar plaintext[2048]; fd_h2_rbuf_t frames[1];
    fd_h2_rbuf_init(frames,plaintext,sizeof(plaintext));
    /* All bytes arrive once.  The first step consumes only 64 frames. */
    for( int i=0; i<80; i++ ) fd_h2_tx(frames,(uchar const *)"",0UL,0xfeU,0U,0U);
    for( ulong i=0UL; i<sizeof(peer_headers); i++ )
      fd_h2_tx(frames,peer_headers+i,1UL,i ? FD_H2_FRAME_TYPE_CONTINUATION : FD_H2_FRAME_TYPE_HEADERS,
               i+1UL==sizeof(peer_headers) ? FD_H2_FLAG_END_HEADERS : 0U,1U);
    fd_h2_tx(frames,message,sizeof(message),FD_H2_FRAME_TYPE_DATA,0U,1U);
    fd_h2_tx(frames,peer_trailers,sizeof(peer_trailers),FD_H2_FRAME_TYPE_HEADERS,
             FD_H2_FLAG_END_HEADERS|FD_H2_FLAG_END_STREAM,1U);
    ulong plaintext_sz=fd_h2_rbuf_used_sz(frames);
    int sock[2]; FD_TEST(!socketpair(AF_UNIX,SOCK_STREAM,0,sock));
    fd_tls_t tls={0}; fd_tlsrec_conn_t cli[1],srv[1];
    if( encrypted ) {
      fd_tlsrec_conn_init(cli,&tls,0); fd_tlsrec_conn_init(srv,&tls,1);
      uchar secret_a[32],secret_b[32];
      for( ulong i=0UL; i<32UL; i++ ) {secret_a[i]=(uchar)i;secret_b[i]=(uchar)(i+32UL);}
      test_tls_install_keys(cli,secret_a,secret_b); test_tls_install_keys(srv,secret_b,secret_a);
      cli->hs.cli.alpn_negotiated=1;
      uchar record[4096]; ulong record_sz=sizeof(record); fd_tlsrec_slice_t slice[1];
      fd_tlsrec_slice_init(slice,plaintext,plaintext_sz);
      FD_TEST(fd_tlsrec_conn_tx(srv,record,&record_sz,slice)==FD_TLSREC_SUCCESS);
      FD_TEST(write(sock[1],record,record_sz)==(long)record_sz);
    } else FD_TEST(write(sock[1],plaintext,plaintext_sz)==(long)plaintext_sz);
    int busy=0;
    FD_TEST((encrypted ? fd_grpc_client_rxtx_tls(client,cli,sock[0],100L,&busy)
                       : fd_grpc_client_rxtx_socket(client,sock[0],100L,&busy))==0);
    FD_TEST(busy && fd_grpc_client_rx_pending(client) && !g_rx_msg_cnt && !g_rx_end_cnt);
    /* No second write or readiness notification.  Buffered RX must run. */
    busy=0;
    FD_TEST((encrypted ? fd_grpc_client_rxtx_tls(client,cli,sock[0],101L,&busy)
                       : fd_grpc_client_rxtx_socket(client,sock[0],101L,&busy))==0);
    FD_TEST(busy && !fd_grpc_client_rx_pending(client) && !client->stream_cnt);
    FD_TEST(g_rx_start_cnt==1UL && g_rx_msg_cnt==1UL && g_rx_end_cnt==1UL);
    FD_TEST(g_cb_resp_hdrs.h2_status==200U && g_cb_resp_hdrs.grpc_status==FD_GRPC_STATUS_OK);
    FD_TEST(!close(sock[0]) && !close(sock[1]));
  }
}

/* A minimum-size gRPC client still advertises the default 16 KiB H2 frame
   limit.  Its RX ring must retain a complete maximum HEADERS frame. */
FD_UNIT_TEST( default_maximum_frame_on_minimum_client_buffer ) {
  new_test_stream();
  uchar header[16384];
  static uchar const initial[] = {0x88,0x5f,0x10,'a','p','p','l','i','c','a','t','i','o','n','/','g','r','p','c'};
  memcpy(header,initial,sizeof(initial));
  ulong off=sizeof(initial);
  header[off++]=0; header[off++]=1; header[off++]='x';
  /* Literal value length 16359: 127 + 16232, encoded with base-128 varint. */
  header[off++]=127; header[off++]=0xe8; header[off++]=0x7e;
  FD_TEST(sizeof(header)-off==16359UL);
  memset(header+off,'v',sizeof(header)-off);
  uchar wire[16393]; fd_h2_rbuf_t frames[1]; fd_h2_rbuf_init(frames,wire,sizeof(wire));
  fd_h2_tx(frames,header,sizeof(header),FD_H2_FRAME_TYPE_HEADERS,FD_H2_FLAG_END_HEADERS,1U);
  int sock[2]; FD_TEST(!socketpair(AF_UNIX,SOCK_STREAM,0,sock));
  FD_TEST(write(sock[1],wire,sizeof(wire))==(long)sizeof(wire));
  int busy=0;
  FD_TEST(fd_grpc_client_rxtx_socket(client,sock[0],100L,&busy)==0);
  FD_TEST(busy && g_rx_start_cnt==1UL && !client->conn->conn_error);
  FD_TEST(fd_h2_rbuf_is_empty(client->frame_rx));
  FD_TEST(!close(sock[0]) && !close(sock[1]));
}

FD_UNIT_TEST( preemption_cannot_commit_after_request_deadline ) {
  fd_grpc_h2_stream_t * stream=new_test_stream();
  fd_grpc_client_deadline_set(stream,FD_GRPC_DEADLINE_HEADER,101L);
  g_timeout_details.deadline_kind=-1;
  g_mono_now+=2L;
  test_rx_frame(FD_H2_FRAME_TYPE_HEADERS,FD_H2_FLAG_END_HEADERS,1U,complete_headers,sizeof(complete_headers));
  FD_TEST(!client->stream_cnt && !g_rx_start_cnt && !g_rx_end_cnt);
  FD_TEST(g_timeout_details.deadline_kind==FD_GRPC_DEADLINE_HEADER);
}

FD_UNIT_TEST( preemption_before_first_block_observation_cannot_restart_timer ) {
  fd_grpc_h2_stream_t * stream=new_test_stream();
  client->conn->rx_hdrs_observed=1U;
  client->conn->rx_hdrs_serial++;
  g_mono_now+=5000000000L;
  fd_grpc_h2_cb_headers(client->conn,&stream->s,complete_headers,sizeof(complete_headers),FD_H2_FLAG_END_HEADERS);
  FD_TEST(g_conn_dead_cnt==1UL && !g_rx_start_cnt && !g_rx_end_cnt);
}

FD_UNIT_TEST( partial_frame_post_receive_service_uses_elapsed_time ) {
  new_test_stream();
  fd_h2_frame_hdr_t hdr=test_incomplete_headers( 0U, 1U );
  int sock[2]; FD_TEST(!socketpair(AF_UNIX,SOCK_STREAM,0,sock));
  FD_TEST(write(sock[1],&hdr,sizeof(hdr))==(long)sizeof(hdr));
  /* First clock read is entry; second is post-RX.  Recognition happened before
     the emulated preemption.  Reanchoring must not restart the block timer. */
  g_mono_jump_after=2U;
  int busy=0;
  FD_TEST(fd_grpc_client_rxtx_socket(client,sock[0],100L,&busy)==-1);
  FD_TEST(!g_mono_jump_after && g_conn_dead_cnt==1UL && !g_rx_start_cnt && !g_rx_end_cnt);
  FD_TEST(!close(sock[0]) && !close(sock[1]));
}

static void
test_socket_write_frame( int sock, uint type, uint flags, uint id,
                         uchar const * payload, ulong payload_sz ) {
  FD_TEST(payload_sz<=256UL);
  fd_h2_frame_hdr_t hdr={.typlen=fd_h2_frame_typlen(type,payload_sz),
                        .flags=(uchar)flags,.r_stream_id=fd_uint_bswap(id)};
  FD_TEST(write(sock,&hdr,sizeof(hdr))==(long)sizeof(hdr));
  FD_TEST(!payload_sz || write(sock,payload,payload_sz)==(long)payload_sz);
}

static fd_grpc_h2_stream_t *
test_cancel_header_owner( int partial ) {
  fd_grpc_h2_stream_t * stream=new_test_stream();
  if( partial ) test_rx_frame(FD_H2_FRAME_TYPE_HEADERS,0U,1U,complete_headers,5UL);
  fd_grpc_client_deadline_set(stream,FD_GRPC_DEADLINE_HEADER,200L);
  fd_grpc_client_service_deadlines(client,200L);
  FD_TEST(!client->stream_cnt && !client->header_block_stream_id);
  return stream;
}

FD_UNIT_TEST( discarded_completion_socket_deadline_and_timely_preemption ) {
  /* Missing singleton owners and canceled continuations both need an exact
     completion check.  The second mono read is now that event; the third
     occurs only afterward, while unrelated post-RX service runs. */
  for( int singleton=0; singleton<2; singleton++ ) {
    for( int late=0; late<2; late++ ) {
      test_cancel_header_owner(!singleton);
      int sock[2];FD_TEST(!socketpair(AF_UNIX,SOCK_STREAM,0,sock));
      test_socket_write_frame(sock[1],singleton ? FD_H2_FRAME_TYPE_HEADERS : FD_H2_FRAME_TYPE_CONTINUATION,
          FD_H2_FLAG_END_HEADERS,1U,singleton ? complete_headers : complete_headers+5UL,
          singleton ? sizeof(complete_headers) : sizeof(complete_headers)-5UL);
      g_mono_jump_after=late ? 2U : 3U;
      int busy=0;
      int ret=fd_grpc_client_rxtx_socket(client,sock[0],300L,&busy);
      FD_TEST(!g_mono_jump_after && !g_rx_start_cnt && !g_rx_end_cnt);
      FD_TEST(g_conn_dead_cnt==(ulong)late);
      FD_TEST((ret==-1)==late && !client->has_block_deadline);
      if( late ) FD_TEST(g_conn_dead_error==FD_H2_ERR_CANCEL);
      else FD_TEST(!client->conn->conn_error && !client->conn->rx_hdrs_observed);
      FD_TEST(!close(sock[0]) && !close(sock[1]));
    }
  }
}

FD_UNIT_TEST( discarded_completion_compression_precedes_expiry ) {
  test_cancel_header_owner(1);
  int sock[2];FD_TEST(!socketpair(AF_UNIX,SOCK_STREAM,0,sock));
  uchar bad[sizeof(complete_headers)-5UL+1UL];
  memcpy(bad,complete_headers+5UL,sizeof(complete_headers)-5UL);
  bad[sizeof(bad)-1UL]=0x80U;
  test_socket_write_frame(sock[1],FD_H2_FRAME_TYPE_CONTINUATION,FD_H2_FLAG_END_HEADERS,1U,bad,sizeof(bad));
  g_mono_jump_after=2U;
  int busy=0;
  FD_TEST(fd_grpc_client_rxtx_socket(client,sock[0],300L,&busy)==-1);
  FD_TEST(!g_mono_jump_after && g_conn_dead_cnt==1UL && g_conn_dead_error==FD_H2_ERR_COMPRESSION);
  FD_TEST(!g_rx_start_cnt && !g_rx_end_cnt);
  FD_TEST(!close(sock[0]) && !close(sock[1]));
}

FD_UNIT_TEST( discarded_completion_reset_preserves_new_buffer ) {
  test_cancel_header_owner(0);
  uchar padded[sizeof(complete_headers)+4UL];
  padded[0]=3U;memcpy(padded+1,complete_headers,sizeof(complete_headers));
  memset(padded+1+sizeof(complete_headers),0,3UL);
  int sock[2];FD_TEST(!socketpair(AF_UNIX,SOCK_STREAM,0,sock));
  test_socket_write_frame(sock[1],FD_H2_FRAME_TYPE_HEADERS,FD_H2_FLAG_PADDED|FD_H2_FLAG_END_HEADERS,
                          1U,padded,sizeof(padded));
  g_reset_callback=4;g_mono_jump_after=2U;
  ulong generation=client->generation;
  int busy=0;
  FD_TEST(fd_grpc_client_rxtx_socket(client,sock[0],300L,&busy)==-1);
  FD_TEST(client->generation==generation+1UL && client->stream_cnt==1UL);
  FD_TEST(client->streams[0]==g_replacement_stream && g_replacement_stream->request_ctx==5678UL);
  FD_TEST(fd_h2_rbuf_used_sz(client->frame_rx)==sizeof(fd_h2_frame_hdr_t));
  FD_TEST(!client->conn->rx_hdrs_observed && !client->has_block_deadline && !g_mono_jump_after);
  g_reset_callback=0;
  FD_TEST(!close(sock[0]) && !close(sock[1]));
}

FD_UNIT_TEST( discarded_completion_next_block_and_reused_owner ) {
  fd_grpc_h2_stream_t * old=test_cancel_header_owner(1);
  fd_grpc_h2_stream_t * replacement=fd_grpc_client_stream_acquire(client,5678UL);
  FD_TEST(replacement==old && replacement->s.stream_id==3U);
  int sock[2];FD_TEST(!socketpair(AF_UNIX,SOCK_STREAM,0,sock));
  test_socket_write_frame(sock[1],FD_H2_FRAME_TYPE_CONTINUATION,FD_H2_FLAG_END_HEADERS,
                          1U,complete_headers+5UL,sizeof(complete_headers)-5UL);
  test_socket_write_frame(sock[1],FD_H2_FRAME_TYPE_HEADERS,0U,3U,complete_headers,1UL);
  int busy=0;
  FD_TEST(fd_grpc_client_rxtx_socket(client,sock[0],300L,&busy)==0);
  FD_TEST(!g_conn_dead_cnt && !g_rx_start_cnt && !g_rx_end_cnt && client->stream_cnt==1UL);
  FD_TEST(client->header_block_stream_id==3U && client->header_block_used==1UL);
  FD_TEST(client->has_block_deadline && client->block_deadline_mono==g_mono_now+5000000000L);
  test_socket_write_frame(sock[1],FD_H2_FRAME_TYPE_CONTINUATION,FD_H2_FLAG_END_HEADERS,
                          3U,complete_headers+1UL,sizeof(complete_headers)-1UL);
  test_socket_write_frame(sock[1],FD_H2_FRAME_TYPE_HEADERS,FD_H2_FLAG_END_HEADERS|FD_H2_FLAG_END_STREAM,
                          3U,ok_trailers,sizeof(ok_trailers));
  FD_TEST(fd_grpc_client_rxtx_socket(client,sock[0],400L,&busy)==0);
  FD_TEST(g_rx_start_cnt==1UL && g_rx_end_cnt==1UL && g_cb_resp_hdrs.grpc_status==FD_GRPC_STATUS_OK);
  FD_TEST(!client->stream_cnt && !client->has_block_deadline && !client->header_block_stream_id);
  FD_TEST(!close(sock[0]) && !close(sock[1]));
}

/* Exercise timeout callbacks that acquire a replacement request.  The
   early-release variant is a private stress control for exact pool-address
   reuse; the ordinary acquire variant leaves A's release to the client. */
static void
timeout_acquire_request( void ) {
  FD_TEST(g_cb_request_ctx==1234UL);
  FD_TEST(g_timeout_details.deadline_kind==FD_GRPC_DEADLINE_HEADER);
  g_timeout_acquire_cnt++;
  if( g_timeout_acquire_kind==3 ) {
    fd_grpc_client_reset(client);
    test_grpc_client_mock_conn(client);
  } else if( g_timeout_acquire_kind==2 ) {
    /* The timeout has already closed A's H2 state. */
    FD_TEST(g_timeout_old->s.state==FD_H2_STREAM_STATE_CLOSED);
    fd_grpc_client_stream_release(client,g_timeout_old);
  }
  FD_TEST(fd_grpc_client_stream_acquire_is_safe(client));
  g_timeout_new=fd_grpc_client_stream_acquire(client,1234UL); /* request_ctx reuse */
  fd_grpc_client_deadline_set(g_timeout_new,FD_GRPC_DEADLINE_HEADER,LONG_MAX);
  if( g_timeout_seed_rx ) {
    fd_h2_frame_hdr_t hdr=test_incomplete_headers( 0U, g_timeout_new->s.stream_id );
    fd_h2_rbuf_push(client->frame_rx,&hdr,sizeof(hdr));
  }
}

static void
timeout_socket( int pair[2] ) {
  FD_TEST(!socketpair(AF_UNIX,SOCK_STREAM,0,pair));
}

static void
timeout_close_socket( int pair[2] ) {
  FD_TEST(!close(pair[0]) && !close(pair[1]));
}

static void
timeout_begin( int pair[2] ) {
  g_timeout_acquire_kind=0;g_timeout_seed_rx=0;g_timeout_acquire_cnt=0UL;
  g_timeout_old=new_test_stream_at_mono(100000000000L);g_timeout_new=NULL;
  fd_grpc_client_deadline_set(g_timeout_old,FD_GRPC_DEADLINE_HEADER,2000L);
  timeout_socket(pair);
  test_socket_write_frame(pair[1],FD_H2_FRAME_TYPE_HEADERS,0U,1U,complete_headers,5UL);
  int busy=0;
  FD_TEST(fd_grpc_client_rxtx_socket(client,pair[0],1000L,&busy)==0);
  FD_TEST(client->has_block_deadline && client->conn->rx_hdrs_observed);
  FD_TEST(client->header_block_stream_id==1U);
  FD_TEST(!g_rx_start_cnt && !g_rx_end_cnt && !g_conn_dead_cnt);
  FD_TEST(client->block_deadline_mono==105000000000L);
}

static void
timeout_fill_tx( void ) {
  static uchar const filler[4096]={0};
  ulong free=fd_h2_rbuf_free_sz(client->frame_tx);
  FD_TEST(free<=sizeof(filler));
  fd_h2_rbuf_push(client->frame_tx,filler,free);
  FD_TEST(!fd_h2_rbuf_free_sz(client->frame_tx));
}

static void
timeout_complete_b( int pair[2], uint b_id, int has_old_cont ) {
  if( has_old_cont ) {
    test_socket_write_frame(pair[1],FD_H2_FRAME_TYPE_CONTINUATION,FD_H2_FLAG_END_HEADERS,
        1U,complete_headers+5UL,sizeof(complete_headers)-5UL);
  }
  test_socket_write_frame(pair[1],FD_H2_FRAME_TYPE_HEADERS,0U,b_id,complete_headers,1UL);
  g_mono_now=100002000000L;
  int busy=0;
  FD_TEST(fd_grpc_client_rxtx_socket(client,pair[0],3000L,&busy)==0);
  FD_TEST(client->stream_cnt==1UL && client->streams[0]==g_timeout_new);
  FD_TEST(client->stream_ids[0]==b_id && g_timeout_new->request_ctx==1234UL);
  FD_TEST(client->header_block_stream_id==b_id);
  FD_TEST(client->header_block_used==1UL && client->has_block_deadline);
  FD_TEST(client->block_deadline_mono==105002000000L);
  FD_TEST(!g_rx_start_cnt && !g_rx_end_cnt && !g_conn_dead_cnt);
  static uchar const msg[]={0U,0U,0U,0U,1U,'v'};
  test_socket_write_frame(pair[1],FD_H2_FRAME_TYPE_CONTINUATION,FD_H2_FLAG_END_HEADERS,
      b_id,complete_headers+1UL,sizeof(complete_headers)-1UL);
  test_socket_write_frame(pair[1],FD_H2_FRAME_TYPE_DATA,0U,b_id,msg,sizeof(msg));
  test_socket_write_frame(pair[1],FD_H2_FRAME_TYPE_HEADERS,FD_H2_FLAG_END_HEADERS|FD_H2_FLAG_END_STREAM,
      b_id,ok_trailers,sizeof(ok_trailers));
  g_mono_now=100003000000L;
  FD_TEST(fd_grpc_client_rxtx_socket(client,pair[0],4000L,&busy)==0);
  FD_TEST(g_rx_start_cnt==1UL && g_rx_msg_cnt==1UL && g_rx_end_cnt==1UL);
  FD_TEST(g_cb_resp_hdrs.grpc_status==FD_GRPC_STATUS_OK && g_cb_request_ctx==1234UL);
  FD_TEST(!client->stream_cnt && !client->has_block_deadline && !client->header_block_stream_id);
}

FD_UNIT_TEST( timeout_callback_replacement_matrix ) {
  ulong cases=0UL, successes=0UL, fatal=0UL, rollbacks=0UL;
  /* Spare TX, full TX, and rollback before the canceled block completes. */
  for( int hook=1; hook<=3; hook++ ) for( int mode=0; mode<3; mode++ ) {
    if( hook==3 && mode==2 ) continue; /* Reset removed the old block entirely. */
    int full=mode==1, rollback=mode==2;
    int pair[2];timeout_begin(pair);
    ulong generation=client->generation;
    ulong serial=client->observed_header_serial;
    long block_deadline=client->block_deadline_mono;
    if(full) timeout_fill_tx();
    g_timeout_acquire_kind=hook;
    g_mono_now=100001000000L;
    fd_grpc_client_service_deadlines(client,2000L);
    g_timeout_acquire_kind=0;
    FD_TEST(g_timeout_acquire_cnt==1UL && g_timeout_new && g_timeout_new->request_ctx==1234UL);
    FD_TEST(!g_rx_start_cnt && !g_rx_end_cnt);
    if(hook==3) {
      FD_TEST(client->generation==generation+1UL);
      FD_TEST(g_timeout_new==g_timeout_old && g_timeout_new->s.stream_id==1U);
      FD_TEST(client->stream_cnt==1UL && client->streams[0]==g_timeout_new);
      FD_TEST(!client->has_block_deadline && !client->conn->rx_hdrs_observed && !g_conn_dead_cnt);
      timeout_close_socket(pair);timeout_socket(pair); /* new logical transport */
      timeout_complete_b(pair,1U,0);successes++;
    } else {
      FD_TEST(client->generation==generation && g_timeout_new->s.stream_id==3U);
      FD_TEST((g_timeout_new==g_timeout_old)==(hook==2));
      if(full) {
        FD_TEST(g_conn_dead_cnt==1UL && g_conn_dead_error==FD_H2_ERR_CANCEL);
        FD_TEST(client->conn->flags & FD_H2_CONN_FLAGS_DEAD);
        FD_TEST(!client->has_block_deadline);fatal++;
      } else {
        FD_TEST(!g_conn_dead_cnt && client->stream_cnt==1UL && client->streams[0]==g_timeout_new);
        FD_TEST(client->has_block_deadline && client->observed_header_serial==serial);
        FD_TEST(client->block_deadline_mono==block_deadline && client->conn->rx_hdrs_discard);
        FD_TEST(!client->header_block_stream_id);
        if( rollback ) {
          g_mono_now=105000000000L;
          fd_grpc_client_service_deadlines(client,-1000000000L);
          FD_TEST(g_conn_dead_cnt==1UL && g_conn_dead_error==FD_H2_ERR_CANCEL);
          FD_TEST((client->conn->flags & FD_H2_CONN_FLAGS_DEAD) && !client->has_block_deadline);
          FD_TEST(g_timeout_acquire_cnt==1UL && !g_rx_start_cnt && !g_rx_end_cnt);
          rollbacks++;
        } else { timeout_complete_b(pair,3U,1);successes++; }
      }
    }
    timeout_close_socket(pair);cases+=(ulong)!rollback;
  }
  FD_TEST(cases==6UL && successes==4UL && fatal==2UL && rollbacks==2UL);
  FD_LOG_NOTICE(("timeout_callback cases=%lu successful_B=%lu same_generation_full_TX_fatal=%lu",cases,successes,fatal));
  FD_LOG_NOTICE(("canceled_block_rollback cases=%lu",rollbacks));
}

FD_UNIT_TEST( timeout_callback_reset_stops_old_rx ) {
  int pair[2];timeout_begin(pair);
  ulong generation=client->generation;
  fd_grpc_client_deadline_set(g_timeout_old,FD_GRPC_DEADLINE_HEADER,1200L);
  test_socket_write_frame(pair[1],FD_H2_FRAME_TYPE_CONTINUATION,FD_H2_FLAG_END_HEADERS,
      1U,complete_headers+5UL,sizeof(complete_headers)-5UL);
  g_timeout_acquire_kind=3;g_timeout_seed_rx=1;g_mono_jump_ns=1000L;g_mono_jump_after=2U;
  int busy=0;
  FD_TEST(fd_grpc_client_rxtx_socket(client,pair[0],1100L,&busy)==-1);
  FD_TEST(!g_mono_jump_after && g_timeout_acquire_cnt==1UL && !g_conn_dead_cnt);
  FD_TEST(client->generation==generation+1UL && client->stream_cnt==1UL);
  FD_TEST(g_timeout_new==g_timeout_old && client->streams[0]==g_timeout_new);
  FD_TEST(g_timeout_new->s.stream_id==1U && g_timeout_new->request_ctx==1234UL);
  FD_TEST(!client->conn->rx_hdrs_observed && !client->has_block_deadline);
  FD_TEST(!g_rx_start_cnt && !g_rx_end_cnt && !g_rx_msg_cnt);
  FD_TEST(fd_h2_rbuf_used_sz(client->frame_rx)==sizeof(fd_h2_frame_hdr_t));
  fd_h2_rbuf_t peek=*client->frame_rx;fd_h2_frame_hdr_t hdr;
  fd_h2_rbuf_pop_copy(&peek,&hdr,sizeof(hdr));
  FD_TEST(fd_h2_frame_type(hdr.typlen)==FD_H2_FRAME_TYPE_HEADERS);
  FD_TEST(fd_h2_frame_length(hdr.typlen)==100U && fd_uint_bswap(hdr.r_stream_id)==1U);
  g_timeout_acquire_kind=0;g_timeout_seed_rx=0;g_mono_jump_ns=5000000000L;
  timeout_close_socket(pair);
  FD_LOG_NOTICE(("timeout_reset_during_completed_CONT cases=1 fresh_RX_bytes=%lu old_generation=%lu new_generation=%lu",fd_h2_rbuf_used_sz(client->frame_rx),generation,client->generation));
}



FD_UNIT_TEST( monotonic_block_rollback_and_request_epoch ) {
  new_test_stream_at_mono(10000000000L);
  int sock[2];timeout_socket(sock);
  fd_h2_frame_hdr_t hdr=test_incomplete_headers( 0U, 1U );
  FD_TEST(write(sock[1],&hdr,sizeof(hdr))==(long)sizeof(hdr));
  int busy=0;
  FD_TEST(!fd_grpc_client_rxtx_socket(client,sock[0],100L,&busy));
  ulong serial=client->observed_header_serial;
  long deadline=client->block_deadline_mono;
  for(long step=1L;step<=5L;step++) {
    g_mono_now=10000000000L+step*1000000000L;
    fd_grpc_client_service_deadlines(client,100L-step*1000000000L);
    FD_TEST(client->observed_header_serial==serial && client->block_deadline_mono==deadline);
    FD_TEST(!!(client->conn->flags & FD_H2_CONN_FLAGS_DEAD)==(step==5L));
  }
  FD_TEST(g_conn_dead_cnt==1UL && !g_rx_start_cnt && !g_rx_end_cnt);
  timeout_close_socket(sock);

  /* A complete block arriving after rollback still cannot commit late. */
  new_test_stream_at_mono(20000000000L);
  test_rx_frame(FD_H2_FRAME_TYPE_HEADERS,FD_H2_FLAG_END_STREAM,1U,complete_headers,1UL);
  fd_grpc_client_service_deadlines(client,-10000000000L);
  g_mono_now+=6000000000L;
  test_rx_frame(FD_H2_FRAME_TYPE_CONTINUATION,FD_H2_FLAG_END_HEADERS,1U,
                complete_headers+1UL,sizeof(complete_headers)-1UL);
  FD_TEST(g_conn_dead_cnt==1UL && !g_rx_start_cnt && !g_rx_end_cnt);

  /* New public request deadlines retain their caller epoch after rollback. */
  fd_grpc_h2_stream_t * stream=new_test_stream_at_mono(30000000000L);
  long wall=-10000000000L;
  fd_grpc_client_service_deadlines(client,wall);
  fd_grpc_client_deadline_set(stream,FD_GRPC_DEADLINE_HEADER,wall+1000000000L);
  g_timeout_details.deadline_kind=-1;
  g_mono_now+=500000000L;
  fd_grpc_client_service_deadlines(client,wall+500000000L);
  FD_TEST(client->stream_cnt==1UL && g_timeout_details.deadline_kind==-1);
  g_mono_now+=500000000L;
  fd_grpc_client_service_deadlines(client,wall+1000000000L);
  FD_TEST(!client->stream_cnt && g_timeout_details.deadline_kind==FD_GRPC_DEADLINE_HEADER);
}

FD_UNIT_TEST( projected_max_and_saturated_monotonic_expiry ) {
  for(int saturated=0;saturated<2;saturated++) {
    new_test_stream_at_mono(saturated ? LONG_MAX-2000000000L : 10000000000L);
    int sock[2];timeout_socket(sock);
    fd_h2_frame_hdr_t hdr=test_incomplete_headers( 0U, 1U );
    FD_TEST(write(sock[1],&hdr,sizeof(hdr))==(long)sizeof(hdr));
    int busy=0;long wall=saturated ? -10000000000L : LONG_MAX-10L;
    FD_TEST(!fd_grpc_client_rxtx_socket(client,sock[0],wall,&busy));
    FD_TEST(client->has_block_deadline && client->conn->rx_hdrs_observed);
    ulong reads=g_mono_reads;
    if(saturated) {
      FD_TEST(client->block_deadline_mono==LONG_MAX);
      g_mono_now=LONG_MAX-1L;
      fd_grpc_client_service_deadlines(client,wall);
      FD_TEST(!(client->conn->flags & FD_H2_CONN_FLAGS_DEAD));
      g_mono_now=LONG_MAX;
    } else {
      FD_TEST(fd_grpc_client_next_deadline(client)==LONG_MAX && g_mono_reads==reads);
      g_mono_now+=5000000000L;
    }
    fd_grpc_client_service_deadlines(client,wall);
    FD_TEST(g_conn_dead_cnt==1UL && (client->conn->flags & FD_H2_CONN_FLAGS_DEAD));
    FD_TEST(!g_rx_start_cnt && !g_rx_end_cnt);
    timeout_close_socket(sock);
  }
}

FD_UNIT_TEST( conservative_service_entry_can_precede_wire_arrival ) {
  fd_grpc_h2_stream_t * stream=new_test_stream_at_mono(20000000000L);
  fd_grpc_client_deadline_set(stream,FD_GRPC_DEADLINE_HEADER,60000000100L);
  g_timeout_details.deadline_kind=-1;
  int sock[2];timeout_socket(sock);
  fd_h2_frame_hdr_t hdr={.typlen=fd_h2_frame_typlen(FD_H2_FRAME_TYPE_HEADERS,sizeof(complete_headers)),
                        .r_stream_id=fd_uint_bswap(1U),
                        .flags=FD_H2_FLAG_END_STREAM|FD_H2_FLAG_END_HEADERS};
  g_after_entry_wire_sz=sizeof(hdr)+sizeof(complete_headers);
  FD_TEST(g_after_entry_wire_sz<=sizeof(g_after_entry_wire));
  fd_memcpy(g_after_entry_wire,&hdr,sizeof(hdr));
  fd_memcpy(g_after_entry_wire+sizeof(hdr),complete_headers,sizeof(complete_headers));
  g_after_entry_cnt=0U;g_after_entry_fd=sock[1];
  int busy=0;
  FD_TEST(fd_grpc_client_rxtx_socket(client,sock[0],100L,&busy)==-1);
  FD_TEST(g_after_entry_fd==-1 && g_after_entry_cnt==1U && g_mono_now==26000000000L);
  FD_TEST(g_conn_dead_cnt==1UL && g_conn_dead_error==FD_H2_ERR_CANCEL);
  FD_TEST(!g_rx_start_cnt && !g_rx_end_cnt && g_timeout_details.deadline_kind==-1);
  timeout_close_socket(sock);
}

FD_UNIT_TEST( pure_deadline_projection_full_width ) {
#if FD_HAS_INT128
  static long const values[]={LONG_MIN,LONG_MIN+1L,-10000000000L,-1L,0L,1L,
                              5000000000L,LONG_MAX-5000000000L,LONG_MAX-1L,LONG_MAX};
  fd_grpc_client_t projection={0};projection.has_block_deadline=1U;
  ulong cases=0UL,reads=g_mono_reads;
  for(ulong w=0UL;w<sizeof(values)/sizeof(values[0]);w++)
    for(ulong m=0UL;m<sizeof(values)/sizeof(values[0]);m++)
      for(ulong d=0UL;d<sizeof(values)/sizeof(values[0]);d++) {
        projection.now_nanos=values[w];projection.now_mono=values[m];projection.block_deadline_mono=values[d];
        int128 remaining=(int128)values[d]-(int128)values[m];
        if(remaining<0)remaining=0;
        int128 expected=(int128)values[w]+remaining;
        if(expected>LONG_MAX)expected=LONG_MAX;
        FD_TEST(fd_grpc_client_next_deadline(&projection)==(long)expected);cases++;
      }
  FD_TEST(g_mono_reads==reads);
  FD_LOG_NOTICE(("pure_deadline_projection cases=%lu clock_reads=0",cases));
#endif
}

static ulong
test_literal_field( uchar * buf, char const * name, ulong n, char const * value, ulong v ) {
  FD_TEST(n<127UL && v<127UL);ulong o=0UL;buf[o++]=0;buf[o++]=(uchar)n;fd_memcpy(buf+o,name,n);o+=n;
  buf[o++]=(uchar)v;fd_memcpy(buf+o,value,v);return o+v;
}
FD_UNIT_TEST( invalid_metadata_all_splits ) {
  static struct { char const * name; ulong n; char const * value; ulong v; int valid; } const cases[]={
    {"X",1,"v",1,0},{"x x",3,"v",1,0},{"",0,"v",1,0},{"x:x",3,"v",1,0},
    {"x",1,"x\0x",3,0},{"x",1,"x\rx",3,0},{"x",1,"x\nx",3,0},{"x",1," v",2,0},
    {"connection",10,"close",5,0},{"te",2,"trailers",8,0},
    {"x!#$%&'*+-.^_`|~09az",20,"",0,1},{"x-bin",5,"x\t x",4,1},{"x",1,"\x80\xff",2,1}
  };
  ulong checks=0UL;
  for( ulong c=0UL;c<sizeof(cases)/sizeof(cases[0]);c++ ) for( int trailer=0;trailer<2;trailer++ ) {
    uchar block[512];ulong sz=0UL;
    if( !trailer ) { block[sz++]=0x88;sz+=test_literal_field(block+sz,"content-type",12,"application/grpc",16); }
    sz+=test_literal_field(block+sz,cases[c].name,cases[c].n,cases[c].value,cases[c].v);
    sz+=test_literal_field(block+sz,"grpc-status",11,"0",1);
    checks+=test_terminal_header_splits(block,sz,trailer,cases[c].valid);
    if( !cases[c].valid ) {
      new_test_stream();block[sz++]=0x80;
      test_rx_frame(FD_H2_FRAME_TYPE_HEADERS,FD_H2_FLAG_END_STREAM|FD_H2_FLAG_END_HEADERS,1U,block,sz);
      FD_TEST(client->conn->conn_error==FD_H2_ERR_COMPRESSION && !g_rx_end_cnt);checks++;
    }
  }
  FD_LOG_NOTICE(("probe framed_parser_checks=%lu",checks));
}


FD_UNIT_TEST( huffman_metadata_all_splits ) {
  /* Common zero-table Huffman content-type and grpc-status surround one field. */
  static uchar const prefix[]={0x88,0x00,0x89,0x21,0xea,0x49,0x6a,0x4a,0xc9,0xf5,0x59,0x7f,0x8b,0x1d,0x75,0xd0,0x62,0x0d,0x26,0x3d,0x4c,0x4d,0x65,0x64,0x00};
  static uchar const suffix[]={0x00,0x88,0x9a,0xca,0xc8,0xb2,0x12,0x34,0xda,0x8f,0x81,0x07};
  static struct { ulong len; int valid; uchar field[12]; } const cases[]={
    { 4UL, 0, { 0x81,0xfc,0x81,0xef } },
    { 7UL, 0, { 0x81,0xf3,0x84,0xf3,0xff,0x8f,0x3f } },
    { 9UL, 0, { 0x81,0xf3,0x86,0xf3,0xff,0xff,0xff,0xef,0x9f } },
    { 5UL, 0, { 0x81,0xf3,0x82,0x53,0xbf } },
    { 10UL, 0, { 0x82,0x49,0x7f,0x86,0x4d,0x83,0x35,0x05,0xb1,0x1f } },
    { 12UL, 1, { 0x84,0xf2,0xb4,0x66,0xab,0x86,0xf3,0xff,0xff,0xd4,0xa7,0x9f } },
    { 9UL, 1, { 0x81,0xf3,0x86,0xff,0xfe,0x6f,0xff,0xff,0xbb } },
    { 3UL, 1, { 0x81,0xf3,0x80 } },
  };
  ulong checks=0UL;
  for( ulong c=0UL;c<sizeof(cases)/sizeof(cases[0]);c++ ) {
    uchar block[sizeof(prefix)+12UL+sizeof(suffix)];
    memcpy(block,prefix,sizeof(prefix));
    memcpy(block+sizeof(prefix),cases[c].field,cases[c].len);
    memcpy(block+sizeof(prefix)+cases[c].len,suffix,sizeof(suffix));
    checks+=test_terminal_header_splits(block,sizeof(prefix)+cases[c].len+sizeof(suffix),0,cases[c].valid);
  }
  FD_LOG_NOTICE(("probe fragmented_huffman_checks=%lu",checks));
}


/* A semantic failure on A is local; a compression suffix is fatal to B too. */
FD_UNIT_TEST( invalid_metadata_compression_and_other_stream ) {
  static uchar const bad_value[] = {0x40,1,'x',1,'\n'};
  static uchar const bad_status[] = {0x40,7,':','s','t','a','t','u','s',1,0};
  static uchar const bad_huffman[] = {0,1,'z',0x81,0xff};
  ulong framed=0UL, recovered=0UL;
  for( uint field=0U; field<2U; field++ ) {
    uchar block[128];
    memcpy(block,complete_headers,sizeof(complete_headers));
    uchar const * bad=field ? bad_status : bad_value;
    ulong bad_sz=field ? sizeof(bad_status) : sizeof(bad_value);
    memcpy(block+sizeof(complete_headers),bad,bad_sz);
    ulong base_sz=sizeof(complete_headers)+bad_sz;
    for( uint suffix=0U; suffix<5U; suffix++ ) {
      ulong n=base_sz;
      if( suffix==1U ) block[n++]=0xbe; /* Illegal dynamic index62 at table size0. */
      if( suffix==2U ) block[n++]=0x21; /* Illegal increase above advertised0. */
      if( suffix==3U ) block[n++]=0x20; /* Size update after a field. */
      if( suffix==4U ) { memcpy(block+n,bad_huffman,sizeof(bad_huffman)); n+=sizeof(bad_huffman); }
      fd_grpc_h2_stream_t * a;
      for( ulong split=0UL; split<=n+1UL; split++ ) {
        a=new_test_stream();
        fd_grpc_h2_stream_t * b=fd_grpc_client_stream_acquire(client,4321UL);
        FD_TEST(a && b && a->s.stream_id==1U && b->s.stream_id==3U);
        ulong generation=client->generation;
        if( split<=n ) {
          test_rx_frame(FD_H2_FRAME_TYPE_HEADERS,FD_H2_FLAG_END_STREAM,1U,block,split);
          /* Malformed Huffman can fail before END_HEADERS, so finish only live input. */
          if( !client->conn->conn_error )
            test_rx_frame(FD_H2_FRAME_TYPE_CONTINUATION,FD_H2_FLAG_END_HEADERS,1U,block+split,n-split);
        } else test_rx_frame(FD_H2_FRAME_TYPE_HEADERS,FD_H2_FLAG_END_STREAM|FD_H2_FLAG_END_HEADERS,1U,block,n);
        FD_TEST(client->generation==generation);
        if( suffix ) {
          FD_TEST(client->conn->conn_error==FD_H2_ERR_COMPRESSION);
          FD_TEST(!g_rx_start_cnt && !g_rx_end_cnt);
          test_rx_frame(FD_H2_FRAME_TYPE_HEADERS,FD_H2_FLAG_END_STREAM|FD_H2_FLAG_END_HEADERS,
                        3U,complete_headers,sizeof(complete_headers));
          FD_TEST(!g_rx_start_cnt && !g_rx_end_cnt);
        } else {
          FD_TEST(!client->conn->conn_error && client->stream_cnt==1UL);
          FD_TEST(!g_rx_start_cnt && g_rx_end_cnt==1UL && g_cb_resp_hdrs.grpc_status==FD_GRPC_STATUS_INTERNAL);
          FD_TEST(client->streams[0]==b && !b->hdrs_received && !b->hdrs.h2_status);
          test_rx_frame(FD_H2_FRAME_TYPE_HEADERS,FD_H2_FLAG_END_STREAM|FD_H2_FLAG_END_HEADERS,
                        3U,complete_headers,sizeof(complete_headers));
          FD_TEST(!client->conn->conn_error && !client->stream_cnt);
          FD_TEST(g_rx_start_cnt==1UL && g_rx_end_cnt==2UL);
          FD_TEST(g_cb_request_ctx==4321UL && g_cb_resp_hdrs.grpc_status==FD_GRPC_STATUS_OK);
          recovered++;
        }
        framed++;
      }
    }
  }
  FD_LOG_NOTICE(("metadata_scope framed=%lu semantic_stream_recovery=%lu",framed,recovered));
}


/* The last deadline service may close without resetting the connection.
   Neither plaintext nor TLS record TX may flush queued bytes afterward. */
FD_UNIT_TEST( post_stream_service_dead_prevents_tx ) {
  for( int tls=0; tls<2; tls++ ) {
    new_test_stream();
    client->request_stream=NULL;
    *client->request_tx_op=(fd_h2_tx_op_t){0};
    fd_h2_frame_hdr_t incomplete=test_incomplete_headers( 0U, 1U );
    fd_h2_rbuf_push(client->frame_rx,&incomplete,sizeof(incomplete));
    ulong cookie=0x0123456789abcdefUL;
    fd_h2_tx(client->frame_tx,(uchar const *)&cookie,sizeof(cookie),FD_H2_FRAME_TYPE_PING,0U,0U);
    int pair[2];FD_TEST(!socketpair(AF_UNIX,SOCK_STREAM,0,pair));
    fd_tls_t config={0};fd_tlsrec_conn_t record[1];
    fd_tlsrec_conn_init(record,&config,0);
    uchar write_secret[32]={1},read_secret[32]={2};
    if(tls) {test_tls_install_keys(record,write_secret,read_secret);record->hs.cli.alpn_negotiated=1;}
    ulong before=g_mono_reads;
    ulong generation=client->generation;
    g_mono_jump_after=4U; /* first post-RX check passes; service_streams then expires */
    int busy=0;
    int rc=tls ? fd_grpc_client_rxtx_tls(client,record,pair[0],100L,&busy)
               : fd_grpc_client_rxtx_socket(client,pair[0],100L,&busy);
    uchar wire[128];errno=0;
    long emitted=(long)recv(pair[1],wire,sizeof(wire),MSG_DONTWAIT);
    int recv_errno=errno;
    FD_TEST(!g_mono_jump_after && g_conn_dead_cnt==1UL && g_conn_dead_error==FD_H2_ERR_CANCEL);
    FD_TEST(client->generation==generation && (client->conn->flags&FD_H2_CONN_FLAGS_DEAD));
    FD_TEST(rc==-1 && emitted==-1 && recv_errno==EAGAIN);
    FD_LOG_NOTICE(("post_stream_service_dead tls=%d return=%d dead=%lu emitted=%ld errno=%d mono_reads=%lu",tls,rc,g_conn_dead_cnt,emitted,recv_errno,g_mono_reads-before));
    FD_TEST(!close(pair[0]) && !close(pair[1]));
  }
}


int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  static uchar client_mem[ 524288 ] __attribute__((aligned(128)));
  ulong const buf_max = 4096UL;
  FD_TEST( fd_grpc_client_footprint( buf_max )<=sizeof(client_mem) );

  fd_grpc_client_callbacks_t callbacks = {
    .conn_dead  = cb_conn_dead,
    .rx_start   = cb_rx_start,
    .rx_msg     = cb_rx_msg,
    .rx_end     = cb_rx_end,
    .rx_timeout = cb_rx_timeout
  };
  fd_grpc_client_metrics_t metrics = {0};
  void * app_ctx = (void *)( 0x1234UL );
  ulong rng_seed = 1UL;
  client = fd_grpc_client_new( client_mem, &callbacks, &metrics, app_ctx, buf_max, rng_seed );
  FD_TEST( client );

  fd_unit_tests( argc, argv );

  fd_grpc_client_delete( client );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}

#endif
