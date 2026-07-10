#define _GNU_SOURCE

#include "fd_bam_tile.h"
#include "test_bam_common.c"
#include "../../ballet/ed25519/fd_ed25519.h"
#include "../../ballet/nanopb/pb_encode.h"
#include "../../ballet/nanopb/pb_decode.h"
#include "../../discof/gossip/fd_gossip_tile.h"
#include "../../discof/replay/fd_replay_tile.h"
#include "../bundle/fd_bundle_crank.h"
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>

static uchar metrics_scratch[ FD_METRICS_FOOTPRINT( 0UL ) ] __attribute__((aligned( FD_METRICS_ALIGN )));

FD_IMPORT_BINARY( bam_dump_txn_fixture, "src/ballet/txn/fixtures/transaction2.bin" );

/* Test-only shims implemented in fd_bam_tile.c so this unit test can
   drive internal BAM tile paths without exposing them through production
   headers. */
extern void
fd_bam_test_receive_ingress_frag( fd_bam_tile_t * ctx,
                                  ulong           in_idx,
                                  ulong           sig,
                                  ulong           chunk,
                                  ulong           sz );

extern void
fd_bam_test_metrics_write( fd_bam_tile_t * ctx );

extern void
fd_bam_test_before_credit( fd_bam_tile_t *    ctx,
                           fd_stem_context_t * stem,
                           int *               charge_busy );

/* Applies a BAM fee configuration to the pack crank state. Updates
   tip-receiver destinations stored in |crank3| and |crank2| and writes
   a clamped copy of commission_bps into |crank3| when a new version is
   observed. |cfg_version| is updated to the applied version and is
   used to detect duplicates. If |crank_enabled| is zero, the call is a
   no-op. */
static inline void
fd_pack_apply_bam_fee_cfg_impl( fd_bam_fee_cfg_t *      cfg,
                                ulong *                 cfg_version,
                                int                     crank_enabled,
                                fd_bundle_crank_3_t *   crank3,
                                fd_bundle_crank_2_t *   crank2 ) {
  if( FD_UNLIKELY( !cfg ) ) return;
  if( FD_UNLIKELY( !crank_enabled ) ) return;

  ulong version = FD_VOLATILE_CONST( cfg->version );
  if( FD_UNLIKELY( !version || version==*cfg_version ) ) return;

  *cfg_version = version;

  if( FD_LIKELY( cfg->has_prio_fee_recipient ) ) {
    fd_memcpy( crank3->new_tip_receiver,
               cfg->prio_fee_recipient,
               sizeof( cfg->prio_fee_recipient ) );
    fd_memcpy( crank2->new_tip_receiver,
               cfg->prio_fee_recipient,
               sizeof( cfg->prio_fee_recipient ) );
  }

  crank3->init_tip_distribution_acct.commission_bps =
      (ushort)fd_uint_min( cfg->commission_bps, 10000U );
}


__attribute__((weak)) char const fdctl_version_string[] = "0.0.0";
__attribute__((weak)) ulong const firedancer_major_version = 0UL;
__attribute__((weak)) ulong const firedancer_minor_version = 0UL;
__attribute__((weak)) ulong const firedancer_patch_version = 0UL;
__attribute__((weak)) uint  const firedancer_commit_ref    = 0U;

static long g_clock = 1L;

__attribute__((weak)) long
fd_bam_now( void ) {
  return g_clock;
}

static void
test_bam_fill_full_txn_batches( bam_types_Packet             (* packets)[ FD_BAM_MAX_TXN_PER_ATOMIC_BATCH ],
                                test_bam_packet_encode_ctx_t *  packet_ctx,
                                bam_types_AtomicTxnBatch *      batches,
                                ulong                           batch_cnt,
                                uint                            seq_base,
                                ulong                           slot_base ) {
  for( ulong i=0UL; i<batch_cnt; i++ ) {
    fd_memset( packets[ i ], 0, sizeof(packets[ i ]) );
    for( ulong j=0UL; j<FD_BAM_MAX_TXN_PER_ATOMIC_BATCH; j++ ) {
      packets[ i ][ j ].data.size      = 1U;
      packets[ i ][ j ].data.bytes[0]  = (uchar)( 'A' + (int)(( i * FD_BAM_MAX_TXN_PER_ATOMIC_BATCH + j ) % 26UL) );
      packets[ i ][ j ].has_meta       = 1U;
      packets[ i ][ j ].meta.has_flags = 1U;
      packets[ i ][ j ].meta.flags.revert_on_error = 1U;
    }

    packet_ctx[ i ] = (test_bam_packet_encode_ctx_t){
      .packets    = packets[ i ],
      .packet_cnt = FD_BAM_MAX_TXN_PER_ATOMIC_BATCH
    };

    batches[ i ] = (bam_types_AtomicTxnBatch)bam_types_AtomicTxnBatch_init_default;
    batches[ i ].seq_id            = (uint32_t)( seq_base + (uint)i );
    batches[ i ].max_schedule_slot = slot_base + i;
    batches[ i ].packets.funcs.encode = test_bam_encode_packets_cb;
    batches[ i ].packets.arg          = &packet_ctx[ i ];
  }
}

static void
test_bam_fill_single_txn_batches( bam_types_Packet *             packets,
                                  test_bam_packet_encode_ctx_t * packet_ctx,
                                  bam_types_AtomicTxnBatch *     batches,
                                  ulong                          batch_cnt,
                                  uint                           seq_base,
                                  ulong                          slot_base ) {
  for( ulong i=0UL; i<batch_cnt; i++ ) {
    packets[ i ] = (bam_types_Packet)bam_types_Packet_init_default;
    packets[ i ].data.size      = 1U;
    packets[ i ].data.bytes[ 0 ] = (uchar)( 'a' + (int)( i % 26UL ) );

    packet_ctx[ i ] = (test_bam_packet_encode_ctx_t){
      .packets    = &packets[ i ],
      .packet_cnt = 1UL
    };

    batches[ i ] = (bam_types_AtomicTxnBatch)bam_types_AtomicTxnBatch_init_default;
    batches[ i ].seq_id            = (uint32_t)( seq_base + (uint)i );
    batches[ i ].max_schedule_slot = slot_base + i;
    batches[ i ].packets.funcs.encode = test_bam_encode_packets_cb;
    batches[ i ].packets.arg          = &packet_ctx[ i ];
  }
}

#define TEST_BAM_ADMIN_RPC_MAX_CALLS   16UL
#define TEST_BAM_ADMIN_RPC_REQ_BUF_SZ  512UL
#define TEST_BAM_ADMIN_RPC_RESP_BUF_SZ 2048UL

typedef struct {
  int  rc;
  char response[ TEST_BAM_ADMIN_RPC_RESP_BUF_SZ ];
} test_bam_admin_rpc_reply_t;

static struct {
  char                        paths[ TEST_BAM_ADMIN_RPC_MAX_CALLS ][ PATH_MAX ];
  char                        requests[ TEST_BAM_ADMIN_RPC_MAX_CALLS ][ TEST_BAM_ADMIN_RPC_REQ_BUF_SZ ];
  test_bam_admin_rpc_reply_t  replies[ TEST_BAM_ADMIN_RPC_MAX_CALLS ];
  ulong                       request_cnt;
  ulong                       reply_cnt;
  ulong                       reply_idx;
} test_bam_admin_rpc_mock;
static ulong * test_bam_admin_rpc_expect_fseq_zero;

static void
test_bam_admin_rpc_mock_reset( void ) {
  fd_memset( &test_bam_admin_rpc_mock, 0, sizeof(test_bam_admin_rpc_mock) );
  test_bam_admin_rpc_expect_fseq_zero = NULL;
}

static void
test_bam_admin_rpc_mock_push_reply( int          rc,
                                    char const * response ) {
  FD_TEST( test_bam_admin_rpc_mock.reply_cnt < TEST_BAM_ADMIN_RPC_MAX_CALLS );
  test_bam_admin_rpc_reply_t * reply = &test_bam_admin_rpc_mock.replies[ test_bam_admin_rpc_mock.reply_cnt++ ];
  reply->rc = rc;
  fd_cstr_ncpy( reply->response, response, sizeof( reply->response ) );
}

int
fd_bam_admin_rpc_request( fd_bam_tile_t * ctx,
                          char const * request,
                          char *       response,
                          ulong        response_max ) {
  FD_TEST( test_bam_admin_rpc_mock.reply_idx < test_bam_admin_rpc_mock.reply_cnt );
  FD_TEST( test_bam_admin_rpc_mock.request_cnt < TEST_BAM_ADMIN_RPC_MAX_CALLS );
  if( FD_UNLIKELY( test_bam_admin_rpc_expect_fseq_zero ) ) {
    FD_TEST( fd_fseq_query( test_bam_admin_rpc_expect_fseq_zero ) == 0UL );
  }

  ulong req_idx = test_bam_admin_rpc_mock.request_cnt++;
  fd_cstr_ncpy( test_bam_admin_rpc_mock.paths[ req_idx ], ctx->admin_rpc_path, PATH_MAX );
  fd_cstr_ncpy( test_bam_admin_rpc_mock.requests[ req_idx ], request, TEST_BAM_ADMIN_RPC_REQ_BUF_SZ );

  test_bam_admin_rpc_reply_t const * reply = &test_bam_admin_rpc_mock.replies[ test_bam_admin_rpc_mock.reply_idx++ ];
  if( FD_UNLIKELY( reply->rc ) ) {
    if( FD_LIKELY( response && response_max ) ) response[ 0 ] = '\0';
    return reply->rc;
  }

  FD_TEST( response );
  FD_TEST( response_max );
  ulong response_len = strnlen( reply->response, sizeof( reply->response ) );
  FD_TEST( response_len < response_max );
  fd_cstr_ncpy( response, reply->response, response_max );
  return 0;
}

static ulong
test_hist_total_cnt( fd_histf_t const * hist ) {
  ulong total = 0UL;
  for( ulong i=0UL; i<FD_HISTF_BUCKET_CNT; i++ ) total += fd_histf_cnt( hist, i );
  return total;
}

static fd_bam_contact_update_t
test_bam_read_gossip_update( fd_wksp_t * mem,
                             ulong       chunk ) {
  fd_bam_contact_update_t msg;
  fd_memcpy( &msg, fd_chunk_to_laddr( mem, chunk ), sizeof(fd_bam_contact_update_t) );
  return msg;
}

static fd_bam_shred_update_t
test_bam_read_shred_update( fd_wksp_t * mem,
                            ulong       chunk ) {
  fd_bam_shred_update_t msg;
  fd_memcpy( &msg, fd_chunk_to_laddr( mem, chunk ), sizeof(fd_bam_shred_update_t) );
  return msg;
}

static fd_wksp_t *
test_bam_setup_gossip_out( test_bam_env_t * env ) {
  fd_bam_tile_t * state = env->state;
  fd_wksp_t * gossip_mem = fd_wksp_containing( env->out_dcache );
  state->gossip_out = (fd_bam_out_ctx_t){
      .idx    = 0UL,
      .mem    = gossip_mem,
      .chunk0 = fd_dcache_compact_chunk0( gossip_mem, env->out_dcache ),
      .chunk  = fd_dcache_compact_chunk0( gossip_mem, env->out_dcache ),
      .wmark  = fd_dcache_compact_wmark( gossip_mem, env->out_dcache, FD_TPU_PARSED_MTU )
  };
  return gossip_mem;
}

typedef struct {
  uchar  status_mem[ FD_FSEQ_FOOTPRINT ] __attribute__((aligned(FD_FSEQ_ALIGN)));
  uchar  gossip_mem[ FD_FSEQ_FOOTPRINT ] __attribute__((aligned(FD_FSEQ_ALIGN)));
  void * status_shmem;
  void * gossip_shmem;
  ulong * status;
  ulong * gossip;
} test_bam_handoff_fseqs_t;

static void
test_bam_setup_handoff_fseqs( test_bam_env_t *           env,
                              test_bam_handoff_fseqs_t * fseqs ) {
  fd_memset( fseqs, 0, sizeof(test_bam_handoff_fseqs_t) );
  fseqs->status_shmem = fd_fseq_new( fseqs->status_mem, 0UL );
  fseqs->gossip_shmem = fd_fseq_new( fseqs->gossip_mem, 0UL );
  FD_TEST( fseqs->status_shmem );
  FD_TEST( fseqs->gossip_shmem );
  fseqs->status = fd_fseq_join( fseqs->status_shmem );
  fseqs->gossip = fd_fseq_join( fseqs->gossip_shmem );
  FD_TEST( fseqs->status );
  FD_TEST( fseqs->gossip );
  env->state->bam_status_fseq = fseqs->status;
  env->state->bam_gossip_fseq = fseqs->gossip;
}

static void
test_bam_teardown_handoff_fseqs( test_bam_env_t *           env,
                                 test_bam_handoff_fseqs_t * fseqs ) {
  env->state->bam_status_fseq = NULL;
  env->state->bam_gossip_fseq = NULL;
  FD_TEST( fd_fseq_leave( fseqs->status ) == fseqs->status_shmem );
  FD_TEST( fd_fseq_leave( fseqs->gossip ) == fseqs->gossip_shmem );
  FD_TEST( fd_fseq_delete( fseqs->status_shmem ) == fseqs->status_shmem );
  FD_TEST( fd_fseq_delete( fseqs->gossip_shmem ) == fseqs->gossip_shmem );
}

static void
test_bam_set_bam_tpu( fd_bam_tile_t * state,
                      char const *    tpu_ip,
                      ushort          tpu_port,
                      char const *    fwd_ip,
                      ushort          fwd_port ) {
  uint tpu_addr = 0U;
  uint fwd_addr = 0U;
  FD_TEST( fd_cstr_to_ip4_addr( tpu_ip, &tpu_addr ) );
  FD_TEST( fd_cstr_to_ip4_addr( fwd_ip, &fwd_addr ) );
  state->bam_tpu     = (fd_ip4_port_t){ .addr = tpu_addr, .port = fd_ushort_bswap( tpu_port ) };
  state->bam_tpu_fwd = (fd_ip4_port_t){ .addr = fwd_addr, .port = fd_ushort_bswap( fwd_port ) };
}

static void
zero_meta_ts( fd_frag_meta_t * meta,
              ulong            depth ) {
  for( ulong i=0UL; i<depth; i++ ) {
    meta[ i ].tsorig = 0U;
    meta[ i ].tspub  = 0U;
  }
}

typedef struct {
  uchar const * const * batches;
  size_t const *         batch_sz;
  size_t                 batch_cnt;
} test_bam_raw_batch_encode_ctx_t;

static bool
test_bam_encode_raw_batches_cb( pb_ostream_t *       stream,
                                pb_field_t const *   field,
                                void * const *       arg ) {
  test_bam_raw_batch_encode_ctx_t const * ctx = (test_bam_raw_batch_encode_ctx_t const *)(*arg);
  for( size_t i=0UL; i<ctx->batch_cnt; i++ ) {
    if( FD_UNLIKELY( !pb_encode_tag_for_field( stream, field ) ) ) return false;
    if( FD_UNLIKELY( !pb_encode_string( stream, ctx->batches[ i ], ctx->batch_sz[ i ] ) ) ) return false;
  }
  return true;
}

static size_t
test_bam_encode_scheduler_multi_batch_response_raw( uchar const * const * batches,
                                                    size_t const *         batch_sz,
                                                    size_t                 batch_cnt,
                                                    uchar *                out,
                                                    size_t                 out_sz ) {
  test_bam_raw_batch_encode_ctx_t batches_ctx = {
      .batches   = batches,
      .batch_sz  = batch_sz,
      .batch_cnt = batch_cnt
  };

  bam_api_SchedulerResponse resp = bam_api_SchedulerResponse_init_default;
  resp.which_versioned_msg = bam_api_SchedulerResponse_v0_tag;
  resp.versioned_msg.v0.which_resp = bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag;
  resp.versioned_msg.v0.resp.multiple_atomic_txn_batch.batches.funcs.encode = test_bam_encode_raw_batches_cb;
  resp.versioned_msg.v0.resp.multiple_atomic_txn_batch.batches.arg          = &batches_ctx;

  pb_ostream_t ostream = pb_ostream_from_buffer( out, out_sz );
  if( FD_UNLIKELY( !pb_encode( &ostream, bam_api_SchedulerResponse_fields, &resp ) ) ) {
    FD_LOG_ERR(( "SchedulerResponse raw encode failed (batch_cnt=%lu, out_sz=%lu): %s", batch_cnt, out_sz, PB_GET_ERROR( &ostream ) ));
  }
  return ostream.bytes_written;
}

static size_t
test_bam_encode_atomic_batch_raw( bam_types_Packet * packets,
                                  size_t             packet_cnt,
                                  uint32_t           seq_id,
                                  uchar *            out,
                                  size_t             out_sz ) {
  test_bam_packet_encode_ctx_t packets_ctx = {
      .packets    = packets,
      .packet_cnt = packet_cnt
  };

  bam_types_AtomicTxnBatch batch = bam_types_AtomicTxnBatch_init_default;
  batch.seq_id = seq_id;
  batch.max_schedule_slot = 1UL;
  batch.packets.funcs.encode = test_bam_encode_packets_cb;
  batch.packets.arg          = &packets_ctx;

  pb_ostream_t ostream = pb_ostream_from_buffer( out, out_sz );
  if( FD_UNLIKELY( !pb_encode( &ostream, bam_types_AtomicTxnBatch_fields, &batch ) ) ) {
    FD_LOG_ERR(( "AtomicTxnBatch encode failed (packet_cnt=%lu, out_sz=%lu): %s", packet_cnt, out_sz, PB_GET_ERROR( &ostream ) ));
  }
  return ostream.bytes_written;
}

static size_t
test_bam_encode_one_packet_atomic_batch_raw( uint32_t seq_id,
                                             uchar    byte,
                                             uchar *  out,
                                             size_t   out_sz ) {
  bam_types_Packet packet = bam_types_Packet_init_default;
  packet.data.size      = 1U;
  packet.data.bytes[0]  = byte;
  return test_bam_encode_atomic_batch_raw( &packet, 1UL, seq_id, out, out_sz );
}

static size_t
test_bam_encode_multiple_atomic_batch_raw( uchar const * const * batches,
                                           size_t const *         batch_sz,
                                           size_t                 batch_cnt,
                                           uchar *                out,
                                           size_t                 out_sz ) {
  test_bam_raw_batch_encode_ctx_t batches_ctx = {
      .batches   = batches,
      .batch_sz  = batch_sz,
      .batch_cnt = batch_cnt
  };

  bam_types_MultipleAtomicTxnBatch multi = bam_types_MultipleAtomicTxnBatch_init_default;
  multi.batches.funcs.encode = test_bam_encode_raw_batches_cb;
  multi.batches.arg          = &batches_ctx;

  pb_ostream_t ostream = pb_ostream_from_buffer( out, out_sz );
  if( FD_UNLIKELY( !pb_encode( &ostream, bam_types_MultipleAtomicTxnBatch_fields, &multi ) ) ) {
    FD_LOG_ERR(( "MultipleAtomicTxnBatch raw encode failed (batch_cnt=%lu, out_sz=%lu): %s", batch_cnt, out_sz, PB_GET_ERROR( &ostream ) ));
  }
  return ostream.bytes_written;
}

static size_t
test_bam_encode_one_packet_multi_payload_raw( uint32_t seq_id,
                                              uchar    byte,
                                              uchar *  out,
                                              size_t   out_sz ) {
  bam_types_Packet packet = bam_types_Packet_init_default;
  packet.data.size      = 1U;
  packet.data.bytes[0]  = byte;

  uchar batch[256];
  size_t batch_sz = test_bam_encode_atomic_batch_raw( &packet, 1UL, seq_id, batch, sizeof( batch ) );
  uchar const * batches[] = { batch };
  size_t const batch_sizes[] = { batch_sz };
  return test_bam_encode_multiple_atomic_batch_raw( batches, batch_sizes, 1UL, out, out_sz );
}

static size_t
test_bam_encode_heartbeat_payload_raw( ulong  time_sent_microseconds,
                                       uchar * out,
                                       size_t out_sz ) {
  bam_types_BuilderHeartBeat hb = bam_types_BuilderHeartBeat_init_default;
  hb.time_sent_microseconds = time_sent_microseconds;

  pb_ostream_t ostream = pb_ostream_from_buffer( out, out_sz );
  if( FD_UNLIKELY( !pb_encode( &ostream, bam_types_BuilderHeartBeat_fields, &hb ) ) ) {
    FD_LOG_ERR(( "BuilderHeartBeat raw encode failed (out_sz=%lu): %s", out_sz, PB_GET_ERROR( &ostream ) ));
  }
  return ostream.bytes_written;
}

static void
test_bam_encode_string_field_raw( pb_ostream_t * stream,
                                  uint32_t       tag,
                                  uchar const *  payload,
                                  size_t         payload_sz ) {
  FD_TEST( pb_encode_tag( stream, PB_WT_STRING, tag ) );
  FD_TEST( pb_encode_string( stream, payload, payload_sz ) );
}

static void
test_bam_encode_varint_field_raw( pb_ostream_t * stream,
                                  uint32_t       tag,
                                  uint64_t       val ) {
  FD_TEST( pb_encode_tag( stream, PB_WT_VARINT, tag ) );
  FD_TEST( pb_encode_varint( stream, val ) );
}

static size_t
test_bam_encode_scheduler_response_v0_raw( uchar const * v0_payload,
                                           size_t        v0_payload_sz,
                                           uchar *       out,
                                           size_t        out_sz ) {
  pb_ostream_t ostream = pb_ostream_from_buffer( out, out_sz );
  if( FD_UNLIKELY( !pb_encode_tag( &ostream, PB_WT_STRING, bam_api_SchedulerResponse_v0_tag ) ) ) {
    FD_LOG_ERR(( "SchedulerResponse v0 raw encode failed (tag, out_sz=%lu): %s", out_sz, PB_GET_ERROR( &ostream ) ));
  }
  if( FD_UNLIKELY( !pb_encode_string( &ostream, v0_payload, v0_payload_sz ) ) ) {
    FD_LOG_ERR(( "SchedulerResponse v0 raw encode failed (payload, out_sz=%lu): %s", out_sz, PB_GET_ERROR( &ostream ) ));
  }
  return ostream.bytes_written;
}

static size_t
test_bam_build_scheduler_batch_msg(uchar *  out,
                                   size_t   out_sz,
                                   uint32_t seq_id,
                                   uchar    packet_cnt,
                                   _Bool    revert_on_error ) {
  bam_types_Packet packets[ packet_cnt ];
  fd_memset( packets, 0, sizeof( packets ) );
  for( size_t i=0; i<packet_cnt; i++ ) {
    packets[ i ].data.size = (pb_size_t)(i + 1);
    for( pb_size_t j=0; j<packets[ i ].data.size; j++ ) {
      packets[ i ].data.bytes[ j ] = (uchar)( 'A' + (int)i + (int)j );
    }
    if( revert_on_error ) {
      packets[ i ].has_meta        = 1;
      packets[ i ].meta.has_flags  = 1;
      packets[ i ].meta.flags.revert_on_error = 1;
    }
  }
  return test_bam_encode_scheduler_response( packets, packet_cnt, seq_id, out, out_sz );
}

static void
test_bam_send_scheduler_heartbeat( fd_bam_tile_t * state,
                                   ulong          time_sent_microseconds ) {
  uchar protobuf[64];
  bam_api_SchedulerResponse resp = bam_api_SchedulerResponse_init_default;
  resp.which_versioned_msg = bam_api_SchedulerResponse_v0_tag;
  resp.versioned_msg.v0.which_resp = bam_api_SchedulerResponseV0_heart_beat_tag;
  resp.versioned_msg.v0.resp.heart_beat.time_sent_microseconds = time_sent_microseconds;

  pb_ostream_t ostream = pb_ostream_from_buffer( protobuf, sizeof(protobuf) );
  FD_TEST( pb_encode( &ostream, bam_api_SchedulerResponse_fields, &resp ) );
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
}

static void
test_bam_send_scheduler_bundle( fd_bam_tile_t * state,
                                uint32_t        seq_id,
                                _Bool           revert_on_error ) {
  uchar protobuf[256];
  size_t protobuf_sz = test_bam_build_scheduler_batch_msg( protobuf, sizeof(protobuf), seq_id, 2, revert_on_error);
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
}

/* --- Scheduler ingestion and validation ----------------------------------------------- */

/* Verify that scheduler batches without revert_on_error are fanned out
 * as individual bundle-sourced transactions with fragment metadata.
 */
static void
test_bam_packets_forwarded( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  uchar protobuf[256];
  size_t protobuf_sz = test_bam_build_scheduler_batch_msg( protobuf, sizeof(protobuf), 0U, 1, 0);

  FD_TEST( state->metrics.transaction_published_cnt == 0UL );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.transaction_published_cnt == 0UL );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 1UL );
  FD_TEST( env->stem_seqs[0] == 0UL );

  uchar fseq_mem[ FD_FSEQ_FOOTPRINT ] __attribute__((aligned(FD_FSEQ_ALIGN)));
  fd_memset( fseq_mem, 0, sizeof(fseq_mem) );
  void * fseq_shmem = fd_fseq_new( fseq_mem, 0UL );
  FD_TEST( fseq_shmem );
  ulong * fseq = fd_fseq_join( fseq_shmem );
  FD_TEST( fseq );
  state->bam_status_fseq = fseq;

  int opt_poll_in = 1;
  int charge_busy = 0;
  fd_bam_test_after_credit( state, env->stem, &opt_poll_in, &charge_busy );
  FD_TEST( env->stem_seqs[0] == 0UL );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 1UL );
  FD_TEST( state->metrics.transaction_published_cnt == 0UL );
  FD_TEST( opt_poll_in == 1 );
  FD_TEST( charge_busy == 0 );

  fd_fseq_update( fseq, FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
  fd_bam_test_after_credit( state, env->stem, &opt_poll_in, &charge_busy );
  FD_TEST( env->stem_seqs[0] == 1UL );
  FD_TEST( state->metrics.transaction_published_cnt == 1UL );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( bam_pending_txn_empty( state->pending_txns ) );
  FD_TEST( opt_poll_in == 0 );
  FD_TEST( charge_busy == 1 );

  zero_meta_ts( env->out_mcache, 1UL );
  fd_frag_meta_t expected[1] = {
    { .seq=0UL, .sig=0UL, .chunk=0UL, .sz=(ushort)(sizeof(fd_txn_m_t)+1UL), .ctl=0U },
  };
  FD_TEST( fd_memeq( env->out_mcache, expected, sizeof(expected) ) );

  fd_txn_m_t * first = (fd_txn_m_t *)fd_chunk_to_laddr( state->verify_out.mem, 0UL );
  FD_TEST( first->source_tpu    == FD_TXN_M_TPU_SOURCE_BAM );
  FD_TEST( first->bam.seq_id    == 0U );
  FD_TEST( first->bam.txn_cnt == 1UL );
  FD_TEST( first->bam.scheduler_gen == state->scheduler_gen );
  FD_TEST( first->block_engine.bundle_id == 0UL );
  FD_TEST( first->block_engine.bundle_txn_cnt == 0UL );
  FD_TEST( first->scheduler_arrival_tspub != 0U );

  FD_TEST( fd_fseq_leave( fseq ) == fseq_shmem );
  FD_TEST( fd_fseq_delete( fseq_shmem ) == fseq_shmem );
  state->bam_status_fseq = NULL;

  test_bam_env_destroy( env );
}

static void
test_bam_atomic_batch_sets_internal_bundle_metadata( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  uchar protobuf[512];
  size_t protobuf_sz = test_bam_build_scheduler_batch_msg( protobuf, sizeof(protobuf), 0U, 2, 1 );
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( state->metrics.transaction_published_cnt == 0UL );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 2UL );

  FD_TEST( test_bam_env_drain_pending_txns( env ) == 2UL );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 1UL );
  FD_TEST( state->metrics.transaction_published_cnt == 2UL );

  fd_txn_m_t * first  = (fd_txn_m_t *)fd_chunk_to_laddr( state->verify_out.mem, env->out_mcache[0].chunk );
  fd_txn_m_t * second = (fd_txn_m_t *)fd_chunk_to_laddr( state->verify_out.mem, env->out_mcache[1].chunk );

  FD_TEST( first->source_tpu == FD_TXN_M_TPU_SOURCE_BAM );
  FD_TEST( second->source_tpu == FD_TXN_M_TPU_SOURCE_BAM );
  FD_TEST( first->bam.seq_id == 0U );
  FD_TEST( second->bam.seq_id == 0U );
  FD_TEST( first->block_engine.bundle_id == 1UL );
  FD_TEST( second->block_engine.bundle_id == 1UL );
  FD_TEST( first->block_engine.bundle_txn_cnt == 2UL );
  FD_TEST( second->block_engine.bundle_txn_cnt == 0UL );

  test_bam_env_destroy( env );
}

static void
test_bam_slot_ingress_timing_tracks_max_schedule_slot_and_rejects_stale_arrival( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  bam_types_Packet packets[1];
  fd_memset( packets, 0, sizeof(packets) );
  packets[0].data.size = (pb_size_t)bam_dump_txn_fixture_sz;
  fd_memcpy( packets[0].data.bytes, bam_dump_txn_fixture, bam_dump_txn_fixture_sz );

  bam_types_AtomicTxnBatch batch = bam_types_AtomicTxnBatch_init_default;
  batch.seq_id = 77U;
  batch.max_schedule_slot = 100UL;
  batch.packets.funcs.encode = test_bam_encode_packets_cb;
  batch.packets.arg          = &(test_bam_packet_encode_ctx_t){
    .packets    = packets,
    .packet_cnt = 1UL
  };

  uchar protobuf[ 4096 ];

  state->bam_leader_state.slot        = 100UL;
  state->bam_leader_state.slot_end_ns = 1500L;
  g_clock = 1000L;
  size_t protobuf_sz = test_bam_encode_scheduler_multi_batch_response( &batch, 1UL, protobuf, sizeof(protobuf) );
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( test_bam_env_drain_pending_txns( env ) == 1UL );

  fd_bam_slot_ingress_timing_t const * entry = fd_bam_slot_ingress_timing_query_const( state, 100UL );
  FD_TEST( entry );
  FD_TEST( entry->first_rx_ts_ns == 1000L );
  FD_TEST( entry->slot_end_ns == 1500L );
  FD_TEST( entry->first_rx_ts_ns - entry->slot_end_ns == -500L );
  FD_TEST( entry->first_rx_after_slot_end == 0U );
  FD_TEST( entry->txn_before_slot_end == 1UL );
  FD_TEST( entry->txn_after_slot_end == 0UL );

  batch.seq_id = 78U;
  batch.max_schedule_slot = 100UL;
  state->bam_leader_state.slot        = 101UL;
  state->bam_leader_state.slot_end_ns = 2500L;
  g_clock = 2000L;
  protobuf_sz = test_bam_encode_scheduler_multi_batch_response( &batch, 1UL, protobuf, sizeof(protobuf) );
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  entry = fd_bam_slot_ingress_timing_query_const( state, 100UL );
  FD_TEST( entry );
  FD_TEST( entry->first_rx_ts_ns == 1000L );
  FD_TEST( entry->slot_end_ns == 1500L );
  FD_TEST( entry->first_rx_ts_ns - entry->slot_end_ns == -500L );
  FD_TEST( entry->first_rx_after_slot_end == 0U );
  FD_TEST( entry->txn_before_slot_end == 1UL );
  FD_TEST( entry->txn_after_slot_end == 1UL );
  FD_TEST( state->metrics.ingress_batch_commit_attempt_cnt == 2UL );
  FD_TEST( state->metrics.ingress_batch_published_cnt == 1UL );
  FD_TEST( state->metrics.transaction_published_cnt == 1UL );
  FD_TEST( state->feedback_queue_depth == 1UL );
  FD_TEST( state->bam_results[0].seq_id == 78U );
  FD_TEST( state->bam_results[0].slot == 100UL );
  FD_TEST( state->bam_results[0].bundle_txn_cnt == 1U );
  FD_TEST( state->bam_results[0].execution_success == 0 );
  FD_TEST( state->bam_results[0].scheduling_error == FD_BAM_SCHED_ERR_OUTSIDE_SLOT );
  FD_TEST( state->bam_results[0].bundle_err == FD_BAM_BUNDLE_ERR_NONE );

  batch.seq_id = 79U;
  batch.max_schedule_slot = 0UL;
  state->bam_leader_state.slot        = 1UL;
  state->bam_leader_state.slot_end_ns = 3500L;
  g_clock = 3000L;
  protobuf_sz = test_bam_encode_scheduler_multi_batch_response( &batch, 1UL, protobuf, sizeof(protobuf) );
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  fd_bam_slot_ingress_timing_t const * zero_entry = fd_bam_slot_ingress_timing_query_const( state, 0UL );
  FD_TEST( zero_entry );
  FD_TEST( zero_entry->first_rx_after_slot_end == 1U );
  FD_TEST( zero_entry->txn_after_slot_end == 0UL );
  FD_TEST( zero_entry->txn_unknown_slot_end == 1UL );
  FD_TEST( state->feedback_queue_depth == 2UL );
  FD_TEST( state->bam_results[1].seq_id == 79U );
  FD_TEST( state->bam_results[1].slot == 0UL );
  FD_TEST( state->bam_results[1].bundle_txn_cnt == 1U );
  FD_TEST( state->bam_results[1].execution_success == 0 );
  FD_TEST( state->bam_results[1].scheduling_error == FD_BAM_SCHED_ERR_OUTSIDE_SLOT );
  FD_TEST( state->bam_results[1].bundle_err == FD_BAM_BUNDLE_ERR_NONE );

  test_bam_env_destroy( env );
}

static void
test_bam_current_slot_work_status_bits( fd_wksp_t * wksp ) {
  fd_metrics_register( (ulong *)fd_metrics_new( metrics_scratch, 0UL ) );

  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;
  test_bam_env_mock_conn_empty( env );
  test_bam_env_mock_h2_hs( state );
  test_bam_env_inject_config_response( state );

  long now = fd_bam_now();
  state->bam_last_builder_activity_ns    = now;
  state->bam_stream_live                 = 1U;
  state->bam_status_recent               = FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY;

  fd_keyswitch_t keyswitch = {0};
  keyswitch.state = FD_KEYSWITCH_STATE_COMPLETED;
  state->keyswitch = &keyswitch;

  uchar fseq_mem[ FD_FSEQ_FOOTPRINT ] __attribute__((aligned(FD_FSEQ_ALIGN)));
  fd_memset( fseq_mem, 0, sizeof(fseq_mem) );
  void * fseq_shmem = fd_fseq_new( fseq_mem, 0UL );
  FD_TEST( fseq_shmem );
  ulong * fseq = fd_fseq_join( fseq_shmem );
  FD_TEST( fseq );
  state->bam_status_fseq = fseq;

  state->bam_leader_state = (fd_bam_leader_state_t){
    .slot               = 42UL,
    .slot_end_ns        = fd_log_wallclock() - 1L,
    .current_slot_has_bam_work = 0U,
  };
  fd_bam_tile_housekeeping( state );
  FD_TEST( fd_fseq_query( fseq ) == FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
  fd_bam_test_metrics_write( state );
  FD_TEST( FD_MGAUGE_GET( BAM, HEALTHY ) == 1UL );
  FD_TEST( FD_MGAUGE_GET( BAM, STREAM_LIVE ) == 1UL );
  FD_TEST( FD_MGAUGE_GET( BAM, LEADER_STATE_SLOT ) == 42UL );
  FD_TEST( FD_MGAUGE_GET( BAM, LEADER_STATE_TICK ) == 0UL );
  FD_TEST( FD_MGAUGE_GET( BAM, LEADER_STATE_SLOT_END_NANOS ) == (ulong)state->bam_leader_state.slot_end_ns );

  state->bam_leader_state = (fd_bam_leader_state_t){
    .slot               = 43UL,
    .tick               = 7U,
    .slot_end_ns        = fd_log_wallclock() + (long)1e9,
    .current_slot_has_bam_work = 1U,
  };
  fd_bam_tile_housekeeping( state );
  FD_TEST( fd_fseq_query( fseq ) == FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
  fd_bam_test_metrics_write( state );
  FD_TEST( FD_MGAUGE_GET( BAM, LEADER_STATE_SLOT ) == 43UL );
  FD_TEST( FD_MGAUGE_GET( BAM, LEADER_STATE_TICK ) == 7UL );
  FD_TEST( FD_MGAUGE_GET( BAM, LEADER_STATE_SLOT_END_NANOS ) == (ulong)state->bam_leader_state.slot_end_ns );

  FD_TEST( fd_fseq_leave( fseq ) == fseq_shmem );
  FD_TEST( fd_fseq_delete( fseq_shmem ) == fseq_shmem );
  state->bam_status_fseq = NULL;
  state->keyswitch = NULL;
  test_bam_env_destroy( env );
}

static void
test_bam_slot_ingress_timing_tracks_hash_collisions( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  fd_bam_batch_ctx_t batch_state;
  fd_memset( &batch_state, 0, sizeof(batch_state) );
  batch_state.packet_cnt = 1U;
  batch_state.packets[ 0 ].data.size = (pb_size_t)bam_dump_txn_fixture_sz;
  fd_memcpy( batch_state.packets[ 0 ].data.bytes, bam_dump_txn_fixture, bam_dump_txn_fixture_sz );

  state->bam_leader_state.slot = 228UL;

  bam_types_AtomicTxnBatch batch_a = bam_types_AtomicTxnBatch_init_default;
  batch_a.seq_id = 123U;
  batch_a.max_schedule_slot = 100UL;
  batch_state.ingress_rx_ts_ns      = 3000L;
  batch_state.ingress_rx_tspub      = 1U;
  g_clock = 3000L;
  fd_bam_publish_batch( state, &batch_state, &batch_a );

  bam_types_AtomicTxnBatch batch_b = bam_types_AtomicTxnBatch_init_default;
  batch_b.seq_id = 124U;
  batch_b.max_schedule_slot = 100UL + FD_BAM_SLOT_INGRESS_TIMING_CNT;
  batch_state.ingress_rx_ts_ns      = 4444L;
  batch_state.ingress_rx_tspub      = 2U;
  g_clock = 4444L;
  fd_bam_publish_batch( state, &batch_state, &batch_b );

  fd_bam_slot_ingress_timing_t const * entry_a = fd_bam_slot_ingress_timing_query_const( state, 100UL );
  fd_bam_slot_ingress_timing_t const * entry_b = fd_bam_slot_ingress_timing_query_const( state, 100UL + FD_BAM_SLOT_INGRESS_TIMING_CNT );
  FD_TEST( entry_a );
  FD_TEST( entry_b );
  FD_TEST( entry_a != entry_b );
  FD_TEST( entry_a->first_rx_ts_ns == 3000L );
  FD_TEST( entry_b->first_rx_ts_ns == 4444L );
  FD_TEST( entry_a->txn_unknown_slot_end == 1UL );
  FD_TEST( entry_b->txn_unknown_slot_end == 1UL );

  test_bam_env_destroy( env );
}

static void
test_bam_publish_batch_uses_captured_ingress_metadata( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  fd_bam_batch_ctx_t batch_state;
  fd_memset( &batch_state, 0, sizeof(batch_state) );
  batch_state.packet_cnt = 1U;
  batch_state.ingress_rx_ts_ns      = 1000L;
  batch_state.ingress_slot_end_ns   = 1500L;
  batch_state.ingress_rx_tspub      = 123456789U;
  batch_state.packets[ 0 ].data.size = (pb_size_t)bam_dump_txn_fixture_sz;
  fd_memcpy( batch_state.packets[ 0 ].data.bytes, bam_dump_txn_fixture, bam_dump_txn_fixture_sz );

  bam_types_AtomicTxnBatch batch = bam_types_AtomicTxnBatch_init_default;
  batch.seq_id = 77U;
  batch.max_schedule_slot = 100UL;

  state->bam_leader_state.slot        = 101UL;
  state->bam_leader_state.slot_end_ns = 2500L;
  g_clock = 2000L;
  fd_bam_publish_batch( state, &batch_state, &batch );
  FD_TEST( test_bam_env_drain_pending_txns( env ) == 1UL );

  fd_bam_slot_ingress_timing_t const * entry = fd_bam_slot_ingress_timing_query_const( state, 100UL );
  FD_TEST( entry );
  FD_TEST( entry->first_rx_ts_ns == 1000L );
  FD_TEST( entry->slot_end_ns == 1500L );
  FD_TEST( entry->txn_before_slot_end == 1UL );
  FD_TEST( entry->txn_after_slot_end == 0UL );
  FD_TEST( entry->txn_unknown_slot_end == 0UL );

  fd_txn_m_t const * first = (fd_txn_m_t const *)fd_chunk_to_laddr( state->verify_out.mem, 0UL );
  FD_TEST( first->scheduler_arrival_tspub == 123456789U );

  test_bam_env_destroy( env );
}

/* Validates that a scheduler response carrying multiple AtomicTxnBatch entries
   fans out into sequential fragments with the correct metadata for each batch. */
static void
test_bam_multiple_batches_forwarded( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  zero_meta_ts( env->out_mcache, 3UL );

  bam_types_Packet first_packets[1];
  fd_memset( first_packets, 0, sizeof(first_packets) );
  first_packets[0].data.size     = 1U;
  first_packets[0].data.bytes[0] = (uchar)'m';

  test_bam_packet_encode_ctx_t first_ctx = {
      .packets    = first_packets,
      .packet_cnt = 1UL
  };

  bam_types_Packet second_packets[2];
  fd_memset( second_packets, 0, sizeof(second_packets) );
  for( size_t i=0UL; i<2UL; i++ ) {
    second_packets[ i ].data.size     = 1U;
    second_packets[ i ].data.bytes[0] = (uchar)('n' + (int)i);
    second_packets[ i ].has_meta = 1U;
    second_packets[ i ].meta.has_flags = 1U;
    second_packets[ i ].meta.flags.revert_on_error = 1U;
  }

  test_bam_packet_encode_ctx_t second_ctx = {
      .packets    = second_packets,
      .packet_cnt = 2UL
  };

  bam_types_AtomicTxnBatch batches[2];
  batches[0] = (bam_types_AtomicTxnBatch)bam_types_AtomicTxnBatch_init_default;
  batches[0].seq_id = 6U;
  batches[0].max_schedule_slot = 100UL;
  batches[0].packets.funcs.encode = test_bam_encode_packets_cb;
  batches[0].packets.arg          = &first_ctx;

  batches[1] = (bam_types_AtomicTxnBatch)bam_types_AtomicTxnBatch_init_default;
  batches[1].seq_id = 7U;
  batches[1].max_schedule_slot = 100UL;
  batches[1].packets.funcs.encode = test_bam_encode_packets_cb;
  batches[1].packets.arg          = &second_ctx;

  uchar protobuf[512];
  size_t protobuf_sz = test_bam_encode_scheduler_multi_batch_response( batches, 2UL, protobuf, sizeof(protobuf) );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 3UL );
  FD_TEST( test_bam_env_drain_pending_txns( env ) == 3UL );

  FD_TEST( state->metrics.transaction_published_cnt == 3UL );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 1UL );
  FD_TEST( state->feedback_queue_depth == 0UL );

  fd_frag_meta_t * meta = env->out_mcache;
  FD_TEST( meta[0].seq == 0UL );
  FD_TEST( meta[1].seq == 1UL );
  FD_TEST( meta[2].seq == 2UL );

  fd_txn_m_t * tx0 = fd_chunk_to_laddr( state->verify_out.mem, meta[0].chunk );
  fd_txn_m_t * tx1 = fd_chunk_to_laddr( state->verify_out.mem, meta[1].chunk );
  fd_txn_m_t * tx2 = fd_chunk_to_laddr( state->verify_out.mem, meta[2].chunk );

  FD_TEST( tx0->bam.seq_id == 6U );
  FD_TEST( tx0->bam.revert_on_error == 0U );
  FD_TEST( tx0->bam.txn_cnt == 1U );
  FD_TEST( tx0->block_engine.bundle_id == 0UL );
  FD_TEST( tx0->block_engine.bundle_txn_cnt == 0UL );

  FD_TEST( tx1->bam.seq_id == 7U );
  FD_TEST( tx1->bam.revert_on_error == 1U );
  FD_TEST( tx1->bam.txn_cnt == 2U );
  FD_TEST( tx1->bam.batch_idx == 0U );
  FD_TEST( tx1->source_tpu == FD_TXN_M_TPU_SOURCE_BAM );
  FD_TEST( tx1->block_engine.bundle_id == 8UL );
  FD_TEST( tx1->block_engine.bundle_txn_cnt == 2UL );

  FD_TEST( tx2->bam.seq_id == 7U );
  FD_TEST( tx2->bam.revert_on_error == 1U );
  FD_TEST( tx2->bam.txn_cnt == 2U );
  FD_TEST( tx2->bam.batch_idx == 1U );
  FD_TEST( tx2->source_tpu == FD_TXN_M_TPU_SOURCE_BAM );
  FD_TEST( tx2->block_engine.bundle_id == 8UL );
  FD_TEST( tx2->block_engine.bundle_txn_cnt == 0UL );

  uchar const * payload0 = fd_txn_m_payload( tx0 );
  uchar const * payload1 = fd_txn_m_payload( tx1 );
  uchar const * payload2 = fd_txn_m_payload( tx2 );
  FD_TEST( payload0[0] == 'm' );
  FD_TEST( payload1[0] == 'n' );
  FD_TEST( payload2[0] == 'o' );

  test_bam_env_destroy( env );
}

static void
test_bam_independent_batches_queue_and_drain_in_order( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  bam_types_Packet packets[3];
  test_bam_packet_encode_ctx_t packet_ctx[3];
  bam_types_AtomicTxnBatch batches[3];
  for( ulong i=0UL; i<3UL; i++ ) {
    packets[ i ] = (bam_types_Packet)bam_types_Packet_init_default;
    packets[ i ].data.size     = 1U;
    packets[ i ].data.bytes[0] = (uchar)( 'x' + (int)i );
    packet_ctx[ i ] = (test_bam_packet_encode_ctx_t){
      .packets    = &packets[ i ],
      .packet_cnt = 1UL
    };
    batches[ i ] = (bam_types_AtomicTxnBatch)bam_types_AtomicTxnBatch_init_default;
    batches[ i ].seq_id            = (uint32_t)( 50U + i );
    batches[ i ].max_schedule_slot = 500UL + i;
    batches[ i ].packets.funcs.encode = test_bam_encode_packets_cb;
    batches[ i ].packets.arg          = &packet_ctx[ i ];
  }

  uchar protobuf[512];
  size_t protobuf_sz = test_bam_encode_scheduler_multi_batch_response( batches, 3UL, protobuf, sizeof(protobuf) );
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( env->stem_seqs[0] == 0UL );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 3UL );
  FD_TEST( state->metrics.transaction_published_cnt == 0UL );
  FD_TEST( state->metrics.ingress_batch_published_cnt == 0UL );

  FD_TEST( test_bam_env_drain_pending_txns( env ) == 3UL );
  FD_TEST( bam_pending_txn_empty( state->pending_txns ) );
  FD_TEST( state->metrics.transaction_published_cnt == 3UL );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( state->metrics.ingress_batch_published_cnt == 3UL );

  for( ulong i=0UL; i<3UL; i++ ) {
    fd_frag_meta_t const * meta = &env->out_mcache[ i ];
    fd_txn_m_t const * txnm = fd_chunk_to_laddr_const( state->verify_out.mem, meta->chunk );
    FD_TEST( meta->seq == i );
    FD_TEST( meta->sig == 0UL );
    FD_TEST( txnm->bam.seq_id == 50U + i );
    FD_TEST( txnm->bam.batch_idx == 0U );
    FD_TEST( txnm->bam.txn_cnt == 1U );
    FD_TEST( txnm->bam.revert_on_error == 0U );
    FD_TEST( fd_txn_m_payload_const( txnm )[0] == (uchar)( 'x' + (int)i ) );
  }

  test_bam_env_destroy( env );
}

static void
test_bam_pending_over_stem_burst_drains_in_multiple_passes( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  bam_types_Packet packets[ FD_BAM_FULL_ATOMIC_BATCHES_PER_STEM_BURST ][ FD_BAM_MAX_TXN_PER_ATOMIC_BATCH ];
  test_bam_packet_encode_ctx_t packet_ctx[ FD_BAM_FULL_ATOMIC_BATCHES_PER_STEM_BURST ];
  bam_types_AtomicTxnBatch batches[ FD_BAM_FULL_ATOMIC_BATCHES_PER_STEM_BURST ];
  test_bam_fill_full_txn_batches( packets, packet_ctx, batches, FD_BAM_FULL_ATOMIC_BATCHES_PER_STEM_BURST, 900U, 900UL );

  uchar protobuf[ TEST_BAM_PROTOBUF_BUF_SZ ];
  size_t protobuf_sz = test_bam_encode_scheduler_multi_batch_response( batches,
                                                                       FD_BAM_FULL_ATOMIC_BATCHES_PER_STEM_BURST,
                                                                       protobuf,
                                                                       sizeof(protobuf) );
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  uchar single_protobuf[256];
  size_t single_protobuf_sz = test_bam_build_scheduler_batch_msg( single_protobuf, sizeof(single_protobuf), 1000U, 1, 0 );
  fd_bam_client_grpc_rx_msg( state,
                             single_protobuf,
                             single_protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == FD_BAM_STEM_BURST + 1UL );
  FD_TEST( test_bam_env_drain_pending_txns( env ) == FD_BAM_STEM_BURST );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 1UL );
  FD_TEST( state->metrics.transaction_published_cnt == FD_BAM_STEM_BURST );
  FD_TEST( state->metrics.atomic_batch_published_cnt == FD_BAM_FULL_ATOMIC_BATCHES_PER_STEM_BURST );
  FD_TEST( state->metrics.ingress_batch_published_cnt == FD_BAM_FULL_ATOMIC_BATCHES_PER_STEM_BURST );

  FD_TEST( test_bam_env_drain_pending_txns( env ) == 1UL );
  FD_TEST( bam_pending_txn_empty( state->pending_txns ) );
  FD_TEST( state->metrics.transaction_published_cnt == FD_BAM_STEM_BURST + 1UL );
  FD_TEST( state->metrics.ingress_batch_published_cnt == FD_BAM_FULL_ATOMIC_BATCHES_PER_STEM_BURST + 1UL );

  fd_frag_meta_t const * last_meta = &env->out_mcache[ FD_BAM_STEM_BURST ];
  fd_txn_m_t const * last = fd_chunk_to_laddr_const( state->verify_out.mem, last_meta->chunk );
  FD_TEST( last_meta->seq == FD_BAM_STEM_BURST );
  FD_TEST( last->bam.seq_id == 1000U );
  FD_TEST( last->bam.revert_on_error == 0U );

  test_bam_env_destroy( env );
}

static void
test_bam_queue_full_rejects_whole_batch_without_partial_enqueue( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  ulong cap = bam_pending_txn_max( state->pending_txns );
  for( ulong i=0UL; i<cap-1UL; i++ ) {
    fd_bam_pending_txn_t * pending = bam_pending_txn_push_tail_nocopy( state->pending_txns );
    fd_memset( pending, 0, sizeof(*pending) );
    pending->payload_sz = 1U;
    pending->payload[0] = (uchar)i;
    pending->seq_id     = (uint)i;
    pending->batch_cnt  = 1U;
  }

  uchar protobuf[512];
  size_t protobuf_sz = test_bam_build_scheduler_batch_msg( protobuf, sizeof(protobuf), 4242U, 2, 1 );
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == cap-1UL );
  FD_TEST( env->stem_seqs[0] == 0UL );
  FD_TEST( state->metrics.transaction_published_cnt == 0UL );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( state->metrics.ingress_batch_published_cnt == 0UL );
  FD_TEST( state->metrics.transaction_rejected_backpressure_cnt == 2UL );
  FD_TEST( state->feedback_queue_depth == 1UL );
  FD_TEST( state->bam_results[0].seq_id == 4242U );
  FD_TEST( state->bam_results[0].bundle_txn_cnt == 2U );
  FD_TEST( state->bam_results[0].scheduling_error == FD_BAM_SCHED_ERR_CONTAINER_FULL );
  FD_TEST( state->bam_results[0].bundle_err == FD_BAM_BUNDLE_ERR_NONE );

  test_bam_env_destroy( env );
}

static void
test_bam_multiple_batches_accept_full_txn_count( fd_wksp_t * wksp,
                                                 ulong       batch_cnt,
                                                 uint        seq_base,
                                                 ulong       slot_base ) {
  FD_TEST( batch_cnt <= FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE );
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  ulong const expected_txn_cnt = batch_cnt * FD_BAM_MAX_TXN_PER_ATOMIC_BATCH;
  zero_meta_ts( env->out_mcache, expected_txn_cnt );

  static bam_types_Packet packets[ FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE ][ FD_BAM_MAX_TXN_PER_ATOMIC_BATCH ];
  static test_bam_packet_encode_ctx_t packet_ctx[ FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE ];
  static bam_types_AtomicTxnBatch batches[ FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE ];
  test_bam_fill_full_txn_batches( packets, packet_ctx, batches, batch_cnt, seq_base, slot_base );

  uchar protobuf[ TEST_BAM_PROTOBUF_BUF_SZ ];
  size_t protobuf_sz = test_bam_encode_scheduler_multi_batch_response( batches,
                                                                       batch_cnt,
                                                                       protobuf,
                                                                       sizeof( protobuf ) );
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == expected_txn_cnt );
  FD_TEST( test_bam_env_drain_all_pending_txns( env ) == expected_txn_cnt );
  FD_TEST( bam_pending_txn_empty( state->pending_txns ) );

  FD_TEST( state->metrics.transaction_published_cnt == expected_txn_cnt );
  FD_TEST( state->metrics.ingress_batch_published_cnt == batch_cnt );
  FD_TEST( state->metrics.atomic_batch_published_cnt == batch_cnt );
  FD_TEST( state->feedback_queue_depth == 0UL );
  FD_TEST( state->metrics.transaction_rejected_backpressure_cnt == 0UL );
  FD_TEST( state->metrics.ingress_message_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_MESSAGE_REJECT_REASON_V_OVERFLOW_MESSAGE_IDX ] == 0UL );

  fd_frag_meta_t * meta = env->out_mcache;
  for( ulong i=0UL; i<expected_txn_cnt; i++ ) {
    ulong batch_idx = i / FD_BAM_MAX_TXN_PER_ATOMIC_BATCH;
    ulong txn_idx   = i % FD_BAM_MAX_TXN_PER_ATOMIC_BATCH;
    FD_TEST( meta[ i ].seq == i );
    fd_txn_m_t * txnm = fd_chunk_to_laddr( state->verify_out.mem, meta[ i ].chunk );
    FD_TEST( txnm->bam.seq_id == seq_base + (uint)batch_idx );
    FD_TEST( txnm->bam.txn_cnt == FD_BAM_MAX_TXN_PER_ATOMIC_BATCH );
    FD_TEST( txnm->bam.batch_idx == (uchar)txn_idx );
    FD_TEST( txnm->bam.revert_on_error == 1U );
    FD_TEST( txnm->block_engine.bundle_id == (ulong)( seq_base + (uint)batch_idx ) + 1UL );
    FD_TEST( txnm->block_engine.bundle_txn_cnt == fd_ulong_if( txn_idx==0UL, FD_BAM_MAX_TXN_PER_ATOMIC_BATCH, 0UL ) );
  }

  test_bam_env_destroy( env );
}

/* Ensures truncated scheduler responses trigger scheduler-envelope failure
   accounting and drop the message without emitting any downstream fragments. */
static void
test_bam_scheduler_truncated_message_dropped( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  fd_memset( env->out_mcache, 0, 3UL * sizeof(fd_frag_meta_t) );

  uchar protobuf[256];
  size_t protobuf_sz = test_bam_build_scheduler_batch_msg( protobuf, sizeof(protobuf), 5U, 2, 0);
  FD_TEST( protobuf_sz > 1UL );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz - 1UL,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_SCHEDULER_ENVELOPE_DECODE_IDX ] == 1UL );
  FD_TEST( state->metrics.ingress_batch_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_INVALID_BATCH_IDX ] == 0UL );
  FD_TEST( state->metrics.transaction_published_cnt == 0UL );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( state->metrics.ingress_packet_oversize_cnt == 0UL );
  FD_TEST( env->stem_seqs[0] == 0UL );
  FD_TEST( env->out_mcache[0].seq == 0UL );
  FD_TEST( env->out_mcache[0].sz == 0 );

  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_trailing_corruption_does_not_publish( fd_wksp_t * wksp ) {
  /* A valid leading payload followed by an invalid outer tag must not publish
     transactions before the decode failure is observed. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;
  zero_meta_ts( env->out_mcache, 2UL );

  bam_types_Packet packet = bam_types_Packet_init_default;
  packet.data.size      = 1U;
  packet.data.bytes[ 0 ] = (uchar)'x';

  uchar protobuf[256];
  size_t protobuf_sz = test_bam_encode_scheduler_response( &packet,
                                                           1UL,
                                                           77U,
                                                           protobuf,
                                                           sizeof( protobuf ) );
  FD_TEST( protobuf_sz + 1UL < sizeof( protobuf ) );
  protobuf[ protobuf_sz ] = 0x00U; /* Invalid outer tag value */

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz + 1UL,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_SCHEDULER_ENVELOPE_DECODE_IDX ] == 1UL );
  FD_TEST( state->metrics.ingress_batch_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_INVALID_BATCH_IDX ] == 0UL );
  FD_TEST( state->metrics.transaction_published_cnt == 0UL );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( state->feedback_queue_depth == 0UL );

  test_bam_env_destroy( env );
}

static void
test_bam_expect_scheduler_decode_failure( test_bam_env_t * env,
                                          uchar const *    protobuf,
                                          size_t           protobuf_sz,
                                          ulong            expected_failure_cnt ) {
  fd_bam_tile_t * state = env->state;
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_SCHEDULER_ENVELOPE_DECODE_IDX ] == expected_failure_cnt );
  FD_TEST( state->metrics.ingress_multi_message_received_cnt == 0UL );
  FD_TEST( state->metrics.ingress_batch_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_INVALID_BATCH_IDX ] == 0UL );
  FD_TEST( state->metrics.transaction_published_cnt == 0UL );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( state->feedback_queue_depth == 0UL );
  FD_TEST( env->stem_seqs[0] == 0UL );
}

static void
test_bam_scheduler_rejects_invalid_custom_decode_fields( fd_wksp_t * wksp ) {
  uchar valid_multi_payload[384];
  size_t valid_multi_payload_sz = test_bam_encode_one_packet_multi_payload_raw( 78U,
                                                                                (uchar)'v',
                                                                                valid_multi_payload,
                                                                                sizeof( valid_multi_payload ) );

  uchar valid_v0_payload[512];
  pb_ostream_t valid_v0_stream = pb_ostream_from_buffer( valid_v0_payload, sizeof( valid_v0_payload ) );
  test_bam_encode_string_field_raw( &valid_v0_stream,
                                    bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag,
                                    valid_multi_payload,
                                    valid_multi_payload_sz );

  bam_types_Packet packet = bam_types_Packet_init_default;
  packet.data.size      = 1U;
  packet.data.bytes[0]  = (uchar)'n';

  uchar batch[256];
  size_t batch_sz = test_bam_encode_atomic_batch_raw( &packet, 1UL, 82U, batch, sizeof( batch ) );

  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  zero_meta_ts( env->out_mcache, 2UL );

  uchar protobuf[768];
  ulong expected_failure_cnt = 0UL;

  pb_ostream_t outer_stream = pb_ostream_from_buffer( protobuf, sizeof( protobuf ) );
  test_bam_encode_varint_field_raw( &outer_stream, bam_api_SchedulerResponse_v0_tag, 1UL );
  test_bam_encode_string_field_raw( &outer_stream,
                                    bam_api_SchedulerResponse_v0_tag,
                                    valid_v0_payload,
                                    valid_v0_stream.bytes_written );
  test_bam_expect_scheduler_decode_failure( env, protobuf, outer_stream.bytes_written, ++expected_failure_cnt );

  outer_stream = pb_ostream_from_buffer( protobuf, sizeof( protobuf ) );
  test_bam_encode_varint_field_raw( &outer_stream, 31U, 1UL );
  test_bam_encode_string_field_raw( &outer_stream,
                                    bam_api_SchedulerResponse_v0_tag,
                                    valid_v0_payload,
                                    valid_v0_stream.bytes_written );
  test_bam_expect_scheduler_decode_failure( env, protobuf, outer_stream.bytes_written, ++expected_failure_cnt );

  uchar v0_payload[640];
  pb_ostream_t v0_stream = pb_ostream_from_buffer( v0_payload, sizeof( v0_payload ) );
  test_bam_encode_varint_field_raw( &v0_stream, 31U, 1UL );
  test_bam_encode_string_field_raw( &v0_stream,
                                    bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag,
                                    valid_multi_payload,
                                    valid_multi_payload_sz );
  size_t protobuf_sz = test_bam_encode_scheduler_response_v0_raw( v0_payload,
                                                                  v0_stream.bytes_written,
                                                                  protobuf,
                                                                  sizeof( protobuf ) );
  test_bam_expect_scheduler_decode_failure( env, protobuf, protobuf_sz, ++expected_failure_cnt );

  v0_stream = pb_ostream_from_buffer( v0_payload, sizeof( v0_payload ) );
  test_bam_encode_varint_field_raw( &v0_stream, bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag, 1UL );
  test_bam_encode_string_field_raw( &v0_stream,
                                    bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag,
                                    valid_multi_payload,
                                    valid_multi_payload_sz );
  protobuf_sz = test_bam_encode_scheduler_response_v0_raw( v0_payload,
                                                           v0_stream.bytes_written,
                                                           protobuf,
                                                           sizeof( protobuf ) );
  test_bam_expect_scheduler_decode_failure( env, protobuf, protobuf_sz, ++expected_failure_cnt );

  uchar multi_payload[384];
  pb_ostream_t multi_stream = pb_ostream_from_buffer( multi_payload, sizeof( multi_payload ) );
  test_bam_encode_varint_field_raw( &multi_stream, 31U, 1UL );
  test_bam_encode_string_field_raw( &multi_stream,
                                    bam_types_MultipleAtomicTxnBatch_batches_tag,
                                    batch,
                                    batch_sz );
  v0_stream = pb_ostream_from_buffer( v0_payload, sizeof( v0_payload ) );
  test_bam_encode_string_field_raw( &v0_stream,
                                    bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag,
                                    multi_payload,
                                    multi_stream.bytes_written );
  protobuf_sz = test_bam_encode_scheduler_response_v0_raw( v0_payload,
                                                           v0_stream.bytes_written,
                                                           protobuf,
                                                           sizeof( protobuf ) );
  test_bam_expect_scheduler_decode_failure( env, protobuf, protobuf_sz, ++expected_failure_cnt );

  multi_stream = pb_ostream_from_buffer( multi_payload, sizeof( multi_payload ) );
  test_bam_encode_varint_field_raw( &multi_stream, bam_types_MultipleAtomicTxnBatch_batches_tag, 1UL );
  test_bam_encode_string_field_raw( &multi_stream,
                                    bam_types_MultipleAtomicTxnBatch_batches_tag,
                                    batch,
                                    batch_sz );
  v0_stream = pb_ostream_from_buffer( v0_payload, sizeof( v0_payload ) );
  test_bam_encode_string_field_raw( &v0_stream,
                                    bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag,
                                    multi_payload,
                                    multi_stream.bytes_written );
  protobuf_sz = test_bam_encode_scheduler_response_v0_raw( v0_payload,
                                                           v0_stream.bytes_written,
                                                           protobuf,
                                                           sizeof( protobuf ) );
  test_bam_expect_scheduler_decode_failure( env, protobuf, protobuf_sz, ++expected_failure_cnt );

  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_v0_oneof_uses_last_field( fd_wksp_t * wksp ) {
  /* Duplicate oneof fields should apply only the final recognized field. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;
  zero_meta_ts( env->out_mcache, 2UL );
  g_clock = (long)20e6;

  uchar multi_payload[384];
  size_t multi_payload_sz = test_bam_encode_one_packet_multi_payload_raw( 93U,
                                                                          (uchar)'m',
                                                                          multi_payload,
                                                                          sizeof( multi_payload ) );

  uchar hb_payload[64];
  size_t hb_payload_sz = test_bam_encode_heartbeat_payload_raw( 12345UL,
                                                                hb_payload,
                                                                sizeof( hb_payload ) );

  uchar v0_payload[640];
  pb_ostream_t v0_stream = pb_ostream_from_buffer( v0_payload, sizeof( v0_payload ) );
  test_bam_encode_string_field_raw( &v0_stream,
                                    bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag,
                                    multi_payload,
                                    multi_payload_sz );
  test_bam_encode_string_field_raw( &v0_stream,
                                    bam_api_SchedulerResponseV0_heart_beat_tag,
                                    hb_payload,
                                    hb_payload_sz );

  uchar protobuf[768];
  size_t protobuf_sz = test_bam_encode_scheduler_response_v0_raw( v0_payload,
                                                                  v0_stream.bytes_written,
                                                                  protobuf,
                                                                  sizeof( protobuf ) );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_SCHEDULER_ENVELOPE_DECODE_IDX ] == 0UL );
  FD_TEST( state->metrics.ingress_multi_message_received_cnt == 0UL );
  FD_TEST( state->metrics.builder_heartbeats_decoded_cnt == 1UL );
  FD_TEST( test_hist_total_cnt( state->metrics.builder_heartbeat_arrival_delta_nanos ) == 1UL );
  ulong expected_latency = (ulong)g_clock - (12345UL * 1000UL);
  FD_TEST( fd_histf_sum( state->metrics.builder_heartbeat_arrival_delta_nanos ) == expected_latency );
  FD_TEST( state->metrics.transaction_published_cnt == 0UL );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( state->feedback_queue_depth == 0UL );
  FD_TEST( env->stem_seqs[0] == 0UL );

  test_bam_env_destroy( env );

  test_bam_env_create( env, wksp );
  state = env->state;
  zero_meta_ts( env->out_mcache, 2UL );
  g_clock = (long)30e6;

  multi_payload_sz = test_bam_encode_one_packet_multi_payload_raw( 94U,
                                                                   (uchar)'n',
                                                                   multi_payload,
                                                                   sizeof( multi_payload ) );

  uchar hb_first[64];
  uchar hb_second[64];
  size_t hb_first_sz  = test_bam_encode_heartbeat_payload_raw( 11111UL, hb_first,  sizeof( hb_first  ) );
  size_t hb_second_sz = test_bam_encode_heartbeat_payload_raw( 22222UL, hb_second, sizeof( hb_second ) );

  v0_stream = pb_ostream_from_buffer( v0_payload, sizeof( v0_payload ) );
  test_bam_encode_string_field_raw( &v0_stream,
                                    bam_api_SchedulerResponseV0_heart_beat_tag,
                                    hb_first,
                                    hb_first_sz );
  test_bam_encode_string_field_raw( &v0_stream,
                                    bam_api_SchedulerResponseV0_heart_beat_tag,
                                    hb_second,
                                    hb_second_sz );
  test_bam_encode_string_field_raw( &v0_stream,
                                    bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag,
                                    multi_payload,
                                    multi_payload_sz );

  protobuf_sz = test_bam_encode_scheduler_response_v0_raw( v0_payload,
                                                           v0_stream.bytes_written,
                                                           protobuf,
                                                           sizeof( protobuf ) );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( test_bam_env_drain_pending_txns( env ) == 1UL );

  FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_SCHEDULER_ENVELOPE_DECODE_IDX ] == 0UL );
  FD_TEST( state->metrics.ingress_multi_message_received_cnt == 1UL );
  FD_TEST( state->metrics.builder_heartbeats_decoded_cnt == 0UL );
  FD_TEST( test_hist_total_cnt( state->metrics.builder_heartbeat_arrival_delta_nanos ) == 0UL );
  FD_TEST( state->metrics.transaction_published_cnt == 1UL );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( state->feedback_queue_depth == 0UL );
  FD_TEST( state->bam_last_builder_activity_ns == g_clock );

  test_bam_env_destroy( env );

  test_bam_env_create( env, wksp );
  state = env->state;
  g_clock = (long)31e6;

  uchar empty_multi_payload[16];
  size_t empty_multi_payload_sz = test_bam_encode_multiple_atomic_batch_raw( NULL,
                                                                             NULL,
                                                                             0UL,
                                                                             empty_multi_payload,
                                                                             sizeof( empty_multi_payload ) );
  hb_payload_sz = test_bam_encode_heartbeat_payload_raw( 1234UL,
                                                         hb_payload,
                                                         sizeof( hb_payload ) );

  v0_stream = pb_ostream_from_buffer( v0_payload, sizeof( v0_payload ) );
  test_bam_encode_string_field_raw( &v0_stream,
                                    bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag,
                                    empty_multi_payload,
                                    empty_multi_payload_sz );
  test_bam_encode_string_field_raw( &v0_stream,
                                    bam_api_SchedulerResponseV0_heart_beat_tag,
                                    hb_payload,
                                    hb_payload_sz );

  protobuf_sz = test_bam_encode_scheduler_response_v0_raw( v0_payload,
                                                           v0_stream.bytes_written,
                                                           protobuf,
                                                           sizeof( protobuf ) );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_SCHEDULER_ENVELOPE_DECODE_IDX ] == 0UL );
  FD_TEST( state->metrics.ingress_multi_message_received_cnt == 0UL );
  FD_TEST( state->metrics.ingress_message_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_MESSAGE_REJECT_REASON_V_EMPTY_MESSAGE_IDX ] == 0UL );
  FD_TEST( state->metrics.ingress_message_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_MESSAGE_REJECT_REASON_V_OVERFLOW_MESSAGE_IDX ] == 0UL );
  FD_TEST( state->metrics.ingress_batch_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_INVALID_BATCH_IDX ] == 0UL );
  FD_TEST( state->metrics.builder_heartbeats_decoded_cnt == 1UL );
  FD_TEST( test_hist_total_cnt( state->metrics.builder_heartbeat_arrival_delta_nanos ) == 1UL );
  FD_TEST( state->metrics.transaction_published_cnt == 0UL );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( state->feedback_queue_depth == 0UL );
  FD_TEST( state->bam_last_builder_activity_ns == g_clock );

  test_bam_env_destroy( env );

  test_bam_env_create( env, wksp );
  state = env->state;
  g_clock = (long)32e6;

  empty_multi_payload_sz = test_bam_encode_multiple_atomic_batch_raw( NULL,
                                                                      NULL,
                                                                      0UL,
                                                                      empty_multi_payload,
                                                                      sizeof( empty_multi_payload ) );
  uchar first_v0_payload[128];
  pb_ostream_t first_v0_stream = pb_ostream_from_buffer( first_v0_payload, sizeof( first_v0_payload ) );
  test_bam_encode_string_field_raw( &first_v0_stream,
                                    bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag,
                                    empty_multi_payload,
                                    empty_multi_payload_sz );

  hb_payload_sz = test_bam_encode_heartbeat_payload_raw( 2345UL,
                                                         hb_payload,
                                                         sizeof( hb_payload ) );
  uchar second_v0_payload[128];
  pb_ostream_t second_v0_stream = pb_ostream_from_buffer( second_v0_payload, sizeof( second_v0_payload ) );
  test_bam_encode_string_field_raw( &second_v0_stream,
                                    bam_api_SchedulerResponseV0_heart_beat_tag,
                                    hb_payload,
                                    hb_payload_sz );

  pb_ostream_t ostream = pb_ostream_from_buffer( protobuf, sizeof( protobuf ) );
  test_bam_encode_string_field_raw( &ostream,
                                    bam_api_SchedulerResponse_v0_tag,
                                    first_v0_payload,
                                    first_v0_stream.bytes_written );
  test_bam_encode_string_field_raw( &ostream,
                                    bam_api_SchedulerResponse_v0_tag,
                                    second_v0_payload,
                                    second_v0_stream.bytes_written );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_SCHEDULER_ENVELOPE_DECODE_IDX ] == 0UL );
  FD_TEST( state->metrics.ingress_multi_message_received_cnt == 0UL );
  FD_TEST( state->metrics.ingress_message_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_MESSAGE_REJECT_REASON_V_EMPTY_MESSAGE_IDX ] == 0UL );
  FD_TEST( state->metrics.builder_heartbeats_decoded_cnt == 1UL );
  FD_TEST( test_hist_total_cnt( state->metrics.builder_heartbeat_arrival_delta_nanos ) == 1UL );
  FD_TEST( state->metrics.transaction_published_cnt == 0UL );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( state->feedback_queue_depth == 0UL );
  FD_TEST( state->bam_last_builder_activity_ns == g_clock );

  test_bam_env_destroy( env );
}

static void
test_bam_multiple_batches_isolate_nested_decode_failures( fd_wksp_t * wksp ) {
  /* MultipleAtomicTxnBatch is only a coalescing wrapper. Malformed siblings
     should not suppress valid siblings, and unattributable failures should not
     synthesize a default seq_id result. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;
  zero_meta_ts( env->out_mcache, 2UL );

  uchar first_batch[256];
  size_t first_batch_sz = test_bam_encode_one_packet_atomic_batch_raw( 71U,
                                                                       (uchar)'a',
                                                                       first_batch,
                                                                       sizeof( first_batch ) );
  uchar malformed_batch[64];
  pb_ostream_t malformed_stream = pb_ostream_from_buffer( malformed_batch, sizeof( malformed_batch ) );
  test_bam_encode_varint_field_raw( &malformed_stream, bam_types_AtomicTxnBatch_seq_id_tag, 72U );
  test_bam_encode_varint_field_raw( &malformed_stream, bam_types_AtomicTxnBatch_max_schedule_slot_tag, 1UL );
  FD_TEST( pb_encode_tag( &malformed_stream, PB_WT_STRING, bam_types_AtomicTxnBatch_packets_tag ) );
  size_t malformed_batch_sz = malformed_stream.bytes_written;
  uchar second_batch[256];
  size_t second_batch_sz = test_bam_encode_one_packet_atomic_batch_raw( 73U,
                                                                        (uchar)'b',
                                                                        second_batch,
                                                                        sizeof( second_batch ) );

  uchar const * batches[] = { first_batch, malformed_batch, second_batch };
  size_t const batch_sz[] = { first_batch_sz, malformed_batch_sz, second_batch_sz };

  uchar protobuf[768];
  size_t protobuf_sz = test_bam_encode_scheduler_multi_batch_response_raw( batches,
                                                                           batch_sz,
                                                                           3UL,
                                                                           protobuf,
                                                                           sizeof( protobuf ) );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 2UL );
  FD_TEST( test_bam_env_drain_pending_txns( env ) == 2UL );

  FD_TEST( state->metrics.transaction_published_cnt == 2UL );
  FD_TEST( state->metrics.ingress_batch_published_cnt == 2UL );
  FD_TEST( state->metrics.ingress_batch_commit_attempt_cnt == 2UL );
  FD_TEST( state->metrics.ingress_batch_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_INVALID_BATCH_IDX ] == 1UL );
  FD_TEST( state->feedback_queue_depth == 1UL );

  fd_frag_meta_t * meta = env->out_mcache;
  FD_TEST( meta[0].seq == 0UL );
  FD_TEST( meta[1].seq == 1UL );
  fd_txn_m_t * first  = fd_chunk_to_laddr( state->verify_out.mem, meta[0].chunk );
  fd_txn_m_t * second = fd_chunk_to_laddr( state->verify_out.mem, meta[1].chunk );
  FD_TEST( first->bam.seq_id == 71U );
  FD_TEST( second->bam.seq_id == 73U );

  fd_bam_bundle_result_t const * result = &state->bam_results[ state->bam_results_head ];
  FD_TEST( result->seq_id == 72U );
  FD_TEST( result->bundle_err == FD_BAM_BUNDLE_ERR_DESER );
  FD_TEST( result->deser_reason == bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE );
  FD_TEST( result->deser_index == 0U );

  test_bam_env_destroy( env );

  test_bam_env_create( env, wksp );
  state = env->state;
  zero_meta_ts( env->out_mcache, 1UL );

  uchar const malformed_before_seq[] = { 0x08U }; /* seq_id tag without varint value */
  uchar oversized_seq_batch[16];
  pb_ostream_t oversized_seq_stream = pb_ostream_from_buffer( oversized_seq_batch, sizeof( oversized_seq_batch ) );
  test_bam_encode_varint_field_raw( &oversized_seq_stream, bam_types_AtomicTxnBatch_seq_id_tag, 1UL<<32 );
  size_t oversized_seq_batch_sz = oversized_seq_stream.bytes_written;
  uchar valid_batch[256];
  size_t valid_batch_sz = test_bam_encode_one_packet_atomic_batch_raw( 81U,
                                                                       (uchar)'v',
                                                                       valid_batch,
                                                                       sizeof( valid_batch ) );
  uchar const * unknown_seq_batches[] = { malformed_before_seq, oversized_seq_batch, valid_batch };
  size_t const unknown_seq_batch_sz[] = { sizeof( malformed_before_seq ), oversized_seq_batch_sz, valid_batch_sz };

  protobuf_sz = test_bam_encode_scheduler_multi_batch_response_raw( unknown_seq_batches,
                                                                    unknown_seq_batch_sz,
                                                                    3UL,
                                                                    protobuf,
                                                                    sizeof( protobuf ) );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 1UL );
  FD_TEST( test_bam_env_drain_pending_txns( env ) == 1UL );

  FD_TEST( state->metrics.transaction_published_cnt == 1UL );
  FD_TEST( state->metrics.ingress_batch_published_cnt == 1UL );
  FD_TEST( state->metrics.ingress_batch_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_INVALID_BATCH_IDX ] == 2UL );
  FD_TEST( state->feedback_queue_depth == 0UL );

  fd_frag_meta_t * unknown_seq_meta = env->out_mcache;
  FD_TEST( unknown_seq_meta[0].seq == 0UL );
  fd_txn_m_t * tx = fd_chunk_to_laddr( state->verify_out.mem, unknown_seq_meta[0].chunk );
  FD_TEST( tx->bam.seq_id == 81U );

  test_bam_env_destroy( env );
}

#define TEST_BAM_MULTI_BATCH_WRAPPER_MAX (FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE + 2UL)

static void
test_bam_multiple_batches_accept_message_limit( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  uint const  seq_base  = 900U;
  ulong const batch_cnt = FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE;
  zero_meta_ts( env->out_mcache, batch_cnt );

  bam_types_Packet packets[ TEST_BAM_MULTI_BATCH_WRAPPER_MAX ];
  test_bam_packet_encode_ctx_t packet_ctx[ TEST_BAM_MULTI_BATCH_WRAPPER_MAX ];
  bam_types_AtomicTxnBatch batches[ TEST_BAM_MULTI_BATCH_WRAPPER_MAX ];
  test_bam_fill_single_txn_batches( packets, packet_ctx, batches, batch_cnt, seq_base, 300UL );

  uchar protobuf[ TEST_BAM_PROTOBUF_BUF_SZ ];
  size_t protobuf_sz = test_bam_encode_scheduler_multi_batch_response( batches,
                                                                       batch_cnt,
                                                                       protobuf,
                                                                       sizeof( protobuf ) );
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( test_bam_env_drain_all_pending_txns( env ) == batch_cnt );

  FD_TEST( state->metrics.transaction_published_cnt == batch_cnt );
  FD_TEST( state->metrics.ingress_batch_published_cnt == batch_cnt );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( state->feedback_queue_depth == 0UL );
  FD_TEST( state->metrics.ingress_message_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_MESSAGE_REJECT_REASON_V_OVERFLOW_MESSAGE_IDX ] == 0UL );

  fd_frag_meta_t * meta = env->out_mcache;
  for( ulong i=0UL; i<batch_cnt; i++ ) {
    FD_TEST( meta[ i ].seq == i );
    fd_txn_m_t * txnm = fd_chunk_to_laddr( state->verify_out.mem, meta[ i ].chunk );
    FD_TEST( txnm->bam.seq_id == seq_base + (uint)i );
    FD_TEST( txnm->bam.txn_cnt == 1U );
  }

  test_bam_env_destroy( env );
}

static void
test_bam_multiple_batches_overflow_results_each_excess( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  uint const  seq_base   = 1200U;
  ulong const excess_cnt = 2UL;
  ulong const batch_cnt = FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE + excess_cnt;
  zero_meta_ts( env->out_mcache, FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE );

  bam_types_Packet packets[ TEST_BAM_MULTI_BATCH_WRAPPER_MAX ];
  test_bam_packet_encode_ctx_t packet_ctx[ TEST_BAM_MULTI_BATCH_WRAPPER_MAX ];
  bam_types_AtomicTxnBatch batches[ TEST_BAM_MULTI_BATCH_WRAPPER_MAX ];
  test_bam_fill_single_txn_batches( packets, packet_ctx, batches, batch_cnt, seq_base, 400UL );

  uchar protobuf[ TEST_BAM_PROTOBUF_BUF_SZ ];
  size_t protobuf_sz = test_bam_encode_scheduler_multi_batch_response( batches,
                                                                       batch_cnt,
                                                                       protobuf,
                                                                       sizeof( protobuf ) );
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( test_bam_env_drain_all_pending_txns( env ) == FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE );

  FD_TEST( state->metrics.transaction_published_cnt == FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE );
  FD_TEST( state->metrics.ingress_batch_published_cnt == FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( state->feedback_queue_depth == excess_cnt );
  FD_TEST( state->metrics.ingress_message_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_MESSAGE_REJECT_REASON_V_OVERFLOW_MESSAGE_IDX ] == 1UL );

  fd_frag_meta_t * meta = env->out_mcache;
  FD_TEST( meta[0].seq == 0UL );
  FD_TEST( meta[FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE-1UL].seq == FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE-1UL );
  fd_txn_m_t * first = fd_chunk_to_laddr( state->verify_out.mem, meta[0].chunk );
  fd_txn_m_t * last  = fd_chunk_to_laddr( state->verify_out.mem, meta[FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE-1UL].chunk );
  FD_TEST( first->bam.seq_id == seq_base );
  FD_TEST( last->bam.seq_id == seq_base + FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE - 1U );

  for( ulong i=0UL; i<excess_cnt; i++ ) {
    fd_bam_bundle_result_t const * result =
        &state->bam_results[ (ushort)(( state->bam_results_head + i ) % FD_BAM_MAX_PENDING_RESULTS) ];
    FD_TEST( result->seq_id == seq_base + FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE + (uint)i );
    FD_TEST( result->bundle_err == FD_BAM_BUNDLE_ERR_DESER );
    FD_TEST( result->deser_reason == bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE );
    FD_TEST( result->deser_index == 0U );
  }

  test_bam_env_destroy( env );
}

static void
test_bam_bundle_forwards_without_builder_info( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  FD_TEST( state->builder_info_valid_until == 0L );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  uchar protobuf[512];
  size_t protobuf_sz = test_bam_build_scheduler_batch_msg( protobuf, sizeof(protobuf), 2U, 2, 1);
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( test_bam_env_drain_pending_txns( env ) == 2UL );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 1UL );
  FD_TEST( state->metrics.transaction_published_cnt == 2UL );
  FD_TEST( state->feedback_queue_depth == 0UL );

  fd_txn_m_t * first = (fd_txn_m_t *)fd_chunk_to_laddr( state->verify_out.mem, env->out_mcache[0].chunk );
  fd_txn_m_t * second = (fd_txn_m_t *)fd_chunk_to_laddr( state->verify_out.mem, env->out_mcache[1].chunk );
  FD_TEST( first->source_tpu == FD_TXN_M_TPU_SOURCE_BAM );
  FD_TEST( first->bam.seq_id == 2U );
  FD_TEST( first->bam.revert_on_error == 1U );
  FD_TEST( first->bam.batch_idx == 0U );
  FD_TEST( first->block_engine.bundle_id == 3UL );
  FD_TEST( first->block_engine.bundle_txn_cnt == 2UL );
  FD_TEST( second->bam.batch_idx == 1U );
  FD_TEST( second->block_engine.bundle_id == 3UL );
  FD_TEST( second->block_engine.bundle_txn_cnt == 0UL );

  test_bam_env_destroy( env );
}

typedef struct {
  uint seq_id;
  long flush_clock_ns;
  int  expected_deser_index; /* <0 means do not assert index */
  struct {
    uchar payload;
    uchar has_meta;
    uchar has_flags;
    _Bool revert_on_error;
  } packet[ 2 ];
} test_bam_revert_case_t;

static void
test_bam_bundle_revert_flag_cases( fd_wksp_t * wksp ) {
  /* These cases verify that mixed/defaulted revert_on_error flags are rejected
     as INCONSISTENT_BUNDLE. */
  test_bam_revert_case_t const cases[] = {
    /* Case 0: Explicit true/false mismatch across two packets. */
    {
      .seq_id = 42U,
      .flush_clock_ns = (long)15e9,
      .expected_deser_index = 0,
      .packet = {
        { .payload = (uchar)'Z', .has_meta = 1U, .has_flags = 1U, .revert_on_error = 1U },
        { .payload = (uchar)'[', .has_meta = 1U, .has_flags = 1U, .revert_on_error = 0U },
      }
    },
    /* Case 1: First packet omits flags (defaults false), second sets true. */
    {
      .seq_id = 44U,
      .flush_clock_ns = (long)17e9,
      .expected_deser_index = 0,
      .packet = {
        { .payload = (uchar)'x', .has_meta = 0U, .has_flags = 0U, .revert_on_error = 0U },
        { .payload = (uchar)'y', .has_meta = 1U, .has_flags = 1U, .revert_on_error = 1U },
      }
    },
    /* Case 2: First packet sets true, second omits flags (defaults false). */
    {
      .seq_id = 45U,
      .flush_clock_ns = (long)18e9,
      .expected_deser_index = 0,
      .packet = {
        { .payload = (uchar)'u', .has_meta = 1U, .has_flags = 1U, .revert_on_error = 1U },
        { .payload = (uchar)'v', .has_meta = 0U, .has_flags = 0U, .revert_on_error = 0U },
      }
    },
    /* Case 3: has_meta without has_flags still defaults false and mismatches true. */
    {
      .seq_id = 46U,
      .flush_clock_ns = (long)19e9,
      .expected_deser_index = 0,
      .packet = {
        { .payload = (uchar)'w', .has_meta = 1U, .has_flags = 0U, .revert_on_error = 0U },
        { .payload = (uchar)'z', .has_meta = 1U, .has_flags = 1U, .revert_on_error = 1U },
      }
    },
  };

  for( ulong case_idx=0UL; case_idx<sizeof(cases)/sizeof(cases[0]); case_idx++ ) {
    test_bam_revert_case_t const * tc = &cases[ case_idx ];

    test_bam_env_t env[1];
    test_bam_env_create( env, wksp );
    test_bam_env_mock_conn( env );
    fd_bam_tile_t * state = env->state;

    bam_types_Packet packets[ 2 ];
    fd_memset( packets, 0, sizeof( packets ) );
    for( ulong i=0UL; i<2UL; i++ ) {
      packets[ i ].data.size     = 1U;
      packets[ i ].data.bytes[0] = tc->packet[ i ].payload;
      if( FD_LIKELY( tc->packet[ i ].has_meta ) ) {
        packets[ i ].has_meta = 1U;
        packets[ i ].meta.has_flags = tc->packet[ i ].has_flags;
        if( FD_LIKELY( tc->packet[ i ].has_flags ) )
          packets[ i ].meta.flags.revert_on_error = tc->packet[ i ].revert_on_error;
      }
    }

    uchar protobuf[256];
    size_t protobuf_sz = test_bam_encode_scheduler_response( packets, 2UL, tc->seq_id, protobuf, sizeof( protobuf ) );

    fd_bam_client_grpc_rx_msg( state,
                               protobuf,
                               protobuf_sz,
                               FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

    FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
    FD_TEST( state->metrics.transaction_published_cnt == 0UL );
    FD_TEST( state->feedback_queue_depth == 1UL );

    test_bam_prepare_scheduler_stream( state );
    g_clock = tc->flush_clock_ns;
    test_bam_keepalive_sync( state, g_clock );
    state->bam_last_config_poll_ns = g_clock;

    FD_TEST( fd_bam_test_flush_results( state ) == 1 );
    FD_TEST( state->feedback_queue_depth == 0UL );

    test_bam_decoded_message_t decoded;
    test_bam_decode_last_message( state, &decoded );
    FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
    FD_TEST( decoded.multi.result_cnt == 1UL );
    bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
    FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
    FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_deserialization_error_tag );
    FD_TEST( result->result.not_committed.reason.deserialization_error.reason == bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE );
    if( FD_UNLIKELY( tc->expected_deser_index>=0 ) )
      FD_TEST( result->result.not_committed.reason.deserialization_error.index == (uint)tc->expected_deser_index );

    test_bam_env_destroy( env );
  }
}

static void
test_bam_non_revert_multi_packet_forwarded( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  bam_types_Packet packets[ 2 ];
  fd_memset( packets, 0, sizeof( packets ) );

  packets[0].data.size     = 1U;
  packets[0].data.bytes[0] = (uchar)'p';

  packets[1].data.size      = 1U;
  packets[1].data.bytes[0]  = (uchar)'q';
  packets[1].has_meta       = 1U;
  packets[1].meta.has_flags = 0U;

  uchar protobuf[256];
  size_t protobuf_sz = test_bam_encode_scheduler_response( packets, 2UL, 47U, protobuf, sizeof( protobuf ) );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( state->metrics.transaction_published_cnt == 0UL );
  FD_TEST( state->feedback_queue_depth == 0UL );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 2UL );

  FD_TEST( test_bam_env_drain_pending_txns( env ) == 2UL );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( state->metrics.transaction_published_cnt == 2UL );
  FD_TEST( state->feedback_queue_depth == 0UL );

  fd_txn_m_t * first  = (fd_txn_m_t *)fd_chunk_to_laddr( state->verify_out.mem, env->out_mcache[0].chunk );
  fd_txn_m_t * second = (fd_txn_m_t *)fd_chunk_to_laddr( state->verify_out.mem, env->out_mcache[1].chunk );

  FD_TEST( first->source_tpu == FD_TXN_M_TPU_SOURCE_BAM );
  FD_TEST( second->source_tpu == FD_TXN_M_TPU_SOURCE_BAM );
  FD_TEST( first->bam.seq_id == 47U );
  FD_TEST( second->bam.seq_id == 47U );
  FD_TEST( first->bam.txn_cnt == 2U );
  FD_TEST( second->bam.txn_cnt == 2U );
  FD_TEST( first->bam.batch_idx == 0U );
  FD_TEST( second->bam.batch_idx == 1U );
  FD_TEST( first->bam.revert_on_error == 0U );
  FD_TEST( second->bam.revert_on_error == 0U );
  FD_TEST( first->block_engine.bundle_id == 0UL );
  FD_TEST( second->block_engine.bundle_id == 0UL );
  FD_TEST( first->block_engine.bundle_txn_cnt == 0UL );
  FD_TEST( second->block_engine.bundle_txn_cnt == 0UL );
  FD_TEST( fd_txn_m_payload( first )[0] == (uchar)'p' );
  FD_TEST( fd_txn_m_payload( second )[0] == (uchar)'q' );

  test_bam_env_destroy( env );
}

static void
test_bam_validation_orders_revert_consistency_before_vote_rejection( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  bam_types_Packet packets[ 2 ];
  fd_memset( packets, 0, sizeof( packets ) );
  test_bam_init_simple_vote_packet( &packets[0], 1U );
  packets[1].data.size = 1U;
  packets[1].data.bytes[0] = (uchar)'x';
  packets[1].has_meta = 1U;
  packets[1].meta.has_flags = 1U;
  packets[1].meta.flags.revert_on_error = 0U;

  uchar protobuf[ 2048 ];
  size_t protobuf_sz = test_bam_encode_scheduler_response( packets, 2UL, 43U, protobuf, sizeof( protobuf ) );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( state->feedback_queue_depth == 1UL );

  test_bam_prepare_scheduler_stream( state );
  g_clock = (long)16e9;
  test_bam_keepalive_sync( state, g_clock );
  state->bam_last_config_poll_ns = g_clock;

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->feedback_queue_depth == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_deserialization_error_tag );
  FD_TEST( result->result.not_committed.reason.deserialization_error.reason ==
           bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE );
  FD_TEST( result->result.not_committed.reason.deserialization_error.index == 0U );

  test_bam_env_destroy( env );
}

static void
test_bam_bundle_rejects_real_vote_payload( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  bam_types_Packet packet[ 1 ];
  test_bam_init_simple_vote_packet( &packet[0], 1U );

  uchar protobuf[ 2048 ];
  size_t protobuf_sz = test_bam_encode_scheduler_response( packet, 1UL, 44U, protobuf, sizeof( protobuf ) );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( state->feedback_queue_depth == 1UL );

  test_bam_prepare_scheduler_stream( state );
  g_clock = (long)17e9;
  test_bam_keepalive_sync( state, g_clock );
  state->bam_last_config_poll_ns = g_clock;

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->feedback_queue_depth == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_deserialization_error_tag );
  FD_TEST( result->result.not_committed.reason.deserialization_error.reason ==
           bam_types_DeserializationErrorReason_VOTE_TRANSACTION_FAILURE );
  FD_TEST( result->result.not_committed.reason.deserialization_error.index == 0U );

  test_bam_env_destroy( env );
}

static void
test_bam_stale_slot_rejects_before_vote_error( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  state->bam_leader_state.slot = 45UL;

  bam_types_Packet packet[ 1 ];
  test_bam_init_simple_vote_packet( &packet[0], 1U );

  uchar protobuf[ 2048 ];
  size_t protobuf_sz = test_bam_encode_scheduler_response( packet, 1UL, 44U, protobuf, sizeof( protobuf ) );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( state->feedback_queue_depth == 1UL );

  test_bam_prepare_scheduler_stream( state );
  g_clock = (long)18e9;
  test_bam_keepalive_sync( state, g_clock );
  state->bam_last_config_poll_ns = g_clock;

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->feedback_queue_depth == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_scheduling_error_tag );
  FD_TEST( result->result.not_committed.reason.scheduling_error == bam_types_SchedulingError_OUTSIDE_LEADER_SLOT );

  test_bam_env_destroy( env );
}

static void
test_bam_bundle_rejects_excess_packet_count( fd_wksp_t * wksp ) {
  /* AtomicTxnBatch must reject 6 txns (hard max is 5) with
     DeserializationErrorReason::SANITIZE_ERROR at index 0. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  size_t packet_cnt = FD_BAM_MAX_TXN_PER_ATOMIC_BATCH + 1UL;
  bam_types_Packet packets[ FD_BAM_MAX_TXN_PER_ATOMIC_BATCH + 1UL ];
  fd_memset( packets, 0, sizeof( packets ) );
  for( size_t i=0UL; i<packet_cnt; i++ ) {
    packets[ i ].data.size = 1U;
    packets[ i ].data.bytes[0] = (uchar)( 'a' + (int)( i % 26UL ) );
  }

  uchar protobuf[ 2048 ];
  size_t protobuf_sz = test_bam_encode_scheduler_response( packets, packet_cnt, 50U, protobuf, sizeof( protobuf ) );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( state->metrics.transaction_published_cnt == 0UL );
  FD_TEST( state->feedback_queue_depth == 1UL );

  test_bam_prepare_scheduler_stream( state );
  g_clock = (long)17e9;
  test_bam_keepalive_sync( state, g_clock );
  state->bam_last_config_poll_ns = g_clock;

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->feedback_queue_depth == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_deserialization_error_tag );
  FD_TEST( result->result.not_committed.reason.deserialization_error.reason ==
           bam_types_DeserializationErrorReason_SANITIZE_ERROR );
  FD_TEST( result->result.not_committed.reason.deserialization_error.index ==
           0U );

  test_bam_env_destroy( env );
}

static void
test_bam_bundle_uses_data_length_for_size_check( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  /* An oversized legacy metadata value must not veto a valid data payload. */
  FD_TEST( bam_dump_txn_fixture_sz<=FD_TXN_MTU );
  uchar txn_buf[ FD_TXN_MAX_SZ ];
  FD_TEST( fd_txn_parse( bam_dump_txn_fixture, bam_dump_txn_fixture_sz, txn_buf, NULL ) );
  FD_TEST( !fd_txn_is_simple_vote_transaction( (fd_txn_t const *)txn_buf, bam_dump_txn_fixture ) );

  bam_types_Packet packet = bam_types_Packet_init_default;
  packet.data.size = (pb_size_t)bam_dump_txn_fixture_sz;
  fd_memcpy( packet.data.bytes, bam_dump_txn_fixture, bam_dump_txn_fixture_sz );
  packet.has_meta  = 1;
  packet.meta.size = UINT64_MAX;

  uchar protobuf[ 4096 ];
  size_t protobuf_sz = test_bam_encode_scheduler_response( &packet, 1UL, 51U, protobuf, sizeof( protobuf ) );
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.ingress_packet_oversize_cnt == 0UL );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 1UL );
  fd_bam_pending_txn_t const * pending = bam_pending_txn_peek_head_const( state->pending_txns );
  FD_TEST( pending->payload_sz == bam_dump_txn_fixture_sz );
  FD_TEST( !memcmp( pending->payload, bam_dump_txn_fixture, bam_dump_txn_fixture_sz ) );
  FD_TEST( test_bam_env_drain_pending_txns( env ) == 1UL );
  FD_TEST( state->metrics.transaction_published_cnt == 1UL );

  test_bam_env_destroy( env );
}

/* Ensure a scheduler batch containing zero packets is surfaced as an EMPTY
   deserialization error while leaving bundle/txn metrics untouched. */
static void
test_bam_bundle_rejects_empty_batch( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  uchar protobuf[256];
  test_bam_packet_encode_ctx_t packets_ctx = {
    .packets    = NULL,
    .packet_cnt = 0UL
  };

  bam_types_AtomicTxnBatch batch = bam_types_AtomicTxnBatch_init_default;
  batch.seq_id = 55;
  batch.max_schedule_slot = 1UL;
  batch.packets.funcs.encode = test_bam_encode_packets_cb;
  batch.packets.arg          = &packets_ctx;

  test_bam_batch_encode_ctx_t batches_ctx = {
    .batches   = &batch,
    .batch_cnt = 1UL
  };

  bam_types_MultipleAtomicTxnBatch multi = bam_types_MultipleAtomicTxnBatch_init_default;
  multi.batches.funcs.encode = test_bam_encode_batches_cb;
  multi.batches.arg          = &batches_ctx;

  bam_api_SchedulerResponse resp = bam_api_SchedulerResponse_init_default;
  resp.which_versioned_msg = bam_api_SchedulerResponse_v0_tag;
  resp.versioned_msg.v0.which_resp = bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag;
  resp.versioned_msg.v0.resp.multiple_atomic_txn_batch = multi;

  pb_ostream_t ostream = pb_ostream_from_buffer( protobuf, sizeof( protobuf ) );
  FD_TEST( pb_encode( &ostream, bam_api_SchedulerResponse_fields, &resp ) );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( state->metrics.transaction_published_cnt == 0UL );
  FD_TEST( state->feedback_queue_depth == 1UL );

  test_bam_prepare_scheduler_stream( state );
  g_clock = (long)19e9;
  test_bam_keepalive_sync( state, g_clock );
  state->bam_last_config_poll_ns = g_clock;

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->feedback_queue_depth == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->seq_id == 55U );
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_deserialization_error_tag );
  FD_TEST( result->result.not_committed.reason.deserialization_error.reason == bam_types_DeserializationErrorReason_EMPTY );
  FD_TEST( result->result.not_committed.reason.deserialization_error.index == 0U );

  test_bam_env_destroy( env );
}

/* Ensure an InitSchedulerStream response that omits the batches array entirely
   is counted as an empty message without fabricating an AtomicTxnBatchResult. */
static void
test_bam_bundle_rejects_missing_batches( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  uchar protobuf[256];
  size_t out_sz = sizeof( protobuf );
  test_bam_batch_encode_ctx_t batches_ctx = {
    .batches   = NULL,
    .batch_cnt = 0UL
  };

  bam_types_MultipleAtomicTxnBatch multi = bam_types_MultipleAtomicTxnBatch_init_default;
  multi.batches.funcs.encode = test_bam_encode_batches_cb;
  multi.batches.arg          = &batches_ctx;

  bam_api_SchedulerResponse resp = bam_api_SchedulerResponse_init_default;
  resp.which_versioned_msg = bam_api_SchedulerResponse_v0_tag;
  resp.versioned_msg.v0.which_resp = bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag;
  resp.versioned_msg.v0.resp.multiple_atomic_txn_batch = multi;

  pb_ostream_t ostream = pb_ostream_from_buffer( protobuf, out_sz );
  FD_TEST( pb_encode( &ostream, bam_api_SchedulerResponse_fields, &resp ) );

  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );

  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( state->metrics.transaction_published_cnt == 0UL );
  FD_TEST( state->feedback_queue_depth == 0UL );
  FD_TEST( state->metrics.ingress_multi_message_received_cnt == 1UL );
  FD_TEST( state->metrics.ingress_message_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_MESSAGE_REJECT_REASON_V_EMPTY_MESSAGE_IDX ] == 1UL );

  test_bam_env_destroy( env );
}

/* --- Connection lifecycle and watchdog ----------------------------------------------- */

static int
test_bam_loopback_listener( struct sockaddr_in * addr ) {
  int listen_sock = socket( AF_INET, SOCK_STREAM|SOCK_CLOEXEC, 0 );
  FD_TEST( listen_sock >= 0 );

  *addr = (struct sockaddr_in) {
    .sin_family      = AF_INET,
    .sin_addr.s_addr = FD_IP4_ADDR( 127, 0, 0, 1 ),
    .sin_port        = 0
  };
  FD_TEST( 0 == bind( listen_sock, fd_type_pun( addr ), sizeof(*addr) ) );
  FD_TEST( 0 == listen( listen_sock, 1 ) );

  socklen_t addr_len = sizeof(*addr);
  FD_TEST( 0 == getsockname( listen_sock, fd_type_pun( addr ), &addr_len ) );
  return listen_sock;
}

static void
test_bam_stream_end_with_pending_txn_reconnects( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  uchar fseq_mem[ FD_FSEQ_FOOTPRINT ] __attribute__((aligned(FD_FSEQ_ALIGN)));
  void * fseq_shmem = fd_fseq_new( fseq_mem, 0UL );
  FD_TEST( fseq_shmem );
  ulong * fseq = fd_fseq_join( fseq_shmem );
  FD_TEST( fseq );
  state->bam_status_fseq = fseq;

  test_bam_env_mock_conn( env );

  /* Queue scheduler work before exercising connection handoff/reset paths. */
  uchar protobuf[ 256 ];
  size_t protobuf_sz = test_bam_build_scheduler_batch_msg( protobuf, sizeof(protobuf), 77U, 1U, 0 );
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             protobuf_sz,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 1UL );
  fd_bam_pending_txn_t const * pending = bam_pending_txn_peek_head_const( state->pending_txns );
  FD_TEST( pending->seq_id == 77U );
  FD_TEST( pending->payload_sz == 1U );
  FD_TEST( pending->payload[0] == (uchar)'A' );

  /* A healthy connection awaiting local activation must preserve receive
     backpressure, while a deferred reset still takes precedence. */
  FD_TEST( 0 == close( env->server_sock ) );
  env->server_sock = -1;
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );

  int charge_busy = 0;
  fd_bam_test_before_credit( state, env->stem, &charge_busy );
  FD_TEST( state->tcp_sock >= 0 );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 1UL );

  state->defer_reset = 1U;
  fd_bam_test_before_credit( state, env->stem, &charge_busy );
  FD_TEST( state->tcp_sock == -1 );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 1UL );

  test_bam_env_mock_conn( env );
  fd_fseq_update( fseq, FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );

  fd_grpc_resp_hdrs_t trailers = {
    .h2_status   = 200U,
    .grpc_status = FD_GRPC_STATUS_OK
  };
  fd_bam_client_grpc_rx_end( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream, &trailers );
  FD_TEST( state->defer_reset == 1U );
  FD_TEST( state->bam_stream_live == 0U );

  charge_busy = 0;
  fd_bam_client_step( state, &charge_busy );
  FD_TEST( charge_busy == 1 );
  FD_TEST( state->defer_reset == 0U );
  FD_TEST( state->tcp_sock == -1 );
  FD_TEST( state->tcp_sock_connected == 0U );
  FD_TEST( fd_fseq_query( fseq ) == 0UL );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 1UL );

  FD_TEST( 0 == close( env->server_sock ) );
  env->server_sock = -1;

  /* The exact post-reset state must still start a new connection even though
     scheduler work remains queued. */
  struct sockaddr_in addr;
  int listen_sock = test_bam_loopback_listener( &addr );
  fd_cstr_ncpy( state->server_fqdn, "127.0.0.1", sizeof(state->server_fqdn) );
  state->server_fqdn_len = sizeof("127.0.0.1")-1UL;
  state->server_tcp_port = fd_ushort_bswap( addr.sin_port );
  state->backoff_until   = 0L;

  charge_busy = 0;
  fd_bam_test_before_credit( state, env->stem, &charge_busy );
  FD_TEST( charge_busy == 1 );
  FD_TEST( state->tcp_sock >= 0 );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 1UL );

  env->server_sock = accept4( listen_sock, NULL, NULL, SOCK_CLOEXEC );
  FD_TEST( env->server_sock >= 0 );
  FD_TEST( 0 == close( listen_sock ) );

  /* Once the override is active, preserve backpressure: drain the queued work
     before servicing a deferred connection reset. */
  int reconnect_sock = state->tcp_sock;
  fd_fseq_update( fseq, FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
  state->defer_reset = 1U;
  charge_busy = 0;
  fd_bam_test_before_credit( state, env->stem, &charge_busy );
  FD_TEST( charge_busy == 0 );
  FD_TEST( state->defer_reset == 1U );
  FD_TEST( state->tcp_sock == reconnect_sock );

  int opt_poll_in = 1;
  fd_bam_test_after_credit( state, env->stem, &opt_poll_in, &charge_busy );
  FD_TEST( charge_busy == 1 );
  FD_TEST( opt_poll_in == 0 );
  FD_TEST( env->stem_seqs[0] == 1UL );
  FD_TEST( bam_pending_txn_empty( state->pending_txns ) );
  fd_txn_m_t const * forwarded = fd_chunk_to_laddr_const( state->verify_out.mem, 0UL );
  FD_TEST( forwarded->bam.seq_id == 77U );
  FD_TEST( forwarded->payload_sz == 1U );
  FD_TEST( fd_txn_m_payload_const( forwarded )[0] == (uchar)'A' );

  charge_busy = 0;
  fd_bam_test_before_credit( state, env->stem, &charge_busy );
  FD_TEST( charge_busy == 1 );
  FD_TEST( state->defer_reset == 0U );
  FD_TEST( state->tcp_sock == -1 );
  FD_TEST( fd_fseq_query( fseq ) == 0UL );

  state->bam_status_fseq = NULL;
  FD_TEST( fd_fseq_leave( fseq ) == fseq_shmem );
  FD_TEST( fd_fseq_delete( fseq_shmem ) == fseq_shmem );
  test_bam_env_destroy( env );
}

static void
test_bam_tcp_connect_completion_uses_so_error( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  struct sockaddr_in addr;
  int listen_sock = test_bam_loopback_listener( &addr );

  int client_sock = socket( AF_INET, SOCK_STREAM|SOCK_CLOEXEC, 0 );
  FD_TEST( client_sock >= 0 );
  FD_TEST( 0 == connect( client_sock, fd_type_pun_const( &addr ), sizeof(addr) ) );

  int server_sock = accept4( listen_sock, NULL, NULL, SOCK_CLOEXEC );
  FD_TEST( server_sock >= 0 );
  FD_TEST( 0 == close( listen_sock ) );

  int flags = fcntl( client_sock, F_GETFL, 0 );
  FD_TEST( flags >= 0 );
  FD_TEST( 0 == fcntl( client_sock, F_SETFL, flags|O_NONBLOCK ) );

  state->tcp_sock           = client_sock;
  state->tcp_sock_connected = 0U;

  int charge_busy = 0;
  fd_bam_client_step( state, &charge_busy );

  FD_TEST( charge_busy == 1 );
  FD_TEST( state->tcp_sock == client_sock );
  FD_TEST( state->tcp_sock_connected == 1U );

  FD_TEST( 0 == close( server_sock ) );
  test_bam_env_destroy( env );
}

static void
test_bam_grpc_end_handling( fd_wksp_t * wksp ) {
  /* Scheduler stream closures should clear bam_stream state and force a reset so
     the next connection attempt re-resolves DNS/load-balances like bundle. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  fd_grpc_client_t * client = state->grpc_client;
  fd_grpc_h2_stream_t * stream = fd_grpc_client_stream_acquire( client, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( stream );
  stream->hdrs.h2_status     = 200;
  stream->hdrs.is_grpc_proto = 1;
  state->bam_stream = stream;
  fd_bam_client_grpc_rx_start( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( state->bam_stream_live == 1U );
  state->bam_leader_pending = 1U;

  fd_grpc_resp_hdrs_t hdrs_fail = {
      .h2_status   = 200,
      .grpc_status = FD_GRPC_STATUS_UNAVAILABLE
  };
  fd_bam_client_grpc_rx_end( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream, &hdrs_fail );
  FD_TEST( state->bam_stream_live == 0U );
  FD_TEST( state->bam_stream_connecting == 0U );
  FD_TEST( state->bam_stream == NULL );
  FD_TEST( state->bam_leader_pending == 0U );
  FD_TEST( state->defer_reset == 1U );
  FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_REQUEST_FAILED_IDX ] == 1UL );
  FD_TEST( state->metrics.leader_pending_dropped_cnt[ FD_METRICS_ENUM_BAM_LEADER_PENDING_DROP_REASON_V_REQUEST_FAILED_IDX ] == 1UL );
  FD_TEST( state->metrics.leader_pending_dropped_cnt[ FD_METRICS_ENUM_BAM_LEADER_PENDING_DROP_REASON_V_STREAM_ENDED_IDX ] == 0UL );

  state->defer_reset = 0U;
  stream = fd_grpc_client_stream_acquire( client, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( stream );
  stream->hdrs.h2_status     = 200;
  stream->hdrs.is_grpc_proto = 1;
  state->bam_stream = stream;
  fd_bam_client_grpc_rx_start( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( state->bam_stream_live == 1U );
  state->bam_leader_pending = 1U;

  fd_grpc_resp_hdrs_t hdrs_ok = {
      .h2_status   = 200,
      .grpc_status = FD_GRPC_STATUS_OK
  };
  fd_bam_client_grpc_rx_end( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream, &hdrs_ok );
  FD_TEST( state->bam_stream_live == 0U );
  FD_TEST( state->bam_stream_connecting == 0U );
  FD_TEST( state->bam_stream == NULL );
  FD_TEST( state->bam_leader_pending == 0U );
  FD_TEST( state->defer_reset == 1U );
  FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_REQUEST_FAILED_IDX ] == 1UL );
  FD_TEST( state->metrics.leader_pending_dropped_cnt[ FD_METRICS_ENUM_BAM_LEADER_PENDING_DROP_REASON_V_REQUEST_FAILED_IDX ] == 1UL );
  FD_TEST( state->metrics.leader_pending_dropped_cnt[ FD_METRICS_ENUM_BAM_LEADER_PENDING_DROP_REASON_V_STREAM_ENDED_IDX ] == 1UL );

  test_bam_env_destroy( env );
}

static void
test_bam_grpc_timeout( fd_wksp_t * wksp ) {
  /* Deadline expiry cancels inflight auth/scheduler attempts and marks the
     client for reset. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  state->bam_auth_inflight      = 1U;
  state->bam_auth_ready         = 1U;
  fd_cstr_ncpy( state->challenge_to_sign, "stale-challenge", sizeof( state->challenge_to_sign ) );
  fd_bam_client_grpc_rx_timeout( state, FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge, FD_GRPC_DEADLINE_HEADER );
  FD_TEST( state->bam_auth_inflight == 0U );
  FD_TEST( state->bam_auth_ready == 0U );
  FD_TEST( state->challenge_to_sign[ 0 ] == '\0' );
  FD_TEST( state->defer_reset == 1U );
  FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_REQUEST_TIMEOUT_IDX ] == 1UL );

  state->defer_reset = 0U;
  state->bam_stream_live       = 1U;
  state->bam_stream_connecting = 1U;
  state->bam_leader_pending    = 1U;
  fd_grpc_h2_stream_t * timeout_stream = fd_grpc_client_stream_acquire( state->grpc_client, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( timeout_stream );
  state->bam_stream = timeout_stream;
  fd_bam_client_grpc_rx_timeout( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream, FD_GRPC_DEADLINE_RX_END );
  FD_TEST( state->bam_stream_live == 0U );
  FD_TEST( state->bam_stream_connecting == 0U );
  FD_TEST( state->bam_leader_pending == 0U );
  FD_TEST( state->defer_reset == 1U );
  FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_REQUEST_TIMEOUT_IDX ] == 2UL );

  test_bam_env_destroy( env );
}

static fd_bam_tile_t *
test_bam_heartbeat_env_start( test_bam_env_t * env,
                              fd_wksp_t *      wksp ) {
  /* Helper to boot a connected client with deterministic keepalive timestamps
     for watchdog tests. */
  g_clock = (long)1e9;
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;
  state->enabled = 1;
  state->keepalive->ts_deadline = LONG_MAX;
  state->keepalive->ts_last_tx = g_clock;
  state->keepalive->ts_last_rx = g_clock;
  state->keepalive->inflight   = 0;
  state->defer_reset = 0;
  test_bam_keepalive_sync( state, g_clock );
  state->keepalive_interval    = LONG_MAX;
  state->keepalive->interval   = 0L;
  state->keepalive->timeout    = LONG_MAX;
  state->keepalive->ts_next_tx = LONG_MAX;
  state->keepalive->ts_deadline = LONG_MAX;
  state->keepalive->inflight   = 0U;
  fd_bam_client_grpc_rx_start( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( state->bam_last_builder_activity_ns == g_clock );
  FD_TEST( state->bam_last_validator_heartbeat_ns == g_clock );
  return state;
}

static void
test_bam_scheduler_stream_replays_only_retained_leader_state( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn_empty( env );
  fd_bam_tile_t * state = env->state;
  test_bam_env_mock_h2_hs( state );

  g_clock = (long)8e9;
  test_bam_keepalive_sync( state, g_clock );
  state->keepalive->ts_next_tx = LONG_MAX;
  state->builder_info_valid_until = g_clock + (long)60e9;
  state->bam_config_received      = 1U;
  state->bam_last_config_poll_ns  = g_clock;
  state->bam_last_validator_heartbeat_ns = g_clock;

  test_bam_prepare_scheduler_stream( state );
  state->bam_stream_live       = 0U;
  state->bam_stream_connecting = 1U;

  FD_TEST( state->bam_leader_state.slot == ULONG_MAX );
  fd_bam_client_grpc_rx_start( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( state->bam_leader_pending == 0U );

  state->bam_leader_state = (fd_bam_leader_state_t){
    .slot = 41UL,
    .tick = 12U,
    .slot_cu_budget_remaining = 321U,
    .slot_end_ns = g_clock - FD_BAM_LEADER_STATE_EXPIRY_GRACE_NS - 1L,
    .current_slot_has_bam_work = 1U
  };
  state->bam_leader_pending = 1U;

  fd_bam_client_grpc_rx_start( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( state->bam_leader_pending == 0U );
  FD_TEST( fd_bam_test_client_step_reconnect( state, g_clock ) == 0 );

  state->bam_leader_state = (fd_bam_leader_state_t){
    .slot = 42UL,
    .tick = 8U,
    .slot_cu_budget_remaining = 456U,
    .slot_end_ns = g_clock - FD_BAM_LEADER_STATE_EXPIRY_GRACE_NS + 1L,
    .current_slot_has_bam_work = 1U
  };
  state->bam_leader_pending = 0U;
  state->bam_stream_live       = 0U;
  state->bam_stream_connecting = 1U;

  fd_bam_client_grpc_rx_start( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( state->bam_leader_pending == 1U );

  int busy = fd_bam_test_client_step_reconnect( state, g_clock );
  FD_TEST( busy == 1 );
  FD_TEST( state->bam_leader_pending == 0U );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.which_versioned_msg == bam_api_SchedulerMessage_v0_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_leader_state_tag );
  bam_types_LeaderState const * ls = &decoded.msg.versioned_msg.v0.msg.leader_state;
  FD_TEST( ls->slot == 42UL );
  FD_TEST( ls->tick == 8U );
  FD_TEST( ls->slot_cu_budget_remaining == 456U );

  state->bam_leader_state = (fd_bam_leader_state_t){
    .slot = 43UL,
    .tick = 7U,
    .slot_cu_budget_remaining = 123U,
    .slot_end_ns = g_clock + (long)1e9,
    .current_slot_has_bam_work = 1U
  };
  state->bam_leader_pending = 0U;
  state->bam_stream_live       = 0U;
  state->bam_stream_connecting = 1U;

  fd_bam_client_grpc_rx_start( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( state->bam_leader_pending == 1U );

  busy = fd_bam_test_client_step_reconnect( state, g_clock );
  FD_TEST( busy == 1 );
  FD_TEST( state->bam_leader_pending == 0U );

  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.which_versioned_msg == bam_api_SchedulerMessage_v0_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_leader_state_tag );
  ls = &decoded.msg.versioned_msg.v0.msg.leader_state;
  FD_TEST( ls->slot == 43UL );
  FD_TEST( ls->tick == 7U );
  FD_TEST( ls->slot_cu_budget_remaining == 123U );

  test_bam_env_destroy( env );
}

static void
test_bam_heartbeat_timeout_forces_disconnect( fd_wksp_t * wksp ) {
  /* Test 1: Watchdog drops the connection once builder activity is stale (>6s) while streams are live. */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    int charge_busy = 0;
    g_clock += FD_BAM_ACTIVITY_TIMEOUT_NS + (long)1e8;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->tcp_sock == -1 );
    FD_TEST( state->tcp_sock_connected == 0U );
    FD_TEST( charge_busy == 1 );
    FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_BUILDER_ACTIVITY_TIMEOUT_IDX ] == 1UL );
    test_bam_env_destroy( env );
  }

  /* Test 2: An uninitialized builder-activity timestamp (0L) should never trip the watchdog. */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    state->bam_last_builder_activity_ns = 0L;
    int charge_busy = 0;
    g_clock += FD_BAM_ACTIVITY_TIMEOUT_NS + (long)1e9;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->tcp_sock >= 0 );
    FD_TEST( state->tcp_sock_connected == 1U );
    test_bam_env_destroy( env );
  }

  /* Test 3: If the scheduler stream is down, the watchdog should stay idle even with stale timestamps. */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    state->bam_stream_live = 0;
    int charge_busy = 0;
    g_clock += FD_BAM_ACTIVITY_TIMEOUT_NS + (long)1e9;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->tcp_sock >= 0 );
    test_bam_env_destroy( env );
  }

  /* Test 4: Just before the 6s boundary we should not disconnect (off-by-one guard). */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    int charge_busy = 0;
    g_clock += FD_BAM_ACTIVITY_TIMEOUT_NS - (long)1e6; /* 1ms before timeout */
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->tcp_sock >= 0 );
    test_bam_env_destroy( env );
  }

  /* Test 5: The exact 6s boundary should still trigger a disconnect. */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    int charge_busy = 0;
    g_clock += FD_BAM_ACTIVITY_TIMEOUT_NS;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->tcp_sock == -1 );
    FD_TEST( charge_busy == 1 );
    test_bam_env_destroy( env );
  }

  /* Test 6: Disabling BAM runtime should bypass the watchdog entirely. */
  {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    state->enabled = 0;
    int charge_busy = 0;
    g_clock += FD_BAM_ACTIVITY_TIMEOUT_NS + (long)1e9;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->tcp_sock >= 0 );
    test_bam_env_destroy( env );
  }
}

static void
test_bam_heartbeat_reset_extends_timeout( fd_wksp_t * wksp ) {
  /* Uses helper-generated scheduler responses to drive activity timestamping
     and ensure batches refresh the watchdog deadline. */

  /* Heartbeat message updates timestamp and produces one heartbeat-latency sample. */
  {
    /* Subtest: direct heartbeat bumps the builder-activity timestamp. */
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    test_bam_send_scheduler_heartbeat( state, 1UL );
    long expected_ts = g_clock;
    FD_TEST( state->bam_last_builder_activity_ns == expected_ts );
    FD_TEST( state->metrics.builder_heartbeats_decoded_cnt == 1UL );
    FD_TEST( test_hist_total_cnt( state->metrics.builder_heartbeat_arrival_delta_nanos ) == 1UL );
    test_bam_env_destroy( env );
  }

  /* 5.9 seconds after heartbeat should NOT timeout: verifies a recent heartbeat defers disconnect. */
  {
    /* Subtest: watchdog stays armed-but-waiting when heartbeat is recent. */
    test_bam_env_t env[1];
   fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    int charge_busy = 0;
    g_clock += FD_BAM_ACTIVITY_TIMEOUT_NS - (long)2e8;
    test_bam_send_scheduler_heartbeat( state, 1UL );
    g_clock += FD_BAM_ACTIVITY_TIMEOUT_NS - (long)1e8;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->tcp_sock >= 0 );
    test_bam_env_destroy( env );
  }

  /* Bundle batches update timestamp because executing work should also count as liveness. */
  {
    /* Subtest: executing a bundle refreshes watchdog like a heartbeat. */
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    test_bam_send_scheduler_bundle( state, 0U, 0 );
    long expected_ts = g_clock;
    FD_TEST( state->bam_last_builder_activity_ns == expected_ts );
    test_bam_env_destroy( env );
  }
}

static void
test_bam_client_status( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;
  test_bam_env_mock_conn_empty( env );
  test_bam_env_mock_h2_hs( state );
  long now = fd_bam_now();
  state->bam_last_builder_activity_ns    = now;
  state->bam_last_validator_heartbeat_ns = now;
  state->bam_stream_live                 = 1U;

  /* Connected transport without a ConfigResponse is still unhealthy. */
  FD_TEST( state->bam_config_received == 0U );
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY );

  {
    bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
    resp.has_block_engine_config = true;
    resp.block_engine_config.builder_commission = 7U;
    uchar pb_buf[ 256 ];
    pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof( pb_buf ) );
    FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
    state->bam_config_inflight = 1U;
    fd_bam_client_grpc_rx_msg( state,
                               pb_buf,
                               ostream.bytes_written,
                               FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );
  }
  FD_TEST( state->bam_config_received == 0U );
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY );

  test_bam_env_inject_config_response( state );
  FD_TEST( state->bam_config_received == 1U );
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );

  fd_bam_tile_t state_backup = *state;
  fd_grpc_client_t client_backup = *state->grpc_client;

  /* Connections should start unhealthy until builder activity arrives. */
  state->bam_last_builder_activity_ns = 0L;
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY );

  /* Stale builder activity should not report healthy. */
  state->bam_last_builder_activity_ns = fd_bam_now() - FD_BAM_ACTIVITY_TIMEOUT_NS;
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY );

  // verify restore gets us back to HEALTHY
  *state = state_backup;
  *state->grpc_client = client_backup;
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );

  /* Runtime disabled should bypass transport checks and report DISABLED. */
  state->enabled = 0;
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISABLED );
  *state = state_backup;
  *state->grpc_client = client_backup;

  /* Keepalive inflight with a future deadline should still be HEALTHY. */
  state->keepalive->inflight   = 1U;
  state->keepalive->ts_deadline = fd_bam_now() + state->keepalive->timeout + (long)1e6;
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
  *state = state_backup;
  *state->grpc_client = client_backup;

  /* Negative/garbled builder-activity timestamp is treated as UNHEALTHY. */
  state->bam_last_builder_activity_ns = -1L;
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY );
  *state = state_backup;
  *state->grpc_client = client_backup;

  state->tcp_sock_connected = 0U;
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED );
  *state = state_backup;

  ushort const conn_dead_flags[] = { FD_H2_CONN_FLAGS_DEAD, FD_H2_CONN_FLAGS_SEND_GOAWAY };
  for( ulong i=0UL; i<sizeof(conn_dead_flags)/sizeof(conn_dead_flags[0]); i++ ) {
    FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
    state->grpc_client->conn->flags |= conn_dead_flags[ i ];
    FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED );
    *state->grpc_client = client_backup;
  }

  ushort const conn_prog_flags[] = {
    FD_H2_CONN_FLAGS_CLIENT_INITIAL,
    FD_H2_CONN_FLAGS_WAIT_SETTINGS_ACK_0,
    FD_H2_CONN_FLAGS_WAIT_SETTINGS_0,
    FD_H2_CONN_FLAGS_SERVER_INITIAL
  };
  for( ulong i=0UL; i<sizeof(conn_prog_flags)/sizeof(conn_prog_flags[0]); i++ ) {
    FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
    state->grpc_client->conn->flags |= conn_prog_flags[ i ];
    FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTING );
    *state->grpc_client = client_backup;
  }

  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
  state->bam_stream_live = 0U;
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTING );
  state->bam_stream_live = 1U;

  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
  state->keepalive->inflight = 1U;
  state->keepalive->ts_deadline = state->keepalive->ts_last_tx;
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED );
  *state = state_backup;
  *state->grpc_client = client_backup;

  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
  state->grpc_client->h2_hs_done = 0;
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTING );

  test_bam_env_destroy( env );
}

/* --- Scheduler/auth messaging -------------------------------------------------------- */

static void
test_bam_make_ed25519_keypair( uchar public_key[ 32 ],
                               uchar private_key[ 32 ],
                               uchar seed ) {
  for( ulong i=0UL; i<32UL; i++ ) private_key[ i ] = (uchar)( seed + i );

  fd_sha512_t _sha[1];
  fd_sha512_t * sha = fd_sha512_join( fd_sha512_new( _sha ) );
  FD_TEST( sha );
  fd_ed25519_public_from_private( public_key, private_key, sha );
}

static void
test_bam_sign_auth_challenge( uchar        signature[ 64 ],
                              uchar const  public_key[ 32 ],
                              uchar const  private_key[ 32 ],
                              char const * challenge,
                              ulong        challenge_len ) {
  uchar payload[ FD_BAM_AUTH_LABEL_LEN + sizeof(bam_api_AuthChallengeResponse) ];
  fd_memcpy( payload, FD_BAM_AUTH_LABEL, FD_BAM_AUTH_LABEL_LEN );
  fd_memcpy( payload + FD_BAM_AUTH_LABEL_LEN, challenge, challenge_len );
  ulong payload_sz = FD_BAM_AUTH_LABEL_LEN + challenge_len;

  fd_sha512_t _sha[1];
  fd_sha512_t * sha = fd_sha512_join( fd_sha512_new( _sha ) );
  FD_TEST( sha );
  fd_ed25519_sign( signature, payload, payload_sz, public_key, private_key, sha );
}

static void
test_bam_publish_keyguard_signature( fd_keyguard_client_t * client,
                                     fd_frag_meta_t *       response_mcache,
                                     uchar const            signature[ 64 ] ) {
  ulong resp_chunk = client->response_chunk0;
  fd_memcpy( fd_chunk_to_laddr( client->response_mem, resp_chunk ),
             signature, 64UL );
  fd_mcache_publish( response_mcache,
                     client->response_depth,
                     client->response_seq,
                     0UL,
                     resp_chunk,
                     64UL,
                     0UL,
                     0UL,
                     0UL );
}

static void
test_bam_auth_challenge_response_verifies_cached_identity( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  ulong const depth        = 8UL;
  ulong const request_mtu  = 256UL;
  ulong const response_mtu = 64UL;

  void * request_mem = fd_wksp_alloc_laddr(
      wksp, fd_mcache_align(), fd_mcache_footprint( depth, 0UL ), 1UL );
  FD_TEST( request_mem );
  fd_frag_meta_t * request_mcache = fd_mcache_join(
      fd_mcache_new( request_mem, depth, 0UL, 0UL ) );
  FD_TEST( request_mcache );

  void * response_mem = fd_wksp_alloc_laddr(
      wksp, fd_mcache_align(), fd_mcache_footprint( depth, 0UL ), 1UL );
  FD_TEST( response_mem );
  fd_frag_meta_t * response_mcache = fd_mcache_join(
      fd_mcache_new( response_mem, depth, 0UL, 0UL ) );
  FD_TEST( response_mcache );

  ulong request_data_sz = fd_dcache_req_data_sz( request_mtu, depth, 1UL, 1 );
  void * request_dcache_shmem = fd_wksp_alloc_laddr( wksp, fd_dcache_align(), fd_dcache_footprint( request_data_sz, 0UL ), 1UL );
  FD_TEST( request_dcache_shmem );
  uchar * request_data = fd_dcache_join( fd_dcache_new( request_dcache_shmem, request_data_sz, 0UL ) );
  FD_TEST( request_data );

  ulong response_data_sz = fd_dcache_req_data_sz( response_mtu, depth, 1UL, 1 );
  void * response_dcache_shmem = fd_wksp_alloc_laddr( wksp, fd_dcache_align(), fd_dcache_footprint( response_data_sz, 0UL ), 1UL );
  FD_TEST( response_dcache_shmem );
  uchar * response_data = fd_dcache_join( fd_dcache_new( response_dcache_shmem, response_data_sz, 0UL ) );
  FD_TEST( response_data );

  FD_TEST( fd_keyguard_client_new( state->keyguard_client,
                                   request_mcache, request_data,
                                   response_mcache, response_data, request_mtu ) );

  bam_api_AuthChallengeResponse bad_resp = bam_api_AuthChallengeResponse_init_default;
  fd_cstr_ncpy( bad_resp.challenge_to_sign, "12345678", sizeof( bad_resp.challenge_to_sign ) );

  uchar bad_pb_buf[ 128 ];
  pb_ostream_t bad_ostream = pb_ostream_from_buffer( bad_pb_buf, sizeof(bad_pb_buf) );
  FD_TEST( pb_encode( &bad_ostream, bam_api_AuthChallengeResponse_fields, &bad_resp ) );

  state->bam_auth_inflight = 1U;
  fd_bam_client_grpc_rx_msg( state,
                             bad_pb_buf,
                             bad_ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge );

  FD_TEST( state->bam_auth_inflight == 0U );
  FD_TEST( state->bam_auth_ready == 0U );
  FD_TEST( state->challenge_to_sign[ 0 ] == '\0' );
  FD_TEST( state->keyguard_client->request_seq == 0UL );

  bam_api_AuthChallengeResponse resp = bam_api_AuthChallengeResponse_init_default;
  char const challenge[] = "unit-test-challenge";
  size_t const challenge_len = strlen( challenge );
  fd_cstr_ncpy( resp.challenge_to_sign, challenge, sizeof( resp.challenge_to_sign ) );

  uchar public_key[ 32 ];
  uchar private_key[ 32 ];
  test_bam_make_ed25519_keypair( public_key, private_key, 1U );
  fd_memcpy( state->bam_identity_pubkey, public_key, sizeof( state->bam_identity_pubkey ) );

  uchar signature[ 64 ];
  test_bam_sign_auth_challenge( signature, public_key, private_key, challenge, (ulong)challenge_len );
  test_bam_publish_keyguard_signature( state->keyguard_client, response_mcache, signature );

  uchar pb_buf[ 128 ];
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_AuthChallengeResponse_fields, &resp ) );

  state->bam_auth_inflight = 1U;

  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge );

  FD_TEST( state->bam_auth_inflight == 0U );
  FD_TEST( state->bam_auth_ready == 1U );
  FD_TEST( 0 == strcmp( state->challenge_to_sign, challenge ) );

  char expected_sig[ FD_BASE58_ENCODED_64_SZ ];
  fd_base58_encode_64( signature, NULL, expected_sig );
  FD_TEST( 0 == strcmp( state->bam_auth_signature, expected_sig ) );
  FD_TEST( state->keyguard_client->request_seq == 1UL );

  bam_api_AuthChallengeResponse mismatch_resp = bam_api_AuthChallengeResponse_init_default;
  char const mismatch_challenge[] = "unit-test-challenge-after-switch";
  size_t const mismatch_challenge_len = strlen( mismatch_challenge );
  fd_cstr_ncpy( mismatch_resp.challenge_to_sign, mismatch_challenge, sizeof( mismatch_resp.challenge_to_sign ) );

  uchar other_public_key[ 32 ];
  uchar other_private_key[ 32 ];
  test_bam_make_ed25519_keypair( other_public_key, other_private_key, 97U );
  test_bam_sign_auth_challenge( signature, other_public_key, other_private_key, mismatch_challenge, (ulong)mismatch_challenge_len );
  test_bam_publish_keyguard_signature( state->keyguard_client, response_mcache, signature );

  uchar mismatch_pb_buf[ 128 ];
  pb_ostream_t mismatch_ostream = pb_ostream_from_buffer( mismatch_pb_buf, sizeof(mismatch_pb_buf) );
  FD_TEST( pb_encode( &mismatch_ostream, bam_api_AuthChallengeResponse_fields, &mismatch_resp ) );

  state->bam_auth_inflight = 1U;
  fd_bam_client_grpc_rx_msg( state,
                             mismatch_pb_buf,
                             mismatch_ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge );

  FD_TEST( state->bam_auth_inflight == 0U );
  FD_TEST( state->bam_auth_ready == 0U );
  FD_TEST( state->challenge_to_sign[ 0 ] == '\0' );
  FD_TEST( state->keyguard_client->request_seq == 2UL );

  fd_wksp_free_laddr( fd_dcache_delete( fd_dcache_leave( request_data ) ) );
  fd_wksp_free_laddr( fd_dcache_delete( fd_dcache_leave( response_data ) ) );
  fd_wksp_free_laddr( fd_mcache_delete( fd_mcache_leave( request_mcache ) ) );
  fd_wksp_free_laddr( fd_mcache_delete( fd_mcache_leave( response_mcache ) ) );

  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_auth_proof_publishes_message( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  state->bam_stream            = NULL;
  state->bam_stream_live       = 0U;
  state->bam_stream_connecting = 0U;
  state->bam_config_inflight   = 1U;
  state->bam_auth_ready        = 1U;
  state->bam_auth_inflight     = 0U;
  state->grpc_client->request_stream = NULL;
  *state->grpc_client->request_tx_op = (fd_h2_tx_op_t){0};

  g_clock = (long)5e9;
  test_bam_keepalive_sync( state, g_clock );
  state->bam_last_config_poll_ns = g_clock;

  char const challenge[] = "challenge-123";
  fd_cstr_ncpy( state->challenge_to_sign, challenge, sizeof( state->challenge_to_sign ) );

  char const signature[] = "sig-abcdef";
  fd_cstr_ncpy( state->bam_auth_signature, signature, sizeof( state->bam_auth_signature ) );

  char const validator_key[] = "validator-key-test";
  fd_cstr_ncpy( state->bam_identity_pubkey_b58, validator_key, sizeof( state->bam_identity_pubkey_b58 ) );

  fd_bam_test_client_step_reconnect( state, g_clock );

  FD_TEST( state->bam_stream != NULL );
  FD_TEST( state->bam_stream_connecting == 1U );
  FD_TEST( state->bam_auth_ready == 0U );
  FD_TEST( state->challenge_to_sign[ 0 ] == '\0' );
  FD_TEST( state->bam_stream_live == 0U );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.which_versioned_msg == bam_api_SchedulerMessage_v0_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_auth_proof_tag );
  FD_TEST( 0 == strcmp( decoded.msg.versioned_msg.v0.msg.auth_proof.challenge_to_sign, challenge ) );
  FD_TEST( 0 == strcmp( decoded.msg.versioned_msg.v0.msg.auth_proof.signature, signature ) );
  FD_TEST( 0 == strcmp( decoded.msg.versioned_msg.v0.msg.auth_proof.validator_pubkey, validator_key ) );

  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_stream_starts_without_builder_info( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn_empty( env );
  fd_bam_tile_t * state = env->state;
  test_bam_env_mock_h2_hs( state );

  state->bam_stream            = NULL;
  state->bam_stream_live       = 0U;
  state->bam_stream_connecting = 0U;
  state->bam_config_inflight   = 1U;
  state->bam_auth_ready        = 1U;
  state->bam_auth_inflight     = 0U;
  state->grpc_client->request_stream = NULL;
  *state->grpc_client->request_tx_op = (fd_h2_tx_op_t){0};

  g_clock = (long)5e9;
  test_bam_keepalive_sync( state, g_clock );
  state->bam_last_config_poll_ns = g_clock;

  fd_cstr_ncpy( state->challenge_to_sign, "challenge-123", sizeof( state->challenge_to_sign ) );
  fd_cstr_ncpy( state->bam_auth_signature, "sig-abcdef", sizeof( state->bam_auth_signature ) );
  fd_cstr_ncpy( state->bam_identity_pubkey_b58, "validator-key-test", sizeof( state->bam_identity_pubkey_b58 ) );

  FD_TEST( state->builder_info_valid_until == 0L );
  fd_bam_test_client_step_reconnect( state, g_clock );

  FD_TEST( state->bam_stream != NULL );
  FD_TEST( state->bam_stream_connecting == 1U );
  FD_TEST( state->bam_auth_ready == 0U );

  test_bam_env_destroy( env );
}

static void
test_bam_missing_config_repolls_despite_valid_builder_info( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn_empty( env );
  fd_bam_tile_t * state = env->state;
  test_bam_env_mock_h2_hs( state );
  test_bam_prepare_scheduler_stream( state );

  g_clock = (long)5e9;
  long now = g_clock;
  test_bam_keepalive_sync( state, now );

  state->builder_info_valid_until = now + (long)( 60e9 * 5. );
  state->bam_config_received      = 0U;
  state->bam_config_inflight      = 0U;
  state->bam_last_config_poll_ns  = 0L;

  int busy = fd_bam_test_client_step_reconnect( state, now );

  FD_TEST( busy == 1 );
  FD_TEST( state->bam_config_inflight == 1U );
  FD_TEST( state->bam_last_config_poll_ns == now );
  FD_TEST( state->grpc_client->stream_cnt >= 2UL );
  fd_grpc_h2_stream_t * stream = &state->grpc_client->stream_pool[ state->grpc_client->stream_cnt-1UL ];
  FD_TEST( stream->request_ctx == FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );

  test_bam_env_destroy( env );
}

static void
test_bam_missing_config_uses_short_repoll_throttle( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn_empty( env );
  fd_bam_tile_t * state = env->state;
  test_bam_env_mock_h2_hs( state );

  g_clock = (long)5e9;
  long now = g_clock;
  test_bam_keepalive_sync( state, now );
  state->keepalive->ts_next_tx = now + (long)10e9;

  state->builder_info_valid_until = now + (long)( 60e9 * 5. );
  state->bam_config_received      = 0U;
  state->bam_config_inflight      = 0U;
  state->bam_last_config_poll_ns  = now;
  state->bam_auth_ready           = 0U;
  state->bam_auth_inflight        = 1U;

  int busy = fd_bam_test_client_step_reconnect( state, now + (long)1e9 - 1L );
  FD_TEST( busy == 0 );
  FD_TEST( state->bam_config_inflight == 0U );

  busy = fd_bam_test_client_step_reconnect( state, now + (long)1e9 );

  FD_TEST( busy == 1 );
  FD_TEST( state->bam_config_inflight == 1U );
  FD_TEST( state->bam_last_config_poll_ns == now + (long)1e9 );
  FD_TEST( state->grpc_client->stream_cnt >= 1UL );
  fd_grpc_h2_stream_t * stream = &state->grpc_client->stream_pool[ state->grpc_client->stream_cnt-1UL ];
  FD_TEST( stream->request_ctx == FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );

  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_heartbeat_publishes_message( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  test_bam_prepare_scheduler_stream( state );

  g_clock = (long)7e9;
  long now = g_clock;
  state->bam_last_validator_heartbeat_ns = 0L;
  state->bam_last_config_poll_ns = now;
  test_bam_keepalive_sync( state, now );
  int busy = fd_bam_test_client_step_reconnect( state, now );
  FD_TEST( busy == 1 );
  FD_TEST( state->bam_last_validator_heartbeat_ns == now );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.which_versioned_msg == bam_api_SchedulerMessage_v0_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_heart_beat_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.msg.heart_beat.time_sent_microseconds == (uint64_t)(now/1000L) );
  FD_TEST( state->metrics.outbound_enqueue_outcome_cnt[ FD_METRICS_ENUM_BAM_ENQUEUE_OUTCOME_V_HEARTBEAT_ENQUEUED_IDX ] == 1UL );

  test_bam_env_destroy( env );
}

static void
test_bam_deliver_scheduler_ping( fd_bam_tile_t * state,
                                 uint32_t        ping_id ) {
  uchar protobuf[64];
  bam_api_SchedulerResponse resp = bam_api_SchedulerResponse_init_default;
  resp.which_versioned_msg = bam_api_SchedulerResponse_v0_tag;
  resp.versioned_msg.v0.which_resp = bam_api_SchedulerResponseV0_ping_tag;
  resp.versioned_msg.v0.resp.ping.id = ping_id;

  pb_ostream_t ostream = pb_ostream_from_buffer( protobuf, sizeof(protobuf) );
  FD_TEST( pb_encode( &ostream, bam_api_SchedulerResponse_fields, &resp ) );
  fd_bam_client_grpc_rx_msg( state,
                             protobuf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
}

static void
test_bam_scheduler_ping_publishes_message( fd_wksp_t * wksp ) {
  /* BAM proto Ping is a scheduler-stream latency probe. Firedancer must answer
     with Pong, but this must not touch heartbeat/watchdog or HTTP/2 keepalive
     accounting. */

  {
    test_bam_env_t env[1];
    test_bam_env_create( env, wksp );
    test_bam_env_mock_conn( env );
    fd_bam_tile_t * state = env->state;

    test_bam_prepare_scheduler_stream( state );
    /* Completed streaming sends can leave request_stream attached with no
       pending request body. That must not block the next Pong. */
    state->grpc_client->request_stream = state->bam_stream;
    state->grpc_client->request_tx_op->chunk_sz = 0UL;

    g_clock = (long)9e9;
    long builder_ts = g_clock - (long)1e8;
    state->bam_last_builder_activity_ns = builder_ts;
    state->metrics.builder_heartbeats_decoded_cnt = 0UL;
    state->metrics.keepalive_acks_cnt       = 0UL;
    state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_SCHEDULER_ENVELOPE_DECODE_IDX ] = 0UL;
    state->defer_reset                = 0U;
    ulong ping_samples_before         = test_hist_total_cnt( state->metrics.scheduler_pong_send_nanos );
    ulong latency_samples_before      = test_hist_total_cnt( state->metrics.builder_heartbeat_arrival_delta_nanos );

    uint32_t ping_id = 0x00c0ffeeU;
    test_bam_deliver_scheduler_ping( state, ping_id );

    FD_TEST( state->defer_reset == 0U );
    FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_SCHEDULER_ENVELOPE_DECODE_IDX ] == 0UL );
    FD_TEST( state->metrics.builder_heartbeats_decoded_cnt == 0UL );
    FD_TEST( state->metrics.keepalive_acks_cnt == 0UL );
    FD_TEST( state->bam_last_builder_activity_ns == builder_ts );
    FD_TEST( test_hist_total_cnt( state->metrics.scheduler_pong_send_nanos ) == ping_samples_before + 1UL );
    FD_TEST( test_hist_total_cnt( state->metrics.builder_heartbeat_arrival_delta_nanos ) == latency_samples_before );
    FD_TEST( fd_histf_sum( state->metrics.scheduler_pong_send_nanos ) == 0UL );
    FD_TEST( state->metrics.scheduler_pong_send_outcome_cnt[ FD_METRICS_ENUM_BAM_SCHEDULER_PONG_SEND_OUTCOME_V_ENQUEUED_IDX ] == 1UL );

    test_bam_decoded_message_t decoded;
    test_bam_decode_last_message( state, &decoded );
    FD_TEST( decoded.msg.which_versioned_msg == bam_api_SchedulerMessage_v0_tag );
    FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_pong_tag );
    FD_TEST( decoded.msg.versioned_msg.v0.msg.pong.id == ping_id );

    test_bam_env_destroy( env );
  }

  {
    test_bam_env_t env[1];
    test_bam_env_create( env, wksp );
    test_bam_env_mock_conn( env );
    fd_bam_tile_t * state = env->state;

    test_bam_prepare_scheduler_stream( state );

    g_clock = (long)95e8;
    ulong ping_samples_before = test_hist_total_cnt( state->metrics.scheduler_pong_send_nanos );
    FD_TEST( fd_h2_tx_ping( fd_grpc_client_h2_conn( state->grpc_client ),
                            fd_grpc_client_rbuf_tx( state->grpc_client ) ) );

    test_bam_deliver_scheduler_ping( state, 0xfeedbeefU );

    FD_TEST( test_hist_total_cnt( state->metrics.scheduler_pong_send_nanos ) == ping_samples_before + 1UL );
    FD_TEST( state->metrics.scheduler_pong_send_outcome_cnt[ FD_METRICS_ENUM_BAM_SCHEDULER_PONG_SEND_OUTCOME_V_ENQUEUED_IDX ] == 0UL );
    FD_TEST( state->metrics.scheduler_pong_send_outcome_cnt[ FD_METRICS_ENUM_BAM_SCHEDULER_PONG_SEND_OUTCOME_V_FRAME_TX_BUSY_IDX ] == 1UL );

    test_bam_env_destroy( env );
  }

  {
    test_bam_env_t env[1];
    test_bam_env_create( env, wksp );
    test_bam_env_mock_conn( env );
    fd_bam_tile_t * state = env->state;

    test_bam_prepare_scheduler_stream( state );
    state->grpc_client->request_stream = state->bam_stream;
    state->grpc_client->request_tx_op->chunk_sz = 1UL;

    g_clock = (long)96e8;
    ulong ping_samples_before = test_hist_total_cnt( state->metrics.scheduler_pong_send_nanos );
    test_bam_deliver_scheduler_ping( state, 0x12345678U );

    FD_TEST( test_hist_total_cnt( state->metrics.scheduler_pong_send_nanos ) == ping_samples_before + 1UL );
    FD_TEST( state->metrics.scheduler_pong_send_outcome_cnt[ FD_METRICS_ENUM_BAM_SCHEDULER_PONG_SEND_OUTCOME_V_ENQUEUED_IDX ] == 0UL );
    FD_TEST( state->metrics.scheduler_pong_send_outcome_cnt[ FD_METRICS_ENUM_BAM_SCHEDULER_PONG_SEND_OUTCOME_V_REQUEST_BUSY_IDX ] == 1UL );

    test_bam_env_destroy( env );
  }

  {
    test_bam_env_t env[1];
    test_bam_env_create( env, wksp );
    test_bam_env_mock_conn( env );
    fd_bam_tile_t * state = env->state;

    test_bam_prepare_scheduler_stream( state );
    state->bam_stream->s.state = FD_H2_STREAM_STATE_CLOSED;

    g_clock = (long)97e8;
    ulong ping_samples_before = test_hist_total_cnt( state->metrics.scheduler_pong_send_nanos );
    test_bam_deliver_scheduler_ping( state, 0x87654321U );

    FD_TEST( test_hist_total_cnt( state->metrics.scheduler_pong_send_nanos ) == ping_samples_before + 1UL );
    FD_TEST( state->metrics.scheduler_pong_send_outcome_cnt[ FD_METRICS_ENUM_BAM_SCHEDULER_PONG_SEND_OUTCOME_V_ENQUEUED_IDX ] == 0UL );
    FD_TEST( state->metrics.scheduler_pong_send_outcome_cnt[ FD_METRICS_ENUM_BAM_SCHEDULER_PONG_SEND_OUTCOME_V_SEND_FAIL_IDX ] == 1UL );

    test_bam_env_destroy( env );
  }

  {
    test_bam_env_t env[1];
    test_bam_env_create( env, wksp );
    test_bam_env_mock_conn( env );
    fd_bam_tile_t * state = env->state;

    test_bam_prepare_scheduler_stream( state );

    g_clock = (long)10e9;
    state->bam_last_builder_activity_ns = g_clock - FD_BAM_ACTIVITY_TIMEOUT_NS - (long)1e8;
    test_bam_keepalive_sync( state, g_clock );
    FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY );

    test_bam_deliver_scheduler_ping( state, 7U );

    FD_TEST( state->bam_last_builder_activity_ns == g_clock - FD_BAM_ACTIVITY_TIMEOUT_NS - (long)1e8 );
    FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY );
    FD_TEST( test_hist_total_cnt( state->metrics.scheduler_pong_send_nanos ) == 1UL );
    FD_TEST( test_hist_total_cnt( state->metrics.builder_heartbeat_arrival_delta_nanos ) == 0UL );
    FD_TEST( state->metrics.scheduler_pong_send_outcome_cnt[ FD_METRICS_ENUM_BAM_SCHEDULER_PONG_SEND_OUTCOME_V_ENQUEUED_IDX ] == 1UL );

    int charge_busy = 0;
    fd_bam_client_step( state, &charge_busy );
    FD_TEST( state->tcp_sock == -1 );
    FD_TEST( charge_busy == 1 );

    test_bam_env_destroy( env );
  }

  {
    test_bam_env_t env[1];
    test_bam_env_create( env, wksp );
    test_bam_env_mock_conn( env );
    fd_bam_tile_t * state = env->state;

    g_clock = (long)11e9;
    state->bam_last_builder_activity_ns = g_clock - (long)1e8;

    test_bam_deliver_scheduler_ping( state, 9U );

    FD_TEST( test_hist_total_cnt( state->metrics.scheduler_pong_send_nanos ) == 0UL );
    for( ulong i=0UL; i<FD_METRICS_ENUM_BAM_SCHEDULER_PONG_SEND_OUTCOME_CNT; i++ ) {
      FD_TEST( state->metrics.scheduler_pong_send_outcome_cnt[ i ] == 0UL );
    }
    FD_TEST( state->bam_last_builder_activity_ns == g_clock - (long)1e8 );

    test_bam_env_destroy( env );
  }
}

static void
test_bam_scheduler_leader_state_publishes_message( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  test_bam_prepare_scheduler_stream( state );

  state->bam_leader_pending = 1U;
  state->bam_leader_state = (fd_bam_leader_state_t){
    .slot = 42UL,
    .tick = 7U,
    .slot_cu_budget_remaining = 123U
  };

  g_clock = (long)8e9;
  long now = g_clock;
  state->bam_last_config_poll_ns = now;
  state->bam_last_validator_heartbeat_ns = now;
  test_bam_keepalive_sync( state, now );
  int busy = fd_bam_test_client_step_reconnect( state, now );
  FD_TEST( busy == 1 );
  FD_TEST( state->bam_leader_pending == 0U );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.which_versioned_msg == bam_api_SchedulerMessage_v0_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_leader_state_tag );
  bam_types_LeaderState const * ls = &decoded.msg.versioned_msg.v0.msg.leader_state;
  FD_TEST( ls->slot == 42UL );
  FD_TEST( ls->tick == 7U );
  FD_TEST( ls->slot_cu_budget_remaining == 123U );

  test_bam_env_destroy( env );
}

static void
test_bam_leader_state_supersede_counts_drop( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  state->bam_leader_pending = 1U;
  state->bam_leader_state = (fd_bam_leader_state_t){
    .slot = 42UL,
    .tick = 7U,
    .slot_cu_budget_remaining = 123U
  };

  fd_bam_leader_state_t newer_state = {
    .slot = 42UL,
    .tick = 8U,
    .slot_cu_budget_remaining = 111U
  };

  fd_bam_stage_leader_state( state, &newer_state );

  FD_TEST( state->bam_leader_pending == 1U );
  FD_TEST( state->bam_leader_state.slot == newer_state.slot );
  FD_TEST( state->bam_leader_state.tick == newer_state.tick );
  FD_TEST( state->bam_leader_state.slot_cu_budget_remaining == newer_state.slot_cu_budget_remaining );

  fd_bam_stage_leader_state( state, &newer_state );

  test_bam_env_destroy( env );
}

static void
test_bam_replay_reset_channel_contract( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  union {
    fd_replay_message_t msg;
    uchar               bytes[ sizeof(fd_replay_message_t) ];
  } ingress = {0};
  ingress.msg.reset.next_leader_slot = 600UL;

  state->replay_out_in_idx = 2UL;
  state->replay_in = (fd_bam_in_ctx_t){
    .mem    = (fd_wksp_t *)ingress.bytes,
    .chunk0 = 0UL,
    .wmark  = 0UL
  };
  state->leader_schedule_gate_start_ns = 1234L;

  fd_bam_test_receive_ingress_frag( state, state->replay_out_in_idx, REPLAY_SIG_RESET, 0UL, sizeof(fd_poh_reset_t) );
  FD_TEST( state->next_leader_slot == 600UL );
  FD_TEST( state->leader_schedule_recheck_slot == FD_BAM_LEADER_SCHEDULE_RECHECK_NONE_SLOT );
  FD_TEST( state->leader_schedule_gate_start_ns == 0L );

  ingress.msg.reset.next_leader_slot = 700UL;
  fd_bam_test_receive_ingress_frag( state, state->replay_out_in_idx, REPLAY_SIG_RESET+1UL, 0UL, sizeof(fd_poh_reset_t) );
  FD_TEST( state->next_leader_slot == 600UL );

  fd_bam_test_receive_ingress_frag( state, state->replay_out_in_idx, REPLAY_SIG_RESET, 0UL, sizeof(fd_bam_bundle_result_t) );
  FD_TEST( state->next_leader_slot == 600UL );

  test_bam_env_destroy( env );
}

static void
test_bam_replay_schedule_recheck_slot_policy( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  union {
    fd_replay_message_t msg;
    uchar               bytes[ sizeof(fd_replay_message_t) ];
  } ingress = {0};

  state->replay_out_in_idx = 2UL;
  state->replay_in = (fd_bam_in_ctx_t){
    .mem    = (fd_wksp_t *)ingress.bytes,
    .chunk0 = 0UL,
    .wmark  = 0UL
  };
  state->next_leader_slot = ULONG_MAX;
  state->leader_schedule_recheck_slot = FD_BAM_LEADER_SCHEDULE_RECHECK_NONE_SLOT;
  state->tcp_sock = -1;
  state->tcp_sock_connected = 0U;

  ingress.msg.reset.completed_slot = 100UL;
  ingress.msg.reset.next_leader_slot = ULONG_MAX;
  state->leader_schedule_gate_start_ns = 111L;
  fd_bam_test_receive_ingress_frag( state, state->replay_out_in_idx, REPLAY_SIG_RESET, 0UL, sizeof(fd_poh_reset_t) );
  FD_TEST( state->leader_schedule_recheck_slot == 164UL );
  FD_TEST( state->leader_schedule_gate_start_ns == 0L );
  FD_TEST( fd_bam_tile_waiting_for_leader_slot( state ) );

  ingress.msg.slot_completed = (fd_replay_slot_completed_t){
    .slot            = 150UL,
    .slot_in_epoch   = 398UL,
    .slots_per_epoch = 400UL
  };
  state->leader_schedule_gate_start_ns = 222L;
  fd_bam_test_receive_ingress_frag( state, state->replay_out_in_idx, REPLAY_SIG_SLOT_COMPLETED, 0UL, sizeof(fd_replay_slot_completed_t) );
  FD_TEST( state->leader_schedule_recheck_slot == 151UL );
  FD_TEST( state->leader_schedule_gate_start_ns == 0L );
  FD_TEST( fd_bam_tile_waiting_for_leader_slot( state ) );

  ingress.msg.slot_completed = (fd_replay_slot_completed_t){
    .slot            = 151UL,
    .slot_in_epoch   = 399UL,
    .slots_per_epoch = 400UL
  };
  state->leader_schedule_gate_start_ns = 333L;
  fd_bam_test_receive_ingress_frag( state, state->replay_out_in_idx, REPLAY_SIG_SLOT_COMPLETED, 0UL, sizeof(fd_replay_slot_completed_t) );
  FD_TEST( state->leader_schedule_recheck_slot == FD_BAM_LEADER_SCHEDULE_RECHECK_DUE_SLOT );
  FD_TEST( state->leader_schedule_gate_start_ns == 0L );
  FD_TEST( !fd_bam_tile_waiting_for_leader_slot( state ) );

  ingress.msg.reset.completed_slot = 153UL;
  ingress.msg.reset.next_leader_slot = 220UL;
  state->leader_schedule_gate_start_ns = 444L;
  fd_bam_test_receive_ingress_frag( state, state->replay_out_in_idx, REPLAY_SIG_RESET, 0UL, sizeof(fd_poh_reset_t) );
  FD_TEST( state->next_leader_slot == 220UL );
  FD_TEST( state->leader_schedule_recheck_slot == FD_BAM_LEADER_SCHEDULE_RECHECK_NONE_SLOT );
  FD_TEST( state->leader_schedule_gate_start_ns == 0L );
  FD_TEST( !fd_bam_tile_waiting_for_leader_slot( state ) );

  test_bam_env_destroy( env );
}

static void
test_bam_replay_wait_gate_policy( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  state->replay_out_in_idx = 2UL;
  state->next_leader_slot  = ULONG_MAX;
  state->tcp_sock          = -1;
  state->tcp_sock_connected = 0U;

  FD_TEST( fd_bam_tile_waiting_for_leader_slot( state ) );

  state->leader_schedule_recheck_slot = FD_BAM_LEADER_SCHEDULE_RECHECK_DUE_SLOT;
  FD_TEST( !fd_bam_tile_waiting_for_leader_slot( state ) );
  state->leader_schedule_recheck_slot = 500UL;
  FD_TEST( fd_bam_tile_waiting_for_leader_slot( state ) );

  state->feedback_queue_depth = 1U;
  FD_TEST( !fd_bam_tile_waiting_for_leader_slot( state ) );
  state->feedback_queue_depth = 0U;

  state->bam_leader_pending = 1U;
  FD_TEST( !fd_bam_tile_waiting_for_leader_slot( state ) );
  state->bam_leader_pending = 0U;

  state->next_leader_slot = 500UL;
  FD_TEST( !fd_bam_tile_waiting_for_leader_slot( state ) );

  state->next_leader_slot = ULONG_MAX;
  state->replay_out_in_idx = ULONG_MAX;
  FD_TEST( !fd_bam_tile_waiting_for_leader_slot( state ) );

  test_bam_env_destroy( env );
}

static void
test_bam_replay_wait_gate_cleared_on_reset_or_disable( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  state->replay_out_in_idx = 2UL;
  state->next_leader_slot = 12345UL;
  state->leader_schedule_gate_start_ns = 1234L;
  state->leader_schedule_recheck_slot = FD_BAM_LEADER_SCHEDULE_RECHECK_DUE_SLOT;

  fd_bam_client_reset( state );
  FD_TEST( state->leader_schedule_gate_start_ns == 0L );
  FD_TEST( state->leader_schedule_recheck_slot == FD_BAM_LEADER_SCHEDULE_RECHECK_NONE_SLOT );
  FD_TEST( state->next_leader_slot == 12345UL );

  state->enabled = 0;
  state->next_leader_slot = 67890UL;
  state->leader_schedule_gate_start_ns = 5678L;
  state->leader_schedule_recheck_slot = FD_BAM_LEADER_SCHEDULE_RECHECK_DUE_SLOT;

  int charge_busy = 0;
  fd_bam_client_step( state, &charge_busy );
  FD_TEST( charge_busy == 0 );
  FD_TEST( state->leader_schedule_gate_start_ns == 0L );
  FD_TEST( state->leader_schedule_recheck_slot == FD_BAM_LEADER_SCHEDULE_RECHECK_NONE_SLOT );
  FD_TEST( state->next_leader_slot == 67890UL );

  test_bam_env_destroy( env );
}

static void
test_bam_seed_scheduler_work( fd_bam_tile_t * state,
                              uint            pending_seq,
                              uint            result_seq ) {
  fd_bam_pending_txn_t * pending = bam_pending_txn_push_tail_nocopy( state->pending_txns );
  fd_memset( pending, 0, sizeof(*pending) );
  pending->payload_sz = 1U;
  pending->batch_cnt  = 1U;
  pending->seq_id     = pending_seq;

  fd_bam_bundle_result_t res = test_make_bundle_result( result_seq, (ulong)result_seq + 1000UL, 1U );
  test_enqueue_bundle_result( state, &res );
}

static void
test_bam_identity_switch_invalidates_leader_schedule_state( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  fd_keyswitch_t keyswitch = {0};
  keyswitch.state = FD_KEYSWITCH_STATE_SWITCH_PENDING;
  for( ulong i=0UL; i<32UL; i++ ) keyswitch.bytes[ i ] = (uchar)( 200UL + i );
  state->keyswitch = &keyswitch;

  char expected_b58[ FD_BASE58_ENCODED_32_SZ ];
  FD_TEST( fd_base58_encode_32( keyswitch.bytes, NULL, expected_b58 ) );

  state->next_leader_slot              = 12345UL;
  state->leader_schedule_recheck_slot  = 67890UL;
  state->leader_schedule_gate_start_ns = 5555L;
  state->bam_leader_state = (fd_bam_leader_state_t){
    .slot                         = 42UL,
    .tick                         = 7U,
    .slot_cu_budget_remaining     = 999U,
    .slot_end_ns                  = 8888L,
    .current_slot_has_bam_work    = 1U,
  };
  state->bam_leader_pending = 1U;
  state->backoff_until      = 111L;
  state->defer_reset        = 1U;
  test_bam_seed_scheduler_work( state, 700U, 701U );
  ulong dropped_before = state->metrics.feedback_results_dropped_cnt;

  fd_bam_tile_housekeeping( state );

  FD_TEST( 0==memcmp( state->bam_identity_pubkey, keyswitch.bytes, 32UL ) );
  FD_TEST( 0==strcmp( state->bam_identity_pubkey_b58, expected_b58 ) );
  FD_TEST( fd_keyswitch_state_query( state->keyswitch ) == FD_KEYSWITCH_STATE_COMPLETED );

  FD_TEST( state->tcp_sock == -1 );
  FD_TEST( state->tcp_sock_connected == 0U );
  FD_TEST( state->bam_stream_live == 0U );
  FD_TEST( state->bam_stream_connecting == 0U );
  FD_TEST( state->defer_reset == 0U );

  FD_TEST( state->next_leader_slot == ULONG_MAX );
  FD_TEST( state->leader_schedule_recheck_slot == FD_BAM_LEADER_SCHEDULE_RECHECK_DUE_SLOT );
  FD_TEST( state->leader_schedule_gate_start_ns == 0L );
  FD_TEST( state->bam_leader_state.slot == ULONG_MAX );
  FD_TEST( state->bam_leader_state.tick == 0U );
  FD_TEST( state->bam_leader_state.slot_cu_budget_remaining == 0U );
  FD_TEST( state->bam_leader_state.slot_end_ns == 0L );
  FD_TEST( state->bam_leader_state.current_slot_has_bam_work == 0U );
  FD_TEST( state->bam_leader_pending == 0U );
  FD_TEST( state->backoff_until == 0L );
  FD_TEST( bam_pending_txn_empty( state->pending_txns ) );
  FD_TEST( state->feedback_queue_depth == 0UL );
  FD_TEST( state->bam_results_head == state->bam_results_tail );
  FD_TEST( state->metrics.feedback_results_dropped_cnt == dropped_before+1UL );

  state->keyswitch = NULL;
  test_bam_env_destroy( env );
}

static void
test_bam_replay_schedule_hint_does_not_disconnect( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;
  g_clock = (long)50e9;
  state->replay_out_in_idx = 2UL;
  state->replay_in.mem = wksp;
  state->next_leader_slot = ULONG_MAX;
  fd_cstr_ncpy( state->server_fqdn, "127.0.0.1", sizeof(state->server_fqdn) );
  state->server_fqdn_len = sizeof("127.0.0.1")-1UL;
  state->server_tcp_port = 1U;

  int charge_busy = 0;
  fd_bam_client_step( state, &charge_busy );
  FD_TEST( charge_busy == 0 );
  FD_TEST( state->tcp_sock < 0 );
  FD_TEST( state->leader_schedule_gate_start_ns == g_clock );
  FD_TEST( state->leader_schedule_recheck_slot == FD_BAM_LEADER_SCHEDULE_RECHECK_NONE_SLOT );

  g_clock += FD_BAM_LEADER_SCHEDULE_RECHECK_WALLCLOCK_NS - 1L;
  charge_busy = 0;
  fd_bam_client_step( state, &charge_busy );
  FD_TEST( charge_busy == 0 );
  FD_TEST( state->tcp_sock < 0 );
  FD_TEST( state->leader_schedule_gate_start_ns == (long)50e9 );
  FD_TEST( state->leader_schedule_recheck_slot == FD_BAM_LEADER_SCHEDULE_RECHECK_NONE_SLOT );

  state->backoff_until = g_clock + 2L;
  g_clock++;
  charge_busy = 0;
  fd_bam_client_step( state, &charge_busy );
  FD_TEST( charge_busy == 0 );
  FD_TEST( state->tcp_sock < 0 );
  FD_TEST( state->leader_schedule_gate_start_ns == 0L );
  FD_TEST( state->leader_schedule_recheck_slot == FD_BAM_LEADER_SCHEDULE_RECHECK_DUE_SLOT );

  state->backoff_until = 0L;

  charge_busy = 0;
  fd_bam_client_step( state, &charge_busy );
  FD_TEST( charge_busy == 1 );
  FD_TEST( state->leader_schedule_recheck_slot == FD_BAM_LEADER_SCHEDULE_RECHECK_NONE_SLOT );
  FD_TEST( state->leader_schedule_gate_start_ns == 0L );

  state->leader_schedule_gate_start_ns = 555L;
  state->feedback_queue_depth = 1U;
  charge_busy = 0;
  fd_bam_client_step( state, &charge_busy );
  FD_TEST( state->leader_schedule_gate_start_ns == 0L );
  state->feedback_queue_depth = 0U;

  test_bam_env_mock_conn( env );

  fd_keyswitch_t keyswitch = {0};
  keyswitch.state = FD_KEYSWITCH_STATE_COMPLETED;
  state->keyswitch = &keyswitch;

  fd_metrics_register( (ulong *)fd_metrics_new( metrics_scratch, 0UL ) );

  fd_bam_tile_housekeeping( state );
  fd_bam_test_metrics_write( state );
  charge_busy = 0;
  fd_bam_client_step( state, &charge_busy );
  FD_TEST( state->bam_status_recent == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
  FD_TEST( FD_MGAUGE_GET( BAM, HEALTHY ) == 1UL );
  FD_TEST( FD_MGAUGE_GET( BAM, STREAM_LIVE ) == 1UL );
  FD_TEST( state->tcp_sock >= 0 );
  FD_TEST( state->tcp_sock_connected == 1U );

  state->next_leader_slot     = 100000UL;
  fd_bam_tile_housekeeping( state );
  fd_bam_test_metrics_write( state );
  FD_TEST( state->bam_status_recent == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
  FD_TEST( FD_MGAUGE_GET( BAM, HEALTHY ) == 1UL );
  FD_TEST( state->tcp_sock >= 0 );
  FD_TEST( state->tcp_sock_connected == 1U );

  state->keyswitch = NULL;
  test_bam_env_destroy( env );
}

static void
test_bam_pack_leader_channel_contract( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  union {
    fd_bam_leader_state_t leader;
    uchar                 bytes[ sizeof(fd_bam_leader_state_t) ];
  } ingress = { .leader = {
    .slot = 77UL,
    .tick = 9U,
    .slot_cu_budget_remaining = 456U
  } };

  state->pack_bam_leader_in_idx = 3UL;
  state->pack_leader_in = (fd_bam_in_ctx_t){
    .mem    = (fd_wksp_t *)ingress.bytes,
    .chunk0 = 0UL,
    .wmark  = 0UL
  };

  fd_bam_test_receive_ingress_frag( state, state->pack_bam_leader_in_idx, 0UL, 0UL, sizeof(fd_bam_leader_state_t) );

  FD_TEST( state->bam_leader_pending == 1U );
  FD_TEST( state->bam_leader_state.slot == ingress.leader.slot );
  FD_TEST( state->bam_leader_state.tick == ingress.leader.tick );
  FD_TEST( state->bam_leader_state.slot_cu_budget_remaining == ingress.leader.slot_cu_budget_remaining );
  FD_TEST( state->feedback_queue_depth == 0U );

  fd_bam_test_receive_ingress_frag( state, state->pack_bam_leader_in_idx, 0UL, 0UL, sizeof(fd_bam_bundle_result_t) );

  FD_TEST( state->bam_leader_pending == 1U );
  FD_TEST( state->bam_leader_state.slot == ingress.leader.slot );
  FD_TEST( state->feedback_queue_depth == 0U );

  test_bam_env_destroy( env );
}

static void
test_bam_pack_leader_slot_change_flushes_immediately( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  test_bam_prepare_scheduler_stream( state );
  state->bam_leader_state = (fd_bam_leader_state_t){
    .slot = 76UL,
    .tick = 8U,
    .slot_cu_budget_remaining = 111U
  };

  union {
    fd_bam_leader_state_t leader;
    uchar                 bytes[ sizeof(fd_bam_leader_state_t) ];
  } ingress = { .leader = {
    .slot = 77UL,
    .tick = 9U,
    .slot_cu_budget_remaining = 456U
  } };

  state->pack_bam_leader_in_idx = 3UL;
  state->pack_leader_in = (fd_bam_in_ctx_t){
    .mem    = (fd_wksp_t *)ingress.bytes,
    .chunk0 = 0UL,
    .wmark  = 0UL
  };

  fd_bam_test_receive_ingress_frag( state, state->pack_bam_leader_in_idx, 0UL, 0UL, sizeof(fd_bam_leader_state_t) );

  FD_TEST( state->bam_leader_pending == 0U );
  FD_TEST( state->bam_leader_state.slot == ingress.leader.slot );
  FD_TEST( state->bam_leader_state.tick == ingress.leader.tick );
  FD_TEST( state->bam_leader_state.slot_cu_budget_remaining == ingress.leader.slot_cu_budget_remaining );
  FD_TEST( state->metrics.outbound_enqueue_outcome_cnt[ FD_METRICS_ENUM_BAM_ENQUEUE_OUTCOME_V_LEADER_STATE_ENQUEUED_IDX ] == 1UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.which_versioned_msg == bam_api_SchedulerMessage_v0_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_leader_state_tag );
  bam_types_LeaderState const * ls = &decoded.msg.versioned_msg.v0.msg.leader_state;
  FD_TEST( ls->slot == ingress.leader.slot );
  FD_TEST( ls->tick == ingress.leader.tick );
  FD_TEST( ls->slot_cu_budget_remaining == ingress.leader.slot_cu_budget_remaining );

  test_bam_env_destroy( env );
}

static void
test_bam_pack_result_channel_contract( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  union {
    fd_bam_bundle_result_t result;
    uchar                  bytes[ sizeof(fd_bam_bundle_result_t) ];
  } ingress = { .result = {0} };
  ingress.result = test_make_bundle_result( 905U, 1905UL, 2U );

  state->pack_bam_result_in_idx = 4UL;
  state->pack_result_in = (fd_bam_in_ctx_t){
    .mem    = (fd_wksp_t *)ingress.bytes,
    .chunk0 = 0UL,
    .wmark  = 0UL
  };

  fd_bam_test_receive_ingress_frag( state, state->pack_bam_result_in_idx, 0UL, 0UL, sizeof(fd_bam_bundle_result_t) );

  FD_TEST( state->feedback_queue_depth == 1U );
  FD_TEST( state->bam_results_head == 0U );
  FD_TEST( state->bam_results_tail == 1U );
  FD_TEST( state->bam_results[ 0 ].seq_id == ingress.result.seq_id );
  FD_TEST( state->bam_results[ 0 ].slot == ingress.result.slot );
  FD_TEST( state->bam_results[ 0 ].bundle_txn_cnt == ingress.result.bundle_txn_cnt );
  FD_TEST( state->bam_leader_pending == 0U );

  fd_bam_test_receive_ingress_frag( state, state->pack_bam_result_in_idx, 0UL, 0UL, sizeof(fd_bam_leader_state_t) );

  FD_TEST( state->feedback_queue_depth == 1U );
  FD_TEST( state->bam_results[ 0 ].seq_id == ingress.result.seq_id );
  FD_TEST( state->bam_leader_pending == 0U );

  test_bam_env_destroy( env );
}

static void
test_bam_bank_result_channel_contract( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  union {
    fd_bam_bundle_result_t result;
    uchar                  bytes[ sizeof(fd_bam_bundle_result_t) ];
  } ingress[2] = {0};
  ingress[0].result = test_make_bundle_result( 501U, 1501UL, 1U );
  ingress[1].result = test_make_bundle_result( 502U, 1502UL, 2U );

  state->bank_bam_in_cnt    = 2UL;
  state->bank_bam_in_idx    = 5UL;
  state->bank_in[0] = (fd_bam_in_ctx_t){
    .mem    = (fd_wksp_t *)ingress[0].bytes,
    .chunk0 = 0UL,
    .wmark  = 0UL
  };
  state->bank_in[1] = (fd_bam_in_ctx_t){
    .mem    = (fd_wksp_t *)ingress[1].bytes,
    .chunk0 = 0UL,
    .wmark  = 0UL
  };

  fd_bam_test_receive_ingress_frag( state, state->bank_bam_in_idx,      0UL, 0UL, sizeof(fd_bam_bundle_result_t) );
  fd_bam_test_receive_ingress_frag( state, state->bank_bam_in_idx+1UL, 0UL, 0UL, sizeof(fd_bam_bundle_result_t) );

  FD_TEST( state->feedback_queue_depth == 2U );
  FD_TEST( state->bam_results_head == 0U );
  FD_TEST( state->bam_results_tail == 2U );
  FD_TEST( state->bam_results[0].seq_id == ingress[0].result.seq_id );
  FD_TEST( state->bam_results[0].slot == ingress[0].result.slot );
  FD_TEST( state->bam_results[1].seq_id == ingress[1].result.seq_id );
  FD_TEST( state->bam_results[1].slot == ingress[1].result.slot );

  fd_bam_test_receive_ingress_frag( state, state->bank_bam_in_idx+2UL, 0UL, 0UL, sizeof(fd_bam_bundle_result_t) );
  fd_bam_test_receive_ingress_frag( state, state->bank_bam_in_idx+1UL, 0UL, 0UL, sizeof(fd_bam_leader_state_t) );

  FD_TEST( state->feedback_queue_depth == 2U );
  FD_TEST( state->bam_results_tail == 2U );

  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_result_publishes_message( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  test_bam_prepare_scheduler_stream( state );

  g_clock = (long)9e9;
  test_bam_keepalive_sync( state, g_clock );

  fd_bam_bundle_result_t res = test_make_bundle_result( 900, 1900, 2 );
  test_enqueue_bundle_result( state, &res );

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->feedback_queue_depth == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.which_versioned_msg == bam_api_SchedulerMessage_v0_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  FD_TEST( decoded.multi.results[0].seq_id == 900U );
  FD_TEST( decoded.multi.results[0].which_result == bam_types_AtomicTxnBatchResult_committed_tag );
  FD_TEST( decoded.multi.committed[0].txn_cnt == res.bundle_txn_cnt );
  FD_TEST( decoded.multi.committed[0].txns[0].cus_consumed == res.consumed_cus[0] );
  FD_TEST( decoded.multi.committed[0].txns[0].feepayer_balance_lamports == res.feepayer_balance_lamports[0] );
  FD_TEST( decoded.multi.committed[0].txns[0].loaded_accounts_data_size == res.loaded_accounts_data_size[0] );

  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_result_committed_with_execution_error_publishes_message( fd_wksp_t * wksp ) {
  /* Committed can still report execution_success=false (fees-only / instruction error). */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  test_bam_prepare_scheduler_stream( state );

  g_clock = (long)9e9;
  test_bam_keepalive_sync( state, g_clock );

  fd_bam_bundle_result_t res = test_make_bundle_result( 908, 1908, 1 );
  res.execution_success     = 1; /* committed */
  fd_bam_result_add_txn_error( &res, 0UL, bam_types_TransactionErrorReason_INSTRUCTION_ERROR );
  test_enqueue_bundle_result( state, &res );

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->feedback_queue_depth == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  FD_TEST( decoded.multi.results[0].seq_id == 908U );
  FD_TEST( decoded.multi.results[0].which_result == bam_types_AtomicTxnBatchResult_committed_tag );
  FD_TEST( decoded.multi.committed[0].txn_cnt == 1UL );
  FD_TEST( decoded.multi.committed[0].txns[0].execution_success == false );

  res = test_make_bundle_result( 914, 1914, 3 );
  res.execution_success = 1; /* committed */
  fd_bam_result_add_txn_error( &res, 1UL, bam_types_TransactionErrorReason_INSTRUCTION_ERROR );
  test_enqueue_bundle_result( state, &res );

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->feedback_queue_depth == 0UL );

  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  FD_TEST( decoded.multi.results[0].seq_id == 914U );
  FD_TEST( decoded.multi.results[0].which_result == bam_types_AtomicTxnBatchResult_committed_tag );
  FD_TEST( decoded.multi.committed[0].txn_cnt == 3UL );
  FD_TEST( decoded.multi.committed[0].txns[0].execution_success == true  );
  FD_TEST( decoded.multi.committed[0].txns[1].execution_success == false );
  FD_TEST( decoded.multi.committed[0].txns[2].execution_success == true  );

  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_result_not_committed_publishes_message( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  test_bam_prepare_scheduler_stream( state );

  g_clock = (long)10e9;
  test_bam_keepalive_sync( state, g_clock );
  state->bam_last_config_poll_ns = g_clock;

  fd_bam_bundle_result_t res = test_make_bundle_result( 901, 1900, 2 );
  res.execution_success = 0;
  res.scheduling_error  = FD_BAM_SCHED_ERR_OUTSIDE_SLOT;
  test_enqueue_bundle_result( state, &res );

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->feedback_queue_depth == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.which_versioned_msg == bam_api_SchedulerMessage_v0_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_scheduling_error_tag );
  FD_TEST( result->result.not_committed.reason.scheduling_error == bam_types_SchedulingError_OUTSIDE_LEADER_SLOT );

  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_result_not_committed_deser_reason( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  test_bam_prepare_scheduler_stream( state );

  g_clock = (long)11e9;
  test_bam_keepalive_sync( state, g_clock );

  fd_bam_bundle_result_t res = test_make_bundle_result( 902, 1902, 2 );
  res.execution_success   = 0;
  res.sanitize_success[1] = 0;
  test_enqueue_bundle_result( state, &res );

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->feedback_queue_depth == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_deserialization_error_tag );
  FD_TEST( result->result.not_committed.reason.deserialization_error.index == 1U );
  FD_TEST( result->result.not_committed.reason.deserialization_error.reason == bam_types_DeserializationErrorReason_SANITIZE_ERROR );

  res = test_make_bundle_result( 913, 1913, 3 );
  res.execution_success      = 0;
  res.bundle_err             = FD_BAM_BUNDLE_ERR_DESER;
  res.deser_index            = 2U;
  res.deser_reason           = bam_types_DeserializationErrorReason_SANITIZE_ERROR;
  res.transaction_err_count  = 0U;
  test_enqueue_bundle_result( state, &res );

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->feedback_queue_depth == 0UL );

  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  result = &decoded.multi.results[0];
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_deserialization_error_tag );
  FD_TEST( result->result.not_committed.reason.deserialization_error.index == 2U );
  FD_TEST( result->result.not_committed.reason.deserialization_error.reason == bam_types_DeserializationErrorReason_SANITIZE_ERROR );

  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_result_not_committed_transaction_error_reason( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  test_bam_prepare_scheduler_stream( state );

  g_clock = (long)12e9;
  test_bam_keepalive_sync( state, g_clock );

  fd_bam_bundle_result_t res = test_make_bundle_result( 903, 1903, 2 );
  res.execution_success  = 0;
  fd_bam_result_add_txn_error( &res, 0UL, bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND );
  test_enqueue_bundle_result( state, &res );

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->feedback_queue_depth == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_transaction_error_tag );
  FD_TEST( result->result.not_committed.reason.transaction_error.index == 0U );
  FD_TEST( result->result.not_committed.reason.transaction_error.reason == bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND );

  res = test_make_bundle_result( 915, 1915, 3 );
  fd_bam_result_mark_sanitize_success_all( &res );
  fd_bam_result_mark_not_committed_txn_error( &res, 1UL, bam_types_TransactionErrorReason_ACCOUNT_IN_USE );
  test_enqueue_bundle_result( state, &res );

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->feedback_queue_depth == 0UL );

  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  result = &decoded.multi.results[0];
  FD_TEST( result->seq_id == 915U );
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_transaction_error_tag );
  FD_TEST( result->result.not_committed.reason.transaction_error.index == 1U );
  FD_TEST( result->result.not_committed.reason.transaction_error.reason == bam_types_TransactionErrorReason_ACCOUNT_IN_USE );

  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_result_not_committed_transaction_error_prefers_non_cancelled( fd_wksp_t * wksp ) {
  /* For atomicity cascades, prefer the non-CommitCancelled reason/index. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  test_bam_prepare_scheduler_stream( state );

  g_clock = (long)12e9;
  test_bam_keepalive_sync( state, g_clock );

  fd_bam_bundle_result_t res = test_make_bundle_result( 909, 1909, 3 );
  res.execution_success = 0;
  for( uint i=0U; i<res.bundle_txn_cnt; i++ ) res.sanitize_success[ i ] = 1;
  fd_bam_result_add_txn_error( &res, 0UL, bam_types_TransactionErrorReason_COMMIT_CANCELLED );
  fd_bam_result_add_txn_error( &res, 1UL, bam_types_TransactionErrorReason_COMMIT_CANCELLED );
  fd_bam_result_add_txn_error( &res, 2UL, bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND );
  test_enqueue_bundle_result( state, &res );

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->feedback_queue_depth == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_transaction_error_tag );
  FD_TEST( result->result.not_committed.reason.transaction_error.index == 2U );
  FD_TEST( result->result.not_committed.reason.transaction_error.reason == bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND );

  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_result_not_committed_all_cancelled_falls_back_to_poh_timeout( fd_wksp_t * wksp ) {
  /* Atomic bundles with only CommitCancelled reasons should fall back to
     SchedulingError::POH_TIMEOUT when no primary transaction reason exists. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  test_bam_prepare_scheduler_stream( state );

  g_clock = (long)12e9;
  test_bam_keepalive_sync( state, g_clock );

  fd_bam_bundle_result_t res = test_make_bundle_result( 910, 1910, 3 );
  res.execution_success = 0;
  for( uint i=0U; i<res.bundle_txn_cnt; i++ ) {
    res.sanitize_success[ i ] = 1U;
    fd_bam_result_add_txn_error( &res, i, bam_types_TransactionErrorReason_COMMIT_CANCELLED );
  }
  test_enqueue_bundle_result( state, &res );

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->feedback_queue_depth == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_scheduling_error_tag );
  FD_TEST( result->result.not_committed.reason.scheduling_error == bam_types_SchedulingError_POH_TIMEOUT );

  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_result_not_committed_generic_failure_reason( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  test_bam_prepare_scheduler_stream( state );

  g_clock = (long)13e9;
  test_bam_keepalive_sync( state, g_clock );

  /* Generic execution failure yields generic_invalid. */
  fd_bam_bundle_result_t generic = test_make_bundle_result( 904, 1904, 2 );
  generic.execution_success = 0;
  test_enqueue_bundle_result( state, &generic );

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  FD_TEST( state->feedback_queue_depth == 0UL );

  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  bam_types_AtomicTxnBatchResult const * result = &decoded.multi.results[0];
  FD_TEST( result->which_result == bam_types_AtomicTxnBatchResult_not_committed_tag );
  FD_TEST( result->result.not_committed.which_reason == bam_types_NotCommitted_generic_invalid_tag );
  FD_TEST( 0 == strcmp( result->result.not_committed.reason.generic_invalid.message,
                      FD_BAM_ERR_MSG_BUNDLE_EXECUTION_FAILED ) );

  test_bam_env_destroy( env );
}

/* --- Control surface --------------------------------------------------------------- */

static void
setup_ctrl_defaults( fd_bam_tile_t * ctx,
                     fd_bam_ctrl_t * ctrl ) {
  ctx->ctrl = ctrl;
  fd_memset( ctrl, 0, sizeof(fd_bam_ctrl_t) );

  /* Mirror the on-startup state: control block idle, HTTP endpoint configured, BAM enabled. */
  ctx->enabled = 1;
  const char host[] = "testnet.bam.jito.wtf";
  ulong host_len = strlen( host );
  FD_TEST( host_len < sizeof( ctx->server_fqdn ) );
  strcpy( ctx->server_fqdn, host );
  ctx->server_fqdn_len = (ushort)host_len;
  strcpy( ctx->server_sni, host );
  ctx->server_sni_len = (ushort)host_len;
  ctx->server_tcp_port = 50055;
  ctx->is_ssl          = 0;
  ctrl->enable         = 1U;
  fd_cstr_ncpy( ctrl->url, "http://testnet.bam.jito.wtf:50055", sizeof( ctrl->url ) );
  fd_cstr_ncpy( ctrl->sni, host, sizeof( ctrl->sni ) );
  ctrl->state = FD_BAM_CTRL_STATE_IDLE;
}

static void
setup_ctrl_dormant( fd_bam_tile_t * ctx,
                    fd_bam_ctrl_t * ctrl ) {
  ctx->ctrl = ctrl;
  fd_memset( ctrl, 0, sizeof(fd_bam_ctrl_t) );

  ctx->enabled = 0;
  ctx->server_fqdn[0]   = '\0';
  ctx->server_fqdn_len  = 0U;
  ctx->server_sni[0]    = '\0';
  ctx->server_sni_len   = 0U;
  ctx->server_tcp_port  = 0U;
  ctx->is_ssl           = 0U;
  ctrl->enable          = 0U;
  ctrl->url[0]          = '\0';
  ctrl->sni[0]          = '\0';
  ctrl->state           = FD_BAM_CTRL_STATE_IDLE;
}

FD_FN_UNUSED static void
test_bam_ctrl_updates_url_and_sni( fd_wksp_t * wksp ) {
  /* Ensure runtime URL/SNI updates reconfigure the client and publish success to the control block. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * ctx = env->state;

  fd_bam_ctrl_t ctrl;
  setup_ctrl_defaults( ctx, &ctrl );

  static fd_keyswitch_t keyswitch = {0};
  keyswitch.magic = FD_KEYSWITCH_MAGIC;
  keyswitch.state = FD_KEYSWITCH_STATE_COMPLETED;
  keyswitch.param = 0UL;
  ctx->keyswitch = &keyswitch;
  test_bam_seed_scheduler_work( ctx, 710U, 711U );
  ulong dropped_before = ctx->metrics.feedback_results_dropped_cnt;

  ctrl.command = FD_BAM_CTRL_CMD_URL | FD_BAM_CTRL_CMD_SNI;
  ctrl.enable  = 1U;
  fd_cstr_ncpy( ctrl.url, "http://new.example.com:8899", sizeof( ctrl.url ) );
  fd_cstr_ncpy( ctrl.sni, "custom.sni.invalid", sizeof( ctrl.sni ) );
  ctrl.state = FD_BAM_CTRL_STATE_REQUEST;

  fd_bam_tile_housekeeping( ctx );

  FD_TEST( ctrl.state == FD_BAM_CTRL_STATE_SUCCESS );
  FD_TEST( ctrl.enable == 1U );
  FD_TEST( !strcmp( ctrl.url, "http://new.example.com:8899" ) );
  FD_TEST( !strcmp( ctrl.sni, "custom.sni.invalid" ) );
  FD_TEST( ctx->enabled == 1 );
  FD_TEST( ctx->server_tcp_port == 8899 );
  FD_TEST( !strcmp( ctx->server_fqdn, "new.example.com" ) );
  FD_TEST( ctx->server_fqdn_len == strlen( "new.example.com" ) );
  FD_TEST( ctx->is_ssl == 0 );
  FD_TEST( !strcmp( ctx->server_sni, "custom.sni.invalid" ) );
  FD_TEST( ctx->server_sni_len == strlen( "custom.sni.invalid" ) );
  FD_TEST( !strcmp( ctx->ctrl->error, "" ) );
  FD_TEST( !strcmp( ctx->grpc_client->host, "custom.sni.invalid" ) );
  FD_TEST( ctx->grpc_client->port == 8899 );
  FD_TEST( bam_pending_txn_empty( ctx->pending_txns ) );
  FD_TEST( ctx->feedback_queue_depth == 0UL );
  FD_TEST( ctx->bam_results_head == ctx->bam_results_tail );
  FD_TEST( ctx->metrics.feedback_results_dropped_cnt == dropped_before+1UL );

  ctx->keyswitch = NULL;
  test_bam_env_destroy( env );
}

FD_FN_UNUSED static void
test_bam_ctrl_defaults_bam_ports( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * ctx = env->state;

  fd_bam_ctrl_t ctrl;
  setup_ctrl_defaults( ctx, &ctrl );

  static fd_keyswitch_t keyswitch = {0};
  keyswitch.magic = FD_KEYSWITCH_MAGIC;
  keyswitch.state = FD_KEYSWITCH_STATE_COMPLETED;
  keyswitch.param = 0UL;
  ctx->keyswitch = &keyswitch;

  ctrl.command = FD_BAM_CTRL_CMD_URL;
  fd_cstr_ncpy( ctrl.url, "http://bam.example.com", sizeof( ctrl.url ) );
  ctrl.state = FD_BAM_CTRL_STATE_REQUEST;

  fd_bam_tile_housekeeping( ctx );

  FD_TEST( ctrl.state == FD_BAM_CTRL_STATE_SUCCESS );
  FD_TEST( !strcmp( ctrl.url, "http://bam.example.com:50055" ) );
  FD_TEST( !strcmp( ctrl.sni, "bam.example.com" ) );
  FD_TEST( !strcmp( ctx->server_fqdn, "bam.example.com" ) );
  FD_TEST( ctx->server_tcp_port == 50055U );
  FD_TEST( ctx->is_ssl == 0U );
  FD_TEST( !strcmp( ctx->grpc_client->host, "bam.example.com" ) );
  FD_TEST( ctx->grpc_client->port == 50055U );

#if FD_HAS_OPENSSL
  ctrl.command = FD_BAM_CTRL_CMD_URL;
  fd_cstr_ncpy( ctrl.url, "https://bam.example.com", sizeof( ctrl.url ) );
  ctrl.state = FD_BAM_CTRL_STATE_REQUEST;

  fd_bam_tile_housekeeping( ctx );

  FD_TEST( ctrl.state == FD_BAM_CTRL_STATE_SUCCESS );
  FD_TEST( !strcmp( ctrl.url, "https://bam.example.com:50056" ) );
  FD_TEST( !strcmp( ctrl.sni, "bam.example.com" ) );
  FD_TEST( !strcmp( ctx->server_fqdn, "bam.example.com" ) );
  FD_TEST( ctx->server_tcp_port == 50056U );
  FD_TEST( ctx->is_ssl == 1U );
  FD_TEST( !strcmp( ctx->grpc_client->host, "bam.example.com" ) );
  FD_TEST( ctx->grpc_client->port == 50056U );

  ctrl.command = FD_BAM_CTRL_CMD_URL;
  fd_cstr_ncpy( ctrl.url, "https://bam.example.com:9443", sizeof( ctrl.url ) );
  ctrl.state = FD_BAM_CTRL_STATE_REQUEST;

  fd_bam_tile_housekeeping( ctx );

  FD_TEST( ctrl.state == FD_BAM_CTRL_STATE_SUCCESS );
  FD_TEST( !strcmp( ctrl.url, "https://bam.example.com:9443" ) );
  FD_TEST( ctx->server_tcp_port == 9443U );
  FD_TEST( ctx->is_ssl == 1U );
  FD_TEST( ctx->grpc_client->port == 9443U );
#endif

  ctx->keyswitch = NULL;
  test_bam_env_destroy( env );
}

FD_FN_UNUSED static void
test_bam_ctrl_toggle_enable_updates_runtime_state( fd_wksp_t * wksp ) {
  /* Validate that toggling enable pauses connectivity and clears the status latch. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * ctx = env->state;

  fd_bam_ctrl_t ctrl;
  setup_ctrl_defaults( ctx, &ctrl );

  static fd_keyswitch_t keyswitch = {0};
  keyswitch.magic = FD_KEYSWITCH_MAGIC;
  keyswitch.state = FD_KEYSWITCH_STATE_COMPLETED;
  keyswitch.param = 0UL;
  ctx->keyswitch = &keyswitch;
  test_bam_seed_scheduler_work( ctx, 720U, 721U );
  ulong dropped_before = ctx->metrics.feedback_results_dropped_cnt;

  uchar fseq_mem[ FD_FSEQ_FOOTPRINT ] __attribute__((aligned(FD_FSEQ_ALIGN)));
  fd_memset( fseq_mem, 0, sizeof(fseq_mem) );
  void * fseq_shmem = fd_fseq_new( fseq_mem, 0UL );
  FD_TEST( fseq_shmem );
  ulong * fseq = fd_fseq_join( fseq_shmem );
  FD_TEST( fseq );
  ctx->bam_status_fseq = fseq;
  fd_bam_publish_active_state( ctx, ctx->stem, 1 );
  FD_TEST( fd_fseq_query( fseq ) == FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );

  fd_cstr_ncpy( ctx->admin_rpc_path, "/tmp/test-bam-admin.rpc", sizeof( ctx->admin_rpc_path ) );
  FD_TEST( fd_cstr_to_ip4_addr( "1.1.1.1", &ctx->default_tpu.addr ) );
  FD_TEST( fd_cstr_to_ip4_addr( "2.2.2.2", &ctx->default_tpu_fwd.addr ) );
  ctx->default_tpu.port     = fd_ushort_bswap( 4242U );
  ctx->default_tpu_fwd.port = fd_ushort_bswap( 4343U );
  ctx->tpu_update_state = FD_BAM_TPU_UPDATE_STATE_APPLIED_BAM;

  test_bam_admin_rpc_mock_reset();
  test_bam_admin_rpc_expect_fseq_zero = fseq;
  test_bam_admin_rpc_mock_push_reply( 0, "{\"jsonrpc\":\"2.0\",\"result\":{\"tpu\":\"1.1.1.1:4242\",\"tpu_forwards\":\"2.2.2.2:4343\"},\"id\":1}" );
  test_bam_admin_rpc_mock_push_reply( 0, "{\"jsonrpc\":\"2.0\",\"result\":null,\"id\":2}" );

  ctrl.command = FD_BAM_CTRL_CMD_ENABLE;
  ctrl.enable  = 0U;
  ctrl.state   = FD_BAM_CTRL_STATE_REQUEST;

  fd_bam_tile_housekeeping( ctx );

  FD_TEST( ctrl.state == FD_BAM_CTRL_STATE_SUCCESS );
  FD_TEST( ctrl.enable == 0U );
  FD_TEST( ctx->enabled == 0 );
  FD_TEST( fd_fseq_query( fseq ) == 0UL );
  FD_TEST( test_bam_admin_rpc_mock.request_cnt == 2UL );
  FD_TEST( strstr( test_bam_admin_rpc_mock.requests[1], "\"method\":\"setContactInfoClientId\"" ) );
  FD_TEST( strstr( test_bam_admin_rpc_mock.requests[1], "[2]" ) );
  FD_TEST( !strcmp( ctrl.url, "http://testnet.bam.jito.wtf:50055" ) );
  FD_TEST( bam_pending_txn_empty( ctx->pending_txns ) );
  FD_TEST( ctx->feedback_queue_depth == 0UL );
  FD_TEST( ctx->bam_results_head == ctx->bam_results_tail );
  FD_TEST( ctx->metrics.feedback_results_dropped_cnt == dropped_before+1UL );

  ctrl.command = FD_BAM_CTRL_CMD_ENABLE;
  ctrl.enable  = 1U;
  ctrl.state   = FD_BAM_CTRL_STATE_REQUEST;

  fd_bam_tile_housekeeping( ctx );

  FD_TEST( ctrl.state == FD_BAM_CTRL_STATE_SUCCESS );
  FD_TEST( ctrl.enable == 1U );
  FD_TEST( ctx->enabled == 1U );
  FD_TEST( fd_fseq_query( fseq ) == 0UL );
  FD_TEST( !strcmp( ctrl.url, "http://testnet.bam.jito.wtf:50055" ) );
  FD_TEST( !strcmp( ctx->server_fqdn, "testnet.bam.jito.wtf" ) );
  FD_TEST( ctx->server_tcp_port == 50055U );

  test_bam_admin_rpc_expect_fseq_zero = NULL;
  FD_TEST( fd_fseq_leave( fseq ) == fseq_shmem );
  FD_TEST( fd_fseq_delete( fseq_shmem ) == fseq_shmem );
  ctx->bam_status_fseq = NULL;
  ctx->keyswitch = NULL;

  test_bam_env_destroy( env );
}

FD_FN_UNUSED static void
test_bam_ctrl_enable_from_disabled_start( fd_wksp_t * wksp ) {
  /* Ensure a tile launched with BAM disabled can be enabled via runtime control. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * ctx = env->state;

  fd_bam_ctrl_t ctrl;
  setup_ctrl_defaults( ctx, &ctrl );

  ctx->enabled = 0;
  ctrl.enable  = 0U;

  static fd_keyswitch_t keyswitch = {0};
  keyswitch.magic = FD_KEYSWITCH_MAGIC;
  keyswitch.state = FD_KEYSWITCH_STATE_COMPLETED;
  keyswitch.param = 0UL;
  ctx->keyswitch = &keyswitch;

  ctrl.command = FD_BAM_CTRL_CMD_ENABLE;
  ctrl.enable  = 1U;
  ctrl.state   = FD_BAM_CTRL_STATE_REQUEST;

  fd_bam_tile_housekeeping( ctx );

  FD_TEST( ctrl.state == FD_BAM_CTRL_STATE_SUCCESS );
  FD_TEST( ctrl.enable == 1U );
  FD_TEST( ctx->enabled == 1 );
  FD_TEST( !strcmp( ctrl.url, "http://testnet.bam.jito.wtf:50055" ) );
  FD_TEST( !strcmp( ctx->ctrl->error, "" ) );

  ctx->keyswitch = NULL;

  test_bam_env_destroy( env );
}

FD_FN_UNUSED static void
test_bam_ctrl_enable_from_dormant_then_set_url( fd_wksp_t * wksp ) {
  /* Dormant BAM has allocated control state but no effective endpoint, so
     --enable alone should set the desired runtime mode without dialing until a
     URL arrives. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * ctx = env->state;

  fd_bam_ctrl_t ctrl;
  setup_ctrl_dormant( ctx, &ctrl );

  static fd_keyswitch_t keyswitch = {0};
  keyswitch.magic = FD_KEYSWITCH_MAGIC;
  keyswitch.state = FD_KEYSWITCH_STATE_COMPLETED;
  keyswitch.param = 0UL;
  ctx->keyswitch = &keyswitch;

  ctrl.command = FD_BAM_CTRL_CMD_ENABLE;
  ctrl.enable  = 1U;
  ctrl.state   = FD_BAM_CTRL_STATE_REQUEST;

  fd_bam_tile_housekeeping( ctx );

  FD_TEST( ctrl.state == FD_BAM_CTRL_STATE_SUCCESS );
  FD_TEST( ctrl.enable == 1U );
  FD_TEST( !strcmp( ctrl.url, "" ) );
  FD_TEST( !strcmp( ctrl.sni, "" ) );
  FD_TEST( ctx->enabled == 1U );
  FD_TEST( !strcmp( ctx->server_fqdn, "" ) );
  FD_TEST( ctx->server_tcp_port == 0U );
  FD_TEST( !strcmp( ctrl.error, "" ) );

  int charge_busy = 0;
  fd_bam_client_step( ctx, &charge_busy );
  FD_TEST( !charge_busy );
  FD_TEST( ctx->tcp_sock < 0 );

  ctrl.command = FD_BAM_CTRL_CMD_URL;
  fd_cstr_ncpy( ctrl.url, "http://bam.example.com:8899", sizeof( ctrl.url ) );
  ctrl.state   = FD_BAM_CTRL_STATE_REQUEST;

  fd_bam_tile_housekeeping( ctx );

  FD_TEST( ctrl.state == FD_BAM_CTRL_STATE_SUCCESS );
  FD_TEST( ctrl.enable == 1U );
  FD_TEST( !strcmp( ctrl.url, "http://bam.example.com:8899" ) );
  FD_TEST( !strcmp( ctrl.sni, "bam.example.com" ) );
  FD_TEST( ctx->enabled == 1U );
  FD_TEST( !strcmp( ctx->server_fqdn, "bam.example.com" ) );
  FD_TEST( ctx->server_fqdn_len == strlen( "bam.example.com" ) );
  FD_TEST( ctx->server_tcp_port == 8899U );
  FD_TEST( !strcmp( ctx->server_sni, "bam.example.com" ) );
  FD_TEST( ctx->server_sni_len == strlen( "bam.example.com" ) );
  FD_TEST( ctx->is_ssl == 0U );
  FD_TEST( !strcmp( ctx->grpc_client->host, "bam.example.com" ) );
  FD_TEST( ctx->grpc_client->port == 8899U );
  FD_TEST( !strcmp( ctrl.error, "" ) );

  ctx->keyswitch = NULL;
  test_bam_env_destroy( env );
}

FD_FN_UNUSED static void
test_bam_ctrl_invalid_url_sets_error_and_preserves_config( fd_wksp_t * wksp ) {
  /* Invalid runtime URLs should surface an error without mutating existing configuration. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * ctx = env->state;

  fd_bam_ctrl_t ctrl;
  setup_ctrl_defaults( ctx, &ctrl );

  static fd_keyswitch_t keyswitch = {0};
  keyswitch.magic = FD_KEYSWITCH_MAGIC;
  keyswitch.state = FD_KEYSWITCH_STATE_COMPLETED;
  keyswitch.param = 0UL;
  ctx->keyswitch = &keyswitch;

  ctrl.command = FD_BAM_CTRL_CMD_URL;
  ctrl.enable  = 1U;
  fd_cstr_ncpy( ctrl.url, "not a url", sizeof( ctrl.url ) );
  ctrl.state = FD_BAM_CTRL_STATE_REQUEST;

  fd_bam_tile_housekeeping( ctx );

  FD_TEST( ctrl.state == FD_BAM_CTRL_STATE_ERROR );
  FD_TEST( strstr( ctrl.error, "Invalid BAM URL" ) != NULL );
  FD_TEST( !strcmp( ctrl.url, "http://testnet.bam.jito.wtf:50055" ) );
  FD_TEST( !strcmp( ctx->server_fqdn, "testnet.bam.jito.wtf" ) );
  FD_TEST( ctx->enabled == 1 );

  ctx->keyswitch = NULL;
  test_bam_env_destroy( env );
}

FD_FN_UNUSED static void
test_bam_ctrl_blank_url_clears_and_disables( fd_wksp_t * wksp ) {
  /* Runtime setBamUrl("") semantics: blank/whitespace clears the URL and
     forces BAM off instead of being treated as a parse error. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * ctx = env->state;

  fd_bam_ctrl_t ctrl;
  setup_ctrl_defaults( ctx, &ctrl );

  static fd_keyswitch_t keyswitch = {0};
  keyswitch.magic = FD_KEYSWITCH_MAGIC;
  keyswitch.state = FD_KEYSWITCH_STATE_COMPLETED;
  keyswitch.param = 0UL;
  ctx->keyswitch = &keyswitch;

  ctrl.command = FD_BAM_CTRL_CMD_URL;
  ctrl.enable  = 1U;
  fd_cstr_ncpy( ctrl.url, "   \t\n", sizeof( ctrl.url ) );
  ctrl.state = FD_BAM_CTRL_STATE_REQUEST;

  fd_bam_tile_housekeeping( ctx );

  FD_TEST( ctrl.state == FD_BAM_CTRL_STATE_SUCCESS );
  FD_TEST( ctrl.enable == 0U );
  FD_TEST( !strcmp( ctrl.url, "" ) );
  FD_TEST( !strcmp( ctrl.sni, "" ) );
  FD_TEST( ctx->enabled == 0 );
  FD_TEST( !strcmp( ctx->server_fqdn, "" ) );
  FD_TEST( ctx->server_fqdn_len == 0U );
  FD_TEST( !strcmp( ctx->server_sni, "" ) );
  FD_TEST( ctx->server_sni_len == 0U );
  FD_TEST( ctx->server_tcp_port == 0U );
  FD_TEST( ctx->is_ssl == 0U );
  FD_TEST( !strcmp( ctx->grpc_client->host, "" ) );
  FD_TEST( ctx->grpc_client->port == 0U );
  FD_TEST( !strcmp( ctx->ctrl->error, "" ) );

  ctx->keyswitch = NULL;
  test_bam_env_destroy( env );
}

static void
test_bam_admin_rpc_apply_success_caches_default_and_marks_applied( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  fd_cstr_ncpy( state->admin_rpc_path, "/tmp/test-bam-admin.rpc", sizeof( state->admin_rpc_path ) );
  FD_TEST( fd_cstr_to_ip4_addr( "9.9.9.9", &state->bam_tpu.addr ) );
  FD_TEST( fd_cstr_to_ip4_addr( "8.8.8.8", &state->bam_tpu_fwd.addr ) );
  state->bam_tpu.port     = fd_ushort_bswap( 7000 );
  state->bam_tpu_fwd.port = fd_ushort_bswap( 7001 );

  test_bam_admin_rpc_mock_reset();
  test_bam_admin_rpc_mock_push_reply( 0, "{\"jsonrpc\":\"2.0\",\"result\":{\"tpu\":\"1.1.1.1:4242\",\"tpu_forwards\":\"2.2.2.2:4343\"},\"id\":1}" );
  test_bam_admin_rpc_mock_push_reply( 0, "{\"jsonrpc\":\"2.0\",\"result\":null,\"id\":2}" );
  test_bam_admin_rpc_mock_push_reply( 0, "{\"jsonrpc\":\"2.0\",\"result\":null,\"id\":3}" );
  test_bam_admin_rpc_mock_push_reply( 0, "{\"jsonrpc\":\"2.0\",\"result\":null,\"id\":4}" );

  fd_bam_gossip_update( state, state->stem, 1 );

  FD_TEST( test_bam_admin_rpc_mock.request_cnt == 4UL );
  FD_TEST( !strcmp( test_bam_admin_rpc_mock.paths[0], "/tmp/test-bam-admin.rpc" ) );
  FD_TEST( strstr( test_bam_admin_rpc_mock.requests[0], "\"method\":\"contactInfo\"" ) );
  FD_TEST( strstr( test_bam_admin_rpc_mock.requests[1], "\"method\":\"setPublicTpuAddress\"" ) );
  FD_TEST( strstr( test_bam_admin_rpc_mock.requests[1], "9.9.9.9:7006" ) );
  FD_TEST( strstr( test_bam_admin_rpc_mock.requests[2], "\"method\":\"setPublicTpuForwardsAddress\"" ) );
  FD_TEST( strstr( test_bam_admin_rpc_mock.requests[2], "8.8.8.8:7007" ) );
  FD_TEST( strstr( test_bam_admin_rpc_mock.requests[3], "\"method\":\"setContactInfoClientId\"" ) );
  FD_TEST( strstr( test_bam_admin_rpc_mock.requests[3], "[12]" ) );

  FD_TEST( state->tpu_update_state == FD_BAM_TPU_UPDATE_STATE_APPLIED_BAM );
  FD_TEST( state->client_id_update_state == FD_BAM_CLIENT_ID_UPDATE_STATE_APPLIED_BAM );
  FD_TEST( fd_ushort_bswap( state->default_tpu.port ) == 4242 );
  FD_TEST( fd_ushort_bswap( state->default_tpu_fwd.port ) == 4343 );

  uint expected_default_tpu = 0U;
  uint expected_default_tpu_fwd = 0U;
  FD_TEST( fd_cstr_to_ip4_addr( "1.1.1.1", &expected_default_tpu ) );
  FD_TEST( fd_cstr_to_ip4_addr( "2.2.2.2", &expected_default_tpu_fwd ) );
  FD_TEST( state->default_tpu.addr == expected_default_tpu );
  FD_TEST( state->default_tpu_fwd.addr == expected_default_tpu_fwd );

  test_bam_env_destroy( env );
}

static void
test_bam_admin_rpc_set_failure_stays_pending( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  fd_cstr_ncpy( state->admin_rpc_path, "/tmp/test-bam-admin.rpc", sizeof( state->admin_rpc_path ) );
  FD_TEST( fd_cstr_to_ip4_addr( "9.9.9.9", &state->bam_tpu.addr ) );
  FD_TEST( fd_cstr_to_ip4_addr( "8.8.8.8", &state->bam_tpu_fwd.addr ) );
  state->bam_tpu.port     = fd_ushort_bswap( 7000 );
  state->bam_tpu_fwd.port = fd_ushort_bswap( 7001 );

  test_bam_admin_rpc_mock_reset();
  test_bam_admin_rpc_mock_push_reply( 0, "{\"jsonrpc\":\"2.0\",\"result\":{\"tpu\":\"1.1.1.1:4242\",\"tpu_forwards\":\"2.2.2.2:4343\"},\"id\":1}" );
  test_bam_admin_rpc_mock_push_reply( -1, NULL );

  fd_bam_gossip_update( state, state->stem, 1 );

  FD_TEST( test_bam_admin_rpc_mock.request_cnt == 2UL );
  FD_TEST( strstr( test_bam_admin_rpc_mock.requests[1], "\"method\":\"setPublicTpuAddress\"" ) );
  FD_TEST( state->tpu_update_state == FD_BAM_TPU_UPDATE_STATE_PENDING_BAM );
  FD_TEST( state->client_id_update_state == FD_BAM_CLIENT_ID_UPDATE_STATE_PENDING_BAM );

  test_bam_env_destroy( env );
}

static void
test_bam_admin_rpc_revert_uses_cached_defaults( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  fd_cstr_ncpy( state->admin_rpc_path, "/tmp/test-bam-admin.rpc", sizeof( state->admin_rpc_path ) );
  FD_TEST( fd_cstr_to_ip4_addr( "9.9.9.9", &state->bam_tpu.addr ) );
  FD_TEST( fd_cstr_to_ip4_addr( "8.8.8.8", &state->bam_tpu_fwd.addr ) );
  state->bam_tpu.port     = fd_ushort_bswap( 7000 );
  state->bam_tpu_fwd.port = fd_ushort_bswap( 7001 );
  FD_TEST( fd_cstr_to_ip4_addr( "1.1.1.1", &state->default_tpu.addr ) );
  FD_TEST( fd_cstr_to_ip4_addr( "2.2.2.2", &state->default_tpu_fwd.addr ) );
  state->default_tpu.port     = fd_ushort_bswap( 4242 );
  state->default_tpu_fwd.port = fd_ushort_bswap( 4343 );

  test_bam_admin_rpc_mock_reset();
  test_bam_admin_rpc_mock_push_reply( 0, "{\"jsonrpc\":\"2.0\",\"result\":{\"tpu\":\"9.9.9.9:7000\",\"tpu_forwards\":\"8.8.8.8:7001\"},\"id\":1}" );
  test_bam_admin_rpc_mock_push_reply( 0, "{\"jsonrpc\":\"2.0\",\"result\":null,\"id\":2}" );
  test_bam_admin_rpc_mock_push_reply( 0, "{\"jsonrpc\":\"2.0\",\"result\":null,\"id\":3}" );
  test_bam_admin_rpc_mock_push_reply( 0, "{\"jsonrpc\":\"2.0\",\"result\":null,\"id\":4}" );

  fd_bam_gossip_update( state, state->stem, 0 );

  FD_TEST( test_bam_admin_rpc_mock.request_cnt == 4UL );
  FD_TEST( strstr( test_bam_admin_rpc_mock.requests[1], "\"method\":\"setContactInfoClientId\"" ) );
  FD_TEST( strstr( test_bam_admin_rpc_mock.requests[1], "[2]" ) );
  FD_TEST( strstr( test_bam_admin_rpc_mock.requests[2], "\"method\":\"setPublicTpuAddress\"" ) );
  FD_TEST( strstr( test_bam_admin_rpc_mock.requests[2], "1.1.1.1:4248" ) );
  FD_TEST( strstr( test_bam_admin_rpc_mock.requests[3], "\"method\":\"setPublicTpuForwardsAddress\"" ) );
  FD_TEST( strstr( test_bam_admin_rpc_mock.requests[3], "2.2.2.2:4349" ) );
  FD_TEST( state->tpu_update_state == FD_BAM_TPU_UPDATE_STATE_APPLIED_DEFAULT );
  FD_TEST( state->client_id_update_state == FD_BAM_CLIENT_ID_UPDATE_STATE_APPLIED_DEFAULT );

  test_bam_env_destroy( env );
}

static void
test_bam_admin_rpc_restart_recovers_configured_defaults_for_revert( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  fd_cstr_ncpy( state->admin_rpc_path, "/tmp/test-bam-admin.rpc", sizeof( state->admin_rpc_path ) );
  FD_TEST( fd_cstr_to_ip4_addr( "9.9.9.9", &state->bam_tpu.addr ) );
  FD_TEST( fd_cstr_to_ip4_addr( "8.8.8.8", &state->bam_tpu_fwd.addr ) );
  state->bam_tpu.port     = fd_ushort_bswap( 7000 );
  state->bam_tpu_fwd.port = fd_ushort_bswap( 7001 );

  FD_TEST( fd_cstr_to_ip4_addr( "1.1.1.1", &state->configured_default_tpu.addr ) );
  state->configured_default_tpu.port = fd_ushort_bswap( 4242 );

  test_bam_admin_rpc_mock_reset();
  test_bam_admin_rpc_mock_push_reply( 0, "{\"jsonrpc\":\"2.0\",\"result\":{\"tpu\":\"9.9.9.9:7000\",\"tpu_forwards\":\"8.8.8.8:7001\"},\"id\":1}" );
  test_bam_admin_rpc_mock_push_reply( 0, "{\"jsonrpc\":\"2.0\",\"result\":null,\"id\":2}" );
  test_bam_admin_rpc_mock_push_reply( 0, "{\"jsonrpc\":\"2.0\",\"result\":null,\"id\":3}" );
  test_bam_admin_rpc_mock_push_reply( 0, "{\"jsonrpc\":\"2.0\",\"result\":null,\"id\":4}" );

  fd_bam_gossip_update( state, state->stem, 0 );

  FD_TEST( test_bam_admin_rpc_mock.request_cnt == 4UL );
  FD_TEST( state->default_tpu.l == state->configured_default_tpu.l );
  FD_TEST( state->default_tpu_fwd.l == state->configured_default_tpu.l );
  FD_TEST( strstr( test_bam_admin_rpc_mock.requests[1], "\"method\":\"setContactInfoClientId\"" ) );
  FD_TEST( strstr( test_bam_admin_rpc_mock.requests[1], "[2]" ) );
  FD_TEST( strstr( test_bam_admin_rpc_mock.requests[2], "1.1.1.1:4248" ) );
  FD_TEST( strstr( test_bam_admin_rpc_mock.requests[3], "1.1.1.1:4248" ) );
  FD_TEST( fd_ushort_bswap( state->default_tpu.port ) == 4242 );
  FD_TEST( fd_ushort_bswap( state->default_tpu_fwd.port ) == 4242 );
  FD_TEST( state->tpu_update_state == FD_BAM_TPU_UPDATE_STATE_APPLIED_DEFAULT );
  FD_TEST( state->client_id_update_state == FD_BAM_CLIENT_ID_UPDATE_STATE_APPLIED_DEFAULT );

  test_bam_env_destroy( env );
}

static void
test_bam_admin_rpc_path_empty_skips_frankendancer_apply( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  fd_wksp_t * gossip_mem = test_bam_setup_gossip_out( env );
  ulong chunk_before = state->gossip_out.chunk;
  ulong seq_before   = env->stem_seqs[ state->gossip_out.idx ];
  state->bam_gossip_handoff_pending = 1U;

  test_bam_admin_rpc_mock_reset();
  FD_TEST( !fd_bam_gossip_update( state, state->stem, 0 ) );

  FD_TEST( test_bam_admin_rpc_mock.request_cnt == 0UL );
  FD_TEST( state->gossip_out.chunk == chunk_before );
  FD_TEST( env->stem_seqs[ state->gossip_out.idx ] == seq_before );
  FD_TEST( state->tpu_update_state == FD_BAM_TPU_UPDATE_STATE_PENDING_DEFAULT );
  FD_TEST( state->client_id_update_state == FD_BAM_CLIENT_ID_UPDATE_STATE_PENDING_DEFAULT );
  FD_TEST( !state->bam_gossip_handoff_pending );

  FD_TEST( fd_cstr_to_ip4_addr( "1.1.1.1", &state->configured_default_tpu.addr ) );
  state->configured_default_tpu.port = fd_ushort_bswap( 4242U );
  state->bam_tpu     = state->configured_default_tpu;
  state->bam_tpu_fwd = state->configured_default_tpu;

  ulong publish_chunk = state->gossip_out.chunk;
  FD_TEST( fd_bam_gossip_update( state, state->stem, 0 ) );
  fd_bam_contact_update_t update = test_bam_read_gossip_update( gossip_mem, publish_chunk );

  FD_TEST( test_bam_admin_rpc_mock.request_cnt == 0UL );
  FD_TEST( update.tpu.l     == state->configured_default_tpu.l );
  FD_TEST( update.tpu_fwd.l == state->configured_default_tpu.l );
  FD_TEST( update.version_client_id == FD_GOSSIP_CONTACT_INFO_CLIENT_FIREDANCER );
  FD_TEST( state->default_tpu.l     == state->configured_default_tpu.l );
  FD_TEST( state->default_tpu_fwd.l == state->configured_default_tpu.l );
  FD_TEST( state->tpu_update_state == FD_BAM_TPU_UPDATE_STATE_APPLIED_DEFAULT );
  FD_TEST( state->client_id_update_state == FD_BAM_CLIENT_ID_UPDATE_STATE_APPLIED_DEFAULT );

  test_bam_env_destroy( env );
}

/* --- Gossip advertisement ------------------------------------------------------------ */

static void
test_bam_gossip_send_stub( void *               ctx,
                           fd_stem_context_t *  stem,
                           uchar const *        data,
                           ulong                sz,
                           fd_ip4_port_t const *peer_address,
                           ulong                now ) {
  (void)stem; (void)data; (void)sz; (void)peer_address; (void)now;
  if( FD_LIKELY( ctx ) ) (*(ulong *)ctx)++;
}

static void
test_bam_gossip_sign_stub( void *        ctx,
                           uchar const * data,
                           ulong         sz,
                           int           sign_type,
                           uchar *       out_signature ) {
  (void)ctx; (void)data; (void)sz; (void)sign_type;
  for( uchar i=0U; i<64U; i++ ) out_signature[ i ] = i;
}

static void
test_bam_gossip_ping_change_stub( void *        ctx,
                                  uchar const * peer_pubkey,
                                  fd_ip4_port_t peer_address,
                                  long          now,
                                  int           change_type ) {
  (void)ctx; (void)peer_pubkey; (void)peer_address; (void)now; (void)change_type;
}

static void
test_bam_gossip_activity_update_stub( void *                           ctx,
                                      fd_pubkey_t const *              identity,
                                      fd_gossip_contact_info_t const * ci,
                                      int                              change_type ) {
  (void)ctx; (void)identity; (void)ci; (void)change_type;
}

static void
test_bam_gossip_tile_applies_contact_update( fd_wksp_t * wksp ) {
  fd_gossip_tile_ctx_t ctx;
  fd_memset( &ctx, 0, sizeof(ctx) );
  ctx.last_wallclock = fd_log_wallclock();

  FD_TEST( FD_TPU_PARSED_MTU >= FD_NET_MTU );
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_wksp_t * out_mem = fd_wksp_containing( env->out_dcache );
  ctx.gossip_out[0] = (fd_gossip_out_ctx_t){
      .idx    = 0UL,
      .mem    = out_mem,
      .chunk0 = fd_dcache_compact_chunk0( out_mem, env->out_dcache ),
      .chunk  = fd_dcache_compact_chunk0( out_mem, env->out_dcache ),
      .wmark  = fd_dcache_compact_wmark( out_mem, env->out_dcache, FD_NET_MTU )
  };

  fd_gossip_contact_info_t base_ci = {0};
  for( uchar i=0U; i<32U; i++ ) ctx.identity_key->uc[ i ] = i;
  base_ci.shred_version  = 1U;
  base_ci.outset         = (ulong)FD_NANOSEC_TO_MICRO( ctx.last_wallclock );
  base_ci.version.client = FD_GOSSIP_CONTACT_INFO_CLIENT_FIREDANCER;

  fd_ip4_port_t default_gossip = { .addr = 0, .port = fd_ushort_bswap( 8000 ) };
  FD_TEST( fd_cstr_to_ip4_addr( "127.0.0.1", &default_gossip.addr ) );
  base_ci.sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_GOSSIP ] =
      (fd_gossip_socket_t){ .is_ipv6 = 0, .ip4 = default_gossip.addr, .port = default_gossip.port };
  *ctx.my_contact_info = base_ci;

  ulong max_values = 128UL;
  fd_ip4_port_t entrypoints[1] = { default_gossip };
  void * gossip_mem = aligned_alloc( fd_gossip_align(), fd_gossip_footprint( max_values, 1UL ) );
  FD_TEST( gossip_mem );
  ulong send_cnt = 0UL;

  fd_rng_t * rng = fd_rng_join( fd_rng_new( ctx.rng, ctx.rng_seed, ctx.rng_idx ) );
  FD_TEST( rng );

  void * shgossip = fd_gossip_new( gossip_mem,
                                   rng,
                                   max_values,
                                   1UL,
                                   entrypoints,
                                   ctx.identity_key->uc,
                                   ctx.my_contact_info,
                                   ctx.last_wallclock,
                                   test_bam_gossip_send_stub,
                                   &send_cnt,
                                   test_bam_gossip_sign_stub,
                                   NULL,
                                   test_bam_gossip_ping_change_stub,
                                   NULL,
                                   test_bam_gossip_activity_update_stub,
                                   NULL,
                                   ctx.gossip_out,
                                   ctx.net_out );
  FD_TEST( shgossip );
  ctx.gossip = fd_gossip_join( shgossip );
  FD_TEST( ctx.gossip );

  uint bam_tpu = 0U;
  FD_TEST( fd_cstr_to_ip4_addr( "9.8.7.6", &bam_tpu ) );
  uint bam_tpu_fwd = 0U;
  FD_TEST( fd_cstr_to_ip4_addr( "4.3.2.1", &bam_tpu_fwd ) );

  fd_bam_contact_update_t contact_update = {
      .tpu               = (fd_ip4_port_t){ .addr = bam_tpu,     .port = fd_ushort_bswap( 5000 ) },
      .tpu_fwd           = (fd_ip4_port_t){ .addr = bam_tpu_fwd, .port = fd_ushort_bswap( 6000 ) },
      .version_client_id = FD_GOSSIP_CONTACT_INFO_CLIENT_BAM,
  };
  send_cnt = 0UL;
  FD_TEST( fd_gossip_tile_apply_bam_contact( &ctx, &contact_update, ctx.last_wallclock + 1L, env->stem ) );
  FD_TEST( send_cnt > 0UL );

  FD_TEST( ctx.my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU ].ip4                == bam_tpu );
  FD_TEST( ctx.my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU ].port               == fd_ushort_bswap( 5000 ) );
  FD_TEST( ctx.my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_VOTE ].ip4           == bam_tpu );
  FD_TEST( ctx.my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_VOTE ].port          == fd_ushort_bswap( 5000 ) );
  FD_TEST( ctx.my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_FORWARDS ].ip4       == bam_tpu_fwd );
  FD_TEST( ctx.my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_FORWARDS ].port      == fd_ushort_bswap( 6000 ) );
  FD_TEST( ctx.my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_QUIC ].ip4           == bam_tpu );
  FD_TEST( ctx.my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_QUIC ].port          == fd_ushort_bswap( 5006 ) );
  FD_TEST( ctx.my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_VOTE_QUIC ].ip4      == bam_tpu );
  FD_TEST( ctx.my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_VOTE_QUIC ].port     == fd_ushort_bswap( 5006 ) );
  FD_TEST( ctx.my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_FORWARDS_QUIC ].ip4  == bam_tpu_fwd );
  FD_TEST( ctx.my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_FORWARDS_QUIC ].port == fd_ushort_bswap( 6006 ) );
  FD_TEST( ctx.my_contact_info->version.client == FD_GOSSIP_CONTACT_INFO_CLIENT_BAM );

  uint default_tpu = 0U;
  FD_TEST( fd_cstr_to_ip4_addr( "1.2.3.4", &default_tpu ) );
  uint default_tpu_fwd = 0U;
  FD_TEST( fd_cstr_to_ip4_addr( "5.6.7.8", &default_tpu_fwd ) );

  contact_update = (fd_bam_contact_update_t) {
      .tpu               = (fd_ip4_port_t){ .addr = default_tpu,     .port = fd_ushort_bswap( 7000 ) },
      .tpu_fwd           = (fd_ip4_port_t){ .addr = default_tpu_fwd, .port = fd_ushort_bswap( 8000 ) },
      .version_client_id = FD_GOSSIP_CONTACT_INFO_CLIENT_FIREDANCER,
  };
  FD_TEST( fd_gossip_tile_apply_bam_contact( &ctx, &contact_update, ctx.last_wallclock + 2L, NULL ) );

  FD_TEST( ctx.my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU ].ip4            == default_tpu );
  FD_TEST( ctx.my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU ].port           == fd_ushort_bswap( 7000 ) );
  FD_TEST( ctx.my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_VOTE ].ip4       == default_tpu );
  FD_TEST( ctx.my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_VOTE ].port      == fd_ushort_bswap( 7000 ) );
  FD_TEST( ctx.my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_QUIC ].ip4       == default_tpu );
  FD_TEST( ctx.my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_QUIC ].port      == fd_ushort_bswap( 7006 ) );
  FD_TEST( ctx.my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_VOTE_QUIC ].ip4  == default_tpu );
  FD_TEST( ctx.my_contact_info->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_VOTE_QUIC ].port == fd_ushort_bswap( 7006 ) );
  FD_TEST( ctx.my_contact_info->version.client == FD_GOSSIP_CONTACT_INFO_CLIENT_FIREDANCER );

  free( gossip_mem );
  test_bam_env_destroy( env );
}

static void
test_bam_gossip_publishes_bam_config_contact( fd_wksp_t * wksp ) {
  /* A healthy BAM tile should advertise the TPU endpoints supplied by BamConfig. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  fd_wksp_t * gossip_mem = test_bam_setup_gossip_out( env );
  state->bam_status_recent = FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY;

  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  resp.has_bam_config = true;
  resp.bam_config.has_tpu_sock = true;
  fd_cstr_ncpy( resp.bam_config.tpu_sock.ip, "10.20.30.40", sizeof( resp.bam_config.tpu_sock.ip ) );
  resp.bam_config.tpu_sock.port = 1122U;
  resp.bam_config.has_tpu_fwd_sock = true;
  fd_cstr_ncpy( resp.bam_config.tpu_fwd_sock.ip, "11.12.13.14", sizeof( resp.bam_config.tpu_fwd_sock.ip ) );
  resp.bam_config.tpu_fwd_sock.port = 3344U;

  uchar pb_buf[ 256 ];
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );

  ulong publish_chunk = state->gossip_out.chunk;
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );

  fd_bam_contact_update_t update = test_bam_read_gossip_update( gossip_mem, publish_chunk );

  uint expected_tpu_addr = 0U;
  FD_TEST( fd_cstr_to_ip4_addr( "10.20.30.40", &expected_tpu_addr ) );
  FD_TEST( update.tpu.addr == expected_tpu_addr );
  FD_TEST( fd_ushort_bswap( update.tpu.port ) == 1122U );

  uint expected_tpu_fwd_addr = 0U;
  FD_TEST( fd_cstr_to_ip4_addr( "11.12.13.14", &expected_tpu_fwd_addr ) );
  FD_TEST( update.tpu_fwd.addr == expected_tpu_fwd_addr );
  FD_TEST( fd_ushort_bswap( update.tpu_fwd.port ) == 3344U );
  FD_TEST( update.version_client_id == FD_GOSSIP_CONTACT_INFO_CLIENT_BAM );

  test_bam_env_destroy( env );
}

static void
test_bam_activation_waits_for_gossip_fseq( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;
  test_bam_setup_gossip_out( env );
  test_bam_set_bam_tpu( state, "10.1.1.1", 5000U, "10.1.1.2", 6000U );

  test_bam_handoff_fseqs_t fseqs[1];
  test_bam_setup_handoff_fseqs( env, fseqs );

  fd_bam_publish_active_state( state, state->stem, 1 );
  FD_TEST( env->stem_seqs[0] == 1UL );
  FD_TEST( state->bam_gossip_handoff_pending );
  FD_TEST( state->bam_gossip_handoff_target == 1UL );
  FD_TEST( fd_fseq_query( fseqs->status ) == 0UL );

  fd_bam_publish_active_state( state, state->stem, 1 );
  FD_TEST( env->stem_seqs[0] == 1UL );
  FD_TEST( fd_fseq_query( fseqs->status ) == 0UL );

  fd_fseq_update( fseqs->gossip, 1UL );
  fd_bam_publish_active_state( state, state->stem, 1 );
  FD_TEST( env->stem_seqs[0] == 1UL );
  FD_TEST( !state->bam_gossip_handoff_pending );
  FD_TEST( fd_fseq_query( fseqs->status ) == FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );

  test_bam_teardown_handoff_fseqs( env, fseqs );
  test_bam_env_destroy( env );
}

static void
test_bam_activation_waits_for_bundle_publication( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  uchar fseq_mem[ FD_FSEQ_FOOTPRINT ] __attribute__((aligned(FD_FSEQ_ALIGN)));
  void * fseq_shmem = fd_fseq_new( fseq_mem, FD_BAM_STATUS_FSEQ_BUNDLE_PUBLISHING );
  FD_TEST( fseq_shmem );
  ulong * fseq = fd_fseq_join( fseq_shmem );
  FD_TEST( fseq );
  state->bam_status_fseq      = fseq;
  state->tpu_update_state     = FD_BAM_TPU_UPDATE_STATE_APPLIED_BAM;
  state->client_id_update_state = FD_BAM_CLIENT_ID_UPDATE_STATE_APPLIED_BAM;

  /* Bundle owns publication, so BAM must leave the word untouched and remain
     unable to drain scheduler work. */
  fd_bam_publish_active_state( state, state->stem, 1 );
  FD_TEST( fd_fseq_query( fseq )==FD_BAM_STATUS_FSEQ_BUNDLE_PUBLISHING );

  /* Once bundle releases ownership, BAM wins the next activation attempt. */
  FD_TEST( FD_ATOMIC_CAS( fseq, FD_BAM_STATUS_FSEQ_BUNDLE_PUBLISHING, 0UL )==FD_BAM_STATUS_FSEQ_BUNDLE_PUBLISHING );
  state->tpu_update_state       = FD_BAM_TPU_UPDATE_STATE_APPLIED_BAM;
  state->client_id_update_state = FD_BAM_CLIENT_ID_UPDATE_STATE_APPLIED_BAM;
  fd_bam_publish_active_state( state, state->stem, 1 );
  FD_TEST( fd_fseq_query( fseq )==FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );

  fd_bam_publish_active_state( state, state->stem, 0 );
  FD_TEST( fd_fseq_query( fseq )==0UL );

  /* Deactivation must not erase a bundle publication claim. */
  fd_fseq_update( fseq, FD_BAM_STATUS_FSEQ_BUNDLE_PUBLISHING );
  fd_bam_publish_active_state( state, state->stem, 0 );
  FD_TEST( fd_fseq_query( fseq )==FD_BAM_STATUS_FSEQ_BUNDLE_PUBLISHING );

  state->bam_status_fseq = NULL;
  FD_TEST( fd_fseq_leave( fseq )==fseq_shmem );
  FD_TEST( fd_fseq_delete( fseq_shmem )==fseq_shmem );
  test_bam_env_destroy( env );
}

static void
test_bam_activation_ignores_stale_gossip_fseq_after_reenable( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;
  test_bam_setup_gossip_out( env );
  test_bam_set_bam_tpu( state, "10.2.1.1", 5000U, "10.2.1.2", 6000U );
  FD_TEST( fd_cstr_to_ip4_addr( "10.2.1.3", &state->configured_default_tpu.addr ) );
  state->configured_default_tpu.port = fd_ushort_bswap( 7000U );

  test_bam_handoff_fseqs_t fseqs[1];
  test_bam_setup_handoff_fseqs( env, fseqs );

  fd_bam_publish_active_state( state, state->stem, 1 );
  FD_TEST( state->bam_gossip_handoff_target == 1UL );
  FD_TEST( env->stem_seqs[0] == 1UL );
  FD_TEST( fd_fseq_query( fseqs->status ) == 0UL );

  fd_bam_publish_active_state( state, state->stem, 0 );
  FD_TEST( !state->bam_gossip_handoff_pending );
  FD_TEST( fd_fseq_query( fseqs->status ) == 0UL );
  FD_TEST( env->stem_seqs[0] == 2UL );

  fd_bam_publish_active_state( state, state->stem, 1 );
  FD_TEST( state->bam_gossip_handoff_pending );
  FD_TEST( state->bam_gossip_handoff_target == 3UL );
  FD_TEST( env->stem_seqs[0] == 3UL );

  fd_fseq_update( fseqs->gossip, 1UL );
  fd_bam_publish_active_state( state, state->stem, 1 );
  FD_TEST( fd_fseq_query( fseqs->status ) == 0UL );
  FD_TEST( env->stem_seqs[0] == 3UL );

  fd_fseq_update( fseqs->gossip, 2UL );
  fd_bam_publish_active_state( state, state->stem, 1 );
  FD_TEST( fd_fseq_query( fseqs->status ) == 0UL );
  FD_TEST( env->stem_seqs[0] == 3UL );

  fd_fseq_update( fseqs->gossip, 3UL );
  fd_bam_publish_active_state( state, state->stem, 1 );
  FD_TEST( fd_fseq_query( fseqs->status ) == FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );

  test_bam_teardown_handoff_fseqs( env, fseqs );
  test_bam_env_destroy( env );
}

static void
test_bam_disable_clears_status_with_pending_gossip_handoff( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;
  test_bam_setup_gossip_out( env );
  test_bam_set_bam_tpu( state, "10.3.1.1", 5000U, "10.3.1.2", 6000U );

  test_bam_handoff_fseqs_t fseqs[1];
  test_bam_setup_handoff_fseqs( env, fseqs );

  fd_bam_publish_active_state( state, state->stem, 1 );
  FD_TEST( state->bam_gossip_handoff_pending );
  fd_fseq_update( fseqs->status, FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );

  fd_bam_publish_active_state( state, state->stem, 0 );
  FD_TEST( fd_fseq_query( fseqs->status ) == 0UL );
  FD_TEST( !state->bam_gossip_handoff_pending );

  fd_fseq_update( fseqs->gossip, 1UL );
  fd_bam_publish_active_state( state, state->stem, 0 );
  FD_TEST( fd_fseq_query( fseqs->status ) == 0UL );

  test_bam_teardown_handoff_fseqs( env, fseqs );
  test_bam_env_destroy( env );
}

static void
test_bam_invalid_quic_base_port_does_not_activate( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;
  test_bam_setup_gossip_out( env );
  test_bam_set_bam_tpu( state, "10.4.1.1", USHRT_MAX, "10.4.1.2", 6000U );

  test_bam_handoff_fseqs_t fseqs[1];
  test_bam_setup_handoff_fseqs( env, fseqs );

  ulong chunk_before = state->gossip_out.chunk;
  fd_bam_publish_active_state( state, state->stem, 1 );
  FD_TEST( env->stem_seqs[0] == 0UL );
  FD_TEST( state->gossip_out.chunk == chunk_before );
  FD_TEST( fd_fseq_query( fseqs->status ) == 0UL );
  FD_TEST( !state->bam_gossip_handoff_pending );
  FD_TEST( state->tpu_update_state == FD_BAM_TPU_UPDATE_STATE_PENDING_BAM );
  FD_TEST( state->client_id_update_state == FD_BAM_CLIENT_ID_UPDATE_STATE_PENDING_BAM );

  fd_fseq_update( fseqs->gossip, 1UL );
  fd_bam_publish_active_state( state, state->stem, 1 );
  FD_TEST( env->stem_seqs[0] == 0UL );
  FD_TEST( fd_fseq_query( fseqs->status ) == 0UL );

  test_bam_teardown_handoff_fseqs( env, fseqs );
  test_bam_env_destroy( env );
}

static void
test_bam_gossip_resets_when_contact_missing( fd_wksp_t * wksp ) {
  /* An incomplete BamConfig without a cached BAM contact should force gossip
     back to defaults even if the previous applied state was BAM. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn_empty( env );
  test_bam_env_mock_h2_hs( env->state );
  fd_bam_tile_t * state = env->state;

  fd_wksp_t * gossip_mem = test_bam_setup_gossip_out( env );
  FD_TEST( fd_cstr_to_ip4_addr( "1.1.1.1", &state->default_tpu.addr ) );
  FD_TEST( fd_cstr_to_ip4_addr( "2.2.2.2", &state->default_tpu_fwd.addr ) );
  state->default_tpu.port     = fd_ushort_bswap( 4242 );
  state->default_tpu_fwd.port = fd_ushort_bswap( 4343 );

  state->tpu_update_state       = FD_BAM_TPU_UPDATE_STATE_APPLIED_BAM;
  state->client_id_update_state = FD_BAM_CLIENT_ID_UPDATE_STATE_APPLIED_BAM;
  state->bam_stream_live        = 1U;
  state->bam_config_received    = 1U;
  state->bam_last_builder_activity_ns = fd_bam_now();
  state->bam_last_validator_heartbeat_ns = fd_bam_now();

  uchar fseq_mem[ FD_FSEQ_FOOTPRINT ] __attribute__((aligned(FD_FSEQ_ALIGN)));
  fd_memset( fseq_mem, 0, sizeof(fseq_mem) );
  void * fseq_shmem = fd_fseq_new( fseq_mem, FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
  FD_TEST( fseq_shmem );
  ulong * fseq = fd_fseq_join( fseq_shmem );
  FD_TEST( fseq );
  state->bam_status_fseq = fseq;

  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  resp.has_bam_config = true;
  resp.bam_config.has_tpu_sock = true;
  fd_cstr_ncpy( resp.bam_config.tpu_sock.ip, "12.34.56.78", sizeof( resp.bam_config.tpu_sock.ip ) );
  resp.bam_config.tpu_sock.port = 2222U;
  resp.bam_config.has_tpu_fwd_sock = false;

  uchar pb_buf[ 256 ];
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );

  ulong publish_chunk = state->gossip_out.chunk;
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );

  FD_TEST( state->bam_tpu.l == 0UL );
  FD_TEST( state->bam_tpu_fwd.l == 0UL );
  FD_TEST( fd_fseq_query( fseq ) == 0UL );

  fd_bam_contact_update_t update = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  FD_TEST( update.tpu.addr     == state->default_tpu.addr );
  FD_TEST( update.tpu.port     == state->default_tpu.port );
  FD_TEST( update.tpu_fwd.addr == state->default_tpu_fwd.addr );
  FD_TEST( update.tpu_fwd.port == state->default_tpu_fwd.port );
  FD_TEST( update.version_client_id == FD_GOSSIP_CONTACT_INFO_CLIENT_FIREDANCER );

  FD_TEST( fd_fseq_leave( fseq ) == fseq_shmem );
  FD_TEST( fd_fseq_delete( fseq_shmem ) == fseq_shmem );
  state->bam_status_fseq = NULL;

  test_bam_env_destroy( env );
}

static void
test_bam_gossip_disconnect_uses_defaults_without_clearing_stored_contact( fd_wksp_t * wksp ) {
  /* Incomplete configs while disconnected should fall back to default gossip
     without clearing the last valid BAM contact. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  test_bam_env_mock_conn( env );
  fd_wksp_t * gossip_mem = test_bam_setup_gossip_out( env );
  state->bam_status_recent = FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY;
  FD_TEST( fd_cstr_to_ip4_addr( "1.1.1.1", &state->default_tpu.addr ) );
  FD_TEST( fd_cstr_to_ip4_addr( "2.2.2.2", &state->default_tpu_fwd.addr ) );
  state->default_tpu.port     = fd_ushort_bswap( 4242 );
  state->default_tpu_fwd.port = fd_ushort_bswap( 4343 );

  fd_bam_contact_update_t updates[4];
  ulong                   update_cnt = 0UL;

  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  resp.has_bam_config = true;
  resp.bam_config.has_tpu_sock = true;
  fd_cstr_ncpy( resp.bam_config.tpu_sock.ip, "9.8.7.6", sizeof( resp.bam_config.tpu_sock.ip ) );
  resp.bam_config.tpu_sock.port = 5000U;
  resp.bam_config.has_tpu_fwd_sock = true;
  fd_cstr_ncpy( resp.bam_config.tpu_fwd_sock.ip, "4.3.2.1", sizeof( resp.bam_config.tpu_fwd_sock.ip ) );
  resp.bam_config.tpu_fwd_sock.port = 6000U;

  uchar pb_buf[ 256 ];
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
  ulong publish_chunk = state->gossip_out.chunk;
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );

  /* Disconnected path: publish default contact so gossip falls back to Firedancer TPU. */
  updates[ update_cnt++ ] = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  FD_TEST( update_cnt == 1UL );

  uint expected_tpu_addr = 0U;
  FD_TEST( fd_cstr_to_ip4_addr( "9.8.7.6", &expected_tpu_addr ) );
  FD_TEST( updates[0].tpu.addr == expected_tpu_addr );
  FD_TEST( fd_ushort_bswap( updates[0].tpu.port ) == 5000U );

  uint expected_tpu_fwd_addr = 0U;
  FD_TEST( fd_cstr_to_ip4_addr( "4.3.2.1", &expected_tpu_fwd_addr ) );
  FD_TEST( updates[0].tpu_fwd.addr == expected_tpu_fwd_addr );
  FD_TEST( fd_ushort_bswap( updates[0].tpu_fwd.port ) == 6000U );
  FD_TEST( updates[0].version_client_id == FD_GOSSIP_CONTACT_INFO_CLIENT_BAM );

  publish_chunk = state->gossip_out.chunk;
  fd_bam_gossip_update( state, state->stem, false );
  updates[ update_cnt++ ] = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  FD_TEST( update_cnt == 2UL );
  FD_TEST( updates[1].tpu.addr     == state->default_tpu.addr );
  FD_TEST( updates[1].tpu.port     == state->default_tpu.port );
  FD_TEST( updates[1].tpu_fwd.addr == state->default_tpu_fwd.addr );
  FD_TEST( updates[1].tpu_fwd.port == state->default_tpu_fwd.port );
  FD_TEST( updates[1].version_client_id == FD_GOSSIP_CONTACT_INFO_CLIENT_FIREDANCER );

  state->bam_status_recent = FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTING;
  state->bam_stream_live = 0U;
  state->tpu_update_state = FD_BAM_TPU_UPDATE_STATE_APPLIED_BAM;

  resp = (bam_api_ConfigResponse)bam_api_ConfigResponse_init_default;
  resp.has_bam_config = true;
  resp.bam_config.has_tpu_sock = false;
  resp.bam_config.has_tpu_fwd_sock = false;
  ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
  publish_chunk = state->gossip_out.chunk;
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );

  FD_TEST( state->bam_tpu.addr == expected_tpu_addr );
  FD_TEST( fd_ushort_bswap( state->bam_tpu.port ) == 5000U );
  FD_TEST( state->bam_tpu_fwd.addr == expected_tpu_fwd_addr );
  FD_TEST( fd_ushort_bswap( state->bam_tpu_fwd.port ) == 6000U );
  updates[ update_cnt++ ] = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  FD_TEST( update_cnt == 3UL );
  FD_TEST( updates[2].tpu.addr     == state->default_tpu.addr );
  FD_TEST( updates[2].tpu.port     == state->default_tpu.port );
  FD_TEST( updates[2].tpu_fwd.addr == state->default_tpu_fwd.addr );
  FD_TEST( updates[2].tpu_fwd.port == state->default_tpu_fwd.port );
  FD_TEST( updates[2].version_client_id == FD_GOSSIP_CONTACT_INFO_CLIENT_FIREDANCER );

  /* Explicit use_bam=false still advertises defaults and does not depend on
     the stored BAM contact being zeroed. */
  publish_chunk = state->gossip_out.chunk;
  fd_bam_gossip_update( state, state->stem, false );
  updates[ update_cnt++ ] = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  FD_TEST( update_cnt == 4UL );
  FD_TEST( updates[3].tpu.addr     == state->default_tpu.addr );
  FD_TEST( updates[3].tpu.port     == state->default_tpu.port );
  FD_TEST( updates[3].tpu_fwd.addr == state->default_tpu_fwd.addr );
  FD_TEST( updates[3].tpu_fwd.port == state->default_tpu_fwd.port );
  FD_TEST( updates[3].version_client_id == FD_GOSSIP_CONTACT_INFO_CLIENT_FIREDANCER );

  test_bam_env_destroy( env );
}

static void
test_bam_runtime_toggle_updates_gossip( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  fd_wksp_t * gossip_mem = test_bam_setup_gossip_out( env );
  state->bam_status_recent = FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY;
  FD_TEST( fd_cstr_to_ip4_addr( "1.1.1.1", &state->configured_default_tpu.addr ) );
  state->configured_default_tpu.port = fd_ushort_bswap( 4242U );

  fd_bam_contact_update_t updates[3];
  ulong                   update_cnt = 0UL;

  /* Initial BamConfig should publish the override while runtime is enabled. */
  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  resp.has_bam_config = true;
  resp.bam_config.has_tpu_sock = true;
  fd_cstr_ncpy( resp.bam_config.tpu_sock.ip, "9.9.9.9", sizeof( resp.bam_config.tpu_sock.ip ) );
  resp.bam_config.tpu_sock.port = 7000U;
  resp.bam_config.has_tpu_fwd_sock = true;
  fd_cstr_ncpy( resp.bam_config.tpu_fwd_sock.ip, "8.8.8.8", sizeof( resp.bam_config.tpu_fwd_sock.ip ) );
  resp.bam_config.tpu_fwd_sock.port = 7001U;

  uchar pb_buf[ 256 ];
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
  ulong publish_chunk = state->gossip_out.chunk;
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );

  updates[ update_cnt++ ] = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  FD_TEST( update_cnt == 1UL );

  uint expected_tpu_addr = 0U;
  FD_TEST( fd_cstr_to_ip4_addr( "9.9.9.9", &expected_tpu_addr ) );
  FD_TEST( updates[0].tpu.addr == expected_tpu_addr );
  FD_TEST( fd_ushort_bswap( updates[0].tpu.port ) == 7000U );

  uint expected_tpu_fwd_addr = 0U;
  FD_TEST( fd_cstr_to_ip4_addr( "8.8.8.8", &expected_tpu_fwd_addr ) );
  FD_TEST( updates[0].tpu_fwd.addr == expected_tpu_fwd_addr );
  FD_TEST( fd_ushort_bswap( updates[0].tpu_fwd.port ) == 7001U );
  FD_TEST( updates[0].version_client_id == FD_GOSSIP_CONTACT_INFO_CLIENT_BAM );

  /* Disabling runtime should immediately revert gossip to the Firedancer defaults. */
  publish_chunk = state->gossip_out.chunk;
  state->enabled = 0;
  fd_bam_gossip_update( state, state->stem, false );
  updates[ update_cnt++ ] = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  FD_TEST( update_cnt == 2UL );
  FD_TEST( updates[1].tpu.l     == state->configured_default_tpu.l );
  FD_TEST( updates[1].tpu_fwd.l == state->configured_default_tpu.l );
  FD_TEST( updates[1].version_client_id == FD_GOSSIP_CONTACT_INFO_CLIENT_FIREDANCER );

  /* Re-enabling runtime while still connected should republish the BamConfig address. */
  publish_chunk = state->gossip_out.chunk;
  state->enabled = 1;
  fd_bam_gossip_update( state, state->stem, true );
  updates[ update_cnt++ ] = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  FD_TEST( update_cnt == 3UL );
  FD_TEST( updates[2].tpu.addr == expected_tpu_addr );
  FD_TEST( fd_ushort_bswap( updates[2].tpu.port ) == 7000U );
  FD_TEST( updates[2].tpu_fwd.addr == expected_tpu_fwd_addr );
  FD_TEST( fd_ushort_bswap( updates[2].tpu_fwd.port ) == 7001U );
  FD_TEST( updates[2].version_client_id == FD_GOSSIP_CONTACT_INFO_CLIENT_BAM );

  test_bam_env_destroy( env );
}

static void
test_bam_shred_update_publishes_receiver_list( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  fd_wksp_t * shred_mem = fd_wksp_containing( env->out_dcache );
  state->shred_out = (fd_bam_out_ctx_t){
      .idx    = 0UL,
      .mem    = shred_mem,
      .chunk0 = fd_dcache_compact_chunk0( shred_mem, env->out_dcache ),
      .chunk  = fd_dcache_compact_chunk0( shred_mem, env->out_dcache ),
      .wmark  = fd_dcache_compact_wmark( shred_mem, env->out_dcache, sizeof(fd_bam_shred_update_t) )
  };

  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  resp.has_bam_config = true;
  resp.bam_config.has_tpu_sock = true;
  fd_cstr_ncpy( resp.bam_config.tpu_sock.ip, "10.20.30.40", sizeof( resp.bam_config.tpu_sock.ip ) );
  resp.bam_config.tpu_sock.port = 1122U;
  resp.bam_config.has_tpu_fwd_sock = true;
  fd_cstr_ncpy( resp.bam_config.tpu_fwd_sock.ip, "11.12.13.14", sizeof( resp.bam_config.tpu_fwd_sock.ip ) );
  resp.bam_config.tpu_fwd_sock.port = 3344U;
  resp.bam_config.shred_sock_count = 4U;
  fd_cstr_ncpy( resp.bam_config.shred_sock[ 0 ].ip, "1.1.1.1", sizeof( resp.bam_config.shred_sock[ 0 ].ip ) );
  resp.bam_config.shred_sock[ 0 ].port = 5001U;
  fd_cstr_ncpy( resp.bam_config.shred_sock[ 1 ].ip, "2.2.2.2", sizeof( resp.bam_config.shred_sock[ 1 ].ip ) );
  resp.bam_config.shred_sock[ 1 ].port = 5002U;
  fd_cstr_ncpy( resp.bam_config.shred_sock[ 2 ].ip, "1.1.1.1", sizeof( resp.bam_config.shred_sock[ 2 ].ip ) );
  resp.bam_config.shred_sock[ 2 ].port = 5001U;
  fd_cstr_ncpy( resp.bam_config.shred_sock[ 3 ].ip, "bad-ip", sizeof( resp.bam_config.shred_sock[ 3 ].ip ) );
  resp.bam_config.shred_sock[ 3 ].port = 0U;

  uchar pb_buf[ 1024 ];
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );

  ulong publish_chunk = state->shred_out.chunk;
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );

  fd_bam_shred_update_t update = test_bam_read_shred_update( shred_mem, publish_chunk );
  FD_TEST( update.shred_sock_cnt == 3UL );
  FD_TEST( state->bam_shred_sock_cnt == 3UL );

  uint ip4 = 0U;
  FD_TEST( fd_cstr_to_ip4_addr( "1.1.1.1", &ip4 ) );
  FD_TEST( update.shred_sock[ 0 ].addr == ip4 );
  FD_TEST( fd_ushort_bswap( update.shred_sock[ 0 ].port ) == 5001U );
  FD_TEST( update.shred_sock[ 2 ].addr == ip4 );
  FD_TEST( fd_ushort_bswap( update.shred_sock[ 2 ].port ) == 5001U );

  FD_TEST( fd_cstr_to_ip4_addr( "2.2.2.2", &ip4 ) );
  FD_TEST( update.shred_sock[ 1 ].addr == ip4 );
  FD_TEST( fd_ushort_bswap( update.shred_sock[ 1 ].port ) == 5002U );

  test_bam_env_destroy( env );
}

static void
test_bam_shred_update_disconnect_uses_empty_without_clearing_receivers( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  fd_wksp_t * shred_mem = fd_wksp_containing( env->out_dcache );
  state->shred_out = (fd_bam_out_ctx_t){
      .idx    = 0UL,
      .mem    = shred_mem,
      .chunk0 = fd_dcache_compact_chunk0( shred_mem, env->out_dcache ),
      .chunk  = fd_dcache_compact_chunk0( shred_mem, env->out_dcache ),
      .wmark  = fd_dcache_compact_wmark( shred_mem, env->out_dcache, sizeof(fd_bam_shred_update_t) )
  };

  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  resp.has_bam_config = true;
  resp.bam_config.has_tpu_sock = true;
  fd_cstr_ncpy( resp.bam_config.tpu_sock.ip, "10.20.30.40", sizeof( resp.bam_config.tpu_sock.ip ) );
  resp.bam_config.tpu_sock.port = 1122U;
  resp.bam_config.has_tpu_fwd_sock = true;
  fd_cstr_ncpy( resp.bam_config.tpu_fwd_sock.ip, "11.12.13.14", sizeof( resp.bam_config.tpu_fwd_sock.ip ) );
  resp.bam_config.tpu_fwd_sock.port = 3344U;
  resp.bam_config.shred_sock_count = 2U;
  fd_cstr_ncpy( resp.bam_config.shred_sock[ 0 ].ip, "3.3.3.3", sizeof( resp.bam_config.shred_sock[ 0 ].ip ) );
  resp.bam_config.shred_sock[ 0 ].port = 7001U;
  fd_cstr_ncpy( resp.bam_config.shred_sock[ 1 ].ip, "4.4.4.4", sizeof( resp.bam_config.shred_sock[ 1 ].ip ) );
  resp.bam_config.shred_sock[ 1 ].port = 7002U;

  uchar pb_buf[ 1024 ];
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );

  ulong publish_chunk = state->shred_out.chunk;
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );

  fd_bam_shred_update_t updates[ 3 ] = {0};
  updates[ 0 ] = test_bam_read_shred_update( shred_mem, publish_chunk );
  FD_TEST( updates[ 0 ].shred_sock_cnt == 2UL );

  publish_chunk = state->shred_out.chunk;
  fd_bam_shred_update( state, state->stem, false );
  updates[ 1 ] = test_bam_read_shred_update( shred_mem, publish_chunk );
  FD_TEST( updates[ 1 ].shred_sock_cnt == 0UL );
  FD_TEST( state->bam_shred_sock_cnt == 2UL );

  publish_chunk = state->shred_out.chunk;
  fd_bam_shred_update( state, state->stem, true );
  updates[ 2 ] = test_bam_read_shred_update( shred_mem, publish_chunk );
  FD_TEST( updates[ 2 ].shred_sock_cnt == 2UL );
  FD_TEST( updates[ 2 ].shred_sock[ 0 ].l == updates[ 0 ].shred_sock[ 0 ].l );
  FD_TEST( updates[ 2 ].shred_sock[ 1 ].l == updates[ 0 ].shred_sock[ 1 ].l );

  test_bam_env_destroy( env );
}

static void
test_bam_config_reuses_cached_contact_for_incomplete_refresh( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  fd_wksp_t * gossip_mem = test_bam_setup_gossip_out( env );
  state->bam_status_recent = FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY;

  FD_TEST( fd_cstr_to_ip4_addr( "1.1.1.1", &state->default_tpu.addr ) );
  FD_TEST( fd_cstr_to_ip4_addr( "2.2.2.2", &state->default_tpu_fwd.addr ) );
  state->default_tpu.port     = fd_ushort_bswap( 4242 );
  state->default_tpu_fwd.port = fd_ushort_bswap( 4343 );

  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  resp.has_bam_config = true;
  resp.bam_config.has_tpu_sock = true;
  fd_cstr_ncpy( resp.bam_config.tpu_sock.ip, "5.5.5.5", sizeof( resp.bam_config.tpu_sock.ip ) );
  resp.bam_config.tpu_sock.port = 5000U;
  resp.bam_config.has_tpu_fwd_sock = true;
  fd_cstr_ncpy( resp.bam_config.tpu_fwd_sock.ip, "6.6.6.6", sizeof( resp.bam_config.tpu_fwd_sock.ip ) );
  resp.bam_config.tpu_fwd_sock.port = 6000U;

  fd_ip4_port_t expected_tpu = { .port = fd_ushort_bswap( 5000U ) };
  fd_ip4_port_t expected_tpu_fwd = { .port = fd_ushort_bswap( 6000U ) };
  FD_TEST( fd_cstr_to_ip4_addr( "5.5.5.5", &expected_tpu.addr ) );
  FD_TEST( fd_cstr_to_ip4_addr( "6.6.6.6", &expected_tpu_fwd.addr ) );

  uchar pb_buf[ 256 ];
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );

  ulong publish_chunk = state->gossip_out.chunk;
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );
  fd_bam_contact_update_t update = test_bam_read_gossip_update( gossip_mem, publish_chunk );

  FD_TEST( state->bam_tpu.l == expected_tpu.l );
  FD_TEST( state->bam_tpu_fwd.l == expected_tpu_fwd.l );
  FD_TEST( update.tpu.l == expected_tpu.l );
  FD_TEST( update.tpu_fwd.l == expected_tpu_fwd.l );
  FD_TEST( update.version_client_id == FD_GOSSIP_CONTACT_INFO_CLIENT_BAM );

  resp = (bam_api_ConfigResponse)bam_api_ConfigResponse_init_default;
  resp.has_bam_config = true;
  resp.bam_config.has_tpu_sock = true;
  fd_cstr_ncpy( resp.bam_config.tpu_sock.ip, "7.7.7.7", sizeof( resp.bam_config.tpu_sock.ip ) );
  resp.bam_config.tpu_sock.port = 7000U;
  resp.bam_config.has_tpu_fwd_sock = false;
  ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
  publish_chunk = state->gossip_out.chunk;
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );
  update = test_bam_read_gossip_update( gossip_mem, publish_chunk );

  FD_TEST( state->bam_tpu.l == expected_tpu.l );
  FD_TEST( state->bam_tpu_fwd.l == expected_tpu_fwd.l );
  FD_TEST( update.tpu.l == expected_tpu.l );
  FD_TEST( update.tpu_fwd.l == expected_tpu_fwd.l );
  FD_TEST( update.version_client_id == FD_GOSSIP_CONTACT_INFO_CLIENT_BAM );

  test_bam_env_destroy( env );
}

static void
test_bam_endpoint_change_forgets_cached_contact_before_incomplete_refresh( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  FD_TEST( fd_bam_has_effective_contact( state ) );
  state->bam_shred_sock_cnt = 1U;
  FD_TEST( fd_cstr_to_ip4_addr( "9.9.9.9", &state->bam_shred_sock[0].addr ) );
  state->bam_shred_sock[0].port = fd_ushort_bswap( 9999U );

  fd_bam_ctrl_t ctrl;
  setup_ctrl_defaults( state, &ctrl );

  static fd_keyswitch_t keyswitch = {0};
  keyswitch.magic = FD_KEYSWITCH_MAGIC;
  keyswitch.state = FD_KEYSWITCH_STATE_COMPLETED;
  keyswitch.param = 0UL;
  state->keyswitch = &keyswitch;

  ushort old_gen = state->scheduler_gen;
  ctrl.command = FD_BAM_CTRL_CMD_URL;
  fd_cstr_ncpy( ctrl.url, "http://new.example.com:8899", sizeof( ctrl.url ) );
  ctrl.state = FD_BAM_CTRL_STATE_REQUEST;

  fd_bam_tile_housekeeping( state );

  FD_TEST( ctrl.state == FD_BAM_CTRL_STATE_SUCCESS );
  FD_TEST( state->scheduler_gen != old_gen );
  FD_TEST( state->bam_tpu.l == 0UL );
  FD_TEST( state->bam_tpu_fwd.l == 0UL );
  FD_TEST( state->bam_shred_sock_cnt == 0U );
  FD_TEST( state->bam_shred_sock[0].l == 0UL );
  FD_TEST( state->feedback_queue_depth == 0UL );

  ulong dropped_before = state->metrics.feedback_results_dropped_cnt;
  fd_bam_bundle_result_t stale = test_make_bundle_result( 730U, 1730UL, 1U );
  stale.scheduler_gen = old_gen;
  fd_bam_enqueue_result( state, &stale );
  FD_TEST( state->feedback_queue_depth == 0UL );
  FD_TEST( state->metrics.feedback_results_dropped_cnt == dropped_before+1UL );

  test_bam_env_mock_conn_empty( env );
  test_bam_env_mock_h2_hs( state );
  state->bam_stream_live = 1U;
  state->bam_last_builder_activity_ns = fd_bam_now();
  state->bam_last_validator_heartbeat_ns = fd_bam_now();

  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  resp.has_bam_config = true;
  resp.bam_config.has_tpu_sock = true;
  fd_cstr_ncpy( resp.bam_config.tpu_sock.ip, "7.7.7.7", sizeof( resp.bam_config.tpu_sock.ip ) );
  resp.bam_config.tpu_sock.port = 7000U;
  resp.bam_config.has_tpu_fwd_sock = false;

  uchar pb_buf[ 256 ];
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
  state->bam_config_inflight = 1U;
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );

  FD_TEST( state->bam_config_received == 1U );
  FD_TEST( state->bam_tpu.l == 0UL );
  FD_TEST( state->bam_tpu_fwd.l == 0UL );
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY );

  state->keyswitch = NULL;
  test_bam_env_destroy( env );
}

/* --- Config and fee propagation ----------------------------------------------------- */

static void
test_bam_config_updates_contact_info( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  FD_TEST( state->bam_tpu.addr == 0U );
  FD_TEST( state->bam_tpu.port == 0U );
  FD_TEST( state->bam_tpu_fwd.addr == 0U );
  FD_TEST( state->bam_tpu_fwd.port == 0U );

  /* Initial config populates TPU endpoints and fee recipient. */
  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  resp.has_bam_config = true;
  resp.bam_config.has_tpu_sock = true;
  fd_cstr_ncpy( resp.bam_config.tpu_sock.ip, "1.2.3.4", sizeof( resp.bam_config.tpu_sock.ip ) );
  resp.bam_config.tpu_sock.port = 9000U;
  resp.bam_config.has_tpu_fwd_sock = true;
  fd_cstr_ncpy( resp.bam_config.tpu_fwd_sock.ip, "5.6.7.8", sizeof( resp.bam_config.tpu_fwd_sock.ip ) );
  resp.bam_config.tpu_fwd_sock.port = 10001U;
  uchar prio_fee_raw[ 32 ];
  char const * prio_fee_b58 = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d";
  FD_TEST( fd_base58_decode_32( prio_fee_b58, prio_fee_raw ) );
  fd_cstr_ncpy( resp.bam_config.prio_fee_recipient_pubkey, prio_fee_b58, sizeof( resp.bam_config.prio_fee_recipient_pubkey ) );
  resp.bam_config.commission_bps = 2750U;

  uchar pb_buf[ 256 ];
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );

  FD_TEST( state->bam_tpu.addr != 0U );

  uint expected_tpu_addr = 0U;
  uint expected_tpu_fwd_addr = 0U;
  FD_TEST( fd_cstr_to_ip4_addr( "1.2.3.4", &expected_tpu_addr ) );
  FD_TEST( fd_cstr_to_ip4_addr( "5.6.7.8", &expected_tpu_fwd_addr ) );
  FD_TEST( state->bam_tpu.addr == expected_tpu_addr );
  FD_TEST( fd_ushort_bswap( state->bam_tpu.port ) == 9000U );

  FD_TEST( state->bam_tpu_fwd.addr == expected_tpu_fwd_addr );
  FD_TEST( fd_ushort_bswap( state->bam_tpu_fwd.port ) == 10001U );
  FD_TEST( state->prio_fee_recipient_set == 1U );
  FD_TEST( state->commission_bps == 2750 );
  FD_TEST( 0 == memcmp( state->prio_fee_recipient, prio_fee_raw, sizeof( prio_fee_raw ) ) );

  ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );
  FD_TEST( state->bam_tpu.addr == expected_tpu_addr );
  FD_TEST( fd_ushort_bswap( state->bam_tpu.port ) == 9000U );
  FD_TEST( state->bam_tpu_fwd.addr == expected_tpu_fwd_addr );
  FD_TEST( fd_ushort_bswap( state->bam_tpu_fwd.port ) == 10001U );

  /* Invalid forward socket should preserve the last valid BAM contact. */
  resp.bam_config.has_tpu_fwd_sock = false;
  fd_memset( resp.bam_config.tpu_fwd_sock.ip, 0, sizeof( resp.bam_config.tpu_fwd_sock.ip ) );
  resp.bam_config.tpu_fwd_sock.port = 0U;
  ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );
  FD_TEST( state->bam_tpu.addr == expected_tpu_addr );
  FD_TEST( fd_ushort_bswap( state->bam_tpu.port ) == 9000U );
  FD_TEST( state->bam_tpu_fwd.addr == expected_tpu_fwd_addr );
  FD_TEST( fd_ushort_bswap( state->bam_tpu_fwd.port ) == 10001U );

  /* Missing both sockets still leaves the stored BAM contact unchanged. */
  resp.bam_config.has_tpu_sock = false;
  fd_memset( resp.bam_config.tpu_sock.ip, 0, sizeof( resp.bam_config.tpu_sock.ip ) );
  resp.bam_config.tpu_sock.port = 0U;
  ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
  fd_bam_client_grpc_rx_msg( state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );
  FD_TEST( state->bam_tpu.addr == expected_tpu_addr );
  FD_TEST( fd_ushort_bswap( state->bam_tpu.port ) == 9000U );
  FD_TEST( state->bam_tpu_fwd.addr == expected_tpu_fwd_addr );
  FD_TEST( fd_ushort_bswap( state->bam_tpu_fwd.port ) == 10001U );

  test_bam_env_destroy( env );
}

static void
test_bam_impl_fee_cfg_propagates_to_pack( fd_wksp_t * wksp ) {
  /* Implementation-specific regression: BAM currently mirrors fee fields into
     shared memory that the pack/crank path consumes. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * bam_state = env->state;

  FD_TEST( bam_state->fee_cfg != NULL );
  fd_bam_fee_cfg_t * shared_cfg = bam_state->fee_cfg;
  fd_memset( shared_cfg, 0, sizeof(fd_bam_fee_cfg_t) );

  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  resp.has_bam_config = true;
  resp.bam_config.has_tpu_sock = true;
  fd_cstr_ncpy( resp.bam_config.tpu_sock.ip, "1.1.1.1", sizeof( resp.bam_config.tpu_sock.ip ) );
  resp.bam_config.tpu_sock.port = 8000U;
  resp.bam_config.has_tpu_fwd_sock = true;
  fd_cstr_ncpy( resp.bam_config.tpu_fwd_sock.ip, "2.2.2.2", sizeof( resp.bam_config.tpu_fwd_sock.ip ) );
  resp.bam_config.tpu_fwd_sock.port = 9000U;
  uchar prio_fee_raw[ 32 ];
  char const * prio_fee_b58 = "4uhcVJyU9pJkvQyS88uRDiswHXSCkY3zQawwpjk2NsNY";
  FD_TEST( fd_base58_decode_32( prio_fee_b58, prio_fee_raw ) );
  fd_cstr_ncpy( resp.bam_config.prio_fee_recipient_pubkey, prio_fee_b58, sizeof( resp.bam_config.prio_fee_recipient_pubkey ) );
  resp.bam_config.commission_bps = 3500U;

  uchar pb_buf[ 256 ];
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) );
  fd_bam_client_grpc_rx_msg( bam_state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );

  FD_TEST( shared_cfg->version == 1UL );
  FD_TEST( shared_cfg->has_prio_fee_recipient == 1U );
  FD_TEST( shared_cfg->commission_bps == 3500U );
  FD_TEST( 0 == memcmp( shared_cfg->prio_fee_recipient, prio_fee_raw, sizeof( prio_fee_raw ) ) );

  fd_bundle_crank_gen_t crank_gen_mem[1];
  fd_memset( crank_gen_mem, 0, sizeof( crank_gen_mem ) );
  fd_bundle_crank_gen_t * crank_gen = crank_gen_mem;
  ulong pack_cfg_version = 0UL;
  int crank_enabled = 1;

  fd_memset( crank_gen->crank3->new_tip_receiver, 0xAA, sizeof( crank_gen->crank3->new_tip_receiver ) );
  fd_memset( crank_gen->crank2->new_tip_receiver, 0xBB, sizeof( crank_gen->crank2->new_tip_receiver ) );
  crank_gen->crank3->init_tip_distribution_acct.commission_bps = (ushort)777U;

  /* New BAM fee config should propagate into both cranks and bump version. */
  fd_pack_apply_bam_fee_cfg_impl( shared_cfg,
                                  &pack_cfg_version,
                                  crank_enabled,
                                  crank_gen->crank3,
                                  crank_gen->crank2 );

  FD_TEST( pack_cfg_version == shared_cfg->version );
  FD_TEST( crank_gen->crank3->init_tip_distribution_acct.commission_bps == 3500U );
  FD_TEST( 0 == memcmp( crank_gen->crank3->new_tip_receiver, prio_fee_raw, sizeof( prio_fee_raw ) ) );
  FD_TEST( 0 == memcmp( crank_gen->crank2->new_tip_receiver, prio_fee_raw, sizeof( prio_fee_raw ) ) );

  uchar sentinel3[32];
  uchar sentinel2[32];
  fd_memset( sentinel3, 0xCC, sizeof( sentinel3 ) );
  fd_memset( sentinel2, 0xDD, sizeof( sentinel2 ) );
  fd_memcpy( crank_gen->crank3->new_tip_receiver, sentinel3, sizeof( sentinel3 ) );
  fd_memcpy( crank_gen->crank2->new_tip_receiver, sentinel2, sizeof( sentinel2 ) );
  crank_gen->crank3->init_tip_distribution_acct.commission_bps = (ushort)1234U;

  /* Without a version bump pack tile should ignore the shared config. */
  shared_cfg->commission_bps = 9000U;
  fd_pack_apply_bam_fee_cfg_impl( shared_cfg,
                                  &pack_cfg_version,
                                  crank_enabled,
                                  crank_gen->crank3,
                                  crank_gen->crank2 );
  FD_TEST( pack_cfg_version == 1UL );
  FD_TEST( crank_gen->crank3->init_tip_distribution_acct.commission_bps == 1234U );
  FD_TEST( 0 == memcmp( crank_gen->crank3->new_tip_receiver, sentinel3, sizeof( sentinel3 ) ) );
  FD_TEST( 0 == memcmp( crank_gen->crank2->new_tip_receiver, sentinel2, sizeof( sentinel2 ) ) );

  /* Second config update bumps version and replaces fee settings everywhere. */
  bam_api_ConfigResponse resp_update = bam_api_ConfigResponse_init_default;
  resp_update.has_bam_config = true;
  resp_update.bam_config.commission_bps = 15000U;
  uchar prio_fee_raw2[ 32 ];
  char const * prio_fee_b58_2 = "EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG";
  FD_TEST( fd_base58_decode_32( prio_fee_b58_2, prio_fee_raw2 ) );
  fd_cstr_ncpy( resp_update.bam_config.prio_fee_recipient_pubkey, prio_fee_b58_2, sizeof( resp_update.bam_config.prio_fee_recipient_pubkey ) );
  ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp_update ) );
  fd_bam_client_grpc_rx_msg( bam_state,
                             pb_buf,
                             ostream.bytes_written,
                             FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );

  FD_TEST( shared_cfg->version == 2UL );
  FD_TEST( shared_cfg->commission_bps == 10000U );
  FD_TEST( shared_cfg->has_prio_fee_recipient == 1U );
  FD_TEST( 0 == memcmp( shared_cfg->prio_fee_recipient, prio_fee_raw2, sizeof( prio_fee_raw2 ) ) );

  fd_pack_apply_bam_fee_cfg_impl( shared_cfg,
                                  &pack_cfg_version,
                                  crank_enabled,
                                  crank_gen->crank3,
                                  crank_gen->crank2 );

  FD_TEST( pack_cfg_version == shared_cfg->version );
  FD_TEST( crank_gen->crank3->init_tip_distribution_acct.commission_bps == 10000U );
  FD_TEST( 0 == memcmp( crank_gen->crank3->new_tip_receiver, prio_fee_raw2, sizeof( prio_fee_raw2 ) ) );
  FD_TEST( 0 == memcmp( crank_gen->crank2->new_tip_receiver, prio_fee_raw2, sizeof( prio_fee_raw2 ) ) );

  test_bam_env_destroy( env );
}

static void
test_bam_builder_fee_info( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  uchar pb_buf[ 256 ];
  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  resp.has_block_engine_config = true;
  uchar pubkey[32] = {1,2,3,4,5};
  FD_TEST( fd_base58_encode_32( pubkey, NULL, resp.block_engine_config.builder_pubkey ) );
  resp.block_engine_config.builder_pubkey[ FD_BASE58_ENCODED_32_SZ-1 ] = '\0';
  resp.block_engine_config.builder_commission = 5U;
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  if( FD_UNLIKELY( !pb_encode( &ostream, bam_api_ConfigResponse_fields, &resp ) ) ) {
    FD_LOG_ERR(( "pb_encode fee info failed: %s", PB_GET_ERROR( &ostream ) ));
  }

  FD_TEST( state->builder_info_valid_until == 0L );
  fd_bam_client_grpc_rx_msg( state, pb_buf, ostream.bytes_written, FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );
  FD_TEST( state->builder_info_valid_until != 0L );
  FD_TEST( state->builder_commission == 5U );
  uchar decoded[32];
  FD_TEST( fd_base58_decode_32( resp.block_engine_config.builder_pubkey, decoded ) );
  FD_TEST( 0 == memcmp( state->builder_pubkey, decoded, 32UL ) );

  /* Invalid block_engine_config values must not partially update builder state. */
  uchar prev_builder_pubkey[ 32 ];
  fd_memcpy( prev_builder_pubkey, state->builder_pubkey, sizeof(prev_builder_pubkey) );
  uchar prev_builder_commission = state->builder_commission;
  long prev_builder_valid_until = state->builder_info_valid_until;

  bam_api_ConfigResponse bad_pubkey_resp = bam_api_ConfigResponse_init_default;
  bad_pubkey_resp.has_block_engine_config = true;
  fd_memset( bad_pubkey_resp.block_engine_config.builder_pubkey, '1', FD_BASE58_ENCODED_32_LEN );
  bad_pubkey_resp.block_engine_config.builder_pubkey[ FD_BASE58_ENCODED_32_LEN ] = '\0';
  bad_pubkey_resp.block_engine_config.builder_commission = 7U;

  ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  if( FD_UNLIKELY( !pb_encode( &ostream, bam_api_ConfigResponse_fields, &bad_pubkey_resp ) ) ) {
    FD_LOG_ERR(( "pb_encode bad fee info failed: %s", PB_GET_ERROR( &ostream ) ));
  }
  fd_bam_client_grpc_rx_msg( state, pb_buf, ostream.bytes_written, FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );

  FD_TEST( 0 == memcmp( state->builder_pubkey, prev_builder_pubkey, sizeof(prev_builder_pubkey) ) );
  FD_TEST( state->builder_commission == prev_builder_commission );
  FD_TEST( state->builder_info_valid_until == prev_builder_valid_until );

  bam_api_ConfigResponse bad_commission_resp = bam_api_ConfigResponse_init_default;
  bad_commission_resp.has_block_engine_config = true;
  uchar bad_commission_pubkey[32] = {9,8,7,6,5};
  FD_TEST( fd_base58_encode_32( bad_commission_pubkey, NULL, bad_commission_resp.block_engine_config.builder_pubkey ) );
  bad_commission_resp.block_engine_config.builder_pubkey[ FD_BASE58_ENCODED_32_SZ-1 ] = '\0';
  bad_commission_resp.block_engine_config.builder_commission = 101U;

  ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  if( FD_UNLIKELY( !pb_encode( &ostream, bam_api_ConfigResponse_fields, &bad_commission_resp ) ) ) {
    FD_LOG_ERR(( "pb_encode bad commission fee info failed: %s", PB_GET_ERROR( &ostream ) ));
  }
  fd_bam_client_grpc_rx_msg( state, pb_buf, ostream.bytes_written, FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );

  FD_TEST( 0 == memcmp( state->builder_pubkey, prev_builder_pubkey, sizeof(prev_builder_pubkey) ) );
  FD_TEST( state->builder_commission == prev_builder_commission );
  FD_TEST( state->builder_info_valid_until == prev_builder_valid_until );

  test_bam_env_destroy( env );
}

/* --- Bundle result durability ------------------------------------------------------- */

/* Verifies that bundle results buffered in the queue survive fd_bam_client_reset
 * and flush after reconnect. The test simulates transport drain between flush
 * attempts because the gRPC client only accepts another stream DATA frame once
 * the previous frame leaves the TX ring. */

static void
test_bam_bundle_result_queue_flushes_after_reconnect( fd_wksp_t * wksp ) {
  /* Client reset should preserve queued bundle results, and the next scheduler
     stream should flush them in FIFO order. */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  fd_bam_tile_t * state = env->state;

  for( uint i=0; i<2; i++ ) {
    fd_bam_bundle_result_t res = test_make_bundle_result( 200 + i, 1200, 2 );
    test_enqueue_bundle_result( state, &res );
  }

  ulong expected_tail = state->bam_results_tail;
  fd_bam_client_reset( state );
  FD_TEST( state->feedback_queue_depth == 2UL );
  FD_TEST( state->bam_results_head == 0UL );
  FD_TEST( state->bam_results_tail == expected_tail );
  FD_TEST( state->bam_results[0].seq_id == 200U );
  FD_TEST( state->bam_results[1].seq_id == 201U );

  test_bam_env_mock_conn( env );
  state->bam_stream_live = 0U;

  fd_grpc_h2_stream_t * stream = fd_grpc_client_stream_acquire( state->grpc_client, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( stream );
  stream->hdrs.h2_status     = 200;
  stream->hdrs.is_grpc_proto = 1;
  state->bam_stream = stream;
  state->bam_stream_live = 1U;
  state->grpc_client->request_stream = NULL;
  *state->grpc_client->request_tx_op = (fd_h2_tx_op_t){0};

  test_bam_decoded_message_t decoded;
  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  FD_TEST( decoded.multi.results[0].seq_id == 200U );
  FD_TEST( state->feedback_queue_depth == 1UL );

  FD_TEST( fd_bam_test_flush_results( state ) == 1 );
  test_bam_decode_last_message( state, &decoded );
  FD_TEST( decoded.msg.versioned_msg.v0.which_msg == bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
  FD_TEST( decoded.multi.result_cnt == 1UL );
  FD_TEST( decoded.multi.results[0].seq_id == 201U );
  FD_TEST( state->feedback_queue_depth == 0UL );
  FD_TEST( state->bam_results_head == state->bam_results_tail );

  test_bam_env_destroy( env );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );
  fd_metrics_register( (ulong *)fd_metrics_new( metrics_scratch, 0UL ) );

  ulong cpu_idx = fd_tile_cpu_id( fd_tile_idx() );
  if( cpu_idx > fd_shmem_cpu_cnt() ) cpu_idx = 0UL;

  char const * _page_sz = fd_env_strip_cmdline_cstr ( &argc, &argv, "--page-sz",     NULL, "normal"                     );
  ulong        page_cnt = fd_env_strip_cmdline_ulong( &argc, &argv, "--page-cnt",    NULL, 2048UL                       );
  ulong        numa_idx = fd_env_strip_cmdline_ulong( &argc, &argv, "--numa-idx",    NULL, fd_shmem_numa_idx( cpu_idx ) );

  fd_wksp_t * wksp = fd_wksp_new_anonymous( fd_cstr_to_shmem_page_sz( _page_sz ), page_cnt, fd_shmem_cpu_idx( numa_idx ), "bam-test", 16UL );
  FD_TEST( wksp );

  /* Scheduler ingestion/validation */
  test_bam_packets_forwarded( wksp );
  test_bam_atomic_batch_sets_internal_bundle_metadata( wksp );
  test_bam_slot_ingress_timing_tracks_max_schedule_slot_and_rejects_stale_arrival( wksp );
  test_bam_current_slot_work_status_bits( wksp );
  test_bam_slot_ingress_timing_tracks_hash_collisions( wksp );
  test_bam_publish_batch_uses_captured_ingress_metadata( wksp );
  test_bam_multiple_batches_forwarded( wksp );
  test_bam_independent_batches_queue_and_drain_in_order( wksp );
  test_bam_pending_over_stem_burst_drains_in_multiple_passes( wksp );
  test_bam_queue_full_rejects_whole_batch_without_partial_enqueue( wksp );
  test_bam_multiple_batches_accept_full_txn_count( wksp, FD_BAM_FULL_ATOMIC_BATCHES_PER_STEM_BURST, 700U, 100UL );
  test_bam_multiple_batches_accept_full_txn_count( wksp, FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE, 1500U, 500UL );
  test_bam_scheduler_truncated_message_dropped( wksp );
  test_bam_scheduler_trailing_corruption_does_not_publish( wksp );
  test_bam_scheduler_rejects_invalid_custom_decode_fields( wksp );
  test_bam_scheduler_v0_oneof_uses_last_field( wksp );
  test_bam_multiple_batches_isolate_nested_decode_failures( wksp );
  test_bam_multiple_batches_accept_message_limit( wksp );
  test_bam_multiple_batches_overflow_results_each_excess( wksp );
  test_bam_bundle_forwards_without_builder_info( wksp );
  test_bam_bundle_revert_flag_cases( wksp );
  test_bam_non_revert_multi_packet_forwarded( wksp );
  test_bam_validation_orders_revert_consistency_before_vote_rejection( wksp );
  test_bam_bundle_rejects_real_vote_payload( wksp );
  test_bam_stale_slot_rejects_before_vote_error( wksp );
  test_bam_bundle_rejects_excess_packet_count( wksp );
  test_bam_bundle_uses_data_length_for_size_check( wksp );
  test_bam_bundle_rejects_empty_batch( wksp );
  test_bam_bundle_rejects_missing_batches( wksp );

  /* Connection lifecycle and watchdog */
  test_bam_stream_end_with_pending_txn_reconnects( wksp );
  test_bam_tcp_connect_completion_uses_so_error( wksp );
  test_bam_grpc_end_handling( wksp );
  test_bam_grpc_timeout( wksp );
  test_bam_heartbeat_timeout_forces_disconnect( wksp );
  test_bam_heartbeat_reset_extends_timeout( wksp );
  test_bam_client_status( wksp );

  /* Scheduler/auth messaging */
  test_bam_auth_challenge_response_verifies_cached_identity( wksp );
  test_bam_scheduler_auth_proof_publishes_message( wksp );
  test_bam_scheduler_stream_starts_without_builder_info( wksp );
  test_bam_missing_config_repolls_despite_valid_builder_info( wksp );
  test_bam_missing_config_uses_short_repoll_throttle( wksp );
  test_bam_scheduler_heartbeat_publishes_message( wksp );
  test_bam_scheduler_ping_publishes_message( wksp );
  test_bam_scheduler_leader_state_publishes_message( wksp );
  test_bam_scheduler_stream_replays_only_retained_leader_state( wksp );
  test_bam_leader_state_supersede_counts_drop( wksp );
  test_bam_replay_reset_channel_contract( wksp );
  test_bam_replay_schedule_recheck_slot_policy( wksp );
  test_bam_replay_wait_gate_policy( wksp );
  test_bam_replay_wait_gate_cleared_on_reset_or_disable( wksp );
  test_bam_identity_switch_invalidates_leader_schedule_state( wksp );
  test_bam_replay_schedule_hint_does_not_disconnect( wksp );
  test_bam_pack_leader_channel_contract( wksp );
  test_bam_pack_leader_slot_change_flushes_immediately( wksp );
  test_bam_pack_result_channel_contract( wksp );
  test_bam_bank_result_channel_contract( wksp );
  test_bam_scheduler_result_publishes_message( wksp );
  test_bam_scheduler_result_committed_with_execution_error_publishes_message( wksp );
  test_bam_scheduler_result_not_committed_publishes_message( wksp );
  test_bam_scheduler_result_not_committed_deser_reason( wksp );
  test_bam_scheduler_result_not_committed_transaction_error_reason( wksp );
  test_bam_scheduler_result_not_committed_transaction_error_prefers_non_cancelled( wksp );
  test_bam_scheduler_result_not_committed_all_cancelled_falls_back_to_poh_timeout( wksp );
  test_bam_scheduler_result_not_committed_generic_failure_reason( wksp );

  /* Control surface */
  test_bam_ctrl_updates_url_and_sni( wksp );
  test_bam_ctrl_defaults_bam_ports( wksp );
  test_bam_ctrl_toggle_enable_updates_runtime_state( wksp );
  test_bam_ctrl_enable_from_disabled_start( wksp );
  test_bam_ctrl_enable_from_dormant_then_set_url( wksp );
  test_bam_ctrl_invalid_url_sets_error_and_preserves_config( wksp );
  test_bam_ctrl_blank_url_clears_and_disables( wksp );
  test_bam_admin_rpc_apply_success_caches_default_and_marks_applied( wksp );
  test_bam_admin_rpc_set_failure_stays_pending( wksp );
  test_bam_admin_rpc_revert_uses_cached_defaults( wksp );
  test_bam_admin_rpc_restart_recovers_configured_defaults_for_revert( wksp );
  test_bam_admin_rpc_path_empty_skips_frankendancer_apply( wksp );

  /* Gossip advertisement */
  test_bam_gossip_tile_applies_contact_update( wksp );
  test_bam_gossip_publishes_bam_config_contact( wksp );
  test_bam_activation_waits_for_gossip_fseq( wksp );
  test_bam_activation_waits_for_bundle_publication( wksp );
  test_bam_activation_ignores_stale_gossip_fseq_after_reenable( wksp );
  test_bam_disable_clears_status_with_pending_gossip_handoff( wksp );
  test_bam_invalid_quic_base_port_does_not_activate( wksp );
  test_bam_gossip_resets_when_contact_missing( wksp );
  test_bam_gossip_disconnect_uses_defaults_without_clearing_stored_contact( wksp );
  test_bam_runtime_toggle_updates_gossip( wksp );
  test_bam_shred_update_publishes_receiver_list( wksp );
  test_bam_shred_update_disconnect_uses_empty_without_clearing_receivers( wksp );
  test_bam_config_reuses_cached_contact_for_incomplete_refresh( wksp );
  test_bam_endpoint_change_forgets_cached_contact_before_incomplete_refresh( wksp );

  /* Config and fees */
  test_bam_config_updates_contact_info( wksp );
  test_bam_impl_fee_cfg_propagates_to_pack( wksp );
  test_bam_builder_fee_info( wksp );

  /* Bundle result durability */
  test_bam_bundle_result_queue_flushes_after_reconnect( wksp );

  fd_wksp_usage_t wksp_usage;
  FD_TEST( fd_wksp_usage( wksp, NULL, 0UL, &wksp_usage ) );
  if( wksp_usage.free_cnt!=wksp_usage.total_cnt ) {
    FD_LOG_WARNING(( "wksp leak: free_cnt=%lu total_cnt=%lu", wksp_usage.free_cnt, wksp_usage.total_cnt ));
  }
  FD_TEST( wksp_usage.free_cnt == wksp_usage.total_cnt );

  fd_wksp_delete_anonymous( wksp );

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
