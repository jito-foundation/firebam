#define _GNU_SOURCE

#include "fd_bam_tile.h"
#include "test_bam_common.c"
#include "../../ballet/ed25519/fd_ed25519.h"
#include "../../ballet/sha256/fd_sha256.h"
#include "../../ballet/txn/fd_compact_u16.h"
#include "../../discof/gossip/fd_gossip_tile.c"
#include "../../discof/replay/fd_replay_tile.h"
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <sys/resource.h>

static uchar metrics_scratch[ FD_METRICS_FOOTPRINT( 0UL ) ] __attribute__((aligned( FD_METRICS_ALIGN )));

FD_IMPORT_BINARY( bam_dump_txn_fixture, "src/ballet/txn/fixtures/transaction2.bin" );
/* Encoded by BAM node commit 0b76a8c3d2 with prost::Message::encode_to_vec
   through simple_forwarder::scheduler_response. */
FD_IMPORT_BINARY( bam_scheduler_response_v1_1233, "src/disco/bam/fixtures/bam_scheduler_response_v1_1233.bin" );
FD_IMPORT_BINARY( bam_scheduler_response_v1_4096, "src/disco/bam/fixtures/bam_scheduler_response_v1_4096.bin" );

static fd_bam_parsed_batch_t const empty_parsed_batch;

static ulong
test_bam_build_v1_txn( uchar payload[ static FD_TXN_MTU ],
                       ulong target_sz,
                       uchar signature_seed ) {
  FD_TEST( target_sz>=175UL && target_sz<=FD_TXN_MTU );
  ulong data_sz = target_sz-175UL;
  FD_TEST( data_sz<=USHORT_MAX );

  uchar * p = payload;
  *p++ = 0x80U | FD_TXN_V1;
  *p++ = 1U; /* signature count */
  *p++ = 0U; /* readonly signed accounts */
  *p++ = 1U; /* readonly unsigned accounts: program */
  fd_memset( p, 0, 4UL ); /* transaction config mask */
  p += 4UL;
  fd_memset( p, 3, FD_TXN_BLOCKHASH_SZ );
  p += FD_TXN_BLOCKHASH_SZ;
  *p++ = 1U; /* instruction count */
  *p++ = 2U; /* account count */
  fd_memset( p, 4, FD_TXN_ACCT_ADDR_SZ ); /* fee payer */
  p += FD_TXN_ACCT_ADDR_SZ;
  fd_memset( p, 5, FD_TXN_ACCT_ADDR_SZ ); /* program */
  p += FD_TXN_ACCT_ADDR_SZ;
  *p++ = 1U; /* program id index */
  *p++ = 1U; /* instruction account count */
  *p++ = (uchar)( data_sz       & 0xffUL );
  *p++ = (uchar)((data_sz >> 8) & 0xffUL );
  *p++ = 0U; /* fee payer account index */
  fd_memset( p, 7, data_sz );
  p += data_sz;
  for( ulong i=0UL; i<FD_TXN_SIGNATURE_SZ; i++ ) *p++ = (uchar)( signature_seed+i );
  FD_TEST( (ulong)(p-payload)==target_sz );

  uchar txn_buf[ FD_TXN_MAX_SZ ];
  FD_TEST( fd_txn_parse( payload, target_sz, txn_buf, NULL ) );
  FD_TEST( ((fd_txn_t const *)txn_buf)->transaction_version==FD_TXN_V1 );
  return target_sz;
}

static ulong
test_bam_build_legacy_or_v0_txn( uchar payload[ static FD_TXN_MTU_V0 ],
                                 _Bool v0 ) {
  uchar * p = payload;
  *p++ = 1U; /* signature count */
  fd_memset( p, 9, FD_TXN_SIGNATURE_SZ );
  p += FD_TXN_SIGNATURE_SZ;
  if( FD_UNLIKELY( v0 ) ) *p++ = 0x80U | FD_TXN_V0;
  *p++ = 1U; /* required signatures */
  *p++ = 0U; /* readonly signed accounts */
  *p++ = 1U; /* readonly unsigned accounts: program */
  p += fd_cu16_enc( 2U, p );
  fd_memset( p, 4, FD_TXN_ACCT_ADDR_SZ );
  p += FD_TXN_ACCT_ADDR_SZ;
  fd_memset( p, 5, FD_TXN_ACCT_ADDR_SZ );
  p += FD_TXN_ACCT_ADDR_SZ;
  fd_memset( p, 3, FD_TXN_BLOCKHASH_SZ );
  p += FD_TXN_BLOCKHASH_SZ;
  p += fd_cu16_enc( 1U, p ); /* instruction count */
  *p++ = 1U;                 /* program id index */
  p += fd_cu16_enc( 1U, p ); /* instruction account count */
  *p++ = 0U;                 /* fee payer account index */

  ulong const trailer_sz = (ulong)v0; /* V0 address table lookup count */
  ulong data_sz = FD_TXN_MTU_V0 - (ulong)(p-payload) - 2UL - trailer_sz;
  FD_TEST( data_sz>=128UL && data_sz<16384UL );
  p += fd_cu16_enc( (ushort)data_sz, p );
  fd_memset( p, 7, data_sz );
  p += data_sz;
  if( FD_UNLIKELY( v0 ) ) p += fd_cu16_enc( 0U, p );
  FD_TEST( (ulong)(p-payload)==FD_TXN_MTU_V0 );

  uchar txn_buf[ FD_TXN_MAX_SZ ];
  FD_TEST( fd_txn_parse( payload, FD_TXN_MTU_V0, txn_buf, NULL ) );
  FD_TEST( ((fd_txn_t const *)txn_buf)->transaction_version==( v0 ? FD_TXN_V0 : FD_TXN_VLEGACY ) );
  return FD_TXN_MTU_V0;
}

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
fd_bam_test_receive_ingress_frag_lapped( fd_bam_tile_t * ctx,
                                         ulong           in_idx,
                                         ulong           sig,
                                         ulong           chunk,
                                         ulong           sz,
                                         void *          payload );

extern void
fd_bam_test_metrics_write( fd_bam_tile_t * ctx );

extern void
fd_bam_test_before_credit( fd_bam_tile_t *    ctx,
                           fd_stem_context_t * stem,
                           int *               charge_busy );

extern void
fd_bam_test_check_credit( fd_bam_tile_t *     ctx,
                          fd_stem_context_t * stem,
                          int *               charge_busy,
                          int *               is_backpressured );

extern long
fd_bam_test_next_deadline( fd_bam_tile_t * ctx );

static long g_clock = 1L;
static ulong g_clock_read_cnt;

__attribute__((weak)) long
fd_bam_now( void ) {
  g_clock_read_cnt++;
  return g_clock;
}

/* Points the client at 127.0.0.1:port. */
static void
test_bam_set_server( fd_bam_tile_t * state,
                     ushort          port ) {
  fd_cstr_ncpy( state->server_fqdn, "127.0.0.1", sizeof(state->server_fqdn) );
  state->server_fqdn_len = sizeof("127.0.0.1")-1UL;
  state->server_tcp_port = port;
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

static void
test_bam_check_contact( fd_bam_contact_update_t const * update,
                        fd_ip4_port_t                   tpu,
                        fd_ip4_port_t                   tpu_fwd,
                        ushort                          client_id ) {
  FD_TEST( update->tpu.l==tpu.l );
  FD_TEST( update->tpu_fwd.l==tpu_fwd.l );
  FD_TEST( update->version_client_id==client_id );
}

static fd_bam_shred_update_t
test_bam_read_shred_update( fd_wksp_t * mem,
                            ulong       chunk ) {
  fd_bam_shred_update_t msg;
  fd_memcpy( &msg, fd_chunk_to_laddr( mem, chunk ), sizeof(fd_bam_shred_update_t) );
  return msg;
}

/* An out link on the env's dcache with the given mtu. */
static fd_bam_out_ctx_t
test_bam_out( test_bam_env_t * env,
              ulong            mtu ) {
  fd_wksp_t * mem    = fd_wksp_containing( env->out_dcache );
  ulong       chunk0 = fd_dcache_compact_chunk0( mem, env->out_dcache );
  return (fd_bam_out_ctx_t){ .idx=0UL, .mem=mem, .chunk0=chunk0, .chunk=chunk0,
                             .wmark=fd_dcache_compact_wmark( mem, env->out_dcache, mtu ) };
}

static fd_wksp_t *
test_bam_setup_gossip_out( test_bam_env_t * env ) {
  env->state->gossip_out = test_bam_out( env, FD_TPU_PARSED_MTU );
  return env->state->gossip_out.mem;
}

/* The tile only queries and updates its fseqs, so plain words stand in
   for them in these tests. */
typedef struct {
  ulong   mem[ 2 ];
  ulong * status;
  ulong * signed_ack;
} test_bam_handoff_fseqs_t;

static void
test_bam_setup_handoff_fseqs( test_bam_env_t *           env,
                              test_bam_handoff_fseqs_t * fseqs ) {
  fd_memset( fseqs, 0, sizeof(test_bam_handoff_fseqs_t) );
  fseqs->status     = env->state->bam_status_fseq        = &fseqs->mem[ 0 ];
  fseqs->signed_ack = env->state->bam_gossip_signed_fseq = &fseqs->mem[ 1 ];
}

/* ip:port in network order, as the tile stores it. */
static fd_ip4_port_t
test_bam_ip4_port( char const * ip,
                   ushort       port ) {
  fd_ip4_port_t addr = { .port = fd_ushort_bswap( port ) };
  FD_TEST( fd_cstr_to_ip4_addr( ip, &addr.addr ) );
  return addr;
}

static void
test_bam_set_bam_tpu( fd_bam_tile_t * state,
                      char const *    tpu_ip,
                      ushort          tpu_port,
                      char const *    fwd_ip,
                      ushort          fwd_port ) {
  state->bam_tpu     = test_bam_ip4_port( tpu_ip, tpu_port );
  state->bam_tpu_fwd = test_bam_ip4_port( fwd_ip, fwd_port );
}

/* The tile's default (non-BAM) contact. */
static void
test_bam_set_default_tpu( fd_bam_tile_t * state ) {
  state->default_tpu     = test_bam_ip4_port( "1.1.1.1", 4242U );
  state->default_tpu_fwd = test_bam_ip4_port( "2.2.2.2", 4343U );
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
test_bam_encode_one_packet_atomic_batch_raw( uint32_t seq_id,
                                             uchar    byte,
                                             uchar *  out,
                                             size_t   out_sz ) {
  bam_types_Packet packet = bam_types_Packet_init_default;
  packet.data.size     = 1U;
  packet.data.bytes[0] = byte;
  test_bam_packet_encode_ctx_t packets_ctx = { .packets = &packet, .packet_cnt = 1UL };
  bam_types_AtomicTxnBatch batch = bam_types_AtomicTxnBatch_init_default;
  batch.seq_id               = seq_id;
  batch.max_schedule_slot    = 1UL;
  batch.packets.funcs.encode = test_bam_encode_packets_cb;
  batch.packets.arg          = &packets_ctx;
  pb_ostream_t ostream = pb_ostream_from_buffer( out, out_sz );
  FD_TEST( pb_encode( &ostream, bam_types_AtomicTxnBatch_fields, &batch ) );
  return ostream.bytes_written;
}

static size_t
test_bam_encode_multiple_atomic_batch_raw( uchar const * const * batches,
                                           size_t const *         batch_sz,
                                           size_t                 batch_cnt,
                                           uchar *                out,
                                           size_t                 out_sz ) {
  test_bam_raw_batch_encode_ctx_t batches_ctx = { .batches = batches, .batch_sz = batch_sz, .batch_cnt = batch_cnt };
  bam_types_MultipleAtomicTxnBatch multi = bam_types_MultipleAtomicTxnBatch_init_default;
  multi.batches.funcs.encode = test_bam_encode_raw_batches_cb;
  multi.batches.arg          = &batches_ctx;
  pb_ostream_t ostream = pb_ostream_from_buffer( out, out_sz );
  FD_TEST( pb_encode( &ostream, bam_types_MultipleAtomicTxnBatch_fields, &multi ) );
  return ostream.bytes_written;
}

static size_t
test_bam_encode_one_packet_multi_payload_raw( uint32_t seq_id,
                                              uchar    byte,
                                              uchar *  out,
                                              size_t   out_sz ) {
  uchar batch[256];
  size_t batch_sz = test_bam_encode_one_packet_atomic_batch_raw( seq_id, byte, batch, sizeof( batch ) );
  uchar const * batches[] = { batch };
  return test_bam_encode_multiple_atomic_batch_raw( batches, &batch_sz, 1UL, out, out_sz );
}

static size_t
test_bam_encode_heartbeat_payload_raw( ulong  time_sent_microseconds,
                                       uchar * out,
                                       size_t out_sz ) {
  bam_types_BuilderHeartBeat hb = bam_types_BuilderHeartBeat_init_default;
  hb.time_sent_microseconds = time_sent_microseconds;
  pb_ostream_t ostream = pb_ostream_from_buffer( out, out_sz );
  FD_TEST( pb_encode( &ostream, bam_types_BuilderHeartBeat_fields, &hb ) );
  return ostream.bytes_written;
}

static size_t
test_bam_encode_scheduler_response_v0_raw( uchar const * v0_payload,
                                           size_t        v0_payload_sz,
                                           uchar *       out,
                                           size_t        out_sz ) {
  pb_ostream_t ostream = pb_ostream_from_buffer( out, out_sz );
  test_bam_encode_string_field_raw( &ostream, bam_api_SchedulerResponse_v0_tag, v0_payload, v0_payload_sz );
  return ostream.bytes_written;
}

static size_t
test_bam_encode_scheduler_multi_payload_raw( uchar const * multi_payload,
                                             size_t        multi_payload_sz,
                                             uchar *       out,
                                             size_t        out_sz ) {
  uchar v0_payload[ 8192 ];
  pb_ostream_t v0_stream = pb_ostream_from_buffer( v0_payload, sizeof(v0_payload) );
  test_bam_encode_string_field_raw( &v0_stream, bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag,
                                    multi_payload, multi_payload_sz );
  return test_bam_encode_scheduler_response_v0_raw( v0_payload, v0_stream.bytes_written, out, out_sz );
}

/* A SchedulerResponse whose MultipleAtomicTxnBatch carries the given
   encoded AtomicTxnBatch payloads. */
static size_t
test_bam_encode_scheduler_multi_batch_response_raw( uchar const * const * batches,
                                                    size_t const *         batch_sz,
                                                    size_t                 batch_cnt,
                                                    uchar *                out,
                                                    size_t                 out_sz ) {
  uchar multi[ 8192 ];
  size_t multi_sz = test_bam_encode_multiple_atomic_batch_raw( batches, batch_sz, batch_cnt, multi, sizeof(multi) );
  return test_bam_encode_scheduler_multi_payload_raw( multi, multi_sz, out, out_sz );
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
test_bam_send_scheduler_bundle( fd_bam_tile_t * state,
                                uint32_t        seq_id,
                                _Bool           revert_on_error ) {
  uchar protobuf[256];
  size_t protobuf_sz = test_bam_build_scheduler_batch_msg( protobuf, sizeof(protobuf), seq_id, 2, revert_on_error);
  test_bam_rx_sched( state, protobuf, protobuf_sz );
}

/* Checks the i-th frag published to verify is a BAM transaction with
   the given batch metadata and returns it. */
static fd_txn_m_t const *
test_bam_check_published( test_bam_env_t * env,
                          ulong            i,
                          uint             seq_id,
                          uint             txn_cnt,
                          uint             batch_idx,
                          uint             revert_on_error,
                          ulong            bundle_id,
                          ulong            bundle_txn_cnt ) {
  FD_TEST( env->out_mcache[ i ].seq==i );
  fd_txn_m_t const * txnm = fd_chunk_to_laddr_const( env->state->verify_out.mem, env->out_mcache[ i ].chunk );
  FD_TEST( txnm->source_tpu==FD_TXN_M_TPU_SOURCE_BAM );
  FD_TEST( txnm->bam.seq_id==seq_id && txnm->bam.txn_cnt==txn_cnt );
  FD_TEST( txnm->bam.batch_idx==batch_idx && txnm->bam.revert_on_error==revert_on_error );
  FD_TEST( txnm->block_engine.bundle_id==bundle_id && txnm->block_engine.bundle_txn_cnt==bundle_txn_cnt );
  return txnm;
}

/* --- Scheduler ingestion and validation ----------------------------------------------- */

/* Verify that scheduler batches without revert_on_error are fanned out
 * as individual bundle-sourced transactions with fragment metadata.
 */
static void
test_bam_packets_forwarded( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  uchar protobuf[256];
  size_t protobuf_sz = test_bam_build_scheduler_batch_msg( protobuf, sizeof(protobuf), 0U, 1, 0);

  FD_TEST( state->metrics.transaction_published_cnt == 0UL );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );

  test_bam_rx_sched( state, protobuf, protobuf_sz );

  FD_TEST( state->metrics.transaction_published_cnt == 0UL );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 1UL );
  FD_TEST( env->stem_seqs[0] == 0UL );

  ulong fseq[1] = { 0UL };
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
  FD_TEST( opt_poll_in == 1 );
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
  FD_TEST( first->txn_t_sz == 0U );
  FD_TEST( first->block_engine.bundle_id == 0UL );
  FD_TEST( first->block_engine.bundle_txn_cnt == 0UL );
  FD_TEST( first->first_seen_nanos == g_clock );

  test_bam_env_destroy( env );
}

static void
test_bam_slot_ingress_timing_tracks_max_schedule_slot_and_rejects_stale_arrival( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

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
  test_bam_rx_sched( state, protobuf, protobuf_sz );
  FD_TEST( test_bam_env_drain_pending_txns( env ) == 1UL );

  fd_bam_slot_ingress_timing_t const * entry = fd_bam_slot_ingress_timing_query_const( state, 100UL );
  FD_TEST( entry );
  FD_TEST( entry->first_rx_ts_ns == 1000L );
  FD_TEST( entry->slot_end_ns == 1500L );
  FD_TEST( entry->first_rx_after_slot_end == 0U );
  FD_TEST( entry->txn_before_slot_end == 1UL );
  FD_TEST( entry->txn_after_slot_end == 0UL );

  /* The states gauge holds the state's value, not a one-hot fan-out. */
  fd_metrics_register( (ulong *)fd_metrics_new( metrics_scratch, 0UL ) );
  fd_bam_test_metrics_write( state );
  FD_TEST( FD_MGAUGE_GET( BAM, LEADER_SLOT_FIRST_INGRESS_STATE )==FD_METRICS_ENUM_BAM_LEADER_SLOT_FIRST_INGRESS_STATE_V_BEFORE_END_IDX );

  batch.seq_id = 78U;
  batch.max_schedule_slot = 100UL;
  state->bam_leader_state.slot        = 101UL;
  state->bam_leader_state.slot_end_ns = 2500L;
  g_clock = 2000L;
  protobuf_sz = test_bam_encode_scheduler_multi_batch_response( &batch, 1UL, protobuf, sizeof(protobuf) );
  test_bam_rx_sched( state, protobuf, protobuf_sz );
  fd_bam_test_metrics_write( state );
  FD_TEST( FD_MGAUGE_GET( BAM, LEADER_SLOT_FIRST_INGRESS_STATE )==FD_METRICS_ENUM_BAM_LEADER_SLOT_FIRST_INGRESS_STATE_V_NO_INGRESS_IDX );

  entry = fd_bam_slot_ingress_timing_query_const( state, 100UL );
  FD_TEST( entry );
  FD_TEST( entry->first_rx_ts_ns == 1000L );
  FD_TEST( entry->slot_end_ns == 1500L );
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
  test_bam_rx_sched( state, protobuf, protobuf_sz );

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
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn_empty( env );
  test_bam_env_mock_h2_hs( state );
  test_bam_env_inject_config_response( state );

  long now = fd_bam_now();
  state->bam_last_builder_activity_ns    = now;
  state->bam_builder_heartbeat_received  = 1U;
  state->bam_stream_live                 = 1U;
  state->bam_status_recent               = FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY;

  fd_keyswitch_t keyswitch = {0};
  keyswitch.state = FD_KEYSWITCH_STATE_COMPLETED;
  state->keyswitch = &keyswitch;

  ulong fseq[1] = { 0UL };
  state->bam_status_fseq = fseq;

  state->bam_leader_state = (fd_bam_leader_state_t){
    .slot               = 42UL,
    .slot_end_ns        = fd_log_wallclock() - 1L,
    .current_slot_has_bam_work = 0U,
  };
  state->epoch_slot0 = 1000UL; state->epoch_slot_cnt = 100UL; /* no leader slot known */
  state->bam_stream_live = 0U;
  fd_bam_tile_housekeeping( state );
  FD_TEST( !fd_bam_tile_leader_slot_known( state ) ); /* not admitted while unhealthy */
  state->bam_stream_live = 1U;
  fd_bam_tile_housekeeping( state );
  FD_TEST( fd_fseq_query( fseq ) == FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
  FD_TEST( fd_bam_tile_leader_slot_known( state ) ); /* admitted this epoch */
  state->epoch_slot0 = 1100UL;
  FD_TEST( !fd_bam_tile_leader_slot_known( state ) ); /* until healthy in the next */
  fd_bam_test_metrics_write( state );
  FD_TEST( FD_MGAUGE_GET( BAM, IS_HEALTHY ) == 1UL );
  FD_TEST( FD_MGAUGE_GET( BAM, IS_STREAM_LIVE ) == 1UL );
  FD_TEST( FD_MGAUGE_GET( BAM, LEADER_STATE_SLOT ) == 42UL );
  FD_TEST( FD_MGAUGE_GET( BAM, LEADER_STATE_TICK ) == 0UL );
  FD_TEST( FD_MGAUGE_GET( BAM, LEADER_STATE_SLOT_END_TIME ) == (ulong)state->bam_leader_state.slot_end_ns );

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
  FD_TEST( FD_MGAUGE_GET( BAM, LEADER_STATE_SLOT_END_TIME ) == (ulong)state->bam_leader_state.slot_end_ns );

  test_bam_env_destroy( env );
}

/* Overruns of the result links (in 1-3 here) count as dropped results. */

static void
test_bam_feedback_results_overrun_dropped( fd_wksp_t * wksp ) {
  static uchar mem[ FD_METRICS_FOOTPRINT( 4UL ) ] __attribute__((aligned( FD_METRICS_ALIGN )));
  ulong * metrics = (ulong *)fd_metrics_new( mem, 4UL );
  fd_metrics_register( metrics );
  for( ulong i=0UL; i<4UL; i++ ) {
    fd_metrics_link_in( metrics, i )[ FD_METRICS_COUNTER_LINK_FRAG_POLLING_OVERRUN_OFF ] =  10UL<<i;
    fd_metrics_link_in( metrics, i )[ FD_METRICS_COUNTER_LINK_FRAG_READING_OVERRUN_OFF ] = 100UL<<i;
  }
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  state->metrics.feedback_results_dropped_cnt = 1UL;
  state->bank_bam_in_idx = 1UL; state->bank_bam_in_cnt = 2UL; state->pack_bam_result_in_idx = 3UL;
  fd_bam_test_metrics_write( state );
  FD_TEST( FD_MCNT_GET( BAM, FEEDBACK_RESULT_DROPPED )==1UL+110UL*(2UL+4UL+8UL) );
  test_bam_env_destroy( env );
  fd_metrics_register( (ulong *)fd_metrics_new( metrics_scratch, 0UL ) );
}

/* bam_grpc_stream_open follows open HTTP/2 streams: it rises when a
   request opens a stream and returns to zero when a reset closes it. */

static void
test_bam_grpc_stream_open_gauge( fd_wksp_t * wksp ) {
  fd_metrics_register( (ulong *)fd_metrics_new( metrics_scratch, 0UL ) );

  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );

  fd_bam_test_metrics_write( state );
  FD_TEST( FD_MGAUGE_GET( BAM, GRPC_STREAM_OPEN )==0UL );

  /* No auth yet, so the step starts the auth challenge request. */
  long now = fd_bam_now();
  test_bam_keepalive_sync( state, now );
  FD_TEST( fd_bam_client_step_reconnect( state, now )==1 );
  FD_TEST( state->bam_auth_inflight );
  fd_bam_test_metrics_write( state );
  FD_TEST( FD_MGAUGE_GET( BAM, GRPC_STREAM_OPEN )==1UL );

  fd_bam_client_reset( state );
  fd_bam_test_metrics_write( state );
  FD_TEST( FD_MGAUGE_GET( BAM, GRPC_STREAM_OPEN )==0UL );

  test_bam_env_destroy( env );
}

static void
test_bam_slot_ingress_timing_tracks_hash_collisions( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  fd_bam_batch_ctx_t batch_state;
  fd_memset( &batch_state, 0, sizeof(batch_state) );
  batch_state.packet_cnt = 1U;
  batch_state.packets[ 0 ].payload    = bam_dump_txn_fixture;
  batch_state.packets[ 0 ].payload_sz = (ushort)bam_dump_txn_fixture_sz;

  state->bam_leader_state.slot = 228UL;

  bam_types_AtomicTxnBatch batch_a = bam_types_AtomicTxnBatch_init_default;
  batch_a.seq_id = 123U;
  batch_a.max_schedule_slot = 100UL;
  batch_state.ingress_rx_ts_ns      = 3000L;
  g_clock = 3000L;
  fd_bam_publish_batch( state, &batch_state, &batch_a, &empty_parsed_batch );

  bam_types_AtomicTxnBatch batch_b = bam_types_AtomicTxnBatch_init_default;
  batch_b.seq_id = 124U;
  batch_b.max_schedule_slot = 100UL + FD_BAM_SLOT_INGRESS_TIMING_CNT;
  batch_state.ingress_rx_ts_ns      = 4444L;
  g_clock = 4444L;
  fd_bam_publish_batch( state, &batch_state, &batch_b, &empty_parsed_batch );

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
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  fd_bam_batch_ctx_t batch_state;
  fd_memset( &batch_state, 0, sizeof(batch_state) );
  batch_state.packet_cnt = 1U;
  batch_state.ingress_rx_ts_ns      = 1000L;
  batch_state.ingress_slot_end_ns   = 1500L;
  batch_state.packets[ 0 ].payload    = bam_dump_txn_fixture;
  batch_state.packets[ 0 ].payload_sz = (ushort)bam_dump_txn_fixture_sz;

  bam_types_AtomicTxnBatch batch = bam_types_AtomicTxnBatch_init_default;
  batch.seq_id = 77U;
  batch.max_schedule_slot = 100UL;

  state->bam_leader_state.slot        = 101UL;
  state->bam_leader_state.slot_end_ns = 2500L;
  g_clock = 2000L;
  fd_bam_publish_batch( state, &batch_state, &batch, &empty_parsed_batch );
  FD_TEST( test_bam_env_drain_pending_txns( env ) == 1UL );

  fd_bam_slot_ingress_timing_t const * entry = fd_bam_slot_ingress_timing_query_const( state, 100UL );
  FD_TEST( entry );
  FD_TEST( entry->first_rx_ts_ns == 1000L );
  FD_TEST( entry->slot_end_ns == 1500L );
  FD_TEST( entry->txn_before_slot_end == 1UL );
  FD_TEST( entry->txn_after_slot_end == 0UL );
  FD_TEST( entry->txn_unknown_slot_end == 0UL );

  fd_txn_m_t const * first = (fd_txn_m_t const *)fd_chunk_to_laddr( state->verify_out.mem, 0UL );
  FD_TEST( first->first_seen_nanos == 1000L );

  test_bam_env_destroy( env );
}

/* Validates that a scheduler response carrying multiple AtomicTxnBatch entries
   fans out into sequential fragments with the correct metadata for each batch. */
static void
test_bam_multiple_batches_forwarded( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  zero_meta_ts( env->out_mcache, 3UL );

  bam_types_Packet first_packets[1];
  fd_memset( first_packets, 0, sizeof(first_packets) );
  first_packets[0].data.size = (pb_size_t)bam_dump_txn_fixture_sz;
  fd_memcpy( first_packets[0].data.bytes, bam_dump_txn_fixture, bam_dump_txn_fixture_sz );

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

  uchar protobuf[1024];
  size_t protobuf_sz = test_bam_encode_scheduler_multi_batch_response( batches, 2UL, protobuf, sizeof(protobuf) );

  test_bam_rx_sched( state, protobuf, protobuf_sz );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 3UL );
  FD_TEST( test_bam_env_drain_pending_txns( env ) == 3UL );

  FD_TEST( state->metrics.transaction_published_cnt == 3UL );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 1UL );
  FD_TEST( state->feedback_queue_depth == 0UL );

  fd_txn_m_t const * tx0 = test_bam_check_published( env, 0UL, 6U, 1U, 0U, 0U, 0UL, 0UL );
  fd_txn_m_t const * tx1 = test_bam_check_published( env, 1UL, 7U, 2U, 0U, 1U, 8UL, 2UL );
  fd_txn_m_t const * tx2 = test_bam_check_published( env, 2UL, 7U, 2U, 1U, 1U, 8UL, 0UL );

  uchar expected[ FD_TXN_MAX_SZ ] __attribute__((aligned(alignof(fd_txn_t))));
  ulong expected_sz = fd_txn_parse( first_packets[0].data.bytes, first_packets[0].data.size, expected, NULL );
  FD_TEST( expected_sz && tx0->txn_t_sz==expected_sz );
  FD_TEST( !memcmp( fd_txn_m_txn_t_const( tx0 ), expected, expected_sz ) );
  FD_TEST( !tx1->txn_t_sz && !tx2->txn_t_sz );

  FD_TEST( !memcmp( fd_txn_m_payload_const( tx0 ), bam_dump_txn_fixture, bam_dump_txn_fixture_sz ) );
  FD_TEST( fd_txn_m_payload_const( tx1 )[0] == 'n' );
  FD_TEST( fd_txn_m_payload_const( tx2 )[0] == 'o' );

  test_bam_env_destroy( env );
}

static void
test_bam_independent_batches_queue_and_drain_in_order( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  bam_types_Packet packets[3];
  test_bam_packet_encode_ctx_t packet_ctx[3];
  bam_types_AtomicTxnBatch batches[3];
  test_bam_fill_single_txn_batches( packets, packet_ctx, batches, 3UL, 50U, 500UL );

  uchar protobuf[512];
  size_t protobuf_sz = test_bam_encode_scheduler_multi_batch_response( batches, 3UL, protobuf, sizeof(protobuf) );
  test_bam_rx_sched( state, protobuf, protobuf_sz );

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
    fd_txn_m_t const * txnm = test_bam_check_published( env, i, 50U+(uint)i, 1U, 0U, 0U, 0UL, 0UL );
    FD_TEST( env->out_mcache[ i ].sig == 0UL );
    FD_TEST( fd_txn_m_payload_const( txnm )[0] == (uchar)( 'a' + (int)i ) );
  }

  test_bam_env_destroy( env );
}

static void
test_bam_pending_over_stem_burst_drains_in_multiple_passes( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  bam_types_Packet packets[ FD_BAM_FULL_ATOMIC_BATCHES_PER_STEM_BURST ][ FD_BAM_MAX_TXN_PER_ATOMIC_BATCH ];
  test_bam_packet_encode_ctx_t packet_ctx[ FD_BAM_FULL_ATOMIC_BATCHES_PER_STEM_BURST ];
  bam_types_AtomicTxnBatch batches[ FD_BAM_FULL_ATOMIC_BATCHES_PER_STEM_BURST ];
  test_bam_fill_full_txn_batches( packets, packet_ctx, batches, FD_BAM_FULL_ATOMIC_BATCHES_PER_STEM_BURST, 900U, 900UL );

  uchar protobuf[ TEST_BAM_PROTOBUF_BUF_SZ ];
  size_t protobuf_sz = test_bam_encode_scheduler_multi_batch_response( batches,
                                                                       FD_BAM_FULL_ATOMIC_BATCHES_PER_STEM_BURST,
                                                                       protobuf,
                                                                       sizeof(protobuf) );
  test_bam_rx_sched( state, protobuf, protobuf_sz );

  uchar single_protobuf[256];
  size_t single_protobuf_sz = test_bam_build_scheduler_batch_msg( single_protobuf, sizeof(single_protobuf), 1000U, 1, 0 );
  test_bam_rx_sched( state, single_protobuf, single_protobuf_sz );

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

/* test_bam_stem_iter mirrors one stem run loop iteration after
   before_credit (fd_stem_run1.c): the default backpressure check, the
   tile's check_credit, after_credit, then a round robin poll of one in.
   A frag found on the polled in is handed to the tile (its payload is a
   bundle result keyed by the frag sig) and consumed.  Returns 0 if the
   stem stayed backpressured, 1 if it ran but consumed no frag, and 2 if
   it consumed a frag. */

static int
test_bam_stem_iter( fd_bam_tile_t *     state,
                    fd_stem_context_t * stem,
                    ulong               in_cnt,
                    ulong *             in_rr,
                    uchar * const *     in_buf ) {
  int charge_busy   = 0;
  int backpressured = *stem->min_cr_avail<FD_BAM_STEM_BURST;
  fd_bam_test_check_credit( state, stem, &charge_busy, &backpressured );
  if( backpressured ) return 0;
  int poll_in = 1;
  fd_bam_test_after_credit( state, stem, &poll_in, &charge_busy );
  if( !poll_in ) return 1;

  fd_stem_tile_in_t * in = &stem->in[ *in_rr ];
  *in_rr = (*in_rr+1UL)%in_cnt;
  if( fd_seq_diff( in->seq, fd_frag_meta_seq_query( in->mline ) ) ) return 1;
  ulong sig = in->mline->sig;
  fd_bam_bundle_result_t res = test_make_bundle_result( (uint)sig, 1000UL+sig, 1U );
  fd_memcpy( in_buf[ in->idx ], &res, sizeof(fd_bam_bundle_result_t) );
  fd_bam_test_receive_ingress_frag( state, in->idx, sig, 0UL, sizeof(fd_bam_bundle_result_t) );
  in->seq   = fd_seq_inc( in->seq, 1UL );
  in->mline = in->mcache + fd_mcache_line_idx( in->seq, in->depth );
  return 2;
}

/* With forwarding work queued and bam_verif short of STEM_BURST
   credits, the stem must keep consuming the feedback ins (motor_bam,
   bank_bam, pack_bam_res).  They are unreliable, so falling behind
   would overrun them and drop results.  With no feedback waiting and a queued batch that does not
   fit, the stem must stay backpressured so an efficient-mode park is
   woken by verify returning credits.  With nothing queued it must not,
   so an efficient-mode park is woken by feedback frags instead.
   bam_verif must never be overrun and a batch must go out whole. */

static void
test_bam_verify_backpressure_keeps_feedback_polling( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  /* Firedancer's bam outs: bam_verif, bam_sign (unreliable consumer),
     bam_gossip and bam_shred.  bam_verif is under STEM_BURST credits. */
  fd_frag_meta_t * mcaches [ 4 ] = { env->out_mcache, NULL, NULL, NULL };
  ulong            seqs    [ 4 ] = { 0UL, 0UL, 0UL, 0UL };
  ulong            depths  [ 4 ] = { env->stem_depths[ 0 ], 128UL, 128UL, 128UL };
  ulong            cr_avail[ 4 ] = { 7UL, 0UL, 128UL, 128UL };
  int              reliable[ 4 ] = { 1, 0, 1, 1 };
  ulong            min_cr_avail  = 7UL;

  /* Polled ins: motor_bam (the only bank_in) and pack_bam_res. */
  ulong const       in_depth = 8UL;
  fd_frag_meta_t *  in_mcache[ 2 ];
  fd_stem_tile_in_t ins[ 2 ];
  fd_memset( ins, 0, sizeof(ins) );
  for( ulong i=0UL; i<2UL; i++ ) {
    void * mem = fd_wksp_alloc_laddr( wksp, fd_mcache_align(), fd_mcache_footprint( in_depth, 0UL ), 1UL );
    FD_TEST( mem );
    in_mcache[ i ] = fd_mcache_join( fd_mcache_new( mem, in_depth, 0UL, 0UL ) );
    FD_TEST( in_mcache[ i ] );
    ins[ i ].mcache = in_mcache[ i ];
    ins[ i ].depth = (uint)in_depth;
    ins[ i ].idx   = (uint)i;
    ins[ i ].seq   = 0UL;
    ins[ i ].mline = ins[ i ].mcache + fd_mcache_line_idx( 0UL, in_depth );
  }
  fd_stem_context_t stem[1] = {{
    .mcaches             = mcaches,
    .seqs                = seqs,
    .depths              = depths,
    .cr_avail            = cr_avail,
    .min_cr_avail        = &min_cr_avail,
    .cr_decrement_amount = 1UL,
    .out_reliable        = reliable,
    .in                  = ins
  }};
  state->out_cnt       = 4UL;
  state->polled_in_cnt = 2UL;

  union {
    fd_bam_bundle_result_t result;
    uchar                  bytes[ sizeof(fd_bam_bundle_result_t) ];
  } in_frag[ 2 ];
  uchar * in_buf[ 2 ] = { in_frag[ 0 ].bytes, in_frag[ 1 ].bytes };
  state->bank_bam_in_cnt        = 1UL;
  state->bank_bam_in_idx        = 0UL;
  state->bank_in[ 0 ]           = (fd_bam_in_ctx_t){ .mem = (fd_wksp_t *)in_buf[ 0 ] };
  state->pack_bam_result_in_idx = 1UL;
  state->pack_result_in         = (fd_bam_in_ctx_t){ .mem = (fd_wksp_t *)in_buf[ 1 ] };

  ulong fseq[1] = { FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE };
  state->bam_status_fseq = fseq;

  /* Two queued atomic 5-txn batches; only the first fits in 7 credits. */
  for( uint b=0U; b<2U; b++ ) {
    for( uchar i=0U; i<5U; i++ ) {
      fd_bam_pending_txn_t * pending = bam_pending_txn_push_tail_nocopy( state->pending_txns );
      fd_memset( pending, 0, sizeof(fd_bam_pending_txn_t) );
      pending->payload_sz      = 1U;
      pending->payload[ 0 ]    = (uchar)( 'a' + (int)b );
      pending->seq_id          = 300U + b;
      pending->batch_idx       = i;
      pending->batch_cnt       = 5U;
      pending->revert_on_error = 1U;
    }
  }

  /* Results waiting: 2 on motor_bam, 3 on pack_bam_res. */
  ulong pub_seq[ 2 ] = { 0UL, 0UL };
  for( ulong k=0UL; k<5UL; k++ ) {
    ulong i = k<2UL ? 0UL : 1UL;
    fd_mcache_publish( in_mcache[ i ], in_depth, pub_seq[ i ], 400UL+k, 0UL, sizeof(fd_bam_bundle_result_t), 0UL, 0UL, 0UL );
    pub_seq[ i ]++;
  }

  /* The first iteration forwards the first batch whole, holds the
     second, and still polls a result.  Every later iteration polls too,
     so all feedback drains while bam_verif is short. */
  ulong in_rr = 0UL;
  FD_TEST( test_bam_stem_iter( state, stem, 2UL, &in_rr, in_buf )==2 );
  FD_TEST( seqs[ 0 ]==5UL );
  ulong consumed = 1UL;
  for( ulong iter=0UL; iter<16UL && consumed<5UL; iter++ ) {
    int r = test_bam_stem_iter( state, stem, 2UL, &in_rr, in_buf );
    FD_TEST( r );
    consumed += (ulong)( r==2 );
  }
  FD_TEST( consumed==5UL );
  FD_TEST( state->feedback_queue_depth==5U );
  FD_TEST( seqs[ 0 ]==5UL );
  FD_TEST( cr_avail[ 0 ]==2UL );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns )==5UL );
  fd_bam_pending_txn_t const * head = bam_pending_txn_peek_head_const( state->pending_txns );
  FD_TEST( head->seq_id==301U );
  FD_TEST( head->batch_idx==0U );

  /* Nothing fits and no feedback is waiting: stay backpressured, so an
     efficient-mode park waits on verify's credits. */
  FD_TEST( !test_bam_stem_iter( state, stem, 2UL, &in_rr, in_buf ) );
  FD_TEST( !test_bam_stem_iter( state, stem, 2UL, &in_rr, in_buf ) );
  FD_TEST( seqs[ 0 ]==5UL );

  /* A new result on either in releases the loop again. */
  fd_mcache_publish( in_mcache[ 1 ], in_depth, pub_seq[ 1 ]++, 405UL, 0UL, sizeof(fd_bam_bundle_result_t), 0UL, 0UL, 0UL );
  consumed = 0UL;
  for( ulong iter=0UL; iter<4UL && !consumed; iter++ ) {
    int r = test_bam_stem_iter( state, stem, 2UL, &in_rr, in_buf );
    FD_TEST( r );
    consumed += (ulong)( r==2 );
  }
  FD_TEST( consumed==1UL );
  FD_TEST( state->feedback_queue_depth==6U );
  FD_TEST( !test_bam_stem_iter( state, stem, 2UL, &in_rr, in_buf ) );

  /* Any other reliable out short of STEM_BURST keeps backpressure even
     with feedback waiting.  bam_sign has no reliable consumer, so its
     credits do not count. */
  fd_mcache_publish( in_mcache[ 0 ], in_depth, pub_seq[ 0 ]++, 406UL, 0UL, sizeof(fd_bam_bundle_result_t), 0UL, 0UL, 0UL );
  int charge_busy   = 0;
  int backpressured = 1;
  fd_bam_test_check_credit( state, stem, &charge_busy, &backpressured );
  FD_TEST( !backpressured );
  cr_avail[ 3 ] = FD_BAM_STEM_BURST-1UL;
  backpressured = 1;
  fd_bam_test_check_credit( state, stem, &charge_busy, &backpressured );
  FD_TEST( backpressured );
  cr_avail[ 3 ] = 128UL;
  cr_avail[ 2 ] = 0UL;
  backpressured = 1;
  fd_bam_test_check_credit( state, stem, &charge_busy, &backpressured );
  FD_TEST( backpressured );
  cr_avail[ 2 ] = 128UL;
  backpressured = 0;
  fd_bam_test_check_credit( state, stem, &charge_busy, &backpressured );
  FD_TEST( !backpressured );
  FD_TEST( !charge_busy );

  /* Once bam_verif has credits for the whole held batch, it goes out
     whole and in order, and the waiting result is still consumed. */
  cr_avail[ 0 ] = 5UL;
  consumed = 0UL;
  for( ulong iter=0UL; iter<4UL && !consumed; iter++ ) {
    int r = test_bam_stem_iter( state, stem, 2UL, &in_rr, in_buf );
    FD_TEST( r );
    consumed += (ulong)( r==2 );
  }
  FD_TEST( consumed==1UL );
  FD_TEST( state->feedback_queue_depth==7U );
  FD_TEST( seqs[ 0 ]==10UL );
  FD_TEST( cr_avail[ 0 ]==0UL );
  FD_TEST( bam_pending_txn_empty( state->pending_txns ) );
  for( ulong i=0UL; i<10UL; i++ ) {
    fd_txn_m_t const * txnm = fd_chunk_to_laddr_const( state->verify_out.mem, env->out_mcache[ i ].chunk );
    FD_TEST( txnm->bam.seq_id==300U+(uint)(i/5UL) );
    FD_TEST( txnm->bam.batch_idx==(uchar)(i%5UL) );
    FD_TEST( txnm->block_engine.bundle_txn_cnt==( (i%5UL) ? 0UL : 5UL ) );
  }

  /* Nothing queued: the loop is released though bam_verif has no
     credits, publishes nothing, and an idle poll finds no frag.  Any
     other reliable out short of STEM_BURST still holds it. */
  FD_TEST( test_bam_stem_iter( state, stem, 2UL, &in_rr, in_buf )==1 );
  FD_TEST( seqs[ 0 ]==10UL );
  cr_avail[ 3 ] = FD_BAM_STEM_BURST-1UL;
  FD_TEST( !test_bam_stem_iter( state, stem, 2UL, &in_rr, in_buf ) );
  cr_avail[ 3 ] = 128UL;

  for( ulong i=0UL; i<2UL; i++ ) fd_wksp_free_laddr( fd_mcache_delete( fd_mcache_leave( in_mcache[ i ] ) ) );
  test_bam_env_destroy( env );
}

static void
test_bam_queue_full_rejects_whole_batch_without_partial_enqueue( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

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
  test_bam_rx_sched( state, protobuf, protobuf_sz );

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
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  ulong const expected_txn_cnt = batch_cnt * FD_BAM_MAX_TXN_PER_ATOMIC_BATCH;
  zero_meta_ts( env->out_mcache, expected_txn_cnt );

  static bam_types_Packet packets[ FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE ][ FD_BAM_MAX_TXN_PER_ATOMIC_BATCH ];
  static test_bam_packet_encode_ctx_t packet_ctx[ FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE ];
  static bam_types_AtomicTxnBatch batches[ FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE ];
  test_bam_fill_full_txn_batches( packets, packet_ctx, batches, batch_cnt, seq_base, slot_base );

  uchar protobuf[ TEST_BAM_PROTOBUF_BUF_SZ ];
  size_t protobuf_sz = test_bam_encode_scheduler_multi_batch_response( batches, batch_cnt, protobuf, sizeof( protobuf ) );
  test_bam_rx_sched( state, protobuf, protobuf_sz );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == expected_txn_cnt );
  FD_TEST( test_bam_env_drain_all_pending_txns( env ) == expected_txn_cnt );
  FD_TEST( bam_pending_txn_empty( state->pending_txns ) );

  FD_TEST( state->metrics.transaction_published_cnt == expected_txn_cnt );
  FD_TEST( state->metrics.ingress_batch_published_cnt == batch_cnt );
  FD_TEST( state->metrics.atomic_batch_published_cnt == batch_cnt );
  FD_TEST( state->feedback_queue_depth == 0UL );
  FD_TEST( state->metrics.transaction_rejected_backpressure_cnt == 0UL );
  FD_TEST( state->metrics.ingress_message_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_MESSAGE_REJECT_REASON_V_OVERFLOW_MESSAGE_IDX ] == 0UL );

  for( ulong i=0UL; i<expected_txn_cnt; i++ ) {
    uint const seq_id  = seq_base + (uint)( i / FD_BAM_MAX_TXN_PER_ATOMIC_BATCH );
    uint const txn_idx = (uint)( i % FD_BAM_MAX_TXN_PER_ATOMIC_BATCH );
    test_bam_check_published( env, i, seq_id, FD_BAM_MAX_TXN_PER_ATOMIC_BATCH, txn_idx, 1U, (ulong)seq_id+1UL,
                              txn_idx ? 0UL : FD_BAM_MAX_TXN_PER_ATOMIC_BATCH );
  }

  test_bam_env_destroy( env );
}

static void
test_bam_max_wire_message_fits_min_grpc_buffer( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  static bam_types_Packet packets[ FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE ][ FD_BAM_MAX_TXN_PER_ATOMIC_BATCH ];
  static test_bam_packet_encode_ctx_t packet_ctx[ FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE ];
  static bam_types_AtomicTxnBatch batches[ FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE ];
  test_bam_fill_full_txn_batches( packets, packet_ctx, batches, FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE, 1700U, 600UL );

  for( ulong i=0UL; i<FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE; i++ ) {
    for( ulong j=0UL; j<FD_BAM_MAX_TXN_PER_ATOMIC_BATCH; j++ ) {
      packets[ i ][ j ].data.size = (pb_size_t)FD_TXN_MTU;
      fd_memset( packets[ i ][ j ].data.bytes, (int)(uchar)( i*FD_BAM_MAX_TXN_PER_ATOMIC_BATCH+j ), FD_TXN_MTU );
      packets[ i ][ j ].meta.size = FD_TXN_MTU;
    }
  }

  static uchar protobuf[ FD_BAM_GRPC_MIN_BUF_SZ ];
  size_t protobuf_sz = test_bam_encode_scheduler_multi_batch_response(
      batches,
      FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE,
      protobuf,
      sizeof(protobuf) );
  FD_TEST( sizeof(fd_grpc_hdr_t)+protobuf_sz<=FD_BAM_GRPC_MIN_BUF_SZ );

  test_bam_rx_sched( state, protobuf, protobuf_sz );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns )==FD_BAM_MAX_TXN_PER_MESSAGE );
  FD_TEST( test_bam_env_drain_all_pending_txns( env )==FD_BAM_MAX_TXN_PER_MESSAGE );

  test_bam_env_destroy( env );
}

static void
test_bam_expect_scheduler_decode_failure( test_bam_env_t * env,
                                          uchar const *    protobuf,
                                          size_t           protobuf_sz,
                                          ulong            expected_failure_cnt ) {
  fd_bam_tile_t * state = env->state;
  test_bam_rx_sched( state, protobuf, protobuf_sz );

  FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_SCHEDULER_ENVELOPE_DECODE_IDX ] == expected_failure_cnt );
  FD_TEST( state->metrics.ingress_multi_message_received_cnt == 0UL );
  FD_TEST( state->metrics.ingress_batch_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_INVALID_BATCH_IDX ] == 0UL );
  FD_TEST( state->metrics.ingress_packet_oversize_cnt == 0UL );
  FD_TEST( state->metrics.transaction_published_cnt == 0UL );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
  FD_TEST( state->feedback_queue_depth == 0UL );
  FD_TEST( env->stem_seqs[0] == 0UL );
}

/* A malformed scheduler message counts one envelope decode failure and is
   dropped whole: nothing publishes, even what decoded before the failure,
   and no result is queued. */
static void
test_bam_scheduler_rejects_invalid_custom_decode_fields( fd_wksp_t * wksp ) {
  uchar valid_multi_payload[384];
  size_t valid_multi_payload_sz = test_bam_encode_one_packet_multi_payload_raw( 78U, (uchar)'v', valid_multi_payload,
                                                                                sizeof( valid_multi_payload ) );
  uchar valid_v0_payload[512];
  pb_ostream_t valid_v0_stream = pb_ostream_from_buffer( valid_v0_payload, sizeof( valid_v0_payload ) );
  test_bam_encode_string_field_raw( &valid_v0_stream, bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag,
                                    valid_multi_payload, valid_multi_payload_sz );
  uchar batch[256];
  size_t batch_sz = test_bam_encode_one_packet_atomic_batch_raw( 82U, (uchar)'n', batch, sizeof( batch ) );

  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  zero_meta_ts( env->out_mcache, 2UL );
  uchar protobuf[768];
  ulong expected_failure_cnt = 0UL;

  /* Truncated */
  size_t protobuf_sz = test_bam_build_scheduler_batch_msg( protobuf, sizeof(protobuf), 5U, 2, 0 );
  test_bam_expect_scheduler_decode_failure( env, protobuf, protobuf_sz-1UL, ++expected_failure_cnt );

  /* A valid message followed by an invalid outer tag */
  protobuf_sz = test_bam_build_scheduler_batch_msg( protobuf, sizeof(protobuf), 77U, 1, 0 );
  protobuf[ protobuf_sz ] = 0x00U;
  test_bam_expect_scheduler_decode_failure( env, protobuf, protobuf_sz+1UL, ++expected_failure_cnt );

  /* Known fields with the wrong wire type, before a valid one */
  pb_ostream_t outer_stream = pb_ostream_from_buffer( protobuf, sizeof( protobuf ) );
  test_bam_encode_varint_field_raw( &outer_stream, bam_api_SchedulerResponse_v0_tag, 1UL );
  test_bam_encode_string_field_raw( &outer_stream, bam_api_SchedulerResponse_v0_tag, valid_v0_payload, valid_v0_stream.bytes_written );
  test_bam_expect_scheduler_decode_failure( env, protobuf, outer_stream.bytes_written, ++expected_failure_cnt );

  outer_stream = pb_ostream_from_buffer( protobuf, sizeof( protobuf ) );
  test_bam_encode_varint_field_raw( &outer_stream, 31U, 1UL );
  test_bam_encode_string_field_raw( &outer_stream, bam_api_SchedulerResponse_v0_tag, valid_v0_payload, valid_v0_stream.bytes_written );
  test_bam_expect_scheduler_decode_failure( env, protobuf, outer_stream.bytes_written, ++expected_failure_cnt );

  uchar v0_payload[640];
  pb_ostream_t v0_stream = pb_ostream_from_buffer( v0_payload, sizeof( v0_payload ) );
  test_bam_encode_varint_field_raw( &v0_stream, bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag, 1UL );
  test_bam_encode_string_field_raw( &v0_stream, bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag,
                                    valid_multi_payload, valid_multi_payload_sz );
  protobuf_sz = test_bam_encode_scheduler_response_v0_raw( v0_payload, v0_stream.bytes_written, protobuf, sizeof( protobuf ) );
  test_bam_expect_scheduler_decode_failure( env, protobuf, protobuf_sz, ++expected_failure_cnt );

  uchar multi_payload[384];
  for( ulong i=0UL; i<2UL; i++ ) {
    pb_ostream_t multi_stream = pb_ostream_from_buffer( multi_payload, sizeof( multi_payload ) );
    test_bam_encode_varint_field_raw( &multi_stream, i ? bam_types_MultipleAtomicTxnBatch_mss_padding_tag
                                                       : bam_types_MultipleAtomicTxnBatch_batches_tag, 1UL );
    test_bam_encode_string_field_raw( &multi_stream, bam_types_MultipleAtomicTxnBatch_batches_tag, batch, batch_sz );
    protobuf_sz = test_bam_encode_scheduler_multi_payload_raw( multi_payload, multi_stream.bytes_written, protobuf, sizeof( protobuf ) );
    test_bam_expect_scheduler_decode_failure( env, protobuf, protobuf_sz, ++expected_failure_cnt );
  }

  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_accepts_mss_padding( fd_wksp_t * wksp ) {
  uchar batch[ 2 ][ 256 ];
  size_t batch_sz[ 2 ];
  batch_sz[ 0 ] = test_bam_encode_one_packet_atomic_batch_raw( 601U, (uchar)'a', batch[ 0 ], sizeof(batch[ 0 ]) );
  batch_sz[ 1 ] = test_bam_encode_one_packet_atomic_batch_raw( 602U, (uchar)'b', batch[ 1 ], sizeof(batch[ 1 ]) );

  uchar plain_multi[ 512 ];
  pb_ostream_t plain = pb_ostream_from_buffer( plain_multi, sizeof(plain_multi) );
  for( ulong i=0UL; i<2UL; i++ )
    test_bam_encode_string_field_raw( &plain, bam_types_MultipleAtomicTxnBatch_batches_tag, batch[ i ], batch_sz[ i ] );
  FD_TEST( plain.bytes_written<3000UL );

  /* The node pads the first message of a leader window by the gap from its
     unpadded MultipleAtomicTxnBatch size to 3000 bytes.  The field tag and
     length prefix make the final nested message 3000..3003 bytes. */
  uchar padding[ 3200 ] = {0};
  size_t padding_sz = 3000UL - plain.bytes_written;

  for( ulong placement=0UL; placement<5UL; placement++ ) {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_env_create( env, wksp );
    zero_meta_ts( env->out_mcache, 2UL );

    uchar multi_payload[ 4096 ];
    pb_ostream_t multi = pb_ostream_from_buffer( multi_payload, sizeof(multi_payload) );
    if( placement==4UL )
      test_bam_encode_string_field_raw( &multi, bam_types_MultipleAtomicTxnBatch_mss_padding_tag, padding, 0UL );
    for( ulong i=0UL; i<=2UL; i++ ) {
      if( i==placement || ( placement==3UL && i==2UL ) )
        test_bam_encode_string_field_raw( &multi, bam_types_MultipleAtomicTxnBatch_mss_padding_tag,
                                          padding, placement==3UL ? sizeof(padding) : padding_sz );
      if( i<2UL )
        test_bam_encode_string_field_raw( &multi, bam_types_MultipleAtomicTxnBatch_batches_tag, batch[ i ], batch_sz[ i ] );
    }
    if( placement==4UL )
      test_bam_encode_string_field_raw( &multi, bam_types_MultipleAtomicTxnBatch_mss_padding_tag, padding, 0UL );
    if( placement<3UL ) FD_TEST( multi.bytes_written>=3000UL && multi.bytes_written<=3003UL );
    if( placement==3UL ) FD_TEST( multi.bytes_written>3200UL );

    uchar protobuf[ 4200 ];
    size_t protobuf_sz = test_bam_encode_scheduler_multi_payload_raw( multi_payload, multi.bytes_written,
                                                                      protobuf, sizeof(protobuf) );
    test_bam_rx_sched( state, protobuf, protobuf_sz );
    FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_SCHEDULER_ENVELOPE_DECODE_IDX ]==0UL );
    FD_TEST( state->metrics.ingress_multi_message_received_cnt==1UL );
    FD_TEST( state->metrics.ingress_batch_commit_attempt_cnt==2UL );
    FD_TEST( state->metrics.ingress_batch_published_cnt==0UL );
    FD_TEST( state->metrics.transaction_published_cnt==0UL );
    FD_TEST( bam_pending_txn_cnt( state->pending_txns )==2UL );
    FD_TEST( test_bam_env_drain_all_pending_txns( env )==2UL );
    FD_TEST( state->metrics.ingress_batch_published_cnt==2UL );
    FD_TEST( state->metrics.transaction_published_cnt==2UL );
    FD_TEST( state->feedback_queue_depth==0U );

    test_bam_env_destroy( env );
  }
}

static void
test_bam_scheduler_malformed_trailer_rolls_back( fd_wksp_t *   wksp,
                                                 uchar const * trailer,
                                                 size_t        trailer_sz ) {
  /* A rejected Packet stages both a decode metric and an attributable
     result.  A malformed trailing field must discard both before commit. */
  uchar oversized_payload[ FD_TXN_MTU+1UL ] = {0};
  uchar raw_packet[ FD_TXN_MTU+32UL ];
  pb_ostream_t packet = pb_ostream_from_buffer( raw_packet, sizeof(raw_packet) );
  test_bam_encode_string_field_raw( &packet, bam_types_Packet_data_tag,
                                    oversized_payload, sizeof(oversized_payload) );

  uchar raw_batch[ FD_TXN_MTU+64UL ];
  pb_ostream_t batch = pb_ostream_from_buffer( raw_batch, sizeof(raw_batch) );
  test_bam_encode_varint_field_raw( &batch, bam_types_AtomicTxnBatch_seq_id_tag, 603U );
  test_bam_encode_varint_field_raw( &batch, bam_types_AtomicTxnBatch_max_schedule_slot_tag, 1UL );
  test_bam_encode_string_field_raw( &batch, bam_types_AtomicTxnBatch_packets_tag,
                                    raw_packet, packet.bytes_written );

  uchar multi_payload[ FD_TXN_MTU+128UL ];
  pb_ostream_t multi = pb_ostream_from_buffer( multi_payload, sizeof(multi_payload) );
  test_bam_encode_string_field_raw( &multi, bam_types_MultipleAtomicTxnBatch_batches_tag,
                                    raw_batch, batch.bytes_written );
  FD_TEST( pb_write( &multi, trailer, trailer_sz ) );

  uchar protobuf[ FD_TXN_MTU+256UL ];
  size_t protobuf_sz = test_bam_encode_scheduler_multi_payload_raw( multi_payload, multi.bytes_written,
                                                                    protobuf, sizeof(protobuf) );

  for( int full=0; full<2; full++ ) {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_env_create( env, wksp );
    if( full ) {
      for( uint i=0U; i<FD_BAM_MAX_PENDING_RESULTS; i++ )
        state->bam_results[ i ] = fd_bam_result_base( 10000U+i, state->scheduler_gen, 1UL, 1U );
      state->bam_results_head  = 0U;
      state->bam_results_tail  = 0U;
      state->feedback_queue_depth = FD_BAM_MAX_PENDING_RESULTS;
    } else {
      ushort last = FD_BAM_MAX_PENDING_RESULTS-1U;
      state->bam_results[ last ] = fd_bam_result_base( 604U, state->scheduler_gen, 1UL, 1U );
      state->bam_results_head  = last;
      state->bam_results_tail  = 0U; /* staged result would cross the ring boundary */
      state->feedback_queue_depth = 1U;
    }

    ushort head_before  = state->bam_results_head;
    ushort tail_before  = state->bam_results_tail;
    ushort depth_before = state->feedback_queue_depth;
    ulong dropped_before = state->metrics.feedback_results_dropped_cnt;
    test_bam_rx_sched( state, protobuf, protobuf_sz );

    FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_SCHEDULER_ENVELOPE_DECODE_IDX ]==1UL );
    FD_TEST( state->metrics.ingress_multi_message_received_cnt==0UL );
    FD_TEST( state->metrics.ingress_packet_oversize_cnt==0UL );
    FD_TEST( state->metrics.ingress_batch_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_INVALID_BATCH_IDX ]==0UL );
    FD_TEST( state->metrics.feedback_results_dropped_cnt==dropped_before );
    FD_TEST( state->metrics.transaction_published_cnt==0UL );
    FD_TEST( bam_pending_txn_empty( state->pending_txns ) );
    FD_TEST( state->bam_results_head==head_before );
    FD_TEST( state->bam_results_tail==tail_before );
    FD_TEST( state->feedback_queue_depth==depth_before );
    FD_TEST( state->bam_results[ head_before ].seq_id==(full ? 10000U : 604U) );

    test_bam_env_destroy( env );
  }
}

static void
test_bam_scheduler_malformed_mss_padding_rolls_back( fd_wksp_t * wksp ) {
  uchar trailer[ 8 ];
  pb_ostream_t t = pb_ostream_from_buffer( trailer, sizeof(trailer) );
  FD_TEST( pb_encode_tag( &t, PB_WT_STRING, bam_types_MultipleAtomicTxnBatch_mss_padding_tag ) );
  FD_TEST( pb_encode_varint( &t, 10UL ) ); /* no ten bytes follow */
  test_bam_scheduler_malformed_trailer_rolls_back( wksp, trailer, t.bytes_written );
}

static void
test_bam_scheduler_mss_padding_does_not_raise_batch_limit( fd_wksp_t * wksp ) {
  uchar batch[ FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE+1UL ][ 256 ];
  size_t batch_sz[ FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE+1UL ];
  for( ulong i=0UL; i<FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE+1UL; i++ )
    batch_sz[ i ] = test_bam_encode_one_packet_atomic_batch_raw( 605U+(uint)i, (uchar)'v', batch[ i ], sizeof(batch[ i ]) );
  uchar const zero = 0U;
  for( ulong batch_cnt=FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE;
       batch_cnt<=FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE+1UL; batch_cnt++ ) {
    uchar multi_payload[ 1024 ];
    pb_ostream_t multi = pb_ostream_from_buffer( multi_payload, sizeof(multi_payload) );
    for( ulong i=0UL; i<batch_cnt; i++ ) {
      if( i==4UL )
        test_bam_encode_string_field_raw( &multi, bam_types_MultipleAtomicTxnBatch_mss_padding_tag, &zero, 1UL );
      test_bam_encode_string_field_raw( &multi, bam_types_MultipleAtomicTxnBatch_batches_tag, batch[ i ], batch_sz[ i ] );
    }

    uchar protobuf[ 1200 ];
    size_t protobuf_sz = test_bam_encode_scheduler_multi_payload_raw( multi_payload, multi.bytes_written,
                                                                      protobuf, sizeof(protobuf) );
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_env_create( env, wksp );
    if( batch_cnt==FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE ) zero_meta_ts( env->out_mcache, batch_cnt );
    test_bam_rx_sched( state, protobuf, protobuf_sz );

    if( batch_cnt==FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE ) {
      FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_SCHEDULER_ENVELOPE_DECODE_IDX ]==0UL );
      FD_TEST( state->metrics.ingress_multi_message_received_cnt==1UL );
      FD_TEST( state->metrics.ingress_batch_commit_attempt_cnt==batch_cnt );
      FD_TEST( state->metrics.ingress_batch_published_cnt==0UL );
      FD_TEST( test_bam_env_drain_all_pending_txns( env )==batch_cnt );
      FD_TEST( state->metrics.ingress_batch_published_cnt==batch_cnt );
    } else {
      FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_SCHEDULER_ENVELOPE_DECODE_IDX ]==1UL );
      FD_TEST( state->metrics.ingress_message_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_MESSAGE_REJECT_REASON_V_OVERFLOW_MESSAGE_IDX ]==1UL );
      FD_TEST( state->metrics.ingress_multi_message_received_cnt==0UL );
      FD_TEST( state->metrics.transaction_published_cnt==0UL );
      FD_TEST( bam_pending_txn_empty( state->pending_txns ) );
    }
    FD_TEST( state->feedback_queue_depth==0U );
    test_bam_env_destroy( env );
  }
}

/* Duplicate oneof fields apply only the final recognized one, within a
   SchedulerResponseV0 and across repeated v0 fields.  Cases: 0 batches
   then a heartbeat; 1 two heartbeats then batches; 2 empty batches then a
   heartbeat; 3 a v0 with empty batches then a v0 with a heartbeat. */
static void
test_bam_scheduler_v0_oneof_uses_last_field( fd_wksp_t * wksp ) {
  uint const  multi_tag = bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag;
  uint const  hb_tag    = bam_api_SchedulerResponseV0_heart_beat_tag;
  ulong const hb_us[ 4 ] = { 12345UL, 22222UL, 1234UL, 2345UL };
  for( ulong i=0UL; i<4UL; i++ ) {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_env_create( env, wksp );
    zero_meta_ts( env->out_mcache, 2UL );
    g_clock = i ? (long)(29UL+i)*(long)1e6 : (long)20e6;

    uchar multi[ 384 ];
    size_t multi_sz = i<2UL ? test_bam_encode_one_packet_multi_payload_raw( 93U+(uint)i, (uchar)( 'm'+i ), multi, sizeof(multi) )
                            : test_bam_encode_multiple_atomic_batch_raw( NULL, NULL, 0UL, multi, sizeof(multi) );
    uchar hb[ 64 ];
    size_t hb_sz = test_bam_encode_heartbeat_payload_raw( hb_us[ i ], hb, sizeof(hb) );
    uchar v0[ 640 ];
    pb_ostream_t v0_stream = pb_ostream_from_buffer( v0, sizeof(v0) );
    uchar protobuf[ 768 ];
    pb_ostream_t ostream = pb_ostream_from_buffer( protobuf, sizeof(protobuf) );
    if( i==1UL ) {
      uchar hb0[ 64 ];
      size_t hb0_sz = test_bam_encode_heartbeat_payload_raw( 11111UL, hb0, sizeof(hb0) );
      test_bam_encode_string_field_raw( &v0_stream, hb_tag,    hb0,   hb0_sz   );
      test_bam_encode_string_field_raw( &v0_stream, hb_tag,    hb,    hb_sz    );
      test_bam_encode_string_field_raw( &v0_stream, multi_tag, multi, multi_sz );
    } else {
      test_bam_encode_string_field_raw( &v0_stream, multi_tag, multi, multi_sz );
      if( i==3UL ) {
        test_bam_encode_string_field_raw( &ostream, bam_api_SchedulerResponse_v0_tag, v0, v0_stream.bytes_written );
        v0_stream = pb_ostream_from_buffer( v0, sizeof(v0) );
      }
      test_bam_encode_string_field_raw( &v0_stream, hb_tag, hb, hb_sz );
    }
    test_bam_encode_string_field_raw( &ostream, bam_api_SchedulerResponse_v0_tag, v0, v0_stream.bytes_written );
    test_bam_rx_sched( state, protobuf, ostream.bytes_written );

    ulong const hb_wins = i!=1UL;
    if( hb_wins ) FD_TEST( bam_pending_txn_empty( state->pending_txns ) );
    else          FD_TEST( test_bam_env_drain_pending_txns( env )==1UL );
    FD_TEST( !state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_SCHEDULER_ENVELOPE_DECODE_IDX ] );
    FD_TEST( state->metrics.ingress_multi_message_received_cnt==!hb_wins );
    FD_TEST( !state->metrics.ingress_message_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_MESSAGE_REJECT_REASON_V_EMPTY_MESSAGE_IDX    ] );
    FD_TEST( !state->metrics.ingress_message_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_MESSAGE_REJECT_REASON_V_OVERFLOW_MESSAGE_IDX ] );
    FD_TEST( !state->metrics.ingress_batch_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_INVALID_BATCH_IDX ] );
    FD_TEST( state->metrics.builder_heartbeats_decoded_cnt==hb_wins );
    FD_TEST( test_hist_total_cnt( state->metrics.builder_heartbeat_arrival_delta_nanos )==hb_wins );
    FD_TEST( state->bam_last_builder_activity_ns==( hb_wins ? g_clock : 0L ) );
    FD_TEST( state->metrics.transaction_published_cnt==!hb_wins && env->stem_seqs[0]==!hb_wins );
    FD_TEST( !state->metrics.atomic_batch_published_cnt && !state->feedback_queue_depth );
    if( !i ) FD_TEST( fd_histf_sum( state->metrics.builder_heartbeat_arrival_delta_nanos )==(ulong)g_clock-12345UL*1000UL );

    test_bam_env_destroy( env );
  }
}

/* Decoding a coalesced wrapper as one unit lets one malformed child suppress
   valid siblings.  Isolate child failures and never invent an unknown identity. */
static void
test_bam_multiple_batches_isolate_nested_decode_failures( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  zero_meta_ts( env->out_mcache, 2UL );

  uchar first_batch[256];
  size_t first_batch_sz = test_bam_encode_one_packet_atomic_batch_raw( 71U, (uchar)'a', first_batch, sizeof( first_batch ) );
  uchar malformed_batch[64];
  pb_ostream_t malformed_stream = pb_ostream_from_buffer( malformed_batch, sizeof( malformed_batch ) );
  test_bam_encode_varint_field_raw( &malformed_stream, bam_types_AtomicTxnBatch_seq_id_tag, 72U );
  test_bam_encode_varint_field_raw( &malformed_stream, bam_types_AtomicTxnBatch_max_schedule_slot_tag, 1UL );
  FD_TEST( pb_encode_tag( &malformed_stream, PB_WT_STRING, bam_types_AtomicTxnBatch_packets_tag ) );
  size_t malformed_batch_sz = malformed_stream.bytes_written;
  uchar second_batch[256];
  size_t second_batch_sz = test_bam_encode_one_packet_atomic_batch_raw( 73U, (uchar)'b', second_batch, sizeof( second_batch ) );

  uchar const * batches[] = { first_batch, malformed_batch, second_batch };
  size_t const batch_sz[] = { first_batch_sz, malformed_batch_sz, second_batch_sz };

  uchar protobuf[768];
  size_t protobuf_sz = test_bam_encode_scheduler_multi_batch_response_raw( batches, batch_sz, 3UL, protobuf, sizeof( protobuf ) );

  test_bam_rx_sched( state, protobuf, protobuf_sz );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 2UL );
  FD_TEST( test_bam_env_drain_pending_txns( env ) == 2UL );

  FD_TEST( state->metrics.transaction_published_cnt == 2UL );
  FD_TEST( state->metrics.ingress_batch_published_cnt == 2UL );
  FD_TEST( state->metrics.ingress_batch_commit_attempt_cnt == 2UL );
  FD_TEST( state->metrics.ingress_batch_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_INVALID_BATCH_IDX ] == 1UL );
  FD_TEST( state->feedback_queue_depth == 0UL );

  fd_frag_meta_t * meta = env->out_mcache;
  FD_TEST( meta[0].seq == 0UL );
  FD_TEST( meta[1].seq == 1UL );
  fd_txn_m_t * first  = fd_chunk_to_laddr( state->verify_out.mem, meta[0].chunk );
  fd_txn_m_t * second = fd_chunk_to_laddr( state->verify_out.mem, meta[1].chunk );
  FD_TEST( first->bam.seq_id == 71U );
  FD_TEST( second->bam.seq_id == 73U );

  test_bam_env_destroy( env );

  state = test_bam_env_create( env, wksp );
  zero_meta_ts( env->out_mcache, 1UL );

  uchar const malformed_before_seq[] = { 0x08U }; /* seq_id tag without varint value */
  uchar oversized_seq_batch[16];
  pb_ostream_t oversized_seq_stream = pb_ostream_from_buffer( oversized_seq_batch, sizeof( oversized_seq_batch ) );
  test_bam_encode_varint_field_raw( &oversized_seq_stream, bam_types_AtomicTxnBatch_seq_id_tag, 1UL<<32 );
  size_t oversized_seq_batch_sz = oversized_seq_stream.bytes_written;
  uchar const malformed_packet[] = { 0x08U, 0x01U }; /* Packet.data with wrong wire type */
  uchar malformed_packet_batch[64];
  pb_ostream_t malformed_packet_stream = pb_ostream_from_buffer( malformed_packet_batch, sizeof( malformed_packet_batch ) );
  test_bam_encode_string_field_raw( &malformed_packet_stream,
                                    bam_types_AtomicTxnBatch_packets_tag,
                                    malformed_packet,
                                    sizeof( malformed_packet ) );
  test_bam_encode_varint_field_raw( &malformed_packet_stream, bam_types_AtomicTxnBatch_seq_id_tag, 82U );
  test_bam_encode_varint_field_raw( &malformed_packet_stream, bam_types_AtomicTxnBatch_max_schedule_slot_tag, 1UL );
  uchar valid_batch[256];
  size_t valid_batch_sz = test_bam_encode_one_packet_atomic_batch_raw( 0U, (uchar)'v', valid_batch, sizeof( valid_batch ) );
  uchar const * unknown_seq_batches[] = { malformed_before_seq, oversized_seq_batch, malformed_packet_batch, valid_batch };
  size_t const unknown_seq_batch_sz[] = { sizeof( malformed_before_seq ), oversized_seq_batch_sz,
                                          malformed_packet_stream.bytes_written, valid_batch_sz };

  protobuf_sz = test_bam_encode_scheduler_multi_batch_response_raw( unknown_seq_batches,
                                                                    unknown_seq_batch_sz,
                                                                    4UL,
                                                                    protobuf,
                                                                    sizeof( protobuf ) );

  test_bam_rx_sched( state, protobuf, protobuf_sz );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 1UL );
  FD_TEST( test_bam_env_drain_pending_txns( env ) == 1UL );

  FD_TEST( state->metrics.transaction_published_cnt == 1UL );
  FD_TEST( state->metrics.ingress_batch_published_cnt == 1UL );
  FD_TEST( state->metrics.ingress_batch_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_INVALID_BATCH_IDX ] == 3UL );
  FD_TEST( state->feedback_queue_depth == 0UL );

  fd_frag_meta_t * unknown_seq_meta = env->out_mcache;
  FD_TEST( unknown_seq_meta[0].seq == 0UL );
  fd_txn_m_t * tx = fd_chunk_to_laddr( state->verify_out.mem, unknown_seq_meta[0].chunk );
  FD_TEST( tx->bam.seq_id == 0U );

  test_bam_env_destroy( env );
}

/* nanopb hands a callback field that is not length-delimited to the
   decode callback as a substream over a stack copy of the raw value.  A
   Packet carried in a varint/fixed32/fixed64 packets field must take the
   malformed Packet path instead of leaving a view into that copy. */
static void
test_bam_scheduler_rejects_non_length_delimited_packets( fd_wksp_t * wksp ) {
  uchar const packet64[ 8 ] = { 0x0aU, 0x06U, 'f', 'i', 'x', 'e', 'd', '8' }; /* Packet{ data[6] } */
  uchar const packet32[ 4 ] = { 0x0aU, 0x02U, 'f', '4' };                     /* Packet{ data[2] } */
  uchar const packetvi[ 1 ] = { 0x0aU };                                      /* Packet.data tag only */
  struct {
    pb_wire_type_t wire_type;
    uchar const *  raw;
    size_t         raw_sz;
  } const cases[ 3 ] = {
    { PB_WT_64BIT,  packet64, sizeof( packet64 ) },
    { PB_WT_32BIT,  packet32, sizeof( packet32 ) },
    { PB_WT_VARINT, packetvi, sizeof( packetvi ) },
  };

  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  for( ulong i=0UL; i<3UL; i++ ) {
    uchar batch[32];
    pb_ostream_t batch_stream = pb_ostream_from_buffer( batch, sizeof( batch ) );
    test_bam_encode_varint_field_raw( &batch_stream, bam_types_AtomicTxnBatch_seq_id_tag, 91U+i );
    test_bam_encode_varint_field_raw( &batch_stream, bam_types_AtomicTxnBatch_max_schedule_slot_tag, 1UL );
    FD_TEST( pb_encode_tag( &batch_stream, cases[ i ].wire_type, bam_types_AtomicTxnBatch_packets_tag ) );
    FD_TEST( pb_write( &batch_stream, cases[ i ].raw, cases[ i ].raw_sz ) );

    uchar const * batches[]  = { batch };
    size_t const  batch_sz[] = { batch_stream.bytes_written };
    uchar protobuf[128];
    size_t protobuf_sz = test_bam_encode_scheduler_multi_batch_response_raw( batches, batch_sz, 1UL, protobuf, sizeof( protobuf ) );
    test_bam_rx_sched( state, protobuf, protobuf_sz );

    FD_TEST( state->decoded_multi->states[0].packet_decode_failed );
    FD_TEST( !state->decoded_multi->states[0].has_deser_err );
    FD_TEST( state->metrics.ingress_multi_message_received_cnt == i+1UL );
    FD_TEST( state->metrics.ingress_batch_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_INVALID_BATCH_IDX ] == i+1UL );
  }

  FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_SCHEDULER_ENVELOPE_DECODE_IDX ] == 0UL );
  FD_TEST( state->metrics.ingress_batch_commit_attempt_cnt == 0UL );
  FD_TEST( state->metrics.transaction_published_cnt == 0UL );
  FD_TEST( bam_pending_txn_empty( state->pending_txns ) );
  FD_TEST( state->feedback_queue_depth == 0UL );
  FD_TEST( env->stem_seqs[0] == 0UL );

  test_bam_env_destroy( env );
}

#define TEST_BAM_MULTI_BATCH_WRAPPER_MAX (FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE + 2UL)

/* A message carrying FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE batches is
   accepted whole, and one carrying more is rejected whole. */
static void
test_bam_multiple_batches_message_limit( fd_wksp_t * wksp ) {
  for( ulong batch_cnt=FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE; batch_cnt<=TEST_BAM_MULTI_BATCH_WRAPPER_MAX; batch_cnt+=2UL ) {
    ulong const overflow = batch_cnt>FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE;
    ulong const accepted = overflow ? 0UL : batch_cnt;
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_env_create( env, wksp );
    zero_meta_ts( env->out_mcache, accepted );

    bam_types_Packet packets[ TEST_BAM_MULTI_BATCH_WRAPPER_MAX ];
    test_bam_packet_encode_ctx_t packet_ctx[ TEST_BAM_MULTI_BATCH_WRAPPER_MAX ];
    bam_types_AtomicTxnBatch batches[ TEST_BAM_MULTI_BATCH_WRAPPER_MAX ];
    test_bam_fill_single_txn_batches( packets, packet_ctx, batches, batch_cnt, 900U, 300UL );
    uchar protobuf[ TEST_BAM_PROTOBUF_BUF_SZ ];
    test_bam_rx_sched( state, protobuf, test_bam_encode_scheduler_multi_batch_response( batches, batch_cnt, protobuf, sizeof( protobuf ) ) );
    FD_TEST( test_bam_env_drain_all_pending_txns( env ) == accepted );

    FD_TEST( state->metrics.transaction_published_cnt == accepted );
    FD_TEST( state->metrics.ingress_batch_published_cnt == accepted );
    FD_TEST( state->metrics.atomic_batch_published_cnt == 0UL );
    FD_TEST( state->feedback_queue_depth == 0UL );
    FD_TEST( state->metrics.ingress_message_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_MESSAGE_REJECT_REASON_V_OVERFLOW_MESSAGE_IDX ] == overflow );
    FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_SCHEDULER_ENVELOPE_DECODE_IDX ] == overflow );

    fd_frag_meta_t * meta = env->out_mcache;
    for( ulong i=0UL; i<accepted; i++ ) {
      FD_TEST( meta[ i ].seq == i );
      fd_txn_m_t * txnm = fd_chunk_to_laddr( state->verify_out.mem, meta[ i ].chunk );
      FD_TEST( txnm->bam.seq_id == 900U + (uint)i );
      FD_TEST( txnm->bam.txn_cnt == 1U );
    }

    test_bam_env_destroy( env );
  }
}

static void
test_bam_bundle_forwards_without_builder_info( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  FD_TEST( fd_mem_iszero( state->builder_pubkey, 32UL ) );
  /* seq_id 0 still yields a nonzero bundle_id. */
  uchar protobuf[512];
  size_t protobuf_sz = test_bam_build_scheduler_batch_msg( protobuf, sizeof(protobuf), 0U, 2, 1);
  test_bam_rx_sched( state, protobuf, protobuf_sz );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 2UL );
  FD_TEST( !state->metrics.atomic_batch_published_cnt && !state->metrics.transaction_published_cnt );
  FD_TEST( test_bam_env_drain_pending_txns( env ) == 2UL );
  FD_TEST( state->metrics.atomic_batch_published_cnt == 1UL );
  FD_TEST( state->metrics.transaction_published_cnt == 2UL );
  FD_TEST( state->feedback_queue_depth == 0UL );

  test_bam_check_published( env, 0UL, 0U, 2U, 0U, 1U, 1UL, 2UL );
  test_bam_check_published( env, 1UL, 0U, 2U, 1U, 1U, 1UL, 0UL );

  test_bam_env_destroy( env );
}

/* Delivers a scheduler message whose one batch ingress rejects, then
   flushes the result it queued at time now and returns its NotCommitted
   reason as the node decodes it. */
static bam_types_NotCommitted const *
test_bam_rejected_result( fd_bam_tile_t *              state,
                          uchar const *                protobuf,
                          ulong                        protobuf_sz,
                          uint                         seq_id,
                          long                         now,
                          test_bam_decoded_message_t * decoded ) {
  test_bam_rx_sched( state, protobuf, protobuf_sz );
  FD_TEST( bam_pending_txn_empty( state->pending_txns ) );
  FD_TEST( state->feedback_queue_depth==1UL );
  test_bam_prepare_scheduler_stream( state );
  g_clock = now;
  test_bam_keepalive_sync( state, g_clock );
  state->bam_last_config_poll_ns = g_clock;
  bam_types_AtomicTxnBatchResult const * result = test_bam_flush_one_result( state, seq_id, decoded );
  FD_TEST( result->which_result==bam_types_AtomicTxnBatchResult_not_committed_tag );
  return &result->result.not_committed;
}

/* Mixed or defaulted revert_on_error flags are rejected as
   INCONSISTENT_BUNDLE. */
static void
test_bam_bundle_revert_flag_cases( fd_wksp_t * wksp ) {
  struct {
    uint seq_id;
    struct { uchar payload, has_meta, has_flags, revert_on_error; } packet[ 2 ];
  } const cases[] = {
    /* Explicit true/false mismatch across two packets. */
    {  0U, { { 'Z', 1, 1, 1 }, { '[', 1, 1, 0 } } },
    /* First packet omits flags (defaults false), second sets true. */
    { 44U, { { 'x', 0, 0, 0 }, { 'y', 1, 1, 1 } } },
    /* First packet sets true, second omits flags (defaults false). */
    { 45U, { { 'u', 1, 1, 1 }, { 'v', 0, 0, 0 } } },
    /* has_meta without has_flags still defaults false and mismatches true. */
    { 46U, { { 'w', 1, 0, 0 }, { 'z', 1, 1, 1 } } },
  };

  for( ulong case_idx=0UL; case_idx<sizeof(cases)/sizeof(cases[0]); case_idx++ ) {
    test_bam_env_t env[1];
    test_bam_env_create( env, wksp );
    test_bam_env_mock_conn( env );

    bam_types_Packet packets[ 2 ];
    fd_memset( packets, 0, sizeof( packets ) );
    for( ulong i=0UL; i<2UL; i++ ) {
      packets[ i ].data.size                  = 1U;
      packets[ i ].data.bytes[0]              = cases[ case_idx ].packet[ i ].payload;
      packets[ i ].has_meta                   = cases[ case_idx ].packet[ i ].has_meta;
      packets[ i ].meta.has_flags             = cases[ case_idx ].packet[ i ].has_flags;
      packets[ i ].meta.flags.revert_on_error = cases[ case_idx ].packet[ i ].revert_on_error;
    }
    uchar protobuf[256];
    size_t protobuf_sz = test_bam_encode_scheduler_response( packets, 2UL, cases[ case_idx ].seq_id, protobuf, sizeof( protobuf ) );
    test_bam_decoded_message_t decoded;
    bam_types_NotCommitted const * nc = test_bam_rejected_result( env->state, protobuf, protobuf_sz, cases[ case_idx ].seq_id,
                                                                  (long)15e9+(long)case_idx*(long)1e9, &decoded );
    FD_TEST( nc->which_reason==bam_types_NotCommitted_deserialization_error_tag );
    FD_TEST( nc->reason.deserialization_error.reason==bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE );
    FD_TEST( nc->reason.deserialization_error.index==0U );
    test_bam_env_destroy( env );
  }
}

/* Ingress rejects a malformed batch whole and attributes the reason to
   its real sequence.  Ingress is also what keeps votes out of the BAM
   pipeline, whose consumers trust verify.  It
   parses every packet, so a vote is rejected whatever its signature,
   position, or scheduler metadata. */
static void
test_bam_ingress_rejects_batch_variants( fd_wksp_t * wksp ) {
  for( ulong variant=0UL; variant<8UL; variant++ ) {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_env_create( env, wksp );
    test_bam_env_mock_conn( env );

    bam_types_Packet packets[ FD_BAM_MAX_TXN_PER_ATOMIC_BATCH+1UL ];
    ulong            packet_cnt = 1UL;
    uint             deser_idx  = 0U;
    uint             reason     = bam_types_DeserializationErrorReason_VOTE_TRANSACTION_FAILURE;
    ulong            reject_idx = FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_VOTE_TXN_IDX;
    fd_memset( packets, 0, sizeof(packets) );
    switch( variant ) {
    case 0UL: /* forged: all signatures zeroed */
      test_bam_init_simple_vote_packet( &packets[0], 1U );
      fd_memset( packets[0].data.bytes+1UL, 0, 64UL*packets[0].data.bytes[0] );
      break;
    case 1UL: /* independent batch, vote after a non-vote packet.  As in
                 jito-solana, a multi-packet batch that does not revert on
                 error is rejected as inconsistent before its packets are
                 inspected: the node sends standalone transactions as
                 one-packet batches, and publishing the members
                 independently would commit some and misreport the batch. */
      packets[0].data.size      = 1U;
      packets[0].data.bytes[0]  = (uchar)'x';
      packets[0].has_meta       = 1U;
      packets[0].meta.has_flags = 1U;
      test_bam_init_simple_vote_packet( &packets[1], 0U );
      packet_cnt = 2UL;
      reason     = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
      reject_idx = FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_INVALID_BATCH_IDX;
      break;
    case 2UL: /* scheduler metadata claims it is not a vote */
      test_bam_init_simple_vote_packet( &packets[0], 1U );
      packets[0].meta.flags.simple_vote_tx = 0U;
      break;
    case 3UL: /* atomic batch, vote after a non-vote packet */
      packets[0].data.size                  = 1U;
      packets[0].data.bytes[0]              = (uchar)'x';
      packets[0].has_meta                   = 1U;
      packets[0].meta.has_flags             = 1U;
      packets[0].meta.flags.revert_on_error = 1U;
      test_bam_init_simple_vote_packet( &packets[1], 1U );
      packet_cnt = 2UL;
      deser_idx  = 1U;
      break;
    case 4UL: /* a real vote */
      test_bam_init_simple_vote_packet( &packets[0], 1U );
      break;
    case 5UL: /* revert flag consistency is checked before the vote */
      test_bam_init_simple_vote_packet( &packets[0], 1U );
      packets[1].data.size      = 1U;
      packets[1].data.bytes[0]  = (uchar)'x';
      packets[1].has_meta       = 1U;
      packets[1].meta.has_flags = 1U;
      packet_cnt = 2UL;
      reason     = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
      reject_idx = FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_INVALID_BATCH_IDX;
      break;
    case 6UL: /* more transactions than an AtomicTxnBatch may hold */
      packet_cnt = FD_BAM_MAX_TXN_PER_ATOMIC_BATCH+1UL;
      for( ulong i=0UL; i<packet_cnt; i++ ) {
        packets[ i ].data.size     = 1U;
        packets[ i ].data.bytes[0] = (uchar)( 'a'+(int)i );
      }
      reason     = bam_types_DeserializationErrorReason_SANITIZE_ERROR;
      reject_idx = FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_INVALID_BATCH_IDX;
      break;
    case 7UL: /* an empty child: treating it like an empty wrapper would
                 lose its real sequence or invent seq_id zero */
      packet_cnt = 0UL;
      reason     = bam_types_DeserializationErrorReason_EMPTY;
      reject_idx = FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_EMPTY_BATCH_IDX;
      break;
    }

    uint const seq_id = (uint)( 60UL+variant );
    uchar protobuf[ 2048 ];
    size_t protobuf_sz = test_bam_encode_scheduler_response( packets, packet_cnt, seq_id, protobuf, sizeof( protobuf ) );
    test_bam_decoded_message_t decoded;
    bam_types_NotCommitted const * nc = test_bam_rejected_result( state, protobuf, protobuf_sz, seq_id, (long)19e9, &decoded );
    FD_TEST( state->metrics.ingress_batch_rejected_cnt[ reject_idx ]==1UL );
    FD_TEST( nc->which_reason==bam_types_NotCommitted_deserialization_error_tag );
    FD_TEST( nc->reason.deserialization_error.reason==reason );
    FD_TEST( nc->reason.deserialization_error.index==deser_idx );

    test_bam_env_destroy( env );
  }
}

static void
test_bam_stale_slot_rejects_before_vote_error( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  state->bam_leader_state.slot = 45UL;

  bam_types_Packet packet[ 1 ];
  test_bam_init_simple_vote_packet( &packet[0], 1U );
  uchar protobuf[ 2048 ];
  size_t protobuf_sz = test_bam_encode_scheduler_response( packet, 1UL, 44U, protobuf, sizeof( protobuf ) );
  test_bam_decoded_message_t decoded;
  bam_types_NotCommitted const * nc = test_bam_rejected_result( state, protobuf, protobuf_sz, 44U, (long)18e9, &decoded );
  FD_TEST( nc->which_reason==bam_types_NotCommitted_scheduling_error_tag );
  FD_TEST( nc->reason.scheduling_error==bam_types_SchedulingError_OUTSIDE_LEADER_SLOT );

  test_bam_env_destroy( env );
}

static void
test_bam_generated_v1_protobuf_fixtures( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );

  struct {
    uchar const * protobuf;
    ulong         protobuf_sz;
    ushort        payload_sz;
    uint          seq_id;
  } const fixtures[] = {
    { bam_scheduler_response_v1_1233, bam_scheduler_response_v1_1233_sz, 1233U, 1233U },
    { bam_scheduler_response_v1_4096, bam_scheduler_response_v1_4096_sz, 4096U, 4096U },
  };

  for( ulong i=0UL; i<sizeof(fixtures)/sizeof(*fixtures); i++ ) {
    test_bam_rx_sched( state, fixtures[ i ].protobuf, fixtures[ i ].protobuf_sz );
    FD_TEST( bam_pending_txn_cnt( state->pending_txns )==1UL );

    fd_bam_pending_txn_t const * pending = bam_pending_txn_peek_head_const( state->pending_txns );
    FD_TEST( pending->seq_id==fixtures[ i ].seq_id );
    FD_TEST( pending->max_schedule_slot==42UL );
    FD_TEST( pending->payload_sz==fixtures[ i ].payload_sz );

    uchar txn_buf[ FD_TXN_MAX_SZ ];
    FD_TEST( fd_txn_parse( pending->payload, pending->payload_sz, txn_buf, NULL ) );
    fd_txn_t const * txn = (fd_txn_t const *)txn_buf;
    FD_TEST( txn->transaction_version==FD_TXN_V1 );
    FD_TEST( (ulong)txn->signature_off+FD_TXN_SIGNATURE_SZ==pending->payload_sz );
    FD_TEST( test_bam_env_drain_pending_txns( env )==1UL );
  }

  test_bam_env_destroy( env );
}

static void
test_bam_v1_packet_size_boundaries( fd_wksp_t * wksp ) {
  FD_STATIC_ASSERT( sizeof(((bam_types_Packet *)0)->data.bytes)==FD_TXN_MTU,
                    bam_packet_payload_holds_max_v1_transaction );

  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );

  ulong const accepted_sz[] = { 1232UL, 1233UL, FD_TXN_MTU };
  for( ulong i=0UL; i<sizeof(accepted_sz)/sizeof(*accepted_sz); i++ ) {
    bam_types_Packet packet = bam_types_Packet_init_default;
    packet.data.size = (pb_size_t)test_bam_build_v1_txn( packet.data.bytes, accepted_sz[ i ], (uchar)( 20U+i ) );
    packet.has_meta = 1U;
    packet.meta.size = accepted_sz[ i ];

    uchar protobuf[ FD_TXN_MTU+256UL ];
    size_t protobuf_sz = test_bam_encode_scheduler_response( &packet, 1UL, (uint32_t)( 510U+i ), protobuf, sizeof(protobuf) );
    test_bam_rx_sched( state, protobuf, protobuf_sz );

    FD_TEST( bam_pending_txn_cnt( state->pending_txns )==1UL );
    fd_bam_pending_txn_t const * pending = bam_pending_txn_peek_head_const( state->pending_txns );
    FD_TEST( pending->payload_sz==accepted_sz[ i ] );
    FD_TEST( !memcmp( pending->payload, packet.data.bytes, accepted_sz[ i ] ) );
    FD_TEST( test_bam_env_drain_pending_txns( env )==1UL );
  }
  FD_TEST( state->metrics.ingress_packet_oversize_cnt==0UL );

  /* The legacy and V0 parsers retain their original 1232-byte limit even
     though the BAM protobuf container now has room for V1. */
  uchar legacy_payload[ FD_TXN_MTU ];
  uchar txn_buf[ FD_TXN_MAX_SZ ];
  for( int v0=0; v0<2; v0++ ) {
    FD_TEST( test_bam_build_legacy_or_v0_txn( legacy_payload, (_Bool)v0 )==FD_TXN_MTU_V0 );
    FD_TEST( fd_txn_parse( legacy_payload, FD_TXN_MTU_V0, txn_buf, NULL ) );
    legacy_payload[ FD_TXN_MTU_V0 ] = 0U;
    FD_TEST( !fd_txn_parse( legacy_payload, FD_TXN_MTU_V0+1UL, txn_buf, NULL ) );
  }

  /* Encode the 4097-byte data field manually: the generated Packet type is
     deliberately bounded at 4096 and therefore cannot represent this input. */
  uchar oversized_payload[ FD_TXN_MTU+1UL ];
  fd_memset( oversized_payload, 0, sizeof(oversized_payload) );

  uchar raw_packet[ FD_TXN_MTU+32UL ];
  pb_ostream_t packet_stream = pb_ostream_from_buffer( raw_packet, sizeof(raw_packet) );
  test_bam_encode_string_field_raw( &packet_stream, bam_types_Packet_data_tag, oversized_payload, sizeof(oversized_payload) );

  uchar raw_batch[ FD_TXN_MTU+64UL ];
  pb_ostream_t batch_stream = pb_ostream_from_buffer( raw_batch, sizeof(raw_batch) );
  test_bam_encode_varint_field_raw( &batch_stream, bam_types_AtomicTxnBatch_seq_id_tag, 519U );
  test_bam_encode_varint_field_raw( &batch_stream, bam_types_AtomicTxnBatch_max_schedule_slot_tag, 1UL );
  test_bam_encode_string_field_raw( &batch_stream, bam_types_AtomicTxnBatch_packets_tag, raw_packet, packet_stream.bytes_written );

  uchar const * batches[] = { raw_batch };
  size_t const batch_sz[] = { batch_stream.bytes_written };
  uchar protobuf[ FD_TXN_MTU+128UL ];
  size_t protobuf_sz = test_bam_encode_scheduler_multi_batch_response_raw( batches, batch_sz, 1UL, protobuf, sizeof(protobuf) );
  test_bam_rx_sched( state, protobuf, protobuf_sz );

  FD_TEST( !bam_pending_txn_cnt( state->pending_txns ) );
  FD_TEST( state->metrics.ingress_packet_oversize_cnt==1UL );
  FD_TEST( state->metrics.ingress_batch_rejected_cnt[
               FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_INVALID_BATCH_IDX ]==1UL );
  FD_TEST( state->feedback_queue_depth==1UL );
  fd_bam_bundle_result_t const * result = &state->bam_results[ state->bam_results_head ];
  FD_TEST( result->seq_id==519U );
  FD_TEST( result->bundle_err==FD_BAM_BUNDLE_ERR_DESER );
  FD_TEST( result->deser_reason==bam_types_DeserializationErrorReason_SANITIZE_ERROR );
  FD_TEST( result->deser_index==0U );

  test_bam_env_destroy( env );
}

/* Legacy metadata is advisory and must not reject otherwise valid packet
   bytes, even when the reported size is nonsensical. */
static void
test_bam_bundle_uses_data_length_for_size_check( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );

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
  test_bam_rx_sched( state, protobuf, protobuf_sz );

  FD_TEST( state->metrics.ingress_packet_oversize_cnt == 0UL );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 1UL );
  fd_bam_pending_txn_t const * pending = bam_pending_txn_peek_head_const( state->pending_txns );
  FD_TEST( pending->payload_sz == bam_dump_txn_fixture_sz );
  FD_TEST( !memcmp( pending->payload, bam_dump_txn_fixture, bam_dump_txn_fixture_sz ) );
  ulong txn_t_sz = fd_txn_parse( bam_dump_txn_fixture, bam_dump_txn_fixture_sz, txn_buf, NULL );
  FD_TEST( pending->txn_t_sz == txn_t_sz );
  FD_TEST( !memcmp( pending->txn_t, txn_buf, txn_t_sz ) );
  FD_TEST( test_bam_env_drain_pending_txns( env ) == 1UL );
  FD_TEST( state->metrics.transaction_published_cnt == 1UL );

  fd_txn_m_t const * forwarded = fd_chunk_to_laddr_const( state->verify_out.mem, env->out_mcache[ 0 ].chunk );
  FD_TEST( forwarded->txn_t_sz == txn_t_sz );
  FD_TEST( !memcmp( fd_txn_m_txn_t_const( forwarded ), txn_buf, txn_t_sz ) );

  test_bam_env_destroy( env );
}

/* Ensure an InitSchedulerStream response that omits the batches array entirely
   is counted as an empty message without fabricating an AtomicTxnBatchResult. */
static void
test_bam_bundle_rejects_missing_batches( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );

  uchar protobuf[256];
  test_bam_rx_sched( state, protobuf, test_bam_encode_scheduler_multi_batch_response( NULL, 0UL, protobuf, sizeof(protobuf) ) );
  FD_TEST( bam_pending_txn_empty( state->pending_txns ) );
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

/* If queued work suppresses gRPC progress after END_STREAM, the client can
   neither drain nor reconnect.  Clear transport state and keep stepping. */
static void
test_bam_stream_end_with_pending_txn_reconnects1( fd_wksp_t * wksp,
                                                  int         waker ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  if( !waker ) test_bam_env_disable_waker( env );

  ulong fseq[1] = { 0UL };
  state->bam_status_fseq = fseq;

  test_bam_env_mock_conn( env );

  /* Queue scheduler work before exercising connection handoff/reset paths. */
  uchar protobuf[ 256 ];
  size_t protobuf_sz = test_bam_build_scheduler_batch_msg( protobuf, sizeof(protobuf), 77U, 1U, 0 );
  test_bam_rx_sched( state, protobuf, protobuf_sz );
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
  fd_bam_client_grpc_callbacks.rx_end( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream, &trailers );
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
  test_bam_set_server( state, fd_ushort_bswap( addr.sin_port ) );
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
  FD_TEST( opt_poll_in == 1 );
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

  test_bam_env_destroy( env );
}

/* Receive backpressure that starts after the socket was drained (no
   waker fire) must not report the ingest step's stale deadline, or a
   stem backpressured on bam_verif spins instead of parking until the
   credit return. */
static void
test_bam_receive_backpressure_parks_without_waker_fire( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  ulong fseq[1] = { FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE };
  state->bam_status_fseq = fseq;
  test_bam_env_mock_conn( env );
  fd_fseq_update( fseq, FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE ); /* the config response released it */

  uchar protobuf[ 256 ];
  size_t protobuf_sz = test_bam_build_scheduler_batch_msg( protobuf, sizeof(protobuf), 78U, 1U, 0 );
  test_bam_rx_sched( state, protobuf, protobuf_sz );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 1UL );

  /* The busy ingest step left next_step_deadline==0 and the socket is
     drained, so the waker does not fire again. */
  state->next_step_deadline = 0L;
  fd_fseq_update( state->waker_fseq, 0UL );
  int charge_busy = 0;
  fd_bam_test_before_credit( state, env->stem, &charge_busy );
  FD_TEST( !charge_busy );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 1UL );
  FD_TEST( fd_bam_test_next_deadline( state ) == LONG_MAX );

  /* A fire while paused is consumed and still parks. */
  fd_fseq_update( state->waker_fseq, 1UL );
  fd_bam_test_before_credit( state, env->stem, &charge_busy );
  FD_TEST( !charge_busy );
  FD_TEST( fd_fseq_query( state->waker_fseq ) == 0UL );
  FD_TEST( fd_bam_test_next_deadline( state ) == LONG_MAX );

  /* Once the queue drains, the consumed fire resumes as a fire: the
     client steps despite a later deadline (a step reads the clock). */
  test_bam_env_drain_pending_txns( env );
  state->next_step_deadline = LONG_MAX;
  g_clock_read_cnt = 0UL;
  fd_bam_test_before_credit( state, env->stem, &charge_busy );
  FD_TEST( !state->waker_paused && g_clock_read_cnt>1UL );

  /* A pause that consumed no fire left the waker armed, so resuming
     waits for the step deadline. */
  protobuf_sz = test_bam_build_scheduler_batch_msg( protobuf, sizeof(protobuf), 79U, 1U, 0 );
  test_bam_rx_sched( state, protobuf, protobuf_sz );
  FD_TEST( bam_pending_txn_cnt( state->pending_txns ) == 1UL );
  state->next_step_deadline = LONG_MAX;
  fd_fseq_update( state->waker_fseq, 0UL );
  fd_bam_test_before_credit( state, env->stem, &charge_busy );
  FD_TEST( state->waker_paused==2U );
  test_bam_env_drain_pending_txns( env );
  g_clock_read_cnt = 0UL;
  fd_bam_test_before_credit( state, env->stem, &charge_busy );
  FD_TEST( !state->waker_paused && g_clock_read_cnt==1UL );

  test_bam_env_destroy( env );
}

static void
test_bam_tcp_connect_completion_uses_so_error( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

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
test_bam_client_services_socket_and_flushes_tx1( fd_wksp_t * wksp,
                                                 int         waker ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  if( !waker ) test_bam_env_disable_waker( env );
  test_bam_env_mock_conn( env );

  /* Keep the protocol state machine idle so this isolates transport I/O. */
  state->bam_auth_inflight = 1U;

  int charge_busy = 0;
  g_clock_read_cnt = 0UL;
  fd_bam_client_step( state, &charge_busy );
  FD_TEST( g_clock_read_cnt==1UL );
  FD_TEST( state->tcp_sock>=0 );

  /* A queued frame flushes without requiring a new receive event. */
  ulong tx_bytes_before = state->grpc_metrics->stream_chunks_tx_bytes;
  FD_TEST( fd_h2_tx_ping( fd_grpc_client_h2_conn( state->grpc_client ),
                          fd_grpc_client_rbuf_tx( state->grpc_client ) ) );
  fd_bam_client_step( state, &charge_busy );
  FD_TEST( state->grpc_metrics->stream_chunks_tx_bytes==tx_bytes_before+sizeof(fd_h2_ping_t) );

  /* Native gRPC I/O polls a readable socket on every scheduled step. */
  uchar partial_h2 = 0U;
  FD_TEST( 1L==write( env->server_sock, &partial_h2, 1UL ) );
  fd_bam_client_step( state, &charge_busy );
  FD_TEST( state->grpc_metrics->stream_chunks_rx_bytes>=1UL );

  FD_TEST( 0==close( env->server_sock ) );
  env->server_sock = -1;

  /* A peer close is observed on the next socket step. */
  fd_bam_client_step( state, &charge_busy );
  FD_TEST( state->tcp_sock==-1 );
  FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_IO_IDX ]==1UL );

  test_bam_env_destroy( env );
}

/* Performance layout has no waker, so a scheduler message that arrives
   after an idle step must not wait for a protocol timer.  The tile polls
   an open socket FD_BAM_GRPC_RX_POLL_INTERVAL_NS after an idle step, and
   on the next loop after progress.  Without a socket it waits for backoff. */
static void
test_bam_polls_socket_without_waker( fd_wksp_t * wksp ) {
  long const saved_clock = g_clock;
  g_clock = (long)10e9;
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_disable_waker( env );
  test_bam_env_mock_conn( env );
  test_bam_set_server( state, 50055U );
  test_bam_keepalive_sync( state, g_clock );
  state->bam_auth_inflight = 1U; /* Keep the protocol state machine idle. */

  int busy = 0;
  fd_bam_test_before_credit( state, env->stem, &busy );
  FD_TEST( !busy && state->tcp_sock>=0 );
  FD_TEST( state->next_step_deadline==g_clock+FD_BAM_GRPC_RX_POLL_INTERVAL_NS );

  ulong rx_before = state->grpc_metrics->stream_chunks_rx_bytes;
  uchar partial_h2 = 0U;
  FD_TEST( 1L==write( env->server_sock, &partial_h2, 1UL ) );
  g_clock += FD_BAM_GRPC_RX_POLL_INTERVAL_NS-1L;
  fd_bam_test_before_credit( state, env->stem, &busy );
  FD_TEST( !busy && state->grpc_metrics->stream_chunks_rx_bytes==rx_before );
  g_clock++;
  fd_bam_test_before_credit( state, env->stem, &busy );
  FD_TEST( busy && state->grpc_metrics->stream_chunks_rx_bytes==rx_before+1UL );
  FD_TEST( state->next_step_deadline==0L );

  busy = 0;
  fd_bam_test_before_credit( state, env->stem, &busy );
  FD_TEST( !busy && state->next_step_deadline==g_clock+FD_BAM_GRPC_RX_POLL_INTERVAL_NS );

  FD_TEST( 0==close( env->server_sock ) );
  env->server_sock = -1;
  g_clock += FD_BAM_GRPC_RX_POLL_INTERVAL_NS;
  fd_bam_test_before_credit( state, env->stem, &busy );
  FD_TEST( state->tcp_sock==-1 );
  FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_IO_IDX ]==1UL );

  busy = 0;
  fd_bam_test_before_credit( state, env->stem, &busy );
  FD_TEST( state->backoff_until>g_clock+FD_BAM_GRPC_RX_POLL_INTERVAL_NS );
  FD_TEST( state->next_step_deadline==state->backoff_until );
  g_clock = state->backoff_until-1L;
  g_clock_read_cnt = 0UL;
  fd_bam_test_before_credit( state, env->stem, &busy );
  FD_TEST( g_clock_read_cnt==1UL && state->tcp_sock==-1 ); /* no step */

  test_bam_env_destroy( env );
  g_clock = saved_clock;
}

/* Only a waker client registers its socket with the inner epoll set.
   Without a waker the pending connect is polled instead. */
static void
test_bam_connect_without_waker_skips_epoll( fd_wksp_t * wksp ) {
  for( int waker=1; waker>=0; waker-- ) {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_env_create( env, wksp );
    if( !waker ) test_bam_env_disable_waker( env );

    struct sockaddr_in addr;
    int listen_sock = test_bam_loopback_listener( &addr );
    test_bam_set_server( state, fd_ushort_bswap( addr.sin_port ) );
    state->backoff_until   = 0L;

    int busy = 0;
    fd_bam_test_before_credit( state, env->stem, &busy );
    FD_TEST( busy && state->tcp_sock>=0 && !state->tcp_sock_connected );
    FD_TEST( state->sock_in_epoll==(uint)waker && state->epoll_out_armed==(uint)waker );
    env->server_sock = accept4( listen_sock, NULL, NULL, SOCK_CLOEXEC );
    FD_TEST( env->server_sock>=0 );
    FD_TEST( 0==close( listen_sock ) );

    int del = epoll_ctl( FD_WAKER_INNER_FD( 0 ), EPOLL_CTL_DEL, state->tcp_sock, NULL );
    FD_TEST( waker ? !del : ( del==-1 && errno==ENOENT ) );
    if( !waker ) {
      FD_TEST( state->next_step_deadline<=g_clock+FD_BAM_GRPC_RX_POLL_INTERVAL_NS );
      g_clock += FD_BAM_GRPC_RX_POLL_INTERVAL_NS;
      fd_bam_test_before_credit( state, env->stem, &busy );
      FD_TEST( state->tcp_sock_connected );
    }
    test_bam_env_destroy( env );
  }
}

/* A peer that accepts TCP but never answers the HTTP/2 preface is reset
   FD_BAM_CONNECT_TIMEOUT_NS after the dial (not 1 ns before), which
   next_deadline schedules.  conn_established disarms the timeout. */
static void
test_bam_connect_timeout_resets_stalled_handshake( fd_wksp_t * wksp ) {
  long const saved_clock = g_clock;
  for( int hs=0; hs<2; hs++ ) {
    g_clock = (long)10e9;
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_env_create( env, wksp );

    struct sockaddr_in addr;
    int listen_sock = test_bam_loopback_listener( &addr );
    test_bam_set_server( state, fd_ushort_bswap( addr.sin_port ) );
    state->backoff_until = 0L;

    int busy = 0;
    fd_bam_client_step( state, &busy );
    long const deadline = g_clock+FD_BAM_CONNECT_TIMEOUT_NS;
    FD_TEST( busy && state->tcp_sock>=0 && !state->tcp_sock_connected );
    FD_TEST( state->connect_deadline==deadline );
    FD_TEST( fd_bam_client_next_deadline( state, g_clock )==deadline );
    env->server_sock = accept4( listen_sock, NULL, NULL, SOCK_CLOEXEC );
    FD_TEST( env->server_sock>=0 );
    FD_TEST( 0==close( listen_sock ) );

    fd_bam_client_step( state, &busy );
    FD_TEST( state->tcp_sock_connected );
    if( hs ) {
      /* The HTTP/2 handshake completes through the gRPC client callback */
      fd_h2_conn_t * conn = fd_grpc_client_h2_conn( state->grpc_client );
      conn->flags = 0;
      fd_grpc_client_h2_callbacks.conn_established( conn );
      FD_TEST( fd_grpc_client_is_connected( state->grpc_client ) && state->connect_deadline==LONG_MAX );
    } else {
      FD_TEST( fd_bam_client_next_deadline( state, g_clock )==deadline );
    }

    g_clock = deadline-1L;
    fd_bam_client_step( state, &busy );
    FD_TEST( state->tcp_sock>=0 );
    g_clock = deadline;
    busy = 0;
    fd_bam_client_step( state, &busy );
    FD_TEST( (state->tcp_sock>=0)==hs && ( hs || busy ) );
    FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_CONNECT_TIMEOUT_IDX ]==(ulong)!hs );

    test_bam_env_destroy( env );
  }
  g_clock = saved_clock;
}

/* The keepalive PING is withheld only from a closing connection, and
   the step is busy only when a PING is queued.  A peer that stalls in a
   header block leaves CONTINUATION set: the PING still goes out, so its
   timeout resets the connection. */
static void
test_bam_keepalive_pings_during_continuation( fd_wksp_t * wksp ) {
  long const saved_clock = g_clock;
  g_clock = (long)10e9;
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  test_bam_keepalive_sync( state, g_clock );
  state->keepalive->ts_next_tx = g_clock;
  state->bam_auth_inflight     = 1U; /* Keep the protocol state machine idle. */
  state->bam_config_inflight   = 1U;
  fd_h2_conn_t * conn = fd_grpc_client_h2_conn( state->grpc_client );
  uchar const ping_tx = conn->ping_tx;

  conn->flags = FD_H2_CONN_FLAGS_SEND_GOAWAY;
  FD_TEST( !fd_bam_client_step_reconnect( state, g_clock ) );
  FD_TEST( conn->ping_tx==ping_tx && !state->keepalive->inflight );

  conn->flags = FD_H2_CONN_FLAGS_CONTINUATION;
  FD_TEST( fd_bam_client_step_reconnect( state, g_clock ) );
  FD_TEST( conn->ping_tx==ping_tx+1 && state->keepalive->inflight );

  long const deadline = state->keepalive->ts_deadline;
  int busy = 0;
  g_clock = deadline-1L;
  fd_bam_client_step( state, &busy );
  FD_TEST( state->tcp_sock>=0 );
  g_clock = deadline;
  busy = 0;
  fd_bam_client_step( state, &busy );
  FD_TEST( busy && state->tcp_sock==-1 );
  FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_KEEPALIVE_TIMEOUT_IDX ]==1UL );

  test_bam_env_destroy( env );
  g_clock = saved_clock;
}

/* Returning early for an incomplete END_STREAM leaves the scheduler stream
   live and reconnect reuses stale resolution state.  Release and reset it. */
static void
test_bam_grpc_end_handling( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );

  /* A failed, then a successful, end of the scheduler stream */
  for( ulong ok=0UL; ok<2UL; ok++ ) {
    state->defer_reset = 0U;
    fd_grpc_h2_stream_t * stream = fd_grpc_client_stream_acquire( state->grpc_client, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
    FD_TEST( stream );
    stream->hdrs.h2_status     = 200;
    stream->hdrs.is_grpc_proto = 1;
    state->bam_stream = stream;
    fd_bam_client_grpc_callbacks.rx_start( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
    FD_TEST( state->bam_stream_live == 1U );
    state->bam_leader_pending = 1U;

    fd_grpc_resp_hdrs_t hdrs = { .h2_status = 200, .grpc_status = ok ? FD_GRPC_STATUS_OK : FD_GRPC_STATUS_UNAVAILABLE };
    fd_bam_client_grpc_callbacks.rx_end( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream, &hdrs );
    FD_TEST( state->bam_stream_live == 0U );
    FD_TEST( state->bam_stream == NULL );
    FD_TEST( state->bam_leader_pending == 0U );
    FD_TEST( state->defer_reset == 1U );
    FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_REQUEST_FAILED_IDX ] == 1UL );
    FD_TEST( state->metrics.leader_pending_dropped_cnt[ FD_METRICS_ENUM_BAM_LEADER_PENDING_DROP_REASON_V_REQUEST_FAILED_IDX ] == 1UL );
    FD_TEST( state->metrics.leader_pending_dropped_cnt[ FD_METRICS_ENUM_BAM_LEADER_PENDING_DROP_REASON_V_STREAM_ENDED_IDX ] == ok );
  }

  test_bam_env_destroy( env );
}

/* Runs one scheduler stream attempt that the BAM node accepts with
   headers and then rejects, then the client steps that reset it and log
   the disconnect.  Returns whether the rejection was treated as a repeat. */
static int
test_bam_scheduler_reject_cycle( test_bam_env_t * env,
                                 uint             grpc_status,
                                 char const *     grpc_msg ) {
  fd_bam_tile_t * state = env->state;
  test_bam_env_mock_conn_empty( env );
  test_bam_env_mock_h2_hs( state );
  test_bam_prepare_scheduler_stream( state );
  fd_bam_client_grpc_callbacks.rx_start( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  state->bam_status_logged = FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY;

  fd_grpc_resp_hdrs_t trailers = { .h2_status=200U, .grpc_status=grpc_status };
  trailers.grpc_msg_len = (uint)strlen( grpc_msg );
  fd_memcpy( trailers.grpc_msg, grpc_msg, trailers.grpc_msg_len );
  fd_bam_client_grpc_callbacks.rx_end( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream, &trailers );
  FD_TEST( state->defer_reset && !state->bam_stream_live );
  int repeat = state->bam_stream_reject_repeat;

  /* A disconnect within 1 ms of the previous status log is logged by a
     later step, which must still see the repeat. */
  state->last_bam_status_log_nanos = LONG_MAX/2L;
  int charge_busy = 0;
  fd_bam_client_step( state, &charge_busy );
  FD_TEST( state->tcp_sock<0 && !state->defer_reset );
  FD_TEST( state->bam_status_logged==FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY );
  FD_TEST( state->bam_stream_reject_repeat==(uint)repeat );

  state->last_bam_status_log_nanos = 0L;
  fd_bam_client_step( state, &charge_busy );
  FD_TEST( state->bam_status_logged==FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED );
  FD_TEST( !state->bam_stream_reject_repeat );
  return repeat;
}

/* Outside the leader schedule every leader schedule recheck is rejected
   with the same PERMISSION_DENIED.  Only the first rejection of a streak
   may warn; a scheduled validator, another reason, or a healthy session
   restarts the streak. */
static void
test_bam_scheduler_not_scheduled_reject_streak( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  char const not_scheduled[] = "Validator is not on the leader schedule: 7heQNXEtxSv3wV8sNbuQsDN5xNGbbpLvHGiyXuJxEf7n";
  char const other[]         = "Validator is not allowed";
  uint const denied          = FD_GRPC_STATUS_PERMISSION_DENIED;

  /* Without replay, the leader schedule is unknown. */
  FD_TEST( !test_bam_scheduler_reject_cycle( env, denied, not_scheduled ) );
  FD_TEST( !test_bam_scheduler_reject_cycle( env, denied, not_scheduled ) );
  FD_TEST( !state->bam_stream_reject_hash );

  state->replay_in_idx = 2UL;
  FD_TEST( state->next_leader_slot==ULONG_MAX );
  FD_TEST( !test_bam_scheduler_reject_cycle( env, denied, not_scheduled ) );
  FD_TEST(  test_bam_scheduler_reject_cycle( env, denied, not_scheduled ) );
  FD_TEST(  test_bam_scheduler_reject_cycle( env, denied, not_scheduled ) );

  FD_TEST( !test_bam_scheduler_reject_cycle( env, denied, other ) );
  FD_TEST(  test_bam_scheduler_reject_cycle( env, denied, other ) );
  ulong other_hash = state->bam_stream_reject_hash;
  fd_grpc_resp_hdrs_t config_reject = { .h2_status=200U, .grpc_status=FD_GRPC_STATUS_UNAVAILABLE };
  config_reject.grpc_msg_len = (uint)strlen( other );
  fd_memcpy( config_reject.grpc_msg, other, config_reject.grpc_msg_len );
  fd_bam_client_grpc_callbacks.rx_end( state, FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig, &config_reject );
  FD_TEST( state->bam_stream_reject_hash==other_hash );
  FD_TEST(  test_bam_scheduler_reject_cycle( env, denied, other ) );
  FD_TEST( !test_bam_scheduler_reject_cycle( env, denied, not_scheduled ) );

  FD_TEST( !test_bam_scheduler_reject_cycle( env, FD_GRPC_STATUS_UNAVAILABLE, not_scheduled ) );
  FD_TEST( !test_bam_scheduler_reject_cycle( env, denied, not_scheduled ) );
  FD_TEST(  test_bam_scheduler_reject_cycle( env, denied, not_scheduled ) );

  state->next_leader_slot = 500UL;
  FD_TEST( !test_bam_scheduler_reject_cycle( env, denied, not_scheduled ) );
  FD_TEST( !test_bam_scheduler_reject_cycle( env, denied, not_scheduled ) );
  FD_TEST( !state->bam_stream_reject_hash );

  state->next_leader_slot = ULONG_MAX;
  FD_TEST( !test_bam_scheduler_reject_cycle( env, denied, not_scheduled ) );
  FD_TEST(  test_bam_scheduler_reject_cycle( env, denied, not_scheduled ) );

  test_bam_env_mock_conn( env );
  int charge_busy = 0;
  fd_bam_client_step( state, &charge_busy );
  FD_TEST( state->bam_status_counted==FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
  FD_TEST( !state->bam_stream_reject_hash );
  FD_TEST( !test_bam_scheduler_reject_cycle( env, denied, not_scheduled ) );
  FD_TEST(  test_bam_scheduler_reject_cycle( env, denied, not_scheduled ) );

  state->epoch_slot_cnt = 100UL; state->bam_admitted_epoch_end = 100UL; /* the node revokes admission */
  FD_TEST(  test_bam_scheduler_reject_cycle( env, denied, not_scheduled ) && !state->bam_admitted_epoch_end );

  test_bam_env_destroy( env );
}

/* Opens a scheduler stream on a fresh connection and delivers its
   response headers. */
static void
test_bam_scheduler_stream_headers( test_bam_env_t * env ) {
  fd_bam_tile_t * state = env->state;
  test_bam_env_mock_conn_empty( env );
  test_bam_env_mock_h2_hs( state );
  test_bam_env_inject_config_response( state );
  test_bam_prepare_scheduler_stream( state );
  fd_bam_client_grpc_callbacks.rx_start( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  test_bam_keepalive_sync( state, fd_bam_now() );
}

/* The BAM node answers the scheduler stream with HTTP 200 before it
   checks the auth proof, and a stream it rejects discards what it
   carried.  Queued results wait for the node's first BuilderHeartBeat,
   which also counts the stream as accepted.  A rejection repeated while
   no leader slot is known drops them, so the leader schedule gate can
   hold off redials. */
static void
test_bam_scheduler_results_wait_for_acceptance( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  ulong const * transition = state->metrics.stream_transition_cnt;
  ulong const * outcome    = state->metrics.outbound_enqueue_outcome_cnt;
  ulong const   d2l        = FD_METRICS_ENUM_BAM_STREAM_TRANSITION_TYPE_V_DOWN_TO_LIVE_IDX;
  ulong const   l2d        = FD_METRICS_ENUM_BAM_STREAM_TRANSITION_TYPE_V_LIVE_TO_DOWN_IDX;
  ulong const   sent       = FD_METRICS_ENUM_BAM_ENQUEUE_RESULT_V_RESULT_ENQUEUED_IDX;
  char const not_scheduled[] = "Validator is not on the leader schedule: 7heQNXEtxSv3wV8sNbuQsDN5xNGbbpLvHGiyXuJxEf7n";
  fd_grpc_resp_hdrs_t denied = { .h2_status=200U, .grpc_status=FD_GRPC_STATUS_PERMISSION_DENIED };
  denied.grpc_msg_len = (uint)strlen( not_scheduled );
  fd_memcpy( denied.grpc_msg, not_scheduled, denied.grpc_msg_len );
  fd_grpc_resp_hdrs_t ended = { .h2_status=200U, .grpc_status=FD_GRPC_STATUS_UNAVAILABLE };
  int charge_busy = 0;

  fd_bam_bundle_result_t res = test_make_bundle_result( 1U, 100UL, 1 );
  res.scheduler_gen = state->scheduler_gen;
  fd_bam_enqueue_result( state, &res );
  state->replay_in_idx = 2UL;
  state->next_leader_slot  = 500UL;

  /* Rejected after the headers: the result stays queued. */
  test_bam_scheduler_stream_headers( env );
  fd_bam_client_step_reconnect( state, fd_bam_now() );
  FD_TEST( state->feedback_queue_depth==1U && !outcome[ sent ] );
  fd_bam_client_grpc_callbacks.rx_end( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream, &denied );
  fd_bam_client_step( state, &charge_busy );
  FD_TEST( state->feedback_queue_depth==1U && !state->metrics.feedback_results_dropped_cnt );
  FD_TEST( !transition[ d2l ] && !transition[ l2d ] );

  /* Accepted: the node's first BuilderHeartBeat releases it. */
  test_bam_scheduler_stream_headers( env );
  fd_bam_client_step_reconnect( state, fd_bam_now() );
  FD_TEST( state->feedback_queue_depth==1U && !outcome[ sent ] );
  test_bam_send_scheduler_heartbeat( state, 0UL );
  test_bam_send_scheduler_heartbeat( state, 0UL );
  FD_TEST( transition[ d2l ]==1UL );
  fd_bam_client_step_reconnect( state, fd_bam_now() );
  FD_TEST( !state->feedback_queue_depth && outcome[ sent ]==1UL );
  fd_bam_client_grpc_callbacks.rx_end( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream, &ended );
  FD_TEST( transition[ l2d ]==1UL );
  fd_bam_client_step( state, &charge_busy );

  /* The same rejection repeated while no leader slot is known drops the
     result, and the leader schedule gate then holds off redials. */
  fd_bam_enqueue_result( state, &res );
  state->next_leader_slot = ULONG_MAX;
  test_bam_scheduler_stream_headers( env );
  fd_bam_client_grpc_callbacks.rx_end( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream, &denied );
  fd_bam_client_step( state, &charge_busy );
  FD_TEST( state->feedback_queue_depth==1U && !fd_bam_tile_leader_schedule_gate_active( state ) );
  test_bam_scheduler_stream_headers( env );
  fd_bam_client_grpc_callbacks.rx_end( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream, &denied );
  FD_TEST( !state->feedback_queue_depth && state->metrics.feedback_results_dropped_cnt==1UL );
  FD_TEST( state->bam_results_head==state->bam_results_tail );
  FD_TEST( transition[ d2l ]==1UL && transition[ l2d ]==1UL );
  fd_bam_client_step( state, &charge_busy );
  FD_TEST( fd_bam_tile_leader_schedule_gate_active( state ) );

  test_bam_env_destroy( env );
}

static void
test_bam_grpc_timeout( fd_wksp_t * wksp ) {
  /* Deadline expiry cancels inflight auth/scheduler attempts and marks the
     client for reset. */
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );

  state->bam_auth_inflight      = 1U;
  state->bam_auth_ready         = 1U;
  fd_cstr_ncpy( state->challenge_to_sign, "stale-challenge", sizeof( state->challenge_to_sign ) );
  fd_bam_client_grpc_callbacks.rx_timeout( state, FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge, FD_GRPC_DEADLINE_HEADER );
  FD_TEST( state->bam_auth_inflight == 0U );
  FD_TEST( state->bam_auth_ready == 0U );
  FD_TEST( state->challenge_to_sign[ 0 ] == '\0' );
  FD_TEST( state->defer_reset == 1U );
  FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_REQUEST_TIMEOUT_IDX ] == 1UL );

  state->defer_reset = 0U;
  state->bam_stream_live       = 1U;
  state->bam_leader_pending    = 1U;
  fd_grpc_h2_stream_t * timeout_stream = fd_grpc_client_stream_acquire( state->grpc_client, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( timeout_stream );
  state->bam_stream = timeout_stream;
  fd_bam_client_grpc_callbacks.rx_timeout( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream, FD_GRPC_DEADLINE_RX_END );
  FD_TEST( state->bam_stream_live == 0U );
  FD_TEST( state->bam_leader_pending == 0U );
  FD_TEST( state->defer_reset == 1U );
  FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_REQUEST_TIMEOUT_IDX ] == 2UL );

  test_bam_env_destroy( env );
}

/* A healthy connection with a live scheduler stream at time now. */
static fd_bam_tile_t *
test_bam_live_env_create( test_bam_env_t * env,
                          fd_wksp_t *      wksp,
                          long             now ) {
  g_clock = now;
  test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
  test_bam_prepare_scheduler_stream( env->state );
  test_bam_keepalive_sync( env->state, now );
  return env->state;
}

static void
test_bam_config_timeout_keeps_scheduler_live( fd_wksp_t * wksp ) {
  int const deadline_kind[2] = { FD_GRPC_DEADLINE_HEADER, FD_GRPC_DEADLINE_RX_END };
  for( ulong i=0UL; i<2UL; i++ ) {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_live_env_create( env, wksp, (long)10e9 );
    test_bam_set_server( state, 50055U );
    state->keepalive->interval   = 0L;
    state->keepalive->ts_next_tx = LONG_MAX;
    state->keepalive->ts_deadline = LONG_MAX;
    state->keepalive->inflight = 0U;

    ulong status[1] = { FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE };
    state->bam_status_fseq = status;

    /* Keep a successful prior config while its refresh is outstanding.
       A hung config RPC is independent of the live scheduler. */
    state->bam_config_inflight = 1U;
    state->bam_last_config_poll_ns = g_clock-FD_BAM_CLIENT_REQUEST_TIMEOUT;
    fd_grpc_h2_stream_t * stream = state->bam_stream;
    uchar commission = state->builder_commission;
    fd_ip4_port_t tpu = state->bam_tpu;
    long backoff = state->backoff_until;
    fd_bam_bundle_result_t result = test_make_bundle_result( 777U+(uint)i, 1UL, 1U );
    test_enqueue_bundle_result( state, &result );

    FD_TEST( fd_bam_client_status( state )==FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
    fd_bam_client_grpc_callbacks.rx_timeout( state, FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig, deadline_kind[i] );
    FD_TEST( !state->bam_config_inflight && state->bam_last_config_poll_ns==g_clock );
    FD_TEST( !state->defer_reset && state->backoff_until==backoff );
    FD_TEST( state->bam_stream==stream && state->bam_stream_live );
    FD_TEST( state->bam_config_received && state->builder_commission==commission && state->bam_tpu.l==tpu.l );
    FD_TEST( state->feedback_queue_depth==1U );
    FD_TEST( fd_fseq_query( state->bam_status_fseq )==FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
    FD_TEST( fd_bam_client_status( state )==FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
    FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_REQUEST_TIMEOUT_IDX ]==1UL );
    FD_TEST( fd_bam_client_next_deadline( state, g_clock )==g_clock+(long)5e9 ); /* heartbeat */

    /* Feedback continues while this config RPC waits for its own retry. */
    FD_TEST( fd_bam_client_step_reconnect( state, g_clock )==1 );
    FD_TEST( !state->feedback_queue_depth && !state->bam_config_inflight );
    test_bam_decoded_message_t decoded;
    test_bam_expect_msg( state, &decoded, bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
    FD_TEST( decoded.multi.result_cnt==1UL && decoded.multi.results[0].seq_id==result.seq_id );

    g_clock += (long)60e9;
    FD_TEST( fd_bam_client_step_reconnect( state, g_clock )==1 );
    FD_TEST( state->bam_config_inflight && state->bam_last_config_poll_ns==g_clock );
    FD_TEST( state->bam_stream==stream && state->bam_stream_live );
    FD_TEST( fd_fseq_query( state->bam_status_fseq )==FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );

    test_bam_env_destroy( env );
  }
}

static ulong
test_bam_encode_config_candidate( uchar * protobuf,
                                  ulong   protobuf_max ) {
  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  uchar pubkey[32] = { 9U, 8U, 7U, 6U, 5U };
  test_bam_config_set_builder( &resp, pubkey, 19U );
  test_bam_config_set_tpu( &resp, "127.0.0.3", 10007U, "127.0.0.4", 10008U );
  return test_bam_encode_config( &resp, protobuf, protobuf_max );
}

/* Config DATA is provisional until the unary RPC completes successfully.
   While it is pending, a due refresh must not start a second request. */
static void
test_bam_config_rpc_applies_only_after_ok( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_live_env_create( env, wksp, (long)10e9 );
  state->keepalive->interval = 0L;
  state->bam_last_validator_heartbeat_ns = g_clock;

  ulong status[1] = { FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE };
  state->bam_status_fseq = status;
  ulong status_before = fd_fseq_query( state->bam_status_fseq );
  uchar commission_before = state->builder_commission;
  fd_ip4_port_t tpu_before = state->bam_tpu;
  uint fee_version_before = state->fee_cfg_version;
  long poll_before = g_clock-(long)60e9;
  state->bam_last_config_poll_ns = poll_before;
  state->bam_config_inflight = 1U;

  uchar protobuf[256];
  ulong protobuf_sz = test_bam_encode_config_candidate( protobuf, sizeof(protobuf) );
  fd_bam_client_grpc_callbacks.rx_msg( state, protobuf, protobuf_sz, FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );
  FD_TEST( state->bam_config_pending_received && !state->bam_config_pending_invalid );
  FD_TEST( state->bam_config_inflight && state->bam_config_received );
  FD_TEST( state->builder_commission==commission_before && state->bam_tpu.l==tpu_before.l );
  FD_TEST( state->fee_cfg_version==fee_version_before );
  FD_TEST( fd_fseq_query( state->bam_status_fseq )==status_before );

  /* The poll is overdue, but DATA alone must leave the RPC inflight. */
  fd_grpc_h2_stream_t * stream = state->bam_stream;
  FD_TEST( !fd_bam_client_step_reconnect( state, g_clock ) );
  FD_TEST( state->bam_config_inflight && state->bam_last_config_poll_ns==poll_before );
  FD_TEST( state->bam_stream==stream && state->bam_stream_live );

  fd_grpc_resp_hdrs_t ok = { .h2_status=200U, .grpc_status=FD_GRPC_STATUS_OK };
  fd_bam_client_grpc_callbacks.rx_end( state, FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig, &ok );
  FD_TEST( !state->bam_config_inflight && !state->bam_config_pending_received && !state->bam_config_pending_invalid );
  FD_TEST( state->builder_commission==19U && state->builder_commission!=commission_before );
  FD_TEST( state->bam_tpu.l!=tpu_before.l && state->fee_cfg_version==fee_version_before+1U );
  FD_TEST( state->bam_stream==stream && state->bam_stream_live && !state->defer_reset );

  test_bam_env_destroy( env );
}

/* Failed config polls are independent of the bidirectional scheduler stream.
   Heartbeats, leader snapshots, and durable results must progress before the
   next config retry, with the last good config and ownership intact. */
static void
test_bam_config_status_failure_keeps_scheduler_live( fd_wksp_t * wksp ) {
  for( ulong i=0UL; i<4UL; i++ ) {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_live_env_create( env, wksp, (long)10e9 );
    state->keepalive->interval = 0L;
    test_bam_set_server( state, 50055U );

    ulong status[1] = { FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE };
    state->bam_status_fseq = status;
    fd_grpc_h2_stream_t * stream = state->bam_stream;
    uchar commission_before = state->builder_commission;
    fd_ip4_port_t tpu_before = state->bam_tpu;
    uint fee_version_before = state->fee_cfg_version;
    long backoff_before = state->backoff_until;
    state->bam_last_config_poll_ns = g_clock-(long)2e9;
    state->bam_config_inflight = 1U;

    uchar protobuf[256];
    ulong protobuf_sz = test_bam_encode_config_candidate( protobuf, sizeof(protobuf) );
    if( i ) {
      fd_bam_client_grpc_callbacks.rx_msg( state, protobuf, protobuf_sz, FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );
      FD_TEST( state->bam_config_pending_received && state->bam_config_inflight );
    }
    fd_grpc_resp_hdrs_t fail = {
      .h2_status = i ? 200U : 503U,
      .grpc_status = i==3UL ? FD_GRPC_STATUS_UNAUTHENTICATED :
                     i==2UL ? FD_GRPC_STATUS_UNKNOWN :
                     i==1UL ? FD_GRPC_STATUS_UNAVAILABLE : FD_GRPC_STATUS_OK,
    };
    fd_bam_client_grpc_callbacks.rx_end( state, FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig, &fail );
    FD_TEST( !state->bam_config_inflight && !state->bam_config_pending_received && !state->bam_config_pending_invalid );
    FD_TEST( state->bam_last_config_poll_ns==g_clock && state->backoff_until==backoff_before );
    FD_TEST( !state->defer_reset && state->bam_stream==stream && state->bam_stream_live );
    FD_TEST( state->bam_auth_ready );
    FD_TEST( state->bam_config_received && state->builder_commission==commission_before );
    FD_TEST( state->bam_tpu.l==tpu_before.l && state->fee_cfg_version==fee_version_before );
    FD_TEST( fd_fseq_query( state->bam_status_fseq )==FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
    FD_TEST( fd_bam_client_status( state )==FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );

    fd_bam_bundle_result_t result = test_make_bundle_result( 880U+(uint)i, 42UL, 1U );
    test_enqueue_bundle_result( state, &result );
    state->bam_last_validator_heartbeat_ns = g_clock-(long)5e9;
    state->bam_leader_pending = 1U;
    state->bam_leader_state = (fd_bam_leader_state_t){
      .slot=42UL, .tick=7U, .slot_cu_budget_remaining=123U
    };
    test_bam_decoded_message_t decoded;
    FD_TEST( fd_bam_client_step_reconnect( state, g_clock )==1 );
    test_bam_expect_msg( state, &decoded, bam_api_SchedulerMessageV0_heart_beat_tag );
    FD_TEST( fd_bam_client_step_reconnect( state, g_clock )==1 );
    test_bam_expect_msg( state, &decoded, bam_api_SchedulerMessageV0_leader_state_tag );
    FD_TEST( fd_bam_client_step_reconnect( state, g_clock )==1 );
    test_bam_expect_msg( state, &decoded, bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
    FD_TEST( decoded.multi.result_cnt==1UL && decoded.multi.results[0].seq_id==result.seq_id );
    FD_TEST( !state->feedback_queue_depth && !state->bam_config_inflight );

    /* A failed poll must not shorten the 60 s cadence. */
    FD_TEST( fd_bam_client_step_reconnect( state, g_clock+(long)30e9 )==1 && !state->bam_config_inflight );
    test_bam_decode_last_message( state, &decoded ); /* drain the heartbeat */

    g_clock += (long)60e9;
    FD_TEST( fd_bam_client_step_reconnect( state, g_clock )==1 );
    FD_TEST( state->bam_config_inflight && state->bam_stream==stream && state->bam_stream_live );

    test_bam_env_destroy( env );
  }
}

static void
test_bam_config_rpc_discards_incomplete_or_invalid_response( fd_wksp_t * wksp ) {
  /* No DATA, duplicate DATA, malformed DATA, timeout after DATA, and reset
     after DATA all leave the last committed config unchanged. */
  for( ulong kind=0UL; kind<5UL; kind++ ) {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_live_env_create( env, wksp, (long)10e9 );
    uchar commission_before = state->builder_commission;
    fd_ip4_port_t tpu_before = state->bam_tpu;
    uint fee_version_before = state->fee_cfg_version;
    fd_grpc_h2_stream_t * stream = state->bam_stream;
    state->bam_config_inflight = 1U;

    uchar protobuf[256];
    ulong protobuf_sz = test_bam_encode_config_candidate( protobuf, sizeof(protobuf) );
    if( kind && kind!=2UL ) {
      fd_bam_client_grpc_callbacks.rx_msg( state, protobuf, protobuf_sz, FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );
      FD_TEST( state->bam_config_pending_received && !state->bam_config_pending_invalid );
    }
    if( kind==1UL ) {
      fd_bam_client_grpc_callbacks.rx_msg( state, protobuf, protobuf_sz, FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );
      FD_TEST( state->bam_config_pending_invalid );
    } else if( kind==2UL ) {
      uchar malformed[] = { 0xffU };
      fd_bam_client_grpc_callbacks.rx_msg( state, malformed, sizeof(malformed), FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );
      FD_TEST( state->bam_config_pending_invalid );
      fd_bam_client_grpc_callbacks.rx_msg( state, protobuf, protobuf_sz, FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );
      FD_TEST( !state->bam_config_pending_received && state->bam_config_pending_invalid );
    }
    if( kind==3UL ) {
      fd_bam_client_grpc_callbacks.rx_timeout( state, FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig, FD_GRPC_DEADLINE_RX_END );
    } else if( kind==4UL ) {
      fd_bam_client_reset( state );
    } else {
      fd_grpc_resp_hdrs_t ok = { .h2_status=200U, .grpc_status=FD_GRPC_STATUS_OK };
      fd_bam_client_grpc_callbacks.rx_end( state, FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig, &ok );
    }

    FD_TEST( !state->bam_config_inflight && !state->bam_config_pending_received && !state->bam_config_pending_invalid );
    /* A reset (kind 4) forgets the session's builder, not the published fee config. */
    FD_TEST( state->builder_commission==( kind==4UL ? 0U : commission_before ) && state->bam_tpu.l==tpu_before.l );
    FD_TEST( state->fee_cfg_version==fee_version_before );
    FD_TEST( state->bam_config_received==(kind==4UL ? 0U : 1U) );
    if( kind!=4UL ) FD_TEST( state->bam_stream==stream && state->bam_stream_live && !state->defer_reset );
    test_bam_env_destroy( env );
  }
}

/* A zero-byte DATA message is still one valid protobuf message.  It has
   partial-update semantics, unlike an RPC with no DATA message at all. */
static void
test_bam_config_rpc_accepts_empty_and_unknown_fields( fd_wksp_t * wksp ) {
  g_clock = (long)10e9;
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn_empty( env );
  test_bam_env_mock_h2_hs( state );
  state->bam_config_inflight = 1U;
  uchar empty = 0U;
  fd_bam_client_grpc_callbacks.rx_msg( state, &empty, 0UL, FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );
  FD_TEST( state->bam_config_pending_received && !state->bam_config_pending_invalid );
  FD_TEST( !state->bam_config_received && state->bam_config_inflight );
  fd_grpc_resp_hdrs_t ok = { .h2_status=200U, .grpc_status=FD_GRPC_STATUS_OK };
  fd_bam_client_grpc_callbacks.rx_end( state, FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig, &ok );
  FD_TEST( state->bam_config_received && !state->bam_config_inflight );

  uchar protobuf[256];
  ulong protobuf_sz = test_bam_encode_config_candidate( protobuf, sizeof(protobuf) );
  FD_TEST( sizeof(protobuf)-protobuf_sz>=3UL );
  protobuf[ protobuf_sz++ ] = 0xf8U; /* unknown field 127, varint */
  protobuf[ protobuf_sz++ ] = 0x07U;
  protobuf[ protobuf_sz++ ] = 0x01U;
  test_bam_apply_config( state, protobuf, protobuf_sz );
  FD_TEST( state->builder_commission==19U );
  test_bam_env_destroy( env );
}

/* Until the first config succeeds, the connection is not healthy and the
   node behind a load-balanced endpoint may be at fault.  A config timeout
   reconnects, like the other setup RPCs.  A failed config keeps the
   session, flushes feedback, and retries on the config poll throttle, like
   later failures (see *_keeps_scheduler_live). */
static void
test_bam_first_config_timeout_reconnects( fd_wksp_t * wksp ) {
  long const saved_clock = g_clock;
  for( ulong i=0UL; i<3UL; i++ ) {
    g_clock = (long)10e9;
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_env_create( env, wksp );
    test_bam_env_mock_conn_empty( env );
    test_bam_env_mock_h2_hs( state );
    test_bam_prepare_scheduler_stream( state );
    state->bam_builder_heartbeat_received = 1U; /* the node accepted the stream */
    test_bam_set_server( state, 50055U );
    test_bam_keepalive_sync( state, g_clock );
    state->bam_last_validator_heartbeat_ns = g_clock;
    state->bam_config_inflight     = 1U;
    state->bam_last_config_poll_ns = g_clock-(long)1e9;
    fd_bam_bundle_result_t result = test_make_bundle_result( 990U+(uint)i, 1UL, 1U );
    test_enqueue_bundle_result( state, &result );

    uchar protobuf[256];
    ulong protobuf_sz = test_bam_encode_config_candidate( protobuf, sizeof(protobuf) );
    fd_bam_client_grpc_callbacks.rx_msg( state, protobuf, protobuf_sz, FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );
    FD_TEST( state->bam_config_pending_received && !state->bam_config_received );

    if( i==0UL ) {
      fd_bam_client_grpc_callbacks.rx_timeout( state, FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig, FD_GRPC_DEADLINE_RX_END );
    } else {
      fd_grpc_resp_hdrs_t fail = { .h2_status = i==1UL ? 503U : 200U, .grpc_status = FD_GRPC_STATUS_UNAVAILABLE };
      fd_bam_client_grpc_callbacks.rx_end( state, FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig, &fail );
    }
    FD_TEST( !state->bam_config_inflight && !state->bam_config_pending_received && !state->bam_config_pending_invalid );
    FD_TEST( !state->bam_config_received && state->bam_last_config_poll_ns==g_clock );
    FD_TEST( state->defer_reset==(i==0UL) );
    FD_TEST( state->backoff_until<=g_clock ); /* feedback keeps flowing */

    int busy = 0;
    fd_bam_client_step( state, &busy );
    if( i==0UL ) {
      FD_TEST( busy && state->tcp_sock<0 && !state->defer_reset );
      FD_TEST( state->feedback_queue_depth==1U ); /* kept for the next stream */
    } else {
      FD_TEST( busy && state->tcp_sock>=0 && state->bam_stream_live );
      FD_TEST( !state->feedback_queue_depth && !state->bam_config_inflight );
      g_clock += (long)1e9;
      busy = 0;
      fd_bam_client_step( state, &busy );
      FD_TEST( busy && state->bam_config_inflight && state->tcp_sock>=0 );
    }
    test_bam_env_destroy( env );
  }
  g_clock = saved_clock;
}

static void
test_bam_scheduler_http_error_requests_transport_reset( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_live_env_create( env, wksp, (long)10e9 );
  fd_bam_bundle_result_t result = test_make_bundle_result( 991U, 42UL, 1U );
  test_enqueue_bundle_result( state, &result );
  state->bam_leader_pending = 1U;
  fd_grpc_resp_hdrs_t fail = { .h2_status=503U };
  fd_bam_client_grpc_callbacks.rx_end( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream, &fail );
  FD_TEST( state->defer_reset && !state->bam_stream_live && !state->bam_stream );
  FD_TEST( !state->bam_auth_ready && !state->bam_leader_pending );
  FD_TEST( state->feedback_queue_depth==1U );
  test_bam_env_destroy( env );
}

static fd_bam_tile_t *
test_bam_heartbeat_env_start( test_bam_env_t * env,
                              fd_wksp_t *      wksp ) {
  /* Helper to boot a connected client with deterministic keepalive timestamps
     for watchdog tests. */
  g_clock = (long)1e9;
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );
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
  fd_bam_client_grpc_callbacks.rx_start( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( state->bam_last_builder_activity_ns == g_clock );
  FD_TEST( state->bam_last_validator_heartbeat_ns == g_clock );
  return state;
}

static void
test_bam_auth_retry_deadline_tracks_backoff( fd_wksp_t * wksp ) {
  long const saved_clock = g_clock;
  g_clock = (long)10e9;
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn_empty( env );
  test_bam_env_mock_h2_hs( state );
  state->enabled = 1U;
  test_bam_set_server( state, 1234U );
  state->keepalive->interval   = (long)5e9;
  state->keepalive->inflight   = 0U;
  state->keepalive->ts_next_tx = g_clock+(long)5e9;
  state->bam_config_received = 1U;
  state->bam_config_inflight = 1U; /* keep the 1 s config poll out of these deadlines */
  state->bam_auth_inflight = 1U;

  /* Config may finish before AuthChallenge on the same H2 connection.  A
     failed auth response must wake the client when its backoff expires. */
  fd_grpc_resp_hdrs_t fail = { .h2_status = 503U };
  fd_bam_client_grpc_callbacks.rx_end( state, FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge, &fail );
  FD_TEST( !state->bam_auth_inflight && !state->bam_auth_ready && !state->bam_stream );
  FD_TEST( !state->defer_reset && state->backoff_until>g_clock );
  long const retry = state->backoff_until;
  FD_TEST( fd_bam_client_next_deadline( state, g_clock )==retry );

  state->keepalive->ts_next_tx = g_clock+1L;
  FD_TEST( fd_bam_client_next_deadline( state, g_clock )==g_clock+1L );
  state->backoff_until         = g_clock+(long)1e6;
  state->keepalive->ts_next_tx = g_clock; /* Overdue before the 1 ms retry. */
  FD_TEST( fd_bam_client_next_deadline( state, g_clock )==state->backoff_until );

  state->backoff_until         = retry;
  state->keepalive->ts_next_tx = g_clock+(long)5e9;
  state->next_step_deadline    = retry;
  g_clock = retry-1L;
  int busy = 0;
  fd_bam_test_before_credit( state, env->stem, &busy );
  FD_TEST( !state->bam_auth_inflight && !busy );
  g_clock = retry;
  fd_bam_test_before_credit( state, env->stem, &busy );
  FD_TEST( state->bam_auth_inflight && busy );
  long const next = fd_bam_client_next_deadline( state, g_clock );
  FD_TEST( next>g_clock+(long)20e6 );
  state->backoff_until = g_clock-1L;
  FD_TEST( fd_bam_client_next_deadline( state, g_clock )==next );

  test_bam_env_destroy( env );
  g_clock = saved_clock;
}

static void
test_bam_blocked_config_deadline_does_not_spin( fd_wksp_t * wksp ) {
  g_clock = (long)10e9;
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn_empty( env );
  test_bam_env_mock_h2_hs( state );
  test_bam_set_server( state, 1234U );
  state->keepalive->interval = 0L;
  state->bam_auth_inflight = 1U; /* isolate config polling */
  state->bam_config_received = 1U; /* without a builder */
  state->bam_config_inflight = 0U;
  state->bam_last_config_poll_ns = g_clock-(long)2e9;

  /* A parked HTTP/2 request leaves the config poll overdue, but the tile
     must wait for readiness or a bounded retry instead of spinning. */
  state->grpc_client->request_tx_op->chunk_sz = 1UL;
  FD_TEST( fd_grpc_client_request_is_blocked( state->grpc_client ) );
  int busy = 0;
  fd_bam_client_step( state, &busy );
  FD_TEST( !busy && !state->bam_config_inflight );
  FD_TEST( fd_bam_client_next_deadline( state, g_clock )==g_clock+(long)20e6 );

  state->bam_last_config_poll_ns = g_clock;
  FD_TEST( fd_bam_client_next_deadline( state, g_clock )==g_clock+(long)1e9 );

  state->bam_last_config_poll_ns = g_clock-(long)2e9;
  state->grpc_client->request_tx_op->chunk_sz = 0UL;
  FD_TEST( !fd_grpc_client_request_is_blocked( state->grpc_client ) );
  busy = 0;
  fd_bam_client_step( state, &busy );
  FD_TEST( busy && state->bam_config_inflight );
  test_bam_env_destroy( env );
}

static void
test_bam_scheduler_stream_replays_only_retained_leader_state( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn_empty( env );
  test_bam_env_mock_h2_hs( state );

  g_clock = (long)8e9;
  test_bam_keepalive_sync( state, g_clock );
  state->keepalive->ts_next_tx = LONG_MAX;
  state->bam_config_received      = 1U;
  state->bam_last_config_poll_ns  = g_clock;
  state->bam_last_validator_heartbeat_ns = g_clock;

  test_bam_prepare_scheduler_stream( state );
  state->bam_stream_live       = 0U;

  FD_TEST( state->bam_leader_state.slot == ULONG_MAX );
  fd_bam_client_grpc_callbacks.rx_start( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( state->bam_leader_pending == 0U );

  state->bam_leader_state = (fd_bam_leader_state_t){
    .slot = 41UL,
    .tick = 12U,
    .slot_cu_budget_remaining = 321U,
    .slot_end_ns = g_clock - FD_BAM_LEADER_STATE_EXPIRY_GRACE_NS - 1L,
    .current_slot_has_bam_work = 1U
  };
  state->bam_leader_pending = 1U;

  fd_bam_client_grpc_callbacks.rx_start( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( state->bam_leader_pending == 0U );
  FD_TEST( fd_bam_client_step_reconnect( state, g_clock ) == 0 );

  state->bam_leader_state = (fd_bam_leader_state_t){
    .slot = 42UL,
    .tick = 8U,
    .slot_cu_budget_remaining = 456U,
    .slot_end_ns = g_clock - FD_BAM_LEADER_STATE_EXPIRY_GRACE_NS + 1L,
    .current_slot_has_bam_work = 1U
  };
  state->bam_leader_pending = 0U;
  state->bam_stream_live       = 0U;

  fd_bam_client_grpc_callbacks.rx_start( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( state->bam_leader_pending == 1U );

  int busy = fd_bam_client_step_reconnect( state, g_clock );
  FD_TEST( busy == 1 );
  FD_TEST( state->bam_leader_pending == 0U );

  test_bam_expect_leader_state( state, 42UL, 8U, 456U );

  state->bam_leader_state = (fd_bam_leader_state_t){
    .slot = 43UL,
    .tick = 7U,
    .slot_cu_budget_remaining = 123U,
    .slot_end_ns = g_clock + (long)1e9,
    .current_slot_has_bam_work = 1U
  };
  state->bam_leader_pending = 0U;
  state->bam_stream_live       = 0U;

  fd_bam_client_grpc_callbacks.rx_start( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream );
  FD_TEST( state->bam_leader_pending == 1U );

  busy = fd_bam_client_step_reconnect( state, g_clock );
  FD_TEST( busy == 1 );
  FD_TEST( state->bam_leader_pending == 0U );

  test_bam_expect_leader_state( state, 43UL, 7U, 123U );

  test_bam_env_destroy( env );
}

/* next_deadline schedules the retry at backoff_until.  A step during
   backoff returns at once instead of sleeping, so bank_bam, pack_bam_res,
   and motor_bam keep draining.  A sleep blocks, so count voluntary context
   switches rather than elapsed time. */
static void
test_bam_backoff_step_returns_without_sleep( fd_wksp_t * wksp ) {
  long const saved_clock = g_clock;
  g_clock = (long)10e9;
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_set_server( state, 50055U );
  state->backoff_until   = g_clock+(long)1e9;

  struct rusage ru0[1];
  FD_TEST( !getrusage( RUSAGE_THREAD, ru0 ) );
  for( ulong i=0UL; i<16UL; i++ ) {
    int busy = 0;
    fd_bam_client_step( state, &busy );
    FD_TEST( !busy && state->tcp_sock<0 );
  }
  struct rusage ru1[1];
  FD_TEST( !getrusage( RUSAGE_THREAD, ru1 ) );
  FD_TEST( ru1->ru_nvcsw-ru0->ru_nvcsw<8L ); /* a 1 ms sleep per step blocked >= 16 times */
  FD_TEST( fd_bam_client_next_deadline( state, g_clock )==state->backoff_until );

  test_bam_env_destroy( env );
  g_clock = saved_clock;
}

/* The watchdog drops the connection once builder activity is
   FD_BAM_ACTIVITY_TIMEOUT_NS old (not 1 ms before) while the scheduler
   stream is live.  An unset (0) activity timestamp, a scheduler stream
   that is down, or BAM disabled at runtime never trips it. */
static void
test_bam_heartbeat_timeout_forces_disconnect( fd_wksp_t * wksp ) {
  struct { long dt; int tweak; int disconnect; } const cases[] = {
    { FD_BAM_ACTIVITY_TIMEOUT_NS+(long)1e8, 0, 1 },
    { FD_BAM_ACTIVITY_TIMEOUT_NS+(long)1e9, 1, 0 }, /* activity unset */
    { FD_BAM_ACTIVITY_TIMEOUT_NS+(long)1e9, 2, 0 }, /* stream down */
    { FD_BAM_ACTIVITY_TIMEOUT_NS-(long)1e6, 0, 0 },
    { FD_BAM_ACTIVITY_TIMEOUT_NS,           0, 1 },
    { FD_BAM_ACTIVITY_TIMEOUT_NS+(long)1e9, 3, 0 }, /* disabled */
  };
  for( ulong i=0UL; i<sizeof(cases)/sizeof(cases[0]); i++ ) {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
    if( cases[ i ].tweak==1 ) state->bam_last_builder_activity_ns = 0L;
    if( cases[ i ].tweak==2 ) state->bam_stream_live              = 0U;
    if( cases[ i ].tweak==3 ) state->enabled                      = 0;
    int charge_busy = 0;
    g_clock += cases[ i ].dt;
    fd_bam_client_step( state, &charge_busy );
    int const disconnect = cases[ i ].disconnect;
    FD_TEST( ( disconnect ? state->tcp_sock==-1 : state->tcp_sock>=0 ) && state->tcp_sock_connected==(uint)!disconnect );
    FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_BUILDER_ACTIVITY_TIMEOUT_IDX ]==(ulong)disconnect );
    if( disconnect ) FD_TEST( charge_busy==1 );
    test_bam_env_destroy( env );
  }
}

/* Only BuilderHeartBeat refreshes the watchdog deadline: a heartbeat sets
   the activity timestamp and records one arrival latency sample, and
   defers the disconnect, while a bundle batch does neither. */
static void
test_bam_heartbeat_reset_extends_timeout( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_heartbeat_env_start( env, wksp );
  test_bam_send_scheduler_heartbeat( state, 1UL );
  FD_TEST( state->bam_last_builder_activity_ns == g_clock );
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
  FD_TEST( state->metrics.builder_heartbeats_decoded_cnt == 1UL );
  FD_TEST( test_hist_total_cnt( state->metrics.builder_heartbeat_arrival_delta_nanos ) == 1UL );
  test_bam_env_destroy( env );

  state = test_bam_heartbeat_env_start( env, wksp );
  int charge_busy = 0;
  g_clock += FD_BAM_ACTIVITY_TIMEOUT_NS - (long)2e8;
  test_bam_send_scheduler_heartbeat( state, 1UL );
  g_clock += FD_BAM_ACTIVITY_TIMEOUT_NS - (long)1e8;
  fd_bam_client_step( state, &charge_busy );
  FD_TEST( state->tcp_sock >= 0 );
  test_bam_env_destroy( env );

  state = test_bam_heartbeat_env_start( env, wksp );
  long expected_ts = state->bam_last_builder_activity_ns;
  g_clock += (long)1e8;
  test_bam_send_scheduler_bundle( state, 0U, 0 );
  FD_TEST( state->bam_last_builder_activity_ns == expected_ts );
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY );
  test_bam_env_destroy( env );
}

/* Each failure arms one random backoff, and the next dial waits it out.
   The dial is not a failure: the new connection's first requests go out
   once HTTP/2 is up, not after a second draw.  A failed request, or a
   failed dial, arms the backoff before the next attempt again. */
static void
test_bam_reconnect_backoff_once_per_failure( fd_wksp_t * wksp ) {
  long const saved_clock = g_clock;
  g_clock = (long)10e9;
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  struct sockaddr_in addr;
  int listen_sock = test_bam_loopback_listener( &addr );
  test_bam_set_server( state, fd_ushort_bswap( addr.sin_port ) );

  /* A failure closes the connection; the dial waits for its backoff. */
  state->bam_last_config_poll_ns = g_clock; /* the next connection still polls at once */
  fd_bam_client_reset( state );
  long const dial = state->backoff_until;
  FD_TEST( dial>g_clock );
  int busy = 0;
  g_clock = dial-1L;
  fd_bam_client_step( state, &busy );
  FD_TEST( state->tcp_sock<0 );
  g_clock = dial;
  fd_bam_client_step( state, &busy );
  FD_TEST( state->tcp_sock>=0 );
  FD_TEST( !fd_bam_tile_should_stall( state, g_clock ) );

  env->server_sock = accept4( listen_sock, NULL, NULL, SOCK_CLOEXEC );
  FD_TEST( env->server_sock>=0 );
  fd_bam_client_step( state, &busy );
  FD_TEST( state->tcp_sock_connected );
  test_bam_env_mock_h2_hs( state );
  for( ulong i=0UL; i<2UL; i++ ) fd_bam_client_step( state, &busy ); /* GetAuthChallenge, then GetBuilderConfig */
  FD_TEST( g_clock==dial && state->bam_auth_inflight && state->bam_config_inflight );

  /* A failed request on the open connection retries after its backoff. */
  fd_grpc_resp_hdrs_t unavailable = { .h2_status = 503U };
  fd_bam_client_grpc_callbacks.rx_end( state, FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge, &unavailable );
  FD_TEST( !state->bam_auth_inflight && state->backoff_until>g_clock );
  g_clock = state->backoff_until-1L;
  fd_bam_client_step( state, &busy );
  FD_TEST( !state->bam_auth_inflight );
  g_clock++;
  fd_bam_client_step( state, &busy );
  FD_TEST( state->bam_auth_inflight );

  /* A rejected stream closes the connection: one backoff before the next dial. */
  fd_grpc_resp_hdrs_t reject = { .h2_status = 200U, .grpc_status = FD_GRPC_STATUS_PERMISSION_DENIED };
  fd_bam_client_grpc_callbacks.rx_end( state, FD_BAM_CLIENT_REQ_BAM_InitSchedulerStream, &reject );
  fd_bam_client_step( state, &busy );
  FD_TEST( state->tcp_sock<0 && state->backoff_until>g_clock );
  long const redial = state->backoff_until;
  g_clock = redial-1L;
  fd_bam_client_step( state, &busy );
  FD_TEST( state->tcp_sock<0 );

  /* A failed dial arms its own backoff. */
  fd_cstr_ncpy( state->server_fqdn, "bam..", sizeof(state->server_fqdn) );
  state->server_fqdn_len = sizeof("bam..")-1UL;
  g_clock = redial;
  fd_bam_client_step( state, &busy );
  FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_RESOLVE_IDX ]==1UL );
  FD_TEST( state->tcp_sock<0 && state->backoff_until>g_clock );

  test_bam_set_server( state, state->server_tcp_port );
  g_clock = state->backoff_until;
  fd_bam_client_step( state, &busy );
  FD_TEST( state->tcp_sock>=0 );
  FD_TEST( !fd_bam_tile_should_stall( state, g_clock ) );

  FD_TEST( 0==close( listen_sock ) );
  test_bam_env_destroy( env );
  g_clock = saved_clock;
}

static void
test_bam_client_status( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn_empty( env );
  test_bam_env_mock_h2_hs( state );
  long now = fd_bam_now();
  state->bam_last_builder_activity_ns    = now;
  state->bam_last_validator_heartbeat_ns = now;
  state->bam_stream_live                 = 1U;

  /* Connected transport without a ConfigResponse is still unhealthy. */
  FD_TEST( state->bam_config_received == 0U );
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY );

  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  resp.has_block_engine_config = true;
  resp.block_engine_config.builder_commission = 7U;
  test_bam_apply_config_resp( state, &resp );
  FD_TEST( state->bam_config_received == 1U );
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY );

  /* As in jito-solana, a session without a valid builder stays
     unhealthy even with a live scheduler stream. */
  test_bam_send_scheduler_heartbeat( state, 1UL );
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY );

  test_bam_env_inject_config_response( state );
  FD_TEST( state->bam_config_received == 1U );
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );

  fd_bam_tile_t state_backup = *state;
  fd_grpc_client_t client_backup = *state->grpc_client;

  /* A stale BuilderHeartBeat should not report healthy. */
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

  state->tcp_sock_connected = 0U;
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED );
  *state = state_backup;

  struct { ushort flag; uint status; } const conn_flags[] = {
    { FD_H2_CONN_FLAGS_DEAD,                FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED },
    { FD_H2_CONN_FLAGS_SEND_GOAWAY,         FD_PLUGIN_MSG_BAM_UPDATE_STATUS_DISCONNECTED },
  };
  for( ulong i=0UL; i<sizeof(conn_flags)/sizeof(conn_flags[0]); i++ ) {
    FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
    state->grpc_client->conn->flags |= conn_flags[ i ].flag;
    FD_TEST( fd_bam_client_status( state ) == conn_flags[ i ].status );
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

/* Signs FD_BAM_AUTH_LABEL||challenge with the key derived from seed, as
   the sign tile would, and returns that key's public half. */
static void
test_bam_sign_auth_challenge( uchar        signature[ 64 ],
                              uchar        public_key[ 32 ],
                              uchar        seed,
                              char const * challenge ) {
  uchar private_key[ 32 ];
  for( ulong i=0UL; i<32UL; i++ ) private_key[ i ] = (uchar)( seed + i );
  fd_sha512_t _sha[1];
  fd_sha512_t * sha = fd_sha512_join( fd_sha512_new( _sha ) );
  FD_TEST( sha );
  fd_ed25519_public_from_private( public_key, private_key, sha );

  uchar payload[ FD_BAM_AUTH_LABEL_LEN + sizeof(bam_api_AuthChallengeResponse) ];
  ulong const challenge_len = strlen( challenge );
  fd_memcpy( payload, FD_BAM_AUTH_LABEL, FD_BAM_AUTH_LABEL_LEN );
  fd_memcpy( payload + FD_BAM_AUTH_LABEL_LEN, challenge, challenge_len );
  fd_ed25519_sign( signature, payload, FD_BAM_AUTH_LABEL_LEN + challenge_len, public_key, private_key, sha );
}

static void
test_bam_publish_keyguard_signature( fd_keyguard_client_t * client,
                                     fd_frag_meta_t *       response_mcache,
                                     uchar const            signature[ 64 ] ) {
  fd_memcpy( fd_chunk_to_laddr( client->response_mem, client->response_chunk0 ), signature, 64UL );
  fd_mcache_publish( response_mcache, client->response_depth, client->response_seq, 0UL, client->response_chunk0, 64UL, 0UL, 0UL, 0UL );
}

/* Delivers an AuthChallengeResponse to an inflight GetAuthChallenge. */
static void
test_bam_rx_auth_challenge( fd_bam_tile_t * state,
                            char const *    challenge ) {
  bam_api_AuthChallengeResponse resp = bam_api_AuthChallengeResponse_init_default;
  fd_cstr_ncpy( resp.challenge_to_sign, challenge, sizeof( resp.challenge_to_sign ) );
  uchar pb_buf[ 128 ];
  pb_ostream_t ostream = pb_ostream_from_buffer( pb_buf, sizeof(pb_buf) );
  FD_TEST( pb_encode( &ostream, bam_api_AuthChallengeResponse_fields, &resp ) );
  state->bam_auth_inflight = 1U;
  fd_bam_client_grpc_callbacks.rx_msg( state, pb_buf, ostream.bytes_written, FD_BAM_CLIENT_REQ_BAM_GetAuthChallenge );
}

/* An eight-byte challenge creates an ambiguous keyguard payload, while an
   identity switch can pair a new signer response with the cached public key.
   Reject the short challenge and verify responses against the cached identity. */
static void
test_bam_auth_challenge_response_verifies_cached_identity( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  /* Keyguard request and response links */
  ulong const      depth    = 8UL;
  ulong const      mtu[ 2 ] = { 256UL, 64UL };
  fd_frag_meta_t * mcache[ 2 ];
  uchar *          dcache[ 2 ];
  for( ulong i=0UL; i<2UL; i++ ) {
    mcache[ i ] = fd_mcache_join( fd_mcache_new( fd_wksp_alloc_laddr( wksp, fd_mcache_align(), fd_mcache_footprint( depth, 0UL ), 1UL ),
                                                 depth, 0UL, 0UL ) );
    ulong const data_sz = fd_dcache_req_data_sz( mtu[ i ], depth, 1UL, 1 );
    dcache[ i ] = fd_dcache_join( fd_dcache_new( fd_wksp_alloc_laddr( wksp, fd_dcache_align(), fd_dcache_footprint( data_sz, 0UL ), 1UL ),
                                                 data_sz, 0UL ) );
    FD_TEST( mcache[ i ] && dcache[ i ] );
  }
  FD_TEST( fd_keyguard_client_new( state->keyguard_client, mcache[ 0 ], dcache[ 0 ], mcache[ 1 ], dcache[ 1 ],
                                   mtu[ 0 ], mtu[ 1 ], NULL, ULONG_MAX, ULONG_MAX ) );

  test_bam_rx_auth_challenge( state, "12345678" );
  FD_TEST( state->bam_auth_inflight == 0U );
  FD_TEST( state->bam_auth_ready == 0U );
  FD_TEST( state->challenge_to_sign[ 0 ] == '\0' );
  FD_TEST( state->keyguard_client->request_seq == 0UL );

  char const challenge[] = "unit-test-challenge";
  uchar signature[ 64 ];
  test_bam_sign_auth_challenge( signature, state->bam_identity_pubkey, 1U, challenge );
  test_bam_publish_keyguard_signature( state->keyguard_client, mcache[ 1 ], signature );
  test_bam_rx_auth_challenge( state, challenge );
  FD_TEST( state->bam_auth_inflight == 0U );
  FD_TEST( state->bam_auth_ready == 1U );
  FD_TEST( 0 == strcmp( state->challenge_to_sign, challenge ) );
  char expected_sig[ FD_BASE58_ENCODED_64_SZ ];
  fd_base58_encode_64( signature, NULL, expected_sig );
  FD_TEST( 0 == strcmp( state->bam_auth_signature, expected_sig ) );
  FD_TEST( state->keyguard_client->request_seq == 1UL );

  /* A signature by another identity */
  char const mismatch_challenge[] = "unit-test-challenge-after-switch";
  uchar other_public_key[ 32 ];
  test_bam_sign_auth_challenge( signature, other_public_key, 97U, mismatch_challenge );
  test_bam_publish_keyguard_signature( state->keyguard_client, mcache[ 1 ], signature );
  test_bam_rx_auth_challenge( state, mismatch_challenge );
  FD_TEST( state->bam_auth_inflight == 0U );
  FD_TEST( state->bam_auth_ready == 0U );
  FD_TEST( state->challenge_to_sign[ 0 ] == '\0' );
  FD_TEST( state->keyguard_client->request_seq == 2UL );

  for( ulong i=0UL; i<2UL; i++ ) {
    fd_wksp_free_laddr( fd_dcache_delete( fd_dcache_leave( dcache[ i ] ) ) );
    fd_wksp_free_laddr( fd_mcache_delete( fd_mcache_leave( mcache[ i ] ) ) );
  }
  test_bam_env_destroy( env );
}

/* With an auth signature ready, the step opens the scheduler stream and
   sends AuthProof, also before any ConfigResponse supplied a builder. */
static void
test_bam_scheduler_auth_proof_publishes_message( fd_wksp_t * wksp ) {
  for( int with_builder=1; with_builder>=0; with_builder-- ) {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_env_create( env, wksp );
    if( with_builder ) {
      test_bam_env_mock_conn( env );
    } else {
      test_bam_env_mock_conn_empty( env );
      test_bam_env_mock_h2_hs( state );
    }

    state->bam_stream            = NULL;
    state->bam_stream_live       = 0U;
    state->bam_config_inflight   = 1U;
    state->bam_auth_ready        = 1U;
    state->bam_auth_inflight     = 0U;
    state->grpc_client->request_stream = NULL;
    *state->grpc_client->request_tx_op = (fd_h2_tx_op_t){0};

    g_clock = (long)5e9;
    test_bam_keepalive_sync( state, g_clock );
    state->bam_last_config_poll_ns = g_clock;

    char const challenge[]     = "challenge-123";
    char const signature[]     = "sig-abcdef";
    char const validator_key[] = "validator-key-test";
    fd_cstr_ncpy( state->challenge_to_sign,       challenge,     sizeof( state->challenge_to_sign       ) );
    fd_cstr_ncpy( state->bam_auth_signature,      signature,     sizeof( state->bam_auth_signature      ) );
    fd_cstr_ncpy( state->bam_identity_pubkey_b58, validator_key, sizeof( state->bam_identity_pubkey_b58 ) );

    fd_bam_client_step_reconnect( state, g_clock );

    FD_TEST( state->bam_stream != NULL );
    FD_TEST( state->bam_auth_ready == 0U );
    FD_TEST( state->challenge_to_sign[ 0 ] == '\0' );
    FD_TEST( state->bam_stream_live == 0U );

    test_bam_decoded_message_t decoded;
    test_bam_expect_msg( state, &decoded, bam_api_SchedulerMessageV0_auth_proof_tag );
    FD_TEST( 0 == strcmp( decoded.msg.versioned_msg.v0.msg.auth_proof.challenge_to_sign, challenge ) );
    FD_TEST( 0 == strcmp( decoded.msg.versioned_msg.v0.msg.auth_proof.signature, signature ) );
    FD_TEST( 0 == strcmp( decoded.msg.versioned_msg.v0.msg.auth_proof.validator_pubkey, validator_key ) );

    test_bam_env_destroy( env );
  }
}

/* A session without a config polls at once, even with its scheduler stream
   up, then every second until it has a builder. */
static void
test_bam_missing_config_repolls_despite_valid_builder_info( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn_empty( env );
  test_bam_env_mock_h2_hs( state );
  test_bam_prepare_scheduler_stream( state );

  g_clock = (long)5e9;
  long now = g_clock;
  test_bam_keepalive_sync( state, now );

  state->bam_config_received      = 0U;
  state->bam_config_inflight      = 0U;
  state->bam_last_config_poll_ns  = 0L;

  int busy = fd_bam_client_step_reconnect( state, now );

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
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn_empty( env );
  test_bam_env_mock_h2_hs( state );

  g_clock = (long)5e9;
  long now = g_clock;
  test_bam_keepalive_sync( state, now );
  state->keepalive->ts_next_tx = now + (long)10e9;

  state->bam_config_received      = 0U;
  state->bam_config_inflight      = 0U;
  state->bam_last_config_poll_ns  = now;
  state->bam_auth_ready           = 0U;
  state->bam_auth_inflight        = 1U;

  int busy = fd_bam_client_step_reconnect( state, now + (long)1e9 - 1L );
  FD_TEST( busy == 0 );
  FD_TEST( state->bam_config_inflight == 0U );

  busy = fd_bam_client_step_reconnect( state, now + (long)1e9 );

  FD_TEST( busy == 1 );
  FD_TEST( state->bam_config_inflight == 1U );
  FD_TEST( state->bam_last_config_poll_ns == now + (long)1e9 );
  FD_TEST( state->grpc_client->stream_cnt >= 1UL );
  fd_grpc_h2_stream_t * stream = &state->grpc_client->stream_pool[ state->grpc_client->stream_cnt-1UL ];
  FD_TEST( stream->request_ctx == FD_BAM_CLIENT_REQ_BAM_GetBuilderConfig );

  test_bam_env_destroy( env );
}

static ulong
test_bam_encode_builder_config( uchar *       buf,
                                ulong         buf_sz,
                                uchar const * builder,
                                uint          commission ) {
  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  test_bam_config_set_builder( &resp, builder, commission );
  test_bam_config_set_tpu( &resp, "127.0.0.1", 9007U, "127.0.0.2", 9008U );
  resp.bam_config.shred_socks_count = 1U;
  fd_cstr_ncpy( resp.bam_config.shred_socks[ 0 ].ip, "127.0.0.3", sizeof( resp.bam_config.shred_socks[ 0 ].ip ) );
  resp.bam_config.shred_socks[ 0 ].port = 9009U;
  return test_bam_encode_config( &resp, buf, buf_sz );
}

/* Once the session has a builder, the config is polled every 60 s (jito-solana
   bam_connection.rs REFRESH_CONFIG_INTERVAL: every second); an unchanged
   builder and commission publish no new fee configuration version, while a
   change publishes one.  An unchanged response is still applied: it restores
   contact state that a BAM deactivation cleared within the session. */
static void
test_bam_config_polls_every_minute_with_builder( fd_wksp_t * wksp ) {
  long const saved_clock = g_clock;
  g_clock = (long)5e9;
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn_empty( env );
  test_bam_env_mock_h2_hs( state );
  test_bam_keepalive_sync( state, g_clock );
  state->keepalive->ts_next_tx = LONG_MAX;
  state->enabled = 1U;
  test_bam_set_server( state, 1234U );
  state->bam_auth_inflight = 1U; /* isolate config polling */
  fd_bam_fee_cfg_t * shared_cfg = state->fee_cfg;

  uchar builder[ 32 ] = {1U, 2U, 3U};
  uchar pb_buf[ bam_api_ConfigResponse_size ];
  ulong pb_sz = test_bam_encode_builder_config( pb_buf, sizeof(pb_buf), builder, 7U );
  test_bam_apply_config( state, pb_buf, pb_sz );
  FD_TEST( state->bam_config_received && shared_cfg->version==1U );

  long poll0 = state->bam_last_config_poll_ns;
  FD_TEST( poll0==g_clock && !state->bam_config_inflight );
  FD_TEST( fd_bam_client_next_deadline( state, g_clock )<=poll0+(long)60e9 );
  FD_TEST( !fd_bam_client_step_reconnect( state, poll0+(long)60e9-1L ) && !state->bam_config_inflight );
  FD_TEST( fd_bam_client_step_reconnect( state, poll0+(long)60e9 ) && state->bam_config_inflight );
  FD_TEST( state->bam_last_config_poll_ns==poll0+(long)60e9 );

  /* The same response: no new fee configuration version. */
  g_clock = poll0+(long)60e9;
  test_bam_apply_config( state, pb_buf, pb_sz );
  FD_TEST( shared_cfg->version==1U && !state->bam_config_inflight );

  /* Contact cleared by a deactivation is restored by the next response. */
  state->bam_tpu            = (fd_ip4_port_t){0};
  state->bam_tpu_fwd        = (fd_ip4_port_t){0};
  state->bam_shred_sock_cnt = 0U;
  test_bam_apply_config( state, pb_buf, pb_sz );
  FD_TEST( state->bam_tpu.l && state->bam_tpu_fwd.l && state->bam_shred_sock_cnt==1U );
  FD_TEST( shared_cfg->version==1U );

  /* A commission or builder change publishes a new version. */
  pb_sz = test_bam_encode_builder_config( pb_buf, sizeof(pb_buf), builder, 8U );
  test_bam_apply_config( state, pb_buf, pb_sz );
  FD_TEST( shared_cfg->version==2U && shared_cfg->builder_commission==8U );
  builder[ 0 ] = 9U;
  pb_sz = test_bam_encode_builder_config( pb_buf, sizeof(pb_buf), builder, 8U );
  test_bam_apply_config( state, pb_buf, pb_sz );
  FD_TEST( shared_cfg->version==3U && !memcmp( shared_cfg->builder_pubkey, builder, 32UL ) );

  test_bam_env_destroy( env );
  g_clock = saved_clock;
}

/* A due validator heartbeat, then pending leader state, go out on the
   scheduler stream. */
static void
test_bam_scheduler_heartbeat_publishes_message( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_live_env_create( env, wksp, (long)7e9 );
  long now = g_clock;
  state->bam_last_validator_heartbeat_ns = 0L;
  state->bam_last_config_poll_ns = now;
  FD_TEST( fd_bam_client_step_reconnect( state, now )==1 );
  FD_TEST( state->bam_last_validator_heartbeat_ns == now );

  test_bam_decoded_message_t decoded;
  test_bam_expect_msg( state, &decoded, bam_api_SchedulerMessageV0_heart_beat_tag );
  FD_TEST( decoded.msg.versioned_msg.v0.msg.heart_beat.time_sent_microseconds == (uint64_t)(now/1000L) );
  FD_TEST( state->metrics.outbound_enqueue_outcome_cnt[ FD_METRICS_ENUM_BAM_ENQUEUE_RESULT_V_HEARTBEAT_ENQUEUED_IDX ] == 1UL );

  state->bam_leader_pending = 1U;
  state->bam_leader_state = (fd_bam_leader_state_t){ .slot = 42UL, .tick = 7U, .slot_cu_budget_remaining = 123U };
  FD_TEST( fd_bam_client_step_reconnect( state, now )==1 );
  FD_TEST( state->bam_leader_pending == 0U );
  test_bam_expect_leader_state( state, 42UL, 7U, 123U );

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
  test_bam_rx_sched( state, protobuf, ostream.bytes_written );
}

/* BAM proto Ping is a scheduler-stream latency probe.  Firedancer must
   answer with Pong, but this must not touch heartbeat/watchdog or HTTP/2
   keepalive accounting.  Cases: 0 request_stream left attached by a
   completed streaming send with no pending body, which must not block the
   Pong; 1 a busy HTTP/2 frame buffer; 2 a pending request body; 3 a closed
   stream; 4 a stale builder, which the Pong does not refresh; 5 no
   scheduler stream, so no Pong. */
static void
test_bam_scheduler_ping_publishes_message( fd_wksp_t * wksp ) {
  for( ulong i=0UL; i<6UL; i++ ) {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_env_create( env, wksp );
    test_bam_env_mock_conn( env );
    if( i<5UL ) test_bam_prepare_scheduler_stream( state );
    g_clock = (long)9e9 + (long)i*(long)5e8;
    long const builder_ts = g_clock - ( i==4UL ? FD_BAM_ACTIVITY_TIMEOUT_NS+(long)1e8 : (long)1e8 );
    state->bam_last_builder_activity_ns = builder_ts;
    ulong outcome = FD_METRICS_ENUM_BAM_SCHEDULER_PONG_SEND_RESULT_V_ENQUEUED_IDX;
    switch( i ) {
    case 0UL:
    case 2UL:
      state->grpc_client->request_stream          = state->bam_stream;
      state->grpc_client->request_tx_op->chunk_sz = i ? 1UL : 0UL;
      if( i ) outcome = FD_METRICS_ENUM_BAM_SCHEDULER_PONG_SEND_RESULT_V_REQUEST_BUSY_IDX;
      break;
    case 1UL:
      FD_TEST( fd_h2_tx_ping( fd_grpc_client_h2_conn( state->grpc_client ), fd_grpc_client_rbuf_tx( state->grpc_client ) ) );
      outcome = FD_METRICS_ENUM_BAM_SCHEDULER_PONG_SEND_RESULT_V_FRAME_TX_BUSY_IDX;
      break;
    case 3UL:
      state->bam_stream->s.state = FD_H2_STREAM_STATE_CLOSED;
      outcome = FD_METRICS_ENUM_BAM_SCHEDULER_PONG_SEND_RESULT_V_SEND_FAILED_IDX;
      break;
    case 4UL:
      test_bam_keepalive_sync( state, g_clock );
      FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY );
      break;
    }

    uint const ping_id = 0x00c0ffeeU+(uint)i;
    test_bam_deliver_scheduler_ping( state, ping_id );

    FD_TEST( !state->defer_reset );
    FD_TEST( !state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_SCHEDULER_ENVELOPE_DECODE_IDX ] );
    FD_TEST( !state->metrics.builder_heartbeats_decoded_cnt && !state->metrics.keepalive_acks_cnt );
    FD_TEST( state->bam_last_builder_activity_ns == builder_ts );
    FD_TEST( test_hist_total_cnt( state->metrics.builder_heartbeat_arrival_delta_nanos ) == 0UL );
    FD_TEST( test_hist_total_cnt( state->metrics.scheduler_pong_send_nanos ) == (ulong)( i<5UL ) );
    for( ulong j=0UL; j<FD_METRICS_ENUM_BAM_SCHEDULER_PONG_SEND_RESULT_CNT; j++ ) {
      FD_TEST( state->metrics.scheduler_pong_send_outcome_cnt[ j ] == (ulong)( i<5UL && j==outcome ) );
    }
    if( i==0UL ) {
      FD_TEST( fd_histf_sum( state->metrics.scheduler_pong_send_nanos ) == 0UL );
      test_bam_decoded_message_t decoded;
      test_bam_expect_msg( state, &decoded, bam_api_SchedulerMessageV0_pong_tag );
      FD_TEST( decoded.msg.versioned_msg.v0.msg.pong.id == ping_id );
    }
    if( i==4UL ) {
      FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_UNHEALTHY );
      int charge_busy = 0;
      fd_bam_client_step( state, &charge_busy );
      FD_TEST( state->tcp_sock == -1 );
      FD_TEST( charge_busy == 1 );
    }
    test_bam_env_destroy( env );
  }
}

/* A send that finds the gRPC client still writing an earlier message
   keeps its work queued without counting a failure, and later steps send
   it.  A send that fails on a ready client still counts as a failure. */

static void
test_bam_outbound_send_blocked_is_not_failure( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_live_env_create( env, wksp, (long)7e9 );
  ulong const * cnt = state->metrics.outbound_enqueue_outcome_cnt;
  long now = g_clock;
  state->bam_last_validator_heartbeat_ns = 0L;
  state->bam_leader_pending = 1U;
  state->bam_leader_state   = (fd_bam_leader_state_t){ .slot = 42UL, .tick = 7U, .slot_cu_budget_remaining = 123U };
  fd_bam_bundle_result_t res = test_make_bundle_result( 300, 1300, 2 );
  test_enqueue_bundle_result( state, &res );

  /* A due config poll writes its request first, so the heartbeat, leader
     state and result in the same step all find the client blocked. */
  state->bam_last_config_poll_ns  = 0L;
  FD_TEST( fd_bam_client_step_reconnect( state, now )==1 );
  FD_TEST( state->bam_config_inflight );
  for( ulong i=0UL; i<FD_METRICS_ENUM_BAM_ENQUEUE_RESULT_CNT; i++ ) FD_TEST( !cnt[ i ] );
  FD_TEST( state->bam_last_validator_heartbeat_ns==0L );
  FD_TEST( state->bam_leader_pending );
  FD_TEST( state->feedback_queue_depth==1U );

  /* Each later step sends the next message once the previous one is out. */
  test_bam_decoded_message_t decoded;
  test_bam_decode_last_message( state, &decoded ); /* drains the config request */
  pb_size_t const order[3] = { bam_api_SchedulerMessageV0_heart_beat_tag,
                               bam_api_SchedulerMessageV0_leader_state_tag,
                               bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag };
  for( ulong i=0UL; i<3UL; i++ ) {
    FD_TEST( fd_bam_client_step_reconnect( state, now )==1 );
    test_bam_expect_msg( state, &decoded, order[ i ] );
  }
  FD_TEST( !state->bam_leader_pending );
  FD_TEST( !state->feedback_queue_depth );
  FD_TEST( cnt[ FD_METRICS_ENUM_BAM_ENQUEUE_RESULT_V_HEARTBEAT_ENQUEUED_IDX    ]==1UL );
  FD_TEST( cnt[ FD_METRICS_ENUM_BAM_ENQUEUE_RESULT_V_LEADER_STATE_ENQUEUED_IDX ]==1UL );
  FD_TEST( cnt[ FD_METRICS_ENUM_BAM_ENQUEUE_RESULT_V_RESULT_ENQUEUED_IDX       ]==1UL );
  FD_TEST( !cnt[ FD_METRICS_ENUM_BAM_ENQUEUE_RESULT_V_HEARTBEAT_ENQUEUE_FAILED_IDX    ] );
  FD_TEST( !cnt[ FD_METRICS_ENUM_BAM_ENQUEUE_RESULT_V_LEADER_STATE_ENQUEUE_FAILED_IDX ] );

  /* A result body still waiting for flow-control credit also blocks. */
  test_enqueue_bundle_result( state, &res );
  state->grpc_client->request_stream          = state->bam_stream;
  state->grpc_client->request_tx_op->chunk_sz = 1UL;
  FD_TEST( !fd_bam_flush_results( state ) );
  FD_TEST( state->feedback_queue_depth==1U );
  FD_TEST( !cnt[ FD_METRICS_ENUM_BAM_ENQUEUE_RESULT_V_RESULT_ENQUEUE_FAILED_IDX ] );

  /* A ready client that rejects the send is a real failure. */
  state->grpc_client->request_stream          = NULL;
  state->grpc_client->request_tx_op->chunk_sz = 0UL;
  state->bam_stream->s.state = FD_H2_STREAM_STATE_CLOSED;
  FD_TEST( !fd_bam_flush_results( state ) );
  FD_TEST( state->feedback_queue_depth==1U );
  FD_TEST( cnt[ FD_METRICS_ENUM_BAM_ENQUEUE_RESULT_V_RESULT_ENQUEUE_FAILED_IDX ]==1UL );

  /* A stream that is not live keeps leader state and results queued
     without counting an outcome. */
  ulong cnt_before[ FD_METRICS_ENUM_BAM_ENQUEUE_RESULT_CNT ];
  fd_memcpy( cnt_before, cnt, sizeof(cnt_before) );
  state->bam_stream_live    = 0U;
  state->bam_leader_pending = 1U;
  FD_TEST( !fd_bam_send_leader_state( state, &state->bam_leader_state ) );
  FD_TEST( !fd_bam_flush_results( state ) );
  FD_TEST( state->bam_leader_pending );
  FD_TEST( state->feedback_queue_depth==1U );
  FD_TEST( !memcmp( cnt_before, cnt, sizeof(cnt_before) ) );

  test_bam_env_destroy( env );
}

static void
test_bam_leader_state_supersede_counts_drop( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  state->bam_leader_pending = 1U;
  state->bam_leader_state = (fd_bam_leader_state_t){
    .slot = 42UL,
    .tick = 7U,
    .slot_cu_budget_remaining = 123U
  };

  fd_bam_leader_state_t newer_state = {
    .slot = 42UL,
    .tick = 8U,
    .slot_cu_budget_remaining = 111U,
    .slot_end_ns = 9000L
  };

  fd_bam_stage_leader_state( state, &newer_state );

  FD_TEST( state->bam_leader_pending == 1U );
  FD_TEST( state->bam_leader_state.slot == newer_state.slot );
  FD_TEST( state->bam_leader_state.tick == newer_state.tick );
  FD_TEST( state->bam_leader_state.slot_cu_budget_remaining == newer_state.slot_cu_budget_remaining );
  ulong tracker_idx = newer_state.slot & (FD_BAM_LEADER_SLOT_END_TRACKER_CNT-1UL);
  FD_TEST( state->leader_slot_end[ tracker_idx ].valid );
  FD_TEST( state->leader_slot_end[ tracker_idx ].slot==newer_state.slot );

  fd_bam_stage_leader_state( state, &newer_state );

  test_bam_env_destroy( env );
}

/* The replay frag the tests deliver on the tile's replay in. */
static union {
  fd_replay_message_t msg;
  uchar               bytes[ sizeof(fd_replay_message_t) ];
} test_bam_replay_frag;

static void
test_bam_replay_in_setup( fd_bam_tile_t * state ) {
  fd_memset( &test_bam_replay_frag, 0, sizeof(test_bam_replay_frag) );
  state->replay_in_idx = 2UL;
  state->replay_in     = (fd_bam_in_ctx_t){ .mem = (fd_wksp_t *)test_bam_replay_frag.bytes };
}

static void
test_bam_replay_reset( fd_bam_tile_t * state,
                       ulong           completed_slot,
                       ulong           next_leader_slot ) {
  test_bam_replay_frag.msg.reset = (fd_poh_reset_t){ .completed_slot = completed_slot, .next_leader_slot = next_leader_slot };
  fd_bam_test_receive_ingress_frag( state, state->replay_in_idx, REPLAY_SIG_RESET, 0UL, sizeof(fd_poh_reset_t) );
}

static void
test_bam_replay_next_leader( fd_bam_tile_t * state,
                             ulong           slot,
                             ulong           next_leader_slot ) {
  test_bam_replay_frag.msg.next_leader = (fd_replay_next_leader_t){ .slot = slot, .next_leader_slot = next_leader_slot };
  fd_bam_test_receive_ingress_frag( state, state->replay_in_idx, REPLAY_SIG_NEXT_LEADER, 0UL, sizeof(fd_replay_next_leader_t) );
}

static void
test_bam_replay_slot_completed( fd_bam_tile_t * state,
                                ulong           slot,
                                ulong           slot_in_epoch,
                                ulong           slots_per_epoch ) {
  test_bam_replay_frag.msg.slot_completed =
      (fd_replay_slot_completed_t){ .slot = slot, .slot_in_epoch = slot_in_epoch, .slots_per_epoch = slots_per_epoch };
  fd_bam_test_receive_ingress_frag( state, state->replay_in_idx, REPLAY_SIG_SLOT_COMPLETED, 0UL, sizeof(fd_replay_slot_completed_t) );
}

static void
test_bam_replay_input_contract( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  test_bam_replay_in_setup( state );
  state->leader_schedule_gate_start_ns = 1234L;
  state->next_step_deadline = 1234L;

  test_bam_replay_reset( state, 0UL, 600UL );
  FD_TEST( state->next_leader_slot == 600UL );
  FD_TEST( state->leader_schedule_recheck_slot == FD_BAM_LEADER_SCHEDULE_RECHECK_NONE_SLOT );
  FD_TEST( state->leader_schedule_gate_start_ns == 0L );
  FD_TEST( state->next_step_deadline == 0L );

  test_bam_replay_frag.msg.reset.next_leader_slot = 700UL;
  state->next_step_deadline = 1234L;
  fd_bam_test_receive_ingress_frag( state, state->replay_in_idx, REPLAY_SIG_RESET+1UL, 0UL, sizeof(fd_poh_reset_t) );
  FD_TEST( state->next_leader_slot == 600UL );
  FD_TEST( state->next_step_deadline == 1234L );

  fd_bam_test_receive_ingress_frag( state, state->replay_in_idx, REPLAY_SIG_TXN_EXECUTED, 0UL, sizeof(fd_replay_txn_executed_t) );
  FD_TEST( state->next_leader_slot == 600UL );
  FD_TEST( state->next_step_deadline == 1234L );

  fd_bam_test_receive_ingress_frag( state, state->replay_in_idx, REPLAY_SIG_RESET, 0UL, sizeof(fd_bam_bundle_result_t) );
  FD_TEST( state->next_leader_slot == 600UL );
  FD_TEST( state->next_step_deadline == 1234L );

  test_bam_replay_frag.msg.next_leader = (fd_replay_next_leader_t){ .slot = 500UL, .next_leader_slot = 700UL };
  ulong const bad_sizes[] = { 0UL, sizeof(fd_replay_next_leader_t)-1UL, sizeof(fd_replay_next_leader_t)+1UL };
  for( ulong i=0UL; i<sizeof(bad_sizes)/sizeof(bad_sizes[0]); i++ ) {
    fd_bam_test_receive_ingress_frag( state, state->replay_in_idx, REPLAY_SIG_NEXT_LEADER, 0UL, bad_sizes[i] );
    FD_TEST( state->next_leader_slot==600UL && state->next_step_deadline==1234L );
  }
  fd_bam_test_receive_ingress_frag( state, state->replay_in_idx, REPLAY_SIG_NEXT_LEADER, 1UL, sizeof(fd_replay_next_leader_t) );
  FD_TEST( state->next_leader_slot==600UL && state->next_step_deadline==1234L );

  test_bam_env_destroy( env );
}

/* Replay announces the first Alpenglow leader before its boot reset.
   This must open BAM's connection gate before the usual unknown-schedule
   retry, and the reset's empty hint must not close it again. */
static void
test_bam_replay_next_leader_schedule( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_replay_in_setup( state );
  state->leader_schedule_gate_start_ns = 1234L;
  state->next_step_deadline = 1234L;
  FD_TEST( fd_bam_tile_waiting_for_leader_slot( state ) );

  test_bam_replay_next_leader( state, 500UL, 600UL );
  FD_TEST( state->next_leader_slot==600UL );
  FD_TEST( state->leader_schedule_recheck_slot==FD_BAM_LEADER_SCHEDULE_RECHECK_NONE_SLOT );
  FD_TEST( !state->leader_schedule_gate_start_ns && !state->next_step_deadline );
  FD_TEST( !fd_bam_tile_leader_schedule_gate_active( state ) );
  test_bam_replay_reset( state, 500UL, ULONG_MAX );
  FD_TEST( state->next_leader_slot==600UL );
  FD_TEST( !fd_bam_tile_leader_schedule_gate_active( state ) );

  /* Unlike an empty boot reset, a schedule announcement with no leader
     is authoritative and restores slot-progress-based retrying. */
  test_bam_replay_next_leader( state, 501UL, ULONG_MAX );
  FD_TEST( state->next_leader_slot==ULONG_MAX );
  FD_TEST( state->leader_schedule_recheck_slot==565UL );
  FD_TEST( fd_bam_tile_waiting_for_leader_slot( state ) );

  /* The new signal uses the same current-epoch admission filter as
     reset hints; it cannot make an old or future epoch eligible. */
  test_bam_replay_slot_completed( state, 500UL, 100UL, 400UL );
  ulong const slots[] = { 399UL, 850UL, 700UL };
  for( ulong i=0UL; i<sizeof(slots)/sizeof(slots[0]); i++ ) {
    state->next_step_deadline = 1234L;
    test_bam_replay_next_leader( state, 501UL, slots[i] );
    FD_TEST( state->next_leader_slot==slots[i] && !state->next_step_deadline );
    FD_TEST( state->epoch_slot0==400UL && state->epoch_slot_cnt==400UL );
    FD_TEST( fd_bam_tile_leader_schedule_gate_active( state )==(i!=2UL) );
  }
  test_bam_replay_next_leader( state, 799UL, 800UL );
  FD_TEST( fd_bam_tile_leader_schedule_gate_active( state ) );
  test_bam_replay_slot_completed( state, 800UL, 0UL, 400UL );
  FD_TEST( !fd_bam_tile_leader_schedule_gate_active( state ) );

  test_bam_env_destroy( env );
}

static void
test_bam_replay_schedule_recheck_slot_policy( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  test_bam_replay_in_setup( state );
  FD_TEST( state->next_leader_slot==ULONG_MAX && state->tcp_sock<0 );
  FD_TEST( state->leader_schedule_recheck_slot==FD_BAM_LEADER_SCHEDULE_RECHECK_NONE_SLOT );

  state->leader_schedule_gate_start_ns = 111L;
  test_bam_replay_reset( state, 100UL, ULONG_MAX );
  FD_TEST( state->leader_schedule_recheck_slot == 164UL );
  FD_TEST( state->leader_schedule_gate_start_ns == 0L );
  FD_TEST( fd_bam_tile_waiting_for_leader_slot( state ) );

  state->leader_schedule_gate_start_ns = 222L;
  state->next_step_deadline = 1234L;
  test_bam_replay_slot_completed( state, 150UL, 398UL, 400UL );
  FD_TEST( state->leader_schedule_recheck_slot == 151UL );
  FD_TEST( state->leader_schedule_gate_start_ns == 0L );
  FD_TEST( fd_bam_tile_waiting_for_leader_slot( state ) );
  FD_TEST( state->next_step_deadline == 0L );

  state->leader_schedule_gate_start_ns = 333L;
  test_bam_replay_slot_completed( state, 151UL, 399UL, 400UL );
  FD_TEST( state->leader_schedule_recheck_slot == FD_BAM_LEADER_SCHEDULE_RECHECK_DUE_SLOT );
  FD_TEST( state->leader_schedule_gate_start_ns == 0L );
  FD_TEST( !fd_bam_tile_waiting_for_leader_slot( state ) );

  state->leader_schedule_gate_start_ns = 444L;
  test_bam_replay_reset( state, 153UL, 220UL );
  FD_TEST( state->next_leader_slot == 220UL );
  FD_TEST( state->leader_schedule_recheck_slot == FD_BAM_LEADER_SCHEDULE_RECHECK_NONE_SLOT );
  FD_TEST( state->leader_schedule_gate_start_ns == 0L );
  FD_TEST( !fd_bam_tile_waiting_for_leader_slot( state ) );

  test_bam_env_destroy( env );
}

/* A node in maintenance mode refuses every retry with the same
   UNAVAILABLE.  The streak warns its duration every interval and keeps
   queued results, also without a known leader slot; another reason, a
   new epoch, or a healthy session restarts it. */
static void
test_bam_scheduler_unavailable_reject_streak( fd_wksp_t * wksp ) {
  long const saved_clock = g_clock;
  g_clock = (long)10e9;
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  char const maintenance[] = "Node is in maintenance mode";
  char const unhealthy[]   = "Node is not healthy";
  uint const unavailable   = FD_GRPC_STATUS_UNAVAILABLE;

  test_bam_replay_in_setup( state );
  test_bam_replay_slot_completed( state, 500UL, 100UL, 400UL ); /* epoch [400,800) */
  state->next_leader_slot = 700UL;
  fd_bam_bundle_result_t res = test_make_bundle_result( 1U, 100UL, 1 );
  res.scheduler_gen = state->scheduler_gen;
  fd_bam_enqueue_result( state, &res );

  long const t0 = g_clock;
  FD_TEST( !test_bam_scheduler_reject_cycle( env, unavailable, maintenance ) );
  g_clock += (long)500e6;
  FD_TEST(  test_bam_scheduler_reject_cycle( env, unavailable, maintenance ) );
  FD_TEST( state->bam_stream_reject_start_ns==t0 && state->bam_stream_reject_warn_ns==t0 );

  /* The first repeat after the interval warns the streak's duration. */
  g_clock = t0 + FD_BAM_DISCONNECTED_WARN_INTERVAL_NS;
  FD_TEST(  test_bam_scheduler_reject_cycle( env, unavailable, maintenance ) );
  FD_TEST( state->bam_stream_reject_start_ns==t0 && state->bam_stream_reject_warn_ns==g_clock );

  state->next_leader_slot = ULONG_MAX;
  FD_TEST(  test_bam_scheduler_reject_cycle( env, unavailable, maintenance ) );
  FD_TEST( state->feedback_queue_depth==1U && !state->metrics.feedback_results_dropped_cnt );

  g_clock += (long)500e6;
  FD_TEST( !test_bam_scheduler_reject_cycle( env, unavailable, unhealthy ) );
  FD_TEST( state->bam_stream_reject_start_ns==g_clock );
  FD_TEST(  test_bam_scheduler_reject_cycle( env, unavailable, unhealthy ) );
  test_bam_replay_slot_completed( state, 800UL, 0UL, 400UL );
  FD_TEST( !test_bam_scheduler_reject_cycle( env, unavailable, unhealthy ) );

  test_bam_env_mock_conn( env );
  int charge_busy = 0;
  fd_bam_client_step( state, &charge_busy );
  FD_TEST( state->bam_status_counted==FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
  FD_TEST( !state->bam_stream_reject_hash );

  /* A PERMISSION_DENIED repeat never warns again within its epoch. */
  char const not_scheduled[] = "Validator is not on the leader schedule";
  FD_TEST( !test_bam_scheduler_reject_cycle( env, FD_GRPC_STATUS_PERMISSION_DENIED, not_scheduled ) );
  long const warned_ns = state->bam_stream_reject_warn_ns;
  g_clock += FD_BAM_DISCONNECTED_WARN_INTERVAL_NS;
  FD_TEST(  test_bam_scheduler_reject_cycle( env, FD_GRPC_STATUS_PERMISSION_DENIED, not_scheduled ) );
  FD_TEST( state->bam_stream_reject_warn_ns==warned_ns );

  test_bam_env_destroy( env );
  g_clock = saved_clock;
}

/* The BAM node admits only validators on its current epoch's leader
   schedule.  A leader slot that replay names in the next epoch or in
   an earlier one (Alpenglow resets name only the slot being led) leaves
   BAM behind its leader schedule gate
   instead of redialing a node that rejects it after every backoff. */

static void
test_bam_replay_leader_slot_outside_epoch( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  test_bam_replay_in_setup( state );

  /* Before replay completes a slot, any leader slot counts. */
  test_bam_replay_reset( state, 500UL, 850UL );
  FD_TEST( !fd_bam_tile_leader_schedule_gate_active( state ) );

  /* No slot for us in epoch [400,800); our first is in the next one.
     Replay's first completed slot is not an epoch change. */
  state->bam_stream_reject_hash = 1UL;
  test_bam_replay_slot_completed( state, 500UL, 100UL, 400UL );
  FD_TEST( state->bam_stream_reject_hash==1UL );
  state->bam_stream_reject_hash = 0UL;
  FD_TEST( state->next_leader_slot==850UL );
  FD_TEST( state->leader_schedule_recheck_slot==564UL );
  FD_TEST( fd_bam_tile_waiting_for_leader_slot( state ) );

  state->leader_schedule_gate_start_ns = 111L;
  test_bam_replay_reset( state, 501UL, 850UL );
  FD_TEST( state->leader_schedule_gate_start_ns==111L );
  FD_TEST( state->leader_schedule_recheck_slot==564UL );
  FD_TEST( fd_bam_tile_waiting_for_leader_slot( state ) );

  /* From slot 722, 128 slots before ours, each replayed slot ends the
     wait after a failed dial: the node refuses auth within 40 slots of
     our slot, so a held redial could lose the window. */
  fd_bam_client_reset( state );
  test_bam_replay_reset( state, 721UL, 850UL );
  FD_TEST( fd_bam_tile_waiting_for_leader_slot( state ) );
  test_bam_replay_reset( state, 722UL, 850UL );
  FD_TEST( !fd_bam_tile_waiting_for_leader_slot( state ) );
  fd_bam_client_reset( state );
  test_bam_replay_slot_completed( state, 722UL, 322UL, 400UL );
  FD_TEST( !fd_bam_tile_waiting_for_leader_slot( state ) );

  /* Our first slot opens the next epoch.  It counts once replay
     completes a slot there, without another reset. */
  test_bam_replay_reset( state, 799UL, 800UL );
  FD_TEST( state->next_leader_slot==800UL );
  FD_TEST( fd_bam_tile_leader_schedule_gate_active( state ) );
  test_bam_replay_slot_completed( state, 800UL, 0UL, 400UL );
  FD_TEST( state->leader_schedule_recheck_slot==FD_BAM_LEADER_SCHEDULE_RECHECK_NONE_SLOT );
  FD_TEST( !fd_bam_tile_leader_schedule_gate_active( state ) );

  /* The slot we led last epoch no longer counts. */
  test_bam_replay_reset( state, 809UL, 810UL );
  FD_TEST( !fd_bam_tile_leader_schedule_gate_active( state ) );
  test_bam_replay_slot_completed( state, 1200UL, 0UL, 400UL );
  FD_TEST( state->leader_schedule_recheck_slot==1264UL );
  FD_TEST( fd_bam_tile_waiting_for_leader_slot( state ) );

  /* A fork still in the previous epoch, or inconsistent epoch fields,
     leave the epoch alone. */
  test_bam_replay_reset( state, 1210UL, 1300UL );
  FD_TEST( !fd_bam_tile_leader_schedule_gate_active( state ) );
  test_bam_replay_slot_completed( state, 1199UL, 399UL, 400UL );
  FD_TEST( !fd_bam_tile_leader_schedule_gate_active( state ) );
  test_bam_replay_reset( state, 1250UL, 1700UL );
  FD_TEST( fd_bam_tile_leader_schedule_gate_active( state ) );
  test_bam_replay_slot_completed( state, 1250UL, 1260UL, 2000UL );
  FD_TEST( fd_bam_tile_leader_schedule_gate_active( state ) );
  test_bam_replay_slot_completed( state, 1251UL, 0UL, 0UL );
  FD_TEST( fd_bam_tile_leader_schedule_gate_active( state ) );

  /* Repeated rejections while our only slot is in the next epoch are
     quiet, as for a validator with no slot at all. */
  char const not_scheduled[] = "Validator is not on the leader schedule: 7heQNXEtxSv3wV8sNbuQsDN5xNGbbpLvHGiyXuJxEf7n";
  FD_TEST( !test_bam_scheduler_reject_cycle( env, FD_GRPC_STATUS_PERMISSION_DENIED, not_scheduled ) );
  FD_TEST(  test_bam_scheduler_reject_cycle( env, FD_GRPC_STATUS_PERMISSION_DENIED, not_scheduled ) );

  /* The streak lasts the epoch.  A new epoch ends it, so a validator
     still rejected warns once per epoch; a fork back in the previous
     epoch does not end it again. */
  test_bam_replay_slot_completed( state, 1260UL, 60UL, 400UL );
  FD_TEST(  test_bam_scheduler_reject_cycle( env, FD_GRPC_STATUS_PERMISSION_DENIED, not_scheduled ) );
  test_bam_replay_slot_completed( state, 2000UL, 0UL, 400UL );
  FD_TEST( !test_bam_scheduler_reject_cycle( env, FD_GRPC_STATUS_PERMISSION_DENIED, not_scheduled ) );
  FD_TEST(  test_bam_scheduler_reject_cycle( env, FD_GRPC_STATUS_PERMISSION_DENIED, not_scheduled ) );
  test_bam_replay_slot_completed( state, 1599UL, 399UL, 400UL );
  FD_TEST(  test_bam_scheduler_reject_cycle( env, FD_GRPC_STATUS_PERMISSION_DENIED, not_scheduled ) );

  test_bam_env_destroy( env );
}

static void
test_bam_replay_wait_gate_policy( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  state->replay_in_idx = 2UL;
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
  state->replay_in_idx = ULONG_MAX;
  FD_TEST( !fd_bam_tile_waiting_for_leader_slot( state ) );

  test_bam_env_destroy( env );
}

static void
test_bam_replay_wait_gate_cleared_on_reset_or_disable( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  state->replay_in_idx = 2UL;
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
test_bam_identity_switch_halts_signing_and_invalidates_leader_schedule_state( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );

  fd_keyswitch_t keyswitch = {0};
  keyswitch.state = FD_KEYSWITCH_STATE_SWITCH_PENDING;
  for( ulong i=0UL; i<32UL; i++ ) keyswitch.bytes[ i ] = (uchar)( 200UL + i );
  state->keyswitch = &keyswitch;

  char expected_b58[ FD_BASE58_ENCODED_32_SZ ];
  FD_TEST( fd_base58_encode_32( keyswitch.bytes, NULL, expected_b58 ) );

  state->next_leader_slot              = 12345UL;
  state->epoch_slot0 = 1000UL; state->epoch_slot_cnt = 100UL; state->bam_admitted_epoch_end = 1100UL;
  state->leader_schedule_recheck_slot  = 67890UL;
  state->leader_schedule_gate_start_ns = 5555L;
  state->bam_stream_reject_hash        = 1UL;
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

  FD_TEST( state->halt_signing == 1U );
  FD_TEST( 0==memcmp( state->bam_identity_pubkey, keyswitch.bytes, 32UL ) );
  FD_TEST( 0==strcmp( state->bam_identity_pubkey_b58, expected_b58 ) );
  FD_TEST( fd_keyswitch_state_query( state->keyswitch ) == FD_KEYSWITCH_STATE_COMPLETED );

  FD_TEST( state->tcp_sock == -1 );
  FD_TEST( state->tcp_sock_connected == 0U );
  FD_TEST( state->bam_stream_live == 0U );
  FD_TEST( state->defer_reset == 0U );

  FD_TEST( state->next_leader_slot == ULONG_MAX && !state->bam_admitted_epoch_end );
  FD_TEST( state->leader_schedule_recheck_slot == FD_BAM_LEADER_SCHEDULE_RECHECK_DUE_SLOT );
  FD_TEST( state->leader_schedule_gate_start_ns == 0L );
  FD_TEST( state->bam_stream_reject_hash == 0UL );
  FD_TEST( state->bam_leader_state.slot == ULONG_MAX );
  FD_TEST( state->bam_leader_state.tick == 0U );
  FD_TEST( state->bam_leader_state.slot_cu_budget_remaining == 0U );
  FD_TEST( state->bam_leader_state.slot_end_ns == 0L );
  FD_TEST( state->bam_leader_state.current_slot_has_bam_work == 0U );
  FD_TEST( state->bam_leader_pending == 0U );
  FD_TEST( bam_pending_txn_empty( state->pending_txns ) );
  FD_TEST( state->feedback_queue_depth == 0UL );
  FD_TEST( state->bam_results_head == state->bam_results_tail );
  FD_TEST( state->metrics.feedback_results_dropped_cnt == dropped_before+1UL );

  /* Client progress, including keyguard signing, remains stopped until the
     identity coordinator confirms that the sign tile has switched keys. */
  state->defer_reset = 1U;
  int charge_busy = 0;
  fd_bam_test_before_credit( state, env->stem, &charge_busy );
  FD_TEST( state->defer_reset == 1U );
  FD_TEST( charge_busy == 0 );

  state->defer_reset = 0U;
  fd_keyswitch_state( state->keyswitch, FD_KEYSWITCH_STATE_UNHALT_PENDING );
  fd_bam_tile_housekeeping( state );
  FD_TEST( state->halt_signing == 0U );
  FD_TEST( state->backoff_until == 0L );
  FD_TEST( fd_keyswitch_state_query( state->keyswitch ) == FD_KEYSWITCH_STATE_COMPLETED );

  test_bam_env_destroy( env );
}

static void
test_bam_replay_schedule_hint_does_not_disconnect( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  g_clock = (long)50e9;
  state->replay_in_idx = 2UL;
  state->replay_in.mem = wksp;
  state->next_leader_slot = ULONG_MAX;
  test_bam_set_server( state, 1U );

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
  FD_TEST( FD_MGAUGE_GET( BAM, IS_HEALTHY ) == 1UL );
  FD_TEST( FD_MGAUGE_GET( BAM, IS_STREAM_LIVE ) == 1UL );
  FD_TEST( state->tcp_sock >= 0 );
  FD_TEST( state->tcp_sock_connected == 1U );

  state->next_leader_slot     = 100000UL;
  fd_bam_tile_housekeeping( state );
  fd_bam_test_metrics_write( state );
  FD_TEST( state->bam_status_recent == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );
  FD_TEST( FD_MGAUGE_GET( BAM, IS_HEALTHY ) == 1UL );
  FD_TEST( state->tcp_sock >= 0 );
  FD_TEST( state->tcp_sock_connected == 1U );

  test_bam_env_destroy( env );
}

static void
test_bam_pack_leader_channel_contract( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  union {
    fd_bam_leader_state_t leader;
    uchar                 bytes[ sizeof(fd_bam_leader_state_t) ];
  } ingress = { .leader = {
    .slot = 77UL,
    .tick = 9U,
    .slot_cu_budget_remaining = 456U
  } };

  state->pack_bam_leader_in_idx = 3UL;
  state->pack_leader_in = (fd_bam_in_ctx_t){ .mem = (fd_wksp_t *)ingress.bytes };

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

/* BAM consumes every input unreliably, so a lapping producer may reuse a
   frag's dcache slot before the stem's overrun check.  after_frag must
   commit what during_frag copied, never re-read the dcache. */
static void
test_bam_ingress_frag_copied_before_overrun_check( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  union {
    fd_bam_bundle_result_t result;
    fd_bam_leader_state_t  leader;
    fd_poh_reset_t         reset;
    fd_replay_next_leader_t next_leader;
    uchar                  bytes[ 1024 ];
  } in[ 4 ];
  fd_memset( in, 0, sizeof(in) );
  state->bank_bam_in_cnt        = 1UL;
  state->bank_bam_in_idx        = 0UL;
  state->bank_in[ 0 ]           = (fd_bam_in_ctx_t){ .mem = (fd_wksp_t *)in[ 0 ].bytes };
  state->pack_bam_result_in_idx = 1UL;
  state->pack_result_in         = (fd_bam_in_ctx_t){ .mem = (fd_wksp_t *)in[ 1 ].bytes };
  state->pack_bam_leader_in_idx = 2UL;
  state->pack_leader_in         = (fd_bam_in_ctx_t){ .mem = (fd_wksp_t *)in[ 2 ].bytes };
  state->replay_in_idx          = 3UL;
  state->replay_in              = (fd_bam_in_ctx_t){ .mem = (fd_wksp_t *)in[ 3 ].bytes };

  for( ulong i=0UL; i<2UL; i++ ) {
    in[ i ].result = test_make_bundle_result( 71U+(uint)i, 900UL, 1U );
    in[ i ].result.scheduler_gen = state->scheduler_gen;
    fd_bam_test_receive_ingress_frag_lapped( state, i, 0UL, 0UL, sizeof(fd_bam_bundle_result_t), in[ i ].bytes );
    FD_TEST( state->feedback_queue_depth==i+1UL );
    FD_TEST( state->bam_results[ i ].seq_id==71U+(uint)i && state->bam_results[ i ].slot==900UL );
  }
  in[ 2 ].leader = (fd_bam_leader_state_t){ .slot = 77UL, .tick = 9U, .slot_cu_budget_remaining = 456U };
  fd_bam_test_receive_ingress_frag_lapped( state, 2UL, 0UL, 0UL, sizeof(fd_bam_leader_state_t), in[ 2 ].bytes );
  FD_TEST( state->bam_leader_state.slot==77UL && state->bam_leader_state.slot_cu_budget_remaining==456U );
  in[ 3 ].reset.next_leader_slot = 88UL;
  in[ 3 ].reset.completed_slot   = 80UL;
  fd_bam_test_receive_ingress_frag_lapped( state, 3UL, REPLAY_SIG_RESET, 0UL, sizeof(fd_poh_reset_t), in[ 3 ].bytes );
  FD_TEST( state->next_leader_slot==88UL );
  in[ 3 ].next_leader = (fd_replay_next_leader_t){ .slot = 90UL, .next_leader_slot = 99UL };
  fd_bam_test_receive_ingress_frag_lapped( state, 3UL, REPLAY_SIG_NEXT_LEADER, 0UL, sizeof(fd_replay_next_leader_t), in[ 3 ].bytes );
  FD_TEST( state->next_leader_slot==99UL );
  in[ 3 ].next_leader = (fd_replay_next_leader_t){ .slot = 100UL, .next_leader_slot = ULONG_MAX };
  fd_bam_test_receive_ingress_frag_lapped( state, 3UL, REPLAY_SIG_NEXT_LEADER, 0UL, sizeof(fd_replay_next_leader_t), in[ 3 ].bytes );
  FD_TEST( state->next_leader_slot==ULONG_MAX && state->leader_schedule_recheck_slot==164UL );

  test_bam_env_destroy( env );
}

static void
test_bam_pack_leader_slot_change_flushes_immediately( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );

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
  state->pack_leader_in = (fd_bam_in_ctx_t){ .mem = (fd_wksp_t *)ingress.bytes };

  fd_bam_test_receive_ingress_frag( state, state->pack_bam_leader_in_idx, 0UL, 0UL, sizeof(fd_bam_leader_state_t) );

  FD_TEST( state->bam_leader_pending == 0U );
  FD_TEST( state->bam_leader_state.slot == ingress.leader.slot );
  FD_TEST( state->bam_leader_state.tick == ingress.leader.tick );
  FD_TEST( state->bam_leader_state.slot_cu_budget_remaining == ingress.leader.slot_cu_budget_remaining );
  FD_TEST( state->metrics.outbound_enqueue_outcome_cnt[ FD_METRICS_ENUM_BAM_ENQUEUE_RESULT_V_LEADER_STATE_ENQUEUED_IDX ] == 1UL );

  test_bam_expect_leader_state( state, ingress.leader.slot, ingress.leader.tick, ingress.leader.slot_cu_budget_remaining );

  /* A slot change that finds the TX path busy stays pending for a later
     step and is not a send failure. */
  FD_TEST( fd_h2_tx_ping( fd_grpc_client_h2_conn( state->grpc_client ),
                          fd_grpc_client_rbuf_tx( state->grpc_client ) ) );
  ingress.leader.slot = 78UL;
  fd_bam_test_receive_ingress_frag( state, state->pack_bam_leader_in_idx, 0UL, 0UL, sizeof(fd_bam_leader_state_t) );
  FD_TEST( state->bam_leader_pending == 1U );
  FD_TEST( state->bam_leader_state.slot == 78UL );
  FD_TEST( state->metrics.outbound_enqueue_outcome_cnt[ FD_METRICS_ENUM_BAM_ENQUEUE_RESULT_V_LEADER_STATE_ENQUEUED_IDX ] == 1UL );
  FD_TEST( state->metrics.outbound_enqueue_outcome_cnt[ FD_METRICS_ENUM_BAM_ENQUEUE_RESULT_V_LEADER_STATE_ENQUEUE_FAILED_IDX ] == 0UL );

  test_bam_env_destroy( env );
}

static void
test_bam_pack_result_channel_contract( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  union {
    fd_bam_bundle_result_t result;
    uchar                  bytes[ sizeof(fd_bam_bundle_result_t) ];
  } ingress = { .result = {0} };
  ingress.result = test_make_bundle_result( 905U, 1905UL, 2U );

  state->pack_bam_result_in_idx = 4UL;
  state->pack_result_in = (fd_bam_in_ctx_t){ .mem = (fd_wksp_t *)ingress.bytes };

  /* A result arriving on a stem input must preempt an old network timer. */
  long future_deadline = fd_bam_now() + (long)5e9;
  state->next_step_deadline = future_deadline;
  fd_bam_test_receive_ingress_frag( state, state->pack_bam_result_in_idx, 0UL, 0UL, sizeof(fd_bam_bundle_result_t) );
  FD_TEST( state->next_step_deadline==0L );
  int charge_busy = 0;
  fd_bam_test_before_credit( state, env->stem, &charge_busy );
  FD_TEST( state->next_step_deadline!=future_deadline );

  FD_TEST( state->feedback_queue_depth == 1U );
  FD_TEST( state->bam_results_head == 0U );
  FD_TEST( state->bam_results_tail == 1U );
  FD_TEST( state->bam_results[ 0 ].seq_id == ingress.result.seq_id );
  FD_TEST( state->bam_results[ 0 ].slot == ingress.result.slot );
  FD_TEST( state->bam_results[ 0 ].bundle_txn_cnt == ingress.result.bundle_txn_cnt );
  FD_TEST( state->bam_leader_pending == 0U );

  fd_bam_test_receive_ingress_frag( state, state->pack_bam_result_in_idx, 0UL, 0UL, sizeof(fd_bam_bundle_result_t) );

  FD_TEST( state->feedback_queue_depth == 1U );
  FD_TEST( state->bam_results_tail == 1U );

  ingress.result.slot++;
  fd_bam_test_receive_ingress_frag( state, state->pack_bam_result_in_idx, 0UL, 0UL, sizeof(fd_bam_bundle_result_t) );

  FD_TEST( state->feedback_queue_depth == 2U );
  FD_TEST( state->bam_results_tail == 2U );
  FD_TEST( state->bam_results[ 1 ].slot == ingress.result.slot );

  fd_bam_test_receive_ingress_frag( state, state->pack_bam_result_in_idx, 0UL, 0UL, sizeof(fd_bam_leader_state_t) );

  FD_TEST( state->feedback_queue_depth == 2U );
  FD_TEST( state->bam_results[ 0 ].seq_id == ingress.result.seq_id );
  FD_TEST( state->bam_leader_pending == 0U );

  test_bam_env_destroy( env );
}

static void
test_bam_bank_result_channel_contract( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  union {
    fd_bam_bundle_result_t result;
    uchar                  bytes[ sizeof(fd_bam_bundle_result_t) ];
  } ingress[2] = {0};
  ingress[0].result = test_make_bundle_result( 501U, 1501UL, 1U );
  ingress[1].result = test_make_bundle_result( 502U, 1502UL, 2U );

  state->bank_bam_in_cnt    = 2UL;
  state->bank_bam_in_idx    = 5UL;
  state->bank_in[0] = (fd_bam_in_ctx_t){ .mem = (fd_wksp_t *)ingress[0].bytes };
  state->bank_in[1] = (fd_bam_in_ctx_t){ .mem = (fd_wksp_t *)ingress[1].bytes };

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

static bam_types_AtomicTxnBatchResult const *
test_bam_round_trip_result( fd_bam_tile_t *               state,
                            fd_bam_bundle_result_t const * result,
                            test_bam_decoded_message_t *   decoded ) {
  test_enqueue_bundle_result( state, result );
  return test_bam_flush_one_result( state, result->seq_id, decoded );
}

/* Bundle results reach the node as a MultipleAtomicTxnBatchResult with
   the committed or NotCommitted reason it expects. */
static void
test_bam_scheduler_result_publishes_message( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_live_env_create( env, wksp, (long)13e9 );
  test_bam_decoded_message_t           decoded;
  test_bam_committed_results_t const * committed = &decoded.multi.committed[ 0 ];
  bam_types_NotCommitted const *       nc        = &decoded.multi.results[ 0 ].result.not_committed;
  uint const                           c_tag     = bam_types_AtomicTxnBatchResult_committed_tag;
  uint const                           nc_tag    = bam_types_AtomicTxnBatchResult_not_committed_tag;

  fd_bam_bundle_result_t res = test_make_bundle_result( 900, 1900, 2 );
  FD_TEST( test_bam_round_trip_result( state, &res, &decoded )->which_result==c_tag );
  FD_TEST( committed->txn_cnt == res.bundle_txn_cnt );
  FD_TEST( committed->txns[0].cus_consumed == res.consumed_cus[0] );
  FD_TEST( committed->txns[0].feepayer_balance_lamports == res.feepayer_balance_lamports[0] );
  FD_TEST( committed->txns[0].loaded_accounts_data_size == res.loaded_accounts_data_size[0] );

  /* Committed can still report execution_success=false (fees-only / instruction error). */
  res = test_make_bundle_result( 908, 1908, 1 );
  fd_bam_result_add_txn_error( &res, 0UL, bam_types_TransactionErrorReason_INSTRUCTION_ERROR );
  FD_TEST( test_bam_round_trip_result( state, &res, &decoded )->which_result==c_tag );
  FD_TEST( committed->txn_cnt == 1UL );
  FD_TEST( committed->txns[0].execution_success == false );

  res = test_make_bundle_result( 914, 1914, 3 );
  fd_bam_result_add_txn_error( &res, 1UL, bam_types_TransactionErrorReason_INSTRUCTION_ERROR );
  FD_TEST( test_bam_round_trip_result( state, &res, &decoded )->which_result==c_tag );
  FD_TEST( committed->txn_cnt == 3UL );
  FD_TEST( committed->txns[0].execution_success == true  );
  FD_TEST( committed->txns[1].execution_success == false );
  FD_TEST( committed->txns[2].execution_success == true  );

  res = test_make_bundle_result( 901, 1900, 2 );
  res.execution_success = 0;
  res.scheduling_error  = FD_BAM_SCHED_ERR_OUTSIDE_SLOT;
  FD_TEST( test_bam_round_trip_result( state, &res, &decoded )->which_result==nc_tag );
  FD_TEST( nc->which_reason == bam_types_NotCommitted_scheduling_error_tag );
  FD_TEST( nc->reason.scheduling_error == bam_types_SchedulingError_OUTSIDE_LEADER_SLOT );

  res = test_make_bundle_result( 902, 1902, 2 );
  res.execution_success   = 0;
  res.sanitize_success[1] = 0;
  FD_TEST( test_bam_round_trip_result( state, &res, &decoded )->which_result==nc_tag );
  FD_TEST( nc->which_reason == bam_types_NotCommitted_deserialization_error_tag );
  FD_TEST( nc->reason.deserialization_error.index == 1U );
  FD_TEST( nc->reason.deserialization_error.reason == bam_types_DeserializationErrorReason_SANITIZE_ERROR );

  res = test_make_bundle_result( 913, 1913, 3 );
  res.execution_success      = 0;
  res.bundle_err             = FD_BAM_BUNDLE_ERR_DESER;
  res.deser_index            = 2U;
  res.deser_reason           = bam_types_DeserializationErrorReason_SANITIZE_ERROR;
  FD_TEST( test_bam_round_trip_result( state, &res, &decoded )->which_result==nc_tag );
  FD_TEST( nc->which_reason == bam_types_NotCommitted_deserialization_error_tag );
  FD_TEST( nc->reason.deserialization_error.index == 2U );
  FD_TEST( nc->reason.deserialization_error.reason == bam_types_DeserializationErrorReason_SANITIZE_ERROR );

  res = test_make_bundle_result( 903, 1903, 2 );
  res.execution_success  = 0;
  fd_bam_result_add_txn_error( &res, 0UL, bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND );
  FD_TEST( test_bam_round_trip_result( state, &res, &decoded )->which_result==nc_tag );
  FD_TEST( nc->which_reason == bam_types_NotCommitted_transaction_error_tag );
  FD_TEST( nc->reason.transaction_error.index == 0U );
  FD_TEST( nc->reason.transaction_error.reason == bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND );

  res = test_make_bundle_result( 915, 1915, 3 );
  fd_bam_result_mark_sanitize_success_all( &res );
  fd_bam_result_mark_not_committed_txn_error( &res, 1UL, bam_types_TransactionErrorReason_ACCOUNT_IN_USE );
  FD_TEST( test_bam_round_trip_result( state, &res, &decoded )->which_result==nc_tag );
  FD_TEST( nc->which_reason == bam_types_NotCommitted_transaction_error_tag );
  FD_TEST( nc->reason.transaction_error.index == 1U );
  FD_TEST( nc->reason.transaction_error.reason == bam_types_TransactionErrorReason_ACCOUNT_IN_USE );

  /* For atomicity cascades, prefer the non-CommitCancelled reason/index. */
  res = test_make_bundle_result( 909, 1909, 3 );
  res.execution_success = 0;
  fd_bam_result_add_txn_error( &res, 0UL, bam_types_TransactionErrorReason_COMMIT_CANCELLED );
  fd_bam_result_add_txn_error( &res, 1UL, bam_types_TransactionErrorReason_COMMIT_CANCELLED );
  fd_bam_result_add_txn_error( &res, 2UL, bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND );
  FD_TEST( test_bam_round_trip_result( state, &res, &decoded )->which_result==nc_tag );
  FD_TEST( nc->which_reason == bam_types_NotCommitted_transaction_error_tag );
  FD_TEST( nc->reason.transaction_error.index == 2U );
  FD_TEST( nc->reason.transaction_error.reason == bam_types_TransactionErrorReason_BLOCKHASH_NOT_FOUND );

  /* Atomic bundles with only CommitCancelled reasons should fall back to
     SchedulingError::POH_TIMEOUT when no primary transaction reason exists. */
  res = test_make_bundle_result( 910, 1910, 3 );
  res.execution_success = 0;
  for( uint i=0U; i<res.bundle_txn_cnt; i++ ) {
    fd_bam_result_add_txn_error( &res, i, bam_types_TransactionErrorReason_COMMIT_CANCELLED );
  }
  FD_TEST( test_bam_round_trip_result( state, &res, &decoded )->which_result==nc_tag );
  FD_TEST( nc->which_reason == bam_types_NotCommitted_scheduling_error_tag );
  FD_TEST( nc->reason.scheduling_error == bam_types_SchedulingError_POH_TIMEOUT );

  /* Generic execution failure yields generic_invalid. */
  res = test_make_bundle_result( 904, 1904, 2 );
  res.execution_success = 0;
  FD_TEST( test_bam_round_trip_result( state, &res, &decoded )->which_result==nc_tag );
  FD_TEST( nc->which_reason == bam_types_NotCommitted_generic_invalid_tag );
  FD_TEST( !strcmp( nc->reason.generic_invalid.message, FD_BAM_ERR_MSG_BUNDLE_EXECUTION_FAILED ) );

  test_bam_env_destroy( env );
}

/* Codes the node treats differently must match jito-solana: an unsupported
   transaction version is not retryable (COMMIT_CANCELLED would surface as a
   retryable POH_TIMEOUT), and a bundle-blacklisted account is a sanitize
   failure. */
static void
test_bam_txn_err_mapping_matches_jito( void ) {
  FD_TEST( fd_bam_txn_err_from_runtime_err( FD_RUNTIME_TXN_ERR_UNSUPPORTED_VERSION )==
           bam_types_TransactionErrorReason_UNSUPPORTED_VERSION );
  FD_TEST( fd_bam_txn_err_from_pack_insert( FD_PACK_INSERT_REJECT_BUNDLE_BLACKLIST )==
           bam_types_TransactionErrorReason_SANITIZE_FAILURE );
}

/* --- Control surface --------------------------------------------------------------- */

/* An idle control block, no endpoint, BAM disabled, and a completed
   keyswitch. */
static void
setup_ctrl_dormant( fd_bam_tile_t * ctx,
                    fd_bam_ctrl_t * ctrl ) {
  static fd_keyswitch_t keyswitch;
  keyswitch = (fd_keyswitch_t){ .magic = FD_KEYSWITCH_MAGIC, .state = FD_KEYSWITCH_STATE_COMPLETED };
  ctx->keyswitch = &keyswitch;
  ctx->ctrl      = ctrl;
  fd_memset( ctrl, 0, sizeof(fd_bam_ctrl_t) );
  ctrl->state = FD_BAM_CTRL_STATE_IDLE;

  ctx->enabled         = 0;
  ctx->server_fqdn[0]  = '\0';
  ctx->server_fqdn_len = 0U;
  ctx->server_sni[0]   = '\0';
  ctx->server_sni_len  = 0U;
  ctx->server_tcp_port = 0U;
  ctx->is_ssl          = 0U;
}

/* Mirrors the on-startup state: control block idle, HTTP endpoint
   configured, BAM enabled. */
static void
setup_ctrl_defaults( fd_bam_tile_t * ctx,
                     fd_bam_ctrl_t * ctrl ) {
  setup_ctrl_dormant( ctx, ctrl );
  char const host[] = "testnet.bam.jito.wtf";
  fd_cstr_ncpy( ctx->server_fqdn, host, sizeof(ctx->server_fqdn) );
  fd_cstr_ncpy( ctx->server_sni,  host, sizeof(ctx->server_sni)  );
  ctx->server_fqdn_len = (ushort)strlen( host );
  ctx->server_sni_len  = (ushort)strlen( host );
  ctx->server_tcp_port = 50055;
  ctx->enabled         = 1;
  ctrl->enable         = 1U;
  ctrl->applied_enable = 1U;
  fd_cstr_ncpy( ctrl->url, "http://testnet.bam.jito.wtf:50055", sizeof( ctrl->url ) );
  fd_cstr_ncpy( ctrl->sni, host, sizeof( ctrl->sni ) );
}

/* Issues a set-bam control request (a NULL url leaves the URL alone),
   runs the housekeeping that serves it and returns the resulting state. */
static uchar
test_bam_ctrl_request( fd_bam_tile_t * ctx,
                       uchar           command,
                       uchar           enable,
                       char const *    url ) {
  fd_bam_ctrl_t * ctrl = ctx->ctrl;
  ctrl->command = command;
  ctrl->enable  = enable;
  if( url ) fd_cstr_ncpy( ctrl->url, url, sizeof( ctrl->url ) );
  ctrl->state = FD_BAM_CTRL_STATE_REQUEST;
  fd_bam_tile_housekeeping( ctx );
  return ctrl->state;
}

/* The endpoint the client dials, as configured and as handed to gRPC. */
static void
test_bam_check_endpoint( fd_bam_tile_t const * ctx,
                         char const *          host,
                         char const *          sni,
                         ushort                port,
                         uint                  is_ssl ) {
  FD_TEST( !strcmp( ctx->server_fqdn, host ) && ctx->server_fqdn_len==strlen( host ) );
  FD_TEST( !strcmp( ctx->server_sni,  sni  ) && ctx->server_sni_len ==strlen( sni  ) );
  FD_TEST( ctx->server_tcp_port==port && ctx->is_ssl==is_ssl );
  FD_TEST( !strcmp( ctx->grpc_client->host, sni ) && ctx->grpc_client->port==port );
}

static void
test_bam_ctrl_updates_url_and_sni( fd_wksp_t * wksp ) {
  /* Ensure runtime URL/SNI updates reconfigure the client and publish success to the control block. */
  test_bam_env_t env[1];
  fd_bam_tile_t * ctx = test_bam_env_create( env, wksp );
  fd_bam_ctrl_t ctrl;
  setup_ctrl_defaults( ctx, &ctrl );

  test_bam_seed_scheduler_work( ctx, 710U, 711U );
  ulong dropped_before = ctx->metrics.feedback_results_dropped_cnt;
  ctx->bam_stream_reject_hash = 1UL;

  fd_cstr_ncpy( ctrl.sni, "custom.sni.invalid", sizeof( ctrl.sni ) );
  FD_TEST( test_bam_ctrl_request( ctx, FD_BAM_CTRL_CMD_URL | FD_BAM_CTRL_CMD_SNI, 1U, "http://new.example.com:8899" )==FD_BAM_CTRL_STATE_SUCCESS );
  FD_TEST( ctrl.enable == 1U );
  FD_TEST( !strcmp( ctrl.url, "http://new.example.com:8899" ) );
  FD_TEST( !strcmp( ctrl.sni, "custom.sni.invalid" ) );
  FD_TEST( !strcmp( ctrl.error, "" ) );
  FD_TEST( ctx->enabled == 1 );
  test_bam_check_endpoint( ctx, "new.example.com", "custom.sni.invalid", 8899U, 0U );
  FD_TEST( bam_pending_txn_empty( ctx->pending_txns ) );
  FD_TEST( ctx->feedback_queue_depth == 0UL );
  FD_TEST( ctx->bam_results_head == ctx->bam_results_tail );
  FD_TEST( ctx->metrics.feedback_results_dropped_cnt == dropped_before+1UL );
  FD_TEST( !ctx->bam_stream_reject_hash );

  test_bam_env_destroy( env );
}

/* A URL without a port takes the BAM default for its scheme. */
static void
test_bam_ctrl_defaults_bam_ports( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * ctx = test_bam_env_create( env, wksp );
  fd_bam_ctrl_t ctrl;
  setup_ctrl_defaults( ctx, &ctrl );

  struct { char const * url; char const * applied; ushort port; uint is_ssl; } const cases[] = {
    { "http://bam.example.com",       "http://bam.example.com:50055",  50055U, 0U },
    { "https://bam.example.com",      "https://bam.example.com:50056", 50056U, 1U },
    { "https://bam.example.com:9443", "https://bam.example.com:9443",  9443U,  1U },
  };
  for( ulong i=0UL; i<sizeof(cases)/sizeof(cases[0]); i++ ) {
    FD_TEST( test_bam_ctrl_request( ctx, FD_BAM_CTRL_CMD_URL, 1U, cases[ i ].url )==FD_BAM_CTRL_STATE_SUCCESS );
    FD_TEST( !strcmp( ctrl.url, cases[ i ].applied ) );
    FD_TEST( !strcmp( ctrl.sni, "bam.example.com" ) );
    test_bam_check_endpoint( ctx, "bam.example.com", "bam.example.com", cases[ i ].port, cases[ i ].is_ssl );
  }

  test_bam_env_destroy( env );
}

static void
test_bam_ctrl_toggle_enable_updates_runtime_state( fd_wksp_t * wksp ) {
  /* Validate that toggling enable pauses connectivity and clears the status latch. */
  test_bam_env_t env[1];
  fd_bam_tile_t * ctx = test_bam_env_create( env, wksp );
  fd_bam_ctrl_t ctrl;
  setup_ctrl_defaults( ctx, &ctrl );

  test_bam_seed_scheduler_work( ctx, 720U, 721U );
  ulong dropped_before = ctx->metrics.feedback_results_dropped_cnt;

  ulong fseq[1] = { 0UL };
  ctx->bam_status_fseq = fseq;
  fd_bam_publish_active_state( ctx, ctx->stem, 1 );
  FD_TEST( fd_fseq_query( fseq ) == FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );

  test_bam_set_default_tpu( ctx );
  ctx->tpu_update_state = FD_BAM_CONTACT_UPDATE_STATE_APPLIED_BAM;

  FD_TEST( test_bam_ctrl_request( ctx, FD_BAM_CTRL_CMD_ENABLE, 0U, NULL )==FD_BAM_CTRL_STATE_SUCCESS );
  FD_TEST( ctrl.enable == 0U );
  FD_TEST( ctrl.applied_enable == 0U );
  FD_TEST( ctx->enabled == 0 );
  FD_TEST( fd_fseq_query( fseq ) == 0UL );
  FD_TEST( !strcmp( ctrl.url, "http://testnet.bam.jito.wtf:50055" ) );
  FD_TEST( bam_pending_txn_empty( ctx->pending_txns ) );
  FD_TEST( ctx->feedback_queue_depth == 0UL );
  FD_TEST( ctx->bam_results_head == ctx->bam_results_tail );
  FD_TEST( ctx->metrics.feedback_results_dropped_cnt == dropped_before+1UL );

  FD_TEST( test_bam_ctrl_request( ctx, FD_BAM_CTRL_CMD_ENABLE, 1U, NULL )==FD_BAM_CTRL_STATE_SUCCESS );
  FD_TEST( ctrl.enable == 1U );
  FD_TEST( ctrl.applied_enable == 1U );
  FD_TEST( ctx->enabled == 1U );
  FD_TEST( fd_fseq_query( fseq ) == 0UL );
  FD_TEST( !strcmp( ctrl.url, "http://testnet.bam.jito.wtf:50055" ) );
  FD_TEST( !strcmp( ctx->server_fqdn, "testnet.bam.jito.wtf" ) );
  FD_TEST( ctx->server_tcp_port == 50055U );

  test_bam_env_destroy( env );
}

static void
test_bam_ctrl_enable_from_disabled_start( fd_wksp_t * wksp ) {
  /* Ensure a tile launched with BAM disabled can be enabled via runtime control. */
  test_bam_env_t env[1];
  fd_bam_tile_t * ctx = test_bam_env_create( env, wksp );
  fd_bam_ctrl_t ctrl;
  setup_ctrl_defaults( ctx, &ctrl );
  ctx->enabled = 0;

  FD_TEST( test_bam_ctrl_request( ctx, FD_BAM_CTRL_CMD_ENABLE, 1U, NULL )==FD_BAM_CTRL_STATE_SUCCESS );
  FD_TEST( ctrl.enable == 1U );
  FD_TEST( ctx->enabled == 1 );
  FD_TEST( !strcmp( ctrl.url, "http://testnet.bam.jito.wtf:50055" ) );
  FD_TEST( !strcmp( ctrl.error, "" ) );

  test_bam_env_destroy( env );
}

static void
test_bam_ctrl_enable_from_dormant_then_set_url( fd_wksp_t * wksp ) {
  /* Dormant BAM has allocated control state but no effective endpoint, so
     --enable alone should set the desired runtime mode without dialing until a
     URL arrives. */
  test_bam_env_t env[1];
  fd_bam_tile_t * ctx = test_bam_env_create( env, wksp );
  fd_bam_ctrl_t ctrl;
  setup_ctrl_dormant( ctx, &ctrl );

  FD_TEST( test_bam_ctrl_request( ctx, FD_BAM_CTRL_CMD_ENABLE, 1U, NULL )==FD_BAM_CTRL_STATE_SUCCESS );
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

  FD_TEST( test_bam_ctrl_request( ctx, FD_BAM_CTRL_CMD_URL, 1U, "http://bam.example.com:8899" )==FD_BAM_CTRL_STATE_SUCCESS );
  FD_TEST( ctrl.enable == 1U );
  FD_TEST( !strcmp( ctrl.url, "http://bam.example.com:8899" ) );
  FD_TEST( !strcmp( ctrl.sni, "bam.example.com" ) );
  FD_TEST( ctx->enabled == 1U );
  test_bam_check_endpoint( ctx, "bam.example.com", "bam.example.com", 8899U, 0U );
  FD_TEST( !strcmp( ctrl.error, "" ) );

  test_bam_env_destroy( env );
}

static void
test_bam_ctrl_invalid_url_sets_error_and_preserves_config( fd_wksp_t * wksp ) {
  /* Invalid runtime URLs should surface an error without mutating existing configuration. */
  test_bam_env_t env[1];
  fd_bam_tile_t * ctx = test_bam_env_create( env, wksp );
  fd_bam_ctrl_t ctrl;
  setup_ctrl_defaults( ctx, &ctrl );

  FD_TEST( test_bam_ctrl_request( ctx, FD_BAM_CTRL_CMD_URL, 1U, "not a url" )==FD_BAM_CTRL_STATE_ERROR );
  FD_TEST( strstr( ctrl.error, "Invalid BAM URL" ) != NULL );
  FD_TEST( !strcmp( ctrl.url, "http://testnet.bam.jito.wtf:50055" ) );
  FD_TEST( !strcmp( ctx->server_fqdn, "testnet.bam.jito.wtf" ) );
  FD_TEST( ctx->enabled == 1 );
  FD_TEST( ctrl.applied_enable == 1U );

  test_bam_env_destroy( env );
}

static void
test_bam_ctrl_blank_url_clears_and_disables( fd_wksp_t * wksp ) {
  /* Runtime setBamUrl("") semantics: blank/whitespace clears the URL and
     forces BAM off instead of being treated as a parse error. */
  test_bam_env_t env[1];
  fd_bam_tile_t * ctx = test_bam_env_create( env, wksp );
  fd_bam_ctrl_t ctrl;
  setup_ctrl_defaults( ctx, &ctrl );

  FD_TEST( test_bam_ctrl_request( ctx, FD_BAM_CTRL_CMD_URL, 1U, "   \t\n" )==FD_BAM_CTRL_STATE_SUCCESS );
  FD_TEST( ctrl.enable == 0U );
  FD_TEST( ctrl.applied_enable == 0U );
  FD_TEST( !strcmp( ctrl.url, "" ) );
  FD_TEST( !strcmp( ctrl.sni, "" ) );
  FD_TEST( ctx->enabled == 0 );
  test_bam_check_endpoint( ctx, "", "", 0U, 0U );
  FD_TEST( !strcmp( ctrl.error, "" ) );

  test_bam_env_destroy( env );
}

static void
test_bam_gossip_update_falls_back_to_configured_default_tpu( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  fd_wksp_t * gossip_mem = test_bam_setup_gossip_out( env );
  ulong chunk_before = state->gossip_out.chunk;
  ulong seq_before   = env->stem_seqs[ state->gossip_out.idx ];
  state->bam_gossip_handoff_pending = 1U;

  FD_TEST( !fd_bam_gossip_update( state, state->stem, 0 ) );

  FD_TEST( state->gossip_out.chunk == chunk_before );
  FD_TEST( env->stem_seqs[ state->gossip_out.idx ] == seq_before );
  FD_TEST( state->tpu_update_state == FD_BAM_CONTACT_UPDATE_STATE_PENDING_DEFAULT );
  FD_TEST( state->client_id_update_state == FD_BAM_CONTACT_UPDATE_STATE_PENDING_DEFAULT );
  FD_TEST( !state->bam_gossip_handoff_pending );

  FD_TEST( fd_cstr_to_ip4_addr( "1.1.1.1", &state->configured_default_tpu.addr ) );
  state->configured_default_tpu.port = fd_ushort_bswap( 4242U );
  state->bam_tpu     = state->configured_default_tpu;
  state->bam_tpu_fwd = state->configured_default_tpu;

  ulong publish_chunk = state->gossip_out.chunk;
  FD_TEST( fd_bam_gossip_update( state, state->stem, 0 ) );
  fd_bam_contact_update_t update = test_bam_read_gossip_update( gossip_mem, publish_chunk );

  FD_TEST( update.tpu.l     == state->configured_default_tpu.l );
  FD_TEST( update.tpu_fwd.l == state->configured_default_tpu.l );
  FD_TEST( update.version_client_id == FD_GOSSIP_CONTACT_INFO_CLIENT_FIREDANCER );
  FD_TEST( state->default_tpu.l     == state->configured_default_tpu.l );
  FD_TEST( state->default_tpu_fwd.l == state->configured_default_tpu.l );
  FD_TEST( state->tpu_update_state == FD_BAM_CONTACT_UPDATE_STATE_APPLIED_DEFAULT );
  FD_TEST( state->client_id_update_state == FD_BAM_CONTACT_UPDATE_STATE_APPLIED_DEFAULT );

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
                           int           sign_type ) {
  (void)data; (void)sz; (void)sign_type;
  if( ctx ) (*(ulong *)ctx)++;
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

/* Mirrors the gossip tile's handling of a sign response. */
static void
test_bam_gossip_sign_response( fd_gossip_tile_ctx_t * ctx,
                               uchar const            signature[ 64 ],
                               fd_stem_context_t *    stem,
                               long                   now ) {
  fd_gossip_sign_response( ctx->gossip, signature, stem, now );
  if( FD_UNLIKELY( ctx->bam_contact.ack_pending ) ) fd_gossip_tile_bam_contact_ack( ctx );
}

static void
test_bam_check_ci_sock( fd_gossip_contact_info_t const * ci,
                        ulong                            idx,
                        uint                             ip4,
                        ushort                           port ) {
  FD_TEST( ci->sockets[ idx ].ip4==ip4 && ci->sockets[ idx ].port==fd_ushort_bswap( port ) );
}

/* Joins ctx as a gossip tile with identity 0..31, ContactInfo gossip
   127.0.0.1:8000 (also the entrypoint) and votes on ports 9000/9006, whose
   ContactInfo updates go to env's out link and whose BAM contact acks go
   to signed_ack.  Returns the gossip memory to free and the creation time
   in *now. */
static void *
test_bam_gossip_ctx_init( fd_gossip_tile_ctx_t * ctx,
                          test_bam_env_t *       env,
                          ulong *                signed_ack,
                          void *                 send_ctx,
                          fd_gossip_sign_fn      sign_fn,
                          void *                 sign_ctx,
                          long *                 now ) {
  fd_memset( ctx, 0, sizeof(*ctx) );
  fd_clock_tile_init( ctx->clock );
  *now = fd_clock_tile_now( ctx->clock );
  for( ulong i=0UL; i<FD_GOSSIP_UPDATE_LINK_CNT; i++ ) ctx->update_out[ i ].idx = ULONG_MAX;
  fd_bam_out_ctx_t const out = test_bam_out( env, FD_NET_MTU );
  ctx->update_out[ FD_GOSSIP_UPDATE_LINK_CI_ADDR ] =
      (fd_gossip_out_ctx_t){ .idx = out.idx, .mem = out.mem, .chunk0 = out.chunk0, .chunk = out.chunk, .wmark = out.wmark };
  ctx->bam_contact.signed_fseq = signed_ack;

  for( uchar i=0U; i<32U; i++ ) ctx->identity_key->uc[ i ] = i;
  fd_ip4_port_t const entrypoint = test_bam_ip4_port( "127.0.0.1", 8000U );
  fd_gossip_contact_info_t * ci = ctx->my_contact_info;
  ci->shred_version  = 1U;
  ci->outset         = (ulong)FD_NANOSEC_TO_MICRO( *now );
  ci->version.client = FD_GOSSIP_CONTACT_INFO_CLIENT_FIREDANCER;
  ci->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_GOSSIP        ] = (fd_gossip_socket_t){ .ip4 = entrypoint.addr, .port = entrypoint.port };
  ci->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_VOTE      ] = (fd_gossip_socket_t){ .ip4 = entrypoint.addr, .port = fd_ushort_bswap( 9000 ) };
  ci->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_VOTE_QUIC ] = (fd_gossip_socket_t){ .ip4 = entrypoint.addr, .port = fd_ushort_bswap( 9006 ) };

  void * gossip_mem = aligned_alloc( fd_gossip_align(), fd_gossip_footprint( 128UL, 1UL ) );
  FD_TEST( gossip_mem );
  fd_rng_t * rng = fd_rng_join( fd_rng_new( ctx->rng, ctx->rng_seed, ctx->rng_idx ) );
  FD_TEST( rng );
  void * shgossip = fd_gossip_new( gossip_mem, rng, ctx->ping_seed, 128UL, 1UL, &entrypoint, ctx->identity_key->uc,
                                   ctx->my_contact_info, *now, test_bam_gossip_send_stub, send_ctx, sign_fn, sign_ctx,
                                   test_bam_gossip_ping_change_stub, NULL, test_bam_gossip_activity_update_stub, NULL,
                                   ctx->update_out, ctx->net_out );
  FD_TEST( shgossip );
  ctx->gossip = fd_gossip_join( shgossip );
  FD_TEST( ctx->gossip );
  return gossip_mem;
}

/* Replacing the full contact record from BAM-derived ports corrupts gossip and
   vote sockets.  Update only TPU_QUIC and TPU_FORWARDS_QUIC. */
static void
test_bam_gossip_tile_applies_contact_update( fd_wksp_t * wksp ) {
  static fd_gossip_tile_ctx_t ctx; /* too large for the stack */
  FD_TEST( FD_TPU_PARSED_MTU >= FD_NET_MTU );
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  ulong send_cnt = 0UL;
  ulong sign_cnt = 0UL;
  ulong signed_ack[1] = { 0UL };
  long test_now;
  void * gossip_mem = test_bam_gossip_ctx_init( &ctx, env, signed_ack, &send_cnt, test_bam_gossip_sign_stub, &sign_cnt, &test_now );
  fd_ip4_port_t const default_gossip = test_bam_ip4_port( "127.0.0.1", 8000U );

  uchar new_identity[ 32UL ];
  for( uchar i=0U; i<32U; i++ ) new_identity[ i ] = (uchar)(32U-i);
  ulong identity_outset_nanos = (ulong)test_now + 1000000UL;
  ulong identity_outset       = (ulong)FD_NANOSEC_TO_MICRO( identity_outset_nanos );
  fd_keyswitch_t keyswitch = { .param = identity_outset_nanos };
  fd_memcpy( keyswitch.bytes, new_identity, sizeof(new_identity) );
  ctx.keyswitch               = &keyswitch;
  ctx.is_halting_signing      = 1;
  ctx.is_pending_set_identity = 1;
  int opt_poll_in = 1;
  int charge_busy = 0;
  after_credit( &ctx, env->stem, &opt_poll_in, &charge_busy );
  FD_TEST( charge_busy );
  FD_TEST( !ctx.is_halting_signing );
  FD_TEST( !ctx.is_pending_set_identity );
  FD_TEST( fd_keyswitch_state_query( &keyswitch )==FD_KEYSWITCH_STATE_COMPLETED );
  FD_TEST( !memcmp( ctx.identity_key->uc, new_identity, sizeof(new_identity) ) );
  FD_TEST( ctx.my_contact_info->outset == identity_outset );

  fd_gossip_contact_info_t const * ci = ctx.my_contact_info;
  fd_bam_contact_update_t const bam_contact = {
      .tpu               = test_bam_ip4_port( "9.8.7.6", 5000U ),
      .tpu_fwd           = test_bam_ip4_port( "4.3.2.1", 6000U ),
      .version_client_id = FD_GOSSIP_CONTACT_INFO_CLIENT_FIRE_BAM,
  };
  uint const bam_tpu     = bam_contact.tpu.addr;
  uint const bam_tpu_fwd = bam_contact.tpu_fwd.addr;
  send_cnt = 0UL;
  FD_TEST( fd_gossip_tile_apply_bam_contact( &ctx, &bam_contact, 0UL, test_now + 1L, env->stem ) );
  FD_TEST( sign_cnt>0UL );
  FD_TEST( fd_fseq_query( ctx.bam_contact.signed_fseq )==0UL );

  test_bam_check_ci_sock( ci, FD_GOSSIP_CONTACT_INFO_SOCKET_TPU,                bam_tpu,             5000U );
  test_bam_check_ci_sock( ci, FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_VOTE,           default_gossip.addr, 9000U );
  test_bam_check_ci_sock( ci, FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_FORWARDS,       bam_tpu_fwd,         6000U );
  test_bam_check_ci_sock( ci, FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_QUIC,           bam_tpu,             5006U );
  test_bam_check_ci_sock( ci, FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_VOTE_QUIC,      default_gossip.addr, 9006U );
  test_bam_check_ci_sock( ci, FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_FORWARDS_QUIC,  bam_tpu_fwd,         6006U );
  FD_TEST( ci->version.client == FD_GOSSIP_CONTACT_INFO_CLIENT_FIRE_BAM );
  FD_TEST( ci->outset == identity_outset );

  fd_bam_contact_update_t contact_update = {
      .tpu               = test_bam_ip4_port( "1.2.3.4", 7000U ),
      .tpu_fwd           = test_bam_ip4_port( "5.6.7.8", 8000U ),
      .version_client_id = FD_GOSSIP_CONTACT_INFO_CLIENT_FIREDANCER,
  };
  uint const default_tpu = contact_update.tpu.addr;
  FD_TEST( fd_gossip_tile_apply_bam_contact( &ctx, &contact_update, 1UL, test_now + 2000000L, env->stem ) );
  FD_TEST( fd_fseq_query( ctx.bam_contact.signed_fseq )==0UL );

  uchar signature[64];
  for( uchar i=0U; i<64U; i++ ) signature[i] = i;
  while( fd_gossip_sign_pend_cnt( ctx.gossip ) ) {
    signature[0]++; /* distinct leading bytes, see test_bam_gossip_fake_signature */
    test_bam_gossip_sign_response( &ctx, signature, env->stem, test_now + 3000000L );
    FD_TEST( fd_fseq_query( ctx.bam_contact.signed_fseq )!=1UL ); /* stale BAM contact cannot ack */
  }
  FD_TEST( fd_fseq_query( ctx.bam_contact.signed_fseq )==2UL );
  fd_gossip_advance( ctx.gossip, test_now + 3000000L, env->stem, NULL );
  FD_TEST( send_cnt>0UL ); /* the next advance sends the signed contact */

  test_bam_check_ci_sock( ci, FD_GOSSIP_CONTACT_INFO_SOCKET_TPU,           default_tpu,         7000U );
  test_bam_check_ci_sock( ci, FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_VOTE,      default_gossip.addr, 9000U );
  test_bam_check_ci_sock( ci, FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_QUIC,      default_tpu,         7006U );
  test_bam_check_ci_sock( ci, FD_GOSSIP_CONTACT_INFO_SOCKET_TPU_VOTE_QUIC, default_gossip.addr, 9006U );
  FD_TEST( ci->version.client == FD_GOSSIP_CONTACT_INFO_CLIENT_FIREDANCER );
  FD_TEST( ci->outset == identity_outset );

  /* An identity halt can drain a signed contact without letting BAM take
     ownership under the old key. The refreshed contact must sign again. */
  FD_TEST( fd_gossip_tile_apply_bam_contact( &ctx, &bam_contact, 2UL, test_now + 4000000L, env->stem ) );
  ctx.is_halting_signing = 1;
  while( fd_gossip_sign_pend_cnt( ctx.gossip ) ) {
    signature[0]++;
    test_bam_gossip_sign_response( &ctx, signature, env->stem, test_now + 5000000L );
  }
  FD_TEST( fd_fseq_query( ctx.bam_contact.signed_fseq )==2UL );
  FD_TEST( ctx.bam_contact.ack_pending );
  ctx.is_halting_signing = 0;
  fd_gossip_set_my_contact_info( ctx.gossip, ctx.my_contact_info, test_now + 6000000L );
  fd_gossip_advance( ctx.gossip, test_now + 6000000L, env->stem, NULL );
  while( fd_gossip_sign_pend_cnt( ctx.gossip ) ) {
    signature[0]++;
    test_bam_gossip_sign_response( &ctx, signature, env->stem, test_now + 7000000L );
  }
  FD_TEST( fd_fseq_query( ctx.bam_contact.signed_fseq )==3UL );
  FD_TEST( !ctx.bam_contact.ack_pending );

  free( gossip_mem );
  test_bam_env_destroy( env );
}

/* Fake signatures fed to the gossip sign response path must differ in
   their first 8 bytes: fd_crds_insert takes a value whose leading
   signature bytes match the installed one as a duplicate of it, which
   would pass for an install. */

static void
test_bam_gossip_fake_signature( uchar signature[ 64 ],
                                ulong k ) {
  fd_memset( signature, 0, 64UL );
  FD_STORE( ulong, signature, k );
}

typedef struct {
  ulong cnt;
  ulong signable_sz;
  uchar signable[ FD_GOSSIP_VALUE_MAX_SZ ];
} test_bam_gossip_signer_t;

static void
test_bam_gossip_sign_record_stub( void *        ctx,
                                  uchar const * data,
                                  ulong         sz,
                                  int           sign_type ) {
  (void)sign_type;
  test_bam_gossip_signer_t * signer = (test_bam_gossip_signer_t *)ctx;
  FD_TEST( sz<=sizeof(signer->signable) );
  signer->cnt++;
  signer->signable_sz = sz;
  fd_memcpy( signer->signable, data, sz );
}

/* fd_gossip_advance also signs a pull-request-only copy of our
   ContactInfo every 100 ms (never inserted in CRDS), queued before a
   contact refresh.  Answers those so only the contact's own signing
   request is pending, and returns how many it answered. */
static ulong
test_bam_gossip_answer_pull_signs( fd_gossip_tile_ctx_t * ctx,
                                   fd_stem_context_t *    stem,
                                   ulong *                sig_k,
                                   long                   now ) {
  ulong cnt = 0UL;
  while( fd_gossip_sign_pend_cnt( ctx->gossip )>1UL ) {
    uchar signature[ 64 ];
    test_bam_gossip_fake_signature( signature, ++*sig_k );
    test_bam_gossip_sign_response( ctx, signature, stem, now );
    cnt++;
  }
  FD_TEST( fd_gossip_sign_pend_cnt( ctx->gossip )==1UL );
  return cnt;
}

/* CRDS hash of the last signing request completed with signature. */
static void
test_bam_gossip_value_hash( test_bam_gossip_signer_t const * signer,
                            uchar const                      signature[ 64 ],
                            uchar                            hash[ 32 ] ) {
  uchar value[ 64UL+FD_GOSSIP_VALUE_MAX_SZ ];
  fd_memcpy( value,      signature,        64UL                );
  fd_memcpy( value+64UL, signer->signable, signer->signable_sz );
  fd_sha256_hash( value, 64UL+signer->signable_sz, hash );
}

/* Counts changed ContactInfo updates for our identity published on
   gossip_ciaddr since seq0.  Unchanged refreshes go to gossip_ciseen.
   *last is the newest changed contact. */
static ulong
test_bam_gossip_own_contact_cnt( test_bam_env_t *                  env,
                                 fd_gossip_tile_ctx_t const *      ctx,
                                 ulong                             seq0,
                                 fd_gossip_contact_info_t const ** last ) {
  ulong cnt = 0UL;
  for( ulong seq=seq0; seq<env->stem_seqs[ 0 ]; seq++ ) {
    fd_frag_meta_t const * meta = env->out_mcache + fd_mcache_line_idx( seq, env->stem_depths[ 0 ] );
    FD_TEST( meta->seq==seq );
    if( meta->sig!=FD_GOSSIP_UPDATE_TAG_CONTACT_INFO ) continue;
    fd_gossip_update_message_t const * msg = fd_chunk_to_laddr_const( ctx->update_out[ FD_GOSSIP_UPDATE_LINK_CI_ADDR ].mem, meta->chunk );
    if( memcmp( msg->origin, ctx->identity_key->uc, 32UL ) ) continue;
    cnt++;
    *last = msg->contact_info->value;
  }
  return cnt;
}

/* A signed contact that loses the local CRDS insert must not ack BAM,
   and Gossip must not re-sign it on every advance.  The periodic
   refresh signs it again with a newer wallclock, which installs it and
   acks.  The contact loses either because our clock stepped back below
   the installed contact's wallclock or, within the same millisecond,
   on the hash tiebreak. */
static void
test_bam_gossip_stale_contact_case( fd_wksp_t * wksp,
                                    int         tie ) {
  static fd_gossip_tile_ctx_t ctx; /* too large for the stack */
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  static test_bam_gossip_signer_t signer[1];
  fd_memset( signer, 0, sizeof(signer) );
  ulong signed_ack[1] = { 0UL };
  long test_now;
  void * gossip_mem = test_bam_gossip_ctx_init( &ctx, env, signed_ack, NULL, test_bam_gossip_sign_record_stub, signer, &test_now );

  fd_bam_contact_update_t contact_update = {
      .tpu               = test_bam_ip4_port( "9.8.7.6", 5000U ),
      .tpu_fwd           = test_bam_ip4_port( "9.8.7.6", 6000U ),
      .version_client_id = FD_GOSSIP_CONTACT_INFO_CLIENT_FIRE_BAM,
  };
  uchar signature[ 64 ];
  ulong sig_k = 0UL;
  ulong pulls = 0UL;
  fd_gossip_contact_info_t const * published = NULL;

  /* Both contacts are signed within one millisecond for the tie, the
     second one 10ms earlier otherwise. */
  long ms0           = ( test_now/1000000L + 20L )*1000000L;
  long installed_now = tie ? ms0 + 100000L : ms0 + 10000000L;
  long stale_now     = tie ? ms0 + 200000L : ms0;

  ulong seq0 = env->stem_seqs[ 0 ];
  FD_TEST( fd_gossip_tile_apply_bam_contact( &ctx, &contact_update, 0UL, installed_now, env->stem ) );
  FD_TEST( signer->cnt==1UL );
  test_bam_gossip_fake_signature( signature, ++sig_k );
  uchar installed_hash[ 32 ];
  test_bam_gossip_value_hash( signer, signature, installed_hash );
  test_bam_gossip_sign_response( &ctx, signature, env->stem, installed_now );
  FD_TEST( fd_fseq_query( ctx.bam_contact.signed_fseq )==1UL );
  FD_TEST( test_bam_gossip_own_contact_cnt( env, &ctx, seq0, &published )==1UL );
  FD_TEST( published->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU ].port==fd_ushort_bswap( 5000 ) );

  contact_update.tpu.port = fd_ushort_bswap( 5100 );
  seq0 = env->stem_seqs[ 0 ];
  FD_TEST( fd_gossip_tile_apply_bam_contact( &ctx, &contact_update, 1UL, stale_now, env->stem ) );
  pulls += test_bam_gossip_answer_pull_signs( &ctx, env->stem, &sig_k, stale_now );
  FD_TEST( signer->cnt==2UL+pulls );
  for(;;) {
    test_bam_gossip_fake_signature( signature, ++sig_k );
    if( !tie ) break;
    uchar hash[ 32 ];
    test_bam_gossip_value_hash( signer, signature, hash );
    if( memcmp( hash, installed_hash, 32UL )<0 ) break; /* loses the tiebreak */
  }
  test_bam_gossip_sign_response( &ctx, signature, env->stem, stale_now );
  FD_TEST( !test_bam_gossip_own_contact_cnt( env, &ctx, seq0, &published ) );
  FD_TEST( fd_fseq_query( ctx.bam_contact.signed_fseq )==1UL );
  FD_TEST( ctx.bam_contact.ack_pending );

  /* The tile advances Gossip on every after_credit.  None of these may
     re-sign the contact. */
  for( long i=0L; i<16L; i++ ) {
    long now = stale_now + i*100000L;
    fd_gossip_advance( ctx.gossip, now, env->stem, NULL );
    while( fd_gossip_sign_pend_cnt( ctx.gossip ) ) {
      test_bam_gossip_fake_signature( signature, ++sig_k );
      test_bam_gossip_sign_response( &ctx, signature, env->stem, now );
    }
  }
  FD_TEST( signer->cnt==2UL+pulls );
  FD_TEST( !test_bam_gossip_own_contact_cnt( env, &ctx, seq0, &published ) );
  FD_TEST( fd_fseq_query( ctx.bam_contact.signed_fseq )==1UL );
  FD_TEST( ctx.bam_contact.ack_pending );

  /* The periodic refresh fires 7.5s after the first advance. */
  long refresh_now = installed_now + 15L*500L*1000L*1000L;
  fd_gossip_advance( ctx.gossip, refresh_now, env->stem, NULL );
  pulls += test_bam_gossip_answer_pull_signs( &ctx, env->stem, &sig_k, refresh_now );
  FD_TEST( signer->cnt==3UL+pulls );
  test_bam_gossip_fake_signature( signature, ++sig_k );
  test_bam_gossip_sign_response( &ctx, signature, env->stem, refresh_now );
  FD_TEST( test_bam_gossip_own_contact_cnt( env, &ctx, seq0, &published )==1UL );
  FD_TEST( published->sockets[ FD_GOSSIP_CONTACT_INFO_SOCKET_TPU ].port==fd_ushort_bswap( 5100 ) );
  FD_TEST( fd_fseq_query( ctx.bam_contact.signed_fseq )==2UL );
  FD_TEST( !ctx.bam_contact.ack_pending );

  /* Switch to another identity and back.  CRDS still holds the contact
     signed under the first outset, which advertises the same BAM
     contact.  It cannot ack the update published after the switch, e.g.
     on the sign response of an unrelated message.  Only the contact
     signed under the new outset can. */
  fd_keyswitch_t keyswitch = {0};
  ctx.keyswitch = &keyswitch;
  long now = refresh_now;
  for( ulong j=0UL; j<2UL; j++ ) {
    now += 1000000L;
    keyswitch.param = (ulong)now;
    for( uchar i=0U; i<32U; i++ ) keyswitch.bytes[ i ] = j ? i : (uchar)(32U-i);
    ctx.is_halting_signing      = 1;
    ctx.is_pending_set_identity = 1;
    int opt_poll_in = 1;
    int charge_busy = 0;
    after_credit( &ctx, env->stem, &opt_poll_in, &charge_busy );
    FD_TEST( ctx.my_contact_info->outset==(ulong)FD_NANOSEC_TO_MICRO( now ) );
  }
  FD_TEST( fd_gossip_tile_apply_bam_contact( &ctx, &contact_update, 2UL, now, env->stem ) );
  pulls += test_bam_gossip_answer_pull_signs( &ctx, env->stem, &sig_k, now );
  FD_TEST( signer->cnt==4UL+pulls );
  fd_gossip_tile_bam_contact_ack( &ctx );
  FD_TEST( fd_fseq_query( ctx.bam_contact.signed_fseq )==2UL );
  test_bam_gossip_fake_signature( signature, ++sig_k );
  test_bam_gossip_sign_response( &ctx, signature, env->stem, now );
  FD_TEST( fd_fseq_query( ctx.bam_contact.signed_fseq )==3UL );

  /* An unchanged signed refresh is suppressed on gossip_ciaddr.  BAM
     must still acknowledge the signed CRDS value with no GUI reader. */
  now += 1000000L;
  seq0 = env->stem_seqs[ 0 ];
  FD_TEST( fd_gossip_tile_apply_bam_contact( &ctx, &contact_update, 3UL, now, env->stem ) );
  FD_TEST( ctx.bam_contact.ack_pending );
  test_bam_gossip_fake_signature( signature, ++sig_k );
  test_bam_gossip_sign_response( &ctx, signature, env->stem, now );
  FD_TEST( !test_bam_gossip_own_contact_cnt( env, &ctx, seq0, &published ) );
  FD_TEST( fd_fseq_query( ctx.bam_contact.signed_fseq )==4UL );
  FD_TEST( !ctx.bam_contact.ack_pending );

  free( gossip_mem );
  test_bam_env_destroy( env );
}

static void
test_bam_gossip_publishes_bam_config_contact( fd_wksp_t * wksp ) {
  /* A healthy BAM tile should advertise the TPU endpoints supplied by BamConfig. */
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );

  fd_wksp_t * gossip_mem = test_bam_setup_gossip_out( env );
  state->bam_status_recent = FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY;

  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  test_bam_config_set_tpu( &resp, "10.20.30.40", 1122U, "11.12.13.14", 3344U );
  ulong publish_chunk = state->gossip_out.chunk;
  test_bam_apply_config_resp( state, &resp );
  fd_bam_contact_update_t update = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  test_bam_check_contact( &update, test_bam_ip4_port( "10.20.30.40", 1122U ), test_bam_ip4_port( "11.12.13.14", 3344U ),
                          FD_GOSSIP_CONTACT_INFO_CLIENT_FIRE_BAM );

  test_bam_env_destroy( env );
}

static void
test_bam_activation_waits_for_gossip_ack( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
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

  fd_fseq_update( fseqs->signed_ack, 1UL );
  fd_bam_publish_active_state( state, state->stem, 1 );
  FD_TEST( env->stem_seqs[0] == 1UL );
  FD_TEST( !state->bam_gossip_handoff_pending );
  FD_TEST( fd_fseq_query( fseqs->status ) == FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );

  test_bam_env_destroy( env );
}

/* Efficient layout: an idle pack and bundle are parked and read bam_gen
   and bam_status only when woken.  BAM rings pack until it acknowledges
   a generation, and both on each override change. */

static void
test_bam_expect_rings( fd_sleep_t * sleep,
                       ulong        w0,
                       ulong        w1 ) {
  FD_TEST( __atomic_exchange_n( &sleep->doorbell[ 0 ], 0UL, __ATOMIC_ACQUIRE )==w0 );
  FD_TEST( __atomic_exchange_n( &sleep->doorbell[ 1 ], 0UL, __ATOMIC_ACQUIRE )==w1 );
}

static void
test_bam_handoff_rings_parked_readers( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  void * sleep_mem = fd_wksp_alloc_laddr( wksp, fd_sleep_align(), fd_sleep_footprint(), 1UL );
  fd_sleep_t * sleep = fd_sleep_join( fd_sleep_new( sleep_mem, fd_tempo_tick_per_ns( NULL ) ) );
  FD_TEST( sleep );
  env->stem->sleep      = sleep;
  state->pack_tile_id   = 3UL;
  state->bundle_tile_id = 70UL; /* another doorbell word */
  ulong const pack = 1UL<<3, bundle = 1UL<<6;

  uchar status_mem[ FD_FSEQ_FOOTPRINT ] __attribute__((aligned(FD_FSEQ_ALIGN)));
  uchar gen_mem   [ FD_FSEQ_FOOTPRINT ] __attribute__((aligned(FD_FSEQ_ALIGN)));
  ulong * status = fd_fseq_join( fd_fseq_new( status_mem, 0UL ) );
  ulong * gen    = fd_fseq_join( fd_fseq_new( gen_mem, (ulong)state->ownership_gen<<1 ) );
  FD_TEST( status && gen );
  state->bam_status_fseq = status;
  state->bam_gen_fseq    = gen;

  fd_bam_publish_active_state( state, NULL, 0 ); /* the first housekeeping runs before the stem is cached */
  fd_bam_publish_active_state( state, state->stem, 1 );
  FD_TEST( fd_fseq_query( status )==FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
  test_bam_expect_rings( sleep, pack, bundle );
  fd_bam_publish_active_state( state, state->stem, 1 );
  test_bam_expect_rings( sleep, 0UL, 0UL );

  /* The override is held until pack acknowledges, which every refresh
     asks for again */
  for( ulong i=0UL; i<2UL; i++ ) {
    fd_bam_publish_active_state( state, state->stem, 0 );
    FD_TEST( fd_fseq_query( status )==FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE && ( fd_fseq_query( gen ) & 1UL ) );
    test_bam_expect_rings( sleep, pack, 0UL );
  }
  fd_bam_publish_active_state( state, NULL, 0 ); /* likewise with a generation pending: set-bam or set-identity at boot */
  fd_fseq_update( gen, fd_fseq_query( gen ) & ~1UL );
  fd_bam_publish_active_state( state, state->stem, 0 );
  FD_TEST( fd_fseq_query( status )==0UL );
  test_bam_expect_rings( sleep, pack, bundle );
  fd_bam_publish_active_state( state, state->stem, 0 );
  test_bam_expect_rings( sleep, 0UL, 0UL );

  /* Without a bundle tile only pack is rung */
  state->bundle_tile_id = ULONG_MAX;
  fd_bam_publish_active_state( state, state->stem, 1 );
  test_bam_expect_rings( sleep, pack, 0UL );

  env->stem->sleep       = NULL;
  state->bam_status_fseq = NULL;
  state->bam_gen_fseq    = NULL;
  FD_TEST( fd_fseq_delete( fd_fseq_leave( status ) )==status_mem );
  FD_TEST( fd_fseq_delete( fd_fseq_leave( gen    ) )==gen_mem    );
  fd_wksp_free_laddr( sleep_mem );
  test_bam_env_destroy( env );
}

/* Housekeeping drives the handoff: while the override lags the client's
   health, the tile parks no longer than one housekeeping interval. */

static void
test_bam_handoff_keeps_housekeeping_cadence( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );
  fd_bam_tile_t * state = env->state;

  uchar status_mem[ FD_FSEQ_FOOTPRINT ] __attribute__((aligned(FD_FSEQ_ALIGN)));
  ulong * status = fd_fseq_join( fd_fseq_new( status_mem, 0UL ) );
  FD_TEST( status );
  state->bam_status_fseq    = status;
  state->next_step_deadline = LONG_MAX;
  long const hk = fd_clock_tile_wallclock_to_tickcount( state->clock, g_clock+(long)10e6 );
  FD_TEST( fd_bam_test_next_deadline( state )==LONG_MAX );

  /* Disconnected: the release is pending, also while a keyswitch halts
     signing */
  fd_fseq_update( status, FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
  FD_TEST( fd_bam_test_next_deadline( state )==hk );
  state->halt_signing = 1U;
  FD_TEST( fd_bam_test_next_deadline( state )==hk );
  state->halt_signing = 0U;

  /* Healthy: the activation is pending until the override is set, also
     while queued transactions pause the waker */
  test_bam_env_mock_conn( env );
  fd_fseq_update( status, 0UL );
  state->next_step_deadline = LONG_MAX;
  FD_TEST( fd_bam_test_next_deadline( state )==hk );
  state->waker_paused = 2U;
  FD_TEST( fd_bam_test_next_deadline( state )==hk );
  state->waker_paused = 0U;
  state->next_step_deadline = g_clock+1L;
  FD_TEST( fd_bam_test_next_deadline( state )==fd_clock_tile_wallclock_to_tickcount( state->clock, g_clock+1L ) );
  fd_fseq_update( status, FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
  state->next_step_deadline = LONG_MAX;
  FD_TEST( fd_bam_test_next_deadline( state )==LONG_MAX );

  state->bam_status_fseq = NULL;
  FD_TEST( fd_fseq_delete( fd_fseq_leave( status ) )==status_mem );
  test_bam_env_destroy( env );
}

static void
test_bam_activation_ignores_stale_gossip_ack_after_reenable( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
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

  fd_fseq_update( fseqs->signed_ack, 1UL );
  fd_bam_publish_active_state( state, state->stem, 1 );
  FD_TEST( fd_fseq_query( fseqs->status ) == 0UL );
  fd_fseq_update( fseqs->signed_ack, 3UL );
  fd_bam_publish_active_state( state, state->stem, 1 );
  FD_TEST( fd_fseq_query( fseqs->status ) == FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );

  test_bam_env_destroy( env );
}

static void
test_bam_disable_clears_status_with_pending_gossip_handoff( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
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

  test_bam_env_destroy( env );
}

/* A BAM contact is advertised as soon as it is published, before
   activation completes (here activation waits for gossip's signed ack).
   Whatever ends that attempt must still restore the default contact:
   a set-bam disable or URL change (both start a new ownership
   generation, as an identity switch does), also while an earlier
   default restore is still pending.  A ConfigResponse that changes the
   BAM TPU while unhealthy is a guard. */

#define TEST_BAM_CONTACT_TRIGGER_DISABLE         (0)
#define TEST_BAM_CONTACT_TRIGGER_URL             (1)
#define TEST_BAM_CONTACT_TRIGGER_PENDING_DEFAULT (2)
#define TEST_BAM_CONTACT_TRIGGER_CONFIG          (3)
#define TEST_BAM_CONTACT_TRIGGER_CNT             (4)

static void
test_bam_pending_activation_restores_default_contact( fd_wksp_t * wksp,
                                                      int         trigger ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  fd_wksp_t * gossip_mem = test_bam_setup_gossip_out( env );
  test_bam_set_bam_tpu( state, "10.5.1.1", 5000U, "10.5.1.2", 6000U );
  FD_TEST( fd_cstr_to_ip4_addr( "10.5.1.3", &state->configured_default_tpu.addr ) );
  state->configured_default_tpu.port = fd_ushort_bswap( 7000U );

  fd_bam_ctrl_t ctrl;
  setup_ctrl_defaults( state, &ctrl );
  test_bam_handoff_fseqs_t fseqs[1];
  test_bam_setup_handoff_fseqs( env, fseqs );
  ulong generation = (ulong)state->ownership_gen<<1;
  state->bam_gen_fseq = &generation;

  fd_bam_publish_active_state( state, state->stem, 1 );
  FD_TEST( env->stem_seqs[0]==1UL );
  FD_TEST( state->tpu_update_state==FD_BAM_CONTACT_UPDATE_STATE_APPLIED_BAM );
  fd_bam_publish_active_state( state, state->stem, 1 );
  FD_TEST( fd_fseq_query( fseqs->status )==0UL );

  ulong publish_chunk = state->gossip_out.chunk;
  if( trigger==TEST_BAM_CONTACT_TRIGGER_CONFIG ) {
    /* The connection is not healthy in this harness. */
    test_bam_env_inject_config_response( state );
  } else {
    if( trigger==TEST_BAM_CONTACT_TRIGGER_PENDING_DEFAULT ) {
      /* The connection drops, but gossip_out has no credit for the default. */
      env->stem->cr_decrement_amount = 1UL;
      env->stem_cr_avail[0]          = 0UL;
      fd_bam_publish_active_state( state, state->stem, 0 );
      FD_TEST( state->tpu_update_state==FD_BAM_CONTACT_UPDATE_STATE_PENDING_DEFAULT );
      env->stem->cr_decrement_amount = 0UL;
      env->stem_cr_avail[0]          = ULONG_MAX;
    }
    int const url = trigger==TEST_BAM_CONTACT_TRIGGER_URL;
    FD_TEST( test_bam_ctrl_request( state, url ? FD_BAM_CTRL_CMD_URL : FD_BAM_CTRL_CMD_ENABLE, (uchar)url,
                                    url ? "http://bam.example.com" : NULL )==FD_BAM_CTRL_STATE_SUCCESS );
    FD_TEST( generation & 1UL );
    FD_TEST( env->stem_seqs[0]==1UL );

    /* Pack acknowledges the retirement. */
    generation &= ~1UL;
    fd_bam_tile_housekeeping( state );
  }
  FD_TEST( env->stem_seqs[0]==2UL );
  fd_bam_contact_update_t update = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  FD_TEST( update.tpu.l==state->configured_default_tpu.l );
  FD_TEST( update.version_client_id==FD_GOSSIP_CONTACT_INFO_CLIENT_FIREDANCER );
  FD_TEST( state->tpu_update_state==FD_BAM_CONTACT_UPDATE_STATE_APPLIED_DEFAULT );
  FD_TEST( fd_fseq_query( fseqs->status )==0UL );

  test_bam_env_destroy( env );
}

static void
test_bam_invalid_quic_base_port_does_not_activate( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
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
  FD_TEST( state->tpu_update_state == FD_BAM_CONTACT_UPDATE_STATE_PENDING_BAM );
  FD_TEST( state->client_id_update_state == FD_BAM_CONTACT_UPDATE_STATE_PENDING_BAM );

  fd_bam_publish_active_state( state, state->stem, 1 );
  FD_TEST( env->stem_seqs[0] == 0UL );
  FD_TEST( fd_fseq_query( fseqs->status ) == 0UL );

  test_bam_env_destroy( env );
}

static void
test_bam_gossip_uses_defaults_when_contact_missing( fd_wksp_t * wksp ) {
  /* An incomplete BamConfig without a cached BAM contact should force gossip
     back to defaults without blocking scheduler activation. */
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn_empty( env );
  test_bam_env_mock_h2_hs( state );

  fd_wksp_t * gossip_mem = test_bam_setup_gossip_out( env );
  test_bam_set_default_tpu( state );

  state->tpu_update_state       = FD_BAM_CONTACT_UPDATE_STATE_APPLIED_BAM;
  state->client_id_update_state = FD_BAM_CONTACT_UPDATE_STATE_APPLIED_BAM;
  state->bam_stream_live        = 1U;
  state->bam_config_received    = 1U;
  state->builder_pubkey[ 0 ]    = 1U; /* a valid builder from an earlier config */
  state->bam_last_builder_activity_ns = fd_bam_now();
  state->bam_builder_heartbeat_received = 1U;
  state->bam_last_validator_heartbeat_ns = fd_bam_now();

  ulong fseq[1] = { 0UL };
  state->bam_status_fseq = fseq;

  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  test_bam_config_set_tpu( &resp, "12.34.56.78", 2222U, NULL, 0U );
  ulong publish_chunk = state->gossip_out.chunk;
  test_bam_apply_config_resp( state, &resp );

  FD_TEST( state->bam_tpu.l == 0UL );
  FD_TEST( state->bam_tpu_fwd.l == 0UL );
  FD_TEST( fd_fseq_query( fseq ) == FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );

  fd_bam_contact_update_t update = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  test_bam_check_contact( &update, state->default_tpu, state->default_tpu_fwd, FD_GOSSIP_CONTACT_INFO_CLIENT_FIREDANCER );

  test_bam_env_destroy( env );
}

static void
test_bam_gossip_disconnect_uses_defaults_without_clearing_stored_contact( fd_wksp_t * wksp ) {
  /* Incomplete configs while disconnected should fall back to default gossip
     without clearing the last valid BAM contact. */
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  test_bam_env_mock_conn( env );
  fd_wksp_t * gossip_mem = test_bam_setup_gossip_out( env );
  state->bam_status_recent = FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY;
  test_bam_set_default_tpu( state );

  fd_ip4_port_t const bam_tpu     = test_bam_ip4_port( "9.8.7.6", 5000U );
  fd_ip4_port_t const bam_tpu_fwd = test_bam_ip4_port( "4.3.2.1", 6000U );
  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  test_bam_config_set_tpu( &resp, "9.8.7.6", 5000U, "4.3.2.1", 6000U );
  ulong publish_chunk = state->gossip_out.chunk;
  test_bam_apply_config_resp( state, &resp );
  fd_bam_contact_update_t update = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  test_bam_check_contact( &update, bam_tpu, bam_tpu_fwd, FD_GOSSIP_CONTACT_INFO_CLIENT_FIRE_BAM );

  /* Disconnected path: publish default contact so gossip falls back to Firedancer TPU. */
  publish_chunk = state->gossip_out.chunk;
  fd_bam_gossip_update( state, state->stem, false );
  update = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  test_bam_check_contact( &update, state->default_tpu, state->default_tpu_fwd, FD_GOSSIP_CONTACT_INFO_CLIENT_FIREDANCER );

  state->bam_status_recent = FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTING;
  state->bam_stream_live = 0U;
  state->tpu_update_state = FD_BAM_CONTACT_UPDATE_STATE_APPLIED_BAM;

  resp = (bam_api_ConfigResponse)bam_api_ConfigResponse_init_default;
  test_bam_config_set_tpu( &resp, NULL, 0U, NULL, 0U );
  publish_chunk = state->gossip_out.chunk;
  test_bam_apply_config_resp( state, &resp );
  FD_TEST( state->bam_tpu.l==bam_tpu.l && state->bam_tpu_fwd.l==bam_tpu_fwd.l );
  update = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  test_bam_check_contact( &update, state->default_tpu, state->default_tpu_fwd, FD_GOSSIP_CONTACT_INFO_CLIENT_FIREDANCER );

  /* Explicit use_bam=false still advertises defaults and does not depend on
     the stored BAM contact being zeroed. */
  publish_chunk = state->gossip_out.chunk;
  fd_bam_gossip_update( state, state->stem, false );
  update = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  test_bam_check_contact( &update, state->default_tpu, state->default_tpu_fwd, FD_GOSSIP_CONTACT_INFO_CLIENT_FIREDANCER );

  test_bam_env_destroy( env );
}

static void
test_bam_runtime_toggle_updates_gossip( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );

  fd_wksp_t * gossip_mem = test_bam_setup_gossip_out( env );
  state->bam_status_recent = FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY;
  state->configured_default_tpu = test_bam_ip4_port( "1.1.1.1", 4242U );
  fd_ip4_port_t const bam_tpu     = test_bam_ip4_port( "9.9.9.9", 7000U );
  fd_ip4_port_t const bam_tpu_fwd = test_bam_ip4_port( "8.8.8.8", 7001U );

  /* Initial BamConfig should publish the override while runtime is enabled. */
  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  test_bam_config_set_tpu( &resp, "9.9.9.9", 7000U, "8.8.8.8", 7001U );
  ulong publish_chunk = state->gossip_out.chunk;
  test_bam_apply_config_resp( state, &resp );
  fd_bam_contact_update_t update = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  test_bam_check_contact( &update, bam_tpu, bam_tpu_fwd, FD_GOSSIP_CONTACT_INFO_CLIENT_FIRE_BAM );

  /* Disabling runtime should immediately revert gossip to the Firedancer defaults. */
  publish_chunk = state->gossip_out.chunk;
  state->enabled = 0;
  fd_bam_gossip_update( state, state->stem, false );
  update = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  test_bam_check_contact( &update, state->configured_default_tpu, state->configured_default_tpu,
                          FD_GOSSIP_CONTACT_INFO_CLIENT_FIREDANCER );

  /* Re-enabling runtime while still connected should republish the BamConfig address. */
  publish_chunk = state->gossip_out.chunk;
  state->enabled = 1;
  fd_bam_gossip_update( state, state->stem, true );
  update = test_bam_read_gossip_update( gossip_mem, publish_chunk );
  test_bam_check_contact( &update, bam_tpu, bam_tpu_fwd, FD_GOSSIP_CONTACT_INFO_CLIENT_FIRE_BAM );

  test_bam_env_destroy( env );
}

/* Passing IPv6 or malformed receivers into the IPv4-only shred path can abort
   transmission.  Drop them during config parsing and retain valid IPv4 entries. */
static void
test_bam_shred_update_publishes_receiver_list( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );

  state->shred_out = test_bam_out( env, sizeof(fd_bam_shred_update_t) );
  fd_wksp_t * shred_mem = state->shred_out.mem;

  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  test_bam_config_set_tpu( &resp, "10.20.30.40", 1122U, "11.12.13.14", 3344U );
  char const * shred_ip  [ 5 ] = { "1.1.1.1", "2.2.2.2", "1.1.1.1", "::1", "bad-ip" };
  uint const   shred_port[ 5 ] = { 5001U,     5002U,     5001U,     5003U, 0U       };
  resp.bam_config.shred_socks_count = 5U;
  for( ulong i=0UL; i<5UL; i++ ) {
    fd_cstr_ncpy( resp.bam_config.shred_socks[ i ].ip, shred_ip[ i ], sizeof( resp.bam_config.shred_socks[ i ].ip ) );
    resp.bam_config.shred_socks[ i ].port = shred_port[ i ];
  }
  ulong publish_chunk = state->shred_out.chunk;
  test_bam_apply_config_resp( state, &resp );

  fd_bam_shred_update_t update = test_bam_read_shred_update( shred_mem, publish_chunk );
  FD_TEST( update.shred_sock_cnt == 2UL );
  FD_TEST( state->bam_shred_sock_cnt == 2UL );
  FD_TEST( update.shred_sock[ 0 ].l==test_bam_ip4_port( "1.1.1.1", 5001U ).l );
  FD_TEST( update.shred_sock[ 1 ].l==test_bam_ip4_port( "2.2.2.2", 5002U ).l );

  test_bam_env_destroy( env );
}

static void
test_bam_control_outputs_retry_without_credit( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_setup_gossip_out( env );
  state->bam_tpu = (fd_ip4_port_t){ .addr=1U, .port=fd_ushort_bswap( 9000U ) };
  state->bam_tpu_fwd = (fd_ip4_port_t){ .addr=2U, .port=fd_ushort_bswap( 9001U ) };
  env->stem->cr_decrement_amount = 1UL;
  env->stem_cr_avail[0] = 0UL;

  FD_TEST( !fd_bam_gossip_update( state, env->stem, 1 ) );
  FD_TEST( env->stem_seqs[0]==0UL );
  FD_TEST( state->tpu_update_state==FD_BAM_CONTACT_UPDATE_STATE_PENDING_BAM );
  FD_TEST( state->client_id_update_state==FD_BAM_CONTACT_UPDATE_STATE_PENDING_BAM );

  env->stem_cr_avail[0] = 1UL;
  FD_TEST( fd_bam_gossip_update( state, env->stem, 1 ) );
  FD_TEST( env->stem_seqs[0]==1UL );
  FD_TEST( state->tpu_update_state==FD_BAM_CONTACT_UPDATE_STATE_APPLIED_BAM );

  state->shred_out = test_bam_out( env, sizeof(fd_bam_shred_update_t) );
  state->bam_shred_sock_cnt = 1U;
  state->bam_shred_sock[0] = (fd_ip4_port_t){ .addr=3U, .port=fd_ushort_bswap( 9002U ) };
  env->stem_cr_avail[0] = 0UL;
  fd_bam_shred_update( state, env->stem, 1 );
  FD_TEST( env->stem_seqs[0]==1UL );
  FD_TEST( state->published_shred_sock_cnt==0U );

  env->stem_cr_avail[0] = 1UL;
  fd_bam_shred_update( state, env->stem, 1 );
  FD_TEST( env->stem_seqs[0]==2UL );
  FD_TEST( state->published_shred_sock_cnt==1U );
  test_bam_env_destroy( env );
}

static void
test_bam_shred_update_disconnect_uses_empty_without_clearing_receivers( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );

  state->shred_out = test_bam_out( env, sizeof(fd_bam_shred_update_t) );
  fd_wksp_t * shred_mem = state->shred_out.mem;

  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  test_bam_config_set_tpu( &resp, "10.20.30.40", 1122U, "11.12.13.14", 3344U );
  resp.bam_config.shred_socks_count = 2U;
  fd_cstr_ncpy( resp.bam_config.shred_socks[ 0 ].ip, "3.3.3.3", sizeof( resp.bam_config.shred_socks[ 0 ].ip ) );
  resp.bam_config.shred_socks[ 0 ].port = 7001U;
  fd_cstr_ncpy( resp.bam_config.shred_socks[ 1 ].ip, "4.4.4.4", sizeof( resp.bam_config.shred_socks[ 1 ].ip ) );
  resp.bam_config.shred_socks[ 1 ].port = 7002U;
  ulong publish_chunk = state->shred_out.chunk;
  test_bam_apply_config_resp( state, &resp );

  fd_bam_shred_update_t updates[ 4 ] = {0};
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

  fd_ip4_port_t expected_tpu     = state->bam_tpu;
  fd_ip4_port_t expected_tpu_fwd = state->bam_tpu_fwd;
  resp = (bam_api_ConfigResponse)bam_api_ConfigResponse_init_default;
  publish_chunk = state->shred_out.chunk;
  test_bam_apply_config_resp( state, &resp );
  updates[ 3 ] = test_bam_read_shred_update( shred_mem, publish_chunk );
  FD_TEST( updates[ 3 ].shred_sock_cnt == 0UL );
  FD_TEST( state->bam_shred_sock_cnt == 0UL );
  FD_TEST( state->bam_tpu.l == expected_tpu.l );
  FD_TEST( state->bam_tpu_fwd.l == expected_tpu_fwd.l );

  test_bam_env_destroy( env );
}

static void
test_bam_config_reuses_cached_contact_for_incomplete_refresh( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );

  fd_wksp_t * gossip_mem = test_bam_setup_gossip_out( env );
  state->bam_status_recent = FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY;

  test_bam_set_default_tpu( state );

  fd_ip4_port_t const expected_tpu     = test_bam_ip4_port( "5.5.5.5", 5000U );
  fd_ip4_port_t const expected_tpu_fwd = test_bam_ip4_port( "6.6.6.6", 6000U );
  for( ulong i=0UL; i<2UL; i++ ) { /* complete, then incomplete */
    bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
    if( !i ) test_bam_config_set_tpu( &resp, "5.5.5.5", 5000U, "6.6.6.6", 6000U );
    else     test_bam_config_set_tpu( &resp, "7.7.7.7", 7000U, NULL,      0U    );
    ulong publish_chunk = state->gossip_out.chunk;
    test_bam_apply_config_resp( state, &resp );
    fd_bam_contact_update_t update = test_bam_read_gossip_update( gossip_mem, publish_chunk );
    FD_TEST( state->bam_tpu.l == expected_tpu.l );
    FD_TEST( state->bam_tpu_fwd.l == expected_tpu_fwd.l );
    test_bam_check_contact( &update, expected_tpu, expected_tpu_fwd, FD_GOSSIP_CONTACT_INFO_CLIENT_FIRE_BAM );
  }

  test_bam_env_destroy( env );
}

static void
test_bam_endpoint_change_forgets_cached_contact_before_incomplete_refresh( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );

  FD_TEST( fd_bam_has_effective_contact( state ) );
  state->bam_shred_sock_cnt = 1U;
  FD_TEST( fd_cstr_to_ip4_addr( "9.9.9.9", &state->bam_shred_sock[0].addr ) );
  state->bam_shred_sock[0].port = fd_ushort_bswap( 9999U );

  fd_bam_ctrl_t ctrl;
  setup_ctrl_defaults( state, &ctrl );

  ushort old_gen = state->scheduler_gen;
  FD_TEST( test_bam_ctrl_request( state, FD_BAM_CTRL_CMD_URL, 1U, "http://new.example.com:8899" )==FD_BAM_CTRL_STATE_SUCCESS );
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
  state->bam_builder_heartbeat_received = 1U;
  state->bam_last_validator_heartbeat_ns = fd_bam_now();

  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  test_bam_config_set_tpu( &resp, "7.7.7.7", 7000U, NULL, 0U );
  /* The endpoint change reset the builder; each session publishes one. */
  uchar const builder[ 32 ] = {7U};
  test_bam_config_set_builder( &resp, builder, 0U );
  test_bam_apply_config_resp( state, &resp );

  FD_TEST( state->bam_config_received == 1U );
  FD_TEST( state->bam_tpu.l == 0UL );
  FD_TEST( state->bam_tpu_fwd.l == 0UL );
  FD_TEST( fd_bam_client_status( state ) == FD_PLUGIN_MSG_BAM_UPDATE_STATUS_CONNECTED_HEALTHY );

  test_bam_env_destroy( env );
}

/* Clearing BAM ownership before pack retires the old generation exposes stale
   BAM work to normal scheduling.  Wait for pack's acknowledgement. */
static void
test_bam_ownership_generation_retirement_waits_for_pack( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  ulong bam_status = FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE;
  ulong generation = (ulong)state->ownership_gen<<1;
  state->bam_status_fseq  = &bam_status;
  state->bam_gen_fseq     = &generation;
  state->ownership_gen_retired = 0U;

  fd_bam_bundle_result_t result = test_make_bundle_result( 731U, 1731UL, 1U );
  result.scheduler_gen = state->scheduler_gen;
  test_enqueue_bundle_result( state, &result );
  FD_TEST( state->feedback_queue_depth==1UL );

  /* A transport reset requests a new ownership generation without
     changing scheduler identity or dropping durable feedback.  Ownership
     remains active until pack acknowledges that old pending work is gone. */
  ushort old_scheduler_gen = state->scheduler_gen;
  ushort old_ownership_gen = state->ownership_gen;
  fd_bam_client_reset( state );
  FD_TEST( state->scheduler_gen==old_scheduler_gen );
  FD_TEST( state->ownership_gen!=old_ownership_gen );
  FD_TEST( generation==(((ulong)state->ownership_gen<<1) | 1UL) );
  FD_TEST( bam_status==FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
  FD_TEST( state->ownership_gen_retired );
  FD_TEST( state->feedback_queue_depth==1UL );

  /* A fast reconnect cannot bypass the still-unacknowledged retirement
     merely because bam_status has not dropped yet. */
  fd_bam_publish_active_state( state, state->stem, 1 );
  FD_TEST( bam_status==FD_BAM_STATUS_FSEQ_OVERRIDE_ACTIVE );
  FD_TEST( state->ownership_gen_retired );

  generation &= ~1UL;
  fd_bam_publish_active_state( state, state->stem, 0 );
  FD_TEST( bam_status==0UL );
  FD_TEST( state->feedback_queue_depth==1UL );

  test_bam_env_destroy( env );
}

/* --- Config and fee propagation ----------------------------------------------------- */

static void
test_bam_config_updates_contact_info( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  FD_TEST( !state->bam_tpu.l && !state->bam_tpu_fwd.l );

  /* Initial config populates TPU endpoints.  A later one with an invalid
     forward socket, or with neither socket, preserves the last valid BAM
     contact. */
  fd_ip4_port_t const expected_tpu     = test_bam_ip4_port( "1.2.3.4", 9000U );
  fd_ip4_port_t const expected_tpu_fwd = test_bam_ip4_port( "5.6.7.8", 10001U );
  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  test_bam_config_set_tpu( &resp, "1.2.3.4", 9000U, "5.6.7.8", 10001U );
  fd_cstr_ncpy( resp.bam_config.prio_fee_recipient_pubkey, "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d",
                sizeof( resp.bam_config.prio_fee_recipient_pubkey ) );
  resp.bam_config.commission_bps = 2750U;
  for( ulong i=0UL; i<3UL; i++ ) {
    if( i==1UL ) resp.bam_config.has_tpu_fwd_sock = false;
    if( i==2UL ) resp.bam_config.has_tpu_sock     = false;
    test_bam_apply_config_resp( state, &resp );
    FD_TEST( state->bam_tpu.l==expected_tpu.l && state->bam_tpu_fwd.l==expected_tpu_fwd.l );
  }

  test_bam_env_destroy( env );
}

/* Delivers a BlockEngineBuilderConfig update.  A NULL pubkey sends a
   string that does not decode to 32 bytes. */
static void
test_bam_apply_builder( fd_bam_tile_t * state,
                        uchar const *   pubkey,
                        uint            commission ) {
  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  if( pubkey ) {
    test_bam_config_set_builder( &resp, pubkey, commission );
  } else {
    resp.has_block_engine_config = true;
    fd_memset( resp.block_engine_config.builder_pubkey, '1', FD_BASE58_ENCODED_32_LEN );
    resp.block_engine_config.builder_commission = commission;
  }
  test_bam_apply_config_resp( state, &resp );
}

/* The session's builder and the fee config published to pack agree. */
static void
test_bam_check_builder( fd_bam_tile_t const * state,
                        uint                  version,
                        uchar const           pubkey[ 32 ],
                        uint                  commission ) {
  FD_TEST( state->fee_cfg->version==version );
  FD_TEST( state->builder_commission==commission && state->fee_cfg->builder_commission==commission );
  FD_TEST( !memcmp( state->builder_pubkey, pubkey, 32UL ) && !memcmp( state->fee_cfg->builder_pubkey, pubkey, 32UL ) );
}

/* As in jito-solana (bam_manager.rs update_block_engine_key_and_commission),
   a builder update applies as a whole: a commission above 100 or an
   undecodable or all-zero (default) pubkey discards the whole update,
   including its otherwise valid half.  Builder metadata comes from
   BlockEngineBuilderConfig only, so a BamConfig commission cannot change
   it.  A client reset forgets the builder (the next session must publish
   one) but not the published fee config. */
static void
test_bam_builder_fee_info( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  uchar const zero       [ 32 ] = {0};
  uchar const pubkey     [ 32 ] = {1,2,3,4,5};
  uchar const replacement[ 32 ] = {9,8,7,6,5};

  /* A valid commission without an initial valid pubkey is not a complete
     builder snapshot. */
  test_bam_apply_builder( state, NULL, 4U );
  test_bam_check_builder( state, 0U, zero, 0U );

  bam_api_ConfigResponse resp = bam_api_ConfigResponse_init_default;
  test_bam_config_set_builder( &resp, pubkey, 5U );
  resp.has_bam_config = true;
  fd_cstr_ncpy( resp.bam_config.prio_fee_recipient_pubkey, "4uhcVJyU9pJkvQyS88uRDiswHXSCkY3zQawwpjk2NsNY",
                sizeof( resp.bam_config.prio_fee_recipient_pubkey ) );
  resp.bam_config.commission_bps = 300U;
  test_bam_apply_config_resp( state, &resp );
  test_bam_check_builder( state, 1U, pubkey, 5U );

  resp = (bam_api_ConfigResponse)bam_api_ConfigResponse_init_default;
  resp.has_bam_config = true;
  resp.bam_config.commission_bps = 15000U;
  test_bam_apply_config_resp( state, &resp );
  test_bam_check_builder( state, 1U, pubkey, 5U );

  test_bam_apply_builder( state, NULL,        7U   );
  test_bam_check_builder( state, 1U, pubkey, 5U );
  test_bam_apply_builder( state, zero,        8U   );
  test_bam_check_builder( state, 1U, pubkey, 5U );
  test_bam_apply_builder( state, replacement, 101U );
  test_bam_check_builder( state, 1U, pubkey, 5U );

  /* A subsequent complete valid update replaces both exported values. */
  test_bam_apply_builder( state, replacement, 9U );
  test_bam_check_builder( state, 2U, replacement, 9U );

  fd_bam_client_reset( state );
  FD_TEST( fd_mem_iszero( state->builder_pubkey, 32UL ) && !state->builder_commission );
  FD_TEST( state->fee_cfg->version==2U && !memcmp( state->fee_cfg->builder_pubkey, replacement, 32UL ) );

  test_bam_env_destroy( env );
}

/* --- Bundle result durability ------------------------------------------------------- */

/* Verifies that bundle results buffered in the queue survive fd_bam_client_reset
 * and flush in FIFO order without exceeding BAM's 24-result wire limit. */

static void
test_bam_bundle_result_queue_flushes_after_reconnect( fd_wksp_t * wksp ) {
  /* Client reset should preserve queued bundle results, and the next scheduler
     stream should flush them in FIFO order. */
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  test_bam_env_mock_conn( env );

  state->bam_results_head = FD_BAM_MAX_PENDING_RESULTS-1U;
  state->bam_results_tail = FD_BAM_MAX_PENDING_RESULTS-1U;
  /* Keep this independent of FD_BAM_RESULTS_PER_MESSAGE: the receiver rejects
     any message with more than 24 results. */
  uint const result_cnt = 49U;
  for( uint i=0U; i<result_cnt; i++ ) {
    fd_bam_bundle_result_t res = test_make_bundle_result( 200 + i, 1200, 2 );
    test_enqueue_bundle_result( state, &res );
  }

  ulong expected_tail = state->bam_results_tail;
  fd_bam_client_reset( state );
  FD_TEST( fd_bam_flush_results( state ) == 0 );
  for( ulong i=0UL; i<FD_METRICS_ENUM_BAM_ENQUEUE_RESULT_CNT; i++ ) FD_TEST( !state->metrics.outbound_enqueue_outcome_cnt[ i ] );
  FD_TEST( state->feedback_queue_depth == result_cnt );
  FD_TEST( state->bam_results_head == FD_BAM_MAX_PENDING_RESULTS-1U );
  FD_TEST( state->bam_results_tail == expected_tail );
  FD_TEST( state->bam_results[ FD_BAM_MAX_PENDING_RESULTS-1U ].seq_id == 200U );
  FD_TEST( state->bam_results[ 0 ].seq_id == 201U );

  test_bam_env_mock_conn( env );
  test_bam_prepare_scheduler_stream( state );

  test_bam_decoded_message_t decoded;
  for( uint sent=0U; sent<result_cnt; ) {
    uint const batch_cnt = fd_uint_min( 24U, result_cnt-sent );
    FD_TEST( fd_bam_flush_results( state ) == 1 );

    /* A busy TX buffer must leave the remaining FIFO untouched and is not
       a send failure. */
    FD_TEST( fd_bam_flush_results( state ) == 0 );
    FD_TEST( state->metrics.outbound_enqueue_outcome_cnt[ FD_METRICS_ENUM_BAM_ENQUEUE_RESULT_V_RESULT_ENQUEUE_FAILED_IDX ] == 0UL );
    test_bam_expect_msg( state, &decoded, bam_api_SchedulerMessageV0_multiple_atomic_txn_batch_result_tag );
    FD_TEST( decoded.multi.result_cnt == batch_cnt );
    for( uint i=0U; i<batch_cnt; i++ ) {
      FD_TEST( decoded.multi.results[ i ].seq_id == 200U+sent+i );
    }
    sent += batch_cnt;
    FD_TEST( state->feedback_queue_depth == result_cnt-sent );
    FD_TEST( state->bam_results_head == (FD_BAM_MAX_PENDING_RESULTS-1U+sent) % FD_BAM_MAX_PENDING_RESULTS );
    FD_TEST( state->bam_results_tail == expected_tail );
  }
  FD_TEST( state->feedback_queue_depth == 0UL );
  FD_TEST( state->bam_results_head == state->bam_results_tail );
  FD_TEST( state->metrics.outbound_enqueue_outcome_cnt[ FD_METRICS_ENUM_BAM_ENQUEUE_RESULT_V_RESULT_ENQUEUED_IDX ] == 3UL );

  test_bam_env_destroy( env );
}

static void
test_bam_full_result_queue_deduplicates_before_reject( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );

  for( uint i=0U; i<FD_BAM_MAX_PENDING_RESULTS; i++ ) {
    fd_bam_bundle_result_t res = test_make_bundle_result( 10000U+i, 2000UL+i, 1U );
    test_enqueue_bundle_result( state, &res );
  }

  ulong dropped_before = state->metrics.feedback_results_dropped_cnt;
  ushort newest_idx = (ushort)(((uint)state->bam_results_tail + FD_BAM_MAX_PENDING_RESULTS - 1U) % FD_BAM_MAX_PENDING_RESULTS);
  fd_bam_enqueue_result( state, &state->bam_results[ newest_idx ] );
  FD_TEST( state->feedback_queue_depth==FD_BAM_MAX_PENDING_RESULTS );
  FD_TEST( state->metrics.feedback_results_dropped_cnt==dropped_before );

  fd_bam_enqueue_result( state, &state->bam_results[ state->bam_results_head ] );
  FD_TEST( state->feedback_queue_depth==FD_BAM_MAX_PENDING_RESULTS );
  FD_TEST( state->metrics.feedback_results_dropped_cnt==dropped_before );

  fd_bam_bundle_result_t unique = test_make_bundle_result( 20000U, 3000UL, 1U );
  unique.scheduler_gen = state->scheduler_gen;
  fd_bam_enqueue_result( state, &unique );
  FD_TEST( state->feedback_queue_depth==FD_BAM_MAX_PENDING_RESULTS );
  FD_TEST( state->metrics.feedback_results_dropped_cnt==dropped_before+1UL );

  test_bam_env_destroy( env );
}

/* A scheduler stream whose response header block never completes (a
   HEADERS frame without END_HEADERS) times out and resets the client.
   Completed headers clear the deadline for the long-lived stream. */
static void
test_bam_scheduler_stream_header_deadline( fd_wksp_t * wksp ) {
  for( ulong end_headers=0UL; end_headers<2UL; end_headers++ ) {
    test_bam_env_t env[1];
    fd_bam_tile_t * state = test_bam_env_create( env, wksp );
    test_bam_env_mock_conn_empty( env );
    test_bam_env_mock_h2_hs( state );
    state->bam_config_inflight = 1U;
    state->bam_auth_ready      = 1U;
    g_clock = (long)5e9;
    test_bam_keepalive_sync( state, g_clock );
    state->bam_last_config_poll_ns = g_clock;
    fd_bam_client_step_reconnect( state, g_clock );
    fd_grpc_h2_stream_t * stream = state->bam_stream;
    FD_TEST( stream && stream->has_header_deadline );

    fd_grpc_h2_cb_headers( state->grpc_client->conn, &stream->s, NULL, 0UL, end_headers ? FD_H2_FLAG_END_HEADERS : 0UL );
    fd_grpc_client_service_streams( state->grpc_client, g_clock+FD_BAM_CLIENT_REQUEST_TIMEOUT-1L );
    FD_TEST( state->bam_stream==stream && !state->defer_reset );
    fd_grpc_client_service_streams( state->grpc_client, g_clock+FD_BAM_CLIENT_REQUEST_TIMEOUT );
    FD_TEST( (state->bam_stream==stream)==(int)end_headers );
    FD_TEST( !!state->defer_reset==!end_headers );
    FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_REQUEST_TIMEOUT_IDX ]==(ulong)!end_headers );

    test_bam_env_destroy( env );
  }
}

/* Appends an unknown field of each skippable wire type (varint, fixed32,
   fixed64, length-delimited) with the given tag. */
static void
test_bam_encode_unknown_fields_raw( pb_ostream_t * stream,
                                    uint32_t       tag ) {
  uint  const u32      = 0x01020304U;
  ulong const u64      = 0x0102030405060708UL;
  uchar const bytes[3] = { 0, 1, 2 };
  test_bam_encode_varint_field_raw( stream, tag, 7UL );
  FD_TEST( pb_encode_tag( stream, PB_WT_32BIT, tag ) && pb_encode_fixed32( stream, &u32 ) );
  FD_TEST( pb_encode_tag( stream, PB_WT_64BIT, tag ) && pb_encode_fixed64( stream, &u64 ) );
  test_bam_encode_string_field_raw( stream, tag, bytes, sizeof(bytes) );
}

/* proto3 receivers skip fields newer than their schema.  Unknown fields
   must not publish on their own or hide malformed input. */
static void
test_bam_scheduler_ignores_unknown_fields( fd_wksp_t * wksp ) {
  uint32_t const unknown_tag[ 3 ] = { 31U, 16U, 1000U }; /* 15 (mss_padding) is known */
  long const clock_before = g_clock;

  uchar batch[ 2 ][ 256 ];
  size_t batch_sz[ 2 ];
  for( ulong i=0UL; i<2UL; i++ )
    batch_sz[ i ] = test_bam_encode_one_packet_atomic_batch_raw( 610U+(uint)i, (uchar)'u', batch[ i ], sizeof(batch[ i ]) );

  uchar multi_payload[ 1024 ];
  uchar v0_payload[ 1280 ];
  uchar protobuf[ 1536 ];

  /* Unknown fields before, between and after batches */
  pb_ostream_t multi = pb_ostream_from_buffer( multi_payload, sizeof(multi_payload) );
  for( ulong i=0UL; i<3UL; i++ ) {
    test_bam_encode_unknown_fields_raw( &multi, unknown_tag[ i ] );
    if( i<2UL ) test_bam_encode_string_field_raw( &multi, bam_types_MultipleAtomicTxnBatch_batches_tag, batch[ i ], batch_sz[ i ] );
  }
  size_t protobuf_sz = test_bam_encode_scheduler_multi_payload_raw( multi_payload, multi.bytes_written,
                                                                    protobuf, sizeof(protobuf) );
  test_bam_env_t env[1];
  fd_bam_tile_t * state = test_bam_env_create( env, wksp );
  zero_meta_ts( env->out_mcache, 2UL );
  g_clock = FD_BAM_UNKNOWN_FIELD_NOTICE_INTERVAL_NS;
  test_bam_rx_sched( state, protobuf, protobuf_sz );
  FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_SCHEDULER_ENVELOPE_DECODE_IDX ]==0UL );
  FD_TEST( state->metrics.ingress_multi_message_received_cnt==1UL );
  FD_TEST( state->metrics.ingress_batch_commit_attempt_cnt==2UL );
  FD_TEST( test_bam_env_drain_all_pending_txns( env )==2UL );
  FD_TEST( state->metrics.transaction_published_cnt==2UL );
  FD_TEST( state->bam_unknown_field_notice_ns==g_clock );

  /* The notice is rate limited */
  g_clock += 1L;
  test_bam_rx_sched( state, protobuf, protobuf_sz );
  FD_TEST( state->bam_unknown_field_notice_ns==g_clock-1L );
  test_bam_env_destroy( env );

  /* Unknown SchedulerResponseV0 fields around a known variant */
  multi = pb_ostream_from_buffer( multi_payload, sizeof(multi_payload) );
  test_bam_encode_string_field_raw( &multi, bam_types_MultipleAtomicTxnBatch_batches_tag, batch[ 0 ], batch_sz[ 0 ] );
  pb_ostream_t v0 = pb_ostream_from_buffer( v0_payload, sizeof(v0_payload) );
  test_bam_encode_unknown_fields_raw( &v0, unknown_tag[ 0 ] );
  test_bam_encode_string_field_raw( &v0, bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag,
                                    multi_payload, multi.bytes_written );
  test_bam_encode_unknown_fields_raw( &v0, unknown_tag[ 2 ] );
  protobuf_sz = test_bam_encode_scheduler_response_v0_raw( v0_payload, v0.bytes_written, protobuf, sizeof(protobuf) );
  state = test_bam_env_create( env, wksp );
  zero_meta_ts( env->out_mcache, 1UL );
  test_bam_rx_sched( state, protobuf, protobuf_sz );
  FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_SCHEDULER_ENVELOPE_DECODE_IDX ]==0UL );
  FD_TEST( state->metrics.ingress_multi_message_received_cnt==1UL );
  FD_TEST( test_bam_env_drain_all_pending_txns( env )==1UL );
  test_bam_env_destroy( env );

  /* A SchedulerResponseV0 holding only an unknown variant is ignored */
  v0 = pb_ostream_from_buffer( v0_payload, sizeof(v0_payload) );
  test_bam_encode_string_field_raw( &v0, unknown_tag[ 0 ], multi_payload, multi.bytes_written );
  protobuf_sz = test_bam_encode_scheduler_response_v0_raw( v0_payload, v0.bytes_written, protobuf, sizeof(protobuf) );
  state = test_bam_env_create( env, wksp );
  test_bam_rx_sched( state, protobuf, protobuf_sz );
  FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_SCHEDULER_ENVELOPE_DECODE_IDX ]==0UL );
  FD_TEST( state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_UNSUPPORTED_VERSION_IDX ]==0UL );
  FD_TEST( state->metrics.ingress_multi_message_received_cnt==0UL );
  FD_TEST( bam_pending_txn_empty( state->pending_txns ) );
  FD_TEST( !state->defer_reset );

  /* As in jito-solana, so is an empty one, but a response without a v0
     wrapper (none, or another version) resets */
  uchar const empty_v0[ 2 ] = { 0x0a, 0x00 }, unsupported[ 2 ] = { 0x12, 0x00 };
  test_bam_rx_sched( state, empty_v0, 2UL );
  FD_TEST( !state->defer_reset && !state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_SCHEDULER_ENVELOPE_DECODE_IDX ] );
  test_bam_rx_sched( state, empty_v0, 0UL );
  FD_TEST( state->defer_reset && state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_SCHEDULER_ENVELOPE_DECODE_IDX ]==1UL );
  state->defer_reset = 0;
  test_bam_rx_sched( state, unsupported, 2UL );
  FD_TEST( state->defer_reset && state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_UNSUPPORTED_VERSION_IDX ]==1UL );
  test_bam_env_destroy( env );

  /* An unskippable unknown field rolls back the staged message */
  uchar trailer[ 8 ];
  pb_ostream_t t = pb_ostream_from_buffer( trailer, sizeof(trailer) );
  FD_TEST( pb_encode_varint( &t, (unknown_tag[ 0 ]<<3) | 3U ) ); /* START_GROUP */
  test_bam_scheduler_malformed_trailer_rolls_back( wksp, trailer, t.bytes_written );

  g_clock = clock_before;
}

/* Captures the warning logged for a scheduler decode failure and checks
   that it names reason. */
static void
test_bam_expect_scheduler_decode_failure_reason( test_bam_env_t * env,
                                                 uchar const *    protobuf,
                                                 size_t           protobuf_sz,
                                                 char const *     reason ) {
  int log_fd    = memfd_create( "test_bam_log", 0 );
  int stderr_fd = dup( STDERR_FILENO );
  FD_TEST( log_fd>=0 && stderr_fd>=0 );
  int level_logfile = fd_log_level_logfile();
  int level_stderr  = fd_log_level_stderr();
  fd_log_level_logfile_set( fd_int_min( level_logfile, 3 ) );
  fd_log_level_stderr_set ( fd_int_min( level_stderr,  3 ) );
  FD_TEST( dup2( log_fd, STDERR_FILENO )==STDERR_FILENO );
  test_bam_rx_sched( env->state, protobuf, protobuf_sz );
  FD_TEST( dup2( stderr_fd, STDERR_FILENO )==STDERR_FILENO );
  fd_log_level_logfile_set( level_logfile );
  fd_log_level_stderr_set ( level_stderr  );

  char log[ 4096 ];
  long log_sz = pread( log_fd, log, sizeof(log)-1UL, 0L );
  FD_TEST( log_sz>0L );
  log[ log_sz ] = '\0';
  FD_TEST( !close( stderr_fd ) && !close( log_fd ) );

  char expected[ 128 ];
  FD_TEST( fd_cstr_printf_check( expected, sizeof(expected), NULL,
                                 "Protobuf decode of (bam_api.SchedulerResponse) failed (%s)", reason ) );
  if( FD_UNLIKELY( !strstr( log, expected ) ) ) FD_LOG_ERR(( "missing \"%s\" in log:\n%s", expected, log ));
}

static void
test_bam_scheduler_decode_failure_names_reason( fd_wksp_t * wksp ) {
  test_bam_env_t env[1];
  test_bam_env_create( env, wksp );

  /* Known MultipleAtomicTxnBatch field with the wrong wire type after a
     valid batch */
  uchar batch[ 256 ];
  size_t batch_sz = test_bam_encode_one_packet_atomic_batch_raw( 606U, (uchar)'r', batch, sizeof(batch) );
  uchar multi_payload[ 512 ];
  pb_ostream_t multi = pb_ostream_from_buffer( multi_payload, sizeof(multi_payload) );
  test_bam_encode_string_field_raw( &multi, bam_types_MultipleAtomicTxnBatch_batches_tag, batch, batch_sz );
  test_bam_encode_varint_field_raw( &multi, bam_types_MultipleAtomicTxnBatch_batches_tag, 1UL );
  uchar protobuf[ 1024 ];
  size_t protobuf_sz = test_bam_encode_scheduler_multi_payload_raw( multi_payload, multi.bytes_written,
                                                                    protobuf, sizeof(protobuf) );
  test_bam_expect_scheduler_decode_failure_reason( env, protobuf, protobuf_sz, "wrong wire type" );

  /* Unskippable MultipleAtomicTxnBatch field after a malformed batch */
  uchar const bad_packet[ 1 ] = { 0 }; /* zero tag */
  uchar bad_batch[ 32 ];
  pb_ostream_t bad = pb_ostream_from_buffer( bad_batch, sizeof(bad_batch) );
  test_bam_encode_varint_field_raw( &bad, bam_types_AtomicTxnBatch_seq_id_tag, 607U );
  test_bam_encode_string_field_raw( &bad, bam_types_AtomicTxnBatch_packets_tag, bad_packet, sizeof(bad_packet) );
  multi = pb_ostream_from_buffer( multi_payload, sizeof(multi_payload) );
  test_bam_encode_string_field_raw( &multi, bam_types_MultipleAtomicTxnBatch_batches_tag, bad_batch, bad.bytes_written );
  FD_TEST( pb_encode_varint( &multi, (31UL<<3) | 3UL ) ); /* START_GROUP */
  protobuf_sz = test_bam_encode_scheduler_multi_payload_raw( multi_payload, multi.bytes_written,
                                                             protobuf, sizeof(protobuf) );
  test_bam_expect_scheduler_decode_failure_reason( env, protobuf, protobuf_sz, "invalid wire_type" );

  /* Unknown SchedulerResponseV0 field that cannot be skipped */
  uchar v0_payload[ 64 ];
  pb_ostream_t v0 = pb_ostream_from_buffer( v0_payload, sizeof(v0_payload) );
  FD_TEST( pb_encode_varint( &v0, (31UL<<3) | 3UL ) ); /* START_GROUP */
  protobuf_sz = test_bam_encode_scheduler_response_v0_raw( v0_payload, v0.bytes_written, protobuf, sizeof(protobuf) );
  test_bam_expect_scheduler_decode_failure_reason( env, protobuf, protobuf_sz, "invalid wire_type" );

  /* SchedulerResponseV0 field longer than its envelope */
  v0 = pb_ostream_from_buffer( v0_payload, sizeof(v0_payload) );
  FD_TEST( pb_encode_tag( &v0, PB_WT_STRING, bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag ) );
  FD_TEST( pb_encode_varint( &v0, 10UL ) ); /* no ten bytes follow */
  protobuf_sz = test_bam_encode_scheduler_response_v0_raw( v0_payload, v0.bytes_written, protobuf, sizeof(protobuf) );
  test_bam_expect_scheduler_decode_failure_reason( env, protobuf, protobuf_sz, "parent stream too short" );

  /* SchedulerResponseV0 tag that is not a valid varint */
  uchar const bad_tag[ 5 ] = { 0x80, 0x80, 0x80, 0x80, 0x10 };
  protobuf_sz = test_bam_encode_scheduler_response_v0_raw( bad_tag, sizeof(bad_tag), protobuf, sizeof(protobuf) );
  test_bam_expect_scheduler_decode_failure_reason( env, protobuf, protobuf_sz, "varint overflow" );

  FD_TEST( env->state->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_CLIENT_FAILURE_REASON_V_SCHEDULER_ENVELOPE_DECODE_IDX ]==5UL );
  FD_TEST( env->state->metrics.ingress_multi_message_received_cnt==0UL );
  FD_TEST( bam_pending_txn_empty( env->state->pending_txns ) );
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
  /* Transaction V1 raises FD_TXN_MTU to 4096 bytes.  The output dcache and
     pending-transaction deque each retain a full scheduler message, so the
     former 8 MiB workspace is no longer large enough. */
  ulong        page_cnt = fd_env_strip_cmdline_ulong( &argc, &argv, "--page-cnt",    NULL, 16384UL                      );
  ulong        numa_idx = fd_env_strip_cmdline_ulong( &argc, &argv, "--numa-idx",    NULL, fd_shmem_numa_idx( cpu_idx ) );

  fd_wksp_t * wksp = fd_wksp_new_anonymous( fd_cstr_to_shmem_page_sz( _page_sz ), page_cnt, fd_shmem_cpu_idx( numa_idx ), "bam-test", 16UL );
  FD_TEST( wksp );

  /* Scheduler ingestion/validation */
  test_bam_packets_forwarded( wksp );
  test_bam_slot_ingress_timing_tracks_max_schedule_slot_and_rejects_stale_arrival( wksp );
  test_bam_current_slot_work_status_bits( wksp );
  test_bam_grpc_stream_open_gauge( wksp );
  test_bam_feedback_results_overrun_dropped( wksp );
  test_bam_slot_ingress_timing_tracks_hash_collisions( wksp );
  test_bam_publish_batch_uses_captured_ingress_metadata( wksp );
  test_bam_multiple_batches_forwarded( wksp );
  test_bam_independent_batches_queue_and_drain_in_order( wksp );
  test_bam_pending_over_stem_burst_drains_in_multiple_passes( wksp );
  test_bam_verify_backpressure_keeps_feedback_polling( wksp );
  test_bam_ingress_frag_copied_before_overrun_check( wksp );
  test_bam_queue_full_rejects_whole_batch_without_partial_enqueue( wksp );
  test_bam_multiple_batches_accept_full_txn_count( wksp, FD_BAM_FULL_ATOMIC_BATCHES_PER_STEM_BURST, 700U, 100UL );
  test_bam_multiple_batches_accept_full_txn_count( wksp, FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE, 1500U, 500UL );
  test_bam_max_wire_message_fits_min_grpc_buffer( wksp );
  test_bam_scheduler_rejects_invalid_custom_decode_fields( wksp );
  test_bam_scheduler_accepts_mss_padding( wksp );
  test_bam_scheduler_malformed_mss_padding_rolls_back( wksp );
  test_bam_scheduler_mss_padding_does_not_raise_batch_limit( wksp );
  test_bam_scheduler_v0_oneof_uses_last_field( wksp );
  test_bam_multiple_batches_isolate_nested_decode_failures( wksp );
  test_bam_scheduler_rejects_non_length_delimited_packets( wksp );
  test_bam_multiple_batches_message_limit( wksp );
  test_bam_bundle_forwards_without_builder_info( wksp );
  test_bam_bundle_revert_flag_cases( wksp );
  test_bam_stale_slot_rejects_before_vote_error( wksp );
  test_bam_ingress_rejects_batch_variants( wksp );
  test_bam_generated_v1_protobuf_fixtures( wksp );
  test_bam_v1_packet_size_boundaries( wksp );
  test_bam_bundle_uses_data_length_for_size_check( wksp );
  test_bam_bundle_rejects_missing_batches( wksp );

  /* Connection lifecycle and watchdog */
  /* The tile is a waker client only in efficient layout; run both. */
  for( int waker=1; waker>=0; waker-- ) test_bam_stream_end_with_pending_txn_reconnects1( wksp, waker );
  test_bam_receive_backpressure_parks_without_waker_fire( wksp );
  test_bam_tcp_connect_completion_uses_so_error( wksp );
  for( int waker=1; waker>=0; waker-- ) test_bam_client_services_socket_and_flushes_tx1( wksp, waker );
  test_bam_grpc_end_handling( wksp );
  test_bam_polls_socket_without_waker( wksp );
  test_bam_connect_without_waker_skips_epoll( wksp );
  test_bam_connect_timeout_resets_stalled_handshake( wksp );
  test_bam_keepalive_pings_during_continuation( wksp );
  test_bam_grpc_timeout( wksp );
  test_bam_config_timeout_keeps_scheduler_live( wksp );
  test_bam_config_rpc_applies_only_after_ok( wksp );
  test_bam_config_status_failure_keeps_scheduler_live( wksp );
  test_bam_config_rpc_discards_incomplete_or_invalid_response( wksp );
  test_bam_config_rpc_accepts_empty_and_unknown_fields( wksp );
  test_bam_first_config_timeout_reconnects( wksp );
  test_bam_scheduler_http_error_requests_transport_reset( wksp );
  test_bam_auth_retry_deadline_tracks_backoff( wksp );
  test_bam_blocked_config_deadline_does_not_spin( wksp );
  test_bam_backoff_step_returns_without_sleep( wksp );
  test_bam_heartbeat_timeout_forces_disconnect( wksp );
  test_bam_heartbeat_reset_extends_timeout( wksp );
  test_bam_client_status( wksp );
  test_bam_reconnect_backoff_once_per_failure( wksp );
  test_bam_scheduler_not_scheduled_reject_streak( wksp );
  test_bam_scheduler_unavailable_reject_streak( wksp );
  test_bam_scheduler_results_wait_for_acceptance( wksp );
  test_bam_ownership_generation_retirement_waits_for_pack( wksp );

  /* Scheduler/auth messaging */
  test_bam_auth_challenge_response_verifies_cached_identity( wksp );
  test_bam_scheduler_auth_proof_publishes_message( wksp );
  test_bam_missing_config_repolls_despite_valid_builder_info( wksp );
  test_bam_missing_config_uses_short_repoll_throttle( wksp );
  test_bam_config_polls_every_minute_with_builder( wksp );
  test_bam_scheduler_heartbeat_publishes_message( wksp );
  test_bam_scheduler_ping_publishes_message( wksp );
  test_bam_outbound_send_blocked_is_not_failure( wksp );
  test_bam_scheduler_stream_replays_only_retained_leader_state( wksp );
  test_bam_leader_state_supersede_counts_drop( wksp );
  test_bam_replay_input_contract( wksp );
  test_bam_replay_next_leader_schedule( wksp );
  test_bam_replay_schedule_recheck_slot_policy( wksp );
  test_bam_replay_leader_slot_outside_epoch( wksp );
  test_bam_replay_wait_gate_policy( wksp );
  test_bam_replay_wait_gate_cleared_on_reset_or_disable( wksp );
  test_bam_identity_switch_halts_signing_and_invalidates_leader_schedule_state( wksp );
  test_bam_replay_schedule_hint_does_not_disconnect( wksp );
  test_bam_pack_leader_channel_contract( wksp );
  test_bam_pack_leader_slot_change_flushes_immediately( wksp );
  test_bam_pack_result_channel_contract( wksp );
  test_bam_bank_result_channel_contract( wksp );
  test_bam_scheduler_result_publishes_message( wksp );
  test_bam_txn_err_mapping_matches_jito();

  /* Control surface */
  test_bam_ctrl_updates_url_and_sni( wksp );
  test_bam_ctrl_defaults_bam_ports( wksp );
  test_bam_ctrl_toggle_enable_updates_runtime_state( wksp );
  test_bam_ctrl_enable_from_disabled_start( wksp );
  test_bam_ctrl_enable_from_dormant_then_set_url( wksp );
  test_bam_ctrl_invalid_url_sets_error_and_preserves_config( wksp );
  test_bam_ctrl_blank_url_clears_and_disables( wksp );
  test_bam_gossip_update_falls_back_to_configured_default_tpu( wksp );

  /* Gossip advertisement */
  test_bam_gossip_tile_applies_contact_update( wksp );
  for( int tie=0; tie<2; tie++ ) test_bam_gossip_stale_contact_case( wksp, tie );
  test_bam_gossip_publishes_bam_config_contact( wksp );
  test_bam_activation_waits_for_gossip_ack( wksp );
  test_bam_handoff_rings_parked_readers( wksp );
  test_bam_handoff_keeps_housekeeping_cadence( wksp );
  test_bam_activation_ignores_stale_gossip_ack_after_reenable( wksp );
  test_bam_disable_clears_status_with_pending_gossip_handoff( wksp );
  for( int trigger=0; trigger<TEST_BAM_CONTACT_TRIGGER_CNT; trigger++ ) test_bam_pending_activation_restores_default_contact( wksp, trigger );
  test_bam_invalid_quic_base_port_does_not_activate( wksp );
  test_bam_gossip_uses_defaults_when_contact_missing( wksp );
  test_bam_gossip_disconnect_uses_defaults_without_clearing_stored_contact( wksp );
  test_bam_runtime_toggle_updates_gossip( wksp );
  test_bam_shred_update_publishes_receiver_list( wksp );
  test_bam_control_outputs_retry_without_credit( wksp );
  test_bam_shred_update_disconnect_uses_empty_without_clearing_receivers( wksp );
  test_bam_config_reuses_cached_contact_for_incomplete_refresh( wksp );
  test_bam_endpoint_change_forgets_cached_contact_before_incomplete_refresh( wksp );

  /* Config and fees */
  test_bam_config_updates_contact_info( wksp );
  test_bam_builder_fee_info( wksp );

  /* Bundle result durability */
  test_bam_bundle_result_queue_flushes_after_reconnect( wksp );
  test_bam_full_result_queue_deduplicates_before_reject( wksp );

  test_bam_scheduler_stream_header_deadline( wksp );

  /* Scheduler schema evolution */
  test_bam_scheduler_ignores_unknown_fields( wksp );

  /* Scheduler decode diagnostics */
  test_bam_scheduler_decode_failure_names_reason( wksp );

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
