#include "../poh/fd_poh_tile.c"
#include "test_bam_poh_fixture.h"

#define TEST_POH_DEPTH (32UL)
#define TEST_POH_TAG   (2UL)

struct test_bam_poh_fixture {
  fd_poh_tile_t ctx[1];
  fd_stem_context_t stem[1];
  fd_frag_meta_t * mcaches[4];
  ulong seqs[4];
  ulong depths[4];
  ulong credits[4];
  ulong min_credit;
  int reliable[4];
  ulong ledger_seq;
  ulong result_seq;
  test_bam_poh_summary_t summary;
};

static void *
test_poh_dcache( fd_wksp_t * wksp,
                 ulong       mtu ) {
  ulong size = fd_dcache_req_data_sz( mtu, TEST_POH_DEPTH, 1UL, 1 );
  void * mem = fd_wksp_alloc_laddr( wksp, fd_dcache_align(), fd_dcache_footprint( size, 0UL ), TEST_POH_TAG );
  FD_TEST( mem );
  void * dcache = fd_dcache_join( fd_dcache_new( mem, size, 0UL ) );
  FD_TEST( dcache );
  return dcache;
}

test_bam_poh_fixture_t *
test_bam_poh_fixture_new( fd_wksp_t * wksp,
                          ulong       slot,
                          uint        pack_idx ) {
  test_bam_poh_fixture_t * f = fd_wksp_alloc_laddr( wksp, alignof(test_bam_poh_fixture_t), sizeof(*f), TEST_POH_TAG );
  FD_TEST( f );
  fd_memset( f, 0, sizeof(*f) );
  f->summary.slot = slot;
  f->summary.expect_pack_idx = pack_idx;
  fd_poh_out_t * outputs[4] = { f->ctx->shred_out, f->ctx->replay_out, f->ctx->executed_txn_out, f->ctx->bam_out };
  ulong mtus[4] = { MAX_MICROBLOCK_SZ, sizeof(fd_poh_leader_slot_ended_t), FD_TXN_SIGNATURE_SZ, sizeof(fd_bam_bundle_result_t) };
  for( ulong i=0UL; i<4UL; i++ ) {
    void * mc = fd_wksp_alloc_laddr( wksp, fd_mcache_align(), fd_mcache_footprint( TEST_POH_DEPTH, 0UL ), TEST_POH_TAG );
    FD_TEST( mc );
    f->mcaches[i] = fd_mcache_join( fd_mcache_new( mc, TEST_POH_DEPTH, 0UL, 0UL ) );
    FD_TEST( f->mcaches[i] );
    void * dc = test_poh_dcache( wksp, mtus[i] );
    ulong chunk0 = fd_dcache_compact_chunk0( wksp, dc );
    *outputs[i] = (fd_poh_out_t){ .idx=i, .mem=wksp, .chunk=chunk0, .chunk0=chunk0,
                                .wmark=fd_dcache_compact_wmark( wksp, dc, mtus[i] ) };
    f->depths[i] = TEST_POH_DEPTH;
    f->credits[i] = TEST_POH_DEPTH;
    f->reliable[i] = 1;
  }
  f->min_credit = TEST_POH_DEPTH;
  *f->stem = (fd_stem_context_t){ .mcaches=f->mcaches, .seqs=f->seqs, .depths=f->depths,
                                .cr_avail=f->credits, .min_cr_avail=&f->min_credit,
                                .cr_decrement_amount=1UL, .out_reliable=f->reliable };
  for( ulong i=0UL; i<2UL; i++ ) {
    void * dc = test_poh_dcache( wksp, MAX_MICROBLOCK_SZ );
    f->ctx->in_kind[i] = IN_KIND_EXECLE;
    f->ctx->in[i].mem = wksp;
    f->ctx->in[i].chunk0 = fd_dcache_compact_chunk0( wksp, dc );
    f->ctx->in[i].wmark = fd_dcache_compact_wmark( wksp, dc, MAX_MICROBLOCK_SZ );
    f->ctx->in[i].mtu = MAX_MICROBLOCK_SZ;
  }
  FD_TEST( fd_poh_join( fd_poh_new( f->ctx->poh ), f->ctx->shred_out, f->ctx->replay_out, NULL ) );
  uchar hash[32] = {0};
  uchar block_id[32] = {1};
  FD_TEST( slot>0UL );
  fd_poh_reset( f->ctx->poh, f->stem, 0L, 62500UL, 64UL, 6250UL, slot-1UL,
                hash, slot, 1024UL, block_id );
  fd_poh_begin_leader( f->ctx->poh, slot, 62500UL, 64UL, 6250UL, 1024UL, 0L );
  f->ctx->expect_pack_idx = pack_idx;
  return f;
}

int
test_bam_poh_fixture_consume( test_bam_poh_fixture_t * f,
                              ulong                    worker,
                              ulong                    sig,
                              void const *             fragment,
                              ulong                    sz ) {
  FD_TEST( worker<2UL && sz<=MAX_MICROBLOCK_SZ );
  ulong chunk = f->ctx->in[worker].chunk0;
  fd_memcpy( fd_chunk_to_laddr( f->ctx->in[worker].mem, chunk ), fragment, sz );
  int held = returnable_frag( f->ctx, worker, 0UL, sig, chunk, sz, 0UL, 0UL, 0UL, f->stem );
  f->summary.expect_pack_idx = f->ctx->expect_pack_idx;
  while( f->ledger_seq<f->seqs[0] ) {
    fd_frag_meta_t const * m = f->mcaches[0]+fd_mcache_line_idx( f->ledger_seq++, TEST_POH_DEPTH );
    FD_TEST( fd_disco_poh_sig_slot( m->sig )==f->summary.slot );
    uchar const * payload = fd_chunk_to_laddr_const( f->ctx->shred_out->mem, m->chunk );
    fd_entry_batch_header_t const * h = (fd_entry_batch_header_t const *)(payload+sizeof(fd_entry_batch_meta_t));
    ulong remaining = m->sz-sizeof(fd_entry_batch_meta_t)-sizeof(fd_entry_batch_header_t);
    payload += sizeof(fd_entry_batch_meta_t)+sizeof(fd_entry_batch_header_t);
    for( ulong i=0UL; i<h->txn_cnt; i++ ) {
      FD_TEST( f->summary.txn_cnt<16UL );
      fd_txn_p_t * txn = &f->summary.txns[f->summary.txn_cnt++];
      ulong payload_sz = 0UL;
      FD_TEST( fd_txn_parse_core( payload, remaining, TXN(txn), NULL, &payload_sz ) );
      FD_TEST( payload_sz<=FD_TPU_MTU && payload_sz<=remaining );
      fd_memcpy( txn->payload, payload, payload_sz );
      txn->payload_sz = (ushort)payload_sz;
      payload += payload_sz;
      remaining -= payload_sz;
    }
    FD_TEST( !remaining );
  }
  while( f->result_seq<f->seqs[3] ) {
    fd_frag_meta_t const * m = f->mcaches[3]+fd_mcache_line_idx( f->result_seq++, TEST_POH_DEPTH );
    FD_TEST( m->sz==sizeof(fd_bam_bundle_result_t) && f->summary.result_cnt<16UL );
    fd_memcpy( &f->summary.results[f->summary.result_cnt++],
               fd_chunk_to_laddr_const( f->ctx->bam_out->mem, m->chunk ), sizeof(fd_bam_bundle_result_t) );
  }
  return held;
}

test_bam_poh_summary_t const *
test_bam_poh_fixture_summary( test_bam_poh_fixture_t const * f ) {
  return &f->summary;
}
