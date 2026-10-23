/* Helpers for the Motor BAM test, included after the tile.  Every output is unreliable, so publishing never touches
   credits. */

int volatile const fd_startup_skip_checks = 1; /* fd_startup.c */

static void *
test_dcache_new( fd_wksp_t * wksp,
                 ulong       depth,
                 ulong       mtu ) {
  ulong data_sz = fd_dcache_req_data_sz( mtu, depth, 1UL, 1 );
  void * mem = fd_wksp_alloc_laddr( wksp, fd_dcache_align(), fd_dcache_footprint( data_sz, 0UL ), 1UL );
  FD_TEST( mem );
  void * dcache = fd_dcache_join( fd_dcache_new( mem, data_sz, 0UL ) );
  FD_TEST( dcache );
  return dcache;
}

static fd_poh_out_t
test_out_new( fd_wksp_t * wksp,
              ulong       idx,
              ulong       depth,
              ulong       mtu ) {
  void * dcache = test_dcache_new( wksp, depth, mtu );
  ulong chunk0 = fd_dcache_compact_chunk0( wksp, dcache );
  return (fd_poh_out_t) {
    .idx    = idx,
    .mem    = wksp,
    .chunk0 = chunk0,
    .wmark  = fd_dcache_compact_wmark( wksp, dcache, mtu ),
    .chunk  = chunk0,
  };
}

typedef struct {
  fd_frag_meta_t *  mcaches[ 4 ];
  ulong             seqs[ 4 ];
  ulong             depths[ 4 ];
  int               out_reliable[ 4 ];
  fd_stem_context_t stem[1];
} test_stem_t;

static fd_stem_context_t *
test_stem_new( test_stem_t * s,
               fd_wksp_t *   wksp,
               ulong         depth ) {
  fd_memset( s, 0, sizeof(test_stem_t) );
  for( ulong i=0UL; i<4UL; i++ ) {
    void * mem = fd_wksp_alloc_laddr( wksp, fd_mcache_align(), fd_mcache_footprint( depth, 0UL ), 1UL );
    FD_TEST( mem );
    s->mcaches[ i ] = fd_mcache_join( fd_mcache_new( mem, depth, 0UL, 0UL ) );
    FD_TEST( s->mcaches[ i ] );
    s->depths[ i ] = depth;
  }
  *s->stem = (fd_stem_context_t){ .mcaches=s->mcaches, .seqs=s->seqs, .depths=s->depths, .out_reliable=s->out_reliable };
  return s->stem;
}

/* A provisional all-success BAM result, as execle attaches it. */

static fd_bam_bundle_result_t
test_result( uint   seq_id,
             ushort scheduler_gen,
             ulong  slot,
             uchar  txn_cnt ) {
  fd_bam_bundle_result_t result = fd_bam_result_base( seq_id, scheduler_gen, slot, txn_cnt );
  result.execution_success = 1U;
  fd_bam_result_mark_sanitize_success_all( &result );
  return result;
}

/* Writes an executed microblock of txn_cnt BAM transactions, with
   result attached if non-NULL, and returns its size. */

static ulong
test_microblock( void *                         fragment,
                 ulong                          slot,
                 uint                           seq_id,
                 ulong                          txn_cnt,
                 fd_bam_bundle_result_t const * result ) {
  fd_txn_p_t * txns = fragment;
  for( ulong i=0UL; i<txn_cnt; i++ ) {
    fd_txn_p_t * txn = txns+i;
    fd_memset( txn, 0, sizeof(fd_txn_p_t) );
    txn->payload_sz = FD_TXN_SIGNATURE_SZ;
    txn->source_tpu = FD_TXN_M_TPU_SOURCE_BAM;
    txn->flags      = FD_TXN_P_FLAGS_SANITIZE_SUCCESS | FD_TXN_P_FLAGS_EXECUTE_SUCCESS;
    txn->first_seen_nanos             = 123456789L;
    txn->scheduler_arrival_time_nanos = 987654321L;
    TXN(txn)->signature_off = 0U;
    fd_memset( txn->payload, (int)(seq_id+i), FD_TXN_SIGNATURE_SZ );
  }

  fd_microblock_trailer_t * trailer = (fd_microblock_trailer_t *)( txns+txn_cnt );
  fd_memset( trailer, 0, sizeof(fd_microblock_trailer_t) );
  trailer->hash[ 0 ] = (uchar)slot;
  if( result ) fd_bam_microblock_attach_result( trailer, result );
  return fd_bam_microblock_footprint( txn_cnt, !!result );
}
