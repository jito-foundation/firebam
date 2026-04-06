/* fd_sol_compat_bam.c — sol_compat ABI wrapper for BAM batch parse/validate.
   Implements sol_compat_bam_parse_batch_v1 which exercises the same structural
   checks as fd_bam_decode_batch + fd_bam_validate_batch without requiring a
   full fd_bam_tile_t context. */

#include "fd_sol_compat.h"
#include "generated/fd_sol_compat_bam.pb.h"
#include "../../../disco/bam/proto/bam_types.pb.h"
#include "../../../ballet/nanopb/pb_decode.h"
#include "../../../ballet/nanopb/pb_encode.h"
#include "../../../ballet/txn/fd_txn.h"

/* Maximum packets per atomic batch — mirrors FD_BAM_MAX_TXN_PER_ATOMIC_BATCH
   defined in fd_bam_client_decode.c (5).  Not included here to avoid pulling
   in the full tile private header. */
#define BAM_COMPAT_MAX_PKT 5U

/* Minimal packet-collection state; mirrors fd_bam_batch_ctx_t without the
   fd_bam_tile_t back-pointer or networking side-effects. */
typedef struct {
  bam_types_Packet packets[ BAM_COMPAT_MAX_PKT ];
  uchar            packet_cnt;
  uchar            revert_on_error;
  uchar            has_deser_err;
  uchar            deser_index;
  uchar            deser_reason;  /* bam_types_DeserializationErrorReason */
} bam_compat_state_t;

/* Nanopb repeated-field callback that collects packets into bam_compat_state_t.
   Mirrors fd_bam_collect_packet() in fd_bam_client_decode.c. */
static bool
bam_compat_collect_packet( pb_istream_t *     stream,
                            pb_field_t const * field,
                            void **            arg ) {
  (void)field;
  bam_compat_state_t * state = *arg;

  /* Max packet count check */
  if( FD_UNLIKELY( state->packet_cnt >= BAM_COMPAT_MAX_PKT ) ) {
    state->has_deser_err = 1;
    state->deser_reason  = (uchar)bam_types_DeserializationErrorReason_SANITIZE_ERROR;
    state->deser_index   = 0;
    return false;
  }

  bam_types_Packet packet = bam_types_Packet_init_default;
  if( FD_UNLIKELY( !pb_decode( stream, &bam_types_Packet_msg, &packet ) ) ) {
    state->has_deser_err = 1;
    state->deser_reason  = (uchar)bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
    state->deser_index   = state->packet_cnt;
    return false;
  }

  /* Check revert_on_error consistency across packets */
  uchar pkt_revert = 0;
  if( packet.has_meta && packet.meta.has_flags ) {
    pkt_revert = (uchar)packet.meta.flags.revert_on_error;
  }
  if( FD_UNLIKELY( state->packet_cnt && state->revert_on_error != pkt_revert ) ) {
    state->has_deser_err = 1;
    state->deser_reason  = (uchar)bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
    state->deser_index   = 0;
    return false;
  }
  state->revert_on_error = pkt_revert;

  /* Oversized packet check — mirrors FD_TXN_MTU guard in fd_bam_collect_packet */
  ulong declared_sz = fd_ulong_max( (ulong)packet.data.size, (ulong)packet.meta.size );
  if( FD_UNLIKELY( declared_sz > FD_TXN_MTU ) ) {
    state->has_deser_err = 1;
    state->deser_reason  = (uchar)bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
    state->deser_index   = state->packet_cnt;
    return false;
  }

  state->packets[ state->packet_cnt++ ] = packet;
  return true;
}

int
sol_compat_bam_parse_batch_v1( uchar *       out,
                               ulong *       out_sz,
                               uchar const * in,
                               ulong         in_sz ) {
  /* 1. Decode AtomicTxnBatch from raw bytes */
  bam_compat_state_t state;
  fd_memset( &state, 0, sizeof(state) );

  bam_types_AtomicTxnBatch batch = bam_types_AtomicTxnBatch_init_default;
  batch.packets = (pb_callback_t){
    .funcs.decode = bam_compat_collect_packet,
    .arg          = &state
  };

  pb_istream_t istream = pb_istream_from_buffer( in, in_sz );
  if( FD_UNLIKELY( !pb_decode( &istream, &bam_types_AtomicTxnBatch_msg, &batch ) ) ) {
    /* Top-level decode failed; use error from callback if already set */
    if( !state.has_deser_err ) {
      state.has_deser_err = 1;
      state.deser_reason  = (uchar)bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
      state.deser_index   = state.packet_cnt;
    }
  }

  /* 2. Validate — mirrors fd_bam_validate_batch() */
  fd_sol_compat_bam_BamParseResult result;
  fd_memset( &result, 0, sizeof(result) );

  if( state.has_deser_err ) {
    result.valid            = false;
    result.has_error        = true;
    result.error.reason     = (bam_types_DeserializationErrorReason)state.deser_reason;
    result.error.index      = (uint32_t)state.deser_index;
  } else if( FD_UNLIKELY( state.packet_cnt == 0 ) ) {
    result.valid            = false;
    result.has_error        = true;
    result.error.reason     = bam_types_DeserializationErrorReason_EMPTY;
    result.error.index      = 0;
  } else if( FD_UNLIKELY( !state.revert_on_error && state.packet_cnt > 1U ) ) {
    /* Non-revert-on-error batches must be exactly one packet */
    result.valid            = false;
    result.has_error        = true;
    result.error.reason     = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
    result.error.index      = 0;
  } else {
    /* Vote transaction check */
    int vote_idx = -1;
    uchar txn_buf[ FD_TXN_MAX_SZ ];
    for( uchar i=0; i<state.packet_cnt; i++ ) {
      bam_types_Packet const * pkt = &state.packets[ i ];
      if( FD_UNLIKELY( !fd_txn_parse( pkt->data.bytes, (ulong)pkt->data.size, txn_buf, NULL ) ) ) continue;
      if( FD_UNLIKELY( fd_txn_is_simple_vote_transaction( (fd_txn_t const *)txn_buf, pkt->data.bytes ) ) ) {
        vote_idx = (int)i;
        break;
      }
    }
    if( FD_UNLIKELY( vote_idx >= 0 ) ) {
      result.valid            = false;
      result.has_error        = true;
      result.error.reason     = bam_types_DeserializationErrorReason_VOTE_TRANSACTION_FAILURE;
      result.error.index      = (uint32_t)vote_idx;
    } else {
      result.valid = true;
    }
  }

  /* 3. Encode result into output buffer */
  pb_ostream_t ostream = pb_ostream_from_buffer( out, *out_sz );
  if( FD_UNLIKELY( !pb_encode( &ostream, &fd_sol_compat_bam_BamParseResult_msg, &result ) ) ) {
    return 0;
  }
  *out_sz = ostream.bytes_written;
  return 1;
}
