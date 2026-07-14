/* protobuf decoding for the BAM client. */

#include "fd_bam_tile_private.h"
#include "proto/bam_types.pb.h"
#include "../../ballet/base58/fd_base58.h"
#include "../../ballet/txn/fd_txn.h"
#include "../../ballet/nanopb/pb_decode.h"
#include "../../flamenco/runtime/fd_system_ids.h"
#include "../../waltz/h2/fd_h2_rbuf.h"

#include <stdarg.h>
#include <stdio.h>

#define FD_BAM_DUMP_ADDR_BUF_SZ                (FD_BASE58_ENCODED_32_SZ + 32UL)
#define FD_BAM_DUMP_PROGRAM_BUF_SZ             (FD_BAM_DUMP_ADDR_BUF_SZ + 32UL)
#define FD_BAM_DUMP_LOG_BUF_SZ                 (31UL*4096UL)
#define FD_BAM_DUMP_HEX_PREVIEW_MAX            24UL
#define FD_BAM_DUMP_IX_ACCT_PREVIEW_MAX        4UL
#define FD_BAM_DUMP_LUT_IDX_PREVIEW_MAX        8UL

static FD_TL char fd_bam_dump_log_buf[ FD_BAM_DUMP_LOG_BUF_SZ ];

static ulong
fd_bam_dump_appendf( char *       buf,
                     ulong        buf_sz,
                     ulong        off,
                     char const * fmt,
                     ... ) {
  if( FD_UNLIKELY( off>=buf_sz ) ) return buf_sz;

  va_list ap;
  va_start( ap, fmt );
  int written = vsnprintf( buf + off, (size_t)( buf_sz - off ), fmt, ap );
  va_end( ap );
  if( FD_UNLIKELY( written<0 ) ) return off;

  ulong written_ulong = (ulong)written;
  if( FD_UNLIKELY( written_ulong >= ( buf_sz - off ) ) ) return buf_sz - 1UL;
  return off + written_ulong;
}

static ulong
fd_bam_dump_append_hex_preview( char *        buf,
                                ulong         buf_sz,
                                ulong         off,
                                uchar const * data,
                                ulong         data_sz,
                                ulong         data_max ) {
  ulong shown = fd_ulong_min( data_sz, data_max );
  for( ulong i=0UL; i<shown; i++ ) {
    off = fd_bam_dump_appendf( buf, buf_sz, off, "%s%02x", i ? " " : "", (uint)data[ i ] );
  }
  if( FD_UNLIKELY( shown < data_sz ) ) off = fd_bam_dump_appendf( buf, buf_sz, off, " ... +%luB", data_sz-shown );
  return off;
}

static ulong
fd_bam_dump_append_idx_preview( char *        buf,
                                ulong         buf_sz,
                                ulong         off,
                                uchar const * idx,
                                ulong         idx_cnt,
                                ulong         idx_max ) {
  ulong shown = fd_ulong_min( idx_cnt, idx_max );
  off = fd_bam_dump_appendf( buf, buf_sz, off, "[" );
  for( ulong i=0UL; i<shown; i++ ) {
    off = fd_bam_dump_appendf( buf, buf_sz, off, "%s%u", i ? ", " : "", (uint)idx[ i ] );
  }
  if( FD_UNLIKELY( shown < idx_cnt ) ) off = fd_bam_dump_appendf( buf, buf_sz, off, ", ... +%lu", idx_cnt-shown );
  return fd_bam_dump_appendf( buf, buf_sz, off, "]" );
}

static ulong
fd_bam_dump_append_inbound_txn( char *                   msg,
                                ulong                    msg_sz,
                                ulong                    off,
                                uchar                    batch_idx,
                                uchar                    batch_cnt,
                                bam_types_Packet const * packet ) {
  uchar const * payload    = packet->data.bytes;
  uint          txn_ord    = (uint)batch_idx + 1U;

  uchar txn_buf[ FD_TXN_MAX_SZ ];
  if( FD_UNLIKELY( !fd_txn_parse( payload, packet->data.size, txn_buf, NULL ) ) ) {
    off = fd_bam_dump_appendf( msg, msg_sz, off,
                               "  txn[%u/%u]: batch_idx=%u payload_sz=%u parse=failed\n",
                               txn_ord, (uint)batch_cnt,
                               (uint)batch_idx,
                               packet->data.size );
    off = fd_bam_dump_appendf( msg, msg_sz, off, "    bytes: " );
    off = fd_bam_dump_append_hex_preview( msg, msg_sz, off,
                                          payload, packet->data.size, FD_BAM_DUMP_HEX_PREVIEW_MAX );
    return fd_bam_dump_appendf( msg, msg_sz, off, "\n" );
  }

  fd_txn_t const * txn  = (fd_txn_t const *)txn_buf;
  fd_ed25519_sig_t const * sigs = fd_txn_get_signatures( txn, payload );
  fd_acct_addr_t const *  keys = fd_txn_get_acct_addrs( txn, payload );
  ulong signer_cnt   = fd_txn_account_cnt( txn, FD_TXN_ACCT_CAT_SIGNER   );
  ulong writable_cnt = fd_txn_account_cnt( txn, FD_TXN_ACCT_CAT_WRITABLE );
  ulong readonly_cnt = fd_txn_account_cnt( txn, FD_TXN_ACCT_CAT_READONLY );

  char fee_payer[ FD_BASE58_ENCODED_32_SZ ] = "<none>";
  if( FD_LIKELY( txn->acct_addr_cnt ) ) fd_base58_encode_32( keys[ 0 ].b, NULL, fee_payer );

  char recent_blockhash[ FD_BASE58_ENCODED_32_SZ ];
  fd_base58_encode_32( fd_txn_get_recent_blockhash( txn, payload ), NULL, recent_blockhash );

  char primary_sig[ FD_BASE58_ENCODED_64_SZ ] = "<none>";
  if( FD_LIKELY( txn->signature_cnt ) ) fd_base58_encode_64( sigs[ 0 ], NULL, primary_sig );

  off = fd_bam_dump_appendf( msg, msg_sz, off,
                             "  txn[%u/%u]: batch_idx=%u payload_sz=%u version=%s sig=%s fee_payer=%s recent_blockhash=%s instructions=%u accounts=%lu (static=%u loaded=%u signers=%lu writable=%lu readonly=%lu luts=%u)%s\n",
                             txn_ord, (uint)batch_cnt,
                             (uint)batch_idx,
                             packet->data.size,
                             txn->transaction_version==FD_TXN_V0 ? "v0" : "legacy",
                             primary_sig,
                             fee_payer,
                             recent_blockhash,
                             (uint)txn->instr_cnt,
                             (ulong)txn->acct_addr_cnt + (ulong)txn->addr_table_adtl_cnt,
                             (uint)txn->acct_addr_cnt,
                             (uint)txn->addr_table_adtl_cnt,
                             signer_cnt,
                             writable_cnt,
                             readonly_cnt,
                             (uint)txn->addr_table_lookup_cnt,
                             fd_txn_is_simple_vote_transaction( txn, payload ) ? " simple_vote=1" : "" );

  off = fd_bam_dump_appendf( msg, msg_sz, off, "    signatures:" );
  for( ulong i=0UL; i<txn->signature_cnt; i++ ) {
    char sig_b58[ FD_BASE58_ENCODED_64_SZ ];
    fd_base58_encode_64( sigs[ i ], NULL, sig_b58 );
    off = fd_bam_dump_appendf( msg, msg_sz, off, " [%lu]=%s", i, sig_b58 );
  }
  if( FD_UNLIKELY( !txn->signature_cnt ) ) off = fd_bam_dump_appendf( msg, msg_sz, off, " none" );
  off = fd_bam_dump_appendf( msg, msg_sz, off, "\n" );

  if( txn->transaction_version==FD_TXN_V0 && txn->addr_table_lookup_cnt ) {
    fd_txn_acct_addr_lut_t const * luts = fd_txn_get_address_tables_const( txn );
    for( ulong i=0UL; i<txn->addr_table_lookup_cnt; i++ ) {
      fd_acct_addr_t const * addr = (fd_acct_addr_t const *)( (ulong)payload + (ulong)luts[i].addr_off );
      char addr_b58[ FD_BASE58_ENCODED_32_SZ ];
      fd_base58_encode_32( addr->b, NULL, addr_b58 );
      uchar const * w_idx = (uchar const *)( (ulong)payload + (ulong)luts[i].writable_off );
      uchar const * r_idx = (uchar const *)( (ulong)payload + (ulong)luts[i].readonly_off );
      off = fd_bam_dump_appendf( msg, msg_sz, off,
                                 "    lut[%lu]: table=%s writable=",
                                 i,
                                 addr_b58 );
      off = fd_bam_dump_append_idx_preview( msg, msg_sz, off,
                                            w_idx, (ulong)luts[ i ].writable_cnt, FD_BAM_DUMP_LUT_IDX_PREVIEW_MAX );
      off = fd_bam_dump_appendf( msg, msg_sz, off, " readonly=" );
      off = fd_bam_dump_append_idx_preview( msg, msg_sz, off,
                                            r_idx, (ulong)luts[ i ].readonly_cnt, FD_BAM_DUMP_LUT_IDX_PREVIEW_MAX );
      off = fd_bam_dump_appendf( msg, msg_sz, off, "\n" );
    }
  }

  for( ulong ix_idx=0UL; ix_idx<(ulong)txn->instr_cnt; ix_idx++ ) {
    fd_txn_instr_t const * instr = &txn->instr[ ix_idx ];
    uchar const * acct_idx = fd_txn_get_instr_accts( instr, payload );
    ulong ix_signer_cnt   = 0UL;
    ulong ix_writable_cnt = 0UL;
    ulong ix_lookup_cnt   = 0UL;
    for( ulong j=0UL; j<instr->acct_cnt; j++ ) {
      uchar acct = acct_idx[ j ];
      ix_signer_cnt   += (ulong)!!fd_txn_is_signer  ( txn, (int)acct );
      ix_writable_cnt += (ulong)!!fd_txn_is_writable( txn,        acct );
      ix_lookup_cnt   += (ulong)( acct >= txn->acct_addr_cnt );
    }

    char prog_descr[ FD_BAM_DUMP_PROGRAM_BUF_SZ ];
    if( FD_LIKELY( instr->program_id < txn->acct_addr_cnt ) ) {
      fd_base58_encode_32( keys[ instr->program_id ].b, NULL, prog_descr );
      fd_pubkey_t const * progkey = (fd_pubkey_t const *)&keys[ instr->program_id ];
      char const * label = NULL;
      if     ( fd_pubkey_eq( progkey, &fd_solana_system_program_id                 ) ) label = "system";
      else if( fd_pubkey_eq( progkey, &fd_solana_vote_program_id                   ) ) label = "vote";
      else if( fd_pubkey_eq( progkey, &fd_solana_stake_program_id                  ) ) label = "stake";
      else if( fd_pubkey_eq( progkey, &fd_solana_config_program_id                 ) ) label = "config";
      else if( fd_pubkey_eq( progkey, &fd_solana_bpf_loader_deprecated_program_id  ) ) label = "bpf_loader_deprecated";
      else if( fd_pubkey_eq( progkey, &fd_solana_bpf_loader_program_id             ) ) label = "bpf_loader";
      else if( fd_pubkey_eq( progkey, &fd_solana_bpf_loader_upgradeable_program_id ) ) label = "bpf_loader_upgradeable";
      else if( fd_pubkey_eq( progkey, &fd_solana_bpf_loader_v4_program_id          ) ) label = "bpf_loader_v4";
      else if( fd_pubkey_eq( progkey, &fd_solana_compute_budget_program_id         ) ) label = "compute_budget";
      else if( fd_pubkey_eq( progkey, &fd_solana_address_lookup_table_program_id   ) ) label = "address_lookup_table";
      else if( fd_pubkey_eq( progkey, &fd_solana_spl_token_id                      ) ) label = "spl_token";
      else if( fd_pubkey_eq( progkey, &fd_solana_ed25519_sig_verify_program_id     ) ) label = "ed25519_sigverify";
      else if( fd_pubkey_eq( progkey, &fd_solana_keccak_secp_256k_program_id       ) ) label = "keccak_secp256k1";
      else if( fd_pubkey_eq( progkey, &fd_solana_secp256r1_program_id              ) ) label = "secp256r1";
      if( FD_LIKELY( label ) ) fd_bam_dump_appendf( prog_descr, FD_BAM_DUMP_PROGRAM_BUF_SZ, strlen( prog_descr ), " [%s]", label );
    } else {
      snprintf( prog_descr, FD_BAM_DUMP_PROGRAM_BUF_SZ, "<lookup acct[%u]>", (uint)instr->program_id );
    }
    off = fd_bam_dump_appendf( msg, msg_sz, off,
                               "    ix[%lu]: program_id_index=%u program=%s acct_cnt=%u signer_cnt=%lu writable_cnt=%lu lookup_cnt=%lu data_sz=%u",
                               ix_idx,
                               (uint)instr->program_id,
                               prog_descr,
                               (uint)instr->acct_cnt,
                               ix_signer_cnt,
                               ix_writable_cnt,
                               ix_lookup_cnt,
                               (uint)instr->data_sz );

    uchar const * data = fd_txn_get_instr_data( instr, payload );
    if( FD_LIKELY( instr->data_sz ) ) {
      off = fd_bam_dump_appendf( msg, msg_sz, off, " data=" );
      off = fd_bam_dump_append_hex_preview( msg, msg_sz, off,
                                            data, (ulong)instr->data_sz, FD_BAM_DUMP_HEX_PREVIEW_MAX );
    }
    off = fd_bam_dump_appendf( msg, msg_sz, off, "\n" );

    if( FD_LIKELY( instr->acct_cnt ) ) {
      ulong shown = fd_ulong_min( (ulong)instr->acct_cnt, FD_BAM_DUMP_IX_ACCT_PREVIEW_MAX );
      off = fd_bam_dump_appendf( msg, msg_sz, off, "    accts: [" );
      for( ulong j=0UL; j<shown; j++ ) {
        uchar acct = acct_idx[ j ];
        char acct_descr[ FD_BAM_DUMP_ADDR_BUF_SZ ];
        if( FD_LIKELY( acct < txn->acct_addr_cnt ) ) fd_base58_encode_32( keys[ acct ].b, NULL, acct_descr );
        else                                         snprintf( acct_descr, FD_BAM_DUMP_ADDR_BUF_SZ, "<lookup acct[%u]>", (uint)acct );

        char acct_flags[ 4 ];
        acct_flags[ 0 ] = fd_txn_is_signer  ( txn, (int)acct ) ? 's' : '-';
        acct_flags[ 1 ] = fd_txn_is_writable( txn,        acct ) ? 'w' : 'r';
        acct_flags[ 2 ] = acct < txn->acct_addr_cnt             ? 'i' : 'l';
        acct_flags[ 3 ] = '\0';

        off = fd_bam_dump_appendf( msg, msg_sz, off, "%s%lu=%u/%s/%s",
                                   j ? ", " : "",
                                   j,
                                   (uint)acct,
                                   acct_flags,
                                   acct_descr );
      }
      if( FD_UNLIKELY( shown < (ulong)instr->acct_cnt ) )
        off = fd_bam_dump_appendf( msg, msg_sz, off, ", ... +%u more", (uint)instr->acct_cnt - (uint)shown );
      off = fd_bam_dump_appendf( msg, msg_sz, off, "]\n" );
    }
  }

  return off;
}

int
fd_bam_should_dump_batch( fd_bam_tile_t * ctx,
                          ulong           max_schedule_slot ) {
  if( FD_UNLIKELY( ctx->dump_bam_mode==FD_BAM_DEBUG_DUMP_MODE_ALL ) ) return 1;
  if( FD_LIKELY( ctx->dump_bam_mode!=FD_BAM_DEBUG_DUMP_MODE_SLOT_FIRST ) ) return 0;

  if( FD_LIKELY( ctx->dump_bam_last_slot_valid && ctx->dump_bam_last_slot==max_schedule_slot ) ) return 0;

  ctx->dump_bam_last_slot       = max_schedule_slot;
  ctx->dump_bam_last_slot_valid = 1U;
  return 1;
}

/* Collects a single Packet from the protobuf stream. Returns true while the
   packet parsed and passed basic validation; returns false to abort the
   surrounding AtomicTxnBatch decode and surface the bundle error. */
static bool
fd_bam_collect_packet( pb_istream_t *         stream,
                       pb_field_t const *     field,
                       void **                arg ) {
  (void)field;
  fd_bam_batch_ctx_t * state = *arg;
  if( FD_UNLIKELY( state->packet_cnt >= FD_BAM_MAX_TXN_PER_ATOMIC_BATCH ) ) {
    FD_LOG_WARNING(( "Received AtomicTxnBatch exceeding max bundle size, already have %u txns", state->packet_cnt ));
    state->has_deser_err   = true;
    state->deser_reason    = bam_types_DeserializationErrorReason_SANITIZE_ERROR;
    state->deser_index     = 0U;
    return false;
  }

  bam_types_Packet packet = bam_types_Packet_init_default;
  if( FD_UNLIKELY( !pb_decode( stream, &bam_types_Packet_msg, &packet ) ) ) {
    state->has_deser_err        = true;
    state->packet_decode_failed = true;
    state->deser_reason         = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
    state->deser_index          = state->packet_cnt;
    return false;
  }

  _Bool packet_revert_on_error = 0;
  if( packet.has_meta && packet.meta.has_flags ) {
    packet_revert_on_error = !!packet.meta.flags.revert_on_error;
  }

  if( FD_UNLIKELY( state->packet_cnt && state->revert_on_error != packet_revert_on_error ) ) {
    FD_LOG_WARNING(( "AtomicTxnBatch contains mixed revert_on_error flags" ));
    state->has_deser_err = true;
    state->deser_reason  = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
    state->deser_index   = 0U;
    return false;
  }
  state->revert_on_error = packet_revert_on_error;

  ulong payload_sz = packet.data.size;
  if( FD_UNLIKELY( payload_sz > FD_TXN_MTU ) ) {
    state->ctx->metrics.ingress_packet_oversize_cnt++;
    FD_LOG_WARNING(( "Received AtomicTxnBatch packet exceeding MTU (%lu>%lu); dropping batch",
                     payload_sz, FD_TXN_MTU ));
    state->has_deser_err = true;
    state->deser_reason = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
    state->deser_index  = state->packet_cnt;
    return false;
  }

  state->packets[ state->packet_cnt ] = packet;
  state->packet_cnt++;
  return true;
}

static fd_bam_slot_ingress_timing_t *
fd_bam_record_batch_ingress_timing( fd_bam_tile_t *            ctx,
                                    fd_bam_batch_ctx_t const * state,
                                    ulong                      max_schedule_slot );

static _Bool
fd_bam_validate_batch( fd_bam_tile_t *                  ctx,
                       fd_bam_batch_ctx_t const *       state,
                       bam_types_AtomicTxnBatch const * batch ) {
  if( FD_UNLIKELY( ctx->bam_leader_state.slot!=ULONG_MAX &&
                   batch->max_schedule_slot<ctx->bam_leader_state.slot ) ) {
    /* Stale scheduler work still contributes to slot-ingress timing for the
       hinted slot even though it will never be published.  This preserves the
       receive-time accounting used by BAM ingress-vs-slot-end metrics. */
    fd_bam_record_batch_ingress_timing( ctx, state, batch->max_schedule_slot );
    fd_bam_enqueue_result( ctx, &(fd_bam_bundle_result_t) {
      .seq_id            = batch->seq_id,
      .scheduler_gen     = ctx->scheduler_gen,
      .slot              = batch->max_schedule_slot,
      .bundle_txn_cnt    = state->packet_cnt,
      .execution_success = 0,
      .scheduling_error  = FD_BAM_SCHED_ERR_OUTSIDE_SLOT,
      .bundle_err        = FD_BAM_BUNDLE_ERR_NONE,
    } );
    return 0;
  }

  if( FD_UNLIKELY( state->has_deser_err ) ) {
    ctx->metrics.ingress_batch_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_INVALID_BATCH_IDX ]++;
    fd_bam_enqueue_result( ctx, &(fd_bam_bundle_result_t) {
      .seq_id            = batch->seq_id,
      .scheduler_gen     = ctx->scheduler_gen,
      .slot              = batch->max_schedule_slot,
      .bundle_txn_cnt    = state->packet_cnt,
      .execution_success = 0,
      .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
      .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
      .deser_reason      = state->deser_reason,
      .deser_index       = state->deser_index
    } );
    return 0;
  }

  if( FD_UNLIKELY( state->packet_cnt == 0U ) ) {
    ctx->metrics.ingress_batch_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_EMPTY_BATCH_IDX ]++;
    fd_bam_enqueue_result( ctx, &(fd_bam_bundle_result_t) {
      .seq_id            = batch->seq_id,
      .scheduler_gen     = ctx->scheduler_gen,
      .slot              = batch->max_schedule_slot,
      .bundle_txn_cnt    = state->packet_cnt,
      .execution_success = 0,
      .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
      .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
      .deser_reason      = bam_types_DeserializationErrorReason_EMPTY,
      .deser_index       = 0
    } );
    return 0;
  }

  int simple_vote_idx = -1;
  uchar txn_buf[ FD_TXN_MAX_SZ ];
  for( uchar i=0U; i<state->packet_cnt; i++ ) {
    bam_types_Packet const * packet = &state->packets[ i ];
    if( FD_UNLIKELY( !fd_txn_parse( packet->data.bytes, packet->data.size, txn_buf, NULL ) ) ) continue;
    if( FD_UNLIKELY( fd_txn_is_simple_vote_transaction( (fd_txn_t const *)txn_buf, packet->data.bytes ) ) ) {
      simple_vote_idx = (int)i;
      break;
    }
  }
  if( FD_UNLIKELY( simple_vote_idx >= 0 ) ) {
    ctx->metrics.ingress_batch_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_VOTE_TRANSACTION_IDX ]++;
    fd_bam_enqueue_result( ctx, &(fd_bam_bundle_result_t) {
      .seq_id            = batch->seq_id,
      .scheduler_gen     = ctx->scheduler_gen,
      .slot              = batch->max_schedule_slot,
      .bundle_txn_cnt    = state->packet_cnt,
      .execution_success = 0,
      .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
      .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
      .deser_reason      = bam_types_DeserializationErrorReason_VOTE_TRANSACTION_FAILURE,
      .deser_index       = (uchar)simple_vote_idx,
    } );
    return 0;
  }

  return 1;
}

static fd_bam_slot_ingress_timing_t *
fd_bam_record_batch_ingress_timing( fd_bam_tile_t *            ctx,
                                    fd_bam_batch_ctx_t const * state,
                                    ulong                      max_schedule_slot ) {
  ulong leader_slot = ctx->bam_leader_state.slot;
  long  rx_ts_ns    = state->ingress_rx_ts_ns;
  long  slot_end_ns = state->ingress_slot_end_ns;
  uchar packet_cnt  = state->packet_cnt;

  if( FD_UNLIKELY( !packet_cnt ) ) return NULL;

  if( FD_UNLIKELY( !slot_end_ns && max_schedule_slot==leader_slot ) ) {
    slot_end_ns = ctx->bam_leader_state.slot_end_ns;
  }

  fd_bam_slot_ingress_timing_t * entry =
      fd_bam_slot_ingress_timing_query_or_insert( ctx, max_schedule_slot, leader_slot );
  if( FD_UNLIKELY( !entry ) ) return NULL;

  if( FD_UNLIKELY( slot_end_ns ) ) {
    entry->slot_end_ns = slot_end_ns;
    entry->first_rx_after_slot_end = (uchar)( entry->first_rx_ts_ns > slot_end_ns );
  }

  _Bool have_slot_end = !!entry->slot_end_ns;
  _Bool first_observation = !( entry->txn_before_slot_end |
                               entry->txn_after_slot_end  |
                               entry->txn_unknown_slot_end );
  _Bool after_slot_end = have_slot_end
    ? rx_ts_ns > entry->slot_end_ns
    : !!( leader_slot!=ULONG_MAX && max_schedule_slot < leader_slot );
  if( FD_UNLIKELY( first_observation ) ) {
    entry->first_rx_ts_ns          = rx_ts_ns;
    entry->first_rx_after_slot_end = (uchar)after_slot_end;
  }

  uint * txn_bucket = &entry->txn_unknown_slot_end;
  if( FD_LIKELY( have_slot_end ) ) {
    txn_bucket = after_slot_end ? &entry->txn_after_slot_end : &entry->txn_before_slot_end;
  }
  *txn_bucket = fd_uint_sat_add( *txn_bucket, (uint)packet_cnt );

  return entry;
}

void
fd_bam_publish_batch( fd_bam_tile_t *            ctx,
                      fd_bam_batch_ctx_t *       state,
                      bam_types_AtomicTxnBatch const * batch ) {
  ulong max_schedule_slot = batch->max_schedule_slot;
  uchar packet_cnt = state->packet_cnt;
  uint scheduler_arrival_tspub = state->ingress_rx_tspub;
  ulong leader_slot = ctx->bam_leader_state.slot;
  fd_bam_slot_ingress_timing_t * entry =
      fd_bam_record_batch_ingress_timing( ctx, state, max_schedule_slot );

  if( FD_UNLIKELY( fd_bam_should_dump_batch( ctx, max_schedule_slot ) ) ) {
    char * msg = fd_bam_dump_log_buf;
    ulong  off = 0UL;
    fd_bam_slot_ingress_timing_t const empty_entry = {0};
    fd_bam_slot_ingress_timing_t const * timing = entry ? entry : &empty_entry;
    long   first_rx_ts_ns = timing->first_rx_ts_ns;
    long   tracked_slot_end_ns = timing->slot_end_ns ? timing->slot_end_ns : state->ingress_slot_end_ns;
    /* Debug-dump fallback only: preserve the pre-refactor log behavior for
       same-slot batches when no timing entry was recorded. This does not feed
       scheduling or metrics, only first_rx_minus_slot_end_ns in the dump. */
    if( FD_UNLIKELY( !tracked_slot_end_ns && max_schedule_slot==leader_slot ) ) tracked_slot_end_ns = ctx->bam_leader_state.slot_end_ns;
    long   first_rx_minus_slot_end_ns = 0L;
    ulong  txn_before_slot_end = timing->txn_before_slot_end;
    ulong  txn_after_slot_end = timing->txn_after_slot_end;
    ulong  txn_unknown_slot_end = timing->txn_unknown_slot_end;
    uint   first_rx_after_slot_end = (uint)timing->first_rx_after_slot_end;
    if( FD_UNLIKELY( first_rx_ts_ns && tracked_slot_end_ns ) ) first_rx_minus_slot_end_ns = first_rx_ts_ns - tracked_slot_end_ns;

    /* Emit one NOTICE record per bundle so unrelated logs cannot split txn details apart. */
    off = fd_bam_dump_appendf( msg, FD_BAM_DUMP_LOG_BUF_SZ, off,
                               "BAM rx bundle: seq_id=%u max_schedule_slot=%lu txns=%u mode=%s\n",
                               batch->seq_id,
                               batch->max_schedule_slot,
                               (uint)packet_cnt,
                               state->revert_on_error ? "atomic" : "independent" );
    for( uchar i=0U; i<packet_cnt; i++ ) {
      if( FD_UNLIKELY( i ) ) off = fd_bam_dump_appendf( msg, FD_BAM_DUMP_LOG_BUF_SZ, off, "\n" );
      off = fd_bam_dump_append_inbound_txn( msg,
                                            FD_BAM_DUMP_LOG_BUF_SZ,
                                            off,
                                            i,
                                            packet_cnt,
                                            &state->packets[ i ] );
    }
    off = fd_bam_dump_appendf( msg, FD_BAM_DUMP_LOG_BUF_SZ, off,
                               "\nfiredancer_slot_timing: max_schedule_slot=%lu first_rx_ns=%ld first_rx_minus_slot_end_ns=%ld first_rx_after_slot_end=%u txns_before_slot_end=%lu txns_after_slot_end=%lu txns_unknown_slot_end=%lu current_leader_slot=%lu",
                               max_schedule_slot,
                               first_rx_ts_ns,
                               first_rx_minus_slot_end_ns,
                               first_rx_after_slot_end,
                               txn_before_slot_end,
                               txn_after_slot_end,
                               txn_unknown_slot_end,
                               leader_slot );
    FD_LOG_NOTICE(( "%s", msg ));
  }

  if( FD_UNLIKELY( bam_pending_txn_avail( ctx->pending_txns ) < (ulong)packet_cnt ) ) {
    ctx->metrics.transaction_rejected_backpressure_cnt += packet_cnt;
    fd_bam_enqueue_result( ctx, &(fd_bam_bundle_result_t) {
      .seq_id            = batch->seq_id,
      .scheduler_gen     = ctx->scheduler_gen,
      .slot              = batch->max_schedule_slot,
      .bundle_txn_cnt    = packet_cnt,
      .execution_success = 0,
      .scheduling_error  = FD_BAM_SCHED_ERR_CONTAINER_FULL,
      .bundle_err        = FD_BAM_BUNDLE_ERR_NONE,
    } );
    return;
  }

  for( uchar i=0; i<packet_cnt; i++ ) {
    bam_types_Packet const * packet = &state->packets[ i ];
    fd_bam_pending_txn_t * pending = bam_pending_txn_push_tail_nocopy( ctx->pending_txns );
    pending->payload_sz                  = (ushort)packet->data.size;
    pending->seq_id                      = batch->seq_id;
    pending->scheduler_arrival_tspub     = scheduler_arrival_tspub;
    pending->source_ipv4                 = 0U;
    pending->max_schedule_slot           = max_schedule_slot;
    pending->batch_idx                   = i;
    pending->batch_cnt                   = packet_cnt;
    pending->revert_on_error             = (uchar)state->revert_on_error;
    fd_memcpy( pending->payload, packet->data.bytes, packet->data.size );
  }
}

typedef struct {
  uint  seq_id;
  ulong max_schedule_slot;
  uchar has_seq_id;
  uchar has_max_schedule_slot;
} fd_bam_batch_identity_t;

typedef struct {
  uchar deser_reason;
  uchar deser_index;
  uchar packet_cnt;
} fd_bam_batch_decode_err_t;

static fd_bam_bundle_result_t
fd_bam_deser_result_from_identity( fd_bam_tile_t *                    ctx,
                                   fd_bam_batch_identity_t const *    identity,
                                   fd_bam_batch_decode_err_t const *  err ) {
  return (fd_bam_bundle_result_t) {
    .seq_id            = identity->seq_id,
    .scheduler_gen     = ctx->scheduler_gen,
    .slot              = identity->has_max_schedule_slot ? identity->max_schedule_slot : 0UL,
    .bundle_txn_cnt    = err->packet_cnt,
    .execution_success = 0,
    .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
    .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
    .deser_reason      = err->deser_reason,
    .deser_index       = err->deser_index
  };
}

/* Decodes one bam_types.AtomicTxnBatch message into staged state only.
   This function never publishes transactions or enqueues terminal results.
   Returns 1 when the batch was fully consumed and 0 when protobuf decoding
   failed. */
static _Bool
fd_bam_decode_batch( fd_bam_tile_t *          ctx,
                     pb_istream_t *           stream,
                     long                     rx_ts_ns,
                     uint                     rx_tspub,
                     ulong                    leader_slot_at_rx,
                     long                     leader_slot_end_ns_at_rx,
                     bam_types_AtomicTxnBatch * batch,
                     fd_bam_batch_ctx_t *       state,
                     fd_bam_batch_decode_err_t * err ) {
  *err = (fd_bam_batch_decode_err_t){0};
  fd_memset( state, 0, sizeof(fd_bam_batch_ctx_t) );
  state->ctx          = ctx;
  state->deser_reason = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
  state->deser_index  = 0U;
  *batch = (bam_types_AtomicTxnBatch)bam_types_AtomicTxnBatch_init_default;
  batch->packets = (pb_callback_t){
    .funcs.decode = fd_bam_collect_packet,
    .arg          = state
  };

  if( FD_UNLIKELY( !pb_decode( stream, &bam_types_AtomicTxnBatch_msg, batch ) ) ) {
    err->deser_reason = state->has_deser_err
                      ? state->deser_reason
                      : (uchar)bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
    err->deser_index  = state->has_deser_err
                      ? state->deser_index
                      : state->packet_cnt;
    err->packet_cnt   = state->packet_cnt;
    ctx->metrics.ingress_batch_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_INVALID_BATCH_IDX ]++;
    FD_LOG_WARNING(( "Protobuf decode of (bam_types.AtomicTxnBatch) failed (%s)", PB_GET_ERROR( stream ) ));
    return 0;
  }

  state->ingress_rx_ts_ns    = rx_ts_ns;
  state->ingress_rx_tspub    = rx_tspub;
  state->ingress_slot_end_ns =
      batch->max_schedule_slot==leader_slot_at_rx
      ? leader_slot_end_ns_at_rx
      : 0L;

  return 1;
}

/* Decodes a bam_types.MultipleAtomicTxnBatch wrapper into staged state.
   Returns 1 once the wrapper bytes were consumed, including handled per-batch
   rejects. Returns 0 on wrapper protobuf failures. No transaction publish
   occurs in this stage. */
static int
fd_bam_decode_multiple_atomic_txn_batch( fd_bam_tile_t * ctx,
                                         pb_istream_t *   stream,
                                         long            rx_ts_ns,
                                         uint            rx_tspub,
                                         ulong           leader_slot_at_rx,
                                         long            leader_slot_end_ns_at_rx,
                                         fd_bam_decoded_multi_batch_t * decoded_multi ) {
  fd_memset( decoded_multi, 0, sizeof(fd_bam_decoded_multi_batch_t) );

  fd_bam_metrics_t const metrics_before = ctx->metrics;
  ushort const results_head_before      = ctx->bam_results_head;
  ushort const results_tail_before      = ctx->bam_results_tail;
  ushort const feedback_depth_before    = ctx->feedback_queue_depth;

#define FD_BAM_MULTI_DECODE_FAIL() do {              \
    ctx->metrics              = metrics_before;       \
    ctx->bam_results_head     = results_head_before;  \
    ctx->bam_results_tail     = results_tail_before;  \
    ctx->feedback_queue_depth = feedback_depth_before;\
    return 0;                                         \
  } while(0)

  uint32_t       tag;
  pb_wire_type_t wire_type;
  bool           eof = false;
  uint           seen_batch_count = 0U;
  uint           batch_cnt = 0U;

  while( pb_decode_tag( stream, &wire_type, &tag, &eof ) ) {
    if( FD_UNLIKELY( !tag ) ) {
      PB_SET_ERROR( stream, "zero tag" );
      FD_BAM_MULTI_DECODE_FAIL();
    }
    if( FD_UNLIKELY( tag != bam_types_MultipleAtomicTxnBatch_batches_tag ) ) {
      PB_SET_ERROR( stream, "unexpected tag" );
      FD_BAM_MULTI_DECODE_FAIL();
    }
    if( FD_UNLIKELY( wire_type != PB_WT_STRING ) ) {
      PB_SET_ERROR( stream, "wrong wire type" );
      FD_BAM_MULTI_DECODE_FAIL();
    }

    pb_istream_t substream;
    if( FD_UNLIKELY( !pb_make_string_substream( stream, &substream ) ) ) FD_BAM_MULTI_DECODE_FAIL();

    fd_bam_batch_identity_t identity = {0};
    char const *            identity_err = NULL;
    pb_istream_t identity_stream = pb_istream_from_buffer( (uchar const *)substream.state, substream.bytes_left );
    uint32_t       identity_tag;
    pb_wire_type_t identity_wire_type;
    bool           identity_eof = false;
    while( pb_decode_tag( &identity_stream, &identity_wire_type, &identity_tag, &identity_eof ) ) {
      if( FD_UNLIKELY( !identity_tag ) ) {
        PB_SET_ERROR( (&identity_stream), "zero tag" );
        break;
      }

      if( identity_tag==bam_types_AtomicTxnBatch_seq_id_tag ) {
        if( FD_UNLIKELY( identity_wire_type!=PB_WT_VARINT ) ) {
          PB_SET_ERROR( (&identity_stream), "wrong seq_id wire type" );
          break;
        }
        uint64_t val = 0UL;
        if( FD_UNLIKELY( !pb_decode_varint( &identity_stream, &val ) ) ) break;
        uint32_t seq_id = (uint32_t)val;
        if( FD_UNLIKELY( (uint64_t)seq_id != val ) ) {
          PB_SET_ERROR( (&identity_stream), "seq_id too large" );
          break;
        }
        identity.seq_id     = (uint)seq_id;
        identity.has_seq_id = 1U;
        continue;
      }

      if( identity_tag==bam_types_AtomicTxnBatch_max_schedule_slot_tag ) {
        if( FD_UNLIKELY( identity_wire_type!=PB_WT_VARINT ) ) {
          PB_SET_ERROR( (&identity_stream), "wrong max_schedule_slot wire type" );
          break;
        }
        uint64_t val = 0UL;
        if( FD_UNLIKELY( !pb_decode_varint( &identity_stream, &val ) ) ) break;
        identity.max_schedule_slot     = (ulong)val;
        identity.has_max_schedule_slot = 1U;
        continue;
      }

      if( FD_UNLIKELY( !pb_skip_field( &identity_stream, identity_wire_type ) ) ) break;
    }

    _Bool const identity_ok = !!identity_eof;
    if( FD_UNLIKELY( !identity_ok ) ) identity_err = PB_GET_ERROR( &identity_stream );

    if( FD_UNLIKELY( seen_batch_count >= FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE ) ) {
      if( FD_UNLIKELY( !pb_close_string_substream( stream, &substream ) ) ) FD_BAM_MULTI_DECODE_FAIL();

      if( FD_UNLIKELY( seen_batch_count==FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE ) ) {
        FD_LOG_WARNING(( "MultipleAtomicTxnBatch exceeded max batch count (%u>%u)",
                         seen_batch_count + 1U,
                         FD_BAM_MAX_ATOMIC_BATCHES_PER_MESSAGE ));
        ctx->metrics.ingress_message_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_MESSAGE_REJECT_REASON_V_OVERFLOW_MESSAGE_IDX ]++;
      }
      if( FD_UNLIKELY( !identity.has_seq_id && !identity_ok ) ) {
        ctx->metrics.ingress_batch_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_INVALID_BATCH_IDX ]++;
        FD_LOG_WARNING(( "Unable to attribute overflow AtomicTxnBatch (%s)",
                         identity_err ? identity_err : "missing seq_id" ));
        seen_batch_count++;
        continue;
      }
      if( FD_UNLIKELY( !identity_ok ) ) {
        ctx->metrics.ingress_batch_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_BATCH_REJECT_REASON_V_INVALID_BATCH_IDX ]++;
        FD_LOG_WARNING(( "Overflow AtomicTxnBatch was malformed after seq_id (%s)",
                         identity_err ? identity_err : "unknown" ));
      }
      fd_bam_batch_decode_err_t overflow_err = {
        .deser_reason = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE,
        .deser_index  = 0U,
        .packet_cnt   = 0U
      };
      fd_bam_bundle_result_t overflow_result =
          fd_bam_deser_result_from_identity( ctx, &identity, &overflow_err );
      fd_bam_enqueue_result( ctx, &overflow_result );
      seen_batch_count++;
      continue;
    }

    fd_bam_batch_decode_err_t decode_err;
    _Bool const decode_ok =
        fd_bam_decode_batch( ctx,
                             &substream,
                             rx_ts_ns,
                             rx_tspub,
                             leader_slot_at_rx,
                             leader_slot_end_ns_at_rx,
                             &decoded_multi->batches[ batch_cnt ],
                             &decoded_multi->states [ batch_cnt ],
                             &decode_err );
    /* A nested Packet decode failure may leave substream unusable for
       advancing the wrapper. identity_stream independently consumed the same
       batch bytes, so close through it to preserve following siblings. */
    if( FD_UNLIKELY( !pb_close_string_substream( stream, &identity_stream ) ) ) FD_BAM_MULTI_DECODE_FAIL();
    if( FD_UNLIKELY( !decode_ok ) ) {
      _Bool const packet_decode_failed = decoded_multi->states[ batch_cnt ].packet_decode_failed;
      if( (identity.has_seq_id || identity_ok) &&
          decoded_multi->states[ batch_cnt ].has_deser_err &&
          !packet_decode_failed ) {
        fd_bam_bundle_result_t decode_result =
            fd_bam_deser_result_from_identity( ctx, &identity, &decode_err );
        fd_bam_enqueue_result( ctx, &decode_result );
      } else {
        FD_LOG_WARNING(( "Unable to attribute malformed AtomicTxnBatch (%s)",
                         packet_decode_failed ? "nested Packet decode failed"
                                              : (identity_err ? identity_err : "missing seq_id") ));
      }
      seen_batch_count++;
      continue;
    }
    batch_cnt++;
    seen_batch_count++;
  }

  if( FD_UNLIKELY( !eof ) ) FD_BAM_MULTI_DECODE_FAIL();
  decoded_multi->batch_cnt = batch_cnt;

  if( FD_UNLIKELY( seen_batch_count == 0U ) ) {
    FD_LOG_WARNING(( "MultipleAtomicTxnBatch contained no AtomicTxnBatch entries" ));
    ctx->metrics.ingress_message_rejected_cnt[ FD_METRICS_ENUM_BAM_INGRESS_MESSAGE_REJECT_REASON_V_EMPTY_MESSAGE_IDX ]++;
  }
#undef FD_BAM_MULTI_DECODE_FAIL
  return 1;
}

/* Decodes bam_api.SchedulerResponse (versioned envelope) using a two-phase
   stage/commit flow. Envelope decode failures and unsupported versions bump
   bam_failure, while handled AtomicTxnBatch rejects stay in
   bam_ingress_batch_rejected or bam_ingress_message_rejected. */
void
fd_bam_handle_scheduler_response( fd_bam_tile_t * ctx,
                                  void const *    data,
                                  ulong           data_sz,
                                  long            rx_ts_ns,
                                  uint            rx_tspub ) {
  pb_istream_t istream = pb_istream_from_buffer( data, data_sz );
  ulong leader_slot_at_rx = ctx->bam_leader_state.slot;
  long  leader_slot_end_ns_at_rx = ctx->bam_leader_state.slot_end_ns;

  uint32_t       tag;
  pb_wire_type_t wire_type;
  bool           eof                     = false;
  uint32_t       unsupported_version_tag = 0U;
  uchar const *  selected_v0_data        = NULL;
  size_t         selected_v0_data_sz     = 0UL;
  uint32_t       selected_tag            = 0U;
  uchar const *  selected_data           = NULL;
  size_t         selected_data_sz        = 0UL;

  while( pb_decode_tag( &istream, &wire_type, &tag, &eof ) ) {
    if( FD_UNLIKELY( !tag ) ) {
      PB_SET_ERROR( (&istream), "zero tag" );
      goto fail;
    }
    if( FD_UNLIKELY( wire_type != PB_WT_STRING ) ) {
      PB_SET_ERROR( (&istream), "wrong wire type" );
      goto fail;
    }
    if( FD_UNLIKELY( tag != bam_api_SchedulerResponse_v0_tag ) ) {
      unsupported_version_tag = tag;
      if( FD_UNLIKELY( !pb_skip_field( &istream, wire_type ) ) ) goto fail;
      continue;
    }

    pb_istream_t substream;
    if( FD_UNLIKELY( !pb_make_string_substream( &istream, &substream ) ) ) goto fail;
    selected_v0_data    = (uchar const *)substream.state;
    selected_v0_data_sz = substream.bytes_left;
    if( FD_UNLIKELY( !pb_close_string_substream( &istream, &substream ) ) ) goto fail;
  }

  if( FD_UNLIKELY( !eof ) ) goto fail;
  if( FD_UNLIKELY( !selected_v0_data ) ) {
    if( unsupported_version_tag ) {
      FD_LOG_WARNING(( "Unsupported SchedulerResponse version (tag=%u); scheduling reset", unsupported_version_tag ));
      ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_UNSUPPORTED_VERSION_IDX ]++;
      ctx->defer_reset = 1;
    } else {
      ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_SCHEDULER_ENVELOPE_DECODE_IDX ]++;
      FD_LOG_WARNING(( "Protobuf decode of (bam_api.SchedulerResponse) missing version" ));
    }
    return;
  }

  pb_istream_t v0_stream = pb_istream_from_buffer( selected_v0_data, selected_v0_data_sz );
  eof = false;
  while( pb_decode_tag( &v0_stream, &wire_type, &tag, &eof ) ) {
    switch( tag ) {
    case bam_api_SchedulerResponseV0_heart_beat_tag:
    case bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag:
    case bam_api_SchedulerResponseV0_ping_tag:
      break;
    case 0:
      PB_SET_ERROR( (&istream), "zero tag" );
      goto fail;
    default:
      PB_SET_ERROR( (&istream), "unexpected tag" );
      goto fail;
    }

    if( FD_UNLIKELY( wire_type != PB_WT_STRING ) ) {
      PB_SET_ERROR( (&istream), "wrong wire type" );
      goto fail;
    }

    pb_istream_t substream;
    if( FD_UNLIKELY( !pb_make_string_substream( &v0_stream, &substream ) ) ) goto fail;
    selected_tag     = tag;
    selected_data    = (uchar const *)substream.state;
    selected_data_sz = substream.bytes_left;
    if( FD_UNLIKELY( !pb_close_string_substream( &v0_stream, &substream ) ) ) goto fail;
  }
  if( FD_UNLIKELY( !eof ) ) goto fail;
  if( FD_UNLIKELY( !selected_tag ) ) {
    PB_SET_ERROR( (&istream), "missing v0 response" );
    goto fail;
  }

  switch( selected_tag ) {
  case bam_api_SchedulerResponseV0_heart_beat_tag: {
    pb_istream_t hb_stream = pb_istream_from_buffer( selected_data, selected_data_sz );
    bam_types_BuilderHeartBeat hb = bam_types_BuilderHeartBeat_init_default;
    if( FD_UNLIKELY( !pb_decode( &hb_stream, &bam_types_BuilderHeartBeat_msg, &hb ) ) ) {
      FD_LOG_WARNING(( "BuilderHeartBeat decode failed: %s", PB_GET_ERROR( &hb_stream ) ));
      PB_SET_ERROR( (&istream), PB_GET_ERROR( &hb_stream ) );
      goto fail;
    }
    ctx->bam_last_builder_activity_ns = rx_ts_ns;
    if( FD_LIKELY( hb.time_sent_microseconds ) ) {
      ulong tsorig_ns = hb.time_sent_microseconds * 1000UL;
      ulong rx_ts_u   = fd_ulong_if( rx_ts_ns >= 0L, (ulong)rx_ts_ns, 0UL );
      fd_histf_sample( ctx->metrics.builder_heartbeat_arrival_delta_nanos, fd_ulong_sat_sub( rx_ts_u, tsorig_ns ) );
    }
    ctx->metrics.builder_heartbeats_decoded_cnt++;
    break;
  }
  case bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag: {
    fd_bam_decoded_multi_batch_t * decoded_multi = ctx->decoded_multi;
    pb_istream_t substream = pb_istream_from_buffer( selected_data, selected_data_sz );
    if( FD_UNLIKELY( !fd_bam_decode_multiple_atomic_txn_batch( ctx,
                                                               &substream,
                                                               rx_ts_ns,
                                                               rx_tspub,
                                                               leader_slot_at_rx,
                                                               leader_slot_end_ns_at_rx,
                                                               decoded_multi ) ) ) {
      PB_SET_ERROR( (&istream), PB_GET_ERROR( &substream ) );
      goto fail;
    }
    ctx->metrics.ingress_multi_message_received_cnt++;
    ctx->metrics.ingress_batch_commit_attempt_cnt += decoded_multi->batch_cnt;
    for( uint i=0U; i<decoded_multi->batch_cnt; i++ ) {
      if( FD_UNLIKELY( !fd_bam_validate_batch( ctx,
                                               &decoded_multi->states [ i ],
                                               &decoded_multi->batches[ i ] ) ) ) {
        continue;
      }
      fd_bam_publish_batch( ctx,
                            &decoded_multi->states [ i ],
                            &decoded_multi->batches[ i ] );
    }
    ctx->bam_last_builder_activity_ns = rx_ts_ns;
    break;
  }
  case bam_api_SchedulerResponseV0_ping_tag: {
    pb_istream_t ping_stream = pb_istream_from_buffer( selected_data, selected_data_sz );
    bam_types_Ping ping = bam_types_Ping_init_default;
    if( FD_UNLIKELY( !pb_decode( &ping_stream, &bam_types_Ping_msg, &ping ) ) ) {
      FD_LOG_WARNING(( "Ping decode failed: %s", PB_GET_ERROR( &ping_stream ) ));
      PB_SET_ERROR( (&istream), PB_GET_ERROR( &ping_stream ) );
      goto fail;
    }
    /* Scheduler proto Ping is only a latency probe. It must be answered on
       the protobuf stream, but it does not refresh the builder-activity
       watchdog or HTTP/2 keepalive state. */
    if( FD_UNLIKELY( !( ctx->grpc_client && ctx->bam_stream && ctx->bam_stream_live ) ) ) break;

    ulong rx_ts_u = fd_ulong_if( rx_ts_ns >= 0L, (ulong)rx_ts_ns, 0UL );
    long  now_ns  = fd_bam_now();
    ulong now_u   = fd_ulong_if( now_ns >= 0L, (ulong)now_ns, 0UL );
    fd_histf_sample( ctx->metrics.scheduler_pong_send_nanos, fd_ulong_sat_sub( now_u, rx_ts_u ) );

    bam_api_SchedulerMessage msg = bam_api_SchedulerMessage_init_default;
    msg.which_versioned_msg        = bam_api_SchedulerMessage_v0_tag;
    msg.versioned_msg.v0.which_msg = bam_api_SchedulerMessageV0_pong_tag;
    msg.versioned_msg.v0.msg.pong.id = ping.id;

    ulong outcome_idx = FD_METRICS_ENUM_BAM_SCHEDULER_PONG_SEND_OUTCOME_V_ENQUEUED_IDX;
    int send_ok = fd_grpc_client_stream_send_msg( ctx->grpc_client, ctx->bam_stream, &bam_api_SchedulerMessage_msg, &msg );
    if( FD_UNLIKELY( !send_ok ) ) {
      if( FD_UNLIKELY( !fd_h2_rbuf_is_empty( fd_grpc_client_rbuf_tx( ctx->grpc_client ) ) ) ) {
        outcome_idx = FD_METRICS_ENUM_BAM_SCHEDULER_PONG_SEND_OUTCOME_V_FRAME_TX_BUSY_IDX;
      } else if( FD_UNLIKELY( fd_grpc_client_request_stream_busy( ctx->grpc_client ) ) ) {
        outcome_idx = FD_METRICS_ENUM_BAM_SCHEDULER_PONG_SEND_OUTCOME_V_REQUEST_BUSY_IDX;
      } else {
        outcome_idx = FD_METRICS_ENUM_BAM_SCHEDULER_PONG_SEND_OUTCOME_V_SEND_FAIL_IDX;
        FD_LOG_WARNING(( "Failed to send BAM scheduler pong (id=%u)", ping.id ));
      }
    }
    ctx->metrics.scheduler_pong_send_outcome_cnt[ outcome_idx ]++;
    break;
  }
  }
  return;

fail:
  ctx->metrics.failure_cnt[ FD_METRICS_ENUM_BAM_FAILURE_V_SCHEDULER_ENVELOPE_DECODE_IDX ]++;
  FD_LOG_WARNING(( "Protobuf decode of (bam_api.SchedulerResponse) failed (%s)", PB_GET_ERROR( &istream ) ));
}
