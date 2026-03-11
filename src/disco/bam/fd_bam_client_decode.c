/* protobuf decoding for the BAM client. */

#include "fd_bam_tile_private.h"
#include "proto/bam_types.pb.h"
#include "../../ballet/base58/fd_base58.h"
#include "../../ballet/txn/fd_txn.h"
#include "../../ballet/nanopb/pb_decode.h"
#include "../../flamenco/runtime/fd_system_ids.h"


#define FD_BAM_MAX_TXN_PER_ATOMIC_BATCH        5U
#define FD_BAM_MAX_ATOMIC_BATCHES_PER_PACKET   8U
#define FD_BAM_DUMP_ADDR_BUF_SZ                (FD_BASE58_ENCODED_32_SZ + 32UL)
#define FD_BAM_DUMP_PROGRAM_BUF_SZ             (FD_BAM_DUMP_ADDR_BUF_SZ + 32UL)
#define FD_BAM_DUMP_LOG_BUF_SZ                 (31UL*4096UL)
#define FD_BAM_DUMP_HEX_PREVIEW_MAX            24UL
#define FD_BAM_DUMP_IX_ACCT_PREVIEW_MAX        4UL
#define FD_BAM_DUMP_LUT_IDX_PREVIEW_MAX        8UL

FD_STATIC_ASSERT( FD_BAM_MAX_TXN_PER_ATOMIC_BATCH <= FD_PACK_MAX_TXN_PER_BUNDLE,
                  bam_decode_packet_limit_fits_staging );

static FD_TL char fd_bam_dump_log_buf[ FD_BAM_DUMP_LOG_BUF_SZ ];

typedef struct {
  bam_types_AtomicTxnBatch batch;
  fd_bam_batch_ctx_t       state;
} fd_bam_decoded_batch_t;

typedef struct {
  fd_bam_decoded_batch_t batches[ FD_BAM_MAX_ATOMIC_BATCHES_PER_PACKET ];
  uint                   batch_cnt;
  _Bool                  has_err_result;
  fd_bam_bundle_result_t err_result;
} fd_bam_decoded_multi_batch_t;

typedef enum {
  FD_BAM_V0_STAGED_NONE      = 0,
  FD_BAM_V0_STAGED_HEARTBEAT = 1,
  FD_BAM_V0_STAGED_MULTI     = 2,
  FD_BAM_V0_STAGED_PING      = 3
} fd_bam_v0_staged_kind_t;

typedef struct {
  fd_bam_v0_staged_kind_t kind;
  uint64_t                heartbeat_time_sent_microseconds;
  uint32_t                ping_id;
  fd_bam_decoded_multi_batch_t multi;
} fd_bam_decoded_v0_t;

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

static inline ulong
fd_bam_resolve_batch_slot( fd_bam_tile_t const *             ctx,
                           bam_types_AtomicTxnBatch const *  batch ) {
  ulong resolved_slot = batch->max_schedule_slot;
  if( FD_UNLIKELY( resolved_slot==FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT ) ) {
    resolved_slot = ctx->bam_leader_state.slot;
  }
  return resolved_slot;
}

int
fd_bam_should_dump_batch( fd_bam_tile_t *               ctx,
                          bam_types_AtomicTxnBatch const * batch ) {
  if( FD_UNLIKELY( !!ctx->dump_bam_txns ) ) return 1;
  if( FD_LIKELY( !ctx->dump_bam_first_slot_txn ) ) return 0;

  /* Scheduler batches often leave max_schedule_slot at the default sentinel.
     Fall back to the current leader-state slot so "first per slot" groups by
     the actual slot boundary instead of collapsing into one lifetime bucket. */
  ulong resolved_slot = fd_bam_resolve_batch_slot( ctx, batch );

  if( FD_LIKELY( ctx->dump_bam_last_slot_valid && ctx->dump_bam_last_slot==resolved_slot ) ) return 0;

  ctx->dump_bam_last_slot       = resolved_slot;
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
    state->has_deser_err   = true;
    state->deser_reason    = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
    state->deser_index     = state->packet_cnt;
    return false;
  }

  uchar packet_revert_on_error = 0U;
  if( packet.has_meta && packet.meta.has_flags ) {
    packet_revert_on_error = packet.meta.flags.revert_on_error;
  }

  if( FD_UNLIKELY( state->packet_cnt && state->revert_on_error != packet_revert_on_error ) ) {
    FD_LOG_WARNING(( "AtomicTxnBatch contains mixed revert_on_error flags" ));
    state->has_deser_err = true;
    state->deser_reason  = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
    state->deser_index   = 0U;
    return false;
  }
  state->revert_on_error = packet_revert_on_error;

  ulong declared_sz = fd_ulong_max( packet.data.size, packet.meta.size );
  if( FD_UNLIKELY( declared_sz > FD_TXN_MTU ) ) {
    state->ctx->metrics.packet_drop_cnt++;
    FD_MCNT_INC( BAM, PACKETS_DROPPED, 1UL );
    FD_LOG_WARNING(( "Received AtomicTxnBatch packet exceeding MTU (%lu>%lu); dropping batch",
                     declared_sz, FD_TXN_MTU ));
    state->has_deser_err = true;
    state->deser_reason = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
    state->deser_index  = state->packet_cnt;
    return false;
  }

  state->packets[ state->packet_cnt ] = packet;
  state->packet_cnt++;
  return true;
}

static _Bool
fd_bam_validate_batch( fd_bam_tile_t *                  ctx,
                       fd_bam_batch_ctx_t const *       state,
                       bam_types_AtomicTxnBatch const * batch ) {
  if( FD_UNLIKELY( state->has_deser_err ) ) {
    ctx->metrics.ingress_reject_deser_cnt++;
    fd_bam_bundle_result_t res = {
      .seq_id            = batch->seq_id,
      .slot              = batch->max_schedule_slot,
      .bundle_txn_cnt    = state->packet_cnt,
      .execution_success = 0,
      .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
      .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
      .deser_reason      = state->deser_reason,
      .deser_index       = state->deser_index
    };
    fd_bam_enqueue_result( ctx, &res );
    ctx->bundle_max_schedule_slot = FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT;
    return 0;
  }

  if( FD_UNLIKELY( state->packet_cnt == 0U ) ) {
    ctx->metrics.ingress_reject_empty_cnt++;
    fd_bam_bundle_result_t res = {
      .seq_id            = batch->seq_id,
      .slot              = batch->max_schedule_slot,
      .bundle_txn_cnt    = state->packet_cnt,
      .execution_success = 0,
      .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
      .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
      .deser_reason      = bam_types_DeserializationErrorReason_EMPTY,
      .deser_index       = 0
    };
    fd_bam_enqueue_result( ctx, &res );
    ctx->bundle_max_schedule_slot = FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT;
    return 0;
  }

  if( FD_UNLIKELY( (!state->revert_on_error) && state->packet_cnt>1U ) ) {
    ctx->metrics.ingress_reject_non_revert_multi_packet_cnt++;
    /* For now, revert_on_error=0 batches are assumed to contain exactly one
       packet so we can return one result per seq_id without BAM-node changes. */
    fd_bam_bundle_result_t res = {
      .seq_id            = batch->seq_id,
      .slot              = batch->max_schedule_slot,
      .bundle_txn_cnt    = state->packet_cnt,
      .execution_success = 0,
      .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
      .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
      .deser_reason      = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE,
      .deser_index       = 0
    };
    fd_bam_enqueue_result( ctx, &res );
    ctx->bundle_max_schedule_slot = FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT;
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
    ctx->metrics.ingress_reject_deser_cnt++;
    fd_bam_bundle_result_t res = {
      .seq_id            = batch->seq_id,
      .slot              = batch->max_schedule_slot,
      .bundle_txn_cnt    = state->packet_cnt,
      .execution_success = 0,
      .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
      .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
      .deser_reason      = bam_types_DeserializationErrorReason_VOTE_TRANSACTION_FAILURE,
      .deser_index       = (uchar)simple_vote_idx,
    };
    fd_bam_enqueue_result( ctx, &res );
    ctx->bundle_max_schedule_slot = FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT;
    return 0;
  }

  return 1;
}

void
fd_bam_publish_batch( fd_bam_tile_t *            ctx,
                      fd_bam_batch_ctx_t *       state,
                      bam_types_AtomicTxnBatch const * batch ) {
  ulong resolved_slot = fd_bam_resolve_batch_slot( ctx, batch );
  fd_bam_slot_ingress_timing_t * entry = &ctx->slot_ingress_timing[ resolved_slot & ( FD_BAM_SLOT_INGRESS_TIMING_CNT - 1UL ) ];
  if( FD_UNLIKELY( !entry->valid || entry->slot!=resolved_slot ) ) {
    fd_memset( entry, 0, sizeof(fd_bam_slot_ingress_timing_t) );
    entry->slot  = resolved_slot;
    entry->valid = 1U;
  }

  _Bool after_slot_end = !!ctx->bam_leader_state.slot && !!resolved_slot && resolved_slot < ctx->bam_leader_state.slot;
  if( FD_UNLIKELY( !entry->txn_before_slot_end && !entry->txn_after_slot_end ) ) {
    entry->first_rx_ts_ns          = fd_bam_now();
    entry->first_rx_after_slot_end = (uchar)after_slot_end;
  }

  if( FD_LIKELY( !after_slot_end ) ) entry->txn_before_slot_end += state->packet_cnt;
  else                               entry->txn_after_slot_end  += state->packet_cnt;

  if( FD_UNLIKELY( fd_bam_should_dump_batch( ctx, batch ) ) ) {
    char * msg = fd_bam_dump_log_buf;
    ulong  off = 0UL;

    /* Emit one NOTICE record per bundle so unrelated logs cannot split txn details apart. */
    off = fd_bam_dump_appendf( msg, FD_BAM_DUMP_LOG_BUF_SZ, off,
                               "BAM rx bundle: seq_id=%u max_schedule_slot=%lu txns=%u mode=%s dispatch=%s\n",
                               batch->seq_id,
                               batch->max_schedule_slot,
                               (uint)state->packet_cnt,
                               state->revert_on_error ? "atomic" : "independent",
                               state->revert_on_error ? "bundle" : "fanout" );
    for( uchar i=0U; i<state->packet_cnt; i++ ) {
      if( FD_UNLIKELY( i ) ) off = fd_bam_dump_appendf( msg, FD_BAM_DUMP_LOG_BUF_SZ, off, "\n" );
      off = fd_bam_dump_append_inbound_txn( msg,
                                            FD_BAM_DUMP_LOG_BUF_SZ,
                                            off,
                                            i,
                                            state->packet_cnt,
                                            &state->packets[ i ] );
    }
    off = fd_bam_dump_appendf( msg, FD_BAM_DUMP_LOG_BUF_SZ, off,
                               "\nslot_timing: slot=%lu first_rx_ns=%ld first_rx_after_slot_end=%u txns_before_slot_end=%lu txns_after_slot_end=%lu current_leader_slot=%lu",
                               resolved_slot,
                               entry->first_rx_ts_ns,
                               (uint)entry->first_rx_after_slot_end,
                               entry->txn_before_slot_end,
                               entry->txn_after_slot_end,
                               ctx->bam_leader_state.slot );
    FD_LOG_NOTICE(( "%s", msg ));
  }

  if( state->revert_on_error ) {
    ctx->bundle_seq                = batch->seq_id;
    ctx->bundle_txn_cnt            = state->packet_cnt;
    ctx->bundle_max_schedule_slot  = batch->max_schedule_slot;

    for( uchar i=0; i<state->packet_cnt; i++ ) {
      fd_bam_tile_publish_bundle_txn( ctx,
                                      state->packets[i].data.bytes,
                                      (ushort)state->packets[i].data.size,
                                      state->packet_cnt,
                                      i,
                                      0 );
    }
    ctx->metrics.bundle_received_cnt++;
  } else {
    for( uchar i=0; i<state->packet_cnt; i++ ) {
      fd_bam_tile_publish_txn( ctx,
                               state->packets[i].data.bytes,
                               state->packets[i].data.size,
                               batch->max_schedule_slot,
                               batch->seq_id,
                               i,
                               state->packet_cnt,
                               state->revert_on_error,
                               0 );
    }
  }
  ctx->bundle_max_schedule_slot = FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT;
}

/* Decodes one bam_types.AtomicTxnBatch message into staged state only.
   This function never publishes transactions. Returns 1 when the batch was
   fully consumed. Returns 0 when protobuf decoding failed after
   logging/enqueuing the appropriate bundle error. */
static _Bool
fd_bam_decode_batch( fd_bam_tile_t *          ctx,
                     pb_istream_t *           stream,
                     fd_bam_decoded_batch_t * decoded ) {
  fd_memset( decoded, 0, sizeof(fd_bam_decoded_batch_t) );
  decoded->state.ctx          = ctx;
  decoded->state.deser_reason = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
  decoded->state.deser_index  = 0U;
  decoded->batch = (bam_types_AtomicTxnBatch)bam_types_AtomicTxnBatch_init_default;
  decoded->batch.packets = (pb_callback_t){
    .funcs.decode = fd_bam_collect_packet,
    .arg          = &decoded->state
  };

  if( FD_UNLIKELY( !pb_decode( stream, &bam_types_AtomicTxnBatch_msg, &decoded->batch ) ) ) {
    uchar deser_reason = decoded->state.has_deser_err
                       ? decoded->state.deser_reason
                       : (uchar)bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE;
    uchar deser_index  = decoded->state.has_deser_err
                       ? decoded->state.deser_index
                       : decoded->state.packet_cnt;
    ctx->metrics.decode_fail_cnt++;
    FD_MCNT_INC( BAM, ERRORS_PROTOBUF, 1UL );
    /* Decode failure still produces a not-committed result, so count this as
       a BAM ingress reject in addition to the protobuf error bucket. */
    ctx->metrics.ingress_batch_reject_cnt++;
    ctx->metrics.ingress_reject_deser_cnt++;
    char const * err = PB_GET_ERROR( stream );
    FD_LOG_WARNING(( "Protobuf decode of (bam_types.AtomicTxnBatch) failed (%s)", err ));
    fd_bam_bundle_result_t res = {
      .seq_id            = decoded->batch.seq_id,
      .slot              = decoded->batch.max_schedule_slot,
      .bundle_txn_cnt    = decoded->state.packet_cnt,
      .execution_success = 0,
      .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
      .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
      .deser_reason      = deser_reason,
      .deser_index       = deser_index
    };
    fd_bam_enqueue_result( ctx, &res );
    return 0;
  }

  return 1;
}

/* Decodes a bam_types.MultipleAtomicTxnBatch wrapper into staged state.
   Returns 1 once the message bytes were consumed, including semantic error
   cases that stage a not-committed result. Returns 0 on protobuf failures.
   No transaction publish occurs in this stage. */
static int
fd_bam_decode_multiple_atomic_txn_batch( fd_bam_tile_t * ctx,
                                         pb_istream_t *   stream,
                                         fd_bam_decoded_multi_batch_t * decoded_multi ) {
  fd_memset( decoded_multi, 0, sizeof(fd_bam_decoded_multi_batch_t) );

  uint32_t       tag;
  pb_wire_type_t wire_type;
  bool           eof = false;
  uint           seen_batch_count = 0U;

  while( pb_decode_tag( stream, &wire_type, &tag, &eof ) ) {
    if( FD_UNLIKELY( tag != bam_types_MultipleAtomicTxnBatch_batches_tag ) ) {
      if( FD_UNLIKELY( !pb_skip_field( stream, wire_type ) ) ) return 0;
      continue;
    }
    if( FD_UNLIKELY( wire_type != PB_WT_STRING ) ) {
      if( FD_UNLIKELY( !pb_skip_field( stream, wire_type ) ) ) return 0;
      continue;
    }

    pb_istream_t substream;
    if( FD_UNLIKELY( !pb_make_string_substream( stream, &substream ) ) ) return 0;

    if( FD_UNLIKELY( seen_batch_count >= FD_BAM_MAX_ATOMIC_BATCHES_PER_PACKET ) ) {
      fd_bam_decoded_batch_t overflow_batch;
      _Bool const ok = fd_bam_decode_batch( ctx, &substream, &overflow_batch );
      pb_close_string_substream( stream, &substream );
      if( FD_UNLIKELY( !ok ) ) return 0;

      FD_LOG_WARNING(( "MultipleAtomicTxnBatch exceeded max batch count (%u>%u)",
                       seen_batch_count + 1U,
                       FD_BAM_MAX_ATOMIC_BATCHES_PER_PACKET ));
      ctx->metrics.ingress_multi_overflow_msg_cnt++;
      ctx->metrics.ingress_batch_reject_cnt++;
      ctx->metrics.ingress_reject_deser_cnt++;
      decoded_multi->batch_cnt = seen_batch_count;
      decoded_multi->has_err_result = 1;
      decoded_multi->err_result = (fd_bam_bundle_result_t){
        .seq_id            = overflow_batch.batch.seq_id,
        .slot              = overflow_batch.batch.max_schedule_slot,
        .bundle_txn_cnt    = overflow_batch.state.packet_cnt,
        .execution_success = 0,
        .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
        .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
        .deser_reason      = bam_types_DeserializationErrorReason_INCONSISTENT_BUNDLE,
        .deser_index       = 0
      };
      return 1;
    }

    _Bool const ok = fd_bam_decode_batch( ctx, &substream, &decoded_multi->batches[ seen_batch_count ] );
    pb_close_string_substream( stream, &substream );
    if( FD_UNLIKELY( !ok ) ) return 0;
    seen_batch_count++;
  }

  if( FD_UNLIKELY( !eof ) ) return 0;
  decoded_multi->batch_cnt = seen_batch_count;

  if( FD_UNLIKELY( seen_batch_count == 0U ) ) {
    FD_LOG_WARNING(( "MultipleAtomicTxnBatch contained no AtomicTxnBatch entries" ));
    ctx->metrics.ingress_multi_empty_msg_cnt++;
    ctx->metrics.ingress_batch_reject_cnt++;
    ctx->metrics.ingress_reject_empty_cnt++;
    bam_types_AtomicTxnBatch batch = bam_types_AtomicTxnBatch_init_default;
    decoded_multi->has_err_result = 1;
    decoded_multi->err_result = (fd_bam_bundle_result_t){
      .seq_id            = batch.seq_id,
      .slot              = batch.max_schedule_slot,
      .bundle_txn_cnt    = 0,
      .execution_success = 0,
      .scheduling_error  = FD_BAM_SCHED_ERR_NONE,
      .bundle_err        = FD_BAM_BUNDLE_ERR_DESER,
      .deser_reason      = bam_types_DeserializationErrorReason_EMPTY,
      .deser_index       = 0
    };
  }
  return 1;
}

static void
fd_bam_commit_multiple_atomic_txn_batch( fd_bam_tile_t *                      ctx,
                                         fd_bam_decoded_multi_batch_t *       decoded_multi ) {
  ctx->metrics.ingress_batch_commit_attempt_cnt += decoded_multi->batch_cnt;

  for( uint i=0U; i<decoded_multi->batch_cnt; i++ ) {
    if( FD_UNLIKELY( !fd_bam_validate_batch( ctx,
                                             &decoded_multi->batches[ i ].state,
                                             &decoded_multi->batches[ i ].batch ) ) ) {
      ctx->metrics.ingress_batch_reject_cnt++;
      continue;
    }
    fd_bam_publish_batch( ctx,
                          &decoded_multi->batches[ i ].state,
                          &decoded_multi->batches[ i ].batch );
    ctx->metrics.ingress_batch_publish_cnt++;
  }

  if( FD_UNLIKELY( decoded_multi->has_err_result ) ) {
    /* Reject counters are recorded at the decode site where err_result was staged
       (overflow/empty wrapper) so the reason taxonomy remains accurate. */
    fd_bam_enqueue_result( ctx, &decoded_multi->err_result );
    ctx->bundle_max_schedule_slot = FD_BAM_MAX_SCHEDULE_SLOT_DEFAULT;
  }
}

/* Decodes the v0 scheduler response payload (heartbeats, nested batches, and
   ping probes) into staged state. Returns 1 once the message was consumed;
   returns 0 on protobuf failures so the caller can reject the message before
   commit. */
static int
fd_bam_decode_scheduler_response_v0( fd_bam_tile_t * ctx,
                                     pb_istream_t *   stream,
                                     fd_bam_decoded_v0_t * decoded_v0 ) {
  fd_memset( decoded_v0, 0, sizeof(fd_bam_decoded_v0_t) );

  uint32_t      tag;
  pb_wire_type_t wire_type;
  bool          eof = false;
  while( pb_decode_tag( stream, &wire_type, &tag, &eof ) ) {
    switch( tag ) {
    case bam_api_SchedulerResponseV0_heart_beat_tag: {
      if( FD_UNLIKELY( wire_type != PB_WT_STRING ) ) {
        if( FD_UNLIKELY( !pb_skip_field( stream, wire_type ) ) ) return 0;
        break;
      }
      pb_istream_t hb_stream;
      if( FD_UNLIKELY( !pb_make_string_substream( stream, &hb_stream ) ) ) return 0;
      bam_types_BuilderHeartBeat hb = bam_types_BuilderHeartBeat_init_default;
      int ok = pb_decode( &hb_stream, &bam_types_BuilderHeartBeat_msg, &hb );
      pb_close_string_substream( stream, &hb_stream );
      if( FD_UNLIKELY( !ok ) ) {
        FD_LOG_WARNING(( "BuilderHeartBeat decode failed: %s", PB_GET_ERROR( &hb_stream ) ));
        return 0;
      }
      ctx->metrics.ingress_v0_heartbeat_msg_cnt++;
      decoded_v0->kind = FD_BAM_V0_STAGED_HEARTBEAT;
      decoded_v0->heartbeat_time_sent_microseconds = hb.time_sent_microseconds;
      break;
    }
    case bam_api_SchedulerResponseV0_multiple_atomic_txn_batch_tag: {
      if( FD_UNLIKELY( wire_type != PB_WT_STRING ) ) {
        if( FD_UNLIKELY( !pb_skip_field( stream, wire_type ) ) ) return 0;
        break;
      }
      pb_istream_t substream;
      if( FD_UNLIKELY( !pb_make_string_substream( stream, &substream ) ) ) return 0;
      fd_bam_decoded_multi_batch_t decoded_multi;
      int ok = fd_bam_decode_multiple_atomic_txn_batch( ctx, &substream, &decoded_multi );
      pb_close_string_substream( stream, &substream );
      if( FD_UNLIKELY( !ok ) ) return 0;
      ctx->metrics.ingress_v0_multi_msg_cnt++;
      ctx->metrics.ingress_multi_batch_total_cnt += decoded_multi.batch_cnt;
      decoded_v0->kind  = FD_BAM_V0_STAGED_MULTI;
      decoded_v0->multi = decoded_multi;
      break;
    }
    case bam_api_SchedulerResponseV0_ping_tag: {
      if( FD_UNLIKELY( wire_type != PB_WT_STRING ) ) {
        if( FD_UNLIKELY( !pb_skip_field( stream, wire_type ) ) ) return 0;
        break;
      }
      pb_istream_t ping_stream;
      if( FD_UNLIKELY( !pb_make_string_substream( stream, &ping_stream ) ) ) return 0;
      bam_types_Ping ping = bam_types_Ping_init_default;
      int ok = pb_decode( &ping_stream, &bam_types_Ping_msg, &ping );
      pb_close_string_substream( stream, &ping_stream );
      if( FD_UNLIKELY( !ok ) ) {
        FD_LOG_WARNING(( "Ping decode failed: %s", PB_GET_ERROR( &ping_stream ) ));
        return 0;
      }
      decoded_v0->kind    = FD_BAM_V0_STAGED_PING;
      decoded_v0->ping_id = ping.id;
      break;
    }
    default:
      if( FD_UNLIKELY( !pb_skip_field( stream, wire_type ) ) ) return 0;
      break;
    }
  }
  if( FD_UNLIKELY( !eof ) ) return 0;
  return 1;
}

/* Decodes bam_api.SchedulerResponse (versioned envelope) using a two-phase
   stage/commit flow. On decode failure or unsupported version it bumps metrics
   and may request a reset; otherwise it commits the staged versioned payload. */
void
fd_bam_handle_scheduler_response( fd_bam_tile_t * ctx,
                                  void const *    data,
                                  ulong           data_sz,
                                  long            rx_ts_ns ) {
  pb_istream_t istream = pb_istream_from_buffer( data, data_sz );

  uint32_t      tag;
  pb_wire_type_t wire_type;
  bool          eof         = false;
  uint32_t      version_tag = 0U;
  int           seen_v0     = 0;
  fd_bam_decoded_v0_t decoded_v0 = {0};

  while( pb_decode_tag( &istream, &wire_type, &tag, &eof ) ) {
    version_tag = tag;
    if( FD_UNLIKELY( tag != bam_api_SchedulerResponse_v0_tag ) ) {
      if( FD_UNLIKELY( !pb_skip_field( &istream, wire_type ) ) ) goto fail;
      continue;
    }

    if( FD_UNLIKELY( wire_type != PB_WT_STRING ) ) {
      if( FD_UNLIKELY( !pb_skip_field( &istream, wire_type ) ) ) goto fail;
      continue;
    }

    pb_istream_t substream;
    if( FD_UNLIKELY( !pb_make_string_substream( &istream, &substream ) ) ) goto fail;
    fd_bam_decoded_v0_t staged_v0;
    int ok = fd_bam_decode_scheduler_response_v0( ctx, &substream, &staged_v0 );
    pb_close_string_substream( &istream, &substream );
    if( FD_UNLIKELY( !ok ) ) goto fail;
    decoded_v0 = staged_v0;
    seen_v0 = 1;
  }

  if( FD_UNLIKELY( !eof ) ) goto fail;
  if( FD_UNLIKELY( !seen_v0 ) ) {
    if( version_tag && version_tag != bam_api_SchedulerResponse_v0_tag ) {
      FD_LOG_WARNING(( "Unsupported SchedulerResponse version (tag=%u); scheduling reset", version_tag ));
      ctx->metrics.transport_fail_cnt++;
      ctx->defer_reset = 1;
    } else {
      ctx->metrics.decode_fail_cnt++;
      FD_LOG_WARNING(( "Protobuf decode of (bam_api.SchedulerResponse) missing version" ));
    }
    return;
  }

  switch( decoded_v0.kind ) {
  case FD_BAM_V0_STAGED_HEARTBEAT:
    ctx->bam_last_builder_heartbeat_ns = rx_ts_ns;
    if( FD_LIKELY( decoded_v0.heartbeat_time_sent_microseconds ) ) {
      ulong tsorig_ns = decoded_v0.heartbeat_time_sent_microseconds * 1000UL;
      ulong rx_ts_u   = fd_ulong_if( rx_ts_ns >= 0L, (ulong)rx_ts_ns, 0UL );
      fd_histf_sample( ctx->metrics.node_hearbeat_network_latency_nanos, fd_ulong_sat_sub( rx_ts_u, tsorig_ns ) );
    }
    ctx->metrics.heartbeat_recv_cnt++;
    break;
  case FD_BAM_V0_STAGED_MULTI:
    fd_bam_commit_multiple_atomic_txn_batch( ctx, &decoded_v0.multi );
    ctx->bam_last_builder_heartbeat_ns = rx_ts_ns;
    break;
  case FD_BAM_V0_STAGED_PING:
    /* Scheduler proto Ping is only a latency probe. It must be answered on
       the protobuf stream, but it does not refresh the builder-heartbeat
       watchdog or HTTP/2 keepalive state. */
    if( FD_LIKELY( ctx->bam_stream && ctx->bam_stream_live ) ) {
      bam_api_SchedulerMessage msg = bam_api_SchedulerMessage_init_default;
      msg.which_versioned_msg        = bam_api_SchedulerMessage_v0_tag;
      msg.versioned_msg.v0.which_msg = bam_api_SchedulerMessageV0_pong_tag;
      msg.versioned_msg.v0.msg.pong.id = decoded_v0.ping_id;
      if( FD_UNLIKELY( !fd_grpc_client_stream_send( ctx->grpc_client, ctx->bam_stream, &bam_api_SchedulerMessage_msg, &msg, 0 ) ) ) {
        FD_LOG_WARNING(( "Failed to send BAM scheduler pong (id=%u)", decoded_v0.ping_id ));
      } else {
        fd_bam_client_sample_scheduler_ping_response( ctx, rx_ts_ns );
      }
    }
    break;
  default:
    break;
  }
  return;

fail:
  ctx->metrics.decode_fail_cnt++;
  FD_LOG_WARNING(( "Protobuf decode of (bam_api.SchedulerResponse) failed (%s)", PB_GET_ERROR( &istream ) ));
}
