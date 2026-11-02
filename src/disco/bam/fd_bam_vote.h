#ifndef HEADER_fd_src_disco_bam_fd_bam_vote_h
#define HEADER_fd_src_disco_bam_fd_bam_vote_h

/* BAM rejects batches carrying simple votes, as jito-solana does.
   Upstream dropped its classifier with vote QOS, so FireBAM keeps
   this copy of it. */

#include "../../ballet/txn/fd_txn.h"
#include "../../flamenco/runtime/fd_system_ids_pp.h"

/* fd_bam_txn_is_simple_vote returns 1 if txn, parsed from payload, is a
   legacy transaction with 1 or 2 signatures and one Vote instruction. */
static inline int
fd_bam_txn_is_simple_vote( fd_txn_t const * txn,
                           void     const * payload ) {
  static const uchar vote_program_id[ FD_TXN_ACCT_ADDR_SZ ] = { VOTE_PROG_ID };

  fd_acct_addr_t const * addr_base = fd_txn_get_acct_addrs( txn, payload );
  if( FD_UNLIKELY( txn->instr_cnt!=1UL ) )                      return 0;
  if( FD_UNLIKELY( txn->transaction_version!=FD_TXN_VLEGACY ) ) return 0;
  if( FD_UNLIKELY( txn->signature_cnt>2UL ) )                   return 0;
  ulong prog_id_idx = (ulong)txn->instr[0].program_id;
  fd_acct_addr_t const * prog_id = addr_base + prog_id_idx;
  return fd_memeq( prog_id->b, vote_program_id, FD_TXN_ACCT_ADDR_SZ );
}

#endif /* HEADER_fd_src_disco_bam_fd_bam_vote_h */
