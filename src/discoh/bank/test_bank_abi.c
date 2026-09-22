/* Inspect the actual ABI cache handed to Agave, including its otherwise
   private Rust-compatible layout.  Only account loading is substituted. */
#include "fd_bank_abi.c"

static fd_acct_addr_t lookup_keys[2];

int
fd_ext_bank_load_account( void const *  bank,
                          int           fixed_root,
                          uchar const * addr,
                          uchar *       owner,
                          uchar *       data,
                          ulong *       data_sz ) {
  (void)bank; (void)fixed_root; (void)addr;
  FD_TEST( *data_sz>=56UL+sizeof(lookup_keys) );
  fd_alut_meta_t meta = { .discriminant=FD_ALUT_STATE_DISC_LOOKUP_TABLE, .deactivation_slot=ULONG_MAX };
  FD_TEST( !fd_alut_state_encode( &meta, data, 56UL ) );
  fd_memcpy( owner, fd_solana_address_lookup_table_program_id.uc, 32UL );
  fd_memcpy( data+56UL, lookup_keys, sizeof(lookup_keys) );
  *data_sz = 56UL+sizeof(lookup_keys);
  return 0;
}

static fd_acct_addr_t const reserved[] = {
  {{ SYSVAR_CLOCK_ID }}, {{ SYSVAR_EPOCH_REWARDS_ID }}, {{ SYSVAR_EPOCH_SCHED_ID }},
  {{ SYSVAR_FEES_ID }}, {{ SYSVAR_INSTRUCTIONS_ID }}, {{ SYSVAR_LAST_RESTART_ID }},
  {{ SYSVAR_RECENT_BLKHASH_ID }}, {{ SYSVAR_RENT_ID }}, {{ SYSVAR_REWARDS_ID }},
  {{ SYSVAR_SLOT_HASHES_ID }}, {{ SYSVAR_SLOT_HIST_ID }}, {{ SYSVAR_STAKE_HIST_ID }},
  {{ SYSVAR_PROG_ID }}, {{ ADDR_LUT_PROG_ID }}, {{ BPF_LOADER_2_PROG_ID }},
  {{ BPF_LOADER_1_PROG_ID }}, {{ BPF_UPGRADEABLE_PROG_ID }}, {{ COMPUTE_BUDGET_PROG_ID }},
  {{ CONFIG_PROG_ID }}, {{ ED25519_SV_PROG_ID }}, {{ FEATURE_ID }},
  {{ LOADER_V4_PROG_ID }}, {{ KECCAK_SECP_PROG_ID }}, {{ SECP256R1_PROG_ID }},
  {{ STAKE_CONFIG_PROG_ID }}, {{ STAKE_PROG_ID }}, {{ SYS_PROG_ID }},
  {{ VOTE_PROG_ID }}, {{ ZK_EL_GAMAL_PROG_ID }}, {{ ZK_TOKEN_PROG_ID }}, {{ NATIVE_LOADER_ID }}
};

static void
test_reserved_permissions( void ) {
  uchar const versions[] = { FD_TXN_VLEGACY, FD_TXN_V0, FD_TXN_V1 };
  for( ulong v=0UL; v<3UL; v++ ) for( ulong key=0UL; key<sizeof(reserved)/sizeof(reserved[0]); key++ )
  for( int loader=0; loader<2; loader++ ) {
    /* If the tested key itself is the loader, it already enables the
       nonreserved invoked program's requested write permission. */
    int has_loader = loader || !memcmp( reserved[key].b, BPF_UPGRADEABLE_PROG_ID1, 32UL );
    uchar payload[512] = {0};
    uchar txn_mem[FD_TXN_MAX_SZ] __attribute__((aligned(8))) = {0};
    fd_txn_t * txn = (fd_txn_t *)txn_mem;
    txn->transaction_version   = versions[v];
    txn->signature_cnt         = 1U;
    txn->signature_off         = 400U;
    txn->acct_addr_off         = 32U;
    txn->acct_addr_cnt         = 5U;
    txn->readonly_unsigned_cnt = 1U;
    txn->recent_blockhash_off  = 240U;
    txn->instr_cnt             = 1U;
    txn->instr[0].program_id   = 2U;
    txn->instr[0].data_off     = 280U;
    txn->instr[0].data_sz      = 1U;
    fd_acct_addr_t * accts = (fd_acct_addr_t *)(payload+txn->acct_addr_off);
    for( ulong i=0UL; i<5UL; i++ ) fd_memset( accts[i].b, (int)(i+1UL), 32UL );
    accts[1] = reserved[key];
    if( loader ) fd_memcpy( accts[3].b, BPF_UPGRADEABLE_PROG_ID1, 32UL );
    uchar original[512]; fd_memcpy( original, payload, sizeof(payload) );
    fd_bank_abi_txn_t out[1];
    uchar sidecar[FD_BANK_ABI_TXN_FOOTPRINT_SIDECAR_MAX] __attribute__((aligned(8)));
    fd_blake3_t blake3[1];
    FD_TEST( fd_bank_abi_txn_init( out, sidecar, NULL, 1UL, blake3, payload, sizeof(payload), txn, 0 )==FD_BANK_ABI_TXN_INIT_SUCCESS );
    uchar const * cache = v==0UL ? out->message.legacy.is_writable_account_cache :
                         v==1UL ? out->message.v0.is_writable_account_cache : out->message.v1.is_writable_account_cache;
    FD_TEST( cache[0]==1U && cache[1]==0U && cache[2]==has_loader && cache[3]==!loader && cache[4]==0U );
    FD_TEST( !memcmp( original, payload, sizeof(payload) ) );
    FD_TEST( fd_txn_account_cnt( txn, FD_TXN_ACCT_CAT_WRITABLE )==4UL );
    fd_pubkey_t pubkey; fd_memcpy( pubkey.uc, reserved[key].b, 32UL );
    FD_TEST( fd_pubkey_is_active_reserved_key( &pubkey ) || fd_pubkey_is_pending_reserved_key( &pubkey ) );
  }

  /* Exercise the real ALT expansion and writable-cache placement, not a
     replacement for the resolver. */
  for( ulong key=0UL; key<sizeof(reserved)/sizeof(reserved[0]); key++ ) {
    uchar payload[512] = {0};
    uchar txn_mem[FD_TXN_MAX_SZ] __attribute__((aligned(8))) = {0};
    fd_txn_t * txn = (fd_txn_t *)txn_mem;
    txn->transaction_version         = FD_TXN_V0;
    txn->signature_cnt               = 1U;
    txn->signature_off               = 400U;
    txn->acct_addr_off               = 32U;
    txn->acct_addr_cnt               = 2U;
    txn->readonly_unsigned_cnt       = 1U;
    txn->recent_blockhash_off        = 240U;
    txn->addr_table_lookup_cnt       = 1U;
    txn->addr_table_adtl_cnt          = 2U;
    txn->addr_table_adtl_writable_cnt = 2U;
    fd_memset( payload+32UL, 1, 32UL );
    fd_memset( payload+64UL, 2, 32UL );
    fd_txn_acct_addr_lut_t * lut = fd_txn_get_address_tables( txn );
    lut->addr_off     = 96U;
    lut->writable_cnt = 2U;
    lut->writable_off = 280U;
    payload[281UL] = 1U;
    lookup_keys[0] = reserved[key];
    fd_memset( lookup_keys[1].b, 3, 32UL );
    fd_bank_abi_txn_t out[1];
    uchar sidecar[FD_BANK_ABI_TXN_FOOTPRINT_SIDECAR_MAX] __attribute__((aligned(8)));
    fd_blake3_t blake3[1];
    FD_TEST( fd_bank_abi_txn_init( out, sidecar, NULL, 1UL, blake3, payload, sizeof(payload), txn, 0 )==FD_BANK_ABI_TXN_INIT_SUCCESS );
    uchar const * cache = out->message.v0.is_writable_account_cache;
    FD_TEST( cache[0]==1U && cache[1]==0U && cache[2]==0U && cache[3]==1U );
    FD_TEST( out->message.v0.loaded_addresses.owned.writable_cnt==2UL );
    FD_TEST( fd_txn_account_cnt( txn, FD_TXN_ACCT_CAT_WRITABLE )==3UL );
  }
}

int
main( int argc, char ** argv ) {
  fd_boot( &argc, &argv );
  test_reserved_permissions();
  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
