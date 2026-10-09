/* FireBAM: optional deterministic genesis accounts for live
   full-Firedancer BAM fuzzing (contrib/fuzz-local-bam-stateful.sh),
   enabled by the FD_DEV_PRESEED_BAM_* environment variables.  Included
   once by fd_genesis_create.c after genesis_solana_t. */

#include "../runtime/fd_alut.h"
#include "../runtime/program/fd_system_program.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

static int
env_truthy( char const * name ) {
  char const * val = getenv( name );
  if( FD_UNLIKELY( !val ) ) return 0;
  return !strcmp( val, "1" ) || !strcmp( val, "true" ) || !strcmp( val, "yes" );
}

static ulong
env_ulong_or( char const * name,
              ulong        fallback ) {
  char const * val = getenv( name );
  if( FD_LIKELY( !val || !val[0] ) ) return fallback;

  errno = 0;
  char * end = NULL;
  unsigned long long parsed = strtoull( val, &end, 10 );
  if( FD_UNLIKELY( errno || !end || end[0] || parsed>ULONG_MAX ) ) {
    FD_LOG_WARNING(( "Ignoring invalid %s=%s", name, val ));
    return fallback;
  }
  return (ulong)parsed;
}

static void
pubkey_from_dev_seed( fd_pubkey_t * pubkey,
                      ulong         seed ) {
  uchar privkey[ 32 ] = {0};
  FD_STORE( ulong, privkey, seed );
  fd_sha512_t sha[1];
  fd_ed25519_public_from_private( pubkey->key, privkey, sha );
}

typedef struct {
  int          alt;
  fd_pubkey_t  alt_table;
  fd_pubkey_t  alt_address;
  uchar *      alt_data;
  ulong        alt_data_sz;
  int          nonce;
  fd_pubkey_t  nonce_account;
  fd_pubkey_t  nonce_auth;
  fd_pubkey_t  nonce_hash_key;
  fd_hash_t    nonce_hash;
  uchar *      nonce_data;
  ulong        alt_idx;
  ulong        nonce_idx;
  ulong        acct_idx;
  ulong        acct_cnt;
  fd_genesis_account_pair_t acct[ 14 ]; /* program ELF, tip programs */
} genesis_bam_preseed_t;

# define REQUIRE(c)                         \
  do {                                      \
    if( FD_UNLIKELY( !(c) ) ) {             \
      FD_LOG_WARNING(( "FAIL: %s", #c ));   \
      return 0;                             \
    }                                       \
  } while(0);

/* genesis_bam_program appends the BPF loader v2 program at the base58
   id with the ELF at path, deployed as jito-solana's test validator
   deploys program-binaries.  Returns 0 on failure. */

static int
genesis_bam_program( genesis_bam_preseed_t *  p,
                     genesis_solana_t const * genesis,
                     char const *             path,
                     char const *             id ) {
  FD_TEST( p->acct_cnt<14UL );
  fd_genesis_account_pair_t * a = &p->acct[ p->acct_cnt++ ];
  REQUIRE( id && fd_base58_decode_32( id, a->key.key ) );
  FILE * file = fopen( path, "rb" );
  REQUIRE( file );
  REQUIRE( !fseek( file, 0L, SEEK_END ) );
  long sz = ftell( file );
  REQUIRE( sz>0L );
  REQUIRE( !fseek( file, 0L, SEEK_SET ) );
  uchar * elf = fd_scratch_alloc( 8UL, (ulong)sz );
  REQUIRE( fread( elf, 1UL, (ulong)sz, file )==(ulong)sz );
  REQUIRE( !fclose( file ) );
  a->account = (fd_genesis_account_t) {
    .lamports   = fd_ulong_max( 1UL, fd_rent_exempt_minimum_balance( &genesis->rent, (ulong)sz ) ),
    .data_len   = (ulong)sz,
    .data       = elf,
    .owner      = fd_solana_bpf_loader_program_id,
    .executable = 1U,
    .rent_epoch = ULONG_MAX
  };
  return 1;
}

/* genesis_bam_pda appends the rent exempt sz byte account that an
   Anchor initialize creates at program's PDA for seed, starting with
   the account discriminator disc.  Stores the bump at *bump and returns
   the zero filled data after disc. */

static uchar *
genesis_bam_pda( genesis_bam_preseed_t *  p,
                 genesis_solana_t const * genesis,
                 fd_pubkey_t const *      program,
                 char const *             seed,
                 ulong                    disc,
                 ulong                    sz,
                 uchar *                  bump ) {
  FD_TEST( p->acct_cnt<14UL );
  fd_genesis_account_pair_t * a = &p->acct[ p->acct_cnt++ ];
  uchar const * seeds[ 1 ] = { (uchar const *)seed };
  ulong         seed_sz    = strlen( seed );
  uint          err;
  FD_TEST( FD_PUBKEY_SUCCESS==fd_pubkey_find_program_address( program, 1UL, seeds, &seed_sz, &a->key, bump, &err ) );
  uchar * data = fd_scratch_alloc( 8UL, sz );
  fd_memset( data, 0, sz );
  FD_STORE( ulong, data, disc );
  a->account = (fd_genesis_account_t) {
    .lamports = fd_rent_exempt_minimum_balance( &genesis->rent, sz ),
    .data_len = sz,
    .data     = data,
    .owner    = *program
  };
  return data;
}

/* genesis_bam_tip_preseed adds the Jito tip programs as deployed and
   initialized on mainnet, which the tip crank needs but does not set up
   (fd_bundle_crank.h): the v0.1.10 tip payment and tip distribution
   programs and SPL Memo v3 from dir (jito-solana's
   program-binaries/src/programs) at the ids they declare, and the
   accounts their initialize instructions create (tip_manager.rs), with
   payer as the payer and authority.  Returns 0 on failure. */

static int
genesis_bam_tip_preseed( genesis_bam_preseed_t *  p,
                         genesis_solana_t const * genesis,
                         fd_pubkey_t const *      payer,
                         char const *             dir ) {
  static char const * const progs[ 3 ][ 2 ] = {
    { "spl-jito_tip_payment-0.1.10.so",      "T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt" },
    { "spl-jito_tip_distribution-0.1.10.so", "4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7" },
    { "spl_memo-3.0.0.so",                   "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr" } };
  for( ulong i=0UL; i<3UL; i++ ) {
    char path[ PATH_MAX ];
    REQUIRE( fd_cstr_printf_check( path, PATH_MAX, NULL, "%s/%s", dir, progs[ i ][ 0 ] ) );
    REQUIRE( genesis_bam_program( p, genesis, path, progs[ i ][ 1 ] ) );
  }
  fd_pubkey_t const * tip_payment = &p->acct[ p->acct_cnt-3UL ].key;
  fd_pubkey_t const * tip_distr   = &p->acct[ p->acct_cnt-2UL ].key;

  /* Tip payment Config: tip_receiver, block_builder,
     block_builder_commission_pct, then the config and 8 tip account
     bumps.  The discriminators are Anchor's "account:Config" and
     "account:TipPaymentAccount". */
  uchar   bump;
  uchar * cfg = genesis_bam_pda( p, genesis, tip_payment, "CONFIG_ACCOUNT", 0x82ccfa1ee0aa0c9bUL, 89UL, &bump );
  fd_memcpy( cfg+ 8UL, payer->key, 32UL );
  fd_memcpy( cfg+40UL, payer->key, 32UL );
  cfg[ 80 ] = bump;
  char seed[] = "TIP_ACCOUNT_0";
  for( ulong i=0UL; i<8UL; i++, seed[ 12 ]++ ) {
    genesis_bam_pda( p, genesis, tip_payment, seed, 0x286144e074f421c9UL, 8UL, cfg+81UL+i );
  }

  /* Tip distribution Config: authority, expired_funds_account,
     num_epochs_valid, max_validator_commission_bps and bump. */
  cfg = genesis_bam_pda( p, genesis, tip_distr, "CONFIG_ACCOUNT", 0x82ccfa1ee0aa0c9bUL, 88UL, &bump );
  fd_memcpy( cfg+ 8UL, payer->key, 32UL );
  fd_memcpy( cfg+40UL, payer->key, 32UL );
  FD_STORE( ulong,  cfg+72UL, 10UL );
  FD_STORE( ushort, cfg+80UL, (ushort)10000 );
  cfg[ 82 ] = bump;
  return 1;
}

/* genesis_bam_preseed_reserve builds the data of the enabled preseeded
   accounts in scratch and reserves their account table slots at
   *accounts_len.  Returns 0 on failure (logged). */

static int
genesis_bam_preseed_reserve( genesis_bam_preseed_t *      p,
                             genesis_solana_t const *     genesis,
                             fd_genesis_options_t const * options,
                             ulong *                      accounts_len ) {
  memset( p, 0, sizeof(genesis_bam_preseed_t) );
  p->alt = env_truthy( "FD_DEV_PRESEED_BAM_ALT" );

  if( FD_UNLIKELY( p->alt ) ) {
    ulong table_seed   = env_ulong_or( "FD_DEV_PRESEED_BAM_ALT_TABLE_SEED", 424242UL );
    ulong address_seed = env_ulong_or( "FD_DEV_PRESEED_BAM_ALT_ADDRESS_SEED", 1UL );

    pubkey_from_dev_seed( &p->alt_table,   table_seed   );
    pubkey_from_dev_seed( &p->alt_address, address_seed );

    p->alt_data_sz = FD_LOOKUP_TABLE_META_SIZE + sizeof(fd_pubkey_t);
    p->alt_data = fd_scratch_alloc( alignof(fd_pubkey_t), p->alt_data_sz );

    fd_alut_meta_t meta = {
      .discriminant                   = FD_ALUT_STATE_DISC_LOOKUP_TABLE,
      .deactivation_slot              = ULONG_MAX,
      .last_extended_slot             = 0UL,
      .last_extended_slot_start_index = 1U,
      .has_authority                  = 0U,
    };
    REQUIRE( !fd_alut_state_encode( &meta, p->alt_data, FD_LOOKUP_TABLE_META_SIZE ) );
    fd_memcpy( p->alt_data + FD_LOOKUP_TABLE_META_SIZE,
               p->alt_address.key,
               sizeof(fd_pubkey_t) );
  }

  p->nonce = env_truthy( "FD_DEV_PRESEED_BAM_NONCE" );

  if( FD_UNLIKELY( p->nonce ) ) {
    ulong account_seed = env_ulong_or( "FD_DEV_PRESEED_BAM_NONCE_ACCOUNT_SEED", 900000UL );
    ulong auth_seed    = env_ulong_or( "FD_DEV_PRESEED_BAM_NONCE_AUTH_SEED",         0UL );
    ulong hash_seed    = env_ulong_or( "FD_DEV_PRESEED_BAM_NONCE_HASH_SEED",   1900000UL );

    pubkey_from_dev_seed( &p->nonce_account,  account_seed );
    pubkey_from_dev_seed( &p->nonce_auth,     auth_seed    );
    pubkey_from_dev_seed( &p->nonce_hash_key, hash_seed    );
    fd_memcpy( p->nonce_hash.hash,
               p->nonce_hash_key.key,
               sizeof(fd_hash_t) );

    p->nonce_data =
        fd_scratch_alloc( alignof(fd_nonce_state_versions_t), FD_SYSTEM_PROGRAM_NONCE_DLEN );
    fd_memset( p->nonce_data, 0, FD_SYSTEM_PROGRAM_NONCE_DLEN );

    fd_nonce_state_versions_t state = {
      .version                = FD_NONCE_VERSION_CURRENT,
      .kind                   = FD_NONCE_STATE_INITIALIZED,
      .authority              = p->nonce_auth,
      .durable_nonce          = p->nonce_hash,
      .lamports_per_signature = genesis->fee_rate_governor.target_lamports_per_signature
    };
    ulong written = 0UL;
    REQUIRE( !fd_nonce_state_versions_encode(
        &state, p->nonce_data, FD_SYSTEM_PROGRAM_NONCE_DLEN, &written ) );
    REQUIRE( written==FD_SYSTEM_PROGRAM_NONCE_DLEN );
  }

  char const * program_path = getenv( "FD_DEV_PRESEED_BAM_PROGRAM_ELF" );
  if( FD_UNLIKELY( program_path && program_path[0] ) ) {
    REQUIRE( genesis_bam_program( p, genesis, program_path, getenv( "FD_DEV_PRESEED_BAM_PROGRAM_ID" ) ) );
  }

  char const * tip_dir = getenv( "FD_DEV_PRESEED_BAM_TIP_PROGRAMS_DIR" );
  if( FD_UNLIKELY( tip_dir && tip_dir[0] ) ) {
    REQUIRE( genesis_bam_tip_preseed( p, genesis, &options->faucet_pubkey, tip_dir ) );
  }

  p->alt_idx = *accounts_len;
  REQUIRE( !__builtin_add_overflow( *accounts_len, p->alt ? 1UL : 0UL, accounts_len ) );
  p->nonce_idx = *accounts_len;
  REQUIRE( !__builtin_add_overflow( *accounts_len, p->nonce ? 1UL : 0UL, accounts_len ) );
  p->acct_idx = *accounts_len;
  REQUIRE( !__builtin_add_overflow( *accounts_len, p->acct_cnt, accounts_len ) );
  return 1;
}

# undef REQUIRE

/* genesis_bam_preseed_fill writes the reserved preseeded accounts into
   genesis->accounts. */

static void
genesis_bam_preseed_fill( genesis_bam_preseed_t const * p,
                          genesis_solana_t *            genesis ) {
  if( FD_UNLIKELY( p->alt ) ) {
    genesis->accounts[ p->alt_idx ] = (fd_genesis_account_pair_t) {
      .key     = p->alt_table,
      .account = (fd_genesis_account_t) {
        .lamports = fd_rent_exempt_minimum_balance( &genesis->rent, p->alt_data_sz ),
        .data_len = p->alt_data_sz,
        .data     = p->alt_data,
        .owner    = fd_solana_address_lookup_table_program_id
      }
    };
  }

  if( FD_UNLIKELY( p->nonce ) ) {
    genesis->accounts[ p->nonce_idx ] = (fd_genesis_account_pair_t) {
      .key     = p->nonce_account,
      .account = (fd_genesis_account_t) {
        .lamports = fd_rent_exempt_minimum_balance( &genesis->rent, FD_SYSTEM_PROGRAM_NONCE_DLEN ),
        .data_len = FD_SYSTEM_PROGRAM_NONCE_DLEN,
        .data     = p->nonce_data,
        .owner    = fd_solana_system_program_id
      }
    };
  }

  for( ulong i=0UL; i<p->acct_cnt; i++ ) genesis->accounts[ p->acct_idx+i ] = p->acct[ i ];
}
