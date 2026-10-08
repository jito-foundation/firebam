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
  int          program;
  fd_pubkey_t  program_key;
  uchar *      program_data;
  ulong        program_sz;
  ulong        alt_idx;
  ulong        nonce_idx;
  ulong        program_idx;
} genesis_bam_preseed_t;

# define REQUIRE(c)                         \
  do {                                      \
    if( FD_UNLIKELY( !(c) ) ) {             \
      FD_LOG_WARNING(( "FAIL: %s", #c ));   \
      return 0;                             \
    }                                       \
  } while(0);

/* genesis_bam_preseed_reserve builds the data of the enabled preseeded
   accounts in scratch and reserves their account table slots at
   *accounts_len.  Returns 0 on failure (logged). */

static int
genesis_bam_preseed_reserve( genesis_bam_preseed_t *  p,
                             genesis_solana_t const * genesis,
                             ulong *                  accounts_len ) {
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
  char const * program_id   = getenv( "FD_DEV_PRESEED_BAM_PROGRAM_ID" );
  p->program                = program_path && program_path[0];

  if( FD_UNLIKELY( p->program ) ) {
    REQUIRE( program_id && program_id[0] );
    REQUIRE( fd_base58_decode_32( program_id, p->program_key.key ) );

    FILE * program_file = fopen( program_path, "rb" );
    REQUIRE( program_file );
    REQUIRE( !fseek( program_file, 0L, SEEK_END ) );
    long program_sz = ftell( program_file );
    REQUIRE( program_sz>0L );
    REQUIRE( !fseek( program_file, 0L, SEEK_SET ) );

    p->program_sz   = (ulong)program_sz;
    p->program_data = fd_scratch_alloc( 8UL, p->program_sz );
    REQUIRE( fread( p->program_data,
                    1UL,
                    p->program_sz,
                    program_file )==p->program_sz );
    REQUIRE( !fclose( program_file ) );
  }

  p->alt_idx = *accounts_len;
  REQUIRE( !__builtin_add_overflow( *accounts_len, p->alt ? 1UL : 0UL, accounts_len ) );
  p->nonce_idx = *accounts_len;
  REQUIRE( !__builtin_add_overflow( *accounts_len, p->nonce ? 1UL : 0UL, accounts_len ) );
  p->program_idx = *accounts_len;
  REQUIRE( !__builtin_add_overflow( *accounts_len, p->program ? 1UL : 0UL, accounts_len ) );
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

  if( FD_UNLIKELY( p->program ) ) {
    genesis->accounts[ p->program_idx ] = (fd_genesis_account_pair_t) {
      .key     = p->program_key,
      .account = (fd_genesis_account_t) {
        .lamports =
            fd_ulong_max( 1UL, fd_rent_exempt_minimum_balance( &genesis->rent, p->program_sz ) ),
        .data_len   = p->program_sz,
        .data       = p->program_data,
        .owner      = fd_solana_bpf_loader_program_id,
        .executable = 1U,
        .rent_epoch = ULONG_MAX
      }
    };
  }
}
