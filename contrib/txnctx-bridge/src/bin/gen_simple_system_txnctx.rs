use std::{fs, path::PathBuf, str::FromStr};

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use prost::Message as ProstMessage;
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_hash::Hash;
use solana_keypair::Keypair;
use solana_message::{v0, AddressLookupTableAccount, VersionedMessage};
use solana_pubkey::Pubkey;
use solana_signer::Signer;
use solana_system_interface::instruction::{
    allocate, assign, create_account, create_account_with_seed, transfer, transfer_with_seed,
};
use solana_transaction::{versioned::VersionedTransaction, Transaction};

pub mod org {
    pub mod solana {
        pub mod sealevel {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/org.solana.sealevel.v1.rs"));
            }
        }
    }
}

use crate::org::solana::sealevel::v1::{
    CompiledInstruction, SanitizedTransaction, TransactionMessage, TxnContext,
};

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, ValueEnum)]
enum SystemKindArg {
    #[default]
    Transfer,
    FeeOnly,
    Assign,
    Allocate,
    CreateAccount,
    TransferWithSeed,
    CreateAccountWithSeed,
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Output .txnctx path
    #[arg(long)]
    output: PathBuf,

    /// Which System-program transaction shape to generate
    #[arg(long, value_enum, default_value_t = SystemKindArg::Transfer)]
    system_kind: SystemKindArg,

    /// Base lamports for the synthetic System-program instruction(s)
    #[arg(long, default_value_t = 123_456)]
    lamports: u64,

    /// Additional lamports added per transfer after the first
    #[arg(long, default_value_t = 0)]
    lamports_step: u64,

    /// Number of synthetic transfer instructions to include
    #[arg(long, default_value_t = 1)]
    transfer_count: usize,

    /// Compute-unit limit for the synthetic compute-budget instruction
    #[arg(long, default_value_t = 300_000)]
    cu_limit: u32,

    /// Optional compute-unit price instruction to prepend
    #[arg(long)]
    cu_price: Option<u64>,

    /// Optional heap-frame request to prepend
    #[arg(long)]
    heap_frame: Option<u32>,

    /// Optional loaded-accounts-data-size-limit instruction to prepend
    #[arg(long)]
    loaded_accounts_data_size_limit: Option<u32>,

    /// Recent blockhash to sign with. Defaults to a deterministic dummy hash for bridge adaptation.
    #[arg(long)]
    recent_blockhash: Option<String>,

    /// Emit a signed v0 message with an extra bogus address-table lookup.
    #[arg(long, default_value_t = false)]
    bogus_address_table_lookup: bool,

    /// Emit a signed v0 message with an extra lookup against this real address table.
    #[arg(long)]
    address_table_lookup: Option<String>,

    /// Deterministic nonexistent lookup table seed used with --bogus-address-table-lookup.
    #[arg(long, default_value_t = 9_999_999)]
    bogus_lookup_seed_index: u64,

    /// Deterministic source secret seed index
    #[arg(long, default_value_t = 9)]
    from_seed_index: u64,

    /// Deterministic destination secret seed index (increments for each extra transfer)
    #[arg(long, default_value_t = 10)]
    to_seed_index: u64,

    /// Space to allocate when --system-kind=create-account, allocate, or create-account-with-seed
    #[arg(long, default_value_t = 0)]
    space: u64,

    /// Deterministic owner secret seed index when --system-kind=create-account, assign, transfer-with-seed, or create-account-with-seed
    #[arg(long, default_value_t = 42)]
    owner_seed_index: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.system_kind == SystemKindArg::Transfer {
        anyhow::ensure!(
            args.transfer_count > 0,
            "--transfer-count must be at least 1"
        );
    }

    let from = seeded_keypair(args.from_seed_index);
    let recent_blockhash = match args.recent_blockhash.as_ref() {
        Some(value) => Hash::from_str(value).context("failed to parse --recent-blockhash")?,
        None => Hash::new_from_array([7u8; 32]),
    };

    if args.bogus_address_table_lookup {
        anyhow::ensure!(
            args.system_kind == SystemKindArg::Transfer,
            "--bogus-address-table-lookup is only supported with --system-kind=transfer"
        );
    }
    if args.address_table_lookup.is_some() {
        anyhow::ensure!(
            args.system_kind == SystemKindArg::Transfer,
            "--address-table-lookup is only supported with --system-kind=transfer"
        );
        anyhow::ensure!(
            !args.bogus_address_table_lookup,
            "--address-table-lookup cannot be combined with --bogus-address-table-lookup"
        );
    }

    let mut instructions = Vec::new();
    if let Some(heap_frame) = args.heap_frame {
        instructions.push(ComputeBudgetInstruction::request_heap_frame(heap_frame));
    }
    instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(
        args.cu_limit,
    ));
    if let Some(cu_price) = args.cu_price {
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(cu_price));
    }
    if let Some(limit) = args.loaded_accounts_data_size_limit {
        instructions.push(ComputeBudgetInstruction::set_loaded_accounts_data_size_limit(limit));
    }

    let (target_pubkey, total_lamports, tx_label) = match args.system_kind {
        SystemKindArg::FeeOnly => {
            let to = seeded_keypair(args.to_seed_index);
            (
                to.pubkey(),
                0,
                "system_kind=fee_only transfer_count=0 lamports_step=0".to_string(),
            )
        }
        SystemKindArg::Transfer => {
            let mut total_lamports = 0u64;
            for transfer_idx in 0..args.transfer_count {
                let to = seeded_keypair(args.to_seed_index + transfer_idx as u64);
                let lamports_delta = args
                    .lamports_step
                    .checked_mul(transfer_idx as u64)
                    .context("transfer lamports step overflowed")?;
                let lamports = args
                    .lamports
                    .checked_add(lamports_delta)
                    .context("transfer lamports overflowed")?;
                total_lamports = total_lamports
                    .checked_add(lamports)
                    .context("total synthetic transfer lamports overflowed")?;
                instructions.push(transfer(&from.pubkey(), &to.pubkey(), lamports));
            }

            (
                seeded_keypair(args.to_seed_index).pubkey(),
                total_lamports,
                format!(
                    "system_kind=transfer transfer_count={} lamports_step={}",
                    args.transfer_count, args.lamports_step
                ),
            )
        }
        SystemKindArg::CreateAccount => {
            let to = seeded_keypair(args.to_seed_index);
            let owner = seeded_keypair(args.owner_seed_index).pubkey();
            instructions.push(create_account(
                &from.pubkey(),
                &to.pubkey(),
                args.lamports,
                args.space,
                &owner,
            ));
            (
                to.pubkey(),
                args.lamports,
                format!(
                    "system_kind=create_account space={} owner={}",
                    args.space, owner
                ),
            )
        }
        SystemKindArg::Allocate => {
            let to = seeded_keypair(args.to_seed_index);
            instructions.push(allocate(&to.pubkey(), args.space));
            (
                to.pubkey(),
                0,
                format!("system_kind=allocate space={}", args.space),
            )
        }
        SystemKindArg::Assign => {
            let to = seeded_keypair(args.to_seed_index);
            let owner = seeded_keypair(args.owner_seed_index).pubkey();
            instructions.push(assign(&to.pubkey(), &owner));
            (
                to.pubkey(),
                0,
                format!("system_kind=assign owner={}", owner),
            )
        }
        SystemKindArg::TransferWithSeed => {
            let to = seeded_keypair(args.to_seed_index);
            let from_owner = seeded_keypair(args.owner_seed_index).pubkey();
            let from_seed = seeded_address_seed(args.to_seed_index);
            let seeded_from = seeded_address(&from.pubkey(), &from_seed, &from_owner)?;
            instructions.push(transfer_with_seed(
                &seeded_from,
                &from.pubkey(),
                from_seed.clone(),
                &from_owner,
                &to.pubkey(),
                args.lamports,
            ));
            (
                to.pubkey(),
                args.lamports,
                format!(
                    "system_kind=transfer_with_seed from_seed={:?} from_owner={} seeded_from={}",
                    from_seed, from_owner, seeded_from
                ),
            )
        }
        SystemKindArg::CreateAccountWithSeed => {
            let owner = seeded_keypair(args.owner_seed_index).pubkey();
            let seed = seeded_address_seed(args.to_seed_index);
            let seeded_target = seeded_address(&from.pubkey(), &seed, &owner)?;
            instructions.push(create_account_with_seed(
                &from.pubkey(),
                &seeded_target,
                &from.pubkey(),
                &seed,
                args.lamports,
                args.space,
                &owner,
            ));
            (
                seeded_target,
                args.lamports,
                format!(
                    "system_kind=create_account_with_seed space={} owner={} seed={:?}",
                    args.space, owner, seed
                ),
            )
        }
    };

    let mut tx = Transaction::new_with_payer(&instructions, Some(&from.pubkey()));
    match args.system_kind {
        SystemKindArg::Transfer | SystemKindArg::FeeOnly => tx.sign(&[&from], recent_blockhash),
        SystemKindArg::CreateAccount => {
            let to = seeded_keypair(args.to_seed_index);
            tx.sign(&[&from, &to], recent_blockhash);
        }
        SystemKindArg::Allocate => {
            let to = seeded_keypair(args.to_seed_index);
            tx.sign(&[&from, &to], recent_blockhash);
        }
        SystemKindArg::Assign => {
            let to = seeded_keypair(args.to_seed_index);
            tx.sign(&[&from, &to], recent_blockhash);
        }
        SystemKindArg::TransferWithSeed | SystemKindArg::CreateAccountWithSeed => {
            tx.sign(&[&from], recent_blockhash);
        }
    }

    let txn_ctx = if args.bogus_address_table_lookup || args.address_table_lookup.is_some() {
        let lookup_key = match args.address_table_lookup.as_ref() {
            Some(value) => {
                Pubkey::from_str(value).context("failed to parse --address-table-lookup")?
            }
            None => seeded_keypair(args.bogus_lookup_seed_index).pubkey(),
        };
        let message = v0::Message::try_compile(
            &from.pubkey(),
            &instructions,
            &[AddressLookupTableAccount {
                key: lookup_key,
                addresses: vec![target_pubkey],
            }],
            recent_blockhash,
        )
        .context("failed to compile synthetic v0 transaction with address table lookup")?;
        let vtx = VersionedTransaction::try_new(VersionedMessage::V0(message), &[&from])
            .context("failed to sign synthetic v0 transaction")?;
        txn_context_from_versioned_transaction(&vtx)
    } else {
        txn_context_from_legacy_transaction(&tx)
    };

    let encoded = txn_ctx.encode_to_vec();
    fs::write(&args.output, encoded)
        .with_context(|| format!("failed to write {}", args.output.display()))?;

    println!(
        "wrote synthetic txnctx to {} from={} target={} total_lamports={} {} cu_limit={} cu_price={} heap_frame={} loaded_accounts_data_size_limit={} recent_blockhash={} bogus_address_table_lookup={}",
        args.output.display(),
        from.pubkey(),
        target_pubkey,
        total_lamports,
        tx_label,
        args.cu_limit,
        args.cu_price
            .map(|v| v.to_string())
            .unwrap_or_else(|| "none".to_string()),
        args.heap_frame
            .map(|v| v.to_string())
            .unwrap_or_else(|| "none".to_string()),
        args.loaded_accounts_data_size_limit
            .map(|v| v.to_string())
            .unwrap_or_else(|| "none".to_string()),
        recent_blockhash,
        args.bogus_address_table_lookup
    );
    if let Some(lookup) = args.address_table_lookup.as_ref() {
        println!("address_table_lookup={lookup}");
    }

    Ok(())
}

fn txn_context_from_legacy_transaction(tx: &Transaction) -> TxnContext {
    TxnContext {
        tx: Some(SanitizedTransaction {
            message: Some(TransactionMessage {
                is_legacy: true,
                header: Some(crate::org::solana::sealevel::v1::MessageHeader {
                    num_required_signatures: u32::from(tx.message.header.num_required_signatures),
                    num_readonly_signed_accounts: u32::from(
                        tx.message.header.num_readonly_signed_accounts,
                    ),
                    num_readonly_unsigned_accounts: u32::from(
                        tx.message.header.num_readonly_unsigned_accounts,
                    ),
                }),
                account_keys: tx
                    .message
                    .account_keys
                    .iter()
                    .map(|key| key.to_bytes().to_vec())
                    .collect(),
                recent_blockhash: tx.message.recent_blockhash.to_bytes().to_vec(),
                instructions: tx
                    .message
                    .instructions
                    .iter()
                    .map(|ix| CompiledInstruction {
                        program_id_index: u32::from(ix.program_id_index),
                        accounts: ix.accounts.iter().map(|idx| u32::from(*idx)).collect(),
                        data: ix.data.clone(),
                    })
                    .collect(),
                address_table_lookups: Vec::new(),
            }),
            message_hash: Vec::new(),
            signatures: tx
                .signatures
                .iter()
                .map(|sig| sig.as_ref().to_vec())
                .collect(),
        }),
        ..TxnContext::default()
    }
}

fn txn_context_from_versioned_transaction(tx: &VersionedTransaction) -> TxnContext {
    match &tx.message {
        VersionedMessage::Legacy(message) => TxnContext {
            tx: Some(SanitizedTransaction {
                message: Some(TransactionMessage {
                    is_legacy: true,
                    header: Some(crate::org::solana::sealevel::v1::MessageHeader {
                        num_required_signatures: u32::from(message.header.num_required_signatures),
                        num_readonly_signed_accounts: u32::from(
                            message.header.num_readonly_signed_accounts,
                        ),
                        num_readonly_unsigned_accounts: u32::from(
                            message.header.num_readonly_unsigned_accounts,
                        ),
                    }),
                    account_keys: message
                        .account_keys
                        .iter()
                        .map(|key| key.to_bytes().to_vec())
                        .collect(),
                    recent_blockhash: message.recent_blockhash.to_bytes().to_vec(),
                    instructions: message
                        .instructions
                        .iter()
                        .map(|ix| CompiledInstruction {
                            program_id_index: u32::from(ix.program_id_index),
                            accounts: ix.accounts.iter().map(|idx| u32::from(*idx)).collect(),
                            data: ix.data.clone(),
                        })
                        .collect(),
                    address_table_lookups: Vec::new(),
                }),
                message_hash: Vec::new(),
                signatures: tx
                    .signatures
                    .iter()
                    .map(|sig| sig.as_ref().to_vec())
                    .collect(),
            }),
            ..TxnContext::default()
        },
        VersionedMessage::V0(message) => TxnContext {
            tx: Some(SanitizedTransaction {
                message: Some(TransactionMessage {
                    is_legacy: false,
                    header: Some(crate::org::solana::sealevel::v1::MessageHeader {
                        num_required_signatures: u32::from(message.header.num_required_signatures),
                        num_readonly_signed_accounts: u32::from(
                            message.header.num_readonly_signed_accounts,
                        ),
                        num_readonly_unsigned_accounts: u32::from(
                            message.header.num_readonly_unsigned_accounts,
                        ),
                    }),
                    account_keys: message
                        .account_keys
                        .iter()
                        .map(|key| key.to_bytes().to_vec())
                        .collect(),
                    recent_blockhash: message.recent_blockhash.to_bytes().to_vec(),
                    instructions: message
                        .instructions
                        .iter()
                        .map(|ix| CompiledInstruction {
                            program_id_index: u32::from(ix.program_id_index),
                            accounts: ix.accounts.iter().map(|idx| u32::from(*idx)).collect(),
                            data: ix.data.clone(),
                        })
                        .collect(),
                    address_table_lookups: message
                        .address_table_lookups
                        .iter()
                        .map(
                            |lookup| crate::org::solana::sealevel::v1::MessageAddressTableLookup {
                                account_key: lookup.account_key.to_bytes().to_vec(),
                                writable_indexes: lookup
                                    .writable_indexes
                                    .iter()
                                    .map(|idx| u32::from(*idx))
                                    .collect(),
                                readonly_indexes: lookup
                                    .readonly_indexes
                                    .iter()
                                    .map(|idx| u32::from(*idx))
                                    .collect(),
                            },
                        )
                        .collect(),
                }),
                message_hash: Vec::new(),
                signatures: tx
                    .signatures
                    .iter()
                    .map(|sig| sig.as_ref().to_vec())
                    .collect(),
            }),
            ..TxnContext::default()
        },
    }
}

fn seeded_keypair(index: u64) -> Keypair {
    let mut seed = [0u8; 32];
    seed[..8].copy_from_slice(&index.to_le_bytes());
    Keypair::new_from_array(seed)
}

fn seeded_address_seed(index: u64) -> String {
    format!("bamseed{index:016x}")
}

fn seeded_address(base: &Pubkey, seed: &str, owner: &Pubkey) -> Result<Pubkey> {
    Pubkey::create_with_seed(base, seed, owner).map_err(|err| {
        anyhow::anyhow!(
            "failed to derive synthetic seeded address from base {} seed {:?} owner {}: {}",
            base,
            seed,
            owner,
            err
        )
    })
}
