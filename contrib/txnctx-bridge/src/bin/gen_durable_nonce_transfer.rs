use std::{fs, path::PathBuf, str::FromStr};

use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use clap::Parser;
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_hash::Hash;
use solana_keypair::{read_keypair_file, Keypair};
use solana_message::Message;
use solana_signer::Signer;
use solana_system_interface::instruction::transfer;
use solana_transaction::Transaction;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Output base64 packet path
    #[arg(long)]
    output: PathBuf,

    /// Durable nonce blockhash stored in the nonce account
    #[arg(long)]
    nonce_hash: String,

    /// Nonce account keypair path
    #[arg(long)]
    nonce_keypair: PathBuf,

    /// Optional nonce authority keypair path. Defaults to --from-keypair.
    #[arg(long)]
    nonce_authority_keypair: Option<PathBuf>,

    /// Optional source/payer keypair path. Defaults to deterministic --from-seed-index.
    #[arg(long)]
    from_keypair: Option<PathBuf>,

    /// Deterministic source secret seed index when --from-keypair is absent
    #[arg(long, default_value_t = 0)]
    from_seed_index: u64,

    /// Deterministic destination secret seed index
    #[arg(long, default_value_t = 1)]
    to_seed_index: u64,

    /// Transfer lamports
    #[arg(long, default_value_t = 123_456)]
    lamports: u64,

    /// Compute-unit limit for the synthetic compute-budget instruction
    #[arg(long, default_value_t = 300_000)]
    cu_limit: u32,

    /// Optional compute-unit price instruction to prepend after nonce advance
    #[arg(long)]
    cu_price: Option<u64>,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let generated_from;
    let from = if let Some(path) = args.from_keypair.as_ref() {
        read_keypair_file(path).map_err(|err| {
            anyhow::anyhow!("failed to read keypair from {}: {}", path.display(), err)
        })?
    } else {
        generated_from = seeded_keypair(args.from_seed_index);
        generated_from
    };
    let nonce_account = read_keypair_file(&args.nonce_keypair).map_err(|err| {
        anyhow::anyhow!(
            "failed to read nonce keypair from {}: {}",
            args.nonce_keypair.display(),
            err
        )
    })?;
    let nonce_authority = match args.nonce_authority_keypair.as_ref() {
        Some(path) => read_keypair_file(path).map_err(|err| {
            anyhow::anyhow!(
                "failed to read nonce authority keypair from {}: {}",
                path.display(),
                err
            )
        })?,
        None => from.insecure_clone(),
    };

    let to = seeded_keypair(args.to_seed_index);
    let nonce_hash = Hash::from_str(&args.nonce_hash).context("failed to parse --nonce-hash")?;

    let mut instructions = vec![ComputeBudgetInstruction::set_compute_unit_limit(
        args.cu_limit,
    )];
    if let Some(cu_price) = args.cu_price {
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(cu_price));
    }
    instructions.push(transfer(&from.pubkey(), &to.pubkey(), args.lamports));

    let message = Message::new_with_nonce(
        instructions,
        Some(&from.pubkey()),
        &nonce_account.pubkey(),
        &nonce_authority.pubkey(),
    );
    let mut tx = Transaction::new_unsigned(message);
    if from.pubkey() == nonce_authority.pubkey() {
        tx.sign(&[&from], nonce_hash);
    } else {
        tx.sign(&[&from, &nonce_authority], nonce_hash);
    }

    let packet =
        bincode::serialize(&tx).context("failed to serialize durable nonce transaction")?;
    fs::write(
        &args.output,
        format!("{}\n", BASE64_STANDARD.encode(&packet)),
    )
    .with_context(|| format!("failed to write {}", args.output.display()))?;

    println!(
        "wrote durable nonce packet output={} from={} target={} nonce_account={} nonce_authority={} nonce_hash={} lamports={} cu_limit={} cu_price={} signature={}",
        args.output.display(),
        from.pubkey(),
        to.pubkey(),
        nonce_account.pubkey(),
        nonce_authority.pubkey(),
        nonce_hash,
        args.lamports,
        args.cu_limit,
        args.cu_price.map_or_else(|| "none".to_string(), |v| v.to_string()),
        tx.signatures[0],
    );

    Ok(())
}

fn seeded_keypair(index: u64) -> Keypair {
    let mut seed = [0u8; 32];
    seed[..8].copy_from_slice(&index.to_le_bytes());
    Keypair::new_from_array(seed)
}
