use std::{path::PathBuf, str::FromStr};

use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use clap::Parser;
use solana_hash::Hash;
use solana_instruction::{AccountMeta, Instruction};
use solana_keypair::{read_keypair_file, Keypair};
use solana_message::Message;
use solana_pubkey::Pubkey;
use solana_signer::Signer;
use solana_transaction::Transaction;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Fee-payer keypair in Solana JSON format.
    #[arg(
        long,
        conflicts_with = "payer_seed_index",
        required_unless_present = "payer_seed_index"
    )]
    payer: Option<PathBuf>,

    /// Deterministic fee-payer secret seed index.
    #[arg(long, conflicts_with = "payer")]
    payer_seed_index: Option<u64>,

    /// Program to invoke.
    #[arg(long)]
    program_id: String,

    /// Recent blockhash used to sign the transaction.
    #[arg(long)]
    recent_blockhash: String,

    /// Instruction data encoded as hexadecimal bytes.
    #[arg(long, default_value = "")]
    data_hex: String,

    /// Number of deterministic readonly, non-signer instruction accounts.
    #[arg(long, default_value_t = 0)]
    account_count: usize,

    /// Serialize the message without signatures.
    #[arg(long, default_value_t = false)]
    unsigned: bool,
}

fn decode_hex(value: &str) -> Result<Vec<u8>> {
    anyhow::ensure!(
        value.len() % 2 == 0,
        "--data-hex must contain an even number of hexadecimal digits"
    );

    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let digits = std::str::from_utf8(pair).expect("hexadecimal input is UTF-8");
            u8::from_str_radix(digits, 16)
                .with_context(|| format!("invalid hexadecimal byte {digits:?}"))
        })
        .collect()
}

fn main() -> Result<()> {
    let args = Args::parse();
    let payer = if let Some(path) = args.payer.as_ref() {
        read_keypair_file(path)
            .map_err(|err| anyhow::anyhow!("failed to read {}: {err}", path.display()))?
    } else {
        seeded_keypair(
            args.payer_seed_index
                .expect("clap requires one payer source"),
        )
    };
    let program_id = Pubkey::from_str(&args.program_id).context("invalid --program-id")?;
    let recent_blockhash =
        Hash::from_str(&args.recent_blockhash).context("invalid --recent-blockhash")?;
    let data = decode_hex(&args.data_hex)?;

    let accounts = (0..args.account_count)
        .map(|idx| {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&(10_000_000u64 + idx as u64).to_le_bytes());
            AccountMeta::new_readonly(Pubkey::new_from_array(bytes), false)
        })
        .collect();
    let instruction = Instruction::new_with_bytes(program_id, &data, accounts);
    let transaction = if args.unsigned {
        let mut message = Message::new(&[instruction], Some(&payer.pubkey()));
        message.recent_blockhash = recent_blockhash;
        Transaction::new_unsigned(message)
    } else {
        Transaction::new_signed_with_payer(
            &[instruction],
            Some(&payer.pubkey()),
            &[&payer],
            recent_blockhash,
        )
    };
    let wire = bincode::serialize(&transaction).context("failed to serialize transaction")?;

    println!("payer={}", payer.pubkey());
    println!(
        "signature={}",
        transaction
            .signatures
            .first()
            .map(ToString::to_string)
            .unwrap_or_default()
    );
    println!("transaction_base64={}", STANDARD.encode(wire));
    Ok(())
}

fn seeded_keypair(index: u64) -> Keypair {
    let mut seed = [0u8; 32];
    seed[..8].copy_from_slice(&index.to_le_bytes());
    Keypair::new_from_array(seed)
}
