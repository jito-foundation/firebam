use std::{fs, path::PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use solana_keypair::Keypair;
use solana_signer::Signer;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Deterministic secret seed index
    #[arg(long)]
    seed_index: u64,

    /// Output keypair JSON path
    #[arg(long)]
    output: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let keypair = seeded_keypair(args.seed_index);
    let bytes = keypair.to_bytes();
    let json = format!(
        "[{}]\n",
        bytes
            .iter()
            .map(u8::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    fs::write(&args.output, json)
        .with_context(|| format!("failed to write {}", args.output.display()))?;
    println!(
        "wrote seeded keypair seed_index={} pubkey={} path={}",
        args.seed_index,
        keypair.pubkey(),
        args.output.display()
    );
    Ok(())
}

fn seeded_keypair(index: u64) -> Keypair {
    let mut seed = [0u8; 32];
    seed[..8].copy_from_slice(&index.to_le_bytes());
    Keypair::new_from_array(seed)
}
