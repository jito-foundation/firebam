use std::{fs, path::PathBuf};

use anyhow::{Context, Result};
use base64::prelude::*;
use clap::Parser;
use solana_slot_hashes::SlotHashes;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Input file containing a SlotHashes sysvar account payload.
    #[arg(long)]
    input: PathBuf,

    /// Treat the input file as base64-encoded instead of raw account bytes.
    #[arg(long, default_value_t = false)]
    base64: bool,

    /// Slot-hash entry index to print (0 is the newest entry).
    #[arg(long, default_value_t = 0)]
    index: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let input = fs::read(&args.input)
        .with_context(|| format!("failed to read {}", args.input.display()))?;
    let raw = if args.base64 {
        let text = std::str::from_utf8(&input)
            .with_context(|| format!("{} was not valid UTF-8 base64 text", args.input.display()))?;
        BASE64_STANDARD
            .decode(text.trim())
            .context("failed to decode base64 slot-hashes payload")?
    } else {
        input
    };

    let slot_hashes: SlotHashes =
        bincode::deserialize(&raw).context("failed to deserialize SlotHashes account")?;
    let (slot, hash) = slot_hashes
        .slot_hashes()
        .get(args.index)
        .copied()
        .with_context(|| {
            format!(
                "slot-hashes account had {} entries, missing index {}",
                slot_hashes.slot_hashes().len(),
                args.index
            )
        })?;

    println!(
        "slot={} hash={} entries={} index={}",
        slot,
        hash,
        slot_hashes.slot_hashes().len(),
        args.index
    );

    Ok(())
}
