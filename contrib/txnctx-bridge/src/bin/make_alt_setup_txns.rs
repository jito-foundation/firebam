use std::{path::PathBuf, str::FromStr};

use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use clap::Parser;
use solana_address_lookup_table_interface::instruction::{
    create_lookup_table, extend_lookup_table,
};
use solana_hash::Hash;
use solana_keypair::read_keypair_file;
use solana_pubkey::Pubkey;
use solana_signer::Signer;
use solana_transaction::Transaction;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Payer and lookup-table authority keypair path.
    #[arg(long)]
    payer_keypair: PathBuf,

    /// Recent finalized slot used to derive the lookup-table address.
    #[arg(long)]
    recent_slot: u64,

    /// Recent blockhash used to sign both setup transactions.
    #[arg(long)]
    recent_blockhash: String,

    /// Address to add to the lookup table.
    #[arg(long)]
    lookup_address: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let payer = read_keypair_file(&args.payer_keypair).map_err(|err| {
        anyhow::anyhow!(
            "failed to read payer keypair from {}: {}",
            args.payer_keypair.display(),
            err
        )
    })?;
    let recent_blockhash =
        Hash::from_str(&args.recent_blockhash).context("failed to parse --recent-blockhash")?;
    let lookup_address =
        Pubkey::from_str(&args.lookup_address).context("failed to parse --lookup-address")?;

    let (create_ix, lookup_table) =
        create_lookup_table(payer.pubkey(), payer.pubkey(), args.recent_slot);
    let extend_ix = extend_lookup_table(
        lookup_table,
        payer.pubkey(),
        Some(payer.pubkey()),
        vec![lookup_address],
    );

    let mut create_tx = Transaction::new_with_payer(&[create_ix], Some(&payer.pubkey()));
    create_tx.sign(&[&payer], recent_blockhash);
    let mut extend_tx = Transaction::new_with_payer(&[extend_ix], Some(&payer.pubkey()));
    extend_tx.sign(&[&payer], recent_blockhash);

    let create_bytes =
        bincode::serialize(&create_tx).context("failed to serialize ALT create transaction")?;
    let extend_bytes =
        bincode::serialize(&extend_tx).context("failed to serialize ALT extend transaction")?;

    println!("lookup_table={}", lookup_table);
    println!("create_tx_base64={}", BASE64_STANDARD.encode(create_bytes));
    println!("extend_tx_base64={}", BASE64_STANDARD.encode(extend_bytes));

    Ok(())
}
