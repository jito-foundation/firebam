use solana_keypair::Keypair;

/// Deterministic keypair shared by the harness tools: the secret seed is
/// `index` little-endian followed by zeroes.
pub fn seeded_keypair(index: u64) -> Keypair {
    let mut seed = [0u8; 32];
    seed[..8].copy_from_slice(&index.to_le_bytes());
    Keypair::new_from_array(seed)
}
