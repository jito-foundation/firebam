use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{bail, Context, Result};
use base64::prelude::*;
use clap::{Parser, ValueEnum};
use prost::Message;
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_hash::Hash;
use solana_instruction::Instruction;
use solana_keypair::{read_keypair_file, Keypair};
use solana_message::{
    compiled_instruction::CompiledInstruction, legacy, v0, MessageHeader, VersionedMessage,
};
use solana_pubkey::Pubkey;
use solana_signature::Signature;
use solana_signer::Signer;
use solana_system_interface::instruction::{
    allocate, assign, create_account, create_account_with_seed, transfer, transfer_with_seed,
    SystemInstruction,
};
use solana_transaction::{versioned::VersionedTransaction, Transaction};
use solana_vote_interface::{instruction::VoteInstruction, program as vote_program, state::Vote};

pub mod org {
    pub mod solana {
        pub mod sealevel {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/org.solana.sealevel.v1.rs"));
            }
        }
    }
}

mod seeded;
use seeded::seeded_keypair;

use crate::org::solana::sealevel::v1::{
    InstrContext, InstrFixture, MessageAddressTableLookup, SanitizedTransaction, TxnContext,
    TxnFixture,
};

#[derive(Debug)]
enum InputContext {
    Txn(TxnContext),
    Instr(InstrContext),
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, ValueEnum)]
enum AdaptMode {
    #[default]
    Raw,
    LocalSystemTransfer,
    LocalSimpleVote,
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Input file (.txnctx, .instrctx, or .fix)
    #[arg(long)]
    input: PathBuf,

    /// Output packet file, one base64-encoded transaction per line
    #[arg(long)]
    output: PathBuf,

    /// Optional helper scenario file that references --output via packets_base64_file.
    #[arg(long)]
    scenario_output: Option<PathBuf>,

    /// Transformation mode to apply before writing the packet.
    #[arg(long, value_enum, default_value_t = AdaptMode::Raw)]
    adapt_mode: AdaptMode,

    /// Recent blockhash to use when --adapt-mode=local-system-transfer.
    #[arg(long)]
    recent_blockhash: Option<String>,

    /// Genesis-funded source account index for local-system-transfer adaptation.
    #[arg(long, default_value_t = 0)]
    from_genesis_account: u64,

    /// Explicit source keypair for local-system-transfer adaptation.
    #[arg(long)]
    from_keypair: Option<PathBuf>,

    /// Genesis-funded destination account index for local-system-transfer adaptation.
    #[arg(long, default_value_t = 1)]
    to_genesis_account: u64,

    /// Node identity keypair path for --adapt-mode=local-simple-vote.
    #[arg(long)]
    node_keypair: Option<PathBuf>,

    /// Vote account keypair path for --adapt-mode=local-simple-vote.
    #[arg(long)]
    vote_keypair: Option<PathBuf>,

    /// Authorized voter keypair path for --adapt-mode=local-simple-vote.
    #[arg(long)]
    authorized_voter_keypair: Option<PathBuf>,

    /// Vote slot to use for --adapt-mode=local-simple-vote.
    #[arg(long)]
    vote_slot: Option<u64>,

    /// Vote hash to use for --adapt-mode=local-simple-vote.
    #[arg(long)]
    vote_hash: Option<String>,

    /// Seq id to use when writing --scenario-output.
    #[arg(long, default_value_t = 1)]
    seq_id: u32,

    /// Sleep before sending the batch in the generated scenario file.
    #[arg(long, default_value_t = 1000)]
    pre_sleep_ms: u64,

    /// Sleep after sending the batch in the generated scenario file.
    #[arg(long, default_value_t = 3000)]
    post_sleep_ms: u64,

    /// max_schedule_slot keyword/number to write in the generated scenario file.
    #[arg(long, default_value = "max")]
    max_schedule_slot: String,

    /// simple_vote_tx flag to write in the generated scenario file.
    #[arg(long, default_value_t = false)]
    simple_vote_tx: bool,

    /// revert_on_error flag to write in the generated scenario file.
    #[arg(long, default_value_t = true)]
    revert_on_error: bool,
}

/// Adapted packet plus its `adapted family=...` summary line.
type Adapted = (Vec<u8>, Option<String>);

#[derive(Debug)]
enum LocalSystemPlan {
    FeeOnly,
    Transfer {
        lamports: Vec<u64>,
    },
    Assign {
        owner: Pubkey,
    },
    Allocate {
        space: u64,
    },
    CreateAccount {
        lamports: u64,
        space: u64,
        owner: Pubkey,
    },
    TransferWithSeed {
        lamports: u64,
    },
    CreateAccountWithSeed {
        lamports: u64,
        space: u64,
        owner: Pubkey,
    },
}

impl LocalSystemPlan {
    /// Summary kind and SystemInstruction variant name.
    fn names(&self) -> (&'static str, &'static str) {
        match self {
            Self::FeeOnly => ("fee_only", "FeeOnly"),
            Self::Transfer { .. } => ("transfer", "Transfer"),
            Self::Assign { .. } => ("assign", "Assign"),
            Self::Allocate { .. } => ("allocate", "Allocate"),
            Self::CreateAccount { .. } => ("create_account", "CreateAccount"),
            Self::TransferWithSeed { .. } => ("transfer_with_seed", "TransferWithSeed"),
            Self::CreateAccountWithSeed { .. } => {
                ("create_account_with_seed", "CreateAccountWithSeed")
            }
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let raw = fs::read(&args.input)
        .with_context(|| format!("failed to read {}", args.input.display()))?;
    let input_ctx = decode_input_context(&args.input, &raw)?;
    let (packet, summary) = input_context_to_packet(&input_ctx, &args)?;
    let encoded = BASE64_STANDARD.encode(packet);

    fs::write(&args.output, format!("{encoded}\n"))
        .with_context(|| format!("failed to write {}", args.output.display()))?;
    println!(
        "wrote 1 packet to {} from {}",
        args.output.display(),
        args.input.display()
    );

    if let Some(summary) = summary {
        println!("{summary}");
    }

    if let Some(scenario_output) = args.scenario_output.as_ref() {
        let scenario = render_scenario(
            &args.output,
            args.seq_id,
            args.pre_sleep_ms,
            args.post_sleep_ms,
            &args.max_schedule_slot,
            args.simple_vote_tx,
            args.revert_on_error,
            &scenario_description(args.adapt_mode, &input_ctx),
        );
        fs::write(scenario_output, scenario)
            .with_context(|| format!("failed to write {}", scenario_output.display()))?;
        println!("wrote scenario to {}", scenario_output.display());
    }

    Ok(())
}

fn decode_input_context(input_path: &Path, raw: &[u8]) -> Result<InputContext> {
    match input_path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or_default()
    {
        "txnctx" => TxnContext::decode(raw)
            .map(InputContext::Txn)
            .with_context(|| format!("failed to decode TxnContext from {}", input_path.display())),
        "instrctx" => InstrContext::decode(raw)
            .map(InputContext::Instr)
            .with_context(|| {
                format!(
                    "failed to decode InstrContext from {}",
                    input_path.display()
                )
            }),
        "fix" => {
            if let Ok(fixture) = TxnFixture::decode(raw) {
                return fixture
                    .input
                    .map(InputContext::Txn)
                    .context("TxnFixture missing input TxnContext");
            }

            let fixture = InstrFixture::decode(raw).with_context(|| {
                format!(
                    "failed to decode {} as TxnFixture or InstrFixture",
                    input_path.display()
                )
            })?;
            fixture
                .input
                .map(InputContext::Instr)
                .context("InstrFixture missing input InstrContext")
        }
        other => bail!(
            "unsupported input extension .{} for {}; expected .txnctx, .instrctx, or .fix",
            other,
            input_path.display()
        ),
    }
}

fn txn_context_to_versioned(ctx: &TxnContext) -> Result<VersionedTransaction> {
    sanitized_tx_to_versioned(ctx.tx.as_ref().context("TxnContext missing tx")?)
}

fn input_context_to_packet(input_ctx: &InputContext, args: &Args) -> Result<Adapted> {
    match args.adapt_mode {
        AdaptMode::Raw => match input_ctx {
            InputContext::Txn(ctx) => Ok((
                bincode::serialize(&txn_context_to_versioned(ctx)?)
                    .context("failed to serialize VersionedTransaction")?,
                None,
            )),
            InputContext::Instr(_) => bail!(
                "--adapt-mode=raw is not supported for InstrContext input; use a semantic adapter"
            ),
        },
        AdaptMode::LocalSystemTransfer => {
            let recent_blockhash = Hash::from_str(args.recent_blockhash.as_deref().context(
                "--recent-blockhash is required for --adapt-mode=local-system-transfer",
            )?)
            .context("failed to parse --recent-blockhash")?;

            let from_keypair = args
                .from_keypair
                .as_ref()
                .map(|path| {
                    read_keypair_file(path).map_err(|err| {
                        anyhow::anyhow!("failed to read keypair from {}: {}", path.display(), err)
                    })
                })
                .transpose()?;

            if from_keypair.is_none() && args.from_genesis_account == args.to_genesis_account {
                bail!(
                    "--from-genesis-account and --to-genesis-account must differ for local-system-transfer adaptation"
                );
            }
            let from_keypair = from_keypair.as_ref();

            match input_ctx {
                InputContext::Txn(ctx) => adapt_local_system_program_txn(
                    &txn_context_to_versioned(ctx)?,
                    recent_blockhash,
                    args.from_genesis_account,
                    from_keypair,
                    args.to_genesis_account,
                ),
                InputContext::Instr(instr) => adapt_local_system_program_instr(
                    instr,
                    recent_blockhash,
                    args.from_genesis_account,
                    from_keypair,
                    args.to_genesis_account,
                ),
            }
        }
        AdaptMode::LocalSimpleVote => {
            let recent_blockhash =
                Hash::from_str(args.recent_blockhash.as_deref().context(
                    "--recent-blockhash is required for --adapt-mode=local-simple-vote",
                )?)
                .context("failed to parse --recent-blockhash")?;
            let vote_hash = Hash::from_str(
                args.vote_hash
                    .as_deref()
                    .context("--vote-hash is required for --adapt-mode=local-simple-vote")?,
            )
            .context("failed to parse --vote-hash")?;
            let vote_slot = args
                .vote_slot
                .context("--vote-slot is required for --adapt-mode=local-simple-vote")?;
            let node_keypair = load_keypair_arg(&args.node_keypair, "--node-keypair")?;
            let vote_keypair = load_keypair_arg(&args.vote_keypair, "--vote-keypair")?;
            let authorized_voter_keypair =
                load_keypair_arg(&args.authorized_voter_keypair, "--authorized-voter-keypair")?;

            match input_ctx {
                InputContext::Txn(ctx) => adapt_local_simple_vote_txn(
                    &txn_context_to_versioned(ctx)?,
                    recent_blockhash,
                    vote_slot,
                    vote_hash,
                    &node_keypair,
                    &vote_keypair,
                    &authorized_voter_keypair,
                ),
                InputContext::Instr(instr) => adapt_local_simple_vote_instr(
                    instr,
                    recent_blockhash,
                    vote_slot,
                    vote_hash,
                    &node_keypair,
                    &vote_keypair,
                    &authorized_voter_keypair,
                ),
            }
        }
    }
}

fn sanitized_tx_to_versioned(tx: &SanitizedTransaction) -> Result<VersionedTransaction> {
    let msg = tx
        .message
        .as_ref()
        .context("SanitizedTransaction missing message")?;
    let hdr = msg
        .header
        .as_ref()
        .context("TransactionMessage missing header")?;

    let header = MessageHeader {
        num_required_signatures: u8::try_from(hdr.num_required_signatures)
            .context("num_required_signatures exceeds u8")?,
        num_readonly_signed_accounts: u8::try_from(hdr.num_readonly_signed_accounts)
            .context("num_readonly_signed_accounts exceeds u8")?,
        num_readonly_unsigned_accounts: u8::try_from(hdr.num_readonly_unsigned_accounts)
            .context("num_readonly_unsigned_accounts exceeds u8")?,
    };

    let account_keys = msg
        .account_keys
        .iter()
        .map(|key| decode_pubkey(key))
        .collect::<Result<Vec<_>>>()?;
    let recent_blockhash = decode_hash(&msg.recent_blockhash)?;
    let instructions = msg
        .instructions
        .iter()
        .map(|instr| {
            Ok(CompiledInstruction {
                program_id_index: u8::try_from(instr.program_id_index)
                    .context("program_id_index exceeds u8")?,
                accounts: instr
                    .accounts
                    .iter()
                    .map(|index| u8::try_from(*index).context("account index exceeds u8"))
                    .collect::<Result<Vec<_>>>()?,
                data: instr.data.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let message = if msg.is_legacy {
        VersionedMessage::Legacy(legacy::Message {
            header,
            account_keys,
            recent_blockhash,
            instructions,
        })
    } else {
        let address_table_lookups = msg
            .address_table_lookups
            .iter()
            .map(decode_address_table_lookup)
            .collect::<Result<Vec<_>>>()?;
        VersionedMessage::V0(v0::Message {
            header,
            account_keys,
            recent_blockhash,
            instructions,
            address_table_lookups,
        })
    };

    let signatures = tx
        .signatures
        .iter()
        .map(|signature| decode_signature(signature))
        .collect::<Result<Vec<_>>>()?;

    Ok(VersionedTransaction {
        signatures,
        message,
    })
}

fn decode_pubkey(raw: &[u8]) -> Result<Pubkey> {
    let arr = <[u8; 32]>::try_from(raw)
        .with_context(|| format!("expected 32-byte pubkey, got {}", raw.len()))?;
    Ok(Pubkey::from(arr))
}

fn decode_hash(raw: &[u8]) -> Result<Hash> {
    let arr = <[u8; 32]>::try_from(raw)
        .with_context(|| format!("expected 32-byte hash, got {}", raw.len()))?;
    Ok(Hash::new_from_array(arr))
}

fn decode_signature(raw: &[u8]) -> Result<Signature> {
    let arr = <[u8; 64]>::try_from(raw)
        .with_context(|| format!("expected 64-byte signature, got {}", raw.len()))?;
    Ok(Signature::from(arr))
}

fn decode_address_table_lookup(
    lookup: &MessageAddressTableLookup,
) -> Result<v0::MessageAddressTableLookup> {
    Ok(v0::MessageAddressTableLookup {
        account_key: decode_pubkey(&lookup.account_key)?,
        writable_indexes: lookup
            .writable_indexes
            .iter()
            .map(|index| u8::try_from(*index).context("writable ALUT index exceeds u8"))
            .collect::<Result<Vec<_>>>()?,
        readonly_indexes: lookup
            .readonly_indexes
            .iter()
            .map(|index| u8::try_from(*index).context("readonly ALUT index exceeds u8"))
            .collect::<Result<Vec<_>>>()?,
    })
}

fn local_system_plan(system_ix: SystemInstruction, input: &str) -> Result<LocalSystemPlan> {
    Ok(match system_ix {
        SystemInstruction::Transfer { lamports } => LocalSystemPlan::Transfer {
            lamports: vec![lamports],
        },
        SystemInstruction::Assign { owner } => LocalSystemPlan::Assign { owner },
        SystemInstruction::Allocate { space } => LocalSystemPlan::Allocate { space },
        SystemInstruction::CreateAccount {
            lamports,
            space,
            owner,
        } => LocalSystemPlan::CreateAccount {
            lamports,
            space,
            owner,
        },
        SystemInstruction::TransferWithSeed { lamports, .. } => {
            LocalSystemPlan::TransferWithSeed { lamports }
        }
        SystemInstruction::CreateAccountWithSeed {
            lamports,
            space,
            owner,
            ..
        } => LocalSystemPlan::CreateAccountWithSeed {
            lamports,
            space,
            owner,
        },
        other => bail!(
            "local-system-transfer adaptation{input} only supports SystemInstruction::Transfer, SystemInstruction::Assign, SystemInstruction::Allocate, SystemInstruction::CreateAccount, SystemInstruction::TransferWithSeed, or SystemInstruction::CreateAccountWithSeed, got {other:?}"
        ),
    })
}

fn adapt_local_system_program_txn(
    tx: &VersionedTransaction,
    recent_blockhash: Hash,
    from_genesis_account: u64,
    from_keypair: Option<&Keypair>,
    to_genesis_account: u64,
) -> Result<Adapted> {
    let (account_keys, compiled_instructions) = message_parts(&tx.message);
    let mut instructions = Vec::<Instruction>::new();
    let mut plan = None;
    let mut compute_budget_ix_count = 0usize;

    for compiled in compiled_instructions {
        let program_id = account_keys
            .get(usize::from(compiled.program_id_index))
            .context("compiled instruction program_id_index out of bounds")?;

        if *program_id == solana_compute_budget_interface::ID {
            instructions.push(rebuild_compute_budget_instruction(&compiled.data)?);
            compute_budget_ix_count += 1;
            continue;
        }

        if *program_id == solana_system_interface::program::ID {
            let system_ix: SystemInstruction = bincode::deserialize(&compiled.data)
                .context("failed to decode system instruction payload")?;
            match (&mut plan, local_system_plan(system_ix, "")?) {
                (None, next) => plan = Some(next),
                (
                    Some(LocalSystemPlan::Transfer { lamports: existing }),
                    LocalSystemPlan::Transfer { lamports },
                ) => existing.extend(lamports),
                (Some(_), LocalSystemPlan::Transfer { .. }) => bail!(
                    "local-system-transfer adaptation does not support mixing System-program families inside one adapted transaction"
                ),
                (Some(_), next) => {
                    let (kind, variant) = next.names();
                    bail!(
                        "local-system-transfer adaptation only supports exactly one SystemInstruction::{variant} when using the {} family",
                        kind.replace('_', "-")
                    );
                }
            }
            continue;
        }

        bail!(
            "local-system-transfer adaptation only supports compute-budget and supported System-program instructions; saw program {}",
            program_id
        );
    }

    let plan = match plan {
        Some(plan) => plan,
        None if compute_budget_ix_count > 0 => LocalSystemPlan::FeeOnly,
        None => bail!(
            "local-system-transfer adaptation did not find a supported System-program or compute-budget instruction"
        ),
    };
    build_local_system_plan(
        plan,
        instructions,
        recent_blockhash,
        from_genesis_account,
        from_keypair,
        to_genesis_account,
        compute_budget_ix_count,
    )
}

fn adapt_local_system_program_instr(
    instr: &InstrContext,
    recent_blockhash: Hash,
    from_genesis_account: u64,
    from_keypair: Option<&Keypair>,
    to_genesis_account: u64,
) -> Result<Adapted> {
    let program_id = decode_pubkey(&instr.program_id)
        .context("InstrContext program_id was not a 32-byte pubkey")?;
    if program_id != solana_system_interface::program::ID {
        bail!(
            "local-system-transfer adaptation for InstrContext only supports the System program, got {}",
            program_id
        );
    }

    let system_ix: SystemInstruction = bincode::deserialize(&instr.data)
        .context("failed to decode InstrContext system instruction payload")?;
    build_local_system_plan(
        local_system_plan(system_ix, " for InstrContext")?,
        Vec::new(),
        recent_blockhash,
        from_genesis_account,
        from_keypair,
        to_genesis_account,
        0,
    )
}

fn adapt_local_simple_vote_txn(
    tx: &VersionedTransaction,
    recent_blockhash: Hash,
    vote_slot: u64,
    vote_hash: Hash,
    node_keypair: &Keypair,
    vote_keypair: &Keypair,
    authorized_voter_keypair: &Keypair,
) -> Result<Adapted> {
    let (account_keys, compiled_instructions) = message_parts(&tx.message);
    let mut vote_ix_count = 0usize;

    for compiled in compiled_instructions {
        let program_id = account_keys
            .get(usize::from(compiled.program_id_index))
            .context("compiled instruction program_id_index out of bounds")?;

        if *program_id == solana_compute_budget_interface::ID {
            continue;
        }

        if *program_id != vote_program::ID {
            bail!(
                "local-simple-vote adaptation only supports compute-budget and vote-program instructions; saw program {}",
                program_id
            );
        }

        let _: VoteInstruction = bincode::deserialize(&compiled.data)
            .context("failed to decode vote instruction payload")?;
        vote_ix_count += 1;
    }

    if vote_ix_count != 1 {
        bail!(
            "local-simple-vote adaptation requires exactly one vote-program instruction, got {}",
            vote_ix_count
        );
    }

    build_local_simple_vote(
        recent_blockhash,
        vote_slot,
        vote_hash,
        node_keypair,
        vote_keypair,
        authorized_voter_keypair,
    )
}

fn adapt_local_simple_vote_instr(
    instr: &InstrContext,
    recent_blockhash: Hash,
    vote_slot: u64,
    vote_hash: Hash,
    node_keypair: &Keypair,
    vote_keypair: &Keypair,
    authorized_voter_keypair: &Keypair,
) -> Result<Adapted> {
    let program_id = decode_pubkey(&instr.program_id)
        .context("InstrContext program_id was not a 32-byte pubkey")?;
    if program_id != vote_program::ID {
        bail!(
            "local-simple-vote adaptation for InstrContext only supports the Vote program, got {}",
            program_id
        );
    }

    let _: VoteInstruction = bincode::deserialize(&instr.data)
        .context("failed to decode InstrContext vote instruction payload")?;

    build_local_simple_vote(
        recent_blockhash,
        vote_slot,
        vote_hash,
        node_keypair,
        vote_keypair,
        authorized_voter_keypair,
    )
}

fn build_local_simple_vote(
    recent_blockhash: Hash,
    vote_slot: u64,
    vote_hash: Hash,
    node_keypair: &Keypair,
    vote_keypair: &Keypair,
    authorized_voter_keypair: &Keypair,
) -> Result<Adapted> {
    let vote_ix = solana_vote_interface::instruction::vote(
        &vote_keypair.pubkey(),
        &authorized_voter_keypair.pubkey(),
        Vote::new(vec![vote_slot], vote_hash),
    );

    let mut tx = Transaction::new_with_payer(&[vote_ix], Some(&node_keypair.pubkey()));

    if node_keypair.pubkey() == authorized_voter_keypair.pubkey() {
        tx.sign(&[node_keypair], recent_blockhash);
    } else {
        tx.partial_sign(&[node_keypair], recent_blockhash);
        tx.partial_sign(&[authorized_voter_keypair], recent_blockhash);
    }

    let packet = bincode::serialize(&tx).context("failed to serialize adapted vote Transaction")?;
    let summary = format!(
        "adapted family=local-simple-vote node={} vote={} authorized_voter={} vote_slot={} vote_hash={} recent_blockhash={}",
        node_keypair.pubkey(),
        vote_keypair.pubkey(),
        authorized_voter_keypair.pubkey(),
        vote_slot,
        vote_hash,
        recent_blockhash
    );

    Ok((packet, Some(summary)))
}

fn message_parts(message: &VersionedMessage) -> (&[Pubkey], &[CompiledInstruction]) {
    match message {
        VersionedMessage::Legacy(msg) => (&msg.account_keys, &msg.instructions),
        VersionedMessage::V0(msg) => (&msg.account_keys, &msg.instructions),
    }
}

fn rebuild_compute_budget_instruction(data: &[u8]) -> Result<Instruction> {
    let (tag, payload) = data
        .split_first()
        .context("compute-budget instruction missing discriminator byte")?;
    match *tag {
        1 => Ok(ComputeBudgetInstruction::request_heap_frame(read_u32(
            payload,
        )?)),
        2 => Ok(ComputeBudgetInstruction::set_compute_unit_limit(read_u32(
            payload,
        )?)),
        3 => Ok(ComputeBudgetInstruction::set_compute_unit_price(read_u64(
            payload,
        )?)),
        4 => Ok(ComputeBudgetInstruction::set_loaded_accounts_data_size_limit(read_u32(payload)?)),
        0 => bail!(
            "local-system-transfer adaptation does not support deprecated compute-budget variant 0"
        ),
        other => bail!("unknown compute-budget discriminator {other}"),
    }
}

fn read_u32(raw: &[u8]) -> Result<u32> {
    let arr = <[u8; 4]>::try_from(raw)
        .with_context(|| format!("expected 4-byte payload, got {}", raw.len()))?;
    Ok(u32::from_le_bytes(arr))
}

fn read_u64(raw: &[u8]) -> Result<u64> {
    let arr = <[u8; 8]>::try_from(raw)
        .with_context(|| format!("expected 8-byte payload, got {}", raw.len()))?;
    Ok(u64::from_le_bytes(arr))
}

fn derived_new_account_keypair(index: u64) -> Keypair {
    let mut seed = [0u8; 32];
    seed[..8].copy_from_slice(&index.to_le_bytes());
    seed[8..16].copy_from_slice(b"bam.new!");
    Keypair::new_from_array(seed)
}

fn local_with_seed_string(index: u64) -> String {
    format!("bamseed{index:016x}")
}

fn derived_seeded_pubkey(base: &Pubkey, seed: &str, owner: &Pubkey) -> Result<Pubkey> {
    Pubkey::create_with_seed(base, seed, owner).map_err(|err| {
        anyhow::anyhow!(
            "failed to derive seeded address from base {} seed {:?} owner {}: {}",
            base,
            seed,
            owner,
            err
        )
    })
}

fn build_local_system_plan(
    plan: LocalSystemPlan,
    mut instructions: Vec<Instruction>,
    recent_blockhash: Hash,
    from_genesis_account: u64,
    from_keypair: Option<&Keypair>,
    to_genesis_account: u64,
    compute_budget_ix_count: usize,
) -> Result<Adapted> {
    let generated_from;
    let from = if let Some(from_keypair) = from_keypair {
        from_keypair
    } else {
        generated_from = seeded_keypair(from_genesis_account);
        &generated_from
    };
    let transfer_target = seeded_keypair(to_genesis_account);
    if from.pubkey() == transfer_target.pubkey() {
        bail!("local-system-transfer source and destination pubkeys must differ");
    }

    let (system_kind, _) = plan.names();
    let family = match plan {
        LocalSystemPlan::FeeOnly => "local-fee-only",
        _ => "local-system-program",
    };
    let new_account;
    let mut extra_signer = None;
    let (to_pubkey, lamports, space, owner) = match plan {
        LocalSystemPlan::FeeOnly => (transfer_target.pubkey(), 0, 0, None),
        LocalSystemPlan::Transfer { lamports } => {
            let mut total_lamports = 0u64;
            for lamports in lamports {
                total_lamports = total_lamports
                    .checked_add(lamports)
                    .context("local-system-transfer adaptation overflowed total lamports")?;
                instructions.push(transfer(
                    &from.pubkey(),
                    &transfer_target.pubkey(),
                    lamports,
                ));
            }
            (transfer_target.pubkey(), total_lamports, 0, None)
        }
        LocalSystemPlan::Assign { owner } => {
            instructions.push(assign(&transfer_target.pubkey(), &owner));
            extra_signer = Some(&transfer_target);
            (transfer_target.pubkey(), 0, 0, Some(owner))
        }
        LocalSystemPlan::Allocate { space } => {
            instructions.push(allocate(&transfer_target.pubkey(), space));
            extra_signer = Some(&transfer_target);
            (
                transfer_target.pubkey(),
                0,
                space,
                Some(solana_system_interface::program::ID),
            )
        }
        LocalSystemPlan::CreateAccount {
            lamports,
            space,
            owner,
        } => {
            new_account = derived_new_account_keypair(to_genesis_account);
            instructions.push(create_account(
                &from.pubkey(),
                &new_account.pubkey(),
                lamports,
                space,
                &owner,
            ));
            extra_signer = Some(&new_account);
            (new_account.pubkey(), lamports, space, Some(owner))
        }
        LocalSystemPlan::TransferWithSeed { lamports } => {
            let local_seed = local_with_seed_string(to_genesis_account);
            let local_from_owner = solana_system_interface::program::ID;
            let seeded_from =
                derived_seeded_pubkey(&from.pubkey(), &local_seed, &local_from_owner)?;
            instructions.push(create_account_with_seed(
                &from.pubkey(),
                &seeded_from,
                &from.pubkey(),
                &local_seed,
                lamports.saturating_add(1_000_000_000),
                0,
                &local_from_owner,
            ));
            instructions.push(transfer_with_seed(
                &seeded_from,
                &from.pubkey(),
                local_seed.clone(),
                &local_from_owner,
                &transfer_target.pubkey(),
                lamports,
            ));
            (
                transfer_target.pubkey(),
                lamports,
                0,
                Some(local_from_owner),
            )
        }
        LocalSystemPlan::CreateAccountWithSeed {
            lamports,
            space,
            owner,
        } => {
            let local_seed = local_with_seed_string(to_genesis_account);
            let seeded_target = derived_seeded_pubkey(&from.pubkey(), &local_seed, &owner)?;
            instructions.push(create_account_with_seed(
                &from.pubkey(),
                &seeded_target,
                &from.pubkey(),
                &local_seed,
                lamports,
                space,
                &owner,
            ));
            (seeded_target, lamports, space, Some(owner))
        }
    };

    let system_ix_count = instructions.len() - compute_budget_ix_count;
    let mut tx = Transaction::new_with_payer(&instructions, Some(&from.pubkey()));
    match extra_signer {
        Some(signer) => tx.sign(&[from, signer], recent_blockhash),
        None => tx.sign(&[from], recent_blockhash),
    }
    let packet = bincode::serialize(&tx).context("failed to serialize adapted Transaction")?;
    let summary = format!(
        "adapted family={} system_kind={} from={} to={} lamports={} space={} owner={} system_ix={} compute_budget_ix={} recent_blockhash={}",
        family,
        system_kind,
        from.pubkey(),
        to_pubkey,
        lamports,
        space,
        owner
            .map(|owner| owner.to_string())
            .unwrap_or_else(|| "none".to_string()),
        system_ix_count,
        compute_budget_ix_count,
        recent_blockhash
    );

    Ok((packet, Some(summary)))
}

fn load_keypair_arg(path: &Option<PathBuf>, flag_name: &str) -> Result<Keypair> {
    let path = path
        .as_ref()
        .with_context(|| format!("{flag_name} is required"))?;
    read_keypair_file(path)
        .map_err(|err| anyhow::anyhow!("failed to read keypair from {}: {}", path.display(), err))
}

fn scenario_description(adapt_mode: AdaptMode, input_ctx: &InputContext) -> String {
    let input = match input_ctx {
        InputContext::Txn(_) => "transaction-context",
        InputContext::Instr(_) => "instruction-context",
    };
    let adapted = match adapt_mode {
        AdaptMode::Raw => "",
        AdaptMode::LocalSystemTransfer => {
            " and adapted into a local signed System-program transaction"
        }
        AdaptMode::LocalSimpleVote => " and adapted into a local signed simple vote",
    };
    format!("Generated from {input} input{adapted}.")
}

fn render_scenario(
    packet_output: &Path,
    seq_id: u32,
    pre_sleep_ms: u64,
    post_sleep_ms: u64,
    max_schedule_slot: &str,
    simple_vote_tx: bool,
    revert_on_error: bool,
    description: &str,
) -> String {
    format!(
        concat!(
            "description = \"{description}\"\n",
            "heartbeat_interval_ms = 1000\n\n",
            "[[events]]\n",
            "type = \"sleep\"\n",
            "ms = {pre_sleep_ms}\n\n",
            "[[events]]\n",
            "type = \"send_batch\"\n",
            "seq_id = {seq_id}\n",
            "packets_base64_file = \"{packet_file}\"\n",
            "max_schedule_slot = \"{max_schedule_slot}\"\n",
            "simple_vote_tx = {simple_vote_tx}\n",
            "revert_on_error = {revert_on_error}\n\n",
            "[[events]]\n",
            "type = \"sleep\"\n",
            "ms = {post_sleep_ms}\n"
        ),
        description = description,
        pre_sleep_ms = pre_sleep_ms,
        seq_id = seq_id,
        packet_file = packet_output.display(),
        max_schedule_slot = max_schedule_slot,
        simple_vote_tx = simple_vote_tx,
        revert_on_error = revert_on_error,
        post_sleep_ms = post_sleep_ms,
    )
}
