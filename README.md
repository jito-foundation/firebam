# FireBAM 🔥💃

FireBAM is a fork of [Firedancer](https://jumpcrypto.com/firedancer/) that adds
validator support for [BAM (Blockspace Assembly Marketplace)](https://bam.dev/),
a next-generation transaction scheduling system for Solana.

* **Fast** Designed from the ground up to be *fast*. The concurrency
model draws from experience in the low latency trading space, and the code
contains many novel high-performance reimplementations of core Solana
primitives.
* **Secure** The architecture of the validator allows it to run with a
highly restrictive sandbox and almost no system calls.
* **Independent** Firedancer is written from scratch. This brings client
diversity to the Solana network and helps it stay resilient to supply
chain attacks in build tooling or dependencies.

## Documentation
If you are an operator or looking to run the FireBAM validator, see the
[FireBAM Setup Guide](https://jito-foundation.gitbook.io/mev/jito-solana/firebam-setup-guide)
and the [FireBAM validator documentation](https://bam.dev/validators/#firebam).

## Releases
If you are an operator looking to run the validator, see the [Releases
Guide](https://docs.firedancer.io/guide/getting-started.html#releases)
in the documentation.

The Firedancer project is producing two validators,

* **Frankendancer** A hybrid validator using parts of Firedancer and
parts of Agave. Frankendancer uses the Firedancer networking stack and
block production components to perform better while leader. Other
functionality including execution and consensus is using the Agave
validator code.
* **Firedancer** A full from-scratch Firedancer with no Agave code.

Both validators are built from this codebase.

## Developing
Firedancer currently only supports Linux and requires a relatively new
kernel, at least v4.18 to build.

```console
$ git clone --branch v26.09 --recurse-submodules https://github.com/jito-foundation/firebam.git
$ cd firebam
$ ./deps.sh
$ if [ -f "$HOME/.cargo/env" ]; then source "$HOME/.cargo/env"; fi
$ source activate  # enter build environment
$ make -j all fdctl firedancer

# Run a new development cluster
$ firedancer-dev

# Join Solana testnet
$ firedancer-dev --testnet
```

The build includes both the full Firedancer validator and Frankendancer
(`fdctl`), which compiles the BAM-patched Agave runtime. Agave and the BAM wire
schema are pinned submodules fetched from the URLs in `.gitmodules`; a fresh
clone does not need sibling checkouts or local patches. After changing branches,
run `git submodule sync --recursive` and
`git submodule update --init --recursive` to use that branch's pinned commits.

`firedancer-dev` (without args) configures your system for validator
operation and creates a new lcoal development cluster. First it creates
a genesis block, some keys, a faucet, and then it starts a validator on
the local machine. `firedancer-dev` will use `sudo` to make privileged
changes to system configuration where needed. If `sudo` is not available,
you may need to run the command as root.

If you wish to join this cluster with other validators, you can define
`[gossip.entrypoints]` in the configuration file to point at your first
validator and join with `firedancer-dev run`.

## License
Firedancer is available under the [Apache 2
license](https://www.apache.org/licenses/LICENSE-2.0). Firedancer also
includes external libraries that are available under a variety of
licenses. See [LICENSE](LICENSE) for the full license text.
