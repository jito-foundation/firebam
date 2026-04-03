# Testing Firedancer

## Golden Configuration

The most reliable system configuration to run tests is as follows.

- Kernel version: Linux 4.18 or newer
- Operating system: RHEL 8 or Ubuntu 22.04 (or Fedora/Debian equivalents)
- Compiler versions: GCC 12 or Clang 15
- CPU: Icelake Server or Epyc 2 (or newer)
- Memory: 2 gigantic pages (2x1 GiB) per core, reserved via `fd_shmem_cfg`.

Although we aim to support tests on a wide variety of hosts (including
architectures other than x86), the above configuration is what the
Firedancer team uses internally.  It also helps eliminate various system
noise such as page table walks, page faults, allocation failures, OOM
kills, etc.

## Quick Start

Assuming system is configured and dependencies are installed:

```
sudo src/util/shmem/fd_shmem_cfg alloc 2 gigantic 0
make -j
make run-unit-test
```

For large page and NUMA configuration, refer to `./test.sh --help`.

## Test Configuration

### Unit Tests

**Unit tests** are C programs that contain test logic for Firedancer's
modules.  They can be found adjacent to source code in the `/src` dir
and are titled `test_{...}.c`.  Example `Local.mk` configuration:
```make
# call make-unit-test,name,         object list,dependencies
$(call make-unit-test,test_mymodule,test_module,fd_ballet fd_util)
```

**Automatic unit tests** are C programs that run without any command-line
parameters.  They may only run on the main thread and complete successfully
given 2 GiB memory (backed by any page type).  They are tested on every
commit as-is, and are run at least weekly with extended instrumentation.
Typically, they only use the main thread and finish in under 5 minutes.
Example `Local.mk` configuration:
```make
# call run-unit-test,name
$(call run-unit-test,test_mymodule)
```

### Fuzz Tests

**Fuzz tests** verify the behavior of a component given a large number
of arbitrary byte sequences.  Fuzzing is typically effective at finding
bugs in parsers.  Differential fuzz tests can detect diverging behavior
when comparing a module to a reference implementation (for example, the
virtual machine).

Fuzz tests are typically combined with sanitizers for improved error
detection.

Example `Local.mk` configuration:
```make
ifdef FD_HAS_HOSTED
# call make-fuzz-test,name,         object list,  dependencies,     link flags (optional)
$(call make-fuzz-test,fuzz_mymodule,fuzz_mymodule,fd_ballet fd_util,-lfoo)
endif
```

In order to find new test inputs, a fuzz engine is required.

| Engine    | Compile command                                         |
|-----------|---------------------------------------------------------|
| libFuzzer | `make CC=clang EXTRAS=fuzz`                             |
| AFL++     | `make CC=clang EXTRAS=afl++ AFL_LIB=/usr/local/lib/afl` |
| Honggfuzz | `make MACHINE=linux_clang_haswell EXTRAS=honggfuzz`     |

The **[libFuzzer]** engine is part of recent versions of LLVM, making
it the most convenient way to get started. It requires Clang.

To use the **[AFL++]** engine, install AFL++ on the host, then point the
Firedancer build at the directory that contains **`libAFLDriver.a`** and
**`afl-compiler-rt.o`** (often **`/usr/local/lib/afl`** after
`sudo make install`).  That directory is **`AFL_LIB`**.  Full install and
build steps are in **B. AFL++** below; there is no `./configure`—AFL++ uses
**`make`** and optional **`LLVM_CONFIG=llvm-config-<N>`** to match your
Clang version.

To use the **[Honggfuzz]** engine (persistent fuzzing mode with
sanitizer-coverage instrumentation), install `hfuzz-clang`.
Note that building with Honggfuzz is only supported via
`make MACHINE=linux_clang_{...} EXTRAS=honggfuzz`, but not via `make CC=hfuzz-clang`.

  [libFuzzer](https://llvm.org/docs/LibFuzzer.html)
  [AFL++](https://aflplus.plus/)
  [Honggfuzz](https://honggfuzz.dev/)

If no fuzzing engine is provided, the fuzz tests are still built with a
stub engine.  The stub engine cannot find any new inputs, but can still
regression test against old inputs like so:
```
$ "$OBJDIR/fuzz-test/fuzz_bla" <input1> <input2> ...
```

#### Paths and placeholders

- **Object directory:** After choosing `CC`, `MACHINE`, and `EXTRAS`, run
  `make help | grep OBJDIR` and use that value as `OBJDIR`.  The fuzz
  binary is always `OBJDIR/fuzz-test/<name>` (for example
  `OBJDIR/fuzz-test/fuzz_bam_client`).
- **`FUZZ_TARGET` in this document** is a **placeholder**, not a name
  that exists in the build.  **Do not** type it literally on the command
  line.
  - In `make … -j <word>`, `<word>` is a **Make goal** (a build target
    defined in the tree).  Writing `-j FUZZ_TARGET` asks Make to build a
    goal called `FUZZ_TARGET`, which is **not** defined—substitute a real
    fuzz name such as `fuzz_bam_client` or `fuzz_quic_wire`.
  - That does **not** create a shell variable.  To reuse a name in Bash,
    assign it yourself, e.g. `FUZZ=fuzz_bam_client`, then use
    `corpus/$FUZZ/` and `$OBJDIR/fuzz-test/$FUZZ`.
  - Real fuzz goal names are the first argument to `make-fuzz-test` in
    each `Local.mk` under `src/` (e.g. `fuzz_bam_client`).

#### One engine per build

libFuzzer, AFL++, and Honggfuzz are **alternatives**.  Each full build
links **one** of them (or the **stub** if you use default GCC without
fuzz extras).  When switching engines or sanitizer bundles, run
`make clean` first so flags and objects stay consistent.

#### A. libFuzzer (step-by-step)

Pick a harness name **`<fuzz>`** (e.g. `fuzz_bam_client`); see **Paths and
placeholders**.  Do not type `FUZZ_TARGET` literally.

<<<<<<< Updated upstream
1. **Prerequisites:** Clang with LLVM (libFuzzer).  Dependencies installed
   (e.g. `./deps.sh` per project docs).
2. **Seeds:** Ensure `corpus/<fuzz>/` exists with at least one file, or use
   an existing `corpus/<fuzz>/` from the repo.  Example:
   `corpus/fuzz_bam_client/`.
3. **(Recommended) Clean:** `make clean`
4. **Build — choose exactly one libFuzzer flavor (do not run both):**
   - **libFuzzer only:**  
     `make CC=clang EXTRAS=fuzz -j fuzz_bam_client`
   - **libFuzzer + AddressSanitizer (recommended for bug finding):**  
     `make CC=clang EXTRAS="fuzz asan" -j fuzz_bam_client`
   - For a different harness, replace `fuzz_bam_client` with another
     goal from `Local.mk` (same spelling as in `make-fuzz-test`).
   - The word after `-j` is a **Make goal**, not a shell variable.
5. **Discover new inputs — choose one approach:**
   - **Manual run** (set `OBJDIR` from `make help` with the **same**
     `CC`/`EXTRAS`; replace `<fuzz>` with the same name you built):
=======
1. **Prerequisites**
   - **Clang** with a recent **LLVM** (libFuzzer is provided by the compiler
     runtime; you do **not** install a separate AFL-style package).
   - Firedancer dependencies (e.g. **`./deps.sh`** per project docs).
   - Use **`CC=clang`** for fuzz builds.

2. **Seeds**
   - Ensure **`corpus/<fuzz>/`** exists with at least one file, or use corpora
     shipped with the repo (example: **`corpus/fuzz_bam_client/`**).

3. **Clean when switching**
   - After changing **`EXTRAS`** (e.g. stub ↔ **fuzz** ↔ **afl++** ↔
     **honggfuzz**) or adding / removing **`asan`**, run **`make clean`**
     so linked libraries and objects stay consistent (see **One engine per
     build** above).

4. **Build fuzz tests**
   - Build **exactly one** of the libFuzzer flavors below for a given tree
     (do not mix objects from both without **`make clean`**).
   - **One harness — libFuzzer only:**
     ```
     make CC=clang EXTRAS=fuzz -j fuzz_bam_client
     ```
   - **One harness — libFuzzer + AddressSanitizer** (common for bug hunting):
     ```
     make CC=clang EXTRAS="fuzz asan" -j fuzz_bam_client
     ```
   - **All harnesses, compile only** (no corpus run):
     ```
     make CC=clang EXTRAS=fuzz -j fuzz-test
     ```
     (Use **`EXTRAS="fuzz asan"`** instead if you chose the ASan flavor.)
   - Replace **`fuzz_bam_client`** with any goal defined via **`make-fuzz-test`**
     in a **`Local.mk`** under **`src/`**.  The token after **`-j`** is a
     **Make goal**, not a shell variable.

5. **`OBJDIR` and binary path**
   - With the **same** **`MACHINE`**, **`CC`**, **`EXTRAS`**, and any other
     flags that affect the build (**`EXTRA_CPPFLAGS`**, etc.) as in step 4:
     ```
     make CC=clang EXTRAS=fuzz help | grep OBJDIR
     ```
   - The harness is **`$OBJDIR/fuzz-test/<fuzz>`** (substitute the printed
     path for **`$OBJDIR`**).

6. **Run tests (corpus regression)**
   - **`run-fuzz-test`** and **`<fuzz>_unit`** invoke **`$OBJDIR/fuzz-test/<name>`**
     via the Makefile; **`OBJDIR`** is computed from the same **`CC`** /
     **`EXTRAS`** / **`MACHINE`** / … as your build.  Reuse **exactly** the
     variable list from step 4 (re-run step 5’s **`help | grep OBJDIR`** if
     unsure).
   - **All harnesses:**
     ```
     make CC=clang EXTRAS=fuzz -j run-fuzz-test
     ```
   - **One harness:**
     ```
     make CC=clang EXTRAS=fuzz -j fuzz_bam_client_unit
     ```
   - Default **`FUZZFLAGS`** (see **`config/base.mk`**) is intended for
     **libFuzzer**, so these targets usually work as-is.  Override on the
     **`make`** command line if you need different limits, e.g.
     **`FUZZFLAGS="-timeout=2 -runs=1"`**.

7. **Find new inputs (libFuzzer exploration)**
   - Unlike step 6, this **mutates** inputs and searches for crashes or new
     coverage.  The last arguments to the harness are **directories of seed
     files**; libFuzzer reads them and writes interesting cases under
     **`-artifact_prefix`** (and related flags).  **`corpus/<fuzz>/`** is
     the usual seed corpus; an empty or scratch dir (e.g.
     **`corpus/<fuzz>/explore`**) is often passed as an additional seed path
     for libFuzzer to populate.
   - **Manual run** (set **`OBJDIR`** from step 5, e.g. **`export OBJDIR=…`**):
>>>>>>> Stashed changes
     ```
     FUZZ=fuzz_bam_client
     mkdir -p corpus/$FUZZ/explore
     "$OBJDIR/fuzz-test/$FUZZ" \
       -artifact_prefix=corpus/$FUZZ/ \
       -max_total_time=300 \
       -timeout=5 \
       corpus/$FUZZ/explore \
       corpus/$FUZZ
     ```
<<<<<<< Updated upstream
   - **Makefile helper** (uses `FUZZFLAGS` from the build; see
     `config/base.mk`; `<fuzz>` is the Make goal name):  
     `make <fuzz>_run`  
     Example: `make fuzz_bam_client_run`
6. **Regression on existing corpus only** (no exploration):  
   `make run-fuzz-test` (all fuzz targets) or `make <fuzz>_unit`
   (one target; example: `make fuzz_bam_client_unit`).
=======
     Adjust timeouts and **`max_total_time`** as needed; see
     [libFuzzer](https://llvm.org/docs/LibFuzzer.html).
   - **Makefile helper** (same **`CC`** / **`EXTRAS`** as the build; uses
     **`FUZZFLAGS`** from **`config/base.mk`** by default):
     ```
     make CC=clang EXTRAS=fuzz -j fuzz_bam_client_run
     ```
     Replace **`fuzz_bam_client`** with your **`<fuzz>`**.
>>>>>>> Stashed changes

#### B. AFL++ (step-by-step)

Pick a harness name **`<fuzz>`** (e.g. `fuzz_bam_client`); see **Paths and
placeholders**.  Do not type `FUZZ_TARGET` literally.

<<<<<<< Updated upstream
1. **Prerequisites:** AFL++ installed; locate the directory that contains
   `libAFLDriver.a` and `afl-compiler-rt.o` (often `/usr/local/lib/afl` or
   `/usr/lib/afl`).  That directory is **`AFL_LIB`**.
2. **Seeds:** `corpus/<fuzz>/` with at least one input file.
3. **(Recommended) Clean:** `make clean`
4. **Build (single command):**  
   `make CC=clang EXTRAS=afl++ AFL_LIB=/path/to/afl/lib -j fuzz_bam_client`  
   Substitute the real path for `AFL_LIB` (or another fuzz goal instead of
   `fuzz_bam_client`).
5. **Discover new inputs:** AFL++ drives the binary; `make run-fuzz-test`
   is **not** the AFL++ campaign runner.  Example:
   ```
   mkdir -p findings
   AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1 AFL_SKIP_CPUFREQ=1 \
     afl-fuzz -i corpus/fuzz_bam_client -o findings -- "$OBJDIR/fuzz-test/fuzz_bam_client"
   ```
   Replace `fuzz_bam_client` with your `<fuzz>`; use your actual `OBJDIR`
   from `make help`.
=======
1. **Install AFL++** (on the fuzz host, once per install)
   - Install build dependencies (compiler, `git`, `cmake`, `python3-dev`,
     etc.) and **Clang/LLVM** for the version you want (Ubuntu example:
     `clang-18`, `llvm-18`, `llvm-18-dev` so **`llvm-config-18`** exists).
   - Clone and build.  AFL++ does **not** use `./configure`; use **`make`**
     and set **`LLVM_CONFIG`** if the default `llvm-config` is not the LLVM
     you want (match the Clang you use for Firedancer when possible):
     ```
     git clone https://github.com/AFLplusplus/AFLplusplus
     cd AFLplusplus
     make LLVM_CONFIG=llvm-config-18 all
     sudo make install
     ```
     **`make all`** is enough for normal **source** fuzzing.  Use
     **`make distrib`** only if you need QEMU/Frida/Nyx-style extras (slow,
     extra deps).  **`git submodule update --init`** pulls large optional
     repos; you can skip it for **`make all`** unless the build tells you
     otherwise.
   - Warnings you may ignore if LLVM mode succeeded: **gcc_plugin** (install
     `gcc-*-plugin-dev` if you want it), **LLVM LTO** (install **`lld-N`**
     matching your LLVM if you want LTO).
   - Find **`AFL_LIB`**: after install, artifacts are usually under
     **`/usr/local/lib/afl`**.  Verify both link objects exist:
     ```
     ls /usr/local/lib/afl/libAFLDriver.a /usr/local/lib/afl/afl-compiler-rt.o
     ```
     If they are elsewhere, search and use the directory that contains
     **both** files, e.g. `find /usr/local /usr/lib -name libAFLDriver.a`.

2. **Firedancer tree**
   - Dependencies as for any build (e.g. `./deps.sh` per project docs).
   - Use **`CC=clang`** for the fuzz build (same major LLVM as AFL++ is a
     good default).

3. **Seeds**
   - Ensure **`corpus/<fuzz>/`** exists with at least one file, or use corpora
     shipped with the repo.

4. **Clean when switching**
   - After changing **`EXTRAS`** (e.g. stub ↔ AFL++ ↔ libFuzzer) or adding
     / removing **`asan`**, run **`make clean`** so libraries are not mixed
     (stale ASan objects cause link errors with non-ASan links, and the
     reverse).

5. **Build fuzz tests**
   - **One harness** (replace `<fuzz>` and **`AFL_LIB`**):
     ```
     make CC=clang EXTRAS=afl++ AFL_LIB=/usr/local/lib/afl -j fuzz_bam_client
     ```
   - **All harnesses, compile only** (no corpus execution):
     ```
     make CC=clang EXTRAS=afl++ AFL_LIB=/usr/local/lib/afl -j fuzz-test
     ```
   - **Optional — AddressSanitizer** (must use **`make clean`** if you
     previously built without **`asan`**):
     ```
     make CC=clang EXTRAS="afl++ asan" AFL_LIB=/usr/local/lib/afl -j fuzz-test
     ```

6. **`OBJDIR` and binary path**
   - With the **same** **`MACHINE`**, **`CC`**, **`EXTRAS`**, **`AFL_LIB`**,
     and any other flags that affect the build (**`EXTRA_CPPFLAGS`**, etc.):
     ```
     make CC=clang EXTRAS=afl++ AFL_LIB=/usr/local/lib/afl help | grep OBJDIR
     ```
   - The harness is **`$OBJDIR/fuzz-test/<fuzz>`** (substitute the printed
     path for **`$OBJDIR`**).

7. **Run tests (corpus regression)**
   - **`run-fuzz-test`** and **`<fuzz>_unit`** run **`$OBJDIR/fuzz-test/<name>`**
     for each harness; **`OBJDIR`** is chosen **inside Make** from the same
     **`MACHINE`**, **`CC`**, **`EXTRAS`**, **`AFL_LIB`**, etc.  You do **not**
     pass **`OBJDIR=...`** on the command line.  Use the **exact same** variable
     list as in step 5 so that internal **`OBJDIR`** is the same directory as
     your build (re-run step 6’s **`help | grep OBJDIR`** if you are unsure).
   - **All harnesses** (build if needed, then replay every **`corpus/<name>/`**):
     ```
     make CC=clang EXTRAS=afl++ AFL_LIB=/usr/local/lib/afl -j run-fuzz-test
     ```
   - **One harness** (same idea; Make still uses **`$(OBJDIR)/fuzz-test/<fuzz>`**
     under the hood):
     ```
     make CC=clang EXTRAS=afl++ AFL_LIB=/usr/local/lib/afl -j fuzz_bam_client_unit
     ```
   - **Optional — run one harness by hand** (after you set **`OBJDIR`** in your
     shell from step 6, e.g. **`export OBJDIR=build/native/clang`**):
     ```
     find corpus/fuzz_bam_client -type f -exec "$OBJDIR/fuzz-test/fuzz_bam_client" {} +
     ```
     Pass **`FUZZFLAGS`** only if your engine understands them; append them
     before **`{} +`** if needed.  For AFL++, you often want no extra flags
     here (see next bullet).
   - Default **`FUZZFLAGS`** on **`make`** (see **`config/base.mk`**) targets
     **libFuzzer**.  If an AFL++ binary fails on unknown options, retry with
     **`FUZZFLAGS=`** on the **`make`** command line for **`run-fuzz-test`** or
     **`<fuzz>_unit`**.

8. **Find new inputs (AFL++ campaign)**
   - Step 7 only **replays** files that already exist under **`corpus/<fuzz>/`**
     (regression).  Step 8 is for **exploration**: **`afl-fuzz`** runs your
     harness in a loop, **mutates** bytes, and keeps inputs that reach new
     code paths or crash.  You are not hand-picking each byte; the engine
     generates the attempts.
   - **`-i` (input seeds):** A directory of **starting** files.  The repo’s
     **`corpus/<fuzz>/`** is the usual choice, but **`-i` can be any path**
     (another folder, a copy of seeds, a minimal single file, inputs from a
     teammate, etc.).  You only need something for AFL++ to vary from; quality
     seeds help coverage.
   - **`-o` (output):** AFL++ writes **new** artifacts here (queue, crashes,
     metadata).  This is **not** the same as **`corpus/`**; promoting
     interesting files back into **`corpus/<fuzz>/`** is a manual or scripted
     follow-up if you want them in version control.
   - Example (set **`OBJDIR`** from step 6, e.g. **`export OBJDIR=…`**):
     ```
     mkdir -p findings
     AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1 AFL_SKIP_CPUFREQ=1 \
       afl-fuzz -i corpus/fuzz_bam_client -o findings -- \
       "$OBJDIR/fuzz-test/fuzz_bam_client"
     ```
   - Replace **`fuzz_bam_client`** with your **`<fuzz>`**; change **`-i`** or
     **`-o`** paths if you want different seed or output directories.  See
     [AFL++](https://aflplus.plus/).
>>>>>>> Stashed changes

#### C. Honggfuzz (step-by-step)

Use a concrete Make goal **`<fuzz>`** (e.g. `fuzz_bam_client`); see
**Paths and placeholders**.

1. **Install Honggfuzz** (on the fuzz host, once per install)
   - Honggfuzz is **not** available as a distro package on most systems.
     Build it from source:
     ```
     sudo apt-get install -y libunwind-dev libbfd-dev binutils-dev libssl-dev
     git clone https://github.com/google/honggfuzz.git
     cd honggfuzz
     make
     ```
   - Instead of `sudo make install`, add the compiled `hfuzz_cc/` directory
     to your `PATH` so the wrappers are found without installing system-wide:
     ```
     export PATH=/path/to/honggfuzz/hfuzz_cc:$PATH
     ```
     Add that line to `~/.bashrc` to persist across sessions.  Confirm with:
     ```
     which hfuzz-clang
     ```

2. **Seeds:** `corpus/<fuzz>/` with at least one input file.
3. **(Recommended) Clean:** `make clean` cleans the **default** (`native/gcc`)
   build, not the `linux_clang_haswell` tree.  When switching engines or
   rebuilding after a Honggfuzz reinstall, remove the target directory
   directly:
   ```
   rm -rf build/linux/clang/haswell
   ```
4. **Build (single command):** Use **`MACHINE=linux_clang_*`** (not
   `CC=hfuzz-clang` alone).  Example:
<<<<<<< Updated upstream
   `make MACHINE=linux_clang_haswell EXTRAS=honggfuzz -j fuzz_bam_client`  
=======
   ```
   make MACHINE=linux_clang_haswell EXTRAS=honggfuzz -j fuzz_bam_client
   ```
>>>>>>> Stashed changes
   Pick `linux_clang_icelake`,
   `linux_clang_zen2`, etc. if they match your CPU; re-check **`OBJDIR`**
   with `make help` after setting `MACHINE`.
5. **`OBJDIR` and binary path**
   ```
   make MACHINE=linux_clang_haswell EXTRAS=honggfuzz help | grep OBJDIR
   ```
   The harness is **`$OBJDIR/fuzz-test/<fuzz>`**.
6. **Run corpus regression** (replay existing inputs, no new mutation)
   - The default `FUZZFLAGS` in `config/base.mk` are libFuzzer-style flags
     (`-max_total_time`, `-timeout`, etc.) that Honggfuzz binaries do **not**
     accept.  Pass `FUZZFLAGS=` to clear them.

   - **All harnesses:**
     ```
     make MACHINE=linux_clang_haswell EXTRAS=honggfuzz FUZZFLAGS= -j run-fuzz-test
     ```
   - **One harness:**
     ```
     make MACHINE=linux_clang_haswell EXTRAS=honggfuzz FUZZFLAGS= -j fuzz_bam_client_unit
     ```
   - **By hand** (after `export OBJDIR=…` from step 5):
     ```
     find corpus/fuzz_bam_client -type f \
       -exec "$OBJDIR/fuzz-test/fuzz_bam_client" {} +
     ```

7. **Discover new inputs** (exploration campaign)
   - The `fuzz_bam_client_run` Make target uses libFuzzer flags and is **not**
     suitable for Honggfuzz.  Run `honggfuzz` directly instead
     (set `OBJDIR` from step 5, e.g. `export OBJDIR=…`):
     ```
     honggfuzz -i corpus/fuzz_bam_client -W hfuzz_out_fuzz_bam_client -- \
       "$OBJDIR/fuzz-test/fuzz_bam_client"
     ```
   - Replace `fuzz_bam_client` with your `<fuzz>`.  See
     [Honggfuzz](https://honggfuzz.dev/) for additional flags (`-n` threads,
     `-t` timeout, etc.).

#### Corpus regression vs. finding new inputs

- **`make run-fuzz-test`** runs each built fuzz binary over **existing**
  files under `corpus/<name>/`.  It does **not** substitute for starting
  libFuzzer exploration, `afl-fuzz`, or `honggfuzz` when you want a **long
  campaign** to generate new inputs.
- **`make run-fuzz-test`** uses whatever engine (or stub) the binaries were
  **already** compiled with.
- In this doc: **libFuzzer** regression vs exploration are **A.6** vs **A.7**;
  **AFL++** are **B.7** vs **B.8**.

### Sanitizers

The codebase supports a number of **sanitizers** that bake in various
runtime checks.  Using sanitizers is not recommended for production.

Sanitizers can be combined with fuzzers.  (You can specify multiple
extras like `make EXTRAS="fuzz asan"`)

| Sanitizer                  | Compile command              |
|----------------------------|------------------------------|
| AddressSanitizer           | `make CC=clang EXTRAS=asan`  |
| UndefinedBehaviorSanitizer | `make CC=clang EXTRAS=ubsan` |
| MemorySanitizer            | `make CC=clang EXTRAS=msan`  |

**[AddressSanitizer]** helps detect invalid memory accesses.

**[UndefinedBehaviorSanitizer]** detects various kinds of hardware and
linguistic U.B.

**[MemorySanitizer]** detects reads of uninitialized memory.
MSan is special because it requires all dependencies to be recompiled.
This is done by running `deps.sh +msan`.

  [AddressSanitizer](https://github.com/google/sanitizers/wiki/AddressSanitizer)
  [UndefinedBehaviorSanitizer](https://clang.llvm.org/docs/UndefinedBehaviorSanitizer.html)
  [MemorySanitizer](https://clang.llvm.org/docs/MemorySanitizer.html)

## Best Practices

We try to encourage in-depth testing by ensuring that the test suite is
reliable and runs on a wide-variety of hosts.

**Determinism**: Running the same test program (with unvarying inputs)
should result in predictable behavior.  When randomness is required, the
program should use a deterministic pseudorandom number generator such as
`fd_rng_t`.  The program may allow the user to change the RNG seed or
iteration count via command-line flags.  Examples of breaking test
determinism include using the current time as a random value, or
expecting the order in which tests are executed to stay the same.

**No inputs**: Unit tests should try to support automatic configuration
to ensure they are run frequently.  Apart from aforementioned requirements,
this is achieved by bundling inputs (`FD_IMPORT_BINARY`), and being able
to run without additional command-line arguments.

**Memory management**: DO NOT CALL `MALLOC()` IN TESTS.  Instead, refer
to the instructions below to acquire memory.

**Use static variables**: If your program requires small-ish amount of
memory (e.g. 4 MiB), use `.bss` by declaring uninitialized static
variables.  This has the benefit of better support for some embedded
targets such as on-chain virtual machines.

**Memory allocation**: If a larger amount of memory is required, tests
should allocate an anon workspace from shmem given the following flags:
- `--page-sz`: Size of memory pages to request (normal/huge/gigantic)
- `--page-cnt`: Number of pages to request for given type
- `--numa-idx`: NUMA node on which memory should be allocated
- Most tests default to 1 "gigantic" page, as per our recommendation to
  use x86 1 GiB pages.
- This can be achieved with the following pattern:
  ```c
  ...
  /* setup */
  ...

  char const * _page_sz = fd_env_strip_cmdline_cstr ( &argc, &argv, "--page-sz",  NULL, "gigantic" );
  ulong        page_cnt = fd_env_strip_cmdline_ulong( &argc, &argv, "--page-cnt", NULL, 1UL        );
  ulong        numa_idx = fd_env_strip_cmdline_ulong( &argc, &argv, "--numa-idx", NULL, fd_shmem_numa_idx(cpu_idx) );

  FD_LOG_NOTICE(( "Creating workspace with --page-cnt %lu --page-sz %s pages on --numa-idx %lu", page_cnt, _page_sz, numa_idx ));
  fd_wksp_t * wksp = fd_wksp_new_anonymous( page_sz, page_cnt, fd_shmem_cpu_idx( numa_idx ), "wksp", 0UL );
  FD_TEST( wksp );

  ...
  /* tests */
  ...

  fd_wksp_delete_anonymous( wksp );
  ```
- Using `fd_scratch` over "raw" shmem pages or `static uchar[]` is also fine.
