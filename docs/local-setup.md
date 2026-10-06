# Local developer setup

From the checkout root, prepare the common executables explicitly:

```bash
make install-tools
make tools-check
export PATH="$PWD/.tools/host/bin:$PWD/.tools/ic/bin:$PATH"
```

The Makefile uses those local paths automatically. Interactive shells need the
export above; setup does not edit shell profiles. Installation downloads tools;
checks are offline and never install missing dependencies. `install-tools` runs
host setup followed by IC setup, stopping on failure. Each set activates
independently: a later IC failure leaves the successfully installed host set.

`make install-host-tools` installs jq, **Mike Farah yq** (including its TOML
parser), and ripgrep with PCRE2 under `.tools/host/bin`. `make host-tools-check`
verifies their bytes before executing version and feature checks. The reviewed selections
live in [ci/tool-versions.env](../ci/tool-versions.env):
[jq 1.8.2](https://github.com/jqlang/jq/releases/tag/jq-1.8.2) and
[yq 4.47.2](https://github.com/mikefarah/yq/releases/tag/v4.47.2), and
[ripgrep 15.2.0](https://github.com/BurntSushi/ripgrep/releases/tag/15.2.0).
Setup itself needs neither parser installed. Distribution packages named `yq`
may provide a different implementation; they do not replace the selected parser.

Host setup downloads official native binaries for Linux x86-64/ARM64 and macOS
Intel/Apple Silicon, checks their pinned SHA-256 values and exact versions, and
atomically activates the selected set. Previous sets and failed candidates
remain under `.tools/host-set.*`. A lock prevents simultaneous installations;
inspect `.tools/.host-tools.lock/owner` and confirm its process has stopped before
removing an abandoned lock. Cleanup remains explicit. The `--versions` option
selects a reviewed shell file, which is executable code like a Makefile.

The shared installer's `--with-ripgrep` flag adds ripgrep explicitly; without it,
the existing jq/yq interface needs no additional pins. Shared Tooling's Make and
CI callers select the flag. Consumers adopting it add `SHARED_TOOLING_RIPGREP_VERSION`
and the four `SHARED_TOOLING_RIPGREP_SHA256_*` archive digests to their reviewed
versions file, and pass the flag to both installation and offline verification.
Offline checks authenticate the retained archive and compare its executable
bytes with installed `rg` before running it. They require the exact version
(allowing the official revision annotation) and a successful `--pcre2-version`.
No Cargo installation, global PATH change or system-package replacement occurs.

The [IC set](ic-tools.md) supplies Quill, ICP CLI, didc, ic-wasm, PocketIC and
wasm-opt. dfx is excluded. Linux ARM64 supports the host set only; the full IC
set lacks a matching Quill asset. Native CI qualifies both sets on Linux x86-64
and both macOS architectures; mapping Linux ARM64 is not native qualification.

## Bootstrap prerequisites

Use the host package manager for the shell, Git, GNU Make, curl, archive tools,
Perl and ordinary utilities. For Linux Mint/Ubuntu, the following explicit
system setup also prepares the shared regression suite's cloc and ripgrep:

```bash
sudo apt-get update
sudo apt-get install -y bash git make curl ca-certificates tar gzip xz-utils perl cloc ripgrep
```

On macOS, with Xcode Command Line Tools and Homebrew already installed:

```bash
brew install make curl xz cloc ripgrep
```

macOS supplies Bash, Git (through Command Line Tools), tar, gzip, Perl and shasum.
Homebrew GNU Make is available as `gmake`; use it where the system Make does not
meet a consumer's requirements. These system packages follow the host's package
manager updates; they are not claimed to be checksum-pinned shared binaries.
Shared setup never invokes sudo or installs a package manager implicitly.

Rust repositories separately prepare their declared Rust toolchain, rustfmt and
pinned cargo-sort. Shared Tooling's portable fixtures also require Cargo and
the cargo-sort version in `ci/tool-versions.env`; prepare it explicitly:

```bash
source ci/tool-versions.env
cargo install cargo-sort --version "$SHARED_TOOLING_CARGO_SORT_VERSION" --locked
```

ShellCheck, actionlint and Gitleaks use the existing explicit CI installers and
pins; GitHub maintenance needs an installed, authenticated `gh`. Consumer-owned
Node/Rust toolchains, SDKs and product dependencies remain part of that repo's
declared setup. See the [complete script dependency table](supported-hosts.md#tool-specific-dependencies).
