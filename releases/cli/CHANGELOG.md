# IcyDB cli changelog

Last coordinated release: [0.294.0](../history.md). Independent cli releases
begin with 0.295.0 below.

## [0.296.5]

### 🩹 Fixed

- Include migration phase and captured child output when process-test outcome
  assertions fail, and retain signal/readiness/output context in cancellation
  controls, making intermittent failures diagnosable
  ([#599 — CLI process failure diagnosis](https://github.com/dragginzgame/icydb/issues/599)).

### 🔧 Changed

- Adopt Shared 0.31.11: close locally implemented, focused-validated issues
  separately from delivery, retaining issue-specific external acceptance.
- Adopt Shared 0.31.10's scoped formatting, richer LOC selection and CI download
  caching, with one repository-wide secret scan. Align Host Text's declared floor
  with its existing 0.74.3 selection; CLI commands and output are unchanged.
- Adopt Host Tools 0.86 with owner-defined bounded Candid decode limits and
  Platform 0.25, preserving CLI commands and output.
- Adopt Shared 0.31.6's hashing inventory and guard; qualify the Cargo-adoption
  fixture with the checker's complete companion files.

[Detailed changes](0.296.md).

## [0.296.4] - 2026-11-07

### 🩹 Fixed

- Retain failed CLI process call traces and fixture scripts in Linux/macOS CI artifacts,
  and run the existing environment-isolation regression on both macOS hosts
  ([#599 — CLI process failure diagnosis](https://github.com/dragginzgame/icydb/issues/599)).

### 🔧 Changed

- Select Build 0.78 and Host Tools 0.85 with Platform 0.24 for native CLI tooling,
  preserving CLI commands, options and output.
- Adopt Shared Tooling 0.31.5 with the complete release-guide export and isolated
  release-command checks; retain independent group identity and full fallback.

[Detailed changes](0.296.md).

## [0.296.3] - 2026-11-07

### 🔧 Changed

- Make release-recovery fixtures select their intended inner runner explicitly,
  so ordinary CI and captured release validation exercise the same contract.
- Link the published IcyDB 0.301 database family while preserving CLI commands,
  options and output.
- Keep release preflight aligned with the pending database notes while preserving
  full-validation fallback and the CLI's independent release identity.
- Remove duplicate generic tool-command fixtures from macOS CI while retaining
  the CLI's native checks and IcyDB-owned setup/automation qualification.
- Select the published Build 0.77 metadata editor through the existing Shared
  installer, retaining CLI release metadata and artifact preparation ownership.
- Adopt Shared Tooling 0.31.3: restore macOS system-Bash checksum admission and
  clearly refuse disappeared LOC scratch inputs. Use the renamed release-batch
  settings and schema-2 journals, automatic group capture and strict checksum verification.
  Keep completed older delivery state with its original reviewed tooling.

[Detailed changes](0.296.md).

## [0.296.2] - 2026-11-06

### 🔧 Changed

- Include published Wire/Derive 0.17.5 in the locked dependency graph.
  Preserve CLI commands, output and the independent database release identity.
- Link the IcyDB 0.300 database family while preserving CLI commands, options
  and output. Native Testkit types and logging ownership follow the database
  contract; storage formats are unchanged.
- Adopt Shared Tooling 0.30.9 with CBOR ownership checks and qualified frozen
  delivery on Linux. Use Shared's saved setup, recovery, status and guarded
  reconciliation commands; keep publication explicit and dependency edits deferred.
- Separate live product invariants from automation regression fixtures and
  support qualified database/CLI selection through the existing proof and recovery
  owners. Retain all affected product and consumer coverage; unknown inputs
  keep the full gate and selected publication still verifies final archives
  ([#603 — Qualify database and CLI release plans](https://github.com/dragginzgame/icydb/issues/603)).

## [0.296.1] - 2026-11-06

### 📝 Summary

- Adopt the published IcyDB 0.299 database family and current Build dependency.
  Preserve CLI commands, options and output; the database family owns its
  Storage 0.68 fresh-installation cut and Testkit 0.88 testing contract.

See [detailed notes](0.296.md).

## [0.296.0] - 2026-11-05

### 🛠️ Tooling

- Adopt Shared Tooling 0.30.4 with the macOS LOC fix and an enforced Candid
  source boundary. Compact historical experiments into topic summaries and
  keep new raw Wasm captures under `target/evidence/`; preserve release and
  recovery records. Report uncompressed module and code-section bytes.
- Adopt Storage 0.67.1, Testkit 0.87, Wire 0.17.2, Platform 0.23.0 and
  Host Tools 0.83 as one root dependency cohort; refresh Shared Tooling 0.30.4.
  Preserve stored layouts and codec bytes; Canic composition remains separately
  unqualified ([#373 — Canic adapter qualification](https://github.com/dragginzgame/icydb/issues/373)).
- Adopt the current Shared LOC presentation: clearer column spacing, `???`
  for unclassified source and diagnostic guidance in help. Preserve JSON
  categories and source totals.
- Restore exported Shared documentation links through the canonical upstream
  repair, without copying central fleet inventories
  ([Shared #355 — Complete exported documentation links](https://github.com/dragginzgame/shared-tooling/issues/355)).
- Adopt Shared Tooling 0.30.0: explicit source categories, plural test directories,
  external test/helper modules and checked lint expectations. Make source scans
  reject missing inputs; retain the independent Canic qualification boundary.
- Select independently qualified CLI-only release checks on Linux, keeping
  database, shared-input and unqualified-host releases on full validation.
  Carry selected evidence through release recovery and keep final Cargo package
  verification before publication
  ([#603 — Qualify separate database and CLI release plans](https://github.com/dragginzgame/icydb/issues/603)).

### ⚠️ Breaking

- Parse diagnostic values and require migration abort/adopt confirmation in Clap
  before execution. Invalid values and missing `--yes` now use usage diagnostics
  and exit 2; payload, domain and execution failures retain exit 1
  ([#627 — Move diagnostic parsing and migration confirmation into Clap](https://github.com/dragginzgame/icydb/issues/627)).

### 🩹 Fixed

- Escape rejected options, subcommands and extra tokens in Clap diagnostics;
  prevent preformatted tips from echoing unsafe input while retaining ordinary
  tips, usage, help and version output
  ([#627 — Terminal-safe Clap parse refusals](https://github.com/dragginzgame/icydb/issues/627)).

### 🧹 Cleanup

- Use published Host Tools for bounded inventory and cycle-balance
  decoding. Remove cycle display-text parsing; retain local funding thresholds,
  unknown-balance handling, sorted inventory and manifest-fallback diagnostics
  ([Tooling #335 — Share bounded ICP inventory and cycle decoding](https://github.com/dragginzgame/ic-tooling/issues/335)).
- Delegate empty/odd-length response checks to Host while preserving response
  normalization and terminal-safe diagnostics. Declare diagnostic environment
  defaults in Clap and pass parsed codes/facts directly to handlers.

[Detailed changes](0.296.md).

## [0.295.2] - 2026-11-04

### 🛠️ Tooling

- Adopt Host Tools 0.81 and the committed Shared Tooling 0.29.4 snapshot.
  Qualify published Host Artifacts/Fs/Process 0.74.3, Metrics 0.29.1 and Runtime
  0.19.1 patches in the shared lock; these upstream changes extract test code.
  Remove the CLI's Metrics dependency; preserve report decoding, exact overflow
  quality and floor-rounded averages using IcyDB's report contract
  ([Observability #130 — Instrumentation-only Metrics](https://github.com/dragginzgame/ic-observability/issues/130)).

- Keep release qualification receipts beside Git recovery evidence so cache
  cleanup cannot erase them; missing evidence still refuses reuse
  ([#603 — Qualify separate database and CLI release plans](https://github.com/dragginzgame/icydb/issues/603), partial).

[Detailed changes](0.295.md).

## [0.295.1] - 2026-11-03

### 🩹 Fixed

- Refuse non-regular inventory fallback manifests without waiting for FIFO writers
  ([#615 — Refuse non-regular ICP manifests](https://github.com/dragginzgame/icydb/issues/615)).
- Resolve fixture funding from effective ICP project facts, including nested
  invocation; unknown or remote facts skip automatic top-up ([#616 — Resolve fixture funding from effective ICP facts](https://github.com/dragginzgame/icydb/issues/616)).
- Retain failed ICP inventory diagnostics across manifest fallback and optional
  setup hints ([#617 — Retain ICP inventory diagnostics during fallback](https://github.com/dragginzgame/icydb/issues/617)).
- Escape target labels in CLI-owned messages while preserving original ICP
  arguments ([#618 — Escape ICP target metadata](https://github.com/dragginzgame/icydb/issues/618)).
- Escape paths in CLI diagnostics and confirmations while preserving filesystem
  and install targets ([#620 — Escape file paths in CLI diagnostics](https://github.com/dragginzgame/icydb/issues/620)).
- Decode fallback inventory as YAML, preserving lists, comments and literal name
  bytes ([#621 — Decode fallback ICP inventories with canonical YAML](https://github.com/dragginzgame/icydb/issues/621)).
- Escape unsupported artifact format values in refusal messages before calls or
  publication ([#623 — Escape unsupported diagnostic artifact formats](https://github.com/dragginzgame/icydb/issues/623)).
- Refuse non-regular diagnostic JSON paths before reading; retain explicit stdin
  and regular-file symlinks ([#624 — Refuse non-regular diagnostic error JSON inputs](https://github.com/dragginzgame/icydb/issues/624)).
- Escape resolved diagnostic schema labels and rejected lookup inputs while
  preserving identity matching ([#625 — Escape diagnostic lookup inputs and schema labels](https://github.com/dragginzgame/icydb/issues/625)).
- Label failed SQL data updates as updates rather than DDL, preserving the
  unknown-outcome warning and refusal to retry automatically.
- Report refresh deployment failures using the selected deploy/reinstall mode,
  and describe unconfirmed local-network facts without claiming a remote target.

### 🧹 Cleanup

- Stream SQL statement boundaries without a temporary character-offset list;
  preserve quoted text and unfinished input
  ([#622 — Remove redundant SQL shell offset buffering](https://github.com/dragginzgame/icydb/issues/622)).
- Use shared ICP status command construction and installation decoding while
  retaining refresh safeguards ([#626 — Delegate ICP status mechanics to Host Tools](https://github.com/dragginzgame/icydb/issues/626)).
- Use shared ICP query/update call syntax while retaining endpoint selection,
  binary Wire arguments and mutation-failure handling.
- Reuse canonical lowercase hex encoding for SQL constraint-finding fingerprints
  and primary keys, preserving displayed bytes.
- Check shell help/exit commands without copying SQL lines to lowercase;
  preserve command spelling rules and submitted SQL text.
- Keep migration adoption confirmation and request construction in one explicit
  flow, removing its single-use callback runner.
- Remove redundant diagnostic artifact identity tracking; unique entity tags
  remain authoritative and resolver matching stays exact.

### 🧪 Testing

- Isolate subprocess fixtures from inherited controls. The historical hosted
  failure's cause remains unproven. Allocate unique fixture directories in Cargo
  test scratch without colliding with retained evidence
  ([#599 — Diagnose intermittent canister-ID test failure](https://github.com/dragginzgame/icydb/issues/599), partial).

### 🛠️ Tooling

- Adopt Platform 0.21 through the database graph without changing CLI protocols.
- Adopt published Host Tools 0.80.0 for native tooling.
- Adopt the Shared Tooling 0.29.1 development snapshot and strengthen release-plan
  evidence and fixture isolation; narrow release gates remain disabled
  ([#603 — Qualify separate database and CLI release plans](https://github.com/dragginzgame/icydb/issues/603), partial).
- Isolate Cargo metadata fixtures from inherited Git context and expose focused
  grouped-ordering checks ([#603 — Qualify separate database and CLI release plans](https://github.com/dragginzgame/icydb/issues/603), partial).

### 📚 Documentation

- Preserve issue-linked fixes in pending entries, standing agent instructions
  and the authoring prompt; keep fixed issues visible in each group's main changelog
  ([#619 — Preserve issue-linked changelog fixes](https://github.com/dragginzgame/icydb/issues/619)).

[Detailed changes](0.295.md).

## [0.295.0] - 2026-11-03

- Release the developer CLI independently with current native tooling, named Storage
  allocation diagnostics and improved canister-ID failure output. Add focused lint/test
  targets and source-bound release-plan qualification; the full release gate remains
  authoritative ([details](0.295.md)).
