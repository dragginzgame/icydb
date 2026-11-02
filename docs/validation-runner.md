# Validation target runner

Reusable execution and retained evidence for consumer-selected Make targets.
Shared Tooling's [validation policy](../DRAGGINZGAME.md#validation-and-evidence)
determines which checks to select; this runner does not authorize broader gates.

`scripts/ci/run-validation-targets.sh` runs one or more Make targets in order,
records their durations, highlights live failures with an `[ERR:<target>]`
prefix, retains complete and condensed failure logs, and writes a GitHub step
summary when one is available. Pass `--fail-fast` to stop after the first
failed target.

The opt-in `--plan FILE` mode admits a source/group-bound, exhaustive consumer
stage selection and always fails fast. See [release group validation](release-group-validation.md)
for its policy, retained evidence and adoption requirements. Positional targets
retain their existing behavior.

Vendor `scripts/ci/check-make-execution.sh`, `scripts/ci/verify-file-checksum.sh`
and [the input-checkpoint companion](../scripts/ci/validation-input-checkpoint.pl),
plus [plan admission](../scripts/ci/release-validation-plan.pl),
alongside the runner through its declared companion closure. The runner captures
that companion with its own source before dispatch, so a gate replacing the
original checkout cannot change the active invocation's input admission.
The execution check rejects
inherited Make ignore-errors, dry-run, question, touch and version-only modes before validation;
normal Make variables and parallel-job settings remain available to targets.
The complete argument list is checked before dispatch: arguments must be named
goals, not Make options, variable assignments or names containing tabs/newlines.
Pass variable selections through the caller's environment or owning Makefile.
`VALIDATION_RUNNER_DEPTH` is inherited nesting metadata: unset or empty starts
at zero; otherwise it must be a canonical non-negative decimal integer of at
most 18 digits. Invalid depth refuses before logs or targets are created.

```bash
scripts/ci/run-validation-targets.sh fmt-check shellcheck test
scripts/ci/run-validation-targets.sh --fail-fast preflight test
```

Opt into prepared-input checkpoints with repeated `--input` options before the
targets, after explicit preparation has selected the files:

```bash
scripts/ci/run-validation-targets.sh --fail-fast \
  --input Cargo.toml --input Cargo.lock --input testing/Cargo.lock \
  fmt-check check test
```

Declare each relevant regular file relative to the selected repository root;
symlinks, escaping paths and missing/unreadable files refuse. SHA-256 identities
are checked before and after every target. A change or observation error stops
later dispatch even without `--fail-fast`, reports the path and requires fresh
preparation and a complete new gate. An already-failed target keeps its original
status and output. Initial and observed identities stay in the announced unique
log directory on failure, alongside failure diagnostics; preparation, dependency
updates and retries are never automatic.

Nested loggers in the same physical checkout inherit the original checkpoint.
They may add explicitly declared inputs, but cannot refresh an inherited hash.
A logger in another checkout starts independently. `VALIDATION_INPUT_ROOT` and
`VALIDATION_INPUT_BASELINE` are internal inherited context, not consumer inputs.
Consumers still own graph selection and stage order. Put the logger at the
actual stage boundaries: wrapping a single target that runs a private Make loop
only checks before/after that entire loop. Checkpoints detect persistent drift;
they do not isolate running commands from concurrent edits or replace the release
runner's source-identity checks.

The script defaults to the repository containing its vendored copy. Set
`VALIDATION_REPOSITORY_ROOT` and `VALIDATION_FAILURE_LOG_DIR` when invoking it
from another location.
If retaining a failed log in the selected directory fails, the runner keeps its
temporary log directory and prints its location instead of deleting the evidence.

Each completed failed batch also prints a unique combined log path and updates
`latest-combined.log` in that failure directory. This is the byte concatenation
of failed targets' raw logs in dispatch order. `latest.log` continues to hold
the last failed target alone. Successful or interrupted batches leave the last
completed combined view intact; interrupted raw logs remain available separately.
Nested runs combine their own target streams without rediscovering child files.
Each failed target's diagnostic excerpt is selected once and reused for retained
highlights and the GitHub summary. Live output still streams as it arrives;
only the outermost runner replays final console excerpts, once at the end of the
batch. Nested runs retain their raw logs and report paths without replaying the
same excerpts through every parent.

`latest.log`, `latest-errors.log` and `latest-combined.log` are atomically replaced
convenience copies. A failed update preserves the previous copy and any failed
candidate. Use the announced unique paths for evidence from a particular run;
the raw and highlighted summary footers identify those files directly.

If GitHub summary publication fails, the runner retains the complete local
summary and raw logs, finishes failure-log collection and preserves a failed
target's status. If all targets passed, the summary failure still fails the run.

Temporary cleanup errors are reported with the affected directory. They fail a
successful run while preserving any existing target or reporting failure status.

Set `VALIDATION_LOG_DIR` to retain successful, failed and interrupted raw output
under a unique run directory announced before dispatch. Its `timings.tsv` has
`target`, `result`, `seconds` and `log` columns, with one row per completed target.
Relative `VALIDATION_LOG_DIR` paths resolve from the invocation directory,
independently of inherited `CDPATH`; leading-dash directory names are supported.
An interrupted target can have a partial raw log without a completed timing row.
`VALIDATION_FAILURE_EVENT_PREFIX` adds a literal line prefix, such as
`[CANIC-TEST:E`, to live highlighting and bounded failure details. Raw logs keep
their original bytes. Live output and retained summaries use the same diagnostic
rules, including fatal, LLVM and missing-file errors before long ordinary log
tails. Surrounding context and passing namespaced Rust tests remain neutral.
Success returns zero; failure preserves the first failed
Make invocation's status (normally 2, rather than the recipe's own status).
A logging-only pipeline failure also returns nonzero. SIGINT/SIGTERM exit with
130/143 when handled.
Both the temporary-source wrapper and validation body require explicit
completion before returning success. Premature exits, including Bash 3.2
nounset errors that report zero, fail and retain their available source or logs.
Nested invocations retain separate run directories and only the outermost writes
the GitHub summary. Consumer callers still own targets and child color policy:
set a product variable such as `CANIC_TEST_COLOR` before dispatch when the caller
has a terminal, respecting an explicit selection and `NO_COLOR`. The runner
passes that environment through without learning product-specific variable names.


For setup, source adoption and consumer-owned commands, see the
[Make interface](make-interface.md) and [release contract](releases.md).
