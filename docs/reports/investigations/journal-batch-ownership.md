> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Single-owner mutation journal batches (F49)

Date: 2026-09-19. Active release notes: 0.259.5.

## Scope and ownership

Mutation preparation moves each JournalBatch into its existing CommitMarker,
rather than retaining one batch in PreparedJournalAppend and cloning another
into the marker. PreparedCommitEffects retains that marker through application;
publication borrows it, and tail append borrows the batch at the existing marker
ordinal. The intermediate CommitWindowPayload wrapper is removed.

CommitGuard still owns the exact encoded control slot. Backlog admission,
startup wake-up checks, fixed-header identity checks, marker bytes, append
ordering and persisted replay validation are unchanged. This is not raw-byte
reuse in place of canonical validation and adds no route, configuration, format,
reference counting or compatibility mechanism. The borrowed begin_commit
signature is propagated to schema/migration callers; their typed markers are
explicitly dropped after publication to preserve their previous lifetime.
Their separate batch clones are outside this mutation-only change.

## Measurement scope

Reuse returning_selected_cells_wasm_cost_matrix unchanged: six disposable
PocketIC cases, small/wide rows × count/id/all response shapes, three sequential
exact-ID UPDATEs with values 37/38/39 per case. Wide text is 1,050,000 bytes.
These are changing-literal calls, not cache-hit or statistical-replication trials.
Wide RETURNING * rejects before commit; it is not a successful full-row response.

Cycles cover the measured update endpoint's balance difference. Instructions
cover the actor's maintained request-execution interval, excluding ingress/egress
Candid encoding. Install, reset/seed, 64 settling ticks and independent readbacks
are excluded. There is no IC time advance or auto-progress. No native timing,
total workload speedup, peak-heap estimate or new production ceiling is claimed.

## Results

Both fresh runs pass all 18 cases and reproduce identical per-message costs.
All response hashes match the baseline; each run independently checks stored
values and the typed rejection. Twelve disposable local PocketIC instances
were created/dropped; no application network changed.

Raw Wasm: 4,219,650 -> 4,219,672 bytes (**+22**, +0.00052%).
Before SHA256:
`2fa4b908662a046eccd560f41ba4ee06b95a7c17ffa0a44157f92a1d61752524`.
After SHA256:
`b5d5c4eedb83770d7df7f4fc5fd24a0dff503f5e05182e7b75c54a0bfc83b7f3`.
Retained post-link keys, under the common artifact-set namespace
`823dde03c7560a3664bd3b412f48ad81c265e1cc1d77fb12be6d3b9aad847199`:

- Before: `89f732fc80d19b33810959e7b0fc1b35f33700b38cb005e9bc6779a4e23a6eb7`.

- After: `a452a95d1e188b3cc20fb3ab88d5dad477211b0e54ea9ee678a7f56440828eed`.

Both artifact hashes were independently reread from the retained output files.

Successful wide count/id writes save 3,442,403–5,869,622 cycles (0.856–1.503%)
and 3,278,779–5,547,543 instructions per measured message. Small-row cycle
changes are mixed (-7,707 to +5,709), not a general small-write improvement.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/19/journal-batch-ownership/01/report.md).
