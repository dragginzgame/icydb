# SQL Regression Corpus

This directory owns reviewed, minimized SQL regression inputs for the sole
current generator and corpus formats.

The current inventory is empty. Current scheduled witnesses own the maintained
aggregate `DISTINCT` and aggregate `FILTER` contracts independently.

Each future entry is one RFC 8785 canonical JSON file named
`<regression-id>.json`. The filename stem must equal the entry's `regression_id`,
which also makes duplicate reviewed identities impossible within this directory.
Entries must decode as the sole current `RegressionCorpusEntry` format. The
generator crate's `checked_in_regression_corpus` function is the only filesystem
inventory; it rejects unsupported entries and bounds reads before the decoder
rejects unknown fields, stale versions, non-canonical bytes, invalid embedded
generator facts, and files above the scheduled 1 MiB bound.

An entry may be created only from a replay whose deterministic minimization is
complete. Conversion retains the minimized typed case and its current expected
behavior, but deliberately drops the historical mismatch and observed failing
outcomes. Corpus execution is therefore a positive current-contract check, never
a failure allowlist or compatibility promise.

Generator or corpus format changes are hard cuts before 1.0. Update or delete every
entry in the same slice; do not add a legacy decoder, alias, or migration fallback.
