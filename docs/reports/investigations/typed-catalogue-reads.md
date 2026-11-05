> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Typed catalogue reads — shared accepted bindings

## Result and bounded disposition

Retain shared immutable facade bindings. In this representative nested
catalogue fixture, ordinary 128-row reads use **16.1–16.3% fewer query
instructions** and **13.1% fewer explicit-update cycles**. Raw Wasm falls
**744 bytes**. All selected fields, IDs, order and continuation results match
independent host expectations. No application code changes are required.

This completes R1 of the authorised 0.260 plan. R2 (selected-field ergonomics)
has not started. W1 still requires the deferred Canic-owned fixture extension.
This is not a measurement of Toko Miner's deployment or its remaining
instruction-cap headroom; no production ceiling is proposed.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/19/typed-catalogue-reads/01/report.md).
