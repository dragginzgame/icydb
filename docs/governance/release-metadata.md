# Release candidate projection

IcyDB uses the published IC Build Cargo metadata editor for root-manifest and
lockfile changes. The selected release is recorded in `Cargo.lock`; the
`source_root` context API is available in IC Build 0.22.1. No sibling checkout or
unpublished executable is required.

The Make release adapters admit the locked, prepared editor through
`CARGO_RELEASE_METADATA_EDITOR` and forward the positive per-document allowance
in `CARGO_RELEASE_METADATA_MAX_BYTES`. Prepare tools through the ordinary
workstation setup before validation. Projection never installs dependencies or
selects an editor from `PATH`.

`render-candidate.sh` provides the same pure projection to version preparation
and receipt admission. IC Build owns Cargo syntax, workspace member identities
and selected version edits. IcyDB supplies captured metadata and retains its
exact internal pin policy. Every candidate renders successfully before
preparation writes any file; the enclosing release transaction owns rollback.
Receipt admission checks producer status before comparing complete bytes.

README text and inherited child manifests are ordinary source. They are not
rewritten by release preparation; receipt checks reject unexpected edits.
Release notes retain their existing finalization and admission rules.
