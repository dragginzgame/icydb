# Caller catalog agreement

`check-tool-catalogs.pl` compares complete effective catalog records for named
callers within a workflow. It performs offline reads only: no consumer execution,
Make evaluation, catalog sourcing, installation, network, pin changes or release
operation. It is an opt-in adoption/maintenance check, not a new release blocker.

```bash
perl scripts/ci/check-tool-catalogs.pl --consumer /path/to/consumer \
  --bindings ci/tool-catalog-bindings.json --json
```

Shared Tooling also exposes `make check-tool-catalogs`, with
`TOOL_CATALOG_CONSUMER` and `TOOL_CATALOG_BINDINGS` selecting those same inputs.
Consumers can invoke the exported script from an optional CI target. The binding
file must exist in the selected consumer root; an absent file is unresolved.

## Bindings and coverage

Each workflow compares one independent kind (`host`, `rust` or `ic`) against an
explicit reference role. Name at least two callers. Include every relevant role:
setup/check, formatter, cache, collector and verifier where applicable. Separate
workflows may intentionally select different catalogs. There is no automatic
caller discovery; passing a partial inventory does not qualify omitted callers.

A minimal declared binding looks like this (replace both source fingerprints
with the SHA-256 of the reviewed file, and repeat the caller object per role):

```json
{
  "workflows": [{
    "name": "native-ci",
    "kind": "ic",
    "reference": "setup",
    "callers": [{
      "role": "setup",
      "catalog": "ci/ic-tools-qualified.tsv",
      "source": "Makefile",
      "source_sha256": "<64 lowercase hex characters>",
      "exception": "Explicit qualified optimizer selection; see local host evidence"
    }, {
      "role": "verifier",
      "catalog": "ci/ic-tools-qualified.tsv",
      "source": "scripts/ci/verify-evidence.sh",
      "source_sha256": "<64 lowercase hex characters>",
      "exception": "Same qualified IC selection as setup and collection"
    }]
  }]
}
```

Declared bindings describe a reviewed selection. They do **not** prove an
arbitrary script reads that path, and results say so explicitly. The fingerprint
makes source changes unresolved until the owner reviews the binding again; it is
not a substitute for a caller integration test. Do not infer a selection from an
isolated string match in Make, shell, Python or another language.

For an observable caller CLI, capture its read-only selection output in a JSON
file containing `catalog`, `source` and `source_sha256`, then replace those three
fields in the caller binding with `"observation": "path/to/captured.json"`.
The CLI should emit the same selected path it uses for its operation. Retain its
command and invocation context with the owning qualification evidence. The check
labels this a **recorded CLI observation**; it does not execute or authenticate
the producer. Hand-authored bindings must not be labelled observations. A stale
source fingerprint or both binding and observation fields produce UNRESOLVED.

For unsupported/computed callers, use a role with
`"unresolved": "Explain the missing binding or observable selection"`. Do not
omit it, guess a default, or execute its Makefile just to obtain a green result.
Conflicting role definitions, missing inputs and invalid schemas also fail.

## Ownership and comparison

Use `exception` for an explicitly selected consumer catalog, with the existing
local reason/qualification reference. This declaration does not approve a new
exception. For a snapshot-owned catalog, replace it with `snapshot` (the manifest
path relative to the consumer) and `snapshot_root` (its documented consumer root,
relative to the selected consumer; `.` for a root snapshot). The checker reports
its recorded revision and checks the selected catalog's exact manifest digest.
This is not whole-snapshot verification; retain the standard snapshot verifier.
Nested snapshots work without treating the binding file's directory as a root.
All source, observation, manifest and catalog inputs stay inside the consumer.

IC uses the canonical IC matrix parser, comparing tool/version/host/digest rows.
Host compares jq, yq, ripgrep and cloc versions and all their supported-host
digests. Rust compares the three common Cargo executable versions. Host and Rust
are independent even when stored in one environment file: a host override never
implicitly selects Rust. The literal environment parser is also used by the Cargo
cache key, preserving its byte fallback for nonliteral catalogs. Executable or
ambiguous catalogs are unresolved in this inspection; they are never sourced.

Comments, record order and different paths do not cause semantic mismatches.
Different digests at the same version and single-host changes do. An unused
snapshot default can coexist with an approved exception selected by every role.
Archive caller-pin byte identity remains a separate evidence contract.

Exit **0** means the listed selections agree, with each evidence label retained.
Exit **1** reports differing records and the affected role relative to its
reference. Exit **2** means invalid/incomplete/unresolved evidence; any known
mismatches are still included when another caller is unresolved. It never means
no findings. Use `--json` for structured roles, owners and record differences.
