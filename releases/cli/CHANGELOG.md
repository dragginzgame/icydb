# IcyDB cli changelog

Last coordinated release: [0.294.0](../history.md). Independent cli releases
begin with 0.295.0 below.

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
