# Provenance

Shared Tooling is maintained collaboratively across Dragginz Game repositories.

- Canic supplied the initial portable tooling and CI mechanics.
- The first cross-consumer review on 2026-09-03 incorporated IcyDB feedback on
  no-build decisions, state-space control, canonical authority, carried
  decision artifacts, reviewable changes, focused validation, and the boundary
  between shared baselines and project policy.
- The 2026-10-05 changelog review compared the local changelog guidance and current
  ledgers across Canic, IcyDB, IC Backup, IC Blob Storage, IC Host Tools, IC Memory,
  IC Metrics, IC Query, IC Testkit, IC Timers and Toko Miner. Canic and IcyDB supplied
  the main conventions for concise root summaries, minor-line detail files,
  compatibility notes and historical preservation in `rules/changelogs.md`.
  Their reviewed policy files were `docs/governance/changelog.md` at Canic revision
  `481e94d0e93080168fb04546cebb3fc48887a66f` and IcyDB revision
  `8e74b6bac5d054eab9dd9de3435f258d85926040`; those policy files were clean, while
  IcyDB's current ledger included working-tree edits. Shared Tooling retains its
  own draft, release and authority contract; consumer-specific presentation and
  scoped exceptions were not promoted into universal rules. This source review
  does not establish adoption in any consumer.

## Shared audit methods

The 2026-10-06 consolidation extracted reusable review questions from these
consumer definitions. Their audit-definition files were clean when reviewed;
unrelated in-progress consumer changes and new reports were not treated as
published method evidence.

| Source revision | Reused material |
| --- | --- |
| [IcyDB `8631511`](https://github.com/dragginzgame/icydb/tree/8631511d703f2b9ecf63ac865b9c4614116cba57/docs/audits) | Audit scope/evidence contract, consolidated flow-convergence and complexity methods, targeted module surface and cleanup playbooks. |
| [Canic `e1a211a`](https://github.com/dragginzgame/canic/tree/e1a211a00f01568ccc99bedc494c62a7141444dd/docs/audits) | Audit identity/comparison discipline, module reachability and runtime-shape proof requirements. |
| [IC Memory `6e98b07`](https://github.com/dragginzgame/ic-memory/blob/6e98b07882ed43d180195f5f915eafaef05f25db/docs/audits/recurring/code-hygiene.md) | API, trust-state, panic, constructor and negative-test hygiene. |
| [IC Timers `98c4b29`](https://github.com/dragginzgame/ic-timers/blob/98c4b296d7461525c15a01e30adbe33b75bcfa38/docs/audits/code-hygiene.md) | Narrow hygiene scope, typed/runtime authority and documentation consistency questions. |
| [Toko Miner `061cfb6`](https://github.com/dragginzgame/toko-miner/blob/061cfb6e3702a7075ab3c118bfaf315d7d2b0053/docs/audits/crosscutting/code-hygiene.md) | Cross-language hygiene, generated boundaries and tracked-artifact discipline. |

The common methods deliberately use individual findings instead of composite
scores, preserve inspection versus repair authority, and keep broad gates
explicit. Consumer-specific architecture, release-line traces, metric rosters,
numeric thresholds and method-catalog machinery remain local. Module cleanup is
an implementation procedure, not an additional audit verdict. Definition
consolidation does not imply consumer snapshot adoption or new runtime proof.

## Local IC tools and verification helpers

The common installer combines existing Canic and IcyDB Binaryen/PocketIC setup
requirements with IC Timers' verification-before-execution checks and IC Metrics'
retained provisioning evidence. Exact default versions match the inspected
consumers; Quill is added from the official release. Platform digests and release
sources are recorded in [IC tools](ic-tools.md). This is setup orchestration;
artifact inspection and execution contracts owned by `ic-host-tools` remain there.

The local jq/yq pair extends the existing shared yq selection in
`ci/tool-versions.env`; jq 1.8.2 digests were read from official release-asset
metadata on 2026-10-06. [Local setup](local-setup.md) records upstream releases
and the distinction between pinned executables and system bootstrap packages.

The evidence-manifest helper and its fixtures came from IC Blob Storage's
uncommitted working tree after `ebd535e` on 2026-10-06; those bytes are not attributed
to that commit or treated as released consumer behavior. The nonempty Cargo
test helper adapts Canic's `docs/audits/scripts/run-nonempty-cargo-test.sh` at
`e1a211a00f01568ccc99bedc494c62a7141444dd`, adding caller-workspace selection,
logging-failure handling and failed-output retention. The exact release-tag
check extracts IC Timers' annotated-tag/selected-commit checks at
`98c4b296d7461525c15a01e30adbe33b75bcfa38`, retaining explicit identity inputs
instead of choosing consumer metadata or HEAD. Consumer migrations remain
separate from preparation of these shared owners.

This record acknowledges sources; it does not make any consumer's local
governance authoritative here. Git history remains the exact source history,
and current repository documents own the maintained shared contract.

## Cargo metadata and CI installer convergence

The 2026-10-06 follow-up compared Canic's workspace inheritance test at
`d815abfc661d791ecf72afc5b1e4b6a990f37a91` plus its working-tree edits with
IcyDB's unchanged dependency graph guard at
`db8a0cc4419a7e0ae0ba419793ae5fea75b1cca7`. The structured inheritance option
replaces their common rule without copying product dependency bans, role
discovery or qualification policy. Its fixture covers the ordinary dependency
table form missed by IcyDB's line-oriented check.

Workspace-version readers were inspected in IC Testkit
`827157434eb8b2d6c13c4b8e47493bd6a38678b9`, IC Timers
`902323a9e896ce3771044fdc23a7a2d03d49cf28`, IC Host Tools
`1e018097a35c48fbc2bbe15c4ffef55d5e3d2e20` and the Canic revision above.
The shared reader uses Cargo's validation and yq/jq projection rather than
promoting a consumer's text parser. Version mutation remains local.

CI installer consolidation reuses Shared Tooling's three existing entry points
at `9f8c7c768793f4ce8f25be9e88282c0f63a06e7f`. Asset mappings, versions and
checksum contracts are unchanged; the shared implementation stages on the
destination filesystem and retains failed candidates. Consumers must review
the expanded snapshot dependency set before refreshing the entry points.

## Documentation, release entry points and registry observation

The local documentation-link checker and exact crates.io observation derive
from IcyDB's `scripts/ci/check-documentation.pl` and
`scripts/ci/publish-workspace.sh` at
`1f4737a9486fa2d0fb0f72927c92b15b6da4d0d4` (both source files clean).
The release-command checker consolidates the common Make adapter checks in
Canic, IC Timers, IcyDB and IC Metrics, including IC Metrics' nested Make/logger
isolation. These extractions add explicit inputs, retained smoke-check failures
and offline regression coverage. Product-specific documentation, receipt and
publication behavior remains with the consumers. See the
[maintained contracts](verification-helpers.md) for scope and adoption.

Portable digest generation extends the existing checksum verifier, informed by
IcyDB's `wasm_report_sha256` in `scripts/ci/wasm-report-common.sh` at the same
revision. Shared Tooling's IC receipt writer and snapshot refresh now use that
owner; consumer Wasm schemas and stream/tree hashes remain local.

RustSec preparation extracts the isolation mechanics from Canic's
`scripts/ci/check-dependency-risk-inventory.sh` at
`e1a211a00f01568ccc99bedc494c62a7141444dd` and IC Query's
`scripts/ci/check-dependencies.sh` at
`af8d50235b926a6a10d8bf37fba87ea574867a92`. The helper adds explicit source and
destination selection, recorded commit identity and retained preparation
diagnostics. It does not adopt either consumer's advisory acceptance policy or
claim that either consumer has migrated its audit invocation.

## Lockfile versions, formatting adoption and ripgrep

The lockfile transformer consolidates Canic's `retain-lock-selection.pl` at
`e1a211a00f01568ccc99bedc494c62a7141444dd`, IC Timers' `update-local-lock.sh` at
`98c4b296d7461525c15a01e30adbe33b75bcfa38`, and IC Blob Storage's release-data
transformation inspected at `ee7eed5` before concurrent adoption work advanced
that checkout. Consumer package discovery and recovery remain local.

The formatting adoption checker shares the mechanical cases inspected in IC
Host Tools, IC Metrics, IC Backup and IC Blob Storage. Those inspections included
working-tree adoption changes; they are not attributed wholesale to committed
releases. Consumer formatter inputs remain explicit rather than promoted into a
universal workspace layout.

Ripgrep's archive matrix comes from Toko Miner `061cfb6e3702a7075ab3c118bfaf315d7d2b0053`.
The four SHA-256 values were checked against official 15.2.0 release metadata.
Canic's working-tree installer at base `e1a211a` supplies the PCRE2 requirement.
Shared host setup reuses its existing activation/retention owner instead of
copying Toko's standalone installation flow. These implementations do not imply
that the consumers have adopted their committed snapshots.

## Explicit tag maintenance

The Perl helper replaces the duplicated mechanics reviewed in Canic's
`scripts/dev/delete-github-tags-up-to.sh` at `e1a211a00f01568ccc99bedc494c62a7141444dd`
and IcyDB's matching script at `1f4737a9486fa2d0fb0f72927c92b15b6da4d0d4`, recorded
in [#11](https://github.com/dragginzgame/shared-tooling/issues/11). Both source
scripts remained locally unchanged when re-inspected. The shared implementation
requires an explicit cutoff, retains saved object identities for retries, and
delegates conditional updates to Git. It does not copy either product's cutoff
defaults or promote their broad tag-deletion commands into the release flow.
