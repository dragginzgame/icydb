# Canic's Dragginzgame dependency audit — 2026-10-03

Targeted source review covered all five externally packaged Dragginzgame
dependencies selected by Canic, plus the originally requested ic-blob-storage
extraction. Four new GitHub issues were published, and two existing issues
received downstream evidence. Existing upstream issues were searched before
posting. This is a boundary audit, not exhaustive certification of these projects.

## Scope and dependency selection

Canic baseline: `97d0cbcb3f1ad9f60e69212341c7fdf57863449f`, workspace 0.110.51.
Selections below came from its root manifest and current lockfile. Internal
Canic workspace packages and third-party dependencies were outside the requested
upstream scope. No additional ichelper/shared-tooling dependency was found in
the inspected manifests and build scripts.

| Repository | Canic requirement | Locked version | Reviewed upstream baseline |
| --- | --- | --- | --- |
| ic-memory | `0.15.3` | 0.15.3 | 0.15.6, `77d228547081904285ee1839e6fdb4ff15610ba3` |
| ic-query | `0.44.2`, subnet-catalog-host | 0.44.2 | 0.44.2, `da4001e120ee4418027c0e811b8230195e7a8946` |
| ic-timers | **`=0.8.1`** | 0.8.1 | 0.8.3, `a0a19c781bf9385e363d2780bdfdabdab2f90ff5` |
| ic-testkit | `0.10.4` | 0.10.4 | `092897efcfc2c3bfc64b94e9e706ca0321b6f232`, then `b998f0c0a076209a892e5bbf160fb97ebdc3314e` |
| icydb | `0.264`, no default features | 0.264.3 | 0.264.4, `9e97fcf6d43b8bf0c2c457eb8c608d145d4e6995` |
| ic-blob-storage | Planned extraction; no Cargo dependency yet | — | 0.9.0, `d4a8759bfa9660ab1362711ec2dec99176b8af89` |

Several upstream worktrees contained concurrent changes. ic-testkit's HEAD
advanced during the review; its newer API hardening is not assumed to be the
registry 0.10.4 implementation. Upstream releases and Canic's locked packages
are separate evidence boundaries. No locked-dependency reproduction is claimed
where only current upstream source was inspected.

## Published findings

| Feedback | Finding | Evidence and status |
| --- | --- | --- |
| [ic-blob-storage #2](https://github.com/dragginzgame/ic-blob-storage/issues/2) | Impossible transfer budgets reach the signed certificate request. | Executed offline with the maintained patched SDK, an in-memory intent journal and refusing transport. Three budgets each caused one claim, one certificate-fetch attempt and an uncertain intent; zero provider requests. |
| [ic-blob-storage #3](https://github.com/dragginzgame/ic-blob-storage/issues/3) | CLI regular-file validation occurs after a blocking FIFO open. | Executed no-writer FIFO cursor reproduction; timeout exit 124 and no output. Directory control returned typed file error, exit 3. The existing binary's exact reviewed-commit binding was not independently established. LocalBody had the same source ordering but was not executed. A concurrent local repair appeared before handoff; see below. |
| [ic-timers #7](https://github.com/dragginzgame/ic-timers/issues/7) | Rejected overflowing initial Watchdog scheduling mutates snapshot observations. | Source-traced at the committed 0.8.3 baseline. The local worktree already contains the repair and regression; it is not treated as released or tested by this audit. |
| [ic-query #1](https://github.com/dragginzgame/ic-query/issues/1) | Cached subnet catalogs are read without a byte ceiling before UTF-8/JSON decoding. | Source-traced through the cache loader and unbounded confined reader, confirmed against GitHub source. Existing bounded IO helpers provide the narrow repair route. No OOM or oversized-file execution. |

For blob publication, the valid 1,024-byte body needs a tree PUT and a chunk PUT.
The tested budgets were: one request; one byte per request; and one total byte.
The offline fetch refuses the certificate request. Successful certificate
issuance, provider transfers, IndexedDB behavior and canister reservation
effects were not executed. Avoid promoting those possible consequences to
observed facts, and preserve conservative uncertainty for genuinely attempted
requests. Budget validation belongs before the certificate/claim boundary.

For the FIFO issue, both the shared native reader and LocalBody open first,
then inspect descriptor metadata. A path-only precheck leaves a replacement
race. The requested outcome is nonblocking open followed by validation of the
same descriptor. The two-second timeout is solely a deadlock escape, not a
performance measurement. No identity or network acquisition was reached.

Before handoff, the local blob worktree acquired a shared `open_regular` repair
using Unix `O_NONBLOCK` and same-descriptor validation, called by both readers.
Added an issue comment recording that uncommitted repair. It was not built or
tested here; the saved native source hashes describe the originally reviewed
committed files, not the subsequently edited worktree. The existing CLI binary
remains separate evidence.

## Existing issues and coverage

- **ic-memory:** reviewed fresh/reopened runtime construction, default bootstrap
  and Canic's admission handoff. [Existing #9](https://github.com/dragginzgame/ic-memory/issues/9)
  already reproduces constructor panic on backing-growth refusal. Added a
  [Canic-specific comment](https://github.com/dragginzgame/ic-memory/issues/9#issuecomment-5967564732)
  tracing default bootstrap into that constructor and its typed error owner.
  The original refusal probe was not rerun here.
- **ic-testkit:** reviewed retained-artifact consumption and dead-PocketIC
  transport classification. Added [consumer evidence to #2](https://github.com/dragginzgame/ic-testkit/issues/2#issuecomment-5967565650):
  Canic retains the ArtifactCacheOutcome while reading the named immutable
  artifact. [Existing #3](https://github.com/dragginzgame/ic-testkit/issues/3)
  tracks the earlier broad-substring classifier defect; current reviewed
  source keeps narrower instance-URL/transport-source recognition and its
  documented heuristic limit. No duplicate defect or new API request.
- **icydb:** reviewed Canic's memory-admission integration, dependency adoption
  and the existing build-flag owner. [Existing #17](https://github.com/dragginzgame/icydb/issues/17)
  already records inherited encoded Rust flags overriding release remaps.
  The source gap remains; it belongs to IcyDB's test/build harness, rather
  than Canic's production use of the IcyDB library. No duplicate issue.
- **ic-query:** reviewed cache/read-through policy, snapshot assurance versus
  freshness and export alias protection. Canic projects validated snapshot
  evidence and evaluates freshness separately; no additional authority defect
  was established. The unbounded catalog read is recorded above.
- **ic-timers:** reviewed request validation, snapshot continuity and Canic's
  exact pin. Existing maintenance/docs feedback in
  [#6](https://github.com/dragginzgame/ic-timers/issues/6) was not duplicated.
- **ic-blob-storage:** reviewed browser publication admission, gateway budgets,
  intent uncertainty and native body/cursor file admission. Canic's existing
  2026-09-29 local feedback already noted FIFO opening; publishing #3 makes
  that actionable upstream. The stale leading version in the status handoff
  was corrected concurrently and was not filed as an outstanding defect.

## Validation and saved evidence

Executed checks passed:

- Three offline SDK budget reproductions, including intent and transport
  counters; [saved results](artifacts/budget-result.json).
- FIFO reproduction and directory control; [saved results](artifacts/fifo-result.json).
- The maintained browser publication frozen-input controls, covering 15 refusal,
  cancellation and input-snapshot cases; [saved results](artifacts/publication-controls-result.json).
- Read-only reverse-application check of the maintained Caffeine 1.1.2 SDK patch.
- Read-back of all four published issues and both added comments.

[The reproduction script](artifacts/reproduce.sh) uses existing upstream SDK
dependencies and an existing CLI binary. Its [bundler](artifacts/bundle.mjs)
and [budget fixture](artifacts/budget-reproduction.mjs) generate results only
in a new caller-selected output directory. Example using this audit's local
tools:

```bash
bash /home/adam/projects/icydb/docs/reports/investigations/2026/10/03/canic-dependencies/01/artifacts/reproduce.sh \
  /home/adam/projects/ic-blob-storage \
  /tmp/canic-dependency-audit-new-output \
  /home/adam/projects/ic-blob-storage/.tmp/publish-prepare-01/local-artifacts/blob-storage \
  /home/adam/projects/ic-blob-storage/.tmp/tools/node-v24.21.0/bin/node
```

Source SHA-256 fingerprints at the original blob review boundary:

| Input | SHA-256 |
| --- | --- |
| Pre-existing CLI binary | `4c11872339c68d96e158f2a150d682fca50c07950cada769bc80de5a4debb572` |
| Browser publication.js | `f259ffc707b02454cb56488e68b3129bd490a2f326a29e08fb55c966fa60c493` |
| Browser gateway.js | `8d1ccb75c576be4585c20cb8cf73ca7171dbba04872454d4f2cd74a33e787370` |
| Browser intents.js | `59351bb1ccef06bf7d0ac36db12261432bcf4faa42137ff10941528ee1f61f57` |
| Native shared reader | `47f99b887e8a8ac81b0eb4addcd8db08bad1f0b3b69226cccdb32f39308ff9e9` |
| Native LocalBody | `72c5cd09712413916bd30be24e7cfd469fbfe1d9c1645973f8beaf8b99deb710` |
| Maintained SDK patch | `55fc44fe155ec3be888e1bee524ccb931b28c8c9e0119ffc012e270238289364` |

Initial audit-harness failures were corrected: default Node 18 lacked the needed
global crypto; the SDK wraps transport errors; bundled controls needed the
original import.meta.url for source fingerprinting. A discarded Node child-process
runner also failed under the local execution environment. The final shell runner
passed. Temporary diagnostics remain under `/tmp/icydb-blob-audit-20261003`;
they are audit setup failures, not additional upstream defects.

No Rust build, Cargo suite, timer test/lint gate, PocketIC deployment, live
Registry call, provider transfer or IC network lifecycle action was performed.
Full suites remain maintainer-owned. Wasm bytes, IC cycles and instructions are
unmeasured. No native timing benchmark was run.

## Delivery and follow-up

This change adds seven files, approximately 420 lines: the report and six small
reproduction/evidence files. It
changes no runtime implementation. Concurrent package/dependency version churn
and all unrelated dirty work were left untouched; no commit or push was run.

Upstream owners should validate and release the recorded repairs, then record
fix versions in the issues. Canic should assess adoption using its actual lock
selection, particularly the exact ic-timers pin. The source-only findings still
need focused upstream execution; no dependency upgrade is implied by this audit.
