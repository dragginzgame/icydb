> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

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

## Delivery and follow-up

This change adds seven files, approximately 420 lines: the report and six small
reproduction/evidence files. It
changes no runtime implementation. Concurrent package/dependency version churn
and all unrelated dirty work were left untouched; no commit or push was run.

Upstream owners should validate and release the recorded repairs, then record
fix versions in the issues. Canic should assess adoption using its actual lock
selection, particularly the exact ic-timers pin. The source-only findings still
need focused upstream execution; no dependency upgrade is implied by this audit.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/10/03/canic-dependencies/01/report.md).
