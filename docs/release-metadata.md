# Release metadata preparation

Cargo candidate rendering belongs to IC Build. IcyDB's preparation and receipt
checks call the same renderer; its own exact internal-pin policy, source binding,
notes, validation and rollback remain local. README examples are ordinary source
documentation and are not rewritten during a release.

This contribution depends on publication of
[IC Build PR #56 — Cargo release candidate editing](https://github.com/dragginzgame/ic-build/pull/56).
Keep it draft until a released editor and its exact prepared tool selection have
been qualified. A locally built PR executable supplies development evidence only.
There is no mutable sibling fallback, implicit Cargo installation or network
retry in rendering.

Set `IC_BUILD_RELEASE_METADATA` to the absolute, explicitly prepared and qualified
`ic-build-release-metadata` executable. `check-versioning` and release preflight
check that selection before the full gate. Each source document has an explicit
8 MiB input ceiling. Failed projections retain their inputs for inspection and
never authorize a receipt or partially generated candidate.

Complete interrupted releases with their original qualified tooling before
adopting this flow. Keep existing receipts, source snapshots and package archives;
do not rewrite old evidence to fit the new renderer.
