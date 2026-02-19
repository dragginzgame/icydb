# IcyDB Core

Runtime engine for IcyDB.

`icydb-core` contains execution, planning, storage-facing runtime logic, and observability plumbing used by the public `icydb` crate.

Most applications should depend on `icydb` directly. Use `icydb-core` only when you explicitly need internal runtime surfaces.

Current execution baseline in `0.18.x` includes stream-native composite execution (`Union`/`Intersection`) and guarded pagination scan budgeting for safe plan shapes.

References:

- Workspace overview: `README.md`
- Execution design/status: `docs/design/0.18-composite-limit-pushdown.md`, `docs/status/0.18-status.md`
- Release notes: `CHANGELOG.md`
