# IcyDB Model

Application-model authoring, validation, and code generation for IcyDB.

This package owns the host-side declaration graph, reusable application types,
validators and normalizers, and lowering into the bounded `icydb-schema`
proposal contract. It does not own accepted schema, storage, planning,
execution, or recovery authority.

Schema-only consumers depend on this package without depending on the IcyDB
database runtime. Generated typed adapters are explicit opt-in output for
consumers that also depend directly on `icydb`.

References:

- Workspace overview: `../../README.md`
- Design:
  `../../docs/design/0.213-schema-authority-and-application-model-separation/0.213-design.md`
- Release notes: `../../CHANGELOG.md`
