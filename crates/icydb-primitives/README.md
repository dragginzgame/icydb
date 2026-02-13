# icydb-primitives

Shared primitive metadata and scalar classification for IcyDB.

This crate defines the canonical scalar registry used across schema and runtime layers, including:
- `ScalarKind`
- `ScalarMetadata`
- `ScalarCoercionFamily`
- `ALL_SCALAR_KINDS`

Use this crate when tooling or internal components need stable scalar capability metadata.
