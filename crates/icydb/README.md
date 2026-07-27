# IcyDB

Public runtime facade for IcyDB.

This crate exposes accepted-schema database sessions, structural writes,
optional SQL and typed reads, shared runtime types, and the host build facade
used by generated canister actor glue.

Application model declarations and macros are owned by `icydb-model` and are
not re-exported here. Schema-proposal vocabulary and canonical scalar metadata
live in `icydb-schema`.

Runtime canister crates normally depend on `icydb`; schema-authoring crates
normally depend on `icydb-model`.

For full setup, examples, and release notes:

- Workspace README: `../../README.md`
- Changelog: `../../CHANGELOG.md`
