# IcyDB Build

Build-time helpers for generated canister wiring, query/export surfaces, and metrics endpoints.

In the current `0.77` line, this crate generates store/session wiring and actor
support code, but it does not generate public canister SQL routing glue.
Canister-owned SQL query helpers remain explicit code in the consuming canister
crate when that facade is needed.

This crate is usually consumed transitively through `icydb` and is published to support downstream dependency resolution.

References:

- Workspace overview: `README.md`
- Release notes: `CHANGELOG.md`
