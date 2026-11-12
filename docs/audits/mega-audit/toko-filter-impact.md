# Toko filter contract impact

Read-only consumer review on 2026-11-12 for the 0.310 InputValue cut. Toko was
not edited, built, upgraded or deployed. Counts are source-search results, not
runtime traffic measurements, and exclude ignored files.

Toko's root Cargo.toml selects IcyDB and IcyDB Model from Git tag v0.258.0.
The local 0.310 change therefore does not affect its current dependency selection.
Its eventual migration spans the intervening releases, not only this literal cut.

## Observed surface

| Symbol | Authored Rust | Authored TypeScript | Generated TypeScript |
| --- | --- | --- | --- |
| FilterValue | 11 matching lines in 2 files | 9 matching lines in 1 file | 20 matching lines in 5 files |
| FilterExpr | 787 matching lines in 72 files | 175 matching lines in 29 files | 46 matching lines in 5 files |
| DynamicQuery | 306 matching lines in 38 files | None found | None found |

Counts use whole-word matches in .rs, .ts and .tsx files. They include imports,
types, comments and tests, so they must not be presented as call counts. Ten
additional DID/JavaScript declaration files mention FilterValue.

## Concrete migration points

Paths below are relative to the Toko repository, not IcyDB.

- `fleets/toko/project/instance/src/ops/token/read.rs`: the positive-u32 parser
  and collection-ID parser directly match FilterValue::String and a singleton
  FilterValue::List. Migrate to InputValue/PublicValue and explicitly admit
  intended typed numeric/ID values rather than assuming every literal is text.
- `fleets/toko/user/shard/src/ops/user.rs`: the equality matcher accepts textual
  project principals and status variants. Its policy needs explicit handling of
  typed Principal/Enum inputs as well as any deliberately retained text policy.
- `frontend/src/lib/utils/filter.utils.ts`: seven comparison/membership helper
  signatures use FilterValue. Callers construct `{ String: ... }`; the stack
  family helper stringifies a nat16, and search helpers also stringify numbers.
  Regenerate the client contract and use its maintained typed literal cases.
- Generated declarations for project instance, project ledger, user shard,
  project generator and project registry expose the retired union. Regenerate
  these from the adopted endpoint contract; do not hand-edit generated output.

Most Rust FilterExpr builder call sites need no spelling change because they
already pass typed values. This is a source inspection conclusion, not a claim
that Toko compiles or is behaviorally qualified against 0.310. Endpoint clients
and external serialized requests must move together at adoption. No legacy
FilterValue decoder should be restored in IcyDB to bridge that deployment.
