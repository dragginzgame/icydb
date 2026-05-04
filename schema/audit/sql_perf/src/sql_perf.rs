use icydb::design::prelude::*;

///
/// PerfAuditCanister
///
/// Dedicated SQL perf audit canister model used only for instruction-sampling
/// and access-shape coverage.
///

#[canister(memory_min = 180, memory_max = 210, commit_memory_id = 182)]
pub struct PerfAuditCanister {}

///
/// PerfAuditStore
///
/// Shared store for the dedicated SQL perf audit entities and index layouts.
///

#[store(
    ident = "PERF_AUDIT_STORE",
    canister = "PerfAuditCanister",
    data_memory_id = 180,
    index_memory_id = 181,
    schema_memory_id = 183
)]
pub struct PerfAuditStore {}

///
/// PerfAuditUser
///
/// User-shaped perf fixture with equality, ordered-range, and casefold
/// expression indexes.
///

#[entity(
    store = "PerfAuditStore",
    pk(field = "id"),
    index(fields = "name"),
    index(fields = "age, id"),
    index(fields = "LOWER(name)"),
    fields(
        field(ident = "id", value(item(prim = "Int32"))),
        field(ident = "name", value(item(prim = "Text", unbounded))),
        field(ident = "age", value(item(prim = "Int32"))),
        field(ident = "age_nat", value(item(prim = "Nat32"))),
        field(ident = "rank", value(item(prim = "Int32"))),
        field(ident = "active", value(item(prim = "Bool")))
    )
)]
pub struct PerfAuditUser {}

///
/// PerfAuditBlob
///
/// Blob-shaped perf fixture with scalar metadata indexes beside thumbnail and
/// chunk payloads so SQL perf scenarios can compare payload-returning queries
/// with byte-length-only projections.
///

#[entity(
    store = "PerfAuditStore",
    pk(field = "id"),
    index(fields = "bucket, id"),
    index(fields = "label"),
    fields(
        field(ident = "id", value(item(prim = "Int32"))),
        field(ident = "label", value(item(prim = "Text", unbounded))),
        field(ident = "bucket", value(item(prim = "Int32"))),
        field(ident = "thumbnail", value(item(prim = "Blob", unbounded))),
        field(ident = "chunk", value(item(prim = "Blob", unbounded)))
    )
)]
pub struct PerfAuditBlob {}

///
/// PerfAuditAccount
///
/// Account-shaped perf fixture with filtered raw and casefolded indexes for
/// active-only and active-plus-tier windows over canonicalized handles.
///

#[entity(
    store = "PerfAuditStore",
    pk(field = "id"),
    index(fields = "handle", predicate = "active = true"),
    index(fields = "LOWER(handle)", predicate = "active = true"),
    index(fields = "tier, handle", predicate = "active = true"),
    index(fields = "tier, LOWER(handle)", predicate = "active = true"),
    fields(
        field(ident = "id", value(item(prim = "Int32"))),
        field(ident = "handle", value(item(prim = "Text", unbounded))),
        field(ident = "tier", value(item(prim = "Text", unbounded))),
        field(ident = "active", value(item(prim = "Bool"))),
        field(ident = "score", value(item(prim = "Int32")))
    )
)]
pub struct PerfAuditAccount {}
