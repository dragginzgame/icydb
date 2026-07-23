use icydb::design::prelude::*;
use icydb_testing_wasm_helpers::{define_fixture_canister, define_fixture_store};

define_fixture_canister!(
    PerfAuditCanister = "PerfAuditCanister",
    namespace = "sql_perf",
    memory_min = 180,
    memory_max = 189,
    commit_memory_id = 188,
);

define_fixture_store!(
    PerfAuditStore = "PERF_AUDIT_STORE",
    canister = "PerfAuditCanister",
    storage(journaled(
        data_memory_id = 180,
        index_memory_id = 181,
        schema_memory_id = 182,
        journal_memory_id = 183,
    )),
);

#[store(
    ident = "PERF_AUDIT_HEAP_STORE",
    store_name = "heap",
    canister = "PerfAuditCanister",
    storage(heap())
)]
pub struct PerfAuditHeapStore {}

#[store(
    ident = "PERF_AUDIT_JOURNALED_STORE",
    store_name = "journaled",
    canister = "PerfAuditCanister",
    storage(journaled(
        data_memory_id = 184,
        index_memory_id = 185,
        schema_memory_id = 186,
        journal_memory_id = 187,
    ))
)]
pub struct PerfAuditJournaledStore {}

///
/// PerfAuditUser
///
/// User-shaped perf fixture with equality, ordered-range, and casefold
/// expression indexes.
///

#[entity(
    store = "PerfAuditStore",
    version = 1,
    pk(fields = ["id"]),
    index(fields = ["name"]),
    index(fields = ["age", "id"]),
    index(fields = ["LOWER(name)"]),
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
/// PerfAuditHeapUser
///
/// Heap mirror of the primary-key user perf shape. It exists only so the
/// integration harness can sample live volatile heap traversal beside the
/// journaled durable storage path.
///

#[entity(
    store = "PerfAuditHeapStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(ident = "id", value(item(prim = "Int32"))),
        field(ident = "name", value(item(prim = "Text", unbounded))),
        field(ident = "age", value(item(prim = "Int32")))
    )
)]
pub struct PerfAuditHeapUser {}

///
/// PerfAuditJournaledUser
///
/// Journaled mirror of the primary-key user perf shape. It exists only so the
/// integration harness can sample IC local instructions for the journaled
/// bounded-query path that previously regressed.
///

#[entity(
    store = "PerfAuditJournaledStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(ident = "id", value(item(prim = "Int32"))),
        field(ident = "name", value(item(prim = "Text", unbounded))),
        field(ident = "age", value(item(prim = "Int32")))
    )
)]
pub struct PerfAuditJournaledUser {}

///
/// PerfAuditBlob
///
/// Blob-shaped perf fixture with a scalar metadata covering index beside
/// thumbnail and chunk payloads so SQL perf scenarios can compare metadata-only,
/// byte-length-only, and payload-returning projections.
///

#[entity(
    store = "PerfAuditStore",
    version = 2,
    pk(fields = ["id"]),
    index(fields = ["bucket", "label", "id"]),
    index(fields = ["label"]),
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
    version = 1,
    pk(fields = ["id"]),
    index(fields = ["handle"], predicate = "active = true"),
    index(fields = ["LOWER(handle)"], predicate = "active = true"),
    index(fields = ["tier", "handle"], predicate = "active = true"),
    index(fields = ["tier", "LOWER(handle)"], predicate = "active = true"),
    fields(
        field(ident = "id", value(item(prim = "Int32"))),
        field(ident = "handle", value(item(prim = "Text", unbounded))),
        field(ident = "tier", value(item(prim = "Text", unbounded))),
        field(ident = "active", value(item(prim = "Bool"))),
        field(ident = "score", value(item(prim = "Int32")))
    )
)]
pub struct PerfAuditAccount {}

///
/// PerfAuditToken
///
/// Token-shaped perf fixture for production list/page queries that filter a
/// fixed collection, branch over a small stage set, and globally order by id.
///

#[entity(
    store = "PerfAuditStore",
    version = 1,
    pk(fields = ["id"]),
    index(fields = ["collection_id", "stage", "id"]),
    fields(
        field(ident = "id", value(item(prim = "Ulid"))),
        field(ident = "collection_id", value(item(prim = "Text", unbounded))),
        field(ident = "stage", value(item(prim = "Text", unbounded))),
        field(ident = "title", value(item(prim = "Text", unbounded)))
    )
)]
pub struct PerfAuditToken {}
