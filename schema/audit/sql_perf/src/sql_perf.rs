use icydb_model::prelude::*;
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

#[entity(source_key = "schema/audit/sql_perf/src/sql_perf.rs::entity::1",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "PerfAuditStore",
    version = 1,
    pk(fields = ["id"]),
    index(source_key = "index.1", fields = ["name"]),
    index(source_key = "index.2", fields = ["age", "id"]),
    index(source_key = "index.3", fields = ["LOWER(name)"]),
    fields(
        field(source_key = "id", ident = "id", value(item(prim = "Int32"))),
        field(source_key = "name", ident = "name", value(item(prim = "Text", unbounded))),
        field(source_key = "age", ident = "age", value(item(prim = "Int32"))),
        field(source_key = "age_nat", ident = "age_nat", value(item(prim = "Nat32"))),
        field(source_key = "rank", ident = "rank", value(item(prim = "Int32"))),
        field(source_key = "active", ident = "active", value(item(prim = "Bool")))
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

#[entity(source_key = "schema/audit/sql_perf/src/sql_perf.rs::entity::2",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "PerfAuditHeapStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(source_key = "id", ident = "id", value(item(prim = "Int32"))),
        field(source_key = "name", ident = "name", value(item(prim = "Text", unbounded))),
        field(source_key = "age", ident = "age", value(item(prim = "Int32")))
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

#[entity(source_key = "schema/audit/sql_perf/src/sql_perf.rs::entity::3",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "PerfAuditJournaledStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(source_key = "id", ident = "id", value(item(prim = "Int32"))),
        field(source_key = "name", ident = "name", value(item(prim = "Text", unbounded))),
        field(source_key = "age", ident = "age", value(item(prim = "Int32")))
    )
)]
pub struct PerfAuditJournaledUser {}

///
/// PerfAuditRelationTarget
///
/// Minimal relation target used only by the bounded integrity performance
/// evidence. Its primary-key domain is shared by deterministic source rows.
///

#[entity(source_key = "schema/audit/sql_perf/src/sql_perf.rs::entity::4",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "PerfAuditStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(source_key = "id", ident = "id", value(item(prim = "Int32"))))
)]
pub struct PerfAuditRelationTarget {}

///
/// PerfAuditRelationSource
///
/// Minimal relation source used to exercise accepted target validation and
/// active reverse-relation verification without a second audit canister.
///

#[entity(source_key = "schema/audit/sql_perf/src/sql_perf.rs::entity::5",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "PerfAuditStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(source_key = "id", ident = "id", value(item(prim = "Int32"))),
        field(source_key = "target_id", ident = "target_id",
            value(item(rel = "PerfAuditRelationTarget", prim = "Int32"))
        )
    )
)]
pub struct PerfAuditRelationSource {}

///
/// PerfAuditBlob
///
/// Blob-shaped perf fixture with a scalar metadata covering index beside
/// thumbnail and chunk payloads so SQL perf scenarios can compare metadata-only,
/// byte-length-only, and payload-returning projections.
///

#[entity(source_key = "schema/audit/sql_perf/src/sql_perf.rs::entity::6",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "PerfAuditStore",
    version = 2,
    pk(fields = ["id"]),
    index(source_key = "index.4", fields = ["bucket", "label", "id"]),
    index(source_key = "index.5", fields = ["label"]),
    fields(
        field(source_key = "id", ident = "id", value(item(prim = "Int32"))),
        field(source_key = "label", ident = "label", value(item(prim = "Text", unbounded))),
        field(source_key = "bucket", ident = "bucket", value(item(prim = "Int32"))),
        field(source_key = "thumbnail", ident = "thumbnail", value(item(prim = "Blob", unbounded))),
        field(source_key = "chunk", ident = "chunk", value(item(prim = "Blob", unbounded)))
    )
)]
pub struct PerfAuditBlob {}

///
/// PerfAuditAccount
///
/// Account-shaped perf fixture with filtered raw and casefolded indexes for
/// active-only and active-plus-tier windows over canonicalized handles.
///

#[entity(source_key = "schema/audit/sql_perf/src/sql_perf.rs::entity::7",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "PerfAuditStore",
    version = 1,
    pk(fields = ["id"]),
    index(source_key = "index.6", fields = ["handle"], predicate = "active = true"),
    index(source_key = "index.7", fields = ["LOWER(handle)"], predicate = "active = true"),
    index(source_key = "index.8", fields = ["tier", "handle"], predicate = "active = true"),
    index(source_key = "index.9", fields = ["tier", "LOWER(handle)"], predicate = "active = true"),
    fields(
        field(source_key = "id", ident = "id", value(item(prim = "Int32"))),
        field(source_key = "handle", ident = "handle", value(item(prim = "Text", unbounded))),
        field(source_key = "tier", ident = "tier", value(item(prim = "Text", unbounded))),
        field(source_key = "active", ident = "active", value(item(prim = "Bool"))),
        field(source_key = "score", ident = "score", value(item(prim = "Int32")))
    )
)]
pub struct PerfAuditAccount {}

///
/// PerfAuditToken
///
/// Token-shaped perf fixture for production list/page queries that filter a
/// fixed collection, branch over a small stage set, and globally order by id.
///

#[entity(source_key = "schema/audit/sql_perf/src/sql_perf.rs::entity::8",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "PerfAuditStore",
    version = 1,
    pk(fields = ["id"]),
    index(source_key = "index.10", fields = ["collection_id", "stage", "id"]),
    fields(
        field(source_key = "id", ident = "id", value(item(prim = "Ulid"))),
        field(source_key = "collection_id", ident = "collection_id", value(item(prim = "Text", unbounded))),
        field(source_key = "stage", ident = "stage", value(item(prim = "Text", unbounded))),
        field(source_key = "title", ident = "title", value(item(prim = "Text", unbounded)))
    )
)]
pub struct PerfAuditToken {}
