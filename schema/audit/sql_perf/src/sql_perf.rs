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
    PerfAuditStore,
    canister = "PerfAuditCanister",
    storage(journaled(
        data_memory_id = 180,
        index_memory_id = 181,
        schema_memory_id = 182,
        journal_memory_id = 183,
    )),
);

#[store(canister = "PerfAuditCanister", storage(heap()))]
pub struct PerfAuditHeapStore {}

#[store(
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

#[entity(store = "PerfAuditStore",
    version = 1,
    pk(fields = ["id"]),
    index(fields = ["name"]),
    index(fields = ["age", "id"]),
    index(fields = ["LOWER(name)"]),
    fields(
        field(name = "id", value(item(prim = "Int32"))),
        field(name = "name", value(item(prim = "Text", unbounded))),
        field(name = "age", value(item(prim = "Int32"))),
        field(name = "age_nat", value(item(prim = "Nat32"))),
        field(name = "rank", value(item(prim = "Int32"))),
        field(name = "active", value(item(prim = "Bool")))
    ),
    timestamps
)]
pub struct PerfAuditUser {}

///
/// PerfAuditHeapUser
///
/// Heap mirror of the primary-key user perf shape. It exists only so the
/// integration harness can sample live volatile heap traversal beside the
/// journaled durable storage path.
///

#[entity(store = "PerfAuditHeapStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id", value(item(prim = "Int32"))),
        field(name = "name", value(item(prim = "Text", unbounded))),
        field(name = "age", value(item(prim = "Int32")))
    ),
    timestamps
)]
pub struct PerfAuditHeapUser {}

///
/// PerfAuditJournaledUser
///
/// Journaled mirror of the primary-key user perf shape. It exists only so the
/// integration harness can sample IC local instructions for the journaled
/// bounded-query path that previously regressed.
///

#[entity(store = "PerfAuditJournaledStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id", value(item(prim = "Int32"))),
        field(name = "name", value(item(prim = "Text", unbounded))),
        field(name = "age", value(item(prim = "Int32")))
    ),
    timestamps
)]
pub struct PerfAuditJournaledUser {}

///
/// PerfAuditMutationToken
///
/// Toko-shaped journaled token used only by the durable mutation-job scale
/// contract. The collection index preserves the production scope shape while
/// `tier` remains the fixed field converged by the first durable phase.
///

#[entity(store = "PerfAuditJournaledStore",
    version = 1,
    pk(fields = ["id"]),
    index(fields = ["collection_id"]),
    fields(
        field(name = "id", value(item(prim = "Int32"))),
        field(name = "collection_id", value(item(prim = "Int32"))),
        field(name = "tier", value(item(prim = "Text", unbounded)))
    ),
    timestamps
)]
pub struct PerfAuditMutationToken {}

///
/// PerfAuditMutationScoringState
///
/// Toko-shaped scoring state paired one-for-one with the token fixture. It is
/// a separate entity so both application phases can be made durable before
/// either phase begins executing.
///

#[entity(store = "PerfAuditJournaledStore",
    version = 1,
    pk(fields = ["id"]),
    index(fields = ["collection_id"]),
    fields(
        field(name = "id", value(item(prim = "Int32"))),
        field(name = "collection_id", value(item(prim = "Int32"))),
        field(name = "score_stale", value(item(prim = "Bool")))
    ),
    timestamps
)]
pub struct PerfAuditMutationScoringState {}

///
/// PerfAuditRelationTarget
///
/// Minimal relation target used only by the bounded integrity performance
/// evidence. Its primary-key domain is shared by deterministic source rows.
///

#[entity(store = "PerfAuditStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(name = "id", value(item(prim = "Int32")))),
    timestamps
)]
pub struct PerfAuditRelationTarget {}

///
/// PerfAuditRelationSource
///
/// Minimal relation source used to exercise accepted target validation and
/// active reverse-relation verification without a second audit canister.
///

#[entity(store = "PerfAuditStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id", value(item(prim = "Int32"))),
        field(name = "target_id",
            value(item(rel = "PerfAuditRelationTarget", prim = "Int32"))
        )
    ),
    timestamps
)]
pub struct PerfAuditRelationSource {}

/// Three-edge accepted relation fixture for the frozen direct-DTO SQL
/// introspection response gate. It intentionally has no data rows because the
/// command projects accepted catalog authority only.
#[entity(store = "PerfAuditStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id", value(item(prim = "Int32"))),
        field(name = "primary_target_id",
            value(item(rel = "PerfAuditRelationTarget", prim = "Int32"))
        ),
        field(name = "secondary_target_id",
            value(item(rel = "PerfAuditRelationTarget", prim = "Int32"))
        ),
        field(name = "tertiary_target_id",
            value(item(rel = "PerfAuditRelationTarget", prim = "Int32"))
        )
    ),
    timestamps
)]
pub struct PerfAuditIntrospectionRelations {}

///
/// PerfAuditBlob
///
/// Blob-shaped perf fixture with a scalar metadata covering index beside
/// thumbnail and chunk payloads so SQL perf scenarios can compare metadata-only,
/// byte-length-only, and payload-returning projections.
///

#[entity(store = "PerfAuditStore",
    version = 1,
    pk(fields = ["id"]),
    index(fields = ["bucket", "label", "id"]),
    index(fields = ["label"]),
    fields(
        field(name = "id", value(item(prim = "Int32"))),
        field(name = "label", value(item(prim = "Text", unbounded))),
        field(name = "bucket", value(item(prim = "Int32"))),
        field(name = "thumbnail", value(item(prim = "Blob", unbounded))),
        field(name = "chunk", value(item(prim = "Blob", unbounded)))
    ),
    timestamps
)]
pub struct PerfAuditBlob {}

///
/// PerfAuditAccount
///
/// Account-shaped perf fixture with filtered raw and casefolded indexes for
/// active-only and active-plus-tier windows over canonicalized handles.
///

#[entity(store = "PerfAuditStore",
    version = 1,
    pk(fields = ["id"]),
    index(fields = ["handle"], predicate = "active = true"),
    index(fields = ["LOWER(handle)"], predicate = "active = true"),
    index(fields = ["tier", "handle"], predicate = "active = true"),
    index(fields = ["tier", "LOWER(handle)"], predicate = "active = true"),
    fields(
        field(name = "id", value(item(prim = "Int32"))),
        field(name = "handle", value(item(prim = "Text", unbounded))),
        field(name = "tier", value(item(prim = "Text", unbounded))),
        field(name = "active", value(item(prim = "Bool"))),
        field(name = "score", value(item(prim = "Int32")))
    ),
    timestamps
)]
pub struct PerfAuditAccount {}

///
/// PerfAuditToken
///
/// Token-shaped perf fixture for production list/page queries that filter a
/// fixed collection, branch over a small stage set, and globally order by id.
///

#[entity(store = "PerfAuditStore",
    version = 1,
    pk(fields = ["id"]),
    index(fields = ["collection_id", "stage", "id"]),
    fields(
        field(name = "id", value(item(prim = "Ulid"))),
        field(name = "collection_id", value(item(prim = "Text", unbounded))),
        field(name = "stage", value(item(prim = "Text", unbounded))),
        field(name = "title", value(item(prim = "Text", unbounded)))
    ),
    timestamps
)]
pub struct PerfAuditToken {}

///
/// PerfAuditStreamingRow
///
/// Deterministic 0.222 executor fixture with two independently selective
/// indexes, their compound control, noncontiguous groups, incompatible order,
/// and bounded wide payload sentinels.
///

#[entity(store = "PerfAuditStore",
    version = 1,
    pk(fields = ["id"]),
    index(fields = ["lane_a", "id"]),
    index(fields = ["lane_b", "id"]),
    index(fields = ["group_key", "id"]),
    fields(
        field(name = "id", value(item(prim = "Int32"))),
        field(name = "lane_a", value(item(prim = "Int32"))),
        field(name = "lane_b", value(item(prim = "Int32"))),
        field(name = "group_key", value(item(prim = "Int32"))),
        field(name = "sort_key", value(item(prim = "Int32"))),
        field(name = "label", value(item(prim = "Text", unbounded))),
        field(name = "payload", value(item(prim = "Blob", unbounded)))
    ),
    timestamps
)]
pub struct PerfAuditStreamingRow {}

/// Compound-index control with the same deterministic rows as
/// `PerfAuditStreamingRow`.
#[entity(store = "PerfAuditStore",
    version = 1,
    pk(fields = ["id"]),
    index(fields = ["lane_a", "lane_b", "id"]),
    fields(
        field(name = "id", value(item(prim = "Int32"))),
        field(name = "lane_a", value(item(prim = "Int32"))),
        field(name = "lane_b", value(item(prim = "Int32"))),
        field(name = "group_key", value(item(prim = "Int32"))),
        field(name = "sort_key", value(item(prim = "Int32"))),
        field(name = "label", value(item(prim = "Text", unbounded))),
        field(name = "payload", value(item(prim = "Blob", unbounded)))
    ),
    timestamps
)]
pub struct PerfAuditStreamingCompoundRow {}
