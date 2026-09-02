use icydb_model::prelude::*;
use icydb_testing_wasm_helpers::{define_fixture_canister, define_fixture_store};

define_fixture_canister!(
    GroupPathAuditCanister = "GroupPathAuditCanister",
    namespace = "group_path_audit",
    memory_min = 100,
    memory_max = 106,
    commit_memory_id = 104,
    startup_memory_id = 106,
    integrity_progress_memory_id = 105,
);

define_fixture_store!(
    GroupPathAuditStore,
    canister = "GroupPathAuditCanister",
    storage(journaled(
        data_memory_id = 100,
        index_memory_id = 101,
        schema_memory_id = 102,
        journal_memory_id = 103,
    )),
);

/// Named single-valued record used by the scalar record-path comparison actor.
#[record(fields(
    field(name = "rank", value(item(prim = "Int32"))),
    field(name = "optional_rank", value(opt, item(prim = "Int32")))
))]
pub struct GroupPathAuditProfile {}

/// Frozen direct-versus-record-path comparison row.
#[entity(
    store = "GroupPathAuditStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id", value(item(prim = "Int32"))),
        field(name = "direct_rank", value(item(prim = "Int32"))),
        field(name = "profile", value(item(is = "GroupPathAuditProfile"))),
        field(
            name = "optional_profile",
            value(opt, item(is = "GroupPathAuditProfile"))
        )
    )
)]
pub struct GroupPathAuditRow {}
