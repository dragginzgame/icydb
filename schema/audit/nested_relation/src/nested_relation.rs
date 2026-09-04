use icydb_model::prelude::*;
use icydb_testing_wasm_helpers::{define_fixture_canister, define_fixture_store};

macro_rules! define_relation_cost_actor {
    ($canister:ident = $canister_name:literal, $namespace:literal) => {
        define_fixture_canister!(
            $canister = $canister_name,
            namespace = $namespace,
            memory_min = 100,
            memory_max = 106,
            commit_memory_id = 104,
            startup_memory_id = 106,
            integrity_progress_memory_id = 105,
        );

        define_fixture_store!(
            RelationCostStore,
            canister = $canister_name,
            storage(journaled(
                data_memory_id = 100,
                index_memory_id = 101,
                schema_memory_id = 102,
                journal_memory_id = 103,
            )),
        );

        #[entity(
                                            store = "RelationCostStore",
                                            version = 1,
                                            pk(fields = ["id"]),
                                            fields(field(name = "id", value(item(prim = "Int32"))))
                                        )]
        pub struct RelationCostTarget {}
    };
}

macro_rules! define_relation_cost_source {
    (plain) => {
        #[entity(
            store = "RelationCostStore",
            version = 1,
            pk(fields = ["id"]),
            fields(
                field(name = "id", value(item(prim = "Int32"))),
                field(name = "target_id", value(item(prim = "Int32")))
            )
        )]
        pub struct RelationCostSource {}
    };
    (direct) => {
        #[entity(
            store = "RelationCostStore",
            version = 1,
            pk(fields = ["id"]),
            fields(
                field(name = "id", value(item(prim = "Int32"))),
                field(
                    name = "target_id",
                    value(item(rel = "RelationCostTarget", prim = "Int32"))
                )
            )
        )]
        pub struct RelationCostSource {}
    };
    (direct, $($extra:tt)*) => {
        #[entity(
            store = "RelationCostStore",
            version = 1,
            pk(fields = ["id"]),
            fields(
                field(name = "id", value(item(prim = "Int32"))),
                field(
                    name = "target_id",
                    value(item(rel = "RelationCostTarget", prim = "Int32"))
                ),
                $($extra)*
            )
        )]
        pub struct RelationCostSource {}
    };
}

/// Scalar control with the same actor code and row layout but no relation.
pub mod none {
    use super::*;

    define_relation_cost_actor!(
        RelationCostNoneCanister = "RelationCostNoneCanister",
        "relation_cost_none"
    );
    define_relation_cost_source!(plain);
}

/// Direct-relation control for the existing scalar fast path.
pub mod direct {
    use super::*;

    define_relation_cost_actor!(
        RelationCostDirectCanister = "RelationCostDirectCanister",
        "relation_cost_direct"
    );
    define_relation_cost_source!(direct);
}

/// Single-valued composite control frozen before nested relations are enabled.
pub mod shallow {
    use super::*;

    define_relation_cost_actor!(
        RelationCostShallowCanister = "RelationCostShallowCanister",
        "relation_cost_shallow"
    );

    #[enum_(
        variant(name = "Absent"),
        variant(name = "Target", value(item(prim = "Int32")))
    )]
    pub struct RelationCostChoice {}

    #[record(fields(
        field(name = "required_target_id", value(item(prim = "Int32"))),
        field(name = "optional_target_id", value(opt, item(prim = "Int32"))),
        field(name = "choice", value(item(is = "RelationCostChoice")))
    ))]
    pub struct RelationCostWrapper {}

    define_relation_cost_source!(
        direct,
        field(
            name = "wrapper",
            value(opt, item(is = "RelationCostWrapper"))
        )
    );
}

/// Repeated-composite control frozen before collection traversal is enabled.
pub mod repeated {
    use super::*;

    define_relation_cost_actor!(
        RelationCostRepeatedCanister = "RelationCostRepeatedCanister",
        "relation_cost_repeated"
    );

    #[list(item(prim = "Int32"))]
    pub struct RelationCostTargetList {}

    #[set(item(prim = "Int32"))]
    pub struct RelationCostTargetSet {}

    #[map(key(prim = "Nat32"), value(item(prim = "Int32")))]
    pub struct RelationCostTargetMap {}

    define_relation_cost_source!(
        direct,
        field(
            name = "target_list",
            value(opt, item(is = "RelationCostTargetList"))
        ),
        field(
            name = "target_set",
            value(opt, item(is = "RelationCostTargetSet"))
        ),
        field(
            name = "target_map",
            value(opt, item(is = "RelationCostTargetMap"))
        )
    );
}
