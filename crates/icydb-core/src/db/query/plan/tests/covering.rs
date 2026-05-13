//! Module: db::query::plan::tests::covering
//! Covers covering-read planner behavior at the query-plan owner boundary.
//! Does not own: local covering helper implementation details.
//! Boundary: keeps covering eligibility and execution-grade contract tests in the subsystem suite.

use crate::{
    db::{
        access::AccessPath,
        direction::Direction,
        predicate::{MissingRowPolicy, Predicate},
        query::plan::{
            AccessPlannedQuery, CoveringExistingRowMode, CoveringProjectionOrder, OrderDirection,
            OrderSpec, covering_read_execution_plan_from_fields,
            expr::{FieldId, ProjectionSelection},
            index_covering_existing_rows_terminal_eligible,
        },
    },
    model::{entity::EntityModel, field::FieldKind, index::IndexModel},
    traits::EntitySchema,
    types::Ulid,
    value::Value,
};

const COVERING_READ_FIELDS_GROUP_RANK: [&str; 2] = ["group", "rank"];
const COVERING_READ_INDEX: IndexModel = IndexModel::generated(
    "plan_tests::covering::idx_group_rank",
    "plan_tests::covering::CoveringReadEntity",
    &COVERING_READ_FIELDS_GROUP_RANK,
    false,
);

crate::test_entity! {
    ident = PlanTestsCoveringReadEntity,
    id = Ulid,
    entity_name = "PlanTestsCoveringReadEntity",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("group", FieldKind::Nat),
        ("rank", FieldKind::Nat),
        ("label", FieldKind::Text { max_len: None }),
    ],
    indexes = [&COVERING_READ_INDEX],
}

fn covering_read_model() -> &'static EntityModel {
    <PlanTestsCoveringReadEntity as EntitySchema>::MODEL
}

fn covering_read_execution_plan(
    plan: &AccessPlannedQuery,
    primary_key_name: &'static str,
    strict_predicate_compatible: bool,
) -> Option<crate::db::query::plan::CoveringReadExecutionPlan> {
    let mut finalized = plan.clone();
    finalized
        .finalize_static_planning_shape_for_model_only(covering_read_model())
        .expect("covering tests require planner-frozen projection metadata");

    covering_read_execution_plan_from_fields(
        covering_read_model().fields(),
        &finalized,
        primary_key_name,
        strict_predicate_compatible,
    )
}

#[test]
fn index_covering_existing_rows_terminal_requires_index_shape() {
    let plan = AccessPlannedQuery::new(AccessPath::FullScan, MissingRowPolicy::Ignore);

    assert!(
        !index_covering_existing_rows_terminal_eligible(&plan, true),
        "full-scan shape must not qualify for index-covering existing-row terminal eligibility",
    );
}

#[test]
fn index_covering_existing_rows_terminal_requires_no_order() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                COVERING_READ_INDEX,
            ),
            values: vec![Value::Nat(7)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "rank",
            OrderDirection::Asc,
        )],
    });

    assert!(
        !index_covering_existing_rows_terminal_eligible(&plan, true),
        "ordered shapes must not qualify for index-covering existing-row terminal eligibility",
    );
}

#[test]
fn index_covering_existing_rows_terminal_accepts_unordered_no_predicate() {
    let plan = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                COVERING_READ_INDEX,
            ),
            values: vec![Value::Nat(7)],
        },
        MissingRowPolicy::Ignore,
    );

    assert!(
        index_covering_existing_rows_terminal_eligible(&plan, false),
        "unordered index-backed shapes without residual predicates should be eligible",
    );
}

#[test]
fn index_covering_existing_rows_terminal_requires_strict_predicate_when_residual_present() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                COVERING_READ_INDEX,
            ),
            values: vec![Value::Nat(7)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().predicate = Some(Predicate::eq("rank".to_string(), Value::Nat(7)));

    assert!(
        !index_covering_existing_rows_terminal_eligible(&plan, false),
        "residual-predicate shapes must be rejected when strict predicate compatibility is absent",
    );
    assert!(
        index_covering_existing_rows_terminal_eligible(&plan, true),
        "residual-predicate shapes should be eligible when strict predicate compatibility is present",
    );
}

#[test]
fn covering_read_execution_plan_marks_secondary_load_shapes_as_planner_proven() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                COVERING_READ_INDEX,
            ),
            values: vec![Value::Nat(7)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("rank")]);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    });

    let covering = covering_read_execution_plan(&plan, "id", true)
        .expect("coverable projected load should derive one execution-grade covering plan");

    assert_eq!(covering.prefix_len, 1);
    assert_eq!(
        covering.order_contract,
        CoveringProjectionOrder::IndexOrder(Direction::Asc),
    );
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::ProvenByPlanner
    );
    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "rank");
}

#[test]
fn covering_read_execution_plan_marks_primary_store_pk_projection_as_planner_proven() {
    let mut plan = AccessPlannedQuery::new(AccessPath::FullScan, MissingRowPolicy::Ignore);
    plan.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("id")]);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });

    let covering = covering_read_execution_plan(&plan, "id", true)
        .expect("primary-store PK-only projections should derive one planner-proven covering plan");

    assert_eq!(covering.prefix_len, 0);
    assert_eq!(
        covering.order_contract,
        CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc),
    );
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::ProvenByPlanner
    );
    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "id");
}

#[test]
fn covering_read_execution_plan_marks_primary_store_pk_range_projection_as_planner_proven() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::KeyRange {
            start: Value::Ulid(Ulid::from_u128(9_511)),
            end: Value::Ulid(Ulid::from_u128(9_512)),
        },
        MissingRowPolicy::Ignore,
    );
    plan.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("id")]);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });

    let covering = covering_read_execution_plan(&plan, "id", true).expect(
        "primary-store PK-range projections should derive one planner-proven covering plan",
    );

    assert_eq!(covering.prefix_len, 0);
    assert_eq!(
        covering.order_contract,
        CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc),
    );
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::ProvenByPlanner
    );
    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "id");
}

#[test]
fn covering_read_execution_plan_marks_by_key_primary_projection_as_row_check_required() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::ByKey(Value::Ulid(Ulid::from_u128(9_501))),
        MissingRowPolicy::Ignore,
    );
    plan.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("id")]);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });

    let covering = covering_read_execution_plan(&plan, "id", true)
        .expect("by-key PK-only projections should derive one row-check covering plan");

    assert_eq!(covering.prefix_len, 0);
    assert_eq!(
        covering.order_contract,
        CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc),
    );
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::RequiresRowPresenceCheck,
    );
    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "id");
}

#[test]
fn covering_read_execution_plan_marks_by_keys_primary_projection_as_row_check_required() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::ByKeys(vec![
            Value::Ulid(Ulid::from_u128(9_501)),
            Value::Ulid(Ulid::from_u128(9_503)),
        ]),
        MissingRowPolicy::Ignore,
    );
    plan.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("id")]);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });

    let covering = covering_read_execution_plan(&plan, "id", true)
        .expect("by-keys PK-only projections should derive one row-check covering plan");

    assert_eq!(covering.prefix_len, 0);
    assert_eq!(
        covering.order_contract,
        CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc),
    );
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::RequiresRowPresenceCheck,
    );
    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "id");
}

#[test]
fn covering_read_execution_plan_rejects_by_keys_desc_primary_projection_for_now() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::ByKeys(vec![
            Value::Ulid(Ulid::from_u128(9_501)),
            Value::Ulid(Ulid::from_u128(9_503)),
        ]),
        MissingRowPolicy::Ignore,
    );
    plan.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("id")]);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Desc,
        )],
    });

    assert!(
        covering_read_execution_plan(&plan, "id", true).is_none(),
        "phase-1 multi-key PK covering should stay fail-closed on descending order until exact-key reorder is explicit",
    );
}
