//! Module: db::query::explain::tests::determinism
//! Covers EXPLAIN determinism and deterministic chosen-index projection behavior.
//! Does not own: grouped explain strategy/fallback behavior.
//! Boundary: keeps explain determinism assertions in one focused owner-level test file.

use crate::{
    db::{
        access::AccessPath,
        predicate::{MissingRowPolicy, Predicate, normalize},
        query::{
            explain::ExplainAccessPath,
            intent::{KeyAccess, build_access_plan_from_keys},
            plan::{AccessPlannedQuery, LoadSpec, LogicalPlan, QueryMode},
        },
    },
    model::index::IndexModel,
    traits::RuntimeValueEncode,
    types::Ulid,
    value::Value,
};

#[test]
fn explain_is_deterministic_for_same_query() {
    let predicate = Predicate::eq("id".to_string(), Ulid::default().to_value());
    let mut plan: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().predicate = Some(predicate);

    assert_eq!(plan.explain(), plan.explain());
}

#[test]
fn explain_is_deterministic_for_equivalent_predicates() {
    let id = Ulid::default();

    let predicate_a = normalize(&Predicate::And(vec![
        Predicate::eq("id".to_string(), id.to_value()),
        Predicate::eq("other".to_string(), Value::Text("x".to_string())),
    ]));
    let predicate_b = normalize(&Predicate::And(vec![
        Predicate::eq("other".to_string(), Value::Text("x".to_string())),
        Predicate::eq("id".to_string(), id.to_value()),
    ]));

    let mut plan_a: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan_a.scalar_plan_mut().predicate = Some(predicate_a);

    let mut plan_b: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan_b.scalar_plan_mut().predicate = Some(predicate_b);

    assert_eq!(plan_a.explain(), plan_b.explain());
}

#[test]
fn explain_is_deterministic_for_by_keys() {
    let a = Ulid::from_u128(1);
    let b = Ulid::from_u128(2);

    let access_a = build_access_plan_from_keys(&KeyAccess::Many(vec![a, b, a]));
    let access_b = build_access_plan_from_keys(&KeyAccess::Many(vec![b, a]));

    let plan_a: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: access_a,
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_planning_shape: None,
    };
    let plan_b: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: access_b,
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_planning_shape: None,
    };

    assert_eq!(plan_a.explain(), plan_b.explain());
}

#[test]
fn explain_reports_deterministic_index_choice() {
    const INDEX_FIELDS: [&str; 1] = ["idx_a"];
    const INDEX_A: IndexModel =
        IndexModel::generated("explain::idx_a", "explain::store", &INDEX_FIELDS, false);
    const INDEX_B: IndexModel =
        IndexModel::generated("explain::idx_a_alt", "explain::store", &INDEX_FIELDS, false);

    let mut indexes = [INDEX_B, INDEX_A];
    indexes.sort_by(|left, right| left.name().cmp(right.name()));
    let chosen = indexes[0];

    let plan: AccessPlannedQuery = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::from_index(chosen),
            values: vec![Value::Text("alpha".to_string())],
        },
        MissingRowPolicy::Ignore,
    );

    let explain = plan.explain();
    match explain.access() {
        ExplainAccessPath::IndexPrefix {
            name,
            fields,
            prefix_len,
            ..
        } => {
            assert_eq!(name, "explain::idx_a");
            assert_eq!(fields.as_slice(), vec!["idx_a".to_string()]);
            assert_eq!(*prefix_len, 1);
        }
        _ => panic!("expected index prefix"),
    }
}
