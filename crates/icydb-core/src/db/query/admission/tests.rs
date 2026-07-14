use std::num::NonZeroU32;

use crate::{
    db::{
        access::{AccessPath, SemanticIndexAccessContract},
        predicate::{MissingRowPolicy, Predicate},
        query::plan::{
            AccessPlannedQuery, AggregateKind, DeleteLimitSpec, DeleteSpec, FieldSlot,
            GroupAggregateSpec, GroupSpec, GroupedExecutionConfig, OrderDirection, OrderSpec,
            OrderTerm, PageSpec, QueryMode,
            expr::{Expr, FieldId},
        },
    },
    model::index::IndexModel,
    value::Value,
};

use super::{
    GroupedAdmissionPolicy, QueryAdmissionAccessKind, QueryAdmissionDecision, QueryAdmissionLane,
    QueryAdmissionOrdering, QueryAdmissionPlanShape, QueryAdmissionPolicy, QueryAdmissionRejection,
    QueryAdmissionResidualFilter, QueryAdmissionSummary, QueryBoundKind,
    QueryMaterializationSummary,
};

const ADMISSION_INDEX_FIELDS: [&str; 1] = ["tag"];
const ADMISSION_INDEX: IndexModel = IndexModel::generated(
    "admission::tag",
    "admission::tag_store",
    &ADMISSION_INDEX_FIELDS,
    false,
);

#[test]
fn public_read_policy_has_safe_defaults() {
    let max_rows = NonZeroU32::new(50).expect("test max rows is non-zero");
    let policy = QueryAdmissionPolicy::public_read(max_rows);

    assert_eq!(policy.lane(), QueryAdmissionLane::PublicRead);
    assert!(policy.require_limit());
    assert!(policy.require_index());
    assert!(!policy.allow_full_scan());
    assert!(!policy.allow_materialized_sort());
    assert_eq!(policy.max_returned_rows(), Some(max_rows));
    assert!(!policy.grouped().has_hard_limits());
}

#[test]
fn diagnostic_explain_lane_does_not_execute_rows() {
    let policy = QueryAdmissionPolicy::diagnostic_explain();

    assert_eq!(policy.lane().as_str(), "diagnostic_explain");
    assert!(!policy.lane().executes_rows());
}

#[test]
fn grouped_policy_requires_group_and_memory_budgets() {
    let max_groups = NonZeroU32::new(8).expect("test group cap is non-zero");
    let max_bytes = NonZeroU32::new(4096).expect("test byte cap is non-zero");
    let policy = GroupedAdmissionPolicy::bounded(max_groups, max_bytes, None);

    assert!(policy.has_hard_limits());
    assert_eq!(policy.max_groups(), Some(max_groups));
    assert_eq!(policy.max_group_bytes(), Some(max_bytes));
}

#[test]
fn proven_or_enforced_bounds_admit_public_reads() {
    assert!(QueryBoundKind::Exact.admits_public_read());
    assert!(QueryBoundKind::ConservativeUpperBound.admits_public_read());
    assert!(QueryBoundKind::EnforcedRuntimeCap.admits_public_read());
    assert!(!QueryBoundKind::Unavailable.admits_public_read());
}

#[test]
fn access_kind_classifies_secondary_indexes_and_full_scans() {
    assert!(QueryAdmissionAccessKind::IndexPrefix.is_secondary_index());
    assert!(QueryAdmissionAccessKind::FullScan.is_full_scan());
    assert!(!QueryAdmissionAccessKind::ByKey.is_secondary_index());
}

#[test]
fn rejection_maps_to_stable_diagnostic() {
    let rejection = QueryAdmissionRejection::PublicQueryRequiresLimit;

    assert_eq!(
        rejection.code(),
        icydb_diagnostic_code::QueryReadAdmissionCode::PublicQueryRequiresLimit,
    );
}

#[test]
fn admission_summary_renders_stable_verbose_explain_block() {
    let summary =
        public_read_policy().evaluate(summary_for_path(AccessPath::<Value>::FullScan, Some(5), 0));

    let rendered = summary.render_text_block();

    assert!(
        rendered.starts_with("admission:\n  lane=public_read\n  decision=rejected"),
        "admission block should start with stable lane and decision fields: {rendered}",
    );
    assert!(
        rendered.contains("\n  reason=unbounded_full_scan_rejected"),
        "admission block should include a stable rejection reason: {rendered}",
    );
    assert!(
        rendered.contains("\n  selected_access=full_scan"),
        "admission block should include the selected access class: {rendered}",
    );
    assert!(
        rendered.contains("\n  grouped=false"),
        "admission block should include grouped classification: {rendered}",
    );
}

#[test]
fn plan_summary_classifies_full_scan_without_overclaiming_bounds() {
    let plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);

    let summary = QueryAdmissionSummary::from_plan(QueryAdmissionLane::PublicRead, &plan);

    assert_eq!(summary.plan_shape(), QueryAdmissionPlanShape::ScalarRead);
    assert_eq!(
        summary.selected_access(),
        QueryAdmissionAccessKind::FullScan
    );
    assert_eq!(summary.selected_index(), None);
    assert_eq!(summary.limit(), None);
    assert_eq!(summary.offset(), Some(0));
    assert_eq!(summary.scan_bound(), None);
    assert_eq!(summary.scan_bound_kind(), QueryBoundKind::Unavailable);
    assert_eq!(summary.returned_row_bound(), None);
    assert_eq!(
        summary.returned_row_bound_kind(),
        QueryBoundKind::Unavailable
    );
    assert_eq!(
        summary.residual_filter(),
        QueryAdmissionResidualFilter::Absent
    );
    assert_eq!(summary.ordering(), QueryAdmissionOrdering::None);
}

#[test]
fn plan_summary_uses_point_lookup_and_limit_as_proven_bounds() {
    let mut plan =
        AccessPlannedQuery::new(AccessPath::ByKey(Value::Nat64(7)), MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(5),
        offset: 2,
    });

    let summary = QueryAdmissionSummary::from_plan(QueryAdmissionLane::PublicRead, &plan);

    assert_eq!(summary.selected_access(), QueryAdmissionAccessKind::ByKey);
    assert_eq!(summary.limit(), Some(5));
    assert_eq!(summary.offset(), Some(2));
    assert_eq!(summary.scan_bound(), Some(1));
    assert_eq!(summary.scan_bound_kind(), QueryBoundKind::Exact);
    assert_eq!(summary.returned_row_bound(), Some(5));
    assert_eq!(
        summary.returned_row_bound_kind(),
        QueryBoundKind::EnforcedRuntimeCap
    );
}

#[test]
fn plan_summary_uses_exact_primary_key_access_as_returned_row_bound_without_limit() {
    let plan =
        AccessPlannedQuery::new(AccessPath::ByKey(Value::Nat64(7)), MissingRowPolicy::Ignore);

    let summary = QueryAdmissionSummary::from_plan(QueryAdmissionLane::PublicRead, &plan);

    assert_eq!(summary.selected_access(), QueryAdmissionAccessKind::ByKey);
    assert_eq!(summary.limit(), None);
    assert_eq!(summary.scan_bound(), Some(1));
    assert_eq!(summary.scan_bound_kind(), QueryBoundKind::Exact);
    assert_eq!(summary.returned_row_bound(), Some(1));
    assert_eq!(
        summary.returned_row_bound_kind(),
        QueryBoundKind::ConservativeUpperBound
    );
}

#[test]
fn plan_summary_uses_exact_primary_key_set_as_returned_row_bound_without_limit() {
    let plan = AccessPlannedQuery::new(
        AccessPath::ByKeys(vec![Value::Nat64(7), Value::Nat64(8)]),
        MissingRowPolicy::Ignore,
    );

    let summary = QueryAdmissionSummary::from_plan(QueryAdmissionLane::PublicRead, &plan);

    assert_eq!(summary.selected_access(), QueryAdmissionAccessKind::ByKeys);
    assert_eq!(summary.limit(), None);
    assert_eq!(summary.scan_bound(), Some(2));
    assert_eq!(summary.scan_bound_kind(), QueryBoundKind::Exact);
    assert_eq!(summary.returned_row_bound(), Some(2));
    assert_eq!(
        summary.returned_row_bound_kind(),
        QueryBoundKind::ConservativeUpperBound
    );
}

#[test]
fn plan_summary_preserves_selected_index_identity() {
    let plan = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: SemanticIndexAccessContract::model_only_from_generated_index(ADMISSION_INDEX),
            values: vec![Value::Text("alpha".to_string())],
        },
        MissingRowPolicy::Ignore,
    );

    let summary = QueryAdmissionSummary::from_plan(QueryAdmissionLane::PublicRead, &plan);

    assert_eq!(
        summary.selected_access(),
        QueryAdmissionAccessKind::IndexPrefix
    );
    assert_eq!(summary.selected_index(), Some("admission::tag"));
    assert_eq!(summary.scan_bound(), None);
    assert_eq!(summary.scan_bound_kind(), QueryBoundKind::Unavailable);
}

#[test]
fn plan_summary_classifies_residual_and_requested_ordering() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().predicate = Some(Predicate::eq(
        "tag".to_string(),
        Value::Text("alpha".to_string()),
    ));
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![OrderTerm::field("tag", OrderDirection::Asc)],
    });

    let summary = QueryAdmissionSummary::from_plan(QueryAdmissionLane::DiagnosticExplain, &plan);

    assert_eq!(
        summary.residual_filter(),
        QueryAdmissionResidualFilter::Predicate
    );
    assert_eq!(summary.ordering(), QueryAdmissionOrdering::Requested);
    assert!(!summary.materialization().materialized_sort());
    assert_eq!(summary.materialization().materialized_rows(), None);
    assert_eq!(
        summary.materialization().row_bound_kind(),
        QueryBoundKind::Unavailable
    );
}

#[test]
fn plan_summary_carries_grouped_execution_budgets() {
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped_with_having_expr(
            GroupSpec {
                group_fields: vec![FieldSlot::from_test_slot(0, "tag")],
                aggregates: vec![GroupAggregateSpec {
                    kind: AggregateKind::Count,
                    input_expr: None,
                    filter_expr: None,
                    distinct: false,
                }],
                execution: GroupedExecutionConfig::with_hard_limits(12, 4096),
            },
            Some(Expr::Field(FieldId::new("tag"))),
        );

    let summary = QueryAdmissionSummary::from_plan(QueryAdmissionLane::DiagnosticExplain, &grouped);
    let grouped = summary
        .grouped()
        .expect("summary should include grouped facts");

    assert_eq!(
        summary.plan_shape(),
        QueryAdmissionPlanShape::GroupedAggregate
    );
    assert_eq!(grouped.group_field_count(), 1);
    assert_eq!(grouped.aggregate_count(), 1);
    assert_eq!(grouped.distinct_aggregate_count(), 0);
    assert_eq!(grouped.max_groups(), 12);
    assert_eq!(grouped.max_group_bytes(), 4096);
    assert!(grouped.has_having_filter());
    assert_eq!(summary.returned_row_bound(), Some(12));
    assert_eq!(
        summary.returned_row_bound_kind(),
        QueryBoundKind::ConservativeUpperBound
    );
}

#[test]
fn plan_summary_reads_delete_window_without_executing_it() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().mode = QueryMode::Delete(DeleteSpec::new());
    plan.scalar_plan_mut().delete_limit = Some(DeleteLimitSpec {
        limit: Some(3),
        offset: 1,
    });

    let summary = QueryAdmissionSummary::from_plan(QueryAdmissionLane::DiagnosticExplain, &plan);

    assert_eq!(summary.plan_shape(), QueryAdmissionPlanShape::Delete);
    assert_eq!(summary.limit(), Some(3));
    assert_eq!(summary.offset(), Some(1));
    assert_eq!(summary.returned_row_bound(), Some(3));
}

#[test]
fn public_read_evaluation_rejects_missing_limit_before_access_shape() {
    let policy = public_read_policy();
    let summary = summary_for_index_prefix(None, 0);

    let evaluated = policy.evaluate(summary);

    assert_eq!(evaluated.decision(), QueryAdmissionDecision::Rejected);
    assert_eq!(
        evaluated.rejection(),
        Some(QueryAdmissionRejection::PublicQueryRequiresLimit)
    );
}

#[test]
fn public_read_evaluation_rejects_full_scan_even_with_limit() {
    let policy = public_read_policy();
    let summary = summary_for_path(AccessPath::<Value>::FullScan, Some(5), 0);

    let evaluated = policy.evaluate(summary);

    assert_eq!(evaluated.decision(), QueryAdmissionDecision::Rejected);
    assert_eq!(
        evaluated.rejection(),
        Some(QueryAdmissionRejection::UnboundedFullScanRejected)
    );
}

#[test]
fn public_read_evaluation_admits_indexed_bounded_scalar_read() {
    let policy = public_read_policy();
    let summary = summary_for_index_prefix(Some(5), 0);

    let evaluated = policy.evaluate(summary);

    assert_eq!(evaluated.decision(), QueryAdmissionDecision::Admitted);
    assert_eq!(evaluated.rejection(), None);
}

#[test]
fn public_read_evaluation_admits_exact_primary_key_read() {
    let policy = public_read_policy();
    let summary = summary_for_path(
        AccessPath::ByKey(Value::Text("primary".to_string())),
        None,
        0,
    );

    let evaluated = policy.evaluate(summary);

    assert_eq!(evaluated.decision(), QueryAdmissionDecision::Admitted);
    assert_eq!(evaluated.limit(), None);
    assert_eq!(evaluated.scan_bound(), Some(1));
    assert_eq!(evaluated.returned_row_bound(), Some(1));
}

#[test]
fn public_read_evaluation_rejects_primary_key_set_above_returned_row_policy() {
    let policy = public_read_policy();
    let keys = (0..=50).map(Value::Nat64).collect();
    let summary = summary_for_path(AccessPath::ByKeys(keys), None, 0);

    let evaluated = policy.evaluate(summary);

    assert_eq!(evaluated.decision(), QueryAdmissionDecision::Rejected);
    assert_eq!(
        evaluated.rejection(),
        Some(QueryAdmissionRejection::ReturnedRowBoundExceedsPolicy)
    );
}

#[test]
fn public_read_evaluation_rejects_returned_row_cap_overflow() {
    let policy = public_read_policy();
    let summary = summary_for_index_prefix(Some(51), 0);

    let evaluated = policy.evaluate(summary);

    assert_eq!(evaluated.decision(), QueryAdmissionDecision::Rejected);
    assert_eq!(
        evaluated.rejection(),
        Some(QueryAdmissionRejection::ReturnedRowBoundExceedsPolicy)
    );
}

#[test]
fn public_read_evaluation_rejects_unresolved_order_materialized_sort() {
    let policy = public_read_policy();
    let summary = summary_for_index_prefix(Some(5), 0);
    let returned_row_bound = summary.returned_row_bound();
    let returned_row_bound_kind = summary.returned_row_bound_kind();
    let summary = summary.with_materialization(QueryMaterializationSummary::sort(
        returned_row_bound,
        returned_row_bound_kind,
    ));

    let evaluated = policy.evaluate(summary);

    assert_eq!(evaluated.decision(), QueryAdmissionDecision::Rejected);
    assert_eq!(
        evaluated.rejection(),
        Some(QueryAdmissionRejection::SortRequiresMaterialization)
    );
}

#[test]
fn public_read_evaluation_admits_exact_key_set_materialized_sort() {
    let policy = public_read_policy();
    let summary = summary_for_path(
        AccessPath::ByKeys(vec![Value::Nat64(1), Value::Nat64(2), Value::Nat64(3)]),
        None,
        0,
    );
    let returned_row_bound = summary.returned_row_bound();
    let returned_row_bound_kind = summary.returned_row_bound_kind();
    let summary = summary.with_materialization(QueryMaterializationSummary::sort(
        returned_row_bound,
        returned_row_bound_kind,
    ));

    let evaluated = policy.evaluate(summary);

    assert_eq!(evaluated.decision(), QueryAdmissionDecision::Admitted);
    assert_eq!(evaluated.rejection(), None);
    assert_eq!(
        evaluated.selected_access(),
        QueryAdmissionAccessKind::ByKeys
    );
    assert_eq!(evaluated.scan_bound(), Some(3));
    assert_eq!(evaluated.returned_row_bound(), Some(3));
    assert!(evaluated.materialization().materialized_sort());
}

#[test]
fn public_read_evaluation_rejects_underbounded_key_set_materialized_sort() {
    let policy = public_read_policy();
    let summary = summary_for_path(
        AccessPath::ByKeys(vec![Value::Nat64(1), Value::Nat64(2), Value::Nat64(3)]),
        Some(1),
        0,
    );
    let returned_row_bound = summary.returned_row_bound();
    let returned_row_bound_kind = summary.returned_row_bound_kind();
    let summary = summary.with_materialization(QueryMaterializationSummary::sort(
        returned_row_bound,
        returned_row_bound_kind,
    ));

    let evaluated = policy.evaluate(summary);

    assert_eq!(evaluated.decision(), QueryAdmissionDecision::Rejected);
    assert_eq!(
        evaluated.rejection(),
        Some(QueryAdmissionRejection::SortRequiresMaterialization)
    );
}

#[test]
fn public_read_evaluation_fails_closed_when_bounded_route_falls_back_to_materialized_order() {
    let policy = public_read_policy();
    let bounded = summary_for_index_prefix(Some(1), 0);
    let admitted = policy.evaluate(bounded.clone());

    assert_eq!(admitted.decision(), QueryAdmissionDecision::Admitted);
    assert_eq!(admitted.returned_row_bound(), Some(1));

    let returned_row_bound = bounded.returned_row_bound();
    let returned_row_bound_kind = bounded.returned_row_bound_kind();
    let fallback = bounded.with_materialization(QueryMaterializationSummary::sort(
        returned_row_bound,
        returned_row_bound_kind,
    ));

    let evaluated = policy.evaluate(fallback);

    assert_eq!(evaluated.decision(), QueryAdmissionDecision::Rejected);
    assert_eq!(
        evaluated.rejection(),
        Some(QueryAdmissionRejection::SortRequiresMaterialization)
    );
}

#[test]
fn public_read_evaluation_rejects_grouped_query_without_group_budgets() {
    let policy = public_read_policy();
    let summary = grouped_summary_for_index_prefix(12, 4096, false);

    let evaluated = policy.evaluate(summary);

    assert_eq!(evaluated.decision(), QueryAdmissionDecision::Rejected);
    assert_eq!(
        evaluated.rejection(),
        Some(QueryAdmissionRejection::GroupedQueryRequiresLimits)
    );
}

#[test]
fn public_read_evaluation_admits_grouped_query_with_group_budgets_without_limit() {
    let policy = public_grouped_read_policy(None);
    let summary = grouped_summary_for_index_prefix(12, 4096, false);

    let evaluated = policy.evaluate(summary);

    assert_eq!(evaluated.decision(), QueryAdmissionDecision::Admitted);
    assert_eq!(evaluated.limit(), None);
    assert_eq!(evaluated.returned_row_bound(), Some(12));
    assert_eq!(evaluated.rejection(), None);
}

#[test]
fn public_read_evaluation_rejects_grouped_query_above_policy_budget() {
    let policy = public_grouped_read_policy(None);
    let summary = grouped_summary_for_index_prefix(51, 4096, false);

    let evaluated = policy.evaluate(summary);

    assert_eq!(evaluated.decision(), QueryAdmissionDecision::Rejected);
    assert_eq!(
        evaluated.rejection(),
        Some(QueryAdmissionRejection::GroupedQueryExceedsBudget)
    );
}

#[test]
fn public_read_evaluation_rejects_distinct_grouped_query_without_distinct_budget() {
    let policy = public_grouped_read_policy(None);
    let summary = grouped_summary_for_index_prefix(12, 4096, true);

    let evaluated = policy.evaluate(summary);

    assert_eq!(evaluated.decision(), QueryAdmissionDecision::Rejected);
    assert_eq!(
        evaluated.rejection(),
        Some(QueryAdmissionRejection::GroupedQueryRequiresLimits)
    );
}

#[test]
fn diagnostic_explain_policy_rejects_row_execution() {
    let policy = QueryAdmissionPolicy::diagnostic_explain();
    let summary = summary_for_index_prefix(Some(5), 0);

    let evaluated = policy.evaluate(summary);

    assert_eq!(evaluated.decision(), QueryAdmissionDecision::Rejected);
    assert_eq!(
        evaluated.rejection(),
        Some(QueryAdmissionRejection::DiagnosticLaneDoesNotExecute)
    );
}

fn public_read_policy() -> QueryAdmissionPolicy {
    QueryAdmissionPolicy::public_read(NonZeroU32::new(50).expect("test public row cap is non-zero"))
}

fn public_grouped_read_policy(distinct_entries: Option<NonZeroU32>) -> QueryAdmissionPolicy {
    public_read_policy().with_grouped_policy(GroupedAdmissionPolicy::bounded(
        NonZeroU32::new(50).expect("test public group cap is non-zero"),
        NonZeroU32::new(8192).expect("test public group byte cap is non-zero"),
        distinct_entries,
    ))
}

fn summary_for_index_prefix(limit: Option<u32>, offset: u32) -> QueryAdmissionSummary {
    summary_for_path(
        AccessPath::IndexPrefix {
            index: SemanticIndexAccessContract::model_only_from_generated_index(ADMISSION_INDEX),
            values: vec![Value::Text("alpha".to_string())],
        },
        limit,
        offset,
    )
}

fn summary_for_path(
    path: AccessPath<Value>,
    limit: Option<u32>,
    offset: u32,
) -> QueryAdmissionSummary {
    let mut plan = AccessPlannedQuery::new(path, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().page = Some(PageSpec { limit, offset });

    QueryAdmissionSummary::from_plan(QueryAdmissionLane::PublicRead, &plan)
}

fn grouped_summary_for_index_prefix(
    max_groups: u64,
    max_group_bytes: u64,
    distinct: bool,
) -> QueryAdmissionSummary {
    let grouped = AccessPlannedQuery::new(index_prefix_path(), MissingRowPolicy::Ignore)
        .into_grouped(GroupSpec {
            group_fields: vec![FieldSlot::from_test_slot(0, "tag")],
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                input_expr: Some(Box::new(Expr::Field(FieldId::new("tag")))),
                filter_expr: None,
                distinct,
            }],
            execution: GroupedExecutionConfig::with_hard_limits(max_groups, max_group_bytes),
        });

    QueryAdmissionSummary::from_plan(QueryAdmissionLane::PublicRead, &grouped)
}

fn index_prefix_path() -> AccessPath<Value> {
    AccessPath::IndexPrefix {
        index: SemanticIndexAccessContract::model_only_from_generated_index(ADMISSION_INDEX),
        values: vec![Value::Text("alpha".to_string())],
    }
}
