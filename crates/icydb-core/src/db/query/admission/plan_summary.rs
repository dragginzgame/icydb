//! Module: db::query::admission::plan_summary
//! Responsibility: project planner facts into read-admission summaries.
//! Does not own: admission policy evaluation or verbose rendering.
//! Boundary: keeps planner-to-admission extraction separate from policy rules.

use std::ops::Bound;

use crate::{
    db::{
        access::IndexBranchSetOrderedSuffix,
        query::plan::{
            AccessPlanProjection, AccessPlannedQuery, GroupPlan, PrimaryKeyInputResourceSummary,
            QueryMode, ResidualFilterShape, ScalarPlan, project_access_plan,
        },
    },
    value::Value,
};

use super::{
    QueryAdmissionAccessKind, QueryAdmissionDecision, QueryAdmissionGroupedSummary,
    QueryAdmissionLane, QueryAdmissionOrdering, QueryAdmissionPlanShape,
    QueryAdmissionResidualFilter, QueryAdmissionSummary, QueryBoundKind,
    QueryMaterializationSummary,
};

pub(super) fn summary_from_plan(
    lane: QueryAdmissionLane,
    plan: &AccessPlannedQuery,
) -> QueryAdmissionSummary {
    let access = summarize_access_plan(plan);
    let grouped = plan.grouped_plan().map(summarize_grouped_plan);
    let (limit, offset) = scalar_limit_and_offset(plan.scalar_plan());
    let (mut returned_row_bound, mut returned_row_bound_kind) =
        returned_row_bound_from_plan(limit, grouped);
    if returned_row_bound.is_none() && grouped.is_none() {
        (returned_row_bound, returned_row_bound_kind) =
            returned_row_bound_from_exact_access(&access);
    }
    let primary_key_input_resource = plan.access_choice().primary_key_input_resource();
    let scan_bound_kind = access.scan_bound_kind();

    QueryAdmissionSummary {
        lane,
        decision: QueryAdmissionDecision::Admitted,
        plan_shape: plan_shape(plan),
        selected_access: access.kind,
        selected_index: access.selected_index,
        limit,
        offset: Some(offset),
        scan_bound: access.exact_scan_bound,
        scan_bound_kind,
        returned_row_bound,
        returned_row_bound_kind,
        response_byte_bound: None,
        response_byte_bound_kind: QueryBoundKind::Unavailable,
        primary_key_input_terms: primary_key_input_resource
            .map(PrimaryKeyInputResourceSummary::raw_term_count),
        primary_key_input_payload_bytes: primary_key_input_resource
            .map(PrimaryKeyInputResourceSummary::estimated_payload_bytes),
        residual_filter: admission_residual_filter(plan.residual_filter_shape()),
        ordering: admission_ordering(plan),
        grouped,
        materialization: QueryMaterializationSummary::none(),
        rejection: None,
    }
}

// Keep the staged extractor live before admission enforcement calls it directly.
const _: fn(QueryAdmissionLane, &AccessPlannedQuery) -> QueryAdmissionSummary = summary_from_plan;

pub(super) const fn access_satisfies_index_requirement(
    kind: QueryAdmissionAccessKind,
    scan_bound: Option<u64>,
) -> bool {
    kind.is_secondary_index()
        || matches!(
            (kind, scan_bound),
            (
                QueryAdmissionAccessKind::ByKey | QueryAdmissionAccessKind::ByKeys,
                Some(_)
            )
        )
}

struct AdmissionAccessProjection;

#[derive(Clone, Debug, Eq, PartialEq)]
struct AdmissionAccessSummary {
    kind: QueryAdmissionAccessKind,
    selected_index: Option<String>,
    exact_scan_bound: Option<u64>,
}

impl AdmissionAccessSummary {
    const fn non_index(kind: QueryAdmissionAccessKind, exact_scan_bound: Option<u64>) -> Self {
        Self {
            kind,
            selected_index: None,
            exact_scan_bound,
        }
    }

    fn secondary_index(kind: QueryAdmissionAccessKind, index_name: &str) -> Self {
        Self {
            kind,
            selected_index: Some(index_name.to_string()),
            exact_scan_bound: None,
        }
    }

    const fn composite(kind: QueryAdmissionAccessKind) -> Self {
        Self {
            kind,
            selected_index: None,
            exact_scan_bound: None,
        }
    }

    const fn scan_bound_kind(&self) -> QueryBoundKind {
        if self.exact_scan_bound.is_some() {
            QueryBoundKind::Exact
        } else {
            QueryBoundKind::Unavailable
        }
    }
}

impl AccessPlanProjection<Value> for AdmissionAccessProjection {
    type Output = AdmissionAccessSummary;

    fn by_key(&mut self, _key: &Value) -> Self::Output {
        AdmissionAccessSummary::non_index(QueryAdmissionAccessKind::ByKey, Some(1))
    }

    fn by_keys(&mut self, keys: &[Value]) -> Self::Output {
        AdmissionAccessSummary::non_index(
            QueryAdmissionAccessKind::ByKeys,
            Some(u64::try_from(keys.len()).unwrap_or(u64::MAX)),
        )
    }

    fn key_range(&mut self, _start: &Value, _end: &Value) -> Self::Output {
        AdmissionAccessSummary::non_index(QueryAdmissionAccessKind::KeyRange, None)
    }

    fn index_prefix(
        &mut self,
        index_name: &str,
        _index_fields: &[String],
        _prefix_len: usize,
        _values: &[Value],
    ) -> Self::Output {
        AdmissionAccessSummary::secondary_index(QueryAdmissionAccessKind::IndexPrefix, index_name)
    }

    fn index_multi_lookup(
        &mut self,
        index_name: &str,
        _index_fields: &[String],
        _values: &[Value],
    ) -> Self::Output {
        AdmissionAccessSummary::secondary_index(
            QueryAdmissionAccessKind::IndexMultiLookup,
            index_name,
        )
    }

    fn index_branch_set(
        &mut self,
        index_name: &str,
        _index_fields: &[String],
        _fixed_values: &[Value],
        _branch_values: &[Value],
        _ordered_suffix: IndexBranchSetOrderedSuffix,
    ) -> Self::Output {
        AdmissionAccessSummary::secondary_index(
            QueryAdmissionAccessKind::IndexBranchSet,
            index_name,
        )
    }

    fn index_range(
        &mut self,
        index_name: &str,
        _index_fields: &[String],
        _prefix_len: usize,
        _prefix: &[Value],
        _lower: &Bound<Value>,
        _upper: &Bound<Value>,
    ) -> Self::Output {
        AdmissionAccessSummary::secondary_index(QueryAdmissionAccessKind::IndexRange, index_name)
    }

    fn full_scan(&mut self) -> Self::Output {
        AdmissionAccessSummary::non_index(QueryAdmissionAccessKind::FullScan, None)
    }

    fn union(&mut self, _children: Vec<Self::Output>) -> Self::Output {
        AdmissionAccessSummary::composite(QueryAdmissionAccessKind::Union)
    }

    fn intersection(&mut self, _children: Vec<Self::Output>) -> Self::Output {
        AdmissionAccessSummary::composite(QueryAdmissionAccessKind::Intersection)
    }
}

fn summarize_access_plan(plan: &AccessPlannedQuery) -> AdmissionAccessSummary {
    project_access_plan(&plan.access, &mut AdmissionAccessProjection)
}

fn summarize_grouped_plan(plan: &GroupPlan) -> QueryAdmissionGroupedSummary {
    QueryAdmissionGroupedSummary::new(
        u32::try_from(plan.group.group_fields.len()).unwrap_or(u32::MAX),
        u32::try_from(plan.group.aggregates.len()).unwrap_or(u32::MAX),
        u32::try_from(
            plan.group
                .aggregates
                .iter()
                .filter(|aggregate| aggregate.distinct)
                .count(),
        )
        .unwrap_or(u32::MAX),
        plan.group.execution.max_groups(),
        plan.group.execution.max_group_bytes(),
        plan.having_expr.is_some(),
    )
}

const fn scalar_limit_and_offset(plan: &ScalarPlan) -> (Option<u32>, u32) {
    match plan.mode {
        QueryMode::Load(load) => match &plan.page {
            Some(page) => (page.limit, page.offset),
            None => (load.limit(), load.offset()),
        },
        QueryMode::Delete(delete) => match plan.delete_limit {
            Some(delete_limit) => (delete_limit.limit, delete_limit.offset),
            None => (delete.limit(), delete.offset()),
        },
    }
}

fn returned_row_bound_from_plan(
    limit: Option<u32>,
    grouped: Option<QueryAdmissionGroupedSummary>,
) -> (Option<u32>, QueryBoundKind) {
    if let Some(limit) = limit {
        return (Some(limit), QueryBoundKind::EnforcedRuntimeCap);
    }

    let Some(grouped) = grouped else {
        return (None, QueryBoundKind::Unavailable);
    };
    if grouped.max_groups() == u64::MAX {
        return (None, QueryBoundKind::Unavailable);
    }

    (
        Some(u32::try_from(grouped.max_groups()).unwrap_or(u32::MAX)),
        QueryBoundKind::ConservativeUpperBound,
    )
}

fn returned_row_bound_from_exact_access(
    access: &AdmissionAccessSummary,
) -> (Option<u32>, QueryBoundKind) {
    match (access.kind, access.exact_scan_bound) {
        (QueryAdmissionAccessKind::ByKey | QueryAdmissionAccessKind::ByKeys, Some(bound)) => (
            Some(clamp_u64_to_u32(bound)),
            QueryBoundKind::ConservativeUpperBound,
        ),
        _ => (None, QueryBoundKind::Unavailable),
    }
}

fn clamp_u64_to_u32(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

const fn admission_residual_filter(shape: ResidualFilterShape) -> QueryAdmissionResidualFilter {
    match shape {
        ResidualFilterShape::Absent => QueryAdmissionResidualFilter::Absent,
        ResidualFilterShape::Predicate => QueryAdmissionResidualFilter::Predicate,
        ResidualFilterShape::Expression => QueryAdmissionResidualFilter::Expression,
        ResidualFilterShape::ExpressionAndPredicate => {
            QueryAdmissionResidualFilter::ExpressionAndPredicate
        }
    }
}

fn admission_ordering(plan: &AccessPlannedQuery) -> QueryAdmissionOrdering {
    if plan.scalar_plan().order.is_none() {
        return QueryAdmissionOrdering::None;
    }

    if plan.resolved_order().is_some() {
        QueryAdmissionOrdering::Resolved
    } else {
        QueryAdmissionOrdering::Requested
    }
}

const fn plan_shape(plan: &AccessPlannedQuery) -> QueryAdmissionPlanShape {
    if plan.grouped_plan().is_some() {
        return QueryAdmissionPlanShape::GroupedAggregate;
    }

    match plan.scalar_plan().mode {
        QueryMode::Load(_) => QueryAdmissionPlanShape::ScalarRead,
        QueryMode::Delete(_) => QueryAdmissionPlanShape::Delete,
    }
}
