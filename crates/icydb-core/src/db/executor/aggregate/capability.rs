//! Module: executor::aggregate::capability
//! Responsibility: aggregate field-kind capability predicates.
//! Does not own: route planning policy or aggregate execution dispatch.
//! Boundary: reusable field capability checks for aggregate/route modules.

use crate::db::{
    direction::Direction,
    executor::{
        aggregate::{AccessPlannedQuery, AggregateKind},
        route::{
            AggregateRouteShape, count_pushdown_shape_supported,
            primary_key_stream_window_shape_supported,
        },
    },
    schema::{AcceptedFieldKind, classify_accepted_field_kind},
};

/// Return true when an accepted field kind permits deterministic aggregate ordering.
#[must_use]
pub(in crate::db::executor) const fn accepted_field_kind_supports_aggregate_ordering(
    kind: &AcceptedFieldKind,
) -> bool {
    classify_accepted_field_kind(kind).is_orderable()
}

/// Return true when an accepted field kind permits checked SUM arithmetic.
#[must_use]
pub(in crate::db::executor) const fn accepted_field_kind_supports_sum(
    kind: &AcceptedFieldKind,
) -> bool {
    classify_accepted_field_kind(kind).supports_arithmetic_numeric()
}

/// Return true when an accepted field kind permits decimal AVG arithmetic.
#[must_use]
pub(in crate::db::executor) const fn accepted_field_kind_supports_average(
    kind: &AcceptedFieldKind,
) -> bool {
    let semantics = classify_accepted_field_kind(kind);
    semantics.is_numeric() && semantics.supports_arithmetic_numeric()
}

/// Return true when one accepted grouped field already arrives in canonical
/// grouped-equality form.
#[must_use]
pub(in crate::db::executor) const fn accepted_field_kind_has_identity_group_canonical_form(
    kind: &AcceptedFieldKind,
) -> bool {
    classify_accepted_field_kind(kind).has_identity_group_canonical_form()
}

/// Return true when one accepted grouped field can use borrowed key probing.
#[must_use]
pub(in crate::db::executor) fn accepted_field_kind_supports_group_probe(
    kind: &AcceptedFieldKind,
) -> bool {
    match kind {
        AcceptedFieldKind::Relation { key_kind, .. } => {
            accepted_field_kind_supports_group_probe(key_kind)
        }
        AcceptedFieldKind::Decimal { .. } => true,
        _ => accepted_field_kind_has_identity_group_canonical_form(kind),
    }
}

///
/// AggregateExecutionPolicyInputs
///
/// Aggregate-policy derivation inputs computed by the route/planning boundary.
/// This keeps residual-filter/order-sort signals explicit while aggregate
/// policy owns the resulting aggregate execution capability contract.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct AggregateExecutionPolicyInputs {
    residual_filter_present: bool,
    requires_post_access_sort: bool,
}

impl AggregateExecutionPolicyInputs {
    /// Construct aggregate-policy derivation inputs.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        residual_filter_present: bool,
        requires_post_access_sort: bool,
    ) -> Self {
        Self {
            residual_filter_present,
            requires_post_access_sort,
        }
    }

    #[must_use]
    const fn residual_filter_present(self) -> bool {
        self.residual_filter_present
    }

    #[must_use]
    const fn requires_post_access_sort(self) -> bool {
        self.requires_post_access_sort
    }
}

///
/// AggregateExecutionPolicy
///
/// Aggregate-policy capability contract consumed by route planning.
/// Aggregates count-pushdown shape support, composite fast-path eligibility,
/// and field-extrema eligibility under one aggregate-owned boundary.
///

#[expect(
    clippy::struct_excessive_bools,
    reason = "independent aggregate route capabilities are derived together"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct AggregateExecutionPolicy {
    count_pushdown_shape_supported: bool,
    composite_aggregate_fast_path_eligible: bool,
    field_min_fast_path: bool,
    field_max_fast_path: bool,
}

impl AggregateExecutionPolicy {
    #[must_use]
    pub(in crate::db::executor) const fn count_pushdown_shape_supported(self) -> bool {
        self.count_pushdown_shape_supported
    }

    #[must_use]
    pub(in crate::db::executor) const fn composite_aggregate_fast_path_eligible(self) -> bool {
        self.composite_aggregate_fast_path_eligible
    }

    #[must_use]
    pub(in crate::db::executor) const fn field_min_fast_path(self) -> bool {
        self.field_min_fast_path
    }

    #[must_use]
    pub(in crate::db::executor) const fn field_max_fast_path(self) -> bool {
        self.field_max_fast_path
    }
}

/// Derive aggregate execution policy from one validated plan and aggregate context.
pub(in crate::db::executor) fn derive_aggregate_execution_policy(
    plan: &AccessPlannedQuery,
    direction: Direction,
    aggregate_shape: Option<AggregateRouteShape<'_>>,
    inputs: AggregateExecutionPolicyInputs,
) -> AggregateExecutionPolicy {
    let access_shape_facts = plan.access_shape_facts();
    let field_min_fast_path = assess_field_extrema_fast_path_eligibility(
        plan,
        direction,
        aggregate_shape,
        AggregateKind::Min,
    );
    let field_max_fast_path = assess_field_extrema_fast_path_eligibility(
        plan,
        direction,
        aggregate_shape,
        AggregateKind::Max,
    );

    AggregateExecutionPolicy {
        count_pushdown_shape_supported: access_shape_facts
            .single_path_facts()
            .is_some_and(|shape_facts| count_pushdown_shape_supported(&shape_facts)),
        composite_aggregate_fast_path_eligible: access_shape_facts.is_composite()
            && !inputs.residual_filter_present()
            && !inputs.requires_post_access_sort(),
        field_min_fast_path,
        field_max_fast_path,
    }
}

/// Derive aggregate-policy field-extrema fast-path eligibility for one plan.
fn assess_field_extrema_fast_path_eligibility(
    plan: &AccessPlannedQuery,
    direction: Direction,
    aggregate_shape: Option<AggregateRouteShape<'_>>,
    extrema_kind: AggregateKind,
) -> bool {
    let Some(aggregate) = aggregate_shape else {
        return false;
    };
    if aggregate.kind() != extrema_kind {
        return false;
    }
    let Some(_target_field) = aggregate.target_field() else {
        return false;
    };
    if !aggregate.target_field_known() {
        return false;
    }
    if !aggregate.target_field_orderable() {
        return false;
    }
    if plan.scalar_plan().distinct {
        return false;
    }
    let offset = usize::try_from(crate::db::cursor::effective_page_offset_for_window(
        plan, false,
    ))
    .unwrap_or(usize::MAX);
    if offset > 0 {
        return false;
    }

    let access_shape_facts = plan.access_shape_facts();
    if access_shape_facts.is_composite() {
        return false;
    }
    if !field_extrema_target_has_matching_index(plan, aggregate) {
        return false;
    }
    if matches!(direction, Direction::Desc)
        && !access_shape_facts.all_paths_support_reverse_traversal()
    {
        return false;
    }
    if plan
        .scalar_plan()
        .page
        .as_ref()
        .is_some_and(|page| page.limit.is_some())
    {
        return false;
    }

    true
}

fn field_extrema_target_has_matching_index(
    plan: &AccessPlannedQuery,
    aggregate: AggregateRouteShape<'_>,
) -> bool {
    let access_shape_facts = plan.access_shape_facts();
    let Some(path_facts) = access_shape_facts.single_path_facts() else {
        return false;
    };
    if aggregate.target_field_is_primary_key() {
        return primary_key_stream_window_shape_supported(&path_facts);
    }
    let Some(target_field) = aggregate.target_field() else {
        return false;
    };
    access_shape_facts
        .single_path_index_prefix_details()
        .or_else(|| access_shape_facts.single_path_index_range_details())
        .is_some_and(|details| {
            details
                .first_key_field()
                .is_some_and(|field| field == target_field)
        })
}
