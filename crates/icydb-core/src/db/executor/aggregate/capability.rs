//! Module: executor::aggregate::capability
//! Responsibility: aggregate field-kind capability predicates.
//! Does not own: route planning policy or aggregate execution dispatch.
//! Boundary: reusable field capability checks for aggregate/route modules.

use crate::{
    db::{
        direction::Direction,
        executor::{aggregate::AggregateKind, route::AggregateRouteShape},
        numeric::field_kind_supports_aggregate_numeric,
        query::plan::{AccessPlannedQuery, expr::classify_field_kind},
    },
    model::{field::FieldKind, index::IndexModel},
};

/// Return true when the field kind is eligible for deterministic aggregate ordering.
#[must_use]
pub(in crate::db::executor) const fn field_kind_supports_aggregate_ordering(
    kind: &FieldKind,
) -> bool {
    classify_field_kind(kind).supports_aggregate_ordering()
}

/// Return true when the field kind supports numeric aggregate arithmetic.
#[must_use]
pub(in crate::db::executor) const fn field_kind_supports_numeric_aggregation(
    kind: &FieldKind,
) -> bool {
    field_kind_supports_aggregate_numeric(kind)
}

///
/// AggregateFieldExtremaIneligibilityReason
///
/// Aggregate-policy reason taxonomy for field-extrema fast-path ineligibility.
/// Route surfaces these reasons for diagnostics but does not own derivation.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum AggregateFieldExtremaIneligibilityReason {
    SpecMissing,
    AggregateKindMismatch,
    TargetFieldMissing,
    UnknownTargetField,
    UnsupportedFieldType,
    DistinctNotSupported,
    PageLimitNotSupported,
    OffsetNotSupported,
    CompositePathNotSupported,
    NoMatchingIndex,
    DescReverseTraversalNotSupported,
}

///
/// AggregateFieldExtremaEligibility
///
/// Aggregate-policy eligibility snapshot for one field-extrema terminal shape.
/// Carries the boolean decision and first ineligibility reason.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct AggregateFieldExtremaEligibility {
    pub(in crate::db::executor) eligible: bool,
    pub(in crate::db::executor) ineligibility_reason:
        Option<AggregateFieldExtremaIneligibilityReason>,
}

impl AggregateFieldExtremaEligibility {
    // Build the canonical ineligible field-extrema policy shape from one
    // aggregate-owned rejection reason.
    const fn ineligible(reason: AggregateFieldExtremaIneligibilityReason) -> Self {
        Self {
            eligible: false,
            ineligibility_reason: Some(reason),
        }
    }

    // Build the canonical eligible field-extrema policy shape.
    const fn eligible() -> Self {
        Self {
            eligible: true,
            ineligibility_reason: None,
        }
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct AggregateExecutionPolicy {
    count_pushdown_shape_supported: bool,
    composite_aggregate_fast_path_eligible: bool,
    field_min_fast_path: AggregateFieldExtremaEligibility,
    field_max_fast_path: AggregateFieldExtremaEligibility,
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
    pub(in crate::db::executor) const fn field_min_fast_path(
        self,
    ) -> AggregateFieldExtremaEligibility {
        self.field_min_fast_path
    }

    #[must_use]
    pub(in crate::db::executor) const fn field_max_fast_path(
        self,
    ) -> AggregateFieldExtremaEligibility {
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
    let access_capabilities = plan.access_strategy().capabilities();
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
        count_pushdown_shape_supported: access_capabilities
            .single_path_supports_count_pushdown_shape(),
        composite_aggregate_fast_path_eligible: access_capabilities.is_composite()
            && !inputs.residual_filter_present()
            && !inputs.requires_post_access_sort(),
        field_min_fast_path,
        field_max_fast_path,
    }
}

/// Derive aggregate-policy field-extrema fast-path eligibility for one plan.
pub(in crate::db::executor) fn assess_field_extrema_fast_path_eligibility(
    plan: &AccessPlannedQuery,
    direction: Direction,
    aggregate_shape: Option<AggregateRouteShape<'_>>,
    extrema_kind: AggregateKind,
) -> AggregateFieldExtremaEligibility {
    let Some(aggregate) = aggregate_shape else {
        return AggregateFieldExtremaEligibility::ineligible(
            AggregateFieldExtremaIneligibilityReason::SpecMissing,
        );
    };
    if aggregate.kind() != extrema_kind {
        return AggregateFieldExtremaEligibility::ineligible(
            AggregateFieldExtremaIneligibilityReason::AggregateKindMismatch,
        );
    }
    let Some(_target_field) = aggregate.target_field() else {
        return AggregateFieldExtremaEligibility::ineligible(
            AggregateFieldExtremaIneligibilityReason::TargetFieldMissing,
        );
    };
    if !aggregate.target_field_known() {
        return AggregateFieldExtremaEligibility::ineligible(
            AggregateFieldExtremaIneligibilityReason::UnknownTargetField,
        );
    }
    if !aggregate.target_field_orderable() {
        return AggregateFieldExtremaEligibility::ineligible(
            AggregateFieldExtremaIneligibilityReason::UnsupportedFieldType,
        );
    }
    if plan.scalar_plan().distinct {
        return AggregateFieldExtremaEligibility::ineligible(
            AggregateFieldExtremaIneligibilityReason::DistinctNotSupported,
        );
    }
    let offset = usize::try_from(crate::db::cursor::effective_page_offset_for_window(
        plan, false,
    ))
    .unwrap_or(usize::MAX);
    if offset > 0 {
        return AggregateFieldExtremaEligibility::ineligible(
            AggregateFieldExtremaIneligibilityReason::OffsetNotSupported,
        );
    }

    let access_capabilities = plan.access_strategy().capabilities();
    if access_capabilities.is_composite() {
        return AggregateFieldExtremaEligibility::ineligible(
            AggregateFieldExtremaIneligibilityReason::CompositePathNotSupported,
        );
    }
    if !field_extrema_target_has_matching_index(plan, aggregate) {
        return AggregateFieldExtremaEligibility::ineligible(
            AggregateFieldExtremaIneligibilityReason::NoMatchingIndex,
        );
    }
    if matches!(direction, Direction::Desc)
        && !access_capabilities.all_paths_support_reverse_traversal()
    {
        return AggregateFieldExtremaEligibility::ineligible(
            AggregateFieldExtremaIneligibilityReason::DescReverseTraversalNotSupported,
        );
    }
    if plan
        .scalar_plan()
        .page
        .as_ref()
        .is_some_and(|page| page.limit.is_some())
    {
        return AggregateFieldExtremaEligibility::ineligible(
            AggregateFieldExtremaIneligibilityReason::PageLimitNotSupported,
        );
    }

    AggregateFieldExtremaEligibility::eligible()
}

fn field_extrema_target_has_matching_index(
    plan: &AccessPlannedQuery,
    aggregate: AggregateRouteShape<'_>,
) -> bool {
    let access_capabilities = plan.access_strategy().capabilities();
    if !access_capabilities.is_single_path() {
        return false;
    }
    if aggregate.target_field_is_primary_key() {
        return access_capabilities.single_path_supports_pk_stream_access();
    }
    let Some(target_field) = aggregate.target_field() else {
        return false;
    };
    access_capabilities
        .single_path_index_prefix_details()
        .or_else(|| access_capabilities.single_path_index_range_details())
        .is_some_and(|details| {
            details
                .index()
                .fields()
                .first()
                .is_some_and(|field| field == &target_field)
        })
}

/// Return whether one aggregate field target is the entity primary key.
#[must_use]
pub(in crate::db::executor) const fn field_target_is_primary_key(
    aggregate: AggregateRouteShape<'_>,
) -> bool {
    aggregate.target_field_is_primary_key()
}

/// Return whether one field-target MAX probe can be treated as tie-free.
/// Tie-free means:
/// - target is the primary key, or
/// - target is backed by one unique single-field index.
#[must_use]
pub(in crate::db::executor) fn field_target_is_tie_free_probe_target(
    aggregate: AggregateRouteShape<'_>,
    index_model: Option<IndexModel>,
) -> bool {
    field_target_is_primary_key(aggregate)
        || aggregate.target_field().is_some_and(|target_field| {
            field_target_is_unique_single_field_index_head(target_field, index_model)
        })
}

fn field_target_is_unique_single_field_index_head(
    target_field: &str,
    index_model: Option<IndexModel>,
) -> bool {
    index_model.is_some_and(|index_model| {
        index_model.is_unique()
            && index_model.fields().len() == 1
            && index_model
                .fields()
                .first()
                .is_some_and(|field| *field == target_field)
    })
}
