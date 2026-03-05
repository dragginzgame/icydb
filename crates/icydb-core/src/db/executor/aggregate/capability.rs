//! Module: executor::aggregate::capability
//! Responsibility: aggregate field-kind capability predicates.
//! Does not own: route planning policy or aggregate execution dispatch.
//! Boundary: reusable field capability checks for aggregate/route modules.

use crate::{
    db::{
        direction::Direction,
        executor::aggregate::AggregateKind,
        numeric::field_kind_supports_aggregate_numeric,
        query::{
            builder::AggregateExpr,
            plan::{AccessPlannedQuery, DistinctExecutionStrategy},
        },
    },
    model::{entity::resolve_field_slot, field::FieldKind},
    traits::EntityKind,
};

/// Return true when the field kind is eligible for deterministic aggregate ordering.
#[must_use]
pub(in crate::db::executor) const fn field_kind_supports_aggregate_ordering(
    kind: &FieldKind,
) -> bool {
    match kind {
        FieldKind::Account
        | FieldKind::Bool
        | FieldKind::Date
        | FieldKind::Decimal { .. }
        | FieldKind::Duration
        | FieldKind::Enum { .. }
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int
        | FieldKind::Int128
        | FieldKind::IntBig
        | FieldKind::Principal
        | FieldKind::Subaccount
        | FieldKind::Text
        | FieldKind::Timestamp
        | FieldKind::Uint
        | FieldKind::Uint128
        | FieldKind::UintBig
        | FieldKind::Ulid
        | FieldKind::Unit => true,
        FieldKind::Relation { key_kind, .. } => field_kind_supports_aggregate_ordering(key_kind),
        FieldKind::Blob
        | FieldKind::List(_)
        | FieldKind::Set(_)
        | FieldKind::Map { .. }
        | FieldKind::Structured { .. } => false,
    }
}

/// Return true when the field kind supports numeric aggregate arithmetic.
#[must_use]
pub(in crate::db::executor) const fn field_kind_supports_numeric_aggregation(
    kind: &FieldKind,
) -> bool {
    field_kind_supports_aggregate_numeric(kind)
}

#[must_use]
/// Return whether the named field supports deterministic aggregate ordering.
pub(in crate::db::executor) fn field_is_orderable<E: EntityKind>(field: &str) -> bool {
    let Some(field_model) = E::MODEL
        .fields
        .iter()
        .find(|candidate| candidate.name == field)
    else {
        return false;
    };

    field_kind_supports_aggregate_ordering(&field_model.kind)
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

///
/// AggregateExecutionPolicyInputs
///
/// Aggregate-policy derivation inputs computed by the route/planning boundary.
/// This keeps residual-filter/order-sort signals explicit while aggregate
/// policy owns the resulting aggregate execution capability contract.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct AggregateExecutionPolicyInputs {
    has_residual_filter: bool,
    requires_post_access_sort: bool,
}

impl AggregateExecutionPolicyInputs {
    /// Construct aggregate-policy derivation inputs.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        has_residual_filter: bool,
        requires_post_access_sort: bool,
    ) -> Self {
        Self {
            has_residual_filter,
            requires_post_access_sort,
        }
    }

    #[must_use]
    const fn has_residual_filter(self) -> bool {
        self.has_residual_filter
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
    count_pushdown_access_shape_supported: bool,
    composite_aggregate_fast_path_eligible: bool,
    field_min_fast_path: AggregateFieldExtremaEligibility,
    field_max_fast_path: AggregateFieldExtremaEligibility,
}

impl AggregateExecutionPolicy {
    #[must_use]
    pub(in crate::db::executor) const fn count_pushdown_access_shape_supported(self) -> bool {
        self.count_pushdown_access_shape_supported
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
pub(in crate::db::executor) fn derive_aggregate_execution_policy<E>(
    plan: &AccessPlannedQuery<E::Key>,
    direction: Direction,
    aggregate_expr: Option<&AggregateExpr>,
    inputs: AggregateExecutionPolicyInputs,
) -> AggregateExecutionPolicy
where
    E: EntityKind,
{
    let access_class = plan.to_executable().class();
    let field_min_fast_path = assess_field_extrema_fast_path_eligibility::<E>(
        plan,
        direction,
        aggregate_expr,
        AggregateKind::Min,
    );
    let field_max_fast_path = assess_field_extrema_fast_path_eligibility::<E>(
        plan,
        direction,
        aggregate_expr,
        AggregateKind::Max,
    );

    AggregateExecutionPolicy {
        count_pushdown_access_shape_supported: access_class
            .single_path_supports_count_pushdown_shape(),
        composite_aggregate_fast_path_eligible: access_class.composite()
            && !inputs.has_residual_filter()
            && !inputs.requires_post_access_sort(),
        field_min_fast_path,
        field_max_fast_path,
    }
}

/// Derive aggregate-policy field-extrema fast-path eligibility for one plan.
#[expect(clippy::too_many_lines)]
pub(in crate::db::executor) fn assess_field_extrema_fast_path_eligibility<E>(
    plan: &AccessPlannedQuery<E::Key>,
    direction: Direction,
    aggregate_expr: Option<&AggregateExpr>,
    extrema_kind: AggregateKind,
) -> AggregateFieldExtremaEligibility
where
    E: EntityKind,
{
    let Some(aggregate) = aggregate_expr else {
        return AggregateFieldExtremaEligibility {
            eligible: false,
            ineligibility_reason: Some(AggregateFieldExtremaIneligibilityReason::SpecMissing),
        };
    };
    if aggregate.kind() != extrema_kind {
        return AggregateFieldExtremaEligibility {
            eligible: false,
            ineligibility_reason: Some(
                AggregateFieldExtremaIneligibilityReason::AggregateKindMismatch,
            ),
        };
    }
    let Some(target_field) = aggregate.target_field() else {
        return AggregateFieldExtremaEligibility {
            eligible: false,
            ineligibility_reason: Some(
                AggregateFieldExtremaIneligibilityReason::TargetFieldMissing,
            ),
        };
    };
    if resolve_field_slot(E::MODEL, target_field).is_none() {
        return AggregateFieldExtremaEligibility {
            eligible: false,
            ineligibility_reason: Some(
                AggregateFieldExtremaIneligibilityReason::UnknownTargetField,
            ),
        };
    }
    if !field_is_orderable::<E>(target_field) {
        return AggregateFieldExtremaEligibility {
            eligible: false,
            ineligibility_reason: Some(
                AggregateFieldExtremaIneligibilityReason::UnsupportedFieldType,
            ),
        };
    }
    if !matches!(
        plan.distinct_execution_strategy(),
        DistinctExecutionStrategy::None
    ) {
        return AggregateFieldExtremaEligibility {
            eligible: false,
            ineligibility_reason: Some(
                AggregateFieldExtremaIneligibilityReason::DistinctNotSupported,
            ),
        };
    }
    let offset = usize::try_from(
        crate::db::executor::traversal::effective_page_offset_for_window(plan, false),
    )
    .unwrap_or(usize::MAX);
    if offset > 0 {
        return AggregateFieldExtremaEligibility {
            eligible: false,
            ineligibility_reason: Some(
                AggregateFieldExtremaIneligibilityReason::OffsetNotSupported,
            ),
        };
    }

    let access_class = plan.to_executable().class();
    if access_class.composite() {
        return AggregateFieldExtremaEligibility {
            eligible: false,
            ineligibility_reason: Some(
                AggregateFieldExtremaIneligibilityReason::CompositePathNotSupported,
            ),
        };
    }
    if !field_extrema_target_has_matching_index::<E>(plan, target_field) {
        return AggregateFieldExtremaEligibility {
            eligible: false,
            ineligibility_reason: Some(AggregateFieldExtremaIneligibilityReason::NoMatchingIndex),
        };
    }
    if matches!(direction, Direction::Desc) && !access_class.reverse_supported() {
        return AggregateFieldExtremaEligibility {
            eligible: false,
            ineligibility_reason: Some(
                AggregateFieldExtremaIneligibilityReason::DescReverseTraversalNotSupported,
            ),
        };
    }
    if plan
        .scalar_plan()
        .page
        .as_ref()
        .is_some_and(|page| page.limit.is_some())
    {
        return AggregateFieldExtremaEligibility {
            eligible: false,
            ineligibility_reason: Some(
                AggregateFieldExtremaIneligibilityReason::PageLimitNotSupported,
            ),
        };
    }

    AggregateFieldExtremaEligibility {
        eligible: true,
        ineligibility_reason: None,
    }
}

fn field_extrema_target_has_matching_index<E>(
    plan: &AccessPlannedQuery<E::Key>,
    target_field: &str,
) -> bool
where
    E: EntityKind,
{
    let access_class = plan.to_executable().class();
    if !access_class.single_path() {
        return false;
    }
    if target_field == E::MODEL.primary_key.name {
        return access_class.single_path_supports_pk_stream_access();
    }
    access_class
        .single_path_index_prefix_details()
        .or_else(|| access_class.single_path_index_range_details())
        .is_some_and(|(index, _)| {
            index
                .fields
                .first()
                .is_some_and(|field| field == &target_field)
        })
}
