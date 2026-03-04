//! Module: executor::route::capability
//! Responsibility: derive route capability snapshots from executable plans.
//! Does not own: fast-path execution dispatch or post-access kernel behavior.
//! Boundary: capability and eligibility helpers for route planning.

use crate::{
    db::{
        access::{AccessPlan, lower_executable_access_plan},
        direction::Direction,
        executor::{
            ExecutableAccessPath, aggregate::AggregateKind,
            aggregate::capability::field_is_orderable, derive_access_capabilities,
            derive_access_path_capabilities, load::LoadExecutor,
            traversal::effective_page_offset_for_window,
        },
        query::builder::AggregateExpr,
        query::plan::{AccessPlannedQuery, DistinctExecutionStrategy},
    },
    model::entity::resolve_field_slot,
    traits::{EntityKind, EntitySchema, EntityValue},
};

use crate::db::executor::route::{
    ExecutionRoutePlan, FieldExtremaEligibility, FieldExtremaIneligibilityReason, RouteCapabilities,
};

/// Return true when this executable access path is eligible for PK stream fast-path execution.
#[must_use]
pub(in crate::db::executor) const fn supports_pk_stream_access_executable_path<K>(
    path: &ExecutableAccessPath<'_, K>,
) -> bool {
    derive_access_path_capabilities(path).supports_pk_stream_access()
}

/// Return bounded primary-scan fetch hints for executable path mechanics only.
pub(in crate::db::executor) const fn primary_scan_fetch_hint_for_executable_access_path<K>(
    path: &ExecutableAccessPath<'_, K>,
    physical_fetch_hint: Option<usize>,
) -> Option<usize> {
    if derive_access_path_capabilities(path).supports_primary_scan_fetch_hint() {
        physical_fetch_hint
    } else {
        None
    }
}

/// Derive budget-safety flags for one plan at the route capability boundary.
pub(in crate::db::executor) fn derive_budget_safety_flags<E, K>(
    plan: &AccessPlannedQuery<K>,
) -> (bool, bool, bool)
where
    E: EntitySchema<Key = K>,
{
    let logical = plan.scalar_plan();
    let has_residual_filter = logical.predicate.is_some();
    let access_order_satisfied_by_path = access_order_satisfied_by_path::<E, K>(plan);
    let has_order = logical
        .order
        .as_ref()
        .is_some_and(|order| !order.fields.is_empty());
    let requires_post_access_sort = has_order && !access_order_satisfied_by_path;

    (
        has_residual_filter,
        access_order_satisfied_by_path,
        requires_post_access_sort,
    )
}

/// Return whether one plan shape is safe for direct streaming execution.
pub(in crate::db::executor) fn streaming_access_shape_safe<E, K>(
    plan: &AccessPlannedQuery<K>,
) -> bool
where
    E: EntitySchema<Key = K>,
{
    if !plan.scalar_plan().mode.is_load() {
        return false;
    }

    let (has_residual_filter, _, requires_post_access_sort) =
        derive_budget_safety_flags::<E, K>(plan);
    if has_residual_filter {
        return false;
    }
    if requires_post_access_sort {
        return false;
    }

    true
}

fn access_order_satisfied_by_path<E, K>(plan: &AccessPlannedQuery<K>) -> bool
where
    E: EntitySchema<Key = K>,
{
    let Some(order) = plan.scalar_plan().order.as_ref() else {
        return false;
    };
    if order.fields.len() != 1 {
        return false;
    }
    if order.fields[0].0 != E::MODEL.primary_key.name {
        return false;
    }

    access_stream_is_pk_ordered(&plan.access)
}

fn access_stream_is_pk_ordered<K>(access: &AccessPlan<K>) -> bool {
    let executable = lower_executable_access_plan(access);
    let access_capabilities = derive_access_capabilities(&executable);

    access_capabilities.all_paths_pk_ordered_stream()
}

/// Return true when bounded physical fetch hints are valid for this direction.
pub(in crate::db::executor::route) const fn direction_allows_physical_fetch_hint(
    direction: Direction,
    desc_physical_reverse_supported: bool,
) -> bool {
    !matches!(direction, Direction::Desc) || desc_physical_reverse_supported
}

impl ExecutionRoutePlan {
    // Return the effective physical fetch hint for fallback stream resolution.
    // DESC fallback must disable bounded hints when reverse traversal is unavailable.
    pub(in crate::db::executor) const fn fallback_physical_fetch_hint(
        &self,
        direction: Direction,
    ) -> Option<usize> {
        if direction_allows_physical_fetch_hint(direction, self.desc_physical_reverse_supported()) {
            self.scan_hints.physical_fetch_hint
        } else {
            None
        }
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Derive one canonical route capability snapshot for a plan + direction.
    pub(in crate::db::executor::route) fn derive_route_capabilities(
        plan: &AccessPlannedQuery<E::Key>,
        direction: Direction,
        aggregate_expr: Option<&AggregateExpr>,
    ) -> RouteCapabilities {
        let field_min_eligibility =
            Self::assess_field_min_fast_path_eligibility(plan, direction, aggregate_expr);
        let field_max_eligibility =
            Self::assess_field_max_fast_path_eligibility(plan, direction, aggregate_expr);

        RouteCapabilities {
            streaming_access_shape_safe: streaming_access_shape_safe::<E, _>(plan),
            pk_order_fast_path_eligible: Self::pk_order_stream_fast_path_shape_supported(plan),
            desc_physical_reverse_supported: Self::is_desc_physical_reverse_traversal_supported(
                &plan.access,
                direction,
            ),
            count_pushdown_access_shape_supported: Self::count_pushdown_access_shape_supported(
                &plan.access,
            ),
            index_range_limit_pushdown_shape_eligible:
                Self::is_index_range_limit_pushdown_shape_eligible(plan),
            composite_aggregate_fast_path_eligible: Self::is_composite_aggregate_fast_path_eligible(
                plan,
            ),
            bounded_probe_hint_safe: Self::bounded_probe_hint_is_safe(plan),
            field_min_fast_path_eligible: field_min_eligibility.eligible,
            field_max_fast_path_eligible: field_max_eligibility.eligible,
            field_min_fast_path_ineligibility_reason: field_min_eligibility.ineligibility_reason,
            field_max_fast_path_ineligibility_reason: field_max_eligibility.ineligibility_reason,
        }
    }

    // Placeholder assessment for future `min(field)` fast paths.
    // Intentionally ineligible in 0.34.x while field-extrema semantics are finalized.
    fn assess_field_min_fast_path_eligibility(
        plan: &AccessPlannedQuery<E::Key>,
        direction: Direction,
        aggregate_expr: Option<&AggregateExpr>,
    ) -> FieldExtremaEligibility {
        Self::assess_field_extrema_fast_path_eligibility(
            plan,
            direction,
            aggregate_expr,
            AggregateKind::Min,
        )
    }

    // Placeholder assessment for future `max(field)` fast paths.
    // Intentionally ineligible in 0.34.x while field-extrema semantics are finalized.
    fn assess_field_max_fast_path_eligibility(
        plan: &AccessPlannedQuery<E::Key>,
        direction: Direction,
        aggregate_expr: Option<&AggregateExpr>,
    ) -> FieldExtremaEligibility {
        Self::assess_field_extrema_fast_path_eligibility(
            plan,
            direction,
            aggregate_expr,
            AggregateKind::Max,
        )
    }

    // Shared scaffolding for future field-extrema eligibility routing.
    // Contract:
    // - field-extrema fast path is enabled only for index-leading
    //   access shapes with full-window semantics.
    // - unsupported shapes return explicit route-owned reasons.
    fn assess_field_extrema_fast_path_eligibility(
        plan: &AccessPlannedQuery<E::Key>,
        direction: Direction,
        aggregate_expr: Option<&AggregateExpr>,
        extrema_kind: AggregateKind,
    ) -> FieldExtremaEligibility {
        let Some(aggregate) = aggregate_expr else {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(FieldExtremaIneligibilityReason::SpecMissing),
            };
        };
        if aggregate.kind() != extrema_kind {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(FieldExtremaIneligibilityReason::AggregateKindMismatch),
            };
        }
        let Some(target_field) = aggregate.target_field() else {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(FieldExtremaIneligibilityReason::TargetFieldMissing),
            };
        };
        if resolve_field_slot(E::MODEL, target_field).is_none() {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(FieldExtremaIneligibilityReason::UnknownTargetField),
            };
        }
        if !field_is_orderable::<E>(target_field) {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(FieldExtremaIneligibilityReason::UnsupportedFieldType),
            };
        }
        let logical = plan.scalar_plan();
        if !matches!(
            plan.distinct_execution_strategy(),
            DistinctExecutionStrategy::None
        ) {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(FieldExtremaIneligibilityReason::DistinctNotSupported),
            };
        }
        let offset =
            usize::try_from(effective_page_offset_for_window(plan, false)).unwrap_or(usize::MAX);
        if offset > 0 {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(FieldExtremaIneligibilityReason::OffsetNotSupported),
            };
        }
        if Self::is_composite_access_shape(&plan.access) {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(
                    FieldExtremaIneligibilityReason::CompositePathNotSupported,
                ),
            };
        }
        if !Self::field_extrema_target_has_matching_index(plan, target_field) {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(FieldExtremaIneligibilityReason::NoMatchingIndex),
            };
        }
        if !direction_allows_physical_fetch_hint(
            direction,
            Self::is_desc_physical_reverse_traversal_supported(&plan.access, direction),
        ) {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(
                    FieldExtremaIneligibilityReason::DescReverseTraversalNotSupported,
                ),
            };
        }
        if logical
            .page
            .as_ref()
            .is_some_and(|page| page.limit.is_some())
        {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(FieldExtremaIneligibilityReason::PageLimitNotSupported),
            };
        }

        FieldExtremaEligibility {
            eligible: true,
            ineligibility_reason: None,
        }
    }

    fn field_extrema_target_has_matching_index(
        plan: &AccessPlannedQuery<E::Key>,
        target_field: &str,
    ) -> bool {
        let executable = plan.to_executable();
        let access_capabilities = derive_access_capabilities(&executable);
        let Some(single_path) = access_capabilities.single_path() else {
            return false;
        };
        if target_field == E::MODEL.primary_key.name {
            return single_path.supports_pk_stream_access();
        }
        single_path
            .index_prefix_model()
            .or_else(|| single_path.index_range_model())
            .is_some_and(|index| {
                index
                    .fields
                    .first()
                    .is_some_and(|field| field == &target_field)
            })
    }

    /// Return whether DESC physical reverse traversal is supported for this access shape.
    pub(super) fn is_desc_physical_reverse_traversal_supported(
        access: &AccessPlan<E::Key>,
        direction: Direction,
    ) -> bool {
        if !matches!(direction, Direction::Desc) {
            return false;
        }

        Self::access_supports_reverse_traversal(access)
    }

    fn access_supports_reverse_traversal(access: &AccessPlan<E::Key>) -> bool {
        let executable = lower_executable_access_plan(access);
        let access_capabilities = derive_access_capabilities(&executable);

        access_capabilities.all_paths_support_reverse_traversal()
    }

    // Composite aggregate fast-path eligibility must stay explicit:
    // - composite access shape only (`Union` / `Intersection`)
    // - no residual predicate filtering
    // - no post-access reordering
    pub(super) fn is_composite_aggregate_fast_path_eligible(
        plan: &AccessPlannedQuery<E::Key>,
    ) -> bool {
        if !Self::is_composite_access_shape(&plan.access) {
            return false;
        }

        let (has_residual_filter, _, requires_post_access_sort) =
            derive_budget_safety_flags::<E, _>(plan);
        if has_residual_filter {
            return false;
        }
        if requires_post_access_sort {
            return false;
        }

        true
    }

    pub(super) fn is_composite_access_shape(access: &AccessPlan<E::Key>) -> bool {
        let executable = lower_executable_access_plan(access);
        let access_capabilities = derive_access_capabilities(&executable);

        access_capabilities.is_composite()
    }

    // Route-owned shape gate for index-range limited pushdown eligibility.
    pub(super) fn is_index_range_limit_pushdown_shape_eligible(
        plan: &AccessPlannedQuery<E::Key>,
    ) -> bool {
        let executable = plan.to_executable();
        let access_capabilities = derive_access_capabilities(&executable);
        let Some(single_path) = access_capabilities.single_path() else {
            return false;
        };
        let Some(details) = single_path.index_range_details() else {
            return false;
        };
        let index = details.index();
        let prefix_len = details.slot_arity();
        let index_fields = index.fields;

        if let Some(order) = plan.scalar_plan().order.as_ref()
            && !order.fields.is_empty()
        {
            let Some(expected_direction) = order.fields.last().map(|(_, direction)| *direction)
            else {
                return false;
            };
            if order
                .fields
                .iter()
                .any(|(_, direction)| *direction != expected_direction)
            {
                return false;
            }

            let mut expected =
                Vec::with_capacity(index_fields.len().saturating_sub(prefix_len) + 1);
            expected.extend(index_fields.iter().skip(prefix_len).copied());
            expected.push(E::MODEL.primary_key.name);
            if order.fields.len() != expected.len() {
                return false;
            }
            if !order
                .fields
                .iter()
                .map(|(field, _)| field.as_str())
                .eq(expected)
            {
                return false;
            }
        }

        true
    }
}
