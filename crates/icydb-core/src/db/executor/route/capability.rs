use crate::{
    db::{
        access::{AccessPath, AccessPlan, PushdownApplicability},
        direction::Direction,
        executor::{
            ExecutionKernel,
            aggregate::capability::field_is_orderable,
            aggregate::{AggregateKind, AggregateSpec},
            load::LoadExecutor,
        },
        query::plan::{AccessPlannedQuery, derive_secondary_pushdown_applicability_validated},
    },
    model::entity::resolve_field_slot,
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::route::{
    ExecutionRoutePlan, FieldExtremaEligibility, FieldExtremaIneligibilityReason, RouteCapabilities,
};

/// Return true when this access path is eligible for PK stream fast-path execution.
#[must_use]
pub(in crate::db::executor) const fn supports_pk_stream_access_path<K>(
    path: &AccessPath<K>,
) -> bool {
    matches!(path, AccessPath::FullScan | AccessPath::KeyRange { .. })
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
    // Route-owned bridge for validated secondary ORDER BY pushdown applicability.
    pub(in crate::db::executor::route) fn derive_secondary_pushdown_applicability(
        plan: &AccessPlannedQuery<E::Key>,
    ) -> PushdownApplicability {
        derive_secondary_pushdown_applicability_validated(E::MODEL, plan)
    }

    // Derive a canonical route capability snapshot for one plan + direction.
    pub(in crate::db::executor::route) fn derive_route_capabilities(
        plan: &AccessPlannedQuery<E::Key>,
        direction: Direction,
        aggregate_spec: Option<&AggregateSpec>,
    ) -> RouteCapabilities {
        let field_min_eligibility =
            Self::assess_field_min_fast_path_eligibility(plan, direction, aggregate_spec);
        let field_max_eligibility =
            Self::assess_field_max_fast_path_eligibility(plan, direction, aggregate_spec);

        RouteCapabilities {
            streaming_access_shape_safe: ExecutionKernel::is_streaming_access_shape_safe::<E, _>(
                plan,
            ),
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
        aggregate_spec: Option<&AggregateSpec>,
    ) -> FieldExtremaEligibility {
        Self::assess_field_extrema_fast_path_eligibility(
            plan,
            direction,
            aggregate_spec,
            AggregateKind::Min,
        )
    }

    // Placeholder assessment for future `max(field)` fast paths.
    // Intentionally ineligible in 0.34.x while field-extrema semantics are finalized.
    fn assess_field_max_fast_path_eligibility(
        plan: &AccessPlannedQuery<E::Key>,
        direction: Direction,
        aggregate_spec: Option<&AggregateSpec>,
    ) -> FieldExtremaEligibility {
        Self::assess_field_extrema_fast_path_eligibility(
            plan,
            direction,
            aggregate_spec,
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
        aggregate_spec: Option<&AggregateSpec>,
        extrema_kind: AggregateKind,
    ) -> FieldExtremaEligibility {
        let Some(spec) = aggregate_spec else {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(FieldExtremaIneligibilityReason::SpecMissing),
            };
        };
        if spec.kind() != extrema_kind {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(FieldExtremaIneligibilityReason::AggregateKindMismatch),
            };
        }
        let Some(target_field) = spec.target_field() else {
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
        if logical.distinct {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(FieldExtremaIneligibilityReason::DistinctNotSupported),
            };
        }
        let offset = usize::try_from(ExecutionKernel::effective_page_offset(plan, None))
            .unwrap_or(usize::MAX);
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
        let Some(path) = plan.access.as_path() else {
            return false;
        };
        if target_field == E::MODEL.primary_key.name {
            return matches!(path, AccessPath::FullScan | AccessPath::KeyRange { .. });
        }

        match path {
            AccessPath::IndexPrefix { index, .. } => index
                .fields
                .first()
                .is_some_and(|field| *field == target_field),
            AccessPath::IndexRange { spec } => spec
                .index()
                .fields
                .first()
                .is_some_and(|field| *field == target_field),
            AccessPath::ByKey(_)
            | AccessPath::ByKeys(_)
            | AccessPath::KeyRange { .. }
            | AccessPath::FullScan => false,
        }
    }

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
        match access {
            AccessPlan::Path(path) => Self::path_supports_reverse_traversal(path),
            AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
                children.iter().all(Self::access_supports_reverse_traversal)
            }
        }
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

        let metadata = ExecutionKernel::budget_safety_metadata::<E, _>(plan);
        if metadata.has_residual_filter {
            return false;
        }
        if metadata.requires_post_access_sort {
            return false;
        }

        true
    }

    pub(super) const fn path_supports_reverse_traversal(path: &AccessPath<E::Key>) -> bool {
        matches!(
            path,
            AccessPath::ByKey(_)
                | AccessPath::KeyRange { .. }
                | AccessPath::IndexPrefix { .. }
                | AccessPath::IndexRange { .. }
                | AccessPath::FullScan
        )
    }

    pub(super) const fn is_composite_access_shape(access: &AccessPlan<E::Key>) -> bool {
        matches!(access, AccessPlan::Union(_) | AccessPlan::Intersection(_))
    }

    // Route-owned shape gate for index-range limited pushdown eligibility.
    pub(super) fn is_index_range_limit_pushdown_shape_eligible(
        plan: &AccessPlannedQuery<E::Key>,
    ) -> bool {
        let Some((index, prefix, _, _)) = plan.access.as_index_range_path() else {
            return false;
        };
        let index_fields = index.fields;
        let prefix_len = prefix.len();

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
