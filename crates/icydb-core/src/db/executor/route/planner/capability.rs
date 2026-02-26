use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        direction::Direction,
        executor::{
            ExecutionKernel,
            aggregate_model::capability::field_is_orderable,
            aggregate_model::{AggregateKind, AggregateSpec},
            load::LoadExecutor,
        },
        plan::AccessPlannedQuery,
        query::predicate::PredicateFieldSlots,
    },
    model::entity::resolve_field_slot,
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::route::{
    FieldExtremaEligibility, FieldExtremaIneligibilityReason, RouteCapabilities,
    direction_allows_physical_fetch_hint,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
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
            streaming_access_shape_safe: plan.is_streaming_access_shape_safe::<E>(),
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

    // Determine whether every compiled predicate field slot is available on
    // the active single-path index access shape.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::db::executor) fn predicate_slots_fully_covered_by_index_path(
        access: &AccessPlan<E::Key>,
        predicate_slots: Option<&PredicateFieldSlots>,
    ) -> bool {
        let Some(predicate_slots) = predicate_slots else {
            return false;
        };
        let required = predicate_slots.required_slots();
        if required.is_empty() {
            return false;
        }
        let Some(mut index_slots) = Self::resolved_index_slots_for_access_path(access) else {
            return false;
        };
        index_slots.sort_unstable();
        index_slots.dedup();

        required
            .iter()
            .all(|slot| index_slots.binary_search(slot).is_ok())
    }

    // Resolve index fields for a single-path index access shape to entity slots.
    pub(in crate::db::executor) fn resolved_index_slots_for_access_path(
        access: &AccessPlan<E::Key>,
    ) -> Option<Vec<usize>> {
        let path = access.as_path()?;
        let index_fields = match path {
            AccessPath::IndexPrefix { index, .. } => index.fields,
            AccessPath::IndexRange { spec } => {
                let index = spec.index();
                index.fields
            }
            AccessPath::ByKey(_)
            | AccessPath::ByKeys(_)
            | AccessPath::KeyRange { .. }
            | AccessPath::FullScan => return None,
        };

        let mut slots = Vec::with_capacity(index_fields.len());
        for field_name in index_fields {
            let slot = resolve_field_slot(E::MODEL, field_name)?;
            slots.push(slot);
        }

        Some(slots)
    }

    // Placeholder assessment for future `min(field)` fast paths.
    // Intentionally ineligible in 0.24.x while field-extrema semantics are finalized.
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
    // Intentionally ineligible in 0.24.x while field-extrema semantics are finalized.
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
        if plan.distinct {
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
        if plan.page.as_ref().is_some_and(|page| page.limit.is_some()) {
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

        let metadata = plan.budget_safety_metadata::<E>();
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

        if let Some(order) = plan.order.as_ref()
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
