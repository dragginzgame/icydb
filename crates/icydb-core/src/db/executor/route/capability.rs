use crate::{
    db::{
        access::{
            AccessPath, AccessPlan, PushdownApplicability, SecondaryOrderPushdownEligibility,
            SecondaryOrderPushdownRejection,
        },
        direction::Direction,
        executor::{
            ExecutionKernel,
            aggregate::capability::field_is_orderable,
            aggregate::{AggregateKind, AggregateSpec},
            load::LoadExecutor,
        },
        plan::{AccessPlannedQuery, OrderDirection},
    },
    model::entity::{EntityModel, resolve_field_slot},
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

fn order_fields_as_direction_refs(
    order_fields: &[(String, OrderDirection)],
) -> Vec<(&str, Direction)> {
    order_fields
        .iter()
        .map(|(field, direction)| (field.as_str(), direction.as_direction()))
        .collect()
}

fn applicability_from_eligibility(
    eligibility: SecondaryOrderPushdownEligibility,
) -> PushdownApplicability {
    match eligibility {
        SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy
            | SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
        ) => PushdownApplicability::NotApplicable,
        other => PushdownApplicability::Applicable(other),
    }
}

fn match_secondary_order_pushdown_core(
    model: &EntityModel,
    order_fields: &[(&str, Direction)],
    index_name: &'static str,
    index_fields: &[&'static str],
    prefix_len: usize,
) -> SecondaryOrderPushdownEligibility {
    let Some((last_field, last_direction)) = order_fields.last() else {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy,
        );
    };
    if *last_field != model.primary_key.name {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::MissingPrimaryKeyTieBreak {
                field: model.primary_key.name.to_string(),
            },
        );
    }

    let expected_direction = *last_direction;
    for (field, direction) in order_fields {
        if *direction != expected_direction {
            return SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::MixedDirectionNotEligible {
                    field: (*field).to_string(),
                },
            );
        }
    }

    let actual_non_pk_len = order_fields.len().saturating_sub(1);
    let matches_expected_suffix = actual_non_pk_len
        == index_fields.len().saturating_sub(prefix_len)
        && order_fields
            .iter()
            .take(actual_non_pk_len)
            .map(|(field, _)| *field)
            .zip(index_fields.iter().skip(prefix_len).copied())
            .all(|(actual, expected)| actual == expected);
    let matches_expected_full = actual_non_pk_len == index_fields.len()
        && order_fields
            .iter()
            .take(actual_non_pk_len)
            .map(|(field, _)| *field)
            .zip(index_fields.iter().copied())
            .all(|(actual, expected)| actual == expected);
    if matches_expected_suffix || matches_expected_full {
        return SecondaryOrderPushdownEligibility::Eligible {
            index: index_name,
            prefix_len,
        };
    }

    SecondaryOrderPushdownEligibility::Rejected(
        SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
            index: index_name,
            prefix_len,
            expected_suffix: index_fields
                .iter()
                .skip(prefix_len)
                .map(|field| (*field).to_string())
                .collect(),
            expected_full: index_fields
                .iter()
                .map(|field| (*field).to_string())
                .collect(),
            actual: order_fields
                .iter()
                .take(actual_non_pk_len)
                .map(|(field, _)| (*field).to_string())
                .collect(),
        },
    )
}

fn assess_secondary_order_pushdown_for_applicable_shape(
    model: &EntityModel,
    order_fields: &[(&str, Direction)],
    index_name: &'static str,
    index_fields: &[&'static str],
    prefix_len: usize,
) -> SecondaryOrderPushdownEligibility {
    match_secondary_order_pushdown_core(model, order_fields, index_name, index_fields, prefix_len)
}

fn assess_secondary_order_pushdown_for_plan<K>(
    model: &EntityModel,
    order_fields: Option<&[(&str, Direction)]>,
    access_plan: &AccessPlan<K>,
) -> SecondaryOrderPushdownEligibility {
    let Some(order_fields) = order_fields else {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy,
        );
    };
    if order_fields.is_empty() {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy,
        );
    }

    let Some(access) = access_plan.as_path() else {
        if let Some((index, prefix_len)) = access_plan.first_index_range_details() {
            return SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                    index,
                    prefix_len,
                },
            );
        }

        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
        );
    };
    if let Some((index, values)) = access.as_index_prefix() {
        if values.len() > index.fields.len() {
            return SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                    prefix_len: values.len(),
                    index_field_len: index.fields.len(),
                },
            );
        }

        return assess_secondary_order_pushdown_for_applicable_shape(
            model,
            order_fields,
            index.name,
            index.fields,
            values.len(),
        );
    }
    if let Some((index, prefix_len)) = access.index_range_details() {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported { index, prefix_len },
        );
    }

    SecondaryOrderPushdownEligibility::Rejected(
        SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
    )
}

// Derive pushdown applicability from a plan already validated by planner + executor.
fn derive_secondary_pushdown_applicability_validated<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
) -> PushdownApplicability {
    debug_assert!(
        !matches!(plan.order.as_ref(), Some(order) if order.fields.is_empty()),
        "validated plan must not contain an empty ORDER BY specification",
    );
    let order_fields = plan
        .order
        .as_ref()
        .map(|order| order_fields_as_direction_refs(&order.fields));

    applicability_from_eligibility(assess_secondary_order_pushdown_for_plan(
        model,
        order_fields.as_deref(),
        &plan.access,
    ))
}
