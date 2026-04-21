//! Module: db::query::plan::planner::predicate
//! Builds predicate-driven access plans from canonical predicate trees and
//! visible index metadata.

use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        predicate::Predicate,
        query::plan::{
            OrderSpec, PlannedNonIndexAccessReason,
            key_item_match::{eq_lookup_value_for_key_item, index_key_item_at},
            planner::{
                AndFamilyCandidateScore, AndFamilyPriorityClass, PlannedAccessSelection,
                and_family_candidate_score_outranks, candidate_satisfies_secondary_order, compare,
                index_literal_matches_schema, index_predicate_guarantees_compare, prefix, range,
            },
        },
        schema::SchemaInfo,
    },
    error::InternalError,
    model::{entity::EntityModel, index::IndexModel},
    value::Value,
};

#[expect(
    clippy::too_many_lines,
    reason = "planner predicate selection still centralizes the bounded family-routing policy in one owner-local entrypoint"
)]
pub(super) fn plan_predicate(
    model: &EntityModel,
    candidate_indexes: &[&'static IndexModel],
    schema: &SchemaInfo,
    predicate: &Predicate,
    order: Option<&OrderSpec>,
    grouped: bool,
) -> Result<PlannedAccessSelection, InternalError> {
    let plan = match predicate {
        Predicate::True
        | Predicate::False
        | Predicate::Not(_)
        | Predicate::CompareFields(_)
        | Predicate::IsNotNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => PlannedAccessSelection::new(
            AccessPlan::full_scan(),
            Some(PlannedNonIndexAccessReason::PlannerFullScanFallback),
        ),
        Predicate::IsNull { field } => {
            // Primary keys are always keyable and therefore never representable
            // as `Value::Null`; lower this impossible shape to an empty access
            // contract instead of scanning all rows.
            if field == model.primary_key.name
                && matches!(schema.field(field), Some(field_type) if field_type.is_keyable())
            {
                PlannedAccessSelection::new(
                    AccessPlan::by_keys(Vec::new()),
                    Some(PlannedNonIndexAccessReason::PlannerKeySetAccess),
                )
            } else {
                PlannedAccessSelection::new(
                    AccessPlan::full_scan(),
                    Some(PlannedNonIndexAccessReason::PlannerFullScanFallback),
                )
            }
        }
        Predicate::And(children) => {
            // Phase 1: derive the planner-owned secondary-index candidates once
            // so child recursion can reuse the chosen index contract for
            // redundancy stripping without reopening candidate extraction.
            let primary_key_range_access =
                range::primary_key_range_from_and(model, schema, children);
            let index_range_access = range::index_range_from_and(
                model,
                candidate_indexes,
                schema,
                children,
                order,
                grouped,
            );
            let prefix_access = prefix::index_prefix_from_and(
                model,
                candidate_indexes,
                schema,
                children,
                order,
                grouped,
            );

            // Phase 2: recurse into conjunctive children once while the
            // strongest secondary-index candidate is still available to strip
            // only the clauses that candidate already guarantees.
            let selected_index_access = index_range_access
                .as_ref()
                .map(|spec| AccessPlan::index_range(spec.clone()))
                .or_else(|| prefix_access.clone());
            let mut plans = children
                .iter()
                .filter(|child| {
                    !child_is_redundant_under_selected_index_access(
                        schema,
                        selected_index_access.as_ref(),
                        child,
                    )
                })
                .map(|child| {
                    plan_predicate(model, candidate_indexes, schema, child, order, grouped)
                        .map(PlannedAccessSelection::into_access)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let family_choice = choose_best_and_family_access(
                model,
                order,
                grouped,
                plans.as_slice(),
                selected_index_access.as_ref(),
                primary_key_range_access.as_ref(),
                index_range_access.as_ref(),
                prefix_access.as_ref(),
            );
            if let Some(family_choice) = family_choice {
                return Ok(family_choice);
            }

            if let Some(prefix) = prefix_access {
                plans.push(prefix);
            }
            if let Some(primary_key_range) = primary_key_range_access {
                plans.push(primary_key_range);
            }

            PlannedAccessSelection::new(
                AccessPlan::intersection(plans),
                Some(PlannedNonIndexAccessReason::PlannerCompositeNonIndex),
            )
        }
        Predicate::Or(children) => PlannedAccessSelection::new(
            AccessPlan::union(
                children
                    .iter()
                    .map(|child| {
                        plan_predicate(model, candidate_indexes, schema, child, order, grouped)
                            .map(PlannedAccessSelection::into_access)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Some(PlannedNonIndexAccessReason::PlannerCompositeNonIndex),
        ),
        Predicate::Compare(cmp) => {
            let access =
                compare::plan_compare(model, candidate_indexes, schema, cmp, order, grouped);

            PlannedAccessSelection::new(
                access.clone(),
                planned_non_index_reason_for_access(&access),
            )
        }
    };

    Ok(plan)
}

// Consolidate the existing `AND` family winner policy into one explicit
// comparison path so planner-family route choice does not depend on ad hoc
// early returns spread through the main recursion body.
#[expect(
    clippy::too_many_arguments,
    reason = "this helper intentionally freezes the current AND-family comparison inputs in one owner-local entrypoint"
)]
fn choose_best_and_family_access(
    model: &EntityModel,
    order: Option<&OrderSpec>,
    grouped: bool,
    child_plans: &[AccessPlan<Value>],
    selected_index_access: Option<&AccessPlan<Value>>,
    primary_key_range_access: Option<&AccessPlan<Value>>,
    index_range_access: Option<&crate::db::access::SemanticIndexRangeSpec>,
    prefix_access: Option<&AccessPlan<Value>>,
) -> Option<PlannedAccessSelection> {
    let mut chosen: Option<(AndFamilyCandidateScore, PlannedAccessSelection)> = None;

    let empty_child_access = has_explicit_empty_child_access(child_plans).then(|| {
        PlannedAccessSelection::new(
            AccessPlan::by_keys(Vec::new()),
            Some(PlannedNonIndexAccessReason::EmptyChildAccessPreferred),
        )
    });
    update_best_and_family_candidate(
        &mut chosen,
        empty_child_access,
        AndFamilyCandidateScore::new(AndFamilyPriorityClass::ExplicitEmpty, false, 0),
    );

    // Project primary-key child routes into explicit family candidates before
    // selection so contradictory singleton children no longer piggyback on the
    // generic key-set access label.
    if let Some((primary_key_child_access, conflicting_children)) =
        primary_key_child_access_candidate(child_plans)
    {
        update_best_and_family_candidate(
            &mut chosen,
            Some(primary_key_child_access),
            AndFamilyCandidateScore::new(
                if conflicting_children {
                    AndFamilyPriorityClass::ConflictingPrimaryKeyChildren
                } else {
                    AndFamilyPriorityClass::SingletonPrimaryKey
                },
                false,
                0,
            ),
        );
    }

    update_best_and_family_candidate(
        &mut chosen,
        primary_key_range_access.cloned().map(|access| {
            PlannedAccessSelection::new(
                access,
                Some(
                    if primary_key_range_access.is_some_and(|candidate| {
                        candidate_outranks_selected_access_on_required_order(
                            model,
                            order,
                            grouped,
                            candidate,
                            selected_index_access,
                        )
                    }) {
                        PlannedNonIndexAccessReason::RequiredOrderPrimaryKeyRangePreferred
                    } else {
                        PlannedNonIndexAccessReason::PlannerPrimaryKeyRange
                    },
                ),
            )
        }),
        AndFamilyCandidateScore::new(
            AndFamilyPriorityClass::Ordinary,
            primary_key_range_access.is_some_and(|candidate| {
                candidate_outranks_selected_access_on_required_order(
                    model,
                    order,
                    grouped,
                    candidate,
                    selected_index_access,
                )
            }),
            1,
        ),
    );

    update_best_and_family_candidate(
        &mut chosen,
        index_range_access
            .cloned()
            .map(AccessPlan::index_range)
            .map(|access| PlannedAccessSelection::new(access, None)),
        AndFamilyCandidateScore::new(AndFamilyPriorityClass::Ordinary, false, 3),
    );

    update_best_and_family_candidate(
        &mut chosen,
        prefix_access
            .cloned()
            .map(|access| PlannedAccessSelection::new(access, None)),
        AndFamilyCandidateScore::new(AndFamilyPriorityClass::Ordinary, false, 2),
    );

    chosen.map(|(_, access)| access)
}

// Keep family-candidate accumulation on one helper so the main `AND` planner
// body does not re-encode comparison precedence for each candidate source.
fn update_best_and_family_candidate(
    chosen: &mut Option<(AndFamilyCandidateScore, PlannedAccessSelection)>,
    candidate_access: Option<PlannedAccessSelection>,
    candidate_score: AndFamilyCandidateScore,
) {
    let Some(candidate_access) = candidate_access else {
        return;
    };

    match chosen {
        None => *chosen = Some((candidate_score, candidate_access)),
        Some((best_score, _))
            if and_family_candidate_score_outranks(candidate_score, *best_score) =>
        {
            *chosen = Some((candidate_score, candidate_access));
        }
        Some(_) => {}
    }
}

// One explicit empty child access route already proves the whole conjunction is
// unsatisfiable, so broader secondary-family candidates must not outrank it.
fn has_explicit_empty_child_access(children: &[AccessPlan<Value>]) -> bool {
    children.iter().any(AccessPlan::is_explicit_empty)
}

// Conjunctive child planning can already discover singleton primary-key access
// routes from direct `id = ?` / singleton `id IN (?)` clauses. That route is a
// stronger planner-visible candidate than any broader secondary index scan, so
// keep one owner-local reducer for this family-level preference.
fn primary_key_child_access_candidate(
    children: &[AccessPlan<Value>],
) -> Option<(PlannedAccessSelection, bool)> {
    let mut chosen_key: Option<&Value> = None;

    for child in children {
        let Some(path) = child.as_path() else {
            continue;
        };
        let Some(candidate_key) = path.as_by_key().or_else(|| match path.as_by_keys() {
            Some([key]) => Some(key),
            Some([..]) | None => None,
        }) else {
            continue;
        };

        match chosen_key {
            None => chosen_key = Some(candidate_key),
            Some(existing) if existing != candidate_key => {
                return Some((
                    PlannedAccessSelection::new(
                        AccessPlan::by_keys(Vec::new()),
                        Some(
                            PlannedNonIndexAccessReason::ConflictingPrimaryKeyChildrenAccessPreferred,
                        ),
                    ),
                    true,
                ));
            }
            Some(_) => {}
        }
    }

    chosen_key.cloned().map(|key| {
        (
            PlannedAccessSelection::new(
                AccessPlan::by_key(key),
                Some(PlannedNonIndexAccessReason::SingletonPrimaryKeyChildAccessPreferred),
            ),
            false,
        )
    })
}

// Map one planner-selected non-index access shape onto the bounded winner
// reason surface before explain or query-plan assembly consumes the route.
fn planned_non_index_reason_for_access(
    access: &AccessPlan<Value>,
) -> Option<PlannedNonIndexAccessReason> {
    if access.as_by_key_path().is_some() {
        return Some(PlannedNonIndexAccessReason::PlannerPrimaryKeyLookup);
    }
    if access.is_explicit_empty()
        || access
            .as_path()
            .and_then(|path| path.as_by_keys())
            .is_some()
    {
        return Some(PlannedNonIndexAccessReason::PlannerKeySetAccess);
    }
    if access.as_primary_key_range_path().is_some() {
        return Some(PlannedNonIndexAccessReason::PlannerPrimaryKeyRange);
    }
    if access.is_single_full_scan() {
        return Some(PlannedNonIndexAccessReason::PlannerFullScanFallback);
    }
    if access.selected_index_model().is_none() {
        return Some(PlannedNonIndexAccessReason::PlannerCompositeNonIndex);
    }

    None
}

// Prefer one planner-visible route over another only when the candidate keeps
// the required order and the selected competitor does not. This keeps family
// competition framed in terms of the shared ordering contract instead of one
// special-cased route name.
fn candidate_outranks_selected_access_on_required_order(
    model: &EntityModel,
    order: Option<&OrderSpec>,
    grouped: bool,
    candidate_access: &AccessPlan<Value>,
    selected_access: Option<&AccessPlan<Value>>,
) -> bool {
    let Some(selected_access) = selected_access else {
        return false;
    };

    let Some(order) = order else {
        return false;
    };

    access_preserves_required_order(model, order, grouped, candidate_access)
        && !access_preserves_required_order(model, order, grouped, selected_access)
}

// Reuse the same planner-owned ordering contract across family competition so
// secondary candidate ranking and family-level route preference do not drift.
fn access_preserves_required_order(
    model: &EntityModel,
    order: &OrderSpec,
    grouped: bool,
    access: &AccessPlan<Value>,
) -> bool {
    if grouped {
        return false;
    }
    if access.as_primary_key_range_path().is_some() {
        return order.is_primary_key_only(model.primary_key.name);
    }
    if let Some((index, prefix_values)) = access.as_index_prefix_path() {
        return candidate_satisfies_secondary_order(
            model,
            Some(order),
            index,
            prefix_values.len(),
            false,
        );
    }
    if let Some((index, prefix_values, _, _)) = access.as_index_range_path() {
        return candidate_satisfies_secondary_order(
            model,
            Some(order),
            index,
            prefix_values.len(),
            false,
        );
    }

    false
}

// Composite filtered/prefix planning can already guarantee some child compare
// clauses through either fixed equality prefix slots or the filtered guard on
// the chosen index. Those clauses should not contribute weaker nested access
// shapes once the selected path already proves them.
fn child_is_redundant_under_selected_index_access(
    schema: &SchemaInfo,
    selected_access: Option<&AccessPlan<Value>>,
    child: &Predicate,
) -> bool {
    let Some(AccessPlan::Path(path)) = selected_access else {
        return false;
    };
    let Predicate::Compare(cmp) = child else {
        return false;
    };

    if cmp.op == crate::db::predicate::CompareOp::Eq
        && selected_index_prefix_guarantees_eq_compare(schema, path.as_ref(), cmp)
    {
        return true;
    }

    path.as_ref()
        .selected_index_model()
        .is_some_and(|index| index_predicate_guarantees_compare(index, cmp))
}

// Selected index prefix and selected index range both carry an equality prefix
// that can already prove one compare predicate. Project that shared contract
// before checking whether the chosen access path makes the child redundant.
fn selected_index_prefix_guarantees_eq_compare(
    schema: &SchemaInfo,
    selected_path: &AccessPath<Value>,
    cmp: &crate::db::predicate::ComparePredicate,
) -> bool {
    let selected_prefix = selected_path.as_index_prefix().or_else(|| {
        selected_path
            .as_index_range()
            .map(|(index, values, _, _)| (index, values))
    });
    let Some((index, prefix_values)) = selected_prefix else {
        return false;
    };

    index_prefix_guarantees_eq_compare(schema, index, prefix_values, cmp)
}

// Prefix guarantees are checked against canonical key-item lowering so mixed
// field/expression prefixes can suppress only the exact equality clauses they
// already prove.
fn index_prefix_guarantees_eq_compare(
    schema: &SchemaInfo,
    index: &IndexModel,
    prefix_values: &[Value],
    cmp: &crate::db::predicate::ComparePredicate,
) -> bool {
    let literal_compatible = index_literal_matches_schema(schema, cmp.field.as_str(), cmp.value());

    prefix_values
        .iter()
        .enumerate()
        .any(|(slot, expected_value)| {
            let Some(key_item) = index_key_item_at(index, slot) else {
                return false;
            };
            let Some(candidate) = eq_lookup_value_for_key_item(
                key_item,
                cmp.field.as_str(),
                cmp.value(),
                cmp.coercion.id,
                literal_compatible,
            ) else {
                return false;
            };

            candidate == *expected_value
        })
}
