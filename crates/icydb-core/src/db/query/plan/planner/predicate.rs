//! Module: db::query::plan::planner::predicate
//! Builds predicate-driven access plans from canonical predicate trees and
//! visible index metadata.

#[cfg(test)]
mod child_tests;
#[cfg(all(test, feature = "sql"))]
mod intersection_tests;
#[cfg(all(test, feature = "sql"))]
mod redundancy_tests;
#[cfg(test)]
mod selection_tests;

use crate::{
    db::{
        access::{AccessPath, AccessPlan, SemanticIndexAccessContract},
        predicate::{CompareOp, ComparePredicate, Predicate},
        query::construction::ConstructionBudget,
        query::plan::{
            OrderSpec, PlannedNonIndexAccessReason,
            key_item_match::lower_lookup_value_for_key_item,
            order_contract::CandidateOrderContract,
            planner::{
                AndFamilyCandidateScore, AndFamilyPriorityClass, PlannedAccessSelection,
                and_family_candidate_score_outranks, compare, index_field_literal_matcher,
                index_literal_matches_schema, prefix, range,
                selected_index_contract_satisfies_secondary_order,
            },
        },
        schema::SchemaInfo,
    },
    error::InternalError,
    value::{Value, canonicalize_value_set},
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use std::cmp::Ordering;

#[expect(
    clippy::too_many_lines,
    reason = "planner predicate selection still centralizes the bounded family-routing policy in one owner-local entrypoint"
)]
pub(super) fn plan_predicate(
    candidate_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    predicate: &Predicate,
    order: Option<&OrderSpec>,
    grouped: bool,
    budget: &dyn ConstructionBudget,
) -> Result<PlannedAccessSelection, InternalError> {
    budget.charge(Resource::PredicateExpressionSteps, 1)?;
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
            if schema.primary_key_names().iter().any(|name| name == field)
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
            // Admit the child destination before candidate extraction/recursion.
            // Family selection consumes its winner directly; only child routes
            // need slots here. Payload construction/proofs remain separate work.
            let mut plans = budget.vec_with_capacity(children.len())?;
            // Phase 1: derive the planner-owned secondary-index candidates once
            // so child recursion can reuse the chosen index contract for
            // redundancy stripping without reopening candidate extraction.
            let primary_key_range_access =
                range::primary_key_range_from_and(schema, children, budget)?;
            let index_range_access = range::index_range_from_and(
                candidate_indexes,
                schema,
                children,
                order,
                grouped,
                budget,
            )?
            .map(|spec| {
                budget
                    .boxed(AccessPath::IndexRange { spec })
                    .map(AccessPlan::Path)
            })
            .transpose()?;
            let prefix_access = prefix::index_prefix_from_and(
                candidate_indexes,
                schema,
                children,
                order,
                grouped,
                budget,
            )?;
            let branch_set_access = prefix::index_branch_set_from_and(
                candidate_indexes,
                schema,
                children,
                order,
                grouped,
                budget,
            )?;

            // Phase 2: recurse into conjunctive children once while the
            // strongest secondary-index candidate is still available to strip
            // only the clauses that candidate already guarantees.
            let selected_index_access = branch_set_access
                .as_ref()
                .or(index_range_access.as_ref())
                .or(prefix_access.as_ref());
            for child in children {
                if !child_is_redundant_under_selected_index_access(
                    schema,
                    selected_index_access,
                    child,
                    budget,
                )? {
                    plans.push(
                        plan_predicate(candidate_indexes, schema, child, order, grouped, budget)?
                            .into_access(),
                    );
                }
            }
            let intersection_access = exact_index_intersection_candidate(
                schema,
                order,
                grouped,
                selected_index_access,
                plans.as_slice(),
                budget,
            )?;
            let required_order_primary_key_range = match primary_key_range_access.as_ref() {
                Some(candidate) => candidate_outranks_selected_access_on_required_order(
                    schema,
                    order,
                    grouped,
                    candidate,
                    selected_index_access,
                    budget,
                )?,
                None => false,
            };
            let family_choice = choose_best_and_family_access(
                primary_key_child_access_candidate(plans.as_slice(), budget)?,
                intersection_access,
                primary_key_range_access,
                index_range_access,
                branch_set_access,
                prefix_access,
                required_order_primary_key_range,
            );
            if let Some(family_choice) = family_choice {
                return Ok(family_choice);
            }

            // Any family candidate would already have supplied a winner.
            PlannedAccessSelection::new(
                AccessPlan::intersection(plans),
                Some(PlannedNonIndexAccessReason::PlannerCompositeNonIndex),
            )
        }
        Predicate::Or(children) => {
            let mut plans = budget.vec_with_capacity(children.len())?;
            for child in children {
                plans.push(
                    plan_predicate(candidate_indexes, schema, child, order, grouped, budget)?
                        .into_access(),
                );
            }
            PlannedAccessSelection::new(
                AccessPlan::union(plans),
                Some(PlannedNonIndexAccessReason::PlannerCompositeNonIndex),
            )
        }
        Predicate::Compare(cmp) => {
            let access =
                compare::plan_compare(candidate_indexes, schema, cmp, order, grouped, budget)?;
            let reason = planned_non_index_reason_for_access(&access);

            PlannedAccessSelection::new(access, reason)
        }
    };

    Ok(plan)
}

// Consolidate the existing `AND` family winner policy into one explicit
// comparison path so planner-family route choice does not depend on ad hoc
// early returns spread through the main recursion body.
fn choose_best_and_family_access(
    child_candidate: Option<(PlannedAccessSelection, AndFamilyPriorityClass)>,
    intersection_access: Option<AccessPlan<Value>>,
    primary_key_range_access: Option<AccessPlan<Value>>,
    index_range_access: Option<AccessPlan<Value>>,
    branch_set_access: Option<AccessPlan<Value>>,
    prefix_access: Option<AccessPlan<Value>>,
    required_order_primary_key_range: bool,
) -> Option<PlannedAccessSelection> {
    let mut chosen: Option<(AndFamilyCandidateScore, PlannedAccessSelection)> = None;

    if let Some((access, priority)) = child_candidate {
        update_best_and_family_candidate(
            &mut chosen,
            Some(access),
            AndFamilyCandidateScore::new(priority, false, 0),
        );
    }

    update_best_and_family_candidate(
        &mut chosen,
        intersection_access.map(|access| {
            PlannedAccessSelection::new(
                access,
                Some(PlannedNonIndexAccessReason::PlannerExactIndexIntersection),
            )
        }),
        AndFamilyCandidateScore::new(AndFamilyPriorityClass::Ordinary, false, 5),
    );

    update_best_and_family_candidate(
        &mut chosen,
        primary_key_range_access.map(|access| {
            PlannedAccessSelection::new(
                access,
                Some(if required_order_primary_key_range {
                    PlannedNonIndexAccessReason::RequiredOrderPrimaryKeyRangePreferred
                } else {
                    PlannedNonIndexAccessReason::PlannerPrimaryKeyRange
                }),
            )
        }),
        AndFamilyCandidateScore::new(
            AndFamilyPriorityClass::Ordinary,
            required_order_primary_key_range,
            1,
        ),
    );

    update_best_and_family_candidate(
        &mut chosen,
        index_range_access.map(|access| PlannedAccessSelection::new(access, None)),
        AndFamilyCandidateScore::new(AndFamilyPriorityClass::Ordinary, false, 3),
    );

    update_best_and_family_candidate(
        &mut chosen,
        branch_set_access.map(|access| PlannedAccessSelection::new(access, None)),
        AndFamilyCandidateScore::new(AndFamilyPriorityClass::Ordinary, false, 4),
    );

    update_best_and_family_candidate(
        &mut chosen,
        prefix_access.map(|access| PlannedAccessSelection::new(access, None)),
        AndFamilyCandidateScore::new(AndFamilyPriorityClass::Ordinary, false, 2),
    );

    chosen.map(|(_, access)| access)
}

const MAX_EXACT_INDEX_INTERSECTION_CHILDREN: usize = 3;

// Build one bounded exact-prefix intersection candidate only when every child
// has primary-key suffix order. Runtime synchronized cardinality and overlap
// authority still decide whether this candidate executes or falls back to the
// first (already planner-preferred) child.
fn exact_index_intersection_candidate(
    schema: &SchemaInfo,
    order: Option<&OrderSpec>,
    grouped: bool,
    selected_index_access: Option<&AccessPlan<Value>>,
    child_plans: &[AccessPlan<Value>],
    budget: &dyn ConstructionBudget,
) -> Result<Option<AccessPlan<Value>>, InternalError> {
    if grouped || !intersection_order_is_primary_key_compatible(schema, order) {
        return Ok(None);
    }

    let Some(selected) = selected_index_access else {
        return Ok(None);
    };
    // Bound candidate, suffix-slot and fixed-size identity visits as one batch.
    // Field-name comparisons and order/schema validation remain separately owned.
    budget.charge(
        Resource::PredicateExpressionSteps,
        (child_plans.len() as u64).saturating_add(1).saturating_mul(
            (schema.primary_key_names().len() as u64)
                .saturating_add(MAX_EXACT_INDEX_INTERSECTION_CHILDREN as u64 + 1),
        ),
    )?;
    let Some(selected) = exact_prefix_with_primary_key_suffix(schema, selected) else {
        return Ok(None);
    };

    // The existing three-child cap fits on the stack. Keep the selected prefix
    // first and retain the first child for each ordinal/generation identity.
    let mut candidates = [selected; MAX_EXACT_INDEX_INTERSECTION_CHILDREN];
    let mut count = 1;
    for child in child_plans {
        let Some(candidate) = exact_prefix_with_primary_key_suffix(schema, child) else {
            continue;
        };
        let duplicate_index = candidates[..count].iter().any(|(index, _)| {
            index.ordinal() == candidate.0.ordinal()
                && index.physical_generation() == candidate.0.physical_generation()
        });
        if !duplicate_index {
            candidates[count] = candidate;
            count += 1;
        }
        if count == MAX_EXACT_INDEX_INTERSECTION_CHILDREN {
            break;
        }
    }

    if count < 2 {
        return Ok(None);
    }
    let mut children = budget.vec_with_capacity(count)?;
    for (index, prefix) in &candidates[..count] {
        let mut values = budget.vec_with_capacity(prefix.len())?;
        for value in *prefix {
            values.push(budget.copy_value(value)?);
        }
        children.push(AccessPlan::Path(budget.boxed(AccessPath::IndexPrefix {
            index: (*index).clone(),
            values,
        })?));
    }
    // Two or three nonempty flat prefix paths are already canonical; the
    // generic flattening constructor would allocate another identical list.
    Ok(Some(AccessPlan::Intersection(children)))
}

fn intersection_order_is_primary_key_compatible(
    schema: &SchemaInfo,
    order: Option<&OrderSpec>,
) -> bool {
    let Some(order) = order else {
        return true;
    };
    order
        .primary_key_only_direction_fields(schema.primary_key_names())
        .is_some()
}

fn exact_prefix_with_primary_key_suffix<'a>(
    schema: &SchemaInfo,
    access: &'a AccessPlan<Value>,
) -> Option<(&'a SemanticIndexAccessContract, &'a [Value])> {
    let AccessPath::IndexPrefix { index, values } = access.as_path()? else {
        return None;
    };
    let primary_key_names = schema.primary_key_names();
    if values.is_empty()
        || values.len().saturating_add(primary_key_names.len()) != index.key_arity()
    {
        return None;
    }

    primary_key_names
        .iter()
        .enumerate()
        .all(|(offset, field)| {
            index.key_field_at(values.len().saturating_add(offset)) == Some(field.as_str())
        })
        .then_some((index, values))
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

// Conjunctive child planning can already discover exact primary-key access
// routes from direct `id = ?` and finite `id IN (...)` clauses. Their
// intersection is a stronger planner-visible candidate than any broader
// secondary index scan, so keep one owner-local reducer for this family-level
// preference.
fn primary_key_child_access_candidate(
    children: &[AccessPlan<Value>],
    budget: &dyn ConstructionBudget,
) -> Result<Option<(PlannedAccessSelection, AndFamilyPriorityClass)>, InternalError> {
    // Explicit emptiness wins even when earlier children already conflict.
    // Admit both structural passes before inspecting or constructing candidates.
    budget.charge(
        Resource::PredicateExpressionSteps,
        (children.len() as u64).saturating_mul(2),
    )?;
    if children.iter().any(AccessPlan::is_explicit_empty) {
        let access = AccessPlan::Path(budget.boxed(AccessPath::ByKeys(Vec::new()))?);
        return Ok(Some((
            PlannedAccessSelection::new(
                access,
                Some(PlannedNonIndexAccessReason::EmptyChildAccessPreferred),
            ),
            AndFamilyPriorityClass::ExplicitEmpty,
        )));
    }
    let mut intersection: Option<Vec<&Value>> = None;

    for child in children {
        let Some(keys) = exact_primary_key_values_from_child_access(child) else {
            continue;
        };
        budget.charge(Resource::PredicateExpressionSteps, keys.len() as u64)?;
        let mut child_keys = budget.vec_with_capacity(keys.len())?;
        child_keys.extend(keys.iter());
        canonicalize_value_set(&mut child_keys);

        match &mut intersection {
            None => intersection = Some(child_keys),
            Some(current_keys) => {
                budget.charge(
                    Resource::PredicateExpressionSteps,
                    (current_keys.len() as u64).saturating_add(child_keys.len() as u64),
                )?;
                intersect_canonical_value_sets(current_keys, child_keys.as_slice());
                if current_keys.is_empty() {
                    return primary_key_children_selection_for_keys(&[], true, budget).map(Some);
                }
            }
        }
    }

    intersection
        .map(|keys| primary_key_children_selection_for_keys(&keys, false, budget))
        .transpose()
}

fn exact_primary_key_values_from_child_access(child: &AccessPlan<Value>) -> Option<&[Value]> {
    let path = child.as_path()?;
    if let Some(key) = path.as_by_key() {
        return Some(std::slice::from_ref(key));
    }

    path.as_by_keys()
}

fn primary_key_children_selection_for_keys(
    keys: &[&Value],
    conflicting_children: bool,
    budget: &dyn ConstructionBudget,
) -> Result<(PlannedAccessSelection, AndFamilyPriorityClass), InternalError> {
    let reason = if conflicting_children {
        PlannedNonIndexAccessReason::ConflictingPrimaryKeyChildrenAccessPreferred
    } else if keys.len() == 1 {
        PlannedNonIndexAccessReason::SingletonPrimaryKeyChildAccessPreferred
    } else {
        PlannedNonIndexAccessReason::PlannerKeySetAccess
    };
    let path = if let [key] = keys {
        AccessPath::ByKey(budget.copy_value(key)?)
    } else {
        let mut values = budget.vec_with_capacity(keys.len())?;
        for key in keys {
            values.push(budget.copy_value(key)?);
        }
        AccessPath::ByKeys(values)
    };

    Ok((
        PlannedAccessSelection::new(AccessPlan::Path(budget.boxed(path)?), Some(reason)),
        if conflicting_children {
            AndFamilyPriorityClass::ConflictingPrimaryKeyChildren
        } else {
            AndFamilyPriorityClass::SingletonPrimaryKey
        },
    ))
}

// Retain representatives from the first canonical child without allocating
// another intersection vector or copying values on each reduction pass.
fn intersect_canonical_value_sets(left: &mut Vec<&Value>, right: &[&Value]) {
    let mut right_idx = 0usize;
    left.retain(|value| {
        while right_idx < right.len() {
            match Value::canonical_cmp(value, right[right_idx]) {
                Ordering::Less => return false,
                Ordering::Greater => right_idx += 1,
                Ordering::Equal => {
                    right_idx += 1;
                    return true;
                }
            }
        }
        false
    });
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
    if !access.has_selected_index_access_path() {
        return Some(PlannedNonIndexAccessReason::PlannerCompositeNonIndex);
    }

    None
}

// Prefer one planner-visible route over another only when the candidate keeps
// the required order and the selected competitor does not. This keeps family
// competition framed in terms of the shared ordering contract instead of one
// special-cased route name.
fn candidate_outranks_selected_access_on_required_order(
    schema: &SchemaInfo,
    order: Option<&OrderSpec>,
    grouped: bool,
    candidate_access: &AccessPlan<Value>,
    selected_access: Option<&AccessPlan<Value>>,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    let Some(selected_access) = selected_access else {
        return Ok(false);
    };

    let Some(order) = order else {
        return Ok(false);
    };

    if grouped {
        return Ok(false);
    }
    let contract = CandidateOrderContract::prepare(schema, Some(order), false, budget)?;
    Ok(
        access_preserves_required_order(schema, order, contract.as_ref(), candidate_access)
            && !access_preserves_required_order(schema, order, contract.as_ref(), selected_access),
    )
}

// Reuse the same planner-owned ordering contract across family competition so
// secondary candidate ranking and family-level route preference do not drift.
fn access_preserves_required_order(
    schema: &SchemaInfo,
    order: &OrderSpec,
    contract: Option<&CandidateOrderContract>,
    access: &AccessPlan<Value>,
) -> bool {
    if access.as_primary_key_range_path().is_some() {
        return order
            .primary_key_only_direction_fields(schema.primary_key_names())
            .is_some();
    }
    if let Some((index, prefix_values)) = access.as_index_prefix_contract_path() {
        return selected_index_contract_satisfies_secondary_order(
            contract,
            &index,
            prefix_values.len(),
        );
    }
    if let Some(spec) = access.as_index_branch_set_spec_path() {
        return selected_index_contract_satisfies_secondary_order(
            contract,
            &spec.index(),
            spec.branch_prefix_len(),
        );
    }
    if let Some(spec) = access.as_index_range_path() {
        return selected_index_contract_satisfies_secondary_order(
            contract,
            &spec.index(),
            spec.prefix_values().len(),
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
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    let Some(AccessPlan::Path(path)) = selected_access else {
        return Ok(false);
    };
    let Predicate::Compare(cmp) = child else {
        return Ok(false);
    };

    if selected_index_branch_set_guarantees_compare(schema, path.as_ref(), cmp, budget)?
        || selected_index_prefix_guarantees_compare(schema, path.as_ref(), cmp, budget)?
    {
        return Ok(true);
    }

    // The implication owner accepts the original borrowed child; manufacturing
    // another Compare predicate would copy its field and operand for no benefit.
    let Some(index) = path.as_ref().selected_index_contract() else {
        return Ok(false);
    };
    let Some(guard) = index.predicate_semantics() else {
        return Ok(false);
    };
    crate::db::query::plan::planner::index_select::predicate_implies_predicate_for_planner(
        guard, child, budget,
    )
}

fn selected_index_branch_set_guarantees_compare(
    schema: &SchemaInfo,
    selected_path: &AccessPath<Value>,
    cmp: &ComparePredicate,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    let Some(spec) = selected_path.as_index_branch_set_spec() else {
        return Ok(false);
    };
    if index_prefix_guarantees_compare(schema, spec.index_ref(), spec.fixed_values(), cmp, budget)?
    {
        return Ok(true);
    }

    let Some(branch_key_item) = spec.branch_key_item() else {
        return Ok(false);
    };
    key_item_guarantees_compare(schema, branch_key_item, spec.branch_values(), cmp, budget)
}

// Selected index prefix and selected index range both carry an equality prefix
// that can already prove one compare predicate. Project that shared contract
// before checking whether the chosen access path makes the child redundant.
fn selected_index_prefix_guarantees_compare(
    schema: &SchemaInfo,
    selected_path: &AccessPath<Value>,
    cmp: &ComparePredicate,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    match selected_path {
        AccessPath::IndexPrefix { index, values } => {
            index_prefix_guarantees_compare(schema, index, values, cmp, budget)
        }
        AccessPath::IndexRange { spec } => index_prefix_guarantees_compare(
            schema,
            spec.index_ref(),
            spec.prefix_values(),
            cmp,
            budget,
        ),
        _ => Ok(false),
    }
}

// Prefix guarantees are checked against canonical key-item lowering so mixed
// field/expression prefixes can suppress only clauses they already prove.
fn index_prefix_guarantees_compare(
    schema: &SchemaInfo,
    index: &SemanticIndexAccessContract,
    prefix_values: &[Value],
    cmp: &ComparePredicate,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    budget.charge(
        Resource::PredicateExpressionSteps,
        prefix_values.len() as u64,
    )?;
    for (slot, expected_value) in prefix_values.iter().enumerate() {
        if let Some(key_item) = index.key_item_at(slot)
            && key_item_guarantees_compare(
                schema,
                key_item,
                std::slice::from_ref(expected_value),
                cmp,
                budget,
            )?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

// Only membership matters here: ordering and duplicate elimination cannot change
// the answer. Borrow identity values and lower expression values once per proof;
// scalar comparisons need neither a list allocation nor an operand copy.
fn key_item_guarantees_compare(
    schema: &SchemaInfo,
    key_item: crate::db::access::SemanticIndexKeyItemRef<'_>,
    expected: &[Value],
    cmp: &ComparePredicate,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    if key_item.field() != cmp.field.as_str() {
        return Ok(false);
    }

    match cmp.op {
        CompareOp::Eq | CompareOp::Ne => {
            budget.charge(
                Resource::PredicateExpressionSteps,
                (expected.len() as u64).saturating_add(1),
            )?;
            let literal_compatible =
                index_literal_matches_schema(schema, cmp.field.as_str(), cmp.value());
            let value = lower_lookup_value_for_key_item(
                key_item,
                cmp.field.as_str(),
                cmp.value(),
                cmp.coercion.id,
                literal_compatible,
                budget,
            )?;
            Ok(value.is_some_and(|value| {
                expected
                    .iter()
                    .all(|expected| (value.as_ref() == expected) == (cmp.op == CompareOp::Eq))
            }))
        }
        CompareOp::In | CompareOp::NotIn => {
            let Value::List(values) = cmp.value() else {
                return Ok(false);
            };
            // Admit literal visits and worst-case membership comparisons once.
            // Payload comparison and schema lookup internals remain separate.
            budget.charge(
                Resource::PredicateExpressionSteps,
                (values.len() as u64)
                    .saturating_mul((expected.len() as u64).saturating_add(1))
                    .saturating_add(expected.len() as u64),
            )?;
            let mut lookup_values = budget.vec_with_capacity(values.len())?;
            let matcher = index_field_literal_matcher(schema, cmp.field.as_str());
            for value in values {
                if let Some(value) = lower_lookup_value_for_key_item(
                    key_item,
                    cmp.field.as_str(),
                    value,
                    cmp.coercion.id,
                    matcher.matches(value),
                    budget,
                )? {
                    lookup_values.push(value);
                }
            }
            Ok(expected.iter().all(|expected| {
                lookup_values.iter().any(|value| value.as_ref() == expected)
                    == (cmp.op == CompareOp::In)
            }))
        }
        CompareOp::Lt
        | CompareOp::Lte
        | CompareOp::Gt
        | CompareOp::Gte
        | CompareOp::StartsWith
        | CompareOp::Contains
        | CompareOp::EndsWith => Ok(false),
    }
}
