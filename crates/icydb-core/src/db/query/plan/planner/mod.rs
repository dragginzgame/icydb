//! Semantic planning from predicates to access strategies; must not assert invariants.
//!
//! Determinism: the planner canonicalizes output so the same model and
//! predicate shape always produce identical access plans.

mod normalize;
mod range;

use super::{AccessPath, AccessPlan, PlanError};
use crate::{
    db::{
        index::encode_canonical_index_component,
        query::predicate::{
            CoercionId, CompareOp, ComparePredicate, Predicate, SchemaInfo,
            normalize as normalize_predicate, validate::literal_matches_type,
        },
    },
    error::InternalError,
    model::entity::EntityModel,
    model::index::IndexModel,
    value::Value,
};
use std::ops::Bound;
use thiserror::Error as ThisError;

///
/// PlannerError
///

#[derive(Debug, ThisError)]
pub enum PlannerError {
    #[error("{0}")]
    Plan(Box<PlanError>),

    #[error("{0}")]
    Internal(Box<InternalError>),
}

impl From<PlanError> for PlannerError {
    fn from(err: PlanError) -> Self {
        Self::Plan(Box::new(err))
    }
}

impl From<InternalError> for PlannerError {
    fn from(err: InternalError) -> Self {
        Self::Internal(Box::new(err))
    }
}

/// Planner entrypoint that operates on a prebuilt schema surface.
///
/// CONTRACT: the caller is responsible for predicate validation.
pub(crate) fn plan_access(
    model: &EntityModel,
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
) -> Result<AccessPlan<Value>, PlannerError> {
    let Some(predicate) = predicate else {
        return Ok(AccessPlan::full_scan());
    };

    // Planner determinism guarantee:
    // Given a validated EntityModel and normalized predicate, planning is pure and deterministic.
    //
    // Planner determinism rules:
    // - Predicate normalization sorts AND/OR children by (field, operator, value, coercion).
    // - Index candidates are considered in lexicographic IndexModel.name order.
    // - Access paths are ranked: primary key lookups, exact index matches, prefix matches, full scans.
    // - Order specs preserve user order after validation (planner does not reorder).
    // - Field resolution uses SchemaInfo's name map (sorted by field name).
    let normalized = normalize_predicate(predicate);
    let plan = normalize_access_plan(plan_predicate(model, schema, &normalized)?);

    Ok(plan)
}

fn plan_predicate(
    model: &EntityModel,
    schema: &SchemaInfo,
    predicate: &Predicate,
) -> Result<AccessPlan<Value>, InternalError> {
    let plan = match predicate {
        Predicate::True
        | Predicate::False
        | Predicate::Not(_)
        | Predicate::IsNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => AccessPlan::full_scan(),
        Predicate::And(children) => {
            if let Some(range) = range::index_range_from_and(model, schema, children) {
                return Ok(AccessPlan::path(range));
            }

            let mut plans = children
                .iter()
                .map(|child| plan_predicate(model, schema, child))
                .collect::<Result<Vec<_>, _>>()?;

            // Composite index planning phase:
            // - Range candidate extraction is resolved before child recursion.
            // - If no range candidate exists, retain equality-prefix planning.
            if let Some(prefix) = index_prefix_from_and(model, schema, children) {
                plans.push(AccessPlan::path(prefix));
            }

            AccessPlan::Intersection(plans)
        }
        Predicate::Or(children) => AccessPlan::Union(
            children
                .iter()
                .map(|child| plan_predicate(model, schema, child))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Predicate::Compare(cmp) => plan_compare(model, schema, cmp),
    };

    Ok(plan)
}

fn plan_compare(
    model: &EntityModel,
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
) -> AccessPlan<Value> {
    if cmp.coercion.id != CoercionId::Strict {
        return AccessPlan::full_scan();
    }

    if is_primary_key_model(schema, model, &cmp.field)
        && let Some(path) = plan_pk_compare(schema, model, cmp)
    {
        return AccessPlan::path(path);
    }

    match cmp.op {
        CompareOp::Eq => {
            if let Some(paths) = index_prefix_for_eq(model, schema, &cmp.field, &cmp.value) {
                return AccessPlan::Union(paths);
            }
        }
        CompareOp::In => {
            if let Value::List(items) = &cmp.value {
                let mut plans = Vec::new();
                for item in items {
                    if let Some(paths) = index_prefix_for_eq(model, schema, &cmp.field, item) {
                        plans.extend(paths);
                    }
                }
                if !plans.is_empty() {
                    return AccessPlan::Union(plans);
                }
            }
        }
        CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
            // Single compare predicates only map directly to one-field indexes.
            // Composite prefix+range extraction remains AND-group driven.
            if range::index_range_literal_is_compatible(schema, &cmp.field, &cmp.value) {
                let (lower, upper) = match cmp.op {
                    CompareOp::Gt => (Bound::Excluded(cmp.value.clone()), Bound::Unbounded),
                    CompareOp::Gte => (Bound::Included(cmp.value.clone()), Bound::Unbounded),
                    CompareOp::Lt => (Bound::Unbounded, Bound::Excluded(cmp.value.clone())),
                    CompareOp::Lte => (Bound::Unbounded, Bound::Included(cmp.value.clone())),
                    _ => unreachable!("range arm must be one of Gt/Gte/Lt/Lte"),
                };

                for index in sorted_indexes(model) {
                    if index.fields.len() == 1 && index.fields[0] == cmp.field.as_str() {
                        return AccessPlan::path(AccessPath::IndexRange {
                            index: *index,
                            prefix: Vec::new(),
                            lower,
                            upper,
                        });
                    }
                }
            }
        }
        _ => {
            // NOTE: Other non-equality comparisons do not currently map to key access paths.
        }
    }

    AccessPlan::full_scan()
}

fn plan_pk_compare(
    schema: &SchemaInfo,
    model: &EntityModel,
    cmp: &ComparePredicate,
) -> Option<AccessPath<Value>> {
    match cmp.op {
        CompareOp::Eq => {
            if !value_matches_pk_model(schema, model, &cmp.value) {
                return None;
            }

            Some(AccessPath::ByKey(cmp.value.clone()))
        }
        CompareOp::In => {
            let Value::List(items) = &cmp.value else {
                return None;
            };

            for item in items {
                if !value_matches_pk_model(schema, model, item) {
                    return None;
                }
            }
            // NOTE: key order is canonicalized during access-plan normalization.
            Some(AccessPath::ByKeys(items.clone()))
        }
        _ => {
            // NOTE: Only Eq/In comparisons can be expressed as key access paths.
            None
        }
    }
}

pub(in crate::db::query::plan::planner) fn sorted_indexes(
    model: &EntityModel,
) -> Vec<&'static IndexModel> {
    let mut indexes = model.indexes.to_vec();
    indexes.sort_by(|left, right| left.name.cmp(right.name));

    indexes
}

fn index_prefix_for_eq(
    model: &EntityModel,
    schema: &SchemaInfo,
    field: &str,
    value: &Value,
) -> Option<Vec<AccessPlan<Value>>> {
    if !index_prefix_literal_is_compatible(schema, field, value) {
        return None;
    }

    let mut out = Vec::new();
    for index in sorted_indexes(model) {
        if index.fields.first() != Some(&field) {
            continue;
        }
        out.push(AccessPlan::path(AccessPath::IndexPrefix {
            index: *index,
            values: vec![value.clone()],
        }));
    }

    if out.is_empty() { None } else { Some(out) }
}

fn index_prefix_from_and(
    model: &EntityModel,
    schema: &SchemaInfo,
    children: &[Predicate],
) -> Option<AccessPath<Value>> {
    let mut field_values = Vec::new();

    for child in children {
        let Predicate::Compare(cmp) = child else {
            continue;
        };
        if cmp.op != CompareOp::Eq {
            continue;
        }
        if cmp.coercion.id != CoercionId::Strict {
            continue;
        }
        field_values.push((cmp.field.as_str(), &cmp.value));
    }

    let mut best: Option<(usize, bool, &IndexModel, Vec<Value>)> = None;
    for index in sorted_indexes(model) {
        let mut prefix = Vec::new();
        for field in index.fields {
            // NOTE: duplicate equality predicates on the same field are assumed
            // to have been validated upstream (no conflict). Planner picks the first.
            let Some((_, value)) = field_values.iter().find(|(name, _)| *name == *field) else {
                break;
            };
            if !index_prefix_literal_is_compatible(schema, field, value) {
                prefix.clear();
                break;
            }
            prefix.push((*value).clone());
        }

        if prefix.is_empty() {
            continue;
        }

        let exact = prefix.len() == index.fields.len();
        match &best {
            None => best = Some((prefix.len(), exact, index, prefix)),
            Some((best_len, best_exact, best_index, _)) => {
                if better_index(
                    (prefix.len(), exact, index),
                    (*best_len, *best_exact, best_index),
                ) {
                    best = Some((prefix.len(), exact, index, prefix));
                }
            }
        }
    }

    best.map(|(_, _, index, values)| AccessPath::IndexPrefix {
        index: *index,
        values,
    })
}

fn better_index(
    candidate: (usize, bool, &IndexModel),
    current: (usize, bool, &IndexModel),
) -> bool {
    let (cand_len, cand_exact, cand_index) = candidate;
    let (best_len, best_exact, best_index) = current;

    cand_len > best_len
        || (cand_len == best_len && cand_exact && !best_exact)
        || (cand_len == best_len && cand_exact == best_exact && cand_index.name < best_index.name)
}

// Normalize composite access plans into canonical, flattened forms.
fn normalize_access_plan(plan: AccessPlan<Value>) -> AccessPlan<Value> {
    normalize::normalize_access_plan(plan)
}

fn is_primary_key_model(schema: &SchemaInfo, model: &EntityModel, field: &str) -> bool {
    field == model.primary_key.name && schema.field(field).is_some()
}

fn value_matches_pk_model(schema: &SchemaInfo, model: &EntityModel, value: &Value) -> bool {
    let field = model.primary_key.name;
    let Some(field_type) = schema.field(field) else {
        return false;
    };

    field_type.is_keyable() && literal_matches_type(value, field_type)
}

// Validate one equality literal for index-prefix planning. The value must match
// the schema field type and be canonically index-encodable.
pub(in crate::db::query::plan::planner) fn index_prefix_literal_is_compatible(
    schema: &SchemaInfo,
    field: &str,
    value: &Value,
) -> bool {
    let Some(field_type) = schema.field(field) else {
        return false;
    };

    literal_matches_type(value, field_type) && encode_canonical_index_component(value).is_ok()
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Ulid;

    #[test]
    fn normalize_union_dedups_identical_paths() {
        let key = Value::Ulid(Ulid::from_u128(1));
        let plan = AccessPlan::Union(vec![
            AccessPlan::path(AccessPath::ByKey(key.clone())),
            AccessPlan::path(AccessPath::ByKey(key)),
        ]);

        let normalized = normalize_access_plan(plan);

        assert_eq!(
            normalized,
            AccessPlan::path(AccessPath::ByKey(Value::Ulid(Ulid::from_u128(1))))
        );
    }

    #[test]
    fn normalize_union_sorts_by_key() {
        let a = Value::Ulid(Ulid::from_u128(1));
        let b = Value::Ulid(Ulid::from_u128(2));
        let plan = AccessPlan::Union(vec![
            AccessPlan::path(AccessPath::ByKey(b.clone())),
            AccessPlan::path(AccessPath::ByKey(a.clone())),
        ]);

        let normalized = normalize_access_plan(plan);
        let AccessPlan::Union(children) = normalized else {
            panic!("expected union");
        };

        assert_eq!(children.len(), 2);
        assert_eq!(children[0], AccessPlan::path(AccessPath::ByKey(a)));
        assert_eq!(children[1], AccessPlan::path(AccessPath::ByKey(b)));
    }

    #[test]
    fn normalize_intersection_removes_full_scan() {
        let key = Value::Ulid(Ulid::from_u128(7));
        let plan = AccessPlan::Intersection(vec![
            AccessPlan::full_scan(),
            AccessPlan::path(AccessPath::ByKey(key)),
        ]);

        let normalized = normalize_access_plan(plan);

        assert_eq!(
            normalized,
            AccessPlan::path(AccessPath::ByKey(Value::Ulid(Ulid::from_u128(7))))
        );
    }
}
