//! Semantic planning from predicates to access strategies; must not assert invariants.
//!
//! Determinism: the planner canonicalizes output so the same model and
//! predicate shape always produce identical access plans.

use super::{AccessPath, AccessPlan, PlanError, canonical, validate_plan_invariants_model};
use crate::{
    db::{
        index::fingerprint,
        query::predicate::{
            CoercionId, CompareOp, ComparePredicate, Predicate, SchemaInfo, normalize,
            validate::{FieldType, ScalarType, literal_matches_type},
        },
    },
    error::InternalError,
    model::entity::EntityModel,
    model::index::IndexModel,
    value::Value,
};
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub(crate) enum PlannerError {
    #[error("{0}")]
    Plan(#[from] PlanError),
    #[error("{0}")]
    Internal(#[from] InternalError),
}

/// Planner entrypoint that operates on a prebuilt schema surface.
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
    crate::db::query::predicate::validate(schema, predicate).map_err(PlanError::from)?;

    let normalized = normalize(predicate);
    let plan = normalize_access_plan(plan_predicate(model, schema, &normalized)?);
    validate_plan_invariants_model(&plan, schema, model, Some(&normalized))?;
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
        | Predicate::TextContainsCi { .. }
        | Predicate::MapContainsKey { .. }
        | Predicate::MapContainsValue { .. }
        | Predicate::MapContainsEntry { .. } => AccessPlan::full_scan(),
        Predicate::And(children) => {
            let mut plans = children
                .iter()
                .map(|child| plan_predicate(model, schema, child))
                .collect::<Result<Vec<_>, _>>()?;

            if let Some(prefix) = index_prefix_from_and(model, schema, children)? {
                plans.push(AccessPlan::Path(prefix));
            }

            AccessPlan::Intersection(plans)
        }
        Predicate::Or(children) => AccessPlan::Union(
            children
                .iter()
                .map(|child| plan_predicate(model, schema, child))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Predicate::Compare(cmp) => plan_compare(model, schema, cmp)?,
    };

    Ok(plan)
}

fn plan_compare(
    model: &EntityModel,
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
) -> Result<AccessPlan<Value>, InternalError> {
    if cmp.coercion.id != CoercionId::Strict {
        return Ok(AccessPlan::full_scan());
    }

    if is_primary_key_model(schema, model, &cmp.field)
        && let Some(path) = plan_pk_compare(schema, model, cmp)
    {
        return Ok(AccessPlan::Path(path));
    }

    match cmp.op {
        CompareOp::Eq => {
            if let Some(paths) = index_prefix_for_eq(model, schema, &cmp.field, &cmp.value)? {
                return Ok(AccessPlan::Union(paths));
            }
        }
        CompareOp::In => {
            if let Value::List(items) = &cmp.value {
                let mut plans = Vec::new();
                for item in items {
                    if let Some(paths) = index_prefix_for_eq(model, schema, &cmp.field, item)? {
                        plans.extend(paths);
                    }
                }
                if !plans.is_empty() {
                    return Ok(AccessPlan::Union(plans));
                }
            }
        }
        _ => {}
    }

    Ok(AccessPlan::full_scan())
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
            Some(AccessPath::ByKeys(items.clone()))
        }
        _ => None,
    }
}

fn sorted_indexes(model: &EntityModel) -> Vec<&'static IndexModel> {
    let mut indexes = model.indexes.to_vec();
    indexes.sort_by(|left, right| left.name.cmp(right.name));
    indexes
}

fn index_prefix_for_eq(
    model: &EntityModel,
    schema: &SchemaInfo,
    field: &str,
    value: &Value,
) -> Result<Option<Vec<AccessPlan<Value>>>, InternalError> {
    let Some(field_type) = schema.field(field) else {
        return Ok(None);
    };

    if !literal_matches_type(value, field_type) {
        return Ok(None);
    }

    if fingerprint::to_index_fingerprint(value)?.is_none() {
        return Ok(None);
    }

    let mut out = Vec::new();
    for index in sorted_indexes(model) {
        if index.fields.first() != Some(&field) {
            continue;
        }
        out.push(AccessPlan::Path(AccessPath::IndexPrefix {
            index: *index,
            values: vec![value.clone()],
        }));
    }

    if out.is_empty() {
        Ok(None)
    } else {
        Ok(Some(out))
    }
}

fn index_prefix_from_and(
    model: &EntityModel,
    schema: &SchemaInfo,
    children: &[Predicate],
) -> Result<Option<AccessPath<Value>>, InternalError> {
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
            let Some((_, value)) = field_values.iter().find(|(name, _)| *name == *field) else {
                break;
            };
            let Some(field_type) = schema.field(field) else {
                prefix.clear();
                break;
            };
            if !literal_matches_type(value, field_type) {
                prefix.clear();
                break;
            }
            if fingerprint::to_index_fingerprint(value)?.is_none() {
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

    Ok(best.map(|(_, _, index, values)| AccessPath::IndexPrefix {
        index: *index,
        values,
    }))
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
    match plan {
        AccessPlan::Path(_) => plan,
        AccessPlan::Union(children) => normalize_union(children),
        AccessPlan::Intersection(children) => normalize_intersection(children),
    }
}

fn normalize_union(children: Vec<AccessPlan<Value>>) -> AccessPlan<Value> {
    let mut out = Vec::new();

    for child in children {
        let child = normalize_access_plan(child);
        if is_full_scan(&child) {
            return AccessPlan::full_scan();
        }

        match child {
            AccessPlan::Union(grand) => out.extend(grand),
            _ => out.push(child),
        }
    }

    // Collapse degenerate unions.
    if out.is_empty() {
        return AccessPlan::full_scan();
    }
    if out.len() == 1 {
        return out.pop().expect("single union child");
    }

    // Canonicalize and deduplicate for deterministic planning.
    canonical::canonicalize_access_plans_value(&mut out);
    out.dedup();
    if out.len() == 1 {
        return out.pop().expect("single union child");
    }
    AccessPlan::Union(out)
}

fn normalize_intersection(children: Vec<AccessPlan<Value>>) -> AccessPlan<Value> {
    let mut out = Vec::new();

    for child in children {
        let child = normalize_access_plan(child);
        if is_full_scan(&child) {
            continue;
        }

        match child {
            AccessPlan::Intersection(grand) => out.extend(grand),
            _ => out.push(child),
        }
    }

    // Collapse degenerate intersections.
    if out.is_empty() {
        return AccessPlan::full_scan();
    }
    if out.len() == 1 {
        return out.pop().expect("single intersection child");
    }

    // Canonicalize and deduplicate for deterministic planning.
    canonical::canonicalize_access_plans_value(&mut out);
    out.dedup();
    if out.len() == 1 {
        return out.pop().expect("single intersection child");
    }
    AccessPlan::Intersection(out)
}

const fn is_full_scan<K>(plan: &AccessPlan<K>) -> bool {
    matches!(plan, AccessPlan::Path(AccessPath::FullScan))
}

fn is_primary_key_model(schema: &SchemaInfo, model: &EntityModel, field: &str) -> bool {
    field == model.primary_key.name && schema.field(field).is_some()
}

fn value_matches_pk_model(schema: &SchemaInfo, model: &EntityModel, value: &Value) -> bool {
    let field = model.primary_key.name;
    let Some(field_type) = schema.field(field) else {
        return false;
    };

    is_key_compatible(field_type) && literal_matches_type(value, field_type)
}

const fn is_key_compatible(field_type: &FieldType) -> bool {
    matches!(
        field_type,
        FieldType::Scalar(
            ScalarType::Account
                | ScalarType::Int
                | ScalarType::Principal
                | ScalarType::Subaccount
                | ScalarType::Timestamp
                | ScalarType::Uint
                | ScalarType::Ulid
                | ScalarType::Unit
        )
    )
}
