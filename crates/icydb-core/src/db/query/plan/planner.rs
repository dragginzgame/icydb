//! Semantic planning from predicates to access strategies; must not assert invariants.
//!
//! Determinism: the planner canonicalizes output so the same model and
//! predicate shape always produce identical access plans.

use super::{AccessPath, AccessPlan, PlanError, canonical, validate_plan_invariants};
use crate::{
    db::{
        index::fingerprint,
        query::predicate::{
            CoercionId, CompareOp, ComparePredicate, Predicate, SchemaInfo, normalize,
            validate::{FieldType, ScalarType, literal_matches_type},
        },
    },
    error::InternalError,
    model::index::IndexModel,
    traits::{EntityKind, FieldValue},
    value::Value,
};
use thiserror::Error as ThisError;

impl<K> AccessPlan<K>
where
    K: Ord,
{
    fn normalize(self) -> Self {
        match self {
            Self::Path(_) => self,
            Self::Union(children) => normalize_union(children),
            Self::Intersection(children) => normalize_intersection(children),
        }
    }
}

#[derive(Debug, ThisError)]
pub(crate) enum PlannerError {
    #[error("{0}")]
    Plan(#[from] PlanError),
    #[error("{0}")]
    Internal(#[from] InternalError),
}

/// Planner entrypoint that operates on a prebuilt schema surface.
pub(crate) fn plan_access<E: EntityKind>(
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
) -> Result<AccessPlan<E::Id>, PlannerError> {
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
    let plan = plan_predicate::<E>(schema, &normalized)?.normalize();
    validate_plan_invariants::<E>(&plan, schema, Some(&normalized))?;
    Ok(plan)
}

fn plan_predicate<E: EntityKind>(
    schema: &SchemaInfo,
    predicate: &Predicate,
) -> Result<AccessPlan<E::Id>, InternalError> {
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
                .map(|child| plan_predicate::<E>(schema, child))
                .collect::<Result<Vec<_>, _>>()?;

            if let Some(prefix) = index_prefix_from_and::<E>(schema, children)? {
                plans.push(AccessPlan::Path(prefix));
            }

            AccessPlan::Intersection(plans)
        }
        Predicate::Or(children) => AccessPlan::Union(
            children
                .iter()
                .map(|child| plan_predicate::<E>(schema, child))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Predicate::Compare(cmp) => plan_compare::<E>(schema, cmp)?,
    };

    Ok(plan)
}

fn plan_compare<E: EntityKind>(
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
) -> Result<AccessPlan<E::Id>, InternalError> {
    if cmp.coercion.id != CoercionId::Strict {
        return Ok(AccessPlan::full_scan());
    }

    if is_primary_key::<E>(schema, &cmp.field)
        && let Some(path) = plan_pk_compare::<E>(schema, cmp)
    {
        return Ok(AccessPlan::Path(path));
    }

    match cmp.op {
        CompareOp::Eq => {
            if let Some(paths) = index_prefix_for_eq::<E>(schema, &cmp.field, &cmp.value)? {
                return Ok(AccessPlan::Union(paths));
            }
        }
        CompareOp::In => {
            if let Value::List(items) = &cmp.value {
                let mut plans = Vec::new();
                for item in items {
                    if let Some(paths) = index_prefix_for_eq::<E>(schema, &cmp.field, item)? {
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

fn plan_pk_compare<E: EntityKind>(
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
) -> Option<AccessPath<E::Id>> {
    match cmp.op {
        CompareOp::Eq => {
            if !value_matches_pk::<E>(schema, &cmp.value) {
                return None;
            }

            let key = <E::Id as FieldValue>::from_value(&cmp.value)?;
            Some(AccessPath::ByKey(key))
        }
        CompareOp::In => {
            let Value::List(items) = &cmp.value else {
                return None;
            };

            let mut keys = Vec::with_capacity(items.len());
            for item in items {
                if !value_matches_pk::<E>(schema, item) {
                    return None;
                }
                let key = <E::Id as FieldValue>::from_value(item)?;
                keys.push(key);
            }
            Some(AccessPath::ByKeys(keys))
        }
        _ => None,
    }
}

fn sorted_indexes<E: EntityKind>() -> Vec<&'static IndexModel> {
    let mut indexes = E::INDEXES.to_vec();
    indexes.sort_by(|left, right| left.name.cmp(right.name));
    indexes
}

fn index_prefix_for_eq<E: EntityKind>(
    schema: &SchemaInfo,
    field: &str,
    value: &Value,
) -> Result<Option<Vec<AccessPlan<E::Id>>>, InternalError> {
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
    for index in sorted_indexes::<E>() {
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

fn index_prefix_from_and<E: EntityKind>(
    schema: &SchemaInfo,
    children: &[Predicate],
) -> Result<Option<AccessPath<E::Id>>, InternalError> {
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
    for index in sorted_indexes::<E>() {
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

fn normalize_union<K>(children: Vec<AccessPlan<K>>) -> AccessPlan<K>
where
    K: Ord,
{
    let mut out = Vec::new();

    for child in children {
        let child = child.normalize();
        if is_full_scan(&child) {
            return AccessPlan::full_scan();
        }

        match child {
            AccessPlan::Union(grand) => out.extend(grand),
            _ => out.push(child),
        }
    }

    if out.is_empty() {
        return AccessPlan::full_scan();
    }
    if out.len() == 1 {
        return out.pop().expect("single union child");
    }

    canonical::canonicalize_access_plans(&mut out);
    out.dedup();
    if out.len() == 1 {
        return out.pop().expect("single union child");
    }
    AccessPlan::Union(out)
}

fn normalize_intersection<K>(children: Vec<AccessPlan<K>>) -> AccessPlan<K>
where
    K: Ord,
{
    let mut out = Vec::new();

    for child in children {
        let child = child.normalize();
        if is_full_scan(&child) {
            continue;
        }

        match child {
            AccessPlan::Intersection(grand) => out.extend(grand),
            _ => out.push(child),
        }
    }

    if out.is_empty() {
        return AccessPlan::full_scan();
    }
    if out.len() == 1 {
        return out.pop().expect("single intersection child");
    }

    canonical::canonicalize_access_plans(&mut out);
    out.dedup();
    if out.len() == 1 {
        return out.pop().expect("single intersection child");
    }
    AccessPlan::Intersection(out)
}

const fn is_full_scan<K>(plan: &AccessPlan<K>) -> bool {
    matches!(plan, AccessPlan::Path(AccessPath::FullScan))
}

fn is_primary_key<E: EntityKind>(schema: &SchemaInfo, field: &str) -> bool {
    field == E::PRIMARY_KEY && schema.field(field).is_some()
}

fn value_matches_pk<E: EntityKind>(schema: &SchemaInfo, value: &Value) -> bool {
    let field = E::PRIMARY_KEY;
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
