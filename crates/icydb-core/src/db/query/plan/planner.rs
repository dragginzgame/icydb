//! Semantic planning from predicates to access strategies; must not assert invariants.
//!
//! Determinism: the planner canonicalizes output so the same model and
//! predicate shape always produce identical access plans.

use super::{AccessPath, AccessPlan, PlanError, canonical};
use crate::{
    db::{
        index::key::encode_canonical_index_component,
        query::predicate::{
            CoercionId, CompareOp, ComparePredicate, Predicate, SchemaInfo,
            coercion::canonical_cmp, normalize, validate::literal_matches_type,
        },
    },
    error::InternalError,
    model::entity::EntityModel,
    model::index::IndexModel,
    value::{CoercionFamily, CoercionFamilyExt, Value},
};
use std::{mem::discriminant, ops::Bound};
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
    let normalized = normalize(predicate);
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
            if let Some(range) = index_range_from_and(model, schema, children) {
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
            if index_range_literal_is_compatible(schema, &cmp.field, &cmp.value) {
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

///
/// RangeConstraint
/// One-field bounded interval used for index-range candidate extraction.
///

#[derive(Clone, Debug, Eq, PartialEq)]
struct RangeConstraint {
    lower: Bound<Value>,
    upper: Bound<Value>,
}

impl Default for RangeConstraint {
    fn default() -> Self {
        Self {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        }
    }
}

///
/// IndexFieldConstraint
/// Per-index-field constraint classification while extracting range candidates.
///

#[derive(Clone, Debug, Eq, PartialEq)]
enum IndexFieldConstraint {
    None,
    Eq(Value),
    Range(RangeConstraint),
}

// Build one deterministic secondary-range candidate from a normalized AND-group.
//
// Extraction contract:
// - Every child must be a Compare predicate.
// - Supported operators are Eq/Gt/Gte/Lt/Lte only.
// - For a chosen index: fields 0..k must be Eq, field k must be Range,
//   fields after k must be unconstrained.
fn index_range_from_and(
    model: &EntityModel,
    schema: &SchemaInfo,
    children: &[Predicate],
) -> Option<AccessPath<Value>> {
    let mut compares = Vec::with_capacity(children.len());
    for child in children {
        let Predicate::Compare(cmp) = child else {
            return None;
        };
        if !matches!(
            cmp.op,
            CompareOp::Eq | CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte
        ) {
            return None;
        }
        if !matches!(
            cmp.coercion.id,
            CoercionId::Strict | CoercionId::NumericWiden
        ) {
            return None;
        }
        compares.push(cmp);
    }

    let mut best: Option<(usize, &'static IndexModel, Vec<Value>, RangeConstraint)> = None;
    for index in sorted_indexes(model) {
        let Some((prefix, range)) = index_range_candidate_for_index(schema, index, &compares)
        else {
            continue;
        };

        let prefix_len = prefix.len();
        match best {
            None => best = Some((prefix_len, index, prefix, range)),
            Some((best_len, best_index, _, _))
                if prefix_len > best_len
                    || (prefix_len == best_len && index.name < best_index.name) =>
            {
                best = Some((prefix_len, index, prefix, range));
            }
            _ => {}
        }
    }

    best.map(|(_, index, prefix, range)| AccessPath::IndexRange {
        index: *index,
        prefix,
        lower: range.lower,
        upper: range.upper,
    })
}

// Extract an index-range candidate for one concrete index.
fn index_range_candidate_for_index(
    schema: &SchemaInfo,
    index: &'static IndexModel,
    compares: &[&ComparePredicate],
) -> Option<(Vec<Value>, RangeConstraint)> {
    let mut constraints = vec![IndexFieldConstraint::None; index.fields.len()];

    for cmp in compares {
        let Some(position) = index
            .fields
            .iter()
            .position(|field| *field == cmp.field.as_str())
        else {
            continue;
        };
        let field = index.fields[position];
        if !index_range_literal_is_compatible(schema, field, &cmp.value) {
            return None;
        }

        match cmp.op {
            CompareOp::Eq => match &mut constraints[position] {
                IndexFieldConstraint::None => {
                    constraints[position] = IndexFieldConstraint::Eq(cmp.value.clone());
                }
                IndexFieldConstraint::Eq(existing) => {
                    if existing != &cmp.value {
                        return None;
                    }
                }
                IndexFieldConstraint::Range(_) => return None,
            },
            CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
                let mut range = match &constraints[position] {
                    IndexFieldConstraint::None => RangeConstraint::default(),
                    IndexFieldConstraint::Eq(_) => return None,
                    IndexFieldConstraint::Range(existing) => existing.clone(),
                };
                if !merge_range_constraint(&mut range, cmp.op, &cmp.value) {
                    return None;
                }
                constraints[position] = IndexFieldConstraint::Range(range);
            }
            _ => return None,
        }
    }

    let mut prefix = Vec::new();
    let mut range: Option<RangeConstraint> = None;
    let mut range_position = None;

    for (position, constraint) in constraints.iter().enumerate() {
        match constraint {
            IndexFieldConstraint::Eq(value) if range.is_none() => {
                prefix.push(value.clone());
            }
            IndexFieldConstraint::Range(candidate) if range.is_none() => {
                range = Some(candidate.clone());
                range_position = Some(position);
            }
            IndexFieldConstraint::None if range.is_none() => return None,
            IndexFieldConstraint::None => {}
            _ => return None,
        }
    }

    let (Some(range_position), Some(range)) = (range_position, range) else {
        return None;
    };
    if range_position >= index.fields.len() {
        return None;
    }
    if prefix.len() >= index.fields.len() {
        return None;
    }

    Some((prefix, range))
}

// Merge one comparison operator into a bounded range without widening semantics.
fn merge_range_constraint(existing: &mut RangeConstraint, op: CompareOp, value: &Value) -> bool {
    let merged = match op {
        CompareOp::Gt => merge_lower_bound(&mut existing.lower, Bound::Excluded(value.clone())),
        CompareOp::Gte => merge_lower_bound(&mut existing.lower, Bound::Included(value.clone())),
        CompareOp::Lt => merge_upper_bound(&mut existing.upper, Bound::Excluded(value.clone())),
        CompareOp::Lte => merge_upper_bound(&mut existing.upper, Bound::Included(value.clone())),
        _ => false,
    };
    if !merged {
        return false;
    }

    range_bounds_are_compatible(existing)
}

fn merge_lower_bound(existing: &mut Bound<Value>, candidate: Bound<Value>) -> bool {
    if !bounds_numeric_variants_compatible(existing, &candidate) {
        return false;
    }

    let replace = match (&candidate, &*existing) {
        (Bound::Unbounded, _) => false,
        (_, Bound::Unbounded) => true,
        (
            Bound::Included(left) | Bound::Excluded(left),
            Bound::Included(right) | Bound::Excluded(right),
        ) => match canonical_cmp(left, right) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => {
                matches!(candidate, Bound::Excluded(_)) && matches!(existing, Bound::Included(_))
            }
        },
    };

    if replace {
        *existing = candidate;
    }

    true
}

fn merge_upper_bound(existing: &mut Bound<Value>, candidate: Bound<Value>) -> bool {
    if !bounds_numeric_variants_compatible(existing, &candidate) {
        return false;
    }

    let replace = match (&candidate, &*existing) {
        (Bound::Unbounded, _) => false,
        (_, Bound::Unbounded) => true,
        (
            Bound::Included(left) | Bound::Excluded(left),
            Bound::Included(right) | Bound::Excluded(right),
        ) => match canonical_cmp(left, right) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Greater => false,
            std::cmp::Ordering::Equal => {
                matches!(candidate, Bound::Excluded(_)) && matches!(existing, Bound::Included(_))
            }
        },
    };

    if replace {
        *existing = candidate;
    }

    true
}

// Validate interval shape and reject empty/mixed-numeric intervals.
fn range_bounds_are_compatible(range: &RangeConstraint) -> bool {
    let (Some(lower), Some(upper)) = (bound_value(&range.lower), bound_value(&range.upper)) else {
        return true;
    };

    if !numeric_variants_compatible(lower, upper) {
        return false;
    }

    match canonical_cmp(lower, upper) {
        std::cmp::Ordering::Less => true,
        std::cmp::Ordering::Greater => false,
        std::cmp::Ordering::Equal => {
            matches!(range.lower, Bound::Included(_)) && matches!(range.upper, Bound::Included(_))
        }
    }
}

const fn bound_value(bound: &Bound<Value>) -> Option<&Value> {
    match bound {
        Bound::Included(value) | Bound::Excluded(value) => Some(value),
        Bound::Unbounded => None,
    }
}

fn bounds_numeric_variants_compatible(left: &Bound<Value>, right: &Bound<Value>) -> bool {
    match (bound_value(left), bound_value(right)) {
        (Some(left), Some(right)) => numeric_variants_compatible(left, right),
        _ => true,
    }
}

fn numeric_variants_compatible(left: &Value, right: &Value) -> bool {
    if left.coercion_family() != CoercionFamily::Numeric
        || right.coercion_family() != CoercionFamily::Numeric
    {
        return true;
    }

    discriminant(left) == discriminant(right)
}

// Normalize composite access plans into canonical, flattened forms.
fn normalize_access_plan(plan: AccessPlan<Value>) -> AccessPlan<Value> {
    match plan {
        AccessPlan::Path(path) => AccessPlan::path(normalize_access_path(*path)),
        AccessPlan::Union(children) => normalize_union(children),
        AccessPlan::Intersection(children) => normalize_intersection(children),
    }
}

// Normalize ByKeys paths to set semantics for deterministic planning.
fn normalize_access_path(path: AccessPath<Value>) -> AccessPath<Value> {
    match path {
        AccessPath::ByKeys(mut keys) => {
            canonical::canonicalize_key_values(&mut keys);
            AccessPath::ByKeys(keys)
        }
        other => other,
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

fn is_full_scan<K>(plan: &AccessPlan<K>) -> bool {
    matches!(plan, AccessPlan::Path(path) if matches!(path.as_ref(), AccessPath::FullScan))
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
fn index_prefix_literal_is_compatible(schema: &SchemaInfo, field: &str, value: &Value) -> bool {
    let Some(field_type) = schema.field(field) else {
        return false;
    };

    literal_matches_type(value, field_type) && encode_canonical_index_component(value).is_ok()
}

// Validate one range literal for index-range planning.
fn index_range_literal_is_compatible(schema: &SchemaInfo, field: &str, value: &Value) -> bool {
    index_prefix_literal_is_compatible(schema, field, value)
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
