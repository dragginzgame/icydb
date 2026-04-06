//! Module: query::plan::planner::range
//! Responsibility: planner range-constraint extraction and index-range candidate derivation.
//! Does not own: runtime range traversal execution or cursor persistence format.
//! Boundary: computes planner-side range constraints from predicate semantics.

use crate::{
    db::{
        access::{AccessPlan, SemanticIndexRangeSpec},
        index::next_text_prefix,
        numeric::compare_numeric_or_strict_order,
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate, canonical_cmp},
        query::plan::{
            key_item_match::{eq_lookup_value_for_key_item, starts_with_lookup_value_for_key_item},
            planner::{index_literal_matches_schema, sorted_indexes},
        },
        schema::{SchemaInfo, literal_matches_type},
    },
    model::{
        entity::EntityModel,
        index::{IndexKeyItem, IndexKeyItemsRef, IndexModel},
    },
    value::Value,
};
use std::{cmp::Ordering, ops::Bound};

// Build one deterministic primary-key half-open range candidate from one
// canonical AND-group.
//
// Phase 1 intentionally admits only the exact safe shape:
// - every child is a Compare predicate
// - every child targets the primary key
// - coercion is Strict
// - one lower bound is `>=`
// - one upper bound is `<`
// - literals already match the primary-key type
pub(in crate::db::query::plan::planner) fn primary_key_range_from_and(
    model: &EntityModel,
    schema: &SchemaInfo,
    children: &[Predicate],
) -> Option<AccessPlan<Value>> {
    let field_type = schema.field(model.primary_key.name)?;
    if !field_type.is_keyable() {
        return None;
    }

    let mut lower = None::<Value>;
    let mut upper = None::<Value>;

    for child in children {
        let Predicate::Compare(cmp) = child else {
            return None;
        };
        if cmp.field != model.primary_key.name || cmp.coercion.id != CoercionId::Strict {
            return None;
        }
        if !literal_matches_type(&cmp.value, field_type) {
            return None;
        }

        match cmp.op {
            CompareOp::Gte if lower.is_none() => lower = Some(cmp.value.clone()),
            CompareOp::Lt if upper.is_none() => upper = Some(cmp.value.clone()),
            _ => return None,
        }
    }

    let (Some(start), Some(end)) = (lower, upper) else {
        return None;
    };
    if canonical_cmp(&start, &end) != Ordering::Less {
        return None;
    }

    Some(AccessPlan::key_range(start, end))
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

///
/// CachedCompare
///
/// Compare predicate plus precomputed planner-side schema compatibility.
///

#[derive(Clone)]
struct CachedCompare<'a> {
    cmp: &'a ComparePredicate,
    literal_compatible: bool,
}

// Build one deterministic secondary-range candidate from a normalized AND-group.
//
// Extraction contract:
// - Every child must be a Compare predicate.
// - Supported operators are Eq/Gt/Gte/Lt/Lte plus StartsWith.
// - For a chosen index: slots 0..k must be Eq, slot k must be Range,
//   slots after k must be unconstrained.
pub(in crate::db::query::plan::planner) fn index_range_from_and(
    model: &EntityModel,
    schema: &SchemaInfo,
    children: &[Predicate],
    query_predicate: &Predicate,
) -> Option<SemanticIndexRangeSpec> {
    let mut compares = Vec::with_capacity(children.len());
    for child in children {
        let Predicate::Compare(cmp) = child else {
            return None;
        };
        if !matches!(
            cmp.op,
            CompareOp::Eq
                | CompareOp::Gt
                | CompareOp::Gte
                | CompareOp::Lt
                | CompareOp::Lte
                | CompareOp::StartsWith
        ) {
            return None;
        }
        if !matches!(
            (cmp.op, cmp.coercion.id),
            (
                CompareOp::Eq
                    | CompareOp::StartsWith
                    | CompareOp::Gt
                    | CompareOp::Gte
                    | CompareOp::Lt
                    | CompareOp::Lte,
                CoercionId::Strict | CoercionId::TextCasefold
            )
        ) {
            return None;
        }
        compares.push(CachedCompare {
            cmp,
            literal_compatible: index_literal_matches_schema(schema, &cmp.field, &cmp.value),
        });
    }

    let mut best: Option<(
        usize,
        &'static IndexModel,
        usize,
        Vec<Value>,
        RangeConstraint,
    )> = None;
    for index in sorted_indexes(model, query_predicate) {
        let Some((range_slot, prefix, range)) =
            index_range_candidate_for_index(index, schema, &compares)
        else {
            continue;
        };

        let prefix_len = prefix.len();
        match best {
            None => best = Some((prefix_len, index, range_slot, prefix, range)),
            Some((best_len, best_index, _, _, _))
                if prefix_len > best_len
                    || (prefix_len == best_len && index.name() < best_index.name()) =>
            {
                best = Some((prefix_len, index, range_slot, prefix, range));
            }
            _ => {}
        }
    }

    best.map(|(_, index, range_slot, prefix, range)| {
        let field_slots = (0..=range_slot).collect();

        SemanticIndexRangeSpec::new(*index, field_slots, prefix, range.lower, range.upper)
    })
}

// Extract an index-range candidate for one concrete index by walking canonical
// key slots directly instead of field names. That keeps mixed field/expression
// indexes on the same planner contract as field-only indexes.
fn index_range_candidate_for_index(
    index: &'static IndexModel,
    schema: &SchemaInfo,
    compares: &[CachedCompare<'_>],
) -> Option<(usize, Vec<Value>, RangeConstraint)> {
    let mut prefix = Vec::new();
    let mut range: Option<RangeConstraint> = None;
    let mut range_position = None;

    match index.key_items() {
        IndexKeyItemsRef::Fields(fields) => {
            for (position, &field_name) in fields.iter().enumerate() {
                let constraint = key_item_constraint_for_index_slot(
                    index,
                    schema,
                    IndexKeyItem::Field(field_name),
                    compares,
                )?;
                if !consume_index_slot_constraint(
                    &mut prefix,
                    &mut range,
                    &mut range_position,
                    position,
                    constraint,
                ) {
                    return None;
                }
            }
        }
        IndexKeyItemsRef::Items(items) => {
            for (position, &key_item) in items.iter().enumerate() {
                let constraint =
                    key_item_constraint_for_index_slot(index, schema, key_item, compares)?;
                if !consume_index_slot_constraint(
                    &mut prefix,
                    &mut range,
                    &mut range_position,
                    position,
                    constraint,
                ) {
                    return None;
                }
            }
        }
    }

    let (Some(range_position), Some(range)) = (range_position, range) else {
        return None;
    };
    if prefix.len() >= index.fields().len() {
        return None;
    }

    Some((range_position, prefix, range))
}

// Consume one canonical slot constraint into the contiguous prefix/range
// extractor state machine.
fn consume_index_slot_constraint(
    prefix: &mut Vec<Value>,
    range: &mut Option<RangeConstraint>,
    range_position: &mut Option<usize>,
    position: usize,
    constraint: IndexFieldConstraint,
) -> bool {
    match constraint {
        IndexFieldConstraint::Eq(value) if range.is_none() => {
            prefix.push(value);
            true
        }
        IndexFieldConstraint::Range(candidate) if range.is_none() => {
            *range = Some(candidate);
            *range_position = Some(position);
            true
        }
        IndexFieldConstraint::None if range.is_none() => false,
        IndexFieldConstraint::None => true,
        _ => false,
    }
}

// Build the effective constraint class for one canonical index slot from the
// compare predicates that can lower onto that slot.
fn key_item_constraint_for_index_slot(
    index: &'static IndexModel,
    schema: &SchemaInfo,
    key_item: IndexKeyItem,
    compares: &[CachedCompare<'_>],
) -> Option<IndexFieldConstraint> {
    let mut constraint = IndexFieldConstraint::None;
    let field_type = schema.field(key_item.field())?;

    for cached in compares {
        let cmp = cached.cmp;
        if cmp.field.as_str() != key_item.field() {
            continue;
        }
        if matches!(key_item, IndexKeyItem::Field(_))
            && cmp.coercion.id == CoercionId::Strict
            && !field_type.is_orderable()
        {
            return None;
        }

        match cmp.op {
            CompareOp::Eq => match &constraint {
                IndexFieldConstraint::None => {
                    let Some(candidate) = eq_lookup_value_for_key_item(
                        key_item,
                        cmp.field.as_str(),
                        &cmp.value,
                        cmp.coercion.id,
                        cached.literal_compatible,
                    ) else {
                        continue;
                    };
                    constraint = IndexFieldConstraint::Eq(candidate);
                }
                IndexFieldConstraint::Eq(existing) => {
                    let Some(candidate) = eq_lookup_value_for_key_item(
                        key_item,
                        cmp.field.as_str(),
                        &cmp.value,
                        cmp.coercion.id,
                        cached.literal_compatible,
                    ) else {
                        continue;
                    };
                    if existing != &candidate {
                        return None;
                    }
                }
                IndexFieldConstraint::Range(_) => return None,
            },
            CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
                merge_ordered_compare_constraint_for_key_item(
                    index,
                    key_item,
                    cached,
                    &mut constraint,
                )?;
            }
            CompareOp::StartsWith => {
                let Some(prefix) = starts_with_lookup_value_for_key_item(
                    key_item,
                    cmp.field.as_str(),
                    &cmp.value,
                    cmp.coercion.id,
                    cached.literal_compatible,
                ) else {
                    continue;
                };

                let candidate = RangeConstraint {
                    lower: Bound::Included(Value::Text(prefix.clone())),
                    upper: match key_item {
                        IndexKeyItem::Field(_) => strict_text_prefix_upper_bound(&prefix),
                        IndexKeyItem::Expression(_) => Bound::Unbounded,
                    },
                };
                let mut range = match &constraint {
                    IndexFieldConstraint::None => candidate.clone(),
                    IndexFieldConstraint::Eq(_) => return None,
                    IndexFieldConstraint::Range(existing) => existing.clone(),
                };
                if !merge_range_constraint_bounds(&mut range, &candidate) {
                    return None;
                }
                constraint = IndexFieldConstraint::Range(range);
            }
            _ => return None,
        }
    }

    Some(constraint)
}

// Merge one ordered compare onto one canonical key-item slot.
// This keeps Eq/In/prefix/range on the same canonical literal-lowering path
// for both raw field keys and the accepted TextCasefold expression keys.
fn merge_ordered_compare_constraint_for_key_item(
    index: &'static IndexModel,
    key_item: IndexKeyItem,
    cached: &CachedCompare<'_>,
    constraint: &mut IndexFieldConstraint,
) -> Option<()> {
    let cmp = cached.cmp;
    let candidate = eq_lookup_value_for_key_item(
        key_item,
        cmp.field.as_str(),
        &cmp.value,
        cmp.coercion.id,
        cached.literal_compatible,
    )?;

    match key_item {
        IndexKeyItem::Field(_) => {
            if cmp.coercion.id != CoercionId::Strict
                || !index.is_field_indexable(key_item.field(), cmp.op)
            {
                return Some(());
            }
        }
        IndexKeyItem::Expression(_) => {
            if cmp.coercion.id != CoercionId::TextCasefold {
                return Some(());
            }
        }
    }

    let mut range = match constraint {
        IndexFieldConstraint::None => RangeConstraint::default(),
        IndexFieldConstraint::Eq(_) => return None,
        IndexFieldConstraint::Range(existing) => existing.clone(),
    };
    if !merge_range_constraint(&mut range, cmp.op, &candidate) {
        return None;
    }

    *constraint = IndexFieldConstraint::Range(range);
    Some(())
}

// Merge one comparison operator into one bounded range under shared numeric
// comparison semantics for numeric values and strict ordering semantics for
// non-numeric values.
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

// Merge one pre-built bounded interval into the current constraint so
// STARTS_WITH can share the same compatibility checks as explicit inequalities.
fn merge_range_constraint_bounds(
    existing: &mut RangeConstraint,
    candidate: &RangeConstraint,
) -> bool {
    if !merge_lower_bound(&mut existing.lower, candidate.lower.clone()) {
        return false;
    }
    if !merge_upper_bound(&mut existing.upper, candidate.upper.clone()) {
        return false;
    }

    range_bounds_are_compatible(existing)
}

fn strict_text_prefix_upper_bound(prefix: &str) -> Bound<Value> {
    next_text_prefix(prefix).map_or(Bound::Unbounded, |next_prefix| {
        Bound::Excluded(Value::Text(next_prefix))
    })
}

fn merge_lower_bound(existing: &mut Bound<Value>, candidate: Bound<Value>) -> bool {
    let replace = match (&candidate, &*existing) {
        (Bound::Unbounded, _) => false,
        (_, Bound::Unbounded) => true,
        (
            Bound::Included(left) | Bound::Excluded(left),
            Bound::Included(right) | Bound::Excluded(right),
        ) => match compare_range_bound_values(left, right) {
            Some(Ordering::Greater) => true,
            Some(Ordering::Less) => false,
            Some(Ordering::Equal) => {
                matches!(candidate, Bound::Excluded(_)) && matches!(existing, Bound::Included(_))
            }
            None => return false,
        },
    };

    if replace {
        *existing = candidate;
    }

    true
}

fn merge_upper_bound(existing: &mut Bound<Value>, candidate: Bound<Value>) -> bool {
    let replace = match (&candidate, &*existing) {
        (Bound::Unbounded, _) => false,
        (_, Bound::Unbounded) => true,
        (
            Bound::Included(left) | Bound::Excluded(left),
            Bound::Included(right) | Bound::Excluded(right),
        ) => match compare_range_bound_values(left, right) {
            Some(Ordering::Less) => true,
            Some(Ordering::Greater) => false,
            Some(Ordering::Equal) => {
                matches!(candidate, Bound::Excluded(_)) && matches!(existing, Bound::Included(_))
            }
            None => return false,
        },
    };

    if replace {
        *existing = candidate;
    }

    true
}

// Validate interval shape and reject empty or incomparable intervals.
fn range_bounds_are_compatible(range: &RangeConstraint) -> bool {
    let (Some(lower), Some(upper)) = (bound_value(&range.lower), bound_value(&range.upper)) else {
        return true;
    };

    let Some(ordering) = compare_range_bound_values(lower, upper) else {
        return false;
    };

    match ordering {
        Ordering::Less => true,
        Ordering::Greater => false,
        Ordering::Equal => {
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

fn compare_range_bound_values(left: &Value, right: &Value) -> Option<Ordering> {
    if let Some(ordering) = compare_numeric_or_strict_order(left, right) {
        return Some(ordering);
    }

    if std::mem::discriminant(left) == std::mem::discriminant(right) {
        return Some(canonical_cmp(left, right));
    }

    None
}
///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            numeric::compare_numeric_or_strict_order,
            query::plan::planner::range::compare_range_bound_values,
        },
        value::Value,
    };
    use std::cmp::Ordering;

    #[test]
    fn range_bound_numeric_compare_reuses_shared_numeric_authority() {
        let left = Value::Int(10);
        let right = Value::Uint(10);

        assert_eq!(
            compare_range_bound_values(&left, &right),
            compare_numeric_or_strict_order(&left, &right),
            "planner range numeric bounds should delegate to shared numeric comparator",
        );
    }

    #[test]
    fn range_bound_mixed_non_numeric_values_are_incomparable() {
        assert_eq!(
            compare_range_bound_values(&Value::Text("x".to_string()), &Value::Uint(1)),
            None,
            "mixed non-numeric variants should remain incomparable in range planning",
        );
    }

    #[test]
    fn range_bound_same_variant_non_numeric_uses_strict_ordering() {
        assert_eq!(
            compare_range_bound_values(
                &Value::Text("a".to_string()),
                &Value::Text("b".to_string())
            ),
            Some(Ordering::Less),
            "same-variant non-numeric bounds should use strict value ordering",
        );
    }
}
