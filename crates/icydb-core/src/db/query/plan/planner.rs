//! Semantic planning from predicates to access strategies; must not assert invariants.
//!
//! Determinism: the planner canonicalizes output so the same model and
//! predicate shape always produce identical access plans.

use crate::{
    db::{
        access::{AccessPath, AccessPlan, SemanticIndexRangeSpec},
        predicate::{
            CoercionId, CompareOp, ComparePredicate, Predicate, SchemaInfo, literal_matches_type,
            normalize as normalize_predicate,
        },
        query::plan::PlanError,
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
    let plan = normalize::normalize_access_plan(plan_predicate(model, schema, &normalized)?);

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
            if let Some(range_spec) = range::index_range_from_and(model, schema, children) {
                return Ok(AccessPlan::path(AccessPath::IndexRange {
                    spec: range_spec,
                }));
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

            AccessPlan::intersection(plans)
        }
        Predicate::Or(children) => AccessPlan::union(
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
                return AccessPlan::union(paths);
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
                    return AccessPlan::union(plans);
                }
            }
        }
        CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
            // Single compare predicates only map directly to one-field indexes.
            // Composite prefix+range extraction remains AND-group driven.
            if index_literal_matches_schema(schema, &cmp.field, &cmp.value) {
                let (lower, upper) = match cmp.op {
                    CompareOp::Gt => (Bound::Excluded(cmp.value.clone()), Bound::Unbounded),
                    CompareOp::Gte => (Bound::Included(cmp.value.clone()), Bound::Unbounded),
                    CompareOp::Lt => (Bound::Unbounded, Bound::Excluded(cmp.value.clone())),
                    CompareOp::Lte => (Bound::Unbounded, Bound::Included(cmp.value.clone())),
                    _ => unreachable!("range arm must be one of Gt/Gte/Lt/Lte"),
                };

                for index in sorted_indexes(model) {
                    if index.fields.len() == 1
                        && index.fields[0] == cmp.field.as_str()
                        && index.is_field_indexable(&cmp.field, cmp.op)
                    {
                        let semantic_range = SemanticIndexRangeSpec::new(
                            *index,
                            vec![0usize],
                            Vec::new(),
                            lower,
                            upper,
                        );

                        return AccessPlan::path(AccessPath::IndexRange {
                            spec: semantic_range,
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
    if !index_literal_matches_schema(schema, field, value) {
        return None;
    }

    let mut out = Vec::new();
    for index in sorted_indexes(model) {
        if index.fields.first() != Some(&field) || !index.is_field_indexable(field, CompareOp::Eq) {
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
    // Cache literal/schema compatibility once per equality literal so index
    // candidate selection does not repeat schema checks on every index iteration.
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
        field_values.push(CachedEqLiteral {
            field: cmp.field.as_str(),
            value: &cmp.value,
            compatible: index_literal_matches_schema(schema, &cmp.field, &cmp.value),
        });
    }

    let mut best: Option<(usize, bool, &IndexModel, Vec<Value>)> = None;
    for index in sorted_indexes(model) {
        let mut prefix = Vec::new();
        for field in index.fields {
            // NOTE: duplicate equality predicates on the same field are assumed
            // to have been validated upstream (no conflict). Planner picks the first.
            let Some(cached) = field_values.iter().find(|cached| cached.field == *field) else {
                break;
            };
            if !index.is_field_indexable(field, CompareOp::Eq) || !cached.compatible {
                prefix.clear();
                break;
            }
            prefix.push(cached.value.clone());
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

///
/// CachedEqLiteral
///
/// Equality literal plus its precomputed planner-side schema compatibility.
///

struct CachedEqLiteral<'a> {
    field: &'a str,
    value: &'a Value,
    compatible: bool,
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

pub(in crate::db::query::plan::planner) fn index_literal_matches_schema(
    schema: &SchemaInfo,
    field: &str,
    value: &Value,
) -> bool {
    let Some(field_type) = schema.field(field) else {
        return false;
    };
    if !literal_matches_type(value, field_type) {
        return false;
    }

    true
}

impl IndexModel {
    /// Return true when this index can structurally support the field/operator pair.
    #[must_use]
    pub(in crate::db::query::plan::planner) fn is_field_indexable(
        &self,
        field: &str,
        op: CompareOp,
    ) -> bool {
        if !self.fields.contains(&field) {
            return false;
        }

        matches!(
            op,
            CompareOp::Eq
                | CompareOp::In
                | CompareOp::Gt
                | CompareOp::Gte
                | CompareOp::Lt
                | CompareOp::Lte
        )
    }
}

///
/// TESTS
///

#[cfg(test)]
mod planner_tests {
    use super::*;
    use crate::types::Ulid;

    #[test]
    fn normalize_union_dedups_identical_paths() {
        let key = Value::Ulid(Ulid::from_u128(1));
        let plan = AccessPlan::Union(vec![
            AccessPlan::path(AccessPath::ByKey(key.clone())),
            AccessPlan::path(AccessPath::ByKey(key)),
        ]);

        let normalized = normalize::normalize_access_plan(plan);

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

        let normalized = normalize::normalize_access_plan(plan);
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

        let normalized = normalize::normalize_access_plan(plan);

        assert_eq!(
            normalized,
            AccessPlan::path(AccessPath::ByKey(Value::Ulid(Ulid::from_u128(7))))
        );
    }
}

mod normalize {
    use crate::{
        db::{
            access::{AccessPath, AccessPlan},
            query::plan::canonical,
        },
        value::Value,
    };

    // Normalize composite access plans into canonical, flattened forms.
    pub(in crate::db::query::plan::planner) fn normalize_access_plan(
        plan: AccessPlan<Value>,
    ) -> AccessPlan<Value> {
        plan.normalize_for_planner()
    }

    impl AccessPlan<Value> {
        // Normalize this access plan into a canonical deterministic form.
        fn normalize_for_planner(self) -> Self {
            match self {
                Self::Path(path) => Self::path(path.normalize_for_planner()),
                Self::Union(children) => Self::normalize_union(children),
                Self::Intersection(children) => Self::normalize_intersection(children),
            }
        }

        fn normalize_union(children: Vec<Self>) -> Self {
            let mut out = Vec::new();

            for child in children {
                let child = child.normalize_for_planner();
                if child.is_single_full_scan() {
                    return Self::full_scan();
                }

                Self::append_union_child(&mut out, child);
            }

            Self::collapse_composite(out, true)
        }

        fn normalize_intersection(children: Vec<Self>) -> Self {
            let mut out = Vec::new();

            for child in children {
                let child = child.normalize_for_planner();
                if child.is_single_full_scan() {
                    continue;
                }

                Self::append_intersection_child(&mut out, child);
            }

            Self::collapse_composite(out, false)
        }

        fn collapse_composite(mut out: Vec<Self>, is_union: bool) -> Self {
            if out.is_empty() {
                return Self::full_scan();
            }
            if out.len() == 1 {
                return out.pop().expect("single composite child");
            }

            canonical::canonicalize_access_plans_value(&mut out);
            out.dedup();
            if out.len() == 1 {
                return out.pop().expect("single composite child");
            }

            if is_union {
                Self::Union(out)
            } else {
                Self::Intersection(out)
            }
        }

        fn append_union_child(out: &mut Vec<Self>, child: Self) {
            match child {
                Self::Union(children) => out.extend(children),
                other => out.push(other),
            }
        }

        fn append_intersection_child(out: &mut Vec<Self>, child: Self) {
            match child {
                Self::Intersection(children) => out.extend(children),
                other => out.push(other),
            }
        }
    }

    impl AccessPath<Value> {
        // Normalize one concrete access path for deterministic planning.
        fn normalize_for_planner(self) -> Self {
            match self {
                Self::ByKeys(mut keys) => {
                    canonical::canonicalize_key_values(&mut keys);
                    Self::ByKeys(keys)
                }
                other => other,
            }
        }
    }
}

mod range {
    use crate::{
        db::{
            access::SemanticIndexRangeSpec,
            predicate::{
                CoercionId, CompareOp, ComparePredicate, Predicate, SchemaInfo, canonical_cmp,
            },
            query::plan::planner::{index_literal_matches_schema, sorted_indexes},
        },
        model::{entity::EntityModel, index::IndexModel},
        value::{CoercionFamily, CoercionFamilyExt, Value},
    };
    use std::{mem::discriminant, ops::Bound};

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
    // - Supported operators are Eq/Gt/Gte/Lt/Lte only.
    // - For a chosen index: fields 0..k must be Eq, field k must be Range,
    //   fields after k must be unconstrained.
    pub(in crate::db::query::plan::planner) fn index_range_from_and(
        model: &EntityModel,
        schema: &SchemaInfo,
        children: &[Predicate],
    ) -> Option<SemanticIndexRangeSpec> {
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
        for index in sorted_indexes(model) {
            let Some((range_slot, prefix, range)) =
                index_range_candidate_for_index(index, &compares)
            else {
                continue;
            };

            let prefix_len = prefix.len();
            match best {
                None => best = Some((prefix_len, index, range_slot, prefix, range)),
                Some((best_len, best_index, _, _, _))
                    if prefix_len > best_len
                        || (prefix_len == best_len && index.name < best_index.name) =>
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

    // Extract an index-range candidate for one concrete index.
    fn index_range_candidate_for_index(
        index: &'static IndexModel,
        compares: &[CachedCompare<'_>],
    ) -> Option<(usize, Vec<Value>, RangeConstraint)> {
        // Phase 1: classify each index field as Eq/Range/None for this compare set.
        let constraints = classify_index_field_constraints(index, compares)?;

        // Phase 2: materialize deterministic prefix+range shape from constraints.
        select_prefix_and_range(index.fields.len(), &constraints)
    }

    // Build per-field constraint classes for one index from compare predicates.
    fn classify_index_field_constraints(
        index: &'static IndexModel,
        compares: &[CachedCompare<'_>],
    ) -> Option<Vec<IndexFieldConstraint>> {
        let mut constraints = vec![IndexFieldConstraint::None; index.fields.len()];

        for cached in compares {
            let cmp = cached.cmp;
            let Some(position) = index
                .fields
                .iter()
                .position(|field| *field == cmp.field.as_str())
            else {
                continue;
            };

            if !cached.literal_compatible || !index.is_field_indexable(cmp.field.as_str(), cmp.op) {
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

        Some(constraints)
    }

    // Convert classified constraints into one valid prefix+range candidate shape.
    fn select_prefix_and_range(
        field_count: usize,
        constraints: &[IndexFieldConstraint],
    ) -> Option<(usize, Vec<Value>, RangeConstraint)> {
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
        if range_position >= field_count {
            return None;
        }
        if prefix.len() >= field_count {
            return None;
        }

        Some((range_position, prefix, range))
    }

    // Merge one comparison operator into a bounded range without widening semantics.
    fn merge_range_constraint(
        existing: &mut RangeConstraint,
        op: CompareOp,
        value: &Value,
    ) -> bool {
        let merged = match op {
            CompareOp::Gt => merge_lower_bound(&mut existing.lower, Bound::Excluded(value.clone())),
            CompareOp::Gte => {
                merge_lower_bound(&mut existing.lower, Bound::Included(value.clone()))
            }
            CompareOp::Lt => merge_upper_bound(&mut existing.upper, Bound::Excluded(value.clone())),
            CompareOp::Lte => {
                merge_upper_bound(&mut existing.upper, Bound::Included(value.clone()))
            }
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
                    matches!(candidate, Bound::Excluded(_))
                        && matches!(existing, Bound::Included(_))
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
                    matches!(candidate, Bound::Excluded(_))
                        && matches!(existing, Bound::Included(_))
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
        let (Some(lower), Some(upper)) = (bound_value(&range.lower), bound_value(&range.upper))
        else {
            return true;
        };

        if !numeric_variants_compatible(lower, upper) {
            return false;
        }

        !range_is_empty(range)
    }

    // Return true when a bounded range is empty under canonical value ordering.
    fn range_is_empty(range: &RangeConstraint) -> bool {
        let (Some(lower), Some(upper)) = (bound_value(&range.lower), bound_value(&range.upper))
        else {
            return false;
        };

        match canonical_cmp(lower, upper) {
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Equal => {
                !matches!(range.lower, Bound::Included(_))
                    || !matches!(range.upper, Bound::Included(_))
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
}
