//! Module: query::plan::planner::compare
//! Responsibility: planner compare-predicate access-path planning and index-range lowering.
//! Does not own: runtime comparator enforcement or continuation resume execution details.
//! Boundary: derives compare-driven `AccessPlan` semantics from schema/predicate contracts.

use crate::{
    db::{
        access::{
            AccessPlan, SemanticIndexAccessContract, SemanticIndexKeyItemRef,
            SemanticIndexKeyItemsRef, SemanticIndexRangeSpec,
        },
        index::{TextPrefixBoundMode, starts_with_component_bounds},
        predicate::{CoercionId, CompareOp, ComparePredicate},
        query::plan::{
            OrderSpec,
            key_item_match::{eq_lookup_value_for_key_item, starts_with_lookup_value_for_key_item},
            planner::{
                AccessCandidateScore, access_candidate_score_from_index_contract,
                access_candidate_score_outranks, index_literal_matches_schema,
                prefix::{index_multi_lookup_for_in, index_prefix_for_eq},
                range_bound_count,
            },
        },
        schema::{FieldType, SchemaInfo, literal_matches_type},
    },
    model::entity::EntityModel,
    value::Value,
};
use std::ops::Bound;

pub(super) fn plan_compare(
    model: &EntityModel,
    candidate_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
    order: Option<&OrderSpec>,
    grouped: bool,
) -> AccessPlan<Value> {
    let primary_key_name = schema.primary_key_name();
    if cmp.coercion.id == CoercionId::Strict
        && primary_key_name.is_some_and(|name| cmp.field == name)
        && let Some(field_type) = primary_key_name.and_then(|name| schema.field(name))
        && let Some(path) = plan_pk_compare(field_type, &cmp.value, cmp.op)
    {
        return path;
    }

    match cmp.op {
        CompareOp::Eq => {
            if !coercion_supports_index_lookup(cmp.coercion.id) {
                return AccessPlan::full_scan();
            }
            if let Some(paths) = index_prefix_for_eq(
                model,
                candidate_indexes,
                schema,
                &cmp.field,
                &cmp.value,
                cmp.coercion.id,
                order,
                grouped,
            ) {
                return paths;
            }
        }
        CompareOp::In => {
            if !coercion_supports_index_lookup(cmp.coercion.id) {
                return AccessPlan::full_scan();
            }
            if let Value::List(items) = &cmp.value {
                // Access canonicalization owns IN-list set normalization
                // (sorting/dedup and singleton collapse).
                // `IN ()` is a constant-empty predicate: no row can satisfy it.
                // Lower directly to an empty access shape instead of full-scan fallback.
                if items.is_empty() {
                    return AccessPlan::by_keys(Vec::new());
                }
                if let Some(paths) = index_multi_lookup_for_in(
                    model,
                    candidate_indexes,
                    schema,
                    &cmp.field,
                    items,
                    cmp.coercion.id,
                ) {
                    return AccessPlan::union(paths);
                }
            }
        }
        CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
            if !coercion_supports_index_lookup(cmp.coercion.id) {
                return AccessPlan::full_scan();
            }
            let Some(field_type) = schema.field(&cmp.field) else {
                return AccessPlan::full_scan();
            };
            if !field_supports_ordered_compare(field_type, cmp.coercion.id) {
                return AccessPlan::full_scan();
            }
            if let Some(path) =
                plan_ordered_compare(model, candidate_indexes, schema, cmp, order, grouped)
            {
                return path;
            }
        }
        CompareOp::StartsWith => {
            if !coercion_supports_index_lookup(cmp.coercion.id) {
                return AccessPlan::full_scan();
            }

            // Keep the starts-with split explicit:
            // - raw field-key text prefixes now lower onto the same bounded
            //   semantic range contract as equivalent `>=`/`< next_prefix`
            //   forms
            // - expression-key lookups still keep their lower-bounded shape
            //   because the derived expression ordering does not yet expose one
            //   tighter planner-owned upper-bound contract
            if let Some(path) =
                plan_starts_with_compare(model, candidate_indexes, schema, cmp, order, grouped)
            {
                return path;
            }
        }
        _ => {
            // NOTE: Other non-equality comparisons do not currently map to key access paths.
        }
    }

    AccessPlan::full_scan()
}

// Planner compare access only supports exact schema semantics or case-folded
// text semantics. Other coercions still require residual filter evaluation.
const fn coercion_supports_index_lookup(coercion: CoercionId) -> bool {
    matches!(coercion, CoercionId::Strict | CoercionId::TextCasefold)
}

// Ordered compare access has one tighter field-type contract on top of the
// generic lookup-coercion gate: strict comparisons require orderable fields,
// and case-folded compares are text-only.
const fn field_supports_ordered_compare(field_type: &FieldType, coercion: CoercionId) -> bool {
    match coercion {
        CoercionId::Strict => field_type.is_orderable(),
        CoercionId::TextCasefold => field_type.is_text(),
        _ => false,
    }
}

fn plan_pk_compare(
    field_type: &FieldType,
    value: &Value,
    op: CompareOp,
) -> Option<AccessPlan<Value>> {
    if !field_type.is_keyable() {
        return None;
    }

    match op {
        CompareOp::Eq => {
            if !literal_matches_type(value, field_type) {
                return None;
            }

            Some(AccessPlan::by_key(value.clone()))
        }
        CompareOp::In => {
            let Value::List(items) = value else {
                return None;
            };

            // Keep planner semantic-only: PK IN literal-set canonicalization is
            // performed by access-plan canonicalization.
            for item in items {
                if !literal_matches_type(item, field_type) {
                    return None;
                }
            }

            Some(AccessPlan::by_keys(items.clone()))
        }
        _ => {
            // NOTE: Only Eq/In comparisons can be expressed as key access paths.
            None
        }
    }
}

fn plan_starts_with_compare(
    model: &EntityModel,
    candidate_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
    order: Option<&OrderSpec>,
    grouped: bool,
) -> Option<AccessPlan<Value>> {
    // This helper owns the shared starts-with range lowering contract for both
    // raw field keys and the expression-key casefold path.
    let field_type = schema.field(&cmp.field)?;
    if !field_type.is_text() {
        return None;
    }
    let literal_compatible = index_literal_matches_schema(schema, &cmp.field, &cmp.value);
    let mut best: Option<(
        AccessCandidateScore,
        SemanticIndexAccessContract,
        Bound<Value>,
        Bound<Value>,
    )> = None;
    for index in candidate_indexes {
        let Some(leading_key_item) = index.key_item_at(0) else {
            continue;
        };
        let Some(prefix) = starts_with_lookup_value_for_key_item(
            leading_key_item,
            cmp.field.as_str(),
            &cmp.value,
            cmp.coercion.id,
            literal_compatible,
        ) else {
            continue;
        };

        // Expression-key components are length-prefixed in raw key framing.
        // A semantic `next_prefix` upper bound can exclude longer matching values,
        // so expression starts-with lowers to a safe lower-bounded envelope and
        // relies on residual filter evaluation for exact prefix semantics.
        let (lower, upper) = starts_with_component_bounds(
            &prefix,
            if leading_key_item.is_expression() {
                TextPrefixBoundMode::LowerOnly
            } else {
                TextPrefixBoundMode::Strict
            },
        )?;

        let score = access_candidate_score_from_index_contract(
            model,
            order,
            index.clone(),
            0,
            false,
            range_bound_count(&lower, &upper),
            grouped,
        );
        match best {
            None => best = Some((score, index.clone(), lower, upper)),
            Some((best_score, best_index, _, _))
                if access_candidate_score_outranks(score, best_score, false)
                    || (score == best_score && index.name() < best_index.name()) =>
            {
                best = Some((score, index.clone(), lower, upper));
            }
            _ => {}
        }
    }

    best.map(|(_, index, lower, upper)| {
        AccessPlan::index_range(SemanticIndexRangeSpec::from_access_contract(
            index,
            vec![0usize],
            Vec::new(),
            lower,
            upper,
        ))
    })
}

fn plan_ordered_compare(
    model: &EntityModel,
    candidate_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
    order: Option<&OrderSpec>,
    grouped: bool,
) -> Option<AccessPlan<Value>> {
    // Ordered bounds must reuse the same canonical literal-lowering authority
    // as Eq/In/prefix matching so expression-key comparisons stay aligned with
    // the stored normalized index value order.
    let literal_compatible = index_literal_matches_schema(schema, &cmp.field, &cmp.value);

    let mut best: Option<(
        AccessCandidateScore,
        SemanticIndexAccessContract,
        Bound<Value>,
        Bound<Value>,
    )> = None;
    for index in candidate_indexes {
        let Some(leading_key_item) = index.key_item_at(0) else {
            continue;
        };
        if index.key_arity() != 1 {
            continue;
        }

        let Some(bound_value) = eq_lookup_value_for_key_item(
            leading_key_item,
            cmp.field.as_str(),
            &cmp.value,
            cmp.coercion.id,
            literal_compatible,
        ) else {
            continue;
        };

        match leading_key_item {
            SemanticIndexKeyItemRef::Field(_) => {
                if cmp.coercion.id != CoercionId::Strict
                    || !matches!(
                        index.key_item_at(0),
                        Some(SemanticIndexKeyItemRef::Field(field))
                            if field == cmp.field.as_str()
                    )
                    || !field_key_contract_supports_operator(index, cmp.field.as_str(), cmp.op)
                {
                    continue;
                }
            }
            SemanticIndexKeyItemRef::Expression(_)
            | SemanticIndexKeyItemRef::AcceptedExpression(_) => {
                if cmp.coercion.id != CoercionId::TextCasefold {
                    continue;
                }
            }
        }

        let (lower, upper) = match cmp.op {
            CompareOp::Gt => (Bound::Excluded(bound_value), Bound::Unbounded),
            CompareOp::Gte => (Bound::Included(bound_value), Bound::Unbounded),
            CompareOp::Lt => (Bound::Unbounded, Bound::Excluded(bound_value)),
            CompareOp::Lte => (Bound::Unbounded, Bound::Included(bound_value)),
            _ => unreachable!("ordered compare helper must receive one of Gt/Gte/Lt/Lte"),
        };
        let score = access_candidate_score_from_index_contract(
            model,
            order,
            index.clone(),
            0,
            false,
            range_bound_count(&lower, &upper),
            grouped,
        );
        match best {
            None => best = Some((score, index.clone(), lower, upper)),
            Some((best_score, best_index, _, _))
                if access_candidate_score_outranks(score, best_score, false)
                    || (score == best_score && index.name() < best_index.name()) =>
            {
                best = Some((score, index.clone(), lower, upper));
            }
            _ => {}
        }
    }

    best.map(|(_, index, lower, upper)| {
        AccessPlan::index_range(SemanticIndexRangeSpec::from_access_contract(
            index,
            vec![0usize],
            Vec::new(),
            lower,
            upper,
        ))
    })
}

fn field_key_contract_supports_operator(
    index_contract: &SemanticIndexAccessContract,
    field: &str,
    op: CompareOp,
) -> bool {
    if index_contract.has_expression_key_items() {
        return false;
    }
    if !contract_contains_field_key(index_contract, field) {
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
            | CompareOp::StartsWith
    )
}

fn contract_contains_field_key(index_contract: &SemanticIndexAccessContract, field: &str) -> bool {
    match index_contract.key_items() {
        SemanticIndexKeyItemsRef::Fields(fields) => {
            fields.iter().any(|key_field| key_field == field)
        }
        SemanticIndexKeyItemsRef::Accepted(items) => items
            .iter()
            .any(|item| matches!(item.as_ref(), SemanticIndexKeyItemRef::Field(key_field) if key_field == field)),
        SemanticIndexKeyItemsRef::Static(crate::model::index::IndexKeyItemsRef::Fields(fields)) => {
            fields.contains(&field)
        }
        SemanticIndexKeyItemsRef::Static(crate::model::index::IndexKeyItemsRef::Items(items)) => items.iter().any(|item| {
            matches!(item, crate::model::index::IndexKeyItem::Field(key_field) if key_field == &field)
        }),
    }
}
