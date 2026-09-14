//! Module: query::plan::planner::compare
//! Responsibility: planner compare-predicate access-path planning and index-range lowering.
//! Does not own: runtime comparator enforcement or continuation resume execution details.
//! Boundary: derives compare-driven `AccessPlan` semantics from schema/predicate contracts.

#[cfg(test)]
mod admission_tests;
#[cfg(test)]
pub(super) mod prefix_tests;

use crate::{
    db::{
        access::{
            AccessPath, AccessPlan, SemanticIndexAccessContract, SemanticIndexKeyItemRef,
            SemanticIndexRangeSpec,
        },
        index::{TextPrefixBoundMode, admit_text_prefix_bounds, starts_with_component_bounds},
        predicate::{CoercionId, CompareOp, ComparePredicate},
        query::construction::ConstructionBudget,
        query::plan::{
            OrderSpec, field_key_contract_supports_operator,
            key_item_match::{
                copy_lookup_value_for_key_item, key_item_supports_lookup_value,
                key_item_supports_starts_with_value,
            },
            planner::{
                AccessCandidateScore, access_candidate_score_from_index_contract,
                access_candidate_score_outranks, index_literal_matches_schema,
                prefix::{index_multi_lookup_for_in, index_prefix_for_eq},
                range_bound_count,
            },
        },
        schema::{FieldType, SchemaInfo, literal_matches_type},
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use std::ops::Bound;

pub(super) fn plan_compare(
    candidate_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
    order: Option<&OrderSpec>,
    grouped: bool,
    budget: &dyn ConstructionBudget,
) -> Result<AccessPlan<Value>, InternalError> {
    // Exact primary-key predicate lowering is scalar-only. Composite primary
    // keys are addressed through full-key values at typed/structural
    // boundaries; partial component predicates must not masquerade as ByKey.
    let primary_key_name = schema.scalar_primary_key_name();
    if primary_key_exact_coercion_supports_access(cmp.coercion.id)
        && primary_key_name.is_some_and(|name| cmp.field == name)
        && let Some(field_type) = primary_key_name.and_then(|name| schema.field(name))
        && let Some(path) = plan_pk_compare(field_type, &cmp.value, cmp.op, budget)?
    {
        return Ok(path);
    }

    match cmp.op {
        CompareOp::Eq => {
            if !coercion_supports_index_lookup(cmp.coercion.id) {
                return Ok(AccessPlan::full_scan());
            }
            if let Some(paths) =
                index_prefix_for_eq(candidate_indexes, schema, cmp, order, grouped, budget)?
            {
                return Ok(paths);
            }
        }
        CompareOp::In => {
            if !coercion_supports_index_lookup(cmp.coercion.id) {
                return Ok(AccessPlan::full_scan());
            }
            if let Value::List(items) = &cmp.value {
                // Access canonicalization owns IN-list set normalization
                // (sorting/dedup and singleton collapse).
                // `IN ()` is a constant-empty predicate: no row can satisfy it.
                // Lower directly to an empty access shape instead of full-scan fallback.
                if items.is_empty() {
                    return Ok(AccessPlan::by_keys(Vec::new()));
                }
                if let Some(path) = index_multi_lookup_for_in(
                    candidate_indexes,
                    schema,
                    cmp,
                    items,
                    order,
                    grouped,
                    budget,
                )? {
                    return Ok(path);
                }
            }
        }
        CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
            if !coercion_supports_index_lookup(cmp.coercion.id) {
                return Ok(AccessPlan::full_scan());
            }
            let Some(field_type) = schema.field(&cmp.field) else {
                return Ok(AccessPlan::full_scan());
            };
            if !field_supports_ordered_compare(field_type, cmp.coercion.id) {
                return Ok(AccessPlan::full_scan());
            }
            if let Some(path) =
                plan_ordered_compare(candidate_indexes, schema, cmp, order, grouped, budget)?
            {
                return Ok(path);
            }
        }
        CompareOp::StartsWith => {
            if !coercion_supports_index_lookup(cmp.coercion.id) {
                return Ok(AccessPlan::full_scan());
            }

            // Keep the starts-with split explicit:
            // - raw field-key text prefixes now lower onto the same bounded
            //   semantic range contract as equivalent `>=`/`< next_prefix`
            //   forms
            // - expression-key lookups still keep their lower-bounded shape
            //   because the derived expression ordering does not yet expose one
            //   tighter planner-owned upper-bound contract
            if let Some(path) =
                plan_starts_with_compare(candidate_indexes, schema, cmp, order, grouped, budget)?
            {
                return Ok(path);
            }
        }
        _ => {
            // NOTE: Other non-equality comparisons do not currently map to key access paths.
        }
    }

    Ok(AccessPlan::full_scan())
}

// Planner compare access only supports exact schema semantics or case-folded
// text semantics. Other coercions still require residual filter evaluation.
const fn coercion_supports_index_lookup(coercion: CoercionId) -> bool {
    matches!(coercion, CoercionId::Strict | CoercionId::TextCasefold)
}

// Primary-key exact access may accept numeric widening only after the accepted
// schema literal gate proves the canonical value already matches the key type.
const fn primary_key_exact_coercion_supports_access(coercion: CoercionId) -> bool {
    matches!(coercion, CoercionId::Strict | CoercionId::NumericWiden)
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
    budget: &dyn ConstructionBudget,
) -> Result<Option<AccessPlan<Value>>, InternalError> {
    if !field_type.is_keyable() {
        return Ok(None);
    }

    let path = match op {
        CompareOp::Eq => {
            if !literal_matches_type(value, field_type) {
                return Ok(None);
            }

            AccessPath::ByKey(budget.copy_value(value)?)
        }
        CompareOp::In => {
            let Value::List(items) = value else {
                return Ok(None);
            };

            // Keep planner semantic-only: PK IN literal-set canonicalization is
            // performed by access-plan canonicalization.
            for item in items {
                if !literal_matches_type(item, field_type) {
                    return Ok(None);
                }
            }

            // Keep original order/duplicates for access canonicalization.
            // Admit each destination before copying; a failed copy publishes
            // no route and is never interpreted as an unsupported candidate.
            let mut keys = budget.vec_with_capacity(items.len())?;
            for item in items {
                keys.push(budget.copy_value(item)?);
            }
            AccessPath::ByKeys(keys)
        }
        _ => {
            // NOTE: Only Eq/In comparisons can be expressed as key access paths.
            return Ok(None);
        }
    };
    Ok(Some(AccessPlan::Path(budget.boxed(path)?)))
}

fn plan_starts_with_compare(
    candidate_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
    order: Option<&OrderSpec>,
    grouped: bool,
    budget: &dyn ConstructionBudget,
) -> Result<Option<AccessPlan<Value>>, InternalError> {
    // This helper owns the shared starts-with range lowering contract for both
    // raw field keys and the expression-key casefold path.
    if !schema.field(&cmp.field).is_some_and(FieldType::is_text) {
        return Ok(None);
    }
    let literal_compatible = index_literal_matches_schema(schema, &cmp.field, &cmp.value);
    budget.charge(
        Resource::PredicateExpressionSteps,
        candidate_indexes.len() as u64,
    )?;
    let mut candidates = candidate_indexes.iter().filter_map(|index| {
        let key = index.key_item_at(0)?;
        key_item_supports_starts_with_value(
            key,
            cmp.field.as_str(),
            &cmp.value,
            cmp.coercion.id,
            literal_compatible,
        )
        .then_some((index, key))
    });
    let Some(first) = candidates.next() else {
        return Ok(None);
    };

    // One predicate's coercion admits either raw field keys or LOWER keys,
    // never a mixture. Their bounds are identical across eligible indexes;
    // construct once, then rank using the actual (possibly unbounded) interval.
    let Some(Value::Text(prefix)) = copy_lookup_value_for_key_item(
        first.1,
        &cmp.field,
        &cmp.value,
        cmp.coercion.id,
        literal_compatible,
        budget,
    )?
    else {
        return Ok(None);
    };
    // Expression framing needs a lower-only envelope plus residual filtering:
    // a semantic successor could exclude longer matching expression values.
    let mode = if first.1.is_expression() {
        TextPrefixBoundMode::LowerOnly
    } else {
        TextPrefixBoundMode::Strict
    };
    admit_text_prefix_bounds(&prefix, mode, budget)?;
    let Some((lower, upper)) = starts_with_component_bounds(&prefix, mode) else {
        return Ok(None);
    };

    let mut best: Option<(AccessCandidateScore, &SemanticIndexAccessContract)> = None;
    for (index, _) in std::iter::once(first).chain(candidates) {
        let score = access_candidate_score_from_index_contract(
            schema,
            order,
            index,
            0,
            false,
            range_bound_count(&lower, &upper),
            grouped,
        );
        match best {
            None => best = Some((score, index)),
            Some((best_score, best_index))
                if access_candidate_score_outranks(score, best_score, false)
                    || (score == best_score && index.name() < best_index.name()) =>
            {
                best = Some((score, index));
            }
            _ => {}
        }
    }

    let Some((_, index)) = best else {
        return Ok(None);
    };
    let mut slots = budget.vec_with_capacity(1)?;
    slots.push(0usize);
    let spec = SemanticIndexRangeSpec::from_access_contract(
        index.clone(),
        slots,
        Vec::new(),
        lower,
        upper,
    );
    Ok(Some(AccessPlan::Path(
        budget.boxed(AccessPath::IndexRange { spec })?,
    )))
}

fn plan_ordered_compare(
    candidate_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
    order: Option<&OrderSpec>,
    grouped: bool,
    budget: &dyn ConstructionBudget,
) -> Result<Option<AccessPlan<Value>>, InternalError> {
    // Ordered bounds must reuse the same canonical literal-lowering authority
    // as Eq/In/prefix matching so expression-key comparisons stay aligned with
    // the stored normalized index value order.
    let literal_compatible = index_literal_matches_schema(schema, &cmp.field, &cmp.value);
    if !literal_compatible
        || !matches!(
            cmp.op,
            CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte
        )
    {
        return Ok(None);
    }

    // Every supported comparison has exactly one bound. Rank borrowed index
    // identities first, then admit and construct only the winning operand.
    budget.charge(
        Resource::PredicateExpressionSteps,
        candidate_indexes.len() as u64,
    )?;
    let mut best: Option<(
        AccessCandidateScore,
        &SemanticIndexAccessContract,
        SemanticIndexKeyItemRef<'_>,
    )> = None;
    for index in candidate_indexes {
        let Some(leading_key_item) = index.key_item_at(0) else {
            continue;
        };
        if !key_item_supports_lookup_value(
            leading_key_item,
            cmp.field.as_str(),
            &cmp.value,
            cmp.coercion.id,
            literal_compatible,
        ) {
            continue;
        }
        if !leading_key_item.is_expression()
            && !field_key_contract_supports_operator(index, cmp.field.as_str(), cmp.op)
        {
            continue;
        }
        let score =
            access_candidate_score_from_index_contract(schema, order, index, 0, false, 1, grouped);
        match best {
            None => best = Some((score, index, leading_key_item)),
            Some((best_score, best_index, _))
                if access_candidate_score_outranks(score, best_score, false)
                    || (score == best_score && index.name() < best_index.name()) =>
            {
                best = Some((score, index, leading_key_item));
            }
            _ => {}
        }
    }

    let Some((_, index, key)) = best else {
        return Ok(None);
    };
    let Some(value) =
        copy_lookup_value_for_key_item(key, &cmp.field, &cmp.value, cmp.coercion.id, true, budget)?
    else {
        return Ok(None);
    };
    let (lower, upper) = match cmp.op {
        CompareOp::Gt => (Bound::Excluded(value), Bound::Unbounded),
        CompareOp::Gte => (Bound::Included(value), Bound::Unbounded),
        CompareOp::Lt => (Bound::Unbounded, Bound::Excluded(value)),
        CompareOp::Lte => (Bound::Unbounded, Bound::Included(value)),
        _ => return Ok(None),
    };
    let mut slots = budget.vec_with_capacity(1)?;
    slots.push(0usize);
    let spec = SemanticIndexRangeSpec::from_access_contract(
        index.clone(),
        slots,
        Vec::new(),
        lower,
        upper,
    );
    Ok(Some(AccessPlan::Path(
        budget.boxed(AccessPath::IndexRange { spec })?,
    )))
}
