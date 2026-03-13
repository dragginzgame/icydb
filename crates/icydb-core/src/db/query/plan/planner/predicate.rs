//! Module: db::query::plan::planner::predicate
//! Responsibility: module-local ownership and contracts for db::query::plan::planner::predicate.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        access::AccessPlan,
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::plan::planner::{compare, index_literal_matches_schema, prefix, range},
        schema::SchemaInfo,
    },
    error::InternalError,
    model::entity::EntityModel,
    value::Value,
};

pub(super) fn plan_predicate(
    model: &EntityModel,
    schema: &SchemaInfo,
    predicate: &Predicate,
    query_predicate: &Predicate,
) -> Result<AccessPlan<Value>, InternalError> {
    let plan = match predicate {
        Predicate::True
        | Predicate::False
        | Predicate::Not(_)
        | Predicate::IsNotNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => AccessPlan::full_scan(),
        Predicate::IsNull { field } => {
            // Primary keys are always keyable and therefore never representable
            // as `Value::Null`; lower this impossible shape to an empty access
            // contract instead of scanning all rows.
            if field == model.primary_key.name
                && matches!(schema.field(field), Some(field_type) if field_type.is_keyable())
            {
                AccessPlan::by_keys(Vec::new())
            } else {
                AccessPlan::full_scan()
            }
        }
        Predicate::And(children) => {
            if let Some(range_spec) =
                range::index_range_from_and(model, schema, children, query_predicate)
            {
                return Ok(AccessPlan::index_range(range_spec));
            }

            let mut plans = children
                .iter()
                .map(|child| plan_predicate(model, schema, child, query_predicate))
                .collect::<Result<Vec<_>, _>>()?;

            // Composite index planning phase:
            // - Range candidate extraction is resolved before child recursion.
            // - If no range candidate exists, retain equality-prefix planning.
            if let Some(prefix) =
                prefix::index_prefix_from_and(model, schema, children, query_predicate)
            {
                plans.push(prefix);
            }

            AccessPlan::intersection(plans)
        }
        Predicate::Or(children) => AccessPlan::union(
            if let Some(rewritten) =
                plan_strict_same_field_eq_or(model, schema, children, query_predicate)
            {
                vec![rewritten]
            } else {
                children
                    .iter()
                    .map(|child| plan_predicate(model, schema, child, query_predicate))
                    .collect::<Result<Vec<_>, _>>()?
            },
        ),
        Predicate::Compare(cmp) => compare::plan_compare(model, schema, cmp, query_predicate),
    };

    Ok(plan)
}

// Fold strictly bounded OR-equality shapes (`a=v1 OR a=v2 ...`) into one
// IN planning path so access selection and explain metadata align.
// Access canonicalization owns IN-list set normalization semantics.
fn plan_strict_same_field_eq_or(
    model: &EntityModel,
    schema: &SchemaInfo,
    children: &[Predicate],
    query_predicate: &Predicate,
) -> Option<AccessPlan<Value>> {
    if children.len() < 2 {
        return None;
    }

    let mut field: Option<&str> = None;
    let mut values = Vec::with_capacity(children.len());
    for child in children {
        let Predicate::Compare(cmp) = child else {
            return None;
        };
        if cmp.coercion.id != CoercionId::Strict || cmp.op != CompareOp::Eq {
            return None;
        }
        if !index_literal_matches_schema(schema, &cmp.field, &cmp.value) {
            return None;
        }
        if let Some(current) = field {
            if current != cmp.field {
                return None;
            }
        } else {
            field = Some(cmp.field.as_str());
        }
        values.push(cmp.value.clone());
    }

    let field = field?;
    let in_compare = ComparePredicate::with_coercion(
        field,
        CompareOp::In,
        Value::List(values),
        CoercionId::Strict,
    );

    Some(compare::plan_compare(
        model,
        schema,
        &in_compare,
        query_predicate,
    ))
}
