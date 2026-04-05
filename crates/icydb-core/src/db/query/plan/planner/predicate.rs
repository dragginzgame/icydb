//! Module: db::query::plan::planner::predicate
//! Responsibility: module-local ownership and contracts for db::query::plan::planner::predicate.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        access::AccessPlan,
        predicate::Predicate,
        query::plan::{
            OrderSpec,
            key_item_match::{eq_lookup_value_for_key_item, index_key_item_at},
            planner::{compare, index_literal_matches_schema, prefix, range},
        },
        schema::SchemaInfo,
    },
    error::InternalError,
    model::{entity::EntityModel, index::IndexModel},
    value::Value,
};

pub(super) fn plan_predicate(
    model: &EntityModel,
    schema: &SchemaInfo,
    predicate: &Predicate,
    query_predicate: &Predicate,
    order: Option<&OrderSpec>,
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

            let prefix_access =
                prefix::index_prefix_from_and(model, schema, children, query_predicate, order);
            let mut plans = children
                .iter()
                .filter(|child| {
                    !eq_child_is_redundant_under_prefix(schema, prefix_access.as_ref(), child)
                })
                .map(|child| plan_predicate(model, schema, child, query_predicate, order))
                .collect::<Result<Vec<_>, _>>()?;

            // Composite index planning phase:
            // - Range candidate extraction is resolved before child recursion.
            // - If no range candidate exists, retain equality-prefix planning.
            if let Some(prefix) = prefix_access {
                plans.push(prefix);
            }

            AccessPlan::intersection(plans)
        }
        Predicate::Or(children) => AccessPlan::union(
            children
                .iter()
                .map(|child| plan_predicate(model, schema, child, query_predicate, order))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Predicate::Compare(cmp) => {
            compare::plan_compare(model, schema, cmp, query_predicate, order)
        }
    };

    Ok(plan)
}

// Composite prefix planning already picked one deterministic leading-slot
// route, so redundant equality children on those same guaranteed slots do not
// need to contribute weaker nested access shapes.
fn eq_child_is_redundant_under_prefix(
    schema: &SchemaInfo,
    prefix_access: Option<&AccessPlan<Value>>,
    child: &Predicate,
) -> bool {
    let Some(AccessPlan::Path(path)) = prefix_access else {
        return false;
    };
    let Some((index, values)) = path.as_ref().as_index_prefix() else {
        return false;
    };
    let Predicate::Compare(cmp) = child else {
        return false;
    };
    if cmp.op != crate::db::predicate::CompareOp::Eq {
        return false;
    }

    index_prefix_guarantees_eq_compare(schema, index, values, cmp)
}

// Prefix guarantees are checked against canonical key-item lowering so mixed
// field/expression prefixes can suppress only the exact equality clauses they
// already prove.
fn index_prefix_guarantees_eq_compare(
    schema: &SchemaInfo,
    index: &IndexModel,
    prefix_values: &[Value],
    cmp: &crate::db::predicate::ComparePredicate,
) -> bool {
    let literal_compatible = index_literal_matches_schema(schema, cmp.field.as_str(), cmp.value());

    prefix_values
        .iter()
        .enumerate()
        .any(|(slot, expected_value)| {
            let Some(key_item) = index_key_item_at(index, slot) else {
                return false;
            };
            let Some(candidate) = eq_lookup_value_for_key_item(
                key_item,
                cmp.field.as_str(),
                cmp.value(),
                cmp.coercion.id,
                literal_compatible,
            ) else {
                return false;
            };

            candidate == *expected_value
        })
}
