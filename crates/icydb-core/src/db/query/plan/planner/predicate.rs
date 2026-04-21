//! Module: db::query::plan::planner::predicate
//! Builds predicate-driven access plans from canonical predicate trees and
//! visible index metadata.

use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        predicate::Predicate,
        query::plan::{
            OrderSpec,
            key_item_match::{eq_lookup_value_for_key_item, index_key_item_at},
            planner::{
                compare, index_literal_matches_schema, index_predicate_guarantees_compare, prefix,
                range,
            },
        },
        schema::SchemaInfo,
    },
    error::InternalError,
    model::{entity::EntityModel, index::IndexModel},
    value::Value,
};

pub(super) fn plan_predicate(
    model: &EntityModel,
    candidate_indexes: &[&'static IndexModel],
    schema: &SchemaInfo,
    predicate: &Predicate,
    order: Option<&OrderSpec>,
    grouped: bool,
) -> Result<AccessPlan<Value>, InternalError> {
    let plan = match predicate {
        Predicate::True
        | Predicate::False
        | Predicate::Not(_)
        | Predicate::CompareFields(_)
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
            let primary_key_range_access =
                range::primary_key_range_from_and(model, schema, children);
            if let Some(range_spec) = range::index_range_from_and(
                model,
                candidate_indexes,
                schema,
                children,
                order,
                grouped,
            ) {
                return Ok(AccessPlan::index_range(range_spec));
            }

            let prefix_access = prefix::index_prefix_from_and(
                model,
                candidate_indexes,
                schema,
                children,
                order,
                grouped,
            );
            let selected_index_access = prefix_access.as_ref();
            let mut plans = children
                .iter()
                .filter(|child| {
                    !child_is_redundant_under_selected_index_access(
                        schema,
                        selected_index_access,
                        child,
                    )
                })
                .map(|child| {
                    plan_predicate(model, candidate_indexes, schema, child, order, grouped)
                })
                .collect::<Result<Vec<_>, _>>()?;

            // Composite index planning phase:
            // - Range candidate extraction is resolved before child recursion.
            // - If no range candidate exists, retain equality-prefix planning.
            if let Some(prefix) = prefix_access {
                plans.push(prefix);
            }
            if let Some(primary_key_range) = primary_key_range_access {
                plans.push(primary_key_range);
            }

            AccessPlan::intersection(plans)
        }
        Predicate::Or(children) => AccessPlan::union(
            children
                .iter()
                .map(|child| {
                    plan_predicate(model, candidate_indexes, schema, child, order, grouped)
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Predicate::Compare(cmp) => {
            compare::plan_compare(model, candidate_indexes, schema, cmp, order, grouped)
        }
    };

    Ok(plan)
}

// Composite filtered/prefix planning can already guarantee some child compare
// clauses through either fixed equality prefix slots or the filtered guard on
// the chosen index. Those clauses should not contribute weaker nested access
// shapes once the selected path already proves them.
fn child_is_redundant_under_selected_index_access(
    schema: &SchemaInfo,
    selected_access: Option<&AccessPlan<Value>>,
    child: &Predicate,
) -> bool {
    let Some(AccessPlan::Path(path)) = selected_access else {
        return false;
    };
    let Predicate::Compare(cmp) = child else {
        return false;
    };

    if cmp.op == crate::db::predicate::CompareOp::Eq
        && selected_index_prefix_guarantees_eq_compare(schema, path.as_ref(), cmp)
    {
        return true;
    }

    path.as_ref()
        .selected_index_model()
        .is_some_and(|index| index_predicate_guarantees_compare(index, cmp))
}

// Selected index prefix and selected index range both carry an equality prefix
// that can already prove one compare predicate. Project that shared contract
// before checking whether the chosen access path makes the child redundant.
fn selected_index_prefix_guarantees_eq_compare(
    schema: &SchemaInfo,
    selected_path: &AccessPath<Value>,
    cmp: &crate::db::predicate::ComparePredicate,
) -> bool {
    let selected_prefix = selected_path.as_index_prefix().or_else(|| {
        selected_path
            .as_index_range()
            .map(|(index, values, _, _)| (index, values))
    });
    let Some((index, prefix_values)) = selected_prefix else {
        return false;
    };

    index_prefix_guarantees_eq_compare(schema, index, prefix_values, cmp)
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
