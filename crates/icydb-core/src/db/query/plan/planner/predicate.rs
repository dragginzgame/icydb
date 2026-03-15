//! Module: db::query::plan::planner::predicate
//! Responsibility: module-local ownership and contracts for db::query::plan::planner::predicate.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        access::AccessPlan,
        predicate::Predicate,
        query::plan::planner::{compare, prefix, range},
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
            children
                .iter()
                .map(|child| plan_predicate(model, schema, child, query_predicate))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Predicate::Compare(cmp) => compare::plan_compare(model, schema, cmp, query_predicate),
    };

    Ok(plan)
}
