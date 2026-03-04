use crate::{
    db::{
        access::AccessPlan,
        predicate::{Predicate, SchemaInfo},
        query::plan::planner::{compare, prefix, range},
    },
    error::InternalError,
    model::entity::EntityModel,
    value::Value,
};

pub(super) fn plan_predicate(
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
                return Ok(AccessPlan::index_range(range_spec));
            }

            let mut plans = children
                .iter()
                .map(|child| plan_predicate(model, schema, child))
                .collect::<Result<Vec<_>, _>>()?;

            // Composite index planning phase:
            // - Range candidate extraction is resolved before child recursion.
            // - If no range candidate exists, retain equality-prefix planning.
            if let Some(prefix) = prefix::index_prefix_from_and(model, schema, children) {
                plans.push(prefix);
            }

            AccessPlan::intersection(plans)
        }
        Predicate::Or(children) => AccessPlan::union(
            children
                .iter()
                .map(|child| plan_predicate(model, schema, child))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Predicate::Compare(cmp) => compare::plan_compare(model, schema, cmp),
    };

    Ok(plan)
}
