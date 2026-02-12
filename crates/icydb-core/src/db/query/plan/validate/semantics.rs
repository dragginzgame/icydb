use crate::db::query::plan::{LogicalPlan, validate::PlanError};

/// Validate plan-level invariants not covered by schema checks.
pub fn validate_plan_semantics<K>(plan: &LogicalPlan<K>) -> Result<(), PlanError> {
    if let Some(order) = &plan.order
        && order.fields.is_empty()
    {
        return Err(PlanError::EmptyOrderSpec);
    }

    if plan.mode.is_delete() {
        if plan.page.is_some() {
            return Err(PlanError::DeletePlanWithPagination);
        }

        if plan.delete_limit.is_some()
            && plan
                .order
                .as_ref()
                .is_none_or(|order| order.fields.is_empty())
        {
            return Err(PlanError::DeleteLimitRequiresOrder);
        }
    }

    if plan.mode.is_load() {
        if plan.delete_limit.is_some() {
            return Err(PlanError::LoadPlanWithDeleteLimit);
        }

        if plan.page.is_some()
            && plan
                .order
                .as_ref()
                .is_none_or(|order| order.fields.is_empty())
        {
            return Err(PlanError::UnorderedPagination);
        }
    }

    Ok(())
}
