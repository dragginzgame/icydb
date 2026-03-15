use crate::{
    db::executor::{
        pipeline::operators::post_access::{
            contracts::BudgetSafetyMetadata, coordinator::PostAccessPlan,
        },
        route::{derive_budget_safety_flags, stream_order_contract_safe},
    },
    traits::EntitySchema,
};

impl<K> PostAccessPlan<'_, K> {
    /// Build budget-safety metadata used by guarded execution scan budgeting.
    #[must_use]
    pub(in crate::db::executor::pipeline::operators::post_access) fn budget_safety_metadata<E>(
        &self,
    ) -> BudgetSafetyMetadata
    where
        E: EntitySchema<Key = K>,
    {
        let (has_residual_filter, access_order_satisfied_by_path, requires_post_access_sort) =
            derive_budget_safety_flags::<E, _>(self.contract.plan());

        BudgetSafetyMetadata {
            has_residual_filter,
            access_order_satisfied_by_path,
            requires_post_access_sort,
        }
    }

    // Shared streaming eligibility gate for execution paths that consume
    // the resolved ordered key stream directly without post-access filtering/sorting.
    #[must_use]
    pub(in crate::db::executor::pipeline::operators::post_access) fn is_stream_order_contract_safe<
        E,
    >(
        &self,
    ) -> bool
    where
        E: EntitySchema<Key = K>,
    {
        stream_order_contract_safe::<E, _>(self.contract.plan())
    }
}
