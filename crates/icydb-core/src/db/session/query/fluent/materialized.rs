//! Module: db::session::query::fluent::materialized
//! Responsibility: session adapters for materialized fluent row and ranking terminals.
//! Does not own: read admission, ranking semantics, or executor route choice.
//! Boundary: delegates materialized row/value/id terminal requests to the load executor.

use crate::{
    db::{
        DbSession, EntityResponse, PersistedRow, Query, QueryError,
        query::plan::FieldSlot,
        session::{AcceptedIdValuesOutput, AcceptedValuesOutput},
    },
    traits::CanisterKind,
};

impl<C: CanisterKind> DbSession<C> {
    // Execute the fluent `bytes()` terminal without leaking executor closure
    // assembly into query fluent code.
    pub(in crate::db) fn execute_fluent_bytes<E>(&self, query: &Query<E>) -> Result<u64, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_with_plan(query, |load, plan| load.bytes(plan))
    }

    // Execute the fluent `bytes_by(field)` terminal at the session boundary.
    pub(in crate::db) fn execute_fluent_bytes_by_slot<E>(
        &self,
        query: &Query<E>,
        target_slot: FieldSlot,
    ) -> Result<u64, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_with_plan(query, move |load, plan| {
            load.bytes_by_slot(plan, target_slot)
        })
    }

    // Execute the fluent `take(k)` terminal at the session boundary.
    pub(in crate::db) fn execute_fluent_take<E>(
        &self,
        query: &Query<E>,
        take_count: u32,
    ) -> Result<EntityResponse<E>, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_with_plan(query, move |load, plan| load.take(plan, take_count))
    }

    // Execute one row-returning fluent `top_k_by(field, k)` terminal at the
    // session boundary.
    pub(in crate::db) fn execute_fluent_top_k_rows_by_slot<E>(
        &self,
        query: &Query<E>,
        target_slot: FieldSlot,
        take_count: u32,
    ) -> Result<EntityResponse<E>, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_with_plan(query, move |load, plan| {
            load.top_k_by_slot(plan, target_slot, take_count)
        })
    }

    // Execute one row-returning fluent `bottom_k_by(field, k)` terminal at the
    // session boundary.
    pub(in crate::db) fn execute_fluent_bottom_k_rows_by_slot<E>(
        &self,
        query: &Query<E>,
        target_slot: FieldSlot,
        take_count: u32,
    ) -> Result<EntityResponse<E>, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_with_plan(query, move |load, plan| {
            load.bottom_k_by_slot(plan, target_slot, take_count)
        })
    }

    // Execute one value-returning fluent `top_k_by_values(field, k)` terminal
    // at the session boundary.
    pub(in crate::db) fn execute_fluent_top_k_values_by_slot<E>(
        &self,
        query: &Query<E>,
        target_slot: FieldSlot,
        take_count: u32,
    ) -> Result<AcceptedValuesOutput, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_with_plan_and_catalog(query, move |load, plan| {
            load.top_k_by_values_slot(plan, target_slot, take_count)
        })
    }

    // Execute one value-returning fluent `bottom_k_by_values(field, k)` terminal
    // at the session boundary.
    pub(in crate::db) fn execute_fluent_bottom_k_values_by_slot<E>(
        &self,
        query: &Query<E>,
        target_slot: FieldSlot,
        take_count: u32,
    ) -> Result<AcceptedValuesOutput, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_with_plan_and_catalog(query, move |load, plan| {
            load.bottom_k_by_values_slot(plan, target_slot, take_count)
        })
    }

    // Execute one id/value-returning fluent `top_k_by_with_ids(field, k)`
    // terminal at the session boundary.
    pub(in crate::db) fn execute_fluent_top_k_values_with_ids_by_slot<E>(
        &self,
        query: &Query<E>,
        target_slot: FieldSlot,
        take_count: u32,
    ) -> Result<AcceptedIdValuesOutput<E>, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_with_plan_and_catalog(query, move |load, plan| {
            load.top_k_by_with_ids_slot(plan, target_slot, take_count)
        })
    }

    // Execute one id/value-returning fluent `bottom_k_by_with_ids(field, k)`
    // terminal at the session boundary.
    pub(in crate::db) fn execute_fluent_bottom_k_values_with_ids_by_slot<E>(
        &self,
        query: &Query<E>,
        target_slot: FieldSlot,
        take_count: u32,
    ) -> Result<AcceptedIdValuesOutput<E>, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_with_plan_and_catalog(query, move |load, plan| {
            load.bottom_k_by_with_ids_slot(plan, target_slot, take_count)
        })
    }
}
