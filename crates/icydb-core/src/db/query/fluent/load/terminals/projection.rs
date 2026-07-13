//! Module: query::fluent::load::terminals::projection
//! Responsibility: fluent projected-value and ranked-row terminal public methods.
//! Does not own: aggregate semantics, materialization terminals, or executor routing.
//! Boundary: delegates slot/admission handoffs to `support` and value shaping to `output`.

use crate::{
    db::{
        PersistedRow,
        query::{
            builder::{
                DistinctValuesBySlotTerminal, FirstValueBySlotTerminal, LastValueBySlotTerminal,
                ValueProjectionExpr, ValuesBySlotTerminal, ValuesBySlotWithIdsTerminal,
            },
            explain::ExplainExecutionNodeDescriptor,
            fluent::load::FluentLoadQuery,
            intent::QueryError,
            plan::AggregateKind,
        },
        response::EntityResponse,
    },
    traits::EntityValue,
    types::Id,
    value::OutputValue,
};

use super::output::{output_optional, output_values, output_values_with_ids};

impl<E> FluentLoadQuery<'_, E>
where
    E: PersistedRow,
{
    /// Execute and return projected field values for the effective result window.
    pub fn values_by(&self, field: impl AsRef<str>) -> Result<Vec<OutputValue>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, ValuesBySlotTerminal::new)
            .and_then(output_values)
    }

    /// Execute and return projected values for one shared bounded projection
    /// over the effective response window.
    pub fn project_values<P>(&self, projection: &P) -> Result<Vec<OutputValue>, QueryError>
    where
        E: EntityValue,
        P: ValueProjectionExpr,
    {
        self.with_admitted_non_paged_slot(projection.field(), |session, query, target_slot| {
            session.execute_fluent_project_values_by_slot(
                query,
                target_slot,
                projection.projection_plan().into_expr(),
            )
        })
        .and_then(output_values)
    }

    /// Explain `project_values(projection)` routing without executing it.
    pub fn explain_project_values<P>(
        &self,
        projection: &P,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
        P: ValueProjectionExpr,
    {
        self.explain_slot_terminal(projection.field(), ValuesBySlotTerminal::new)
    }

    /// Explain `values_by(field)` routing without executing the terminal.
    pub fn explain_values_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.explain_slot_terminal(field, ValuesBySlotTerminal::new)
    }

    /// Execute and return the first `k` rows from the effective response window.
    pub fn take(&self, take_count: u32) -> Result<EntityResponse<E>, QueryError>
    where
        E: EntityValue,
    {
        self.with_admitted_non_paged(|session, query| {
            session.execute_fluent_take(query, take_count)
        })
    }

    /// Execute and return the top `k` rows by `field` under deterministic
    /// ordering `(field desc, primary_key asc)` over the effective response
    /// window.
    ///
    /// This terminal applies its own ordering and does not preserve query
    /// `order_term(...)` row order in the returned rows. For `k = 1`, this
    /// matches `max_by(field)` selection semantics.
    pub fn top_k_by(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<EntityResponse<E>, QueryError>
    where
        E: EntityValue,
    {
        self.with_admitted_non_paged_slot(field, |session, query, target_slot| {
            session.execute_fluent_top_k_rows_by_slot(query, target_slot, take_count)
        })
    }

    /// Execute and return the bottom `k` rows by `field` under deterministic
    /// ordering `(field asc, primary_key asc)` over the effective response
    /// window.
    ///
    /// This terminal applies its own ordering and does not preserve query
    /// `order_term(...)` row order in the returned rows. For `k = 1`, this
    /// matches `min_by(field)` selection semantics.
    pub fn bottom_k_by(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<EntityResponse<E>, QueryError>
    where
        E: EntityValue,
    {
        self.with_admitted_non_paged_slot(field, |session, query, target_slot| {
            session.execute_fluent_bottom_k_rows_by_slot(query, target_slot, take_count)
        })
    }

    /// Execute and return projected values for the top `k` rows by `field`
    /// under deterministic ordering `(field desc, primary_key asc)` over the
    /// effective response window.
    ///
    /// Ranking is applied before projection and does not preserve query
    /// `order_term(...)` row order in the returned values. For `k = 1`, this
    /// matches `max_by(field)` projected to one value.
    pub fn top_k_by_values(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<Vec<OutputValue>, QueryError>
    where
        E: EntityValue,
    {
        self.with_admitted_non_paged_slot(field, |session, query, target_slot| {
            session.execute_fluent_top_k_values_by_slot(query, target_slot, take_count)
        })
        .and_then(output_values)
    }

    /// Execute and return projected values for the bottom `k` rows by `field`
    /// under deterministic ordering `(field asc, primary_key asc)` over the
    /// effective response window.
    ///
    /// Ranking is applied before projection and does not preserve query
    /// `order_term(...)` row order in the returned values. For `k = 1`, this
    /// matches `min_by(field)` projected to one value.
    pub fn bottom_k_by_values(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<Vec<OutputValue>, QueryError>
    where
        E: EntityValue,
    {
        self.with_admitted_non_paged_slot(field, |session, query, target_slot| {
            session.execute_fluent_bottom_k_values_by_slot(query, target_slot, take_count)
        })
        .and_then(output_values)
    }

    /// Execute and return projected id/value pairs for the top `k` rows by
    /// `field` under deterministic ordering `(field desc, primary_key asc)`
    /// over the effective response window.
    ///
    /// Ranking is applied before projection and does not preserve query
    /// `order_term(...)` row order in the returned values. For `k = 1`, this
    /// matches `max_by(field)` projected to one `(id, value)` pair.
    pub fn top_k_by_with_ids(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, OutputValue)>, QueryError>
    where
        E: EntityValue,
    {
        self.with_admitted_non_paged_slot(field, |session, query, target_slot| {
            session.execute_fluent_top_k_values_with_ids_by_slot(query, target_slot, take_count)
        })
        .and_then(output_values_with_ids::<E>)
    }

    /// Execute and return projected id/value pairs for the bottom `k` rows by
    /// `field` under deterministic ordering `(field asc, primary_key asc)`
    /// over the effective response window.
    ///
    /// Ranking is applied before projection and does not preserve query
    /// `order_term(...)` row order in the returned values. For `k = 1`, this
    /// matches `min_by(field)` projected to one `(id, value)` pair.
    pub fn bottom_k_by_with_ids(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, OutputValue)>, QueryError>
    where
        E: EntityValue,
    {
        self.with_admitted_non_paged_slot(field, |session, query, target_slot| {
            session.execute_fluent_bottom_k_values_with_ids_by_slot(query, target_slot, take_count)
        })
        .and_then(output_values_with_ids::<E>)
    }

    /// Execute and return distinct projected field values for the effective
    /// result window, preserving first-observed value order.
    pub fn distinct_values_by(&self, field: impl AsRef<str>) -> Result<Vec<OutputValue>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, DistinctValuesBySlotTerminal::new)
            .and_then(output_values)
    }

    /// Explain `distinct_values_by(field)` routing without executing the terminal.
    pub fn explain_distinct_values_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.explain_slot_terminal(field, DistinctValuesBySlotTerminal::new)
    }

    /// Execute and return projected field values paired with row ids for the
    /// effective result window.
    pub fn values_by_with_ids(
        &self,
        field: impl AsRef<str>,
    ) -> Result<Vec<(Id<E>, OutputValue)>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, ValuesBySlotWithIdsTerminal::new)
            .and_then(output_values_with_ids::<E>)
    }

    /// Execute and return projected id/value pairs for one shared bounded
    /// projection over the effective response window.
    pub fn project_values_with_ids<P>(
        &self,
        projection: &P,
    ) -> Result<Vec<(Id<E>, OutputValue)>, QueryError>
    where
        E: EntityValue,
        P: ValueProjectionExpr,
    {
        self.with_admitted_non_paged_slot(projection.field(), |session, query, target_slot| {
            session.execute_fluent_project_values_with_ids_by_slot(
                query,
                target_slot,
                projection.projection_plan().into_expr(),
            )
        })
        .and_then(output_values_with_ids::<E>)
    }

    /// Explain `values_by_with_ids(field)` routing without executing the terminal.
    pub fn explain_values_by_with_ids(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.explain_slot_terminal(field, ValuesBySlotWithIdsTerminal::new)
    }

    /// Execute and return the first projected field value in effective response
    /// order, if any.
    pub fn first_value_by(&self, field: impl AsRef<str>) -> Result<Option<OutputValue>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, FirstValueBySlotTerminal::new)
            .and_then(output_optional)
    }

    /// Execute and return the first projected value for one shared bounded
    /// projection in effective response order, if any.
    pub fn project_first_value<P>(&self, projection: &P) -> Result<Option<OutputValue>, QueryError>
    where
        E: EntityValue,
        P: ValueProjectionExpr,
    {
        self.with_admitted_non_paged_slot(projection.field(), |session, query, target_slot| {
            session.execute_fluent_project_terminal_value_by_slot(
                query,
                target_slot,
                AggregateKind::First,
                projection.projection_plan().into_expr(),
            )
        })
        .and_then(output_optional)
    }

    /// Explain `first_value_by(field)` routing without executing the terminal.
    pub fn explain_first_value_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.explain_slot_terminal(field, FirstValueBySlotTerminal::new)
    }

    /// Execute and return the last projected field value in effective response
    /// order, if any.
    pub fn last_value_by(&self, field: impl AsRef<str>) -> Result<Option<OutputValue>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, LastValueBySlotTerminal::new)
            .and_then(output_optional)
    }

    /// Execute and return the last projected value for one shared bounded
    /// projection in effective response order, if any.
    pub fn project_last_value<P>(&self, projection: &P) -> Result<Option<OutputValue>, QueryError>
    where
        E: EntityValue,
        P: ValueProjectionExpr,
    {
        self.with_admitted_non_paged_slot(projection.field(), |session, query, target_slot| {
            session.execute_fluent_project_terminal_value_by_slot(
                query,
                target_slot,
                AggregateKind::Last,
                projection.projection_plan().into_expr(),
            )
        })
        .and_then(output_optional)
    }

    /// Explain `last_value_by(field)` routing without executing the terminal.
    pub fn explain_last_value_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.explain_slot_terminal(field, LastValueBySlotTerminal::new)
    }
}
