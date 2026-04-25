//! Module: query::fluent::load::terminals
//! Responsibility: fluent load terminal APIs and terminal-plan explanation entrypoints.
//! Does not own: planner semantic validation or executor runtime routing decisions.
//! Boundary: delegates to session planning/execution and returns typed query results.

use crate::{
    db::{
        DbSession, PersistedRow, Query,
        query::{
            api::ResponseCardinalityExt,
            builder::{
                PreparedFluentAggregateExplainStrategy, PreparedFluentExistingRowsTerminalStrategy,
                PreparedFluentNumericFieldStrategy, PreparedFluentOrderSensitiveTerminalStrategy,
                PreparedFluentProjectionStrategy, PreparedFluentScalarTerminalStrategy,
                ValueProjectionExpr,
            },
            explain::{ExplainAggregateTerminalPlan, ExplainExecutionNodeDescriptor},
            fluent::load::{
                FluentLoadQuery, FluentProjectionTerminalOutput, FluentScalarTerminalOutput,
                LoadQueryResult,
            },
            intent::QueryError,
            plan::AggregateKind,
        },
        response::EntityResponse,
    },
    error::InternalError,
    traits::EntityValue,
    types::{Decimal, Id},
    value::{OutputValue, Value},
};

type MinMaxByIds<E> = Option<(Id<E>, Id<E>)>;

// Convert one runtime projection value into the public output boundary type.
fn output(value: Value) -> OutputValue {
    OutputValue::from(value)
}

// Convert one ordered runtime projection vector into the public output form.
fn output_values(values: Vec<Value>) -> Vec<OutputValue> {
    values.into_iter().map(output).collect()
}

// Convert one ordered runtime `(id, value)` projection vector into the public output form.
fn output_values_with_ids<E: PersistedRow>(
    values: Vec<(Id<E>, Value)>,
) -> Vec<(Id<E>, OutputValue)> {
    values
        .into_iter()
        .map(|(id, value)| (id, output(value)))
        .collect()
}

impl<E> FluentLoadQuery<'_, E>
where
    E: PersistedRow,
{
    // ------------------------------------------------------------------
    // Execution (single semantic boundary)
    // ------------------------------------------------------------------

    /// Execute this query using the session's policy settings.
    pub fn execute(&self) -> Result<LoadQueryResult<E>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;
        self.session.execute_query_result(self.query())
    }

    // Run one read-only query/session projection through the canonical
    // non-paged fluent policy gate so explain-style helpers do not each
    // repeat the same readiness check and session handoff shell.
    fn map_non_paged_query_output<T>(
        &self,
        map: impl FnOnce(&DbSession<E::Canister>, &Query<E>) -> Result<T, QueryError>,
    ) -> Result<T, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;
        map(self.session, self.query())
    }

    // Run one explain-visible aggregate terminal through the canonical
    // non-paged fluent policy gate using the prepared aggregate strategy as
    // the single explain projection source.
    fn explain_prepared_aggregate_non_paged_terminal<S>(
        &self,
        strategy: &S,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
        S: PreparedFluentAggregateExplainStrategy,
    {
        self.map_non_paged_query_output(|session, query| {
            session.explain_query_prepared_aggregate_terminal_with_visible_indexes(query, strategy)
        })
    }

    // Run one prepared projection/distinct explain terminal through the
    // canonical non-paged fluent policy gate using the prepared projection
    // strategy as the single explain projection source.
    fn explain_prepared_projection_non_paged_terminal(
        &self,
        strategy: &PreparedFluentProjectionStrategy,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.map_non_paged_query_output(|session, query| {
            session.explain_query_prepared_projection_terminal_with_visible_indexes(query, strategy)
        })
    }

    // Resolve the structural execution descriptor for this fluent load query
    // through the session-owned visible-index explain path once.
    fn explain_execution_descriptor(&self) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.map_non_paged_query_output(DbSession::explain_query_execution_with_visible_indexes)
    }

    // Render one descriptor-derived execution surface so text/json explain
    // terminals do not each forward the same session explain call ad hoc.
    fn render_execution_descriptor(
        &self,
        render: impl FnOnce(ExplainExecutionNodeDescriptor) -> String,
    ) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        let descriptor = self.explain_execution_descriptor()?;

        Ok(render(descriptor))
    }

    // Execute one prepared existing-rows terminal and decode the typed output
    // through one shared mismatch/error-mapping lane.
    fn map_prepared_existing_rows_terminal_output<T>(
        &self,
        strategy: PreparedFluentExistingRowsTerminalStrategy,
        map: impl FnOnce(FluentScalarTerminalOutput<E>) -> Result<T, InternalError>,
    ) -> Result<T, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;
        let output = self
            .session
            .execute_fluent_existing_rows_terminal(self.query(), strategy)?;

        map(output).map_err(QueryError::execute)
    }

    // Execute one prepared fluent numeric-field terminal through the canonical
    // non-paged fluent policy gate using the prepared numeric strategy as the
    // single runtime source.
    fn execute_prepared_numeric_field_terminal(
        &self,
        strategy: PreparedFluentNumericFieldStrategy,
    ) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_fluent_numeric_field_terminal(self.query(), strategy)
    }

    // Execute one prepared order-sensitive terminal and decode the typed
    // output through one shared mismatch/error-mapping lane.
    fn map_prepared_order_sensitive_terminal_output<T>(
        &self,
        strategy: PreparedFluentOrderSensitiveTerminalStrategy,
        map: impl FnOnce(FluentScalarTerminalOutput<E>) -> Result<T, InternalError>,
    ) -> Result<T, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;
        let output = self
            .session
            .execute_fluent_order_sensitive_terminal(self.query(), strategy)?;

        map(output).map_err(QueryError::execute)
    }

    // Execute one prepared fluent projection/distinct terminal through the
    // canonical non-paged fluent policy gate using the prepared projection
    // strategy as the single runtime source.
    fn execute_prepared_projection_terminal_output(
        &self,
        strategy: PreparedFluentProjectionStrategy,
    ) -> Result<FluentProjectionTerminalOutput<E>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_fluent_projection_terminal(self.query(), strategy)
    }

    // Execute one prepared projection terminal and decode the typed output
    // through one shared mismatch/error-mapping lane.
    fn map_prepared_projection_terminal_output<T>(
        &self,
        strategy: PreparedFluentProjectionStrategy,
        map: impl FnOnce(FluentProjectionTerminalOutput<E>) -> Result<T, InternalError>,
    ) -> Result<T, QueryError>
    where
        E: EntityValue,
    {
        let output = self.execute_prepared_projection_terminal_output(strategy)?;

        map(output).map_err(QueryError::execute)
    }

    // Execute one prepared scalar terminal and decode the typed output through
    // one shared mismatch/error-mapping lane.
    fn map_prepared_scalar_terminal_output<T>(
        &self,
        strategy: PreparedFluentScalarTerminalStrategy,
        map: impl FnOnce(FluentScalarTerminalOutput<E>) -> Result<T, InternalError>,
    ) -> Result<T, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;
        let output = self
            .session
            .execute_fluent_scalar_terminal(self.query(), strategy)?;

        map(output).map_err(QueryError::execute)
    }

    // Apply one shared bounded value projection to iterator-like terminal
    // output while preserving source order and cardinality.
    fn project_terminal_items<P, T, U>(
        projection: &P,
        values: impl IntoIterator<Item = T>,
        mut map: impl FnMut(&P, T) -> Result<U, QueryError>,
    ) -> Result<Vec<U>, QueryError>
    where
        P: ValueProjectionExpr,
    {
        values
            .into_iter()
            .map(|value| map(projection, value))
            .collect()
    }

    // ------------------------------------------------------------------
    // Execution terminals — semantic only
    // ------------------------------------------------------------------

    /// Execute and return whether the result set is empty.
    pub fn is_empty(&self) -> Result<bool, QueryError>
    where
        E: EntityValue,
    {
        self.not_exists()
    }

    /// Execute and return whether no matching row exists.
    pub fn not_exists(&self) -> Result<bool, QueryError>
    where
        E: EntityValue,
    {
        Ok(!self.exists()?)
    }

    /// Execute and return whether at least one matching row exists.
    pub fn exists(&self) -> Result<bool, QueryError>
    where
        E: EntityValue,
    {
        self.map_prepared_existing_rows_terminal_output(
            PreparedFluentExistingRowsTerminalStrategy::exists_rows(),
            FluentScalarTerminalOutput::into_exists,
        )
    }

    /// Explain scalar `exists()` routing without executing the terminal.
    pub fn explain_exists(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_prepared_aggregate_non_paged_terminal(
            &PreparedFluentExistingRowsTerminalStrategy::exists_rows(),
        )
    }

    /// Explain scalar `not_exists()` routing without executing the terminal.
    ///
    /// This remains an `exists()` execution plan with negated boolean semantics.
    pub fn explain_not_exists(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_exists()
    }

    /// Explain scalar load execution shape without executing the query.
    pub fn explain_execution(&self) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.explain_execution_descriptor()
    }

    /// Explain scalar load execution shape as deterministic text.
    pub fn explain_execution_text(&self) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        self.render_execution_descriptor(|descriptor| descriptor.render_text_tree())
    }

    /// Explain scalar load execution shape as canonical JSON.
    pub fn explain_execution_json(&self) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        self.render_execution_descriptor(|descriptor| descriptor.render_json_canonical())
    }

    /// Explain scalar load execution shape as verbose text with diagnostics.
    pub fn explain_execution_verbose(&self) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        self.map_non_paged_query_output(
            DbSession::explain_query_execution_verbose_with_visible_indexes,
        )
    }

    /// Execute and return the number of matching rows.
    pub fn count(&self) -> Result<u32, QueryError>
    where
        E: EntityValue,
    {
        self.map_prepared_existing_rows_terminal_output(
            PreparedFluentExistingRowsTerminalStrategy::count_rows(),
            FluentScalarTerminalOutput::into_count,
        )
    }

    /// Execute and return the total persisted payload bytes for the effective
    /// result window.
    pub fn bytes(&self) -> Result<u64, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session.execute_fluent_bytes(self.query())
    }

    /// Execute and return the total serialized bytes for `field` over the
    /// effective result window.
    pub fn bytes_by(&self, field: impl AsRef<str>) -> Result<u64, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.session
                .execute_fluent_bytes_by_slot(self.query(), target_slot)
        })
    }

    /// Explain `bytes_by(field)` routing without executing the terminal.
    pub fn explain_bytes_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.session
                .explain_query_bytes_by_with_visible_indexes(self.query(), target_slot.field())
        })
    }

    /// Execute and return the smallest matching identifier, if any.
    pub fn min(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.map_prepared_scalar_terminal_output(
            PreparedFluentScalarTerminalStrategy::id_terminal(AggregateKind::Min),
            FluentScalarTerminalOutput::into_id,
        )
    }

    /// Explain scalar `min()` routing without executing the terminal.
    pub fn explain_min(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_prepared_aggregate_non_paged_terminal(
            &PreparedFluentScalarTerminalStrategy::id_terminal(AggregateKind::Min),
        )
    }

    /// Execute and return the id of the row with the smallest value for `field`.
    ///
    /// Ties are deterministic: equal field values resolve by primary key ascending.
    pub fn min_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.map_prepared_scalar_terminal_output(
                PreparedFluentScalarTerminalStrategy::id_by_slot(AggregateKind::Min, target_slot),
                FluentScalarTerminalOutput::into_id,
            )
        })
    }

    /// Execute and return the largest matching identifier, if any.
    pub fn max(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.map_prepared_scalar_terminal_output(
            PreparedFluentScalarTerminalStrategy::id_terminal(AggregateKind::Max),
            FluentScalarTerminalOutput::into_id,
        )
    }

    /// Explain scalar `max()` routing without executing the terminal.
    pub fn explain_max(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_prepared_aggregate_non_paged_terminal(
            &PreparedFluentScalarTerminalStrategy::id_terminal(AggregateKind::Max),
        )
    }

    /// Execute and return the id of the row with the largest value for `field`.
    ///
    /// Ties are deterministic: equal field values resolve by primary key ascending.
    pub fn max_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.map_prepared_scalar_terminal_output(
                PreparedFluentScalarTerminalStrategy::id_by_slot(AggregateKind::Max, target_slot),
                FluentScalarTerminalOutput::into_id,
            )
        })
    }

    /// Execute and return the id at zero-based ordinal `nth` when rows are
    /// ordered by `field` ascending, with primary-key ascending tie-breaks.
    pub fn nth_by(&self, field: impl AsRef<str>, nth: usize) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.map_prepared_order_sensitive_terminal_output(
                PreparedFluentOrderSensitiveTerminalStrategy::nth_by_slot(target_slot, nth),
                FluentScalarTerminalOutput::into_id,
            )
        })
    }

    /// Execute and return the sum of `field` over matching rows.
    pub fn sum_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.execute_prepared_numeric_field_terminal(
                PreparedFluentNumericFieldStrategy::sum_by_slot(target_slot),
            )
        })
    }

    /// Explain scalar `sum_by(field)` routing without executing the terminal.
    pub fn explain_sum_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.explain_prepared_aggregate_non_paged_terminal(
                &PreparedFluentNumericFieldStrategy::sum_by_slot(target_slot),
            )
        })
    }

    /// Execute and return the sum of distinct `field` values.
    pub fn sum_distinct_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.execute_prepared_numeric_field_terminal(
                PreparedFluentNumericFieldStrategy::sum_distinct_by_slot(target_slot),
            )
        })
    }

    /// Explain scalar `sum(distinct field)` routing without executing the terminal.
    pub fn explain_sum_distinct_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.explain_prepared_aggregate_non_paged_terminal(
                &PreparedFluentNumericFieldStrategy::sum_distinct_by_slot(target_slot),
            )
        })
    }

    /// Execute and return the average of `field` over matching rows.
    pub fn avg_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.execute_prepared_numeric_field_terminal(
                PreparedFluentNumericFieldStrategy::avg_by_slot(target_slot),
            )
        })
    }

    /// Explain scalar `avg_by(field)` routing without executing the terminal.
    pub fn explain_avg_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.explain_prepared_aggregate_non_paged_terminal(
                &PreparedFluentNumericFieldStrategy::avg_by_slot(target_slot),
            )
        })
    }

    /// Execute and return the average of distinct `field` values.
    pub fn avg_distinct_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.execute_prepared_numeric_field_terminal(
                PreparedFluentNumericFieldStrategy::avg_distinct_by_slot(target_slot),
            )
        })
    }

    /// Explain scalar `avg(distinct field)` routing without executing the terminal.
    pub fn explain_avg_distinct_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.explain_prepared_aggregate_non_paged_terminal(
                &PreparedFluentNumericFieldStrategy::avg_distinct_by_slot(target_slot),
            )
        })
    }

    /// Execute and return the median id by `field` using deterministic ordering
    /// `(field asc, primary key asc)`.
    ///
    /// Even-length windows select the lower median.
    pub fn median_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.map_prepared_order_sensitive_terminal_output(
                PreparedFluentOrderSensitiveTerminalStrategy::median_by_slot(target_slot),
                FluentScalarTerminalOutput::into_id,
            )
        })
    }

    /// Execute and return the number of distinct values for `field` over the
    /// effective result window.
    pub fn count_distinct_by(&self, field: impl AsRef<str>) -> Result<u32, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.map_prepared_projection_terminal_output(
                PreparedFluentProjectionStrategy::count_distinct_by_slot(target_slot),
                FluentProjectionTerminalOutput::into_count,
            )
        })
    }

    /// Explain `count_distinct_by(field)` routing without executing the terminal.
    pub fn explain_count_distinct_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.explain_prepared_projection_non_paged_terminal(
                &PreparedFluentProjectionStrategy::count_distinct_by_slot(target_slot),
            )
        })
    }

    /// Execute and return both `(min_by(field), max_by(field))` in one terminal.
    ///
    /// Tie handling is deterministic for both extrema: primary key ascending.
    pub fn min_max_by(&self, field: impl AsRef<str>) -> Result<MinMaxByIds<E>, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.map_prepared_order_sensitive_terminal_output(
                PreparedFluentOrderSensitiveTerminalStrategy::min_max_by_slot(target_slot),
                FluentScalarTerminalOutput::into_id_pair,
            )
        })
    }

    /// Execute and return projected field values for the effective result window.
    pub fn values_by(&self, field: impl AsRef<str>) -> Result<Vec<OutputValue>, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.map_prepared_projection_terminal_output(
                PreparedFluentProjectionStrategy::values_by_slot(target_slot),
                FluentProjectionTerminalOutput::into_values,
            )
            .map(output_values)
        })
    }

    /// Execute and return projected values for one shared bounded projection
    /// over the effective response window.
    pub fn project_values<P>(&self, projection: &P) -> Result<Vec<OutputValue>, QueryError>
    where
        E: EntityValue,
        P: ValueProjectionExpr,
    {
        self.with_non_paged_slot(projection.field(), |target_slot| {
            let values = self
                .execute_prepared_projection_terminal_output(
                    PreparedFluentProjectionStrategy::values_by_slot(target_slot),
                )?
                .into_values()
                .map_err(QueryError::execute)?;

            Self::project_terminal_items(projection, values, |projection, value| {
                projection.apply_value(value)
            })
            .map(output_values)
        })
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
        self.with_non_paged_slot(projection.field(), |target_slot| {
            self.explain_prepared_projection_non_paged_terminal(
                &PreparedFluentProjectionStrategy::values_by_slot(target_slot),
            )
        })
    }

    /// Explain `values_by(field)` routing without executing the terminal.
    pub fn explain_values_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.explain_prepared_projection_non_paged_terminal(
                &PreparedFluentProjectionStrategy::values_by_slot(target_slot),
            )
        })
    }

    /// Execute and return the first `k` rows from the effective response window.
    pub fn take(&self, take_count: u32) -> Result<EntityResponse<E>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session.execute_fluent_take(self.query(), take_count)
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
        self.with_non_paged_slot(field, |target_slot| {
            self.session.execute_fluent_ranked_rows_by_slot(
                self.query(),
                target_slot,
                take_count,
                true,
            )
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
        self.with_non_paged_slot(field, |target_slot| {
            self.session.execute_fluent_ranked_rows_by_slot(
                self.query(),
                target_slot,
                take_count,
                false,
            )
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
        self.with_non_paged_slot(field, |target_slot| {
            self.session
                .execute_fluent_ranked_values_by_slot(self.query(), target_slot, take_count, true)
                .map(output_values)
        })
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
        self.with_non_paged_slot(field, |target_slot| {
            self.session
                .execute_fluent_ranked_values_by_slot(self.query(), target_slot, take_count, false)
                .map(output_values)
        })
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
        self.with_non_paged_slot(field, |target_slot| {
            self.session
                .execute_fluent_ranked_values_with_ids_by_slot(
                    self.query(),
                    target_slot,
                    take_count,
                    true,
                )
                .map(output_values_with_ids)
        })
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
        self.with_non_paged_slot(field, |target_slot| {
            self.session
                .execute_fluent_ranked_values_with_ids_by_slot(
                    self.query(),
                    target_slot,
                    take_count,
                    false,
                )
                .map(output_values_with_ids)
        })
    }

    /// Execute and return distinct projected field values for the effective
    /// result window, preserving first-observed value order.
    pub fn distinct_values_by(&self, field: impl AsRef<str>) -> Result<Vec<OutputValue>, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.map_prepared_projection_terminal_output(
                PreparedFluentProjectionStrategy::distinct_values_by_slot(target_slot),
                FluentProjectionTerminalOutput::into_values,
            )
            .map(output_values)
        })
    }

    /// Explain `distinct_values_by(field)` routing without executing the terminal.
    pub fn explain_distinct_values_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.explain_prepared_projection_non_paged_terminal(
                &PreparedFluentProjectionStrategy::distinct_values_by_slot(target_slot),
            )
        })
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
        self.with_non_paged_slot(field, |target_slot| {
            self.map_prepared_projection_terminal_output(
                PreparedFluentProjectionStrategy::values_by_with_ids_slot(target_slot),
                FluentProjectionTerminalOutput::into_values_with_ids,
            )
            .map(output_values_with_ids)
        })
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
        self.with_non_paged_slot(projection.field(), |target_slot| {
            let values = self
                .execute_prepared_projection_terminal_output(
                    PreparedFluentProjectionStrategy::values_by_with_ids_slot(target_slot),
                )?
                .into_values_with_ids()
                .map_err(QueryError::execute)?;

            Self::project_terminal_items(projection, values, |projection, (id, value)| {
                Ok((id, projection.apply_value(value)?))
            })
            .map(output_values_with_ids)
        })
    }

    /// Explain `values_by_with_ids(field)` routing without executing the terminal.
    pub fn explain_values_by_with_ids(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.explain_prepared_projection_non_paged_terminal(
                &PreparedFluentProjectionStrategy::values_by_with_ids_slot(target_slot),
            )
        })
    }

    /// Execute and return the first projected field value in effective response
    /// order, if any.
    pub fn first_value_by(&self, field: impl AsRef<str>) -> Result<Option<OutputValue>, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.map_prepared_projection_terminal_output(
                PreparedFluentProjectionStrategy::first_value_by_slot(target_slot),
                FluentProjectionTerminalOutput::into_terminal_value,
            )
            .map(|value| value.map(output))
        })
    }

    /// Execute and return the first projected value for one shared bounded
    /// projection in effective response order, if any.
    pub fn project_first_value<P>(&self, projection: &P) -> Result<Option<OutputValue>, QueryError>
    where
        E: EntityValue,
        P: ValueProjectionExpr,
    {
        self.with_non_paged_slot(projection.field(), |target_slot| {
            let value = self
                .execute_prepared_projection_terminal_output(
                    PreparedFluentProjectionStrategy::first_value_by_slot(target_slot),
                )?
                .into_terminal_value()
                .map_err(QueryError::execute)?;

            let mut projected =
                Self::project_terminal_items(projection, value, |projection, value| {
                    projection.apply_value(value)
                })?;

            Ok(projected.pop().map(output))
        })
    }

    /// Explain `first_value_by(field)` routing without executing the terminal.
    pub fn explain_first_value_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.explain_prepared_projection_non_paged_terminal(
                &PreparedFluentProjectionStrategy::first_value_by_slot(target_slot),
            )
        })
    }

    /// Execute and return the last projected field value in effective response
    /// order, if any.
    pub fn last_value_by(&self, field: impl AsRef<str>) -> Result<Option<OutputValue>, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.map_prepared_projection_terminal_output(
                PreparedFluentProjectionStrategy::last_value_by_slot(target_slot),
                FluentProjectionTerminalOutput::into_terminal_value,
            )
            .map(|value| value.map(output))
        })
    }

    /// Execute and return the last projected value for one shared bounded
    /// projection in effective response order, if any.
    pub fn project_last_value<P>(&self, projection: &P) -> Result<Option<OutputValue>, QueryError>
    where
        E: EntityValue,
        P: ValueProjectionExpr,
    {
        self.with_non_paged_slot(projection.field(), |target_slot| {
            let value = self
                .execute_prepared_projection_terminal_output(
                    PreparedFluentProjectionStrategy::last_value_by_slot(target_slot),
                )?
                .into_terminal_value()
                .map_err(QueryError::execute)?;

            let mut projected =
                Self::project_terminal_items(projection, value, |projection, value| {
                    projection.apply_value(value)
                })?;

            Ok(projected.pop().map(output))
        })
    }

    /// Explain `last_value_by(field)` routing without executing the terminal.
    pub fn explain_last_value_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged_slot(field, |target_slot| {
            self.explain_prepared_projection_non_paged_terminal(
                &PreparedFluentProjectionStrategy::last_value_by_slot(target_slot),
            )
        })
    }

    /// Execute and return the first matching identifier in response order, if any.
    pub fn first(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.map_prepared_order_sensitive_terminal_output(
            PreparedFluentOrderSensitiveTerminalStrategy::first(),
            FluentScalarTerminalOutput::into_id,
        )
    }

    /// Explain scalar `first()` routing without executing the terminal.
    pub fn explain_first(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_prepared_aggregate_non_paged_terminal(
            &PreparedFluentOrderSensitiveTerminalStrategy::first(),
        )
    }

    /// Execute and return the last matching identifier in response order, if any.
    pub fn last(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.map_prepared_order_sensitive_terminal_output(
            PreparedFluentOrderSensitiveTerminalStrategy::last(),
            FluentScalarTerminalOutput::into_id,
        )
    }

    /// Explain scalar `last()` routing without executing the terminal.
    pub fn explain_last(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_prepared_aggregate_non_paged_terminal(
            &PreparedFluentOrderSensitiveTerminalStrategy::last(),
        )
    }

    /// Execute and require exactly one matching row.
    pub fn require_one(&self) -> Result<(), QueryError>
    where
        E: EntityValue,
    {
        self.execute()?.into_rows()?.require_one()?;
        Ok(())
    }

    /// Execute and require at least one matching row.
    pub fn require_some(&self) -> Result<(), QueryError>
    where
        E: EntityValue,
    {
        self.execute()?.into_rows()?.require_some()?;
        Ok(())
    }
}
