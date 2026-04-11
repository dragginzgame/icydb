//! Module: query::fluent::load::terminals
//! Responsibility: fluent load terminal APIs and terminal-plan explanation entrypoints.
//! Does not own: planner semantic validation or executor runtime routing decisions.
//! Boundary: delegates to session planning/execution and returns typed query results.

use crate::{
    db::{
        PersistedRow,
        executor::{
            LoadExecutor, PreparedExecutionPlan, ScalarNumericFieldBoundaryRequest,
            ScalarProjectionBoundaryRequest, ScalarTerminalBoundaryOutput,
            ScalarTerminalBoundaryRequest,
        },
        query::{
            api::ResponseCardinalityExt,
            builder::{
                PreparedFluentAggregateExplainStrategy,
                PreparedFluentExistingRowsTerminalRuntimeRequest,
                PreparedFluentExistingRowsTerminalStrategy,
                PreparedFluentNumericFieldRuntimeRequest, PreparedFluentNumericFieldStrategy,
                PreparedFluentOrderSensitiveTerminalRuntimeRequest,
                PreparedFluentOrderSensitiveTerminalStrategy,
                PreparedFluentProjectionRuntimeRequest, PreparedFluentProjectionStrategy,
                PreparedFluentScalarTerminalRuntimeRequest, PreparedFluentScalarTerminalStrategy,
            },
            explain::{ExplainAggregateTerminalPlan, ExplainExecutionNodeDescriptor},
            fluent::load::FluentLoadQuery,
            intent::QueryError,
            plan::AggregateKind,
        },
        response::EntityResponse,
    },
    error::InternalError,
    traits::EntityValue,
    types::{Decimal, Id},
    value::Value,
};

type MinMaxByIds<E> = Option<(Id<E>, Id<E>)>;

impl<E> FluentLoadQuery<'_, E>
where
    E: PersistedRow,
{
    // ------------------------------------------------------------------
    // Execution (single semantic boundary)
    // ------------------------------------------------------------------

    /// Execute this query using the session's policy settings.
    pub fn execute(&self) -> Result<EntityResponse<E>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session.execute_query(self.query())
    }

    // Run one scalar terminal through the canonical non-paged fluent policy
    // gate before handing execution to the session load-query adapter.
    fn execute_scalar_non_paged_terminal<T, F>(&self, execute: F) -> Result<T, QueryError>
    where
        E: EntityValue,
        F: FnOnce(LoadExecutor<E>, PreparedExecutionPlan<E>) -> Result<T, InternalError>,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session.execute_load_query_with(self.query(), execute)
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
        self.ensure_non_paged_mode_ready()?;

        self.session
            .explain_query_prepared_aggregate_terminal_with_visible_indexes(self.query(), strategy)
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
        self.ensure_non_paged_mode_ready()?;

        self.session
            .explain_query_prepared_projection_terminal_with_visible_indexes(self.query(), strategy)
    }

    // Execute one prepared fluent scalar terminal through the canonical
    // non-paged fluent policy gate using the prepared runtime request as the
    // single execution source.
    fn execute_prepared_scalar_terminal_output(
        &self,
        strategy: PreparedFluentScalarTerminalStrategy,
    ) -> Result<ScalarTerminalBoundaryOutput, QueryError>
    where
        E: EntityValue,
    {
        let runtime_request = strategy.into_runtime_request();

        self.execute_scalar_non_paged_terminal(move |load, plan| {
            load.execute_scalar_terminal_request(
                plan,
                scalar_terminal_boundary_request_from_prepared(runtime_request),
            )
        })
    }

    // Execute one prepared fluent existing-rows terminal through the
    // canonical non-paged fluent policy gate using the prepared existing-rows
    // strategy as the single runtime source.
    fn execute_prepared_existing_rows_terminal_output(
        &self,
        strategy: PreparedFluentExistingRowsTerminalStrategy,
    ) -> Result<ScalarTerminalBoundaryOutput, QueryError>
    where
        E: EntityValue,
    {
        let runtime_request = strategy.into_runtime_request();

        self.execute_scalar_non_paged_terminal(move |load, plan| {
            load.execute_scalar_terminal_request(
                plan,
                existing_rows_terminal_boundary_request_from_prepared(runtime_request),
            )
        })
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
        let (target_field, runtime_request) = strategy.into_runtime_parts();

        self.execute_scalar_non_paged_terminal(move |load, plan| {
            load.execute_numeric_field_boundary(
                plan,
                target_field,
                numeric_field_boundary_request_from_prepared(runtime_request),
            )
        })
    }

    // Execute one prepared fluent order-sensitive terminal through the
    // canonical non-paged fluent policy gate using the prepared order-sensitive
    // strategy as the single runtime source.
    fn execute_prepared_order_sensitive_terminal_output(
        &self,
        strategy: PreparedFluentOrderSensitiveTerminalStrategy,
    ) -> Result<ScalarTerminalBoundaryOutput, QueryError>
    where
        E: EntityValue,
    {
        let runtime_request = strategy.into_runtime_request();

        self.execute_scalar_non_paged_terminal(move |load, plan| {
            load.execute_scalar_terminal_request(
                plan,
                order_sensitive_terminal_boundary_request_from_prepared(runtime_request),
            )
        })
    }

    // Execute one prepared fluent projection/distinct terminal through the
    // canonical non-paged fluent policy gate using the prepared projection
    // strategy as the single runtime source.
    fn execute_prepared_projection_terminal_output(
        &self,
        strategy: PreparedFluentProjectionStrategy,
    ) -> Result<crate::db::executor::ScalarProjectionBoundaryOutput, QueryError>
    where
        E: EntityValue,
    {
        let (target_field, runtime_request) = strategy.into_runtime_parts();

        self.execute_scalar_non_paged_terminal(move |load, plan| {
            load.execute_scalar_projection_boundary(
                plan,
                target_field,
                projection_boundary_request_from_prepared(runtime_request),
            )
        })
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
        self.execute_prepared_existing_rows_terminal_output(
            PreparedFluentExistingRowsTerminalStrategy::exists_rows(),
        )?
        .into_exists()
        .map_err(QueryError::execute)
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
        self.session
            .explain_query_execution_with_visible_indexes(self.query())
    }

    /// Explain scalar load execution shape as deterministic text.
    pub fn explain_execution_text(&self) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        self.session
            .explain_query_execution_text_with_visible_indexes(self.query())
    }

    /// Explain scalar load execution shape as canonical JSON.
    pub fn explain_execution_json(&self) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        self.session
            .explain_query_execution_json_with_visible_indexes(self.query())
    }

    /// Explain scalar load execution shape as verbose text with diagnostics.
    pub fn explain_execution_verbose(&self) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        self.session
            .explain_query_execution_verbose_with_visible_indexes(self.query())
    }

    /// Execute and return the number of matching rows.
    pub fn count(&self) -> Result<u32, QueryError>
    where
        E: EntityValue,
    {
        self.execute_prepared_existing_rows_terminal_output(
            PreparedFluentExistingRowsTerminalStrategy::count_rows(),
        )?
        .into_count()
        .map_err(QueryError::execute)
    }

    /// Execute and return the total persisted payload bytes for the effective
    /// result window.
    pub fn bytes(&self) -> Result<u64, QueryError>
    where
        E: EntityValue,
    {
        self.execute_scalar_non_paged_terminal(|load, plan| load.bytes(plan))
    }

    /// Execute and return the total serialized bytes for `field` over the
    /// effective result window.
    pub fn bytes_by(&self, field: impl AsRef<str>) -> Result<u64, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.bytes_by_slot(plan, target_slot)
                })
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
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .explain_query_bytes_by_with_visible_indexes(self.query(), target_slot.field())
        })
    }

    /// Execute and return the smallest matching identifier, if any.
    pub fn min(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_prepared_scalar_terminal_output(
            PreparedFluentScalarTerminalStrategy::id_terminal(AggregateKind::Min),
        )?
        .into_id()
        .map_err(QueryError::execute)
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
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.execute_prepared_scalar_terminal_output(
                PreparedFluentScalarTerminalStrategy::id_by_slot(AggregateKind::Min, target_slot),
            )?
            .into_id()
            .map_err(QueryError::execute)
        })
    }

    /// Execute and return the largest matching identifier, if any.
    pub fn max(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_prepared_scalar_terminal_output(
            PreparedFluentScalarTerminalStrategy::id_terminal(AggregateKind::Max),
        )?
        .into_id()
        .map_err(QueryError::execute)
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
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.execute_prepared_scalar_terminal_output(
                PreparedFluentScalarTerminalStrategy::id_by_slot(AggregateKind::Max, target_slot),
            )?
            .into_id()
            .map_err(QueryError::execute)
        })
    }

    /// Execute and return the id at zero-based ordinal `nth` when rows are
    /// ordered by `field` ascending, with primary-key ascending tie-breaks.
    pub fn nth_by(&self, field: impl AsRef<str>, nth: usize) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.execute_prepared_order_sensitive_terminal_output(
                PreparedFluentOrderSensitiveTerminalStrategy::nth_by_slot(target_slot, nth),
            )?
            .into_id()
            .map_err(QueryError::execute)
        })
    }

    /// Execute and return the sum of `field` over matching rows.
    pub fn sum_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
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
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
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
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
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
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
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
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
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
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
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
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
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
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
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
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.execute_prepared_order_sensitive_terminal_output(
                PreparedFluentOrderSensitiveTerminalStrategy::median_by_slot(target_slot),
            )?
            .into_id()
            .map_err(QueryError::execute)
        })
    }

    /// Execute and return the number of distinct values for `field` over the
    /// effective result window.
    pub fn count_distinct_by(&self, field: impl AsRef<str>) -> Result<u32, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.execute_prepared_projection_terminal_output(
                PreparedFluentProjectionStrategy::count_distinct_by_slot(target_slot),
            )?
            .into_count()
            .map_err(QueryError::execute)
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
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
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
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.execute_prepared_order_sensitive_terminal_output(
                PreparedFluentOrderSensitiveTerminalStrategy::min_max_by_slot(target_slot),
            )?
            .into_id_pair()
            .map_err(QueryError::execute)
        })
    }

    /// Execute and return projected field values for the effective result window.
    pub fn values_by(&self, field: impl AsRef<str>) -> Result<Vec<Value>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.execute_prepared_projection_terminal_output(
                PreparedFluentProjectionStrategy::values_by_slot(target_slot),
            )?
            .into_values()
            .map_err(QueryError::execute)
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
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
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
        self.execute_scalar_non_paged_terminal(|load, plan| load.take(plan, take_count))
    }

    /// Execute and return the top `k` rows by `field` under deterministic
    /// ordering `(field desc, primary_key asc)` over the effective response
    /// window.
    ///
    /// This terminal applies its own ordering and does not preserve query
    /// `order_by(...)` row order in the returned rows. For `k = 1`, this
    /// matches `max_by(field)` selection semantics.
    pub fn top_k_by(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<EntityResponse<E>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.top_k_by_slot(plan, target_slot, take_count)
                })
        })
    }

    /// Execute and return the bottom `k` rows by `field` under deterministic
    /// ordering `(field asc, primary_key asc)` over the effective response
    /// window.
    ///
    /// This terminal applies its own ordering and does not preserve query
    /// `order_by(...)` row order in the returned rows. For `k = 1`, this
    /// matches `min_by(field)` selection semantics.
    pub fn bottom_k_by(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<EntityResponse<E>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.bottom_k_by_slot(plan, target_slot, take_count)
                })
        })
    }

    /// Execute and return projected values for the top `k` rows by `field`
    /// under deterministic ordering `(field desc, primary_key asc)` over the
    /// effective response window.
    ///
    /// Ranking is applied before projection and does not preserve query
    /// `order_by(...)` row order in the returned values. For `k = 1`, this
    /// matches `max_by(field)` projected to one value.
    pub fn top_k_by_values(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<Vec<Value>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.top_k_by_values_slot(plan, target_slot, take_count)
                })
        })
    }

    /// Execute and return projected values for the bottom `k` rows by `field`
    /// under deterministic ordering `(field asc, primary_key asc)` over the
    /// effective response window.
    ///
    /// Ranking is applied before projection and does not preserve query
    /// `order_by(...)` row order in the returned values. For `k = 1`, this
    /// matches `min_by(field)` projected to one value.
    pub fn bottom_k_by_values(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<Vec<Value>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.bottom_k_by_values_slot(plan, target_slot, take_count)
                })
        })
    }

    /// Execute and return projected id/value pairs for the top `k` rows by
    /// `field` under deterministic ordering `(field desc, primary_key asc)`
    /// over the effective response window.
    ///
    /// Ranking is applied before projection and does not preserve query
    /// `order_by(...)` row order in the returned values. For `k = 1`, this
    /// matches `max_by(field)` projected to one `(id, value)` pair.
    pub fn top_k_by_with_ids(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.top_k_by_with_ids_slot(plan, target_slot, take_count)
                })
        })
    }

    /// Execute and return projected id/value pairs for the bottom `k` rows by
    /// `field` under deterministic ordering `(field asc, primary_key asc)`
    /// over the effective response window.
    ///
    /// Ranking is applied before projection and does not preserve query
    /// `order_by(...)` row order in the returned values. For `k = 1`, this
    /// matches `min_by(field)` projected to one `(id, value)` pair.
    pub fn bottom_k_by_with_ids(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.bottom_k_by_with_ids_slot(plan, target_slot, take_count)
                })
        })
    }

    /// Execute and return distinct projected field values for the effective
    /// result window, preserving first-observed value order.
    pub fn distinct_values_by(&self, field: impl AsRef<str>) -> Result<Vec<Value>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.execute_prepared_projection_terminal_output(
                PreparedFluentProjectionStrategy::distinct_values_by_slot(target_slot),
            )?
            .into_values()
            .map_err(QueryError::execute)
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
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
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
    ) -> Result<Vec<(Id<E>, Value)>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.execute_prepared_projection_terminal_output(
                PreparedFluentProjectionStrategy::values_by_with_ids_slot(target_slot),
            )?
            .into_values_with_ids()
            .map_err(QueryError::execute)
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
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.explain_prepared_projection_non_paged_terminal(
                &PreparedFluentProjectionStrategy::values_by_with_ids_slot(target_slot),
            )
        })
    }

    /// Execute and return the first projected field value in effective response
    /// order, if any.
    pub fn first_value_by(&self, field: impl AsRef<str>) -> Result<Option<Value>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.execute_prepared_projection_terminal_output(
                PreparedFluentProjectionStrategy::first_value_by_slot(target_slot),
            )?
            .into_terminal_value()
            .map_err(QueryError::execute)
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
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.explain_prepared_projection_non_paged_terminal(
                &PreparedFluentProjectionStrategy::first_value_by_slot(target_slot),
            )
        })
    }

    /// Execute and return the last projected field value in effective response
    /// order, if any.
    pub fn last_value_by(&self, field: impl AsRef<str>) -> Result<Option<Value>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.execute_prepared_projection_terminal_output(
                PreparedFluentProjectionStrategy::last_value_by_slot(target_slot),
            )?
            .into_terminal_value()
            .map_err(QueryError::execute)
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
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
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
        self.execute_prepared_order_sensitive_terminal_output(
            PreparedFluentOrderSensitiveTerminalStrategy::first(),
        )?
        .into_id()
        .map_err(QueryError::execute)
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
        self.execute_prepared_order_sensitive_terminal_output(
            PreparedFluentOrderSensitiveTerminalStrategy::last(),
        )?
        .into_id()
        .map_err(QueryError::execute)
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
        self.execute()?.require_one()?;
        Ok(())
    }

    /// Execute and require at least one matching row.
    pub fn require_some(&self) -> Result<(), QueryError>
    where
        E: EntityValue,
    {
        self.execute()?.require_some()?;
        Ok(())
    }
}

fn scalar_terminal_boundary_request_from_prepared(
    request: PreparedFluentScalarTerminalRuntimeRequest,
) -> ScalarTerminalBoundaryRequest {
    match request {
        PreparedFluentScalarTerminalRuntimeRequest::IdTerminal { kind } => {
            ScalarTerminalBoundaryRequest::IdTerminal { kind }
        }
        PreparedFluentScalarTerminalRuntimeRequest::IdBySlot { kind, target_field } => {
            ScalarTerminalBoundaryRequest::IdBySlot { kind, target_field }
        }
    }
}

const fn existing_rows_terminal_boundary_request_from_prepared(
    request: PreparedFluentExistingRowsTerminalRuntimeRequest,
) -> ScalarTerminalBoundaryRequest {
    match request {
        PreparedFluentExistingRowsTerminalRuntimeRequest::CountRows => {
            ScalarTerminalBoundaryRequest::Count
        }
        PreparedFluentExistingRowsTerminalRuntimeRequest::ExistsRows => {
            ScalarTerminalBoundaryRequest::Exists
        }
    }
}

const fn numeric_field_boundary_request_from_prepared(
    request: PreparedFluentNumericFieldRuntimeRequest,
) -> ScalarNumericFieldBoundaryRequest {
    match request {
        PreparedFluentNumericFieldRuntimeRequest::Sum => ScalarNumericFieldBoundaryRequest::Sum,
        PreparedFluentNumericFieldRuntimeRequest::SumDistinct => {
            ScalarNumericFieldBoundaryRequest::SumDistinct
        }
        PreparedFluentNumericFieldRuntimeRequest::Avg => ScalarNumericFieldBoundaryRequest::Avg,
        PreparedFluentNumericFieldRuntimeRequest::AvgDistinct => {
            ScalarNumericFieldBoundaryRequest::AvgDistinct
        }
    }
}

fn order_sensitive_terminal_boundary_request_from_prepared(
    request: PreparedFluentOrderSensitiveTerminalRuntimeRequest,
) -> ScalarTerminalBoundaryRequest {
    match request {
        PreparedFluentOrderSensitiveTerminalRuntimeRequest::ResponseOrder { kind } => {
            ScalarTerminalBoundaryRequest::IdTerminal { kind }
        }
        PreparedFluentOrderSensitiveTerminalRuntimeRequest::NthBySlot { target_field, nth } => {
            ScalarTerminalBoundaryRequest::NthBySlot { target_field, nth }
        }
        PreparedFluentOrderSensitiveTerminalRuntimeRequest::MedianBySlot { target_field } => {
            ScalarTerminalBoundaryRequest::MedianBySlot { target_field }
        }
        PreparedFluentOrderSensitiveTerminalRuntimeRequest::MinMaxBySlot { target_field } => {
            ScalarTerminalBoundaryRequest::MinMaxBySlot { target_field }
        }
    }
}

const fn projection_boundary_request_from_prepared(
    request: PreparedFluentProjectionRuntimeRequest,
) -> ScalarProjectionBoundaryRequest {
    match request {
        PreparedFluentProjectionRuntimeRequest::Values => ScalarProjectionBoundaryRequest::Values,
        PreparedFluentProjectionRuntimeRequest::DistinctValues => {
            ScalarProjectionBoundaryRequest::DistinctValues
        }
        PreparedFluentProjectionRuntimeRequest::CountDistinct => {
            ScalarProjectionBoundaryRequest::CountDistinct
        }
        PreparedFluentProjectionRuntimeRequest::ValuesWithIds => {
            ScalarProjectionBoundaryRequest::ValuesWithIds
        }
        PreparedFluentProjectionRuntimeRequest::TerminalValue { terminal_kind } => {
            ScalarProjectionBoundaryRequest::TerminalValue { terminal_kind }
        }
    }
}
