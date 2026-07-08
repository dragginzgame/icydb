//! Module: query::fluent::load::terminals
//! Responsibility: fluent load terminal APIs and terminal-plan explanation entrypoints.
//! Does not own: planner semantic validation or executor runtime routing decisions.
//! Boundary: delegates to session planning/execution and returns typed query results.
//!
//! Terminal Execution Model
//!
//! Fluent terminals are concrete descriptors with a 1:1 mapping to session
//! execution and explain entrypoints. This module may orchestrate descriptor
//! construction, non-paged gating, and public output shaping, but it must not
//! carry terminal-kind enums, transport output enums, or match-based execution
//! dispatch. Adding a new terminal means adding a new descriptor type and one
//! direct `TerminalStrategyDriver` implementation for that descriptor.

mod driver;
mod output;
mod read_intent;
mod support;

#[cfg(feature = "diagnostics")]
use crate::db::FluentTerminalExecutionAttribution;
#[cfg(feature = "diagnostics")]
use crate::db::QueryExecutionAttribution;
use crate::db::query::read_intent::ReadIntentKind;
use crate::{
    db::{
        DbSession, PersistedRow,
        query::{
            api::ResponseCardinalityExt,
            builder::{
                AvgBySlotTerminal, AvgDistinctBySlotTerminal, CountDistinctBySlotTerminal,
                CountRowsTerminal, DistinctValuesBySlotTerminal, FirstIdTerminal,
                FirstValueBySlotTerminal, LastIdTerminal, LastValueBySlotTerminal,
                MaxIdBySlotTerminal, MaxIdTerminal, MedianIdBySlotTerminal, MinIdBySlotTerminal,
                MinIdTerminal, MinMaxIdBySlotTerminal, NthIdBySlotTerminal, SumBySlotTerminal,
                SumDistinctBySlotTerminal, ValueProjectionExpr, ValuesBySlotTerminal,
                ValuesBySlotWithIdsTerminal,
            },
            explain::{ExplainAggregateTerminalPlan, ExplainExecutionNodeDescriptor},
            fluent::load::{FluentLoadQuery, LoadQueryResult},
            intent::{IntentError, QueryError},
            plan::AggregateKind,
        },
        response::EntityResponse,
    },
    traits::EntityValue,
    types::{Decimal, Id},
    value::OutputValue,
};
use driver::MinMaxByIds;
use output::{output, output_values, output_values_with_ids};

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
        self.with_admitted_non_paged(DbSession::execute_query_result)
    }

    /// Execute this query with diagnostics attribution through the same
    /// admitted non-paged boundary as `execute()`.
    #[cfg(feature = "diagnostics")]
    #[doc(hidden)]
    pub fn execute_with_attribution(
        &self,
    ) -> Result<(LoadQueryResult<E>, QueryExecutionAttribution), QueryError>
    where
        E: EntityValue,
    {
        self.with_admitted_non_paged(DbSession::execute_query_result_with_attribution)
    }

    /// Execute this query through the scalar rows-only session boundary.
    pub fn execute_rows(&self) -> Result<EntityResponse<E>, QueryError>
    where
        E: EntityValue,
    {
        self.with_admitted_non_paged(DbSession::execute_scalar_query_rows)
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
        self.with_non_paged(DbSession::explain_query_execution_json_with_visible_indexes)
    }

    /// Explain scalar load execution shape as verbose text with diagnostics.
    pub fn explain_execution_verbose(&self) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged(DbSession::explain_query_execution_verbose_with_visible_indexes)
    }

    /// Execute and return the number of matching rows.
    pub fn count(&self) -> Result<u32, QueryError>
    where
        E: EntityValue,
    {
        self.execute_terminal(CountRowsTerminal::new())
    }

    /// Execute and return the exact number of matching rows.
    ///
    /// Unlike `count()`, this semantic aggregate rejects a prior raw
    /// `limit(...)` so exact counts cannot accidentally mean "count the first
    /// N rows."
    pub fn count_exact(&self) -> Result<u32, QueryError>
    where
        E: EntityValue,
    {
        self.execute_exact_aggregate_terminal(
            IntentError::raw_limit_before_count_exact_terminal(),
            CountRowsTerminal::new(),
        )
    }

    /// Explain exact count routing without executing the terminal.
    pub fn explain_count_exact(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_checked_exact_aggregate_terminal(
            IntentError::raw_limit_before_count_exact_terminal(),
            &CountRowsTerminal::new(),
        )
    }

    /// Execute and return the number of matching rows with terminal attribution.
    #[cfg(feature = "diagnostics")]
    #[doc(hidden)]
    pub fn count_with_attribution(
        &self,
    ) -> Result<(u32, FluentTerminalExecutionAttribution), QueryError>
    where
        E: EntityValue,
    {
        self.execute_count_terminal_with_attribution(ReadIntentKind::BoundedRowWindow)
    }

    /// Execute and return the exact number of matching rows with terminal attribution.
    #[cfg(feature = "diagnostics")]
    #[doc(hidden)]
    pub fn count_exact_with_attribution(
        &self,
    ) -> Result<(u32, FluentTerminalExecutionAttribution), QueryError>
    where
        E: EntityValue,
    {
        self.ensure_exact_aggregate_intent_owns_limit(
            IntentError::raw_limit_before_count_exact_terminal(),
        )?;

        self.execute_count_terminal_with_attribution(ReadIntentKind::ExactAggregate)
    }

    /// Execute and return the total persisted payload bytes for the effective
    /// result window.
    pub fn bytes(&self) -> Result<u64, QueryError>
    where
        E: EntityValue,
    {
        self.with_admitted_non_paged(DbSession::execute_fluent_bytes)
    }

    /// Execute and return the total serialized bytes for `field` over the
    /// effective result window.
    pub fn bytes_by(&self, field: impl AsRef<str>) -> Result<u64, QueryError>
    where
        E: EntityValue,
    {
        self.with_admitted_non_paged_slot(field, |session, query, target_slot| {
            session.execute_fluent_bytes_by_slot(query, target_slot)
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
        self.with_non_paged_slot(field, |session, query, target_slot| {
            session.explain_query_bytes_by_with_visible_indexes(query, target_slot.field())
        })
    }

    /// Execute and return the smallest matching identifier, if any.
    pub fn min(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_terminal(MinIdTerminal::new())
    }

    /// Execute and return the exact smallest matching identifier.
    ///
    /// Unlike `min()`, this semantic aggregate rejects a prior raw
    /// `limit(...)` so exact minimum selection cannot accidentally mean
    /// "minimum over the first N rows."
    pub fn min_id_exact(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_exact_aggregate_terminal(
            IntentError::raw_limit_before_min_exact_terminal(),
            MinIdTerminal::new(),
        )
    }

    /// Explain scalar `min()` routing without executing the terminal.
    pub fn explain_min(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_terminal(&MinIdTerminal::new())
    }

    /// Explain exact `min_id_exact()` routing without executing the terminal.
    pub fn explain_min_id_exact(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_checked_exact_aggregate_terminal(
            IntentError::raw_limit_before_min_exact_terminal(),
            &MinIdTerminal::new(),
        )
    }

    /// Execute and return the id of the row with the smallest value for `field`.
    ///
    /// Ties are deterministic: equal field values resolve by primary key ascending.
    pub fn min_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, MinIdBySlotTerminal::new)
    }

    /// Execute and return the id of the row with the exact minimum `field` value.
    ///
    /// Ties are deterministic: equal field values resolve by primary key ascending.
    /// A prior row-window cap is rejected because the terminal owns the exact
    /// aggregate intent.
    pub fn min_exact_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_exact_aggregate_by_slot_terminal(
            field,
            IntentError::raw_limit_before_min_exact_terminal(),
            MinIdBySlotTerminal::new,
        )
    }

    /// Explain exact `min_exact_by(field)` routing without executing the terminal.
    pub fn explain_min_exact_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_exact_aggregate_by_slot_terminal(
            field,
            IntentError::raw_limit_before_min_exact_terminal(),
            MinIdBySlotTerminal::new,
        )
    }

    /// Execute and return the largest matching identifier, if any.
    pub fn max(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_terminal(MaxIdTerminal::new())
    }

    /// Execute and return the exact largest matching identifier.
    ///
    /// Unlike `max()`, this semantic aggregate rejects a prior row-window cap
    /// so exact maximum selection cannot accidentally mean
    /// "maximum over the first N rows."
    pub fn max_id_exact(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_exact_aggregate_terminal(
            IntentError::raw_limit_before_max_exact_terminal(),
            MaxIdTerminal::new(),
        )
    }

    /// Explain scalar `max()` routing without executing the terminal.
    pub fn explain_max(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_terminal(&MaxIdTerminal::new())
    }

    /// Explain exact `max_id_exact()` routing without executing the terminal.
    pub fn explain_max_id_exact(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_checked_exact_aggregate_terminal(
            IntentError::raw_limit_before_max_exact_terminal(),
            &MaxIdTerminal::new(),
        )
    }

    /// Execute and return the id of the row with the largest value for `field`.
    ///
    /// Ties are deterministic: equal field values resolve by primary key ascending.
    pub fn max_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, MaxIdBySlotTerminal::new)
    }

    /// Execute and return the id of the row with the exact maximum `field` value.
    ///
    /// Ties are deterministic: equal field values resolve by primary key ascending.
    /// A prior row-window cap is rejected because the terminal owns the exact
    /// aggregate intent.
    pub fn max_exact_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_exact_aggregate_by_slot_terminal(
            field,
            IntentError::raw_limit_before_max_exact_terminal(),
            MaxIdBySlotTerminal::new,
        )
    }

    /// Explain exact `max_exact_by(field)` routing without executing the terminal.
    pub fn explain_max_exact_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_exact_aggregate_by_slot_terminal(
            field,
            IntentError::raw_limit_before_max_exact_terminal(),
            MaxIdBySlotTerminal::new,
        )
    }

    /// Execute and return the id at zero-based ordinal `nth` when rows are
    /// ordered by `field` ascending, with primary-key ascending tie-breaks.
    pub fn nth_by(&self, field: impl AsRef<str>, nth: usize) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, |target_slot| {
            NthIdBySlotTerminal::new(target_slot, nth)
        })
    }

    /// Execute and return the sum of `field` over matching rows.
    pub fn sum_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, SumBySlotTerminal::new)
    }

    /// Execute and return the exact sum of `field` over matching rows.
    ///
    /// Unlike `sum_by(...)`, this semantic aggregate rejects a prior raw
    /// `limit(...)` so exact sums cannot accidentally mean "sum the first N
    /// rows."
    pub fn sum_exact(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_exact_aggregate_by_slot_terminal(
            field,
            IntentError::raw_limit_before_sum_exact_terminal(),
            SumBySlotTerminal::new,
        )
    }

    /// Explain exact sum routing without executing the terminal.
    pub fn explain_sum_exact(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_exact_aggregate_by_slot_terminal(
            field,
            IntentError::raw_limit_before_sum_exact_terminal(),
            SumBySlotTerminal::new,
        )
    }

    /// Explain scalar `sum_by(field)` routing without executing the terminal.
    pub fn explain_sum_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_slot_terminal(field, SumBySlotTerminal::new)
    }

    /// Execute and return the sum of distinct `field` values.
    pub fn sum_distinct_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, SumDistinctBySlotTerminal::new)
    }

    /// Explain scalar `sum(distinct field)` routing without executing the terminal.
    pub fn explain_sum_distinct_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_slot_terminal(field, SumDistinctBySlotTerminal::new)
    }

    /// Execute and return the average of `field` over matching rows.
    pub fn avg_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, AvgBySlotTerminal::new)
    }

    /// Execute and return the exact average of `field` over matching rows.
    ///
    /// Unlike `avg_by(...)`, this semantic aggregate rejects a prior raw
    /// `limit(...)` so exact averages cannot accidentally mean "average the
    /// first N rows."
    pub fn avg_exact(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_exact_aggregate_by_slot_terminal(
            field,
            IntentError::raw_limit_before_avg_exact_terminal(),
            AvgBySlotTerminal::new,
        )
    }

    /// Explain scalar `avg_by(field)` routing without executing the terminal.
    pub fn explain_avg_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_slot_terminal(field, AvgBySlotTerminal::new)
    }

    /// Explain exact `avg_exact(field)` routing without executing the terminal.
    pub fn explain_avg_exact(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_exact_aggregate_by_slot_terminal(
            field,
            IntentError::raw_limit_before_avg_exact_terminal(),
            AvgBySlotTerminal::new,
        )
    }

    /// Execute and return the average of distinct `field` values.
    pub fn avg_distinct_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, AvgDistinctBySlotTerminal::new)
    }

    /// Explain scalar `avg(distinct field)` routing without executing the terminal.
    pub fn explain_avg_distinct_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_slot_terminal(field, AvgDistinctBySlotTerminal::new)
    }

    /// Execute and return the median id by `field` using deterministic ordering
    /// `(field asc, primary key asc)`.
    ///
    /// Even-length windows select the lower median.
    pub fn median_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, MedianIdBySlotTerminal::new)
    }

    /// Execute and return the number of distinct values for `field` over the
    /// effective result window.
    pub fn count_distinct_by(&self, field: impl AsRef<str>) -> Result<u32, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, CountDistinctBySlotTerminal::new)
    }

    /// Explain `count_distinct_by(field)` routing without executing the terminal.
    pub fn explain_count_distinct_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.explain_slot_terminal(field, CountDistinctBySlotTerminal::new)
    }

    /// Execute and return both `(min_by(field), max_by(field))` in one terminal.
    ///
    /// Tie handling is deterministic for both extrema: primary key ascending.
    pub fn min_max_by(&self, field: impl AsRef<str>) -> Result<MinMaxByIds<E>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, MinMaxIdBySlotTerminal::new)
    }

    /// Execute and return projected field values for the effective result window.
    pub fn values_by(&self, field: impl AsRef<str>) -> Result<Vec<OutputValue>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, ValuesBySlotTerminal::new)
            .map(output_values)
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
        .map(output_values)
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
        .map(output_values)
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
        .map(output_values)
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
        .map(output_values_with_ids)
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
        .map(output_values_with_ids)
    }

    /// Execute and return distinct projected field values for the effective
    /// result window, preserving first-observed value order.
    pub fn distinct_values_by(&self, field: impl AsRef<str>) -> Result<Vec<OutputValue>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, DistinctValuesBySlotTerminal::new)
            .map(output_values)
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
            .map(output_values_with_ids)
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
        .map(output_values_with_ids)
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
            .map(|value| value.map(output))
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
        .map(|value| value.map(output))
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
            .map(|value| value.map(output))
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
        .map(|value| value.map(output))
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

    /// Execute and return the first matching identifier in response order, if any.
    pub fn first(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_terminal(FirstIdTerminal::new())
    }

    /// Explain scalar `first()` routing without executing the terminal.
    pub fn explain_first(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_terminal(&FirstIdTerminal::new())
    }

    /// Execute and return the last matching identifier in response order, if any.
    pub fn last(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_terminal(LastIdTerminal::new())
    }

    /// Explain scalar `last()` routing without executing the terminal.
    pub fn explain_last(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_terminal(&LastIdTerminal::new())
    }

    /// Execute and require exactly one matching row.
    pub fn require_one(&self) -> Result<(), QueryError>
    where
        E: EntityValue,
    {
        self.execute_rows()?.require_one()?;
        Ok(())
    }

    /// Execute and require at least one matching row.
    pub fn require_some(&self) -> Result<(), QueryError>
    where
        E: EntityValue,
    {
        self.execute_rows()?.require_some()?;
        Ok(())
    }
}
