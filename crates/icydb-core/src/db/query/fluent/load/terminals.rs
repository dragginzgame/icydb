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

use std::num::NonZeroU32;

#[cfg(feature = "diagnostics")]
use crate::db::FluentTerminalExecutionAttribution;
#[cfg(feature = "diagnostics")]
use crate::db::QueryExecutionAttribution;
use crate::db::query::read_intent::ReadIntentKind;
use crate::{
    db::{
        DbSession, PersistedRow, Query,
        query::{
            admission::QueryAdmissionPolicy,
            api::ResponseCardinalityExt,
            builder::{
                AvgBySlotTerminal, AvgDistinctBySlotTerminal, CountDistinctBySlotTerminal,
                CountRowsTerminal, DistinctValuesBySlotTerminal, ExistsRowsTerminal,
                FirstIdTerminal, FirstValueBySlotTerminal, LastIdTerminal, LastValueBySlotTerminal,
                MaxIdBySlotTerminal, MaxIdTerminal, MedianIdBySlotTerminal, MinIdBySlotTerminal,
                MinIdTerminal, MinMaxIdBySlotTerminal, NthIdBySlotTerminal, SumBySlotTerminal,
                SumDistinctBySlotTerminal, ValueProjectionExpr, ValuesBySlotTerminal,
                ValuesBySlotWithIdsTerminal,
            },
            explain::{ExplainAggregateTerminalPlan, ExplainExecutionNodeDescriptor},
            fluent::load::{FluentLoadQuery, LoadQueryResult},
            intent::{IntentError, QueryError},
            plan::{AggregateKind, FieldSlot},
            read_intent::{
                COMPLETE_SMALL_EXECUTION_LIMIT, COMPLETE_SMALL_MAX_ROWS,
                PUBLIC_PAGE_MAX_RESPONSE_BYTES,
            },
        },
        response::EntityResponse,
    },
    traits::EntityValue,
    types::{Decimal, Id},
    value::{OutputValue, Value},
};

type MinMaxByIds<E> = Option<(Id<E>, Id<E>)>;

///
/// TerminalStrategyDriver
///
/// TerminalStrategyDriver is the fluent terminal wiring adapter between a
/// query-owned strategy object and the session-owned execution/explain
/// boundary. Implementations are deliberately thin: they only choose the
/// matching `DbSession` method for an existing strategy type.
///

trait TerminalStrategyDriver<E: PersistedRow + EntityValue> {
    type Output;
    type ExplainOutput;

    fn execute(
        self,
        session: &DbSession<E::Canister>,
        query: &Query<E>,
    ) -> Result<Self::Output, QueryError>;

    fn explain(
        &self,
        session: &DbSession<E::Canister>,
        query: &Query<E>,
    ) -> Result<Self::ExplainOutput, QueryError>;
}

// Define one aggregate-style terminal driver implementation. The macro keeps
// terminal descriptors 1:1 with session methods while removing repeated explain
// wiring that is identical for every aggregate terminal.
macro_rules! impl_aggregate_terminal_driver {
    ($terminal:ty, $output:ty, $execute:ident) => {
        impl<E> TerminalStrategyDriver<E> for $terminal
        where
            E: PersistedRow + EntityValue,
        {
            type Output = $output;
            type ExplainOutput = ExplainAggregateTerminalPlan;

            fn execute(
                self,
                session: &DbSession<E::Canister>,
                query: &Query<E>,
            ) -> Result<Self::Output, QueryError> {
                session.$execute(query, self)
            }

            fn explain(
                &self,
                session: &DbSession<E::Canister>,
                query: &Query<E>,
            ) -> Result<Self::ExplainOutput, QueryError> {
                session.explain_query_prepared_aggregate_terminal_with_visible_indexes(query, self)
            }
        }
    };
}

// Define one projection-style terminal driver implementation. Projection
// terminals share the execution/explain shape but return execution descriptors
// rather than aggregate-terminal plans.
macro_rules! impl_projection_terminal_driver {
    ($terminal:ty, $output:ty, $execute:ident) => {
        impl<E> TerminalStrategyDriver<E> for $terminal
        where
            E: PersistedRow + EntityValue,
        {
            type Output = $output;
            type ExplainOutput = ExplainExecutionNodeDescriptor;

            fn execute(
                self,
                session: &DbSession<E::Canister>,
                query: &Query<E>,
            ) -> Result<Self::Output, QueryError> {
                session.$execute(query, self)
            }

            fn explain(
                &self,
                session: &DbSession<E::Canister>,
                query: &Query<E>,
            ) -> Result<Self::ExplainOutput, QueryError> {
                session.explain_query_prepared_projection_terminal_with_visible_indexes(query, self)
            }
        }
    };
}

impl_aggregate_terminal_driver!(CountRowsTerminal, u32, execute_fluent_count_rows_terminal);
impl_aggregate_terminal_driver!(
    ExistsRowsTerminal,
    bool,
    execute_fluent_exists_rows_terminal
);
impl_aggregate_terminal_driver!(MinIdTerminal, Option<Id<E>>, execute_fluent_min_id_terminal);
impl_aggregate_terminal_driver!(MaxIdTerminal, Option<Id<E>>, execute_fluent_max_id_terminal);
impl_aggregate_terminal_driver!(
    MinIdBySlotTerminal,
    Option<Id<E>>,
    execute_fluent_min_id_by_slot
);
impl_aggregate_terminal_driver!(
    MaxIdBySlotTerminal,
    Option<Id<E>>,
    execute_fluent_max_id_by_slot
);
impl_aggregate_terminal_driver!(
    SumBySlotTerminal,
    Option<Decimal>,
    execute_fluent_sum_by_slot
);
impl_aggregate_terminal_driver!(
    SumDistinctBySlotTerminal,
    Option<Decimal>,
    execute_fluent_sum_distinct_by_slot
);
impl_aggregate_terminal_driver!(
    AvgBySlotTerminal,
    Option<Decimal>,
    execute_fluent_avg_by_slot
);
impl_aggregate_terminal_driver!(
    AvgDistinctBySlotTerminal,
    Option<Decimal>,
    execute_fluent_avg_distinct_by_slot
);
impl_aggregate_terminal_driver!(
    FirstIdTerminal,
    Option<Id<E>>,
    execute_fluent_first_id_terminal
);
impl_aggregate_terminal_driver!(
    LastIdTerminal,
    Option<Id<E>>,
    execute_fluent_last_id_terminal
);
impl_aggregate_terminal_driver!(
    NthIdBySlotTerminal,
    Option<Id<E>>,
    execute_fluent_nth_id_by_slot
);
impl_aggregate_terminal_driver!(
    MedianIdBySlotTerminal,
    Option<Id<E>>,
    execute_fluent_median_id_by_slot
);
impl_aggregate_terminal_driver!(
    MinMaxIdBySlotTerminal,
    MinMaxByIds<E>,
    execute_fluent_min_max_id_by_slot
);

impl_projection_terminal_driver!(
    ValuesBySlotTerminal,
    Vec<Value>,
    execute_fluent_values_by_slot
);
impl_projection_terminal_driver!(
    DistinctValuesBySlotTerminal,
    Vec<Value>,
    execute_fluent_distinct_values_by_slot
);
impl_projection_terminal_driver!(
    CountDistinctBySlotTerminal,
    u32,
    execute_fluent_count_distinct_by_slot
);
impl_projection_terminal_driver!(
    ValuesBySlotWithIdsTerminal,
    Vec<(Id<E>, Value)>,
    execute_fluent_values_by_with_ids_slot
);
impl_projection_terminal_driver!(
    FirstValueBySlotTerminal,
    Option<Value>,
    execute_fluent_first_value_by_slot
);
impl_projection_terminal_driver!(
    LastValueBySlotTerminal,
    Option<Value>,
    execute_fluent_last_value_by_slot
);

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

fn non_zero_u32(value: u32) -> Result<NonZeroU32, QueryError> {
    NonZeroU32::new(value).ok_or_else(QueryError::invariant)
}

fn collect_complete_entities<E>(response: EntityResponse<E>) -> Result<Vec<E>, QueryError>
where
    E: PersistedRow,
{
    if response.count() > COMPLETE_SMALL_MAX_ROWS {
        return Err(QueryError::intent(
            IntentError::complete_read_too_many_rows(),
        ));
    }

    Ok(response.entities())
}

#[cfg(feature = "diagnostics")]
fn with_fluent_terminal_read_intent<T>(
    result: Result<(T, FluentTerminalExecutionAttribution), QueryError>,
    read_intent: ReadIntentKind,
) -> Result<(T, FluentTerminalExecutionAttribution), QueryError> {
    let (value, attribution) = result?;

    Ok((value, attribution.with_read_intent(read_intent)))
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
        self.with_admitted_non_paged(DbSession::execute_query_result)
    }

    /// Execute this query through the scalar rows-only session boundary.
    pub fn execute_rows(&self) -> Result<EntityResponse<E>, QueryError>
    where
        E: EntityValue,
    {
        self.with_admitted_non_paged(DbSession::execute_scalar_query_rows)
    }

    fn with_admitted_non_paged<T>(
        &self,
        map: impl FnOnce(&DbSession<E::Canister>, &Query<E>) -> Result<T, QueryError>,
    ) -> Result<T, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;
        self.ensure_default_read_admission()?;
        map(self.session, self.query())
    }

    // Run one terminal operation through the canonical non-paged fluent policy
    // gate so execution and explain helpers cannot drift on readiness checks.
    fn with_non_paged<T>(
        &self,
        map: impl FnOnce(&DbSession<E::Canister>, &Query<E>) -> Result<T, QueryError>,
    ) -> Result<T, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;
        map(self.session, self.query())
    }

    // Resolve the structural execution descriptor for this fluent load query
    // through the session-owned visible-index explain path once.
    fn explain_execution_descriptor(&self) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged(DbSession::explain_query_execution_with_visible_indexes)
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

    // Execute one prepared terminal descriptor through the canonical
    // non-paged fluent policy gate.
    fn execute_terminal<S>(&self, strategy: S) -> Result<S::Output, QueryError>
    where
        E: EntityValue,
        S: TerminalStrategyDriver<E>,
    {
        self.with_admitted_non_paged(|session, query| strategy.execute(session, query))
    }

    // Explain one prepared terminal strategy through the same non-paged fluent
    // policy gate used by execution.
    fn explain_terminal<S>(&self, strategy: &S) -> Result<S::ExplainOutput, QueryError>
    where
        E: EntityValue,
        S: TerminalStrategyDriver<E>,
    {
        self.with_non_paged(|session, query| strategy.explain(session, query))
    }

    fn execute_existence_terminal(&self) -> Result<bool, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_exists_intent_owns_limit()?;

        self.execute_terminal(ExistsRowsTerminal::new())
    }

    fn explain_existence_terminal(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_exists_intent_owns_limit()?;

        self.explain_terminal(&ExistsRowsTerminal::new())
            .map(|plan| plan.with_read_intent(ReadIntentKind::ExistenceCheck))
    }

    #[cfg(feature = "diagnostics")]
    fn execute_existence_terminal_with_attribution(
        &self,
    ) -> Result<(bool, FluentTerminalExecutionAttribution), QueryError>
    where
        E: EntityValue,
    {
        self.ensure_exists_intent_owns_limit()?;

        with_fluent_terminal_read_intent(
            self.with_admitted_non_paged(|session, query| {
                session.execute_fluent_exists_rows_terminal_with_attribution(
                    query,
                    ExistsRowsTerminal::new(),
                )
            }),
            ReadIntentKind::ExistenceCheck,
        )
    }

    fn explain_checked_exact_aggregate_terminal<S>(
        &self,
        err: IntentError,
        strategy: &S,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
        S: TerminalStrategyDriver<E, ExplainOutput = ExplainAggregateTerminalPlan>,
    {
        self.ensure_exact_aggregate_intent_owns_limit(err)?;

        self.explain_exact_aggregate_terminal(strategy)
    }

    fn explain_exact_aggregate_terminal<S>(
        &self,
        strategy: &S,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
        S: TerminalStrategyDriver<E, ExplainOutput = ExplainAggregateTerminalPlan>,
    {
        self.explain_terminal(strategy)
            .map(|plan| plan.with_read_intent(ReadIntentKind::ExactAggregate))
    }

    fn execute_exact_aggregate_terminal<S>(
        &self,
        err: IntentError,
        strategy: S,
    ) -> Result<S::Output, QueryError>
    where
        E: EntityValue,
        S: TerminalStrategyDriver<E>,
    {
        self.ensure_exact_aggregate_intent_owns_limit(err)?;

        self.execute_terminal(strategy)
    }

    fn execute_exact_aggregate_by_slot_terminal<S>(
        &self,
        field: impl AsRef<str>,
        err: IntentError,
        make_strategy: impl FnOnce(FieldSlot) -> S,
    ) -> Result<S::Output, QueryError>
    where
        E: EntityValue,
        S: TerminalStrategyDriver<E>,
    {
        let target_slot = self.resolve_exact_aggregate_slot(field, err)?;

        self.execute_terminal(make_strategy(target_slot))
    }

    fn explain_exact_aggregate_by_slot_terminal<S>(
        &self,
        field: impl AsRef<str>,
        err: IntentError,
        make_strategy: impl FnOnce(FieldSlot) -> S,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
        S: TerminalStrategyDriver<E, ExplainOutput = ExplainAggregateTerminalPlan>,
    {
        let target_slot = self.resolve_exact_aggregate_slot(field, err)?;
        let strategy = make_strategy(target_slot);

        self.explain_exact_aggregate_terminal(&strategy)
    }

    fn resolve_exact_aggregate_slot(
        &self,
        field: impl AsRef<str>,
        err: IntentError,
    ) -> Result<FieldSlot, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_exact_aggregate_intent_owns_limit(err)?;

        self.resolve_non_paged_slot(field)
    }

    // Run one complete-small-set operation through the terminal-owned
    // lookahead limit and public-read policy. Both result and attribution
    // paths share this handoff so the terminal cannot drift on admission
    // bounds.
    fn with_complete_small_query<T>(
        &self,
        map: impl FnOnce(&DbSession<E::Canister>, &Query<E>) -> Result<T, QueryError>,
    ) -> Result<T, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_collect_complete_intent_owns_limit()?;
        self.ensure_non_paged_mode_ready()?;

        let query = self.query().with_load_limit(COMPLETE_SMALL_EXECUTION_LIMIT);
        let policy = QueryAdmissionPolicy::public_read(
            non_zero_u32(COMPLETE_SMALL_EXECUTION_LIMIT)?,
            non_zero_u32(PUBLIC_PAGE_MAX_RESPONSE_BYTES)?,
        );

        self.session
            .ensure_query_read_admission_policy(&query, &policy)?;

        map(self.session, &query)
    }

    #[cfg(feature = "diagnostics")]
    fn execute_count_terminal_with_attribution(
        &self,
        read_intent: ReadIntentKind,
    ) -> Result<(u32, FluentTerminalExecutionAttribution), QueryError>
    where
        E: EntityValue,
    {
        with_fluent_terminal_read_intent(
            self.with_admitted_non_paged(|session, query| {
                session.execute_fluent_count_rows_terminal_with_attribution(
                    query,
                    CountRowsTerminal::new(),
                )
            }),
            read_intent,
        )
    }

    fn ensure_exists_intent_owns_limit(&self) -> Result<(), QueryError> {
        self.ensure_semantic_terminal_owns_limit(IntentError::raw_limit_before_exists_terminal())
    }

    fn ensure_exact_aggregate_intent_owns_limit(&self, err: IntentError) -> Result<(), QueryError> {
        self.ensure_semantic_terminal_owns_limit(err)
    }

    fn ensure_collect_complete_intent_owns_limit(&self) -> Result<(), QueryError> {
        self.ensure_semantic_terminal_owns_limit(
            IntentError::raw_limit_before_collect_complete_terminal(),
        )
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
        self.execute_existence_terminal()
    }

    /// Execute and return whether at least one matching row exists with
    /// terminal attribution.
    #[cfg(feature = "diagnostics")]
    #[doc(hidden)]
    pub fn exists_with_attribution(
        &self,
    ) -> Result<(bool, FluentTerminalExecutionAttribution), QueryError>
    where
        E: EntityValue,
    {
        self.execute_existence_terminal_with_attribution()
    }

    /// Explain scalar `exists()` routing without executing the terminal.
    pub fn explain_exists(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_existence_terminal()
    }

    /// Execute and return all matching rows if the complete result fits in
    /// the default public-read small-set cap.
    ///
    /// This semantic terminal rejects a prior row-window cap. It owns an
    /// internal lookahead limit so it can distinguish a complete small set
    /// from a silently truncated row window.
    pub fn collect_complete(&self) -> Result<Vec<E>, QueryError>
    where
        E: EntityValue,
    {
        let response = self.with_complete_small_query(DbSession::execute_scalar_query_rows)?;

        collect_complete_entities(response)
    }

    /// Execute and return all matching rows with query diagnostics attribution
    /// if the complete result fits in the default public-read small-set cap.
    #[cfg(feature = "diagnostics")]
    #[doc(hidden)]
    pub fn collect_complete_with_attribution(
        &self,
    ) -> Result<(Vec<E>, QueryExecutionAttribution), QueryError>
    where
        E: EntityValue,
    {
        let (result, attribution) =
            self.with_complete_small_query(DbSession::execute_query_result_with_attribution)?;
        let response = result.into_rows()?;
        let entities = collect_complete_entities(response)?;

        Ok((
            entities,
            attribution.with_read_intent(ReadIntentKind::CompleteSmallSet),
        ))
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
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.with_admitted_non_paged(|session, query| {
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
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.with_non_paged(|session, query| {
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
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.execute_terminal(MinIdBySlotTerminal::new(target_slot))
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
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.execute_terminal(MaxIdBySlotTerminal::new(target_slot))
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
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.execute_terminal(NthIdBySlotTerminal::new(target_slot, nth))
    }

    /// Execute and return the sum of `field` over matching rows.
    pub fn sum_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.execute_terminal(SumBySlotTerminal::new(target_slot))
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
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.explain_terminal(&SumBySlotTerminal::new(target_slot))
    }

    /// Execute and return the sum of distinct `field` values.
    pub fn sum_distinct_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.execute_terminal(SumDistinctBySlotTerminal::new(target_slot))
    }

    /// Explain scalar `sum(distinct field)` routing without executing the terminal.
    pub fn explain_sum_distinct_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.explain_terminal(&SumDistinctBySlotTerminal::new(target_slot))
    }

    /// Execute and return the average of `field` over matching rows.
    pub fn avg_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.execute_terminal(AvgBySlotTerminal::new(target_slot))
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
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.explain_terminal(&AvgBySlotTerminal::new(target_slot))
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
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.execute_terminal(AvgDistinctBySlotTerminal::new(target_slot))
    }

    /// Explain scalar `avg(distinct field)` routing without executing the terminal.
    pub fn explain_avg_distinct_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.explain_terminal(&AvgDistinctBySlotTerminal::new(target_slot))
    }

    /// Execute and return the median id by `field` using deterministic ordering
    /// `(field asc, primary key asc)`.
    ///
    /// Even-length windows select the lower median.
    pub fn median_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.execute_terminal(MedianIdBySlotTerminal::new(target_slot))
    }

    /// Execute and return the number of distinct values for `field` over the
    /// effective result window.
    pub fn count_distinct_by(&self, field: impl AsRef<str>) -> Result<u32, QueryError>
    where
        E: EntityValue,
    {
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.execute_terminal(CountDistinctBySlotTerminal::new(target_slot))
    }

    /// Explain `count_distinct_by(field)` routing without executing the terminal.
    pub fn explain_count_distinct_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.explain_terminal(&CountDistinctBySlotTerminal::new(target_slot))
    }

    /// Execute and return both `(min_by(field), max_by(field))` in one terminal.
    ///
    /// Tie handling is deterministic for both extrema: primary key ascending.
    pub fn min_max_by(&self, field: impl AsRef<str>) -> Result<MinMaxByIds<E>, QueryError>
    where
        E: EntityValue,
    {
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.execute_terminal(MinMaxIdBySlotTerminal::new(target_slot))
    }

    /// Execute and return projected field values for the effective result window.
    pub fn values_by(&self, field: impl AsRef<str>) -> Result<Vec<OutputValue>, QueryError>
    where
        E: EntityValue,
    {
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.execute_terminal(ValuesBySlotTerminal::new(target_slot))
            .map(output_values)
    }

    /// Execute and return projected values for one shared bounded projection
    /// over the effective response window.
    pub fn project_values<P>(&self, projection: &P) -> Result<Vec<OutputValue>, QueryError>
    where
        E: EntityValue,
        P: ValueProjectionExpr,
    {
        let target_slot = self.resolve_non_paged_slot(projection.field())?;

        self.with_admitted_non_paged(|session, query| {
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
        let target_slot = self.resolve_non_paged_slot(projection.field())?;

        self.explain_terminal(&ValuesBySlotTerminal::new(target_slot))
    }

    /// Explain `values_by(field)` routing without executing the terminal.
    pub fn explain_values_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.explain_terminal(&ValuesBySlotTerminal::new(target_slot))
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
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.with_admitted_non_paged(|session, query| {
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
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.with_admitted_non_paged(|session, query| {
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
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.with_admitted_non_paged(|session, query| {
            session
                .execute_fluent_top_k_values_by_slot(query, target_slot, take_count)
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
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.with_admitted_non_paged(|session, query| {
            session
                .execute_fluent_bottom_k_values_by_slot(query, target_slot, take_count)
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
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.with_admitted_non_paged(|session, query| {
            session
                .execute_fluent_top_k_values_with_ids_by_slot(query, target_slot, take_count)
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
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.with_admitted_non_paged(|session, query| {
            session
                .execute_fluent_bottom_k_values_with_ids_by_slot(query, target_slot, take_count)
                .map(output_values_with_ids)
        })
    }

    /// Execute and return distinct projected field values for the effective
    /// result window, preserving first-observed value order.
    pub fn distinct_values_by(&self, field: impl AsRef<str>) -> Result<Vec<OutputValue>, QueryError>
    where
        E: EntityValue,
    {
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.execute_terminal(DistinctValuesBySlotTerminal::new(target_slot))
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
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.explain_terminal(&DistinctValuesBySlotTerminal::new(target_slot))
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
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.execute_terminal(ValuesBySlotWithIdsTerminal::new(target_slot))
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
        let target_slot = self.resolve_non_paged_slot(projection.field())?;

        self.with_admitted_non_paged(|session, query| {
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
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.explain_terminal(&ValuesBySlotWithIdsTerminal::new(target_slot))
    }

    /// Execute and return the first projected field value in effective response
    /// order, if any.
    pub fn first_value_by(&self, field: impl AsRef<str>) -> Result<Option<OutputValue>, QueryError>
    where
        E: EntityValue,
    {
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.execute_terminal(FirstValueBySlotTerminal::new(target_slot))
            .map(|value| value.map(output))
    }

    /// Execute and return the first projected value for one shared bounded
    /// projection in effective response order, if any.
    pub fn project_first_value<P>(&self, projection: &P) -> Result<Option<OutputValue>, QueryError>
    where
        E: EntityValue,
        P: ValueProjectionExpr,
    {
        let target_slot = self.resolve_non_paged_slot(projection.field())?;

        self.with_admitted_non_paged(|session, query| {
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
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.explain_terminal(&FirstValueBySlotTerminal::new(target_slot))
    }

    /// Execute and return the last projected field value in effective response
    /// order, if any.
    pub fn last_value_by(&self, field: impl AsRef<str>) -> Result<Option<OutputValue>, QueryError>
    where
        E: EntityValue,
    {
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.execute_terminal(LastValueBySlotTerminal::new(target_slot))
            .map(|value| value.map(output))
    }

    /// Execute and return the last projected value for one shared bounded
    /// projection in effective response order, if any.
    pub fn project_last_value<P>(&self, projection: &P) -> Result<Option<OutputValue>, QueryError>
    where
        E: EntityValue,
        P: ValueProjectionExpr,
    {
        let target_slot = self.resolve_non_paged_slot(projection.field())?;

        self.with_admitted_non_paged(|session, query| {
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
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.explain_terminal(&LastValueBySlotTerminal::new(target_slot))
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
