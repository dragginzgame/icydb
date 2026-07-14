//! Module: query::fluent::load::terminals::support
//! Responsibility: shared private terminal gates and handoff helpers.
//! Does not own: public terminal method list or descriptor-driver mappings.
//! Boundary: keeps terminal readiness/admission/read-intent checks in one place.

use std::num::NonZeroU32;

#[cfg(feature = "diagnostics")]
use crate::db::FluentTerminalExecutionAttribution;
#[cfg(feature = "diagnostics")]
use crate::db::query::builder::CountRowsTerminal;
use crate::db::query::read_intent::ReadIntentKind;
use crate::{
    db::{
        DbSession, EntityResponse, PersistedRow, Query,
        query::{
            admission::QueryAdmissionPolicy,
            builder::ExistsRowsTerminal,
            explain::{ExplainAggregateTerminalPlan, ExplainExecutionNodeDescriptor},
            fluent::load::FluentLoadQuery,
            intent::{IntentError, QueryError},
            plan::FieldSlot,
            read_intent::{COMPLETE_SMALL_EXECUTION_LIMIT, COMPLETE_SMALL_MAX_ROWS},
        },
    },
    entity::EntityValue,
};

use super::driver::TerminalStrategyDriver;

fn non_zero_u32(value: u32) -> Result<NonZeroU32, QueryError> {
    NonZeroU32::new(value).ok_or_else(QueryError::invariant)
}

pub(super) fn collect_complete_entities<E>(
    response: EntityResponse<E>,
) -> Result<Vec<E>, QueryError>
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
    pub(super) fn with_admitted_non_paged<T>(
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
    pub(super) fn with_non_paged<T>(
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
    pub(super) fn explain_execution_descriptor(
        &self,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.with_non_paged(DbSession::explain_query_execution_with_visible_indexes)
    }

    // Render one descriptor-derived execution surface so text/json explain
    // terminals do not each forward the same session explain call ad hoc.
    pub(super) fn render_execution_descriptor(
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
    pub(super) fn execute_terminal<S>(&self, strategy: S) -> Result<S::Output, QueryError>
    where
        E: EntityValue,
        S: TerminalStrategyDriver<E>,
    {
        self.with_admitted_non_paged(|session, query| strategy.execute(session, query))
    }

    // Explain one prepared terminal strategy through the same non-paged fluent
    // policy gate used by execution.
    pub(super) fn explain_terminal<S>(&self, strategy: &S) -> Result<S::ExplainOutput, QueryError>
    where
        E: EntityValue,
        S: TerminalStrategyDriver<E>,
    {
        self.with_non_paged(|session, query| strategy.explain(session, query))
    }

    pub(super) fn execute_slot_terminal<S>(
        &self,
        field: impl AsRef<str>,
        make_strategy: impl FnOnce(FieldSlot) -> S,
    ) -> Result<S::Output, QueryError>
    where
        E: EntityValue,
        S: TerminalStrategyDriver<E>,
    {
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.execute_terminal(make_strategy(target_slot))
    }

    pub(super) fn explain_slot_terminal<S>(
        &self,
        field: impl AsRef<str>,
        make_strategy: impl FnOnce(FieldSlot) -> S,
    ) -> Result<S::ExplainOutput, QueryError>
    where
        E: EntityValue,
        S: TerminalStrategyDriver<E>,
    {
        let target_slot = self.resolve_non_paged_slot(field)?;
        let strategy = make_strategy(target_slot);

        self.explain_terminal(&strategy)
    }

    pub(super) fn with_admitted_non_paged_slot<T>(
        &self,
        field: impl AsRef<str>,
        map: impl FnOnce(&DbSession<E::Canister>, &Query<E>, FieldSlot) -> Result<T, QueryError>,
    ) -> Result<T, QueryError>
    where
        E: EntityValue,
    {
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.with_admitted_non_paged(|session, query| map(session, query, target_slot))
    }

    pub(super) fn with_non_paged_slot<T>(
        &self,
        field: impl AsRef<str>,
        map: impl FnOnce(&DbSession<E::Canister>, &Query<E>, FieldSlot) -> Result<T, QueryError>,
    ) -> Result<T, QueryError>
    where
        E: EntityValue,
    {
        let target_slot = self.resolve_non_paged_slot(field)?;

        self.with_non_paged(|session, query| map(session, query, target_slot))
    }

    pub(super) fn execute_existence_terminal(&self) -> Result<bool, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_exists_intent_owns_limit()?;

        self.execute_terminal(ExistsRowsTerminal::new())
    }

    pub(super) fn explain_existence_terminal(
        &self,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_exists_intent_owns_limit()?;

        self.explain_terminal(&ExistsRowsTerminal::new())
            .map(|plan| plan.with_read_intent(ReadIntentKind::ExistenceCheck))
    }

    #[cfg(feature = "diagnostics")]
    pub(super) fn execute_existence_terminal_with_attribution(
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

    pub(super) fn explain_checked_exact_aggregate_terminal<S>(
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

    pub(super) fn explain_exact_aggregate_terminal<S>(
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

    pub(super) fn execute_exact_aggregate_terminal<S>(
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

    pub(super) fn execute_exact_aggregate_by_slot_terminal<S>(
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

    pub(super) fn explain_exact_aggregate_by_slot_terminal<S>(
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

    pub(super) fn resolve_exact_aggregate_slot(
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
    pub(super) fn with_complete_small_query<T>(
        &self,
        map: impl FnOnce(&DbSession<E::Canister>, &Query<E>) -> Result<T, QueryError>,
    ) -> Result<T, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_collect_complete_intent_owns_limit()?;
        self.ensure_non_paged_mode_ready()?;

        let query = self.query().with_load_limit(COMPLETE_SMALL_EXECUTION_LIMIT);
        let policy =
            QueryAdmissionPolicy::public_read(non_zero_u32(COMPLETE_SMALL_EXECUTION_LIMIT)?);

        self.session
            .ensure_query_read_admission_policy(&query, &policy)?;

        map(self.session, &query)
    }

    #[cfg(feature = "diagnostics")]
    pub(super) fn execute_count_terminal_with_attribution(
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

    pub(super) fn ensure_exists_intent_owns_limit(&self) -> Result<(), QueryError> {
        self.ensure_semantic_terminal_owns_limit(IntentError::raw_limit_before_exists_terminal())
    }

    pub(super) fn ensure_exact_aggregate_intent_owns_limit(
        &self,
        err: IntentError,
    ) -> Result<(), QueryError> {
        self.ensure_semantic_terminal_owns_limit(err)
    }

    pub(super) fn ensure_collect_complete_intent_owns_limit(&self) -> Result<(), QueryError> {
        self.ensure_semantic_terminal_owns_limit(
            IntentError::raw_limit_before_collect_complete_terminal(),
        )
    }
}
