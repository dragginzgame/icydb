//! Module: query::fluent::load::terminals::read_intent
//! Responsibility: semantic read-intent terminals for existence and complete reads.
//! Does not own: aggregate terminals, materialization terminals, or executor routing.
//! Boundary: delegates shared readiness/admission gates to `support`.

#[cfg(feature = "diagnostics")]
use crate::db::FluentTerminalExecutionAttribution;
#[cfg(feature = "diagnostics")]
use crate::db::QueryExecutionAttribution;
#[cfg(feature = "diagnostics")]
use crate::db::query::read_intent::ReadIntentKind;
use crate::{
    db::{
        DbSession, PersistedRow,
        query::{
            explain::ExplainAggregateTerminalPlan, fluent::load::FluentLoadQuery,
            intent::QueryError,
        },
    },
    entity::EntityValue,
};

use super::support::collect_complete_entities;

impl<E> FluentLoadQuery<'_, E>
where
    E: PersistedRow,
{
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
    /// This semantic terminal owns an internal lookahead limit so it can distinguish a complete small set
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
}
