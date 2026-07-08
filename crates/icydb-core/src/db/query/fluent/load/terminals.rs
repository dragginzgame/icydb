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

mod aggregate;
mod driver;
mod output;
mod projection;
mod read_intent;
mod support;

#[cfg(feature = "diagnostics")]
use crate::db::QueryExecutionAttribution;
use crate::{
    db::{
        DbSession, PersistedRow,
        query::{
            api::ResponseCardinalityExt,
            builder::{FirstIdTerminal, LastIdTerminal},
            explain::{ExplainAggregateTerminalPlan, ExplainExecutionNodeDescriptor},
            fluent::load::{FluentLoadQuery, LoadQueryResult},
            intent::QueryError,
        },
        response::EntityResponse,
    },
    traits::EntityValue,
    types::Id,
};

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
