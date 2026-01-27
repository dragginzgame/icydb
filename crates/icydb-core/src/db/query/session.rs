use crate::{
    db::{
        DbSession,
        query::{
            IntentError, Query, QueryError,
            plan::{ExecutablePlan, ExplainPlan},
            predicate::Predicate,
        },
        response::Response,
    },
    traits::{CanisterKind, EntityKind},
};

///
/// SessionQuery
///
/// Fluent, session-bound query wrapper that keeps intent pure
/// while routing execution through the `DbSession` boundary.
///

pub struct SessionQuery<'a, C: CanisterKind, E: EntityKind<Canister = C>> {
    session: &'a DbSession<C>,
    intent: Query<E>,
    intent_error: Option<IntentError>,
}

impl<'a, C: CanisterKind, E: EntityKind<Canister = C>> SessionQuery<'a, C, E> {
    pub(crate) const fn new(session: &'a DbSession<C>, intent: Query<E>) -> Self {
        Self {
            session,
            intent,
            intent_error: None,
        }
    }

    /// Return a reference to the underlying intent.
    pub const fn intent(&self) -> Result<&Query<E>, QueryError> {
        self.checked_intent()
    }

    /// Add a predicate, implicitly AND-ing with any existing predicate.
    #[must_use]
    pub fn filter(mut self, predicate: Predicate) -> Self {
        if self.intent_error.is_none() {
            self.intent = self.intent.filter(predicate);
        }
        self
    }

    /// Append an ascending sort key.
    #[must_use]
    pub fn order_by(mut self, field: &'static str) -> Self {
        if self.intent_error.is_none() {
            self.intent = self.intent.order_by(field);
        }
        self
    }

    /// Append a descending sort key.
    #[must_use]
    pub fn order_by_desc(mut self, field: &'static str) -> Self {
        if self.intent_error.is_none() {
            self.intent = self.intent.order_by_desc(field);
        }
        self
    }

    /// Apply a limit to the current mode.
    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        if self.intent_error.is_none() {
            self.intent = self.intent.limit(limit);
        }
        self
    }

    /// Apply a load offset to the current intent.
    #[must_use]
    pub fn offset(mut self, offset: u64) -> Self {
        if self.intent_error.is_none() {
            if self.intent.mode().is_load() {
                self.intent = self.intent.offset(offset);
            } else {
                self.intent_error = Some(IntentError::OffsetOnDelete);
            }
        }
        self
    }

    /// Explain this intent without executing it.
    pub fn explain(&self) -> Result<ExplainPlan, QueryError> {
        self.checked_intent()?.explain()
    }

    /// Plan this intent into an executor-ready plan.
    pub fn plan(&self) -> Result<ExecutablePlan<E>, QueryError> {
        self.checked_intent()?.plan()
    }

    /// Execute this intent using the session's policy settings.
    pub fn execute(&self) -> Result<Response<E>, QueryError> {
        self.session.execute_query(self.checked_intent()?)
    }

    /// Execute a load intent and return all entities.
    pub fn all(&self) -> Result<Vec<E>, QueryError> {
        let response = self.execute_load()?;
        Ok(response.entities())
    }

    /// Execute a load intent and require exactly one entity.
    pub fn one(&self) -> Result<E, QueryError> {
        let response = self.execute_load()?;
        response.entity().map_err(QueryError::Execute)
    }

    /// Execute a load intent and return zero or one entity.
    pub fn one_opt(&self) -> Result<Option<E>, QueryError> {
        let response = self.execute_load()?;
        response.try_entity().map_err(QueryError::Execute)
    }

    /// Execute a delete intent and return the deleted rows.
    pub fn delete_rows(&self) -> Result<Response<E>, QueryError> {
        self.ensure_delete_intent()?;
        self.session.execute_query(self.checked_intent()?)
    }

    // Guard that load-only helpers are not used on delete intents.
    fn ensure_load_intent(&self) -> Result<(), QueryError> {
        if self.checked_intent()?.mode().is_load() {
            Ok(())
        } else {
            Err(QueryError::from(IntentError::ExecuteLoadOnDelete))
        }
    }

    // Guard that delete-only helpers are not used on load intents.
    fn ensure_delete_intent(&self) -> Result<(), QueryError> {
        if self.checked_intent()?.mode().is_delete() {
            Ok(())
        } else {
            Err(QueryError::from(IntentError::ExecuteDeleteOnLoad))
        }
    }

    // Execute a load intent and surface response-level errors as query errors.
    fn execute_load(&self) -> Result<Response<E>, QueryError> {
        self.ensure_load_intent()?;
        self.session.execute_query(self.checked_intent()?)
    }

    // Surface any builder-level intent errors before planning or execution.
    const fn checked_intent(&self) -> Result<&Query<E>, QueryError> {
        if let Some(err) = self.intent_error {
            return Err(QueryError::Intent(err));
        }

        Ok(&self.intent)
    }
}
