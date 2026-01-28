use crate::{
    db::{
        DbSession,
        query::{
            Query, QueryError, eq,
            plan::{ExecutablePlan, ExplainPlan},
            predicate::Predicate,
        },
        response::Response,
    },
    key::Key,
    traits::{CanisterKind, EntityKind},
};

///
/// SessionLoadQuery
///
/// Fluent, session-bound load query wrapper that keeps intent pure
/// while routing execution through the `DbSession` boundary.
///

pub struct SessionLoadQuery<'a, C: CanisterKind, E: EntityKind<Canister = C>> {
    session: &'a DbSession<C>,
    query: Query<E>,
}

impl<'a, C: CanisterKind, E: EntityKind<Canister = C>> SessionLoadQuery<'a, C, E> {
    pub(crate) const fn new(session: &'a DbSession<C>, query: Query<E>) -> Self {
        Self { session, query }
    }

    /// Return a reference to the underlying intent.
    #[must_use]
    pub const fn query(&self) -> &Query<E> {
        &self.query
    }

    #[must_use]
    pub fn key(mut self, key: Key) -> Self {
        self.query = self.query.filter(eq(E::PRIMARY_KEY, key));
        self
    }

    /// Add a predicate, implicitly AND-ing with any existing predicate.
    #[must_use]
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.query = self.query.filter(predicate);
        self
    }

    /// Append an ascending sort key.
    #[must_use]
    pub fn order_by(mut self, field: &'static str) -> Self {
        self.query = self.query.order_by(field);
        self
    }

    /// Append a descending sort key.
    #[must_use]
    pub fn order_by_desc(mut self, field: &'static str) -> Self {
        self.query = self.query.order_by_desc(field);
        self
    }

    /// Apply a load limit to bound result size.
    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        self.query = self.query.limit(limit);
        self
    }

    /// Apply a load offset.
    #[must_use]
    pub fn offset(mut self, offset: u64) -> Self {
        self.query = self.query.offset(offset);
        self
    }

    /// Explain this query without executing it.
    pub fn explain(&self) -> Result<ExplainPlan, QueryError> {
        self.query.explain()
    }

    /// Plan this query into an executor-ready plan.
    pub fn plan(&self) -> Result<ExecutablePlan<E>, QueryError> {
        self.query.plan()
    }

    /// Execute this query using the session's policy settings.
    pub fn execute(&self) -> Result<Response<E>, QueryError> {
        self.session.execute_query(self.query())
    }

    /// Execute a load query and return all entities.
    pub fn all(&self) -> Result<Vec<E>, QueryError> {
        let response = self.execute()?;

        Ok(response.entities())
    }

    /// Execute a load query and require exactly one entity.
    pub fn one(&self) -> Result<E, QueryError> {
        let response = self.execute()?;

        response.entity().map_err(QueryError::Execute)
    }

    /// Execute a load query and return zero or one entity.
    pub fn one_opt(&self) -> Result<Option<E>, QueryError> {
        let response = self.execute()?;

        response.try_entity().map_err(QueryError::Execute)
    }
}

///
/// SessionDeleteQuery
///
/// Fluent, session-bound delete query wrapper that keeps query intent pure
/// while routing execution through the `DbSession` boundary.
///

pub struct SessionDeleteQuery<'a, C: CanisterKind, E: EntityKind<Canister = C>> {
    session: &'a DbSession<C>,
    query: Query<E>,
}

impl<'a, C: CanisterKind, E: EntityKind<Canister = C>> SessionDeleteQuery<'a, C, E> {
    pub(crate) const fn new(session: &'a DbSession<C>, query: Query<E>) -> Self {
        Self { session, query }
    }

    /// Return a reference to the underlying query.
    #[must_use]
    pub const fn query(&self) -> &Query<E> {
        &self.query
    }

    /// Delete by primary key.
    #[must_use]
    pub fn key(mut self, key: Key) -> Self {
        self.query = self.query.filter(eq(E::PRIMARY_KEY, key));
        self
    }

    /// Add a predicate, implicitly AND-ing with any existing predicate.
    #[must_use]
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.query = self.query.filter(predicate);
        self
    }

    /// Append an ascending sort key.
    #[must_use]
    pub fn order_by(mut self, field: &'static str) -> Self {
        self.query = self.query.order_by(field);
        self
    }

    /// Append a descending sort key.
    #[must_use]
    pub fn order_by_desc(mut self, field: &'static str) -> Self {
        self.query = self.query.order_by_desc(field);
        self
    }

    /// Apply a delete limit to bound mutation size.
    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        self.query = self.query.limit(limit);
        self
    }

    /// Explain this query without executing it.
    pub fn explain(&self) -> Result<ExplainPlan, QueryError> {
        self.query.explain()
    }

    /// Plan this query into an executor-ready plan.
    pub fn plan(&self) -> Result<ExecutablePlan<E>, QueryError> {
        self.query.plan()
    }

    /// Execute this query using the session's policy settings.
    pub fn execute(&self) -> Result<Response<E>, QueryError> {
        self.session.execute_query(self.query())
    }

    /// Execute a delete query and return the deleted rows.
    pub fn delete_rows(&self) -> Result<Response<E>, QueryError> {
        self.execute()
    }
}
