use crate::{
    db::{
        DbSession,
        query::{
            Query, QueryError,
            expr::{FilterExpr, SortExpr},
            plan::{ExecutablePlan, ExplainPlan},
            predicate::Predicate,
        },
        response::Response,
    },
    traits::{CanisterKind, EntityKind, UnitKey},
};

///
/// SessionLoadQuery
///
/// Session-bound load query wrapper.
/// Owns intent construction and execution routing only.
/// All result inspection and projection is performed on `Response<E>`.
///

pub struct SessionLoadQuery<'a, C: CanisterKind, E: EntityKind<Canister = C>> {
    session: &'a DbSession<C>,
    query: Query<E>,
}

impl<'a, C: CanisterKind, E: EntityKind<Canister = C>> SessionLoadQuery<'a, C, E> {
    pub(crate) const fn new(session: &'a DbSession<C>, query: Query<E>) -> Self {
        Self { session, query }
    }

    // ------------------------------------------------------------------
    // Intent inspection
    // ------------------------------------------------------------------

    #[must_use]
    pub const fn query(&self) -> &Query<E> {
        &self.query
    }

    // ------------------------------------------------------------------
    // Intent builders (pure)
    // ------------------------------------------------------------------

    #[must_use]
    pub fn by_key(mut self, key: E::PrimaryKey) -> Self {
        self.query = self.query.by_key(key.into());
        self
    }

    #[must_use]
    pub fn many<I>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = E::PrimaryKey>,
    {
        self.query = self.query.by_keys(keys.into_iter().map(Into::into));
        self
    }

    #[must_use]
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.query = self.query.filter(predicate);
        self
    }

    pub fn filter_expr(mut self, expr: FilterExpr) -> Result<Self, QueryError> {
        self.query = self.query.filter_expr(expr)?;
        Ok(self)
    }

    pub fn sort_expr(mut self, expr: SortExpr) -> Result<Self, QueryError> {
        self.query = self.query.sort_expr(expr)?;
        Ok(self)
    }

    #[must_use]
    pub fn order_by(mut self, field: impl AsRef<str>) -> Self {
        self.query = self.query.order_by(field);
        self
    }

    #[must_use]
    pub fn order_by_desc(mut self, field: impl AsRef<str>) -> Self {
        self.query = self.query.order_by_desc(field);
        self
    }

    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        self.query = self.query.limit(limit);
        self
    }

    #[must_use]
    pub fn offset(mut self, offset: u32) -> Self {
        self.query = self.query.offset(offset);
        self
    }

    // ------------------------------------------------------------------
    // Planning / diagnostics
    // ------------------------------------------------------------------

    pub fn explain(&self) -> Result<ExplainPlan, QueryError> {
        self.query.explain()
    }

    pub fn plan(&self) -> Result<ExecutablePlan<E>, QueryError> {
        self.query.plan()
    }

    // ------------------------------------------------------------------
    // Execution (single semantic boundary)
    // ------------------------------------------------------------------

    /// Execute this query using the session's policy settings.
    pub fn execute(&self) -> Result<Response<E>, QueryError> {
        self.session.execute_query(self.query())
    }

    // ------------------------------------------------------------------
    // Execution terminals — semantic only
    // ------------------------------------------------------------------

    /// Execute and return whether the result set is empty.
    pub fn is_empty(&self) -> Result<bool, QueryError> {
        Ok(self.execute()?.is_empty())
    }

    /// Execute and return the number of matching rows.
    pub fn count(&self) -> Result<u32, QueryError> {
        Ok(self.execute()?.count())
    }

    /// Execute and require exactly one matching row.
    pub fn require_one(&self) -> Result<(), QueryError> {
        self.execute()?.require_one().map_err(QueryError::Response)
    }

    /// Execute and require at least one matching row.
    pub fn require_some(&self) -> Result<(), QueryError> {
        self.execute()?.require_some().map_err(QueryError::Response)
    }
}

impl<C: CanisterKind, E: EntityKind<Canister = C>> SessionLoadQuery<'_, C, E>
where
    E::PrimaryKey: UnitKey,
{
    /// Load the singleton entity identified by the unit primary key `()`.
    #[must_use]
    pub fn only(mut self) -> Self {
        self.query = self.query.only();
        self
    }
}
