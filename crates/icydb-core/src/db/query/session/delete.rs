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
    traits::{CanisterKind, EntityKind, EntityValue, SingletonEntity},
    types::Ref,
};

///
/// SessionDeleteQuery
///
/// Session-bound delete query wrapper.
/// This type owns *intent construction* and *execution routing only*.
/// All result projection and cardinality handling lives on `Response<E>`.
///

pub struct SessionDeleteQuery<'a, C, E>
where
    C: CanisterKind,
    E: EntityKind<Canister = C>,
{
    session: &'a DbSession<C>,
    query: Query<E>,
}

impl<'a, C, E> SessionDeleteQuery<'a, C, E>
where
    C: CanisterKind,
    E: EntityKind<Canister = C>,
{
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
    // Intent builders
    // ------------------------------------------------------------------

    #[must_use]
    pub fn by_key(mut self, key: E::Id) -> Self {
        self.query = self.query.by_key(key);
        self
    }

    /// Set the access path to a typed reference lookup.
    #[must_use]
    pub fn by_ref(mut self, reference: Ref<E>) -> Self {
        self.query = self.query.by_ref(reference);
        self
    }

    /// Set the access path to a batch of typed reference lookups.
    #[must_use]
    pub fn many_refs(self, refs: &[Ref<E>]) -> Self {
        self.many(refs.iter().map(|reference| reference.key()))
    }

    #[must_use]
    pub fn many<I>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = E::Id>,
    {
        self.query = self.query.by_keys(keys);
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
    // Execution (minimal core surface)
    // ------------------------------------------------------------------

    /// Execute this delete using the session's policy settings.
    ///
    /// All result inspection and projection is performed on `Response<E>`.
    pub fn execute(&self) -> Result<Response<E>, QueryError>
    where
        E: EntityValue,
    {
        self.session.execute_query(self.query())
    }

    /// Execute and return whether any rows were affected.
    pub fn is_empty(&self) -> Result<bool, QueryError>
    where
        E: EntityValue,
    {
        Ok(self.execute()?.is_empty())
    }

    /// Execute and return the number of affected rows.
    pub fn count(&self) -> Result<u32, QueryError>
    where
        E: EntityValue,
    {
        Ok(self.execute()?.count())
    }

    /// Execute and require exactly one affected row.
    pub fn require_one(&self) -> Result<(), QueryError>
    where
        E: EntityValue,
    {
        self.execute()?.require_one().map_err(QueryError::Response)
    }

    /// Execute and require at least one affected row.
    pub fn require_some(&self) -> Result<(), QueryError>
    where
        E: EntityValue,
    {
        self.execute()?.require_some().map_err(QueryError::Response)
    }
}

impl<C, E> SessionDeleteQuery<'_, C, E>
where
    C: CanisterKind,
    E: EntityKind<Canister = C> + SingletonEntity,
{
    /// Delete the singleton entity identified by the unit primary key `()`.
    #[must_use]
    pub fn only(mut self, id: E::Id) -> Self {
        self.query = self.query.only(id);
        self
    }
}
