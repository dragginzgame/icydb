use crate::{
    db::{
        DbSession,
        query::{
            Query, QueryError,
            expr::{FilterExpr, SortExpr},
            plan::{ExecutablePlan, ExplainPlan},
            predicate::Predicate,
        },
        response::{Response, Row},
    },
    key::Key,
    traits::{CanisterKind, EntityKind},
    view::View,
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

    // ==================================================================
    // Intent inspection
    // ==================================================================

    /// Return a reference to the underlying query intent.
    #[must_use]
    pub const fn query(&self) -> &Query<E> {
        &self.query
    }

    // ==================================================================
    // Intent builders (pure, no execution)
    // ==================================================================

    /// Filter by primary key.
    #[must_use]
    pub fn by_key(mut self, key: impl Into<Key>) -> Self {
        self.query = self.query.by_key(key.into());
        self
    }

    /// Load multiple entities by primary key.
    ///
    /// Semantics:
    /// - Equivalent to `WHERE pk IN (…)`
    /// - Uses key-based access (ByKey / ByKeys)
    /// - Missing keys are ignored in MissingOk mode
    /// - Strict mode treats missing rows as corruption
    #[must_use]
    pub fn many<I>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = E::PrimaryKey>,
    {
        self.query = self.query.by_keys(keys.into_iter().map(Into::into));
        self
    }

    /// Add a predicate, implicitly AND-ing with any existing predicate.
    #[must_use]
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.query = self.query.filter(predicate);
        self
    }

    /// Apply a dynamic filter expression.
    pub fn filter_expr(mut self, expr: FilterExpr) -> Result<Self, QueryError> {
        self.query = self.query.filter_expr(expr)?;
        Ok(self)
    }

    /// Apply a dynamic sort expression.
    pub fn sort_expr(mut self, expr: SortExpr) -> Result<Self, QueryError> {
        self.query = self.query.sort_expr(expr)?;
        Ok(self)
    }

    /// Append an ascending sort key.
    #[must_use]
    pub fn order_by(mut self, field: impl AsRef<str>) -> Self {
        self.query = self.query.order_by(field);
        self
    }

    /// Append a descending sort key.
    #[must_use]
    pub fn order_by_desc(mut self, field: impl AsRef<str>) -> Self {
        self.query = self.query.order_by_desc(field);
        self
    }

    /// Apply a load limit.
    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        self.query = self.query.limit(limit);
        self
    }

    /// Apply a load offset.
    #[must_use]
    pub fn offset(mut self, offset: u32) -> Self {
        self.query = self.query.offset(offset);
        self
    }

    // ==================================================================
    // Planning / diagnostics (no execution)
    // ==================================================================

    /// Explain this query without executing it.
    pub fn explain(&self) -> Result<ExplainPlan, QueryError> {
        self.query.explain()
    }

    /// Plan this query into an executor-ready plan.
    pub fn plan(&self) -> Result<ExecutablePlan<E>, QueryError> {
        self.query.plan()
    }

    // ==================================================================
    // Execution boundary (single entry point)
    // ==================================================================

    /// Execute this query using the session's policy settings.
    pub fn execute(&self) -> Result<Response<E>, QueryError> {
        self.session.execute_query(self.query())
    }

    // ==================================================================
    // Execution terminals — cardinality / existence
    // ==================================================================

    /// Return whether any rows match this query.
    pub fn exists(&self) -> Result<bool, QueryError> {
        Ok(self.count()? > 0)
    }

    /// Execute and return whether the response is empty.
    pub fn is_empty(&self) -> Result<bool, QueryError> {
        Ok(self.execute()?.is_empty())
    }

    /// Execute and return the number of matching rows.
    pub fn count(&self) -> Result<u64, QueryError> {
        Ok(self.execute()?.count())
    }

    /// Execute and require exactly one row.
    pub fn require_one(&self) -> Result<(), QueryError> {
        self.execute()?.require_one().map_err(QueryError::Response)
    }

    /// Execute and require at least one row.
    pub fn require_some(&self) -> Result<(), QueryError> {
        self.execute()?.require_some().map_err(QueryError::Response)
    }

    // ==================================================================
    // Execution terminals — rows
    // ==================================================================

    pub fn row(&self) -> Result<Row<E>, QueryError> {
        self.execute()?.row().map_err(QueryError::Response)
    }

    pub fn try_row(&self) -> Result<Option<Row<E>>, QueryError> {
        self.execute()?.try_row().map_err(QueryError::Response)
    }

    pub fn rows(&self) -> Result<Vec<Row<E>>, QueryError> {
        Ok(self.execute()?.rows())
    }

    // ==================================================================
    // Execution terminals — entities
    // ==================================================================

    pub fn entity(&self) -> Result<E, QueryError> {
        self.execute()?.entity().map_err(QueryError::Response)
    }

    pub fn try_entity(&self) -> Result<Option<E>, QueryError> {
        self.execute()?.try_entity().map_err(QueryError::Response)
    }

    pub fn entities(&self) -> Result<Vec<E>, QueryError> {
        Ok(self.execute()?.entities())
    }

    /// Alias for `entity`.
    pub fn one(&self) -> Result<E, QueryError> {
        self.entity()
    }

    /// Alias for `try_entity`.
    pub fn one_opt(&self) -> Result<Option<E>, QueryError> {
        self.try_entity()
    }

    /// Alias for `entities`.
    pub fn all(&self) -> Result<Vec<E>, QueryError> {
        self.entities()
    }

    // ==================================================================
    // Execution terminals — store keys
    // ==================================================================

    pub fn key(&self) -> Result<Option<Key>, QueryError> {
        Ok(self.execute()?.key())
    }

    pub fn key_strict(&self) -> Result<Key, QueryError> {
        self.execute()?.key_strict().map_err(QueryError::Response)
    }

    pub fn try_key(&self) -> Result<Option<Key>, QueryError> {
        self.execute()?.try_key().map_err(QueryError::Response)
    }

    pub fn keys(&self) -> Result<Vec<Key>, QueryError> {
        Ok(self.execute()?.keys())
    }

    pub fn contains_key(&self, key: &Key) -> Result<bool, QueryError> {
        Ok(self.execute()?.contains_key(key))
    }

    // ==================================================================
    // Execution terminals — primary keys
    // ==================================================================

    pub fn primary_key(&self) -> Result<E::PrimaryKey, QueryError> {
        self.execute()?.primary_key().map_err(QueryError::Response)
    }

    pub fn try_primary_key(&self) -> Result<Option<E::PrimaryKey>, QueryError> {
        self.execute()?
            .try_primary_key()
            .map_err(QueryError::Response)
    }

    pub fn primary_keys(&self) -> Result<Vec<E::PrimaryKey>, QueryError> {
        Ok(self.execute()?.primary_keys())
    }

    // ==================================================================
    // Execution terminals — views
    // ==================================================================

    pub fn views(&self) -> Result<Vec<View<E>>, QueryError> {
        Ok(self.execute()?.views())
    }

    pub fn view(&self) -> Result<View<E>, QueryError> {
        self.execute()?.view().map_err(QueryError::Response)
    }

    pub fn view_opt(&self) -> Result<Option<View<E>>, QueryError> {
        self.execute()?.view_opt().map_err(QueryError::Response)
    }
}

impl<C: CanisterKind, E: EntityKind<Canister = C, PrimaryKey = ()>> SessionLoadQuery<'_, C, E> {
    /// Load the singleton entity identified by the unit primary key `()`.
    ///
    /// Semantics:
    /// - Equivalent to `WHERE pk = ()`
    /// - Uses key-based access (ByKey)
    /// - Does not allow predicates
    /// - MissingOk mode returns empty
    /// - Strict mode treats missing row as corruption
    #[must_use]
    pub fn only(mut self) -> Self {
        self.query = self.query.only();
        self
    }
}
