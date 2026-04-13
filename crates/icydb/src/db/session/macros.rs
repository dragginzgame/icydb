macro_rules! impl_session_query_shape_methods {
    () => {
        /// Filter by a single typed primary-key value.
        #[must_use]
        pub fn by_id(mut self, id: Id<E>) -> Self {
            self.inner = self.inner.by_id(id);
            self
        }

        /// Filter by multiple typed primary-key values.
        #[must_use]
        pub fn by_ids<I>(mut self, ids: I) -> Self
        where
            I: IntoIterator<Item = Id<E>>,
        {
            self.inner = self.inner.by_ids(ids);
            self
        }

        /// Attach a predicate filter.
        #[must_use]
        pub fn filter(mut self, predicate: Predicate) -> Self {
            self.inner = self.inner.filter(predicate);
            self
        }

        /// Attach a typed filter expression.
        pub fn filter_expr(mut self, expr: FilterExpr) -> Result<Self, Error> {
            let core_expr = expr.lower::<E>()?;
            self.inner = self.inner.filter_expr(core_expr)?;
            Ok(self)
        }

        /// Attach a typed sort expression.
        pub fn sort_expr(mut self, expr: SortExpr) -> Result<Self, Error> {
            let core_expr = expr.lower();
            self.inner = self.inner.sort_expr(core_expr)?;
            Ok(self)
        }

        /// Order ascending by field.
        #[must_use]
        pub fn order_by(mut self, field: impl AsRef<str>) -> Self {
            self.inner = self.inner.order_by(field);
            self
        }

        /// Order descending by field.
        #[must_use]
        pub fn order_by_desc(mut self, field: impl AsRef<str>) -> Self {
            self.inner = self.inner.order_by_desc(field);
            self
        }

        /// Apply a result limit.
        #[must_use]
        pub fn limit(mut self, limit: u32) -> Self {
            self.inner = self.inner.limit(limit);
            self
        }
    };
}

macro_rules! impl_session_materialization_methods {
    () => {
        /// Execute the session query and materialize scalar or grouped rows.
        pub fn execute(&self) -> Result<QueryResponse<E>, Error>
        where
            E: EntityValue,
        {
            Ok(QueryResponse::from_core(self.inner.execute()?))
        }

        /// Return true when the result set has no rows.
        pub fn is_empty(&self) -> Result<bool, Error>
        where
            E: EntityValue,
        {
            Ok(self.inner.is_empty()?)
        }

        /// Return the row count.
        pub fn count(&self) -> Result<u32, Error>
        where
            E: EntityValue,
        {
            Ok(self.inner.count()?)
        }

        /// Require exactly one row.
        pub fn require_one(&self) -> Result<(), Error>
        where
            E: EntityValue,
        {
            icydb_core::db::ResponseCardinalityExt::require_one(&self.inner.execute()?.into_rows()?)
                .map_err(Into::into)
        }

        /// Require at least one row.
        pub fn require_some(&self) -> Result<(), Error>
        where
            E: EntityValue,
        {
            icydb_core::db::ResponseCardinalityExt::require_some(
                &self.inner.execute()?.into_rows()?,
            )
            .map_err(Into::into)
        }

        /// Materialize one row.
        pub fn row(&self) -> Result<Row<E>, Error>
        where
            E: EntityValue,
        {
            icydb_core::db::ResponseCardinalityExt::row(self.inner.execute()?.into_rows()?)
                .map_err(Into::into)
        }

        /// Materialize an optional row.
        pub fn try_row(&self) -> Result<Option<Row<E>>, Error>
        where
            E: EntityValue,
        {
            icydb_core::db::ResponseCardinalityExt::try_row(self.inner.execute()?.into_rows()?)
                .map_err(Into::into)
        }

        /// Materialize all rows.
        pub fn rows(&self) -> Result<Vec<Row<E>>, Error>
        where
            E: EntityValue,
        {
            Ok(self.inner.execute()?.into_rows()?.rows())
        }

        /// Materialize an optional id.
        pub fn id(&self) -> Result<Option<Id<E>>, Error>
        where
            E: EntityValue,
        {
            Ok(self
                .inner
                .execute()?
                .into_rows()?
                .iter()
                .next()
                .map(|row| row.id()))
        }

        /// Materialize one required id.
        pub fn require_id(&self) -> Result<Id<E>, Error>
        where
            E: EntityValue,
        {
            icydb_core::db::ResponseCardinalityExt::require_id(self.inner.execute()?.into_rows()?)
                .map_err(Into::into)
        }

        /// Materialize an optional id from an optional row.
        pub fn try_id(&self) -> Result<Option<Id<E>>, Error>
        where
            E: EntityValue,
        {
            icydb_core::db::ResponseCardinalityExt::try_row(self.inner.execute()?.into_rows()?)
                .map(|row| row.map(|entry| entry.id()))
                .map_err(Into::into)
        }

        /// Materialize all ids.
        pub fn ids(&self) -> Result<Vec<Id<E>>, Error>
        where
            E: EntityValue,
        {
            Ok(self.inner.execute()?.into_rows()?.ids().collect())
        }

        /// Check whether an id is present in the response.
        pub fn contains_id(&self, id: &Id<E>) -> Result<bool, Error>
        where
            E: EntityValue,
        {
            Ok(self.inner.execute()?.into_rows()?.contains_id(id))
        }

        /// Materialize one entity.
        pub fn entity(&self) -> Result<E, Error>
        where
            E: EntityValue,
        {
            icydb_core::db::ResponseCardinalityExt::entity(self.inner.execute()?.into_rows()?)
                .map_err(Into::into)
        }

        /// Materialize an optional entity.
        pub fn try_entity(&self) -> Result<Option<E>, Error>
        where
            E: EntityValue,
        {
            icydb_core::db::ResponseCardinalityExt::try_entity(self.inner.execute()?.into_rows()?)
                .map_err(Into::into)
        }

        /// Materialize all entities.
        pub fn entities(&self) -> Result<Vec<E>, Error>
        where
            E: EntityValue,
        {
            Ok(self.inner.execute()?.into_rows()?.entities())
        }
    };
}

pub(crate) use impl_session_materialization_methods;
pub(crate) use impl_session_query_shape_methods;
