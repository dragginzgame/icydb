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

        fn filter_expr(mut self, expr: crate::db::query::FilterExpr) -> Self {
            self.inner = self.inner.filter(expr);
            self
        }

        /// Attach one pre-built predicate expression.
        ///
        /// Prefer the `filter_*` helpers for app-level query code. This raw
        /// expression hook is for advanced composition where the caller already
        /// owns a `FilterExpr`.
        #[must_use]
        pub fn filter(self, expr: impl Into<FilterExpr>) -> Self {
            self.filter_expr(expr.into())
        }

        /// Filter by strict equality on one field.
        #[must_use]
        pub fn filter_eq(
            self,
            field: impl AsRef<str>,
            value: impl Into<crate::db::query::FilterValue>,
        ) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::eq(field.as_ref(), value))
        }

        /// Filter by strict inequality on one field.
        #[must_use]
        pub fn filter_ne(
            self,
            field: impl AsRef<str>,
            value: impl Into<crate::db::query::FilterValue>,
        ) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::ne(field.as_ref(), value))
        }

        /// Filter by `field < value`.
        #[must_use]
        pub fn filter_lt(
            self,
            field: impl AsRef<str>,
            value: impl Into<crate::db::query::FilterValue>,
        ) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::lt(field.as_ref(), value))
        }

        /// Filter by `field <= value`.
        #[must_use]
        pub fn filter_lte(
            self,
            field: impl AsRef<str>,
            value: impl Into<crate::db::query::FilterValue>,
        ) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::lte(field.as_ref(), value))
        }

        /// Filter by `field > value`.
        #[must_use]
        pub fn filter_gt(
            self,
            field: impl AsRef<str>,
            value: impl Into<crate::db::query::FilterValue>,
        ) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::gt(field.as_ref(), value))
        }

        /// Filter by `field >= value`.
        #[must_use]
        pub fn filter_gte(
            self,
            field: impl AsRef<str>,
            value: impl Into<crate::db::query::FilterValue>,
        ) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::gte(field.as_ref(), value))
        }

        /// Filter by case-insensitive text equality on one field.
        #[must_use]
        pub fn filter_text_eq_ci(
            self,
            field: impl AsRef<str>,
            value: impl Into<crate::db::query::FilterValue>,
        ) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::eq_ci(field.as_ref(), value))
        }

        /// Filter by strict equality between two fields.
        #[must_use]
        pub fn filter_eq_field(
            self,
            left_field: impl AsRef<str>,
            right_field: impl AsRef<str>,
        ) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::eq_field(
                left_field.as_ref(),
                right_field.as_ref(),
            ))
        }

        /// Filter by strict inequality between two fields.
        #[must_use]
        pub fn filter_ne_field(
            self,
            left_field: impl AsRef<str>,
            right_field: impl AsRef<str>,
        ) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::ne_field(
                left_field.as_ref(),
                right_field.as_ref(),
            ))
        }

        /// Filter by `left_field < right_field`.
        #[must_use]
        pub fn filter_lt_field(
            self,
            left_field: impl AsRef<str>,
            right_field: impl AsRef<str>,
        ) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::lt_field(
                left_field.as_ref(),
                right_field.as_ref(),
            ))
        }

        /// Filter by `left_field <= right_field`.
        #[must_use]
        pub fn filter_lte_field(
            self,
            left_field: impl AsRef<str>,
            right_field: impl AsRef<str>,
        ) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::lte_field(
                left_field.as_ref(),
                right_field.as_ref(),
            ))
        }

        /// Filter by `left_field > right_field`.
        #[must_use]
        pub fn filter_gt_field(
            self,
            left_field: impl AsRef<str>,
            right_field: impl AsRef<str>,
        ) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::gt_field(
                left_field.as_ref(),
                right_field.as_ref(),
            ))
        }

        /// Filter by `left_field >= right_field`.
        #[must_use]
        pub fn filter_gte_field(
            self,
            left_field: impl AsRef<str>,
            right_field: impl AsRef<str>,
        ) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::gte_field(
                left_field.as_ref(),
                right_field.as_ref(),
            ))
        }

        /// Filter by membership in a fixed value list.
        #[must_use]
        pub fn filter_in<I, V>(self, field: impl AsRef<str>, values: I) -> Self
        where
            I: IntoIterator<Item = V>,
            V: Into<crate::db::query::FilterValue>,
        {
            self.filter_expr(crate::db::query::FilterExpr::in_list(
                field.as_ref(),
                values,
            ))
        }

        /// Filter by absence from a fixed value list.
        #[must_use]
        pub fn filter_not_in<I, V>(self, field: impl AsRef<str>, values: I) -> Self
        where
            I: IntoIterator<Item = V>,
            V: Into<crate::db::query::FilterValue>,
        {
            self.filter_expr(crate::db::query::FilterExpr::not_in(field.as_ref(), values))
        }

        /// Filter by collection containment.
        #[must_use]
        pub fn filter_contains(
            self,
            field: impl AsRef<str>,
            value: impl Into<crate::db::query::FilterValue>,
        ) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::contains(
                field.as_ref(),
                value,
            ))
        }

        /// Filter by an explicitly null field value.
        #[must_use]
        pub fn filter_is_null(self, field: impl AsRef<str>) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::is_null(field.as_ref()))
        }

        /// Filter by a present non-null field value.
        #[must_use]
        pub fn filter_is_not_null(self, field: impl AsRef<str>) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::is_not_null(field.as_ref()))
        }

        /// Filter by a missing field.
        #[must_use]
        pub fn filter_is_missing(self, field: impl AsRef<str>) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::is_missing(field.as_ref()))
        }

        /// Filter by an empty field value.
        #[must_use]
        pub fn filter_is_empty(self, field: impl AsRef<str>) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::is_empty(field.as_ref()))
        }

        /// Filter by a non-empty field value.
        #[must_use]
        pub fn filter_is_not_empty(self, field: impl AsRef<str>) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::is_not_empty(field.as_ref()))
        }

        /// Filter by case-sensitive text containment.
        #[must_use]
        pub fn filter_text_contains(
            self,
            field: impl AsRef<str>,
            value: impl Into<crate::db::query::FilterValue>,
        ) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::text_contains(
                field.as_ref(),
                value,
            ))
        }

        /// Filter by case-insensitive text containment.
        #[must_use]
        pub fn filter_text_contains_ci(
            self,
            field: impl AsRef<str>,
            value: impl Into<crate::db::query::FilterValue>,
        ) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::text_contains_ci(
                field.as_ref(),
                value,
            ))
        }

        /// Filter by case-sensitive text prefix.
        #[must_use]
        pub fn filter_text_starts_with(
            self,
            field: impl AsRef<str>,
            value: impl Into<crate::db::query::FilterValue>,
        ) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::starts_with(
                field.as_ref(),
                value,
            ))
        }

        /// Filter by case-insensitive text prefix.
        #[must_use]
        pub fn filter_text_starts_with_ci(
            self,
            field: impl AsRef<str>,
            value: impl Into<crate::db::query::FilterValue>,
        ) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::starts_with_ci(
                field.as_ref(),
                value,
            ))
        }

        /// Filter by case-sensitive text suffix.
        #[must_use]
        pub fn filter_text_ends_with(
            self,
            field: impl AsRef<str>,
            value: impl Into<crate::db::query::FilterValue>,
        ) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::ends_with(
                field.as_ref(),
                value,
            ))
        }

        /// Filter by case-insensitive text suffix.
        #[must_use]
        pub fn filter_text_ends_with_ci(
            self,
            field: impl AsRef<str>,
            value: impl Into<crate::db::query::FilterValue>,
        ) -> Self {
            self.filter_expr(crate::db::query::FilterExpr::ends_with_ci(
                field.as_ref(),
                value,
            ))
        }

        /// Filter by inclusive scalar range.
        #[must_use]
        pub fn filter_between(
            self,
            field: impl AsRef<str>,
            lower: impl Into<crate::db::query::FilterValue>,
            upper: impl Into<crate::db::query::FilterValue>,
        ) -> Self {
            let field = field.as_ref();
            self.filter_expr(crate::db::query::FilterExpr::and(vec![
                crate::db::query::FilterExpr::gte(field, lower),
                crate::db::query::FilterExpr::lte(field, upper),
            ]))
        }

        /// Filter by inclusive field-to-field range.
        #[must_use]
        pub fn filter_between_fields(
            self,
            field: impl AsRef<str>,
            lower_field: impl AsRef<str>,
            upper_field: impl AsRef<str>,
        ) -> Self {
            let field = field.as_ref();
            self.filter_expr(crate::db::query::FilterExpr::and(vec![
                crate::db::query::FilterExpr::gte_field(field, lower_field.as_ref()),
                crate::db::query::FilterExpr::lte_field(field, upper_field.as_ref()),
            ]))
        }

        /// Filter by values outside an inclusive scalar range.
        #[must_use]
        pub fn filter_not_between(
            self,
            field: impl AsRef<str>,
            lower: impl Into<crate::db::query::FilterValue>,
            upper: impl Into<crate::db::query::FilterValue>,
        ) -> Self {
            let field = field.as_ref();
            self.filter_expr(crate::db::query::FilterExpr::or(vec![
                crate::db::query::FilterExpr::lt(field, lower),
                crate::db::query::FilterExpr::gt(field, upper),
            ]))
        }

        /// Filter by values outside an inclusive field-to-field range.
        #[must_use]
        pub fn filter_not_between_fields(
            self,
            field: impl AsRef<str>,
            lower_field: impl AsRef<str>,
            upper_field: impl AsRef<str>,
        ) -> Self {
            let field = field.as_ref();
            self.filter_expr(crate::db::query::FilterExpr::or(vec![
                crate::db::query::FilterExpr::lt_field(field, lower_field.as_ref()),
                crate::db::query::FilterExpr::gt_field(field, upper_field.as_ref()),
            ]))
        }

        /// Order by one typed ORDER BY term.
        #[must_use]
        pub fn order_term(mut self, term: crate::db::query::OrderTerm) -> Self {
            self.inner = self.inner.order_term(term);
            self
        }

        /// Order by one field/expression using one explicit direction.
        #[must_use]
        pub fn order_by(
            self,
            direction: crate::db::query::OrderDirection,
            expr: impl Into<crate::db::query::OrderExpr>,
        ) -> Self {
            match direction {
                crate::db::query::OrderDirection::Asc => self.order_asc(expr),
                crate::db::query::OrderDirection::Desc => self.order_desc(expr),
            }
        }

        /// Order by one field/expression ascending.
        #[must_use]
        pub fn order_asc(self, expr: impl Into<crate::db::query::OrderExpr>) -> Self {
            self.order_term(crate::db::query::asc(expr))
        }

        /// Order by one field/expression descending.
        #[must_use]
        pub fn order_desc(self, expr: impl Into<crate::db::query::OrderExpr>) -> Self {
            self.order_term(crate::db::query::desc(expr))
        }

        /// Order by multiple typed ORDER BY terms in declaration order.
        #[must_use]
        pub fn order_terms<I>(mut self, terms: I) -> Self
        where
            I: IntoIterator<Item = crate::db::query::OrderTerm>,
        {
            self.inner = self.inner.order_terms(terms);
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
