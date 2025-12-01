use crate::{
    db::primitives::filter::{FilterDsl, FilterExpr, IntoFilterExpr},
    traits::{EntityKind, FieldValue},
};

/// Anything with a filter slot (e.g. a query builder)
pub trait FilterSlot {
    fn filter_slot(&mut self) -> &mut Option<FilterExpr>;
}

/// Extension trait for builder-style composition
impl<T: FilterSlot> FilterExt for T {}

pub trait FilterExt: FilterSlot + Sized {
    //
    // ---------- AND ------------
    //

    #[must_use]
    fn filter<F, I>(mut self, f: F) -> Self
    where
        F: FnOnce(FilterDsl) -> I,
        I: IntoFilterExpr,
    {
        let expr: FilterExpr = f(FilterDsl).into_expr();

        let slot = self.filter_slot();
        let combined = match slot.take() {
            Some(existing) => existing.and(expr),
            None => expr,
        };

        *slot = Some(combined);
        self
    }

    #[must_use]
    fn filter_opt<F, I>(self, f: F) -> Self
    where
        F: FnOnce(FilterDsl) -> Option<I>,
        I: IntoFilterExpr,
    {
        if let Some(i) = f(FilterDsl) {
            self.filter(|_| i)
        } else {
            self
        }
    }

    //
    // ---------- OR ------------
    //

    #[must_use]
    fn or_filter<F>(self, f: F) -> Self
    where
        F: FnOnce(FilterDsl) -> FilterExpr,
    {
        let expr = f(FilterDsl);
        self.or_filter_expr(expr)
    }

    #[must_use]
    fn or_filter_expr(mut self, expr: FilterExpr) -> Self {
        let slot = self.filter_slot();
        let newf = match slot.take() {
            Some(existing) => existing.or(expr),
            None => expr,
        };
        *slot = Some(newf);

        self
    }

    #[must_use]
    fn or_filter_expr_opt(self, expr: Option<FilterExpr>) -> Self {
        if let Some(expr) = expr {
            self.or_filter_expr(expr)
        } else {
            self
        }
    }

    #[must_use]
    fn simplify(mut self) -> Self {
        if let Some(f) = self.filter_slot().take() {
            *self.filter_slot() = Some(f.simplify());
        }
        self
    }

    //
    // Convenience primary-key filters
    //

    #[must_use]
    fn one<E: EntityKind>(self, value: impl FieldValue) -> Self {
        self.filter(|f| f.eq(E::PRIMARY_KEY, value))
    }

    #[must_use]
    fn only<E: EntityKind>(self) -> Self {
        self.filter(|f| f.eq(E::PRIMARY_KEY, ()))
    }

    #[must_use]
    fn many<E, I>(self, values: I) -> Self
    where
        E: EntityKind,
        I: IntoIterator,
        I::Item: FieldValue,
    {
        self.filter(|f| f.in_iter(E::PRIMARY_KEY, values))
    }
}
