use crate::{
    db::primitives::filter::{Cmp, FilterClause, FilterExpr},
    traits::FieldValue,
};

///
/// FilterDsl
///

#[derive(Clone, Copy, Debug, Default)]
pub struct FilterDsl;

macro_rules! cmp_fns {
    ($( $name:ident => $cmp:ident ),*) => {
        $(
            pub fn $name(self, field: impl AsRef<str>, v: impl FieldValue) -> FilterExpr {
                FilterExpr::Clause(FilterClause::new(field.as_ref(), Cmp::$cmp, v))
            }
        )*
    }
}

impl FilterDsl {}

impl FilterDsl {
    //
    // Comparators
    //

    cmp_fns! {
        eq => Eq,
        eq_ci => EqCi,
        ne => Ne,
        ne_ci => NeCi,
        lt => Lt,
        lte => Lte,
        gt => Gt,
        gte => Gte,
        contains => Contains,
        contains_ci => ContainsCi,
        starts_with => StartsWith,
        starts_with_ci => StartsWithCi,
        ends_with => EndsWith,
        ends_with_ci => EndsWithCi
    }

    //
    // ───────────────────────────────────────────────
    // LOGICAL COMBINATORS
    // ───────────────────────────────────────────────
    //

    #[must_use]
    pub fn not(expr: FilterExpr) -> FilterExpr {
        FilterExpr::Not(Box::new(expr))
    }

    #[must_use]
    pub fn not_expr(self, expr: FilterExpr) -> FilterExpr {
        Self::not(expr)
    }

    pub fn all<I>(items: I) -> FilterExpr
    where
        I: IntoIterator<Item = FilterExpr>,
    {
        let mut it = items.into_iter();
        match it.next() {
            Some(first) => it.fold(first, FilterExpr::and),
            None => FilterExpr::True,
        }
    }

    pub fn all_expr<I>(self, items: I) -> FilterExpr
    where
        I: IntoIterator<Item = FilterExpr>,
    {
        Self::all(items)
    }

    pub fn any<I>(items: I) -> FilterExpr
    where
        I: IntoIterator<Item = FilterExpr>,
    {
        let mut it = items.into_iter();
        match it.next() {
            Some(first) => it.fold(first, FilterExpr::or),
            None => FilterExpr::False,
        }
    }

    pub fn any_expr<I>(self, items: I) -> FilterExpr
    where
        I: IntoIterator<Item = FilterExpr>,
    {
        Self::any(items)
    }

    //
    // ───────────────────────────────────────────────
    // VALUE LIST OPERATORS (scalar list filtering)
    // ───────────────────────────────────────────────
    //

    /// field IN (v1, v2, v3)
    #[inline]
    pub fn in_iter<I>(self, field: impl AsRef<str>, vals: I) -> FilterExpr
    where
        I: IntoIterator,
        I::Item: FieldValue,
    {
        Self::cmp_iter(field, Cmp::In, vals)
    }

    /// ergonomic alias
    #[inline]
    pub fn in_list<I>(self, field: impl AsRef<str>, vals: I) -> FilterExpr
    where
        I: IntoIterator,
        I::Item: FieldValue,
    {
        self.in_iter(field, vals)
    }

    /// NOT IN (v1, v2, v3)
    #[inline]
    pub fn not_in_iter<I>(self, field: impl AsRef<str>, vals: I) -> FilterExpr
    where
        I: IntoIterator,
        I::Item: FieldValue,
    {
        Self::cmp_iter(field, Cmp::NotIn, vals)
    }

    #[inline]
    pub fn not_in_list<I>(self, field: impl AsRef<str>, vals: I) -> FilterExpr
    where
        I: IntoIterator,
        I::Item: FieldValue,
    {
        self.not_in_iter(field, vals)
    }

    /// ANY elements of values are included
    #[inline]
    pub fn any_in<I>(self, field: impl AsRef<str>, vals: I) -> FilterExpr
    where
        I: IntoIterator,
        I::Item: FieldValue,
    {
        Self::cmp_iter(field, Cmp::AnyIn, vals)
    }

    #[inline]
    pub fn has_any<I>(self, field: impl AsRef<str>, vals: I) -> FilterExpr
    where
        I: IntoIterator,
        I::Item: FieldValue,
    {
        self.any_in(field, vals)
    }

    /// ALL values are included
    #[inline]
    pub fn all_in<I>(self, field: impl AsRef<str>, vals: I) -> FilterExpr
    where
        I: IntoIterator,
        I::Item: FieldValue,
    {
        Self::cmp_iter(field, Cmp::AllIn, vals)
    }

    #[inline]
    pub fn has_all<I>(self, field: impl AsRef<str>, vals: I) -> FilterExpr
    where
        I: IntoIterator,
        I::Item: FieldValue,
    {
        self.all_in(field, vals)
    }

    // CI versions (keep!)
    #[inline]
    pub fn any_in_ci<I>(self, field: impl AsRef<str>, vals: I) -> FilterExpr
    where
        I: IntoIterator,
        I::Item: FieldValue,
    {
        Self::cmp_iter(field, Cmp::AnyInCi, vals)
    }

    #[inline]
    pub fn all_in_ci<I>(self, field: impl AsRef<str>, vals: I) -> FilterExpr
    where
        I: IntoIterator,
        I::Item: FieldValue,
    {
        Self::cmp_iter(field, Cmp::AllInCi, vals)
    }

    //
    // ───────────────────────────────────────────────
    // CONDITIONAL HELPERS (already good)
    // ───────────────────────────────────────────────
    //

    #[inline]
    pub fn when(self, cond: bool, f: impl FnOnce() -> FilterExpr) -> Option<FilterExpr> {
        cond.then(f)
    }

    #[inline]
    pub fn when_some<T>(
        self,
        opt: Option<T>,
        f: impl FnOnce(T) -> FilterExpr,
    ) -> Option<FilterExpr> {
        opt.map(f)
    }

    //
    // Always / Never
    // (good placeholders for constructing queries)
    //

    #[must_use]
    pub const fn always(self) -> FilterExpr {
        FilterExpr::True
    }
    #[must_use]
    pub const fn never(self) -> FilterExpr {
        FilterExpr::False
    }

    //
    // Empty
    //

    pub fn is_empty(self, field: impl AsRef<str>) -> FilterExpr {
        FilterExpr::Clause(FilterClause::new(field.as_ref(), Cmp::IsEmpty, ()))
    }
    pub fn is_not_empty(self, field: impl AsRef<str>) -> FilterExpr {
        FilterExpr::Clause(FilterClause::new(field.as_ref(), Cmp::IsNotEmpty, ()))
    }

    //
    // Presence / None
    //

    pub fn is_some(self, field: impl AsRef<str>) -> FilterExpr {
        FilterExpr::Clause(FilterClause::new(field.as_ref(), Cmp::IsSome, ()))
    }
    pub fn is_none(self, field: impl AsRef<str>) -> FilterExpr {
        FilterExpr::Clause(FilterClause::new(field.as_ref(), Cmp::IsNone, ()))
    }

    //
    // Collections
    //

    fn cmp_iter<I>(field: impl AsRef<str>, cmp: Cmp, vals: I) -> FilterExpr
    where
        I: IntoIterator,
        I::Item: FieldValue,
    {
        let list = vals.into_iter().map(|v| v.to_value()).collect::<Vec<_>>();

        FilterExpr::Clause(FilterClause::new(field.as_ref(), cmp, list))
    }

    //
    // Maps
    //

    pub fn map_contains_key(self, field: impl AsRef<str>, key: impl FieldValue) -> FilterExpr {
        FilterExpr::Clause(FilterClause::new(field.as_ref(), Cmp::MapContainsKey, key))
    }

    pub fn map_not_contains_key(self, field: impl AsRef<str>, key: impl FieldValue) -> FilterExpr {
        FilterExpr::Clause(FilterClause::new(
            field.as_ref(),
            Cmp::MapNotContainsKey,
            key,
        ))
    }

    pub fn map_contains_value(self, field: impl AsRef<str>, value: impl FieldValue) -> FilterExpr {
        FilterExpr::Clause(FilterClause::new(
            field.as_ref(),
            Cmp::MapContainsValue,
            value,
        ))
    }

    pub fn map_not_contains_value(
        self,
        field: impl AsRef<str>,
        value: impl FieldValue,
    ) -> FilterExpr {
        FilterExpr::Clause(FilterClause::new(
            field.as_ref(),
            Cmp::MapNotContainsValue,
            value,
        ))
    }

    pub fn map_contains_entry(
        self,
        field: impl AsRef<str>,
        key: impl FieldValue,
        value: impl FieldValue,
    ) -> FilterExpr {
        let entry = vec![key.to_value(), value.to_value()];
        FilterExpr::Clause(FilterClause::new(
            field.as_ref(),
            Cmp::MapContainsEntry,
            entry,
        ))
    }

    pub fn map_not_contains_entry(
        self,
        field: impl AsRef<str>,
        key: impl FieldValue,
        value: impl FieldValue,
    ) -> FilterExpr {
        let entry = vec![key.to_value(), value.to_value()];
        FilterExpr::Clause(FilterClause::new(
            field.as_ref(),
            Cmp::MapNotContainsEntry,
            entry,
        ))
    }
}
