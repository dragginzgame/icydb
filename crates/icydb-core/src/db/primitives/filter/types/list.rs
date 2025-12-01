use crate::{
    db::primitives::filter::{
        FilterDsl, FilterExpr, FilterKind, IntoScopedFilterExpr, RangeFilter,
    },
    traits::FieldValue,
    types::{Decimal, Int, Nat},
};
use candid::CandidType;
use serde::{Deserialize, Serialize};

///
/// FilterKinds
///

pub struct TextListFilterKind;
impl FilterKind for TextListFilterKind {
    type Payload = TextListFilter;
}

pub struct Int64ListFilterKind;
impl FilterKind for Int64ListFilterKind {
    type Payload = Int64ListFilter;
}

pub struct IntListFilterKind;
impl FilterKind for IntListFilterKind {
    type Payload = IntListFilter;
}

pub struct Nat64ListFilterKind;
impl FilterKind for Nat64ListFilterKind {
    type Payload = Nat64ListFilter;
}

pub struct NatListFilterKind;
impl FilterKind for NatListFilterKind {
    type Payload = NatListFilter;
}

pub struct BoolListFilterKind;
impl FilterKind for BoolListFilterKind {
    type Payload = BoolListFilter;
}

pub struct DecimalListFilterKind;
impl FilterKind for DecimalListFilterKind {
    type Payload = DecimalListFilter;
}

///
/// Aliases
///

pub type TextListFilter = ListValueFilter<String>;
pub type Int64ListFilter = ListValueFilter<i64>;
pub type IntListFilter = ListValueFilter<Int>;
pub type Nat64ListFilter = ListValueFilter<u64>;
pub type NatListFilter = ListValueFilter<Nat>;
pub type BoolListFilter = ListValueFilter<bool>;
pub type DecimalListFilter = ListValueFilter<Decimal>;

///
/// ListValueFilter
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct ListValueFilter<T>
where
    T: FieldValue,
{
    pub contains: Option<T>,
    pub has_any: Option<Vec<T>>,
    pub has_all: Option<Vec<T>>,

    // NEW negative variants
    pub not_contains: Option<T>,
    pub has_none: Option<Vec<T>>,
    pub lacks_all: Option<Vec<T>>,

    pub len: Option<RangeFilter<i64>>,
}

impl<T> IntoScopedFilterExpr for ListValueFilter<T>
where
    T: FieldValue,
{
    fn into_scoped(self, field: &str) -> FilterExpr {
        let mut exprs = Vec::new();
        let dsl = FilterDsl;

        // positive
        if let Some(v) = self.contains {
            exprs.push(dsl.contains(field, v));
        }
        if let Some(values) = self.has_any {
            exprs.push(dsl.any_in(field, values));
        }
        if let Some(values) = self.has_all {
            exprs.push(dsl.all_in(field, values));
        }

        //  negative variants
        if let Some(v) = self.not_contains {
            exprs.push(FilterDsl::not(dsl.contains(field, v)));
        }
        if let Some(vs) = self.has_none {
            exprs.push(FilterDsl::not(dsl.any_in(field, vs)));
        }
        if let Some(vs) = self.lacks_all {
            exprs.push(FilterDsl::not(dsl.all_in(field, vs)));
        }

        // len
        if let Some(r) = self.len {
            // backend treats this as a length constraint on `field`
            exprs.push(r.into_scoped(field));
        }

        FilterDsl::all(exprs)
    }
}

///
/// ListFilter
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct ListFilter<T>
where
    T: IntoScopedFilterExpr,
{
    pub any: Option<Vec<T>>,
    pub all: Option<Vec<T>>,
    pub none: Option<Vec<T>>,
}

impl<T> IntoScopedFilterExpr for ListFilter<T>
where
    T: IntoScopedFilterExpr,
{
    fn into_scoped(self, field: &str) -> FilterExpr {
        let dsl = FilterDsl;
        let mut exprs = Vec::new();

        // ANY — at least one element matches
        if let Some(filters) = self.any {
            let mapped = filters.into_iter().map(|f| f.into_scoped(field));
            exprs.push(dsl.any_expr(mapped));
        }

        // ALL — all elements must match
        if let Some(filters) = self.all {
            let mapped = filters.into_iter().map(|f| f.into_scoped(field));
            exprs.push(dsl.all_expr(mapped));
        }

        // NONE — no element must match
        if let Some(filters) = self.none {
            let mapped = filters.into_iter().map(|f| f.into_scoped(field));
            exprs.push(dsl.not_expr(dsl.any_expr(mapped)));
        }

        FilterDsl::all(exprs)
    }
}
