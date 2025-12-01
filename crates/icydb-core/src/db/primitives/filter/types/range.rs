use crate::{
    db::primitives::filter::{FilterDsl, FilterExpr, FilterKind, IntoScopedFilterExpr},
    traits::FieldValue,
    types::{Decimal, Int, Nat},
};
use candid::CandidType;
use serde::{Deserialize, Serialize};

///
/// FilterKinds
///

pub struct DecimalRangeFilterKind;
impl FilterKind for DecimalRangeFilterKind {
    type Payload = DecimalRangeFilter;
}

pub struct Int64RangeFilterKind;
impl FilterKind for Int64RangeFilterKind {
    type Payload = Int64RangeFilter;
}

pub struct IntRangeFilterKind;
impl FilterKind for IntRangeFilterKind {
    type Payload = IntRangeFilter;
}

pub struct Nat64RangeFilterKind;
impl FilterKind for Nat64RangeFilterKind {
    type Payload = Nat64RangeFilter;
}

pub struct NatRangeFilterKind;
impl FilterKind for NatRangeFilterKind {
    type Payload = NatRangeFilter;
}

///
/// Aliases
///

pub type DecimalRangeFilter = RangeFilter<Decimal>;
pub type Int64RangeFilter = RangeFilter<i64>;
pub type IntRangeFilter = RangeFilter<Int>;
pub type Nat64RangeFilter = RangeFilter<u64>;
pub type NatRangeFilter = RangeFilter<Nat>;

///
/// RangeFilter
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct RangeFilter<V> {
    pub gt: Option<V>,
    pub gte: Option<V>,
    pub lt: Option<V>,
    pub lte: Option<V>,
    pub between: Option<(V, V)>,
}

impl<V> RangeFilter<V>
where
    V: CandidType + Default + FieldValue,
{
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn gt(mut self, v: impl Into<V>) -> Self {
        self.gt = Some(v.into());
        self
    }

    #[must_use]
    pub fn gte(mut self, v: impl Into<V>) -> Self {
        self.gte = Some(v.into());
        self
    }

    #[must_use]
    pub fn lt(mut self, v: impl Into<V>) -> Self {
        self.lt = Some(v.into());
        self
    }

    #[must_use]
    pub fn lte(mut self, v: impl Into<V>) -> Self {
        self.lte = Some(v.into());
        self
    }

    #[must_use]
    pub fn between(mut self, min: impl Into<V>, max: impl Into<V>) -> Self {
        self.between = Some((min.into(), max.into()));
        self
    }
}

impl<V> IntoScopedFilterExpr for RangeFilter<V>
where
    V: CandidType + Default + FieldValue,
{
    fn into_scoped(self, path: &str) -> FilterExpr {
        let dsl = FilterDsl;
        let mut exprs = vec![];

        if let Some(v) = self.gt {
            exprs.push(dsl.gt(path, v));
        }
        if let Some(v) = self.gte {
            exprs.push(dsl.gte(path, v));
        }
        if let Some(v) = self.lt {
            exprs.push(dsl.lt(path, v));
        }
        if let Some(v) = self.lte {
            exprs.push(dsl.lte(path, v));
        }
        if let Some((min, max)) = self.between {
            exprs.push(dsl.gte(path, min));
            exprs.push(dsl.lte(path, max));
        }

        FilterDsl::all(exprs)
    }
}
