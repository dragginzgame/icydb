use crate::{
    db::primitives::filter::{FilterDsl, FilterExpr, FilterKind, IntoScopedFilterExpr},
    traits::FieldValue,
    types::Decimal,
};
use candid::CandidType;
use serde::{Deserialize, Serialize};

///
/// FilterKinds
///

pub struct TextEqualityFilterKind;
impl FilterKind for TextEqualityFilterKind {
    type Payload = TextEqualityFilter;
}

pub struct Int64EqualityFilterKind;
impl FilterKind for Int64EqualityFilterKind {
    type Payload = Int64EqualityFilter;
}

pub struct Nat64EqualityFilterKind;
impl FilterKind for Nat64EqualityFilterKind {
    type Payload = Nat64EqualityFilter;
}

pub struct BoolEqualityFilterKind;
impl FilterKind for BoolEqualityFilterKind {
    type Payload = BoolEqualityFilter;
}

pub struct DecimalEqualityFilterKind;
impl FilterKind for DecimalEqualityFilterKind {
    type Payload = DecimalEqualityFilter;
}

///
/// Aliases
///

pub type TextEqualityFilter = EqualityFilter<String>;
pub type Int64EqualityFilter = EqualityFilter<i64>;
pub type Nat64EqualityFilter = EqualityFilter<u64>;
pub type BoolEqualityFilter = EqualityFilter<bool>;
pub type DecimalEqualityFilter = EqualityFilter<Decimal>;

///
/// EqualityFilter
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct EqualityFilter<T>
where
    T: Default,
{
    pub eq: Option<T>,
    pub ne: Option<T>,
    pub in_: Option<Vec<T>>,
    pub not_in: Option<Vec<T>>,
    pub is_none: Option<bool>,
}

impl<T> EqualityFilter<T>
where
    T: Default,
{
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    // --- single value comparisons ---

    #[must_use]
    pub fn eq(mut self, value: T) -> Self {
        self.eq = Some(value);
        self
    }

    #[must_use]
    pub fn ne(mut self, value: T) -> Self {
        self.ne = Some(value);
        self
    }

    // --- multi-value comparisons ---

    #[must_use]
    pub fn in_<I>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        Self::extend_slot(&mut self.in_, values);
        self
    }

    #[must_use]
    pub fn not_in<I>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        Self::extend_slot(&mut self.not_in, values);
        self
    }

    // --- null/none tests ---

    #[must_use]
    pub const fn is_none(mut self, val: bool) -> Self {
        self.is_none = Some(val);
        self
    }

    #[must_use]
    pub const fn none(self) -> Self {
        self.is_none(true)
    }

    #[must_use]
    pub const fn some(self) -> Self {
        self.is_none(false)
    }

    // internal helper
    fn extend_slot<I>(slot: &mut Option<Vec<T>>, values: I)
    where
        I: IntoIterator<Item = T>,
    {
        slot.get_or_insert_with(Vec::new).extend(values);
    }
}

///
/// IntoScopedFilterExpr implementation
///
/// Converts T into Value internally (in a single place)
///

impl<T> IntoScopedFilterExpr for EqualityFilter<T>
where
    T: CandidType + Default + FieldValue,
{
    fn into_scoped(self, field: &str) -> FilterExpr {
        let dsl = FilterDsl;
        let mut exprs = vec![];

        // eq
        if let Some(v) = self.eq {
            exprs.push(dsl.eq(field, v.to_value()));
        }

        // ne
        if let Some(v) = self.ne {
            exprs.push(dsl.ne(field, v.to_value()));
        }

        // in
        if let Some(vs) = self.in_ {
            let vs: Vec<_> = vs.into_iter().map(|v| v.to_value()).collect();
            exprs.push(dsl.in_iter(field, vs));
        }

        // not_in
        if let Some(vs) = self.not_in {
            let vs: Vec<_> = vs.into_iter().map(|v| v.to_value()).collect();
            exprs.push(dsl.not_in_iter(field, vs));
        }

        // is_none / is_some
        if let Some(is_none) = self.is_none {
            exprs.push(if is_none {
                dsl.is_none(field)
            } else {
                dsl.is_some(field)
            });
        }

        FilterDsl::all(exprs)
    }
}
