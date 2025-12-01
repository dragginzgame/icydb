mod equality;
mod list;
mod range;
mod text;

pub use equality::*;
pub use list::*;
pub use range::*;
pub use text::*;

use crate::db::primitives::filter::FilterExpr;
use candid::CandidType;
use serde::{Deserialize, Serialize};

///
/// FilterKind
///

pub trait FilterKind {
    type Payload: IntoScopedFilterExpr;

    fn to_expr(payload: Self::Payload, path: &str) -> FilterExpr {
        payload.into_scoped(path)
    }
}

///
/// IntoFilterExpr
/// Root-level: combines many field filters into one expression
///

pub trait IntoFilterExpr {
    fn into_expr(self) -> FilterExpr;
}

///
/// IntoScopedFilterExpr
/// Scoped-level: payloads and nested filters need the field path
///

pub trait IntoScopedFilterExpr {
    fn into_scoped(self, path: &str) -> FilterExpr;
}

///
/// NoFilterKind
///

pub struct NoFilterKind;

impl FilterKind for NoFilterKind {
    type Payload = NoFilter;
}

///
/// NoFilter
/// (#nofilter)
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct NoFilter;

impl IntoScopedFilterExpr for NoFilter {
    fn into_scoped(self, _path: &str) -> FilterExpr {
        FilterExpr::True
    }
}
