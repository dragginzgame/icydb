mod ext;

pub use ext::*;

use crate::{
    db::query::{QueryError, QueryValidate},
    traits::EntityKind,
};
use candid::CandidType;
use serde::{Deserialize, Serialize};

///
/// LimitExpr
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct LimitExpr {
    pub limit: Option<u32>,
    pub offset: u32,
}

impl LimitExpr {
    #[must_use]
    pub const fn new(limit: u32) -> Self {
        Self {
            limit: Some(limit),
            offset: 0,
        }
    }

    #[must_use]
    pub const fn with_offset(limit: u32, offset: u32) -> Self {
        Self {
            limit: Some(limit),
            offset,
        }
    }

    #[must_use]
    pub const fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    #[must_use]
    pub const fn offset(mut self, offset: u32) -> Self {
        self.offset = offset;
        self
    }
}

impl<E: EntityKind> QueryValidate<E> for LimitExpr {
    fn validate(&self) -> Result<(), QueryError> {
        Ok(())
    }
}
