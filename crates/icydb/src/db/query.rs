use candid::CandidType;
use icydb_core as core;
use serde::{Deserialize, Serialize};

///
/// Re-exports
///
pub use core::db::query::{
    DeleteQuery, IndexPlan, LoadQuery, QueryError, QueryPlan, QueryPlanner, QueryValidate, SaveMode,
};

///
/// SaveQuery
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct SaveQuery {
    pub mode: SaveMode,
    pub bytes: Vec<u8>,
}

impl SaveQuery {
    /// Create a new save query for the given mode.
    #[must_use]
    pub const fn new(mode: SaveMode) -> Self {
        Self {
            mode,
            bytes: vec![],
        }
    }

    /// Use an already-serialized entity payload.
    #[must_use]
    pub fn from_bytes(mut self, bytes: &[u8]) -> Self {
        self.bytes = bytes.to_vec();
        self
    }

    pub(crate) fn into_inner(self) -> core::db::query::SaveQuery {
        core::db::query::SaveQuery {
            mode: self.mode,
            bytes: self.bytes,
        }
    }
}

impl From<icydb_core::db::query::SaveQuery> for SaveQuery {
    fn from(query: icydb_core::db::query::SaveQuery) -> Self {
        Self {
            mode: query.mode,
            bytes: query.bytes,
        }
    }
}

impl From<SaveQuery> for icydb_core::db::query::SaveQuery {
    fn from(query: SaveQuery) -> Self {
        query.into_inner()
    }
}

/// Start building a `LoadQuery`.
#[must_use]
pub fn load() -> LoadQuery {
    icydb_core::db::query::load()
}

/// Start building a `DeleteQuery`.
#[must_use]
pub fn delete() -> DeleteQuery {
    icydb_core::db::query::delete()
}

/// Build an insert `SaveQuery`.
#[must_use]
pub const fn insert() -> SaveQuery {
    SaveQuery::new(SaveMode::Insert)
}

/// Build an update `SaveQuery`.
#[must_use]
pub const fn update() -> SaveQuery {
    SaveQuery::new(SaveMode::Update)
}

/// Build a replace `SaveQuery`.
#[must_use]
pub const fn replace() -> SaveQuery {
    SaveQuery::new(SaveMode::Replace)
}
