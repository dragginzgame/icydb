use candid::CandidType;
use core::db::query::SaveQuery;
use icydb_core as core;
use serde::{Deserialize, Serialize};

///
/// Re-exports
/// Query planning types are exposed for diagnostics and intent composition.
///
pub use core::db::query::{
    DeleteLimit, IntentError, Page, Query, QueryError, QueryMode, ReadConsistency, SaveMode,
    builder, builder::*, diagnostics, predicate,
};

pub mod plan {
    pub use icydb_core::db::query::plan::{
        ExplainAccessPath, ExplainDeleteLimit, ExplainOrder, ExplainOrderBy, ExplainPagination,
        ExplainPlan, ExplainPredicate, ExplainProjection, OrderDirection, PlanError,
        PlanFingerprint,
    };
}

///
/// SaveCommand
///
/// Serialized save command intended for transport across canister or process
/// boundaries. This is a low-level wire format, not a fluent API.
///
/// Prefer `DbSession::{insert, update, replace}` for ergonomic, typed saves.
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct SaveCommand {
    pub mode: SaveMode,
    pub bytes: Vec<u8>,
}

impl SaveCommand {
    /// Create a new save command for the given mode.
    #[must_use]
    pub const fn new(mode: SaveMode) -> Self {
        Self {
            mode,
            bytes: Vec::new(),
        }
    }

    /// Attach an already-serialized entity payload.
    #[must_use]
    pub fn with_bytes(mut self, bytes: &[u8]) -> Self {
        self.bytes = bytes.to_vec();
        self
    }

    pub(crate) fn into_inner(self) -> SaveQuery {
        SaveQuery {
            mode: self.mode,
            bytes: self.bytes,
        }
    }

    #[must_use]
    pub const fn insert() -> Self {
        Self::new(SaveMode::Insert)
    }

    #[must_use]
    pub const fn update() -> Self {
        Self::new(SaveMode::Update)
    }

    #[must_use]
    pub const fn replace() -> Self {
        Self::new(SaveMode::Replace)
    }
}

impl From<SaveQuery> for SaveCommand {
    fn from(query: SaveQuery) -> Self {
        Self {
            mode: query.mode,
            bytes: query.bytes,
        }
    }
}

impl From<SaveCommand> for icydb_core::db::query::SaveQuery {
    fn from(query: SaveCommand) -> Self {
        query.into_inner()
    }
}
