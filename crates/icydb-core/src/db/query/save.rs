use crate::{runtime_error::RuntimeError, serialize, traits::EntityKind};
use candid::CandidType;
use derive_more::Display;
use serde::{Deserialize, Serialize};

///
/// SaveMode
///
/// Create  : will only insert a row if it's empty
/// Replace : will change the row regardless of what was there
/// Update  : will only change an existing row
///

#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Display, Serialize)]
pub enum SaveMode {
    #[default]
    Insert,
    Replace,
    Update,
    //    Upsert,
}

///
/// SaveQuery
///

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct SaveQuery {
    pub mode: SaveMode,
    pub bytes: Vec<u8>,
}

impl SaveQuery {
    #[must_use]
    /// Create a new save query for the given mode.
    pub fn new(mode: SaveMode) -> Self {
        Self {
            mode,
            ..Default::default()
        }
    }

    // from
    /// Serialize an entity into the query payload.
    pub fn from<E: EntityKind>(mut self, input: impl Into<E>) -> Result<Self, RuntimeError> {
        let entity = input.into();
        self.bytes = serialize(&entity)?;

        Ok(self)
    }

    // from_bytes
    #[must_use]
    /// Use an already-serialized entity payload.
    pub fn from_bytes(mut self, bytes: &[u8]) -> Self {
        self.bytes = bytes.to_vec();
        self
    }

    // from_entity
    /// Serialize the provided entity into the query payload.
    pub fn from_entity<E: EntityKind>(mut self, entity: E) -> Result<Self, RuntimeError> {
        self.bytes = serialize(&entity)?;

        Ok(self)
    }
}
