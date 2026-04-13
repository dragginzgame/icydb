//! Module: executor::mutation::save
//! Responsibility: save-mode execution (`insert`/`update`/`replace`) and batch lanes.
//! Does not own: relation-domain validation semantics or commit marker protocol internals.
//! Boundary: save preflight + commit-window handoff for one entity type.

mod batch;
mod shared;
mod structural;
mod typed;

use crate::{
    db::{Db, commit::CommitSchemaFingerprint, data::PersistedRow, schema::SchemaInfo},
    error::InternalError,
    sanitize::{SanitizeWriteContext, SanitizeWriteMode},
    traits::{EntityCreateInput, EntityValue},
    types::Timestamp,
};
use candid::CandidType;
use serde::{Deserialize, Serialize};

// Debug assertions below are diagnostic sentinels; correctness is enforced by
// runtime validation earlier in the pipeline.

//
// SaveMode
//
// Create  : will only insert a row if it's empty
// Replace : will change the row regardless of what was there
// Update  : will only change an existing row
//

#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Serialize)]
enum SaveMode {
    #[default]
    Insert,
    Replace,
    Update,
}

//
// SaveExecutor
//

#[derive(Clone, Copy)]
pub(in crate::db) struct SaveExecutor<E: PersistedRow + EntityValue> {
    pub(in crate::db::executor::mutation) db: Db<E::Canister>,
}

//
// SaveRule
//
// Canonical save precondition for resolving the current row baseline.
//

#[derive(Clone, Copy)]
enum SaveRule {
    RequireAbsent,
    RequirePresent,
    AllowAny,
}

impl SaveRule {
    const fn from_mode(mode: SaveMode) -> Self {
        match mode {
            SaveMode::Insert => Self::RequireAbsent,
            SaveMode::Update => Self::RequirePresent,
            SaveMode::Replace => Self::AllowAny,
        }
    }
}

///
/// SavePreflightInputs
///
/// Bundles the resolved write-boundary metadata reused across one save lane so
/// helper signatures stay explicit without scattering write-context ownership.
///
#[derive(Clone, Copy)]
struct SavePreflightInputs<'a> {
    schema: &'a SchemaInfo,
    schema_fingerprint: CommitSchemaFingerprint,
    validate_relations: bool,
    write_context: SanitizeWriteContext,
    authored_create_slots: Option<&'a [usize]>,
}

//
// MutationMode
//
// MutationMode makes the structural patch path spell out the same
// row-existence contract the typed save surface already owns.
// This keeps future structural callers from smuggling write-mode meaning
// through ad hoc helper choice once the seam moves beyond `icydb-core`.
//

#[derive(Clone, Copy)]
pub enum MutationMode {
    Insert,
    Replace,
    Update,
}

impl MutationMode {
    const fn save_rule(self) -> SaveRule {
        match self {
            Self::Insert => SaveRule::RequireAbsent,
            Self::Replace => SaveRule::AllowAny,
            Self::Update => SaveRule::RequirePresent,
        }
    }

    const fn sanitize_write_mode(self) -> SanitizeWriteMode {
        match self {
            Self::Insert => SanitizeWriteMode::Insert,
            Self::Replace => SanitizeWriteMode::Replace,
            Self::Update => SanitizeWriteMode::Update,
        }
    }
}

impl<E: PersistedRow + EntityValue> SaveExecutor<E> {
    // Build one canonical write preflight context for one typed save mode.
    const fn save_write_context(mode: SaveMode, now: Timestamp) -> SanitizeWriteContext {
        let mode = match mode {
            SaveMode::Insert => SanitizeWriteMode::Insert,
            SaveMode::Replace => SanitizeWriteMode::Replace,
            SaveMode::Update => SanitizeWriteMode::Update,
        };

        SanitizeWriteContext::new(mode, now)
    }

    // ======================================================================
    // Construction & configuration
    // ======================================================================

    /// Construct one save executor bound to a database handle.
    #[must_use]
    pub(in crate::db) const fn new(db: Db<E::Canister>, _debug: bool) -> Self {
        Self { db }
    }

    // ======================================================================
    // Single-entity save operations
    // ======================================================================

    /// Insert a brand-new entity (errors if the key already exists).
    pub(crate) fn insert(&self, entity: E) -> Result<E, InternalError> {
        self.save_entity(SaveMode::Insert, entity)
    }

    /// Create one authored typed input, preserving authored-slot provenance.
    pub(crate) fn create<I>(&self, input: I) -> Result<E, InternalError>
    where
        I: EntityCreateInput<Entity = E>,
    {
        self.save_typed_create_input(input)
    }

    /// Update an existing entity (errors if it does not exist).
    pub(crate) fn update(&self, entity: E) -> Result<E, InternalError> {
        self.save_entity(SaveMode::Update, entity)
    }

    /// Replace an entity, inserting if missing.
    pub(crate) fn replace(&self, entity: E) -> Result<E, InternalError> {
        self.save_entity(SaveMode::Replace, entity)
    }
}
