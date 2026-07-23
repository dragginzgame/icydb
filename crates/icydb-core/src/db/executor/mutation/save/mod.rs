//! Module: executor::mutation::save
//! Responsibility: save-mode execution (`insert`/`update`/`replace`) and batch lanes.
//! Does not own: relation-domain validation semantics or commit marker protocol internals.
//! Boundary: save preflight + commit-window handoff for one entity type.

mod batch;
mod shared;
mod structural;
mod typed;

#[cfg(feature = "sql")]
pub(in crate::db) use structural::StructuralMutationTargetKey;

use crate::{
    db::{
        Db,
        commit::CommitSchemaFingerprint,
        data::PersistedRow,
        schema::{AcceptedRowDecodeContract, CompiledAcceptedRowConstraints, SchemaInfo},
    },
    entity::EntityCreateInput,
    error::InternalError,
    metrics::sink::{MetricsEvent, SaveMutationKind, record},
    sanitize::{SanitizeWriteContext, SanitizeWriteMode},
    traits::AuthoredFieldProjection,
    types::Timestamp,
};

// Debug assertions below are diagnostic sentinels; correctness is enforced by
// runtime validation earlier in the pipeline.

//
// SaveMode
//
// Create  : will only insert a row if it's empty
// Replace : will change the row regardless of what was there
// Update  : will only change an existing row
//

#[derive(Clone, Copy, Debug, Default)]
enum SaveMode {
    #[default]
    Insert,
    Replace,
    Update,
}

//
// SaveExecutor
//

#[derive(Clone)]
pub(in crate::db) struct SaveExecutor<E: PersistedRow> {
    pub(in crate::db::executor::mutation) db: Db<E::Canister>,
    accepted_row_decode_contract: AcceptedRowDecodeContract,
    accepted_schema_info: SchemaInfo,
    accepted_schema_fingerprint: CommitSchemaFingerprint,
    accepted_row_constraints: CompiledAcceptedRowConstraints,
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

    const fn save_mutation_kind(self) -> SaveMutationKind {
        match self {
            Self::RequireAbsent => SaveMutationKind::Insert,
            Self::RequirePresent => SaveMutationKind::Update,
            Self::AllowAny => SaveMutationKind::Replace,
        }
    }

    const fn mutation_mode(self) -> MutationMode {
        match self {
            Self::RequireAbsent => MutationMode::Insert,
            Self::RequirePresent => MutationMode::Update,
            Self::AllowAny => MutationMode::Replace,
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

    const fn save_mutation_kind(self) -> SaveMutationKind {
        match self {
            Self::Insert => SaveMutationKind::Insert,
            Self::Replace => SaveMutationKind::Replace,
            Self::Update => SaveMutationKind::Update,
        }
    }
}

impl<E: PersistedRow> SaveExecutor<E> {
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

    /// Construct one save executor bound to accepted schema authority.
    #[must_use]
    pub(in crate::db) const fn new_with_accepted_contract(
        db: Db<E::Canister>,
        _debug: bool,
        accepted_row_decode_contract: AcceptedRowDecodeContract,
        accepted_schema_info: SchemaInfo,
        accepted_schema_fingerprint: CommitSchemaFingerprint,
        accepted_row_constraints: CompiledAcceptedRowConstraints,
    ) -> Self {
        Self {
            db,
            accepted_row_decode_contract,
            accepted_schema_info,
            accepted_schema_fingerprint,
            accepted_row_constraints,
        }
    }

    /// Construct one test save executor from generated-compatible schema facts.
    ///
    /// Production save construction must pass an accepted schema-store contract.
    /// Executor tests use this generated proposal bridge to keep low-level
    /// fixtures focused on save mechanics instead of schema-store setup.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn new(db: Db<E::Canister>, _debug: bool) -> Self {
        let proposal = crate::db::schema::compiled_schema_proposal_for_model(E::MODEL);
        let accepted = crate::db::schema::AcceptedSchemaSnapshot::try_new(
            proposal.initial_persisted_schema_snapshot(),
        )
        .expect("test save executor schema snapshot should be accepted");
        let accepted_schema_info =
            SchemaInfo::from_snapshot_with_generated_model_for_test(E::MODEL, &accepted);
        let accepted_schema_fingerprint =
            crate::db::schema::accepted_commit_schema_fingerprint(&accepted)
                .expect("test save executor schema fingerprint should derive");
        let descriptor =
            crate::db::schema::AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
                .expect("test save executor runtime contract should build");
        let (catalog, composite_catalog) =
            crate::db::schema::build_initial_accepted_catalogs_for_tests(&[E::MODEL])
                .expect("test save executor catalogs should build");
        let catalog = crate::db::schema::AcceptedValueCatalogHandle::new_for_tests(
            catalog,
            composite_catalog,
            crate::db::schema::AcceptedSchemaRevision::INITIAL,
        );
        let accepted_row_decode_contract = descriptor.row_decode_contract(catalog);
        let accepted_row_constraints = CompiledAcceptedRowConstraints::compile(
            &accepted,
            accepted_row_decode_contract.value_catalog_handle(),
            accepted_schema_fingerprint,
        )
        .expect("test accepted check program should compile");

        Self {
            db,
            accepted_row_decode_contract,
            accepted_schema_info,
            accepted_schema_fingerprint,
            accepted_row_constraints,
        }
    }

    // Borrow the accepted row contract selected by the session write boundary.
    // Save execution is no longer a dual-path generated/accepted surface: the
    // session must prove accepted schema compatibility before constructing it.
    pub(in crate::db::executor::mutation) const fn accepted_row_decode_contract(
        &self,
    ) -> &AcceptedRowDecodeContract {
        &self.accepted_row_decode_contract
    }

    // Borrow the accepted schema info selected by the session write boundary.
    // Save validation uses this schema view instead of reopening generated
    // model metadata after accepted schema compatibility has already been
    // established.
    pub(in crate::db::executor::mutation) const fn accepted_schema_info(&self) -> &SchemaInfo {
        &self.accepted_schema_info
    }

    // Borrow the accepted schema fingerprint selected by the session write
    // boundary. Commit markers emitted by save lanes use this value so replay
    // validation follows the same accepted schema snapshot as row validation.
    pub(in crate::db::executor::mutation) const fn accepted_schema_fingerprint(
        &self,
    ) -> CommitSchemaFingerprint {
        self.accepted_schema_fingerprint
    }

    // Borrow the accepted check program pinned to this save authority.
    pub(in crate::db::executor::mutation) const fn accepted_row_constraints(
        &self,
    ) -> &CompiledAcceptedRowConstraints {
        &self.accepted_row_constraints
    }

    // Record the committed save mode after the row mutation has crossed the
    // commit boundary so failed preflight attempts do not inflate write metrics.
    fn record_save_mutation(kind: SaveMutationKind, rows_touched: u64) {
        record(MetricsEvent::SaveMutation {
            entity_path: E::PATH,
            kind,
            rows_touched,
        });
    }

    // ======================================================================
    // Single-entity save operations
    // ======================================================================

    /// Insert a brand-new entity (errors if the key already exists).
    pub(in crate::db) fn insert(&self, entity: E) -> Result<E, InternalError>
    where
        E: AuthoredFieldProjection,
    {
        self.save_entity(SaveMode::Insert, entity)
    }

    /// Create one authored typed input, preserving authored-slot provenance.
    pub(in crate::db) fn create<I>(&self, input: I) -> Result<E, InternalError>
    where
        I: EntityCreateInput<Entity = E>,
        E: AuthoredFieldProjection,
    {
        self.save_typed_create_input(input)
    }

    /// Update an existing entity (errors if it does not exist).
    pub(in crate::db) fn update(&self, entity: E) -> Result<E, InternalError>
    where
        E: AuthoredFieldProjection,
    {
        self.save_entity(SaveMode::Update, entity)
    }

    /// Replace an entity, inserting if missing.
    pub(in crate::db) fn replace(&self, entity: E) -> Result<E, InternalError>
    where
        E: AuthoredFieldProjection,
    {
        self.save_entity(SaveMode::Replace, entity)
    }
}
