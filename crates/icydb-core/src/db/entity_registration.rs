//! Module: db::entity_registration
//! Responsibility: generated proposal registration and model-free runtime
//! entity routing.
//! Does not own: accepted schema authority, commit semantics, or relation
//! validation.
//! Boundary: generated wiring registers one proposal/runtime pair; runtime
//! consumers receive only the model-free routing half and must load accepted
//! catalog authority before use.

use crate::{
    db::{
        Db,
        commit::{
            CommitPrepareContext, CommitRowOp, CommitSchemaFingerprint, PreparedRowCommitOp,
            prepare_commit_context_for_runtime_registration, prepare_row_commit_with_context,
        },
        data::RawDataStoreKey,
    },
    entity::EntityKind,
    error::InternalError,
    model::entity::EntityModel,
    traits::{CanisterKind, Path},
    types::EntityTag,
};
use std::{collections::BTreeSet, marker::PhantomData};

///
/// EntitySchemaProposal
///
/// Proposal-only generated entity metadata consumed by schema reconciliation.
/// Accepted runtime code cannot construct or receive this type.
///

pub(in crate::db) struct EntitySchemaProposal<C: CanisterKind> {
    pub(in crate::db) entity_tag: EntityTag,
    pub(in crate::db) model: &'static EntityModel,
    pub(in crate::db) entity_path: &'static str,
    pub(in crate::db) store_path: &'static str,
    _marker: PhantomData<C>,
}

impl<C: CanisterKind> EntitySchemaProposal<C> {
    const fn new(
        entity_tag: EntityTag,
        model: &'static EntityModel,
        entity_path: &'static str,
        store_path: &'static str,
    ) -> Self {
        Self {
            entity_tag,
            model,
            entity_path,
            store_path,
            _marker: PhantomData,
        }
    }
}

///
/// EntityRuntimeRegistration
///
/// Model-free entity routing facts supplied by generated canister wiring.
/// These facts select a store-local accepted catalog; they are never semantic
/// authority by themselves.
///

pub(in crate::db) struct EntityRuntimeRegistration<C: CanisterKind> {
    pub(in crate::db) entity_tag: EntityTag,
    pub(in crate::db) entity_path: &'static str,
    pub(in crate::db) store_path: &'static str,
    _marker: PhantomData<C>,
}

impl<C: CanisterKind> EntityRuntimeRegistration<C> {
    const fn new(
        entity_tag: EntityTag,
        entity_path: &'static str,
        store_path: &'static str,
    ) -> Self {
        Self {
            entity_tag,
            entity_path,
            store_path,
            _marker: PhantomData,
        }
    }

    /// Resolve accepted commit authority for this registered entity.
    pub(in crate::db) fn prepare_commit_context(
        self,
        db: &Db<C>,
        schema_fingerprint: CommitSchemaFingerprint,
        include_candidate_relation_effects: bool,
    ) -> Result<CommitPrepareContext, InternalError> {
        prepare_commit_context_for_runtime_registration(
            db,
            self.entity_path,
            self.entity_tag,
            self.store_path,
            schema_fingerprint,
            include_candidate_relation_effects,
        )
    }
}

impl<C: CanisterKind> Clone for EntityRuntimeRegistration<C> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<C: CanisterKind> Copy for EntityRuntimeRegistration<C> {}

///
/// EntityRegistration
///
/// Generated canister wiring pair. Schema reconciliation consumes only the
/// proposal half; accepted runtime routing consumes only the model-free half.
///

pub struct EntityRegistration<C: CanisterKind> {
    proposal: EntitySchemaProposal<C>,
    runtime: EntityRuntimeRegistration<C>,
}

impl<C: CanisterKind> EntityRegistration<C> {
    /// Build one generated registration without executable application hooks.
    #[must_use]
    pub(in crate::db) const fn new(
        entity_tag: EntityTag,
        model: &'static EntityModel,
        entity_path: &'static str,
        store_path: &'static str,
    ) -> Self {
        Self {
            proposal: EntitySchemaProposal::new(entity_tag, model, entity_path, store_path),
            runtime: EntityRuntimeRegistration::new(entity_tag, entity_path, store_path),
        }
    }

    /// Build proposal and runtime registration from one generated entity type.
    #[must_use]
    pub const fn for_entity<E>() -> Self
    where
        E: EntityKind<Canister = C>,
    {
        Self::new(E::ENTITY_TAG, E::MODEL, E::PATH, E::Store::PATH)
    }

    /// Borrow proposal-only metadata for schema reconciliation.
    #[must_use]
    pub(in crate::db) const fn proposal(&self) -> &EntitySchemaProposal<C> {
        &self.proposal
    }

    /// Copy model-free runtime routing facts.
    #[must_use]
    pub(in crate::db) const fn runtime(&self) -> EntityRuntimeRegistration<C> {
        self.runtime
    }
}

/// Validate that each registration owns one unique entity tag.
///
/// This runs only in debug builds at table construction time so duplicate
/// generated wiring fails before runtime dispatch begins.
///
/// # Panics
///
/// Panics when two registrations declare the same entity tag.
#[must_use]
#[cfg(debug_assertions)]
pub(in crate::db) const fn debug_assert_unique_entity_registrations<C: CanisterKind>(
    registrations: &[EntityRegistration<C>],
) -> bool {
    let mut i = 0usize;
    while i < registrations.len() {
        let mut j = i + 1;
        while j < registrations.len() {
            let left = registrations[i].runtime();
            let right = registrations[j].runtime();
            assert!(
                left.entity_tag.value() != right.entity_tag.value(),
                "entity registration invariant"
            );
            j += 1;
        }
        i += 1;
    }

    true
}

/// Resolve exactly one model-free runtime registration by entity tag.
pub(in crate::db) fn resolve_runtime_registration_by_tag<C: CanisterKind>(
    registrations: &[EntityRegistration<C>],
    entity_tag: EntityTag,
) -> Result<EntityRuntimeRegistration<C>, InternalError> {
    let mut matched = None;
    for registration in registrations {
        let runtime = registration.runtime();
        if runtime.entity_tag != entity_tag {
            continue;
        }
        if matched.is_some() {
            return Err(InternalError::duplicate_entity_registrations_for_tag(
                entity_tag,
            ));
        }
        matched = Some(runtime);
    }

    matched.ok_or_else(|| InternalError::unsupported_entity_tag_in_data_store(entity_tag))
}

/// Resolve exactly one model-free runtime registration by entity path.
pub(in crate::db) fn resolve_runtime_registration_by_path<C: CanisterKind>(
    registrations: &[EntityRegistration<C>],
    entity_path: &str,
) -> Result<EntityRuntimeRegistration<C>, InternalError> {
    let mut matched = None;
    for registration in registrations {
        let runtime = registration.runtime();
        if runtime.entity_path != entity_path {
            continue;
        }
        if matched.is_some() {
            return Err(InternalError::duplicate_entity_registrations_for_path(
                entity_path,
            ));
        }
        matched = Some(runtime);
    }

    matched.ok_or_else(|| InternalError::unsupported_entity_path(entity_path))
}

/// Prepare one row commit through model-free runtime routing.
pub(in crate::db) fn prepare_row_commit_with_registration<C: CanisterKind>(
    db: &Db<C>,
    registrations: &[EntityRegistration<C>],
    op: &CommitRowOp,
) -> Result<PreparedRowCommitOp, InternalError> {
    let runtime = resolve_runtime_registration_by_path(registrations, op.entity_path.as_ref())?;
    let store = db.store_handle(runtime.store_path)?;
    let context = runtime.prepare_commit_context(db, op.schema_fingerprint, true)?;

    prepare_row_commit_with_context(db, op, &context, &store, &store)
}

/// Prepare one recovery-rebuild row without live candidate effects.
pub(in crate::db) fn prepare_row_commit_with_registration_for_rebuild<C: CanisterKind>(
    db: &Db<C>,
    registrations: &[EntityRegistration<C>],
    op: &CommitRowOp,
) -> Result<PreparedRowCommitOp, InternalError> {
    let runtime = resolve_runtime_registration_by_path(registrations, op.entity_path.as_ref())?;
    let store = db.store_handle(runtime.store_path)?;
    let context = runtime.prepare_commit_context(db, op.schema_fingerprint, false)?;

    prepare_row_commit_with_context(db, op, &context, &store, &store)
}

/// Validate delete-side relation constraints through accepted source schemas.
pub(in crate::db) fn validate_delete_relations_with_registrations<C: CanisterKind>(
    db: &Db<C>,
    registrations: &[EntityRegistration<C>],
    target_path: &str,
    deleted_target_keys: &BTreeSet<RawDataStoreKey>,
) -> Result<(), InternalError> {
    if deleted_target_keys.is_empty() {
        return Ok(());
    }

    crate::db::relation::validate_candidate_relation_target_delete_barrier(
        db,
        target_path,
        deleted_target_keys,
    )?;

    for registration in registrations {
        let runtime = registration.runtime();
        let source_store = db.store_handle(runtime.store_path)?;
        if !source_store.with_schema(|schema_store| {
            schema_store.entity_has_relation_to_target(runtime.entity_tag, target_path)
        })? {
            continue;
        }
        crate::db::relation::validate_delete_relations_for_registered_source(
            db,
            runtime.entity_tag,
            runtime.entity_path,
            runtime.store_path,
            target_path,
            deleted_target_keys,
        )?;
    }

    Ok(())
}
