//! Module: db::entity_registration
//! Responsibility: generated model-free runtime entity routing.
//! Does not own: accepted schema authority, schema proposals, commit semantics,
//! or relation validation.
//! Boundary: generated wiring names immutable entity/store routes; consumers
//! resolve accepted identity from persisted source bindings before use.

use crate::{
    db::{
        Db,
        commit::{
            CommitRowOp, PreparedRowCommitOp, prepare_commit_context_for_runtime_registration,
            prepare_row_commit_with_context,
        },
        data::RawDataStoreKey,
    },
    error::InternalError,
    traits::CanisterKind,
    types::EntityTag,
};
use std::{collections::BTreeSet, marker::PhantomData};

///
/// GeneratedEntityRoute
///
/// Model-free source and routing facts supplied by generated canister wiring.
/// These facts select a store-local accepted source-binding catalog; they are
/// never accepted identity or semantic authority by themselves.
///

pub(in crate::db) struct GeneratedEntityRoute<C: CanisterKind> {
    pub(in crate::db) source_key: &'static str,
    pub(in crate::db) entity_path: &'static str,
    pub(in crate::db) store_path: &'static str,
    _marker: PhantomData<C>,
}

impl<C: CanisterKind> GeneratedEntityRoute<C> {
    const fn new(
        source_key: &'static str,
        entity_path: &'static str,
        store_path: &'static str,
    ) -> Self {
        Self {
            source_key,
            entity_path,
            store_path,
            _marker: PhantomData,
        }
    }

    /// Resolve the accepted entity identity from this store's source-binding
    /// authority.
    pub(in crate::db) fn accepted_entity_tag(self, db: &Db<C>) -> Result<EntityTag, InternalError> {
        let source = icydb_schema::EntitySourceKey::try_new(self.source_key)
            .map_err(|_| InternalError::store_invariant())?;
        let store = db.store_handle(self.store_path)?;
        let bundle = store
            .with_schema(crate::db::schema::SchemaStore::current_accepted_schema_bundle)?
            .ok_or_else(InternalError::store_corruption)?;

        bundle
            .source_bindings()
            .entity(&source)
            .ok_or_else(InternalError::store_corruption)
    }

    /// Resolve this authored route into current accepted runtime identity.
    pub(in crate::db) fn resolve(
        self,
        db: &Db<C>,
    ) -> Result<EntityRuntimeRegistration<C>, InternalError> {
        Ok(EntityRuntimeRegistration {
            entity_tag: self.accepted_entity_tag(db)?,
            entity_path: self.entity_path,
            store_path: self.store_path,
            _marker: PhantomData,
        })
    }
}

impl<C: CanisterKind> Clone for GeneratedEntityRoute<C> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<C: CanisterKind> Copy for GeneratedEntityRoute<C> {}

///
/// EntityRuntimeRegistration
///
/// One generated source route joined to current persisted accepted identity.
///

pub(in crate::db) struct EntityRuntimeRegistration<C: CanisterKind> {
    pub(in crate::db) entity_tag: EntityTag,
    pub(in crate::db) entity_path: &'static str,
    pub(in crate::db) store_path: &'static str,
    _marker: PhantomData<C>,
}

impl<C: CanisterKind> Clone for EntityRuntimeRegistration<C> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<C: CanisterKind> Copy for EntityRuntimeRegistration<C> {}

impl<C: CanisterKind> EntityRuntimeRegistration<C> {
    /// Resolve accepted commit authority for this already-bound entity route.
    pub(in crate::db) fn prepare_commit_context(
        self,
        db: &Db<C>,
        schema_fingerprint: crate::db::commit::CommitSchemaFingerprint,
        include_candidate_relation_effects: bool,
    ) -> Result<crate::db::commit::CommitPrepareContext, InternalError> {
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

///
/// EntityRegistration
///
/// Generated canister wiring for one immutable authored source route.
///

pub struct EntityRegistration<C: CanisterKind> {
    runtime: GeneratedEntityRoute<C>,
}

impl<C: CanisterKind> EntityRegistration<C> {
    /// Build one generated source route without accepted identity or
    /// executable application hooks.
    #[must_use]
    pub const fn new(
        source_key: &'static str,
        entity_path: &'static str,
        store_path: &'static str,
    ) -> Self {
        Self {
            runtime: GeneratedEntityRoute::new(source_key, entity_path, store_path),
        }
    }

    /// Copy model-free runtime routing facts.
    #[must_use]
    pub(in crate::db) const fn runtime(&self) -> GeneratedEntityRoute<C> {
        self.runtime
    }
}

/// Resolve exactly one model-free runtime registration by entity tag.
pub(in crate::db) fn resolve_runtime_registration_by_tag<C: CanisterKind>(
    db: &Db<C>,
    registrations: &[EntityRegistration<C>],
    entity_tag: EntityTag,
) -> Result<EntityRuntimeRegistration<C>, InternalError> {
    let mut matched = None;
    for registration in registrations {
        let runtime = registration.runtime();
        let resolved = runtime.resolve(db)?;
        if resolved.entity_tag != entity_tag {
            continue;
        }
        if matched.is_some() {
            return Err(InternalError::duplicate_entity_registrations_for_tag(
                entity_tag,
            ));
        }
        matched = Some(resolved);
    }

    matched.ok_or_else(|| InternalError::unsupported_entity_tag_in_data_store(entity_tag))
}

/// Resolve exactly one model-free runtime registration by entity path.
pub(in crate::db) fn resolve_runtime_registration_by_path<C: CanisterKind>(
    db: &Db<C>,
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
        matched = Some(runtime.resolve(db)?);
    }

    matched.ok_or_else(|| InternalError::unsupported_entity_path(entity_path))
}

/// Prepare one row commit through model-free runtime routing.
pub(in crate::db) fn prepare_row_commit_with_registration<C: CanisterKind>(
    db: &Db<C>,
    registrations: &[EntityRegistration<C>],
    op: &CommitRowOp,
) -> Result<PreparedRowCommitOp, InternalError> {
    let runtime = resolve_runtime_registration_by_path(db, registrations, op.entity_path.as_ref())?;
    let store = db.store_handle(runtime.store_path)?;
    let context = prepare_commit_context_for_runtime_registration(
        db,
        runtime.entity_path,
        runtime.entity_tag,
        runtime.store_path,
        op.schema_fingerprint,
        true,
    )?;

    prepare_row_commit_with_context(db, op, &context, &store, &store)
}

/// Prepare one recovery-rebuild row without live candidate effects.
pub(in crate::db) fn prepare_row_commit_with_registration_for_rebuild<C: CanisterKind>(
    db: &Db<C>,
    registrations: &[EntityRegistration<C>],
    op: &CommitRowOp,
) -> Result<PreparedRowCommitOp, InternalError> {
    let runtime = resolve_runtime_registration_by_path(db, registrations, op.entity_path.as_ref())?;
    let store = db.store_handle(runtime.store_path)?;
    let context = prepare_commit_context_for_runtime_registration(
        db,
        runtime.entity_path,
        runtime.entity_tag,
        runtime.store_path,
        op.schema_fingerprint,
        false,
    )?;

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
        let entity_tag = runtime.accepted_entity_tag(db)?;
        let source_store = db.store_handle(runtime.store_path)?;
        if !source_store.with_schema(|schema_store| {
            schema_store.entity_has_relation_to_target(entity_tag, target_path)
        })? {
            continue;
        }
        crate::db::relation::validate_delete_relations_for_registered_source(
            db,
            entity_tag,
            runtime.entity_path,
            runtime.store_path,
            target_path,
            deleted_target_keys,
        )?;
    }

    Ok(())
}
