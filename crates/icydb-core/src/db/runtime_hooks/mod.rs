//! Module: db::runtime_hooks
//! Responsibility: runtime entity hook contracts and lookup helpers.
//! Does not own: commit protocol, relation semantics, or executor branching.
//! Boundary: db root owns hook registration; commit/delete consume callback lanes.

use crate::{
    db::{
        Db,
        commit::{
            CommitPrepareContext, CommitRowOp, CommitSchemaFingerprint, PreparedRowCommitOp,
            prepare_commit_context_for_runtime_entity,
            prepare_commit_context_for_runtime_entity_rebuild, prepare_row_commit_with_context,
        },
        data::RawDataStoreKey,
        relation::RelationDeleteValidateFn,
    },
    entity::{EntityKind, EntityValue},
    error::InternalError,
    model::entity::EntityModel,
    traits::{CanisterKind, Path},
    types::EntityTag,
};
use std::collections::BTreeSet;

///
/// EntityRuntimeHooks
///
/// Per-entity runtime callbacks used by commit preparation and delete-side
/// relation validation. The registry keeps entity and store routing
/// metadata next to callback roots so runtime recovery and structural preflight
/// can resolve typed behavior without reintroducing typed entity parameters.
///

pub struct EntityRuntimeHooks<C: CanisterKind> {
    pub(in crate::db) entity_tag: EntityTag,
    pub(in crate::db) model: &'static EntityModel,
    pub(in crate::db) entity_path: &'static str,
    pub(in crate::db) store_path: &'static str,
    pub(in crate::db) validate_delete_relations: RelationDeleteValidateFn<C>,
}

impl<C: CanisterKind> EntityRuntimeHooks<C> {
    /// Build one runtime hook contract for a concrete runtime entity.
    #[must_use]
    pub(in crate::db) const fn new(
        entity_tag: EntityTag,
        model: &'static EntityModel,
        entity_path: &'static str,
        store_path: &'static str,
        validate_delete_relations: RelationDeleteValidateFn<C>,
    ) -> Self {
        Self {
            entity_tag,
            model,
            entity_path,
            store_path,
            validate_delete_relations,
        }
    }

    /// Build runtime hooks from one entity type.
    #[must_use]
    pub const fn for_entity<E>() -> Self
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Self::new(
            E::ENTITY_TAG,
            E::MODEL,
            E::PATH,
            E::Store::PATH,
            crate::db::relation::validate_delete_relations_for_source::<E>,
        )
    }

    /// Resolve accepted commit authority once for a batch targeting this entity.
    pub(in crate::db) fn prepare_commit_context(
        &self,
        db: &Db<C>,
        schema_fingerprint: CommitSchemaFingerprint,
    ) -> Result<CommitPrepareContext, InternalError> {
        prepare_commit_context_for_runtime_entity(
            db,
            self.entity_path,
            self.entity_tag,
            self.store_path,
            self.model,
            schema_fingerprint,
        )
    }

    /// Resolve accepted commit authority for a complete recovery rebuild.
    pub(in crate::db) fn prepare_rebuild_commit_context(
        &self,
        db: &Db<C>,
        schema_fingerprint: CommitSchemaFingerprint,
    ) -> Result<CommitPrepareContext, InternalError> {
        prepare_commit_context_for_runtime_entity_rebuild(
            db,
            self.entity_path,
            self.entity_tag,
            self.store_path,
            self.model,
            schema_fingerprint,
        )
    }
}

/// Validate that each runtime hook owns one unique entity tag.
///
/// This runs only in debug builds at hook table construction time so duplicate
/// registrations fail before runtime dispatch begins.
///
/// # Panics
///
/// Panics when two runtime hooks declare the same entity tag.
#[must_use]
#[cfg(debug_assertions)]
pub(in crate::db) const fn debug_assert_unique_runtime_hook_tags<C: CanisterKind>(
    entity_runtime_hooks: &[EntityRuntimeHooks<C>],
) -> bool {
    let mut i = 0usize;
    while i < entity_runtime_hooks.len() {
        let mut j = i + 1;
        while j < entity_runtime_hooks.len() {
            if entity_runtime_hooks[i].entity_tag.value()
                == entity_runtime_hooks[j].entity_tag.value()
            {
                panic!("runtime hook invariant");
            }
            j += 1;
        }
        i += 1;
    }

    true
}

/// Resolve exactly one runtime hook for a persisted `EntityTag`.
/// Duplicate matches are treated as store invariants.
pub(in crate::db) fn resolve_runtime_hook_by_tag<C: CanisterKind>(
    entity_runtime_hooks: &[EntityRuntimeHooks<C>],
    entity_tag: EntityTag,
) -> Result<&EntityRuntimeHooks<C>, InternalError> {
    let mut matched = None;
    for hooks in entity_runtime_hooks {
        if hooks.entity_tag != entity_tag {
            continue;
        }

        if matched.is_some() {
            return Err(InternalError::duplicate_runtime_hooks_for_entity_tag(
                entity_tag,
            ));
        }

        matched = Some(hooks);
    }

    matched.ok_or_else(|| InternalError::unsupported_entity_tag_in_data_store(entity_tag))
}

/// Resolve exactly one runtime hook for a persisted entity path.
/// Duplicate matches are treated as store invariants.
pub(in crate::db) fn resolve_runtime_hook_by_path<'a, C: CanisterKind>(
    entity_runtime_hooks: &'a [EntityRuntimeHooks<C>],
    entity_path: &str,
) -> Result<&'a EntityRuntimeHooks<C>, InternalError> {
    let mut matched = None;
    for hooks in entity_runtime_hooks {
        if hooks.entity_path != entity_path {
            continue;
        }

        if matched.is_some() {
            return Err(InternalError::duplicate_runtime_hooks_for_entity_path(
                entity_path,
            ));
        }

        matched = Some(hooks);
    }

    matched.ok_or_else(|| InternalError::unsupported_entity_path(entity_path))
}

/// Prepare one row commit op through the runtime hook registry.
pub(in crate::db) fn prepare_row_commit_with_hook<C: CanisterKind>(
    db: &Db<C>,
    entity_runtime_hooks: &[EntityRuntimeHooks<C>],
    op: &CommitRowOp,
) -> Result<PreparedRowCommitOp, InternalError> {
    let hooks = resolve_runtime_hook_by_path(entity_runtime_hooks, op.entity_path.as_ref())?;
    let store = db.store_handle(hooks.store_path)?;
    let context = hooks.prepare_commit_context(db, op.schema_fingerprint)?;

    prepare_row_commit_with_context(db, op, &context, &store, &store)
}

/// Prepare one recovery-rebuild row without replaying live candidate effects.
pub(in crate::db) fn prepare_row_commit_with_hook_for_rebuild<C: CanisterKind>(
    db: &Db<C>,
    entity_runtime_hooks: &[EntityRuntimeHooks<C>],
    op: &CommitRowOp,
) -> Result<PreparedRowCommitOp, InternalError> {
    let hooks = resolve_runtime_hook_by_path(entity_runtime_hooks, op.entity_path.as_ref())?;
    let store = db.store_handle(hooks.store_path)?;
    let context = hooks.prepare_rebuild_commit_context(db, op.schema_fingerprint)?;

    prepare_row_commit_with_context(db, op, &context, &store, &store)
}

/// Validate delete-side relation constraints through runtime hooks.
pub(in crate::db) fn validate_delete_relations_with_hooks<C: CanisterKind>(
    db: &Db<C>,
    entity_runtime_hooks: &[EntityRuntimeHooks<C>],
    target_path: &str,
    deleted_target_keys: &BTreeSet<RawDataStoreKey>,
) -> Result<(), InternalError> {
    // Skip hook traversal when no target keys were deleted.
    if deleted_target_keys.is_empty() {
        return Ok(());
    }

    crate::db::relation::validate_candidate_relation_target_delete_barrier(
        db,
        target_path,
        deleted_target_keys,
    )?;

    // Consult accepted catalog authority before entering a typed hook so
    // unrelated entities do not rebuild row contracts during every delete.
    for hooks in entity_runtime_hooks {
        let source_store = db.store_handle(hooks.store_path)?;
        if !source_store.with_schema(|schema_store| {
            schema_store.entity_has_relation_to_target(hooks.entity_tag, target_path)
        })? {
            continue;
        }
        (hooks.validate_delete_relations)(db, target_path, deleted_target_keys)?;
    }

    Ok(())
}
