//! Module: db::runtime_hooks
//! Responsibility: runtime entity hook contracts and lookup helpers.
//! Does not own: commit protocol, relation semantics, or executor branching.
//! Boundary: db root owns hook registration; commit/delete consume callback lanes.

use crate::{
    db::{
        Db,
        commit::{
            CommitRowOp, PreparedRowCommitOp, prepare_row_commit_for_entity_with_structural_readers,
        },
        data::RawDataKey,
        index::{StructuralIndexEntryReader, StructuralPrimaryRowReader},
        relation::{StrongRelationDeleteValidateFn, model_has_strong_relations_to_target},
    },
    error::InternalError,
    model::entity::EntityModel,
    traits::{CanisterKind, EntityKind, EntityValue, Path},
    types::EntityTag,
};
use std::collections::BTreeSet;

/// Runtime hook callback used when commit preparation must read existing
/// primary rows and index entries through structural reader facades.
pub(in crate::db) type PrepareRowCommitWithReadersFn<C> =
    fn(
        &Db<C>,
        &CommitRowOp,
        &dyn StructuralPrimaryRowReader,
        &dyn StructuralIndexEntryReader,
    ) -> Result<PreparedRowCommitOp, InternalError>;

///
/// EntityRuntimeHooks
///
/// Per-entity runtime callbacks used by commit preparation and delete-side
/// strong relation validation. The registry keeps entity and store routing
/// metadata next to callback roots so runtime recovery and structural preflight
/// can resolve typed behavior without reintroducing typed entity parameters.
///

pub struct EntityRuntimeHooks<C: CanisterKind> {
    pub(crate) entity_tag: EntityTag,
    pub(crate) model: &'static EntityModel,
    pub(crate) entity_path: &'static str,
    pub(crate) store_path: &'static str,
    pub(in crate::db) prepare_row_commit_with_readers: PrepareRowCommitWithReadersFn<C>,
    pub(crate) validate_delete_strong_relations: StrongRelationDeleteValidateFn<C>,
}

impl<C: CanisterKind> EntityRuntimeHooks<C> {
    /// Build one runtime hook contract for a concrete runtime entity.
    #[must_use]
    pub(in crate::db) const fn new(
        entity_tag: EntityTag,
        model: &'static EntityModel,
        entity_path: &'static str,
        store_path: &'static str,
        prepare_row_commit_with_readers: PrepareRowCommitWithReadersFn<C>,
        validate_delete_strong_relations: StrongRelationDeleteValidateFn<C>,
    ) -> Self {
        Self {
            entity_tag,
            model,
            entity_path,
            store_path,
            prepare_row_commit_with_readers,
            validate_delete_strong_relations,
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
            prepare_row_commit_for_entity_with_structural_readers::<E>,
            crate::db::relation::validate_delete_strong_relations_for_source::<E>,
        )
    }
}

/// Return whether this db has any registered runtime hook callbacks.
#[must_use]
pub(in crate::db) const fn has_runtime_hooks<C: CanisterKind>(
    entity_runtime_hooks: &[EntityRuntimeHooks<C>],
) -> bool {
    !entity_runtime_hooks.is_empty()
}

/// Validate that each runtime hook owns one unique entity tag.
///
/// This runs only in debug builds at hook table construction time so duplicate
/// registrations fail before runtime dispatch begins.
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
                panic!("duplicate EntityTag detected in runtime hooks");
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

    (hooks.prepare_row_commit_with_readers)(db, op, &store, &store)
}

/// Validate delete-side strong relation constraints through runtime hooks.
pub(in crate::db) fn validate_delete_strong_relations_with_hooks<C: CanisterKind>(
    db: &Db<C>,
    entity_runtime_hooks: &[EntityRuntimeHooks<C>],
    target_path: &str,
    deleted_target_keys: &BTreeSet<RawDataKey>,
) -> Result<(), InternalError> {
    // Skip hook traversal when no target keys were deleted.
    if deleted_target_keys.is_empty() {
        return Ok(());
    }

    // Delegate delete-side relation validation to each entity runtime hook.
    for hooks in entity_runtime_hooks {
        if !model_has_strong_relations_to_target(hooks.model, target_path) {
            continue;
        }

        (hooks.validate_delete_strong_relations)(db, target_path, deleted_target_keys)?;
    }

    Ok(())
}
