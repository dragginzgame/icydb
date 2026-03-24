//! Module: commit::hooks
//! Responsibility: runtime hook contracts and resolution for commit/recovery orchestration.
//! Does not own: planner semantics, executor branching, or relation invariants.
//! Boundary: db root delegates hook discovery and hook contract shape to commit.

use crate::{
    db::index::{StructuralIndexEntryReader, StructuralPrimaryRowReader},
    db::{
        Db,
        commit::{
            CommitRowOp, PreparedRowCommitOp, prepare_row_commit_for_entity,
            prepare_row_commit_for_entity_with_structural_readers,
        },
        relation::StrongRelationDeleteValidateFn,
    },
    error::InternalError,
    model::entity::EntityModel,
    traits::{CanisterKind, EntityKind, EntityValue, Path},
    types::EntityTag,
};

// Runtime hook callback used when commit preparation must read existing primary
// rows and index entries through structural reader facades.
type PrepareRowCommitWithReadersFn<C> = fn(
    &Db<C>,
    &CommitRowOp,
    &dyn StructuralPrimaryRowReader,
    &dyn StructuralIndexEntryReader,
) -> Result<PreparedRowCommitOp, InternalError>;

// Runtime hook callback used for the normal row-commit preparation path.
type PrepareRowCommitFn<C> = fn(&Db<C>, &CommitRowOp) -> Result<PreparedRowCommitOp, InternalError>;

///
/// EntityRuntimeHooks
///
/// Per-entity runtime callbacks used for commit preparation and delete-side
/// strong relation validation.
/// Keeps entity and store routing metadata alongside callback roots so runtime
/// recovery and structural preflight can resolve the right store without
/// reintroducing typed entity parameters.
///

pub struct EntityRuntimeHooks<C: CanisterKind> {
    pub(crate) entity_tag: EntityTag,
    pub(crate) model: &'static EntityModel,
    pub(crate) entity_path: &'static str,
    pub(crate) store_path: &'static str,
    pub(in crate::db) prepare_row_commit: PrepareRowCommitFn<C>,
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
        prepare_row_commit: PrepareRowCommitFn<C>,
        prepare_row_commit_with_readers: PrepareRowCommitWithReadersFn<C>,
        validate_delete_strong_relations: StrongRelationDeleteValidateFn<C>,
    ) -> Self {
        Self {
            entity_tag,
            model,
            entity_path,
            store_path,
            prepare_row_commit,
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
            prepare_row_commit_for_entity::<E>,
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
