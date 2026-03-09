//! Module: commit::hooks
//! Responsibility: runtime hook contracts and resolution for commit/recovery orchestration.
//! Does not own: planner semantics, executor branching, or relation invariants.
//! Boundary: db root delegates hook discovery and hook contract shape to commit.

use crate::{
    db::{
        Db,
        commit::{
            CommitRowOp, CommitSchemaFingerprint, PreparedRowCommitOp,
            commit_schema_fingerprint_for_entity, prepare_row_commit_for_entity,
        },
        relation::StrongRelationDeleteValidateFn,
    },
    error::InternalError,
    traits::{CanisterKind, EntityIdentity, EntityKind, EntityValue},
};

///
/// EntityRuntimeHooks
///
/// Per-entity runtime callbacks used for commit preparation and delete-side
/// strong relation validation.
///

pub struct EntityRuntimeHooks<C: CanisterKind> {
    pub(crate) entity_name: &'static str,
    pub(crate) entity_path: &'static str,
    pub(in crate::db) commit_schema_fingerprint: fn() -> CommitSchemaFingerprint,
    pub(in crate::db) prepare_row_commit:
        fn(&Db<C>, &CommitRowOp) -> Result<PreparedRowCommitOp, InternalError>,
    pub(crate) validate_delete_strong_relations: StrongRelationDeleteValidateFn<C>,
}

impl<C: CanisterKind> EntityRuntimeHooks<C> {
    /// Build one runtime hook contract for a concrete runtime entity.
    #[must_use]
    pub(in crate::db) const fn new(
        entity_name: &'static str,
        entity_path: &'static str,
        commit_schema_fingerprint: fn() -> CommitSchemaFingerprint,
        prepare_row_commit: fn(&Db<C>, &CommitRowOp) -> Result<PreparedRowCommitOp, InternalError>,
        validate_delete_strong_relations: StrongRelationDeleteValidateFn<C>,
    ) -> Self {
        Self {
            entity_name,
            entity_path,
            commit_schema_fingerprint,
            prepare_row_commit,
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
            <E as EntityIdentity>::ENTITY_NAME,
            E::PATH,
            commit_schema_fingerprint_for_runtime_entity::<E>,
            prepare_row_commit_for_entity::<E>,
            crate::db::relation::validate_delete_strong_relations_for_source::<E>,
        )
    }
}

fn commit_schema_fingerprint_for_runtime_entity<E>() -> CommitSchemaFingerprint
where
    E: EntityKind,
{
    commit_schema_fingerprint_for_entity::<E>()
}

/// Return whether this db has any registered runtime hook callbacks.
#[must_use]
pub(in crate::db) const fn has_runtime_hooks<C: CanisterKind>(
    entity_runtime_hooks: &[EntityRuntimeHooks<C>],
) -> bool {
    !entity_runtime_hooks.is_empty()
}

/// Resolve exactly one runtime hook for a persisted entity name.
/// Duplicate matches are treated as store invariants.
pub(in crate::db) fn resolve_runtime_hook_by_name<'a, C: CanisterKind>(
    entity_runtime_hooks: &'a [EntityRuntimeHooks<C>],
    entity_name: &str,
) -> Result<&'a EntityRuntimeHooks<C>, InternalError> {
    let mut matched = None;
    for hooks in entity_runtime_hooks {
        if hooks.entity_name != entity_name {
            continue;
        }

        if matched.is_some() {
            return Err(InternalError::store_invariant(format!(
                "duplicate runtime hooks for entity name '{entity_name}'"
            )));
        }

        matched = Some(hooks);
    }

    matched.ok_or_else(|| {
        InternalError::store_unsupported(format!(
            "unsupported entity name in data store: '{entity_name}'"
        ))
    })
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
            return Err(InternalError::store_invariant(format!(
                "duplicate runtime hooks for entity path '{entity_path}'"
            )));
        }

        matched = Some(hooks);
    }

    matched.ok_or_else(|| InternalError::unsupported_entity_path(entity_path))
}
