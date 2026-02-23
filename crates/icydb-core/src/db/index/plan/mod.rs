mod commit_ops;
mod load;
mod unique;

use crate::{
    db::{
        Db,
        commit::CommitIndexOp,
        index::{IndexEntryCorruption, IndexKey, IndexStore},
    },
    error::InternalError,
    model::index::IndexModel,
    traits::{EntityKind, EntityValue},
};
use std::{cell::RefCell, thread::LocalKey};

///
/// IndexApplyPlan
///

#[derive(Debug)]
pub(crate) struct IndexApplyPlan {
    pub index: &'static IndexModel,
    pub store: &'static LocalKey<RefCell<IndexStore>>,
}

///
/// IndexMutationPlan
///

#[derive(Debug)]
pub(crate) struct IndexMutationPlan {
    pub apply: Vec<IndexApplyPlan>,
    pub commit_ops: Vec<CommitIndexOp>,
}

/// Plan all index mutations for a single entity transition.
///
/// This function:
/// - Loads existing index entries
/// - Validates unique constraints
/// - Computes the exact index writes/deletes required
///
/// All fallible work happens here. The returned plan is safe to apply
/// infallibly after a commit marker is written.
pub(in crate::db) fn plan_index_mutation_for_entity<E: EntityKind + EntityValue>(
    db: &Db<E::Canister>,
    old: Option<&E>,
    new: Option<&E>,
) -> Result<IndexMutationPlan, InternalError> {
    let old_entity_key = old.map(|entity| entity.id().key());
    let new_entity_key = new.map(|entity| entity.id().key());

    let mut apply = Vec::with_capacity(E::INDEXES.len());
    let mut commit_ops = Vec::new();

    for index in E::INDEXES {
        let store = db
            .with_store_registry(|registry| registry.try_get_store(index.store))?
            .index_store();

        let old_key = match old {
            Some(entity) => IndexKey::new(entity, index)?,
            None => None,
        };
        let new_key = match new {
            Some(entity) => IndexKey::new(entity, index)?,
            None => None,
        };

        let old_entry = load::load_existing_entry(store, index, old)?;

        // Prevalidate membership so commit-phase mutations cannot surface corruption.
        if let Some(old_key) = &old_key {
            let Some(old_entity_key) = old_entity_key else {
                return Err(InternalError::index_internal(
                    "missing old entity key for index removal".to_string(),
                ));
            };

            let entry = old_entry.as_ref().ok_or_else(|| {
                InternalError::index_plan_index_corruption(format!(
                    "index corrupted: {} ({}) -> {}",
                    E::PATH,
                    index.fields.join(", "),
                    IndexEntryCorruption::missing_key(old_key.to_raw(), old_entity_key)
                ))
            })?;

            if index.unique && entry.len() > 1 {
                return Err(InternalError::index_plan_index_corruption(format!(
                    "index corrupted: {} ({}) -> {}",
                    E::PATH,
                    index.fields.join(", "),
                    IndexEntryCorruption::NonUniqueEntry { keys: entry.len() }
                )));
            }

            if !entry.contains(old_entity_key) {
                return Err(InternalError::index_plan_index_corruption(format!(
                    "index corrupted: {} ({}) -> {}",
                    E::PATH,
                    index.fields.join(", "),
                    IndexEntryCorruption::missing_key(old_key.to_raw(), old_entity_key)
                )));
            }
        }

        let new_entry = if old_key == new_key {
            old_entry.clone()
        } else {
            load::load_existing_entry(store, index, new)?
        };

        // Unique validation is evaluated against the currently committed store
        // state for the target unique value. Commit-op synthesis then applies
        // remove-old/add-new semantics, so valid key transitions are evaluated
        // on the correct post-transition logical ownership model.
        unique::validate_unique_constraint::<E>(db, index, new_entity_key.as_ref(), new)?;

        commit_ops::build_commit_ops_for_index::<E>(
            &mut commit_ops,
            index,
            old_key,
            new_key,
            old_entry,
            new_entry,
            old_entity_key,
            new_entity_key,
        )?;

        apply.push(IndexApplyPlan { index, store });
    }

    Ok(IndexMutationPlan { apply, commit_ops })
}
