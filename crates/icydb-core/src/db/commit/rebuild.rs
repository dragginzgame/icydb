//! Module: db::commit::rebuild
//! Responsibility: rebuild secondary indexes from authoritative persisted rows.
//! Does not own: marker-bound journal publication, commit-marker persistence, or query planning.
//! Boundary: commit::recovery -> commit::rebuild -> commit::{prepare,apply} (one-way).

#[cfg(test)]
use crate::db::commit::failpoint::{CommitFailpoint, hit_commit_failpoint};
use crate::{
    db::{
        Db,
        commit::{CommitRowOp, CommitSchemaFingerprint},
        data::{
            AcceptedStructuralRowAuthority, DataStore, DecodedDataStoreKey, StoreVisit,
            StructuralRowContract, StructuralSlotReader,
        },
        index::{IndexEntryValue, IndexStore},
        registry::{StoreHandle, StoreRecoveryCapability},
        relation::{RelationConstraintProjection, ReverseRelationSourceInfo},
        schema::{
            AcceptedSchemaSnapshot, ConstraintActivationKind, ConstraintActivationState,
            ConstraintValidationJob, UniqueConstraintProjection,
            accepted_commit_schema_fingerprint, ensure_accepted_schema_snapshot,
        },
    },
    error::InternalError,
    traits::CanisterKind,
    types::EntityTag,
};
use std::collections::{BTreeMap, btree_map::Entry};

///
/// RebuildEntityAuthority
///
/// Accepted per-entity facts reused while one recovery scan rebuilds indexes.
/// Recovery owns this transient cache; rows never reconstruct schema identity.
///

struct RebuildEntityAuthority {
    entity_path: &'static str,
    schema_fingerprint: CommitSchemaFingerprint,
    candidate_unique: Option<RebuildCandidateUniqueAuthority>,
    candidate_relation: Option<RebuildCandidateRelationAuthority>,
}

/// Candidate projection and durable checkpoint used to reconstruct only the
/// prefix that Forward had published, or the complete generation in Verify.

struct RebuildCandidateUniqueAuthority {
    contract: StructuralRowContract,
    projection: UniqueConstraintProjection,
    job: ConstraintValidationJob,
}

/// Candidate reverse projection and durable checkpoint reconstructed by recovery.

struct RebuildCandidateRelationAuthority {
    contract: StructuralRowContract,
    projection: RelationConstraintProjection,
    job: ConstraintValidationJob,
}

/// Rebuild all secondary indexes from authoritative data rows.
///
/// Invariant: row stores are the source of truth; index stores are fully
/// derived and can be recreated exactly from persisted rows.
pub(in crate::db) fn rebuild_secondary_indexes_from_rows(
    db: &Db<impl CanisterKind>,
) -> Result<(), InternalError> {
    // Derived indexes have one recovery direction: clear and rebuild from the
    // accepted schema plus authoritative rows. Failure leaves the store
    // non-Ready so guarded retry starts forward from another complete clear.
    let stores = sorted_store_handles(db);
    match rebuild_secondary_indexes_in_place(db, &stores) {
        Ok(()) => Ok(()),
        Err(error) => {
            // Discard any prefix derived before rejection. This is not
            // rollback to a before-image: the stores remain Building and the
            // next guarded recovery attempt starts forward from empty state.
            for (_, handle) in &stores {
                handle.with_index_mut(IndexStore::clear);
            }
            Err(error)
        }
    }
}

/// Collect store handles in deterministic path order for stable rebuild behavior.
fn sorted_store_handles(db: &Db<impl CanisterKind>) -> Vec<(&'static str, StoreHandle)> {
    let mut stores = db.with_store_registry(|registry| registry.iter().collect::<Vec<_>>());
    stores.retain(|(_, handle)| {
        matches!(
            handle.storage_capabilities().recovery(),
            StoreRecoveryCapability::StableBasePlusJournalReplay
        )
    });
    // StoreRegistry iteration is HashMap-backed and intentionally unordered.
    // Recovery semantics must remain deterministic, so sort explicitly by path.
    stores.sort_by_key(|(path, _)| *path);
    debug_assert!(
        stores.windows(2).all(|pair| pair[0].0 <= pair[1].0),
        "store registry iteration order must not affect semantic rebuild ordering",
    );

    stores
}

fn rebuild_secondary_indexes_in_place(
    db: &Db<impl CanisterKind>,
    stores: &[(&'static str, StoreHandle)],
) -> Result<(), InternalError> {
    // Phase 1: fail closed during rebuild so no query path can treat one
    // partially rebuilt secondary index as authoritative.
    for (_, handle) in stores {
        handle.mark_index_building();
    }

    // Phase 2: clear all index stores before deterministic full rebuild.
    for (_, handle) in stores {
        handle.with_index_mut(IndexStore::clear);
    }
    #[cfg(test)]
    hit_commit_failpoint(CommitFailpoint::AfterSecondaryIndexRebuildClear)?;

    // Phase 3: rebuild index entries from authoritative row stores.
    for (_, handle) in stores {
        let mut authorities = BTreeMap::<EntityTag, RebuildEntityAuthority>::new();
        handle.with_data(|data_store| {
            data_store.visit_entries(|raw_key, raw_row| {
                let data_key = DecodedDataStoreKey::try_from_raw(raw_key)
                    .map_err(|_| InternalError::startup_index_rebuild_invalid_data_key())?;
                let entity_tag = data_key.entity_tag();
                let authority = match authorities.entry(entity_tag) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => {
                        let hooks = db.runtime_hook_for_entity_tag(entity_tag)?;
                        let accepted_schema = handle.with_schema_mut(|schema_store| {
                            ensure_accepted_schema_snapshot(
                                schema_store,
                                hooks.entity_tag,
                                hooks.entity_path,
                                hooks.store_path,
                                hooks.model,
                            )
                        })?;
                        let candidate_unique = rebuild_candidate_unique_authority(
                            *handle,
                            hooks.entity_tag,
                            hooks.entity_path,
                            hooks.store_path,
                            &accepted_schema,
                        )?;
                        let candidate_relation = rebuild_candidate_relation_authority(
                            db,
                            *handle,
                            hooks.entity_tag,
                            hooks.entity_path,
                            hooks.store_path,
                            &accepted_schema,
                        )?;
                        entry.insert(RebuildEntityAuthority {
                            entity_path: hooks.entity_path,
                            schema_fingerprint: accepted_commit_schema_fingerprint(
                                &accepted_schema,
                            )?,
                            candidate_unique,
                            candidate_relation,
                        })
                    }
                };
                let row_op = CommitRowOp::new(
                    authority.entity_path,
                    raw_key.clone(),
                    None,
                    Some(raw_row.as_bytes().to_vec()),
                    authority.schema_fingerprint,
                );
                let prepared = db.prepare_row_commit_op_for_rebuild(&row_op)?;

                for index_op in prepared.index_ops {
                    index_op.apply();
                }
                rebuild_candidate_generations(*handle, authority, raw_key, raw_row, &data_key)?;

                Ok::<StoreVisit, InternalError>(StoreVisit::Continue)
            })
        })?;

        let data_generation = handle.with_data(DataStore::generation);
        handle.with_index_mut(|index_store| {
            index_store.mark_prefix_cardinality_data_generation(data_generation);
        });
    }

    Ok(())
}

fn rebuild_candidate_generations(
    handle: StoreHandle,
    authority: &RebuildEntityAuthority,
    raw_key: &crate::db::data::RawDataStoreKey,
    raw_row: &crate::db::data::RawRow,
    data_key: &DecodedDataStoreKey,
) -> Result<(), InternalError> {
    if let Some(candidate) = authority.candidate_unique.as_ref()
        && candidate.job.candidate_staging_contains(raw_key)
    {
        let row = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
            raw_row,
            &candidate.contract,
        )?;
        row.validate_primary_key(data_key)?;
        if let Some(key) = candidate
            .projection
            .derive_key(&data_key.primary_key_value(), &row)?
        {
            handle.with_index_mut(|index_store| {
                index_store.insert(key, IndexEntryValue::presence());
            });
        }
    }
    if let Some(candidate) = authority.candidate_relation.as_ref()
        && candidate.job.candidate_staging_contains(raw_key)
    {
        let row = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
            raw_row,
            &candidate.contract,
        )?;
        row.validate_primary_key(data_key)?;
        let projected =
            candidate
                .projection
                .project_row(&data_key.primary_key_value(), &row, true)?;
        for entry in projected.into_entries() {
            entry.target_store().with_index_mut(|index_store| {
                index_store.insert(entry.key().clone(), IndexEntryValue::presence());
            });
        }
    }

    Ok(())
}

fn rebuild_candidate_unique_authority(
    handle: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    store_path: &'static str,
    accepted: &AcceptedSchemaSnapshot,
) -> Result<Option<RebuildCandidateUniqueAuthority>, InternalError> {
    let snapshot = accepted.persisted_snapshot();
    let [candidate] = snapshot.candidate_indexes() else {
        if snapshot.candidate_indexes().is_empty() {
            return Ok(None);
        }
        return Err(InternalError::store_corruption());
    };
    let activation = snapshot
        .constraint_activations()
        .iter()
        .find(|activation| {
            matches!(
                activation.kind(),
                ConstraintActivationKind::Unique { index_id }
                    if *index_id == candidate.schema_id()
            )
        })
        .ok_or_else(InternalError::store_corruption)?;
    if activation.state() == ConstraintActivationState::EnforcingNewWrites {
        return Ok(None);
    }
    let job = handle
        .with_schema(|schema_store| {
            schema_store.constraint_validation_job(entity_tag, activation.id())
        })?
        .ok_or_else(InternalError::store_corruption)?;
    if job.staged_generation() != Some(candidate.physical_generation()) {
        return Err(InternalError::store_corruption());
    }
    let selection = handle
        .with_schema(|schema_store| {
            schema_store.current_accepted_catalog_selection(entity_tag, entity_path, store_path)
        })?
        .ok_or_else(InternalError::store_corruption)?;
    let selected = selection.decode_verified()?;
    if selected.persisted_snapshot() != snapshot {
        return Err(InternalError::store_corruption());
    }
    let contract = AcceptedStructuralRowAuthority::from_catalog_selection(entity_path, &selection)?
        .into_row_contract();
    let projection = UniqueConstraintProjection::new(entity_tag, candidate, &contract)?;
    Ok(Some(RebuildCandidateUniqueAuthority {
        contract,
        projection,
        job,
    }))
}

fn rebuild_candidate_relation_authority(
    db: &Db<impl CanisterKind>,
    handle: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    store_path: &'static str,
    accepted: &AcceptedSchemaSnapshot,
) -> Result<Option<RebuildCandidateRelationAuthority>, InternalError> {
    let snapshot = accepted.persisted_snapshot();
    let [candidate] = snapshot.candidate_relations() else {
        if snapshot.candidate_relations().is_empty() {
            return Ok(None);
        }
        return Err(InternalError::store_corruption());
    };
    let activation = snapshot
        .constraint_activations()
        .iter()
        .find(|activation| {
            matches!(
                activation.kind(),
                ConstraintActivationKind::Relation { relation_id }
                    if *relation_id == candidate.id()
            )
        })
        .ok_or_else(InternalError::store_corruption)?;
    if activation.state() == ConstraintActivationState::EnforcingNewWrites {
        return Ok(None);
    }
    let job = handle
        .with_schema(|schema_store| {
            schema_store.constraint_validation_job(entity_tag, activation.id())
        })?
        .ok_or_else(InternalError::store_corruption)?;
    if job.staged_generation() != Some(candidate.physical_generation()) {
        return Err(InternalError::store_corruption());
    }
    let selection = handle
        .with_schema(|schema_store| {
            schema_store.current_accepted_catalog_selection(entity_tag, entity_path, store_path)
        })?
        .ok_or_else(InternalError::store_corruption)?;
    let selected = selection.decode_verified()?;
    if selected.persisted_snapshot() != snapshot {
        return Err(InternalError::store_corruption());
    }
    let contract = AcceptedStructuralRowAuthority::from_catalog_selection(entity_path, &selection)?
        .into_row_contract();
    let projection = RelationConstraintProjection::new(
        db,
        ReverseRelationSourceInfo::new(entity_path, entity_tag),
        snapshot,
        &contract,
        candidate,
    )?;
    Ok(Some(RebuildCandidateRelationAuthority {
        contract,
        projection,
        job,
    }))
}
