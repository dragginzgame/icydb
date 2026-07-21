//! Module: db::schema::reconcile::user_index_domain
//! Responsibility: adapt authoritative stored rows to schema-owned derived-domain staging.
//! Does not own: index derivation, physical apply, accepted-schema publication, or recovery.
//! Boundary: accepted catalog + stored rows -> one zero-write staged derived domain.

#[cfg(feature = "sql")]
use crate::db::schema::AcceptedSchemaSnapshot;
use crate::{
    db::{
        Db,
        commit::PreparedIndexMutation,
        data::{
            AcceptedStructuralRowAuthority, DecodedDataStoreKey, StoreVisit, StructuralRowContract,
            StructuralSlotReader,
        },
        registry::StoreHandle,
        relation::{ReverseRelationSourceInfo, StagedReverseRelationDomainEffectsBuilder},
        schema::{
            AcceptedCatalogIdentity, AcceptedCatalogSnapshotSelection, CandidateSchemaRevision,
            PersistedSchemaSnapshot, SchemaUserIndexDomainRow, StagedUserIndexDomainError,
            StagedUserIndexDomainReplacement, StagedUserIndexDomainReplacementBuilder,
        },
    },
    error::InternalError,
    traits::CanisterKind,
    types::EntityTag,
};

///
/// StagedDerivedDomainReplacement
///
/// Inseparable user-index replacement and reverse-relation effects for one
/// accepted entity candidate. The user-index stage owns candidate identity;
/// reverse-relation effects are derived during the same row traversal.
///

pub(in crate::db) struct StagedDerivedDomainReplacement {
    user_indexes: StagedUserIndexDomainReplacement,
    reverse_relations: Vec<PreparedIndexMutation>,
}

impl StagedDerivedDomainReplacement {
    /// Borrow the candidate identity owner used by publication validation.
    #[must_use]
    pub(in crate::db) const fn user_indexes(&self) -> &StagedUserIndexDomainReplacement {
        &self.user_indexes
    }

    /// Consume the stage into its two mechanical apply payloads.
    pub(in crate::db) fn into_apply_parts(
        self,
    ) -> (StagedUserIndexDomainReplacement, Vec<PreparedIndexMutation>) {
        (self.user_indexes, self.reverse_relations)
    }
}

/// Stage one complete accepted-after user-index domain for startup schema
/// reconciliation without changing schema or physical index state.
pub(super) fn stage_startup_derived_domain_replacement<C: CanisterKind>(
    db: &Db<C>,
    store: StoreHandle,
    entity_tag: EntityTag,
    store_path: &'static str,
    entity_path: &'static str,
    candidate: &CandidateSchemaRevision,
) -> Result<StagedDerivedDomainReplacement, InternalError> {
    let accepted_before_selection = store
        .with_schema(|schema_store| {
            schema_store.current_accepted_catalog_selection(entity_tag, entity_path, store_path)
        })?
        .ok_or_else(InternalError::store_corruption)?;
    let accepted_after_selection = AcceptedCatalogSnapshotSelection::from_candidate(
        candidate,
        entity_tag,
        entity_path,
        store_path,
    )?
    .ok_or_else(InternalError::store_corruption)?;
    let accepted_before_identity = accepted_before_selection.identity();
    let (accepted_before, accepted_before_row_contract) =
        AcceptedStructuralRowAuthority::from_catalog_selection(
            entity_path,
            &accepted_before_selection,
        )?
        .into_parts();
    let (accepted_after, accepted_after_row_contract) =
        AcceptedStructuralRowAuthority::from_catalog_selection(
            entity_path,
            &accepted_after_selection,
        )?
        .into_parts();

    stage_generated_derived_domain_replacement(
        db,
        store,
        accepted_before_identity,
        accepted_before.persisted_snapshot(),
        accepted_after.persisted_snapshot(),
        accepted_before_row_contract,
        accepted_after_row_contract,
    )
}

/// Stage one complete accepted-after user-index domain for SQL index DDL
/// without changing schema or physical index state.
#[cfg(feature = "sql")]
pub(super) fn stage_sql_ddl_user_index_domain_replacement(
    store: StoreHandle,
    accepted_before_identity: AcceptedCatalogIdentity,
    accepted_before: &PersistedSchemaSnapshot,
    accepted_after: &PersistedSchemaSnapshot,
) -> Result<StagedUserIndexDomainReplacement, InternalError> {
    let (_, accepted_before_row_contract) = catalog_backed_row_authority(
        store,
        accepted_before_identity.entity_tag(),
        accepted_before_identity.store_path(),
        accepted_before_identity.entity_path(),
        accepted_before,
    )?;

    let accepted_after_snapshot = AcceptedSchemaSnapshot::try_new(accepted_after.clone())?;
    let selection = store
        .with_schema(|schema_store| {
            schema_store.current_accepted_catalog_selection(
                accepted_before_identity.entity_tag(),
                accepted_before_identity.entity_path(),
                accepted_before_identity.store_path(),
            )
        })?
        .ok_or_else(InternalError::store_corruption)?;
    let accepted_after_row_contract = AcceptedStructuralRowAuthority::from_candidate_snapshot(
        accepted_before_identity.entity_path(),
        accepted_after_snapshot,
        selection.value_catalog_handle().clone(),
    )?
    .into_row_contract();

    stage_user_index_domain_replacement(
        store,
        accepted_before_identity,
        accepted_before,
        accepted_after,
        accepted_before_row_contract,
        accepted_after_row_contract,
    )
}

/// Resolve the accepted catalog's structural row contract for non-index SQL
/// DDL that rewrites current rows.
#[cfg(feature = "sql")]
pub(super) fn catalog_backed_row_contract_for_sql_ddl(
    store: StoreHandle,
    accepted_before_identity: AcceptedCatalogIdentity,
    accepted_before: &PersistedSchemaSnapshot,
) -> Result<StructuralRowContract, InternalError> {
    catalog_backed_row_authority(
        store,
        accepted_before_identity.entity_tag(),
        accepted_before_identity.store_path(),
        accepted_before_identity.entity_path(),
        accepted_before,
    )
    .map(|(_, row_contract)| row_contract)
}

#[cfg(feature = "sql")]
fn catalog_backed_row_authority(
    store: StoreHandle,
    entity_tag: EntityTag,
    store_path: &'static str,
    entity_path: &'static str,
    accepted_before: &PersistedSchemaSnapshot,
) -> Result<(AcceptedCatalogIdentity, StructuralRowContract), InternalError> {
    let selection = store
        .with_schema(|schema_store| {
            schema_store.current_accepted_catalog_selection(entity_tag, entity_path, store_path)
        })?
        .ok_or_else(InternalError::store_corruption)?;
    let identity = selection.identity();
    let authority =
        AcceptedStructuralRowAuthority::from_catalog_selection(entity_path, &selection)?;
    if authority.accepted_schema().persisted_snapshot() != accepted_before {
        return Err(InternalError::store_unsupported());
    }

    Ok((identity, authority.into_row_contract()))
}

#[cfg(feature = "sql")]
fn stage_user_index_domain_replacement(
    store: StoreHandle,
    accepted_before_identity: AcceptedCatalogIdentity,
    accepted_before: &PersistedSchemaSnapshot,
    accepted_after: &PersistedSchemaSnapshot,
    accepted_before_row_contract: StructuralRowContract,
    accepted_after_row_contract: StructuralRowContract,
) -> Result<StagedUserIndexDomainReplacement, InternalError> {
    let entity_tag = accepted_before_identity.entity_tag();
    let entity_path = accepted_before_identity.entity_path();
    let mut builder = store.with_index(|index_store| {
        StagedUserIndexDomainReplacementBuilder::new(
            accepted_before_identity,
            accepted_before,
            accepted_after,
            Some(&accepted_before_row_contract),
            Some(&accepted_after_row_contract),
            index_store,
        )
        .map_err(StagedUserIndexDomainError::into_internal_error)
    })?;
    store.with_data(|data_store| {
        data_store.visit_entries(|raw_key, raw_row| {
            let data_key = DecodedDataStoreKey::try_from_raw(raw_key).map_err(|error| {
                let _ = (&error, entity_path);
                InternalError::store_corruption()
            })?;
            if data_key.entity_tag() != entity_tag {
                return Ok::<StoreVisit, InternalError>(StoreVisit::Continue);
            }
            let accepted_before_slots = StructuralSlotReader::from_raw_row_with_validated_contract(
                raw_row,
                accepted_before_row_contract.clone(),
            )?;
            accepted_before_slots.validate_primary_key(&data_key)?;
            let accepted_after_slots = StructuralSlotReader::from_raw_row_with_validated_contract(
                raw_row,
                accepted_after_row_contract.clone(),
            )?;
            accepted_after_slots.validate_primary_key(&data_key)?;
            let row = SchemaUserIndexDomainRow::new(
                data_key.primary_key_value(),
                &accepted_before_slots,
                &accepted_after_slots,
                raw_row.len(),
            );
            builder
                .observe_row(&row)
                .map_err(StagedUserIndexDomainError::into_internal_error)?;
            Ok::<StoreVisit, InternalError>(StoreVisit::Continue)
        })
    })?;
    store.with_index(|index_store| {
        builder
            .finish(index_store)
            .map_err(StagedUserIndexDomainError::into_internal_error)
    })
}

fn stage_generated_derived_domain_replacement<C: CanisterKind>(
    db: &Db<C>,
    store: StoreHandle,
    accepted_before_identity: AcceptedCatalogIdentity,
    accepted_before: &PersistedSchemaSnapshot,
    accepted_after: &PersistedSchemaSnapshot,
    accepted_before_row_contract: StructuralRowContract,
    accepted_after_row_contract: StructuralRowContract,
) -> Result<StagedDerivedDomainReplacement, InternalError> {
    let entity_tag = accepted_before_identity.entity_tag();
    let entity_path = accepted_before_identity.entity_path();
    let mut user_indexes = store.with_index(|index_store| {
        StagedUserIndexDomainReplacementBuilder::new(
            accepted_before_identity,
            accepted_before,
            accepted_after,
            Some(&accepted_before_row_contract),
            Some(&accepted_after_row_contract),
            index_store,
        )
        .map_err(StagedUserIndexDomainError::into_internal_error)
    })?;
    let mut reverse_relations = StagedReverseRelationDomainEffectsBuilder::new(
        db,
        ReverseRelationSourceInfo::new(entity_path, entity_tag),
        accepted_before_identity,
        accepted_before,
        accepted_after,
        accepted_before_row_contract.clone(),
        accepted_after_row_contract.clone(),
    )?;
    store.with_data(|data_store| {
        data_store.visit_entries(|raw_key, raw_row| {
            let data_key = DecodedDataStoreKey::try_from_raw(raw_key).map_err(|error| {
                let _ = (&error, entity_path);
                InternalError::store_corruption()
            })?;
            if data_key.entity_tag() != entity_tag {
                return Ok::<StoreVisit, InternalError>(StoreVisit::Continue);
            }
            let accepted_before_slots = StructuralSlotReader::from_raw_row_with_validated_contract(
                raw_row,
                accepted_before_row_contract.clone(),
            )?;
            accepted_before_slots.validate_primary_key(&data_key)?;
            let accepted_after_slots = StructuralSlotReader::from_raw_row_with_validated_contract(
                raw_row,
                accepted_after_row_contract.clone(),
            )?;
            accepted_after_slots.validate_primary_key(&data_key)?;
            let primary_key = data_key.primary_key_value();
            let row = SchemaUserIndexDomainRow::new(
                primary_key,
                &accepted_before_slots,
                &accepted_after_slots,
                raw_row.len(),
            );
            user_indexes
                .observe_row(&row)
                .map_err(StagedUserIndexDomainError::into_internal_error)?;
            reverse_relations.observe_row(
                &primary_key,
                &accepted_before_slots,
                &accepted_after_slots,
            )?;
            Ok::<StoreVisit, InternalError>(StoreVisit::Continue)
        })
    })?;
    let user_indexes = store.with_index(|index_store| {
        user_indexes
            .finish(index_store)
            .map_err(StagedUserIndexDomainError::into_internal_error)
    })?;
    let reverse_relations = reverse_relations.finish(user_indexes.usage().staged_raw_bytes())?;

    Ok(StagedDerivedDomainReplacement {
        user_indexes,
        reverse_relations,
    })
}
