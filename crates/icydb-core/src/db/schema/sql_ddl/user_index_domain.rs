//! Module: db::schema::sql_ddl::user_index_domain
//! Responsibility: stage one SQL-DDL-owned accepted user-index domain.
//! Does not own: index derivation, physical apply, or accepted publication.
//! Boundary: accepted catalog plus stored rows to one zero-write staged domain.

use crate::{
    db::{
        data::{
            AcceptedStructuralRowAuthority, DecodedDataStoreKey, StoreVisit, StructuralRowContract,
            StructuralSlotReader,
        },
        registry::StoreHandle,
        schema::{
            AcceptedCatalogIdentity, AcceptedSchemaSnapshot, PersistedSchemaSnapshot,
            SchemaUserIndexDomainRow, StagedUserIndexDomainError, StagedUserIndexDomainReplacement,
            StagedUserIndexDomainReplacementBuilder,
        },
    },
    error::InternalError,
    types::EntityTag,
};

/// Stage one complete accepted-after user-index domain for SQL index DDL
/// without changing schema or physical index state.
pub(super) fn stage_sql_ddl_user_index_domain_replacement(
    store: StoreHandle,
    accepted_before_identity: &AcceptedCatalogIdentity,
    accepted_before: &PersistedSchemaSnapshot,
    accepted_after: &PersistedSchemaSnapshot,
) -> Result<StagedUserIndexDomainReplacement, InternalError> {
    let accepted_before_row_contract = catalog_backed_row_authority(
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
        accepted_before_identity.clone(),
        accepted_before,
        accepted_after,
        accepted_before_row_contract,
        accepted_after_row_contract,
    )
}

fn catalog_backed_row_authority(
    store: StoreHandle,
    entity_tag: EntityTag,
    store_path: &'static str,
    entity_path: &str,
    accepted_before: &PersistedSchemaSnapshot,
) -> Result<StructuralRowContract, InternalError> {
    let selection = store
        .with_schema(|schema_store| {
            schema_store.current_accepted_catalog_selection(entity_tag, entity_path, store_path)
        })?
        .ok_or_else(InternalError::store_corruption)?;
    let authority =
        AcceptedStructuralRowAuthority::from_catalog_selection(entity_path, &selection)?;
    if authority.accepted_schema().persisted_snapshot() != accepted_before {
        return Err(InternalError::store_unsupported());
    }

    Ok(authority.into_row_contract())
}

fn stage_user_index_domain_replacement(
    store: StoreHandle,
    accepted_before_identity: AcceptedCatalogIdentity,
    accepted_before: &PersistedSchemaSnapshot,
    accepted_after: &PersistedSchemaSnapshot,
    accepted_before_row_contract: StructuralRowContract,
    accepted_after_row_contract: StructuralRowContract,
) -> Result<StagedUserIndexDomainReplacement, InternalError> {
    let entity_tag = accepted_before_identity.entity_tag();
    let entity_path = accepted_before_identity.entity_path_handle();
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
                let _ = (&error, entity_path.as_ref());
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
