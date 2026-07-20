//! Module: db::schema::reconcile::user_index_domain
//! Responsibility: adapt authoritative stored rows to schema-owned index-domain staging.
//! Does not own: index derivation, physical apply, accepted-schema publication, or recovery.
//! Boundary: accepted catalog + stored rows -> zero-write staged user-index domain.

use crate::{
    db::{
        data::{
            AcceptedStructuralRowAuthority, DecodedDataStoreKey, StoreVisit, StructuralRowContract,
            StructuralSlotReader,
        },
        registry::StoreHandle,
        schema::{
            AcceptedCatalogIdentity, PersistedSchemaSnapshot, SchemaUserIndexDomainRow,
            StagedUserIndexDomainError, StagedUserIndexDomainReplacement,
            StagedUserIndexDomainReplacementBuilder,
        },
    },
    error::InternalError,
    types::EntityTag,
};

/// Stage one complete accepted-after user-index domain for startup schema
/// reconciliation without changing schema or physical index state.
pub(super) fn stage_startup_user_index_domain_replacement(
    store: StoreHandle,
    entity_tag: EntityTag,
    store_path: &'static str,
    entity_path: &'static str,
    accepted_before: &PersistedSchemaSnapshot,
    accepted_after: &PersistedSchemaSnapshot,
) -> Result<StagedUserIndexDomainReplacement, InternalError> {
    let (accepted_before_identity, row_contract) =
        catalog_backed_row_authority(store, entity_tag, store_path, entity_path, accepted_before)?;

    stage_user_index_domain_replacement(
        store,
        accepted_before_identity,
        accepted_before,
        accepted_after,
        row_contract,
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
    let (_, row_contract) = catalog_backed_row_authority(
        store,
        accepted_before_identity.entity_tag(),
        accepted_before_identity.store_path(),
        accepted_before_identity.entity_path(),
        accepted_before,
    )?;

    stage_user_index_domain_replacement(
        store,
        accepted_before_identity,
        accepted_before,
        accepted_after,
        row_contract,
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

fn stage_user_index_domain_replacement(
    store: StoreHandle,
    accepted_before_identity: AcceptedCatalogIdentity,
    accepted_before: &PersistedSchemaSnapshot,
    accepted_after: &PersistedSchemaSnapshot,
    row_contract: StructuralRowContract,
) -> Result<StagedUserIndexDomainReplacement, InternalError> {
    let entity_tag = accepted_before_identity.entity_tag();
    let entity_path = accepted_before_identity.entity_path();
    let mut builder = store.with_index(|index_store| {
        StagedUserIndexDomainReplacementBuilder::new(
            accepted_before_identity,
            accepted_before,
            accepted_after,
            Some(&row_contract),
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
            let slots = StructuralSlotReader::from_raw_row_with_validated_contract(
                raw_row,
                row_contract.clone(),
            )?;
            slots.validate_primary_key(&data_key)?;
            let row =
                SchemaUserIndexDomainRow::new(data_key.primary_key_value(), &slots, raw_row.len());
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
