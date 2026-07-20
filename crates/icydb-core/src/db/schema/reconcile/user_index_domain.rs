//! Module: db::schema::reconcile::user_index_domain
//! Responsibility: adapt authoritative stored rows to schema-owned index-domain staging.
//! Does not own: index derivation, physical apply, accepted-schema publication, or recovery.
//! Boundary: accepted catalog + stored rows -> zero-write staged user-index domain.

use crate::{
    db::{
        data::{
            AcceptedStructuralRowAuthority, DecodedDataStoreKey, RawRow, StoreVisit,
            StructuralRowContract, StructuralSlotReader,
        },
        key_taxonomy::PrimaryKeyValue,
        registry::StoreHandle,
        schema::{
            AcceptedCatalogIdentity, PersistedSchemaSnapshot, SchemaUserIndexDomainRow,
            StagedUserIndexDomainError, StagedUserIndexDomainReplacement,
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
    let raw_rows = user_index_domain_raw_rows(store, entity_tag, entity_path)?;
    let decoded_rows =
        decode_user_index_domain_rows(raw_rows.as_slice(), entity_tag, row_contract.clone())?;
    let rows = decoded_rows
        .iter()
        .zip(raw_rows.iter())
        .map(|(decoded, raw)| {
            SchemaUserIndexDomainRow::new(decoded.primary_key_value, &decoded.slots, raw.row.len())
        });

    store.with_index(|index_store| {
        StagedUserIndexDomainReplacement::stage(
            accepted_before_identity,
            accepted_before,
            accepted_after,
            Some(&row_contract),
            rows,
            index_store,
        )
        .map_err(StagedUserIndexDomainError::into_internal_error)
    })
}

// Raw rows remain owned until all borrowed structural slot readers finish
// complete-domain derivation.
struct UserIndexDomainRawRow {
    primary_key_value: PrimaryKeyValue,
    row: RawRow,
}

// The decoded reader borrows the raw row and carries the accepted row layout.
struct DecodedUserIndexDomainRow<'a> {
    primary_key_value: PrimaryKeyValue,
    slots: StructuralSlotReader<'a>,
}

fn user_index_domain_raw_rows(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
) -> Result<Vec<UserIndexDomainRawRow>, InternalError> {
    store.with_data(|data_store| {
        let mut rows = Vec::new();
        data_store.visit_entries(|raw_key, raw_row| {
            let data_key = DecodedDataStoreKey::try_from_raw(raw_key).map_err(|error| {
                let _ = (&error, entity_path);
                InternalError::store_corruption()
            })?;
            if data_key.entity_tag() != entity_tag {
                return Ok::<StoreVisit, InternalError>(StoreVisit::Continue);
            }
            rows.push(UserIndexDomainRawRow {
                primary_key_value: data_key.primary_key_value(),
                row: raw_row.clone(),
            });
            Ok::<StoreVisit, InternalError>(StoreVisit::Continue)
        })?;

        Ok::<_, InternalError>(rows)
    })
}

fn decode_user_index_domain_rows(
    rows: &[UserIndexDomainRawRow],
    entity_tag: EntityTag,
    row_contract: StructuralRowContract,
) -> Result<Vec<DecodedUserIndexDomainRow<'_>>, InternalError> {
    rows.iter()
        .map(|row| {
            let slots = StructuralSlotReader::from_raw_row_with_validated_contract(
                &row.row,
                row_contract.clone(),
            )?;
            let data_key =
                DecodedDataStoreKey::new_primary_key_value(entity_tag, &row.primary_key_value);
            slots.validate_primary_key(&data_key)?;

            Ok(DecodedUserIndexDomainRow {
                primary_key_value: row.primary_key_value,
                slots,
            })
        })
        .collect()
}
