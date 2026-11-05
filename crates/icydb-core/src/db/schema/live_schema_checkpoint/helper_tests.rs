//! Test-only helpers for the parent module.

use super::*;

pub(in crate::db) fn take_migration_record_load_count_for_tests() -> u64 {
    MIGRATION_RECORD_LOADS.with(|loads| loads.replace(0))
}

pub(in crate::db) fn entity_source_lineage_matches_for_tests(
    operation: &EntitySourceLineageCatalogOp,
) -> Result<bool, InternalError> {
    let Some(catalog) = load_entity_source_lineage_catalog()? else {
        return Ok(false);
    };
    Ok(
        crate::db::schema::migration_lineage::encode_entity_source_lineage_catalog(&catalog)?
            == operation.after_bytes(),
    )
}

pub(in crate::db) fn schema_migration_record_matches_for_tests(
    operation: &SchemaMigrationRecordOp,
) -> Result<bool, InternalError> {
    let Some(record) = load_schema_migration_record()? else {
        return Ok(false);
    };
    Ok(
        crate::db::schema::migration_record::encode_schema_migration_record(&record)?
            == operation.after_bytes(),
    )
}

thread_local! {
    pub(super) static MIGRATION_RECORD_LOADS: Cell<u64> = const { Cell::new(0) };
}
