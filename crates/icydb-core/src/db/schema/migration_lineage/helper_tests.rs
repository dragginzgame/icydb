//! Test-only constructors and adapters for the parent module.

use super::*;

impl AcceptedEntitySourceLineageCatalog {
    pub(in crate::db::schema) fn insert(
        &mut self,
        store: TargetStoreIdentity,
        entity: EntityTag,
        lineage: AcceptedEntitySourceLineage,
    ) -> Result<(), InternalError> {
        if entity.value() == 0
            || self.entries.len() >= MAX_ENTITY_SOURCE_LINEAGE_ENTRIES
            || self.entries.insert((store, entity), lineage).is_some()
        {
            return Err(InternalError::store_unsupported());
        }
        Ok(())
    }
}

pub(in crate::db) fn unadopted_entity_source_lineage_op_for_tests(
    store: TargetStoreIdentity,
    entity: EntityTag,
    accepted_head: ExpectedAcceptedHead,
) -> Result<EntitySourceLineageCatalogOp, InternalError> {
    let catalog = AcceptedEntitySourceLineageCatalog::try_new(BTreeMap::from([(
        (store, entity),
        AcceptedEntitySourceLineage::unadopted(accepted_head)?,
    )]))?;
    EntitySourceLineageCatalogOp::replace(None, &catalog)
}
