//! Test-only constructors and adapters for the parent module.

use super::*;

impl AcceptedRelationTargetIdentity {
    pub(super) fn try_new(
        target_path: &str,
        target_entity_name: &str,
        target_entity_tag: EntityTag,
        target_store_path: &str,
        key_kinds: &[AcceptedFieldKind],
    ) -> Result<Self, InternalError> {
        Ok(Self {
            authority: AcceptedRelationTargetAuthority::try_new(
                target_path,
                target_entity_name,
                target_entity_tag,
                target_store_path,
            )?,
            primary_key: AcceptedRelationTargetPrimaryKey::try_from_component_kinds(key_kinds)?,
        })
    }
}
