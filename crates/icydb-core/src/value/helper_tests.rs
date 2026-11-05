//! Test-only constructors and adapters for the parent module.

use super::*;

impl ValueEnum {
    pub(crate) const fn test_unit(type_id: u32, variant_id: u32) -> Self {
        Self::new(
            EnumTypeId::new(type_id).expect("test enum type ID must be non-zero"),
            EnumVariantId::new(variant_id).expect("test enum variant ID must be non-zero"),
            CanonicalEnumBody::Unit,
        )
    }

    pub(crate) fn test_payload(type_id: u32, variant_id: u32, payload: Value) -> Self {
        Self::new(
            EnumTypeId::new(type_id).expect("test enum type ID must be non-zero"),
            EnumVariantId::new(variant_id).expect("test enum variant ID must be non-zero"),
            CanonicalEnumBody::Payload(Box::new(payload)),
        )
    }

    pub(crate) fn test_with_payload(self, payload: Value) -> Self {
        Self::new(
            self.type_id(),
            self.variant_id(),
            CanonicalEnumBody::Payload(Box::new(payload)),
        )
    }
}
