//! Canonical store-local enum identity shared by schema admission and runtime values.

use std::num::NonZeroU32;

/// Stable non-zero identity of one enum type inside an accepted store catalog.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct EnumTypeId(NonZeroU32);

impl EnumTypeId {
    #[must_use]
    pub(crate) const fn new(value: u32) -> Option<Self> {
        match NonZeroU32::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    #[must_use]
    pub(crate) const fn get(self) -> u32 {
        self.0.get()
    }
}

/// Stable non-zero identity of one variant inside an accepted enum type.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct EnumVariantId(NonZeroU32);

impl EnumVariantId {
    #[must_use]
    pub(crate) const fn new(value: u32) -> Option<Self> {
        match NonZeroU32::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    #[must_use]
    pub(crate) const fn get(self) -> u32 {
        self.0.get()
    }
}

/// Canonical store-local enum identity and explicit body shape.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct CanonicalEnumValue<V> {
    type_id: EnumTypeId,
    variant_id: EnumVariantId,
    body: CanonicalEnumBody<V>,
}

impl<V> CanonicalEnumValue<V> {
    #[must_use]
    pub(crate) const fn new(
        type_id: EnumTypeId,
        variant_id: EnumVariantId,
        body: CanonicalEnumBody<V>,
    ) -> Self {
        Self {
            type_id,
            variant_id,
            body,
        }
    }

    #[must_use]
    pub(crate) const fn type_id(&self) -> EnumTypeId {
        self.type_id
    }

    #[must_use]
    pub(crate) const fn variant_id(&self) -> EnumVariantId {
        self.variant_id
    }

    #[must_use]
    pub(crate) const fn body(&self) -> &CanonicalEnumBody<V> {
        &self.body
    }

    /// Consume the canonical identity and transfer its owned body.
    #[must_use]
    pub(crate) fn into_body(self) -> CanonicalEnumBody<V> {
        self.body
    }
}

/// Canonical unit or payload-bearing enum body.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum CanonicalEnumBody<V> {
    Unit,
    Payload(Box<V>),
}

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_fields!(CanonicalEnumBody<V> {
Self::Unit => [],
Self::Payload(field_0) => [field_0],
});
crate::retained::retained_fields!(CanonicalEnumValue<V> {
Self{type_id,variant_id,body} => [type_id,variant_id,body],
});
crate::retained::retained_copy!(EnumTypeId);
crate::retained::retained_copy!(EnumVariantId);
