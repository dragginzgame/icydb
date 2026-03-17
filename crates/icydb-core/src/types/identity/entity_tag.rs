//! Module: types::identity::entity_tag
//! Responsibility: module-local ownership and contracts for types::identity::entity_tag.
//! Does not own: schema naming or runtime dispatch policy.
//! Boundary: compact, copyable runtime entity identity primitive.

///
/// EntityTag
///
/// Stable runtime entity identity token used on hot execution paths.
///

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EntityTag(pub u64);

impl EntityTag {
    /// Construct one tag from a raw `u64`.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the raw tag value.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }

    /// Derive one stable tag from an entity name using the frozen FNV-1a
    /// `u64` contract used by code generation.
    #[must_use]
    pub const fn from_entity_name(name: &str) -> Self {
        const FNV1A_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
        const FNV1A_PRIME: u64 = 0x0000_0100_0000_01b3;

        let bytes = name.as_bytes();
        let mut i = 0usize;
        let mut hash = FNV1A_OFFSET_BASIS;
        while i < bytes.len() {
            hash ^= bytes[i] as u64;
            hash = hash.wrapping_mul(FNV1A_PRIME);
            i += 1;
        }

        Self(hash)
    }
}
