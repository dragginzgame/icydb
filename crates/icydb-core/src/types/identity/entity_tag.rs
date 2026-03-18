//! Module: types::identity::entity_tag
//! Responsibility: module-local ownership and contracts for types::identity::entity_tag.
//! Does not own: schema naming or runtime dispatch policy.
//! Boundary: compact, copyable runtime entity identity primitive.

///
/// EntityTag
///
/// Stable runtime entity identity token used on hot execution paths.
///

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EntityTag(u64);

impl EntityTag {
    /// Construct one tag from a raw `u64`.
    /// Registry/codegen is the intended caller; runtime code should pass
    /// through existing tag constants instead of synthesizing new values.
    #[must_use]
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the raw tag value.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}
