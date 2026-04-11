//! Module: types::identity::entity_tag
//! Defines the compact runtime entity tag used by execution, registry, and
//! code-generated schema plumbing.

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
    /// Registry and codegen are the intended callers; runtime code should pass
    /// through existing tag constants instead of synthesizing fresh values.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the raw tag value.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}
