use crate::prelude::*;

///
/// PrimaryKey
///
/// Structured primary-key metadata for an entity schema.
///

#[derive(Clone, Debug, Serialize)]
pub struct PrimaryKey {
    field: &'static str,
    source: PrimaryKeySource,
}

impl PrimaryKey {
    /// Build one primary-key declaration from field name and source.
    #[must_use]
    pub const fn new(field: &'static str, source: PrimaryKeySource) -> Self {
        Self { field, source }
    }

    /// Borrow the primary-key field name.
    #[must_use]
    pub const fn field(&self) -> &'static str {
        self.field
    }

    /// Borrow the primary-key source declaration.
    #[must_use]
    pub const fn source(&self) -> PrimaryKeySource {
        self.source
    }
}

///
/// PrimaryKeySource
///
/// Declares where primary-key values originate.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub enum PrimaryKeySource {
    #[default]
    Internal,

    External,
}
