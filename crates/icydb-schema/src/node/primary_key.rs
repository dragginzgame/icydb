use crate::prelude::*;

///
/// PrimaryKey
///
/// Structured primary-key metadata for an entity schema.
///

#[derive(Clone, Debug, Serialize)]
pub struct PrimaryKey {
    fields: &'static [&'static str],
    source: PrimaryKeySource,
}

impl PrimaryKey {
    /// Build one primary-key declaration from ordered field names and source.
    #[must_use]
    pub const fn new(fields: &'static [&'static str], source: PrimaryKeySource) -> Self {
        Self { fields, source }
    }

    /// Borrow the ordered primary-key field names.
    #[must_use]
    pub const fn fields(&self) -> &'static [&'static str] {
        self.fields
    }

    /// Borrow the scalar primary-key field name used by the current runtime.
    #[must_use]
    pub const fn field(&self) -> &'static str {
        self.fields[0]
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

#[cfg(test)]
mod tests {
    use super::{PrimaryKey, PrimaryKeySource};

    #[test]
    fn primary_key_keeps_ordered_field_list_and_scalar_projection() {
        let primary_key = PrimaryKey::new(&["tenant_id", "local_id"], PrimaryKeySource::External);

        assert_eq!(primary_key.fields(), ["tenant_id", "local_id"]);
        assert_eq!(primary_key.field(), "tenant_id");
        assert_eq!(primary_key.source(), PrimaryKeySource::External);
    }
}
