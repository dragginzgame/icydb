use std::fmt::{self, Display};

///
/// IndexModel
///
/// Runtime-only descriptor for an index used by the executor and stores.
/// Keeps core decoupled from the schema `Index` shape.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexModel {
    /// Stable index name used for diagnostics and planner identity.
    pub name: &'static str,
    pub store: &'static str,
    pub fields: &'static [&'static str],
    pub unique: bool,
}

impl IndexModel {
    #[must_use]
    pub const fn new(
        name: &'static str,
        store: &'static str,
        fields: &'static [&'static str],
        unique: bool,
    ) -> Self {
        Self {
            name,
            store,
            fields,
            unique,
        }
    }

    #[must_use]
    /// Whether this index's field prefix matches the start of another index.
    pub fn is_prefix_of(&self, other: &Self) -> bool {
        self.fields.len() < other.fields.len() && other.fields.starts_with(self.fields)
    }
}

impl Display for IndexModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fields = self.fields.join(", ");

        if self.unique {
            write!(f, "{}: UNIQUE {}({})", self.name, self.store, fields)
        } else {
            write!(f, "{}: {}({})", self.name, self.store, fields)
        }
    }
}
