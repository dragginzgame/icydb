use std::fmt::{self, Display};

///
/// IndexSpec
/// Runtime-only descriptor for an index used by the executor and stores.
/// Keeps core decoupled from the schema `Index` shape.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexSpec {
    pub store: &'static str,
    pub fields: &'static [&'static str],
    pub unique: bool,
}

impl IndexSpec {
    #[must_use]
    pub const fn new(store: &'static str, fields: &'static [&'static str], unique: bool) -> Self {
        Self {
            store,
            fields,
            unique,
        }
    }

    #[must_use]
    pub fn is_prefix_of(&self, other: &Self) -> bool {
        self.fields.len() < other.fields.len() && other.fields.starts_with(self.fields)
    }
}

impl Display for IndexSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fields = self.fields.join(", ");

        if self.unique {
            write!(f, "UNIQUE {}({})", self.store, fields)
        } else {
            write!(f, "{}({})", self.store, fields)
        }
    }
}
