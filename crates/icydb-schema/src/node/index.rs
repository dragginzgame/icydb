use crate::prelude::*;
use std::{
    fmt::{self, Display},
    ops::Not,
};

///
/// Index
///

#[derive(Clone, Debug, Serialize)]
pub struct Index {
    fields: &'static [&'static str],

    #[serde(default, skip_serializing_if = "Not::not")]
    unique: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    predicate: Option<&'static str>,
}

impl Index {
    /// Build one index declaration from field-list and uniqueness metadata.
    #[must_use]
    pub const fn new(fields: &'static [&'static str], unique: bool) -> Self {
        Self::new_with_predicate(fields, unique, None)
    }

    /// Build one index declaration with optional conditional predicate metadata.
    #[must_use]
    pub const fn new_with_predicate(
        fields: &'static [&'static str],
        unique: bool,
        predicate: Option<&'static str>,
    ) -> Self {
        Self {
            fields,
            unique,
            predicate,
        }
    }

    /// Borrow index field sequence.
    #[must_use]
    pub const fn fields(&self) -> &'static [&'static str] {
        self.fields
    }

    /// Return whether the index enforces uniqueness.
    #[must_use]
    pub const fn is_unique(&self) -> bool {
        self.unique
    }

    /// Return optional conditional-index predicate metadata.
    #[must_use]
    pub const fn predicate(&self) -> Option<&'static str> {
        self.predicate
    }

    #[must_use]
    pub fn is_prefix_of(&self, other: &Self) -> bool {
        self.fields().len() < other.fields().len() && other.fields().starts_with(self.fields())
    }
}

impl Display for Index {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fields = self.fields().join(", ");

        if self.is_unique() {
            if let Some(predicate) = self.predicate() {
                write!(f, "UNIQUE ({fields}) WHERE {predicate}")
            } else {
                write!(f, "UNIQUE ({fields})")
            }
        } else if let Some(predicate) = self.predicate() {
            write!(f, "({fields}) WHERE {predicate}")
        } else {
            write!(f, "({fields})")
        }
    }
}

impl MacroNode for Index {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl ValidateNode for Index {}

impl VisitableNode for Index {
    fn route_key(&self) -> String {
        self.fields().join(", ")
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::node::index::Index;

    #[test]
    fn index_with_predicate_reports_conditional_shape() {
        let index = Index::new_with_predicate(&["email"], false, Some("active = true"));

        assert_eq!(index.predicate(), Some("active = true"));
        assert_eq!(index.to_string(), "(email) WHERE active = true");
    }

    #[test]
    fn index_without_predicate_preserves_legacy_shape() {
        let index = Index::new(&["email"], true);

        assert_eq!(index.predicate(), None);
        assert_eq!(index.to_string(), "UNIQUE (email)");
    }
}
