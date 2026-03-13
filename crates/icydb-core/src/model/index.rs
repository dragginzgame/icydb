//! Module: model::index
//! Responsibility: module-local ownership and contracts for model::index.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use std::fmt::{self, Display};

///
/// IndexModel
///
/// Runtime-only descriptor for an index used by the executor and stores.
/// Keeps core decoupled from the schema `Index` shape.
/// Indexing is hash-based over `Value` equality for all variants.
/// Unique indexes enforce value equality; hash collisions surface as corruption.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexModel {
    /// Stable index name used for diagnostics and planner identity.
    name: &'static str,
    store: &'static str,
    fields: &'static [&'static str],
    unique: bool,
    predicate: Option<&'static str>,
}

impl IndexModel {
    #[must_use]
    pub const fn new(
        name: &'static str,
        store: &'static str,
        fields: &'static [&'static str],
        unique: bool,
    ) -> Self {
        Self::new_with_predicate(name, store, fields, unique, None)
    }

    /// Construct one index descriptor with an optional conditional predicate.
    #[must_use]
    pub const fn new_with_predicate(
        name: &'static str,
        store: &'static str,
        fields: &'static [&'static str],
        unique: bool,
        predicate: Option<&'static str>,
    ) -> Self {
        Self {
            name,
            store,
            fields,
            unique,
            predicate,
        }
    }

    /// Return the stable index name.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Return the backing index store path.
    #[must_use]
    pub const fn store(&self) -> &'static str {
        self.store
    }

    /// Return the canonical index field list.
    #[must_use]
    pub const fn fields(&self) -> &'static [&'static str] {
        self.fields
    }

    /// Return whether the index enforces value uniqueness.
    #[must_use]
    pub const fn is_unique(&self) -> bool {
        self.unique
    }

    /// Return optional schema-declared conditional index predicate metadata.
    #[must_use]
    pub const fn predicate(&self) -> Option<&'static str> {
        self.predicate
    }

    /// Whether this index's field prefix matches the start of another index.
    #[must_use]
    pub fn is_prefix_of(&self, other: &Self) -> bool {
        self.fields().len() < other.fields().len() && other.fields().starts_with(self.fields())
    }
}

impl Display for IndexModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fields = self.fields().join(", ");
        if self.is_unique() {
            if let Some(predicate) = self.predicate() {
                write!(
                    f,
                    "{}: UNIQUE {}({}) WHERE {}",
                    self.name(),
                    self.store(),
                    fields,
                    predicate
                )
            } else {
                write!(f, "{}: UNIQUE {}({})", self.name(), self.store(), fields)
            }
        } else if let Some(predicate) = self.predicate() {
            write!(
                f,
                "{}: {}({}) WHERE {}",
                self.name(),
                self.store(),
                fields,
                predicate
            )
        } else {
            write!(f, "{}: {}({})", self.name(), self.store(), fields)
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::model::index::IndexModel;

    #[test]
    fn index_model_with_predicate_exposes_predicate_metadata() {
        let model = IndexModel::new_with_predicate(
            "users|email|active",
            "users::index",
            &["email"],
            false,
            Some("active = true"),
        );

        assert_eq!(model.predicate(), Some("active = true"));
        assert_eq!(
            model.to_string(),
            "users|email|active: users::index(email) WHERE active = true"
        );
    }

    #[test]
    fn index_model_without_predicate_preserves_legacy_display_shape() {
        let model = IndexModel::new("users|email", "users::index", &["email"], true);

        assert_eq!(model.predicate(), None);
        assert_eq!(model.to_string(), "users|email: UNIQUE users::index(email)");
    }
}
