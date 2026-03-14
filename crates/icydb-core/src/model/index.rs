//! Module: model::index
//! Responsibility: module-local ownership and contracts for model::index.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use std::fmt::{self, Display};

///
/// IndexKeyItem
///
/// Canonical index key-item metadata.
/// `Field` preserves legacy field-key behavior.
/// `Expression` reserves deterministic expression-key identity metadata.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexKeyItem {
    Field(&'static str),
    Expression(&'static str),
}

impl IndexKeyItem {
    /// Borrow this key-item's canonical text payload.
    #[must_use]
    pub const fn text(&self) -> &'static str {
        match self {
            Self::Field(field) | Self::Expression(field) => field,
        }
    }
}

///
/// IndexKeyItemsRef
///
/// Borrowed view over index key-item metadata.
/// Field-only indexes use `Fields`; mixed/explicit key metadata uses `Items`.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexKeyItemsRef {
    Fields(&'static [&'static str]),
    Items(&'static [IndexKeyItem]),
}

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
    key_items: Option<&'static [IndexKeyItem]>,
    unique: bool,
    // Raw schema-declared predicate text is input metadata only.
    // Runtime/planner semantics must flow through canonical_index_predicate(...).
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
        Self::new_with_key_items_and_predicate(name, store, fields, None, unique, None)
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
        Self::new_with_key_items_and_predicate(name, store, fields, None, unique, predicate)
    }

    /// Construct one index descriptor with explicit canonical key-item metadata.
    #[must_use]
    pub const fn new_with_key_items(
        name: &'static str,
        store: &'static str,
        fields: &'static [&'static str],
        key_items: &'static [IndexKeyItem],
        unique: bool,
    ) -> Self {
        Self::new_with_key_items_and_predicate(name, store, fields, Some(key_items), unique, None)
    }

    /// Construct one index descriptor with explicit key-item + predicate metadata.
    #[must_use]
    pub const fn new_with_key_items_and_predicate(
        name: &'static str,
        store: &'static str,
        fields: &'static [&'static str],
        key_items: Option<&'static [IndexKeyItem]>,
        unique: bool,
        predicate: Option<&'static str>,
    ) -> Self {
        Self {
            name,
            store,
            fields,
            key_items,
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

    /// Borrow canonical key-item metadata for this index.
    #[must_use]
    pub const fn key_items(&self) -> IndexKeyItemsRef {
        if let Some(items) = self.key_items {
            IndexKeyItemsRef::Items(items)
        } else {
            IndexKeyItemsRef::Fields(self.fields)
        }
    }

    /// Return whether this index includes expression key items.
    #[must_use]
    pub const fn has_expression_key_items(&self) -> bool {
        let Some(items) = self.key_items else {
            return false;
        };

        let mut index = 0usize;
        while index < items.len() {
            if matches!(items[index], IndexKeyItem::Expression(_)) {
                return true;
            }
            index = index.saturating_add(1);
        }

        false
    }

    /// Return whether the index enforces value uniqueness.
    #[must_use]
    pub const fn is_unique(&self) -> bool {
        self.unique
    }

    /// Return optional schema-declared conditional index predicate text metadata.
    ///
    /// This string is input-only and must be lowered through the canonical
    /// index-predicate boundary before semantic use.
    #[must_use]
    pub const fn predicate(&self) -> Option<&'static str> {
        self.predicate
    }

    /// Whether this index's field prefix matches the start of another index.
    #[must_use]
    pub fn is_prefix_of(&self, other: &Self) -> bool {
        self.fields().len() < other.fields().len() && other.fields().starts_with(self.fields())
    }

    fn joined_key_items(&self) -> String {
        match self.key_items() {
            IndexKeyItemsRef::Fields(fields) => fields.join(", "),
            IndexKeyItemsRef::Items(items) => items
                .iter()
                .map(IndexKeyItem::text)
                .collect::<Vec<_>>()
                .join(", "),
        }
    }
}

impl Display for IndexModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fields = self.joined_key_items();
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
    use crate::model::index::{IndexKeyItem, IndexKeyItemsRef, IndexModel};

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

    #[test]
    fn index_model_with_explicit_key_items_exposes_expression_items() {
        static KEY_ITEMS: [IndexKeyItem; 2] = [
            IndexKeyItem::Field("tenant_id"),
            IndexKeyItem::Expression("LOWER(email)"),
        ];
        let model = IndexModel::new_with_key_items(
            "users|tenant|email_expr",
            "users::index",
            &["tenant_id"],
            &KEY_ITEMS,
            false,
        );

        assert!(model.has_expression_key_items());
        assert_eq!(
            model.to_string(),
            "users|tenant|email_expr: users::index(tenant_id, LOWER(email))"
        );
        assert!(matches!(
            model.key_items(),
            IndexKeyItemsRef::Items(items)
                if items == KEY_ITEMS.as_slice()
        ));
    }
}
