use crate::prelude::*;
use std::{
    fmt::{self, Display},
    ops::Not,
};

///
/// IndexKeyItem
///
/// Canonical index key-item metadata.
/// `Field` preserves legacy field-key behavior.
/// `Expression` reserves deterministic expression-key identity metadata.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
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
/// Index
///

#[derive(Clone, Debug, Serialize)]
pub struct Index {
    fields: &'static [&'static str],

    #[serde(default, skip_serializing_if = "Option::is_none")]
    key_items: Option<&'static [IndexKeyItem]>,

    #[serde(default, skip_serializing_if = "Not::not")]
    unique: bool,

    // Raw predicate SQL remains input metadata until lowered into canonical
    // predicate semantics at runtime schema boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    predicate: Option<&'static str>,
}

impl Index {
    /// Build one index declaration from field-list and uniqueness metadata.
    #[must_use]
    pub const fn new(fields: &'static [&'static str], unique: bool) -> Self {
        Self::new_with_key_items_and_predicate(fields, None, unique, None)
    }

    /// Build one index declaration with optional conditional predicate metadata.
    #[must_use]
    pub const fn new_with_predicate(
        fields: &'static [&'static str],
        unique: bool,
        predicate: Option<&'static str>,
    ) -> Self {
        Self::new_with_key_items_and_predicate(fields, None, unique, predicate)
    }

    /// Build one index declaration with explicit canonical key-item metadata.
    #[must_use]
    pub const fn new_with_key_items(
        fields: &'static [&'static str],
        key_items: &'static [IndexKeyItem],
        unique: bool,
    ) -> Self {
        Self::new_with_key_items_and_predicate(fields, Some(key_items), unique, None)
    }

    /// Build one index declaration with explicit key items + predicate metadata.
    #[must_use]
    pub const fn new_with_key_items_and_predicate(
        fields: &'static [&'static str],
        key_items: Option<&'static [IndexKeyItem]>,
        unique: bool,
        predicate: Option<&'static str>,
    ) -> Self {
        Self {
            fields,
            key_items,
            unique,
            predicate,
        }
    }

    /// Borrow index field sequence.
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

    /// Return whether the index enforces uniqueness.
    #[must_use]
    pub const fn is_unique(&self) -> bool {
        self.unique
    }

    /// Return optional conditional-index predicate SQL metadata.
    ///
    /// This text is input-only; runtime/planner semantics must consume the
    /// canonical lowered predicate form.
    #[must_use]
    pub const fn predicate(&self) -> Option<&'static str> {
        self.predicate
    }

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

impl Display for Index {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fields = self.joined_key_items();

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
        self.joined_key_items()
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::node::index::{Index, IndexKeyItem, IndexKeyItemsRef};

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

    #[test]
    fn index_with_explicit_key_items_exposes_expression_items() {
        static KEY_ITEMS: [IndexKeyItem; 2] = [
            IndexKeyItem::Field("tenant_id"),
            IndexKeyItem::Expression("LOWER(email)"),
        ];
        let index = Index::new_with_key_items(&["tenant_id"], &KEY_ITEMS, false);

        assert!(index.has_expression_key_items());
        assert_eq!(index.to_string(), "(tenant_id, LOWER(email))");
        assert!(matches!(
            index.key_items(),
            IndexKeyItemsRef::Items(items)
                if items == KEY_ITEMS.as_slice()
        ));
    }
}
