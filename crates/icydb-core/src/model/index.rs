//! Module: model::index
//! Responsibility: module-local ownership and contracts for model::index.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::Predicate;
use std::fmt::{self, Display};

///
/// IndexExpression
///
/// Canonical deterministic expression key metadata for expression indexes.
/// This enum is semantic authority across schema/runtime/planner boundaries.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexExpression {
    Lower(&'static str),
    Upper(&'static str),
    Trim(&'static str),
    LowerTrim(&'static str),
    Date(&'static str),
    Year(&'static str),
    Month(&'static str),
    Day(&'static str),
}

impl IndexExpression {
    /// Borrow the referenced field for this expression key item.
    #[must_use]
    pub const fn field(&self) -> &'static str {
        match self {
            Self::Lower(field)
            | Self::Upper(field)
            | Self::Trim(field)
            | Self::LowerTrim(field)
            | Self::Date(field)
            | Self::Year(field)
            | Self::Month(field)
            | Self::Day(field) => field,
        }
    }

    /// Return one stable discriminant for fingerprint hashing.
    #[must_use]
    pub const fn kind_tag(&self) -> u8 {
        match self {
            Self::Lower(_) => 0x01,
            Self::Upper(_) => 0x02,
            Self::Trim(_) => 0x03,
            Self::LowerTrim(_) => 0x04,
            Self::Date(_) => 0x05,
            Self::Year(_) => 0x06,
            Self::Month(_) => 0x07,
            Self::Day(_) => 0x08,
        }
    }

    /// Return whether planner/access Eq/In lookup lowering supports this expression
    /// under `TextCasefold` coercion in the current release.
    #[must_use]
    pub const fn supports_text_casefold_lookup(&self) -> bool {
        matches!(self, Self::Lower(_) | Self::Upper(_))
    }
}

impl Display for IndexExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lower(field) => write!(f, "LOWER({field})"),
            Self::Upper(field) => write!(f, "UPPER({field})"),
            Self::Trim(field) => write!(f, "TRIM({field})"),
            Self::LowerTrim(field) => write!(f, "LOWER(TRIM({field}))"),
            Self::Date(field) => write!(f, "DATE({field})"),
            Self::Year(field) => write!(f, "YEAR({field})"),
            Self::Month(field) => write!(f, "MONTH({field})"),
            Self::Day(field) => write!(f, "DAY({field})"),
        }
    }
}

///
/// IndexKeyItem
///
/// Canonical index key-item metadata.
/// `Field` preserves field-key behavior.
/// `Expression` reserves deterministic expression-key identity metadata.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexKeyItem {
    Field(&'static str),
    Expression(IndexExpression),
}

impl IndexKeyItem {
    /// Borrow this key-item's referenced field.
    #[must_use]
    pub const fn field(&self) -> &'static str {
        match self {
            Self::Field(field) => field,
            Self::Expression(expression) => expression.field(),
        }
    }

    /// Render one deterministic canonical text form for diagnostics/display.
    #[must_use]
    pub fn canonical_text(&self) -> String {
        match self {
            Self::Field(field) => (*field).to_string(),
            Self::Expression(expression) => expression.to_string(),
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
/// GeneratedIndexPredicateResolver
///
/// Generated filtered indexes resolve canonical predicate semantics through
/// one zero-argument function so runtime planning can borrow a shared static
/// AST without reparsing SQL text.
///
pub type GeneratedIndexPredicateResolver = fn() -> &'static Predicate;

///
/// IndexPredicateMetadata
///
/// Canonical generated filtered-index predicate metadata.
/// Raw SQL text is retained for diagnostics/display only.
/// Runtime semantics always flow through `semantics()`.
///
#[derive(Clone, Copy, Debug)]
pub struct IndexPredicateMetadata {
    sql: &'static str,
    semantics: GeneratedIndexPredicateResolver,
}

impl IndexPredicateMetadata {
    /// Build one generated filtered-index predicate metadata bundle.
    #[must_use]
    #[doc(hidden)]
    pub const fn generated(sql: &'static str, semantics: GeneratedIndexPredicateResolver) -> Self {
        Self { sql, semantics }
    }

    /// Borrow the original schema-declared predicate text for diagnostics.
    #[must_use]
    pub const fn sql(&self) -> &'static str {
        self.sql
    }

    /// Borrow the canonical generated predicate semantics.
    #[must_use]
    pub fn semantics(&self) -> &'static Predicate {
        (self.semantics)()
    }
}

impl PartialEq for IndexPredicateMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.sql == other.sql && std::ptr::fn_addr_eq(self.semantics, other.semantics)
    }
}

impl Eq for IndexPredicateMetadata {}

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
    /// Stable per-entity ordinal used for runtime index identity.
    ordinal: u16,

    /// Stable index name used for diagnostics and planner identity.
    name: &'static str,
    store: &'static str,
    fields: &'static [&'static str],
    key_items: Option<&'static [IndexKeyItem]>,
    unique: bool,
    // Raw schema text remains for diagnostics/display only.
    // Runtime/planner semantics must use the generated canonical predicate AST.
    predicate: Option<IndexPredicateMetadata>,
}

impl IndexModel {
    /// Construct one generated index descriptor.
    ///
    /// This constructor exists for derive/codegen output and trusted test
    /// fixtures. Runtime planning and execution treat `IndexModel` values as
    /// build-time-validated metadata.
    #[must_use]
    #[doc(hidden)]
    pub const fn generated(
        name: &'static str,
        store: &'static str,
        fields: &'static [&'static str],
        unique: bool,
    ) -> Self {
        Self::generated_with_ordinal_and_key_items_and_predicate(
            0, name, store, fields, None, unique, None,
        )
    }

    /// Construct one index descriptor with one explicit stable ordinal.
    #[must_use]
    #[doc(hidden)]
    pub const fn generated_with_ordinal(
        ordinal: u16,
        name: &'static str,
        store: &'static str,
        fields: &'static [&'static str],
        unique: bool,
    ) -> Self {
        Self::generated_with_ordinal_and_key_items_and_predicate(
            ordinal, name, store, fields, None, unique, None,
        )
    }

    /// Construct one index descriptor with an optional conditional predicate.
    #[must_use]
    #[doc(hidden)]
    pub const fn generated_with_predicate(
        name: &'static str,
        store: &'static str,
        fields: &'static [&'static str],
        unique: bool,
        predicate: Option<IndexPredicateMetadata>,
    ) -> Self {
        Self::generated_with_ordinal_and_key_items_and_predicate(
            0, name, store, fields, None, unique, predicate,
        )
    }

    /// Construct one index descriptor with an explicit stable ordinal and optional predicate.
    #[must_use]
    #[doc(hidden)]
    pub const fn generated_with_ordinal_and_predicate(
        ordinal: u16,
        name: &'static str,
        store: &'static str,
        fields: &'static [&'static str],
        unique: bool,
        predicate: Option<IndexPredicateMetadata>,
    ) -> Self {
        Self::generated_with_ordinal_and_key_items_and_predicate(
            ordinal, name, store, fields, None, unique, predicate,
        )
    }

    /// Construct one index descriptor with explicit canonical key-item metadata.
    #[must_use]
    #[doc(hidden)]
    pub const fn generated_with_key_items(
        name: &'static str,
        store: &'static str,
        fields: &'static [&'static str],
        key_items: &'static [IndexKeyItem],
        unique: bool,
    ) -> Self {
        Self::generated_with_ordinal_and_key_items_and_predicate(
            0,
            name,
            store,
            fields,
            Some(key_items),
            unique,
            None,
        )
    }

    /// Construct one index descriptor with an explicit stable ordinal and key-item metadata.
    #[must_use]
    #[doc(hidden)]
    pub const fn generated_with_ordinal_and_key_items(
        ordinal: u16,
        name: &'static str,
        store: &'static str,
        fields: &'static [&'static str],
        key_items: &'static [IndexKeyItem],
        unique: bool,
    ) -> Self {
        Self::generated_with_ordinal_and_key_items_and_predicate(
            ordinal,
            name,
            store,
            fields,
            Some(key_items),
            unique,
            None,
        )
    }

    /// Construct one index descriptor with explicit key-item + predicate metadata.
    #[must_use]
    #[doc(hidden)]
    pub const fn generated_with_key_items_and_predicate(
        name: &'static str,
        store: &'static str,
        fields: &'static [&'static str],
        key_items: Option<&'static [IndexKeyItem]>,
        unique: bool,
        predicate: Option<IndexPredicateMetadata>,
    ) -> Self {
        Self::generated_with_ordinal_and_key_items_and_predicate(
            0, name, store, fields, key_items, unique, predicate,
        )
    }

    /// Construct one index descriptor with full explicit runtime identity metadata.
    #[must_use]
    #[doc(hidden)]
    pub const fn generated_with_ordinal_and_key_items_and_predicate(
        ordinal: u16,
        name: &'static str,
        store: &'static str,
        fields: &'static [&'static str],
        key_items: Option<&'static [IndexKeyItem]>,
        unique: bool,
        predicate: Option<IndexPredicateMetadata>,
    ) -> Self {
        Self {
            ordinal,
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

    /// Return the stable per-entity index ordinal.
    #[must_use]
    pub const fn ordinal(&self) -> u16 {
        self.ordinal
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
    /// Runtime planning and execution treat this as display metadata only.
    #[must_use]
    pub const fn predicate(&self) -> Option<&'static str> {
        match self.predicate {
            Some(predicate) => Some(predicate.sql()),
            None => None,
        }
    }

    /// Return the canonical generated conditional index predicate semantics.
    #[must_use]
    pub fn predicate_semantics(&self) -> Option<&'static Predicate> {
        self.predicate.map(|predicate| predicate.semantics())
    }

    /// Whether this index's field prefix matches the start of another index.
    #[must_use]
    pub fn is_prefix_of(&self, other: &Self) -> bool {
        self.fields().len() < other.fields().len() && other.fields().starts_with(self.fields())
    }

    fn joined_key_items(&self) -> String {
        match self.key_items() {
            IndexKeyItemsRef::Fields(fields) => fields.join(", "),
            IndexKeyItemsRef::Items(items) => {
                let mut joined = String::new();

                for item in items {
                    if !joined.is_empty() {
                        joined.push_str(", ");
                    }
                    joined.push_str(item.canonical_text().as_str());
                }

                joined
            }
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
    use crate::{
        db::Predicate,
        model::index::{
            IndexExpression, IndexKeyItem, IndexKeyItemsRef, IndexModel, IndexPredicateMetadata,
        },
    };
    use std::sync::LazyLock;

    static ACTIVE_TRUE_PREDICATE: LazyLock<Predicate> =
        LazyLock::new(|| Predicate::eq("active".to_string(), true.into()));

    fn active_true_predicate() -> &'static Predicate {
        &ACTIVE_TRUE_PREDICATE
    }

    #[test]
    fn index_model_with_predicate_exposes_predicate_metadata() {
        let model = IndexModel::generated_with_predicate(
            "users|email|active",
            "users::index",
            &["email"],
            false,
            Some(IndexPredicateMetadata::generated(
                "active = true",
                active_true_predicate,
            )),
        );

        assert_eq!(model.predicate(), Some("active = true"));
        assert_eq!(model.predicate_semantics(), Some(active_true_predicate()),);
        assert_eq!(
            model.to_string(),
            "users|email|active: users::index(email) WHERE active = true"
        );
    }

    #[test]
    fn index_model_without_predicate_preserves_display_shape() {
        let model = IndexModel::generated("users|email", "users::index", &["email"], true);

        assert_eq!(model.predicate(), None);
        assert_eq!(model.to_string(), "users|email: UNIQUE users::index(email)");
    }

    #[test]
    fn index_model_with_explicit_key_items_exposes_expression_items() {
        static KEY_ITEMS: [IndexKeyItem; 2] = [
            IndexKeyItem::Field("tenant_id"),
            IndexKeyItem::Expression(IndexExpression::Lower("email")),
        ];
        let model = IndexModel::generated_with_key_items(
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

    #[test]
    fn index_expression_lookup_support_matrix_is_explicit() {
        assert!(IndexExpression::Lower("email").supports_text_casefold_lookup());
        assert!(IndexExpression::Upper("email").supports_text_casefold_lookup());
        assert!(!IndexExpression::Trim("email").supports_text_casefold_lookup());
        assert!(!IndexExpression::LowerTrim("email").supports_text_casefold_lookup());
        assert!(!IndexExpression::Date("created_at").supports_text_casefold_lookup());
        assert!(!IndexExpression::Year("created_at").supports_text_casefold_lookup());
        assert!(!IndexExpression::Month("created_at").supports_text_casefold_lookup());
        assert!(!IndexExpression::Day("created_at").supports_text_casefold_lookup());
    }
}
