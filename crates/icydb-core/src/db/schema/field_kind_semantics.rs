//! Module: db::schema::field_kind_semantics
//! Responsibility: semantic classification for accepted persisted schema field kinds.
//! Does not own: SQL lowering, executor routing, or relation validation policy.
//! Boundary: exposes narrow persisted-kind facts consumed by schema-adjacent policy layers.

use crate::db::schema::AcceptedFieldKind;
use icydb_schema::ScalarKind;

/// Return true when this scalar kind carries numeric runtime semantics.
const fn scalar_kind_is_numeric(kind: ScalarKind) -> bool {
    matches!(
        kind,
        ScalarKind::Decimal
            | ScalarKind::Duration
            | ScalarKind::Float32
            | ScalarKind::Float64
            | ScalarKind::Int
            | ScalarKind::Int128
            | ScalarKind::IntBig
            | ScalarKind::Timestamp
            | ScalarKind::Nat
            | ScalarKind::Nat128
            | ScalarKind::NatBig
    )
}

#[cfg(test)]
const fn scalar_kind_is_signed_numeric(kind: ScalarKind) -> bool {
    matches!(
        kind,
        ScalarKind::Int | ScalarKind::Int128 | ScalarKind::IntBig
    )
}

#[cfg(test)]
const fn scalar_kind_is_unsigned_numeric(kind: ScalarKind) -> bool {
    matches!(
        kind,
        ScalarKind::Nat | ScalarKind::Nat128 | ScalarKind::NatBig
    )
}

/// Return true when arithmetic numeric aggregates may consume this kind.
const fn scalar_kind_supports_arithmetic_numeric(kind: ScalarKind) -> bool {
    matches!(
        kind,
        ScalarKind::Decimal
            | ScalarKind::Float32
            | ScalarKind::Float64
            | ScalarKind::Int
            | ScalarKind::Int128
            | ScalarKind::IntBig
            | ScalarKind::Nat
            | ScalarKind::Nat128
            | ScalarKind::NatBig
    )
}

/// Return true when SQL equality predicates may compare this kind.
const fn scalar_kind_is_sql_comparable(kind: ScalarKind) -> bool {
    !matches!(kind, ScalarKind::Unit)
}

/// Return true when this kind alone proves stable grouping-key bytes.
const fn scalar_kind_supports_stable_group_key(kind: ScalarKind) -> bool {
    !matches!(kind, ScalarKind::Enum | ScalarKind::Unit)
}

/// Return true when lossless predicate numeric widening supports this kind.
const fn scalar_kind_supports_predicate_numeric_widen(kind: ScalarKind) -> bool {
    matches!(
        kind,
        ScalarKind::Decimal
            | ScalarKind::Float32
            | ScalarKind::Float64
            | ScalarKind::Int
            | ScalarKind::Nat
    )
}

///
/// AcceptedFieldKindCategory
///
/// Top-level persisted field-kind category. Relation fields retain the
/// classified scalar semantics of their key kind so consumers can delegate
/// through relation wrappers without treating relation fields as plain scalars.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedFieldKindCategory {
    Scalar(ScalarKind),
    Relation(Option<ScalarKind>),
    Collection,
    Composite,
}

impl AcceptedFieldKindCategory {
    #[must_use]
    const fn scalar_kind(self) -> Option<ScalarKind> {
        match self {
            Self::Scalar(kind) | Self::Relation(Some(kind)) => Some(kind),
            Self::Relation(None) | Self::Collection | Self::Composite => None,
        }
    }
}

///
/// AcceptedFieldKindSemantics
///
/// Narrow semantic contract for one accepted persisted schema field kind.
/// The contract describes the persisted kind only; SQL, executor, and relation
/// layers remain responsible for their own admission and execution policy.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedFieldKindSemantics {
    category: AcceptedFieldKindCategory,
}

impl AcceptedFieldKindSemantics {
    #[must_use]
    const fn new(category: AcceptedFieldKindCategory) -> Self {
        Self { category }
    }

    /// Return the top-level persisted kind category.
    #[must_use]
    pub(in crate::db) const fn category(self) -> AcceptedFieldKindCategory {
        self.category
    }

    /// Return true when the field kind itself is scalar.
    #[must_use]
    pub(in crate::db) const fn is_scalar(self) -> bool {
        matches!(self.category, AcceptedFieldKindCategory::Scalar(_))
    }

    /// Return true when the field kind or relation key carries numeric semantics.
    #[must_use]
    pub(in crate::db) const fn is_numeric(self) -> bool {
        match self.category.scalar_kind() {
            Some(kind) => scalar_kind_is_numeric(kind),
            None => false,
        }
    }

    /// Return true when the field kind or relation key is signed numeric.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn is_signed_numeric(self) -> bool {
        match self.category.scalar_kind() {
            Some(kind) => scalar_kind_is_signed_numeric(kind),
            None => false,
        }
    }

    /// Return true when the field kind or relation key is unsigned numeric.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn is_unsigned_numeric(self) -> bool {
        match self.category.scalar_kind() {
            Some(kind) => scalar_kind_is_unsigned_numeric(kind),
            None => false,
        }
    }

    /// Return true when arithmetic numeric aggregates may consume this kind.
    #[must_use]
    pub(in crate::db) const fn supports_arithmetic_numeric(self) -> bool {
        match self.category.scalar_kind() {
            Some(kind) => scalar_kind_supports_arithmetic_numeric(kind),
            None => false,
        }
    }

    /// Return true when predicate comparison may use lossless numeric widening.
    #[must_use]
    pub(in crate::db) const fn supports_predicate_numeric_widen(self) -> bool {
        match self.category.scalar_kind() {
            Some(kind) => scalar_kind_supports_predicate_numeric_widen(kind),
            None => false,
        }
    }

    /// Return true when the field kind or relation key has stable ordering.
    #[must_use]
    pub(in crate::db) const fn is_orderable(self) -> bool {
        match self.category.scalar_kind() {
            Some(kind) => kind.supports_ordering(),
            None => false,
        }
    }

    /// Return true when SQL equality predicates may compare this kind.
    #[must_use]
    pub(in crate::db) const fn is_sql_comparable(self) -> bool {
        match self.category.scalar_kind() {
            Some(kind) => scalar_kind_is_sql_comparable(kind),
            None => false,
        }
    }

    /// Return true when this kind can encode as a relation key component.
    #[must_use]
    pub(in crate::db) const fn is_relation_key_eligible(self) -> bool {
        match self.category.scalar_kind() {
            Some(kind) => kind.is_primary_key_component_encodable(),
            None => false,
        }
    }

    /// Return true when grouping is safe without additional catalog evidence.
    #[must_use]
    pub(in crate::db) const fn supports_stable_group_key(self) -> bool {
        match self.category.scalar_kind() {
            Some(kind) => scalar_kind_supports_stable_group_key(kind),
            None => false,
        }
    }

    /// Return true when the field kind is a collection.
    #[must_use]
    pub(in crate::db) const fn is_collection(self) -> bool {
        matches!(self.category, AcceptedFieldKindCategory::Collection)
    }

    /// Return true when the field kind is an exact composite.
    #[must_use]
    pub(in crate::db) const fn is_composite(self) -> bool {
        matches!(self.category, AcceptedFieldKindCategory::Composite)
    }
}

/// Classify one accepted persisted schema field kind.
#[must_use]
pub(in crate::db) const fn classify_accepted_field_kind(
    kind: &AcceptedFieldKind,
) -> AcceptedFieldKindSemantics {
    match kind {
        AcceptedFieldKind::Account => {
            AcceptedFieldKindSemantics::new(AcceptedFieldKindCategory::Scalar(ScalarKind::Account))
        }
        AcceptedFieldKind::Blob { .. } => {
            AcceptedFieldKindSemantics::new(AcceptedFieldKindCategory::Scalar(ScalarKind::Blob))
        }
        AcceptedFieldKind::Bool => {
            AcceptedFieldKindSemantics::new(AcceptedFieldKindCategory::Scalar(ScalarKind::Bool))
        }
        AcceptedFieldKind::Date => {
            AcceptedFieldKindSemantics::new(AcceptedFieldKindCategory::Scalar(ScalarKind::Date))
        }
        AcceptedFieldKind::Decimal { .. } => {
            AcceptedFieldKindSemantics::new(AcceptedFieldKindCategory::Scalar(ScalarKind::Decimal))
        }
        AcceptedFieldKind::Duration => {
            AcceptedFieldKindSemantics::new(AcceptedFieldKindCategory::Scalar(ScalarKind::Duration))
        }
        AcceptedFieldKind::Enum { .. } => {
            AcceptedFieldKindSemantics::new(AcceptedFieldKindCategory::Scalar(ScalarKind::Enum))
        }
        AcceptedFieldKind::Float32 => {
            AcceptedFieldKindSemantics::new(AcceptedFieldKindCategory::Scalar(ScalarKind::Float32))
        }
        AcceptedFieldKind::Float64 => {
            AcceptedFieldKindSemantics::new(AcceptedFieldKindCategory::Scalar(ScalarKind::Float64))
        }
        AcceptedFieldKind::Int8
        | AcceptedFieldKind::Int16
        | AcceptedFieldKind::Int32
        | AcceptedFieldKind::Int64 => {
            AcceptedFieldKindSemantics::new(AcceptedFieldKindCategory::Scalar(ScalarKind::Int))
        }
        AcceptedFieldKind::Int128 => {
            AcceptedFieldKindSemantics::new(AcceptedFieldKindCategory::Scalar(ScalarKind::Int128))
        }
        AcceptedFieldKind::IntBig { .. } => {
            AcceptedFieldKindSemantics::new(AcceptedFieldKindCategory::Scalar(ScalarKind::IntBig))
        }
        AcceptedFieldKind::Principal => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(ScalarKind::Principal),
        ),
        AcceptedFieldKind::Subaccount => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(ScalarKind::Subaccount),
        ),
        AcceptedFieldKind::Text { .. } => {
            AcceptedFieldKindSemantics::new(AcceptedFieldKindCategory::Scalar(ScalarKind::Text))
        }
        AcceptedFieldKind::Timestamp => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(ScalarKind::Timestamp),
        ),
        AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64 => {
            AcceptedFieldKindSemantics::new(AcceptedFieldKindCategory::Scalar(ScalarKind::Nat))
        }
        AcceptedFieldKind::Nat128 => {
            AcceptedFieldKindSemantics::new(AcceptedFieldKindCategory::Scalar(ScalarKind::Nat128))
        }
        AcceptedFieldKind::NatBig { .. } => {
            AcceptedFieldKindSemantics::new(AcceptedFieldKindCategory::Scalar(ScalarKind::NatBig))
        }
        AcceptedFieldKind::Ulid => {
            AcceptedFieldKindSemantics::new(AcceptedFieldKindCategory::Scalar(ScalarKind::Ulid))
        }
        AcceptedFieldKind::Unit => {
            AcceptedFieldKindSemantics::new(AcceptedFieldKindCategory::Scalar(ScalarKind::Unit))
        }
        AcceptedFieldKind::U256 => {
            AcceptedFieldKindSemantics::new(AcceptedFieldKindCategory::Scalar(ScalarKind::U256))
        }
        AcceptedFieldKind::Relation { key_kind, .. } => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Relation(classify_relation_scalar_kind(key_kind)),
        ),
        AcceptedFieldKind::List(_) | AcceptedFieldKind::Set(_) | AcceptedFieldKind::Map { .. } => {
            AcceptedFieldKindSemantics::new(AcceptedFieldKindCategory::Collection)
        }
        AcceptedFieldKind::Composite { .. } => {
            AcceptedFieldKindSemantics::new(AcceptedFieldKindCategory::Composite)
        }
    }
}

const fn classify_relation_scalar_kind(kind: &AcceptedFieldKind) -> Option<ScalarKind> {
    match classify_accepted_field_kind(kind).category() {
        AcceptedFieldKindCategory::Scalar(kind)
        | AcceptedFieldKindCategory::Relation(Some(kind)) => Some(kind),
        AcceptedFieldKindCategory::Relation(None)
        | AcceptedFieldKindCategory::Collection
        | AcceptedFieldKindCategory::Composite => None,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{AcceptedFieldKindCategory, classify_accepted_field_kind};
    use crate::{db::schema::AcceptedFieldKind, types::EntityTag};
    use icydb_schema::ScalarKind;

    fn relation_to_key(key_kind: AcceptedFieldKind) -> AcceptedFieldKind {
        AcceptedFieldKind::Relation {
            target_path: "target::Entity".into(),
            target_entity_name: "Target".into(),
            target_entity_tag: EntityTag::new(77),
            target_store_path: "target::Store".into(),
            key_kind: Box::new(key_kind),
        }
    }

    #[test]
    fn classify_persisted_numeric_scalar_kind() {
        let semantics = classify_accepted_field_kind(&AcceptedFieldKind::Nat64);

        assert_eq!(
            semantics.category(),
            AcceptedFieldKindCategory::Scalar(ScalarKind::Nat),
        );
        assert!(semantics.is_scalar());
        assert!(semantics.is_numeric());
        assert!(!semantics.is_signed_numeric());
        assert!(semantics.is_unsigned_numeric());
        assert!(semantics.is_orderable());
        assert!(semantics.is_sql_comparable());
        assert!(semantics.is_relation_key_eligible());
    }

    #[test]
    fn classify_relation_delegates_to_key_semantics_without_becoming_scalar() {
        let relation = relation_to_key(AcceptedFieldKind::Nat128);
        let semantics = classify_accepted_field_kind(&relation);

        assert_eq!(
            semantics.category(),
            AcceptedFieldKindCategory::Relation(Some(ScalarKind::Nat128)),
        );
        assert!(!semantics.is_scalar());
        assert!(semantics.is_numeric());
        assert!(semantics.is_unsigned_numeric());
        assert!(semantics.is_orderable());
        assert!(semantics.is_sql_comparable());
        assert!(semantics.is_relation_key_eligible());
    }

    #[test]
    fn classify_collection_and_composite_kinds_stay_non_scalar() {
        let collection = classify_accepted_field_kind(&AcceptedFieldKind::List(Box::new(
            AcceptedFieldKind::Text { max_len: None },
        )));
        let composite = classify_accepted_field_kind(&AcceptedFieldKind::test_composite());

        assert!(collection.is_collection());
        assert!(!collection.is_scalar());
        assert!(!collection.is_sql_comparable());

        assert!(composite.is_composite());
        assert!(!composite.is_collection());
        assert!(!composite.is_orderable());
    }

    #[test]
    fn classify_scalar_edges_match_current_persisted_contracts() {
        let blob = classify_accepted_field_kind(&AcceptedFieldKind::Blob { max_len: None });
        let unit = classify_accepted_field_kind(&AcceptedFieldKind::Unit);
        let date = classify_accepted_field_kind(&AcceptedFieldKind::Date);
        let timestamp = classify_accepted_field_kind(&AcceptedFieldKind::Timestamp);
        let bigint = classify_accepted_field_kind(&AcceptedFieldKind::IntBig { max_bytes: 32 });

        assert!(blob.is_sql_comparable());
        assert!(!blob.is_orderable());

        assert!(!unit.is_sql_comparable());
        assert!(unit.is_orderable());
        assert!(unit.is_relation_key_eligible());

        assert!(date.is_orderable());
        assert!(!date.is_relation_key_eligible());

        assert!(timestamp.is_numeric());
        assert!(timestamp.is_relation_key_eligible());

        assert!(bigint.is_signed_numeric());
        assert!(!bigint.is_relation_key_eligible());
    }
}
