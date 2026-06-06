//! Module: db::schema::field_kind_semantics
//! Responsibility: semantic classification for accepted persisted schema field kinds.
//! Does not own: SQL lowering, executor routing, or relation validation policy.
//! Boundary: exposes narrow persisted-kind facts consumed by schema-adjacent policy layers.

use crate::db::schema::PersistedFieldKind;

///
/// PersistedScalarClass
///
/// Schema-owned scalar semantic class for one accepted persisted field kind.
/// This is narrower than the full schema shape and exists so consumers can ask
/// semantic questions without rebuilding raw `PersistedFieldKind` ladders.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum PersistedScalarClass {
    Account,
    Blob,
    Bool,
    Date,
    Decimal,
    Duration,
    Enum,
    Float32,
    Float64,
    Signed64,
    Signed128,
    SignedBig,
    Principal,
    Subaccount,
    Text,
    Timestamp,
    Unsigned64,
    Unsigned128,
    UnsignedBig,
    Ulid,
    Unit,
}

impl PersistedScalarClass {
    /// Return true when the class carries numeric runtime semantics.
    #[must_use]
    const fn is_numeric(self) -> bool {
        matches!(
            self,
            Self::Decimal
                | Self::Duration
                | Self::Float32
                | Self::Float64
                | Self::Signed64
                | Self::Signed128
                | Self::SignedBig
                | Self::Timestamp
                | Self::Unsigned64
                | Self::Unsigned128
                | Self::UnsignedBig
        )
    }

    /// Return true when this is a signed numeric class.
    #[must_use]
    const fn is_signed_numeric(self) -> bool {
        matches!(self, Self::Signed64 | Self::Signed128 | Self::SignedBig)
    }

    /// Return true when this is an unsigned numeric class.
    #[must_use]
    const fn is_unsigned_numeric(self) -> bool {
        matches!(
            self,
            Self::Unsigned64 | Self::Unsigned128 | Self::UnsignedBig
        )
    }

    /// Return true when arithmetic numeric aggregates may consume this class.
    #[must_use]
    const fn supports_arithmetic_numeric(self) -> bool {
        matches!(
            self,
            Self::Decimal
                | Self::Float32
                | Self::Float64
                | Self::Signed64
                | Self::Signed128
                | Self::SignedBig
                | Self::Unsigned64
                | Self::Unsigned128
                | Self::UnsignedBig
        )
    }

    /// Return true when this class has stable scalar ordering.
    #[must_use]
    const fn is_orderable(self) -> bool {
        !matches!(self, Self::Blob | Self::Unit)
    }

    /// Return true when SQL equality predicates may compare this class.
    #[must_use]
    const fn is_sql_comparable(self) -> bool {
        !matches!(self, Self::Unit)
    }

    /// Return true when this class can encode as a persisted relation key component.
    #[must_use]
    const fn is_relation_key_eligible(self) -> bool {
        matches!(
            self,
            Self::Account
                | Self::Signed64
                | Self::Signed128
                | Self::Principal
                | Self::Subaccount
                | Self::Timestamp
                | Self::Unsigned64
                | Self::Unsigned128
                | Self::Ulid
                | Self::Unit
        )
    }
}

///
/// PersistedFieldKindCategory
///
/// Top-level persisted field-kind category. Relation fields retain the
/// classified scalar semantics of their key kind so consumers can delegate
/// through relation wrappers without treating relation fields as plain scalars.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum PersistedFieldKindCategory {
    Scalar(PersistedScalarClass),
    Relation(Option<PersistedScalarClass>),
    Collection,
    Structured { queryable: bool },
}

impl PersistedFieldKindCategory {
    #[must_use]
    const fn scalar_class(self) -> Option<PersistedScalarClass> {
        match self {
            Self::Scalar(class) | Self::Relation(Some(class)) => Some(class),
            Self::Relation(None) | Self::Collection | Self::Structured { .. } => None,
        }
    }
}

///
/// PersistedFieldKindSemantics
///
/// Narrow semantic contract for one accepted persisted schema field kind.
/// The contract describes the persisted kind only; SQL, executor, and relation
/// layers remain responsible for their own admission and execution policy.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct PersistedFieldKindSemantics {
    category: PersistedFieldKindCategory,
}

impl PersistedFieldKindSemantics {
    #[must_use]
    const fn new(category: PersistedFieldKindCategory) -> Self {
        Self { category }
    }

    /// Return the top-level persisted kind category.
    #[must_use]
    pub(in crate::db) const fn category(self) -> PersistedFieldKindCategory {
        self.category
    }

    /// Return true when the field kind itself is scalar.
    #[must_use]
    pub(in crate::db) const fn is_scalar(self) -> bool {
        matches!(self.category, PersistedFieldKindCategory::Scalar(_))
    }

    /// Return true when the field kind or relation key carries numeric semantics.
    #[must_use]
    pub(in crate::db) const fn is_numeric(self) -> bool {
        match self.category.scalar_class() {
            Some(class) => class.is_numeric(),
            None => false,
        }
    }

    /// Return true when the field kind or relation key is signed numeric.
    #[must_use]
    pub(in crate::db) const fn is_signed_numeric(self) -> bool {
        match self.category.scalar_class() {
            Some(class) => class.is_signed_numeric(),
            None => false,
        }
    }

    /// Return true when the field kind or relation key is unsigned numeric.
    #[must_use]
    pub(in crate::db) const fn is_unsigned_numeric(self) -> bool {
        match self.category.scalar_class() {
            Some(class) => class.is_unsigned_numeric(),
            None => false,
        }
    }

    /// Return true when arithmetic numeric aggregates may consume this kind.
    #[must_use]
    pub(in crate::db) const fn supports_arithmetic_numeric(self) -> bool {
        match self.category.scalar_class() {
            Some(class) => class.supports_arithmetic_numeric(),
            None => false,
        }
    }

    /// Return true when the field kind or relation key has stable ordering.
    #[must_use]
    pub(in crate::db) const fn is_orderable(self) -> bool {
        match self.category.scalar_class() {
            Some(class) => class.is_orderable(),
            None => false,
        }
    }

    /// Return true when SQL equality predicates may compare this kind.
    #[must_use]
    pub(in crate::db) const fn is_sql_comparable(self) -> bool {
        match self.category.scalar_class() {
            Some(class) => class.is_sql_comparable(),
            None => false,
        }
    }

    /// Return true when this kind can encode as a relation key component.
    #[must_use]
    pub(in crate::db) const fn is_relation_key_eligible(self) -> bool {
        match self.category.scalar_class() {
            Some(class) => class.is_relation_key_eligible(),
            None => false,
        }
    }

    /// Return true when the field kind is a collection.
    #[must_use]
    pub(in crate::db) const fn is_collection(self) -> bool {
        matches!(self.category, PersistedFieldKindCategory::Collection)
    }

    /// Return true when the field kind is structured.
    #[must_use]
    pub(in crate::db) const fn is_structured(self) -> bool {
        matches!(self.category, PersistedFieldKindCategory::Structured { .. })
    }
}

/// Classify one accepted persisted schema field kind.
#[must_use]
pub(in crate::db) const fn classify_persisted_field_kind(
    kind: &PersistedFieldKind,
) -> PersistedFieldKindSemantics {
    match kind {
        PersistedFieldKind::Account => PersistedFieldKindSemantics::new(
            PersistedFieldKindCategory::Scalar(PersistedScalarClass::Account),
        ),
        PersistedFieldKind::Blob { .. } => PersistedFieldKindSemantics::new(
            PersistedFieldKindCategory::Scalar(PersistedScalarClass::Blob),
        ),
        PersistedFieldKind::Bool => PersistedFieldKindSemantics::new(
            PersistedFieldKindCategory::Scalar(PersistedScalarClass::Bool),
        ),
        PersistedFieldKind::Date => PersistedFieldKindSemantics::new(
            PersistedFieldKindCategory::Scalar(PersistedScalarClass::Date),
        ),
        PersistedFieldKind::Decimal { .. } => PersistedFieldKindSemantics::new(
            PersistedFieldKindCategory::Scalar(PersistedScalarClass::Decimal),
        ),
        PersistedFieldKind::Duration => PersistedFieldKindSemantics::new(
            PersistedFieldKindCategory::Scalar(PersistedScalarClass::Duration),
        ),
        PersistedFieldKind::Enum { .. } => PersistedFieldKindSemantics::new(
            PersistedFieldKindCategory::Scalar(PersistedScalarClass::Enum),
        ),
        PersistedFieldKind::Float32 => PersistedFieldKindSemantics::new(
            PersistedFieldKindCategory::Scalar(PersistedScalarClass::Float32),
        ),
        PersistedFieldKind::Float64 => PersistedFieldKindSemantics::new(
            PersistedFieldKindCategory::Scalar(PersistedScalarClass::Float64),
        ),
        PersistedFieldKind::Int8
        | PersistedFieldKind::Int16
        | PersistedFieldKind::Int32
        | PersistedFieldKind::Int64 => PersistedFieldKindSemantics::new(
            PersistedFieldKindCategory::Scalar(PersistedScalarClass::Signed64),
        ),
        PersistedFieldKind::Int128 => PersistedFieldKindSemantics::new(
            PersistedFieldKindCategory::Scalar(PersistedScalarClass::Signed128),
        ),
        PersistedFieldKind::IntBig { .. } => PersistedFieldKindSemantics::new(
            PersistedFieldKindCategory::Scalar(PersistedScalarClass::SignedBig),
        ),
        PersistedFieldKind::Principal => PersistedFieldKindSemantics::new(
            PersistedFieldKindCategory::Scalar(PersistedScalarClass::Principal),
        ),
        PersistedFieldKind::Subaccount => PersistedFieldKindSemantics::new(
            PersistedFieldKindCategory::Scalar(PersistedScalarClass::Subaccount),
        ),
        PersistedFieldKind::Text { .. } => PersistedFieldKindSemantics::new(
            PersistedFieldKindCategory::Scalar(PersistedScalarClass::Text),
        ),
        PersistedFieldKind::Timestamp => PersistedFieldKindSemantics::new(
            PersistedFieldKindCategory::Scalar(PersistedScalarClass::Timestamp),
        ),
        PersistedFieldKind::Nat8
        | PersistedFieldKind::Nat16
        | PersistedFieldKind::Nat32
        | PersistedFieldKind::Nat64 => PersistedFieldKindSemantics::new(
            PersistedFieldKindCategory::Scalar(PersistedScalarClass::Unsigned64),
        ),
        PersistedFieldKind::Nat128 => PersistedFieldKindSemantics::new(
            PersistedFieldKindCategory::Scalar(PersistedScalarClass::Unsigned128),
        ),
        PersistedFieldKind::NatBig { .. } => PersistedFieldKindSemantics::new(
            PersistedFieldKindCategory::Scalar(PersistedScalarClass::UnsignedBig),
        ),
        PersistedFieldKind::Ulid => PersistedFieldKindSemantics::new(
            PersistedFieldKindCategory::Scalar(PersistedScalarClass::Ulid),
        ),
        PersistedFieldKind::Unit => PersistedFieldKindSemantics::new(
            PersistedFieldKindCategory::Scalar(PersistedScalarClass::Unit),
        ),
        PersistedFieldKind::Relation { key_kind, .. } => PersistedFieldKindSemantics::new(
            PersistedFieldKindCategory::Relation(classify_relation_scalar_class(key_kind)),
        ),
        PersistedFieldKind::List(_)
        | PersistedFieldKind::Set(_)
        | PersistedFieldKind::Map { .. } => {
            PersistedFieldKindSemantics::new(PersistedFieldKindCategory::Collection)
        }
        PersistedFieldKind::Structured { queryable } => {
            PersistedFieldKindSemantics::new(PersistedFieldKindCategory::Structured {
                queryable: *queryable,
            })
        }
    }
}

const fn classify_relation_scalar_class(kind: &PersistedFieldKind) -> Option<PersistedScalarClass> {
    match classify_persisted_field_kind(kind).category() {
        PersistedFieldKindCategory::Scalar(class)
        | PersistedFieldKindCategory::Relation(Some(class)) => Some(class),
        PersistedFieldKindCategory::Relation(None)
        | PersistedFieldKindCategory::Collection
        | PersistedFieldKindCategory::Structured { .. } => None,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{PersistedFieldKindCategory, PersistedScalarClass, classify_persisted_field_kind};
    use crate::{
        db::schema::{PersistedFieldKind, PersistedRelationStrength},
        types::EntityTag,
    };

    fn relation_to_key(key_kind: PersistedFieldKind) -> PersistedFieldKind {
        PersistedFieldKind::Relation {
            target_path: "target::Entity".into(),
            target_entity_name: "Target".into(),
            target_entity_tag: EntityTag::new(77),
            target_store_path: "target::Store".into(),
            key_kind: Box::new(key_kind),
            strength: PersistedRelationStrength::Weak,
        }
    }

    #[test]
    fn classify_persisted_numeric_scalar_kind() {
        let semantics = classify_persisted_field_kind(&PersistedFieldKind::Nat64);

        assert_eq!(
            semantics.category(),
            PersistedFieldKindCategory::Scalar(PersistedScalarClass::Unsigned64),
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
        let relation = relation_to_key(PersistedFieldKind::Nat128);
        let semantics = classify_persisted_field_kind(&relation);

        assert_eq!(
            semantics.category(),
            PersistedFieldKindCategory::Relation(Some(PersistedScalarClass::Unsigned128)),
        );
        assert!(!semantics.is_scalar());
        assert!(semantics.is_numeric());
        assert!(semantics.is_unsigned_numeric());
        assert!(semantics.is_orderable());
        assert!(semantics.is_sql_comparable());
        assert!(semantics.is_relation_key_eligible());
    }

    #[test]
    fn classify_collection_and_structured_kinds_stay_non_scalar() {
        let collection = classify_persisted_field_kind(&PersistedFieldKind::List(Box::new(
            PersistedFieldKind::Text { max_len: None },
        )));
        let structured =
            classify_persisted_field_kind(&PersistedFieldKind::Structured { queryable: true });

        assert!(collection.is_collection());
        assert!(!collection.is_scalar());
        assert!(!collection.is_sql_comparable());

        assert!(structured.is_structured());
        assert!(!structured.is_collection());
        assert!(!structured.is_orderable());
    }

    #[test]
    fn classify_scalar_edges_match_current_persisted_contracts() {
        let blob = classify_persisted_field_kind(&PersistedFieldKind::Blob { max_len: None });
        let unit = classify_persisted_field_kind(&PersistedFieldKind::Unit);
        let date = classify_persisted_field_kind(&PersistedFieldKind::Date);
        let timestamp = classify_persisted_field_kind(&PersistedFieldKind::Timestamp);
        let bigint = classify_persisted_field_kind(&PersistedFieldKind::IntBig { max_bytes: 32 });

        assert!(blob.is_sql_comparable());
        assert!(!blob.is_orderable());

        assert!(!unit.is_sql_comparable());
        assert!(unit.is_relation_key_eligible());

        assert!(date.is_orderable());
        assert!(!date.is_relation_key_eligible());

        assert!(timestamp.is_numeric());
        assert!(timestamp.is_relation_key_eligible());

        assert!(bigint.is_signed_numeric());
        assert!(!bigint.is_relation_key_eligible());
    }
}
