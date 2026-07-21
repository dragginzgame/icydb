//! Module: db::schema::field_kind_semantics
//! Responsibility: semantic classification for accepted persisted schema field kinds.
//! Does not own: SQL lowering, executor routing, or relation validation policy.
//! Boundary: exposes narrow persisted-kind facts consumed by schema-adjacent policy layers.

use crate::db::schema::AcceptedFieldKind;

///
/// AcceptedScalarClass
///
/// Schema-owned scalar semantic class for one accepted persisted field kind.
/// This is narrower than the full schema shape and exists so consumers can ask
/// semantic questions without rebuilding raw `AcceptedFieldKind` ladders.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedScalarClass {
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

impl AcceptedScalarClass {
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
        !matches!(self, Self::Blob | Self::Enum | Self::Unit)
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

    /// Return true when this coarse kind alone proves stable grouping-key bytes.
    #[must_use]
    const fn supports_stable_group_key(self) -> bool {
        !matches!(self, Self::Enum | Self::Unit)
    }

    /// Return true when lossless predicate numeric widening supports this class.
    #[must_use]
    const fn supports_predicate_numeric_widen(self) -> bool {
        matches!(
            self,
            Self::Decimal | Self::Float32 | Self::Float64 | Self::Signed64 | Self::Unsigned64
        )
    }
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
    Scalar(AcceptedScalarClass),
    Relation(Option<AcceptedScalarClass>),
    Collection,
    Composite,
}

impl AcceptedFieldKindCategory {
    #[must_use]
    const fn scalar_class(self) -> Option<AcceptedScalarClass> {
        match self {
            Self::Scalar(class) | Self::Relation(Some(class)) => Some(class),
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

    /// Return true when predicate comparison may use lossless numeric widening.
    #[must_use]
    pub(in crate::db) const fn supports_predicate_numeric_widen(self) -> bool {
        match self.category.scalar_class() {
            Some(class) => class.supports_predicate_numeric_widen(),
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

    /// Return true when grouping is safe without additional catalog evidence.
    #[must_use]
    pub(in crate::db) const fn supports_stable_group_key(self) -> bool {
        match self.category.scalar_class() {
            Some(class) => class.supports_stable_group_key(),
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
        AcceptedFieldKind::Account => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Account),
        ),
        AcceptedFieldKind::Blob { .. } => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Blob),
        ),
        AcceptedFieldKind::Bool => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Bool),
        ),
        AcceptedFieldKind::Date => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Date),
        ),
        AcceptedFieldKind::Decimal { .. } => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Decimal),
        ),
        AcceptedFieldKind::Duration => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Duration),
        ),
        AcceptedFieldKind::Enum { .. } => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Enum),
        ),
        AcceptedFieldKind::Float32 => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Float32),
        ),
        AcceptedFieldKind::Float64 => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Float64),
        ),
        AcceptedFieldKind::Int8
        | AcceptedFieldKind::Int16
        | AcceptedFieldKind::Int32
        | AcceptedFieldKind::Int64 => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Signed64),
        ),
        AcceptedFieldKind::Int128 => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Signed128),
        ),
        AcceptedFieldKind::IntBig { .. } => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::SignedBig),
        ),
        AcceptedFieldKind::Principal => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Principal),
        ),
        AcceptedFieldKind::Subaccount => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Subaccount),
        ),
        AcceptedFieldKind::Text { .. } => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Text),
        ),
        AcceptedFieldKind::Timestamp => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Timestamp),
        ),
        AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64 => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Unsigned64),
        ),
        AcceptedFieldKind::Nat128 => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Unsigned128),
        ),
        AcceptedFieldKind::NatBig { .. } => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::UnsignedBig),
        ),
        AcceptedFieldKind::Ulid => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Ulid),
        ),
        AcceptedFieldKind::Unit => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Unit),
        ),
        AcceptedFieldKind::Relation { key_kind, .. } => AcceptedFieldKindSemantics::new(
            AcceptedFieldKindCategory::Relation(classify_relation_scalar_class(key_kind)),
        ),
        AcceptedFieldKind::List(_) | AcceptedFieldKind::Set(_) | AcceptedFieldKind::Map { .. } => {
            AcceptedFieldKindSemantics::new(AcceptedFieldKindCategory::Collection)
        }
        AcceptedFieldKind::Composite { .. } => {
            AcceptedFieldKindSemantics::new(AcceptedFieldKindCategory::Composite)
        }
    }
}

const fn classify_relation_scalar_class(kind: &AcceptedFieldKind) -> Option<AcceptedScalarClass> {
    match classify_accepted_field_kind(kind).category() {
        AcceptedFieldKindCategory::Scalar(class)
        | AcceptedFieldKindCategory::Relation(Some(class)) => Some(class),
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
    use super::{AcceptedFieldKindCategory, AcceptedScalarClass, classify_accepted_field_kind};
    use crate::{db::schema::AcceptedFieldKind, types::EntityTag};

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
            AcceptedFieldKindCategory::Scalar(AcceptedScalarClass::Unsigned64),
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
            AcceptedFieldKindCategory::Relation(Some(AcceptedScalarClass::Unsigned128)),
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
        assert!(unit.is_relation_key_eligible());

        assert!(date.is_orderable());
        assert!(!date.is_relation_key_eligible());

        assert!(timestamp.is_numeric());
        assert!(timestamp.is_relation_key_eligible());

        assert!(bigint.is_signed_numeric());
        assert!(!bigint.is_relation_key_eligible());
    }
}
