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
            | ScalarKind::Timestamp
            | ScalarKind::Nat
            | ScalarKind::Nat128
    )
}

/// Return true when arithmetic numeric aggregates may consume this kind.
const fn scalar_kind_supports_arithmetic_numeric(kind: ScalarKind) -> bool {
    kind.supports_arithmetic()
}

/// Return true when SQL equality predicates may compare this kind.
pub(in crate::db::schema) const fn scalar_kind_is_sql_comparable(kind: ScalarKind) -> bool {
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

    /// Return true when grouped equality can consume the persisted value as-is.
    #[must_use]
    pub(in crate::db) const fn has_identity_group_canonical_form(self) -> bool {
        matches!(self.category, AcceptedFieldKindCategory::Scalar(kind) if !matches!(kind, ScalarKind::Decimal | ScalarKind::Enum | ScalarKind::Unit))
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
mod tests;

#[cfg(test)]
mod helper_tests;
