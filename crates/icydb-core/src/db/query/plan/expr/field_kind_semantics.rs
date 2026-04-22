//! Module: query::plan::expr::field_kind_semantics
//! Responsibility: planner-owned semantic classification for runtime `FieldKind`.
//! Does not own: predicate normalization, binding conversion, or executor policy.
//! Boundary: one semantic spine that adjacent layers consume instead of rebuilding ad hoc field-kind ladders.

use crate::model::field::FieldKind;

///
/// FieldKindNumericClass
///
/// Planner-owned numeric family projection for one field kind.
/// This keeps narrow-vs-wide-vs-float-vs-decimal distinctions explicit so
/// consumers can answer capability questions without rebuilding exact kind ladders.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum FieldKindNumericClass {
    Signed64,
    Unsigned64,
    SignedWide,
    UnsignedWide,
    FloatLike,
    DecimalLike,
    DurationLike,
    TimestampLike,
}

///
/// FieldKindScalarClass
///
/// Planner-owned scalar semantic family for one field kind.
/// This is the coarse semantic layer that downstream capability answers are
/// derived from instead of matching directly on `FieldKind` everywhere.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum FieldKindScalarClass {
    Boolean,
    Numeric(FieldKindNumericClass),
    Text,
    OrderedOpaque,
    Opaque,
}

///
/// FieldKindCategory
///
/// Planner-owned top-level field-kind category for semantic classification.
/// Relations keep their referenced scalar class explicit so consumers can
/// recurse semantically without hand-rolling relation-key ladders.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum FieldKindCategory {
    Scalar(FieldKindScalarClass),
    Relation(FieldKindScalarClass),
    Collection,
    Structured { queryable: bool },
}

impl FieldKindCategory {
    /// Return true when this category participates in planner arithmetic.
    #[must_use]
    pub(in crate::db) const fn supports_expr_numeric(self) -> bool {
        matches!(self, Self::Scalar(FieldKindScalarClass::Numeric(_)))
    }

    /// Return true when this category participates in numeric aggregates.
    #[must_use]
    pub(in crate::db) const fn supports_aggregate_numeric(self) -> bool {
        matches!(
            self,
            Self::Scalar(FieldKindScalarClass::Numeric(_))
                | Self::Relation(FieldKindScalarClass::Numeric(_))
        )
    }

    /// Return true when this category supports deterministic aggregate ordering.
    #[must_use]
    pub(in crate::db) const fn supports_aggregate_ordering(self) -> bool {
        match self {
            Self::Scalar(class) | Self::Relation(class) => scalar_class_supports_ordering(class),
            Self::Collection | Self::Structured { .. } => false,
        }
    }

    /// Return true when this category participates in predicate numeric widening.
    #[must_use]
    pub(in crate::db) const fn supports_predicate_numeric_widen(self) -> bool {
        matches!(
            self,
            Self::Scalar(FieldKindScalarClass::Numeric(
                FieldKindNumericClass::Signed64
                    | FieldKindNumericClass::Unsigned64
                    | FieldKindNumericClass::FloatLike
                    | FieldKindNumericClass::DecimalLike,
            )) | Self::Relation(FieldKindScalarClass::Numeric(
                FieldKindNumericClass::Signed64
                    | FieldKindNumericClass::Unsigned64
                    | FieldKindNumericClass::FloatLike
                    | FieldKindNumericClass::DecimalLike,
            ))
        )
    }
}

///
/// FieldKindSemantics
///
/// Planner-owned semantic contract for one `FieldKind`.
/// Consumers read capabilities and coarse family identity from this contract
/// instead of rebuilding interpretation ladders locally.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct FieldKindSemantics {
    category: FieldKindCategory,
}

impl FieldKindSemantics {
    /// Build one planner-owned field-kind semantic contract.
    #[must_use]
    pub(in crate::db) const fn new(category: FieldKindCategory) -> Self {
        Self { category }
    }

    /// Return the coarse semantic category for this field kind.
    #[must_use]
    pub(in crate::db) const fn category(self) -> FieldKindCategory {
        self.category
    }

    /// Return true when this field kind participates in planner arithmetic.
    #[must_use]
    pub(in crate::db) const fn supports_expr_numeric(self) -> bool {
        self.category.supports_expr_numeric()
    }

    /// Return true when this field kind participates in numeric aggregates.
    #[must_use]
    pub(in crate::db) const fn supports_aggregate_numeric(self) -> bool {
        self.category.supports_aggregate_numeric()
    }

    /// Return true when this field kind supports deterministic aggregate ordering.
    #[must_use]
    pub(in crate::db) const fn supports_aggregate_ordering(self) -> bool {
        self.category.supports_aggregate_ordering()
    }

    /// Return true when this field kind participates in predicate numeric widening.
    #[must_use]
    pub(in crate::db) const fn supports_predicate_numeric_widen(self) -> bool {
        self.category.supports_predicate_numeric_widen()
    }
}

/// Classify one runtime `FieldKind` through the planner-owned semantic contract.
#[must_use]
pub(in crate::db) const fn classify_field_kind(kind: &FieldKind) -> FieldKindSemantics {
    match kind {
        FieldKind::Account
        | FieldKind::Date
        | FieldKind::Principal
        | FieldKind::Subaccount
        | FieldKind::Ulid
        | FieldKind::Unit => FieldKindSemantics::new(FieldKindCategory::Scalar(
            FieldKindScalarClass::OrderedOpaque,
        )),
        FieldKind::Blob => {
            FieldKindSemantics::new(FieldKindCategory::Scalar(FieldKindScalarClass::Opaque))
        }
        FieldKind::Bool => {
            FieldKindSemantics::new(FieldKindCategory::Scalar(FieldKindScalarClass::Boolean))
        }
        FieldKind::Decimal { .. } => FieldKindSemantics::new(FieldKindCategory::Scalar(
            FieldKindScalarClass::Numeric(FieldKindNumericClass::DecimalLike),
        )),
        FieldKind::Duration => FieldKindSemantics::new(FieldKindCategory::Scalar(
            FieldKindScalarClass::Numeric(FieldKindNumericClass::DurationLike),
        )),
        FieldKind::Int => FieldKindSemantics::new(FieldKindCategory::Scalar(
            FieldKindScalarClass::Numeric(FieldKindNumericClass::Signed64),
        )),
        FieldKind::Int128 | FieldKind::IntBig => {
            FieldKindSemantics::new(FieldKindCategory::Scalar(FieldKindScalarClass::Numeric(
                FieldKindNumericClass::SignedWide,
            )))
        }
        FieldKind::Timestamp => FieldKindSemantics::new(FieldKindCategory::Scalar(
            FieldKindScalarClass::Numeric(FieldKindNumericClass::TimestampLike),
        )),
        FieldKind::Uint => FieldKindSemantics::new(FieldKindCategory::Scalar(
            FieldKindScalarClass::Numeric(FieldKindNumericClass::Unsigned64),
        )),
        FieldKind::Uint128 | FieldKind::UintBig => {
            FieldKindSemantics::new(FieldKindCategory::Scalar(FieldKindScalarClass::Numeric(
                FieldKindNumericClass::UnsignedWide,
            )))
        }
        FieldKind::Enum { .. } | FieldKind::Text => {
            FieldKindSemantics::new(FieldKindCategory::Scalar(FieldKindScalarClass::Text))
        }
        FieldKind::Float32 | FieldKind::Float64 => {
            FieldKindSemantics::new(FieldKindCategory::Scalar(FieldKindScalarClass::Numeric(
                FieldKindNumericClass::FloatLike,
            )))
        }
        FieldKind::Relation { key_kind, .. } => FieldKindSemantics::new(
            FieldKindCategory::Relation(classify_relation_scalar_class(key_kind)),
        ),
        FieldKind::List(_) | FieldKind::Map { .. } | FieldKind::Set(_) => {
            FieldKindSemantics::new(FieldKindCategory::Collection)
        }
        FieldKind::Structured { queryable } => {
            FieldKindSemantics::new(FieldKindCategory::Structured {
                queryable: *queryable,
            })
        }
    }
}

// Reduce one relation key kind onto the scalar semantic class that adjacent
// planner/executor capabilities are allowed to consume.
const fn classify_relation_scalar_class(kind: &FieldKind) -> FieldKindScalarClass {
    match classify_field_kind(kind).category() {
        FieldKindCategory::Scalar(class) | FieldKindCategory::Relation(class) => class,
        FieldKindCategory::Collection | FieldKindCategory::Structured { .. } => {
            FieldKindScalarClass::Opaque
        }
    }
}

// Keep ordering eligibility derived from one scalar semantic family instead of
// rebuilding ad hoc field-kind allowlists at each consumer.
const fn scalar_class_supports_ordering(class: FieldKindScalarClass) -> bool {
    !matches!(class, FieldKindScalarClass::Opaque)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::query::plan::expr::{
            FieldKindCategory, FieldKindNumericClass, FieldKindScalarClass, classify_field_kind,
        },
        model::field::FieldKind,
    };

    #[test]
    fn classify_numeric_scalar_field_kind() {
        let semantics = classify_field_kind(&FieldKind::Uint);

        assert_eq!(
            semantics.category(),
            FieldKindCategory::Scalar(FieldKindScalarClass::Numeric(
                FieldKindNumericClass::Unsigned64,
            )),
        );
        assert!(semantics.supports_expr_numeric());
        assert!(semantics.supports_aggregate_numeric());
        assert!(semantics.supports_aggregate_ordering());
        assert!(semantics.supports_predicate_numeric_widen());
    }

    #[test]
    fn classify_relation_uses_key_semantics_without_expr_numeric() {
        static UINT_KEY_KIND: FieldKind = FieldKind::Uint;
        static RELATION_KIND: FieldKind = FieldKind::Relation {
            target_path: "demo::Target",
            target_entity_name: "Target",
            target_entity_tag: crate::types::EntityTag::new(1),
            target_store_path: "demo::store::TargetStore",
            key_kind: &UINT_KEY_KIND,
            strength: crate::model::field::RelationStrength::Strong,
        };

        let semantics = classify_field_kind(&RELATION_KIND);

        assert_eq!(
            semantics.category(),
            FieldKindCategory::Relation(FieldKindScalarClass::Numeric(
                FieldKindNumericClass::Unsigned64,
            )),
        );
        assert!(!semantics.supports_expr_numeric());
        assert!(semantics.supports_aggregate_numeric());
        assert!(semantics.supports_aggregate_ordering());
        assert!(semantics.supports_predicate_numeric_widen());
    }

    #[test]
    fn classify_collection_and_blob_stay_non_orderable() {
        let collection = classify_field_kind(&FieldKind::List(&FieldKind::Text));
        let blob = classify_field_kind(&FieldKind::Blob);

        assert_eq!(collection.category(), FieldKindCategory::Collection);
        assert!(!collection.supports_expr_numeric());
        assert!(!collection.supports_aggregate_ordering());

        assert_eq!(
            blob.category(),
            FieldKindCategory::Scalar(FieldKindScalarClass::Opaque),
        );
        assert!(!blob.supports_aggregate_ordering());
    }

    #[test]
    fn classify_wide_integer_and_temporal_kinds_keep_distinct_numeric_facets() {
        let wide = classify_field_kind(&FieldKind::Int128);
        let duration = classify_field_kind(&FieldKind::Duration);
        let timestamp = classify_field_kind(&FieldKind::Timestamp);

        assert_eq!(
            wide.category(),
            FieldKindCategory::Scalar(FieldKindScalarClass::Numeric(
                FieldKindNumericClass::SignedWide,
            )),
        );
        assert_eq!(
            duration.category(),
            FieldKindCategory::Scalar(FieldKindScalarClass::Numeric(
                FieldKindNumericClass::DurationLike,
            )),
        );
        assert_eq!(
            timestamp.category(),
            FieldKindCategory::Scalar(FieldKindScalarClass::Numeric(
                FieldKindNumericClass::TimestampLike,
            )),
        );

        assert!(!wide.supports_predicate_numeric_widen());
        assert!(!duration.supports_predicate_numeric_widen());
        assert!(!timestamp.supports_predicate_numeric_widen());
    }
}
