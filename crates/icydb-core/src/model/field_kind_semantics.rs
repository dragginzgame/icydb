//! Module: model::field_kind_semantics
//! Responsibility: runtime field-kind semantic classification and lossless literal canonicalization for runtime `FieldKind`.
//! Does not own: predicate normalization or executor policy.
//! Boundary: one semantic spine that adjacent layers consume instead of rebuilding ad hoc field-kind ladders.

use crate::{
    model::field::FieldKind,
    types::{Account, Decimal, Float32, Float64, Int, Int128, Nat, Nat128, Principal, Ulid},
    value::{Value, ValueEnum},
};
use std::str::FromStr;

///
/// FieldKindNumericClass
///
/// Runtime model-owned numeric family projection for one field kind.
/// This keeps narrow-vs-wide-vs-float-vs-decimal distinctions explicit so
/// consumers can answer capability questions without rebuilding exact kind ladders.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FieldKindNumericClass {
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
/// Runtime model-owned scalar semantic family for one field kind.
/// This is the coarse semantic layer that downstream capability answers are
/// derived from instead of matching directly on `FieldKind` everywhere.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FieldKindScalarClass {
    Boolean,
    Numeric(FieldKindNumericClass),
    Text,
    OrderedOpaque,
    Opaque,
}

///
/// FieldKindCategory
///
/// Runtime model-owned top-level field-kind category for semantic classification.
/// Relations keep their referenced scalar class explicit so consumers can
/// recurse semantically without hand-rolling relation-key ladders.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FieldKindCategory {
    Scalar(FieldKindScalarClass),
    Relation(FieldKindScalarClass),
    Collection,
    Structured { queryable: bool },
}

impl FieldKindCategory {
    /// Return true when this category participates in numeric aggregates.
    #[must_use]
    pub(crate) const fn supports_aggregate_numeric(self) -> bool {
        matches!(
            self,
            Self::Scalar(FieldKindScalarClass::Numeric(_))
                | Self::Relation(FieldKindScalarClass::Numeric(_))
        )
    }

    /// Return true when this category supports deterministic aggregate ordering.
    #[must_use]
    pub(crate) const fn supports_aggregate_ordering(self) -> bool {
        match self {
            Self::Scalar(class) | Self::Relation(class) => scalar_class_supports_ordering(class),
            Self::Collection | Self::Structured { .. } => false,
        }
    }

    /// Return true when this category participates in predicate numeric widening.
    #[must_use]
    pub(crate) const fn supports_predicate_numeric_widen(self) -> bool {
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
/// Runtime model-owned semantic contract for one `FieldKind`.
/// Consumers read capabilities and coarse family identity from this contract
/// instead of rebuilding interpretation ladders locally.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FieldKindSemantics {
    category: FieldKindCategory,
}

impl FieldKindSemantics {
    /// Build one runtime model-owned field-kind semantic contract.
    #[must_use]
    pub(crate) const fn new(category: FieldKindCategory) -> Self {
        Self { category }
    }

    /// Return the coarse semantic category for this field kind.
    #[must_use]
    pub(crate) const fn category(self) -> FieldKindCategory {
        self.category
    }

    /// Return true when this field kind participates in numeric aggregates.
    #[must_use]
    pub(crate) const fn supports_aggregate_numeric(self) -> bool {
        self.category.supports_aggregate_numeric()
    }

    /// Return true when this field kind supports deterministic aggregate ordering.
    #[must_use]
    pub(crate) const fn supports_aggregate_ordering(self) -> bool {
        self.category.supports_aggregate_ordering()
    }

    /// Return true when this field kind participates in predicate numeric widening.
    #[must_use]
    pub(crate) const fn supports_predicate_numeric_widen(self) -> bool {
        self.category.supports_predicate_numeric_widen()
    }
}

/// Return true when one single grouped field kind already arrives in canonical
/// grouped-equality form.
#[must_use]
pub(crate) const fn field_kind_has_identity_group_canonical_form(kind: FieldKind) -> bool {
    !matches!(
        kind,
        FieldKind::Decimal { .. }
            | FieldKind::Enum { .. }
            | FieldKind::Relation { .. }
            | FieldKind::List(_)
            | FieldKind::Set(_)
            | FieldKind::Map { .. }
            | FieldKind::Structured { .. }
            | FieldKind::Unit
    )
}

/// Canonicalize one grouped-key compare literal against one grouped field kind
/// when the Int<->Uint conversion is lossless and unambiguous.
///
/// Both fluent grouped `HAVING` and SQL grouped `HAVING` bind through this
/// helper so those two surfaces cannot drift on grouped-key numeric literal
/// normalization again.
#[must_use]
pub(crate) fn canonicalize_grouped_having_numeric_literal_for_field_kind(
    field_kind: Option<FieldKind>,
    value: &Value,
) -> Option<Value> {
    canonicalize_lossless_field_literal_for_kind(field_kind?, value, false)
}

/// Convert one parsed strict SQL literal into the exact runtime `Value`
/// variant required by the field kind when that conversion is lossless and
/// unambiguous.
///
/// This keeps SQL string tokens usable for scalar key types like `Ulid`
/// without widening text coercion across the general predicate surface.
#[must_use]
pub(crate) fn canonicalize_strict_sql_literal_for_kind(
    kind: &FieldKind,
    value: &Value,
) -> Option<Value> {
    canonicalize_strict_sql_literal_for_kind_impl(*kind, value)
}

/// Convert one frontend filter literal into the exact runtime `Value` variant
/// required by the field kind when that conversion is lossless and
/// unambiguous.
///
/// This keeps the public filter wire contract string-backed while the
/// schema-aware query boundary still rehydrates typed IDs and numerics before
/// planner validation consumes the predicate.
#[must_use]
pub(crate) fn canonicalize_filter_literal_for_kind(
    kind: &FieldKind,
    value: &Value,
) -> Option<Value> {
    canonicalize_lossless_field_literal_for_kind(*kind, value, true)
}

/// Classify one runtime `FieldKind` through the runtime model-owned semantic contract.
#[must_use]
pub(crate) const fn classify_field_kind(kind: &FieldKind) -> FieldKindSemantics {
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
        FieldKind::Enum { .. } | FieldKind::Text { .. } => {
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

// Canonicalize one lossless field-key literal while keeping the grouped-key
// numeric path and SQL strict-literal path on one recursive field-kind owner.
#[expect(clippy::too_many_lines)]
fn canonicalize_lossless_field_literal_for_kind(
    kind: FieldKind,
    value: &Value,
    allow_text_ulid: bool,
) -> Option<Value> {
    match kind {
        FieldKind::Account => match value {
            Value::Account(inner) => Some(Value::Account(*inner)),
            Value::Text(inner) => Account::from_str(inner).ok().map(Value::Account),
            _ => None,
        },
        FieldKind::Bool => match value {
            Value::Bool(inner) => Some(Value::Bool(*inner)),
            _ => None,
        },
        FieldKind::Decimal { .. } => match value {
            Value::Decimal(inner) => Some(Value::Decimal(*inner)),
            Value::Text(inner) => Decimal::from_str(inner).ok().map(Value::Decimal),
            _ => None,
        },
        FieldKind::Enum { .. } => match value {
            Value::Enum(inner) => Some(Value::Enum(inner.clone())),
            Value::Text(inner) => Some(Value::Enum(ValueEnum::loose(inner))),
            _ => None,
        },
        FieldKind::Float32 => match value {
            Value::Float32(inner) => Some(Value::Float32(*inner)),
            Value::Text(inner) => inner
                .parse::<f32>()
                .ok()
                .and_then(Float32::try_new)
                .map(Value::Float32),
            _ => None,
        },
        FieldKind::Float64 => match value {
            Value::Float64(inner) => Some(Value::Float64(*inner)),
            Value::Text(inner) => inner
                .parse::<f64>()
                .ok()
                .and_then(Float64::try_new)
                .map(Value::Float64),
            _ => None,
        },
        FieldKind::Relation { key_kind, .. } => {
            canonicalize_lossless_field_literal_for_kind(*key_kind, value, allow_text_ulid)
        }
        FieldKind::Int => match value {
            Value::Int(inner) => Some(Value::Int(*inner)),
            Value::Uint(inner) => i64::try_from(*inner).ok().map(Value::Int),
            Value::Text(inner) => inner.parse::<i64>().ok().map(Value::Int),
            _ => None,
        },
        FieldKind::Int128 => match value {
            Value::Int128(inner) => Some(Value::Int128(*inner)),
            Value::Text(inner) => inner
                .parse::<i128>()
                .ok()
                .map(Int128::from)
                .map(Value::Int128),
            _ => None,
        },
        FieldKind::IntBig => match value {
            Value::IntBig(inner) => Some(Value::IntBig(inner.clone())),
            Value::Text(inner) => Int::from_str(inner).ok().map(Value::IntBig),
            _ => None,
        },
        FieldKind::List(inner) | FieldKind::Set(inner) => match value {
            Value::List(values) => Some(Value::List(
                values
                    .iter()
                    .map(|item| {
                        canonicalize_lossless_field_literal_for_kind(*inner, item, allow_text_ulid)
                            .unwrap_or_else(|| item.clone())
                    })
                    .collect(),
            )),
            _ => None,
        },
        FieldKind::Principal => match value {
            Value::Principal(inner) => Some(Value::Principal(*inner)),
            Value::Text(inner) => Principal::from_str(inner).ok().map(Value::Principal),
            _ => None,
        },
        FieldKind::Text { .. } => match value {
            Value::Text(inner) => Some(Value::Text(inner.clone())),
            _ => None,
        },
        FieldKind::Uint => match value {
            Value::Int(inner) => u64::try_from(*inner).ok().map(Value::Uint),
            Value::Uint(inner) => Some(Value::Uint(*inner)),
            Value::Text(inner) => inner.parse::<u64>().ok().map(Value::Uint),
            _ => None,
        },
        FieldKind::Uint128 => match value {
            Value::Uint128(inner) => Some(Value::Uint128(*inner)),
            Value::Text(inner) => inner
                .parse::<u128>()
                .ok()
                .map(Nat128::from)
                .map(Value::Uint128),
            _ => None,
        },
        FieldKind::UintBig => match value {
            Value::UintBig(inner) => Some(Value::UintBig(inner.clone())),
            Value::Text(inner) => Nat::from_str(inner).ok().map(Value::UintBig),
            _ => None,
        },
        FieldKind::Unit => match value {
            Value::Null | Value::Unit => Some(Value::Unit),
            _ => None,
        },
        FieldKind::Ulid if allow_text_ulid => match value {
            Value::Text(inner) => Ulid::from_str(inner).ok().map(Value::Ulid),
            Value::Ulid(inner) => Some(Value::Ulid(*inner)),
            _ => None,
        },
        _ => None,
    }
}

// Keep strict SQL literal canonicalization on its original narrow contract:
// it only upgrades parsed numeric tokens onto exact integer field kinds and
// adds the explicit text-to-ULID escape hatch that SQL literal syntax needs.
fn canonicalize_strict_sql_literal_for_kind_impl(kind: FieldKind, value: &Value) -> Option<Value> {
    match kind {
        FieldKind::Relation { key_kind, .. } => {
            canonicalize_strict_sql_literal_for_kind_impl(*key_kind, value)
        }
        FieldKind::Int => match value {
            Value::Uint(inner) => i64::try_from(*inner).ok().map(Value::Int),
            _ => None,
        },
        FieldKind::Uint => match value {
            Value::Int(inner) => u64::try_from(*inner).ok().map(Value::Uint),
            _ => None,
        },
        FieldKind::Ulid => match value {
            Value::Text(inner) => Ulid::from_str(inner).ok().map(Value::Ulid),
            _ => None,
        },
        FieldKind::List(inner) | FieldKind::Set(inner) => match value {
            Value::List(values) => values
                .iter()
                .map(|item| canonicalize_strict_sql_literal_for_kind_impl(*inner, item))
                .collect::<Option<Vec<_>>>()
                .map(Value::List),
            _ => None,
        },
        _ => None,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        model::{
            FieldKindCategory, FieldKindNumericClass, FieldKindScalarClass,
            canonicalize_grouped_having_numeric_literal_for_field_kind,
            canonicalize_strict_sql_literal_for_kind, classify_field_kind, field::FieldKind,
            field_kind_has_identity_group_canonical_form,
        },
        value::Value,
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
        assert!(semantics.supports_aggregate_numeric());
        assert!(semantics.supports_aggregate_ordering());
        assert!(semantics.supports_predicate_numeric_widen());
    }

    #[test]
    fn classify_collection_and_blob_stay_non_orderable() {
        let collection = classify_field_kind(&FieldKind::List(&FieldKind::Text { max_len: None }));
        let blob = classify_field_kind(&FieldKind::Blob);

        assert_eq!(collection.category(), FieldKindCategory::Collection);
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

    #[test]
    fn grouped_field_kind_helpers_keep_decimal_relation_and_unit_edges_explicit() {
        static UINT_KEY_KIND: FieldKind = FieldKind::Uint;
        static RELATION_KIND: FieldKind = FieldKind::Relation {
            target_path: "demo::Target",
            target_entity_name: "Target",
            target_entity_tag: crate::types::EntityTag::new(1),
            target_store_path: "demo::store::TargetStore",
            key_kind: &UINT_KEY_KIND,
            strength: crate::model::field::RelationStrength::Strong,
        };

        assert!(field_kind_has_identity_group_canonical_form(
            FieldKind::Text { max_len: None }
        ));
        assert!(!field_kind_has_identity_group_canonical_form(
            FieldKind::Decimal { scale: 2 }
        ));
        assert!(!field_kind_has_identity_group_canonical_form(RELATION_KIND));

        assert!(FieldKind::Decimal { scale: 2 }.supports_group_probe());
        assert!(RELATION_KIND.supports_group_probe());
        assert!(!FieldKind::Unit.supports_group_probe());
    }

    #[test]
    fn runtime_value_acceptance_recurses_through_nested_field_kinds() {
        static TEXT_KIND: FieldKind = FieldKind::Text { max_len: None };
        static UINT_KIND: FieldKind = FieldKind::Uint;
        static RELATION_KIND: FieldKind = FieldKind::Relation {
            target_path: "demo::Target",
            target_entity_name: "Target",
            target_entity_tag: crate::types::EntityTag::new(1),
            target_store_path: "demo::store::TargetStore",
            key_kind: &UINT_KIND,
            strength: crate::model::field::RelationStrength::Strong,
        };

        assert!(
            FieldKind::Map {
                key: &TEXT_KIND,
                value: &UINT_KIND,
            }
            .accepts_value(&Value::Map(vec![(Value::Text("a".into()), Value::Uint(1))]))
        );
        assert!(RELATION_KIND.accepts_value(&Value::Uint(9)));
        assert!(!FieldKind::List(&TEXT_KIND).accepts_value(&Value::List(vec![Value::Uint(1)])));
    }

    #[test]
    fn grouped_having_numeric_canonicalization_keeps_numeric_relation_recursion() {
        static UINT_KIND: FieldKind = FieldKind::Uint;
        static RELATION_KIND: FieldKind = FieldKind::Relation {
            target_path: "demo::Target",
            target_entity_name: "Target",
            target_entity_tag: crate::types::EntityTag::new(1),
            target_store_path: "demo::store::TargetStore",
            key_kind: &UINT_KIND,
            strength: crate::model::field::RelationStrength::Strong,
        };

        assert_eq!(
            canonicalize_grouped_having_numeric_literal_for_field_kind(
                Some(FieldKind::Int),
                &Value::Uint(7),
            ),
            Some(Value::Int(7)),
        );
        assert_eq!(
            canonicalize_grouped_having_numeric_literal_for_field_kind(
                Some(RELATION_KIND),
                &Value::Int(7),
            ),
            Some(Value::Uint(7)),
        );
        assert_eq!(
            canonicalize_grouped_having_numeric_literal_for_field_kind(
                Some(FieldKind::Ulid),
                &Value::Text("01ARZ3NDEKTSV4RRFFQ69G5FAV".into()),
            ),
            None,
        );
    }

    #[test]
    fn strict_sql_literal_canonicalization_adds_ulid_without_widening_other_kinds() {
        let ulid_text = "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_string();

        assert!(matches!(
            canonicalize_strict_sql_literal_for_kind(&FieldKind::Ulid, &Value::Text(ulid_text),),
            Some(Value::Ulid(_)),
        ));
        assert_eq!(
            canonicalize_strict_sql_literal_for_kind(&FieldKind::Uint, &Value::Int(4)),
            Some(Value::Uint(4)),
        );
        assert_eq!(
            canonicalize_strict_sql_literal_for_kind(
                &FieldKind::Text { max_len: None },
                &Value::Text("x".into())
            ),
            None,
        );
    }
}
