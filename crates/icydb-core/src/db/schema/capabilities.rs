//! Module: db::schema::capabilities
//! Responsibility: SQL capability projection from persisted schema field kinds.
//! Does not own: SQL lowering, query planning, or executor routing.
//! Boundary: classifies what SQL may request from accepted live schema fields.

#[cfg(feature = "sql")]
use crate::db::schema::AcceptedEnumCatalog;
#[cfg(feature = "sql")]
use crate::db::schema::enum_catalog::{EqualityCapability, enum_equality_capability};
use crate::db::schema::{
    AcceptedFieldKind, AcceptedFieldKindCategory, AcceptedFieldKindSemantics,
    classify_accepted_field_kind,
};
#[cfg(feature = "sql")]
use crate::model::{classify_field_kind, field::FieldKind};

const SQL_CAPABILITY_SELECTABLE: u8 = 1 << 0;
const SQL_CAPABILITY_COMPARABLE: u8 = 1 << 1;
const SQL_CAPABILITY_ORDERABLE: u8 = 1 << 2;
const SQL_CAPABILITY_GROUPABLE: u8 = 1 << 3;

///
/// SqlAggregateInputCapabilities
///
/// SQL aggregate input capability projection for one persisted field kind.
/// This keeps aggregate admission facts next to the persisted schema shape
/// instead of rebuilding them in SQL lowering or executor code.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct SqlAggregateInputCapabilities {
    count: bool,
    numeric: bool,
    extrema: bool,
}

impl SqlAggregateInputCapabilities {
    /// Build one aggregate input capability set from explicit facts.
    #[must_use]
    const fn new(count: bool, numeric: bool, extrema: bool) -> Self {
        Self {
            count,
            numeric,
            extrema,
        }
    }

    /// Return true when `COUNT(field)` may consume this field.
    #[must_use]
    pub(in crate::db) const fn count(self) -> bool {
        self.count
    }

    /// Return true when numeric aggregates such as `SUM`/`AVG` may consume this field.
    #[must_use]
    pub(in crate::db) const fn numeric(self) -> bool {
        self.numeric
    }

    /// Return true when extrema aggregates such as `MIN`/`MAX` may consume this field.
    #[must_use]
    pub(in crate::db) const fn extrema(self) -> bool {
        self.extrema
    }
}

///
/// SqlCapabilities
///
/// SQL operation capability projection for one persisted field kind.
/// The projection is derived from schema metadata only; query planning consumes
/// this from accepted schema views so SQL capability checks do not fall back to
/// generated field-kind tables.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct SqlCapabilities {
    flags: u8,
    aggregate_input: SqlAggregateInputCapabilities,
    #[cfg(feature = "sql")]
    enum_equality: Option<EqualityCapability>,
}

impl SqlCapabilities {
    /// Build one SQL capability set from explicit facts.
    #[must_use]
    const fn new(flags: u8, aggregate_input: SqlAggregateInputCapabilities) -> Self {
        Self {
            flags,
            aggregate_input,
            #[cfg(feature = "sql")]
            enum_equality: None,
        }
    }

    #[cfg(feature = "sql")]
    #[must_use]
    const fn with_enum_equality(mut self, capability: EqualityCapability) -> Self {
        self.enum_equality = Some(capability);
        self
    }

    /// Return true when SQL result projection may transport this field.
    #[must_use]
    pub(in crate::db) const fn selectable(self) -> bool {
        self.flags & SQL_CAPABILITY_SELECTABLE != 0
    }

    /// Return true when SQL equality-style predicates may compare this field.
    #[must_use]
    pub(in crate::db) const fn comparable(self) -> bool {
        self.flags & SQL_CAPABILITY_COMPARABLE != 0
    }

    /// Return true when SQL ordering predicates may order this field.
    #[must_use]
    pub(in crate::db) const fn orderable(self) -> bool {
        self.flags & SQL_CAPABILITY_ORDERABLE != 0
    }

    /// Return true when SQL grouping or DISTINCT may use this field as identity.
    #[must_use]
    pub(in crate::db) const fn groupable(self) -> bool {
        self.flags & SQL_CAPABILITY_GROUPABLE != 0
    }

    /// Return aggregate-input capabilities for this field.
    #[must_use]
    pub(in crate::db) const fn aggregate_input(self) -> SqlAggregateInputCapabilities {
        self.aggregate_input
    }

    /// Return accepted enum equality-key capability when this field is an enum.
    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db) const fn enum_equality(self) -> Option<EqualityCapability> {
        self.enum_equality
    }
}

/// Return the SQL capability projection for one persisted schema field kind.
#[must_use]
pub(in crate::db) fn sql_capabilities(kind: &AcceptedFieldKind) -> SqlCapabilities {
    let semantics = classify_accepted_field_kind(kind);
    match semantics.category() {
        AcceptedFieldKindCategory::Scalar(_) | AcceptedFieldKindCategory::Relation(Some(_)) => {
            sql_capabilities_for_scalar_semantics(semantics)
        }
        AcceptedFieldKindCategory::Relation(None) => {
            unreachable!("schema capability invariant")
        }
        AcceptedFieldKindCategory::Collection => SqlCapabilities::new(
            SQL_CAPABILITY_SELECTABLE,
            SqlAggregateInputCapabilities::new(false, false, false),
        ),
        AcceptedFieldKindCategory::Structured { queryable } => SqlCapabilities::new(
            if queryable {
                SQL_CAPABILITY_SELECTABLE
            } else {
                0
            },
            SqlAggregateInputCapabilities::new(false, false, false),
        ),
    }
}

/// Return coarse SQL capabilities for generated model-only schema views.
/// Accepted runtime views use `sql_capabilities` with catalog-resolved kinds.
#[cfg(feature = "sql")]
pub(in crate::db) fn sql_capabilities_for_model_kind(kind: &FieldKind) -> SqlCapabilities {
    match kind {
        FieldKind::List(_) | FieldKind::Set(_) | FieldKind::Map { .. } => SqlCapabilities::new(
            SQL_CAPABILITY_SELECTABLE,
            SqlAggregateInputCapabilities::new(false, false, false),
        ),
        FieldKind::Structured { queryable } => SqlCapabilities::new(
            if *queryable {
                SQL_CAPABILITY_SELECTABLE
            } else {
                0
            },
            SqlAggregateInputCapabilities::new(false, false, false),
        ),
        FieldKind::Relation { key_kind, .. } => sql_capabilities_for_model_kind(key_kind),
        _ => {
            let semantics = classify_field_kind(kind);
            let comparable = !matches!(kind, FieldKind::Unit);
            let numeric = semantics.supports_aggregate_numeric();
            let orderable = semantics.supports_aggregate_ordering();
            let groupable = comparable && !matches!(kind, FieldKind::Enum { .. });
            let mut flags = SQL_CAPABILITY_SELECTABLE;
            if comparable {
                flags |= SQL_CAPABILITY_COMPARABLE;
            }
            if orderable {
                flags |= SQL_CAPABILITY_ORDERABLE;
            }
            if groupable {
                flags |= SQL_CAPABILITY_GROUPABLE;
            }
            SqlCapabilities::new(
                flags,
                SqlAggregateInputCapabilities::new(comparable, numeric, orderable),
            )
        }
    }
}

/// Return SQL capabilities enriched by one verified accepted enum catalog.
#[cfg(feature = "sql")]
#[must_use]
pub(in crate::db) fn sql_capabilities_with_enum_catalog(
    kind: &AcceptedFieldKind,
    catalog: &AcceptedEnumCatalog,
) -> SqlCapabilities {
    let capabilities = sql_capabilities(kind);
    let AcceptedFieldKind::Enum { type_id } = kind else {
        return capabilities;
    };
    let capability =
        enum_equality_capability(catalog, *type_id).unwrap_or(EqualityCapability::PairwiseOnly);

    capabilities.with_enum_equality(capability)
}

const fn sql_capabilities_for_scalar_semantics(
    semantics: AcceptedFieldKindSemantics,
) -> SqlCapabilities {
    let comparable = semantics.is_sql_comparable();
    let orderable = semantics.is_orderable();
    let groupable = comparable && semantics.supports_stable_group_key();
    let numeric = semantics.is_numeric() && semantics.supports_arithmetic_numeric();
    let mut flags = SQL_CAPABILITY_SELECTABLE;
    if comparable {
        flags |= SQL_CAPABILITY_COMPARABLE;
    }
    if orderable {
        flags |= SQL_CAPABILITY_ORDERABLE;
    }
    if groupable {
        flags |= SQL_CAPABILITY_GROUPABLE;
    }

    SqlCapabilities::new(
        flags,
        SqlAggregateInputCapabilities::new(comparable, numeric, orderable),
    )
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::schema::{AcceptedFieldKind, AcceptedRelationEnforcement},
        types::EntityTag,
    };

    use crate::db::schema::capabilities::sql_capabilities;

    fn relation_to_key(key_kind: AcceptedFieldKind) -> AcceptedFieldKind {
        AcceptedFieldKind::Relation {
            target_path: "target::Entity".into(),
            target_entity_name: "Target".into(),
            target_entity_tag: EntityTag::new(77),
            target_store_path: "target::Store".into(),
            key_kind: Box::new(key_kind),
            enforcement: AcceptedRelationEnforcement::Unchecked,
        }
    }

    #[test]
    fn sql_capabilities_keep_blob_selectable_and_comparable_but_not_orderable() {
        let capabilities = sql_capabilities(&AcceptedFieldKind::Blob { max_len: None });

        assert!(capabilities.selectable());
        assert!(capabilities.comparable());
        assert!(!capabilities.orderable());
        assert!(capabilities.groupable());
        assert!(capabilities.aggregate_input().count());
        assert!(!capabilities.aggregate_input().numeric());
        assert!(!capabilities.aggregate_input().extrema());
    }

    #[test]
    fn sql_capabilities_keep_numeric_arithmetic_and_extrema_distinct() {
        let amount = sql_capabilities(&AcceptedFieldKind::Decimal { scale: 3 });
        let timestamp = sql_capabilities(&AcceptedFieldKind::Timestamp);

        assert!(amount.aggregate_input().numeric());
        assert!(amount.aggregate_input().extrema());
        assert!(!timestamp.aggregate_input().numeric());
        assert!(timestamp.aggregate_input().extrema());
    }

    #[test]
    fn sql_capabilities_keep_enum_equality_only_without_catalog_key_proof() {
        let capabilities = sql_capabilities(&AcceptedFieldKind::Enum {
            type_id: crate::value::EnumTypeId::new(1).expect("test enum type ID should be valid"),
        });

        assert!(capabilities.selectable());
        assert!(capabilities.comparable());
        assert!(!capabilities.orderable());
        assert!(!capabilities.groupable());
        #[cfg(feature = "sql")]
        assert_eq!(capabilities.enum_equality(), None);
        assert!(capabilities.aggregate_input().count());
        assert!(!capabilities.aggregate_input().numeric());
        assert!(!capabilities.aggregate_input().extrema());
    }

    #[test]
    fn sql_capabilities_reject_collection_and_structured_predicates() {
        let list = sql_capabilities(&AcceptedFieldKind::List(Box::new(
            AcceptedFieldKind::Text { max_len: None },
        )));
        let structured = sql_capabilities(&AcceptedFieldKind::Structured { queryable: false });

        assert!(list.selectable());
        assert!(!list.comparable());
        assert!(!list.orderable());
        assert!(!list.groupable());
        assert!(!structured.selectable());
        assert!(!structured.comparable());
    }

    #[test]
    fn sql_capabilities_relation_inherits_key_capabilities() {
        let relation = sql_capabilities(&relation_to_key(AcceptedFieldKind::Nat64));

        assert!(relation.selectable());
        assert!(relation.comparable());
        assert!(relation.orderable());
        assert!(relation.aggregate_input().numeric());
    }
}
