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

const SQL_CAPABILITY_SELECTABLE: u8 = 1 << 0;
const SQL_CAPABILITY_ORDERABLE: u8 = 1 << 2;
const SQL_CAPABILITY_GROUPABLE: u8 = 1 << 3;
const SQL_AGGREGATE_INPUT_COUNT: u8 = 1 << 0;
const SQL_AGGREGATE_INPUT_SUM: u8 = 1 << 1;
const SQL_AGGREGATE_INPUT_AVERAGE: u8 = 1 << 2;
const SQL_AGGREGATE_INPUT_EXTREMA: u8 = 1 << 3;

///
/// SqlAggregateInputCapabilities
///
/// SQL aggregate input capability projection for one persisted field kind.
/// This keeps aggregate admission facts next to the persisted schema shape
/// instead of rebuilding them in SQL lowering or executor code.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct SqlAggregateInputCapabilities {
    flags: u8,
}

impl SqlAggregateInputCapabilities {
    /// Build one aggregate input capability set from explicit facts.
    #[must_use]
    const fn new(flags: u8) -> Self {
        Self { flags }
    }

    /// Return true when `COUNT(field)` may consume this field.
    #[must_use]
    pub(in crate::db) const fn count(self) -> bool {
        self.flags & SQL_AGGREGATE_INPUT_COUNT != 0
    }

    /// Return true when `SUM(field)` may consume this field.
    #[must_use]
    pub(in crate::db) const fn sum(self) -> bool {
        self.flags & SQL_AGGREGATE_INPUT_SUM != 0
    }

    /// Return true when `AVG(field)` may consume this field.
    #[must_use]
    pub(in crate::db) const fn average(self) -> bool {
        self.flags & SQL_AGGREGATE_INPUT_AVERAGE != 0
    }

    /// Return true when extrema aggregates such as `MIN`/`MAX` may consume this field.
    #[must_use]
    pub(in crate::db) const fn extrema(self) -> bool {
        self.flags & SQL_AGGREGATE_INPUT_EXTREMA != 0
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
pub(in crate::db) const fn sql_capabilities(kind: &AcceptedFieldKind) -> SqlCapabilities {
    let semantics = classify_accepted_field_kind(kind);
    match semantics.category() {
        AcceptedFieldKindCategory::Scalar(_) | AcceptedFieldKindCategory::Relation(Some(_)) => {
            sql_capabilities_for_scalar_semantics(semantics)
        }
        AcceptedFieldKindCategory::Relation(None)
        | AcceptedFieldKindCategory::Collection
        | AcceptedFieldKindCategory::Composite => SqlCapabilities::new(
            SQL_CAPABILITY_SELECTABLE,
            SqlAggregateInputCapabilities::new(0),
        ),
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
    let sum = semantics.supports_arithmetic_numeric();
    let average = semantics.is_numeric() && sum;
    let mut flags = SQL_CAPABILITY_SELECTABLE;
    if orderable {
        flags |= SQL_CAPABILITY_ORDERABLE;
    }
    if groupable {
        flags |= SQL_CAPABILITY_GROUPABLE;
    }
    let mut aggregate_flags = 0;
    if comparable {
        aggregate_flags |= SQL_AGGREGATE_INPUT_COUNT;
    }
    if sum {
        aggregate_flags |= SQL_AGGREGATE_INPUT_SUM;
    }
    if average {
        aggregate_flags |= SQL_AGGREGATE_INPUT_AVERAGE;
    }
    if orderable {
        aggregate_flags |= SQL_AGGREGATE_INPUT_EXTREMA;
    }

    SqlCapabilities::new(flags, SqlAggregateInputCapabilities::new(aggregate_flags))
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{db::schema::AcceptedFieldKind, types::EntityTag};

    use crate::db::schema::capabilities::sql_capabilities;

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
    fn sql_capabilities_keep_blob_selectable_and_comparable_but_not_orderable() {
        let capabilities = sql_capabilities(&AcceptedFieldKind::Blob { max_len: None });

        assert!(capabilities.selectable());
        assert!(!capabilities.orderable());
        assert!(capabilities.groupable());
        assert!(capabilities.aggregate_input().count());
        assert!(!capabilities.aggregate_input().sum());
        assert!(!capabilities.aggregate_input().average());
        assert!(!capabilities.aggregate_input().extrema());
    }

    #[test]
    fn sql_capabilities_keep_numeric_arithmetic_and_extrema_distinct() {
        let amount = sql_capabilities(&AcceptedFieldKind::Decimal { scale: 3 });
        let timestamp = sql_capabilities(&AcceptedFieldKind::Timestamp);

        assert!(amount.aggregate_input().sum());
        assert!(amount.aggregate_input().average());
        assert!(amount.aggregate_input().extrema());
        assert!(!timestamp.aggregate_input().sum());
        assert!(!timestamp.aggregate_input().average());
        assert!(timestamp.aggregate_input().extrema());
    }

    #[test]
    fn sql_capabilities_keep_enum_equality_only_without_catalog_key_proof() {
        let capabilities = sql_capabilities(&AcceptedFieldKind::Enum {
            type_id: crate::value::EnumTypeId::new(1).expect("test enum type ID should be valid"),
        });

        assert!(capabilities.selectable());
        assert!(!capabilities.orderable());
        assert!(!capabilities.groupable());
        #[cfg(feature = "sql")]
        assert_eq!(capabilities.enum_equality(), None);
        assert!(capabilities.aggregate_input().count());
        assert!(!capabilities.aggregate_input().sum());
        assert!(!capabilities.aggregate_input().average());
        assert!(!capabilities.aggregate_input().extrema());
    }

    #[test]
    fn sql_capabilities_admit_unit_ordering_without_group_or_numeric_semantics() {
        let capabilities = sql_capabilities(&AcceptedFieldKind::Unit);

        assert!(capabilities.selectable());
        assert!(capabilities.orderable());
        assert!(!capabilities.groupable());
        assert!(!capabilities.aggregate_input().count());
        assert!(!capabilities.aggregate_input().sum());
        assert!(!capabilities.aggregate_input().average());
        assert!(capabilities.aggregate_input().extrema());
    }

    #[test]
    fn sql_capabilities_transport_collections_and_composites_without_scalar_operations() {
        let list = sql_capabilities(&AcceptedFieldKind::List(Box::new(
            AcceptedFieldKind::Text { max_len: None },
        )));
        let composite = sql_capabilities(&AcceptedFieldKind::test_composite());

        assert!(list.selectable());
        assert!(!list.orderable());
        assert!(!list.groupable());
        assert!(composite.selectable());
        assert!(!composite.orderable());
        assert!(!composite.groupable());
        assert!(!composite.aggregate_input().count());
    }

    #[test]
    fn sql_capabilities_relation_inherits_key_capabilities() {
        let relation = sql_capabilities(&relation_to_key(AcceptedFieldKind::Nat64));

        assert!(relation.selectable());
        assert!(relation.orderable());
        assert!(relation.aggregate_input().sum());
        assert!(relation.aggregate_input().average());
    }

    #[test]
    fn sql_capabilities_admit_u256_sum_without_numeric_widening_or_average() {
        let capabilities = sql_capabilities(&AcceptedFieldKind::U256);

        assert!(capabilities.selectable());
        assert!(capabilities.orderable());
        assert!(capabilities.groupable());
        assert!(capabilities.aggregate_input().sum());
        assert!(!capabilities.aggregate_input().average());
        assert!(capabilities.aggregate_input().extrema());
    }

    #[test]
    fn sql_capabilities_fail_closed_for_non_scalar_relation_keys() {
        let relation = sql_capabilities(&relation_to_key(AcceptedFieldKind::test_composite()));

        assert!(relation.selectable());
        assert!(!relation.orderable());
        assert!(!relation.groupable());
        assert!(!relation.aggregate_input().count());
    }
}
