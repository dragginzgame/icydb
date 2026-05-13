//! Module: db::schema::capabilities
//! Responsibility: SQL capability projection from persisted schema field kinds.
//! Does not own: SQL lowering, query planning, or executor routing.
//! Boundary: classifies what SQL may request from accepted live schema fields.

use crate::db::schema::PersistedFieldKind;

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
}

impl SqlCapabilities {
    /// Build one SQL capability set from explicit facts.
    #[must_use]
    const fn new(flags: u8, aggregate_input: SqlAggregateInputCapabilities) -> Self {
        Self {
            flags,
            aggregate_input,
        }
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
}

///
/// PersistedSqlScalarFamily
///
/// Internal scalar-family projection for persisted field kinds.
/// The families are deliberately coarse and exist only to derive SQL
/// capabilities without converting back into generated `FieldKind`.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PersistedSqlScalarFamily {
    Boolean,
    Numeric { arithmetic: bool },
    Text,
    OrderedOpaque,
    Opaque,
    Unit,
}

impl PersistedSqlScalarFamily {
    // Return true when SQL may compare this scalar with equality predicates.
    const fn comparable(self) -> bool {
        !matches!(self, Self::Unit)
    }

    // Return true when SQL may use this scalar in ORDER BY.
    const fn orderable(self) -> bool {
        matches!(
            self,
            Self::Boolean | Self::Numeric { .. } | Self::Text | Self::OrderedOpaque
        )
    }

    // Return true when SQL may use this scalar as grouping identity.
    const fn groupable(self) -> bool {
        !matches!(self, Self::Unit)
    }

    // Return true when SQL `SUM`/`AVG` style numeric aggregates may consume this scalar.
    const fn supports_numeric_aggregate(self) -> bool {
        matches!(self, Self::Numeric { arithmetic: true })
    }
}

/// Return the SQL capability projection for one persisted schema field kind.
#[must_use]
pub(in crate::db) fn sql_capabilities(kind: &PersistedFieldKind) -> SqlCapabilities {
    match persisted_sql_scalar_family(kind) {
        Some(family) => sql_capabilities_for_scalar_family(family),
        None => sql_capabilities_for_non_scalar(kind),
    }
}

const fn sql_capabilities_for_scalar_family(family: PersistedSqlScalarFamily) -> SqlCapabilities {
    let comparable = family.comparable();
    let orderable = family.orderable();
    let groupable = family.groupable();
    let numeric = family.supports_numeric_aggregate();
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

fn sql_capabilities_for_non_scalar(kind: &PersistedFieldKind) -> SqlCapabilities {
    match kind {
        PersistedFieldKind::List(_)
        | PersistedFieldKind::Set(_)
        | PersistedFieldKind::Map { .. } => SqlCapabilities::new(
            SQL_CAPABILITY_SELECTABLE,
            SqlAggregateInputCapabilities::new(false, false, false),
        ),
        PersistedFieldKind::Structured { queryable } => SqlCapabilities::new(
            if *queryable {
                SQL_CAPABILITY_SELECTABLE
            } else {
                0
            },
            SqlAggregateInputCapabilities::new(false, false, false),
        ),
        PersistedFieldKind::Account
        | PersistedFieldKind::Blob { .. }
        | PersistedFieldKind::Bool
        | PersistedFieldKind::Date
        | PersistedFieldKind::Decimal { .. }
        | PersistedFieldKind::Duration
        | PersistedFieldKind::Enum { .. }
        | PersistedFieldKind::Float32
        | PersistedFieldKind::Float64
        | PersistedFieldKind::Int
        | PersistedFieldKind::Int128
        | PersistedFieldKind::IntBig
        | PersistedFieldKind::Principal
        | PersistedFieldKind::Subaccount
        | PersistedFieldKind::Text { .. }
        | PersistedFieldKind::Timestamp
        | PersistedFieldKind::Nat
        | PersistedFieldKind::Nat128
        | PersistedFieldKind::NatBig
        | PersistedFieldKind::Ulid
        | PersistedFieldKind::Unit
        | PersistedFieldKind::Relation { .. } => {
            unreachable!(
                "scalar persisted field kind should be handled by scalar-family projection"
            )
        }
    }
}

fn persisted_sql_scalar_family(kind: &PersistedFieldKind) -> Option<PersistedSqlScalarFamily> {
    match kind {
        PersistedFieldKind::Account
        | PersistedFieldKind::Date
        | PersistedFieldKind::Principal
        | PersistedFieldKind::Subaccount
        | PersistedFieldKind::Ulid => Some(PersistedSqlScalarFamily::OrderedOpaque),
        PersistedFieldKind::Blob { .. } => Some(PersistedSqlScalarFamily::Opaque),
        PersistedFieldKind::Bool => Some(PersistedSqlScalarFamily::Boolean),
        PersistedFieldKind::Decimal { .. }
        | PersistedFieldKind::Float32
        | PersistedFieldKind::Float64
        | PersistedFieldKind::Int
        | PersistedFieldKind::Int128
        | PersistedFieldKind::IntBig
        | PersistedFieldKind::Nat
        | PersistedFieldKind::Nat128
        | PersistedFieldKind::NatBig => {
            Some(PersistedSqlScalarFamily::Numeric { arithmetic: true })
        }
        PersistedFieldKind::Duration | PersistedFieldKind::Timestamp => {
            Some(PersistedSqlScalarFamily::Numeric { arithmetic: false })
        }
        PersistedFieldKind::Enum { .. } | PersistedFieldKind::Text { .. } => {
            Some(PersistedSqlScalarFamily::Text)
        }
        PersistedFieldKind::Unit => Some(PersistedSqlScalarFamily::Unit),
        PersistedFieldKind::Relation { key_kind, .. } => persisted_sql_scalar_family(key_kind),
        PersistedFieldKind::List(_)
        | PersistedFieldKind::Set(_)
        | PersistedFieldKind::Map { .. }
        | PersistedFieldKind::Structured { .. } => None,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::schema::{PersistedFieldKind, PersistedRelationStrength},
        types::EntityTag,
    };

    use crate::db::schema::capabilities::sql_capabilities;

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
    fn sql_capabilities_keep_blob_selectable_and_comparable_but_not_orderable() {
        let capabilities = sql_capabilities(&PersistedFieldKind::Blob { max_len: None });

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
        let amount = sql_capabilities(&PersistedFieldKind::Decimal { scale: 3 });
        let timestamp = sql_capabilities(&PersistedFieldKind::Timestamp);

        assert!(amount.aggregate_input().numeric());
        assert!(amount.aggregate_input().extrema());
        assert!(!timestamp.aggregate_input().numeric());
        assert!(timestamp.aggregate_input().extrema());
    }

    #[test]
    fn sql_capabilities_reject_collection_and_structured_predicates() {
        let list = sql_capabilities(&PersistedFieldKind::List(Box::new(
            PersistedFieldKind::Text { max_len: None },
        )));
        let structured = sql_capabilities(&PersistedFieldKind::Structured { queryable: false });

        assert!(list.selectable());
        assert!(!list.comparable());
        assert!(!list.orderable());
        assert!(!list.groupable());
        assert!(!structured.selectable());
        assert!(!structured.comparable());
    }

    #[test]
    fn sql_capabilities_relation_inherits_key_capabilities() {
        let relation = sql_capabilities(&relation_to_key(PersistedFieldKind::Nat));

        assert!(relation.selectable());
        assert!(relation.comparable());
        assert!(relation.orderable());
        assert!(relation.aggregate_input().numeric());
    }
}
