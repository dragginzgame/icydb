//! Module: access::path
//! Responsibility: access-path contract types shared by planning/lowering/runtime.
//! Does not own: path validation or canonicalization policy.
//! Boundary: used by access-plan construction and executor interpretation.

#[cfg(test)]
use crate::model::index::IndexModel;
use crate::{
    db::{
        Predicate,
        predicate::{normalize, parse_sql_predicate},
        schema::{
            PersistedIndexExpressionOp, SchemaExpressionIndexInfo, SchemaExpressionIndexKeyItemInfo,
        },
    },
    model::index::{IndexExpression, IndexKeyItem, IndexKeyItemsRef},
    value::Value,
};
use std::ops::Bound;

///
/// AccessPathKind
///
/// Coarse semantic path discriminator for callers that need access shape
/// without borrowing or inspecting variant payloads.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AccessPathKind {
    ByKey,
    ByKeys,
    KeyRange,
    IndexPrefix,
    IndexMultiLookup,
    IndexRange,
    FullScan,
}

///
/// SemanticIndexAccessContract
///
/// Reduced secondary-index facts carried after planner selection.
/// Keeps runtime access consumers on accepted/schema-shaped index identity
/// and key metadata instead of reopening the full generated model surface.
///

#[derive(Clone, Debug)]
pub(crate) struct SemanticIndexAccessContract {
    pub(in crate::db::access) inner: std::sync::Arc<SemanticIndexAccessContractInner>,
}

#[derive(Debug)]
pub(in crate::db::access) struct SemanticIndexAccessContractInner {
    pub(in crate::db::access) ordinal: u16,
    pub(in crate::db::access) name: String,
    pub(in crate::db::access) store_path: String,
    pub(in crate::db::access) key_items: SemanticIndexKeyItems,
    pub(in crate::db::access) unique: bool,
    pub(in crate::db::access) predicate_semantics: Option<Predicate>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::access) enum SemanticIndexKeyItems {
    Fields(Vec<String>),
    Accepted(Vec<SemanticIndexKeyItem>),
    Static(IndexKeyItemsRef),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum SemanticIndexKeyItemsRef<'a> {
    Fields(&'a [String]),
    Accepted(&'a [SemanticIndexKeyItem]),
    Static(IndexKeyItemsRef),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum SemanticIndexKeyItemRef<'a> {
    Field(&'a str),
    Expression(IndexExpression),
    AcceptedExpression(&'a SemanticIndexExpression),
}

impl<'a> SemanticIndexKeyItemRef<'a> {
    #[must_use]
    pub(crate) const fn field(self) -> &'a str {
        match self {
            Self::Field(field) => field,
            Self::Expression(expression) => expression.field(),
            Self::AcceptedExpression(expression) => expression.field(),
        }
    }

    #[must_use]
    pub(crate) fn canonical_text(self) -> String {
        match self {
            Self::Field(field) => field.to_string(),
            Self::Expression(expression) => expression.to_string(),
            Self::AcceptedExpression(expression) => expression.canonical_order_text(),
        }
    }

    #[must_use]
    pub(crate) const fn is_expression(self) -> bool {
        matches!(self, Self::Expression(_) | Self::AcceptedExpression(_))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum SemanticIndexKeyItem {
    Field(String),
    Expression(SemanticIndexExpression),
}

impl SemanticIndexKeyItem {
    pub(crate) const fn as_ref(&self) -> SemanticIndexKeyItemRef<'_> {
        match self {
            Self::Field(field) => SemanticIndexKeyItemRef::Field(field.as_str()),
            Self::Expression(expression) => SemanticIndexKeyItemRef::AcceptedExpression(expression),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SemanticIndexExpression {
    op: PersistedIndexExpressionOp,
    field: String,
}

impl SemanticIndexExpression {
    #[must_use]
    pub(in crate::db) const fn new(op: PersistedIndexExpressionOp, field: String) -> Self {
        Self { op, field }
    }

    #[must_use]
    pub(in crate::db) const fn field(&self) -> &str {
        self.field.as_str()
    }

    #[must_use]
    pub(in crate::db) const fn op(&self) -> PersistedIndexExpressionOp {
        self.op
    }

    #[must_use]
    pub(in crate::db) const fn supports_text_casefold_lookup(&self) -> bool {
        matches!(
            self.op,
            PersistedIndexExpressionOp::Lower | PersistedIndexExpressionOp::Upper
        )
    }

    #[must_use]
    pub(in crate::db) fn canonical_order_text(&self) -> String {
        let field = self.field();

        match self.op {
            PersistedIndexExpressionOp::Lower => format!("LOWER({field})"),
            PersistedIndexExpressionOp::Upper => format!("UPPER({field})"),
            PersistedIndexExpressionOp::Trim => format!("TRIM({field})"),
            PersistedIndexExpressionOp::LowerTrim => format!("LOWER(TRIM({field}))"),
            PersistedIndexExpressionOp::Date => format!("DATE({field})"),
            PersistedIndexExpressionOp::Year => format!("YEAR({field})"),
            PersistedIndexExpressionOp::Month => format!("MONTH({field})"),
            PersistedIndexExpressionOp::Day => format!("DAY({field})"),
        }
    }
}

impl PartialEq for SemanticIndexAccessContract {
    fn eq(&self, other: &Self) -> bool {
        self.inner.ordinal == other.inner.ordinal
            && self.inner.name == other.inner.name
            && self.inner.store_path == other.inner.store_path
            && self.inner.key_items == other.inner.key_items
            && self.inner.unique == other.inner.unique
            && self.inner.predicate_semantics == other.inner.predicate_semantics
    }
}

impl Eq for SemanticIndexAccessContract {}

impl SemanticIndexAccessContract {
    #[must_use]
    pub(in crate::db) fn from_accepted_field_path_index(
        accepted: &crate::db::schema::SchemaIndexInfo,
    ) -> Self {
        Self {
            inner: std::sync::Arc::new(SemanticIndexAccessContractInner {
                ordinal: accepted.ordinal(),
                name: accepted.name().to_string(),
                store_path: accepted.store().to_string(),
                key_items: SemanticIndexKeyItems::Fields(
                    accepted
                        .fields()
                        .iter()
                        .map(|field| {
                            if field.path().len() <= 1 {
                                field.field_name().to_string()
                            } else {
                                field.path().join(".")
                            }
                        })
                        .collect(),
                ),
                unique: accepted.unique(),
                predicate_semantics: accepted_index_predicate_semantics(accepted),
            }),
        }
    }

    #[must_use]
    pub(in crate::db) fn from_accepted_expression_index(
        accepted: &SchemaExpressionIndexInfo,
    ) -> Self {
        Self {
            inner: std::sync::Arc::new(SemanticIndexAccessContractInner {
                ordinal: accepted.ordinal(),
                name: accepted.name().to_string(),
                store_path: accepted.store().to_string(),
                key_items: SemanticIndexKeyItems::Accepted(
                    accepted
                        .key_items()
                        .iter()
                        .map(accepted_expression_key_item)
                        .collect(),
                ),
                unique: accepted.unique(),
                predicate_semantics: accepted_index_predicate_semantics_from_sql(
                    accepted.predicate_sql(),
                ),
            }),
        }
    }

    #[must_use]
    pub(in crate::db) fn ordinal(&self) -> u16 {
        self.inner.ordinal
    }

    #[must_use]
    pub(in crate::db) fn name(&self) -> &str {
        self.inner.name.as_str()
    }

    #[must_use]
    pub(in crate::db) fn store_path(&self) -> &str {
        self.inner.store_path.as_str()
    }

    #[must_use]
    pub(in crate::db) fn key_items(&self) -> SemanticIndexKeyItemsRef<'_> {
        match &self.inner.key_items {
            SemanticIndexKeyItems::Fields(fields) => SemanticIndexKeyItemsRef::Fields(fields),
            SemanticIndexKeyItems::Accepted(items) => SemanticIndexKeyItemsRef::Accepted(items),
            SemanticIndexKeyItems::Static(items) => SemanticIndexKeyItemsRef::Static(*items),
        }
    }

    #[must_use]
    pub(in crate::db) fn key_arity(&self) -> usize {
        match &self.inner.key_items {
            SemanticIndexKeyItems::Fields(fields) => fields.len(),
            SemanticIndexKeyItems::Accepted(items) => items.len(),
            SemanticIndexKeyItems::Static(items) => match items {
                IndexKeyItemsRef::Fields(fields) => fields.len(),
                IndexKeyItemsRef::Items(items) => items.len(),
            },
        }
    }

    #[must_use]
    pub(in crate::db) fn key_item_at(&self, slot: usize) -> Option<SemanticIndexKeyItemRef<'_>> {
        match &self.inner.key_items {
            SemanticIndexKeyItems::Fields(fields) => {
                if slot < fields.len() {
                    Some(SemanticIndexKeyItemRef::Field(fields[slot].as_str()))
                } else {
                    None
                }
            }
            SemanticIndexKeyItems::Accepted(items) => {
                items.get(slot).map(SemanticIndexKeyItem::as_ref)
            }
            SemanticIndexKeyItems::Static(items) => match items {
                IndexKeyItemsRef::Fields(fields) => {
                    if slot < fields.len() {
                        Some(SemanticIndexKeyItemRef::Field(fields[slot]))
                    } else {
                        None
                    }
                }
                IndexKeyItemsRef::Items(items) => {
                    if slot < items.len() {
                        Some(match items[slot] {
                            IndexKeyItem::Field(field) => SemanticIndexKeyItemRef::Field(field),
                            IndexKeyItem::Expression(expression) => {
                                SemanticIndexKeyItemRef::Expression(expression)
                            }
                        })
                    } else {
                        None
                    }
                }
            },
        }
    }

    #[must_use]
    pub(in crate::db) fn key_field_at(&self, slot: usize) -> Option<&str> {
        match self.key_item_at(slot)? {
            SemanticIndexKeyItemRef::Field(field) => Some(field),
            SemanticIndexKeyItemRef::Expression(_)
            | SemanticIndexKeyItemRef::AcceptedExpression(_) => None,
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn fields(&self) -> Vec<String> {
        match self.key_items() {
            SemanticIndexKeyItemsRef::Fields(fields) => fields.to_vec(),
            SemanticIndexKeyItemsRef::Accepted(items) => items
                .iter()
                .map(|item| item.as_ref().field().to_string())
                .collect(),
            SemanticIndexKeyItemsRef::Static(IndexKeyItemsRef::Fields(fields)) => {
                fields.iter().copied().map(str::to_string).collect()
            }
            SemanticIndexKeyItemsRef::Static(IndexKeyItemsRef::Items(items)) => {
                items.iter().map(|item| item.field().to_string()).collect()
            }
        }
    }

    #[must_use]
    pub(in crate::db) fn is_unique(&self) -> bool {
        self.inner.unique
    }

    #[must_use]
    pub(in crate::db) fn is_filtered(&self) -> bool {
        self.inner.predicate_semantics.is_some()
    }

    #[must_use]
    pub(in crate::db) fn has_expression_key_items(&self) -> bool {
        match &self.inner.key_items {
            SemanticIndexKeyItems::Fields(_)
            | SemanticIndexKeyItems::Static(IndexKeyItemsRef::Fields(_)) => false,
            SemanticIndexKeyItems::Accepted(items) => items
                .iter()
                .any(|item| matches!(item, SemanticIndexKeyItem::Expression(_))),
            SemanticIndexKeyItems::Static(IndexKeyItemsRef::Items(items)) => {
                let mut index = 0usize;
                while index < items.len() {
                    if matches!(items[index], IndexKeyItem::Expression(_)) {
                        return true;
                    }
                    index = index.saturating_add(1);
                }

                false
            }
        }
    }

    #[must_use]
    pub(in crate::db) fn predicate_semantics(&self) -> Option<&Predicate> {
        self.inner.predicate_semantics.as_ref()
    }
}

fn accepted_expression_key_item(item: &SchemaExpressionIndexKeyItemInfo) -> SemanticIndexKeyItem {
    match item {
        SchemaExpressionIndexKeyItemInfo::FieldPath(field) => {
            SemanticIndexKeyItem::Field(accepted_field_path_term(field.field_name(), field.path()))
        }
        SchemaExpressionIndexKeyItemInfo::Expression(expression) => {
            SemanticIndexKeyItem::Expression(SemanticIndexExpression {
                op: expression.op(),
                field: accepted_field_path_term(
                    expression.source().field_name(),
                    expression.source().path(),
                ),
            })
        }
    }
}

fn accepted_field_path_term(field_name: &str, path: &[String]) -> String {
    if path.len() <= 1 {
        field_name.to_string()
    } else {
        path.join(".")
    }
}

fn accepted_index_predicate_semantics(
    accepted: &crate::db::schema::SchemaIndexInfo,
) -> Option<Predicate> {
    accepted_index_predicate_semantics_from_sql(accepted.predicate_sql())
}

fn accepted_index_predicate_semantics_from_sql(predicate_sql: Option<&str>) -> Option<Predicate> {
    let predicate_sql = predicate_sql?;

    Some(
        parse_sql_predicate(predicate_sql)
            .map_or(Predicate::False, |predicate| normalize(&predicate)),
    )
}

///
/// SemanticIndexRangeSpec
///
/// Semantic index-range request for one secondary index path.
/// Stores field-slot shape plus semantic bounds only; no encoded/raw key material.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SemanticIndexRangeSpec {
    index: SemanticIndexAccessContract,
    field_slots: Vec<usize>,
    prefix_values: Vec<Value>,
    lower: Bound<Value>,
    upper: Bound<Value>,
}

impl SemanticIndexRangeSpec {
    #[must_use]
    #[cfg(test)]
    pub(crate) fn new(
        index: IndexModel,
        field_slots: Vec<usize>,
        prefix_values: Vec<Value>,
        lower: Bound<Value>,
        upper: Bound<Value>,
    ) -> Self {
        debug_assert!(
            !field_slots.is_empty(),
            "semantic index-range field slots must include the range slot",
        );
        debug_assert_eq!(
            field_slots.len(),
            prefix_values.len().saturating_add(1),
            "semantic index-range slots must include one slot per prefix field plus range slot",
        );
        debug_assert!(
            prefix_values.len() < index.fields().len(),
            "semantic index-range prefix must be shorter than index arity",
        );

        Self {
            index: SemanticIndexAccessContract::model_only_from_generated_index(index),
            field_slots,
            prefix_values,
            lower,
            upper,
        }
    }

    #[must_use]
    pub(crate) fn from_access_contract(
        index: SemanticIndexAccessContract,
        field_slots: Vec<usize>,
        prefix_values: Vec<Value>,
        lower: Bound<Value>,
        upper: Bound<Value>,
    ) -> Self {
        debug_assert!(
            !field_slots.is_empty(),
            "semantic index-range field slots must include the range slot",
        );
        debug_assert_eq!(
            field_slots.len(),
            prefix_values.len().saturating_add(1),
            "semantic index-range slots must include one slot per prefix field plus range slot",
        );
        debug_assert!(
            prefix_values.len() < index.key_arity(),
            "semantic index-range prefix must be shorter than index arity",
        );

        Self {
            index,
            field_slots,
            prefix_values,
            lower,
            upper,
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn from_prefix_and_bounds(
        index: IndexModel,
        prefix_values: Vec<Value>,
        lower: Bound<Value>,
        upper: Bound<Value>,
    ) -> Self {
        let slot_count = prefix_values.len().saturating_add(1);
        let field_slots = (0..slot_count).collect();

        Self::new(index, field_slots, prefix_values, lower, upper)
    }

    #[must_use]
    pub(crate) fn index(&self) -> SemanticIndexAccessContract {
        self.index.clone()
    }

    #[must_use]
    pub(crate) const fn field_slots(&self) -> &[usize] {
        self.field_slots.as_slice()
    }

    #[must_use]
    pub(crate) const fn prefix_values(&self) -> &[Value] {
        self.prefix_values.as_slice()
    }

    #[must_use]
    pub(crate) const fn lower(&self) -> &Bound<Value> {
        &self.lower
    }

    #[must_use]
    pub(crate) const fn upper(&self) -> &Bound<Value> {
        &self.upper
    }
}

///
/// AccessPath
/// Concrete runtime access path selected by query planning.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AccessPath<K> {
    /// Direct lookup by a single primary key.
    ByKey(K),

    /// Batched lookup by multiple primary keys.
    ///
    /// Keys are treated as a set; order is canonicalized and duplicates are ignored.
    /// Empty key lists are a valid no-op and return no rows.
    ByKeys(Vec<K>),

    /// Range scan over primary keys (inclusive).
    KeyRange { start: K, end: K },

    /// Index scan using a prefix of index fields and bound values.
    ///
    /// Contract guarantees:
    /// - `values.len() <= index.fields().len()`
    /// - All values correspond to strict coercions
    IndexPrefix {
        index: SemanticIndexAccessContract,
        values: Vec<Value>,
    },

    /// Index multi-lookup over one leading index field and multiple literal values.
    ///
    /// Contract guarantees:
    /// - `values` are canonicalized as a set (sorted, deduplicated)
    /// - each value targets the leading index slot (`prefix_len == 1`)
    /// - execution semantics are equivalent to a union of one-field index-prefix lookups
    IndexMultiLookup {
        index: SemanticIndexAccessContract,
        values: Vec<Value>,
    },

    /// Index scan using an equality prefix plus one bounded range component.
    ///
    /// This variant is dedicated to secondary range traversal and wraps
    /// semantic range metadata.
    IndexRange { spec: SemanticIndexRangeSpec },

    /// Full entity scan with no index assistance.
    FullScan,
}

impl<K> AccessPath<K> {
    /// Return the coarse semantic discriminator for this path.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> AccessPathKind {
        match self {
            Self::ByKey(_) => AccessPathKind::ByKey,
            Self::ByKeys(_) => AccessPathKind::ByKeys,
            Self::KeyRange { .. } => AccessPathKind::KeyRange,
            Self::IndexPrefix { .. } => AccessPathKind::IndexPrefix,
            Self::IndexMultiLookup { .. } => AccessPathKind::IndexMultiLookup,
            Self::IndexRange { .. } => AccessPathKind::IndexRange,
            Self::FullScan => AccessPathKind::FullScan,
        }
    }

    /// Construct one semantic index-range path from semantic bounds.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn index_range(
        index: IndexModel,
        prefix_values: Vec<Value>,
        lower: Bound<Value>,
        upper: Bound<Value>,
    ) -> Self {
        Self::IndexRange {
            spec: SemanticIndexRangeSpec::from_prefix_and_bounds(
                index,
                prefix_values,
                lower,
                upper,
            ),
        }
    }

    /// Return true when this path is a full scan.
    #[must_use]
    pub(crate) const fn is_full_scan(&self) -> bool {
        matches!(self, Self::FullScan)
    }

    /// Return true when this path is a direct primary-key lookup.
    #[must_use]
    pub(crate) const fn is_by_key(&self) -> bool {
        matches!(self, Self::ByKey(_))
    }

    /// Return true when this path is an index multi-lookup.
    #[must_use]
    pub(crate) const fn is_index_multi_lookup(&self) -> bool {
        matches!(self, Self::IndexMultiLookup { .. })
    }

    /// Borrow the primary key payload when this path is `ByKey`.
    #[must_use]
    pub(crate) const fn as_by_key(&self) -> Option<&K> {
        match self {
            Self::ByKey(key) => Some(key),
            Self::ByKeys(_)
            | Self::KeyRange { .. }
            | Self::IndexPrefix { .. }
            | Self::IndexMultiLookup { .. }
            | Self::IndexRange { .. }
            | Self::FullScan => None,
        }
    }

    /// Borrow the primary-key set when this path is `ByKeys`.
    #[must_use]
    pub(crate) const fn as_by_keys(&self) -> Option<&[K]> {
        match self {
            Self::ByKeys(keys) => Some(keys.as_slice()),
            Self::ByKey(_)
            | Self::KeyRange { .. }
            | Self::IndexPrefix { .. }
            | Self::IndexMultiLookup { .. }
            | Self::IndexRange { .. }
            | Self::FullScan => None,
        }
    }

    /// Borrow reduced index-prefix details when this path is `IndexPrefix`.
    #[must_use]
    pub(in crate::db) fn as_index_prefix_contract(
        &self,
    ) -> Option<(SemanticIndexAccessContract, &[Value])> {
        match self {
            Self::IndexPrefix { index, values } => Some((index.clone(), values.as_slice())),
            _ => None,
        }
    }

    /// Borrow reduced index multi-lookup details when this path is `IndexMultiLookup`.
    #[must_use]
    pub(in crate::db) fn as_index_multi_lookup_contract(
        &self,
    ) -> Option<(SemanticIndexAccessContract, &[Value])> {
        match self {
            Self::IndexMultiLookup { index, values } => Some((index.clone(), values.as_slice())),
            _ => None,
        }
    }

    /// Borrow index-range details when this path is `IndexRange`.
    #[must_use]
    pub(crate) const fn as_index_range(&self) -> Option<&SemanticIndexRangeSpec> {
        match self {
            Self::IndexRange { spec } => Some(spec),
            _ => None,
        }
    }

    /// Borrow the reduced selected secondary-index contract when this path uses one.
    #[must_use]
    pub(in crate::db) fn selected_index_contract(&self) -> Option<SemanticIndexAccessContract> {
        match self {
            Self::IndexPrefix { index, .. } | Self::IndexMultiLookup { index, .. } => {
                Some(index.clone())
            }
            Self::IndexRange { spec } => Some(spec.index()),
            Self::ByKey(_) | Self::ByKeys(_) | Self::KeyRange { .. } | Self::FullScan => None,
        }
    }

    /// Borrow the primary-key range endpoints when this path is `KeyRange`.
    #[must_use]
    pub(crate) const fn as_key_range(&self) -> Option<(&K, &K)> {
        match self {
            Self::KeyRange { start, end } => Some((start, end)),
            Self::ByKey(_)
            | Self::ByKeys(_)
            | Self::IndexPrefix { .. }
            | Self::IndexMultiLookup { .. }
            | Self::IndexRange { .. }
            | Self::FullScan => None,
        }
    }

    /// Return whether this path reads authoritative primary-store traversal
    /// keys directly from row storage.
    #[must_use]
    pub(crate) const fn is_primary_store_authoritative_scan(&self) -> bool {
        matches!(self, Self::KeyRange { .. } | Self::FullScan)
    }

    /// Return whether this path is one exact primary-key lookup shape.
    #[must_use]
    pub(crate) const fn is_primary_key_lookup(&self) -> bool {
        matches!(self, Self::ByKey(_) | Self::ByKeys(_))
    }

    /// Map the key payload of this access path while preserving structural shape.
    pub(crate) fn map_keys<T, E, F>(self, mut map_key: F) -> Result<AccessPath<T>, E>
    where
        F: FnMut(K) -> Result<T, E>,
    {
        match self {
            Self::ByKey(key) => Ok(AccessPath::ByKey(map_key(key)?)),
            Self::ByKeys(keys) => {
                let mut mapped = Vec::with_capacity(keys.len());
                for key in keys {
                    mapped.push(map_key(key)?);
                }

                Ok(AccessPath::ByKeys(mapped))
            }
            Self::KeyRange { start, end } => Ok(AccessPath::KeyRange {
                start: map_key(start)?,
                end: map_key(end)?,
            }),
            Self::IndexPrefix { index, values } => Ok(AccessPath::IndexPrefix { index, values }),
            Self::IndexMultiLookup { index, values } => {
                Ok(AccessPath::IndexMultiLookup { index, values })
            }
            Self::IndexRange { spec } => Ok(AccessPath::IndexRange { spec }),
            Self::FullScan => Ok(AccessPath::FullScan),
        }
    }
}
