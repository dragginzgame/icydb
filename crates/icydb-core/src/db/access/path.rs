//! Module: access::path
//! Responsibility: access-path contract types shared by planning/lowering/runtime.
//! Does not own: path validation or canonicalization policy.
//! Boundary: used by access-plan construction and executor interpretation.

use crate::{
    db::Predicate,
    model::index::{IndexKeyItem, IndexKeyItemsRef, IndexModel},
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

#[derive(Clone, Copy, Debug)]
pub(crate) struct SemanticIndexAccessContract {
    ordinal: u16,
    name: &'static str,
    store_path: &'static str,
    key_items: IndexKeyItemsRef,
    unique: bool,
    predicate_semantics: Option<fn() -> &'static Predicate>,
}

impl PartialEq for SemanticIndexAccessContract {
    fn eq(&self, other: &Self) -> bool {
        self.ordinal == other.ordinal
            && self.name == other.name
            && self.store_path == other.store_path
            && self.key_items == other.key_items
            && self.unique == other.unique
            && match (self.predicate_semantics, other.predicate_semantics) {
                (Some(left), Some(right)) => std::ptr::fn_addr_eq(left, right),
                (None, None) => true,
                (Some(_), None) | (None, Some(_)) => false,
            }
    }
}

impl Eq for SemanticIndexAccessContract {}

impl SemanticIndexAccessContract {
    /// Project one generated index model into the narrow access contract used
    /// past planner candidate selection.
    #[must_use]
    pub(in crate::db) const fn from_index(index: IndexModel) -> Self {
        Self {
            ordinal: index.ordinal(),
            name: index.name(),
            store_path: index.store(),
            key_items: index.key_items(),
            unique: index.is_unique(),
            predicate_semantics: index.predicate_resolver(),
        }
    }

    #[must_use]
    pub(in crate::db) const fn ordinal(self) -> u16 {
        self.ordinal
    }

    #[must_use]
    pub(in crate::db) const fn name(self) -> &'static str {
        self.name
    }

    #[must_use]
    pub(in crate::db) const fn store_path(self) -> &'static str {
        self.store_path
    }

    #[must_use]
    pub(in crate::db) const fn key_items(self) -> IndexKeyItemsRef {
        self.key_items
    }

    #[must_use]
    pub(in crate::db) const fn key_arity(self) -> usize {
        match self.key_items {
            IndexKeyItemsRef::Fields(fields) => fields.len(),
            IndexKeyItemsRef::Items(items) => items.len(),
        }
    }

    #[must_use]
    pub(in crate::db) const fn key_item_at(self, slot: usize) -> Option<IndexKeyItem> {
        match self.key_items {
            IndexKeyItemsRef::Fields(fields) => {
                if slot < fields.len() {
                    Some(IndexKeyItem::Field(fields[slot]))
                } else {
                    None
                }
            }
            IndexKeyItemsRef::Items(items) => {
                if slot < items.len() {
                    Some(items[slot])
                } else {
                    None
                }
            }
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn fields(self) -> Vec<&'static str> {
        match self.key_items {
            IndexKeyItemsRef::Fields(fields) => fields.to_vec(),
            IndexKeyItemsRef::Items(items) => items.iter().map(IndexKeyItem::field).collect(),
        }
    }

    #[must_use]
    pub(in crate::db) const fn is_unique(self) -> bool {
        self.unique
    }

    #[must_use]
    pub(in crate::db) const fn is_filtered(self) -> bool {
        self.predicate_semantics.is_some()
    }

    #[must_use]
    pub(in crate::db) const fn has_expression_key_items(self) -> bool {
        match self.key_items {
            IndexKeyItemsRef::Fields(_) => false,
            IndexKeyItemsRef::Items(items) => {
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
    pub(in crate::db) fn predicate_semantics(self) -> Option<&'static Predicate> {
        self.predicate_semantics.map(|resolver| resolver())
    }
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
            index: SemanticIndexAccessContract::from_index(index),
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
    pub(crate) const fn index(&self) -> SemanticIndexAccessContract {
        self.index
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
    pub(in crate::db) const fn as_index_prefix_contract(
        &self,
    ) -> Option<(SemanticIndexAccessContract, &[Value])> {
        match self {
            Self::IndexPrefix { index, values } => Some((*index, values.as_slice())),
            _ => None,
        }
    }

    /// Borrow reduced index multi-lookup details when this path is `IndexMultiLookup`.
    #[must_use]
    pub(in crate::db) const fn as_index_multi_lookup_contract(
        &self,
    ) -> Option<(SemanticIndexAccessContract, &[Value])> {
        match self {
            Self::IndexMultiLookup { index, values } => Some((*index, values.as_slice())),
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
    pub(in crate::db) const fn selected_index_contract(
        &self,
    ) -> Option<SemanticIndexAccessContract> {
        match self {
            Self::IndexPrefix { index, .. } | Self::IndexMultiLookup { index, .. } => Some(*index),
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
