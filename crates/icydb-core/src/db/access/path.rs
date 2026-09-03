//! Module: access::path
//! Responsibility: access-path contract types shared by planning/lowering/runtime.
//! Does not own: path validation or canonicalization policy.
//! Boundary: used by access-plan construction and executor interpretation.

use crate::{
    db::{
        Predicate,
        index::SemanticIndexExpression,
        predicate::normalized_accepted_index_predicate,
        schema::{SchemaExpressionIndexInfo, SchemaExpressionIndexKeyItemInfo},
    },
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
    IndexBranchSet,
    IndexRange,
    FullScan,
}

/// Conservative candidate cap for branch-aware composite prefix access routes.
pub(in crate::db) const MAX_INDEX_BRANCH_SET_VALUES: usize = 16;

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
    pub(in crate::db::access) physical_generation: u64,
    pub(in crate::db::access) name: String,
    pub(in crate::db::access) store_path: String,
    pub(in crate::db::access) key_items: Vec<SemanticIndexKeyItem>,
    pub(in crate::db::access) unique: bool,
    pub(in crate::db::access) predicate_semantics: Option<Predicate>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum SemanticIndexKeyItemRef<'a> {
    Field(&'a str),
    AcceptedExpression(&'a SemanticIndexExpression),
}

impl<'a> SemanticIndexKeyItemRef<'a> {
    #[must_use]
    pub(crate) const fn field(self) -> &'a str {
        match self {
            Self::Field(field) => field,
            Self::AcceptedExpression(expression) => expression.field(),
        }
    }

    #[must_use]
    pub(crate) fn canonical_text(self) -> String {
        match self {
            Self::Field(field) => field.to_string(),
            Self::AcceptedExpression(expression) => expression.canonical_order_text(),
        }
    }

    #[must_use]
    pub(crate) const fn is_expression(self) -> bool {
        matches!(self, Self::AcceptedExpression(_))
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

impl PartialEq for SemanticIndexAccessContract {
    fn eq(&self, other: &Self) -> bool {
        self.inner.ordinal == other.inner.ordinal
            && self.inner.physical_generation == other.inner.physical_generation
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
                physical_generation: accepted.physical_generation(),
                name: accepted.name().to_string(),
                store_path: accepted.store().to_string(),
                key_items: accepted
                    .fields()
                    .iter()
                    .map(|field| {
                        SemanticIndexKeyItem::Field(accepted_field_path_term(
                            field.field_name(),
                            field.path(),
                        ))
                    })
                    .collect(),
                unique: accepted.unique(),
                predicate_semantics: normalized_accepted_index_predicate(accepted.predicate_sql()),
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
                physical_generation: accepted.physical_generation(),
                name: accepted.name().to_string(),
                store_path: accepted.store().to_string(),
                key_items: accepted
                    .key_items()
                    .iter()
                    .map(accepted_expression_key_item)
                    .collect(),
                unique: accepted.unique(),
                predicate_semantics: normalized_accepted_index_predicate(accepted.predicate_sql()),
            }),
        }
    }

    #[must_use]
    pub(in crate::db) fn ordinal(&self) -> u16 {
        self.inner.ordinal
    }

    /// Return the isolated physical key generation.
    #[must_use]
    pub(in crate::db) fn physical_generation(&self) -> u64 {
        self.inner.physical_generation
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
    pub(in crate::db) fn key_items(&self) -> &[SemanticIndexKeyItem] {
        self.inner.key_items.as_slice()
    }

    #[must_use]
    pub(in crate::db) fn key_arity(&self) -> usize {
        self.inner.key_items.len()
    }

    #[must_use]
    pub(in crate::db) fn key_item_at(&self, slot: usize) -> Option<SemanticIndexKeyItemRef<'_>> {
        self.inner
            .key_items
            .get(slot)
            .map(SemanticIndexKeyItem::as_ref)
    }

    #[must_use]
    pub(in crate::db) fn key_field_at(&self, slot: usize) -> Option<&str> {
        match self.key_item_at(slot)? {
            SemanticIndexKeyItemRef::Field(field) => Some(field),
            SemanticIndexKeyItemRef::AcceptedExpression(_) => None,
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
        self.inner
            .key_items
            .iter()
            .any(|item| matches!(item, SemanticIndexKeyItem::Expression(_)))
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
            SemanticIndexKeyItem::Expression(SemanticIndexExpression::new(
                expression.op(),
                accepted_field_path_term(
                    expression.source().field_name(),
                    expression.source().path(),
                ),
            ))
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
/// IndexBranchSetSpec
///
/// Branch-aware composite-prefix access request for one secondary index.
/// Stores fixed equality prefix values plus the exact value set for the next
/// prefix slot, and owns the derived slot/prefix helpers so callers do not
/// reconstruct the branch proof from loose vector lengths.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexBranchSetSpec {
    index: SemanticIndexAccessContract,
    fixed_values: Vec<Value>,
    branch_values: Vec<Value>,
}

impl IndexBranchSetSpec {
    /// Construct a branch-set request from one reduced access contract after
    /// the planner has proven a shared ascending primary-key suffix.
    #[must_use]
    pub(crate) const fn from_primary_key_asc_contract(
        index: SemanticIndexAccessContract,
        fixed_values: Vec<Value>,
        branch_values: Vec<Value>,
    ) -> Self {
        Self {
            index,
            fixed_values,
            branch_values,
        }
    }

    /// Borrow the selected index contract.
    #[must_use]
    pub(in crate::db) const fn index_ref(&self) -> &SemanticIndexAccessContract {
        &self.index
    }

    /// Clone the selected index contract for APIs that own access contracts.
    #[must_use]
    pub(in crate::db) fn index(&self) -> SemanticIndexAccessContract {
        self.index.clone()
    }

    /// Borrow equality-bound leading prefix values.
    #[must_use]
    pub(in crate::db) const fn fixed_values(&self) -> &[Value] {
        self.fixed_values.as_slice()
    }

    /// Borrow the exact branch value set for the next prefix slot.
    #[must_use]
    pub(in crate::db) const fn branch_values(&self) -> &[Value] {
        self.branch_values.as_slice()
    }

    /// Return the branch slot in the selected index.
    #[must_use]
    pub(in crate::db) const fn branch_slot(&self) -> usize {
        self.fixed_values.len()
    }

    /// Return the consumed prefix length after including the branch slot.
    #[must_use]
    pub(in crate::db) const fn branch_prefix_len(&self) -> usize {
        self.fixed_values.len().saturating_add(1)
    }

    /// Return the number of exact branch values.
    #[must_use]
    pub(in crate::db) const fn branch_count(&self) -> usize {
        self.branch_values.len()
    }

    /// Return whether this branch set has no branches.
    #[must_use]
    pub(in crate::db) const fn is_empty(&self) -> bool {
        self.branch_values.is_empty()
    }

    /// Borrow the branch index key item, if the selected index has one.
    #[must_use]
    pub(in crate::db) fn branch_key_item(&self) -> Option<SemanticIndexKeyItemRef<'_>> {
        self.index.key_item_at(self.branch_slot())
    }

    /// Build the concrete prefix values for one branch scan.
    #[must_use]
    pub(in crate::db) fn branch_prefix_values(&self, branch_value: &Value) -> Vec<Value> {
        let mut values = Vec::with_capacity(self.branch_prefix_len());
        values.extend_from_slice(self.fixed_values());
        values.push(branch_value.clone());
        values
    }

    /// Consume the spec into its raw storage parts for canonicalization.
    #[must_use]
    pub(crate) fn into_parts(self) -> (SemanticIndexAccessContract, Vec<Value>, Vec<Value>) {
        (self.index, self.fixed_values, self.branch_values)
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

    /// Branch-aware composite prefix scan over fixed leading values and a
    /// small exact set in the next index slot.
    ///
    /// Contract guarantees:
    /// - `fixed_values` are equality-bound leading prefix slots
    /// - `branch_values` are canonicalized as a small set (sorted, deduplicated)
    /// - each lowered branch scans `fixed_values + branch_value` as one prefix
    /// - the planner only selects this path when the suffix order is preserved
    IndexBranchSet { spec: IndexBranchSetSpec },

    /// Index scan using an equality prefix plus one bounded range component.
    ///
    /// This variant is dedicated to secondary range traversal and wraps
    /// semantic range metadata.
    IndexRange { spec: SemanticIndexRangeSpec },

    /// Full entity scan with no index assistance.
    FullScan,
}

impl<K> AccessPath<K> {
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
            | Self::IndexBranchSet { .. }
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
            | Self::IndexBranchSet { .. }
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

    /// Borrow branch-aware composite prefix spec when this path is
    /// `IndexBranchSet`.
    #[must_use]
    pub(in crate::db) const fn as_index_branch_set_spec(&self) -> Option<&IndexBranchSetSpec> {
        match self {
            Self::IndexBranchSet { spec } => Some(spec),
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
            Self::IndexBranchSet { spec } => Some(spec.index()),
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
            | Self::IndexBranchSet { .. }
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
            Self::IndexBranchSet { spec } => Ok(AccessPath::IndexBranchSet { spec }),
            Self::IndexRange { spec } => Ok(AccessPath::IndexRange { spec }),
            Self::FullScan => Ok(AccessPath::FullScan),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SemanticIndexAccessContract, SemanticIndexAccessContractInner, SemanticIndexKeyItem,
        SemanticIndexKeyItemRef,
    };
    use crate::db::{index::SemanticIndexExpression, schema::PersistedIndexExpressionOp};
    use std::sync::Arc;

    #[test]
    fn semantic_index_key_items_preserve_one_ordered_field_expression_contract() {
        let contract = SemanticIndexAccessContract {
            inner: Arc::new(SemanticIndexAccessContractInner {
                ordinal: 4,
                physical_generation: 7,
                name: "by_tenant_lower_email".to_string(),
                store_path: "ByTenantLowerEmail".to_string(),
                key_items: vec![
                    SemanticIndexKeyItem::Field("tenant".to_string()),
                    SemanticIndexKeyItem::Expression(SemanticIndexExpression::new(
                        PersistedIndexExpressionOp::Lower,
                        "email".to_string(),
                    )),
                ],
                unique: false,
                predicate_semantics: None,
            }),
        };

        assert_eq!(contract.key_arity(), 2);
        assert_eq!(
            contract.key_item_at(0),
            Some(SemanticIndexKeyItemRef::Field("tenant"))
        );
        let Some(SemanticIndexKeyItemRef::AcceptedExpression(expression)) = contract.key_item_at(1)
        else {
            panic!("second semantic key item should remain an expression");
        };
        assert_eq!(expression.canonical_order_text(), "LOWER(email)");
        assert!(contract.has_expression_key_items());
    }
}
