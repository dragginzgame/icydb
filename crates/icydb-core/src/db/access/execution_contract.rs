//! Module: db::access::execution_contract
//! Responsibility: shared normalized access contracts consumed by query/cursor/executor.
//! Does not own: logical access-path selection policy.
//! Boundary: planner lowers `AccessPlan`/`AccessPath` into these execution mechanics.

use crate::{
    db::{
        access::{
            lowering::lower_executable_access_plan,
            plan::{
                AccessPlan, PushdownApplicability, SecondaryOrderPushdownEligibility,
                SecondaryOrderPushdownRejection,
            },
        },
        direction::Direction,
    },
    model::{entity::EntityModel, index::IndexModel},
    value::Value,
};
use std::{fmt, ops::Bound};

// Audit Summary:
// - `path: &AccessPath<K>` was previously used only by stream physical lowering.
// - `index_prefix_details`, `index_range_details`, and `index_fields_for_slot_map` duplicated
//   data already available in `ExecutionBounds`.
// - Behavioral `AccessPath` matching in executor runtime has been removed in favor of
//   `ExecutableAccessPath` payload + mechanical execution fields.

///
/// ExecutionMode
///
/// Coarse execution mode used by executor routing.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutionMode {
    FullScan,
    IndexRange,
    OrderedIndexScan,
    Intersect,
    Composite,
}

///
/// ExecutionOrdering
///
/// Ordering contract required by executor traversal mechanics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutionOrdering {
    Natural,
    ByIndex(Direction),
}

///
/// ExecutionDistinctMode
///
/// Distinct handling mode required by execution mechanics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutionDistinctMode {
    None,
    PreOrdered,
    RequiresMaterialization,
}

///
/// ExecutionBounds
///
/// Minimal bound shape required by executor path mechanics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutionBounds {
    Unbounded,
    PrimaryKeyRange,
    IndexPrefix {
        index: IndexModel,
        prefix_len: usize,
    },
    IndexRange {
        index: IndexModel,
        prefix_len: usize,
    },
}

///
/// ExecutionPathKind
///
/// Canonical path discriminant used by executor runtime checks.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutionPathKind {
    ByKey,
    ByKeys,
    KeyRange,
    IndexPrefix,
    IndexMultiLookup,
    IndexRange,
    FullScan,
}

///
/// ExecutionPathPayload
///
/// Variant payload needed for mechanical access execution only.
/// This contract intentionally excludes planner semantics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutionPathPayload<'a, K> {
    ByKey(&'a K),
    ByKeys(&'a [K]),
    KeyRange {
        start: &'a K,
        end: &'a K,
    },
    IndexPrefix,
    IndexMultiLookup {
        value_count: usize,
    },
    IndexRange {
        prefix_values: &'a [Value],
        lower: &'a Bound<Value>,
        upper: &'a Bound<Value>,
    },
    FullScan,
}

///
/// AccessRouteClass
///
/// Access-owned routing capability snapshot derived from one lowered executable
/// access plan. Router/executor policy layers consume this contract instead of
/// repeatedly branching over raw access tree structure.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[expect(clippy::struct_excessive_bools)]
pub(in crate::db) struct AccessRouteClass {
    single_path: bool,
    composite: bool,
    range_scan: bool,
    prefix_scan: bool,
    ordered: bool,
    reverse_supported: bool,
    single_path_supports_pk_stream_access: bool,
    single_path_supports_count_pushdown_shape: bool,
    single_path_index_prefix_details: Option<(IndexModel, usize)>,
    single_path_index_range_details: Option<(IndexModel, usize)>,
    first_index_range_details: Option<(IndexModel, usize)>,
}

impl AccessRouteClass {
    #[must_use]
    pub(in crate::db) const fn single_path(self) -> bool {
        self.single_path
    }

    #[must_use]
    pub(in crate::db) const fn composite(self) -> bool {
        self.composite
    }

    #[must_use]
    pub(in crate::db) const fn range_scan(self) -> bool {
        self.range_scan
    }

    #[must_use]
    pub(in crate::db) const fn prefix_scan(self) -> bool {
        self.prefix_scan
    }

    #[must_use]
    pub(in crate::db) const fn ordered(self) -> bool {
        self.ordered
    }

    #[must_use]
    pub(in crate::db) const fn reverse_supported(self) -> bool {
        self.reverse_supported
    }

    #[must_use]
    pub(in crate::db) const fn single_path_supports_pk_stream_access(self) -> bool {
        self.single_path_supports_pk_stream_access
    }

    #[must_use]
    pub(in crate::db) const fn single_path_supports_count_pushdown_shape(self) -> bool {
        self.single_path_supports_count_pushdown_shape
    }

    #[must_use]
    pub(in crate::db) const fn single_path_index_prefix_details(
        self,
    ) -> Option<(IndexModel, usize)> {
        self.single_path_index_prefix_details
    }

    #[must_use]
    pub(in crate::db) const fn single_path_index_range_details(
        self,
    ) -> Option<(IndexModel, usize)> {
        self.single_path_index_range_details
    }

    #[must_use]
    pub(in crate::db) const fn first_index_range_details(self) -> Option<(IndexModel, usize)> {
        self.first_index_range_details
    }

    /// Derive secondary ORDER BY pushdown applicability from one access class
    /// and normalized ORDER BY fields.
    #[must_use]
    pub(in crate::db) fn secondary_order_pushdown_applicability(
        self,
        model: &EntityModel,
        order_fields: &[(&str, Direction)],
    ) -> PushdownApplicability {
        if !self.single_path() {
            if let Some((index, prefix_len)) = self.first_index_range_details() {
                return PushdownApplicability::Applicable(
                    SecondaryOrderPushdownEligibility::Rejected(
                        SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                            index: index.name,
                            prefix_len,
                        },
                    ),
                );
            }

            return PushdownApplicability::NotApplicable;
        }

        if self.prefix_scan() {
            let Some((index, prefix_len)) = self.single_path_index_prefix_details() else {
                debug_assert!(
                    false,
                    "access route class invariant: prefix-scan single-path routes must expose prefix details",
                );
                return PushdownApplicability::NotApplicable;
            };
            if prefix_len > index.fields.len() {
                return PushdownApplicability::Applicable(
                    SecondaryOrderPushdownEligibility::Rejected(
                        SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                            prefix_len,
                            index_field_len: index.fields.len(),
                        },
                    ),
                );
            }

            return PushdownApplicability::Applicable(match_secondary_order_pushdown_core(
                model,
                order_fields,
                index.name,
                index.fields,
                prefix_len,
            ));
        }

        if self.range_scan() {
            let Some((index, prefix_len)) = self.single_path_index_range_details() else {
                debug_assert!(
                    false,
                    "access route class invariant: range-scan single-path routes must expose range details",
                );
                return PushdownApplicability::NotApplicable;
            };
            if prefix_len > index.fields.len() {
                return PushdownApplicability::Applicable(
                    SecondaryOrderPushdownEligibility::Rejected(
                        SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                            prefix_len,
                            index_field_len: index.fields.len(),
                        },
                    ),
                );
            }

            let eligibility = match_secondary_order_pushdown_core(
                model,
                order_fields,
                index.name,
                index.fields,
                prefix_len,
            );
            return match eligibility {
                SecondaryOrderPushdownEligibility::Eligible { .. } => {
                    PushdownApplicability::Applicable(eligibility)
                }
                SecondaryOrderPushdownEligibility::Rejected(_) => {
                    PushdownApplicability::Applicable(SecondaryOrderPushdownEligibility::Rejected(
                        SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                            index: index.name,
                            prefix_len,
                        },
                    ))
                }
            };
        }

        PushdownApplicability::NotApplicable
    }

    /// Return true when this access class supports index-range limit pushdown
    /// for the supplied ORDER BY field sequence.
    #[must_use]
    pub(in crate::db) fn index_range_limit_pushdown_shape_eligible_for_order<D>(
        self,
        order_fields: Option<&[(String, D)]>,
        primary_key_name: &'static str,
    ) -> bool
    where
        D: Copy + Eq,
    {
        if !self.single_path() {
            return false;
        }
        let Some((index, prefix_len)) = self.single_path_index_range_details() else {
            return false;
        };
        let index_fields = index.fields;

        let Some(order_fields) = order_fields else {
            return true;
        };
        if order_fields.is_empty() {
            return true;
        }
        let Some((_, expected_direction)) = order_fields.last() else {
            return false;
        };
        if order_fields
            .iter()
            .any(|(_, direction)| *direction != *expected_direction)
        {
            return false;
        }

        let mut expected = Vec::with_capacity(index_fields.len().saturating_sub(prefix_len) + 1);
        expected.extend(index_fields.iter().skip(prefix_len).copied());
        expected.push(primary_key_name);
        if order_fields.len() != expected.len() {
            return false;
        }
        order_fields
            .iter()
            .map(|(field, _)| field.as_str())
            .eq(expected)
    }
}

///
/// AccessStrategy
///
/// Pre-resolved access execution contract produced once from planner-selected
/// access shape and consumed by runtime layers. This keeps path lowering and
/// route-class derivation under one access-owned authority object.
///

#[derive(Clone, Eq, PartialEq)]
pub(in crate::db) struct AccessStrategy<'a, K> {
    executable: ExecutableAccessPlan<'a, K>,
    class: AccessRouteClass,
}

impl<'a, K> AccessStrategy<'a, K> {
    /// Resolve one access strategy from one planner-selected access plan.
    #[must_use]
    pub(in crate::db) fn from_plan(plan: &'a AccessPlan<K>) -> Self {
        let executable = lower_executable_access_plan(plan);
        Self::from_executable(executable)
    }

    /// Resolve one access strategy from one already lowered executable access plan.
    #[must_use]
    pub(in crate::db) fn from_executable(executable: ExecutableAccessPlan<'a, K>) -> Self {
        let class = executable.class();
        Self { executable, class }
    }

    /// Borrow the lowered executable access contract.
    #[must_use]
    pub(in crate::db) const fn executable(&self) -> &ExecutableAccessPlan<'a, K> {
        &self.executable
    }

    /// Consume this strategy and return the lowered executable access contract.
    #[must_use]
    pub(in crate::db) fn into_executable(self) -> ExecutableAccessPlan<'a, K> {
        self.executable
    }

    /// Return access-owned route class capability snapshot.
    #[must_use]
    pub(in crate::db) const fn class(&self) -> AccessRouteClass {
        self.class
    }

    /// Borrow direct path payload when this strategy is single-path.
    #[must_use]
    pub(in crate::db) const fn as_path(&self) -> Option<&ExecutableAccessPath<'a, K>> {
        self.executable.as_path()
    }

    /// Derive a load-window early-stop scan-budget hint for this access shape.
    ///
    /// This helper keeps access-shape mechanics (`ordered` stream support)
    /// centralized under `AccessStrategy`, while callers provide route-owned
    /// continuation and streaming-safety policy gates.
    #[must_use]
    pub(in crate::db) const fn load_window_early_stop_hint(
        &self,
        continuation_applied: bool,
        streaming_access_shape_safe: bool,
        fetch_count: Option<usize>,
    ) -> Option<usize> {
        if continuation_applied {
            return None;
        }
        if !streaming_access_shape_safe {
            return None;
        }
        if !self.class().ordered() {
            return None;
        }

        fetch_count
    }
}

impl<K> AccessStrategy<'_, K>
where
    K: fmt::Debug,
{
    /// Return one concise debug summary of the resolved access strategy shape.
    #[must_use]
    pub(in crate::db) fn debug_summary(&self) -> String {
        summarize_executable_access_plan(&self.executable)
    }
}

impl<K> fmt::Debug for AccessStrategy<'_, K>
where
    K: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AccessStrategy")
            .field("summary", &self.debug_summary())
            .field("class", &self.class)
            .finish()
    }
}

fn summarize_executable_access_plan<K>(plan: &ExecutableAccessPlan<'_, K>) -> String
where
    K: fmt::Debug,
{
    match plan.node() {
        ExecutableAccessNode::Path(path) => summarize_executable_access_path(path),
        ExecutableAccessNode::Union(children) => {
            format!("Union({})", summarize_composite_children(children))
        }
        ExecutableAccessNode::Intersection(children) => {
            format!("Intersection({})", summarize_composite_children(children))
        }
    }
}

fn summarize_composite_children<K>(children: &[ExecutableAccessPlan<'_, K>]) -> String
where
    K: fmt::Debug,
{
    let preview_len = children.len().min(3);
    let mut preview = Vec::with_capacity(preview_len);
    for child in children.iter().take(preview_len) {
        preview.push(summarize_executable_access_plan(child));
    }

    if children.len() > preview_len {
        preview.push(format!("... +{} more", children.len() - preview_len));
    }

    preview.join(", ")
}

fn summarize_executable_access_path<K>(path: &ExecutableAccessPath<'_, K>) -> String
where
    K: fmt::Debug,
{
    match path.payload() {
        ExecutionPathPayload::ByKey(key) => format!("IndexLookup(pk={key:?})"),
        ExecutionPathPayload::ByKeys(keys) => format!("IndexLookupMany(pk_count={})", keys.len()),
        ExecutionPathPayload::KeyRange { start, end } => {
            format!("PrimaryKeyRange([{start:?}, {end:?}))")
        }
        ExecutionPathPayload::IndexPrefix => {
            if let Some((index, prefix_len)) = path.index_prefix_details() {
                if prefix_len == 0 {
                    format!("IndexPrefix({})", index.name)
                } else {
                    format!("IndexPrefix({} prefix_len={prefix_len})", index.name)
                }
            } else {
                "IndexPrefix".to_string()
            }
        }
        ExecutionPathPayload::IndexMultiLookup { value_count } => {
            if let Some((index, _)) = path.index_prefix_details() {
                format!("IndexMultiLookup({} values={value_count})", index.name)
            } else {
                format!("IndexMultiLookup(values={value_count})")
            }
        }
        ExecutionPathPayload::IndexRange {
            prefix_values,
            lower,
            upper,
        } => {
            if let Some((index, prefix_len)) = path.index_range_details() {
                summarize_index_range_with_model(index, prefix_len, prefix_values, lower, upper)
            } else {
                format!(
                    "IndexRange(prefix={prefix_values:?} {})",
                    summarize_interval(lower, upper),
                )
            }
        }
        ExecutionPathPayload::FullScan => "FullScan".to_string(),
    }
}

fn summarize_index_range_with_model(
    index: IndexModel,
    prefix_len: usize,
    prefix_values: &[Value],
    lower: &Bound<Value>,
    upper: &Bound<Value>,
) -> String {
    let prefix = summarize_index_prefix_terms(index.fields, prefix_values);
    let interval = summarize_interval(lower, upper);

    if let Some(range_field) = index.fields.get(prefix_len) {
        if prefix.is_empty() {
            format!("IndexRange({range_field} {interval})")
        } else {
            format!("IndexRange({prefix}; {range_field} {interval})")
        }
    } else if prefix.is_empty() {
        format!("IndexRange({interval})")
    } else {
        format!("IndexRange({prefix}; {interval})")
    }
}

fn summarize_index_prefix_terms(index_fields: &[&'static str], values: &[Value]) -> String {
    index_fields
        .iter()
        .copied()
        .zip(values.iter())
        .map(|(field, value)| format!("{field}={}", summarize_value(value)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn summarize_interval(lower: &Bound<Value>, upper: &Bound<Value>) -> String {
    let (lower_bracket, lower_value) = match lower {
        Bound::Included(value) => ("[", summarize_value(value)),
        Bound::Excluded(value) => ("(", summarize_value(value)),
        Bound::Unbounded => ("(", "-inf".to_string()),
    };
    let (upper_value, upper_bracket) = match upper {
        Bound::Included(value) => (summarize_value(value), "]"),
        Bound::Excluded(value) => (summarize_value(value), ")"),
        Bound::Unbounded => ("+inf".to_string(), ")"),
    };

    format!("{lower_bracket}{lower_value}, {upper_value}{upper_bracket}")
}

fn summarize_value(value: &Value) -> String {
    match value {
        Value::Text(text) => format!("{text:?}"),
        _ => format!("{value:?}"),
    }
}

// Core matcher for secondary ORDER BY pushdown eligibility.
fn match_secondary_order_pushdown_core(
    model: &EntityModel,
    order_fields: &[(&str, Direction)],
    index_name: &'static str,
    index_fields: &[&'static str],
    prefix_len: usize,
) -> SecondaryOrderPushdownEligibility {
    let Some((last_field, last_direction)) = order_fields.last() else {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy,
        );
    };
    if *last_field != model.primary_key.name {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::MissingPrimaryKeyTieBreak {
                field: model.primary_key.name.to_string(),
            },
        );
    }

    let expected_direction = *last_direction;
    for (field, direction) in order_fields {
        if *direction != expected_direction {
            return SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::MixedDirectionNotEligible {
                    field: (*field).to_string(),
                },
            );
        }
    }

    let actual_non_pk_len = order_fields.len().saturating_sub(1);
    let matches_expected_suffix = actual_non_pk_len
        == index_fields.len().saturating_sub(prefix_len)
        && order_fields
            .iter()
            .take(actual_non_pk_len)
            .map(|(field, _)| *field)
            .zip(index_fields.iter().skip(prefix_len).copied())
            .all(|(actual, expected)| actual == expected);
    let matches_expected_full = actual_non_pk_len == index_fields.len()
        && order_fields
            .iter()
            .take(actual_non_pk_len)
            .map(|(field, _)| *field)
            .zip(index_fields.iter().copied())
            .all(|(actual, expected)| actual == expected);
    if matches_expected_suffix || matches_expected_full {
        return SecondaryOrderPushdownEligibility::Eligible {
            index: index_name,
            prefix_len,
        };
    }

    SecondaryOrderPushdownEligibility::Rejected(
        SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
            index: index_name,
            prefix_len,
            expected_suffix: index_fields
                .iter()
                .skip(prefix_len)
                .map(|field| (*field).to_string())
                .collect(),
            expected_full: index_fields
                .iter()
                .map(|field| (*field).to_string())
                .collect(),
            actual: order_fields
                .iter()
                .take(actual_non_pk_len)
                .map(|(field, _)| (*field).to_string())
                .collect(),
        },
    )
}

///
/// ExecutableAccessPath
///
/// Normalized execution contract for one concrete access path.
/// Holds compact execution mechanics plus variant payload needed for traversal.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ExecutableAccessPath<'a, K> {
    mode: ExecutionMode,
    ordering: ExecutionOrdering,
    bounds: ExecutionBounds,
    distinct: ExecutionDistinctMode,
    requires_decoded_id: bool,
    payload: ExecutionPathPayload<'a, K>,
}

impl<'a, K> ExecutableAccessPath<'a, K> {
    /// Construct a normalized executable-path contract.
    #[must_use]
    pub(in crate::db) const fn new(
        mode: ExecutionMode,
        ordering: ExecutionOrdering,
        bounds: ExecutionBounds,
        distinct: ExecutionDistinctMode,
        requires_decoded_id: bool,
        payload: ExecutionPathPayload<'a, K>,
    ) -> Self {
        Self {
            mode,
            ordering,
            bounds,
            distinct,
            requires_decoded_id,
            payload,
        }
    }

    /// Borrow the execution payload for this path.
    #[must_use]
    pub(in crate::db) const fn payload(&self) -> &ExecutionPathPayload<'a, K> {
        &self.payload
    }

    /// Return the canonical execution path kind.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> ExecutionPathKind {
        match self.payload {
            ExecutionPathPayload::ByKey(_) => ExecutionPathKind::ByKey,
            ExecutionPathPayload::ByKeys(_) => ExecutionPathKind::ByKeys,
            ExecutionPathPayload::KeyRange { .. } => ExecutionPathKind::KeyRange,
            ExecutionPathPayload::IndexPrefix => ExecutionPathKind::IndexPrefix,
            ExecutionPathPayload::IndexMultiLookup { .. } => ExecutionPathKind::IndexMultiLookup,
            ExecutionPathPayload::IndexRange { .. } => ExecutionPathKind::IndexRange,
            ExecutionPathPayload::FullScan => ExecutionPathKind::FullScan,
        }
    }

    /// Return the coarse execution mode.
    #[must_use]
    pub(in crate::db) const fn mode(&self) -> ExecutionMode {
        self.mode
    }

    /// Return ordering mechanics for this path.
    #[must_use]
    pub(in crate::db) const fn ordering(&self) -> ExecutionOrdering {
        self.ordering
    }

    /// Return bound mechanics for this path.
    #[must_use]
    pub(in crate::db) const fn bounds(&self) -> ExecutionBounds {
        self.bounds
    }

    /// Return distinct mode for this path.
    #[must_use]
    pub(in crate::db) const fn distinct(&self) -> ExecutionDistinctMode {
        self.distinct
    }

    /// Return whether this path requires decoded-id materialization.
    #[must_use]
    pub(in crate::db) const fn requires_decoded_id(&self) -> bool {
        self.requires_decoded_id
    }

    /// Borrow semantic index-range bounds required for cursor envelope validation.
    #[must_use]
    pub(in crate::db) const fn index_range_semantic_bounds(
        &self,
    ) -> Option<(&'a [Value], &'a Bound<Value>, &'a Bound<Value>)> {
        match self.payload {
            ExecutionPathPayload::IndexRange {
                prefix_values,
                lower,
                upper,
            } => Some((prefix_values, lower, upper)),
            ExecutionPathPayload::ByKey(_)
            | ExecutionPathPayload::ByKeys(_)
            | ExecutionPathPayload::KeyRange { .. }
            | ExecutionPathPayload::IndexPrefix
            | ExecutionPathPayload::IndexMultiLookup { .. }
            | ExecutionPathPayload::FullScan => None,
        }
    }

    /// Borrow index-prefix details when this path is index-prefix.
    #[must_use]
    pub(in crate::db) const fn index_prefix_details(&self) -> Option<(IndexModel, usize)> {
        match self.bounds {
            ExecutionBounds::IndexPrefix { index, prefix_len } => Some((index, prefix_len)),
            ExecutionBounds::Unbounded
            | ExecutionBounds::PrimaryKeyRange
            | ExecutionBounds::IndexRange { .. } => None,
        }
    }

    /// Borrow index-range details when this path is index-range.
    #[must_use]
    pub(in crate::db) const fn index_range_details(&self) -> Option<(IndexModel, usize)> {
        match self.bounds {
            ExecutionBounds::IndexRange { index, prefix_len } => Some((index, prefix_len)),
            ExecutionBounds::Unbounded
            | ExecutionBounds::PrimaryKeyRange
            | ExecutionBounds::IndexPrefix { .. } => None,
        }
    }
}

///
/// ExecutableAccessNode
///
/// Recursive normalized execution tree for one access plan.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutableAccessNode<'a, K> {
    Path(ExecutableAccessPath<'a, K>),
    Union(Vec<ExecutableAccessPlan<'a, K>>),
    Intersection(Vec<ExecutableAccessPlan<'a, K>>),
}

///
/// ExecutableAccessPlan
///
/// Normalized execution contract for one access plan.
/// This is executor-consumed and planner-lowered.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ExecutableAccessPlan<'a, K> {
    pub(in crate::db) mode: ExecutionMode,
    pub(in crate::db) ordering: ExecutionOrdering,
    pub(in crate::db) bounds: ExecutionBounds,
    pub(in crate::db) distinct: ExecutionDistinctMode,
    pub(in crate::db) requires_decoded_id: bool,
    node: ExecutableAccessNode<'a, K>,
}

impl<'a, K> ExecutableAccessPlan<'a, K> {
    /// Construct one path-backed executable access plan.
    #[must_use]
    pub(in crate::db) const fn for_path(path: ExecutableAccessPath<'a, K>) -> Self {
        Self {
            mode: path.mode(),
            ordering: path.ordering(),
            bounds: path.bounds(),
            distinct: path.distinct(),
            requires_decoded_id: path.requires_decoded_id(),
            node: ExecutableAccessNode::Path(path),
        }
    }

    /// Construct one union executable access plan.
    #[must_use]
    pub(in crate::db) fn union(children: Vec<Self>) -> Self {
        Self {
            mode: ExecutionMode::Composite,
            ordering: ExecutionOrdering::Natural,
            bounds: ExecutionBounds::Unbounded,
            distinct: ExecutionDistinctMode::RequiresMaterialization,
            requires_decoded_id: children.iter().any(|child| child.requires_decoded_id),
            node: ExecutableAccessNode::Union(children),
        }
    }

    /// Construct one intersection executable access plan.
    #[must_use]
    pub(in crate::db) fn intersection(children: Vec<Self>) -> Self {
        Self {
            mode: ExecutionMode::Intersect,
            ordering: ExecutionOrdering::Natural,
            bounds: ExecutionBounds::Unbounded,
            distinct: ExecutionDistinctMode::RequiresMaterialization,
            requires_decoded_id: children.iter().any(|child| child.requires_decoded_id),
            node: ExecutableAccessNode::Intersection(children),
        }
    }

    /// Borrow the normalized execution tree node.
    #[must_use]
    pub(in crate::db) const fn node(&self) -> &ExecutableAccessNode<'a, K> {
        &self.node
    }

    /// Borrow path execution contract when this plan is one path node.
    #[must_use]
    pub(in crate::db) const fn as_path(&self) -> Option<&ExecutableAccessPath<'a, K>> {
        match &self.node {
            ExecutableAccessNode::Path(path) => Some(path),
            ExecutableAccessNode::Union(_) | ExecutableAccessNode::Intersection(_) => None,
        }
    }

    /// Derive one access-owned route class from this lowered executable plan.
    #[must_use]
    pub(in crate::db) fn class(&self) -> AccessRouteClass {
        // Route-class capability projection is delegated to access/capabilities.
        // This keeps route-shape predicates under one authority surface.
        let capabilities = self.capabilities();
        let single_path = capabilities.single_path();
        let single_path_index_prefix_details = single_path
            .and_then(|path| path.index_prefix_details())
            .map(|details| (details.index(), details.slot_arity()));
        let single_path_index_range_details = single_path
            .and_then(|path| path.index_range_details())
            .map(|details| (details.index(), details.slot_arity()));
        let first_index_range_details = capabilities
            .first_index_range_details()
            .map(|details| (details.index(), details.slot_arity()));

        AccessRouteClass {
            single_path: single_path.is_some(),
            composite: capabilities.is_composite(),
            range_scan: single_path_index_range_details.is_some(),
            prefix_scan: single_path_index_prefix_details.is_some(),
            ordered: true,
            reverse_supported: capabilities.all_paths_support_reverse_traversal(),
            single_path_supports_pk_stream_access: single_path
                .is_some_and(|path| path.supports_pk_stream_access()),
            single_path_supports_count_pushdown_shape: single_path
                .is_some_and(|path| path.supports_count_pushdown_shape()),
            single_path_index_prefix_details,
            single_path_index_range_details,
            first_index_range_details,
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::access::{AccessPlan, AccessStrategy},
        model::index::IndexModel,
        value::Value,
    };

    const INDEX_MULTI_LOOKUP_TEST_FIELDS: [&str; 1] = ["group"];

    #[test]
    fn access_strategy_debug_summary_reports_scalar_path_shape() {
        let plan = AccessPlan::by_key(7u64);
        let strategy = AccessStrategy::from_plan(&plan);

        assert_eq!(
            strategy.debug_summary(),
            "IndexLookup(pk=7)",
            "single-key strategies should render concise path summaries",
        );
    }

    #[test]
    fn access_strategy_debug_summary_reports_composite_shape() {
        let plan = AccessPlan::union(vec![AccessPlan::by_key(1u64), AccessPlan::by_key(2u64)]);
        let strategy = AccessStrategy::from_plan(&plan);
        let summary = strategy.debug_summary();

        assert!(
            summary.starts_with("Union("),
            "composite strategies should render union summary headings",
        );
        assert!(
            summary.contains("IndexLookup(pk=1)") && summary.contains("IndexLookup(pk=2)"),
            "composite strategy summaries should include child path summaries",
        );
        assert!(
            format!("{strategy:?}").contains("summary"),
            "debug output should include the summarized route label",
        );
    }

    #[test]
    fn access_strategy_debug_summary_reports_index_multi_lookup_shape() {
        let index = IndexModel::new(
            "tests::idx_group",
            "tests::store",
            &INDEX_MULTI_LOOKUP_TEST_FIELDS,
            false,
        );
        let plan: AccessPlan<u64> =
            AccessPlan::index_multi_lookup(index, vec![Value::Uint(7), Value::Uint(9)]);
        let strategy = AccessStrategy::from_plan(&plan);

        assert!(
            strategy.debug_summary().contains("IndexMultiLookup"),
            "index multi-lookup strategies should render dedicated path summaries",
        );
    }
}
