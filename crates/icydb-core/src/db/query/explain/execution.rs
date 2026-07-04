//! Module: query::explain::execution
//! Responsibility: stable execution-descriptor vocabulary for EXPLAIN.
//! Does not own: logical plan projection or rendering logic.
//! Boundary: execution descriptor types consumed by explain renderers.

use crate::{
    db::query::{
        admission::QueryAdmissionSummary,
        explain::{ExplainAccessPath, ExplainPlan, ExplainPredicate},
        plan::{AggregateKind, ResidualFilterShape},
        trace::TraceReuseEvent,
    },
    value::Value,
};
use std::fmt::{self, Debug};

#[cfg_attr(
    doc,
    doc = "ExplainPropertyMap\n\nStable ordered property map for EXPLAIN metadata.\nKeeps deterministic key order without `BTreeMap`."
)]
#[derive(Clone, Default, Eq, PartialEq)]
pub struct ExplainPropertyMap {
    entries: Vec<(&'static str, Value)>,
}

impl ExplainPropertyMap {
    /// Build an empty EXPLAIN property map.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Insert or replace one stable property.
    pub fn insert(&mut self, key: &'static str, value: Value) -> Option<Value> {
        match self
            .entries
            .binary_search_by_key(&key, |(existing_key, _)| *existing_key)
        {
            Ok(index) => Some(std::mem::replace(&mut self.entries[index].1, value)),
            Err(index) => {
                self.entries.insert(index, (key, value));
                None
            }
        }
    }

    /// Borrow one property value by key.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.entries
            .binary_search_by_key(&key, |(existing_key, _)| *existing_key)
            .ok()
            .map(|index| &self.entries[index].1)
    }

    /// Return whether the property map contains the given key.
    #[must_use]
    #[cfg(test)]
    pub fn contains_key(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    /// Return whether the property map is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterate over all stored properties in deterministic key order.
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, &Value)> {
        self.entries.iter().map(|(key, value)| (*key, value))
    }
}

impl Debug for ExplainPropertyMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map = f.debug_map();
        for (key, value) in self.iter() {
            map.entry(&key, value);
        }
        map.finish()
    }
}

/// Stable EXPLAIN node-property key vocabulary shared by descriptor builders
/// and renderers.
pub(in crate::db) mod property_keys {
    pub(in crate::db) const ACCESS_ALTERNATIVES: &str = "acc_alts";
    pub(in crate::db) const ACCESS_CHOICE: &str = "acc_choice";
    pub(in crate::db) const ACCESS_REASON: &str = "acc_reason";
    pub(in crate::db) const ACCESS_REJECTIONS: &str = "acc_reject";
    pub(in crate::db) const AGGREGATE_CONTRACT: &str = "aggregate_contract";
    pub(in crate::db) const AGGREGATE_PHYSICAL: &str = "aggregate_physical";
    pub(in crate::db) const CONTINUATION_MODE: &str = "cont_mode";
    pub(in crate::db) const COUNT_FOLD: &str = "count_fold";
    pub(in crate::db) const COVERING_FIELDS: &str = "covering_fields";
    pub(in crate::db) const COVERING_KIND: &str = "covering_kind";
    pub(in crate::db) const COVERING_ORDER: &str = "covering_order";
    pub(in crate::db) const COVERING_READ_KIND: &str = "cov_read_kind";
    pub(in crate::db) const COVERING_READ_ROUTE: &str = "cov_read_route";
    pub(in crate::db) const COVERING_SCAN_REASON: &str = "cov_scan_reason";
    pub(in crate::db) const COVERING_SOURCES: &str = "covering_sources";
    pub(in crate::db) const EXISTING_ROW_MODE: &str = "existing_row_mode";
    #[cfg(feature = "sql-explain")]
    pub(in crate::db) const FILTER_EXPR: &str = "filter_expr";
    pub(in crate::db) const FAST_PATH: &str = "fast_path";
    pub(in crate::db) const FAST_REASON: &str = "fast_reason";
    pub(in crate::db) const FAST_REJECTIONS: &str = "fast_reject";
    pub(in crate::db) const FETCH: &str = "fetch";
    pub(in crate::db) const GROUPED_EXECUTION_MODE: &str = "grouped_execution_mode";
    pub(in crate::db) const GROUPED_PLAN_FALLBACK_REASON: &str = "grouped_plan_fallback_reason";
    pub(in crate::db) const GROUPED_ROUTE_ELIGIBLE: &str = "grouped_route_eligible";
    pub(in crate::db) const GROUPED_ROUTE_OUTCOME: &str = "grouped_route_outcome";
    pub(in crate::db) const GROUPED_ROUTE_REJECTION_REASON: &str = "grouped_route_rejection_reason";
    #[cfg(feature = "sql-explain")]
    pub(in crate::db) const AGGREGATE_DIRECT_COUNT_METADATA_ELIGIBLE: &str =
        "aggregate_direct_count_metadata_eligible";
    #[cfg(feature = "sql-explain")]
    pub(in crate::db) const AGGREGATE_DIRECT_COUNT_PREFIXES: &str =
        "aggregate_direct_count_prefixes";
    pub(in crate::db) const INDEX: &str = "index";
    pub(in crate::db) const OFFSET: &str = "offset";
    pub(in crate::db) const ORDER_BY_INDEX: &str = "order_by_idx";
    pub(in crate::db) const ORDER_BY_INDEX_HINT: &str = "order_by_idx_hint";
    pub(in crate::db) const ORDER_ROUTE_MODE: &str = "ord_route_mode";
    pub(in crate::db) const ORDER_ROUTE_REASON: &str = "ord_route_reason";
    pub(in crate::db) const PREDICATE_INDEX_CAPABILITY: &str = "pred_idx_cap";
    pub(in crate::db) const PREFIX_LEN: &str = "prefix_len";
    pub(in crate::db) const PREFIX_VALUES: &str = "prefix_values";
    pub(in crate::db) const PROJECTION_FIELD: &str = "proj_field";
    pub(in crate::db) const PROJECTION_FIELDS: &str = "proj_fields";
    #[cfg(feature = "sql-explain")]
    pub(in crate::db) const PROJECTION_MATERIALIZATION: &str = "proj_materialization";
    pub(in crate::db) const PROJECTION_MODE: &str = "proj_mode";
    pub(in crate::db) const PROJECTION_PUSHDOWN: &str = "proj_pushdown";
    pub(in crate::db) const PUSHDOWN: &str = "pushdown";
    pub(in crate::db) const RESIDUAL_FILTER_SHAPE: &str = "residual_filter_shape";
    pub(in crate::db) const RESUME_FROM: &str = "resume_from";
    pub(in crate::db) const SCAN_DIRECTION: &str = "scan_dir";
    pub(in crate::db) const TERMINAL: &str = "terminal";
    pub(in crate::db) const TERMINAL_FIELD: &str = "terminal_field";
    pub(in crate::db) const TERMINAL_INDEX_ONLY: &str = "terminal_index_only";
    pub(in crate::db) const TERMINAL_OUTPUT: &str = "terminal_output";
    pub(in crate::db) const TERMINAL_PROJECTION_MODE: &str = "terminal_projection_mode";
}

/// Stable EXPLAIN scalar label vocabulary shared only where the same semantic
/// label is intentionally projected on multiple surfaces.
pub(in crate::db) mod property_values {
    pub(in crate::db) const COVERING_READ: &str = "covering_read";
    #[cfg(feature = "sql-explain")]
    pub(in crate::db) const DIRECT_SLOT_ROW: &str = "direct_slot_row";
    pub(in crate::db) const HYBRID_COVERING: &str = "hybrid_covering";
    pub(in crate::db) const MATERIALIZED: &str = "materialized";
    pub(in crate::db) const NONE: &str = "none";
    pub(in crate::db) const PURE_COVERING: &str = "pure_covering";
    #[cfg(feature = "sql-explain")]
    pub(in crate::db) const SCALAR_PROJECTION: &str = "scalar_projection";
    pub(in crate::db) const STRICT_ALL_OR_NONE: &str = "strict_all_or_none";
}

#[cfg_attr(
    doc,
    doc = "ExplainAggregateTerminalPlan\n\nCombined EXPLAIN payload for one scalar aggregate request."
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainAggregateTerminalPlan {
    pub(in crate::db) query: ExplainPlan,
    pub(in crate::db) terminal: AggregateKind,
    pub(in crate::db) execution: ExplainExecutionDescriptor,
}

#[cfg_attr(
    doc,
    doc = "ExplainExecutionOrderingSource\n\nOrdering-origin label used by execution EXPLAIN output."
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExplainExecutionOrderingSource {
    AccessOrder,
    Materialized,
    IndexSeekFirst { fetch: usize },
    IndexSeekLast { fetch: usize },
}

#[cfg_attr(
    doc,
    doc = "ExplainExecutionMode\n\nExecution mode used by EXPLAIN descriptors."
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExplainExecutionMode {
    Streaming,
    Materialized,
}

#[cfg_attr(
    doc,
    doc = "ExplainExecutionDescriptor\n\nScalar execution descriptor consumed by terminal EXPLAIN surfaces.\nKeeps execution projection centralized for renderers."
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainExecutionDescriptor {
    pub(in crate::db) access_strategy: ExplainAccessPath,
    pub(in crate::db) covering_projection: bool,
    pub(in crate::db) aggregation: AggregateKind,
    pub(in crate::db) execution_mode: ExplainExecutionMode,
    pub(in crate::db) ordering_source: ExplainExecutionOrderingSource,
    pub(in crate::db) limit: Option<u32>,
    pub(in crate::db) cursor: bool,
    pub(in crate::db) node_properties: ExplainPropertyMap,
}

#[cfg_attr(
    doc,
    doc = "ExplainExecutionNodeType\n\nExecution-node vocabulary for EXPLAIN descriptors."
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExplainExecutionNodeType {
    ByKeyLookup,
    ByKeysLookup,
    PrimaryKeyRangeScan,
    IndexPrefixScan,
    IndexRangeScan,
    IndexMultiLookup,
    IndexBranchSet,
    FullScan,
    Union,
    Intersection,
    IndexPredicatePrefilter,
    ResidualFilter,
    OrderByAccessSatisfied,
    OrderByMaterializedSort,
    DistinctPreOrdered,
    DistinctMaterialized,
    ProjectionMaterialized,
    CoveringRead,
    LimitOffset,
    CursorResume,
    IndexRangeLimitPushdown,
    TopNSeek,
    AggregateCount,
    AggregateExists,
    AggregateMin,
    AggregateMax,
    AggregateFirst,
    AggregateLast,
    AggregateSum,
    AggregateSeekFirst,
    AggregateSeekLast,
    GroupedAggregateHashMaterialized,
    GroupedAggregateOrderedMaterialized,
    SecondaryOrderPushdown,
}

#[cfg_attr(
    doc,
    doc = "ExplainExecutionNodeDescriptor\n\nCanonical execution-node descriptor for EXPLAIN renderers.\nOptional fields are node-family specific."
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainExecutionNodeDescriptor {
    pub(in crate::db) node_type: ExplainExecutionNodeType,
    pub(in crate::db) execution_mode: ExplainExecutionMode,
    pub(in crate::db) access_strategy: Option<ExplainAccessPath>,
    pub(in crate::db) predicate_pushdown: Option<String>,
    pub(in crate::db) filter_expr: Option<String>,
    pub(in crate::db) residual_filter_expr: Option<String>,
    pub(in crate::db) residual_filter_predicate: Option<ExplainPredicate>,
    pub(in crate::db) projection: Option<String>,
    pub(in crate::db) ordering_source: Option<ExplainExecutionOrderingSource>,
    pub(in crate::db) limit: Option<u32>,
    pub(in crate::db) cursor: Option<bool>,
    pub(in crate::db) covering_scan: Option<bool>,
    pub(in crate::db) rows_expected: Option<u64>,
    pub(in crate::db) children: Vec<Self>,
    pub(in crate::db) node_properties: ExplainPropertyMap,
}

///
/// FinalizedQueryDiagnostics
///
/// FinalizedQueryDiagnostics freezes one immutable execution-explain
/// diagnostics artifact after descriptor assembly and plan-level diagnostics
/// projection are complete.
/// Session and SQL wrappers render this artifact directly instead of
/// reconstructing verbose diagnostics from separate local line builders.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct FinalizedQueryDiagnostics {
    pub(in crate::db) execution: ExplainExecutionNodeDescriptor,
    pub(in crate::db) admission: Option<QueryAdmissionSummary>,
    pub(in crate::db) route_diagnostics: Vec<String>,
    pub(in crate::db) logical_diagnostics: Vec<String>,
    pub(in crate::db) reuse: Option<TraceReuseEvent>,
}

impl ExplainAggregateTerminalPlan {
    /// Borrow the underlying query explain payload.
    #[must_use]
    pub const fn query(&self) -> &ExplainPlan {
        &self.query
    }

    /// Return terminal aggregate kind.
    #[must_use]
    pub const fn terminal(&self) -> AggregateKind {
        self.terminal
    }

    /// Borrow projected execution descriptor.
    #[must_use]
    pub const fn execution(&self) -> &ExplainExecutionDescriptor {
        &self.execution
    }

    #[must_use]
    pub(in crate::db) const fn new(
        query: ExplainPlan,
        terminal: AggregateKind,
        execution: ExplainExecutionDescriptor,
    ) -> Self {
        Self {
            query,
            terminal,
            execution,
        }
    }
}

impl ExplainExecutionDescriptor {
    /// Borrow projected access strategy.
    #[must_use]
    pub const fn access_strategy(&self) -> &ExplainAccessPath {
        &self.access_strategy
    }

    /// Return whether projection can be served from index payload only.
    #[must_use]
    pub const fn covering_projection(&self) -> bool {
        self.covering_projection
    }

    /// Return projected aggregate kind.
    #[must_use]
    pub const fn aggregation(&self) -> AggregateKind {
        self.aggregation
    }

    /// Return projected execution mode.
    #[must_use]
    pub const fn execution_mode(&self) -> ExplainExecutionMode {
        self.execution_mode
    }

    /// Return projected ordering source.
    #[must_use]
    pub const fn ordering_source(&self) -> ExplainExecutionOrderingSource {
        self.ordering_source
    }

    /// Return projected execution limit.
    #[must_use]
    pub const fn limit(&self) -> Option<u32> {
        self.limit
    }

    /// Return whether continuation was applied.
    #[must_use]
    pub const fn cursor(&self) -> bool {
        self.cursor
    }

    /// Borrow projected execution node properties.
    #[must_use]
    pub const fn node_properties(&self) -> &ExplainPropertyMap {
        &self.node_properties
    }
}

impl FinalizedQueryDiagnostics {
    /// Construct one immutable execution diagnostics artifact.
    #[must_use]
    pub(in crate::db) const fn new(
        execution: ExplainExecutionNodeDescriptor,
        route_diagnostics: Vec<String>,
        logical_diagnostics: Vec<String>,
        reuse: Option<TraceReuseEvent>,
    ) -> Self {
        Self {
            execution,
            admission: None,
            route_diagnostics,
            logical_diagnostics,
            reuse,
        }
    }

    /// Borrow the frozen execution descriptor carried by this artifact.
    #[must_use]
    pub(in crate::db) const fn execution(&self) -> &ExplainExecutionNodeDescriptor {
        &self.execution
    }

    /// Attach an admission summary to this diagnostics artifact.
    #[must_use]
    pub(in crate::db) fn with_admission(mut self, admission: QueryAdmissionSummary) -> Self {
        self.admission = Some(admission);
        self
    }

    /// Borrow the admission summary carried by this artifact, if present.
    #[must_use]
    pub(in crate::db) const fn admission(&self) -> Option<&QueryAdmissionSummary> {
        self.admission.as_ref()
    }
}

/// Annotate one aggregate execution node with the shared semantic/physical
/// identity vocabulary consumed by SQL, fluent, and JSON EXPLAIN surfaces.
pub(in crate::db) fn annotate_aggregate_execution_identity_properties(
    node_properties: &mut ExplainPropertyMap,
    contract: &'static str,
    physical: &'static str,
) {
    node_properties.insert(property_keys::AGGREGATE_CONTRACT, Value::from(contract));
    node_properties.insert(property_keys::AGGREGATE_PHYSICAL, Value::from(physical));
}

impl ExplainAggregateTerminalPlan {
    /// Build an execution-node descriptor for aggregate terminal plans.
    #[must_use]
    pub fn execution_node_descriptor(&self) -> ExplainExecutionNodeDescriptor {
        let mut node_properties = self.execution.node_properties.clone();
        annotate_aggregate_execution_identity_properties(
            &mut node_properties,
            "singleton",
            scalar_aggregate_physical_label(self.execution.ordering_source),
        );

        ExplainExecutionNodeDescriptor {
            node_type: match self.execution.ordering_source {
                ExplainExecutionOrderingSource::IndexSeekFirst { .. } => {
                    ExplainExecutionNodeType::AggregateSeekFirst
                }
                ExplainExecutionOrderingSource::IndexSeekLast { .. } => {
                    ExplainExecutionNodeType::AggregateSeekLast
                }
                ExplainExecutionOrderingSource::AccessOrder
                | ExplainExecutionOrderingSource::Materialized => {
                    self.terminal.explain_execution_node_type()
                }
            },
            execution_mode: self.execution.execution_mode,
            access_strategy: Some(self.execution.access_strategy.clone()),
            predicate_pushdown: None,
            filter_expr: None,
            residual_filter_expr: None,
            residual_filter_predicate: None,
            projection: None,
            ordering_source: Some(self.execution.ordering_source),
            limit: self.execution.limit,
            cursor: Some(self.execution.cursor),
            covering_scan: Some(self.execution.covering_projection),
            rows_expected: None,
            children: Vec::new(),
            node_properties,
        }
    }
}

const fn scalar_aggregate_physical_label(
    ordering_source: ExplainExecutionOrderingSource,
) -> &'static str {
    match ordering_source {
        ExplainExecutionOrderingSource::IndexSeekFirst { .. } => "scalar_seek_first",
        ExplainExecutionOrderingSource::IndexSeekLast { .. } => "scalar_seek_last",
        ExplainExecutionOrderingSource::AccessOrder
        | ExplainExecutionOrderingSource::Materialized => "scalar_terminal",
    }
}

impl AggregateKind {
    /// Return the canonical explain execution-node type for this aggregate
    /// terminal kind when no seek-first/seek-last override applies.
    #[must_use]
    pub(in crate::db) const fn explain_execution_node_type(self) -> ExplainExecutionNodeType {
        match self {
            Self::Count => ExplainExecutionNodeType::AggregateCount,
            Self::Exists => ExplainExecutionNodeType::AggregateExists,
            Self::Min => ExplainExecutionNodeType::AggregateMin,
            Self::Max => ExplainExecutionNodeType::AggregateMax,
            Self::First => ExplainExecutionNodeType::AggregateFirst,
            Self::Last => ExplainExecutionNodeType::AggregateLast,
            Self::Sum | Self::Avg => ExplainExecutionNodeType::AggregateSum,
        }
    }
}

impl ExplainExecutionNodeType {
    /// Return the stable string label used by explain renderers.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ByKeyLookup => "ByKeyLookup",
            Self::ByKeysLookup => "ByKeysLookup",
            Self::PrimaryKeyRangeScan => "PrimaryKeyRangeScan",
            Self::IndexPrefixScan => "IndexPrefixScan",
            Self::IndexRangeScan => "IndexRangeScan",
            Self::IndexMultiLookup => "IndexMultiLookup",
            Self::IndexBranchSet => "IndexBranchSet",
            Self::FullScan => "FullScan",
            Self::Union => "Union",
            Self::Intersection => "Intersection",
            Self::IndexPredicatePrefilter => "IndexPredicatePrefilter",
            Self::ResidualFilter => "ResidualFilter",
            Self::OrderByAccessSatisfied => "OrderByAccessSatisfied",
            Self::OrderByMaterializedSort => "OrderByMaterializedSort",
            Self::DistinctPreOrdered => "DistinctPreOrdered",
            Self::DistinctMaterialized => "DistinctMaterialized",
            Self::ProjectionMaterialized => "ProjectionMaterialized",
            Self::CoveringRead => "CoveringRead",
            Self::LimitOffset => "LimitOffset",
            Self::CursorResume => "CursorResume",
            Self::IndexRangeLimitPushdown => "IndexRangeLimitPushdown",
            Self::TopNSeek => "TopNSeek",
            Self::AggregateCount => "AggregateCount",
            Self::AggregateExists => "AggregateExists",
            Self::AggregateMin => "AggregateMin",
            Self::AggregateMax => "AggregateMax",
            Self::AggregateFirst => "AggregateFirst",
            Self::AggregateLast => "AggregateLast",
            Self::AggregateSum => "AggregateSum",
            Self::AggregateSeekFirst => "AggregateSeekFirst",
            Self::AggregateSeekLast => "AggregateSeekLast",
            Self::GroupedAggregateHashMaterialized => "GroupedAggregateHashMaterialized",
            Self::GroupedAggregateOrderedMaterialized => "GroupedAggregateOrderedMaterialized",
            Self::SecondaryOrderPushdown => "SecondaryOrderPushdown",
        }
    }

    /// Return the owning execution layer label for this node type.
    #[must_use]
    pub const fn layer_label(self) -> &'static str {
        crate::db::query::explain::nodes::layer_label(self)
    }
}

impl ExplainExecutionNodeDescriptor {
    /// Visit this execution-descriptor tree in deterministic preorder.
    pub(in crate::db) fn for_each_preorder(&self, visit: &mut impl FnMut(&Self)) {
        visit(self);

        for child in self.children() {
            child.for_each_preorder(visit);
        }
    }

    /// Return whether this descriptor tree contains the requested node type.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn contains_type(&self, target: ExplainExecutionNodeType) -> bool {
        let mut found = false;
        self.for_each_preorder(&mut |node| {
            if node.node_type() == target {
                found = true;
            }
        });

        found
    }

    /// Return node type.
    #[must_use]
    pub const fn node_type(&self) -> ExplainExecutionNodeType {
        self.node_type
    }

    /// Return execution mode.
    #[must_use]
    pub const fn execution_mode(&self) -> ExplainExecutionMode {
        self.execution_mode
    }

    /// Borrow optional access strategy annotation.
    #[must_use]
    pub const fn access_strategy(&self) -> Option<&ExplainAccessPath> {
        self.access_strategy.as_ref()
    }

    /// Borrow optional predicate pushdown annotation.
    #[must_use]
    pub fn predicate_pushdown(&self) -> Option<&str> {
        self.predicate_pushdown.as_deref()
    }

    /// Borrow optional semantic scalar filter expression annotation.
    #[must_use]
    pub fn filter_expr(&self) -> Option<&str> {
        self.filter_expr.as_deref()
    }

    /// Borrow the optional explicit residual scalar filter expression.
    #[must_use]
    pub fn residual_filter_expr(&self) -> Option<&str> {
        self.residual_filter_expr.as_deref()
    }

    /// Borrow the optional derived residual predicate annotation emitted
    /// alongside `filter_expr` when execution still benefits from predicate
    /// pushdown labeling.
    #[must_use]
    pub const fn residual_filter_predicate(&self) -> Option<&ExplainPredicate> {
        self.residual_filter_predicate.as_ref()
    }

    /// Return this node's residual-filter annotation shape.
    #[must_use]
    pub(in crate::db) const fn residual_filter_shape(&self) -> ResidualFilterShape {
        ResidualFilterShape::from_presence(
            self.residual_filter_expr.is_some(),
            self.residual_filter_predicate.is_some(),
        )
    }

    /// Return whether this node carries any residual filter annotation.
    #[must_use]
    pub const fn has_residual_filter(&self) -> bool {
        !self.residual_filter_shape().is_absent()
    }

    /// Borrow optional projection annotation.
    #[must_use]
    pub fn projection(&self) -> Option<&str> {
        self.projection.as_deref()
    }

    /// Return optional ordering source annotation.
    #[must_use]
    pub const fn ordering_source(&self) -> Option<ExplainExecutionOrderingSource> {
        self.ordering_source
    }

    /// Return optional limit annotation.
    #[must_use]
    pub const fn limit(&self) -> Option<u32> {
        self.limit
    }

    /// Return optional continuation annotation.
    #[must_use]
    pub const fn cursor(&self) -> Option<bool> {
        self.cursor
    }

    /// Return optional covering-scan annotation.
    #[must_use]
    pub const fn covering_scan(&self) -> Option<bool> {
        self.covering_scan
    }

    /// Return optional row-count expectation annotation.
    #[must_use]
    pub const fn rows_expected(&self) -> Option<u64> {
        self.rows_expected
    }

    /// Borrow child execution nodes.
    #[must_use]
    pub const fn children(&self) -> &[Self] {
        self.children.as_slice()
    }

    /// Borrow node properties.
    #[must_use]
    pub const fn node_properties(&self) -> &ExplainPropertyMap {
        &self.node_properties
    }
}

pub(in crate::db::query::explain) const fn execution_mode_label(
    mode: ExplainExecutionMode,
) -> &'static str {
    match mode {
        ExplainExecutionMode::Streaming => "Streaming",
        ExplainExecutionMode::Materialized => "Materialized",
    }
}

pub(in crate::db::query::explain) const fn ordering_source_label(
    ordering_source: ExplainExecutionOrderingSource,
) -> &'static str {
    match ordering_source {
        ExplainExecutionOrderingSource::AccessOrder => "AccessOrder",
        ExplainExecutionOrderingSource::Materialized => "Materialized",
        ExplainExecutionOrderingSource::IndexSeekFirst { .. } => "IndexSeekFirst",
        ExplainExecutionOrderingSource::IndexSeekLast { .. } => "IndexSeekLast",
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::query::explain::{
        ExplainExecutionMode, ExplainExecutionNodeDescriptor, ExplainExecutionNodeType,
        ExplainPropertyMap,
    };

    fn node(
        node_type: ExplainExecutionNodeType,
        children: Vec<ExplainExecutionNodeDescriptor>,
    ) -> ExplainExecutionNodeDescriptor {
        ExplainExecutionNodeDescriptor {
            node_type,
            execution_mode: ExplainExecutionMode::Materialized,
            access_strategy: None,
            predicate_pushdown: None,
            filter_expr: None,
            residual_filter_expr: None,
            residual_filter_predicate: None,
            projection: None,
            ordering_source: None,
            limit: None,
            cursor: None,
            covering_scan: None,
            rows_expected: None,
            children,
            node_properties: ExplainPropertyMap::new(),
        }
    }

    #[test]
    fn execution_node_contains_type_scans_preorder_tree() {
        let root = node(
            ExplainExecutionNodeType::Union,
            vec![
                node(ExplainExecutionNodeType::FullScan, Vec::new()),
                node(
                    ExplainExecutionNodeType::Intersection,
                    vec![node(ExplainExecutionNodeType::ResidualFilter, Vec::new())],
                ),
            ],
        );

        assert!(root.contains_type(ExplainExecutionNodeType::ResidualFilter));
        assert!(!root.contains_type(ExplainExecutionNodeType::TopNSeek));
    }

    #[test]
    fn execution_node_preorder_visits_parent_before_children() {
        let root = node(
            ExplainExecutionNodeType::Union,
            vec![
                node(ExplainExecutionNodeType::FullScan, Vec::new()),
                node(
                    ExplainExecutionNodeType::Intersection,
                    vec![node(ExplainExecutionNodeType::ResidualFilter, Vec::new())],
                ),
            ],
        );
        let mut visited = Vec::new();

        root.for_each_preorder(&mut |node| visited.push(node.node_type()));

        assert_eq!(
            visited,
            vec![
                ExplainExecutionNodeType::Union,
                ExplainExecutionNodeType::FullScan,
                ExplainExecutionNodeType::Intersection,
                ExplainExecutionNodeType::ResidualFilter,
            ],
        );
    }
}
