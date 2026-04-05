//! Module: query::explain::execution
//! Responsibility: stable execution-descriptor vocabulary for EXPLAIN.
//! Does not own: logical plan projection or rendering logic.
//! Boundary: execution descriptor types consumed by explain renderers.

use crate::{
    db::query::{
        explain::{ExplainAccessPath, ExplainPlan, ExplainPredicate},
        plan::AggregateKind,
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

#[cfg_attr(
    doc,
    doc = "ExplainAggregateTerminalRoute\n\nScalar aggregate route label used by EXPLAIN output."
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExplainAggregateTerminalRoute {
    Standard,
    IndexSeekFirst { fetch: usize },
    IndexSeekLast { fetch: usize },
}

#[cfg_attr(
    doc,
    doc = "ExplainAggregateTerminalPlan\n\nCombined EXPLAIN payload for one scalar aggregate request."
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainAggregateTerminalPlan {
    pub(crate) query: ExplainPlan,
    pub(crate) terminal: AggregateKind,
    pub(crate) route: ExplainAggregateTerminalRoute,
    pub(crate) execution: ExplainExecutionDescriptor,
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
    pub(crate) access_strategy: ExplainAccessPath,
    pub(crate) covering_projection: bool,
    pub(crate) aggregation: AggregateKind,
    pub(crate) execution_mode: ExplainExecutionMode,
    pub(crate) ordering_source: ExplainExecutionOrderingSource,
    pub(crate) limit: Option<u32>,
    pub(crate) cursor: bool,
    pub(crate) node_properties: ExplainPropertyMap,
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
    FullScan,
    Union,
    Intersection,
    IndexPredicatePrefilter,
    ResidualPredicateFilter,
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
    pub(crate) node_type: ExplainExecutionNodeType,
    pub(crate) execution_mode: ExplainExecutionMode,
    pub(crate) access_strategy: Option<ExplainAccessPath>,
    pub(crate) predicate_pushdown: Option<String>,
    pub(crate) residual_predicate: Option<ExplainPredicate>,
    pub(crate) projection: Option<String>,
    pub(crate) ordering_source: Option<ExplainExecutionOrderingSource>,
    pub(crate) limit: Option<u32>,
    pub(crate) cursor: Option<bool>,
    pub(crate) covering_scan: Option<bool>,
    pub(crate) rows_expected: Option<u64>,
    pub(crate) children: Vec<Self>,
    pub(crate) node_properties: ExplainPropertyMap,
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

    /// Return projected aggregate terminal route.
    #[must_use]
    pub const fn route(&self) -> ExplainAggregateTerminalRoute {
        self.route
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
        let route = execution.route();

        Self {
            query,
            terminal,
            route,
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

    #[must_use]
    pub(in crate::db) const fn route(&self) -> ExplainAggregateTerminalRoute {
        match self.ordering_source {
            ExplainExecutionOrderingSource::IndexSeekFirst { fetch } => {
                ExplainAggregateTerminalRoute::IndexSeekFirst { fetch }
            }
            ExplainExecutionOrderingSource::IndexSeekLast { fetch } => {
                ExplainAggregateTerminalRoute::IndexSeekLast { fetch }
            }
            ExplainExecutionOrderingSource::AccessOrder
            | ExplainExecutionOrderingSource::Materialized => {
                ExplainAggregateTerminalRoute::Standard
            }
        }
    }
}

impl ExplainAggregateTerminalPlan {
    /// Build an execution-node descriptor for aggregate terminal plans.
    #[must_use]
    pub fn execution_node_descriptor(&self) -> ExplainExecutionNodeDescriptor {
        ExplainExecutionNodeDescriptor {
            node_type: aggregate_execution_node_type(self.terminal, self.execution.ordering_source),
            execution_mode: self.execution.execution_mode,
            access_strategy: Some(self.execution.access_strategy.clone()),
            predicate_pushdown: None,
            residual_predicate: None,
            projection: None,
            ordering_source: Some(self.execution.ordering_source),
            limit: self.execution.limit,
            cursor: Some(self.execution.cursor),
            covering_scan: Some(self.execution.covering_projection),
            rows_expected: None,
            children: Vec::new(),
            node_properties: self.execution.node_properties.clone(),
        }
    }
}

const fn aggregate_execution_node_type(
    terminal: AggregateKind,
    ordering_source: ExplainExecutionOrderingSource,
) -> ExplainExecutionNodeType {
    match ordering_source {
        ExplainExecutionOrderingSource::IndexSeekFirst { .. } => {
            ExplainExecutionNodeType::AggregateSeekFirst
        }
        ExplainExecutionOrderingSource::IndexSeekLast { .. } => {
            ExplainExecutionNodeType::AggregateSeekLast
        }
        ExplainExecutionOrderingSource::AccessOrder
        | ExplainExecutionOrderingSource::Materialized => {
            ExplainExecutionNodeType::aggregate_terminal(terminal)
        }
    }
}

impl ExplainExecutionNodeType {
    /// Return the canonical execution-node type for one aggregate terminal kind.
    #[must_use]
    pub(in crate::db) const fn aggregate_terminal(kind: AggregateKind) -> Self {
        match kind {
            AggregateKind::Count => Self::AggregateCount,
            AggregateKind::Exists => Self::AggregateExists,
            AggregateKind::Min => Self::AggregateMin,
            AggregateKind::Max => Self::AggregateMax,
            AggregateKind::First => Self::AggregateFirst,
            AggregateKind::Last => Self::AggregateLast,
            AggregateKind::Sum | AggregateKind::Avg => Self::AggregateSum,
        }
    }

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
            Self::FullScan => "FullScan",
            Self::Union => "Union",
            Self::Intersection => "Intersection",
            Self::IndexPredicatePrefilter => "IndexPredicatePrefilter",
            Self::ResidualPredicateFilter => "ResidualPredicateFilter",
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

    /// Borrow optional residual predicate annotation.
    #[must_use]
    pub const fn residual_predicate(&self) -> Option<&ExplainPredicate> {
        self.residual_predicate.as_ref()
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
