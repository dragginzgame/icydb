//! Module: query::explain
//! Responsibility: deterministic, read-only projection of logical query plans.
//! Does not own: plan execution or semantic validation.
//! Boundary: diagnostics/explain surface over intent/planner outputs.

use crate::{
    db::{
        access::{
            AccessPlan, PushdownSurfaceEligibility, SecondaryOrderPushdownEligibility,
            SecondaryOrderPushdownRejection,
        },
        predicate::{
            CoercionSpec, CompareOp, ComparePredicate, MissingRowPolicy, Predicate, normalize,
        },
        query::{
            access::{AccessPathVisitor, visit_explain_access_path},
            plan::{
                AccessPlanProjection, AccessPlannedQuery, AggregateKind, DeleteLimitSpec,
                GroupHavingClause, GroupHavingSpec, GroupHavingSymbol, GroupedPlanStrategyHint,
                LogicalPlan, OrderDirection, OrderSpec, PageSpec, QueryMode, ScalarPlan,
                grouped_plan_strategy_hint_for_plan, project_access_plan,
            },
        },
    },
    model::entity::EntityModel,
    traits::FieldValue,
    value::Value,
};
use std::{collections::BTreeMap, fmt::Write, ops::Bound};

///
/// ExplainPlan
///
/// Stable, deterministic representation of a planned query for observability.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainPlan {
    pub(crate) mode: QueryMode,
    pub(crate) access: ExplainAccessPath,
    pub(crate) predicate: ExplainPredicate,
    predicate_model: Option<Predicate>,
    pub(crate) order_by: ExplainOrderBy,
    pub(crate) distinct: bool,
    pub(crate) grouping: ExplainGrouping,
    pub(crate) order_pushdown: ExplainOrderPushdown,
    pub(crate) page: ExplainPagination,
    pub(crate) delete_limit: ExplainDeleteLimit,
    pub(crate) consistency: MissingRowPolicy,
}

///
/// ExplainAggregateTerminalRoute
///
/// Executor-projected scalar aggregate terminal route label for explain output.
/// Keeps seek-edge fast-path labels explicit without exposing route internals.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExplainAggregateTerminalRoute {
    Standard,
    IndexSeekFirst { fetch: usize },
    IndexSeekLast { fetch: usize },
}

///
/// ExplainAggregateTerminalPlan
///
/// Combined explain payload for one scalar aggregate terminal request.
/// Includes logical explain projection plus executor route label.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainAggregateTerminalPlan {
    pub(crate) query: ExplainPlan,
    pub(crate) terminal: AggregateKind,
    pub(crate) route: ExplainAggregateTerminalRoute,
    pub(crate) execution: ExplainExecutionDescriptor,
}

///
/// ExplainExecutionOrderingSource
///
/// Stable ordering-origin projection used by terminal execution explain output.
/// This keeps index-seek labels and materialized fallback labels explicit.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExplainExecutionOrderingSource {
    AccessOrder,
    Materialized,
    IndexSeekFirst { fetch: usize },
    IndexSeekLast { fetch: usize },
}

///
/// ExplainExecutionMode
///
/// Stable execution-mode projection used by execution explain descriptors.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExplainExecutionMode {
    Streaming,
    Materialized,
}

///
/// ExplainExecutionDescriptor
///
/// Stable scalar execution descriptor consumed by terminal EXPLAIN surfaces.
/// This keeps execution authority projection centralized and avoids ad-hoc
/// terminal-specific explain branching at call sites.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainExecutionDescriptor {
    pub(crate) access_strategy: ExplainAccessPath,
    pub(crate) covering_projection: bool,
    pub(crate) aggregation: AggregateKind,
    pub(crate) execution_mode: ExplainExecutionMode,
    pub(crate) ordering_source: ExplainExecutionOrderingSource,
    pub(crate) limit: Option<u32>,
    pub(crate) cursor: bool,
    pub(crate) node_properties: BTreeMap<String, Value>,
}

///
/// ExplainExecutionNodeType
///
/// Stable execution-node vocabulary for EXPLAIN descriptor projection.
///
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
    ProjectionIndexOnly,
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

///
/// ExplainExecutionNodeDescriptor
///
/// Canonical execution-node descriptor used by EXPLAIN text/verbose/json
/// renderers. Optional fields are node-family specific and are additive.
///
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
    pub(crate) node_properties: BTreeMap<String, Value>,
}

impl ExplainPlan {
    /// Return query mode projected by this explain plan.
    #[must_use]
    pub const fn mode(&self) -> QueryMode {
        self.mode
    }

    /// Borrow projected access-path shape.
    #[must_use]
    pub const fn access(&self) -> &ExplainAccessPath {
        &self.access
    }

    /// Borrow projected predicate shape.
    #[must_use]
    pub const fn predicate(&self) -> &ExplainPredicate {
        &self.predicate
    }

    /// Borrow projected ORDER BY shape.
    #[must_use]
    pub const fn order_by(&self) -> &ExplainOrderBy {
        &self.order_by
    }

    /// Return whether DISTINCT is enabled.
    #[must_use]
    pub const fn distinct(&self) -> bool {
        self.distinct
    }

    /// Borrow projected grouped-shape metadata.
    #[must_use]
    pub const fn grouping(&self) -> &ExplainGrouping {
        &self.grouping
    }

    /// Borrow projected ORDER pushdown status.
    #[must_use]
    pub const fn order_pushdown(&self) -> &ExplainOrderPushdown {
        &self.order_pushdown
    }

    /// Borrow projected pagination status.
    #[must_use]
    pub const fn page(&self) -> &ExplainPagination {
        &self.page
    }

    /// Borrow projected delete-limit status.
    #[must_use]
    pub const fn delete_limit(&self) -> &ExplainDeleteLimit {
        &self.delete_limit
    }

    /// Return missing-row consistency policy.
    #[must_use]
    pub const fn consistency(&self) -> MissingRowPolicy {
        self.consistency
    }
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
    pub const fn node_properties(&self) -> &BTreeMap<String, Value> {
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
        | ExplainExecutionOrderingSource::Materialized => match terminal {
            AggregateKind::Count => ExplainExecutionNodeType::AggregateCount,
            AggregateKind::Exists => ExplainExecutionNodeType::AggregateExists,
            AggregateKind::Min => ExplainExecutionNodeType::AggregateMin,
            AggregateKind::Max => ExplainExecutionNodeType::AggregateMax,
            AggregateKind::First => ExplainExecutionNodeType::AggregateFirst,
            AggregateKind::Last => ExplainExecutionNodeType::AggregateLast,
            AggregateKind::Sum => ExplainExecutionNodeType::AggregateSum,
        },
    }
}

impl ExplainExecutionNodeType {
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
            Self::ProjectionIndexOnly => "ProjectionIndexOnly",
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
    pub const fn node_properties(&self) -> &BTreeMap<String, Value> {
        &self.node_properties
    }

    #[must_use]
    pub fn render_text_tree(&self) -> String {
        let mut lines = Vec::new();
        self.render_text_tree_into(0, &mut lines);
        lines.join("\n")
    }

    #[must_use]
    pub fn render_json_canonical(&self) -> String {
        let mut out = String::new();
        write_execution_node_json(self, &mut out);
        out
    }

    #[must_use]
    pub fn render_text_tree_verbose(&self) -> String {
        let mut lines = Vec::new();
        self.render_text_tree_verbose_into(0, &mut lines);
        lines.join("\n")
    }

    fn render_text_tree_into(&self, depth: usize, lines: &mut Vec<String>) {
        let mut line = format!(
            "{}{} execution_mode={}",
            "  ".repeat(depth),
            self.node_type.as_str(),
            execution_mode_label(self.execution_mode)
        );

        if let Some(access_strategy) = self.access_strategy.as_ref() {
            let _ = write!(line, " access={}", access_strategy_label(access_strategy));
        }
        if let Some(predicate_pushdown) = self.predicate_pushdown.as_ref() {
            let _ = write!(line, " predicate_pushdown={predicate_pushdown}");
        }
        if let Some(residual_predicate) = self.residual_predicate.as_ref() {
            let _ = write!(line, " residual_predicate={residual_predicate:?}");
        }
        if let Some(projection) = self.projection.as_ref() {
            let _ = write!(line, " projection={projection}");
        }
        if let Some(ordering_source) = self.ordering_source {
            let _ = write!(
                line,
                " ordering_source={}",
                ordering_source_label(ordering_source)
            );
        }
        if let Some(limit) = self.limit {
            let _ = write!(line, " limit={limit}");
        }
        if let Some(cursor) = self.cursor {
            let _ = write!(line, " cursor={cursor}");
        }
        if let Some(covering_scan) = self.covering_scan {
            let _ = write!(line, " covering_scan={covering_scan}");
        }
        if let Some(rows_expected) = self.rows_expected {
            let _ = write!(line, " rows_expected={rows_expected}");
        }
        if !self.node_properties.is_empty() {
            let _ = write!(
                line,
                " node_properties={}",
                render_node_properties(&self.node_properties)
            );
        }

        lines.push(line);

        for child in &self.children {
            child.render_text_tree_into(depth.saturating_add(1), lines);
        }
    }

    fn render_text_tree_verbose_into(&self, depth: usize, lines: &mut Vec<String>) {
        // Emit the node heading line first so child metadata stays visually scoped.
        let node_indent = "  ".repeat(depth);
        let field_indent = "  ".repeat(depth.saturating_add(1));
        lines.push(format!(
            "{}{} execution_mode={}",
            node_indent,
            self.node_type.as_str(),
            execution_mode_label(self.execution_mode)
        ));

        // Emit all optional node-local fields in a deterministic order.
        if let Some(access_strategy) = self.access_strategy.as_ref() {
            lines.push(format!("{field_indent}access_strategy={access_strategy:?}"));
        }
        if let Some(predicate_pushdown) = self.predicate_pushdown.as_ref() {
            lines.push(format!(
                "{field_indent}predicate_pushdown={predicate_pushdown}"
            ));
        }
        if let Some(residual_predicate) = self.residual_predicate.as_ref() {
            lines.push(format!(
                "{field_indent}residual_predicate={residual_predicate:?}"
            ));
        }
        if let Some(projection) = self.projection.as_ref() {
            lines.push(format!("{field_indent}projection={projection}"));
        }
        if let Some(ordering_source) = self.ordering_source {
            lines.push(format!(
                "{}ordering_source={}",
                field_indent,
                ordering_source_label(ordering_source)
            ));
        }
        if let Some(limit) = self.limit {
            lines.push(format!("{field_indent}limit={limit}"));
        }
        if let Some(cursor) = self.cursor {
            lines.push(format!("{field_indent}cursor={cursor}"));
        }
        if let Some(covering_scan) = self.covering_scan {
            lines.push(format!("{field_indent}covering_scan={covering_scan}"));
        }
        if let Some(rows_expected) = self.rows_expected {
            lines.push(format!("{field_indent}rows_expected={rows_expected}"));
        }
        if !self.node_properties.is_empty() {
            lines.push(format!(
                "{}node_properties={}",
                field_indent,
                render_node_properties(&self.node_properties)
            ));
        }

        // Recurse in execution order to preserve stable tree topology.
        for child in &self.children {
            child.render_text_tree_verbose_into(depth.saturating_add(1), lines);
        }
    }
}

const fn execution_mode_label(mode: ExplainExecutionMode) -> &'static str {
    match mode {
        ExplainExecutionMode::Streaming => "Streaming",
        ExplainExecutionMode::Materialized => "Materialized",
    }
}

fn render_node_properties(node_properties: &BTreeMap<String, Value>) -> String {
    let mut rendered = String::new();
    let mut first = true;
    for (key, value) in node_properties {
        if first {
            first = false;
        } else {
            rendered.push(',');
        }
        let _ = write!(rendered, "{key}={value:?}");
    }
    rendered
}

fn write_execution_node_json(node: &ExplainExecutionNodeDescriptor, out: &mut String) {
    out.push('{');

    write_json_field_name(out, "node_type");
    write_json_string(out, node.node_type.as_str());
    out.push(',');

    write_json_field_name(out, "execution_mode");
    write_json_string(out, execution_mode_label(node.execution_mode));
    out.push(',');

    write_json_field_name(out, "access_strategy");
    match node.access_strategy.as_ref() {
        Some(access) => write_access_json(access, out),
        None => out.push_str("null"),
    }
    out.push(',');

    write_json_field_name(out, "predicate_pushdown");
    match node.predicate_pushdown.as_ref() {
        Some(predicate_pushdown) => write_json_string(out, predicate_pushdown),
        None => out.push_str("null"),
    }
    out.push(',');

    write_json_field_name(out, "residual_predicate");
    match node.residual_predicate.as_ref() {
        Some(residual_predicate) => write_json_string(out, &format!("{residual_predicate:?}")),
        None => out.push_str("null"),
    }
    out.push(',');

    write_json_field_name(out, "projection");
    match node.projection.as_ref() {
        Some(projection) => write_json_string(out, projection),
        None => out.push_str("null"),
    }
    out.push(',');

    write_json_field_name(out, "ordering_source");
    match node.ordering_source {
        Some(ordering_source) => write_json_string(out, ordering_source_label(ordering_source)),
        None => out.push_str("null"),
    }
    out.push(',');

    write_json_field_name(out, "limit");
    match node.limit {
        Some(limit) => out.push_str(&limit.to_string()),
        None => out.push_str("null"),
    }
    out.push(',');

    write_json_field_name(out, "cursor");
    match node.cursor {
        Some(cursor) => out.push_str(if cursor { "true" } else { "false" }),
        None => out.push_str("null"),
    }
    out.push(',');

    write_json_field_name(out, "covering_scan");
    match node.covering_scan {
        Some(covering_scan) => out.push_str(if covering_scan { "true" } else { "false" }),
        None => out.push_str("null"),
    }
    out.push(',');

    write_json_field_name(out, "rows_expected");
    match node.rows_expected {
        Some(rows_expected) => out.push_str(&rows_expected.to_string()),
        None => out.push_str("null"),
    }
    out.push(',');

    write_json_field_name(out, "children");
    out.push('[');
    for (index, child) in node.children.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        write_execution_node_json(child, out);
    }
    out.push(']');
    out.push(',');

    write_json_field_name(out, "node_properties");
    write_node_properties_json(&node.node_properties, out);

    out.push('}');
}

///
/// ExplainJsonVisitor
///
/// Visitor that renders one `ExplainAccessPath` subtree into stable JSON.
///

struct ExplainJsonVisitor<'a> {
    out: &'a mut String,
}

impl AccessPathVisitor<()> for ExplainJsonVisitor<'_> {
    fn visit_by_key(&mut self, key: &Value) {
        self.out.push('{');
        write_json_field_name(self.out, "type");
        write_json_string(self.out, "ByKey");
        self.out.push(',');
        write_json_field_name(self.out, "key");
        write_json_string(self.out, &format!("{key:?}"));
        self.out.push('}');
    }

    fn visit_by_keys(&mut self, keys: &[Value]) {
        self.out.push('{');
        write_json_field_name(self.out, "type");
        write_json_string(self.out, "ByKeys");
        self.out.push(',');
        write_json_field_name(self.out, "keys");
        write_value_vec_as_debug_json(keys, self.out);
        self.out.push('}');
    }

    fn visit_key_range(&mut self, start: &Value, end: &Value) {
        self.out.push('{');
        write_json_field_name(self.out, "type");
        write_json_string(self.out, "KeyRange");
        self.out.push(',');
        write_json_field_name(self.out, "start");
        write_json_string(self.out, &format!("{start:?}"));
        self.out.push(',');
        write_json_field_name(self.out, "end");
        write_json_string(self.out, &format!("{end:?}"));
        self.out.push('}');
    }

    fn visit_index_prefix(
        &mut self,
        name: &'static str,
        fields: &[&'static str],
        prefix_len: usize,
        values: &[Value],
    ) {
        self.out.push('{');
        write_json_field_name(self.out, "type");
        write_json_string(self.out, "IndexPrefix");
        self.out.push(',');
        write_json_field_name(self.out, "name");
        write_json_string(self.out, name);
        self.out.push(',');
        write_json_field_name(self.out, "fields");
        write_str_vec_json(fields, self.out);
        self.out.push(',');
        write_json_field_name(self.out, "prefix_len");
        self.out.push_str(&prefix_len.to_string());
        self.out.push(',');
        write_json_field_name(self.out, "values");
        write_value_vec_as_debug_json(values, self.out);
        self.out.push('}');
    }

    fn visit_index_multi_lookup(
        &mut self,
        name: &'static str,
        fields: &[&'static str],
        values: &[Value],
    ) {
        self.out.push('{');
        write_json_field_name(self.out, "type");
        write_json_string(self.out, "IndexMultiLookup");
        self.out.push(',');
        write_json_field_name(self.out, "name");
        write_json_string(self.out, name);
        self.out.push(',');
        write_json_field_name(self.out, "fields");
        write_str_vec_json(fields, self.out);
        self.out.push(',');
        write_json_field_name(self.out, "values");
        write_value_vec_as_debug_json(values, self.out);
        self.out.push('}');
    }

    fn visit_index_range(
        &mut self,
        name: &'static str,
        fields: &[&'static str],
        prefix_len: usize,
        prefix: &[Value],
        lower: &Bound<Value>,
        upper: &Bound<Value>,
    ) {
        self.out.push('{');
        write_json_field_name(self.out, "type");
        write_json_string(self.out, "IndexRange");
        self.out.push(',');
        write_json_field_name(self.out, "name");
        write_json_string(self.out, name);
        self.out.push(',');
        write_json_field_name(self.out, "fields");
        write_str_vec_json(fields, self.out);
        self.out.push(',');
        write_json_field_name(self.out, "prefix_len");
        self.out.push_str(&prefix_len.to_string());
        self.out.push(',');
        write_json_field_name(self.out, "prefix");
        write_value_vec_as_debug_json(prefix, self.out);
        self.out.push(',');
        write_json_field_name(self.out, "lower");
        write_json_string(self.out, &format!("{lower:?}"));
        self.out.push(',');
        write_json_field_name(self.out, "upper");
        write_json_string(self.out, &format!("{upper:?}"));
        self.out.push('}');
    }

    fn visit_full_scan(&mut self) {
        self.out.push('{');
        write_json_field_name(self.out, "type");
        write_json_string(self.out, "FullScan");
        self.out.push('}');
    }

    fn visit_union(&mut self, children: &[ExplainAccessPath]) {
        self.out.push('{');
        write_json_field_name(self.out, "type");
        write_json_string(self.out, "Union");
        self.out.push(',');
        write_json_field_name(self.out, "children");
        self.out.push('[');
        for (index, child) in children.iter().enumerate() {
            if index > 0 {
                self.out.push(',');
            }
            visit_explain_access_path(child, self);
        }
        self.out.push(']');
        self.out.push('}');
    }

    fn visit_intersection(&mut self, children: &[ExplainAccessPath]) {
        self.out.push('{');
        write_json_field_name(self.out, "type");
        write_json_string(self.out, "Intersection");
        self.out.push(',');
        write_json_field_name(self.out, "children");
        self.out.push('[');
        for (index, child) in children.iter().enumerate() {
            if index > 0 {
                self.out.push(',');
            }
            visit_explain_access_path(child, self);
        }
        self.out.push(']');
        self.out.push('}');
    }
}

fn write_access_json(access: &ExplainAccessPath, out: &mut String) {
    let mut visitor = ExplainJsonVisitor { out };
    visit_explain_access_path(access, &mut visitor);
}

fn write_node_properties_json(node_properties: &BTreeMap<String, Value>, out: &mut String) {
    out.push('{');
    for (index, (key, value)) in node_properties.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        write_json_field_name(out, key);
        write_json_string(out, &format!("{value:?}"));
    }
    out.push('}');
}

fn write_value_vec_as_debug_json(values: &[Value], out: &mut String) {
    out.push('[');
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        write_json_string(out, &format!("{value:?}"));
    }
    out.push(']');
}

fn write_str_vec_json(values: &[&str], out: &mut String) {
    out.push('[');
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        write_json_string(out, value);
    }
    out.push(']');
}

fn write_json_field_name(out: &mut String, key: &str) {
    write_json_string(out, key);
    out.push(':');
}

fn write_json_string(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            _ => out.push(ch),
        }
    }
    out.push('"');
}

fn access_strategy_label(access: &ExplainAccessPath) -> String {
    struct ExplainLabelVisitor;

    impl AccessPathVisitor<String> for ExplainLabelVisitor {
        fn visit_by_key(&mut self, _key: &Value) -> String {
            "ByKey".to_string()
        }

        fn visit_by_keys(&mut self, _keys: &[Value]) -> String {
            "ByKeys".to_string()
        }

        fn visit_key_range(&mut self, _start: &Value, _end: &Value) -> String {
            "KeyRange".to_string()
        }

        fn visit_index_prefix(
            &mut self,
            name: &'static str,
            _fields: &[&'static str],
            _prefix_len: usize,
            _values: &[Value],
        ) -> String {
            format!("IndexPrefix({name})")
        }

        fn visit_index_multi_lookup(
            &mut self,
            name: &'static str,
            _fields: &[&'static str],
            _values: &[Value],
        ) -> String {
            format!("IndexMultiLookup({name})")
        }

        fn visit_index_range(
            &mut self,
            name: &'static str,
            _fields: &[&'static str],
            _prefix_len: usize,
            _prefix: &[Value],
            _lower: &Bound<Value>,
            _upper: &Bound<Value>,
        ) -> String {
            format!("IndexRange({name})")
        }

        fn visit_full_scan(&mut self) -> String {
            "FullScan".to_string()
        }

        fn visit_union(&mut self, children: &[ExplainAccessPath]) -> String {
            format!("Union({})", children.len())
        }

        fn visit_intersection(&mut self, children: &[ExplainAccessPath]) -> String {
            format!("Intersection({})", children.len())
        }
    }

    let mut visitor = ExplainLabelVisitor;
    visit_explain_access_path(access, &mut visitor)
}

const fn ordering_source_label(ordering_source: ExplainExecutionOrderingSource) -> &'static str {
    match ordering_source {
        ExplainExecutionOrderingSource::AccessOrder => "AccessOrder",
        ExplainExecutionOrderingSource::Materialized => "Materialized",
        ExplainExecutionOrderingSource::IndexSeekFirst { .. } => "IndexSeekFirst",
        ExplainExecutionOrderingSource::IndexSeekLast { .. } => "IndexSeekLast",
    }
}

impl ExplainPlan {
    /// Return the canonical predicate model used for hashing/fingerprints.
    ///
    /// The explain projection must remain a faithful rendering of this model.
    #[must_use]
    pub(crate) fn predicate_model_for_hash(&self) -> Option<&Predicate> {
        if let Some(predicate) = &self.predicate_model {
            debug_assert_eq!(
                self.predicate,
                ExplainPredicate::from_predicate(predicate),
                "explain predicate surface drifted from canonical predicate model"
            );
            Some(predicate)
        } else {
            debug_assert!(
                matches!(self.predicate, ExplainPredicate::None),
                "missing canonical predicate model requires ExplainPredicate::None"
            );
            None
        }
    }
}

///
/// ExplainGrouping
///
/// Grouped-shape annotation for deterministic explain/fingerprint surfaces.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainGrouping {
    None,
    Grouped {
        strategy: ExplainGroupedStrategy,
        group_fields: Vec<ExplainGroupField>,
        aggregates: Vec<ExplainGroupAggregate>,
        having: Option<ExplainGroupHaving>,
        max_groups: u64,
        max_group_bytes: u64,
    },
}

///
/// ExplainGroupedStrategy
///
/// Deterministic explain projection of grouped strategy selection.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExplainGroupedStrategy {
    HashGroup,
    OrderedGroup,
}

impl From<GroupedPlanStrategyHint> for ExplainGroupedStrategy {
    fn from(value: GroupedPlanStrategyHint) -> Self {
        match value {
            GroupedPlanStrategyHint::HashGroup => Self::HashGroup,
            GroupedPlanStrategyHint::OrderedGroup => Self::OrderedGroup,
        }
    }
}

///
/// ExplainGroupField
///
/// Stable grouped-key field identity carried by explain/hash surfaces.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainGroupField {
    pub(crate) slot_index: usize,
    pub(crate) field: String,
}

impl ExplainGroupField {
    /// Return grouped slot index.
    #[must_use]
    pub const fn slot_index(&self) -> usize {
        self.slot_index
    }

    /// Borrow grouped field name.
    #[must_use]
    pub const fn field(&self) -> &str {
        self.field.as_str()
    }
}

///
/// ExplainGroupAggregate
///
/// Stable explain-surface projection of one grouped aggregate terminal.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainGroupAggregate {
    pub(crate) kind: AggregateKind,
    pub(crate) target_field: Option<String>,
    pub(crate) distinct: bool,
}

impl ExplainGroupAggregate {
    /// Return grouped aggregate kind.
    #[must_use]
    pub const fn kind(&self) -> AggregateKind {
        self.kind
    }

    /// Borrow optional grouped aggregate target field.
    #[must_use]
    pub fn target_field(&self) -> Option<&str> {
        self.target_field.as_deref()
    }

    /// Return whether grouped aggregate uses DISTINCT input semantics.
    #[must_use]
    pub const fn distinct(&self) -> bool {
        self.distinct
    }
}

///
/// ExplainGroupHaving
///
/// Deterministic explain projection of grouped HAVING clauses.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainGroupHaving {
    pub(crate) clauses: Vec<ExplainGroupHavingClause>,
}

impl ExplainGroupHaving {
    /// Borrow grouped HAVING clauses.
    #[must_use]
    pub const fn clauses(&self) -> &[ExplainGroupHavingClause] {
        self.clauses.as_slice()
    }
}

///
/// ExplainGroupHavingClause
///
/// Stable explain-surface projection for one grouped HAVING clause.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainGroupHavingClause {
    pub(crate) symbol: ExplainGroupHavingSymbol,
    pub(crate) op: CompareOp,
    pub(crate) value: Value,
}

impl ExplainGroupHavingClause {
    /// Borrow grouped HAVING symbol.
    #[must_use]
    pub const fn symbol(&self) -> &ExplainGroupHavingSymbol {
        &self.symbol
    }

    /// Return grouped HAVING comparison operator.
    #[must_use]
    pub const fn op(&self) -> CompareOp {
        self.op
    }

    /// Borrow grouped HAVING literal value.
    #[must_use]
    pub const fn value(&self) -> &Value {
        &self.value
    }
}

///
/// ExplainGroupHavingSymbol
///
/// Stable explain-surface identity for grouped HAVING symbols.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainGroupHavingSymbol {
    GroupField { slot_index: usize, field: String },
    AggregateIndex { index: usize },
}

///
/// ExplainOrderPushdown
///
/// Deterministic ORDER BY pushdown eligibility reported by explain.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainOrderPushdown {
    MissingModelContext,
    EligibleSecondaryIndex {
        index: &'static str,
        prefix_len: usize,
    },
    Rejected(SecondaryOrderPushdownRejection),
}

///
/// ExplainAccessPath
///
/// Deterministic projection of logical access path shape for diagnostics.
/// Mirrors planner-selected structural paths without runtime cursor state.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainAccessPath {
    ByKey {
        key: Value,
    },
    ByKeys {
        keys: Vec<Value>,
    },
    KeyRange {
        start: Value,
        end: Value,
    },
    IndexPrefix {
        name: &'static str,
        fields: Vec<&'static str>,
        prefix_len: usize,
        values: Vec<Value>,
    },
    IndexMultiLookup {
        name: &'static str,
        fields: Vec<&'static str>,
        values: Vec<Value>,
    },
    IndexRange {
        name: &'static str,
        fields: Vec<&'static str>,
        prefix_len: usize,
        prefix: Vec<Value>,
        lower: Bound<Value>,
        upper: Bound<Value>,
    },
    FullScan,
    Union(Vec<Self>),
    Intersection(Vec<Self>),
}

///
/// ExplainPredicate
///
/// Deterministic projection of canonical predicate structure for explain output.
/// This preserves normalized predicate shape used by hashing/fingerprints.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainPredicate {
    None,
    True,
    False,
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Compare {
        field: String,
        op: CompareOp,
        value: Value,
        coercion: CoercionSpec,
    },
    IsNull {
        field: String,
    },
    IsMissing {
        field: String,
    },
    IsEmpty {
        field: String,
    },
    IsNotEmpty {
        field: String,
    },
    TextContains {
        field: String,
        value: Value,
    },
    TextContainsCi {
        field: String,
        value: Value,
    },
}

///
/// ExplainOrderBy
///
/// Deterministic projection of canonical ORDER BY shape.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainOrderBy {
    None,
    Fields(Vec<ExplainOrder>),
}

///
/// ExplainOrder
///
/// One canonical ORDER BY field + direction pair.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainOrder {
    pub(crate) field: String,
    pub(crate) direction: OrderDirection,
}

impl ExplainOrder {
    /// Borrow ORDER BY field name.
    #[must_use]
    pub const fn field(&self) -> &str {
        self.field.as_str()
    }

    /// Return ORDER BY direction.
    #[must_use]
    pub const fn direction(&self) -> OrderDirection {
        self.direction
    }
}

///
/// ExplainPagination
///
/// Explain-surface projection of pagination window configuration.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainPagination {
    None,
    Page { limit: Option<u32>, offset: u32 },
}

///
/// ExplainDeleteLimit
///
/// Explain-surface projection of delete-limit configuration.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainDeleteLimit {
    None,
    Limit { max_rows: u32 },
}

impl<K> AccessPlannedQuery<K>
where
    K: FieldValue,
{
    /// Produce a stable, deterministic explanation of this logical plan.
    #[must_use]
    pub(crate) fn explain(&self) -> ExplainPlan {
        self.explain_inner(None)
    }

    /// Produce a stable, deterministic explanation of this logical plan
    /// with optional model context for query-layer projections.
    ///
    /// Query explain intentionally does not evaluate executor route pushdown
    /// feasibility to keep query-layer dependencies executor-agnostic.
    #[must_use]
    pub(crate) fn explain_with_model(&self, model: &EntityModel) -> ExplainPlan {
        self.explain_inner(Some(model))
    }

    fn explain_inner(&self, model: Option<&EntityModel>) -> ExplainPlan {
        // Phase 1: project logical plan variant into scalar core + grouped metadata.
        let (logical, grouping) = match &self.logical {
            LogicalPlan::Scalar(logical) => (logical, ExplainGrouping::None),
            LogicalPlan::Grouped(logical) => (
                &logical.scalar,
                ExplainGrouping::Grouped {
                    strategy: grouped_plan_strategy_hint_for_plan(self)
                        .map_or(ExplainGroupedStrategy::HashGroup, Into::into),
                    group_fields: logical
                        .group
                        .group_fields
                        .iter()
                        .map(|field_slot| ExplainGroupField {
                            slot_index: field_slot.index(),
                            field: field_slot.field().to_string(),
                        })
                        .collect(),
                    aggregates: logical
                        .group
                        .aggregates
                        .iter()
                        .map(|aggregate| ExplainGroupAggregate {
                            kind: aggregate.kind,
                            target_field: aggregate.target_field.clone(),
                            distinct: aggregate.distinct,
                        })
                        .collect(),
                    having: explain_group_having(logical.having.as_ref()),
                    max_groups: logical.group.execution.max_groups(),
                    max_group_bytes: logical.group.execution.max_group_bytes(),
                },
            ),
        };

        // Phase 2: project scalar plan + access path into deterministic explain surface.
        explain_scalar_inner(logical, grouping, model, &self.access)
    }
}

fn explain_group_having(having: Option<&GroupHavingSpec>) -> Option<ExplainGroupHaving> {
    let having = having?;

    Some(ExplainGroupHaving {
        clauses: having
            .clauses()
            .iter()
            .map(explain_group_having_clause)
            .collect(),
    })
}

fn explain_group_having_clause(clause: &GroupHavingClause) -> ExplainGroupHavingClause {
    ExplainGroupHavingClause {
        symbol: explain_group_having_symbol(clause.symbol()),
        op: clause.op(),
        value: clause.value().clone(),
    }
}

fn explain_group_having_symbol(symbol: &GroupHavingSymbol) -> ExplainGroupHavingSymbol {
    match symbol {
        GroupHavingSymbol::GroupField(field_slot) => ExplainGroupHavingSymbol::GroupField {
            slot_index: field_slot.index(),
            field: field_slot.field().to_string(),
        },
        GroupHavingSymbol::AggregateIndex(index) => {
            ExplainGroupHavingSymbol::AggregateIndex { index: *index }
        }
    }
}

fn explain_scalar_inner<K>(
    logical: &ScalarPlan,
    grouping: ExplainGrouping,
    model: Option<&EntityModel>,
    access: &AccessPlan<K>,
) -> ExplainPlan
where
    K: FieldValue,
{
    // Phase 1: derive canonical predicate projection from normalized predicate model.
    let predicate_model = logical.predicate.as_ref().map(normalize);
    let predicate = match &predicate_model {
        Some(predicate) => ExplainPredicate::from_predicate(predicate),
        None => ExplainPredicate::None,
    };

    // Phase 2: project scalar-plan fields into explain-specific enums.
    let order_by = explain_order(logical.order.as_ref());
    let order_pushdown = explain_order_pushdown(model);
    let page = explain_page(logical.page.as_ref());
    let delete_limit = explain_delete_limit(logical.delete_limit.as_ref());

    // Phase 3: assemble one stable explain payload.
    ExplainPlan {
        mode: logical.mode,
        access: ExplainAccessPath::from_access_plan(access),
        predicate,
        predicate_model,
        order_by,
        distinct: logical.distinct,
        grouping,
        order_pushdown,
        page,
        delete_limit,
        consistency: logical.consistency,
    }
}

const fn explain_order_pushdown(model: Option<&EntityModel>) -> ExplainOrderPushdown {
    let _ = model;

    // Query explain does not own physical pushdown feasibility routing.
    ExplainOrderPushdown::MissingModelContext
}

impl From<SecondaryOrderPushdownEligibility> for ExplainOrderPushdown {
    fn from(value: SecondaryOrderPushdownEligibility) -> Self {
        Self::from(PushdownSurfaceEligibility::from(&value))
    }
}

impl From<PushdownSurfaceEligibility<'_>> for ExplainOrderPushdown {
    fn from(value: PushdownSurfaceEligibility<'_>) -> Self {
        match value {
            PushdownSurfaceEligibility::EligibleSecondaryIndex { index, prefix_len } => {
                Self::EligibleSecondaryIndex { index, prefix_len }
            }
            PushdownSurfaceEligibility::Rejected { reason } => Self::Rejected(reason.clone()),
        }
    }
}

struct ExplainAccessProjection;

impl<K> AccessPlanProjection<K> for ExplainAccessProjection
where
    K: FieldValue,
{
    type Output = ExplainAccessPath;

    fn by_key(&mut self, key: &K) -> Self::Output {
        ExplainAccessPath::ByKey {
            key: key.to_value(),
        }
    }

    fn by_keys(&mut self, keys: &[K]) -> Self::Output {
        ExplainAccessPath::ByKeys {
            keys: keys.iter().map(FieldValue::to_value).collect(),
        }
    }

    fn key_range(&mut self, start: &K, end: &K) -> Self::Output {
        ExplainAccessPath::KeyRange {
            start: start.to_value(),
            end: end.to_value(),
        }
    }

    fn index_prefix(
        &mut self,
        index_name: &'static str,
        index_fields: &[&'static str],
        prefix_len: usize,
        values: &[Value],
    ) -> Self::Output {
        ExplainAccessPath::IndexPrefix {
            name: index_name,
            fields: index_fields.to_vec(),
            prefix_len,
            values: values.to_vec(),
        }
    }

    fn index_multi_lookup(
        &mut self,
        index_name: &'static str,
        index_fields: &[&'static str],
        values: &[Value],
    ) -> Self::Output {
        ExplainAccessPath::IndexMultiLookup {
            name: index_name,
            fields: index_fields.to_vec(),
            values: values.to_vec(),
        }
    }

    fn index_range(
        &mut self,
        index_name: &'static str,
        index_fields: &[&'static str],
        prefix_len: usize,
        prefix: &[Value],
        lower: &Bound<Value>,
        upper: &Bound<Value>,
    ) -> Self::Output {
        ExplainAccessPath::IndexRange {
            name: index_name,
            fields: index_fields.to_vec(),
            prefix_len,
            prefix: prefix.to_vec(),
            lower: lower.clone(),
            upper: upper.clone(),
        }
    }

    fn full_scan(&mut self) -> Self::Output {
        ExplainAccessPath::FullScan
    }

    fn union(&mut self, children: Vec<Self::Output>) -> Self::Output {
        ExplainAccessPath::Union(children)
    }

    fn intersection(&mut self, children: Vec<Self::Output>) -> Self::Output {
        ExplainAccessPath::Intersection(children)
    }
}

impl ExplainAccessPath {
    pub(in crate::db) fn from_access_plan<K>(access: &AccessPlan<K>) -> Self
    where
        K: FieldValue,
    {
        let mut projection = ExplainAccessProjection;
        project_access_plan(access, &mut projection)
    }
}

impl ExplainPredicate {
    fn from_predicate(predicate: &Predicate) -> Self {
        match predicate {
            Predicate::True => Self::True,
            Predicate::False => Self::False,
            Predicate::And(children) => {
                Self::And(children.iter().map(Self::from_predicate).collect())
            }
            Predicate::Or(children) => {
                Self::Or(children.iter().map(Self::from_predicate).collect())
            }
            Predicate::Not(inner) => Self::Not(Box::new(Self::from_predicate(inner))),
            Predicate::Compare(compare) => Self::from_compare(compare),
            Predicate::IsNull { field } => Self::IsNull {
                field: field.clone(),
            },
            Predicate::IsMissing { field } => Self::IsMissing {
                field: field.clone(),
            },
            Predicate::IsEmpty { field } => Self::IsEmpty {
                field: field.clone(),
            },
            Predicate::IsNotEmpty { field } => Self::IsNotEmpty {
                field: field.clone(),
            },
            Predicate::TextContains { field, value } => Self::TextContains {
                field: field.clone(),
                value: value.clone(),
            },
            Predicate::TextContainsCi { field, value } => Self::TextContainsCi {
                field: field.clone(),
                value: value.clone(),
            },
        }
    }

    fn from_compare(compare: &ComparePredicate) -> Self {
        Self::Compare {
            field: compare.field.clone(),
            op: compare.op,
            value: compare.value.clone(),
            coercion: compare.coercion.clone(),
        }
    }
}

fn explain_order(order: Option<&OrderSpec>) -> ExplainOrderBy {
    let Some(order) = order else {
        return ExplainOrderBy::None;
    };

    if order.fields.is_empty() {
        return ExplainOrderBy::None;
    }

    ExplainOrderBy::Fields(
        order
            .fields
            .iter()
            .map(|(field, direction)| ExplainOrder {
                field: field.clone(),
                direction: *direction,
            })
            .collect(),
    )
}

const fn explain_page(page: Option<&PageSpec>) -> ExplainPagination {
    match page {
        Some(page) => ExplainPagination::Page {
            limit: page.limit,
            offset: page.offset,
        },
        None => ExplainPagination::None,
    }
}

const fn explain_delete_limit(limit: Option<&DeleteLimitSpec>) -> ExplainDeleteLimit {
    match limit {
        Some(limit) => ExplainDeleteLimit::Limit {
            max_rows: limit.max_rows,
        },
        None => ExplainDeleteLimit::None,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
