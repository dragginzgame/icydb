//! Module: query::explain::plan
//! Responsibility: deterministic planned-query projection for EXPLAIN,
//! including logical shape, access shape, and pushdown observability.
//! Does not own: execution descriptor rendering or access visitor adapters.
//! Boundary: explain DTOs and plan-side projection logic for query observability.

use crate::{
    db::{
        access::AccessPlan,
        predicate::{CoercionSpec, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
        query::{
            builder::scalar_projection::render_scalar_projection_expr_plan_label,
            explain::{
                access_projection::write_access_json, explain_access_plan, writer::JsonWriter,
            },
            plan::{
                AccessPlannedQuery, AggregateKind, DeleteLimitSpec, GroupedPlanFallbackReason,
                LogicalPlan, OrderDirection, OrderSpec, PageSpec, QueryMode, ScalarPlan,
                expr::Expr, grouped_plan_strategy, render_scalar_filter_expr_plan_label,
            },
        },
    },
    traits::KeyValueCodec,
    value::Value,
};
use std::ops::Bound;

///
/// ExplainPlan
///
/// Stable, deterministic representation of a planned query for observability.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainPlan {
    pub(in crate::db) mode: QueryMode,
    pub(in crate::db) access: ExplainAccessPath,
    pub(in crate::db) filter_expr: Option<String>,
    filter_expr_model: Option<Expr>,
    pub(in crate::db) predicate: ExplainPredicate,
    predicate_model: Option<Predicate>,
    pub(in crate::db) order_by: ExplainOrderBy,
    pub(in crate::db) distinct: bool,
    pub(in crate::db) grouping: ExplainGrouping,
    pub(in crate::db) order_pushdown: ExplainOrderPushdown,
    pub(in crate::db) page: ExplainPagination,
    pub(in crate::db) delete_limit: ExplainDeleteLimit,
    pub(in crate::db) consistency: MissingRowPolicy,
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

    /// Borrow projected semantic scalar filter expression when present.
    #[must_use]
    pub fn filter_expr(&self) -> Option<&str> {
        self.filter_expr.as_deref()
    }

    /// Borrow the canonical scalar filter model used for identity hashing.
    #[must_use]
    pub(in crate::db::query) fn filter_expr_model_for_hash(&self) -> Option<&Expr> {
        if let Some(filter_expr_model) = &self.filter_expr_model {
            debug_assert_eq!(
                self.filter_expr(),
                Some(render_scalar_filter_expr_plan_label(filter_expr_model).as_str()),
                "explain scalar filter label drifted from canonical filter model"
            );
            Some(filter_expr_model)
        } else {
            debug_assert!(
                self.filter_expr.is_none(),
                "missing canonical filter model requires filter_expr=None"
            );
            None
        }
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

impl ExplainPlan {
    /// Return the canonical predicate model used as the fallback hash surface.
    ///
    /// When a semantic scalar `filter_expr` exists, hashing now prefers that
    /// canonical filter surface instead. The explain predicate projection must
    /// still remain a faithful rendering of this fallback model.
    #[must_use]
    pub(in crate::db::query) fn predicate_model_for_hash(&self) -> Option<&Predicate> {
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

    /// Render this logical explain plan as deterministic canonical text.
    ///
    /// This surface is frontend-facing and intentionally stable for SQL/CLI
    /// explain output and snapshot-style diagnostics.
    #[must_use]
    pub fn render_text_canonical(&self) -> String {
        format!(
            concat!(
                "mode={:?}\n",
                "access={:?}\n",
                "filter_expr={:?}\n",
                "predicate={:?}\n",
                "order_by={:?}\n",
                "distinct={}\n",
                "grouping={:?}\n",
                "order_pushdown={:?}\n",
                "page={:?}\n",
                "delete_limit={:?}\n",
                "consistency={:?}",
            ),
            self.mode(),
            self.access(),
            self.filter_expr(),
            self.predicate(),
            self.order_by(),
            self.distinct(),
            self.grouping(),
            self.order_pushdown(),
            self.page(),
            self.delete_limit(),
            self.consistency(),
        )
    }

    /// Render this logical explain plan as canonical JSON.
    #[must_use]
    pub fn render_json_canonical(&self) -> String {
        let mut out = String::new();
        write_logical_explain_json(self, &mut out);

        out
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
        strategy: &'static str,
        fallback_reason: Option<&'static str>,
        group_fields: Vec<ExplainGroupField>,
        aggregates: Vec<ExplainGroupAggregate>,
        having: Option<ExplainGroupHaving>,
        max_groups: u64,
        max_group_bytes: u64,
    },
}

///
/// ExplainGroupField
///
/// Stable grouped-key field identity carried by explain/hash surfaces.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainGroupField {
    pub(in crate::db) slot_index: usize,
    pub(in crate::db) field: String,
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
    pub(in crate::db) kind: AggregateKind,
    pub(in crate::db) target_field: Option<String>,
    pub(in crate::db) input_expr: Option<String>,
    pub(in crate::db) filter_expr: Option<String>,
    pub(in crate::db) distinct: bool,
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

    /// Borrow optional grouped aggregate input expression label.
    #[must_use]
    pub fn input_expr(&self) -> Option<&str> {
        self.input_expr.as_deref()
    }

    /// Borrow optional grouped aggregate filter expression label.
    #[must_use]
    pub fn filter_expr(&self) -> Option<&str> {
        self.filter_expr.as_deref()
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
/// This surface now carries the shared planner-owned post-aggregate expression
/// directly so explain no longer keeps a second grouped HAVING AST.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainGroupHaving {
    pub(in crate::db) expr: Expr,
}

impl ExplainGroupHaving {
    /// Borrow grouped HAVING expression.
    #[must_use]
    pub(in crate::db) const fn expr(&self) -> &Expr {
        &self.expr
    }
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
/// SecondaryOrderPushdownRejection
///
/// Stable explain-surface reason why secondary-index ORDER BY pushdown was
/// rejected. Executor route planning converts its runtime route reasons into
/// this neutral query DTO before rendering explain payloads.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SecondaryOrderPushdownRejection {
    NoOrderBy,
    AccessPathNotSingleIndexPrefix,
    AccessPathIndexRangeUnsupported {
        index: &'static str,
        prefix_len: usize,
    },
    InvalidIndexPrefixBounds {
        prefix_len: usize,
        index_field_len: usize,
    },
    MissingPrimaryKeyTieBreak {
        field: String,
    },
    PrimaryKeyDirectionNotAscending {
        field: String,
    },
    MixedDirectionNotEligible {
        field: String,
    },
    OrderFieldsDoNotMatchIndex {
        index: &'static str,
        prefix_len: usize,
        expected_suffix: Vec<String>,
        expected_full: Vec<String>,
        actual: Vec<String>,
    },
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
    CompareFields {
        left_field: String,
        op: CompareOp,
        right_field: String,
        coercion: CoercionSpec,
    },
    IsNull {
        field: String,
    },
    IsNotNull {
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
    pub(in crate::db) field: String,
    pub(in crate::db) direction: OrderDirection,
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
    Window { limit: Option<u32>, offset: u32 },
}

impl AccessPlannedQuery {
    /// Produce a stable, deterministic explanation of this logical plan.
    #[must_use]
    pub(in crate::db) fn explain(&self) -> ExplainPlan {
        self.explain_inner()
    }

    pub(in crate::db::query::explain) fn explain_inner(&self) -> ExplainPlan {
        // Phase 1: project logical plan variant into scalar core + grouped metadata.
        let (logical, grouping) = match &self.logical {
            LogicalPlan::Scalar(logical) => (logical, ExplainGrouping::None),
            LogicalPlan::Grouped(logical) => {
                let grouped_strategy = grouped_plan_strategy(self).expect(
                    "grouped logical explain projection requires planner-owned grouped strategy",
                );

                (
                    &logical.scalar,
                    ExplainGrouping::Grouped {
                        strategy: grouped_strategy.code(),
                        fallback_reason: grouped_strategy
                            .fallback_reason()
                            .map(GroupedPlanFallbackReason::code),
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
                                target_field: aggregate.target_field().map(str::to_string),
                                input_expr: aggregate
                                    .input_expr()
                                    .map(render_scalar_projection_expr_plan_label),
                                filter_expr: aggregate
                                    .filter_expr()
                                    .map(render_scalar_projection_expr_plan_label),
                                distinct: aggregate.distinct,
                            })
                            .collect(),
                        having: explain_group_having(logical),
                        max_groups: logical.group.execution.max_groups(),
                        max_group_bytes: logical.group.execution.max_group_bytes(),
                    },
                )
            }
        };

        // Phase 2: project scalar plan + access path into deterministic explain surface.
        explain_scalar_inner(logical, grouping, &self.access)
    }
}

fn explain_group_having(logical: &crate::db::query::plan::GroupPlan) -> Option<ExplainGroupHaving> {
    let expr = logical.effective_having_expr()?;

    Some(ExplainGroupHaving {
        expr: expr.into_owned(),
    })
}

fn explain_scalar_inner<K>(
    logical: &ScalarPlan,
    grouping: ExplainGrouping,
    access: &AccessPlan<K>,
) -> ExplainPlan
where
    K: KeyValueCodec,
{
    // Phase 1: consume canonical predicate model from planner-owned scalar semantics.
    let filter_expr = logical
        .filter_expr
        .as_ref()
        .map(render_scalar_filter_expr_plan_label);
    let filter_expr_model = logical.filter_expr.clone();
    let predicate_model = logical.predicate.clone();
    let predicate = match &predicate_model {
        Some(predicate) => ExplainPredicate::from_predicate(predicate),
        None => ExplainPredicate::None,
    };

    // Phase 2: project scalar-plan fields into explain-specific enums.
    let order_by = explain_order(logical.order.as_ref());
    let order_pushdown = explain_order_pushdown();
    let page = explain_page(logical.page.as_ref());
    let delete_limit = explain_delete_limit(logical.delete_limit.as_ref());

    // Phase 3: assemble one stable explain payload.
    ExplainPlan {
        mode: logical.mode,
        access: explain_access_plan(access),
        filter_expr,
        filter_expr_model,
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

const fn explain_order_pushdown() -> ExplainOrderPushdown {
    // Query explain does not own physical pushdown feasibility routing.
    ExplainOrderPushdown::MissingModelContext
}

impl ExplainPredicate {
    pub(in crate::db) fn from_predicate(predicate: &Predicate) -> Self {
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
            Predicate::CompareFields(compare) => Self::CompareFields {
                left_field: compare.left_field().to_string(),
                op: compare.op(),
                right_field: compare.right_field().to_string(),
                coercion: compare.coercion().clone(),
            },
            Predicate::IsNull { field } => Self::IsNull {
                field: field.clone(),
            },
            Predicate::IsNotNull { field } => Self::IsNotNull {
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
            .map(|term| ExplainOrder {
                field: term.rendered_label(),
                direction: term.direction(),
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
        Some(limit) if limit.offset == 0 => match limit.limit {
            Some(max_rows) => ExplainDeleteLimit::Limit { max_rows },
            None => ExplainDeleteLimit::Window {
                limit: None,
                offset: 0,
            },
        },
        Some(limit) => ExplainDeleteLimit::Window {
            limit: limit.limit,
            offset: limit.offset,
        },
        None => ExplainDeleteLimit::None,
    }
}

fn write_logical_explain_json(explain: &ExplainPlan, out: &mut String) {
    let mut object = JsonWriter::begin_object(out);
    object.field_with("mode", |out| {
        let mut object = JsonWriter::begin_object(out);
        match explain.mode() {
            QueryMode::Load(spec) => {
                object.field_str("type", "Load");
                match spec.limit() {
                    Some(limit) => object.field_u64("limit", u64::from(limit)),
                    None => object.field_null("limit"),
                }
                object.field_u64("offset", u64::from(spec.offset()));
            }
            QueryMode::Delete(spec) => {
                object.field_str("type", "Delete");
                match spec.limit() {
                    Some(limit) => object.field_u64("limit", u64::from(limit)),
                    None => object.field_null("limit"),
                }
            }
        }
        object.finish();
    });
    object.field_with("access", |out| write_access_json(explain.access(), out));
    match explain.filter_expr() {
        Some(filter_expr) => object.field_str("filter_expr", filter_expr),
        None => object.field_null("filter_expr"),
    }
    object.field_value_debug("predicate", explain.predicate());
    object.field_value_debug("order_by", explain.order_by());
    object.field_bool("distinct", explain.distinct());
    object.field_value_debug("grouping", explain.grouping());
    object.field_value_debug("order_pushdown", explain.order_pushdown());
    object.field_with("page", |out| {
        let mut object = JsonWriter::begin_object(out);
        match explain.page() {
            ExplainPagination::None => {
                object.field_str("type", "None");
            }
            ExplainPagination::Page { limit, offset } => {
                object.field_str("type", "Page");
                match limit {
                    Some(limit) => object.field_u64("limit", u64::from(*limit)),
                    None => object.field_null("limit"),
                }
                object.field_u64("offset", u64::from(*offset));
            }
        }
        object.finish();
    });
    object.field_with("delete_limit", |out| {
        let mut object = JsonWriter::begin_object(out);
        match explain.delete_limit() {
            ExplainDeleteLimit::None => {
                object.field_str("type", "None");
            }
            ExplainDeleteLimit::Limit { max_rows } => {
                object.field_str("type", "Limit");
                object.field_u64("max_rows", u64::from(*max_rows));
            }
            ExplainDeleteLimit::Window { limit, offset } => {
                object.field_str("type", "Window");
                object.field_with("limit", |out| match limit {
                    Some(limit) => out.push_str(&limit.to_string()),
                    None => out.push_str("null"),
                });
                object.field_u64("offset", u64::from(*offset));
            }
        }
        object.finish();
    });
    object.field_value_debug("consistency", &explain.consistency());
    object.finish();
}
