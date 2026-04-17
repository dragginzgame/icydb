//! Module: query::explain::plan
//! Responsibility: deterministic logical-plan projection for EXPLAIN.
//! Does not own: execution descriptor rendering or access visitor adapters.
//! Boundary: logical explain DTOs and plan-side projection logic.

use crate::{
    db::{
        access::{
            AccessPlan, PushdownSurfaceEligibility, SecondaryOrderPushdownEligibility,
            SecondaryOrderPushdownRejection,
        },
        predicate::{CoercionSpec, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
        query::{
            builder::scalar_projection::render_scalar_projection_expr_sql_label,
            explain::{access_projection::write_access_json, writer::JsonWriter},
            plan::{
                AccessPlannedQuery, AggregateKind, DeleteLimitSpec, GroupHavingExpr,
                GroupHavingValueExpr, GroupedPlanFallbackReason, LogicalPlan, OrderDirection,
                OrderSpec, PageSpec, QueryMode, ScalarPlan, grouped_plan_strategy,
            },
        },
    },
    traits::FieldValue,
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

#[expect(clippy::large_enum_variant)]
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
    pub(crate) input_expr: Option<String>,
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

    /// Borrow optional grouped aggregate input expression label.
    #[must_use]
    pub fn input_expr(&self) -> Option<&str> {
        self.input_expr.as_deref()
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
    pub(crate) expr: ExplainGroupHavingExpr,
}

impl ExplainGroupHaving {
    /// Borrow widened grouped HAVING expression.
    #[must_use]
    pub const fn expr(&self) -> &ExplainGroupHavingExpr {
        &self.expr
    }
}

///
/// ExplainGroupHavingExpr
///
/// Stable explain-surface projection for widened grouped HAVING boolean
/// expressions.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainGroupHavingExpr {
    Compare {
        left: ExplainGroupHavingValueExpr,
        op: CompareOp,
        right: ExplainGroupHavingValueExpr,
    },
    And(Vec<Self>),
}

///
/// ExplainGroupHavingValueExpr
///
/// Stable explain-surface projection for grouped HAVING value expressions.
/// Leaves remain restricted to grouped keys, aggregate outputs, and literals.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainGroupHavingValueExpr {
    GroupField {
        slot_index: usize,
        field: String,
    },
    AggregateIndex {
        index: usize,
    },
    Literal(Value),
    FunctionCall {
        function: String,
        args: Vec<Self>,
    },
    Unary {
        op: String,
        expr: Box<Self>,
    },
    Case {
        when_then_arms: Vec<ExplainGroupHavingCaseArm>,
        else_expr: Box<Self>,
    },
    Binary {
        op: String,
        left: Box<Self>,
        right: Box<Self>,
    },
}

///
/// ExplainGroupHavingCaseArm
///
/// Stable explain-surface projection for one grouped HAVING searched-CASE arm.
/// This keeps explain output aligned with the planner-owned grouped HAVING
/// expression seam when searched CASE support is admitted through SQL.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainGroupHavingCaseArm {
    pub(crate) condition: ExplainGroupHavingValueExpr,
    pub(crate) result: ExplainGroupHavingValueExpr,
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
    Window { limit: Option<u32>, offset: u32 },
}

impl AccessPlannedQuery {
    /// Produce a stable, deterministic explanation of this logical plan.
    #[must_use]
    pub(crate) fn explain(&self) -> ExplainPlan {
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
                                target_field: aggregate.target_field.clone(),
                                input_expr: aggregate
                                    .input_expr()
                                    .map(render_scalar_projection_expr_sql_label),
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
        expr: explain_group_having_expr(expr.as_ref()),
    })
}

fn explain_group_having_expr(expr: &GroupHavingExpr) -> ExplainGroupHavingExpr {
    match expr {
        GroupHavingExpr::Compare { left, op, right } => ExplainGroupHavingExpr::Compare {
            left: explain_group_having_value_expr(left),
            op: *op,
            right: explain_group_having_value_expr(right),
        },
        GroupHavingExpr::And(children) => {
            ExplainGroupHavingExpr::And(children.iter().map(explain_group_having_expr).collect())
        }
    }
}

fn explain_group_having_value_expr(expr: &GroupHavingValueExpr) -> ExplainGroupHavingValueExpr {
    match expr {
        GroupHavingValueExpr::GroupField(field_slot) => ExplainGroupHavingValueExpr::GroupField {
            slot_index: field_slot.index(),
            field: field_slot.field().to_string(),
        },
        GroupHavingValueExpr::AggregateIndex(index) => {
            ExplainGroupHavingValueExpr::AggregateIndex { index: *index }
        }
        GroupHavingValueExpr::Literal(value) => ExplainGroupHavingValueExpr::Literal(value.clone()),
        GroupHavingValueExpr::FunctionCall { function, args } => {
            ExplainGroupHavingValueExpr::FunctionCall {
                function: function.sql_label().to_string(),
                args: args.iter().map(explain_group_having_value_expr).collect(),
            }
        }
        GroupHavingValueExpr::Unary { op, expr } => ExplainGroupHavingValueExpr::Unary {
            op: explain_group_having_unary_op_label(*op).to_string(),
            expr: Box::new(explain_group_having_value_expr(expr)),
        },
        GroupHavingValueExpr::Case {
            when_then_arms,
            else_expr,
        } => ExplainGroupHavingValueExpr::Case {
            when_then_arms: when_then_arms
                .iter()
                .map(|arm| ExplainGroupHavingCaseArm {
                    condition: explain_group_having_value_expr(arm.condition()),
                    result: explain_group_having_value_expr(arm.result()),
                })
                .collect(),
            else_expr: Box::new(explain_group_having_value_expr(else_expr)),
        },
        GroupHavingValueExpr::Binary { op, left, right } => ExplainGroupHavingValueExpr::Binary {
            op: explain_group_having_binary_op_label(*op).to_string(),
            left: Box::new(explain_group_having_value_expr(left)),
            right: Box::new(explain_group_having_value_expr(right)),
        },
    }
}

const fn explain_group_having_unary_op_label(
    op: crate::db::query::plan::expr::UnaryOp,
) -> &'static str {
    match op {
        crate::db::query::plan::expr::UnaryOp::Not => "NOT",
    }
}

const fn explain_group_having_binary_op_label(
    op: crate::db::query::plan::expr::BinaryOp,
) -> &'static str {
    match op {
        crate::db::query::plan::expr::BinaryOp::Or => "OR",
        crate::db::query::plan::expr::BinaryOp::And => "AND",
        crate::db::query::plan::expr::BinaryOp::Eq => "=",
        crate::db::query::plan::expr::BinaryOp::Ne => "!=",
        crate::db::query::plan::expr::BinaryOp::Lt => "<",
        crate::db::query::plan::expr::BinaryOp::Lte => "<=",
        crate::db::query::plan::expr::BinaryOp::Gt => ">",
        crate::db::query::plan::expr::BinaryOp::Gte => ">=",
        crate::db::query::plan::expr::BinaryOp::Add => "+",
        crate::db::query::plan::expr::BinaryOp::Sub => "-",
        crate::db::query::plan::expr::BinaryOp::Mul => "*",
        crate::db::query::plan::expr::BinaryOp::Div => "/",
    }
}

fn explain_scalar_inner<K>(
    logical: &ScalarPlan,
    grouping: ExplainGrouping,
    access: &AccessPlan<K>,
) -> ExplainPlan
where
    K: FieldValue,
{
    // Phase 1: consume canonical predicate model from planner-owned scalar semantics.
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

const fn explain_order_pushdown() -> ExplainOrderPushdown {
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
