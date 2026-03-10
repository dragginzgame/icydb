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
        predicate::{
            CoercionSpec, CompareOp, ComparePredicate, MissingRowPolicy, Predicate, normalize,
        },
        query::plan::{
            AccessPlannedQuery, AggregateKind, DeleteLimitSpec, GroupHavingClause, GroupHavingSpec,
            GroupHavingSymbol, GroupedPlanStrategyHint, LogicalPlan, OrderDirection, OrderSpec,
            PageSpec, QueryMode, ScalarPlan, grouped_plan_strategy_hint,
        },
    },
    model::entity::EntityModel,
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
    #[cfg(test)]
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
                    strategy: grouped_plan_strategy_hint(self)
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
