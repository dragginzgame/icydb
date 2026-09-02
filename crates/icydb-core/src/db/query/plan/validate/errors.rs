//! Module: db::query::plan::validate::errors
//! Responsibility: own the query-plan validation error taxonomy and its
//! mapping from lower planner, cursor, and schema validation domains.
//! Does not own: the validation logic that decides which error applies.
//! Boundary: keeps query-plan validation failures under one planner-owned error surface.

use crate::db::{
    access::AccessPlanError,
    cursor::CursorPlanError,
    predicate::CompareOp,
    query::plan::{
        AggregateKind,
        expr::{BinaryOp, ExprType, Function, UnaryOp},
    },
    schema::ValidateError,
};
use icydb_diagnostic_code::{
    DiagnosticAggregateKind, DiagnosticFactTag, DiagnosticFunctionKind, DiagnosticOperatorKind,
    DiagnosticTypeFamily, MAX_PUBLIC_QUERY_FIELD_BYTES, QueryFieldRole,
};

type DiagnosticFacts = Vec<(DiagnosticFactTag, u64)>;

const fn diagnostic_index(index: usize) -> u64 {
    // IcyDB supports 32-bit Wasm and 64-bit native targets, so every `usize`
    // position is represented exactly by one `u64` fact.
    index as u64
}

const fn diagnostic_aggregate_kind(kind: AggregateKind) -> DiagnosticAggregateKind {
    match kind {
        AggregateKind::Count => DiagnosticAggregateKind::Count,
        AggregateKind::Sum => DiagnosticAggregateKind::Sum,
        AggregateKind::Avg => DiagnosticAggregateKind::Avg,
        AggregateKind::Exists => DiagnosticAggregateKind::Exists,
        AggregateKind::Min => DiagnosticAggregateKind::Min,
        AggregateKind::Max => DiagnosticAggregateKind::Max,
        AggregateKind::First => DiagnosticAggregateKind::First,
        AggregateKind::Last => DiagnosticAggregateKind::Last,
    }
}

const fn diagnostic_compare_op(op: CompareOp) -> DiagnosticOperatorKind {
    match op {
        CompareOp::Eq => DiagnosticOperatorKind::Eq,
        CompareOp::Ne => DiagnosticOperatorKind::Ne,
        CompareOp::Lt => DiagnosticOperatorKind::Lt,
        CompareOp::Lte => DiagnosticOperatorKind::Lte,
        CompareOp::Gt => DiagnosticOperatorKind::Gt,
        CompareOp::Gte => DiagnosticOperatorKind::Gte,
        CompareOp::In => DiagnosticOperatorKind::In,
        CompareOp::NotIn => DiagnosticOperatorKind::NotIn,
        CompareOp::Contains => DiagnosticOperatorKind::Contains,
        CompareOp::StartsWith => DiagnosticOperatorKind::StartsWith,
        CompareOp::EndsWith => DiagnosticOperatorKind::EndsWith,
    }
}

const fn diagnostic_unary_op(op: UnaryOp) -> DiagnosticOperatorKind {
    match op {
        UnaryOp::Not => DiagnosticOperatorKind::Not,
    }
}

const fn diagnostic_binary_op(op: BinaryOp) -> DiagnosticOperatorKind {
    match op {
        BinaryOp::Add => DiagnosticOperatorKind::Add,
        BinaryOp::And => DiagnosticOperatorKind::And,
        BinaryOp::Div => DiagnosticOperatorKind::Div,
        BinaryOp::Eq => DiagnosticOperatorKind::Eq,
        BinaryOp::Gt => DiagnosticOperatorKind::Gt,
        BinaryOp::Gte => DiagnosticOperatorKind::Gte,
        BinaryOp::Lt => DiagnosticOperatorKind::Lt,
        BinaryOp::Lte => DiagnosticOperatorKind::Lte,
        BinaryOp::Mul => DiagnosticOperatorKind::Mul,
        BinaryOp::Ne => DiagnosticOperatorKind::Ne,
        BinaryOp::Or => DiagnosticOperatorKind::Or,
        BinaryOp::Sub => DiagnosticOperatorKind::Sub,
    }
}

const fn diagnostic_function(function: Function) -> DiagnosticFunctionKind {
    match function {
        Function::Abs => DiagnosticFunctionKind::Abs,
        Function::Cbrt => DiagnosticFunctionKind::Cbrt,
        Function::Ceiling => DiagnosticFunctionKind::Ceiling,
        Function::Coalesce => DiagnosticFunctionKind::Coalesce,
        Function::CollectionContains => DiagnosticFunctionKind::CollectionContains,
        Function::Contains => DiagnosticFunctionKind::Contains,
        Function::EndsWith => DiagnosticFunctionKind::EndsWith,
        Function::Exp => DiagnosticFunctionKind::Exp,
        Function::Floor => DiagnosticFunctionKind::Floor,
        Function::InList => DiagnosticFunctionKind::InList,
        Function::IsEmpty => DiagnosticFunctionKind::IsEmpty,
        Function::IsMissing => DiagnosticFunctionKind::IsMissing,
        Function::IsNotEmpty => DiagnosticFunctionKind::IsNotEmpty,
        Function::IsNotNull => DiagnosticFunctionKind::IsNotNull,
        Function::IsNull => DiagnosticFunctionKind::IsNull,
        Function::Left => DiagnosticFunctionKind::Left,
        Function::Length => DiagnosticFunctionKind::Length,
        Function::Ln => DiagnosticFunctionKind::Ln,
        Function::Log => DiagnosticFunctionKind::Log,
        Function::Log2 => DiagnosticFunctionKind::Log2,
        Function::Log10 => DiagnosticFunctionKind::Log10,
        Function::Lower => DiagnosticFunctionKind::Lower,
        Function::Ltrim => DiagnosticFunctionKind::Ltrim,
        Function::Mod => DiagnosticFunctionKind::Mod,
        Function::NullIf => DiagnosticFunctionKind::NullIf,
        Function::OctetLength => DiagnosticFunctionKind::OctetLength,
        Function::Position => DiagnosticFunctionKind::Position,
        Function::Power => DiagnosticFunctionKind::Power,
        Function::Replace => DiagnosticFunctionKind::Replace,
        Function::Right => DiagnosticFunctionKind::Right,
        Function::Round => DiagnosticFunctionKind::Round,
        Function::Rtrim => DiagnosticFunctionKind::Rtrim,
        Function::Sign => DiagnosticFunctionKind::Sign,
        Function::Sqrt => DiagnosticFunctionKind::Sqrt,
        Function::StartsWith => DiagnosticFunctionKind::StartsWith,
        Function::Substring => DiagnosticFunctionKind::Substring,
        Function::Trim => DiagnosticFunctionKind::Trim,
        Function::Trunc => DiagnosticFunctionKind::Trunc,
        Function::Upper => DiagnosticFunctionKind::Upper,
    }
}

const fn diagnostic_expr_type_family(expr_type: &ExprType) -> DiagnosticTypeFamily {
    match expr_type {
        ExprType::Blob => DiagnosticTypeFamily::Blob,
        ExprType::Bool => DiagnosticTypeFamily::Bool,
        ExprType::Collection => DiagnosticTypeFamily::Collection,
        #[cfg(test)]
        ExprType::Null => DiagnosticTypeFamily::Null,
        ExprType::Numeric(_) => DiagnosticTypeFamily::Numeric,
        ExprType::Opaque | ExprType::U256 => DiagnosticTypeFamily::Opaque,
        ExprType::Structured => DiagnosticTypeFamily::Structured,
        ExprType::Text => DiagnosticTypeFamily::Text,
        ExprType::Unknown => DiagnosticTypeFamily::Unknown,
    }
}

///
/// PlanError
///
/// Root plan validation taxonomy split by domain axis.
/// User-shape failures are grouped under `PlanUserError`.
/// Policy/capability failures are grouped under `PlanPolicyError`.
/// Cursor continuation failures remain in `CursorPlanError`.
///

#[derive(Debug)]
pub struct PlanError {
    kind: PlanErrorKind,
    query_field: Option<QueryFieldContext>,
}

#[derive(Debug)]
pub(crate) enum PlanErrorKind {
    User(Box<PlanUserError>),

    Policy(Box<PlanPolicyError>),

    Cursor(Box<CursorPlanError>),
}

#[derive(Debug)]
struct QueryFieldContext {
    role: QueryFieldRole,
    field: String,
}

impl QueryFieldContext {
    fn new(role: QueryFieldRole, field: &str) -> Option<Self> {
        (!field.is_empty() && field.len() <= MAX_PUBLIC_QUERY_FIELD_BYTES).then(|| Self {
            role,
            field: field.to_owned(),
        })
    }
}

impl PlanError {
    /// Project retained planner context into production-safe numeric facts.
    pub(crate) fn diagnostic_facts(&self) -> DiagnosticFacts {
        match &self.kind {
            PlanErrorKind::User(error) => error.diagnostic_facts(),
            PlanErrorKind::Policy(error) => error.diagnostic_facts(),
            PlanErrorKind::Cursor(error) => error.diagnostic_facts(),
        }
    }

    /// Borrow the bounded rejected-field role and identity, when one was attached.
    #[must_use]
    pub(crate) fn query_field_context(&self) -> Option<(QueryFieldRole, &str)> {
        self.query_field
            .as_ref()
            .map(|context| (context.role, context.field.as_str()))
    }

    pub(in crate::db::query) fn attach_query_field(mut self, role: QueryFieldRole) -> Self {
        if self.query_field.is_some() {
            return self;
        }

        self.query_field = self
            .query_field_source(role)
            .and_then(|field| QueryFieldContext::new(role, field));
        self
    }

    #[cfg(feature = "sql")]
    pub(in crate::db) fn from_sql_unknown_field(
        role: QueryFieldRole,
        field: String,
    ) -> Option<Self> {
        if !matches!(
            role,
            QueryFieldRole::Predicate
                | QueryFieldRole::Projection
                | QueryFieldRole::GroupBy
                | QueryFieldRole::AggregateTarget
        ) {
            return None;
        }

        Some(Self::from(ExprPlanError::unknown_field(field)).attach_query_field(role))
    }

    fn query_field_source(&self, role: QueryFieldRole) -> Option<&str> {
        match (&self.kind, role) {
            (PlanErrorKind::User(error), QueryFieldRole::Predicate) => match error.as_ref() {
                PlanUserError::PredicateInvalid(error) => match error.as_ref() {
                    ValidateError::UnknownField { field } => Some(field),
                    _ => None,
                },
                PlanUserError::Expr(error) => match error.as_ref() {
                    ExprPlanError::UnknownField { field } => Some(field),
                    _ => None,
                },
                _ => None,
            },
            (PlanErrorKind::User(error), QueryFieldRole::Projection | QueryFieldRole::OrderBy) => {
                match error.as_ref() {
                    PlanUserError::Expr(error) => match error.as_ref() {
                        ExprPlanError::UnknownExprField { field } => Some(field),
                        ExprPlanError::UnknownField { field }
                            if role == QueryFieldRole::Projection =>
                        {
                            Some(field)
                        }
                        _ => None,
                    },
                    PlanUserError::Order(error) if role == QueryFieldRole::OrderBy => {
                        match error.as_ref() {
                            OrderPlanError::UnknownField { field, .. } => Some(field),
                            _ => None,
                        }
                    }
                    _ => None,
                }
            }
            (
                PlanErrorKind::User(error),
                QueryFieldRole::GroupBy | QueryFieldRole::Having | QueryFieldRole::AggregateTarget,
            ) => match error.as_ref() {
                PlanUserError::Group(error) => match (error.as_ref(), role) {
                    (GroupPlanError::UnknownGroupField { field, .. }, QueryFieldRole::GroupBy)
                    | (
                        GroupPlanError::HavingNonGroupFieldReference { field, .. },
                        QueryFieldRole::Having,
                    )
                    | (
                        GroupPlanError::UnknownAggregateTargetField { field, .. },
                        QueryFieldRole::AggregateTarget,
                    ) => Some(field),
                    _ => None,
                },
                PlanUserError::Expr(error) => match (error.as_ref(), role) {
                    (
                        ExprPlanError::UnknownField { field },
                        QueryFieldRole::GroupBy | QueryFieldRole::AggregateTarget,
                    ) => Some(field),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn into_kind(self) -> PlanErrorKind {
        self.kind
    }

    /// Return whether this plan error carries invalid external continuation state.
    #[must_use]
    pub(crate) fn is_invalid_continuation_cursor(&self) -> bool {
        matches!(
            &self.kind,
            PlanErrorKind::Cursor(error) if error.is_invalid_continuation_cursor()
        )
    }

    /// Return whether this plan error is the deterministic pagination policy failure.
    #[must_use]
    pub fn is_unordered_pagination(&self) -> bool {
        matches!(
            &self.kind,
            PlanErrorKind::Policy(inner)
                if matches!(
                    inner.as_ref(),
                    PlanPolicyError::Policy(policy)
                        if matches!(policy.as_ref(), PolicyPlanError::UnorderedPagination)
                )
        )
    }
}

///
/// PlanUserError
///
/// Planner user-shape validation failures independent of continuation cursors.
/// This axis intentionally excludes runtime routing/execution policy state and
/// release-gating capability decisions.
///

#[derive(Debug)]
pub enum PlanUserError {
    PredicateInvalid(Box<ValidateError>),

    Order(Box<OrderPlanError>),

    Access(Box<AccessPlanError>),

    Group(Box<GroupPlanError>),

    Expr(Box<ExprPlanError>),
}

impl PlanUserError {
    fn diagnostic_facts(&self) -> DiagnosticFacts {
        match self {
            Self::Order(error) => error.diagnostic_facts(),
            Self::Group(error) => error.diagnostic_facts(),
            Self::Expr(error) => error.diagnostic_facts(),
            Self::PredicateInvalid(_) | Self::Access(_) => Vec::new(),
        }
    }
}

///
/// PlanPolicyError
///
/// Planner policy/capability validation failures.
/// This axis captures query-shape constraints that are valid syntactically but
/// not supported by the current execution policy surface.
///

#[derive(Debug)]
pub enum PlanPolicyError {
    Policy(Box<PolicyPlanError>),

    Group(Box<GroupPlanError>),
}

impl PlanPolicyError {
    fn diagnostic_facts(&self) -> DiagnosticFacts {
        match self {
            Self::Group(error) => error.diagnostic_facts(),
            Self::Policy(_) => Vec::new(),
        }
    }
}

///
/// OrderPlanError
///
/// ORDER BY-specific validation failures.
///

#[derive(Debug)]
pub enum OrderPlanError {
    /// ORDER BY references an unknown field.
    UnknownField { term_index: usize, field: String },

    /// ORDER BY references a field that cannot be ordered.
    UnorderableField { term_index: usize },

    /// ORDER BY references the same non-primary-key field multiple times.
    DuplicateOrderField {
        first_term_index: usize,
        duplicate_term_index: usize,
    },

    /// Ordered plans must include every primary-key tie-break component.
    MissingPrimaryKeyTieBreak { primary_key_index: usize },
}

impl OrderPlanError {
    fn diagnostic_facts(&self) -> DiagnosticFacts {
        match self {
            Self::UnknownField { term_index, .. } | Self::UnorderableField { term_index } => {
                vec![(DiagnosticFactTag::TermIndex, diagnostic_index(*term_index))]
            }
            Self::DuplicateOrderField {
                first_term_index,
                duplicate_term_index,
            } => vec![
                (
                    DiagnosticFactTag::FirstTermIndex,
                    diagnostic_index(*first_term_index),
                ),
                (
                    DiagnosticFactTag::DuplicateTermIndex,
                    diagnostic_index(*duplicate_term_index),
                ),
            ],
            Self::MissingPrimaryKeyTieBreak { primary_key_index } => vec![(
                DiagnosticFactTag::ComponentIndex,
                diagnostic_index(*primary_key_index),
            )],
        }
    }

    /// Construct one unknown-field validation error.
    pub(in crate::db::query) fn unknown_field(term_index: usize, field: impl Into<String>) -> Self {
        Self::UnknownField {
            term_index,
            field: field.into(),
        }
    }

    /// Construct one unorderable-field validation error.
    pub(in crate::db::query) const fn unorderable_field(term_index: usize) -> Self {
        Self::UnorderableField { term_index }
    }

    /// Construct one duplicate non-primary-key ORDER BY field validation error.
    pub(in crate::db::query) const fn duplicate_order_field(
        first_term_index: usize,
        duplicate_term_index: usize,
    ) -> Self {
        Self::DuplicateOrderField {
            first_term_index,
            duplicate_term_index,
        }
    }

    /// Construct one missing primary-key tie-break validation error.
    pub(in crate::db::query) const fn missing_primary_key_tie_break(
        primary_key_index: usize,
    ) -> Self {
        Self::MissingPrimaryKeyTieBreak { primary_key_index }
    }
}

///
/// PolicyPlanError
///
/// Plan-shape policy failures.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PolicyPlanError {
    /// ORDER BY must specify at least one field.
    EmptyOrderSpec,

    /// Delete plans must not carry grouped query wrappers.
    DeletePlanWithGrouping,

    /// Delete plans must not carry pagination.
    DeletePlanWithPagination,

    /// Load plans must not carry delete limits.
    LoadPlanWithDeleteLimit,

    /// Ordered delete windows require an explicit ordering.
    DeleteWindowRequiresOrder,

    /// Pagination requires an explicit ordering.
    UnorderedPagination,
}

impl PolicyPlanError {
    /// Construct one empty-order-spec policy error.
    pub(in crate::db::query) const fn empty_order_spec() -> Self {
        Self::EmptyOrderSpec
    }

    /// Construct one delete-plan-with-grouping policy error.
    pub(in crate::db::query) const fn delete_plan_with_grouping() -> Self {
        Self::DeletePlanWithGrouping
    }

    /// Construct one delete-plan-with-pagination policy error.
    pub(in crate::db::query) const fn delete_plan_with_pagination() -> Self {
        Self::DeletePlanWithPagination
    }

    /// Construct one load-plan-with-delete-limit policy error.
    pub(in crate::db::query) const fn load_plan_with_delete_limit() -> Self {
        Self::LoadPlanWithDeleteLimit
    }

    /// Construct one ordered-delete-window-requires-order policy error.
    pub(in crate::db::query) const fn delete_window_requires_order() -> Self {
        Self::DeleteWindowRequiresOrder
    }

    /// Construct one unordered-pagination policy error.
    pub(in crate::db::query) const fn unordered_pagination() -> Self {
        Self::UnorderedPagination
    }
}

///
/// GroupPlanError
///
/// GROUP BY wrapper validation failures owned by query planning.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GroupPlanError {
    /// HAVING requires GROUP BY grouped plan shape.
    HavingRequiresGroupBy,

    /// Grouped validation entrypoint received a scalar logical plan.
    GroupedLogicalPlanRequired,

    /// GROUP BY requires at least one declared grouping field.
    EmptyGroupFields,

    /// Global DISTINCT aggregate shapes without GROUP BY are restricted.
    GlobalDistinctAggregateShapeUnsupported,

    /// GROUP BY requires at least one aggregate terminal.
    EmptyAggregates,

    /// GROUP BY references an unknown group field.
    UnknownGroupField {
        group_index: Option<usize>,
        field: String,
    },

    /// GROUP BY must not repeat the same resolved group slot.
    DuplicateGroupField { group_index: usize, field: String },

    /// GROUP BY does not accept DISTINCT unless adjacency eligibility is explicit.
    DistinctAdjacencyEligibilityRequired,

    /// GROUP BY ORDER BY shape must start with grouped-key prefix.
    OrderPrefixNotAlignedWithGroupKeys,

    /// GROUP BY ORDER BY expression parses but is not admissible for grouped ordering.
    OrderExpressionNotAdmissible { term: String },

    /// Aggregate ORDER BY requires an explicit LIMIT for bounded execution.
    OrderRequiresLimit,

    /// HAVING with DISTINCT is deferred until grouped DISTINCT support expands.
    DistinctHavingUnsupported,

    /// HAVING currently supports compare operators only.
    HavingUnsupportedCompareOp { index: usize, op: CompareOp },

    /// HAVING group-field symbols must reference declared grouped keys.
    HavingNonGroupFieldReference { index: usize, field: String },

    /// HAVING aggregate references must resolve to declared grouped terminals.
    HavingAggregateIndexOutOfBounds {
        index: usize,
        aggregate_index: usize,
        aggregate_count: usize,
    },

    /// DISTINCT grouped terminal kinds outside the admitted set are unsupported.
    DistinctAggregateKindUnsupported {
        index: usize,
        kind: Option<AggregateKind>,
    },

    /// DISTINCT over grouped field-target terminals is deferred with field-target support.
    DistinctAggregateFieldTargetUnsupported {
        index: usize,
        kind: AggregateKind,
        field: String,
    },

    /// Aggregate target fields must resolve in the model schema.
    UnknownAggregateTargetField { index: usize, field: String },

    /// Global DISTINCT SUM requires a numeric field target.
    GlobalDistinctSumTargetNotNumeric { index: usize, field: String },

    /// Field-target grouped terminals are not enabled in grouped execution.
    FieldTargetAggregatesUnsupported {
        index: usize,
        kind: AggregateKind,
        field: String,
    },
}

impl GroupPlanError {
    fn diagnostic_facts(&self) -> DiagnosticFacts {
        match self {
            Self::HavingUnsupportedCompareOp { index, op } => vec![
                (DiagnosticFactTag::ClauseIndex, diagnostic_index(*index)),
                (
                    DiagnosticFactTag::OperatorKind,
                    diagnostic_compare_op(*op).raw(),
                ),
            ],
            Self::HavingNonGroupFieldReference { index, .. } => {
                vec![(DiagnosticFactTag::ClauseIndex, diagnostic_index(*index))]
            }
            Self::HavingAggregateIndexOutOfBounds {
                index,
                aggregate_index,
                aggregate_count,
            } => vec![
                (DiagnosticFactTag::ClauseIndex, diagnostic_index(*index)),
                (
                    DiagnosticFactTag::AggregateIndex,
                    diagnostic_index(*aggregate_index),
                ),
                (
                    DiagnosticFactTag::ActualCount,
                    diagnostic_index(*aggregate_count),
                ),
            ],
            Self::DistinctAggregateKindUnsupported { index, kind } => {
                let mut facts = vec![(DiagnosticFactTag::AggregateIndex, diagnostic_index(*index))];
                if let Some(kind) = kind {
                    facts.push((
                        DiagnosticFactTag::AggregateKind,
                        diagnostic_aggregate_kind(*kind).raw(),
                    ));
                }
                facts
            }
            Self::DistinctAggregateFieldTargetUnsupported { index, kind, .. }
            | Self::FieldTargetAggregatesUnsupported { index, kind, .. } => vec![
                (DiagnosticFactTag::AggregateIndex, diagnostic_index(*index)),
                (
                    DiagnosticFactTag::AggregateKind,
                    diagnostic_aggregate_kind(*kind).raw(),
                ),
            ],
            Self::UnknownAggregateTargetField { index, .. } => {
                vec![(DiagnosticFactTag::AggregateIndex, diagnostic_index(*index))]
            }
            Self::UnknownGroupField {
                group_index: Some(index),
                ..
            }
            | Self::DuplicateGroupField {
                group_index: index, ..
            } => vec![(DiagnosticFactTag::GroupIndex, diagnostic_index(*index))],
            Self::GlobalDistinctSumTargetNotNumeric { index, .. } => vec![
                (DiagnosticFactTag::AggregateIndex, diagnostic_index(*index)),
                (
                    DiagnosticFactTag::AggregateKind,
                    DiagnosticAggregateKind::Sum.raw(),
                ),
            ],
            Self::HavingRequiresGroupBy
            | Self::GroupedLogicalPlanRequired
            | Self::EmptyGroupFields
            | Self::GlobalDistinctAggregateShapeUnsupported
            | Self::EmptyAggregates
            | Self::UnknownGroupField {
                group_index: None, ..
            }
            | Self::DistinctAdjacencyEligibilityRequired
            | Self::OrderPrefixNotAlignedWithGroupKeys
            | Self::OrderExpressionNotAdmissible { .. }
            | Self::OrderRequiresLimit
            | Self::DistinctHavingUnsupported => Vec::new(),
        }
    }

    /// Construct one grouped-logical-plan-required validation error.
    pub(in crate::db::query) const fn grouped_logical_plan_required() -> Self {
        Self::GroupedLogicalPlanRequired
    }

    /// Construct one unsupported global-DISTINCT aggregate shape validation error.
    pub(in crate::db::query) const fn global_distinct_aggregate_shape_unsupported() -> Self {
        Self::GlobalDistinctAggregateShapeUnsupported
    }

    /// Construct one grouped DISTINCT adjacency-eligibility-required policy error.
    pub(in crate::db::query) const fn distinct_adjacency_eligibility_required() -> Self {
        Self::DistinctAdjacencyEligibilityRequired
    }

    /// Construct one grouped DISTINCT HAVING unsupported policy error.
    pub(in crate::db::query) const fn distinct_having_unsupported() -> Self {
        Self::DistinctHavingUnsupported
    }

    /// Construct one unknown grouped-field validation error.
    pub(in crate::db::query) fn unknown_group_field(field: impl Into<String>) -> Self {
        Self::UnknownGroupField {
            group_index: None,
            field: field.into(),
        }
    }

    /// Construct one unknown grouped-field error with its declaration index.
    pub(in crate::db::query) fn unknown_group_field_at(
        group_index: usize,
        field: impl Into<String>,
    ) -> Self {
        Self::UnknownGroupField {
            group_index: Some(group_index),
            field: field.into(),
        }
    }

    /// Construct one duplicate grouped-field validation error.
    pub(in crate::db::query) fn duplicate_group_field(
        group_index: usize,
        field: impl Into<String>,
    ) -> Self {
        Self::DuplicateGroupField {
            group_index,
            field: field.into(),
        }
    }

    /// Construct one aggregate ORDER BY requires LIMIT validation error.
    pub(in crate::db::query) const fn order_requires_limit() -> Self {
        Self::OrderRequiresLimit
    }

    /// Construct one grouped ORDER BY prefix-alignment validation error.
    pub(in crate::db::query) const fn order_prefix_not_aligned_with_group_keys() -> Self {
        Self::OrderPrefixNotAlignedWithGroupKeys
    }

    /// Construct one grouped ORDER BY expression admission validation error.
    pub(in crate::db::query) fn order_expression_not_admissible(term: impl Into<String>) -> Self {
        Self::OrderExpressionNotAdmissible { term: term.into() }
    }

    /// Construct one empty grouped-field-set validation error.
    /// Construct one empty grouped-aggregate-set validation error.
    pub(in crate::db::query) const fn empty_aggregates() -> Self {
        Self::EmptyAggregates
    }

    /// Construct one grouped HAVING non-group-field reference validation error.
    pub(in crate::db::query) fn having_non_group_field_reference(
        index: usize,
        field: impl Into<String>,
    ) -> Self {
        Self::HavingNonGroupFieldReference {
            index,
            field: field.into(),
        }
    }

    /// Construct one grouped HAVING aggregate-index-out-of-bounds validation error.
    pub(in crate::db::query) const fn having_aggregate_index_out_of_bounds(
        index: usize,
        aggregate_index: usize,
        aggregate_count: usize,
    ) -> Self {
        Self::HavingAggregateIndexOutOfBounds {
            index,
            aggregate_index,
            aggregate_count,
        }
    }

    /// Construct one grouped HAVING unsupported-operator policy error.
    pub(in crate::db::query) const fn having_unsupported_compare_op(
        index: usize,
        op: CompareOp,
    ) -> Self {
        Self::HavingUnsupportedCompareOp { index, op }
    }

    /// Construct one grouped DISTINCT aggregate-kind unsupported policy error.
    pub(in crate::db::query) const fn distinct_aggregate_kind_unsupported(
        index: usize,
        kind: Option<AggregateKind>,
    ) -> Self {
        Self::DistinctAggregateKindUnsupported { index, kind }
    }

    /// Construct one grouped DISTINCT field-target unsupported policy error.
    pub(in crate::db::query) fn distinct_aggregate_field_target_unsupported(
        index: usize,
        kind: AggregateKind,
        field: impl Into<String>,
    ) -> Self {
        Self::DistinctAggregateFieldTargetUnsupported {
            index,
            kind,
            field: field.into(),
        }
    }

    /// Construct one grouped field-target aggregate unsupported policy error.
    pub(in crate::db::query) fn field_target_aggregates_unsupported(
        index: usize,
        kind: AggregateKind,
        field: impl Into<String>,
    ) -> Self {
        Self::FieldTargetAggregatesUnsupported {
            index,
            kind,
            field: field.into(),
        }
    }

    /// Construct one global DISTINCT SUM non-numeric-target policy error.
    pub(in crate::db::query) fn global_distinct_sum_target_not_numeric(
        index: usize,
        field: impl Into<String>,
    ) -> Self {
        Self::GlobalDistinctSumTargetNotNumeric {
            index,
            field: field.into(),
        }
    }

    /// Construct one unknown grouped aggregate-target-field validation error.
    pub(in crate::db::query) fn unknown_aggregate_target_field(
        index: usize,
        field: impl Into<String>,
    ) -> Self {
        Self::UnknownAggregateTargetField {
            index,
            field: field.into(),
        }
    }
}

///
/// ExprPlanError
///
/// Expression-spine inference failures owned by planner semantics.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExprPlanError {
    /// SQL lowering references a field that does not exist in schema.
    UnknownField { field: String },

    /// Expression references a field that does not exist in schema.
    UnknownExprField { field: String },

    /// Aggregate terminal requires a numeric target field.
    NonNumericAggregateTarget {
        kind: AggregateKind,
        found: DiagnosticTypeFamily,
    },

    /// Aggregate expression requires an explicit target field.
    AggregateTargetRequired { kind: AggregateKind },

    /// Function call received an unsupported argument count.
    InvalidFunctionArity {
        function: DiagnosticFunctionKind,
        expected: usize,
        actual: usize,
    },

    /// Function call received one incompatible argument type.
    InvalidFunctionArgument {
        function: DiagnosticFunctionKind,
        argument_index: usize,
        found: DiagnosticTypeFamily,
    },

    /// Function call received incompatible dynamic argument types.
    IncompatibleFunctionArguments {
        function: DiagnosticFunctionKind,
        left_argument_index: usize,
        right_argument_index: usize,
        left: DiagnosticTypeFamily,
        right: DiagnosticTypeFamily,
    },

    /// Unary operation is incompatible with inferred operand type.
    InvalidUnaryOperand {
        op: DiagnosticOperatorKind,
        found: DiagnosticTypeFamily,
    },

    /// CASE branch condition is not boolean-typed.
    InvalidCaseConditionType {
        arm_index: usize,
        found: DiagnosticTypeFamily,
    },

    /// CASE result branches cannot agree on one shared scalar type.
    IncompatibleCaseBranchTypes {
        left_branch_index: Option<usize>,
        right_branch_index: Option<usize>,
        left: DiagnosticTypeFamily,
        right: DiagnosticTypeFamily,
    },

    /// Binary operation is incompatible with inferred operand types.
    InvalidBinaryOperands {
        op: DiagnosticOperatorKind,
        left: DiagnosticTypeFamily,
        right: DiagnosticTypeFamily,
    },

    /// GROUP BY projections must not reference fields outside grouped keys.
    GroupedProjectionReferencesNonGroupField { index: usize },
}

impl ExprPlanError {
    fn diagnostic_facts(&self) -> DiagnosticFacts {
        match self {
            Self::NonNumericAggregateTarget { kind, found } => vec![
                (
                    DiagnosticFactTag::AggregateKind,
                    diagnostic_aggregate_kind(*kind).raw(),
                ),
                (DiagnosticFactTag::TypeFamily, found.raw()),
            ],
            Self::AggregateTargetRequired { kind } => vec![(
                DiagnosticFactTag::AggregateKind,
                diagnostic_aggregate_kind(*kind).raw(),
            )],
            Self::InvalidFunctionArity {
                function,
                expected,
                actual,
            } => vec![
                (DiagnosticFactTag::FunctionKind, function.raw()),
                (
                    DiagnosticFactTag::ExpectedArity,
                    diagnostic_index(*expected),
                ),
                (DiagnosticFactTag::ActualArity, diagnostic_index(*actual)),
            ],
            Self::InvalidFunctionArgument {
                function,
                argument_index,
                found,
            } => vec![
                (DiagnosticFactTag::FunctionKind, function.raw()),
                (
                    DiagnosticFactTag::ArgumentIndex,
                    diagnostic_index(*argument_index),
                ),
                (DiagnosticFactTag::TypeFamily, found.raw()),
            ],
            Self::IncompatibleFunctionArguments {
                function,
                left_argument_index,
                right_argument_index,
                left,
                right,
            } => vec![
                (DiagnosticFactTag::FunctionKind, function.raw()),
                (
                    DiagnosticFactTag::ArgumentIndex,
                    diagnostic_index(*left_argument_index),
                ),
                (DiagnosticFactTag::TypeFamily, left.raw()),
                (
                    DiagnosticFactTag::ArgumentIndex,
                    diagnostic_index(*right_argument_index),
                ),
                (DiagnosticFactTag::TypeFamily, right.raw()),
            ],
            Self::InvalidUnaryOperand { op, found } => vec![
                (DiagnosticFactTag::OperatorKind, op.raw()),
                (DiagnosticFactTag::TypeFamily, found.raw()),
            ],
            Self::InvalidCaseConditionType { arm_index, found } => vec![
                (DiagnosticFactTag::BranchIndex, diagnostic_index(*arm_index)),
                (DiagnosticFactTag::TypeFamily, found.raw()),
            ],
            Self::IncompatibleCaseBranchTypes {
                left_branch_index,
                right_branch_index,
                left,
                right,
            } => {
                let mut facts = Vec::with_capacity(4);
                if let Some(index) = left_branch_index {
                    facts.push((DiagnosticFactTag::BranchIndex, diagnostic_index(*index)));
                }
                facts.push((DiagnosticFactTag::TypeFamily, left.raw()));
                if let Some(index) = right_branch_index {
                    facts.push((DiagnosticFactTag::BranchIndex, diagnostic_index(*index)));
                }
                facts.push((DiagnosticFactTag::TypeFamily, right.raw()));
                facts
            }
            Self::InvalidBinaryOperands { op, left, right } => vec![
                (DiagnosticFactTag::OperatorKind, op.raw()),
                (DiagnosticFactTag::TypeFamily, left.raw()),
                (DiagnosticFactTag::TypeFamily, right.raw()),
            ],
            Self::GroupedProjectionReferencesNonGroupField { index } => {
                vec![(DiagnosticFactTag::ProjectionIndex, diagnostic_index(*index))]
            }
            Self::UnknownField { .. } | Self::UnknownExprField { .. } => Vec::new(),
        }
    }

    /// Construct one unknown-field planner error.
    pub(in crate::db::query) fn unknown_field(field: impl Into<String>) -> Self {
        Self::UnknownField {
            field: field.into(),
        }
    }

    /// Construct one unknown-expression-field planner error.
    pub(in crate::db::query) fn unknown_expr_field(field: impl Into<String>) -> Self {
        Self::UnknownExprField {
            field: field.into(),
        }
    }

    /// Construct one aggregate-target-required planner error.
    pub(in crate::db::query) const fn aggregate_target_required(kind: AggregateKind) -> Self {
        Self::AggregateTargetRequired { kind }
    }

    /// Construct one non-numeric aggregate-target planner error.
    pub(in crate::db::query) const fn non_numeric_aggregate_target(
        kind: AggregateKind,
        found: &ExprType,
    ) -> Self {
        Self::NonNumericAggregateTarget {
            kind,
            found: diagnostic_expr_type_family(found),
        }
    }

    /// Construct one invalid function-arity planner error.
    pub(in crate::db::query) const fn invalid_function_arity(
        function: Function,
        expected: usize,
        actual: usize,
    ) -> Self {
        Self::InvalidFunctionArity {
            function: diagnostic_function(function),
            expected,
            actual,
        }
    }

    /// Construct one invalid function-argument planner error.
    pub(in crate::db::query) const fn invalid_function_argument(
        function: Function,
        argument_index: usize,
        found: &ExprType,
    ) -> Self {
        Self::InvalidFunctionArgument {
            function: diagnostic_function(function),
            argument_index,
            found: diagnostic_expr_type_family(found),
        }
    }

    /// Construct one incompatible dynamic-function-arguments planner error.
    pub(in crate::db::query) const fn incompatible_function_arguments(
        function: Function,
        left_argument_index: usize,
        right_argument_index: usize,
        left: &ExprType,
        right: &ExprType,
    ) -> Self {
        Self::IncompatibleFunctionArguments {
            function: diagnostic_function(function),
            left_argument_index,
            right_argument_index,
            left: diagnostic_expr_type_family(left),
            right: diagnostic_expr_type_family(right),
        }
    }

    /// Construct one invalid unary-operand planner error.
    pub(in crate::db::query) const fn invalid_unary_operand(op: UnaryOp, found: &ExprType) -> Self {
        Self::InvalidUnaryOperand {
            op: diagnostic_unary_op(op),
            found: diagnostic_expr_type_family(found),
        }
    }

    /// Construct one invalid CASE-condition planner error.
    pub(in crate::db::query) const fn invalid_case_condition_type(
        arm_index: usize,
        found: &ExprType,
    ) -> Self {
        Self::InvalidCaseConditionType {
            arm_index,
            found: diagnostic_expr_type_family(found),
        }
    }

    /// Construct one incompatible CASE-branch-types planner error.
    pub(in crate::db::query) const fn incompatible_case_branch_types(
        left_branch_index: Option<usize>,
        right_branch_index: Option<usize>,
        left: &ExprType,
        right: &ExprType,
    ) -> Self {
        Self::IncompatibleCaseBranchTypes {
            left_branch_index,
            right_branch_index,
            left: diagnostic_expr_type_family(left),
            right: diagnostic_expr_type_family(right),
        }
    }

    /// Construct one invalid binary-operands planner error.
    pub(in crate::db::query) const fn invalid_binary_operands(
        op: BinaryOp,
        left: &ExprType,
        right: &ExprType,
    ) -> Self {
        Self::InvalidBinaryOperands {
            op: diagnostic_binary_op(op),
            left: diagnostic_expr_type_family(left),
            right: diagnostic_expr_type_family(right),
        }
    }

    /// Construct one grouped projection non-group-field reference planner error.
    pub(in crate::db::query) const fn grouped_projection_references_non_group_field(
        index: usize,
    ) -> Self {
        Self::GroupedProjectionReferencesNonGroupField { index }
    }
}

///
/// CursorOrderPlanShapeError
///
/// Logical cursor-order plan-shape failures used by cursor/runtime boundary adapters.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum CursorOrderPlanShapeError {
    MissingExplicitOrder,
    EmptyOrderSpec,
}

impl CursorOrderPlanShapeError {
    /// Construct one missing-explicit-order shape error.
    pub(in crate::db) const fn missing_explicit_order() -> Self {
        Self::MissingExplicitOrder
    }

    /// Construct one empty-order-spec shape error.
    pub(in crate::db) const fn empty_order_spec() -> Self {
        Self::EmptyOrderSpec
    }

    /// Map one cursor-order shape error into one cursor plan error.
    pub(in crate::db) const fn to_cursor_plan_error(self) -> CursorPlanError {
        match self {
            Self::MissingExplicitOrder => CursorPlanError::continuation_cursor_invariant(),
            Self::EmptyOrderSpec => CursorPlanError::cursor_requires_non_empty_order(),
        }
    }
}

///
/// IntentKeyAccessKind
impl From<ValidateError> for PlanError {
    fn from(err: ValidateError) -> Self {
        Self::from(PlanUserError::from(err))
    }
}

impl From<OrderPlanError> for PlanError {
    fn from(err: OrderPlanError) -> Self {
        Self::from(PlanUserError::from(err))
    }
}

impl From<AccessPlanError> for PlanError {
    fn from(err: AccessPlanError) -> Self {
        Self::from(PlanUserError::from(err))
    }
}

impl From<PolicyPlanError> for PlanError {
    fn from(err: PolicyPlanError) -> Self {
        Self::from(PlanPolicyError::from(err))
    }
}

impl From<CursorPlanError> for PlanError {
    fn from(err: CursorPlanError) -> Self {
        Self {
            kind: PlanErrorKind::Cursor(Box::new(err)),
            query_field: None,
        }
    }
}

impl From<GroupPlanError> for PlanError {
    fn from(err: GroupPlanError) -> Self {
        if err.belongs_to_policy_axis() {
            return Self::from(PlanPolicyError::from(err));
        }

        Self::from(PlanUserError::from(err))
    }
}

impl From<ExprPlanError> for PlanError {
    fn from(err: ExprPlanError) -> Self {
        Self::from(PlanUserError::from(err))
    }
}

impl From<PlanUserError> for PlanError {
    fn from(err: PlanUserError) -> Self {
        Self {
            kind: PlanErrorKind::User(Box::new(err)),
            query_field: None,
        }
    }
}

impl From<PlanPolicyError> for PlanError {
    fn from(err: PlanPolicyError) -> Self {
        Self {
            kind: PlanErrorKind::Policy(Box::new(err)),
            query_field: None,
        }
    }
}

impl From<ValidateError> for PlanUserError {
    fn from(err: ValidateError) -> Self {
        Self::PredicateInvalid(Box::new(err))
    }
}

impl From<OrderPlanError> for PlanUserError {
    fn from(err: OrderPlanError) -> Self {
        Self::Order(Box::new(err))
    }
}

impl From<AccessPlanError> for PlanUserError {
    fn from(err: AccessPlanError) -> Self {
        Self::Access(Box::new(err))
    }
}

impl From<GroupPlanError> for PlanUserError {
    fn from(err: GroupPlanError) -> Self {
        Self::Group(Box::new(err))
    }
}

impl From<ExprPlanError> for PlanUserError {
    fn from(err: ExprPlanError) -> Self {
        Self::Expr(Box::new(err))
    }
}

impl From<PolicyPlanError> for PlanPolicyError {
    fn from(err: PolicyPlanError) -> Self {
        Self::Policy(Box::new(err))
    }
}

impl From<GroupPlanError> for PlanPolicyError {
    fn from(err: GroupPlanError) -> Self {
        Self::Group(Box::new(err))
    }
}

impl GroupPlanError {
    // Group-plan variants that represent release-gating/capability constraints
    // are classified under the policy axis to keep user-shape and policy
    // domains separated at the top-level `PlanError`.
    const fn belongs_to_policy_axis(&self) -> bool {
        matches!(
            self,
            Self::GlobalDistinctAggregateShapeUnsupported
                | Self::DistinctAdjacencyEligibilityRequired
                | Self::OrderPrefixNotAlignedWithGroupKeys
                | Self::OrderExpressionNotAdmissible { .. }
                | Self::OrderRequiresLimit
                | Self::DistinctHavingUnsupported
                | Self::HavingUnsupportedCompareOp { .. }
                | Self::DistinctAggregateKindUnsupported { .. }
                | Self::DistinctAggregateFieldTargetUnsupported { .. }
                | Self::FieldTargetAggregatesUnsupported { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{ExprPlanError, GroupPlanError, OrderPlanError, PlanError, diagnostic_index};
    use crate::db::{
        QueryError, ValidateError,
        predicate::CompareOp,
        query::plan::{
            AggregateKind,
            expr::{BinaryOp, ExprType, Function, NumericSubtype},
        },
    };
    use icydb_diagnostic_code::{
        DiagnosticAggregateKind, DiagnosticFactTag, DiagnosticFunctionKind, DiagnosticOperatorKind,
        DiagnosticTypeFamily, MAX_PUBLIC_QUERY_FIELD_BYTES, QueryFieldRole,
    };

    fn attached_context(
        error: PlanError,
        role: QueryFieldRole,
    ) -> Option<(QueryFieldRole, String)> {
        error
            .attach_query_field(role)
            .query_field_context()
            .map(|(role, field)| (role, field.to_string()))
    }

    fn assert_attachment_roles(
        make_error: impl Fn() -> PlanError,
        field: &str,
        admitted_roles: &[QueryFieldRole],
    ) {
        for role in [
            QueryFieldRole::Predicate,
            QueryFieldRole::Projection,
            QueryFieldRole::GroupBy,
            QueryFieldRole::Having,
            QueryFieldRole::OrderBy,
            QueryFieldRole::AggregateTarget,
        ] {
            let expected = admitted_roles
                .contains(&role)
                .then(|| (role, field.to_string()));
            assert_eq!(attached_context(make_error(), role), expected);
        }
    }

    #[test]
    fn planner_attachment_matrix_is_closed_and_exact() {
        assert_attachment_roles(
            || {
                PlanError::from(ValidateError::UnknownField {
                    field: "predicate_field".to_string(),
                })
            },
            "predicate_field",
            &[QueryFieldRole::Predicate],
        );
        assert_attachment_roles(
            || PlanError::from(ExprPlanError::unknown_expr_field("expression_field")),
            "expression_field",
            &[QueryFieldRole::Projection, QueryFieldRole::OrderBy],
        );
        assert_attachment_roles(
            || PlanError::from(ExprPlanError::unknown_field("sql_field")),
            "sql_field",
            &[
                QueryFieldRole::Predicate,
                QueryFieldRole::Projection,
                QueryFieldRole::GroupBy,
                QueryFieldRole::AggregateTarget,
            ],
        );
        assert_attachment_roles(
            || PlanError::from(OrderPlanError::unknown_field(2, "order_field")),
            "order_field",
            &[QueryFieldRole::OrderBy],
        );
        assert_attachment_roles(
            || PlanError::from(GroupPlanError::unknown_group_field_at(1, "group_field")),
            "group_field",
            &[QueryFieldRole::GroupBy],
        );
        assert_attachment_roles(
            || {
                PlanError::from(GroupPlanError::having_non_group_field_reference(
                    3,
                    "having_field",
                ))
            },
            "having_field",
            &[QueryFieldRole::Having],
        );
        assert_attachment_roles(
            || {
                PlanError::from(GroupPlanError::unknown_aggregate_target_field(
                    4,
                    "aggregate_field",
                ))
            },
            "aggregate_field",
            &[QueryFieldRole::AggregateTarget],
        );
        assert_eq!(
            attached_context(
                PlanError::from(OrderPlanError::unorderable_field(0)),
                QueryFieldRole::OrderBy,
            ),
            None
        );
    }

    #[test]
    fn planner_context_bound_preserves_exact_utf8_or_omits_it() {
        let exact = "é".repeat(MAX_PUBLIC_QUERY_FIELD_BYTES / 2);
        let over = format!("{exact}a");
        let empty = String::new();

        assert_eq!(
            attached_context(
                PlanError::from(OrderPlanError::unknown_field(0, exact.clone())),
                QueryFieldRole::OrderBy,
            ),
            Some((QueryFieldRole::OrderBy, exact))
        );
        for field in [over, empty] {
            assert_eq!(
                attached_context(
                    PlanError::from(OrderPlanError::unknown_field(0, field)),
                    QueryFieldRole::OrderBy,
                ),
                None
            );
        }
    }

    #[cfg(feature = "sql")]
    #[test]
    fn sql_unknown_field_attachment_accepts_only_lowering_roles() {
        for role in [
            QueryFieldRole::Predicate,
            QueryFieldRole::Projection,
            QueryFieldRole::GroupBy,
            QueryFieldRole::AggregateTarget,
        ] {
            let error = PlanError::from_sql_unknown_field(role, "missing".to_string())
                .expect("lowering role should be admitted");
            assert_eq!(error.query_field_context(), Some((role, "missing")));
        }
        for role in [QueryFieldRole::Having, QueryFieldRole::OrderBy] {
            assert!(
                PlanError::from_sql_unknown_field(role, "missing".to_string()).is_none(),
                "non-lowering role {role:?} must fail closed"
            );
        }
    }

    #[test]
    fn order_diagnostics_retain_exact_term_and_component_positions() {
        assert_eq!(
            OrderPlanError::duplicate_order_field(2, 5).diagnostic_facts(),
            vec![
                (DiagnosticFactTag::FirstTermIndex, 2),
                (DiagnosticFactTag::DuplicateTermIndex, 5),
            ],
        );
        assert_eq!(
            OrderPlanError::missing_primary_key_tie_break(3).diagnostic_facts(),
            vec![(DiagnosticFactTag::ComponentIndex, 3)],
        );
        assert_eq!(diagnostic_index(usize::MAX), usize::MAX as u64);

        let query_error =
            QueryError::from(PlanError::from(OrderPlanError::unknown_field(7, "missing")));
        assert_eq!(
            query_error.diagnostic_facts(),
            vec![(DiagnosticFactTag::TermIndex, 7)],
        );
    }

    #[test]
    fn group_diagnostics_retain_clause_aggregate_count_and_kind() {
        assert_eq!(
            GroupPlanError::having_aggregate_index_out_of_bounds(1, 4, 3).diagnostic_facts(),
            vec![
                (DiagnosticFactTag::ClauseIndex, 1),
                (DiagnosticFactTag::AggregateIndex, 4),
                (DiagnosticFactTag::ActualCount, 3),
            ],
        );
        assert_eq!(
            GroupPlanError::having_unsupported_compare_op(2, CompareOp::NotIn).diagnostic_facts(),
            vec![
                (DiagnosticFactTag::ClauseIndex, 2),
                (
                    DiagnosticFactTag::OperatorKind,
                    DiagnosticOperatorKind::NotIn.raw(),
                ),
            ],
        );
        assert_eq!(
            GroupPlanError::field_target_aggregates_unsupported(
                6,
                AggregateKind::Last,
                "private-name",
            )
            .diagnostic_facts(),
            vec![
                (DiagnosticFactTag::AggregateIndex, 6),
                (
                    DiagnosticFactTag::AggregateKind,
                    DiagnosticAggregateKind::Last.raw(),
                ),
            ],
        );
        assert_eq!(
            GroupPlanError::unknown_group_field_at(3, "private-name").diagnostic_facts(),
            vec![(DiagnosticFactTag::GroupIndex, 3)],
        );
        assert_eq!(
            GroupPlanError::duplicate_group_field(4, "private-name").diagnostic_facts(),
            vec![(DiagnosticFactTag::GroupIndex, 4)],
        );
    }

    #[test]
    fn expression_diagnostics_retain_function_arity_and_argument_types() {
        assert_eq!(
            ExprPlanError::invalid_function_arity(Function::InList, 2, 3).diagnostic_facts(),
            vec![
                (
                    DiagnosticFactTag::FunctionKind,
                    DiagnosticFunctionKind::InList.raw(),
                ),
                (DiagnosticFactTag::ExpectedArity, 2),
                (DiagnosticFactTag::ActualArity, 3),
            ],
        );
        assert_eq!(
            ExprPlanError::incompatible_function_arguments(
                Function::Coalesce,
                0,
                2,
                &ExprType::Text,
                &ExprType::Numeric(NumericSubtype::Integer),
            )
            .diagnostic_facts(),
            vec![
                (
                    DiagnosticFactTag::FunctionKind,
                    DiagnosticFunctionKind::Coalesce.raw(),
                ),
                (DiagnosticFactTag::ArgumentIndex, 0),
                (
                    DiagnosticFactTag::TypeFamily,
                    DiagnosticTypeFamily::Text.raw(),
                ),
                (DiagnosticFactTag::ArgumentIndex, 2),
                (
                    DiagnosticFactTag::TypeFamily,
                    DiagnosticTypeFamily::Numeric.raw(),
                ),
            ],
        );
    }

    #[test]
    fn expression_diagnostics_retain_operator_and_branch_positions() {
        assert_eq!(
            ExprPlanError::invalid_binary_operands(
                BinaryOp::Add,
                &ExprType::Text,
                &ExprType::Bool,
            )
            .diagnostic_facts(),
            vec![
                (
                    DiagnosticFactTag::OperatorKind,
                    DiagnosticOperatorKind::Add.raw(),
                ),
                (
                    DiagnosticFactTag::TypeFamily,
                    DiagnosticTypeFamily::Text.raw(),
                ),
                (
                    DiagnosticFactTag::TypeFamily,
                    DiagnosticTypeFamily::Bool.raw(),
                ),
            ],
        );
        assert_eq!(
            ExprPlanError::incompatible_case_branch_types(
                Some(1),
                None,
                &ExprType::Blob,
                &ExprType::Structured,
            )
            .diagnostic_facts(),
            vec![
                (DiagnosticFactTag::BranchIndex, 1),
                (
                    DiagnosticFactTag::TypeFamily,
                    DiagnosticTypeFamily::Blob.raw(),
                ),
                (
                    DiagnosticFactTag::TypeFamily,
                    DiagnosticTypeFamily::Structured.raw(),
                ),
            ],
        );
    }
}
