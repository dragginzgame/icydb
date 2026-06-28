use crate::db::query::{
    builder::AggregateExpr,
    plan::{AggregateIdentity, AggregateKind, AggregateSemanticKey, FieldSlot, expr::Expr},
};
use crate::db::sql::lowering::SqlLoweringError;

///
/// AggregateTerminalSemanticKey
///
/// AggregateTerminalSemanticKey is the executable semantic key used while
/// collecting unique global aggregate terminals. It combines canonical
/// aggregate meaning with the optional filter so filtered and unfiltered
/// aggregates do not alias during projection remapping.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::sql::lowering::aggregate) struct AggregateTerminalSemanticKey {
    semantic_key: AggregateSemanticKey,
}

impl AggregateTerminalSemanticKey {
    // Build a canonical executable semantic key from a raw aggregate expression
    // without consuming it. This lets projection dedup compare aggregate meaning
    // before deciding whether to retain the first-seen lowered terminal.
    #[must_use]
    pub(in crate::db::sql::lowering::aggregate) fn from_aggregate_expr(
        aggregate_expr: &AggregateExpr,
    ) -> Self {
        Self {
            semantic_key: AggregateSemanticKey::from_aggregate_expr(aggregate_expr),
        }
    }

    // Split the semantic key into the normalized aggregate meaning and its
    // optional per-row filter expression for model-bound strategy preparation.
    pub(in crate::db::sql::lowering::aggregate) fn into_identity_and_filter(
        self,
    ) -> (AggregateIdentity, Option<Expr>) {
        self.semantic_key.into_identity_and_filter()
    }
}

///
/// PreparedAggregateTarget
///
/// PreparedAggregateTarget seals the mutually exclusive row, field, and
/// expression input shapes after model binding has resolved raw field names.
/// Strategy stores this target inside prepared aggregate semantics so executor
/// terminal construction cannot combine incompatible input representations.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::sql::lowering::aggregate) enum PreparedAggregateTarget {
    Rows,
    Field(FieldSlot),
    Expr(Expr),
}

impl PreparedAggregateTarget {
    // Borrow the field slot when this target is field-backed.
    #[cfg(any(test, feature = "sql-explain"))]
    pub(in crate::db::sql::lowering::aggregate) const fn field_slot(&self) -> Option<&FieldSlot> {
        match self {
            Self::Field(field_slot) => Some(field_slot),
            Self::Rows | Self::Expr(_) => None,
        }
    }

    // Borrow the scalar input expression when this target is expression-backed.
    #[cfg(test)]
    pub(in crate::db::sql::lowering::aggregate) const fn input_expr(&self) -> Option<&Expr> {
        match self {
            Self::Expr(input_expr) => Some(input_expr),
            Self::Rows | Self::Field(_) => None,
        }
    }

    // Convert the sealed target into the current executor terminal tuple. This
    // is the only expansion point from the semantic target into executor shape.
    pub(in crate::db::sql::lowering::aggregate) fn into_terminal_inputs(
        self,
    ) -> (Option<FieldSlot>, Option<Expr>) {
        match self {
            Self::Rows => (None, None),
            Self::Field(target_slot) => (Some(target_slot), None),
            Self::Expr(input_expr) => (None, Some(input_expr)),
        }
    }
}

///
/// PreparedAggregateSemantics
///
/// PreparedAggregateSemantics is the model-bound aggregate meaning consumed by
/// strategy. DISTINCT remains structurally present only on aggregate families
/// where it changes reducer behavior, so `MIN` and `MAX` cannot accidentally
/// carry a runtime DISTINCT bit.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::sql::lowering::aggregate) enum PreparedAggregateSemantics {
    Count {
        target: PreparedAggregateTarget,
        distinct: bool,
    },
    Sum {
        target: PreparedAggregateTarget,
        distinct: bool,
    },
    Avg {
        target: PreparedAggregateTarget,
        distinct: bool,
    },
    Min {
        target: PreparedAggregateTarget,
    },
    Max {
        target: PreparedAggregateTarget,
    },
}

impl PreparedAggregateSemantics {
    // Combine normalized aggregate kind/DISTINCT semantics with a model-bound
    // target. MIN/MAX deliberately discard the supplied DISTINCT bit.
    pub(in crate::db::sql::lowering::aggregate) fn try_from_kind_target_and_distinct(
        kind: AggregateKind,
        target: PreparedAggregateTarget,
        distinct: bool,
    ) -> Result<Self, SqlLoweringError> {
        let semantics = match kind {
            AggregateKind::Count => Self::Count { target, distinct },
            AggregateKind::Sum => Self::Sum { target, distinct },
            AggregateKind::Avg => Self::Avg { target, distinct },
            AggregateKind::Min => Self::Min { target },
            AggregateKind::Max => Self::Max { target },
            AggregateKind::Exists | AggregateKind::First | AggregateKind::Last => {
                return Err(SqlLoweringError::unsupported_global_aggregate_projection());
            }
        };

        Ok(semantics)
    }

    // Return the aggregate kind represented by this prepared semantic terminal.
    #[cfg(any(test, feature = "sql-explain"))]
    pub(in crate::db::sql::lowering::aggregate) const fn aggregate_kind(&self) -> AggregateKind {
        match self {
            Self::Count { .. } => AggregateKind::Count,
            Self::Sum { .. } => AggregateKind::Sum,
            Self::Avg { .. } => AggregateKind::Avg,
            Self::Min { .. } => AggregateKind::Min,
            Self::Max { .. } => AggregateKind::Max,
        }
    }

    // Return the observable DISTINCT behavior for this prepared semantic
    // terminal. Extrema variants cannot carry DISTINCT.
    pub(in crate::db::sql::lowering::aggregate) const fn distinct_input(&self) -> bool {
        match self {
            Self::Count { distinct, .. }
            | Self::Sum { distinct, .. }
            | Self::Avg { distinct, .. } => *distinct,
            Self::Min { .. } | Self::Max { .. } => false,
        }
    }

    // Borrow the prepared target without exposing the enum representation to
    // strategy callers.
    #[cfg(any(test, feature = "sql-explain"))]
    const fn target(&self) -> &PreparedAggregateTarget {
        match self {
            Self::Count { target, .. }
            | Self::Sum { target, .. }
            | Self::Avg { target, .. }
            | Self::Min { target }
            | Self::Max { target } => target,
        }
    }

    // Borrow the field slot when this semantic terminal is field-backed.
    #[cfg(any(test, feature = "sql-explain"))]
    pub(in crate::db::sql::lowering::aggregate) const fn target_slot(&self) -> Option<&FieldSlot> {
        self.target().field_slot()
    }

    // Borrow the expression input when this semantic terminal is expression-backed.
    #[cfg(test)]
    pub(in crate::db::sql::lowering::aggregate) const fn input_expr(&self) -> Option<&Expr> {
        self.target().input_expr()
    }

    // Move this prepared semantic terminal into executor inputs.
    pub(in crate::db::sql::lowering::aggregate) fn into_terminal_inputs(
        self,
    ) -> (Option<FieldSlot>, Option<Expr>) {
        match self {
            Self::Count { target, .. }
            | Self::Sum { target, .. }
            | Self::Avg { target, .. }
            | Self::Min { target }
            | Self::Max { target } => target.into_terminal_inputs(),
        }
    }
}
