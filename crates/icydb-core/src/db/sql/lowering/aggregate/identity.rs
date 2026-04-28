use crate::db::{
    query::plan::{AggregateIdentity, AggregateKind, FieldSlot, expr::Expr},
    sql::lowering::aggregate::terminal::{AggregateInput, SqlGlobalAggregateTerminal},
};

fn aggregate_input_expr(input: AggregateInput) -> Option<Expr> {
    match input {
        AggregateInput::Rows => None,
        AggregateInput::Field(field) => Some(Expr::Field(
            crate::db::query::plan::expr::FieldId::new(field),
        )),
        AggregateInput::Expr(input_expr) => Some(input_expr),
    }
}

pub(in crate::db::sql::lowering::aggregate) fn aggregate_input_from_identity(
    identity: AggregateIdentity,
) -> AggregateInput {
    match identity.into_input_expr() {
        None => AggregateInput::Rows,
        Some(Expr::Field(field)) => AggregateInput::Field(field.as_str().to_string()),
        Some(input_expr) => AggregateInput::Expr(input_expr),
    }
}

fn aggregate_identity_from_terminal(terminal: &SqlGlobalAggregateTerminal) -> AggregateIdentity {
    AggregateIdentity::from_parts(
        terminal.kind,
        aggregate_input_expr(terminal.input.clone()),
        terminal.distinct,
    )
}

///
/// AggregateTerminalIdentity
///
/// AggregateTerminalIdentity is the executable identity used while collecting
/// unique global aggregate terminals. It combines canonical aggregate
/// identity with the optional filter so filtered and unfiltered aggregates do
/// not alias during projection remapping.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::sql::lowering::aggregate) struct AggregateTerminalIdentity {
    identity: AggregateIdentity,
    filter_expr: Option<Expr>,
}

impl AggregateTerminalIdentity {
    // Build a canonical executable identity from a raw syntactic terminal
    // without consuming it. This lets projection dedup compare aggregate identity
    // before deciding whether to retain the raw first-seen terminal.
    #[must_use]
    pub(in crate::db::sql::lowering::aggregate) fn from_terminal(
        terminal: &SqlGlobalAggregateTerminal,
    ) -> Self {
        Self {
            identity: aggregate_identity_from_terminal(terminal),
            filter_expr: terminal.filter_expr.clone(),
        }
    }
}

///
/// PreparedAggregateTerminal
///
/// PreparedAggregateTerminal is the identity terminal handed from SQL lowering
/// to strategy preparation. It keeps the normalized aggregate identity and the
/// filter expression together after the raw terminal has been consumed.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::sql::lowering::aggregate) struct PreparedAggregateTerminal {
    identity: AggregateIdentity,
    filter_expr: Option<Expr>,
}

impl PreparedAggregateTerminal {
    // Consume one raw syntactic terminal and return the canonical identity
    // terminal that strategy preparation is allowed to interpret.
    #[must_use]
    pub(in crate::db::sql::lowering::aggregate) fn from_terminal(
        terminal: SqlGlobalAggregateTerminal,
    ) -> Self {
        Self {
            identity: aggregate_identity_from_terminal(&terminal),
            filter_expr: terminal.filter_expr,
        }
    }

    // Split the identity terminal into the normalized aggregate meaning and
    // its optional per-row filter expression.
    pub(in crate::db::sql::lowering::aggregate) fn into_parts(
        self,
    ) -> (AggregateIdentity, Option<Expr>) {
        (self.identity, self.filter_expr)
    }
}

///
/// PreparedAggregateTarget
///
/// PreparedAggregateTarget seals the mutually exclusive row, field, and
/// expression input shapes after model binding has resolved raw field names.
/// Strategy stores this target inside prepared aggregate identity so executor
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
    // is the only expansion point from the identity target into executor shape.
    pub(in crate::db::sql::lowering::aggregate) fn into_executor_parts(
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
/// PreparedAggregateIdentity
///
/// PreparedAggregateIdentity is the model-bound aggregate meaning consumed by
/// strategy. DISTINCT remains structurally present only on aggregate families
/// where it changes reducer behavior, so `MIN` and `MAX` cannot accidentally
/// carry a runtime DISTINCT bit.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::sql::lowering::aggregate) enum PreparedAggregateIdentity {
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

impl PreparedAggregateIdentity {
    // Combine normalized aggregate kind/DISTINCT identity with a model-bound
    // target. MIN/MAX deliberately discard the supplied DISTINCT bit.
    pub(in crate::db::sql::lowering::aggregate) fn from_parts(
        kind: AggregateKind,
        target: PreparedAggregateTarget,
        distinct: bool,
    ) -> Self {
        match kind {
            AggregateKind::Count => Self::Count { target, distinct },
            AggregateKind::Sum => Self::Sum { target, distinct },
            AggregateKind::Avg => Self::Avg { target, distinct },
            AggregateKind::Min => Self::Min { target },
            AggregateKind::Max => Self::Max { target },
            AggregateKind::Exists | AggregateKind::First | AggregateKind::Last => {
                unreachable!("unsupported SQL aggregate kind reached prepared aggregate identity")
            }
        }
    }

    // Return the aggregate kind represented by this prepared identity terminal.
    pub(in crate::db::sql::lowering::aggregate) const fn aggregate_kind(&self) -> AggregateKind {
        match self {
            Self::Count { .. } => AggregateKind::Count,
            Self::Sum { .. } => AggregateKind::Sum,
            Self::Avg { .. } => AggregateKind::Avg,
            Self::Min { .. } => AggregateKind::Min,
            Self::Max { .. } => AggregateKind::Max,
        }
    }

    // Return the observable DISTINCT behavior for this prepared identity
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
    const fn target(&self) -> &PreparedAggregateTarget {
        match self {
            Self::Count { target, .. }
            | Self::Sum { target, .. }
            | Self::Avg { target, .. }
            | Self::Min { target }
            | Self::Max { target } => target,
        }
    }

    // Borrow the field slot when this identity terminal is field-backed.
    pub(in crate::db::sql::lowering::aggregate) const fn target_slot(&self) -> Option<&FieldSlot> {
        self.target().field_slot()
    }

    // Borrow the expression input when this identity terminal is expression-backed.
    #[cfg(test)]
    pub(in crate::db::sql::lowering::aggregate) const fn input_expr(&self) -> Option<&Expr> {
        self.target().input_expr()
    }

    // Move this prepared identity terminal into executor input parts.
    pub(in crate::db::sql::lowering::aggregate) fn into_executor_parts(
        self,
    ) -> (Option<FieldSlot>, Option<Expr>) {
        match self {
            Self::Count { target, .. }
            | Self::Sum { target, .. }
            | Self::Avg { target, .. }
            | Self::Min { target }
            | Self::Max { target } => target.into_executor_parts(),
        }
    }
}
