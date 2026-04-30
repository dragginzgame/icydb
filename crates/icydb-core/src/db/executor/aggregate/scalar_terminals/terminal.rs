//! Module: executor::aggregate::scalar_terminals::terminal
//! Responsibility: scalar aggregate terminal definitions and conversion.
//! Boundary: owns structural-to-prepared and prepared-to-interned terminal flow.

use crate::{
    db::{
        executor::{
            aggregate::scalar_terminals::expr_cache::intern_scalar_terminal_expr,
            pipeline::{
                contracts::{CursorEmissionMode, ProjectionMaterializationMode},
                runtime::compile_retained_slot_layout_for_mode_with_extra_slots,
            },
            terminal::RetainedSlotLayout,
        },
        query::plan::{
            AccessPlannedQuery, AggregateKind, FieldSlot, GroupedAggregateExecutionSpec,
            expr::{CompiledExpr, Expr, FieldId, compile_scalar_projection_expr},
        },
    },
    error::InternalError,
    model::entity::EntityModel,
};

///
/// StructuralAggregateTerminal
///
/// StructuralAggregateTerminal describes one scalar aggregate terminal before
/// executor-local projection programs are compiled. It keeps canonical
/// expression and resolved field-slot inputs together for terminal preparation.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct StructuralAggregateTerminal {
    kind: StructuralAggregateTerminalKind,
    target_slot: Option<FieldSlot>,
    input_expr: Option<Expr>,
    filter_expr: Option<Expr>,
    distinct: bool,
}

impl StructuralAggregateTerminal {
    /// Build one structural scalar aggregate terminal request.
    #[must_use]
    pub(in crate::db) const fn new(
        kind: StructuralAggregateTerminalKind,
        target_slot: Option<FieldSlot>,
        input_expr: Option<Expr>,
        filter_expr: Option<Expr>,
        distinct: bool,
    ) -> Self {
        Self {
            kind,
            target_slot,
            input_expr,
            filter_expr,
            distinct,
        }
    }

    pub(super) fn uses_shared_count_terminal(&self, model: &EntityModel) -> bool {
        match self.kind {
            StructuralAggregateTerminalKind::CountRows => {
                self.filter_expr.is_none() && !self.distinct
            }
            StructuralAggregateTerminalKind::CountValues => {
                if self.filter_expr.is_some() || self.input_expr.is_some() || self.distinct {
                    return false;
                }
                let Some(target_slot) = self.target_slot.as_ref() else {
                    return false;
                };
                let Some(field) = model.fields().get(target_slot.index()) else {
                    return false;
                };

                !field.nullable()
            }
            StructuralAggregateTerminalKind::Sum
            | StructuralAggregateTerminalKind::Avg
            | StructuralAggregateTerminalKind::Min
            | StructuralAggregateTerminalKind::Max => false,
        }
    }
}

///
/// StructuralAggregateTerminalKind
///
/// StructuralAggregateTerminalKind selects the reducer family requested by one
/// structural global aggregate terminal. Count-row and count-value variants
/// stay separate because they have different input and fast-count eligibility.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum StructuralAggregateTerminalKind {
    CountRows,
    CountValues,
    Sum,
    Avg,
    Min,
    Max,
}

///
/// PreparedScalarAggregateTerminalSet
///
/// PreparedScalarAggregateTerminalSet carries the uncached scalar aggregate
/// terminals that one caller wants to reduce over a prepared scalar access
/// and window plan. It exists so callers can keep aggregate-specific metadata
/// out of `SharedPreparedExecutionPlan` while executor code owns row reduction.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PreparedScalarAggregateTerminalSet {
    terminals: Vec<InternedPreparedScalarAggregateTerminal>,
    input_exprs: Vec<CompiledExpr>,
    filter_exprs: Vec<CompiledExpr>,
}

impl PreparedScalarAggregateTerminalSet {
    /// Build one terminal set from caller-prepared scalar aggregate terminals.
    #[must_use]
    pub(super) fn new(terminals: Vec<PreparedScalarAggregateTerminal>) -> Self {
        let mut input_exprs = Vec::new();
        let mut filter_exprs = Vec::new();
        let terminals = terminals
            .into_iter()
            .map(|terminal| terminal.into_interned(&mut input_exprs, &mut filter_exprs))
            .collect();

        Self {
            terminals,
            input_exprs,
            filter_exprs,
        }
    }

    pub(super) const fn is_empty(&self) -> bool {
        self.terminals.is_empty()
    }

    #[cfg(feature = "diagnostics")]
    pub(super) const fn terminal_count(&self) -> usize {
        self.terminals.len()
    }

    #[cfg(feature = "diagnostics")]
    pub(super) const fn input_expr_count(&self) -> usize {
        self.input_exprs.len()
    }

    #[cfg(feature = "diagnostics")]
    pub(super) const fn filter_expr_count(&self) -> usize {
        self.filter_exprs.len()
    }

    pub(super) fn retained_slot_layout(
        &self,
        model: &EntityModel,
        plan: &AccessPlannedQuery,
    ) -> Result<RetainedSlotLayout, InternalError> {
        let mut extra_slots = Vec::new();

        // Phase 1: collect only terminal-local slot requirements. The scalar
        // runtime helper will add access, residual filter, ordering, and other
        // plan-owned requirements from the prepared scalar plan.
        for terminal in &self.terminals {
            terminal.input.extend_referenced_slots(&mut extra_slots);
        }
        for expr in &self.input_exprs {
            expr.extend_referenced_slots(&mut extra_slots);
        }
        for expr in &self.filter_exprs {
            expr.extend_referenced_slots(&mut extra_slots);
        }

        // Phase 2: compile a retained-slot layout for this execution only.
        // RetainSlotRows keeps even empty-slot COUNT(*) filters row-shaped so
        // the reducer still sees one retained row per scalar input row.
        compile_retained_slot_layout_for_mode_with_extra_slots(
            model,
            plan,
            ProjectionMaterializationMode::RetainSlotRows,
            CursorEmissionMode::Suppress,
            extra_slots.as_slice(),
        )
        .ok_or_else(|| {
            InternalError::query_executor_invariant(
                "scalar aggregate terminal execution requires a retained-slot layout",
            )
        })
    }

    pub(super) fn into_runtime_parts(
        self,
    ) -> (
        Vec<InternedPreparedScalarAggregateTerminal>,
        Vec<CompiledExpr>,
        Vec<CompiledExpr>,
    ) {
        (self.terminals, self.input_exprs, self.filter_exprs)
    }
}

///
/// PreparedScalarAggregateTerminal
///
/// PreparedScalarAggregateTerminal describes one executor-owned scalar
/// aggregate reducer. The input and optional filter are already compiled to
/// scalar projection programs so execution reads retained slots without
/// reopening adapter or planner expression trees.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PreparedScalarAggregateTerminal {
    kind: ScalarAggregateTerminalKind,
    input: ScalarAggregateInput,
    filter: Option<CompiledExpr>,
    distinct: bool,
    empty_behavior: AggregateEmptyBehavior,
}

impl PreparedScalarAggregateTerminal {
    /// Build one prepared scalar aggregate terminal from validated parts.
    #[must_use]
    pub(super) const fn from_validated_parts(
        kind: ScalarAggregateTerminalKind,
        input: ScalarAggregateInput,
        filter: Option<CompiledExpr>,
        distinct: bool,
    ) -> Self {
        Self {
            kind,
            input,
            filter,
            distinct,
            empty_behavior: kind.empty_behavior(),
        }
    }

    fn into_interned(
        self,
        input_exprs: &mut Vec<CompiledExpr>,
        filter_exprs: &mut Vec<CompiledExpr>,
    ) -> InternedPreparedScalarAggregateTerminal {
        let input = match self.input {
            ScalarAggregateInput::Rows => InternedScalarAggregateInput::Rows,
            ScalarAggregateInput::Field { slot, field } => {
                InternedScalarAggregateInput::Field { slot, field }
            }
            ScalarAggregateInput::Expr(expr) => {
                InternedScalarAggregateInput::Expr(intern_scalar_terminal_expr(input_exprs, expr))
            }
        };
        let filter = self
            .filter
            .map(|expr| intern_scalar_terminal_expr(filter_exprs, expr));

        InternedPreparedScalarAggregateTerminal {
            kind: self.kind,
            input,
            filter,
            distinct: self.distinct,
            empty_behavior: self.empty_behavior,
        }
    }
}

///
/// ScalarAggregateTerminalKind
///
/// ScalarAggregateTerminalKind selects the reducer family used for one
/// scalar-window aggregate terminal. These variants intentionally model only
/// row-count, value-count, numeric, and extrema reducers, not grouped folds.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ScalarAggregateTerminalKind {
    CountRows,
    CountValues,
    Sum,
    Avg,
    Min,
    Max,
}

impl ScalarAggregateTerminalKind {
    // Keep empty-window behavior attached to the executor-owned terminal kind
    // so callers cannot choose a reducer/finalizer combination that drifts
    // from aggregate empty-window policy.
    const fn empty_behavior(self) -> AggregateEmptyBehavior {
        match self {
            Self::CountRows | Self::CountValues => AggregateEmptyBehavior::Zero,
            Self::Sum | Self::Avg | Self::Min | Self::Max => AggregateEmptyBehavior::Null,
        }
    }
}

///
/// ScalarAggregateInput
///
/// ScalarAggregateInput identifies how one terminal obtains its per-row input
/// value from the retained-slot scalar row. `Rows` consumes only row presence,
/// while field and expression inputs materialize one value per admitted row.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ScalarAggregateInput {
    Rows,
    Field { slot: usize, field: String },
    Expr(CompiledExpr),
}

///
/// InternedPreparedScalarAggregateTerminal
///
/// InternedPreparedScalarAggregateTerminal is the executor-local runtime form of
/// one scalar aggregate terminal after the containing terminal set has assigned
/// repeated input and filter expressions to shared expression tables.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct InternedPreparedScalarAggregateTerminal {
    pub(super) kind: ScalarAggregateTerminalKind,
    pub(super) input: InternedScalarAggregateInput,
    pub(super) filter: Option<usize>,
    pub(super) distinct: bool,
    pub(super) empty_behavior: AggregateEmptyBehavior,
}

///
/// InternedScalarAggregateInput
///
/// InternedScalarAggregateInput keeps direct row and field inputs inline while
/// expression-backed inputs refer to the terminal set's shared input-expression
/// table, allowing execution to evaluate each unique expression once per row.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum InternedScalarAggregateInput {
    Rows,
    Field { slot: usize, field: String },
    Expr(usize),
}

impl InternedScalarAggregateInput {
    fn extend_referenced_slots(&self, slots: &mut Vec<usize>) {
        match self {
            Self::Rows | Self::Expr(_) => {}
            Self::Field { slot, .. } => {
                if !slots.contains(slot) {
                    slots.push(*slot);
                }
            }
        }
    }
}

///
/// AggregateEmptyBehavior
///
/// AggregateEmptyBehavior preserves the scalar aggregate finalization
/// contract for empty or all-NULL input windows. COUNT terminals finalize to
/// zero, while numeric and extrema terminals finalize to NULL.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AggregateEmptyBehavior {
    Zero,
    Null,
}

///
/// ResolvedStructuralAggregateTerminal
///
/// ResolvedStructuralAggregateTerminal is the single internal interpretation
/// of one structural aggregate terminal.
/// Request compilation and prepared reducer construction both consume this
/// view so aggregate kind, input, filter, and distinct semantics converge in
/// one owner.
///

pub(super) struct ResolvedStructuralAggregateTerminal<'a> {
    aggregate_kind: AggregateKind,
    scalar_kind: ScalarAggregateTerminalKind,
    input: ResolvedStructuralAggregateInput<'a>,
    filter_expr: Option<&'a Expr>,
    distinct: bool,
}

impl ResolvedStructuralAggregateTerminal<'_> {
    pub(super) fn into_grouped_spec(self) -> GroupedAggregateExecutionSpec {
        GroupedAggregateExecutionSpec::from_uncompiled_parts(
            self.aggregate_kind,
            self.input.target_slot().cloned(),
            self.input.grouped_input_expr(),
            self.filter_expr.cloned(),
            self.distinct,
        )
    }

    fn into_prepared(
        self,
        model: &EntityModel,
    ) -> Result<PreparedScalarAggregateTerminal, InternalError> {
        let input = self.input.into_prepared(model)?;
        let filter = self
            .filter_expr
            .map(|expr| compile_structural_aggregate_expr(model, expr, "filter"))
            .transpose()?;

        Ok(PreparedScalarAggregateTerminal::from_validated_parts(
            self.scalar_kind,
            input,
            filter,
            self.distinct,
        ))
    }
}

///
/// ResolvedStructuralAggregateInput
///
/// ResolvedStructuralAggregateInput keeps the structural terminal input
/// decision shared across grouped-spec construction and scalar reducer
/// preparation.
/// Field inputs retain the planner-resolved slot for scalar execution and can
/// still project the equivalent grouped field expression when needed.
///

enum ResolvedStructuralAggregateInput<'a> {
    Rows,
    Field(&'a FieldSlot),
    Expr(&'a Expr),
    MissingFieldTarget,
}

impl ResolvedStructuralAggregateInput<'_> {
    const fn target_slot(&self) -> Option<&FieldSlot> {
        match self {
            Self::Rows | Self::Expr(_) | Self::MissingFieldTarget => None,
            Self::Field(slot) => Some(slot),
        }
    }

    fn grouped_input_expr(&self) -> Option<Expr> {
        match self {
            Self::Rows | Self::MissingFieldTarget => None,
            Self::Field(slot) => Some(Expr::Field(FieldId::new(slot.field()))),
            Self::Expr(expr) => Some((*expr).clone()),
        }
    }

    fn into_prepared(self, model: &EntityModel) -> Result<ScalarAggregateInput, InternalError> {
        match self {
            Self::Rows => Ok(ScalarAggregateInput::Rows),
            Self::Field(target_slot) => Ok(ScalarAggregateInput::Field {
                slot: target_slot.index(),
                field: target_slot.field().to_string(),
            }),
            Self::Expr(input_expr) => Ok(ScalarAggregateInput::Expr(
                compile_structural_aggregate_expr(model, input_expr, "input")?,
            )),
            Self::MissingFieldTarget => Err(InternalError::query_executor_invariant(
                "field-target structural aggregate terminal requires a resolved field slot",
            )),
        }
    }
}

pub(super) const fn resolve_structural_aggregate_terminal(
    terminal: &StructuralAggregateTerminal,
) -> ResolvedStructuralAggregateTerminal<'_> {
    let (aggregate_kind, scalar_kind) = terminal.kind.resolved_kinds();
    let input = match terminal.kind {
        StructuralAggregateTerminalKind::CountRows => ResolvedStructuralAggregateInput::Rows,
        StructuralAggregateTerminalKind::CountValues
        | StructuralAggregateTerminalKind::Sum
        | StructuralAggregateTerminalKind::Avg
        | StructuralAggregateTerminalKind::Min
        | StructuralAggregateTerminalKind::Max => {
            if let Some(input_expr) = terminal.input_expr.as_ref() {
                ResolvedStructuralAggregateInput::Expr(input_expr)
            } else if let Some(target_slot) = terminal.target_slot.as_ref() {
                ResolvedStructuralAggregateInput::Field(target_slot)
            } else {
                ResolvedStructuralAggregateInput::MissingFieldTarget
            }
        }
    };

    ResolvedStructuralAggregateTerminal {
        aggregate_kind,
        scalar_kind,
        input,
        filter_expr: terminal.filter_expr.as_ref(),
        distinct: terminal.distinct,
    }
}

pub(super) fn compile_structural_scalar_aggregate_terminal(
    model: &EntityModel,
    terminal: &StructuralAggregateTerminal,
) -> Result<PreparedScalarAggregateTerminal, InternalError> {
    resolve_structural_aggregate_terminal(terminal).into_prepared(model)
}

impl StructuralAggregateTerminalKind {
    const fn resolved_kinds(self) -> (AggregateKind, ScalarAggregateTerminalKind) {
        match self {
            Self::CountRows => (AggregateKind::Count, ScalarAggregateTerminalKind::CountRows),
            Self::CountValues => (
                AggregateKind::Count,
                ScalarAggregateTerminalKind::CountValues,
            ),
            Self::Sum => (AggregateKind::Sum, ScalarAggregateTerminalKind::Sum),
            Self::Avg => (AggregateKind::Avg, ScalarAggregateTerminalKind::Avg),
            Self::Min => (AggregateKind::Min, ScalarAggregateTerminalKind::Min),
            Self::Max => (AggregateKind::Max, ScalarAggregateTerminalKind::Max),
        }
    }
}

fn compile_structural_aggregate_expr(
    model: &EntityModel,
    expr: &Expr,
    label: &str,
) -> Result<CompiledExpr, InternalError> {
    if let Some(field) = first_unknown_structural_aggregate_expr_field(model, expr) {
        return Err(InternalError::query_executor_invariant(format!(
            "unknown expression field '{field}'",
        )));
    }

    let scalar = compile_scalar_projection_expr(model, expr).ok_or_else(|| {
        InternalError::query_executor_invariant(format!(
            "structural aggregate {label} expression must compile on the scalar seam",
        ))
    })?;

    Ok(CompiledExpr::compile(&scalar))
}

fn first_unknown_structural_aggregate_expr_field(
    model: &EntityModel,
    expr: &Expr,
) -> Option<String> {
    let mut first_unknown = None;
    let _ = expr.try_for_each_tree_expr(&mut |node| {
        if first_unknown.is_some() {
            return Ok(());
        }
        if let Expr::Field(field) = node
            && model.resolve_field_slot(field.as_str()).is_none()
        {
            first_unknown = Some(field.as_str().to_string());
        }

        Ok::<(), ()>(())
    });

    first_unknown
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            executor::aggregate::scalar_terminals::terminal::{
                InternedScalarAggregateInput, PreparedScalarAggregateTerminal,
                PreparedScalarAggregateTerminalSet, ScalarAggregateInput,
                ScalarAggregateTerminalKind,
            },
            query::plan::expr::{BinaryOp, CompiledExpr},
        },
        value::Value,
    };

    fn literal_uint(value: u64) -> CompiledExpr {
        CompiledExpr::Literal(Value::Uint(value))
    }

    fn repeated_input_expr() -> CompiledExpr {
        CompiledExpr::Binary {
            op: BinaryOp::Add,
            left: Box::new(literal_uint(41)),
            right: Box::new(literal_uint(1)),
        }
    }

    fn repeated_filter_expr() -> CompiledExpr {
        CompiledExpr::Binary {
            op: BinaryOp::Gte,
            left: Box::new(literal_uint(42)),
            right: Box::new(literal_uint(1)),
        }
    }

    #[test]
    fn scalar_aggregate_terminal_set_interns_duplicate_input_and_filter_exprs() {
        let input = repeated_input_expr();
        let filter = repeated_filter_expr();
        let terminals = PreparedScalarAggregateTerminalSet::new(vec![
            PreparedScalarAggregateTerminal::from_validated_parts(
                ScalarAggregateTerminalKind::Sum,
                ScalarAggregateInput::Expr(input.clone()),
                Some(filter.clone()),
                false,
            ),
            PreparedScalarAggregateTerminal::from_validated_parts(
                ScalarAggregateTerminalKind::Avg,
                ScalarAggregateInput::Expr(input),
                Some(filter),
                false,
            ),
        ]);

        assert_eq!(
            terminals.input_exprs.len(),
            1,
            "duplicate SUM/AVG input expressions should share one interned input expression",
        );
        assert_eq!(
            terminals.filter_exprs.len(),
            1,
            "duplicate aggregate FILTER expressions should share one interned filter expression",
        );
        assert!(
            terminals
                .terminals
                .iter()
                .all(|terminal| matches!(terminal.input, InternedScalarAggregateInput::Expr(0))),
            "every expression-backed terminal should point at the shared input expression",
        );
        assert!(
            terminals
                .terminals
                .iter()
                .all(|terminal| terminal.filter == Some(0)),
            "every filtered terminal should point at the shared filter expression",
        );
    }

    #[test]
    fn scalar_aggregate_terminal_set_keeps_field_inputs_out_of_expr_table() {
        let terminals = PreparedScalarAggregateTerminalSet::new(vec![
            PreparedScalarAggregateTerminal::from_validated_parts(
                ScalarAggregateTerminalKind::CountValues,
                ScalarAggregateInput::Field {
                    slot: 2,
                    field: "age".to_string(),
                },
                None,
                false,
            ),
            PreparedScalarAggregateTerminal::from_validated_parts(
                ScalarAggregateTerminalKind::Sum,
                ScalarAggregateInput::Expr(repeated_input_expr()),
                None,
                false,
            ),
        ]);

        assert_eq!(
            terminals.input_exprs.len(),
            1,
            "only expression-backed aggregate inputs should enter the input expression table",
        );
        assert!(
            matches!(
                terminals.terminals[0].input,
                InternedScalarAggregateInput::Field { slot: 2, .. }
            ),
            "field-backed aggregate inputs should remain direct retained-slot reads",
        );
        assert!(
            matches!(
                terminals.terminals[1].input,
                InternedScalarAggregateInput::Expr(0)
            ),
            "the expression-backed aggregate input should point at its interned expression",
        );
    }
}
