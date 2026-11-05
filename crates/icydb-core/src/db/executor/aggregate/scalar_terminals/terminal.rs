//! Module: executor::aggregate::scalar_terminals::terminal
//! Responsibility: scalar aggregate terminal definitions and conversion.
//! Boundary: owns structural-to-prepared and prepared-to-interned terminal flow.

use crate::{
    db::{
        executor::{
            EntityAuthority,
            aggregate::{
                AccessPlannedQuery, AggregateKind, CompiledExpr, Expr, FieldId, FieldSlot,
                GroupedAggregateExecutionSpec, compile_scalar_projection_expr_with_schema,
                scalar_terminals::expr_cache::intern_scalar_terminal_expr,
            },
            pipeline::{
                contracts::{CursorEmissionMode, ProjectionMaterializationMode},
                runtime::compile_retained_slot_layout_for_mode_with_extra_slots,
            },
            terminal::RetainedSlotLayout,
        },
        schema::SchemaInfo,
    },
    error::InternalError,
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
        // Size expression tables from terminals that actually intern payloads.
        // Row and field inputs stay inline and should not reserve expression
        // table slots just because they share the same terminal set.
        let input_expr_capacity = terminals
            .iter()
            .filter(|terminal| matches!(terminal.input, ScalarAggregateInput::Expr(_)))
            .count();
        let filter_expr_capacity = terminals
            .iter()
            .filter(|terminal| terminal.filter.is_some())
            .count();
        let mut input_exprs = Vec::with_capacity(input_expr_capacity);
        let mut filter_exprs = Vec::with_capacity(filter_expr_capacity);
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

    /// Return the single direct-field extrema candidate that may borrow the
    /// canonical aggregate route. Terminal-local filters, DISTINCT state,
    /// expressions, and multi-terminal reductions remain complete scans.
    pub(super) const fn single_field_extrema_route_candidate(
        &self,
    ) -> Option<(AggregateKind, &str)> {
        let [terminal] = self.terminals.as_slice() else {
            return None;
        };
        if terminal.filter.is_some() || terminal.distinct {
            return None;
        }
        let InternedScalarAggregateInput::Field { field, .. } = &terminal.input else {
            return None;
        };
        let kind = match terminal.kind {
            ScalarAggregateTerminalKind::Min => AggregateKind::Min,
            ScalarAggregateTerminalKind::Max => AggregateKind::Max,
            ScalarAggregateTerminalKind::CountRows
            | ScalarAggregateTerminalKind::CountValues
            | ScalarAggregateTerminalKind::Sum
            | ScalarAggregateTerminalKind::Avg => return None,
        };

        Some((kind, field.as_str()))
    }

    pub(super) fn retained_slot_layout(
        &self,
        authority: &EntityAuthority,
        plan: &AccessPlannedQuery,
    ) -> Result<RetainedSlotLayout, InternalError> {
        let mut extra_slots = Vec::with_capacity(self.terminals.len());

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
            authority,
            plan,
            ProjectionMaterializationMode::RetainSlotRows,
            CursorEmissionMode::Suppress,
            extra_slots.as_slice(),
        )?
        .ok_or_else(InternalError::query_executor_invariant)
    }

    pub(super) fn into_runtime_inputs(
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
}

impl PreparedScalarAggregateTerminal {
    /// Build one prepared scalar aggregate terminal from validated inputs.
    #[must_use]
    pub(super) const fn from_validated_inputs(
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
        GroupedAggregateExecutionSpec::from_uncompiled_inputs(
            self.aggregate_kind,
            self.input.target_slot().cloned(),
            self.input.grouped_input_expr(),
            self.filter_expr.cloned(),
            self.distinct,
        )
    }

    fn into_prepared(
        self,
        schema: &SchemaInfo,
        budget: &dyn crate::db::resource::construction::ConstructionBudget,
    ) -> Result<PreparedScalarAggregateTerminal, InternalError> {
        let input = self.input.into_prepared(schema, budget)?;
        let filter = self
            .filter_expr
            .map(|expr| compile_structural_aggregate_expr(schema, expr, budget))
            .transpose()?;

        Ok(PreparedScalarAggregateTerminal::from_validated_inputs(
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

    fn into_prepared(
        self,
        schema: &SchemaInfo,
        budget: &dyn crate::db::resource::construction::ConstructionBudget,
    ) -> Result<ScalarAggregateInput, InternalError> {
        match self {
            Self::Rows => Ok(ScalarAggregateInput::Rows),
            Self::Field(target_slot) => Ok(ScalarAggregateInput::Field {
                slot: target_slot.index(),
                field: budget.copy_text(target_slot.field())?,
            }),
            Self::Expr(input_expr) => Ok(ScalarAggregateInput::Expr(
                compile_structural_aggregate_expr(schema, input_expr, budget)?,
            )),
            Self::MissingFieldTarget => Err(InternalError::query_executor_invariant()),
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
    schema: &SchemaInfo,
    terminal: &StructuralAggregateTerminal,
    budget: &dyn crate::db::resource::construction::ConstructionBudget,
) -> Result<PreparedScalarAggregateTerminal, InternalError> {
    resolve_structural_aggregate_terminal(terminal).into_prepared(schema, budget)
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
    schema: &SchemaInfo,
    expr: &Expr,
    budget: &dyn crate::db::resource::construction::ConstructionBudget,
) -> Result<CompiledExpr, InternalError> {
    // The scalar compiler owns field/root admission as well as construction.
    // Unsupported shapes and unknown fields share this payload-free boundary.
    let scalar = compile_scalar_projection_expr_with_schema(schema, expr, budget)?
        .ok_or_else(InternalError::query_executor_invariant)?;

    Ok(scalar)
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
